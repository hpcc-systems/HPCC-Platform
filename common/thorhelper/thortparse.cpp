/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "jliball.hpp"
#include "eclrtl.hpp"
#include "rtlds_imp.hpp"
#include "thortparse.ipp"

//#define TRACING

//---------------------------------------------------------------------------
void doUnwindRelease(GrammarSymbol * symbol, CIArrayOf<GrammarSymbol> & pending)
{
    if (!symbol->isPacked())
    {
        unsigned level = pending.ordinality();
        unsigned num = symbol->numChildren();
        for (unsigned i = 0; i < num; i++)
            pending.append(*LINK(symbol->queryChild(i)));
        if (!symbol->Release())
            pending.trunc(level);
    }
    else
        symbol->Release();
}


bool GrammarSymbol::canMerge(const GrammarSymbol * other)
{
    //MORE: Shouldn't this be enabled at some point?
    return false;

    byte * leftRow = queryResultRow();
    byte * rightRow = other->queryResultRow();
    if (leftRow || rightRow)
    {
        assertex(leftRow && rightRow);              // if in same state they really should have consistency here
        assertex(queryResultSize() == other->queryResultSize());
        if (memcmp(leftRow, rightRow, queryResultSize()) != 0)
            return false;
    }

    assertex(features.info == other->features.info);
    size_t len = features.values.length();
    return memcmp(features.values.get(), other->features.values.get(), len) == 0;
}

GrammarSymbol * GrammarSymbol::createMerged(GrammarSymbol * other)
{
    Owned<GrammarSymbol> merged = new PackedSymbol(this);
    return merged->createMerged(other);
}

//---------------------------------------------------------------------------

Terminal::Terminal(symbol_id _id, const FeatureInfo * featureInfo, unsigned _len, const byte * _start) : GrammarSymbol(_id)
{
    if (featureInfo && featureInfo->getNum() != 0)
    {
        features.info = featureInfo;
        unsigned size = featureInfo->getSize();
        void * resultFeatures = features.values.allocate(size);
        memcpy(resultFeatures, featureInfo->defaults, size);
    }

    len = _len;
    start = _start;
}

//---------------------------------------------------------------------------

NonTerminal::NonTerminal(symbol_id _id, IAtom * _name, FeatureValue & _features, unsigned numSymbols, GrammarSymbol * * symbols, const byte * _reducePtr, size32_t _resultSize, byte * _resultRow) : GrammarSymbol(_id), resultSize(_resultSize), resultRow(_resultRow)
{
    unsigned nullCount = 0;
    unsigned nonNullIndex = 0;
    reduced.ensure(numSymbols);
    for (unsigned i= 0; i < numSymbols; i++)
    {
        GrammarSymbol * cur = symbols[i];
        if (cur->isNull())
            nullCount++;
        else
            nonNullIndex = i;

        reduced.append(*LINK(cur));
        addPenalty(cur->getPenalty());
    }

    cachedIsNull = false;
    if (nullCount == numSymbols)
        cachedIsNull = true;
    else if (nullCount != 0)
    {
        if (nonNullIndex+1 != numSymbols)
        {
            const byte * pos = symbols[nonNullIndex]->queryEndPtr();
            for (unsigned i1 = nonNullIndex+1; i1 < numSymbols; i1++)
                symbols[i1]->resetPosition(pos);
        }
        for (unsigned i2 = nonNullIndex; i2-- > 0;)
        {
            if (symbols[i2]->isNull())
                symbols[i2]->resetPosition(symbols[i2+1]->queryStartPtr());
        }
    }

    name = _name;
    size_t len = _features.values.length();
    features.info = _features.info;
    if (len)
        features.values.setOwn(len, _features.values.detach());
    if (numSymbols == 0)
        reducePtr = _reducePtr;
    else
        reducePtr = NULL;
}


NonTerminal::~NonTerminal()
{
    if (resultRow)
        rtlReleaseRow(resultRow);

    //Try and kill tail recursion when freeing a very large match structure
    while (reduced.ordinality())
        doUnwindRelease(&reduced.popGet(), reduced);
}

void NonTerminal::resetPosition(const byte * pos)
{
    assertex(cachedIsNull);
    reducePtr = pos;
    ForEachItemInRev(idx, reduced)
        reduced.item(idx).resetPosition(pos);
}

const byte * NonTerminal::queryStartPtr() const
{ 
    if (reduced.ordinality())
        return reduced.item(0).queryStartPtr();
    return reducePtr;
}

const byte * NonTerminal::queryEndPtr() const
{ 
    if (reduced.ordinality())
        return reduced.tos().queryEndPtr();
    return reducePtr;
}

GrammarSymbol * NonTerminal::queryChild(unsigned i)         
{ 
    if (reduced.isItem(i))
        return &reduced.item(i);
    return NULL; 
}
//---------------------------------------------------------------------------

PackedSymbol::PackedSymbol(GrammarSymbol * symbol) : GrammarSymbol(symbol->id)
{
    //copy the features...
    assertex(!symbol->isNull());
    equivalents.append(*LINK(symbol));
    penalty = symbol->getPenalty();
}

void PackedSymbol::resetPosition(const byte * pos)
{
    throwUnexpected();
}

GrammarSymbol * PackedSymbol::createMerged(GrammarSymbol * other)
{
    assertex(!other->isNull());
    assertex(other->queryStartPtr() == queryStartPtr());
    assertex(other->queryEndPtr() == queryEndPtr());
    if (other->getPenalty() < penalty)
        penalty = other->getPenalty();
    if (other->isPacked())
    {
        //Can happen when two activity states are merged.
        for (unsigned i = 0; ; i++)
        {
            GrammarSymbol * child = queryPacked(i);
            if (!child)
                break;
            equivalents.append(*LINK(child));
        }
    }
    else
        equivalents.append(*LINK(other));
    return LINK(this);
}

GrammarSymbol * PackedSymbol::queryPacked(unsigned i)
{ 
    if (equivalents.isItem(i))
        return &equivalents.item(i);
    return NULL;
}

const byte * PackedSymbol::queryStartPtr() const                
{ 
    return equivalents.item(0).queryStartPtr(); 
}


const byte * PackedSymbol::queryEndPtr() const              
{ 
    return equivalents.item(0).queryEndPtr(); 
}

//---------------------------------------------------------------------------

TomitaMatchWalker::TomitaMatchWalker(const PackedSymbolChoice & _choice, GrammarSymbol * _symbol) : choice(_choice)
{
    symbol = _symbol;
    while (symbol->isPacked())
    {
        symbol = symbol->queryPacked(choice.getInstance(symbol));
    }
}

IAtom * TomitaMatchWalker::queryName()
{
    return symbol->queryName();
}

unsigned TomitaMatchWalker::queryID()
{
    return symbol->getId();
}

size32_t TomitaMatchWalker::queryMatchSize()
{
    return symbol->getLength();
}

const void * TomitaMatchWalker::queryMatchStart()
{
    return symbol->queryStartPtr();
}

unsigned TomitaMatchWalker::numChildren()
{
    return symbol->numChildren();
}

IMatchWalker * TomitaMatchWalker::getChild(unsigned idx)
{
    GrammarSymbol * child = symbol->queryChild(idx);
    if (child)
        return new TomitaMatchWalker(choice, child);
    return NULL;
}

//---------------------------------------------------------------------------

void PackedSymbolChoice::expandFirst(GrammarSymbol * symbol)
{
    if (symbol->isPacked())
    {
        symbols.append(*LINK(symbol));
        branches.append(0);
        expandFirst(symbol->queryPacked(0));
    }
    else 
    {
        unsigned max = symbol->numChildren();
        for (unsigned i = 0; i < max; i++)
            expandFirst(symbol->queryChild(i));
    }
}

void PackedSymbolChoice::first(GrammarSymbol * symbol)
{
    branches.kill();
    symbols.kill();
    expandFirst(symbol);
}

void PackedSymbolChoice::expandBest(GrammarSymbol * symbol)
{
    if (symbol->isPacked())
    {
        symbols.append(*LINK(symbol));
        unsigned bestIndex = 0;
        int bestPenalty = symbol->queryPacked(bestIndex)->getPenalty();
        for (unsigned j=1;;j++)
        {
            GrammarSymbol * cur = symbol->queryPacked(j);
            if (!cur) break;
            if (bestPenalty > cur->getPenalty())
            {
                bestIndex = j;
                bestPenalty = cur->getPenalty();
            }
        }
        branches.append(bestIndex);
        expandBest(symbol->queryPacked(bestIndex));
    }
    else 
    {
        unsigned max = symbol->numChildren();
        for (unsigned i = 0; i < max; i++)
            expandBest(symbol->queryChild(i));
    }
}

void PackedSymbolChoice::selectBest(GrammarSymbol * symbol)
{
    branches.kill();
    symbols.kill();
    expandBest(symbol);
}

bool PackedSymbolChoice::expandNext(unsigned & level, GrammarSymbol * symbol)
{
    unsigned thisLevel = level;
    if (symbol->isPacked())
    {
        assertex(&symbols.item(thisLevel) == symbol);
        unsigned curBranch = branches.item(thisLevel);
        bool isLastLevel = (thisLevel == symbols.ordinality()-1);
        level++;
        if (!isLastLevel && expandNext(level, symbol->queryPacked(curBranch)))
            return true;

        curBranch++;
        if (symbol->queryPacked(curBranch))
        {
            branches.replace(curBranch, thisLevel);
            expandFirst(symbol->queryPacked(curBranch));
            return true;
        }
        branches.pop();
        symbols.pop();
        return false;
    }
    else 
    {
        unsigned max = symbol->numChildren();
        for (unsigned i = 0; i < max; i++)
            if (!expandNext(level, symbol->queryChild(i)))
                return false;
        return true;
    }
}

bool PackedSymbolChoice::next(GrammarSymbol * symbol)
{
    if (symbols.ordinality() == 0)
        return false;
    unsigned level = 0;
    return expandNext(level, symbol);
}

unsigned PackedSymbolChoice::getInstance(GrammarSymbol * symbol) const
{
    unsigned match = symbols.find(*symbol);
    assertex(match != NotFound);
    return branches.item(match);
}


//---------------------------------------------------------------------------

StackElement::StackElement(GrammarSymbol * _shifted, state_id _state, StackElement * _prev, LRParser * _pool)
{
    shifted.set(_shifted);
    state = _state;
    prev.set(_prev);
    pool = _pool;
}

void StackElement::addSibling(StackElement * sib)
{
    sib->sibling.setown(sibling.getClear());
    sibling.set(sib);
}


void StackElement::addToPool()
{
    StackElementArray pending;

    gatherPoolPending(pending);
    doAddToPool();

    while (pending.ordinality())
    {
        Owned<StackElement> cur = &pending.popGet();
        cur->gatherPoolPending(pending);
        cur->doAddToPool();
    }
}

void StackElement::doAddToPool()
{
    shifted.clear();
    prev.clear();
    sibling.clear();
    state = 0;
    pool->addFreeElement(this);
    pool = NULL;
}

void StackElement::Release()
{
    if (pool && !IsShared())
        addToPool();
    CInterface::Release();
}

void StackElement::getDebugText(StringBuffer & s)
{
    if (prev)
        prev->getDebugText(s);
    s.appendf("%p", this);
    s.append("[");
    if(shifted)
    {
        s.append(shifted->id).append(" ");
        s.appendf("{%p}", shifted->queryStartPtr());
    }
    s.append(state).append("] ");
}

bool StackElement::potentialPackNode(StackElement & other) const
{
    GrammarSymbol * curSymbol = shifted;
    GrammarSymbol * nextSymbol = other.shifted;
    if (curSymbol && !curSymbol->isNull() && curSymbol->id == nextSymbol->id)
    {
        if (other.prev->state == prev->state)
        {
            if (curSymbol->queryStartPtr() == nextSymbol->queryStartPtr())
            {
                assertex(curSymbol->queryEndPtr() == nextSymbol->queryEndPtr());
                assertex(state == other.state);
                return true;
            }
        }
    }
    return false;
}



void StackElement::gatherPoolPending(CIArrayOf<StackElement> & pending)
{
    if (prev && !prev->IsShared())
        pending.append(*prev.getClear());
    if (sibling && !sibling->IsShared())
        pending.append(*sibling.getClear());
}

//---------------------------------------------------------------------------
LRActiveState::LRActiveState(unsigned _maxStates)       
{ 
    maxStates = _maxStates;
    cacheBytes = maxStates * sizeof(StackElement *);
    cache = new StackElement * [maxStates]; 
    beenReduced = new bool [maxStates];
    memset(beenReduced, 0, maxStates);
    memset(cache, 0, cacheBytes); 
}

LRActiveState::~LRActiveState()                                     
{ 
    delete [] cache; 
    delete [] beenReduced;
}

void LRActiveState::addElementOwn(StackElement * next, bool keepBest)
{
    unsigned stateId = next->state;
    if (cache[stateId])
    {
        if (!mergePackedNode(stateId, next, keepBest))
            cache[stateId]->addSibling(next);
        next->Release();
    }
    else
    {
        cache[stateId] = next;
        elements.append(*next);
    }
}

void LRActiveState::clearReduced()
{
    ForEachItemIn(idx, elements)
        beenReduced[elements.item(idx).state] = false;
}

bool LRActiveState::mergePackedNode(unsigned stateId, StackElement * next, bool keepBest)
{
    StackElement * cur = cache[stateId];
    assertex(cur->state == next->state);
    assertex(!next->sibling);
    // If two symbols have the same id/attributes, and lie between the same states, then it is a locally
    // ambiguous tree, so common the symbol values up.
    GrammarSymbol * nextSymbol = next->shifted;
    if (!nextSymbol)
        return false;       // MORE: Should this common???
    if (nextSymbol->isNull())       // Hard to know the position of these things...
        return false;

    StackElement * prev = NULL;
    do
    {
        GrammarSymbol * curSymbol = cur->shifted;
        if (curSymbol && !curSymbol->isNull() && (curSymbol->id == nextSymbol->id))
        {
            if (next->prev->state == cur->prev->state)
            {
                if (curSymbol->queryStartPtr() == nextSymbol->queryStartPtr())
                {
                    assertex(curSymbol->queryEndPtr() == nextSymbol->queryEndPtr());
                    assertex(cur->state == next->state);
                    if (keepBest)
                    {
                        int curPenalty = curSymbol->getPenalty();
                        int nextPenalty = nextSymbol->getPenalty();
                        if (curPenalty != nextPenalty)
                        {
                            //Existing element is better=>throw away the new one.
                            if (nextPenalty > curPenalty)
                                return true;

                            //The new element is better, throw away all the mismatches, by removing this element, and going around again
                            if (prev)
                            {
                                cur = prev;
                                prev->sibling.set(prev->sibling->sibling);
                            }
                            else
                            {
                                StackElement * sibling = cur->sibling.getClear();
                                unsigned replacePos = elements.find(*cur);
                                if (sibling)
                                {
                                    //More than one element => replace the head with the (linked) sibling
                                    elements.replace(*sibling, replacePos);
                                    cache[stateId] = sibling;
                                    cur = sibling;
                                    continue;
                                }
                                else
                                {
                                    elements.replace(*LINK(next), replacePos);
                                    cache[stateId] = next;
                                    return true;
                                }
                            }
                        }
                    }
                    if (curSymbol->canMerge(nextSymbol))
                    {
                        cur->shifted.setown(curSymbol->createMerged(nextSymbol));
#ifdef TRACING
                        LOG(MCdebugProgress, unknownJob, "Nodes Merged: %p = %p, %p", cur->shifted.get(), curSymbol, nextSymbol);
#endif
                        return true;
                    }
                }
            }
        }
        prev = cur;
        cur = cur->sibling;
    } while (cur);
    return false;
}
            
void LRActiveState::reinit()
{ 
    memset(cache, 0, cacheBytes); 
    elements.kill(); 
}

void LRActiveState::mergeIn(LRActiveState & other, bool keepBest)
{
    ForEachItemIn(idx, other.elements)
    {
        Linked<StackElement> cur = &other.elements.item(idx);

        while (cur)
        {
            Linked<StackElement> next = cur->sibling;
            cur->sibling.clear();
            addElementOwn(cur.getClear(), keepBest);
            cur.setown(next.getClear());
        }
    }
}

//---------------------------------------------------------------------------

LRParser::LRParser(const LRTable & _table, const TomitaParserCallback & _rowState) : table(_table), rowState(_rowState)
{
    reduced = new LRActiveState(table.numStates);
    reducedOverflow[0] = new LRActiveState(table.numStates);
    reducedOverflow[1] = new LRActiveState(table.numStates);
    _clear(positions);
    init();
}

LRParser::~LRParser()
{
    //Safely kill the free list.
    StackElement * head = nextFreeElement.getClear();
    while (head)
    {
        StackElement * next = head->sibling.getClear();
        head->Release();
        head = next;
    }

    for (position_t pos = 0; pos < MAX_POSITIONS; pos++)
        delete positions[pos];

    delete reduced;
    delete reducedOverflow[0];
    delete reducedOverflow[1];
}

void LRParser::addFreeElement(StackElement * element)
{
    element->sibling.setown(nextFreeElement.getClear());
    nextFreeElement.set(element);
}

void LRParser::beginParse(bool _chooseBest)
{
    chooseBest = _chooseBest;
    selectEndPosition(0);
    addStartState();
    accepted.kill();
}


void LRParser::endParse()
{
}


void LRParser::clear()
{
    activeInput = NULL;
    activeOutput = NULL;
    firstPosition = NotFound;
    endPosition = NotFound;
}

void LRParser::cleanupPosition(position_t pos)
{
    unsigned index = pos % MAX_POSITIONS;
    positions[index]->reinit();
}

void LRParser::init()
{
    chooseBest = false;
    reduced->reinit();
    reducedOverflow[0]->reinit();
    reducedOverflow[1]->reinit();
    clear();
}

void LRParser::addStartState()
{
    activeOutput->addElementOwn(createState(NULL, (state_id)table.rootState, NULL), false);
}

StackElement * LRParser::createState(StackElement * prev, state_id nextState, GrammarSymbol * shifted)
{
    StackElement * newState;
    if (nextFreeElement)
    {
        newState = nextFreeElement.getClear();
        nextFreeElement.setown(newState->sibling.getClear());
        newState->shifted.set(shifted);
        newState->prev.set(prev);
        newState->state = nextState;
        newState->pool = this;
    }
    else
    {
        newState = new StackElement(shifted, nextState, prev, this);
    }
#ifdef TRACING
    LOG(MCdebugProgress, unknownJob, "%p: Push state %d symbol %d[%p] previous: %d[%p]", newState, nextState, shifted ? shifted->id : -1, shifted, prev ? prev->state : -1, prev);
    StringBuffer s;
    newState->getDebugText(s);
    s.newline();
    LOG(MCdebugProgress, unknownJob, s.str());
#endif
    return newState;
}

void LRParser::doReductions(LRActiveState & active, GrammarSymbol * next)
{
    //At the moment reduce can not remove items.
    //Done as a double loop to reduce calls to active.ordinality()
    unsigned max = active.elements.ordinality();
    for (unsigned i=0; i < max; )
    {
        for (; i < max; i++)
        {
            StackElement & cur = active.elements.item(i);
            active.markReduced(cur.state);
            reduce(cur, next);
        }
        max = active.elements.ordinality();
    }
    active.clearReduced();
}

void LRParser::doReductions(GrammarSymbol * next, bool singleToken)
{
    //The reduction need to go to a separate list - since may be different for each token
    //but want to optimize case where only a single token.
    LRActiveState * output = activeInput;
    if (!singleToken)
    {
        output = reduced;
        reduced->reinit();
    }

    activeOutput = output;
    curOverflow = reducedOverflow[0];
    doReductions(*activeInput, next);
    if (!singleToken)
        doReductions(*reduced, next);

    //You get weird situations where reductions add to states thathave already been reduced,
    //so any clashes like that need to be sent to other lists, processed and merged in.
    unsigned overflowIndex = 0;
    while (curOverflow->elements.ordinality())
    {
        LRActiveState * oldOverflow = curOverflow;
        overflowIndex = 1-overflowIndex;
        activeOutput = oldOverflow;
        curOverflow = reducedOverflow[overflowIndex];
        doReductions(*oldOverflow, next);
        output->mergeIn(*oldOverflow, chooseBest);
        oldOverflow->reinit();
    }

    curOverflow = NULL;
    activeOutput = NULL;
}

void LRParser::process(GrammarSymbol * next, bool singleToken)
{
#ifdef TRACING
    LOG(MCdebugProgress, unknownJob, "Process token '%.*s' %d at position %d", next->queryEndPtr()-next->queryStartPtr(), next->queryStartPtr(), next->id, next->queryStartPtr()-rowState.inputText);
#endif
    doReductions(next, singleToken);
    selectEndPosition((size32_t)(next->queryEndPtr()-rowState.inputText));
    doShifts(next, singleToken);
}


//Build up an array of the symbols to expand - may involve uncommoning nodes
void LRParser::expandReduction(StackElement & element, LRProduction * production, unsigned numSymbols)
{
    if (numSymbols == 0)
    {
        const byte * reducePtr = firstPosition+rowState.inputText;
        Owned<GrammarSymbol> reduced = production->reduce(reducedArgs, reducePtr, rowState);
        if (reduced)
        {
            state_id nextState = table.states[element.state]->getGoto(reduced->id);
#ifdef TRACING
            StringBuffer s;
            for (unsigned i = 0; i < production->getNumSymbols(); i++)
                s.appendf("%p ", reducedArgs[i]);
            LOG(MCdebugProgress, unknownJob, "Reduce by production %d new element %p[%s]", reduced->id, reduced.get(), s.str());
#endif
            //MORE: Some kind of recursion checking needed?
            StackElement * cached = activeOutput->cache[nextState];
            if (activeOutput->okToAddReduction(nextState))
                activeOutput->addElementOwn(createState(&element, nextState, reduced), chooseBest);
            else
                curOverflow->addElementOwn(createState(&element, nextState, reduced), chooseBest);
        }
        return;
    }

    StackElement * curElement = &element;
    do
    {
        reducedArgs[numSymbols-1] = curElement->shifted;
        expandReduction(*curElement->prev, production, numSymbols-1);
        curElement = curElement->sibling;
    } while (curElement);
}

LRActiveState * LRParser::getPosition(position_t pos)
{
    unsigned index = pos % MAX_POSITIONS;
    return positions[index];
}

void LRParser::setPositionOwn(position_t pos, LRActiveState * value)
{
    unsigned index = pos % MAX_POSITIONS;
    positions[index] = value;
}

void LRParser::reduce(StackElement & element, GrammarSymbol * next)
{
    LRState & curState = *table.states[element.state];
    symbol_id nextId = next->id;
    unsigned numReductions = curState.numReductions(nextId);
    for (unsigned i = 0; i < numReductions; i++)
    {
        unsigned productionIndex = curState.queryReduction(nextId, i);
        LRProduction * production = &table.productions[productionIndex];
        expandReduction(element, production, production->getNumSymbols());
    }
}


void LRParser::doShifts(LRActiveState * active, GrammarSymbol * next)
{
    symbol_id nextid = next->id;
    ForEachItemIn(idx, active->elements)
    {
        StackElement & cur = active->elements.item(idx);
        LRState & curState = *table.states[cur.state];
        state_id nextState = curState.getShift(nextid);
        if (nextState != NO_STATE)
        {
#ifdef TRACING
            LOG(MCdebugProgress, unknownJob, "Shift to state %d", nextState);
#endif
            activeOutput->addElementOwn(createState(&cur, nextState, next), chooseBest);
        }
        if (curState.canAccept(nextid))
        {
            for (StackElement * accept = &cur; accept; accept = accept->sibling)
            {
                GrammarSymbol * sym = accept->shifted;
                accepted.append(*LINK(sym));
#ifdef TRACING
                LOG(MCdebugProgress, unknownJob, "Accepted %p[%p]", accept, sym);
#endif
            }
        }
    }
}


void LRParser::doShifts(GrammarSymbol * next, bool singleToken)
{
    doShifts(activeInput, next);
    if (!singleToken)
        doShifts(reduced, next);
}


position_t LRParser::getFirstPosition()
{
    return firstPosition;
}

void LRParser::removePosition(position_t pos)
{
    position_t cur = pos+1;
    cleanupPosition(pos);
    while (cur < endPosition)
    {
        LRActiveState * active = getPosition(cur);
        if (active && active->elements.ordinality())
            break;
        cur++;
    }

    if (pos == firstPosition)
    {
        firstPosition = cur;
        if (firstPosition == endPosition)
        {
            firstPosition = NotFound;
            endPosition = NotFound;
        }
    }
    else
    {
        assertex(!"This should work, but why is it happening???");
    }
}


void LRParser::selectEndPosition(unsigned offset)
{
    if (firstPosition == endPosition)
    {
        firstPosition = offset;
        endPosition = offset;
    }
    assertex(offset >= firstPosition && offset < firstPosition+MAX_POSITIONS);
    if (offset >= endPosition)
        endPosition = offset+1;

    if (!getPosition(offset))
        setPositionOwn(offset, new LRActiveState(table.numStates));

    activeOutput = getPosition(offset);
}

void LRParser::selectStartPosition(unsigned offset)
{
    assertex(offset >= firstPosition && offset < endPosition);
    activeInput = getPosition(offset);
}


//---------------------------------------------------------------------------
#if 0
void LRMultiParser::parse(IMultiTokenLexer & lexer)
{
    selectEndPosition(0);
    addStartState();

    unsigned eofId = lexer.getEofId();
    GrammarSymbolArray tokens;
    bool done = false;
    while (!done)
    {
        position_t nextPosition = getFirstPosition();
        if (nextPosition == NotFound)
            break;
        selectStartPosition(nextPosition);
        tokens.kill();
        if (lexer.next(nextPosition, tokens)) // doesn't return whitespace.
        {
            ForEachItemIn(idx, tokens)
            {
                GrammarSymbol & curToken = tokens.item(idx);
                
                process(&curToken, tokens.ordinality() == 1);
            }
            if (tokens.ordinality() == 1 && tokens.item(0).id == eofId)
                done = true;
        }
        removePosition(nextPosition);
    }
}
#endif

//---------------------------------------------------------------------------

TomitaResultIterator::TomitaResultIterator(const TomitaStateInformation & _rowState, TomitaAlgorithm * _def) : rowState(_rowState), results(_def->matchInfo)
{
    def = LINK(_def);
    matchFirst = (def->matchAction == INlpParseAlgorithm::NlpMatchFirst);
    singleMatchPerSymbol = def->chooseMin || def->chooseMax || def->chooseBest || matchFirst;
}

TomitaResultIterator::~TomitaResultIterator()
{
    results.kill();
    def->Release();
}

int compareStartPtr(CInterface * const * pLeft, CInterface * const * pRight)
{
    //try and make it different to improve sort performance, and so results are reproducible between windows and linux, but it can't be guaranteed.
    GrammarSymbol * left = static_cast<GrammarSymbol *>(*pLeft);
    GrammarSymbol * right = static_cast<GrammarSymbol *>(*pRight);
    int delta = (int)(left->queryStartPtr() - right->queryStartPtr());              // ok unless records can be 2Gb.
    if (delta != 0)
        return delta;
    delta = left->getPenalty() - right->getPenalty();
    if (delta)
        return delta;
    delta = left->getLength() - right->getLength();
    if (delta)
        return delta;
    return 0;
}

bool TomitaResultIterator::isBetter(const GrammarSymbol * left, const GrammarSymbol * right)
{
    if (def->chooseMin || def->chooseMax)
    {
        unsigned leftLength = left->getLength();
        unsigned rightLength = right->getLength();
        if (leftLength != rightLength)
        {
            if (def->chooseMin)
                return leftLength < rightLength;
            else if (def->chooseMax)
                return leftLength > rightLength;
        }
    }
    if (def->chooseBest)
    {
        int leftPenalty = left->getPenalty();
        int rightPenalty = right->getPenalty();
        return (leftPenalty < rightPenalty);
    }
    return false;
}

void TomitaResultIterator::reset(const GrammarSymbolArray & _values)
{
    values.kill();
    curIndex = NotFound;
    unsigned numValues = _values.ordinality();
    if (numValues == 0)
    {
        if (def->notMatchedOnly || def->notMatched)
        {
            //Add a dummy matched entry...
            values.append(*new Terminal(def->eofId, NULL, 0, NULL));
        }
        return;
    }

    if (def->notMatchedOnly)
        return;

    values.ensure(numValues);
    for (unsigned i = 0; i < numValues; i++)
        values.append(OLINK(_values.item(i)));

    if (def->scanAction != INlpParseAlgorithm::NlpScanWhole)
        values.sort(compareStartPtr);

    if (def->chooseMin || def->chooseMax || def->chooseBest)
    {
        GrammarSymbol * best = &values.item(0);
        for (unsigned j=1; j < numValues;)
        {
            GrammarSymbol & cur = values.item(j);
            if (cur.queryStartPtr() != best->queryStartPtr())
            {
                //Scan none: remove all entries that are at a different offset
                if (def->scanAction == INlpParseAlgorithm::NlpScanNone)
                {
                    values.popn(numValues-j);
                    break;
                }
                if (!def->singleChoicePerLine)
                {
                    if (def->scanAction == INlpParseAlgorithm::NlpScanNext)
                    {
                        if (cur.queryStartPtr() < best->queryEndPtr())
                        {
                            numValues--;
                            values.remove(j);
                            continue;
                        }
                    }
                    best = &cur;
                }
            }
            else
            {
                if (matchFirst)
                {
                    numValues--;
                    values.remove(j);
                    continue;
                }
            }

            if (best != &cur)
            {
                if (isBetter(&cur, best))
                {
                    values.zap(*best);
                    best = &cur;
                }
                else
                    values.remove(j);
                numValues--;
                //NB: j is now pointing at the next entry
            }
            else
                j++;
        }
    }
    else
    {
        GrammarSymbol * last = &values.item(0);
        for (unsigned j=1; j < numValues;)
        {
            GrammarSymbol & cur = values.item(j);
            if (cur.queryStartPtr() != last->queryStartPtr())
            {
                if (def->scanAction == INlpParseAlgorithm::NlpScanNone)
                {
                    //Scan none: remove all entries that are at a different offset
                    values.popn(numValues-j);
                    break;
                }
                else if ((def->scanAction == INlpParseAlgorithm::NlpScanNext) && 
                         (cur.queryStartPtr() < last->queryEndPtr()))
                {
                    numValues--;
                    values.remove(j);
                }
                else
                {
                    last = &cur;
                    j++;
                }
            }
            else
            {
                if (matchFirst)
                {
                    numValues--;
                    values.remove(j);
                }
                else
                {
                    if (def->scanAction == INlpParseAlgorithm::NlpScanNext)
                    {
                        if (cur.queryEndPtr() < last->queryEndPtr())
                            last = &cur;
                    }
                    j++;
                }
            }
        }
    }
}

bool TomitaResultIterator::first()
{
    curIndex = 0;
    if (!isValid())
        return false;

    firstChoice();
    return true;
}

void TomitaResultIterator::firstChoice()
{
    if (def->chooseBest)
        choice.selectBest(&values.item(curIndex));
    else
        choice.first(&values.item(curIndex));
}

bool TomitaResultIterator::next()
{
    if (!isValid())
        return false;

    if (!singleMatchPerSymbol && choice.next(&values.item(curIndex)))
        return true;
    curIndex++;
    if (!isValid())
        return false;
    
    firstChoice();
    return true;
}

void TomitaResultIterator::invalidate()
{
    curIndex = (unsigned)-1;
    values.kill();
}


bool TomitaResultIterator::isValid()
{
    return values.isItem(curIndex);
}

const void * TomitaResultIterator::getRow()
{
    Owned<IMatchWalker> walker = getWalker();
    results.extractResults(&values.item(curIndex), choice, rowState.inputText);
//  results.extractResults(walker, rowState.inputText);

    RtlDynamicRowBuilder rowBuilder(outputAllocator);
    unsigned newSize = rowState.action->onMatch(rowBuilder, rowState.row, &results, walker);
    assertex(newSize);
    return rowBuilder.finalizeRowClear(newSize);
}


//---------------------------------------------------------------------------

TomitaParser::TomitaParser(ICodeContext * ctx, TomitaAlgorithm * _def, unsigned _activityId, INlpHelper * _helper, IHThorParseArg * arg) 
: parser(_def->table, rowState), lexer(_def->tokenDfa, _def->skipDfa, _def->endTokenChars, _def->eofId), iter(rowState, _def)
{
    assertex(ctx);
    def = _def;
    helper = _helper;
    helperArg = arg;
    eofId = def->eofId;

    outputAllocator.setown(ctx->getRowAllocator(arg->queryOutputMeta(), _activityId));
    iter.setAllocator(outputAllocator);

    //Now ensure that allocators are created for each of the production call backs.
    for (unsigned i=0; i < def->numProductions(); i++)
    {
        IOutputMetaData * rs = arg->queryProductionMeta(i);
        if (rs && !rowState.queryAllocator(rs))
        {
            Owned<IEngineRowAllocator> allocator = ctx->getRowAllocator(rs, _activityId);
            if (allocator)
                rowState.addAllocator(*allocator);
        }
    }
}

bool TomitaParser::performMatch(IMatchedAction & action, const void * row, unsigned len, const void * data)
{
    rowState.row = row;
    rowState.lengthInputText = len;
    rowState.inputText = (const byte *)data;
    rowState.action = &action;
    rowState.inputFormat = def->inputFormat;
    rowState.helper = helper;
    rowState.helperArg = helperArg;

    lexer.setDocument(len, data);
    parser.beginParse(def->chooseBest);

    bool scanWhole = (def->scanAction == INlpParseAlgorithm::NlpScanWhole);
    loop
    {
        position_t nextPosition = parser.getFirstPosition();
        if (nextPosition == NotFound)
            break;

        tokens.kill();
        unsigned numTokens = lexer.next(nextPosition, tokens);

        parser.selectStartPosition(nextPosition);
        if (numTokens)
        {
            bool insertEnd = !scanWhole;
            bool isOnlyToken = false;
            if (numTokens == 1)
            {
                if (tokens.item(0).getId() == eofId)
                {
                    insertEnd = false;
                    isOnlyToken = true;
                }
                else
                    isOnlyToken = scanWhole;
            }

            for (unsigned idx=0; idx < numTokens;idx++)
            {
                GrammarSymbol & curToken = tokens.item(idx);
                
                parser.process(&curToken, isOnlyToken);
                if (!scanWhole)
                    // a bit nasty - end position is still set up after the process() call.
                    parser.addStartState();
            }

            if (insertEnd)
            {
                Owned<Terminal> eofToken = new Terminal(eofId, NULL, 0, (const byte *)data + nextPosition);
                parser.process(eofToken, false);
            }
        }
        else if (!scanWhole && (nextPosition != len))
        {
            Owned<Terminal> eofToken = new Terminal(eofId, NULL, 0, (const byte *)data + nextPosition);
            parser.process(eofToken, true);
            parser.selectEndPosition(nextPosition+1);
            parser.addStartState();
        }

        parser.removePosition(nextPosition);
        if (nextPosition == len)
            break;
    }

    parser.endParse();
    iter.reset(parser.queryAccepted());
    //MORE: Post filter lost of options.. including whole etc...
    return parser.numAccepted() != 0;
}

INlpResultIterator * TomitaParser::queryResultIter()
{
    return &iter;
}

void TomitaParser::reset()
{
    iter.invalidate();
}

#if 0
To be done to finish multi parser
 a) Save reductions on separate stack if >1 token at same position
 b) Use two states for the single token case, and swap between to avoid copy.

Implementing a multiple lex parser:

 1) Parser maintain an ordered list of current start positions.
     active[position-firstPosition] - use pointers into an array of states;  States are cached on a list.
 2) lex from the first position in the list
 3) pass end position to parser - selects where new nodes get placed.
 4) Reductions have to be placed on to a separate list for each token, since dependant.
 4) Parser has associative array on position[x].
    Items are processed from position x to position y

Notes:

Caching: 
  Can only prune a branch when shifting, which loses all elements simultaneously, so a singlely linked list suffices, 
  and solves link counting problems that a doubly linked list would have,

Local ambiguity packing:
  When shifting a value, if it gets commoned up, and one of the common branches shifts the same symbol, and the preceeding state for that branch is the same, then common up that node.

ToDo:

* Do some more planning re:
  o Augmented grammars
  o Generating the lexer. Especially what we do about unknown words/multiple possible matches.  [Other implictations if tokens do not necessarily lie on the same boundaries].
  o Representing penalties and probabilities.
  o Translating the regex syntax into parser input.
  o Conditional reductions - where how/do they occur? What arguments do they need?
  o Returning multiple rows from a single match?

* Parameterised patterns - how do they related to augmented grammars[do not], and what is needed to implement them?

* Design in detail the table generator
  o LR or LALR?
  o Pathological grammars e.g., S := | S | ...   -> reread and understand doc.  Can we cope?

* Use cases:
  o MAX and BEST()

* Misc
  Error if ": define()" is applied to a pattern
  MAX,MIN in regex implementation
  Stack problems with regex




#endif
