/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "thortalgo.ipp"
#include "thortparse.ipp"

IEngineRowAllocator * TomitaParserCallback::queryAllocator(IOutputMetaData * searchMeta) const
{
    ForEachItemIn(i, allocators)
    {
        IEngineRowAllocator & cur = allocators.item(i);
        if (cur.queryOutputMeta() == searchMeta)
            return &cur;
    }
    return NULL;
}

void TomitaStateInformation::set(const TomitaStateInformation & other)
{
    action = other.action;
    row = other.row;
    inputText = other.inputText;
    lengthInputText = other.lengthInputText;
    helper = other.helper;
    helperArg = other.helperArg;
}

//---------------------------------------------------------------------------

GrammarSymbol * TomitaMatchSearchInstance::findInChildren(GrammarSymbol * top, const TomitaMatchPath & path, unsigned depth)
{
    unsigned prevExactMatchDepth = lastExactMatchDepth;
    for (unsigned i = 0;; i++)
    {
        GrammarSymbol * child = top->queryChild(i);
        if (!child)
            return NULL;
        GrammarSymbol * ret = find(child, path, depth);
        if (prevExactMatchDepth != lastExactMatchDepth)
            return ret;
    }
    return NULL;
}

GrammarSymbol * TomitaMatchSearchInstance::find(GrammarSymbol * top, const TomitaMatchPath & path, unsigned depth)
{
    if (top->isPacked())
        top = top->queryPacked(choices->getInstance(top));

    regexid_t id = path.getId(depth);
    if (top->getId() == id)
    {
        bool matchAny = path.matchAny(depth);
        if (matchAny || (nextIndex == 1))
        {
            if (depth+1 == path.numItems())
            {
                lastExactMatchDepth = depth+1;
                return top;
            }

            if (!matchAny)
            {
                lastExactMatchDepth = depth+1;
                nextIndex = path.nextExactMatchIndex(depth+1);
            }

            return findInChildren(top, path, depth+1);
        }
        else
        {
            nextIndex--;
            return NULL;
        }
    }
    else
        return findInChildren(top, path, depth);
}

IMatchedElement * TomitaMatchPath::getMatch(GrammarSymbol * top, PackedSymbolChoice & choice) const
{
    TomitaMatchSearchInstance search;
    search.lastExactMatchDepth = 0;
    search.nextIndex = nextExactMatchIndex(0);
    search.choices = &choice;
    GrammarSymbol * state = search.find(top, *this, 0);
    if (!state)
        return NULL;
    return LINK(state);
}


void CTomitaMatchedResults::extractResults(GrammarSymbol * top, PackedSymbolChoice & choice, const byte * _in)
{
    in = _in;
    notMatched.ptr = in;
    ForEachItemIn(idx, def->matchResults)
    {
        ::Release(matched[idx]);
        matched[idx] = ((TomitaMatchPath &)def->matchResults.item(idx)).getMatch(top, choice);
        if (!matched[idx]) matched[idx] = LINK(&notMatched);
    }
    rootResult = top->queryResultRow();
}


//---------------------------------------------------------------------------

FeatureInfo::FeatureInfo()
{
    numMask = 0;
    numFlat = 0;
    defaults = NULL;
}

FeatureInfo::~FeatureInfo()
{
    free(defaults);
}

void FeatureInfo::deserialize(MemoryBuffer & in)
{
    in.read(numMask);
    in.read(numFlat);
    unsigned size = getSize();
    defaults = malloc(size);
    in.read(size, defaults);
}

void FeatureInfo::serialize(MemoryBuffer & out)
{
    out.append(numMask);
    out.append(numFlat);
    out.append(getSize(), defaults);
}

//---------------------------------------------------------------------------

FeatureAction::FeatureAction()
{
    featureKind = 0;;
    srcSymbol = 0;
    srcFeatureIndex = 0;
    tgtFeatureIndex = 0;
}

void FeatureAction::deserialize(MemoryBuffer & in)
{
    in.read(featureKind).read(srcSymbol).read(srcFeatureIndex).read(tgtFeatureIndex);
}

void FeatureAction::serialize(MemoryBuffer & out)
{
    out.append(featureKind).append(srcSymbol).append(srcFeatureIndex).append(tgtFeatureIndex);
}

void ProductionFeatureInfo::deserialize(MemoryBuffer & in)
{
    result.deserialize(in);
    extra.deserialize(in);
    unsigned numActions;
    in.read(numActions);
    for (unsigned i = 0; i < numActions; numActions++)
    {
        FeatureAction & cur = * new FeatureAction;
        cur.deserialize(in);
        actions.append(cur);
    }
}

void ProductionFeatureInfo::serialize(MemoryBuffer & out)
{
    result.serialize(out);
    extra.serialize(out);
    unsigned numActions = actions.ordinality();
    out.append(numActions);
    ForEachItemIn(i, actions)
        actions.item(i).serialize(out);
}

//---------------------------------------------------------------------------

inline bool isMaskValid(mask_feature_t & value, const mask_feature_t test)
{
    return (value &= test) != 0;
}

inline bool isFlatValid(flat_feature_t & value, const flat_feature_t test)
{
    if (value == UNKNOWN_FEATURE)
        value = test;
    else if ((value != test) && (test != UNKNOWN_FEATURE))
        return false;
    return true;
}

bool mergeFeatures(FeatureValue & result, const ProductionFeatureInfo * info, GrammarSymbol * * symbols)
{
    if (info->totalFeatures() == 0)
        return true;

    //Features come in two types - masks and flat.  For masks we perform intersection, flat must be equal.
    //A production may have associated features (in result), and may also have features checked within the production (extra)
    mask_feature_t * resultMaskFeature = NULL;
    flat_feature_t * resultFlatFeature = NULL;
    if (info->result.getNum() != 0)
    {
        unsigned size = info->result.getSize();
        void * resultFeatures = result.values.allocate(size);
        memcpy(resultFeatures, info->result.defaults, size);

        resultMaskFeature = (mask_feature_t *)resultFeatures;
        resultFlatFeature = (flat_feature_t *)(resultMaskFeature + info->result.numMask);
    }

    mask_feature_t * extraMaskFeature = NULL;
    flat_feature_t * extraFlatFeature = NULL;
    MemoryAttr featuresTemp;
    if (info->extra.getNum() != 0)
    {
        unsigned size = info->extra.getSize();
        void * extraFeatures = featuresTemp.allocate(size);
        memcpy(extraFeatures, info->extra.defaults, size);

        extraMaskFeature = (mask_feature_t *)extraFeatures;
        extraFlatFeature = (flat_feature_t *)(extraMaskFeature + info->extra.numMask);
    }

    ForEachItemIn(i, info->actions)
    {
        const FeatureAction & cur = info->actions.item(i);
        const GrammarSymbol * srcSymbol = symbols[cur.srcSymbol];
        const FeatureValue & src = srcSymbol->features;
        const mask_feature_t * srcMaskValues = (const mask_feature_t *)src.values.get();
        unsigned tgtFeatureIndex = cur.tgtFeatureIndex;
        switch (cur.featureKind)
        {
        case FKmask:
            {
                mask_feature_t srcValue = srcMaskValues[cur.srcFeatureIndex];
                if (!isMaskValid(resultMaskFeature[tgtFeatureIndex], srcValue)) 
                    return false;
                break;
            }
        case FKflat:
            {
                unsigned numSrcMask = srcSymbol->features.info->numMask;
                flat_feature_t * srcFlatValues = (flat_feature_t *)(srcMaskValues + numSrcMask);
                flat_feature_t srcValue = srcFlatValues[cur.srcFeatureIndex];
                if (!isFlatValid(resultFlatFeature[tgtFeatureIndex], srcValue))
                    return false;
                break;
            }
        case FKemask:
            {
                mask_feature_t srcValue = srcMaskValues[cur.srcFeatureIndex];
                if (!isMaskValid(extraMaskFeature[tgtFeatureIndex], srcValue)) 
                    return false;
                break;
            }
        case FKeflat:
            {
                unsigned numSrcMask = srcSymbol->features.info->numMask;
                flat_feature_t * srcFlatValues = (flat_feature_t *)(srcMaskValues + numSrcMask);
                flat_feature_t srcValue = srcFlatValues[cur.srcFeatureIndex];
                if (!isFlatValid(extraFlatFeature[tgtFeatureIndex], srcValue))
                    return false;
                break;
            }
        }
    }
    result.info = &info->result;
    return true;
}

//---------------------------------------------------------------------------
LRValidator::LRValidator()
{
    kind = LRVnone;
    dfa = NULL;
}

LRValidator::~LRValidator()
{
    delete dfa;
}

void LRValidator::deserialize(MemoryBuffer & in)
{
    in.read(kind);
    switch (kind)
    {
    case LRVnone:
    case LRVfirst:
    case LRVlast:
        break;
    case LRVvalidateasc:
    case LRVvalidateuni:
        in.read(validatorIndex);
        break;
    case LRVchecklength:
        in.read(minExpectedBytes).read(maxExpectedBytes);
        break;
    case LRVcheckin:
    case LRVbefore:
        dfa = new AsciiDfa;
        dfa->deserialize(in);
        break;
    case LRVafter:
        in.read(minExpectedBytes).read(maxExpectedBytes);
        dfa = new AsciiDfa;
        dfa->deserialize(in);
        break;
    default:
        UNIMPLEMENTED;
    }
}

void LRValidator::serialize(MemoryBuffer & out)
{
    out.append(kind);
    switch (kind)
    {
    case LRVnone:
    case LRVfirst:
    case LRVlast:
        break;
    case LRVvalidateasc:
    case LRVvalidateuni:
        out.append(validatorIndex);
        break;
    case LRVchecklength:
        out.append(minExpectedBytes).append(maxExpectedBytes);
        break;
    case LRVcheckin:
    case LRVbefore:
        dfa->serialize(out);
        break;
    case LRVafter:
        out.append(minExpectedBytes).append(maxExpectedBytes);
        dfa->serialize(out);
        break;
    default:
        UNIMPLEMENTED;
    }
}

StringBuffer & LRValidator::trace(StringBuffer & out)
{
    switch (kind)
    {
    case LRVfirst:
        return out.append(" valid(first)");
    case LRVlast:
        return out.append(" valid(last)");
    case LRVvalidateasc:
        return out.appendf(" valid(asc:%d)", validatorIndex);
    case LRVvalidateuni:
        return out.appendf(" valid(uni:%d)", validatorIndex);
    case LRVchecklength:
        return out.appendf(" valid(length:%d..%d)", minExpectedBytes, maxExpectedBytes);
    case LRVcheckin:
        return out.append(" valid(in)");
    case LRVbefore:
        return out.append(" valid(before)");
    case LRVafter:
        return out.appendf(" valid(after:%d..%d)", minExpectedBytes, maxExpectedBytes);
    }
    return out;
}

bool LRValidator::isValid(unsigned numSymbols, GrammarSymbol * * symbols, const byte * reducePtr, const TomitaParserCallback & state)
{
    switch (kind)
    {
    case LRVnone:
        return true;
    case LRVfirst:
        if (numSymbols != 0)
            return (symbols[0]->queryStartPtr() == state.inputText);
        return (reducePtr == state.inputText);
    case LRVlast:
        if (numSymbols != 0)
            return (symbols[0]->queryEndPtr() == state.inputText+state.lengthInputText);
        return (reducePtr == state.inputText+state.lengthInputText);
    case LRVvalidateasc:
        {
            IStringValidator * validator = (IStringValidator *)state.helper->queryValidator(validatorIndex);
            unsigned len = 0;
            const byte * start = NULL;
            if (numSymbols)
            {
                start = symbols[0]->queryStartPtr();
                len = (size32_t)(symbols[numSymbols-1]->queryEndPtr() - start);
            }

            if (state.inputFormat == NlpAscii)
                return validator->isValid(len, (const char *)start);

            unsigned asciiLen;
            char * asciiText;
            if (state.inputFormat == NlpUnicode)
                rtlUnicodeToStrX(asciiLen, asciiText, len/sizeof(UChar), (const UChar *)start);
            else
                rtlUtf8ToStrX(asciiLen, asciiText, rtlUtf8Length(len, start), (const char *)start);
            bool ok = validator->isValid(asciiLen, asciiText);
            rtlFree(asciiText);
            return ok;
        }
    case LRVvalidateuni:
        {
            IUnicodeValidator * validator = (IUnicodeValidator *)state.helper->queryValidator(validatorIndex);
            unsigned len = 0;
            const byte * start = NULL;
            if (numSymbols)
            {
                start = symbols[0]->queryStartPtr();
                len = (size32_t)(symbols[numSymbols-1]->queryEndPtr() - start);
            }

            if (state.inputFormat == NlpUnicode)
                return validator->isValid(len/sizeof(UChar), (const UChar *)start);

            unsigned unicodeLen;
            UChar * unicodeText;
            if (state.inputFormat == NlpAscii)
                rtlStrToUnicodeX(unicodeLen, unicodeText, len, (const char *)start);
            else
                rtlUtf8ToUnicodeX(unicodeLen, unicodeText, rtlUtf8Length(len, start), (const char *)start);

            bool ok = validator->isValid(unicodeLen, unicodeText);
            rtlFree(unicodeText);
            return ok;
        }
    case LRVchecklength:
        {
            unsigned len = 0;
            if (numSymbols)
            {
                const byte * start = symbols[0]->queryStartPtr();
                len = (size32_t)(symbols[numSymbols-1]->queryEndPtr() - start);
                if (state.inputFormat == NlpUtf8)
                    len = rtlUtf8Length(len, start);
            }
            return (len >= minExpectedBytes) && (len <= maxExpectedBytes);
        }
    case LRVcheckin:
        {
            unsigned len = 0;
            const byte * start = NULL;
            if (numSymbols)
            {
                start = symbols[0]->queryStartPtr();
                len = (size32_t)(symbols[numSymbols-1]->queryEndPtr() - start);
            }
            return (getMaximumMatchLength(*dfa, len, start) == len);
        }
    case LRVbefore:
        {
            const byte * nextPtr = reducePtr;
            if (numSymbols)
                nextPtr = symbols[numSymbols-1]->queryEndPtr();
            const byte * startInputText = state.inputText;
            const byte * endInputText = startInputText + state.lengthInputText;
            return (getMaximumMatchLength(*dfa, (size32_t)(endInputText - nextPtr), nextPtr) != NotFound);
        }
    case LRVafter:
        {
            const byte * startPtr = reducePtr;
            if (numSymbols)
                startPtr = symbols[0]->queryStartPtr();

            const byte * startInputText = state.inputText;
            const byte * firstPtr = (startPtr >= startInputText + maxExpectedBytes) ? startPtr - maxExpectedBytes : startInputText;
            const byte * lastPtr = (startPtr >= startInputText + minExpectedBytes) ? startPtr - minExpectedBytes : startInputText;
            for (const byte * ptr = firstPtr; ptr <= lastPtr; ptr++)
            {
                if (getMaximumMatchLength(*dfa, (size32_t)(startPtr-ptr), ptr) != NotFound)
                    return true;
            }
            return false;
        }
    default:
        UNIMPLEMENTED;
    }
}

//---------------------------------------------------------------------------

class ProductionCallback : public CInterface, implements IProductionCallback
{
public:
    ProductionCallback(const TomitaParserCallback & _state, unsigned _numSymbols, GrammarSymbol * * _symbols) : state(_state)
    {
        numSymbols = _numSymbols;
        symbols = _symbols;
    }

    virtual void getText(size32_t & outlen, char * & out, unsigned idx)
    {
        assertex(idx < numSymbols);
        const GrammarSymbol * cur = symbols[idx];
        const byte * start = cur->queryStartPtr(); 
        size32_t size = (size32_t)(cur->queryEndPtr() - start); 

        switch (state.inputFormat)
        {
        case NlpAscii:
            rtlStrToStrX(outlen, out, size, start);
            break;
        case NlpUtf8:
            {
                unsigned len = rtlUtf8Length(size, start);
                rtlUtf8ToStrX(outlen, out, len, (const char *)start);
                break;
            }
        case NlpUnicode:
            rtlUnicodeToStrX(outlen, out, size/sizeof(UChar), (const UChar *)start);
            break;
        }
    }

    virtual void getUnicode(size32_t & outlen, UChar * & out, unsigned idx)
    {
        assertex(idx < numSymbols);
        const GrammarSymbol * cur = symbols[idx];
        const byte * start = cur->queryStartPtr(); 
        size32_t size = (size32_t)(cur->queryEndPtr() - start); 

        switch (state.inputFormat)
        {
        case NlpAscii:
            rtlStrToUnicodeX(outlen, out, size, (const char *)start);
            break;
        case NlpUtf8:
            {
                unsigned len = rtlUtf8Length(size, start);
                rtlUtf8ToUnicodeX(outlen, out, len, (const char *)start);
                break;
            }
        case NlpUnicode:
            rtlUnicodeToUnicodeX(outlen, out, size/sizeof(UChar), (const UChar*)start);
            break;
        }
    }

    virtual void getUtf8(size32_t & outlen, char * & out, unsigned idx)
    {
        assertex(idx < numSymbols);
        const GrammarSymbol * cur = symbols[idx];
        const byte * start = cur->queryStartPtr(); 
        size32_t size = (size32_t)(cur->queryEndPtr() - start); 

        switch (state.inputFormat)
        {
        case NlpAscii:
            rtlStrToUtf8X(outlen, out, size, (const char *)start);
            break;
        case NlpUtf8:
            {
                unsigned len = rtlUtf8Length(size, start);
                rtlUtf8ToUtf8X(outlen, out, len, (const char*)start);
                break;
            }
        case NlpUnicode:
            rtlUnicodeToUtf8X(outlen, out, size/sizeof(UChar), (const UChar*)start);
            break;
        }
    }

    virtual byte * queryResult(unsigned idx)
    {
        assertex(idx < numSymbols);
        return symbols[idx]->queryResultRow();
    }

protected:
    const TomitaParserCallback & state;
    unsigned numSymbols;
    GrammarSymbol * * symbols;
};


//---------------------------------------------------------------------------

LRProduction::LRProduction()
{
    prodId = NotFound;
    ruleId = NotFound;
    numSymbols = 0;
    ruleName = NULL;
    transformClonesFirstSymbol = false;
}

void LRProduction::deserialize(unsigned _prodId, MemoryBuffer & in)
{
    prodId = _prodId;
    StringAttr temp;
    unsigned numSymbolsMask;
    in.read(ruleId).read(numSymbolsMask).read(penalty);
    numSymbols = (unsigned short)numSymbolsMask;
    transformClonesFirstSymbol = (numSymbolsMask & NSFclonesFirstSymbol) != 0;
    ::deserialize(in, temp);
    ruleName = createAtom(temp);
    feature.deserialize(in);
    validator.deserialize(in);
}

void LRProduction::serialize(MemoryBuffer & out)
{
    unsigned numSymbolsMask = numSymbols;
    if (transformClonesFirstSymbol)
        numSymbolsMask |= NSFclonesFirstSymbol;
    out.append(ruleId).append(numSymbolsMask).append(penalty);
    ::serialize(out, ruleName->str());
    feature.serialize(out);
    validator.serialize(out);
}

void LRProduction::setMetaData(IOutputMetaData * size)
{
    resultSize.set(size);
}

GrammarSymbol * LRProduction::reduce(GrammarSymbol * * symbols, const byte * reducePtr, const TomitaParserCallback & state)
{
    FeatureValue resultFeatures;
    //check whether guard conditions are met.
    if (!mergeFeatures(resultFeatures, &feature, symbols))
        return NULL;

    //Is the user 
    if (!validator.isValid(numSymbols, symbols, reducePtr, state))
        return NULL;

    size32_t rowSize = 0;
    const void * rowData = NULL;
    if (resultSize)
    {
        if (transformClonesFirstSymbol)
        {
            //Need to clone from the first (and only) symbol that has an associated row
            GrammarSymbol * match = NULL;
            for (unsigned i = 0; i < numSymbols; i++)
            {
                GrammarSymbol * cur = symbols[i];
                if (cur->queryResultRow())
                {
                    assertex(!match);
                    match = cur;
                }
            }

            rowSize = match->queryResultSize();

            //Avoid cloning the transform information
            void * matchRow = match->queryResultRow();
            assertex(matchRow);
            rowData = rtlLinkRow(matchRow);
        }
        else
        {
            IEngineRowAllocator * allocator = state.queryAllocator(resultSize);

            ProductionCallback callback(state, numSymbols, symbols);
            RtlDynamicRowBuilder rowBuilder(allocator);
            try
            {
                rowSize = state.helperArg->executeProduction(rowBuilder, prodId, &callback);
            }
            catch (...)
            {
                allocator->releaseRow(rowData);
                throw;
            }

            if (rowSize == 0)
            {
                //more: executeProduction could throw an exception
                return NULL;
            }

            rowData = rowBuilder.finalizeRowClear(rowSize);
        }
    }

    GrammarSymbol * ret = new NonTerminal(ruleId, ruleName, resultFeatures, numSymbols, symbols, reducePtr, rowSize, (byte *)rowData);
    ret->addPenalty(penalty);
    return ret;
}

StringBuffer & LRProduction::trace(StringBuffer & out, unsigned id)
{
    out.appendf("\t[%d]   rule:%d pop:%d", id, ruleId, numSymbols);
    validator.trace(out);
    return out.newline();
}


//---------------------------------------------------------------------------

StringBuffer & LRAction::trace(StringBuffer & out)
{
    switch (getAction())
    {
    case AcceptAction:  return out.append("A");
    case ShiftAction:   return out.append("S").append(getExtra());
    case ReduceAction:  return out.append("R").append(getExtra());
    default:
        UNIMPLEMENTED;
    }
}

//---------------------------------------------------------------------------

LRState::LRState(LRTable * _table, LRAction * _actions, unsigned * _gotos)
{
    table = _table;
    actions = _actions;
    gotos = _gotos;
}

LRState::~LRState()
{
}

bool LRState::canAccept(token_id sym) const
{
    LRAction & cur = actions[sym];
    ActionKind action = cur.getAction();
    if (action == MultiAction)
    {
        LRAction * multi = table->extraActions + cur.getExtra();
        loop
        {
            action = multi->getAction();
            if (action == NoAction)
                return false;
            else if (action == AcceptAction)
                return true;
            multi++;
        }
    }
    return action == AcceptAction;
}

state_id LRState::getGoto(symbol_id sym) const
{
    return gotos[sym-table->numTokens];
}

state_id LRState::getShift(token_id sym) const
{
    LRAction & cur = actions[sym];
    ActionKind action = cur.getAction();
    if (action == MultiAction)
    {
        LRAction * multi = table->extraActions + cur.getExtra();
        loop
        {
            action = multi->getAction();
            if (action == NoAction)
                return false;
            else if (action == ShiftAction)
                return multi->getExtra();
            multi++;
        }
    }
    if (action == ShiftAction)
        return cur.getExtra();
    return NotFound;
}

unsigned LRState::numReductions(token_id sym) const
{
    LRAction & cur = actions[sym];
    ActionKind action = cur.getAction();
    if (action == MultiAction)
    {
        unsigned count = 0;
        LRAction * multi = table->extraActions + cur.getExtra();
        loop
        {
            action = multi->getAction();
            if (action == NoAction)
                return count;
            else if (action == ReduceAction)
                count++;
            multi++;
        }
    }
    if (action == ReduceAction)
        return 1;
    return 0;
}

unsigned LRState::queryReduction(token_id sym, unsigned idx) const
{
    LRAction & cur = actions[sym];
    ActionKind action = cur.getAction();
    if (action == MultiAction)
    {
        LRAction * multi = table->extraActions + cur.getExtra();
        loop
        {
            action = multi->getAction();
            if (action == NoAction)
                return NotFound;
            else if (action == ReduceAction)
            {
                if (idx == 0)
                    return multi->getExtra();
                idx--;
            }
            multi++;
        }
    }
    if ((action == ReduceAction) && (idx == 0))
        return cur.getExtra();
    return NotFound;
}

StringBuffer & LRState::trace(StringBuffer & out, unsigned id) const
{
    out.appendf("\t[%d]\t", id);
    if (id < 10) out.append('\t');
#if 1
    for (unsigned i=0; i < table->numTokens; i++)
    {
        LRAction & cur = actions[i];
        ActionKind action = cur.getAction();
        if (action != NoAction)
        {
            if (action == MultiAction)
            {
                out.append("{");
                LRAction * multi = table->extraActions + cur.getExtra();
                loop
                {
                    action = multi->getAction();
                    if (action == NoAction)
                        break;
                    multi->trace(out);
                    multi++;
                }
                out.append("}");
            }
            else
                cur.trace(out);
            out.append("\t");
        }
        else
            out.append("\t");
    }
    out.append("\tGoto:  ");
    for (unsigned j = table->numTokens; j < table->numSymbols; j++)
    {
        if (gotos[j-table->numTokens])
            out.append(j).append("->").append(gotos[j-table->numTokens]).append(" ");
    }
#else
    for (unsigned i=0; i < table->numTokens; i++)
    {
        LRAction & cur = actions[i];
        ActionKind action = cur.getAction();
        if (action != NoAction)
        {
            out.append(i).append(":");
            if (action == MultiAction)
            {
                out.append("{");
                LRAction * multi = table->extraActions + cur.getExtra();
                loop
                {
                    action = multi->getAction();
                    if (action == NoAction)
                        break;
                    multi->trace(out);
                    multi++;
                }
                out.append("}");
            }
            else
                cur.trace(out);
            out.append(" ");
        }
    }
    out.newline();
    out.append("\t\t\tGoto:  ");
    for (unsigned j = table->numTokens; j < table->numSymbols; j++)
    {
        if (gotos[j-table->numTokens])
            out.append(j).append("->").append(gotos[j-table->numTokens]).append(" ");
    }
#endif
    return out.newline();
}

//---------------------------------------------------------------------------

LRTable::LRTable()
{
    allActions = NULL;
    extraActions = NULL;
    allGotos = NULL;
    productions = NULL;
    states = NULL;
    rootState = NotFound;
    numStates = 0;
    numProductions = 0;
    numExtraActions = 0;
    numSymbols = 0;
    numTokens = 0;
}


LRTable::~LRTable()
{
    for (unsigned i=0; i < numStates; i++)
        delete states[i];
    delete [] allActions;
    delete [] extraActions;
    delete [] allGotos;
    delete [] productions;
    delete [] states;
}


void LRTable::alloc()
{
    unsigned numNonTerminals = (numSymbols-numTokens);

    allActions = new LRAction[numStates * numTokens];
    extraActions = numExtraActions ? new LRAction[numExtraActions] : NULL;
    allGotos = new unsigned[numStates * numNonTerminals];
    memset(allGotos, 0, sizeof(unsigned) * numStates * numNonTerminals);
    productions = new LRProduction[numProductions];
    states = new LRState * [numStates];
    for (unsigned i=0; i < numStates; i++)
        states[i] = new LRState(this, &allActions[i*numTokens], &allGotos[i*numNonTerminals]);
}

void LRTable::deserialize(MemoryBuffer & in)
{
    in.read(numStates).read(numTokens).read(numSymbols).read(numProductions).read(numExtraActions);
    in.read(rootState);
    alloc();

    unsigned numNonTerminals = (numSymbols-numTokens);
    in.read(numStates*numTokens*sizeof(*allActions), allActions);
    in.read(numExtraActions*sizeof(*extraActions), extraActions);
    in.read(numStates*numNonTerminals*sizeof(*allGotos), allGotos);
    for (unsigned j=0; j < numProductions; j++)
        productions[j].deserialize(j, in);
}


void LRTable::serialize(MemoryBuffer & out)
{
    out.append(numStates).append(numTokens).append(numSymbols).append(numProductions).append(numExtraActions);
    out.append(rootState);

    unsigned numNonTerminals = (numSymbols-numTokens);
    out.append(numStates*numTokens*sizeof(*allActions), allActions);
    out.append(numExtraActions*sizeof(*extraActions), extraActions);
    out.append(numStates*numNonTerminals*sizeof(*allGotos), allGotos);
    for (unsigned j=0; j < numProductions; j++)
        productions[j].serialize(out);
}

StringBuffer & LRTable::trace(StringBuffer & out)
{
    out.append("States:\n");
    out.appendf("\tRoot=%d",rootState).newline();
    for (unsigned i = 0; i < numStates; i++)
        states[i]->trace(out, i);
    out.append("Productions:\n");
    for (unsigned j = 0; j < numProductions; j++)
        productions[j].trace(out, j);
    return out;
}

//---------------------------------------------------------------------------
//-- LRTableBuilder --

static int compareActions(CInterface * * _left, CInterface * * _right)
{
    LRActionItem * left = static_cast<LRActionItem *>(*_left);
    LRActionItem * right = static_cast<LRActionItem *>(*_right);
    if (left->id < right->id) return -1;
    if (left->id > right->id) return +1;
    if (left->action.getValue() < right->action.getValue()) return -1;
    if (left->action.getValue() > right->action.getValue()) return +1;
    return 0;
}

LRTableBuilder::LRTableBuilder(LRTable & _table) : table(_table)
{
    curState = NULL;
    ambiguous = false;
}

LRTableBuilder::~LRTableBuilder()
{
}

void LRTableBuilder::init(unsigned _numStates, unsigned _numTokens, unsigned _numSymbols, unsigned _numProductions)
{
    table.numStates = _numStates;
    table.numTokens = _numTokens;
    table.numSymbols = _numSymbols;
    table.numProductions = _numProductions;
    table.numExtraActions = 0;
    table.alloc();
}


void LRTableBuilder::addAccept(token_id id)
{
    assertex(id < table.numTokens); 
    actions.append(* new LRActionItem(id, AcceptAction, 0));
}

void LRTableBuilder::addShift(token_id id, unsigned newState)
{
    assertex(id < table.numTokens); 
    actions.append(* new LRActionItem(id, ShiftAction, newState));
}

void LRTableBuilder::addGoto(symbol_id id, unsigned newState)
{
    assertex(id >= table.numTokens && id < table.numSymbols); 
    curState->gotos[id - table.numTokens] = newState;
}

void LRTableBuilder::addProduction(unsigned id, unsigned ruleId, IAtom * ruleName, unsigned numToPop, int penalty, bool transformClonesFirstSymbol)
{
    assertex(id < table.numProductions); 
    LRProduction & cur = table.productions[id];
    cur.prodId = id;
    cur.numSymbols = numToPop;
    cur.ruleId = ruleId;
    cur.ruleName = ruleName;
    cur.penalty = penalty;
    cur.transformClonesFirstSymbol = transformClonesFirstSymbol;
}


void LRTableBuilder::addValidator(unsigned prodId, byte kind, unsigned low, unsigned high, AsciiDfa * dfa)
{
    LRProduction & cur = table.productions[prodId];
    LRValidator & validator = cur.validator;
    assertex(validator.kind == LRVnone);
    validator.kind = kind;
    validator.minExpectedBytes = low;
    validator.maxExpectedBytes = high;
    validator.dfa = dfa;
    validator.validatorIndex = low;
}


void LRTableBuilder::addReduce(token_id id, unsigned prod)
{
    assertex(id < table.numTokens && prod < table.numProductions); 
    actions.append(* new LRActionItem(id, ReduceAction, prod));
}

void LRTableBuilder::beginState(unsigned id)
{
    assertex(!curState);
    curState = table.states[id];
}

void LRTableBuilder::endState()
{
    assertex(curState);
    actions.sort(compareActions);

    removeDuplicateShiftStates();

    unsigned max = actions.ordinality();
    for (unsigned i = 0; i < max; i++)
    {
        LRActionItem & cur = actions.item(i);
        unsigned j;
        for (j = i+1; j < max; j++)
        {
            if (actions.item(j).id != cur.id)
                break;
        }

        if (j - i > 1)
        {
            //Multiple entries.
            curState->actions[cur.id].set(MultiAction, extraActions.ordinality());
            for (unsigned k=i; k < j; k++)
                extraActions.append(actions.item(k).action.getValue());
            extraActions.append(0);
            i = j-1;
            ambiguous = true;
        }
        else
            curState->actions[cur.id].setValue(cur.action.getValue());
    }

    actions.kill();
    curState = NULL;
}

void LRTableBuilder::finished(unsigned rootId)
{
    assertex(sizeof(LRAction) == sizeof(unsigned));
    table.rootState = rootId;
    table.numExtraActions = extraActions.ordinality();
    table.extraActions = new LRAction[table.numExtraActions];
    memcpy(table.extraActions, extraActions.getArray(), sizeof(unsigned) * table.numExtraActions);
}


void LRTableBuilder::removeDuplicateShiftStates()
{
    //Easiest way of handling duplicate shift states is to remove them in post-processing.
    unsigned max = actions.ordinality();
    if (max <= 1)
        return;

    for (unsigned i=max-1; i--; )
    {
        LRActionItem & cur = actions.item(i);
        if (cur.action.getAction() == ShiftAction)
        {
            LRActionItem & next = actions.item(i+1);
            if (cur.id == next.id && next.action.getAction() == ShiftAction)
            {
                assertex(cur.action.getValue() == next.action.getValue());
                actions.remove(i+1);
            }
        }
    }
}


//---------------------------------------------------------------------------

TomitaAlgorithm::TomitaAlgorithm(IRecordSize * _outRecordSize) : NlpAlgorithm(new CTomitaMatchedResultInfo)
{
    outRecordSize.set(_outRecordSize);
}

TomitaAlgorithm::~TomitaAlgorithm()
{
}

void TomitaAlgorithm::init(IHThorParseArg & arg)
{
    for (unsigned i=0; i < table.numProductions; i++)
    {
        IOutputMetaData * rs = arg.queryProductionMeta(i);
        if (rs)
        {
            LRProduction & production = table.productions[i];
            production.setMetaData(rs);
        }
    }
}

void TomitaAlgorithm::serialize(MemoryBuffer & out)
{
    out.append((byte)NLPAtomita);
    NlpAlgorithm::serialize(out);
    table.serialize(out);
    tokenDfa.serialize(out);
    skipDfa.serialize(out);
    out.append(eofId);

    unsigned num = endTokenChars.ordinality();
    out.append(num);
    for (unsigned i=0; i < num; i++)
        out.append(endTokenChars.item(i));
}

void TomitaAlgorithm::deserialize(MemoryBuffer & in)
{
    NlpAlgorithm::deserialize(in);
    table.deserialize(in);
    tokenDfa.deserialize(in);
    skipDfa.deserialize(in);
    in.read(eofId);

    unsigned num;
    in.read(num);
    for (unsigned i=0; i < num; i++)
    {
        unsigned temp;
        in.read(temp);
        endTokenChars.append(temp);
    }
}



INlpParser * TomitaAlgorithm::createParser(ICodeContext * ctx, unsigned activityId, INlpHelper * helper, IHThorParseArg * arg)
{ 
    return new TomitaParser(ctx, this, activityId, helper, arg); 
}

INlpParseAlgorithm * createTomitaParser(MemoryBuffer & buffer, IOutputMetaData * outRecordSize)
{
    TomitaAlgorithm * ret = new TomitaAlgorithm(outRecordSize);
    ret->deserialize(buffer);
    return ret;
}
