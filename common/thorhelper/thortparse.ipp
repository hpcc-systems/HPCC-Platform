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

#ifndef __THORTPARSE_IPP_
#define __THORTPARSE_IPP_

#include "thorparse.ipp"
#include "thortalgo.ipp"

#define MAX_POSITIONS MAX_TOKEN_LENGTH

typedef CIArrayOf<GrammarSymbol> GrammarSymbolArray;

//---------------------------------------------------------------------------

class MultiLexer
{
    friend class TomitaContext;
public:
    MultiLexer(const AsciiDfa & _tokens, const AsciiDfa & _skip, const UnsignedArray & _endTokenChars, unsigned _eofId);

    unsigned getEofId() { return eofId; }
    unsigned next(position_t pos, GrammarSymbolArray & symbols);
    void setDocument(size32_t len, const void * _start);

protected:
    GrammarSymbol * createToken(symbol_id id, unsigned len, const byte * start);
    position_t skipWhitespace(position_t pos);

protected:
    const AsciiDfa & skip;
    const AsciiDfa & tokens;
    bool            isEndToken[256];
    unsigned        eofId;
    struct
    {
        const byte * start;
        const byte * end;
    } state;
};

//---------------------------------------------------------------------------

class GrammarSymbol : public CInterface, public IMatchedElement
{
public:
    GrammarSymbol(symbol_id _id)                            { id = _id; penalty = 0; }
    IMPLEMENT_IINTERFACE

    bool canMerge(const GrammarSymbol * other);

    virtual GrammarSymbol * createMerged(GrammarSymbol * other);

    inline symbol_id  getId() const                         { return id; }

    inline position_t getLength() const                     { return position_t(queryEndPtr() - queryStartPtr()); }
    inline int getPenalty() const                           { return penalty; }
    inline void addPenalty(int delta)                       { penalty += delta; }

    virtual bool isNull() const = 0;
    virtual bool isPacked() const                           { return false; }
    virtual unsigned numChildren() const                    { return 0; }
    virtual GrammarSymbol * queryChild(unsigned i)          { return NULL; }
    virtual IAtom * queryName() const                         { return NULL; }
    virtual GrammarSymbol * queryPacked(unsigned i)         { return NULL; }
    virtual size32_t queryResultSize() const                { return 0; }
    virtual byte * queryResultRow() const                   { return NULL; }
    virtual void resetPosition(const byte * pos) = 0;

    virtual const byte * queryStartPtr() const = 0;
    virtual const byte * queryEndPtr() const = 0;
    virtual const byte * queryRow() const                   { return queryResultRow(); }

public:
    symbol_id id;
    int penalty;
    FeatureValue features;
};

class Terminal : public GrammarSymbol
{
public:
    Terminal(symbol_id _id, const FeatureInfo * _feature, unsigned _len, const byte * _start);

    virtual GrammarSymbol * merge(GrammarSymbol * other) { UNIMPLEMENTED; }

    virtual bool isNull() const                             { return false; }
    virtual const byte * queryStartPtr() const              { return start; }
    virtual const byte * queryEndPtr() const                { return start+len; }
    virtual void resetPosition(const byte * pos)            { UNIMPLEMENTED; }

protected:
    const byte * start;
    unsigned len;
};

//This can be deleted once we switch to an exclusively link counted row thor
class NonTerminal : public GrammarSymbol
{
public:
    NonTerminal(symbol_id id, IAtom * _name, FeatureValue & _features, unsigned numSymbols, GrammarSymbol * * symbols, const byte * _reducePtr, size32_t _resultSize, byte * _resultRow);
    ~NonTerminal();

    virtual bool isNull() const                             { return cachedIsNull; }
    virtual const byte * queryStartPtr() const;
    virtual const byte * queryEndPtr() const;
    virtual IAtom * queryName() const                         { return name; }

    virtual void resetPosition(const byte * pos);
    virtual unsigned numChildren() const                    { return reduced.ordinality(); }
    virtual GrammarSymbol * queryChild(unsigned i);
    virtual size32_t queryResultSize() const                { return resultSize; }
    virtual byte * queryResultRow() const                   { return resultRow; }

protected:
    const byte * reducePtr;
    byte * resultRow;
    IAtom * name;
    size32_t resultSize;                            // This really shouldn't be needed - it is needed to support old style record cloning, and doRowsMatch - which needs to be done with a helper.
    CIArrayOf<GrammarSymbol> reduced;
    bool cachedIsNull;
};

class PackedSymbol : public GrammarSymbol
{
public:
    PackedSymbol(GrammarSymbol * symbol);

    virtual GrammarSymbol * createMerged(GrammarSymbol * other);

    virtual const byte * queryStartPtr() const;
    virtual const byte * queryEndPtr() const;

    virtual void resetPosition(const byte * pos);
    virtual bool isNull() const                             { return false; }
    virtual bool isPacked() const                           { return true; }
    virtual unsigned numChildren() const                    { UNIMPLEMENTED; }
    virtual GrammarSymbol * queryChild(unsigned i)          { UNIMPLEMENTED; }
    virtual IAtom * queryName() const                         { return equivalents.item(0).queryName(); }
    virtual GrammarSymbol * queryPacked(unsigned i);

private:
    GrammarSymbolArray equivalents;
};

//---------------------------------------------------------------------------

class PackedSymbolChoice;
class TomitaMatchWalker : public CInterface, implements IMatchWalker
{
public:
    TomitaMatchWalker(const PackedSymbolChoice & _choice, GrammarSymbol * _symbol);
    IMPLEMENT_IINTERFACE

    virtual IAtom * queryName();
    virtual unsigned queryID();
    virtual size32_t queryMatchSize();
    virtual const void * queryMatchStart();
    virtual unsigned numChildren();
    virtual IMatchWalker * getChild(unsigned idx);

protected:
    const PackedSymbolChoice & choice;
    GrammarSymbol * symbol;
};


class PackedSymbolChoice
{
public:
    void first(GrammarSymbol * symbol);
    bool next(GrammarSymbol * symbol);
    unsigned getInstance(GrammarSymbol * symbol) const;
    void selectBest(GrammarSymbol * symbol);

protected:
    void expandBest(GrammarSymbol * symbol);
    void expandFirst(GrammarSymbol * symbol);
    bool expandNext(unsigned & level, GrammarSymbol * symbol);

public:
    GrammarSymbolArray symbols;
    UnsignedArray branches;
};

//---------------------------------------------------------------------------


class LRParser;
class StackElement : public CInterface
{
public:
    StackElement(GrammarSymbol * _shifted, state_id _state, StackElement * _prev, LRParser * _pool);
    virtual void Release();

    void addToPool();
    void addSibling(StackElement * sib);
    void getDebugText(StringBuffer & s);

    bool potentialPackNode(StackElement & other) const;

protected:
    void doAddToPool();
    void gatherPoolPending(CIArrayOf<StackElement> & pending);

public:
    Linked<GrammarSymbol> shifted;
    state_id state;
    Linked<StackElement> prev;
    Linked<StackElement> sibling;
    LRParser * pool;
};
typedef CIArrayOf<StackElement> StackElementArray;

//would it be better to not link count this??
class LRActiveState
{
public:
    LRActiveState(unsigned maxStates);
    ~LRActiveState();

    void addElementOwn(StackElement * next, bool keepBest);
    void clearReduced();
    void markReduced(state_id id)                           { beenReduced[id] = true; }
    void mergeIn(LRActiveState & other, bool keepBest);
    bool mergePackedNode(unsigned stateId, StackElement * next, bool keepBest);
    bool okToAddReduction(state_id id)                      { return !beenReduced[id]; }
    void reinit();

public:
    StackElement * *    cache;// [MAX_STATES];              // elements in here are NOT linked.
    bool *              beenReduced;
    StackElementArray   elements;
    unsigned            cacheBytes;
    unsigned            maxStates;
};


class LRParser
{
public:
    LRParser(const LRTable & _table, const TomitaParserCallback & _rowState);
    ~LRParser();

    void init();
    void addStartState();
    void process(GrammarSymbol * next, bool singleToken);

    unsigned numAccepted()                                  { return accepted.ordinality(); }
    const GrammarSymbolArray & queryAccepted()              { return accepted; }

// Code to support DAG lex graphs...
    position_t getFirstPosition();
    void removePosition(position_t pos);
    void beginParse(bool _chooseBest);
    void endParse();
    void selectEndPosition(unsigned offset);
    void selectStartPosition(unsigned offset);

    void addFreeElement(StackElement * element);

protected:
    StackElement * createState(StackElement * prev, state_id nextState, GrammarSymbol * shifted);
    void cleanupPosition(position_t pos);
    void clear();
    void doReductions(LRActiveState & active, GrammarSymbol * next);
    void doReductions(GrammarSymbol * next, bool singleToken);
    void doShifts(LRActiveState * active, GrammarSymbol * next);
    void doShifts(GrammarSymbol * next, bool singleToken);
    void expandReduction(StackElement & element, LRProduction * production, unsigned numSymbols);
    LRActiveState * getPosition(position_t pos);
    void setPositionOwn(position_t pos, LRActiveState * value);
    void reduce(StackElement & element, GrammarSymbol * next);
    
protected:
    LRActiveState *     activeInput;
    LRActiveState *     activeOutput;
    LRActiveState *     reduced;                                // temporary array used while reducing
    LRActiveState *     reducedOverflow[2];                     // temporary array used while reducing
    LRActiveState *     curOverflow;                            // temporary array used while reducing

//These are common to a lexer position.
    const LRTable   &   table;
    StackElementArray   shiftPending;                           // temporary array used while shifting.
    GrammarSymbol *     reducedArgs[MAX_PRODUCTION_LENGTH];     // preallocated
    GrammarSymbolArray  accepted;

//Position management
    LRActiveState *     positions[MAX_POSITIONS];
    position_t          firstPosition;
    position_t          endPosition;
    const TomitaParserCallback & rowState;                  // valid between beginParse() and endParse()
    Owned<StackElement> nextFreeElement;
    bool                chooseBest;
};


class TomitaResultIterator : public CInterface, public INlpResultIterator
{
public:
    TomitaResultIterator(const TomitaStateInformation & _rowState, TomitaAlgorithm * _def);
    ~TomitaResultIterator();

    void reset(const GrammarSymbolArray & values);

    virtual bool first();
    virtual bool next();
    virtual bool isValid();
    virtual const void * getRow();

    void setAllocator(IEngineRowAllocator * _outputAllocator)   { outputAllocator.set(_outputAllocator); }
    void invalidate();

protected:
    TomitaMatchWalker * getWalker()                         { return new TomitaMatchWalker(choice, &values.item(curIndex)); }
    void firstChoice();
    bool isBetter(const GrammarSymbol * left, const GrammarSymbol * right);

protected:
    const TomitaStateInformation & rowState;                // currently a 1:1 correspondence between Parser and Iterator.  Needs to be contained if that changes
    Owned<IEngineRowAllocator> outputAllocator;
    TomitaAlgorithm * def;
    PackedSymbolChoice choice;
    unsigned curIndex;
    GrammarSymbolArray values;
    CTomitaMatchedResults results;
    bool singleMatchPerSymbol;
    bool matchFirst;
};

class TomitaParser : public CInterface, public INlpParser
{
public:
    TomitaParser(ICodeContext * ctx, TomitaAlgorithm * _def, unsigned _activityId, INlpHelper * _helper, IHThorParseArg * arg);
    IMPLEMENT_IINTERFACE

    virtual bool performMatch(IMatchedAction & action, const void * in, unsigned len, const void * data);
    virtual INlpResultIterator * queryResultIter();
    virtual void reset();

public:
    TomitaParserCallback rowState;          // keep first since passed as reference to parser constructor
    Owned<IEngineRowAllocator> outputAllocator;
    TomitaAlgorithm * def;
    INlpHelper * helper;
    unsigned eofId;
    unsigned activityId;
    MultiLexer lexer;
    LRParser parser;
    TomitaResultIterator iter;
    StackElementArray accepted;
    GrammarSymbolArray tokens;
    IHThorParseArg * helperArg;
};

#endif
