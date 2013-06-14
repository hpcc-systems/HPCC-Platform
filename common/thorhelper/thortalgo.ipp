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

#ifndef __THORTALGO_IPP_
#define __THORTALGO_IPP_

#include "thorparse.ipp"

#define MAX_PRODUCTION_LENGTH       100
#define MAX_TOKEN_LENGTH            1024

typedef unsigned token_id;
typedef unsigned symbol_id;
typedef unsigned state_id;
typedef unsigned position_t;

typedef unsigned mask_feature_t;
typedef unsigned flat_feature_t;
class GrammarSymbol;
class PackedSymbolChoice;
//NB: sizeof(mask_feature_t) >= sizeof(flat_feature_t)


struct TomitaStateInformation
{
public:
    void set(const TomitaStateInformation & other);

public:
    IMatchedAction * action;
    const void * row;
    const byte * inputText;
    unsigned lengthInputText;
    INlpHelper * helper;
    IHThorParseArg * helperArg;
};
    

struct TomitaParserCallback : public TomitaStateInformation
{
public:
    void addAllocator(IEngineRowAllocator & allocator) { allocators.append(OLINK(allocator)); }
    IEngineRowAllocator * queryAllocator(IOutputMetaData * resultSize) const;

public:
    IArrayOf<IEngineRowAllocator> allocators;
    NlpInputFormat inputFormat;
};

class TomitaMatchPath;
class TomitaMatchSearchInstance : public NlpMatchSearchInstance
{
public:
    TomitaMatchSearchInstance() { choices = NULL; }

    GrammarSymbol * find(GrammarSymbol * top, const TomitaMatchPath & path, unsigned depth);
    GrammarSymbol * findInChildren(GrammarSymbol * top, const TomitaMatchPath & path, unsigned depth);

    PackedSymbolChoice * choices;
};

class THORHELPER_API TomitaMatchPath : public NlpMatchPath
{
public:
    TomitaMatchPath(MemoryBuffer & in) : NlpMatchPath(in) { }
    TomitaMatchPath(const UnsignedArray & _ids, const UnsignedArray & _indices) : NlpMatchPath(_ids, _indices) { }

    IMatchedElement * getMatch(GrammarSymbol * top, PackedSymbolChoice & choice) const;
};


class THORHELPER_API CTomitaMatchedResultInfo : public CMatchedResultInfo
{
public:
    virtual void addResult(const UnsignedArray & ids, const UnsignedArray & indices)
                                                            { matchResults.append(*new TomitaMatchPath(ids, indices)); }
    virtual NlpMatchPath * createMatchPath(MemoryBuffer & in) { return new TomitaMatchPath(in); }
};

class THORHELPER_API CTomitaMatchedResults : public CMatchedResults
{
public:
    CTomitaMatchedResults(CMatchedResultInfo * _def) : CMatchedResults(_def) {}

    void extractResults(GrammarSymbol * top, PackedSymbolChoice & choice, const byte * _in);
};



//---------------------------------------------------------------------------

class FeatureInfo
{
public:
    FeatureInfo();
    ~FeatureInfo();

    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);

    inline size32_t getSize() const         { return numMask*sizeof(mask_feature_t)+numFlat*sizeof(flat_feature_t); }
    inline unsigned getNum() const          { return numMask+numFlat; }

    mask_feature_t * getMaskDefaults()      { return (mask_feature_t*)defaults; }
    flat_feature_t * getFlatDefaults()      { return (flat_feature_t *)((mask_feature_t*)defaults + numMask); }
    
    byte        numMask;
    byte        numFlat;
    void *      defaults;
};

//---------------------------------------------------------------------------

#define UNKNOWN_FEATURE     (flat_feature_t)-1

enum { FKmask, FKflat, FKemask, FKeflat, FKmax };

class FeatureAction : public CInterface
{
public:
    FeatureAction();

    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);

public:
    byte        featureKind;
    unsigned    srcSymbol;
    unsigned    srcFeatureIndex;        // index within the feature kind
    unsigned    tgtFeatureIndex;
};

class ProductionFeatureInfo : public FeatureInfo
{
public:
    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);

public:
    //MORE: Cache this value...
    unsigned totalFeatures() const      { return result.numMask + result.numFlat + extra.numMask + extra.numFlat; }

    FeatureInfo result;
    FeatureInfo extra;
    CIArrayOf<FeatureAction> actions;
};

class FeatureValue
{
public:
    FeatureValue() { info = NULL; }

    const FeatureInfo * info;
    MemoryAttr      values;
};

//---------------------------------------------------------------------------

enum 
{
    LRVnone,
    LRVfirst,
    LRVlast,
    LRVvalidateasc,
    LRVvalidateuni,
    LRVchecklength,
    LRVcheckin,
    LRVbefore,
    LRVafter,
    LRVmax
};

struct LRValidator
{
    friend class LRTableBuilder;
public:
    LRValidator();
    ~LRValidator();

    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);
    bool isValid(unsigned numSymbols, GrammarSymbol * * symbols, const byte * reducePtr, const TomitaParserCallback & state);
    StringBuffer & trace(StringBuffer & out);

public:
    byte        kind;
    unsigned    minExpectedBytes;
    unsigned    maxExpectedBytes;
    AsciiDfa *  dfa;
    unsigned    validatorIndex;
};

class THORHELPER_API LRProduction
{
    friend class LRTableBuilder;
    //Following are used in serialization
    enum { NSFclonesFirstSymbol = 0x00010000 };

public:
    LRProduction();

    GrammarSymbol * reduce(GrammarSymbol * *, const byte * reducePtr, const TomitaParserCallback & state);
    unsigned getNumSymbols()                            { return numSymbols; }
    void setMetaData(IOutputMetaData * _meta);
    StringBuffer & trace(StringBuffer & out, unsigned id);

    void deserialize(unsigned _prodId, MemoryBuffer & in);
    void serialize(MemoryBuffer & out);

protected:
    unsigned prodId;   
    symbol_id ruleId;
    IAtom * ruleName;
    unsigned numSymbols;
    ProductionFeatureInfo feature;
    int penalty;
    LRValidator validator;
    Linked<IOutputMetaData> resultSize;
    bool transformClonesFirstSymbol;
};

//---------------------------------------------------------------------------

enum ActionKind { NoAction, AcceptAction, ShiftAction, ReduceAction, MultiAction };
struct THORHELPER_API LRAction
{
public:
    LRAction()                                              { value = 0; }
    LRAction(ActionKind action, unsigned extra)             { value = extra | (action << KindShift); }

    unsigned getExtra() const                               { return value & ExtraMask; }
    ActionKind getAction() const                            { return (ActionKind)(value >> KindShift); }
    unsigned getValue() const                               { return value; }
    void set(ActionKind action, unsigned extra)             { value = extra | (action << KindShift); }
    void setValue(unsigned _value)                          { value = _value; }
    void deserialize(MemoryBuffer & in)                     { in.read(value); }
    void serialize(MemoryBuffer & out)                      { out.append(value); }
    StringBuffer & trace(StringBuffer & out);

protected:
    enum { ExtraMask = 0xfffffff, KindMask = 0xf0000000, KindShift = 28 };
    unsigned value;
};


#define NO_STATE ((state_id)-1)

class LRTable;
class THORHELPER_API LRState
{
    friend class LRTableBuilder;
public:
    LRState(LRTable * _table, LRAction * _actions, unsigned * _gotos);
    ~LRState();

    bool canAccept(token_id sym) const;
    state_id getGoto(symbol_id sym) const;
    state_id getShift(token_id sym) const;
    unsigned numReductions(token_id sym) const;
    unsigned queryReduction(token_id sym, unsigned idx) const;
    StringBuffer & trace(StringBuffer & out, unsigned id) const;

private:
    LRAction *          actions;        // [numTokens]
    unsigned *          gotos;          // [numSymbols-numTokens]
    LRTable *           table;
};

class THORHELPER_API LRTable
{
public:
    LRTable();
    ~LRTable();

    void alloc();
    void deserialize(MemoryBuffer & in);
    void serialize(MemoryBuffer & out);
    StringBuffer & trace(StringBuffer & out);

public:
    unsigned    numStates;
    unsigned    numSymbols;
    unsigned    numTokens;
    unsigned    numProductions;
    unsigned    numExtraActions;
    state_id    rootState;
    LRState * * states;
    LRAction *  allActions;
    unsigned *  allGotos;
    LRAction *  extraActions;
    LRProduction * productions;
};


struct LRActionItem : public CInterface
{
public:
    LRActionItem(token_id _id, ActionKind _action, unsigned _extra) : action(_action, _extra) { id = _id; }

public:
    token_id    id;
    LRAction    action;
};

class THORHELPER_API LRTableBuilder
{
public:
    LRTableBuilder(LRTable & _table);
    ~LRTableBuilder();

    void init(unsigned _numStates, unsigned _numTokens, unsigned _numSymbols, unsigned _numProductions);

    void addAccept(token_id id);
    void addShift(token_id id, state_id newState);
    void addGoto(symbol_id id, state_id newState);
    void addProduction(unsigned id, unsigned ruleId, IAtom * ruleName, unsigned numToPop, int penalty, bool transformClonesFirstSymbol);
    void addReduce(token_id id, unsigned prod);
    void addValidator(unsigned prodId, byte kind, unsigned low, unsigned high, AsciiDfa * dfa);
    void beginState(unsigned id);
    void endState();
    void finished(unsigned rootId);
    void removeDuplicateShiftStates();

    inline bool isAmbiguous() const { return ambiguous; }

protected:
    LRTable & table;
    CIArrayOf<LRActionItem> actions;
    UnsignedArray extraActions;
    LRState * curState;
    bool ambiguous;
};


class THORHELPER_API TomitaAlgorithm : public NlpAlgorithm
{
    friend class TomitaParser;
public:
    TomitaAlgorithm(IRecordSize * _outRecordSize);
    ~TomitaAlgorithm();

    virtual INlpParser * createParser(ICodeContext * ctx, unsigned activityId, INlpHelper * helper, IHThorParseArg * arg);

    virtual void init(IHThorParseArg & arg);
    virtual void serialize(MemoryBuffer & out);
            void deserialize(MemoryBuffer & in);

    inline unsigned numProductions() const { return table.numProductions; }

public:
    LRTable table;
    AsciiDfa skipDfa;
    AsciiDfa tokenDfa;
    UnsignedArray endTokenChars;
    unsigned eofId;
    Owned<IRecordSize> outRecordSize;
};

#endif
