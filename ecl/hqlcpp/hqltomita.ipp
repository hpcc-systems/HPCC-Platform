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
#ifndef __HQLTOMITA_IPP_
#define __HQLTOMITA_IPP_

#include "hqlnlp.ipp"
#include "thortparse.ipp"

enum { ValueUnknown, ValueRecursive, ValueKnown };
 
class TomFeature;
class TomRule;
typedef CIArrayOf<TomFeature> TomFeatureArray;
typedef Owned<TomFeature> OwnedTomFeature;

interface IResolveContext
{
    virtual TomRule * queryDefine(IHqlExpression * defineName) = 0;
};

//-- Features etc. --------

enum TomFeatureKind { TomMaskFeature, TomStrFeature, TomIntFeature };

class TomFeature : public CInterface
{
public:
    TomFeature(IHqlExpression * expr, TomFeatureKind _kind);

    void addValue(TomFeature * feature) { values.append(*LINK(feature)); }

    StringBuffer & getName(StringBuffer & out);
    StringBuffer & getValue(StringBuffer & out);
    StringBuffer & getGuardValue(StringBuffer & out);

    StringBuffer & getDebugText(StringBuffer & out);
    bool matches(IHqlExpression * expr)         { return expr == def; }

protected:
    OwnedHqlExpr    def;
    TomFeatureKind  kind;
    TomFeatureArray values;
    unsigned        mask;       // or value if single valued.
};

//-- Guards ------------------

class TomGuard : public CInterface
{
public:
    TomGuard(TomFeature * _feature)         { feature.set(_feature); }

    virtual StringBuffer & getDebugText(StringBuffer & out) = 0;

protected:
    OwnedTomFeature feature;
};

class TomMaskGuard : public TomGuard
{
public:
    TomMaskGuard(TomFeature * _feature, TomFeature * _value) : TomGuard(_feature)   { value.set(_value); }

    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    OwnedTomFeature value;      // anonymous entry
};

class TomStrGuard : public TomGuard
{
public:
    TomStrGuard(TomFeature * _feature, IAtom * _value) : TomGuard(_feature)   { value = _value; }

    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    IAtom *           value;
};

class TomIntGuard : public TomGuard
{
public:
    TomIntGuard(TomFeature * _feature, unsigned _value) : TomGuard(_feature)    { value = _value; }

    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    unsigned        value;
};

typedef CIArrayOf<TomGuard> TomGuardArray;

//-- Tokens --
class TomitaContext;
class TomToken : public CInterface
{
    friend class TomitaContext;
public:
    TomToken(IHqlExpression * expr, const TomGuardArray & _guards);

    StringBuffer & getDebugText(StringBuffer & out);
    inline unsigned getId()                         { return id; }
    StringBuffer & getName(StringBuffer & out);
    bool matches(IHqlExpression * expr)             { return expr == pattern; }
    inline void setId(unsigned _id)                 { id = _id; }

protected:
    unsigned id;
    HqlExprAttr pattern;
    TomGuardArray guards;
};
    

class TomTokenSet
{
    TomTokenSet(const TomTokenSet &);
    void operator = (const TomTokenSet & other);
public:
    TomTokenSet() {}

    bool add(TomToken & token);
    bool add(TomTokenSet & other);

    inline unsigned ordinality() const                      { return tokens.ordinality(); }
    inline TomToken & item(unsigned i) const                { return tokens.item(i); }
    void kill()                                             { tokens.kill(); }

protected:
    CIArrayOf<TomToken> tokens;
};

//-- Different types of steps - the elements on the rhs of a production

class TomRule;
class TomProduction;
class TomStep : public CInterface
{
public:
    virtual bool calcCanBeNull(bool isRoot, bool & result);
    virtual bool calcFirst(bool isRoot, TomTokenSet & tokens);
    virtual bool canBeNull() = 0;
    virtual TomStep * expandRecursion(IResolveContext & ctx) { return NULL; }
    virtual StringBuffer & getDebugText(StringBuffer & out);
    virtual bool isTerminal() const                         { return false; }
    virtual unsigned getId()                                { UNIMPLEMENTED; }
    virtual TomProduction * queryExpandRules()              { return NULL; }
    virtual TomRule * queryRule()                           { return NULL; }
};

class TomConditionStep : public TomStep
{
public:
    TomConditionStep(IHqlExpression * expr, TomRule * _rule) { rule = _rule; condition.set(expr); }

    virtual bool canBeNull() { return true; }
    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    TomRule * rule;
    OwnedHqlExpr condition;
};

class TomGuardStep : public TomStep
{
public:
    TomGuardStep(const TomGuardArray & _guards);

    virtual bool canBeNull() { return true; }
    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    TomGuardArray guards;
};

class TomNonTerminalStep : public TomStep
{
public:
    TomNonTerminalStep(TomRule * _rule, const TomGuardArray & _guards);

    virtual bool calcCanBeNull(bool isRoot, bool & result);
    virtual bool calcFirst(bool isRoot, TomTokenSet & tokens);
    virtual bool canBeNull();
    virtual StringBuffer & getDebugText(StringBuffer & out);
    virtual unsigned getId();
    virtual TomProduction * queryExpandRules();
    virtual TomRule * queryRule() { return rule; }

protected:
    TomRule * rule;             // unlinked pointer to avoid cyclic references
    TomGuardArray guards;
};

class TomPenaltyStep : public TomStep
{
public:
    TomPenaltyStep(int _penalty)        { penalty = _penalty; }

    virtual bool canBeNull() { return true; }
    virtual StringBuffer & getDebugText(StringBuffer & out);

protected:
    int penalty;
};

class TomTerminalStep : public TomStep
{
public:
    TomTerminalStep(TomToken * _token) { token.set(_token); }

    virtual bool calcFirst(bool isRoot, TomTokenSet & tokens);
    virtual bool canBeNull() { return false; }
    virtual StringBuffer & getDebugText(StringBuffer & out);
    virtual bool isTerminal() const                         { return true; }
    virtual unsigned getId()                                { return token->getId(); }

protected:
    Owned<TomToken> token;
};

class TomUseStep : public TomStep
{
public:
    TomUseStep(IHqlExpression * _expr, const TomGuardArray & _guards);

    virtual bool canBeNull()                                { UNIMPLEMENTED; }
    virtual TomStep * expandRecursion(IResolveContext & ctx);
    virtual StringBuffer & getDebugText(StringBuffer & out) { UNIMPLEMENTED; }

protected:
    OwnedHqlExpr expr;
    TomGuardArray guards;
};

//-- Production --
typedef CIArrayOf<TomStep> TomStepArray;

struct GenerateTomitaCtx;
class TomProduction : public CInterface
{
    friend class TomitaContext;
public:
    TomProduction(TomRule * _rule)                          { rule = _rule; isRoot = false; id = NotFound; penalty = 0; seq = getUniqueId(); }
    void addPenalty(int value)                              { penalty += value; }
    void addStepOwn(TomStep * step)                         { steps.append(*step); }
    void buildProduction(LRTableBuilder & builder, IHqlExpression * test, GenerateTomitaCtx & ctx);
    void buildReduce(LRTableBuilder & builder);
    bool calcCanBeNull(bool isRoot, bool & result);
    bool calcFirst(bool isRoot, TomTokenSet & tokens);
    bool calcFollow();
    bool canBeNull();
    void expandRecursion(IResolveContext & ctx);
    StringBuffer & getDebugText(StringBuffer & out);
    bool isSimple(bool singleUse);
    void optimizeRules();
    bool transformClonesFirstSymbol();

    inline unsigned getId()                                 { return id; }
    inline bool isRootReduction()                           { return isRoot; }
    inline TomStep * queryStep(unsigned i)                  { if (steps.isItem(i)) return &steps.item(i); return NULL; }
    inline void setId(unsigned _id)                         { id = _id; }
    inline void setRoot()                                   { isRoot = true; }
    inline IHqlExpression * queryTransform()                { return transform; }

public:
    unique_id_t seq;
    
protected:
    unsigned id;
    TomRule * rule;
    OwnedHqlExpr transform;
    TomFeatureArray localFeatures;
    TomStepArray steps;
    int penalty;
    bool isRoot;

};

class TomRule : public CInterface
{
    friend class TomitaContext;
    friend class HqlLRState;
public:
    TomRule(IHqlExpression * expr, IAtom * name, const TomFeatureArray & _features, bool implicit, bool _isMatched);

    void addProductionOwn(TomProduction * production)       { productions.append(*production); }
    bool addFollowOwn(TomToken & token)                     { return follow.add(token); }
    void buildProductions(LRTableBuilder & builder, GenerateTomitaCtx & ctx);
    void buildReduce(LRTableBuilder & builder, TomProduction * production);
    bool canBeNull();
    bool calcCanBeNull(bool isRoot, bool & result);
    bool calcFirst(bool isRoot, TomTokenSet & tokens);
    bool calcFollow();
    void expandRecursion(IResolveContext & ctx);
    bool gatherFollow(TomRule & rule);
    bool gatherFollow(TomStep & step);
    StringBuffer & getDebugText(StringBuffer & out);
    void getFeatures(TomFeatureArray & target);
    StringBuffer & getName(StringBuffer & out);
    bool matches(IHqlExpression * expr, IAtom * name)                     { return expr == def && name == cachedName; }
    bool matchesDefine(IHqlExpression * name);
    TomProduction * queryExpand();
    IAtom * queryName()                                       { return cachedName; }
    IHqlExpression * queryRecord();
    void optimizeRules();

    void addUse()                                           { numUses++; }
    void setProductionIds(HqlExprArray & productionMappings, unsigned & id);
    void setTest(IHqlExpression * value)                    { test.set(value); }
    
    inline unsigned getId()                                 { return id; }
    inline void setId(unsigned _id)                         { id = _id; }

    //Used when generating tables to expand the kernal entries.
    bool alreadyExpanded(unsigned newState)                 { if (newState == curExpandState) return true; curExpandState = newState; return false; }

protected:
    OwnedHqlExpr def;
    OwnedHqlExpr test;
    OwnedHqlExpr defineName;
    IAtom * cachedName;
    unsigned id;
    bool implicit;  // if implicit, need to inherit features from children...
    CIArrayOf<TomProduction> productions;
    TomFeatureArray features;               // what features does this rule have?
    TomTokenSet first;
    TomTokenSet follow;
    unsigned numUses;
    unsigned curExpandState;
    bool isNull;
    byte isNullState;
    byte firstState;
    bool isMatched;
};


struct HqlLRItem : public CInterface
{
public:
    HqlLRItem(TomProduction * _production, unsigned _index) { production = _production; index = _index; }

    StringBuffer & getDebugText(StringBuffer & out)         { return out.append("[").append(production->getId()).append(',').append(index).append("]"); }
public:
    TomProduction * production;
    unsigned        index;
};


class HqlLRItemSet
{
    HqlLRItemSet(const HqlLRItemSet &);
    void operator = (const HqlLRItemSet & other);
public:
    HqlLRItemSet() {}

    void add(HqlLRItem * value);
    int compare(const HqlLRItemSet & _other) const;
    bool equals(const HqlLRItemSet & _other) const;

    inline unsigned ordinality() const                      { return values.ordinality(); }
    inline HqlLRItem & item(unsigned i)                     { return values.item(i); }

private:
    CIArrayOf<HqlLRItem> values;
};

class HqlLRState;
struct HqlLRGoto : public CInterface
{
public:
    HqlLRGoto(token_id _symbol, HqlLRState * _state)            { symbol = _symbol; state = _state; }

    StringBuffer & getDebugText(StringBuffer & out);
public:
    token_id symbol;
    HqlLRState * state;
};

class HqlLRState : public CInterface
{
public:
    void addItem(TomProduction * production, unsigned idx);
    void addGoto(unsigned id, HqlLRState * state);
    void buildNullReductions(LRTableBuilder & builder, TomRule * rule);
    void buildStateItem(LRTableBuilder & builder, TomProduction * production, unsigned index);
    void buildState(LRTableBuilder & builder, TomToken * eofToken, unsigned maxToken);
    StringBuffer & getDebugText(StringBuffer & out);
    HqlLRState * queryGoto(token_id id);

public:
    unsigned id;
    HqlLRItemSet items;
    CIArrayOf<HqlLRGoto> gotos;
};

//-- Interface to the NLP stuff...

class TomitaContext : public NlpParseContext, public IResolveContext
{
    friend class TomProduction;
public:
    TomitaContext(IHqlExpression * _expr, IWorkUnit * _wu, const HqlCppOptions & _options, ITimeReporter * _timeReporter);
    ~TomitaContext();

    virtual void compileSearchPattern();
    virtual void getDebugText(StringBuffer & s, unsigned detail);
    virtual bool isGrammarAmbiguous() const { return grammarAmbiguous; };
    virtual INlpParseAlgorithm * queryParser()              { return &parser; }

protected:
    void addLexerTerminator(IHqlExpression * expr);
    void addGotos(TomProduction * production, unsigned idx);
    virtual unsigned addMatchReference(IHqlExpression * expr);
    HqlLRState * addState(HqlLRState * state);
    void associateIds();
    void calculateFeatureMasks();
    void calculateFollowing();
    void calculateStates();
    bool canCreateLexerToken(IHqlExpression * expr, bool insideOr);
    TomFeature * createAnonFeature(TomFeatureArray & values);
    TomFeature * createFeature(IHqlExpression * expr);
    TomFeature * createFeatureValue(IHqlExpression * expr);
    void createFeatures(TomFeatureArray & target, IHqlExpression * expr);
    TomGuard * createGuard(IHqlExpression * featureExpr, IHqlExpression * valueExpr);
    void createGuards(TomGuardArray & guards, IHqlExpression * expr);
    void createImplicitProduction(TomRule * rule, TomRule * self, IHqlExpression * expr, bool expandTokens);
    TomRule * createImplicitRule(TomRule * self, IHqlExpression * expr, bool expandTokens);
    void createProduction(TomRule * self, TomRule * rule, IHqlExpression * expr, bool expandTokens);
    void createProductions(TomRule * rule, IHqlExpression * expr, bool expandTokens);
    TomRule * createRule(IHqlExpression * expr, IAtom * name, bool expandTokens);
    void createStepAsImplicitRule(TomRule * self, TomProduction * production, IHqlExpression * expr, bool expandTokens);
    void createSteps(TomRule * self, TomProduction * production, IHqlExpression * expr, bool expandTokens);
    TomToken * createToken(IHqlExpression * expr);
    void expandRecursion();
    void generateLexer();
    void generateParser();
    void generateTables();
    bool isToken(IHqlExpression * expr, bool expandTokens);
    void optimizeRules();

    virtual TomRule * queryDefine(IHqlExpression * defineName);
    TomFeature * queryFeature(IHqlExpression * expr);
    TomRule * queryRule(IHqlExpression * expr, IAtom * name);
    
protected:
    TomitaAlgorithm parser;
    const HqlCppOptions & translatorOptions;
    CIArrayOf<TomRule> rules;
    CIArrayOf<TomToken> tokens;
    CIArrayOf<TomFeature> features;
    CopyCIArrayOf<TomRule> activeRules;
    unsigned numTerminals;
    unsigned numSymbols;
    unsigned numProductions;
    CIArrayOf<HqlLRState> states;
    CIArrayOf<HqlLRState> orderedStates;
    Owned<TomRule> grammarRule;
    Owned<TomRule> rootRule;
    Owned<TomProduction> rootProduction;
    Owned<HqlLRState> rootState;
    Owned<TomToken> eofToken;
    HqlLRState * * gotos;
    bool * done;
    bool maximizeLexer;
    bool grammarAmbiguous;
};

struct GenerateTomitaCtx
{
    GenerateTomitaCtx(const TomitaContext & _ctx, const ParseInformation & _info, RegexIdAllocator & _idAllocator) : regex(_ctx), info(_info), idAllocator(_idAllocator) { }

    const TomitaContext & regex;
    const ParseInformation & info;
    RegexIdAllocator & idAllocator;
};

#endif
