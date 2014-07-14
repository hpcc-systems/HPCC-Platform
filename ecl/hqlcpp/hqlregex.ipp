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
#ifndef __HQLREGEX_IPP_
#define __HQLREGEX_IPP_

#include "hqlnlp.ipp"

//---------------------------------------------------------------------------
#define NO_DFA_SCORE    ((unsigned)-1)

class HqlRegexExpr;
typedef CIArrayOf<HqlRegexExpr> HqlRegexExprArray;

class RegexContext;
struct GenerateRegexCtx;

class HqlNamedRegex : public CInterface
{
    friend class HqlRegexExpr;
public:
    HqlNamedRegex(IHqlExpression * _expr, IAtom * _name, IHqlExpression * _searchExpr, node_operator _kind, bool _caseSensitive, bool _isMatched);
    ~HqlNamedRegex();

    void addBeginEnd(const ParseInformation & options);
    void analyseNullLeadTrail();
    void calcDfaScore();
    void calculateNext();
    bool canBeNull();
    void cleanup();
    void createDFAs(RegexContext & ctx);
    void expandNamedSymbols();
    void expandRecursion(RegexContext & ctx, HqlNamedRegex * self);
    void generateDFAs();
    void generateRegex(GenerateRegexCtx & info);
    unsigned getDfaScore();
    RegexPattern * queryRootPattern();
    void insertSeparators(IHqlExpression * separator, RegexContext * ctx);
    void markDFAs(unsigned complexity);
    bool matches(IHqlExpression * _def, IAtom * _name, bool _caseSensitive)                   { return searchExpr == _def && name == _name && caseSensitive == _caseSensitive; }
    bool matches(IHqlExpression * _def, IAtom * _name, node_operator _op, bool _caseSensitive)    { return searchExpr == _def && name == _name && kind == _op && caseSensitive == _caseSensitive; }
    bool matchesDefine(IHqlExpression * name, bool _caseSensitive);
    void mergeCreateSets();
    bool queryExpandInline();
    bool queryExpandedRecursion()                           { return doneExpandRecursion; }
    void setRegexOwn(HqlRegexExpr * _def);

    void addUse() { numUses++; }
    void addRecursiveUse() { numUses++; isRecursive = true; isMatched = true; }
    bool isUsed() { return (numUses != 0); }
    void removeUse() { numUses--; }

    unsigned getMinLength() const { return limit.minLength; }
    unsigned getMaxLength() const { return limit.maxLength; }
    const LengthLimit & queryLimit() { return limit; }

protected:
    unsigned numUses;
    IAtom * name;
    LengthLimit limit;
    node_operator kind;
    OwnedHqlExpr searchExpr;
    OwnedHqlExpr expr;
    Owned<HqlRegexExpr> def;
    Owned<RegexNamed> created;
    OwnedHqlExpr defineName;
    bool isMatched:1;
    bool isRecursive:1;
    bool doneExpandNamed:1;
    bool doneCalcDfaScore:1;
    bool doneCreateDFA:1;
    bool doneExpandRecursion:1;
    bool doneMarkDfas:1;
    bool noGenerate:1;
    bool caseSensitive:1;
};


class HqlRegexHashTable : public SuperHashTable
{
public:
    using SuperHashTable::add;
    using SuperHashTable::find;
    ~HqlRegexHashTable() { releaseAll(); }

private:
    virtual void     onAdd(void *et);
    virtual void     onRemove(void *et);
    virtual unsigned getHashFromElement(const void *et) const;
    virtual unsigned getHashFromFindParam(const void *fp) const;
    virtual const void * getFindParam(const void *et) const;
    virtual bool     matchesFindParam(const void *et, const void *key, unsigned fphash) const;
};

class HqlRegexUniqueArray
{
public:
    void append(HqlRegexExpr & expr);
    unsigned ordinality() const             { return array.ordinality(); }
    HqlRegexExpr & item(unsigned i) const   { return array.item(i); }
    void kill()                             { array.kill(); hash.kill(); }

protected:
    HqlRegexExprArray array;
    HqlRegexHashTable hash;
};

//Can't use the IHqlExpressions because instances can't be commoned up....
class HqlDfaInfo;
class SymbolArray;
class HqlRegexExpr : public CInterface
{
    friend class HqlNamedRegex;
    friend class HqlComplexRegexExpr;
public:
    HqlRegexExpr(const ParseInformation & _options, IHqlExpression * _expr, bool _caseSensitive);
    HqlRegexExpr(const ParseInformation & _options, node_operator _op, IHqlExpression * _expr, bool _caseSensitive);
    ~HqlRegexExpr();

//Functions like IHqlExpression....
    node_operator getOperator() const                       { return op; }
    HqlRegexExpr * queryChild(unsigned idx) const           { return args.isItem(idx) ? &args.item(idx) : NULL; }
    unsigned numChildren() const                            { return args.ordinality(); }

    void addOperand(HqlRegexExpr * _arg)                    { args.append(*_arg); }
    void replaceOperand(HqlRegexExpr * _arg, unsigned idx)  { args.replace(*_arg, idx); }
    virtual void setNamed(HqlNamedRegex * _named)           { UNIMPLEMENTED; }
    virtual void setSubPattern(HqlNamedRegex * _sub)        { UNIMPLEMENTED; }

    bool canBeNull()                                        { return limit.canBeNull(); }
    const LengthLimit & queryLimit()                        { return limit; }
    unsigned getAcceptId();
    void setAcceptId(unsigned id)                           { dfaScore = id; }      // change later...
    unsigned getRepeatMin();
    unsigned getRepeatMax();
    bool isStandardRepeat();
    unique_id_t queryUid()                                  { return uid; }

    virtual HqlNamedRegex * queryNamed()                    { return NULL; }
    virtual HqlNamedRegex * querySubPattern()               { return NULL; }

    virtual void analyseNullLeadTrail() = 0;
    virtual void gatherLeading(HqlRegexUniqueArray & target) = 0;
    virtual void gatherTrailing(HqlRegexUniqueArray & target) = 0;
    virtual unsigned numTrailing() const = 0;
    virtual HqlRegexExpr & queryTrailing(unsigned idx) = 0;

//General processing functions
    void calcDfaScore();
    void calculateNext();
    bool canConsume(unsigned next);
    virtual void cleanup();
    virtual HqlRegexExpr * clone();
    void connectRegex();
    HqlRegexExpr * createDFAs(RegexContext & ctx);
    HqlRegexExpr * expandAsDFA();
    HqlRegexExpr * expandAsRepeatedDFA(unsigned count);
    HqlRegexExpr * expandNamedSymbols();
    void generateDFA(IDfaPattern * pattern);
    void generateDFAs();
    void generateRegex(GenerateRegexCtx & info);
    HqlRegexExpr * insertSeparators(IHqlExpression * separator, RegexContext * ctx);
    void markDFAs(unsigned complexity);
    HqlRegexExpr * mergeCreateSets();
    HqlRegexExpr * expandRecursion(RegexContext & ctx, HqlNamedRegex * self);
    void gatherConsumeSymbols(SymbolArray & next);
    unsigned getHash()  
    { 
        HqlRegexExpr * key = this;
        return hashc((const byte *)&key, sizeof(key), 0);
    }

protected:
    bool isWorthConvertingToDFA();
    bool isSimpleStringList();

private:
    void init();

protected:
    unsigned uid;
    OwnedHqlExpr expr;
    node_operator op;                           // always create an expression and remove this?
    HqlRegexExprArray args;                     // move to complex!
    Owned<RegexPattern> created;
    const ParseInformation & options;
    HqlDfaInfo * dfa;                           // pointer because forward declared
                                                // create a derived HqlDfaRegexExpr and move this there..
    HqlRegexUniqueArray following;
    LengthLimit limit;                          // move to complex!
    unsigned dfaScore;                          // move to complex!
    bool createDFA:1;
    bool cleaned:1;
    bool connected:1;
    bool analysed:1;
    bool added:1;
    bool caseSensitive:1;
};

//MORE: Should move some of the logic to complex + remove the 

class HqlSimpleRegexExpr : public HqlRegexExpr
{
    friend class HqlNamedRegex;
public:
    HqlSimpleRegexExpr(const ParseInformation & _options, IHqlExpression * _expr, bool _caseSensitive) : HqlRegexExpr(_options, _expr, _caseSensitive) {}
    HqlSimpleRegexExpr(const ParseInformation & _options, node_operator _op, IHqlExpression * _expr, bool _caseSensitive) : HqlRegexExpr(_options, _op, _expr, _caseSensitive) {}

    virtual void analyseNullLeadTrail();
    virtual void gatherLeading(HqlRegexUniqueArray & target);
    virtual void gatherTrailing(HqlRegexUniqueArray & target);
    virtual unsigned numTrailing() const;
    virtual HqlRegexExpr & queryTrailing(unsigned idx);
};

class HqlComplexRegexExpr : public HqlRegexExpr
{
    friend class HqlNamedRegex;
public:
    HqlComplexRegexExpr(const ParseInformation & _options, IHqlExpression * _expr, bool _caseSensitive) : HqlRegexExpr(_options, _expr, _caseSensitive) {}
    HqlComplexRegexExpr(const ParseInformation & _options, node_operator _op, IHqlExpression * _expr, bool _caseSensitive) : HqlRegexExpr(_options, _op, _expr, _caseSensitive) {}

    virtual void analyseNullLeadTrail();
    virtual void gatherLeading(HqlRegexUniqueArray & target);
    virtual void gatherTrailing(HqlRegexUniqueArray & target);
    virtual unsigned numTrailing() const                            { return trailing.ordinality(); }
    virtual HqlRegexExpr & queryTrailing(unsigned idx)              { return trailing.item(idx); }

    virtual HqlNamedRegex * queryNamed()                        { return named; }
    virtual HqlNamedRegex * querySubPattern()                   { return subPattern; }

    virtual void setNamed(HqlNamedRegex * _named)               { named.set(_named); }
    virtual void setSubPattern(HqlNamedRegex * _sub)            { subPattern.set(_sub); }
    virtual void cleanup();

private:
    Owned<HqlNamedRegex> named;
    Owned<HqlNamedRegex> subPattern;
    HqlRegexUniqueArray leading;
    HqlRegexUniqueArray trailing;
};


class HqlRegexExprSet
{
public:
    void add(HqlRegexExpr * value);
    int compare(const HqlRegexExprSet & _other) const;
    bool equals(const HqlRegexExprSet & _other) const;

    inline unsigned ordinality() const                      { return values.ordinality(); }
    inline HqlRegexExpr & item(unsigned i)                  { return values.item(i); }

private:
    CopyCIArrayOf<HqlRegexExpr> values;
};

class HqlDfaState;
class HqlDfaTransition : public CInterface
{
public:
    HqlDfaTransition(unsigned _code, HqlDfaState * _next) { code = _code; next = _next; }

    unsigned code;
    HqlDfaState * next;
};


class HqlDfaState : public CInterface
{
public:
    HqlDfaState(unsigned _id)               { id = _id; }
    bool isAccepting();

public:
    unsigned id;
    HqlRegexExprSet position;
    CIArrayOf<HqlDfaTransition> transitions;
};


class HqlDfaInfo : public CInterface
{
public:
    CIArrayOf<HqlDfaState> states;
    CIArrayOf<HqlDfaState> orderedStates;
};


class RegexContext : public NlpParseContext
{
    friend class HqlNamedRegex;
    friend class HqlRegexExpr;
public:
    RegexContext(IHqlExpression * _expr, IWorkUnit * wu, const HqlCppOptions & options, ITimeReporter * _timeReporter, byte _algorithm);
    ~RegexContext();

    virtual void compileSearchPattern();
    virtual void getDebugText(StringBuffer & s, unsigned detail);
    virtual bool isGrammarAmbiguous() const { return false; }
    virtual INlpParseAlgorithm * queryParser()              { return &parser; }

    HqlNamedRegex * queryDefine(IHqlExpression * defineName, bool caseSensitive);

//When used to generate a lexer - should be split out of RegexContext when I have the time
    void addLexerToken(unsigned id, IHqlExpression * pattern);
    void beginLexer();
    void generateLexer(IDfaPattern * builder);

protected:
    void analysePattern();
    void buildStructure();
    HqlRegexExpr * createFollow(HqlRegexExpr * start, IHqlExpression * body, node_operator endOp, IHqlExpression * endExprs, bool caseSensitive);
    HqlRegexExpr * createStructure(IHqlExpression * expr, bool caseSensitive);
    void expandRecursion();
    void generateRegex();
    void insertSeparators();
    void optimizePattern();
    void optimizeSpotDFA();
    void updateTimer(const char * name, unsigned timems);
    HqlNamedRegex * queryNamed(IHqlExpression * defn, IAtom * name, node_operator op, bool caseSensitive);
    HqlNamedRegex * createNamed(IHqlExpression * expr, IAtom * name, node_operator op, bool caseSensitive);

protected:
    CIArrayOf<HqlNamedRegex> named;
    CIArrayOf<HqlNamedRegex> deadNamed;
    Owned<HqlNamedRegex> root;
    RegexAlgorithm parser;
    bool createLexer;
    Owned<HqlNamedRegex> lexerNamed;
    Owned<HqlRegexExpr> lexerRoot;
    Owned<HqlRegexExpr> lexerOr;
};

struct GenerateRegexCtx
{
    GenerateRegexCtx(const RegexContext & _ctx, const ParseInformation & _info, RegexIdAllocator & _idAllocator) : regex(_ctx), info(_info), idAllocator(_idAllocator) { namedKind = no_none; }

    const RegexContext & regex;
    const ParseInformation & info;
    RegexIdAllocator & idAllocator;
    node_operator namedKind;
};


#endif
