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
#ifndef U_OVERRIDE_CXX_ALLOCATION
#define U_OVERRIDE_CXX_ALLOCATION 0 // Enabling this forces all allocation of ICU objects to ICU's heap, but is incompatible with jmemleak
#endif
#include <algorithm>
#include "jliball.hpp"
#include "eclrtl.hpp"
#include "hqlexpr.hpp"
#include "hqlcerrors.hpp"
#include "hqlutil.hpp"
#include "hqlregex.ipp"
#include "hqlhtcpp.ipp"
#include "hqlcatom.hpp"
#include "hqlcpputil.hpp"
#include "thorralgo.ipp"
#include "hqlthql.hpp"

#include "unicode/uchar.h"

//#define NEW_DFA_CALC
//#define DEFAULT_DFA_COMPLEXITY    0
#define DEFAULT_UNICODE_DFA_COMPLEXITY      0
#define DEFAULT_UTF8_DFA_COMPLEXITY         2000
#define DEFAULT_DFA_COMPLEXITY              10000

inline unsigned addWithoutOverflow(unsigned x, unsigned y)
{
    if (UINT_MAX - x <= y)
        return UINT_MAX;
    return x + y;
}

inline unsigned multiplyWithoutOverflow(unsigned x, unsigned y)
{
    if (UINT_MAX / x <= y)
        return UINT_MAX;
    return x * y;
}

//===========================================================================


static IValue * getCastConstant(IValue * value, type_t tc)
{
    switch (tc)
    {
    case type_unicode:
        return value->castTo(unknownUnicodeType);
    case type_utf8:
        return value->castTo(unknownUtf8Type);
    case type_string:
        return value->castTo(unknownStringType);
    }
    throwUnexpected();
}


static HqlRegexExpr * createRegexExpr(const ParseInformation & options, IHqlExpression * expr, bool caseSensitive) 
{
    switch (expr->getOperator())
    {
    case no_null:
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
    case no_pat_endpattern:
    case no_pat_singlechar:
        return new HqlSimpleRegexExpr(options, expr, caseSensitive);
    }

    return new HqlComplexRegexExpr(options, expr, caseSensitive);
}


static HqlRegexExpr * createRegexExpr(const ParseInformation & options, node_operator op, IHqlExpression * expr, bool caseSensitive)
{
    switch (op)
    {
    case no_null:
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
    case no_pat_endpattern:
    case no_pat_singlechar:
        return new HqlSimpleRegexExpr(options, op, expr, caseSensitive);
    }
    return new HqlComplexRegexExpr(options, op, expr, caseSensitive);
}

//---------------------------------------------------------------------------

void HqlRegexHashTable::onAdd(void *et) 
{  
    ((HqlRegexExpr*)et)->Link();
}

void HqlRegexHashTable::onRemove(void *et) 
{
    ((HqlRegexExpr*)et)->Release();
}

unsigned HqlRegexHashTable::getHashFromElement(const void *et) const
{
    return ((HqlRegexExpr*)et)->getHash();
}

unsigned HqlRegexHashTable::getHashFromFindParam(const void *fp) const
{
    return ((HqlRegexExpr*)fp)->getHash();
}

const void * HqlRegexHashTable::getFindParam(const void *et) const
{
    return et;
}
bool HqlRegexHashTable::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    return et == key;
}

void HqlRegexUniqueArray::append(HqlRegexExpr & expr)
{
    if (!hash.find(&expr))
    {
        array.append(expr);
        hash.add(&expr);
    }
    else
    {
        expr.Release();
    }
}

//---------------------------------------------------------------------------

inline unsigned limitedAdd(unsigned a, unsigned b)
{
    if (PATTERN_UNLIMITED_LENGTH - a <= b)
        return PATTERN_UNLIMITED_LENGTH;
    return a+b;
}

inline unsigned limitedMult(unsigned a, unsigned b)
{
    if (a == 0)
        return 0;
    if (PATTERN_UNLIMITED_LENGTH / a <= b)
        return PATTERN_UNLIMITED_LENGTH;
    return a*b;
}

IHqlExpression * RegexIdAllocator::createKey(IHqlExpression * expr, IAtom * name)
{
    if (!expr)
        return NULL;
    IHqlExpression * body = expr->queryBody();
    if (name)
        return createSymbol(createIdAtom(name->str()), LINK(body), ob_private);
    return LINK(body);
}

void RegexIdAllocator::setID(IHqlExpression * expr, IAtom * name, unsigned value)
{
    OwnedHqlExpr key = createKey(expr, name);
    map.setValue(key, value);
}

regexid_t RegexIdAllocator::queryID(IHqlExpression * expr, IAtom * name)
{
    OwnedHqlExpr key = createKey(expr, name);
    regexid_t * match = map.getValue(key);
    if (match)
        return *match;
    map.setValue(key, ++nextId);
    return nextId;
}


int compareUnsigned(unsigned * left, unsigned * right) 
{ 
    return (*left < *right) ? -1 : (*left > *right) ? +1 : 0; 
}


class SymbolArray
{
friend class SymbolArrayIterator;
public:
    void addUniqueRange(unsigned low, unsigned high)
    {
        unsigned max = symbols.ordinality();
        unsigned i;
        for (i = 0; i < max; i++)
        {
            unsigned cur = symbols.item(i);
            if (cur == low)
                low++;
            if (cur > low)
            {
                if (cur > high)
                    cur = high+1;
                while (low != cur)
                    symbols.add(low++, i++);
                low++;
            }
            if (cur > high)
                return;
        }
        while (low <= high)
            symbols.append(low++);
    }

    inline void addUnique(unsigned value)
    {
        bool isNew;
        symbols.bAdd(value, compareUnsigned, isNew);
    }

protected:
    UnsignedArray symbols;
};

class SymbolArrayIterator
{
public:
    SymbolArrayIterator(SymbolArray & _table) : table(_table) { cur = 0; }
    inline bool first() 
    { 
        cur = 0; return table.symbols.isItem(cur);
    }
    inline bool isValid()
    {
        return table.symbols.isItem(cur);
    }
    inline unsigned get()
    {
        return table.symbols.item(cur);
    }
    inline bool next()
    {
        cur++; return table.symbols.isItem(cur);
    }
protected:
    SymbolArray & table;
    unsigned cur;
};

//---------------------------------------------------------------------------

HqlNamedRegex::HqlNamedRegex(IHqlExpression * _expr, IAtom * _name, IHqlExpression * _searchExpr, node_operator _kind, bool _caseSensitive, bool _isMatched)
{ 
    kind = _kind;
    expr.set(_expr); 
    searchExpr.set(_searchExpr);
    name = _name;
    numUses = 1;
    isMatched = _isMatched;
    isRecursive = false;
    doneCalcDfaScore = false;
    doneCreateDFA = false;
    doneExpandNamed = false;
    doneExpandRecursion = false;
    doneMarkDfas = false;
    noGenerate = (kind == no_pat_dfa);
    caseSensitive = _caseSensitive;
}

HqlNamedRegex::~HqlNamedRegex() 
{ 
    cleanup();
}


void HqlNamedRegex::addBeginEnd(const ParseInformation & options)
{
    //Add nodes at either end of the pattern - at the start as a placeholder for all the possible first patterns,
    //and at the end to mark which paths are terminal.
    HqlRegexExpr * follow = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);
    follow->addOperand(createRegexExpr(options, isRecursive ? no_pat_beginrecursive : no_pat_beginpattern, NULL, caseSensitive));
    follow->addOperand(LINK(def));
    if (isRecursive)
        follow->addOperand(createRegexExpr(options, no_pat_endrecursive, NULL, caseSensitive));
    follow->addOperand(createRegexExpr(options, no_pat_endpattern, NULL, caseSensitive));
    def.setown(follow);
}

void HqlNamedRegex::analyseNullLeadTrail()
{
    def->analyseNullLeadTrail();
    limit = def->limit;
}

void HqlNamedRegex::calcDfaScore()
{
    if (!doneCalcDfaScore)
    {
        doneCalcDfaScore = true;
        def->calcDfaScore();
    }
}

void HqlNamedRegex::calculateNext()
{
    def->calculateNext();
}

bool HqlNamedRegex::canBeNull()
{
    return limit.canBeNull();
}

void HqlNamedRegex::cleanup()
{
    if (def)
        def->cleanup();
    if (created)
        created->dispose();
}

void HqlNamedRegex::createDFAs(RegexContext & ctx)
{
    if (!doneCreateDFA)
    {
        doneCreateDFA = true;
        def.setown(def->createDFAs(ctx));
    }
}


void HqlNamedRegex::expandNamedSymbols()
{
    if (!doneExpandNamed)
    {
        doneExpandNamed = true;
        def.setown(def->expandNamedSymbols());
    }
}

void HqlNamedRegex::expandRecursion(RegexContext & ctx, HqlNamedRegex * self)
{
    if (!doneExpandRecursion)
    {
        doneExpandRecursion = true;
        if (kind == no_pat_instance)
            self = this;
        def.setown(def->expandRecursion(ctx, self));
    }
}

void HqlNamedRegex::generateDFAs()
{
    if (!noGenerate)
        def->generateDFAs();
}

void HqlNamedRegex::generateRegex(GenerateRegexCtx & ctx)
{
    if (created || noGenerate)
        return;

    node_operator savedKind = ctx.namedKind;
    ctx.namedKind = kind;
    created.setown(new RegexNamed(name, ctx.idAllocator.queryID(expr, name)));
    def->generateRegex(ctx);
    def->connectRegex();

    HqlRegexExpr * first = def->queryChild(0);
    if ((first->getOperator() == no_pat_beginpattern) && (first->following.ordinality() == 1))
        first = &first->following.item(0);
    created->setFirst(first->created);
    ctx.namedKind = savedKind;
}


unsigned HqlNamedRegex::getDfaScore()
{
    if (isRecursive)
        return NO_DFA_SCORE;
    return def->dfaScore;
}

void HqlNamedRegex::insertSeparators(IHqlExpression * separator, RegexContext * ctx)
{
    if (expr->queryType()->getTypeCode() == type_pattern)
        return;
    def.setown(def->insertSeparators(separator, ctx));
}

void HqlNamedRegex::markDFAs(unsigned complexity)
{
    if (!doneMarkDfas)
    {
        doneMarkDfas = true;
        def->markDFAs(complexity);
    }
}

bool HqlNamedRegex::matchesDefine(IHqlExpression * name, bool _caseSensitive)
{
    return (name->queryBody() == defineName) && (caseSensitive == _caseSensitive);
}

void HqlNamedRegex::mergeCreateSets()
{
    def.setown(def->mergeCreateSets());
}

bool HqlNamedRegex::queryExpandInline()
{
    expandNamedSymbols();
    if (!isMatched)
    {
        if (numUses == 1)
            return true;

        //expand if trivial - this should be improved once dfa support is implemented.
        switch (def->getOperator())
        {
        case no_pat_instance:
        case no_pat_set:
        case no_pat_const:
        case no_pat_first:
        case no_pat_last:
        case no_pat_anychar:
            return true;
        }
    }
    return false;
}

RegexPattern * HqlNamedRegex::queryRootPattern()                        
{ 
    //return a reference to the begin 
    HqlRegexExpr * first = def->queryChild(0);
    if ((first->getOperator() == no_pat_beginpattern) && (first->following.ordinality() == 1))
        first = &first->following.item(0);
    return first->created;
}

void HqlNamedRegex::setRegexOwn(HqlRegexExpr * _def)    
{ 
    def.setown(_def); 
    IHqlExpression * defExpr = def->expr;
    if (def->getOperator() == no_define)
        defineName.set(defExpr->queryChild(1)->queryBody());
}

//---------------------------------------------------------------------------

static HqlRegexExpr * expandStringAsChars(IHqlExpression * expr, const ParseInformation & options, bool caseSensitive)
{
    Owned<IValue> castValue = getCastConstant(expr->queryChild(0)->queryValue(), options.type);

    //convert text strings into a sequence of characters...
    ITypeInfo * type = castValue->queryType();
    unsigned len = type->getStringLen();
    if (len == 0)
        return createRegexExpr(options, no_null, NULL, caseSensitive);

    HqlRegexExpr * expanded = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);

    bool readUChar = false;
    const void * value = castValue->queryValue();
    switch (options.type)
    {
    case type_unicode:
        readUChar = true;
        break;
    case type_utf8:
        //UTF8 is matched as a sequence of characters...
        len = rtlUtf8Size(len, value);
        break;
    }

    for (unsigned i = 0; i < len; i++)
    {
        unsigned nextValue;
        if (readUChar)
            nextValue = ((UChar *)value)[i];
        else
            nextValue = ((unsigned char *)value)[i];
        OwnedHqlExpr next = getSizetConstant(nextValue);    
        expanded->addOperand(createRegexExpr(options, no_pat_singlechar, next, caseSensitive));
    }
    return expanded;
}

//---------------------------------------------------------------------------

HqlRegexExpr::HqlRegexExpr(const ParseInformation & _options, IHqlExpression * _expr, bool _caseSensitive) : options(_options)
{ 
    expr.set(_expr);
    op = expr->getOperator(); 
    caseSensitive = _caseSensitive;
    init();
}

HqlRegexExpr::HqlRegexExpr(const ParseInformation & _options, node_operator _op, IHqlExpression * _expr, bool _caseSensitive) : options(_options)
{ 
    expr.set(_expr);
    op = _op;
    caseSensitive = _caseSensitive;
    init();
}

HqlRegexExpr::~HqlRegexExpr() 
{
    ::Release(dfa);
}

void HqlRegexExpr::init()
{
    uid = (unsigned)(getUniqueId()-options.uidBase);
    cleaned = false;
    connected = false;
    analysed = false;
    createDFA = false;
    dfaScore = NO_DFA_SCORE;
    dfa = NULL;
    added = false;
}

void inherit(HqlRegexUniqueArray & target, const HqlRegexUniqueArray & source)
{
    ForEachItemIn(idx, source)
        target.append(OLINK(source.item(idx)));
}

void HqlRegexExpr::calcDfaScore()
{
    ForEachItemIn(idx, args)
        args.item(idx).calcDfaScore();
    if (querySubPattern())
        querySubPattern()->calcDfaScore();
    if (queryNamed())
        queryNamed()->calcDfaScore();
    //on entry dfaScore = NO_DFA_SCORE
    switch (getOperator())
    {
    case no_null:
        dfaScore = 0;
        break;
    case no_pat_const:
        dfaScore = expr->queryChild(0)->queryType()->getStringLen();
        break;
    case no_pat_anychar:
        dfaScore = 1;
        break;
    case no_pat_set:
        {
            unsigned score = 0;
            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                switch (child->getOperator())
                {
                case no_range:
                    {
                        unsigned low = (unsigned)child->queryChild(0)->queryValue()->getIntValue();
                        unsigned high = (unsigned)child->queryChild(1)->queryValue()->getIntValue();
                        score = addWithoutOverflow(score, (high-low)+1);
                        break;
                    }
                case no_constant:
                    score = addWithoutOverflow(score, 1);
                    break;
                }
            }
            dfaScore = score;
            break;
        }
    case no_pat_repeat:
        if (isStandardRepeat())
        {
            unsigned argScore = args.item(0).dfaScore;
            if ((getRepeatMax() > 1) && (argScore != NO_DFA_SCORE))
                dfaScore = addWithoutOverflow(argScore, argScore);
            else
                dfaScore = argScore;
        }
        else if (!expr->hasProperty(minimalAtom))
        {
            unsigned max = getRepeatMax();
            unsigned namedDfaScore = queryNamed()->getDfaScore();
            if (max <= options.dfaRepeatMax)
                dfaScore = multiplyWithoutOverflow(max, namedDfaScore);
        }
        break;
    case no_pat_instance:
        if (!queryNamed()->isMatched)
            dfaScore = queryNamed()->getDfaScore();
        break;
    case no_pat_or:
        {
            unsigned totalScore = 0;
            ForEachItemIn(idx, args)
            {
                unsigned score = args.item(idx).dfaScore;
                if (score == NO_DFA_SCORE)
                {
                    totalScore = NO_DFA_SCORE;
                    break;
                }
                totalScore = addWithoutOverflow(totalScore, score);
            }
            dfaScore = totalScore;

            if (dfaScore == NO_DFA_SCORE)
            {
                //Work out if there are two or more entires which can be converted into a dfa, and if so combine them
                //Also potentially separate simple sting lists into a separate or, so we always generate them
                unsigned scoreCount = 0;
                unsigned stringListCount = 0;
                ForEachItemIn(iCount, args)
                {
                    HqlRegexExpr & cur = args.item(iCount);
                    if (cur.dfaScore != NO_DFA_SCORE)
                    {
                        scoreCount++;
                        if (cur.isSimpleStringList())
                            stringListCount++;
                    }
                }

                if (scoreCount > 1)
                {
                    HqlRegexExpr * newor = createRegexExpr(options, no_pat_or, NULL, caseSensitive);
                    HqlRegexExpr * stringListOr = ((stringListCount > 1) && (scoreCount != stringListCount)) ? createRegexExpr(options, no_pat_or, NULL, caseSensitive) : NULL;
                    unsigned totalScore = 0;
                    unsigned stringListScore = 0;
                    for (unsigned i =0; i < args.ordinality(); )
                    {
                        HqlRegexExpr & cur = args.item(i);
                        unsigned score = cur.dfaScore;
                        if (score != NO_DFA_SCORE)
                        {
                            if (stringListOr && cur.isSimpleStringList())
                            {
                                stringListScore = addWithoutOverflow(stringListScore, score);
                                stringListOr->addOperand(&OLINK(cur));
                            }
                            else
                                newor->addOperand(&OLINK(cur));

                            totalScore = addWithoutOverflow(totalScore, score);
                            args.remove(i);
                        }
                        else
                            i++;
                    }
                    if (stringListOr)
                    {
                        stringListOr->dfaScore = stringListScore;
                        newor->addOperand(stringListOr);
                    }
                    newor->dfaScore = totalScore;
                    //add the dfa first - since it will probably be quickest
                    args.add(*newor, 0);
                }
            }
            break;
        }
    case no_pat_follow:
        {
            //MORE: Should extract applicable subranges out of the sequences
            //MORE: This should multiple rather than add.
            dfaScore = 0;
            ForEachItemIn(idx, args)
            {
                unsigned score = args.item(idx).dfaScore;
                if (score == NO_DFA_SCORE)
                {
                    dfaScore = NO_DFA_SCORE;
                    break;
                }
                dfaScore = addWithoutOverflow(dfaScore, score);
            }
            if (dfaScore == NO_DFA_SCORE)
            {
                //Try and find ranges that are valid, and extract those into sub-elements.
                for (unsigned i =0; i < args.ordinality(); i++)
                {
                    unsigned score = args.item(i).dfaScore;
                    if (score != NO_DFA_SCORE)
                    {
                        unsigned max = args.ordinality();
                        unsigned j;
                        for (j=i+1; j < max; j++)
                        {
                            unsigned thisScore = args.item(j).dfaScore;
                            if (thisScore == NO_DFA_SCORE)
                                break;
                            score = addWithoutOverflow(score, thisScore);
                        }

                        if (j != i+1)
                        {
                            HqlRegexExpr * follow = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);
                            for (unsigned k=i; k < j; k++)
                                follow->addOperand(&OLINK(args.item(k)));
                            follow->dfaScore = score;
                            while (j != i)
                                args.remove(--j);
                            args.add(*follow, i);
                        }
                    }
                }
            }
            break;
        }
    }
}



void HqlRegexExpr::calculateNext()
{
    //Dragon p138
    //if a concatenation (a,b), then all the trailing from (a) have leading(b) in their following set
    //if a repeat [max > 1] then all positions in leading(x) are also in the following list
    //Limited/minimal repeats aren't done the same way - so they aren't added in the same way
    //Because I am cheating the order is important - repeats need to be done first since they are greedy.
    switch (getOperator())
    {
    case no_pat_repeat:
        if (isStandardRepeat() && getRepeatMax() > 1)
        {
            //All the trailing elements have the leading elements as possible next.
            unsigned max = numTrailing();
            for (unsigned idx=0; idx < max; idx++)
                gatherLeading(queryTrailing(idx).following);
        }
        break;
    }

    ForEachItemIn(idx, args)
        args.item(idx).calculateNext();

    switch (getOperator())
    {
    case no_pat_follow:
    case no_pat_separator:
        {
            //Internally connect up following items within the no_pat_follow.
            unsigned max = args.ordinality();
            assertex(max);
            for (unsigned pairs=0; pairs<max-1; pairs++)
            {
                HqlRegexExpr * left = queryChild(pairs);
                for (unsigned other=pairs+1; other<max; other++)
                {
                    HqlRegexExpr * right = queryChild(other);
                    unsigned maxTrail = left->numTrailing();
                    for (unsigned trailIdx=0; trailIdx<maxTrail; trailIdx++)
                    {
                        HqlRegexExpr & cur = left->queryTrailing(trailIdx);
                        right->gatherLeading(cur.following);
                    }
                    if (!right->canBeNull())
                        break;
                }
            }
            break;
        }
    }
}

#if 0
//convert strings/unicode to correct kind depending on what is being searched, when the expr is built.
UBool U_CALL_CONV noteUCharRange(const void * context, UChar32 start, UChar limit, UCharCategory type)
{
}

void gatherRange(void * context, UCharCategory type)
{
    u_enumCharTypes(noteUCharRange range, context);
}
#endif

static bool canConsume(const ParseInformation & options, unsigned nextChar, unsigned matcherChar, bool caseSensitive)
{
    if (nextChar == matcherChar)
        return true;
    if (matcherChar & RegexSpecialMask)
    {
        assertex(options.type != type_utf8);
        if (!(nextChar & RegexSpecialMask))
        {
            if (options.type != type_string)
                return isUnicodeMatch(matcherChar, nextChar);
            else
                return isAsciiMatch(matcherChar, nextChar);
        }

        //Should only occur if unicode.
        switch (matcherChar)
        {
        case RCCalnum: 
            return (nextChar == RCCalpha) || (nextChar == RCClower) || (nextChar == RCCupper) || (nextChar == RCCdigit);
        case RCCalpha:
            return (nextChar == RCClower) || (nextChar == RCCupper);
        case RCCspace: 
            return (nextChar == RCCblank);
        case RCCprint: 
            return (nextChar == RCCalnum) || (nextChar == RCCalpha) || (nextChar == RCClower) || (nextChar == RCCupper) || (nextChar == RCCdigit) || (nextChar == RCCpunct) || (nextChar == RCCxdigit) || (nextChar == RCCgraph);
        case RCCgraph: 
            return (nextChar == RCCalnum) || (nextChar == RCCalpha) || (nextChar == RCClower) || (nextChar == RCCupper) || (nextChar == RCCdigit) || (nextChar == RCCpunct) || (nextChar == RCCxdigit);
        case RCCxdigit: 
            return (nextChar == RCCdigit);
        case RCCany:
            return true;
        }
        return false;
    }
    else if (nextChar & RegexSpecialMask)
        return false;

    if (caseSensitive || (options.type == type_utf8))
        return false;

    if (options.type == type_unicode)
    {
        //MORE: This needs improving for full folding.
        return u_foldCase(nextChar, U_FOLD_CASE_DEFAULT) == u_foldCase(matcherChar, U_FOLD_CASE_DEFAULT);
    }

    return (nextChar == tolower(matcherChar)) || (nextChar == toupper(matcherChar));
}

bool HqlRegexExpr::canConsume(unsigned nextChar)
{
    switch (getOperator())
    {
    case no_pat_singlechar:
        return ::canConsume(options, nextChar, (unsigned)expr->queryValue()->getIntValue(), caseSensitive);
    case no_pat_utf8single:
        assertex(!(nextChar & RegexSpecialMask));
        return nextChar < 0x80;
    case no_pat_utf8lead:
        assertex(!(nextChar & RegexSpecialMask));
        return nextChar >= 0xc0;
    case no_pat_utf8follow:
        assertex(!(nextChar & RegexSpecialMask));
        return nextChar >= 0x80 && nextChar <= 0xbf;
    case no_pat_anychar:
        assertex(options.type != type_utf8);
        return true;
    case no_pat_set:
        {
            assertex(options.type != type_utf8);
            bool invert = expr->hasProperty(notAtom);
            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                switch (child->getOperator())
                {
                case no_range:
                    {
                        unsigned low = (unsigned)child->queryChild(0)->queryValue()->getIntValue();
                        unsigned high = (unsigned)child->queryChild(1)->queryValue()->getIntValue();

                        if (!(nextChar & RegexSpecialMask))
                        {
                            if (!caseSensitive)
                            {
                                if (options.type == type_unicode)
                                {
                                    //MORE: Improved unicode
                                    if (u_foldCase(nextChar, U_FOLD_CASE_DEFAULT) >= u_foldCase(low, U_FOLD_CASE_DEFAULT) &&
                                        u_foldCase(nextChar, U_FOLD_CASE_DEFAULT) <= u_foldCase(high, U_FOLD_CASE_DEFAULT))
                                        return !invert;
                                }
                                else
                                {
                                    if (nextChar >= (unsigned)tolower(low) && nextChar <= (unsigned)tolower(high))
                                        return !invert;
                                    if (nextChar >= (unsigned)toupper(low) && nextChar <= (unsigned)toupper(high))
                                        return !invert;
                                }
                            }
                            else
                            {
                                if (nextChar >= low && nextChar <= high)
                                    return !invert;
                            }
                        }
                        break;
                    }
                case no_constant:
                    {
                        unsigned value = (unsigned)child->queryValue()->getIntValue();
                        if (::canConsume(options, nextChar, value, caseSensitive))
                            return !invert;
                        break;
                    }
                case no_attr:
                case no_attr_expr:
                case no_attr_link:
                    break;
                default:
                    UNIMPLEMENTED;
                }
            }
            return invert;
        }
    case no_pat_endpattern:
        return false;
    default:
        UNIMPLEMENTED;
    }
}

void HqlRegexExpr::gatherConsumeSymbols(SymbolArray & symbols)
{
    switch (getOperator())
    {
    case no_pat_singlechar:
        {
            unsigned charValue = (unsigned)expr->queryValue()->getIntValue();
            if (!caseSensitive && (options.type != type_utf8))
            {
                if (options.type == type_unicode)
                {
                    //MORE: Unicode - can you have several values, what about case conversion?
                    //MORE: String might need expanding to ('a'|'A')('ss'|'SS'|'B') or something similar.
                    symbols.addUnique(u_tolower(charValue));
                    symbols.addUnique(u_toupper(charValue));
                }
                else
                {
                    symbols.addUnique(tolower(charValue));
                    symbols.addUnique(toupper(charValue));
                }
            }
            else
                symbols.addUnique(charValue);
            break;
        }
    case no_pat_anychar:
        assertex(options.type != type_utf8);
        if (options.type == type_unicode)
            symbols.addUnique(RCCany);
        else
            symbols.addUniqueRange(0, 255);
        return;
    case no_pat_endpattern:
        return;
    case no_pat_set:
        assertex(options.type == type_string);
        if (options.type == type_unicode)
        {
            //MORE: Need some kind of implementation for unicode - although invert and ranges may not be possible.
            if (expr->hasProperty(notAtom))
                throwError(HQLERR_DfaTooComplex);

            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                switch (child->getOperator())
                {
                case no_range:
                    {
                        unsigned low = (unsigned)child->queryChild(0)->queryValue()->getIntValue();
                        unsigned high = (unsigned)child->queryChild(1)->queryValue()->getIntValue();

                        if (!caseSensitive)
                        {
                            //MORE: This really doesn't work very well - I'm not even sure what it means...
                            symbols.addUniqueRange(u_tolower(low), u_tolower(high));
                            symbols.addUniqueRange(u_toupper(low), u_toupper(high));
                        }
                        else
                            symbols.addUniqueRange(low, high);
                        break;
                    }
                case no_constant:
                    {
                        unsigned value = (unsigned)child->queryValue()->getIntValue();
                        if (caseSensitive || (value & RegexSpecialMask))
                            symbols.addUnique(value);
                        else
                        {
                            symbols.addUnique(u_tolower(value));
                            symbols.addUnique(u_toupper(value));
                        }
                        break;
                    }
                case no_attr:
                case no_attr_expr:
                case no_attr_link:
                    break;
                default:
                    UNIMPLEMENTED;
                }
            }
        }
        else
        {
            //This is probably the easiest way to cope with inverted sets and all other complications.
            for (unsigned i = 0; i < 256; i++)
                if (canConsume(i))
                    symbols.addUnique(i);
        }
        return;
    case no_pat_utf8single:
        {
            for (unsigned i = 0; i < 0x80; i++)
                symbols.addUnique(i);
            break;
        }
    case no_pat_utf8lead:
        {
            for (unsigned i = 0xc0; i < 0x100; i++)
                symbols.addUnique(i);
            break;
        }
    case no_pat_utf8follow:
        {
            for (unsigned i = 0x80; i < 0xc0; i++)
                symbols.addUnique(i);
            break;
        }
    default:
        throwError1(HQLERR_BadDfaOperator, getOpString(getOperator()));
    }
}


void HqlRegexExpr::cleanup()
{
    if (cleaned)
        return;
    cleaned = true;
    //kill anything that could have a loop.
    ForEachItemIn(i0, args)
        args.item(i0).cleanup();
    if (querySubPattern())
        querySubPattern()->cleanup();
    if (created)
        created->dispose();
    following.kill();
    args.kill();
}

HqlRegexExpr * HqlRegexExpr::clone()
{
    assertex(!created && !analysed);
    HqlRegexExpr * cloned = createRegexExpr(options, op, expr, caseSensitive);
    ForEachItemIn(idx, args)
        cloned->args.append(*args.item(idx).clone());
    cloned->setNamed(queryNamed());
    cloned->setSubPattern(querySubPattern());
    return cloned;
}

void HqlRegexExpr::connectRegex()
{
    if (connected)
        return;
    connected = true;
    ForEachItemIn(idx, args)
        args.item(idx).connectRegex();
    if (created)
    {
        ForEachItemIn(idx2, following)
            created->addLink(following.item(idx2).created);

        HqlNamedRegex * named = queryNamed();
        if (named && named->created && !named->noGenerate)
            created->setBody(named->created);
        if (querySubPattern())
            created->setSubPattern(querySubPattern()->queryRootPattern());
    }
}

HqlRegexExpr * HqlRegexExpr::createDFAs(RegexContext & ctx)
{
    if (createDFA)
    {
        OwnedHqlExpr searchExpr = createValue(no_pat_dfa, makePatternType(), createUniqueId());

        HqlNamedRegex * newNamed = new HqlNamedRegex(searchExpr, NULL, searchExpr, no_pat_dfa, caseSensitive, false);
        ctx.named.append(*newNamed);
        newNamed->setRegexOwn(expandAsDFA());

        HqlRegexExpr * dfa = createRegexExpr(options, no_pat_dfa, NULL, caseSensitive);
        dfa->setNamed(newNamed);
        dfa->dfaScore = dfaScore;
        //MORE: Need to save the original as a *named/subPattern* attribute in case conversion to a DFA fails
        return dfa;
    }

    unsigned max = args.ordinality();
    unsigned idx;
    for (idx = 0; idx < max; idx++)
    {
        HqlRegexExpr & cur = args.item(idx);
        HqlRegexExpr * transformed = cur.createDFAs(ctx);
        args.replace(*transformed, idx);
        if (transformed->getOperator() == no_pat_dfa)
        {
            if ((idx != 0) && (idx+1 != max) && (getOperator() == no_pat_follow))
            {
                if ((args.item(idx-1).getOperator() == no_pat_begintoken) &&
                    (args.item(idx+1).getOperator() == no_pat_endtoken))
                {
                    transformed->addOperand(&OLINK(args.item(idx-1)));
                    args.remove(idx+1);
                    args.remove(idx-1);
                    idx--;
                    max -= 2;
                }
            }
        }
    }
    if (querySubPattern())
        querySubPattern()->createDFAs(ctx);
    if (queryNamed())
        queryNamed()->createDFAs(ctx);
    return LINK(this);
}

HqlRegexExpr * HqlRegexExpr::expandAsRepeatedDFA(unsigned count)
{
    if (count == 0)
        return createRegexExpr(options, no_null, NULL, caseSensitive);
    if (count == 1)
        return expandAsDFA();

    HqlRegexExpr * cloned = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);
    for (unsigned i=0; i < count; i++)
        cloned->args.append(*expandAsDFA());
    return cloned;
}

HqlRegexExpr * HqlRegexExpr::expandAsDFA()
{
    assertex(!created && !analysed);
    switch (getOperator())
    {
    case no_pat_const:
        return expandStringAsChars(expr, options, caseSensitive);
    case no_pat_instance:
        return queryNamed()->def->expandAsDFA();
    case no_pat_or:
    case no_pat_follow:
    case no_null:
    case no_pat_singlechar:
        break;
    case no_pat_anychar:
        if (options.type == type_utf8)
        {
            // convert to low | follow(lead, tail+);
            Owned<HqlRegexExpr> orRegexExpr = createRegexExpr(options, no_pat_or, NULL, caseSensitive);
            orRegexExpr->addOperand(createRegexExpr(options, no_pat_utf8single, NULL, caseSensitive));
            Owned<HqlRegexExpr> followRegexExpr = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);
            followRegexExpr->addOperand(createRegexExpr(options, no_pat_utf8lead, NULL, caseSensitive));
            
            OwnedHqlExpr trailExpr = createValue(no_pat_utf8follow, makePatternType());
            Owned<HqlRegexExpr> trailRegexExpr = createRegexExpr(options, no_pat_utf8follow, NULL, caseSensitive);

            OwnedHqlExpr repeatExpr = createValue(no_pat_repeat, makePatternType(), LINK(trailExpr), createConstantOne(), createValue(no_any, makeNullType()));
            Owned<HqlRegexExpr> repeatRegexExpr = createRegexExpr(options, no_pat_repeat, repeatExpr, caseSensitive);
            repeatRegexExpr->addOperand(trailRegexExpr.getClear());
            followRegexExpr->addOperand(repeatRegexExpr.getClear());
            orRegexExpr->addOperand(followRegexExpr.getClear());
            return orRegexExpr.getClear();
        }
        break;
    case no_pat_set:
        if (options.type == type_utf8)
        {
            throwUnexpected();
            //expand set values...
//          return low | follow(lead, tail+);
        }
        break;
    case no_pat_dfa:
        return queryNamed()->def->expandAsDFA();
    case no_pat_repeat:
        {
            if (isStandardRepeat())
                break;
            unsigned min = getRepeatMin();
            unsigned max = getRepeatMax();
            if (min == max)
                return queryNamed()->def->expandAsRepeatedDFA(min);
            HqlRegexExpr * cloned = createRegexExpr(options, no_pat_or, NULL, caseSensitive);
            for (unsigned i=min; i <= max; i++)
                cloned->args.append(*queryNamed()->def->expandAsRepeatedDFA(i));
            return cloned;
        }
    default:
        UNIMPLEMENTED;
    }

    HqlRegexExpr * cloned = createRegexExpr(options, op, expr, caseSensitive);
    ForEachItemIn(idx, args)
        cloned->args.append(*args.item(idx).expandAsDFA());
    assertex(!queryNamed());
    assertex(!querySubPattern());
    return cloned;
}


HqlRegexExpr * HqlRegexExpr::expandNamedSymbols()
{
    ForEachItemIn(idx, args)
        args.replace(*args.item(idx).expandNamedSymbols(), idx);
    if (querySubPattern())
        querySubPattern()->expandNamedSymbols();
    if (queryNamed())
        queryNamed()->expandNamedSymbols();

    switch (getOperator())
    {
    case no_pat_instance:
        if (queryNamed()->queryExpandInline())
        {
            queryNamed()->removeUse();
            return queryNamed()->def->clone();
        }
        break;
    case no_pat_follow:
        {
            ForEachItemIn(idx, args)
            {
                node_operator op = args.item(idx).getOperator();
                switch (op)
                {
                    //Remove begin_token/end_token around the following, otherwise 
                    //we might get too many separators which may be non-optional
                    case no_pat_first:
                    case no_pat_last:
                    case no_penalty:
                    case no_pat_x_before_y:
                    case no_pat_before_y:
                    case no_pat_x_after_y:
                    case no_pat_after_y:
                    case no_pat_guard:
                        if ((idx > 0) && args.item(idx-1).getOperator() == no_pat_begintoken)
                        {
                            assertex(args.item(idx+1).getOperator() == no_pat_endtoken);
                            args.remove(idx+1);
                            args.remove(idx-1);
                            if (args.isItem(idx) && (args.item(idx).getOperator() == no_pat_separator))
                                args.remove(idx);
                        }
                        break;
                }
            }
            break;
        }
    }
    return LINK(this);
}

HqlRegexExpr * HqlRegexExpr::expandRecursion(RegexContext & ctx, HqlNamedRegex * self)
{
    ForEachItemIn(idx, args)
        args.replace(*args.item(idx).expandRecursion(ctx, self), idx);
    if (querySubPattern())
        querySubPattern()->expandRecursion(ctx, self);
    if (queryNamed())
        queryNamed()->expandRecursion(ctx, self);

    HqlNamedRegex * replacement = NULL;
    switch (getOperator())
    {
    case no_self:
        replacement = self;
        break;
    case no_pat_use:
        assertex(expr->queryChild(0)->queryValue());
        replacement = ctx.queryDefine(expr->queryChild(0), caseSensitive);
        break;
    case no_define:
        return LINK(&args.item(0));
    }
    if (replacement)
    {
        replacement->expandRecursion(ctx, self);
        replacement->addRecursiveUse();
        HqlRegexExpr * instance = createRegexExpr(options, no_pat_instance, NULL, caseSensitive);
        instance->setNamed(replacement);
        return instance;
    }
    return LINK(this);
}

void HqlRegexExpr::generateDFA(IDfaPattern * pattern)
{
    //Estimate the number of transitions to avoid lots of reallocs.
    unsigned numTransitions = 0;
    {
        ForEachItemIn(idx, dfa->states)
        {
            HqlDfaState & cur = dfa->states.item(idx);
            unsigned min = 255;
            unsigned max = 0;
            ForEachItemIn(idx2, cur.transitions)
            {
                HqlDfaTransition & curTransition = cur.transitions.item(idx2);
                if (curTransition.code < min) min = curTransition.code;
                if (curTransition.code > max) max = curTransition.code;
            }
            if (min <= max)
                numTransitions += (max-min)+1;
        }
    }
    pattern->init(dfa->states.ordinality(), numTransitions);
    ForEachItemIn(idx, dfa->states)
    {
        HqlDfaState & cur = dfa->states.item(idx);
        assertex(cur.id == idx);
        pattern->beginState(cur.id);
        ForEachItemIn(idx, cur.position)
        {
            HqlRegexExpr & curExpr = cur.position.item(idx);
            if (curExpr.getOperator() == no_pat_endpattern)
                pattern->setStateAccept(curExpr.getAcceptId());
        }

        ForEachItemIn(idx2, cur.transitions)
        {
            HqlDfaTransition & curTransition = cur.transitions.item(idx2);
            pattern->addTransition(curTransition.code, curTransition.next->id);
        }
        pattern->endState();
    }
    pattern->finished();
}

void HqlRegexExpr::generateRegex(GenerateRegexCtx & ctx)
{
    ForEachItemIn(idx, args)
        args.item(idx).generateRegex(ctx);
    assertex(!created);
    if (queryNamed())
        queryNamed()->generateRegex(ctx);
    if (querySubPattern())
        querySubPattern()->generateRegex(ctx);
    switch (getOperator())
    {
    case no_null:
    case no_pat_beginpattern:
        created.setown(new RegexNullPattern);
        break;
    case no_pat_beginrecursive:
        created.setown(new RegexRecursivePattern);
        break;
    case no_pat_beginseparator:
        created.setown(new RegexBeginSeparatorPattern);
        break;
    case no_pat_endseparator:
        created.setown(new RegexEndSeparatorPattern);
        break;
    case no_pat_endrecursive:
        created.setown(new RegexEndRecursivePattern);
        break;
    case no_pat_instance:
        created.setown(new RegexNamedPattern);
        break;
    case no_pat_begincheck:
    case no_pat_beginvalidate:
        created.setown(new RegexBeginCheckPattern);
        break;
    case no_pat_endcheckin:
        {
            bool stripSeparator = ctx.info.addedSeparators && (expr->queryType()->getTypeCode() != type_pattern);
            created.setown(new RegexCheckInPattern(expr->hasProperty(notAtom), stripSeparator));
            break;
        }
    case no_pat_endchecklength:
        {
            unsigned minLength = 0;
            unsigned maxLength = PATTERN_UNLIMITED_LENGTH;
            getCheckRange(expr->queryChild(1), minLength, maxLength, ctx.info.charSize);
            bool stripSeparator = ctx.info.addedSeparators && (expr->queryType()->getTypeCode() != type_pattern);
            created.setown(new RegexCheckLengthPattern(expr->hasProperty(notAtom), stripSeparator, minLength, maxLength));
            break;
        }
    case no_pat_endvalidate:
        {
            unsigned idx = ctx.regex.getValidatorIndex(expr);
            assertex(idx != NotFound);
            ValidateKind validatorKind = getValidateKind(ctx.regex.queryValidateExpr(expr));
            bool stripSeparator = ctx.info.addedSeparators && (expr->queryType()->getTypeCode() != type_pattern);
            switch (ctx.info.inputFormat())
            {
            case NlpUnicode:
                if (validatorKind != ValidateIsString)
                    created.setown(new RegexValidateUniAsUniPattern(stripSeparator, idx));
                else
                    created.setown(new RegexValidateUniAsAscPattern(stripSeparator, idx));
                break;
            case NlpUtf8:
                if (validatorKind != ValidateIsString)
                    created.setown(new RegexValidateUtf8AsUniPattern(stripSeparator, idx));
                else
                    created.setown(new RegexValidateUtf8AsAscPattern(stripSeparator, idx));
                break;
            case NlpAscii:
                if (validatorKind != ValidateIsUnicode)
                    created.setown(new RegexValidateAscAsAscPattern(stripSeparator, idx));
                else
                    created.setown(new RegexValidateAscAsUniPattern(stripSeparator, idx));
                break;
            }
            break;
        }
    case no_pat_begintoken:
        created.setown(new RegexBeginTokenPattern);
        break;
    case no_pat_endtoken:
        created.setown(new RegexEndTokenPattern);
        break;
    case no_pat_x_before_y:
    case no_pat_before_y:
        created.setown(new RegexAssertNextPattern(expr->hasProperty(notAtom)));
        break;
    case no_pat_x_after_y:
    case no_pat_after_y:
        {
            unsigned minSize = limitedMult(querySubPattern()->limit.minLength, ctx.info.charSize);
            unsigned maxSize = limitedMult(querySubPattern()->limit.maxLength, ctx.info.charSize);
            created.setown(new RegexAssertPrevPattern(expr->hasProperty(notAtom), minSize, maxSize));
            break;
        }
    case no_pat_first:
        created.setown(new RegexStartPattern);
        break;
    case no_pat_last:
        created.setown(new RegexFinishPattern);
        break;
    case no_pat_anychar:
        created.setown(new RegexAnyCharPattern);
        break;
    case no_penalty:
        {
            int penalty = 1;
            if (expr->numChildren() > 0)
                penalty = (int)expr->queryChild(0)->queryValue()->getIntValue();
            created.setown(new RegexPenaltyPattern(penalty));
        }
        break;
    case no_pat_set:
        {
            RegexSetBasePattern * pattern;
            if (ctx.info.type != type_string)
                pattern = new RegexUnicodeSetPattern(caseSensitive);
            else if (caseSensitive)
                pattern = new RegexAsciiSetPattern;
            else
                pattern = new RegexAsciiISetPattern;
            created.setown(pattern);
            if (expr->hasProperty(notAtom))
                pattern->setInvert(true);
            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                switch (child->getOperator())
                {
                case no_range:
                    pattern->addRange((unsigned)child->queryChild(0)->queryValue()->getIntValue(), (unsigned)child->queryChild(1)->queryValue()->getIntValue());
                    break;
                case no_constant:
                    pattern->addRange((unsigned)child->queryValue()->getIntValue(), (unsigned)child->queryValue()->getIntValue());
                    break;
                case no_attr:
                case no_attr_expr:
                case no_attr_link:
                    break;
                default:
                    UNIMPLEMENTED;
                }
            }
            break;
        }
    case no_pat_const:
        {
            IValue * value = expr->queryChild(0)->queryValue();
            Owned<IValue> castValue = getCastConstant(value, ctx.info.type);
            unsigned len = castValue->queryType()->getStringLen();
            switch (ctx.info.type)
            {
            case type_unicode:
                {
                    const UChar * data = (const UChar *)castValue->queryValue();
                    if (caseSensitive)
                        created.setown(new RegexUnicodePattern(len, data));
                    else
                        created.setown(new RegexUnicodeIPattern(len, data));
                    break;
                }
            case type_utf8:
                {
                    const char * data = (const char *)castValue->queryValue();
                    if (caseSensitive)
                        created.setown(new RegexUtf8Pattern(len, data));
                    else
                        created.setown(new RegexUtf8IPattern(len, data));
                    break;
                }
            case type_string:
                {
                    const char * data = (const char *)castValue->queryValue();
                    if (caseSensitive)
                        created.setown(new RegexAsciiPattern(len, data));
                    else
                        created.setown(new RegexAsciiIPattern(len, data));
                    break;
                }
            }
            break;
        }
    case no_pat_or:
    case no_pat_follow:
    case no_pat_separator:
        //Nothing is generated for these....
        break;
    case no_pat_dfa:
        {
            RegexDfaPattern * pattern;
            bool isToken = false;
            HqlRegexExpr * child = queryChild(0);
            if (child && child->getOperator() == no_pat_begintoken)
                isToken = true;
            if (options.type == type_unicode)
            {
                throwUnexpected();
                pattern = new RegexUnicodeDfaPattern;
            }
            else
                pattern = new RegexAsciiDfaPattern(isToken);
            created.setown(pattern);
            Owned<IDfaPattern> builder = pattern->createBuilder();
            generateDFA(builder);
        }
        break;
    case no_pat_repeat:
        if (!isStandardRepeat())
        {
            if (expr->queryChild(0)->getOperator() == no_pat_anychar)
            {
                created.setown(new RegexRepeatAnyPattern(getRepeatMin(), getRepeatMax(), expr->hasProperty(minimalAtom)));
                queryNamed()->noGenerate = true;
            }
            else
                created.setown(new RegexRepeatPattern(getRepeatMin(), getRepeatMax(), !expr->hasProperty(minimalAtom)));
        }
        break;
    case no_pat_endpattern:
        switch (ctx.namedKind)
        {
        case no_pat_instance:
        case no_pat_repeat:
            created.setown(new RegexEndNestedPattern);
            break;
        case no_pat_checkin:
            created.setown(new RegexDonePattern);
            break;
        case no_parse:
            created.setown(new RegexDonePattern);
            break;
        default:
            UNIMPLEMENTED;
        }
        break;
    default:
        UNIMPLEMENTED;
    }
}

unsigned HqlRegexExpr::getAcceptId()
{
    //Overloaded, and a bit nasty, but will do for the moment....
    if (dfaScore == NO_DFA_SCORE)
        return 99;
    return dfaScore;
}

unsigned HqlRegexExpr::getRepeatMin()
{
    return ::getRepeatMin(expr);
}

unsigned HqlRegexExpr::getRepeatMax()
{
    return ::getRepeatMax(expr);
}

bool HqlRegexExpr::isStandardRepeat()
{
    if (::isStandardRepeat(expr))
    {
        if (options.expandRepeatAnyAsDfa || expr->queryChild(0)->getOperator() != no_pat_anychar)
            return true;
    }
    return false;
}

HqlRegexExpr * HqlRegexExpr::insertSeparators(IHqlExpression * separator, RegexContext * ctx)
{
    switch (getOperator())
    {
    case no_pat_follow:
        {
            ForEachItemInRev(idx, args)
            {
                HqlRegexExpr & cur = args.item(idx);
                if (cur.getOperator() == no_pat_endtoken)
                {
                    if (args.item(idx-1).getOperator() != no_pat_last)
                    {
                        //Separators are  slightly weird.  Because they are implicit, the whole thing should
                        //be optional (unlike a name where just the contents are optional).
                        //Should probably remove the optionality from the separator's value, and
                        //put it on the surrounding separator instead.
                        HqlRegexExpr * sepRegex = createRegexExpr(options, no_pat_separator, NULL, caseSensitive);
                        sepRegex->addOperand(createRegexExpr(options, no_pat_beginseparator, NULL, caseSensitive));
                        sepRegex->addOperand(ctx->createStructure(separator, ctx->isCaseSensitive()));
                        sepRegex->addOperand(createRegexExpr(options, no_pat_endseparator, NULL, caseSensitive));
                        args.add(*sepRegex, idx+1);
                    }
                }
            }
            break;
        }
    }

    ForEachChild(idx, this)
    {
        replaceOperand(queryChild(idx)->insertSeparators(separator, ctx), idx);
    }
    return LINK(this);
}

bool HqlRegexExpr::isWorthConvertingToDFA()
{
    switch (getOperator())
    {
    case no_pat_or: case no_pat_repeat:
    case no_pat_follow:
        return true;
    }

    ForEachItemIn(idx, args)
        if (args.item(idx).isWorthConvertingToDFA())
            return true;

    return false;
}


bool HqlRegexExpr::isSimpleStringList()
{
    switch (getOperator())
    {
    case no_pat_or:
    case no_pat_case:
    case no_pat_nocase:
        break;
    case no_pat_const:
        return true;
    default:
        return false;
    }

    ForEachItemIn(idx, args)
        if (!args.item(idx).isSimpleStringList())
            return false;

    return true;
}


void HqlRegexExpr::markDFAs(unsigned complexity)
{
    if (dfaScore != NO_DFA_SCORE)
    {
        //Don't convert instances - convert the definition so it can be reused.
        //and if not worth converting, then children won't be either.
        if (getOperator() != no_pat_instance)
        {
            if (dfaScore <= complexity)
            {
                createDFA = isWorthConvertingToDFA();
                return;
            }
            else if (isSimpleStringList())
            {
                createDFA = true;
                return;
            }
        }
    }
    ForEachItemIn(idx, args)
        args.item(idx).markDFAs(complexity);
    if (queryNamed())
        queryNamed()->markDFAs(complexity);
    if (querySubPattern())
        querySubPattern()->markDFAs(complexity);
}

HqlRegexExpr * HqlRegexExpr::mergeCreateSets()
{
    ForEachItemIn(idx, args)
        replaceOperand(args.item(idx).mergeCreateSets(), idx);

    if (getOperator() == no_pat_or)
    {
        unsigned singleCount = 0;
        unsigned setCount = 0;
        unsigned nullCount = 0;
        HqlExprArray setArgs;
        HqlRegexExprArray newArgs;

        ForEachItemIn(idx, args)
        {
            HqlRegexExpr * cur = &args.item(idx);
            bool curIsOptional = false;
            if ((cur->getOperator() == no_pat_repeat) && (cur->getRepeatMin() == 0) && (cur->getRepeatMax() == 1))
            {
                curIsOptional = true;
                cur = cur->queryChild(0);
            }
            if (cur->caseSensitive == caseSensitive)
            {
                switch (cur->getOperator())
                {
                case no_null:
                    nullCount++;
                    break;
                case no_pat_set:
                    {
                        setCount++;
                        ForEachChild(iset, cur->expr)
                        {
                            IHqlExpression * child = cur->expr->queryChild(iset);
                            if (setArgs.find(*child) == NotFound)
                                setArgs.append(*LINK(child));
                        }
                        if (curIsOptional) nullCount++;
                        break;
                    }
                case no_pat_const:
                    {
                        ITypeInfo * type = cur->expr->queryChild(0)->queryType();
                        unsigned len = type->getStringLen();
                        if (len == 0)
                            nullCount++;
                        else if (len == 1)
                        {
                            const void * value = cur->expr->queryChild(0)->queryValue()->queryValue();
                            unsigned nextValue;
                            if (type->getTypeCode() == type_utf8)
                                nextValue = rtlUtf8Char(value);
                            else if (isUnicodeType(type))
                                nextValue = *(UChar *)value;
                            else
                                nextValue = *(unsigned char *)value;
                            OwnedHqlExpr nextExpr = createConstant((int)nextValue);
                            if (setArgs.find(*nextExpr) == NotFound)
                                setArgs.append(*nextExpr.getClear());
                            singleCount++;
                            if (curIsOptional) nullCount++;
                        }
                        else
                            newArgs.append(*LINK(cur));
                        break;
                    }
                default:
                    newArgs.append(*LINK(cur));
                    break;
                }
            }
            else
                newArgs.append(*LINK(cur));
        }

        if ((setCount > 1) || (setCount == 1 && singleCount > 0) || (singleCount > 1))
        {
            OwnedHqlExpr newSetExpr = createValue(no_pat_set, makePatternType(), setArgs);
            if (nullCount)
                newSetExpr.setown(createValue(no_pat_repeat, makePatternType(), newSetExpr.getClear(), createConstant(0), createConstantOne()));
            HqlRegexExpr * newSetRegex = createRegexExpr(options, newSetExpr, caseSensitive);

            if (newArgs.ordinality() == 0)
                return newSetRegex;
            args.kill();
            ForEachItemIn(idx, newArgs)
                args.append(OLINK(newArgs.item(idx)));
            args.append(*newSetRegex);
        }
    }
    return LINK(this);
}

#if 0
HqlRegexExpr * HqlRegexExpr::removeTrivialInstances()
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_pat_instance:
        {
            IHqlExpression * def = expr->queryChild(0);
            if (!queryNamed()->isMatched && def->getOperator() == no_pat_instance)
            {
                queryNamed()->removeUse();
                return 
#if 0
            //Do this later - recursion means we can't do it yet
            //Optimization whilst building the tree.  If a named symbol is just a definition of another named symbol
            //and this named symbol isn't matched explicitly, then expand it.
            if (!isMatched(def) && def->getOperator() == no_pat_instance)
                return createStructure(def);
            else
            }
        }
    }
}
#endif
#endif

//---------------------------------------------------------------------------

void HqlSimpleRegexExpr::analyseNullLeadTrail()
{
    if (analysed)
        return;
    analysed = true;
    switch (getOperator())
    {
    case no_null:
        limit.minLength = 0;
        limit.maxLength = 0;
        break;
    case no_pat_beginrecursive:
        limit.minLength = 0;
        limit.maxLength = 0;
        limit.containsAssertion = true;
        break;
    case no_pat_beginpattern:
        limit.minLength = 0;
        limit.maxLength = 0;
        break;
    case no_pat_endpattern:
        limit.minLength = 0;
        limit.maxLength = 0;
        limit.containsAssertion = true;
        break;
    case no_pat_singlechar:
        limit.minLength = 1;
        limit.maxLength = 1;
        break;
    }
}

void HqlSimpleRegexExpr::gatherLeading(HqlRegexUniqueArray & target)
{
    switch (op)
    {
    case no_null:
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
        break;
    case no_pat_endpattern:
    case no_pat_singlechar:
        target.append(OLINK(*this));
        break;
    default:
        UNIMPLEMENTED;
    }
}

void HqlSimpleRegexExpr::gatherTrailing(HqlRegexUniqueArray & target)
{
    unsigned max = numTrailing();
    for (unsigned i = 0; i < max; i++)
        target.append(OLINK(queryTrailing(i)));
}

unsigned HqlSimpleRegexExpr::numTrailing() const
{
    switch (getOperator())
    {
    case no_null:
    case no_pat_endpattern:
        return 0;
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
    case no_pat_singlechar:
        return 1;
    default:
        UNIMPLEMENTED;
    }
}

HqlRegexExpr & HqlSimpleRegexExpr::queryTrailing(unsigned idx)
{
    switch (getOperator())
    {
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
    case no_pat_singlechar:
        if (idx == 0)
            return *this;
        break;
    }
    UNIMPLEMENTED;
}


//---------------------------------------------------------------------------

void HqlComplexRegexExpr::analyseNullLeadTrail()
{
    if (analysed)
        return;
    analysed = true;
    ForEachItemIn(idx, args)
        args.item(idx).analyseNullLeadTrail();
    if (queryNamed())
        named->analyseNullLeadTrail();
    switch (getOperator())
    {
    case no_null:
    case no_pat_beginrecursive:
    case no_pat_beginpattern:
    case no_pat_endpattern:
    case no_pat_singlechar:
        //Should be handled by simpleRegex
        UNIMPLEMENTED;
        break;
    case no_pat_instance:
        limit = named->queryLimit();
        limit.containsAssertion = true; // require us to walk it, even if internally matches nothing.
        leading.append(*LINK(this));
        trailing.append(*LINK(this));
        break;
    case no_pat_dfa:
        limit = named->queryLimit();
        limit.containsAssertion = true; // this means that it will process optional values 
        leading.append(*LINK(this));
        trailing.append(*LINK(this));
        break;
    case no_pat_endrecursive:
    case no_pat_x_before_y:
    case no_pat_before_y:
    case no_pat_x_after_y:
    case no_pat_after_y:
    case no_pat_first:
    case no_pat_last:
    case no_pat_begintoken:
    case no_pat_endtoken:
    case no_pat_begincheck:
    case no_pat_endcheckin:
    case no_pat_endchecklength:
    case no_pat_beginseparator:
    case no_pat_endseparator:
    case no_pat_beginvalidate:
    case no_pat_endvalidate:
    case no_penalty:
        limit.containsAssertion = true;
        limit.minLength = 0;
        limit.maxLength = 0;
        leading.append(*LINK(this));
        trailing.append(*LINK(this));
        break;
    case no_pat_anychar:
    case no_pat_set:
        limit.minLength = 1;
        limit.maxLength = 1;
        leading.append(*LINK(this));
        trailing.append(*LINK(this));
        break;
    case no_pat_const:
        {
            unsigned len = expr->queryChild(0)->queryType()->getStringLen();
            limit.minLength = len;
            limit.maxLength = len;
            if (len != 0)
            {
                leading.append(*LINK(this));
                trailing.append(*LINK(this));
            }
        }
        break;
    case no_pat_or:
        {
            limit.minLength = PATTERN_UNLIMITED_LENGTH;
            limit.maxLength = 0;
            limit.containsAssertion = true;

            ForEachItemIn(idx, args)
            {
                HqlRegexExpr & cur = args.item(idx);
                if (cur.limit.minLength < limit.minLength)
                    limit.minLength = cur.limit.minLength;
                if (cur.limit.maxLength > limit.maxLength)
                    limit.maxLength = cur.limit.maxLength;
                if (cur.limit.canBeNull())
                    limit.containsAssertion = false;
                cur.gatherLeading(leading);
                cur.gatherTrailing(trailing);
            }
            break;
        }
    case no_pat_separator:
        {
            assertex(args.ordinality() == 3);
            limit = args.item(1).limit;
            leading.append(OLINK(args.item(0)));
            trailing.append(OLINK(args.item(2)));
            break;
        }
    case no_pat_follow:
        {
            limit.minLength = 0;
            limit.maxLength = 0;
            ForEachItemIn(idx0, args)
            {
                HqlRegexExpr & cur = args.item(idx0);
                limit.minLength = limitedAdd(limit.minLength, cur.limit.minLength);
                limit.maxLength = limitedAdd(limit.maxLength, cur.limit.maxLength);
                if (cur.limit.containsAssertion)
                    limit.containsAssertion = true;
            }
            ForEachItemIn(idx1, args)
            {
                HqlRegexExpr & cur = args.item(idx1);
                cur.gatherLeading(leading);
                if (!cur.canBeNull())
                    break;
            }
            ForEachItemInRev(idx2, args)
            {
                HqlRegexExpr & cur = args.item(idx2);
                cur.gatherTrailing(trailing);
                if (!cur.canBeNull())
                    break;
            }
            break;
        }
    case no_pat_repeat:
        {
            if (isStandardRepeat())
            {
                HqlRegexExpr & arg = args.item(0);
                limit.minLength = limitedMult(getRepeatMin(), arg.limit.minLength);
                limit.maxLength = limitedMult(getRepeatMax(), arg.limit.maxLength);
                if (getRepeatMin() != 0) limit.containsAssertion = arg.limit.containsAssertion;
                arg.gatherLeading(leading);
                arg.gatherTrailing(trailing);
            }
            else
            {
                limit.minLength = limitedMult(getRepeatMin(), named->getMinLength());
                limit.maxLength = limitedMult(getRepeatMax(), named->getMaxLength());
                //if (getRepeatMin() != 0) limit.containsAssertion = named->queryLimit().containsAssertion;
                limit.containsAssertion = true;
                leading.append(*LINK(this));
                trailing.append(*LINK(this));
            }
            break;
        }
    case no_pat_pattern:
        // Should have been converted by the time we get here...
    default:
        UNIMPLEMENTED;
        break;
    }
}


void HqlComplexRegexExpr::cleanup()
{
    if (cleaned)
        return;
    HqlRegexExpr::cleanup();
    trailing.kill();
    leading.kill();
    subPattern.clear();
}

void HqlComplexRegexExpr::gatherLeading(HqlRegexUniqueArray & target)       
{ 
    inherit(target, leading);
}

void HqlComplexRegexExpr::gatherTrailing(HqlRegexUniqueArray & target)      
{ 
    inherit(target, trailing);
}


//---------------------------------------------------------------------------
// DFA helper classes.

inline int compareHqlRegexExpr(HqlRegexExpr * left, HqlRegexExpr * right) 
{ 
    unique_id_t idl = left->queryUid();
    unique_id_t idr = right->queryUid();
    return (idl < idr) ? -1 : (idl > idr) ? +1 : 0; 
}

int compareHqlRegexExpr(CInterface * * left, CInterface * * right) 
{ 
    return compareHqlRegexExpr((HqlRegexExpr *)*left, (HqlRegexExpr *)*right);
}


void HqlRegexExprSet::add(HqlRegexExpr * value)
{
    bool isNew;
    CInterface * castValue = value;
    values.bAdd(castValue, compareHqlRegexExpr, isNew);
}


int HqlRegexExprSet::compare(const HqlRegexExprSet & other) const
{
    unsigned numThis = values.ordinality();
    unsigned numOther = other.values.ordinality();
    unsigned numCommon = std::min(numThis, numOther);
    for (unsigned i = 0; i < numCommon; i++)
    {
        HqlRegexExpr & left = values.item(i);
        HqlRegexExpr & right = other.values.item(i);
        int c = compareHqlRegexExpr(&left, &right);
        if (c)
            return c;
    }
    if (numThis > numOther)
        return +1;
    else if (numThis < numOther)
        return -1;
    else
        return 0;
}

bool HqlRegexExprSet::equals(const HqlRegexExprSet & other) const
{
    unsigned numThis = values.ordinality();
    unsigned numOther = other.values.ordinality();
    if (numThis != numOther)
        return false;
    for (unsigned i = 0; i < numThis; i++)
    {
        if (&values.item(i) != &other.values.item(i))
            return false;
    }
    return true;
}



bool HqlDfaState::isAccepting()
{
    ForEachItemIn(idx, position)
        if (position.item(idx).getOperator() == no_pat_endpattern)
            return true;
    return false;
}


static inline int compareState(HqlDfaState * left, HqlDfaState * right) 
{ 
    return left->position.compare(right->position);
}

static int compareState(CInterface * * left, CInterface * * right) 
{ 
    return compareState((HqlDfaState *)*left, (HqlDfaState *)*right);
}


void HqlRegexExpr::generateDFAs()
{
    if (op != no_pat_dfa)
    {
        ForEachItemIn(idx, args)
            args.item(idx).generateDFAs();
        return;
    }

    //Adapted from Dragon p141 Fig 3.44
    //Main variation is that the set of potential symbols is calculated first, rather than trying the whole alphabet
    //it might also have character-classes e.g., [[:alpha:]] in the stream.
    dfa = new HqlDfaInfo;

    HqlDfaState * firstState = new HqlDfaState(0);
    HqlRegexExpr * first = queryNamed()->def->queryChild(0);
    ForEachItemIn(idx, first->following)
        firstState->position.add(&first->following.item(idx));

    //Store a list of states, and an ordered list.  First marks which have been processed,
    //second gives efficient duplicate detection.
    dfa->states.append(*firstState);
    dfa->orderedStates.append(*LINK(firstState));

    unsigned curStateIndex = 0;
    while (curStateIndex < dfa->states.ordinality())
    {
        HqlDfaState & curState = dfa->states.item(curStateIndex++);

        //First gather the potential symbols that come next (otherwise we would die with unicode!)
        //it also speeds up ascii by a fairly large factor on sets of strings.
        SymbolArray nextSymbols;
        ForEachItemIn(ip, curState.position)
            curState.position.item(ip).gatherConsumeSymbols(nextSymbols);

        //For each symbol, work out what combination of states we could become translated to.
        SymbolArrayIterator iter(nextSymbols);
        ForEach(iter)
        {
            unsigned curSymbol = iter.get();
            HqlDfaState * nextState = new HqlDfaState(dfa->states.ordinality());

            //For each NFA state we could be in, what new NFA state could be now be in...
            ForEachItemIn(ip, curState.position)
            {
                HqlRegexExpr & curRegex = curState.position.item(ip);
                if (curRegex.canConsume(curSymbol))
                {
                    ForEachItemIn(idx2, curRegex.following)
                        nextState->position.add(&curRegex.following.item(idx2));
                }
            }

            //If no states, then don't bother adding a transition - we're done..
            if (nextState->position.ordinality())
            {
                bool isNew = false;
                CInterface * castState = nextState;
                unsigned matchIndex = dfa->orderedStates.bAdd(castState, compareState, isNew);
                if (isNew)
                    dfa->states.append(*LINK(nextState));
                else
                    nextState->Release();
    
                HqlDfaState * matchedState = &dfa->orderedStates.item(matchIndex);

                //Finally associate a transition for this symbol...
                curState.transitions.append(*new HqlDfaTransition(curSymbol, matchedState));
            }
            else
                nextState->Release();
        }
    }
}


/**
Some unicode pseudo code


ForEach(ip in curState.position)
{
    position(ip).getConsumed(consumeSet);
    positionSet = [ip];
    if (consumeSet.ordinality())
        Merge(consumeSet, 0, positionSet);
}

Merge(consumeSet, first, positionSet)
{
    for (i = first; i < sets.ordinality(); i++)
    {
        diff = intersect(sets(i).consume, consumeSet);
        if (diff.ordinality())
        {
            rightOnly = sets(i).consume - diff;
            if (rightOnly.ordinality() == 0)
                sets(i).positions.union(positionSet);
            else
            {
                sets(i).consume = rightOnly;
                Merge(diff, i+1, union(sets(i).positions, positionSet);
            }
            newOnly = consumeSet - diff;
            if (newOnly = [])
                return;
    
            consumeSet = newOnly;
        }
    }

    sets.append(consumeSet, positionSet);
}

Output would be a set of disjoint sets, together with a lits of positions that are matched by those sets.
They could then be used to generate the tables required by a dfa matcher list of
low, high, target-state
which was binary chopped.

It might be better to not use the icu sets, except for generating a set of ranges, and handle everything else ourselves.
Possibly a set of [low,high,positions], or maybe even [low, high, position], sorted by low
with some clever code to walk through and retain lists of which ones are active.
**/

//---------------------------------------------------------------------------

RegexContext::RegexContext(IHqlExpression * _expr, IWorkUnit * _wu, const HqlCppOptions & _options, ITimeReporter * _timeReporter, byte _algorithm) : NlpParseContext(_expr, _wu, _options, _timeReporter), parser(NULL, _algorithm)
{
    info.addedSeparators = false;
    switch (info.type)
    {
    case type_string: info.dfaComplexity = DEFAULT_DFA_COMPLEXITY; break;
    case type_utf8: info.dfaComplexity = DEFAULT_UTF8_DFA_COMPLEXITY; break;
    case type_unicode: info.dfaComplexity = DEFAULT_UNICODE_DFA_COMPLEXITY; break;
    }
    if (_options.parseDfaComplexity != (unsigned)-1)
        info.dfaComplexity = _options.parseDfaComplexity;
    info.expandRepeatAnyAsDfa = _options.expandRepeatAnyAsDfa;
    createLexer = false;
}

RegexContext::~RegexContext()
{
    ForEachItemIn(idx, named)
        named.item(idx).cleanup();
}

HqlNamedRegex * RegexContext::queryNamed(IHqlExpression * defn, IAtom * name, node_operator op, bool caseSensitive)
{
    ForEachItemIn(idx, named)
    {
        HqlNamedRegex & cur = named.item(idx);
        if (cur.matches(defn, name, caseSensitive))
        {
            assertex(cur.matches(defn, name, op, caseSensitive));
            return &cur;
        }
    }
    return NULL;
}


HqlNamedRegex * RegexContext::createNamed(IHqlExpression * expr, IAtom * name, node_operator op, bool caseSensitive)
{
    LinkedHqlExpr searchExpr = expr;
    if (op != no_pat_instance)
        //Create an expression that should never clash with anything we would find in the parse tree
        searchExpr.setown(createValue(op, expr->getType(), LINK(expr), createAttribute(tempAtom)));

    HqlNamedRegex * match = queryNamed(searchExpr, name, op, caseSensitive);
    if (match)
        match->addUse();
    else
    {
        match = new HqlNamedRegex(expr, name, searchExpr, op, caseSensitive, isMatched(searchExpr, name));
        named.append(*match);   // add to list first to avoid recursion problems.
        match->setRegexOwn(createStructure(expr, caseSensitive));
    }
    return match;
}

static HqlRegexExpr * createFollow(const ParseInformation & options, HqlRegexExpr * a1, HqlRegexExpr * a2, HqlRegexExpr * a3, bool caseSensitive)
{
    HqlRegexExpr * follow = createRegexExpr(options, no_pat_follow, NULL, caseSensitive);
    follow->addOperand(a1);
    follow->addOperand(a2);
    if (a3)
        follow->addOperand(a3);
    return follow;
}

HqlRegexExpr * RegexContext::createFollow(HqlRegexExpr * start, IHqlExpression * body, node_operator endOp, IHqlExpression * endExpr, bool caseSensitive)
{
    HqlRegexExpr * next = createStructure(body, caseSensitive);
    HqlRegexExpr * finish = createRegexExpr(info, endOp, endExpr, caseSensitive);
    return ::createFollow(info, start, next, finish, caseSensitive);
}

HqlRegexExpr * RegexContext::createStructure(IHqlExpression * expr, bool caseSensitive)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_pat_production:
        throwError(HQLERR_RegexNoTransformSupport);
    case no_pat_instance:
        {
            IHqlExpression * def = expr->queryChild(0);
            if (createLexer)
                return createStructure(def, caseSensitive);
            Owned<HqlRegexExpr> instance = createRegexExpr(info, expr, caseSensitive);
            IHqlExpression * nameExpr = expr->queryChild(1);
            instance->setNamed(createNamed(def, nameExpr ? nameExpr->queryName() : NULL, op, caseSensitive));
            return instance.getClear();
        }
    case no_pat_guard:
        return createRegexExpr(info, no_null, NULL, caseSensitive);
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return NULL;
    case no_implicitcast:
        //can sometimes occur when parameters are bound
        return createStructure(expr->queryChild(0), caseSensitive);
    case no_pat_const:
        if (createLexer)
            return expandStringAsChars(expr, info, caseSensitive);
        return createRegexExpr(info, expr, caseSensitive);
    case no_pat_set:
    case no_penalty:
        //Don't walk children...
        return createRegexExpr(info, expr, caseSensitive);
    case no_pat_x_before_y:
    case no_pat_x_after_y:
        {
            Owned<HqlRegexExpr> pattern = createStructure(expr->queryChild(0), caseSensitive);
            Owned<HqlRegexExpr> check = createRegexExpr(info, expr, caseSensitive);
            check->setSubPattern(createNamed(expr->queryChild(1), NULL, no_pat_checkin, caseSensitive));
            if (op == no_pat_x_before_y)
                return ::createFollow(info, pattern.getClear(), check.getClear(), NULL, caseSensitive);
            else
                return ::createFollow(info, check.getClear(), pattern.getClear(), NULL, caseSensitive);
        }
    case no_pat_before_y:
    case no_pat_after_y:
        {
            Owned<HqlRegexExpr> check = createRegexExpr(info, expr, caseSensitive);
            check->setSubPattern(createNamed(expr->queryChild(0), NULL, no_pat_checkin, caseSensitive));
            return check.getClear();
        }
    case no_pat_checkin:
        {
            Owned<HqlRegexExpr> start = createRegexExpr(info, no_pat_begincheck, expr, caseSensitive);
            Owned<HqlRegexExpr> next = createStructure(expr->queryChild(0), caseSensitive);
            Owned<HqlRegexExpr> finish = createRegexExpr(info, no_pat_endcheckin, expr, caseSensitive);
            finish->setSubPattern(createNamed(expr->queryChild(1), NULL, no_pat_checkin, caseSensitive));
            return ::createFollow(info, start.getClear(), next.getClear(), finish.getClear(), caseSensitive);
        }
    case no_pat_checklength:
        {
            HqlRegexExpr * start = createRegexExpr(info, no_pat_begincheck, expr, caseSensitive);
            return createFollow(start, expr->queryChild(0), no_pat_endchecklength, expr, caseSensitive);
        }
    case no_pat_validate:
        {
            HqlRegexExpr * start = createRegexExpr(info, no_pat_beginvalidate, expr, caseSensitive);
            return createFollow(start, expr->queryChild(0), no_pat_endvalidate, expr, caseSensitive);
        }
    case no_pat_token:
    case no_pat_imptoken:
        {
            IHqlExpression * child = expr->queryChild(0);
            //MORE: Covert into a function to allow multiple values of following:
            switch (child->getOperator())
            {
            case no_null:
            case no_pat_first:
            case no_pat_last:
                return createStructure(child, caseSensitive);
            case no_penalty:
            case no_pat_x_before_y:
            case no_pat_before_y:
            case no_pat_x_after_y:
            case no_pat_after_y:
            case no_pat_guard:
                return createStructure(child, caseSensitive);
            case no_pat_const:
//              if (!info.separator)
//                  return createStructure(child);
                break;
            }
            HqlRegexExpr * start = createRegexExpr(info, no_pat_begintoken, expr, caseSensitive);
            return createFollow(start, expr->queryChild(0), no_pat_endtoken, expr, caseSensitive);
        }
    case no_pat_repeat:
        {
            HqlRegexExpr * ret = createRegexExpr(info, expr, caseSensitive);
            IHqlExpression * repeated = expr->queryChild(0);
            if (ret->isStandardRepeat())
                ret->addOperand(createStructure(repeated, caseSensitive));
            else
                ret->setNamed(createNamed(repeated, NULL, op, caseSensitive));
            return ret;
        }
    case no_pat_or:
    case no_pat_follow:
        {
            //Expand these out as much as possible....
            HqlExprArray args;
            expr->unwindList(args, op);
            OwnedHqlExpr flatExpr = expr->clone(args);
            Owned<HqlRegexExpr> ret = createRegexExpr(info, flatExpr, caseSensitive);
            ForEachChild(idx, flatExpr)
            {
                HqlRegexExpr * child = createStructure(flatExpr->queryChild(idx), caseSensitive);
                if (child)
                    ret->addOperand(child);
            }
            return ret.getClear();
        }
    case no_pat_case:
    case no_pat_nocase:
        return createStructure(expr->queryChild(0), op==no_pat_case);
    case no_pat_featureactual:
        throwError(HQLERR_RegexFeatureNotSupport);
    }
    Owned<HqlRegexExpr> ret = createRegexExpr(info, expr, caseSensitive);
    ForEachChild(idx, expr)
    {
        HqlRegexExpr * child = createStructure(expr->queryChild(idx), caseSensitive);
        if (child)
            ret->addOperand(child);
    }
    return ret.getClear();
}

void RegexContext::buildStructure()
{
    unsigned startTime = msTick();
    IHqlExpression * grammar = expr->queryChild(2);
    assertex(grammar->getOperator() == no_pat_instance);
    IAtom * name = grammar->queryChild(1)->queryName();
    OwnedHqlExpr structure = LINK(grammar);//createValue(no_pat_instance, makeRuleType(NULL), LINK(grammar), LINK(grammar->queryChild(1)));

    HqlRegexExpr * rootRegex = createStructure(structure, isCaseSensitive());
    root.setown(new HqlNamedRegex(structure, internalAtom, structure, no_parse, isCaseSensitive(), false));
    root->setRegexOwn(rootRegex);
    named.append(*LINK(root));

    DEBUG_TIMERX(timeReporter, "EclServer: Generate PARSE: Create Structure", msTick()-startTime);
}

void RegexContext::expandRecursion()
{
    //First add all the symbols referenced by the use() attributes on the parse so they can be matched.
    ForEachChild(idx, expr)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (cur->getOperator() == no_pat_use)
        {
            //NB: Does not return a linked item.
            HqlNamedRegex * named = createNamed(cur->queryChild(0), NULL, no_pat_instance, true);
            named->removeUse(); // Not actually used at the moment...
            HqlNamedRegex * named2 = createNamed(cur->queryChild(0), NULL, no_pat_instance, false);
            named2->removeUse();    // Not actually used at the moment...
        }
    }

    root->expandRecursion(*this, NULL);
    ForEachItemInRev(idx2, named)
        if (!named.item(idx2).queryExpandedRecursion())
            named.remove(idx2);
}

void RegexContext::insertSeparators()
{
    if (info.separator)
    {
        ForEachItemIn(idx, named)
            named.item(idx).insertSeparators(info.separator, this);

        //add separator/first onto the front of root.
        info.addedSeparators = true;
    }
}


void RegexContext::optimizeSpotDFA()
{
    if (info.dfaComplexity > 0)
    {
        ForEachItemIn(idx, named)
            named.item(idx).calcDfaScore();
        root->markDFAs(info.dfaComplexity);
        ForEachItemIn(idx2, named)
            named.item(idx2).createDFAs(*this);
    }
}


void RegexContext::optimizePattern()
{
    unsigned startTime = msTick();
    ForEachItemIn(idx1, named)
        named.item(idx1).mergeCreateSets();
    root->expandNamedSymbols();
    ForEachItemInRev(idx2, named)
    {
        if (!named.item(idx2).isUsed())
        {
            //Can't delete it otherwise everything gets cleaned up...
            deadNamed.append(named.item(idx2));
            named.remove(idx2, true);
        }
    }
    optimizeSpotDFA();
    DEBUG_TIMERX(timeReporter, "EclServer: Generate PARSE: Optimize", msTick()-startTime);
}


HqlNamedRegex * RegexContext::queryDefine(IHqlExpression * defineName, bool caseSensitive)
{
    ForEachItemIn(idx, named)
    {
        HqlNamedRegex & cur = named.item(idx);
        if (cur.matchesDefine(defineName,caseSensitive))
            return &cur;
    }
    StringBuffer s;
    defineName->toString(s);
    throwError1(HQLERR_DefineUseStrNotFound, s.str());
    return NULL;
}


void RegexContext::analysePattern()
{
    unsigned startTime = msTick();
    //This conversion is based around the description in the Dragon book:
    //3.9 From a regular expression to a DFA
    //even though we don't always convert it, the steps form a useful algorithm
    ForEachItemIn(idx0, named)
        named.item(idx0).addBeginEnd(info);

    ForEachItemIn(idx1, named)
        named.item(idx1).analyseNullLeadTrail();

    ForEachItemIn(idx2, named)
        named.item(idx2).calculateNext();

    ForEachItemIn(idx3, named)
        named.item(idx3).generateDFAs();

    DEBUG_TIMERX(timeReporter, "EclServer: Generate PARSE: Analyse", msTick()-startTime);
}


void RegexContext::generateRegex()
{
    unsigned startTime = msTick();

    parser.addedSeparators = info.addedSeparators;
    setParserOptions(parser);

    GenerateRegexCtx ctx(*this, info, idAllocator);
    ForEachItemIn(idx0, named)
    {
        HqlNamedRegex & cur = named.item(idx0);
        cur.generateRegex(ctx);
    }

    parser.grammar.set(root->queryRootPattern());
    parser.minPatternLength = root->getMinLength();

    DEBUG_TIMERX(timeReporter, "EclServer: Generate PARSE: Generate", msTick()-startTime);
}


//void RegexContext::removeTrivialInstances()
//{
//  ForEachItemIn(idx, named)
//      named.item(idx).removeTrivialInstances();
//}

void RegexContext::compileSearchPattern()
{
    checkValidMatches();
    buildStructure();
    expandRecursion();
//  removeTrivialInstances();
    insertSeparators();
    optimizePattern();
    analysePattern();
    generateRegex();
    compileMatched(parser);
}

void RegexContext::getDebugText(StringBuffer & s, unsigned detail)
{
    NlpParseContext::getDebugText(s, detail);
    regexToXml(s, parser.grammar, detail);
}

NlpParseContext * createRegexContext(IHqlExpression * expr, IWorkUnit * wu, const HqlCppOptions & options, ITimeReporter * timeReporter, byte algorithm)
{
    return new RegexContext(expr, wu, options, timeReporter, algorithm);
}

//-- Lexer creation

void RegexContext::beginLexer()
{
    createLexer = true;
    info.expandRepeatAnyAsDfa = true;

    OwnedHqlExpr searchExpr = createValue(no_pat_dfa, makePatternType(), createUniqueId());

    lexerNamed.setown(new HqlNamedRegex(searchExpr, NULL, searchExpr, no_pat_dfa, isCaseSensitive(), false));
    lexerOr.setown(createRegexExpr(info, no_pat_or, NULL, isCaseSensitive()));
    lexerNamed->setRegexOwn(LINK(lexerOr));

    lexerRoot.setown(createRegexExpr(info, no_pat_dfa, NULL, isCaseSensitive()));
    lexerRoot->setNamed(lexerNamed);

    root.setown(new HqlNamedRegex(expr, NULL, expr, no_parse, isCaseSensitive(), false));
    root->setRegexOwn(LINK(lexerRoot));

    named.append(*LINK(lexerNamed));
    named.append(*LINK(root));
}

void RegexContext::addLexerToken(unsigned id, IHqlExpression * pattern)
{
    HqlRegexExpr * token = createStructure(pattern, isCaseSensitive());
    HqlRegexExpr * end = createRegexExpr(info, no_pat_endpattern, NULL, isCaseSensitive());
    end->setAcceptId(id);
    if (token->getOperator() == no_pat_follow)
        token->addOperand(end);
    else
    {
        HqlRegexExpr * follow = createRegexExpr(info, no_pat_follow, NULL, isCaseSensitive());
        follow->addOperand(token);
        follow->addOperand(end);
        token = follow;
    }
    lexerOr->addOperand(token);
}

void RegexContext::generateLexer(IDfaPattern * builder)
{
    //MORE: Need to call some elements of optimizePattern() to expand limited repeats.
    analysePattern();
    lexerRoot->generateDFA(builder);
}

/*
ToDo:

  o Should move some of the logic from HqlregexExpr to complex + remove the queyNamed() etc. virtuals.
  */
