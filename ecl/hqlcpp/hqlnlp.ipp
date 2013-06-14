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
#ifndef __HQLNLP_IPP_
#define __HQLNLP_IPP_

#include "thorparse.ipp"
#include "thorregex.hpp"
#include "thorralgo.ipp"
#include "hqlhtcpp.ipp"

//---------------------------------------------------------------------------
#define NO_DFA_SCORE    ((unsigned)-1)

typedef MapBetween<LinkedHqlExpr, IHqlExpression *, regexid_t, regexid_t> RegexIdMapping;

class RegexIdAllocator
{
public:
    RegexIdAllocator() { nextId = 0; }

    regexid_t queryID(IHqlExpression * expr, IAtom * name);
    void setID(IHqlExpression * expr, IAtom * name, regexid_t id);

protected:
    IHqlExpression * createKey(IHqlExpression * expr, IAtom * name);

protected:
    unsigned nextId;
    RegexIdMapping map;
};


struct LengthLimit
{
    LengthLimit() { minLength = 0; maxLength = PATTERN_UNLIMITED_LENGTH; containsAssertion = false; }

    bool canBeNull()                                        { return (minLength == 0) && !containsAssertion; }

    unsigned minLength;
    unsigned maxLength;
    bool containsAssertion;
};

struct ParseInformation;
class MatchReference : public CInterface
{
public:
    MatchReference(IHqlExpression * expr);

    void compileMatched(RegexIdAllocator & idAllocator, UnsignedArray & ids, UnsignedArray & indexValues);
    bool equals(const MatchReference & _other) const;
    StringBuffer & getDebugText(StringBuffer & out, RegexIdAllocator & idAllocator);
    void getPath(StringBuffer & path);

protected:
    void expand(IHqlExpression * expr, bool isLast);

public:
    HqlExprArray names;
    HqlExprArray indices;
};

struct ParseInformation
{
public:
    NlpInputFormat inputFormat() const
    {
        switch (type)
        {
        case type_string: return NlpAscii;
        case type_utf8: return NlpUtf8;
        case type_unicode: return NlpUnicode;
        }
        throwUnexpected();
    }
public:
    OwnedHqlExpr separator;
    unsigned charSize;
    type_t type;
    bool caseSensitive;
    bool expandRepeatAnyAsDfa;
    unsigned dfaComplexity;
    bool addedSeparators;
    unsigned dfaRepeatMax;
    unsigned dfaRepeatMaxScore;
    unique_id_t uidBase;
};

class NlpParseContext : public CInterface
{
public:
    NlpParseContext(IHqlExpression * _expr, IWorkUnit * _wu, const HqlCppOptions & options, ITimeReporter * _timeReporter);

    void addAllMatched();
    virtual unsigned addMatchReference(IHqlExpression * expr);

    void buildProductions(HqlCppTranslator & translator, BuildCtx & classctx, BuildCtx & startctx);
    void buildValidators(HqlCppTranslator & translator, BuildCtx & classctx);
    void extractValidates(IHqlExpression * expr);
    bool isMatched(IHqlExpression * expr, IAtom * name);

    virtual void compileSearchPattern() = 0;
    virtual void getDebugText(StringBuffer & s, unsigned detail) = 0;
    virtual bool isGrammarAmbiguous() const = 0;
    virtual INlpParseAlgorithm * queryParser() = 0;

    bool isCaseSensitive() const                            { return info.caseSensitive; }
    type_t searchType() const                               { return info.type; }
    IWorkUnit * wu()                                        { return workunit; }

protected:
    void checkValidMatches();
    void compileMatched(NlpAlgorithm & parser);
    void extractMatchedSymbols(IHqlExpression * expr);
    bool isValidMatch(MatchReference & match, unsigned depth, IHqlExpression * pattern);
    unsigned getValidatorIndex(IHqlExpression * expr) const { return validators.find(*expr); }
    IHqlExpression * queryValidateExpr(IHqlExpression * expr) const;
    void setParserOptions(INlpParseAlgorithm & parser);

private:
    void doExtractValidates(IHqlExpression * expr);

protected:
    ParseInformation info;
    OwnedHqlExpr expr;
    CIArrayOf<MatchReference> matches;
    RegexIdAllocator idAllocator;
    HqlExprArray matchedSymbols;
    HqlExprArray productions;
    HqlExprArray validators;
    bool allMatched;
    IWorkUnit * workunit;
    Linked<ITimeReporter> timeReporter;
};

void getCheckRange(IHqlExpression * range, unsigned & minLength, unsigned & maxLength, unsigned charLength);

enum ValidateKind { ValidateIsString, ValidateIsUnicode, ValidateIsEither };
ValidateKind getValidateKind(IHqlExpression * expr);

NlpParseContext * createRegexContext(IHqlExpression * expr, IWorkUnit * wu, const HqlCppOptions & options, ITimeReporter * timeReporter, byte algorithm);
NlpParseContext * createTomitaContext(IHqlExpression * expr, IWorkUnit * wu, const HqlCppOptions & options, ITimeReporter * timeReporter);

#endif
