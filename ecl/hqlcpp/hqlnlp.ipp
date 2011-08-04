/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */
#ifndef __HQLNLP_IPP_
#define __HQLNLP_IPP_

#include "thorparse.ipp"
#include "thorregex.hpp"
#include "thorrparse.ipp"
#include "hqlhtcpp.ipp"

//---------------------------------------------------------------------------
#define NO_DFA_SCORE    ((unsigned)-1)

typedef MapBetween<LinkedHqlExpr, IHqlExpression *, regexid_t, regexid_t> RegexIdMapping;

class RegexIdAllocator
{
public:
    RegexIdAllocator() { nextId = 0; }

    regexid_t queryID(IHqlExpression * expr, _ATOM name);
    void setID(IHqlExpression * expr, _ATOM name, regexid_t id);

protected:
    IHqlExpression * createKey(IHqlExpression * expr, _ATOM name);

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
    bool isMatched(IHqlExpression * expr, _ATOM name);

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
