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
#include "hqlerrors.hpp"
#include "hqlexpr.hpp"
#include "hqlpregex.hpp"
#include "thorregex.hpp"

//---------------------------------------------------------------------------

const unsigned PatEOF = (unsigned)-1;
class HqlRegexParser
{
public:
    IHqlExpression * parsePattern(unsigned len, const char * text);
    IHqlExpression * parsePattern(unsigned len, const UChar * text);
    IHqlExpression * parseUtf8Pattern(unsigned len, const char * text);

private:
    IHqlExpression * parse(bool isRoot);
    IHqlExpression * parse0();
    IHqlExpression * parse1();
    IHqlExpression * parse2();
    IHqlExpression * parseSet();
    IHqlExpression * parseSetBound();

    void advance();
    bool done() const           { return (cur >= end); }
    unsigned next() const;
    unsigned nextAdvance()      { unsigned temp = next(); if (temp != PatEOF) advance(); return temp; }
    bool nextIsRepeat() const;
    bool nextIsSpecial() const;
    void illegal();
    unsigned readNumber();
    unsigned getPos() const { return cur-start; }
    void setPos(unsigned offset) { cur = start+offset; }

private:
    const byte * start;
    const byte * cur;
    const byte * end;
    type_t type;
};

void HqlRegexParser::illegal()
{ 
    throwError(HQLERR_IllegalRegexPattern);
}

void HqlRegexParser::advance()
{ 
    if (cur >= end) 
        illegal(); 
    switch (type)
    {
    case type_unicode:
        cur += sizeof(UChar);
        break;
    case type_utf8:
        cur += rtlUtf8Size(cur);
        break;
    case type_string:
        cur += sizeof(char);
        break;
    }
}

unsigned HqlRegexParser::next() const
{ 
    if (cur != end) 
    {
        switch (type)
        {
        case type_unicode:
            return *(const UChar *)cur; 
        case type_utf8:
            return rtlUtf8Char(cur); 
        default:
            return *cur; 
        }
    }
    return PatEOF; 
}

unsigned HqlRegexParser::readNumber()
{
    unsigned first = nextAdvance();
    if ((first < '0') || (first > '9'))
        illegal();
    unsigned value = (first - '0');
    for (;;)
    {
        unsigned x = next();
        if ((x < '0') || (x > '9'))
            return value;
        value = value * 10 + (x - '0');
        advance();
    }
}

IHqlExpression * HqlRegexParser::parse(bool isRoot)
{
    OwnedHqlExpr prev = parse0();
    while (next() == '|')
    {
        advance();
        IHqlExpression * arg = parse0();
        prev.setown(createValue(no_pat_or, makePatternType(), prev.getClear(), arg));
    }
    assertex(!isRoot || next() == PatEOF);
    return prev.getClear();
}

IHqlExpression * HqlRegexParser::parse0()
{
    if (next() == '|' || next() == PatEOF || next() == ')')
        return createValue(no_null, makePatternType());
    OwnedHqlExpr prev = parse1();
    for (;;)
    {
        switch (next())
        {
        case PatEOF:
        case '|':
        case ')':
            return prev.getClear();
        }

        IHqlExpression * next = parse1();
        prev.setown(createValue(no_pat_follow, makePatternType(), prev.getClear(), next));
    }
    return prev.getClear();
}

IHqlExpression * HqlRegexParser::parse1()
{
    OwnedHqlExpr arg = parse2();
    for (;;)
    {
        unsigned low = 0;
        unsigned high = PATTERN_UNLIMITED_LENGTH;
        switch (next())
        {
        case '*':
            break;
        case '+':
            low = 1;
            break;
        case '?':
            high = 1;
            break;
        case '{':
            {
                advance();
                if (next() != ',')
                    low = readNumber();
                if (next() == ',')
                {
                    advance();
                    if (next() != '}')
                        high = readNumber();
                }
                else
                    high = low;
                //get low, high numbers;
                if (next() != '}')
                    illegal();
                break;
            }
        default:
            return arg.getClear();
        }
        IHqlExpression * minimal = NULL;
        advance();
        if (next() == '?')
        {
            advance();
            minimal = createAttribute(minimalAtom);
        }
        arg.setown(createValue(no_pat_repeat, makePatternType(), { arg.getClear(), createConstant((__int64)low), createConstant((__int64)high), minimal }));
    }
}

IHqlExpression * HqlRegexParser::parse2()
{
    switch (next())
    {
    case '[':
        return parseSet();
    case '(':
        {
            advance();
            if (next() == '?')
            {
                advance();
                OwnedHqlExpr attr;
                node_operator op = no_none;
                switch (next())
                {
                case '=': 
                    op = no_pat_before_y;
                    break;
                case '!':
                    op = no_pat_before_y;
                    attr.setown(createAttribute(notAtom));
                    break;
                case '<':
                    advance();
                    switch (next())
                    {
                    case '=': 
                        op = no_pat_after_y; 
                        break;
                    case '!':
                        op = no_pat_after_y;
                        attr.setown(createAttribute(notAtom));
                        break;
                    }
                    break;
                case '#':
                    //weird python syntax for a comment.
                    do
                    {
                        advance();
                    } while (next() && next() != ')');
                    return parse2();
                case 'P':
                    //Python again I think. - this isn't implemented, but we will probably need something similar.
                    advance();
                    if (next() == '=')
                    {
                        //same as a previous named group
                    }
                    break;
                }
                if (op == no_none)
                    illegal();
                advance();
                OwnedHqlExpr pattern = parse(false);
                if (nextAdvance() != ')')
                    illegal();
                return createValue(op, makePatternType(), pattern.getClear(), attr.getClear());
            }
            else
            {
                OwnedHqlExpr match = parse(false);
                if (nextAdvance() != ')')
                    illegal();
                return match.getClear();
            }
        }
    case '^':
        advance();
        return createValue(no_pat_first, makeRuleType(NULL));
    case '$':
        advance();
        return createValue(no_pat_last, makeRuleType(NULL));
    case '.':
        advance();
        return createValue(no_pat_anychar, makePatternType());
    case PatEOF:
        illegal();
        return NULL;
    case '\\':
        {
            advance();
            //MORE: This really needs to be utf-8 compliant...
            char next = (char)nextAdvance();
            //MORE: interpret special characters....
            return createValue(no_pat_const, makePatternType(), createConstant(createStringValue(&next, 1)));
        }
    default:
        if (nextIsSpecial())
            illegal();

        //Read continuous sequences of characters as a single string, but be careful about trailing repeat characters (bug #51644)
        const byte * start = cur;
        advance();
        unsigned restorePos = getPos();
        for (;;)
        {
            if (done())
                break;
            if (nextIsSpecial())
            {
                //Don't include the last character in the string if it is repeated. (if > 1 char)
                if (nextIsRepeat())
                    setPos(restorePos);
                break;
            }
            restorePos = getPos();
            advance();
        }

        IValue * value = NULL;
        switch (type)
        {
        case type_unicode:
            {
                unsigned len = (cur-start)/2;
                ITypeInfo * uType = makeUnicodeType(len, NULL);
                value = createUnicodeValue(len, start, uType);
                break;
            }
        case type_utf8:
            {
                unsigned len = rtlUtf8Length(cur-start, start);
                ITypeInfo * uType = makeUtf8Type(len, NULL);
                value = createUtf8Value((const char *)start, uType);
                break;
            }
        default:
            {
                value = createStringValue((const char *)start, cur-start);
                break;
            }
        }
        return createValue(no_pat_const, makePatternType(), createConstant(value));
    }
}

IHqlExpression * HqlRegexParser::parseSet()
{
    bool invert = false;
    advance();
    if (next() == '^')
    {
        invert = true;
        advance();
    }

    HqlExprArray args;
    while (next() != ']')
    {
        OwnedHqlExpr firstExpr = parseSetBound();
        if (next() == '-')
        {
            advance();
            IHqlExpression * lastExpr = parseSetBound();
            args.append(*createValue(no_range, makeNullType(), firstExpr.getClear(), lastExpr));
        }
        else
            args.append(*firstExpr.getClear());
    }
    advance();
    if (invert)
        args.append(*createAttribute(notAtom));
    return createValue(no_pat_set, makePatternType(), args);
}

IHqlExpression * HqlRegexParser::parseSetBound()
{
    unsigned first = nextAdvance();
    if (first == '\\')
        first = nextAdvance();
    else if (first == '[')
    {
        switch (next())
        {
        case ':':
            {
                advance();
                StringBuffer name;
                for (;;)
                {
                    unsigned c = nextAdvance();
                    if (c == PatEOF)
                        illegal();
                    if (c == ':')
                        break;
                    name.append((char)c);
                }
                if (nextAdvance() != ']')
                    illegal();
                const char * className = name;
                if (stricmp(className, "alnum") == 0) first = RCCalnum;
                else if (stricmp(className, "cntrl") == 0) first = RCCcntrl;
                else if (stricmp(className, "lower") == 0) first = RCClower;
                else if (stricmp(className, "upper") == 0) first = RCCupper;
                else if (stricmp(className, "space") == 0) first = RCCspace;
                else if (stricmp(className, "alpha") == 0) first = RCCalpha;
                else if (stricmp(className, "digit") == 0) first = RCCdigit;
                else if (stricmp(className, "print") == 0) first = RCCprint;
                else if (stricmp(className, "blank") == 0) first = RCCblank;
                else if (stricmp(className, "graph") == 0) first = RCCgraph;
                else if (stricmp(className, "punct") == 0) first = RCCpunct;
                else if (stricmp(className, "xdigit") == 0) first = RCCxdigit;
                else
                    throwError1(HQLERR_UnknownCharClassX, className);
                break;
            }
        case '.':
            {
                throwError(HQLERR_CollectionNotYetSupported);
                advance();
                first = nextAdvance() | RegexSpecialCollationClass;
                if ((nextAdvance() != '.') || (nextAdvance() != ']'))
                    illegal();
                break;
            }
        case '=':
            {
                throwError(HQLERR_EquivalenceNotYetSupported);
                advance();
                first = nextAdvance() | RegexSpecialEquivalenceClass;
                if ((nextAdvance() != '=') || (nextAdvance() != ']'))
                    illegal();
                break;
            }
        default:
            break;  // not illegal - just a [ character.
        }
    }
    if (first == PatEOF)
        illegal();
    return createConstant((int)first);
}

bool HqlRegexParser::nextIsSpecial() const
{
    switch (next())
    {
    case '|':
    case '*':
    case '+':
    case '?':
    case '(':
    case ')':
    case '{':
    case '}':
    case '[':
    case ']':
    case '^':
    case '$':
    case '\\':
        return true;
    default:
        return false;
    }
}

bool HqlRegexParser::nextIsRepeat() const
{
    switch (next())
    {
    case '*':
    case '+':
    case '?':
        return true;
    default:
        return false;
    }
}

IHqlExpression * HqlRegexParser::parsePattern(unsigned len, const char * text)
{
    type = type_string;
    start = (const byte *)text;
    end = start + len;
    cur = start;
    return parse(true);
}

IHqlExpression * HqlRegexParser::parseUtf8Pattern(unsigned len, const char * text)
{
    type = type_utf8;
    start = (const byte *)text;
    end = start + rtlUtf8Size(len, text);
    cur = start;
    return parse(true);
}

IHqlExpression * HqlRegexParser::parsePattern(unsigned len, const UChar * text)
{
    type = type_unicode;
    start = (const byte *)text;
    end = start + len * sizeof(UChar);
    cur = start;
    return parse(true);
}

IHqlExpression * convertPatternToExpression(unsigned len, const char * text)
{
    HqlRegexParser parser;
    return parser.parsePattern(len, text);
}


IHqlExpression * convertUtf8PatternToExpression(unsigned len, const char * text)
{
    HqlRegexParser parser;
    return parser.parseUtf8Pattern(len, text);
}


IHqlExpression * convertPatternToExpression(unsigned len, const UChar * text)
{
    HqlRegexParser parser;
    return parser.parsePattern(len, text);
}

