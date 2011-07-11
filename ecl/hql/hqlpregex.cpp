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
#include "jliball.hpp"
#include "eclrtl.hpp"
#include "hqlerrors.hpp"
#include "hqlexpr.hpp"
#include "hqlpregex.hpp"
#include "thorregex.hpp"

//---------------------------------------------------------------------------

const unsigned PatEOF = (unsigned)-1;
class RegexParser
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

void RegexParser::illegal() 
{ 
    throwError(HQLERR_IllegalRegexPattern);
}

void RegexParser::advance()             
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

unsigned RegexParser::next() const
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

unsigned RegexParser::readNumber()
{
    unsigned first = nextAdvance();
    if ((first < '0') || (first > '9'))
        illegal();
    unsigned value = (first - '0');
    loop
    {
        unsigned x = next();
        if ((x < '0') || (x > '9'))
            return value;
        value = value * 10 + (x - '0');
        advance();
    }
}

IHqlExpression * RegexParser::parse(bool isRoot)
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

IHqlExpression * RegexParser::parse0()
{
    if (next() == '|' || next() == PatEOF || next() == ')')
        return createValue(no_null, makePatternType());
    OwnedHqlExpr prev = parse1();
    loop
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

IHqlExpression * RegexParser::parse1()
{
    OwnedHqlExpr arg = parse2();
    loop
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
        arg.setown(createValue(no_pat_repeat, makePatternType(), arg.getClear(), createConstant((__int64)low), createConstant((__int64)high), minimal));
    }
}

IHqlExpression * RegexParser::parse2()
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
            return createValue(no_pat_const, createConstant(createStringValue(&next, 1)));
        }
    default:
        if (nextIsSpecial())
            illegal();

        //Read continuous sequences of characters as a single string, but be careful about trailing repeat characters (bug #51644)
        const byte * start = cur;
        advance();
        unsigned restorePos = getPos();
        loop
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
        return createValue(no_pat_const, createConstant(value));
    }
}

IHqlExpression * RegexParser::parseSet()
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

IHqlExpression * RegexParser::parseSetBound()
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
                loop
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

bool RegexParser::nextIsSpecial() const
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

bool RegexParser::nextIsRepeat() const
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

IHqlExpression * RegexParser::parsePattern(unsigned len, const char * text)
{
    type = type_string;
    start = (const byte *)text;
    end = start + len;
    cur = start;
    return parse(true);
}

IHqlExpression * RegexParser::parseUtf8Pattern(unsigned len, const char * text)
{
    type = type_utf8;
    start = (const byte *)text;
    end = start + rtlUtf8Size(len, text);
    cur = start;
    return parse(true);
}

IHqlExpression * RegexParser::parsePattern(unsigned len, const UChar * text)
{
    type = type_unicode;
    start = (const byte *)text;
    end = start + len * sizeof(UChar);
    cur = start;
    return parse(true);
}

IHqlExpression * convertPatternToExpression(unsigned len, const char * text)
{
    RegexParser parser;
    return parser.parsePattern(len, text);
}


IHqlExpression * convertUtf8PatternToExpression(unsigned len, const char * text)
{
    RegexParser parser;
    return parser.parseUtf8Pattern(len, text);
}


IHqlExpression * convertPatternToExpression(unsigned len, const UChar * text)
{
    RegexParser parser;
    return parser.parsePattern(len, text);
}

