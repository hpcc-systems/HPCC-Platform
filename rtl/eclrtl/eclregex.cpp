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

#include "limits.h"
#if defined(_USE_BOOST_REGEX)
#include "boost/regex.hpp" // must precede platform.h ; n.b. this uses a #pragma comment(lib, ...) to link the appropriate .lib in MSVC
#elif defined(_USE_C11_REGEX)
#include <regex>
#endif
#include "platform.h"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#ifdef _USE_ICU
#include "unicode/regex.h"
#endif

#define UTF8_CODEPAGE "UTF-8"
#define UTF8_MAXSIZE     4

#if defined(_USE_BOOST_REGEX) || defined(_USE_C11_REGEX)

#if defined(_USE_BOOST_REGEX)
using boost::regex;
using boost::regex_search;
using boost::regex_replace;
using boost::regex_iterator;
using boost::cmatch;
using boost::match_results;
#else
using std::regex;
using std::regex_search;
using std::regex_replace;
using std::regex_iterator;
using std::cmatch;
using std::match_results;
#endif

class CStrRegExprFindInstance : implements IStrRegExprFindInstance
{
private:
    bool            matched;
    const regex * regEx;
    cmatch   subs;
    char *          sample; //only required if findstr/findvstr will be called

public:
    CStrRegExprFindInstance(const regex * _regEx, const char * _str, size32_t _from, size32_t _len, bool _keep)
        : regEx(_regEx)
    {
        matched = false;
        sample = NULL;
        try
        {
            if (_keep)
            {
                sample = (char *)rtlMalloc(_len + 1);  //required for findstr
                memcpy(sample, _str + _from, _len);
                sample[_len] = (char)NULL;
                matched = regex_search(sample, subs, *regEx);
            }
            else
            {
                matched = regex_search(_str + _from, _str + _len, subs, *regEx);
            }
        }
        catch (const std::runtime_error & e)
        {
            std::string msg = "Error in regex search: ";
            msg += e.what();
#if defined(_USE_BOOST_REGEX)
            msg += "(regex: ";
            msg += regEx->str();
            msg += ")";
#endif
            rtlFail(0, msg.c_str());
        }

    }

    ~CStrRegExprFindInstance() //CAVEAT non-virtual destructor !
    {
        free(sample);
    }

    //IStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, char * & out, unsigned n = 0) const
    {
        if (matched && (n < subs.size()))
        {
            outlen = subs[n].second - subs[n].first;
            out = (char *)rtlMalloc(outlen);
            memcpy_iflen(out, subs[n].first, outlen);
        }
        else
        {
            outlen = 0;
            out = NULL;
        }
    }

    char const * findvstr(unsigned outlen, char * out, unsigned n = 0)
    {
        if (matched && (n < subs.size()))
        {
            unsigned sublen = subs[n].second - subs[n].first;
            if (sublen >= outlen)
                sublen = outlen - 1;
            memcpy(out, subs[n].first, sublen);
            out[sublen] = 0;
        }
        else
        {
            out[0] = 0;
        }
        return out;
    }
};

//---------------------------------------------------------------------------

class CCompiledStrRegExpr : implements ICompiledStrRegExpr
{
private:
    regex    regEx;

public:
    CCompiledStrRegExpr(const char * _regExp, bool _isCaseSensitive = false)
    {
        try
        {
#if defined(_USE_BOOST_REGEX)
            if (_isCaseSensitive)
                regEx.assign(_regExp, regex::perl);
            else
                regEx.assign(_regExp, regex::perl | regex::icase);
#else
            if (_isCaseSensitive)
                regEx.assign(_regExp, regex::ECMAScript);
            else
                regEx.assign(_regExp, regex::ECMAScript | regex::icase);
#endif
        }
#if defined(_USE_BOOST_REGEX)
        catch(const boost::bad_expression & e)
#else
        catch(const std::regex_error & e)
#endif
        {
            std::string msg = "Bad regular expression: ";
            msg += e.what();
            msg += ": ";
            msg += _regExp;
            rtlFail(0, msg.c_str());  //throws
        }
    }

    //ICompiledStrRegExpr

    void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const
    {
        std::string src(str, str + slen);
        std::string fmt(replace, replace + rlen);
        std::string tgt;
        try
        {
//          tgt = boost::regex_merge(src, cre->regEx, fmt, boost::format_perl); //Algorithm regex_merge has been renamed regex_replace, existing code will continue to compile, but new code should use regex_replace instead.
#if defined(_USE_BOOST_REGEX)
            tgt = regex_replace(src, regEx, fmt, boost::format_perl);
#else
            tgt = regex_replace(src, regEx, fmt);
#endif
        }
        catch(const std::runtime_error & e)
        {
            std::string msg = "Error in regex replace: ";
            msg += e.what();
#if defined(_USE_BOOST_REGEX)
            msg += "(regex: ";
            msg += regEx.str();
            msg += ")";
#endif
            rtlFail(0, msg.c_str());
        }
        outlen = tgt.length();
        out = (char *)rtlMalloc(outlen);
        memcpy_iflen(out, tgt.data(), outlen);
    }

    IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const
    {
        CStrRegExprFindInstance * findInst = new CStrRegExprFindInstance(&regEx, str, from, len, needToKeepSearchString);
        return findInst;
    }

    void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _srcLen, const char * _search)
    {
        rtlRowBuilder out;
        size32_t outBytes = 0;
        const char * search_end = _search+_srcLen;

        regex_iterator<const char *> cur(_search, search_end, regEx);
        regex_iterator<const char *> end; // Default contructor creates an end of list marker
        for (; cur != end; ++cur)
        {
            const match_results<const char *> &match = *cur;
            if (match[0].first==search_end) break;

            const size32_t lenBytes = match[0].second - match[0].first;
            out.ensureAvailable(outBytes+lenBytes+sizeof(size32_t));
            byte *outData = out.getbytes()+outBytes;

            * (size32_t *) outData = lenBytes;
            rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, match[0].first);

            outBytes += lenBytes+sizeof(size32_t);
        }
        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    };

};

//---------------------------------------------------------------------------

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExpr, isCaseSensitive);
    return expr;
}

ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiledExpr)
{
    if (compiledExpr)
        delete (CCompiledStrRegExpr*)compiledExpr;
}

ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * findInst)
{
    if (findInst)
        delete (CStrRegExprFindInstance*)findInst;
}

//---------------------------------------------------------------------------

// RegEx Compiler for unicode strings

#ifdef _USE_ICU
using icu::RegexMatcher;
using icu::RegexPattern;
using icu::UnicodeString;

class CUStrRegExprFindInstance : implements IUStrRegExprFindInstance
{
private:
    bool            matched;
    RegexMatcher *  matcher;
    UnicodeString   sample;
    unsigned        matchedSize;

public:
    CUStrRegExprFindInstance(RegexMatcher * _matcher, const UChar * _str, size32_t _from, size32_t _len)
        : matcher(_matcher), matchedSize(0)
    {
        matched = false;

#if U_ICU_VERSION_MAJOR_NUM>=59
        sample.setTo((const char16_t *) (_str + _from), _len);
#else
        sample.setTo(_str + _from, _len);
#endif
        matcher->reset(sample);
        matched = matcher->find() != FALSE;
        if (matched)
            matchedSize = (unsigned)matcher->groupCount() + 1;
    }

    //IUStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, UChar * & out, unsigned n = 0) const
    {
        if(matched && (n < matchedSize))
        {
            UErrorCode uerr = U_ZERO_ERROR;
            int32_t start = n ? matcher->start(n, uerr) : matcher->start(uerr);
            int32_t end = n ? matcher->end(n, uerr) : matcher->end(uerr);
            outlen = end - start;

            out = (UChar *)rtlMalloc(outlen*2);
            sample.extract(start, outlen, out);
        }
        else
        {
            outlen = 0;
            out = NULL;
        }
    }

    UChar const * findvstr(unsigned outlen, UChar * out, unsigned n = 0)
    {
        if(matched && (n < matchedSize))
        {
            UErrorCode uerr = U_ZERO_ERROR;
            int32_t start = n ? matcher->start(n, uerr) : matcher->start(uerr);
            int32_t end = n ? matcher->end(n, uerr) : matcher->end(uerr);
            unsigned sublen = end - start;
            if(sublen >= outlen)
                sublen = outlen - 1;
            sample.extract(start, sublen, out);
            out[sublen] = 0;
        }
        else
        {
            out[0] = 0;
        }
        return out;
    }

};

//---------------------------------------------------------------------------

class CCompiledUStrRegExpr : implements ICompiledUStrRegExpr
{
private:
    RegexPattern *  pattern;
    RegexMatcher *  matcher;

public:
    CCompiledUStrRegExpr(const UChar * _UregExp, bool _isCaseSensitive = false)
    {
        UErrorCode uerr = U_ZERO_ERROR;
        UParseError uperr;
        if (_isCaseSensitive)
            pattern = RegexPattern::compile(_UregExp, uperr, uerr);
        else
            pattern = RegexPattern::compile(_UregExp, UREGEX_CASE_INSENSITIVE, uperr, uerr);

        matcher = pattern ? pattern->matcher(uerr) : NULL;
        if (U_FAILURE(uerr))
        {
            char * expAscii;
            unsigned expAsciiLen;
            rtlUnicodeToEscapedStrX(expAsciiLen, expAscii, rtlUnicodeStrlen(_UregExp), _UregExp);
            std::string msg = "Bad regular expression: ";
            msg += u_errorName(uerr);
            msg += ": ";
            msg.append(expAscii, expAsciiLen);
            rtlFree(expAscii);
            delete matcher;
            delete pattern;
            matcher = NULL;
            pattern = NULL;
            rtlFail(0, msg.c_str());  //throws
        }
    }

    ~CCompiledUStrRegExpr()
    {
        if (matcher)
            delete matcher;
        if (pattern)
            delete pattern;
    }

    void replace(size32_t & outlen, UChar * & out, size32_t slen, const UChar * str, size32_t rlen, UChar const * replace) const
    {
        UnicodeString const src(str, slen);
        UErrorCode err = U_ZERO_ERROR;
        RegexMatcher * replacer = pattern->matcher(src, err);
        UnicodeString const fmt(replace, rlen);
        UnicodeString const tgt = replacer->replaceAll(fmt, err);
        outlen = tgt.length();
        out = (UChar *)rtlMalloc(outlen*2);
        tgt.extract(0, outlen, out);
        delete replacer;
    }

    IUStrRegExprFindInstance * find(const UChar * str, size32_t from, size32_t len) const
    {
        CUStrRegExprFindInstance * findInst = new CUStrRegExprFindInstance(matcher, str, from, len);
        return findInst;
    }

    void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _srcLen, const UChar * _search)
    {
        rtlRowBuilder out;
        size32_t outBytes = 0;
        UErrorCode uerr = U_ZERO_ERROR;
        UnicodeString uStrSearch;

#if U_ICU_VERSION_MAJOR_NUM>=59
        uStrSearch.setTo((const char16_t *) _search, _srcLen);
#else
        uStrSearch.setTo(_search, _srcLen);
#endif
        matcher->reset(uStrSearch);
        while (matcher->find())
        {
            uerr = U_ZERO_ERROR;
            int32_t start = matcher->start(uerr);
            if ((size32_t) start==_srcLen) break;
            int32_t end = matcher->end(uerr);
            int32_t numUChars = end - start;

            out.ensureAvailable(outBytes+numUChars*sizeof(UChar)+sizeof(size32_t));
            byte *outData = out.getbytes()+outBytes;
            * (size32_t *) outData = numUChars;
            uStrSearch.extract(start,numUChars,(UChar *) (outData+sizeof(size32_t)));

            outBytes += numUChars*sizeof(UChar) + sizeof(size32_t);
        }
        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }

};

//---------------------------------------------------------------------------

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    CCompiledUStrRegExpr * expr = new CCompiledUStrRegExpr(regExpr, isCaseSensitive);
    return expr;
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
    if (compiledExpr)
        delete (CCompiledUStrRegExpr*)compiledExpr;
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
    if (findInst)
        delete (CUStrRegExprFindInstance*)findInst;
}
#else
ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    rtlFail(0, "ICU regex disabled");
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
}
#endif

#else // _USE_BOOST_REGEX or _USE_C11_REGEX not set
ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    rtlFail(0, "Boost/C++11 regex disabled");
}

ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * findInst)
{
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    rtlFail(0, "Boost/C++11 regex disabled");
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
}
#endif // _USE_BOOST_REGEX or _USE_C11_REGEX
