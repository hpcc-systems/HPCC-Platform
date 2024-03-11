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

// PCRE2_CODE_UNIT_WIDTH must be defined before the pcre.h include;
// set PCRE2_CODE_UNIT_WIDTH to zero (meaning, no default char bith width)
// so we can freely switch between 8 and 16 bits (STRING and UTF-8 support for
// the former, UNICODE support for the latter); this means we have to use
// width-specific functions and tyoe declarations
#define PCRE2_CODE_UNIT_WIDTH 0
#include "pcre2.h"

#include "platform.h"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "jlib.hpp"
#ifdef _USE_ICU
#include "unicode/regex.h"
#endif

#define UTF8_CODEPAGE "UTF-8"
#define UTF8_MAXSIZE 4

//---------------------------------------------------------------------------

// PCRE2 8-bit context module variables
static pcre2_general_context_8 * pcre2GeneralContext8;
static pcre2_compile_context_8 * pcre2CompileContext8;
static pcre2_match_context_8 * pcre2MatchContext8;

// PCRE2 memory hooks
static void * pcre2Malloc(size_t size, void * /*userData*/)
{
    return rtlMalloc(size);
}

static void pcre2Free(void * block, void * /*userData*/)
{
    if (block)
        rtlFree(block);
}

static void failOnPCRE2Error(int errCode, const char * msgPrefix, const char * regex = nullptr)
{
    int errBuffSize = 120;
    char errBuff[errBuffSize];
    std::string msg = msgPrefix;
    pcre2_get_error_message_8(errCode, (PCRE2_UCHAR8 *)errBuff, errBuffSize);
    msg += std::string(errBuff, errBuffSize);
    if (regex)
    {
        msg += " (regex: ";
        msg += regex;
        msg += ")";
    }
    rtlFail(0, msg.c_str());
}

//---------------------------------------------------------------------------

class CStrRegExprFindInstance : implements IStrRegExprFindInstance
{
private:
    bool matched = false;
    pcre2_code_8 * compiledRegex = nullptr; // do not free; this will be owned by caller
    pcre2_match_data_8 * matchData = nullptr;
    const char * subject = nullptr; // points to current subject of regex; do not free
    char * sample = nullptr; //only required if findstr/findvstr will be called

public:
    CStrRegExprFindInstance(pcre2_code_8 * _compiledRegex, const char * _str, size32_t _from, size32_t _len, bool _keep)
        : compiledRegex(_compiledRegex)
    {
        matched = false;
        sample = nullptr;
        matchData = pcre2_match_data_create_from_pattern_8(compiledRegex, pcre2GeneralContext8);

        if (_keep)
        {
            sample = (char *)rtlMalloc(_len + 1);  //required for findstr
            memcpy(sample, _str + _from, _len);
            sample[_len] = '\0';
            subject = sample;
        }
        else
        {
            subject = _str + _from;
        }

        int matchCode = pcre2_match_8(compiledRegex, (PCRE2_SPTR8)subject, _len, 0, 0, matchData, pcre2MatchContext8);

        matched = matchCode > 0;

        if (matchCode < 0 && matchCode != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything else as an error
            failOnPCRE2Error(matchCode, "Error in regex search: ");
        }

    }

    ~CStrRegExprFindInstance() //CAVEAT non-virtual destructor !
    {
        if (sample)
            rtlFree(sample);
        pcre2_match_data_free_8(matchData);
    }

    //IStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, char * & out, unsigned n = 0) const
    {
        if (matched && (n < pcre2_get_ovector_count_8(matchData)))
        {
            PCRE2_SIZE pcreLen;
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            PCRE2_UCHAR8 * matchStart = (PCRE2_UCHAR8 *)subject + ovector[2 * n];
            pcre2_substring_length_bynumber_8(matchData, n, &pcreLen);
            out = (char *)rtlMalloc(pcreLen);
            memcpy(out, matchStart, pcreLen);
            outlen = pcreLen;
        }
        else
        {
            outlen = 0;
            out = nullptr;
        }
    }

    char const * findvstr(unsigned outlen, char * out, unsigned n = 0)
    {
        if (matched && (n < pcre2_get_ovector_count_8(matchData)))
        {
            PCRE2_SIZE pcreLen;
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            PCRE2_UCHAR8 * matchStart = (PCRE2_UCHAR8 *)subject + ovector[2 * n];
            pcre2_substring_length_bynumber_8(matchData, n, &pcreLen);
            if (pcreLen >= outlen)
                pcreLen = outlen - 1;
            memcpy(out, matchStart, pcreLen);
            out[pcreLen] = 0;
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
    pcre2_code_8 * compiledRegex = nullptr;
    pcre2_match_data_8 * matchData = nullptr;

public:
    CCompiledStrRegExpr(const char * _regex, bool _isCaseSensitive = false)
    {
        int errNum = 0;
        PCRE2_SIZE errOffset;
        uint32_t options = (_isCaseSensitive ? 0 : PCRE2_CASELESS);

        compiledRegex = pcre2_compile_8((PCRE2_SPTR8)_regex, PCRE2_ZERO_TERMINATED, options, &errNum, &errOffset, pcre2CompileContext8);

        if (compiledRegex == nullptr)
        {
            failOnPCRE2Error(errNum, "Error in regex pattern: ", _regex);
        }

        matchData = pcre2_match_data_create_from_pattern_8(compiledRegex, pcre2GeneralContext8);
    }

    ~CCompiledStrRegExpr() //CAVEAT non-virtual destructor !
    {
        pcre2_match_data_free_8(matchData);
        pcre2_code_free_8(compiledRegex);
    }

    //ICompiledStrRegExpr

    void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const
    {
        PCRE2_SIZE pcreLen = 0;

        // Call it once to get the size of the output, then allocate memory for it
        int replaceResult = pcre2_substitute_8(compiledRegex, (PCRE2_SPTR8)str, slen, 0, PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext8, (PCRE2_SPTR8)replace, rlen, nullptr, &pcreLen);

        if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
        {
            failOnPCRE2Error(replaceResult, "Error in regex replace: ");
        }

        outlen = pcreLen;
        out = (char *)rtlMalloc(outlen);

        replaceResult = pcre2_substitute_8(compiledRegex, (PCRE2_SPTR8)str, slen, 0, PCRE2_SUBSTITUTE_GLOBAL, matchData, pcre2MatchContext8, (PCRE2_SPTR8)replace, rlen, (PCRE2_UCHAR8 *)out, &pcreLen);

        if (replaceResult < 0)
        {
            failOnPCRE2Error(replaceResult, "Error in regex replace: ");
        }
    }

    IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const
    {
        CStrRegExprFindInstance * findInst = new CStrRegExprFindInstance(compiledRegex, str, from, len, needToKeepSearchString);
        return findInst;
    }

    void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const char * _subject)
    {
        rtlRowBuilder out;
        size32_t outBytes = 0;

        int matchCode = pcre2_match_8(compiledRegex, (PCRE2_SPTR8)_subject, _subjectLen, 0, 0, matchData, pcre2MatchContext8);

        if (matchCode < 0)
        {
            if (matchCode != PCRE2_ERROR_NOMATCH)
            {
                // Treat everything else as an error
                failOnPCRE2Error(matchCode, "Error in regex getMatchSet: ");
            }
        }
        else if (matchCode > 0 )
        {
            PCRE2_SIZE pcreLen = 0;
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);

            for (unsigned int n = 0; n < pcre2_get_ovector_count_8(matchData); n++)
            {
                PCRE2_UCHAR8 * matchStart = (PCRE2_UCHAR8 *)_subject + ovector[2 * n];
                pcre2_substring_length_bynumber_8(matchData, n, &pcreLen);

                out.ensureAvailable(outBytes + pcreLen + sizeof(size32_t));
                byte *outData = out.getbytes() + outBytes;

                * (size32_t *) outData = pcreLen;
                memcpy(outData + sizeof(size32_t), matchStart, pcreLen);

                outBytes += pcreLen + sizeof(size32_t);
            }
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
            out = nullptr;
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

MODULE_INIT(INIT_PRIORITY_ECLRTL_ECLRTL)
{
    pcre2GeneralContext8 = pcre2_general_context_create_8(pcre2Malloc, pcre2Free, NULL);
    pcre2CompileContext8 = pcre2_compile_context_create_8(pcre2GeneralContext8);
    pcre2MatchContext8 = pcre2_match_context_create_8(pcre2GeneralContext8);
    return true;
}

MODULE_EXIT()
{
    pcre2_match_context_free_8(pcre2MatchContext8);
    pcre2_compile_context_free_8(pcre2CompileContext8);
    pcre2_general_context_free_8(pcre2GeneralContext8);
}
