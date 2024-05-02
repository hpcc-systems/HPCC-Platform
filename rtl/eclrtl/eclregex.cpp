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
// set PCRE2_CODE_UNIT_WIDTH to zero (meaning, no default char bit width)
// so we can freely switch between 8 and 16 bits (STRING and UTF-8 support for
// the former, UNICODE support for the latter); this means we have to use
// width-specific functions and type declarations
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

// PCRE2 memory allocation hook
static void * pcre2Malloc(size_t size, void * /*userData*/)
{
    return rtlMalloc(size);
}

// PCRE2 memory deallocation hook
static void pcre2Free(void * block, void * /*userData*/)
{
    if (block)
        rtlFree(block);
}

/// @brief Convert a PCRE2 error code to a string and throw an exception
/// @param errCode      PCRE2 error code
/// @param msgPrefix    Prefix for error message; can be an empty string;
///                     include a trailing space if a non-empty message is passed
/// @param regex        OPTIONAL; regex pattern that was in play when error occurred
/// @param errOffset    OPTIONAL; offset in regex pattern where error occurred;
///                     ignored if regex is null
static void failWithPCRE2Error(int errCode, const char * msgPrefix, const char * regex = nullptr, int errOffset = -1)
{
    const int errBuffSize = 120;
    char errBuff[errBuffSize];
    std::string msg = msgPrefix;
    int msgLen = pcre2_get_error_message_8(errCode, (PCRE2_UCHAR8 *)errBuff, errBuffSize);
    if (msgLen > 0)
    {
        msg += errBuff;
    }
    else
    {
        msg += "PCRE2 error code: " ;
        msg += std::to_string(errCode);
        msg += " (no error message available)";
    }
    if (regex && regex[0])
    {
        msg += " (regex: '";
        msg += regex;
        msg += "'";
        if (errOffset >= 0)
        {
            msg += " at offset ";
            msg += std::to_string(errOffset);
        }
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
    CStrRegExprFindInstance(pcre2_code_8 * _compiledRegex, const char * _subject, size32_t _from, size32_t _len, bool _keep)
        : compiledRegex(_compiledRegex)
    {
        matched = false;
        sample = nullptr;
        matchData = pcre2_match_data_create_from_pattern_8(compiledRegex, pcre2GeneralContext8);

        // See if UTF-8 is enabled on this compiled regex
        uint32_t option_bits;
        pcre2_pattern_info_8(compiledRegex, PCRE2_INFO_ALLOPTIONS, &option_bits);
        bool utf8Enabled = (option_bits & PCRE2_UTF) != 0;
        size32_t from = (utf8Enabled ? rtlUtf8Size(_from, _subject) : _from);
        size32_t len = (utf8Enabled ? rtlUtf8Size(_len, _subject) : _len);

        if (_keep)
        {
            sample = (char *)rtlMalloc(len + 1);  //required for findstr
            memcpy(sample, _subject + from, len);
            sample[len] = '\0';
            subject = sample;
        }
        else
        {
            subject = _subject + from;
        }

        int numMatches = pcre2_match_8(compiledRegex, (PCRE2_SPTR8)subject, len, 0, 0, matchData, pcre2MatchContext8);

        matched = numMatches > 0;

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything else as an error
            failWithPCRE2Error(numMatches, "Error in regex search: ");
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
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            const char * matchStart = subject + ovector[2 * n];
            outlen = ovector[2 * n + 1] - ovector[2 * n];
            out = (char *)rtlMalloc(outlen);
            memcpy(out, matchStart, outlen);
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
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            const char * matchStart = subject + ovector[2 * n];
            unsigned substrLen = ovector[2 * n + 1] - ovector[2 * n];
            if (substrLen >= outlen)
                substrLen = outlen - 1;
            memcpy(out, matchStart, substrLen);
            out[substrLen] = 0;
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
    bool isUTF8Enabled = false;

public:
    CCompiledStrRegExpr(const char * _regex, bool _isCaseSensitive, bool _enableUTF8)
    : isUTF8Enabled(_enableUTF8)
    {
        int errNum = 0;
        PCRE2_SIZE errOffset;
        uint32_t options = ((_isCaseSensitive ? 0 : PCRE2_CASELESS) | (_enableUTF8 ? PCRE2_UTF : 0));

        compiledRegex = pcre2_compile_8((PCRE2_SPTR8)_regex, PCRE2_ZERO_TERMINATED, options, &errNum, &errOffset, pcre2CompileContext8);

        if (compiledRegex == nullptr)
        {
            failWithPCRE2Error(errNum, "Error in regex pattern: ", _regex, errOffset);
        }
    }

    CCompiledStrRegExpr(int _regexLength, const char * _regex, bool _isCaseSensitive, bool _enableUTF8)
    : isUTF8Enabled(_enableUTF8)
    {
        int errNum = 0;
        PCRE2_SIZE errOffset;
        uint32_t options = ((_isCaseSensitive ? 0 : PCRE2_CASELESS) | (_enableUTF8 ? PCRE2_UTF : 0));
        size32_t regexSize = (isUTF8Enabled ? rtlUtf8Size(_regexLength, _regex) : _regexLength);

        compiledRegex = pcre2_compile_8((PCRE2_SPTR8)_regex, regexSize, options, &errNum, &errOffset, pcre2CompileContext8);

        if (compiledRegex == nullptr)
        {
            failWithPCRE2Error(errNum, "Error in regex pattern: ", _regex, errOffset);
        }
    }

    ~CCompiledStrRegExpr() //CAVEAT non-virtual destructor !
    {
        pcre2_code_free_8(compiledRegex);
    }

    //ICompiledStrRegExpr

    void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const
    {
        PCRE2_SIZE pcreLen = 0;
        outlen = 0;
        pcre2_match_data_8 * matchData = pcre2_match_data_create_from_pattern_8(compiledRegex, pcre2GeneralContext8);

        // This method is often called through an ECL interface and the provided lengths
        // (slen and rlen) are in characters, not bytes; we need to convert these to a
        // byte count for PCRE2
        size32_t sourceSize = (isUTF8Enabled ? rtlUtf8Size(slen, str) : slen);
        size32_t replaceSize = (isUTF8Enabled ? rtlUtf8Size(rlen, replace) : rlen);

        // Call it once to get the size of the output, then allocate memory for it;
        // Note that pcreLen will include space for a terminating null character;
        // we have to allocate memory for that byte to avoid a buffer overrun,
        // but we won't count that terminating byte
        int replaceResult = pcre2_substitute_8(compiledRegex, (PCRE2_SPTR8)str, sourceSize, 0, PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext8, (PCRE2_SPTR8)replace, replaceSize, nullptr, &pcreLen);

        if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
        {
            // PCRE2_ERROR_NOMEMORY is a normal result when we're just asking for the size of the output
            pcre2_match_data_free_8(matchData);
            failWithPCRE2Error(replaceResult, "Error in regex replace: ");
        }

        if (pcreLen > 0)
        {
            out = (char *)rtlMalloc(pcreLen);

            replaceResult = pcre2_substitute_8(compiledRegex, (PCRE2_SPTR8)str, sourceSize, 0, PCRE2_SUBSTITUTE_GLOBAL, matchData, pcre2MatchContext8, (PCRE2_SPTR8)replace, replaceSize, (PCRE2_UCHAR8 *)out, &pcreLen);

            // Note that, weirdly, pcreLen will now contain the number of bytes
            // in the result *excluding* the null terminator, so pcreLen will
            // become our final result length

            if (replaceResult < 0)
            {
                pcre2_match_data_free_8(matchData);
                failWithPCRE2Error(replaceResult, "Error in regex replace: ");
            }
        }

        pcre2_match_data_free_8(matchData);
        // We need to return the number of characters here, not the byte count
        outlen = (isUTF8Enabled ? rtlUtf8Length(pcreLen, out) : pcreLen);
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
        PCRE2_SIZE offset = 0;
        PCRE2_SIZE subjectSize = (isUTF8Enabled ? rtlUtf8Size(_subjectLen, _subject) : _subjectLen);
        pcre2_match_data_8 * matchData = pcre2_match_data_create_from_pattern_8(compiledRegex, pcre2GeneralContext8);

        // Capture groups are ignored when gathering match results into a set,
        // so we will focus on only the first match (the entire matched string);
        // then we need to repeatedly match, adjusting the offset into the
        // subject string each time, until no more matches are found

        while (offset < subjectSize)
        {
            int numMatches = pcre2_match_8(compiledRegex, (PCRE2_SPTR8)_subject, subjectSize, offset, 0, matchData, pcre2MatchContext8);

            if (numMatches < 0)
            {
                if (numMatches == PCRE2_ERROR_NOMATCH)
                {
                    // No more matches; bail out of loop
                    break;
                }
                else
                {
                    // Treat everything else as an error
                    pcre2_match_data_free_8(matchData);
                    failWithPCRE2Error(numMatches, "Error in regex getMatchSet: ");
                }
            }
            else if (numMatches > 0)
            {
                PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
                const char * matchStart = _subject + ovector[0];
                unsigned matchSize = ovector[1] - ovector[0]; // code units

                // Copy match to output buffer
                out.ensureAvailable(outBytes + matchSize + sizeof(size32_t));
                byte * outData = out.getbytes() + outBytes;
                // Append the number of characters in the match
                * (size32_t *) outData = (isUTF8Enabled ? rtlUtf8Length(matchSize, matchStart) : matchSize);
                // Copy the bytes
                memcpy(outData + sizeof(size32_t), matchStart, matchSize);
                outBytes += matchSize + sizeof(size32_t);

                // Update search offset (which is in code units)
                offset = ovector[1];
            }
            else
            {
                // This should never happen
                break;
            }
        }

        pcre2_match_data_free_8(matchData);

        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    };

};

//---------------------------------------------------------------------------

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExpr, isCaseSensitive, false);
    return expr;
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExprLength, regExpr, isCaseSensitive, false);
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

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExpr, isCaseSensitive, true);
    return expr;
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExpr(int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    CCompiledStrRegExpr * expr = new CCompiledStrRegExpr(regExprLength, regExpr, isCaseSensitive, true);
    return expr;
}

ECLRTL_API void rtlDestroyCompiledU8StrRegExpr(ICompiledStrRegExpr * compiledExpr)
{
    if (compiledExpr)
        delete (CCompiledStrRegExpr*)compiledExpr;
}

ECLRTL_API void rtlDestroyU8StrRegExprFindInstance(IStrRegExprFindInstance * findInst)
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
