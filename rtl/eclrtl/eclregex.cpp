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
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "jhash.hpp"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"

#include <memory>

//---------------------------------------------------------------------------

// PCRE2 8-bit context module variables, used for STRING and UTF-8 support
static pcre2_general_context_8 * pcre2GeneralContext8;
static pcre2_compile_context_8 * pcre2CompileContext8;
static pcre2_match_context_8 * pcre2MatchContext8;

#ifdef _USE_ICU
// PCRE2 16-bit context module variables, used for UNICODE support
static pcre2_general_context_16 * pcre2GeneralContext16;
static pcre2_compile_context_16 * pcre2CompileContext16;
static pcre2_match_context_16 * pcre2MatchContext16;
#endif // _USE_ICU

// PCRE2 memory allocation hook; size will always be in bytes
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

/**
 * @brief Handles failure reporting with a regex and throws an exception with the given error code and message.
 *
 * @param errCode       The error code indicating the type of error that occurred.
 * @param msgPrefix     The prefix to be added to the error message; can be an empty string; include a trailing space if a non-empty regex is passed.
 * @param regex         The regular expression pattern; may be an empty string.
 * @param regexLength   The length (in code points) of the regular expression pattern.
 * @param errOffset     The offset into regex at which the error occurred.
 */
static void failWithPCRE2Error(int errCode, const std::string & msgPrefix, const std::string & regex, int errOffset)
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
    if (!regex.empty())
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

/**
 * @brief Handles the failure of a regular expression operation and throws an exception with the given error code and message.
 *
 * @param errCode   The error code associated with the failure.
 * @param msg       The error message describing the failure.
 */
static void failWithPCRE2Error(int errCode, const std::string & msg)
{
    failWithPCRE2Error(errCode, msg, "", -1);
}

/**
 * @brief Handles failure reporting with Unicode regex and throws an exception with the given error code and message.
 *
 * @param errCode       The error code indicating the type of error that occurred.
 * @param msgPrefix     The prefix to be added to the error message; can be an empty string; include a trailing space if a non-empty message is passed.
 * @param regex         The regular expression pattern in UChar format.
 * @param regexLength   The length (in code points) of the regular expression pattern.
 * @param errOffset     The offset into regex at which the error occurred.
 */
static void failWithPCRE2Error(int errCode, const std::string & msgPrefix, const UChar * regex, int regexLength, int errOffset)
{
    std::string regexPattern;
    if (regex && regex[0])
    {
        char * regexStr = nullptr;
        unsigned regexStrLen;
        rtlUnicodeToEscapedStrX(regexStrLen, regexStr, regexLength, regex);
        regexPattern = std::string(regexStr, regexStrLen);
        rtlFree(regexStr);
    }
    failWithPCRE2Error(errCode, msgPrefix, regexPattern, errOffset);
}

//---------------------------------------------------------------------------

// RAII class for PCRE2 8-bit match data
class PCRE2MatchData8
{
private:
    pcre2_match_data_8 * matchData = nullptr;

public:
    PCRE2MatchData8() = default;
    PCRE2MatchData8(pcre2_match_data_8 * newData) : matchData(newData) {}
    PCRE2MatchData8(const PCRE2MatchData8 & other) = delete;
    ~PCRE2MatchData8() { pcre2_match_data_free_8(matchData); }
    operator pcre2_match_data_8 * () const { return matchData; }
    pcre2_match_data_8 * operator = (pcre2_match_data_8 * newData)
    {
        pcre2_match_data_free_8(matchData); // safe for nullptr
        matchData = newData;
        return matchData;
    }
};

//---------------------------------------------------------------------------

// RAII class for PCRE2 16-bit match data
class PCRE2MatchData16
{
private:
    pcre2_match_data_16 * matchData = nullptr;

public:
    PCRE2MatchData16() = default;
    PCRE2MatchData16(pcre2_match_data_16 * newData) : matchData(newData) {}
    PCRE2MatchData16(const PCRE2MatchData16 & other) = delete;
    ~PCRE2MatchData16() { pcre2_match_data_free_16(matchData); }
    operator pcre2_match_data_16 * () const { return matchData; }
    pcre2_match_data_16 * operator = (pcre2_match_data_16 * newData)
    {
        pcre2_match_data_free_16(matchData); // safe for nullptr
        matchData = newData;
        return matchData;
    }
};

//---------------------------------------------------------------------------

/**
 * @brief Parent class of all compiled regular expression pattern classes; used for caching.
 */
class RegexCacheEntry
{
private:
    uint32_t savedOptions = 0; // set when the object is cached
    std::string savedPattern; // used as a blob store; set when the object is cached
    std::shared_ptr<pcre2_code_8> compiledRegex8 = nullptr;
    std::shared_ptr<pcre2_code_16> compiledRegex16 = nullptr;

public:
    RegexCacheEntry() = delete;

    RegexCacheEntry(size32_t _patternSize, const char * _pattern, uint32_t _options, std::shared_ptr<pcre2_code_8> _compiledRegex8)
    : savedOptions(_options), savedPattern(_pattern, _patternSize), compiledRegex8(std::move(_compiledRegex8))
    {}

    RegexCacheEntry(size32_t _patternSize, const char * _pattern, uint32_t _options, std::shared_ptr<pcre2_code_16> _compiledRegex16)
    : savedOptions(_options), savedPattern(_pattern, _patternSize), compiledRegex16(std::move(_compiledRegex16))
    {}

    RegexCacheEntry(const RegexCacheEntry & other) = delete;

    static hash64_t hashValue(size32_t patternSize, const char * pattern, uint32_t options)
    {
        hash64_t hash = HASH64_INIT;
        hash = rtlHash64Data(patternSize, pattern, hash);
        hash = rtlHash64Data(sizeof(options), &options, hash);
        return hash;
    }

    bool hasSamePattern(size32_t patternSize, const char * pattern, uint32_t options) const
    {
        if ((patternSize == 0) || (patternSize != savedPattern.size()))
            return false;
        if (options != savedOptions)
            return false;
        return (memcmp(pattern, savedPattern.data(), patternSize) == 0);
    }

    std::shared_ptr<pcre2_code_8> getCompiledRegex8() const { return compiledRegex8; }
    std::shared_ptr<pcre2_code_16> getCompiledRegex16() const { return compiledRegex16; }
};

//---------------------------------------------------------------------------

#define DEFAULT_CACHE_MAX_SIZE 500
static CLRUCache<hash64_t, std::shared_ptr<RegexCacheEntry>> compiledStrRegExprCache(DEFAULT_CACHE_MAX_SIZE);
static CriticalSection compiledStrRegExprLock;
enum class CompiledCacheState : int
{
    Uninitialized = 0,
    Disabled = 1,
    Enabled = 2
};
static std::atomic<CompiledCacheState> compiledCacheState = CompiledCacheState::Uninitialized;

/**
 * @brief Provide an optional override to the maximum cache size for regex patterns.
 *
 * Function searches with the containerized "expert" section or the bare-metal
 * <Software/Globals> section for an optional "regex" subsection with a "cacheSize" attribute
 * By default, the maximum cache size is set to 500 patterns.  Override with 0 to disable caching.
 */
static void initMaxCacheSize() // NB: called when compiledStrRegExprLock held
{
#ifdef _CONTAINERIZED
    Owned<IPropertyTree> expert;
#else
    Owned<IPropertyTree> envtree;
    IPropertyTree * expert = nullptr;
#endif

    try
    {
#ifdef _CONTAINERIZED
        expert.setown(getGlobalConfigSP()->getPropTree("expert"));
#else
        envtree.setown(getHPCCEnvironment());
        if (envtree)
            expert = envtree->queryPropTree("Software/Globals");
#endif
    }
    catch (IException *e)
    {
        e->Release();
    }
    catch (...)
    {
    }

    size32_t cacheMaxSize = DEFAULT_CACHE_MAX_SIZE;

    if (expert)
    {
        IPropertyTree *regexProps = expert->queryPropTree("regex");
        if (regexProps)
        {
            cacheMaxSize = regexProps->getPropInt("@cacheSize", cacheMaxSize);
        }
    }

    if (cacheMaxSize > 0)
    {
        compiledStrRegExprCache.setMaxCacheSize(cacheMaxSize); // NOTE: Called from init code - no need to lock
        compiledCacheState = CompiledCacheState::Enabled;
    }
    else
        compiledCacheState = CompiledCacheState::Disabled;
}

inline bool isCacheEnabled()
{
    if (likely(CompiledCacheState::Enabled == compiledCacheState)) // common case
        return true;
    else if (compiledCacheState == CompiledCacheState::Uninitialized)
    {
        CriticalBlock b(compiledStrRegExprLock);
        if (CompiledCacheState::Uninitialized == compiledCacheState) // check again now have crit
            initMaxCacheSize();
    }
    return CompiledCacheState::Enabled == compiledCacheState;
}
//---------------------------------------------------------------------------

class CStrRegExprFindInstance final : implements IStrRegExprFindInstance
{
private:
    bool matched = false;
    std::shared_ptr<pcre2_code_8> compiledRegex = nullptr;
    PCRE2MatchData8 matchData;
    const char * subject = nullptr; // points to current subject of regex; do not free
    char * sample = nullptr; //only required if findstr/findvstr will be called
    bool isUTF8Enabled = false;;

public:
    CStrRegExprFindInstance(std::shared_ptr<pcre2_code_8> _compiledRegex, const char * _subject, size32_t _from, size32_t _len, bool _keep)
        : compiledRegex(std::move(_compiledRegex))
    {
        // See if UTF-8 is enabled on this compiled regex
        uint32_t option_bits;
        pcre2_pattern_info_8(compiledRegex.get(), PCRE2_INFO_ALLOPTIONS, &option_bits);
        isUTF8Enabled = (option_bits & PCRE2_UTF) != 0;
        // Make sure the offset and length is in code points (bytes), not characters
        size32_t subjectOffset = (isUTF8Enabled ? rtlUtf8Size(_from, _subject) : _from);
        size32_t subjectSize = (isUTF8Enabled ? rtlUtf8Size(_len, _subject) : _len);

        if (_keep)
        {
            sample = (char *)rtlMalloc(subjectSize + 1);  //required for findstr
            memcpy_iflen(sample, _subject + subjectOffset, subjectSize);
            sample[subjectSize] = '\0';
            subject = sample;
        }
        else
        {
            subject = _subject + subjectOffset;
        }

        matched = false;
        matchData = pcre2_match_data_create_from_pattern_8(compiledRegex.get(), pcre2GeneralContext8);

        int numMatches = pcre2_match_8(compiledRegex.get(), (PCRE2_SPTR8)subject, subjectSize, 0, 0, matchData, pcre2MatchContext8);

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
            memcpy_iflen(out, matchStart, outlen);
        }
        else
        {
            outlen = 0;
            out = nullptr;
        }
    }

    void getMatchXFixed(size32_t tlen, char * tgt, unsigned n = 0) const
    {
        if (tlen == 0)
            return;

        if (matched && (n < pcre2_get_ovector_count_8(matchData)))
        {
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            const char * matchStart = subject + ovector[2 * n];
            size32_t foundSize = ovector[2 * n + 1] - ovector[2 * n];
            if (foundSize <= tlen)
            {
                memcpy_iflen(tgt, matchStart, foundSize);
                memset_iflen(tgt + foundSize, ' ', tlen - foundSize);
            }
            else
            {
                if (isUTF8Enabled)
                {
                    rtlUtf8ToUtf8(tlen, tgt, foundSize, matchStart);
                }
                else
                {
                    memcpy_iflen(tgt, matchStart, tlen);
                }
            }
        }
        else
        {
            // Return an empty string
            memset_iflen(tgt, ' ', tlen);
        }
    }

    char const * findvstr(unsigned outlen, char * out, unsigned n)
    {
        if (matched && (n < pcre2_get_ovector_count_8(matchData)))
        {
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            const char * matchStart = subject + ovector[2 * n];
            unsigned substrLen = ovector[2 * n + 1] - ovector[2 * n];
            if (substrLen >= outlen)
                substrLen = outlen - 1;
            memcpy_iflen(out, matchStart, substrLen);
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

class CCompiledStrRegExpr final : implements ICompiledStrRegExpr
{
private:
    std::shared_ptr<pcre2_code_8> compiledRegex = nullptr;
    bool isUTF8Enabled = false;
    Owned<IException> deferredException;

public:
    static pcre2_code_8 * compileRegex(int _regexLength, const char * _regex, bool _isCaseSensitive, bool _enableUTF8)
    {
        int errNum = 0;
        PCRE2_SIZE errOffset;
        uint32_t options = ((_isCaseSensitive ? 0 : PCRE2_CASELESS) | (_enableUTF8 ? PCRE2_UTF : 0));
        size32_t regexSize = (_enableUTF8 ? rtlUtf8Size(_regexLength, _regex) : _regexLength);

        pcre2_code_8 * newCompiledRegex = pcre2_compile_8((PCRE2_SPTR8)_regex, regexSize, options, &errNum, &errOffset, pcre2CompileContext8);

        if (newCompiledRegex == nullptr)
        {
            failWithPCRE2Error(errNum, "Error in regex pattern: ", std::string(_regex, _regexLength), errOffset);
        }

        return newCompiledRegex;
    }

public:
    CCompiledStrRegExpr(int _regexLength, const char * _regex, bool _isCaseSensitive, bool _enableUTF8, bool _throwOnError = false)
    : isUTF8Enabled(_enableUTF8)
    {
        try
        {
            compiledRegex = std::shared_ptr<pcre2_code_8>(compileRegex(_regexLength, _regex, _isCaseSensitive, _enableUTF8), pcre2_code_free_8);
        }
        catch (IException *e)
        {
            deferredException.setown(e);
        }
    }

    CCompiledStrRegExpr(const RegexCacheEntry& cacheEntry, bool _enableUTF8)
    : compiledRegex(cacheEntry.getCompiledRegex8()), isUTF8Enabled(_enableUTF8)
    {}

    std::shared_ptr<pcre2_code_8> getCompiledRegex() const { return compiledRegex; }

    //ICompiledStrRegExpr

    void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * rstr) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        outlen = 0;
        PCRE2MatchData8 matchData = pcre2_match_data_create_from_pattern_8(compiledRegex.get(), pcre2GeneralContext8);

        // This method is often called through an ECL interface and the provided lengths
        // (slen and rlen) are in code points (characters), not bytes; we need to convert these to a
        // byte count for PCRE2
        size32_t sourceSize = (isUTF8Enabled ? rtlUtf8Size(slen, str) : slen);
        size32_t replaceSize = (isUTF8Enabled ? rtlUtf8Size(rlen, rstr) : rlen);

        // Execute an explicit match first to see if we match at all; if we do, matchData will be populated
        // with data that can be used by pcre2_substitute to bypass some work
        int numMatches = pcre2_match_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, 0, matchData, pcre2MatchContext8);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex replace: ");
        }

        if (numMatches > 0)
        {
            uint32_t replaceOptions = PCRE2_SUBSTITUTE_MATCHED|PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_EXTENDED|PCRE2_SUBSTITUTE_UNKNOWN_UNSET|PCRE2_SUBSTITUTE_UNSET_EMPTY;
            PCRE2_SIZE pcreSize = 0;

            // Call substitute once to get the size of the output (pushed into pcreSize);
            // note that pcreSize will include space for a terminating null character even though we don't want it
            int replaceResult = pcre2_substitute_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, replaceOptions|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext8, (PCRE2_SPTR8)rstr, replaceSize, nullptr, &pcreSize);

            if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
            {
                // PCRE2_ERROR_NOMEMORY is a normal result when we're just asking for the size of the output;
                // everything else is an error
                failWithPCRE2Error(replaceResult, "Error in regex replace: ");
            }

            if (pcreSize > 1)
            {
                out = (char *)rtlMalloc(pcreSize);

                replaceResult = pcre2_substitute_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, replaceOptions, matchData, pcre2MatchContext8, (PCRE2_SPTR8)rstr, replaceSize, (PCRE2_UCHAR8 *)out, &pcreSize);

                // Note that, weirdly, pcreSize will now contain the number of code points
                // in the result *excluding* the null terminator, so pcreSize will
                // become our final result length

                if (replaceResult < 0)
                {
                    failWithPCRE2Error(replaceResult, "Error in regex replace: ");
                }
            }
            else
            {
                // The replacement results in an empty string
                outlen = 0;
                out = nullptr;
                return;
            }

            // We need to return the number of characters here, not the byte count
            outlen = (isUTF8Enabled ? rtlUtf8Length(pcreSize, out) : pcreSize);
        }
        else
        {
            // No match found; return the original string
            out = (char *)rtlMalloc(sourceSize);
            memcpy_iflen(out, str, sourceSize);
            outlen = slen;
        }
    }

    void replaceTimed(ISectionTimer * timer, size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * rstr) const
    {
        SectionTimerBlock sectionTimer(timer);
        replace(outlen, out, slen, str, rlen, rstr);
    }

    // This method supports "fixed length UTF-8" even though that isn't really a thing;
    // it's here more for completeness, in case we ever implement some version of it
    void replaceFixed(size32_t tlen, char * tgt, size32_t slen, char const * str, size32_t rlen, char const * rstr) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        if (tlen == 0)
            return;

        PCRE2MatchData8 matchData = pcre2_match_data_create_from_pattern_8(compiledRegex.get(), pcre2GeneralContext8);

        // This method is often called through an ECL interface and the provided lengths
        // (slen and rlen) are in code points (characters), not bytes; we need to convert these to a
        // byte count for PCRE2
        size32_t sourceSize = (isUTF8Enabled ? rtlUtf8Size(slen, str) : slen);
        size32_t replaceSize = (isUTF8Enabled ? rtlUtf8Size(rlen, rstr) : rlen);

        // Execute an explicit match first to see if we match at all; if we do, matchData will be populated
        // with data that can be used by pcre2_substitute to bypass some work
        int numMatches = pcre2_match_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, 0, matchData, pcre2MatchContext8);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex replace: ");
        }

        if (numMatches > 0)
        {
            uint32_t replaceOptions = PCRE2_SUBSTITUTE_MATCHED|PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_EXTENDED|PCRE2_SUBSTITUTE_UNKNOWN_UNSET|PCRE2_SUBSTITUTE_UNSET_EMPTY;
            PCRE2_SIZE pcreSize = 0;

            // Call substitute once to get the size of the output and see if it will fit within fixedOutLen;
            // if it does then we can substitute within the given buffer and then pad with spaces, if not then
            // we have to allocate memory, substitute into that memory, then copy into the given buffer;
            // note that pcreSize will include space for a terminating null character even though we don't want it
            int replaceResult = pcre2_substitute_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, replaceOptions|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext8, (PCRE2_SPTR8)rstr, replaceSize, nullptr, &pcreSize);

            if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
            {
                // PCRE2_ERROR_NOMEMORY is a normal result when we're just asking for the size of the output;
                // everything else is an error
                failWithPCRE2Error(replaceResult, "Error in regex replace: ");
            }

            if (pcreSize > 1)
            {
                std::string tempBuffer;
                bool useFixedBuffer = (pcreSize <= tlen);
                char * replaceBuffer = nullptr;

                if (useFixedBuffer)
                {
                    replaceBuffer = tgt;
                }
                else
                {
                    tempBuffer.reserve(pcreSize);
                    replaceBuffer = (char *)tempBuffer.data();
                }

                replaceResult = pcre2_substitute_8(compiledRegex.get(), (PCRE2_SPTR8)str, sourceSize, 0, replaceOptions, matchData, pcre2MatchContext8, (PCRE2_SPTR8)rstr, replaceSize, (PCRE2_UCHAR8 *)replaceBuffer, &pcreSize);

                if (replaceResult < 0)
                {
                    failWithPCRE2Error(replaceResult, "Error in regex replace: ");
                }

                // Note that after a successful replace, pcreSize will contain the number of code points in
                // the result *excluding* the null terminator

                if (useFixedBuffer)
                {
                    // We used the fixed buffer so we only need to pad the result with spaces
                    if (isUTF8Enabled)
                    {
                        memset_iflen(tgt + pcreSize, ' ', tlen - rtlUtf8Length(pcreSize, tgt));
                    }
                    else
                    {
                        memset_iflen(tgt + pcreSize, ' ', tlen - pcreSize);
                    }
                }
                else
                {
                    // We used a separate buffer, so we need to copy the result into the fixed buffer;
                    // temp buffer was larger so we don't have to worry about padding
                    if (isUTF8Enabled)
                    {
                        rtlUtf8ToUtf8(tlen, tgt, pcreSize, replaceBuffer);
                    }
                    else
                    {
                        memcpy_iflen(tgt, replaceBuffer, tlen);
                    }
                    
                }
            }
            else
            {
                // The replacement results in an empty string
                memset_iflen(tgt, ' ', tlen);
            }
        }
        else
        {
            // No match found; return the original string
            if (isUTF8Enabled)
            {
                if (tlen == slen)
                {
                    memcpy_iflen(tgt, str, sourceSize);
                }
                else
                {
                    rtlUtf8ToUtf8(tlen, tgt, slen, str);
                }
            }
            else
            {
                if (slen <= tlen)
                {
                    memcpy_iflen(tgt, str, sourceSize);
                    memset_iflen(tgt + sourceSize, ' ', tlen - sourceSize);
                }
                else
                {
                    memcpy_iflen(tgt, str, tlen);
                }
            }
        }
    }

    void replaceFixedTimed(ISectionTimer * timer, size32_t tlen, char * tgt, size32_t slen, char const * str, size32_t rlen, char const * rstr) const
    {
        SectionTimerBlock sectionTimer(timer);
        replaceFixed(tlen, tgt, slen, str, rlen, rstr);
    }

    IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        CStrRegExprFindInstance * findInst = new CStrRegExprFindInstance(compiledRegex, str, from, len, needToKeepSearchString);
        return findInst;
    }

    IStrRegExprFindInstance * findTimed(ISectionTimer * timer, const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const
    {
        SectionTimerBlock sectionTimer(timer);
        return find(str, from, len, needToKeepSearchString);
    }

    void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const char * _subject)
    {
        if (deferredException)
            throw deferredException.getLink();
        
        rtlRowBuilder out;
        size32_t outBytes = 0;
        PCRE2_SIZE offset = 0;
        uint32_t matchOptions = 0;
        PCRE2_SIZE subjectSize = (isUTF8Enabled ? rtlUtf8Size(_subjectLen, _subject) : _subjectLen);
        PCRE2MatchData8 matchData = pcre2_match_data_create_from_pattern_8(compiledRegex.get(), pcre2GeneralContext8);

        // Capture groups are ignored when gathering match results into a set,
        // so we will focus on only the first match (the entire matched string);
        // then we need to repeatedly match, adjusting the offset into the
        // subject string each time, until no more matches are found

        while (offset < subjectSize)
        {
            int numMatches = pcre2_match_8(compiledRegex.get(), (PCRE2_SPTR8)_subject, subjectSize, offset, matchOptions, matchData, pcre2MatchContext8);

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
                memcpy_iflen(outData + sizeof(size32_t), matchStart, matchSize);
                outBytes += matchSize + sizeof(size32_t);

                // Update search offset (which is in code units)
                offset = ovector[1];

                // Update options for subsequent matches; these are for performance reasons
                matchOptions = (isUTF8Enabled ? PCRE2_NO_UTF_CHECK : 0);
            }
            else
            {
                // This should never happen
                break;
            }
        }

        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    };

    void getMatchSetTimed(ISectionTimer * timer, bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const char * _subject)
    {
        SectionTimerBlock sectionTimer(timer);
        getMatchSet(__isAllResult, __resultBytes, __result, _subjectLen, _subject);
    }

    void getExtractTimed(ISectionTimer * timer, bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const char * _subject)
    {
        if (deferredException)
            throw deferredException.getLink();

        SectionTimerBlock sectionTimer(timer);
        std::string subjectStr;
        std::vector<std::string> matches;
        rtlRowBuilder out;
        size32_t outBytes = 0;
        PCRE2_SIZE offset = 0;
        uint32_t matchOptions = 0;
        PCRE2_SIZE subjectSize = (isUTF8Enabled ? rtlUtf8Size(_subjectLen, _subject) : _subjectLen);
        PCRE2MatchData8 matchData = pcre2_match_data_create_from_pattern_8(compiledRegex.get(), pcre2GeneralContext8);
        uint32_t captureCount = 0;
        pcre2_pattern_info_8(compiledRegex.get(), PCRE2_INFO_CAPTURECOUNT, &captureCount);

        int numMatches = pcre2_match_8(compiledRegex.get(), (PCRE2_SPTR8)_subject, subjectSize, offset, matchOptions, matchData, pcre2MatchContext8);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex extract: ");
        }

        if (numMatches > 0 && captureCount > 0)
        {
            // Multiple match groups can actually overlap, which makes deleting
            // those characters from the source string a bit tricky; we use the
            // technique of marking each character that is matched and then,
            // after all match group characters are marked, rewriting the source
            // string without marked characters
            std::vector<bool> bytesToErase(subjectSize, false);
            bool doSourceRewrite = false;

            // Copy substrings to our match list; these will be listed in the order in which they were found;
            // we have to collect them before stuffing them into the result because the first thing in the result
            // is the possibly-rewritten source string, and we don't know what that will be until all matches
            // are processed
            matches.reserve(captureCount);
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_8(matchData);
            for (unsigned int x = 1; x <= captureCount; x++)
            {
                if (ovector[2 * x] == PCRE2_UNSET || ovector[2 * x + 1] == PCRE2_UNSET)
                {
                    // Copy an empty string for a non-matching match group; it is unlikely that
                    // this will happen without failing the entire match, but we need
                    // to be prepared for it
                    matches.emplace_back("", 0);
                }
                else
                {
                    const char * matchStart = _subject + ovector[2 * x];
                    unsigned matchSize = ovector[2 * x + 1] - ovector[2 * x]; // code units

                    // Copy match to output buffer
                    matches.emplace_back(matchStart, matchSize);

                    // Flag source characters that will be removed
                    for (unsigned y = ovector[2 * x]; y < ovector[2 * x + 1]; y++)
                    {
                        bytesToErase[y] = true;
                    }
                    doSourceRewrite = true;
                }
            }

            // Rebuild source string, skipping the characters that were matched, if needed
            if (doSourceRewrite)
            {
                subjectStr.reserve(subjectSize);
                for (size32_t x = 0; x < subjectSize; x++)
                {
                    if (!bytesToErase[x])
                    {
                        subjectStr += _subject[x];
                    }
                }
            }
            else
            {
                // No characters to erase, source string is unchanged
                subjectStr = std::string(_subject, subjectSize);
            }
        }
        else
        {
            // No matches found or no capture groups defined, so source string is unchanged
            subjectStr = std::string(_subject, subjectSize);
        }

        // ----- Build the result block

        // source string to return
        outBytes += (sizeof(size32_t) + subjectStr.size());
        // matches
        for (const auto& match : matches)
            outBytes += sizeof(size32_t) + match.size();
        
        out.ensureAvailable(outBytes);
        byte * destPtr = out.getbytes();

        // Copy source
        size32_t outStrLen = (isUTF8Enabled ? rtlUtf8Length(subjectStr.size(), subjectStr.data()) : subjectStr.size());
        *((size32_t*)destPtr) = outStrLen;
        destPtr += sizeof(size32_t);
        memcpy_iflen(destPtr, subjectStr.data(), subjectStr.size());
        destPtr += subjectStr.size();

        // Copy matches
        for (const auto& match : matches)
        {
            outStrLen = (isUTF8Enabled ? rtlUtf8Length(match.size(), match.data()) : match.size());
            *((size32_t*)destPtr) = outStrLen;
            destPtr += sizeof(size32_t);
            memcpy_iflen(destPtr, match.data(), match.size());
            destPtr += match.size();
        }

        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }

};

//---------------------------------------------------------------------------
// STRING implementation
//---------------------------------------------------------------------------

/**
 * @brief Fetches or creates a compiled string regular expression object.
 *
 * This function fetches a compiled string regular expression object from the cache if it exists,
 * or creates a new one if it doesn't. The regular expression object is created based on the provided
 * regex pattern, length, and case sensitivity flag. The created object is then cached for future use.
 *
 * @param sectionTimer      Pointer to a stat-aware section timer object; may be null.
 * @param _regexLength      The length of the regex pattern.
 * @param _regex            The regex pattern.
 * @param _isCaseSensitive  Flag indicating whether the regex pattern is case sensitive or not.
 * @return  A pointer to a copy of the fetched or created CCompiledStrRegExpr object.  The returned object
 * *        must eventually be deleted.
 */
CCompiledStrRegExpr* fetchOrCreateCompiledStrRegExpr(ISectionTimer * sectionTimer, int _regexLength, const char * _regex, bool _isCaseSensitive)
{
    if (isCacheEnabled())
    {
        CCompiledStrRegExpr * compiledObjPtr = nullptr;
        uint32_t options = (_isCaseSensitive ? 0 : PCRE2_CASELESS);
        hash64_t regexHash = RegexCacheEntry::hashValue(_regexLength, _regex, options);
        
        // Check the cache
        {
            CriticalBlock lock(compiledStrRegExprLock);
            RegexCacheEntry * cacheEntry = compiledStrRegExprCache.get(regexHash).get();

            if (cacheEntry && cacheEntry->hasSamePattern(_regexLength, _regex, options))
            {
                // Note a cache hit
                if (sectionTimer)
                    sectionTimer->addStatistic(StNumCacheHits, 1);
                // Return a new compiled pattern object based on the cached information
                return new CCompiledStrRegExpr(*cacheEntry, false);
            }

            // Create a new compiled pattern object
            compiledObjPtr = new CCompiledStrRegExpr(_regexLength, _regex, _isCaseSensitive, false);
            if (compiledObjPtr->getCompiledRegex())
            {
                // Create a cache entry for the new object
                compiledStrRegExprCache.set(regexHash, std::make_shared<RegexCacheEntry>(_regexLength, _regex, options, compiledObjPtr->getCompiledRegex()));
                // Note a cache miss (add-to-cache)
                if (sectionTimer)
                {
                    sectionTimer->addStatistic(StNumCacheAdds, 1);
                    sectionTimer->mergeStatistic(StNumPeakCacheObjects, compiledStrRegExprCache.getCacheSize());
                }
            }
        }

        return compiledObjPtr;
    }
    else
    {
        return new CCompiledStrRegExpr(_regexLength, _regex, _isCaseSensitive, false);
    }
}

//---------------------------------------------------------------------------

ECLRTL_API bool rtlSyntaxCheckStrRegExpr(int regExprLength, const char * regExpr)
{
    pcre2_code_8 * compiled = CCompiledStrRegExpr::compileRegex(regExprLength, regExpr, false, false);
    pcre2_code_free_8(compiled);
    return true;
}

ECLRTL_API bool rtlSyntaxCheckStrRegExpr(const char * regExpr)
{
    return rtlSyntaxCheckStrRegExpr(strlen(regExpr), regExpr);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledStrRegExpr(nullptr, strlen(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExprTimed(ISectionTimer * timer, const char * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledStrRegExpr(timer, strlen(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledStrRegExpr(nullptr, regExprLength, regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExprTimed(ISectionTimer * timer, int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledStrRegExpr(timer, regExprLength, regExpr, isCaseSensitive);
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
// UTF8 implementation
//---------------------------------------------------------------------------

/**
 * @brief Fetches or creates a compiled UTF-8 regular expression object.
 *
 * This function fetches a compiled UTF-8 regular expression object from the cache if it exists,
 * or creates a new one if it doesn't. The regular expression object is created based on the provided
 * regex pattern, length, and case sensitivity flag. The created object is then cached for future use.
 *
 * @param sectionTimer      Pointer to a stat-aware section timer object; may be null.
 * @param _regexLength      The length of the regex pattern, in code points.
 * @param _regex            The regex pattern.
 * @param _isCaseSensitive  Flag indicating whether the regex pattern is case sensitive or not.
 * @return  A pointer to a copy of the fetched or created CCompiledStrRegExpr object.  The returned object
 * *        must eventually be deleted.
 */
CCompiledStrRegExpr* fetchOrCreateCompiledU8StrRegExpr(ISectionTimer * sectionTimer, int _regexLength, const char * _regex, bool _isCaseSensitive)
{
    if (isCacheEnabled())
    {
        CCompiledStrRegExpr * compiledObjPtr = nullptr;
        unsigned int regexSize = rtlUtf8Size(_regexLength, _regex);
        uint32_t options = PCRE2_UTF | (_isCaseSensitive ? 0 : PCRE2_CASELESS);
        hash64_t regexHash = RegexCacheEntry::hashValue(regexSize, _regex, options);
        
        // Check the cache
        {
            CriticalBlock lock(compiledStrRegExprLock);
            RegexCacheEntry * cacheEntry = compiledStrRegExprCache.get(regexHash).get();

            if (cacheEntry && cacheEntry->hasSamePattern(regexSize, _regex, options))
            {
                // Note a cache hit
                if (sectionTimer)
                    sectionTimer->addStatistic(StNumCacheHits, 1);
                // Return a new compiled pattern object based on the cached information
                return new CCompiledStrRegExpr(*cacheEntry, true);
            }

            // Create a new compiled pattern object
            compiledObjPtr = new CCompiledStrRegExpr(_regexLength, _regex, _isCaseSensitive, true);
            if (compiledObjPtr->getCompiledRegex())
            {
                // Create a cache entry for the new object
                compiledStrRegExprCache.set(regexHash, std::make_shared<RegexCacheEntry>(regexSize, _regex, options, compiledObjPtr->getCompiledRegex()));
                // Note a cache miss (add-to-cache)
                if (sectionTimer)
                {
                    sectionTimer->addStatistic(StNumCacheAdds, 1);
                    sectionTimer->mergeStatistic(StNumPeakCacheObjects, compiledStrRegExprCache.getCacheSize());
                }
            }
        }

        return compiledObjPtr;
    }
    else
    {
        return new CCompiledStrRegExpr(_regexLength, _regex, _isCaseSensitive, true);
    }
}

//---------------------------------------------------------------------------

ECLRTL_API bool rtlSyntaxCheckU8StrRegExpr(int regExprLength, const char * regExpr)
{
    pcre2_code_8 * compiled = CCompiledStrRegExpr::compileRegex(regExprLength, regExpr, false, true);
    pcre2_code_free_8(compiled);
    return true;
}

ECLRTL_API bool rtlSyntaxCheckU8StrRegExpr(const char * regExpr)
{
    return rtlSyntaxCheckU8StrRegExpr(rtlUtf8Length(regExpr), regExpr);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExpr(const char * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledU8StrRegExpr(nullptr, rtlUtf8Length(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExprTimed(ISectionTimer * timer, const char * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledU8StrRegExpr(timer, rtlUtf8Length(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExpr(int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledU8StrRegExpr(nullptr, regExprLength, regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledU8StrRegExprTimed(ISectionTimer * timer, int regExprLength, const char * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledU8StrRegExpr(timer, regExprLength, regExpr, isCaseSensitive);
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

class CUStrRegExprFindInstance final : implements IUStrRegExprFindInstance
{
private:
    bool matched = false;
    std::shared_ptr<pcre2_code_16> compiledRegex = nullptr;
    PCRE2MatchData16 matchData;
    const UChar * subject = nullptr; // points to current subject of regex; do not free

public:
    CUStrRegExprFindInstance(std::shared_ptr<pcre2_code_16> _compiledRegex, const UChar * _subject, size32_t _from, size32_t _len)
        : compiledRegex(std::move(_compiledRegex))
    {
        subject = _subject + _from;
        matched = false;
        matchData = pcre2_match_data_create_from_pattern_16(compiledRegex.get(), pcre2GeneralContext16);
        int numMatches = pcre2_match_16(compiledRegex.get(), (PCRE2_SPTR16)subject, _len, 0, 0, matchData, pcre2MatchContext16);

        matched = numMatches > 0;

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything else as an error
            failWithPCRE2Error(numMatches, "Error in regex search: ");
        }

    }

    ~CUStrRegExprFindInstance() = default;

    //IUStrRegExprFindInstance

    bool found() const { return matched; }

    void getMatchX(unsigned & outlen, UChar * & out, unsigned n = 0) const
    {
        if (matched && (n < pcre2_get_ovector_count_16(matchData)))
        {
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_16(matchData);
            const UChar * matchStart = subject + ovector[2 * n];
            outlen = ovector[2 * n + 1] - ovector[2 * n];
            PCRE2_SIZE outSize = outlen * sizeof(UChar);
            out = (UChar *)rtlMalloc(outSize);
            memcpy_iflen(out, matchStart, outSize);
        }
        else
        {
            outlen = 0;
            out = nullptr;
        }
    }

    void getMatchXFixed(size32_t tlen, UChar * tgt, unsigned n = 0) const
    {
        if (tlen == 0)
            return;
        
        if (matched && (n < pcre2_get_ovector_count_16(matchData)))
        {
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_16(matchData);
            const UChar * matchStart = subject + ovector[2 * n];
            size32_t foundSize = ovector[2 * n + 1] - ovector[2 * n];
            if (foundSize <= tlen)
            {
                memcpy_iflen(tgt, matchStart, foundSize * sizeof(UChar));
                while (foundSize < tlen)
                    tgt[foundSize++] = ' ';
            }
            else
            {
                memcpy_iflen(tgt, matchStart, tlen * sizeof(UChar));
            }
        }
        else
        {
            // Return an empty string
            size32_t pos = 0;
            while (pos < tlen)
                tgt[pos++] = ' ';
        }
    }

    UChar const * findvstr(unsigned outlen, UChar * out, unsigned n)
    {
        if (matched && (n < pcre2_get_ovector_count_16(matchData)))
        {
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_16(matchData);
            const UChar * matchStart = subject + ovector[2 * n];
            unsigned substrLen = ovector[2 * n + 1] - ovector[2 * n];
            if (substrLen >= outlen)
                substrLen = outlen - 1;
            memcpy_iflen(out, matchStart, substrLen * sizeof(UChar));
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

class CCompiledUStrRegExpr final : implements ICompiledUStrRegExpr
{
private:
    std::shared_ptr<pcre2_code_16> compiledRegex = nullptr;
    Owned<IException> deferredException;

public:
    static pcre2_code_16 * compileRegex(int _regexLength, const UChar * _regex, bool _isCaseSensitive)
    {
        int errNum = 0;
        PCRE2_SIZE errOffset;
        uint32_t options = (PCRE2_UCP | (_isCaseSensitive ? 0 : PCRE2_CASELESS));

        pcre2_code_16 * newCompiledRegex = pcre2_compile_16((PCRE2_SPTR16)_regex, _regexLength, options, &errNum, &errOffset, pcre2CompileContext16);

        if (newCompiledRegex == nullptr)
        {
            failWithPCRE2Error(errNum, "Error in regex pattern: ", _regex, _regexLength, errOffset);
        }

        return newCompiledRegex;
    }

public:
    CCompiledUStrRegExpr(int _regexLength, const UChar * _regex, bool _isCaseSensitive = false)
    {
        try
        {
            compiledRegex = std::shared_ptr<pcre2_code_16>(compileRegex(_regexLength, _regex, _isCaseSensitive), pcre2_code_free_16);
        }
        catch (IException *e)
        {
            deferredException.setown(e);
        }
    }

    CCompiledUStrRegExpr(const RegexCacheEntry& cacheEntry)
    : compiledRegex(cacheEntry.getCompiledRegex16())
    {}

    std::shared_ptr<pcre2_code_16> getCompiledRegex() const { return compiledRegex; }

    void replace(size32_t & outlen, UChar * & out, size32_t slen, const UChar * str, size32_t rlen, UChar const * rstr) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        outlen = 0;
        PCRE2MatchData16 matchData = pcre2_match_data_create_from_pattern_16(compiledRegex.get(), pcre2GeneralContext16);

        // Execute an explicit match first to see if we match at all; if we do, matchData will be populated
        // with data that can be used by pcre2_substitute to bypass some work
        int numMatches = pcre2_match_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, 0, matchData, pcre2MatchContext16);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex replace: ");
        }

        if (numMatches > 0)
        {
            uint32_t replaceOptions = PCRE2_SUBSTITUTE_MATCHED|PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_EXTENDED|PCRE2_SUBSTITUTE_UNKNOWN_UNSET|PCRE2_SUBSTITUTE_UNSET_EMPTY;
            PCRE2_SIZE pcreSize = 0;

            // Call substitute once to get the size of the output, then allocate memory for it;
            // Note that pcreSize will include space for a terminating null character;
            // we have to allocate memory for that byte to avoid a buffer overrun,
            // but we won't count that terminating byte
            int replaceResult = pcre2_substitute_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, replaceOptions|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext16, (PCRE2_SPTR16)rstr, rlen, nullptr, &pcreSize);

            if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
            {
                // PCRE2_ERROR_NOMEMORY is a normal result when we're just asking for the size of the output
                failWithPCRE2Error(replaceResult, "Error in regex replace: ");
            }

            if (pcreSize > 1)
            {
                out = (UChar *)rtlMalloc(pcreSize * sizeof(UChar));

                replaceResult = pcre2_substitute_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, replaceOptions, matchData, pcre2MatchContext16, (PCRE2_SPTR16)rstr, rlen, (PCRE2_UCHAR16 *)out, &pcreSize);

                // Note that, weirdly, pcreSize will now contain the number of code points
                // in the result *excluding* the null terminator, so pcreSize will
                // become our final result length

                if (replaceResult < 0)
                {
                    failWithPCRE2Error(replaceResult, "Error in regex replace: ");
                }
            }
            else
            {
                // The replacement results in an empty string
                outlen = 0;
                out = nullptr;
                return;
            }

            outlen = pcreSize;
        }
        else
        {
            // No match found; return the original string
            out = (UChar *)rtlMalloc(slen * sizeof(UChar));
            memcpy_iflen(out, str, slen * sizeof(UChar));
            outlen = slen;
        }
    }

    void replaceTimed(ISectionTimer * timer, size32_t & outlen, UChar * & out, size32_t slen, const UChar * str, size32_t rlen, UChar const * rstr) const
    {
        SectionTimerBlock sectionTimer(timer);
        replace(outlen, out, slen, str, rlen, rstr);
    }

    void replaceFixed(size32_t tlen, UChar * tgt, size32_t slen, UChar const * str, size32_t rlen, UChar const * replace) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        if (tlen == 0)
            return;

        PCRE2MatchData16 matchData = pcre2_match_data_create_from_pattern_16(compiledRegex.get(), pcre2GeneralContext16);

        // Execute an explicit match first to see if we match at all; if we do, matchData will be populated
        // with data that can be used by pcre2_substitute to bypass some work
        int numMatches = pcre2_match_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, 0, matchData, pcre2MatchContext16);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex replace: ");
        }

        if (numMatches > 0)
        {
            uint32_t replaceOptions = PCRE2_SUBSTITUTE_MATCHED|PCRE2_SUBSTITUTE_GLOBAL|PCRE2_SUBSTITUTE_EXTENDED|PCRE2_SUBSTITUTE_UNKNOWN_UNSET|PCRE2_SUBSTITUTE_UNSET_EMPTY;
            PCRE2_SIZE pcreSize = 0;

            // Call substitute once to get the size of the output and see if it will fit within fixedOutLen;
            // if it does then we can substitute within the given buffer and then pad with spaces, if not then
            // we have to allocate memory, substitute into that memory, then copy into the given buffer;
            // note that pcreSize will include space for a terminating null character even though we don't want it
            int replaceResult = pcre2_substitute_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, replaceOptions|PCRE2_SUBSTITUTE_OVERFLOW_LENGTH, matchData, pcre2MatchContext16, (PCRE2_SPTR16)replace, rlen, nullptr, &pcreSize);

            if (replaceResult < 0 && replaceResult != PCRE2_ERROR_NOMEMORY)
            {
                // PCRE2_ERROR_NOMEMORY is a normal result when we're just asking for the size of the output;
                // everything else is an error
                failWithPCRE2Error(replaceResult, "Error in regex replace: ");
            }

            if (pcreSize > 1)
            {
                std::string tempBuffer;
                bool useFixedBuffer = (pcreSize <= tlen);
                UChar * replaceBuffer = nullptr;

                if (useFixedBuffer)
                {
                    replaceBuffer = tgt;
                }
                else
                {
                    tempBuffer.reserve(pcreSize * sizeof(UChar));
                    replaceBuffer = (UChar *)tempBuffer.data();
                }

                replaceResult = pcre2_substitute_16(compiledRegex.get(), (PCRE2_SPTR16)str, slen, 0, replaceOptions, matchData, pcre2MatchContext16, (PCRE2_SPTR16)replace, rlen, (PCRE2_UCHAR16 *)replaceBuffer, &pcreSize);

                if (replaceResult < 0)
                {
                    failWithPCRE2Error(replaceResult, "Error in regex replace: ");
                }

                // Note that after a successful replace, pcreSize will contain the number of code points in
                // the result *excluding* the null terminator

                if (useFixedBuffer)
                {
                    // We used the fixed buffer so we only need to pad the result with spaces
                    while (pcreSize < tlen)
                        tgt[pcreSize++] = ' ';
                }
                else
                {
                    // We used a separate buffer, so we need to copy the result into the fixed buffer;
                    // temp buffer was larger so we don't have to worry about padding
                    memcpy_iflen(tgt, replaceBuffer, (tlen * sizeof(UChar)));
                }
            }
            else
            {
                // The replacement results in an empty string
                size32_t pos = 0;
                while (pos < tlen)
                    tgt[pos++] = ' ';
            }
        }
        else
        {
            // No match found; return the original string
            if (slen <= tlen)
            {
                memcpy_iflen(tgt, str, (slen * sizeof(UChar)));
                while (slen < tlen)
                    tgt[slen++] = ' ';
            }
            else
            {
                memcpy_iflen(tgt, str, (tlen * sizeof(UChar)));
            }
        }
    }

    void replaceFixedTimed(ISectionTimer * timer, size32_t tlen, UChar * tgt, size32_t slen, UChar const * str, size32_t rlen, UChar const * replace) const
    {
        SectionTimerBlock sectionTimer(timer);
        replaceFixed(tlen, tgt, slen, str, rlen, replace);
    }

    IUStrRegExprFindInstance * find(const UChar * str, size32_t from, size32_t len) const
    {
        if (deferredException)
            throw deferredException.getLink();
        
        CUStrRegExprFindInstance * findInst = new CUStrRegExprFindInstance(compiledRegex, str, from, len);
        return findInst;
    }

    IUStrRegExprFindInstance * findTimed(ISectionTimer * timer, const UChar * str, size32_t from, size32_t len) const
    {
        SectionTimerBlock sectionTimer(timer);
        return find(str, from, len);
    }

    void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const UChar * _subject)
    {
        if (deferredException)
            throw deferredException.getLink();
        
        rtlRowBuilder out;
        size32_t outBytes = 0;
        PCRE2_SIZE offset = 0;
        uint32_t matchOptions = 0;
        PCRE2MatchData16 matchData = pcre2_match_data_create_from_pattern_16(compiledRegex.get(), pcre2GeneralContext16);

        // Capture groups are ignored when gathering match results into a set,
        // so we will focus on only the first match (the entire matched string);
        // then we need to repeatedly match, adjusting the offset into the
        // subject string each time, until no more matches are found

        while (offset < _subjectLen)
        {
            int numMatches = pcre2_match_16(compiledRegex.get(), (PCRE2_SPTR16)_subject, _subjectLen, offset, matchOptions, matchData, pcre2MatchContext16);

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
                    failWithPCRE2Error(numMatches, "Error in regex getMatchSet: ");
                }
            }
            else if (numMatches > 0)
            {
                PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_16(matchData);
                const UChar * matchStart = _subject + ovector[0];
                unsigned matchLen = ovector[1] - ovector[0];
                unsigned matchSize = matchLen * sizeof(UChar);

                // Copy match to output buffer; this is number of characters used
                // by the string, not the number of bytes
                out.ensureAvailable(outBytes + matchSize + sizeof(size32_t));
                byte * outData = out.getbytes() + outBytes;
                * (size32_t *) outData = matchLen;
                memcpy_iflen(outData + sizeof(size32_t), matchStart, matchSize);
                outBytes += matchSize + sizeof(size32_t);

                // Update offset
                offset = ovector[1];

                // Update options for subsequent matches; these are for performance reasons
                matchOptions = PCRE2_NO_UTF_CHECK;
            }
            else
            {
                // This should never happen
                break;
            }
        }

        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }

    void getMatchSetTimed(ISectionTimer * timer, bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const UChar * _subject)
    {
        SectionTimerBlock sectionTimer(timer);
        getMatchSet(__isAllResult, __resultBytes, __result, _subjectLen, _subject);
    }

    void getExtractTimed(ISectionTimer * timer, bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _subjectLen, const UChar * _subject)
    {
        if (deferredException)
            throw deferredException.getLink();

        SectionTimerBlock sectionTimer(timer);
        std::basic_string<UChar> subjectStr;
        std::vector<std::basic_string<UChar>> matches;
        rtlRowBuilder out;
        size32_t outBytes = 0;
        PCRE2_SIZE offset = 0;
        uint32_t matchOptions = 0;
        PCRE2MatchData16 matchData = pcre2_match_data_create_from_pattern_16(compiledRegex.get(), pcre2GeneralContext16);
        uint32_t captureCount = 0;
        pcre2_pattern_info_16(compiledRegex.get(), PCRE2_INFO_CAPTURECOUNT, &captureCount);

        int numMatches = pcre2_match_16(compiledRegex.get(), (PCRE2_SPTR16)_subject, _subjectLen, offset, matchOptions, matchData, pcre2MatchContext16);

        if (numMatches < 0 && numMatches != PCRE2_ERROR_NOMATCH)
        {
            // Treat everything other than PCRE2_ERROR_NOMATCH as an error
            failWithPCRE2Error(numMatches, "Error in regex extract: ");
        }

        if (numMatches > 0 && captureCount > 0)
        {
            // Multiple match groups can actually overlap, which makes deleting
            // those characters from the source string a bit tricky; we use the
            // technique of marking each character that is matched and then,
            // after all match group characters are marked, rewriting the source
            // string without marked characters
            std::vector<bool> charsToErase(_subjectLen, false);
            bool doSourceRewrite = false;

            // Copy substrings to our match list; these will be listed in the order in which they were found;
            // we have to collect them before stuffing them into the result because the first thing in the result
            // is the possibly-rewritten source string, and we don't know what that will be until all matches
            // are processed
            matches.reserve(captureCount);
            PCRE2_SIZE * ovector = pcre2_get_ovector_pointer_16(matchData);
            for (unsigned int x = 1; x <= captureCount; x++)
            {
                if (ovector[2 * x] == PCRE2_UNSET || ovector[2 * x + 1] == PCRE2_UNSET)
                {
                    // Copy an empty string for a non-matching match group; it is unlikely that
                    // this will happen without failing the entire match, but we need
                    // to be prepared for it
                    matches.emplace_back(std::basic_string<UChar>());
                }
                else
                {
                    const UChar * matchStart = _subject + ovector[2 * x];
                    unsigned matchSize = ovector[2 * x + 1] - ovector[2 * x]; // code units

                    // Copy match to output buffer
                    matches.emplace_back(matchStart, matchSize);

                    // Flag source characters that will be removed
                    for (unsigned y = ovector[2 * x]; y < ovector[2 * x + 1]; y++)
                    {
                        charsToErase[y] = true;
                    }
                    doSourceRewrite = true;
                }
            }

            // Rebuild source string, skipping the characters that were matched, if needed
            if (doSourceRewrite)
            {
                subjectStr.reserve(_subjectLen);
                for (size32_t x = 0; x < _subjectLen; x++)
                {
                    if (!charsToErase[x])
                    {
                        subjectStr += _subject[x];
                    }
                }
            }
            else
            {
                // No characters to erase, source string is unchanged
                subjectStr = std::basic_string<UChar>(_subject, _subjectLen);
            }
        }
        else
        {
            // No matches found or no capture groups defined, so source string is unchanged
            subjectStr = std::basic_string<UChar>(_subject, _subjectLen);
        }

        // ----- Build the result block

        // source string to return (size in bytes)
        outBytes += (sizeof(size32_t) + (subjectStr.size() * sizeof(UChar)));
        // matches (sizes in bytes)
        for (const auto& match : matches)
            outBytes += sizeof(size32_t) + (match.size() * sizeof(UChar));
        
        out.ensureAvailable(outBytes);
        byte * destPtr = out.getbytes();

        // Copy source
        size32_t subjectSize = subjectStr.size() * sizeof(UChar);
        *((size32_t*)destPtr) = subjectStr.size();
        destPtr += sizeof(size32_t);
        memcpy_iflen(destPtr, subjectStr.data(), subjectSize);
        destPtr += subjectSize;

        // Copy matches
        for (const auto& match : matches)
        {
            size32_t matchSize = match.size() * sizeof(UChar);
            *((size32_t*)destPtr) = match.size();
            destPtr += sizeof(size32_t);
            memcpy_iflen(destPtr, match.data(), matchSize);
            destPtr += matchSize;
        }

        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }
};

//---------------------------------------------------------------------------
// UNICODE implementation
//---------------------------------------------------------------------------

/**
 * @brief Fetches or creates a compiled Unicode regular expression object.
 *
 * This function fetches a compiled Unicode regular expression object from the cache if it exists,
 * or creates a new one if it doesn't. The regular expression object is created based on the provided
 * regex pattern, length, and case sensitivity flag. The created object is then cached for future use.
 *
 * @param sectionTimer      Pointer to a stat-aware section timer object; may be null.
 * @param _regexLength      The length of the regex pattern, in code points.
 * @param _regex            The regex pattern.
 * @param _isCaseSensitive  Flag indicating whether the regex pattern is case sensitive or not.
 * @return  A pointer to a copy of the fetched or created CCompiledUStrRegExpr object.  The returned object
 * *        must eventually be deleted.
 */
CCompiledUStrRegExpr* fetchOrCreateCompiledUStrRegExpr(ISectionTimer * sectionTimer, int _regexLength, const UChar * _regex, bool _isCaseSensitive)
{
    if (isCacheEnabled())
    {
        CCompiledUStrRegExpr * compiledObjPtr = nullptr;
        unsigned int regexSize = _regexLength * sizeof(UChar);
        uint32_t options = PCRE2_UCP | (_isCaseSensitive ? 0 : PCRE2_CASELESS);
        hash64_t regexHash = RegexCacheEntry::hashValue(regexSize, reinterpret_cast<const char *>(_regex), options);
        
        // Check the cache
        {
            CriticalBlock lock(compiledStrRegExprLock);
            RegexCacheEntry * cacheEntry = compiledStrRegExprCache.get(regexHash).get();

            if (cacheEntry && cacheEntry->hasSamePattern(regexSize, reinterpret_cast<const char *>(_regex), options))
            {
                // Note a cache hit
                if (sectionTimer)
                    sectionTimer->addStatistic(StNumCacheHits, 1);
                // Return a new copy of the cached object
                return new CCompiledUStrRegExpr(*cacheEntry);
            }

            // Create a new compiled pattern object
            compiledObjPtr = new CCompiledUStrRegExpr(_regexLength, _regex, _isCaseSensitive);
            if (compiledObjPtr->getCompiledRegex())
            {
                // Create a cache entry for the new object
                compiledStrRegExprCache.set(regexHash, std::make_shared<RegexCacheEntry>(regexSize, reinterpret_cast<const char *>(_regex), options, compiledObjPtr->getCompiledRegex()));
                // Note a cache miss (add-to-cache)
                if (sectionTimer)
                    sectionTimer->addStatistic(StNumCacheAdds, 1);
            }
        }

        return compiledObjPtr;
    }
    else
    {
        return new CCompiledUStrRegExpr(_regexLength, _regex, _isCaseSensitive);
    }
}

//---------------------------------------------------------------------------

ECLRTL_API bool rtlSyntaxCheckUStrRegExpr(int regExprLength, const UChar * regExpr)
{
    pcre2_code_16 * compiled = CCompiledUStrRegExpr::compileRegex(regExprLength, regExpr, false);
    pcre2_code_free_16(compiled);
    return true;
}

ECLRTL_API bool rtlSyntaxCheckUStrRegExpr(const UChar * regExpr)
{
    return rtlSyntaxCheckUStrRegExpr(rtlUnicodeStrlen(regExpr), regExpr);
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledUStrRegExpr(nullptr, rtlUnicodeStrlen(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExprTimed(ISectionTimer * timer, const UChar * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledUStrRegExpr(timer, rtlUnicodeStrlen(regExpr), regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(int regExprLength, const UChar * regExpr, bool isCaseSensitive)
{
    return fetchOrCreateCompiledUStrRegExpr(nullptr, regExprLength, regExpr, isCaseSensitive);
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExprTimed(ISectionTimer * timer, int regExprLength, const UChar * regExpr, bool isCaseSensitive)
{
    SectionTimerBlock sectionTimer(timer);
    return fetchOrCreateCompiledUStrRegExpr(timer, regExprLength, regExpr, isCaseSensitive);
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

#else // _USE_ICU

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive)
{
    rtlFail(0, "ICU disabled");
}

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(int regExprLength, const UChar * regExpr, bool isCaseSensitive)
{
    rtlFail(0, "ICU disabled");
}

ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiledExpr)
{
}

ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * findInst)
{
}
#endif // _USE_ICU

MODULE_INIT(INIT_PRIORITY_ECLRTL_ECLRTL)
{
    pcre2GeneralContext8 = pcre2_general_context_create_8(pcre2Malloc, pcre2Free, NULL);
    pcre2CompileContext8 = pcre2_compile_context_create_8(pcre2GeneralContext8);
    pcre2MatchContext8 = pcre2_match_context_create_8(pcre2GeneralContext8);
#ifdef _USE_ICU
    pcre2GeneralContext16 = pcre2_general_context_create_16(pcre2Malloc, pcre2Free, NULL);
    pcre2CompileContext16 = pcre2_compile_context_create_16(pcre2GeneralContext16);
    pcre2MatchContext16 = pcre2_match_context_create_16(pcre2GeneralContext16);
#endif // _USE_ICU
    return true;
}

MODULE_EXIT()
{
    pcre2_match_context_free_8(pcre2MatchContext8);
    pcre2_compile_context_free_8(pcre2CompileContext8);
    pcre2_general_context_free_8(pcre2GeneralContext8);
#ifdef _USE_ICU
    pcre2_match_context_free_16(pcre2MatchContext16);
    pcre2_compile_context_free_16(pcre2CompileContext16);
    pcre2_general_context_free_16(pcre2GeneralContext16);
#endif // _USE_ICU
}
