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


#include "platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <math.h>
#include <algorithm>

#include "jstring.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jutil.hpp"
#include "junicode.hpp"

#define DOUBLE_FORMAT   "%.16g"
#define FLOAT_FORMAT    "%.7g"

#ifndef va_copy
 /* WARNING - DANGER - ASSUMES TYPICAL STACK MACHINE */
 #define va_copy(dst, src) ((void)((dst) = (src)))
#endif

static const char * TheNullStr = "";

constexpr unsigned STRING_FIRST_CHUNK_SIZE=8;
constexpr unsigned STRING_DOUBLE_LIMIT = 0x100000;          // must be a power of 2
constexpr unsigned STRING_DETACH_GRANULARITY = 16;

//===========================================================================

StringBuffer::StringBuffer()
{
    init();
}

#if 0
StringBuffer::StringBuffer(size_t initial)
{
    init();
    ensureCapacity(initial);
}
#endif

StringBuffer::StringBuffer(String & value)
{
    init();
    append(value);
}

StringBuffer::StringBuffer(const char *value)
{
    init();
    append(value);
}

StringBuffer::StringBuffer(char value)
{
    init();
    append(value);
}

StringBuffer::StringBuffer(size_t len, const char *value)
{
    init();
    append(len, value);
}

StringBuffer::StringBuffer(const StringBuffer & value)
{
    init();
    append(value);
}

StringBuffer::StringBuffer(StringBuffer && value)
{
    init();
    swapWith(value);
}

StringBuffer::~StringBuffer()
{
#ifdef CATCH_USE_AFTER_FREE
    if (buffer == internalBuffer)
        strncpy(internalBuffer, "use-after-free", InternalBufferSize-1);
    else
        strncpy(buffer, "use-after-free", curLen);
#endif
    freeBuffer();
#ifdef CATCH_USE_AFTER_FREE
    //Try and cause any use after free to access invalid memory
    curLen = 0;
    maxLen = (size_t)-1;
    buffer = nullptr;
#endif
}

void StringBuffer::freeBuffer()
{
    if (buffer != internalBuffer)
        free(buffer);
}

void StringBuffer::setBuffer(size_t buffLen, char * newBuff, size_t strLen)
{
    assertex(newBuff);
    assertex(buffLen>0 && strLen<buffLen);

    freeBuffer();
    buffer = newBuff;
    maxLen = buffLen;
    curLen = strLen;
}

void StringBuffer::_realloc(size_t newLen)
{
    if (newLen >= maxLen)
    {
        size_t newMax = maxLen;
        if (newMax == 0)
            newMax = STRING_FIRST_CHUNK_SIZE;
        if (newLen > STRING_DOUBLE_LIMIT)
        {
            newMax = (newLen + STRING_DOUBLE_LIMIT) & ~(STRING_DOUBLE_LIMIT-1);
            if (newLen >= newMax)
                throw MakeStringException(MSGAUD_operator, -1, "StringBuffer::_realloc: Request for %zu bytes oldMax = %zu", newLen, maxLen);
        }
        else
        {
            while (newLen >= newMax)
                newMax += newMax;
        }
        char * newStr;
        char * originalBuffer = (buffer == internalBuffer) ? NULL : buffer;
        if (!newMax || !(newStr=(char *)realloc(originalBuffer, newMax)))
        {
            DBGLOG("StringBuffer::_realloc: Failed to realloc = %zu, oldMax = %zu", newMax, maxLen);
            PrintStackReport();
            PrintMemoryReport();
            throw MakeStringException(MSGAUD_operator, -1, "StringBuffer::_realloc: Failed to realloc = %zu, oldMax = %zu", newMax, maxLen);
        }
        if (useInternal())
            memcpy_iflen(newStr, internalBuffer, curLen);
        buffer = newStr;
        maxLen = newMax;
    }
}


char * StringBuffer::detach()
{
    dbgassertex(buffer);
    char * result;
    if (buffer == internalBuffer)
    {
        result = (char *)malloc(curLen+1);
        memcpy_iflen(result, buffer, curLen);
    }
    else
    {
        if (maxLen>curLen+1+STRING_DETACH_GRANULARITY)
            buffer = (char *)realloc(buffer,curLen+1); // shrink
        result = buffer;
    }
    result[curLen] = '\0';          // There is always room for this null
    init();
    return result;
}

StringBuffer & StringBuffer::append(char value)
{
    ensureCapacity(1);
    buffer[curLen] = value;
    ++curLen;
    return *this;
}


StringBuffer & StringBuffer::append(unsigned char value)
{
    ensureCapacity(1);
    buffer[curLen] = value;
    ++curLen;
    return *this;
}


StringBuffer & StringBuffer::append(const char * value)
{
    if (likely(value))
    {
        size_t SourceLen = strlen(value);
        
        if (likely(SourceLen))
        {
            ensureCapacity(SourceLen);
            memcpy(buffer + curLen, value, SourceLen);
            curLen += SourceLen;
        }
    }
    return *this;
}

StringBuffer & StringBuffer::append(size_t len, const char * value)
{
    if (likely(len))
    {
        unsigned truncLen = (unsigned)len;
        assertex(truncLen == len); // MORE: StringBuffer should use size_t throughout
        if (likely(truncLen))
        {
            ensureCapacity(truncLen);
            memcpy(buffer + curLen, value, truncLen);
            curLen += truncLen;
        }
    }
    return *this;
}

StringBuffer & StringBuffer::append(const unsigned char * value)
{
    return append((const char *) value);
}

StringBuffer & StringBuffer::append(const char * value, size_t offset, size_t len)
{
    if (likely(len))
    {
        ensureCapacity(len);
        memcpy(buffer + curLen, value+offset, len);
        curLen += len;
    }
    return *this;
}

StringBuffer & StringBuffer::append(const IAtom * value)
{
    if (value)
        append(value->queryStr());
    return *this;
}

StringBuffer & StringBuffer::append(double value)
{
    size_t len = length();
    size_t newlen = appendf(DOUBLE_FORMAT, value).length();
    while (len < newlen)
    {
        switch (charAt(len))
        {
        case '.':
        case 'E':
        case 'e':
        case 'N': // Not a number/infinity
        case 'n':
            return *this;
        }
        len++;
    }
    return append(".0");
}

StringBuffer & StringBuffer::append(float value)
{
    size_t len = length();
    size_t newlen = appendf(FLOAT_FORMAT, value).length();
    while (len < newlen)
    {
        switch (charAt(len))
        {
        case '.':
        case 'E':
        case 'e':
        case 'N': // Not a number/infinity
        case 'n':
            return *this;
        }
        len++;
    }
    return append(".0");
}

StringBuffer & StringBuffer::append(int value)
{
    char temp[12];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::append(unsigned value)
{
    char temp[12];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::appendlong(long value)
{
    char temp[24];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::appendulong(unsigned long value)
{
    char temp[24];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::append(__int64 value)
{
    char temp[24];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::append(unsigned __int64 value)
{
    char temp[24];
    
    unsigned written = numtostr(temp, value);
    return append(written, temp);
}

StringBuffer & StringBuffer::append(const String & value)
{
    size_t SourceLen = value.length();
    
    ensureCapacity(SourceLen);
    value.getChars(0, SourceLen, buffer, curLen);
    curLen += SourceLen;
    return *this;
}

StringBuffer & StringBuffer::append(const IStringVal & value)
{
    return append(value.str());
}

StringBuffer & StringBuffer::append(const IStringVal * value)
{
    if (value)
        return append(value->str());
    else
        return *this;
}

StringBuffer & StringBuffer::append(const StringBuffer & value)
{
    size_t SourceLen = value.length();
    
    ensureCapacity(SourceLen);
    value.getChars(0, SourceLen, buffer + curLen);
    curLen += SourceLen;
    return *this;
}

StringBuffer & StringBuffer::appendf(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    valist_appendf(format, args);
    va_end(args);
    return *this;
}

StringBuffer & StringBuffer::appendLower(size_t len, const char * value)
{
    if (len)
    {
        ensureCapacity(len);
        const byte * from = reinterpret_cast<const byte *>(value);
        for (size_t i = 0; i < len; i++)
            buffer[curLen + i] = tolower(from[i]);
        curLen += len;
    }
    return *this;
}


StringBuffer & StringBuffer::setf(const char *format, ...)
{
    clear();
    va_list args;
    va_start(args, format);
    valist_appendf(format, args);
    va_end(args);
    return *this;
}

StringBuffer & StringBuffer::limited_valist_appendf(size_t szLimit, const char *format, va_list args)
{
#define BUF_SIZE 1024
#define MAX_BUF_SIZE (1024*1024) // limit buffer size to 1MB when doubling
    
    // handle string that is bigger that BUF_SIZE bytes
    size_t size = (0 == szLimit||szLimit>BUF_SIZE)?BUF_SIZE:szLimit;
    int len;
    va_list args2;
    va_copy(args2, args);
    try { ensureCapacity(size); }
    catch (IException *e)
    {
        StringBuffer eMsg;
        IException *e2 = MakeStringException(-1, "StringBuffer::valist_appendf(\"%s\"): vsnprintf failed or result exceeds limit (%zu): %s", format, size, e->errorMessage(eMsg).str());
        e->Release();
        throw e2;
    }
    len = _vsnprintf(buffer+curLen,size,format,args);
    if (len >= 0)
    {
        if ((size_t)len >= size)
        {
            if (szLimit && (size_t)len >= szLimit)
            {
                if ((size_t)len>szLimit)
                {
                    len = size;
                    if (len>3) memcpy(buffer+len-3, "...", 3);
                }
            }
            else
            {
                ensureCapacity(len);
                // no need for _vsnprintf since the buffer is already made big enough
                vsprintf(buffer+curLen,format,args2); 
            }
        }
    }
    else if (size == szLimit)
    {
        len = size;
        if (len>3) memcpy(buffer+len-3, "...", 3);
    }
    else
    {
        size = BUF_SIZE * 2;
        for (;;)
        {
            if (0 != szLimit && size>szLimit) size = szLimit; // if so, will be last attempt
            if (size>MAX_BUF_SIZE)
            {
                IWARNLOG("StringBuffer::valist_appendf(\"%s\"): vsnprintf exceeds limit (%zu)", format, size);
                size = szLimit = MAX_BUF_SIZE;
            }
            try { ensureCapacity(size); }
            catch (IException *e)
            {
                StringBuffer eMsg;
                IException *e2 = MakeStringException(-1, "StringBuffer::valist_appendf(\"%s\"): vsnprintf failed (%zu): %s", format, size, e->errorMessage(eMsg).str());
                e->Release();
                throw e2;
            }
            va_list args3;
            va_copy(args3, args2);
            len = _vsnprintf(buffer+curLen,size,format,args3);
            va_end(args3);
            if (len>=0) // NB: len>size not possible, 1st _vsnprintf would have handled.
                break;
            if (size == szLimit)
            {
                len = size;
                if (len>3) memcpy(buffer+len-3, "...", 3);
                break;
            }
            size <<= 1;
        }
    }
    va_end(args2);
    curLen += len;
    return *this;
}

StringBuffer & StringBuffer::appendN(size_t count, char fill)
{
    ensureCapacity(count);
    memset(buffer+curLen, fill, count);
    curLen += count;
    return *this;
}

void StringBuffer::setLength(size_t len)
{
    if (len > curLen)
    {
        ensureCapacity(len-curLen);
    }
    curLen = len;
}

char * StringBuffer::ensureCapacity(size_t max, size_t & got)
{
    if (maxLen <= curLen + max)
        _realloc(curLen + max);
    got = maxLen - curLen - 1;
    return buffer + curLen;
}

size32_t StringBuffer::lengthUtf8() const
{
    size_t chars = 0;
    for (size_t offset=0; offset < curLen; offset += readUtf8Size(buffer+offset))
        chars++;
    return (size32_t)chars; // NB: preserving return type as size32_t for backward compatibility (as might be used in serialization)
}

char * StringBuffer::reserve(size_t size)
{
    ensureCapacity(size);
    char *ret = buffer+curLen;
    curLen += size;
    return ret;
}

char * StringBuffer::reserveTruncate(size_t size)
{
    size_t newMax = curLen+size+1;
    if (buffer == internalBuffer)
    {
        if (newMax > InternalBufferSize)
        {
            char * newStr = (char *)malloc(newMax);
            if (!newStr)
                throw MakeStringException(-1, "StringBuffer::_realloc: Failed to realloc newMax = %zu, oldMax = %zu", newMax, maxLen);
            memcpy_iflen(newStr, buffer, curLen);
            buffer = newStr;
            maxLen = newMax;
        }
    }
    else if (newMax != maxLen)
    {
        char * newStr = (char *) realloc(buffer, newMax);
        if (!newStr)
            throw MakeStringException(-1, "StringBuffer::_realloc: Failed to realloc newMax = %zu, oldMax = %zu", newMax, maxLen);
        buffer = newStr;
        maxLen = newMax;
    }
    char *ret = buffer+curLen;
    curLen += size;
    return ret;
}

void StringBuffer::swapWith(StringBuffer &other)
{
    //Swap max
    size_t tempMax = maxLen;
    maxLen = other.maxLen;
    other.maxLen = tempMax;

    //Swap lengths
    size_t thisLen = curLen;
    size_t otherLen = other.curLen;
    curLen = otherLen;
    other.curLen = thisLen;

    //Swap buffers
    char * thisBuffer = buffer;
    char * otherBuffer = other.buffer;
    if (useInternal())
    {
        if (other.useInternal())
        {
            //NOTE: The c++ compiler can generate better code for the fixed size memcpy than it can
            //      if only the required characters are copied.
            char temp[InternalBufferSize];
            memcpy(temp, thisBuffer, InternalBufferSize);
            memcpy(thisBuffer, otherBuffer, InternalBufferSize);
            memcpy(otherBuffer, temp, InternalBufferSize);
            //buffers already point in the correct place
        }
        else
        {
            memcpy(other.internalBuffer, thisBuffer, InternalBufferSize);
            buffer = otherBuffer;
            other.buffer = other.internalBuffer;
        }
    }
    else
    {
        if (other.useInternal())
        {
            memcpy(internalBuffer, otherBuffer, InternalBufferSize);
            buffer = internalBuffer;
            other.buffer = thisBuffer;
        }
        else
        {
            buffer = otherBuffer;
            other.buffer = thisBuffer;
        }
    }
}

void StringBuffer::setown(StringBuffer &other)
{
    maxLen = other.maxLen;
    curLen = other.curLen;
    freeBuffer();
    if (other.useInternal())
    {
        memcpy(internalBuffer, other.internalBuffer, InternalBufferSize);
        buffer = internalBuffer;
    }
    else
    {
        buffer = other.buffer;
    }
    other.init();
}

void StringBuffer::kill()
{
    freeBuffer();
    init();
}

void StringBuffer::getChars(size_t srcBegin, size_t srcEnd, char * target) const
{
    if (srcEnd > curLen)
        srcEnd = curLen;
    const int len = srcEnd - srcBegin;
    if (target && buffer && len > 0)
        memcpy(target, buffer + srcBegin, len);
}

void StringBuffer::_insert(size_t offset, size_t insertLen)
{
    ensureCapacity(insertLen);
    memmove(buffer + offset + insertLen, buffer + offset, curLen - offset);
    curLen += insertLen;
}


StringBuffer & StringBuffer::insert(size_t offset, char value)
{
    _insert(offset, 1);
    buffer[offset] = value;
    return *this;
}

StringBuffer & StringBuffer::insert(size_t offset, const char * value)
{
    if (!value) return *this;
    
    size_t len = strlen(value);
    if (likely(len))
    {
        _insert(offset, len);
        memcpy(buffer + offset, value, len);
    }
    return *this;
}

StringBuffer & StringBuffer::insert(size_t offset, double value)
{
    char temp[36];
    sprintf(temp, "%f", value);
    insert(offset, temp);
    return *this;
}

StringBuffer & StringBuffer::insert(size_t offset, float value)
{
    return insert(offset, (double)value);
}

StringBuffer & StringBuffer::insert(size_t offset, int value)
{
    char temp[12];
    numtostr(temp, value);
    return insert(offset, temp);
}

StringBuffer & StringBuffer::insert(size_t offset, unsigned value)
{
    char temp[12];
    
    numtostr(temp, value);
    return insert(offset, temp);
}

#if 0
StringBuffer & StringBuffer::insert(size_t offset, long value)
{
    char temp[24];
    numtostr(temp, value);
    return insert(offset, temp);
}
#endif

StringBuffer & StringBuffer::insert(size_t offset, __int64 value)
{
    char temp[24];
    
    numtostr(temp, value);
    return insert(offset, temp);
}

StringBuffer & StringBuffer::insert(size_t offset, const String & value)
{
    size_t len = value.length();
    
    _insert(offset, len);
    value.getChars(0, len, buffer, offset);
    return *this;
}

StringBuffer & StringBuffer::insert(size_t offset, const StringBuffer & value)
{
    size_t len = value.length();
    
    _insert(offset, len);
    value.getChars(0, len, buffer+offset);
    return *this;
}

StringBuffer & StringBuffer::insert(size_t offset, const IStringVal & value)
{
    return insert(offset, value.str());
}

StringBuffer & StringBuffer::insert(size_t offset, const IStringVal * value)
{
    if (value)
        return insert(offset, value->str());
    else
        return *this;
}

StringBuffer & StringBuffer::newline()
{
    return append("\n"); 
}

StringBuffer & StringBuffer::pad(size_t count)
{
    ensureCapacity(count);
    memset(buffer + curLen, ' ', count);
    curLen += count;
    return *this;
}

StringBuffer & StringBuffer::padTo(size_t count)
{
    if (curLen<count)
        pad(count-curLen);
    return *this;
}

StringBuffer & StringBuffer::clip()
{
    while (curLen && isspace(buffer[curLen-1]))
        curLen--;
    return *this;
}

StringBuffer & StringBuffer::trim()
{
    return clip().trimLeft();
}

StringBuffer & StringBuffer::trimLeft()
{
    char *p;

    if (curLen==0)
        return *this;

    buffer[curLen] = 0; 
    for(p = buffer;isspace(*p);p++)
        ; 
    if (p!=buffer)
    {
        curLen -= p-buffer;
        memmove(buffer,p,curLen);
    }
        
    return *this;
}

StringBuffer & StringBuffer::remove(size_t start, size_t len)
{
    if (start > curLen) start = curLen;
    if (start + len > curLen) len = curLen - start;
    unsigned start2 = start + len;
    memmove(buffer + start, buffer + start2, curLen - start2);
    setLength(curLen - len);
    return *this;
}

StringBuffer &StringBuffer::reverse()
{
    size_t max = curLen/2;
    char * end = buffer + curLen;
    
    size_t idx;
    for (idx = 0; idx < max; idx++)
    {
        char temp = buffer[idx];
        end--;
        buffer[idx] = *end;
        *end = temp;
    }
    
    return *this;
}

MemoryBuffer & StringBuffer::deserialize(MemoryBuffer & in)
{
    unsigned len;
    in.read(len);
    append(len, (const char *)in.readDirect(len));
    return in;
}

MemoryBuffer & StringBuffer::serialize(MemoryBuffer & out) const
{
    return out.append((unsigned)curLen).append(curLen, buffer);
}

StringBuffer &StringBuffer::loadFile(const char *filename, bool binaryMode)
{
    FILE *in = fopen(filename, binaryMode?"rb":"rt");
    if (in)
    {
        char buffer[1024];
        size_t bytes;
        for (;;)
        {
            bytes = (size_t)fread(buffer, 1, sizeof(buffer), in);
            if (!bytes)
                break;
            append(buffer, 0, bytes);
        }
        fclose(in);
        return *this;
    }
    else
        throw MakeStringException(errno, "File %s could not be opened", filename);
}

StringBuffer &  StringBuffer::loadFile(IFile* f)
{
    if(!f)
        return *this;

    Owned<IFileIO> io = f->open(IFOread);
    if(!io)
        throw MakeStringException(errno, "file %s could not be opened for reading", f->queryFilename());

    char buf[2048];
    const unsigned requestedSize = sizeof(buf);
    offset_t pos = 0;
    for (;;)
    {
        size32_t len = io->read(pos, requestedSize, buf);
        if (len == 0)
            break;

        append(len, buf);
        pos += len;

        if (len != requestedSize)
            break;
    }

    return *this;
}

void StringBuffer::setCharAt(size_t offset, char value)
{
    if (offset < curLen)
        buffer[offset] = value;
}

void StringBuffer::replace(size_t offset, size_t len, const void * value)
{
    if ((offset >= curLen) || (len == 0))
        return;
    if (offset + len > curLen)
        len = curLen - offset;
    memcpy(buffer + offset, value, len);
}

StringBuffer & StringBuffer::toLowerCase()
{
    size_t l = curLen;
    for (size_t i = 0; i < l; i++)
    {
        if (isupper(buffer[i]))
            buffer[i] = tolower(buffer[i]);
    }
    return *this;
}

StringBuffer & StringBuffer::toUpperCase()
{
    size32_t l = curLen;
    for (size32_t i = 0; i < l; i++)
    {
        if (islower(buffer[i]))
            buffer[i] = toupper(buffer[i]);
    }
    return *this;
}

StringBuffer & StringBuffer::replace(char oldChar, char newChar)
{
    size_t l = curLen;
    for (size_t i = 0; i < l; i++)
    {
        if (buffer[i] == oldChar)
        {
            buffer[i] = newChar;
            if (newChar == '\0')
            {
                curLen = i;
                break;
            }
        }
    }
    return *this;
}

// Copy source to result, replacing all occurrences of "oldStr" with "newStr"
bool replaceString(StringBuffer & result, size_t lenSource, const char *source, size_t lenOldStr, const char* oldStr, size_t lenNewStr, const char* newStr, bool avoidCopyIfUnmatched)
{
    if (lenOldStr && lenSource >= lenOldStr)
    {
        size_t offset = 0;
        size_t lastCopied = 0;
        size_t maxOffset = lenSource - lenOldStr + 1;
        char firstChar = oldStr[0];
        while (offset < maxOffset)
        {
            if (unlikely(source[offset] == firstChar)
                && unlikely((lenOldStr == 1) || memcmp(source + offset, oldStr, lenOldStr)==0))
            {
                // Wait to allocate memory until a match is found
                if (!lastCopied)
                    result.ensureCapacity(lenSource); // Avoid allocating an unnecessarly large buffer and match the source string

                // If lastCopied matches the offset nothing is appended, but we can avoid a test for offset == lastCopied
                result.append(offset - lastCopied, source + lastCopied);
                result.append(lenNewStr, newStr);
                offset += lenOldStr;
                lastCopied = offset;
            }
            else
                offset++;
        }

        if (lastCopied || !avoidCopyIfUnmatched)
            result.append(lenSource - lastCopied, source + lastCopied); // Append the remaining characters

        return lastCopied != 0;
    }
    else if (!avoidCopyIfUnmatched)
        result.append(lenSource, source); // Search string does not fit in source or is empty

    return false;
}

StringBuffer &replaceVariables(StringBuffer & result, const char *source, bool exceptions, IVariableSubstitutionHelper *helper, const char* delim, const char* term)
{
    if (isEmptyString(source) || isEmptyString(delim) || isEmptyString(term))
        return result;
    size_t lenDelim = strlen(delim);
    size_t lenTerm = strlen(term);
    size_t left = strlen(source);
    size_t minLen = lenDelim + lenTerm + 1;
    while (left >= minLen)
    {
        if (memcmp(source, delim, lenDelim)==0)
        {
            const char *finger = source + lenDelim;
            const char *thumb = strstr(finger, term);
            if (thumb)
            {
                StringAttr name(finger, (size_t)(thumb - finger));
                if (helper->findVariable(name, result))
                {
                    size_t replaced = (thumb - source) + lenTerm;
                    source = thumb + lenTerm;
                    left -= replaced;
                    continue;
                }
                if (exceptions)
                    throw MakeStringException(-1, "string substitution variable %s not set", name.str());
            }
        }
        result.append(*source);
        source++;
        left--;
    }

    // there are no more possible replacements, make sure we keep the end of the original buffer
    result.append(left, source);
    return result;
}

class CEnvVariableSubstitutionHelper : public CInterfaceOf<IVariableSubstitutionHelper>
{
public:
    CEnvVariableSubstitutionHelper(){}
    virtual bool findVariable(const char *name, StringBuffer &value) override
    {
        const char *s = getenv(name);
        if (s)
            value.append(s);
        return s!=nullptr;
    }
};

StringBuffer &replaceEnvVariables(StringBuffer & result, const char *source, bool exceptions, const char* delim, const char* term)
{
    CEnvVariableSubstitutionHelper helper;
    return replaceVariables(result, source, exceptions, &helper, delim, term);
}

StringBuffer &replaceStringNoCase(StringBuffer & result, size_t lenSource, const char *source, size_t lenOldStr, const char* oldStr, size_t lenNewStr, const char* newStr)
{
    if (lenSource)
    {
        size_t left = lenSource;
        while (left >= lenOldStr)
        {
            if (memicmp(source, oldStr, lenOldStr)==0)
            {
                result.append(lenNewStr, newStr);
                source += lenOldStr;
                left -= lenOldStr;
            }
            else
            {
                result.append(*source);
                source++;
                left--;
            }
        }

        // there are no more possible replacements, make sure we keep the end of the original buffer
        result.append(left, source);
    }
    return result;
}

// this method will replace all occurrences of "oldStr" with "newStr"
StringBuffer & StringBuffer::replaceString(const char* oldStr, const char* newStr)
{
    if (curLen && oldStr)
    {
        size_t oldlen = strlen(oldStr);
        if (oldlen > curLen)
            return *this;

        size_t newlen = newStr ? strlen(newStr) : 0;
        // If target string (newStr) is shorter than or equal to search string, do an in-place replacement to avoid allocation
        if (oldlen >= newlen)
        {
            size_t maxOffset = curLen - oldlen;
            size_t offset = 0;
            size_t targetOffset = 0;
            size_t lastCopied = 0;
            char firstChar = oldStr[0];
            while (offset <= maxOffset)
            {
                if (unlikely(buffer[offset] == firstChar)
                    && unlikely((oldlen == 1) || memcmp(buffer + offset, oldStr, oldlen)==0))
                {
                    if (lastCopied != targetOffset && likely(lastCopied != offset))
                        memcpy(buffer + targetOffset, buffer + lastCopied, offset - lastCopied);
                    targetOffset += offset - lastCopied;
                    memcpy(buffer + targetOffset, newStr, newlen);
                    offset += oldlen;
                    targetOffset += newlen;
                    lastCopied = offset;
                }
                else
                    offset++;
            }
            // Copy remaining characters if in-place replacements were made
            if (lastCopied)
            {
                if (lastCopied != targetOffset && lastCopied != curLen)
                    memcpy(buffer + targetOffset, buffer + lastCopied, curLen - lastCopied);
                setLength(targetOffset + (curLen - lastCopied));
            }
        }
        else
        {
            StringBuffer temp;
            if (::replaceString(temp, curLen, buffer, oldlen, oldStr, newlen, newStr, true))
                swapWith(temp);
        }
    }
    return *this;
}

StringBuffer & StringBuffer::replaceStringNoCase(const char* oldStr, const char* newStr)
{
    if (curLen)
    {
        StringBuffer temp;
        size_t oldlen = oldStr ? strlen(oldStr) : 0;
        size_t newlen = newStr ? strlen(newStr) : 0;
        ::replaceStringNoCase(temp, curLen, buffer, oldlen, oldStr, newlen, newStr);
        swapWith(temp);
    }
    return *this;
}

StringBuffer & StringBuffer::stripChar(char oldChar)
{
    size_t delta = 0;
    size_t l = curLen;
    for (size_t i = 0; i < l; i++)
    {
        if (buffer[i] == oldChar)
            delta++;
        else if (delta)
            buffer[i-delta] = buffer[i];
    }
    if (delta)
        curLen = curLen - delta;
    return *this;
}

const char * StringBuffer::str() const
{
    buffer[curLen] = '\0';          // There is always room for this null
    return buffer;
}

//===========================================================================

VStringBuffer::VStringBuffer(const char* format, ...)
{
    va_list args;
    va_start(args,format);
    valist_appendf(format,args);
    va_end(args);
}

//===========================================================================

StringAttrBuilder::StringAttrBuilder(StringAttr & _target) : target(_target)
{
}

StringAttrBuilder::~StringAttrBuilder()
{
    target.setown(*this);
}

//===========================================================================


String::String()
{
  text = (char *)TheNullStr;
}

String::String(const char * value)
{
  text = (value ? strdup(value) : (char *)TheNullStr);
}

String::String(const char * value, int offset, int _count)
{
  text = (char *)malloc(_count+1);
  memcpy_iflen(text, value+offset, _count);
  text[_count]=0;
}

String::String(String & value)
{
  text = strdup(value.str());
}

String::String(StringBuffer & value)
{
  unsigned len = value.length();
  text = (char *)malloc(len+1);
  value.getChars(0,len,text);
  text[len] = 0;
}

String::~String()
{
    if (text != TheNullStr)
        free(text);
#ifdef CATCH_USE_AFTER_FREE
    text = nullptr;
#endif
}

char String::charAt(size32_t index) const
{
  return text[index];
}

int String::compareTo(const String & value) const
{
    return strcmp(text, value.str());
}

int String::compareTo(const char* value)  const
{
    return strcmp(text,value);
}

String * String::concat(const String & value) const
{
  StringBuffer temp(str());
  temp.append(value);
  return new String(temp.str());
}

bool String::endsWith(const String & value) const
{
  unsigned lenValue = value.length();
  unsigned len = (size32_t)strlen(text);

  if (len >= lenValue)
    return (memcmp(text+(len-lenValue),value.str(),lenValue) == 0);
  return false;
}

bool String::endsWith(const char* value) const
{
    return ::endsWith(this->text, value);
}


bool String::equals(String & value) const
{
    return strcmp(text, value.str())==0;
}


bool String::equalsIgnoreCase(const String & value) const
{
  return stricmp(text, value.str())==0;
}

void String::getBytes(int srcBegin, int srcEnd, void * dest, int dstBegin) const
{
    memcpy_iflen((char *)dest+dstBegin, text+srcBegin, srcEnd-srcBegin);
}

void String::getChars(int srcBegin, int srcEnd, void * dest, int dstBegin) const
{
    memcpy_iflen((char *)dest+dstBegin, text+srcBegin, srcEnd-srcBegin);
}

int String::hashCode() const
{
    return (int)hashc((const byte *)text,length(),0);
}

int String::indexOf(int ch) const
{
  char * match = strchr(text, ch);
  return match ? (int)(match - text) : -1;
}

int String::indexOf(int ch, int from) const
{
  char * match = strchr(text + from, ch);
  return match ? (int)(match - text) : -1;
}

int String::indexOf(const String & search) const
{
  const char * str = search.str();
  const char * match = strstr(text, str);
  return match ? (int)(match - text) : -1;
}

int String::indexOf(const String & search, int from) const
{
  const char * str = search.str();
  const char * match = strstr(text + from, str);
  return match ? (int)(match - text) : -1;
}

int String::lastIndexOf(int ch) const
{
  char * match = strrchr(text, ch);
  return match ? (int)(match - text) : -1;
}

int String::lastIndexOf(int ch, int from) const
{
  for (;(from > 0);--from)
    if (text[from] == ch)
      return from;
  return -1;
}

int String::lastIndexOf(const String & search) const
{
  assertex(!"TBD");
  return -1;
}

int String::lastIndexOf(const String & search, int from) const
{
  assertex(!"TBD");
  return -1;
}

size32_t String::length() const
{
  return (size32_t)strlen(text);
}

bool String::startsWith(String & value) const
{
  unsigned lenValue = value.length();
  const char * search = value.str();
  return (memcmp(text, search, lenValue) == 0);
}

bool String::startsWith(String & value, int offset) const
{
  unsigned lenValue = value.length();
  const char * search = value.str();
  return (memcmp(text + offset, search, lenValue) == 0);
}

bool String::startsWith(const char* value) const
{
    return ::startsWith(this->text,value);
}

String * String::substring(int beginIndex) const
{
  return new String(text+beginIndex);
}

String * String::substring(int beginIndex, int endIndex) const
{
  return new String(text, beginIndex, endIndex - beginIndex);
}

const char *String::str() const
{
  return text;
}

String * String::toLowerCase() const
{
    String *ret = new String();
    size32_t l = length();
    if (l) 
    {
        ret->text = (char *)malloc(l+1);
        for (unsigned i = 0; i < l; i++)
            ret->text[i] = tolower(text[i]);
        ret->text[l]=0;
    }
    return ret;
}

String * String::toString()
{
  Link();
  return this;
}

String * String::toUpperCase() const
{
    String *ret = new String();
    size32_t l = length();
    if (l) 
    {
        ret->text = (char *)malloc(l+1);
        for (unsigned i = 0; i < l; i++)
            ret->text[i] = toupper(text[i]);
        ret->text[l]=0;
    }
    return ret;
}

String * String::trim() const
{
    size32_t l = length();
    while (l && isspace(text[l-1]))
        l--;
    return new String(text, 0, l);
}

//------------------------------------------------

#if 0
String & String::valueOf(char value)
{
  return * new String(&value, 0, 1);
}

String & String::valueOf(const char * value)
{
  return * new String(value);
}

String & String::valueOf(const char * value, int offset, int count)
{
  return * new String(value, offset, count);
}

String & String::valueOf(double value)
{
  StringBuffer temp;
  return temp.append(value).toString();
}

String & String::valueOf(float value)
{
  StringBuffer temp;
  return temp.append(value).toString();
}

String & String::valueOf(int value)
{
  StringBuffer temp;
  return temp.append(value).toString();
}

String & String::valueOf(long value)
{
  StringBuffer temp;
  return temp.append(value).toString();
}
#endif 


//------------------------------------------------

StringAttr::StringAttr(const char * _text)
{
  text = _text ? strdup(_text) : NULL;
}

StringAttr::StringAttr(const char * _text, size_t _len)
{
    text = NULL;
    set(_text, _len);
}

StringAttr::StringAttr(const StringAttr & src)
{
    text = NULL;
    set(src.get());
}

StringAttr::StringAttr(StringAttr && src)
{
    text = src.text;
    src.text = nullptr;
}

StringAttr& StringAttr::operator = (StringAttr && from)
{
    char *temp = text;
    text = from.text;
    from.text = temp;
    return *this;
}

StringAttr& StringAttr::operator = (const StringAttr & from)
{
    set(from.get());
    return *this;
}

void StringAttr::set(const char * _text)
{
    char * oldtext = text;
    text = _text ? strdup(_text) : NULL;
    free(oldtext);
}

void StringAttr::swapWith(StringAttr & other)
{
    char * temp = text;
    text = other.text;
    other.text = temp;
}

void StringAttr::set(const char * _text, size_t _len)
{
    char * oldtext = text;
    text = (char *)malloc(_len+1);
    memcpy_iflen(text, _text, _len);
    text[_len] = 0;
    free(oldtext);
}

void StringAttr::setown(const char * _text)
{
    char * oldtext = text;
    text = (char *)_text;
    free(oldtext);
}

void StringAttr::set(const StringBuffer & source)
{
    if (source.length())
        set(source.str());
    else
        clear();
}

void StringAttr::setown(StringBuffer & source)
{
    if (source.length())
        setown(source.detach());
    else
        clear();
}

void StringAttr::toLowerCase()
{
    if (text)
    {
        char * cur = text;
        char next;
        while ((next = *cur) != 0)
        {
            if (isupper(next))
                *cur = tolower(next);
            cur++;
        }
    }
}

void StringAttr::toUpperCase()
{
    if (text)
    {
        char * cur = text;
        char next;
        while ((next = *cur) != 0)
        {
            if (islower(next))
              *cur = toupper(next);
            cur++;
        }
    }
}


StringAttrItem::StringAttrItem(const char *_text, unsigned _len)
{
    text.set(_text, _len);
}


inline char hex(char c, bool lower)
{
  if (c < 10)
    return '0' + c;
  else if (lower)
    return 'a' + c - 10;
  else
    return 'A' + c - 10;
}

StringBuffer &  StringBuffer::appendhex(unsigned char c, bool lower)
{
    append(hex(c>>4, lower));
    append(hex(c&0xF, lower));
    return *this;
}

void appendURL(StringBuffer *dest, const char *src, size32_t len, char lower, bool keepUnderscore)
{
  if (len == (size32_t)-1)
    len = (size32_t)strlen(src);
  while (len)
  {
      // isalnum seems to give weird results for chars > 127....
    unsigned char c = (unsigned char) *src;
    if (c == ' ')
      dest->append('+');
    else if (c == '_' && keepUnderscore)
      dest->append(c);
    else if ((c & 0x80) || !isalnum(*src))
    {
      dest->append('%');
      dest->appendhex(c, lower);
    }
    else
      dest->append(c);
    src++;
    len--;
  }
}

inline char translateHex(char hex)
{
    if(hex >= 'A')
        return (hex & 0xdf) - 'A' + 10;
    else
        return hex - '0';
}

inline char translateHex(char h1, char h2)
{
    return (translateHex(h1) * 16 + translateHex(h2));
}

StringBuffer &appendDecodedURL(StringBuffer &s, const char *url)
{
    if(!url)
        return s;

    while (*url)
    {
        char c = *url++;
        if (c == '+')
            c = ' ';
        else if (c == '%')
        {
            if (isxdigit(url[0]) && isxdigit(url[1]))
            {
                c = translateHex(url[0], url[1]);
                url+=2;
            }
        }
        s.append(c);
    }
    return s;
}

StringBuffer &appendDecodedURL(StringBuffer &s, size_t len, const char *url)
{
    size_t i = 0;
    while (i < len)
    {
        char c = url[i++];
        if (c == '+')
            c = ' ';
        else if (c == '%' && i + 1 < len)
        {
            if (isxdigit(url[i]) && isxdigit(url[i+1]))
            {
                c = translateHex(url[i], url[i+1]);
                i+=2;
            }
        }
        s.append(c);
    }
    return s;
}


static StringBuffer & appendStringExpandControl(StringBuffer &out, unsigned len, const char * src, bool addBreak, bool isCpp, bool isUtf8)
{
    const int minBreakPos = 0;
    const int commaBreakPos = 70;
    const int maxBreakPos = 120;

    const char * startLine = src;
    out.ensureCapacity(len+2);
    for (; len > 0; --len)
    {
        unsigned char c = *src++;
        bool insertBreak = false;
        bool allowBreak = true;
        switch (c)
        {
            case '\n':
                {
                    out.append("\\n");
                    if (src-startLine > minBreakPos)
                        insertBreak = true;
                    break;
                }
            case ',':
                {
                    out.append(c);
                    if (src-startLine > commaBreakPos)
                        insertBreak = true;
                    break;
                }
            case '\r': out.append("\\r"); break;
            case '\t': out.append("\\t"); break;
            case '"':
                if (isCpp)
                    out.append("\\");
                out.append(c); 
                break;
            case '\'':
                if (!isCpp)
                    out.append("\\");
                out.append(c); 
                break;
            case '\\': out.append("\\\\"); break;
            case '?':
                if (isCpp)
                {
                    //stop trigraphs being generated.... quote the second ?
                    out.append(c);
                    if ((len!=1) && (*src == '?'))
                    {
                        out.append('\\');
                        allowBreak = false;
                    }
                }
                else
                    out.append(c);
                break;
            default:
                //Some characters < 32 are illegal unicode, but this may have come from a string/data so output them cleanly as \ooo
                if ((c >= ' ') && (isUtf8 || (c <= 126)))
                    out.append(c);
                else
                    out.appendf("\\%03o", c); 
                break;
        }
        if (addBreak && (insertBreak || (allowBreak && src-startLine >= maxBreakPos)))
        {
            out.append("\"").newline().append("\t\t\"");
            startLine = src;
        }
    }
    return out;
}


StringBuffer & appendStringAsCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak)
{
    return appendStringExpandControl(out, len, src, addBreak, true, false);
}


StringBuffer & appendStringAsECL(StringBuffer &out, unsigned len, const char * src)
{
    return appendStringExpandControl(out, len, src, false, false, false);
}


StringBuffer & appendUtf8AsECL(StringBuffer &out, unsigned len, const char * src)
{
    return appendStringExpandControl(out, len, src, false, false, true);
}

StringBuffer & appendStringAsUtf8(StringBuffer &out, unsigned len, const char * src)
{
    for (unsigned i=0; i < len; i++)
        appendUtf8(out, (byte)src[i]);
    return out;
}


StringBuffer & appendStringAsQuotedCPP(StringBuffer &out, unsigned len, const char * src, bool addBreak)
{
    out.ensureCapacity(len+2);
    out.append('\"');
    appendStringAsCPP(out, len, src, addBreak);
    return out.append('\"');
}


StringBuffer & appendStringAsQuotedECL(StringBuffer &out, unsigned len, const char * src)
{
    out.ensureCapacity(len+2);
    out.append('\'');
    appendStringAsECL(out, len, src);
    return out.append('\'');
}


void extractItem(StringBuffer & res, const char * src, const char * sep, int whichItem, bool caps)
{  
    bool isSeparator[256];
    
    memset(isSeparator,0,sizeof(isSeparator));
    unsigned char * finger = (unsigned char *)sep;
    while (*finger !=0)
        isSeparator[*finger++] = true;
    isSeparator[0]=true;
    
    finger = (unsigned char *)src;
    unsigned char next;
    for (;;)
    { 
        while (isSeparator[(next = *finger)])
        { 
            if (next == 0) return;
            finger++;
        }
        
        if (whichItem == 0)
        { 
            while (!isSeparator[(next = *finger)])
            { 
                if (caps)
                    next = toupper(next);
                res.append(next);
                finger++;
            }
            return;
        } 

        while (!isSeparator[*finger]) 
            finger++;
            
        whichItem--;
    }
}


int utf8CharLen(unsigned char ch)
{
    //return 1 if this is an ascii character, 
    //or 0 if its not a valid utf-8 character
    if (ch < 128)
        return 1;
    if (ch < 192)
        return 0;
    
    unsigned char len = 1;
    for (unsigned char lead = ch << 1; (lead & 0x80); lead <<=1)
        len++;
    
    return len;
}

int utf8CharLen(const unsigned char *ch, unsigned maxsize)
{
    //return 1 if this is an ascii character,
    //or 0 if its not a valid utf-8 character
    if (*ch < 128)
        return 1;

    unsigned char len = utf8CharLen(*ch);
    if (len>maxsize)
        return 0;
    for (unsigned pos = 1; pos < len; pos++)
        if ((ch[pos] < 128) || (ch[pos] >= 192))
            return 0;  //its not a valid utf-8 character after all

    return len;
}

const char *encodeXML(const char *x, StringBuffer &ret, unsigned flags, unsigned len, bool utf8)
{
    //Cost of strlen is small compared to benefits of ensureCapacity and the complexity of the rest of the function.
    if (len == (unsigned)-1)
        len = strlen(x);
    // This is the minimum length that will get generated, and potentially saves large number of relocations for large strings
    ret.ensureCapacity(len);
    while (len)
    {
        switch(*x)
        {
        case '&':
            ret.append("&amp;");
            break;
        case '<':
            ret.append("&lt;");
            break;
        case '>':
            ret.append("&gt;");
            break;
        case '\"':
            ret.append("&quot;");
            break;
        case '\'':
            ret.append("&apos;");
            break;
        case ' ':
            ret.append(flags & ENCODE_SPACES?"&#32;":" ");
            break;
        case '\n':
            ret.append(flags & ENCODE_NEWLINES?"&#10;":"\n");
            break;
        case '\r':
            ret.append(flags & ENCODE_NEWLINES?"&#13;":"\r");
            break;
        case '\t':
            ret.append(flags & ENCODE_SPACES?"&#9;":"\t");
            break;
        case '\0':
            ret.append("&#xe000;");   // hack!!! Characters below 0x20 are not legal in strict xml, even encoded.
            break;
        default:
            if (*x >= ' ' && ((byte)*x) < 128)
                ret.append(*x);
            else if (*x < ' ' && *x > 0)
                ret.append("&#xe0").appendhex(*x, true).append(';'); // HACK
            else if (utf8)
            {
                unsigned chlen = utf8CharLen((const unsigned char *)x);     
                if (chlen==0 || (chlen > len)) // invalid utf8, or missing multi byte characters
                    ret.append("&#").append((unsigned int)*(unsigned char *) x).append(';');
                else
                {
                    ret.append(*x);
                    while(--chlen)
                    {
                        len--;
                        ret.append(*(++x));
                    }
                }
            }
            else
                ret.append("&#").append((unsigned int)*(unsigned char *) x).append(';');

            break;
        }
        len--;
        ++x;
    }
    return ret.str();
}

void encodeXML(const char *x, IIOStream &out, unsigned flags, unsigned len, bool utf8)
{
    while (len)
    {
        switch(*x)
        {
        case '&':
            writeStringToStream(out, "&amp;");
            break;
        case '<':
            writeStringToStream(out, "&lt;");
            break;
        case '>':
            writeStringToStream(out, "&gt;");
            break;
        case '\"':
            writeStringToStream(out, "&quot;");
            break;
        case '\'':
            writeStringToStream(out, "&apos;");
            break;
        case ' ':
            writeStringToStream(out, flags & ENCODE_SPACES?"&#32;":" ");
            break;
        case '\n':
            writeStringToStream(out, flags & ENCODE_NEWLINES?"&#10;":"\n");
            break;
        case '\r':
            writeStringToStream(out, flags & ENCODE_NEWLINES?"&#13;":"\r");
            break;
        case '\t':
            writeStringToStream(out, flags & ENCODE_SPACES?"&#9;":"\t");
            break;
        case '\0':
            if (len == (unsigned) -1)
                return;
            writeStringToStream(out, "&#xe000;");   // hack!!! Characters below 0x20 are not legal in strict xml, even encoded.
            break;
        default:
            if (*x >= ' ' && ((byte)*x) < 128)
                writeCharToStream(out, *x);
            else if (*x < ' ' && *x > 0)
            {
                writeStringToStream(out, "&#xe0");
                unsigned char c = *(unsigned char *)x;
                writeCharToStream(out, hex(c>>4, true));
                writeCharToStream(out, hex(c&0xF, true));
                writeCharToStream(out, ';'); // HACK
            }
            else if (utf8)
            {
                int chlen = utf8CharLen((const unsigned char *)x);      
                if (chlen==0)
                {
                    writeStringToStream(out, "&#");
                    char tmp[12];           
                    unsigned written = numtostr(tmp, *(unsigned char *)x);
                    out.write(written, tmp);
                    writeCharToStream(out, ';');
                }
                else
                {
                    writeCharToStream(out, *x);
                    while(--chlen)
                    {
                        if (len != (unsigned) -1)
                            len--;
                        writeCharToStream(out, *(++x));
                    }
                }
            }
            else
            {
                writeStringToStream(out, "&#");
                char tmp[12];           
                unsigned written = numtostr(tmp, *(unsigned char *)x);
                out.write(written, tmp);
                writeCharToStream(out, ';');
            }
            break;
        }
        if (len != (unsigned) -1)
            len--;
        ++x;
    }
}

static void writeUtf8(unsigned c, StringBuffer &out)
{
    if (c < 0x80)
        out.append((char)c);
    else if (c < 0x800)
    {
        out.append((char)(0xC0 | (c>>6)));
        out.append((char)(0x80 | (c & 0x3F)));
    }
    else if (c < 0x10000)
    {
        out.append((char) (0xE0 | (c>>12)));
        out.append((char) (0x80 | (c>>6 & 0x3F)));
        out.append((char) (0x80 | (c & 0x3F)));
    }
    else if (c < 0x200000)
    {
        out.append((char) (0xF0 | (c>>18)));
        out.append((char) (0x80 | (c>>12 & 0x3F)));
        out.append((char) (0x80 | (c>>6 & 0x3F)));
        out.append((char) (0x80 | (c & 0x3F)));
    }
    else if (c < 0x4000000)
    {
        out.append((char) (0xF8 | (c>>24)));
        out.append((char) (0x80 | (c>>18 & 0x3F)));
        out.append((char) (0x80 | (c>>12 & 0x3F)));
        out.append((char) (0x80 | (c>>6 & 0x3F)));
        out.append((char) (0x80 | (c & 0x3F)));
    }
    else if (c < 0x80000000)
    {
        out.append((char) (0xFC | (c>>30)));
        out.append((char) (0x80 | (c>>24 & 0x3F)));
        out.append((char) (0x80 | (c>>18 & 0x3F)));
        out.append((char) (0x80 | (c>>12 & 0x3F)));
        out.append((char) (0x80 | (c>>6 & 0x3F)));
        out.append((char) (0x80 | (c & 0x3F)));
    }
    else
        assertex(false);
}

#define JSONSTRICT
const char *decodeJSON(const char *j, StringBuffer &ret, unsigned len, const char **errMark)
{
    if (!j)
        return ret.str();
    if ((unsigned)-1 == len)
        len = (unsigned)strlen(j);
    try
    {
        for (const char *end = j+len; j<end && *j; j++)
        {
            if (*j!='\\')
                ret.append(*j);
            else
            {
                switch (*++j)
                {
                case 'u':
                {
                    j++;
                    if (end-j>=4)
                    {
                        char *endptr;
                        StringAttr s(j, 4);
                        unsigned val = strtoul(s.get(), &endptr, 16);
                        if (endptr && !*endptr)
                        {
                            writeUtf8(val, ret);
                            j+=3;
                            break;
                        }
                    }
#ifdef JSONSTRICT
                    throw MakeStringException(-1, "invalid json \\u escaped sequence");
#endif
                    ret.append(*j);
                    break;
                }
                case '\"':
                case '\\':
                case '/':
                    ret.append(*j);
                    break;
                case 'b':
                    ret.append('\b');
                    break;
                case 'f':
                    ret.append('\f');
                    break;
                case 'n':
                    ret.append('\n');
                    continue;
                case 'r':
                    ret.append('\r');
                    break;
                case 't':
                    ret.append('\t');
                    break;
                default:
                {
#ifdef JSONSTRICT
                    throw MakeStringException(-1, "invalid json escaped sequence");
#endif
                    ret.append('\\');
                    ret.append(*j);
                    break;
                }
                }
            }
        }
    }
    catch (IException *)
    {
        if (errMark) *errMark = j;
        throw;
    }
    return ret.str();
}

void decodeXML(ISimpleReadStream &in, StringBuffer &out, unsigned len)
{
    // TODO
    UNIMPLEMENTED;
}

const char *decodeXML(const char *x, StringBuffer &ret, const char **errMark, IEntityHelper *entityHelper, bool strict)
{
    if (!x)
        return ret.str();
    try
    {
        while (*x)
        {
            if ('&' == *x)
            {
                switch (x[1])
                {
                case 'a':
                    switch (x[2])
                    {
                        case 'm':
                        {
                            if ('p' == x[3] && ';' == x[4])
                            {
                                x += 5;
                                ret.append('&');
                                continue;
                            }
                            break;
                        }
                        case 'p':
                        {
                            if ('o' == x[3] && 's' == x[4] && ';' == x[5])
                            {
                                x += 6;
                                ret.append('\'');
                                continue;
                            }
                            break;
                        }
                    }
                    break;
                case 'l':
                    if ('t' == x[2] && ';' == x[3])
                    {
                        x += 4;
                        ret.append('<');
                        continue;
                    }
                    break;
                case 'g':
                    if ('t' == x[2] && ';' == x[3])
                    {
                        x += 4;
                        ret.append('>');
                        continue;
                    }
                    break;
                case 'q':
                    if ('u' == x[2] && 'o' == x[3] && 't' == x[4] && ';' == x[5])
                    {
                        x += 6;
                        ret.append('"');
                        continue;
                    }
                    break;
                case 'n':
                    if ('b' == x[2] && 's' == x[3] && 'p' == x[4] && ';' == x[5])
                    {
                        x += 6;
                        writeUtf8(0xa0, ret);
                        continue;
                    }
                    break;
                case '#':
                {
                    const char *numstart = x+2;
                    int base = 10;
                    if (*numstart == 'x')
                    {
                        base = 16;
                        numstart++;
                    }
                    char *numend;
                    unsigned val = strtoul(numstart, &numend, base);
                    if (numstart==numend || *numend != ';')
                    {
                        if (strict)
                            throw MakeStringException(-1, "invalid escaped sequence");
                    }
                    else // always convert to utf-8. Should potentially throw error if not marked as utf-8 encoded doc and out of ascii range.
                    {
                        writeUtf8(val, ret);
                        x = numend+1;
                        continue;
                    }
                    break;
                }
                case ';':
                case '\0':
                    if (strict)
                        throw MakeStringException(-1, "invalid escaped sequence");
                    break;
                default:
                    if (entityHelper)
                    {
                        const char *start=x+1;
                        const char *finger=start;
                        while (*finger && *finger != ';')
                            ++finger;
                        if (*finger == ';')
                        {
                            StringBuffer entity(finger-start, start);
                            if (entityHelper->find(entity, ret))
                            {
                                x = finger + 1;
                                continue;
                            }
                        }
                    }
                    if (strict)
                        throw MakeStringException(-1, "invalid escaped sequence");
                    break;
                }
            }
            ret.append(*x);
            ++x;
        }
    }
    catch (IException *)
    {
        if (errMark) *errMark = x;
        throw;
    }
    return ret.str();
}

StringBuffer & appendXMLOpenTag(StringBuffer &xml, const char *tag, const char *prefix, bool complete, bool close, const char *uri)
{
    if (!tag || !*tag)
        return xml;

    xml.append('<');
    appendXMLTagName(xml, tag, prefix);

    if (uri && *uri)
    {
        xml.append(" xmlns");
        if (prefix && *prefix)
            xml.append(':').append(prefix);
        xml.append("=\"").append(uri).append('\"');
    }

    if (complete)
    {
        if (close)
            xml.append('/');
        xml.append('>');
    }
    return xml;
}

jlib_decl StringBuffer &appendJSONName(StringBuffer &s, const char *name)
{
    if (!name || !*name)
        return s;
    delimitJSON(s);
    return encodeJSON(s.append('"'), name).append("\": ");
}

jlib_decl StringBuffer &appendfJSONName(StringBuffer &s, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    StringBuffer vs;
    vs.valist_appendf(format, args);
    va_end(args);
    return appendJSONName(s, vs);
}

static char hexchar[] = "0123456789ABCDEF";
jlib_decl StringBuffer &appendJSONDataValue(StringBuffer& s, const char *name, unsigned len, const void *_value)
{
    appendJSONNameOrDelimit(s, name);
    s.append('"');
    const unsigned char *value = (const unsigned char *) _value;
    for (unsigned int i = 0; i < len; i++)
        s.append(hexchar[value[i] >> 4]).append(hexchar[value[i] & 0x0f]);
    return s.append('"');
}

StringBuffer &appendJSONRealValue(StringBuffer& s, const char *name, double value)
{
    appendJSONNameOrDelimit(s, name);
    bool quoted = j_isnan(value) || j_isinf(value);
    if (quoted)
        s.append('"');
    s.append(value);
    if (quoted)
        s.append('"');
    return s;
}

inline StringBuffer &encodeJSONChar(StringBuffer &s, const char *&ch, unsigned &remaining)
{
    byte next = *ch;
    switch (next)
    {
        case '\b':
            s.append("\\b");
            break;
        case '\f':
            s.append("\\f");
            break;
        case '\n':
            s.append("\\n");
            break;
        case '\r':
            s.append("\\r");
            break;
        case '\t':
            s.append("\\t");
            break;
        case '\"':
        case '\\':
            s.append('\\');
            s.append(next);
            break;
        case '\0':
            s.append("\\u0000");
            break;
        case '\x7f':
            s.append("\\u007f");
            break;
        default:
            if (next >= ' ' && next < 128)
                s.append(next);
            else if (next < ' ')
                s.append("\\u00").appendhex(next, true);
            else //json is always supposed to be utf8 (or other unicode formats)
            {
                unsigned chlen = utf8CharLen((const unsigned char *)ch, remaining);
                if (chlen==0)
                    s.append("\\u00").appendhex(next, true);
                else
                {
                    s.append(chlen, ch);
                    ch += (chlen-1);
                    remaining -= (chlen-1);
                }
            }
            break;
    }
    ch++;
    remaining--;
    return s;
}

StringBuffer &encodeJSON(StringBuffer &s, unsigned size, const char *value)
{
    if (!value)
        return s;
    s.ensureCapacity(size); // Minimum size that will be written
    while (size)
        encodeJSONChar(s, value, size);
    return s;
}

StringBuffer &encodeJSON(StringBuffer &s, const char *value)
{
    if (!value)
        return s;
    return encodeJSON(s, strlen(value), value);
}

inline StringBuffer & encodeCSVChar(StringBuffer & encodedCSV, char ch)
{
    byte next = ch;
    switch (next)
    {
        case '\"':
            encodedCSV.append("\"");
            encodedCSV.append(next);
            break;
        //Any other character that needs to be escaped?
        default:
            encodedCSV.append(next);
            break;
    }
    return encodedCSV;
}

StringBuffer & encodeCSVColumn(StringBuffer & encodedCSV, unsigned size, const char *rawCSVCol)
{
    if (!rawCSVCol)
        return encodedCSV;
    encodedCSV.ensureCapacity(size+2); // Minimum size that will be written
    encodedCSV.append("\"");
    for (size32_t i = 0; i < size; i++)
        encodeCSVChar(encodedCSV, rawCSVCol[i]);
    encodedCSV.append("\"");
    return encodedCSV;
}

StringBuffer & encodeCSVColumn(StringBuffer & encodedCSV, const char *rawCSVCol)
{
    if (!rawCSVCol)
        return encodedCSV;
    return encodeCSVColumn(encodedCSV, strlen(rawCSVCol), rawCSVCol);
}

bool checkUnicodeLiteral(char const * str, unsigned length, unsigned & ep, StringBuffer & msg)
{
    unsigned i;
    for(i = 0; i < length; i++)
    {
        if (str[i] == '\\')
        {
            unsigned char next = str[++i];
            if (next == '\'' || next == '\\' || next == 'n' || next == 'r' || next == 't' || next == 'a' || next == 'b' || next == 'f' || next == 'v' || next == '?' || next == '"')
            {
                continue;
            }
            else if (isdigit(next) && next < '8')
            {
                unsigned count;
                for(count = 1; count < 3; count++)
                {
                    next = str[++i];
                    if(!isdigit(next) || next >= '8')
                    {
                        msg.append("3-digit numeric escape sequence contained non-octal digit: ").append(next);
                        ep = i;
                        return false;
                    }
                }
            }
            else if (next == 'u' || next == 'U')
            {
                unsigned count;
                unsigned max = (next == 'u') ? 4 : 8;
                for(count = 0; count < max; count++)
                {
                    next = str[++i];
                    if(!isdigit(next) && (!isalpha(next) || tolower(next) > 'f'))
                    {
                        msg.append((max == 4) ? '4' : '8').append("-digit unicode escape sequence contained non-hex digit: ").append(next);
                        ep = i;
                        return false;
                    }
                }
            }
            else
            {
                msg.append("Unrecognized escape sequence: ").append("\\").append(next);
                ep = i;
                return false;
            }
        }
    }
    return true;
}


void decodeCppEscapeSequence(StringBuffer & out, const char * in, bool errorIfInvalid)
{
    out.ensureCapacity((size32_t)strlen(in));
    while (*in)
    {
        char c = *in++;
        if (c == '\\')
        {
            char next = *in;
            if (next)
            {
                in++;
                switch (next)
                {
                case 'a': c = '\a'; break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case 'v': c = '\v'; break;
                case '\\':
                case '\'':
                case '?':
                case '\"': break;
                case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
                    {
                        c = next - '0';
                        if (*in >= '0' && *in <= '7')
                        {
                            c = c << 3 | (*in++-'0');
                            if (*in >= '0' && *in <= '7')
                                c = c << 3 | (*in++-'0');
                        }
                        break;
                    }
                case 'x':
                    c = 0;
                    while (isxdigit(*in))
                    {
                        next = *in++;
                        c = c << 4;
                        if (next >= '0' && next <= '9') c |= (next - '0');
                        else if (next >= 'A' && next <= 'F') c |= (next - 'A' + 10);
                        else if (next >= 'a' && next <= 'f') c |= (next - 'a' + 10);
                    }
                    break;
                default:
                    if (errorIfInvalid)
                        throw MakeStringException(1, "unrecognised character escape sequence '\\%c'", next);
                    in--;   // keep it as is.
                    break;
                }
            }
        }
        out.append(c);
    }
}



bool isPrintable(unsigned len, const char * src)
{
    while (len--)
    {
        if (!isprint(*((unsigned char *)src)))
            return false;
        src++;
    }
    return true;
}

//make this as fast as possible...
StringBuffer & appendStringAsSQL(StringBuffer & out, unsigned len, const char * src)
{
    if (!isPrintable(len, src))
    {
        out.append("X'");
        appendDataAsHex(out, len, src);
        return out.append('\'');
    }

    out.ensureCapacity(2 + len);
    out.append('\'');
    for (;;)
    {
        char * next = (char *)memchr(src, '\'', len);
        if (!next)
            break;
        unsigned chunk=(size32_t)(next-src)+1;
        out.append(chunk, src).append('\'');
        len -= chunk;
        src += chunk;
    }
    return out.append(len, src).append('\'');
}


static const char * hexText = "0123456789ABCDEF";
StringBuffer & appendDataAsHex(StringBuffer &out, unsigned len, const void * data)
{
    char * target = (char *)out.reserve(len*2);
    unsigned char * start = (unsigned char *)data;
    for (unsigned count=len; count> 0; --count)
    {
        unsigned next = *start++;
        *target++ = hexText[next >>4];
        *target++ = hexText[next & 15];
    }
    return out;
}


bool strToBool(size_t len, const char * text)
{
    switch (len)
    {
    case 4:
        if (memicmp(text, "true", 4) == 0)
            return true;
        break;
    case 3:
        if (memicmp(text, "yes", 3) == 0)
            return true;
        break;
    case 2:
        if (memicmp(text, "on", 2) == 0)
            return true;
        break;
    case 1:
        if ((memicmp(text, "t", 1) == 0) || (memicmp(text, "y", 1) == 0))
            return true;
        break;
    }
    while (len && isspace(*text))
    {
        len--;
        text++;
    }
    while (len-- && isdigit(*text))
    {
        if (*text++ != '0') return true;
    }
    return false;
}

bool strToBool(const char * text)
{
    return strToBool(strlen(text), text);
}

bool clipStrToBool(size_t len, const char * text)
{
    while (len && *text==' ')
    {
        len--;
        text++;
    }
    while (len && text[len-1]== ' ')
        len--;
    return strToBool(len, text);
}

bool clipStrToBool(const char * text)
{
    return clipStrToBool(strlen(text), text);
}


void toLower(std::string & value)
{
    //Ugly.  Because we overload tolower in windows the std::transform resolution fails.
    //Therefore assign the function to a variable to force the disambiguation.
    int (*func)(int) = tolower;
    std::transform(value.cbegin(), value.cend(), value.begin(), func);
}



StringBuffer & ncnameEscape(char const * in, StringBuffer & out)
{
    if(!isalpha(*in))
    {
        out.appendf("_%02X", static_cast<unsigned char>(*in));
        in++;
    }
    char const * finger = in;
    while(*finger)
    {
        if(!isalnum(*finger))
        {
            if(finger>in)
                out.append((size32_t)(finger-in), in);
            out.appendf("_%02X", static_cast<unsigned char>(*finger));
            in = ++finger;
        }
        else
        {
            finger++;
        }
    }
    if(finger>in)
        out.append((size32_t)(finger-in), in);
    return out;
}

StringBuffer & ncnameUnescape(char const * in, StringBuffer & out)
{
    char const * finger = in;
    while(*finger)
    {
        if(*finger == '_')
        {
            if(finger>in)
                out.append((size32_t)(finger-in), in);
            unsigned char chr = 16 * hex2num(finger[1]) + hex2num(finger[2]);
            out.append(static_cast<char>(chr));
            in = (finger+=3);
        }
        else
        {
            finger++;
        }
    }
    if(finger>in)
        out.append((size32_t)(finger-in), in);
    return out;
}



bool startsWith(const char* src, const char* prefix)
{
    while (*prefix && *prefix == *src) { src++; prefix++; }
    return *prefix==0;
}

bool startsWithIgnoreCase(const char* src, const char* prefix)
{
    while (*prefix && tolower(*prefix) == tolower(*src)) { src++; prefix++; }
    return *prefix==0;
}

bool endsWith(const char* src, const char* suffix)
{
    size_t srcLen = strlen(src);
    size_t suffixLen = strlen(suffix);
    if (suffixLen<=srcLen)
        return memcmp(suffix, src+srcLen-suffixLen, suffixLen)==0;
    return false;
}

bool endsWithIgnoreCase(const char* src, const char* suffix)
{
    size_t srcLen = strlen(src);
    size_t suffixLen = strlen(suffix);
    if (suffixLen<=srcLen)
        return memicmp(suffix, src+srcLen-suffixLen, suffixLen)==0;
    return false;
}

unsigned matchString(const char * search, const char * const * strings)
{
    for (unsigned i=0;;i++)
    {
        const char * cur = strings[i];
        if (!cur)
            return UINT_MAX;
        if (streq(search, cur))
            return i;
    }
}

char *j_strtok_r(char *str, const char *delim, char **saveptr)
{
    if (!str) 
        str = *saveptr;
    char c;
    for (;;) {
        c = *str;
        if (!c) {
            *saveptr = str;
            return NULL;
        }
        if (!strchr(delim,c))
            break;
        str++;
    }
    char *ret=str;
    do {
        c = *(++str);
    } while (c&&!strchr(delim,c));
    if (c) 
        *(str++) = 0;
    *saveptr = str;
    return ret; 
}

int j_memicmp (const void *s1, const void *s2, size32_t len) 
{
    const byte *b1 = (const byte *)s1;
    const byte *b2 = (const byte *)s2;
    int ret = 0;
    while (len&&((ret = tolower(*b1)-tolower(*b2)) == 0)) {
        b1++;
        b2++;
        len--;
    }
    return ret;
}

size32_t memcount(size32_t len, const char * str, char search)
{
    size32_t count = 0;
    for (size32_t i=0; i < len; i++)
    {
        if (str[i] == search)
            count++;
    }
    return count;
}

StringBuffer & elideString(StringBuffer & s, unsigned maxLength)
{
    if (s.length() > maxLength)
    {
        s.setLength(maxLength);
        s.append("...");
    }
    return s;
}

const char * nullText(const char * text)
{
    if (text) return text;
    return "(null)";
}

StringBuffer& StringBuffer::operator=(StringBuffer&& value)
{
    swapWith(value);
    return *this;
}

bool loadBinaryFile(StringBuffer & contents, const char *filename, bool throwOnError)
{
    int fd = open(filename, O_RDONLY);
    bool ok = false;
    if (fd != -1)
    {
        const unsigned chunkSize = 0x10000;
        ssize_t bytes;
        for (;;)
        {
            void * buffer = contents.reserve(chunkSize);
            bytes = (size_t)read(fd, buffer, chunkSize);
            if (bytes != chunkSize)
                break;
        }

        if (bytes >= 0)
            ok = true;
        else
            bytes = 0;
        contents.setLength(contents.length() - (chunkSize - bytes));
        close(fd);
    }
    else if (throwOnError)
        throw MakeStringException(errno, "File %s could not be opened", filename);

    return ok;
}


//Support option1=value1,option2(value2),option3,option4(nested=value,nested(value))
// option1: value1
// option2: value2
// option3: 1
// option4: nested=value,nested(value)
void processOptionString(const char * options, optionCallback callback)
{
    if (!options || !callback)
        return;

    StringBuffer option;
    StringBuffer value;
    while (true)
    {
        const char * start = options;
        const char * endname = nullptr;
        const char * end = nullptr;
        const char * comma = nullptr;
        unsigned nesting = 0;
        for (;;)
        {
            byte next = *options;
            if (next == '\0')
                break;
            else if (next == '(')
            {
                if ((nesting++ == 0) && !endname)
                    endname = options;
            }
            else if (next == ')')
            {
                if (nesting && --nesting==0 && !end)
                {
                    end = options;
                    //All text after the closing ) until a comma is ignored.
                }
            }
            else if (next == '=')
            {
                if (!nesting && !endname)
                {
                    endname = options;
                }
            }
            else if (next == ',' && nesting == 0)
            {
                if (!end)
                    end = options;
                comma = options;
                break;
            }
            options++;
        }

        option.clear();
        value.clear();
        if (endname)
        {
            option.append(endname-start, start);
            if (end)
                value.append(end-(endname+1), endname+1);
            else
                value.append(endname+1);
        }
        else
        {
            value.append("1");
            if (end)
                option.append(end-start, start);
            else
                option.append(start);
        }

        if (option.length())
            callback(option, value);

        if (!comma)
            break;

        options = comma+1;
    }
}

void getSnakeCase(StringBuffer & out, const char * camelValue)
{
    if (isEmptyString(camelValue))
        return;

    out.append((char)tolower(*camelValue++));

    for (;;)
    {
        byte next = *camelValue++;
        if (!next)
            break;

        if (isupper(next))
        {
            out.append('_');
            out.append((char)tolower(next));
        }
        else
            out.append((char)next);
    }
}

void ensureSeparator(StringBuffer & out, char separator)
{
    if (out.length() && (out.charAt(out.length()-1) != separator))
        out.append(separator);
}

/**
 * stristr - Case insensitive strstr()
 * @haystack: Where we will search for our @needle
 * @needle:   Search pattern.
 *
 * Description:
 * This function is an ANSI version of strstr() with case insensitivity.
 *
 * It is a commodity funciton found on the web, cut'd, 'n', pasted..
 * URL: http://www.brokersys.com/snippets/STRISTR.C
 *
 * Hereby donated to public domain.
 *
 * Returns:  char *pointer if needle is found in haystack, otherwise NULL.
 *
 * Rev History:  11/30/23                    JLibify
 *               01/20/05  Joachim Nilsson   Cleanups
 *               07/04/95  Bob Stout         ANSI-fy
 *               02/03/94  Fred Cole         Original
 */

const char * stristr (const char *haystack, const char *needle)
{
    if (isEmptyString(haystack) || isEmptyString(needle))
        return nullptr;

    const char * start = haystack;
    size_t slen  = strlen(haystack); /* Total size of haystack   */
    size_t nlen  = strlen(needle);   /* Length of our needle     */
    char needle0 = tolower(*needle);
    for (; slen >= nlen; start++, slen--)
    {
        if (tolower(*start) == needle0)
        {
            size_t i = 1;
            for (;;)
            {
                if (i == nlen)
                    return start;
                if (tolower(start[i]) != tolower(needle[i]))
                    break;
                i++;
            }
        }
    }
    return nullptr;
}


const void * jmemmem(size_t lenHaystack, const void * haystack, size_t lenNeedle, const void *needle)
{
    if (lenNeedle == 0)
        return haystack;

    if (lenHaystack < lenNeedle)
        return nullptr;

    const char * search = (const char *)needle;
    char first = *search;
    if (lenNeedle == 1)
        return memchr(haystack, first, lenHaystack);

    const char * buffer = (const char *)haystack;
    for (size_t i = 0; i <= lenHaystack - lenNeedle; i++)
    {
        //Special case the first character to avoid a function call each iteration.
        if (buffer[i] == first)
        {
            if (memcmp(buffer + i + 1, search + 1, lenNeedle-1) == 0)
                return buffer + i;
        }
    }

    return nullptr;
}

/**
 * For preventing command injection, sanitize the argument to be passed to the system command.
 * - Quote the entire argument with single quotes to prevent interpretation of shell metacharacters.
 * - Since a single-quoted string can't contain single quotes, even escaped, replace each single
 *   quote in the argument with the sequence '"'"' . That closes the single quoted string, appends
 *   a literal single quote, and reopens the single quoted string
 */
StringBuffer& sanitizeCommandArg(const char* arg, StringBuffer& sanitized)
{
#if defined(__linux__) || defined(__APPLE__)
    if (!isEmptyString(arg))
    {
        size_t len = strlen(arg);
        sanitized.append('\'');
        for (size_t i = 0; i < len; i++)
        {
            if (arg[i] == '\'')
                sanitized.append(R"('"'"')");
            else
                sanitized.append(arg[i]);
        }
        sanitized.append('\'');
    }
#else
    sanitized.append(arg);
#endif
    return sanitized;
}
