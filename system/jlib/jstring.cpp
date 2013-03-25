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


#include "platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <math.h>

#include "jstring.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jutil.hpp"

#define DOUBLE_FORMAT   "%.16g"
#define FLOAT_FORMAT    "%.7g"

#ifndef va_copy
 /* WARNING - DANGER - ASSUMES TYPICAL STACK MACHINE */
 #define va_copy(dst, src) ((void)((dst) = (src)))
#endif

static const char * TheNullStr = "";

#define FIRST_CHUNK_SIZE  8
#define DOUBLE_LIMIT      0x100000          // must be a power of 2
#define DETACH_GRANULARITY 16

//===========================================================================

StringBuffer::StringBuffer()
{
    init();
}

#if 0
StringBuffer::StringBuffer(int initial)
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

StringBuffer::StringBuffer(unsigned len, const char *value)
{
    init();
    append(len, value);
}

StringBuffer::StringBuffer(const StringBuffer & value)
{
    init();
    append(value);
}

void    StringBuffer::setBuffer(size32_t buffLen, char * newBuff, size32_t strLen)
{
    assertex(buffLen>0 && newBuff!=NULL && strLen<buffLen);

    if (buffer)
        free(buffer);

    buffer = newBuff;
    maxLen=buffLen;
    curLen=strLen;
}

void StringBuffer::_realloc(size32_t newLen)
{
    if (newLen >= maxLen)
    {
        size32_t newMax = maxLen;
        if (newMax == 0)
            newMax = FIRST_CHUNK_SIZE;
        if (newLen > DOUBLE_LIMIT)
        {
            newMax = (newLen + DOUBLE_LIMIT) & ~(DOUBLE_LIMIT-1);
            if (newLen >= newMax)
                throw MakeStringException(MSGAUD_operator, -1, "StringBuffer::_realloc: Request for %d bytes oldMax = %d", newLen, maxLen);
        }
        else
        {
            while (newLen >= newMax)
                newMax += newMax;
        }
        char * newStr;
        if(!newMax || !(newStr=(char *)realloc(buffer, newMax)))
        {
            DBGLOG("StringBuffer::_realloc: Failed to realloc = %d, oldMax = %d", newMax, maxLen);
            PrintStackReport();
            PrintMemoryReport();
            throw MakeStringException(MSGAUD_operator, -1, "StringBuffer::_realloc: Failed to realloc = %d, oldMax = %d", newMax, maxLen);
        }       
        buffer = newStr;
        maxLen = newMax;
    }
}


char * StringBuffer::detach()
{
    if (buffer)
    {
        if (maxLen>curLen+1+DETACH_GRANULARITY)
            buffer = (char *)realloc(buffer,curLen+1); // shrink
        buffer[curLen] = '\0';          // There is always room for this null
        char *ret = buffer;
        init();
        return ret;
    }
    return strdup(TheNullStr);
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
    if (value)
    {
        size32_t SourceLen = (size32_t)::strlen(value);
        
        ensureCapacity(SourceLen);
        memcpy(buffer + curLen, value, SourceLen);
        curLen += SourceLen;
    }
    return *this;
}

StringBuffer & StringBuffer::append(unsigned len, const char * value)
{
    if (len)
    {
        ensureCapacity(len);
        memcpy(buffer + curLen, value, len);
        curLen += len;
    }
    return *this;
}

StringBuffer & StringBuffer::append(const unsigned char * value)
{
    return append((const char *) value);
}

StringBuffer & StringBuffer::append(const char * value, int offset, int len)
{
    ensureCapacity(len);
    memcpy(buffer + curLen, value+offset, len);
    curLen += len;
    return *this;
}

StringBuffer & StringBuffer::append(const IAtom * value)
{
    if (value)
        append(value->getAtomNamePtr());
    return *this;
}

StringBuffer & StringBuffer::append(double value)
{
    int len = length();
    int newlen = appendf(DOUBLE_FORMAT, value).length();
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
    int len = length();
    int newlen = appendf(FLOAT_FORMAT, value).length();
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
    size32_t SourceLen = value.length();
    
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
    size32_t SourceLen = value.length();
    
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

StringBuffer & StringBuffer::appendLower(unsigned len, const char * value)
{
    if (len)
    {
        ensureCapacity(len);
        const byte * from = reinterpret_cast<const byte *>(value);
        for (unsigned i = 0; i < len; i++)
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

StringBuffer & StringBuffer::limited_valist_appendf(unsigned szLimit, const char *format, va_list args)
{
#define BUF_SIZE 1024
#define MAX_BUF_SIZE (1024*1024) // limit buffer size to 1MB when doubling
    
    // handle string that is bigger that BUF_SIZE bytes
    unsigned size = (0 == szLimit||szLimit>BUF_SIZE)?BUF_SIZE:szLimit;
    int len;
    va_list args2;
    va_copy(args2, args);
    try { ensureCapacity(size); }
    catch (IException *e)
    {
        StringBuffer eMsg;
        IException *e2 = MakeStringException(-1, "StringBuffer::valist_appendf(\"%s\"): vsnprintf failed or result exceeds limit (%d): %s", format, size, e->errorMessage(eMsg).str());
        e->Release();
        throw e2;
    }
    len = _vsnprintf(buffer+curLen,size,format,args);
    if (len >= 0)
    {
        if ((unsigned)len >= size)
        {
            if (szLimit && (unsigned)len >= szLimit)
            {
                if ((unsigned)len>szLimit)
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
        loop
        {
            if (0 != szLimit && size>szLimit) size = szLimit; // if so, will be last attempt
            if (size>MAX_BUF_SIZE)
            {
                WARNLOG("StringBuffer::valist_appendf(\"%s\"): vsnprintf exceeds limit (%d)", format, size);
                size = szLimit = MAX_BUF_SIZE;
            }
            try { ensureCapacity(size); }
            catch (IException *e)
            {
                StringBuffer eMsg;
                IException *e2 = MakeStringException(-1, "StringBuffer::valist_appendf(\"%s\"): vsnprintf failed (%d): %s", format, size, e->errorMessage(eMsg).str());
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

StringBuffer & StringBuffer::appendN(size32_t count, char fill)
{
    ensureCapacity(count);
    memset(buffer+curLen, fill, count);
    curLen += count;
    return *this;
}

void StringBuffer::setLength(unsigned len)
{
    if (len > curLen)
    {
        ensureCapacity(len-curLen);
    }
    curLen = len;
}

char *  StringBuffer::reserve(size32_t size)
{
    ensureCapacity(size);
    char *ret = buffer+curLen;
    curLen += size;
    return ret;
}

char *  StringBuffer::reserveTruncate(size32_t size)
{
    size32_t newMax = curLen+size+1;
    if (newMax != maxLen) {
        char * newStr = (char *) realloc(buffer, newMax);
        if (!newStr)
            throw MakeStringException(-1, "StringBuffer::_realloc: Failed to realloc newMax = %d, oldMax = %d", newMax, maxLen);
        buffer = newStr;
        maxLen = newMax;
    }
    char *ret = buffer+curLen;
    curLen += size;
    return ret;
}

void StringBuffer::swapWith(StringBuffer &other)
{
    size32_t tmpsz = curLen;
    curLen = other.curLen;
    other.curLen = tmpsz;
    tmpsz = maxLen;
    maxLen = other.maxLen;
    other.maxLen = tmpsz;
    char *tmpbuf = buffer;
    buffer = other.buffer;
    other.buffer = tmpbuf;
}

void StringBuffer::kill()
{
    if (buffer)
        free(buffer);
    init();
}

void StringBuffer::getChars(int srcBegin, int srcEnd, char * target) const
{
    const int len = srcEnd - srcBegin;
    if (target && buffer && len > 0)
        memcpy(target, buffer + srcBegin, len);
}

void StringBuffer::_insert(unsigned offset, size32_t insertLen)
{
    ensureCapacity(insertLen);
    memmove(buffer + offset + insertLen, buffer + offset, curLen - offset);
    curLen += insertLen;
}


StringBuffer & StringBuffer::insert(int offset, char value)
{
    _insert(offset, 1);
    buffer[offset] = value;
    return *this;
}

StringBuffer & StringBuffer::insert(int offset, const char * value)
{
    if (!value) return *this;
    
    unsigned len = (size32_t)strlen(value);
    _insert(offset, len);
    memcpy(buffer + offset, value, len);
    return *this;
}

StringBuffer & StringBuffer::insert(int offset, double value)
{
    char temp[36];
    sprintf(temp, "%f", value);
    insert(offset, temp);
    return *this;
}

StringBuffer & StringBuffer::insert(int offset, float value)
{
    return insert(offset, (double)value);
}

StringBuffer & StringBuffer::insert(int offset, int value)
{
    char temp[12];
    numtostr(temp, value);
    return insert(offset, temp);
}

StringBuffer & StringBuffer::insert(int offset, unsigned value)
{
    char temp[12];
    
    numtostr(temp, value);
    return insert(offset, temp);
}

#if 0
StringBuffer & StringBuffer::insert(int offset, long value)
{
    char temp[24];
    numtostr(temp, value);
    return insert(offset, temp);
}
#endif

StringBuffer & StringBuffer::insert(int offset, __int64 value)
{
    char temp[24];
    
    numtostr(temp, value);
    return insert(offset, temp);
}

StringBuffer & StringBuffer::insert(int offset, const String & value)
{
    size32_t len = value.length();
    
    _insert(offset, len);
    value.getChars(0, len, buffer, offset);
    return *this;
}

StringBuffer & StringBuffer::insert(int offset, const StringBuffer & value)
{
    size32_t len = value.length();
    
    _insert(offset, len);
    value.getChars(0, len, buffer+offset);
    return *this;
}

StringBuffer & StringBuffer::insert(int offset, const IStringVal & value)
{
    return insert(offset, value.str());
}

StringBuffer & StringBuffer::insert(int offset, const IStringVal * value)
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

StringBuffer & StringBuffer::pad(unsigned count)
{
    ensureCapacity(count);
    memset(buffer + curLen, ' ', count);
    curLen += count;
    return *this;
}

StringBuffer & StringBuffer::padTo(unsigned count)
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

StringBuffer & StringBuffer::remove(unsigned start, unsigned len)
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
    unsigned max = curLen/2;
    char * end = buffer + curLen;
    
    unsigned idx;
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
    return out.append(curLen).append(curLen, buffer);
}

StringBuffer &StringBuffer::loadFile(const char *filename, bool binaryMode)
{
    FILE *in = fopen(filename, binaryMode?"rb":"rt");
    if (in)
    {
        char buffer[1024];
        int bytes;
        for (;;)
        {
            bytes = (size32_t)fread(buffer, 1, sizeof(buffer), in);
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
    loop
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

void StringBuffer::setCharAt(unsigned offset, char value)
{
    if (offset < curLen)
        buffer[offset] = value;
}

StringBuffer & StringBuffer::toLowerCase()
{
    if (buffer)
    {
        int l = curLen;
        for (int i = 0; i < l; i++)
            if (isupper(buffer[i]))
                buffer[i] = tolower(buffer[i]);
    }
    return *this;
}

StringBuffer & StringBuffer::toUpperCase()
{
    if (buffer)
    {
        int l = curLen;
        for (int i = 0; i < l; i++)
            if (islower(buffer[i]))
                buffer[i] = toupper(buffer[i]);
    }
    return *this;
}

StringBuffer & StringBuffer::replace(char oldChar, char newChar)
{
    if (buffer)
    {
        int l = curLen;
        for (int i = 0; i < l; i++)
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

// this method will replace all occurrances of "oldStr" with "newStr"
StringBuffer & StringBuffer::replaceString(const char* oldStr, const char* newStr)
{
    if (buffer)
    {
        const char* s = str();  // get null terminated version of the string
        int left = length();
        int oldStr_len = (size32_t)strlen(oldStr);

        StringBuffer tempbuff;

        while (left >= oldStr_len)
        {
            if ( memcmp(s, oldStr, oldStr_len) == 0)
            {
                tempbuff.append(newStr);
                s += oldStr_len;
                left -= oldStr_len;
            }
            else
            {
                tempbuff.append(*s);
                s++;
                left--;
            }
        }

        // there are no more possible replacements, make sure we keep the end of the original buffer
        tempbuff.append(s);
        
        //*this = tempbuff;
        swapWith(tempbuff);
        
    }

    return *this;
}

const char * StringBuffer::toCharArray() const
{
    if (buffer)
    {
        buffer[curLen] = '\0';          // There is always room for this null
        return buffer;
    }
    return TheNullStr;
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
  memcpy(text, value+offset, _count);
  text[_count]=0;
}

String::String(String & value)
{
  text = strdup(value.toCharArray());
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
  if (text != TheNullStr) free(text);
}

char String::charAt(size32_t index) const
{
  return text[index];
}

int String::compareTo(const String & value) const
{
    return strcmp(text, value.toCharArray());
}

int String::compareTo(const char* value)  const
{
    return strcmp(text,value);
}

String * String::concat(const String & value) const
{
  StringBuffer temp(toCharArray());
  temp.append(value);
  return new String(temp.str());
}

bool String::endsWith(const String & value) const
{
  unsigned lenValue = value.length();
  unsigned len = (size32_t)strlen(text);

  if (len >= lenValue)
    return (memcmp(text+(len-lenValue),value.toCharArray(),lenValue) == 0);
  return false;
}

bool String::endsWith(const char* value) const
{
    return ::endsWith(this->text, value);
}


bool String::equals(String & value) const
{
    return strcmp(text, value.toCharArray())==0;
}


bool String::equalsIgnoreCase(const String & value) const
{
  return stricmp(text, value.toCharArray())==0;
}

void String::getBytes(int srcBegin, int srcEnd, void * dest, int dstBegin) const
{
  memcpy((char *)dest+dstBegin, text+srcBegin, srcEnd-srcBegin);
}

void String::getChars(int srcBegin, int srcEnd, void * dest, int dstBegin) const
{
  memcpy((char *)dest+dstBegin, text+srcBegin, srcEnd-srcBegin);
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
  const char * str = search.toCharArray();
  const char * match = strstr(text, str);
  return match ? (int)(match - text) : -1;
}

int String::indexOf(const String & search, int from) const
{
  const char * str = search.toCharArray();
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
  const char * search = value.toCharArray();
  return (memcmp(text, search, lenValue) == 0);
}

bool String::startsWith(String & value, int offset) const
{
  unsigned lenValue = value.length();
  const char * search = value.toCharArray();
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

const char *String::toCharArray() const
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

StringAttr::StringAttr(const char * _text, unsigned _len)
{
    text = NULL;
    set(_text, _len);
}

StringAttr::StringAttr(const StringAttr & src)
{
    text = NULL;
    set(src.get());
}

void StringAttr::set(const char * _text)
{
    free(text);
    text = _text ? strdup(_text) : NULL;
}

void StringAttr::set(const char * _text, unsigned _len)
{
  if (text)
    free(text);
  text = (char *)malloc(_len+1);
  memcpy(text, _text, _len);
  text[_len] = 0;
}

void StringAttr::setown(const char * _text)
{
  if (text)
    free(text);
  text = (char *)_text;
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


inline char hex(char c, char lower)
{
  if (c < 10)
    return '0' + c;
  else if (lower)
    return 'a' + c - 10;
  else
    return 'A' + c - 10;
}

StringBuffer &  StringBuffer::appendhex(unsigned char c, char lower)
{
    append(hex(c>>4, lower));
    append(hex(c&0xF, lower));
    return *this;
}

void appendURL(StringBuffer *dest, const char *src, size32_t len, char lower)
{
  if (len == (size32_t)-1)
    len = (size32_t)strlen(src);
  while (len)
  {
      // isalnum seems to give weird results for chars > 127....
    unsigned char c = (unsigned char) *src;
    if (c == ' ')
      dest->append('+');
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


static StringBuffer & appendStringExpandControl(StringBuffer &out, unsigned len, const char * src, bool addBreak, bool isCpp)
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
                if ((c >= ' ') && (c <= 126))
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
    return appendStringExpandControl(out, len, src, addBreak, true);
}


StringBuffer & appendStringAsECL(StringBuffer &out, unsigned len, const char * src)
{
    return appendStringExpandControl(out, len, src, false, false);
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
    loop
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


int utf8CharLen(const unsigned char *ch)
{
    //return 1 if this is an ascii character, 
    //or 0 if its not a valid utf-8 character
    if (*ch < 128)
        return 1;
    if (*ch < 192)
        return 0;
    
    unsigned char len = 1;
    for (unsigned char lead = *ch << 1; (lead & 0x80); lead <<=1)
        len++;
    
    for (unsigned pos = 1; pos < len; pos++)
        if ((ch[pos] < 128) || (ch[pos] >= 192))
            return 0;  //its not a valid utf-8 character after all

    return len;
}

const char *encodeXML(const char *x, StringBuffer &ret, unsigned flags, unsigned len, bool utf8)
{
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
            if (len == (unsigned) -1)
                return x;
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
                if (chlen==0)
                    ret.append("&#").append((unsigned int)*(unsigned char *) x).append(';');
                else
                {
                    ret.append(*x);
                    while(--chlen)
                    {
                        if (len != (unsigned) -1)
                            len--;
                        ret.append(*(++x));
                    }
                }
            }
            else
                ret.append("&#").append((unsigned int)*(unsigned char *) x).append(';');

            break;
        }
        if (len != (unsigned) -1)
            len--;
        ++x;
    }
    return x;
}

const char *encodeXML(const char *x, IIOStream &out, unsigned flags, unsigned len, bool utf8)
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
                return x;
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
    return x;
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
        return j;
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
    return j;
}

void decodeXML(ISimpleReadStream &in, StringBuffer &out, unsigned len)
{
    // TODO
    UNIMPLEMENTED;
}

const char *decodeXML(const char *x, StringBuffer &ret, const char **errMark, IEntityHelper *entityHelper, bool strict)
{
    if (!x)
        return x;
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
                        bool error = false;
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
    return x;
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
jlib_decl StringBuffer &appendJSONValue(StringBuffer& s, const char *name, unsigned len, const void *_value)
{
    appendJSONNameOrDelimit(s, name);
    s.append('"');
    const unsigned char *value = (const unsigned char *) _value;
    for (unsigned int i = 0; i < len; i++)
        s.append(hexchar[value[i] >> 4]).append(hexchar[value[i] & 0x0f]);
    return s.append('"');
}

inline StringBuffer &encodeJSON(StringBuffer &s, const char ch)
{
    switch (ch)
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
        case '/':
            s.append('\\'); //fall through
        default:
            s.append(ch);
    }
    return s;
}

StringBuffer &encodeJSON(StringBuffer &s, unsigned len, const char *value)
{
    if (!value)
        return s;
    unsigned pos=0;
    while(pos<len && value[pos]!=0)
        encodeJSON(s, value[pos++]);
    return s;
}

StringBuffer &encodeJSON(StringBuffer &s, const char *value)
{
    if (!value)
        return s;
    while (*value)
        encodeJSON(s, *value++);
    return s;
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
    loop
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



bool startsWith(const char* src, const char* dst)
{
    while (*dst && *dst == *src) { src++; dst++; }
    return *dst==0;
}

bool startsWithIgnoreCase(const char* src, const char* dst)
{
    while (*dst && tolower(*dst) == tolower(*src)) { src++; dst++; }
    return *dst==0;
}

bool endsWith(const char* src, const char* dst)
{
    size_t srcLen = strlen(src);
    size_t dstLen = strlen(dst);
    if (dstLen<=srcLen)
        return memcmp(dst, src+srcLen-dstLen, dstLen)==0;
    return false;
}

bool endsWithIgnoreCase(const char* src, const char* dst)
{
    size_t srcLen = strlen(src);
    size_t dstLen = strlen(dst);
    if (dstLen<=srcLen)
        return memicmp(dst, src+srcLen-dstLen, dstLen)==0;
    return false;
}

char *j_strtok_r(char *str, const char *delim, char **saveptr)
{
    if (!str) 
        str = *saveptr;
    char c;
    loop {
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
