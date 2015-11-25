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

#ifndef __HIDL_UTILS_HPP__
#define __HIDL_UTILS_HPP__

//
// To avoid recursive dependencies, hidl can NOT use jlib. Hence this utils. 
// Most of these are borrowed from jlib or a simplified version in jlib.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

// directory
bool checkDirExists(const char * filename);
void createDirectory(const char* dir);

// create file
int createFile(const char* src, const char* ext);
int createFile(const char* src, const char* tail, const char* ext);

// filenames
char * changeext(const char *fn,const char *ext);
char * changetail(const char *fn,const char *tail, const char *ext);
bool hasext(const char *fn,const char *ext);
char * gettail(const char *fn);

typedef unsigned size32_t;

// a simplified StringBuffer class
class StrBuffer
{
public:
    StrBuffer();
    StrBuffer(const char *value);
    StrBuffer(unsigned len, const char *value);
    ~StrBuffer() { if (buffer) free(buffer); }

    inline size32_t length() const                      { return curLen; }
    void            setLength(unsigned len);
    inline void     ensureCapacity(unsigned max)        { if (maxLen <= curLen + max) _realloc(curLen + max); }
    
    StrBuffer &  append(int value);
    StrBuffer &  append(unsigned int value);
    StrBuffer &  append(char c);
    StrBuffer &  append(double value);
    StrBuffer &  append(const char * value);
    StrBuffer &  append(unsigned len, const char * value);

    StrBuffer &  appendf(const char *format, ...) __attribute__((format(printf, 2, 3)));
    StrBuffer &  setf(const char* format, ...) __attribute__((format(printf, 2, 3)));
    StrBuffer &  set(const char* val) { return clear().append(val); }
    StrBuffer &  clear() { curLen = 0; return *this; }
    StrBuffer & va_append(const char *format, va_list args)  __attribute__((format(printf,2,0)));
    
    const char * str() const;
    operator const char* () const { return str(); }
    char charAt(size32_t pos) { return buffer[pos]; }
    void setCharAt(size32_t pos, char value) { buffer[pos] = value; }
    char* detach() 
    { 
        if (buffer)
        {
            buffer[curLen] = 0;
            char* ret = buffer; 
            init();
            return ret; 
        }
        return strdup("");
    }

    StrBuffer& operator= (const char* s) {  if (s) set(s); return *this; }

protected:
    void init()
    {
        buffer = NULL;
        curLen = 0;
        maxLen = 0;
    }
    void _realloc(size32_t newLen);

private:    
    mutable char * buffer;
    size32_t       curLen;
    size32_t       maxLen;
};

class VStrBuffer : public StrBuffer
{
public:
    VStrBuffer(const char* format, ...) __attribute__((format(printf, 2, 3)));
};

StrBuffer& encodeXML(const char *x, StrBuffer &ret);
StrBuffer& printfEncode(const char* src, StrBuffer& encoded);

void initLeakCheck(bool fullMemoryCheck);

#endif
