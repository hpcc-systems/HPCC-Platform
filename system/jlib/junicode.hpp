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

#ifndef JUNICODE_HPP
#define JUNICODE_HPP

#include "jiface.hpp"

class StringMatcher;

typedef unsigned UTF32; /* at least 32 bits */
typedef unsigned short  UTF16;  /* at least 16 bits */
typedef unsigned char   UTF8;   /* typically 8 bits */

#define UTF8_BOM    (const char *)"\357\273\277"

const UTF32 sourceIllegal = (UTF32)-1;
const UTF32 sourceExhausted = (UTF32)-2;
const UTF32 errorLowerLimit = (UTF32)-16;       // Any value above this is an error code...

class jlib_decl UtfReader : public CInterface
{
public:
    enum UtfFormat { Utf8, Utf16le, Utf16be, Utf32le, Utf32be };
    UtfReader(UtfFormat _type, bool _strictConversion) { type = _type; strictConversion = _strictConversion; set(0, NULL); }

    size32_t getLegalLength();
    void set(size32_t len, const void * start)  { cur = (const byte *)start; end = cur + len; }
    UTF32 next();
    bool done()     { return cur == end; }

protected:
    UTF32 next8();
    UTF32 next16le();
    UTF32 next16be();
    UTF32 next32le();
    UTF32 next32be();

public:
    const byte * cur;
    const byte * end;
    UtfFormat type;
    bool strictConversion;
};

extern jlib_decl unsigned writeUtf8(void * target, unsigned maxLength, UTF32 value);
extern jlib_decl unsigned writeUtf16le(void * target, unsigned maxLength, UTF32 value);
extern jlib_decl unsigned writeUtf16be(void * target, unsigned maxLength, UTF32 value);
extern jlib_decl unsigned writeUtf32le(void * target, unsigned maxLength, UTF32 value);
extern jlib_decl unsigned writeUtf32be(void * target, unsigned maxLength, UTF32 value);

extern jlib_decl MemoryBuffer & appendUtf8(MemoryBuffer & out, UTF32 value);
extern jlib_decl StringBuffer & appendUtf8(StringBuffer & out, UTF32 value);
extern jlib_decl MemoryBuffer & appendUtf16le(MemoryBuffer & out, UTF32 value);
extern jlib_decl MemoryBuffer & appendUtf16be(MemoryBuffer & out, UTF32 value);
extern jlib_decl MemoryBuffer & appendUtf32le(MemoryBuffer & out, UTF32 value);
extern jlib_decl MemoryBuffer & appendUtf32be(MemoryBuffer & out, UTF32 value);
extern jlib_decl bool convertUtf(MemoryBuffer & target, UtfReader::UtfFormat targetType, unsigned sourceLength, const void * source, UtfReader::UtfFormat sourceType);
extern jlib_decl bool convertToUtf8(MemoryBuffer & target, unsigned sourceLength, const void * source);

extern jlib_decl void addUtfActionList(StringMatcher & matcher, const char * text, unsigned action, unsigned * maxElementLength, UtfReader::UtfFormat utfFormat);
extern jlib_decl UTF32 readUtf8Character(unsigned len, const byte * & cur);

extern jlib_decl size32_t readUtf8Size(const void * _data);
extern jlib_decl UTF32 readUtf8Char(const void * _data);

typedef MemoryBuffer & (*utfReplacementFunc)(MemoryBuffer & target, UTF32 match, UtfReader::UtfFormat type, const void * source, int len, bool start);
extern jlib_decl bool replaceUtf(utfReplacementFunc func, MemoryBuffer & target, UtfReader::UtfFormat type, unsigned sourceLength, const void * source);
extern jlib_decl bool appendUtfXmlName(MemoryBuffer & target, UtfReader::UtfFormat type, unsigned sourceLength, const void * source);

//Does the string only contain characters < 128 (i.e. is identical when converted to utf8)
extern jlib_decl bool containsOnlyAscii(const char * source);
extern jlib_decl bool containsOnlyAscii(unsigned sourceLength, const char * source);

inline StringBuffer &appendUtf8XmlName(StringBuffer & target, unsigned sourceLength, const void * source)
{
    MemoryBuffer mb;
    appendUtfXmlName(mb, UtfReader::Utf8, sourceLength, source);
    return target.append(mb.length(), mb.toByteArray());
}


#endif
