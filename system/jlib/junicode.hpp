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

#ifndef JUNICODE_HPP
#define JUNICODE_HPP

#include "jexpdef.hpp"

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


#endif
