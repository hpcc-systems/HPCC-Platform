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
#include "jliball.hpp"
#include "jerror.hpp"
#include "junicode.hpp"

/* Based on code extracted from the following source...  Changed quite signficantly */

/*
 * Copyright 2001 Unicode, Inc.
 * 
 * Disclaimer
 * 
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 * 
 * Limitations on Rights to Redistribute This Code
 * 
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

//----------------------------------------------------------------------------
static const int halfShift  = 10; /* used for shifting by 10 bits */

static const UTF32 halfBase = 0x0010000UL;
static const UTF32 halfMask = 0x3FFUL;

#define UNI_REPLACEMENT_CHAR (UTF32)0x0000FFFD
#define UNI_MAX_BMP (UTF32)0x0000FFFF
#define UNI_MAX_UTF16 (UTF32)0x0010FFFF
#define UNI_MAX_UTF32 (UTF32)0x7FFFFFFF

#define UNI_SUR_HIGH_START  (UTF32)0xD800
#define UNI_SUR_HIGH_END    (UTF32)0xDBFF
#define UNI_SUR_LOW_START   (UTF32)0xDC00
#define UNI_SUR_LOW_END     (UTF32)0xDFFF


UTF32 UtfReader::next()
{
    switch (type)
    {
    case Utf8:    return next8();
    case Utf16le:  return next16le();
    case Utf16be:  return next16be();
    case Utf32le:  return next32le();
    case Utf32be:  return next32be();
    }
    UNIMPLEMENTED;
}

size32_t UtfReader::getLegalLength()
{
    const byte * saved = cur;
    while (next() < errorLowerLimit)
    {
    }
    size32_t ret = (size32_t)(cur-saved);
    cur = saved;
    return ret;
}

//---------------------------------------------------------------------------

UTF32 UtfReader::next32le()
{
    if (end - cur < 4) return sourceExhausted;
    UTF32 ch = *(UTF32 *)cur;
    if (strictConversion && ((ch >= UNI_SUR_HIGH_START) && (ch <= UNI_SUR_LOW_END)))
        return sourceIllegal;
    cur += sizeof(UTF32);
    return ch;
}

//---------------------------------------------------------------------------

UTF32 UtfReader::next32be()
{
    if (end - cur < 4) return sourceExhausted;
    UTF32 ch;
    _cpyrev4(&ch, cur);
    if (strictConversion && ((ch >= UNI_SUR_HIGH_START) && (ch <= UNI_SUR_LOW_END)))
        return sourceIllegal;
    cur += sizeof(UTF32);
    return ch;
}

//---------------------------------------------------------------------------

UTF32 UtfReader::next16le()
{
    if (end - cur < 2) return sourceExhausted;

    const byte * source = cur;
    UTF32 ch = source[0] | (source[1] << 8);
    source += 2;

    if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END)
    {
        if (end - cur < 2)
            return sourceExhausted;

        UTF32 ch2 = source[0] | (source[1] << 8);
        source += 2;

        if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
        {
            ch = ((ch - UNI_SUR_HIGH_START) << halfShift)
                + (ch2 - UNI_SUR_LOW_START) + halfBase;
        } 
        else if (strictConversion) /* it's an unpaired high surrogate */
            return sourceIllegal;
    } 
    else if ((strictConversion) && (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END))
        return sourceIllegal;

    cur = (const byte *)source;
    return ch;
}

//---------------------------------------------------------------------------

UTF32 UtfReader::next16be()
{
    if (end - cur < 2) return sourceExhausted;

    const byte * source = cur;
    UTF32 ch = (source[0] << 8) | source[1];
    source += 2;

    if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
    {
        if (end - cur < 2)
            return sourceExhausted;

        UTF32 ch2 = (source[0] << 8) | source[1];
        source += 2;

        if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
        {
            ch = ((ch - UNI_SUR_HIGH_START) << halfShift)
                + (ch2 - UNI_SUR_LOW_START) + halfBase;
        } 
        else if (strictConversion) /* it's an unpaired high surrogate */
            return sourceIllegal;
    } 
    else if ((strictConversion) && (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END))
        return sourceIllegal;
    cur = source;
    return ch;
}

//---------------------------------------------------------------------------

//This is probably faster than a table lookup on modern processors since it would avoid a cache hit.
//Especially because first branch is the most common.
inline unsigned getTrailingBytesForUTF8(byte value)
{
    if (value < 0xc0)
        return 0;
    if (value < 0xe0)
        return 1;
    if (value < 0xf0)
        return 2;
    if (value < 0xf8)
        return 3;
    if (value < 0xfc)
        return 4;
    return 5;
}

/*
 * Magic values subtracted from a buffer value during UTF8 conversion.
 * This table contains as many values as there might be trailing bytes
 * in a UTF-8 sequence.
 */
static const UTF32 offsetsFromUTF8[6] = { 0x00000000UL, 0x00003080UL, 0x000E2080UL, 
                     0x03C82080UL, 0xFA082080UL, 0x82082080UL };

/*
 * Utility routine to tell whether a sequence of bytes is legal UTF-8.
 * This must be called with the length pre-determined by the first byte.
 * If not calling this from ConvertUTF8to*, then the length can be set by:
 *  length = trailingBytesForUTF8[*source]+1;
 * and the sequence is illegal right away if there aren't that many bytes
 * available.
 * If presented with a length > 4, this returns false.  The Unicode
 * definition of UTF-8 goes up to 4-byte sequences.
 */
unsigned readUtf8Size(const void * _data)
{
    const byte * ptr = (const byte *)_data;
    return getTrailingBytesForUTF8(*ptr)+1;
}


UTF32 readUtf8Char(const void * _data)
{
    const byte * ptr = (const byte *)_data;
    unsigned short extraBytesToRead = getTrailingBytesForUTF8(*ptr);
    UTF32 ch = 0;
    switch (extraBytesToRead) {
        case 3: ch += *ptr++; ch <<= 6; // fallthrough
        case 2: ch += *ptr++; ch <<= 6; // fallthrough
        case 1: ch += *ptr++; ch <<= 6; // fallthrough
        case 0: ch += *ptr++;
    }
    return ch - offsetsFromUTF8[extraBytesToRead];
}

inline bool isLegalUTF8(const UTF8 *source, unsigned length) 
{
    UTF8 a;
    const UTF8 *srcptr = source+length;
    switch (length) 
    {
    default: return false;
        /* Everything else falls through when "true"... */
    case 4:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return false;
        // fallthrough
    case 3:
        if ((a = (*--srcptr)) < 0x80 || a > 0xBF) return false;
        // fallthrough
    case 2: if ((a = (*--srcptr)) > 0xBF) return false;
        switch (*source) 
        {
            /* no fall-through in this inner switch */
            case 0xE0: if (a < 0xA0) return false; break;
            case 0xF0: if (a < 0x90) return false; break;
            case 0xF4: if (a > 0x8F) return false; break;
            default:  if (a < 0x80) return false;
        }
        // fallthrough
    case 1:
        if (*source >= 0x80 && *source < 0xC2) return false;
        if (*source > 0xF4) return false;
    }
    return true;
}

/* --------------------------------------------------------------------- */

UTF32 UtfReader::next8()
{
    const UTF8* source = (const UTF8*)cur;
    if (source >= end) return sourceExhausted;

    unsigned short extraBytesToRead = getTrailingBytesForUTF8(*source);
    if (source + extraBytesToRead >= end)
        return sourceExhausted;

    /* Do this check whether lenient or strict */
    if (! isLegalUTF8(source, extraBytesToRead+1))
        return sourceIllegal;

    /*
     * The cases all fall through. See "Note A" below.
     */
    UTF32 ch = 0;
    switch (extraBytesToRead) {
        case 3: ch += *source++; ch <<= 6; // fallthrough
        case 2: ch += *source++; ch <<= 6; // fallthrough
        case 1: ch += *source++; ch <<= 6; // fallthrough
        case 0: ch += *source++;
    }
    cur = (const byte *)source;
    return ch - offsetsFromUTF8[extraBytesToRead];
}

//---------------------------------------------------------------------------

UTF32 readUtf8Character(unsigned len, const byte * & cur)
{
    const UTF8* source = (const UTF8*)cur;

    if (len == 0) return sourceExhausted;
    unsigned short extraBytesToRead = getTrailingBytesForUTF8(*source);
    if (extraBytesToRead >= len)
        return sourceExhausted;

    /* Do this check whether lenient or strict */
    if (! isLegalUTF8(source, extraBytesToRead+1))
        return sourceIllegal;

    /*
     * The cases all fall through. See "Note A" below.
     */
    UTF32 ch = 0;
    switch (extraBytesToRead) {
        case 3: ch += *source++; ch <<= 6; // fallthrough
        case 2: ch += *source++; ch <<= 6; // fallthrough
        case 1: ch += *source++; ch <<= 6; // fallthrough
        case 0: ch += *source++;
    }
    cur = (const byte *)source;
    return ch - offsetsFromUTF8[extraBytesToRead];
}

/*
 * Once the bits are split out into bytes of UTF-8, this is a mask OR-ed
 * into the first byte, depending on how many bytes follow.  There are
 * as many entries in this table as there are UTF-8 sequence types.
 * (I.e., one byte sequence, two byte... six byte sequence.)
 */
static const UTF8 firstByteMark[7] = { 0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC };
static const UTF32 byteMask = 0xBF;
static const UTF32 byteMark = 0x80; 

unsigned writeUtf8(void * vtarget, unsigned maxLength, UTF32 ch)
{
    unsigned short bytesToWrite;
    /* Figure out how many bytes the result will require */
    if (ch < (UTF32)0x80) 
        bytesToWrite = 1;
    else if (ch < (UTF32)0x800) 
        bytesToWrite = 2;
    else if (ch < (UTF32)0x10000) 
        bytesToWrite = 3;
    else if (ch < (UTF32)0x200000)
        bytesToWrite = 4;
    else {              
        bytesToWrite = 2;
        ch = UNI_REPLACEMENT_CHAR;
    }

    if (bytesToWrite > maxLength)
        return 0;

    UTF8 * target = (UTF8 *)vtarget + bytesToWrite;
    switch (bytesToWrite) { /* note: everything falls through. */
        case 4: *--target = (ch | byteMark) & byteMask; ch >>= 6; // fallthrough
        case 3: *--target = (ch | byteMark) & byteMask; ch >>= 6; // fallthrough
        case 2: *--target = (ch | byteMark) & byteMask; ch >>= 6; // fallthrough
        case 1: *--target =  ch | firstByteMark[bytesToWrite];
    }
    return bytesToWrite;
}


unsigned writeUtf16le(void * vtarget, unsigned maxLength, UTF32 ch)
{
    if (maxLength < 2)
        return 0;

    UTF16 * target = (UTF16 *)vtarget;
    if (ch <= UNI_MAX_BMP) 
    { 
        /* Target is a character <= 0xFFFF */
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_LOW_END)
            ch = UNI_REPLACEMENT_CHAR;
        *target = ch;   /* normal case */
        return 2;
    } 
        
    if (ch > UNI_MAX_UTF16) 
    {
        *target = UNI_REPLACEMENT_CHAR;
        return 2;
    } 

    /* target is a character in range 0xFFFF - 0x10FFFF. */
    if (maxLength < 4)
        return 0;
    ch -= halfBase;
    target[0] = (ch >> halfShift) + UNI_SUR_HIGH_START;
    target[1] = (ch & halfMask) + UNI_SUR_LOW_START;
    return 4;
}

unsigned writeUtf16be(void * vtarget, unsigned maxLength, UTF32 ch)
{
    if (maxLength < 2)
        return 0;

    UTF16 * target = (UTF16 *)vtarget;
    UTF16 temp;
    if (ch <= UNI_MAX_BMP) 
    { 
        /* Target is a character <= 0xFFFF */
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_LOW_END)
            ch = UNI_REPLACEMENT_CHAR;
        temp = ch;
        _cpyrev2(target, &temp);    /* normal case */
        return 2;
    } 
        
    if (ch > UNI_MAX_UTF16) 
    {
        temp = UNI_REPLACEMENT_CHAR;
        _cpyrev2(target, &temp);
        return 2;
    } 

    /* target is a character in range 0xFFFF - 0x10FFFF. */
    if (maxLength < 4)
        return 0;
    ch -= halfBase;
    temp = (ch >> halfShift) + UNI_SUR_HIGH_START;
    _cpyrev2(target, &temp);
    temp = (ch & halfMask) + UNI_SUR_LOW_START;
    _cpyrev2(target+1, &temp);
    return 4;
}

unsigned writeUtf32le(void * vtarget, unsigned maxLength, UTF32 ch)
{
    if (maxLength < 4) return 0;
    *(UTF32 *)vtarget = ch;
    return 4;
}

unsigned writeUtf32be(void * vtarget, unsigned maxLength, UTF32 ch)
{
    if (maxLength < 4) return 0;
    _cpyrev4(vtarget, &ch);
    return 4;
}

//---------------------------------------------------------------------------

MemoryBuffer & appendUtf8(MemoryBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf8(temp, sizeof(temp), value), temp);
}

StringBuffer & appendUtf8(StringBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf8(temp, sizeof(temp), value), temp);
}

MemoryBuffer & appendUtf16le(MemoryBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf16le(temp, sizeof(temp), value), temp);
}

MemoryBuffer & appendUtf16be(MemoryBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf16be(temp, sizeof(temp), value), temp);
}

MemoryBuffer & appendUtf32le(MemoryBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf32le(temp, sizeof(temp), value), temp);
}

MemoryBuffer & appendUtf32be(MemoryBuffer & out, UTF32 value)
{
    char temp[4];
    return out.append(writeUtf32be(temp, sizeof(temp), value), temp);
}

MemoryBuffer & appendUtf(MemoryBuffer & out, UtfReader::UtfFormat targetType, UTF32 value)
{
    switch (targetType)
    {
    case UtfReader::Utf8:    appendUtf8(out, value); break;
    case UtfReader::Utf16le: appendUtf16le(out, value); break;
    case UtfReader::Utf16be: appendUtf16be(out, value); break;
    case UtfReader::Utf32le: appendUtf32le(out, value); break;
    case UtfReader::Utf32be: appendUtf32be(out, value); break;
    }
    return out;
}

/* ---------------------------------------------------------------------

    Note A.
    The fall-through switches in UTF-8 reading code save a
    temp variable, some decrements & conditionals.  The switches
    are equivalent to the following loop:
        {
            int tmpBytesToRead = extraBytesToRead+1;
            do {
                ch += *source++;
                --tmpBytesToRead;
                if (tmpBytesToRead) ch <<= 6;
            } while (tmpBytesToRead > 0);
        }
    In UTF-8 writing code, the switches on "bytesToWrite" are
    similarly unrolled loops.

   --------------------------------------------------------------------- */


bool convertUtf(MemoryBuffer & target, UtfReader::UtfFormat targetType, unsigned sourceLength, const void * source, UtfReader::UtfFormat sourceType)
{
    UtfReader input(sourceType, false);
    input.set(sourceLength, source);
    unsigned originalLength = target.length();
    for (;;)
    {
        UTF32 next = input.next();
        if (next == sourceExhausted)
            return true;
        if (next == sourceIllegal)
        {
            target.setLength(originalLength);
            return false;
        }
        appendUtf(target, targetType, next);
    }
}

bool convertToUtf8(MemoryBuffer & target, unsigned sourceLength, const void * source)
{
    if (sourceLength < 2)
        return false;

    const byte * text = (const byte *)source;
    //check for leading BOM of 0xfeff in the appropriate encoding
    if ((text[0] == 0xfe) && (text[1] == 0xff))
        return convertUtf(target, UtfReader::Utf8, sourceLength-2, text+2, UtfReader::Utf16be);

    if ((text[0] == 0xff) && (text[1] == 0xfe))
    {
        if (sourceLength >= 4 && (text[2] == 0) && (text[3] == 0))
            return convertUtf(target, UtfReader::Utf8, sourceLength-4, text+4, UtfReader::Utf32le);
        return convertUtf(target, UtfReader::Utf8, sourceLength-2, text+2, UtfReader::Utf16le);
    }
    if (sourceLength > 4 && (text[0] == 0) && (text[1] == 0) && (text[2] == 0xfe) && (text[3] == 0xff))
        return convertUtf(target, UtfReader::Utf8, sourceLength-4, text+4, UtfReader::Utf32be);

    //Try and guess the format
    if (text[0] && !text[1])
    {
        if (text[2])
        {
            if (convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf16le))
                return true;
        }
        else
        {
            if (convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf32le))
                return true;
        }
    }
    else if (!text[0])
    {
        if (text[1])
        {
            if (convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf16be))
                return true;
        }
        else
        {
            if (convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf32be))
                return true;
        }
    }

    //No idea first one that matches wins!
    return
        convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf16le) ||
        convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf16be) ||
        convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf32le) ||
        convertUtf(target, UtfReader::Utf8, sourceLength, source, UtfReader::Utf32be);
}

//---------------------------------------------------------------------------

void addUtfActionList(StringMatcher & matcher, const char * text, unsigned action, unsigned * maxElementLength, UtfReader::UtfFormat utfFormat)
{
    if (!text)
        return;

    unsigned idx=0;
    while (*text)
    {
        StringBuffer str;
        while (*text)
        {
            char next = *text++;
            if (next == ',')
                break;
            if (next == '\\' && *text)
            {
                next = *text++;
                switch (next)
                {
                case 'r': next = '\r'; break;
                case 'n': next = '\n'; break;
                case 't': next = '\t'; break;
                case 'x':
                    //hex constant - at least we can define spaces then...
                    if (text[0] && text[1])
                    {
                        next = (hex2num(*text) << 4) | hex2num(text[1]);
                        text+=2;
                    }
                    break;
                default:
                    break; //otherwise \ just quotes the character e.g. \,
                }
            }
            str.append(next);
        }
        if (str.length())
        {
            MemoryBuffer converted;
            if (!convertUtf(converted, utfFormat, str.length(), str.str(), UtfReader::Utf8))
               throwError(JLIBERR_BadUtf8InArguments);
            matcher.queryAddEntry(converted.length(), converted.toByteArray(), action+(idx++<<8));
            if (maxElementLength && (converted.length() > *maxElementLength))
                *maxElementLength = converted.length();
        }
    }
}

extern jlib_decl bool replaceUtf(utfReplacementFunc func, MemoryBuffer & target, UtfReader::UtfFormat type, unsigned sourceLength, const void * source)
{
    UtfReader input(type, false);
    input.set(sourceLength, source);
    unsigned originalLength = target.length();
    for (;;)
    {
        const byte * cur = input.cur;
        UTF32 next = input.next();
        if (next == sourceExhausted)
            return true;
        if (next == sourceIllegal)
        {
            target.setLength(originalLength);
            return false;
        }
        func(target, next, type, cur, input.cur-cur, cur==source);
    }
}

struct utf32ValidXmlCharRange
{
    UTF32 min;
    UTF32 max;
    bool start;
};

utf32ValidXmlCharRange utf32ValidXmlCharRanges[] = {
    {'0', '9', false},
    {'A', 'Z', true},
    {'a', 'z', true},
    {0xC0, 0xD6, true},
    {0xD8, 0xF6, true},
    {0xF8, 0x2FF, true},
    {0x300, 0x36F, false},
    {0x370, 0x37D, true},
    {0x37F, 0x1FFF, true},
    {0x200C, 0x200D, true},
    {0x203F, 0x2040, false},
    {0x2070, 0x218F, true},
    {0x2C00, 0x2FEF, true},
    {0x3001, 0xD7FF, true},
    {0xF900, 0xFDCF, true},
    {0xFDF0, 0xFFFD, true},
    {0x10000, 0xEFFFF, true},
    {0, 0, false}
};

inline bool replaceBelowRange(UTF32 match, UTF32 replace, int id, MemoryBuffer & target, UtfReader::UtfFormat type, const void * source, int len, bool start)
{
    utf32ValidXmlCharRange &r = utf32ValidXmlCharRanges[id];
    if (r.min==0)
        return true;
    if (match>r.max)
        return false;
    if (match<r.min)
    {
        appendUtf(target, type, replace);
        return true;
    }
    if (!r.start && start)
        appendUtf(target, type, replace);
    else
        target.append(len, source); //src and target are same, no need to reconvert
    return true;
}

MemoryBuffer & utfXmlNameReplacementFunc(MemoryBuffer & target, UTF32 match, UtfReader::UtfFormat type, const void * source, int len, bool start)
{
    if (match==':' || match=='_' || (!start && (match=='-' || match=='.' || match==0xB7)))
        return target.append(len, source);

    for (int i=0; !replaceBelowRange(match, '_', i, target, type, source, len, start); i++);

    return target;
}

extern jlib_decl bool appendUtfXmlName(MemoryBuffer & target, UtfReader::UtfFormat type, unsigned sourceLength, const void * source)
{
    return replaceUtf(utfXmlNameReplacementFunc, target, type, sourceLength, source);
}

extern jlib_decl bool containsOnlyAscii(const char * source)
{
    for (;;)
    {
        byte next = *source++;
        if (!next)
            return true;
        if (next >= 128)
            return false;
    }
}

extern jlib_decl bool containsOnlyAscii(size_t sourceLength, const char * source)
{
    for (size_t i=0; i < sourceLength; i++)
    {
        byte next = source[i];
        if (next >= 128)
            return false;
    }
    return true;
}
