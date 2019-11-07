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
#include "platform.h"
#include <math.h>
#include <stdio.h>
#include <algorithm>

#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "eclrtl.hpp"
#include "rtlbcd.hpp"
#include "jlog.hpp"
#include "jmd5.hpp"

//=============================================================================
// Miscellaneous string functions...

inline unsigned QStrLength(unsigned size)       { return (size * 4) / 3; }
inline unsigned QStrSize(unsigned length)       { return (length + 1) * 3 / 4; }

byte lastQStrByteMask(unsigned tlen)
{
    switch (tlen & 3)
    {
    case 1:
        return 0xfc;
    case 2:
        return 0xf0;
    case 3:
        return 0xc0;
    }
    return 0xff;
}

inline byte expandQChar(byte c)
{
    return ' ' + c;
}

#if 1

static const char compressXlat[256] =
{  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0x00 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0x10 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,     // 0x20 
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,     // 0x30 
    0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,     // 0x40 
    0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,      
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,     // 0x50  
    0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 
    0x00, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,     // 0x60  
    0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,     // 0x70  
    0x38, 0x39, 0x3a, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0x80  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0x90  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xA0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xB0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xC0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xD0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xE0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,     // 0xF0  
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
};

#define compressQChar(c) compressXlat[(byte)c]

#else

inline byte compressQChar(byte c)
{
    if (c > 0x20)
    {
        if (c < 0x60)
            return c - 0x20;
        if ((c >= 'a') && (c <= 'z'))
            return c - 0x40;
    }
    return 0;
}

#endif


byte getQChar(const byte * buffer, size32_t index)
{
    size32_t offset = (index * 3) / 4;
    switch (index & 3)
    {
    case 0:
        return buffer[offset] >> 2;
    case 1:
        return ((buffer[offset] & 0x3) << 4) | (buffer[offset+1] >> 4);
    case 2:
        return ((buffer[offset] & 0xf) << 2) | (buffer[offset+1] >> 6);
    case 3:
        return (buffer[offset] & 0x3f);
    }
    return 0;
}


void setQChar(byte * buffer, size32_t index, byte value)
{
    size32_t offset = (index * 3) / 4;
    byte cur = buffer[offset];
    switch (index & 3)
    {
    case 0:
        cur = (cur & 0x03) | value << 2;
        break;
    case 1:
        cur = (cur & 0xFC) | (value >> 4);
        buffer[offset+1] = (buffer[offset+1] & 0x0f) | ((value & 0x0F) << 4);
        break;
    case 2:
        cur = (cur & 0xF0) | (value >> 2);
        buffer[offset+1] = (buffer[offset+1] & 0x3f) | ((value & 0x03) << 6);
        break;
    case 3:
        cur = (cur & 0xC0) | value;
        break;
    }
    buffer[offset] = cur;
}


//-------------------------------------------------------------------------------------------------------------------

bool incrementQString(byte *buf, size32_t size)
{
    int i = size;
    while (i--)
    {
        byte next = getQChar(buf, i);
        next = (next + 1) & 0x3F;
        setQChar(buf, i, next);
        if (next != 0)
            return true;
    }
    return false;
}

bool decrementQString(byte *buf, size32_t size)
{
    int i = size;
    while (i--)
    {
        byte next = getQChar(buf, i);
        next = (next - 1) & 0x3F;
        setQChar(buf, i, next);
        if (next != 0x3f)
            return true;
    }
    return false;
}


//---------------------------------------------------------------------------

class QStrReader
{
public:
    QStrReader(const byte * _buffer) { buffer = _buffer; curLen = 0; offset = 0; }

    byte curQChar()
    {
        switch (curLen & 3)
        {
        case 0:
            return buffer[offset] >> 2;
        case 1:
            return ((buffer[offset] & 0x3) << 4) | (buffer[offset+1] >> 4);
        case 2:
            return ((buffer[offset] & 0xf) << 2) | (buffer[offset+1] >> 6);
        case 3:
            return (buffer[offset] & 0x3f);
        }
        return 0;
    }

    byte nextQChar()
    {
        byte c = curQChar();
        if ((curLen & 3) != 0)
            offset++;
        curLen++;
        return c;
    }

    byte prevQChar()
    {
        curLen--;
        if ((curLen & 3) != 0)
            offset--;
        return curQChar();
    }

    char nextChar()
    {
        return expandQChar(nextQChar());
    }

    inline void seek(unsigned pos)
    {
        curLen = pos;
        offset = (pos* 3)/4;
    }

protected:
    const byte * buffer;
    unsigned curLen;
    unsigned offset;
};


//---------------------------------------------------------------------------

class QStrBuilder
{
public:
    QStrBuilder(void * _buffer) { buffer = (byte *)_buffer; curLen = 0; pending = 0; }

    void appendChar(char next)
    {
        appendQChar(compressQChar(next));
    }

    void appendCharN(unsigned len, char next)
    {
        byte c = compressQChar(next);
        while (len--)
            appendQChar(c);
    }

    void appendQStr(unsigned len, const char * text)
    {
        QStrReader reader((const byte *)text);
        while (len--)
            appendQChar(reader.nextQChar());
    }

    void appendStr(unsigned len, const char * text)
    {
        while (len--)
            appendChar(*text++);
    }

    void appendQChar(byte c)
    {
        switch (curLen & 3)
        {
        case 0:
            pending = c << 2;
            break;
        case 1:
            *buffer++ = pending | (c >> 4);
            pending = c << 4;
            break;
        case 2:
            *buffer++ = pending | (c >> 2);
            pending = c << 6;
            break;
        case 3:
            *buffer++ = pending | c;
            pending = 0;
            break;
        }
        curLen++;
    }

    void finish(unsigned max, byte fill)
    {
        while (curLen < max)
            appendQChar(fill & 0x3F);
        //force a final character to be output, but never writes too many.
        appendQChar(fill & 0x3F);
        //curLen is now undefined.
    }

protected:
    byte * buffer;
    unsigned curLen;
    byte pending;
};


//=============================================================================

void copyQStrRange(unsigned tlen, char * tgt, const char * src, unsigned from, unsigned to)
{
    unsigned copylen = to - from;
    if ((from & 3) == 0)
    {
        //can index the qstring directly...
        rtlQStrToQStr(tlen, tgt, copylen, src+QStrSize(from));
        //make sure the contents are in canonical format
        if ((copylen & 3) != 0)
        {
            unsigned copysize = QStrSize(copylen);
            tgt[copysize-1] &= lastQStrByteMask(copylen);
        }
    }
    else if (copylen == 0)
    {
        memset(tgt, 0, QStrSize(tlen));
    }
    else
    {
        //More: Could implement this cleverly by shifting and copying, but not worth it at the moment
        unsigned tempSrcLen;
        char * tempSrcPtr;
        rtlQStrToStrX(tempSrcLen, tempSrcPtr, from+copylen, src);
        rtlStrToQStr(tlen, tgt, copylen, tempSrcPtr+from);
        rtlFree(tempSrcPtr);
    }
}

//-----------------------------------------------------------------------------

unsigned rtlQStrLength(unsigned size)       { return QStrLength(size); }
unsigned rtlQStrSize(unsigned length)       { return QStrSize(length); }

unsigned rtlTrimQStrLen(size32_t l, const char * t)
{
    QStrReader reader((const byte *)t);
    reader.seek(l);
    while (l && (reader.prevQChar() == 0))
        l--;
    return l;
}


void rtlStrToQStr(size32_t outlen, char * out, size32_t inlen, const void *in)
{
    unsigned outSize = QStrSize(outlen);
    if (inlen >= outlen)
        inlen = outlen;
    else
    {
        size32_t size = QStrSize(inlen);
        memset(out+size, 0, outSize-size);
    }

    byte * curIn = (byte *)in;
    byte * endIn = curIn + inlen;
    byte * curOut = (byte *)out;
    while ((endIn-curIn)>=4)
    {
        byte c0 = compressQChar(curIn[0]);
        byte c1 = compressQChar(curIn[1]);
        byte c2 = compressQChar(curIn[2]);
        byte c3 = compressQChar(curIn[3]);
        curOut[0] = (c0 << 2) | (c1 >> 4);
        curOut[1] = (c1 << 4) | (c2 >> 2);
        curOut[2] = (c2 << 6) | c3;
        curIn += 4;
        curOut += 3;
    }

    byte c0;
    byte c1 = 0;
    byte c2 = 0;
    switch (endIn - curIn)
    {
    case 3:
        c2 = compressQChar(curIn[2]);
        curOut[2] = (c2 << 6);
        //fallthrough
    case 2:
        c1 = compressQChar(curIn[1]);
        curOut[1] = (c1 << 4) | (c2 >> 2);
        //fall through
    case 1:
        c0 = compressQChar(curIn[0]);
        curOut[0] = (c0 << 2) | (c1 >> 4);
        break;
    case 0:
        break;
    default:
        UNIMPLEMENTED;
    }
}


void rtlStrToQStrX(size32_t & outlen, char * & out, size32_t inlen, const void *in)
{
    outlen = inlen;
    out = (char *)malloc(QStrSize(inlen));
    rtlStrToQStr(inlen, out, inlen, in);
}


void rtlStrToQStrNX(size32_t & outlen, char * & out, size32_t inlen, const void * in, size32_t logicalLength)
{
    outlen = logicalLength;
    out = (char *)malloc(QStrSize(logicalLength));
    rtlStrToQStr(logicalLength, out, inlen, in);
}


void rtlQStrToData(size32_t outlen, void * out, size32_t inlen, const char *in)
{
    if (inlen >= outlen)
        inlen = outlen;
    else
        memset((char *)out+inlen, 0, outlen-inlen);
    rtlQStrToStr(inlen, (char *)out, inlen, in);
}

void rtlQStrToDataX(size32_t & outlen, void * & out, size32_t inlen, const char *in)
{
    outlen = inlen;
    out = (char *)malloc(inlen);
    rtlQStrToStr(inlen, (char *)out, inlen, in);
}

void rtlQStrToVStr(size32_t outlen, char * out, size32_t inlen, const char *in)
{
    out[--outlen] = 0;
    if (inlen >= outlen)
        inlen = outlen;
    else
        memset((char *)out+inlen, 0, outlen-inlen);

    rtlQStrToStr(inlen, out, inlen, in);
}

//NB: Need to be careful when expanding qstring3 to string3, that 4 bytes aren't written.
void rtlQStrToStr(size32_t outlen, char * out, size32_t inlen, const char * in)
{
    if (inlen < outlen)
    {
        memset(out+inlen, ' ', outlen-inlen);
        outlen = inlen;
    }

    const byte * curIn = (const byte *)in;
    byte * curOut = (byte *)out;
    byte * endOut = curOut + outlen;
    while ((endOut-curOut)>=4)
    {
        byte c0 = curIn[0];
        byte c1 = curIn[1];
        byte c2 = curIn[2];
        curOut[0] = expandQChar(c0 >> 2);
        curOut[1] = expandQChar(((c0 & 0x3) << 4) | (c1 >> 4));
        curOut[2] = expandQChar(((c1 & 0xF) << 2) | (c2 >> 6));
        curOut[3] = expandQChar(c2 & 0x3F);
        curIn += 3;
        curOut += 4;
    }

    switch (endOut - curOut)
    {
    case 3:
        curOut[2] = expandQChar(((curIn[1] & 0xF) << 2) | (curIn[2] >> 6));
        //fallthrough
    case 2:
        curOut[1] = expandQChar(((curIn[0] & 0x3) << 4) | (curIn[1] >> 4));
        //fallthrough
    case 1:
        curOut[0] = expandQChar(curIn[0] >> 2);
        break;
    case 0:
        break;
    default:
        UNIMPLEMENTED;
    }
}

void rtlQStrToStrX(size32_t & outlen, char * & out, size32_t inlen, const char *in)
{
    outlen = inlen;
    out = (char *)malloc(inlen);
    rtlQStrToStr(inlen, out, inlen, in);
}

void rtlQStrToQStr(size32_t outlen, char * out, size32_t inlen, const char * in)
{
    size32_t inSize = QStrSize(inlen);
    size32_t outSize = QStrSize(outlen);
    if (inSize >= outSize)
    {
        memcpy_iflen(out, in, outSize);
    }
    else
    {
        memcpy_iflen(out, in, inSize);
        memset(out+inSize, 0, outSize-inSize);
    }
}

void rtlQStrToQStrX(unsigned & outlen, char * & out, unsigned inlen, const char * in)
{
    size32_t inSize = QStrSize(inlen);
    char * data  = (char *)malloc(inSize);
    memcpy_iflen(data, in, inSize);

    outlen = inlen;
    out = data;
}

int rtlCompareQStrQStr(size32_t llen, const void * left, size32_t rlen, const void * right)
{
    size32_t lsize = QStrSize(llen);
    size32_t rsize = QStrSize(rlen);
    if (lsize < rsize)
    {
        int ret = memcmp_iflen(left, right, lsize);
        if (ret == 0)
        {
            const byte * r = (const byte *)right;
            while (lsize < rsize)
            {
                if (r[lsize])
                    return -1;
                lsize++;
            }
        }
        return ret;
    }
    int ret = memcmp_iflen(left, right, rsize);
    if (ret == 0)
    {
        const byte * l = (const byte *)left;
        while (lsize > rsize)
        {
            if (l[rsize])
                return +1;
            rsize++;
        }
    }
    return ret;
}


int rtlSafeCompareQStrQStr(size32_t llen, const void * left, size32_t rlen, const void * right)
{
    QStrReader leftIter((const byte *)left);
    QStrReader rightIter((const byte *)right);

    size32_t len = std::min(llen, rlen);
    for (unsigned i =0; i < len; i++)
    {
        int diff = (int)leftIter.nextQChar() - (int)rightIter.nextQChar();
        if (diff != 0)
            return diff;
    }
    if (len < llen)
    {
        while (len++ < llen)
        {
            if (leftIter.nextQChar() != 0)
                return +1;
        }
    }
    else
    {
        while (len++ < rlen)
        {
            if (rightIter.nextQChar() != 0)
                return -1;
        }
    }
    return 0;
}


void rtlDecPushQStr(size32_t len, const void * data)
{
    char * strData = (char *)alloca(len);
    rtlQStrToStr(len, strData, len, (const char *)data);
    DecPushString(len, strData);
}

bool rtlQStrToBool(size32_t inlen, const char *in)
{
    unsigned size = QStrSize(inlen);
    while (size--)
        if (in[size])
            return true;
    return false;
}

//---------------------------------------------------------------------------

ECLRTL_API void rtlCreateQStrRange(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * qstr, byte fill)
{
    //NB: Keep in sync with rtlCreateRange()
    if (compareLen > fieldLen)
    {
        if ((int)compareLen >= 0) 
        {
            //x[1..m] = y, m is larger than fieldLen, so truncate to fieldLen
            compareLen = fieldLen;
        }
        else
            compareLen = 0;             // probably m[1..-1] or something silly
    }

    //y has been trimmed when this function is called.  If y is longer than field length, then it is never going to match
    //so change the search range to FF,FF,FF ..  00.00.00 which will then never match.
    if (len > fieldLen)
    {
        compareLen = 0;
        fill = (fill == 0) ? 255 : 0;
    }

    outlen = fieldLen;
    out = (char *)malloc(QStrSize(fieldLen));
    QStrBuilder builder(out);
    if (len >= compareLen)
        builder.appendQStr(compareLen, qstr);
    else
    {
        builder.appendQStr(len, qstr);
        builder.appendCharN(compareLen-len, ' ');
    }
    builder.finish(fieldLen, fill);
}


ECLRTL_API void rtlCreateQStrRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * qstr)
{
    len = rtlTrimQStrLen(len, qstr);
    rtlCreateQStrRange(outlen, out, fieldLen, compareLen, len, qstr, 0);
}


ECLRTL_API void rtlCreateQStrRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * qstr)
{
    len = rtlTrimQStrLen(len, qstr);
    rtlCreateQStrRange(outlen, out, fieldLen, compareLen, len, qstr, 255);
}

void serializeQStrX(size32_t len, const char * data, MemoryBuffer &out)
{
    out.append(len).append(QStrSize(len), data);
}

void deserializeQStrX(size32_t & len, char * & data, MemoryBuffer &in)
{
    free(data);
    in.read(sizeof(len), &len);
    unsigned size = QStrSize(len);
    data = (char *)malloc(size);
    in.read(size, data);
}

