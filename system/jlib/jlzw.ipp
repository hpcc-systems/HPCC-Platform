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


#ifndef JLZW_IPP
#define JLZW_IPP

#include "jiface.hpp"

#include "jlzw.hpp"

#define LZW_HASH_TABLE_SIZE  0xc000 // 48K

class jlib_decl LZWDictionary 
{
public:
    LZWDictionary();
    void initdict();
    bool bumpbits();
    int nextcode;
    int curbits;
    int nextbump;
    int dictparent[LZW_HASH_TABLE_SIZE];
    unsigned char dictchar[LZW_HASH_TABLE_SIZE];
};


class jlib_decl CLZWCompressor : public CInterface, public ICompressor
{
public:
    IMPLEMENT_IINTERFACE;

    CLZWCompressor(bool _supportbigendian);
    virtual         ~CLZWCompressor();
    virtual void    open(void *blk,size32_t blksize);
    virtual void    close();
    virtual size32_t    write(const void *buf,size32_t len);
    virtual void *  bufptr() { return outbuf;}
    virtual size32_t    buflen() { return outlen;}
    virtual void    startblock();
    virtual void    commitblock();

protected:
    void flushbuf();
    virtual void initdict();
    size32_t inlen;
    size32_t inlenblk;
    size32_t outlen;
    size32_t maxlen;
    int curcode;
    size32_t bufalloc;
    void          *outbuf;
    unsigned char *outbytes;  // byte output
    unsigned char *outbits;   // for trailing bits
    unsigned char *outnext;   // next block
    unsigned char *inlast;    // for xoring
    unsigned char inuseflag;
    unsigned char outbitbuf;
    unsigned curShift;

    LZWDictionary dict;
    unsigned char dictinuse[LZW_HASH_TABLE_SIZE];
    int dictcode[LZW_HASH_TABLE_SIZE];
    bool supportbigendian;

};


class jlib_decl CLZWExpander : public CInterface, public IExpander
{
public:
    IMPLEMENT_IINTERFACE;

    CLZWExpander(bool _supportbigendian);
    ~CLZWExpander();
    virtual size32_t  init(const void *blk); // returns size required
    virtual void expand(void *target);
    virtual void *bufptr() { return outbuf;}
    virtual size32_t   buflen() { return outlen;}
protected:
    unsigned char *outbuf;
    size32_t outlen;
    size32_t outmax;
    size32_t bufalloc;
    unsigned char *inbytes;
    unsigned char *inbits;
    unsigned char *innext;
    unsigned curShift;
    LZWDictionary dict;
    unsigned char stack[LZW_HASH_TABLE_SIZE]; // assume compiler will align
    bool supportbigendian;
};




#define DEFAULT_DIFFCOMP_BUFSIZE 0x10000
class jlib_decl CDiffCompressedWriter : public CInterface
{
    byte *buffer, *previous, *bufPtr;
    size32_t recSize, remaining, bufSize;
    size32_t maxCompressedRecSize;

public:
    CDiffCompressedWriter()
    {
        buffer = previous = bufPtr = NULL;
        recSize = remaining = bufSize = 0;
    }

    ~CDiffCompressedWriter()
    {
        if (buffer) free((void *)buffer);
        if (previous) free((void *)previous);
    }

    void init(size32_t _recSize, size32_t _bufSize=DEFAULT_DIFFCOMP_BUFSIZE)
    {
        assertex(_bufSize>=_recSize);
        recSize = _recSize;
        bufSize = _bufSize;
        if (buffer) free(buffer);
        buffer = (byte *)malloc(bufSize); assertex(buffer);
        if (previous) { free(previous); previous = NULL; }
        maxCompressedRecSize = MaxDiffCompressedRowSize(recSize)+1; // one for some future extension
        reset();
    }

    void reset()
    {
        remaining = bufSize;
        bufPtr = buffer;
    }

    void flush()
    {
        verifyex(write(buffer, bufSize-remaining));
        reset();
    }

    void prime(const void *rec)
    {
        previous = (byte *)malloc(recSize); assertex(previous);
        memcpy(bufPtr, rec, recSize);
        memcpy(previous, rec, recSize);
        bufPtr += recSize;
        remaining -= recSize;
    }

    bool put(const void *rec)
    {
        if (!previous)
            prime(rec);
        else
        {
            if (remaining<maxCompressedRecSize) 
                flush();
            size32_t compSz = DiffCompress(rec, bufPtr, (void*)previous, recSize);
            bufPtr += compSz;
            remaining -= compSz;
        }
        return true;
    }

    bool putn(const void *recs, count_t n) // more efficient, can avoid copying previous
    {
        const byte *rec = (const byte *)recs;
        if (!previous)
        {
            prime(rec);
            rec += recSize;
            n--;
        }
        while (n--)
        {
            if (remaining<maxCompressedRecSize) 
                flush();
            // NB: previous maintained by DiffCompress
            size32_t compSz = DiffCompress(rec, bufPtr, (void*)previous, recSize);
            bufPtr += compSz;
            remaining -= compSz;
            rec += recSize;
        }
        return true;
    }

    virtual bool write(const byte *buf, size32_t sz) = 0;
};

#define READSIZE_UNSPECIFIED ((unsigned) -1)
class jlib_decl CDiffCompressedReader : public CInterface
{
    byte *bufferBase, *buffer, *bufPtr, *midPoint, *previous;
    size32_t recSize, remaining, bufSize, baseBufSize, fetched, overflow, halfBufSize;
    bool eof, chunkRead, firstHalf;
public:
    CDiffCompressedReader()
    {
        buffer = bufPtr = previous = NULL;
        recSize = remaining = bufSize = baseBufSize = 0;
    }

    ~CDiffCompressedReader()
    {
        if (buffer) free(bufferBase);
        if (previous) free(previous);
    }

    void init(size32_t _recSize, bool _chunkRead=false, size32_t _bufSize = DEFAULT_DIFFCOMP_BUFSIZE)
    {
        chunkRead = _chunkRead;
        recSize = _recSize;
        baseBufSize = _bufSize;
        if (baseBufSize < 4*recSize)
            baseBufSize = 4*recSize; // needs this min amount of room to play.
        overflow = recSize * 2;
        bufSize = baseBufSize + overflow*(chunkRead?2:1);
        if (buffer) free(bufferBase);
        if (previous) { free(previous); previous = NULL; }
        bufferBase = (byte *)malloc(bufSize); assertex(bufferBase);
        buffer = bufPtr = bufferBase+overflow;
        halfBufSize = baseBufSize/2;
        midPoint = buffer+halfBufSize+overflow;
        eof = false;
        firstHalf = true;
        fetched = remaining = 0;
    }

    bool more()
    {
        if (!eof && remaining < overflow)
        {
            // chunkRead reads a chunk of the buffer at a time, ensuring others are free to be filled.
            // (using 2 chunks - 1st half/2nd half)
            if (chunkRead)
            {
                size32_t r = remaining;
                byte *b, *bs;
                if (firstHalf)
                {
                    b = bs = buffer-remaining;
                    firstHalf = false;
                }
                else
                {
                    b = bs = midPoint-remaining;
                    firstHalf = true;
                }
                while (r--)
                    *b++ = *bufPtr++;
                bufPtr = bs;
                fetched = 0;
                loop
                {
                    size32_t sz = read(b+fetched, halfBufSize-fetched);
                    fetched += sz;
                    remaining += sz;
                    if (0 == sz)
                    {
                        eof = true;
                        break;
                    }
                    else if (remaining >= recSize * 2)
                        break;
                }
            }
            else
            {
                size32_t sz = readSize();
                if (sz)
                {
                    size32_t r = remaining;
                    byte *b = buffer-remaining;
                    byte *bs = b;
                    while (r--)
                        *b++ = *bufPtr++;
                    bufPtr = bs;
                    fetched = 0;
                    loop
                    {
                        if (READSIZE_UNSPECIFIED == sz)
                            sz = baseBufSize;
                        else if (sz > baseBufSize)
                        {
                            baseBufSize = sz;
                            bufSize = baseBufSize + overflow;
                            bufferBase = (byte *)checked_realloc(bufferBase, bufSize, baseBufSize, -31);
                            buffer = bufferBase + overflow;
                            bufPtr = buffer-remaining;
                        }
                        sz = read(buffer+fetched, sz-fetched);
                        fetched += sz;
                        remaining += sz;
                        if (0 == sz)
                        {
                            eof = true;
                            break;
                        }
                        else if (remaining >= recSize * 2)
                            break;
                        sz = readSize();
                    }
                }
            }
        }
        return (remaining>0);
    }

    void prime(void *rec)
    {
        previous = (byte *)malloc(recSize); assertex(previous);
        memcpy(rec, bufPtr, recSize);
        bufPtr += recSize;
        remaining -= recSize;
    }

    bool get(void *rec)
    {
        if (!more()) return false;
        if (!previous)
            prime(rec);
        else
        {
            size32_t sz = DiffExpand(bufPtr, rec, previous, recSize);
            bufPtr += sz;
            remaining -= sz;
        }
        memcpy(previous, rec, recSize);
        return true;
    }

    bool eos()
    {
        if (remaining)
            return false;
        if (eof)
            return true;
        return (doneInput());
    }

    unsigned getn(void *_dst, size32_t maxSize)
    {
        if (!more()) return 0;
        unsigned n=0;
        byte *dst = (byte *)_dst;
        if (!previous)
        {
            prime(dst);
            memcpy(previous, dst, recSize);
            dst += recSize;
            maxSize -= recSize;
            n++;
        }

        byte *_previous = previous;
        while (maxSize >= recSize)
        {
            if (!more()) break;
            size32_t sz = DiffExpand(bufPtr, dst, _previous, recSize);
            bufPtr += sz;
            remaining -= sz;
            _previous = dst;
            dst += recSize;
            maxSize -= recSize;
            n++;
        }
        if (n>1) memcpy(previous, dst-recSize, recSize);
        return n;
    }

    virtual size32_t readSize() = 0;
    virtual size32_t read(void *buf, size32_t max) = 0;
    virtual bool doneInput() { assertex(!"doneInput"); return false; } // only needs implementing if eos used
};



#endif

