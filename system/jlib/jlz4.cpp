/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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
#include "jlz4.hpp"

#define COMMITTED ((size32_t)-1)

class jlib_decl CLZ4Compressor : public CInterface, public ICompressor
{
    size32_t blksz;
    size32_t bufalloc;
    MemoryAttr inma;        // equals blksize len
    byte *inbuf;
    size32_t inmax;         // remaining
    size32_t inlen;
    size32_t inlenblk;      // set to COMMITTED when so
    bool trailing;
    byte *outbuf;
    size32_t outlen;
    size32_t wrmax;

    inline void setinmax()
    {
        inmax = blksz-outlen-sizeof(size32_t);
        if (inmax<256)
            trailing = true;    // too small to bother compressing
        else {
            trailing = false;
            // inmax -= (fastlzSlack(inmax) + sizeof(size32_t));
            size32_t slack = LZ4_COMPRESSBOUND(inmax) - inmax;
            inmax -= (slack + sizeof(size32_t));
        }
    }

    inline void flushcommitted()
    {
        // only does non trailing
        if (trailing)
            return;
        size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
        if (toflush == 0)
            return;

        // printf("flushcommited() inlenblk=%d inlen=%d blksz=%d outlen=%d\n", inlenblk, inlen, blksz, outlen);

        assertex(outlen+sizeof(size32_t)*2+LZ4_COMPRESSBOUND(toflush)<=blksz);

        size32_t *cmpsize = (size32_t *)(outbuf+outlen);
        byte *out = (byte *)(cmpsize+1);

        *cmpsize = LZ4_compress((const char *)inbuf, (char *)out, toflush);

        if (*cmpsize<toflush) {
            *(size32_t *)outbuf += toflush;
            outlen += *cmpsize+sizeof(size32_t);
            if (inlenblk==COMMITTED)
                inlen = 0;
            else {
                inlen -= inlenblk;
                memmove(inbuf,inbuf+toflush,inlen);
            }
            setinmax();
            return;
        }
        trailing = true;
    }


public:
    IMPLEMENT_IINTERFACE;

    CLZ4Compressor()
    {
        outlen = 0;
        outbuf = NULL;      // only set on close
        bufalloc = 0;
        wrmax = 0;          // set at open
    }

    virtual ~CLZ4Compressor()
    {
        if (bufalloc)
            free(outbuf);
    }


    virtual void open(void *buf,size32_t max)
    {
        if (buf) {
            if (bufalloc) {
                free(outbuf);
            }
            bufalloc = 0;
            outbuf = (byte *)buf;
        }
        else if (max>bufalloc) {
            if (bufalloc)
                free(outbuf);
            bufalloc = max;
            outbuf = (byte *)malloc(bufalloc);
        }
        blksz = max;
        if (blksz!=inma.length())
            inbuf = (byte *)inma.allocate(blksz);
        else
            inbuf = (byte *)inma.bufferBase();
        if (blksz<1024)
            throw MakeStringException(-1,"CLZ4Compressor::open - block size (%d) not large enough", blksz);
        *(size32_t *)outbuf = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        inlenblk = COMMITTED;
        setinmax();
        wrmax = inmax;
        // printf("open() inlenblk=%d inlen=%d trailing=%d blksz=%d outlen=%d\n", inlenblk, inlen, trailing, blksz, outlen);
    }

    virtual void close()
    {
        if (inlenblk!=COMMITTED) {
            inlen = inlenblk; // transaction failed
            inlenblk = COMMITTED;
        }
        flushcommitted();
        // printf("close() inlenblk=%d inlen=%d trailing=%d blksz=%d outlen=%d\n", inlenblk, inlen, trailing, blksz, outlen);
        size32_t totlen = outlen+sizeof(size32_t)+inlen;
        assertex(blksz>=totlen);
        size32_t *tsize = (size32_t *)(outbuf+outlen);
        *tsize = inlen;
        memcpy(tsize+1,inbuf,inlen);
        outlen = totlen;
        *(size32_t *)outbuf += inlen;
        inbuf = NULL;
    }


    size32_t write(const void *buf,size32_t len)
    {
        // no more than wrmax per write
        size32_t lenb = wrmax;
        byte *b = (byte *)buf;
        size32_t written = 0;
        while (len)
        {
            if (len < lenb)
                lenb = len;
            if (lenb+inlen>inmax) {
                if (trailing)
                    return written;
                size32_t lenb2 = inmax - inlen;
                if (lenb2 >= 0x2000) {
                    memcpy(inbuf+inlen,b,lenb2);
                    b += lenb2;
                    inlen += lenb2;
                    len -= lenb2;
                    written += lenb2;
                    if (len < lenb)
                        lenb = len;
                }
                flushcommitted();
                if (lenb+inlen>inmax)
                    lenb = inmax-inlen;
            }
            if (lenb == 0)
                return written;
            memcpy(inbuf+inlen,b,lenb);
            b += lenb;
            inlen += lenb;
            len -= lenb;
            written += lenb;
        }
        return written;
    }

    void *  bufptr()
    {
        assertex(!inbuf);  // i.e. closed
        return outbuf;
    }
    size32_t    buflen()
    {
        assertex(!inbuf);  // i.e. closed
        return outlen;
    }
    void    startblock()
    {
        inlenblk = inlen;
    }
    void commitblock()
    {
        inlenblk = COMMITTED;
    }


};

class jlib_decl CLZ4Expander : public CInterface, public IExpander
{

    byte *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    const size32_t *in;

public:
    IMPLEMENT_IINTERFACE;

    CLZ4Expander()
    {
        outbuf = NULL;
        outlen = 0;
        bufalloc = 0;
    }
    ~CLZ4Expander()
    {
        if (bufalloc)
            free(outbuf);

    }

    virtual size32_t  init(const void *blk)
    {
        const size32_t *expsz = (const size32_t *)blk;
        outlen = *expsz;
        in = (expsz+1);
        return outlen;
    }

    virtual void expand(void *buf)
    {
        if (!outlen)
            return;
        if (buf) {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = (unsigned char *)buf;
        }
        else if (outlen>bufalloc) {
            if (bufalloc)
                free(outbuf);
            bufalloc = outlen;
            outbuf = (unsigned char *)malloc(bufalloc);
            if (!outbuf)
                throw MakeStringException(MSGAUD_operator,0, "Out of memory in LZ4Expander::expand, requesting %d bytes", bufalloc);
        }
        size32_t done = 0;
        loop {
            const size32_t szchunk = *in;
            in++;
            if (szchunk+done<outlen) {

                size32_t written = LZ4_decompress_safe((const char *)in, (char *)((byte *)buf+done), szchunk, outlen-done);

                done += written;
                if (!written||(done>outlen))
                    throw MakeStringException(0, "LZ4Expander - corrupt data(1) %d %d",written,szchunk);
            }
            else {
                if (szchunk+done!=outlen)
                    throw MakeStringException(0, "LZ4Expander - corrupt data(2) %d %d",szchunk,outlen);
                memcpy((byte *)buf+done,in,szchunk);
                break;
            }
            in = (const size32_t *)(((const byte *)in)+szchunk);
        }
    }

    virtual void *bufptr() { return outbuf;}
    virtual size32_t   buflen() { return outlen;}
};

void LZ4CompressToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
    size32_t outbase = out.length();

    size32_t *sz = (size32_t *)out.reserve(LZ4_COMPRESSBOUND(len)+sizeof(size32_t)*2);

    *sz = len;
    sz++;

    *sz = (len>16)?LZ4_compress((const char *)src, (char *)(sz+1), len):16;

    if (*sz>=len) {
        *sz = len;
        memcpy(sz+1,src,len);
    }
    out.setLength(outbase+*sz+sizeof(size32_t)*2);
}

void LZ4DecompressToBuffer(MemoryBuffer & out, const void * src)
{
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {

        size32_t written = LZ4_decompress_safe((const char *)sz, (char *)o, cmpsz, expsz);

        if (written!=expsz)
            throw MakeStringException(0, "LZ4DecompressToBuffer - corrupt data(1) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
}

void LZ4DecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in)
{
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {

        size32_t written = LZ4_decompress_safe((const char *)in.readDirect(cmpsz), (char *)o, cmpsz, expsz);

        if (written!=expsz)
            throw MakeStringException(0, "LZ4DecompressToBuffer - corrupt data(3) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
}

void LZ4DecompressToAttr(MemoryAttr & out, const void * src)
{
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {

        size32_t written = LZ4_decompress_safe((const char *)sz, (char *)o, cmpsz, expsz);

        if (written!=expsz)
            throw MakeStringException(0, "LZ4DecompressToBuffer - corrupt data(2) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
}

void LZ4DecompressToBuffer(MemoryAttr & out, MemoryBuffer & in)
{
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {

        size32_t written = LZ4_decompress_safe((const char *)in.readDirect(cmpsz), (char *)o, cmpsz, expsz);

        if (written!=expsz)
            throw MakeStringException(0, "LZ4DecompressToBuffer - corrupt data(4) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
}


ICompressor *createLZ4Compressor()
{
    return new CLZ4Compressor;
}

IExpander *createLZ4Expander()
{
    return new CLZ4Expander;
}

#define LZ4_BUFFER_SIZE (0x100000)

#define LZ4STRMCOMPRESSEDFILEFLAG (I64C(0xc3526de42f15da57)) // mck - what is an ok value ?


struct LZ4CompressedFileTrailer
{
    offset_t        zfill1;             // must be first
    offset_t        expandedSize;
    __int64         compressedType;
    unsigned        zfill2;             // must be last
};


class CLZ4Stream : public CInterface, implements IFileIOStream
{
    Linked<IFileIO> baseio;
    offset_t expOffset;     // expanded offset
    offset_t cmpOffset;     // compressed offset in file
    bool reading;
    MemoryAttr ma;
    size32_t bufsize;
    size32_t bufpos;        // reading only
    offset_t expSize;

    bool load()
    {
        bufpos = 0;
        bufsize = 0;
        if (expOffset==expSize)
            return false;
        size32_t sz[2];
        if (baseio->read(cmpOffset,sizeof(size32_t)*2,&sz)!=sizeof(size32_t)*2)
            return false;
        bufsize = sz[0];
        if (!bufsize)
            return false;
        cmpOffset += sizeof(size32_t)*2;
        if (ma.length()<bufsize)
            ma.allocate(bufsize);
        MemoryAttr cmpma;
        byte *cmpbuf = (byte *)cmpma.allocate(sz[1]);
        if (baseio->read(cmpOffset,sz[1],cmpbuf)!=sz[1])
            throw MakeStringException(-1,"CLZ4Stream: file corrupt.1");

        size32_t amnt = LZ4_decompress_safe((const char *)cmpbuf, (char *)ma.bufferBase(), sz[1], bufsize);

        if (amnt!=bufsize)
            throw MakeStringException(-1,"CLZ4Stream: file corrupt.2");

        cmpOffset += sz[1];
        return true;
    }

    void save()
    {
        if (bufsize) {
            MemoryAttr dstma;

            byte *dst = (byte *)dstma.allocate(sizeof(size32_t)*2+LZ4_COMPRESSBOUND(bufsize));

            size32_t sz = LZ4_compress((const char *)ma.get(), (char *)(sizeof(size32_t)*2+dst), bufsize);

            memcpy(dst,&bufsize,sizeof(size32_t));
            memcpy(dst+sizeof(size32_t),&sz,sizeof(size32_t));
            baseio->write(cmpOffset,sz+sizeof(size32_t)*2,dst);
            cmpOffset += sz+sizeof(size32_t)*2;
        }
        bufsize = 0;
    }


public:
    IMPLEMENT_IINTERFACE;

    CLZ4Stream()
    {
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;
    }

    ~CLZ4Stream()
    {
        flush();
    }

    bool attach(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;

        LZ4CompressedFileTrailer trailer;
        offset_t filesize = baseio->size();
        if (filesize<sizeof(trailer))
            return false;
        baseio->read(filesize-sizeof(trailer),sizeof(trailer),&trailer);
        expSize = trailer.expandedSize;
        return trailer.compressedType==LZ4STRMCOMPRESSEDFILEFLAG;
    }

    void create(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = false;
        bufpos = 0;
        bufsize = 0;
        ma.allocate(LZ4_BUFFER_SIZE);
        expSize = (offset_t)-1;
    }

    void seek(offset_t pos, IFSmode origin)
    {
        if ((origin==IFScurrent)&&(pos==0))
            return;
        if ((origin==IFSbegin)||(pos!=0))
            throw MakeStringException(-1,"CLZ4Stream seek not supported");
        expOffset = 0;
        bufpos = 0;
        bufsize = 0;
    }

    offset_t size()
    {
        return (expSize==(offset_t)-1)?0:expSize;
    }

    offset_t tell()
    {
        return expOffset;
    }


    size32_t read(size32_t len, void * data)
    {
        if (!reading)
            throw MakeStringException(-1,"CLZ4Stream read to stream being written");
        size32_t ret=0;
        while (len) {
            size32_t cpy = bufsize-bufpos;
            if (!cpy) {
                if (!load())
                    break;
                cpy = bufsize-bufpos;
            }
            if (cpy>len)
                cpy = len;
            memcpy(data,(const byte *)ma.get()+bufpos,cpy);
            bufpos += cpy;
            len -= cpy;
            ret += cpy;
        }
        expOffset += ret;
        return ret;
    }

    size32_t write(size32_t len, const void * data)
    {
        if (reading)
            throw MakeStringException(-1,"CLZ4Stream write to stream being read");
        size32_t ret = len;
        while (len+bufsize>LZ4_BUFFER_SIZE) {
            size32_t cpy = LZ4_BUFFER_SIZE-bufsize;
            memcpy((byte *)ma.bufferBase()+bufsize,data,cpy);
            data = (const byte *)data+cpy;
            len -= cpy;
            bufsize = LZ4_BUFFER_SIZE;
            save();
        }
        memcpy((byte *)ma.bufferBase()+bufsize,data,len);
        bufsize += len;
        expOffset += len;
        return ret;
    }

    void flush()
    {
        if (!reading&&(expSize!=expOffset)) {
            save();
            LZ4CompressedFileTrailer trailer;
            memset(&trailer,0,sizeof(trailer));
            trailer.compressedType = LZ4STRMCOMPRESSEDFILEFLAG;
            trailer.expandedSize = expOffset;
            baseio->write(cmpOffset,sizeof(trailer),&trailer);
            expSize = expOffset;
        }
    }

};

IFileIOStream *createLZ4StreamRead(IFileIO *base)
{
    Owned<CLZ4Stream> strm = new CLZ4Stream();
    if (strm->attach(base))
        return strm.getClear();
    return NULL;
}

IFileIOStream *createLZ4StreamWrite(IFileIO *base)
{
    Owned<CLZ4Stream> strm = new CLZ4Stream();
    strm->create(base);
    return strm.getClear();
}
