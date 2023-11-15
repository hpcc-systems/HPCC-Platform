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
#include "jfcmp.hpp"
#include "jlz4.hpp"
#include "lz4.h"
#include "lz4hc.h"

/* Format:
    size32_t totalexpsize;
    { size32_t subcmpsize; bytes subcmpdata; }
    size32_t trailsize; bytes traildata;    // unexpanded
*/

class CLZ4Compressor final : public CFcmpCompressor
{
    bool hc;
protected:
    virtual void setinmax() override
    {
        if (blksz <= outlen+sizeof(size32_t))
            trailing = true;    // too small to bother compressing
        else
        {
            trailing = false;
            inmax = blksz-outlen-sizeof(size32_t);
            size32_t slack = LZ4_COMPRESSBOUND(inmax) - inmax;
            if (inmax <= (slack + sizeof(size32_t)))
                trailing = true;
            else
                inmax = inmax - (slack + sizeof(size32_t));
        }
    }

    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(bufalloc == 0 && !outBufMb);       // Only supported when a fixed size buffer is provided
        assertex(inlenblk == COMMITTED);            // not inside a transaction
        assertex(newLimit <= originalMax);

        //Reject the limit change if it is too small for the data already committed.
        if (newLimit < inlen + outlen + sizeof(size32_t))
            return false;

        blksz = newLimit;
        setinmax();
        return true;
    }

    virtual void flushcommitted() override
    {
        // only does non trailing
        if (trailing)
            return;
        size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
        if (toflush == 0)
            return;

        if (false && toflush < 256)
        {
            trailing = true;
            return;
        }

        size32_t outSzRequired = outlen+sizeof(size32_t)*2+LZ4_COMPRESSBOUND(toflush);
        if (!dynamicOutSz)
            assertex(outSzRequired<=blksz);
        else
        {
            if (outSzRequired>dynamicOutSz)
            {
                verifyex(outBufMb->ensureCapacity(outBufStart+outSzRequired));
                dynamicOutSz = outBufMb->capacity();
                outbuf = ((byte *)outBufMb->bufferBase()+outBufStart);
            }
        }
        size32_t *cmpsize = (size32_t *)(outbuf+outlen);
        byte *out = (byte *)(cmpsize+1);

        if (hc)
            *cmpsize = LZ4_compress_HC((const char *)inbuf, (char *)out, toflush, LZ4_COMPRESSBOUND(toflush), LZ4HC_CLEVEL_DEFAULT);
        else
            *cmpsize = LZ4_compress_default((const char *)inbuf, (char *)out, toflush, LZ4_COMPRESSBOUND(toflush));
        if (*cmpsize && *cmpsize<toflush)
        {
            *(size32_t *)outbuf += toflush;
            outlen += *cmpsize+sizeof(size32_t);
            if (inlenblk==COMMITTED)
                inlen = 0;
            else
            {
                inlen -= inlenblk;
                memmove(inbuf,inbuf+toflush,inlen);
            }
            setinmax();
            return;
        }
        trailing = true;
    }
public:
    CLZ4Compressor(bool _hc) : hc(_hc)
    {        
    }
};


class jlib_decl CLZ4Expander : public CFcmpExpander
{
public:
    virtual void expand(void *buf)
    {
        if (!outlen)
            return;
        if (buf)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = (unsigned char *)buf;
        }
        else if (outlen>bufalloc)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = outlen;
            outbuf = (unsigned char *)malloc(bufalloc);
            if (!outbuf)
                throw MakeStringException(MSGAUD_operator,0, "Out of memory in LZ4Expander::expand, requesting %d bytes", bufalloc);
        }
        size32_t done = 0;
        for (;;)
        {
            const size32_t szchunk = *in;
            in++;
            if (szchunk+done<outlen)
            {
                size32_t written = LZ4_decompress_safe((const char *)in, (char *)((byte *)buf+done), szchunk, outlen-done);
                done += written;
                if (!written||(done>outlen))
                    throw MakeStringException(0, "LZ4Expander - corrupt data(1) %d %d",written,szchunk);
            }
            else
            {
                if (szchunk+done!=outlen)
                    throw MakeStringException(0, "LZ4Expander - corrupt data(2) %d %d",szchunk,outlen);
                memcpy((byte *)buf+done,in,szchunk);
                break;
            }
            in = (const size32_t *)(((const byte *)in)+szchunk);
        }
    }

};

void LZ4CompressToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
    size32_t outbase = out.length();
    out.append(len);
    DelayedMarker<size32_t> cmpSzMarker(out);
    void *cmpData = out.reserve(LZ4_COMPRESSBOUND(len));
    if (len < 64)
        memcpy(cmpData, src, len);
    else
    {
        size32_t cmpSz = LZ4_compress_default((const char *)src, (char *)cmpData, len, LZ4_COMPRESSBOUND(len));
        if (!cmpSz)
            memcpy(cmpData, src, len);
        else
            len = cmpSz;
    }
    cmpSzMarker.write(len);
    out.setLength(outbase+len+sizeof(size32_t)*2);
}

void LZ4DecompressToBuffer(MemoryBuffer & out, const void * src)
{
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz)
    {
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
    if (cmpsz!=expsz)
    {
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
    if (cmpsz!=expsz)
    {
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
    if (cmpsz!=expsz)
    {
        size32_t written = LZ4_decompress_safe((const char *)in.readDirect(cmpsz), (char *)o, cmpsz, expsz);
        if (written!=expsz)
            throw MakeStringException(0, "LZ4DecompressToBuffer - corrupt data(4) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
}


ICompressor *createLZ4Compressor(bool hc)
{
    return new CLZ4Compressor(hc);
}

IExpander *createLZ4Expander()
{
    return new CLZ4Expander;
}

#define LZ4STRMCOMPRESSEDFILEFLAG (I64C(0xc129b02d53545e91))

class CLZ4Stream : public CFcmpStream
{
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
        if (bufsize)
        {
            MemoryAttr dstma;
            byte *dst = (byte *)dstma.allocate(sizeof(size32_t)*2+LZ4_COMPRESSBOUND(bufsize));
            size32_t sz = LZ4_compress_default((const char *)ma.get(), (char *)(sizeof(size32_t)*2+dst), bufsize, LZ4_COMPRESSBOUND(bufsize));
            if (!sz)
            {
                sz = bufsize;
                memcpy((sizeof(size32_t)*2+dst), ma.get(), bufsize);
            }
            memcpy(dst,&bufsize,sizeof(size32_t));
            memcpy(dst+sizeof(size32_t),&sz,sizeof(size32_t));
            baseio->write(cmpOffset,sz+sizeof(size32_t)*2,dst);
            cmpOffset += sz+sizeof(size32_t)*2;
        }
        bufsize = 0;
    }


public:
    CLZ4Stream() : CFcmpStream(LZ4STRMCOMPRESSEDFILEFLAG) { }

    virtual ~CLZ4Stream() { flush(); }

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
