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
#include "jden.hpp"

#ifdef _USE_DENSITY
# include "../density/density_api.h"
 /*
  * DENSITY_COMPRESSION_MODE_CHAMELEON_ALGORITHM - low compression but fastest
  * DENSITY_COMPRESSION_MODE_CHEETAH_ALGORITHM   - med compression but slower
  * DENSITY_COMPRESSION_MODE_LION_ALGORITHM      - high compression but slowest
  */
# define DEN_COMP_METHOD DENSITY_COMPRESSION_MODE_CHAMELEON_ALGORITHM
#endif

#define COMMITTED ((size32_t)-1)

/* Format:
    size32_t totalexpsize;
    { size32_t subcmpsize; bytes subcmpdata; }
    size32_t trailsize; bytes traildata;    // unexpanded
*/

#ifdef _USE_DENSITY
class jlib_decl CDENCompressor : public CInterface, public ICompressor
{
    size32_t blksz;
    size32_t bufalloc;
    MemoryBuffer inma;      // equals blksize len
    MemoryBuffer *outBufMb; // used when dynamic output buffer (when open() used)
    size32_t outBufStart;
    byte *inbuf;
    size32_t inmax;         // remaining
    size32_t inlen;
    size32_t inlenblk;      // set to COMMITTED when so
    bool trailing;
    byte *outbuf;
    size32_t outlen;
    size32_t wrmax;
    size32_t dynamicOutSz;

    inline void setinmax()
    {
        inmax = blksz-outlen-2*sizeof(size32_t);
        trailing = false;
        inmax /= 2; // compress needs addl space or it will fail
        if (inmax <= DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE)
            trailing = true;    // too small to bother compressing
    }

    inline void flushcommitted()
    {
        // only does non trailing
        if (trailing)
            return;
        size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
        if (toflush == 0)
            return;

        if (toflush <= DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE)
        {
            trailing = true;
            return;
        }

        size32_t outSzRequired = outlen+sizeof(size32_t)*2+density_buffer_compress_safe_size(toflush);
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

        density_buffer_processing_result result = density_buffer_compress((const uint8_t *)inbuf, toflush, (uint8_t *)out, density_buffer_compress_safe_size(toflush), DEN_COMP_METHOD, DENSITY_BLOCK_TYPE_DEFAULT);
        if (result.state)
        {
            trailing = true;
            return;
        }
        *cmpsize = result.bytesWritten;
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

    void initCommon()
    {
        blksz = inma.capacity();
        *(size32_t *)outbuf = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        inlenblk = COMMITTED;
        setinmax();
    }
public:
    IMPLEMENT_IINTERFACE;

    CDENCompressor()
    {
        outlen = 0;
        outbuf = NULL;      // only set on close
        bufalloc = 0;
        wrmax = 0;          // set at open
        dynamicOutSz = 0;
        outBufMb = NULL;
        outBufStart = 0;
        inbuf = NULL;
    }

    virtual ~CDENCompressor()
    {
        if (bufalloc)
            free(outbuf);
    }


    virtual void open(void *buf,size32_t max)
    {
        if (max<1024)
            throw MakeStringException(-1,"CDENCompressor::open - block size (%d) not large enough", blksz);
        wrmax = max;
        if (buf)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = 0;
            outbuf = (byte *)buf;
        }
        else if (max>bufalloc)
        {
            if (bufalloc)
                free(outbuf);
            bufalloc = max;
            outbuf = (byte *)malloc(bufalloc);
        }
        outBufMb = NULL;
        outBufStart = 0;
        dynamicOutSz = 0;
        inbuf = (byte *)inma.ensureCapacity(max);
        initCommon();
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize)
    {
        if (!initialSize)
            initialSize = 0x100000; // 1MB
        if (initialSize<1024)
            throw MakeStringException(-1,"CDENCompressor::open - block size (%d) not large enough", initialSize);
        wrmax = initialSize;
        if (bufalloc)
        {
            free(outbuf);
            bufalloc = 0;
        }
        inbuf = (byte *)inma.ensureCapacity(initialSize);
        outBufMb = &mb;
        outBufStart = mb.length();
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
        dynamicOutSz = outBufMb->capacity();
        initCommon();
    }

    virtual void close()
    {
        if (inlenblk!=COMMITTED) {
            inlen = inlenblk; // transaction failed
            inlenblk = COMMITTED;
        }
        flushcommitted();
        size32_t totlen = outlen+sizeof(size32_t)+inlen;
        assertex(blksz>=totlen);
        size32_t *tsize = (size32_t *)(outbuf+outlen);
        *tsize = inlen;
        memcpy(tsize+1,inbuf,inlen);
        outlen = totlen;
        *(size32_t *)outbuf += inlen;
        inbuf = NULL;
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }


    size32_t write(const void *buf,size32_t len)
    {
        // no more than wrmax per write (unless dynamically sizing)
        size32_t lenb = wrmax;
        byte *b = (byte *)buf;
        size32_t written = 0;
        while (len)
        {
            if (len < lenb)
                lenb = len;
            if (lenb+inlen>inmax)
            {
                if (trailing)
                    return written;
                flushcommitted();
                if (lenb+inlen>inmax)
                {
                    if (outBufMb) // sizing input buffer, but outBufMb!=NULL is condition of whether in use or not
                    {
                        blksz += len > 0x100000 ? len : 0x100000;
                        verifyex(inma.ensureCapacity(blksz));
                        blksz = inma.capacity();
                        inbuf = (byte *)inma.bufferBase();
                        wrmax = blksz;
                        setinmax();
                    }
                    lenb = inmax-inlen;
                    if (len < lenb)
                        lenb = len;
                }
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

class jlib_decl CDENExpander : public CInterface, public IExpander
{
    byte *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    const size32_t *in;

public:
    IMPLEMENT_IINTERFACE;

    CDENExpander()
    {
        outbuf = NULL;
        outlen = 0;
        bufalloc = 0;
    }
    ~CDENExpander()
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
                throw MakeStringException(MSGAUD_operator,0, "Out of memory in DENExpander::expand, requesting %d bytes", bufalloc);
        }
        size32_t done = 0;
        loop {
            const size32_t szchunk = *in;
            in++;
            // DBGLOG("szchunk = %u  outlen-done = %u", szchunk, outlen-done);
            if (szchunk+done<outlen) {
                density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)in, szchunk, ((byte *)buf+done), outlen-done);
                if (result.state)
                    throw MakeStringException(0, "DENExpander - corrupt data(3) %d %d %d %d", szchunk, outlen, done, result.state);
                size32_t written = result.bytesWritten;
                done += written;
                if (!written||(done>outlen))
                    throw MakeStringException(0, "DENExpander - corrupt data(1) %d %d",written,szchunk);
            }
            else {
                if (szchunk+done!=outlen)
                    throw MakeStringException(0, "DENExpander - corrupt data(2) %d %d",szchunk,outlen);
                memcpy((byte *)buf+done,in,szchunk);
                break;
            }
            in = (const size32_t *)(((const byte *)in)+szchunk);
        }
    }

    virtual void *bufptr() { return outbuf;}
    virtual size32_t   buflen() { return outlen;}
};
#endif

void DENCompressToBuffer(MemoryBuffer & out, size32_t len, const void * src)
{
#ifdef _USE_DENSITY
    size32_t outbase = out.length();
    size32_t *sz = (size32_t *)out.reserve(density_buffer_compress_safe_size(len)+sizeof(size32_t)*2);
    *sz = len;
    sz++;
    if (len <= DENSITY_MINIMUM_OUTPUT_BUFFER_SIZE)
    {
        *sz = len;
        memcpy(sz+1,src,len);
    }
    else
    {
        density_buffer_processing_result result = density_buffer_compress((const uint8_t *)src, len, (uint8_t *)(sz+1), density_buffer_compress_safe_size(len), DEN_COMP_METHOD, DENSITY_BLOCK_TYPE_DEFAULT);
        if (result.state)
            throw MakeStringException(-1,"DENCompressToBuffer - corrupt data(5) %u %d", len, result.state);
        *sz = result.bytesWritten;
    }
    out.setLength(outbase+*sz+sizeof(size32_t)*2);
#else
    throw MakeStringException(-1,"Density CompressToBuffer not supported on this platform");
#endif
}

void DENDecompressToBuffer(MemoryBuffer & out, const void * src)
{
#ifdef _USE_DENSITY
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {
        density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)sz, cmpsz, (uint8_t *)o, expsz);
        if (result.state)
            throw MakeStringException(-1,"DENDecompressToBuffer - corrupt data(1) %u %u %d", cmpsz, expsz, result.state);
        size32_t written = result.bytesWritten;
        if (written!=expsz)
            throw MakeStringException(-1, "DENDecompressToBuffer - corrupt data(1) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
#else
    throw MakeStringException(-1,"Density DecompressToBuffer not supported on this platform");
#endif
}

void DENDecompressToBuffer(MemoryBuffer & out, MemoryBuffer & in)
{
#ifdef _USE_DENSITY
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.reserve(expsz);
    if (cmpsz!=expsz) {
        density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)in.readDirect(cmpsz), cmpsz, (uint8_t *)o, expsz);
        if (result.state)
            throw MakeStringException(-1,"DENDecompressToBuffer - corrupt data(3) %u %u %d", cmpsz, expsz, result.state);
        size32_t written = result.bytesWritten;
        if (written!=expsz)
            throw MakeStringException(-1, "DENDecompressToBuffer - corrupt data(3) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
#else
    throw MakeStringException(-1,"Density DecompressToBuffer not supported on this platform");
#endif
}

void DENDecompressToAttr(MemoryAttr & out, const void * src)
{
#ifdef _USE_DENSITY
    size32_t *sz = (size32_t *)src;
    size32_t expsz = *(sz++);
    size32_t cmpsz = *(sz++);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {
        density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)sz, cmpsz, (uint8_t *)o, expsz);
        if (result.state)
            throw MakeStringException(-1,"DENDecompressToBuffer - corrupt data(2) %u %u %d", cmpsz, expsz, result.state);
        size32_t written = result.bytesWritten;
        if (written!=expsz)
            throw MakeStringException(-1, "DENDecompressToBuffer - corrupt data(2) %d %d",written,expsz);
    }
    else
        memcpy(o,sz,expsz);
#else
    throw MakeStringException(-1,"Density DecompressToBuffer not supported on this platform");
#endif
}

void DENDecompressToBuffer(MemoryAttr & out, MemoryBuffer & in)
{
#ifdef _USE_DENSITY
    size32_t expsz;
    size32_t cmpsz;
    in.read(expsz).read(cmpsz);
    void *o = out.allocate(expsz);
    if (cmpsz!=expsz) {
        density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)in.readDirect(cmpsz), cmpsz, (uint8_t *)o, expsz);
        if (result.state)
            throw MakeStringException(-1,"DENDecompressToBuffer - corrupt data(4) %u %u %d", cmpsz, expsz, result.state);
        size32_t written = result.bytesWritten;
        if (written!=expsz)
            throw MakeStringException(-1, "DENDecompressToBuffer - corrupt data(4) %d %d",written,expsz);
    }
    else
        memcpy(o,in.readDirect(cmpsz),expsz);
#else
    throw MakeStringException(-1,"Density DecompressToBuffer not supported on this platform");
#endif
}


ICompressor *createDENCompressor()
{
#ifdef _USE_DENSITY
    return new CDENCompressor;
#else
    throw MakeStringException(-1, "Density Compressor not supported on this platform");
#endif
}

IExpander *createDENExpander()
{
#ifdef _USE_DENSITY
    return new CDENExpander;
#else
    throw MakeStringException(-1, "Density Expander not supported on this platform");
#endif
}

#define DEN_BUFFER_SIZE (0x100000)

#define DENSTRMCOMPRESSEDFILEFLAG (I64C(0xc3526de42f24db61))


struct DENCompressedFileTrailer
{
    offset_t        zfill1;             // must be first
    offset_t        expandedSize;
    __int64         compressedType;
    unsigned        zfill2;             // must be last
};


#ifdef _USE_DENSITY
class CDENStream : public CInterface, implements IFileIOStream
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
            throw MakeStringException(-1,"CDENStream: file corrupt.1");
        density_buffer_processing_result result = density_buffer_decompress((const uint8_t *)cmpbuf, sz[1], (uint8_t *)ma.bufferBase(), bufsize);
        if (result.state)
            throw MakeStringException(-1,"CDENStream: file corrupt.3 %u %u %d", sz[1], bufsize, result.state);
        size32_t amnt = result.bytesWritten;
        if (amnt!=bufsize)
            throw MakeStringException(-1,"CDENStream: file corrupt.2");
        cmpOffset += sz[1];
        return true;
    }

    void save()
    {
        if (bufsize) {
            MemoryAttr dstma;
            byte *dst = (byte *)dstma.allocate(sizeof(size32_t)*2+density_buffer_compress_safe_size(bufsize));
            density_buffer_processing_result result = density_buffer_compress((const uint8_t *)ma.get(), bufsize, (uint8_t *)(sizeof(size32_t)*2+dst), density_buffer_compress_safe_size(bufsize), DEN_COMP_METHOD, DENSITY_BLOCK_TYPE_DEFAULT);
            if (result.state)
                throw MakeStringException(-1,"CDENStream: file corrupt.4 %u %lu %d", bufsize, density_buffer_compress_safe_size(bufsize), result.state);
            size32_t sz = result.bytesWritten;
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
    IMPLEMENT_IINTERFACE;

    CDENStream()
    {
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;
    }

    ~CDENStream()
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

        DENCompressedFileTrailer trailer;
        offset_t filesize = baseio->size();
        if (filesize<sizeof(trailer))
            return false;
        baseio->read(filesize-sizeof(trailer),sizeof(trailer),&trailer);
        expSize = trailer.expandedSize;
        return trailer.compressedType==DENSTRMCOMPRESSEDFILEFLAG;
    }

    void create(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = false;
        bufpos = 0;
        bufsize = 0;
        ma.allocate(DEN_BUFFER_SIZE);
        expSize = (offset_t)-1;
    }

    void seek(offset_t pos, IFSmode origin)
    {
        if ((origin==IFScurrent)&&(pos==0))
            return;
        if ((origin==IFSbegin)||(pos!=0))
            throw MakeStringException(-1,"CDENStream seek not supported");
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
            throw MakeStringException(-1,"CDENStream read to stream being written");
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
            throw MakeStringException(-1,"CDENStream write to stream being read");
        size32_t ret = len;
        while (len+bufsize>DEN_BUFFER_SIZE) {
            size32_t cpy = DEN_BUFFER_SIZE-bufsize;
            memcpy((byte *)ma.bufferBase()+bufsize,data,cpy);
            data = (const byte *)data+cpy;
            len -= cpy;
            bufsize = DEN_BUFFER_SIZE;
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
            DENCompressedFileTrailer trailer;
            memset(&trailer,0,sizeof(trailer));
            trailer.compressedType = DENSTRMCOMPRESSEDFILEFLAG;
            trailer.expandedSize = expOffset;
            baseio->write(cmpOffset,sizeof(trailer),&trailer);
            expSize = expOffset;
        }
    }
};
#endif

IFileIOStream *createDENStreamRead(IFileIO *base)
{
#ifdef _USE_DENSITY
    Owned<CDENStream> strm = new CDENStream();
    if (strm->attach(base))
        return strm.getClear();
    return NULL;
#else
    throw MakeStringException(-1, "Density Stream Expander not supported on this platform");
    return NULL;
#endif
}

IFileIOStream *createDENStreamWrite(IFileIO *base)
{
#ifdef _USE_DENSITY
    Owned<CDENStream> strm = new CDENStream();
    strm->create(base);
    return strm.getClear();
#else
    throw MakeStringException(-1, "Density Stream Compressor not supported on this platform");
    return NULL;
#endif
}
