/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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
#include "jlzw.hpp"

#define COMMITTED ((size32_t)-1)

#define FCMP_BUFFER_SIZE (0x100000)

class jlib_decl CFcmpCompressor : public CSimpleInterfaceOf<ICompressor>
{
protected:
    size32_t blksz = 0;
    size32_t bufalloc = 0;
    MemoryBuffer inma;      // equals blksize len
    MemoryBuffer *outBufMb = nullptr; // used when dynamic output buffer (when open() used)
    size32_t outBufStart = 0;
    byte *inbuf = nullptr;
    size32_t inmax = 0;         // remaining
    size32_t inlen = 0;
    size32_t inlenblk = 0;      // set to COMMITTED when so
    bool trailing = false;
    byte *outbuf = nullptr;
    size32_t outlen = 0;
    size32_t wrmax = 0;
    size32_t dynamicOutSz = 0;

    virtual void setinmax() = 0;
    virtual void flushcommitted() = 0;

    void initCommon(size32_t initialSize)
    {
        blksz = initialSize;
        *(size32_t *)outbuf = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        inlenblk = COMMITTED;
        setinmax();
    }

public:
    CFcmpCompressor()
    {
        outbuf = NULL;      // only set on close
        wrmax = 0;          // set at open
    }

    virtual ~CFcmpCompressor()
    {
        if (bufalloc)
            free(outbuf);
    }

    virtual void open(void *buf,size32_t max)
    {
        if (max<1024)
            throw MakeStringException(-1,"CFcmpCompressor::open - block size (%d) not large enough", max);
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
            outbuf = (byte *)malloc(max);
            if (!outbuf)
                throw MakeStringException(-1,"CFcmpCompressor::open - out of memory, requesting %d bytes", max);
            bufalloc = max;
        }
        outBufMb = NULL;
        outBufStart = 0;
        dynamicOutSz = 0;
        inbuf = (byte *)inma.ensureCapacity(max);
        initCommon(max);
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize)
    {
        if (!initialSize)
            initialSize = FCMP_BUFFER_SIZE; // 1MB
        if (initialSize<1024)
            throw MakeStringException(-1,"CFcmpCompressor::open - block size (%d) not large enough", initialSize);
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
        initCommon(initialSize);
    }

    virtual void close()
    {
        if (inlenblk!=COMMITTED)
        {
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
                        blksz += len > FCMP_BUFFER_SIZE ? len : FCMP_BUFFER_SIZE;
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

    void * bufptr()
    {
        assertex(!inbuf);  // i.e. closed
        return outbuf;
    }

    size32_t buflen()
    {
        assertex(!inbuf);  // i.e. closed
        return outlen;
    }

    void startblock()
    {
        inlenblk = inlen;
    }

    void commitblock()
    {
        inlenblk = COMMITTED;
    }

};


class jlib_decl CFcmpExpander : public CSimpleInterfaceOf<IExpander>
{
protected:
    byte *outbuf;
    size32_t outlen;
    size32_t bufalloc;
    const size32_t *in = nullptr;

public:
    CFcmpExpander()
    {
        outbuf = NULL;
        outlen = 0;
        bufalloc = 0;
    }

    virtual ~CFcmpExpander()
    {
        if (bufalloc)
            free(outbuf);
    }

    virtual size32_t init(const void *blk)
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
                throw MakeStringException(MSGAUD_operator,0, "Out of memory in FcmpExpander::expand, requesting %d bytes", bufalloc);
        }
        size32_t done = 0;
        for (;;)
        {
            const size32_t szchunk = *in;
            in++;
            if (szchunk+done<outlen)
            {
                memcpy((byte *)buf+done, in, szchunk);
                size32_t written = szchunk;
                done += written;
                if (!written||(done>outlen))
                    throw MakeStringException(0, "FcmpExpander - corrupt data(1) %d %d",written,szchunk);
            }
            else
            {
                if (szchunk+done!=outlen)
                    throw MakeStringException(0, "FcmpExpander - corrupt data(2) %d %d",szchunk,outlen);
                memcpy((byte *)buf+done,in,szchunk);
                break;
            }
            in = (const size32_t *)(((const byte *)in)+szchunk);
        }
    }

    virtual void *bufptr() { return outbuf;}
    virtual size32_t buflen() { return outlen;}
};

struct FcmpCompressedFileTrailer
{
    offset_t        zfill1;             // must be first
    offset_t        expandedSize;
    __int64         compressedType;
    unsigned        zfill2;             // must be last
};

class CFcmpStream : public CSimpleInterfaceOf<IFileIOStream>
{
protected:
    Linked<IFileIO> baseio;
    offset_t expOffset;     // expanded offset
    offset_t cmpOffset;     // compressed offset in file
    bool reading;
    MemoryAttr ma;
    size32_t bufsize;
    size32_t bufpos = 0;        // reading only
    offset_t expSize = 0;
    __int64 compType;

public:
    CFcmpStream(__int64 _compType) : compType(_compType)
    {
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;
    }

    virtual ~CFcmpStream() { flush(); }

    virtual bool load()
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
            throw MakeStringException(-1,"CFcmpStream: file corrupt.1");
        memcpy(ma.bufferBase(), cmpbuf, sz[1]);
        size32_t amnt = sz[1];
        if (amnt!=bufsize)
            throw MakeStringException(-1,"CFcmpStream: file corrupt.2");
        cmpOffset += sz[1];
        return true;
    }

    virtual void save()
    {
        if (bufsize)
        {
            MemoryAttr dstma;
            byte *dst = (byte *)dstma.allocate(sizeof(size32_t)*2+bufsize);
            memcpy((sizeof(size32_t)*2+dst), ma.get(), bufsize);
            size32_t sz = bufsize;
            memcpy(dst,&bufsize,sizeof(size32_t));
            memcpy(dst+sizeof(size32_t),&sz,sizeof(size32_t));
            baseio->write(cmpOffset,sz+sizeof(size32_t)*2,dst);
            cmpOffset += sz+sizeof(size32_t)*2;
        }
        bufsize = 0;
    }

    virtual bool attach(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = true;
        bufpos = 0;
        bufsize = 0;

        FcmpCompressedFileTrailer trailer;
        offset_t filesize = baseio->size();
        if (filesize<sizeof(trailer))
            return false;
        baseio->read(filesize-sizeof(trailer),sizeof(trailer),&trailer);
        expSize = trailer.expandedSize;
        return trailer.compressedType==compType;
    }

    virtual void create(IFileIO *_baseio)
    {
        baseio.set(_baseio);
        expOffset = 0;
        cmpOffset = 0;
        reading = false;
        bufpos = 0;
        bufsize = 0;
        ma.allocate(FCMP_BUFFER_SIZE);
        expSize = (offset_t)-1;
    }

    virtual void seek(offset_t pos, IFSmode origin)
    {
        if ((origin==IFScurrent)&&(pos==0))
            return;
        if ((origin==IFSbegin)||(pos!=0))
            throw MakeStringException(-1,"CFcmpStream seek not supported");
        expOffset = 0;
        bufpos = 0;
        bufsize = 0;
    }

    virtual offset_t size()
    {
        return (expSize==(offset_t)-1)?0:expSize;
    }

    virtual offset_t tell()
    {
        return expOffset;
    }

    virtual size32_t read(size32_t len, void * data)
    {
        if (!reading)
            throw MakeStringException(-1,"CFcmpStream read to stream being written");
        size32_t ret=0;
        while (len)
        {
            size32_t cpy = bufsize-bufpos;
            if (!cpy)
            {
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

    virtual size32_t write(size32_t len, const void * data)
    {
        if (reading)
            throw MakeStringException(-1,"CFcmpStream write to stream being read");
        size32_t ret = len;
        while (len+bufsize>FCMP_BUFFER_SIZE)
        {
            size32_t cpy = FCMP_BUFFER_SIZE-bufsize;
            memcpy((byte *)ma.bufferBase()+bufsize,data,cpy);
            data = (const byte *)data+cpy;
            len -= cpy;
            bufsize = FCMP_BUFFER_SIZE;
            save();
        }
        memcpy((byte *)ma.bufferBase()+bufsize,data,len);
        bufsize += len;
        expOffset += len;
        return ret;
    }

    virtual void flush()
    {
        if (!reading&&(expSize!=expOffset))
        {
            save();
            FcmpCompressedFileTrailer trailer;
            memset(&trailer,0,sizeof(trailer));
            trailer.compressedType = compType;
            trailer.expandedSize = expOffset;
            baseio->write(cmpOffset,sizeof(trailer),&trailer);
            expSize = expOffset;
        }
    }

};
