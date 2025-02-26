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
    int hcLevel = LZ4HC_CLEVEL_DEFAULT;
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
        if (newLimit < LZ4_COMPRESSBOUND(inlen) + outlen + sizeof(size32_t))
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
            *cmpsize = LZ4_compress_HC((const char *)inbuf, (char *)out, toflush, LZ4_COMPRESSBOUND(toflush), hcLevel);
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


    size32_t buflen() override
    {
        if (inbuf)
        {
            //calling flushcommitted() would mean everything is serialized as trailing
            size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
            return outlen+sizeof(size32_t)*2+LZ4_COMPRESSBOUND(toflush);
        }
        return outlen;
    }

    virtual bool supportsBlockCompression() const override { return true; }
    virtual bool supportsIncrementalCompression() const override { return false; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        if (destSize <= 3 * sizeof(size32_t))
            return 0;

        //General format for lz4 compressed data is
        //total-uncompressed-size, (compressed size, compressedData)* (trailing-uncompressed size, trailing data)
        //This function will always store 0 as the trailing size.
        size32_t * ptrUnSize = (size32_t *)dest;
        size32_t * ptrCmpSize = ptrUnSize+1;
        byte * remaining = (byte *)(ptrCmpSize+1);
        size32_t remainingSize = destSize - 3 * sizeof(size32_t);
        int compressedSize;
        if (hc)
            compressedSize = LZ4_compress_HC((const char *)src, (char *)remaining, srcSize, remainingSize, hcLevel);
        else
            compressedSize = LZ4_compress_default((const char *)src, (char *)remaining, srcSize, remainingSize);

        if (compressedSize == 0)
            return 0;

        *ptrUnSize = srcSize;
        *ptrCmpSize = compressedSize;

        //Should be improved - currently appends a block of memcpy data.  Unnecessary if all data is
        //compressed, or that is implemented elsewhere.
        *(size32_t *)(remaining + compressedSize) = 0;

        return compressedSize + 3 * sizeof(size32_t);
    }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        dbgassertex(srcSize != 0);
        int compressedSize;
        if (numCompressed)
        {
            //Write as much data as possible into the target buffer - update numCompressed with size actually written
            int numRead = srcSize;
            if (hc)
            {
                MemoryAttr state(LZ4_sizeofStateHC());
                compressedSize = LZ4_compress_HC_destSize(state.mem(), (const char *)src, (char *)dest, &numRead, destSize, hcLevel);
            }
            else
            {
                compressedSize = LZ4_compress_destSize((const char *)src, (char *)dest, &numRead, destSize);
            }
            *numCompressed = numRead;
        }
        else
        {
            if (hc)
                compressedSize = LZ4_compress_HC((const char *)src, (char *)dest, srcSize, destSize, hcLevel);
            else
                compressedSize = LZ4_compress_default((const char *)src, (char *)dest, srcSize, destSize);
        }

        return compressedSize;
    }

    virtual CompressionMethod getCompressionMethod() const override { return hc ? COMPRESS_METHOD_LZ4HC : COMPRESS_METHOD_LZ4; }
public:
    CLZ4Compressor(const char * options, bool _hc) : hc(_hc)
    {
        auto processOption = [this](const char * option, const char * value)
        {
            if (strieq(option, "hclevel"))
                hcLevel = atoi(value);
        };
        processOptionString(options, processOption);
    }
};

//---------------------------------------------------------------------------------------------------------------------

class CLZ4NewCompressor final : public CSimpleInterfaceOf<ICompressor>
{
protected:
    size32_t blksz = 0;
    size32_t bufalloc = 0;
    MemoryBuffer inma;      // equals blksize len
    MemoryBuffer *outBufMb = nullptr; // used when dynamic output buffer (when open() used)
    size32_t outBufStart = 0;
    byte *inbuf = nullptr;
    size32_t inlen = 0;
    size32_t inlenblk = 0;      // set to COMMITTED when so
    bool trailing = false;
    byte *outbuf = nullptr;
    size32_t outlen = 0;
    size32_t wrmax = 0;
    size32_t dynamicOutSz = 0;
    size32_t originalMax = 0;

    void initCommon(size32_t initialSize)
    {
        blksz = initialSize;
        *(size32_t *)outbuf = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        inlenblk = COMMITTED;
    }

    bool hc = false;
    int hcLevel = LZ4HC_CLEVEL_DEFAULT;


protected:
    virtual void open(void *buf,size32_t max) override
    {
        wrmax = max;
        originalMax = max;
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

    virtual void open(MemoryBuffer &mb, size32_t initialSize) override
    {
        throwUnexpected();
        if (!initialSize)
            initialSize = FCMP_BUFFER_SIZE; // 1MB
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

    virtual void close() override
    {
        //Protect against close() being called more than once on a compressor
        if (!inbuf)
        {
            if (isDebugBuild())
                throwUnexpectedX("CFCmpCompressor::close() called more than once");
            return;
        }
        if (inlenblk!=COMMITTED)
        {
            inlen = inlenblk; // transaction failed
            inlenblk = COMMITTED;
        }

        if (flushcommitted(inlen) == inlen)
            inlen = 0;

        //Any remaining data is copied uncompressed.
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

    size32_t write(const void *buf,size32_t len) override
    {
        // no more than wrmax per write (unless dynamically sizing)
        const byte * buffer = (const byte *)buf;
        size32_t written = 0;

        //Keep looping until all data is written and we can guarantee that the remaining data will fit into the
        //buffer uncompressed - so that we can guarantee the data will be written fully.

        while (len)
        {
            size32_t originalLen = inlen;
            size32_t remaining = blksz - inlen;
            size32_t toCopy = remaining>len ? len : remaining;
            //Fill the input buffer with as much data as will fit
            if (toCopy)
            {
                memcpy(inbuf+inlen, buffer, toCopy);
                buffer += toCopy;
                len -= toCopy;
                inlen += toCopy;
            }

            //How much data can be stored uncompressed in the buffer?
            size32_t uncompressedMax = blksz-outlen-sizeof(size32_t)*2;
            if ((inlen == blksz) || (inlen > uncompressedMax))
            {
                size32_t sizeToWrite = inlen;
                size32_t sizeWritten = flushcommitted(originalLen); // Pass originalLen to ensure that is always committed.
                written += (sizeWritten - originalLen);
                inlen = 0; // either all written, or this write will return a partial success - no room left

                //If failed to write a complete block then return data actually written.
                //write() will be called again with the remainder of the data.
                if (sizeWritten != sizeToWrite)
                    return written;
            }
            else
            {
                written += (inlen - originalLen);
                return written;
            }
        }
        return written;
    }

    virtual void * bufptr() override
    {
        assertex(!inbuf);  // i.e. closed
        return outbuf;
    }

    virtual void startblock() override
    {
        inlenblk = inlen;
    }

    virtual void commitblock() override
    {
        inlenblk = COMMITTED;
    }

protected:
    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(bufalloc == 0 && !outBufMb);       // Only supported when a fixed size buffer is provided
        assertex(inlenblk == COMMITTED);            // not inside a transaction
        assertex(newLimit <= originalMax);

        //Reject the limit change if it is too small for the data already committed.
        if (newLimit < LZ4_COMPRESSBOUND(inlen) + outlen + sizeof(size32_t))
            return false;

        blksz = newLimit;
        return true;
    }

    size32_t flushcommitted(size32_t minToWrite)
    {
        // only does non trailing
        size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
        if (toflush == 0)
            return 0;

        size32_t spaceLeft = blksz - outlen - sizeof(size32_t)*2;
        assertex(!dynamicOutSz);

        size32_t *cmpsize = (size32_t *)(outbuf+outlen);
        byte *out = (byte *)(cmpsize+1);

        int outSize;
        int numWritten = toflush;
        if (hc)
        {
            LZ4_streamHC_t * ctx = LZ4_createStreamHC();
            outSize = LZ4_compress_HC_destSize(ctx, (const char *)inbuf, (char *)out, &numWritten, spaceLeft, hcLevel);
            LZ4_freeStreamHC(ctx);
        }
        else
        {
            outSize = LZ4_compress_destSize((const char *)inbuf, (char *)out, &numWritten, spaceLeft);
        }

        //Catch the weird situation, where compressing the data takes up more room, so it the data must be stored uncompressed.
        if ((outSize == 0) || (size32_t)numWritten < minToWrite)
            return 0;

        *cmpsize = outSize;

        //Any data that could not be compressed into the current block is
        *(size32_t *)outbuf += numWritten;
        outlen += outSize+sizeof(size32_t);
        return numWritten;
    }


    size32_t buflen() override
    {
        if (inbuf)
        {
            //calling flushcommitted() would mean everything is serialized as trailing
            size32_t toflush = (inlenblk==COMMITTED)?inlen:inlenblk;
            return outlen+sizeof(size32_t)*2+LZ4_COMPRESSBOUND(toflush);
        }
        return outlen;
    }

    virtual bool supportsBlockCompression() const override { return true; }
    virtual bool supportsIncrementalCompression() const override { return false; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        if (destSize <= 3 * sizeof(size32_t))
            return 0;

        //General format for lz4 compressed data is
        //total-uncompressed-size, (compressed size, compressedData)* (trailing-uncompressed size, trailing data)
        //This function will always store 0 as the trailing size.
        size32_t * ptrUnSize = (size32_t *)dest;
        size32_t * ptrCmpSize = ptrUnSize+1;
        byte * remaining = (byte *)(ptrCmpSize+1);
        size32_t remainingSize = destSize - 3 * sizeof(size32_t);
        int compressedSize;
        if (hc)
            compressedSize = LZ4_compress_HC((const char *)src, (char *)remaining, srcSize, remainingSize, hcLevel);
        else
            compressedSize = LZ4_compress_default((const char *)src, (char *)remaining, srcSize, remainingSize);

        if (compressedSize == 0)
            return 0;

        *ptrUnSize = srcSize;
        *ptrCmpSize = compressedSize;

        //Should be improved - currently appends a block of memcpy data.  Unnecessary if all data is
        //compressed, or that is implemented elsewhere.
        *(size32_t *)(remaining + compressedSize) = 0;

        return compressedSize + 3 * sizeof(size32_t);
    }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        dbgassertex(srcSize != 0);
        int compressedSize;
        if (numCompressed)
        {
            //Write as much data as possible into the target buffer - update numCompressed with size actually written
            int numRead = srcSize;
            if (hc)
            {
                MemoryAttr state(LZ4_sizeofStateHC());
                compressedSize = LZ4_compress_HC_destSize(state.mem(), (const char *)src, (char *)dest, &numRead, destSize, hcLevel);
            }
            else
            {
                compressedSize = LZ4_compress_destSize((const char *)src, (char *)dest, &numRead, destSize);
            }
            *numCompressed = numRead;
        }
        else
        {
            if (hc)
                compressedSize = LZ4_compress_HC((const char *)src, (char *)dest, srcSize, destSize, hcLevel);
            else
                compressedSize = LZ4_compress_default((const char *)src, (char *)dest, srcSize, destSize);
        }

        return compressedSize;
    }

    virtual CompressionMethod getCompressionMethod() const override { return hc ? COMPRESS_METHOD_LZ4HC : COMPRESS_METHOD_LZ4; }
public:
    CLZ4NewCompressor(const char * options, bool _hc) : hc(_hc)
    {
        auto processOption = [this](const char * option, const char * value)
        {
            if (strieq(option, "hclevel"))
                hcLevel = atoi(value);
        };
        processOptionString(options, processOption);
    }
    virtual ~CLZ4NewCompressor()
    {
        if (bufalloc)
            free(outbuf);
    }
};


//---------------------------------------------------------------------------------------------------------------------

class jlib_decl CLZ4Expander : public CFcmpExpander
{
    size32_t totalExpanded = 0;
public:
    virtual void expand(void *buf) override
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

    virtual size32_t expandFirst(MemoryBuffer & target, const void * src) override
    {
        init(src);
        totalExpanded = 0;
        return expandNext(target);
    }

    virtual size32_t expandNext(MemoryBuffer & target) override
    {
        if (totalExpanded == outlen)
            return 0;

        const size32_t szchunk = *in;
        in++;

        target.clear();
        size32_t written;
        if (szchunk+totalExpanded<outlen)
        {
            if (unlikely(szchunk == 0))
            {
                //Special case this corruption - otherwise it enters an infinite loop
                VStringBuffer msg("Unexpected zero length block at block offset %u", (size32_t)((const byte *)in - (const byte *)original));
                throwUnexpectedX(msg.str());
            }

            //All but the last block are compressed (see expand() function above).
            //Slightly concerning there always has to be one trailing byte for this to work!
            size32_t maxOut = target.capacity();
            size32_t maxEstimate = (outlen - totalExpanded);
            size32_t estimate = szchunk; // start conservatively - likely to be preallocated to correct size already.
            if (estimate > maxEstimate)
                estimate = maxEstimate;
            if (maxOut < estimate)
                maxOut = estimate;

            for (;;)
            {
                //Try and decompress into the current target buffer.  If too small increase size and repeat
                written = LZ4_decompress_safe((const char *)in, (char *)target.reserve(maxOut), szchunk, maxOut);
                if ((int)written > 0)
                {
                    target.setLength(written);
                    break;
                }

                //Sanity check to catch corrupt lz4 data that always returns an error.
                if (maxOut > outlen)
                {
                    VStringBuffer msg("Decompression expected max %u bytes, but now %u at block offset %u", outlen, maxOut, (size32_t)((const byte *)in - (const byte *)original));
                    throwUnexpectedX(msg.str());
                }

                maxOut += szchunk; // Likely to quickly approach the actual expanded size
                target.clear();
            }
        }
        else
        {
            void * buf = target.reserve(szchunk);
            written = szchunk;
            memcpy(buf,in,szchunk);
        }

        in = (const size32_t *)(((const byte *)in)+szchunk);
        totalExpanded += written;
        if (totalExpanded > outlen)
            throw MakeStringException(0, "LZ4Expander - corrupt data(3) %d %d",written,szchunk);
        return written;
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        return LZ4_decompress_safe((const char *)src, (char *)dest, srcSize, destSize);
    }

    virtual bool supportsBlockDecompression() const override
    {
        return true;
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


ICompressor *createLZ4Compressor(const char * options, bool hc)
{
    return new CLZ4NewCompressor(options, hc);
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
