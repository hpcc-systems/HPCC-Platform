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
#include "jlog.hpp"
#include "jlzbase.hpp"

/* Format:
    size32_t totalexpsize;
    { size32_t subcmpsize; bytes subcmpdata; }
    size32_t trailsize; bytes traildata;    // unexpanded
*/

//--------------------------------------------------------------------------------------------------------------------
// Non Streaming implementation of LZ4 compression
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
        assertex(!outBufMb);       // Only supported when a fixed size buffer is provided
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
        size32_t toflush = inlen;
        if (toflush == 0)
            return;

        if (toflush > LZ4_MAX_INPUT_SIZE)
            throw makeStringExceptionV(-1, "LZ4Compressor::flushcommitted - input size %u exceeds maximum %d", toflush, LZ4_MAX_INPUT_SIZE);

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
            inlen = 0;
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
            size32_t toflush = inlen;
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
            if (strieq(option, "hclevel") || strieq(option, "level"))
                hcLevel = atoi(value);
        };
        processOptionString(options, processOption);
    }
};

//---------------------------------------------------------------------------------------------------------------------

/*

This compression works as follows:
- The compressor has a fixed size block that matches the output block size.
- Data is appended to tbe block until it can no longer fit in the the buffer
-   Try and compress the data.
-   If it fits
-     good.
-   else if allow partial compression, and smaller than uncompressed
-     keep as much as possible
-   else
-     unwind the last write.
*/

class CLZ4NewCompressor final : public CBlockCompressor
{
public:
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

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        return LZ4_decompress_safe((const char *)src, (char *)dest, srcSize, destSize);
    }

public:
    CLZ4NewCompressor(const char * options, bool _hc)
    : CBlockCompressor(true), hc(_hc)
    {
        auto processOption = [this](const char * option, const char * value)
        {
            if (strieq(option, "hclevel"))
                hcLevel = atoi(value);
        };
        processOptionString(options, processOption);
    }

protected:
    bool hc = false;
    int hcLevel = LZ4HC_CLEVEL_DEFAULT;
};



//---------------------------------------------------------------------------------------------------------------------

class CLZ4Expander final : public CBlockExpander
{
public:
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


//---------------------------------------------------------------------------------------------------------------------
// Streaming implementation of LZ4 compression
//
// See notes on CStreamCompressor in jlzbase.hpp for the serialized stream format
class CLZ4StreamCompressor final : public CStreamCompressor
{
public:
    CLZ4StreamCompressor(const char * options, bool _hc) : hc(_hc)
    {
        auto processOption = [this](const char * option, const char * textValue)
        {
            this->processOption(option, textValue);
        };
        processOptionString(options, processOption);
        if (hc)
            lz4HCStream = LZ4_createStreamHC();
        else
            lz4Stream = LZ4_createStream();
    }

    virtual ~CLZ4StreamCompressor()
    {
        LZ4_freeStream(lz4Stream);
        LZ4_freeStreamHC(lz4HCStream);
    }

    virtual void open(void *buf,size32_t max, size32_t fixedRowSize, bool _allowPartialWrites) override
    {
        CStreamCompressor::open(buf, max, fixedRowSize, _allowPartialWrites);
        if (hc)
            LZ4_resetStreamHC_fast(lz4HCStream, compressionLevel);
        else
            LZ4_resetStream_fast(lz4Stream);
    }

    virtual CompressionMethod getCompressionMethod() const override { return hc ? COMPRESS_METHOD_LZ4SHC : COMPRESS_METHOD_LZ4S; }

protected:
    bool processOption(const char * option, const char * textValue)
    {
        if (CStreamCompressor::processOption(option, textValue))
            return true;
        int intValue = atoi(textValue);
        if (strieq(option, "hclevel") || strieq(option, "level"))
        {
            if ((intValue >= LZ4HC_CLEVEL_MIN) && (intValue <= LZ4HC_CLEVEL_MAX))
                compressionLevel = intValue;
        }
        else
            return false;
        return true;
    };

    virtual void resetStreamContext() override
    {
        if (hc)
            LZ4_resetStreamHC_fast(lz4HCStream, compressionLevel);
        else
            LZ4_resetStream_fast(lz4Stream);
    }

    virtual bool tryCompress(bool isFinalCompression [[maybe_unused]]) override
    {
        size32_t uncompressed = inlen - lastCompress;
        //Sanity check - this could be the case when a limit has been reduced
        if (uncompressed < minSizeToCompress)
            return false;

        size32_t compressedSize = outlen;
        size32_t remaining = outMax - compressedSize - outputExtra;

        const char * from = (const char *)inbuf + lastCompress;
        char * to = (char *)queryOutputBuffer() + outlen;
        int newCompressedSize;
        if (hc)
            newCompressedSize = LZ4_compress_HC_continue(lz4HCStream, from, to, uncompressed, remaining);
        else
            newCompressedSize = LZ4_compress_fast_continue(lz4Stream, from, to, uncompressed, remaining, 1);

#ifdef LZ4LOGGING
        DBGLOG("Compress limit(%u,%u) compressed(%u:0..%u), uncompressed(%u:%u..%u)->%u total(%u)", outMax, remaining, outlen, lastCompress, uncompressed, lastCompress, inlen, newCompressedSize, outlen + 8 + inlen - lastCompress);
#endif

        if (newCompressedSize == 0)
            return false;

        size32_t lenLen = sizePacked(newCompressedSize);
        if (newCompressedSize + lenLen > remaining)
            return false;

        compressedSizes.append(newCompressedSize);
        outputExtra += lenLen;
        outlen += newCompressedSize;
        lastCompress = inlen;
        return true;
    }

protected:
    LZ4_stream_t * lz4Stream = nullptr;
    LZ4_streamHC_t * lz4HCStream = nullptr;

    //Options for configuring the compressor:
    bool hc = false;
    byte compressionLevel = LZ4HC_CLEVEL_DEFAULT;
};


//---------------------------------------------------------------------------------------------------------------------

class CLZ4StreamExpander final : public CStreamExpander
{
public:
    CLZ4StreamExpander()
    {
        lz4Stream = LZ4_createStreamDecode();
    }

    ~CLZ4StreamExpander()
    {
        LZ4_freeStreamDecode(lz4Stream);
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        return LZ4_decompress_safe((const char *)src, (char *)dest, srcSize, destSize);
    }

protected:
    virtual void resetStreamContext() override
    {
        LZ4_setStreamDecode(lz4Stream, nullptr, 0);
    }

    virtual int decodeStreamBlock(const void * src, size32_t srcSize, void * dest, size32_t destSize) override
    {
        return LZ4_decompress_safe_continue(lz4Stream, (const char *)src, (char *)dest, srcSize, destSize);

    }

    virtual void *bufptr() { return nullptr;}
    virtual size32_t buflen() { return outlen;}

protected:
    LZ4_streamDecode_t * lz4Stream = nullptr;
};

ICompressor *createLZ4Compressor(const char * options, bool hc)
{
    return new CLZ4NewCompressor(options, hc);
}

IExpander *createLZ4Expander()
{
    return new CLZ4Expander;
}

ICompressor *createLZ4StreamCompressor(const char * options, bool hc)
{
    return new CLZ4StreamCompressor(options, hc);
}

IExpander *createLZ4StreamExpander()
{
    return new CLZ4StreamExpander();
}
