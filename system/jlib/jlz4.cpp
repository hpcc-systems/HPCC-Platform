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

class CLZ4NewCompressor final : public CSimpleInterfaceOf<ICompressor>
{
protected:
//output
    size32_t originalMax = 0;   // Used to ensure the limit never exceeds the original buffer size
    size32_t maxOutputSize = 0; // Current maximum length of the output buffer
    MemoryBuffer *outBufMb = nullptr; // used when dynamic output buffer (when open() used)
    byte * outbuf = nullptr;    // pointer to the output buffer
    size32_t outBufStart = 0;   // What offset in the target buffer are we starting to write at (when appending to a MemoryBuffer) - used to update the final length
    size32_t outlen = 0;
    size32_t totalWritten = 0;  // How many bytes written in all blocks
    bool full = false;          // No room for any other data...

//input
    MemoryBuffer inma;          // the buffer that is used to hold the input data
    byte *inbuf = nullptr;      // == inma.buffer()
    size32_t maxInputSize = 0;  // Maximum length of the input buffer
    size32_t inlen = 0;

//open options
    bool allowPartialWrites{true};

    void initCommon(size32_t initialSize)
    {
        maxInputSize = initialSize;
        inbuf = (byte *)inma.ensureCapacity(maxInputSize);
        totalWritten = 0;
        outlen = sizeof(size32_t);
        inlen = 0;
        full = false;
    }

    bool hc = false;
    int hcLevel = LZ4HC_CLEVEL_DEFAULT;

protected:
    virtual void open(void *buf,size32_t max, size32_t fixedRowSize, bool _allowPartialWrites) override
    {
        assertex(buf && max);
        originalMax = max;
        maxOutputSize = max;
        allowPartialWrites = _allowPartialWrites;
        outBufStart = 0;
        outBufMb = nullptr;
        outbuf = (byte *)buf;
        initCommon(max);
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize) override
    {
        if (!initialSize)
            initialSize = FCMP_BUFFER_SIZE; // 1MB

        originalMax = 0;
        maxOutputSize = initialSize;
        allowPartialWrites = false;
        outBufStart = mb.length();
        outBufMb = &mb;
        outbuf = (byte *)outBufMb->ensureCapacity(initialSize);
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

        // If writes all fit within the buffer, should we compress the data?
        // This should possibly be conditional on whether to compress if there is space in a fixed size buffer
        if (!full)
            (void)flushCompress(0);    // either returns 0 if it cannot compress, or inlen if it did.

        //Any remaining data is copied uncompressed.
        size32_t totlen = outlen+sizeof(size32_t)+inlen;
        assertex(maxOutputSize>=totlen);
        size32_t *tsize = (size32_t *)(outbuf+outlen);
        *tsize = inlen;
        memcpy(tsize+1,inbuf,inlen);
        outlen = totlen;
        *(size32_t *)outbuf = totalWritten +inlen;
        inbuf = NULL;
        if (outBufMb)
        {
            outBufMb->setWritePos(outBufStart+outlen);
            outBufMb = NULL;
        }
    }

    size32_t write(const void *buf,size32_t len) override
    {
        if (unlikely(full))
            return 0;

        // no more than wrmax per write (unless dynamically sizing)
        const byte * buffer = (const byte *)buf;
        size32_t written = 0;

        //Keep looping until all data is written and we can guarantee that the remaining data will fit into the
        //buffer uncompressed - so that we can guarantee the data will be written fully.

        while (len)
        {
            size32_t remaining = maxInputSize - inlen;
            size32_t toCopy = remaining>len ? len : remaining;

            //Fill the input buffer with as much data as will fit in the input buffer, but do not update inlen
            if (toCopy)
                memcpy(inbuf+inlen, buffer, toCopy);

            //How much data can be stored uncompressed in the buffer?
            size32_t uncompressedMax = maxOutputSize-outlen-sizeof(size32_t);
            size32_t nextlen = inlen+toCopy;
            if ((nextlen == maxInputSize) || (nextlen >= uncompressedMax))
            {
                size32_t extraWritten = flushCompress(toCopy); // Pass originalLen to ensure that is always committed.
                written += extraWritten;

                assertex(extraWritten <= toCopy);

                //If failed to write a complete block then return data actually written.
                //write() will be called again with the remainder of the data.
                if (extraWritten != toCopy)
                {
                    if (allowPartialWrites || written == 0)
                       return written;

                    // Disallow partial writes, but a block of data including part of the row has already been compressed
                    // A full solution would require the previous block to be decompressed and inlen restored.
                    // For now, adjust the total size, but do not undo the compression.
                    // This may cause problems if it was used in inplace:zstd compression and the available payload area increased.
                    // (Similar problems occur with inplace:lzw)
                    assertex(inlen == 0);
                    totalWritten -= written;
                    return 0;
                }
            }
            else
            {
                inlen += toCopy;
                written += toCopy;
                return written;
            }

            buffer += toCopy;
            len -= toCopy;
        }
        return written;
    }

    virtual void * bufptr() override
    {
        assertex(!inbuf);  // i.e. closed
        return outbuf;
    }

protected:
    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(!outBufMb);       // Only supported when a fixed size buffer is provided
        assertex(newLimit <= originalMax);

        //Reject the limit change if it is too small for the data already committed.
        size32_t reservedSpace = outlen + sizeof(size32_t) * 2;
        if (newLimit < reservedSpace)
            return false;

        maxOutputSize = newLimit;
        return true;
    }

    //Try and compress inlen + extra bytes of data - inlen is guaranteed to fit uncompressed.
    //if the data is successfully compressed then inlen is updated
    size32_t flushCompress(size32_t extra)
    {
        size32_t toCompress = inlen+extra;
        if (toCompress == 0)
            return 0;

        // Loop to allow for dynamic output buffer to be resized.
        size32_t numWritten = 0;
        size32_t outSize;

        //Space required is existing data + length for compressed data + length uncompressed data
        size32_t reservedSpace = outlen + sizeof(size32_t) * 2;
        for (;;)
        {
            if (likely(reservedSpace < maxOutputSize))
            {
                size32_t spaceLeft = maxOutputSize - reservedSpace;
                byte * out = outbuf + outlen + sizeof(size32_t);

                outSize = compressDirect(spaceLeft, out, toCompress, inbuf, &numWritten);
                assertex(outSize != (size32_t)-1); // Should have been rejected by adjustLimit
                assertex(numWritten != (size32_t)-1); // Should have been rejected by adjustLimit

                // compressed data fits - all good.
                if (numWritten == toCompress)
                    break;

                if (!outBufMb)
                {
                    full = true;

                    if (!allowPartialWrites)
                        return 0;

                    //The data was larger after compression - keep as much data as will fit uncompressed..
                    //Includes the case where outSize == 0
                    if (outSize >= numWritten)
                    {
                        spaceLeft += sizeof(size32_t);  //Can squeeze in 4 more bytes because there is no compressed size
                        assertex(spaceLeft >= inlen);
                        size_t delta = spaceLeft - inlen;
                        inlen = spaceLeft;
                        return delta;
                    }

                    // Fit as much compressed data as possible
                    break;
                }
            }
            else if (!outBufMb)
            {
                full = true;
                return 0;
            }

            //Increase the available space by 50% of the input size
            size32_t newSize = maxOutputSize + maxInputSize / 2;
            outbuf = (byte *)outBufMb->ensureCapacity(newSize);
            maxOutputSize = newSize;

            //Loop and try again...
        }

        //If we are compressing the last block of data, and the compressed size is larger than the uncompressed size
        //then do not compres it - otherwise the expander will fail.
        //There really should be a flag on the compress blocks to indicate it is the last block.
        if ((extra == 0) && (outSize >= numWritten))
            return 0;

        // Prefix the compressed data with the compressed size
        size32_t *cmpsize = (size32_t *)(outbuf+outlen);
        *cmpsize = outSize;

        //Any data that could not be compressed into the current block is
        totalWritten += numWritten;
        outlen += outSize+sizeof(size32_t);
        size_t delta = numWritten - inlen;
        inlen = 0;
        return delta;
    }


    size32_t buflen() override
    {
        if (inbuf)
        {
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
    CLZ4NewCompressor(const char * options, bool _hc) : hc(_hc)
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

//---------------------------------------------------------------------------------------------------------------------

class CLZ4Expander final : public CFcmpExpander
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
