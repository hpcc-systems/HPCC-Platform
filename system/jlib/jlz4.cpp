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

//Enable the following to debug details of the stream compression
//#define LZ4LOGGING


//Helper functions for storing "packed" numbers, where the top bit indicates the number is continued in the next byte
static unsigned sizePacked(size32_t value)
{
    unsigned size = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        size++;
    }
    return size;
}

static void writePacked(byte * out, size32_t & offset, unsigned __int64  value)
{
    constexpr unsigned maxBytes = 9;
    unsigned index = maxBytes;
    byte result[maxBytes];

    result[--index] = value & 0x7f;
    while (value >= 0x80)
    {
        value >>= 7;
        result[--index] = (value & 0x7f) | 0x80;
    }

    size32_t len = maxBytes - index;
    memcpy(out+offset, result + index, len);
    offset += len;
}

static unsigned readPacked32(const byte * & cur)
{
    byte next = *cur++;
    unsigned value = next;
    if (unlikely(next >= 0x80))
    {
        value = value & 0x7f;
        do
        {
            next = *cur++;
            value = (value << 7) | (next & 0x7f);
        } while (next & 0x80);
    }
    return value;
}



/*
 * The serialized stream compressed data has the following format:
 *
 * 4-bytes - length of the uncompressed data
 * List of packed lengths of the compressed blocks of data, followed by a zero (=packed int 0)
 * Blocks of compressed data concatenated together
 *    packedlength - length of the compressed data
 *    data         - block of compressed data
 * 0 byte (i.e. zero packed length)
 * <fill data> - uncompressed length - length already expanded
 *
 * The compressor builds up the compressed data into a pair of output buffers
 * When recompressing the other buffer is used as the target, so that the previous
 * compression is preserved in the unusual situation where recompressing makes it larger.
*/
class CLZ4StreamCompressor final : public CSimpleInterfaceOf<ICompressor>
{
public:
    CLZ4StreamCompressor(const char * options, bool _hc) : hc(_hc)
    {
        auto processOption = [this](const char * option, const char * textValue)
        {
            int intValue = atoi(textValue);
            if (strieq(option, "hclevel"))
            {
                if ((intValue >= LZ4HC_CLEVEL_MIN) && (intValue <= LZ4HC_CLEVEL_MAX))
                    hcLevel = intValue;
            }
            else if (strieq(option, "maxcompression"))
            {
                if ((intValue > 1) && (intValue < 100))
                    maxCompression = intValue;
            }
            else if (strieq(option, "maxRecompress"))
            {
                if ((intValue >= 0) && (intValue < 100))
                    maxRecompress = intValue;
            }
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

    virtual void open(void *buf,size32_t max) override
    {
        result = (byte *)buf;
        outMax = max;
        originalMax = max;
        outputs[0].ensureCapacity(max);
        outputs[1].ensureCapacity(max);

        outputExtra = sizeof(size32_t) + 1;
        lastCompress = 0;
        inMax = max*maxCompression;
        inbuf = (byte *)inma.ensureCapacity(inMax);

        outlen = 0;
        inlen = 0;
        lastCompress = 0;
        active = 0;
        recompressed = 0;
        compressedSizes.clear();
        if (hc)
            LZ4_resetStreamHC_fast(lz4HCStream, hcLevel);
        else
            LZ4_resetStream_fast(lz4Stream);
    }

    virtual void open(MemoryBuffer &mb, size32_t initialSize) override
    {
        throw makeStringException(99, "CLZ4StreamCompressor does not support MemoryBuffer output");
    }

    virtual void close() override
    {
        //Protect against close() being called more than once on a compressor
        if (!inbuf)
        {
            if (isDebugBuild())
                throwUnexpectedX("CLZ4StreamCompressor::close() called more than once");
            return;
        }

        //At this point outbuf contains the compressed blocks.  We now need to build up the final output format
        //This either goes into result (if provided), otherwise the other output buffer
        size32_t uncompressed = inlen - lastCompress;
        assertex(outputExtra + outlen + uncompressed <= outMax);
        size32_t targetLen = 0;

        //Either build up the result in the buffer provided to open, or the spare output buffer
        byte * target = result;
        if (!target)
            target = (byte *)outputs[1-active].bufferBase();

        //Head of the data is a 4 byte uncompressed total length
        *(size32_t *)target = inlen;
        targetLen += sizeof(size32_t);

        //Then a list of sizes of each of the compressed blocks
        for (unsigned i=0; i < compressedSizes.ordinality(); i++)
        {
            size32_t compressedSize = compressedSizes.item(i);
            writePacked(target, targetLen, compressedSize);
        }

        // zero byte marks the end of the packed lengths
        writePacked(target, targetLen, 0);
        assertex(targetLen == outputExtra); // sanity check

        //Append the compressed data
        memcpy(target + targetLen, queryOutputBuffer(), outlen);
        targetLen += outlen;

        //Then any trailing uncompressed data
        memcpy(target + targetLen, inbuf + lastCompress, uncompressed);
        targetLen += uncompressed;

        if (targetLen > outMax)
            throw makeStringExceptionV(99, "Total size too large uncompressed(%u) = %u > %u", uncompressed, outlen, outMax);

        active = 1-active; // The newly compressed data should be returned from bufptr()
        outlen = targetLen;
        inbuf = NULL;
    }

    virtual size32_t write(const void *buf,size32_t len) override
    {
        if (len + inlen > inMax)
        {
#ifdef LZ4LOGGING
            DBGLOG("Input data too large for expected compression %u+%u>%u", len, inlen, inMax);
#endif
            return 0;
        }

        //Save all the input text - LZ4 needs all the data, and we also periodically recompress all the data
        memcpy(inbuf+inlen, buf, len);
        inlen += len;

        size32_t compressedSize = outlen;
        size32_t remaining = outMax - compressedSize - outputExtra;
        size32_t uncompressed = inlen - lastCompress;
        //If there is enough room to store the new data uncompressed then succeed and continue building up the input.
        if (uncompressed <= remaining)
            return len;

        if (tryFullCompress())
            return len;

        inlen -= len;
        return 0;
    }

    virtual void * bufptr() override
    {
        assertex(!inbuf);  // i.e. closed
        return result ? result : queryOutputBuffer();
    }

    virtual void startblock() override
    {
        //This class either accepts or rejects each write - i.e. all writes are blocked, larger blocking is not supported
    }

    virtual void commitblock() override
    {
    }

    virtual bool adjustLimit(size32_t newLimit) override
    {
        assertex(newLimit <= originalMax); // sanity check

        size32_t curlen = buflen();

#ifdef LZ4LOGGING
        DBGLOG("Compress limit(%u->%u) compressed(%u:0..%u), uncompressed(%u:%u..%u) total(%u)", outMax, newLimit, outlen, lastCompress, inlen - lastCompress, lastCompress, inlen, outlen + 8 + inlen - lastCompress);
#endif

        //Reducing the limit means that the current data will not fit - but there may be uncompressed data that can be compressed so that it does.
        if (curlen >= newLimit)
        {
            if (!tryFullCompress())
            {
#ifdef LZ4LOGGING
                DBGLOG("Failed to recompress limit(%u->%u) compressed(%u:0..%u), uncompressed(%u:%u..%u) total(%u)", outMax, newLimit, outlen, lastCompress, inlen - lastCompress, lastCompress, inlen, outlen + 8 + inlen - lastCompress);
#endif
                return false;
            }

            //The data compressed - check if there is now scope for reducing the limit.
            curlen = buflen();
            if (curlen >= newLimit)
            {
#ifdef LZ4LOGGING
                DBGLOG("No space after recompress limit(%u->%u) compressed(%u:0..%u), uncompressed(%u:%u..%u) total(%u)", outMax, newLimit, outlen, lastCompress, inlen - lastCompress, lastCompress, inlen, outlen + 8 + inlen - lastCompress);
#endif
                return false;
            }
        }

        outMax = newLimit;
        //NOTE: We could adjust inmax to prevent the compression ratio exceeding maxCompression.
        //Leave as it is for the moment, because expansion relative to the buffer size is most
        //important for limiting the size when expanded.
        return true;
    }

    virtual size32_t buflen() override
    {
        if (inbuf)
            return outputExtra+outlen+(inlen-lastCompress);
        return outlen;
    }

    virtual bool supportsBlockCompression() const override { return false; }
    virtual bool supportsIncrementalCompression() const override { return true; }

    virtual size32_t compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        return 0;
    }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        throwUnimplemented();
    }

    virtual CompressionMethod getCompressionMethod() const override { return hc ? COMPRESS_METHOD_LZ4SHC : COMPRESS_METHOD_LZ4S; }

protected:
    byte * queryOutputBuffer() { return (byte *)outputs[active].bufferBase(); }

    //Try and compress the uncompressed data using the stream functions.  If that does not succeed try
    //recompressing all the data that has been written so far to see if that shrinks.
    bool tryFullCompress()
    {
        if (tryCompress())
            return true;

        //No benefit in trying to recompress if there is no more data to compress, and we already have a single block
        if ((inlen == lastCompress) && (compressedSizes.ordinality() == 1))
            return false;

        if (recompressed < maxRecompress)
        {
#ifdef LZ4LOGGING
            DBGLOG("RECOMPRESS entire data stream");
#endif
            //Try and recompress the entire data stream
            recompressed++;
            if (hc)
                LZ4_resetStreamHC_fast(lz4HCStream, hcLevel);
            else
                LZ4_resetStream_fast(lz4Stream);

            size32_t savedOutlen = outlen;
            size32_t savedOutputExtra = outputExtra;
            size32_t savedLastCompress = lastCompress;

            //Compress to the other output buffer - so that the streamed compression is still available if
            //compressing everything takes up more room.
            active = 1-active;
            outlen = 0;
            outputExtra = sizeof(size32_t) + 1;
            lastCompress = 0;

            if (tryCompress())
            {
                //Now only a single compressed block
                compressedSizes.clear();
                compressedSizes.append(outlen);
                return true;
            }

            //Unusual situation - the size when compressed in a single call is larger then
            //when compressed as streams, possibly because there is extra uncompressed data.
            //Restore the previous state - use the previous stream compressed blocks
            active = 1-active;
            outlen = savedOutlen;
            outputExtra = savedOutputExtra;
            lastCompress = savedLastCompress;
        }

        return false;
    }

    bool tryCompress()
    {
        size32_t uncompressed = inlen - lastCompress;
        //Sanity check - this could be the case when a limit has been reduced
        if (uncompressed == 0)
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
    size32_t originalMax = 0;
    size32_t outMax = 0;    // maximum output size
    size32_t inMax = 0;     // maximum size of the input buffer - to limit the compression ratio
                            // and because the input buffer cannot be reallocated with the streaming api
    size32_t inlen = 0;     // total length of input data
    size32_t lastCompress = 0;  // the offset in the input data that has not been compressed yet.
    size32_t outlen = 0;    // How much compressed data has been generated, or final size once closed
    MemoryBuffer outputs[2];
    MemoryBuffer inma;      // Buffer allocated to store a copy of all the input data - must not be reallocated
    byte *inbuf = nullptr;  // pointer to the input buffer, cleared when the compression is complete
    byte *result = nullptr;
    byte recompressed = 0;  // Number of times we have recompressed the entire buffer
    byte active = 0;        // Which output buffer is active?

    UnsignedArray compressedSizes;
    size32_t outputExtra = 0;

    LZ4_stream_t * lz4Stream = nullptr;
    LZ4_streamHC_t * lz4HCStream = nullptr;

    //Options for configuring the compressor:
    bool hc = false;
    byte hcLevel = LZ4HC_CLEVEL_DEFAULT;
    byte maxCompression = 20;   // Avoid compressing more than 20x because allocating when expanding is painful.
    byte maxRecompress = 1;     // How many times should the code try and recompress all the smaller streams as one?
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

//---------------------------------------------------------------------------------------------------------------------
class jlib_decl CLZ4StreamExpander : public CExpanderBase
{
public:
    virtual void expand(void *buf) override
    {
        assertex(buf);
        if (!outlen)
            return;

        const byte * src = in;
        //Skip until we have a zero size - don't bother decoding
        //Note, a zero byte may be part of a packed size, so cannot just search for 0 bytes
        for (;;)
        {
            byte next = *src++;
            if (next == 0)
                break;
            while (next & 0x80)
                next = *src++;
        }

        LZ4_streamDecode_t * lz4Stream = LZ4_createStreamDecode();

        char * out = (char *)buf;
        const byte * sizes = in;
        size32_t expandedOffset = 0;
        for (;;)
        {
            size32_t srcSize = readPacked32(sizes);
            if (srcSize == 0)
                break;
            int expanded = LZ4_decompress_safe_continue (lz4Stream, (const char *)src, out+expandedOffset, srcSize, outlen - expandedOffset);
            if (expanded <= 0)
            {
                LZ4_freeStreamDecode(lz4Stream);
                throw MakeStringException(MSGAUD_operator,0, "Failed to expand offset(%u) size(%u) to %u - returned %d", (size32_t)(src - in), srcSize, outlen, expanded);
            }
            src += srcSize;
            expandedOffset += expanded;
        }

        //finally fill with any uncompressed data
        memcpy(out+expandedOffset, src, outlen - expandedOffset);
        LZ4_freeStreamDecode(lz4Stream);
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        return LZ4_decompress_safe((const char *)src, (char *)dest, srcSize, destSize);
    }

    virtual size32_t init(const void *blk)
    {
        const size32_t *expsz = (const size32_t *)blk;
        outlen = *expsz;
        in = (const byte *)(expsz+1);
        return outlen;
    }

    virtual bool supportsBlockDecompression() const override
    {
        return false;
    }

protected:
    virtual void *bufptr() { return nullptr;}
    virtual size32_t buflen() { return outlen;}

protected:
    const byte *in = nullptr;
    size32_t outlen = 0;
};

//---------------------------------------------------------------------------------------------------------------------

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
    return new CLZ4Compressor(options, hc);
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
