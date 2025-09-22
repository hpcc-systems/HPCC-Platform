/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "jlog.hpp"
#include "jlzbase.hpp"

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

void CBlockCompressor::initCommon(size32_t initialSize)
{
    maxInputSize = initialSize;
    inbuf = (byte *)inma.ensureCapacity(maxInputSize);
    totalWritten = 0;
    outlen = sizeof(size32_t);
    inlen = 0;
    full = false;
}

void CBlockCompressor::open(void *buf,size32_t max, size32_t fixedRowSize, bool _allowPartialWrites)
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

void CBlockCompressor::open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize)
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

void CBlockCompressor::close()
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

size32_t CBlockCompressor::write(const void *buf,size32_t len)
{
    if (unlikely(full))
        return 0;

    // no more than wrmax per write (unless dynamically sizing)
    const byte * buffer = (const byte *)buf;
    size32_t written = 0;
    size32_t savedOutlen = outlen;
    size32_t savedInlen = inlen;
    size32_t savedTotalWritten = totalWritten;

    //Keep looping until all data is written and we can guarantee that the remaining data will fit into the
    //buffer uncompressed - so that we can guarantee the data will be written fully.

    while (len)
    {
        size32_t uncompressedMax = maxOutputSize-outlen-sizeof(size32_t);
        size32_t maxToCompress = maxInputSize;
        //If the compressor allows us to compress as much as possible, then there is no penalty for compressing the
        //maximum input size each time.
        //Otherwise only compress as much as we expect to be able to compress
        if (!supportsPartialCompression)
        {
            //Should this be adaptive based on the compression ratio so far?  Initial investigation suggests not.
            size32_t bestEstimate = uncompressedMax;
            if (bestEstimate < maxToCompress)
                maxToCompress = bestEstimate;
        }

        assertex(inlen <= maxToCompress);
        size32_t remaining = maxToCompress - inlen;
        size32_t toCopy = remaining>len ? len : remaining;
        if (toCopy == 0)
            break;

        //Fill the input buffer with as much data as will fit in the input buffer, but do not update inlen
        if (toCopy)
            memcpy(inbuf+inlen, buffer, toCopy);

        //How much data can be stored uncompressed in the buffer?
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
                // We need to undo the first compression to restore the previous good data.
                byte * prevout = outbuf + savedOutlen;
                size32_t compressedSize = *(size32_t *)prevout;

                //Only need to expand the first block that was compressed by this write call
                size32_t expanded = expandDirect(maxInputSize, inbuf, compressedSize, prevout + sizeof(size32_t));
                assertex(expanded >= savedInlen);

                inlen = savedInlen;
                outlen = savedOutlen;
                totalWritten = savedTotalWritten;      // Restore - could possibly be multiple compress blocks.
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

bool CBlockCompressor::adjustLimit(size32_t newLimit)
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
size32_t CBlockCompressor::flushCompress(size32_t extra)
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

size32_t CBlockCompressor::compressBlock(size32_t destSize, void * dest, size32_t srcSize, const void * src)
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

    int compressedSize = compressDirect(remainingSize, remaining, srcSize, src, nullptr);
    if (compressedSize == 0)
        return 0;

    *ptrUnSize = srcSize;
    *ptrCmpSize = compressedSize;

    //Should be improved - currently appends a block of memcpy data.  Unnecessary if all data is
    //compressed, or that is implemented elsewhere.
    *(size32_t *)(remaining + compressedSize) = 0;

    return compressedSize + 3 * sizeof(size32_t);
}

bool CBlockCompressor::processOption(const char * option, const char * textValue)
{
    return false;
}

//---------------------------------------------------------------------------------------------------------------------

void CBlockExpander::expand(void *buf)
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
            throw makeStringExceptionV(MSGAUD_operator, 0, "Out of memory in BlockExpander::expand, requesting %u bytes", bufalloc);
    }

    size32_t done = 0;
    for (;;)
    {
        const size32_t szchunk = *in;
        in++;
        if (szchunk+done<outlen)
        {
            size32_t written = expandDirect(outlen - done, outbuf + done, szchunk, in);
            done += written;
            if (!written||(done>outlen))
                throw makeStringExceptionV(0, "BlockExpander - corrupt data(1) %u %u",written,szchunk);
        }
        else
        {
            if (szchunk+done!=outlen)
                throw makeStringExceptionV(0, "BlockExpander - corrupt data(2) %u %u",szchunk,outlen);
            memcpy(outbuf+done,in,szchunk);
            break;
        }
        in = (const size32_t *)(((const byte *)in)+szchunk);
    }
}

size32_t CBlockExpander::expandFirst(MemoryBuffer & target, const void * src)
{
    init(src);
    totalExpanded = 0;
    return expandNext(target);
}

size32_t CBlockExpander::expandNext(MemoryBuffer & target)
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
            written = expandDirect(maxOut, target.reserve(maxOut), szchunk, in);
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
        throw makeStringExceptionV(0, "BlockExpander - corrupt data(3) %u %u",written,szchunk);
    return written;
}

//---------------------------------------------------------------------------------------------------------------------

//Enable the following to debug details of the stream compression
//#define LZ4LOGGING

//Helper functions for storing "packed" numbers, where the top bit indicates the number is continued in the next byte
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
void CStreamCompressor::open(void *buf, size32_t max, size32_t fixedRowSize, bool _allowPartialWrites)
{
    assertex(buf);
    result = (byte *)buf;
    outMax = max;
    originalMax = max;
    allowPartialWrites = _allowPartialWrites;
    outputs[0].ensureCapacity(max);
    outputs[1].ensureCapacity(max);

    inMax = max*maxCompression;
    inbuf = (byte *)inma.ensureCapacity(inMax);

    reinitBufferIndexes(0);

    active = 0;
    recompressed = 0;
    compressedSizes.clear();
}

void CStreamCompressor::open(MemoryBuffer &mb, size32_t initialSize, size32_t fixedRowSize)
{
    throw makeStringException(99, "CLZ4StreamCompressor does not support MemoryBuffer output");
}

void CStreamCompressor::close()
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

size32_t CStreamCompressor::write(const void *buf,size32_t len)
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

void * CStreamCompressor::bufptr()
{
    assertex(!inbuf);  // i.e. closed
    return result ? result : queryOutputBuffer();
}

bool CStreamCompressor::adjustLimit(size32_t newLimit)
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

size32_t CStreamCompressor::buflen()
{
    if (inbuf)
        return outputExtra+outlen+(inlen-lastCompress);
    return outlen;
}



bool CStreamCompressor::processOption(const char * option, const char * textValue)
{
    int intValue = atoi(textValue);
    if (strieq(option, "maxcompression"))
    {
        if ((intValue > 1) && (intValue < 100))
            maxCompression = intValue;
    }
    else if (strieq(option, "maxRecompress"))
    {
        if ((intValue >= 0) && (intValue < 100))
            maxRecompress = intValue;
    }
    else if (strieq(option, "minSizeToCompress"))
    {
        if (intValue > 0)
            minSizeToCompress = intValue;
    }
    else
        return false;
    return true;
};

void CStreamCompressor::reinitBufferIndexes(size32_t newInLen)
{
    outlen = 0;
    outputExtra = sizeof(size32_t) + 1;
    inlen = newInLen;
    lastCompress = 0;
}

//Try and compress the uncompressed data using the stream functions.  If that does not succeed try
//recompressing all the data that has been written so far to see if that shrinks.
bool CStreamCompressor::tryFullCompress()
{
    if (tryCompress(false))
        return true;

    //No benefit in trying to recompress if there is no more data to compress, and we already have a single block
    if ((inlen < lastCompress + minSizeToCompress) && (compressedSizes.ordinality() == 1))
        return false;

    if (recompressed < maxRecompress)
    {
#ifdef LZ4LOGGING
        DBGLOG("RECOMPRESS entire data stream");
#endif
        //Try and recompress the entire data stream
        recompressed++;
        if (recompressInput(recompressed == maxRecompress))
            return true;
    }

    return false;
}

bool CStreamCompressor::recompressInput(bool isFinalCompression)
{
    resetStreamContext();

    size32_t savedOutlen = outlen;
    size32_t savedOutputExtra = outputExtra;
    size32_t savedLastCompress = lastCompress;

    //Compress to the other output buffer - so that the streamed compression is still available if
    //compressing everything takes up more room.
    active = 1-active;

    reinitBufferIndexes(inlen);

    if (tryCompress(isFinalCompression))
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
    return false;
}

//---------------------------------------------------------------------------------------------------------------------

void CStreamExpander::expand(void *buf)
{
    assertex(buf);
    if (!outlen)
        return;

    resetStreamContext();

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


    char * out = (char *)buf;
    const byte * sizes = in;
    size32_t expandedOffset = 0;
    for (;;)
    {
        size32_t srcSize = readPacked32(sizes);
        if (srcSize == 0)
            break;
        int expanded = decodeStreamBlock(src, srcSize, out+expandedOffset, outlen - expandedOffset);
        if (expanded <= 0)
            throw makeStringExceptionV(MSGAUD_operator, 0, "Failed to expand offset(%u) size(%u) to %u - returned %d", (size32_t)(src - in), srcSize, outlen, expanded);

        src += srcSize;
        expandedOffset += expanded;
    }

    //finally fill with any uncompressed data
    memcpy(out+expandedOffset, src, outlen - expandedOffset);
}

size32_t CStreamExpander::init(const void *blk)
{
    const size32_t *expsz = (const size32_t *)blk;
    outlen = *expsz;
    in = (const byte *)(expsz+1);
    return outlen;
}
