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
            throw MakeStringException(MSGAUD_operator,0, "Failed to expand offset(%u) size(%u) to %u - returned %d", (size32_t)(src - in), srcSize, outlen, expanded);

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
