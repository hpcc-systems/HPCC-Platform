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
#include "jmisc.hpp"
#include "jlib.hpp"
#include <time.h>
#include "jfile.hpp"
#include "jencrypt.hpp"
#include "jflz.hpp"
#include "jlzbase.hpp"
#include "jzstd.hpp"

#include <zstd.h>

class CZStdCompressor final : public CBlockCompressor
{
public:
    CZStdCompressor(const char * options) : CBlockCompressor(false)
    {
        auto processOption = [this](const char * option, const char * textValue)
        {
            this->processOption(option, textValue);
        };
        processOptionString(options, processOption);
    }

    virtual size32_t compressDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src, size32_t * numCompressed) override
    {
        dbgassertex(srcSize != 0);
        //ZStd has no option to compress as much as possible - so either succeed or fail.
        size_t compressedSize = ZSTD_compress(dest, destSize, src, srcSize, compressionLevel);
        if (ZSTD_isError(compressedSize))
        {
            if (unlikely(ZSTD_getErrorCode(compressedSize) != ZSTD_error_dstSize_tooSmall))
                throw makeStringExceptionV(0, "ZStd compression error: %s", ZSTD_getErrorName(compressedSize));

            if (numCompressed)
                *numCompressed = 0;
            return 0;
        }
        if (numCompressed)
            *numCompressed = srcSize; // ZSTD_compress always consumes all input
        return (size32_t)compressedSize;
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        size_t result = ZSTD_decompress(dest, destSize, src, srcSize);
        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "ZStd decompression error: %s", ZSTD_getErrorName(result));
        return (size32_t)result;
    }


    virtual CompressionMethod getCompressionMethod() const override { return COMPRESS_METHOD_ZSTD; }

protected:
    bool processOption(const char * option, const char * textValue)
    {
        if (CBlockCompressor::processOption(option, textValue))
            return true;
        int intValue = atoi(textValue);
        if (strieq(option, "level"))
        {
            if ((intValue >= ZSTD_minCLevel()) && (intValue <= ZSTD_maxCLevel()))
                compressionLevel = intValue;
        }
        else
            return false;
        return true;
    };

protected:
    //Options for configuring the compressor:
    int compressionLevel = ZSTD_CLEVEL_DEFAULT;
};



//---------------------------------------------------------------------------------------------------------------------

class CZStdExpander final : public CBlockExpander
{
public:
    CZStdExpander()
    {
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        size_t result = ZSTD_decompress(dest, destSize, src, srcSize);
        if (ZSTD_isError(result))
        {
            if (unlikely(ZSTD_getErrorCode(result) != ZSTD_error_dstSize_tooSmall))
                throw makeStringExceptionV(0, "ZStd decompression error: %s", ZSTD_getErrorName(result));
            //If the buffer is too small, return 0, and the caller can try again
            return 0;
        }
        return (size32_t)result;
    }

    virtual bool supportsBlockDecompression() const override
    {
        return true;
    }
};

//---------------------------------------------------------------------------------------------------------------------

ICompressor *createZStdCompressor(const char * options)
{
    return new CZStdCompressor(options);
}

IExpander *createZStdExpander()
{
    return new CZStdExpander();
}

//---------------------------------------------------------------------------------------------------------------------

// See notes on CStreamCompressor for the serialized stream format

// The ZStd streaming functions compress data in blocks. We are using ZSTD_e_flush.
// If all the data is written then there will be a complete frame.
// If the data is partially written, there will be a complete frame up to the data that was not included.  The last frame will not be marked
// as the end of the stream, but as long as we do not rely on that the data can be decompressed.

class CZStdStreamCompressor final : public CStreamCompressor
{
public:
    CZStdStreamCompressor(const char * options)
    {
        auto processOption = [this](const char * option, const char * textValue)
        {
            this->processOption(option, textValue);
        };
        processOptionString(options, processOption);
        zstdStream = ZSTD_createCStream();
        if (!zstdStream)
            throw makeStringException(0, "Failed to create ZStd compression stream");
    }

    virtual ~CZStdStreamCompressor()
    {
        if (zstdStream)
            ZSTD_freeCStream(zstdStream);
    }

    virtual void open(void *buf, size32_t max, size32_t fixedRowSize, bool _allowPartialWrites) override
    {
        CStreamCompressor::open(buf, max, fixedRowSize, _allowPartialWrites);
        size_t result = ZSTD_initCStream(zstdStream, compressionLevel);
        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "Failed to initialize ZStd compression stream: %s", ZSTD_getErrorName(result));
    }

    virtual CompressionMethod getCompressionMethod() const override { return COMPRESS_METHOD_ZSTDS; }

protected:
    bool processOption(const char * option, const char * textValue)
    {
        if (CStreamCompressor::processOption(option, textValue))
            return true;
        int intValue = atoi(textValue);
        if (strieq(option, "level"))
        {
            if ((intValue >= ZSTD_minCLevel()) && (intValue <= ZSTD_maxCLevel()))
                compressionLevel = intValue;
        }
        else
            return false;
        return true;
    };

    virtual void resetStreamContext() override
    {
        size_t result = ZSTD_initCStream(zstdStream, compressionLevel);
        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "Failed to initialize ZStd compression stream: %s", ZSTD_getErrorName(result));
    }

    virtual bool tryCompress(bool isFinalCompression) override
    {
        size32_t uncompressed = inlen - lastCompress;
        //Sanity check - this could be the case when a limit has been reduced
        if (uncompressed < minSizeToCompress)
            return false;

        size32_t compressedSize = outlen;
        size32_t remaining = outMax - compressedSize - outputExtra;

        const char * from = (const char *)inbuf + lastCompress;
        char * to = (char *)queryOutputBuffer() + outlen;

        // Set up ZStd input and output buffers
        ZSTD_inBuffer input = { from, uncompressed, 0 };
        ZSTD_outBuffer output = { to, remaining, 0 };

        // Compress the data using ZSTD_compressStream2 with continue mode

        ZSTD_EndDirective mode = isFinalCompression ? ZSTD_e_end : ZSTD_e_flush;
        size_t result = ZSTD_compressStream2(zstdStream, &output, &input, mode);
        size32_t newCompressedSize = (size32_t)output.pos;

        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "ZStd compression error: %s.  Compress limit(%u,%u) compressed(%u:0..%u), uncompressed(%u:%u..%u)->%u total(%u)", ZSTD_getErrorName(result), outMax, remaining, outlen, lastCompress, uncompressed, lastCompress, inlen, newCompressedSize, outlen + 8 + inlen - lastCompress);

        if (result != 0)
        {
            //Not all the data could be flushed - the previous streamed blocks are still valid though - so we can unwind the last block of data written
            return false;
        }

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
    ZSTD_CStream * zstdStream = nullptr;

    //Options for configuring the compressor:
    int compressionLevel = ZSTD_CLEVEL_DEFAULT;
};

//---------------------------------------------------------------------------------------------------------------------
class CZStdStreamExpander final : public CStreamExpander
{
public:
    CZStdStreamExpander()
    {
        zstdDStream = ZSTD_createDStream();
        if (!zstdDStream)
            throw makeStringException(0, "Failed to create ZStd decompression stream");
    }

    ~CZStdStreamExpander()
    {
        if (zstdDStream)
            ZSTD_freeDStream(zstdDStream);
    }

    virtual size32_t expandDirect(size32_t destSize, void * dest, size32_t srcSize, const void * src) override
    {
        assertex(destSize != 0);
        size_t result = ZSTD_decompress(dest, destSize, src, srcSize);
        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "ZStd decompression error: %s", ZSTD_getErrorName(result));

        return (size32_t)result;
    }

protected:
    virtual void resetStreamContext() override
    {
        size_t result = ZSTD_initDStream(zstdDStream);
        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "Failed to reset ZStd decompression stream: %s", ZSTD_getErrorName(result));
    }

    virtual int decodeStreamBlock(const void * src, size32_t srcSize, void * dest, size32_t destSize) override
    {
        ZSTD_inBuffer input = { src, srcSize, 0 };
        ZSTD_outBuffer output = { dest, destSize, 0 };

        size_t result = ZSTD_decompressStream(zstdDStream, &output, &input);

        if (ZSTD_isError(result))
            throw makeStringExceptionV(0, "ZStd stream decompression error: %s", ZSTD_getErrorName(result));

        // Return the number of bytes written to the output buffer
        return (int)output.pos;
    }

    virtual void *bufptr() { return nullptr;}
    virtual size32_t buflen() { return outlen;}

protected:
    ZSTD_DStream * zstdDStream = nullptr;
};

//---------------------------------------------------------------------------------------------------------------------

ICompressor *createZStdStreamCompressor(const char * options)
{
    return new CZStdStreamCompressor(options);
}

IExpander *createZStdStreamExpander()
{
    return new CZStdStreamExpander();
}
