/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems.

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

#ifndef JLZBASE_INCL
#define JLZBASE_INCL

#include "jlzw.hpp"

// Base classes for the LZ4 and ZStd compression
// They are private to jlib, so they do not have a jlib_decl

inline unsigned sizePacked(size32_t value)
{
    unsigned size = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        size++;
    }
    return size;
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
class CStreamCompressor : public CSimpleInterfaceOf<ICompressor>
{
public:
    virtual void open(void *buf,size32_t max) override;
    virtual void open(MemoryBuffer &mb, size32_t initialSize) override;
    virtual bool adjustLimit(size32_t newLimit) override;
    virtual void close() final override;

    virtual size32_t write(const void *buf,size32_t len) final override;
    virtual void * bufptr() final override;
    virtual size32_t buflen() final override;

    virtual void startblock() override
    {
        //This class either accepts or rejects each write - i.e. all writes are blocked, larger blocking is not supported
    }

    virtual void commitblock() override
    {
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

protected:
//Need to be implemented by the derived classes
    virtual bool tryCompress(bool isFinalCompression) = 0;
    virtual void resetStreamContext() = 0;

    bool processOption(const char * option, const char * textValue);
    void reinitBufferIndexes(size32_t newInLen);

    byte * queryOutputBuffer() { return (byte *)outputs[active].bufferBase(); }

    bool tryFullCompress();
    bool recompressInput(bool isFinalCompression);

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

    //Options for configuring the compressor:
    byte maxCompression = 20;   // Avoid compressing more than 20x because allocating when expanding is painful.
    byte maxRecompress = 1;     // How many times should the code try and recompress all the smaller streams as one?
    unsigned minSizeToCompress = 1; // If the uncompressed data is less than this size, don't bother compressing it
};

//---------------------------------------------------------------------------------------------------------------------

class jlib_decl CStreamExpander : public CExpanderBase
{
public:
    virtual void expand(void *buf) override;
    virtual size32_t init(const void *blk);
    virtual bool supportsBlockDecompression() const override
    {
        return false;
    }

protected:
    virtual void resetStreamContext() = 0;
    virtual int decodeStreamBlock(const void * src, size32_t srcSize, void * dest, size32_t destSize) = 0;

    virtual void *bufptr() { return nullptr;}
    virtual size32_t buflen() { return outlen;}

protected:
    const byte *in = nullptr;
    size32_t outlen = 0;
};


#endif
