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
#include "jstream.ipp"
#include "jstring.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include <assert.h>
#include <stdio.h>
#ifdef _WIN32
#include <io.h>
#endif
#include "jlzw.hpp"
#include "jcrc.hpp"

constexpr size32_t minBlockReadSize = 0x4000;       //16K - used when fetching a single row from a file (e.g. FETCH/KEYED JOIN)
constexpr size32_t defaultBlockReadSize = 0x100000; //1MB

#ifdef _DEBUG
constexpr bool fillInvalidMemory = true;
#else
constexpr bool fillInvalidMemory = false;
#endif

//================================================================================
void CStringBufferOutputStream::writeByte(byte b)
{
    out.append((char)b);
}

void CStringBufferOutputStream::writeBytes(const void * data, int len)
{
    out.append(len, (const char *)data);
}

extern jlib_decl IByteOutputStream *createOutputStream(StringBuffer &to)
{
    return new CStringBufferOutputStream(to);
}

//===========================================================================

CSocketOutputStream::CSocketOutputStream(ISocket *s)
{
    sock = s;
    s->Link();
}

CSocketOutputStream::~CSocketOutputStream()
{
    this->sock->Release();
}

void CSocketOutputStream::writeByte(byte b)
{
    this->sock->write((const char *) &b, 1);
}

void CSocketOutputStream::writeBytes(const void *b, int len)
{
    if (len)
        this->sock->write((const char *) b, len);
}

//===========================================================================

CFileOutputStream::CFileOutputStream(int _handle)
{
    handle = _handle;
}

void CFileOutputStream::writeByte(byte b)
{
    if (_write(handle, &b, 1) != 1)
        throw MakeStringException(-1, "Error while writing byte 0x%x\n", (unsigned)b);
}

void CFileOutputStream::writeBytes(const void *b, int len)
{
    ssize_t written = _write(handle, b, len);
    if (written < 0)
        throw MakeStringException(-1, "Error while writing %d bytes\n", len);
    if (written != len)
        throw MakeStringException(-1, "Truncated (%d) while writing %d bytes\n", (int)written, len);
}

extern jlib_decl IByteOutputStream *createOutputStream(int handle)
{
    return new CFileOutputStream(handle);
}

//===========================================================================

// This class ensures that data is read in fixed block sizes - NOT a fixed buffer size.
// This means the buffer size is likely to be bigger than the block size - the class is passed
// an initial estimate for the potential overlap.

class CBlockedSerialInputStream final : public CInterfaceOf<IBufferedSerialInputStream>
{
public:
    CBlockedSerialInputStream(ISerialInputStream * _input, size32_t _blockReadSize)
    : input(_input), blockReadSize(_blockReadSize)
    {
        // A blockReadSize of 0 is often used for a fetch from a file, where only a single record is likely to be read.
        // So choose a sensible low value that will avoid too many reads, but not use too much memory.
        if (blockReadSize < minBlockReadSize)
            blockReadSize = minBlockReadSize;
        else if (blockReadSize == (size32_t)-1)
            blockReadSize = defaultBlockReadSize;

        //Allocate the input buffer slightly bigger than the block read size, so that a small peek at the end of a block
        //does not have to expand the block.  (Avoid extra allocation for for pathological unittests where blockReadSize <= 1024)
        size32_t extraSize = (blockReadSize > 1024) ? 1024 : 0;
        buffer.allocate(blockReadSize + extraSize);
        nextBlockOffset = input->tell();
    }

    virtual size32_t read(size32_t len, void * ptr) override
    {
        size32_t sizeRead = 0;
        byte * target = (byte *)ptr;
        if (likely(bufferOffset < dataLength))
        {
            size32_t toCopy = std::min(len, available());
            memcpy(target, data(bufferOffset), toCopy);
            if (fillInvalidMemory)
                memset(data(bufferOffset), 0xcc, toCopy);
            bufferOffset += toCopy;
            if (likely(toCopy == len))
                return toCopy;

            sizeRead = toCopy;
        }

        //While there are blocks larger than the buffer size read directly into the target buffer
        while (unlikely(sizeRead + blockReadSize <= len))
        {
            //Read multiple blocks in a single operation
            size32_t numBlocks = (len - sizeRead) / blockReadSize;
            size32_t got = readNext(blockReadSize * numBlocks, target+sizeRead);
            if ((got == 0) || (got == BufferTooSmall))
                break;
            sizeRead += got;
            nextBlockOffset += got;
        }

        while (likely((sizeRead < len) && !endOfStream))
        {
            assertex(bufferOffset == dataLength);
            // NOTE: This could read less than a block, even if a whole block was requested.
            // Will set endOfStream if there is no more to read - do not special case end-of-file here.
            readNextBlock();

            size32_t toCopy = std::min(len-sizeRead, available());
            memcpy(target + sizeRead, data(bufferOffset), toCopy);
            if (fillInvalidMemory)
                memset(data(bufferOffset), 0xcc, toCopy);
            bufferOffset += toCopy;
            sizeRead += toCopy;
        }
        return sizeRead;
    }

    virtual const void * peek(size32_t wanted, size32_t &got) override
    {
        if (likely(wanted <= available()))
        {
            got = available();
            return data(bufferOffset);
        }
        //Split this into a separate non-inlined function so that the fastpath function does not need to protect the stack
        return peekAndExpand(wanted, got);
    }

    const void * peekAndExpand(size32_t wanted, size32_t &got) __attribute__((noinline))
    {
        while (unlikely(wanted > available()))
        {
            if (endOfStream)
                break;
            readNextBlock(); // will be appended onto the end of the existing buffer
        }
        got = available();
        if (unlikely(got == 0))
            return nullptr;
        return data(bufferOffset);
    }

    virtual void get(size32_t len, void * ptr) override
    {
        size32_t numRead = read(len, ptr);
        if (unlikely(numRead != len))
            throw makeStringExceptionV(-1, "End of input stream for read of %u bytes at offset %llu", len, tell()-numRead);
    }

    virtual bool eos() override
    {
        if (available())
            return false;

        size32_t got;
        return peek(1, got) == nullptr;
    }

    virtual void skip(size32_t sz) override
    {
        size32_t remaining = available();
        if (likely(sz <= remaining))
        {
            if (fillInvalidMemory)
                memset(data(bufferOffset), 0xcc, sz);
            bufferOffset += sz;
        }
        else
        {
            if (fillInvalidMemory)
                memset(data(bufferOffset), 0xcc, remaining);
            bufferOffset = dataLength;
            skipInput(sz - remaining);
        }
    }
    virtual offset_t tell() const override
    {
        dbgassertex(nextBlockOffset == input->tell());
        return nextBlockOffset + bufferOffset - dataLength;
    }

    virtual void reset(offset_t _offset, offset_t _flen) override
    {
        endOfStream = false;
        nextBlockOffset = _offset;
        bufferOffset = 0;
        dataLength = 0;
        input->reset(_offset, _flen);
    }

protected:
    inline byte * data(size32_t offset) { return (byte *)buffer.get() + offset; }
    inline size32_t available() const { return dataLength - bufferOffset; }

    const void * peekBuffer(size32_t &got)
    {
        got = available();
        return data(bufferOffset);
    }


private:
    void skipInput(size32_t size)
    {
        input->skip(size);
        nextBlockOffset += size;
    }

    void readNextBlock()
    {
        if (endOfStream)
            return;

        size32_t remaining = available();

        //If there is remaining data that isn't at the head of the buffer, move it to the head
        //This is only worth doing if it will prevent the buffer being expanded - otherwise
        //the data will be moved when it is expanded
        if (bufferOffset && (remaining + blockReadSize < buffer.length()) && (remaining < blockReadSize / 4))
        {
            if (remaining)
                memmove(data(0), data(bufferOffset), remaining);

            dataLength = remaining;
            bufferOffset = 0;
        }

        size32_t nextReadSize = blockReadSize;
        for (;;)
        {
            expandBuffer(dataLength + nextReadSize);
            size32_t got = readNext(nextReadSize, data(dataLength)); // will set endOfStream if finished
            if (likely(got != BufferTooSmall))
            {
                nextBlockOffset += got;
                dataLength += got;
                break;
            }

            //This can occur when decompressing - if the next block is too big to fit in the requested buffer
            nextReadSize += blockReadSize;
        }
    }

    size32_t readNext(size32_t len, void * ptr)
    {
        size32_t got = input->read(len, ptr);
        if (got == 0)
            endOfStream = true;
        return got;
    }

    void expandBuffer(size32_t newLength)
    {
        if (buffer.length() < newLength)
        {
            MemoryAttr expandedBuffer(newLength);
            //NOTE: This could use realloc instead - may not be better because it would copy all the data, not just the valid data.
            memcpy(expandedBuffer.mem(), data(bufferOffset), available());
            buffer.swapWith(expandedBuffer);
            dataLength -= bufferOffset;
            bufferOffset = 0;
        }
    }


protected:
    Linked<ISerialInputStream> input;
    MemoryAttr buffer;
    offset_t nextBlockOffset = 0;
    size32_t blockReadSize = 0;
    size32_t bufferOffset = 0;
    size32_t dataLength = 0;
    bool endOfStream = false;
};

IBufferedSerialInputStream * createBufferedInputStream(ISerialInputStream * input, size32_t blockReadSize)
{
    return new CBlockedSerialInputStream(input, blockReadSize);
}

//---------------------------------------------------------------------------

class CDecompressingSerialInputStream final : public CInterfaceOf<ISerialInputStream>
{
public:
    CDecompressingSerialInputStream(IBufferedSerialInputStream * _input, IExpander * _decompressor)
    : input(_input), decompressor(_decompressor)
    {
    }

    virtual size32_t read(size32_t len, void * ptr) override
    {
        size32_t available;
        constexpr size32_t sizeHeader = 2 * sizeof(size32_t);

    again:
        const byte * next = static_cast<const byte *>(input->peek(sizeHeader, available));
        if (available == 0)
            return 0;
        if (available < sizeHeader)
            throw makeStringExceptionV(-1, "End of input stream for read of %u bytes at offset %llu (%llu)", len, tell(), input->tell());

        size32_t decompressedSize = *(const size32_t *)next;   // Technically illegal - should copy to an aligned object
        size32_t compressedSize = *((const size32_t *)next + 1); // Technically illegal - should copy to an aligned object
        if (unlikely(decompressedSize <= skipPending))
        {
            input->skip(sizeHeader + compressedSize);
            nextOffset += decompressedSize;
            skipPending -= decompressedSize;
            goto again; // go around the loop again. - a goto seems the cleanest way to code this.
        }

        //Check there is enough space in the buffer for the next block once we have skipped any whole blocks
        if (len < decompressedSize)
            return BufferTooSmall; // Need to expand the buffer

        //If the input buffer is not big enough skip the header so it does not need to be contiguous with the data
        //but that means various offsets need adjusting further down.
        size32_t sizeNextHeader = sizeHeader;
        if (unlikely(available < sizeHeader + compressedSize))
        {
            input->skip(sizeHeader);
            next = static_cast<const byte *>(input->peek(compressedSize, available));
            if (available < compressedSize)
                throw makeStringExceptionV(-1, "End of input stream for read of %u bytes at offset %llu (%llu)", len, tell(), input->tell());
            sizeNextHeader = 0;
        }

        size32_t expanded = decompressor->expandDirect(decompressedSize, ptr, compressedSize, next + sizeNextHeader);
        assertex(expanded == decompressedSize);
        nextOffset += decompressedSize;
        input->skip(sizeNextHeader + compressedSize);

        if (skipPending > 0)
        {
            //Yuk.  This works, but will require copying the whole buffer
            memmove(ptr, (const byte *)ptr+skipPending, decompressedSize - skipPending);
            decompressedSize -= skipPending;
            skipPending = 0;
        }

        return decompressedSize;
    }

    virtual void get(size32_t len, void * ptr) override
    {
        //This function cannot be implemented because the caller would have to request exactly the correct length
        throwUnexpected();
    }

    virtual void skip(size32_t sz) override
    {
        skipPending += sz;
    }
    virtual offset_t tell() const override
    {
        return nextOffset + skipPending;
    }

    virtual void reset(offset_t _offset, offset_t _flen=(offset_t)-1) override
    {
        nextOffset = _offset;
        skipPending = 0;
        input->reset(_offset, _flen);
    }

protected:
    Linked<IBufferedSerialInputStream> input;
    Linked<IExpander> decompressor;
    offset_t nextOffset = 0;
    offset_t skipPending = 0;
};

ISerialInputStream * createDecompressingInputStream(IBufferedSerialInputStream * input, IExpander * decompressor)
{
    assertex(decompressor->supportsBlockDecompression());
    return new CDecompressingSerialInputStream(input, decompressor);
}

//---------------------------------------------------------------------------

bool readZeroTerminatedString(StringBuffer & out, IBufferedSerialInputStream & in)
{
    for (;;)
    {
        size32_t got;
        const char * start = (const char *)in.peek(1,got);
        if (!start)
            return false;  // eof before nul detected;

        const char * cur = start;
        const char * end = start + got;
        while (cur != end)
        {
            if (!*cur)
            {
                out.append(cur - start, start);
                in.skip((cur - start) + 1); // skip the text and the nul
                return true;
            }
            cur++;
        }
        out.append(got, start);
        in.skip(got);
    }
}

const char * queryZeroTerminatedString(IBufferedSerialInputStream & in, size32_t & len)
{
    size32_t scanned = 0;
    for (;;)
    {
        size32_t got;
        const char * start = (const char *)in.peek(scanned+1,got);
        if (got <= scanned)
            return nullptr;  // eof before nul detected;

        const char * cur = start + scanned;
        const char * end = start + got;
        while (cur != end)
        {
            if (!*cur)
            {
                len = cur - start;
                return start;
            }
            cur++;
        }
        scanned = got;
    }
}

extern jlib_decl std::pair<const char *, const char *> peekKeyValuePair(IBufferedSerialInputStream & in, size32_t & len)
{
    size32_t scanned = 0;
    size32_t valueOffset = (size32_t)-1;
    for (;;)
    {
        size32_t got;
        const char * start = (const char *)in.peek(scanned+1,got);
        if (got <= scanned)
        {
            len = scanned;  // It is not clear what should be returned in this case.
            if (valueOffset == (size32_t)-1)
                return std::pair(nullptr, nullptr);  // eof before nul detected;

            //a key was found, but no associated value
            return std::pair(start, nullptr);
        }

        const char * cur = start + scanned;
        const char * end = start + got;
        while (cur != end)
        {
            if (!*cur)
            {
                if (valueOffset != (size32_t)-1)
                {
                    len = cur - start;
                    return std::pair(start, start+valueOffset);
                }
                valueOffset = cur + 1 - start;
            }
            cur++;
        }
        scanned = got;
    }
}

const char * peekStringList(std::vector<size32_t> & matchOffsets, IBufferedSerialInputStream & in, size32_t & len, unsigned grouping)
{
    size32_t scanned = 0;
    size32_t startNext = 0;
    for (;;)
    {
        size32_t got;
        const char * start = (const char *)in.peek(scanned+1,got);
        if (got <= scanned)
        {
            len = scanned;
            if (startNext == scanned)
            {
                //End of file, but the last string was null terminated...
                return start;
            }
            return nullptr;
        }

        for (size32_t offset = scanned; offset < got; offset++)
        {
            char next = start[offset];
            if (!next)
            {
                if (offset == startNext && (grouping == 0 || matchOffsets.size() % grouping == 0))
                {
                    //A zero length string terminates the list - include the empty string in the length
                    len = offset + 1;
                    return start;
                }
                matchOffsets.push_back(startNext);
                startNext = offset + 1;
            }
        }
        scanned = got;
    }
}

//---------------------------------------------------------------------------

class CFileSerialInputStream final : public CInterfaceOf<ISerialInputStream>
{
public:
    CFileSerialInputStream(IFileIO * _input, offset_t startOffset, offset_t length)
    : input(_input), nextOffset(startOffset)
    {
        if (length != UnknownOffset)
            lastOffset = startOffset + length;
    }

    virtual size32_t read(size32_t len, void * ptr) override
    {
        if (nextOffset + len > lastOffset)
            len = lastOffset - nextOffset;
        unsigned numRead = input->read(nextOffset, len, ptr);
        nextOffset += numRead;
        return numRead;
    }

    virtual void get(size32_t len, void * ptr) override
    {
        size32_t numRead = read(len, ptr);
        if (numRead != len)
            throw makeStringExceptionV(-1, "End of input stream for read of %u bytes at offset %llu", len, tell()-numRead);
    }

    virtual void skip(size32_t len) override
    {
        if (nextOffset + len <= lastOffset)
            nextOffset += len;
        else
            nextOffset = lastOffset;
    }

    virtual offset_t tell() const override
    {
        return nextOffset;
    }

    virtual void reset(offset_t _offset, offset_t _flen) override
    {
        nextOffset = _offset;
        lastOffset = (_flen == UnknownOffset) ? UnknownOffset : _offset + _flen;
        assertex(nextOffset <= lastOffset);
    }

protected:
    Linked<IFileIO> input;
    offset_t nextOffset = 0;
    offset_t lastOffset = UnknownOffset;        // max(offset_t), so input file length is not limited
};

//Temporary class - long term goal is to have IFile create this directly and avoid an indirect call.
ISerialInputStream * createSerialInputStream(IFileIO * input)
{
    return new CFileSerialInputStream(input, 0, UnknownOffset);
}


ISerialInputStream * createSerialInputStream(IFileIO * input, offset_t startOffset, offset_t length)
{
    return new CFileSerialInputStream(input, startOffset, length);
}

//---------------------------------------------------------------------------

class CCrcInputStream : public CSimpleInterfaceOf<ICrcSerialInputStream>
{
    CRC32 crc;
    Linked<ISerialInputStream> input;

public:
    CCrcInputStream(ISerialInputStream *_input)
        : input(_input) {}

    virtual unsigned queryCrc() const override { return crc.get(); }

    virtual size32_t read(size32_t len, void * ptr) override
    {
        size32_t numRead = input->read(len, ptr);
        crc.tally(numRead, ptr);
        return numRead;
    }

    virtual void get(size32_t len, void * ptr) override
    {
        input->get(len, ptr);
        crc.tally(len, ptr);
    }

    virtual void skip(size32_t sz) override
    {
        if (unlikely(sz == 0))
            return;

        constexpr size32_t maxBufferSize = 8192; // 8KB max buffer for reading skipped data
        char buffer[maxBufferSize];

        while (sz > 0)
        {
            size32_t toRead = std::min(sz, maxBufferSize);
            size32_t numRead = input->read(toRead, buffer);
            if (numRead == 0)
                break; // End of stream
            crc.tally(numRead, buffer);
            sz -= numRead;
        }
    }

    virtual offset_t tell() const override
    {
        return input->tell();
    }

    virtual void reset(offset_t _offset, offset_t _flen) override
    {
        crc.reset();
        input->reset(_offset, _flen);
    }
};

ICrcSerialInputStream *createCrcInputStream(ISerialInputStream *input)
{
    return new CCrcInputStream(input);
}

//---------------------------------------------------------------------------

class CCrcOutputStream : public CSimpleInterfaceOf<ICrcSerialOutputStream>
{
    CRC32 crc;
    Linked<ISerialOutputStream> output;

public:
    CCrcOutputStream(ISerialOutputStream *_output)
        : output(_output) {}

    virtual unsigned queryCrc() const override { return crc.get(); }

    virtual void flush() override { output->flush(); }
    virtual void put(size32_t len, const void *data) override
    {
        output->put(len, data);
        crc.tally(len, data);
    }
    virtual offset_t tell() const override { return output->tell(); }
};

ICrcSerialOutputStream *createCrcOutputStream(ISerialOutputStream *output)
{
    return new CCrcOutputStream(output);
}

//---------------------------------------------------------------------------


//===========================================================================

// This class ensures that data is read in fixed block sizes - NOT a fixed buffer size.
// This means the buffer size is likely to be bigger than the block size - the class is passed
// an initial estimate for the potential overlap.

class CBlockedSerialOutputStream final : public CInterfaceOf<IBufferedSerialOutputStream>
{
public:
    CBlockedSerialOutputStream(ISerialOutputStream * _output, size32_t _blockWriteSize)
    : output(_output), blockWriteSize(_blockWriteSize)
    {
        size32_t initialBufferSize = blockWriteSize;
        buffer.allocate(initialBufferSize);
    }

    ~CBlockedSerialOutputStream()
    {
        try
        {
            flush();
        }
        catch (IException * e)
        {
            EXCLOG(e, "CBlockedSerialOutputStream::~CBlockedSerialOutputStream");
            e->Release();
        }
    }

    virtual void flush() override
    {
        assertex(!isSuspended());
        flushBlocks(true);
        output->flush();
    }

    virtual void put(size32_t len, const void * ptr) override
    {
        //Special case where there is space in the buffer - so the function has much lower overhead
        if (likely(!isSuspended()))
        {
            if (likely(bufferOffset + len <= blockWriteSize))
            {
                memcpy(data(bufferOffset), ptr, len);
                bufferOffset += len;
                return;
            }
        }
        doWrite(len, ptr);
    }

    //General purpose write routine.
    size32_t doWrite(size32_t len, const void * ptr) __attribute__((noinline))
    {
        size32_t sizeWritten = 0;
        const byte * src = (const byte *)ptr;
        if (likely(!isSuspended()))
        {
            //NOTE: If we are writing more than blockWriteSize and bufferOffset == 0 then we
            //could avoid this first memcpy.  However, the tests would slow down the very common cases
            //so live with the potential inefficiency

            //First fill up any remaining output block.
            //bufferOffset can only be > blockWriteSize if it is suspended
            //When a suspended output is resumed all pending blocks will be written out.
            assertex(bufferOffset <= blockWriteSize);
            size32_t space = (blockWriteSize - bufferOffset);
            if (likely(space))
            {
                size32_t toCopy = std::min(len, space);
                memcpy(data(bufferOffset), src, toCopy);
                bufferOffset += toCopy;
                sizeWritten += toCopy;
                // Otherwise this would have been processed by the fast-path.  The following code is still correct if condition is false.
                dbgassertex(sizeWritten < len);
            }

            output->put(blockWriteSize, data(0));
            //Revisit when compression can indicate that the buffer is only half consumed
            blockOffset += blockWriteSize;
            bufferOffset = 0;

            //While there are blocks larger than the block write size write directly to the output
            while (unlikely(sizeWritten + blockWriteSize <= len))
            {
                output->put(blockWriteSize, src+sizeWritten);
                sizeWritten += blockWriteSize;
                blockOffset += blockWriteSize;
            }
        }

        size32_t remaining = len - sizeWritten;
        if (likely(remaining))
        {
            ensureSpace(remaining);
            memcpy(data(bufferOffset), src+sizeWritten, remaining);
            bufferOffset += remaining;
        }
        return sizeWritten + remaining;
    }

    inline bool readyToWrite() const
    {
        return (bufferOffset >= blockWriteSize) && !isSuspended();
    }

    virtual byte * reserve(size32_t wanted, size32_t & got) override
    {
        ensureSpace(wanted);
        got = available();
        return data(bufferOffset);
    }

    virtual void commit(size32_t written) override
    {
        doCommit(written);
        checkWriteIfNotSuspended();
    }

    virtual void suspend(size32_t len) override
    {
        suspendOffsets.append(bufferOffset);
        ensureSpace(len);
        //Leave space for the data to be written afterwards
        bufferOffset += len;
    }

    virtual void resume(size32_t len, const void * ptr) override
    {
        doResume(len, ptr);
        checkWriteIfNotSuspended();
    }

    virtual offset_t tell() const override { return blockOffset+bufferOffset; }

    virtual void replaceOutput(ISerialOutputStream * newOutput) override
    {
        output.set(newOutput);
    }

//-------------------------------------------------------
//Helper functions for CThreadedBlockedSerialOutputStream
//doCommit() and doResume are also used by this class

    void doCommit(size32_t written)
    {
        bufferOffset += written;
    }

    void doResume(size32_t len, const void * ptr)
    {
        size32_t offset = suspendOffsets.tos();
        suspendOffsets.pop();
        if (likely(len))
            memcpy(data(offset), ptr, len);
    }

    size32_t writeToBuffer(size32_t len, const void * ptr)
    {
        if (likely(!isSuspended()))
        {
            assertex(bufferOffset <= blockWriteSize);
            size32_t space = (blockWriteSize - bufferOffset);
            size32_t toCopy = std::min(len, space);
            memcpy(data(bufferOffset), ptr, toCopy);
            bufferOffset += toCopy;
            return toCopy;
        }
        else
        {
            ensureSpace(len);
            memcpy(data(bufferOffset), ptr, len);
            bufferOffset += len;
            return len;
        }
    }

    size32_t flushAllButLastBlock()
    {
        assertex(bufferOffset >= blockWriteSize);

        unsigned from = 0;
        for (;;)
        {
            size32_t remaining = bufferOffset - from;
            if (remaining < 2*blockWriteSize)
                return from;

            output->put(blockWriteSize, data(from));
            blockOffset += blockWriteSize;
            from += blockWriteSize;
        }
    }

    //This can be executed in parallel with writeBlock()
    void copyRemainingAndReset(CBlockedSerialOutputStream & other, size32_t marker)
    {
        size32_t from = marker + blockWriteSize;
        size32_t remaining = bufferOffset - from;
        if (remaining)
        {
            size32_t written = other.writeToBuffer(remaining, data(from));
            assertex(written == remaining);
        }

        bufferOffset = 0;
    }

    //This can be executed in parallel with copyRemainingAndReset()
    void writeBlock(unsigned from)
    {
        output->put(blockWriteSize, data(from));
        blockOffset += blockWriteSize;
    }


protected:
    inline byte * data(size32_t offset) { return (byte *)buffer.get() + offset; }
    inline size32_t available() const { return buffer.length() - bufferOffset; }
    inline bool isSuspended() const { return suspendOffsets.ordinality() != 0; }

    void ensureSpace(size32_t required) __attribute__((always_inline))
    {
        if (unlikely(required > available()))
            expandBuffer(bufferOffset + required);
    }
    void expandBuffer(size32_t newLength)
    {
        if (buffer.length() < newLength)
        {
            //When serializing a child dataset with many rows (e.g. 500K) this could be called many times, each time
            //causing the buffer to reallocate.  Therefore ensure the new size is rounded up to avoid excessive
            //reallocation and copying.
            //This could use blockWriteSize /4 but that still scales badly when number of rows > 1M
            size32_t alignment = buffer.length() / 4;
            if (alignment < 32)
                alignment = 32;
            newLength += (alignment - 1);
            newLength -= newLength % alignment;

            MemoryAttr expandedBuffer(newLength);
            memcpy(expandedBuffer.mem(), data(0), bufferOffset);
            buffer.swapWith(expandedBuffer);
        }
    }

    inline void checkWriteIfNotSuspended() __attribute__((always_inline))
    {
        if (likely(bufferOffset < blockWriteSize))
            return;

        if (unlikely(isSuspended()))
            return;

        flushBlocks(false);
    }

    void flushBlocks(bool flushLast)
    {
        unsigned from = 0;
        //Write out any pending blocks
        for (;;)
        {
            size32_t remaining = bufferOffset - from;
            if (remaining == 0)
                break;
            if ((remaining < blockWriteSize) && !flushLast)
                break;
            size32_t writeSize = std::min(remaining, blockWriteSize);
            output->put(writeSize, data(from));
            blockOffset += writeSize;
            from += writeSize;
        }

        if ((from != 0) && (from != bufferOffset))
            memcpy(data(0), data(from), bufferOffset - from);

        bufferOffset -= from;
    }

protected:
    Linked<ISerialOutputStream> output;
    MemoryAttr buffer;
    UnsignedArray suspendOffsets;
    offset_t blockOffset = 0;
    size32_t blockWriteSize = 0;
    size32_t bufferOffset = 0;
};

IBufferedSerialOutputStream * createBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize)
{
    assertex(blockWriteSize);
    return new CBlockedSerialOutputStream(output, blockWriteSize);
}

//---------------------------------------------------------------------------

class CThreadedBlockedSerialOutputStream final : public CInterfaceOf<IBufferedSerialOutputStream>, public IThreaded
{
public:
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IBufferedSerialOutputStream>)

    CThreadedBlockedSerialOutputStream(ISerialOutputStream * _output, size32_t _blockWriteSize)
    : threaded("CThreadedBlockedSerialOutputStream"), stream{{_output,_blockWriteSize},{_output,_blockWriteSize}}
    {
        active = 0;
        threaded.init(this, false);
    }
    ~CThreadedBlockedSerialOutputStream()
    {
        abort = true;
        go.signal();
        threaded.join();
    }

    virtual void flush() override
    {
        ensureWriteComplete();
        stream[active].flush();
    }

    virtual void put(size32_t len, const void * ptr) override
    {
        size32_t sizeWritten = 0;
        const byte * src = (const byte *)ptr;
        for (;;)
        {
            CBlockedSerialOutputStream & activeStream = stream[active];
            size32_t written = activeStream.writeToBuffer(len-sizeWritten, src+sizeWritten);
            sizeWritten += written;

            checkForPendingWrite();

            if (sizeWritten == len)
                return;
        }
    }

    virtual byte * reserve(size32_t wanted, size32_t & got) override
    {
        return stream[active].reserve(wanted, got);
    }

    virtual void commit(size32_t written) override
    {
        stream[active].doCommit(written);
        checkForPendingWrite();
    }
    virtual void suspend(size32_t len) override
    {
        stream[active].suspend(len);
    }
    virtual void resume(size32_t len, const void * ptr) override
    {
        stream[active].doResume(len, ptr);
        checkForPendingWrite();
    }

    virtual void replaceOutput(ISerialOutputStream * newOutput) override
    {
        stream[0].replaceOutput(newOutput);
        stream[1].replaceOutput(newOutput);
    }

protected:
    virtual offset_t tell() const override
    {
        return stream[0].tell() + stream[1].tell();
    }

    inline void checkForPendingWrite() __attribute__((always_inline))
    {
        if (unlikely(stream[active].readyToWrite()))
        {
            startActiveWrite(active);
            active = 1-active;
        }
    }

    void ensureWriteComplete()
    {
        if (running)
        {
            done.wait();
            running = false;
            if (pendingException)
                throw pendingException.getClear();
        }
    }

    void startActiveWrite(unsigned whichStream) __attribute__((noinline))
    {
        ensureWriteComplete();
        //write out all but the last block
        size32_t marker = stream[whichStream].flushAllButLastBlock();

        //The following calls can be executed in parallel.  Copy remaining afterwards to allow for better thread overlap
        startActiveWrite(whichStream, marker);
        stream[whichStream].copyRemainingAndReset(stream[1-whichStream], marker);
    }

    void startActiveWrite(unsigned whichStream, unsigned marker)
    {
        runningStream = whichStream;
        runningMarker = marker;
        running = true;

        constexpr bool useThreading = true;
        if (useThreading)
            go.signal();
        else
            run();
    }

    void run()
    {
        try
        {
            stream[runningStream].writeBlock(runningMarker);
        }
        catch (IException * e)
        {
            pendingException.setown(e);
        }
        done.signal();
    }

    virtual void threadmain() override
    {
        for (;;)
        {
            go.wait();
            if (abort)
                break;
            run();
        }
    }

protected:
    CThreaded threaded;
    CBlockedSerialOutputStream stream[2];
    Semaphore go;
    Semaphore done;
    Owned<IException> pendingException;
    bool running{false};
    std::atomic<bool> abort{false};
    unsigned active = 0;
//Used by the thread that outputs the data
    unsigned runningStream = 0;
    unsigned runningMarker = 0;
};

IBufferedSerialOutputStream * createThreadedBufferedOutputStream(ISerialOutputStream * output, size32_t blockWriteSize)
{
    assertex(blockWriteSize);
    return new CThreadedBlockedSerialOutputStream(output, blockWriteSize);
}

//---------------------------------------------------------------------------

class CCompressingSerialOutputStream final : public CInterfaceOf<ISerialOutputStream>
{
public:
    CCompressingSerialOutputStream(IBufferedSerialOutputStream * _output, ICompressor * _compressor)
    : output(_output), compressor(_compressor)
    {
    }

    virtual void flush() override
    {
        output->flush();
    }

    virtual void put(size32_t len, const void * data) override
    {
        constexpr size32_t sizeHeader = 2 * sizeof(size32_t);
        unsigned expectedSize = sizeHeader + len;   // MORE: Reduce this to minimize the size of the target buffer.
        for (;;)
        {
            //Future: When general file support is added this will need to keep track of where it is in the output
            //block and manage packing data into the boundaries cleanly.  (Probably implement with a new class)
            size32_t available;
            byte * target = output->reserve(expectedSize, available);
            assertex(available >= sizeHeader);

            //MORE: Support option to compress as much as possible when packing into fixed size target buffers
            size32_t written = compressor->compressDirect(available-sizeHeader, target + sizeHeader, len, data, nullptr);
            if (written != 0)
            {
                *(size32_t *)target = len;   // Technically illegal - should copy to an aligned object
                *((size32_t *)target + 1) = written; // Technically illegal - should copy to an aligned object
                output->commit(sizeHeader + written);
                return;
            }

            //Increase the buffer size and try again
            expectedSize += len / 4;
        }
    }

    virtual offset_t tell() const override { throwUnexpected(); }

protected:
    Linked<IBufferedSerialOutputStream> output;
    Linked<ICompressor> compressor;
};

ISerialOutputStream * createCompressingOutputStream(IBufferedSerialOutputStream * output, ICompressor * compressor)
{
    assertex(compressor->supportsBlockCompression());
    return new CCompressingSerialOutputStream(output, compressor);
}

//---------------------------------------------------------------------------

class CFileSerialOutputStream final : public CInterfaceOf<ISerialOutputStream>
{
public:
    CFileSerialOutputStream(IFileIO * _output, offset_t _offset)
    : output(_output), nextOffset(_offset)
    {
    }

    virtual void flush() override
    {
        output->flush();
    }

    virtual void put(size32_t len, const void * ptr) override
    {
        unsigned written = output->write(nextOffset, len, ptr);
        nextOffset += len;
        if (written != len)
            throw makeStringExceptionV(-1, "Failed to write %u bytes at offset %llu", len-written, nextOffset);
    }

    virtual offset_t tell() const override
    {
        return nextOffset;
    }

protected:
    Linked<IFileIO> output;
    offset_t nextOffset = 0;
};

//Temporary class - long term goal is to have IFile create this directly and avoid an indirect call.
ISerialOutputStream * createSerialOutputStream(IFileIO * output, offset_t offset)
{
    return new CFileSerialOutputStream(output, offset);
}

//---------------------------------------------------------------------------------------------------------------------

//This base class was created in case we ever want a lighter weight implementation that only implements ISerialOutputStream
template <class BUFFER_CLASS>
class CBufferSerialOutputStreamBase : public CInterfaceOf<IBufferedSerialOutputStream>
{
public:
    CBufferSerialOutputStreamBase(BUFFER_CLASS & _target)
    : target(_target)
    {
    }

    virtual void flush() override
    {
    }

    virtual void put(size32_t len, const void * ptr) override
    {
        target.append(len, (const char *)ptr);
    }

    virtual offset_t tell() const override
    {
        return target.length();
    }

    virtual void replaceOutput(ISerialOutputStream * newOutput) override
    {
        throwUnimplemented();
    }

protected:
    BUFFER_CLASS & target;
};


template <class BUFFER_CLASS>
class CBufferSerialOutputStream final : public CBufferSerialOutputStreamBase<BUFFER_CLASS>
{
    using CBufferSerialOutputStreamBase<BUFFER_CLASS>::target;
public:
    CBufferSerialOutputStream(BUFFER_CLASS & _target)
    : CBufferSerialOutputStreamBase<BUFFER_CLASS>(_target)
    {
    }

    virtual byte * reserve(size32_t wanted, size32_t & got) override
    {
        size_t sizeGot;
        void * ret = target.ensureCapacity(wanted, sizeGot);
        got = sizeGot;
        return (byte *)ret;
    }

    virtual void commit(size32_t written) override
    {
        //The data has already been written - the cleanest function to call to skip the write pos forward
        target.reserve(written);
    }

    virtual void suspend(size32_t wanted) override
    {
        suspendOffsets.append(target.length());
        //Leave space for the data to be written afterwards
        target.reserve(wanted);
    }
    virtual void resume(size32_t len, const void * ptr) override
    {
        size32_t offset = suspendOffsets.tos();
        suspendOffsets.pop();
        target.replace(offset, len, ptr);
    }

protected:
    Unsigned64Array suspendOffsets;
};


IBufferedSerialOutputStream * createBufferedSerialOutputStream(StringBuffer & target)
{
    return new CBufferSerialOutputStream(target);
}

IBufferedSerialOutputStream * createBufferedSerialOutputStream(MemoryBuffer & target)
{
    return new CBufferSerialOutputStream(target);
}


//---------------------------------------------------------------------------------------------------------------------

class CBufferSerialInputStream final : public CInterfaceOf<IBufferedSerialInputStream>
{
public:
    CBufferSerialInputStream(MemoryBuffer & _source)
    : source(_source)
    {
    }

    virtual size32_t read(size32_t len, void * ptr) override
    {
        size32_t available = source.remaining();
        size32_t toCopy = std::min(len, available);
        if (fillInvalidMemory)
        {
            const byte * data = source.readDirect(toCopy);
            memcpy(ptr, data, toCopy);
            memset(const_cast<byte *>(data), 0xcc, toCopy);
        }
        else
            source.read(toCopy, ptr);
        return toCopy;
    }

    virtual void skip(size32_t len) override
    {
        assertex(len <= source.remaining());
        if (fillInvalidMemory)
            memset(const_cast<byte *>(source.readDirect(0)), 0xcc, len);
        source.skip(len);
    }

    virtual void get(size32_t len, void * ptr) override
    {
        assertex(len <= source.remaining());
        source.read(len, ptr);
    }
    virtual bool eos() override
    {
        return source.remaining() == 0;
    }
    virtual void reset(offset_t _offset, offset_t _flen) override
    {
        UNIMPLEMENTED;
    }
    virtual offset_t tell() const override
    {
        return source.getPos();
    }

    virtual const void * peek(size32_t wanted, size32_t &got) override
    {
        size32_t available = source.remaining();
        got = available;
        if (available == 0)
            return nullptr;
        return source.readDirect(0);
    }

protected:
    MemoryBuffer & source;
};

IBufferedSerialInputStream * createBufferedSerialInputStream(MemoryBuffer & source)
{
    return new CBufferSerialInputStream(source);
}

ISerialInputStream *createProgressStream(ISerialInputStream *stream, offset_t offset, offset_t len, const char *msg, unsigned periodSecs)
{
    class CProgressStream : public CSimpleInterfaceOf<ISerialInputStream>
    {
        Linked<ISerialInputStream> stream;
        offset_t offset = 0;
        offset_t totalSize = 0;
        StringAttr msg;
        void log(double pct)
        {
            PROGLOG("%s - %.2f%% complete", msg.get(), pct);
        }
        void logProgressIfNeeded()
        {
            if (periodTimer.hasElapsed())
            {
                if (unlikely(0 == totalSize))
                    log(100.0);
                else
                {
                    offset_t pos = stream->tell();
                    double pct = ((double)pos) / totalSize * 100;
                    log(pct);
                }
            }
        }
        PeriodicTimer periodTimer;
    public:
        CProgressStream(ISerialInputStream *_stream, offset_t _offset, offset_t len, const char *_msg, unsigned periodSecs)
            : stream(_stream), offset(_offset), msg(_msg), periodTimer(periodSecs*1000, true)
        {
            totalSize = offset+len;
        }
        ~CProgressStream()
        {
            offset_t pos = stream->tell();
            if (pos == totalSize)
                log(100.0);
        }
        // implements ISimpleReadStream
        virtual size32_t read(size32_t len, void * data) override
        {
            size32_t rd = stream->read(len, data);
            logProgressIfNeeded();
            return rd;
        }
        virtual void skip(size32_t sz) override
        {
            stream->skip(sz);
            logProgressIfNeeded();
        }
        virtual void get(size32_t len, void * ptr) override
        {
            stream->get(len, ptr);
            logProgressIfNeeded();
        }
        virtual void reset(offset_t offset, offset_t len) override
        {
            stream->reset(offset, len);
        }
        virtual offset_t tell() const override
        {
            return stream->tell();
        }
    };
    return new CProgressStream(stream, offset, len, msg, periodSecs);
}



/*
 * Future work:
 *
 * Add an intercept input/output
 * Add support for parital compression into a fixed block size (including) zero padding the rest of the block if too small
 */
