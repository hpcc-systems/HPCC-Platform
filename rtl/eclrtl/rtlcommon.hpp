#ifndef ECLCOMMON_HPP
#define ECLCOMMON_HPP


#include "jiface.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"

//The CContiguousRowBuffer is a buffer used for reading ahead into a file, and unsuring there is a contiguous
//block of data available to the reader.  Fixed size files could use this directly.
class ECLRTL_API CContiguousRowBuffer
{
public:
    CContiguousRowBuffer() = default;
    CContiguousRowBuffer(ISerialStream * _in);

    void setStream(ISerialStream *_in);
    const byte * peekBytes(size32_t maxSize);
    const byte * peekFirstByte();
    void skipBytes(size32_t size)
    {
        cur += size;
        available -= size;
        // call eos() to ensure stream->eos() is true if this class has got to the end of the stream
        eos();
    }

    inline bool eos()
    {
        if (likely(available))
            return false;
        return checkInputEos();
    }
    inline offset_t tell() const   { return in->tell() + (cur - buffer); }
    inline const byte * queryRow() const { return cur; }
    inline size_t maxAvailable() const { return available; }
    inline void clearStream()      { setStream(nullptr); }

    inline void reset(offset_t offset, offset_t flen = (offset_t)-1)
    {
        in->reset(offset, flen);
        clearBuffer();
    }

protected:
    void peekBytesDirect(size32_t size); // skip any consumed data and directly peek bytes from the input

private:
    bool checkInputEos();
    void clearBuffer()
    {
        buffer = nullptr;
        cur = nullptr;
        available = 0;
    }

protected:
    const byte * cur = nullptr;
private:
    ISerialStream* in = nullptr;
    const byte * buffer = nullptr;
    size32_t available = 0;
};


//The CThorContiguousRowBuffer is the source for a readAhead call to ensure the entire row
//is in a contiguous block of memory.  The read() and skip() functions must be implemented
class ECLRTL_API CThorContiguousRowBuffer : public CContiguousRowBuffer, implements IRowPrefetcherSource
{
public:
    CThorContiguousRowBuffer() = default;
    CThorContiguousRowBuffer(ISerialStream * _in);

    inline void setStream(ISerialStream *_in)
    {
        CContiguousRowBuffer::setStream(_in);
        readOffset = 0;
    }

    virtual const byte * peek(size32_t maxSize) override;
    virtual offset_t beginNested() override;
    virtual bool finishedNested(offset_t & len) override;

    virtual size32_t read(size32_t len, void * ptr) override;
    virtual size32_t readSize() override;
    virtual size32_t readPackedInt(void * ptr) override;
    virtual size32_t readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len) override;
    virtual size32_t readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize) override;
    virtual size32_t readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize) override;

    //The following functions should only really be called when used by the readAhead() function
    virtual void skip(size32_t size) override;
    virtual void skipPackedInt() override;
    virtual void skipUtf8(size32_t len) override;
    virtual void skipVStr() override;
    virtual void skipVUni() override;

    virtual const byte * querySelf() override;      // Dubious - used from ifblocks
    virtual void noteStartChild() override;
    virtual void noteFinishChild() override;

    inline const byte * queryRow() const { return cur; }
    inline size32_t queryRowSize() const { return readOffset; }
    inline void finishedRow()
    {
        skipBytes(readOffset);
        readOffset = 0;
    }

    inline void reset(offset_t offset, offset_t flen = (offset_t)-1)
    {
        CContiguousRowBuffer::reset(offset, flen);
        readOffset = 0;
    }

protected:
    size32_t sizePackedInt();
    size32_t sizeUtf8(size32_t len);
    size32_t sizeVStr();
    size32_t sizeVUni();
    void reportReadFail();

private:

    void doRead(size32_t len, void * ptr);

    inline void ensureAccessible(size32_t required)
    {
        if (required > maxAvailable())
        {
            peekBytesDirect(required);
            assertex(required <= maxAvailable());
        }
    }

protected:
    size32_t readOffset = 0;            // Offset within the current row
    UnsignedArray childStartOffsets;
};

#endif
