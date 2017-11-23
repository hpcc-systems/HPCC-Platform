#ifndef ECLCOMMON_HPP
#define ECLCOMMON_HPP


#include "jiface.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"

//The CThorContiguousRowBuffer is the source for a readAhead call to ensure the entire row
//is in a contiguous block of memory.  The read() and skip() functions must be implemented
class ECLRTL_API CThorContiguousRowBuffer : implements IRowPrefetcherSource
{
public:
    CThorContiguousRowBuffer() {};
    CThorContiguousRowBuffer(ISerialStream * _in);

    inline void setStream(ISerialStream *_in) { in = _in; maxOffset = 0; readOffset = 0; }

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

    virtual const byte * querySelf() override;
    virtual void noteStartChild() override;
    virtual void noteFinishChild() override;

    inline bool eos()
    {
        return in->eos();
    }

    inline offset_t tell() const
    {
        return in->tell();
    }

    inline void clearStream()
    {
        in = nullptr;
        maxOffset = 0;
        readOffset = 0;
    }

    inline const byte * queryRow() const { return buffer; }
    inline size32_t queryRowSize() const { return readOffset; }
    inline void finishedRow()
    {
        if (readOffset)
            in->skip(readOffset);
        maxOffset = 0;
        readOffset = 0;
    }

    inline void reset(offset_t offset, offset_t flen = (offset_t)-1)
    {
        in->reset(offset, flen);
        buffer = nullptr;
        maxOffset = 0;
        readOffset = 0;
    }


protected:
    size32_t sizePackedInt();
    size32_t sizeUtf8(size32_t len);
    size32_t sizeVStr();
    size32_t sizeVUni();
    void reportReadFail();

private:
    inline void doPeek(size32_t maxSize)
    {
        buffer = static_cast<const byte *>(in->peek(maxSize, maxOffset));
    }

    void doRead(size32_t len, void * ptr);

    inline void ensureAccessible(size32_t required)
    {
        if (required > maxOffset)
        {
            doPeek(required);
            assertex(required <= maxOffset);
        }
    }

protected:
    ISerialStream* in = nullptr;
    const byte * buffer = nullptr;
    size32_t maxOffset = 0;
    size32_t readOffset = 0;
    UnsignedArray childStartOffsets;
};

#endif
