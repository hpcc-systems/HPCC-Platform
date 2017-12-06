#include "jiface.hpp"
#include "jbuff.hpp"
#include "jstring.hpp"
#include "junicode.hpp"
#include "rtlcommon.hpp"

CThorContiguousRowBuffer::CThorContiguousRowBuffer(ISerialStream * _in) : in(_in)
{
    buffer = NULL;
    maxOffset = 0;
    readOffset = 0;
}

void CThorContiguousRowBuffer::doRead(size32_t len, void * ptr)
{
    ensureAccessible(readOffset + len);
    memcpy(ptr, buffer+readOffset, len);
    readOffset += len;
}


size32_t CThorContiguousRowBuffer::read(size32_t len, void * ptr)
{
    doRead(len, ptr);
    return len;
}

size32_t CThorContiguousRowBuffer::readSize()
{
    size32_t value;
    doRead(sizeof(value), &value);
    return value;
}

size32_t CThorContiguousRowBuffer::readPackedInt(void * ptr)
{
    size32_t size = sizePackedInt();
    doRead(size, ptr);
    return size;
}

size32_t CThorContiguousRowBuffer::readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len)
{
    if (len == 0)
        return 0;

    size32_t size = sizeUtf8(len);
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVStr();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVUni();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}


size32_t CThorContiguousRowBuffer::sizePackedInt()
{
    ensureAccessible(readOffset+1);
    return rtlGetPackedSizeFromFirst(buffer[readOffset]);
}

size32_t CThorContiguousRowBuffer::sizeUtf8(size32_t len)
{
    if (len == 0)
        return 0;

    //The len is the number of utf characters, size depends on which characters are included.
    size32_t nextOffset = readOffset;
    while (len)
    {
        ensureAccessible(nextOffset+1);

        for (;nextOffset < maxOffset;)
        {
            nextOffset += readUtf8Size(buffer+nextOffset);  // This function only accesses the first byte
            if (--len == 0)
                break;
        }
    }
    return nextOffset - readOffset;
}

size32_t CThorContiguousRowBuffer::sizeVStr()
{
    size32_t nextOffset = readOffset;
    for (;;)
    {
        ensureAccessible(nextOffset+1);

        for (; nextOffset < maxOffset; nextOffset++)
        {
            if (buffer[nextOffset] == 0)
                return (nextOffset + 1) - readOffset;
        }
    }
}

size32_t CThorContiguousRowBuffer::sizeVUni()
{
    size32_t nextOffset = readOffset;
    const size32_t sizeOfUChar = 2;
    for (;;)
    {
        ensureAccessible(nextOffset+sizeOfUChar);

        for (; nextOffset+1 < maxOffset; nextOffset += sizeOfUChar)
        {
            if (buffer[nextOffset] == 0 && buffer[nextOffset+1] == 0)
                return (nextOffset + sizeOfUChar) - readOffset;
        }
    }
}


void CThorContiguousRowBuffer::reportReadFail()
{
    throwUnexpected();
}


const byte * CThorContiguousRowBuffer::peek(size32_t maxSize)
{
    if (maxSize+readOffset > maxOffset)
        doPeek(maxSize+readOffset);
    return buffer + readOffset;
}

offset_t CThorContiguousRowBuffer::beginNested()
{
    size32_t len = readSize();
    //Currently nested datasets are readahead by skipping the number of bytes in the datasets, rather than calling
    //beginNested(). If this function was ever called from readAhead() then it would need to call noteStartChild()
    //so that the self pointer is correct for the child rows
    return len+readOffset;
}

bool CThorContiguousRowBuffer::finishedNested(offset_t & endPos)
{
    //See note above, if this was ever called from readAhead() then it would need to call noteFinishChild() and noteStartChild() if incomplete;
    return readOffset >= endPos;
}

void CThorContiguousRowBuffer::skip(size32_t size)
{
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipPackedInt()
{
    size32_t size = sizePackedInt();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipUtf8(size32_t len)
{
    size32_t size = sizeUtf8(len);
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVStr()
{
    size32_t size = sizeVStr();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVUni()
{
    size32_t size = sizeVUni();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

const byte * CThorContiguousRowBuffer::querySelf()
{
    if (maxOffset == 0)
        doPeek(0);
    if (childStartOffsets.ordinality())
        return buffer + childStartOffsets.tos();
    return buffer;
}

void CThorContiguousRowBuffer::noteStartChild()
{
    childStartOffsets.append(readOffset);
}

void CThorContiguousRowBuffer::noteFinishChild()
{
    childStartOffsets.pop();
}
