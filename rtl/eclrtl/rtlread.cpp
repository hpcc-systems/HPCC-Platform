/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jliball.hpp"
#include "junicode.hpp"

#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlread_imp.hpp"

//=====================================================================================================

CThorStreamDeserializerSource::CThorStreamDeserializerSource(ISerialStream * _in) : in(_in)
{
}

CThorStreamDeserializerSource::CThorStreamDeserializerSource(size32_t len, const void * data)
{
    in.setown(createMemorySerialStream(data, len, NULL));
}

size32_t CThorStreamDeserializerSource::read(size32_t len, void * ptr)
{
    doRead(len, ptr);
    return len;
}

size32_t CThorStreamDeserializerSource::readSize()
{
    size32_t value;
    doRead(sizeof(value), &value);
    return value;
}

const byte * CThorStreamDeserializerSource::peek(size32_t maxSize)
{
    size32_t available;
    //Alien datatypes.  Unsafe, but can't check that a certain number have been read.
    return static_cast<const byte *>(in->peek(maxSize, available));
}

offset_t CThorStreamDeserializerSource::beginNested()
{
    size32_t len;
    doRead(sizeof(len), &len);
    return len+in->tell();
}

bool CThorStreamDeserializerSource::finishedNested(offset_t & endPos)
{
    return in->tell() >= endPos;
}

size32_t CThorStreamDeserializerSource::readPackedInt(void * ptr)
{
    size32_t available;
    const byte * temp = doPeek(1, available);
    size32_t size = rtlGetPackedSizeFromFirst(*temp);
    doRead(size, ptr);
    return size;
}

size32_t CThorStreamDeserializerSource::readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len)
{
    //The len is the number of utf characters, size depends on which characters are included.
    size32_t totalSize = 0;
    loop
    {
        if (len == 0)
            return totalSize;

        size32_t available;
        const byte * cur = doPeek(1, available);

        size32_t copyLen;
        for (copyLen = 0; copyLen < available;)
        {
            copyLen += readUtf8Size(cur+copyLen);   // This function only accesses the first byte
            if (--len == 0)
                break;
        }

        byte * self = target.ensureCapacity(fixedSize + totalSize + copyLen, NULL);
        doRead(copyLen, self+offset+totalSize);
        totalSize += copyLen;
    }
}

size32_t CThorStreamDeserializerSource::readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t totalSize = 0;
    loop
    {
        size32_t available;
        const byte * cur = doPeek(1, available);

        const byte * end = static_cast<const byte *>(memchr(cur, 0, available));
        size32_t copyLen = end ? (end+1) - cur : available;

        byte * self = target.ensureCapacity(fixedSize + totalSize + copyLen, NULL);
        doRead(copyLen, self+offset+totalSize);
        totalSize += copyLen;
        if (end)
            return totalSize;
    }
}

size32_t CThorStreamDeserializerSource::readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t totalSize = 0;
    bool done = false;
    loop
    {
        size32_t available;
        const byte * cur = doPeek(2, available);

        size32_t copyLen = 0;
        for (copyLen = 0; copyLen+1 < available; copyLen+=2)
        {
            if (cur[copyLen] == 0 && cur[copyLen+1] == 0)
            {
                copyLen += 2;
                done = true;
                break;
            }
        }

        byte * self = target.ensureCapacity(fixedSize + totalSize + copyLen, NULL);
        doRead(copyLen, self+offset+totalSize);
        totalSize += copyLen;
        if (done)
            return totalSize;
    }
}


void CThorStreamDeserializerSource::reportReadFail()
{
    rtlSysFail(1, "Premature end of input, more data expected");
}


void CThorStreamDeserializerSource::skip(size32_t size)
{
    in->skip(size);
}
    
void CThorStreamDeserializerSource::skipPackedInt()
{
    throwUnexpected();
    size32_t available;
    const byte * temp = doPeek(1, available);
    size32_t size = rtlGetPackedSizeFromFirst(*temp);
    in->skip(size);
}

void CThorStreamDeserializerSource::skipUtf8(size32_t len)
{
    throwUnexpected();
    loop
    {
        if (len == 0)
            return;

        size32_t available;
        const byte * cur = doPeek(1, available);

        size32_t copyLen;
        for (copyLen = 0; copyLen < available;)
        {
            copyLen += readUtf8Size(cur+copyLen);   // This function only accesses the first byte
            len--;
        }

        in->skip(copyLen);
    }
}

void CThorStreamDeserializerSource::skipVStr()
{
    throwUnexpected();
}

void CThorStreamDeserializerSource::skipVUni()
{
    throwUnexpected();
}

