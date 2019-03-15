/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include "compressutil.hpp"

#include "jlzw.hpp"

size32_t LZWCompress(const void* src, size32_t srcSz, void* dest, size32_t destSz, size32_t threshold)
{
    assertex(destSz>=srcSz+sizeof(size32_t));
    if (srcSz>=threshold)
    {
        Owned<ICompressor> compressor = createLZWCompressor();
        compressor->open((byte*)dest + sizeof(size32_t), srcSz * 4 / 5);
        if(compressor->write(src, srcSz)==srcSz)
        {
            compressor->close();
            memcpy(dest, &srcSz, sizeof(size32_t)); 
            return compressor->buflen() + sizeof(size32_t);
        }
    }
    memcpy((byte*)dest + sizeof(size32_t), src, srcSz);
    memset(dest, 0, sizeof(size32_t));
    return srcSz + sizeof(size32_t);
}


size32_t LZWCompress(const void* src, size32_t srcSz, MemoryBuffer& dest, size32_t threshold)
{
    size32_t prev = dest.length();
    size32_t dSz = srcSz + sizeof(size32_t);
    void* d = dest.reserve(dSz);
    size32_t ret = LZWCompress(src, srcSz, d, dSz, threshold);
    dest.setLength(prev+ret);
    return ret;
}

size32_t LZWCompress(const void* src, size32_t srcSz, StringBuffer& dest, size32_t threshold)
{
    size32_t prev = dest.length();
    size32_t dSz = srcSz + sizeof(size32_t);
    void* d = dest.reserve(dSz);
    size32_t ret = LZWCompress(src, srcSz, d, dSz, threshold);
    dest.setLength(prev+ret);
    return ret;
}

size32_t LZWCompress(MemoryBuffer& src, MemoryBuffer& dest, size32_t threshold)
{
    return LZWCompress((const void*)src.toByteArray(), src.length(), dest, threshold);
}

size32_t LZWExpand(const void* src, size32_t srcSz, void* dest, size32_t destSz)
{
    size32_t ret;
    memcpy(&ret, src, sizeof(size32_t));
    byte* data = (byte*)src+sizeof(size32_t);
    if(ret)     // compressed
    {
        Owned<IExpander> expander = createLZWExpander();
        assertex(destSz >= ret);
        expander->init(data);
        expander->expand(dest);
    }
    else
    {
        ret = srcSz - sizeof(size32_t);
        assertex(destSz >= ret);
        memcpy(dest, data, ret);
    }
    return ret;
}

size32_t LZWExpand(const void* src, size32_t srcSz, StringBuffer& dest)
{
    size32_t sz;
    memcpy(&sz, src, sizeof(size32_t));
    size32_t bufSz = (sz == 0) ? (srcSz-sizeof(size32_t)) : sz;
    void* buf = dest.reserve(bufSz);
    return LZWExpand(src, srcSz, buf, bufSz); 
}

size32_t LZWExpand(const void* src, size32_t srcSz, MemoryBuffer& dest)
{
    size32_t sz;
    memcpy(&sz, src, sizeof(size32_t));
    size32_t bufSz = (sz == 0) ? (srcSz-sizeof(size32_t)) : sz;
    void* buf = dest.reserve(bufSz);
    return LZWExpand(src, srcSz, buf, bufSz); 
}

size32_t LZWExpand(MemoryBuffer& src, MemoryBuffer& dest)
{
    size32_t len = src.remaining();
    const void* pSrc = src.readDirect(len);
    return LZWExpand(pSrc, len, dest);
}


size32_t LZWExpend(const void* src, size32_t srcSz, CLargeMemoryAllocator& mem)
{
    size32_t sz;
    memcpy(&sz, src, sizeof(size32_t));
    size32_t bufSz = (sz == 0) ? (srcSz-sizeof(size32_t)) : sz;
    byte* buf = mem.alloc(bufSz);
    return LZWExpand(src, srcSz, buf, bufSz); 
}
