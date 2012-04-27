/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "thcompressutil.hpp"

#include "jlzw.hpp"

size32_t ThorCompress(const void * src, size32_t srcSz, void * dest, size32_t destSz, size32_t threshold)
{
    assertex(destSz>=srcSz+sizeof(size32_t));
    if (srcSz>=threshold) {
        Owned<ICompressor> compressor = createLZWCompressor();
        compressor->open((byte *)dest + sizeof(size32_t), srcSz * 4 / 5);
        if(compressor->write(src, srcSz)==srcSz)
        {
            compressor->close();
            memcpy(dest, &srcSz, sizeof(size32_t)); 
            return compressor->buflen() + sizeof(size32_t);
        }
    }
    memcpy((byte *)dest + sizeof(size32_t), src, srcSz);
    memset(dest, 0, sizeof(size32_t));
    return srcSz + sizeof(size32_t);
}


size32_t ThorCompress(const void * src, size32_t srcSz, MemoryBuffer & dest, size32_t threshold)
{
    size32_t prev = dest.length();
    size32_t dSz = srcSz + sizeof(size32_t);
    void * d = dest.reserve(dSz);
    size32_t ret = ThorCompress(src, srcSz, d, dSz, threshold);
    dest.setLength(prev+ret);
    return ret;
}

size32_t ThorCompress(MemoryBuffer & src, MemoryBuffer & dest, size32_t threshold)
{
    return ThorCompress((const void *)src.toByteArray(), src.length(), dest, threshold);
}

size32_t ThorExpand(const void * src, size32_t srcSz, void * dest, size32_t destSz)
{
    size32_t ret;
    memcpy(&ret, src, sizeof(size32_t));
    byte *data = (byte *)src+sizeof(size32_t);
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

size32_t ThorExpand(const void * src, size32_t srcSz, MemoryBuffer & dest)
{
    size32_t sz;
    memcpy(&sz, src, sizeof(size32_t));
    size32_t bufSz = (sz == 0) ? (srcSz-sizeof(size32_t)) : sz;
    void * buf = dest.reserve(bufSz);   
    return ThorExpand(src, srcSz, buf, bufSz); 
}

size32_t ThorExpand(MemoryBuffer & src, MemoryBuffer & dest)
{
    size32_t len = src.remaining();
    const void *pSrc = src.readDirect(len);
    return ThorExpand(pSrc, len, dest);
}


size32_t ThorExpand(const void * src, size32_t srcSz, CLargeMemoryAllocator &mem)
{
    size32_t sz;
    memcpy(&sz, src, sizeof(size32_t));
    size32_t bufSz = (sz == 0) ? (srcSz-sizeof(size32_t)) : sz;
    byte * buf = mem.alloc(bufSz);  
    return ThorExpand(src, srcSz, buf, bufSz); 
}
