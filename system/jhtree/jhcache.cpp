/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef __linux__
#include <alloca.h>
#endif
#include <algorithm>

#include "jmisc.hpp"
#include "jset.hpp"
#include "hlzw.h"

#include "ctfile.hpp"
#include "jhinplace.hpp"
#include "jstats.h"
#include "jptree.hpp"
#include "jhcache.hpp"
#include "jutil.hpp"

byte * CCachedIndexRead::getBufferForUpdate(offset_t offset, size32_t writeSize)
{
    baseOffset = offset;
    size = writeSize;
    void * base = data.ensure(writeSize);
    return static_cast<byte *>(base);
}

const byte * CCachedIndexRead::queryBuffer(offset_t offset, size32_t readSize) const
{
    if ((offset < baseOffset) || (offset + readSize > baseOffset + size))
        return nullptr;
    return static_cast<const byte *>(data.get()) + (offset - baseOffset);
}

//--------------------------------------------------------------------------------------------------------------------

// A trivial (single page) cache as a demonstration of the interface
class CDemoPageCache final : public CInterfaceOf<IPageCache>
{
public:
    CDemoPageCache(const IPropertyTree * config)
    {
        size = config->getPropInt64("@sizeMB", 32) * 0x100000;
        pageSize = config->getPropInt("@pageSize", 0x10000);
        readSize = config->getPropInt("@readSize", pageSize);
        cacheMissPenalty = config->getPropInt("@debugCacheMissPenalty", 0);
        offsetMask = ~(offset_t)(pageSize-1);

        if ((pageSize < 8192) || !isPowerOf2(pageSize))
            throw makeStringExceptionV(0, "Invalid page size %u", pageSize);

        // If read size is smaller than the page size, and a power of 2 then pageSize must be a multiple of readSize
        if ((readSize > pageSize) || !isPowerOf2(readSize))
            throw makeStringExceptionV(0, "Invalid read size %u for page %u", readSize, pageSize);
        cache.data.allocate(pageSize);
    }

    virtual size32_t queryPageSize() const override
    {
        return pageSize;
    }

    // Called when the node has been hit in the memory cache - ensure LRU is updated
    // The offset will need to be aligned to the page boundary
    virtual void noteUsed(unsigned fileId, offset_t offset) override
    {
        CriticalBlock b(crit);
        offset_t pageOffset = offset & offsetMask;
        DBGLOG("CDemoPageCache::noteUsed (%u, %llu)", fileId, pageOffset);
    }

    // Searching for a node in the disk node cache.  Return true if found, and ensure nodeData is populated
    // if it returns false then the node is not in the cache.
    virtual bool read(unsigned fileId, offset_t offset, size32_t size, CCachedIndexRead & nodeData) override
    {
        dbgassertex(size <= pageSize);

        CriticalBlock b(crit);
        if ((fileId != cache.file) || (cache.offset == invalidOffset))
            return onCacheMiss(fileId, offset, size);

        if ((offset < cache.offset) || (offset + size > cache.offset + pageSize))
            return onCacheMiss(fileId, offset, size);

        // If the requested size to read is smaller than the recommended read size for this cache, read more.
        size32_t sizeToRead = size;
        if (sizeToRead < readSize)
            sizeToRead = readSize;

        //Read data starting at the requested offset, but if that extends beyond the page boundary, read the last
        //sizeToRead bytes from the end of the page
        size32_t offsetInPage = (size32_t)(offset - cache.offset);
        if (offsetInPage + sizeToRead > pageSize)
            offsetInPage = pageSize - sizeToRead;

        byte * data = nodeData.getBufferForUpdate(cache.offset + offsetInPage, sizeToRead);
        memcpy(data, cache.data.bytes() + offsetInPage, sizeToRead);
        DBGLOG("CDemoPageCache::read(%u, %llu, %u) - cache hit", fileId, offset, size);
        return true;
    }

    // Insert a block of data into the disk node cache.
    // If reservation is non null, then it will match the value from a previous call to readNodeOrReserve
    // If reservation is null the data is being preemptively added to the cache (e.g. at startup)
    virtual void write(unsigned fileId, offset_t offset, const byte * data) override
    {
        CriticalBlock b(crit);
        //check to see if the data is already in the cache
        if ((fileId == cache.file) && (offset == cache.offset))
        {
            // Keep a count of the number of times this occurs - if it is high we may need protection elsewhere
            DBGLOG("CDemoPageCache::write(%u, %llu) - data is already in the cache", fileId, offset);
            return;
        }

        cache.file = fileId;
        cache.offset = offset;
        memcpy(cache.data.mem(), data, pageSize);
        DBGLOG("CDemoPageCache::write(%u, %llu) - update cache contents", fileId, offset);
    }

    // Called to gather the current state of the cache - so it can be preserved for quick cache warming.
    virtual void gatherState(ICacheInfoRecorder & recorder) override
    {
        CriticalBlock b(crit);

        DBGLOG("CDemoPageCache::gatherState");
        if (cache.offset != invalidOffset)
            recorder.noteWarm(cache.file, cache.offset, pageSize, NodeNone);
    }

protected:
    bool onCacheMiss(unsigned fileId, offset_t offset, size32_t size)
    {
        DBGLOG("CDemoPageCache::read(%u, %llu, %u) - cache miss", fileId, offset, size);
        if (cacheMissPenalty)
            MilliSleep(cacheMissPenalty);
        return false;
    }

protected:
    CriticalSection crit;
    offset_t size;
    size32_t pageSize{0};
    size32_t readSize{0};
    offset_t offsetMask{0};
    unsigned cacheMissPenalty{0};

    static constexpr offset_t invalidOffset = ~(offset_t)0;
    struct
    {
        unsigned file{0};
        offset_t offset{invalidOffset};
        MemoryAttr data;
    } cache;
};

IPageCache * createDemoPageCache(const IPropertyTree * config)
{
    return new CDemoPageCache(config);
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "eclrtl.hpp"


#endif
