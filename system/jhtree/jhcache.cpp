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
#include <sys/syscall.h>
#include <sys/uio.h>
#endif
#include <algorithm>

#include <cmath>
#include <deque>

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

static unsigned cache_index(offset_t cacheKey, unsigned numCacheEntries)
{
    // murmur64 bit mix modified to use mix13 parameters
    cacheKey ^= (cacheKey >> 30);
    cacheKey *= 0xbf58476d1ce4e5b9;
    cacheKey ^= (cacheKey >> 27);
    cacheKey *= 0x94d049bb133111eb;
    cacheKey ^= (cacheKey >> 31);
    return (cacheKey % numCacheEntries);
}

bool isPrime(unsigned x)
{
    if (x <= 1)
        return false;
    else if (x == 2)
        return true;
    else if (x % 2 == 0)
        return false;

    unsigned t = sqrt(x) + 1;
    unsigned i;
    for (i=3; i<=t; i+= 2)
    {
        if ((x % i) == 0)
            return false;
    }
    return true;
}

unsigned largestPrime(unsigned x)
{
    if (x <= 1)
        return 0;
    unsigned ix;
    for (ix=x-1; ix>=1; ix--)
    {
        if (isPrime(ix))
            return ix;
    }
    return 0;
}

//--------------------------------------------------------------------------------------------------------------------

class CMyPageCache final : public CInterfaceOf<IPageCache>
{
public:
    CMyPageCache(const IPropertyTree * config)
    {
#ifdef __linux__
        totSize = config->getPropInt64("@sizeMB", 100) * 0x100000;
        pageSize = config->getPropInt("@pageSize", 0x10000);
        readSize = config->getPropInt("@readSize", pageSize);
        numSets = config->getPropInt("@numSets", 16);
        cacheMissPenalty = config->getPropInt("@debugCacheMissPenalty", 0);
        offsetMask = ~(offset_t)(pageSize - 1);

        if ((pageSize < 8192) || !isPowerOf2(pageSize))
            throw makeStringExceptionV(0, "Invalid page size %u", pageSize);

        // If read size is smaller than the page size, and a power of 2 then pageSize must be a multiple of readSize
        // TODO: MCK - ???
        if ((readSize > pageSize) || !isPowerOf2(readSize))
            throw makeStringExceptionV(0, "Invalid read size %u for page %u", readSize, pageSize);

        if (numSets < 1)
            numSets = 1;
        if (numSets > 16)
            numSets = 16;

        DBGLOG("mck: cache pageSize: %u", pageSize);
        DBGLOG("mck: cache readSize: %u", readSize);
        DBGLOG("mck: cache num sets: %d", numSets);

        // adjust size so that number of entries is the largest possible prime ...

        numEntries = largestPrime((totSize / pageSize) / numSets);
        if (numEntries < 1)
            numEntries = 1;
        setSize = numEntries * pageSize;
        totSize = setSize * numSets;

        DBGLOG("mck: cache file size: %llu", totSize);
        DBGLOG("mck: cache set size: %llu", setSize);
        DBGLOG("mck: cache num entries per set: %u", numEntries);

        int openFlags = O_CREAT | O_RDWR;

        useDirect = false;
#if 0 // enable for testing ...
        bool tryDirect = false;
        if (config->queryProp("@directIO"))
            tryDirect = config->getPropBool("@directIO");
        if (tryDirect)
        {
            if ((pageSize % 4096) == 0)
            {
                int rc = posix_memalign((void **)&alignedAddr, pageSize, pageSize);
                if (rc == 0)
                {
                    useDirect = true;
                    openFlags |= O_DIRECT;
                }
            }
        }
#else
# ifndef RWF_UNCACHED
        if ((pageSize % 4096) == 0)
        {
            int rc = posix_memalign((void **)&alignedAddr, pageSize, pageSize);
            if (rc == 0)
            {
                useDirect = true;
                openFlags |= O_DIRECT;
            }
        }
        if (useDirect)
            DBGLOG("mck: disk page cache using O_DIRECT");
        else
            DBGLOG("mck: disk page cache using normal i/o");
# else
            DBGLOG("mck: disk page cache using RWF_UNCACHED");
# endif
#endif

        const char *cacheFile = config->queryProp("@file");
        if (!cacheFile)
            cacheFile = "/tmp/roxiecache.tmp";

        cacheFd = open(cacheFile, openFlags, 0644);
        if (cacheFd < 0)
        {
            DBGLOG("Error opening pagecache file %s, errno %d", cacheFile, errno);
            // disable cache
            totSize = 0;
        }

        int rc = posix_fallocate(cacheFd, 0ULL, totSize);
        if (rc != 0)
        {
            DBGLOG("Error allocating pagecache file %s, errno %d", cacheFile, rc);
            // disable cache
            totSize = 0;
        }

        posix_fadvise(cacheFd, 0ULL, totSize, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);

        // not quite ...
        // DBGLOG("mck: cache total mem size: %lu", numEntries * (sizeof(std::deque<uint8_t>) + (numSets * sizeof(std::vector<CacheSet>))));

        cache = new CacheEntry [numEntries];
        for (unsigned i=0; i<numEntries; i++)
        {
            cache[i].cSet.reserve(numSets);
            for (uint8_t j=0; j<numSets; j++)
            {
                cache[i].lru.push_back(j);
                cache[i].cSet.push_back(CacheSet());
                // cache[i].cSet.push_back( { invalidOffset, invalidFileId } );
            }
        }
#endif
    }

    virtual size32_t queryPageSize() const override
    {
        return pageSize;
    }

    virtual std::deque<uint8_t>::iterator findEntry(unsigned fileId, unsigned findex, offset_t alignedPos)
    {
        for (uint8_t ix=0; ix<numSets; ix++)
        {
            if ( (cache[findex].cSet[ix].fileId == fileId) && (cache[findex].cSet[ix].offset == alignedPos) )
            {
                std::deque<uint8_t>::iterator it;
                for (it = cache[findex].lru.begin(); it != cache[findex].lru.end(); ++it)
                {
                    if (*it == ix)
                        return it;
                }
            }
        }
        return cache[findex].lru.end();
    }

    // Called when the node has been hit in the memory cache - ensure LRU is updated
    // The offset will need to be aligned to the page boundary
    virtual void noteUsed(unsigned fileId, offset_t offset) override
    {
#ifdef __linux__
        if ( (totSize == 0) || (fileId == invalidFileId) || (offset == invalidOffset) )
            return;

        offset_t alignedPos = offset & offsetMask;

        unsigned fid = fileId & 0xffffff;
        offset_t oid = alignedPos & 0xffffffffff;
        offset_t key = fid | (oid << 24);

        unsigned findex = cache_index(key, numEntries);

        CriticalBlock b(crit);

        std::deque<uint8_t>::iterator it = findEntry(fileId, findex, alignedPos);

        if (it != cache[findex].lru.end())
        {
            uint8_t ix = *it;
            cache[findex].lru.erase(it);
            cache[findex].lru.push_back(ix);
        }
#endif
    }

    // Searching for a node in the disk node cache.  Return true if found, and ensure nodeData is populated
    // if it returns false then the node is not in the cache.
    virtual bool read(unsigned fileId, offset_t offset, size32_t size, CCachedIndexRead & nodeData) override
    {
#ifdef __linux__
        dbgassertex(size <= pageSize);

        if ( (totSize == 0) || (fileId == invalidFileId) || (offset == invalidOffset) )
            return onCacheMiss(fileId, offset, size);

        offset_t alignedPos = offset & offsetMask;

        unsigned fid = fileId & 0xffffff;
        offset_t oid = alignedPos & 0xffffffffff;
        offset_t key = fid | (oid << 24);
        // offset_t keyCantor = (fid + oid) * (fid + oid + 1);

        unsigned findex = cache_index(key, numEntries);

        CriticalBlock b(crit);

        std::deque<uint8_t>::iterator it = findEntry(fileId, findex, alignedPos);

        if (it == cache[findex].lru.end())
            return onCacheMiss(fileId, offset, size);

        uint8_t ix = *it;
        offset_t cacheFileOffset = (ix * setSize) + (findex * pageSize);

        // TODO: MCK - handle if readSize != pageSize ...

        byte * data = nodeData.getBufferForUpdate(alignedPos, pageSize);

#ifdef __NR_preadv2
        struct iovec iov[1];
        iov[0].iov_len = pageSize;

        int pFlags = 0;
        if (useDirect)
        {
            iov[0].iov_base = alignedAddr;
#ifdef RWF_HIPRI
            pFlags |= RWF_HIPRI;
#endif
        }
        else
        {
            iov[0].iov_base = data;
#ifdef RWF_UNCACHED
            pFlags |= RWF_UNCACHED;
#endif
        }

        int rc = preadv2(cacheFd, iov, 1, cacheFileOffset, pFlags);
#else
        int rc;
        if (useDirect)
            rc = pread64(cacheFd, alignedAddr, pageSize, cacheFileOffset);
        else
            rc = pread64(cacheFd, data, pageSize, cacheFileOffset);
#endif

        cache[findex].lru.erase(it);

        if (rc == pageSize)
        {
            if (useDirect)
                memcpy(data, alignedAddr, pageSize);
            cache[findex].lru.push_back(ix);
            // hit++
            return true;
        }

        cache[findex].cSet[ix].fileId = invalidFileId;
        cache[findex].cSet[ix].offset = invalidOffset;
        cache[findex].lru.push_front(ix);
#endif
        return onCacheMiss(fileId, offset, size);
    }

    // Insert a block of data into the disk node cache.
    // If reservation is non null, then it will match the value from a previous call to readNodeOrReserve
    // If reservation is null the data is being preemptively added to the cache (e.g. at startup)
    virtual void write(unsigned fileId, offset_t alignedPos, const byte * data) override
    {
#ifdef __linux__
        if ( (totSize == 0) || (fileId == invalidFileId) || (alignedPos == invalidOffset) )
            return;

        unsigned fid = fileId & 0xffffff;
        offset_t oid = alignedPos & 0xffffffffff;
        offset_t key = fid | (oid << 24);

        unsigned findex = cache_index(key, numEntries);

        CriticalBlock b(crit);

        uint8_t ix = cache[findex].lru.front();

        cache[findex].cSet[ix].fileId = fileId;
        cache[findex].cSet[ix].offset = alignedPos;

        offset_t cacheFileOffset = (ix * setSize) + (findex * pageSize);

        if (useDirect)
            memcpy(alignedAddr, data, pageSize);

#ifdef __NR_pwritev2
        struct iovec iov[1];
        iov[0].iov_len = pageSize;

        int pFlags = 0;
        if (useDirect)
        {
            iov[0].iov_base = alignedAddr;
#ifdef RWF_HIPRI
            pFlags |= RWF_HIPRI;
#endif
        }
        else
        {
            iov[0].iov_base = (void *)data;
#ifdef RWF_UNCACHED
            pFlags |= RWF_UNCACHED;
#endif
        }

        int rc = pwritev2(cacheFd, iov, 1, cacheFileOffset, pFlags);
#else
        int rc;
        if (useDirect)
            rc = pwrite64(cacheFd, alignedAddr, pageSize, cacheFileOffset);
        else
            rc = pwrite64(cacheFd, data, pageSize, cacheFileOffset);
#endif

        if (rc == pageSize)
        {
            cache[findex].lru.pop_front();
            cache[findex].lru.push_back(ix);
            return;
        }

        cache[findex].cSet[ix].fileId = invalidFileId;
        cache[findex].cSet[ix].offset = invalidOffset;
        // ix is at front ...
#endif
    }

    // Called to gather the current state of the cache - so it can be preserved for quick cache warming.
    virtual void gatherState(ICacheInfoRecorder & recorder) override
    {
        CriticalBlock b(crit);
        // TODO: MCK ...
    }

protected:
    bool onCacheMiss(unsigned fileId, offset_t offset, size32_t size)
    {
        if (cacheMissPenalty)
            MilliSleep(cacheMissPenalty);
        // miss++
        return false;
    }

protected:
    CriticalSection crit;
    offset_t totSize{0};
    offset_t setSize{0};
    size32_t pageSize{0};
    size32_t readSize{0};
    offset_t offsetMask{0};
    uint64_t hit{0};
    uint64_t miss{0};
    unsigned cacheMissPenalty{0};
    unsigned numEntries{1};
    int numSets{0};
    int cacheFd{-1};
    byte *alignedAddr{nullptr};
    bool useDirect{false};

    static constexpr offset_t invalidOffset = ~(offset_t)0;
    static constexpr unsigned invalidFileId = ~(unsigned)0;
    struct CacheSet
    {
        offset_t offset{invalidOffset};
        unsigned fileId{invalidFileId};
    };
    struct CacheEntry
    {
        std::deque<uint8_t> lru;
        std::vector<CacheSet> cSet;
    };
    CacheEntry *cache;
};

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

IPageCache * createMyPageCache(const IPropertyTree * config)
{
    return new CMyPageCache(config);
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "eclrtl.hpp"


#endif
