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
#include <strings.h>
#include <alloca.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/file.h>
#endif
#include <algorithm>

#include <cmath>

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

#ifdef __linux__
static unsigned cacheIndex(offset_t cacheVal, unsigned numCacheEntries)
{
    // murmur64 bit mix modified to use mix13 parameters
    cacheVal ^= (cacheVal >> 30);
    cacheVal *= 0xbf58476d1ce4e5b9;
    cacheVal ^= (cacheVal >> 27);
    cacheVal *= 0x94d049bb133111eb;
    cacheVal ^= (cacheVal >> 31);
    return cacheVal % numCacheEntries;
}

static bool isPrime(unsigned x)
{
    if (x <= 1)
        return false;
    else if (x == 2)
        return true;
    else if (x % 2 == 0)
        return false;

    unsigned t = std::sqrt(x) + 1;
    unsigned i;
    for (i=3; i<=t; i+=2)
    {
        if ((x % i) == 0)
            return false;
    }
    return true;
}

static unsigned largestPrime(unsigned x)
{
    if (x <= 1)
        return 0;
    unsigned ix;
    for (ix=x; ix>=1; ix--)
    {
        if (isPrime(ix))
            return ix;
    }
    return 0;
}
#endif

enum IOMethod { NORMAL, UNCACHED, DIRECT };

//--------------------------------------------------------------------------------------------------------------------

class CDiskPageCache final : public CInterfaceOf<IPageCache>
{
public:
    CDiskPageCache(const IPropertyTree * config)
    {
#ifdef __linux__
        totSize = config->getPropInt64("@sizeMB", 100) * 0x100000;
        pageSize = config->getPropInt("@pageSize", 0x10000);
        readSize = config->getPropInt("@readSize", pageSize);
        cacheMissPenalty = config->getPropInt("@debugCacheMissPenalty", 0);

        if ( (config->getPropBool("@disable", false)) || (totSize == 0) )
            return;

        // throws means roxie will not start ...
        if ((pageSize < 8192) || !isPowerOf2(pageSize))
            throw makeStringExceptionV(0, "Disk pageCache invalid page size %u", pageSize);

        // If read size is smaller than the page size, and a power of 2 then pageSize must be a multiple of readSize
        if ((readSize > pageSize) || !isPowerOf2(readSize))
            throw makeStringExceptionV(0, "Disk pageCache invalid read size %u for page %u", readSize, pageSize);

        pageOffsetMask = ~(offset_t)(pageSize - 1);
        readOffsetMask = ~(offset_t)(readSize - 1);
        pageSizeExp = ffs(pageSize) - 1;

        // adjust size so that number of entries is the largest possible prime ...

        numEntries = largestPrime((totSize / pageSize) / numSets);
        if (numEntries < 1)
            numEntries = 1;
        setSize = (offset_t)numEntries * (offset_t)pageSize;
        totSize = setSize * numSets;

        int rc = 0;
        int openFlags = O_CREAT | O_RDWR;

        ioMethod = NORMAL;
        const char *ioMethodStr = config->queryProp("@ioMethod");
        if (ioMethodStr)
        {
            if ( (strisame(ioMethodStr, "direct")) && ((readSize % 4096) == 0) )
            {
                // same as fCrit ...
                rc = posix_memalign((void **)&alignedAddr, 4096, numCrits * pageSize);
                if (rc == 0)
                {
                    ioMethod = DIRECT;
                    openFlags |= O_DIRECT;
                }
                else
                    DBGLOG("Error allocating aligned buffer, disabling direct i/o, errno %d", rc);
            }
#ifdef RWF_UNCACHED
            else if (strisame(ioMethodStr, "uncached"))
                ioMethod = UNCACHED;
#endif
        }

        // should we check config for some HPCC dir ?
        const char *cacheFile = config->queryProp("@file");
        if (!cacheFile)
            cacheFile = "/tmp/roxiecache.tmp";

        rc = openPageCacheFile(cacheFile, openFlags);
        if (rc < 0)
            return;

        cache = new CacheEntry [numEntries];

        DBGLOG("Disk pageCache readSize: %u pageSize %u", readSize, pageSize);
        DBGLOG("Disk pageCache num sets: %d file size: %llu MB critsecs: %d", numSets, totSize / 1048576ULL, numCrits);

        StringBuffer ioMsg("Disk pageCache i/o method: ");
        if (ioMethod == NORMAL)
            ioMsg.append("normal");
        else if (ioMethod == DIRECT)
            ioMsg.append("direct");
        else if (ioMethod == UNCACHED)
            ioMsg.append("uncached");
        DBGLOG("%s", ioMsg.str());

        size_t directMem = 0;
        if (ioMethod == DIRECT)
            directMem = numCrits * pageSize;
        size_t totMem = (sizeof(CDiskPageCache) + directMem + (numEntries * sizeof(CacheEntry))) / 1024;
        DBGLOG("Disk pageCache memory size: %zu + %zu = %zu KB", sizeof(CDiskPageCache) + directMem, (numEntries * sizeof(CacheEntry)), totMem);

        cacheOK = true;
#endif
    }

    // NOTE: dtor/Release not called because MODULE_EXIT not called at _exit()
    virtual ~CDiskPageCache()
    {
        if (cacheFd >= 0)
        {
            close(cacheFd);
            cacheFd = -1;
        }
        if (alignedAddr)
        {
            free(alignedAddr);
            alignedAddr = nullptr;
        }
        if (cache)
        {
            delete[] cache;
            cache = nullptr;
        }
    }

#ifdef __linux__
    int openPageCacheFile(const char *cacheFile, unsigned openFlags)
    {
        cacheFd = open(cacheFile, openFlags, 0600);
        if (cacheFd < 0)
        {
            DBGLOG("Error opening disk page cache file %s, errno %d", cacheFile, errno);
            return -1;
        }

        // prevent another HPCC process from accidentally using same file ...
        int rc = flock(cacheFd, LOCK_EX | LOCK_NB);
        if (rc != 0)
        {
            DBGLOG("Error locking disk page cache file %s, errno %d", cacheFile, errno);
            return -1;
        }

        rc = posix_fallocate(cacheFd, 0ULL, totSize);
        if (rc != 0)
        {
            DBGLOG("Error allocating all space for disk page cache file %s (size: %llu), errno %d", cacheFile, totSize, rc);
            return -1;
        }

        // if was previously larger ...
        rc = ftruncate(cacheFd, totSize);
        if (rc != 0)
        {
            DBGLOG("Error setting size of disk page cache file %s (size: %llu), errno %d", cacheFile, totSize, rc);
            return -1;
        }

        posix_fadvise(cacheFd, 0ULL, totSize, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);

        return 0;
    }

    void getCacheInfo(unsigned fileId, offset_t alignedPosShift, offset_t &cacheVal, unsigned &cacheKey, unsigned &critIndex)
    {
        cacheVal = fileId | (alignedPosShift << fileIdBits);
        cacheKey = cacheIndex(cacheVal, numEntries);
        critIndex = cacheKey % numCrits;
    }

    bool readCacheFile(offset_t cacheFileOffset, unsigned critIndex, unsigned toRead, byte *data)
    {
#ifdef __NR_preadv2
        struct iovec iov[1];
        iov[0].iov_len = toRead;

        int pFlags = 0;
        if (ioMethod == DIRECT)
        {
            iov[0].iov_base = &alignedAddr[critIndex * pageSize];
# ifdef RWF_HIPRI
            pFlags |= RWF_HIPRI;
# endif
        }
        else
        {
            iov[0].iov_base = data;
# ifdef RWF_UNCACHED
            // GH NOTE: could still cache reads ...
            if (ioMethod == UNCACHED)
                pFlags |= RWF_UNCACHED;
# endif
        }

        int rc = preadv2(cacheFd, iov, 1, cacheFileOffset, pFlags);

#else // preadv2
        int rc;
        if (ioMethod == DIRECT)
            rc = pread64(cacheFd, &alignedAddr[critIndex * pageSize], toRead, cacheFileOffset);
        else
            rc = pread64(cacheFd, data, toRead, cacheFileOffset);
#endif // preadv2

        if (rc == toRead)
        {
            if (ioMethod == DIRECT)
                memcpy(data, &alignedAddr[critIndex * pageSize], toRead);
            return true;
        }

        return false;
    }

    bool writeCacheFile(offset_t cacheFileOffset, unsigned critIndex, const byte *data)
    {
        if (ioMethod == DIRECT)
            memcpy(&alignedAddr[critIndex * pageSize], data, pageSize);

#ifdef __NR_pwritev2
        struct iovec iov[1];
        iov[0].iov_len = pageSize;

        int pFlags = 0;
        if (ioMethod == DIRECT)
        {
            iov[0].iov_base = &alignedAddr[critIndex * pageSize];
# ifdef RWF_HIPRI
            pFlags |= RWF_HIPRI;
# endif
        }
        else
        {
            iov[0].iov_base = (void *)data;
# ifdef RWF_UNCACHED
            if (ioMethod == UNCACHED)
                pFlags |= RWF_UNCACHED;
# endif
        }

        int rc = pwritev2(cacheFd, iov, 1, cacheFileOffset, pFlags);

#else // pwritev2
        int rc;
        if (ioMethod == DIRECT)
            rc = pwrite64(cacheFd, &alignedAddr[critIndex * pageSize], pageSize, cacheFileOffset);
        else
            rc = pwrite64(cacheFd, data, pageSize, cacheFileOffset);
#endif // pwritev2

        return (rc == pageSize);
    }
#endif

    virtual size32_t queryPageSize() const override
    {
        return pageSize;
    }

    virtual void noteUsed(unsigned fileId, offset_t offset) override { }

    // Searching for a node in the disk node cache.  Return true if found, and ensure nodeData is populated
    // if it returns false then the node is not in the cache.
    virtual bool read(unsigned fileId, offset_t offset, size32_t size, CCachedIndexRead & nodeData) override
    {
#ifdef __linux__
        dbgassertex(size <= pageSize);

        offset_t alignedPos = offset & pageOffsetMask;
        offset_t alignedPosShift = alignedPos >> pageSizeExp;

        // strip off topmost bit (cache tracking code might set this) or should we just use lower fileIdBits ?
        fileId &= ~(1U << 31);

        if ( (fileId >= (1U << fileIdBits)) || (alignedPosShift >= (1ULL << offsetBits)) )
            throw makeStringExceptionV(0, "disk page cache read: invalid fileId %u / offset %llu", fileId, alignedPos);

        offset_t cacheVal;
        unsigned cacheKey;
        unsigned critIndex;
        getCacheInfo(fileId, alignedPosShift, cacheVal, cacheKey, critIndex);

        CriticalBlock b(fCrit[critIndex]);

        CacheEntry & curCacheSet = cache[cacheKey];

        uint8_t setIndex = curCacheSet.findInCacheSet(cacheVal);

        if (setIndex == numSets)
            return onCacheMiss(fileId, offset, size);

        offset_t cacheFileOffset = (setIndex * setSize) + (cacheKey * pageSize);

        if (readSize < pageSize)
        {
            offset_t readAlignedPos = offset & readOffsetMask;
            offset_t deltaOffset = readAlignedPos - alignedPos;
            alignedPos += deltaOffset;
            cacheFileOffset += deltaOffset;
        }

        byte * data = nodeData.getBufferForUpdate(alignedPos, readSize);

        bool ret = readCacheFile(cacheFileOffset, critIndex, readSize, data);
        if (ret)
        {
            curCacheSet.delegateIndexToMostRecentlyUsed(setIndex);
            // hit++
            return true;
        }

        curCacheSet.setEntry(setIndex, invalidValue);
        curCacheSet.promoteIndexToLeastRecentlyUsed(setIndex);
#endif
        return onCacheMiss(fileId, offset, size);
    }

    // Insert a block of data into the disk node cache.
    // If reservation is non null, then it will match the value from a previous call to readNodeOrReserve
    // If reservation is null the data is being preemptively added to the cache (e.g. at startup)
    virtual void write(unsigned fileId, offset_t alignedPos, const byte * data) override
    {
#ifdef __linux__
        offset_t alignedPosShift = alignedPos >> pageSizeExp;

        // strip off topmost bit (cache tracking code might set this) or should we just use lower fileIdBits ?
        fileId &= ~(1U << 31);

        if ( (fileId >= (1U << fileIdBits)) || (alignedPosShift >= (1ULL << offsetBits)) )
            throw makeStringExceptionV(0, "disk page cache write: invalid fileId %u / offset %llu", fileId, alignedPos);

        offset_t cacheVal;
        unsigned cacheKey;
        unsigned critIndex;
        getCacheInfo(fileId, alignedPosShift, cacheVal, cacheKey, critIndex);

        CriticalBlock b(fCrit[critIndex]);

        CacheEntry & curCacheSet = cache[cacheKey];

        uint8_t setIndex = curCacheSet.getLeastRecentlyUsedIndex();

        offset_t cacheFileOffset = (setIndex * setSize) + (cacheKey * pageSize);

        bool ret = writeCacheFile(cacheFileOffset, critIndex, data);
        if (ret)
        {
            curCacheSet.setEntry(setIndex, cacheVal);
            curCacheSet.delegateIndexToMostRecentlyUsed(setIndex);
            return;
        }

        curCacheSet.setEntry(setIndex, invalidValue);
        // setIndex is already at least recently used ...
#endif
    }

    // Called to gather the current state of the cache - so it can be preserved for quick cache warming.
    virtual void gatherState(ICacheInfoRecorder & recorder) override
    {
        CriticalBlock b(crit);
        // TODO: MCK will need a global lock here and then check/pause readCacheFile/writeCacheFile
        //       to get complete cache state ...
    }

    virtual bool isInitializedOK() const override
    {
        return cacheOK;
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
    static constexpr unsigned offsetBits = 40;
    static constexpr unsigned fileIdBits = 64 - offsetBits;
    static constexpr int numSets = 16; // 4, 8, 16 ...
    static constexpr int numCrits = 256;
    static constexpr offset_t invalidValue = ~(offset_t)0;
    offset_t totSize{0};
    offset_t setSize{0};
    size32_t pageSize{0};
    size32_t readSize{0};
    offset_t pageOffsetMask{0};
    offset_t readOffsetMask{0};
    unsigned cacheMissPenalty{0};
    unsigned numEntries{1};
    int pageSizeExp{0};
    int cacheFd{-1};
    CriticalSection fCrit[numCrits];
    IOMethod ioMethod{NORMAL};
    byte *alignedAddr{nullptr};
    CriticalSection crit;
    bool cacheOK{false};

    struct CacheSet
    {
        offset_t value{invalidValue};
    };
    struct CacheEntry
    {
        CacheEntry()
        {
            for (uint8_t j=0; j<numSets; j++)
            {
                lruArray[j] = j;
                cSet[j].value = invalidValue;
            }
        }

        CacheSet cSet[numSets];

        // array of cache set indexes in order from LRU (0) at the head to MRU (numSets-1) at the tail
        uint8_t lruArray[numSets];

        void setEntry(uint8_t indx, offset_t val)
        {
            cSet[indx].value = val;
        }

        uint8_t findInCacheSet(offset_t cacheValue)
        {
            for (uint8_t ix=0; ix<numSets; ix++)
            {
                if (cSet[ix].value == cacheValue)
                    return ix;
            }
            return numSets;
        }

        uint8_t getLeastRecentlyUsedIndex()
        {
            return lruArray[0];
        }

        void delegateIndexToMostRecentlyUsed(uint8_t indx)
        {
            // move index to MRU tail, shifting others before index
            if (lruArray[numSets-1] == indx)
                return;

            unsigned cur = 0;
            while (cur < numSets && lruArray[cur] != indx)
                cur++;
            while (cur < (numSets-1))
            {
                lruArray[cur] = lruArray[cur+1];
                cur++;
            }
            lruArray[numSets-1] = indx;
        }

        void promoteIndexToLeastRecentlyUsed(uint8_t indx)
        {
            // move index to LRU head, shifting others after index
            if (lruArray[0] == indx)
                return;

            unsigned cur = numSets-1;
            while (cur > 0 && lruArray[cur] != indx)
                cur--;
            while (cur > 0)
            {
                lruArray[cur] = lruArray[cur-1];
                cur--;
            }
            lruArray[0] = indx;
        }

    };
    CacheEntry *cache{nullptr};
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

    virtual bool isInitializedOK() const override
    {
        return true;
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

IPageCache * createDiskPageCache(const IPropertyTree * config)
{
    IPageCache *pCache = new CDiskPageCache(config);
    if (pCache->isInitializedOK())
        return pCache;
    pCache->Release();
    return nullptr;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "eclrtl.hpp"


#endif
