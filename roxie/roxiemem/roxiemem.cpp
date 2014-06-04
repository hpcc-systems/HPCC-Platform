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

#include "roxiemem.hpp"
#include "roxierowbuff.hpp"
#include "jlog.hpp"
#include <new>

#ifndef _WIN32
#include <sys/mman.h>
#endif

#ifdef _DEBUG
#define _CLEAR_ALLOCATED_ROW
//#define _CLEAR_ALLOCATED_HUGE_ROW
#endif

#ifdef _WIN32
//Visual studio complains that the constructors for heaplets could throw exceptions, so there should be a matching
//operator delete otherwise there will be leaks.  The constructors can't throw exceptions, so disable the warning.
#pragma warning(disable:4291)
#endif

namespace roxiemem {

#define NOTIFY_UNUSED_PAGES_ON_FREE     // avoid linux swapping 'freed' pages to disk

unsigned memTraceLevel = 1;
memsize_t memTraceSizeLimit = 0;

unsigned DATA_ALIGNMENT_SIZE=0x400;
/*------------
Some thoughts on improving this memory manager:
1. There is no real reason that the link counts need to be adjacent to the data (nor indeed ahead of it)
   We might want to reposition them such that the actual row data was on a better alignment boundary.
   They don't really need to be 4 bytes either (though we'd need some new atomic-types if not)
   We could have an array of link-counts/activity ids at the start of the block...
2. Fix the list per chunk size to be clean
3. Use lockfree lists
4. In 64bit with no memory space constraints, should I simplify?? Give each manager a slab of memory space whose size = limit.
   Does that simplify? Not really sure it does (unless it allows me to use a third party suballocator?)
5. If I double the size of a row can I keep it in the same chunk if the space ahead of me is free? Don't see why not...
   In general allocating 2n blocks in the n size chunkmgr is fine, but need to be careful when freeing them that we add two onto free
   chain. Allocating from hwm is just as efficient, but allocating from free chain less so. Does it lead to fragmentation?
6. Use the knowledge that all chunk sizes are 8-byte aligned to reduce by a factor of 8 the size of a lookup table to retrieve the
   allocator for a chunk size (but then again may not care if you move that lookup to the creation of the EngineRowAllocator
7. Are any things like defaultRight or the row array for topn REALLY allocated once (and thus could use a singleton mgr) or are they
   potentially actually per child query? I fear it is the latter...
8. So what REALLY is the pattern of memory allocations in Roxie? Can I gather stats from anywhere? Do the current stats tell me
   anything interesting?
   - some things allocated directly (not via EngineRowAllocator) - typically not many of them, typically long lived
   - some row sizes allocated a LOT - often short lived but not always
   - Variable size rows common in roxie, fairly common in thor. But people will use fixed if speed gains are there...
   - Child datasets ar a mixed blessing. They make more rows fixed size but also generate a need for variable-size blocks for
     the rowsets themselves.
   - Knowing the row sizes isn't likely to be a massive help to avoid fragmentation in thor. In Roxie fragmentation isn't issue anyway
     and probably sticking to the powers-of-two will be smart
   - If we somehow knew (when creating our engineRowAllocator) how long the row is likely to last, we could decide whether to
     shrink as we finalize)
   - OR (maybe this is equivalent) we could NEVER shrink as we finalize BUT do a clone when we want to hang on to a row. The clone
     be a 'lazy shrink' of just rows which survived long enough to be worth considering shrinking, and would be able to do nothing
     (avoid the call even?) in cases where there was no shrinking to be done...

*/
//================================================================================
// Allocation of aligned blocks from os

#define HEAPERROR(a) throw MakeStringException(ROXIEMM_HEAP_ERROR, a)

#ifdef _DEBUG
const unsigned maxLeakReport = 20;
#else
const unsigned maxLeakReport = 4;
#endif

static char *heapBase;
static char *heapEnd;   // Equal to heapBase + (heapTotalPages * page size)
static bool heapUseHugePages;
static unsigned *heapBitmap;
static unsigned heapBitmapSize;
static unsigned heapTotalPages; // derived from heapBitmapSize - here for code clarity
static unsigned heapLWM;
static unsigned heapLargeBlocks;
static unsigned heapLargeBlockGranularity;
static ILargeMemCallback * heapLargeBlockCallback;
static unsigned __int64 lastStatsCycles;
static unsigned __int64 statsCyclesInterval;

static unsigned heapAllocated;
static atomic_t dataBufferPages;
static atomic_t dataBuffersActive;

const unsigned UNSIGNED_BITS = sizeof(unsigned) * 8;
const unsigned UNSIGNED_ALLBITS = (unsigned) -1;
const unsigned TOPBITMASK = 1<<(UNSIGNED_BITS-1);

template <typename VALUE_TYPE, typename ALIGN_TYPE>
inline VALUE_TYPE align_pow2(VALUE_TYPE value, ALIGN_TYPE alignment)
{
    return (value + alignment -1) & ~((VALUE_TYPE)(alignment) -1);
}

#define PAGES(x, alignment)    (((x) + ((alignment)-1)) / (alignment))           // hope the compiler converts to a shift

typedef MapBetween<unsigned, unsigned, memsize_t, memsize_t> MapActivityToMemsize;

static CriticalSection heapBitCrit;

static void initializeHeap(bool allowHugePages, unsigned pages, unsigned largeBlockGranularity, ILargeMemCallback * largeBlockCallback)
{
    if (heapBase) return;
    // CriticalBlock b(heapBitCrit); // unnecessary - must call this exactly once before any allocations anyway!
    heapBitmapSize = (pages + UNSIGNED_BITS - 1) / UNSIGNED_BITS;
    heapTotalPages = heapBitmapSize * UNSIGNED_BITS;
    heapLargeBlockGranularity = largeBlockGranularity;
    heapLargeBlockCallback = largeBlockCallback;
    memsize_t memsize = memsize_t(heapTotalPages) * HEAP_ALIGNMENT_SIZE;
#ifdef _WIN32
    // Not the world's best code but will do 
    char *next = (char *) HEAP_ALIGNMENT_SIZE;
    loop
    {
        heapBase = (char *) VirtualAlloc(next, memsize, MEM_RESERVE|MEM_COMMIT, PAGE_READWRITE);
        if (heapBase)
            break;
        next += HEAP_ALIGNMENT_SIZE;
        if (!next) 
        {
            DBGLOG("RoxieMemMgr: VirtualAlloc(size=%u) failed - alignment=%u", HEAP_ALIGNMENT_SIZE, memsize);
            HEAPERROR("RoxieMemMgr: Unable to create heap");
        }
    }
#else
    heapUseHugePages = false;

#ifdef MAP_HUGETLB
    if (allowHugePages)
    {
        heapBase = (char *)mmap(NULL, memsize, (PROT_READ | PROT_WRITE), (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), 0, 0);
        if (heapBase != MAP_FAILED)
        {
            heapUseHugePages = true;
            DBGLOG("Using Huge Pages for roxiemem");
        }
        else
        {
            heapBase = NULL;
            DBGLOG("Huge Pages requested but unavailable");
        }
    }
#else
    if (allowHugePages)
        DBGLOG("Huge Pages requested but not supported by the system");
#endif

    if (!heapBase)
    {
        int ret;
        if ((ret = posix_memalign((void **) &heapBase, HEAP_ALIGNMENT_SIZE, memsize)) != 0) {

        	switch (ret)
        	{
        	case EINVAL:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%"I64F"u, size=%"I64F"u) failed - ret=%d "
        				"(EINVAL The alignment argument was not a power of two, or was not a multiple of sizeof(void *)!)",
        		                    (unsigned __int64) HEAP_ALIGNMENT_SIZE, (unsigned __int64) memsize, ret);
        		break;

        	case ENOMEM:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%"I64F"u, size=%"I64F"u) failed - ret=%d "
        				"(ENOMEM There was insufficient memory to fulfill the allocation request.)",
        		        		                    (unsigned __int64) HEAP_ALIGNMENT_SIZE, (unsigned __int64) memsize, ret);
        		break;

        	default:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%"I64F"u, size=%"I64F"u) failed - ret=%d",
        		                    (unsigned __int64) HEAP_ALIGNMENT_SIZE, (unsigned __int64) memsize, ret);
        		break;

        	}
            HEAPERROR("RoxieMemMgr: Unable to create heap");
        }
    }
#endif

    assertex(((memsize_t)heapBase & (HEAP_ALIGNMENT_SIZE-1)) == 0);

    heapEnd = heapBase + memsize;
    heapBitmap = new unsigned [heapBitmapSize];
    memset(heapBitmap, 0xff, heapBitmapSize*sizeof(unsigned));
    heapLargeBlocks = 1;
    heapLWM = 0;

    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: %u Pages successfully allocated for the pool - memsize=%"I64F"u base=%p alignment=%"I64F"u bitmapSize=%u", 
                heapTotalPages, (unsigned __int64) memsize, heapBase, (unsigned __int64) HEAP_ALIGNMENT_SIZE, heapBitmapSize);
}

extern void releaseRoxieHeap()
{
    if (heapBase)
    {
        if (memTraceLevel)
            DBGLOG("RoxieMemMgr: releasing heap");
        delete [] heapBitmap;
        heapBitmap = NULL;
#ifdef _WIN32
        VirtualFree(heapBase, 0, MEM_RELEASE);
#else
        if (heapUseHugePages)
        {
            memsize_t memsize = memsize_t(heapTotalPages) * HEAP_ALIGNMENT_SIZE;
            munmap(heapBase, memsize);
            heapUseHugePages = false;
        }
        else
            free(heapBase);
#endif
        heapBase = NULL;
        heapEnd = NULL;
        heapBitmapSize = 0;
        heapTotalPages = 0;
    }
}

extern void memstats(unsigned &totalpg, unsigned &freepg, unsigned &maxblk)
{
    totalpg = heapTotalPages;
    unsigned freePages = 0;
    unsigned maxBlock = 0;
    unsigned thisBlock = 0;

    {
        CriticalBlock b(heapBitCrit);
        for (unsigned i = 0; i < heapBitmapSize; i++)
        {
            unsigned t = heapBitmap[i];
            if (t)
            {
                if (t==UNSIGNED_ALLBITS)
                {
                    thisBlock += UNSIGNED_BITS;
                    freePages += UNSIGNED_BITS;
                }
                else
                {
                    do
                    {
                        if (t&1)
                        {
                            freePages++;
                            thisBlock++;
                        }
                        else
                        {
                            if (thisBlock > maxBlock)
                                maxBlock = thisBlock;
                            thisBlock = 0;
                        }
                        t >>= 1;
                    } while (t);
                }
            }
            else if (thisBlock)
            {
                if (thisBlock > maxBlock)
                    maxBlock = thisBlock;
                thisBlock = 0;
            }
        }
    }

    if (thisBlock > maxBlock)
        maxBlock = thisBlock;

    freepg = freePages;
    maxblk = maxBlock;
}

extern StringBuffer &memstats(StringBuffer &stats)
{
    unsigned totalPages;
    unsigned freePages;
    unsigned maxBlock;
    memstats(totalPages, freePages, maxBlock);
    return stats.appendf("Heap size %u pages, %u free, largest block %u", heapTotalPages, freePages, maxBlock);
}

IPerfMonHook *createRoxieMemStatsPerfMonHook(IPerfMonHook *chain)
{
    class memstatsPerfMonHook : public CInterface, implements IPerfMonHook
    {
        Linked<IPerfMonHook> chain;
    public:
        IMPLEMENT_IINTERFACE;
        memstatsPerfMonHook(IPerfMonHook *_chain)
            : chain(_chain)
        {
        }
        
        void processPerfStats(unsigned processorUsage, unsigned memoryUsage, unsigned memoryTotal, unsigned __int64 firstDiskUsage, unsigned __int64 firstDiskTotal, unsigned __int64 secondDiskUsage, unsigned __int64 secondDiskTotal, unsigned threadCount)
        {
            if (chain)
                chain->processPerfStats(processorUsage, memoryUsage, memoryTotal, firstDiskUsage,firstDiskTotal, secondDiskUsage, secondDiskTotal, threadCount);
        }
        StringBuffer &extraLogging(StringBuffer &extra)
        {
            unsigned totalPages;
            unsigned freePages;
            unsigned maxBlock;
            memstats(totalPages, freePages, maxBlock);
            if (totalPages) {
                if (extra.length())
                    extra.append(' ');
                extra.appendf("RMU=%3u%% RMX=%uM",100-freePages*100/totalPages,(unsigned)(((memsize_t)freePages*(memsize_t)HEAP_ALIGNMENT_SIZE)/0x100000));
            }
            if (chain)
                return chain->extraLogging(extra);
            return extra;
        }

    };
    return new memstatsPerfMonHook(chain);
}

static StringBuffer &memmap(StringBuffer &stats)
{
    CriticalBlock b(heapBitCrit);
    unsigned freePages = 0;
    unsigned maxBlock = 0;
    unsigned thisBlock = 0;
    stats.appendf("========== MANAGED HEAP MEMORY MAP (1 free, 0 used) ================="); 
    for (unsigned i = 0; i < heapBitmapSize; i++)
    {
        if (i % 2) stats.appendf("  ");
        else stats.appendf("\n%p: ", heapBase + i*UNSIGNED_BITS*HEAP_ALIGNMENT_SIZE);

        if (heapBitmap[i] == UNSIGNED_ALLBITS) {
            stats.appendf("11111111111111111111111111111111");
            freePages += UNSIGNED_BITS;
            thisBlock += UNSIGNED_BITS;
            if (thisBlock > maxBlock)
                maxBlock = thisBlock;
        }
        else if (heapBitmap[i] == 0) {
            stats.appendf("00000000000000000000000000000000");
            thisBlock = 0;
        }
        else {
            unsigned mask = 1;
            while (mask)
            {
                if (heapBitmap[i] & mask)
                {
                    stats.appendf("1");
                    freePages++;
                    thisBlock++;
                    if (thisBlock > maxBlock)
                        maxBlock = thisBlock;
                }
                else {
                    thisBlock = 0;
                    stats.appendf("0");
                }
                mask <<= 1;
            }
        }
    }
    return stats.appendf("\nHeap size %u pages, %u free, largest block %u", heapTotalPages, freePages, maxBlock);
}

static void throwHeapExhausted(unsigned pages)
{
    VStringBuffer msg("Memory pool exhausted: pool (%u pages) exhausted, requested %u", heapTotalPages, pages);
    DBGLOG("%s", msg.str());
    throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
}

static void throwHeapExhausted(unsigned newPages, unsigned oldPages)
{
    VStringBuffer msg("Memory pool exhausted: pool (%u pages) exhausted, requested %u, had %u", heapTotalPages, newPages, oldPages);
    DBGLOG("%s", msg.str());
    throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
}

static void *suballoc_aligned(size32_t pages, bool returnNullWhenExhausted)
{
    //It would be tempting to make this lock free and use cas, but on reflection I suspect it will perform worse.
    //The problem is allocating multiple pages which fit into two unsigneds.  Because they can't be covered by a
    //single cas you are likely to livelock if two occurred at the same time.
    //It could be mitigated by starting at a "random" position, but that is likely to increase fragmentation,
    //It can be revisited if it proves to be a bottleneck - unlikely until we have several Tb of memory.
    if (statsCyclesInterval)
    {
        //To report on every allocation set statsCyclesInterval to 1
        unsigned __int64 cyclesNow = get_cycles_now();
        if ((cyclesNow - lastStatsCycles) >= statsCyclesInterval)
        {
            // No need to lock - worst that can happen is we call too often which is relatively harmless
            lastStatsCycles = cyclesNow;
            StringBuffer s;
            memstats(s);
            s.appendf(", heapLWM %u, dataBuffersActive=%d, dataBufferPages=%d", heapLWM, atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
            DBGLOG("RoxieMemMgr: %s", s.str());
        }
    }
    CriticalBlock b(heapBitCrit);
    if (heapAllocated + pages > heapTotalPages) {
        if (returnNullWhenExhausted)
            return NULL;
        throwHeapExhausted(pages);
    }
    if (heapLargeBlockGranularity)
    {
        //Short circuit the divide for the common case
        if ((heapAllocated + pages) > heapLargeBlockGranularity)
        {
            unsigned largeBlocksRequired = PAGES(heapAllocated + pages, heapLargeBlockGranularity);
            if (largeBlocksRequired > heapLargeBlocks)
            {
                //MORE: Only test every second?  Less likely to be called very often since failure will trigger a callback flush.
                if (!heapLargeBlockCallback->take((largeBlocksRequired - heapLargeBlocks)*HEAP_ALIGNMENT_SIZE))
                {
                    if (returnNullWhenExhausted)
                        return NULL;
                    VStringBuffer msg("Memory pool exhausted: Request for large memory denied (%u..%u)", heapLargeBlocks, largeBlocksRequired);
                    DBGLOG("%s", msg.str());
                    throw MakeStringException(ROXIEMM_LARGE_MEMORY_EXHAUSTED, "%s", msg.str());
                }

                heapLargeBlocks = largeBlocksRequired;
            }
        }
    }

    if (pages == 1)
    {
        unsigned i;
        for (i = heapLWM; i < heapBitmapSize; i++)
        {
            unsigned hbi = heapBitmap[i];
            if (hbi)
            {
                unsigned mask = 1;
                char *ret = heapBase + i*UNSIGNED_BITS*HEAP_ALIGNMENT_SIZE;
                while (!(hbi & mask))
                {
                    ret += HEAP_ALIGNMENT_SIZE;
                    mask <<= 1;
                }
                heapBitmap[i] = (hbi & ~mask);
                heapLWM = i;
                heapAllocated++;
                if (memTraceLevel >= 2)
                    DBGLOG("RoxieMemMgr: suballoc_aligned() 1 page ok - addr=%p", ret);
                return ret;
            }
        }
    }
    else
    {
        // Usage pattern is such that we expect normally to succeed immediately.
        unsigned i = heapBitmapSize;
        unsigned matches = 0;
        while (i)
        {
            unsigned hbi = heapBitmap[--i];
            unsigned mask = TOPBITMASK;
            if (hbi)
            {
                for (unsigned b = UNSIGNED_BITS; b > 0; b--)
                {
                    if (hbi & mask)
                    {
                        matches++;
                        if (matches==pages)
                        {
                            char *ret = heapBase + (i*UNSIGNED_BITS+b-1)*HEAP_ALIGNMENT_SIZE;
                            loop
                            {
                                hbi &= ~mask;
                                if (!--matches)
                                    break;
                                if (mask==TOPBITMASK)
                                {
                                    heapBitmap[i] = hbi;
                                    mask=1;
                                    i++;
                                    hbi = heapBitmap[i];
                                }
                                else
                                    mask <<= 1;
                            }
                            heapBitmap[i] = hbi;
                            if (memTraceLevel >= 2)
                                DBGLOG("RoxieMemMgr: suballoc_aligned() %u pages ok - addr=%p", pages, ret);
                            heapAllocated += pages;
                            return ret;
                        }
                    }
                    else
                        matches = 0;
                    mask >>= 1;
                }
            }
            else
                matches = 0;
        }
    }
    if (returnNullWhenExhausted)
        return NULL;
    throwHeapExhausted(pages);
    return NULL;
}

static void subfree_aligned(void *ptr, unsigned pages = 1)
{
    unsigned _pages = pages;
    memsize_t offset = (char *)ptr - heapBase;
    memsize_t pageOffset = offset / HEAP_ALIGNMENT_SIZE;
    if (!pages)
    {
        DBGLOG("RoxieMemMgr: Invalid parameter (pages=%u) to subfree_aligned", pages);
        HEAPERROR("RoxieMemMgr: Invalid parameter (num pages) to subfree_aligned");
    }
    if (pageOffset + pages > heapTotalPages)
    {
        DBGLOG("RoxieMemMgr: Freed area not in heap (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Freed area not in heap");
    }
    if (pageOffset*HEAP_ALIGNMENT_SIZE != offset)
    {
        DBGLOG("RoxieMemMgr: Incorrect alignment of freed area (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Incorrect alignment of freed area");
    }
#ifdef NOTIFY_UNUSED_PAGES_ON_FREE
#ifdef _WIN32
    VirtualAlloc(ptr, pages*HEAP_ALIGNMENT_SIZE, MEM_RESET, PAGE_READWRITE);
#else
    // for linux mark as unwanted
    madvise(ptr,pages*HEAP_ALIGNMENT_SIZE,MADV_DONTNEED);
#endif
#endif
    unsigned wordOffset = (unsigned) (pageOffset / UNSIGNED_BITS);
    unsigned bitOffset = (unsigned) (pageOffset % UNSIGNED_BITS);
    unsigned mask = 1<<bitOffset;
    unsigned nextPageOffset = (pageOffset+pages + (UNSIGNED_BITS-1)) / UNSIGNED_BITS;
    {
        CriticalBlock b(heapBitCrit);
        heapAllocated -= pages;

        //Return large free blocks to the interprocess allocator.
        if (heapLargeBlocks > 1)
        {
            //Allow a leaway of heapLargeBlockGranularity/8 pages to avoid thrashing around the boundary
            unsigned leaway = heapLargeBlockGranularity / 8;
            unsigned largeBlocksRequired = PAGES(heapAllocated + leaway, heapLargeBlockGranularity);
            if (largeBlocksRequired == 0)
                largeBlocksRequired = 1;
            if (largeBlocksRequired != heapLargeBlocks)
            {
                heapLargeBlockCallback->give((heapLargeBlocks - largeBlocksRequired) * HEAP_ALIGNMENT_SIZE);
                heapLargeBlocks = largeBlocksRequired;
            }
        }

        if (wordOffset < heapLWM)
            heapLWM = wordOffset;
        loop
        {
            unsigned prev = heapBitmap[wordOffset];
            if ((prev & mask) == 0)
                heapBitmap[wordOffset] = (prev|mask);
            else
                HEAPERROR("RoxieMemMgr: Page freed twice");
            if (!--pages)
                break;
            if (mask==TOPBITMASK)
            {
                mask = 1;
                wordOffset++;
            }
            else
                mask <<= 1;
        }
    }
    if (memTraceLevel >= 2)
        DBGLOG("RoxieMemMgr: subfree_aligned() %u pages ok - addr=%p heapLWM=%u totalPages=%u", _pages, ptr, heapLWM, heapTotalPages);
}

static void clearBits(unsigned start, unsigned len)
{
    // Note - should already have locked before calling this
    // These functions should really be member functions (and the heapBitmap should be a member variable) of a
    // HeapAlignedBitmap class.
    if (len)
    {
        unsigned wordOffset = (unsigned) (start / UNSIGNED_BITS);
        unsigned bitOffset = (unsigned) (start % UNSIGNED_BITS);
        unsigned mask = 1<<bitOffset;
        heapAllocated += len;
        unsigned heapword = heapBitmap[wordOffset];
        while (len--)
        {
            if (heapword & mask)
                heapword &= ~mask;
            else
                HEAPERROR("RoxieMemMgr: Page freed twice");
            if (mask==TOPBITMASK)
            {
                heapBitmap[wordOffset] = heapword;
                mask = 1;
                wordOffset++;
                if (wordOffset==heapBitmapSize)
                    return;    // Avoid read off end of array
                heapword = heapBitmap[wordOffset];
            }
            else
                mask <<= 1;
        }
        heapBitmap[wordOffset] = heapword;
    }
}

static void *subrealloc_aligned(void *ptr, unsigned pages, unsigned newPages)
{
    assertex(newPages > 0);
    unsigned _pages = pages;
    memsize_t offset = (char *)ptr - heapBase;
    memsize_t pageOffset = offset / HEAP_ALIGNMENT_SIZE;
    if (!pages)
    {
        DBGLOG("RoxieMemMgr: Invalid parameter (pages=%u) to subrealloc_aligned", pages);
        HEAPERROR("RoxieMemMgr: Invalid parameter (num pages) to subrealloc_aligned");
    }
    if (pageOffset + pages > heapTotalPages)
    {
        DBGLOG("RoxieMemMgr: Realloced area not in heap (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Realloced area not in heap");
    }
    if (pageOffset*HEAP_ALIGNMENT_SIZE != offset)
    {
        DBGLOG("RoxieMemMgr: Incorrect alignment of realloced area (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Incorrect alignment of realloced area");
    }
    if (pages > newPages)
    {
        subfree_aligned((char *) ptr + pages*HEAP_ALIGNMENT_SIZE, pages - newPages);
        return ptr;
    }
    else if (pages==newPages)
    {
        return ptr;
    }
    else
    {
        CriticalBlock b(heapBitCrit);
        unsigned shortfall = newPages - pages;
        unsigned topOffset = pageOffset + pages;
        // First see if we can find n free bits above the allocated region
        unsigned wordOffset = (unsigned) (topOffset / UNSIGNED_BITS);
        if (wordOffset < heapBitmapSize)
        {
            unsigned bitOffset = (unsigned) (topOffset % UNSIGNED_BITS);
            unsigned mask = 1<<bitOffset;
            unsigned heapword = heapBitmap[wordOffset];
            while (shortfall)
            {
                if ((heapword & mask) == 0)
                    break;
                shortfall--;
                if (mask==TOPBITMASK)
                {
                    mask = 1;
                    wordOffset++;
                    if (wordOffset==heapBitmapSize)
                        break;
                    heapword = heapBitmap[wordOffset];
                    while (shortfall >= UNSIGNED_BITS)
                    {
                        if (heapword != UNSIGNED_ALLBITS)
                            break;
                        wordOffset++;
                        if (wordOffset==heapBitmapSize)
                            goto doublebreak;
                        heapword = heapBitmap[wordOffset];
                        shortfall -= UNSIGNED_BITS;
                    }
                }
                else
                    mask <<= 1;
            }
            if (!shortfall)
            {
                clearBits(topOffset, newPages - pages);
                return ptr;
            }
        }
    doublebreak:
        // Then see if we can find remaining free bits below the allocated region
        wordOffset = (unsigned) (pageOffset / UNSIGNED_BITS);
        if (wordOffset < heapBitmapSize)
        {
            unsigned bitOffset = (unsigned) (pageOffset % UNSIGNED_BITS);
            unsigned mask = 1<<bitOffset;
            unsigned foundAbove = (newPages - pages) - shortfall;
            unsigned needBelow = shortfall;
            unsigned heapword = heapBitmap[wordOffset];
            while (shortfall)
            {
                if (mask==1)
                {
                    while (shortfall >= UNSIGNED_BITS && wordOffset > 0)
                    {
                        wordOffset--;
                        heapword = heapBitmap[wordOffset];
                        if (heapword != UNSIGNED_ALLBITS)
                            return NULL;
                        shortfall -= UNSIGNED_BITS;
                    }
                    if (wordOffset==0 || shortfall==0)
                        break;
                    mask = TOPBITMASK;
                    wordOffset--;
                    heapword = heapBitmap[wordOffset];
                }
                else
                    mask >>= 1;
                if ((heapword & mask) == 0)
                    break;
                shortfall--;
            }
            if (!shortfall)
            {
                pageOffset -= needBelow;
                clearBits(pageOffset, needBelow);
                clearBits(topOffset, foundAbove);
                // NOTE: it's up to the caller to move the data - they know how much of it they actually care about.
                return heapBase + pageOffset*HEAP_ALIGNMENT_SIZE;
            }
        }
        else
            throwUnexpected();  // equivalently, assertex(wordOffset < heapBitmapSize)
        return NULL; // can't realloc
    }
}

//---------------------------------------------------------------------------------------------------------------------
static inline unsigned getRealActivityId(unsigned allocatorId, const IRowAllocatorCache *allocatorCache)
{
    if ((allocatorId & ACTIVITY_FLAG_ISREGISTERED) && allocatorCache)
        return allocatorCache->getActivityId(allocatorId & MAX_ACTIVITY_ID);
    else
        return allocatorId & MAX_ACTIVITY_ID;
}

static inline bool isValidRoxiePtr(const void *_ptr)
{
    const char *ptr = (const char *) _ptr;
    return ptr >= heapBase && ptr < heapEnd;
}

void HeapletBase::release(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        h->noteReleased(ptr);
    }
}


void HeapletBase::releaseRowset(unsigned count, byte * * rowset)
{
    if (rowset)
    {
        //MORE: There is a small window of simultaneous releases that could lead to the children being leaked
        if (!isShared(rowset))
        {
            byte * * finger = rowset;
            while (count--)
                release(*finger++);
        }
        release(rowset);
    }
}


void HeapletBase::link(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        h->noteLinked(ptr);
    }
}

bool HeapletBase::isShared(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        return h->_isShared(ptr);
    }
    if (ptr)
        return true;  // Objects outside the Roxie heap are implicitly 'infinitely shared'
    // isShared(NULL) is an error
    throwUnexpected();
}

void HeapletBase::internalReleaseNoDestructor(const void *ptr)
{
    dbgassertex(isValidRoxiePtr(ptr));
    HeapletBase *h = findBase(ptr);
    return h->_internalReleaseNoDestructor(ptr);
}

memsize_t HeapletBase::capacity(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        return h->_capacity();
    }
    throwUnexpected();   // should never ask about capacity of anything but a row you allocated from Roxie heap
}

const void * HeapletBase::compactRow(const void * ptr, HeapCompactState & state)
{
    if (!isValidRoxiePtr(ptr))
        return ptr;
    HeapletBase *h = findBase(ptr);
    return h->_compactRow(ptr, state);
}

void HeapletBase::setDestructorFlag(const void *ptr)
{
    dbgassertex(isValidRoxiePtr(ptr));
    HeapletBase *h = findBase(ptr);
    h->_setDestructorFlag(ptr);
}

bool HeapletBase::hasDestructor(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        return h->_hasDestructor(ptr);
    }
    else
        return false;
}

class CHeap;
static void noteEmptyPage(CHeap * heap);
class Heaplet : public HeapletBase
{
    friend class CHeap;
protected:
    Heaplet *next;
    const IRowAllocatorCache *allocatorCache;
    CHeap * const heap;
    memsize_t chunkCapacity;
    
    inline unsigned getActivityId(unsigned allocatorId) const
    {
        return getRealActivityId(allocatorId, allocatorCache);
    }

public:
    Heaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, memsize_t _chunkCapacity) : heap(_heap), chunkCapacity(_chunkCapacity)
    {
        assertex(heap);
        next = NULL;
        allocatorCache = _allocatorCache;
    }

    virtual size32_t sizeInPages() = 0;

    virtual memsize_t _capacity() const { return chunkCapacity; }
    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const = 0;
    virtual void checkHeap() const = 0;
    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const = 0;

#ifdef _WIN32
#ifdef new
#define __new_was_defined
#undef new
#endif
#endif

    void operator delete(void * p)
    {
        subfree_aligned(p, 1);
    }
};


//================================================================================
// Fixed size (chunking) heaplet
#define RBLOCKS_CAS_TAG        HEAP_ALIGNMENT_SIZE           // must be >= HEAP_ALIGNMENT_SIZE and power of 2
#define RBLOCKS_OFFSET_MASK    (RBLOCKS_CAS_TAG-1)
#define RBLOCKS_CAS_TAG_MASK   ~RBLOCKS_OFFSET_MASK
#define ROWCOUNT_MASK            0x7fffffff
#define ROWCOUNT_DESTRUCTOR_FLAG 0x80000000
#define ROWCOUNT(x)              (x & ROWCOUNT_MASK)

#define CACHE_LINE_SIZE 64
#define HEAPLET_DATA_AREA_OFFSET(heapletType) ((size32_t) ((sizeof(heapletType) + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE) * CACHE_LINE_SIZE)

class CChunkedHeap;
class ChunkedHeaplet : public Heaplet
{
protected:
    atomic_t r_blocks;  // the free chain as a relative pointer
    atomic_t freeBase;
    const size32_t chunkSize;
    unsigned sharedAllocatorId;

    inline char *data() const
    {
        return ((char *) this) + dataOffset();
    }

    //NB: Derived classes should not contain any derived data.  The choice would be to put data() into the derived
    //classes, but that means it is hard to common up some of the code efficiently

public:
    ChunkedHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t _chunkSize, size32_t _chunkCapacity)
        : Heaplet(_heap, _allocatorCache, _chunkCapacity), chunkSize(_chunkSize)
    {
        sharedAllocatorId = 0;
        atomic_set(&freeBase, 0);
        atomic_set(&r_blocks, 0);
    }

    virtual size32_t sizeInPages() { return 1; }

    inline unsigned numChunks() const { return queryCount()-1; }

    inline static unsigned dataOffset() { return HEAPLET_DATA_AREA_OFFSET(ChunkedHeaplet); }

    inline static size32_t dataAreaSize() { return  (size32_t)(HEAP_ALIGNMENT_SIZE - dataOffset()); }

    virtual void _setDestructorFlag(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr;
        ptr -= sizeof(atomic_t);
        atomic_set((atomic_t *)ptr, 1|ROWCOUNT_DESTRUCTOR_FLAG);
    }

    virtual bool _hasDestructor(const void *ptr) const
    {
        const atomic_t * curCount = reinterpret_cast<const atomic_t *>((const char *) ptr - sizeof(atomic_t));
        unsigned rowCount = atomic_read(curCount);
        return (rowCount & ROWCOUNT_DESTRUCTOR_FLAG) != 0;
    }

    virtual const void * _compactRow(const void * ptr, HeapCompactState & state);

    const void * moveRow(const void * row)
    {
        //If this pointer already lives in this page don't move it (but update the current page source)
        if (findBase(row) == this)
            return row;

        //Don't clone rows into empty pages
        if (isEmpty())
            return NULL;

        char * chunk = allocateChunk();
        if (!chunk)
            return NULL;
        return doMoveRow(chunk, row);
    }

    virtual const void * doMoveRow(char * chunk, const void * row) = 0;

    virtual void * initChunk(char * chunk, unsigned allocatorId) = 0;

    void * allocate(unsigned allocatorId)
    {
        char * ret = allocateChunk();
        if (!ret)
            return NULL;
        return initChunk(ret, allocatorId);
    }

    char * allocateChunk();

protected:
    inline unsigned makeRelative(const char *ptr)
    {
        dbgassertex(ptr);
        ptrdiff_t diff = ptr - (char *) this;
        assert(diff < HEAP_ALIGNMENT_SIZE);
        return (unsigned) diff;
    }

    inline char * makeAbsolute(unsigned v)
    {
        dbgassertex(v && v < HEAP_ALIGNMENT_SIZE);
        return ((char *) this) + v;
    }

    inline bool inlineIsShared(const void *_ptr) const
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr;
        ptr -= sizeof(atomic_t);
        return ROWCOUNT(atomic_read((atomic_t *) ptr)) !=1;
    }

    inline void inlineNoteLinked(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr;
        ptr -= sizeof(atomic_t);
        atomic_inc((atomic_t *) ptr);
    }

    inline void inlineReleasePointer(char * ptr)
    {
        unsigned r_ptr = makeRelative(ptr);
        loop
        {
            //To prevent the ABA problem the top part of r_blocks stores an incrementing tag
            //which is incremented whenever something is added to the free list
            unsigned old_blocks = atomic_read(&r_blocks);
            * (unsigned *) ptr = (old_blocks & RBLOCKS_OFFSET_MASK);
            unsigned new_tag = ((old_blocks & RBLOCKS_CAS_TAG_MASK) + RBLOCKS_CAS_TAG);
            unsigned new_blocks = new_tag | r_ptr;
            if (atomic_cas(&r_blocks, new_blocks, old_blocks))
                break;
        }

        CHeap * savedHeap = heap;
        // after the following dec it is possible that the page could be freed, so cannot access any members of this
        compiler_memory_barrier();
        if (atomic_dec_and_read(&count) == 1)
            noteEmptyPage(savedHeap);
    }
};

//================================================================================

class FixedSizeHeaplet : public ChunkedHeaplet
{
    //NOTE: This class should not contain any more data - otherwise the dataOffset() may be calculated incorrectly
    struct ChunkHeader
    {
        unsigned allocatorId;
        atomic_t count;  //count must be the last item in the header
    };
public:
    enum { chunkHeaderSize = sizeof(ChunkHeader) };

    FixedSizeHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t size)
    : ChunkedHeaplet(_heap, _allocatorCache, size, size - chunkHeaderSize)
    {
    }

    virtual bool _isShared(const void *_ptr) const
    {
        checkPtr(_ptr, "isShared");
        return inlineIsShared(_ptr);
    }

    virtual void noteReleased(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        checkPtr(_ptr, "Release");

        char *ptr = (char *) _ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
        unsigned rowCount = atomic_dec_and_read(&header->count);
        if (ROWCOUNT(rowCount) == 0)
        {
            if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
            {
                unsigned id = header->allocatorId;
                allocatorCache->onDestroy(id & MAX_ACTIVITY_ID, ptr + chunkHeaderSize);
            }

            inlineReleasePointer(ptr);
        }
    }

    virtual void _internalReleaseNoDestructor(const void * _ptr)
    {
        char *ptr = (char *) _ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
        unsigned rowCount = atomic_dec_and_read(&header->count);
        assertex(ROWCOUNT(rowCount) == 0);
        inlineReleasePointer(ptr);
    }


    virtual void noteLinked(const void *_ptr)
    {
        checkPtr(_ptr, "Link");
        inlineNoteLinked(_ptr);
    }

    virtual void * initChunk(char * chunk, unsigned allocatorId)
    {
        char * ret = chunk;
        ChunkHeader * header = (ChunkHeader *)ret;
        header->allocatorId = (allocatorId & ACTIVITY_MASK) | ACTIVITY_MAGIC;
        atomic_set(&header->count, 1);
        ret += chunkHeaderSize;
#ifdef _CLEAR_ALLOCATED_ROW
        memset(ret, 0xcc, chunkCapacity);
#endif
        return ret;
    }

    virtual const void * doMoveRow(char * chunk, const void * row)
    {
        char * ret = (char *)chunk;
        //Clone across the information from the old row
        char * oldPtr = (char *)row - chunkHeaderSize;
        ChunkHeader * oldHeader = (ChunkHeader *)oldPtr;
        ChunkHeader * newHeader = (ChunkHeader *)ret;
        unsigned rowCountValue = atomic_read(&oldHeader->count);
        unsigned destructFlag = (rowCountValue & ROWCOUNT_DESTRUCTOR_FLAG);
        unsigned allocatorId = oldHeader->allocatorId;
        newHeader->allocatorId = allocatorId;
        atomic_set(&newHeader->count, 1|destructFlag);
        ret += chunkHeaderSize;
        memcpy(ret, row, chunkCapacity);

        //If this was the only instance then release the old row without calling the destructor
        if (ROWCOUNT(rowCountValue) == 1)
            HeapletBase::internalReleaseNoDestructor(row);
        else
        {
            if (destructFlag)
                allocatorCache->onClone(allocatorId & MAX_ACTIVITY_ID, ret);
            ReleaseRoxieRow(row);
        }
        return ret;
    }

    inline static size32_t maxHeapSize()
    {
        return dataAreaSize() - chunkHeaderSize;
    }

    virtual unsigned _rawAllocatorId(const void *ptr) const
    {
        ChunkHeader * header = (ChunkHeader *)ptr - 1;
        return header->allocatorId;
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const
    {
        //This function may not give correct results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        while (leaked > 0 && base < limit)
        {
            const char *block = data() + base;
            const char *ptr = block + (chunkSize-chunkCapacity);  // assumes the overhead is all at the start
            unsigned rowCount = atomic_read((atomic_t *) (ptr - sizeof(atomic_t)));
            if (ROWCOUNT(rowCount) != 0)
            {
                reportLeak(block, logctx);
                leaked--;
            }
            base += chunkSize;
        }
    }

    virtual void checkHeap() const 
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        while (base < limit)
        {
            const char *block = data() + base;
            ChunkHeader * header = (ChunkHeader *)block;
            unsigned rowCount = atomic_read(&header->count);
            if (ROWCOUNT(rowCount) != 0)
            {
                //MORE: Potential race: could be freed while the pointer is being checked
                checkPtr(block+chunkHeaderSize, "Check");
                unsigned id = header->allocatorId;
                if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
                    allocatorCache->checkValid(id & MAX_ACTIVITY_ID, block + chunkHeaderSize);
            }
            base += chunkSize;
        }
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const 
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        memsize_t running = 0;
        unsigned runningCount = 0;
        unsigned lastId = 0;
        while (base < limit)
        {
            const char *block = data() + base;
            ChunkHeader * header = (ChunkHeader *)block;
            unsigned allocatorId = header->allocatorId;
            //Potential race condition - a block could become allocated between these two lines.
            //That may introduce invalid activityIds (from freed memory) in the memory tracing.
            unsigned rowCount = atomic_read(&header->count);
            if (ROWCOUNT(rowCount) != 0)
            {
                if (allocatorId != lastId)
                {
                    if (lastId)
                        map->noteMemUsage(lastId, running, runningCount);
                    lastId = allocatorId;
                    running = chunkSize;
                    runningCount = 1;
                }
                else
                {
                    running += chunkSize;
                    runningCount++;
                }
            }
            base += chunkSize;
        }
        if (lastId)
            map->noteMemUsage(lastId, running, runningCount);
    }

private:
    inline void checkPtr(const void * _ptr, const char *reason) const
    {
        const char * ptr = (const char *)_ptr;
        const char *baseptr = ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)baseptr;
#ifdef _DEBUG
        if (memTraceLevel >= 100)
        {
            DBGLOG("%s: Pointer %p %x", reason, ptr, *(unsigned *) baseptr);
            if (memTraceLevel >= 1000)
                PrintStackReport();
        }
#endif
        if ((header->allocatorId & ~ACTIVITY_MASK) != ACTIVITY_MAGIC)
        {
            DBGLOG("%s: Invalid pointer %p %x", reason, ptr, *(unsigned *) baseptr);
            PrintStackReport();
            PrintMemoryReport();
            HEAPERROR("Invalid pointer");
        }
    }

    void reportLeak(const void * block, const IContextLogger &logctx) const
    {
        ChunkHeader * header = (ChunkHeader *)block;
        unsigned allocatorId = header->allocatorId;
        unsigned rowCount = atomic_read(&header->count);
        bool hasChildren = (rowCount & ROWCOUNT_DESTRUCTOR_FLAG) != 0;

        const char * ptr = (const char *)block + chunkHeaderSize;
        logctx.CTXLOG("Block size %u at %p %swas allocated by activity %u and not freed (%d)", chunkSize, ptr, hasChildren ? "(with children) " : "", getActivityId(allocatorId), ROWCOUNT(rowCount));
    }
};

//================================================================================

// NOTE - this delivers rows that are NOT 8 byte aligned, so can't safely be used to allocate ptr arrays

class PackedFixedSizeHeaplet : public ChunkedHeaplet
{
    //NOTE: This class should not contain any more data - otherwise the dataOffset() may be calculated incorrectly
    struct ChunkHeader
    {
        atomic_t count;
    };
public:
    enum { chunkHeaderSize = sizeof(ChunkHeader) };

    PackedFixedSizeHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t size, unsigned _allocatorId)
        : ChunkedHeaplet(_heap, _allocatorCache, size, size - chunkHeaderSize)
    {
        sharedAllocatorId = _allocatorId;
    }

    virtual bool _isShared(const void *_ptr) const
    {
        return inlineIsShared(_ptr);
    }

    virtual void noteReleased(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
        unsigned rowCount = atomic_dec_and_read(&header->count);
        if (ROWCOUNT(rowCount) == 0)
        {
            if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
                allocatorCache->onDestroy(sharedAllocatorId & MAX_ACTIVITY_ID, ptr + chunkHeaderSize);

            inlineReleasePointer(ptr);
        }
    }

    virtual void _internalReleaseNoDestructor(const void * _ptr)
    {
        char *ptr = (char *) _ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
        unsigned rowCount = atomic_dec_and_read(&header->count);
        assertex(ROWCOUNT(rowCount) == 0);
        inlineReleasePointer(ptr);
    }


    virtual void noteLinked(const void *_ptr)
    {
        inlineNoteLinked(_ptr);
    }

    virtual void * initChunk(char * chunk, unsigned allocatorId)
    {
        char * ret = chunk;
        ChunkHeader * header = (ChunkHeader *)ret;
        atomic_set(&header->count, 1);
        ret += chunkHeaderSize;
#ifdef _CLEAR_ALLOCATED_ROW
        memset(ret, 0xcc, chunkCapacity);
#endif
        return ret;
    }

    virtual const void * doMoveRow(char * chunk, const void * row)
    {
        char * ret = chunk;

        //Clone across the information from the old row
        char * oldPtr = (char *)row - chunkHeaderSize;
        ChunkHeader * oldHeader = (ChunkHeader *)oldPtr;
        ChunkHeader * newHeader = (ChunkHeader *)ret;
        unsigned rowCountValue = atomic_read(&oldHeader->count);
        unsigned destructFlag = (rowCountValue & ROWCOUNT_DESTRUCTOR_FLAG);
        atomic_set(&newHeader->count, 1|destructFlag);
        ret += chunkHeaderSize;
        memcpy(ret, row, chunkCapacity);

        //If this was the only instance then release the old row without calling the destructor
        //(to avoid the overhead of having to call onClone).
        if (ROWCOUNT(rowCountValue) == 1)
            HeapletBase::internalReleaseNoDestructor(row);
        else
        {
            if (destructFlag)
                allocatorCache->onClone(sharedAllocatorId & MAX_ACTIVITY_ID, ret);
            ReleaseRoxieRow(row);
        }
        return ret;
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const
    {
        unsigned numLeaked = queryCount()-1;
        //Because there is only a 4 byte counter on each field which is reused for the field list
        //it isn't possible to walk the rows in 0..freeBase and see if they are allocated or not
        //so have to be content with a summary
        if (numLeaked)
            logctx.CTXLOG("Packed allocator for size %"I64F"u activity %u leaks %u rows", (unsigned __int64) chunkCapacity, getActivityId(sharedAllocatorId), numLeaked);
        leaked -= numLeaked;
    }

    virtual unsigned _rawAllocatorId(const void *ptr) const
    {
        return sharedAllocatorId;
    }

    virtual void checkHeap() const
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        while (base < limit)
        {
            const char *block = data() + base;
            ChunkHeader * header = (ChunkHeader *)block;
            unsigned rowCount = atomic_read(&header->count);
            if (ROWCOUNT(rowCount) != 0)
            {
                //MORE: Potential race: could be freed while the pointer is being checked
                if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
                    allocatorCache->checkValid(sharedAllocatorId & MAX_ACTIVITY_ID, block + chunkHeaderSize);
            }
            base += chunkSize;
        }
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned numAllocs = queryCount()-1;
        if (numAllocs)
        {
            map->noteMemUsage(sharedAllocatorId, numAllocs * chunkSize, numAllocs);
        }
    }
};

//================================================================================
// Row manager - chunking

class CHugeHeap;
class HugeHeaplet : public Heaplet
{
protected:
    unsigned allocatorId;
    atomic_t rowCount;  // A separate rowcount is required, otherwise the page could be freed before the destructor is called

    inline unsigned _sizeInPages() const
    {
        return PAGES(chunkCapacity + dataOffset(), HEAP_ALIGNMENT_SIZE);
    }

    static inline memsize_t calcCapacity(memsize_t requestedSize)
    {
        return align_pow2(requestedSize + dataOffset(), HEAP_ALIGNMENT_SIZE) - dataOffset();
    }

    inline char *data() const
    {
        return ((char *) this) + dataOffset();
    }

public:
    HugeHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, memsize_t _hugeSize, unsigned _allocatorId) : Heaplet(_heap, _allocatorCache, calcCapacity(_hugeSize))
    {
        allocatorId = _allocatorId;
    }

    memsize_t setCapacity(memsize_t newsize)
    {
        chunkCapacity = calcCapacity(newsize);
        return chunkCapacity;
    }

    bool _isShared(const void *ptr) const
    {
        return atomic_read(&rowCount) > 1;
    }

    virtual unsigned sizeInPages() 
    {
        return _sizeInPages();
    }

    static inline unsigned dataOffset()
    {
        return HEAPLET_DATA_AREA_OFFSET(HugeHeaplet);
    }

    void operator delete(void * p)
    {
        // MORE: Depending on the members/methods of an Object in the delete operator 
        //      is not a good idea, specially if another class derives from it.
        //      This is OK for the time being since no object derive from this
        //      and we provide/control both operators, new and delete.
        //
        //      I thought of alloc extra (4 bytes) at allocation for communication
        //      between new and delete were the size of the memory is stored, and what is
        //      returned from new is a ptr passed the 4 bytes, but that may not work fine
        //      with HeapletBase::findbase() called from release. Might want to put at end
        //      of alloc space !!!! Future work/design...
        subfree_aligned(p, ((HugeHeaplet*)p)->_sizeInPages());
    }

    virtual void noteReleased(const void *ptr)
    {
        if (atomic_dec_and_test(&rowCount))
        {
            if (allocatorId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
                allocatorCache->onDestroy(allocatorId & MAX_ACTIVITY_ID, (void *)ptr);

            CHeap * savedHeap = heap;
            // after the following dec(count) it is possible that the page could be freed, so cannot access any members of this
            compiler_memory_barrier();
            unsigned cnt = atomic_dec_and_read(&count);
            assertex(cnt == 1);
            noteEmptyPage(savedHeap);
        }
    }

    virtual void _setDestructorFlag(const void *ptr)
    {
        //MORE: This should probably use the top bit of the count as well.
        allocatorId |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
    }

    virtual bool _hasDestructor(const void *ptr) const
    {
        return (allocatorId & ACTIVITY_FLAG_NEEDSDESTRUCTOR) != 0;
    }

    virtual unsigned _rawAllocatorId(const void *ptr) const
    {
        return allocatorId;
    }

    virtual void noteLinked(const void *ptr)
    {
        atomic_inc(&rowCount);
    }

    void *allocateHuge(memsize_t size)
    {
        atomic_inc(&count);
        atomic_set(&rowCount, 1);
        dbgassertex(size <= chunkCapacity);
#ifdef _CLEAR_ALLOCATED_HUGE_ROW
        memset(data(), 0xcc, chunkCapacity);
#endif
        return data();
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const 
    {
        logctx.CTXLOG("Block size %"I64F"u was allocated by activity %u and not freed", (unsigned __int64) chunkCapacity, getActivityId(allocatorId));
        leaked--;
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const 
    {
        map->noteMemUsage(allocatorId, chunkCapacity, 1);
        map->noteHeapUsage(chunkCapacity, RHFpacked, _sizeInPages(), chunkCapacity);
    }

    virtual void checkHeap() const
    {
        //MORE
    }

    virtual const void * _compactRow(const void * ptr, HeapCompactState & state)
    {
        return ptr;
    }

    virtual void _internalReleaseNoDestructor(const void *ptr)
    {
        throwUnexpected();
    }
};

//================================================================================
//
struct ActivityEntry 
{
    unsigned allocatorId;
    unsigned allocations;
    memsize_t usage;
};

struct HeapEntry : public CInterface
{
public:
    HeapEntry(memsize_t _allocatorSize, RoxieHeapFlags _heapFlags, memsize_t _numPages, memsize_t _memUsed) :
        allocatorSize(_allocatorSize), heapFlags(_heapFlags), numPages(_numPages), memUsed(_memUsed)
    {
    }

    memsize_t allocatorSize;
    RoxieHeapFlags heapFlags;
    memsize_t numPages;
    memsize_t memUsed;
};

typedef MapBetween<unsigned, unsigned, ActivityEntry, ActivityEntry> MapActivityToActivityEntry;

class CActivityMemoryUsageMap : public CInterface, implements IActivityMemoryUsageMap
{
    MapActivityToActivityEntry map;
    CIArrayOf<HeapEntry> heaps;
    memsize_t maxUsed;
    memsize_t totalUsed;
    unsigned allocatorIdMax;

public:
    IMPLEMENT_IINTERFACE;
    CActivityMemoryUsageMap()
    {
        maxUsed = 0;
        totalUsed = 0;
        allocatorIdMax = 0;
    }

    virtual void noteMemUsage(unsigned allocatorId, memsize_t memUsed, unsigned numAllocs)
    {
        totalUsed += memUsed;
        ActivityEntry *ret = map.getValue(allocatorId);
        if (ret)
        {
            memUsed += ret->usage;
            ret->usage = memUsed;
            ret->allocations += numAllocs;
        }
        else
        {
            ActivityEntry e = {allocatorId, numAllocs, memUsed};
            map.setValue(allocatorId, e);
        }
        if (memUsed > maxUsed)
        {
            maxUsed = memUsed;
            allocatorIdMax = allocatorId;
        }
    }

    void noteHeapUsage(memsize_t allocatorSize, RoxieHeapFlags heapFlags, memsize_t numPages, memsize_t memUsed)
    {
        heaps.append(*new HeapEntry(allocatorSize, heapFlags, numPages, memUsed));
    }

    static int sortUsage(const void *_l, const void *_r)
    {
        const ActivityEntry *l = *(const ActivityEntry **) _l;
        const ActivityEntry *r = *(const ActivityEntry **) _r;
        return l->usage - r->usage;
    }

    virtual void report(const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache)
    {
        if (logctx.queryTraceLevel())
        {
            ActivityEntry **results = new ActivityEntry *[map.count()];
            HashIterator i(map);
            unsigned j = 0;
            for(i.first();i.isValid();i.next())
            {
                IMapping &cur = i.query();
                results[j] = map.mapToValue(&cur);
                j++;
            }
            qsort(results, j, sizeof(results[0]), sortUsage);
            logctx.CTXLOG("Snapshot of peak memory usage: %"I64F"u total bytes allocated", (unsigned __int64) totalUsed);
            while (j)
            {
                j--;
                unsigned activityId = getRealActivityId(results[j]->allocatorId, allocatorCache);
                logctx.CTXLOG("%"I64F"u bytes allocated by activity %u (%u allocations)", (unsigned __int64) results[j]->usage, activityId, results[j]->allocations);
            }

            memsize_t totalHeapPages = 0;
            memsize_t totalHeapUsed = 0;
            logctx.CTXLOG("Heaps:");
            ForEachItemIn(iHeap, heaps)
            {
                HeapEntry & cur = heaps.item(iHeap);
                StringBuffer flags;
                if (cur.heapFlags & RHFpacked)
                    flags.append("P");
                if (cur.heapFlags & RHFunique)
                    flags.append("U");
                if (cur.heapFlags & RHFvariable)
                    flags.append("V");

                //Should never be called with numPages == 0, but protect against divide by zero in case of race condition etc.
                unsigned __int64 memReserved = cur.numPages * HEAP_ALIGNMENT_SIZE;
                unsigned percentUsed = cur.numPages ? (unsigned)((cur.memUsed * 100) / memReserved) : 100;
                logctx.CTXLOG("size: %"I64F"u [%s] reserved: %"I64F"up %u%% (%"I64F"u/%"I64F"u) used",
                        (unsigned __int64) cur.allocatorSize, flags.str(), (unsigned __int64) cur.numPages, percentUsed, (unsigned __int64) cur.memUsed, (unsigned __int64) memReserved);
                totalHeapPages += cur.numPages;
                totalHeapUsed += cur.memUsed;
            }

            //Should never be called with numPages == 0, but protect against divide by zero in case of race condition etc.
            unsigned __int64 totalReserved = totalHeapPages * HEAP_ALIGNMENT_SIZE;
            unsigned percentUsed = totalHeapPages ? (unsigned)((totalHeapUsed * 100) / totalReserved) : 100;
            logctx.CTXLOG("Total: %"I64F"up %u%% (%"I64F"u/%"I64F"u) used",
                    (unsigned __int64) totalHeapPages, percentUsed, (unsigned __int64) totalHeapUsed, (unsigned __int64) totalReserved);

            logctx.CTXLOG("------------------ End of snapshot");
            delete [] results;
        }
    }

    virtual void reportStatistics(IStatisticTarget & target, unsigned detailtarget, const IRowAllocatorCache *allocatorCache)
    {
        ActivityEntry **results = new ActivityEntry *[map.count()];
        HashIterator i(map);
        unsigned j = 0;
        for(i.first();i.isValid();i.next())
        {
            IMapping &cur = i.query();
            results[j] = map.mapToValue(&cur);
            j++;
        }
        qsort(results, j, sizeof(results[0]), sortUsage);
        StringBuffer activityText;
        while (j)
        {
            j--;
            unsigned allocatorId = results[j]->allocatorId;
            unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
            activityText.clear();
            if (allocatorId & ACTIVITY_FLAG_ISREGISTERED)
                activityText.append("ac").append(activityId);
            else if ((allocatorId & MAX_ACTIVITY_ID) == UNKNOWN_ROWSET_ID)
                activityText.append("rowset");
            else
                activityText.append("ac").append(allocatorId & MAX_ACTIVITY_ID);
            target.addStatistic(NULL, activityText.str(), "roxiepeakmem", NULL, SMEASURE_MEM_KB, results[j]->usage / 1024, results[j]->allocations, 0, false);
        }
        delete [] results;

        target.addStatistic(NULL, NULL, "roxiepeakmem", NULL, SMEASURE_MEM_KB, totalUsed / 1024, 1, 0, false);
    }
};

//================================================================================
//
class CChunkingRowManager;
class CFixedChunkedHeap;
class CPackedChunkingHeap;

class CRoxieFixedRowHeapBase : implements IFixedRowHeap, public CInterface
{
public:
    CRoxieFixedRowHeapBase(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags)
        : rowManager(_rowManager), allocatorId(_allocatorId), flags(_flags)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual void *finalizeRow(void *final);
    virtual void beforeDispose();

    //This should never be called while rows are being allocated since that implies the row manager is being killed
    //too early.  => No need to protect with a critical section.
    virtual void clearRowManager()
    {
        rowManager = NULL;
    }

protected:
    CChunkingRowManager * rowManager;       // Lifetime of rowManager is guaranteed to be longer
    unsigned allocatorId;
    RoxieHeapFlags flags;
};


class CRoxieFixedRowHeap : public CRoxieFixedRowHeapBase
{
public:
    CRoxieFixedRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags, size32_t _chunkCapacity)
        : CRoxieFixedRowHeapBase(_rowManager, _allocatorId, _flags), chunkCapacity(_chunkCapacity)
    {
    }

    virtual void *allocate();

protected:
    size32_t chunkCapacity;
};


class CRoxieDirectFixedRowHeap : public CRoxieFixedRowHeapBase
{
public:
    CRoxieDirectFixedRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags, CFixedChunkedHeap * _heap)
        : CRoxieFixedRowHeapBase(_rowManager, _allocatorId, _flags), heap(_heap)
    {
    }

    virtual void *allocate();

    virtual void clearRowManager()
    {
        heap.clear();
        CRoxieFixedRowHeapBase::clearRowManager();
    }

protected:
    Owned<CFixedChunkedHeap> heap;
};

class CRoxieDirectPackedRowHeap : public CRoxieFixedRowHeapBase
{
public:
    CRoxieDirectPackedRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags, CPackedChunkingHeap * _heap)
        : CRoxieFixedRowHeapBase(_rowManager, _allocatorId, _flags), heap(_heap)
    {
    }

    virtual void *allocate();

    virtual void clearRowManager()
    {
        heap.clear();
        CRoxieFixedRowHeapBase::clearRowManager();
    }

protected:
    Owned<CPackedChunkingHeap> heap;
};

//================================================================================
//
class CRoxieVariableRowHeap : implements IVariableRowHeap, public CInterface
{
public:
    CRoxieVariableRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags)
        : rowManager(_rowManager), allocatorId(_allocatorId), flags(_flags)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual void *allocate(memsize_t size, memsize_t & capacity);
    virtual void *resizeRow(void * original, memsize_t copysize, memsize_t newsize, memsize_t &capacity);
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize);

protected:
    CChunkingRowManager * rowManager;       // Lifetime of rowManager is guaranteed to be longer
    unsigned allocatorId;
    RoxieHeapFlags flags;
};


//================================================================================
//
//Responsible for allocating memory for a chain of chunked blocks
class CHeap : public CInterface
{
public:
    CHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, unsigned _flags)
        : logctx(_logctx), rowManager(_rowManager), allocatorCache(_allocatorCache), active(NULL), flags(_flags)
    {
        atomic_set(&possibleEmptyPages, 0);
    }

    ~CHeap()
    {
        Heaplet *finger = active;
        while (finger)
        {
            if (memTraceLevel >= 3)
                logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor freeing heaplet linked in active list - addr=%p rowMgr=%p",
                        finger, this);
            Heaplet *next = getNext(finger);
            delete finger;
            finger = next;
        }
        active = NULL;
    }

    void reportLeaks(unsigned &leaked) const
    {
        SpinBlock c1(crit);
        Heaplet *finger = active;
        while (leaked && finger)
        {
            if (leaked && memTraceLevel >= 1)
                finger->reportLeaks(leaked, logctx);
            finger = getNext(finger);
        }
    }

    void checkHeap()
    {
        SpinBlock c1(crit);
        Heaplet *finger = active;
        while (finger)
        {
            finger->checkHeap();
            finger = getNext(finger);
        }
    }

    unsigned allocated()
    {
        unsigned total = 0;
        SpinBlock c1(crit);
        Heaplet *finger = active;
        while (finger)
        {
            total += finger->queryCount() - 1; // There is one refcount for the page itself on the active q
            finger = getNext(finger);
        }
        return total;
    }

    unsigned releaseEmptyPages(bool forceFreeAll)
    {
        if (atomic_read(&possibleEmptyPages) == 0)
            return 0;

        //You will get a false positive if possibleEmptyPages is set while walking the active page list, but that
        //only mean the list is walked more than it needs to be.
        //If releaseEmptyPages() is called between the last release on a page (setting count to 1), and this flag
        //getting set, it won't release the page *this time*.  But that is the same as the release happening
        //slightly later.
        atomic_set(&possibleEmptyPages, 0);
        unsigned total = 0;
        Heaplet *prev = NULL;
        SpinBlock c1(crit);
        Heaplet *finger = active;
        while (finger)
        {
            Heaplet *next = getNext(finger);
            if (finger->queryCount()==1)
            {
                //If this is the only page then only free it if forced to.
                if (!prev && !next && !forceFreeAll)
                {
                    //There is still a potential page to release so reset the flag.
                    atomic_set(&possibleEmptyPages, 1);
                    break;
                }

                if (memTraceLevel >= 3)
                    logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::pages() freeing Heaplet linked in active list - addr=%p pages=%u capacity=%"I64F"u rowMgr=%p",
                            finger, finger->sizeInPages(), (unsigned __int64) finger->_capacity(), this);
                if (prev)
                    setNext(prev, next);
                else
                    active = next;

                total += finger->sizeInPages();
                delete finger;
            }
            else
            {
                prev = finger;
            }
            finger = next;
        }

        return total;
    }

    void getPeakActivityUsage(IActivityMemoryUsageMap * usageMap) const
    {
        unsigned numPages = 0;
        memsize_t numAllocs = 0;
        {
            SpinBlock c1(crit);
            Heaplet *finger = active;
            while (finger)
            {
                unsigned thisCount = finger->queryCount()-1;
                if (thisCount != 0)
                    finger->getPeakActivityUsage(usageMap);
                numAllocs += thisCount;
                numPages++;
                finger = getNext(finger);
            }
        }
        if (numPages)
            reportHeapUsage(usageMap, numPages, numAllocs);
    }

    inline bool isEmpty() const { return !active; }

    virtual bool matches(size32_t searchSize, unsigned searchActivity, unsigned searchFlags) const
    {
        return false;
    }

    void noteEmptyPage() { atomic_set(&possibleEmptyPages, 1); }

    inline void internalLock() { crit.enter(); }
    inline void internalUnlock() { crit.leave(); }


protected:
    virtual void reportHeapUsage(IActivityMemoryUsageMap * usageMap, unsigned numPages, memsize_t numAllocs) const = 0;

    inline Heaplet * getNext(const Heaplet * ptr) const { return ptr->next; }
    inline void setNext(Heaplet * ptr, Heaplet * next) const { ptr->next = next; }

    inline void moveHeapletToHead(Heaplet * prev, Heaplet * newHead)
    {
        Heaplet * next = getNext(newHead);
        setNext(prev, next);
        setNext(newHead, active);
        active = newHead;
    }

protected:
    unsigned flags; // before the pointer so it packs better in 64bit.
    Heaplet *active;
    CChunkingRowManager * rowManager;
    const IRowAllocatorCache *allocatorCache;
    const IContextLogger & logctx;
    mutable SpinLock crit;      // MORE: Can probably be a NonReentrantSpinLock if we're careful
    atomic_t possibleEmptyPages;  // Are there any pages with 0 records.  Primarily here to avoid walking long page chains.
};


//---------------------------------------------------------------------------------------------------------------------
class HeapCompactState
{
public:
    inline HeapCompactState() : numPagesEmptied(0), heap(NULL), next(NULL)
    {
    }

    inline ~HeapCompactState()
    {
        if (heap)
            heap->internalUnlock();
    }


    inline void protectHeap(CHeap * _heap)
    {
        if (heap != _heap)
        {
            if (heap)
                heap->internalUnlock();
            heap = _heap;
            if (_heap)
                _heap->internalLock();
            next = NULL;
        }
    }
public:
    unsigned numPagesEmptied;
    CHeap * heap;
    Heaplet * next; // which heaplet to try to compact into next.  Not so good if > 1 heaps in use.
};

//---------------------------------------------------------------------------------------------------------------------

class CHugeHeap : public CHeap
{
public:
    CHugeHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache)
        : CHeap(_rowManager, _logctx, _allocatorCache, 0)
    {
    }

    void * doAllocate(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost);
    bool expandHeap(void * original, memsize_t copysize, memsize_t oldcapacity, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback);

protected:
    HugeHeaplet * allocateHeaplet(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost);

    virtual void reportHeapUsage(IActivityMemoryUsageMap * usageMap, unsigned numPages, memsize_t numAllocs) const
    {
        //Already processed in HugeHeaplet::getPeakActivityUsage(IActivityMemoryUsageMap *map) const
    }
};

class CChunkedHeap : public CHeap
{
public:
    CChunkedHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, unsigned _chunkSize, unsigned _flags)
        : CHeap(_rowManager, _logctx, _allocatorCache, _flags), chunkSize(_chunkSize)
    {
        chunksPerPage  = FixedSizeHeaplet::dataAreaSize() / chunkSize;
    }

    void * doAllocate(unsigned allocatorId, unsigned maxSpillCost);

    virtual void reportHeapUsage(IActivityMemoryUsageMap * usageMap, unsigned numPages, memsize_t numAllocs) const
    {
        usageMap->noteHeapUsage(chunkSize, (RoxieHeapFlags)flags, numPages, chunkSize * numAllocs);
    }

    const void * compactRow(const void * ptr, HeapCompactState & state);

    inline unsigned maxChunksPerPage() const { return chunksPerPage; }

protected:
    inline void * inlineDoAllocate(unsigned allocatorId, unsigned maxSpillCost);
    virtual ChunkedHeaplet * allocateHeaplet() = 0;

protected:
    size32_t chunkSize;
    unsigned chunksPerPage;
};

class CFixedChunkedHeap : public CChunkedHeap
{
public:
    CFixedChunkedHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, size32_t _chunkSize, unsigned _flags, unsigned _defaultSpillCost)
        : CChunkedHeap(_rowManager, _logctx, _allocatorCache, _chunkSize, _flags), defaultSpillCost(_defaultSpillCost)
    {
    }

    void * allocate(unsigned allocatorId);

    virtual bool matches(size32_t searchSize, unsigned /*searchActivity*/, unsigned searchFlags) const
    {
        //Check the size matches, and any flags we are interested in.
        return (searchSize == chunkSize) &&
               (searchFlags == flags);
    }

protected:
    virtual ChunkedHeaplet * allocateHeaplet();

protected:
    unsigned defaultSpillCost;
};

class CPackedChunkingHeap : public CChunkedHeap
{
public:
    CPackedChunkingHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, size32_t _chunkSize, unsigned _flags, unsigned _allocatorId, unsigned _defaultSpillCost)
        : CChunkedHeap(_rowManager, _logctx, _allocatorCache, _chunkSize, _flags), allocatorId(_allocatorId), defaultSpillCost(_defaultSpillCost)
    {
    }

    void * allocate();

    virtual bool matches(size32_t searchSize, unsigned searchActivity, unsigned searchFlags) const
    {
        return (searchSize == chunkSize) &&
               (searchFlags == flags) &&
               (allocatorId == searchActivity);
    }

protected:
    virtual ChunkedHeaplet * allocateHeaplet();

protected:
    unsigned allocatorId;
    unsigned defaultSpillCost;
};

//================================================================================
//

void noteEmptyPage(CHeap * const heap)
{
    heap->noteEmptyPage();
}


char * ChunkedHeaplet::allocateChunk()
{
    char *ret;
    const size32_t size = chunkSize;
    loop
    {
        unsigned old_blocks = atomic_read(&r_blocks);
        unsigned r_ret = (old_blocks & RBLOCKS_OFFSET_MASK);
        if (r_ret)
        {
            ret = makeAbsolute(r_ret);
            //may have been allocated by another thread, but still legal to dereference
            //the cas will fail if the contents are invalid.
            unsigned next = *(unsigned *)ret;

            //There is a potential ABA problem if other thread(s) allocate two or more items, and free the first
            //item in the window before the following cas.  r_block would match, but next would be invalid.
            //To avoid that a tag is stored in the top bits of r_blocks which is modified whenever an item is added
            //onto the free list.  The offsets in the freelist do not need tags.
            unsigned new_blocks = (old_blocks & RBLOCKS_CAS_TAG_MASK) | next;
            if (atomic_cas(&r_blocks, new_blocks, old_blocks))
                break;
        }
        else
        {
            unsigned curFreeBase = atomic_read(&freeBase);
            //There is no ABA issue on freeBase because it is never decremented (and no next chain with it)
            size32_t bytesFree = dataAreaSize() - curFreeBase;
            if (bytesFree >= size)
            {
                if (atomic_cas(&freeBase, curFreeBase + size, curFreeBase))
                {
                    ret = data() + curFreeBase;
                    break;
                }
            }
            else
                return NULL;
        }
    }

    atomic_inc(&count);
    return ret;
}

const void * ChunkedHeaplet::_compactRow(const void * ptr, HeapCompactState & state)
{
    //NB: If this already belongs to a full heaplet then leave it where it is..
    CChunkedHeap * chunkedHeap = static_cast<CChunkedHeap *>(heap);
    if (numChunks() == chunkedHeap->maxChunksPerPage())
        return ptr;
    return chunkedHeap->compactRow(ptr, state);
}

//================================================================================
#define ROUNDED(heap, size) (unsigned)((heap) * HEAP_ALIGNMENT_SIZE + (size))
#define ROUNDEDSIZE(rounded) ((rounded) & ((size32_t)HEAP_ALIGNMENT_SIZE -1))
#define ROUNDEDHEAP(rounded) ((rounded) / (size32_t)HEAP_ALIGNMENT_SIZE)

class BufferedRowCallbackManager
{
    friend class ReleaseBufferThread;

    class BackgroundReleaseBufferThread : public Thread
    {
    public:
        BackgroundReleaseBufferThread(BufferedRowCallbackManager * _owner)
        : Thread("BackgroundReleaseBufferThread"), owner(_owner)
        {
        }

        virtual int run()
        {
            owner->runReleaseBufferThread();
            return 0;
        }

    private:
        BufferedRowCallbackManager * owner;
    };

    class ReleaseBufferThread : public Thread
    {
    public:
        ReleaseBufferThread(BufferedRowCallbackManager & _owner)
        : Thread("ReleaseBufferThread"), owner(_owner), abort(false)
        {
            args.critical = false;
            args.result = NULL;
        }

        virtual int run()
        {
            loop
            {
                goSem.wait();
                if (abort)
                    break;
                //This class is only used for stress testing => free all we can.
                args.result = owner.releaseBuffersNow(SpillAllCost, args.critical, false, 0);
                doneSem.signal();
            }
            return 0;
        }

        bool releaseBuffers(unsigned maxSpillCost, const bool critical)
        {
            if (isCurrentThread())
                return owner.releaseBuffersNow(maxSpillCost, critical, false, 0);

            bool ok;
            {
                CriticalBlock block(cs);
                args.critical = critical;
                goSem.signal();
                doneSem.wait();
                ok = args.result;
            }
            return ok;
        }

        void stop()
        {
            //This should not be called while any active heap operations could be occuring
            CriticalBlock block(cs);
            abort = true;
            goSem.signal();
        }

    private:
        BufferedRowCallbackManager & owner;
        CriticalSection cs;
        Semaphore goSem;
        Semaphore doneSem;
        struct
        {
            bool critical;
            bool result;
        } args;
        bool abort;
    };

public:
    BufferedRowCallbackManager(IRowManager * _owner) : owner(_owner)
    {
        atomic_set(&releasingBuffers, 0);
        atomic_set(&releaseSeq, 0);
        abortBufferThread = false;
        minCallbackThreshold = 1;
        releaseWhenModifyCallback = false;
        releaseWhenModifyCallbackCritical = false;
        backgroundReleaseCost = SpillAllCost;
    }
    ~BufferedRowCallbackManager()
    {
        stopReleaseBufferThread();
    }

    //If this sequence number has changed then it is likely that some rows have been freed up, so worth
    //trying to allocate again.
    inline unsigned getReleaseSeq() { return atomic_read(&releaseSeq); }

    void addRowBuffer(IBufferedRowCallback * callback)
    {
        if (releaseWhenModifyCallback)
            releaseBuffers(SpillAllCost, releaseWhenModifyCallbackCritical, false, 0);

        CriticalBlock block(callbackCrit);
        //Assuming a small number so perform an insertion sort.
        unsigned max = rowBufferCallbacks.ordinality();
        unsigned cost = callback->getSpillCost();
        unsigned insertPos = 0;
        for (; insertPos < max; insertPos++)
        {
            IBufferedRowCallback * curCallback = rowBufferCallbacks.item(insertPos);
            if (curCallback->getSpillCost() > cost)
                break;
        }
        rowBufferCallbacks.add(callback, insertPos);
        updateCallbackInfo();
    }

    void removeRowBuffer(IBufferedRowCallback * callback)
    {
        if (releaseWhenModifyCallback)
            releaseBuffers(SpillAllCost, releaseWhenModifyCallbackCritical, false, 0);

        CriticalBlock block(callbackCrit);
        rowBufferCallbacks.zap(callback);
        updateCallbackInfo();
    }

    void updateCallbackInfo()
    {
        //Possibly over the top, but calculate information so we can do a round robin at various
        //different levels of cost
        callbackRanges.kill();
        nextCallbacks.kill();
        nextCallbacks.append(0);
        unsigned prevCost = 0;
        ForEachItemIn(i, rowBufferCallbacks)
        {
            unsigned cost = rowBufferCallbacks.item(i)->getSpillCost();
            if (i && (cost != prevCost))
            {
                callbackRanges.append(i);
                nextCallbacks.append(i);
            }
            prevCost = cost;
        }
        callbackRanges.append(rowBufferCallbacks.ordinality());
    }

    bool callbackReleasesRows(IBufferedRowCallback * callback, bool critical)
    {
        //If already processing this callback then don't call it again
        if (activeCallbacks.contains(callback))
        {
            DBGLOG("RoxieMemMgr: allocation in IBufferedRowCallback[%p]::freeBufferedRows() requested new page", callback);
            return false;
        }

        activeCallbacks.append(callback);
        try
        {
            bool ok = callback->freeBufferedRows(critical);
            activeCallbacks.pop();
            return ok;
        }
        catch (...)
        {
            activeCallbacks.pop();
            throw;
        }
    }

    //Release buffers will ensure that the rows are attempted to be cleaned up before returning
    bool releaseBuffers(unsigned maxSpillCost, const bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        if (!releaseBuffersThread)
            return releaseBuffersNow(maxSpillCost, critical, checkSequence, prevReleaseSeq);
        return releaseBuffersThread->releaseBuffers(maxSpillCost, critical);
    }

    void runReleaseBufferThread()
    {
        loop
        {
            releaseBuffersSem.wait();
            if (abortBufferThread)
                break;
            releaseBuffersNow(backgroundReleaseCost, false, false, 0);
            atomic_set(&releasingBuffers, 0);
        }
    }

    void releaseBuffersInBackground()
    {
        if (atomic_cas(&releasingBuffers, 1, 0))
        {
            assertex(backgroundReleaseBuffersThread);
            releaseBuffersSem.signal();
        }
    }

    void startReleaseBufferThread(unsigned maxSpillCost)
    {
        backgroundReleaseCost = maxSpillCost;
        if (!backgroundReleaseBuffersThread)
        {
            backgroundReleaseBuffersThread.setown(new BackgroundReleaseBufferThread(this));
            backgroundReleaseBuffersThread->start();
        }
    }

    void stopReleaseBufferThread()
    {
        if (backgroundReleaseBuffersThread)
        {
            abortBufferThread = true;
            releaseBuffersSem.signal();
            backgroundReleaseBuffersThread->join();
            backgroundReleaseBuffersThread.clear();
            abortBufferThread = false;
        }
    }

    void setCallbackOnThread(bool value)
    {
        //May crash if called in parallel with any ongoing memory operations.
        if (value)
        {
            if (!releaseBuffersThread)
            {
                releaseBuffersThread.setown(new ReleaseBufferThread(*this));
                releaseBuffersThread->start();
            }
        }
        else
        {
            if (releaseBuffersThread)
            {
                releaseBuffersThread->stop();
                releaseBuffersThread->join();
                releaseBuffersThread.clear();
            }
        }
    }

    void setMemoryCallbackThreshold(unsigned value)
    {
        minCallbackThreshold = value;
    }

    virtual void setReleaseWhenModifyCallback(bool value, bool critical)
    {
        releaseWhenModifyCallback = value;
        releaseWhenModifyCallbackCritical = critical;
    }

protected:
    bool doReleaseBuffers(unsigned maxSpillCost, const bool critical, const unsigned minSuccess)
    {
        const unsigned numCallbacks = rowBufferCallbacks.ordinality();
        if (numCallbacks == 0)
            return false;

        unsigned first = 0;
        unsigned numSuccess = 0;
        //Loop through each set of different costs
        ForEachItemIn(level, callbackRanges)
        {
            if (rowBufferCallbacks.item(first)->getSpillCost() > maxSpillCost)
                break;

            unsigned last = callbackRanges.item(level);
            unsigned start = nextCallbacks.item(level);
            unsigned cur = start;
            assertex(cur >= first && cur < last);
            //First perform a round robin on the elements with the same cost
            loop
            {
                IBufferedRowCallback * curCallback = rowBufferCallbacks.item(cur);
                unsigned next = cur+1;
                if (next == last)
                    next = first;


                if (callbackReleasesRows(curCallback, critical))
                {
                    if (++numSuccess >= minSuccess)
                    {
                        nextCallbacks.replace(next, level);
                        return true;
                    }
                }

                cur = next;
                if (cur == start)
                    break;
            }
            first = last;
        }
        return (numSuccess != 0);
    }

    //Release buffers will ensure that the rows are attempted to be cleaned up before returning
    bool releaseBuffersNow(unsigned maxSpillCost, const bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        const unsigned minSuccess = minCallbackThreshold;
        CriticalBlock block(callbackCrit);
        //While we were waiting check if something freed some memory up
        //if so try again otherwise we might end up calling more callbacks than we need to.
        if (checkSequence && (prevReleaseSeq != getReleaseSeq()))
            return true;

        //Call non critical first, then critical - if applicable.
        //Should there be more levels of importance than critical/non critical?
        if (doReleaseBuffers(maxSpillCost, false, minSuccess) || (critical && doReleaseBuffers(maxSpillCost, true, minSuccess)))
        {
            //Increment first so that any called knows some rows may have been freed
            atomic_inc(&releaseSeq);
            owner->releaseEmptyPages(critical);
            //incremented again because some rows may now have been freed.  A difference may give a
            //false positive, but better than a false negative.
            atomic_inc(&releaseSeq);
            return true;
        }
        else if (owner->releaseEmptyPages(critical))
        {
            atomic_inc(&releaseSeq);
            return true;
        }
        return false;
    }

protected:
    CriticalSection callbackCrit;
    Semaphore releaseBuffersSem;
    PointerArrayOf<IBufferedRowCallback> rowBufferCallbacks;
    PointerArrayOf<IBufferedRowCallback> activeCallbacks;
    Owned<BackgroundReleaseBufferThread> backgroundReleaseBuffersThread;
    Owned<ReleaseBufferThread> releaseBuffersThread;
    UnsignedArray callbackRanges;  // the maximum index of the callbacks for the nth cost
    UnsignedArray nextCallbacks;  // the next call back to try and free for the nth cost
    IRowManager * owner;
    atomic_t releasingBuffers;  // boolean if pre-emptive releasing thread is active
    atomic_t releaseSeq;
    unsigned minCallbackThreshold;
    unsigned backgroundReleaseCost;
    bool releaseWhenModifyCallback;
    bool releaseWhenModifyCallbackCritical;
    volatile bool abortBufferThread;
};

//Constants are here to ensure they can all be constant folded
const unsigned roundupDoubleLimit = 2048;  // Values up to this limit are rounded to the nearest power of 2
const unsigned roundupStepSize = 4096;  // Above the roundupDoubleLimit memory for a row is allocated in this size step
const unsigned limitStepBlock = FixedSizeHeaplet::maxHeapSize()/MAX_FRAC_ALLOCATOR;  // until it gets to this size
const unsigned numStepBlocks = PAGES(limitStepBlock, roundupStepSize); // how many step blocks are there?
const unsigned maxStepSize = numStepBlocks * roundupStepSize;
const bool hasAnyStepBlocks = roundupDoubleLimit <= roundupStepSize;
const unsigned firstFractionalHeap = (FixedSizeHeaplet::dataAreaSize()/(maxStepSize+ALLOC_ALIGNMENT))+1;

class CVariableRowResizeCallback : public IRowResizeCallback
{
public:
    inline CVariableRowResizeCallback(memsize_t & _capacity, void * & _row) : capacity(_capacity), row(_row) {}

    virtual void lock() { }
    virtual void unlock() { }
    virtual void update(memsize_t size, void * ptr) { capacity = size; row = ptr; }
    virtual void atomicUpdate(memsize_t size, void * ptr) { capacity = size; row = ptr; }

public:
    memsize_t & capacity;
    void * & row;
};



class CChunkingRowManager : public CInterface, implements IRowManager
{
    friend class CRoxieFixedRowHeap;
    friend class CRoxieVariableRowHeap;
    friend class CHugeHeap;
    friend class CChunkedHeap;
    friend class CFixedChunkedHeap;

    CriticalSection activeBufferCS; // Potentially slow
    mutable SpinLock peakSpinLock; // Very small window, low contention so fine to be a spin lock
    mutable SpinLock fixedSpinLock; // Main potential for contention releasingEmptyHeaps and gathering peak usage.  Shouldn't be likely.
                                    // Should possibly be a ReadWriteLock - better with high contention, worse with low
    CIArrayOf<CFixedChunkedHeap> normalHeaps;
    CHugeHeap hugeHeap;
    ITimeLimiter *timeLimit;
    DataBufferBase *activeBuffs;
    const IContextLogger &logctx;
    unsigned pageLimit;
    unsigned spillPageLimit;
    unsigned peakPages;
    unsigned dataBuffs;
    unsigned dataBuffPages;
    atomic_t possibleGoers;
    atomic_t totalHeapPages;
    BufferedRowCallbackManager callbacks;
    Owned<IActivityMemoryUsageMap> peakUsageMap;
    CIArrayOf<CHeap> fixedHeaps;
    CopyCIArrayOf<CRoxieFixedRowHeapBase> fixedRowHeaps;  // These are observed, NOT linked
    const IRowAllocatorCache *allocatorCache;
    unsigned __int64 cyclesChecked;       // When we last checked timelimit
    unsigned __int64 cyclesCheckInterval; // How often we need to check timelimit
    bool ignoreLeaks;
    bool trackMemoryByActivity;
    bool minimizeFootprint;
    bool minimizeFootprintCritical;

    inline unsigned getActivityId(unsigned rawId) const
    {
        return getRealActivityId(rawId, allocatorCache);
    }

public:
    IMPLEMENT_IINTERFACE;

    CChunkingRowManager(memsize_t _memLimit, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, bool _ignoreLeaks)
        : callbacks(this), logctx(_logctx), allocatorCache(_allocatorCache), hugeHeap(this, _logctx, _allocatorCache)
    {
        logctx.Link();
        //Use roundup() to calculate the sizes of the different heaps, and double check that the heap mapping
        //is consistent.  This ensures each rounded size has a unique heap.
        size32_t prevSize = FixedSizeHeaplet::chunkHeaderSize;
        while (prevSize < FixedSizeHeaplet::maxHeapSize() + FixedSizeHeaplet::chunkHeaderSize)
        {
            size32_t rounded = roundup(prevSize+1);
            dbgassertex(ROUNDEDHEAP(rounded) == normalHeaps.ordinality());
            size32_t thisSize = ROUNDEDSIZE(rounded);
            normalHeaps.append(*new CFixedChunkedHeap(this, _logctx, _allocatorCache, thisSize, RHFvariable, SpillAllCost));
            prevSize = thisSize;
        }
        pageLimit = (unsigned) PAGES(_memLimit, HEAP_ALIGNMENT_SIZE);
        spillPageLimit = 0;
        timeLimit = _tl;
        peakPages = 0;
        dataBuffs = 0;
        atomic_set(&possibleGoers, 0);
        atomic_set(&totalHeapPages, 0);
        activeBuffs = NULL;
        dataBuffPages = 0;
        ignoreLeaks = _ignoreLeaks;
        minimizeFootprint = false;
        minimizeFootprintCritical = false;
#ifdef _DEBUG
        trackMemoryByActivity = true; 
#else
        trackMemoryByActivity = false;
#endif
        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager c-tor memLimit=%"I64F"u pageLimit=%u rowMgr=%p", (unsigned __int64)_memLimit, pageLimit, this);
        if (timeLimit)
        {
            cyclesChecked = get_cycles_now();
            cyclesCheckInterval = nanosec_to_cycle(1000000 * timeLimit->checkInterval());
        }
        else
        {
            cyclesChecked = 0;
            cyclesCheckInterval = 0;
        }
    }

    ~CChunkingRowManager()
    {
        callbacks.stopReleaseBufferThread();

        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor pageLimit=%u peakPages=%u dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p", 
                    pageLimit, peakPages, dataBuffs, dataBuffPages, atomic_read(&possibleGoers), this);

        if (!ignoreLeaks)
            reportLeaks(2);

        //Ensure that the rowHeaps release any references to the fixed heaps, and no longer call back when they
        //are destroyed
        {
            SpinBlock block(fixedSpinLock);
            ForEachItemIn(i, fixedRowHeaps)
                fixedRowHeaps.item(i).clearRowManager();
        }

        DataBufferBase *dfinger = activeBuffs;
        while (dfinger)
        {
            DataBufferBase *next = dfinger->next;
            if (memTraceLevel >= 2 && dfinger->queryCount()!=1)
                logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active dataBuffer list - addr=%p rowMgr=%p", 
                        dfinger->queryCount()-1, dfinger, this);
            dfinger->next = NULL;
            dfinger->mgr = NULL; // Avoid calling back to noteDataBufferReleased, which would be unhelpful
            atomic_set(&dfinger->count, 0);
            dfinger->released();
            dfinger = next;
        }
        logctx.Release();
    }

    virtual void reportLeaks()
    {
        reportLeaks(1);
    }

    virtual void checkHeap()
    {
        ForEachItemIn(iNormal, normalHeaps)
            normalHeaps.item(iNormal).checkHeap();

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occuring
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & fixedHeap = fixedHeaps.item(i);
            fixedHeap.checkHeap();
        }
    }

    virtual void setActivityTracking(bool val)
    {
        trackMemoryByActivity = val;
    }

    virtual unsigned allocated()
    {
        unsigned total = 0;
        ForEachItemIn(iNormal, normalHeaps)
            total += normalHeaps.item(iNormal).allocated();
        total += hugeHeap.allocated();

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occuring
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & fixedHeap = fixedHeaps.item(i);
            total += fixedHeap.allocated();
        }

        return total;
    }

    virtual unsigned numPagesAfterCleanup(bool forceFreeAll)
    {
        releaseEmptyPages(forceFreeAll);
        return dataBuffPages + atomic_read(&totalHeapPages);
    }

    void removeUnusedHeaps()
    {
        SpinBlock block(fixedSpinLock);
        unsigned numHeaps = fixedHeaps.ordinality();
        unsigned i = 0;
        while (i < numHeaps)
        {
            CHeap & fixedHeap = fixedHeaps.item(i);
            if (fixedHeap.isEmpty() && !fixedHeap.IsShared())
            {
                fixedHeaps.remove(i);
                numHeaps--;
            }
            else
                i++;
        }
    }
    virtual bool releaseEmptyPages(bool forceFreeAll)
    {
        unsigned total = 0;
        ForEachItemIn(iNormal, normalHeaps)
            total += normalHeaps.item(iNormal).releaseEmptyPages(forceFreeAll);
        total += hugeHeap.releaseEmptyPages(true);

        bool hadUnusedHeap = false;
        {
            SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occuring
            ForEachItemIn(i, fixedHeaps)
            {
                CHeap & fixedHeap = fixedHeaps.item(i);
                total += fixedHeap.releaseEmptyPages(forceFreeAll);
                //if this heap has no pages, and no external references then it can be removed
                if (fixedHeap.isEmpty() && !fixedHeap.IsShared())
                    hadUnusedHeap = true;
            }
        }

        if (hadUnusedHeap)
            removeUnusedHeaps();

        if (total)
            atomic_add(&totalHeapPages, -(int)total);
        return (total != 0);
    }

    void getPeakActivityUsage()
    {
        Owned<IActivityMemoryUsageMap> map = getActivityUsage();

        SpinBlock block(peakSpinLock);
        peakUsageMap.setown(map.getClear());
    }

    //MORE: inline??
    static size32_t roundup(size32_t size)
    {
        dbgassertex((size >= FixedSizeHeaplet::chunkHeaderSize) && (size <= FixedSizeHeaplet::maxHeapSize() + FixedSizeHeaplet::chunkHeaderSize));
        //MORE: A binary chop on sizes is likely to be better.
        if (size<=256)
        {
            if (size<=64)
            {
                if (size<=32)
                {
                    if (size<=16)
                        return ROUNDED(0, 16);
                    return ROUNDED(1, 32);
                }
                return ROUNDED(2, 64);
            }
            if (size<=128)
                return ROUNDED(3, 128);
            return ROUNDED(4, 256);
        }
        if (size<=1024)
        {
            if (size<=512)
                return ROUNDED(5, 512);
            return ROUNDED(6, 1024);
        }
        if (size<=2048)
            return ROUNDED(7, 2048);

        if (size <= maxStepSize)
        {
            size32_t blocks = PAGES(size, roundupStepSize);
            return ROUNDED(7 + blocks, blocks * roundupStepSize);
        }

        unsigned baseBlock = 7;
        if (hasAnyStepBlocks)
            baseBlock += numStepBlocks;

        // Ensure the size is rounded up so it is correctly aligned. (Note the size already includes chunk overhead.)
        unsigned minsize = align_pow2(size, ALLOC_ALIGNMENT);

        // What fraction of the data area would this allocation take up?
        unsigned frac = FixedSizeHeaplet::dataAreaSize()/minsize;

        // Calculate the size of that fraction of the data.  Round down the divided size to ensure it is correctly aligned.
        // (Round down to guarantee that frac allocations will fit in the data area).
        size32_t usesize = FixedSizeHeaplet::dataAreaSize()/frac;
        size32_t alignedsize = usesize &~ (ALLOC_ALIGNMENT-1);
        dbgassertex(alignedsize >= size);

        return ROUNDED(baseBlock + (firstFractionalHeap-frac), alignedsize);
    }

    inline void beforeAllocate(memsize_t _size, unsigned activityId)
    {
        if (memTraceSizeLimit && _size >= memTraceSizeLimit)
        {
            logctx.CTXLOG("Activity %u requesting %"I64F"u bytes!", getActivityId(activityId), (unsigned __int64) _size);
            PrintStackReport();
        }
        if (timeLimit)
        {
            unsigned __int64 cyclesNow = get_cycles_now();
            if (cyclesNow - cyclesChecked >= cyclesCheckInterval)
            {
                timeLimit->checkAbort();
                cyclesChecked = cyclesNow;  // No need to lock - worst that can happen is we call too often which is harmless
            }
        }
    }

    virtual void *allocate(memsize_t _size, unsigned activityId)
    {
        beforeAllocate(_size, activityId);
        if (_size > FixedSizeHeaplet::maxHeapSize())
            return hugeHeap.doAllocate(_size, activityId, SpillAllCost);
        size32_t size32 = (size32_t) _size;

        size32_t rounded = roundup(size32 + FixedSizeHeaplet::chunkHeaderSize);
        size32_t whichHeap = ROUNDEDHEAP(rounded);
        CFixedChunkedHeap & normalHeap = normalHeaps.item(whichHeap);
        return normalHeap.doAllocate(activityId, SpillAllCost);
    }

    virtual void *allocate(memsize_t _size, unsigned activityId, unsigned maxSpillCost)
    {
        beforeAllocate(_size, activityId);
        if (_size > FixedSizeHeaplet::maxHeapSize())
            return hugeHeap.doAllocate(_size, activityId, maxSpillCost);
        size32_t size32 = (size32_t) _size;

        size32_t rounded = roundup(size32 + FixedSizeHeaplet::chunkHeaderSize);
        size32_t whichHeap = ROUNDEDHEAP(rounded);
        CFixedChunkedHeap & normalHeap = normalHeaps.item(whichHeap);
        return normalHeap.doAllocate(activityId, maxSpillCost);
    }

    virtual const char *cloneVString(size32_t len, const char *str)
    {
        //Converting a empty string to a vstring should return a real string - not NULL
        char *ret = (char *) allocate(len+1, 0);
        memcpy(ret, str, len);
        ret[len] = 0;
        return (const char *) ret;
    }

    virtual const char *cloneVString(const char *str)
    {
        if (str)
            return cloneVString(strlen(str), str);
        else
            return NULL;
    }

    virtual void setMemoryLimit(memsize_t bytes, memsize_t spillSize, unsigned backgroundReleaseCost)
    {
        memsize_t systemMemoryLimit = getTotalMemoryLimit();
        if (bytes > systemMemoryLimit)
            bytes = systemMemoryLimit;
        if (spillSize > systemMemoryLimit)
            spillSize = systemMemoryLimit;
        pageLimit = (unsigned) PAGES(bytes, HEAP_ALIGNMENT_SIZE);
        spillPageLimit = (unsigned) PAGES(spillSize, HEAP_ALIGNMENT_SIZE);

        //The test allows no limit on memory, but spill above a certain amount.  Not sure if useful...
        if (spillPageLimit && (pageLimit != spillPageLimit))
            callbacks.startReleaseBufferThread(backgroundReleaseCost);
        else
            callbacks.stopReleaseBufferThread();

        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::setMemoryLimit new memlimit=%"I64F"u pageLimit=%u spillLimit=%u rowMgr=%p", (unsigned __int64) bytes, pageLimit, spillPageLimit, this);
    }

    virtual void setMemoryCallbackThreshold(unsigned value)
    {
        callbacks.setMemoryCallbackThreshold(value);
    }

    virtual void setCallbackOnThread(bool value)
    {
        callbacks.setCallbackOnThread(value);
    }

    virtual void setMinimizeFootprint(bool value, bool critical)
    {
        minimizeFootprint = value;
        minimizeFootprintCritical = critical;
    }

    virtual void setReleaseWhenModifyCallback(bool value, bool critical)
    {
        callbacks.setReleaseWhenModifyCallback(value, critical);
    }

    virtual void resizeRow(memsize_t &capacity, void * & ptr, memsize_t copysize, memsize_t newsize, unsigned activityId)
    {
        void * const original = ptr;
        assertex(newsize);
        assertex(!HeapletBase::isShared(original));
        memsize_t curCapacity = HeapletBase::capacity(original);
        if (newsize <= curCapacity)
        {
            //resizeRow never shrinks memory
            capacity = curCapacity;
            return;
        }
        if (curCapacity > FixedSizeHeaplet::maxHeapSize())
        {
            CVariableRowResizeCallback callback(capacity, ptr);
            hugeHeap.expandHeap(original, copysize, curCapacity, newsize, activityId, SpillAllCost, callback);
            return;
        }

        void *ret = allocate(newsize, activityId);
        memcpy(ret, original, copysize);
        memsize_t newCapacity = HeapletBase::capacity(ret);
        HeapletBase::release(original);
        capacity = newCapacity;
        ptr = ret;
    }

    virtual bool resizeRow(void * original, memsize_t copysize, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback)
    {
        assertex(newsize);
        assertex(!HeapletBase::isShared(original));
        memsize_t curCapacity = HeapletBase::capacity(original);
        if (newsize <= curCapacity)
        {
            //resizeRow never shrinks memory
            return true;
        }
        if (curCapacity > FixedSizeHeaplet::maxHeapSize())
            return hugeHeap.expandHeap(original, copysize, curCapacity, newsize, activityId, maxSpillCost, callback);

        void *ret = allocate(newsize, activityId, maxSpillCost);
        if (!ret)
            return false;
        memcpy(ret, original, copysize);
        memsize_t newCapacity = HeapletBase::capacity(ret);
        callback.atomicUpdate(newCapacity, ret);
        HeapletBase::release(original);
        return true;
    }

    virtual void *finalizeRow(void * original, memsize_t initialSize, memsize_t finalSize, unsigned activityId)
    {
        dbgassertex(finalSize);
        dbgassertex(!HeapletBase::isShared(original));
        dbgassertex(finalSize<=initialSize);

        // Finalize never shrinks
        // MORE - if we were paranoid we could assert that supplied activityId matched the one stored with the row
        if (activityId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
            HeapletBase::setDestructorFlag(original);
        return original;
    }

    virtual unsigned getMemoryUsage()
    {
        Owned<IActivityMemoryUsageMap> map;
        {
            SpinBlock block(peakSpinLock);
            map.set(peakUsageMap);
        }
        if (map)
            map->report(logctx, allocatorCache);
        return peakPages;
    }

    virtual bool attachDataBuff(DataBuffer *dataBuff) 
    {
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: attachDataBuff() attaching DataBuff to rowMgr - addr=%p dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p", 
                    dataBuff, dataBuffs, dataBuffPages, atomic_read(&possibleGoers), this);

        dataBuff->Link();
        DataBufferBase *last = NULL;
        bool needCheck;
        {
            CriticalBlock b(activeBufferCS);
            DataBufferBase *finger = activeBuffs;
            while (finger && atomic_read(&possibleGoers))
            {
                // MORE - if we get a load of data in and none out this can start to bog down...
                DataBufferBase *next = finger->next;
                if (finger->queryCount()==1)
                {
                    if (memTraceLevel >= 4)
                        logctx.CTXLOG("RoxieMemMgr: attachDataBuff() detaching DataBuffer linked in active list - addr=%p rowMgr=%p",
                                finger, this);
                    finger->next = NULL;
                    finger->Release();
                    dataBuffs--;
                    atomic_dec(&possibleGoers);
                    if (last)
                        last->next = next;
                    else
                        activeBuffs = next;    // MORE - this is yukky code - surely there's a cleaner way!
                }
                else
                    last = finger;
                finger = next;
            }
            assert(dataBuff->next==NULL);
            dataBuff->next = activeBuffs;
            activeBuffs = dataBuff;
            dataBuffs++;

            unsigned ndataBuffPages = (dataBuffs * DATA_ALIGNMENT_SIZE) / HEAP_ALIGNMENT_SIZE;
            needCheck = (ndataBuffPages > dataBuffPages);
            dataBuffPages = ndataBuffPages;
        }
        if (needCheck)
        {
            const unsigned maxSpillCost = 0;
            checkLimit(0, maxSpillCost);
        }
        return true;
    }
    
    virtual void noteDataBuffReleased(DataBuffer *dataBuff)
    {
        atomic_inc(&possibleGoers);
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::noteDataBuffReleased dataBuffs=%u dataBuffPages=%u possibleGoers=%u dataBuff=%p rowMgr=%p", 
                    dataBuffs, dataBuffPages, atomic_read(&possibleGoers), dataBuff, this);
    }

    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags, unsigned maxSpillCost)
    {
        CRoxieFixedRowHeapBase * rowHeap = doCreateFixedRowHeap(fixedSize, activityId, roxieHeapFlags, maxSpillCost);

        SpinBlock block(fixedSpinLock);
        //The Row heaps are not linked by the row manager so it can determine when they are released.
        fixedRowHeaps.append(*rowHeap);
        return rowHeap;
    }

    void noteReleasedHeap(CRoxieFixedRowHeapBase * rowHeap)
    {
        SpinBlock block(fixedSpinLock);
        fixedRowHeaps.zap(*rowHeap);
    }

    virtual IVariableRowHeap * createVariableRowHeap(unsigned activityId, unsigned roxieHeapFlags)
    {
        //Although the activityId is passed here, there is nothing to stop multiple RowHeaps sharing the same ChunkAllocator
        return new CRoxieVariableRowHeap(this, activityId, (RoxieHeapFlags)roxieHeapFlags);
    }

    bool checkLimit(unsigned numRequested, unsigned maxSpillCost)
    {
        unsigned totalPages;
        releaseEmptyPages(false);
        if (minimizeFootprint)
            callbacks.releaseBuffers(SpillAllCost, minimizeFootprintCritical, false, 0);

        loop
        {
            unsigned lastReleaseSeq = callbacks.getReleaseSeq();
            //We need to ensure that the number of allocated pages is updated atomically so multiple threads can't all
            //succeed and have the total take them over the limit.
            unsigned numHeapPages = atomic_read(&totalHeapPages);
            unsigned pageCount = dataBuffPages + numHeapPages;
            totalPages = pageCount + numRequested;
            if (totalPages <= pageLimit)
            {
                //Use atomic_cas so that only one thread can increase the number of pages at a time.
                //(Don't use atomic_add because we need to check the limit hasn't been exceeded.)
                if (!atomic_cas(&totalHeapPages, numHeapPages + numRequested, numHeapPages))
                    continue;
                break;
            }
            else if (!pageLimit)
            {
                atomic_add(&totalHeapPages, numRequested);
                break;
            }

            if (releaseEmptyPages(true))
                continue;

            //Try and directly free up some buffers.  It is worth trying again if one of the release functions thinks it
            //freed up some memory.
            //The following reduces the nubmer of times the callback is called, but I'm not sure how this affects
            //performance.  I think better if a single free is likely to free up some memory, and worse if not.
            const bool skipReleaseIfAnotherThreadReleases = true;
            if (!callbacks.releaseBuffers(maxSpillCost, true, skipReleaseIfAnotherThreadReleases, lastReleaseSeq))
            {
                //Check if a background thread has freed up some memory.  That can be checked by a comparing value of a counter
                //which is incremented each time releaseBuffers is successful.
                if (lastReleaseSeq == callbacks.getReleaseSeq())
                {
                    //very unusual: another thread may have just released a lot of memory (e.g., it has finished), but
                    //the empty pages haven't been cleaned up
                    releaseEmptyPages(true);
                    if (numHeapPages == atomic_read(&totalHeapPages))
                    {
                        if (maxSpillCost != SpillAllCost)
                            return false;

                        VStringBuffer msg("Memory limit exceeded: current %u, requested %u, limit %u", pageCount, numRequested, pageLimit);
                        logctx.CTXLOG("%s", msg.str());
                        reportMemoryUsage(false);
                        PrintStackReport();
                        throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "%s", msg.str());
                    }
                }
            }
        }

        if (spillPageLimit && (totalPages > spillPageLimit))
        {
            callbacks.releaseBuffersInBackground();
        }

        if (totalPages > peakPages)
        {
            if (trackMemoryByActivity)
                getPeakActivityUsage();
            peakPages = totalPages;
        }
        return true;
    }

    virtual void reportPeakStatistics(IStatisticTarget & target, unsigned detail)
    {
        Owned<IActivityMemoryUsageMap> map;
        {
            SpinBlock block(peakSpinLock);
            map.set(peakUsageMap);
        }
        if (map)
            map->reportStatistics(target, detail, allocatorCache);
        target.addStatistic(NULL, NULL, "roxiehwm", NULL, SMEASURE_MEM_KB, peakPages * (HEAP_ALIGNMENT_SIZE / 1024), 1, 0, false);
    }

    void restoreLimit(unsigned numRequested)
    {
        atomic_add(&totalHeapPages, -(int)numRequested);
    }

    bool releaseCallbackMemory(unsigned maxSpillCost, bool critical)
    {
        return callbacks.releaseBuffers(maxSpillCost, critical, false, 0);
    }

protected:
    CRoxieFixedRowHeapBase * doCreateFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags, unsigned maxSpillCost)
    {
        if ((roxieHeapFlags & RHFoldfixed) || (fixedSize > FixedSizeHeaplet::maxHeapSize()))
            return new CRoxieFixedRowHeap(this, activityId, (RoxieHeapFlags)roxieHeapFlags, fixedSize);

        unsigned heapFlags = roxieHeapFlags & (RHFunique|RHFpacked);
        if (heapFlags & RHFpacked)
        {
            CPackedChunkingHeap * heap = createPackedHeap(fixedSize, activityId, heapFlags, maxSpillCost);
            return new CRoxieDirectPackedRowHeap(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
        }
        else
        {
            CFixedChunkedHeap * heap = createFixedHeap(fixedSize, activityId, heapFlags, maxSpillCost);
            return new CRoxieDirectFixedRowHeap(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
        }
    }

    CHeap * getExistingHeap(size32_t chunkSize, unsigned activityId, unsigned flags)
    {
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & heap = fixedHeaps.item(i);
            if (heap.matches(chunkSize, activityId, flags))
            {
                heap.Link();
                if (heap.isAlive())
                    return &heap;
            }
        }
        return NULL;
    }

    IActivityMemoryUsageMap * getActivityUsage() const
    {
        Owned<IActivityMemoryUsageMap> map = new CActivityMemoryUsageMap;
        ForEachItemIn(iNormal, normalHeaps)
            normalHeaps.item(iNormal).getPeakActivityUsage(map);
        hugeHeap.getPeakActivityUsage(map);

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occuring
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & fixedHeap = fixedHeaps.item(i);
            fixedHeap.getPeakActivityUsage(map);
        }
        return map.getClear();
    }

    CFixedChunkedHeap * createFixedHeap(size32_t size, unsigned activityId, unsigned flags, unsigned maxSpillCost)
    {
        dbgassertex(!(flags & RHFpacked));
        size32_t rounded = roundup(size + FixedSizeHeaplet::chunkHeaderSize);

        //If not unique I think it is quite possibly better to reuse one of the existing heaps used for variable length
        //Advantage is fewer pages uses.  Disadvantage is greater likelihood of fragementation being unable to copy
        //rows to consolidate them.  Almost certainly packed or unique will be set in that situation though.
        if (!(flags & RHFunique))
        {
            size32_t whichHeap = ROUNDEDHEAP(rounded);
            return LINK(&normalHeaps.item(whichHeap));
        }

        size32_t chunkSize = ROUNDEDSIZE(rounded);
        //Not time critical, so don't worry about the scope of the spinblock around the new
        SpinBlock block(fixedSpinLock);
        if (!(flags & RHFunique))
        {
            CHeap * match = getExistingHeap(chunkSize, activityId, flags);
            if (match)
                return static_cast<CFixedChunkedHeap *>(match);
        }

        CFixedChunkedHeap * heap = new CFixedChunkedHeap(this, logctx, allocatorCache, chunkSize, flags, maxSpillCost);
        fixedHeaps.append(*LINK(heap));
        return heap;
    }

    CPackedChunkingHeap * createPackedHeap(size32_t size, unsigned activityId, unsigned flags, unsigned maxSpillCost)
    {
        dbgassertex(flags & RHFpacked);
        //Must be 4 byte aligned otherwise the atomic increments on the counts may not be atomic
        //Should this have a larger granularity (e.g., sizeof(void * *) if size is above a threshold?
        size32_t chunkSize = align_pow2(size + PackedFixedSizeHeaplet::chunkHeaderSize, PACKED_ALIGNMENT);

        //Not time critical, so don't worry about the scope of the spinblock around the new
        SpinBlock block(fixedSpinLock);
        if (!(flags & RHFunique))
        {
            CHeap * match = getExistingHeap(chunkSize, activityId, flags);
            if (match)
                return static_cast<CPackedChunkingHeap *>(match);
        }

        CPackedChunkingHeap * heap = new CPackedChunkingHeap(this, logctx, allocatorCache, chunkSize, flags, activityId, maxSpillCost);
        fixedHeaps.append(*LINK(heap));
        return heap;
    }

    void reportLeaks(unsigned level)
    {
        unsigned leaked = allocated();
        if (leaked == 0)
            return;

        if (memTraceLevel >= level)
            logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active heaplet list - rowMgr=%p", leaked, this);
        if (leaked > maxLeakReport)
            leaked = maxLeakReport;

        ForEachItemIn(iNormal, normalHeaps)
            normalHeaps.item(iNormal).reportLeaks(leaked);
        hugeHeap.reportLeaks(leaked);
        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occuring
        ForEachItemIn(i, fixedHeaps)
        {
            if (leaked == 0)
                break;
            CHeap & fixedHeap = fixedHeaps.item(i);
            fixedHeap.reportLeaks(leaked);
        }
    }

    virtual void addRowBuffer(IBufferedRowCallback * callback)
    {
        callbacks.addRowBuffer(callback);
    }

    virtual void removeRowBuffer(IBufferedRowCallback * callback)
    {
        callbacks.removeRowBuffer(callback);
    }

    virtual bool compactRows(memsize_t count, const void * * rows)
    {
        HeapCompactState state;
        bool memoryAvailable = false;
        for (memsize_t i = 0; i < count; i++)
        {
            const void * row = rows[i];
            if (row)
            {
                const void * packed = HeapletBase::compactRow(row, state);
                rows[i] = packed;  // better to always assign
            }
        }
        return (state.numPagesEmptied != 0);
    }

    virtual void reportMemoryUsage(bool peak) const
    {
        if (peak)
        {
            Owned<IActivityMemoryUsageMap> map;
            {
                SpinBlock block(peakSpinLock);
                map.set(peakUsageMap);
            }
            if (map)
                map->report(logctx, allocatorCache);
        }
        else
        {
            logctx.CTXLOG("RoxieMemMgr: pageLimit=%u peakPages=%u dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p",
                          pageLimit, peakPages, dataBuffs, dataBuffPages, atomic_read(&possibleGoers), this);
            Owned<IActivityMemoryUsageMap> map = getActivityUsage();
            map->report(logctx, allocatorCache);
        }
    }

    virtual memsize_t getExpectedCapacity(memsize_t size, unsigned heapFlags)
    {
        if (size > FixedSizeHeaplet::maxHeapSize())
        {
            memsize_t numPages = PAGES(size + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
            return (numPages * HEAP_ALIGNMENT_SIZE) - HugeHeaplet::dataOffset();
        }
        size32_t size32 = (size32_t) size;

        if (heapFlags & RHFpacked)
            return align_pow2(size32 + PackedFixedSizeHeaplet::chunkHeaderSize, PACKED_ALIGNMENT) - PackedFixedSizeHeaplet::chunkHeaderSize;

        size32_t rounded = roundup(size32 + FixedSizeHeaplet::chunkHeaderSize);
        size32_t heapSize = ROUNDEDSIZE(rounded);
        return heapSize - FixedSizeHeaplet::chunkHeaderSize;
    }

    virtual memsize_t getExpectedFootprint(memsize_t size, unsigned heapFlags)
    {
        if (size > FixedSizeHeaplet::maxHeapSize())
        {
            memsize_t numPages = PAGES(size + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
            return (numPages * HEAP_ALIGNMENT_SIZE);
        }
        size32_t size32 = (size32_t) size;

        if (heapFlags & RHFpacked)
            return align_pow2(size32 + PackedFixedSizeHeaplet::chunkHeaderSize, PACKED_ALIGNMENT);

        size32_t rounded = roundup(size32 + FixedSizeHeaplet::chunkHeaderSize);
        size32_t heapSize = ROUNDEDSIZE(rounded);
        return heapSize;
    }
};


void CRoxieFixedRowHeapBase::beforeDispose()
{
    if (rowManager)
        rowManager->noteReleasedHeap(this);
}

void * CRoxieFixedRowHeapBase::finalizeRow(void *final)
{
    //MORE: Checking heap checks for not shared.
    if (flags & RHFhasdestructor)
        HeapletBase::setDestructorFlag(final);
    return final;
}

void * CRoxieFixedRowHeap::allocate()
{
    return rowManager->allocate(chunkCapacity, allocatorId);
}

void * CRoxieDirectFixedRowHeap::allocate()
{
    return heap->allocate(allocatorId);
}

void * CRoxieDirectPackedRowHeap::allocate()
{
    return heap->allocate();
}


void * CRoxieVariableRowHeap::allocate(memsize_t size, memsize_t & capacity)
{
    void * ret = rowManager->allocate(size, allocatorId);
    capacity = RoxieRowCapacity(ret);
    return ret;
}

void * CRoxieVariableRowHeap::resizeRow(void * original, memsize_t copysize, memsize_t newsize, memsize_t &capacity)
{
    rowManager->resizeRow(capacity, original, copysize, newsize, allocatorId);
    return original;
}

void * CRoxieVariableRowHeap::finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize)
{
    //If rows never shrink then the following is sufficient.
    if (flags & RHFhasdestructor)
        HeapletBase::setDestructorFlag(final);
    return final;
}

//================================================================================

//MORE: Make this a nested class??
HugeHeaplet * CHugeHeap::allocateHeaplet(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost)
{
    unsigned numPages = PAGES(_size + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);

    loop
    {
        if (!rowManager->checkLimit(numPages, maxSpillCost))
            return NULL;

        //If the allocation fails, then try and free some memory by calling the callbacks
        void * memory = suballoc_aligned(numPages, true);
        if (memory)
            return new (memory) HugeHeaplet(this, allocatorCache, _size, allocatorId);

        rowManager->restoreLimit(numPages);
        if (!rowManager->releaseCallbackMemory(maxSpillCost, true))
        {
            if (maxSpillCost == SpillAllCost)
                throwHeapExhausted(numPages);
            return NULL;
        }
    }
}

void * CHugeHeap::doAllocate(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost)
{
    HugeHeaplet *head = allocateHeaplet(_size, allocatorId, maxSpillCost);
    if (!head)
    {
        if (memTraceLevel >= 2)
        {
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %"I64F"u) failed to allocate a block - pageLimit=%u peakPages=%u rowMgr=%p",
                (unsigned __int64) _size, rowManager->pageLimit, rowManager->peakPages, this);
        }
        return NULL;
    }

    if (memTraceLevel >= 2 || (memTraceSizeLimit && _size >= memTraceSizeLimit))
    {
        unsigned numPages = head->sizeInPages();
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %"I64F"u) allocated new HugeHeaplet size %"I64F"u - addr=%p pages=%u pageLimit=%u peakPages=%u rowMgr=%p",
            (unsigned __int64) _size, (unsigned __int64) (numPages*HEAP_ALIGNMENT_SIZE), head, numPages, rowManager->pageLimit, rowManager->peakPages, this);
    }

    SpinBlock b(crit);
    setNext(head, active);
    active = head;
    return head->allocateHuge(_size);
}

bool CHugeHeap::expandHeap(void * original, memsize_t copysize, memsize_t oldcapacity, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback)
{
    unsigned newPages = PAGES(newsize + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
    unsigned oldPages = PAGES(oldcapacity + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
    assert(newPages > oldPages);
    unsigned numPages = newPages - oldPages;
    void *oldbase =  (void *) ((memsize_t) original & HEAP_ALIGNMENT_MASK);
    loop
    {
        // NOTE: we request permission only for the difference between the old
        // and new sizes, even though we may temporarily hold both. This not only
        // simplifies the code considerably, it's probably desirable
        if (!rowManager->checkLimit(numPages, maxSpillCost))
            return false;

        bool release = false;
        void *realloced = subrealloc_aligned(oldbase, oldPages, newPages);
        if (!realloced)
        {
            realloced = suballoc_aligned(newPages, true);
            release = true;
        }
        if (realloced)
        {
            HugeHeaplet *oldhead = (HugeHeaplet *) oldbase;
            HugeHeaplet *head = (HugeHeaplet *) realloced;
            if (realloced != oldbase)
            {
                // Remove the old block from the chain
                {
                    SpinBlock b(crit);
                    if (active==oldhead)
                    {
                        active = getNext(oldhead);
                    }
                    else
                    {
                        Heaplet *finger = active;
                        // Remove old pointer from the chain
                        while (finger)
                        {
                            Heaplet *next = getNext(finger);
                            if (next == oldhead)
                            {
                                setNext(finger, getNext(oldhead));
                                break;
                            }
                            else
                                finger = next;
                        }
                        assert(finger != NULL); // Should always have found it
                    }
                }

                //Copying data within the block => must lock for the duration
                if (!release)
                    callback.lock();

                // MORE - If we were really clever, we could manipulate the page table to avoid moving ANY data here...
                memmove(realloced, oldbase, copysize + HugeHeaplet::dataOffset());  // NOTE - assumes no trailing data (e.g. end markers)
                SpinBlock b(crit);
                // Add at front of chain
                setNext(head, active);
                active = head;
            }
            void * ret = (char *) realloced + HugeHeaplet::dataOffset();
            memsize_t newCapacity = head->setCapacity(newsize);
            if (release)
            {
                //Update the pointer before the old one becomes invalid
                callback.atomicUpdate(newCapacity, ret);

                subfree_aligned(oldbase, oldPages);
            }
            else
            {
                if (realloced != oldbase)
                {
                    //previously locked => update the pointer and then unlock
                    callback.update(newCapacity, ret);
                    callback.unlock();
                }
                else
                {
                    //Extended at the end - update the max capacity
                    callback.atomicUpdate(newCapacity, ret);
                }
            }

            return true;
        }

        //If the allocation fails, then try and free some memory by calling the callbacks

        rowManager->restoreLimit(numPages);
        if (!rowManager->releaseCallbackMemory(maxSpillCost, true))
        {
            if (maxSpillCost == SpillAllCost)
                throwHeapExhausted(newPages, oldPages);
            return false;
        }
    }
}


//An inline function used to common up the allocation code for fixed and non fixed sizes.
void * CChunkedHeap::inlineDoAllocate(unsigned allocatorId, unsigned maxSpillCost)
{
    //Only hold the spinblock while walking the list - so subsequent calls to checkLimit don't deadlock.
    //NB: The allocation is split into two - finger->allocateChunk, and finger->initializeChunk().
    //The latter is done outside the spinblock, to reduce the window for contention.
    ChunkedHeaplet * donorHeaplet;
    char * chunk;
    {
        Heaplet *prev = NULL;
        SpinBlock b(crit);
        Heaplet *finger = active;
        while (finger)
        {
            //This cast is safe because we are within a member of CChunkedHeap
            donorHeaplet = static_cast<ChunkedHeaplet *>(finger);
            chunk = donorHeaplet->allocateChunk();
            if (chunk)
            {
                if (prev)
                    moveHeapletToHead(prev, finger);

                //The code at the end of this function needs to be executed outside of the spinblock.
                //Just occasionally gotos are the best way of expressing something
                goto gotChunk;
            }
            prev = finger;
            finger = getNext(finger);
        }
    }

    loop
    {
        if (!rowManager->checkLimit(1, maxSpillCost))
            return NULL;

        donorHeaplet = allocateHeaplet();
        if (donorHeaplet)
            break;
        rowManager->restoreLimit(1);
        if (!rowManager->releaseCallbackMemory(maxSpillCost, true))
        {
            if (maxSpillCost == SpillAllCost)
                throwHeapExhausted(1);
            return NULL;
        }
    }

    if (memTraceLevel >= 5 || (memTraceLevel >= 3 && chunkSize > 32000))
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %u) allocated new FixedSizeHeaplet size %u - addr=%p pageLimit=%u peakPages=%u rowMgr=%p",
                chunkSize, chunkSize, donorHeaplet, rowManager->pageLimit, rowManager->peakPages, this);

    {
        SpinBlock b(crit);
        setNext(donorHeaplet, active);
        active = donorHeaplet;
        //If no protecting spinblock there would be a race e.g., if another thread allocates all the rows!
        chunk = donorHeaplet->allocateChunk();
        dbgassertex(chunk);
    }
    
gotChunk:
    //since a chunk has been allocated from donorHeaplet it cannot be released at this point.
    return donorHeaplet->initChunk(chunk, allocatorId);
}

const void * CChunkedHeap::compactRow(const void * ptr, HeapCompactState & state)
{
    Heaplet *prev = NULL;

    //Use protect heap instead of a lock, so that multiple compacts on the same heap (very likely) avoid
    //re-entering the critical sections.
    state.protectHeap(this);
    Heaplet *finger = state.next;
    if (!finger)
        finger = active;
    while (finger)
    {
       //This cast is safe because we are within a member of CChunkedHeap
        ChunkedHeaplet * chunkedFinger = static_cast<ChunkedHeaplet *>(finger);
        const void *ret = chunkedFinger->moveRow(ptr);
        if (ret)
        {
            //Instead of moving this block to the head of the list, save away the next block to try to put a block into
            //since we know what all blocks before this must be filled.
            state.next = finger;
            //if (prev)
            //    moveHeapletToHead(prev, finger);

            HeapletBase *srcBase = HeapletBase::findBase(ptr);
            if (srcBase->isEmpty())
            {
                state.numPagesEmptied++;
                //could call releaseEmptyPages(false) at this point since already in the crit section.
            }
            return ret;
        }
        prev = finger;
        dbgassertex(((ChunkedHeaplet*)finger)->numChunks() == maxChunksPerPage());
        finger = getNext(finger);
    }
    return ptr;
}

void * CChunkedHeap::doAllocate(unsigned activityId, unsigned maxSpillCost)
{
    return inlineDoAllocate(activityId, maxSpillCost);
}

//================================================================================

ChunkedHeaplet * CFixedChunkedHeap::allocateHeaplet()
{
    void * memory = suballoc_aligned(1, true);
    if (!memory)
        return NULL;
    return new (memory) FixedSizeHeaplet(this, allocatorCache, chunkSize);
}

void * CFixedChunkedHeap::allocate(unsigned activityId)
{
    rowManager->beforeAllocate(chunkSize-FixedSizeHeaplet::chunkHeaderSize, activityId);
    return inlineDoAllocate(activityId, defaultSpillCost);
}


ChunkedHeaplet * CPackedChunkingHeap::allocateHeaplet()
{
    void * memory = suballoc_aligned(1, true);
    if (!memory)
        return NULL;
    return new (memory) PackedFixedSizeHeaplet(this, allocatorCache, chunkSize, allocatorId);
}

void * CPackedChunkingHeap::allocate()
{
    rowManager->beforeAllocate(chunkSize-PackedFixedSizeHeaplet::chunkHeaderSize, allocatorId);
    return inlineDoAllocate(allocatorId, defaultSpillCost);
}


//================================================================================
// Buffer manager - blocked 

void DataBufferBase::noteReleased(const void *ptr)
{
    //The link counter is shared by all the rows that are contained in this DataBuffer
    if (atomic_dec_and_test(&count))
        released();
}

void DataBufferBase::noteLinked(const void *ptr)
{
    atomic_inc(&count);
}

bool DataBufferBase::_isShared(const void *ptr) const
{
    // Because the link counter is shared you cannot know if an individual pointer is shared
    throwUnexpected();
    return true;
}

void DataBufferBase::Release()
{
    if (atomic_read(&count)==2 && mgr)
        mgr->noteDataBuffReleased((DataBuffer*) this);
    if (atomic_dec_and_test(&count)) released(); 
}

void DataBuffer::released()
{
    assert(next == NULL);
    DataBufferBottom *bottom = (DataBufferBottom *)findBase(this);
    assert((char *)bottom != (char *)this);
    if (memTraceLevel >= 4)
        DBGLOG("RoxieMemMgr: DataBuffer::released() releasing DataBuffer - addr=%p", this);
    atomic_dec(&dataBuffersActive);
    bottom->addToFreeChain(this);
    HeapletBase::release(bottom);
}

bool DataBuffer::attachToRowMgr(IRowManager *rowMgr)
{
    try
    {
        assert(mgr == NULL);
        mgr = rowMgr;
        return (rowMgr->attachDataBuff(this));
    }
    catch (IException *E)
    {
        EXCLOG(E);
        ::Release(E);
        return false;
    }
}

memsize_t DataBuffer::_capacity() const { throwUnexpected(); }
void DataBuffer::_setDestructorFlag(const void *ptr) { throwUnexpected(); }

class CDataBufferManager : public CInterface, implements IDataBufferManager
{
    friend class DataBufferBottom;
    CriticalSection crit;
    char *curBlock;
    char *nextAddr;
    DataBufferBottom *freeChain;
    atomic_t freePending;

    void unlink(DataBufferBottom *goer)
    {
        // NOTE - this is called inside a CriticalBlock on crit
        if (goer->nextBottom == goer)
        {
            assert(goer == goer->prevBottom);
            assert(goer==freeChain);
            freeChain = NULL;
        }
        else
        {
            goer->prevBottom->nextBottom = goer->nextBottom;
            goer->nextBottom->prevBottom = goer->prevBottom;
            if (freeChain == goer)
                freeChain = goer->nextBottom;
        }
        if ((char *) goer==curBlock)
        {
            curBlock = NULL;
            nextAddr = NULL;
            assertex(!"ERROR: heap block freed too many times!");
        }
    }

    void freeUnused()
    {
        DataBufferBottom *finger = freeChain;
        if (finger)
        {
            loop
            {
                // NOTE - do NOT put a CriticalBlock c(finger->crit) here since:
                // 1. It's not needed - no-one modifies finger->nextBottom chain but me and I hold CDataBufferManager::crit
                // 2. finger->crit is about to get released back to the pool and it's important that it is not locked at the time!
                if (atomic_read(&finger->okToFree) == 1)
                {
                    assert(!finger->isAlive());
                    DataBufferBottom *goer = finger;
                    finger = finger->nextBottom;
                    unlink(goer);
                    if (memTraceLevel >= 3)
                        DBGLOG("RoxieMemMgr: DataBufferBottom::allocate() freeing DataBuffers Page - addr=%p", goer);
                    goer->~DataBufferBottom(); 
#ifdef _DEBUG
                    memset(goer, 0xcc, HEAP_ALIGNMENT_SIZE);
#endif
                    subfree_aligned(goer, 1);
                    atomic_dec(&dataBufferPages);
                }
                else
                    finger = finger->nextBottom;
                if (finger==freeChain)
                    break;
            }
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CDataBufferManager(size32_t size)
    {
        assertex(size==DATA_ALIGNMENT_SIZE);
        curBlock = NULL;
        nextAddr = NULL;
        freeChain = NULL;
        atomic_set(&freePending, 0);
    }

    DataBuffer *allocate()
    {
        CriticalBlock b(crit);

        if (memTraceLevel >= 5)
            DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() curBlock=%p nextAddr=%p", curBlock, nextAddr);

        if (atomic_cas(&freePending, 0, 1))
            freeUnused();

        loop
        {
            if (curBlock)
            {
                DataBufferBottom *bottom = (DataBufferBottom *) curBlock;
                CriticalBlock c(bottom->crit);
                if (bottom->freeChain)
                {
                    atomic_inc(&dataBuffersActive);
                    HeapletBase::link(curBlock);
                    DataBufferBase *x = bottom->freeChain;
                    bottom->freeChain = x->next;
                    x->next = NULL;
                    if (memTraceLevel >= 4)
                        DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() reallocated DataBuffer - addr=%p", x);
                    return ::new(x) DataBuffer();
                }
                else if ((memsize_t)(nextAddr - curBlock) <= HEAP_ALIGNMENT_SIZE-DATA_ALIGNMENT_SIZE)
                {
                    atomic_inc(&dataBuffersActive);
                    HeapletBase::link(curBlock);
                    DataBuffer *x = ::new(nextAddr) DataBuffer();
                    nextAddr += DATA_ALIGNMENT_SIZE;
                    if (nextAddr - curBlock == HEAP_ALIGNMENT_SIZE)
                    {
                        // MORE: May want to delete this "if" logic !!
                        //       and let it be handled in the similar logic of "else" part below.
                        HeapletBase::release(curBlock);
                        curBlock = NULL;
                        nextAddr = NULL;
                    }
                    if (memTraceLevel >= 4)
                        DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() allocated DataBuffer - addr=%p", x);
                    return x;
                }
                else
                {
                    HeapletBase::release(curBlock);
                    curBlock = NULL;
                    nextAddr = NULL;
                }
            }
            // see if a previous block that is waiting to be freed has any on its free chain
            DataBufferBottom *finger = freeChain;
            if (finger)
            {
                loop
                {
                    CriticalBlock c(finger->crit);
                    if (finger->freeChain)
                    {
                        HeapletBase::link(finger); // Link once for the reference we save in curBlock
                                                   // Release (to dec ref count) when no more free blocks in the page
                        if (finger->isAlive())
                        {
                            curBlock = (char *) finger;
                            nextAddr = curBlock + HEAP_ALIGNMENT_SIZE;
                            atomic_inc(&dataBuffersActive);
                            HeapletBase::link(finger); // and once for the value we are about to return
                            DataBufferBase *x = finger->freeChain;
                            finger->freeChain = x->next;
                            x->next = NULL;
                            if (memTraceLevel >= 4)
                                DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() reallocated DataBuffer - addr=%p", x);
                            return ::new(x) DataBuffer();
                        }
                    }
                    finger = finger->nextBottom;
                    if (finger==freeChain)
                        break;
                }
            }
            curBlock = nextAddr = (char *) suballoc_aligned(1, false);
            atomic_inc(&dataBufferPages);
            assertex(curBlock);
            if (memTraceLevel >= 3)
                    DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() allocated new DataBuffers Page - addr=%p", curBlock);
            freeChain = ::new(nextAddr) DataBufferBottom(this, freeChain);   // we never allocate the lowest one in the heap - used just for refcounting the rest
            nextAddr += DATA_ALIGNMENT_SIZE;
        }
    }

    void poolStats(StringBuffer &s)
    {
        if (memTraceLevel > 1) memmap(s); // MORE: may want to make a separate interface (poolMap) and control query for this
        else memstats(s);
        s.appendf(", DataBuffsActive=%d, DataBuffPages=%d", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        DBGLOG("%s", s.str());
    }
};

DataBufferBottom::DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain)
{
    atomic_set(&okToFree, 0);
    owner = _owner;
    if (ownerFreeChain)
    {
        nextBottom = ownerFreeChain;
        prevBottom = ownerFreeChain->prevBottom;
        ownerFreeChain->prevBottom = this;
        prevBottom->nextBottom = this;
    }
    else
    {
        prevBottom = this;
        nextBottom = this;
    }
    freeChain = NULL;
}

void DataBufferBottom::addToFreeChain(DataBufferBase * buffer)
{
    CriticalBlock b(crit);
    buffer->next = freeChain;
    freeChain = buffer;
}

void DataBufferBottom::released()
{
    // Not safe to free here as owner may be in the middle of allocating from it
    // instead, give owner a hint that it's worth thinking about freeing this page next time it is safe
    if (atomic_cas(&count, DEAD_PSEUDO_COUNT, 0))
    {
        atomic_set(&owner->freePending, 1);
        atomic_set(&okToFree, 1);
    }
}

void DataBufferBottom::noteReleased(const void *ptr)
{
    HeapletBase * base = realBase(ptr);
    if (base == this)
        DataBufferBase::noteReleased(ptr);
    else
        base->noteReleased(ptr);
}

void DataBufferBottom::noteLinked(const void *ptr)
{
    HeapletBase * base = realBase(ptr);
    if (base == this)
        DataBufferBase::noteLinked(ptr);
    else
        base->noteLinked(ptr);
}

bool DataBufferBottom::_isShared(const void *ptr) const
{
    HeapletBase * base = realBase(ptr);
    if (base == this)
        return DataBufferBase::_isShared(ptr);
    else
        return base->_isShared(ptr);
}

memsize_t DataBufferBottom::_capacity() const { throwUnexpected(); }
void DataBufferBottom::_setDestructorFlag(const void *ptr) { throwUnexpected(); }

//================================================================================
//

#ifdef __new_was_defined
#define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

extern IRowManager *createRowManager(memsize_t memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks)
{
    return new CChunkingRowManager(memLimit, tl, logctx, allocatorCache, ignoreLeaks);
}

extern void setMemoryStatsInterval(unsigned secs)
{
    statsCyclesInterval = nanosec_to_cycle(I64C(1000000000) * secs);
    lastStatsCycles = get_cycles_now();
}

extern void setTotalMemoryLimit(bool allowHugePages, memsize_t max, memsize_t largeBlockSize, ILargeMemCallback * largeBlockCallback)
{
    assertex(largeBlockSize == align_pow2(largeBlockSize, HEAP_ALIGNMENT_SIZE));
    unsigned totalMemoryLimit = (unsigned) (max / HEAP_ALIGNMENT_SIZE);
    unsigned largeBlockGranularity = (unsigned)(largeBlockSize / HEAP_ALIGNMENT_SIZE);
    if ((max != 0) && (totalMemoryLimit == 0))
        totalMemoryLimit = 1;
    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: Setting memory limit to %"I64F"d bytes (%u pages)", (unsigned __int64) max, totalMemoryLimit);
    initializeHeap(allowHugePages, totalMemoryLimit, largeBlockGranularity, largeBlockCallback);
}

extern memsize_t getTotalMemoryLimit()
{
    return (memsize_t)heapTotalPages * HEAP_ALIGNMENT_SIZE;
}

extern bool memPoolExhausted()
{
   return (heapAllocated == heapTotalPages);
}

extern unsigned getHeapAllocated()
{
   return heapAllocated;
}

extern unsigned getHeapPercentAllocated()
{
   return (heapAllocated * 100)/heapTotalPages;
}

extern unsigned getDataBufferPages()
{
   return atomic_read(&dataBufferPages);
}

extern unsigned getDataBuffersActive()
{
   return atomic_read(&dataBuffersActive);
}

extern IDataBufferManager *createDataBufferManager(size32_t size)
{
    return new CDataBufferManager(size);
}

extern void setDataAlignmentSize(unsigned size)
{
    if (memTraceLevel >= 3) 
        DBGLOG("RoxieMemMgr: setDataAlignmentSize to %u", size); 

    if (size==0x400 || size==0x2000)
        DATA_ALIGNMENT_SIZE = size;
    else
        throw MakeStringException(ROXIEMM_INVALID_MEMORY_ALIGNMENT, "Invalid parameter to setDataAlignmentSize %u", size);
}

} // namespace roxiemem

//============================================================================================================
#ifdef _USE_CPPUNIT
#include "unittests.hpp"

namespace roxiemem {

//================================================================================

//A simple implementation of a buffered rows class - used for testing
class SimpleRowBuffer : implements IBufferedRowCallback
{
public:
    SimpleRowBuffer(IRowManager * rowManager, unsigned _cost) : cost(_cost), rows(rowManager, 0, 1, UNKNOWN_ROWSET_ID)
    {
    }

//interface IBufferedRowCallback
    virtual unsigned getSpillCost() const { return cost; }
    virtual bool freeBufferedRows(bool critical)
    {
        RoxieOutputRowArrayLock block(rows);
        return spillRows();
    }

    void addRow(const void * row)
    {
        if (!rows.append(row))
        {
            {
                roxiemem::RoxieOutputRowArrayLock block(rows);
                spillRows();
                rows.flush();
            }
            if (!rows.append(row))
                throwUnexpected();
        }
    }

    bool spillRows()
    {
        unsigned numCommitted = rows.numCommitted();
        if (numCommitted == 0)
            return false;
        const void * * committed = rows.getBlock(numCommitted);
        for (unsigned i=0; i < numCommitted; i++)
            ReleaseRoxieRow(committed[i]);
        rows.noteSpilled(numCommitted);
        return true;
    }

    void kill()
    {
        RoxieOutputRowArrayLock block(rows);
        rows.kill();
    }

protected:
    DynamicRoxieOutputRowArray rows;
    unsigned cost;
};

//A buffered row class - used for testing
class CallbackBlockAllocator : implements IBufferedRowCallback
{
public:
    CallbackBlockAllocator(IRowManager * _rowManager, unsigned _size, unsigned _cost) : cost(_cost), rowManager(_rowManager), size(_size)
    {
        rowManager->addRowBuffer(this);
    }
    ~CallbackBlockAllocator()
    {
        rowManager->removeRowBuffer(this);
    }

//interface IBufferedRowCallback
    virtual unsigned getSpillCost() const { return cost; }

    void allocate()
    {
        row.setown(rowManager->allocate(size, 0));
    }

    void costAllocate(unsigned allocCost)
    {
        row.setown(rowManager->allocate(size, 0, allocCost));
    }

    inline bool hasRow() const { return row != NULL; }

protected:
    OwnedRoxieRow row;
    IRowManager * rowManager;
    unsigned size;
    unsigned cost;
};


//Free the block as soon as requested
class SimpleCallbackBlockAllocator : public CallbackBlockAllocator
{
public:
    SimpleCallbackBlockAllocator(IRowManager * _rowManager, unsigned _size, unsigned _cost)
        : CallbackBlockAllocator(_rowManager, _size, _cost)
    {
    }

    virtual bool freeBufferedRows(bool critical)
    {
        if (!row)
            return false;
        row.clear();
        return true;
    }
};

//Allocate another row before disposing of the first
class NastyCallbackBlockAllocator : public CallbackBlockAllocator
{
public:
    NastyCallbackBlockAllocator(IRowManager * _rowManager, unsigned _size, unsigned _cost)
        : CallbackBlockAllocator(_rowManager, _size, _cost)
    {
    }

    virtual bool freeBufferedRows(bool critical)
    {
        if (!row)
            return false;

        OwnedRoxieRow tempRow = rowManager->allocate(size, 0);
        row.clear();
        return true;
    }
};


const static bool useLargeMemory = false;   // Set to true to test compacting and other test on significant sized memory
const static unsigned smallMemory = 300;
const static unsigned largeMemory = 10100;

class IStdException : extends std::exception
{
    Owned<IException> jException;
public:
    IStdException(IException *E) throw() : jException(E) {};
   virtual ~IStdException() throw() {}
};

class RoxieMemTests : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( RoxieMemTests );
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testCostCallbacks);
        CPPUNIT_TEST(testRoundup);
        CPPUNIT_TEST(testBitmapThreading);
        CPPUNIT_TEST(testAllocSize);
        CPPUNIT_TEST(testHuge);
        CPPUNIT_TEST(testCas);
        CPPUNIT_TEST(testAll);
        CPPUNIT_TEST(testDatamanager);
        CPPUNIT_TEST(testBitmap);
        CPPUNIT_TEST(testDatamanagerThreading);
        CPPUNIT_TEST(testCallbacks);
        CPPUNIT_TEST(testRecursiveCallbacks);
        CPPUNIT_TEST(testCompacting);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieMemTests() : logctx(queryDummyContextLogger())
    {
    }

    ~RoxieMemTests()
    {
    }

protected:
    void testSetup()
    {
        printf("Heaplet: cacheline(%u) base(%u) fixedbase(%u) fixed(%u) packed(%u) huge(%u)\n",
                CACHE_LINE_SIZE, (size32_t)sizeof(Heaplet), (size32_t)sizeof(ChunkedHeaplet), (size32_t)sizeof(FixedSizeHeaplet), (size32_t)sizeof(PackedFixedSizeHeaplet), (size32_t)sizeof(HugeHeaplet));
        printf("Heap: fixed(%u) packed(%u) huge(%u)\n",
                (size32_t)sizeof(CFixedChunkedHeap), (size32_t)sizeof(CPackedChunkingHeap), (size32_t)sizeof(CHugeHeap));
        printf("IHeap: fixed(%u) directfixed(%u) packed(%u) variable(%u)\n",
                (size32_t)sizeof(CRoxieFixedRowHeap), (size32_t)sizeof(CRoxieDirectFixedRowHeap), (size32_t)sizeof(CRoxieDirectPackedRowHeap), (size32_t)sizeof(CRoxieVariableRowHeap));

        ASSERT(FixedSizeHeaplet::dataOffset() >= sizeof(FixedSizeHeaplet));
        ASSERT(PackedFixedSizeHeaplet::dataOffset() >= sizeof(PackedFixedSizeHeaplet));
        ASSERT(HugeHeaplet::dataOffset() >= sizeof(HugeHeaplet));

        initializeHeap(false, useLargeMemory ? largeMemory : smallMemory, 0, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    static int mc4(const void *x, const void *y)
    {
        return -memcmp(x, y, sizeof(void*));
    }

    void testDatamanager()
    {
        CDataBufferManager dm(DATA_ALIGNMENT_SIZE);
        int i;

        DataBuffer *pages[3000];
        for (i = 0; i < 3000; i++)
            pages[i] = 0;
        //printf("\n----Begin DataBuffsActive=%d, DataBuffPages=%d ------ \n", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        for (i = 0; i < 2046; i++)
            pages[i] = dm.allocate();
        //printf("\n----Mid 1 DataBuffsActive=%d, DataBuffPages=%d ------ \n", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        ASSERT(atomic_read(&dataBufferPages)==2);
        pages[1022]->Release(); // release from first page
        pages[1022] = 0;  
        pages[2100] = dm.allocate(); // allocate from first page 
        //printf("\n----Mid 2 DataBuffsActive=%d, DataBuffPages=%d ------ \n", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        ASSERT(atomic_read(&dataBufferPages)==2);
        pages[2101] = dm.allocate(); // allocate from a new page (third)
        //printf("\n----Mid 3 DataBuffsActive=%d, DataBuffPages=%d ------ \n", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        // Release all blocks, which releases all pages, except active one
        for (i = 0; i < 3000; i++)
            if (pages[i]) 
                pages[i]->Release();
        //printf("\n----End DataBuffsActive=%d, DataBuffPages=%d ------ \n", atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        dm.allocate()->Release();
        ASSERT(atomic_read(&dataBufferPages)==1);

        for (i = 0; i < 1022; i++)
            dm.allocate()->Release();
        dm.allocate()->Release();
        dm.allocate()->Release();
        ASSERT(atomic_read(&dataBufferPages)==1);

        for (i = 0; i < 2000; i++)
            pages[i] = dm.allocate();
        for (i = 0; i < 1000; i++)
            pages[i]->Release();
        for (i = 0; i < 1000; i++)
            pages[i] = dm.allocate();
        ASSERT(atomic_read(&dataBufferPages)==2);
        for (i = 0; i < 1999; i++)
            pages[i]->Release();
        pages[1999]->Release();
        dm.allocate()->Release();
        ASSERT(atomic_read(&dataBufferPages)==1);
    }

    void testDatamanagerThreading()
    {
        CDataBufferManager dm(DATA_ALIGNMENT_SIZE);

        class casyncfor: public CAsyncFor
        {
        public:
            casyncfor(CDataBufferManager &_dm) : dm(_dm) {}

            void Do(unsigned idx)
            {
                for (unsigned j=0; j < 1000; j++)
                {
                    DataBuffer *pages[10000];
                    unsigned i;
                    for (i = 0; i < 10000; i++)
                    {
                        pages[i] = dm.allocate();
                    }
                    for (i = 0; i < 10000; i++)
                    {
                        pages[i]->Release();
                    }
                }
            }
        private:
            CDataBufferManager& dm;
        } afor(dm);
        afor.For(5,5);
        DBGLOG("ok");
    }

    class HeapPreserver
    {
    public:
        HeapPreserver()
        {
            _heapBase = heapBase;
            _heapEnd = heapEnd;
            _heapBitmap = heapBitmap;
            _heapBitmapSize = heapBitmapSize;
            _heapTotalPages = heapTotalPages;
            _heapLWM = heapLWM;
            _heapAllocated = heapAllocated;
            _heapUseHugePages = heapUseHugePages;
        }
        ~HeapPreserver()
        {
            heapBase = _heapBase;
            heapEnd = _heapEnd;
            heapBitmap = _heapBitmap;
            heapBitmapSize = _heapBitmapSize;
            heapTotalPages = _heapTotalPages;
            heapLWM = _heapLWM;
            heapAllocated = _heapAllocated;
            heapUseHugePages = _heapUseHugePages;
        }
        char *_heapBase;
        char *_heapEnd;
        unsigned *_heapBitmap;
        unsigned _heapBitmapSize;
        unsigned _heapTotalPages;
        unsigned _heapLWM;
        unsigned _heapAllocated;
        bool _heapUseHugePages;
    };
    void initBitmap(unsigned size)
    {
        heapBase = (char *) (memsize_t) 0x80000000;
        heapBitmap = new unsigned[size];
        memset(heapBitmap, 0xff, size*sizeof(unsigned));
        heapBitmapSize = size;
        heapTotalPages = heapBitmapSize * UNSIGNED_BITS;
        heapLWM = 0;
        heapAllocated = 0;
    }

    void testBitmap()
    {
        HeapPreserver preserver;

        initBitmap(32);
        unsigned i;
        for (i=0; i < 100; i++)
        {
            ASSERT(suballoc_aligned(1, false)==(void *)(memsize_t)(0x80000000 + 0x100000*i));
            ASSERT(suballoc_aligned(3, false)==(void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)));
        }
        for (i=0; i < 100; i+=2)
        {
            subfree_aligned((void *)(memsize_t)(0x80000000 + 0x100000*i), 1);
            subfree_aligned((void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)), 3);
        }
        for (i=0; i < 100; i+=2)
        {
            ASSERT(suballoc_aligned(1, false)==(void *)(memsize_t)(0x80000000 + 0x100000*i));
            ASSERT(suballoc_aligned(3, false)==(void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)));
        }
        for (i=0; i < 100; i++)
        {
            subfree_aligned((void *)(memsize_t)(0x80000000 + 0x100000*i), 1);
            subfree_aligned((void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)), 3);
        }

        // Try a realloc that can expand above only.
        void *t = suballoc_aligned(1, false);
        ASSERT(t==(void *)(memsize_t)(0x80000000));
        void *r = subrealloc_aligned(t, 1, 50);
        ASSERT(r == t)
        void *t1 = suballoc_aligned(1, false);
        ASSERT(t1==(void *)(memsize_t)(0x80000000 + 0x100000*50));
        subfree_aligned(r, 50);
        subfree_aligned(t1, 1);

        // Try a realloc that can expand below only.
        t = suballoc_aligned(2, false);
        ASSERT(t==(void *)(memsize_t)(0xc0000000 - 0x200000));
        r = subrealloc_aligned(t, 2, 50);
        ASSERT(r==(void *)(memsize_t)(0xc0000000 - 0x100000*50));
        t1 = suballoc_aligned(2, false);
        ASSERT(t1==(void *)(memsize_t)(0xc0000000 - 0x100000*52));
        subfree_aligned(r, 50);
        subfree_aligned(t1, 2);

        // Try a realloc that has to do both.
        t = suballoc_aligned(20, false);
        ASSERT(t==(void *)(memsize_t)(0xc0000000 - 0x100000*20));
        t1 = suballoc_aligned(20, false);
        ASSERT(t1==(void *)(memsize_t)(0xc0000000 - 0x100000*40));
        subfree_aligned(t, 20);
        r = subrealloc_aligned(t1, 20, 80);
        ASSERT(r==(void *)(memsize_t)(0xc0000000 - 0x100000*80));
        t1 = suballoc_aligned(2, false);
        ASSERT(t1==(void *)(memsize_t)(0xc0000000 - 0x100000*82));
        subfree_aligned(r, 80);
        subfree_aligned(t1, 2);

        // Try a realloc that can't quite manage it.
        t = suballoc_aligned(20, false);
        ASSERT(t==(void *)(memsize_t)(0xc0000000 - 0x100000*20));
        t1 = suballoc_aligned(20, false);
        ASSERT(t1==(void *)(memsize_t)(0xc0000000 - 0x100000*40));
        void * t2 = suballoc_aligned(20, false);
        ASSERT(t2==(void *)(memsize_t)(0xc0000000 - 0x100000*60));
        void *t3 = suballoc_aligned(20, false);
        ASSERT(t3==(void *)(memsize_t)(0xc0000000 - 0x100000*80));
        subfree_aligned(t, 20);
        subfree_aligned(t2, 20);
        r = subrealloc_aligned(t1, 20, 61);
        ASSERT(r==NULL);
        // Then one that just can
        r = subrealloc_aligned(t1, 20, 60);
        ASSERT(r==(void *)(memsize_t)(0xc0000000 - 0x100000*60));
        subfree_aligned(r, 60);
        subfree_aligned(t3, 20);

        // Check some error cases
        try
        {
            subfree_aligned((void*)0, 1);
            ASSERT(false);
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "RoxieMemMgr: Freed area not in heap")==0);
            E->Release();
        }
        try
        {
            subfree_aligned((void*)(memsize_t)0x80010000, 1);
            ASSERT(false);
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "RoxieMemMgr: Incorrect alignment of freed area")==0);
            E->Release();
        }
        try
        {
            subfree_aligned((void*)(memsize_t)0xa0000000, 1);
            ASSERT(false);
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "RoxieMemMgr: Page freed twice")==0);
            E->Release();
        }
        try
        {
            subfree_aligned((void*)(memsize_t)0xbfe00000, 3);
            ASSERT(false);
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "RoxieMemMgr: Freed area not in heap")==0);
            E->Release();
        }
        try
        {
            subfree_aligned((void*)(memsize_t)0xbfe00000, 0);
            ASSERT(false);
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "RoxieMemMgr: Invalid parameter (num pages) to subfree_aligned")==0);
            E->Release();
        }
        delete[] heapBitmap;
    }

#ifdef __64BIT__
    enum { numBitmapThreads = 20, maxBitmapSize = (unsigned)(I64C(0xFFFFFFFFFF) / HEAP_ALIGNMENT_SIZE / UNSIGNED_BITS) };      // Test larger range - in case we ever reduce the granularity
#else
    // Restrict heap sizes on 32-bit systems
    enum { numBitmapThreads = 20, maxBitmapSize = (unsigned)(I64C(0xFFFFFFFF) / HEAP_ALIGNMENT_SIZE / UNSIGNED_BITS) };      // 4Gb
#endif
    class BitmapAllocatorThread : public Thread
    {
    public:
        BitmapAllocatorThread(Semaphore & _sem, unsigned _size) : Thread("AllocatorThread"), sem(_sem), size(_size)
        {
        }

        int run()
        {
            unsigned numBitmapIter = (maxBitmapSize * 32 / size) / numBitmapThreads;
            sem.wait();
            memsize_t total = 0;
            for (unsigned i=0; i < numBitmapIter; i++)
            {
                void * ptr1 = suballoc_aligned(size, false);
                subfree_aligned(ptr1, size);
                void * ptr2 = suballoc_aligned(size, false);
                subfree_aligned(ptr2, size);
                void * ptr3 = suballoc_aligned(size, false);
                subfree_aligned(ptr3, size);
                total ^= (memsize_t)suballoc_aligned(size, false);
            }
            final = total;
            return 0;
        }
    protected:
        Semaphore & sem;
        const unsigned size;
        volatile memsize_t final;
    };
    void testBitmapThreading(unsigned size)
    {
        HeapPreserver preserver;

        initBitmap(maxBitmapSize);

        Semaphore sem;
        BitmapAllocatorThread * threads[numBitmapThreads];
        for (unsigned i1 = 0; i1 < numBitmapThreads; i1++)
            threads[i1] = new BitmapAllocatorThread(sem, size);
        for (unsigned i2 = 0; i2 < numBitmapThreads; i2++)
            threads[i2]->start();

        unsigned startTime = msTick();
        sem.signal(numBitmapThreads);
        for (unsigned i3 = 0; i3 < numBitmapThreads; i3++)
            threads[i3]->join();
        unsigned endTime = msTick();

        for (unsigned i4 = 0; i4 < numBitmapThreads; i4++)
            threads[i4]->Release();
        DBGLOG("Time taken for bitmap threading(%d) = %d", size, endTime-startTime);

        unsigned totalPages;
        unsigned freePages;
        unsigned maxBlock;
        memstats(totalPages, freePages, maxBlock);
        ASSERT(totalPages == maxBitmapSize * 32);
        unsigned numAllocated = ((maxBitmapSize * 32 / size) / numBitmapThreads) * numBitmapThreads * size;
        ASSERT(freePages == maxBitmapSize * 32 - numAllocated);

        delete[] heapBitmap;
    }
    void testBitmapThreading()
    {
#ifndef NOTIFY_UNUSED_PAGES_ON_FREE
        //Don't run this with NOTIFY_UNUSED_PAGES_ON_FREE enabled - I'm not sure what the calls to map out random memory are likely to do!
        testBitmapThreading(1);
        testBitmapThreading(3);
        testBitmapThreading(11);
#endif
    }

    void testHuge()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        ReleaseRoxieRow(rm1->allocate(1800000, 0));
        ASSERT(rm1->numPagesAfterCleanup(false)==0); // page should be freed even if force not specified
        ASSERT(rm1->getMemoryUsage()==2);
    }

    void testSizes()
    {
        ASSERT(ChunkedHeaplet::dataOffset() == CACHE_LINE_SIZE);
        ASSERT(HugeHeaplet::dataOffset() == CACHE_LINE_SIZE);
        ASSERT(FixedSizeHeaplet::chunkHeaderSize == 8);
        ASSERT(PackedFixedSizeHeaplet::chunkHeaderSize == 4);  // NOTE - this is NOT 8 byte aligned, so can't safely be used to allocate ptr arrays

        // Check some alignments
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        OwnedRoxieRow rs = rm1->allocate(18, 0);
        OwnedRoxieRow rh = rm1->allocate(1800000, 0);
        ASSERT((((memsize_t) rs.get()) & 0x7) == 0);
        ASSERT((((memsize_t) rh.get()) & 0x7) == 0);
    }

    void testAll()
    {
        testSizes();
        testRelease();
        testCycling();
        testFixed();
        StringBuffer s; DBGLOG("%s", memstats(s).str());
        testFixed();
        testFixed();
        testFixed();
        testFixed();
        testFixed();
        testFixed();
        testFixed();
        testMixed();
        testExhaust();
    }

    void testRelease()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        ReleaseRoxieRow(rm1->allocate(1000, 0));
        ASSERT(rm1->numPagesAfterCleanup(true)==0);
        ASSERT(rm1->getMemoryUsage()==1);

        void *r1 = rm1->allocate(1000, 0);
        void *r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm1->numPagesAfterCleanup(true)==0);
        ASSERT(rm1->getMemoryUsage()==1);


        Owned<IRowManager> rm2 = createRowManager(0, NULL, logctx, NULL);
        ReleaseRoxieRow(rm2->allocate(4000000, 0));
        ASSERT(rm2->numPagesAfterCleanup(true)==0);
        ASSERT(rm2->getMemoryUsage()==4);

        r1 = rm2->allocate(4000000, 0);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm2->numPagesAfterCleanup(true)==0);
        ASSERT(rm2->getMemoryUsage()==8);

        for (unsigned d = 0; d < 50; d++)
        {
            Owned<IRowManager> rm3 = createRowManager(0, NULL, logctx, NULL);
            ReleaseRoxieRow(rm3->allocate(HEAP_ALIGNMENT_SIZE - d + 10, 0));
            ASSERT(rm3->numPagesAfterCleanup(true)==0);
        }

        // test leak reporting does not crash....
        Owned<IRowManager> rm4 = createRowManager(0, NULL, logctx, NULL);
        rm4->allocate(4000000, 0);
        rm4.clear();

        Owned<IRowManager> rm5 = createRowManager(0, NULL, logctx, NULL);
        rm5->allocate(4000, 0);
        rm5.clear();
    }

    void testFixed()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        Owned<IRowManager> rm2 = createRowManager(0, NULL, logctx, NULL);
        void *ptrs[20000];
        unsigned i;
        for (i = 0; i < 10000; i++)
        {
            ptrs[i*2] = rm1->allocate(100, 0);
            ASSERT(ptrs[i*2] != NULL);
            ptrs[i*2 + 1] = rm2->allocate(1000, 0);
            ASSERT(ptrs[i*2+1] != NULL);
        }
        qsort(ptrs, 20000, sizeof(void *), mc4);
        for (i = 1; i < 20000; i++)
            ASSERT(ptrs[i] != ptrs[i-1]);
        ASSERT(rm1->allocated()==10000);
        ASSERT(rm2->allocated()==10000);
        for (i = 1; i < 20000; i++)
        {
            ReleaseRoxieRow(ptrs[i]);
        }
        ASSERT(rm1->allocated()+rm2->allocated()==1);

        // test some likely boundary error cases....
        for (i = 1; i < 10000; i++)
            ReleaseRoxieRow(rm1->allocate(i, 0));
        for (i = HEAP_ALIGNMENT_SIZE-100; i <= HEAP_ALIGNMENT_SIZE+100; i++)
            ReleaseRoxieRow(rm1->allocate(i, 0));
        for (i = HEAP_ALIGNMENT_SIZE*2-100; i <= HEAP_ALIGNMENT_SIZE*2+100; i++)
            ReleaseRoxieRow(rm1->allocate(i, 0));
        ptrs[0] = rm1->allocate(10000000, 0);
        ptrs[1] = rm1->allocate(1000, 0);
        ptrs[2] = rm1->allocate(10000000, 0);
        ptrs[3] = rm2->allocate(10000000, 0);
        ptrs[4] = rm2->allocate(1000, 0);
        ptrs[5] = rm2->allocate(10000000, 0);
        ptrs[6] = rm2->allocate(10000000, 0);
        ptrs[7] = rm2->allocate(10000000, 0);
        ptrs[8] = rm2->allocate(10000000, 0);
        ASSERT(rm1->allocated()+rm2->allocated()==10);
        qsort(ptrs, 6, sizeof(void *), mc4);
        for (i = 1; i < 6; i++)
        {
            ASSERT(ptrs[i] != ptrs[i-1]);
            ReleaseRoxieRow(ptrs[i]);
        }
        ASSERT(rm1->allocated()+rm2->allocated()==5);
    }

    void testMixed()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        void *ptrs[20000];
        unsigned i;
        for (i = 0; i < 10000; i++)
        {
            ptrs[i*2] = rm1->allocate(100, 0);
            ASSERT(ptrs[i*2] != NULL);
            ptrs[i*2 + 1] = rm1->allocate(1000, 0);
            ASSERT(ptrs[i*2+1] != NULL);
        }
        qsort(ptrs, 20000, sizeof(void *), mc4);
        for (i = 1; i < 20000; i++)
            ASSERT(ptrs[i] != ptrs[i-1]);
        ASSERT(rm1->allocated()==20000);
        for (i = 1; i < 20000; i++)
            ReleaseRoxieRow(ptrs[i]);
        ASSERT(rm1->allocated()==1);
        ReleaseRoxieRow(ptrs[0]);
        ASSERT(rm1->allocated()==0);
    }
    void testExhaust()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        rm1->setMemoryLimit(20*1024*1024);
        try
        {
            unsigned i = 0;
            loop
            {
                ASSERT(rm1->allocate(i++, 0) != NULL);
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            ASSERT(strcmp(E->errorMessage(s).str(), "memory limit exceeded")==0);
            E->Release();
        }
        ASSERT(rm1->numPagesAfterCleanup(true)==20);
    }
    void testCycling()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        rm1->setMemoryLimit(2*1024*1024);
        unsigned i;
        for (i = 0; i < 1000; i++)
        {
            void *v1 = rm1->allocate(1000, 0);
            ReleaseRoxieRow(v1);
        }
        for (i = 0; i < 1000; i++)
        {
            void *v1 = rm1->allocate(1000, 0);
            void *v2 = rm1->allocate(1000, 0);
            void *v3 = rm1->allocate(1000, 0);
            void *v4 = rm1->allocate(1000, 0);
            void *v5 = rm1->allocate(1000, 0);
            void *v6 = rm1->allocate(1000, 0);
            void *v7 = rm1->allocate(1000, 0);
            void *v8 = rm1->allocate(1000, 0);
            void *v9 = rm1->allocate(1000, 0);
            void *v10 = rm1->allocate(1000, 0);
            ReleaseRoxieRow(v10);
            ReleaseRoxieRow(v9);
            ReleaseRoxieRow(v8);
            ReleaseRoxieRow(v7);
            ReleaseRoxieRow(v6);
            ReleaseRoxieRow(v5);
            ReleaseRoxieRow(v4);
            ReleaseRoxieRow(v3);
            ReleaseRoxieRow(v2);
            ReleaseRoxieRow(v1);
        }
        for (i = 0; i < 1000; i++)
        {
            void *v1 = rm1->allocate(1000, 0);
            void *v2 = rm1->allocate(1000, 0);
            void *v3 = rm1->allocate(1000, 0);
            void *v4 = rm1->allocate(1000, 0);
            void *v5 = rm1->allocate(1000, 0);
            void *v6 = rm1->allocate(1000, 0);
            void *v7 = rm1->allocate(1000, 0);
            void *v8 = rm1->allocate(1000, 0);
            void *v9 = rm1->allocate(1000, 0);
            void *v10 = rm1->allocate(1000, 0);
            ReleaseRoxieRow(v1);
            ReleaseRoxieRow(v2);
            ReleaseRoxieRow(v3);
            ReleaseRoxieRow(v4);
            ReleaseRoxieRow(v5);
            ReleaseRoxieRow(v6);
            ReleaseRoxieRow(v7);
            ReleaseRoxieRow(v8);
            ReleaseRoxieRow(v9);
            ReleaseRoxieRow(v10);
        }
        for (i = 0; i < 1000; i++)
        {
            void *v1 = rm1->allocate(1000, 0);
            void *v2 = rm1->allocate(1000, 0);
            void *v3 = rm1->allocate(1000, 0);
            void *v4 = rm1->allocate(1000, 0);
            void *v5 = rm1->allocate(1000, 0);
            void *v6 = rm1->allocate(1000, 0);
            void *v7 = rm1->allocate(1000, 0);
            void *v8 = rm1->allocate(1000, 0);
            void *v9 = rm1->allocate(1000, 0);
            void *v10 = rm1->allocate(1000, 0);
            ReleaseRoxieRow(v1);
            ReleaseRoxieRow(v3);
            ReleaseRoxieRow(v5);
            ReleaseRoxieRow(v7);
            ReleaseRoxieRow(v9);
            ReleaseRoxieRow(v10);
            ReleaseRoxieRow(v8);
            ReleaseRoxieRow(v6);
            ReleaseRoxieRow(v4);
            ReleaseRoxieRow(v2);
        }
    }
    void testCapacity(IRowManager * rm, unsigned size, unsigned expectedPages=1)
    {
        void * alloc1 = rm->allocate(size, 0);
        unsigned capacity = RoxieRowCapacity(alloc1);
        memset(alloc1, 99, capacity);
        void * alloc2 = rm->allocate(capacity, 0);
        ASSERT(RoxieRowCapacity(alloc2)==capacity);
        ASSERT(rm->numPagesAfterCleanup(true)==expectedPages);
        memset(alloc2, 99, capacity);
        ReleaseRoxieRow(alloc1);
        ReleaseRoxieRow(alloc2);
    }

    void testAllocSize()
    {
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, NULL);
        testCapacity(rm, 1);
        testCapacity(rm, 32);
        testCapacity(rm, 32768);
        testCapacity(rm, HEAP_ALIGNMENT_SIZE,4);

        void * alloc1 = rm->allocate(1, 0);
        unsigned capacity = RoxieRowCapacity(alloc1);
        ReleaseRoxieRow(alloc1);

        Owned<IFixedRowHeap> rowHeap = rm->createFixedRowHeap(capacity, 0, RHFoldfixed);
        void * alloc2 = rowHeap->allocate();
        ASSERT(RoxieRowCapacity(alloc2) == capacity);
        ReleaseRoxieRow(alloc2);

        Owned<IFixedRowHeap> rowHeap2 = rm->createFixedRowHeap(capacity, 0, 0);
        void * alloc3 = rowHeap2->allocate();
        ASSERT(RoxieRowCapacity(alloc3) == capacity);
        ReleaseRoxieRow(alloc3);
    }
    class CountingRowAllocatorCache : public CSimpleInterface, public IRowAllocatorCache
    {
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CountingRowAllocatorCache() { atomic_set(&counter, 0); }
        virtual unsigned getActivityId(unsigned cacheId) const { return 0; }
        virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const { return out.append(cacheId); }
        virtual void onDestroy(unsigned cacheId, void *row) const { atomic_inc(&counter); }
        virtual void onClone(unsigned cacheId, void *row) const { atomic_dec(&counter); }
        virtual void checkValid(unsigned cacheId, const void *row) const { }

        void clear() { atomic_set(&counter, 0); }

        mutable atomic_t counter;
    };
    enum { numCasThreads = 20, numCasIter = 50, numCasAlloc = 1000 };
    class CasAllocatorThread : public Thread
    {
    public:
        CasAllocatorThread(Semaphore & _sem, IRowManager * _rm) : Thread("AllocatorThread"), sem(_sem), rm(_rm), cost(0)
        {
        }

        virtual void * allocate() = 0;
        virtual void * finalize(void * ptr) = 0;

        void setCost(unsigned _cost) { cost = _cost; }

        int run()
        {
            SimpleRowBuffer saved(rm, cost);
            if (rm)
                rm->addRowBuffer(&saved);
            sem.wait();
            try
            {
                //Allocate two rows and then release 1 trying to trigger potential ABA problems in the cas code.
                for (unsigned i=0; i < numCasIter; i++)
                {
                    for (unsigned j=0; j < numCasAlloc; j++)
                    {
                        //Allocate 2 rows, and add first back on the list again
                        void * alloc1 = allocate();
                        void * alloc2 = allocate();
                        *(unsigned*)alloc1 = 0xdddddddd;
                        *(unsigned*)alloc2 = 0xdddddddd;
                        alloc1 = finalize(alloc1);
                        alloc2 = finalize(alloc2);
                        ReleaseRoxieRow(alloc1);
                        saved.addRow(alloc2);
                    }
                    saved.kill();
                }
                if (rm)
                    rm->removeRowBuffer(&saved);
            }
            catch (...)
            {
                if (rm)
                    rm->removeRowBuffer(&saved);
                throw;
            }
            return 0;
        }
    protected:
        Semaphore & sem;
        IRowManager * rm;
        unsigned cost;
    };
    void runCasTest(const char * title, Semaphore & sem, CasAllocatorThread * threads[])
    {
        for (unsigned i2 = 0; i2 < numCasThreads; i2++)
        {
            threads[i2]->start();
        }

        unsigned startTime = msTick();
        sem.signal(numCasThreads);
        for (unsigned i3 = 0; i3 < numCasThreads; i3++)
            threads[i3]->join();
        unsigned endTime = msTick();

        for (unsigned i4 = 0; i4 < numCasThreads; i4++)
            threads[i4]->Release();
        DBGLOG("Time taken for %s cas = %d", title, endTime-startTime);
    }
    class HeapletCasAllocatorThread : public CasAllocatorThread
    {
    public:
        HeapletCasAllocatorThread(FixedSizeHeaplet * _heaplet, Semaphore & _sem, IRowManager * _rm) : CasAllocatorThread(_sem, _rm), heaplet(_heaplet)
        {
        }

        virtual void * allocate() { return heaplet->allocate(ACTIVITY_FLAG_ISREGISTERED|0); }
        virtual void * finalize(void * ptr) { heaplet->_setDestructorFlag(ptr); return ptr; }

    protected:
        FixedSizeHeaplet * heaplet;
    };
    void testHeapletCas()
    {
        memsize_t maxMemory = numCasThreads * numCasIter * numCasAlloc * 32;
        //Because this is allocating from a single heaplet check if it can overflow the memory
        if (maxMemory > FixedSizeHeaplet::dataAreaSize())
            return;

        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        CountingRowAllocatorCache rowCache;
        void * memory = suballoc_aligned(1, true);
        CFixedChunkedHeap dummyHeap((CChunkingRowManager*)rowManager.get(), logctx, &rowCache, 32, 0, SpillAllCost);
        FixedSizeHeaplet * heaplet = new (memory) FixedSizeHeaplet(&dummyHeap, &rowCache, 32);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new HeapletCasAllocatorThread(heaplet, sem, rowManager);

        runCasTest("heaplet", sem, threads);

        delete heaplet;
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    class FixedCasAllocatorThread : public CasAllocatorThread
    {
    public:
        FixedCasAllocatorThread(IFixedRowHeap * _rowHeap, Semaphore & _sem, IRowManager * _rm) : CasAllocatorThread(_sem, _rm), rowHeap(_rowHeap)
        {
        }

        virtual void * allocate() { return rowHeap->allocate(); }
        virtual void * finalize(void * ptr) { return rowHeap->finalizeRow(ptr); }

    protected:
        Linked<IFixedRowHeap> rowHeap;
    };
    void testOldFixedCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|RHFoldfixed);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager);

        runCasTest("old fixed allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    void testSharedFixedCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IFixedRowHeap> rowHeap;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        //For this test the row heap is assign to a variable that will be destroyed after the manager, to ensure that works.
        rowHeap.setown(rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor));
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager);

        runCasTest("shared fixed allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    void testFixedCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFunique|RHFhasdestructor);
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager);
        }

        runCasTest("separate fixed allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    void testPackedCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFunique|RHFpacked|RHFhasdestructor);
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager);
        }

        runCasTest("separate packed allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    class GeneralCasAllocatorThread : public CasAllocatorThread
    {
    public:
        GeneralCasAllocatorThread(IRowManager * _rowManager, Semaphore & _sem) : CasAllocatorThread(_sem, _rowManager), rowManager(_rowManager)
        {
        }

        virtual void * allocate() { return rowManager->allocate(8, ACTIVITY_FLAG_ISREGISTERED|0); }
        virtual void * finalize(void * ptr) { return rowManager->finalizeRow(ptr, 8, 8, ACTIVITY_FLAG_ISREGISTERED|ACTIVITY_FLAG_NEEDSDESTRUCTOR|0); }

    protected:
        IRowManager * rowManager;
    };
    void testGeneralCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new GeneralCasAllocatorThread(rowManager, sem);

        runCasTest("general allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    class VariableCasAllocatorThread : public CasAllocatorThread
    {
    public:
        VariableCasAllocatorThread(IRowManager * _rowManager, Semaphore & _sem, unsigned _seed) : CasAllocatorThread(_sem, _rowManager), rowManager(_rowManager), seed(_seed)
        {
        }

        virtual void * allocate()
        {
            //use fnv hash to generate the next "random" number
            seed = (seed * 0x01000193) ^ 0xef;
            //Try and generate a vaguely logarithmic distribution across the different bucket sizes
            unsigned base = seed & 32767;
            unsigned shift = (seed / 32768) % 12;
            unsigned size = (base >> shift) + 4;
            return rowManager->allocate(size, ACTIVITY_FLAG_ISREGISTERED|0);
        }
        virtual void * finalize(void * ptr) {
            unsigned capacity = RoxieRowCapacity(ptr);
            return rowManager->finalizeRow(ptr, capacity, capacity, ACTIVITY_FLAG_ISREGISTERED|ACTIVITY_FLAG_NEEDSDESTRUCTOR|0);
        }

    protected:
        IRowManager * rowManager;
        unsigned seed;
    };
    void testVariableCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new VariableCasAllocatorThread(rowManager, sem, i1);

        runCasTest("variable allocator", sem, threads);
        ASSERT(atomic_read(&rowCache.counter) == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    void timeRoundup()
    {
        unsigned startTime = msTick();
        unsigned seed = 1;
        unsigned total = 0;
        for (unsigned i=0; i < 100000000; i++)
        {
            seed = (seed * 0x01000193) ^ 0xef;
            //Try and generate a vaguely logarithmic distribution across the different bucket sizes
            unsigned base = seed & 32767;
            unsigned shift = (seed / 32768) % 12;
            unsigned size = (base >> shift) + 4;
            size32_t thisSize = CChunkingRowManager::roundup(size+FixedSizeHeaplet::chunkHeaderSize);
            ASSERT((thisSize & (ALLOC_ALIGNMENT-1)) == 0);
            total += thisSize;
        }
        unsigned endTime = msTick();

        DBGLOG("Time to calculate %u = %d", total, endTime-startTime);
    }
    void testRoundup()
    {
        Owned<IRowManager> rowManager = createRowManager(1, NULL, logctx, NULL);
        const unsigned maxFrac = firstFractionalHeap;

        const void * tempRow[MAX_FRAC_ALLOCATOR];
        for (unsigned frac=1; frac < maxFrac; frac++)
        {
            size32_t fracSize = FixedSizeHeaplet::dataAreaSize() / frac;
            for (unsigned j = 0; j < ALLOC_ALIGNMENT; j++)
            {
                size32_t allocSize = fracSize - j;
                size32_t thisSize = CChunkingRowManager::roundup(allocSize);
                ASSERT((thisSize & (ALLOC_ALIGNMENT-1)) == 0);
            }

            //Ensure you can allocate frac allocations in a single page (The row manager only has 1 page.)
            size32_t testSize = (fracSize - FixedSizeHeaplet::chunkHeaderSize) & ~(ALLOC_ALIGNMENT-1);
            for (unsigned i1=0; i1 < frac; i1++)
            {
                tempRow[i1] = rowManager->allocate(testSize, 0);
                ASSERT(((memsize_t)tempRow[i1] & (ALLOC_ALIGNMENT-1)) == 0);
            }
            for (unsigned i2=0; i2 < frac; i2++)
                ReleaseRoxieRow(tempRow[i2]);
        }

        //Check small allocations are also aligned, and that size estimates are correct
        size32_t limitSize = firstFractionalHeap;
        for (size_t nextSize = 1; nextSize < limitSize; nextSize++)
        {
            {
                OwnedRoxieRow row = rowManager->allocate(nextSize, 0);
                ASSERT(((memsize_t)row.get() & (ALLOC_ALIGNMENT-1)) == 0);
                ASSERT(RoxieRowCapacity(row) == rowManager->getExpectedCapacity(nextSize, 0));
                ASSERT(RoxieRowCapacity(row) < rowManager->getExpectedFootprint(nextSize, 0));
            }
            {
                Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(nextSize, ACTIVITY_FLAG_ISREGISTERED|0, RHFpacked|RHFhasdestructor);
                OwnedRoxieRow row = rowHeap->allocate();
                ASSERT(RoxieRowCapacity(row) == rowManager->getExpectedCapacity(nextSize, RHFpacked));
                ASSERT(RoxieRowCapacity(row) < rowManager->getExpectedFootprint(nextSize, RHFpacked));
            }
        }


    }
    void testFixedRelease()
    {
        //Ensure that row heaps (and therefore row allocators) can be destroyed before and after the rowManager
        //and that row heaps are cleaned up correctly when the row manager is destroyed.
        CountingRowAllocatorCache rowCache;
        Owned<IFixedRowHeap> savedRowHeap;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|RHFunique);
            void * row = rowHeap->allocate();
            ReleaseRoxieRow(row);
        }
        void * otherrow = rowManager->allocate(100, 0);
        savedRowHeap.setown(rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|RHFunique));
        void * leakedRow = savedRowHeap->allocate();
        ReleaseRoxieRow(otherrow);
        rowManager.clear();
    }

    void testCas()
    {
        testFixedRelease();
        timeRoundup();
        testVariableCas();
        testHeapletCas();
        testOldFixedCas();
        testSharedFixedCas();
        testFixedCas();
        testPackedCas();
        testGeneralCas();
    }

    void testCallback(unsigned numPerPage, unsigned pages, unsigned spillPages, double scale, unsigned flags)
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache);
        rowManager->setMemoryLimit(pages * numCasThreads * HEAP_ALIGNMENT_SIZE, spillPages * numCasThreads * HEAP_ALIGNMENT_SIZE);
        rowManager->setMemoryCallbackThreshold((unsigned)-1);
        rowManager->setCallbackOnThread(true);
        rowManager->setMinimizeFootprint(true, true);
        rowManager->setReleaseWhenModifyCallback(true, true);

        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        size32_t allocSize = (0x100000 - 0x200) / numPerPage;
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(allocSize, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|flags);
            FixedCasAllocatorThread * cur = new FixedCasAllocatorThread(rowHeap, sem, rowManager);
            cur->setCost((unsigned)(i1*scale)+1);
            threads[i1] = cur;
        }
        VStringBuffer title("callback(%u,%u,%u,%f,%x)", numPerPage,pages, spillPages, scale, flags);
        runCasTest(title.str(), sem, threads);
        //This test can very occasionally fail if each thread has 1 single row from a different page buffered, and a buffer allocated from a different page
        CPPUNIT_ASSERT_EQUAL(2 * numCasThreads * numCasIter * numCasAlloc, (int)atomic_read(&rowCache.counter));
    }
    void testCallbacks()
    {
        testCallback(16, 2, 0, 0, 0);
        testCallback(16, 2, 1, 1, 0);
        testCallback(16, 10, 5, 1, 0); // 1 at each cost level - can cause exhaustion since rows tend to get left in highest cost.
        testCallback(16, 10, 5, 0, 0); // all at the same cost level
        testCallback(16, 10, 5, 0.25, 0);  // 4 at each cost level
        testCallback(16, 10, 5, 0.25, RHFunique);  // 4 at each cost level
        testCallback(128, 10, 5, 0.25, RHFunique);  // 4 at each cost level
        testCallback(1024, 10, 5, 0.25, RHFunique);  // 4 at each cost level
    }
    const static size32_t compactingAllocSize = 32;
    void testCompacting(IRowManager * rowManager, IFixedRowHeap * rowHeap, unsigned numRows, unsigned milliFraction)
    {
        const void * * rows = new const void * [numRows];
        for (unsigned i1 = 0; i1 < numRows; i1++)
            rows[i1] = rowHeap->allocate();

        unsigned numPagesFull = rowManager->numPagesAfterCleanup(true);
        unsigned numRowsLeft = 0;
        for (unsigned i2 = 0; i2 < numRows; i2++)
        {
            if ((i2 * 7) % 1000 >= milliFraction)
                ReleaseClearRoxieRow(rows[i2]);
            else
                numRowsLeft++;
        }

        //NOTE: The efficiency of the packing does depend on the row order, so ideally this would test multiple orderings
        //of the array
        unsigned rowsPerPage = (HEAP_ALIGNMENT_SIZE - sizeof(FixedSizeHeaplet)) / compactingAllocSize;
        unsigned numPagesBefore = rowManager->numPagesAfterCleanup(false);
        unsigned expectedPages = (numRowsLeft + rowsPerPage-1)/rowsPerPage;
        ASSERT(numPagesFull == numPagesBefore);
        unsigned startTime = msTick();
        bool compacted = rowManager->compactRows(numRows, rows);
        unsigned endTime = msTick();
        unsigned numPagesAfter = rowManager->numPagesAfterCleanup(false);
        if (compacted != (numPagesBefore != numPagesAfter))
            DBGLOG("Compacted not returned correctly");
        ASSERT(compacted == (numPagesBefore != numPagesAfter));
        DBGLOG("Compacting %d[%d] (%d->%d cf %d) Before: Time taken %d", numRows, milliFraction, numPagesBefore, numPagesAfter, expectedPages, endTime-startTime);
        ASSERT(numPagesAfter == expectedPages);

        for (unsigned i3 = 0; i3 < numRows; i3++)
        {
            ReleaseClearRoxieRow(rows[i3]);
        }

        delete [] rows;
    }

    void testCompacting()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);

        Owned<IFixedRowHeap> rowHeap1 = rowManager->createFixedRowHeap(compactingAllocSize-FixedSizeHeaplet::chunkHeaderSize, 0, 0);
        Owned<IFixedRowHeap> rowHeap2 = rowManager->createFixedRowHeap(compactingAllocSize-PackedFixedSizeHeaplet::chunkHeaderSize, 0, RHFpacked);

        unsigned rowsPerPage = (HEAP_ALIGNMENT_SIZE - sizeof(FixedSizeHeaplet)) / compactingAllocSize;
        memsize_t maxRows = (useLargeMemory ? largeMemory : smallMemory) * rowsPerPage;
        testCompacting(rowManager, rowHeap1, maxRows, 50);
        testCompacting(rowManager, rowHeap1, maxRows, 800);
        testCompacting(rowManager, rowHeap1, maxRows, 960);
        testCompacting(rowManager, rowHeap1, maxRows, 999);
        unsigned rowCount = maxRows/10;
        testCompacting(rowManager, rowHeap1, rowCount, 5);
        testCompacting(rowManager, rowHeap2, rowCount, 5);
        for (unsigned percent = 10; percent <= 90; percent += 20)
        {
            testCompacting(rowManager, rowHeap1, rowCount, percent*10);
            testCompacting(rowManager, rowHeap2, rowCount, percent*10);
        }

        //Where the rows occupy a small fraction of the memory the time is approximately O(n)
        for (unsigned rowCount1 = 10000; rowCount1 <= maxRows; rowCount1 *= 2)
            testCompacting(rowManager, rowHeap1, rowCount1, 50);

        //Where the rows occupy the main fraction of the memory the time is approximately O(n^2)
        //This needs more investigation - it could become a problem once > 10Gb of memory in use.
        //I suspect it the traversal of the heaplet list - save pointer to filled blocks somehow?
        for (unsigned rowCount2 = 10000; rowCount2 <= maxRows; rowCount2 *= 2)
            testCompacting(rowManager, rowHeap1, rowCount2, 800);
    }
    void testRecursiveCallbacks1()
    {
        const size32_t bigRowSize = HEAP_ALIGNMENT_SIZE * 2 / 3;
        Owned<IRowManager> rowManager = createRowManager(2 * HEAP_ALIGNMENT_SIZE, NULL, logctx, NULL);

        //The lower cost allocator allocates an extra row when it is called to free all its rows.
        //this will only succeed if the higher cost allocator is then called to free its data.
        NastyCallbackBlockAllocator alloc1(rowManager, bigRowSize, 10);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 20);

        alloc1.allocate();
        alloc2.allocate();
        OwnedRoxieRow tempRow = rowManager->allocate(bigRowSize, 0);
    }
    void testRecursiveCallbacks2()
    {
        const size32_t bigRowSize = HEAP_ALIGNMENT_SIZE * 2 / 3;
        Owned<IRowManager> rowManager = createRowManager(2 * HEAP_ALIGNMENT_SIZE, NULL, logctx, NULL);

        //Both allocators allocate extra memory when they are requested to free.  Ensure that an exception
        //is thrown instead of the previous stack fault.
        NastyCallbackBlockAllocator alloc1(rowManager, bigRowSize, 10);
        NastyCallbackBlockAllocator alloc2(rowManager, bigRowSize, 20);

        alloc1.allocate();
        alloc2.allocate();
        bool ok = false;
        try
        {
            OwnedRoxieRow tempRow = rowManager->allocate(bigRowSize, 0);
        }
        catch (IException * e)
        {
            e->Release();
            ok = true;
        }
        ASSERT(ok);
    }
    void testRecursiveCallbacks()
    {
        testRecursiveCallbacks1();
        testRecursiveCallbacks2();
    }
    void testCostCallbacks1()
    {
        //Test with a limit set on the memory manager
        const size32_t bigRowSize = HEAP_ALIGNMENT_SIZE * 2 / 3;
        Owned<IRowManager> rowManager = createRowManager(1, NULL, logctx, NULL);

        SimpleCallbackBlockAllocator alloc1(rowManager, bigRowSize, 20);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 10);

        alloc1.allocate();
        ASSERT(alloc1.hasRow());
        alloc2.costAllocate(10);
        ASSERT(alloc1.hasRow());
        ASSERT(!alloc2.hasRow());
        alloc2.costAllocate(20);
        ASSERT(!alloc1.hasRow());
        ASSERT(alloc2.hasRow());
    }
    void testCostCallbacks2()
    {
        //Test with no limit set on the memory manager
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);

        const memsize_t bigRowSize = HEAP_ALIGNMENT_SIZE * (heapTotalPages * 2 / 3);
        SimpleCallbackBlockAllocator alloc1(rowManager, bigRowSize, 20);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 10);

        alloc1.allocate();
        ASSERT(alloc1.hasRow());
        alloc2.costAllocate(10);
        ASSERT(alloc1.hasRow());
        ASSERT(!alloc2.hasRow());
        alloc2.costAllocate(20);
        ASSERT(!alloc1.hasRow());
        ASSERT(alloc2.hasRow());
    }
    void testCostCallbacks()
    {
        testCostCallbacks1();
        testCostCallbacks2();
    }
};

class CSimpleRowResizeCallback : public CVariableRowResizeCallback
{
public:
    CSimpleRowResizeCallback(memsize_t & _capacity, void * & _row) : CVariableRowResizeCallback(_capacity, _row), locks(0) {}

    virtual void lock() { ++locks; }
    virtual void unlock() { --locks; }

public:
    unsigned locks;
};


const memsize_t memorySize = 0x60000000;
class RoxieMemStressTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieMemStressTests );
    CPPUNIT_TEST(testSetup);
    CPPUNIT_TEST(testFragmenting);
    CPPUNIT_TEST(testDoubleFragmenting);
    CPPUNIT_TEST(testResizeDoubleFragmenting);
    CPPUNIT_TEST(testResizeFragmenting);
    CPPUNIT_TEST(testSequential);
    CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieMemStressTests() : logctx(queryDummyContextLogger())
    {
    }

    ~RoxieMemStressTests()
    {
    }

protected:
    void testSetup()
    {
        setTotalMemoryLimit(false, memorySize, 0, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    void testSequential()
    {
        unsigned requestSize = 20;
        unsigned allocSize = 32 + sizeof(void*);
        memsize_t numAlloc = (memorySize / allocSize) * 3 /4;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        void * * rows = (void * *)rowManager->allocate(numAlloc * sizeof(void*), 0);
        unsigned startTime = msTick();
        for (memsize_t i=0; i < numAlloc; i++)
        {
            rows[i] = rowManager->allocate(requestSize, 1);
            if (i % 0x100000 == 0xfffff)
                DBGLOG("Time for sequential allocate %d = %d", (unsigned)(i / 0x100000), msTick()-startTime);
        }
        for (memsize_t i=0; i < numAlloc; i++)
        {
            ReleaseRoxieRow(rows[i]);
        }
        unsigned endTime = msTick();
        ReleaseRoxieRow(rows);
        DBGLOG("Time for sequential allocate = %d", endTime - startTime);
    }

    void testFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        unsigned startTime = msTick();
        void * prev = rowManager->allocate(requestSize, 1);
        try
        {
            loop
            {
                memsize_t nextSize = (memsize_t)(requestSize*1.25);
                void *next = rowManager->allocate(nextSize, 1);
                memcpy(next, prev, requestSize);
                requestSize = nextSize;
                ReleaseRoxieRow(prev);
                prev = next;
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            memmap(s);
            DBGLOG("Unable to allocate more:\n%s", s.str());
            E->Release();
        }
        ReleaseRoxieRow(prev);
        unsigned endTime = msTick();
        DBGLOG("Time for fragmenting allocate = %d, max allocation=%"I64F"u, limit = %"I64F"u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/4);
    }

    void testDoubleFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        unsigned startTime = msTick();
        void * prev1 = rowManager->allocate(requestSize, 1);
        void * prev2 = rowManager->allocate(requestSize, 1);
        try
        {
            loop
            {
                memsize_t nextSize = (memsize_t)(requestSize*1.25);
                void *next1 = rowManager->allocate(nextSize, 1);
                memcpy(next1, prev1, requestSize);
                ReleaseRoxieRow(prev1);
                prev1 = next1;
                void *next2 = rowManager->allocate(nextSize, 1);
                memcpy(next2, prev2, requestSize);
                ReleaseRoxieRow(prev2);
                prev2 = next2;
                requestSize = nextSize;
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            memmap(s);
            DBGLOG("Unable to allocate more:\n%s", s.str());
            E->Release();
        }
        ReleaseRoxieRow(prev1);
        ReleaseRoxieRow(prev2);
        unsigned endTime = msTick();
        DBGLOG("Time for fragmenting double allocate = %d, max allocation=%"I64F"u, limit = %"I64F"u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/8);
    }

    void testResizeFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        unsigned startTime = msTick();
        void * prev = rowManager->allocate(requestSize, 1);
        try
        {
            loop
            {
                memsize_t nextSize = (memsize_t)(requestSize*1.25);
                memsize_t curSize = RoxieRowCapacity(prev);
                CSimpleRowResizeCallback callback(curSize, prev);
                rowManager->resizeRow(prev, requestSize, nextSize, 1, SpillAllCost, callback);
                ASSERT(curSize >= nextSize);
                requestSize = nextSize;
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            memmap(s);
            DBGLOG("Unable to allocate more:\n%s", s.str());
            E->Release();
        }
        ReleaseRoxieRow(prev);
        unsigned endTime = msTick();
        DBGLOG("Time for fragmenting resize = %d, max allocation=%"I64F"u, limit = %"I64F"u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/1.3);
    }

    void testResizeDoubleFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        unsigned startTime = msTick();
        void * prev1 = rowManager->allocate(requestSize, 1);
        void * prev2 = rowManager->allocate(requestSize, 1);
        try
        {
            loop
            {
                memsize_t nextSize = (memsize_t)(requestSize*1.25);
                memsize_t newSize1 = RoxieRowCapacity(prev1);
                memsize_t newSize2 = RoxieRowCapacity(prev2);
                CSimpleRowResizeCallback callback1(newSize1, prev1);
                CSimpleRowResizeCallback callback2(newSize2, prev2);
                rowManager->resizeRow(prev1, requestSize, nextSize, 1, SpillAllCost, callback1);
                ASSERT(newSize1 >= nextSize);
                rowManager->resizeRow(prev2, requestSize, nextSize, 1, SpillAllCost, callback2);
                ASSERT(newSize2 >= nextSize);
                requestSize = nextSize;
            }
        }
        catch (IException *E)
        {
            StringBuffer s;
            memmap(s);
            DBGLOG("Unable to allocate more:\n%s", s.str());
            E->Release();
        }
        ReleaseRoxieRow(prev1);
        ReleaseRoxieRow(prev2);
        unsigned endTime = msTick();
        DBGLOG("Time for fragmenting double resize = %d, max allocation=%"I64F"u, limit = %"I64F"u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/4);
    }


};

const memsize_t hugeMemorySize = (memsize_t)0x110000000;
const memsize_t hugeAllocSize = (memsize_t)0x100000001;
// const memsize_t initialAllocSize = hugeAllocSize/2; // need to support expand block for that to fit
const memsize_t initialAllocSize = 0x100;

class RoxieMemHugeTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieMemHugeTests );
    CPPUNIT_TEST(testSetup);
    CPPUNIT_TEST(testHuge);
    CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieMemHugeTests() : logctx(queryDummyContextLogger())
    {
    }

    ~RoxieMemHugeTests()
    {
    }

protected:
    void testSetup()
    {
        setTotalMemoryLimit(false, hugeMemorySize, 0, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    void testHuge()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        void * huge = rowManager->allocate(hugeAllocSize, 1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==4097);
        ReleaseRoxieRow(huge);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);
        memsize_t capacity;
        void *huge1 = rowManager->allocate(initialAllocSize, 1);
        rowManager->resizeRow(capacity, huge1, initialAllocSize, hugeAllocSize, 1);
        ASSERT(capacity > hugeAllocSize);
        ASSERT(rowManager->numPagesAfterCleanup(true)==4097);
        ReleaseRoxieRow(huge1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);

        huge1 = rowManager->allocate(hugeAllocSize/2, 1);
        rowManager->resizeRow(capacity, huge1, hugeAllocSize/2, hugeAllocSize, 1);
        ASSERT(capacity > hugeAllocSize);
        ASSERT(rowManager->numPagesAfterCleanup(true)==4097);
        ReleaseRoxieRow(huge1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);

        ASSERT(rowManager->getExpectedCapacity(hugeAllocSize, RHFnone) > hugeAllocSize);
        ASSERT(rowManager->getExpectedFootprint(hugeAllocSize, RHFnone) > hugeAllocSize);
        ASSERT(true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemTests, "RoxieMemTests" );
CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemStressTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemStressTests, "RoxieMemStressTests" );
#ifdef __64BIT__
//CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemHugeTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemHugeTests, "RoxieMemHugeTests" );
#endif
} // namespace roxiemem
#endif
