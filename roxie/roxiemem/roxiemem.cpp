/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jset.hpp"
#include <new>
#ifdef _USE_TBB
#include "tbb/tbb_stddef.h"
#include "tbb/task.h"
#include "tbb/task_scheduler_init.h"
#endif
#include <utility>

#ifndef _WIN32
#include <sys/mman.h>
# if defined(__linux__)
   // MADV_HUGEPAGE on CentOS
#  include <linux/mman.h>
# endif
#endif

#if defined(_USE_TBB)
 //Only enable for TBB >=3 because code had problems with spawn as a non static function (see HPC-14588)
 #if defined(TBB_VERSION_MAJOR)
  #if (TBB_VERSION_MAJOR >= 3)
//Release blocks of rows in parallel - always likely to improve performance
   #define PARALLEL_SYNC_RELEASE
  #endif
 #endif
#endif

#undef roundup  // Sometimes defined as a macro in sys/param.h

//#define _CLEAR_ALLOCATED_ROW
//#define _CLEAR_FREED_ROW
//#define _CLEAR_ALLOCATED_HUGE_ROW

#ifdef _WIN32
//Visual studio complains that the constructors for heaplets could throw exceptions, so there should be a matching
//operator delete otherwise there will be leaks.  The constructors can't throw exceptions, so disable the warning.
#pragma warning(disable:4291)
#endif

namespace roxiemem {

#define NOTIFY_UNUSED_PAGES_ON_FREE     // avoid linux swapping 'freed' pages to disk
//#define ALWAYS_USE_SCAN_HEAP          // option to test out using the scanning heaplets
//#define ALWAYS_DELAY_RELEASE          // option to test out using delayed releasing with the scanning heaplets
//#define OLD_ROW_COMPACT

//The following constants should probably be tuned depending on the architecture - see Tuning Test at the end of the file.
#define DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY       2048        // default
#define RELEASE_THRESHOLD_SCALING                       64     // rather high, may reduce if memory manager restructured
#define DEFAULT_PARALLEL_SYNC_RELEASE_THRESHOLD         (DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY * RELEASE_THRESHOLD_SCALING)

static unsigned memTraceLevel = 1;
static memsize_t memTraceSizeLimit = 0;
static const unsigned ScanReportThreshold = 10; // If average more than 10 scans per allocate then notify us.  More than 10 starts slowing the query down.

void setMemTraceLevel(unsigned value)
{
    memTraceLevel = value;
}

void setMemTraceSizeLimit(memsize_t value)
{
    memTraceSizeLimit = 0;
}

unsigned DATA_ALIGNMENT_SIZE=0x400;
const static unsigned UNLIMITED_PAGES = (unsigned)-1;

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

Link counted rows and memory order:
===================================

In the general implementation of linkcounted objects, decrement/store to the link count should be an release operation - to ensure
that any writes to the contents of the object are committed before it is decremented, and there should be an acquire barrier
after the last decrement - to ensure that memory cannot appear to be modified after the object is freed.

However the roxiemem implementation has some additional considerations:

* allocations are protected by an acquire/release spinblock (or cs).  That means that one allocation cannot access an inconsistent view
  of an object from a previous allocation.

* Only the thread that allocates the memory modifies it.  As long as rows are passed to other threads with acquire/release this means
  that the row will always be up to date in all threads.

Therefore decrements do not need release (since memory is already synchronized - either by the allocation cs, or not shared with another thread),
and accesses do not need to use acquire.

If this assumption about modifying on other threads is ever broken then there needs to be an appriate barrier after the last write and
before the object is released.

*/
//================================================================================
// Allocation of aligned blocks from os

#define HEAPERROR(a) throw MakeStringException(ROXIEMM_HEAP_ERROR, a)

#ifdef _DEBUG
const unsigned maxLeakReport = 20;
#else
const unsigned maxLeakReport = 4;
#endif

#ifdef _ARCH_PPC64EL_
typedef unsigned __int64 heap_t; // huge pages are 16M on power pc - this allow them to be released back to the OS
#else
typedef unsigned heap_t; // currently more efficient on intel machines
#endif

static char *heapBase;
static char *heapEnd;   // Equal to heapBase + (heapTotalPages * page size)
static bool heapUseHugePages;
static heap_t *heapBitmap;
static unsigned heapBitmapSize;
static unsigned heapTotalPages; // derived from heapBitmapSize - here for code clarity
static unsigned heapLWM;
static unsigned heapHWM;
static unsigned heapLargeBlocks;
static unsigned heapLargeBlockGranularity;
static ILargeMemCallback * heapLargeBlockCallback;
static bool heapNotifyUnusedEachFree = true;
static bool heapNotifyUnusedEachBlock = false;
static unsigned __int64 lastStatsCycles;
static unsigned __int64 statsCyclesInterval;
static std::atomic<unsigned> activeRowManagers;

static unsigned heapAllocated;
static std::atomic_uint dataBufferPages;
static std::atomic_uint dataBuffersActive;

const unsigned HEAP_BITS = sizeof(heap_t) * 8;
const heap_t HEAP_ALLBITS = (heap_t) -1;
const heap_t TOPBITMASK = ((heap_t)1U)<<(HEAP_BITS-1);
const memsize_t heapBlockSize = HEAP_BITS*HEAP_ALIGNMENT_SIZE;

//Constants used when maintaining a list of blocks.  The blocks are stored as unsigned numbers, null has an unusual number so
//that block 0 can be the heaplet at address heapBase.  The top bits are used as a mask to prevent the ABA problem in
//a lockless list.  I suspect the mask could be increased.
const unsigned BLOCKLIST_MASK = 0xFFFFFF;
const unsigned BLOCKLIST_ABA_INC = (BLOCKLIST_MASK+1);
const unsigned BLOCKLIST_ABA_MASK = ~BLOCKLIST_MASK;
const unsigned BLOCKLIST_NULL = BLOCKLIST_MASK; // Used to represent a null entry
const unsigned BLOCKLIST_LIMIT = BLOCKLIST_MASK; // Values above this are not valid

inline bool isNullBlock(unsigned block)
{
    return (block & BLOCKLIST_MASK) == BLOCKLIST_NULL;
}

template <typename VALUE_TYPE, typename ALIGN_TYPE>
inline VALUE_TYPE align_pow2(VALUE_TYPE value, ALIGN_TYPE alignment)
{
    return (value + alignment -1) & ~((VALUE_TYPE)(alignment) -1);
}

#define PAGES(x, alignment)    (((x) + ((alignment)-1)) / (alignment))           // hope the compiler converts to a shift

inline void notifyMemoryUnused(void * address, memsize_t size)
{
#ifdef NOTIFY_UNUSED_PAGES_ON_FREE
#ifdef _WIN32
        VirtualAlloc(address, size, MEM_RESET, PAGE_READWRITE);
#else
        // for linux mark as unwanted
        madvise(address,size,MADV_DONTNEED);
#endif
#endif
}

//---------------------------------------------------------------------------------------------------------------------

typedef MapBetween<unsigned, unsigned, memsize_t, memsize_t> MapActivityToMemsize;

static CriticalSection heapBitCrit;

static void initializeHeap(bool allowHugePages, bool allowTransparentHugePages, bool retainMemory, memsize_t pages, memsize_t largeBlockGranularity, ILargeMemCallback * largeBlockCallback)
{
    if (heapBase) return;

    // CriticalBlock b(heapBitCrit); // unnecessary - must call this exactly once before any allocations anyway!
    memsize_t bitmapSize = (pages + HEAP_BITS - 1) / HEAP_BITS;
    memsize_t totalPages = bitmapSize * HEAP_BITS;
    memsize_t memsize = totalPages * HEAP_ALIGNMENT_SIZE;

    if (totalPages > (unsigned)-1)
        throw makeStringExceptionV(ROXIEMM_TOO_MUCH_MEMORY,
                    "Heap cannot support memory of size %" I64F "u - decrease memory or increase HEAP_ALIGNMENT_SIZE",
                    (__uint64)memsize);

    if (totalPages >= BLOCKLIST_LIMIT)
        throw makeStringExceptionV(ROXIEMM_TOO_MUCH_MEMORY,
                    "Heap cannot support memory of size %" I64F "u - decrease memory or increase HEAP_ALIGNMENT_SIZE or BLOCKLIST_MASK",
                    (__uint64)memsize);

    heapBitmapSize = (unsigned)bitmapSize;
    heapTotalPages = (unsigned)totalPages;
    heapLargeBlockGranularity = largeBlockGranularity;
    heapLargeBlockCallback = largeBlockCallback;

    heapNotifyUnusedEachFree = !retainMemory;
    heapNotifyUnusedEachBlock = false;

#ifdef _WIN32
    // Not the world's best code but will do 
    char *next = (char *) HEAP_ALIGNMENT_SIZE;
    for (;;)
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
            heapNotifyUnusedEachFree = false;
            heapNotifyUnusedEachBlock = !retainMemory;
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
        const memsize_t hugePageSize = getHugePageSize();
        HugePageMode mode = queryTransparentHugePagesMode();
        bool hasTransparenHugePages = areTransparentHugePagesEnabled(mode);
        bool useTransparentHugePages = allowTransparentHugePages && hasTransparenHugePages;
        memsize_t heapAlignment = useTransparentHugePages ? hugePageSize : HEAP_ALIGNMENT_SIZE;
        if (mode == HugePageMode::Always)
        {
            //Always return memory in multiples of the huge page size - even if it is not being used
            heapAlignment = hugePageSize;
            OERRLOG("WARNING: The OS is configured to always use transparent huge pages.  This may cause unexplained pauses "
                    "while transparent huge pages are coalesced. The recommended setting for "
                    "/sys/kernel/mm/transparent_hugepage/enabled is madvise");
        }

        if (heapAlignment < HEAP_ALIGNMENT_SIZE)
            heapAlignment = HEAP_ALIGNMENT_SIZE;

        int ret;
        if ((ret = posix_memalign((void **) &heapBase, heapAlignment, memsize)) != 0) {

        	switch (ret)
        	{
        	case EINVAL:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%" I64F "u, size=%" I64F "u) failed - ret=%d "
        				"(EINVAL The alignment argument was not a power of two, or was not a multiple of sizeof(void *)!)",
        		                    (unsigned __int64) heapAlignment, (unsigned __int64) memsize, ret);
        		break;

        	case ENOMEM:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%" I64F "u, size=%" I64F "u) failed - ret=%d "
        				"(ENOMEM There was insufficient memory to fulfill the allocation request.)",
        		        		                    (unsigned __int64) heapAlignment, (unsigned __int64) memsize, ret);
        		break;

        	default:
        		DBGLOG("RoxieMemMgr: posix_memalign (alignment=%" I64F "u, size=%" I64F "u) failed - ret=%d",
        		                    (unsigned __int64) heapAlignment, (unsigned __int64) memsize, ret);
        		break;

        	}
            HEAPERROR("RoxieMemMgr: Unable to create heap");
        }

        //If system supports transparent huge pages, use madvise to mark the memory as request huge pages
#ifdef MADV_HUGEPAGE
        if (useTransparentHugePages)
        {
            if (madvise(heapBase,memsize,MADV_HUGEPAGE) == 0)
            {
                //Prevent the transparent huge page code from working hard trying to defragment memory when single heaplets are released
                heapNotifyUnusedEachFree = false;
                if ((heapBlockSize % hugePageSize) == 0)
                {
                    //If we notify heapBlockSize items at a time it will always be a multiple of hugePageSize so shouldn't trigger defragmentation
                    heapNotifyUnusedEachBlock = !retainMemory;
                }
                if (memTraceLevel)
                    DBGLOG("Transparent huge pages used for roxiemem heap");
            }
        }
        else
        {
            if (!allowTransparentHugePages)
            {
                madvise(heapBase,memsize,MADV_NOHUGEPAGE);
                if (memTraceLevel)
                    DBGLOG("Transparent huge pages disabled in configuration by user.");
            }
            else if (memTraceLevel)
                DBGLOG("Transparent huge pages unsupported or disabled by system.");
        }
#else
        if (memTraceLevel)
            DBGLOG("Transparent huge pages are not supported on this kernel.  Requires kernel version > 2.6.38.");
#endif
    }
#endif

    if (heapNotifyUnusedEachFree)
    {
        if (memTraceLevel)
            DBGLOG("Memory released to OS on each %uk 'page'", (unsigned)(HEAP_ALIGNMENT_SIZE/1024));
    }
    else if (heapNotifyUnusedEachBlock)
    {
        if (memTraceLevel)
            DBGLOG("Memory released to OS in %uk blocks", (unsigned)(HEAP_ALIGNMENT_SIZE*HEAP_BITS/1024));
    }
    else
    {
        DBGLOG("MEMORY WILL NOT BE RELEASED TO OS");
        if (!retainMemory)
            DBGLOG("Increase HEAP_ALIGNMENT_SIZE so HEAP_ALIGNMENT_SIZE*%u (0x%" I64F "x) is a multiple of system huge page size (0x%" I64F "x)",
                    HEAP_BITS, (unsigned __int64)(HEAP_ALIGNMENT_SIZE * HEAP_BITS), (unsigned __int64) getHugePageSize());
    }

    assertex(((memsize_t)heapBase & (HEAP_ALIGNMENT_SIZE-1)) == 0);

    heapEnd = heapBase + memsize;
    heapBitmap = new heap_t [heapBitmapSize];
    memset(heapBitmap, 0xff, heapBitmapSize*sizeof(heap_t));
    heapLargeBlocks = 1;
    heapLWM = 0;
    heapHWM = heapBitmapSize;

    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: %u Pages successfully allocated for the pool - memsize=%" I64F "u base=%p alignment=%" I64F "u bitmapSize=%u", 
                heapTotalPages, (unsigned __int64) memsize, heapBase, (unsigned __int64) HEAP_ALIGNMENT_SIZE, heapBitmapSize);
}

#ifdef _USE_CPPUNIT
static void adjustHeapSize(unsigned numPages)
{
    memsize_t bitmapSize = (numPages + HEAP_BITS - 1) / HEAP_BITS;
    memsize_t totalPages = bitmapSize * HEAP_BITS;
    memsize_t memsize = totalPages * HEAP_ALIGNMENT_SIZE;
    heapEnd = heapBase + memsize;
    heapBitmapSize = (unsigned)bitmapSize;
    heapTotalPages = (unsigned)totalPages;
}
#endif

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
            heap_t t = heapBitmap[i];
            if (t)
            {
                if (t==HEAP_ALLBITS)
                {
                    thisBlock += HEAP_BITS;
                    freePages += HEAP_BITS;
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

#ifdef _USE_CPPUNIT
static void dumpHeapState()
{
    StringBuffer s;
    for (unsigned i = 0; i < heapBitmapSize; i++)
    {
        heap_t t = heapBitmap[i];
        s.appendf("%0*" I64F "x", (int)(sizeof(heap_t)*2), (unsigned __int64)t);
    }

    DBGLOG("Heap: %s", s.str());
}
#endif

IPerfMonHook *createRoxieMemStatsPerfMonHook(IPerfMonHook *chain)
{
    class memstatsPerfMonHook : implements IPerfMonHook, public CInterface
    {
        Linked<IPerfMonHook> chain;
    public:
        IMPLEMENT_IINTERFACE;
        memstatsPerfMonHook(IPerfMonHook *_chain)
            : chain(_chain)
        {
        }
        
        virtual void processPerfStats(unsigned processorUsage, unsigned memoryUsage, unsigned memoryTotal, unsigned __int64 firstDiskUsage, unsigned __int64 firstDiskTotal, unsigned __int64 secondDiskUsage, unsigned __int64 secondDiskTotal, unsigned threadCount)
        {
            if (chain)
                chain->processPerfStats(processorUsage, memoryUsage, memoryTotal, firstDiskUsage,firstDiskTotal, secondDiskUsage, secondDiskTotal, threadCount);
        }
        virtual StringBuffer &extraLogging(StringBuffer &extra)
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
        virtual void log(int level, const char *message)
        {
            PROGLOG("%s", message);
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
        else stats.appendf("\n%p: ", heapBase + i*HEAP_BITS*HEAP_ALIGNMENT_SIZE);

        if (heapBitmap[i] == HEAP_ALLBITS) {
            stats.appendf("%.*s", HEAP_BITS, "1111111111111111111111111111111111111111111111111111111111111111");
            freePages += HEAP_BITS;
            thisBlock += HEAP_BITS;
            if (thisBlock > maxBlock)
                maxBlock = thisBlock;
        }
        else if (heapBitmap[i] == 0) {
            stats.appendf("%.*s", HEAP_BITS, "0000000000000000000000000000000000000000000000000000000000000000");
            thisBlock = 0;
        }
        else {
            heap_t mask = 1;
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

static void throwGlobalHeapExhausted(unsigned pages)
{
    VStringBuffer msg("Global memory exhausted: (%u pages) exhausted, requested %u active(%u) heap(%u/%u)", heapTotalPages, pages, activeRowManagers.load(), heapAllocated, heapTotalPages);
    DBGLOG("%s", msg.str());
    throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
}

static void throwGlobalHeapExhausted(unsigned activityId, unsigned newPages, unsigned oldPages)
{
    VStringBuffer msg("Global memory exhausted: activity id %u (%u pages) exhausted, requested %u, had %u active(%u) heap(%u/%u)", activityId, heapTotalPages, newPages, oldPages, activeRowManagers.load(), heapAllocated, heapTotalPages);
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
            s.appendf(", heapLWM %u, heapHWM %u, dataBuffersActive=%d, dataBufferPages=%d", heapLWM, heapHWM, dataBuffersActive.load(std::memory_order_relaxed), dataBufferPages.load(std::memory_order_relaxed));
            DBGLOG("RoxieMemMgr: %s", s.str());
        }
    }
    CriticalBlock b(heapBitCrit);
    if (heapAllocated + pages > heapTotalPages) {
        if (returnNullWhenExhausted)
            return NULL;
        throwGlobalHeapExhausted(pages);
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
                    throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
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
            heap_t hbi = heapBitmap[i];
            if (hbi)
            {
                const unsigned pos = countTrailingUnsetBits(hbi);
                const heap_t mask = ((heap_t)1U) << pos;
                const unsigned match = i*HEAP_BITS + pos;
                char *ret = heapBase + match*HEAP_ALIGNMENT_SIZE;
                hbi &= ~mask;
                heapBitmap[i] = hbi;
                //If no more free pages in this mask increment the low water mark
                if (hbi == 0)
                    i++;
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
        dbgassertex(heapHWM != 0); // should have been caught much earlier

        unsigned i = heapHWM;
        //Check if the last page allocated created a new run of completely allocated memory
        if (heapBitmap[i-1] == 0)
        {
            for (;;)
            {
                i--;
                dbgassertex(i != 0); // should never occur - memory must be full, and should have been caught much earlier
                if (heapBitmap[i-1])
                    break;
            }
            heapHWM = i;
        }
        unsigned matches = 0;
        while (i)
        {
            heap_t hbi = heapBitmap[--i];
            heap_t mask = TOPBITMASK;
            if (hbi)
            {
                for (unsigned b = HEAP_BITS; b > 0; b--)
                {
                    if (hbi & mask)
                    {
                        matches++;
                        if (matches==pages)
                        {
                            unsigned start = i;
                            heap_t startHbi = hbi;
                            char *ret = heapBase + (i*HEAP_BITS+b-1)*HEAP_ALIGNMENT_SIZE;
                            for (;;)
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

                            //Check if all memory above this allocation is allocated - if so extend the HWM
                            if ((hbi == 0) && (i+1 == heapHWM))
                            {
                                //Could be some space in the first chunk - will be checked next time allocate is called.
                                heapHWM = start+1;
                            }

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
    throwGlobalHeapExhausted(pages);
    return NULL;
}

static bool subfree_aligned(void *ptr, unsigned pages = 1)
{
    unsigned _pages = pages;
    memsize_t offset = (char *)ptr - heapBase;
    memsize_t pageOffset = offset / HEAP_ALIGNMENT_SIZE;
    if (!pages)
    {
        IERRLOG("RoxieMemMgr: Invalid parameter (pages=%u) to subfree_aligned", pages);
        return false;
    }
    if (pageOffset + pages > heapTotalPages)
    {
        IERRLOG("RoxieMemMgr: Freed area not in heap (ptr=%p)", ptr);
        return false;
    }
    if (pageOffset*HEAP_ALIGNMENT_SIZE != offset)
    {
        IERRLOG("RoxieMemMgr: Incorrect alignment of freed area (ptr=%p)", ptr);
        return false;
    }
    if (heapNotifyUnusedEachFree)
        notifyMemoryUnused(ptr, pages*HEAP_ALIGNMENT_SIZE);

    unsigned wordOffset = (unsigned) (pageOffset / HEAP_BITS);
    unsigned bitOffset = (unsigned) (pageOffset % HEAP_BITS);
    heap_t mask = ((heap_t)1U)<<bitOffset;
    char * firstReleaseBlock = NULL;
    char * lastReleaseBlock = NULL;
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

        for (;;)
        {
            heap_t prev = heapBitmap[wordOffset];
            if (unlikely((prev & mask) != 0))
            {
                IERRLOG("RoxieMemMgr: Page freed twice (ptr=%p)", ptr);
                return false;
            }

            heap_t next = prev | mask;
            heapBitmap[wordOffset] = next;
            if ((next == HEAP_ALLBITS) && heapNotifyUnusedEachBlock)
            {
                char * address = heapBase + wordOffset * heapBlockSize;
                if (!firstReleaseBlock)
                    firstReleaseBlock = address;
                lastReleaseBlock = address;
            }
            if (!--pages)
                break;
            mask <<= 1;
            if (mask==0)
            {
                mask = 1;
                wordOffset++;
            }
        }

        if (wordOffset >= heapHWM)
            heapHWM = wordOffset+1;

        if (firstReleaseBlock)
            notifyMemoryUnused(firstReleaseBlock, (lastReleaseBlock - firstReleaseBlock) + heapBlockSize);
    }

    if (memTraceLevel >= 2)
        DBGLOG("RoxieMemMgr: subfree_aligned() %u pages ok - addr=%p heapLWM=%u heapHWM=%u totalPages=%u", _pages, ptr, heapLWM, heapHWM, heapTotalPages);
    return true;
}

static void clearBits(unsigned start, unsigned len)
{
    // Note - should already have locked before calling this
    // These functions should really be member functions (and the heapBitmap should be a member variable) of a
    // HeapAlignedBitmap class.
    if (len)
    {
        unsigned wordOffset = (unsigned) (start / HEAP_BITS);
        unsigned bitOffset = (unsigned) (start % HEAP_BITS);
        heap_t mask = ((heap_t)1U)<<bitOffset;
        heapAllocated += len;
        heap_t heapword = heapBitmap[wordOffset];
        while (len--)
        {
            if (unlikely((heapword & mask) == 0))
                HEAPERROR("RoxieMemMgr: Page freed twice");
            heapword &= ~mask;
            mask <<= 1;
            if (mask==0)
            {
                heapBitmap[wordOffset] = heapword;
                wordOffset++;
                if (wordOffset==heapBitmapSize)
                    return;    // Avoid read off end of array
                heapword = heapBitmap[wordOffset];
                mask = 1;
            }
        }
        heapBitmap[wordOffset] = heapword;
    }
}

static void *subrealloc_aligned(void *ptr, unsigned pages, unsigned newPages)
{
    //If this function shrinks the size of the block, then the call to subfree_aligned() may increase heapHWM.
    //If this function increases the size of the block, it is possible heapHWM could decrease - it will be
    //updated on the next multi-page allocation
    assertex(newPages > 0);
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
        subfree_aligned((char *) ptr + newPages*HEAP_ALIGNMENT_SIZE, pages - newPages);
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
        unsigned wordOffset = (unsigned) (topOffset / HEAP_BITS);
        if (wordOffset < heapBitmapSize)
        {
            unsigned bitOffset = (unsigned) (topOffset % HEAP_BITS);
            heap_t mask = ((heap_t)1U)<<bitOffset;
            heap_t heapword = heapBitmap[wordOffset];
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
                    while (shortfall >= HEAP_BITS)
                    {
                        if (heapword != HEAP_ALLBITS)
                            break;
                        shortfall -= HEAP_BITS;
                        wordOffset++;
                        if (wordOffset==heapBitmapSize)
                            goto doublebreak;
                        heapword = heapBitmap[wordOffset];
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
        wordOffset = (unsigned) (pageOffset / HEAP_BITS);
        if (wordOffset < heapBitmapSize)
        {
            unsigned bitOffset = (unsigned) (pageOffset % HEAP_BITS);
            heap_t mask = ((heap_t)1U)<<bitOffset;
            unsigned foundAbove = (newPages - pages) - shortfall;
            unsigned needBelow = shortfall;
            heap_t heapword = heapBitmap[wordOffset];
            while (shortfall)
            {
                if (mask==1)
                {
                    while (shortfall >= HEAP_BITS && wordOffset > 0)
                    {
                        wordOffset--;
                        heapword = heapBitmap[wordOffset];
                        if (heapword != HEAP_ALLBITS)
                            return NULL;
                        shortfall -= HEAP_BITS;
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

static size32_t getCompressedSize(memsize_t heapSize)
{
    heapSize /= PACKED_ALIGNMENT;
    unsigned size;
    for (size = 0; heapSize; size++)
        heapSize >>= 8;
    return size;
}

HeapPointerCompressor::HeapPointerCompressor()
{
    compressedSize = getCompressedSize(heapEnd - heapBase);
}

void HeapPointerCompressor::compress(void * target, const void * ptr) const
{
    memsize_t value;
    if (ptr)
    {
        value = ((char *)ptr - heapBase);
        dbgassertex((value % PACKED_ALIGNMENT) == 0);
        value = value / PACKED_ALIGNMENT;
    }
    else
        value = 0;

    if (compressedSize == sizeof(unsigned))
    {
        *(unsigned *)target = (unsigned)value;
    }
    else
    {
#if __BYTE_ORDER == __LITTLE_ENDIAN
        memcpy(target, &value, compressedSize);
#else
        #error HeapPointerCompressor::compress unimplemented
#endif
    }
}

void * HeapPointerCompressor::decompress(const void * source) const
{
    memsize_t temp = 0;
    if (compressedSize == sizeof(unsigned))
    {
        temp = *(const unsigned *)source;
    }
    else
    {
#if __BYTE_ORDER == __LITTLE_ENDIAN
        memcpy(&temp, source, compressedSize);
#else
        #error HeapPointerCompressor::decompress unimplemented
#endif
    }
    if (temp)
        return (temp * PACKED_ALIGNMENT) + heapBase;
    return NULL;
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

size_t getRelativeRoxiePtr(const void *_ptr)
{
    const char *ptr = (const char *) _ptr;
    if (ptr >= heapBase && ptr < heapEnd)
        return ptr - heapBase;
    return -1;
}


inline static HeapletBase *findBase(const void *ptr)
{
    return (HeapletBase *) ((memsize_t) ptr & HEAP_ALIGNMENT_MASK);
}


void HeapletBase::release(const void *ptr)
{
    if (isValidRoxiePtr(ptr))
    {
        HeapletBase *h = findBase(ptr);
        h->noteReleased(ptr);
    }
}


void HeapletBase::releaseRowset(unsigned count, const byte * * rowset)
{
    if (isValidRoxiePtr(rowset))
    {
        HeapletBase *h = findBase(rowset);
        h->noteReleased(count, rowset);
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

unsigned HeapletBase::calcNumAllocated(bool updateCount) const
{
    // There is one refcount for the page itself
    return count.load(std::memory_order_relaxed)-1;
}

void HeapletBase::internalFreeNoDestructor(const void *ptr)
{
    dbgassertex(isValidRoxiePtr(ptr));
    HeapletBase *h = findBase(ptr);
    return h->_internalFreeNoDestructor(ptr);
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

void HeapletBase::prepareToCompactRow(const void * ptr, NewHeapCompactState & state)
{
    if (!isValidRoxiePtr(ptr))
        return;
    HeapletBase *h = findBase(ptr);
    h->_prepareToCompactRow(ptr, state);
}

const void * HeapletBase::newCompactRow(const void * ptr, NewHeapCompactState & state)
{
    if (!isValidRoxiePtr(ptr))
        return ptr;
    HeapletBase *h = findBase(ptr);
    return h->_newCompactRow(ptr, state);
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

//================================================================================

#ifdef _USE_CPPUNIT
static size_t parallelSyncReleaseGranularity = DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY;
static size_t parallelSyncReleaseThreshold = DEFAULT_PARALLEL_SYNC_RELEASE_THRESHOLD;

//Only used by the tuning unit tests, should possibly use a different flag to fix them as constant
static void setParallelSyncReleaseGranularity(size_t granularity, unsigned scaling)
{
    parallelSyncReleaseGranularity = granularity;
    parallelSyncReleaseThreshold = granularity * scaling;
}
#elif defined(PARALLEL_SYNC_RELEASE)
const static size_t parallelSyncReleaseGranularity = DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY;
const static size_t parallelSyncReleaseThreshold = DEFAULT_PARALLEL_SYNC_RELEASE_THRESHOLD;
#endif


inline void inlineReleaseRoxieRowArray(size_t count, const void * * rows)
{
    for (size_t i = 0; i < count; i++)
        ReleaseRoxieRow(rows[i]);
}

#ifdef PARALLEL_SYNC_RELEASE
class sync_releaser_task : public tbb::task
{
public:
    sync_releaser_task(size_t _num, const void * * _rows) : num(_num), rows(_rows) {}

    virtual task * execute()
    {
        inlineReleaseRoxieRowArray(num, rows);
        return NULL;
    }

public:
    size_t num;
    const void * * rows;
};

void ParallelReleaseRoxieRowArray(size_t count, const void * * rows)
{
    //This is quicker than creating a task list and then spawning all.
    tbb::task * completed_task = new (tbb::task::allocate_root()) tbb::empty_task();
    unsigned numTasks = (unsigned)((count + parallelSyncReleaseGranularity -1) / parallelSyncReleaseGranularity);
    completed_task->set_ref_count(1+numTasks);

    //Arrange the blocks that are freed from the array so that if the rows are allocated in order, adjacent blocks
    //are not released at the same time, therefore reducing contention.
    const unsigned stride = 8;
    for (unsigned j= 0; j < stride; j++)
    {
        size_t i = j * parallelSyncReleaseGranularity;
        for (; i < count; i += parallelSyncReleaseGranularity * stride)
        {
            size_t remain = count - i;
            size_t blockRows = (remain > parallelSyncReleaseGranularity) ? parallelSyncReleaseGranularity : remain;
            tbb::task * next = new (completed_task->allocate_child()) sync_releaser_task(blockRows, rows + i);
            tbb::task::spawn(*next);
        }
    }

    completed_task->wait_for_all();
    tbb::task::destroy(*completed_task);
}
#endif


void ReleaseRoxieRowArray(size_t count, const void * * rows)
{
#ifdef PARALLEL_SYNC_RELEASE
    if (count >= parallelSyncReleaseThreshold)
    {
        try
        {
            ParallelReleaseRoxieRowArray(count, rows);
        }
        catch (const std::exception & e)
        {
            //Catch and report the exception, but continue anyway...
            DBGLOG("TBB exception: %s in ReleaseRoxieRows", e.what());
        }
        return;
    }
#endif

    inlineReleaseRoxieRowArray(count, rows);
}

//Not implemented as ReleaseRoxieRowArray(to - from, rows + from) to avoid "from > to" wrapping issues
void roxiemem_decl ReleaseRoxieRowRange(const void * * rows, size_t from, size_t to)
{
#ifdef PARALLEL_SYNC_RELEASE
    if ((from < to) && ((to - from) >= parallelSyncReleaseThreshold))
    {
        try
        {
            ParallelReleaseRoxieRowArray(to-from, rows+from);
        }
        catch (const std::exception & e)
        {
            //Catch and report the exception, but continue anyway...
            DBGLOG("TBB exception: %s in ReleaseRoxieRowRange", e.what());
        }
        return;
    }
#endif
    for (size_t i = from; i < to; i++)
        ReleaseRoxieRow(rows[i]);
}


void ReleaseRoxieRows(ConstPointerArray &rows)
{
    ReleaseRoxieRowArray(rows.ordinality(), rows.getArray());
    rows.kill();
}

//================================================================================

struct HeapletStats
{
    __uint64 totalAllocs = 0;
    __uint64 totalDistanceScanned = 0;
};

class CHeap;
static void noteEmptyPage(CHeap * heap);
class Heaplet : public HeapletBase
{
    friend class CHeap;
protected:
    Heaplet *next;
    Heaplet *prev;
    const IRowAllocatorCache *allocatorCache;
    CHeap * const heap;
    memsize_t chunkCapacity;
    std::atomic_uint nextSpace; // guaranteed to be set if it is in the maybeFreeList.
    
    inline unsigned getActivityId(unsigned allocatorId) const
    {
        return getRealActivityId(allocatorId, allocatorCache);
    }

public:
    Heaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, memsize_t _chunkCapacity) : heap(_heap), chunkCapacity(_chunkCapacity), nextSpace(0)
    {
        assertex(heap);
        next = NULL;
        prev = NULL;
        allocatorCache = _allocatorCache;
    }

    virtual size32_t sizeInPages() = 0;

    virtual memsize_t _capacity() const { return chunkCapacity; }
    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const = 0;
    virtual void checkHeap() const = 0;
    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const = 0;
    virtual bool isFull() const = 0;
    virtual void releaseAllRows() = 0;

    void operator delete(void * p)
    {
        subfree_aligned(p, 1);
    }

    inline void addToSpaceList();
    virtual void verifySpaceList();

    inline bool isWithinHeap(CHeap * search) const { return heap == search; }
    const CHeap * queryHeap() const { return heap; }
};


//================================================================================
// Fixed size (chunking) heaplet
#define RBLOCKS_CAS_TAG        HEAP_ALIGNMENT_SIZE           // must be >= HEAP_ALIGNMENT_SIZE and power of 2
#define RBLOCKS_OFFSET_MASK    (RBLOCKS_CAS_TAG-1)
#define RBLOCKS_CAS_TAG_MASK   ~RBLOCKS_OFFSET_MASK
#define ROWCOUNT_MASK            0x7fffffff
#define ROWCOUNT_DESTRUCTOR_FLAG 0x80000000
#define ROWCOUNT(x)              ((x) & ROWCOUNT_MASK)

#define HEAPLET_DATA_AREA_OFFSET(heapletType) ((size32_t) ((sizeof(heapletType) + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE) * CACHE_LINE_SIZE)

const static unsigned FREE_ROW_COUNT = (unsigned)-1;
const static unsigned DELAYFREE_ROW_COUNT = (unsigned)-2;
const static unsigned FREE_ROW_COUNT_MIN = DELAYFREE_ROW_COUNT;


class CChunkedHeap;
class ChunkedHeaplet : public Heaplet
{
protected:
    std::atomic_uint r_blocks;  // the free chain as a relative pointer
    std::atomic_uint freeBase;
    const size32_t chunkSize;
    const unsigned heapFlags;
    unsigned sharedAllocatorId;
    unsigned nextMatchOffset = 0;
public:
    bool compacting = false;
    bool moveRows = false; // only valid if compacting
    bool hasSharedRow = false;
    std::atomic_bool hadDelayedRelease = {false};
    unsigned compactingRowCount = 0;  // only valid if compacting

    inline char *data() const
    {
        return ((char *) this) + dataOffset();
    }

    //NB: Derived classes should not contain any derived data.  The choice would be to put data() into the derived
    //classes, but that means it is hard to common up some of the code efficiently

public:
    ChunkedHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t _chunkSize, size32_t _chunkCapacity, unsigned _heapFlags)
        : Heaplet(_heap, _allocatorCache, _chunkCapacity), r_blocks(0), freeBase(0), chunkSize(_chunkSize), heapFlags(_heapFlags), sharedAllocatorId(0)
    {
    }

    virtual size32_t sizeInPages() { return 1; }

    //Is there any space within the heaplet that hasn't ever been allocated.
    inline bool hasAnyUnallocatedSpace() const
    {
        //This could use a special value of freeBase to indicate it was full, but that would add complication to
        //the allocation code which is more time critical.
        unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);
        size32_t bytesFree = dataAreaSize() - curFreeBase;
        return (bytesFree >= chunkSize);
    }

    virtual bool isFull() const override;

    virtual bool isEmpty() override;

    virtual unsigned calcNumAllocated(bool updateCount) const;

    inline static unsigned dataOffset() { return HEAPLET_DATA_AREA_OFFSET(ChunkedHeaplet); }

    inline static size32_t dataAreaSize() { return  (size32_t)(HEAP_ALIGNMENT_SIZE - dataOffset()); }

    virtual void _setDestructorFlag(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr;
        std::atomic_uint * curCount = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
        //Use memory_order_relaxed since this is called as the end of the constructor() on the creator thread
        curCount->store(1|ROWCOUNT_DESTRUCTOR_FLAG, std::memory_order_relaxed);
    }

    virtual bool _hasDestructor(const void *ptr) const
    {
        const std::atomic_uint * curCount = reinterpret_cast<const std::atomic_uint *>((const char *) ptr - sizeof(std::atomic_uint));
        // can access relaxed since the flag being tested is only set when constructing the row
        unsigned rowCount = curCount->load(std::memory_order_relaxed);
        return (rowCount & ROWCOUNT_DESTRUCTOR_FLAG) != 0;
    }

    virtual const void * _compactRow(const void * ptr, HeapCompactState & state);
    virtual void _prepareToCompactRow(const void * ptr, NewHeapCompactState & state);
    virtual const void * _newCompactRow(const void * ptr, NewHeapCompactState & state);

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

    const void * newMoveRow(const void * row)
    {
        //If this pointer already lives in this page don't move it (but update the current page source)
        //Shouldn't ever happen with the new code
        if (findBase(row) == this)
            return row;
        if (_isShared(row))
            return row;

        char * chunk = allocateChunk();
        if (!chunk)
            return NULL;
        return doMoveRow(chunk, row);
    }

    virtual const void * doMoveRow(char * chunk, const void * row) = 0;

    virtual void * initChunk(char * chunk, unsigned allocatorId) = 0;

    void * testAllocate(unsigned allocatorId)
    {
        //NOTE: There is no protecting lock before this call to allocate, but precreateFreeChain() has been called.
        char * ret = allocateChunk();
        if (!ret)
            return NULL;
        return initChunk(ret, allocatorId);
    }

    void precreateFreeChain()
    {
        if (heapFlags & RHFscanning)
            return;
        //The function is to aid testing - it allows the cas code to be tested without a surrounding lock
        //Allocate all possible rows and add them to the free space map.
        //This is not worth doing in general because it effectively replaces atomic sets with atomic cas
        //relaxed memory order since there will be no multi-threaded access
        unsigned nextFree = freeBase.load(std::memory_order_relaxed);
        unsigned nextBlock = r_blocks.load(std::memory_order_relaxed);
        for (;;)
        {
            size32_t bytesFree = dataAreaSize() - nextFree;
            if (bytesFree < chunkSize)
                break;

            char * ret = data() + nextFree;
            nextFree += chunkSize;

            //Add to the free chain
            * (unsigned *)ret = nextBlock;
            nextBlock = makeRelative(ret);
        }
        freeBase.store(nextFree, std::memory_order_relaxed);
        r_blocks.store(nextBlock, std::memory_order_relaxed);
    }

    inline char * allocateSingle(unsigned allocated, bool incCounter, unsigned & alreadyIncremented) __attribute__((always_inline));
    char * allocateChunk();
    unsigned allocateMultiChunk(unsigned max, char * * rows);   // allocates at least 1 row
    virtual void verifySpaceList();

    void startCompacting();
    void stopCompacting();
    inline bool isCompacting() const
    {
        return compacting;
    }
    inline bool isEmptyAfterCompact()
    {
        return moveRows && isEmpty();
    }
    inline unsigned numCompactingRows() const
    {
        return compactingRowCount;
    }
    inline void setMoveRows() { moveRows = true; }

protected:
    inline unsigned makeRelative(const char *ptr)
    {
        dbgassertex(ptr);
        ptrdiff_t diff = ptr - (char *) this;
        assert(diff < (ptrdiff_t) HEAP_ALIGNMENT_SIZE);
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
        std::atomic_uint * count = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
        //memory_order_relaxed may give false positives, but that will not cause issues.
        return ROWCOUNT(count->load(std::memory_order_relaxed)) !=1;
    }

    inline void inlineNoteLinked(const void *_ptr)
    {
        dbgassertex(_ptr != this);
        char *ptr = (char *) _ptr;
        std::atomic_uint * count = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
        count->fetch_add(1, std::memory_order_relaxed);
    }

    inline void inlineReleasePointer(char * ptr)
    {
        if (heapFlags & RHFscanning)
        {
            if (heapFlags & RHFdelayrelease)
            {
                bool anyFreed = hadDelayedRelease.load(std::memory_order_relaxed);
                ((std::atomic_uint *)ptr)->store(DELAYFREE_ROW_COUNT, std::memory_order_release);
                if (nextSpace.load(std::memory_order_relaxed) == 0)
                    addToSpaceList();
                if (!anyFreed)
                    hadDelayedRelease.store(true, std::memory_order_relaxed);
                return; // Link count is not decremented at this point.
            }
            else
            {
                ((std::atomic_uint *)ptr)->store(FREE_ROW_COUNT, std::memory_order_release);
                if (nextSpace.load(std::memory_order_relaxed) == 0)
                    addToSpaceList();
            }
        }
        else
        {
            unsigned r_ptr = makeRelative(ptr);
#ifdef HAS_EFFICIENT_CAS
            unsigned old_blocks = r_blocks.load(std::memory_order_relaxed);  // can be relaxed because the cas will fail if not up to date.
#endif
            for (;;)
            {
#ifndef HAS_EFFICIENT_CAS
                unsigned old_blocks = r_blocks.load(std::memory_order_relaxed);  // can be relaxed because the cas will fail if not up to date.
#endif
                //To prevent the ABA problem the top part of r_blocks stores an incrementing tag
                //which is incremented whenever something is added to the free list
                * (unsigned *) ptr = (old_blocks & RBLOCKS_OFFSET_MASK);
                unsigned new_tag = ((old_blocks & RBLOCKS_CAS_TAG_MASK) + RBLOCKS_CAS_TAG);
                unsigned new_blocks = new_tag | r_ptr;

                //memory_order_release ensures updates to next and count etc are available once the cas completes.
                if (compare_exchange_efficient(r_blocks, old_blocks, new_blocks, std::memory_order_release, std::memory_order_relaxed))
                {
                    //Try and add it to the potentially free page chain if it isn't already present.
                    //It is impossible to make it more restrictive -e.g., only when freeing and full because of
                    //various race conditions.
                    if (nextSpace.load(std::memory_order_relaxed) == 0)
                        addToSpaceList();
                    break;
                }
            }
        }

        CHeap * savedHeap = heap;
        // after the following dec it is possible that the page could be freed, so cannot access any members of this
        // std::memory_order_release ensures that savedHeap will not be evaluated after the fetch_sub.
        if (count.fetch_sub(1, std::memory_order_release) == 2)
        {
            //This thread does not access anything else from the object, so no need for an acquire fence!
            noteEmptyPage(savedHeap);
        }
    }
};

//================================================================================

class FixedSizeHeaplet : public ChunkedHeaplet
{
    //NOTE: This class should not contain any more data - otherwise the dataOffset() may be calculated incorrectly
    struct ChunkHeader
    {
        unsigned allocatorId;
        std::atomic_uint count;  //count must be the last item in the header
    };
public:
    enum { chunkHeaderSize = sizeof(ChunkHeader) };

    FixedSizeHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t size, unsigned _heapFlags)
    : ChunkedHeaplet(_heap, _allocatorCache, size, size - chunkHeaderSize, _heapFlags)
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
        //If count == 1 then no other thread can be releasing at the same time - so avoid locked operation
        //Subtract 1 here to try and minimize the conditional branches/simplify the fast path
        unsigned rowCount = header->count.load(std::memory_order_relaxed)-1;

        //NOTEs on memory order:
        //    The fetch_sub() needs to have memory_order_release to ensure earlier *reads* in this thread happen before
        //    any side-effects on another thread (which could occur after the row has been deleted).
        //
        //    The store (when count==1) cannot be effected by another thread since the other thread will never call the
        //    destructor, so the store can be relaxed.  (There is no other thread to synchronize with.)
        //
        //    An acquire fence would normally be required before the destructor to ensure that data written from another
        //    thread is correctly interpreted in the destructor.  (This would be true even if the data was updated inside
        //    a critical section.)
        //    However if we assert that data that is used in the destructor can only be initialised on the creating thread
        //    (i.e. rows, row arrays) the the acquire is not required.  If this assumption is broken the destructor must
        //    contain an acquire barrier.

        //It is coded this way to avoid re-evaluating ROWCOUNT() == 0. You could code it using a goto, but it generates worse code.
        if ((ROWCOUNT(rowCount) == 0) ?
                //If the count is zero then use comma expression to set the count for the record to zero as a
                //side-effect of this condition.  Could be avoided if leak checking and checkHeap worked differently.
                (header->count.store(rowCount, std::memory_order_relaxed), true) :
                //otherwise atomically decrement the count, and check if this thread was the last one to release
                //Note: the assignment to rowCount allows the compiler to reuse a register, improving the code slightly
                ROWCOUNT(rowCount = header->count.fetch_sub(1, std::memory_order_release)) == 1)
        {
            //See note above detailing the missing acquire barrier
            if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
            {
                unsigned id = header->allocatorId;
                allocatorCache->onDestroy(id & MAX_ACTIVITY_ID, ptr + chunkHeaderSize);
            }

#ifdef _CLEAR_FREED_ROW
            memset((void *)_ptr, 0xdd, chunkCapacity);
#endif
            inlineReleasePointer(ptr);
        }
    }

    virtual void noteReleased(unsigned numRows, const byte * * rowset) override
    {
        checkPtr(rowset, "Release");

        char *ptr = (char *) rowset - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
        //If count == 1 then no other thread can be releasing at the same time - so avoid locked operation
        //Subtract 1 here to try and minimize the conditional branches/simplify the fast path
        unsigned rowCount = header->count.load(std::memory_order_relaxed)-1;

        //NOTEs on memory order:
        //    The fetch_sub() needs to have memory_order_release to ensure earlier *reads* in this thread happen before
        //    any side-effects on another thread (which could occur after the row has been deleted).
        //
        //    The store (when count==1) cannot be effected by another thread since the other thread will never call the
        //    destructor, so the store can be relaxed.  (There is no other thread to synchronize with.)
        //
        //    An acquire fence would normally be required before the destructor to ensure that data written from another
        //    thread is correctly interpreted in the destructor.  (This would be true even if the data was updated inside
        //    a critical section.)
        //    However if we assert that data that is used in the destructor can only be initialised on the creating thread
        //    (i.e. rows, row arrays) the the acquire is not required.  If this assumption is broken the destructor must
        //    contain an acquire barrier.

        //It is coded this way to avoid re-evaluating ROWCOUNT() == 0. You could code it using a goto, but it generates worse code.
        if ((ROWCOUNT(rowCount) == 0) ?
                //If the count is zero then use comma expression to set the count for the record to zero as a
                //side-effect of this condition.  Could be avoided if leak checking and checkHeap worked differently.
                (header->count.store(rowCount, std::memory_order_relaxed), true) :
                //otherwise atomically decrement the count, and check if this thread was the last one to release
                //Note: the assignment to rowCount allows the compiler to reuse a register, improving the code slightly
                ROWCOUNT(rowCount = header->count.fetch_sub(1, std::memory_order_release)) == 1)
        {
            //See note above detailing the missing acquire barrier
            ReleaseRoxieRowArray(numRows, (const void * *)rowset);

#ifdef _CLEAR_FREED_ROW
            memset((void *)rowset, 0xdd, chunkCapacity);
#endif
            inlineReleasePointer(ptr);
        }
    }

    virtual void _internalFreeNoDestructor(const void * _ptr)
    {
        char *ptr = (char *) _ptr - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;
#ifdef _DEBUG
        unsigned rowCount = header->count.load(std::memory_order_relaxed);
        assertex(ROWCOUNT(rowCount) == 1);
#endif
        //Set to zero so that leak checking doesn't get false positives.  If the leak checking
        //worked differently - e.g, deducing from the free list this could be removed.

        //memory order can be relaxed - this must be the only thread with an active pointer to this sub-allocation,
        //and inlineReleasePointer() will ensure writes are updated before adding to the free list
        header->count.store(0, std::memory_order_relaxed);

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
        header->count.store(1, std::memory_order_relaxed);
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
        unsigned rowCountValue = oldHeader->count.load(std::memory_order_relaxed);
        unsigned destructFlag = (rowCountValue & ROWCOUNT_DESTRUCTOR_FLAG);
        unsigned allocatorId = oldHeader->allocatorId;
        newHeader->allocatorId = allocatorId;
        newHeader->count.store(1|destructFlag, std::memory_order_relaxed); // relaxed since initialisation code
        ret += chunkHeaderSize;
        memcpy(ret, row, chunkCapacity);

        //If this was the only instance then release the old row without calling the destructor
        if (ROWCOUNT(rowCountValue) == 1)
            HeapletBase::internalFreeNoDestructor(row);
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
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
        while (leaked > 0 && base < limit)
        {
            const char *block = data() + base;
            if (heapFlags & RHFscanning)
            {
                if (((std::atomic_uint *)block)->load(std::memory_order_relaxed) < FREE_ROW_COUNT_MIN)
                {
                    reportLeak(block, logctx);
                    leaked--;
                }
            }
            else
            {
                const char *ptr = block + chunkHeaderSize;  // assumes the overhead is all at the start
                std::atomic_uint * ptrCount = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
                unsigned rowCount = ptrCount->load(std::memory_order_relaxed);
                if (ROWCOUNT(rowCount) != 0)
                {
                    reportLeak(block, logctx);
                    leaked--;
                }
            }
            base += chunkSize;
        }
    }

    virtual void releaseAllRows() override
    {
        unsigned base = 0;
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
        while (base < limit)
        {
            char *block = data() + base;
            if (heapFlags & RHFscanning)
            {
                if (((std::atomic_uint *)block)->load(std::memory_order_relaxed) < FREE_ROW_COUNT_MIN)
                {
                    callDestructor(block);
                }
            }
            else
            {
                const char *ptr = block + chunkHeaderSize;  // assumes the overhead is all at the start
                std::atomic_uint * ptrCount = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
                unsigned rowCount = ptrCount->load(std::memory_order_relaxed);
                if (ROWCOUNT(rowCount) != 0)
                    callDestructor(block);
            }
            base += chunkSize;
        }
    }


    virtual void checkHeap() const 
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
        while (base < limit)
        {
            const char *block = data() + base;
            ChunkHeader * header = (ChunkHeader *)block;
            unsigned rowCount = header->count.load(std::memory_order_relaxed);
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
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
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
            unsigned rowCount = header->count.load(std::memory_order_relaxed);
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
        if (unlikely((header->allocatorId & ~ACTIVITY_MASK) != ACTIVITY_MAGIC))
        {
            DBGLOG("%s: Invalid pointer %p(%zu) id(%x) cnt(%x)", reason, ptr, getRelativeRoxiePtr(ptr), header->allocatorId, header->count.load());
            PrintStackReport();
            PrintMemoryReport();
            HEAPERROR("Invalid pointer");
        }
    }

    void reportLeak(const void * block, const IContextLogger &logctx) const
    {
        ChunkHeader * header = (ChunkHeader *)block;
        unsigned allocatorId = header->allocatorId;
        unsigned rowCount = header->count.load();
        bool hasChildren = (rowCount & ROWCOUNT_DESTRUCTOR_FLAG) != 0;

        const char * ptr = (const char *)block + chunkHeaderSize;
        logctx.CTXLOG("Block size %u at %p %swas allocated by activity %u and not freed (%d)", chunkSize, ptr, hasChildren ? "(with children) " : "", getActivityId(allocatorId), ROWCOUNT(rowCount));
    }

    void callDestructor(char * block)
    {
        ChunkHeader * header = (ChunkHeader *)block;
        unsigned rowCount = header->count.load();
        if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
        {
            unsigned allocatorId = header->allocatorId;
            allocatorCache->onDestroy(allocatorId & MAX_ACTIVITY_ID, (block + chunkHeaderSize));
        }
    }
};

//================================================================================

// NOTE - this delivers rows that are NOT 8 byte aligned, so can't safely be used to allocate ptr arrays

class PackedFixedSizeHeaplet : public ChunkedHeaplet
{
    //NOTE: This class should not contain any more data - otherwise the dataOffset() may be calculated incorrectly
    struct ChunkHeader
    {
        std::atomic_uint count;
    };
public:
    enum { chunkHeaderSize = sizeof(ChunkHeader) };

    PackedFixedSizeHeaplet(CHeap * _heap, const IRowAllocatorCache *_allocatorCache, size32_t size, unsigned _allocatorId, unsigned _heapFlags)
        : ChunkedHeaplet(_heap, _allocatorCache, size, size - chunkHeaderSize, _heapFlags)
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

        //If count == 1 then no other thread can be releasing at the same time - so avoid locked operation
        //Subtract 1 here to try and minimize the conditional branches
        unsigned rowCount = header->count.load(std::memory_order_relaxed)-1;

        //NORE: See comment on FixedSizeHeaplet::noteReleased() regarding the memory order operands

        //No need to reassign to rowCount if dec and read is required - since only top bit is used, and that must be the same
        //No need to ensure header->count is 0 because the free list overlaps the count so it is never checked.
        if (ROWCOUNT(rowCount) == 0 || ROWCOUNT(header->count.fetch_sub(1, std::memory_order_release)) == 1)
        {
            //No need for a fence - there is no problem if reads from below are processed before the condition since read only.
            if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
                allocatorCache->onDestroy(sharedAllocatorId & MAX_ACTIVITY_ID, ptr + chunkHeaderSize);

#ifdef _CLEAR_FREED_ROW
            memset((void *)_ptr, 0xdd, chunkCapacity);
#endif
            inlineReleasePointer(ptr);
        }
    }

    virtual void noteReleased(unsigned numRows, const byte * * rowset) override
    {
        char *ptr = (char *) rowset - chunkHeaderSize;
        ChunkHeader * header = (ChunkHeader *)ptr;

        //If count == 1 then no other thread can be releasing at the same time - so avoid locked operation
        //Subtract 1 here to try and minimize the conditional branches
        unsigned rowCount = header->count.load(std::memory_order_relaxed)-1;

        //NORE: See comment on FixedSizeHeaplet::noteReleased() regarding the memory order operands

        //No need to reassign to rowCount if dec and read is required - since only top bit is used, and that must be the same
        //No need to ensure header->count is 0 because the free list overlaps the count so it is never checked.
        if (ROWCOUNT(rowCount) == 0 || ROWCOUNT(header->count.fetch_sub(1, std::memory_order_release)) == 1)
        {
            //No need for a fence - there is no problem if reads from below are processed before the condition since read only.
            ReleaseRoxieRowArray(numRows, (const void * *)rowset);

#ifdef _CLEAR_FREED_ROW
            memset((void *)rowset, 0xdd, chunkCapacity);
#endif
            inlineReleasePointer(ptr);
        }
    }

    virtual void _internalFreeNoDestructor(const void * _ptr)
    {
        char *ptr = (char *) _ptr - chunkHeaderSize;
#ifdef _DEBUG
        ChunkHeader * header = (ChunkHeader *)ptr;
        unsigned rowCount = header->count.load(std::memory_order_relaxed);
        assertex(ROWCOUNT(rowCount) == 1);
#endif
        //NOTE: The free list overlaps the count, so there is no point in updating the count.
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
        header->count.store(1, std::memory_order_relaxed);
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
        unsigned rowCountValue = oldHeader->count.load(std::memory_order_relaxed);
        unsigned destructFlag = (rowCountValue & ROWCOUNT_DESTRUCTOR_FLAG);
        newHeader->count.store(1|destructFlag, std::memory_order_relaxed); // relaxed since initialising
        ret += chunkHeaderSize;
        memcpy(ret, row, chunkCapacity);

        //If this was the only instance then release the old row without calling the destructor
        //(to avoid the overhead of having to call onClone).
        if (ROWCOUNT(rowCountValue) == 1)
            HeapletBase::internalFreeNoDestructor(row);
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
        unsigned numLeaked = calcNumAllocated(true);
        //Because there is only a 4 byte counter on each field which is reused for the field list
        //it isn't possible to walk the rows in 0..freeBase and see if they are allocated or not
        //so have to be content with a summary
        if (numLeaked)
            logctx.CTXLOG("Packed allocator for size %" I64F "u activity %u leaks %u rows", (unsigned __int64) chunkCapacity, getActivityId(sharedAllocatorId), numLeaked);
        leaked -= numLeaked;
    }

    virtual void releaseAllRows() override
    {
        unsigned base = 0;
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
        while (base < limit)
        {
            char *block = data() + base;
            if (heapFlags & RHFscanning)
            {
                if (((std::atomic_uint *)block)->load(std::memory_order_relaxed) < FREE_ROW_COUNT_MIN)
                    callDestructor(block);
            }
            else
            {
                const char *ptr = block + chunkHeaderSize;  // assumes the overhead is all at the start
                std::atomic_uint * ptrCount = (std::atomic_uint *)(ptr - sizeof(std::atomic_uint));
                unsigned rowCount = ptrCount->load(std::memory_order_relaxed);
                if (ROWCOUNT(rowCount) != 0)
                    callDestructor(block);
            }
            base += chunkSize;
        }
    }

    virtual unsigned _rawAllocatorId(const void *ptr) const
    {
        return sharedAllocatorId;
    }

    virtual void checkHeap() const
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = freeBase.load(std::memory_order_acquire); // acquire ensures that any link counts will be initialised
        while (base < limit)
        {
            const char *block = data() + base;
            ChunkHeader * header = (ChunkHeader *)block;
            unsigned rowCount = header->count.load(std::memory_order_relaxed);
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
        //calcNumAllocated() will already have been called.
        unsigned numAllocs = queryCount()-1;
        if (numAllocs)
        {
            map->noteMemUsage(sharedAllocatorId, numAllocs * chunkSize, numAllocs);
        }
    }

    void callDestructor(char * block)
    {
        ChunkHeader * header = (ChunkHeader *)block;
        unsigned rowCount = header->count.load();
        if (rowCount & ROWCOUNT_DESTRUCTOR_FLAG)
            allocatorCache->onDestroy(sharedAllocatorId & MAX_ACTIVITY_ID, (block + chunkHeaderSize));
    }
};

//================================================================================
// Row manager - chunking

class CHugeHeap;
class HugeHeaplet : public Heaplet
{
protected:
    unsigned allocatorId;
    std::atomic_uint rowCount;  // A separate rowcount is required, otherwise the page could be freed before the destructor is called

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
        return rowCount.load(std::memory_order_relaxed) > 1;
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
        //NORE: See comment on FixedSizeHeaplet::noteReleased() regarding the memory order operands

        //If rowCount == 1 then this must be the last reference - avoid a locked operation.
        //rowCount is not used once the heaplet is known to be freed, so no need to ensure rowCount=0
        if (rowCount.load(std::memory_order_relaxed) == 1 || rowCount.fetch_sub(1, std::memory_order_release) == 1)
        {
            //No need for a memory barrier - any reads moved earlier will be valid
            if (allocatorId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
                allocatorCache->onDestroy(allocatorId & MAX_ACTIVITY_ID, (void *)ptr);

            CHeap * savedHeap = heap;
            addToSpaceList();
            // after the following dec(count) it is possible that the page could be freed, so cannot access any members of this
            // the memory order release ensures savedHeap is evaluated before the decrement
            unsigned cnt = count.fetch_sub(1, std::memory_order_release);
            assertex(cnt == 2);
            noteEmptyPage(savedHeap);
        }
    }

    virtual void noteReleased(unsigned numRows, const byte * * rowset) override
    {
        //NORE: See comment on FixedSizeHeaplet::noteReleased() regarding the memory order operands

        //If rowCount == 1 then this must be the last reference - avoid a locked operation.
        //rowCount is not used once the heaplet is known to be freed, so no need to ensure rowCount=0
        if (rowCount.load(std::memory_order_relaxed) == 1 || rowCount.fetch_sub(1, std::memory_order_release) == 1)
        {
            //No need for a memory barrier - any reads moved earlier will be valid
            ReleaseRoxieRowArray(numRows, (const void * *)rowset);

            CHeap * savedHeap = heap;
            addToSpaceList();
            // after the following dec(count) it is possible that the page could be freed, so cannot access any members of this
            // the memory order release ensures savedHeap is evaluated before the decrement
            unsigned cnt = count.fetch_sub(1, std::memory_order_release);
            assertex(cnt == 2);
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
        rowCount.fetch_add(1, std::memory_order_relaxed);
    }

    void *allocateHuge(memsize_t size)
    {
        count.fetch_add(1, std::memory_order_relaxed); // setting to 2 would be more efficient!
        rowCount.store(1, std::memory_order_relaxed);
        dbgassertex(size <= chunkCapacity);
#ifdef _CLEAR_ALLOCATED_HUGE_ROW
        memset(data(), 0xcc, chunkCapacity);
#endif
        return data();
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const 
    {
        logctx.CTXLOG("Block size %" I64F "u was allocated by activity %u and not freed", (unsigned __int64) chunkCapacity, getActivityId(allocatorId));
        leaked--;
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const 
    {
        map->noteMemUsage(allocatorId, chunkCapacity, 1);
        map->noteHeapUsage(chunkCapacity, RHFhuge, _sizeInPages(), chunkCapacity, allocatorId);
    }

    virtual void checkHeap() const
    {
        //MORE
    }

    virtual const void * _compactRow(const void * ptr, HeapCompactState & state)
    {
        return ptr;
    }

    virtual void _prepareToCompactRow(const void * ptr, NewHeapCompactState & state)
    {
    }

    virtual const void * _newCompactRow(const void * ptr, NewHeapCompactState & state)
    {
        return ptr;
    }

    virtual void _internalFreeNoDestructor(const void *ptr)
    {
        throwUnexpected();
    }

    virtual bool isFull() const
    {
        return (count.load(std::memory_order_relaxed) > 1);
    }

    virtual void releaseAllRows() override
    {
        //Weird for this to ever be called.
        if (allocatorId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
            allocatorCache->onDestroy(allocatorId & MAX_ACTIVITY_ID, data());
    }
};

//================================================================================

inline unsigned heapletToBlock(Heaplet * heaplet)
{
    dbgassertex(heaplet);
    return ((char *)heaplet - heapBase) / HEAP_ALIGNMENT_SIZE;
}

inline Heaplet * blockToHeaplet(unsigned block)
{
    unsigned maskedBlock = block & BLOCKLIST_MASK;
    dbgassertex(maskedBlock != BLOCKLIST_NULL);
    return (Heaplet *)(heapBase + maskedBlock * HEAP_ALIGNMENT_SIZE);
}


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
    HeapEntry(memsize_t _allocatorSize, RoxieHeapFlags _heapFlags, memsize_t _numPages, memsize_t _memUsed, unsigned _allocatorId) :
        allocatorSize(_allocatorSize), heapFlags(_heapFlags), numPages(_numPages), memUsed(_memUsed), allocatorId(_allocatorId)
    {
    }

    memsize_t allocatorSize;
    RoxieHeapFlags heapFlags;
    memsize_t numPages;
    memsize_t memUsed;
    unsigned allocatorId;
};

typedef MapBetween<unsigned, unsigned, ActivityEntry, ActivityEntry> MapActivityToActivityEntry;

class CActivityMemoryUsageMap : implements IActivityMemoryUsageMap, public CInterface
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

    virtual void noteMemUsage(unsigned allocatorId, memsize_t memUsed, unsigned numAllocs) override
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

    void noteHeapUsage(memsize_t allocatorSize, RoxieHeapFlags heapFlags, memsize_t numPages, memsize_t memUsed, unsigned allocatorId) override
    {
        heaps.append(*new HeapEntry(allocatorSize, heapFlags, numPages, memUsed, allocatorId));
    }

    static int sortUsage(const void *_l, const void *_r)
    {
        const ActivityEntry *l = *(const ActivityEntry **) _l;
        const ActivityEntry *r = *(const ActivityEntry **) _r;
        return (l->usage > r->usage) ? +1 : (l->usage < r->usage) ? -1 : 0;
    }

    virtual void report(const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache) override
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
            logctx.CTXLOG("Snapshot of peak memory usage: %" I64F "u total bytes allocated", (unsigned __int64) totalUsed);
            while (j)
            {
                j--;
                unsigned activityId = getRealActivityId(results[j]->allocatorId, allocatorCache);
                logctx.CTXLOG("%" I64F "u bytes allocated by activity %u (%u allocations)", (unsigned __int64) results[j]->usage, activityId, results[j]->allocations);
            }

            memsize_t totalHeapPages = 0;
            memsize_t totalHeapUsed = 0;
            logctx.CTXLOG("Heaps:");
            ForEachItemIn(iHeap, heaps)
            {
                HeapEntry & cur = heaps.item(iHeap);
                StringBuffer flags;
                if (cur.heapFlags & RHFhasdestructor)
                    flags.append("~");
                if (cur.heapFlags & RHFpacked)
                    flags.append("P");
                if (cur.heapFlags & RHFunique)
                    flags.append("U");
                if (cur.heapFlags & RHFvariable)
                    flags.append("V");
                if (cur.heapFlags & RHFscanning)
                    flags.append("S");
                if (cur.heapFlags & RHFdelayrelease)
                    flags.append("D");
                if (cur.heapFlags & RHFhuge)
                    flags.append("H");
                if (cur.heapFlags & RHForphaned)
                    flags.append("O");
                if (cur.allocatorId)
                    flags.append("@").append(getRealActivityId(cur.allocatorId, allocatorCache));

                //Should never be called with numPages == 0, but protect against divide by zero in case of race condition etc.
                unsigned __int64 memReserved = cur.numPages * HEAP_ALIGNMENT_SIZE;
                unsigned percentUsed = cur.numPages ? (unsigned)((cur.memUsed * 100) / memReserved) : 100;
                logctx.CTXLOG("size: %" I64F "u [%s] reserved: %" I64F "up %u%% (%" I64F "u/%" I64F "u) used",
                        (unsigned __int64) cur.allocatorSize, flags.str(), (unsigned __int64) cur.numPages, percentUsed, (unsigned __int64) cur.memUsed, (unsigned __int64) memReserved);
                totalHeapPages += cur.numPages;
                totalHeapUsed += cur.memUsed;
            }

            //Should never be called with numPages == 0, but protect against divide by zero in case of race condition etc.
            unsigned __int64 totalReserved = totalHeapPages * HEAP_ALIGNMENT_SIZE;
            unsigned percentUsed = totalHeapPages ? (unsigned)((totalHeapUsed * 100) / totalReserved) : 100;
            logctx.CTXLOG("Total: %" I64F "up %u%% (%" I64F "u/%" I64F "u) used",
                    (unsigned __int64) totalHeapPages, percentUsed, (unsigned __int64) totalHeapUsed, (unsigned __int64) totalReserved);

            logctx.CTXLOG("------------------ End of snapshot");
            delete [] results;
        }
    }

    virtual void reportStatistics(IStatisticTarget & target, unsigned detailtarget, const IRowAllocatorCache *allocatorCache) override
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

            target.addStatistic(SSTallocator, activityText.str(), StSizePeakMemory, NULL, results[j]->usage, results[j]->allocations, 0, StatsMergeMax);
        }
        delete [] results;

        target.addStatistic(SSTglobal, NULL, StSizePeakMemory, NULL, totalUsed, 1, 0, StatsMergeMax);
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

    virtual void emptyCache() override
    {
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

    virtual void gatherStats(CRuntimeStatisticCollection & stats) override {}

    virtual void *allocate();

    virtual void releaseAllRows()
    {
        throw MakeStringExceptionDirect(ROXIEMM_RELEASE_ALL_SHARED_HEAP, "Illegal to release all rows for a shared heap");
    }

protected:
    size32_t chunkCapacity;
};


template <class T>
class CRoxieDirectFixedRowHeap : public CRoxieFixedRowHeapBase
{
public:
    CRoxieDirectFixedRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags, T * _heap)
        : CRoxieFixedRowHeapBase(_rowManager, _allocatorId, _flags), heap(_heap)
    {
    }
    ~CRoxieDirectFixedRowHeap()
    {
        if (heap)
        {
            heap->checkScans(allocatorId);
            if (flags & RHFunique)
                heap->noteOrphaned();
        }
    }

    virtual void gatherStats(CRuntimeStatisticCollection & stats) override
    {
        heap->gatherStats(stats);
    }

    virtual void *allocate()
    {
        return heap->allocate(allocatorId);
    }

    virtual void clearRowManager()
    {
        if (heap)
            heap->checkScans(allocatorId);
        heap.clear();
        CRoxieFixedRowHeapBase::clearRowManager();
    }

    virtual void releaseAllRows() override
    {
        heap->releaseAllRows();
    }

protected:
    Owned<T> heap;
};

template <class T>
class CRoxieDirectFixedBlockedRowHeap : public CRoxieDirectFixedRowHeap<T>
{
public:
    CRoxieDirectFixedBlockedRowHeap(CChunkingRowManager * _rowManager, unsigned _allocatorId, RoxieHeapFlags _flags, T * _heap)
        : CRoxieDirectFixedRowHeap<T>(_rowManager, _allocatorId, _flags, _heap)
    {
    }

    virtual void *allocate()
    {
        if (curRow == numRows)
        {
            numRows = this->heap->allocateBlock(this->allocatorId, maxRows, rows);
            curRow = 0;
        }
        return rows[curRow++];
    }

    virtual void clearRowManager()
    {
        emptyCache();
        CRoxieDirectFixedRowHeap<T>::clearRowManager();
    }

    virtual void emptyCache() override
    {
        while (curRow < numRows)
            ::ReleaseRoxieRow(rows[curRow++]);
    }

    virtual void releaseAllRows() override
    {
        curRow = numRows;
        CRoxieDirectFixedRowHeap<T>::releaseAllRows();
    }

protected:
    static const unsigned maxRows = 16; // Maximum number of rows to allocate at once.
    char * rows[maxRows]; // Deliberately uninitialized
    unsigned curRow = 0;
    unsigned numRows = 0;
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

    virtual void gatherStats(CRuntimeStatisticCollection & stats) override;
    virtual void *allocate(memsize_t size, memsize_t & capacity);
    virtual void *resizeRow(void * original, memsize_t copysize, memsize_t newsize, memsize_t &capacity);
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize);

protected:
    CChunkingRowManager * rowManager;       // Lifetime of rowManager is guaranteed to be longer
    unsigned __int64 numAllocations = 0;    // Not necessary accurate if concurrent calls to allocate
    unsigned allocatorId;
    RoxieHeapFlags flags;
};


//================================================================================
//
//Responsible for allocating memory for a chain of chunked blocks
class CHeap : public CInterface
{
    friend class HeapCompactState;
    friend class ChunkedHeaplet;
public:
    CHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, unsigned _flags)
        : flags(_flags), rowManager(_rowManager), allocatorCache(_allocatorCache), logctx(_logctx)
    {
    }

    ~CHeap()
    {
        if (memTraceLevel >= 3)
        {
            //ensure verifySpaceListConsistency isn't triggered by leaked allocations that are never freed.
            if (activeHeaplet && activeHeaplet->nextSpace.load(std::memory_order_relaxed) == 0)
                activeHeaplet->nextSpace.store(BLOCKLIST_NULL, std::memory_order_relaxed);

            verifySpaceListConsistency();
        }

        if (heaplets)
        {
            Heaplet *finger = heaplets;

            //Note: This loop doesn't unlink the list because the list and all blocks are going to be disposed.
            do
            {
                if (memTraceLevel >= 3)
                    logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor freeing heaplet linked in active list - addr=%p rowMgr=%p",
                            finger, this);
                Heaplet *next = getNext(finger);
                delete finger;
                finger = next;
            } while (finger != heaplets);
        }
        heaplets = NULL;
        activeHeaplet = NULL;
    }

    void addToSpaceList(Heaplet * heaplet)
    {
        //Careful: Two threads might be calling this at exactly the same time: ensure only one goes any further
        unsigned expected = 0;
        if (!heaplet->nextSpace.compare_exchange_strong(expected, BLOCKLIST_NULL, std::memory_order_relaxed))
            return;

        unsigned block = heapletToBlock(heaplet);
        unsigned head = headMaybeSpace.load(std::memory_order_relaxed); // can be relaxed since not accessing anything from another thread
        for (;;)
        {
            //Update the next pointer.  BLOCKLIST_ABA_INC is ORed with the value to ensure it is non-zero.
            //Relaxed - no one else can access, and following release will ensure it is available.
            heaplet->nextSpace.store(head | BLOCKLIST_ABA_INC, std::memory_order_relaxed);

            //Ensure any items added onto the list have a new aba tag
            unsigned newHead = block + (head & BLOCKLIST_ABA_MASK) + BLOCKLIST_ABA_INC;
            if (headMaybeSpace.compare_exchange_weak(head, newHead, std::memory_order_release)) // release to ensure next is initialised once added to the list
                break;
        }
    }

    Heaplet * popFromSpaceList()
    {
        //This must only be called within a critical section since some functions assume only one active thread is
        //allowed to remove elements from the list
        unsigned head = headMaybeSpace.load(std::memory_order_acquire); // acquire so that heaplet->nextSpace is available
        for (;;)
        {
            if (isNullBlock(head))
                return NULL;

            Heaplet * heaplet = blockToHeaplet(head);

            //Always valid to access a heaplet on a list, because we must remove from all lists before disposing.
            unsigned next = heaplet->nextSpace.load(std::memory_order_relaxed);

            //No need to update the aba mask on removal since removal cannot create a false positives.
            if (headMaybeSpace.compare_exchange_weak(head, next, std::memory_order_acquire))
            {
                //Indicate that this item is no longer on the list.
                heaplet->nextSpace.store(0, std::memory_order_relaxed);

                //NOTE: If another thread tries to add it before this set succeeds that doesn't cause a problem since on return this heaplet will be processed
                return heaplet;
            }
        }
    }

    void removeFromSpaceList(Heaplet * toRemove)
    {
        //This must only be called within a critical section so only one thread can access at once.
        //NOTE: We don't care about items being added while this loop is iterating - since the item
        //being removed cannot be being added.
        //And nothing else can be being removed - since we are protected by the critical section

        //Load the head of the free list with acquire semantics to ensure value of nextSpace will be synchronized
        unsigned head = headMaybeSpace.load(std::memory_order_acquire);

        //NextSpace can't change while this function is executing
        unsigned nextSpace = toRemove->nextSpace.load(std::memory_order_relaxed);
        //If not on the list then return immediately
        if (nextSpace == 0)
            return;

        //Special case head because that can change while this is being executed...
        unsigned searchBlock = heapletToBlock(toRemove);
        if (isNullBlock(head))
        {
            //The block wasn't found on the space list even though it should have been
            throwUnexpected();
            return;
        }

        if ((head & BLOCKLIST_MASK) == searchBlock)
        {
            //Currently head of the list, try and remove it
            if (headMaybeSpace.compare_exchange_weak(head, nextSpace, std::memory_order_acquire)) // acquire ensures prevHeplet->nextSpace is up to date
            {
                toRemove->nextSpace.store(0, std::memory_order_relaxed); // ensure the cas is seen by other threads before this write.
                return;
            }

            //head changed - head already updated - fall through since it must now be a child of that new head
        }

        //Not at the head of the list, and head is not NULL
        Heaplet * prevHeaplet = blockToHeaplet(head);
        for (;;)
        {
            unsigned next = prevHeaplet->nextSpace.load(std::memory_order_relaxed);
            if (isNullBlock(next))
            {
                //The block wasn't found on the space list even though it should have been
                throwUnexpected();
                return;
            }

            Heaplet * heaplet = blockToHeaplet(next);
            if (heaplet == toRemove)
            {
                //Remove the item from the list, and indicate it is no longer on the list
                prevHeaplet->nextSpace.store(nextSpace, std::memory_order_relaxed);
                //Use release to ensure that the block is removed from the list before setting next to 0
                toRemove->nextSpace.store(0, std::memory_order_release);
                return;
            }
            prevHeaplet = heaplet;
        }
    }

    bool mayHaveEmptySpace() const
    {
        unsigned head = headMaybeSpace.load(std::memory_order_relaxed);
        return !isNullBlock(head);
    }

    void reportLeaks(unsigned &leaked) const
    {
        CriticalBlock c1(heapletLock);
        Heaplet * start = heaplets;
        if (start)
        {
            Heaplet * finger = start;
            while (leaked)
            {
                if (leaked && memTraceLevel >= 1)
                    finger->reportLeaks(leaked, logctx);
                finger = getNext(finger);
                if (finger == start)
                    break;
            }
        }
    }

    void checkHeap()
    {
        CriticalBlock c1(heapletLock);
        Heaplet * start = heaplets;
        if (start)
        {
            Heaplet * finger = start;
            do
            {
                finger->checkHeap();
                finger = getNext(finger);
            } while (finger != start);
        }
    }

    unsigned allocated()
    {
        unsigned total = 0;
        CriticalBlock c1(heapletLock);
        Heaplet * start = heaplets;
        if (start)
        {
            Heaplet * finger = start;
            do
            {
                total += finger->calcNumAllocated(true);
                finger = getNext(finger);
            } while (finger != start);
        }
        return total;
    }

    void verifySpaceListConsistency()
    {
        //Check that all blocks are either full, on the with-possible-space list or active
        Heaplet * start = heaplets;
        Heaplet * finger = start;
        if (start)
        {
            do
            {
                if (!finger->isFull())
                    finger->verifySpaceList();
                finger = getNext(finger);
            } while (finger != start);
        }
    }

    unsigned releasePage(Heaplet * finger)
    {
        if (memTraceLevel >= 3)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::pages() freeing Heaplet linked in active list - addr=%p pages=%u capacity=%" I64F "u rowMgr=%p",
                    finger, finger->sizeInPages(), (unsigned __int64) finger->_capacity(), this);

        removeHeaplet(finger);
        if (finger == activeHeaplet)
            activeHeaplet = NULL;

        unsigned size = finger->sizeInPages();
        //It is possible (but very unlikely) for another thread to have added this block to the space list.
        //Ensure it is not on the list.
        removeFromSpaceList(finger);
        delete finger;
        return size;
    }

    unsigned releaseEmptyPages(bool forceFreeAll)
    {
        //If releaseEmptyPages() is called between the last release on a page (setting count to 1), and this flag
        //getting set, it won't release the page *this time*.  But that is the same as the release happening
        //slightly later.
        if (!possibleEmptyPages.load(std::memory_order_relaxed) && !(flags & RHFdelayrelease))
            return 0;

        unsigned total = 0;
        CriticalBlock c1(heapletLock);
        //Check again in case other thread has also called this function and no other pages have been released.
        if (!possibleEmptyPages.load(std::memory_order_acquire) && !(flags & RHFdelayrelease))
            return 0;

        if (flags & RHForphaned)
            forceFreeAll = true;

        //You will get a false positive if possibleEmptyPages is set while walking the active page list, but that
        //only mean the list is walked more than it needs to be.
        possibleEmptyPages.store(false, std::memory_order_release);

        //Any blocks that could be freed must either be the active block and/or on the maybe space list.
        Heaplet * headHeaplet;
        Heaplet * preserved = NULL;
        //First free any empty blocks at the head of the maybe space list
        for (;;)
        {
            unsigned head = headMaybeSpace.load(std::memory_order_acquire);
            if (isNullBlock(head))
            {
                headHeaplet = NULL;
                break;
            }

            headHeaplet = blockToHeaplet(head);
            //Is it possible to free this heaplet?
            if (!headHeaplet->isEmpty())
                break;

            //If this is the only page then only free it if forced to.
            if ((headHeaplet->next == headHeaplet) && !forceFreeAll)
            {
                preserved = headHeaplet;
                break;
            }

            //Always valid to access a heaplet on a list, because we must remove from all lists before disposing.
            unsigned next = headHeaplet->nextSpace.load(std::memory_order_relaxed);

            //No need to update the aba mask on removal since removal cannot create a false positives.
            if (headMaybeSpace.compare_exchange_weak(head, next, std::memory_order_acquire))
            {
                headHeaplet->nextSpace.store(0, std::memory_order_relaxed);
                total += releasePage(headHeaplet);
            }
        }

        //Not going to modify head, so can now walk the rest of the items, other threads can only add to head.
        if (headHeaplet)
        {
            Heaplet * prevHeaplet = headHeaplet;
            for (;;)
            {
                unsigned curSpace = prevHeaplet->nextSpace.load(std::memory_order_relaxed);
                if (isNullBlock(curSpace))
                    break;

                Heaplet * heaplet = blockToHeaplet(curSpace);
                if (heaplet->isEmpty())
                {
                    //Remove it directly rather than walking the list to remove it.
                    unsigned nextSpace = heaplet->nextSpace.load(std::memory_order_relaxed);
                    prevHeaplet->nextSpace.store(nextSpace, std::memory_order_relaxed);
                    heaplet->nextSpace.store(0, std::memory_order_release); // ensure nextSpace is updated after it is removed from the list
                    total += releasePage(heaplet);
                }
                else
                    prevHeaplet = heaplet;
            }
        }

        if (activeHeaplet && forceFreeAll)
        {
            //I am not convinced this can ever lead to an extra page being released - except when it is released
            //after the space list has been walked.  Keep for the moment just to be sure.
            assertex(!preserved);
            if (activeHeaplet->isEmpty())
                total += releasePage(activeHeaplet);
        }
        else if (preserved)
        {
            //Add this page back onto the potential-space list
            possibleEmptyPages.store(true, std::memory_order_release);
            //No barrier - another thread will only read from the space list if possibleEmptyPages is true
            addToSpaceList(preserved);
        }

        return total;
    }

    void getPeakActivityUsage(IActivityMemoryUsageMap * usageMap) const
    {
        unsigned numPages = 0;
        memsize_t numAllocs = 0;
        {
            CriticalBlock c1(heapletLock);
            Heaplet * start = heaplets;
            if (start)
            {
                Heaplet * finger = start;
                do
                {
                    unsigned thisCount = finger->calcNumAllocated(true);
                    if (thisCount != 0)
                        finger->getPeakActivityUsage(usageMap);
                    numAllocs += thisCount;
                    numPages++;
                    finger = getNext(finger);
                } while (finger != start);
            }
        }
        if (numPages)
            reportHeapUsage(usageMap, numPages, numAllocs);
    }

    inline bool isEmpty() const { return !heaplets; }

    virtual bool matches(size32_t searchSize, unsigned searchActivity, unsigned searchFlags) const
    {
        return false;
    }

    void noteEmptyPage() { possibleEmptyPages.store(true, std::memory_order_release); }

    inline void updateNumAllocs(unsigned __int64 allocs)
    {
        stats.totalAllocs += allocs;
    }

    inline void updateDistanceScanned(unsigned __int64 distance)
    {
        stats.totalDistanceScanned += distance;
    }

protected:
    virtual void reportHeapUsage(IActivityMemoryUsageMap * usageMap, unsigned numPages, memsize_t numAllocs) const = 0;

    inline Heaplet * getNext(const Heaplet * ptr) const { return ptr->next; }
    inline void setNext(Heaplet * ptr, Heaplet * next) const { ptr->next = next; }
    inline Heaplet * getPrev(const Heaplet * ptr) const { return ptr->prev; }
    inline void setPrev(Heaplet * ptr, Heaplet * prev) const { ptr->prev = prev; }

    //Must be called within a critical section
    void insertHeaplet(Heaplet * ptr)
    {
        if (!heaplets)
        {
            ptr->prev = ptr;
            ptr->next = ptr;
            heaplets = ptr;
        }
        else
        {
            Heaplet * next = heaplets;
            Heaplet * prev = next->prev;
            ptr->next = next;
            ptr->prev = prev;
            prev->next = ptr;
            next->prev = ptr;
        }
    }

    void removeHeaplet(Heaplet * ptr)
    {
        Heaplet * next = ptr->next;
        if (next != ptr)
        {
            Heaplet * prev = ptr->prev;
            next->prev = prev;
            prev->next = next;
            //Ensure that heaplets isn't invalid
            heaplets = next;
        }
        else
            heaplets = NULL;
        //NOTE: We do not clear next/prev in the heaplet being removed.
    }

    inline void internalLock()
    {
        if (compacting++ == 0)
            heapletLock.enter();
    }
    inline void internalUnlock()
    {
        if (--compacting == 0)
            heapletLock.leave();
    }

    void throwHeapExhausted(unsigned allocatorId, unsigned pages) const;

protected:
    unsigned flags; // before the pointer so it packs better in 64bit.
    Heaplet * activeHeaplet = nullptr; // which block is the current candidate for adding rows.
    Heaplet * heaplets = nullptr; // the linked list of heaplets for this heap
    CChunkingRowManager * rowManager;
    const IRowAllocatorCache *allocatorCache;
    const IContextLogger & logctx;
    mutable CriticalSection heapletLock;
    std::atomic_uint headMaybeSpace{BLOCKLIST_NULL};  // The head of the list of heaplets which potentially have some space.  When adding must use mo_release, when removing must use mo_acquire
    std::atomic_uint possibleEmptyPages{false};  // Are there any pages with 0 records.  Primarily here to avoid walking long page chains.
    HeapletStats stats;
    mutable unsigned compacting = 0;
};


//---------------------------------------------------------------------------------------------------------------------

static CriticalSection compactCs;
class HeapCompactState
{
public:
    inline HeapCompactState() : lock(compactCs), numPagesEmptied(0), heap(NULL), next(NULL)
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
            {
                _heap->internalLock();
                next = _heap->heaplets;
            }
            else
                next = NULL;
        }
    }
public:
    CriticalBlock lock;
    unsigned numPagesEmptied;
    CHeap * heap;
    Heaplet * next; // which heaplet to try to compact into next.  Not so good if > 1 heaps in use.
};

class NewHeapCompactState
{
public:
    NewHeapCompactState() : lock(compactCs)
    {
    }

    ~NewHeapCompactState()
    {
        ForEachItemIn(i, heaplets)
            heaplets.item(i)->stopCompacting();
    }

    bool calculateRowsToMove();

    void noteHeaplet(ChunkedHeaplet * heaplet)
    {
        if (!heaplet->isCompacting())
        {
            //This call will lock access to the heap the heaplet belongs to
            heaplet->startCompacting();
            heaplets.append(heaplet);
        }
    }

    unsigned numPagesEmptied()
    {
        unsigned numEmpty = 0;
        ForEachItemIn(i, heaplets)
        {
            ChunkedHeaplet * cur = heaplets.item(i);
            if (cur->isEmpty())
                numEmpty++;
            else if (heaplets.item(i)->moveRows)
                cur->isEmpty();
        }
        return numEmpty;
    }

    ChunkedHeaplet * queryHeaplet(unsigned i) { return heaplets.item(i); }

protected:
    bool processHeap(unsigned low, unsigned max);

protected:
    CriticalBlock lock;
    PointerArrayOf<ChunkedHeaplet> heaplets;
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
    void expandHeap(void * original, memsize_t copysize, memsize_t oldcapacity, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback);

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
        usageMap->noteHeapUsage(chunkSize, (RoxieHeapFlags)flags, numPages, chunkSize * numAllocs, 0);
    }

    const void * compactRow(const void * ptr, HeapCompactState & state);
    const void * newCompactRow(const void * ptr, NewHeapCompactState & state);

    inline unsigned maxChunksPerPage() const { return chunksPerPage; }

    //No longer any external references to a unique heap.  Mark so it can be cleaned up early.
    void noteOrphaned()
    {
        dbgassertex(flags & RHFunique);
        flags |= RHForphaned;
    }
    void checkScans(unsigned allocatorId);
    virtual void reportScanProblem(unsigned allocatorId, unsigned __int64 numScans, const HeapletStats & mergedStats) = 0;

    void gatherStats(CRuntimeStatisticCollection & stats);
    void setCompactTarget(unsigned target)
    {
        curCompactTarget = target;
    }

    void releaseAllRows();

protected:
    void * doAllocateRow(unsigned allocatorId, unsigned maxSpillCost);
    unsigned doAllocateRowBlock(unsigned allocatorId, unsigned maxSpillCost, unsigned max, char * * rows);

    virtual ChunkedHeaplet * allocateHeaplet() = 0;

protected:
    size32_t chunkSize;
    unsigned chunksPerPage;
    unsigned curCompactTarget = 0;
    unsigned __int64 totalAllocsLastScanCheck = 0;
};

class CFixedChunkedHeap : public CChunkedHeap
{
public:
    CFixedChunkedHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, size32_t _chunkSize, unsigned _flags, unsigned _defaultSpillCost)
        : CChunkedHeap(_rowManager, _logctx, _allocatorCache, _chunkSize, _flags), defaultSpillCost(_defaultSpillCost)
    {
    }

    void * allocate(unsigned allocatorId);
    unsigned allocateBlock(unsigned activityId, unsigned maxRows, char * * rows);

    virtual bool matches(size32_t searchSize, unsigned /*searchActivity*/, unsigned searchFlags) const
    {
        //Check the size matches, and any flags we are interested in.
        return (searchSize == chunkSize) &&
               (searchFlags == flags);
    }

    virtual void reportScanProblem(unsigned allocatorId, unsigned __int64 numScans, const HeapletStats & mergedStats) override;

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

    void * allocate(unsigned allocatorId);
    unsigned allocateBlock(unsigned activityId, unsigned maxRows, char * * rows);

    virtual bool matches(size32_t searchSize, unsigned searchActivity, unsigned searchFlags) const
    {
        return (searchSize == chunkSize) &&
               (searchFlags == flags) &&
               (allocatorId == searchActivity);
    }

    virtual void reportHeapUsage(IActivityMemoryUsageMap * usageMap, unsigned numPages, memsize_t numAllocs) const
    {
        usageMap->noteHeapUsage(chunkSize, (RoxieHeapFlags)flags, numPages, chunkSize * numAllocs, allocatorId);
    }

    virtual void reportScanProblem(unsigned allocatorId, unsigned __int64 numScans, const HeapletStats & mergedStats) override;

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

void Heaplet::addToSpaceList()
{
    if (nextSpace.load(std::memory_order_relaxed) != 0)
        return;
    heap->addToSpaceList(this);
}

void Heaplet::verifySpaceList()
{
    if (nextSpace.load(std::memory_order_relaxed) == 0)
    {
        IERRLOG("%p@%" I64F "u: Verify failed: %p %u", heap, (unsigned __int64)GetCurrentThreadId(), this, isFull());
    }
}

void ChunkedHeaplet::verifySpaceList()
{
    if (nextSpace.load(std::memory_order_relaxed) == 0)
    {
        IERRLOG("%p@%" I64F "u: Verify failed: %p %u %x %x", heap, (unsigned __int64)GetCurrentThreadId(), this, isFull(), freeBase.load(), r_blocks.load());
    }
}

//This function must be called inside a protecting lock on the heap it belongs to, or after precreateFreeChain() has been called.
char * ChunkedHeaplet::allocateSingle(unsigned allocated, bool incCounter, unsigned & alreadyIncremented)
{
    //The spin lock for the heap this chunk belongs to must be held when this function is called
    char *ret;
    const size32_t size = chunkSize;

    if (heapFlags & RHFscanning)
    {
        unsigned numAllocs = count.load(std::memory_order_acquire)+allocated-1;
        unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);

        //If more than half full allocate an entry from the expanding free area
        if (numAllocs * size  * 2 >= curFreeBase)
        {
            //There is no ABA issue on freeBase because it is never decremented (and no next chain with it)
            size32_t bytesFree = dataAreaSize() - curFreeBase;
            if (bytesFree >= size)
            {
                //This is the only place that modifies freeBase, so it can be unconditional since caller must have a lock.
                ret = data() + curFreeBase;
                ((std::atomic_uint *)ret)->store(0, std::memory_order_relaxed);
                freeBase.store(curFreeBase + size, std::memory_order_release); // ensure link count is written before this is incremented.
                goto done;
            }

            CChunkedHeap * chunkHeap = static_cast<CChunkedHeap *>(heap);
            unsigned maxAllocs = chunkHeap->maxChunksPerPage();
            if (numAllocs == maxAllocs)
            {
                if (!(heapFlags & RHFdelayrelease))
                    return nullptr;
                //If no row has ever been freed then it must really be full - optimize the situation filling a buffer
                if (!hadDelayedRelease.load(std::memory_order_relaxed))
                    return nullptr;
            }
        }

        {
            //Scan through all the memory, checking for a block marked as free - should terminate very quickly unless highly fragmented
            const size_t startOffset = nextMatchOffset;
            size32_t offset = startOffset;
            for (;;)
            {
                ret = data() + offset;
                offset += size;
                if (offset == curFreeBase)
                    offset = 0;
                __builtin_prefetch(data() + offset);

                unsigned tag = ((std::atomic_uint *)ret)->load(std::memory_order_relaxed);
                if (tag >= FREE_ROW_COUNT_MIN)
                {
                    if (tag == DELAYFREE_ROW_COUNT)
                        alreadyIncremented++;
                    break;
                }

                if (offset == startOffset)
                {
                    assertex(heapFlags & RHFdelayrelease);
                    heap->updateDistanceScanned(curFreeBase);
                    return nullptr;
                }
            }

            // either (offset - startOffset) or (curFreeBase - startOffset) + offset.  Simplified to...
            unsigned thisDistance = ((offset > startOffset) ? 0 : curFreeBase) + (offset - startOffset) - size;
            heap->updateDistanceScanned(thisDistance);
            nextMatchOffset = offset;
            //save offset
        }

        //Mark as allocated before return - while spin lock is still guaranteed to be active
        ((std::atomic_uint *)ret)->store(0, std::memory_order_relaxed);
    }
    else
    {
#ifdef HAS_EFFICIENT_CAS
        unsigned old_blocks = r_blocks.load(std::memory_order_acquire); // acquire ensures that *(unsigned *)ret is up to date
#endif
        for (;;)
        {
#ifndef HAS_EFFICIENT_CAS
            unsigned old_blocks = r_blocks.load(std::memory_order_acquire); // acquire ensures that *(unsigned *)ret is up to date
#endif

            unsigned r_ret = (old_blocks & RBLOCKS_OFFSET_MASK);
            if (r_ret)
            {
                ret = makeAbsolute(r_ret);
                //may have been allocated by another thread, but still legal to dereference
                //the cas will fail if the contents are invalid.  May be flagged as a benign race.
                unsigned next = *(unsigned *)ret;

                //There is a potential ABA problem if other thread(s) allocate two or more items, and free the first
                //item in the window before the following cas.  r_block would match, but next would be invalid.
                //To avoid that a tag is stored in the top bits of r_blocks which is modified whenever an item is added
                //onto the free list.  The offsets in the freelist do not need tags.
                unsigned new_blocks = (old_blocks & RBLOCKS_CAS_TAG_MASK) | next;
                if (compare_exchange_efficient(r_blocks, old_blocks, new_blocks, std::memory_order_acquire, std::memory_order_acquire))
                    break;

                //NOTE: Currently I think a lock is always held before allocating from a chunk, so I'm not sure there is an ABA problem!
            }
            else
            {
                unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);
                //There is no ABA issue on freeBase because it is never decremented (and no next chain with it)
                size32_t bytesFree = dataAreaSize() - curFreeBase;
                if (bytesFree < size)
                    return NULL;

                //This is the only place that modifies freeBase, so it can be unconditional since caller must have a lock.
                freeBase.store(curFreeBase + size, std::memory_order_relaxed);
                ret = data() + curFreeBase;
                break;
            }
        }
    }

done:
    if (incCounter && !alreadyIncremented)
    {
        count.fetch_add(1, std::memory_order_relaxed);
        heap->updateNumAllocs(1);
    }

    return ret;
}

//This function must be called inside a protecting lock on the heap it belongs to, or after precreateFreeChain() has been called.
char * ChunkedHeaplet::allocateChunk()
{
    unsigned alreadyIncremented = 0;
    return allocateSingle(0, true, alreadyIncremented);
}

unsigned ChunkedHeaplet::allocateMultiChunk(unsigned max, char * * rows)
{
    //The spin lock for the heap this chunk belongs to must be held when this function is called
    unsigned allocated = 0;
    unsigned alreadyIncremented = 0;
    do
    {
        bool needIncrement = false;
        char * ret = allocateSingle(allocated, false, alreadyIncremented);
        if (!ret)
        {
            if (allocated == 0)
                return 0;
            break;
        }
        rows[allocated++] = ret;
    } while (allocated < max);

    heap->updateNumAllocs(allocated);
    if (allocated != alreadyIncremented)
        count.fetch_add(allocated-alreadyIncremented, std::memory_order_relaxed);

    return allocated;
}

bool ChunkedHeaplet::isFull() const
{
    CChunkedHeap * chunkHeap = static_cast<CChunkedHeap *>(heap);
    unsigned numAllocs = count.load(std::memory_order_acquire)-1;
    unsigned maxAllocs = chunkHeap->maxChunksPerPage();
    if (numAllocs != maxAllocs)
        return false;
    if (heapFlags & RHFdelayrelease)
    {
        if (!hadDelayedRelease.load(std::memory_order_relaxed))
            return true;

        //Scan through all the memory, checking for a block marked as free - should terminate very quickly unless highly fragmented
        size32_t offset = nextMatchOffset;
        const size32_t size = chunkSize;
        const unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);
        do
        {
            char * ret = data() + offset;
            unsigned tag = ((std::atomic_uint *)ret)->load(std::memory_order_relaxed);
            dbgassertex(tag != FREE_ROW_COUNT);
            if (tag == DELAYFREE_ROW_COUNT)
            {
                /*
                ((std::atomic_uint *)ret)->store(FREE_ROW_COUNT, std::memory_order_relaxed);
                //This heap must be locked to be calling this function
                if (count.fetch_sub(1, std::memory_order_release) == 2)
                {
                    //This thread does not access anything else from the object, so no need for an acquire fence!
                    //I'm not sure this will accomplish anything useful!
                    heap->noteEmptyPage();
                }*/

                return false;
            }
            offset += size;
            if (offset == curFreeBase)
                offset = 0;
        } while (offset != nextMatchOffset);
    }
    return true;
}

bool ChunkedHeaplet::isEmpty()
{
    unsigned numAllocs = count.load(std::memory_order_acquire)-1;
    if (numAllocs == 0)
        return true;

    if (!(heapFlags & RHFdelayrelease))
        return false;

    //Scan through all the memory, checking for a block marked as free - should terminate very quickly unless highly fragmented
    size32_t offset = 0;
    const size32_t size = chunkSize;
    const unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);
    do
    {
        char * ret = data() + offset;
        unsigned tag = ((std::atomic_uint *)ret)->load(std::memory_order_relaxed);
        if (tag < FREE_ROW_COUNT_MIN)
            return false;
        offset += size;
    } while (offset < curFreeBase);

    return true;
}

unsigned ChunkedHeaplet::calcNumAllocated(bool updateCount) const
{
    unsigned numAllocs = count.load(std::memory_order_acquire)-1;
    if (heapFlags & RHFdelayrelease)
    {
        if (numAllocs == 0)
            return 0;

        //Scan through all the memory, checking for a block marked as free - should terminate very quickly unless highly fragmented
        size32_t offset = 0;
        const size32_t size = chunkSize;
        const unsigned curFreeBase = freeBase.load(std::memory_order_relaxed);
        unsigned numDelayed = 0;
        do
        {
            char * ret = data() + offset;
            unsigned tag = ((std::atomic_uint *)ret)->load(std::memory_order_relaxed);
            if (tag == DELAYFREE_ROW_COUNT)
            {
                numDelayed++;
                if (updateCount)
                    ((std::atomic_uint *)ret)->store(FREE_ROW_COUNT, std::memory_order_relaxed);
            }
            offset += size;
        } while (offset < curFreeBase);

        if (numDelayed)
        {
            if (updateCount)
            {
                if (count.fetch_sub(numDelayed, std::memory_order_release) == numDelayed+1)
                {
                    //This thread does not access anything else from the object, so no need for an acquire fence!
                    //I'm not sure this will accomplish anything useful!
                    heap->noteEmptyPage();
                }
            }
            return numAllocs - numDelayed;
        }
    }
    return numAllocs;
}


const void * ChunkedHeaplet::_compactRow(const void * ptr, HeapCompactState & state)
{
    //NB: If this already belongs to a full heaplet then leave it where it is..
    CChunkedHeap * chunkedHeap = static_cast<CChunkedHeap *>(heap);
    if (isFull())
        return ptr;
    return chunkedHeap->compactRow(ptr, state);
}

void ChunkedHeaplet::startCompacting()
{
    heap->internalLock(); // prevent any allocations while this operation is going on.
    compacting = true;
    compactingRowCount = calcNumAllocated(false); // Don't update the counts because it will make allocating rows less efficient.
    moveRows = false;
    hasSharedRow = false;
}
void ChunkedHeaplet::stopCompacting()
{
    compacting = false;
    heap->internalUnlock();
}

void ChunkedHeaplet::_prepareToCompactRow(const void * ptr, NewHeapCompactState & state)
{
    state.noteHeaplet(this);
    if (_isShared(ptr))
        hasSharedRow = true;
}

const void * ChunkedHeaplet::_newCompactRow(const void * ptr, NewHeapCompactState & state)
{
    assertex(compacting);
    if (!moveRows)
        return ptr;

    CChunkedHeap * chunkedHeap = static_cast<CChunkedHeap *>(heap);
    return chunkedHeap->newCompactRow(ptr, state);
}

//================================================================================

//Allow private virtual functions to be added to the abstract row manager:
class CRowManager : implements IRowManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    virtual bool releaseEmptyPages(unsigned slaveId, bool forceFreeAll) = 0; // ensures any empty pages are freed back to the heap
    virtual unsigned querySlaveId() const = 0;
};


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
            args.slaveId = 0;
            args.result = false;
        }

        virtual int run()
        {
            for (;;)
            {
                goSem.wait();
                if (abort)
                    break;
                //This class is only used for stress testing => free all we can.
                args.result = owner.releaseBuffersNow(args.slaveId, SpillAllCost, args.critical, false, 0);
                doneSem.signal();
            }
            return 0;
        }

        bool releaseBuffers(unsigned slaveId, unsigned maxSpillCost, const bool critical)
        {
            if (isCurrentThread())
                return owner.releaseBuffersNow(slaveId, maxSpillCost, critical, false, 0);

            bool ok;
            {
                CriticalBlock block(cs);
                args.critical = critical;
                args.slaveId = slaveId;

                goSem.signal();
                doneSem.wait();
                ok = args.result;
            }
            return ok;
        }

        void stop()
        {
            //This should not be called while any active heap operations could be occurring
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
            unsigned slaveId;
            bool critical;
            bool result;
        } args;
        bool abort;
    };

    class CallbackItem : public CInterface
    {
        typedef std::pair<unsigned, IBufferedRowCallback *> CallbackPair;
        typedef StructArrayOf<CallbackPair> CallbackPairArray;

    public:
        CallbackItem(unsigned _cost, unsigned _activityId) : cost(_cost), activityId(_activityId), nextCallback(0)
        {
        }
        void addCallback(unsigned slaveId, IBufferedRowCallback * callback)
        {
            callbacks.append(CallbackPair(slaveId, callback));
        }
        inline bool isEmpty() const
        {
            return callbacks.empty();
        }
        inline bool matches(unsigned searchCost, unsigned searchActivityId) const
        {
            return (cost == searchCost) && (activityId == searchActivityId);
        }
        void removeCallback(unsigned searchSlaveId, IBufferedRowCallback * searchCallback)
        {
            ForEachItemIn(i, callbacks)
            {
                const CallbackPair * iter = &callbacks.item(i);
                if ((iter->first == searchSlaveId) && (iter->second == searchCallback))
                {
                    callbacks.remove(i);
                    return;
                }
            }
            //Silently ignore removing a callback that hasn't been registered
        }
        unsigned releaseRows(BufferedRowCallbackManager & manager, unsigned whichSlave, unsigned minSuccess, bool critical)
        {
            unsigned max = callbacks.ordinality();
            unsigned cur = 0;
            //MORE: Should nextCallback be tracked per slave?  There are arguments for and against.
            if (nextCallback < max)
                cur += nextCallback;

            unsigned numSuccess = 0;
            for (unsigned i = 0; i < max; i++)
            {
                const CallbackPair * iter = &callbacks.item(cur);
                const unsigned curSlave = iter->first;
                if ((whichSlave == 0) || (whichSlave == curSlave) || (curSlave == 0))
                {
                    if (manager.callbackReleasesRows(iter->second, critical))
                        numSuccess++;
                }

                ++cur;
                if (cur == max)
                    cur = 0;

                if (numSuccess >= minSuccess)
                    break;
            }
            nextCallback = cur;
            return numSuccess;
        }

        void report(const IContextLogger &logctx) const
        {
            if (callbacks.ordinality())
            {
                StringBuffer msg;
                msg.appendf(" ac(%u) cost(%u):", activityId, cost);
                ForEachItemIn(i, callbacks)
                {
                    if (i == nextCallback)
                        msg.append(" {").append(callbacks.item(i).first).append("}");
                    else
                        msg.append(" ").append(callbacks.item(i).first);
                }
                logctx.CTXLOG("%s", msg.str());
            }
        }
        inline unsigned getSpillCost() const { return cost; }
        inline unsigned getActivityId() const { return activityId; }

    protected:
        unsigned cost;
        unsigned activityId;
        unsigned nextCallback;
        CallbackPairArray callbacks;
    };


public:
    BufferedRowCallbackManager(CRowManager * _owner) : owner(_owner), releasingBuffers(false), releaseSeq(0)
    {
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
    inline unsigned getReleaseSeq() const { return releaseSeq.load(std::memory_order_acquire); }

    void addRowBuffer(unsigned slaveId, IBufferedRowCallback * callback)
    {
        if (releaseWhenModifyCallback)
            releaseBuffers(slaveId, SpillAllCost, releaseWhenModifyCallbackCritical, false, 0);

        unsigned cost = callback->getSpillCost();
        unsigned activityId = callback->getActivityId();
        unsigned insertPos = 0;

        CriticalBlock block(callbackCrit);
        //Assuming a small number so perform an insertion sort (ordered by cost, activityId)
        //Switch to std::map if that is assumption is no longer valid
        unsigned max = rowBufferCallbacks.ordinality();
        for (; insertPos < max; insertPos++)
        {
            CallbackItem & curCallback = rowBufferCallbacks.item(insertPos);
            unsigned curCost = curCallback.getSpillCost();
            if (curCost > cost)
                break;
            if (curCost == cost)
            {
                unsigned curId = curCallback.getActivityId();
                if (curId > activityId)
                    break;
                if (curId == activityId)
                {
                    curCallback.addCallback(slaveId, callback);
                    return;
                }
            }
        }

        CallbackItem * newCallback = new CallbackItem(cost, activityId);
        rowBufferCallbacks.add(*newCallback, insertPos);
        newCallback->addCallback(slaveId, callback);
    }

    void removeRowBuffer(unsigned slaveId, IBufferedRowCallback * callback)
    {
        if (releaseWhenModifyCallback)
            releaseBuffers(slaveId, SpillAllCost, releaseWhenModifyCallbackCritical, false, 0);

        unsigned cost = callback->getSpillCost();
        unsigned activityId = callback->getActivityId();
        CriticalBlock block(callbackCrit);
        ForEachItemIn(i, rowBufferCallbacks)
        {
            CallbackItem & curCallback = rowBufferCallbacks.item(i);
            if (curCallback.matches(cost, activityId))
            {
                curCallback.removeCallback(slaveId, callback);
                //Do not remove entry (i) from rowBufferCallbacks if curCallback.isEmpty() to avoid inefficiency adding and removing a callback for example in a child query
                break;
            }
        }
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
    bool releaseBuffers(unsigned slaveId, unsigned maxSpillCost, const bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        if (!releaseBuffersThread)
            return releaseBuffersNow(slaveId, maxSpillCost, critical, checkSequence, prevReleaseSeq);
        return releaseBuffersThread->releaseBuffers(slaveId, maxSpillCost, critical);
    }

    void reportActive(const IContextLogger &logctx) const
    {
        logctx.CTXLOG("--Active callbacks--");
        CriticalBlock block(callbackCrit);
        ForEachItemIn(i, rowBufferCallbacks)
            rowBufferCallbacks.item(i).report(logctx);
    }

    void runReleaseBufferThread()
    {
        for (;;)
        {
            releaseBuffersSem.wait();
            if (abortBufferThread)
                break;
            releaseBuffersNow(0, backgroundReleaseCost, false, false, 0);
            releasingBuffers.store(false);
        }
    }

    void releaseBuffersInBackground()
    {
        bool expected = false;
        if (releasingBuffers.compare_exchange_strong(expected, true))
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
    bool doReleaseBuffers(unsigned slaveId, unsigned maxSpillCost, const bool critical, const unsigned minSuccess)
    {
        const unsigned numCallbacks = rowBufferCallbacks.ordinality();
        if (numCallbacks == 0)
            return false;

        unsigned numSuccess = 0;
        ForEachItemIn(i, rowBufferCallbacks)
        {
            CallbackItem & cur = rowBufferCallbacks.item(i);
            if (cur.getSpillCost() > maxSpillCost)
                break;

            unsigned numReleased = cur.releaseRows(*this, slaveId, minSuccess-numSuccess, critical);
            numSuccess += numReleased;
            if (numSuccess >= minSuccess)
                return true;
        }
        return (numSuccess != 0);
    }

    //Release buffers will ensure that the rows are attempted to be cleaned up before returning
    bool releaseBuffersNow(unsigned slaveId, unsigned maxSpillCost, const bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        const unsigned minSuccess = minCallbackThreshold;
        CriticalBlock block(callbackCrit);
        //While we were waiting check if something freed some memory up
        //if so try again otherwise we might end up calling more callbacks than we need to.
        if (checkSequence && (prevReleaseSeq != getReleaseSeq()))
            return true;

        //Call non critical first, then critical - if applicable.
        //Should there be more levels of importance than critical/non critical?
        if (doReleaseBuffers(slaveId, maxSpillCost, false, minSuccess) || (critical && doReleaseBuffers(slaveId, maxSpillCost, true, minSuccess)))
        {
            //Increment first so that any called knows some rows may have been freed
            releaseSeq.fetch_add(1, std::memory_order_release);
            owner->releaseEmptyPages(slaveId, critical);
            //incremented again because some rows may now have been freed.  A difference may give a
            //false positive, but better than a false negative.
            releaseSeq.fetch_add(1, std::memory_order_release); // release ensure that other threads see this updated after the pages are freed.
            return true;
        }
        else if (owner->releaseEmptyPages(slaveId, critical))
        {
            releaseSeq.fetch_add(1, std::memory_order_release);
            return true;
        }
        return false;
    }

protected:
    mutable CriticalSection callbackCrit;
    Semaphore releaseBuffersSem;
    CIArrayOf<CallbackItem> rowBufferCallbacks;
    PointerArrayOf<IBufferedRowCallback> activeCallbacks;
    Owned<BackgroundReleaseBufferThread> backgroundReleaseBuffersThread;
    Owned<ReleaseBufferThread> releaseBuffersThread;
    CRowManager * owner;
    std::atomic_bool releasingBuffers;  // if pre-emptive releasing thread is active.
    std::atomic_uint releaseSeq;
    unsigned minCallbackThreshold;
    unsigned backgroundReleaseCost;
    bool releaseWhenModifyCallback;
    bool releaseWhenModifyCallbackCritical;
    std::atomic_bool abortBufferThread;
};

//Constants are here to ensure they can all be constant folded
const unsigned roundupDoubleLimit = MAX_SIZE_DIRECT_BUCKET;  // Values up to this limit are directly mapped to a bucket size
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


//---------------------------------------------------------------------------------------------------------------------

static unsigned numDirectBuckets;
//This array contains details of the actual sizes that are used to allocate an item of size bytes.
//The entry allocSize((size-1)/ALLOC_ALIGNMENT is a value that indicates which heap and the size of that heap
//NOTE: using "size-1" ensures that values X*ALLOC_ALIGNMENT-(ALLOC_ALIGNMENT-1)..X*ALLOC_ALIGNMENT are mapped to the same bin
static unsigned allocSizeMapping[MAX_SIZE_DIRECT_BUCKET/ALLOC_ALIGNMENT+1];

const static unsigned defaultAllocSizes[] = { 16, 32, 64, 128, 256, 512, 1024, 2048, 0 };

void initAllocSizeMappings(const unsigned * sizes)
{
    size32_t bucketSize = sizes[0];
    unsigned bucket = 0;
    for (unsigned size=ALLOC_ALIGNMENT; size <= MAX_SIZE_DIRECT_BUCKET; size += ALLOC_ALIGNMENT)
    {
        if (size > bucketSize)
        {
            bucket++;
            dbgassertex(bucketSize < sizes[bucket]);
            bucketSize = sizes[bucket];
        }
        allocSizeMapping[(size-1)/ALLOC_ALIGNMENT] = ROUNDED(bucket, bucketSize);
    }
    assertex(sizes[bucket+1] == 0);
    numDirectBuckets = bucket+1;
}


//---------------------------------------------------------------------------------------------------------------------

class CChunkingRowManager : public CRowManager
{
    friend class CRoxieFixedRowHeap;
    friend class CRoxieVariableRowHeap;
    friend class CHugeHeap;
    friend class CChunkedHeap;
    friend class CFixedChunkedHeap;
private:
    CriticalSection activeBufferCS; // Potentially slow
    mutable NonReentrantSpinLock peakSpinLock; // Very small window, low contention so fine to be a spin lock
    mutable SpinLock fixedSpinLock; // Main potential for contention releasingEmptyHeaps and gathering peak usage.  Shouldn't be likely.
                                    // Should possibly be a ReadWriteLock - better with high contention, worse with low
    CIArrayOf<CFixedChunkedHeap> normalHeaps;
    CHugeHeap hugeHeap;
    ITimeLimiter *timeLimit;
    DataBuffer *activeBuffs;
    unsigned peakPages;
    unsigned dataBuffs;
    unsigned dataBuffPages;
    std::atomic_uint possibleGoers = {0};
    std::atomic_uint totalHeapPages = {0};
    Owned<IActivityMemoryUsageMap> peakUsageMap;
    CIArrayOf<CHeap> fixedHeaps;
    CICopyArrayOf<CRoxieFixedRowHeapBase> fixedRowHeaps;  // These are observed, NOT linked
    unsigned __int64 cyclesChecked;       // When we last checked timelimit
    unsigned __int64 cyclesCheckInterval; // How often we need to check timelimit
    bool outputOOMReports;
    bool trackMemoryByActivity;
    bool minimizeFootprint;
    bool minimizeFootprintCritical;

protected:
    const IRowAllocatorCache *allocatorCache;
    const IContextLogger &logctx;
    unsigned maxPageLimit;
    unsigned spillPageLimit;

    inline unsigned getActivityId(unsigned rawId) const
    {
        return getRealActivityId(rawId, allocatorCache);
    }

public:
    CChunkingRowManager(memsize_t _memLimit, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, bool _outputOOMReports)
        : hugeHeap(this, _logctx, _allocatorCache), logctx(_logctx), allocatorCache(_allocatorCache)
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
            unsigned flags = RHFvariable;
#ifdef ALWAYS_USE_SCAN_HEAP
            flags |= RHFscanning;
#ifdef ALWAYS_DELAY_RELEASE
            flags |= RHFdelayrelease;
#endif
#endif
            normalHeaps.append(*new CFixedChunkedHeap(this, _logctx, _allocatorCache, thisSize, flags, SpillAllCost));
            prevSize = thisSize;
        }
        maxPageLimit = (unsigned) PAGES(_memLimit, HEAP_ALIGNMENT_SIZE);
        if (maxPageLimit == 0)
            maxPageLimit = UNLIMITED_PAGES;
        spillPageLimit = 0;
        timeLimit = _tl;
        peakPages = 0;
        dataBuffs = 0;
        activeBuffs = NULL;
        dataBuffPages = 0;
        outputOOMReports = _outputOOMReports;
        minimizeFootprint = false;
        minimizeFootprintCritical = false;
#ifdef _DEBUG
        trackMemoryByActivity = true; 
#else
        trackMemoryByActivity = false;
#endif
        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager c-tor memLimit=%" I64F "u pageLimit=%u rowMgr=%p num=%u",
                          (unsigned __int64)_memLimit, maxPageLimit, this, activeRowManagers.load());
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
        activeRowManagers++;
    }

    ~CChunkingRowManager()
    {
        activeRowManagers--;
        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor pageLimit=%u peakPages=%u dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p num=%u",
                    maxPageLimit, peakPages, dataBuffs, dataBuffPages, possibleGoers.load(), this, activeRowManagers.load());

        //Ensure that the rowHeaps release any references to the fixed heaps, and no longer call back when they
        //are destroyed
        {
            SpinBlock block(fixedSpinLock);
            ForEachItemIn(i, fixedRowHeaps)
                fixedRowHeaps.item(i).clearRowManager();
        }

        DataBuffer *dfinger = activeBuffs;
        while (dfinger)
        {
            DataBuffer *next = dfinger->next;
            if (memTraceLevel >= 2 && dfinger->queryCount()!=1)
                logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active dataBuffer list - addr=%p rowMgr=%p", 
                        dfinger->queryCount()-1, dfinger, this);
            dfinger->next = NULL;
            dfinger->mgr = NULL; // Avoid calling back to noteDataBufferReleased, which would be unhelpful
            dfinger->count.store(0, std::memory_order_relaxed);
            dfinger->released();
            dfinger = next;
        }
        logctx.Release();
    }

    inline void doOomReport()
    {
        if (outputOOMReports)
        {
            reportMemoryUsage(false);
            PrintStackReport();
        }
    }

    virtual void reportLeaks()
    {
        reportLeaks(1);
    }

    virtual void checkHeap()
    {
        ForEachItemIn(iNormal, normalHeaps)
            normalHeaps.item(iNormal).checkHeap();

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occurring
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

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occurring
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & fixedHeap = fixedHeaps.item(i);
            total += fixedHeap.allocated();
        }

        return total;
    }

    virtual unsigned numPagesAfterCleanup(bool forceFreeAll)
    {
        releaseEmptyPages(0, forceFreeAll);
        return dataBuffPages + totalHeapPages.load(std::memory_order_relaxed);
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
    virtual bool releaseEmptyPages(unsigned slaveId, bool forceFreeAll)
    {
        unsigned total = 0;
        ForEachItemIn(iNormal, normalHeaps)
            total += normalHeaps.item(iNormal).releaseEmptyPages(forceFreeAll);
        total += hugeHeap.releaseEmptyPages(true);

        bool hadUnusedHeap = false;
        {
            SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occurring
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
            totalHeapPages.fetch_sub(total, std::memory_order_release);
        return (total != 0);
    }

    void getPeakActivityUsage()
    {
        Owned<IActivityMemoryUsageMap> map = getActivityUsage();

        NonReentrantSpinBlock block(peakSpinLock);
        peakUsageMap.swap(map);
    }

    //MORE: inline??
    static size32_t roundup(size32_t size)
    {
        dbgassertex((size >= FixedSizeHeaplet::chunkHeaderSize) && (size <= FixedSizeHeaplet::maxHeapSize() + FixedSizeHeaplet::chunkHeaderSize));
        //MORE: A binary chop on sizes is likely to be better.
        if (size <= MAX_SIZE_DIRECT_BUCKET)
            return allocSizeMapping[(size-1)/ALLOC_ALIGNMENT];

        if (size <= maxStepSize)
        {
            size32_t blocks = PAGES(size, roundupStepSize);
            return ROUNDED((numDirectBuckets-1) + blocks, blocks * roundupStepSize);
        }

        unsigned baseBlock = (numDirectBuckets-1);
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
        //MORE: We could avoid calling beforeAllocate() at all if neither of these flags are set at heap creation time.
        if (unlikely(memTraceSizeLimit))
            checkTraceAllocationSize(_size, activityId);
        if (unlikely(timeLimit))
            checkTimeLimit();
    }

    virtual void *allocate(memsize_t _size, unsigned activityId)
    {
        try
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
        catch (IException *e)
        {
            EXCLOG(e, "CChunkingRowManager::allocate(memsize_t _size, unsigned activityId)");
            doOomReport();
            throw;
        }
    }

    virtual void *allocate(memsize_t _size, unsigned activityId, unsigned maxSpillCost)
    {
        try
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
        catch (IException *e)
        {
            EXCLOG(e, "CChunkingRowManager::allocate(memsize_t _size, unsigned activityId, unsigned maxSpillCost)");
            if (maxSpillCost == SpillAllCost)
                doOomReport();
            throw;
        }
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
        maxPageLimit = (unsigned) PAGES(bytes, HEAP_ALIGNMENT_SIZE);
        if (maxPageLimit == 0)
            maxPageLimit = UNLIMITED_PAGES;
        spillPageLimit = (unsigned) PAGES(spillSize, HEAP_ALIGNMENT_SIZE);

        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::setMemoryLimit new memlimit=%" I64F "u pageLimit=%u spillLimit=%u rowMgr=%p", (unsigned __int64) bytes, maxPageLimit, spillPageLimit, this);
    }

    virtual void setMinimizeFootprint(bool value, bool critical)
    {
        minimizeFootprint = value;
        minimizeFootprintCritical = critical;
    }

    virtual void resizeRow(memsize_t &capacity, void * & ptr, memsize_t copysize, memsize_t newsize, unsigned activityId)
    {
        void * const original = ptr;
        assertex(newsize);
        assertex(!HeapletBase::isShared(original));
        memsize_t curCapacity = HeapletBase::capacity(original);
        if (newsize <= curCapacity)
        {
            //resizeRow never shrinks memory, except if we can free some extra huge memory blocks.
            //If current allocation is >1 block then try resizing
            if (curCapacity <= FixedSizeHeaplet::maxHeapSize())
            {
                capacity = curCapacity;
                return;
            }
        }
        if (curCapacity > FixedSizeHeaplet::maxHeapSize())
        {
            CVariableRowResizeCallback callback(capacity, ptr);
            try
            {
                hugeHeap.expandHeap(original, copysize, curCapacity, newsize, activityId, SpillAllCost, callback);
            }
            catch (IException *e)
            {
                EXCLOG(e, "CChunkingRowManager::resizeRow(memsize_t &capacity, void * & ptr, memsize_t copysize, memsize_t newsize, unsigned activityId)");
                doOomReport();
                throw;
            }
            return;
        }

        void *ret = allocate(newsize, activityId);
        memcpy(ret, original, copysize);
        memsize_t newCapacity = HeapletBase::capacity(ret);
        HeapletBase::release(original);
        capacity = newCapacity;
        ptr = ret;
    }

    virtual void resizeRow(void * original, memsize_t copysize, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback)
    {
        assertex(newsize);
        assertex(!HeapletBase::isShared(original));
        memsize_t curCapacity = HeapletBase::capacity(original);
        if (newsize <= curCapacity)
        {
            //resizeRow never shrinks memory, except if we can free some extra huge memory blocks.
            //If current allocation is >1 block then try resizing
            if (curCapacity <= FixedSizeHeaplet::maxHeapSize())
                return;
        }
        if (curCapacity > FixedSizeHeaplet::maxHeapSize())
        {
            try
            {
                hugeHeap.expandHeap(original, copysize, curCapacity, newsize, activityId, maxSpillCost, callback);
                return;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CChunkingRowManager::resizeRow(void * original, memsize_t copysize, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback)");
                if (maxSpillCost == SpillAllCost)
                    doOomReport();
                throw;
            }
        }

        void *ret = allocate(newsize, activityId, maxSpillCost);
        memsize_t newCapacity = HeapletBase::capacity(ret);

        //Copying data must lock for the duration (otherwise another thread modifying the data may leave it out of sync)
        callback.lock();

        memcpy(ret, original, copysize);

        //previously locked => update the pointer and then unlock
        callback.update(newCapacity, ret);
        callback.unlock();

        HeapletBase::release(original);
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

    virtual memsize_t getMemoryUsage()
    {
        Owned<IActivityMemoryUsageMap> map;
        {
            NonReentrantSpinBlock block(peakSpinLock);
            map.set(peakUsageMap);
        }
        if (map)
            map->report(logctx, allocatorCache);
        return ((memsize_t)peakPages)*HEAP_ALIGNMENT_SIZE;
    }

    virtual bool attachDataBuff(DataBuffer *dataBuff) 
    {
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: attachDataBuff() attaching DataBuff to rowMgr - addr=%p dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p", 
                    dataBuff, dataBuffs, dataBuffPages, possibleGoers.load(), this);

        dataBuff->Link();
        DataBuffer *last = NULL;
        bool needCheck;
        {
            CriticalBlock b(activeBufferCS);
            DataBuffer *finger = activeBuffs;
            while (finger && possibleGoers.load(std::memory_order_relaxed))
            {
                // MORE - if we get a load of data in and none out this can start to bog down...
                DataBuffer *next = finger->next;
                if (finger->queryCount()==1)
                {
                    if (memTraceLevel >= 4)
                        logctx.CTXLOG("RoxieMemMgr: attachDataBuff() detaching DataBuffer linked in active list - addr=%p rowMgr=%p",
                                finger, this);
                    finger->next = NULL;
                    finger->Release();
                    dataBuffs--;
                    possibleGoers.fetch_sub(1, std::memory_order_relaxed); // It doesn't matter when other threads see this update
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
        possibleGoers.fetch_add(1, std::memory_order_release);
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::noteDataBuffReleased dataBuffs=%u dataBuffPages=%u possibleGoers=%u dataBuff=%p rowMgr=%p", 
                    dataBuffs, dataBuffPages, possibleGoers.load(), dataBuff, this);
    }

    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags)
    {
#ifdef ALWAYS_USE_SCAN_HEAP
        roxieHeapFlags |= RHFscanning;
#ifdef ALWAYS_DELAY_RELEASE
        roxieHeapFlags |= RHFdelayrelease;
#endif
#endif
        unsigned defaultSpillCost = SpillAllCost; //i.e. if the allocation must fails then abort the query
        CRoxieFixedRowHeapBase * rowHeap = doCreateFixedRowHeap(fixedSize, activityId, roxieHeapFlags, defaultSpillCost);

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

    virtual unsigned checkLimit(unsigned numRequested, unsigned maxSpillCost)
    {
        unsigned totalPages;
        releaseEmptyPages(querySlaveId(), false);
        if (minimizeFootprint)
            releaseCallbackMemory(SpillAllCost, minimizeFootprintCritical, false, 0);

        for (;;)
        {
            unsigned lastReleaseSeq = getReleaseSeq();
            //We need to ensure that the number of allocated pages is updated atomically so multiple threads can't all
            //succeed and have the total take them over the limit.
            unsigned numHeapPages = totalHeapPages.load(std::memory_order_acquire); //I suspect relaxed would be sufficient, but not *that* time critical.
            unsigned pageCount = dataBuffPages + numHeapPages;
            totalPages = pageCount + numRequested;
            unsigned pageLimit = getPageLimit();
            if (totalPages <= pageLimit)
            {
                if (pageLimit != UNLIMITED_PAGES)
                {
                    //Use compare_exchange so that only one thread can increase the number of pages at a time.
                    //(Don't use atomic_add because we need to check the limit hasn't been exceeded.)
                    if (!totalHeapPages.compare_exchange_weak(numHeapPages, numHeapPages + numRequested, std::memory_order_relaxed))
                        continue;
                }
                else
                {
                    //Unlimited pages => just increment the total
                    totalHeapPages.fetch_add(numRequested, std::memory_order_relaxed);
                }
                break;
            }

            if (releaseEmptyPages(querySlaveId(), true))
                continue;

            //Try and directly free up some buffers.  It is worth trying again if one of the release functions thinks it
            //freed up some memory.
            //The following reduces the number of times the callback is called, but I'm not sure how this affects
            //performance.  I think better if a single free is likely to free up some memory, and worse if not.
            const bool skipReleaseIfAnotherThreadReleases = true;
            if (!releaseCallbackMemory(maxSpillCost, true, skipReleaseIfAnotherThreadReleases, lastReleaseSeq))
            {
                //Check if a background thread has freed up some memory.  That can be checked by a comparing value of a counter
                //which is incremented each time releaseBuffers is successful.
                if (lastReleaseSeq == getReleaseSeq())
                {
                    //very unusual: another thread may have just released a lot of memory (e.g., it has finished), but
                    //the empty pages haven't been cleaned up
                    releaseEmptyPages(querySlaveId(), true);
                    if (numHeapPages == totalHeapPages.load(std::memory_order_relaxed))
                    {
                        VStringBuffer msg("Memory limit exceeded: current %u, requested %u, limit %u active(%u) heap(%u/%u)", pageCount, numRequested, pageLimit, activeRowManagers.load(), heapAllocated, heapTotalPages);
                        logctx.CTXLOG("%s", msg.str());

                        //Avoid a stack trace if the allocation is optional
                        if (maxSpillCost == SpillAllCost)
                            doOomReport();
                        throw MakeStringExceptionDirect(ROXIEMM_MEMORY_LIMIT_EXCEEDED, msg.str());
                    }
                }
            }
        }

        if (totalPages > peakPages)
        {
            if (trackMemoryByActivity)
                getPeakActivityUsage();
            peakPages = totalPages;
        }
        return totalPages;
    }

    virtual void reportPeakStatistics(IStatisticTarget & target, unsigned detail)
    {
        Owned<IActivityMemoryUsageMap> map;
        {
            NonReentrantSpinBlock block(peakSpinLock);
            map.set(peakUsageMap);
        }
        if (map)
            map->reportStatistics(target, detail, allocatorCache);
        target.addStatistic(SSTglobal, NULL, StSizePeakMemory, NULL, peakPages * HEAP_ALIGNMENT_SIZE, 1, 0, StatsMergeMax);
    }

    void restoreLimit(unsigned numRequested)
    {
        totalHeapPages.fetch_sub(numRequested, std::memory_order_relaxed);
    }

    inline bool releaseCallbackMemory(unsigned maxSpillCost, bool critical)
    {
        return releaseCallbackMemory(maxSpillCost, critical, false, 0);
    }

    virtual bool releaseCallbackMemory(unsigned maxSpillCost, bool critical, bool checkSequence, unsigned prevReleaseSeq) = 0;
    virtual unsigned getReleaseSeq() const = 0;
    virtual unsigned getPageLimit() const = 0;

    inline unsigned getActiveHeapPages() const { return totalHeapPages.load(std::memory_order_relaxed); }

    virtual void throwHeapExhausted(unsigned allocatorId, unsigned pages)
    {
        unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
        VStringBuffer msg("Pool memory exhausted: pool id %u{@%u} exhausted, requested %u heap(%u/%u) global(%u/%u) WM(%u..%u)", allocatorId, activityId, pages, getActiveHeapPages(), getPageLimit(), heapAllocated, heapTotalPages, heapLWM, heapHWM);
        DBGLOG("%s", msg.str());
        throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
    }

protected:
    void checkTraceAllocationSize(memsize_t _size, unsigned activityId);
    void checkTimeLimit();

    CRoxieFixedRowHeapBase * doCreateFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags, unsigned maxSpillCost)
    {
        if ((roxieHeapFlags & RHFoldfixed) || (fixedSize > FixedSizeHeaplet::maxHeapSize()))
            return new CRoxieFixedRowHeap(this, activityId, (RoxieHeapFlags)roxieHeapFlags, fixedSize);

        unsigned heapFlags = roxieHeapFlags & (RHFunique|RHFpacked|RHFscanning|RHFdelayrelease);
        if (heapFlags & RHFpacked)
        {
            CPackedChunkingHeap * heap = createPackedHeap(fixedSize, activityId, heapFlags, maxSpillCost);
            if (roxieHeapFlags & RHFblocked)
                return new CRoxieDirectFixedBlockedRowHeap<CPackedChunkingHeap>(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
            return new CRoxieDirectFixedRowHeap<CPackedChunkingHeap>(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
        }
        else
        {
            CFixedChunkedHeap * heap = createFixedHeap(fixedSize, activityId, heapFlags, maxSpillCost);
            if (roxieHeapFlags & RHFblocked)
                return new CRoxieDirectFixedBlockedRowHeap<CFixedChunkedHeap>(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
            return new CRoxieDirectFixedRowHeap<CFixedChunkedHeap>(this, activityId, (RoxieHeapFlags)roxieHeapFlags, heap);
        }
    }

    CHeap * getExistingHeap(size32_t chunkSize, unsigned activityId, unsigned flags)
    {
        ForEachItemIn(i, fixedHeaps)
        {
            CHeap & heap = fixedHeaps.item(i);
            if (heap.matches(chunkSize, activityId, flags))
            {
                if (heap.isAliveAndLink())
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

        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occurring
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
#ifdef ALWAYS_USE_SCAN_HEAP
        if (!(flags & RHFunique))
#else
        if (!(flags & (RHFunique|RHFscanning)))
#endif
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
        SpinBlock block(fixedSpinLock); //Spinblock needed if we can add/remove fixed heaps while allocations are occurring
        ForEachItemIn(i, fixedHeaps)
        {
            if (leaked == 0)
                break;
            CHeap & fixedHeap = fixedHeaps.item(i);
            fixedHeap.reportLeaks(leaked);
        }
    }

    virtual memsize_t compactRows(memsize_t count, const void * * rows)
    {
#ifndef OLD_ROW_COMPACT
        NewHeapCompactState state;
        for (memsize_t i = 0; i < count; i++)
        {
            const void * row = rows[i];
            if (row)
                HeapletBase::prepareToCompactRow(row, state);
        }

        if (!state.calculateRowsToMove())
            return 0;

        for (memsize_t i = 0; i < count; i++)
        {
            const void * row = rows[i];
            if (row)
            {
                const void * packed = HeapletBase::newCompactRow(row, state);
                rows[i] = packed;  // better to always assign
            }
        }
        return state.numPagesEmptied();
#else
        HeapCompactState state;
        for (memsize_t i = 0; i < count; i++)
        {
            const void * row = rows[i];
            if (row)
            {
                const void * packed = HeapletBase::compactRow(row, state);
                rows[i] = packed;  // better to always assign
            }
        }
        return state.numPagesEmptied;
#endif
    }

    virtual void reportMemoryUsage(bool peak) const
    {
        if (peak)
        {
            Owned<IActivityMemoryUsageMap> map;
            {
                NonReentrantSpinBlock block(peakSpinLock);
                map.set(peakUsageMap);
            }
            if (map)
                map->report(logctx, allocatorCache);
        }
        else
        {
            logctx.CTXLOG("RoxieMemMgr: pageLimit=%u peakPages=%u dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p cnt(%u)",
                          maxPageLimit, peakPages, dataBuffs, dataBuffPages, possibleGoers.load(), this, activeRowManagers.load());
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

    virtual IRowManager * querySlaveRowManager(unsigned slave) { return NULL; }
};

void CChunkingRowManager::checkTraceAllocationSize(memsize_t _size, unsigned activityId)
{
    if (memTraceSizeLimit && _size >= memTraceSizeLimit)
    {
        logctx.CTXLOG("Activity %u requesting %" I64F "u bytes!", getActivityId(activityId), (unsigned __int64) _size);
        PrintStackReport();
    }
}

void CChunkingRowManager::checkTimeLimit()
{
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


//================================================================================

class CGlobalRowManager;
class CSlaveRowManager : public CChunkingRowManager
{
public:
    CSlaveRowManager(unsigned _slaveId, CGlobalRowManager * _globalManager, memsize_t _memLimit, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, bool _outputOOMReports)
        : CChunkingRowManager(_memLimit, _tl, _logctx, _allocatorCache, _outputOOMReports), slaveId(_slaveId), globalManager(_globalManager)
    {
    }

    virtual bool releaseCallbackMemory(unsigned maxSpillCost, bool critical, bool checkSequence, unsigned prevReleaseSeq);
    virtual unsigned getReleaseSeq() const;
    virtual void addRowBuffer(IBufferedRowCallback * callback);
    virtual void removeRowBuffer(IBufferedRowCallback * callback);
    virtual void setMemoryCallbackThreshold(unsigned value) { throwUnexpected(); }
    virtual void setCallbackOnThread(bool value) { throwUnexpected(); }
    virtual void setMinimizeFootprint(bool value, bool critical) { throwUnexpected(); }
    virtual void setReleaseWhenModifyCallback(bool value, bool critical) { throwUnexpected(); }
    virtual unsigned querySlaveId() const { return slaveId; }
    virtual void reportMemoryUsage(bool peak) const;
    virtual void throwHeapExhausted(unsigned allocatorId, unsigned pages);

protected:
    virtual unsigned getPageLimit() const;

protected:
    unsigned slaveId;
    CGlobalRowManager * globalManager;
};

//================================================================================

static int compareHeaplets(void * const * left, void * const * right)
{
    ChunkedHeaplet * leftHeaplet = *reinterpret_cast<ChunkedHeaplet * const *>(left);
    ChunkedHeaplet * rightHeaplet = *reinterpret_cast<ChunkedHeaplet * const *>(right);
    if (leftHeaplet->queryHeap() != rightHeaplet->queryHeap())
        return (leftHeaplet->queryHeap() < rightHeaplet->queryHeap()) ? -1 : +1;

    if (leftHeaplet->hasSharedRow)
    {
        if (!rightHeaplet->hasSharedRow)
            return -1;
    }
    else
    {
        if (rightHeaplet->hasSharedRow)
            return +1;
    }

    return rightHeaplet->numCompactingRows() - leftHeaplet->numCompactingRows();
}

bool NewHeapCompactState::calculateRowsToMove()
{
    if (heaplets.ordinality() <= 1)
        return false;

    //Sort the list of heaplets by heap, heaplet, realCount
    heaplets.sort(compareHeaplets);

    //Walk through each heaplet in turn, processing a heap at a time.
    bool foundPossibility = false;
    unsigned firstHeaplet = 0;
    const CHeap * curHeap = nullptr;
    ForEachItemIn(i, heaplets)
    {
        ChunkedHeaplet * cur = heaplets.item(i);
        if (cur->queryHeap() != curHeap)
        {
            if (processHeap(firstHeaplet, i))
                foundPossibility = true;
            firstHeaplet = i;
            curHeap = cur->queryHeap();
        }
    }
    if (processHeap(firstHeaplet, heaplets.ordinality()))
        foundPossibility = true;
    return foundPossibility;
}


bool NewHeapCompactState::processHeap(unsigned low, unsigned max)
{
    //0 or 1 entries - do nothing.
    const unsigned numHeaplets = (max - low);
    if (numHeaplets <= 1)
        return false;

    //Calculate the total number of rows in the heaplet
    //MORE: This doesn't cover situations where other parts of the heaplet have lots of allocations....
    //if more than one heaplet for this heap.
    memsize_t totalRows = 0;
    for (unsigned i1=low; i1 < max; i1++)
        totalRows += heaplets.item(i1)->numCompactingRows();

    //work backwards and tag the heaplets that contain rows that should be moved.
    const CChunkedHeap * curHeap = static_cast<const CChunkedHeap *>(heaplets.item(low)->queryHeap());
    unsigned maxRows = curHeap->maxChunksPerPage();
    unsigned heapletsRequired = (unsigned)((totalRows + maxRows - 1) / maxRows);
    if (heapletsRequired >= numHeaplets)
        return false;

    //Start cloning rows into the heaplet with the largest number
    const_cast<CChunkedHeap *>(curHeap)->setCompactTarget(low);
    for (unsigned i2=low + heapletsRequired; i2 < max; i2++)
        heaplets.item(i2)->setMoveRows();
    return true;
}


//================================================================================

class CCallbackRowManager : public CChunkingRowManager
{
public:

    CCallbackRowManager(memsize_t _memLimit, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, bool _outputOOMReports)
        : CChunkingRowManager(_memLimit, _tl, _logctx, _allocatorCache, _outputOOMReports), callbacks(this)
    {
    }

    ~CCallbackRowManager()
    {
        callbacks.stopReleaseBufferThread();
    }

    virtual void setMemoryLimit(memsize_t bytes, memsize_t spillSize, unsigned backgroundReleaseCost)
    {
        CChunkingRowManager::setMemoryLimit(bytes, spillSize, backgroundReleaseCost);
        //The test allows no limit on memory, but spill above a certain amount.  Not sure if useful...
        if (spillPageLimit && (maxPageLimit != spillPageLimit))
            callbacks.startReleaseBufferThread(backgroundReleaseCost);
        else
            callbacks.stopReleaseBufferThread();
    }

    virtual void setMemoryCallbackThreshold(unsigned value)
    {
        callbacks.setMemoryCallbackThreshold(value);
    }

    virtual void setCallbackOnThread(bool value)
    {
        callbacks.setCallbackOnThread(value);
    }

    virtual void setReleaseWhenModifyCallback(bool value, bool critical)
    {
        callbacks.setReleaseWhenModifyCallback(value, critical);
    }

    virtual unsigned checkLimit(unsigned numRequested, unsigned maxSpillCost)
    {
        unsigned totalPages = CChunkingRowManager::checkLimit(numRequested, maxSpillCost);

        if (spillPageLimit && (totalPages > spillPageLimit))
        {
            callbacks.releaseBuffersInBackground();
        }
        return totalPages;
    }

    virtual bool releaseCallbackMemory(unsigned maxSpillCost, bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        dbgassertex(querySlaveId() == 0);   // this derived class is only used for non-slave row managers
        return callbacks.releaseBuffers(0, maxSpillCost, critical, checkSequence, prevReleaseSeq);
    }

    virtual unsigned getReleaseSeq() const
    {
        return callbacks.getReleaseSeq();
    }

    virtual unsigned getPageLimit() const
    {
        return maxPageLimit;
    }

    virtual unsigned querySlaveId() const { return 0; }

    virtual void reportMemoryUsage(bool peak) const
    {
        CChunkingRowManager::reportMemoryUsage(peak);
        callbacks.reportActive(logctx);
    }

protected:
    virtual void addRowBuffer(IBufferedRowCallback * callback)
    {
        callbacks.addRowBuffer(0, callback);
    }

    virtual void removeRowBuffer(IBufferedRowCallback * callback)
    {
        callbacks.removeRowBuffer(0, callback);
    }

protected:
    BufferedRowCallbackManager callbacks;
};

//================================================================================

/*
 * The Global Memory Manager allows a single block of global memory to be shared by multiple slave channels.
 *
 * memLimit is the total memory that is available globally and to all the slave channels
 * globalLimit is the maximum memory that can be allocated globally
 *
 * It has the following requirements:
 * - Global row manager running out of memory needs to be able to spill any slave callbacks.
 * - Slave row manager will need to be able to call global callbacks, but not other slaves.
 * - Once an activity spills, other slaves should also preferentially spill that activity. (Ideally on other nodes as well).
 *
 * The memory available to the global row manager should be:
 *      totalMemory - sum(slave-allocated) limited to globalLimit.
 *
 * The memory available to each slave should be:
 *      (totalMemory - globallyAllocatedMemory) / numSlaves
 *
 * Note: This currently does not allow slave channels to use more than their fair share of memory.
 */
class CGlobalRowManager : public CCallbackRowManager
{
public:
    CGlobalRowManager(memsize_t _memLimit, memsize_t _globalLimit, unsigned _numSlaves, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, const IRowAllocatorCache **slaveAllocatorCaches, bool _outputOOMReports)
        : CCallbackRowManager(_memLimit, _tl, _logctx, _allocatorCache, _outputOOMReports), numSlaves(_numSlaves)
    {
        DBGLOG("Create Global/Slave Row Manager %uMB total global can use max %uMB", (unsigned)(_memLimit / 0x100000), (unsigned)(_globalLimit / 0x100000));
        assertex(_globalLimit <= _memLimit);
        globalPageLimit = (unsigned) PAGES(_globalLimit, HEAP_ALIGNMENT_SIZE);
        slaveRowManagers = new CChunkingRowManager * [numSlaves];
        for (unsigned i=0; i < numSlaves; i++)
            slaveRowManagers[i] = new CSlaveRowManager(i+1, this, _memLimit, _tl, _logctx, slaveAllocatorCaches ? slaveAllocatorCaches[i] : _allocatorCache, _outputOOMReports);
    }
    ~CGlobalRowManager()
    {
        for (unsigned i=0; i < numSlaves; i++)
            slaveRowManagers[i]->Release();
        delete [] slaveRowManagers;
    }

    bool releaseSlaveCallbackMemory(unsigned slaveId, unsigned maxSpillCost, bool critical, bool checkSequence, unsigned prevReleaseSeq)
    {
        return callbacks.releaseBuffers(slaveId, maxSpillCost, critical, checkSequence, prevReleaseSeq);
    }

    void addSlaveRowBuffer(unsigned slaveId, IBufferedRowCallback * callback)
    {
        callbacks.addRowBuffer(slaveId, callback);
    }

    void removeSlaveRowBuffer(unsigned slaveId, IBufferedRowCallback * callback)
    {
        callbacks.removeRowBuffer(slaveId, callback);
    }

    virtual bool releaseEmptyPages(unsigned slaveId, bool forceFreeAll)
    {
        dbgassertex(slaveId <= numSlaves);
        bool success = CCallbackRowManager::releaseEmptyPages(slaveId, false);
        if (slaveId == 0)
        {
            for (unsigned slave = 0; slave < numSlaves; slave++)
            {
                if (slaveRowManagers[slave]->releaseEmptyPages(slaveId, forceFreeAll))
                    success = true;
            }
        }
        else
        {
            if (slaveRowManagers[slaveId-1]->releaseEmptyPages(slaveId, forceFreeAll))
                success = true;
        }
        return success;
    }

    virtual unsigned getPageLimit() const
    {
        //Sum the total pages allocated by the slaves.  Not called very frequently.
        //Do this in preference to maintaining a slaveTotal since that would involve an atomic add for
        //each slave page allocation.
        unsigned slavePages = 0;
        for (unsigned i=0; i < numSlaves; i++)
            slavePages += slaveRowManagers[i]->getActiveHeapPages();
        unsigned available = maxPageLimit - slavePages;
        if (available > globalPageLimit)
            return globalPageLimit;
        else
            return available;
    }

    virtual IRowManager * querySlaveRowManager(unsigned slave)
    {
        assertex(slave < numSlaves);
        return slaveRowManagers[slave];
    }

    void throwHeapExhausted(unsigned allocatorId, unsigned pages)
    {
        unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
        VStringBuffer msg("Shared global memory exhausted: pool id %u{@%u} exhausted, requested %u heap(%u/%u/%u/%u)) global(%u/%u) WM(%u..%u)", allocatorId, activityId, pages, getActiveHeapPages(), getPageLimit(), globalPageLimit, maxPageLimit, heapAllocated, heapTotalPages, heapLWM, heapHWM);
        DBGLOG("%s", msg.str());
        throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
    }

public:
    unsigned getSlavePageLimit(unsigned slaveId) const
    {
        //ugly....If there are 4 pages, 2 slaves, and 1 global page allocated, slave1 has a limit of 1, while slave 2 has a limit of 2
        const bool strict = true;
        if (strict)
            return (maxPageLimit - getActiveHeapPages() + (slaveId-1)) / numSlaves;

        return (maxPageLimit - getActiveHeapPages() + (numSlaves-1)) / numSlaves;
    }

protected:
    unsigned numSlaves;
    unsigned globalPageLimit;
    CChunkingRowManager * * slaveRowManagers;
};


//================================================================================

bool CSlaveRowManager::releaseCallbackMemory(unsigned maxSpillCost, bool critical, bool checkSequence, unsigned prevReleaseSeq)
{
    return globalManager->releaseSlaveCallbackMemory(slaveId, maxSpillCost, critical, checkSequence, prevReleaseSeq);
}

unsigned CSlaveRowManager::getReleaseSeq() const
{
    return globalManager->getReleaseSeq();
}

void CSlaveRowManager::addRowBuffer(IBufferedRowCallback * callback)
{
    globalManager->addSlaveRowBuffer(slaveId, callback);
}

void CSlaveRowManager::removeRowBuffer(IBufferedRowCallback * callback)
{
    globalManager->removeSlaveRowBuffer(slaveId, callback);
}

unsigned CSlaveRowManager::getPageLimit() const
{
    return globalManager->getSlavePageLimit(slaveId);
}

void CSlaveRowManager::reportMemoryUsage(bool peak) const
{
    CChunkingRowManager::reportMemoryUsage(peak);
    globalManager->reportMemoryUsage(peak);
}

void CSlaveRowManager::throwHeapExhausted(unsigned allocatorId, unsigned pages)
{
    unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
    VStringBuffer msg("Channel memory exhausted: pool id %u{@%u} exhausted, requested %u heap(%u/%u/%u)) global(%u/%u)", allocatorId, activityId, pages, getActiveHeapPages(), getPageLimit(), maxPageLimit, heapAllocated, heapTotalPages);
    DBGLOG("%s", msg.str());
    throw MakeStringExceptionDirect(ROXIEMM_MEMORY_POOL_EXHAUSTED, msg.str());
}


//================================================================================

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

void * CRoxieVariableRowHeap::allocate(memsize_t size, memsize_t & capacity)
{
    void * ret = rowManager->allocate(size, allocatorId);
    dbgassertex(ret);
    capacity = RoxieRowCapacity(ret);
    numAllocations++; // not thread safe, but missing entries do not matter
    return ret;
}

void CRoxieVariableRowHeap::gatherStats(CRuntimeStatisticCollection & merged)
{
    merged.addStatistic(StNumAllocations, numAllocations);
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

void CHeap::throwHeapExhausted(unsigned allocatorId, unsigned pages) const
{
    rowManager->throwHeapExhausted(allocatorId, pages);
}

//MORE: Make this a nested class??
HugeHeaplet * CHugeHeap::allocateHeaplet(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost)
{
    unsigned numPages = PAGES(_size + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);

    bool retryOnFailure = true; // if this fails release memory and try again
    for (;;)
    {
        rowManager->checkLimit(numPages, maxSpillCost);

        //If the allocation fails, then try and free some memory by calling the callbacks
        void * memory = suballoc_aligned(numPages, true);
        if (memory)
            return new (memory) HugeHeaplet(this, allocatorCache, _size, allocatorId);

        rowManager->restoreLimit(numPages);

        if (!retryOnFailure)
        {
            if (maxSpillCost == SpillAllCost)
                rowManager->reportMemoryUsage(false);
            throwHeapExhausted(allocatorId, numPages);
        }

        retryOnFailure = rowManager->releaseCallbackMemory(maxSpillCost, true);
    }
}

void * CHugeHeap::doAllocate(memsize_t _size, unsigned allocatorId, unsigned maxSpillCost)
{
    HugeHeaplet *head = allocateHeaplet(_size, allocatorId, maxSpillCost);

    if (memTraceLevel >= 2 || (memTraceSizeLimit && _size >= memTraceSizeLimit))
    {
        unsigned numPages = head->sizeInPages();
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %" I64F "u) allocated new HugeHeaplet size %" I64F "u - addr=%p pages=%u pageLimit=%u peakPages=%u rowMgr=%p",
            (unsigned __int64) _size, (unsigned __int64) (numPages*HEAP_ALIGNMENT_SIZE), head, numPages, rowManager->getPageLimit(), rowManager->peakPages, this);
    }

    CriticalBlock b(heapletLock);
    insertHeaplet(head);
    return head->allocateHuge(_size);
}

void CHugeHeap::expandHeap(void * original, memsize_t copysize, memsize_t oldcapacity, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback)
{
    unsigned newPages = PAGES(newsize + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
    unsigned oldPages = PAGES(oldcapacity + HugeHeaplet::dataOffset(), HEAP_ALIGNMENT_SIZE);
    void *oldbase =  (void *) ((memsize_t) original & HEAP_ALIGNMENT_MASK);
    HugeHeaplet * oldHeaplet = (HugeHeaplet *)oldbase;
    assertex(oldHeaplet->isWithinHeap(this));

    //Check if we are shrinking the number of pages.
    if (newPages <= oldPages)
    {
        //Can always do in place by releasing pages at the end of the block
        void *realloced = subrealloc_aligned(oldbase, oldPages, newPages);
        assertex(realloced == oldbase);
        void * ret = (char *) realloced + HugeHeaplet::dataOffset();
        HugeHeaplet *head = (HugeHeaplet *) realloced;
        memsize_t newCapacity = head->setCapacity(newsize);

        //Update the max capacity
        callback.atomicUpdate(newCapacity, ret);
        rowManager->restoreLimit(oldPages-newPages);
        return;
    }

    unsigned numPages = newPages - oldPages;
    bool retryOnFailure = true; // if this fails release memory and try again
    for (;;)
    {
        // NOTE: we request permission only for the difference between the old
        // and new sizes, even though we may temporarily hold both. This not only
        // simplifies the code considerably, it's probably desirable
        rowManager->checkLimit(numPages, maxSpillCost);

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
            //NOTE: Huge pages are only added to the space list when they are freed => no need to check
            //if it needs removing and re-adding to that list.
            if (realloced != oldbase)
            {
                // Remove the old block from the chain
                {
                    CriticalBlock b(heapletLock);
                    removeHeaplet(oldhead);
                }

                //Copying data must lock for the duration (otherwise another thread modifying the data may leave it out of sync)
                callback.lock();

                // MORE - If we were really clever, we could manipulate the page table to avoid moving ANY data here...
                memmove(realloced, oldbase, copysize + HugeHeaplet::dataOffset());  // NOTE - assumes no trailing data (e.g. end markers)

                void * ret = (char *) realloced + HugeHeaplet::dataOffset();
                memsize_t newCapacity = head->setCapacity(newsize);

                //previously locked => update the pointer and then unlock
                callback.update(newCapacity, ret);
                callback.unlock();

                {
                    CriticalBlock b(heapletLock);
                    insertHeaplet(head);
                }

                if (release)
                    subfree_aligned(oldbase, oldPages);
            }
            else
            {
                memsize_t newCapacity = head->setCapacity(newsize);
                callback.atomicUpdate(newCapacity, original);
            }

            return;
        }

        //If the allocation fails, then try and free some memory by calling the callbacks

        rowManager->restoreLimit(numPages);
        if (!retryOnFailure)
        {
            if (maxSpillCost == SpillAllCost)
                rowManager->reportMemoryUsage(false);
            throwGlobalHeapExhausted(activityId, newPages, oldPages);
        }

        retryOnFailure = rowManager->releaseCallbackMemory(maxSpillCost, true);
    }
}


void CChunkedHeap::gatherStats(CRuntimeStatisticCollection & result)
{
    HeapletStats merged;
    {
        //Copy the stats when a lock is held to ensure num and distance are consistent.
        CriticalBlock b(heapletLock);
        merged = stats;
    }

    if (merged.totalAllocs)
    {
        result.addStatistic(StNumAllocations, merged.totalAllocs);
        result.addStatistic(StNumAllocationScans, merged.totalDistanceScanned / chunkSize);
    }
}

void * CChunkedHeap::doAllocateRow(unsigned allocatorId, unsigned maxSpillCost)
{
    //Only hold the lock while walking the list - so subsequent calls to checkLimit don't deadlock.
    //NB: The allocation is split into two - finger->allocateChunk, and finger->initializeChunk().
    //The latter is done outside the lock, to reduce the window for contention.
    ChunkedHeaplet * donorHeaplet;
    char * chunk;
    bool retryOnFailure = true; // if this fails release memory and try again
    for (;;)
    {
        {
            CriticalBlock block(heapletLock);

            if (likely(activeHeaplet))
            {
                //This cast is safe because we are within a member of CChunkedHeap
                donorHeaplet = static_cast<ChunkedHeaplet *>(activeHeaplet);
                chunk = donorHeaplet->allocateChunk();
                if (likely(chunk))
                {
                    //The code at the end of this function needs to be executed outside of the spinblock.
                    //Just occasionally gotos are the best way of expressing something
                    goto gotChunk;
                }
                activeHeaplet = NULL;
            }

            //Now walk the list of blocks which may potentially have some free space:
            for (;;)
            {
                Heaplet * next = popFromSpaceList();
                if (!next)
                    break;

                //This cast is safe because we are within a member of CChunkedHeap
                donorHeaplet = static_cast<ChunkedHeaplet *>(next);
                chunk = donorHeaplet->allocateChunk();
                if (chunk)
                {
                    activeHeaplet = donorHeaplet;
                    //The code at the end of this function needs to be executed outside of the spinblock.
                    //Just occasionally gotos are the best way of expressing something
                    goto gotChunk;
                }
            }
        }

        //NB: At this point activeHeaplet = NULL;
        //This code needs to be outside the critical section, otherwise it may deadlock with another thread trying
        //to dispose of any free pages.
        try
        {
            rowManager->checkLimit(1, maxSpillCost);
        }
        catch (IException * e)
        {
            //No pages left, but freeing the buffers may have freed some records from the current heap
            if (!mayHaveEmptySpace())
                throw;
            logctx.CTXLOG("Checking for space in existing heap following callback");
            e->Release();
            continue;
        }

        donorHeaplet = allocateHeaplet();
        if (donorHeaplet)
            break;
        rowManager->restoreLimit(1);

        //Could check if activeHeaplet was now set (and therefore allocated by another thread), and if so restart
        //the function, but unlikley to be worthwhile
        if (!retryOnFailure)
            throwHeapExhausted(allocatorId, 1);

        //Release some memory, but only try to release again if some memory was released this time
        retryOnFailure = rowManager->releaseCallbackMemory(maxSpillCost, true);
    }

    if (memTraceLevel >= 3 && (memTraceLevel >= 5 || chunkSize > 32000))
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %u) allocated new FixedSizeHeaplet size %u - addr=%p pageLimit=%u peakPages=%u rowMgr=%p",
                chunkSize, chunkSize, donorHeaplet, rowManager->getPageLimit(), rowManager->peakPages, this);

    {
        CriticalBlock block(heapletLock);
        insertHeaplet(donorHeaplet);

        //While this thread was allocating a block, another thread may have also done the same.
        chunk = nullptr;
        if (activeHeaplet)
        {
            ChunkedHeaplet * active = static_cast<ChunkedHeaplet *>(activeHeaplet);

            //check if we can allocate from the block the other thread allocated
            chunk = active->allocateChunk();
            if (chunk)
            {
                //yes => add the block we allocated to the freespace list.
                addToSpaceList(donorHeaplet);
                donorHeaplet = active;
            }
            else
            {
                addToSpaceList(active);
                activeHeaplet = donorHeaplet;
            }
        }
        else
            activeHeaplet = donorHeaplet;

        //If no protecting lock there would be a race e.g., if another thread allocates all the rows!
        if (!chunk)
            chunk = donorHeaplet->allocateChunk();
        dbgassertex(chunk);
    }
    
gotChunk:

    //since a chunk has been allocated from donorHeaplet it cannot be released at this point.
    return donorHeaplet->initChunk(chunk, allocatorId);
}

//MORE: Consider allocating single rows via this function as well.
unsigned CChunkedHeap::doAllocateRowBlock(unsigned allocatorId, unsigned maxSpillCost, unsigned maxRows, char * * rows)
{
    //Only hold the spinblock while walking the list - so subsequent calls to checkLimit don't deadlock.
    //NB: The allocation is split into two - finger->allocateChunk, and finger->initializeChunk().
    //The latter is done outside the spinblock, to reduce the window for contention.
    ChunkedHeaplet * donorHeaplet;
    unsigned allocated = 0;
    bool retryOnFailure = true; // if this fails release memory and try again
    for (;;)
    {
        {
            CriticalBlock b(heapletLock);
            if (activeHeaplet)
            {
                //This cast is safe because we are within a member of CChunkedHeap
                donorHeaplet = static_cast<ChunkedHeaplet *>(activeHeaplet);
                allocated = donorHeaplet->allocateMultiChunk(maxRows, rows);
                if (allocated)
                {
                    //The code at the end of this function needs to be executed outside of the spinblock.
                    //Just occasionally gotos are the best way of expressing something
                    goto gotChunk;
                }
                activeHeaplet = NULL;
            }

            //Now walk the list of blocks which may potentially have some free space:
            for (;;)
            {
                Heaplet * next = popFromSpaceList();
                if (!next)
                    break;

                //This cast is safe because we are within a member of CChunkedHeap
                donorHeaplet = static_cast<ChunkedHeaplet *>(next);
                allocated = donorHeaplet->allocateMultiChunk(maxRows, rows);
                if (allocated)
                {
                    activeHeaplet = donorHeaplet;
                    //The code at the end of this function needs to be executed outside of the spinblock.
                    //Just occasionally gotos are the best way of expressing something
                    goto gotChunk;
                }
            }
        }

        //NB: At this point activeHeaplet = NULL;
        try
        {
            rowManager->checkLimit(1, maxSpillCost);
        }
        catch (IException * e)
        {
            //No pages left, but freeing the buffers may have freed some records from the current heap
            if (!mayHaveEmptySpace())
                throw;
            logctx.CTXLOG("Checking for space in existing heap following callback");
            e->Release();
            continue;
        }

        donorHeaplet = allocateHeaplet();
        if (donorHeaplet)
            break;
        rowManager->restoreLimit(1);

        //Could check if activeHeaplet was now set (and therefore allocated by another thread), and if so restart
        //the function, but grabbing the spin lock would be inefficient.
        if (!retryOnFailure)
            throwHeapExhausted(allocatorId, 1);

        retryOnFailure = rowManager->releaseCallbackMemory(maxSpillCost, true);
    }

    if (memTraceLevel >= 5 || (memTraceLevel >= 3 && chunkSize > 32000))
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %u) allocated new FixedSizeHeaplet size %u - addr=%p pageLimit=%u peakPages=%u rowMgr=%p",
                chunkSize, chunkSize, donorHeaplet, rowManager->getPageLimit(), rowManager->peakPages, this);

    {
        CriticalBlock b(heapletLock);
        insertHeaplet(donorHeaplet);

        //While this thread was allocating a block, another thread also did the same.  Ensure that other block is
        //placed on the list of those with potentially free space.
        if (activeHeaplet)
            addToSpaceList(activeHeaplet);
        activeHeaplet = donorHeaplet;

        //If no protecting spinblock there would be a race e.g., if another thread allocates all the rows!
        allocated = donorHeaplet->allocateMultiChunk(maxRows, rows);
        dbgassertex(allocated);
    }

gotChunk:
    //since a chunk has been allocated from donorHeaplet it cannot be released at this point.
    for (unsigned i=0; i < allocated; i++)
        rows[i] = (char *)donorHeaplet->initChunk(rows[i], allocatorId);
    return allocated;
}

const void * CChunkedHeap::compactRow(const void * ptr, HeapCompactState & state)
{
    //Use protect heap instead of a lock, so that multiple compacts on the same heap (very likely) avoid
    //re-entering the critical sections.
    state.protectHeap(this);
    Heaplet *finger = state.next;

    //This loop currently walks through the heaplet list.  It *might* be more efficient to walk the list of
    //heaplets with potential space
    if (finger)
    {
        for (;;)
        {
           //This cast is safe because we are within a member of CChunkedHeap
            ChunkedHeaplet * chunkedFinger = static_cast<ChunkedHeaplet *>(finger);
            const void *ret = chunkedFinger->moveRow(ptr);
            if (ret)
            {
                //Instead of moving this block to the head of the list, save away the next block to try to put a block into
                //since we know what all blocks before this must be filled.
                state.next = finger;

                HeapletBase *srcBase = findBase(ptr);
                if (srcBase->isEmpty())
                {
                    state.numPagesEmptied++;
                    //could call releaseEmptyPages(false) at this point since already in the crit section.
                }
                return ret;
            }

            //heaplet was either empty or full (it may no longer be full if another thread has freed a row)
            finger = getNext(finger);

            //Check if we have looped all the way around
            if (finger == heaplets)
                break;
        }
    }
    return ptr;
}

const void * CChunkedHeap::newCompactRow(const void * ptr, NewHeapCompactState & state)
{
    for (;;)
    {
        ChunkedHeaplet * curTarget = state.queryHeaplet(curCompactTarget);
        assertex(curTarget->queryHeap() == this);
        const void * ret = curTarget->newMoveRow(ptr);
        if (ret)
            return ret;

        //Heaplet is full.  If allocations are occuring at the same time it is possible that
        curCompactTarget++;
    }

    return ptr;
}

void * CChunkedHeap::doAllocate(unsigned activityId, unsigned maxSpillCost)
{
    return doAllocateRow(activityId, maxSpillCost);
}

void CChunkedHeap::checkScans(unsigned allocatorId)
{
    if (stats.totalAllocs == totalAllocsLastScanCheck)
        return;

    HeapletStats merged;
    {
        //Copy the stats when a lock is held to ensure num and distance are consistent.
        CriticalBlock b(heapletLock);
        merged = stats;
    }

    totalAllocsLastScanCheck = merged.totalAllocs;

    unsigned __int64 numScans = merged.totalDistanceScanned / chunkSize;
    if (numScans && (numScans >= merged.totalAllocs * ScanReportThreshold))
        reportScanProblem(allocatorId, numScans, merged);
}

void CChunkedHeap::releaseAllRows()
{
    CriticalBlock b(heapletLock);

    if (heaplets)
    {
        Heaplet *finger = heaplets;

        //Note: This loop doesn't unlink the list because the list and all blocks are going to be disposed.
        do
        {
            Heaplet *next = getNext(finger);
            finger->releaseAllRows();
            delete finger;
            finger = next;
        } while (finger != heaplets);

        heaplets = nullptr;
        activeHeaplet = nullptr;
    }

    possibleEmptyPages.store(false, std::memory_order_release);
    headMaybeSpace.store(BLOCKLIST_NULL, std::memory_order_release);
}


//================================================================================

ChunkedHeaplet * CFixedChunkedHeap::allocateHeaplet()
{
    void * memory = suballoc_aligned(1, true);
    if (!memory)
        return NULL;
    return new (memory) FixedSizeHeaplet(this, allocatorCache, chunkSize, flags);
}

void * CFixedChunkedHeap::allocate(unsigned activityId)
{
    rowManager->beforeAllocate(chunkSize-FixedSizeHeaplet::chunkHeaderSize, activityId);
    return doAllocateRow(activityId, defaultSpillCost);
}


unsigned CFixedChunkedHeap::allocateBlock(unsigned activityId, unsigned maxRows, char * * rows)
{
    rowManager->beforeAllocate(chunkSize-FixedSizeHeaplet::chunkHeaderSize, activityId);
    return doAllocateRowBlock(activityId, defaultSpillCost, maxRows, rows);
}


void CFixedChunkedHeap::reportScanProblem(unsigned allocatorId, unsigned __int64 numScans, const HeapletStats & mergedStats)
{
    unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
    logctx.CTXLOG("Excessive scans in shared heap{%x:%u} (%.2f).  Size(%u) scans(%" I64F "u/%" I64F "u)",
           flags, activityId, (double)numScans / mergedStats.totalAllocs, chunkSize-FixedSizeHeaplet::chunkHeaderSize, numScans, mergedStats.totalAllocs);
}

ChunkedHeaplet * CPackedChunkingHeap::allocateHeaplet()
{
    void * memory = suballoc_aligned(1, true);
    if (!memory)
        return NULL;
    return new (memory) PackedFixedSizeHeaplet(this, allocatorCache, chunkSize, allocatorId, flags);
}

void * CPackedChunkingHeap::allocate(unsigned allocatorId)
{
    rowManager->beforeAllocate(chunkSize-PackedFixedSizeHeaplet::chunkHeaderSize, allocatorId);
    return doAllocateRow(allocatorId, defaultSpillCost);
}


unsigned CPackedChunkingHeap::allocateBlock(unsigned activityId, unsigned maxRows, char * * rows)
{
    rowManager->beforeAllocate(chunkSize-PackedFixedSizeHeaplet::chunkHeaderSize, activityId);
    return doAllocateRowBlock(activityId, defaultSpillCost, maxRows, rows);
}


void CPackedChunkingHeap::reportScanProblem(unsigned, unsigned __int64 numScans, const HeapletStats & mergedStats)
{
    unsigned activityId = getRealActivityId(allocatorId, allocatorCache);
    logctx.CTXLOG("Excessive scans in heap{%x} for activity %u (%.2f).  Size(%u) scans(%" I64F "u/%" I64F "u)",
           flags, activityId, (double)numScans / mergedStats.totalAllocs, chunkSize-PackedFixedSizeHeaplet::chunkHeaderSize, numScans, mergedStats.totalAllocs);
}

//================================================================================
// Buffer manager - blocked 

void DataBuffer::Release()
{
    if (count.load(std::memory_order_relaxed)==2 && mgr)
        mgr->noteDataBuffReleased((DataBuffer*) this);
    if (count.fetch_sub(1, std::memory_order_release) == 1)
    {
        //No acquire fence - released() is assumed not to access the data in this buffer
        //If it does it should contain an acquire fence
        released();
    }
}

void DataBuffer::released()
{
    assert(next == NULL);
    DataBufferBottom *bottom = (DataBufferBottom *)findBase(this);
    assert((char *)bottom != (char *)this);
    if (memTraceLevel >= 4)
        DBGLOG("RoxieMemMgr: DataBuffer::released() releasing DataBuffer - addr=%p", this);
    dataBuffersActive.fetch_sub(1);
    bottom->addToFreeChain(this);
    bottom->Release();
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

void DataBuffer::noteReleased(const void *ptr)
{
    //The link counter is shared by all the rows that are contained in this DataBuffer
    if (count.fetch_sub(1, std::memory_order_release) == 1)
    {
        //No acquire fence - released() is assumed not to access the data in this buffer
        //If it does it should contain an acquire fence
        released();
    }
}

void DataBuffer::noteLinked(const void *ptr)
{
    count.fetch_add(1, std::memory_order_relaxed);
}

class CDataBufferManager : implements IDataBufferManager, public CInterface
{
    friend class DataBufferBottom;
    CriticalSection crit;
    DataBufferBottom *curBlock;
    DataBufferBottom *freeChain;
    char * nextBase = nullptr;
    size32_t nextOffset = 0; // offset within a page
    std::atomic_uint freePending;

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
        if (goer==curBlock)
        {
            curBlock = NULL;
            nextBase = NULL;
            assertex(!"ERROR: heap block freed too many times!");
        }
    }

    void freeUnused()
    {
        DataBufferBottom *finger = freeChain;
        if (finger)
        {
            for (;;)
            {
                // NOTE - do NOT put a CriticalBlock c(finger->crit) here since:
                // 1. It's not needed - no-one modifies finger->nextBottom chain but me and I hold CDataBufferManager::crit
                // 2. finger->crit is about to get released back to the pool and it's important that it is not locked at the time!
                if (finger->okToFree.load(std::memory_order_acquire))
                {
                    assert(!finger->isAlive());
                    DataBufferBottom *goer = finger;
                    finger = finger->nextBottom;
                    unlink(goer);
                    if (memTraceLevel >= 3)
                        DBGLOG("RoxieMemMgr: DataBufferBottom::allocate() freeing DataBuffers Page - addr=%p", goer);
                    goer->~DataBufferBottom(); 
#ifdef _DEBUG
                    memset((void *) goer, 0xcc, HEAP_ALIGNMENT_SIZE);
#endif
                    subfree_aligned(goer, 1);
                    dataBufferPages.fetch_sub(1);
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

    CDataBufferManager(size32_t size) : freePending(false)
    {
        assertex(size==DATA_ALIGNMENT_SIZE);
        curBlock = NULL;
        freeChain = NULL;
    }

    DataBuffer *allocate()
    {
        CriticalBlock b(crit);

        if (memTraceLevel >= 5)
            DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() curBlock=%p nextAddr=%p:%x", curBlock, nextBase, nextOffset);

        if (freePending.exchange(false, std::memory_order_acquire))
            freeUnused();

        for (;;)
        {
            if (curBlock)
            {
                DataBufferBottom *bottom = curBlock;
                CriticalBlock c(bottom->crit);
                if (bottom->freeChain)
                {
                    dataBuffersActive.fetch_add(1);
                    curBlock->Link();
                    DataBuffer *x = bottom->freeChain;
                    bottom->freeChain = x->next;
                    x->next = NULL;
                    if (memTraceLevel >= 4)
                        DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() reallocated DataBuffer - addr=%p", x);
                    return ::new(x) DataBuffer();
                }
                else if (nextOffset < HEAP_ALIGNMENT_SIZE) // Is there any space in the current block (it must be a whole block)
                {
                    dataBuffersActive.fetch_add(1);
                    curBlock->Link();
                    DataBuffer *x = ::new(nextBase+nextOffset) DataBuffer();
                    nextOffset += DATA_ALIGNMENT_SIZE;
                    if (nextOffset == HEAP_ALIGNMENT_SIZE)
                    {
                        // MORE: May want to delete this "if" logic !!
                        //       and let it be handled in the similar logic of "else" part below.
                        curBlock->Release();
                        curBlock = NULL;
                        nextBase = NULL;
                        //nextOffset = 0 - not needed since only used if curBlock is set
                    }
                    if (memTraceLevel >= 4)
                        DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() allocated DataBuffer - addr=%p", x);
                    return x;
                }
                else
                {
                    curBlock->Release();
                    curBlock = NULL;
                    nextBase = NULL;
                    //nextOffset = 0 - not needed since only used if curBlock is set
                }
            }
            // see if a previous block that is waiting to be freed has any on its free chain
            DataBufferBottom *finger = freeChain;
            if (finger)
            {
                for (;;)
                {
                    CriticalBlock c(finger->crit);
                    if (finger->freeChain)
                    {
                        finger->Link(); // Link once for the reference we save in curBlock
                                        // Release (to dec ref count) when no more free blocks in the page
                        if (finger->isAlive())
                        {
                            curBlock = finger;
                            nextBase = nullptr; // should never be accessed
                            nextOffset = HEAP_ALIGNMENT_SIZE; // only use the free chain to allocate
                            dataBuffersActive.fetch_add(1);
                            finger->Link(); // and once for the value we are about to return
                            DataBuffer *x = finger->freeChain;
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
            nextBase = (char *)suballoc_aligned(1, false);
            nextOffset = DATA_ALIGNMENT_SIZE;
            curBlock = (DataBufferBottom *)nextBase;
            dataBufferPages.fetch_add(1);
            assertex(curBlock);
            if (memTraceLevel >= 3)
                    DBGLOG("RoxieMemMgr: CDataBufferManager::allocate() allocated new DataBuffers Page - addr=%p", curBlock);
            freeChain = ::new(curBlock) DataBufferBottom(this, freeChain);   // we never allocate the lowest one in the heap - used just for refcounting the rest
        }
    }

    void cleanUp()
    {
        freeUnused();
    }

    void poolStats(StringBuffer &s)
    {
        if (memTraceLevel > 1) memmap(s); // MORE: may want to make a separate interface (poolMap) and control query for this
        else memstats(s);
        s.appendf(", DataBuffsActive=%d, DataBuffPages=%d", dataBuffersActive.load(std::memory_order_relaxed), dataBufferPages.load(std::memory_order_relaxed));
        DBGLOG("%s", s.str());
    }
};

DataBufferBottom::DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain) : okToFree(false)
{
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

void DataBufferBottom::addToFreeChain(DataBuffer * buffer)
{
    CriticalBlock b(crit);
    buffer->next = freeChain;
    freeChain = buffer;
}

void DataBufferBottom::Release()
{
    if (count.fetch_sub(1, std::memory_order_release) == 1)
    {
        //No acquire fence - released() is assumed not to access the data in this buffer
        //If it does it should contain an acquire fence
        released();
    }
}

void DataBufferBottom::released()
{
    // Not safe to free here as owner may be in the middle of allocating from it
    // instead, give owner a hint that it's worth thinking about freeing this page next time it is safe
    unsigned expected = 0;
    if (count.compare_exchange_strong(expected, DEAD_PSEUDO_COUNT, std::memory_order_release))
    {
        //Need to save the value of owner because the object can be freed as soon as okToFree is set
        CDataBufferManager * savedOwner = owner;
        //No acquire fence required since the following code doesn't read anything from the object
        okToFree.store(true, std::memory_order_release);
        savedOwner->freePending.store(true, std::memory_order_release);
    }
}

void DataBufferBottom::noteReleased(const void *ptr)
{
    DataBuffer * buffer = queryDataBuffer(ptr);
    assertex(buffer);
    buffer->noteReleased(ptr);
}

void DataBufferBottom::noteReleased(unsigned numRows, const byte * * rowset)
{
    throwUnexpected();
}

void DataBufferBottom::noteLinked(const void *ptr)
{
    DataBuffer * buffer = queryDataBuffer(ptr);
    assertex(buffer);
    return buffer->noteLinked(ptr);
}

bool DataBufferBottom::_isShared(const void *ptr) const
{
    DataBuffer * buffer = queryDataBuffer(ptr);
    assertex(buffer);
    // Because the link counter is shared assume all pointers are shared
    return true;
}

memsize_t DataBufferBottom::_capacity() const { throwUnexpected(); }
void DataBufferBottom::_setDestructorFlag(const void *ptr) { throwUnexpected(); }

//================================================================================
//

extern IRowManager *createRowManager(memsize_t memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool outputOOMReports)
{
    if (numDirectBuckets == 0)
        throw MakeStringException(ROXIEMM_HEAP_ERROR, "createRowManager() called before setTotalMemoryLimit()");

    return new CCallbackRowManager(memLimit, tl, logctx, allocatorCache, outputOOMReports);
}

extern IRowManager *createGlobalRowManager(memsize_t memLimit, memsize_t globalLimit, unsigned numSlaves, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, const IRowAllocatorCache **slaveAllocatorCaches, bool outputOOMReports)
{
    if (numDirectBuckets == 0)
        throw MakeStringException(ROXIEMM_HEAP_ERROR, "createRowManager() called before setTotalMemoryLimit()");

    return new CGlobalRowManager(memLimit, globalLimit, numSlaves, tl, logctx, allocatorCache, slaveAllocatorCaches, outputOOMReports);
}

extern void setMemoryStatsInterval(unsigned secs)
{
    statsCyclesInterval = nanosec_to_cycle(I64C(1000000000) * secs);
    lastStatsCycles = get_cycles_now();
}

extern void setTotalMemoryLimit(bool allowHugePages, bool allowTransparentHugePages, bool retainMemory, memsize_t max, memsize_t largeBlockSize, const unsigned * allocSizes, ILargeMemCallback * largeBlockCallback)
{
    assertex(largeBlockSize == align_pow2(largeBlockSize, HEAP_ALIGNMENT_SIZE));
    memsize_t totalMemoryLimit = (unsigned) (max / HEAP_ALIGNMENT_SIZE);
    memsize_t largeBlockGranularity = (unsigned)(largeBlockSize / HEAP_ALIGNMENT_SIZE);
    if ((max != 0) && (totalMemoryLimit == 0))
        totalMemoryLimit = 1;
    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: Setting memory limit to %" I64F "d bytes (%" I64F "u pages)", (unsigned __int64) max, (unsigned __int64)totalMemoryLimit);
    initializeHeap(allowHugePages, allowTransparentHugePages, retainMemory, totalMemoryLimit, largeBlockGranularity, largeBlockCallback);
    initAllocSizeMappings(allocSizes ? allocSizes : defaultAllocSizes);
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
   return dataBufferPages.load(std::memory_order_relaxed);
}

extern unsigned getDataBuffersActive()
{
   return dataBuffersActive.load(std::memory_order_relaxed);
}

extern IDataBufferManager *createDataBufferManager(size32_t size)
{
    return new CDataBufferManager(size);
}

extern void setDataAlignmentSize(unsigned size)
{
    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: setDataAlignmentSize to %u", size); 

    if ((size == 0) || ((HEAP_ALIGNMENT_SIZE % size) != 0))
        throw MakeStringException(ROXIEMM_INVALID_MEMORY_ALIGNMENT, "setDataAlignmentSize %u must be a factor of %u", size, (unsigned)HEAP_ALIGNMENT_SIZE);

    if (size>=0x400 && size<=0x2000)
        DATA_ALIGNMENT_SIZE = size;
    else
        throw MakeStringException(ROXIEMM_INVALID_MEMORY_ALIGNMENT, "Invalid parameter to setDataAlignmentSize %u", size);
}

} // namespace roxiemem

//============================================================================================================
#ifdef _USE_CPPUNIT
#include "unittests.hpp"

#include <algorithm>

namespace roxiemem {

//================================================================================

//A simple implementation of a buffered rows class - used for testing
class SimpleRowBuffer : implements IBufferedRowCallback
{
public:
    SimpleRowBuffer(IRowManager * rowManager, unsigned _cost, unsigned _id) : cost(_cost), rows(rowManager, 0, 1, UNKNOWN_ROWSET_ID), id(_id)
    {
    }

//interface IBufferedRowCallback
    virtual unsigned getSpillCost() const { return cost; }
    virtual unsigned getActivityId() const { return id; }

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
        ReleaseRoxieRowArray(numCommitted, committed);
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
    unsigned id;
};

// A row buffer which does not allocate memory for the row array from roxiemem
class TestingRowBuffer : implements IBufferedRowCallback
{
public:
    TestingRowBuffer(unsigned _cost, unsigned _id) : cost(_cost), id(_id)
    {
    }
    ~TestingRowBuffer() { kill(); }

//interface IBufferedRowCallback
    virtual unsigned getSpillCost() const { return cost; }
    virtual unsigned getActivityId() const { return id; }
    virtual bool freeBufferedRows(bool critical)
    {
        if (rows.ordinality() == 0)
            return false;
        kill();
        return true;
    }

    void addRow(const void * row)
    {
        rows.append(row);
    }

    void kill()
    {
        ReleaseRoxieRows(rows);
    }

protected:
    ConstPointerArray rows;
    unsigned cost;
    unsigned id;
};

//A buffered row class - used for testing
class CallbackBlockAllocator : implements IBufferedRowCallback
{
public:
    CallbackBlockAllocator(IRowManager * _rowManager, memsize_t _size, unsigned _cost, unsigned _id) : cost(_cost), id(_id), rowManager(_rowManager), size(_size)
    {
        rowManager->addRowBuffer(this);
    }
    ~CallbackBlockAllocator()
    {
        rowManager->removeRowBuffer(this);
    }

//interface IBufferedRowCallback
    virtual unsigned getSpillCost() const { return cost; }
    virtual unsigned getActivityId() const { return id; }

    void allocate()
    {
        row.setown(rowManager->allocate(size, 0));
    }

    void costAllocate(unsigned allocCost)
    {
        try
        {
            row.setown(rowManager->allocate(size, 0, allocCost));
        }
        catch (IException * e)
        {
            row.clear();
            e->Release();
        }
    }

    inline bool hasRow() const { return row != NULL; }

protected:
    OwnedRoxieRow row;
    IRowManager * rowManager;
    memsize_t size;
    unsigned cost;
    unsigned id;
};


//Free the block as soon as requested
class SimpleCallbackBlockAllocator : public CallbackBlockAllocator
{
public:
    SimpleCallbackBlockAllocator(IRowManager * _rowManager, memsize_t _size, unsigned _cost, unsigned _id)
        : CallbackBlockAllocator(_rowManager, _size, _cost, _id)
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
    NastyCallbackBlockAllocator(IRowManager * _rowManager, unsigned _size, unsigned _cost, unsigned _id)
        : CallbackBlockAllocator(_rowManager, _size, _cost, _id)
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
        CPPUNIT_TEST(testRegisterCallbacks);
        CPPUNIT_TEST(testCostCallbacks);
        CPPUNIT_TEST(testGlobalCallbacks);
        CPPUNIT_TEST(testRoundup);
        CPPUNIT_TEST(testCompressSize);
        CPPUNIT_TEST(testBitmap);
        CPPUNIT_TEST(testAllocSize);
        CPPUNIT_TEST(testReleaseAll);
        CPPUNIT_TEST(testHuge);
        CPPUNIT_TEST(testAll);
        CPPUNIT_TEST(testRecursiveCallbacks);
        CPPUNIT_TEST(testResize);
        CPPUNIT_TEST(testResizeLock);
        //MORE: The following currently leak pages, so should go last
        CPPUNIT_TEST(testDatamanager);
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
        DBGLOG("Heaplet: cacheline(%u) base(%u) fixedbase(%u) fixed(%u) packed(%u) huge(%u)",
                CACHE_LINE_SIZE, (size32_t)sizeof(Heaplet), (size32_t)sizeof(ChunkedHeaplet), (size32_t)sizeof(FixedSizeHeaplet), (size32_t)sizeof(PackedFixedSizeHeaplet), (size32_t)sizeof(HugeHeaplet));
        DBGLOG("Heap: fixed(%u) packed(%u) huge(%u)",
                (size32_t)sizeof(CFixedChunkedHeap), (size32_t)sizeof(CPackedChunkingHeap), (size32_t)sizeof(CHugeHeap));
        DBGLOG("IHeap: fixed(%u) directfixed(%u) variable(%u)",
                (size32_t)sizeof(CRoxieFixedRowHeap), (size32_t)sizeof(CRoxieDirectFixedRowHeap<CPackedChunkingHeap>), (size32_t)sizeof(CRoxieVariableRowHeap));

        ASSERT(FixedSizeHeaplet::dataOffset() >= sizeof(FixedSizeHeaplet));
        ASSERT(PackedFixedSizeHeaplet::dataOffset() >= sizeof(PackedFixedSizeHeaplet));
        ASSERT(HugeHeaplet::dataOffset() >= sizeof(HugeHeaplet));

        memsize_t memory = (useLargeMemory ? largeMemory : smallMemory) * (unsigned __int64)0x100000U;
        const bool retainMemory = true; // remove the time releasing pages from the timing overhead
        initializeHeap(false, true, retainMemory, (unsigned)(memory / HEAP_ALIGNMENT_SIZE), 0, NULL);
        initAllocSizeMappings(defaultAllocSizes);
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
        //printf("\n----Begin DataBuffsActive=%d, DataBuffPages=%d ------ \n", dataBuffersActive.load(std::memory_order_relaxed), dataBufferPages.load(std::memory_order_relaxed));
        for (i = 0; i < 2046; i++)
            pages[i] = dm.allocate();
        //printf("\n----Mid 1 DataBuffsActive=%d, DataBuffPages=%d ------ \n", dataBuffersActive.load(), dataBufferPages.load());

        ASSERT(dataBufferPages.load()==PAGES(2046 * DATA_ALIGNMENT_SIZE, (HEAP_ALIGNMENT_SIZE- DATA_ALIGNMENT_SIZE)));
        pages[1022]->Release(); // release from first page
        pages[1022] = 0;  
        pages[2100] = dm.allocate(); // allocate from first page 
        //printf("\n----Mid 2 DataBuffsActive=%d, DataBuffPages=%d ------ \n", dataBuffersActive.load(), dataBufferPages.load());
        ASSERT(dataBufferPages.load(std::memory_order_relaxed)==PAGES(2046 * DATA_ALIGNMENT_SIZE, (HEAP_ALIGNMENT_SIZE- DATA_ALIGNMENT_SIZE)));
        pages[2101] = dm.allocate(); // allocate from a new page (third)
        //printf("\n----Mid 3 DataBuffsActive=%d, DataBuffPages=%d ------ \n", dataBuffersActive.load(), dataBufferPages.load());
        // Release all blocks, which releases all pages, except active one
        for (i = 0; i < 3000; i++)
            if (pages[i]) 
                pages[i]->Release();
        //printf("\n----End DataBuffsActive=%d, DataBuffPages=%d ------ \n", dataBuffersActive.load(), dataBufferPages.load());
        dm.allocate()->Release();
        ASSERT(dataBufferPages.load()==1);

        for (i = 0; i < 1022; i++)
            dm.allocate()->Release();
        dm.allocate()->Release();
        dm.allocate()->Release();
        ASSERT(dataBufferPages.load()==1);

        for (i = 0; i < 2000; i++)
            pages[i] = dm.allocate();
        for (i = 0; i < 1000; i++)
            pages[i]->Release();
        for (i = 0; i < 1000; i++)
            pages[i] = dm.allocate();
        ASSERT(dataBufferPages.load()==PAGES(2000 * DATA_ALIGNMENT_SIZE, (HEAP_ALIGNMENT_SIZE- DATA_ALIGNMENT_SIZE)));
        for (i = 0; i < 1999; i++)
            pages[i]->Release();
        pages[1999]->Release();
        dm.allocate()->Release();
        ASSERT(dataBufferPages.load()==1);

        dm.cleanUp();
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
            _heapHWM = heapHWM;
            _heapAllocated = heapAllocated;
            _heapUseHugePages = heapUseHugePages;
            _heapNotifyUnusedEachFree = heapNotifyUnusedEachFree;
            _heapNotifyUnusedEachBlock = heapNotifyUnusedEachBlock;
        }
        ~HeapPreserver()
        {
            heapBase = _heapBase;
            heapEnd = _heapEnd;
            heapBitmap = _heapBitmap;
            heapBitmapSize = _heapBitmapSize;
            heapTotalPages = _heapTotalPages;
            heapLWM = _heapLWM;
            heapHWM = _heapHWM;
            heapAllocated = _heapAllocated;
            heapUseHugePages = _heapUseHugePages;
            heapNotifyUnusedEachFree = _heapNotifyUnusedEachFree;
            heapNotifyUnusedEachBlock = _heapNotifyUnusedEachBlock;
        }
        char *_heapBase;
        char *_heapEnd;
        heap_t *_heapBitmap;
        unsigned _heapBitmapSize;
        unsigned _heapTotalPages;
        unsigned _heapLWM;
        unsigned _heapHWM;
        unsigned _heapAllocated;
        bool _heapUseHugePages;
        bool _heapNotifyUnusedEachFree;
        bool _heapNotifyUnusedEachBlock;
    };
    void initBitmap(unsigned size)
    {
        heapBase = (char *) (memsize_t) 0x80000000;
        heapBitmap = new heap_t[size];
        memset(heapBitmap, 0xff, size*sizeof(heap_t));
        heapBitmapSize = size;
        heapTotalPages = heapBitmapSize * HEAP_BITS;
        heapLWM = 0;
        heapHWM = heapBitmapSize;
        heapAllocated = 0;
    }

    void testBitmap()
    {
        HeapPreserver preserver;

        const unsigned bitmapSize = 32;
        initBitmap(bitmapSize);
        unsigned i;
        memsize_t minAddr = 0x80000000;
        memsize_t maxAddr = minAddr + bitmapSize * HEAP_BITS * HEAP_ALIGNMENT_SIZE;
        for (i=0; i < 100; i++)
        {
            ASSERT(suballoc_aligned(1, false)==(void *)(memsize_t)(minAddr + HEAP_ALIGNMENT_SIZE*i));
            ASSERT(suballoc_aligned(3, false)==(void *)(memsize_t)(maxAddr - (3*HEAP_ALIGNMENT_SIZE)*(i+1)));
        }
        for (i=0; i < 100; i+=2)
        {
            subfree_aligned((void *)(memsize_t)(minAddr + HEAP_ALIGNMENT_SIZE*i), 1);
            subfree_aligned((void *)(memsize_t)(maxAddr - (3*HEAP_ALIGNMENT_SIZE)*(i+1)), 3);
        }
        for (i=0; i < 100; i+=2)
        {
            ASSERT(suballoc_aligned(1, false)==(void *)(memsize_t)(minAddr + HEAP_ALIGNMENT_SIZE*i));
            ASSERT(suballoc_aligned(3, false)==(void *)(memsize_t)(maxAddr - (3*HEAP_ALIGNMENT_SIZE)*(i+1)));
        }
        for (i=0; i < 100; i++)
        {
            subfree_aligned((void *)(memsize_t)(minAddr + HEAP_ALIGNMENT_SIZE*i), 1);
            subfree_aligned((void *)(memsize_t)(maxAddr - 3*HEAP_ALIGNMENT_SIZE*(i+1)), 3);
        }

        // Try a realloc that can expand above only.
        void *t = suballoc_aligned(1, false);
        ASSERT(t==(void *)(memsize_t)(minAddr));
        void *r = subrealloc_aligned(t, 1, 50);
        ASSERT(r == t)
        void *t1 = suballoc_aligned(1, false);
        ASSERT(t1==(void *)(memsize_t)(minAddr + HEAP_ALIGNMENT_SIZE*50));
        subfree_aligned(r, 50);
        subfree_aligned(t1, 1);

        // Try a realloc that can expand below only.
        t = suballoc_aligned(2, false);
        ASSERT(t==(void *)(memsize_t)(maxAddr - 2*HEAP_ALIGNMENT_SIZE));
        r = subrealloc_aligned(t, 2, 50);
        ASSERT(r==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*50));
        t1 = suballoc_aligned(2, false);
        ASSERT(t1==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*52));
        subfree_aligned(r, 50);
        subfree_aligned(t1, 2);

        // Try a realloc that has to do both.
        t = suballoc_aligned(20, false);
        ASSERT(t==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*20));
        t1 = suballoc_aligned(20, false);
        ASSERT(t1==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*40));
        subfree_aligned(t, 20);
        r = subrealloc_aligned(t1, 20, 80);
        ASSERT(r==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*80));
        t1 = suballoc_aligned(2, false);
        ASSERT(t1==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*82));
        subfree_aligned(r, 80);
        subfree_aligned(t1, 2);

        // Try a realloc that can't quite manage it.
        t = suballoc_aligned(20, false);
        ASSERT(t==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*20));
        t1 = suballoc_aligned(20, false);
        ASSERT(t1==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*40));
        void * t2 = suballoc_aligned(20, false);
        ASSERT(t2==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*60));
        void *t3 = suballoc_aligned(20, false);
        ASSERT(t3==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*80));
        subfree_aligned(t, 20);
        subfree_aligned(t2, 20);
        r = subrealloc_aligned(t1, 20, 61);
        ASSERT(r==NULL);
        // Then one that just can
        r = subrealloc_aligned(t1, 20, 60);
        ASSERT(r==(void *)(memsize_t)(maxAddr - HEAP_ALIGNMENT_SIZE*60));
        subfree_aligned(r, 60);
        bool ok = subfree_aligned(t3, 20);
        ASSERT(ok);

        // Check some error cases
        ok = subfree_aligned((void*)0, 1);
        ASSERT(!ok);
        ok = subfree_aligned((void*)(minAddr + HEAP_ALIGNMENT_SIZE / 2), 1);
        ASSERT(!ok);
        ok = subfree_aligned((void*)(memsize_t)(minAddr + 20 * HEAP_ALIGNMENT_SIZE), 1);
        ASSERT(!ok);
        ok = subfree_aligned((void*)(memsize_t)(maxAddr - 2 * HEAP_ALIGNMENT_SIZE), 3);
        ASSERT(!ok);
        ok = subfree_aligned((void*)(memsize_t)0xbfe00000, 0);
        ASSERT(!ok);

        delete[] heapBitmap;
    }

#ifdef __64BIT__
    //Testing allocating bits that represent 1Tb of memory.  With 256K pages, that is simulating 4M pages.
    enum { maxBitmapThreads = 20, maxBitmapSize = (unsigned)(I64C(0xFFFFFFFFFF) / HEAP_ALIGNMENT_SIZE / HEAP_BITS) };      // Test larger range - in case we ever reduce the granularity
#else
    // Restrict heap sizes on 32-bit systems
    enum { maxBitmapThreads = 20, maxBitmapSize = (unsigned)(I64C(0xFFFFFFFF) / HEAP_ALIGNMENT_SIZE / HEAP_BITS) };      // 4Gb
#endif
    class BitmapAllocatorThread : public Thread
    {
    public:
        BitmapAllocatorThread(Semaphore & _sem, unsigned _size, unsigned _numThreads) : Thread("AllocatorThread"), sem(_sem), size(_size), numThreads(_numThreads)
        {
        }

        int run()
        {
            unsigned numBitmapIter = (maxBitmapSize * HEAP_BITS / size) / numThreads;
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
        const unsigned numThreads;
        volatile memsize_t final = 0;
    };
    void testBitmapThreading(unsigned size, unsigned numThreads)
    {
        HeapPreserver preserver;

        initBitmap(maxBitmapSize);
        heapNotifyUnusedEachFree = false; // prevent calls to map out random chunks of memory!
        heapNotifyUnusedEachBlock = false;

        Semaphore sem;
        BitmapAllocatorThread * threads[maxBitmapThreads];
        for (unsigned i1 = 0; i1 < numThreads; i1++)
            threads[i1] = new BitmapAllocatorThread(sem, size, numThreads);
        for (unsigned i2 = 0; i2 < numThreads; i2++)
            threads[i2]->start();

        unsigned startTime = msTick();
        sem.signal(numThreads);
        for (unsigned i3 = 0; i3 < numThreads; i3++)
            threads[i3]->join();
        unsigned endTime = msTick();

        for (unsigned i4 = 0; i4 < numThreads; i4++)
            threads[i4]->Release();
        DBGLOG("Time taken for bitmap threading(%d) = %d", size, endTime-startTime);

        unsigned totalPages;
        unsigned freePages;
        unsigned maxBlock;
        memstats(totalPages, freePages, maxBlock);
        ASSERT(totalPages == maxBitmapSize * HEAP_BITS);
        unsigned numAllocated = ((maxBitmapSize * HEAP_BITS / size) / numThreads) * numThreads * size;
        ASSERT(freePages == maxBitmapSize * HEAP_BITS - numAllocated);

        delete[] heapBitmap;
    }
    void testBitmapThreading()
    {
        testBitmapThreading(1, 1);
        testBitmapThreading(3, 1);
        testBitmapThreading(11, 1);
        testBitmapThreading(1, maxBitmapThreads);
        testBitmapThreading(3, maxBitmapThreads);
        testBitmapThreading(11, maxBitmapThreads);
    }

    void testCompressSize()
    {
#ifdef __64BIT__
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(0), 0U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT), 1U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0xFF), 1U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x100), 2U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0xFFFF), 2U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x10000), 3U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0xFFFFFF), 3U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x1000000), 4U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0xFFFFFFFF), 4U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x100000000), 5U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0xFFFFFFFFFF), 5U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x10000000000), 6U);
        CPPUNIT_ASSERT_EQUAL(getCompressedSize(PACKED_ALIGNMENT*0x1FFFFFFFFFFF), 6U);
#endif
        HeapPointerCompressor compressor;
        const unsigned numAllocs = 100;
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
        memsize_t max = numAllocs * compressor.getSize();
        byte * memory = (byte *)malloc(max + 1);
        void * * ptrs = new void * [numAllocs];
        memory[max] = 0xcb;
        for (unsigned i = 0; i < numAllocs; i++)
        {
            void * next = rm1->allocate(i*7, 0);
            ptrs[i] = next;
            compressor.compress(memory + i * compressor.getSize(), next);
            ASSERT(compressor.decompress(memory + i * compressor.getSize()) == next);
        }
        for (unsigned i1 = 0; i1 < numAllocs; i1++)
        {
            ASSERT(compressor.decompress(memory + i1 * compressor.getSize()) == ptrs[i1]);
            ReleaseRoxieRow(ptrs[i1]);
        }
        delete [] ptrs;
        free(memory);
    }
    void testHuge()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
        ReleaseRoxieRow(rm1->allocate(1800000, 0));
        ASSERT(rm1->numPagesAfterCleanup(false)==0); // page should be freed even if force not specified
        ASSERT(rm1->getMemoryUsage()== PAGES(1800000+sizeof(HugeHeaplet), HEAP_ALIGNMENT_SIZE)*HEAP_ALIGNMENT_SIZE);
    }

    void testSizes()
    {
        ASSERT(ChunkedHeaplet::dataOffset() % CACHE_LINE_SIZE == 0);
        ASSERT(HugeHeaplet::dataOffset() % CACHE_LINE_SIZE == 0);
        ASSERT(FixedSizeHeaplet::chunkHeaderSize == 8);
        ASSERT(PackedFixedSizeHeaplet::chunkHeaderSize == 4);  // NOTE - this is NOT 8 byte aligned, so can't safely be used to allocate ptr arrays

        // Check some alignments
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
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
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
        ReleaseRoxieRow(rm1->allocate(1000, 0));
        ASSERT(rm1->numPagesAfterCleanup(true)==0);
        ASSERT(rm1->getMemoryUsage()==HEAP_ALIGNMENT_SIZE);

        void *r1 = rm1->allocate(1000, 0);
        void *r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm1->numPagesAfterCleanup(true)==0);
        ASSERT(rm1->getMemoryUsage()==HEAP_ALIGNMENT_SIZE);


        Owned<IRowManager> rm2 = createRowManager(0, NULL, logctx, NULL, false);
        ReleaseRoxieRow(rm2->allocate(4000000, 0));
        ASSERT(rm2->numPagesAfterCleanup(true)==0);
        ASSERT(rm2->getMemoryUsage()==PAGES(4000000+sizeof(HugeHeaplet), HEAP_ALIGNMENT_SIZE)*HEAP_ALIGNMENT_SIZE);

        r1 = rm2->allocate(4000000, 0);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm2->numPagesAfterCleanup(true)==0);
        ASSERT(rm2->getMemoryUsage()==2*PAGES(4000000+sizeof(HugeHeaplet), HEAP_ALIGNMENT_SIZE)*HEAP_ALIGNMENT_SIZE);

        for (unsigned d = 0; d < 50; d++)
        {
            Owned<IRowManager> rm3 = createRowManager(0, NULL, logctx, NULL, false);
            ReleaseRoxieRow(rm3->allocate(HEAP_ALIGNMENT_SIZE - d + 10, 0));
            ASSERT(rm3->numPagesAfterCleanup(true)==0);
        }

        // test leak reporting does not crash....
        Owned<IRowManager> rm4 = createRowManager(0, NULL, logctx, NULL, false);
        rm4->allocate(4000000, 0);
        rm4.clear();

        Owned<IRowManager> rm5 = createRowManager(0, NULL, logctx, NULL, false);
        rm5->allocate(4000, 0);
        rm5.clear();
    }

    void testFixed()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
        Owned<IRowManager> rm2 = createRowManager(0, NULL, logctx, NULL, false);
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
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
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
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
        rm1->setMemoryLimit(20*HEAP_ALIGNMENT_SIZE);
        try
        {
            unsigned i = 0;
            for (;;)
            {
                void * ret = rm1->allocate(i++, 0);
                ASSERT(ret != NULL);
            }
        }
        catch (IException *E)
        {
            ASSERT(E->errorCode() == ROXIEMM_MEMORY_LIMIT_EXCEEDED);
            E->Release();
        }
        ASSERT(rm1->numPagesAfterCleanup(true)==20);
    }
    void testCycling()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL, false);
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
        memsize_t capacity = RoxieRowCapacity(alloc1);
        memset(alloc1, 99, capacity);
        void * alloc2 = rm->allocate(capacity, 0);
        CPPUNIT_ASSERT_EQUAL(RoxieRowCapacity(alloc2), capacity);
        CPPUNIT_ASSERT_EQUAL(rm->numPagesAfterCleanup(true), expectedPages);
        memset(alloc2, 99, capacity);
        ReleaseRoxieRow(alloc1);
        ReleaseRoxieRow(alloc2);
    }

    void testAllocSize()
    {
        Owned<IRowManager> rm = createRowManager(0, NULL, logctx, NULL, false);
        testCapacity(rm, 1);
        testCapacity(rm, 32);
        testCapacity(rm, 32768, PAGES(2 * 32768, (HEAP_ALIGNMENT_SIZE- sizeof(FixedSizeHeaplet))));
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

        CountingRowAllocatorCache() : counter(0) { }
        virtual unsigned getActivityId(unsigned cacheId) const { return 0; }
        virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const { return out.append(cacheId); }
        virtual void onDestroy(unsigned cacheId, void *row) const { counter++; }
        virtual void onClone(unsigned cacheId, void *row) const { counter--; }
        virtual void checkValid(unsigned cacheId, const void *row) const { }

        void clear() { counter = 0; }
        unsigned getCounter() { return counter.load(); };

        mutable std::atomic_uint counter;
    };
    enum { numCasThreads = 20, numCasIter = 100, numCasAlloc = 500 };
    class CasAllocatorThread : public Thread
    {
    public:
        CasAllocatorThread(Semaphore & _sem, IRowManager * _rm, unsigned _id) : Thread("AllocatorThread"), sem(_sem), rm(_rm), cost(0), id(_id)
        {
        }

        virtual void * allocate() = 0;
        virtual void * finalize(void * ptr) = 0;

        void setCost(unsigned _cost) { cost = _cost; }

        int run()
        {
            SimpleRowBuffer saved(rm, cost, id);
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
                aborted = true;
                if (rm)
                    rm->removeRowBuffer(&saved);
                throw;
            }
            return 0;
        }
        bool queryAborted() const { return aborted; }
    protected:
        Semaphore & sem;
        IRowManager * rm;
        unsigned cost;
        unsigned id;
        bool aborted = false;
    };
    unsigned runCasTest(const char * title, Semaphore & sem, CasAllocatorThread * threads[], bool * wasAborted = nullptr)
    {
        for (unsigned i2 = 0; i2 < numCasThreads; i2++)
        {
            threads[i2]->start();
        }

        bool aborted = false;
        unsigned startTime = msTick();
        sem.signal(numCasThreads);
        for (unsigned i3 = 0; i3 < numCasThreads; i3++)
        {
            threads[i3]->join();
            if (threads[i3]->queryAborted())
                aborted = true;
        }
        unsigned endTime = msTick();
        if (wasAborted)
            *wasAborted = aborted;

        for (unsigned i4 = 0; i4 < numCasThreads; i4++)
            threads[i4]->Release();
        unsigned timeTaken = endTime-startTime;
        DBGLOG("Time taken for %s cas = %d", title, timeTaken);
        return timeTaken;
    }
    class HeapletCasAllocatorThread : public CasAllocatorThread
    {
    public:
        HeapletCasAllocatorThread(FixedSizeHeaplet * _heaplet, Semaphore & _sem, IRowManager * _rm, unsigned _id) : CasAllocatorThread(_sem, _rm, _id), heaplet(_heaplet)
        {
        }

        virtual void * allocate() { return heaplet->testAllocate(ACTIVITY_FLAG_ISREGISTERED|0); }
        virtual void * finalize(void * ptr) { heaplet->_setDestructorFlag(ptr); return ptr; }

    protected:
        FixedSizeHeaplet * heaplet;
    };
    class NullCasAllocatorThread : public CasAllocatorThread
    {
    public:
        NullCasAllocatorThread(Semaphore & _sem, IRowManager * _rm, unsigned _id) : CasAllocatorThread(_sem, _rm, _id)
        {
        }

        virtual void * allocate() { return temp; }
        virtual void * finalize(void * ptr) { return ptr; }

    protected:
        char temp[32];
    };
    void testHeapletCas()
    {
        unsigned numThreads = FixedSizeHeaplet::dataAreaSize() / ((numCasAlloc+1) * 32);
        if (numThreads > numCasThreads)
            numThreads = numCasThreads;

        //Because this is allocating from a single heaplet check if it can overflow the memory
        if (numThreads == 0)
            return;

        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        CountingRowAllocatorCache rowCache;
        void * memory = suballoc_aligned(1, true);
        assertex(memory);

        unsigned heapFlags = 0;
        CFixedChunkedHeap dummyHeap((CChunkingRowManager*)rowManager.get(), logctx, &rowCache, 32, 0, SpillAllCost);
        FixedSizeHeaplet * heaplet = new (memory) FixedSizeHeaplet(&dummyHeap, &rowCache, 32, heapFlags);
        heaplet->precreateFreeChain();
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numThreads; i1++)
            threads[i1] = new HeapletCasAllocatorThread(heaplet, sem, rowManager, i1);
        for (unsigned i2 = numThreads; i2 < numCasThreads; i2++)
            threads[i2] = new NullCasAllocatorThread(sem, rowManager, i2);

        VStringBuffer label("heaplet.%u", numThreads);
        runCasTest(label.str(), sem, threads);

        delete heaplet;
        ASSERT(rowCache.counter == 2 * numThreads * numCasIter * numCasAlloc);
    }
    class FixedCasAllocatorThread : public CasAllocatorThread
    {
    public:
        FixedCasAllocatorThread(IFixedRowHeap * _rowHeap, Semaphore & _sem, IRowManager * _rm, unsigned _id) : CasAllocatorThread(_sem, _rm, _id), rowHeap(_rowHeap)
        {
        }
        ~FixedCasAllocatorThread()
        {
            rowHeap->emptyCache();
        }

        virtual void * allocate() { return rowHeap->allocate(); }
        virtual void * finalize(void * ptr) { return rowHeap->finalizeRow(ptr); }

    protected:
        Linked<IFixedRowHeap> rowHeap;
    };
    void testOldFixedCas()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|RHFoldfixed);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager, i1);

        runCasTest("old fixed allocator", sem, threads);
        ASSERT(rowCache.counter == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    void testSharedFixedCas(const char * variant, unsigned flags)
    {
        CountingRowAllocatorCache rowCache;
        Owned<IFixedRowHeap> rowHeap;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        //For this test the row heap is assign to a variable that will be destroyed after the manager, to ensure that works.
        rowHeap.setown(rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|flags));
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager, i1);

        VStringBuffer title("shared %s allocator", variant);
        runCasTest(title, sem, threads);
        ASSERT(rowCache.counter == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    unsigned doTestFixedCas(const char * variant, unsigned flags)
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, ACTIVITY_FLAG_ISREGISTERED|0, RHFunique|RHFhasdestructor|flags);
            threads[i1] = new FixedCasAllocatorThread(rowHeap, sem, rowManager, i1);
        }

        VStringBuffer title("separate %s allocator", variant);
        unsigned timeTaken = runCasTest(title.str(), sem, threads);
        ASSERT(rowCache.counter == 2 * numCasThreads * numCasIter * numCasAlloc);
        return timeTaken;
    }
    void testFixedCas(const char * variant, unsigned flags)
    {
        const unsigned maxIters = 5;
        unsigned timings[maxIters];
        for (unsigned i=0; i < maxIters; i++)
            timings[i] = doTestFixedCas(variant, flags);
        std::sort(timings, timings+maxIters);
        DBGLOG("Median %s = %u", variant, timings[maxIters/2]);
    }
    class GeneralCasAllocatorThread : public CasAllocatorThread
    {
    public:
        GeneralCasAllocatorThread(IRowManager * _rowManager, Semaphore & _sem, unsigned _id) : CasAllocatorThread(_sem, _rowManager, _id), rowManager(_rowManager)
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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new GeneralCasAllocatorThread(rowManager, sem, i1);

        runCasTest("general allocator", sem, threads);
        ASSERT(rowCache.counter == 2 * numCasThreads * numCasIter * numCasAlloc);
    }
    class VariableCasAllocatorThread : public CasAllocatorThread
    {
    public:
        VariableCasAllocatorThread(IRowManager * _rowManager, Semaphore & _sem, unsigned _seed, unsigned _id) : CasAllocatorThread(_sem, _rowManager, _id), rowManager(_rowManager), seed(_seed)
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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new VariableCasAllocatorThread(rowManager, sem, i1, i1);

        runCasTest("variable allocator", sem, threads);
        ASSERT(rowCache.counter == 2 * numCasThreads * numCasIter * numCasAlloc);
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
        Owned<IRowManager> rowManager = createRowManager(1, NULL, logctx, NULL, false);
        CChunkingRowManager * managerObject = static_cast<CChunkingRowManager *>(rowManager.get());
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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
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
        testSharedFixedCas("fixed", 0);
        testSharedFixedCas("fixed scan", RHFscanning);
        //NB: blocked allocators cannot be shared
        testFixedCas("fixed", 0);
        testFixedCas("packed", RHFpacked);
        testFixedCas("packed scan", RHFpacked|RHFscanning);
        testFixedCas("packed blocked", RHFpacked|RHFblocked);
        testFixedCas("packed blocked scan", RHFpacked|RHFscanning|RHFblocked);
        testFixedCas("packed delayed scan", RHFpacked|RHFscanning|RHFdelayrelease);
        testFixedCas("packed blocked", RHFpacked|RHFblocked);
        testFixedCas("packed delayed blocked scan", RHFpacked|RHFscanning|RHFblocked|RHFdelayrelease);
        testGeneralCas();
    }

    void testReleaseAll(IFixedRowHeap * heap, CountingRowAllocatorCache & rowCache)
    {
        rowCache.clear();

        const unsigned numRows = 200;
        const void * rows[numRows];
        for (unsigned i=0; i < numRows; i++)
            rows[i] = heap->finalizeRow(heap->allocate());

        //test that calling releaseAllRows() calls all the destructors
        CPPUNIT_ASSERT_EQUAL(0U, rowCache.getCounter());
        heap->releaseAllRows();
        CPPUNIT_ASSERT_EQUAL(numRows, rowCache.getCounter());

        //Check the heap is still in a valid state
        //Test again - releasing half the rows before calling releaseAllRows() to free any that remain
        rowCache.clear();
        for (unsigned i2=0; i2 < numRows; i2++)
            rows[i2] = heap->finalizeRow(heap->allocate());

        for (unsigned i3=0; i3 < numRows; i3 += 2)
            ::ReleaseRoxieRow(rows[i3]);

        CPPUNIT_ASSERT_EQUAL(numRows / 2, rowCache.getCounter());
        heap->releaseAllRows();
        CPPUNIT_ASSERT_EQUAL(numRows, rowCache.getCounter());
    }

    void testReleaseAll(CountingRowAllocatorCache & rowCache, IRowManager * rowManager, unsigned flags)
    {
        Owned<IFixedRowHeap> heap = rowManager->createFixedRowHeap(100, ACTIVITY_FLAG_ISREGISTERED|0, flags);
        testReleaseAll(heap, rowCache);
    }

    void testReleaseAll()
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);

        //Test releaseAll on various different heap variants
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFpacked);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFscanning);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFpacked|RHFscanning);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFblocked);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFpacked|RHFblocked);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFscanning|RHFdelayrelease);
        testReleaseAll(rowCache, rowManager, RHFhasdestructor|RHFunique|RHFpacked|RHFscanning|RHFdelayrelease);
    }

    void testCallback(unsigned numPerPage, unsigned pages, unsigned spillPages, double scale, unsigned flags)
    {
        CountingRowAllocatorCache rowCache;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, &rowCache, false);
        rowManager->setMemoryLimit(pages * numCasThreads * HEAP_ALIGNMENT_SIZE, spillPages * numCasThreads * HEAP_ALIGNMENT_SIZE);
        rowManager->setMemoryCallbackThreshold((unsigned)-1);
        rowManager->setCallbackOnThread(true);
        rowManager->setMinimizeFootprint(true, true);
        rowManager->setReleaseWhenModifyCallback(true, true);

        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        size32_t allocSize = (HEAP_ALIGNMENT_SIZE - 0x200) / numPerPage;
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
        {
            Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(allocSize, ACTIVITY_FLAG_ISREGISTERED|0, RHFhasdestructor|flags);
            FixedCasAllocatorThread * cur = new FixedCasAllocatorThread(rowHeap, sem, rowManager, i1);
            cur->setCost((unsigned)(i1*scale)+1);
            threads[i1] = cur;
        }
        VStringBuffer title("callback(%u,%u,%u,%f,%x)", numPerPage,pages, spillPages, scale, flags);
        bool aborted = true;
        runCasTest(title.str(), sem, threads, &aborted);

        //This test can very occasionally fail if each thread has 1 single row from a different page buffered, and a buffer allocated from a different page
        if (aborted)
            CPPUNIT_ASSERT(2 * numCasThreads * numCasIter * numCasAlloc != (int)rowCache.counter);
        else
            CPPUNIT_ASSERT_EQUAL(2 * numCasThreads * numCasIter * numCasAlloc, (int)rowCache.counter);
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
        testCallbacks2();
    }
    const static size32_t compactingAllocSize = 32;
    void testCompacting(IRowManager * rowManager, IFixedRowHeap * rowHeap, unsigned numRows, unsigned milliFraction)
    {
        const void * * rows = new const void * [numRows];
        unsigned beginTime = msTick();
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
        unsigned rowsPerPage = (HEAP_ALIGNMENT_SIZE - FixedSizeHeaplet::dataOffset()) / compactingAllocSize;
        unsigned numPagesBefore = rowManager->numPagesAfterCleanup(false);
        unsigned expectedPages = (numRowsLeft + rowsPerPage-1)/rowsPerPage;
        CPPUNIT_ASSERT_EQUAL(numPagesFull, numPagesBefore);
        unsigned startTime = msTick();
        memsize_t compacted = rowManager->compactRows(numRows, rows);
        unsigned endTime = msTick();
        unsigned numPagesAfter = rowManager->numPagesAfterCleanup(false);

        for (unsigned i3 = 0; i3 < numRows; i3++)
        {
            ReleaseClearRoxieRow(rows[i3]);
        }

        unsigned finalTime = msTick();
        if ((compacted>0) != (numPagesBefore != numPagesAfter))
            DBGLOG("Compacted not returned correctly %u %u=%u", (unsigned)compacted, numPagesBefore, numPagesAfter);
        CPPUNIT_ASSERT_EQUAL(compacted != 0, (numPagesBefore != numPagesAfter));
        DBGLOG("Compacting %d[%d] (%d->%d [%d] cf %d) Before: Time taken %u [%u]", numRows, milliFraction, numPagesBefore, numPagesAfter, (unsigned)compacted, expectedPages, endTime-startTime, finalTime-beginTime);
        ASSERT(numPagesAfter == expectedPages);
        delete [] rows;
    }
    void testRepeatCompacting(IRowManager * rowManager, IFixedRowHeap * rowHeap, unsigned maxRows, unsigned milliFraction, unsigned numLocked)
    {
        unsigned numRows = maxRows;
        const void * * rows = new const void * [numRows];
        for (unsigned i1 = 0; i1 < numRows; i1++)
            rows[i1] = rowHeap->allocate();

        //shuffle
        unsigned i=numRows;
        while (i>1) {
            unsigned j = getRandom()%i;  // NB i is correct here
            i--;
            const void * t = rows[j];
            rows[j] = rows[i];
            rows[i] = t;
        }

        const void * * locked = new const void * [numLocked];
        for (unsigned i=0; i < numLocked; i++)
        {
            LinkRoxieRow(rows[i]);
            locked[i] = rows[i];
        }

        for (unsigned pass=0; pass < 8; pass++)
        {
            unsigned numPagesFull = rowManager->numPagesAfterCleanup(true);
            unsigned numRowsLeft = 0;
            unsigned target=0;
            for (unsigned i2 = 0; i2 < numRows; i2++)
            {
                const void * row = rows[i2];
                rows[i2] = nullptr;
                if ((i2 >= numLocked) && (i2 * 7) % 1000 >= milliFraction)
                {
                    ReleaseRoxieRow(row);
                }
                else
                {
                    rows[target++] = row;
                    numRowsLeft++;
                }
            }

            //NOTE: The efficiency of the packing does depend on the row order, so ideally this would test multiple orderings
            //of the array
            unsigned rowsPerPage = (HEAP_ALIGNMENT_SIZE - FixedSizeHeaplet::dataOffset()) / compactingAllocSize;
            unsigned numPagesBefore = rowManager->numPagesAfterCleanup(false);
            unsigned expectedPages = (numRowsLeft + rowsPerPage-1)/rowsPerPage;
            //CPPUNIT_ASSERT_EQUAL(numPagesFull, numPagesBefore);
            unsigned startTime = msTick();
            memsize_t compacted = rowManager->compactRows(numRowsLeft, rows);
            unsigned endTime = msTick();
            unsigned numPagesAfter = rowManager->numPagesAfterCleanup(false);
            if ((compacted>0) != (numPagesBefore != numPagesAfter))
                DBGLOG("Compacted not returned correctly");
            CPPUNIT_ASSERT_EQUAL(compacted != 0, (numPagesBefore != numPagesAfter));
            DBGLOG("Compacting %d[%d] (%d->%d [%d] cf %d) locked(%u) Before: Time taken %u", numRows, milliFraction, numPagesBefore, numPagesAfter, (unsigned)compacted, expectedPages, numLocked, endTime-startTime);

            if (numLocked == 0)
                CPPUNIT_ASSERT_EQUAL(expectedPages, numPagesAfter);

            //Now fill the rest up with new rows
            for (unsigned i=numRowsLeft; i < numRows; i++)
                rows[i] = rowHeap->allocate();
        }

        for (unsigned i=0; i < numLocked; i++)
            ReleaseRoxieRow(locked[i]);

        for (unsigned i3 = 0; i3 < numRows; i3++)
        {
            ReleaseClearRoxieRow(rows[i3]);
        }
        delete [] rows;
    }
    void testCompacting()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);

        Owned<IFixedRowHeap> rowHeap1 = rowManager->createFixedRowHeap(compactingAllocSize-FixedSizeHeaplet::chunkHeaderSize, 0, 0);
        Owned<IFixedRowHeap> rowHeap2 = rowManager->createFixedRowHeap(compactingAllocSize-PackedFixedSizeHeaplet::chunkHeaderSize, 0, RHFpacked);

        unsigned rowsPerPage = (HEAP_ALIGNMENT_SIZE - FixedSizeHeaplet::dataOffset()) / compactingAllocSize;
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
            testRepeatCompacting(rowManager, rowHeap1, maxRows, percent*10, 0);
            testRepeatCompacting(rowManager, rowHeap2, maxRows, percent*10, 0);
            testRepeatCompacting(rowManager, rowHeap2, maxRows, percent*10, 50);
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
        Owned<IRowManager> rowManager = createRowManager(2 * HEAP_ALIGNMENT_SIZE, NULL, logctx, NULL, false);

        //The lower cost allocator allocates an extra row when it is called to free all its rows.
        //this will only succeed if the higher cost allocator is then called to free its data.
        NastyCallbackBlockAllocator alloc1(rowManager, bigRowSize, 10, 1);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 20, 2);

        alloc1.allocate();
        alloc2.allocate();
        OwnedRoxieRow tempRow = rowManager->allocate(bigRowSize, 0);
    }
    void testRecursiveCallbacks2()
    {
        const size32_t bigRowSize = HEAP_ALIGNMENT_SIZE * 2 / 3;
        Owned<IRowManager> rowManager = createRowManager(2 * HEAP_ALIGNMENT_SIZE, NULL, logctx, NULL, false);

        //Both allocators allocate extra memory when they are requested to free.  Ensure that an exception
        //is thrown instead of the previous stack fault.
        NastyCallbackBlockAllocator alloc1(rowManager, bigRowSize, 10, 1);
        NastyCallbackBlockAllocator alloc2(rowManager, bigRowSize, 20, 2);

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
    void testFragmentCallbacks(IRowManager * rowManager, unsigned numPages)
    {
        //Allocate rows, but only free a proportion in the callbacks
        TestingRowBuffer buff1(1, 1);
        TestingRowBuffer buff2(2, 2);

        rowManager->addRowBuffer(&buff2);
        unsigned numPerPage = 32;
        unsigned chunkSize = FixedSizeHeaplet::dataAreaSize() / numPerPage;
        unsigned allocSize = chunkSize-16;
        unsigned proportion = 4;
        unsigned numAllocations = numPerPage * (proportion - 1) * numPages;
        for (unsigned i=0; i < numAllocations; i++)
        {
            const void * row = rowManager->allocate(allocSize, 0);
            if ((i % proportion) == 0)
                buff1.addRow(row);
            else
                buff2.addRow(row);
        }
        buff2.freeBufferedRows(false);
        rowManager->removeRowBuffer(&buff2);
    }
    void testCallbacks2()
    {
        //Test allocating within the heap when a limit is set on the row manager
        {
            Owned<IRowManager> rowManager = createRowManager(HEAP_ALIGNMENT_SIZE, NULL, logctx, NULL, false);
            testFragmentCallbacks(rowManager, 1);
        }

        //Test allocating within the heap when the limit is the number of pages
        {
            HeapPreserver preserver;
            adjustHeapSize(HEAP_BITS);
            Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
            testFragmentCallbacks(rowManager, HEAP_BITS);
        }
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
        Owned<IRowManager> rowManager = createRowManager(1, NULL, logctx, NULL, false);

        SimpleCallbackBlockAllocator alloc1(rowManager, bigRowSize, 20, 1);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 10, 2);

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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);

        const memsize_t bigRowSize = HEAP_ALIGNMENT_SIZE * (heapTotalPages * 2 / 3);
        SimpleCallbackBlockAllocator alloc1(rowManager, bigRowSize, 20, 1);
        SimpleCallbackBlockAllocator alloc2(rowManager, bigRowSize, 10, 2);

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

    void testRegisterCallbacks1(unsigned numCallbacks, unsigned numCosts, unsigned numIter, unsigned numIds, unsigned numSlaves)
    {
        TestingRowBuffer * * callbacks = new TestingRowBuffer *[numCallbacks];

        for (unsigned i=0; i < numCallbacks; i++)
            callbacks[i] = new TestingRowBuffer(i % numCosts, i % (numCosts * numIds));

        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        unsigned startTime = msTick();
        for (unsigned iter=0; iter < numIter; iter++)
        {
            for (unsigned cur=0; cur < numCallbacks; cur++)
                rowManager->addRowBuffer(callbacks[cur]);

            for (unsigned cur2=0; cur2 < numCallbacks; cur2++)
                rowManager->removeRowBuffer(callbacks[cur2]);
        }
        unsigned endTime = msTick();
        unsigned elapsed = endTime - startTime;
        unsigned ns = (unsigned)(((unsigned __int64)elapsed * 1000000) / (numCallbacks * numIter));
        DBGLOG("Register/Unregister %u*%u (%u,%u) = %ums (%uns)", numCallbacks, numIter, numCosts, numIds, elapsed, ns);

        for (unsigned i2=0; i2 < numCallbacks; i2++)
            delete callbacks[i2];
        delete [] callbacks;
    }
    void testRegisterCallbacks()
    {
        testRegisterCallbacks1(4, 20, 20000, 1, 1);
        testRegisterCallbacks1(40, 20, 2000, 1, 10);
        testRegisterCallbacks1(400, 20, 200, 1, 1);
        testRegisterCallbacks1(4000, 20, 20, 1, 10);
    }
    void testGlobalCallbacks(unsigned numSlaves, unsigned numIter)
    {
        const memsize_t allMemory = HEAP_ALIGNMENT_SIZE * numSlaves;
        const memsize_t halfPage = HEAP_ALIGNMENT_SIZE / 2;
        const memsize_t allMemoryAlloc = allMemory - halfPage;
        IRowManager * * slaveManagers = new IRowManager * [numSlaves];
        SimpleCallbackBlockAllocator * * allocators = new SimpleCallbackBlockAllocator * [numSlaves];
        Owned<IRowManager> globalManager = createGlobalRowManager(allMemory, allMemory, numSlaves, NULL, logctx, NULL, NULL, false);
        for (unsigned i1 = 0; i1 < numSlaves; i1++)
        {
            slaveManagers[i1] = globalManager->querySlaveRowManager(i1);
            allocators[i1] = new SimpleCallbackBlockAllocator(slaveManagers[i1], HEAP_ALIGNMENT_SIZE / 16, 100, 0);
        }

        SimpleCallbackBlockAllocator globalAllocator(globalManager, allMemoryAlloc, 100, 0);

        unsigned startTime = msTick();
        //Check that each slave can allocate a block of memory
        {
            for (unsigned i=0; i < numSlaves; i++)
                allocators[i]->allocate();
        }

        //check that global can allocate and cause each of the slaves to spill
        {
            globalAllocator.allocate();
            ASSERT(globalAllocator.hasRow());
            for (unsigned i=0; i < numSlaves; i++)
                ASSERT(!allocators[i]->hasRow());
        }

        //Check that slave can allocate and cause global to spill
        for (unsigned iter = 0; iter < numIter; iter++)
        {
            for (unsigned i=0; i < numSlaves; i++)
            {
                allocators[i]->allocate();
                ASSERT(!globalAllocator.hasRow());
                globalAllocator.allocate();
                ASSERT(!allocators[i]->hasRow());
            }
        }
        unsigned endTime = msTick();

        //check that a slave cannot cause other slaves to spill.
        {
            OwnedRoxieRow globalAllocation = globalManager->allocate(allMemoryAlloc - HEAP_ALIGNMENT_SIZE, 0);
            ASSERT(!globalAllocator.hasRow());

            allocators[numSlaves-1]->allocate();
            ASSERT(allocators[numSlaves-1]->hasRow());

            try
            {
                allocators[0]->allocate();
                ASSERT(!"Should have failed to allocate");
            }
            catch (IException * e)
            {
                e->Release();
            }
        }

        delete [] slaveManagers;

        for (unsigned i3 = 0; i3 < numSlaves; i3++)
            delete allocators[i3];
        delete [] allocators;

        unsigned elapsed = endTime - startTime;
        DBGLOG("Slave spill (%u,%u) = %ums", numSlaves, numIter, elapsed);
    }
    void testGlobalCallbacks()
    {
        testGlobalCallbacks(2, 1000);
        testGlobalCallbacks(20, 100);
        testGlobalCallbacks(200, 10);
    }

    //Useful to add to the test list to see what is leaking pages.
    void testDump()
    {
        dumpHeapState();
    }
    void testResize()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        memsize_t maxMemory = heapTotalPages * HEAP_ALIGNMENT_SIZE;
        memsize_t wasted = 0;

        std::unique_ptr<void *[]> pages(new void * [heapTotalPages]);
        //Allocate a whole set of 1 page allocations
        unsigned scale = 1;
        memsize_t curSize =  HEAP_ALIGNMENT_SIZE * scale - HugeHeaplet::dataOffset();
        for (unsigned i = 0; i < heapTotalPages; i += scale)
        {
            pages[i] = rowManager->allocate(curSize, 0);
        }

        //Free half the pages, and resize the others to fill the space, Repeat until we only have a single page.
        while (scale * 2 <= heapTotalPages)
        {
            unsigned newScale = scale * 2;
            memsize_t newSize =  HEAP_ALIGNMENT_SIZE * newScale - HugeHeaplet::dataOffset();
            for (unsigned i = 0; i + newScale <= heapTotalPages; i += newScale)
            {
                ReleaseRoxieRow(pages[i+scale]);
                pages[i+scale] = 0;

                memsize_t newCapacity;
                rowManager->resizeRow(newCapacity, pages[i], 100, newSize, 0);
            }
            scale = newScale;
        }

        //Half the pages, double them, halve them and then allocate a block to fill the space, Repeat until we have allocated single pages.
        while (scale != 1)
        {
            memsize_t curSize =  HEAP_ALIGNMENT_SIZE * scale - HugeHeaplet::dataOffset();
            memsize_t nextSize =  HEAP_ALIGNMENT_SIZE * (scale / 2) - HugeHeaplet::dataOffset();
            for (unsigned i2 = 0; i2 + scale <= heapTotalPages; i2 += scale)
            {
                memsize_t newCapacity;
                //Shrink
                rowManager->resizeRow(newCapacity, pages[i2], 100, nextSize, 0);

                //Expand
                rowManager->resizeRow(newCapacity, pages[i2], 100, curSize, 0);

                //Shrink
                rowManager->resizeRow(newCapacity, pages[i2], 100, nextSize, 0);

                pages[i2+scale/2] = rowManager->allocate(nextSize, 0);
            }

            scale = scale / 2;
        }

        for (unsigned i = 0; i < heapTotalPages; i += 1)
        {
            ReleaseRoxieRow(pages[i]);
        }

        CPPUNIT_ASSERT_EQUAL(rowManager->numPagesAfterCleanup(true), 0U);
    }
    class ResizeCallback : public IRowResizeCallback
    {
    public:
        virtual void lock() { cs.enter(); }
        virtual void unlock() { cs.leave(); }
        virtual void update(memsize_t capacity, void * ptr) { max = capacity; rows = (const void * *)ptr; }
        virtual void atomicUpdate(memsize_t capacity, void * ptr) { lock(); update(capacity, ptr); unlock(); }

        bool failed = false;
        memsize_t max = 0;
        std::atomic<memsize_t> count = {0};
        const void * * rows = nullptr;
        CriticalSection cs;
    };
    const static unsigned maxAlloc = 1000;
    const static unsigned step = 10;

    class ResizeThread : public Thread
    {
    public:
        ResizeThread(ResizeCallback & _callback, IRowManager * _rm) : Thread("resizeThread"), callback(_callback), rm(_rm)
        {
        }

        int run()
        {
            for (unsigned i=0; i < maxAlloc; i++)
            {
                if (i % step == 0)
                    rm->resizeRow(callback.rows, i * sizeof(void *), (i+step)*sizeof(void*), 0, 0, callback);

                callback.rows[i] = rm->allocate(20, 0);
                callback.count = i+1;
            }
            return 0;
        }
    protected:
        ResizeCallback & callback;
        IRowManager * rm;
    };

    class ReleaseThread : public Thread
    {
    public:
        ReleaseThread(ResizeCallback & _callback, IRowManager * _rm) : Thread("releaseThread"), callback(_callback), rm(_rm)
        {
        }

        int run()
        {
            try
            {
                for (;;)
                {
                    unsigned limit = callback.count;
                    callback.lock();
                    for (unsigned i=0; i < limit; i++)
                    {
                        if (callback.rows[i])
                            ReleaseClearRoxieRow(callback.rows[i]);
                    }
                    callback.unlock();
                    if (limit == maxAlloc)
                        return 0;
                }
            }
            catch (IException * e)
            {
                DBGLOG(e);
                callback.unlock();
                callback.failed = true;
                return 0;
            }
        }
    protected:
        ResizeCallback & callback;
        IRowManager * rm;
    };

    void testResizeLock()
    {
        ResizeCallback callback;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        callback.rows = (const void * *)rowManager->allocate(1, 0);
        Owned<ReleaseThread> releaser = new ReleaseThread(callback, rowManager);
        Owned<ResizeThread> resizer = new ResizeThread(callback, rowManager);
        releaser->start();
        resizer->start();
        resizer->join();
        releaser->join();
        CPPUNIT_ASSERT(!callback.failed);
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

class RoxieMemTimingTests1 : public RoxieMemTests
{
    CPPUNIT_TEST_SUITE( RoxieMemTimingTests1 );
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testBitmapThreading);
        CPPUNIT_TEST(testCas);
        CPPUNIT_TEST(testCallbacks);
        CPPUNIT_TEST(testCompacting);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
};

const memsize_t memorySize = 0x60000000;
class RoxieMemStressTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieMemStressTests );
    CPPUNIT_TEST(testSetup);
    CPPUNIT_TEST(testHugeFragmentation);
    CPPUNIT_TEST(testFragmenting);
    CPPUNIT_TEST(testDoubleFragmenting);
    CPPUNIT_TEST(testResizeDoubleFragmenting);
    CPPUNIT_TEST(testResizeFragmenting);
    CPPUNIT_TEST(testSequential);
    CPPUNIT_TEST(testDatamanagerThreading);
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
        setTotalMemoryLimit(true, true, false, memorySize, 0, NULL, NULL);
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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
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
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        unsigned startTime = msTick();
        void * prev = rowManager->allocate(requestSize, 1);
        try
        {
            for (;;)
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
        DBGLOG("Time for fragmenting allocate = %d, max allocation=%" I64F "u, limit = %" I64F "u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/4);
    }

    void testDoubleFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        unsigned startTime = msTick();
        void * prev1 = rowManager->allocate(requestSize, 1);
        void * prev2 = rowManager->allocate(requestSize, 1);
        try
        {
            for (;;)
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
        DBGLOG("Time for fragmenting double allocate = %d, max allocation=%" I64F "u, limit = %" I64F "u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/8);
    }

    void testResizeFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        unsigned startTime = msTick();
        void * prev = rowManager->allocate(requestSize, 1);
        try
        {
            for (;;)
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
        DBGLOG("Time for fragmenting resize = %d, max allocation=%" I64F "u, limit = %" I64F "u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/1.3);
    }

    void testResizeDoubleFragmenting()
    {
        memsize_t requestSize = 32;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        unsigned startTime = msTick();
        void * prev1 = rowManager->allocate(requestSize, 1);
        void * prev2 = rowManager->allocate(requestSize, 1);
        try
        {
            for (;;)
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
        DBGLOG("Time for fragmenting double resize = %d, max allocation=%" I64F "u, limit = %" I64F "u", endTime - startTime, (unsigned __int64) requestSize, (unsigned __int64) memorySize);
        ASSERT(requestSize > memorySize/4);
    }

    class IncrementalRowBuffer : implements IBufferedRowCallback
    {
    public:
        IncrementalRowBuffer(size_t _maxRows, const void * * _rowset, unsigned _delta) :
            maxRows(_maxRows), rowset(_rowset), delta(_delta)
        {
        }

    //interface IBufferedRowCallback
        virtual unsigned getSpillCost() const { return 100; }
        virtual unsigned getActivityId() const { return 0; }
        virtual bool freeBufferedRows(bool critical)
        {
            VStringBuffer msg("Pool memory exhausted: global(%u/%u) WM(%u..%u)", heapAllocated, heapTotalPages, heapLWM, heapHWM);
            DBGLOG("%s", msg.str());
            unsigned togo = delta;
            while (togo--)
            {
                unsigned start = next;
                do
                {
                    unsigned cur = next;
                    next = (next+1) % maxRows;
                    if (rowset[cur])
                    {
                        ::ReleaseRoxieRow(rowset[cur]);
                        rowset[cur] = nullptr;
                        break;
                    }
                } while (start != next);
                if (start == next)
                {
                    throwUnexpected();
                    return false;
                }
            }
            return true;
        }

    protected:
        size_t next = 0;
        size_t maxRows;
        unsigned delta;
        const void * * rowset;
    };


    void testHugeFragmentation()
    {
        const unsigned maxAllocs = memorySize / HEAP_ALIGNMENT_SIZE;
        const unsigned numAllocs = maxAllocs/4;
        const unsigned rowsetSize = numAllocs*4;
        const unsigned releaseSize = numAllocs * 3 / 4;
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        const void * * rowset = new const void * [rowsetSize];
        memset(rowset, 0, sizeof(void *) * rowsetSize);

        IncrementalRowBuffer callback(rowsetSize, rowset, releaseSize);
        rowManager->addRowBuffer(&callback);
        try
        {
            for (unsigned i=0; i < 10000; i++)
            {
                unsigned total = 0;
                for (unsigned j=0; j < numAllocs; j++)
                {
                    unsigned blks = ((rand() % 4) + 1);
                    size_t size = blks * HEAP_ALIGNMENT_SIZE - HEAP_ALIGNMENT_SIZE/2;
                    unsigned target = rand() % rowsetSize;
                    if (rowset[target])
                        continue;
                    void * newrow = rowManager->allocate(size, 0);
                    rowset[target] = newrow;
                    total += numAllocs;
                }
            }
        }
        catch (...)
        {
            rowManager->removeRowBuffer(&callback);
            ReleaseRoxieRowArray(rowsetSize, rowset);
            delete [] rowset;
            throw;
        }

        rowManager->removeRowBuffer(&callback);
        ReleaseRoxieRowArray(rowsetSize, rowset);
        delete [] rowset;
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
                    DataBuffer *pages[1000];
                    unsigned i;
                    for (i = 0; i < 1000; i++)
                    {
                        pages[i] = dm.allocate();
                    }
                    for (i = 0; i < 1000; i++)
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
        dm.cleanUp();
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
        setTotalMemoryLimit(true, true, false, hugeMemorySize, 0, NULL, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    void testHuge()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        void * huge = rowManager->allocate(hugeAllocSize, 1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==16385);
        ReleaseRoxieRow(huge);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);
        memsize_t capacity;
        void *huge1 = rowManager->allocate(initialAllocSize, 1);
        rowManager->resizeRow(capacity, huge1, initialAllocSize, hugeAllocSize, 1);
        ASSERT(capacity > hugeAllocSize);
        ASSERT(rowManager->numPagesAfterCleanup(true)==16385);
        ReleaseRoxieRow(huge1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);

        huge1 = rowManager->allocate(hugeAllocSize/2, 1);
        rowManager->resizeRow(capacity, huge1, hugeAllocSize/2, hugeAllocSize, 1);
        ASSERT(capacity > hugeAllocSize);
        ASSERT(rowManager->numPagesAfterCleanup(true)==16385);
        ReleaseRoxieRow(huge1);
        ASSERT(rowManager->numPagesAfterCleanup(true)==0);

        ASSERT(rowManager->getExpectedCapacity(hugeAllocSize, RHFnone) > hugeAllocSize);
        ASSERT(rowManager->getExpectedFootprint(hugeAllocSize, RHFnone) > hugeAllocSize);
        ASSERT(true);
    }
};


//#define RUN_SINGLE_TEST

static const memsize_t tuningMemorySize = I64C(0x100000000);
static const unsigned tuningAllocSize = 64;
static const memsize_t numTuningRows = tuningMemorySize / (tuningAllocSize * 2);
static const memsize_t numTuningIters = 5;
static const size_t minGranularity = 256;
static const size_t maxGranularity = 0x2000;
static const size_t defaultGranularity = DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY;

static int compareTiming(const void * pLeft, const void * pRight)
{
    const unsigned * left = (const unsigned *)pLeft;
    const unsigned * right = (const unsigned *)pRight;
    return (*left < *right) ? -1 : (*left > *right) ? +1 : 0;
}


class RoxieMemTimingTests2 : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( RoxieMemTimingTests2 );
    CPPUNIT_TEST(testSetup);
#ifdef RUN_SINGLE_TEST
    CPPUNIT_TEST(testOne);
#else
    CPPUNIT_TEST(testSyncOrderRelease);
    CPPUNIT_TEST(testSyncShuffleRelease);
#endif
    CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieMemTimingTests2() : logctx(queryDummyContextLogger())
    {
    }

    ~RoxieMemTimingTests2()
    {
    }

protected:
    void testSetup()
    {
        setTotalMemoryLimit(true, true, true, tuningMemorySize, 0, NULL, NULL);
    }

    void testCleanup()
    {
        releaseRoxieHeap();
    }

    void testOne()
    {
        unsigned sequential = 0;
        unsigned numTestRows = 0x10000;
        testSync(numTestRows, DEFAULT_PARALLEL_SYNC_RELEASE_GRANULARITY, true, sequential);
    }

    void testSyncOrderRelease()
    {
        testSyncRows(false);
        testSyncRelease(numTuningRows / 0x100, false);
        testSyncRelease(numTuningRows / 0x10, false);
        testSyncRelease(numTuningRows, false);
    }
    void testSyncShuffleRelease()
    {
        testSyncRows(true);
        testSyncRelease(numTuningRows / 256, true);
        testSyncRelease(numTuningRows / 16, true);
        testSyncRelease(numTuningRows, true);
    }
    void testSyncRelease(size_t numRows, bool shuffle)
    {
        size_t granularity = minGranularity;

        //First timing is not done in parallel
        unsigned sequential = testSync(numRows, numTuningRows, shuffle, 0);
        while ((granularity < maxGranularity) && (granularity * 2< numRows))
        {
            testSync(numRows, granularity, shuffle, sequential);
            testSync(numRows, granularity+granularity/2, shuffle, sequential);
            granularity <<= 1;
        }
    }

    void testSyncRows(bool shuffle)
    {
        unsigned numRows = numTuningRows;
        while (numRows >= defaultGranularity)
        {
            unsigned sequential = testSync(numRows, numRows+1, shuffle, 0);
            testSync(numRows, defaultGranularity, shuffle, sequential);
            numRows = numRows / 2;
        }
    }

    unsigned testSync(size_t numRows, size_t granularity, bool shuffle, unsigned sequential)
    {
        setParallelSyncReleaseGranularity(granularity, 2);
        unsigned times[numTuningIters];
        for (unsigned iter=0; iter < numTuningIters; iter++)
            times[iter] = testSingleSync(numRows, granularity, shuffle);

        unsigned median = reportSummary("Sync", numRows, granularity, shuffle, sequential, times);
        return median;
    }

    unsigned reportSummary(const char * type, size_t numRows, size_t granularity, bool shuffle, unsigned sequential, unsigned * times)
    {
        //Calculate the median
        qsort(times, numTuningIters, sizeof(*times), compareTiming);
        //Give an estimate of the range - exclude the min and max values
        unsigned low = 0;
        unsigned high = 0;
        unsigned median = times[numTuningIters/2];
        if (numTuningIters >= 5)
        {
            high = (times[numTuningIters-2] - median);
            low = median - times[1];
        }
        double percent = sequential ? ((double)median * 100) / sequential : 100.0;
        const char * compare = (sequential ? ((median < sequential) ? "<" : ">") : " ");
        printf("%s %s %s (%u) took %u(-%u..+%u) us {%.2f%%} for %" I64F "u rows\n", type, shuffle ? "Shuffle" : "Ordered", compare, (unsigned)granularity, median, low, high, percent, (unsigned __int64)numRows);
        return median;
    }

    unsigned testSingleSync(size_t numRows, size_t granularity, bool shuffle)
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL, false);
        ConstPointerArray rows;
        createRows(rowManager, numRows, rows, shuffle);
        cycle_t start = get_cycles_now();
        ReleaseRoxieRowArray(rows.ordinality(), rows.getArray());
        unsigned microsecs = cycle_to_microsec(get_cycles_now() - start);
        return microsecs;
    }


    void createRows(IRowManager * rowManager, size_t numRows, ConstPointerArray & target, bool shuffle)
    {
        Owned<IFixedRowHeap> heap = rowManager->createFixedRowHeap(tuningAllocSize, 0, RHFpacked);
        target.ensureSpace(numTuningRows);
        for (size_t i = 0; i < numRows; i++)
            target.append(heap->allocate());

        if (shuffle)
        {
            Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
            random->seed(123456789);
            unsigned i = target.ordinality();
            while (i > 1)
            {
                unsigned j = random->next() % i;
                i--;
                target.swap(i, j);
            }
        }
    }

};


CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemTests, "RoxieMemTests" );
CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemTimingTests1 );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemTimingTests1, "RoxieMemTimingTests1" );
CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemStressTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemStressTests, "RoxieMemStressTests" );

CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemTimingTests2 );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemTimingTests2, "RoxieMemTimingTests2" );

#ifdef __64BIT__
CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemHugeTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemHugeTests, "RoxieMemHugeTests" );
#endif
} // namespace roxiemem
#endif
