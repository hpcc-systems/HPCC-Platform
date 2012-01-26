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

#include "roxiemem.hpp"
#include "jlog.hpp"
#include <new>

#ifndef _WIN32
#include <sys/mman.h>
#endif

#ifdef _DEBUG
#define _CLEAR_ALLOCATED_ROW
#endif

namespace roxiemem {

#define USE_MADVISE_ON_FREE     // avoid linux swapping 'freed' pages to disk
#define TIMEOUT_CHECK_FREQUENCY_MILLISECONDS 10

unsigned memTraceLevel = 1;
size32_t memTraceSizeLimit = 0;

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

#define HEAPERROR(a) throw MakeStringException(ROXIE_HEAP_ERROR, a)

unsigned totalMemoryLimit = 0;   // in blocks
#ifdef _DEBUG
unsigned maxLeakReport = 20;
#else
unsigned maxLeakReport = 4;
#endif

static char *heapBase;
static unsigned *heapBitmap;
static unsigned heapBitmapSize;
static unsigned heapLWM;
static bool heapExhausted = false;
static time_t lastStatsLog = 0;
unsigned statsLogFreq = 0;
atomic_t dataBufferPages;
atomic_t dataBuffersActive;

const unsigned UNSIGNED_BITS = sizeof(unsigned) * 8;
const unsigned TOPBITMASK = 1<<(UNSIGNED_BITS-1);

typedef MapBetween<unsigned, unsigned, memsize_t, memsize_t> MapActivityToMemsize;

CriticalSection heapBitCrit;

void initializeHeap(unsigned pages)
{
    if (heapBase) return;
    // CriticalBlock b(heapBitCrit); // unnecessary - must call this exactly once before any allocations anyway!
    heapBitmapSize = (pages + UNSIGNED_BITS - 1) / UNSIGNED_BITS;
    memsize_t memsize = memsize_t(heapBitmapSize) * UNSIGNED_BITS * HEAP_ALIGNMENT_SIZE;
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
    int ret;
    if ((ret = posix_memalign((void **) &heapBase, HEAP_ALIGNMENT_SIZE, memsize)) != 0) {
        DBGLOG("RoxieMemMgr: posix_memalign (alignment=%"I64F"u, size=%"I64F"u) failed - ret=%d", 
                (unsigned __int64) HEAP_ALIGNMENT_SIZE, (unsigned __int64) memsize, ret);
        HEAPERROR("RoxieMemMgr: Unable to create heap");
    }
#endif
    heapBitmap = new unsigned [heapBitmapSize];
    memset(heapBitmap, 0xff, heapBitmapSize*sizeof(unsigned));
    heapLWM = 0;

    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: %u Pages successfully allocated for the pool - memsize=%"I64F"u base=%p alignment=%"I64F"u bitmapSize=%u", 
                heapBitmapSize * UNSIGNED_BITS, (unsigned __int64) memsize, heapBase, (unsigned __int64) HEAP_ALIGNMENT_SIZE, heapBitmapSize);
}

extern void releaseRoxieHeap()
{
    if (heapBase)
    {
        delete [] heapBitmap;
        heapBitmap = NULL;
        heapBitmapSize = 0;
#ifdef _WIN32
        VirtualFree(heapBase, 0, MEM_RELEASE);
#else
        free(heapBase);
#endif
        heapBase = NULL;
    }
}

void memstats(unsigned &totalpg, unsigned &freepg, unsigned &maxblk)
{
    CriticalBlock b(heapBitCrit);
    totalpg = heapBitmapSize * UNSIGNED_BITS;
    unsigned freePages = 0;
    unsigned maxBlock = 0;
    unsigned thisBlock = 0;
#if 1 // Nigel's version (doubt that important but make a bit faster as want to call periodically in stats in Thor)
    for (unsigned i = 0; i < heapBitmapSize; i++)
    {
        unsigned t = heapBitmap[i];
        if (t) 
        {
            if (t==(unsigned)-1) 
            {
                thisBlock += UNSIGNED_BITS; 
                freePages += UNSIGNED_BITS;
            }
            else 
            {
                unsigned mask = 1;
                do 
                {
                    if (t&mask) 
                    {
                        freePages++;
                        thisBlock++;
                    }
                    else 
                    {
                        if (thisBlock > maxBlock)
                            maxBlock = thisBlock;
                        thisBlock = 0;
                        if (!t)
                            break; // short circuit
                    }
                    mask <<= 1;
                } while (mask);
            }
        }
        else if (thisBlock) 
        {
            if (thisBlock > maxBlock)
                maxBlock = thisBlock;
            thisBlock = 0;
        }
    }
    if (thisBlock > maxBlock)
        maxBlock = thisBlock;

#else // old version
    for (unsigned i = 0; i < heapBitmapSize; i++)
    {
        unsigned mask = 1;
        while (mask)
        {
            if (heapBitmap[i] & mask)
            {
                freePages++;
                thisBlock++;
                if (thisBlock > maxBlock)
                    maxBlock = thisBlock;
            }
            else
                thisBlock = 0;
            mask <<= 1;
        }
    }
#endif
    freepg = freePages;
    maxblk = maxBlock;
}

StringBuffer &memstats(StringBuffer &stats)
{
    unsigned totalPages;
    unsigned freePages;
    unsigned maxBlock;
    memstats(totalPages, freePages, maxBlock);
    return stats.appendf("Heap size %d pages, %d free, largest block %d", heapBitmapSize * UNSIGNED_BITS, freePages, maxBlock);
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

StringBuffer &memmap(StringBuffer &stats)
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

        if (heapBitmap[i] == (unsigned) -1) {
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
    return stats.appendf("\nHeap size %d pages, %d free, largest block %d", heapBitmapSize * UNSIGNED_BITS, freePages, maxBlock);
}

void *suballoc_aligned(size32_t pages)
{
    CriticalBlock b(heapBitCrit);
    time_t now;
    time(&now);
    if ((statsLogFreq && (now - lastStatsLog) > statsLogFreq) || (memTraceLevel >= 2))
    {
        // MORE - do we really want to lock heap as we do this?
        lastStatsLog = now;
        StringBuffer s;
        memstats(s);
        s.appendf(", heapLWM %u, dataBuffersActive=%d, dataBufferPages=%d", heapLWM, atomic_read(&dataBuffersActive), atomic_read(&dataBufferPages));
        DBGLOG("RoxieMemMgr: %s", s.str());
    }
    if (heapExhausted) {
        DBGLOG("RoxieMemMgr: Memory pool (%u pages) exhausted", heapBitmapSize * UNSIGNED_BITS);
        throw MakeStringException(ROXIE_MEMORY_POOL_EXHAUSTED, "Memory pool exhausted"); // MORE: use error codes
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
                heapBitmap[i] &= ~mask;
                heapLWM = i;
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
                                heapBitmap[i] &= ~mask;
                                if (!--matches)
                                    break;
                                if (mask==TOPBITMASK)
                                {
                                    mask=1;
                                    i++;
                                }
                                else
                                    mask <<= 1;
                            }
                            if (memTraceLevel >= 2)
                                DBGLOG("RoxieMemMgr: suballoc_aligned() %d page ok - addr=%p", pages, ret);
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
    heapExhausted = true; 
    DBGLOG("RoxieMemMgr: Memory pool (%u pages) exhausted", heapBitmapSize * UNSIGNED_BITS);
    throw MakeStringException(ROXIE_MEMORY_POOL_EXHAUSTED, "Memory pool exhausted");  
    // Arguably should fail process here. Or could wait a while and see if some gets freed.
}

void subfree_aligned(void *ptr, unsigned pages = 1)
{
    unsigned _pages = pages;
    memsize_t offset = (char *)ptr - heapBase;
    memsize_t pageOffset = offset / HEAP_ALIGNMENT_SIZE;
    if (!pages) 
    {
        DBGLOG("RoxieMemMgr: Invalid parameter (pages=%u) to subfree_aligned", pages);
        HEAPERROR("RoxieMemMgr: Invalid parameter (num pages) to subfree_aligned");
    }
    if (pageOffset + pages > heapBitmapSize*UNSIGNED_BITS) 
    {
        DBGLOG("RoxieMemMgr: Freed area not in heap (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Freed area not in heap");
    }
    if (pageOffset*HEAP_ALIGNMENT_SIZE != offset) 
    {
        DBGLOG("RoxieMemMgr: Incorrect alignment of freed area (ptr=%p)", ptr);
        HEAPERROR("RoxieMemMgr: Incorrect alignment of freed area");
    }
#ifndef _WIN32
#ifdef USE_MADVISE_ON_FREE
    // for linux mark as unwanted
    madvise(ptr,pages*HEAP_ALIGNMENT_SIZE,MADV_DONTNEED);
#endif
#endif
    unsigned wordOffset = (unsigned) (pageOffset / UNSIGNED_BITS);
    unsigned bitOffset = (unsigned) (pageOffset % UNSIGNED_BITS);
    unsigned mask = 1<<bitOffset;
    CriticalBlock b(heapBitCrit);
    if (wordOffset < heapLWM)
        heapLWM = wordOffset;
    loop
    {
        if ((heapBitmap[wordOffset] & mask) == 0)
            heapBitmap[wordOffset] |= mask;
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
    if (heapExhausted)
    {
        DBGLOG("RoxieMemMgr: Memory pool is NOT exhausted now");
        heapExhausted = false;
    }
    if (memTraceLevel >= 2)
        DBGLOG("RoxieMemMgr: subfree_aligned() %d pages ok - addr=%p heapLWM=%u totalPages=%d", _pages, ptr, heapLWM, heapBitmapSize * UNSIGNED_BITS);
}

static inline unsigned getRealActivityId(unsigned rawId, const IRowAllocatorCache *allocatorCache)
{
    if ((rawId & ACTIVITY_FLAG_ISREGISTERED) && allocatorCache)
        return allocatorCache->getActivityId(rawId & MAX_ACTIVITY_ID);
    else
        return rawId & MAX_ACTIVITY_ID;
}

class BigHeapletBase : public HeapletBase
{
    friend class CChunkingHeap;
protected:
    BigHeapletBase *next;
    const IRowAllocatorCache *allocatorCache;
    
    inline unsigned getActivityId(unsigned rawId) const
    {
        return getRealActivityId(rawId, allocatorCache);
    }

public:
    BigHeapletBase(const IRowAllocatorCache *_allocatorCache)
    {
        next = NULL;
        allocatorCache = _allocatorCache;
    }

    bool isPointer(void *ptr)
    {
        HeapletBase *h = findBase(ptr);
        return h==this;
    }

    size32_t _capacity() const { throwUnexpected(); }

    virtual size32_t sizeInPages() { throwUnexpected(); }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const { throwUnexpected(); }
    virtual void checkHeap() const { throwUnexpected(); }
    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const { throwUnexpected(); }

#ifdef _WIN32
#ifdef new
#define __new_was_defined
#undef new
#endif
#endif

public:
    void * operator new (memsize_t len)
    {
        void *ret = suballoc_aligned(1);
        return ret;
    }

    void operator delete(void * p)
    {
        subfree_aligned(p, 1);
    }

    virtual void *allocate(unsigned size, unsigned activityId)
    {
        throwUnexpected();
    }
};


//================================================================================
// Fixed size (chunking) heaplet
#define RBLOCKS_CAS_TAG        HEAP_ALIGNMENT_SIZE           // must be >= HEAP_ALIGNMENT_SIZE and power of 2
#define RBLOCKS_OFFSET_MASK    (RBLOCKS_CAS_TAG-1)
#define RBLOCKS_CAS_TAG_MASK   ~RBLOCKS_OFFSET_MASK

class FixedSizeHeaplet : public BigHeapletBase
{
protected:
    atomic_t r_blocks;  // the free chain as a relative pointer
    atomic_t freeBase;
    size32_t fixedSize; 
    char data[1];  // n really

private:

    inline void checkPtr(const char *ptr, const char *reason) const
    {
        const char *baseptr = ptr - sizeof(atomic_t) - sizeof(unsigned);
        if (((*(unsigned *) baseptr) & ~ACTIVITY_MASK) != ACTIVITY_MAGIC)
        {
            DBGLOG("%s: Attempt to free invalid pointer %p %x", reason, ptr, *(unsigned *) baseptr);
            PrintStackReport();
            PrintMemoryReport();
            HEAPERROR("Attempt to free invalid pointer");
        }
    }

    inline unsigned makeRelative(const char *ptr)
    {
        assert(ptr);
        ptrdiff_t diff = ptr - (char *) this;
        assert(diff < HEAP_ALIGNMENT_SIZE);
        return (unsigned) diff;
    }

    inline char * makeAbsolute(unsigned v)
    {
        assert(v && v < HEAP_ALIGNMENT_SIZE);
        return ((char *) this) + v;
    }

public:
    FixedSizeHeaplet(const IRowAllocatorCache *_allocatorCache, size32_t size) : BigHeapletBase(_allocatorCache)
    {
        fixedSize = size;
        atomic_set(&freeBase, 0);
        atomic_set(&r_blocks, 0);
    }

    virtual size32_t sizeInPages() { return 1; }

    virtual bool _isShared(const void *_ptr) const
    {
        assert(_ptr != this);
        char *ptr = (char *) _ptr;
        checkPtr(ptr, "isShared");
        ptr -= sizeof(atomic_t);
        return atomic_read((atomic_t *) ptr)!=1;
    }

    virtual size32_t _capacity() const
    {
        return fixedSize - sizeof(atomic_t) - sizeof(unsigned);
    }

    virtual void _setDestructorFlag(const void *_ptr)
    {
        if (_ptr != this)
        {
            char *ptr = (char *) _ptr;
            ptr -= sizeof(atomic_t);
            ptr -= sizeof(unsigned);
            unsigned *id = (unsigned *) ptr;
            assertex(*id & ACTIVITY_FLAG_ISREGISTERED);
            *id |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
        }
        else
            throwUnexpected();
    }

    virtual void noteReleased(const void *_ptr)
    {
        assert(_ptr != this);

        char *ptr = (char *) _ptr;
        checkPtr(ptr, "Release");
        ptr -= sizeof(atomic_t);
        if (atomic_dec_and_test((atomic_t *) ptr))
        {
            ptr -= sizeof(unsigned);
            unsigned id = *(unsigned *) ptr;
            if (id & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
                allocatorCache->onDestroy(id & MAX_ACTIVITY_ID, ptr + sizeof(unsigned) + sizeof(atomic_t));

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

            atomic_dec(&count);
        }
    }

    virtual void noteLinked(const void *_ptr)
    {
        assertex(_ptr != this);
        char *ptr = (char *) _ptr;
        checkPtr(ptr, "Link");
        ptr -= sizeof(atomic_t);
        atomic_inc((atomic_t *) ptr);
    }

    virtual void *allocate(unsigned size, unsigned activityId)
    {
        if (size != fixedSize)
            return NULL;

        char *ret;
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
                size32_t bytesFree = (HEAP_ALIGNMENT_SIZE - offsetof(FixedSizeHeaplet,data)) - curFreeBase;
                if (bytesFree >= size)
                {
                    if (atomic_cas(&freeBase, curFreeBase + size, curFreeBase))
                    {
                        ret = data + curFreeBase;
                        break;
                    }
                }
                else
                    return NULL;
            }
        }

        atomic_inc(&count);
        * (unsigned *) ret = (activityId & ACTIVITY_MASK) | ACTIVITY_MAGIC;
        ret += sizeof(unsigned);
        atomic_set((atomic_t *) ret, 1);
        ret += sizeof(atomic_t);
#ifdef _CLEAR_ALLOCATED_ROW
        memset(ret, 0xcc, size- (sizeof(atomic_t) + sizeof(unsigned)));
#endif
        return ret;
    }

    inline static unsigned maxHeapSize()
    {
        return HEAP_ALIGNMENT_SIZE - (offsetof(FixedSizeHeaplet, data) + sizeof(atomic_t) + sizeof(unsigned));
    }

    static inline unsigned dataAreaSize()
    {
        return HEAP_ALIGNMENT_SIZE - offsetof(FixedSizeHeaplet, data);
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const 
    {
        //This function may not give correct results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        while (leaked > 0 && base < limit)
        {
            const char *block = data + base;
            unsigned count = atomic_read((atomic_t *) (block + sizeof(unsigned)));
            if (count != 0)
            {
                unsigned activityId = *(unsigned *) block;
                bool hasChildren = (activityId & ACTIVITY_FLAG_NEEDSDESTRUCTOR) != 0;

                block += sizeof(unsigned) + sizeof(atomic_t);
                logctx.CTXLOG("Block size %u at %p %swas allocated by activity %u and not freed (%d)", fixedSize, block, hasChildren ? "(with children) " : "", getActivityId(activityId), count);
                leaked--;
            }
            base += fixedSize;
        }
    }

    virtual void checkHeap() const 
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        while (base < limit)
        {
            const char *block = data + base;
            unsigned count = atomic_read((atomic_t *) (block + sizeof(unsigned)));
            if (count != 0)
            {
                //MORE: Potential race: could be freed while the pointer is being checked
                checkPtr(block+sizeof(atomic_t)+sizeof(unsigned), "Check");
            }
            base += fixedSize;
        }
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const 
    {
        //This function may not give 100% accurate results if called if there are concurrent allocations/releases
        unsigned base = 0;
        unsigned limit = atomic_read(&freeBase);
        memsize_t running = 0;
        unsigned lastId = 0;
        while (base < limit)
        {
            const char *block = data + base;
            unsigned activityId = getActivityId(*(unsigned *) block);
            //Potential race condition - a block could become allocated between these two lines.
            //That may introduce invalid activityIds (from freed memory) in the memory tracing.
            if (atomic_read((atomic_t *) (block + sizeof(unsigned))) != 0)
            {
                if (activityId != lastId)
                {
                    if (lastId)
                        map->noteMemUsage(lastId, running);
                    lastId = activityId;
                    running = fixedSize;
                }
                else
                    running += fixedSize;
            }
            base += fixedSize;
        }
        if (lastId)
            map->noteMemUsage(lastId, running);
    }
};

//================================================================================
// Row manager - chunking

class HugeHeaplet : public BigHeapletBase
{
protected:
    size32_t hugeSize;
    unsigned activityId;
    char data[1];  // n really

    inline unsigned _sizeInPages() 
    {
        // Can't call a virtual in the delete method....
        return ((hugeSize + offsetof(HugeHeaplet, data) + HEAP_ALIGNMENT_SIZE - 1) / HEAP_ALIGNMENT_SIZE);
    }

public:
    HugeHeaplet(const IRowAllocatorCache *_allocatorCache, unsigned _hugeSize, unsigned _activityId) : BigHeapletBase(_allocatorCache)
    {
        hugeSize = _hugeSize;
        activityId = _activityId;
    }

    virtual size32_t _capacity() const { return ((hugeSize + dataOffset() + HEAP_ALIGNMENT_SIZE - 1) & HEAP_ALIGNMENT_MASK) - dataOffset(); }

    bool _isShared(const void *ptr) const
    {
        return atomic_read(&count) > 2; // The heaplet itself has a usage count of 1
    }

    virtual unsigned sizeInPages() 
    {
        return _sizeInPages();
    }

    static unsigned dataOffset()
    {
        return (offsetof(HugeHeaplet, data));
    }

    void * operator new (memsize_t len, unsigned _hugeSize)
    {
        unsigned sizeInPages = ((_hugeSize + offsetof(HugeHeaplet, data) + HEAP_ALIGNMENT_SIZE - 1) / HEAP_ALIGNMENT_SIZE);
        void *ret = suballoc_aligned(sizeInPages);
        return ret;
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
    void operator delete(void * p, unsigned _hugeSize)  
    {
        unsigned sizeInPages = ((_hugeSize + offsetof(HugeHeaplet, data) + HEAP_ALIGNMENT_SIZE - 1) / HEAP_ALIGNMENT_SIZE);
        subfree_aligned(p, sizeInPages);
    }

    virtual void noteReleased(const void *ptr)
    {
        if (atomic_dec_and_test(&count))
        {
            if (activityId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
                allocatorCache->onDestroy(activityId & MAX_ACTIVITY_ID, (void *)ptr);
        }
    }

    virtual void _setDestructorFlag(const void *ptr)
    {
        activityId |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
    }

    virtual void noteLinked(const void *ptr)
    {
        atomic_inc(&count);
    }

    virtual void *allocate(unsigned size, unsigned activityId)
    {
        throwUnexpected();
    }

    void *allocateHuge(unsigned size, unsigned activityId)
    {
        atomic_inc(&count);
        assertex(size == hugeSize);
#ifdef _CLEAR_ALLOCATED_ROW
        memset(&data, 0xcc, size);
#endif
        return &data;
    }

    virtual void reportLeaks(unsigned &leaked, const IContextLogger &logctx) const 
    {
        logctx.CTXLOG("Block size %u was allocated by activity %u and not freed", hugeSize, getActivityId(activityId));
        leaked--;
    }

    virtual void getPeakActivityUsage(IActivityMemoryUsageMap *map) const 
    {
        map->noteMemUsage(activityId, hugeSize);
    }

    virtual void checkHeap() const
    {
        //MORE
    }
};

//================================================================================
//
struct ActivityEntry 
{
    unsigned activityId;
    unsigned allocations;
    memsize_t usage;
};

typedef MapBetween<unsigned, unsigned, ActivityEntry, ActivityEntry> MapActivityToActivityEntry;

class CActivityMemoryUsageMap : public CInterface, implements IActivityMemoryUsageMap
{
    MapActivityToActivityEntry map;
    memsize_t maxUsed;
    memsize_t totalUsed;
    unsigned maxActivity;


public:
    IMPLEMENT_IINTERFACE;
    CActivityMemoryUsageMap()
    {
        maxUsed = 0;
        totalUsed = 0;
        maxActivity = 0;
    }

    virtual void noteMemUsage(unsigned activityId, unsigned memUsed)
    {
        totalUsed += memUsed;
        ActivityEntry *ret = map.getValue(activityId);
        if (ret)
        {
            memUsed += ret->usage;
            ret->usage = memUsed;
            ret->allocations++;
        }
        else
        {
            ActivityEntry e = {activityId, 1, memUsed};
            map.setValue(activityId, e);
        }
        if (memUsed > maxUsed)
        {
            maxUsed = memUsed;
            maxActivity = activityId;
        }
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
                logctx.CTXLOG("%"I64F"u bytes allocated by activity %u (%u allocations)", (unsigned __int64) results[j]->usage, getRealActivityId(results[j]->activityId, allocatorCache), results[j]->allocations);
            }
            logctx.CTXLOG("------------------ End of snapshot");
            delete [] results;
        }
    }
};

//================================================================================
//
class CChunkingRowManager;

class CRoxieFixedRowHeap : implements IFixedRowHeap, public CInterface
{
public:
    CRoxieFixedRowHeap(CChunkingRowManager * _rowManager, size32_t _fixedSize, unsigned _activityId, RoxieHeapFlags _flags)
        : rowManager(_rowManager), fixedSize(_fixedSize), activityId(_activityId), flags(_flags)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual void *allocate();
    virtual void *finalizeRow(void *final);

protected:
    CChunkingRowManager * rowManager;       // Lifetime of rowManager is guaranteed to be longer
    size32_t fixedSize;
    unsigned activityId;
    RoxieHeapFlags flags;
};


//================================================================================
//
class CRoxieVariableRowHeap : implements IVariableRowHeap, public CInterface
{
public:
    CRoxieVariableRowHeap(CChunkingRowManager * _rowManager, unsigned _activityId, RoxieHeapFlags _flags)
        : rowManager(_rowManager), activityId(_activityId), flags(_flags)
    {
    }
    IMPLEMENT_IINTERFACE

    virtual void *allocate(size32_t size, size32_t & capacity);
    virtual void *resizeRow(void * original, size32_t oldsize, size32_t newsize, size32_t &capacity);
    virtual void *finalizeRow(void *final, size32_t originalSize, size32_t finalSize);

protected:
    CChunkingRowManager * rowManager;       // Lifetime of rowManager is guaranteed to be longer
    unsigned activityId;
    RoxieHeapFlags flags;
};


//================================================================================
//
//Responsible for allocating memory for a chain of chunked blocks
class CChunkingHeap
{
public:
    CChunkingHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache)
        : logctx(_logctx), rowManager(_rowManager), allocatorCache(_allocatorCache), active(NULL)
    {
    }

    void destroy(unsigned &leaked)
    {
        BigHeapletBase *finger = active;
        while (finger)
        {
            if (memTraceLevel >= 3)
                logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor freeing heaplet linked in active list - addr=%p rowMgr=%p",
                        finger, this);
            if (leaked && memTraceLevel >= 2)
                finger->reportLeaks(leaked, logctx);
            BigHeapletBase *next = getNext(finger);
            delete finger;
            finger = next;
        }
        active = NULL;
    }

    void reportLeaks(unsigned &leaked) const
    {
        SpinBlock c1(crit);
        BigHeapletBase *finger = active;
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
        BigHeapletBase *finger = active;
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
        BigHeapletBase *finger = active;
        while (finger)
        {
            total += finger->queryCount() - 1; // There is one refcount for the page itself on the active q
            finger = getNext(finger);
        }
        return total;
    }

    unsigned pages()
    {
        unsigned total = 0;
        SpinBlock c1(crit);
        BigHeapletBase *finger = active;
        BigHeapletBase *prev = NULL;
        while (finger)
        {
            BigHeapletBase *next = getNext(finger);
            if (finger->queryCount()==1)
            {
                if (memTraceLevel >= 3)
                    logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::pages() freeing Heaplet linked in active list - addr=%p pages=%u capacity=%u rowMgr=%p",
                            finger, finger->sizeInPages(), finger->_capacity(), this);
                if (prev)
                    setNext(prev, next);
                else
                    active = next;
                delete finger;
            }
            else
            {
                total += finger->sizeInPages();
                prev = finger;
            }
            finger = next;
        }
        return total;
    }

    void getPeakActivityUsage(IActivityMemoryUsageMap * usageMap)
    {
        SpinBlock c1(crit);
        BigHeapletBase *finger = active;
        while (finger)
        {
            if (finger->queryCount()!=1)
                finger->getPeakActivityUsage(usageMap);
            finger = getNext(finger);
        }
    }

protected:
    inline BigHeapletBase * getNext(const BigHeapletBase * ptr) const { return ptr->next; }
    inline void setNext(BigHeapletBase * ptr, BigHeapletBase * next) const { ptr->next = next; }

protected:
    BigHeapletBase *active;
    CChunkingRowManager * rowManager;
    const IRowAllocatorCache *allocatorCache;
    const IContextLogger & logctx;
    mutable SpinLock crit;
};

class CHugeChunkingHeap : public CChunkingHeap
{
public:
    CHugeChunkingHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache)
        : CChunkingHeap(_rowManager, _logctx, _allocatorCache)
    {
    }

    void * doAllocate(unsigned _size, unsigned activityId);
};

class CNormalChunkingHeap : public CChunkingHeap
{
public:
    CNormalChunkingHeap(CChunkingRowManager * _rowManager, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache)
        : CChunkingHeap(_rowManager, _logctx, _allocatorCache)
    {
    }

    void * doAllocate(size32_t chunkSize, unsigned activityId);
};

//================================================================================
//
class CChunkingRowManager : public CInterface, implements IRowManager
{
    friend class CRoxieFixedRowHeap;
    friend class CRoxieVariableRowHeap;
    friend class CHugeChunkingHeap;
    friend class CNormalChunkingHeap;

    SpinLock crit;
    CNormalChunkingHeap normalHeap;
    CHugeChunkingHeap hugeHeap;
    unsigned pageLimit;
    ITimeLimiter *timeLimit;
    unsigned peakPages;
    unsigned dataBuffs;
    unsigned dataBuffPages;
    atomic_t possibleGoers;
    DataBufferBase *activeBuffs;
    const IContextLogger &logctx;
    bool ignoreLeaks;
    bool trackMemoryByActivity;
    Owned<IActivityMemoryUsageMap> usageMap;
    const IRowAllocatorCache *allocatorCache;
    unsigned __int64 cyclesChecked;       // When we last checked timelimit
    unsigned __int64 cyclesCheckInterval; // How often we need to check timelimit

    inline unsigned getActivityId(unsigned rawId) const
    {
        return getRealActivityId(rawId, allocatorCache);
    }

public:
    IMPLEMENT_IINTERFACE;

    CChunkingRowManager (unsigned _memLimit, ITimeLimiter *_tl, const IContextLogger &_logctx, const IRowAllocatorCache *_allocatorCache, bool _ignoreLeaks)
        : logctx(_logctx), allocatorCache(_allocatorCache), normalHeap(this, _logctx, _allocatorCache), hugeHeap(this, _logctx, _allocatorCache)
    {
        logctx.Link();
        pageLimit = _memLimit / HEAP_ALIGNMENT_SIZE;
        timeLimit = _tl;
        peakPages = 0;
        dataBuffs = 0;
        atomic_set(&possibleGoers, 0);
        activeBuffs = NULL;
        dataBuffPages = 0;
        ignoreLeaks = _ignoreLeaks;
#ifdef _DEBUG
        trackMemoryByActivity = true; 
#else
        trackMemoryByActivity = false;
#endif
        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager c-tor memLimit=%u pageLimit=%u rowMgr=%p", _memLimit, pageLimit, this);
        if (timeLimit)
        {
            cyclesChecked = get_cycles_now();
            cyclesCheckInterval = nanosec_to_cycle(1000000 * TIMEOUT_CHECK_FREQUENCY_MILLISECONDS); // Could perhaps ask timelimit object what a suitable frequency is..
        }
        else
        {
            cyclesChecked = 0;
            cyclesCheckInterval = 0;
        }
    }

    ~CChunkingRowManager()
    {
        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager d-tor pageLimit=%u peakPages=%u dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p", 
                    pageLimit, peakPages, dataBuffs, dataBuffPages, atomic_read(&possibleGoers), this);

        unsigned leaked = ignoreLeaks ? 0 : allocated();
        if (leaked)
        {
            if (memTraceLevel >= 2)
                logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active heaplet list - rowMgr=%p", leaked, this);
            if (leaked > maxLeakReport)
                leaked = maxLeakReport;
        }

        normalHeap.destroy(leaked);
        hugeHeap.destroy(leaked);

        DataBufferBase *dfinger = activeBuffs;
        while (dfinger)
        {
            DataBufferBase *next = dfinger->next;
            if (memTraceLevel >= 2 && dfinger->queryCount()!=1)
                logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active dataBuffer list - addr=%p rowMgr=%p", 
                        dfinger->queryCount()-1, dfinger, this);
            dfinger->next = NULL;
            dfinger->mgr = NULL;
            dfinger->Release();
            dfinger = next;
        }
        logctx.Release();
    }

    virtual void reportLeaks()
    {
        unsigned leaked = allocated();
        if (leaked)
        {
            if (memTraceLevel >= 1)
                logctx.CTXLOG("RoxieMemMgr: Memory leak: %d records remain linked in active heaplet list - rowMgr=%p", leaked, this);
            if (leaked > maxLeakReport)
                leaked = maxLeakReport;
        }

        normalHeap.reportLeaks(leaked);
        hugeHeap.reportLeaks(leaked);
    }

    virtual void checkHeap()
    {
        normalHeap.checkHeap();
    }

    virtual void setActivityTracking(bool val)
    {
        trackMemoryByActivity = val;
    }

    virtual unsigned allocated()
    {
        unsigned total = 0;
        total += normalHeap.allocated();
        total += hugeHeap.allocated();
        return total;
    }

    virtual unsigned pages()
    {
        unsigned total = dataBuffPages;
        total += normalHeap.pages();
        total += hugeHeap.pages();
        return total;
    }

    virtual void getPeakActivityUsage()
    {
        SpinBlock c1(crit);
        usageMap.setown(new CActivityMemoryUsageMap);
        normalHeap.getPeakActivityUsage(usageMap);
        hugeHeap.getPeakActivityUsage(usageMap);
    }

    size32_t roundup(size32_t size) const
    {
        if (size <= FixedSizeHeaplet::maxHeapSize())
        {
            size += sizeof(atomic_t) + sizeof(unsigned);
            if (size<=32)
                return 32;
            else if (size<=64)
                return 64;
            else if (size<=128)
                return 128;
            else if (size<=256)
                return 256;
            else if (size<=512)
                return 512;
            else if (size<=1024)
                return 1024;
            else if (size<=2048)
                return 2048;
            else if (size <= FixedSizeHeaplet::maxHeapSize()/20)
                return (((size-1) / 4096)+1) * 4096;
            else
            {
                // Round up to nearest whole fraction of available heap space
                unsigned frac = FixedSizeHeaplet::dataAreaSize()/size;
                unsigned usesize = FixedSizeHeaplet::dataAreaSize()/frac;
                return usesize;
            }
        }
        else
        {
            // roundup of larger values is used to decide whether worth shrinking or not...
            unsigned numPages = ((size + HugeHeaplet::dataOffset() - 1) / HEAP_ALIGNMENT_SIZE) + 1;
            return (numPages*HEAP_ALIGNMENT_SIZE) - HugeHeaplet::dataOffset();
        }
    }

    virtual unsigned maxSimpleBlock()
    {
        return FixedSizeHeaplet::maxHeapSize();
    }

    virtual void *allocate(unsigned _size, unsigned activityId)
    {
        if (memTraceSizeLimit && _size > memTraceSizeLimit)
        {
            logctx.CTXLOG("Activity %u requesting %u bytes!", getActivityId(activityId), _size);
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
        if (_size > FixedSizeHeaplet::maxHeapSize())
        {
            return hugeHeap.doAllocate(_size, activityId);
        }
        else
        {
            unsigned needSize = roundup(_size);
            return normalHeap.doAllocate(needSize, activityId);
        }
    }

    virtual void setMemoryLimit(memsize_t bytes)
    {
        pageLimit = (unsigned) (bytes / HEAP_ALIGNMENT_SIZE);

        if (memTraceLevel >= 2)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::setMemoryLimit new memlimit=%"I64F"u pageLimit=%u rowMgr=%p", (unsigned __int64) bytes, pageLimit, this);
    }

    virtual void *resizeRow(void * original, size32_t oldsize, size32_t newsize, unsigned activityId, size32_t &capacity)
    {
        assertex(newsize);
        assertex(!HeapletBase::isShared(original));
        capacity = HeapletBase::capacity(original);
        if (newsize <= capacity)
        {
            if (newsize >= oldsize || roundup(newsize) == roundup(oldsize))
                return original;

            void *ret = allocate(newsize, activityId);
            memcpy(ret, original, newsize);
            HeapletBase::release(original);
            capacity = HeapletBase::capacity(ret);
            return ret;
        }
        else
        {
            void *ret = allocate(newsize, activityId);
            memcpy(ret, original, oldsize);
            HeapletBase::release(original);
            capacity = HeapletBase::capacity(ret);
            return ret;
        }
    }

    virtual void *finalizeRow(void * original, size32_t initialSize, size32_t finalSize, unsigned activityId)
    {
#ifdef _DEBUG
        assertex(finalSize);
        assertex(!HeapletBase::isShared(original));
        assertex(finalSize<=initialSize);
#endif
        if (finalSize==initialSize || roundup(finalSize) == roundup(initialSize))
        {
            // MORE - if we were paranoid we could assert that supplied activityId matched the one stored with the row
            if (activityId & ACTIVITY_FLAG_NEEDSDESTRUCTOR)
                HeapletBase::setDestructorFlag(original);
            return original;
        }
        else
        {
            // Shrink rows that were initially allocated larger than necessary. Expansion (if needed) has been done while building.
            // No need to setDestructorFlag here - it's already in the activityId we are passing in (if needed).
            void *ret = allocate(finalSize, activityId);
            memcpy(ret, original, finalSize);
            HeapletBase::release(original);
            return ret;
        }
    }

    virtual unsigned getMemoryUsage()
    {
        if (usageMap)
            usageMap->report(logctx, allocatorCache);
        return peakPages;
    }

    virtual bool attachDataBuff(DataBuffer *dataBuff) 
    {
        SpinBlock b(crit);
        
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: attachDataBuff() attaching DataBuff to rowMgr - addr=%p dataBuffs=%u dataBuffPages=%u possibleGoers=%u rowMgr=%p", 
                    dataBuff, dataBuffs, dataBuffPages, atomic_read(&possibleGoers), this);

        LINK(dataBuff);
        DataBufferBase *finger = activeBuffs;
        DataBufferBase *last = NULL;
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
        bool needCheck = (ndataBuffPages > dataBuffPages);
        dataBuffPages = ndataBuffPages;
        if (needCheck)
            checkLimit(0);
        return true;
    }
    
    virtual void noteDataBuffReleased(DataBuffer *dataBuff)
    {
        atomic_inc(&possibleGoers);
        if (memTraceLevel >= 4)
            logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::noteDataBuffReleased dataBuffs=%u dataBuffPages=%u possibleGoers=%u dataBuff=%p rowMgr=%p", 
                    dataBuffs, dataBuffPages, atomic_read(&possibleGoers), dataBuff, this);
    }

    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, RoxieHeapFlags flags)
    {
        //Although the activityId is passed here, there is nothing to stop multiple RowHeaps sharing the same ChunkAllocator
        return new CRoxieFixedRowHeap(this, fixedSize, activityId, flags);
    }

    virtual IVariableRowHeap * createVariableRowHeap(unsigned activityId, RoxieHeapFlags flags)
    {
        //Although the activityId is passed here, there is nothing to stop multiple RowHeaps sharing the same ChunkAllocator
        return new CRoxieVariableRowHeap(this, activityId, flags);
    }

    void checkLimit(unsigned numRequested)
    {
        unsigned pageCount = pages();
        if (pageCount + numRequested > peakPages)
        {
            if (trackMemoryByActivity)
                getPeakActivityUsage();
            peakPages = pageCount + numRequested;

            if (pageLimit && peakPages > pageLimit)
            {
                logctx.CTXLOG("RoxieMemMgr: Memory limit exceeded - current %d, requested %d, limit %d", pageCount, numRequested, pageLimit);
                throw MakeStringException(ROXIE_MEMORY_LIMIT_EXCEEDED, "memory limit exceeded");
            }
        }
    }
};


void * CRoxieFixedRowHeap::allocate()
{
    return rowManager->allocate(fixedSize, activityId);
}

void * CRoxieFixedRowHeap::finalizeRow(void *final)
{
    //MORE: Checking heap checks for not shared.
    if (flags & RHFhasdestructor)
        HeapletBase::setDestructorFlag(final);
    return final;
}

void * CRoxieVariableRowHeap::allocate(size32_t size, size32_t & capacity)
{
    void * ret = rowManager->allocate(size, activityId);
    capacity = RoxieRowCapacity(ret);
    return ret;
}

void * CRoxieVariableRowHeap::resizeRow(void * original, size32_t oldsize, size32_t newsize, size32_t &capacity)
{
    return rowManager->resizeRow(original, oldsize, newsize, activityId, capacity);
}

void * CRoxieVariableRowHeap::finalizeRow(void *final, size32_t originalSize, size32_t finalSize)
{
    return rowManager->finalizeRow(final, originalSize, finalSize, activityId);
    //If never shrink the following should be sufficient.
    //MORE: Checking heap checks for not shared.
    if (flags & RHFhasdestructor)
        HeapletBase::setDestructorFlag(final);
    return final;
}

//================================================================================

//MORE: Make this a nested class??
void * CHugeChunkingHeap::doAllocate(unsigned _size, unsigned activityId)
{
    SpinBlock b(crit);
    unsigned numPages = ((_size + HugeHeaplet::dataOffset() - 1) / HEAP_ALIGNMENT_SIZE) + 1;
    rowManager->checkLimit(numPages);
    HugeHeaplet *head = new (_size) HugeHeaplet(allocatorCache, _size, activityId);
    if (memTraceLevel >= 2 || _size>= 10000000)
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %u) allocated new HugeHeaplet size %u - addr=%p pages=%u pageLimit=%u peakPages=%u rowMgr=%p",
                _size, (unsigned) (numPages*HEAP_ALIGNMENT_SIZE), head, numPages, rowManager->pageLimit, rowManager->peakPages, this);
    setNext(head, active);
    active = head;
    return head->allocateHuge(_size, activityId);
}

void * CNormalChunkingHeap::doAllocate(size32_t chunkSize, unsigned activityId)
{
    SpinBlock b(crit);
    BigHeapletBase *finger = active;
    BigHeapletBase *prev = NULL;
    while (finger)
    {
        void *ret = finger->allocate(chunkSize, activityId);
        if (ret)
        {
            if (prev)
            {
                BigHeapletBase * next = getNext(finger);
                setNext(prev, next);
                setNext(finger, active);
                active = finger;
            }
            return ret;
        }
        prev = finger;
        finger = getNext(finger);
    }
    rowManager->checkLimit(1); // MORE - could make this more efficient by not re-counting active
    FixedSizeHeaplet *head = new FixedSizeHeaplet(allocatorCache, chunkSize);
    if (memTraceLevel >= 5 || (memTraceLevel >= 3 && chunkSize > 32000))
        logctx.CTXLOG("RoxieMemMgr: CChunkingRowManager::allocate(size %u) allocated new FixedSizeHeaplet size %u - addr=%p pageLimit=%u peakPages=%u rowMgr=%p",
                chunkSize, chunkSize, head, rowManager->pageLimit, rowManager->peakPages, this);
    setNext(head, active);
    active = head;
    //Race if another thread allocates all the rows!
    return head->allocate(chunkSize, activityId);
}

//================================================================================
// Buffer manager - blocked 

void DataBufferBase::noteReleased(const void *ptr)
{
    if (atomic_dec_and_test(&count))
        released();
}

void DataBufferBase::noteLinked(const void *ptr)
{
    atomic_inc(&count);
}

bool DataBufferBase::_isShared(const void *ptr) const
{
    return atomic_read(&count) > 2; // The heaplet itself has a usage count of 1
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
    DataBufferBottom *bottom = (DataBufferBottom *) ((memsize_t) this & HEAP_ALIGNMENT_MASK);
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

size32_t DataBuffer::_capacity() const { throwUnexpected(); }
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
                else if (nextAddr - curBlock <= HEAP_ALIGNMENT_SIZE-DATA_ALIGNMENT_SIZE)
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
            curBlock = nextAddr = (char *) suballoc_aligned(1);
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

size32_t DataBufferBottom::_capacity() const { throwUnexpected(); }
void DataBufferBottom::_setDestructorFlag(const void *ptr) { throwUnexpected(); }


//================================================================================
//

#ifdef __new_was_defined
#define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

extern IRowManager *createRowManager(unsigned memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks)
{
    return new CChunkingRowManager(memLimit, tl, logctx, allocatorCache, ignoreLeaks);
}

extern void setMemoryStatsInterval(unsigned secs)
{
    statsLogFreq = secs;
}

extern void setTotalMemoryLimit(memsize_t max)
{
    totalMemoryLimit = (unsigned) (max / HEAP_ALIGNMENT_SIZE);
    if (memTraceLevel)
        DBGLOG("RoxieMemMgr: Setting memory limit to %"I64F"d bytes (%d pages)", (unsigned __int64) max, totalMemoryLimit);
    initializeHeap(totalMemoryLimit);
}

extern memsize_t getTotalMemoryLimit()
{
    return totalMemoryLimit;
}

extern bool memPoolExhausted()
{
   return(heapExhausted);
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
        throw MakeStringException(ROXIE_INVALID_MEMORY_ALIGNMENT, "Invalid parameter to setDataAlignmentSize %d", size);
}

} // namespace roxiemem

//============================================================================================================
#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>
#define ASSERT(a) { if (!(a)) CPPUNIT_ASSERT(a); }

namespace roxiemem {

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
        CPPUNIT_TEST(testHuge);
        CPPUNIT_TEST(testHeapletCas);
        CPPUNIT_TEST(testCas);
        CPPUNIT_TEST(testAll);
        CPPUNIT_TEST(testDatamanager);
        CPPUNIT_TEST(testBitmap);
        CPPUNIT_TEST(testDatamanagerThreading);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

public:
    RoxieMemTests() : logctx(queryDummyContextLogger())
    {
        initializeHeap(300);
    }

    ~RoxieMemTests()
    {
        releaseRoxieHeap();
    }

protected:
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

    void testBitmap()
    {
        char *_heapBase = heapBase;
        unsigned *_heapBitmap = heapBitmap;
        unsigned _heapBitmapSize = heapBitmapSize;
        unsigned _heapLWM = heapLWM;
        bool _heapExhausted = heapExhausted;

        try
        {
            heapBase = (char *) (memsize_t) 0x80000000;
            heapBitmap = new unsigned[32];
            memset(heapBitmap, 0xff, 32*sizeof(unsigned));
            heapBitmapSize = 32;
            heapLWM = 0;
            heapExhausted = false;

            unsigned i;
            for (i=0; i < 100; i++)
            {
                ASSERT(suballoc_aligned(1)==(void *)(memsize_t)(0x80000000 + 0x100000*i));
                ASSERT(suballoc_aligned(3)==(void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)));
            }
            for (i=0; i < 100; i+=2)
            {
                subfree_aligned((void *)(memsize_t)(0x80000000 + 0x100000*i), 1);
                subfree_aligned((void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)), 3);
            }
            for (i=0; i < 100; i+=2)
            {
                ASSERT(suballoc_aligned(1)==(void *)(memsize_t)(0x80000000 + 0x100000*i));
                ASSERT(suballoc_aligned(3)==(void *)(memsize_t)(0xc0000000 - 0x300000*(i+1)));
            }
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
            heapBase = _heapBase;
            heapBitmap = _heapBitmap;
            heapBitmapSize = _heapBitmapSize;
            heapLWM = _heapLWM;
            heapExhausted = _heapExhausted;
        }
        catch (...)
        {
            heapBase = _heapBase;
            heapBitmap = _heapBitmap;
            heapBitmapSize = _heapBitmapSize;
            heapLWM = _heapLWM;
            heapExhausted = _heapExhausted;
            throw;
        }
    }

    void testHuge()
    {
        Owned<IRowManager> rm1 = createRowManager(0, NULL, logctx, NULL);
        ReleaseRoxieRow(rm1->allocate(1800000, 0));
        ASSERT(rm1->pages()==0);
        ASSERT(rm1->getMemoryUsage()==2);
    }

    void testAll()
    {
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
        ASSERT(rm1->pages()==0);
        ASSERT(rm1->getMemoryUsage()==1);

        void *r1 = rm1->allocate(1000, 0);
        void *r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm1->allocate(1000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm1->pages()==0);
        ASSERT(rm1->getMemoryUsage()==1);


        Owned<IRowManager> rm2 = createRowManager(0, NULL, logctx, NULL);
        ReleaseRoxieRow(rm2->allocate(4000000, 0));
        ASSERT(rm2->pages()==0);
        ASSERT(rm2->getMemoryUsage()==4);

        r1 = rm2->allocate(4000000, 0);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        r1 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r2);
        r2 = rm2->allocate(4000000, 0);
        ReleaseRoxieRow(r1);
        ReleaseRoxieRow(r2);
        ASSERT(rm2->pages()==0);
        ASSERT(rm2->getMemoryUsage()==8);

        for (unsigned d = 0; d < 50; d++)
        {
            Owned<IRowManager> rm3 = createRowManager(0, NULL, logctx, NULL);
            ReleaseRoxieRow(rm3->allocate(HEAP_ALIGNMENT_SIZE - d + 10, 0));
            ASSERT(rm3->pages()==0);
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
        ASSERT(rm1->pages()==20);
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
    enum { numCasThreads = 20, numCasIter = 500, numCasAlloc = 1000 };
    class HeapletCasAllocatorThread : public Thread
    {
    public:
        HeapletCasAllocatorThread(FixedSizeHeaplet * _heaplet, Semaphore & _sem) : Thread("CasAllocatorThread"), heaplet(_heaplet), sem(_sem)
        {
        }

        int run()
        {
            void * saved[numCasAlloc];
            sem.wait();
            //Allocate two rows and then release 1 trying to trigger potential ABA problems in the cas code.
            for (unsigned i=0; i < numCasIter; i++)
            {
                for (unsigned j=0; j < numCasAlloc; j++)
                {
                    //Allocate 2 rows, and add first back on the list again
                    void * alloc1 = heaplet->allocate(32, 0);
                    void * alloc2 = heaplet->allocate(32, 0);
                    *(unsigned*)alloc1 = 0xdddddddd;
                    *(unsigned*)alloc2 = 0xdddddddd;
                    ReleaseRoxieRow(alloc1);
                    saved[j] = alloc2;
                }
                for (unsigned j=0; j < numCasAlloc; j++)
                    ReleaseRoxieRow(saved[j]);
            }
            return 0;
        }
    protected:
        FixedSizeHeaplet * heaplet;
        Semaphore & sem;
    };
    void testHeapletCas()
    {
        FixedSizeHeaplet * heaplet = new FixedSizeHeaplet(NULL, 32);
        Semaphore sem;
        HeapletCasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new HeapletCasAllocatorThread(heaplet, sem);
        for (unsigned i2 = 0; i2 < numCasThreads; i2++)
            threads[i2]->start();

        unsigned startTime = msTick();
        sem.signal(numCasThreads);
        for (unsigned i3 = 0; i3 < numCasThreads; i3++)
            threads[i3]->join();
        unsigned endTime = msTick();

        for (unsigned i4 = 0; i4 < numCasThreads; i4++)
            threads[i4]->Release();
        delete heaplet;
        DBGLOG("Time taken for heaplet cas = %d", endTime-startTime);
    }
    class CasAllocatorThread : public Thread
    {
    public:
        CasAllocatorThread(IFixedRowHeap * _rowHeap, Semaphore & _sem) : Thread("CasAllocatorThread"), rowHeap(_rowHeap), sem(_sem)
        {
        }

        int run()
        {
            void * saved[numCasAlloc];
            sem.wait();
            //Allocate two rows and then release 1 trying to trigger potential ABA problems in the cas code.
            for (unsigned i=0; i < numCasIter; i++)
            {
                for (unsigned j=0; j < numCasAlloc; j++)
                {
                    //Allocate 2 rows, and add first back on the list again
                    void * alloc1 = rowHeap->allocate();
                    void * alloc2 = rowHeap->allocate();
                    *(unsigned*)alloc1 = 0xdddddddd;
                    *(unsigned*)alloc2 = 0xdddddddd;
                    ReleaseRoxieRow(alloc1);
                    saved[j] = alloc2;
                }
                for (unsigned j=0; j < numCasAlloc; j++)
                    ReleaseRoxieRow(saved[j]);
            }
            return 0;
        }
    protected:
        IFixedRowHeap * rowHeap;
        Semaphore & sem;
    };
    void testCas()
    {
        Owned<IRowManager> rowManager = createRowManager(0, NULL, logctx, NULL);
        Owned<IFixedRowHeap> rowHeap = rowManager->createFixedRowHeap(8, 0, RHFnone);
        Semaphore sem;
        CasAllocatorThread * threads[numCasThreads];
        for (unsigned i1 = 0; i1 < numCasThreads; i1++)
            threads[i1] = new CasAllocatorThread(rowHeap, sem);
        for (unsigned i2 = 0; i2 < numCasThreads; i2++)
            threads[i2]->start();

        unsigned startTime = msTick();
        sem.signal(numCasThreads);
        for (unsigned i3 = 0; i3 < numCasThreads; i3++)
            threads[i3]->join();
        unsigned endTime = msTick();

        for (unsigned i4 = 0; i4 < numCasThreads; i4++)
            threads[i4]->Release();
        DBGLOG("Time taken for row allocator cas = %d", endTime-startTime);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( RoxieMemTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( RoxieMemTests, "RoxieMemTests" );

} // namespace roxiemem
#endif
