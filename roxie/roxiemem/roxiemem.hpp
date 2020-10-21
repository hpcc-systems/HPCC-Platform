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

#ifndef _ROXIEMEM_INCL
#define _ROXIEMEM_INCL
#include "platform.h"
#include "jlib.hpp"
#include "jlog.hpp"
#include "jdebug.hpp"
#include "jstats.h"
#include "errorlist.h"
#include <atomic>

#ifdef ROXIEMEM_EXPORTS
#define roxiemem_decl DECL_EXPORT
#else
#define roxiemem_decl DECL_IMPORT
#endif

#define ROXIEMM_MEMORY_LIMIT_EXCEEDED     ROXIEMM_ERROR_START
#define ROXIEMM_MEMORY_POOL_EXHAUSTED     ROXIEMM_ERROR_START+1
#define ROXIEMM_INVALID_MEMORY_ALIGNMENT  ROXIEMM_ERROR_START+2
#define ROXIEMM_HEAP_ERROR                ROXIEMM_ERROR_START+3
#define ROXIEMM_TOO_MUCH_MEMORY           ROXIEMM_ERROR_START+4
#define ROXIEMM_RELEASE_ALL_SHARED_HEAP   ROXIEMM_ERROR_START+5
// NB: max ROXIEMM_* error is ROXIEMM_ERROR_END (see errorlist.h)

#ifdef __64BIT__
#define HEAP_ALIGNMENT_SIZE I64C(0x40000u)                      // 256kb heaplets
#else
#define HEAP_ALIGNMENT_SIZE 0x40000                             // 256kb heaplets
#endif
#define HEAP_PAGE_OFFSET_MASK (HEAP_ALIGNMENT_SIZE-1)
#define HEAP_ALIGNMENT_MASK ((~(HEAP_ALIGNMENT_SIZE)) + 1)
#define ACTIVITY_MASK   0x00ffffff                              // must be > HEAP_ALIGNMENT_SIZE
#define ACTIVITY_MAGIC  0xac000000                              // We use the top 8 bits of the activityId field (which overlays the freechain relative pointers) to detect invalid frees

// How the bits in ACTIVITY_MASK are broken down....
#define ACTIVITY_FLAG_NEEDSDESTRUCTOR   0x00800000
#define ACTIVITY_FLAG_ISREGISTERED      0x00400000
#define MAX_ACTIVITY_ID                 0x003fffff
// MAX_ACTIVITY_ID is further subdivided:
#define ALLOCATORID_CHECK_MASK          0x00300000
#define ALLOCATORID_MASK                0x000fffff
#define UNKNOWN_ROWSET_ID               0x000F8421              // Use as the allocatorId for a rowset from an unknown activity
#define UNKNOWN_ACTIVITY                123456789

#define MAX_SIZE_DIRECT_BUCKET          2048                    // Sizes below this are directly mapped to a particular bucket
#define ALLOC_ALIGNMENT                 sizeof(void *)          // Minimum alignment of data allocated from the heap manager
#define PACKED_ALIGNMENT                4                       // Minimum alignment of packed blocks

#define MAX_FRAC_ALLOCATOR              20

//================================================================================
// Roxie heap

namespace roxiemem {

interface IRowAllocatorCache : extends IInterface
{
    virtual unsigned getActivityId(unsigned cacheId) const = 0;
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const = 0;
    virtual void onDestroy(unsigned cacheId, void *row) const = 0;
    virtual void onClone(unsigned cacheId, void *row) const = 0;
    virtual void checkValid(unsigned cacheId, const void *row) const = 0;
};

//This interface allows activities that hold on to large numbers of rows to be called back to try and free up
//memory.  E.g., sorts can spill to disk, read ahead buffers can reduce the number being readahead etc.
//Lower cost callbacks are called before higher cost.
//The freeBufferedRows will call all callbacks with critical=false, before calling with critical=true
const static unsigned SpillAllCost = (unsigned)-1;
interface IBufferedRowCallback
{
    virtual unsigned getSpillCost() const = 0; // lower values get freed up first.
    virtual unsigned getActivityId() const = 0;
    virtual bool freeBufferedRows(bool critical) = 0; // return true if and only if managed to free something.
};

//This interface is called to gain exclusive access to a memory blocks shared between processes.
//MORE: The block size needs to be consistent for all thors on this node
interface ILargeMemCallback: extends IInterface
{
    virtual bool take(memsize_t largeMemory)=0;   // called when a memory request about to be satisfied will require an extra block.
    virtual void give(memsize_t largeMemory)=0;   // called when the memory allocated falls back below the limit
};

//This class allows pointers to allocated heap rows to be stored in less memory by taking minimal alignment and
//the size of the  heap into account.
//It should not be used for pointers within allocated blocks, since the alignment may not be correct.
class HeapPointerCompressor
{
public:
    HeapPointerCompressor();
    void compress(void * target, const void * ptr) const;
    void * decompress(const void * source) const;
    size32_t getSize() const { return compressedSize; }
protected:
    size32_t compressedSize;
};

class HeapCompactState;
class NewHeapCompactState;
struct roxiemem_decl HeapletBase
{
    friend class DataBufferBottom;
protected:
    mutable std::atomic_uint count;

    HeapletBase() : count(1) // Starts off active
    {
    }

    virtual ~HeapletBase()
    {
    }

    virtual void noteReleased(const void *ptr) = 0;
    virtual void noteReleased(unsigned count, const byte * * rowset) = 0;
    virtual bool _isShared(const void *ptr) const = 0;
    virtual memsize_t _capacity() const = 0;
    virtual void _setDestructorFlag(const void *ptr) = 0;
    virtual bool _hasDestructor(const void *ptr) const = 0;
    virtual unsigned _rawAllocatorId(const void *ptr) const = 0;
    virtual void noteLinked(const void *ptr) = 0;
    virtual const void * _compactRow(const void * ptr, HeapCompactState & state) = 0;
    virtual void _prepareToCompactRow(const void * ptr, NewHeapCompactState & state) = 0;
    virtual const void * _newCompactRow(const void * ptr, NewHeapCompactState & state) = 0;
    virtual void _internalFreeNoDestructor(const void *ptr) = 0;

public:
    inline bool isAlive() const
    {
        return count.load(std::memory_order_relaxed) < DEAD_PSEUDO_COUNT;        //only safe if Link() is called first
    }

    static void release(const void *ptr);
    static void releaseRowset(unsigned count, const byte * * rowset);
    static bool isShared(const void *ptr);
    static void link(const void *ptr);
    static memsize_t capacity(const void *ptr);
    static bool isWorthCompacting(const void *ptr);
    static const void * compactRow(const void * ptr, HeapCompactState & state);
    static void prepareToCompactRow(const void * ptr, NewHeapCompactState & state);
    static const void * newCompactRow(const void * ptr, NewHeapCompactState & state);

    static void setDestructorFlag(const void *ptr);
    static bool hasDestructor(const void *ptr);

    static void internalFreeNoDestructor(const void *ptr);

    static inline void releaseClear(const void *&ptr)
    {
        release(ptr);
        ptr = NULL;
    }

    static inline void releaseClear(void *&ptr)
    {
        release(ptr);
        ptr = NULL;
    }

    inline unsigned queryCount() const
    {
        return count.load(std::memory_order_relaxed);
    }

    virtual unsigned calcNumAllocated(bool updateCount) const;
    virtual bool isEmpty()
    {
        return queryCount() == 1;
    }
};

extern roxiemem_decl unsigned DATA_ALIGNMENT_SIZE;  // Permissible values are 0x400 and 0x2000

#define DATA_ALIGNMENT_MASK ((~((memsize_t) DATA_ALIGNMENT_SIZE)) + 1)

class DataBuffer;
interface IDataBufferManager;
interface IRowManager;

class roxiemem_decl DataBuffer
{
    friend class CDataBufferManager;
    friend class CChunkingRowManager;
private:
    void released();

protected:
    DataBuffer() : count(1)
    {
    }
public:
    // Link and release are used to keep count of the references to the buffers.
    void Link()
    {
        count.fetch_add(1, std::memory_order_relaxed);
    }
    void Release();
    inline unsigned queryCount() const
    {
        return count.load(std::memory_order_relaxed);
    }
    void noteReleased(const void *ptr);
    void noteLinked(const void *ptr);
public:
    std::atomic_uint count;
    IRowManager *mgr = nullptr;
    DataBuffer *next = nullptr;   // Used when chaining them together in rowMgr
    DataBuffer *msgNext = nullptr;    // Next databuffer in same slave message
    bool attachToRowMgr(IRowManager *rowMgr);
    char data[1]; // actually DATA_PAYLOAD
};

#define DATA_PAYLOAD (roxiemem::DATA_ALIGNMENT_SIZE - sizeof(roxiemem::DataBuffer)) // actually should be offsetof(DataBuffer, data)

class CDataBufferManager;
class roxiemem_decl DataBufferBottom : public HeapletBase
{
private:
    friend class CDataBufferManager;
    CDataBufferManager * volatile owner;
    std::atomic_uint okToFree;      // use uint since it is more efficient on some architectures
    DataBufferBottom *nextBottom;   // Used when chaining them together in CDataBufferManager 
    DataBufferBottom *prevBottom;   // Used when chaining them together in CDataBufferManager 
    DataBuffer *freeChain;
    CriticalSection crit;

    void released();

    virtual void noteReleased(const void *ptr) override;
    virtual void noteReleased(unsigned count, const byte * * rowset) override;
    virtual bool _isShared(const void *ptr) const;
    virtual memsize_t _capacity() const;
    virtual void _setDestructorFlag(const void *ptr);
    virtual bool _hasDestructor(const void *ptr) const { return false; }
    virtual unsigned _rawAllocatorId(const void *ptr) const { return 0; }
    virtual void noteLinked(const void *ptr);
    virtual const void * _compactRow(const void * ptr, HeapCompactState & state) { return ptr; }
    virtual void _prepareToCompactRow(const void * ptr, NewHeapCompactState & state) { }
    virtual const void * _newCompactRow(const void * ptr, NewHeapCompactState & state) { return ptr; }
    virtual void _internalFreeNoDestructor(const void *ptr) { throwUnexpected(); }

    inline DataBuffer * queryDataBuffer(const void *ptr) const
    {
        if ((((memsize_t)ptr) & HEAP_PAGE_OFFSET_MASK) == 0)
            return NULL;
        return (DataBuffer *) ((memsize_t) ptr & DATA_ALIGNMENT_MASK);
    }

public:
    DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain);

    void addToFreeChain(DataBuffer * buffer);
    void Link() { count.fetch_add(1, std::memory_order_relaxed); }
    void Release();
};

//Actions applied to roxie rows
#define ReleaseRoxieRow(row) roxiemem::HeapletBase::release(row)
#define ReleaseClearRoxieRow(row) roxiemem::HeapletBase::releaseClear(row)
#define LinkRoxieRow(row) roxiemem::HeapletBase::link(row)

#define ReleaseRoxieRowset(cnt, rowset) roxiemem::HeapletBase::releaseRowset(cnt, rowset)
#define LinkRoxieRowset(rowset) roxiemem::HeapletBase::link(rowset)

//Functions to determine information about roxie rows
#define RoxieRowCapacity(row)  roxiemem::HeapletBase::capacity(row)
#define RoxieRowHasDestructor(row)  roxiemem::HeapletBase::hasDestructor(row)
#define RoxieRowAllocatorId(row) roxiemem::HeapletBase::getAllocatorId(row)
#define RoxieRowIsShared(row)  roxiemem::HeapletBase::isShared(row)

void roxiemem_decl ReleaseRoxieRowArray(size_t count, const void * * rows);
void roxiemem_decl ReleaseRoxieRowRange(const void * * rows, size_t from, size_t to);
void roxiemem_decl ReleaseRoxieRows(ConstPointerArray &rows);


class OwnedRoxieRow;
class OwnedConstRoxieRow
{
public:
    inline OwnedConstRoxieRow()                             { ptr = NULL; }
    inline OwnedConstRoxieRow(const void * _ptr)            { ptr = _ptr; }
    inline OwnedConstRoxieRow(const OwnedConstRoxieRow & other) { ptr = other.getLink(); }

    inline ~OwnedConstRoxieRow()                            { if (ptr) ReleaseRoxieRow(ptr); }
    
private: 
    /* these overloaded operators are the devil of memory leak. Use set, setown instead. */
    void operator = (const void * _ptr)              { set(_ptr);  }
    void operator = (const OwnedConstRoxieRow & other) { set(other.get());  }

    /* this causes -ve memory leak */
    void setown(const OwnedConstRoxieRow &other) {  }
    inline OwnedConstRoxieRow(const OwnedRoxieRow & other)  { }

public:
    inline const void * operator -> () const        { return ptr; } 
    inline operator const void *() const            { return ptr; } 
    
    inline void clear()                     { const void *temp=ptr; ptr=NULL; ReleaseRoxieRow(temp); }
    inline const void * get() const             { return ptr; }
    inline const void * getClear()              { const void * temp = ptr; ptr = NULL; return temp; }
    inline const void * getLink() const         { LinkRoxieRow(ptr); return ptr; }
    inline void set(const void * _ptr)          { const void * temp = ptr; if (_ptr) LinkRoxieRow(_ptr); ptr = _ptr; ReleaseRoxieRow(temp); }
    inline void setown(const void * _ptr)           { const void * temp = ptr; ptr = _ptr; ReleaseRoxieRow(temp); }
    
    inline void set(const OwnedConstRoxieRow &other) { set(other.get()); }
    
private:
    const void * ptr;
};



class OwnedRoxieRow
{
public:
    inline OwnedRoxieRow()                              { ptr = NULL; }
    inline OwnedRoxieRow(void * _ptr)                   { ptr = _ptr; }
    inline OwnedRoxieRow(const OwnedRoxieRow & other)   { ptr = other.getLink(); }

    inline ~OwnedRoxieRow()                             { ReleaseRoxieRow(ptr); }
    
private: 
    /* these overloaded operators are the devil of memory leak. Use set, setown instead. */
    void operator = (void * _ptr)                { set(_ptr);  }
    void operator = (const OwnedRoxieRow & other) { set(other.get());  }

    /* this causes -ve memory leak */
    void setown(const OwnedRoxieRow &other) {  }

public:
    inline void * operator -> () const      { return ptr; } 
    inline operator void *() const          { return ptr; } 
    
    inline void clear()                     { void *temp=ptr; ptr=NULL; ReleaseRoxieRow(temp); }
    inline void * get() const               { return ptr; }
    inline void * getClear()                { void * temp = ptr; ptr = NULL; return temp; }
    inline void * getLink() const           { LinkRoxieRow(ptr); return ptr; }
    inline void set(void * _ptr)            { void * temp = ptr; if (_ptr) LinkRoxieRow(_ptr); ptr = _ptr; ReleaseRoxieRow(temp); }
    inline void setown(void * _ptr)         { void * temp = ptr; ptr = _ptr; ReleaseRoxieRow(temp); }
    
    inline void set(const OwnedRoxieRow &other) { set(other.get()); }
    
private:
    void * ptr;
};

class OwnedRoxieString
{
public:
    inline OwnedRoxieString()                               { ptr = NULL; }
    inline OwnedRoxieString(const char * _ptr)             { ptr = _ptr; }
    inline OwnedRoxieString(const OwnedRoxieString & other) { ptr = other.getLink(); }

    inline ~OwnedRoxieString() { ReleaseRoxieRow(ptr); }

    inline operator const char *() const { return ptr; }
    inline const char * get() const { return ptr; }
    inline const char * getLink() const { LinkRoxieRow(ptr); return ptr; }
    inline const char * set(const char * _ptr) { const char * temp = ptr; if (_ptr) LinkRoxieRow(_ptr); ptr = _ptr; ReleaseRoxieRow(temp); return ptr; }
    inline const char * setown(const char * _ptr) { const char * temp = ptr; ptr = _ptr; ReleaseRoxieRow(temp); return ptr; }
    inline void clear() { const char * temp = ptr; ptr = NULL; ReleaseRoxieRow(temp);  }
private:
    /* Disable use of some constructs that often cause memory leaks by creating private members */
    void operator = (const void * _ptr)              {  }
    void operator = (const OwnedRoxieString & other) { }
    void setown(const OwnedRoxieString &other) {  }

private:
    const char * ptr;
};


interface IRowHeap : extends IInterface
{
    virtual void gatherStats(CRuntimeStatisticCollection & stats) = 0;
};

interface IFixedRowHeap : extends IRowHeap
{
    virtual void *allocate() = 0;
    virtual void *finalizeRow(void *final) = 0;
    virtual void emptyCache() = 0;
    virtual void releaseAllRows() = 0; // Release any active heaplets and rows within those heaplets.  Use with extreme care.
};

interface IVariableRowHeap : extends IRowHeap
{
    virtual void *allocate(memsize_t size, memsize_t & capacity) = 0;
    virtual void *resizeRow(void * original, memsize_t copysize, memsize_t newsize, memsize_t &capacity) = 0;
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize) = 0;
};

enum RoxieHeapFlags
{
    RHFnone             = 0x0000,
    RHFpacked           = 0x0001,
    RHFhasdestructor    = 0x0002,
    RHFunique           = 0x0004,  // create a separate fixed size allocator
    RHFoldfixed         = 0x0008,  // Don't create a special fixed size heap for this
    RHFvariable         = 0x0010,  // only used for tracing
    RHFblocked          = 0x0040,  // allocate blocks of rows
    RHFscanning         = 0x0080,  // scan the heaplet for free items instead of using a free list
    RHFdelayrelease     = 0x0100,

    //internal flags
    RHFhuge             = 0x40000000,   // only used for tracing
    RHForphaned         = 0x80000000,   // heap will no longer be used, can be deleted
};
inline RoxieHeapFlags operator | (RoxieHeapFlags l, RoxieHeapFlags r) { return (RoxieHeapFlags)((unsigned)l | (unsigned)r); }

//This interface is here to allow atomic updates to allocations when they are being resized.  There are a few complications:
//- If a new block is allocated, we just need to update the capacity/pointer atomically
//  but they need to be updated at the same time, otherwise a spill occuring after the pointer update, but before
//  the resizeRow returns could lead to an out of date capacity.
//- If a block is resized by expanding earlier then the pointer needs to be locked while the data is being copied.
//- If any intermediate function adds extra bytes to the amount to be allocated it will need a local implementation
//  to apply a delta to the values being passed through.
//
//NOTE: update will not be called if the allocation was already large enough => the size must be set to the capacity.

interface IRowResizeCallback
{
    virtual void lock() = 0; // prevent access to the row pointer
    virtual void unlock() = 0; // allow access to the row pointer
    virtual void update(memsize_t capacity, void * ptr) = 0; // update the capacity, row pointer while a lock is held.
    virtual void atomicUpdate(memsize_t capacity, void * ptr) = 0; // update the row pointer while no lock is held
};

// Variable size aggregated link-counted Roxie (etc) row manager
interface IRowManager : extends IInterface
{
    virtual void *allocate(memsize_t size, unsigned activityId) = 0;
    virtual void *allocate(memsize_t _size, unsigned activityId, unsigned maxSpillCost) = 0;
    virtual const char *cloneVString(const char *str) = 0;
    virtual const char *cloneVString(size32_t len, const char *str) = 0;
    virtual void resizeRow(void * original, memsize_t copysize, memsize_t newsize, unsigned activityId, unsigned maxSpillCost, IRowResizeCallback & callback) = 0;
    virtual void resizeRow(memsize_t & capacity, void * & original, memsize_t copysize, memsize_t newsize, unsigned activityId) = 0;
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize, unsigned activityId) = 0;
    virtual unsigned allocated() = 0;
    virtual unsigned numPagesAfterCleanup(bool forceFreeAll) = 0; // calls releaseEmptyPages() then returns
    virtual memsize_t getMemoryUsage() = 0;
    virtual bool attachDataBuff(DataBuffer *dataBuff) = 0 ;
    virtual void noteDataBuffReleased(DataBuffer *dataBuff) = 0 ;
    virtual void reportLeaks() = 0;
    virtual void checkHeap() = 0;
    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags) = 0;
    virtual IVariableRowHeap * createVariableRowHeap(unsigned activityId, unsigned roxieHeapFlags) = 0;            // should this be passed the initial size?
    virtual void addRowBuffer(IBufferedRowCallback * callback) = 0;
    virtual void removeRowBuffer(IBufferedRowCallback * callback) = 0;
    virtual void reportMemoryUsage(bool peak) const = 0;
    virtual memsize_t compactRows(memsize_t count, const void * * rows) = 0;
    virtual memsize_t getExpectedCapacity(memsize_t size, unsigned heapFlags) = 0; // what is the expected capacity for a given size allocation
    virtual memsize_t getExpectedFootprint(memsize_t size, unsigned heapFlags) = 0; // how much memory will a given size allocation actually use.
    virtual void reportPeakStatistics(IStatisticTarget & target, unsigned detail) = 0;

//Allow various options to be configured
    virtual void setActivityTracking(bool val) = 0;
    virtual void setMemoryLimit(memsize_t size, memsize_t spillSize = 0, unsigned backgroundReleaseCost = SpillAllCost) = 0;  // First size is max memory, second is the limit which will trigger a background thread to reduce memory

    //set the number of callbacks that successfully free some memory before deciding it is good enough.
    //Default is 1, use -1 to free all possible memory whenever an out of memory occurs
    virtual void setMemoryCallbackThreshold(unsigned value) = 0;

    //If set release callbacks are called on separate threads - it maximises the likelihood of hitting a deadlock.
    virtual void setCallbackOnThread(bool value) = 0;
    //If enabled, callbacks will be called whenever a new block of memory needs to be allocated
    virtual void setMinimizeFootprint(bool value, bool critical) = 0;
    //If set, and changes to the callback list always triggers the callbacks to be called.
    virtual void setReleaseWhenModifyCallback(bool value, bool critical) = 0;
    virtual IRowManager * querySlaveRowManager(unsigned slave) = 0;  // 0..numSlaves-1
};

extern roxiemem_decl void setDataAlignmentSize(unsigned size);

#define MIN_ABORT_CHECK_INTERVAL 10
#define MAX_ABORT_CHECK_INTERVAL 5000

interface ITimeLimiter 
{
    virtual void checkAbort() = 0;
    virtual unsigned checkInterval() const = 0;
};

interface IActivityMemoryUsageMap : public IInterface
{
    virtual void noteMemUsage(unsigned activityId, memsize_t memUsed, unsigned numAllocs) = 0;
    virtual void noteHeapUsage(memsize_t allocatorSize, RoxieHeapFlags heapFlags, memsize_t memReserved, memsize_t memUsed, unsigned allocatorId) = 0;
    virtual void report(const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache) = 0;
    virtual void reportStatistics(IStatisticTarget & target, unsigned detailtarget, const IRowAllocatorCache *allocatorCache) = 0;
};

extern roxiemem_decl IRowManager *createRowManager(memsize_t memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool outputOOMReports);
extern roxiemem_decl IRowManager *createGlobalRowManager(memsize_t memLimit, memsize_t globalLimit, unsigned numSlaves, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, const IRowAllocatorCache **slaveAllocatorCaches, bool outputOOMReports);

// Fixed size aggregated link-counted zero-overhead data Buffer manager

interface IDataBufferManager : extends IInterface
{
    virtual DataBuffer *allocate() = 0;
    virtual void poolStats(StringBuffer &memStats) = 0;
};

extern roxiemem_decl IDataBufferManager *createDataBufferManager(size32_t size);
extern roxiemem_decl void setMemoryStatsInterval(unsigned secs);
extern roxiemem_decl void setTotalMemoryLimit(bool allowHugePages, bool allowTransparentHugePages, bool retainMemory, memsize_t max, memsize_t largeBlockSize, const unsigned * allocSizes, ILargeMemCallback * largeBlockCallback);
extern roxiemem_decl memsize_t getTotalMemoryLimit();
extern roxiemem_decl void releaseRoxieHeap();
extern roxiemem_decl bool memPoolExhausted();
extern roxiemem_decl unsigned getHeapAllocated();
extern roxiemem_decl unsigned getHeapPercentAllocated();
extern roxiemem_decl unsigned getDataBufferPages();
extern roxiemem_decl unsigned getDataBuffersActive();

//Various options to stress the memory

extern roxiemem_decl void setMemTraceLevel(unsigned value);
extern roxiemem_decl void setMemTraceSizeLimit(memsize_t value);


#define ALLOCATE(a) allocate(a, activityId)
#define CLONE(a,b) clone(a, b, activityId)

extern roxiemem_decl StringBuffer &memstats(StringBuffer &stats);
extern roxiemem_decl void memstats(unsigned &totalpg, unsigned &freepg, unsigned &maxblk);
extern roxiemem_decl IPerfMonHook *createRoxieMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor
extern roxiemem_decl size_t getRelativeRoxiePtr(const void *_ptr); // Useful for debugging - to provide a value that is consistent from run to run

} // namespace roxiemem
#endif
