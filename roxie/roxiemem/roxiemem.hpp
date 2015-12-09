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

#if defined(_USE_TBB)
//Release blocks of rows in parallel - always likely to improve performance
#define PARALLEL_SYNC_RELEASE
#endif

#ifdef _WIN32
 #ifdef ROXIEMEM_EXPORTS
  #define roxiemem_decl __declspec(dllexport)
 #else
  #define roxiemem_decl __declspec(dllimport)
 #endif
#else
 #define roxiemem_decl
#endif

#define ROXIEMM_MEMORY_LIMIT_EXCEEDED     ROXIEMM_ERROR_START
#define ROXIEMM_MEMORY_POOL_EXHAUSTED     ROXIEMM_ERROR_START+1
#define ROXIEMM_INVALID_MEMORY_ALIGNMENT  ROXIEMM_ERROR_START+2
#define ROXIEMM_HEAP_ERROR                ROXIEMM_ERROR_START+3
#define ROXIEMM_TOO_MUCH_MEMORY           ROXIEMM_ERROR_START+4


#ifdef __64BIT__
#define HEAP_ALIGNMENT_SIZE I64C(0x40000u)                      // 256kb heaplets
#else
#define HEAP_ALIGNMENT_SIZE 0x40000                             // 256kb heaplets
#endif
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
struct roxiemem_decl HeapletBase
{
    friend class DataBufferBottom;
protected:
    atomic_t count;

    HeapletBase()
    {
        atomic_set(&count,1);  // Starts off active
    }

    virtual ~HeapletBase()
    {
    }

    virtual void noteReleased(const void *ptr) = 0;
    virtual bool _isShared(const void *ptr) const = 0;
    virtual memsize_t _capacity() const = 0;
    virtual void _setDestructorFlag(const void *ptr) = 0;
    virtual bool _hasDestructor(const void *ptr) const = 0;
    virtual unsigned _rawAllocatorId(const void *ptr) const = 0;
    virtual void noteLinked(const void *ptr) = 0;
    virtual const void * _compactRow(const void * ptr, HeapCompactState & state) = 0;
    virtual void _internalFreeNoDestructor(const void *ptr) = 0;

public:
    inline static HeapletBase *findBase(const void *ptr)
    {
        return (HeapletBase *) ((memsize_t) ptr & HEAP_ALIGNMENT_MASK);
    }

    inline bool isAlive() const
    {
        return atomic_read(&count) < DEAD_PSEUDO_COUNT;        //only safe if Link() is called first
    }

    static void release(const void *ptr);
    static void releaseRowset(unsigned count, byte * * rowset);
    static bool isShared(const void *ptr);
    static void link(const void *ptr);
    static memsize_t capacity(const void *ptr);
    static bool isWorthCompacting(const void *ptr);
    static const void * compactRow(const void * ptr, HeapCompactState & state);

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
        return atomic_read(&count);
    }

    inline bool isEmpty() const
    {
        return atomic_read(&count) == 1;
    }
};

extern roxiemem_decl unsigned DATA_ALIGNMENT_SIZE;  // Permissible values are 0x400 and 0x2000

#define DATA_ALIGNMENT_MASK ((~((memsize_t) DATA_ALIGNMENT_SIZE)) + 1)

class DataBuffer;
interface IDataBufferManager;
interface IRowManager;

class roxiemem_decl DataBufferBase : public HeapletBase
{
    friend class CDataBufferManager;
    friend class CChunkingRowManager;
    friend class DataBufferBottom;
protected:
    //Called when the last reference to this item is Released() - the object will be reused or freed later
    virtual void released() = 0;
    DataBufferBase()
    {
        next = NULL;
        mgr = NULL;
    }
    inline DataBufferBase *realBase(const void *ptr, memsize_t mask) const
    {
        return (DataBufferBase *) ((memsize_t) ptr & mask);
    }
    inline DataBufferBase *realBase(const void *ptr) const { return realBase(ptr, DATA_ALIGNMENT_MASK); }

public:
    // Link and release are used to keep count of the references to the buffers.
    void Link() 
    { 
        atomic_inc(&count); 
    }
    void Release();

    //These functions are called on rows allocated within the DataBuffers
    //They are called on DataBuffferBottom which maps them to the correct DataBuffer using realBase()
    virtual void noteReleased(const void *ptr);
    virtual void noteLinked(const void *ptr);
    virtual bool _isShared(const void *ptr) const;

protected:
    DataBufferBase *next;   // Used when chaining them together in rowMgr
    IRowManager *mgr;
};


class roxiemem_decl DataBuffer : public DataBufferBase
{
    friend class CDataBufferManager;
private:
    virtual void released();
    virtual memsize_t _capacity() const;
    virtual void _setDestructorFlag(const void *ptr);
    virtual bool _hasDestructor(const void *ptr) const { return false; }
    virtual unsigned _rawAllocatorId(const void *ptr) const { return 0; }
    virtual const void * _compactRow(const void * ptr, HeapCompactState & state) { return ptr; }
    virtual void _internalFreeNoDestructor(const void *ptr) { throwUnexpected(); }
protected:
    DataBuffer()
    {
        msgNext = NULL;
    }
public:
    DataBuffer *msgNext;    // Next databuffer in same slave message
    virtual bool attachToRowMgr(IRowManager *rowMgr);
    char data[1]; // actually DATA_PAYLOAD
};

#define DATA_PAYLOAD (roxiemem::DATA_ALIGNMENT_SIZE - sizeof(roxiemem::DataBuffer)) // actually should be offsetof(DataBuffer, data)

class CDataBufferManager;
class roxiemem_decl DataBufferBottom : public DataBufferBase
{
private:
    friend class CDataBufferManager;
    CDataBufferManager * volatile owner;
    atomic_t okToFree;
    DataBufferBottom *nextBottom;   // Used when chaining them together in CDataBufferManager 
    DataBufferBottom *prevBottom;   // Used when chaining them together in CDataBufferManager 
    DataBufferBase *freeChain;
    CriticalSection crit;

    virtual void released();
    virtual void noteReleased(const void *ptr);
    virtual bool _isShared(const void *ptr) const;
    virtual memsize_t _capacity() const;
    virtual void _setDestructorFlag(const void *ptr);
    virtual bool _hasDestructor(const void *ptr) const { return false; }
    virtual unsigned _rawAllocatorId(const void *ptr) const { return 0; }
    virtual void noteLinked(const void *ptr);
    virtual const void * _compactRow(const void * ptr, HeapCompactState & state) { return ptr; }
    virtual void _internalFreeNoDestructor(const void *ptr) { throwUnexpected(); }

public:
    DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain);

    void addToFreeChain(DataBufferBase * buffer);
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

    inline ~OwnedConstRoxieRow()                            { ReleaseRoxieRow(ptr); }
    
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

interface IFixedRowHeap : extends IInterface
{
    virtual void *allocate() = 0;
    virtual void *finalizeRow(void *final) = 0;
};

interface IVariableRowHeap : extends IInterface
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
};

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
    virtual unsigned getMemoryUsage() = 0;
    virtual bool attachDataBuff(DataBuffer *dataBuff) = 0 ;
    virtual void noteDataBuffReleased(DataBuffer *dataBuff) = 0 ;
    virtual void reportLeaks() = 0;
    virtual void checkHeap() = 0;
    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags, unsigned maxSpillCost = SpillAllCost) = 0;
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
    virtual void noteHeapUsage(memsize_t allocatorSize, RoxieHeapFlags heapFlags, memsize_t memReserved, memsize_t memUsed) = 0;
    virtual void report(const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache) = 0;
    virtual void reportStatistics(IStatisticTarget & target, unsigned detailtarget, const IRowAllocatorCache *allocatorCache) = 0;
};

extern roxiemem_decl IRowManager *createRowManager(memsize_t memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks = false, bool outputOOMReports = false);
extern roxiemem_decl IRowManager *createGlobalRowManager(memsize_t memLimit, memsize_t globalLimit, unsigned numSlaves, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks, bool outputOOMReports);

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

extern roxiemem_decl unsigned memTraceLevel;
extern roxiemem_decl memsize_t memTraceSizeLimit;


#define ALLOCATE(a) allocate(a, activityId)
#define CLONE(a,b) clone(a, b, activityId)

extern roxiemem_decl StringBuffer &memstats(StringBuffer &stats);
extern roxiemem_decl void memstats(unsigned &totalpg, unsigned &freepg, unsigned &maxblk);
extern roxiemem_decl IPerfMonHook *createRoxieMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

} // namespace roxiemem
#endif
