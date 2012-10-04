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

#ifndef _ROXIEMEM_INCL
#define _ROXIEMEM_INCL
#include "jlib.hpp"
#include "jlog.hpp"
#include "jdebug.hpp"
#include "errorlist.h"

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
#define ROXIEMM_LARGE_MEMORY_EXHAUSTED    ROXIEMM_ERROR_START+4


#ifdef __64BIT__
#define HEAP_ALIGNMENT_SIZE I64C(0x100000u)                     // 1 mb heaplets - may be too big?
#else
#define HEAP_ALIGNMENT_SIZE 0x100000                            // 1 mb heaplets - may be too big?
#endif
#define HEAP_ALIGNMENT_MASK ((~(HEAP_ALIGNMENT_SIZE)) + 1)
#define ACTIVITY_MASK   0x00ffffff                              // must be > HEAP_ALIGNMENT_SIZE
#define ACTIVITY_MAGIC  0xac000000                              // We use the top 8 bits of the activityId field (which overlays the freechain relative pointers) to detect invalid frees

// How the bits in ACTIVITY_MASK are broken down....
#define ACTIVITY_FLAG_NEEDSDESTRUCTOR   0x00800000
#define ACTIVITY_FLAG_ISREGISTERED      0x00400000
#define MAX_ACTIVITY_ID                 0x003fffff

#define ALLOC_ALIGNMENT                 sizeof(void *)          // Minimum alignment of data allocated from the heap manager
#define PACKED_ALIGNMENT                4                       // Minimum alignment of packed blocks

#define MAX_FRAC_ALLOCATOR              20

//================================================================================
// Roxie heap

namespace roxiemem {

interface IRowAllocatorCache
{
    virtual unsigned getActivityId(unsigned cacheId) const = 0;
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const = 0;
    virtual void onDestroy(unsigned cacheId, void *row) const = 0;
    virtual void checkValid(unsigned cacheId, const void *row) const = 0;
};

//This interface allows activities that hold on to large numbers of rows to be called back to try and free up
//memory.  E.g., sorts can spill to disk, read ahead buffers can reduce the number being readahead etc.
//Lower priority callbacks are called before higher priority.
//The freeBufferedRows will call all callbacks with critical=false, before calling with critical=true
interface IBufferedRowCallback
{
    virtual unsigned getPriority() const = 0; // lower values get freed up first.
    virtual bool freeBufferedRows(bool critical) = 0; // return true if and only if managed to free something.
};

//This interface is called to gain exclusive access to a memory blocks shared between processes.
//MORE: The block size needs to be consistent for all thors on this node
interface ILargeMemCallback: extends IInterface
{
    virtual bool take(memsize_t largeMemory)=0;   // called when a memory request about to be satisfied will require an extra block.
    virtual void give(memsize_t largeMemory)=0;   // called when the memory allocated falls back below the limit
};


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

    inline static HeapletBase *findBase(const void *ptr)
    {
        return (HeapletBase *) ((memsize_t) ptr & HEAP_ALIGNMENT_MASK);
    }

public:
    inline bool isAlive() const
    {
        return atomic_read(&count) < DEAD_PSEUDO_COUNT;        //only safe if Link() is called first
    }

    static void release(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            h->noteReleased(ptr);
        }
    }

    static bool isShared(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            return h->_isShared(ptr);
        }
        // isShared(NULL) or isShared on an object that shares a link-count is an error
        throwUnexpected();
    }

    static memsize_t capacity(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            //MORE: If capacity was always the size stored in the first word of the block this could be non virtual
            //and the whole function could be inline.
            return h->_capacity();
        }
        throwUnexpected();
    }

    static void setDestructorFlag(const void *ptr)
    {
        dbgassertex(ptr);
        HeapletBase *h = findBase(ptr);
        h->_setDestructorFlag(ptr);
    }

    static bool hasDestructor(const void *ptr)
    {
        dbgassertex(ptr);
        HeapletBase *h = findBase(ptr);
        return h->_hasDestructor(ptr);
    }

    static unsigned getAllocatorId(const void *ptr)
    {
        dbgassertex(ptr);
        HeapletBase *h = findBase(ptr);
        unsigned id = h->_rawAllocatorId(ptr);
        return (id & ACTIVITY_MASK);
    }

    static void releaseClear(const void *&ptr)
    {
        if (ptr)
        {
            release(ptr);
            ptr = NULL;
        }
    }

    static void releaseClear(void *&ptr)
    {
        if (ptr)
        {
            release(ptr);
            ptr = NULL;
        }
    }

    static void link(const void *ptr)
    {
        HeapletBase *h = findBase(ptr);
        h->noteLinked(ptr);
    }

    inline unsigned queryCount() const
    {
        return atomic_read(&count);
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

public:
    DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain);

    void addToFreeChain(DataBufferBase * buffer);
};

//Actions applied to roxie rows
#define ReleaseRoxieRow(row) roxiemem::HeapletBase::release(row)
#define ReleaseClearRoxieRow(row) roxiemem::HeapletBase::releaseClear(row)
#define LinkRoxieRow(row) roxiemem::HeapletBase::link(row)

//Functions to determine information about roxie rows
#define RoxieRowCapacity(row)  roxiemem::HeapletBase::capacity(row)
#define RoxieRowHasDestructor(row)  roxiemem::HeapletBase::hasDestructor(row)
#define RoxieRowAllocatorId(row) roxiemem::HeapletBase::getAllocatorId(row)

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


interface IFixedRowHeap : extends IInterface
{
    virtual void *allocate() = 0;
    virtual void *finalizeRow(void *final) = 0;
};

interface IVariableRowHeap : extends IInterface
{
    virtual void *allocate(memsize_t size, memsize_t & capacity) = 0;
    virtual void *resizeRow(void * original, memsize_t oldsize, memsize_t newsize, memsize_t &capacity) = 0;
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize) = 0;
};

enum RoxieHeapFlags
{
    RHFnone             = 0x0000,
    RHFpacked           = 0x0001,
    RHFhasdestructor    = 0x0002,
    RHFunique           = 0x0004,  // create a separate fixed size allocator
    RHFoldfixed         = 0x0008,  // Don't create a special fixed size heap for this
};

// Variable size aggregated link-counted Roxie (etc) row manager
interface IRowManager : extends IInterface
{
    virtual void *allocate(memsize_t size, unsigned activityId) = 0;
    virtual void *resizeRow(void * original, memsize_t oldsize, memsize_t newsize, unsigned activityId, memsize_t &capacity) = 0;
    virtual void *finalizeRow(void *final, memsize_t originalSize, memsize_t finalSize, unsigned activityId) = 0;
    virtual void setMemoryLimit(memsize_t size, memsize_t spillSize = 0) = 0;
    virtual unsigned allocated() = 0;
    virtual unsigned numPagesAfterCleanup(bool forceFreeAll) = 0; // calls releaseEmptyPages() then returns
    virtual bool releaseEmptyPages(bool forceFreeAll) = 0; // ensures any empty pages are freed back to the heap
    virtual unsigned getMemoryUsage() = 0;
    virtual bool attachDataBuff(DataBuffer *dataBuff) = 0 ;
    virtual void noteDataBuffReleased(DataBuffer *dataBuff) = 0 ;
    virtual void setActivityTracking(bool val) = 0;
    virtual void reportLeaks() = 0;
    virtual void checkHeap() = 0;
    virtual IFixedRowHeap * createFixedRowHeap(size32_t fixedSize, unsigned activityId, unsigned roxieHeapFlags) = 0;
    virtual IVariableRowHeap * createVariableRowHeap(unsigned activityId, unsigned roxieHeapFlags) = 0;            // should this be passed the initial size?
    virtual void addRowBuffer(IBufferedRowCallback * callback) = 0;
    virtual void removeRowBuffer(IBufferedRowCallback * callback) = 0;
    virtual memsize_t getExpectedCapacity(memsize_t size, unsigned heapFlags) = 0; // what is the expected capacity for a given size allocation
    virtual memsize_t getExpectedFootprint(memsize_t size, unsigned heapFlags) = 0; // how much memory will a given size allocation actually use.
};

extern roxiemem_decl void setDataAlignmentSize(unsigned size);

interface ITimeLimiter 
{
    virtual void checkAbort() = 0 ;
};

interface IActivityMemoryUsageMap : public IInterface
{
    virtual void noteMemUsage(unsigned activityId, unsigned memUsed) = 0;
    virtual void report(const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache) = 0;
};

extern roxiemem_decl IRowManager *createRowManager(memsize_t memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks = false);

// Fixed size aggregated link-counted zero-overhead data Buffer manager

interface IDataBufferManager : extends IInterface
{
    virtual DataBuffer *allocate() = 0;
    virtual void poolStats(StringBuffer &memStats) = 0;
};

extern roxiemem_decl IDataBufferManager *createDataBufferManager(size32_t size);
extern roxiemem_decl void setMemoryStatsInterval(unsigned secs);
extern roxiemem_decl void setTotalMemoryLimit(memsize_t max, memsize_t largeBlockSize, ILargeMemCallback * largeBlockCallback);
extern roxiemem_decl memsize_t getTotalMemoryLimit();
extern roxiemem_decl void releaseRoxieHeap();
extern roxiemem_decl bool memPoolExhausted();
extern roxiemem_decl unsigned memTraceLevel;
extern roxiemem_decl memsize_t memTraceSizeLimit;

extern roxiemem_decl atomic_t dataBufferPages;
extern roxiemem_decl atomic_t dataBuffersActive;

#define ALLOCATE(a) allocate(a, activityId)
#define CLONE(a,b) clone(a, b, activityId)
#define RESIZEROW(a,b,c) resizeRow(a, b, c, activityId)
#define SHRINKROW(a,b,c) resizeRow(a, b, c, activityId)

extern roxiemem_decl StringBuffer &memstats(StringBuffer &stats);
extern roxiemem_decl void memstats(unsigned &totalpg, unsigned &freepg, unsigned &maxblk);
extern roxiemem_decl IPerfMonHook *createRoxieMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

} // namespace roxiemem
#endif
