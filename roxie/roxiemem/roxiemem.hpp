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

#ifndef _ROXIEMEM_INCL
#define _ROXIEMEM_INCL
#include "jlib.hpp"
#include "jlog.hpp"
#include "jdebug.hpp"
#include "roxie.hpp"

#ifdef _WIN32
 #ifdef ROXIEMEM_EXPORTS
  #define roxiemem_decl __declspec(dllexport)
 #else
  #define roxiemem_decl __declspec(dllimport)
 #endif
#else
 #define roxiemem_decl
#endif

#ifdef _DEBUG
#define CHECKING_HEAP
#endif

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

//================================================================================
// Roxie heap

#define NOTE_RELEASES       1
#define EXTRA_DEBUG_INFO    2

namespace roxiemem {

interface IRowAllocatorCache
{
    virtual unsigned getActivityId(unsigned cacheId) const = 0;
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const = 0;
    virtual void onDestroy(unsigned cacheId, void *row) const = 0;
};

struct roxiemem_decl HeapletBase
{
protected:
    memsize_t mask;
    atomic_t count;
    unsigned flags;

    HeapletBase(memsize_t _mask)
    {
        mask = _mask;
        atomic_set(&count,1);  // Starts off active
        flags = 0;
    }

    virtual ~HeapletBase()
    {
    }

    virtual void released() = 0;
    virtual void noteReleased(const void *ptr) = 0;
    virtual bool _isShared(const void *ptr) const = 0;
    virtual size32_t _capacity() const = 0;
    virtual void _setDestructorFlag(const void *ptr) = 0;
    virtual void noteLinked(const void *ptr) = 0;

    inline static HeapletBase *findBase(const void *ptr)
    {
        HeapletBase *h = (HeapletBase *) ((memsize_t) ptr & HEAP_ALIGNMENT_MASK);
        if (h->mask != HEAP_ALIGNMENT_MASK)
        {
            h = (HeapletBase *) ((memsize_t) ptr & h->mask);
        }
        return h;
    }

public:

    static void release(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            if (h->flags & NOTE_RELEASES)
                h->noteReleased(ptr);
            if (atomic_dec_and_test(&h->count))
                h->released();
        }
    }

    static bool isShared(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            if (h->flags & NOTE_RELEASES)
                return h->_isShared(ptr);
            return atomic_read(&h->count)!= 2; // The heaplet itself has a usage count of 1
        }
        // isShared(NULL) or isShared on an object that shares a link-count is an error
        throwUnexpected();
    }

    static size32_t capacity(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            return h->_capacity();
        }
        throwUnexpected();
    }

    static void setDestructorFlag(const void *ptr)
    {
        if (ptr)
        {
            HeapletBase *h = findBase(ptr);
            if (h->flags & NOTE_RELEASES)
                h->_setDestructorFlag(ptr);
            else
                throwUnexpected();
        }
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
        if (h->flags & NOTE_RELEASES)
            h->noteLinked(ptr);
        atomic_inc(&h->count);
    }

    inline unsigned queryCount() const
    {
        return atomic_read(&count);
    }

    inline void setFlag(unsigned flag)
    {
        flags |= flag;
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
    DataBufferBase *next;   // Used when chaining them together in rowMgr
    IRowManager *mgr;
    DataBufferBase() : HeapletBase(DATA_ALIGNMENT_MASK)
    {
        next = NULL;
        mgr = NULL;
    }
public:
    void Link() 
    { 
        atomic_inc(&count); 
    }
    void Release();

};


class roxiemem_decl DataBuffer : public DataBufferBase
{
    friend class CDataBufferManager;
private:
    virtual void released();
    virtual void noteReleased(const void *ptr);
    virtual bool _isShared(const void *ptr) const;
    virtual size32_t _capacity() const;
    virtual void _setDestructorFlag(const void *ptr);
    virtual void noteLinked(const void *ptr);
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
    virtual size32_t _capacity() const;
    virtual void _setDestructorFlag(const void *ptr);
    virtual void noteLinked(const void *ptr);
public:
    DataBufferBottom(CDataBufferManager *_owner, DataBufferBottom *ownerFreeChain);

    void addToFreeChain(DataBufferBase * buffer);
};

#define ReleaseRoxieRow(row) roxiemem::HeapletBase::release(row)
#define ReleaseClearRoxieRow(row) roxiemem::HeapletBase::releaseClear(row)
#define LinkRoxieRow(row) roxiemem::HeapletBase::link(row)

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



// Variable size aggregated link-counted Roxie (etc) row manager
interface IRowManager : extends IInterface
{
    virtual void *allocate(size32_t size, unsigned activityId=0) = 0;
    virtual void *clone(size32_t size, const void *source, unsigned activityId=0) = 0;
    virtual void *resizeRow(void * original, size32_t oldsize, size32_t newsize, unsigned activityId, size32_t &capacity) = 0;
    virtual void *finalizeRow(void *final, unsigned originalSize, unsigned finalSize, unsigned activityId) = 0;
    virtual void setMemoryLimit(memsize_t size) = 0;
    virtual unsigned allocated() = 0;
    virtual unsigned pages() = 0;
    virtual unsigned getMemoryUsage() = 0;
    virtual bool attachDataBuff(DataBuffer *dataBuff) = 0 ;
    virtual void noteDataBuffReleased(DataBuffer *dataBuff) = 0 ;
    virtual void setActivityTracking(bool val) = 0;
    virtual void setCheckingHeap(int level) = 0;
    virtual void reportLeaks() = 0;
    virtual unsigned maxSimpleBlock() = 0;
    virtual void checkHeap() = 0;
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

extern roxiemem_decl IRowManager *createRowManager(unsigned memLimit, ITimeLimiter *tl, const IContextLogger &logctx, const IRowAllocatorCache *allocatorCache, bool ignoreLeaks = false);

// Fixed size aggregated link-counted zero-overhead data Buffer manager

interface IDataBufferManager : extends IInterface
{
    virtual DataBuffer *allocate() = 0;
    virtual void poolStats(StringBuffer &memStats) = 0;
};

extern roxiemem_decl IDataBufferManager *createDataBufferManager(size32_t size);
extern roxiemem_decl void setMemoryStatsInterval(unsigned secs);
extern roxiemem_decl void setTotalMemoryLimit(memsize_t max);
extern roxiemem_decl memsize_t getTotalMemoryLimit();
extern roxiemem_decl void releaseRoxieHeap();
extern roxiemem_decl bool memPoolExhausted();
extern roxiemem_decl unsigned memTraceLevel;
extern roxiemem_decl size32_t memTraceSizeLimit;

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
