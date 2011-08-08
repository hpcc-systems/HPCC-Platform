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

#include "platform.h"

#include "jlib.hpp"
#include "jlog.hpp"
#include "jmalloc.hpp"
#include "jmutex.hpp"
#include "jcrc.hpp"
#include "thexception.hpp"

#include "thalloc.hpp"


#ifdef _DEBUG
#define ASSERTEX(c) assertex(c)
#else
#define ASSERTEX(c)
#endif

inline size32_t pow2roundupmin1k(size32_t sz)
{
    assertex(sz<0x80000000);
    size32_t ret = 1024;
    while (ret<sz)
        ret *= 2;
    return ret;
}

struct ThorRowHeader
{
    atomic_t count;
    size32_t memsize;               // includes child row memsize
    unsigned short flags;           // activity flags
    unsigned short extra;           // used for crc 
    inline unsigned short id() { return flags&MAX_ACTIVITY_ID; }
    inline bool needsDestruct() { return (flags&ACTIVITY_FLAG_NEEDSDESTRUCTOR)!=0; }

}; // __attribute__((__packed__));

interface ICRCException : extends IException
{
};


class CCRCException : public CSimpleInterface, implements ICRCException
{
private:
    const void *ptr;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCRCException(const void * _ptr) { ptr = _ptr; }

// IThorException
    virtual int errorCode() const { return TE_RowCrc; }

    virtual StringBuffer & errorMessage(StringBuffer & str) const
    { 
        return str.appendf("Row CRC error at address %p",ptr);
    }
    MessageAudience errorAudience() const { return MSGAUD_operator; }   // indicates memory corruption

};

class CThorRowManager: public CSimpleInterface, implements IThorRowManager
{
    Owned<IAllocator> allocator;
    const IThorRowAllocatorCache &allocatorcache;
    bool ignoreleaks;
    bool crcenabled;
public:
    static CThorRowManager *self;  // this is a singleton

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowManager(const IThorRowAllocatorCache &_allocatorcache)
        : allocatorcache(_allocatorcache)
    {
        ignoreleaks = true;
        crcenabled = false;
    }

    void init(memsize_t memLimit,bool _ignoreleaks)
    {
        assertex(!self);
        self = this;
        ignoreleaks = _ignoreleaks;
        allocator.setown(createMemoryAllocator(memLimit,1024));
        if (!allocator.get())
            throw MakeStringException(-1,"CThorRowManager could not create allocator");
        PROGLOG("CThorRowManager initialized, memlimit = %"I64F"d",(offset_t)memLimit);
        assertex(sizeof(ThorRowHeader)==12);
        crcenabled = false;
    }

    ~CThorRowManager()
    {
        if (!ignoreleaks&&allocator.get())
            reportLeaks();
        allocator.clear();
        self = NULL;
    }

    virtual void *allocate(size32_t size, unsigned activityId)
    {
        ASSERTEX(activityId<MAX_ACTIVITY_ID);
        size32_t us;
        ThorRowHeader *h = (ThorRowHeader *)allocator->allocMem2(sizeof(ThorRowHeader)+size,us);
        h->flags = (unsigned short)activityId;
        h->extra = 0;
        h->memsize = us;  
        atomic_set(&h->count, 1);
        return h+1;
    }
    virtual void *clone(size32_t size, const void *source, unsigned activityId)
    {
        if (!source)
            return NULL;
        ASSERTEX(activityId<MAX_ACTIVITY_ID);
        void *ret = allocate(size, activityId?activityId:(((ThorRowHeader *)source-1)->id()));
        memcpy(ret, source, size); // could copy crc also? TBD
        return ret;
    }
    virtual void *resizeRow(void * original, size32_t oldsize, size32_t newsize, unsigned activityId)
    {
        ASSERTEX(activityId<MAX_ACTIVITY_ID);
        ASSERTEX(newsize);
        ASSERTEX(original);
        ASSERTEX(!isRowShared(original));
        if (oldsize==newsize)
            return original;
        size32_t us;
        ThorRowHeader *h = (ThorRowHeader *)allocator->reallocMem2(((ThorRowHeader *)original)-1,sizeof(ThorRowHeader)+newsize,us);
        h->extra = 0;  // we might recalc crc? TBD
        h->memsize = us;
        return h+1;
    }
    virtual void *allocateExt(size32_t size, unsigned activityId, size32_t &outsize)
    {
        ASSERTEX(activityId<MAX_ACTIVITY_ID);
        outsize = pow2roundupmin1k(size);
        size32_t us;
        ThorRowHeader *h = (ThorRowHeader *)allocator->allocMem2(sizeof(ThorRowHeader)+outsize,us);
        h->flags = (unsigned short)activityId;
        h->extra = 0;
        h->memsize = us;  
        atomic_set(&h->count, 1);
        return h+1;
    }
    virtual void *extendRow(void * original, size32_t newsize, unsigned activityId, size32_t &size) // NB in 'size' == max
    {
        ASSERTEX(activityId<MAX_ACTIVITY_ID);
        ASSERTEX(newsize);
        ASSERTEX(original);
        ASSERTEX(!isRowShared(original));
        size32_t oldsize = size;
        size = pow2roundupmin1k(newsize);
        if (size==pow2roundupmin1k(oldsize))
            return original;
        size32_t us;
        ThorRowHeader *h = (ThorRowHeader *)allocator->reallocMem2(((ThorRowHeader *)original)-1,sizeof(ThorRowHeader)+size,us);
        h->extra = 0;  // we might recalc crc? TBD
        h->memsize = us;
        return h+1;
    }
    virtual void *finalizeRow(void * original, size32_t newsize, unsigned activityId, bool dup)
    {
        ASSERTEX(newsize);
        ASSERTEX(!isRowShared(original));
        ASSERTEX(original);
        ThorRowHeader *h = ((ThorRowHeader *)original)-1;
        size32_t us = allocator->usableSize(h);
        newsize += sizeof(ThorRowHeader);
        ASSERTEX(us>=newsize); // finalize cannot make bigger!
        if (allocator->roundupSize(newsize)<us) {
            if (dup) {
                const void *old = h;
                h = (ThorRowHeader *)allocator->allocMem2(newsize,us);
                memcpy(h,old,newsize); // newsize is correct here
            }
            else 
                h = (ThorRowHeader *)allocator->reallocMem2(h,newsize,us);
            h->memsize = us;
        }
        if (activityId&ACTIVITY_FLAG_NEEDSDESTRUCTOR) 
            h->flags |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
        if (h->needsDestruct())
            h->memsize += allocatorcache.subSize(h->id(), h+1);
        if (crcenabled) 
            h->extra = (0x8000|chksum16(h+1,us-sizeof(ThorRowHeader))); // don't CRC header NB usableSize not newsize
        // now add in child memory sizes 

        return h+1;
    }
    virtual memsize_t allocated()
    {
        return allocator->totalAllocated();
    }

    virtual memsize_t remaining()
    {   
        memsize_t a = allocator->totalAllocated();
        memsize_t t = allocator->totalMax();
        if (a<t)
            return t-a;
        return 0;
    }

    virtual void reportLeaks()
    {
        if (allocator)
#ifdef _DEBUG
            allocator->logMemLeaks(true);
#else
            allocator->logMemLeaks(false); // just summary
#endif
    }

    inline void crcError(const void *p)
    {
        PrintStackReport();
        ERRLOG("ROW CRC check failed at address %p",p);     // make sure doesn't get lost!
        logThorRow("row",p);
        throw new CCRCException(p);
    }

    inline void releaseRow(const void *ptr)
    {
        if (!ptr)
            return;
        ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
        if (atomic_dec_and_test(&h->count))  {
            if (crcenabled&&(h->extra&0x8000)) {
                unsigned short crc = (0x8000|chksum16(ptr,allocator->usableSize(h)-sizeof(ThorRowHeader))); // don't CRC header
                if (crc!=h->extra) {
                    crcError(ptr);
                }
            }
            if (h->needsDestruct())
                allocatorcache.onDestroy(h->id(), (void *)ptr);
            allocator->freeMem(h);
        }
    }

    static inline void linkRow(const void *ptr)
    {
        if (!ptr)
            return;
        ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
        if (atomic_inc_and_test(&h->count))  
            atomic_dec(&h->count);      // won't happen in practice
    }

    static inline bool isRowShared(const void *ptr)
    {
        ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
        return atomic_read(&h->count) > 1;
    }

    void setLCRrowCRCchecking(bool on=true)
    {
        crcenabled = on;
    }

    inline void setRowCRC(const void *ptr)
    {
        if (crcenabled&&ptr) {
            ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
            h->extra = (0x8000|chksum16(ptr,allocator->usableSize(h)-sizeof(ThorRowHeader)));   // don't CRC header
        }
    }

    
    static inline void clearRowCRC(const void *ptr)
    {
        if (ptr) {
            ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
            h->extra = 0;
        }
    }

    
    inline void checkRowCRC(const void *ptr)
    {
        if (crcenabled&&ptr) {
            ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
            if (h->extra&0x8000) {
                unsigned short crc = (0x8000|chksum16(ptr,allocator->usableSize(h)-sizeof(ThorRowHeader))); // don't CRC header
                if (crc!=h->extra) {
                    crcError(ptr);
                }
            }
        }
    }

    static inline size32_t rowMemoryFootprint(const void *ptr)
    {
        if (ptr) {
            ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
            return h->memsize;
        }
        else
            return 0;
    }

    inline size32_t usableSize(const void *ptr)
    {
        if (ptr) {
            ThorRowHeader *h = ((ThorRowHeader *)ptr)-1;
            size32_t us=allocator->usableSize(h);
            ASSERTEX(us>=sizeof(ThorRowHeader));
            return us-sizeof(ThorRowHeader);
        }
        return 0;
    }



    void logRow(const char *prefix,const void *row)
    {
        if (row) {
            ThorRowHeader *h = ((ThorRowHeader *)row)-1;
            unsigned us = allocator->usableSize(h);
            assertex(us>=sizeof(ThorRowHeader));
            us -= sizeof(ThorRowHeader);
            StringBuffer s;
            for (unsigned i=0;(i<64)&&(i<us);i++) 
                s.appendhex(*((byte *)row+i),true);
            PROGLOG("%s: %p, {cnt=%d,crc=%x,flg=%x} %u: %s",prefix?prefix:"row", row, (int)atomic_read(&h->count), (unsigned)h->extra, (unsigned)h->flags, us, s.str());
        }
        else 
            PROGLOG("%s: NULL row",prefix?prefix:"row");
    }

};

CThorRowManager *CThorRowManager::self = NULL;


IThorRowManager *createThorRowManager(memsize_t memLimit, const IThorRowAllocatorCache *allocatorCache, bool ignoreLeaks)
{
    assertex(allocatorCache);
    Owned<CThorRowManager> rm = new CThorRowManager(*allocatorCache);
    rm->init(memLimit,ignoreLeaks);
    return rm.getClear();
}

void ReleaseThorRow(const void *ptr)
{
    ASSERTEX(CThorRowManager::self);
    CThorRowManager::self->releaseRow(ptr);
}

void ReleaseClearThorRow(const void *&ptr)
{
    ASSERTEX(CThorRowManager::self);
    const void *p = ptr;
    ptr = NULL;
    CThorRowManager::self->releaseRow(p);
}
void LinkThorRow(const void *ptr)
{
    CThorRowManager::linkRow(ptr);
}

bool isThorRowShared(const void *ptr)
{
    return CThorRowManager::isRowShared(ptr);
}

void setThorRowCRC(const void *ptr)
{
    ASSERTEX(CThorRowManager::self);
    return CThorRowManager::self->setRowCRC(ptr);
}

void clearThorRowCRC(const void *ptr)
{
    return CThorRowManager::clearRowCRC(ptr);
}

void checkThorRowCRC(const void *ptr)
{
    return CThorRowManager::self->checkRowCRC(ptr);
}

void logThorRow(const char *prefix,const void *row)
{
    ASSERTEX(CThorRowManager::self);
    CThorRowManager::self->logRow(prefix,row);
}

size32_t thorRowMemoryFootprint(const void *ptr)
{
    if (!ptr)
        return 0;
    size32_t ret = CThorRowManager::rowMemoryFootprint(ptr);
    ASSERTEX(ret!=0);   // size should be set 
    if (ret)
        return ret;
    ASSERTEX(CThorRowManager::self);  
    return CThorRowManager::self->usableSize(ptr)+sizeof(ThorRowHeader);
}
