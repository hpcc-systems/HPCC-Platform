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

#ifndef __THMEM__
#define __THMEM__

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif

#include "jexcept.hpp"
#include "jbuff.hpp"
#include "jsort.hpp"
#include "thormisc.hpp"
#include "eclhelper.hpp"
#include "rtlread_imp.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.hpp"
#include "thorcommon.ipp"

interface IRecordSize;
interface ILargeMemLimitNotify;
interface ISortKeySerializer;
interface ICompare;

#ifdef _DEBUG
#define TEST_ROW_LINKS
//#define PARANOID_TEST_ROW_LINKS
#endif

//#define INCLUDE_POINTER_ARRAY_SIZE        




graph_decl void setThorInABox(unsigned num);


// used for non-row allocations
#define ThorMalloc(a) malloc(a)
#define ThorRealloc(p,a) realloc(p,a)
#define ThorFree(p) free(p)


// ---------------------------------------------------------
// Thor link counted rows

// these may be inline later

#ifdef TEST_ROW_LINKS
#define TESTROW(r) if (r) { LinkThorRow(r); ReleaseThorRow(r); }
#else
#define TESTROW(r) 
#endif
#ifdef PARANOID_TEST_ROW_LINKS
#define PARANOIDTESTROW(r) if (r) { LinkThorRow(r); ReleaseThorRow(r); }
#else
#define PARANOIDTESTROW(r) 
#endif



extern graph_decl void ReleaseThorRow(const void *ptr);
extern graph_decl void ReleaseClearThorRow(const void *&ptr);
extern graph_decl void LinkThorRow(const void *ptr);
extern graph_decl bool isThorRowShared(const void *ptr);

class OwnedConstThorRow 
{
public:
    inline OwnedConstThorRow()                              { ptr = NULL; }
    inline OwnedConstThorRow(const void * _ptr)             { TESTROW(_ptr); ptr = _ptr; }
    inline OwnedConstThorRow(const OwnedConstThorRow & other)   { ptr = other.getLink(); }

    inline ~OwnedConstThorRow()                             { ReleaseThorRow(ptr); }
    
private: 
    /* these overloaded operators are the devil of memory leak. Use set, setown instead. */
    void operator = (const void * _ptr)          { set(_ptr);  }
    void operator = (const OwnedConstThorRow & other) { set(other.get());  }

    /* this causes -ve memory leak */
    void setown(const OwnedConstThorRow &other) {  }

public:
    inline const void * operator -> () const        { PARANOIDTESTROW(ptr); return ptr; } 
    inline operator const void *() const            { PARANOIDTESTROW(ptr); return ptr; } 
    
    inline void clear()                         { const void *temp=ptr; ptr=NULL; ReleaseThorRow(temp); }
    inline const void * get() const             { PARANOIDTESTROW(ptr); return ptr; }
    inline const void * getClear()              
    { 
        const void * ret = ptr; 
        ptr = NULL; 
        TESTROW(ret);
        return ret; 
    }
    inline const void * getLink() const         { LinkThorRow(ptr); return ptr; }
    inline void set(const void * _ptr)          
    { 
        const void * temp = ptr; 
        LinkThorRow(_ptr); 
        ptr = _ptr; 
        if (temp)
            ReleaseThorRow(temp); 
    }
    inline void setown(const void * _ptr)       
    { 
        TESTROW(_ptr);
        const void * temp = ptr; 
        ptr = _ptr; 
        if (temp)
            ReleaseThorRow(temp); 
    }
    
    inline void set(const OwnedConstThorRow &other) { set(other.get()); }

    inline void deserialize(IRowInterfaces *rowif, size32_t memsz, const void *mem)
    {
        if (memsz) {
            RtlDynamicRowBuilder rowBuilder(rowif->queryRowAllocator());
            //GH->NH This now has a higher overhead than you are likely to want at this point...
            CThorStreamDeserializerSource dsz(memsz,mem);
            size32_t size = rowif->queryRowDeserializer()->deserialize(rowBuilder,dsz);
            setown(rowBuilder.finalizeRowClear(size));
        }
        else
            clear();
    }
    
private:
    const void * ptr;
};





interface IThorRowAllocator: extends IEngineRowAllocator
{
};



extern graph_decl void initThorMemoryManager(size32_t sz, unsigned memtracelevel, unsigned memstatinterval);

extern graph_decl void resetThorMemoryManager();
extern graph_decl IThorRowAllocator *createThorRowAllocator(IOutputMetaData * _meta, unsigned _activityId);
extern graph_decl IOutputMetaData *createOutputMetaDataWithExtra(IOutputMetaData *meta, size32_t sz);
extern graph_decl IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz);

 
class CThorRowLinkCounter: public CSimpleInterface, implements IRowLinkCounter
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    virtual void releaseRow(const void *row)
    {
        ReleaseThorRow(row);
    }
    virtual void linkRow(const void *row)
    {
        LinkThorRow(row);
    }
};


extern graph_decl memsize_t ThorRowMemoryAvailable();



// ---------------------------------------------------------


interface CLargeThorLinkedRowArrayOutOfMemException: extends IException
{
};

interface IThorRowSequenceCompare: implements ICompare, extends IInterface
{
};

#define PERROWOVERHEAD    (sizeof(atomic_t) + sizeof(unsigned) + sizeof(void *))
                          //    link + activityid + stable sort ptr

interface IThorRowArrayException: extends IException
{
};

extern graph_decl void checkMultiThorMemoryThreshold(bool inc);
extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify);

class graph_decl CThorRowArray
{
    MemoryBuffer ptrbuf;
    unsigned numelem;
    memsize_t totalsize;
    memsize_t maxtotal;
    size32_t overhead;
    Linked<IOutputRowSerializer> serializer;
    bool keepsize;
    bool sizing;
    bool raiseexceptions;

    void adjSize(const void *row, bool inc);


public:
    CThorRowArray()
    {
        numelem = 0;
        totalsize = 0;
        overhead = 0;
        sizing = false;
        raiseexceptions = false;
        memsize_t tmp = ((unsigned __int64)ThorRowMemoryAvailable())*7/8;   // don't fill up completely
        if (tmp>0xffffffff)
            maxtotal = 0xffffffff;
        else
            maxtotal = (unsigned)tmp;
        if (maxtotal<0x100000)
            maxtotal = 0x100000;
    }


    ~CThorRowArray()
    {
        reset(true);
    }

    void reset(bool freeptrs)
    {
        const void ** row = (const void **)base();
        unsigned remn = 0;
        while (numelem) {
            const void * r = *(row++);
            if (r) {
                remn++;
                ReleaseThorRow(r);
            }
            numelem--;
        }
        if (freeptrs)
            ptrbuf.resetBuffer();
        else
            ptrbuf.setLength(0);
        if (sizing&&remn) 
            checkMultiThorMemoryThreshold(false);
        totalsize = 0;
        overhead = 0;
    }

    inline void clear() { reset(true); }

    void append(const void *row) // takes ownership
    {
        if (sizing) 
            adjSize(row,true);
        ptrbuf.append(sizeof(row),&row);
        numelem++;
    }

    void removeRows(unsigned i,unsigned n);

    inline const byte * item(unsigned idx) const
    {
        if (idx>=numelem)
            return NULL;
        return *(((const byte **)ptrbuf.toByteArray())+idx);

    }
    inline const byte ** base() const
    {
        return (const byte **)ptrbuf.toByteArray();
    }

    inline const byte * itemClear(unsigned idx) // sets old to NULL 
    {
        if (idx>=numelem)
            return NULL;
        byte ** rp = ((byte **)ptrbuf.toByteArray())+idx;
        const byte *ret = *rp;
        if (sizing) 
            adjSize(ret,false);
        *rp = NULL;
        return ret;

    }

    inline unsigned ordinality() const
    {
        return numelem;
    }

    inline memsize_t totalSize() const
    {
#ifdef _DEBUG
        assertex(sizing); 
#endif
        return totalsize;
    }

    void setMaxTotal(memsize_t tot)
    {   
        maxtotal = tot;
    }

    inline memsize_t totalMem()
    {
        return 
#ifdef INCLUDE_POINTER_ARRAY_SIZE           
        ptrbuf.length()+ptrbuf.capacity()+
#endif
        totalsize+overhead;
    }

    inline bool isFull()
    {
        memsize_t sz = totalMem();
#ifdef _DEBUG
        assertex(sizing&&!raiseexceptions);
        if (sz>maxtotal) {
            PROGLOG("CThorRowArray isFull(totalsize=%"I64F"u,ptrbuf.length()=%u,ptrbuf.capacity()=%u,overhead=%u,maxtotal=%"I64F"u",
                     (unsigned __int64) totalsize,ptrbuf.length(),ptrbuf.capacity(),overhead,(unsigned __int64) maxtotal);
            return true;
        }
        return false;
#endif
        return sz>maxtotal;
    }

    void sort(ICompare & compare, bool stable, unsigned maxcores)
    {
        unsigned n = ordinality();
        if (n>1) {
            const byte ** res = base();
            if (stable) {
                MemoryAttr tmp;
                void ** ptrs = (void **)tmp.allocate(n*sizeof(void *));
                memcpy(ptrs,res,n*sizeof(void **));
                parqsortvecstable(ptrs, n, compare, (void ***)res, maxcores); // use res for index
                while (n--) {
                    *res = **((byte ***)res);
                    res++;
                }
            }
            else 
                parqsortvec((void **)res, n, compare, maxcores);
        }
    }

    void partition(ICompare & compare,unsigned num,UnsignedArray &out) // returns num+1 points
    {
        unsigned p=0;
        unsigned n = ordinality();
        const byte **ptrs = (const byte **)ptrbuf.toByteArray();
        while (num) {
            out.append(p);
            if (p<n) {
                unsigned q = p+(n-p)/num;
                if (p==q) { // skip to next group
                    while (q<n) {
                        q++;
                        if ((q<n)&&(compare.docompare(ptrs[p],ptrs[q])!=0)) // ensure at next group
                            break;
                    }
                }
                else {
                    while ((q<n)&&(q!=p)&&(compare.docompare(ptrs[q-1],ptrs[q])==0)) // ensure at start of group
                        q--;
                }
                p = q;
            }
            num--;
        }
        out.append(n);
    }

    void setSizing(bool _sizing,bool _raiseexceptions) // ,IOutputRowSerializer *_serializer)
    {
        sizing = _sizing;
        raiseexceptions = _raiseexceptions;
    }

    unsigned load(IRowStream &stream,bool ungroup); // doesn't check for overflow
    unsigned load(IRowStream &stream, bool ungroup, bool &abort, bool *overflowed=NULL);
    unsigned load2(IRowStream &stream, bool ungroup, CThorRowArray &prev, IFile &savefile, IOutputRowSerializer *prevserializer, IEngineRowAllocator *preallocator, bool &prevsaved, bool &overflowed);
    
    IRowStream *createRowStream(unsigned start=0,unsigned num=(unsigned)-1, bool streamowns=true);
    unsigned save(IRowWriter *writer,unsigned start=0,unsigned num=(unsigned)-1, bool streamowns=true);
    void setNull(unsigned idx);
    void transfer(CThorRowArray &from);
    void swapWith(CThorRowArray &from);

    void serialize(IOutputRowSerializer *_serializer,IRowSerializerTarget &out);
    void serialize(IOutputRowSerializer *_serializer,MemoryBuffer &mb,bool hasnulls);
    unsigned serializeblk(IOutputRowSerializer *_serializer,MemoryBuffer &mb,size32_t dstmax, unsigned idx, unsigned count);
    void deserialize(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,size32_t sz,const void *buf,bool hasnulls);
    void deserializerow(IEngineRowAllocator &allocator,IOutputRowDeserializer *deserializer,IRowDeserializerSource &in); // NB single row not NULL

    void reorder(unsigned start,unsigned num, unsigned *neworder);

    void setRaiseExceptions(bool on=true) { raiseexceptions=on; }

    void reserve(unsigned n);
    void setRow(unsigned idx,const void *row) // takes ownership of row
    {
        assertex(idx<numelem);
        const byte ** rp = ((const byte **)ptrbuf.toByteArray())+idx;
        OwnedConstThorRow old = *rp;
        if (old&&sizing) 
            adjSize(old,false);
        *rp = (const byte *)row;
        if (sizing) 
            adjSize(row,true);
    }
    void ensure(unsigned size)
    {
        if (size<=numelem) return;
        reserve(size-numelem);
    }


};





class CSDSServerStatus;


extern graph_decl ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *status=NULL);

extern graph_decl void setThorVMSwapDirectory(const char *swapdir);

class IPerfMonHook; 
extern graph_decl IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

extern graph_decl void setLCRrowCRCchecking(bool on=true);



#endif
