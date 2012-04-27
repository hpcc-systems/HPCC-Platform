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
#include "roxiemem.hpp"

#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.hpp"
#include "thorcommon.ipp"

interface IRecordSize;
interface ILargeMemLimitNotify;
interface ISortKeySerializer;
interface ICompare;

//#define INCLUDE_POINTER_ARRAY_SIZE


#define ReleaseThorRow(row) ReleaseRoxieRow(row)
#define ReleaseClearThorRow(row) ReleaseClearRoxieRow(row)
#define LinkThorRow(row) LinkRoxieRow(row)



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
        if (_ptr)
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

interface IThorAllocator : extends IInterface
{
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const = 0;
    virtual roxiemem::IRowManager *queryRowManager() const = 0;
};

IThorAllocator *createThorAllocator(memsize_t memSize);

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

extern graph_decl IThorRowArrayException *createRowArrayException(size32_t sz);

extern graph_decl void checkMultiThorMemoryThreshold(bool inc);
extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify);

extern graph_decl memsize_t setLargeMemSize(unsigned limit);

/////////////

// JCSMORE
enum {
    InitialSortElements = 0,
    //The number of rows that can be added without entering a critical section, and therefore also the number
    //of rows that might not get freed when memory gets tight.
    CommitStep=32
};

graph_decl StringBuffer &getRecordString(const void *key, IOutputRowSerializer *serializer, const char *prefix, StringBuffer &out);

#define SPILL_PRIORITY_JOIN 10
#define SPILL_PRIORITY_SELFJOIN 10
#define SPILL_PRIORITY_HASHJOIN 10
#define SPILL_PRIORITY_LARGESORT 10
#define SPILL_PRIORITY_GROUPSORT 20
#define SPILL_PRIORITY_OVERFLOWABLE_BUFFER 50
#define SPILL_PRIORITY_SPILLABLE_STREAM 50

class CThorSpillableRowArray;
class graph_decl CThorExpandingRowArray : public CSimpleInterface
{
protected:
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;
    IOutputRowDeserializer *deserializer;

    roxiemem::IRowManager *rowManager;
    const void **rows;
    void **stableSortTmp;
    bool stableSort, throwOnOom, allowNulls;
    rowcount_t maxRows;  // Number of rows that can fit in the allocated memory.
    rowcount_t numRows;  // rows that have been added can only be updated by writing thread.

    void init(rowcount_t initialSize, bool stable);
    const void *allocateNewRows(rowcount_t requiredRows, OwnedConstThorRow &newStableSortTmp);
    void serialize(IRowSerializerTarget &out);
    void doSort(unsigned n, void **const rows, ICompare &compare, unsigned maxCores);

public:
    CThorExpandingRowArray(CActivityBase &activity, bool allowNulls=false, bool stableSort=false, bool throwOnOom=true, rowcount_t initialSize=InitialSortElements);
    CThorExpandingRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool allowNulls=false, bool stableSort=false, bool throwOnOom=true, rowcount_t initialSize=InitialSortElements);
    ~CThorExpandingRowArray();
	CActivityBase &queryActivity() { return activity; }
    // NB: throws error on OOM by default
    void setup(IRowInterfaces *rowIf, bool allowNulls=false, bool stableSort=false, bool throwOnOom=true);
    inline void setAllowNulls(bool b) { allowNulls = b; }

    void clearRows();
    void kill();

    void setRow(rowcount_t idx, const void *row) // NB: takes ownership
    {
        OwnedConstThorRow _row = row;
        assertex(idx < maxRows);
        const void *oldRow = rows[idx];
        if (oldRow)
            ReleaseThorRow(oldRow);
        rows[idx] = _row.getClear();
        if (idx+1>numRows)
            numRows = idx+1;
    }
    inline bool append(const void *row) // NB: takes ownership on success
    {
        assertex(row || allowNulls);
        if (numRows >= maxRows)
        {
            if (!ensure(numRows+1))
                return false;
        }
        rows[numRows++] = row;
        return true;
    }
    inline const void *query(rowcount_t i) const
    {
        if (i>=numRows)
            return NULL;
        return rows[i];
    }
    inline const void *get(rowcount_t i) const
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        if (row)
            LinkThorRow(row);
        return row;
    }
    inline const void *getClear(rowcount_t i)
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        rows[i] = NULL;
        return row;
    }
    inline rowcount_t ordinality() const { return numRows; }

    inline const void **getRowArray() { return rows; }
    void swap(CThorExpandingRowArray &src);
    void transfer(CThorExpandingRowArray &from)
    {
        kill();
        swap(from);
    }
    void transferRows(rowcount_t & outNumRows, const void * * & outRows);
	void transferFrom(CThorExpandingRowArray &src); 
	void transferFrom(CThorSpillableRowArray &src);
    void removeRows(rowcount_t start, rowcount_t n);
    void clearUnused();
    void sort(ICompare &compare, unsigned maxCores);
    void reorder(rowcount_t start, rowcount_t num, unsigned *neworder);

    bool equal(ICompare *icmp, CThorExpandingRowArray &other);
    bool checkSorted(ICompare *icmp);

    IRowStream *createRowStream(rowcount_t start=0, rowcount_t num=(rowcount_t)-1, bool streamOwns=true);

    void partition(ICompare &compare, unsigned num, UnsignedArray &out); // returns num+1 points

    offset_t serializedSize();
    void serialize(MemoryBuffer &mb);
    void serializeCompress(MemoryBuffer &mb);
    unsigned serializeBlock(MemoryBuffer &mb, size32_t dstmax, unsigned idx, unsigned count);
    void deserializeRow(IRowDeserializerSource &in); // NB single row not NULL
    void deserialize(size32_t sz, const void *buf);
    void deserializeExpand(size32_t sz, const void *data);

    virtual bool ensure(rowcount_t requiredRows);
};

interface IWritePosCallback : extends IInterface
{
    virtual rowcount_t queryRecordNumber() = 0;
    virtual void filePosition(offset_t pos) = 0;
};

class graph_decl CThorSpillableRowArray : private CThorExpandingRowArray
{
    const size32_t commitDelta;  // How many rows need to be written before they are added to the committed region?
    rowcount_t firstRow; // Only rows firstRow..numRows are considered initialized.  Only read/write within cs.
    rowcount_t commitRows;  // can only be updated by writing thread within a critical section
    mutable CriticalSection cs;
    ICopyArrayOf<IWritePosCallback> writeCallbacks;

protected:
    virtual bool ensure(rowcount_t requiredRows);

public:

    class CThorSpillableRowArrayLock
    {
        CThorSpillableRowArrayLock(CThorSpillableRowArrayLock &); // avoid accidental use
        const CThorSpillableRowArray & rows;
    public:
        inline CThorSpillableRowArrayLock(const CThorSpillableRowArray &_rows) : rows(_rows) { rows.lock(); }
        inline ~CThorSpillableRowArrayLock() { rows.unlock(); }
    };

    CThorSpillableRowArray(CThorSpillableRowArray &other); // NB: swaps
    CThorSpillableRowArray(CActivityBase &activity, bool allowNulls=false, bool stableSort=false, rowcount_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    CThorSpillableRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool allowNulls=false, bool stableSort=false, rowcount_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    ~CThorSpillableRowArray();
    // NB: throwOnOom false
    void setup(IRowInterfaces *rowIf, bool allowNulls=false, bool stableSort=false, bool throwOnOom=false)
    {
        CThorExpandingRowArray::setup(rowIf, allowNulls, stableSort, throwOnOom);
    }
    void registerWriteCallback(IWritePosCallback &cb);
    void unregisterWriteCallback(IWritePosCallback &cb);
    inline void setAllowNulls(bool b) { CThorExpandingRowArray::setAllowNulls(b); }
    void kill();
    void clearRows();
    void transferRows(rowcount_t & outNumRows, const void * * & outRows);
    void flush();
    inline bool append(const void *row)
    {
        assertex(row || allowNulls);
        if (numRows >= maxRows)
        {
            if (!ensure(numRows+1))
            {
                flush();
                if (numRows >= maxRows)
                    return false;
            }
        }
        rows[numRows++] = row;
        if (numRows >= commitRows + commitDelta)
            flush();
        return true;
    }

    //The following can be accessed from the reader without any need to lock
    inline const void *query(rowcount_t i) const
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::query(i);
    }
    inline const void *get(rowcount_t i) const
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::get(i);
    }
    inline const void *getClear(rowcount_t i)
    {
        CThorSpillableRowArrayLock block(*this);
        return CThorExpandingRowArray::getClear(i);
    }

    //A thread calling the following functions must own the lock, or guarantee no other thread will access
    void sort(ICompare & compare, unsigned maxcores);
    unsigned save(IFile &file, rowcount_t watchRecNum=(rowcount_t)-1, offset_t *watchFilePosResult=NULL);
    const void **getBlock(rowcount_t readRows);
    inline void noteSpilled(rowcount_t spilledRows)
    {
        firstRow += spilledRows;
    }

    //The block returned is only valid until the critical section is released

    inline rowcount_t firstCommitted() const { return firstRow; }
    inline rowcount_t numCommitted() const { return commitRows - firstRow; }

    //Locking functions - use CThorSpillableRowArrayLock above
    inline void lock() const { cs.enter(); }
    inline void unlock() const { cs.leave(); }

// access to
    void swap(CThorSpillableRowArray &src);
    void transfer(CThorSpillableRowArray &from)
    {
        kill();
        swap(from);
    }
	void transferFrom(CThorExpandingRowArray &src); 
    void transferFrom(CThorSpillableRowArray &src);

    IRowStream *createRowStream();

    offset_t serializedSize()
    {
        if (firstRow > 0)
            throwUnexpected();
        return CThorExpandingRowArray::serializedSize();
    }
    void serialize(MemoryBuffer &mb)
    {
        if (firstRow > 0)
            throwUnexpected();
        CThorExpandingRowArray::serialize(mb);
    }
    void deserialize(size32_t sz, const void *buf, bool hasNulls){ CThorExpandingRowArray::deserialize(sz, buf); }
    void deserializeRow(IRowDeserializerSource &in) { CThorExpandingRowArray::deserializeRow(in); }
};


enum RowCollectorFlags { rc_mixed, rc_allMem, rc_allDisk, rc_allDiskOrAllMem };
interface IThorRowCollectorCommon : extends IInterface
{
    virtual rowcount_t numRows() const = 0;
    virtual unsigned numOverflows() const = 0;
    virtual unsigned overflowScale() const = 0;
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort=true) = 0;
    virtual void transferRowsIn(CThorExpandingRowArray &src) = 0;
};

interface IThorRowLoader : extends IThorRowCollectorCommon
{
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50) = 0;
    virtual IRowStream *load(IRowStream *in, const bool &abort, bool preserveGrouping=false, CThorExpandingRowArray *allMemRows=NULL) = 0;
    virtual IRowStream *loadGroup(IRowStream *in, const bool &abort, CThorExpandingRowArray *allMemRows=NULL) = 0;
};

interface IThorRowCollector : extends IThorRowCollectorCommon
{
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50, bool preserveGrouping=false) = 0;
    virtual IRowWriter *getWriter() = 0;
    virtual void reset() = 0;
    virtual IRowStream *getStream(bool shared=false) = 0;
};

extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50);
extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50, bool preserveGrouping=false);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity, ICompare *iCompare=NULL, bool isStable=false, RowCollectorFlags diskMemMix=rc_mixed, unsigned spillPriority=50, bool preserveGrouping=false);




class CSDSServerStatus;


extern graph_decl ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *status=NULL);

extern graph_decl void setThorVMSwapDirectory(const char *swapdir);

class IPerfMonHook; 
extern graph_decl IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

extern graph_decl void setLCRrowCRCchecking(bool on=true);



#endif
