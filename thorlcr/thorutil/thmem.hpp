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

using roxiemem::OwnedRoxieString;

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
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) const = 0;
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const = 0;
    virtual roxiemem::IRowManager *queryRowManager() const = 0;
    virtual roxiemem::RoxieHeapFlags queryFlags() const = 0;
    virtual bool queryCrc() const = 0;
};

IThorAllocator *createThorAllocator(memsize_t memSize, unsigned memorySpillAt, IContextLogger &logctx, bool crcChecking, bool usePacked);

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

//NB: low priorities are spilt 1st
#define SPILL_PRIORITY_VERYLOW 50
#define SPILL_PRIORITY_LOW  100
#define SPILL_PRIORITY_HIGH 1000000
#define SPILL_PRIORITY_DEFAULT SPILL_PRIORITY_LOW
#define SPILL_PRIORITY_DISABLE UINT_MAX

#define SPILL_PRIORITY_OVERFLOWABLE_BUFFER SPILL_PRIORITY_LOW
#define SPILL_PRIORITY_SPILLABLE_STREAM SPILL_PRIORITY_LOW
#define SPILL_PRIORITY_RESULT SPILL_PRIORITY_LOW

#define SPILL_PRIORITY_GROUPSORT SPILL_PRIORITY_LOW+1000

#define SPILL_PRIORITY_HASHDEDUP_REHASH SPILL_PRIORITY_LOW+1900
#define SPILL_PRIORITY_HASHDEDUP SPILL_PRIORITY_LOW+2000
#define SPILL_PRIORITY_HASHDEDUP_BUCKET_POSTSPILL SPILL_PRIORITY_VERYLOW // very low, by this stage it's cheap to dispose of

#define SPILL_PRIORITY_JOIN SPILL_PRIORITY_HIGH
#define SPILL_PRIORITY_SELFJOIN SPILL_PRIORITY_HIGH
#define SPILL_PRIORITY_HASHJOIN SPILL_PRIORITY_HIGH
#define SPILL_PRIORITY_LARGESORT SPILL_PRIORITY_HIGH
#define SPILL_PRIORITY_LOOKUPJOIN SPILL_PRIORITY_HIGH


enum StableSortFlag { stableSort_none, stableSort_earlyAlloc, stableSort_lateAlloc };
class CThorSpillableRowArray;

// general really
interface IThorArrayLock
{
    virtual void lock() const = 0;
    virtual void unlock() const = 0;
};

class CThorArrayLockBlock
{
    CThorArrayLockBlock(CThorArrayLockBlock &); // avoid accidental use
    const IThorArrayLock &alock;
public:
    inline CThorArrayLockBlock(const IThorArrayLock &_alock) : alock(_alock) { alock.lock(); }
    inline ~CThorArrayLockBlock() { alock.unlock(); }
};
class CThorArrayLockUnblock
{
    CThorArrayLockUnblock(CThorArrayLockUnblock &); // avoid accidental use
    const IThorArrayLock &alock;
public:
    inline CThorArrayLockUnblock(const IThorArrayLock &_alock) : alock(_alock) { alock.unlock(); }
    inline ~CThorArrayLockUnblock() { alock.lock(); }
};

class graph_decl CThorExpandingRowArray : public CSimpleInterface
{
    class CDummyLock : implements IThorArrayLock
    {
    public:
        // IThorArrayLock
        virtual void lock() const { }
        virtual void unlock() const {  }
    } dummyLock;

    bool resizeRowTable(void **&_rows, rowidx_t requiredRows, bool copy, unsigned maxSpillCost, memsize_t &newCapacity, const char *errMsg);
    bool _resize(rowidx_t requiredRows, unsigned maxSpillCost);
    const void *_allocateRowTable(rowidx_t num, unsigned maxSpillCost);

// for direct access by another CThorExpandingRowArray only
    inline void transferRowsCopy(const void **outRows, bool takeOwnership);

protected:
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;
    IOutputRowDeserializer *deserializer;
    roxiemem::IRowManager *rowManager;
    const void **rows;
    void **stableTable;
    bool throwOnOom; // tested during array expansion (resize())
    bool allowNulls;
    StableSortFlag stableSort;
    rowidx_t maxRows;  // Number of rows that can fit in the allocated memory.
    rowidx_t numRows;  // High water mark of rows added
    unsigned defaultMaxSpillCost;

    const void *allocateRowTable(rowidx_t num);
    const void *allocateRowTable(rowidx_t num, unsigned maxSpillCost);
    rowidx_t getNewSize(rowidx_t requiredRows);
    void serialize(IRowSerializerTarget &out);
    void doSort(rowidx_t n, void **const rows, ICompare &compare, unsigned maxCores);
    inline rowidx_t getRowsCapacity() const { return rows ? RoxieRowCapacity(rows) / sizeof(void *) : 0; }
public:
    CThorExpandingRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool allowNulls=false, StableSortFlag stableSort=stableSort_none, bool throwOnOom=true, rowidx_t initialSize=InitialSortElements);
    ~CThorExpandingRowArray();
    CActivityBase &queryActivity() { return activity; }
    // NB: throws error on OOM by default
    void setup(IRowInterfaces *rowIf, bool allowNulls=false, StableSortFlag stableSort=stableSort_none, bool throwOnOom=true);
    inline void setAllowNulls(bool b) { allowNulls = b; }
    inline void setDefaultMaxSpillCost(unsigned _defaultMaxSpillCost) { defaultMaxSpillCost = _defaultMaxSpillCost; }
    inline unsigned queryDefaultMaxSpillCost() const { return defaultMaxSpillCost; }
    void clearRows();
    void kill();

    void setRow(rowidx_t idx, const void *row) // NB: takes ownership
    {
        OwnedConstThorRow _row = row;
        dbgassertex(idx < maxRows);
        const void *oldRow = rows[idx];
        if (oldRow)
            ReleaseThorRow(oldRow);
        rows[idx] = _row.getClear();
        if (idx+1>numRows) // keeping high water mark
            numRows = idx+1;
    }
    inline bool append(const void *row) // NB: takes ownership on success
    {
        assertex(row || allowNulls);
        if (numRows >= maxRows)
        {
            if (!resize(numRows+1))
                return false;
        }
        rows[numRows++] = row;
        return true;
    }
    bool binaryInsert(const void *row, ICompare &compare, bool dropLast=false); // NB: takes ownership on success
    inline const void *query(rowidx_t i) const
    {
        if (i>=numRows)
            return NULL;
        return rows[i];
    }
    inline const void *get(rowidx_t i) const
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        if (row)
            LinkThorRow(row);
        return row;
    }
    inline const void *getClear(rowidx_t i)
    {
        if (i>=numRows)
            return NULL;
        const void *row = rows[i];
        rows[i] = NULL;
        return row;
    }
    inline rowidx_t ordinality() const { return numRows; }
    inline rowidx_t queryMaxRows() const { return maxRows; }

    inline const void **getRowArray() { return rows; }
    void swap(CThorExpandingRowArray &src);
    void transfer(CThorExpandingRowArray &from)
    {
        kill();
        swap(from);
    }
    void transferRows(rowidx_t & outNumRows, const void * * & outRows);
    void transferFrom(CThorExpandingRowArray &src);
    void transferFrom(CThorSpillableRowArray &src);
    void removeRows(rowidx_t start, rowidx_t n);
    bool appendRows(CThorExpandingRowArray &inRows, bool takeOwnership);
    bool appendRows(CThorSpillableRowArray &inRows, bool takeOwnership);
    void clearUnused();
    void sort(ICompare &compare, unsigned maxCores);
    void reorder(rowidx_t start, rowidx_t num, rowidx_t *neworder);

    bool equal(ICompare *icmp, CThorExpandingRowArray &other);
    bool checkSorted(ICompare *icmp);

    IRowStream *createRowStream(rowidx_t start=0, rowidx_t num=(rowidx_t)-1, bool streamOwns=true);

    void partition(ICompare &compare, unsigned num, UnsignedArray &out); // returns num+1 points

    offset_t serializedSize();
    memsize_t getMemUsage();
    void serialize(MemoryBuffer &mb);
    void serializeCompress(MemoryBuffer &mb);
    rowidx_t serializeBlock(MemoryBuffer &mb, rowidx_t idx, rowidx_t count, size32_t dstmax, bool hardMax);
    void deserializeRow(IRowDeserializerSource &in); // NB single row not NULL
    void deserialize(size32_t sz, const void *buf);
    void deserializeExpand(size32_t sz, const void *data);
    bool resize(rowidx_t requiredRows);
    bool resize(rowidx_t requiredRows, unsigned maxSpillCost);
    void compact();
    virtual IThorArrayLock &queryLock() { return dummyLock; }

friend class CThorSpillableRowArray;
};

interface IWritePosCallback : extends IInterface
{
    virtual rowidx_t queryRecordNumber() = 0;
    virtual void filePosition(offset_t pos) = 0;
};

class graph_decl CThorSpillableRowArray : private CThorExpandingRowArray, implements IThorArrayLock
{
    const size32_t commitDelta;  // How many rows need to be written before they are added to the committed region?
    rowidx_t firstRow; // Only rows firstRow..numRows are considered initialized.  Only read/write within cs.
    rowidx_t commitRows;  // can only be updated by writing thread within a critical section
    mutable CriticalSection cs;
    ICopyArrayOf<IWritePosCallback> writeCallbacks;
public:

    CThorSpillableRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool allowNulls=false, StableSortFlag stableSort=stableSort_none, rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep);
    ~CThorSpillableRowArray();
    // NB: default throwOnOom to false
    void setup(IRowInterfaces *rowIf, bool allowNulls=false, StableSortFlag stableSort=stableSort_none, bool throwOnOom=false)
    {
        CThorExpandingRowArray::setup(rowIf, allowNulls, stableSort, throwOnOom);
    }
    void registerWriteCallback(IWritePosCallback &cb);
    void unregisterWriteCallback(IWritePosCallback &cb);
    inline void setAllowNulls(bool b) { CThorExpandingRowArray::setAllowNulls(b); }
    inline void setDefaultMaxSpillCost(unsigned defaultMaxSpillCost) { CThorExpandingRowArray::setDefaultMaxSpillCost(defaultMaxSpillCost); }
    inline unsigned queryDefaultMaxSpillCost() const { return CThorExpandingRowArray::queryDefaultMaxSpillCost(); }
    void kill();
    void compact();
    void flush();
    inline bool isFlushed() const { return numRows == numCommitted(); }
    inline bool append(const void *row) __attribute__((warn_unused_result))
    {
        //GH->JCS Should this really be inline?
        assertex(row || allowNulls);
        if (numRows >= maxRows)
        {
            if (!resize(numRows+1))
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
    bool appendRows(CThorExpandingRowArray &inRows, bool takeOwnership);

    //The following must either be accessed within a lock, or when no rows can be appended,
    //(otherwise flush() might move all the rows, invalidating the indexes - or for query() the row)
    inline const void *query(rowidx_t i) const
    {
        return CThorExpandingRowArray::query(i);
    }
    inline const void *get(rowidx_t i) const
    {
        return CThorExpandingRowArray::get(i);
    }
    inline const void *getClear(rowidx_t i)
    {
        return CThorExpandingRowArray::getClear(i);
    }

    //A thread calling the following functions must own the lock, or guarantee no other thread will access
    void sort(ICompare & compare, unsigned maxcores);
    rowidx_t save(IFile &file, bool useCompression, const char *tracingPrefix);

    inline rowidx_t numCommitted() const { return commitRows - firstRow; } //MORE::Not convinced this is very safe!

// access to
    void swap(CThorSpillableRowArray &src);
    void transfer(CThorSpillableRowArray &from)
    {
        kill();
        swap(from);
    }
    void transferFrom(CThorExpandingRowArray &src);
    void transferFrom(CThorSpillableRowArray &src);

    IRowStream *createRowStream(unsigned spillPriority=SPILL_PRIORITY_SPILLABLE_STREAM);

    offset_t serializedSize()
    {
        if (firstRow > 0)
            throwUnexpected();
        return CThorExpandingRowArray::serializedSize();
    }
    memsize_t getMemUsage() { return CThorExpandingRowArray::getMemUsage(); }
    void serialize(MemoryBuffer &mb)
    {
        if (firstRow > 0)
            throwUnexpected();
        CThorExpandingRowArray::serialize(mb);
    }
    void deserialize(size32_t sz, const void *buf, bool hasNulls){ CThorExpandingRowArray::deserialize(sz, buf); }
    void deserializeRow(IRowDeserializerSource &in) { CThorExpandingRowArray::deserializeRow(in); }
    bool resize(rowidx_t requiredRows) { return CThorExpandingRowArray::resize(requiredRows); }
    void transferRowsCopy(const void **outRows, bool takeOwnership);
    void readBlock(const void **outRows, rowidx_t readRows);

    virtual IThorArrayLock &queryLock() { return *this; }
// IThorArrayLock
    virtual void lock() const { cs.enter(); }
    virtual void unlock() const { cs.leave(); }

private:
    void clearRows();
    const void **getBlock(rowidx_t readRows);
};


enum RowCollectorSpillFlags { rc_mixed, rc_allMem, rc_allDisk, rc_allDiskOrAllMem };
enum RowCollectorOptionFlags { rcflag_noAllInMemSort=0x01 };
interface IThorRowCollectorCommon : extends IInterface
{
    virtual rowcount_t numRows() const = 0;
    virtual unsigned numOverflows() const = 0;
    virtual unsigned overflowScale() const = 0;
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort=true) = 0;
    virtual void transferRowsIn(CThorExpandingRowArray &src) = 0;
    virtual void transferRowsIn(CThorSpillableRowArray &src) = 0;
    virtual void setup(ICompare *iCompare, StableSortFlag stableSort=stableSort_none, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=50) = 0;
    virtual void resize(rowidx_t max) = 0;
    virtual void setOptions(unsigned options) = 0;
};

interface IThorRowLoader : extends IThorRowCollectorCommon
{
    virtual IRowStream *load(IRowStream *in, const bool &abort, bool preserveGrouping=false, CThorExpandingRowArray *allMemRows=NULL, memsize_t *memUsage=NULL, bool doReset=true) = 0;
    virtual IRowStream *loadGroup(IRowStream *in, const bool &abort, CThorExpandingRowArray *allMemRows=NULL, memsize_t *memUsage=NULL, bool doReset=true) = 0;
};

interface IThorRowCollector : extends IThorRowCollectorCommon
{
    virtual void setPreserveGrouping(bool tf) = 0;
    virtual IRowWriter *getWriter() = 0;
    virtual void reset() = 0;
    virtual IRowStream *getStream(bool shared=false, CThorExpandingRowArray *allMemRows=NULL) = 0;
};

extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, StableSortFlag stableSort=stableSort_none, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=SPILL_PRIORITY_DEFAULT);
extern graph_decl IThorRowLoader *createThorRowLoader(CActivityBase &activity, ICompare *iCompare=NULL, StableSortFlag stableSort=stableSort_none, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=SPILL_PRIORITY_DEFAULT);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare=NULL, StableSortFlag stableSort=stableSort_none, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=SPILL_PRIORITY_DEFAULT, bool preserveGrouping=false);
extern graph_decl IThorRowCollector *createThorRowCollector(CActivityBase &activity, ICompare *iCompare=NULL, StableSortFlag stableSort=stableSort_none, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=SPILL_PRIORITY_DEFAULT, bool preserveGrouping=false);




class CSDSServerStatus;


extern graph_decl ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *status=NULL);

extern graph_decl void setThorVMSwapDirectory(const char *swapdir);

//The following array defines the buckets that are created for pooling row allocations.  The sizes must be in ascending order, and terminated with 0
const static unsigned thorAllocSizes[] = { 16, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 0 };

#endif
