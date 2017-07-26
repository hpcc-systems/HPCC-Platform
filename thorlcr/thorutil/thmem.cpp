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

#include "platform.h"

#include "jmisc.hpp"
#include "jio.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jflz.hpp"

#include "thbufdef.hpp"
#include "thor.hpp"
#include "thormisc.hpp"
#include "thorsort.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "daclient.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "eclrtl.hpp"
#include "roxiemem.hpp"
#include "roxierow.hpp"

#include "thmem.hpp"
#include "thgraph.hpp"

#include "thalloc.hpp"

#undef ALLOCATE
#undef CLONE
#undef MEMACTIVITYTRACESTRING 


#include "thbuf.hpp"
#include "thmem.hpp"

#ifdef _DEBUG
//#define _TESTING
#define ASSERTEX(c) assertex(c)
#else
#define ASSERTEX(c)
#endif

static memsize_t MTthreshold=0; 
static CriticalSection MTcritsect;  // held when blocked 
static Owned<ILargeMemLimitNotify> MTthresholdnotify;
static bool MTlocked = false;

#define DEFAULT_SORT_COMPBLKSZ 0x10000 // 64K

void checkMultiThorMemoryThreshold(bool inc)
{
    if (MTthresholdnotify.get()) {
        CriticalBlock block(MTcritsect);
        memsize_t used = 0; // JCSMORE - might work via callback in new scheme
        if (MTlocked) {
            if (used<MTthreshold/2) {
                DBGLOG("Multi Thor threshold lock released: %" I64F "d",(offset_t)used);
                MTlocked = false;
                MTthresholdnotify->give(used);
            }
        }
        else if (used>MTthreshold) {
            DBGLOG("Multi Thor threshold  exceeded: %" I64F "d",(offset_t)used);
            if (!MTthresholdnotify->take(used)) {
                throw createOutOfMemException(-9,
                    1024,  // dummy value
                    used);
            }
            DBGLOG("Multi Thor lock taken");
            MTlocked = true;
        }
    }
}

extern graph_decl void setMultiThorMemoryNotify(size32_t size,ILargeMemLimitNotify *notify)
{
    CriticalBlock block(MTcritsect);
    if (MTthresholdnotify.get()&&!notify&&MTlocked) {
        MTlocked = false;
        MTthresholdnotify->give(0);
    }
    MTthreshold = size;
    MTthresholdnotify.set(notify);
    if (notify)
        checkMultiThorMemoryThreshold(true);
}


// =================================

StringBuffer &getRecordString(const void *key, IOutputRowSerializer *serializer, const char *prefix, StringBuffer &out)
{
    MemoryBuffer mb;
    const byte *k = (const byte *)key;
    size32_t sz = 0;
    if (serializer&&k) {
        CMemoryRowSerializer mbsz(mb);
        serializer->serialize(mbsz,(const byte *)k);
        k = (const byte *)mb.bufferBase();
        sz = mb.length();
    }
    if (sz)
        out.appendf("%s(%d): ",prefix,sz);
    else {
        out.append(prefix).append(": ");
        if (k)
            sz = 16;
        else
            out.append("NULL");
    }
    bool first=false;
    while (sz) {
        if (first)
            first=false;
        else
            out.append(',');
        if ((sz>=3)&&isprint(k[0])&&isprint(k[1])&&isprint(k[2])) {
            out.append('"');
            do {
                out.append(*k);
                sz--;
                if (sz==0)
                    break;
                if (out.length()>1024)
                    break;
                k++;
            } while (isprint(*k));
            out.append('"');
        }
        if (out.length()>1024) {
            out.append("...");
            break;
        }
        if (sz) {
            out.appendf("%2x",(unsigned)*k);
            k++;
            sz--;
        }
    }
    return out;
}

class CSpillable : public CSimpleInterfaceOf<roxiemem::IBufferedRowCallback>
{
protected:
    bool mmRegistered = false;
    bool mmActivated = false;
    bool clearCB = false; // if true, deregisters the roxiemem callback on deactivation, otherwise leaves registered but inactive.
    unsigned spillPriority = SPILL_PRIORITY_DISABLE;
    IThorRowInterfaces *rowIf = nullptr;
    roxiemem::IRowManager *rowManager = nullptr;
    CActivityBase &activity;
public:
    CSpillable(CActivityBase &_activity, IThorRowInterfaces *_rowIf, unsigned _spillPriority) : activity(_activity), rowIf(_rowIf), spillPriority(_spillPriority)
    {
        if (rowIf)
            rowManager = rowIf->queryRowManager();
    }
    ~CSpillable()
    {
        ensureSpillingCallbackRemoved();
    }
    inline bool spillingEnabled() const { return SPILL_PRIORITY_DISABLE != spillPriority; }
    inline void activateSpillingCallback()
    {
        if (!mmActivated && spillingEnabled())
        {
            ensureSpillingCallbackInstalled();
            mmActivated = true;
        }
    }
    inline void deactivateSpillingCallback()
    {
        if (mmActivated)
        {
            if (clearCB)
                ensureSpillingCallbackRemoved(); // will re-add on next activateSpillingCallback()
            else // leave registered
                mmActivated = false;
        }
    }
    inline void ensureSpillingCallbackInstalled()
    {
        if (!mmRegistered)
        {
            assertex(rowManager);
            rowManager->addRowBuffer(this);
            mmRegistered = true;
        }
    }
    inline void ensureSpillingCallbackRemoved()
    {
        if (mmRegistered)
        {
            rowManager->removeRowBuffer(this);
            mmActivated = mmRegistered = false;
        }
    }
// IBufferedRowCallback
    virtual unsigned getSpillCost() const
    {
        return spillPriority;
    }
    virtual unsigned getActivityId() const
    {
        return activity.queryActivityId();
    }
};

//====

// NB: rows are transferred into derivatives of CSpillableStreamBase and read or spilt, but are never written to
class CSpillableStreamBase : public CSpillable
{
protected:
    bool ownsRows;
    EmptyRowSemantics emptyRowSemantics;
    unsigned spillCompInfo;
    CThorSpillableRowArray rows;
    OwnedIFile spillFile;

    bool spillRows()
    {
        // NB: Should always be called whilst 'rows' is locked (with CThorArrayLockBlock)
        rowidx_t numRows = rows.numCommitted();
        if (0 == numRows)
            return false;

        StringBuffer tempName;
        VStringBuffer tempPrefix("streamspill_%d", activity.queryId());
        GetTempName(tempName, tempPrefix.str(), true);
        spillFile.setown(createIFile(tempName.str()));

        VStringBuffer spillPrefixStr("SpillableStream(%d)", SPILL_PRIORITY_SPILLABLE_STREAM); // const for now
        rows.save(*spillFile, spillCompInfo, false, spillPrefixStr.str()); // saves committed rows
        rows.kill(); // no longer needed, readers will pull from spillFile. NB: ok to kill array as rows is never written to or expanded
        return true;
    }
public:
    CSpillableStreamBase(CActivityBase &_activity, CThorSpillableRowArray &inRows, IThorRowInterfaces *_rowIf, EmptyRowSemantics _emptyRowSemantics, unsigned _spillPriority)
        : CSpillable(_activity, _rowIf, _spillPriority), rows(_activity), emptyRowSemantics(_emptyRowSemantics)
    {
        assertex(inRows.isFlushed());
        ownsRows = false;
        spillCompInfo = 0x0;
        rows.setup(rowIf, emptyRowSemantics);
        rows.swap(inRows);
    }
    ~CSpillableStreamBase()
    {
        ensureSpillingCallbackRemoved();
        if (spillFile)
            spillFile->remove();
    }
// IBufferedRowCallback
    virtual bool freeBufferedRows(bool critical)
    {
        if (spillFile) // i.e. if spilt already. NB: this is thread-safe, as 'spillFile' only set by spillRows() call below and can't be called on multiple threads concurrently.
            return false;
        CThorArrayLockBlock block(rows);
        return spillRows();
    }
friend class CRowsLockBlock;
};

class CRowsLockBlock
{
    CSpillableStreamBase &owner;
public:
    inline CRowsLockBlock(CSpillableStreamBase &_owner) : owner(_owner)
    {
        owner.rows.lock();
        clearCB = false;
    }
    inline ~CRowsLockBlock()
    {
        owner.rows.unlock();
        if (clearCB)
            owner.deactivateSpillingCallback();
    }
    bool clearCB;
};

// NB: Shared/spillable, holds all rows in mem until needs to spill.
// spills all to disk, and stream continue reading from row in file
class CSharedSpillableRowSet : public CSpillableStreamBase
{
    class CStream : public CSimpleInterface, implements IRowStream, implements IWritePosCallback
    {
        rowidx_t pos = 0;
        offset_t outputOffset = (offset_t)-1;
        Owned<IRowStream> spillStream;
        Linked<CSharedSpillableRowSet> owner;
        rowidx_t toRead = 0;
        bool eos = false;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStream(CSharedSpillableRowSet &_owner, rowidx_t _toRead) : owner(&_owner), toRead(_toRead)
        {
            // NB: CStream constructor called within rows lock and only called if not yet spilled
            owner->rows.registerWriteCallback(*this);
        }
        ~CStream()
        {
            spillStream.clear(); // NB: clear stream 1st
            owner->rows.safeUnregisterWriteCallback(*this);
            owner.clear();
        }
    // IRowStream
        virtual const void *nextRow()
        {
            if (!eos)
            {
                const void *ret;
                if (spillStream)
                    ret = spillStream->nextRow();
                else
                {
                    CRowsLockBlock block(*owner);
                    if (owner->spillFile) // i.e. has spilt
                    {
                        block.clearCB = true;
                        assertex(((offset_t)-1) != outputOffset);
                        unsigned rwFlags = DEFAULT_RWFLAGS | mapESRToRWFlags(owner->emptyRowSemantics);
                        spillStream.setown(::createRowStreamEx(owner->spillFile, owner->rowIf, outputOffset, (offset_t)-1, (unsigned __int64)-1, rwFlags));
                        owner->rows.unregisterWriteCallback(*this); // no longer needed
                        ret = spillStream->nextRow();
                    }
                    else
                    {
                        // NB: would not reach here if nothing left to read
                        ret = owner->rows.get(pos++);
                        if (pos == toRead)
                        {
                            owner->rows.unregisterWriteCallback(*this); // no longer needed
                            eos = true; // for any subsequent calls
                        }
                        return ret;
                    }
                }
                if (ret)
                    return ret;
                if (ers_forbidden == owner->emptyRowSemantics)
                    eos = true;
            }
            return nullptr;
        }
        virtual void stop()
        {
            owner->rows.safeUnregisterWriteCallback(*this); // no longer needed
        }
    // IWritePosCallback
        virtual rowidx_t queryRecordNumber()
        {
            return pos;
        }
        virtual void filePosition(offset_t pos)
        {
            // Called via spilling save, stream will continue reading from file @ pos
            outputOffset = pos;
        }
    };

public:
    CSharedSpillableRowSet(CActivityBase &_activity, CThorSpillableRowArray &inRows, IThorRowInterfaces *_rowIf, EmptyRowSemantics _emptyRowSemantics, unsigned _spillPriority)
        : CSpillableStreamBase(_activity, inRows, _rowIf, _emptyRowSemantics, _spillPriority)
    {
        activateSpillingCallback();
    }
    IRowStream *createRowStream()
    {
        CRowsLockBlock block(*this);
        if (spillFile) // already spilled?
        {
            block.clearCB = true;
            unsigned rwFlags = DEFAULT_RWFLAGS | mapESRToRWFlags(emptyRowSemantics);
            return ::createRowStream(spillFile, rowIf, rwFlags);
        }
        rowidx_t toRead = rows.numCommitted();
        if (toRead)
            return new CStream(*this, toRead);
        else
            return createNullRowStream();
    }
};

// NB: A single unshared spillable stream
class CSpillableStream : public CSpillableStreamBase, implements IRowStream
{
    rowidx_t pos, numReadRows, granularity;
    const void **readRows;
    Owned<IRowStream> spillStream;

public:
    IMPLEMENT_IINTERFACE_USING(CSpillableStreamBase);

    CSpillableStream(CActivityBase &_activity, CThorSpillableRowArray &inRows, IThorRowInterfaces *_rowIf, EmptyRowSemantics _emptyRowSemantics, unsigned _spillPriority, unsigned _spillCompInfo)
        : CSpillableStreamBase(_activity, inRows, _rowIf, _emptyRowSemantics, _spillPriority)
    {
        spillCompInfo = _spillCompInfo;
        pos = numReadRows = 0;
        granularity = 500; // JCSMORE - rows

        // a small amount of rows to read from swappable rows
        readRows = static_cast<const void * *>(rowIf->queryRowManager()->allocate(granularity * sizeof(void*), activity.queryContainer().queryId(), inRows.queryDefaultMaxSpillCost()));
        activateSpillingCallback();
    }
    ~CSpillableStream()
    {
        spillStream.clear();
        roxiemem::ReleaseRoxieRowRange(readRows, pos, numReadRows);
        ReleaseThorRow(readRows);
    }

// IRowStream
    virtual const void *nextRow()
    {
        if (spillStream)
            return spillStream->nextRow();
        if (pos == numReadRows)
        {
            CRowsLockBlock block(*this);
            if (spillFile)
            {
                block.clearCB = true;
                unsigned rwFlags = DEFAULT_RWFLAGS;
                if (spillCompInfo)
                {
                    rwFlags |= rw_compress;
                    rwFlags |= spillCompInfo;
                }
                rwFlags |= mapESRToRWFlags(emptyRowSemantics);
                spillStream.setown(createRowStream(spillFile, rowIf, rwFlags));
                return spillStream->nextRow();
            }
            rowidx_t available = rows.numCommitted();
            if (0 == available)
            {
                block.clearCB = true;
                return NULL;
            }
            rowidx_t fetch = (available >= granularity) ? granularity : available;
            // consume 'fetch' rows
            rows.readBlock(readRows, fetch);
            if (available == fetch)
            {
                block.clearCB = true;
                rows.kill();
            }
            numReadRows = fetch;
            pos = 0;
        }
        const void *row = readRows[pos];
        readRows[pos] = NULL;
        ++pos;
        return row;
    }
    virtual void stop()
    {
        deactivateSpillingCallback();
    }
};

//====

class CResizeRowCallback : implements roxiemem::IRowResizeCallback
{
    IThorArrayLock &alock;
    void **&rows;
    memsize_t &capacity;
public:
    CResizeRowCallback(void **&_rows, memsize_t &_capacity, IThorArrayLock &_alock) : rows(_rows), capacity(_capacity), alock(_alock) { }
    virtual void lock() { alock.lock(); }
    virtual void unlock() { alock.unlock(); }
    virtual void update(memsize_t _capacity, void * ptr) { capacity = _capacity; rows = (void **)ptr; }
    virtual void atomicUpdate(memsize_t capacity, void * ptr)
    {
        CThorArrayLockBlock block(alock);
        update(capacity, ptr);
    }
};

//====

const void **CThorExpandingRowArray::allocateRowTable(rowidx_t num)
{
    return _allocateRowTable(num, defaultMaxSpillCost);
}

const void **CThorExpandingRowArray::allocateRowTable(rowidx_t num, unsigned maxSpillCost)
{
    return _allocateRowTable(num, maxSpillCost);
}


rowidx_t CThorExpandingRowArray::getNewSize(rowidx_t requiredRows)
{
    rowidx_t newSize = maxRows;
    //This condition must be <= at least 1/scaling factor below otherwise you'll get an infinite loop.
    if (newSize <= 4)
        newSize = requiredRows;
    else
    {
        //What algorithm should we use to increase the size?  Trading memory usage against copying row pointers.
        // adding 50% would reduce the number of allocations.
        // anything below 32% would mean that blocks n,n+1 when freed have enough space for block n+3 which might
        //   reduce fragmentation.
        //Use 25% for the moment.  It should possibly be configurable - e.g., higher for thor global sort.
        while (newSize < requiredRows)
            newSize += newSize/4;
    }
    return newSize;
}

bool CThorExpandingRowArray::resizeRowTable(void **&_rows, rowidx_t requiredRows, bool copy, unsigned maxSpillCost, memsize_t &capacity, const char *errMsg)
{
    //Only the writer is allowed to reallocate rows (otherwise append can't be optimized), so rows is valid outside the lock

    // NB: only resize alters row capacity, so no locking required to protect getRowsCapacity()
    memsize_t copySize;
    if (requiredRows > maxRows)
    {
        capacity = ((memsize_t)getNewSize(requiredRows)) * sizeof(void *);
        copySize = copy&&_rows?RoxieRowCapacity(_rows):0;
    }
    else
    {
        capacity = ((memsize_t)requiredRows) * sizeof(void *);
        copySize = copy?capacity:0;
    }

    CResizeRowCallback callback(_rows, capacity, queryLock());
    try
    {
        if (_rows)
            rowManager->resizeRow(_rows, copySize, capacity, activity.queryContainer().queryId(), maxSpillCost, callback);
        else
        {
            void **newRows = (void **)rowManager->allocate(capacity, activity.queryContainer().queryId(), maxSpillCost);
            callback.atomicUpdate(RoxieRowCapacity(newRows), newRows);
        }
    }
    catch (IException *e)
    {
        //Pathological cases - not enough memory to reallocate the target row buffer, or no contiguous pages available.
        unsigned code = e->errorCode();
        if ((code == ROXIEMM_MEMORY_LIMIT_EXCEEDED) || (code == ROXIEMM_MEMORY_POOL_EXHAUSTED))
        {
            e->Release();
            if (throwOnOom)
                throw MakeActivityException(&activity, code, "Out of memory, resizing %s, had %" RIPF "d, trying to allocate %" RIPF "d elements", errMsg, ordinality(), requiredRows);
            return false;
        }
        throw;
    }
    return true;
}

void CThorExpandingRowArray::doSort(rowidx_t n, void **const rows, ICompare &compare, unsigned maxCores)
{
    // NB: will only be called if numRows>1
    if (stableSort_none != stableSort)
    {
        OwnedConstThorRow tmpStableTable;
        void **stableTablePtr;
        if (stableSort_lateAlloc == stableSort)
        {
            dbgassertex(NULL == stableTable);
            tmpStableTable.setown(rowManager->allocate(getRowsCapacity() * sizeof(void *), activity.queryContainer().queryId(), defaultMaxSpillCost));
            stableTablePtr = (void **)tmpStableTable.get();
        }
        else
        {
            dbgassertex(NULL != stableTable);
            stableTablePtr = stableTable;
        }
        parsortvecstableinplace(rows, n, compare, stableTablePtr, maxCores);
    }
    else
        parqsortvec((void **const)rows, n, compare, maxCores);
}

inline const void **CThorExpandingRowArray::_allocateRowTable(rowidx_t num, unsigned maxSpillCost)
{
    try
    {
        return (const void **)rowManager->allocate(num * sizeof(void*), activity.queryContainer().queryId(), maxSpillCost);
    }
    catch (IException * e)
    {
        unsigned code = e->errorCode();
        if ((code == ROXIEMM_MEMORY_LIMIT_EXCEEDED) || (code == ROXIEMM_MEMORY_POOL_EXHAUSTED))
        {
            e->Release();
            return NULL;
        }
        throw;
    }
}

inline bool CThorExpandingRowArray::_resize(rowidx_t requiredRows, unsigned maxSpillCost)
{
    //Only the writer is allowed to reallocate rows (otherwise append can't be optimized), so rows is valid outside the lock
    if (0 == requiredRows)
    {
        CThorArrayLockBlock block(queryLock());
        clearRows();
        ReleaseThorRow(rows);
        ReleaseThorRow(stableTable);
        rows = NULL;
        stableTable = NULL;
        numRows = maxRows = 0;
        return true;
    }

    // NB: only resize alters row capacity, so no locking required to protect getRowsCapacity()
    memsize_t capacity;
    rowidx_t currentMaxRows = getRowsCapacity();
    if (currentMaxRows == requiredRows)
        capacity = rows ? RoxieRowCapacity(rows) : 0;
    else
    {
        if (currentMaxRows > requiredRows) // shrink
        {
            if (numRows > requiredRows)
            {
                roxiemem::ReleaseRoxieRowRange(rows, requiredRows, numRows);
                numRows = requiredRows;
            }
        }
        if (!resizeRowTable((void **&)rows, requiredRows, true, maxSpillCost, capacity, "row array"))
            return false;
    }
    if (stableSort_earlyAlloc == stableSort)
    {
        memsize_t dummy;
        if (!resizeRowTable(stableTable, requiredRows, false, maxSpillCost, dummy, "stable row array"))
            return false;
        // NB: If allocation of stableTable fails, 'rows' has expanded, but maxRows has not
        // this means, that on a subsequent resize() call, it will only need to [attempt] to resize the stable ptr array.
        // (see comment if (currentMaxRows < requiredRows) check above
    }

    // Both row tables updated, only now update maxRows
    CThorArrayLockBlock block(queryLock());
    maxRows = capacity / sizeof(void *);
    return true;
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity) : activity(_activity)
{
    rowManager = activity.queryRowManager();
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, IThorRowInterfaces *_rowIf, EmptyRowSemantics _emptyRowSemantics, StableSortFlag _stableSort, bool _throwOnOom, rowidx_t initialSize)
    : activity(_activity)
{
    setup(_rowIf, _emptyRowSemantics, _stableSort, _throwOnOom);
    if (initialSize)
    {
        rows = static_cast<const void * *>(rowManager->allocate(initialSize * sizeof(void*), activity.queryContainer().queryId(), defaultMaxSpillCost));
        maxRows = getRowsCapacity();
        memset(rows, 0, maxRows * sizeof(void *));
        if (stableSort_earlyAlloc == stableSort)
            stableTable = static_cast<void **>(rowManager->allocate(maxRows * sizeof(void*), activity.queryContainer().queryId(), defaultMaxSpillCost));
    }
}

CThorExpandingRowArray::~CThorExpandingRowArray()
{
    clearRows();
    ReleaseThorRow(rows);
    ReleaseThorRow(stableTable);
}

void CThorExpandingRowArray::setup(IThorRowInterfaces *_rowIf, EmptyRowSemantics _emptyRowSemantics, StableSortFlag _stableSort, bool _throwOnOom)
{
    rowIf = _rowIf;
    emptyRowSemantics = _emptyRowSemantics;
    stableSort = _stableSort;
    throwOnOom = _throwOnOom;
    if (rowIf)
    {
        allocator = rowIf->queryRowAllocator();
        deserializer = rowIf->queryRowDeserializer();
        serializer = rowIf->queryRowSerializer();
        rowManager = rowIf->queryRowManager();
    }
    else
    {
        allocator = NULL;
        deserializer = NULL;
        serializer = NULL;
        rowManager = activity.queryRowManager();
    }
    if (maxRows && (NULL != stableTable) && (stableSort_earlyAlloc != stableSort))
    {
        ReleaseThorRow(stableTable);
        stableTable = NULL;
    }
}

void CThorExpandingRowArray::clearRows()
{
    roxiemem::ReleaseRoxieRowArray(numRows, rows);
    numRows = 0;
}

void CThorExpandingRowArray::compact()
{
    const void **freeFinger = rows;
    const void **filledFinger = NULL;
    const void **rowEnd = rows+numRows;
    //skip any leading filled in entries.
    while (freeFinger != rowEnd && *freeFinger)
        freeFinger++;

    // move any subsequent filled in entries.
    for (filledFinger = freeFinger; filledFinger != rowEnd; filledFinger++)
    {
        if (*filledFinger)
        {
            *freeFinger++ = *filledFinger;
            *filledFinger = NULL;
        }
    }
    numRows = freeFinger-rows;
    memsize_t numEmptiedPages = rowManager->compactRows(numRows, rows);

#ifdef _DEBUG
    ActPrintLog(&activity, "CThorExpandingRowArray::compact(): compactRows freed %" I64F "d pages", (unsigned __int64)numEmptiedPages);
#endif

    // NB: As always shrinking, it will never reallocate, default maxSpillCost will not be used by roxiemem.
    if (!resize(numRows) && throwOnOom)
        throw MakeActivityException(&activity, ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Out of memory trying to compact row pointer array");
}

void CThorExpandingRowArray::kill()
{
    clearRows();
    maxRows = 0;
    ReleaseThorRow(rows);
    ReleaseThorRow(stableTable);
    rows = NULL;
    stableTable = NULL;
}

void CThorExpandingRowArray::swap(CThorExpandingRowArray &other)
{
    roxiemem::IRowManager *otherRowManager = other.rowManager;
    IThorRowInterfaces *otherRowIf = other.rowIf;
    const void **otherRows = other.rows;
    void **otherStableTable = other.stableTable;
    EmptyRowSemantics otherEmptyRowSemantics = other.emptyRowSemantics;
    StableSortFlag otherStableSort = other.stableSort;
    bool otherThrowOnOom = other.throwOnOom;
    unsigned otherDefaultMaxSpillCost = other.defaultMaxSpillCost;
    rowidx_t otherMaxRows = other.maxRows;
    rowidx_t otherNumRows = other.numRows;

    other.rowManager = rowManager;
    other.rows = rows;
    other.stableTable = stableTable;
    other.maxRows = maxRows;
    other.numRows = numRows;
    other.setup(rowIf, emptyRowSemantics, stableSort, throwOnOom);
    other.setDefaultMaxSpillCost(defaultMaxSpillCost);

    rowManager = otherRowManager;
    rows = otherRows;
    stableTable = otherStableTable;
    maxRows = otherMaxRows;
    numRows = otherNumRows;
    setup(otherRowIf, otherEmptyRowSemantics, otherStableSort, otherThrowOnOom);
    setDefaultMaxSpillCost(otherDefaultMaxSpillCost);
}

void CThorExpandingRowArray::transferRows(rowidx_t & outNumRows, const void * * & outRows)
{
    outNumRows = numRows;
    outRows = rows;
    numRows = 0;
    maxRows = 0;
    rows = NULL;
    ReleaseThorRow(stableTable);
    stableTable = NULL;
}

void CThorExpandingRowArray::transferRowsCopy(const void **outRows, bool takeOwnership)
{
    if (0 == numRows)
        return;
    memcpy(outRows, rows, numRows*sizeof(void **));
    if (takeOwnership)
        numRows = 0;
    else
    {
        const void **lastNewRow = outRows+numRows-1;
        for (;;)
        {
            LinkThorRow(*outRows);
            if (outRows == lastNewRow)
                break;
            outRows++;
        }
    }
}

void CThorExpandingRowArray::transferFrom(CThorExpandingRowArray &donor)
{
    kill();
    donor.transferRows(numRows, rows);
    maxRows = numRows;
    if (maxRows && (stableSort_earlyAlloc == stableSort))
        resize(maxRows);
}

void CThorExpandingRowArray::transferFrom(CThorSpillableRowArray &donor)
{
    transferFrom((CThorExpandingRowArray &)donor);
    donor.kill();
}

void CThorExpandingRowArray::removeRows(rowidx_t start, rowidx_t n)
{
    assertex(numRows-start >= n);
    assertex(!n || rows);
    if (rows)
    {
        rowidx_t end = start+n;
        roxiemem::ReleaseRoxieRowRange(rows, start, end);
        //firstRow = 0;
        const void **from = rows+start;
        memmove(from, from+n, (numRows-end) * sizeof(void *));
        numRows -= n;
    }
}

bool CThorExpandingRowArray::appendRows(CThorExpandingRowArray &inRows, bool takeOwnership)
{
    rowidx_t num = inRows.ordinality();
    if (0 == num)
        return true;
    if (numRows+num >= maxRows)
    {
        if (!resize(numRows + num))
            return false;
    }
    const void **newRows = rows+numRows;
    inRows.transferRowsCopy(newRows, takeOwnership);

    numRows += num;
    return true;
}

bool CThorExpandingRowArray::appendRows(CThorSpillableRowArray &inRows, bool takeOwnership)
{
    rowidx_t num = inRows.numCommitted();
    if (0 == num)
        return true;
    if (numRows+num >= maxRows)
    {
        if (!resize(numRows + num))
            return false;
    }
    const void **newRows = rows+numRows;
    inRows.transferRowsCopy(newRows, takeOwnership);

    numRows += num;
    return true;
}

bool CThorExpandingRowArray::binaryInsert(const void *row, ICompare &compare, bool dropLast)
{
    dbgassertex(NULL != row);
    if (numRows >= maxRows)
    {
        if (!resize(numRows+1))
            return false;
    }
    binary_vec_insert_stable(row, rows, numRows, compare); // takes ownership of row
    if (dropLast)
    {
        // last row falls out, i.e. release last row and don't increment numRows
        dbgassertex(numRows); // numRows must be >=1 for dropLast
        ReleaseThorRow(rows[numRows]);
    }
    else
        ++numRows;
    return true;
}

void CThorExpandingRowArray::clearUnused()
{
    if (rows)
        memset(rows+numRows, 0, (maxRows-numRows) * sizeof(void *));
}

bool CThorExpandingRowArray::resize(rowidx_t requiredRows, unsigned maxSpillCost)
{
    return _resize(requiredRows, maxSpillCost);
}

void CThorExpandingRowArray::sort(ICompare &compare, unsigned maxCores)
{
    if (numRows>1)
        doSort(numRows, (void **const)rows, compare, maxCores);
}

void CThorExpandingRowArray::reorder(rowidx_t start, rowidx_t num, rowidx_t *neworder)
{
    if (start>=numRows)
        return;
    if (start+num>numRows)
        num = numRows-start;
    if (!num)
        return;
    MemoryAttr ma;
    void **tmp = (void **)ma.allocate(num*sizeof(void *));
    const void **p = rows + start;
    memcpy(tmp, p, num*sizeof(void *));
    for (rowidx_t i=0; i<num; i++)
        p[i] = tmp[neworder[i]];
}

bool CThorExpandingRowArray::equal(ICompare *icmp, CThorExpandingRowArray &other)
{
    // slow but better than prev!
    rowidx_t n = other.ordinality();
    if (n!=ordinality())
        return false;
    for (rowidx_t i=0;i<n;i++)
    {
        const void *p1 = rows[i];
        const void *p2 = other.query(i);
        if (0 != icmp->docompare(p1, p2))
            return false;
    }
    return true;
}

bool CThorExpandingRowArray::checkSorted(ICompare *icmp)
{
    rowidx_t n=ordinality();
    for (rowidx_t i=1; i<n; i++)
    {
        if (icmp->docompare(rows[i-1], rows[i])>0)
            return false;
    }
    return true;
}

IRowStream *CThorExpandingRowArray::createRowStream(rowidx_t start, rowidx_t num, bool streamOwns)
{
    class CRowOwningStream : public CSimpleInterface, implements IRowStream
    {
        CThorExpandingRowArray rows;
        rowidx_t pos, lastRow;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CRowOwningStream(CThorExpandingRowArray &_rows, rowidx_t firstRow, rowidx_t _lastRow)
            : pos(firstRow), lastRow(_lastRow), rows(_rows.queryActivity())
        {
            rows.swap(_rows);
        }

    // IRowStream
        virtual const void *nextRow()
        {
            if (pos >= lastRow)
            {
                rows.kill();
                return NULL;
            }
            return rows.getClear(pos++);
        }
        virtual void stop()
        {
            rows.kill();
        }
    };
    class CStream : public CSimpleInterface, implements IRowStream
    {
        CThorExpandingRowArray *parent;
        rowidx_t pos, lastRow;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStream(CThorExpandingRowArray &_parent, rowidx_t firstRow, rowidx_t _lastRow)
            : pos(firstRow), lastRow(_lastRow)
        {
            parent = &_parent;
        }

    // IRowStream
        virtual const void *nextRow()
        {
            if (pos >= lastRow)
                return NULL;
            return parent->get(pos++);
        }
        virtual void stop() { }
    };

    dbgassertex(!streamOwns || ((0 == start) && ((rowidx_t)-1 == num)));
    if (start>ordinality())
        start = ordinality();
    rowidx_t lastRow;
    if ((num==(rowidx_t)-1)||(start+num>ordinality()))
        lastRow = ordinality();
    else
        lastRow = start+num;

    if (streamOwns)
        return new CRowOwningStream(*this, start, lastRow);
    else
        return new CStream(*this, start, lastRow);
}

void CThorExpandingRowArray::partition(ICompare &compare, unsigned num, UnsignedArray &out)
{
    rowidx_t p=0;
    rowidx_t n = ordinality();
    while (num)
    {
        out.append(p);
        if (p<n)
        {
            rowidx_t q = p+(n-p)/num;
            if (p==q) // skip to next group
            {
                while (q<n)
                {
                    q++;
                    if ((q<n)&&(compare.docompare(rows[p],rows[q])!=0)) // ensure at next group
                        break;
                }
            }
            else
            {
                while ((q<n)&&(q!=p)&&(compare.docompare(rows[q-1],rows[q])==0)) // ensure at start of group
                    q--;
            }
            p = q;
        }
        num--;
    }
    out.append(n);
}

offset_t CThorExpandingRowArray::serializedSize()
{
    IOutputMetaData *meta = rowIf->queryRowMetaData();
    IOutputMetaData *diskMeta = meta->querySerializedDiskMeta();
    rowidx_t c = ordinality();
    offset_t total = 0;
    if (diskMeta->isFixedSize())
        total = ((offset_t)c) * diskMeta->getFixedSize();
    else
    {
        Owned<IOutputRowSerializer> diskSerializer = diskMeta->createDiskSerializer(rowIf->queryCodeContext(), rowIf->queryActivityId());
        CSizingSerializer ssz;
        for (rowidx_t i=0; i<c; i++)
        {
            diskSerializer->serialize(ssz, (const byte *)rows[i]);
            total += ssz.size();
            ssz.reset();
        }
    }
    return total;
}


memsize_t CThorExpandingRowArray::getMemUsage()
{
    IOutputMetaData *meta = rowIf->queryRowMetaData();
    IOutputMetaData *diskMeta = meta->querySerializedDiskMeta(); // GH->JCS - really I want a internalMeta here.
    rowidx_t c = ordinality();
    memsize_t total = 0;
    if (diskMeta->isFixedSize())
        total = c * rowManager->getExpectedFootprint(diskMeta->getFixedSize(), 0);
    else
    {
        CSizingSerializer ssz;
        for (rowidx_t i=0; i<c; i++)
        {
            serializer->serialize(ssz, (const byte *)rows[i]);
            total += rowManager->getExpectedFootprint(ssz.size(), 0);
            ssz.reset();
        }
    }
    // NB: worst case, when expanding (see resize method)
    memsize_t sz = rowManager->getExpectedFootprint(maxRows * sizeof(void *), 0);
    memsize_t szE = sz / 100 * 125; // don't care if sz v. small
    if (stableSort_none == stableSort)
        total += sz + szE;
    else
        total += sz + szE * 2;
    return total;
}

void CThorExpandingRowArray::serialize(IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    rowidx_t n = ordinality();
    if (n)
    {
        for (rowidx_t i = 0; i < n; i++)
        {
            const void *row = query(i);
            if (row)
                serializer->serialize(out, (const byte *)row);
            else if (warnnull)
            {
                WARNLOG("CThorExpandingRowArray::serialize ignoring NULL row");
                warnnull = false;
            }
        }
    }
}

void CThorExpandingRowArray::serialize(MemoryBuffer &mb)
{
    assertex(serializer);
    CMemoryRowSerializer s(mb);
    if (emptyRowSemantics == ers_forbidden)
        serialize(s);
    else
    {
        unsigned short guard = 0x7631;
        mb.append(guard);
        rowidx_t n = ordinality();
        if (n)
        {
            for (rowidx_t i = 0; i < n; i++)
            {
                const void *row = query(i);
                bool isnull = (row==NULL);
                mb.append(isnull);
                if (!isnull)
                    serializer->serialize(s, (const byte *)row);
            }
        }
    }
}

void CThorExpandingRowArray::serializeCompress(MemoryBuffer &mb)
{
    MemoryBuffer exp;
    serialize(exp);
    fastLZCompressToBuffer(mb,exp.length(), exp.toByteArray());
}

rowidx_t CThorExpandingRowArray::serializeBlock(MemoryBuffer &mb, rowidx_t idx, rowidx_t count, size32_t dstmax, bool hardMax)
{
    assertex(serializer);
    CMemoryRowSerializer out(mb);
    bool warnnull = true;
    rowidx_t num=ordinality();
    if (idx>=num)
        return 0;
    if (num-idx<count)
        count = num-idx;
    rowidx_t ret = 0;
    for (rowidx_t i=0;i<count;i++)
    {
        size32_t ln = mb.length();
        const void *row = query(i+idx);
        if (row)
            serializer->serialize(out,(const byte *)row);
        else if (warnnull)
        {
            WARNLOG("CThorExpandingRowArray::serialize ignoring NULL row");
            warnnull = false;
        }
        // allows at least one
        if (mb.length()>dstmax)
        {
            if (hardMax && ln) // remove last if above limit
                mb.setLength(ln);
            else
                ++ret;
            break;
        }
        else
            ++ret;
    }
    return ret;
}

void CThorExpandingRowArray::deserializeRow(IRowDeserializerSource &in)
{
    RtlDynamicRowBuilder rowBuilder(allocator);
    size32_t sz = deserializer->deserialize(rowBuilder,in);
    append(rowBuilder.finalizeRowClear(sz));
}

void CThorExpandingRowArray::deserialize(size32_t sz, const void *buf)
{
    if (emptyRowSemantics != ers_forbidden)
    {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos())
    {
        if (emptyRowSemantics != ers_forbidden)
        {
            bool nullrow;
            d.read(sizeof(bool),&nullrow);
            if (nullrow)
            {
                append(NULL);
                continue;
            }
        }
        deserializeRow(d);
    }
}

void CThorExpandingRowArray::deserializeExpand(size32_t sz, const void *data)
{
    MemoryBuffer mb;
    fastLZDecompressToBuffer(mb, data);
    deserialize(mb.length(), mb.bufferBase());
}

//////////////////

void CThorSpillableRowArray::registerWriteCallback(IWritePosCallback &cb)
{
    writeCallbacks.append(cb); // NB not linked to avoid circular dependency
}

void CThorSpillableRowArray::unregisterWriteCallback(IWritePosCallback &cb)
{
    writeCallbacks.zap(cb);
}

void CThorSpillableRowArray::safeRegisterWriteCallback(IWritePosCallback &cb)
{
    CThorArrayLockBlock block(*this);
    writeCallbacks.append(cb); // NB not linked to avoid circular dependency
}

void CThorSpillableRowArray::safeUnregisterWriteCallback(IWritePosCallback &cb)
{
    CThorArrayLockBlock block(*this);
    writeCallbacks.zap(cb);
}

CThorSpillableRowArray::CThorSpillableRowArray(CActivityBase &activity)
    : CThorExpandingRowArray(activity)
{
    throwOnOom = false;
}

CThorSpillableRowArray::CThorSpillableRowArray(CActivityBase &activity, IThorRowInterfaces *rowIf, EmptyRowSemantics emptyRowSemantics, StableSortFlag stableSort, rowidx_t initialSize, size32_t _commitDelta)
    : CThorExpandingRowArray(activity, rowIf, ers_forbidden, stableSort, false, initialSize), commitDelta(_commitDelta)
{
}

CThorSpillableRowArray::~CThorSpillableRowArray()
{
    clearRows();
}

void CThorSpillableRowArray::clearRows()
{
    roxiemem::ReleaseRoxieRowRange(rows, firstRow, numRows);
    numRows = 0;
    firstRow = 0;
    commitRows = 0;
}

void CThorSpillableRowArray::compact()
{
    CThorArrayLockBlock block(*this);
    assertex(0 == firstRow && numRows == commitRows);
    CThorExpandingRowArray::compact();
    commitRows = numRows;
}

void CThorSpillableRowArray::kill()
{
    clearRows();
    CThorExpandingRowArray::kill();
}

void CThorSpillableRowArray::sort(ICompare &compare, unsigned maxCores)
{
    // NB: only to be called inside lock
    rowidx_t n = numCommitted();
    if (n>1)
    {
        void **const rows = (void **const)getBlock(n);
        doSort(n, rows, compare, maxCores);
    }
}

static int callbackSortRev(IInterface * const *cb2, IInterface * const *cb1)
{
    rowidx_t i2 = ((IWritePosCallback *)(*cb2))->queryRecordNumber();
    rowidx_t i1 = ((IWritePosCallback *)(*cb1))->queryRecordNumber();

    if (i1==i2) return 0;
    if (i1<i2) return -1;
    return 1;
}

rowidx_t CThorSpillableRowArray::save(IFile &iFile, unsigned _spillCompInfo, bool skipNulls, const char *tracingPrefix)
{
    rowidx_t n = numCommitted();
    if (0 == n)
        return 0;
    ActPrintLog(&activity, "%s: CThorSpillableRowArray::save (skipNulls=%s, emptyRowSemantics=%u) max rows = %"  RIPF "u", tracingPrefix, boolToStr(skipNulls), emptyRowSemantics, n);

    if (_spillCompInfo)
        assertex(0 == writeCallbacks.ordinality()); // incompatible

    unsigned rwFlags = DEFAULT_RWFLAGS;
    if (_spillCompInfo)
    {
        rwFlags |= rw_compress;
        rwFlags |= _spillCompInfo;
    }
    rwFlags |= mapESRToRWFlags(emptyRowSemantics);

    // NB: This is always called within a CThorArrayLockBlock, as such no writebacks are added or updating
    rowidx_t nextCBI = RCIDXMAX; // indicates none
    IWritePosCallback *nextCB = NULL;
    ICopyArrayOf<IWritePosCallback> cbCopy;
    if (writeCallbacks.ordinality())
    {
        ForEachItemIn(c, writeCallbacks)
            cbCopy.append(writeCallbacks.item(c));
        cbCopy.sort(callbackSortRev);
        nextCB = &cbCopy.popGet();
        nextCBI = nextCB->queryRecordNumber();
    }
    Owned<IExtRowWriter> writer = createRowWriter(&iFile, rowIf, rwFlags, nullptr, compBlkSz);
    rowidx_t i=0;
    rowidx_t rowsWritten=0;
    try
    {
        const void **rows = getBlock(n);
        while (i<n)
        {
            const void *row = rows[i];
            if (i == nextCBI)
            {
                writer->flush();
                do
                {
                    nextCB->filePosition(writer->getPosition());
                    if (cbCopy.ordinality())
                    {
                        nextCB = &cbCopy.popGet();
                        nextCBI = nextCB->queryRecordNumber();
                    }
                    else
                        nextCBI = RCIDXMAX; // indicating no more
                }
                while (i == nextCBI); // loop as may be >1 IWritePosCallback at same pos
            }
            if (row)
            {
                ++rowsWritten;
                rows[i] = NULL;
                writer->putRow(row); // NB: putRow takes ownership/should avoid leaking if fails
            }
            else if (!skipNulls)
            {
                assertex(emptyRowSemantics != ers_forbidden);
                writer->putRow(NULL);
            }
            ++i;
        }
        writer->flush();
    }
    catch (IException *e)
    {
        EXCLOG(e, "CThorSpillableRowArray::save");
        firstRow += i; // ensure released rows are noted.
        throw;
    }
    firstRow += n;
    offset_t bytesWritten = writer->getPosition();
    writer.clear();
    ActPrintLog(&activity, "%s: CThorSpillableRowArray::save done, rows written = %" RIPF "u, bytes = %" I64F "u", tracingPrefix, rowsWritten, (__int64)bytesWritten);
    return n;
}


// JCSMORE - these methods are essentially borrowed from RoxieOutputRowArray, would be good to unify
const void **CThorSpillableRowArray::getBlock(rowidx_t readRows)
{
    dbgassertex(firstRow+readRows <= commitRows);
    return rows + firstRow;
}

void CThorSpillableRowArray::doFlush()
{
    //A block of rows was removed - copy these rows to the start of the block.
    memmove(rows, rows+firstRow, (numRows-firstRow) * sizeof(void *));
    numRows -= firstRow;
    firstRow = 0;
    commitRows = numRows;
}

bool CThorSpillableRowArray::_flush(bool force)
{
    dbgassertex(numRows >= commitRows);
    // if firstRow over 50% of commitRows, meaning over half of row array is empty, then reduce
    if (needToMoveRows(force))
    {
        doFlush();
        return true;
    }
    else
    {
        commitRows = numRows;
        return false;
    }
}

bool CThorSpillableRowArray::shrink()
{
    // NB: Should only be called from writer thread
    CThorArrayLockBlock block(*this);
    _flush(true);
    rowidx_t prevMaxRows = maxRows;
    shrink(numRows);
    return maxRows != prevMaxRows;
}

bool CThorSpillableRowArray::flush()
{
    CThorArrayLockBlock block(*this);
    return _flush(false);
}

bool CThorSpillableRowArray::appendRows(CThorExpandingRowArray &inRows, bool takeOwnership)
{
    rowidx_t num = inRows.ordinality();
    if (0 == num)
        return true;
    if (numRows+num >= maxRows)
    {
        if (!resize(numRows + num))
        {
            flush();
            if (numRows+num >= maxRows)
                return false;
        }
    }
    const void **newRows = rows+numRows;
    inRows.transferRowsCopy(newRows, takeOwnership);

    numRows += num;
    if (numRows >= commitRows + commitDelta)
        flush();
    return true;
}

void CThorSpillableRowArray::transferFrom(CThorExpandingRowArray &src)
{
    CThorArrayLockBlock block(*this);
    CThorExpandingRowArray::transferFrom(src);
    commitRows = numRows;
}

void CThorSpillableRowArray::transferFrom(CThorSpillableRowArray &src)
{
    CThorArrayLockBlock block(*this);
    CThorExpandingRowArray::transferFrom(src);
    commitRows = numRows;
}

void CThorSpillableRowArray::swap(CThorSpillableRowArray &other)
{
    CThorArrayLockBlock block(*this);
    CThorExpandingRowArray::swap(other);
    rowidx_t otherFirstRow = other.firstRow;
    rowidx_t otherCommitRows = other.commitRows;

    other.firstRow = firstRow;
    other.commitRows = commitRows;

    firstRow = otherFirstRow;
    commitRows = otherCommitRows;
}

void CThorSpillableRowArray::readBlock(const void **outRows, rowidx_t readRows)
{
    CThorArrayLockBlock block(*this);
    dbgassertex(firstRow + readRows <= commitRows);
    memcpy(outRows, rows + firstRow, readRows*sizeof(void *));
    firstRow += readRows;
}

bool CThorSpillableRowArray::shrink(rowidx_t requiredRows)
{
    dbgassertex(requiredRows <= numRows);
    return CThorExpandingRowArray::resize(requiredRows);
}

bool CThorSpillableRowArray::resize(rowidx_t requiredRows, unsigned maxSpillCost)
{
    if (needToMoveRows(false))
    {
        CThorArrayLockBlock block(*this);
        doFlush();
    }
    return CThorExpandingRowArray::resize(requiredRows, maxSpillCost);
}

void CThorSpillableRowArray::transferRowsCopy(const void **outRows, bool takeOwnership)
{
    CThorArrayLockBlock block(*this);
    if (0 == numRows)
        return;
    assertex(numRows == commitRows);
    memcpy(outRows, rows, numRows*sizeof(void *));
    if (takeOwnership)
        firstRow = commitRows = numRows = 0;
    else
    {
        const void **lastNewRow = outRows+numRows-1;
        for (;;)
        {
            LinkThorRow(*outRows);
            if (outRows == lastNewRow)
                break;
            outRows++;
        }
    }
}

IRowStream *CThorSpillableRowArray::createRowStream(unsigned spillPriority, unsigned spillCompInfo)
{
    assertex(rowIf);
    return new CSpillableStream(activity, *this, rowIf, emptyRowSemantics, spillPriority, spillCompInfo);
}



class CThorRowCollectorBase : public CSpillable
{
protected:
    CThorSpillableRowArray spillableRows;
    IPointerArrayOf<CFileOwner> spillFiles;
    Owned<IOutputRowSerializer> serializer;
    RowCollectorSpillFlags diskMemMix;
    rowcount_t totalRows;
    unsigned overflowCount;
    unsigned maxCores;
    unsigned outStreams;
    offset_t sizeSpill;
    ICompare *iCompare;
    StableSortFlag stableSort;
    EmptyRowSemantics emptyRowSemantics = ers_forbidden;
    CriticalSection readerLock;
    Owned<CSharedSpillableRowSet> spillableRowSet;
    unsigned options;
    unsigned spillCompInfo = 0;
    __uint64 spillCycles;
    __uint64 sortCycles;

    bool spillRows(bool critical)
    {
        //This must only be called while a lock is held on spillableRows
        rowidx_t numRows = spillableRows.numCommitted();
        if (numRows == 0)
            return false; // cannot shrink(), as requires a flush and only writer thread can do that.

        CCycleTimer spillTimer;
        totalRows += numRows;
        StringBuffer tempPrefix, tempName;
        if (iCompare)
        {
            ActPrintLog(&activity, "Sorting %" RIPF "d rows", spillableRows.numCommitted());
            CCycleTimer timer;
            spillableRows.sort(*iCompare, maxCores); // sorts committed rows
            sortCycles += timer.elapsedCycles();
            ActPrintLog(&activity, "Sort took: %f", ((float)timer.elapsedMs())/1000);
            tempPrefix.append("srt");
        }
        tempPrefix.appendf("spill_%d", activity.queryId());
        GetTempName(tempName, tempPrefix.str(), true);
        Owned<IFile> iFile = createIFile(tempName.str());
        VStringBuffer spillPrefixStr("RowCollector(%d)", spillPriority);
        spillableRows.save(*iFile, spillCompInfo, false, spillPrefixStr.str()); // saves committed rows
        spillFiles.append(new CFileOwner(iFile.getLink()));
        ++overflowCount;
        sizeSpill += iFile->size();
        spillCycles += spillTimer.elapsedCycles();
        return true;
    }
    void setEmptyRowSemantics(EmptyRowSemantics _emptyRowSemantics)
    {
        emptyRowSemantics = _emptyRowSemantics;
        spillableRows.setEmptyRowSemantics(emptyRowSemantics);
    }
    bool flush()
    {
        return spillableRows.flush();
    }
    bool shrink(StringBuffer *traceInfo)
    {
        rowidx_t maxRows = spillableRows.queryMaxRows();
        bool ret = spillableRows.shrink();
        if (traceInfo)
            traceInfo->append("shrink() - previous maxRows=").append(maxRows).append(", new maxRows=").append(spillableRows.queryMaxRows());
        return ret;
    }
    void putRow(const void *row)
    {
        if (!spillableRows.append(row))
        {
            bool oom = false;
            if (spillingEnabled())
            {
                CThorArrayLockBlock block(spillableRows);
                //We should have been called back to free any committed rows, but occasionally it may not (e.g., if
                //the problem is global memory is exhausted) - in which case force a spill here (but add any pending
                //rows first).
                if (spillableRows.numCommitted() != 0)
                {
                    flush();
                    spillRows(false);
                }
                // This is a good time to shrink the row table back. shrink() force a flush.
                StringBuffer info;
                if (shrink(&info))
                    activity.ActPrintLog("CThorRowCollectorBase: shrink - %s", info.str());

                if (!spillableRows.append(row))
                    oom = true;
            }
            else
                oom = true;
            if (oom)
            {
                ReleaseThorRow(row);
                throw MakeActivityException(&activity, ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Insufficient memory to append sort row");
            }
        }
    }
    IRowStream *getStream(CThorExpandingRowArray *allMemRows, memsize_t *memUsage, bool shared)
    {
        CriticalBlock b(readerLock);
        if (0 == outStreams)
        {
            flush();
            if (spillingEnabled())
            {
                // i.e. all disk OR (some on disk already AND allDiskOrAllMem)
                if (((rc_allDisk == diskMemMix) || ((rc_allDiskOrAllMem == diskMemMix) && overflowCount)))
                {
                    CThorArrayLockBlock block(spillableRows);
                    if (spillableRows.numCommitted())
                    {
                        spillRows(false);
                        spillableRows.kill();
                    }
                }
            }
        }
        ++outStreams;

        /* Ensure existing callback is cleared, before:
         * a) instreams are built, since new spillFiles can be added to as long as existing callback is active
         * b) locked CThorSpillableRowArrayLock section below, which in turn may add a new callback.
         *    Otherwise, once this section has the lock, the existing callback may be called by roxiemem and block,
         *    causing this section to deadlock inside roxiemem, if it tries to add a new callback.
         */
        deactivateSpillingCallback();

        // NB: CStreamFileOwner links CFileOwner - last usage will auto delete file
        // which may be one of these streams or CThorRowCollectorBase itself
        unsigned rwFlags = DEFAULT_RWFLAGS;
        if (spillCompInfo)
        {
            rwFlags |= rw_compress;
            rwFlags |= spillCompInfo;
        }
        rwFlags |= mapESRToRWFlags(emptyRowSemantics);
        IArrayOf<IRowStream> instrms;
        ForEachItemIn(f, spillFiles)
        {
            CFileOwner *fileOwner = spillFiles.item(f);
            Owned<IExtRowStream> strm = createRowStream(&fileOwner->queryIFile(), rowIf, rwFlags);
            instrms.append(* new CStreamFileOwner(fileOwner, strm));
        }

        {
            if (spillableRowSet)
                instrms.append(*spillableRowSet->createRowStream());
            else if (spillableRows.numCommitted())
            {
                totalRows += spillableRows.numCommitted();
                if (iCompare && (1 == outStreams))
                {
                    // Option(rcflag_noAllInMemSort) - avoid sorting allMemRows
                    if ((NULL == allMemRows) || (0 == (options & rcflag_noAllInMemSort)))
                    {
                        CCycleTimer timer;
                        spillableRows.sort(*iCompare, maxCores);
                        sortCycles += timer.elapsedCycles();
                    }
                }

                if ((rc_allDiskOrAllMem == diskMemMix) || // must supply allMemRows, only here if no spilling (see above)
                    (NULL!=allMemRows && (rc_allMem == diskMemMix)) ||
                    (NULL!=allMemRows && (rc_mixed == diskMemMix) && 0 == overflowCount) // if allMemRows given, only if no spilling
                   )
                {
                    assertex(allMemRows);
                    assertex(1 == outStreams);
                    if (memUsage)
                        *memUsage = spillableRows.getMemUsage(); // a bit expensive if variable rows
                    allMemRows->transferFrom(spillableRows);
                    // stream cannot be used
                    return NULL;
                }
                if (!shared)
                    instrms.append(*spillableRows.createRowStream(spillPriority, spillCompInfo)); // NB: stream will take ownership of rows in spillableRows
                else
                {
                    spillableRowSet.setown(new CSharedSpillableRowSet(activity, spillableRows, rowIf, emptyRowSemantics, spillPriority));
                    instrms.append(*spillableRowSet->createRowStream());
                }
            }
            else
            {
                // If 0 rows, no overflow, don't return stream, except for rc_allDisk which will never fill allMemRows
                if (allMemRows && (0 == overflowCount) && (diskMemMix != rc_allDisk))
                    return NULL;
            }
        }
        if (0 == instrms.ordinality())
            return createNullRowStream();
        else if (1 == instrms.ordinality())
            return LINK(&instrms.item(0));
        else if (iCompare)
        {
            Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
            return createRowStreamMerger(instrms.ordinality(), instrms.getArray(), iCompare, false, linkcounter);
        }
        else
            return createConcatRowStream(instrms.ordinality(),instrms.getArray());
    }
    void reset()
    {
        spillableRows.kill();
        spillFiles.kill();
        totalRows = 0;
        overflowCount = outStreams = 0;
        sizeSpill = 0;
    }
public:
    CThorRowCollectorBase(CActivityBase &_activity, IThorRowInterfaces *_rowIf, ICompare *_iCompare, StableSortFlag _stableSort, RowCollectorSpillFlags _diskMemMix, unsigned _spillPriority)
        : CSpillable(_activity, _rowIf, _spillPriority),
          iCompare(_iCompare), stableSort(_stableSort), diskMemMix(_diskMemMix),
          spillableRows(_activity)
    {
        totalRows = 0;
        overflowCount = outStreams = 0;
        sizeSpill = 0;
        if (rc_allMem == diskMemMix)
            spillPriority = SPILL_PRIORITY_DISABLE; // all mem, implies no spilling
        else
            activateSpillingCallback();
        maxCores = activity.queryMaxCores();
        options = 0;
        spillableRows.setup(rowIf, ers_forbidden, stableSort);
        if (activity.getOptBool(THOROPT_COMPRESS_SPILLS, true))
        {
            StringBuffer compType;
            activity.getOpt(THOROPT_COMPRESS_SPILL_TYPE, compType);
            setCompFlag(compType, spillCompInfo);
        }
        spillCycles = 0;
        sortCycles = 0;
        if (iCompare)
        {
            /* NB: See HPCC-17231 for details
             *
             * If creating spill files that will be merged, e.g. for sorted stream,
             * reduce default compressed file block size.
             * If the block size is large, the expanded block size can be many times larger and
             * if there are a lot of spill files, the merge opens them all and causes excessive
             * memory usage.
             */
            size32_t compBlkSz = activity.getOptUInt(THOROPT_SORT_COMPBLKSZ, DEFAULT_SORT_COMPBLKSZ);
            activity.ActPrintLog("Spilling will use compressed block size = %u", compBlkSz);
            spillableRows.setCompBlockSize(compBlkSz);
        }
    }
    ~CThorRowCollectorBase()
    {
        reset();
        ensureSpillingCallbackRemoved();
    }
    void transferRowsOut(CThorExpandingRowArray &out, bool sort)
    {
        CThorArrayLockBlock block(spillableRows);
        flush();
        totalRows += spillableRows.numCommitted();
        if (sort && iCompare)
        {
            CCycleTimer timer;
            spillableRows.sort(*iCompare, maxCores);
            sortCycles += timer.elapsedCycles();
        }
        out.transferFrom(spillableRows);
    }
// IThorRowCollectorCommon
    virtual rowcount_t numRows() const
    {
        return totalRows+spillableRows.numCommitted();
    }
    virtual unsigned numOverflows() const
    {
        return overflowCount;
    }
    virtual unsigned overflowScale() const
    {
        // 1 if no spill
        if (!overflowCount)
            return 1;
        return overflowCount*2+3; // bit arbitrary
    }
    virtual void transferRowsIn(CThorExpandingRowArray &src)
    {
        reset();
        spillableRows.transferFrom(src);
        activateSpillingCallback();
    }
    virtual void transferRowsIn(CThorSpillableRowArray &src)
    {
        reset();
        spillableRows.transferFrom(src);
        activateSpillingCallback();
    }
    virtual const void *probeRow(unsigned r)
    {
        if (r>=spillableRows.numCommitted())
            return NULL;
        return spillableRows.query(r);
    }
    virtual void setup(ICompare *_iCompare, StableSortFlag _stableSort, RowCollectorSpillFlags _diskMemMix, unsigned _spillPriority)
    {
        iCompare = _iCompare;
        stableSort = _stableSort;
        diskMemMix = _diskMemMix;
        spillPriority = _spillPriority;
        if (rc_allMem == diskMemMix)
            spillPriority = SPILL_PRIORITY_DISABLE; // all mem, implies no spilling
        if (mmRegistered && !spillingEnabled())
        {
            mmRegistered = false;
            activity.queryRowManager()->removeRowBuffer(this);
        }
        spillableRows.setup(rowIf, ers_forbidden, stableSort);
    }
    virtual void resize(rowidx_t max)
    {
        spillableRows.resize(max);
    }
    virtual void setOptions(unsigned _options)
    {
        options = _options;
    }
    virtual bool hasSpilt() const { return overflowCount >= 1; }

// IThorArrayLock
    virtual void lock() const { spillableRows.lock(); }
    virtual void unlock() const { spillableRows.unlock(); }

// IBufferedRowCallback
    virtual bool freeBufferedRows(bool critical)
    {
        if (!mmActivated || !spillingEnabled())
            return false;
        CThorArrayLockBlock block(spillableRows);
        return spillRows(critical);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        switch (kind)
        {
        case StCycleSpillElapsedCycles:
            return spillCycles;
        case StCycleSortElapsedCycles:
            return sortCycles;
        case StTimeSpillElapsed:
            return cycle_to_nanosec(spillCycles);
        case StTimeSortElapsed:
            return cycle_to_nanosec(sortCycles);
        case StNumSpills:
            return overflowCount;
        case StSizeSpillFile:
            return sizeSpill;
        }
        return 0;
    }
};

enum TRLGroupFlag { trl_ungroup, trl_preserveGrouping, trl_stopAtEog };
class CThorRowLoader : public CThorRowCollectorBase, implements IThorRowLoader
{
    IRowStream *load(IRowStream *in, const bool &abort, TRLGroupFlag grouping, CThorExpandingRowArray *allMemRows, memsize_t *memUsage, bool doReset)
    {
        if (doReset)
            reset();
        activateSpillingCallback();
        setEmptyRowSemantics(trl_preserveGrouping == grouping ? ers_eogonly : ers_forbidden);
        while (!abort)
        {
            const void *next = in->nextRow();
            if (!next)
            {
                if (grouping == trl_stopAtEog)
                    break;
                else
                {
                    next = in->nextRow();
                    if (!next)
                        break;
                    if (grouping == trl_preserveGrouping)
                        putRow(NULL);
                }
            }
            putRow(next);
        }
        return getStream(allMemRows, memUsage, false);
    }

public:
    IMPLEMENT_IINTERFACE_USING(CThorRowCollectorBase);

    CThorRowLoader(CActivityBase &activity, IThorRowInterfaces *rowIf, ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix, unsigned spillPriority)
        : CThorRowCollectorBase(activity, rowIf, iCompare, stableSort, diskMemMix, spillPriority)
    {
    }
// IThorRowCollectorCommon
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorExpandingRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
    virtual void transferRowsIn(CThorSpillableRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
    virtual const void *probeRow(unsigned r) { return CThorRowCollectorBase::probeRow(r); }
    virtual void setup(ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=50)
    {
        CThorRowCollectorBase::setup(iCompare, stableSort, diskMemMix, spillPriority);
    }
    virtual void resize(rowidx_t max) { CThorRowCollectorBase::resize(max); }
    virtual void setOptions(unsigned options)  { CThorRowCollectorBase::setOptions(options); }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return CThorRowCollectorBase::getStatistic(kind); }
    virtual bool hasSpilt() const { return CThorRowCollectorBase::hasSpilt(); }

// IThorArrayLock
    virtual void lock() const { CThorRowCollectorBase::lock(); }
    virtual void unlock() const { CThorRowCollectorBase::unlock(); }
// IThorRowLoader
    virtual IRowStream *load(IRowStream *in, const bool &abort, bool preserveGrouping, CThorExpandingRowArray *allMemRows, memsize_t *memUsage, bool doReset)
    {
        assertex(!iCompare || !preserveGrouping); // can't sort if group preserving
        return load(in, abort, preserveGrouping?trl_preserveGrouping:trl_ungroup, allMemRows, memUsage, doReset);
    }
    virtual IRowStream *loadGroup(IRowStream *in, const bool &abort, CThorExpandingRowArray *allMemRows, memsize_t *memUsage, bool doReset)
    {
        return load(in, abort, trl_stopAtEog, allMemRows, memUsage, doReset);
    }
};

IThorRowLoader *createThorRowLoader(CActivityBase &activity, IThorRowInterfaces *rowIf, ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix, unsigned spillPriority)
{
    return new CThorRowLoader(activity, rowIf, iCompare, stableSort, diskMemMix, spillPriority);
}

IThorRowLoader *createThorRowLoader(CActivityBase &activity, ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix, unsigned spillPriority)
{
    return createThorRowLoader(activity, &activity, iCompare, stableSort, diskMemMix, spillPriority);
}



class CThorRowCollector : public CThorRowCollectorBase, implements IThorRowCollector
{
public:
    IMPLEMENT_IINTERFACE_USING(CThorRowCollectorBase);

    CThorRowCollector(CActivityBase &activity, IThorRowInterfaces *rowIf, ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix, unsigned spillPriority)
        : CThorRowCollectorBase(activity, rowIf, iCompare, stableSort, diskMemMix, spillPriority)
    {
    }
// IThorRowCollectorCommon
    virtual void setEmptyRowSemantics(EmptyRowSemantics emptyGroupSemantics)
    {
        assertex(!iCompare || (ers_forbidden == emptyGroupSemantics)); // can't sort if preserving end of groups or nulls
        CThorRowCollectorBase::setEmptyRowSemantics(emptyGroupSemantics);
    }
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorExpandingRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
    virtual void transferRowsIn(CThorSpillableRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
    virtual const void *probeRow(unsigned r) { return CThorRowCollectorBase::probeRow(r); }
    virtual void setup(ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix=rc_mixed, unsigned spillPriority=50)
    {
        CThorRowCollectorBase::setup(iCompare, stableSort, diskMemMix, spillPriority);
    }
    virtual void resize(rowidx_t max) { CThorRowCollectorBase::resize(max); }
    virtual void setOptions(unsigned options) { CThorRowCollectorBase::setOptions(options); }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return CThorRowCollectorBase::getStatistic(kind); }
    virtual bool hasSpilt() const { return CThorRowCollectorBase::hasSpilt(); }
// IThorArrayLock
    virtual void lock() const { CThorRowCollectorBase::lock(); }
    virtual void unlock() const { CThorRowCollectorBase::unlock(); }
// IThorRowCollector
    virtual IRowWriter *getWriter()
    {
        class CWriter : public CSimpleInterface, implements IRowWriter
        {
            Linked<CThorRowCollector> parent;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWriter(CThorRowCollector *_parent) : parent(_parent)
            {
            }
            ~CWriter()
            {
                flush();
            }
        // IRowWriter
            virtual void putRow(const void *row)
            {
                parent->putRow(row);
            }
            virtual void flush()
            {
                parent->flush();
            }
        };
        return new CWriter(this);
    }
    virtual void reset()
    {
        CThorRowCollectorBase::reset();
    }
    virtual IRowStream *getStream(bool shared, CThorExpandingRowArray *allMemRows)
    {
        return CThorRowCollectorBase::getStream(allMemRows, NULL, shared);
    }
    virtual bool spill(bool critical)
    {
        CThorArrayLockBlock block(spillableRows);
        return spillRows(critical);
    }
    virtual bool flush() { return CThorRowCollectorBase::flush(); }
    virtual bool shrink(StringBuffer *traceInfo) { return CThorRowCollectorBase::shrink(traceInfo); }
};

IThorRowCollector *createThorRowCollector(CActivityBase &activity, IThorRowInterfaces *rowIf, ICompare *iCompare, StableSortFlag stableSort, RowCollectorSpillFlags diskMemMix, unsigned spillPriority, EmptyRowSemantics emptyRowSemantics)
{
    Owned<IThorRowCollector> collector = new CThorRowCollector(activity, rowIf, iCompare, stableSort, diskMemMix, spillPriority);
    collector->setEmptyRowSemantics(emptyRowSemantics);
    return collector.getClear();
}

void setThorInABox(unsigned num)
{
}


class cMultiThorResourceMutex: public CSimpleInterface, implements ILargeMemLimitNotify, implements IDaliMutexNotifyWaiting
{
    class cMultiThorResourceMutexThread: public Thread
    {
        cMultiThorResourceMutex &parent;
    public:
        cMultiThorResourceMutexThread(cMultiThorResourceMutex &_parent)
            : Thread("cMultiThorResourceMutexThread"),parent(_parent)
        {
        }

        int run() 
        {
            parent.run();
            return 0;
        }
    };
    Owned<cMultiThorResourceMutexThread> thread;
    Owned<IDaliMutex> mutex;
    bool stopping;
    Linked<ICommunicator> nodeComm;
    CSDSServerStatus *status;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    cMultiThorResourceMutex(const char *groupname,CSDSServerStatus *_status)
    {
        status = _status;
        stopping = false;
        nodeComm.set(&queryNodeComm());
        if (nodeComm->queryGroup().rank(queryMyNode())==0) { // master so start thread
            thread.setown(new cMultiThorResourceMutexThread(*this));
            thread->start();
            StringBuffer mname("thorres:");
            mname.append(groupname);
            mutex.setown(createDaliMutex(mname.str()));
        }

    }

    ~cMultiThorResourceMutex()
    {
        stopping = true;
        if (thread) 
            stop();
    }

    void run() // on master
    {
        PROGLOG("cMultiThorResourceMutex thread run");
        try {
            CMessageBuffer mbuf;
            while (!stopping) {
                mbuf.clear();
                rank_t from;
                unsigned timeout = 1000*60*5;
                if (nodeComm->recv(mbuf,RANK_ALL,MPTAG_THORRESOURCELOCK,&from,timeout)) {
                    byte req;
                    mbuf.read(req);
                    if (req==1) {
                        if (mutex) 
                            mutex->enter();
                    }
                    else if (req==0) {
                        if (mutex) 
                            mutex->leave();
                    }
                    nodeComm->reply(mbuf,1000*60*5);
                }
            }
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::run");
        }
    }

    void stop()
    {
        PROGLOG("cMultiThorResourceMutex::stop enter");
        stopping = true;
        if (mutex) 
            mutex->kill();
        try {
            nodeComm->cancel(RANK_ALL,MPTAG_THORRESOURCELOCK);
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::stop");
        }
        if (thread)
            thread->join();
        mutex.clear();
        PROGLOG("cMultiThorResourceMutex::stop leave");
    }

    bool take(memsize_t tot)
    {
        if (stopping)
            return true;
        if (mutex) 
            return mutex->enter();
        if (stopping)
            return false;
        CMessageBuffer mbuf;
        byte req = 1;
        mbuf.append(req);
        try {
            if (!nodeComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::take");
        }
        return !stopping;
    }
                                            // will raise oom exception if false returned
    void give(memsize_t tot)
    {
        if (mutex) {
            mutex->leave();
            return;
        }
        if (stopping)
            return;
        CMessageBuffer mbuf;
        byte req = 0;
        mbuf.append(req);
        try {
            if (!nodeComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
                stopping = true;
        }
        catch (IException *e) {
            EXCLOG(e,"cMultiThorResourceMutex::give");
        }

    }

    //IDaliMutexNotifyWaiting
    void startWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",1);
    }
    void cycleWait()
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",status->queryProperties()->getPropInt("@memoryBlocked")+1);
    }
    void stopWait(bool got)
    {
        if (status)
            status->queryProperties()->setPropInt("@memoryBlocked",0);
    }

};


ILargeMemLimitNotify *createMultiThorResourceMutex(const char *grpname,CSDSServerStatus *_status)
{
    return new cMultiThorResourceMutex(grpname,_status);
}


IThorRowInterfaces *createThorRowInterfaces(roxiemem::IRowManager *rowManager, IOutputMetaData *meta, unsigned actId, unsigned heapFlags, ICodeContext *context)
{
    class CThorRowInterfaces : public CSimpleInterfaceOf<IThorRowInterfaces>
    {
        roxiemem::IRowManager *rowManager;
        Owned<IRowInterfaces> baseRowIf;
    public:
        CThorRowInterfaces(roxiemem::IRowManager *_rowManager, IOutputMetaData *meta, unsigned actId, unsigned heapFlags, ICodeContext *context)
            : rowManager(_rowManager)
        {
            baseRowIf.setown(createRowInterfaces(meta, actId, heapFlags, context));
        }
        virtual IEngineRowAllocator * queryRowAllocator() { return baseRowIf->queryRowAllocator(); }
        virtual IOutputRowSerializer * queryRowSerializer() { return baseRowIf->queryRowSerializer(); }
        virtual IOutputRowDeserializer * queryRowDeserializer() { return baseRowIf->queryRowDeserializer(); }
        virtual IOutputMetaData *queryRowMetaData() { return baseRowIf->queryRowMetaData(); }
        virtual unsigned queryActivityId() const { return baseRowIf->queryActivityId(); }
        virtual ICodeContext *queryCodeContext() { return baseRowIf->queryCodeContext(); }
        virtual roxiemem::IRowManager *queryRowManager() const { return rowManager; }
    };
    return new CThorRowInterfaces(rowManager, meta, actId, heapFlags, context);
};


class CThorAllocator : public CSimpleInterface, implements IThorAllocator, implements IRowAllocatorMetaActIdCacheCallback
{
protected:
    mutable Owned<IRowAllocatorMetaActIdCache> allocatorMetaCache;
    IArrayOf<IRowAllocatorMetaActIdCache> slaveAllocatorMetaCaches;
    Owned<roxiemem::IRowManager> rowManager;
    roxiemem::RoxieHeapFlags defaultFlags;
    IContextLogger *logctx;
    unsigned numChannels;
    IArrayOf<CThorAllocator> slaveAllocators;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorAllocator(unsigned memLimitMB, unsigned sharedMemLimitMB, unsigned _numChannels, unsigned memorySpillAtPercentage, IContextLogger &_logctx, bool crcChecking, roxiemem::RoxieHeapFlags _defaultFlags);
    CThorAllocator(roxiemem::RoxieHeapFlags _defaultFlags, IContextLogger *_logCtx) : defaultFlags(_defaultFlags), logctx(_logCtx)
    {
        allocatorMetaCache.setown(createRowAllocatorCache(this));
        numChannels = 0;
    }
    ~CThorAllocator()
    {
        rowManager.clear();
        allocatorMetaCache.clear();
    }
    void setRowManager(roxiemem::IRowManager *_rowManager)
    {
        rowManager.set(_rowManager);
    }
    IRowAllocatorMetaActIdCache *getAllocatorCache()
    {
        return allocatorMetaCache.getLink();
    }
// roxiemem::IRowAllocatorMetaActIdCacheCallback
    virtual IEngineRowAllocator *createAllocator(IRowAllocatorMetaActIdCache * cache, IOutputMetaData *meta, unsigned activityId, unsigned id, roxiemem::RoxieHeapFlags flags) const
    {
        return createRoxieRowAllocator(cache, *rowManager, meta, activityId, id, flags);
    }
// IThorAllocator
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, activity_id activityId, roxiemem::RoxieHeapFlags flags) const
    {
        return allocatorMetaCache->ensure(meta, activityId, flags);
    }
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, activity_id activityId) const
    {
        return allocatorMetaCache->ensure(meta, activityId, defaultFlags);
    }
    virtual roxiemem::IRowManager *queryRowManager() const
    {
        return rowManager;
    }
    virtual roxiemem::RoxieHeapFlags queryFlags() const { return defaultFlags; }
    virtual bool queryCrc() const { return false; }
    virtual IThorAllocator *getSlaveAllocator(unsigned channel)
    {
        assertex(numChannels>1);
        return LINK(&slaveAllocators.item(channel));
    }
};

// derived to avoid a 'crcChecking' check per getRowAllocator only
class CThorCrcCheckingAllocator : public CThorAllocator
{
public:
    CThorCrcCheckingAllocator(unsigned memLimitMB, unsigned sharedMemLimitMB, unsigned numChannels, unsigned memorySpillAtPercentage, IContextLogger &logctx, roxiemem::RoxieHeapFlags flags)
        : CThorAllocator(memLimitMB, sharedMemLimitMB, numChannels, memorySpillAtPercentage, logctx, true, flags)
    {
    }
    CThorCrcCheckingAllocator(roxiemem::RoxieHeapFlags defaultFlags, IContextLogger *logCtx)
        : CThorAllocator(defaultFlags, logCtx)
    {
    }
// IThorAllocator
    virtual bool queryCrc() const { return true; }
    virtual IThorAllocator *getSlaveAllocator(unsigned channel)
    {
        assertex(numChannels>1);
        return new CThorCrcCheckingAllocator(defaultFlags, logctx);
    }
// roxiemem::IRowAllocatorMetaActIdCacheCallback
    virtual IEngineRowAllocator *createAllocator(IRowAllocatorMetaActIdCache * cache, IOutputMetaData *meta, unsigned activityId, unsigned cacheId, roxiemem::RoxieHeapFlags flags) const
    {
        return createCrcRoxieRowAllocator(cache, *rowManager, meta, activityId, cacheId, flags);
    }
};

CThorAllocator::CThorAllocator(unsigned memLimitMB, unsigned sharedMemLimitMB, unsigned _numChannels, unsigned memorySpillAtPercentage, IContextLogger &_logctx, bool crcChecking, roxiemem::RoxieHeapFlags _defaultFlags)
    : numChannels(_numChannels), logctx(&_logctx), defaultFlags(_defaultFlags)
{
    memsize_t memLimit = ((memsize_t)memLimitMB)*0x100000;
    memsize_t sharedMemLimit = ((memsize_t)sharedMemLimitMB)*0x100000;
    allocatorMetaCache.setown(createRowAllocatorCache(this));
    if (numChannels>1)
    {
        for (unsigned c=0; c<numChannels; c++)
        {
            CThorAllocator *slaveAllocator;
            if (crcChecking)
                slaveAllocator = new CThorCrcCheckingAllocator(defaultFlags, logctx);
            else
                slaveAllocator = new CThorAllocator(defaultFlags, logctx);
            slaveAllocators.append(*slaveAllocator);
            slaveAllocatorMetaCaches.append(*slaveAllocator->getAllocatorCache());
        }
        rowManager.setown(roxiemem::createGlobalRowManager(memLimit, sharedMemLimit, numChannels, NULL, *logctx, allocatorMetaCache, (const roxiemem::IRowAllocatorCache **)slaveAllocatorMetaCaches.getArray(), false, true));
        for (unsigned c=0; c<numChannels; c++)
            slaveAllocators.item(c).setRowManager(rowManager->querySlaveRowManager(c));
    }
    else
        rowManager.setown(roxiemem::createRowManager(memLimit, NULL, *logctx, allocatorMetaCache, false, true));

    rowManager->setMemoryLimit(memLimit, 0==memorySpillAtPercentage ? 0 : memLimit/100*memorySpillAtPercentage);
    const bool paranoid = false;
    if (paranoid)
    {
        //you probably want to test these options individually
        rowManager->setMemoryCallbackThreshold((unsigned)-1);
        rowManager->setCallbackOnThread(true);
        rowManager->setMinimizeFootprint(true, true);
        rowManager->setReleaseWhenModifyCallback(true, true);
    }
}


IThorAllocator *createThorAllocator(unsigned memLimitMB, unsigned sharedMemLimitMB, unsigned numChannels, unsigned memorySpillAtPercentage, IContextLogger &logctx, bool crcChecking, bool usePacked)
{
    PROGLOG("Thor allocator: Size=%d (MB), sharedLimit=%d (MB), CRC=%s, Packed=%s", memLimitMB, sharedMemLimitMB, crcChecking?"ON":"OFF", usePacked?"ON":"OFF");
    roxiemem::RoxieHeapFlags flags;
    if (usePacked)
        flags = roxiemem::RHFpacked;
    else
        flags = roxiemem::RHFnone;
    dbgassertex(numChannels);
    dbgassertex((1==numChannels) || sharedMemLimitMB);
    if (crcChecking)
        return new CThorCrcCheckingAllocator(memLimitMB, sharedMemLimitMB, numChannels, memorySpillAtPercentage, logctx, flags);
    else
        return new CThorAllocator(memLimitMB, sharedMemLimitMB, numChannels, memorySpillAtPercentage, logctx, false, flags);
}


#define OUTPUTMETACHILDROW_VERSION 2 // for now, it's only significant that non-zero
class COutputMetaWithChildRow : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IEngineRowAllocator> childAllocator;
    IOutputMetaData *childMeta;
    size32_t extraSz;
    Owned<IOutputRowSerializer> diskSerializer;
    Owned<IOutputRowDeserializer> diskDeserializer;
    Owned<IOutputRowSerializer> internalSerializer;
    Owned<IOutputRowDeserializer> internalDeserializer;
    Owned<ISourceRowPrefetcher> prefetcher;

    class CSerializer : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> childSerializer;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerializer(IOutputRowSerializer *_childSerializer, size32_t _extraSz) : childSerializer(_childSerializer), extraSz(_extraSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(extraSz, self);
            const byte *childRow = *(const byte **)(self+extraSz);
            if (childRow)
            {
                byte b=1;
                out.put(1, &b);
                childSerializer->serialize(out, childRow);
            }
            else
            {
                byte b=0;
                out.put(1, &b);
            }
        }
    };
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> childDeserializer;
        Linked<IEngineRowAllocator> childAllocator;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_childDeserializer, IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childDeserializer(_childDeserializer), childAllocator(_childAllocator), extraSz(_extraSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            byte * self = rowBuilder.getSelf();
            in.read(extraSz, self);
            byte b;
            in.read(1, &b);
            const void *fChildRow;
            if (b)
            {
                RtlDynamicRowBuilder childBuilder(childAllocator);
                size32_t sz = childDeserializer->deserialize(childBuilder, in);
                fChildRow = childBuilder.finalizeRowClear(sz);
            }
            else
                fChildRow = NULL;
            memcpy(self+extraSz, &fChildRow, sizeof(const void *));
            return extraSz + sizeof(const void *);
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t extraSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _extraSz) : childPrefetcher(_childPrefetcher), extraSz(_extraSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(extraSz);
            byte b;
            in.read(1, &b);
            if (b)
                childPrefetcher->readAhead(in);
        }
    };


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithChildRow(IEngineRowAllocator *_childAllocator, size32_t _extraSz) : childAllocator(_childAllocator), extraSz(_extraSz)
    {
        childMeta = childAllocator->queryOutputMeta();
    }
    virtual size32_t getRecordSize(const void *) { return extraSz + sizeof(const void *); }
    virtual size32_t getMinRecordSize() const { return extraSz + sizeof(const void *); }
    virtual size32_t getFixedSize() const { return extraSz + sizeof(const void *); }
    virtual void toXML(const byte * self, IXmlWriter & out) 
    { 
         // ignoring xml'ing extra
        //GH: I think this is what it should do
        childMeta->toXML(*(const byte **)(self+extraSz), out); 
    }
    virtual unsigned getVersion() const { return OUTPUTMETACHILDROW_VERSION; }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return MDFneeddestruct|childMeta->getMetaFlags(); }
    virtual void destruct(byte * self)
    {
        OwnedConstThorRow childRow = *(const void **)(self+extraSz);
    }
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!diskSerializer)
            diskSerializer.setown(new CSerializer(childMeta->createDiskSerializer(ctx, activityId), extraSz));
        return LINK(diskSerializer);
    }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!diskDeserializer)
            diskDeserializer.setown(new CDeserializer(childMeta->createDiskDeserializer(ctx, activityId), childAllocator, extraSz));
        return LINK(diskDeserializer);
    }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(childMeta->createDiskPrefetcher(ctx, activityId), extraSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedDiskMeta() { return this; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!internalSerializer)
            internalSerializer.setown(new CSerializer(childMeta->createInternalSerializer(ctx, activityId), extraSz));
        return LINK(internalSerializer);
    }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!internalDeserializer)
            internalDeserializer.setown(new CDeserializer(childMeta->createInternalDeserializer(ctx, activityId), childAllocator, extraSz));
        return LINK(internalDeserializer);
    }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) 
    {
        //GH: I think this is what it should do, please check
        visitor.visitRow(*(const byte **)(self+extraSz)); 
    }
    virtual IOutputMetaData * queryChildMeta(unsigned i)
    {
        return childMeta->queryChildMeta(i);
    }
    virtual const RtlRecord &queryRecordAccessor(bool expand) const
    {
        throwUnexpected();  // used for internal structures only - no need to implement
    }
};

IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz)
{
    return new COutputMetaWithChildRow(childAllocator, extraSz);
}


