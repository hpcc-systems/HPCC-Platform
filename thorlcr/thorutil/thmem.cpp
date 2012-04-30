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

#include "jmisc.hpp"
#include "jio.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jvmem.hpp"
#include "jflz.hpp"

#include "thbufdef.hpp"
#include "thor.hpp"
#include "thormisc.hpp"
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
#undef RESIZEROW
#undef SHRINKROW
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


class CThorRowArrayException: public CSimpleInterface, public IThorRowArrayException
{
    size32_t sz;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CThorRowArrayException(size32_t _sz) 
    {
//      DebugBreak();
        sz = _sz;
    };
    
    int             errorCode() const { return 101; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        return str.append("CThorRowArray: Group too large: ").append(sz).append(" bytes");
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

IThorRowArrayException *createRowArrayException(size32_t sz)
{
    return new CThorRowArrayException(sz);
}


void checkMultiThorMemoryThreshold(bool inc)
{
    if (MTthresholdnotify.get()) {
        CriticalBlock block(MTcritsect);
        memsize_t used = 0; // JCSMORE - might work via callback in new scheme
        if (MTlocked) {
            if (used<MTthreshold/2) {
                DBGLOG("Multi Thor threshold lock released: %"I64F"d",(offset_t)used);
                MTlocked = false;
                MTthresholdnotify->give(used);
            }
        }
        else if (used>MTthreshold) {
            DBGLOG("Multi Thor threshold  exceeded: %"I64F"d",(offset_t)used);
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


static memsize_t largeMemSize = 0;
memsize_t setLargeMemSize(unsigned limitMB)
{
    memsize_t prevLargeMemSize = largeMemSize;
    largeMemSize = 1024*1024*(memsize_t)limitMB;
    return prevLargeMemSize;
}

memsize_t queryLargeMemSize()
{
    if (0 == largeMemSize)
        throwUnexpected();
    return largeMemSize;
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

//====

class CSpillableStreamBase : public CSimpleInterface, implements roxiemem::IBufferedRowCallback
{
protected:
    CActivityBase &activity;
    rowcount_t pos;
    IRowInterfaces *rowIf;
    bool preserveNulls, ownsRows;
    CThorSpillableRowArray rows;
    OwnedIFile spillFile;
    Owned<IRowStream> spillStream;

    bool spillRows()
    {
        rowcount_t numRows = rows.numCommitted();
        if (0 == numRows)
            return false;

        StringBuffer tempname;
        GetTempName(tempname,"streamspill", true);
        spillFile.setown(createIFile(tempname.str()));

        rows.save(*spillFile); // saves committed rows
        rows.noteSpilled(numRows);
        return true;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSpillableStreamBase(CActivityBase &_activity, CThorSpillableRowArray &inRows, IRowInterfaces *_rowIf, bool _preserveNulls)
        : activity(_activity), rowIf(_rowIf), rows(_activity, _rowIf, _preserveNulls), preserveNulls(_preserveNulls)
    {
        rows.swap(inRows);
        pos = 0;

        activity.queryJob().queryRowManager()->addRowBuffer(this);
    }
    ~CSpillableStreamBase()
    {
        activity.queryJob().queryRowManager()->removeRowBuffer(this);
        spillStream.clear();
        if (spillFile)
            spillFile->remove();
    }

// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return SPILL_PRIORITY_SPILLABLE_STREAM;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        CThorSpillableRowArray::CThorSpillableRowArrayLock block(rows);
        return spillRows();
    }
};

// NB: Shared/spillable, hols all rows in mem until needs to spill.
// spills all to disk, and stream continue reading from row in file
class CSharedSpillableRowSet : public CSpillableStreamBase, implements IInterface
{
    class CStream : public CSimpleInterface, implements IRowStream, implements IWritePosCallback
    {
        rowcount_t pos;
        offset_t outputOffset;
        Owned<IRowStream> spillStream;
        Linked<CSharedSpillableRowSet> owner;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStream(CSharedSpillableRowSet &_owner) : owner(&_owner)
        {
            pos = 0;
            outputOffset = (offset_t)-1;
            owner->rows.registerWriteCallback(*this);
        }
        ~CStream()
        {
            spillStream.clear(); // NB: clear stream 1st
            owner->rows.unregisterWriteCallback(*this);
            owner.clear();
        }
    // IRowStream
        virtual const void *nextRow()
        {
            if (spillStream)
                return spillStream->nextRow();
            CThorSpillableRowArray::CThorSpillableRowArrayLock block(owner->rows);
            if (pos == owner->rows.numCommitted())
                return NULL;
            else if (owner->spillFile) // i.e. has spilt
            {
                assertex(((offset_t)-1) != outputOffset);
                spillStream.setown(::createRowStream(owner->spillFile, owner->rowIf, outputOffset, (offset_t)-1, (unsigned __int64)-1, false, owner->preserveNulls));
                return spillStream->nextRow();
            }
            return owner->rows.get(pos++);
        }
        virtual void stop() { }
    // IWritePosCallback
        virtual rowcount_t queryRecordNumber()
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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSharedSpillableRowSet(CActivityBase &_activity, CThorSpillableRowArray &inRows, IRowInterfaces *_rowIf, bool _preserveNulls)
        : CSpillableStreamBase(_activity, inRows, _rowIf, _preserveNulls)
    {
    }
    IRowStream *createRowStream()
    {
        {
            // already spilled?
            CThorSpillableRowArray::CThorSpillableRowArrayLock block(rows);
            if (spillFile)
                return ::createRowStream(spillFile, rowIf, 0, (offset_t)-1, (unsigned __int64)-1, false, preserveNulls);
        }
        return new CStream(*this);
    }
};

// NB: A single unshared spillable stream
class CSpillableStream : public CSpillableStreamBase, implements IRowStream
{
    rowcount_t numReadRows, granularity;
    const void **readRows;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSpillableStream(CActivityBase &_activity, CThorSpillableRowArray &inRows, IRowInterfaces *_rowIf, bool _preserveNulls)
        : CSpillableStreamBase(_activity, inRows, _rowIf, _preserveNulls)
    {
        numReadRows = 0;
        granularity = 500; // JCSMORE - rows

        // a small amount of rows to read from swappable rows
        roxiemem::IRowManager *rowManager = activity.queryJob().queryRowManager();
        readRows = static_cast<const void * *>(rowManager->allocate(granularity * sizeof(void*), activity.queryContainer().queryId()));
    }
    ~CSpillableStream()
    {
        while (pos < numReadRows)
        {
            ReleaseThorRow(readRows[pos++]);
        }
        ReleaseThorRow(readRows);
    }

// IRowStream
    virtual const void *nextRow()
    {
        if (spillStream)
            return spillStream->nextRow();
        if (pos == numReadRows)
        {
            CThorSpillableRowArray::CThorSpillableRowArrayLock block(rows);
            if (spillFile)
            {
                spillStream.setown(createRowStream(spillFile, rowIf, 0, (offset_t)-1, (unsigned __int64)-1, false, preserveNulls));
                return spillStream->nextRow();
            }
            rowcount_t fetch = rows.numCommitted();
            if (0 == fetch)
                return NULL;
            if (fetch >= granularity)
                fetch = granularity;
            // consume 'fetch' rows
            const void **toRead = rows.getBlock(fetch);
            memcpy(readRows, toRead, fetch * sizeof(void *));
            rows.noteSpilled(fetch);
            rows.flush();
            numReadRows = fetch;
            pos = 0;
        }
        const void *row = readRows[pos];
        readRows[pos] = NULL;
        ++pos;
        return row;
    }
    virtual void stop() { }
};

//====

void CThorExpandingRowArray::init(rowcount_t initialSize, bool _stableSort)
{
    rowManager = activity.queryJob().queryRowManager();
    stableSort = _stableSort;
    throwOnOom = false;
    stableSortTmp = NULL;
    if (initialSize)
    {
        rows = static_cast<const void * *>(rowManager->allocate(initialSize * sizeof(void*), activity.queryContainer().queryId()));
        maxRows = RoxieRowCapacity(rows) / sizeof(void *);
        memset(rows, 0, maxRows * sizeof(void *));
        if (stableSort)
            stableSortTmp = static_cast<void **>(rowManager->allocate(initialSize * sizeof(void*), activity.queryContainer().queryId()));
    }
    else
    {
        rows = NULL;
        maxRows = 0;
    }
    numRows = 0;
}

const void *CThorExpandingRowArray::allocateNewRows(rowcount_t requiredRows, OwnedConstThorRow &newStableSortTmp)
{
    unsigned newSize = maxRows;
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
    OwnedConstThorRow newRows;
    try
    {
        newRows.setown(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
        if (!newRows)
            return NULL;
        if (stableSort)
        {
            newStableSortTmp.setown(rowManager->allocate(newSize * sizeof(void*), activity.queryContainer().queryId()));
            if (!newStableSortTmp)
                return NULL;
        }
    }
    catch (IException * e)
    {
        //Pahological cases - not enough memory to reallocate the target row buffer, or no contiguous pages available.
        unsigned code = e->errorCode();
        if ((code == ROXIEMM_MEMORY_LIMIT_EXCEEDED) || (code == ROXIEMM_MEMORY_POOL_EXHAUSTED))
        {
            e->Release();
            return NULL;
        }
        throw;
    }
    return newRows.getClear();
}

void CThorExpandingRowArray::doSort(unsigned n, void **const rows, ICompare &compare, unsigned maxCores)
{
    if (stableSort)
    {
        void **_rows = rows;
        memcpy(stableSortTmp, _rows, n*sizeof(void **));
        parqsortvecstable(stableSortTmp, n, compare, (void ***)_rows, maxCores);
        while (n--)
        {
            *_rows = **((void ***)_rows);
            _rows++;
        }
    }
    else
        parqsortvec((void **const)rows, n, compare, maxCores);
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, bool _allowNulls, bool _stableSort, bool _throwOnOom, rowcount_t initialSize) : activity(_activity)
{
    init(initialSize, _stableSort);
    setup(&_activity, _allowNulls, _stableSort, _throwOnOom);
}

CThorExpandingRowArray::CThorExpandingRowArray(CActivityBase &_activity, IRowInterfaces *_rowIf, bool _allowNulls, bool _stableSort, bool _throwOnOom, rowcount_t initialSize) : activity(_activity)
{
    init(initialSize, _stableSort);
    setup(_rowIf, _allowNulls, _stableSort, _throwOnOom);
}

CThorExpandingRowArray::~CThorExpandingRowArray()
{
    clearRows();
    ReleaseThorRow(rows);
    ReleaseThorRow(stableSortTmp);
}

void CThorExpandingRowArray::setup(IRowInterfaces *_rowIf, bool _allowNulls, bool _stableSort, bool _throwOnOom)
{
    rowIf = _rowIf;
    stableSort = _stableSort;
    throwOnOom = _throwOnOom;
    allowNulls = _allowNulls;
    if (rowIf)
    {
        allocator = rowIf->queryRowAllocator();
        deserializer = rowIf->queryRowDeserializer();
        serializer = rowIf->queryRowSerializer();
    }
    else
    {
        allocator = NULL;
        deserializer = NULL;
        serializer = NULL;
    }
}

void CThorExpandingRowArray::clearRows()
{
    for (rowcount_t i = 0; i < numRows; i++)
        ReleaseThorRow(rows[i]);
    numRows = 0;
}

void CThorExpandingRowArray::kill()
{
    clearRows();
    maxRows = 0;
    ReleaseThorRow(rows);
    ReleaseThorRow(stableSortTmp);
    rows = NULL;
    stableSortTmp = NULL;
}

void CThorExpandingRowArray::swap(CThorExpandingRowArray &other)
{
    roxiemem::IRowManager *otherRowManager = other.rowManager;
    IRowInterfaces *otherRowIf = other.rowIf;
    const void **otherRows = other.rows;
    void **otherStableSortTmp = other.stableSortTmp;
    bool otherAllowNulls = other.allowNulls;
    bool otherStableSort = other.stableSort;
    bool otherThrowOnOom = other.throwOnOom;
    rowcount_t otherMaxRows = other.maxRows;
    rowcount_t otherNumRows = other.numRows;

    other.rowManager = rowManager;
    other.setup(rowIf, allowNulls, stableSort, throwOnOom);
    other.rows = rows;
    other.stableSortTmp = stableSortTmp;
    other.maxRows = maxRows;
    other.numRows = numRows;

    rowManager = otherRowManager;
    setup(otherRowIf, otherAllowNulls, otherStableSort, otherThrowOnOom);
    rows = otherRows;
    stableSortTmp = otherStableSortTmp;
    maxRows = otherMaxRows;
    numRows = otherNumRows;
}

void CThorExpandingRowArray::transferRows(rowcount_t & outNumRows, const void * * & outRows)
{
    outNumRows = numRows;
    outRows = rows;
    numRows = 0;
    maxRows = 0;
    rows = NULL;
    ReleaseThorRow(stableSortTmp);
    stableSortTmp = NULL;
}

void CThorExpandingRowArray::transferFrom(CThorExpandingRowArray &donor)
{
    kill();
    donor.transferRows(numRows, rows);
    maxRows = numRows;
    if (stableSort && maxRows)
        ensure(maxRows);
}

void CThorExpandingRowArray::transferFrom(CThorSpillableRowArray &donor)
{
	transferFrom((CThorExpandingRowArray &)donor);
}

void CThorExpandingRowArray::removeRows(rowcount_t start, rowcount_t n)
{
    assertex(numRows-start >= n);
    assertex(!n || rows);
    if (rows)
    {
        for (rowcount_t i = start; i < n; i++)
            ReleaseThorRow(rows[i]);
        //firstRow = 0;
        numRows -= n;
        const void **from = rows+start;
        memmove(from, from+n, n * sizeof(void *));
    }
}

void CThorExpandingRowArray::clearUnused()
{
    if (rows)
        memset(rows+numRows, 0, (maxRows-numRows) * sizeof(void *));
}

bool CThorExpandingRowArray::ensure(rowcount_t requiredRows)
{
    OwnedConstThorRow newStableSortTmp;
    OwnedConstThorRow newRows = allocateNewRows(requiredRows, newStableSortTmp);
    if (!newRows)
        throw MakeActivityException(&activity, 0, "Out of memory, allocating row array, had %"RCPF"d, trying to allocate %"RCPF"d elements", ordinality(), requiredRows);

    const void **oldRows = rows;
    void **oldStableSortTmp = stableSortTmp;

    memcpy((void *)newRows.get(), rows, numRows * sizeof(void*));

    rows = (const void **)newRows.getClear();
    maxRows = RoxieRowCapacity(rows) / sizeof(void *);
    stableSortTmp = (void **)newStableSortTmp.getClear();
    ReleaseThorRow(oldRows);
    ReleaseThorRow(oldStableSortTmp);

    return true;
}

void CThorExpandingRowArray::sort(ICompare &compare, unsigned maxCores)
{
    if (numRows>1)
        doSort(numRows, (void **const)rows, compare, maxCores);
}

void CThorExpandingRowArray::reorder(rowcount_t start, rowcount_t num, unsigned *neworder)
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
    for (unsigned i=0; i<num; i++)
        p[i] = tmp[neworder[i]];
}

bool CThorExpandingRowArray::equal(ICompare *icmp, CThorExpandingRowArray &other)
{
    // slow but better than prev!
    unsigned n = other.ordinality();
    if (n!=ordinality())
        return false;
    for (unsigned i=0;i<n;i++)
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
    unsigned i;
    unsigned n=ordinality();
    for (i=1; i<n; i++)
    {
        if (icmp->docompare(rows[i-1], rows[i])>0)
            return false;
    }
    return true;
}

IRowStream *CThorExpandingRowArray::createRowStream(rowcount_t start, rowcount_t num, bool streamOwns)
{
    class CStream : public CSimpleInterface, implements IRowStream
    {
        rowcount_t pos;
        CThorExpandingRowArray &parent;
        bool owns;
        rowcount_t lastRow;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStream(CThorExpandingRowArray &_parent, rowcount_t firstRow, rowcount_t _lastRow, bool _owns)
            : parent(_parent), pos(firstRow), lastRow(_lastRow), owns(_owns)
        {
        }

    // IRowStream
        virtual const void *nextRow()
        {
            if (pos >= lastRow)
                return NULL;
            if (owns)
                return parent.getClear(pos++);
            else
                return parent.get(pos++);
        }
        virtual void stop() { }
    };

    if (start>ordinality())
        start = ordinality();
    rowcount_t lastRow;
    if ((num==(rowcount_t)-1)||(start+num>ordinality()))
        lastRow = ordinality();
    else
        lastRow = start+num;

    return new CStream(*this, start, lastRow, streamOwns);
}

void CThorExpandingRowArray::partition(ICompare &compare, unsigned num, UnsignedArray &out)
{
    unsigned p=0;
    unsigned n = ordinality();
    while (num)
    {
        out.append(p);
        if (p<n)
        {
            unsigned q = p+(n-p)/num;
            if (p==q){ // skip to next group
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
    rowcount_t c = ordinality();
    assertex(serializer);
    offset_t total = 0;
    for (unsigned i=0; i<c; i++)
    {
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)rows[i]);
        total += ssz.size();
    }
    return total;
}

void CThorExpandingRowArray::serialize(IRowSerializerTarget &out)
{
    bool warnnull = true;
    assertex(serializer);
    rowcount_t n = ordinality();
    if (n)
    {
        for (rowcount_t i = 0; i < n; i++)
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
    if (!allowNulls)
        serialize(s);
    else
    {
        unsigned short guard = 0x7631;
        mb.append(guard);
        rowcount_t n = ordinality();
        if (n)
        {
            for (rowcount_t i = 0; i < n; i++)
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

unsigned CThorExpandingRowArray::serializeBlock(MemoryBuffer &mb, size32_t dstmax, unsigned idx, unsigned count)
{
    assertex(serializer);
    CMemoryRowSerializer out(mb);
    bool warnnull = true;
    unsigned num=ordinality();
    if (idx>=num)
        return 0;
    if (num-idx<count)
        count = num-idx;
    unsigned ret = 0;
    for (unsigned i=0;i<count;i++)
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
        if (mb.length()>dstmax)
        {
            if (ln)
                mb.setLength(ln);   // make sure one row
            break;
        }
        ret++;
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
    if (allowNulls)
    {
        ASSERTEX((sz>=sizeof(short))&&(*(unsigned short *)buf==0x7631)); // check for mismatch
        buf = (const byte *)buf+sizeof(unsigned short);
        sz -= sizeof(unsigned short);
    }
    CThorStreamDeserializerSource d(sz,buf);
    while (!d.eos())
    {
        if (allowNulls)
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
    CThorSpillableRowArrayLock block(*this);
    writeCallbacks.append(cb); // NB not linked to avoid circular dependency
}
void CThorSpillableRowArray::unregisterWriteCallback(IWritePosCallback &cb)
{
    CThorSpillableRowArrayLock block(*this);
    writeCallbacks.zap(cb);
}

CThorSpillableRowArray::CThorSpillableRowArray(CThorSpillableRowArray &other)
    : CThorExpandingRowArray(other.activity), commitDelta(CommitStep)
{
    transferFrom(other);
}

CThorSpillableRowArray::CThorSpillableRowArray(CActivityBase &activity, bool allowNulls, bool stable, rowcount_t initialSize, size32_t _commitDelta)
    : CThorExpandingRowArray(activity, &activity, false, stable, false, initialSize), commitDelta(_commitDelta)
{
    commitRows = 0;
    firstRow = 0;
}

CThorSpillableRowArray::CThorSpillableRowArray(CActivityBase &activity, IRowInterfaces *rowIf, bool allowNulls, bool stable, rowcount_t initialSize, size32_t _commitDelta)
    : CThorExpandingRowArray(activity, rowIf, false, stable, false, initialSize), commitDelta(_commitDelta)
{
    commitRows = 0;
    firstRow = 0;
}

CThorSpillableRowArray::~CThorSpillableRowArray()
{
    clearRows();
}

void CThorSpillableRowArray::clearRows()
{
    for (rowcount_t i = firstRow; i < numRows; i++)
        ReleaseThorRow(rows[i]);
    numRows = 0;
    firstRow = 0;
    commitRows = 0;
}

void CThorSpillableRowArray::kill()
{
    clearRows();
    CThorExpandingRowArray::kill();
}

bool CThorSpillableRowArray::ensure(rowcount_t requiredRows)
{
    //Only the writer is allowed to reallocate rows (otherwise append can't be optimized), so rows is valid outside the lock

    OwnedConstThorRow newStableSortTmp;
    OwnedConstThorRow newRows = allocateNewRows(requiredRows, newStableSortTmp);
    if (!newRows)
        return false;

    const void **oldRows;
    void **oldStableSortTmp;
    {
        CThorSpillableRowArrayLock block(*this);

        oldRows = rows;
        oldStableSortTmp = stableSortTmp;
        memcpy((void *)newRows.get(), rows+firstRow, (numRows - firstRow) * sizeof(void*));
        numRows -= firstRow;
        commitRows -= firstRow;
        firstRow = 0;

        rows = (const void **)newRows.getClear();
        maxRows = RoxieRowCapacity(rows) / sizeof(void *);
        stableSortTmp = (void **)newStableSortTmp.getClear();
    }
    ReleaseThorRow(oldRows);
    ReleaseThorRow(oldStableSortTmp);
	return true;
}

void CThorSpillableRowArray::sort(ICompare &compare, unsigned maxCores)
{
    unsigned n = numCommitted();
    if (n>1)
    {
        void **const rows = (void **const)getBlock(n);
        doSort(n, rows, compare, maxCores);
    }
}

unsigned CThorSpillableRowArray::save(IFile &iFile, rowcount_t watchRecNum, offset_t *watchFilePosResult)
{
    rowcount_t n = numCommitted();
    if (0 == n)
        return 0;
    const void **rows = getBlock(n);
    Owned<IExtRowWriter> writer = createRowWriter(&iFile, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), allowNulls, false, true);
    ActPrintLog(&activity, "CThorSpillableRowArray::save %"RCPF"d rows", numRows);
    offset_t startPos = writer->getPosition();
    for (rowcount_t i=0; i < n; i++)
    {
        const void *row = rows[i];
        assertex(row || allowNulls);
        writer->putRow(row);
        rows[i] = NULL;
        ForEachItemIn(c, writeCallbacks)
        {
            IWritePosCallback &callback = writeCallbacks.item(c);
            if (i == callback.queryRecordNumber())
            {
                writer->flush();
                callback.filePosition(writer->getPosition());
            }
        }
    }
    writer->flush();
    offset_t bytesWritten = writer->getPosition() - startPos;
    writer.clear();
    ActPrintLog(&activity, "CThorSpillableRowArray::save done, bytes = %"I64F"d", (__int64)bytesWritten);
    return n;
}


// JCSMORE - these methods are essentially borrowed from RoxieOutputRowArray, would be good to unify
const void **CThorSpillableRowArray::getBlock(rowcount_t readRows)
{
    dbgassertex(firstRow+readRows <= commitRows);
    return rows + firstRow;
}

void CThorSpillableRowArray::flush()
{
    CThorSpillableRowArrayLock block(*this);
    dbgassertex(numRows >= commitRows);
    //This test could be improved...
    if (firstRow != 0 && firstRow == commitRows)
    {
        //A block of rows was removed - copy these rows to the start of the block.
        memmove(rows, rows+firstRow, (numRows-firstRow) * sizeof(void *));
        numRows -= firstRow;
        firstRow = 0;
    }

    commitRows = numRows;
}

void CThorSpillableRowArray::transferRows(rowcount_t & outNumRows, const void * * & outRows)
{
    assertex(firstRow == 0);  // could allow that to be transferred as well
    CThorSpillableRowArrayLock block(*this);
    CThorExpandingRowArray::transferRows(outNumRows, outRows);
    //firstRows = 0;
    commitRows = 0;
}

void CThorSpillableRowArray::transferFrom(CThorExpandingRowArray &src)
{
    CThorSpillableRowArrayLock block(*this);
    CThorExpandingRowArray::transferFrom(src);
    commitRows = numRows;
}

void CThorSpillableRowArray::transferFrom(CThorSpillableRowArray &donor)
{
	transferFrom((CThorExpandingRowArray &)donor);
}

void CThorSpillableRowArray::swap(CThorSpillableRowArray &other)
{
    CThorSpillableRowArrayLock block(*this);
    CThorExpandingRowArray::swap(other);
    rowcount_t otherFirstRow = other.firstRow;
    rowcount_t otherCommitRows = other.commitRows;

    other.firstRow = firstRow;
    other.commitRows = commitRows;

    firstRow = otherFirstRow;
    commitRows = otherCommitRows;
}

IRowStream *CThorSpillableRowArray::createRowStream()
{
    return new CSpillableStream(activity, *this, rowIf, allowNulls);
}



class CThorRowCollectorBase : public CSimpleInterface, implements roxiemem::IBufferedRowCallback
{
    class CFileOwner : public CSimpleInterface, implements IInterface
    {
        IFile *iFile;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        CFileOwner(IFile *_iFile) : iFile(_iFile)
        {
        }
        ~CFileOwner()
        {
            iFile->remove();
        }
        IFile &queryIFile() const { return *iFile; }
    };
protected:
    CActivityBase &activity;
    CThorSpillableRowArray spillableRows;
    PointerIArrayOf<CFileOwner> spillFiles;
    Owned<IOutputRowSerializer> serializer;
    RowCollectorFlags diskMemMix;
    unsigned spillPriority;
    unsigned totalRows;
    unsigned overflowCount;
    unsigned maxCores;
    unsigned outStreams;
    ICompare *iCompare;
    bool isStable, preserveGrouping;
    IRowInterfaces *rowIf;
    SpinLock readerLock;
    bool mmRegistered;
    Owned<CSharedSpillableRowSet> spillableRowSet;

    bool spillRows()
    {
        if (rc_allMem == diskMemMix)
            return false;
        rowcount_t numRows = spillableRows.numCommitted();
        if (numRows == 0)
            return false;

        totalRows += numRows;
        if (iCompare)
            spillableRows.sort(*iCompare, maxCores); // sorts committed rows

        StringBuffer tempname;
        GetTempName(tempname,"srtspill",true);
        Owned<IFile> iFile = createIFile(tempname.str());
        spillFiles.append(new CFileOwner(iFile.getLink()));
        spillableRows.save(*iFile); // saves committed rows
        spillableRows.noteSpilled(numRows);

        ++overflowCount;

        return true;
    }
    void setPreserveGrouping(bool _preserveGrouping)
    {
        preserveGrouping = _preserveGrouping;
        spillableRows.setAllowNulls(preserveGrouping);
    }
    void putRow(const void *row)
    {
        if (!spillableRows.append(row))
        {
            bool oom = false;
            if (rc_allMem != diskMemMix)
            {
                CThorSpillableRowArray::CThorSpillableRowArrayLock block(spillableRows);
                //We should have been called back to free any committed rows, but occasionally it may not (e.g., if
                //the problem is global memory is exhausted) - in which case force a spill here (but add any pending
                //rows first).
                if (spillableRows.numCommitted() != 0)
                {
                    spillableRows.flush();
                    spillRows();
                }
                //Ensure new rows are written to the head of the array.  It needs to be a separate call because
                //spillRows() cannot shift active row pointer since it can be called from any thread
                spillableRows.flush();
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
    IRowStream *getStream(CThorExpandingRowArray *allMemRows, bool shared)
    {
        SpinBlock b(readerLock);
        if (0 == outStreams)
        {
            spillableRows.flush();
            if ((rc_allDisk == diskMemMix) || ((rc_allDiskOrAllMem == diskMemMix) && overflowCount))
            {
                CThorSpillableRowArray::CThorSpillableRowArrayLock block(spillableRows);
                if (spillableRows.numCommitted())
                {
                    spillRows();
                    spillableRows.kill();
                }
            }
        }
        ++outStreams;

        class CStreamFileOwner : public CSimpleInterface, implements IExtRowStream
        {
            Linked<CFileOwner> fileOwner;
            IExtRowStream *stream;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
            CStreamFileOwner(CFileOwner *_fileOwner, IExtRowStream *_stream) : fileOwner(_fileOwner)
            {
                stream = LINK(_stream);
            }
            ~CStreamFileOwner()
            {
                stream->Release();
            }
        // IExtRowStream
            virtual const void *nextRow() { return stream->nextRow(); }
            virtual void stop() { stream->stop(); }
            virtual offset_t getOffset() { return stream->getOffset(); }
            virtual void stop(CRC32 *crcout=NULL) { stream->stop(); }
            virtual const void *prefetchRow(size32_t *sz=NULL) { return stream->prefetchRow(sz); }
            virtual void prefetchDone() { stream->prefetchDone(); }
            virtual void reinit(offset_t offset, offset_t len, unsigned __int64 maxRows)
            {
                stream->reinit(offset, len, maxRows);
            }
        };

        // NB: CStreamFileOwner, shares reference so CFileOwner, last usage, will auto delete file
        // which may be one of these streams of CThorRowCollectorBase itself
        IArrayOf<IRowStream> instrms;
        ForEachItemIn(f, spillFiles)
        {
            CFileOwner *fileOwner = spillFiles.item(f);
            Owned<IExtRowStream> strm = createRowStream(&fileOwner->queryIFile(), rowIf, 0, (offset_t) -1, (unsigned __int64)-1, false, preserveGrouping);
            instrms.append(* new CStreamFileOwner(fileOwner, strm));
        }

        {
            CThorSpillableRowArray::CThorSpillableRowArrayLock block(spillableRows);
            if (spillableRowSet)
                instrms.append(*spillableRowSet->createRowStream());
            else if (spillableRows.numCommitted())
            {
                totalRows += spillableRows.numCommitted();
                if (iCompare && (1 == outStreams))
                    spillableRows.sort(*iCompare, maxCores);
                if (rc_allDiskOrAllMem == diskMemMix || (NULL!=allMemRows && (rc_allMem == diskMemMix)))
                {
                    assertex(allMemRows);
                    assertex(1 == outStreams);
                    allMemRows->transferFrom(spillableRows);
                    // stream cannot be used
                    return NULL;
                }
                if (!shared)
                    instrms.append(*spillableRows.createRowStream()); // NB: stream will take ownership of rows in spillableRows
                else
                {
                    spillableRowSet.setown(new CSharedSpillableRowSet(activity, spillableRows, rowIf, preserveGrouping));
                    instrms.append(*spillableRowSet->createRowStream());
                }
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
        totalRows = overflowCount = outStreams = 0;
    }
public:
    CThorRowCollectorBase(CActivityBase &_activity)
        : activity(_activity),
          spillableRows(_activity)
    {
        rowIf = NULL;
        iCompare = NULL;
        preserveGrouping = isStable = false;
        totalRows = 0;
        overflowCount = 0;
        diskMemMix = rc_mixed;
        spillPriority = 50;
        outStreams = 0;
        activity.queryJob().queryRowManager()->addRowBuffer(this);
        mmRegistered = true;
        maxCores = activity.queryMaxCores();
    }
    ~CThorRowCollectorBase()
    {
        reset();
        if (mmRegistered)
            activity.queryJob().queryRowManager()->removeRowBuffer(this);
    }
    void setup(IRowInterfaces *_rowIf, ICompare *_iCompare, bool _isStable, RowCollectorFlags _diskMemMix, unsigned _spillPriority)
    {
        rowIf = _rowIf;
        iCompare = _iCompare;
        isStable = _isStable;
        diskMemMix = _diskMemMix;
        spillPriority = _spillPriority;
        if (rc_allMem == diskMemMix)
            spillPriority = UINT_MAX; // all mem, implies no spilling
        if (mmRegistered && (UINT_MAX == spillPriority))
        {
            mmRegistered = false;
            activity.queryJob().queryRowManager()->removeRowBuffer(this);
        }
        spillableRows.setup(rowIf, false, isStable);
    }
    void transferRowsOut(CThorExpandingRowArray &out, bool sort)
    {
        CThorSpillableRowArray::CThorSpillableRowArrayLock block(spillableRows);
        totalRows += spillableRows.numCommitted();
        if (sort && iCompare)
            spillableRows.sort(*iCompare, maxCores);
        out.transferFrom(spillableRows);
    }
    bool hasOverflowed() const
    {
        return 0 != overflowCount;
    }
    rowcount_t numRows() const
    {
        return totalRows;
    }
    unsigned numOverflows() const
    {
        return overflowCount;
    }
    unsigned overflowScale() const
    {
        // 1 if no spill
        if (!overflowCount)
            return 1;
        return overflowCount*2+3; // bit arbitrary
    }
    void transferRowsIn(CThorExpandingRowArray &src)
    {
        reset();
        spillableRows.transferFrom(src);
    }
// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return spillPriority;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        if (UINT_MAX == spillPriority)
            return false;
        CThorSpillableRowArray::CThorSpillableRowArrayLock block(spillableRows);
        return spillRows();
    }
};

enum TRLGroupFlag { trl_ungroup, trl_preserveGrouping, trl_stopAtEog };
class CThorRowLoader : public CThorRowCollectorBase, implements IThorRowLoader
{
    IRowStream *load(IRowStream *in, const bool &abort, TRLGroupFlag grouping, CThorExpandingRowArray *allMemRows)
    {
        reset();
        setPreserveGrouping(trl_preserveGrouping == grouping);
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
        return getStream(allMemRows, false);
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowLoader(CActivityBase &activity) : CThorRowCollectorBase(activity)
    {
    }
// IThorRowCollectorCommon
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority)
    {
        CThorRowCollectorBase::setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
    }
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorExpandingRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
// IThorRowLoader
    virtual IRowStream *load(IRowStream *in, const bool &abort, bool preserveGrouping, CThorExpandingRowArray *allMemRows)
    {
        assertex(!iCompare || !preserveGrouping); // can't sort if group preserving
        return load(in, abort, preserveGrouping?trl_preserveGrouping:trl_ungroup, allMemRows);
    }
    virtual IRowStream *loadGroup(IRowStream *in, const bool &abort, CThorExpandingRowArray *allMemRows)
    {
        return load(in, abort, trl_stopAtEog, allMemRows);
    }
};

IThorRowLoader *createThorRowLoader(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority)
{
    Owned<IThorRowLoader> loader = new CThorRowLoader(activity);
    loader->setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
    return loader.getClear();
}

IThorRowLoader *createThorRowLoader(CActivityBase &activity, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority)
{
    return createThorRowLoader(activity, &activity, iCompare, isStable, diskMemMix, spillPriority);
}



class CThorRowCollector : public CThorRowCollectorBase, implements IThorRowCollector
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorRowCollector(CActivityBase &activity) : CThorRowCollectorBase(activity)
    {
    }
// IThorRowCollectorCommon
    virtual void setup(IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority, bool preserveGrouping)
    {
        assertex(!iCompare || !preserveGrouping); // can't sort if group preserving
        CThorRowCollectorBase::setup(rowIf, iCompare, isStable, diskMemMix, spillPriority);
        setPreserveGrouping(preserveGrouping);
    }
    virtual rowcount_t numRows() const { return CThorRowCollectorBase::numRows(); }
    virtual unsigned numOverflows() const { return CThorRowCollectorBase::numOverflows(); }
    virtual unsigned overflowScale() const { return CThorRowCollectorBase::overflowScale(); }
    virtual void transferRowsOut(CThorExpandingRowArray &dst, bool sort) { CThorRowCollectorBase::transferRowsOut(dst, sort); }
    virtual void transferRowsIn(CThorExpandingRowArray &src) { CThorRowCollectorBase::transferRowsIn(src); }
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
        // IRowWriter
            virtual void putRow(const void *row)
            {
                parent->putRow(row);
            }
            virtual void flush()
            {
            }
        };
        return new CWriter(this);
    }
    virtual void reset()
    {
        CThorRowCollectorBase::reset();
    }
    virtual IRowStream *getStream(bool shared)
    {
        return CThorRowCollectorBase::getStream(NULL, shared);
    }
};

IThorRowCollector *createThorRowCollector(CActivityBase &activity, IRowInterfaces *rowIf, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority, bool preserveGrouping)
{
    Owned<IThorRowCollector> collector = new CThorRowCollector(activity);
    collector->setup(rowIf, iCompare, isStable, diskMemMix, spillPriority, preserveGrouping);
    return collector.getClear();
}

IThorRowCollector *createThorRowCollector(CActivityBase &activity, ICompare *iCompare, bool isStable, RowCollectorFlags diskMemMix, unsigned spillPriority, bool preserveGrouping)
{
    return createThorRowCollector(activity, &activity, iCompare, isStable, diskMemMix, spillPriority, preserveGrouping);
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
    Linked<ICommunicator> clusterComm;
    CSDSServerStatus *status;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    cMultiThorResourceMutex(const char *groupname,CSDSServerStatus *_status)
    {
        status = _status;
        stopping = false;
        clusterComm.set(&queryClusterComm());
        if (clusterComm->queryGroup().rank(queryMyNode())==0) { // master so start thread
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
                if (clusterComm->recv(mbuf,RANK_ALL,MPTAG_THORRESOURCELOCK,&from,timeout)) {
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
                    clusterComm->reply(mbuf,1000*60*5);
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
            clusterComm->cancel(RANK_ALL,MPTAG_THORRESOURCELOCK);
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
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
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
            if (!clusterComm->sendRecv(mbuf,0,MPTAG_THORRESOURCELOCK,(unsigned)-1))
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


class CThorAllocator : public CSimpleInterface, implements roxiemem::IRowAllocatorCache, implements IRtlRowCallback, implements IThorAllocator
{
    mutable IArrayOf<IEngineRowAllocator> allAllocators;
    mutable SpinLock allAllocatorsLock;
    Owned<roxiemem::IRowManager> rowManager;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorAllocator(memsize_t memSize)
    {
        rowManager.setown(roxiemem::createRowManager(memSize, NULL, queryDummyContextLogger(), this, false));
        rtlSetReleaseRowHook(this);
    }
    ~CThorAllocator()
    {
        rowManager.clear();
        allAllocators.kill();
        rtlSetReleaseRowHook(NULL); // nothing should use it beyond this point anyway
    }

// IThorAllocator
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        // MORE - may need to do some caching/commoning up here otherwise GRAPH in a child query may use too many
        SpinBlock b(allAllocatorsLock);
        IEngineRowAllocator *ret = createRoxieRowAllocator(*rowManager, meta, activityId, allAllocators.ordinality(), false);
        LINK(ret);
        allAllocators.append(*ret);
        return ret;
    }
    virtual roxiemem::IRowManager *queryRowManager() const
    {
        return rowManager;
    }

// IRowAllocatorCache
    virtual unsigned getActivityId(unsigned cacheId) const
    {
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(cacheId))
            return allAllocators.item(cacheId).queryActivityId();
        else
        {
            //assert(false);
            return 12345678; // Used for tracing, better than a crash...
        }
    }
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const
    {
        SpinBlock b(allAllocatorsLock);
        if (allAllocators.isItem(cacheId))
            return allAllocators.item(cacheId).getId(out);
        else
        {
            assert(false);
            return out.append("unknown"); // Used for tracing, better than a crash...
        }
    }
    virtual void onDestroy(unsigned cacheId, void *row) const
    {
        IEngineRowAllocator *allocator;
        {
            SpinBlock b(allAllocatorsLock); // just protect the access to the array - don't keep locked for the call of destruct or may deadlock
            if (allAllocators.isItem(cacheId))
                allocator = &allAllocators.item(cacheId);
            else
            {
                assert(false);
                return;
            }
        }
        allocator->queryOutputMeta()->destruct((byte *) row);
    }
    virtual void checkValid(unsigned cacheId, const void *row) const
    {
        // JCSMORE
    }
// IRtlRowCallback
    virtual void releaseRow(const void * row) const
    {
        ReleaseThorRow(row);
    }
    virtual void releaseRowset(unsigned count, byte * * rowset) const
    {
        if (rowset)
        {
            if (!roxiemem::HeapletBase::isShared(rowset))
            {
                byte * * finger = rowset;
                while (count--)
                    ReleaseThorRow(*finger++);
            }
            ReleaseThorRow(rowset);
        }
    }
    virtual void *linkRow(const void * row) const
    {
        if (row) 
            LinkThorRow(row);
        return const_cast<void *>(row);
    }
    virtual byte * * linkRowset(byte * * rowset) const
    {
        if (rowset)
            LinkThorRow(rowset);
        return const_cast<byte * *>(rowset);
    }
};

IThorAllocator *createThorAllocator(memsize_t memSize)
{
    return new CThorAllocator(memSize);
}


#define OUTPUTMETACHILDROW_VERSION 2 // for now, it's only significant that non-zero
class COutputMetaWithChildRow : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IEngineRowAllocator> childAllocator;
    IOutputMetaData *childMeta;
    size32_t extraSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
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
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerializer(childMeta->createRowSerializer(ctx, activityId), extraSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(childMeta->createRowDeserializer(ctx, activityId), childAllocator, extraSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(childMeta->createRowPrefetcher(ctx, activityId), extraSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() { return this; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) 
    {
        //GH: I think this is what it should do, please check
        visitor.visitRow(*(const byte **)(self+extraSz)); 
    }
};

IOutputMetaData *createOutputMetaDataWithChildRow(IEngineRowAllocator *childAllocator, size32_t extraSz)
{
    return new COutputMetaWithChildRow(childAllocator, extraSz);
}


class COutputMetaWithExtra : public CSimpleInterface, implements IOutputMetaData
{
    Linked<IOutputMetaData> meta;
    size32_t metaSz;
    Owned<IOutputRowSerializer> serializer;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<ISourceRowPrefetcher> prefetcher;
    Owned<IOutputMetaData> serializedmeta;

    class CSerialization : public CSimpleInterface, implements IOutputRowSerializer
    {
        Owned<IOutputRowSerializer> serializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSerialization(IOutputRowSerializer *_serializer, size32_t _metaSz) : serializer(_serializer), metaSz(_metaSz)
        {
        }
        virtual void serialize(IRowSerializerTarget &out, const byte *self)
        {
            out.put(metaSz, self);
            serializer->serialize(out, self+metaSz);
        }
    };
    //GH - This code is the same as CPrefixedRowDeserializer
    class CDeserializer : public CSimpleInterface, implements IOutputRowDeserializer
    {
        Owned<IOutputRowDeserializer> deserializer;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDeserializer(IOutputRowDeserializer *_deserializer, size32_t _metaSz) : deserializer(_deserializer), metaSz(_metaSz)
        {
        }
        virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource &in)
        {
            in.read(metaSz, rowBuilder.getSelf());
            CPrefixedRowBuilder prefixedBuilder(metaSz, rowBuilder);
            size32_t sz = deserializer->deserialize(prefixedBuilder, in);
            return sz+metaSz;
        }
    };

    class CPrefetcher : public CSimpleInterface, implements ISourceRowPrefetcher
    {
        Owned<ISourceRowPrefetcher> childPrefetcher;
        size32_t metaSz;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CPrefetcher(ISourceRowPrefetcher *_childPrefetcher, size32_t _metaSz) : childPrefetcher(_childPrefetcher), metaSz(_metaSz)
        {
        }
        virtual void readAhead(IRowDeserializerSource &in)
        {
            in.skip(metaSz);
            childPrefetcher->readAhead(in);
        }
    };

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COutputMetaWithExtra(IOutputMetaData *_meta, size32_t _metaSz) : meta(_meta), metaSz(_metaSz)
    {
    }
    virtual size32_t getRecordSize(const void *rec) 
    {
        size32_t sz = meta->getRecordSize(rec?((byte *)rec)+metaSz:NULL); 
        return sz+metaSz;
    }
    virtual size32_t getMinRecordSize() const 
    { 
        return meta->getMinRecordSize() + metaSz;
    }
    virtual size32_t getFixedSize() const 
    {
        size32_t sz = meta->getFixedSize();
        if (!sz)
            return 0;
        return sz+metaSz;
    }

    virtual void toXML(const byte * self, IXmlWriter & out) { meta->toXML(self, out); }
    virtual unsigned getVersion() const { return meta->getVersion(); }

//The following can only be called if getMetaDataVersion >= 1, may seh otherwise.  Creating a different interface was too painful
    virtual unsigned getMetaFlags() { return meta->getMetaFlags(); }
    virtual void destruct(byte * self) { meta->destruct(self); }
    virtual IOutputRowSerializer * createRowSerializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!serializer)
            serializer.setown(new CSerialization(meta->createRowSerializer(ctx, activityId), metaSz));
        return LINK(serializer);
    }
    virtual IOutputRowDeserializer * createRowDeserializer(ICodeContext * ctx, unsigned activityId)
    {
        if (!deserializer)
            deserializer.setown(new CDeserializer(meta->createRowDeserializer(ctx, activityId), metaSz));
        return LINK(deserializer);
    }
    virtual ISourceRowPrefetcher * createRowPrefetcher(ICodeContext * ctx, unsigned activityId)
    {
        if (!prefetcher)
            prefetcher.setown(new CPrefetcher(meta->createRowPrefetcher(ctx, activityId), metaSz));
        return LINK(prefetcher);
    }
    virtual IOutputMetaData * querySerializedMeta() 
    { 
        IOutputMetaData *sm = meta->querySerializedMeta();
        if (sm==meta.get())
            return this;
        if (!serializedmeta.get())
            serializedmeta.setown(new COutputMetaWithExtra(sm,metaSz));
        return serializedmeta.get();
    } 
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor)
    {
        meta->walkIndirectMembers(self, visitor);
    }
};

IOutputMetaData *createOutputMetaDataWithExtra(IOutputMetaData *meta, size32_t sz)
{
    return new COutputMetaWithExtra(meta, sz);
}



IPerfMonHook *createThorMemStatsPerfMonHook(IPerfMonHook *chain)
{
    return LINK(chain);
}

void setLCRrowCRCchecking(bool on)
{
    // JCSMORE!
}
