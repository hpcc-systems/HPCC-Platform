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

#include "jlib.hpp"
#include "jsort.hpp"
#include "jsorta.hpp"
#include "jisem.hpp"
#include "jregexp.hpp"
#include "ccd.hpp"
#include "ccdkey.hpp"
#include "ccdfile.hpp"
#include "jhtree.hpp"
#include "eclhelper.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"

#ifdef _DEBUG
//#define _LIMIT_FILECOUNT 5    // Useful for debugging queries when not enough memory to load data...
#endif

#ifdef __64BIT__
#define BASED_POINTERS
#endif

class PtrToOffsetMapper 
{
    struct FragmentInfo
    {
        const char *base;    // base address of file chunk
        offset_t length;
        offset_t baseOffset; // corresponding file offset
        unsigned partNo;     // corresponding part number
    };

    FragmentInfo *fragments;
    unsigned numBases;
    unsigned maxBases;

    FragmentInfo &findBase(const char *ptr) const
    {
        // MORE - could cache last hit
        unsigned int a = 0;
        int b = numBases;
        int rc;
        while ((int)a<b)
        {
            int i = a+(b+1-a)/2;
            rc = ptr - fragments[i-1].base;
            if (rc>=0)
                a = i;
            else
                b = i-1;
        }
        assertex(a > 0);    // or incoming value was not part of ANY fragment
        // MORE - could also check it's in the range for this fragment
        return fragments[a-1];
    }

    FragmentInfo &findBase(offset_t pos) const
    {
        // MORE - could cache last hit
        unsigned int a = 0;
        int b = numBases;
        while ((int)a<b)
        {
            int i = (a+b+1)/2;
            if (pos >= fragments[i-1].baseOffset)
                a = i;
            else
                b = i-1;
        }
        assertex(a > 0);    // or incoming value was not part of ANY fragment
        // MORE - could also check it's in the range for this fragment
        return fragments[a-1];
    }

    PtrToOffsetMapper(const PtrToOffsetMapper &);  // Not implemented

public:
    PtrToOffsetMapper()
    {
        fragments = NULL;
        numBases = 0;
        maxBases = 0;
    }

    ~PtrToOffsetMapper()
    {
        delete [] fragments;
    }

    static int compareFragments(const void *_l, const void *_r) 
    {
        FragmentInfo *l = (FragmentInfo *) _l;
        FragmentInfo *r = (FragmentInfo *) _r;
        return l->base - r->base;
    }

    void addFragment(const void *base, offset_t length, unsigned partNo, offset_t baseOffset, unsigned pass)
    {
        if (!pass)
            maxBases++;
        else
        {
            assertex(numBases < maxBases);
            if (!numBases)
            {
                fragments = new FragmentInfo [maxBases];
            }
            fragments[numBases].base = (const char *) base;
            fragments[numBases].length = length;
            fragments[numBases].baseOffset = baseOffset;
            fragments[numBases].partNo = partNo;
            numBases++;
            if (numBases==maxBases)
            {
                qsort(fragments, numBases, sizeof(fragments[0]), compareFragments); 
            }
        }
    }

    offset_t ptrToFilePosition(const void *_ptr) const
    {
        // Binchop into bases to find which one is mine.
        const char *ptr = (const char *) _ptr;
        FragmentInfo &frag = findBase(ptr);
        return frag.baseOffset + ptr - frag.base;
    }

    offset_t ptrToLocalFilePosition(const void *_ptr) const
    {
        // Binchop into bases to find which one is mine.
        const char *ptr = (const char *) _ptr;
        FragmentInfo &frag = findBase(ptr);
        return makeLocalFposOffset(frag.partNo, ptr - frag.base);
    }

    offset_t makeFilePositionLocal(offset_t pos)
    {
        FragmentInfo &frag = findBase(pos);
        return makeLocalFposOffset(frag.partNo, pos - frag.baseOffset);
    }

    void splitPos(offset_t &offset, offset_t &size, unsigned partNo, unsigned numParts)
    {
        assert(numParts > 0);
        assert(partNo < numParts);
        if (numBases)
        {
            unsigned startingPart = (partNo * numBases) / numParts;
            unsigned endingPart = ((partNo+1) * numBases) / numParts;
            if (startingPart < endingPart)
            {
                // set offset and size such that we will process part n of m
                const char *startBase = fragments[startingPart].base;
                offset = startBase - fragments[0].base;
                size = (fragments[endingPart-1].base - startBase) + fragments[endingPart-1].length;
                return;
            }
        }
        size = 0;
        offset = 0;
    }
};

class InMemoryIndexCursor;
class InMemoryIndexTest;

static StringBuffer& describeSegmonitors(SegMonitorArray &segMonitors, StringBuffer &ret)
{
    ForEachItemIn(idx, segMonitors)
    {
        if (idx)
            ret.append('.');
        IKeySegmentMonitor &item = segMonitors.item(idx);
        if (item.getSize()==1 && item.getOffset()>='a' && item.getOffset() <='z')
            ret.appendf("%c", item.getOffset());
        else
            ret.appendf("%u.%u", item.getOffset(), item.getSize());
    }
    return ret;
}

#ifdef BASED_POINTERS
typedef unsigned t_indexentry;
#define GETROW(a) ((void *) ((char *)base+ptrs[a]))
#else
typedef const void *t_indexentry;
#define GETROW(a) (ptrs[a])
#endif

class InMemoryIndex : public CInterface, implements IInterface, implements ICompare, implements IIndexReadContext
{
    // A list of pointers to all the records in a memory-loaded disk file, ordered by a field/fields in the file
    // there is a bit of commonality between this and IKeyManager.... but this is better I think

    friend class InMemoryIndexCursor;
    friend class InMemoryIndexTest;

    IArrayOf<IKeySegmentMonitor> segMonitors;   // defining the sort order
#ifdef BASED_POINTERS
    const void *base;
#endif
    t_indexentry *ptrs;
    unsigned numPtrs;
    unsigned maxPtrs;
    unsigned totalCount;
    mutable ReadWriteLock inUse;
    CriticalSection stateCrit;

    enum { unbuilt, building, buildingDeprecated, deprecated, active } state;

public:
    IMPLEMENT_IINTERFACE;

    InMemoryIndex()
    {
        maxPtrs = 0;
        numPtrs = 0;
        totalCount = 0;
        ptrs = NULL;
#ifdef BASED_POINTERS
        base = NULL;
#endif
        state = unbuilt;
    }

    InMemoryIndex(SegMonitorArray &segments)
    {
        maxPtrs = 0;
        numPtrs = 0;
        totalCount = 0;
        ptrs = NULL;
#ifdef BASED_POINTERS
        base = NULL;
#endif
        state = unbuilt;
        ForEachItemIn(idx, segments)
            append(LINK(&segments.item(idx)));
    }

    InMemoryIndex(IPropertyTree &x)
    {
        maxPtrs = 0;
        numPtrs = 0;
        totalCount = 0;
        ptrs = NULL;
#ifdef BASED_POINTERS
        base = NULL;
#endif
        state = unbuilt;
        Owned<IPropertyTreeIterator> fields = x.getElements("Field");
        ForEach(*fields)
        {
            IPropertyTree &field = fields->query();
            unsigned offset = field.getPropInt("@offset", 0);
            unsigned size = field.getPropInt("@size", 0);
            bool isSigned = field.getPropBool("@isSigned", false);
            bool isLittleEndian = field.getPropBool("@isLittleEndian", false);
            append(createDummyKeySegmentMonitor(offset, size, isSigned, isLittleEndian));
        }
    }

    ~InMemoryIndex()
    {
        free(ptrs);
    }

    // IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment)
    {
        segMonitors.append(*segment);
        totalCount += segment->getSize();
    }

    virtual void setMergeBarrier(unsigned barrierOffset)
    {
        // We don't merge segmonitors so nothing to do
    }

    virtual unsigned ordinality() const
    {
        return segMonitors.length();
    }

    virtual IKeySegmentMonitor *item(unsigned idx) const
    {
        if (segMonitors.isItem(idx))
            return &segMonitors.item(idx);
        else
            return NULL;
    }

    void serializeCursorPos(MemoryBuffer &mb)
    {
        // We are saving a unique signature of this index that can be used to ensure that any continuation will identify the same index
        // Note that the continuation may be executed by a different slave
        // This code is not great but is probably ok.
        StringBuffer b;
        toString(b);
        mb.append(b);
    }

    // ICompare
    virtual int docompare(const void *l, const void *r) const
    {
#ifdef BASED_POINTERS
        l = (char *) base + *(unsigned *) l;
        r = (char *) base + *(unsigned *) r;
#endif
        ForEachItemIn(idx, segMonitors)
        {
            int ret = segMonitors.item(idx).docompareraw(l, r);
            if (ret)
                return ret;
        }
        // Compare the pointers to guarantee stable sort
        return (const char *) l - (const char *) r;
    }

    inline void deprecate()
    {
        CriticalBlock b(stateCrit);
        switch(state)
        {
        case building:
            state = buildingDeprecated;
            break;
        case active:
            state = deprecated;
            break;
        }
    }

    inline void undeprecate()
    {
        CriticalBlock b(stateCrit);
        switch(state)
        {
        case building:
            state = active;
            break;
        case buildingDeprecated:
            state = deprecated;
            break;
        }
    }

    inline void setBuilding()
    {
        CriticalBlock b(stateCrit);
        assertex(state==deprecated || state==unbuilt);
        state = building;
    }

    inline bool available()
    {
        CriticalBlock b(stateCrit);
        return state==active || state==deprecated;
    }

    StringBuffer& toString(StringBuffer &ret)
    {
        return describeSegmonitors(segMonitors, ret);
    }

    StringBuffer& toXML(StringBuffer &ret, unsigned indent)
    {
        ret.pad(indent).append("<FieldSet>\n");
        ForEachItemIn(idx, segMonitors)
        {
            IKeySegmentMonitor &item = segMonitors.item(idx);
            ret.pad(indent+1).appendf("<Field offset='%d' size='%d' isSigned='%d' isLittleEndian='%d'/>\n", item.getOffset(), item.getSize(), item.isSigned(), item.isLittleEndian());
        }
        ret.pad(indent).append("</FieldSet>\n");
        return ret;
    }

    void load(const void *_base, offset_t length, IRecordSize *recordSize, unsigned pass)
    {
        // NOTE - we require that memory has been reserved in advance. Two passes if need be (i.e. variable size)
#ifdef BASED_POINTERS
        base = _base;
#endif
        if (pass==0 && recordSize->isFixedSize())
        {
            size32_t size = recordSize->getFixedSize();
            if (length % size)
                throw MakeStringException(ROXIE_FILE_ERROR, "File size %" I64F "u is not a multiple of fixed record size %u", length, size);
            if (length / size > UINT_MAX)
                throw MakeStringException(ROXIE_FILE_ERROR, "Maximum row count %u exceeded", UINT_MAX);
            unsigned oldMax = maxPtrs;
            maxPtrs += (unsigned)(length / size);
            if (oldMax > maxPtrs) // Check if it wrapped
                throw MakeStringException(ROXIE_FILE_ERROR, "Maximum row count %u exceeded", UINT_MAX);
        }
        else
        {
            if (pass==1 && !ptrs)
                ptrs = (t_indexentry *) malloc(maxPtrs * sizeof(t_indexentry));
            const char *finger = (const char *) _base;
            while (length)
            {
                unsigned thisRecSize = recordSize->getRecordSize(finger);
                assertex(thisRecSize <= length);
                if (pass==0)
                {
                    maxPtrs++;
                    if (!maxPtrs) // Check if it wrapped
                        throw MakeStringException(ROXIE_FILE_ERROR, "Maximum row count %u exceeded", UINT_MAX);
                }
                else
                {
                    assertex(numPtrs < maxPtrs);
#ifdef BASED_POINTERS
                    ptrs[numPtrs++] = finger - (const char *) _base;
#else
                    ptrs[numPtrs++] = finger;
#endif
                }
                length -= thisRecSize;
                finger += thisRecSize;
            }
        }
    }

    void load(const InMemoryIndex &donor)
    {
        assertex(!ptrs);
#ifdef BASED_POINTERS
        base = donor.base;
#endif
        maxPtrs = donor.maxPtrs;
        ptrs = (t_indexentry *) malloc(maxPtrs * sizeof(t_indexentry));
        memcpy(ptrs, donor.ptrs, donor.numPtrs * sizeof(t_indexentry));
        numPtrs = donor.numPtrs;
    }

    void resort(const InMemoryIndex &order)
    {
        WriteLockBlock b(inUse);
        segMonitors.kill();
        totalCount = 0;
        ForEachItemIn(idx, order.segMonitors)
            append(LINK(&order.segMonitors.item(idx)));
        StringBuffer x; DBGLOG("Sorting key %s", toString(x).str());
#ifdef BASED_POINTERS
        qsortarray(ptrs, numPtrs, *this);
#else
        qsortvec((void **) ptrs, numPtrs, *this);
#endif
        DBGLOG("Finished sorting key %s", x.str());
        undeprecate();
    }

    unsigned maxScore()
    {
        return totalCount;
    }

    bool deletePending()
    {
        CriticalBlock b(stateCrit);
        return state != active;
    }

    void lockRead()
    {
        inUse.lockRead();
    }

    void unlockRead()
    {
        inUse.unlockRead();
    }

    bool equals(const InMemoryIndex &other)
    {
        if (other.totalCount != totalCount || other.segMonitors.length() != segMonitors.length())
            return false;
        ForEachItemIn(idx, segMonitors)
        {
            if (!segMonitors.item(idx).equivalentTo(other.segMonitors.item(idx)))
                return false;
        }
        return true;
    }
};

class InMemoryIndexManager;
class KeyReporter
{
    CriticalSection managerCrit;
    CIArrayOf<InMemoryIndexManager> indexManagers;

public:
    void addManager(InMemoryIndexManager *);
    void removeManager(InMemoryIndexManager *);
    void report(StringBuffer &reply, const char *filename, unsigned numKeys);
} *keyReporter;

typedef IArrayOf<InMemoryIndex> InMemoryIndexSet;

#define MAX_TRACKED 100
#define MAX_FIELD_OFFSET 256

/*====================================================================================================
 * Some notes on how IDirectReader works (and should work)
 * Assuming numParts is 1...
 *   The InMemoryReader is designed to deliver the whole file in a single go. It has been read into a single slab of memory.
 *   The baseMap allows us to translate memory ptrs into file positions and file parts
 *   The BufferedDisk variant will hide any gaps between parts from the caller
 *   The idea is to return large slabs of data to the caller so that they can process multiple records between refill() calls
 *   In the in-memory case this slab is the entire file.
 * When numParts is > 1
 *   This indicates that the caller has divided the task between multiple threads, and we are to give each thread a portion of the
 *   file to work with.
 *   The in-memory case uses the baseMap to divide the number of files originally loaded into N 
 *     (the file boundaries are the only place we are sure there are record boundaries - fixed size case could do better in theory)
 *   The disk case gives an empty set to all but the first thread - could certainly do better
 * How it should work
 *   IDirectReader should be derived from ISerialStream (with the addition of the ptr to offset mapping functions)
 *   We should not peek past end of a single file
 *   We are then free to use memory-mapped files and/or existing fileIO stream code, and IDirectReader is much simpler, 
 *   just handling the move from one file to the next and the PtrToOffset stuff
 * Implications for caller
 *   Callers already use different code for variable vs fixed cases to avoid conditional code inside inner loop
 *   Callers in fixed case can peek multiple rows ahead (and when in memory will be given very large peeks...)
 *   Callers in variable case will switch to prefetch to get row size rather than getRecordSize().
 *   If we really want maximum performance on variable-size in-memory cases we can have a third variant that relies on initial peek 
 *   giving entire file, and uses getRecordSize(). Will avoid a one or more virtual peek() calls per row.
 *====================================================================================================*/

class InMemoryDirectReader : implements IDirectReader, implements IThorDiskCallback, implements ISimpleReadStream, public CInterface
{
    // MORE - might be able to use some of the jlib IStream implementations. But I don't want any indirections...
public:
    IMPLEMENT_IINTERFACE;
    memsize_t pos;
    const char *start;
    offset_t memsize;
    PtrToOffsetMapper &baseMap;

    InMemoryDirectReader(offset_t _readPos, const char *_start, memsize_t _memsize, PtrToOffsetMapper &_baseMap, unsigned _partNo, unsigned _numParts)
        : baseMap(_baseMap)
    {
        if (_numParts == 1)
        {
            memsize = _memsize;
            start = _start;
        }
        else
        {
            offset_t offset;
            baseMap.splitPos(offset, memsize, _partNo, _numParts);
            start = _start + offset;
        }
        assertex(_readPos <= memsize);
        pos = (memsize_t) _readPos;
    }

    // Interface ISerialStream

    virtual const void * peek(size32_t wanted,size32_t &got)
    {
        offset_t remaining = memsize - pos;
        if (remaining)
        {
            if (remaining >= UINT_MAX)
                got = UINT_MAX-1;
            else
                got = (size32_t) remaining;
            return start + pos;
        }
        else
        {
            got = 0;
            return NULL;
        }
    }
    virtual void get(size32_t len, void * ptr)
    {
        offset_t remaining = memsize - pos;
        if (len > remaining)
            throw MakeStringException(-1, "InMemoryDirectReader::get: requested %u bytes, only %u available", len, (unsigned) remaining);
        memcpy(ptr, start+pos, len);
        pos += len;
    }
    virtual bool eos()
    {
        return pos == memsize;
    }
    virtual void skip(size32_t sz)
    {
        assertex(pos + sz <= memsize);
        pos += sz;
    }
    virtual offset_t tell()
    {
        return pos;
    }
    virtual void reset(offset_t _offset, offset_t _flen)
    {
        assertex(_offset <= memsize);
        pos = (memsize_t) _offset;
    }

    // Interface ISimpleReadStream

    virtual size32_t read(size32_t max_len, void * data)
    {
        size32_t got;
        const void *ptr = peek(max_len, got);
        if (!got)
            return 0;
        memcpy(data, ptr, got);
        skip(got);
        return got;     
    }

    // Interface IDirectReader

    virtual ISimpleReadStream *querySimpleStream()
    {
        return this;
    }

    virtual IThorDiskCallback *queryThorDiskCallback()
    {
        return this;
    }

    virtual unsigned queryFilePart() const
    {
        throwUnexpected(); // only supported for disk files
    }

    // Interface IThorDiskCallback 

    virtual unsigned __int64 getFilePosition(const void *_ptr)
    {
        return baseMap.ptrToFilePosition(_ptr);
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr)
    {
        return baseMap.ptrToLocalFilePosition(_ptr);
    }

    virtual unsigned __int64 makeFilePositionLocal(offset_t pos)
    {
        return baseMap.makeFilePositionLocal(pos);
    }

    virtual const char * queryLogicalFilename(const void * row) 
    { 
        UNIMPLEMENTED;
    }
};

class BufferedDirectReader : implements IDirectReader, implements IThorDiskCallback, implements ISimpleReadStream, public CInterface
{
    // MORE - could combine some code with in memory version.
public:
    IMPLEMENT_IINTERFACE;

    Linked<IFileIOArray> f;
    offset_t thisFileStartPos;
    offset_t completedStreamsSize;
    Owned<IFileIO> thisPart;
    Owned<ISerialStream> curStream;
    unsigned thisPartIdx;

    BufferedDirectReader(offset_t _startPos, IFileIOArray *_f, unsigned _partNo, unsigned _numParts) : f(_f)
    {
        thisFileStartPos = 0;
        completedStreamsSize = 0;

        thisPartIdx = 0;
        unsigned maxParts = f ? f->length() : 0;

        // This code is supposed to divide the work between multiple parallel processors. 
        // See the inMem version for details. 
        // For now until get round to doing better, we give all the work to the first slot...
        while (!_partNo && !thisPart && ++thisPartIdx < maxParts) // MORE
        {
            thisPart.setown(f->getFilePart(thisPartIdx, thisFileStartPos));
            if (thisPart && _startPos >= thisPart->size())
            {
                _startPos -= thisPart->size();
                completedStreamsSize += thisPart->size();
                thisPart.clear();
            }
        }

        if (thisPart)
        {       
            curStream.setown(createFileSerialStream(thisPart, _startPos));
        }
        else
        {
            // No files for this particular reader to do
            thisPartIdx = maxParts;
        }
    }

    void nextFile()
    {
        completedStreamsSize += thisPart->size();
        unsigned maxParts = f->length();
        thisPart.clear();
        curStream.clear();
        while (!thisPart && ++thisPartIdx < maxParts)
        {
            thisPart.setown(f->getFilePart(thisPartIdx, thisFileStartPos ));
        }
        if (thisPart)
        {
            curStream.setown(createFileSerialStream(thisPart));
        }
    }

    virtual const void * peek(size32_t wanted,size32_t &got)
    {
        if (curStream)
        {
            while (curStream->eos())
            {
                nextFile();
                if (!curStream)
                {
                    got = 0;
                    return NULL;
                }
            }
            return curStream->peek(wanted, got);
        }
        else
        {
            got = 0;
            return NULL;
        }
    }

    virtual void get(size32_t len, void * ptr)
    {
        if (curStream)
            curStream->get(len, ptr);
        else
            throw MakeStringException(-1, "BufferedDirectReader::get: requested %u bytes at eof", len);
    }
    virtual bool eos() 
    {
        for (;;)
        {
            if (!curStream)
                return true;
            if (curStream->eos())
                nextFile();
            else
                return false;
        }
    }
    virtual void skip(size32_t len)
    {
        if (curStream)
            curStream->skip(len);
        else
            throw MakeStringException(-1, "BufferedDirectReader::skip: tried to skip %u bytes at eof", len);
    }
    virtual offset_t tell()
    {
        // Note that tell() means the position with this stream, not the file position within the overall logical file.
        if (curStream)
            return completedStreamsSize + curStream->tell();
        else
            return completedStreamsSize;
    }
    virtual void reset(offset_t _offset,offset_t _flen)
    {
        throwUnexpected(); // Not designed to be reset
    }

    // Interface ISimpleReadStream

    virtual size32_t read(size32_t max_len, void * data)
    {
        size32_t got;
        const void *ptr = peek(max_len, got);
        if (!got)
            return 0;
        if (got > max_len)
            got = max_len;
        memcpy(data, ptr, got);
        skip(got);
        return got;     
    }

    // Interface IDirectReader

    virtual ISimpleReadStream *querySimpleStream()
    {
        return this;
    }

    virtual IThorDiskCallback *queryThorDiskCallback()
    {
        return this;
    }

    virtual unsigned queryFilePart() const
    {
        return thisPartIdx;
    }

    // Interface IThorDiskCallback

    virtual unsigned __int64 getFilePosition(const void *_ptr)
    {
        // MORE - could do with being faster than this!
        assertex(curStream != NULL);
        size32_t dummy;
        return thisFileStartPos + curStream->tell() + ((const char *)_ptr - (const char *)curStream->peek(1, dummy));
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr)
    {
        // MORE - could do with being faster than this!
        assertex(curStream != NULL);
        size32_t dummy;
        return makeLocalFposOffset(thisPartIdx-1, curStream->tell() + (const char *)_ptr - (const char *)curStream->peek(1, dummy));
    }

    virtual unsigned __int64 makeFilePositionLocal(offset_t pos)
    {
        assertex(pos >= thisFileStartPos);
        return makeLocalFposOffset(thisPartIdx-1, pos - thisFileStartPos);
    }

    virtual const char * queryLogicalFilename(const void * row) 
    { 
        return f->queryLogicalFilename(thisPartIdx);
    }

};

class InMemoryIndexManager : implements IInMemoryIndexManager, public CInterface
{
    // manages key selection and rebuilding.

    friend class InMemoryIndexTest;

    SegMonitorArray **tracked;
    unsigned *hits;
    unsigned trackLimit;
    unsigned numTracked;
    CriticalSection trackedCrit;
    CriticalSection activeCrit;
    CriticalSection pendingCrit;
    CriticalSection loadCrit;
    InMemoryIndexSet activeIndexes;  // already built...
    InMemoryIndexSet pendingOrders;  // waiting to build... should really be array of SegMonitorArray but these are easier.

    unsigned recordCount;
    unsigned numKeys;
    offset_t totalSize;
    bool loaded;
    bool loadedIntoMemory;
    bool isOpt;
    IArrayOf<IDirectReader> dataFragments;

    PtrToOffsetMapper baseMap;
    char *fileStart;
    char *fileEnd;

    Linked<IFileIOArray> files;
    StringAttr fileName;

    int compare(SegMonitorArray *l, SegMonitorArray *r)
    {
        unsigned idx = 0;
        for (;;)
        {
            if (l->isItem(idx))
            {
                if (r->isItem(idx))
                {
                    IKeySegmentMonitor &litem = l->item(idx);
                    IKeySegmentMonitor &ritem = r->item(idx);
                    int diff = litem.queryHashCode() - ritem.queryHashCode();
                    if (diff)
                        return diff;
                    diff = litem.getOffset() - ritem.getOffset();
                    if (diff)
                        return diff;
                    diff = litem.getSize() - ritem.getSize();
                    if (diff)
                        return diff;
                    diff = (int) litem.isSigned() - (int) ritem.isSigned();
                    if (diff)
                        return diff;
                    diff = (int) litem.isLittleEndian() - (int) ritem.isLittleEndian();
                    if (diff)
                        return diff;
                    idx++;
                }
                else
                    return 1;
            }
            else if (r->isItem(idx))
                return -1;
            else
                return 0;
        }
    }

    void removeAged(int &insertPoint, unsigned numHits)
    {
        if (numHits)
        {
            // we are merging key stats from multiple sources. Expand as needed.
            trackLimit += trackLimit;
            tracked = (SegMonitorArray **) realloc(tracked, trackLimit * sizeof(tracked[0]));
            hits = (unsigned *) realloc(hits, trackLimit * sizeof(hits[0]));
        }
        else
        {
            unsigned i = 0;
            while (i < numTracked)
            {
                if (hits[i] > 10)
                    hits[i++] -= 10;
                else
                {
                    memmove(&tracked[i], &tracked[i+1], (numTracked - i - 1) * sizeof(tracked[0]));
                    memmove(&hits[i], &hits[i+1], (numTracked - i - 1) * sizeof(hits[0]));
                    if (i < (unsigned)insertPoint)
                        insertPoint--;
                    numTracked--;
                }
            }
        }
    }

    static void listKeys(InMemoryIndexSet &whichSet)
    {
        DBGLOG("New key set (%d keys):", whichSet.length());
        ForEachItemIn(idx, whichSet)
        {
            StringBuffer s;
            DBGLOG("Key %d: %s", idx, whichSet.item(idx).toString(s).str());
        }
    }

    void getTrackedInfo(const char *id, StringBuffer &xml)
    {
        CriticalBlock cb(trackedCrit);
        xml.appendf("<File id='%s' numKeys='%d' fileName='%s'>", id, numKeys, fileName.get());
        for (unsigned i = 0; i < numTracked; i++)
        {
            SegMonitorArray &m = *tracked[i];
            xml.appendf("<FieldSet hits='%d'>\n", hits[i]);
            ForEachItemIn(idx, m)
            {
                IKeySegmentMonitor &seg = m.item(idx);
                xml.appendf("<Field offset='%d' size='%d' isSigned='%d' isLittleEndian='%d'/>\n", seg.getOffset(), seg.getSize(), seg.isSigned(), seg.isLittleEndian());
//              xml.appendf("<Field offset='%d' size='%d' flags='%d'/>\n", seg.getOffset(), seg.getSize(), seg.getFlags());
            }
            xml.append("</FieldSet>\n");
        }
        xml.append("</File>");
    }

    static void addPending(InMemoryIndexSet &keyset, SegMonitorArray *query, unsigned *fieldCounts)
    {
        class SortByFieldCount : implements ICompare
        {
            unsigned *fieldCounts;
        public:
            SortByFieldCount(unsigned *_fieldCounts) : fieldCounts(_fieldCounts)
            {
            }
            virtual int docompare(const void *l,const void *r) const
            {
                IKeySegmentMonitor *ll = (IKeySegmentMonitor *) l;
                IKeySegmentMonitor *rr = (IKeySegmentMonitor *) r;
                int rc = fieldCounts[rr->getOffset()] - fieldCounts[ll->getOffset()];  // descending sort by field weight
                if (!rc)
                    rc = ll->getOffset() - rr->getOffset(); // ascending sort by offset where weights equal
                return rc;
            }
        } byFieldCount(fieldCounts);

        SegMonitorArray key;
        ForEachItemIn(idx, *query)
        {
            IKeySegmentMonitor &item = query->item(idx);
            key.append(*LINK(&item));
        }
        qsortvec((void **) key.getArray(), key.length(), byFieldCount);
        keyset.append(*new InMemoryIndex(key));

        ForEachItemIn(idx1, *query)
        {
            IKeySegmentMonitor &item = query->item(idx1);
            fieldCounts[item.getOffset()] = 0;
        }
    }

    void append(InMemoryIndex &newIndex) // used when testing
    {
        CriticalBlock b(activeCrit);
        activeIndexes.append(newIndex);
    }

    InMemoryIndex &getSpareIndex()
    {
        CriticalBlock b(activeCrit);
        unsigned idx = 0;
        while (idx < activeIndexes.length())
        {
            InMemoryIndex &index = activeIndexes.item(idx);
            if (index.deletePending())
            {
                index.setBuilding();
                return index;
            }
            idx++;
        }
        throwUnexpected(); // Should always be one available 
    }

    InMemoryIndex *nextPending()
    {
        CriticalBlock b(pendingCrit);
        if (pendingOrders.length())
        {
            InMemoryIndex *ret = LINK(&pendingOrders.item(0));
            pendingOrders.remove(0);
            return ret;
        }
        return NULL;
    }

    void processInMemoryKeys(IRecordSize *recordSize, int _numKeys)
    {
        numKeys = _numKeys;
        // If we are building keys, we preload the record pointers for the keys. Don't sort yet though until we know what orders
        // Note - we need to preload at least one set of record pointers (for variable record case) while we have a IRecordSize
        for (unsigned key = 0; key < numKeys; key++)
        {
            Owned<InMemoryIndex> dummy = new InMemoryIndex();
            if (key)
            {
                dummy->load(activeIndexes.item(0));
            }
            else
            {
                for (unsigned pass=0; pass < 2; pass++)
                {
                    dummy->load(fileStart, totalSize, recordSize, pass);
                }
            }
            activeIndexes.append(*dummy.getClear());
        }
        loadedIntoMemory = true;
    }

    bool addInMemoryKeys(int _numNewKeys)
    {
        // this should only be called if we already called processInMemoryKeys - assumes we have added to the activeIndexes - not first creating
        if (activeIndexes.ordinality() == 0)
        {
            DBGLOG("trying to add to activeIndexes, when there were none already existing");
            return false;
        }

        numKeys += _numNewKeys;
        for (int key = 0; key < _numNewKeys; key++)
        {
            Owned<InMemoryIndex> dummy = new InMemoryIndex();
            dummy->load(activeIndexes.item(0));
            activeIndexes.append(*dummy.getClear());
        }
        
        return true;
    }

public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }

    InMemoryIndexManager(bool _isOpt, const char *_fileName) : fileName(_fileName)
    {
        numKeys = 0;
        numTracked = 0;
        recordCount = 0;
        loaded = false;
        loadedIntoMemory = false;
        totalSize = 0;
        trackLimit = MAX_TRACKED;
        tracked = (SegMonitorArray **) malloc(trackLimit * sizeof(tracked[0]));
        hits = (unsigned *) malloc(trackLimit * sizeof(hits[0]));
        for (unsigned i = 0; i < trackLimit; i++)
        {
            tracked[i] = NULL;
            hits[i] = 0;
        }
        keyReporter->addManager(this);
        fileStart = NULL;
        fileEnd = NULL;
        isOpt = _isOpt;
    }

    ~InMemoryIndexManager()
    {
        while (numTracked)
        {
            numTracked--;
            delete(tracked[numTracked]);
        }
        free(tracked);
        free(hits);
        keyReporter->removeManager(this);
        free (fileStart);
    }

    void deserializeCursorPos(MemoryBuffer &mb, InMemoryIndexCursor *cursor);
    virtual IInMemoryIndexCursor *createCursor(const RtlRecord &recInfo);
    bool selectKey(InMemoryIndexCursor *cursor);

    inline const char *queryFileName() const
    {
        return fileName.get();
    }

    void mergeTrackedInfo(const InMemoryIndexManager &from)
    {
        CriticalBlock cb(trackedCrit);
        for (unsigned i = 0; i < from.numTracked; i++)
        {
            SegMonitorArray &m = *from.tracked[i];
            noteQuery(m, from.hits[i]);
        }
    }

    bool buildNextKey()
    {
        // called from IndexBuilder thread
        Owned<InMemoryIndex> pending = nextPending();
        if (pending)
        {
            InMemoryIndex &newIndex = getSpareIndex();
            newIndex.resort(*pending);
            return true;
        }
        return false;
    }

    virtual IDirectReader *createReader(offset_t readPos, unsigned partNo, unsigned numParts)
    {
        if (loadedIntoMemory)
            return new InMemoryDirectReader(readPos, fileStart, fileEnd-fileStart, baseMap, partNo, numParts);
        else
            return new BufferedDirectReader(readPos, files, partNo, numParts);
    }

    StringBuffer &queryId(StringBuffer &ret)
    {
        if (files)
            files->getId(ret);
        return ret;
    }

    virtual void load(IFileIOArray *_files, IRecordSize *recordSize, bool preload, int _numKeys)
    {
        // MORE - if numKeys is greater than previously then we may need to take action here....
        CriticalBlock b(loadCrit);
        if (!loaded)
        {
            files.set(_files);
            totalSize = 0;
            fileEnd = NULL; 
            if (files)
            {
#ifdef _LIMIT_FILECOUNT
                for (unsigned idx=0; idx < _LIMIT_FILECOUNT; idx++)
#else
                for (unsigned idx=0; idx < files->length(); idx++)
#endif
                {
                    if (files->isValid(idx))
                    {
                        offset_t base;
                        Owned<IFileIO> file = files->getFilePart(idx, base);
                        offset_t size = file->size();
                        baseMap.addFragment(fileEnd, size, idx-1, base, 0);
                        if (traceLevel > 6)
                            DBGLOG("File fragment %d size %" I64F "d", idx, size);
                        totalSize += size; // MORE - check for overflow here
                    }
                }
            }
            loaded = true;
        }
        if (preload && !loadedIntoMemory) // loaded but NOT originally seen as preload, lets try to generate keys...
        {
            if ((size_t)totalSize != totalSize)
            {
                IException *E = makeStringException(ROXIE_MEMORY_ERROR, "Preload file is larger than maximum object size");
                EXCLOG(MCoperatorError, E);
                throw E;
            }
            if (traceLevel > 2)
                DBGLOG("Loading in-memory file, size %" I64F "d", totalSize);
            // MORE - 32-bit systems could wrap here if totalSize > 2^32
            fileEnd = fileStart = (char *) malloc((size_t)totalSize);
            if (!fileStart)
            {
                IException *E = MakeStringException(ROXIE_MEMORY_ERROR, "Insufficient memory to preload file");
                EXCLOG(MCoperatorError, E);
                throw E;
            }
            if (files)
            {
#ifdef _LIMIT_FILECOUNT
                for (unsigned idx=0; idx < _LIMIT_FILECOUNT; idx++)
#else
                for (unsigned idx=0; idx < files->length(); idx++)
#endif
                {
                    if (files->isValid(idx))
                    {
                        offset_t base;
                        Owned<IFileIO> file = files->getFilePart(idx, base);
                        offset_t size = file->size();
                        baseMap.addFragment(fileEnd, size, idx-1, base, 1);
                        file->read(0, size, fileEnd); // MORE - size may be > 2^32....
                        fileEnd += size;
                    }
                }
            }
            if (_numKeys > 0)
                processInMemoryKeys(recordSize, _numKeys);
            loadedIntoMemory = true;
        }
        else if (_numKeys > numKeys)  // already in memory, but more in memory keys are requested, so let's try to create them
            addInMemoryKeys(_numKeys);
        // MORE - if already loaded could do consistency check.
    }

    void noteQuery(SegMonitorArray &perfect, unsigned noteHits)
    {
        // note - incoming array should be sorted by offset
        ForEachItemIn(idx, perfect)
        {
            IKeySegmentMonitor &seg = perfect.item(idx);
            if (!(seg.isWild() || seg.isSimple()))
                return;
        }
        CriticalBlock cb(trackedCrit);

        int a = 0;
        int b = numTracked;
        while (a<b)
        {
            int i = a+(b-a)/2;
            SegMonitorArray *m = tracked[i];
            int rc = compare(&perfect, m);
            if (rc==0)
            {
                // MORE - could cache...
                hits[i] += noteHits ? noteHits : 1;
                return;
            }
            else if (rc>0)
                a = i+1;
            else
                b = i;
        }
        // no match. insert at a?
        // if there's room, yes. Otherwise, maybe!
        if (numTracked == trackLimit)
            removeAged(a, noteHits);
        if (numTracked < trackLimit)
        {
            memmove(&tracked[a+1], &tracked[a], (numTracked - a) * sizeof(tracked[0]));
            memmove(&hits[a+1], &hits[a], (numTracked - a) * sizeof(hits[0]));
            SegMonitorArray *newEntry = new SegMonitorArray;
            ForEachItemIn(idx, perfect)
            {
                // MORE - we could share them, then comparison would be faster (pointer compare)
                IKeySegmentMonitor &seg = perfect.item(idx);
                unsigned offset = seg.getOffset();
                unsigned size = seg.getSize();
                bool isSigned = seg.isSigned();
                bool isLittleEndian = seg.isLittleEndian();
                newEntry->append(*createDummyKeySegmentMonitor(offset, size, isSigned, isLittleEndian));
            }
            tracked[a] = newEntry;
            hits[a] = noteHits ? noteHits : 1;
            numTracked++;
        }
    }

    void generateKeys(InMemoryIndexSet &keyset)
    {
        CriticalBlock b(trackedCrit);
        DBGLOG("Regenerating up to %d keys", numKeys);
        unsigned fieldCounts[MAX_FIELD_OFFSET];
        unsigned keyScores[MAX_TRACKED];
        memset(fieldCounts, 0, sizeof(fieldCounts));
        memset(keyScores, (unsigned)-1, sizeof(keyScores));
        unsigned i;
        // 1. generate a list of fields by frequency (weighted by size, perhaps)
        unsigned totalHits = 0;
        for (i = 0; i <numTracked; i++)
        {
            SegMonitorArray *query = tracked[i];
            ForEachItemIn(idx, *query)
            {
                IKeySegmentMonitor &item = query->item(idx);
                fieldCounts[item.getOffset()] += item.getSize() * hits[i];
                totalHits += hits[i];
            }
        }
        DBGLOG("Looking at %d sample queries (%d different)", totalHits, numTracked);
        while (numKeys--)
        {
            // 2. score each tracked query by total field score, and pick the best
            unsigned best = 0;
            unsigned bestIdx;
            for (i = 0; i <numTracked; i++)
            {
                if (keyScores[i] > best) // worth looking at this key...
                {
                    unsigned score = 0;
                    SegMonitorArray *query = tracked[i];
                    ForEachItemIn(idx, *query)
                    {
                        IKeySegmentMonitor &item = query->item(idx);
                        score += fieldCounts[item.getOffset()];
                    }
                    keyScores[i] = score;
                    if (score > best)
                    {
                        best = score;
                        bestIdx = i;
                    }
                }
            }
            if (!best)
                break;
            addPending(keyset, tracked[bestIdx], fieldCounts); // resets fieldCounts of used fields to zero... (A bit dodgy...? We don't cover ALL cases of those fields)
        }
        listKeys(keyset);
    }

    void setNumKeys(unsigned n)
    {
        CriticalBlock b(trackedCrit);
        if (n > numKeys)
            numKeys = n;
    }

    virtual void setKeyInfo(IPropertyTree &indexInfo)
    {
        StringBuffer x;
        toXML(&indexInfo, x);
        DBGLOG("SetKeyInfo merging new index set %s", x.str());

        CriticalBlock b(pendingCrit);
        CriticalBlock b1(activeCrit);
        // NOTE: in order to be sure that we have a consistent set of indexes on each slave (even after a restart) and that an index that is required for a 
        // continuation query is always available, it is important that indexes are only added, never removed. Thus optimizations such as removing an index
        // a.b.c when adding an index a.b cannot be applied.
        Owned<IPropertyTreeIterator> indexes = indexInfo.getElements("FieldSet");
        ForEach(*indexes)
        {
            InMemoryIndex *newOrder = new InMemoryIndex(indexes->query());
            ForEachItemIn(idx, activeIndexes)
            {
                InMemoryIndex &active = activeIndexes.item(idx);
                if (active.equals(*newOrder))
                {
                    newOrder->Release();
                    newOrder = NULL;
                    break;
                }
            }
            if (newOrder)
            {
                pendingOrders.append(*newOrder);
            }
        }
        // It's coded in this slightly odd way to support async index building on separate thread, but we don't use that any more
        while (pendingOrders.length())
            buildNextKey();
    }
};

static IInMemoryIndexManager *emptyManager;

extern IInMemoryIndexManager *getEmptyIndexManager()
{
    return LINK(emptyManager);
}

void KeyReporter::addManager(InMemoryIndexManager *newMan)
{
    CriticalBlock b(managerCrit);
    indexManagers.append(*newMan);
}

void KeyReporter::removeManager(InMemoryIndexManager *newMan)
{
    CriticalBlock b(managerCrit);
    indexManagers.zap(*newMan, true);
}

void KeyReporter::report(StringBuffer &reply, const char *filename, unsigned numKeys)
{
    try
    {
        // Merge all info across slave parts...
        // id should be the same for a given successful LFN resolution

        MapStringToMyClass<InMemoryIndexManager> map;
        ForEachItemIn(idx, indexManagers)
        {
            InMemoryIndexManager &index = indexManagers.item(idx);
            if (!filename || WildMatch(index.queryFileName(), filename, true))
            {
                StringBuffer id;
                index.queryId(id);
                if (id.length())  // the 'empty' file is not worth worrying about since may be commoned between disjoint queries
                {
                    InMemoryIndexManager *tmpIndex = map.getValue(id);
                    if (!tmpIndex)
                    {
                        tmpIndex = new InMemoryIndexManager(true, index.queryFileName());
                        map.setValue(id, tmpIndex);
                    }
                    tmpIndex->mergeTrackedInfo(index);
                }
            }
        }

        HashIterator allIndexes(map);
        for (allIndexes.first(); allIndexes.isValid(); allIndexes.next())
        {
            StringBuffer bestKeyInfo;
            IMapping &cur = allIndexes.query();
            InMemoryIndexManager &index = *map.mapToValue(&cur);
            InMemoryIndexSet newset;
            index.setNumKeys(numKeys);
            index.generateKeys(newset);
            reply.appendf("<SuperFile id='%s'>\n <MemIndex uid='%s'>\n", index.queryFileName(), (char *) cur.getKey());
            ForEachItemIn(key, newset)
                newset.item(key).toXML(reply, 2);
            reply.append(" </MemIndex>\n</SuperFile>\n");
        }
    }
    catch(IException *E)
    {
        EXCLOG(E);
        E->Release();
    }
    catch(...)
    {
        IException *E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught in globalRebuild");
        EXCLOG(E);
        E->Release();
    }
}

extern void reportInMemoryIndexStatistics(StringBuffer &reply, const char *filename, unsigned count)
{
    keyReporter->report(reply, filename, count);
}


class InMemoryIndexCursor : implements IInMemoryIndexCursor, public CInterface
{
    friend class InMemoryIndexTest;
    friend class InMemoryIndexManager;

#ifdef BASED_POINTERS
    const void *base;
#endif
    t_indexentry *ptrs;
    unsigned numPtrs;
    Linked<InMemoryIndex> index;

    SegMonitorArray segMonitors;   // defining the values we are looking for
    SegMonitorArray postFilter;    // ones that did not match the key...
    unsigned maxScore;
    unsigned recSize;
    void *keyBuffer;
    unsigned keySize;
    bool canMatch;
    bool postFiltering;
    PtrToOffsetMapper &baseMap;
    const RtlRecord &recInfo;
    RtlDynRow rowinfo;
    unsigned numSegFieldsRequired = 0;
    unsigned numPostFieldsRequired = 0;

    Owned<IKeySegmentMonitor> lastmatch;
    unsigned lastoffset;
    IKeySegmentMonitor *findKSM(size32_t offset, bool remove)
    {
        // MORE - just assuming an offset match is not really enough
        if (offset==lastoffset)
            return lastmatch.getLink();
        lastoffset = offset;
        int a = 0;
        int b = postFilter.length();
        while (a<b)
        {
            int i = a+(b-a)/2;
            IKeySegmentMonitor *k = &postFilter.item(i);
            int rc = offset-k->getOffset();
            if (rc==0)
            {
                lastmatch.set(k);
                if (remove)
                    postFilter.remove(i);
                return lastmatch.getLink();
            }
            else if (rc>0)
                a = i+1;
            else
                b = i;
        }
        lastmatch.clear();
        return NULL;
    }

    static int compareSegments(IInterface * const *v1, IInterface * const *v2)
    {
        // MORE - just assuming an offset match is not really enough. Use the same code as we did in tracking

        IKeySegmentMonitor *k1 = (IKeySegmentMonitor*) *v1;
        IKeySegmentMonitor *k2 = (IKeySegmentMonitor*) *v2;
        return k1->getOffset() - k2->getOffset();
    }

    unsigned scoreKey(InMemoryIndex &candidate, unsigned scoreToBeat)
    {
        // MORE - we should wildcard where given permission to do so? Wild segments need special care in matching... Or do we remove them before here...
        unsigned score = 0;
        if (candidate.maxScore() > scoreToBeat)
        {
            ForEachItemIn(idx, candidate.segMonitors)
            {
                IKeySegmentMonitor &ksm = candidate.segMonitors.item(idx);
                size32_t o = ksm.getOffset();
                IKeySegmentMonitor *match = findKSM(o, false);
                if (!match)
                    break;  // MORE - could do wildcarding.... should we?
                score += match->getSize();
                if (match->getSize() != ksm.getSize())
                {
                    match->Release();
                    break;  // MORE - could do wildcarding. MORE - size must match exactly if not string... but should never happen?
                    // not sure under what circumstances I would get size mismatches
                }
                match->Release();
            }
        }
        return score;
    }

    void resolve(InMemoryIndex &winner)
    {
        keySize = 0;
        ForEachItemIn(idx, winner.segMonitors)
        {
            IKeySegmentMonitor &ksm = winner.segMonitors.item(idx);
            size32_t o = ksm.getOffset();
            IKeySegmentMonitor *match = findKSM(o, true);
            if (!match)
                break;  // MORE - could do wildcarding....
            segMonitors.append(*match);
            if (match->numFieldsRequired() > numSegFieldsRequired)
                numSegFieldsRequired = match->numFieldsRequired();
            size32_t lim = o+match->getSize();
            if (lim > keySize)
                keySize = lim;
            if (match->getSize() != ksm.getSize())
                break;  // MORE - could do wildcarding. MORE - size must match exactly if not string... but should never happen?
        }
        keyBuffer = malloc(keySize);
        if (index)
            index->unlockRead();
        index.set(&winner);
        winner.lockRead();
#ifdef BASED_POINTERS
        base = index->base;
#endif
        ptrs = index->ptrs;
        numPtrs = index->numPtrs;
    }

    Linked<InMemoryIndexManager> manager;

    // ICompare
    virtual int docompare(const void *l, const void *r) const
    {
        ForEachItemIn(idx, segMonitors)
        {
            int ret = segMonitors.item(idx).docompare(l, r);
            if (ret)
                return ret;
        }
        return 0;
    }

    bool selectKey()
    {
        if (canMatch && inMemoryKeysEnabled) // no point picking keys if it can't or if we already did...
            return manager->selectKey(this);
        else
            return false;
    }

    void setLow(unsigned segno) const
    {
        unsigned lim = segMonitors.length();
        while (segno < lim)
            segMonitors.item(segno++).setLow(keyBuffer);
    }

    void endRange(unsigned segno) const
    {
        unsigned lim = segMonitors.length();
        while (segno < lim)
            segMonitors.item(segno++).endRange(keyBuffer);
    }

    bool incrementKey(unsigned segno, const void *rowval) const
    {
        // Increment the key buffer to next acceptable value after rowval
        for(;;)
        {
            IKeySegmentMonitor &seg = segMonitors.item(segno);
            if (rowval)
                seg.copy(keyBuffer, rowval);
            if (seg.increment(keyBuffer))
            {
                setLow(segno+1);
                return true;
            }
            if (!segno)
                return false;
            segno--;
        }
    }

    bool matches(const RtlRow *r, const SegMonitorArray &segs) const
    {
        ForEachItemIn(idx, segs)
        {
            if (!segs.item(idx).matches(r))
                return false;
        }
        return true;
    }

    unsigned firstInRange(unsigned a) const
    {
        if (!segMonitors.length())
            return a;
        for (;;)
        {
            // binchop...
            int b = numPtrs;
            int rc;
            while ((int)a<b)
            {
                int i = a+(b-a)/2;
                rc = docompare(keyBuffer, GETROW(i));
                if (rc>0)
                    a = i+1;
                else
                    b = i;
            }
            if (a<numPtrs)
            {
                const void *row = GETROW(a);
                unsigned seg = 0;
                unsigned lim = segMonitors.length();
                for (;;)
                {
                    if (!segMonitors.item(seg).matchesBuffer(row))  // Technically should be using RtlRow here but we don't support varoffset in indexes yet
                        break;
                    seg++;
                    if (seg==lim)
                        return a;
                }
                if (!incrementKey(seg, row))
                    break;
            }
            else
                break;
        }
        return (unsigned) -1;
    }

    unsigned lastInRange(unsigned start) const
    {
        if (!segMonitors.length())
            return numPtrs-1;
        endRange(segMonitors.length()-1);

        // binchop...
        int b = numPtrs;
        int rc;
        unsigned a = start;
        while ((int)a<b)
        {
            int i = a+(b+1-a)/2;
            rc = docompare(keyBuffer, GETROW(i-1));
            if (rc>=0)
                a = i;
            else
                b = i-1;
        }
        if (a>start)
            a--;
        return a;
    }

    unsigned midx;
    unsigned lidx;
    bool eof;

    bool nextRange()
    {
        if (!segMonitors.length() || !incrementKey(segMonitors.length()-1, NULL))
            return false;
        midx = firstInRange(lidx+1);
        if (midx != -1)
        {
            lidx = lastInRange(midx);
            return true;
        }
        return false;
    }

public:
    IMPLEMENT_IINTERFACE; 

    InMemoryIndexCursor(InMemoryIndexManager *_manager, PtrToOffsetMapper &_baseMap, const RtlRecord &_recInfo) : manager(_manager), baseMap(_baseMap), recInfo(_recInfo), rowinfo(_recInfo)
    {
#ifdef BASED_POINTERS
        base = NULL;
#endif
        ptrs = NULL;
        keyBuffer = NULL;
        keySize = 0;
        numPtrs = 0;
        recSize = 0;
        maxScore = 0;
        canMatch = true;
        postFiltering = true;
        midx = 1;
        lidx = 0;
        lastoffset = (unsigned) -1;
        eof = false;
    }

    ~InMemoryIndexCursor()
    {
        if (index)
            index->unlockRead();
        free(keyBuffer);
    }

    // IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment)
    {
        if (segment->isEmpty())
            canMatch = false;
        if (canMatch)
        {
            if (!segment->isWild())
            {
                maxScore += segment->getSize();
                IInterface *val = LINK(segment);
                bool added;
                // Unless and until resolve is called, all segmonitors are postfilter...
                postFilter.bAdd(val, compareSegments, added);
                if (segment->numFieldsRequired() > numPostFieldsRequired)
                    numPostFieldsRequired = segment->numFieldsRequired();
                assertex(added);
            }
        }
        segment->Release();
    }

    virtual void setMergeBarrier(unsigned)
    {
        // we don't merge segmonitors - nothing to do
    }

    virtual unsigned ordinality() const
    {
        return segMonitors.length();
    }

    virtual IKeySegmentMonitor *item(unsigned idx) const
    {
        if (segMonitors.isItem(idx))
            return &segMonitors.item(idx);
        else
            return NULL;
    }

    virtual void reset()
    {
        if (canMatch)
        {
            postFiltering = postFilter.length() != 0;
            eof = false;
            setLow(0);
            midx = firstInRange(0);
            if (numPtrs && midx != -1)
                lidx = lastInRange(midx);
            else
            {
                midx = 1;
                lidx = 0;
                eof = true;
            }
        }
        else
        {
            midx = 1;
            lidx = 0;
            eof = true;
        }
    }

    virtual const void *nextMatch()
    {
        for (;;)
        {
            const void *row;
            if (midx <= lidx)
                row = GETROW(midx++);
            else 
            {
                if (eof || !nextRange())
                {
                    eof = true;
                    return NULL;
                }
                row = GETROW(midx++);
            }
            if (!postFiltering)
                return row;
            rowinfo.setRow(row, numPostFieldsRequired);
            if (matches(&rowinfo, postFilter))
                return row;
        }
    }

    virtual bool isFiltered(const void *row)
    {
        if (!canMatch)
            return true;
        if (!postFiltering)
            return false;
        rowinfo.setRow(row, numPostFieldsRequired);
        return !matches(&rowinfo, postFilter);
    }

    virtual unsigned __int64 getFilePosition(const void *_ptr)
    {
        return baseMap.ptrToFilePosition(_ptr);
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr)
    {
        return baseMap.ptrToLocalFilePosition(_ptr);
    }

    virtual const char * queryLogicalFilename(const void * row) 
    { 
        UNIMPLEMENTED;
    }

    virtual void serializeCursorPos(MemoryBuffer &mb) const 
    {
        mb.append(keySize);
        mb.append(keySize, keyBuffer);
        index->serializeCursorPos(mb);
        mb.append(midx);
        mb.append(lidx);
        mb.append(eof);
    }

    virtual void deserializeCursorPos(MemoryBuffer &mb)
    {
        mb.read(keySize);
        assertex(!keyBuffer);
        keyBuffer = malloc(keySize);
        mb.read(keySize, keyBuffer);
        manager->deserializeCursorPos(mb, this);
        mb.read(midx);
        mb.read(lidx);
        mb.read(eof);
    }
};

IInMemoryIndexCursor *InMemoryIndexManager::createCursor(const RtlRecord &recInfo)
{
    return new InMemoryIndexCursor(this, baseMap, recInfo);
}

void InMemoryIndexManager::deserializeCursorPos(MemoryBuffer &mb, InMemoryIndexCursor *cursor)
{
    StringBuffer indexSig;
    mb.read(indexSig);
    ForEachItemIn(idx, activeIndexes)
    {
        InMemoryIndex &thisIndex = activeIndexes.item(idx);
        if (thisIndex.available())
        {
            StringBuffer sig;
            thisIndex.toString(sig);
            if (strcmp(sig, indexSig)==0)
            {
                cursor->resolve(thisIndex);
                return;
            }
        }
    }
    throwUnexpected();
}

bool InMemoryIndexManager::selectKey(InMemoryIndexCursor *cursor)
{
    noteQuery(cursor->postFilter, 0);
    unsigned best = 0;

    CriticalBlock b(activeCrit);
    InMemoryIndex *bestIndex = NULL;
    if (traceLevel > 5)
    {
        StringBuffer s;
        describeSegmonitors(cursor->postFilter, s);
        DBGLOG("Looking for a key %s", s.str());
    }
    ForEachItemIn(idx, activeIndexes)
    {
        InMemoryIndex &thisIndex = activeIndexes.item(idx);
        if (thisIndex.available())
        {
            unsigned score = cursor->scoreKey(thisIndex, best);
            if (score > best)
            {
                bestIndex = &thisIndex;
                best = score;
            }
            if (score==cursor->maxScore)
                break;
        }
    }
    if (bestIndex)
    {
        if (traceLevel > 5)
        {
            StringBuffer s;
            DBGLOG("Selected a key %s", bestIndex->toString(s).str());
        }
        cursor->resolve(*bestIndex);
        return true;
    }
    else
        return false;
}

extern IInMemoryIndexManager *createInMemoryIndexManager(bool isOpt, const char *fileName)
{
    return new InMemoryIndexManager(isOpt, fileName);
}

// Initialization/termination

MODULE_INIT(INIT_PRIORITY_STANDARD+10)
{
    keyReporter = new KeyReporter;
    emptyManager = new InMemoryIndexManager(true, NULL);
    return true;
}

MODULE_EXIT()
{ 
    ::Release(emptyManager);
    delete keyReporter;
}

//=======================================================================================================

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class InMemoryIndexTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( InMemoryIndexTest );
        CPPUNIT_TEST(test1);
        CPPUNIT_TEST(testResolve);
        CPPUNIT_TEST(testStatistician);
        CPPUNIT_TEST(testPtrToOffsetMapper);
    CPPUNIT_TEST_SUITE_END();

protected:
    void test1()
    {
        class irs : public CInterface, implements IRecordSize
        {
        public:
            IMPLEMENT_IINTERFACE;
            virtual size32_t getRecordSize(const void *) { return sizeof(unsigned); }
            virtual size32_t getFixedSize() const { return sizeof(unsigned); }
            virtual size32_t getMinRecordSize() const { return sizeof(unsigned); }
        } x;
        RtlIntTypeInfo ty1(type_int|type_unsigned, sizeof(unsigned));
        RtlFieldInfo f1("f1", nullptr, &ty1);
        const RtlFieldInfo * const fields [] = {&f1, nullptr};
        RtlRecord dummy(fields, true);
        unsigned testarray[] = {1,2,2,2,4,3,8,9,0,5};
        InMemoryIndex di;
        di.load(testarray, sizeof(testarray), &x, 0);
        di.load(testarray, sizeof(testarray), &x, 1);
        unsigned searchval = 1;
        InMemoryIndex order;
        order.append(createSingleKeySegmentMonitor(false, 0, sizeof(unsigned), &searchval));
        di.setBuilding();
        di.resort(order);
        InMemoryIndexManager indexes(false, "test1");
        indexes.append(*LINK(&di));

        Owned<IInMemoryIndexCursor> d = indexes.createCursor(dummy);
        d->append(createSingleKeySegmentMonitor(false, 0, sizeof(unsigned), &searchval));
        d->selectKey();
        d->reset();
        ASSERT(d->nextMatch()==&testarray[0]);
        ASSERT(d->nextMatch()==NULL);
        ASSERT(d->nextMatch()==NULL);

        d.setown(indexes.createCursor(dummy));
        searchval = 10;
        d->append(createSingleKeySegmentMonitor(false, 0, sizeof(unsigned), &searchval));
        d->selectKey();
        d->reset();
        ASSERT(d->nextMatch()==NULL);
        ASSERT(d->nextMatch()==NULL);
        d.clear();
        di.deprecate();

        Owned<IPropertyTree> keyInfo = createPTreeFromXMLString("<R><FieldSet><Field isLittleEndian='1' isSigned='0' offset='0' size='4'/><Field isLittleEndian='1' isSigned='1' offset='0' size='4'/></FieldSet></R>");
        indexes.setKeyInfo(*keyInfo.get());
        Sleep(1000); // to give key time to build!

        d.setown(indexes.createCursor(dummy));
        IStringSet *set = createStringSet(sizeof(unsigned));
        searchval = 2;
        set->addRange(&searchval, &searchval);
        searchval = 9;
        set->addRange(&searchval, &searchval);
        d->append(createKeySegmentMonitor(false, set, 0, sizeof(unsigned)));
        d->selectKey();
        d->reset();
        ASSERT(*(int *) d->nextMatch()==2);
        ASSERT(*(int *) d->nextMatch()==2);
        ASSERT(*(int *) d->nextMatch()==2);
        ASSERT(*(int *) d->nextMatch()==9);
        ASSERT(d->nextMatch()==NULL);

    }

    void testResolve()
    {
        InMemoryIndex di;
        unsigned searchval = 1;
        RtlIntTypeInfo ty1(type_int|type_unsigned, sizeof(unsigned));
        RtlFieldInfo f1("f1", nullptr, &ty1);
        const RtlFieldInfo * const fields [] = {&f1, nullptr};
        RtlRecord dummy(fields, true); // NOTE - not accurate but good enough for these tests

        IKeySegmentMonitor *ksm = createSingleLittleKeySegmentMonitor(false, 8, sizeof(unsigned), &searchval);
        ASSERT(ksm->isSigned()==false);
        ASSERT(ksm->isLittleEndian()==true);
        di.append(ksm);
        di.append(createSingleKeySegmentMonitor(false, 4, sizeof(unsigned), &searchval));
        di.append(createSingleKeySegmentMonitor(false, 0, sizeof(unsigned), &searchval));
        di.setBuilding();
        di.undeprecate();

        InMemoryIndex di2;
        di2.append(createSingleKeySegmentMonitor(false, 12, sizeof(unsigned), &searchval));
        di2.append(createSingleKeySegmentMonitor(false, 16, sizeof(unsigned), &searchval));
        di2.append(createSingleKeySegmentMonitor(false, 0, sizeof(unsigned), &searchval));
        di2.setBuilding();
        di2.undeprecate();
        
        InMemoryIndexManager indexes(false, "test2");
        indexes.append(*LINK(&di));
        indexes.append(*LINK(&di2));

        Owned<IInMemoryIndexCursor> dd = indexes.createCursor(dummy);
        InMemoryIndexCursor *d = QUERYINTERFACE(dd.get(), InMemoryIndexCursor);
        ASSERT(d != nullptr);
        dd->append(createSingleKeySegmentMonitor(false, 4, sizeof(unsigned), &searchval));
        dd->append(createSingleKeySegmentMonitor(false, 8, sizeof(unsigned), &searchval));
        ASSERT(d->postFilter.length()==2);
        ASSERT(d->segMonitors.length()==0);
        ASSERT(d->postFilter.item(0).getOffset()==4);
        ASSERT(d->postFilter.item(1).getOffset()==8);
        dd->selectKey();
        ASSERT(d->postFilter.length()==0);
        ASSERT(d->segMonitors.length()==2);
        ASSERT(d->segMonitors.item(0).getOffset()==8);
        ASSERT(d->segMonitors.item(1).getOffset()==4);

        dd.setown(indexes.createCursor(dummy));
        d = QUERYINTERFACE(dd.get(), InMemoryIndexCursor);
        ASSERT(d != nullptr);
        dd->append(createSingleKeySegmentMonitor(false, 16, sizeof(unsigned), &searchval));
        dd->append(createSingleKeySegmentMonitor(false, 8, sizeof(unsigned), &searchval));
        ASSERT(d->postFilter.length()==2);
        ASSERT(d->segMonitors.length()==0);
        ASSERT(d->postFilter.item(0).getOffset()==8);
        ASSERT(d->postFilter.item(1).getOffset()==16);
        dd->selectKey();
        ASSERT(d->postFilter.length()==1);
        ASSERT(d->segMonitors.length()==1);
        ASSERT(d->segMonitors.item(0).getOffset()==8);
        ASSERT(d->postFilter.item(0).getOffset()==16);
    
        dd.setown(indexes.createCursor(dummy));
        d = QUERYINTERFACE(dd.get(), InMemoryIndexCursor);
        ASSERT(d != nullptr);
        dd->append(createSingleKeySegmentMonitor(false, 12, sizeof(unsigned), &searchval));
        dd->append(createSingleKeySegmentMonitor(false, 16, sizeof(unsigned), &searchval));
        dd->append(createSingleKeySegmentMonitor(false, 8, sizeof(unsigned), &searchval));
        ASSERT(d->postFilter.length()==3);
        ASSERT(d->segMonitors.length()==0);
        ASSERT(d->postFilter.item(0).getOffset()==8);
        ASSERT(d->postFilter.item(1).getOffset()==12);
        ASSERT(d->postFilter.item(2).getOffset()==16);
        dd->selectKey();
        ASSERT(d->postFilter.length()==1);
        ASSERT(d->segMonitors.length()==2);
        ASSERT(d->segMonitors.item(0).getOffset()==12);
        ASSERT(d->segMonitors.item(1).getOffset()==16);
        ASSERT(d->postFilter.item(0).getOffset()==8);
    }

    void testStatistician()
    {
        InMemoryIndexManager stats(false, "test3");

        SegMonitorArray q;
        unsigned char searchval;
        q.append(*createSingleKeySegmentMonitor(false, 'p', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'q', 1, &searchval));
        unsigned i;
        for (i = 0; i < 104; i++)
            stats.noteQuery(q, 0);

        q.kill();
        q.append(*createSingleKeySegmentMonitor(false, 'a', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'b', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'x', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'y', 1, &searchval));
        for (i = 0; i < 103; i++)
            stats.noteQuery(q, 0);

        q.kill();
        q.append(*createSingleKeySegmentMonitor(false, 'c', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'd', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'x', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'y', 1, &searchval));
        for (i = 0; i < 102; i++)
            stats.noteQuery(q, 0);

        q.kill();
        q.append(*createSingleKeySegmentMonitor(false, 'e', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'f', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'x', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'y', 1, &searchval));
        for (i = 0; i < 101; i++)
            stats.noteQuery(q, 0);

        q.kill();
        q.append(*createSingleKeySegmentMonitor(false, 'g', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'h', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'x', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'y', 1, &searchval));
        for (i = 0; i < 100; i++)
            stats.noteQuery(q, 0);

        q.kill();
        q.append(*createSingleKeySegmentMonitor(false, 'g', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'x', 1, &searchval));
        q.append(*createSingleKeySegmentMonitor(false, 'y', 1, &searchval));
        for (i = 0; i < 100; i++)
            stats.noteQuery(q, 0);

        ASSERT(stats.numTracked == 6);

        InMemoryIndexSet keys;
        stats.numKeys = 6;
        stats.generateKeys(keys);
        ASSERT(keys.length()==5); // g.x.y is covered by x.y.g.h
        ASSERT(keys.item(0).segMonitors.length()==4);
        ASSERT(keys.item(0).segMonitors.item(0).getOffset()=='x');
        ASSERT(keys.item(0).segMonitors.item(1).getOffset()=='y');
        ASSERT(keys.item(0).segMonitors.item(2).getOffset()=='g');
        ASSERT(keys.item(0).segMonitors.item(3).getOffset()=='h');

        ASSERT(keys.item(1).segMonitors.length()==2);
        ASSERT(keys.item(1).segMonitors.item(0).getOffset()=='p');
        ASSERT(keys.item(1).segMonitors.item(1).getOffset()=='q');

        ASSERT(keys.item(2).segMonitors.length()==4);
        ASSERT(keys.item(2).segMonitors.item(0).getOffset()=='a');
        ASSERT(keys.item(2).segMonitors.item(1).getOffset()=='b');
        ASSERT(keys.item(2).segMonitors.item(2).getOffset()=='x');
        ASSERT(keys.item(2).segMonitors.item(3).getOffset()=='y');

        ASSERT(keys.item(3).segMonitors.length()==4);
        ASSERT(keys.item(3).segMonitors.item(0).getOffset()=='c');
        ASSERT(keys.item(3).segMonitors.item(1).getOffset()=='d');
        ASSERT(keys.item(3).segMonitors.item(2).getOffset()=='x');
        ASSERT(keys.item(3).segMonitors.item(3).getOffset()=='y');

        ASSERT(keys.item(4).segMonitors.length()==4);
        ASSERT(keys.item(4).segMonitors.item(0).getOffset()=='e');
        ASSERT(keys.item(4).segMonitors.item(1).getOffset()=='f');
        ASSERT(keys.item(4).segMonitors.item(2).getOffset()=='x');
        ASSERT(keys.item(4).segMonitors.item(3).getOffset()=='y');
    }

    void testPtrToOffsetMapper()
    {
        PtrToOffsetMapper p;
        for (unsigned pass = 0; pass < 2; pass++)
        {
            char *base = (char *) 0x10000;
            offset_t filepos = 0;
            unsigned size = 0x100;
            for (unsigned partno = 0; partno < 47; partno++)
            {
                if (partno % 2)
                {
                    p.addFragment(base, size, partno, filepos, pass);
                    base += size;
                    filepos += size*2;
                }
            }
        }
        ASSERT(p.ptrToFilePosition((void *) 0x10000) == 0);
    }
};

class StringSetTest : public CppUnit::TestFixture  
{
    // Should really be in jset but did not want to have jlib dependent on cppunit
    CPPUNIT_TEST_SUITE( StringSetTest );
        CPPUNIT_TEST(testPathogenic);
        CPPUNIT_TEST(test1);
        CPPUNIT_TEST(test2);
        CPPUNIT_TEST(test3);
        CPPUNIT_TEST(testStrange);
        CPPUNIT_TEST(testUnionIntersect);
    CPPUNIT_TEST_SUITE_END();

protected:
    void test1()
    {
        for (int i = -1000; i < 1000; i++)
            for (int j = -1; j < 2; j++)
            {
                int d1;

                d1 = memcmplittlesigned(&i, &j, sizeof(i));
                ASSERT((d1 < 0) == (i < j));
                ASSERT((d1 == 0) == (i == j));
                ASSERT((d1 > 0) == (i > j));

                int i1 = i;
                _WINREV(i1);
                int j1 = j;
                _WINREV(j1);
                d1 = memcmpbigsigned(&i1, &j1, sizeof(i1));
                ASSERT((d1 < 0) == (i < j));
                ASSERT((d1 == 0) == (i == j));
                ASSERT((d1 > 0) == (i > j));

                unsigned int i2 = i;
                unsigned int j2 = j;
                d1 = memcmplittleunsigned(&i2, &j2, sizeof(i2));
                ASSERT((d1 < 0) == (i2 < j2));
                ASSERT((d1 == 0) == (i2 == j2));
                ASSERT((d1 > 0) == (i2 > j2));

                unsigned int i3 = i2;
                _WINREV(i3);
                int j3 = j2;
                _WINREV(j3);
                d1 = memcmp(&i3, &j3, sizeof(i3));
                ASSERT((d1 < 0) == (i2 < j2));
                ASSERT((d1 == 0) == (i2 == j2));
                ASSERT((d1 > 0) == (i2 > j2));
            }

    }

    void test2()
    {
        // test some simple unions
        Owned<IStringSet> slu1 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        Owned<IStringSet> slu2 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        unsigned i=6, j=10;
        slu1->addRange(&i, &j);
        i=0, j=5;
        slu2->addRange(&i, &j);
        Owned<IStringSet> slu = slu1->unionSet(slu2);
        for (i = 0; i < 10; i++)
        {
            unsigned transition;
            unsigned transitionValue;
            bool b = slu->inRange(&i, transition);
            slu->getTransitionValue(&transitionValue, transition);
            ASSERT(b==true);
            ASSERT(transitionValue==10); // check that they merged
        }

        Owned<IStringSet> slu3 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        Owned<IStringSet> slu4 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        i=7, j=7;
        slu3->addRange(&i, &j);
        i=4, j=5;
        slu4->addRange(&i, &j);
        slu.setown(slu3->unionSet(slu4));
        for (i = 0; i < 10; i++)
        {
            unsigned transition;
            unsigned transitionValue;
            bool b = slu->inRange(&i, transition);
            slu->getTransitionValue(&transitionValue, transition);
            if (i<4)
            {
                ASSERT(b==false);
                ASSERT(transitionValue==4);
            }
            else if (i<6)
            {
                ASSERT(b==true);
                ASSERT(transitionValue==5);
            }
            else if (i<7)
            {
                ASSERT(b==false);
                ASSERT(transitionValue==7);
            }
            else if (i<8)
            {
                ASSERT(b==true);
                ASSERT(transitionValue==7);
            }
            else
            {
                ASSERT(b==false);
                ASSERT(transition==(unsigned)-1);
            }
        }
    
    }

    void test3()
    {
        // test various ways to create the same set...
        Owned<IStringSet> slu = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        unsigned i;
        for (i = 6; i < 10; i++)
            slu->addRange(&i, &i);
        for (i = 0; i < 20; i+=2)
            slu->addRange(&i, &i);
        for (i = 0; i < 6; i++)
            slu->addRange(&i, &i);
        i = 30;
        unsigned j = 40;
        slu->addRange(&i, &j);
        i = 43;
        j = 44;
        slu->addRange(&i, &j);
        checkValues3(slu);

        Owned<IStringSet> slu1 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        for (i = 6; i < 10; i++)
            slu1->addRange(&i, &i);
        Owned<IStringSet> slu2 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        for (i = 0; i < 20; i+=2)
            slu2->addRange(&i, &i);
        Owned<IStringSet> slu3 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        for (i = 0; i < 6; i++)
            slu3->addRange(&i, &i);
        i = 30;
        j = 40;
        Owned<IStringSet> slu4 = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        slu4->addRange(&i, &j);
        i = 43;
        j = 44;
        slu4->addRange(&i, &j);

        slu.setown(slu4->unionSet(slu3));
        slu3.setown(slu2->unionSet(slu1));
        slu4.setown(slu3->unionSet(slu));
        checkValues3(slu4);

        
        slu.setown(createStringSet(sizeof(int), false, false));
        slu->addAll();
        for (i = 11; i < 20; i+=2)
            slu->killRange(&i, &i);
        i = 20;
        j = 29;
        slu->killRange(&i, &j);
        i = 41;
        j = 42;
        slu->killRange(&i, &j);
        i = 45;
        slu->killRange(&i, NULL);
        checkValues3(slu);


        slu.setown(createStringSet(sizeof(int), false, false));
        slu->addAll();
        for (i = 11; i < 20; i+=2)
            slu->killRange(&i, &i);
        i = 20;
        j = 29;
        slu2.setown(createStringSet(sizeof(int), false, false));
        slu2->addAll();
        slu2->killRange(&i, &j);
        i = 41;
        j = 42;
        slu3.setown(createStringSet(sizeof(int), false, false));
        slu3->addAll();
        slu3->killRange(&i, &j);
        i = 45;
        slu4.setown(createStringSet(sizeof(int), false, false));
        slu4->addAll();
        slu4->killRange(&i, NULL);
        slu1.setown(slu->intersectSet(slu2));
        slu2.setown(slu3->intersectSet(slu4));
        slu.setown(slu1->intersectSet(slu2));
        checkValues3(slu);

    }

    void checkValues3(IStringSet *slu)
    {
        for (unsigned i = 0; i < 50; i++)
        {
            unsigned transition;
            unsigned transitionValue;
            bool b = slu->inRange(&i, transition);
            slu->getTransitionValue(&transitionValue, transition);
            if (i < 10)
            {
                ASSERT(b==true);
                ASSERT(transitionValue==10); // check that they merged
            }
            else if (i < 19)
            {
                ASSERT(b==((i & 1) == 0));
                ASSERT(transitionValue==((i+1) & 0xfffe));
            }
            else if (i < 30)
            {
                ASSERT(b==false);
                ASSERT(transitionValue==30);
            }
            else if (i <= 40)
            {
                ASSERT(b==true);
                ASSERT(transitionValue==40);
            }
            else if (i < 43)
            {
                ASSERT(b==false);
                ASSERT(transitionValue==43);
            }
            else if (i <= 44)
            {
                ASSERT(b==true);
                ASSERT(transitionValue==44);
            }
            else
            {
                ASSERT(b==false);
                ASSERT(transition==(unsigned)-1);
            }
        }

    }

    void testStrange()
    {
        for (unsigned i = 0; i < 2; i++)
            for (unsigned j = 0; i < 2; i++)
            {
                Owned<IStringSet> slu = createStringSet(sizeof(int), i==0, j==0);
                ASSERT(slu->isEmptySet());
                ASSERT(!slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                Owned<IStringSet> inverse = slu->invertSet();
                ASSERT(!inverse->isEmptySet());
                ASSERT(inverse->isFullSet());
                ASSERT(!slu->isSingleValue());

                slu->addAll();
                ASSERT(!slu->isEmptySet());
                ASSERT(slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                inverse.setown(slu->invertSet());
                ASSERT(inverse->isEmptySet());
                ASSERT(!inverse->isFullSet());
                ASSERT(!inverse->isSingleValue());

                int x = 0x6543;
                slu->killRange(&x, &x);
                ASSERT(!slu->isEmptySet());
                ASSERT(!slu->isFullSet());
                ASSERT(!slu->isSingleValue());
                inverse.setown(slu->invertSet());
                ASSERT(!inverse->isEmptySet());
                ASSERT(!inverse->isFullSet());
                ASSERT(inverse->isSingleValue());
                inverse->killRange(&x, &x);
                ASSERT(inverse->isEmptySet());
                ASSERT(!inverse->isFullSet());
                ASSERT(!inverse->isSingleValue());

                slu->addRange(&x, &x);
                ASSERT(!slu->isEmptySet());
                ASSERT(slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                slu->addRange(&x, &x);  // adding to full should still be full
                ASSERT(!slu->isEmptySet());
                ASSERT(slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                inverse->killRange(&x, &x); // removing from empty should still be empty
                ASSERT(inverse->isEmptySet());
                ASSERT(!inverse->isFullSet());
                ASSERT(!inverse->isSingleValue());
                inverse->addRange(&x, &x);
                ASSERT(!inverse->isEmptySet());
                ASSERT(!inverse->isFullSet());
                ASSERT(inverse->isSingleValue());

                slu->killRange(&x, NULL);
                ASSERT(!slu->isEmptySet());
                ASSERT(!slu->isFullSet());
                ASSERT(!slu->isSingleValue());
                slu->addRange(&x, NULL);
                ASSERT(!slu->isEmptySet());
                ASSERT(slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                slu->killRange(&x, NULL);
                slu->killRange(NULL, &x);
                ASSERT(slu->isEmptySet());
                ASSERT(!slu->isFullSet());
                ASSERT(!slu->isSingleValue());

                slu->addRange(&x, NULL);
                slu->addRange(NULL, &x);
                ASSERT(!slu->isEmptySet());
                ASSERT(slu->isFullSet());
                ASSERT(!slu->isSingleValue());
            }
    }

    void testPathogenic()
    {
        IStringSet *set = createStringSet(sizeof(unsigned));
        for (unsigned i = 0; i < 10000; i+= 2)
        {
            unsigned val = i;
            _WINREV(val);
            set->addRange(&val, &val);
        }
        ASSERT(set->numValues() == 5000);
    }

    void testUnionIntersect()
    {
        // test some simple unions using the rtl functions with tests for special casing for all/none.
        Owned<IStringSet> setx = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        Owned<IStringSet> sety = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        Owned<IStringSet> setall = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        Owned<IStringSet> setnone = createStringSet(sizeof(int), false, false); // little-endian, unsigned
        unsigned i=6, j=10;
        setx->addRange(&i, &j);
        i=0, j=5;
        sety->addRange(&i, &j);
        setall->addAll();

        Owned<IStringSet> s1 = rtlUnionSet(setnone, sety);
        ASSERT(s1 == sety);
        Owned<IStringSet> s2 = rtlUnionSet(setall, sety);
        ASSERT(s2 == setall);
        Owned<IStringSet> s3 = rtlUnionSet(setx, setnone);
        ASSERT(s3 == setx);
        Owned<IStringSet> s4 = rtlUnionSet(setx, setall);
        ASSERT(s4 == setall);

        Owned<IStringSet> s5 = rtlIntersectSet(setnone, sety);
        ASSERT(s5 == setnone);
        Owned<IStringSet> s6 = rtlIntersectSet(setall, sety);
        ASSERT(s6 == sety);
        Owned<IStringSet> s7 = rtlIntersectSet(setx, setnone);
        ASSERT(s7 == setnone);
        Owned<IStringSet> s8 = rtlIntersectSet(setx, setall);
        ASSERT(s8 == setx);

        unsigned search = 6;
        Owned<IStringSet> suxy = rtlUnionSet(setx, sety);
        unsigned transition;
        bool b = suxy->inRange(&search, transition);
        ASSERT(b==true);

        Owned<IStringSet> sixy = rtlIntersectSet(setx, sety);
        b = sixy->inRange(&search, transition);
        ASSERT(b==false);
    }

};


CPPUNIT_TEST_SUITE_REGISTRATION( StringSetTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( StringSetTest, "StringSetTest" );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( InMemoryIndexTest, "InMemoryIndexTest" );
CPPUNIT_TEST_SUITE_REGISTRATION( InMemoryIndexTest );

#endif

