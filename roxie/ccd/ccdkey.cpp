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
#include "rtlnewkey.hpp"
#include "rtldynfield.hpp"

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

    offset_t makeFilePositionLocal(offset_t pos) const
    {
        FragmentInfo &frag = findBase(pos);
        return makeLocalFposOffset(frag.partNo, pos - frag.baseOffset);
    }

    void splitPos(offset_t &offset, offset_t &size, unsigned partNo, unsigned numParts) const
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

static UnexpectedVirtualFieldCallback unexpectedVirtualFieldCallback;
class InMemoryIndexCursor;
class InMemoryIndexTest;

#ifdef BASED_POINTERS
typedef unsigned t_indexentry;
#define GETROW(a) ((void *) ((char *)base+ptrs[a]))
#else
typedef const void *t_indexentry;
#define GETROW(a) (ptrs[a])
#endif

class InMemoryIndex : public CInterface, implements IInterface, implements ICompare
{
    // A list of pointers to all the records in a memory-loaded disk file, ordered by a field/fields in the file

    friend class InMemoryIndexManager;
    friend class InMemoryIndexCursor;
    friend class InMemoryIndexTest;

    UnsignedArray sortFields;   // defining the sort order
#ifdef BASED_POINTERS
    const void *base = nullptr;
#endif
    t_indexentry *ptrs = nullptr;
    unsigned numPtrs = 0;
    unsigned maxPtrs = 0;
    unsigned totalScore = 0;
    CriticalSection stateCrit;
    const RtlRecord &recInfo;

public:
    IMPLEMENT_IINTERFACE;

    InMemoryIndex(const RtlRecord &_recInfo, UnsignedArray &filters) : recInfo(_recInfo)
    {
        ForEachItemIn(idx, filters)
            append(filters.item(idx));
    }

    InMemoryIndex(const RtlRecord &_recInfo, IPropertyTree &x) : recInfo(_recInfo)
    {
        Owned<IPropertyTreeIterator> fields = x.getElements("Field");
        ForEach(*fields)
        {
            IPropertyTree &field = fields->query();
            const char *fieldname = field.queryProp("@name");
            if (!fieldname || !*fieldname)
                throw MakeStringException(0, "Invalid MemIndex specification - missing field name");
            unsigned fieldNum = recInfo.getFieldNum(fieldname);
            if (fieldNum == (unsigned) -1)
            {
                StringBuffer s;
                for (unsigned idx = 0; idx <  recInfo.getNumFields(); idx++)
                    s.append(',').append(recInfo.queryName(idx));
                if (!s.length())
                    s.append(",<no fields found>");
                throw MakeStringException(0, "Invalid MemIndex specification - field name %s not found (fields are %s)", fieldname, s.str()+1);
            }
            append(fieldNum);
        }
    }

    ~InMemoryIndex()
    {
        free(ptrs);
    }

    void append(unsigned fieldIdx)
    {
        sortFields.append(fieldIdx);
        const RtlTypeInfo *type = recInfo.queryType(fieldIdx);
        unsigned score = type->getMinSize();
        if (!score)
            score = 5;   // Arbitrary guess for average field length in a variable size field
        totalScore += score;
    }

    void serializeCursorPos(MemoryBuffer &mb) const
    {
        // We are saving a unique signature of this index that can be used to ensure that any continuation will identify the same index
        // Note that the continuation may be executed by a different slave
        // This code is not great but is probably ok.
        StringBuffer b;
        toString(b);
        mb.append(b);
    }

    // ICompare - used for the sort of the pointers
    virtual int docompare(const void *l, const void *r) const
    {
#ifdef BASED_POINTERS
        l = (char *) base + *(unsigned *) l;
        r = (char *) base + *(unsigned *) r;
#endif
        unsigned numOffsets = recInfo.getNumVarFields() + 1;  // MORE - could use max offset of any sort field to avoid calculating ones we don't compare on
        size_t * variableOffsetsL = (size_t *)alloca(numOffsets * sizeof(size_t));
        size_t * variableOffsetsR = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow lr(recInfo, l, numOffsets, variableOffsetsL);
        RtlRow rr(recInfo, r, numOffsets, variableOffsetsR);
        ForEachItemIn(idx, sortFields)
        {
            unsigned sortField = sortFields.item(idx);
            int ret = recInfo.queryType(sortField)->compare(lr.queryField(sortField), rr.queryField(sortField));
            if (ret)
                return ret;
        }
        // Compare the pointers to guarantee stable sort
        return (const char *) l - (const char *) r;
    }

    StringBuffer& toString(StringBuffer &ret) const
    {
        ForEachItemIn(idx, sortFields)
        {
            if (idx)
                ret.append('.');
            ret.appendf("%s.%d", recInfo.queryName(sortFields.item(idx)),sortFields.item(idx));
        }
        return ret;
    }

    StringBuffer& toXML(StringBuffer &ret, unsigned indent)
    {
        ret.pad(indent).append("<FieldSet>\n");
        ForEachItemIn(idx, sortFields)
        {
            ret.pad(indent+1).appendf("<Field fieldNum='%u'/>\n", sortFields.item(idx));  // Could consider names here as well/instead?
        }
        ret.pad(indent).append("</FieldSet>\n");
        return ret;
    }

    void doload(const void *_base, offset_t length, unsigned pass)
    {
        // NOTE - we require that memory has been reserved in advance. Two passes if need be (i.e. variable size)
#ifdef BASED_POINTERS
        base = _base;
#endif
        size32_t size = recInfo.getFixedSize();
        if (pass==0 && size)
        {
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
                unsigned thisRecSize = recInfo.getRecordSize(finger);
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

    void load(const void *_base, offset_t length)
    {
        for (unsigned pass=0; pass < 2; pass++)
            doload(_base, length, pass);
        sort();
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
        sort();
    }

    void sort()
    {
        StringBuffer x; DBGLOG("Sorting key %s", toString(x).str());
#ifdef BASED_POINTERS
        qsortarray(ptrs, numPtrs, *this);
#else
        qsortvec((void **) ptrs, numPtrs, *this);
#endif
        DBGLOG("Finished sorting key %s", x.str());
    }

    unsigned maxScore()
    {
        return totalScore;
    }

    bool equals(const InMemoryIndex &other)
    {
        if (other.totalScore != totalScore || other.sortFields.length() != sortFields.length())
            return false;
        ForEachItemIn(idx, sortFields)
        {
            if (sortFields.item(idx) != other.sortFields.item(idx))
                return false;
        }
        return true;
    }
};

class InMemoryIndexManager;

typedef IArrayOf<InMemoryIndex> InMemoryIndexSet;

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

class CDirectReaderBase : public CInterface, implements IDirectReader, implements IDirectStreamReader
{
protected:
    MemoryBuffer buf;  // Used if translating to hold on to current row;
    CThorContiguousRowBuffer deserializeSource;
    Owned<ISourceRowPrefetcher> prefetcher;
    Linked<const ITranslatorSet> translators;
    const IDynamicTransform *translator = nullptr;
    const RtlRecord *actual= nullptr;
    const RowFilter &postFilter;
    bool grouped = false;
    bool eogPending = false;
    bool anyThisGroup = false;
    UnexpectedVirtualFieldCallback fieldCallback;

    const byte * _nextRow(bool &eogPending)
    {
        prefetcher->readAhead(deserializeSource);
        if (grouped)
            deserializeSource.read(1, &eogPending);

        unsigned numOffsets = actual->getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow row(*actual, nullptr, numOffsets, variableOffsets);
        row.setRow(deserializeSource.queryRow(), 0);
        if (!postFilter.matches(row))
            return nullptr;
        else if (translator)
        {
            buf.setLength(0);
            MemoryBufferBuilder aBuilder(buf, 0);
            translator->translate(aBuilder, *this, row);
            return aBuilder.getSelf();
        }
        else
            return row.queryRow();
    }
public:
    CDirectReaderBase(const ITranslatorSet *_translators, const RowFilter &_postFilter, bool _grouped)
    : translators(_translators), postFilter(_postFilter), grouped(_grouped)
    {}
    virtual bool isKeyed() const override { return false; }

    virtual IDirectStreamReader *queryDirectStreamReader() override
    {
        assertex(!translators->isTranslating());
        return this;
    };
    virtual bool eos() = 0;

    virtual void serializeCursorPos(MemoryBuffer &mb) const override
    {
        mb.append(deserializeSource.tell());
    }

    virtual const byte *nextRow() override
    {
        while (!eos())
        {
            if (eogPending)
            {
                eogPending = false;
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return nullptr;
                }
            }
            const byte *row = _nextRow(eogPending);
            if (row)
            {
                anyThisGroup = true;
                return row;
            }
            else
                finishedRow();
        }
        return nullptr;
    }

    virtual void finishedRow() override
    {
        deserializeSource.finishedRow();
    }
    virtual const byte * lookupBlob(unsigned __int64 id) { throwUnexpected(); }

};

class InMemoryDirectReader : public CDirectReaderBase
{
    // MORE - might be able to use some of the jlib IStream implementations. But I don't want any indirections...
public:
    IMPLEMENT_IINTERFACE;
    memsize_t pos;
    const char *start;
    offset_t memsize;
    const PtrToOffsetMapper &baseMap;

    InMemoryDirectReader(const RowFilter &_postFilter, bool _grouped, offset_t _readPos,
                         const char *_start, memsize_t _memsize,
                         const PtrToOffsetMapper &_baseMap, unsigned _partNo, unsigned _numParts,
                         const ITranslatorSet *_translators)
        : CDirectReaderBase(_translators, _postFilter, _grouped), baseMap(_baseMap)
    {
        translator = translators->queryTranslator(0);  // Any one would do
        prefetcher.setown(translators->getPrefetcher(0));
        actual = &translators->queryActualLayout(0)->queryRecordAccessor(true);
        deserializeSource.setStream(this);
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

    virtual const void * peek(size32_t wanted,size32_t &got) override
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
    virtual void get(size32_t len, void * ptr) override
    {
        offset_t remaining = memsize - pos;
        if (len > remaining)
            throw MakeStringException(-1, "InMemoryDirectReader::get: requested %u bytes, only %u available", len, (unsigned) remaining);
        memcpy(ptr, start+pos, len);
        pos += len;
    }
    virtual bool eos() override
    {
        return pos == memsize;
    }
    virtual void skip(size32_t sz) override
    {
        assertex(pos + sz <= memsize);
        pos += sz;
    }
    virtual offset_t tell() const override
    {
        return pos;
    }
    virtual void reset(offset_t _offset, offset_t _flen) override
    {
        assertex(_offset <= memsize);
        pos = (memsize_t) _offset;
    }

    // Interface ISimpleReadStream

    virtual size32_t read(size32_t max_len, void * data) override
    {
        size32_t got;
        const void *ptr = peek(max_len, got);
        if (!got)
            return 0;
        memcpy(data, ptr, got);
        skip(got);
        return got;     
    }

    // Interface IDirectReaderEx

    virtual unsigned queryFilePart() const override
    {
        throwUnexpected(); // only supported for disk files
    }

    virtual unsigned __int64 makeFilePositionLocal(offset_t pos) override
    {
        return baseMap.makeFilePositionLocal(pos);
    }

    // Interface IThorDiskCallback 

    virtual unsigned __int64 getFilePosition(const void *_ptr) override
    {
        if (translator)
        {
            assertex(_ptr==buf.toByteArray());
            _ptr = deserializeSource.queryRow();
        }
        return baseMap.ptrToFilePosition(_ptr);
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr) override
    {
        if (translator)
        {
            assertex(_ptr==buf.toByteArray());
            _ptr = deserializeSource.queryRow();
        }
        return baseMap.ptrToLocalFilePosition(_ptr);
    }

    virtual const char * queryLogicalFilename(const void * row) override
    { 
        UNIMPLEMENTED;
        // Could implement fairly trivially since we don't support superfiles.
        // Even if we did can work out the partno using makeFilePositionLocal() and get the lfn from that
    }
};

class BufferedDirectReader : public CDirectReaderBase
{
public:
    IMPLEMENT_IINTERFACE;

    Linked<IFileIOArray> f;
    offset_t thisFileStartPos;
    offset_t completedStreamsSize;
    Owned<IFileIO> thisPart;
    Owned<ISerialStream> curStream;
    unsigned thisPartIdx;

    BufferedDirectReader(const RowFilter &_postFilter, bool _grouped, offset_t _startPos, IFileIOArray *_f, unsigned _partNo, unsigned _numParts, const ITranslatorSet *_translators)
    : CDirectReaderBase(_translators, _postFilter, _grouped), f(_f)
    {
        deserializeSource.setStream(this);
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
            unsigned subFileIdx = f->getSubFile(thisPartIdx);
            prefetcher.setown(translators->getPrefetcher(subFileIdx));
            translator = translators->queryTranslator(subFileIdx);
            actual = &translators->queryActualLayout(subFileIdx)->queryRecordAccessor(true);
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
        prefetcher.clear();
        while (!thisPart && ++thisPartIdx < maxParts)
        {
            thisPart.setown(f->getFilePart(thisPartIdx, thisFileStartPos ));
        }
        if (thisPart)
        {
            curStream.setown(createFileSerialStream(thisPart));
            unsigned subFileIdx = f->getSubFile(thisPartIdx);
            prefetcher.setown(translators->getPrefetcher(subFileIdx));
            translator = translators->queryTranslator(subFileIdx);
            actual = &translators->queryActualLayout(subFileIdx)->queryRecordAccessor(true);
        }
    }

    virtual const void * peek(size32_t wanted,size32_t &got) override
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

    virtual void get(size32_t len, void * ptr) override
    {
        if (curStream)
            curStream->get(len, ptr);
        else
            throw MakeStringException(-1, "BufferedDirectReader::get: requested %u bytes at eof", len);
    }
    virtual bool eos() override
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
    virtual void skip(size32_t len) override
    {
        if (curStream)
            curStream->skip(len);
        else
            throw MakeStringException(-1, "BufferedDirectReader::skip: tried to skip %u bytes at eof", len);
    }
    virtual offset_t tell() const override
    {
        // Note that tell() means the position with this stream, not the file position within the overall logical file.
        if (curStream)
            return completedStreamsSize + curStream->tell();
        else
            return completedStreamsSize;
    }
    virtual void reset(offset_t _offset,offset_t _flen) override
    {
        throwUnexpected(); // Not designed to be reset
    }

    // Interface ISimpleReadStream

    virtual size32_t read(size32_t max_len, void * data) override
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

    // Interface IDirectReaderEx

    virtual unsigned queryFilePart() const override
    {
        return thisPartIdx;
    }

    virtual unsigned __int64 makeFilePositionLocal(offset_t pos) override
    {
        assertex(pos >= thisFileStartPos);
        return makeLocalFposOffset(thisPartIdx-1, pos - thisFileStartPos);
    }

    // Interface IThorDiskCallback

    virtual unsigned __int64 getFilePosition(const void *_ptr) override
    {
        // MORE - could do with being faster than this!
        assertex(curStream != NULL);
        unsigned __int64 pos = deserializeSource.tell();
        return pos + thisFileStartPos;
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr) override
    {
        // MORE - could do with being faster than this!
        assertex(curStream != NULL);
        unsigned __int64 pos = deserializeSource.tell();
        return makeLocalFposOffset(thisPartIdx-1, pos);
    }

    virtual const char * queryLogicalFilename(const void * row) override
    { 
        return f->queryLogicalFilename(thisPartIdx);
    }
};

//------

unsigned ScoredRowFilter::scoreKey(const UnsignedArray &sortFields) const
{
    unsigned score = 0;
    ForEachItemIn(idx, sortFields)
    {
        unsigned fieldIdx = sortFields.item(idx);
        const IFieldFilter *match = findFilter(fieldIdx);
        if (!match)
            break;
        score += match->queryScore();
    }
    return score;
}

unsigned ScoredRowFilter::getMaxScore() const
{
    unsigned score = 0;
    ForEachItemIn(idx, filters)
    {
        score += filters.item(idx).queryScore();
    }
    return score;
}

//------

class InMemoryIndexManager : implements IInMemoryIndexManager, public CInterface
{
    // manages key selection and building.

    friend class InMemoryIndexTest;

    mutable CriticalSection activeCrit;
    InMemoryIndexSet activeIndexes;

    unsigned recordCount;
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
    const RtlRecord &recInfo; // This should refer to the one deserialized from dali info - to ensure correct lifetime

public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const override { return CInterface::IsShared(); }

    InMemoryIndexManager(const RtlRecord &_recInfo, bool _isOpt, const char *_fileName) : fileName(_fileName), recInfo(_recInfo)
    {
        recordCount = 0;
        loaded = false;
        loadedIntoMemory = false;
        totalSize = 0;
        fileStart = NULL;
        fileEnd = NULL;
        isOpt = _isOpt;
    }

    ~InMemoryIndexManager()
    {
        free (fileStart);
    }

    virtual IDirectReader *selectKey(MemoryBuffer &sig, ScoredRowFilter &postFilters, const ITranslatorSet *translators) const override;
    virtual IDirectReader *selectKey(ScoredRowFilter &filter, const ITranslatorSet *translators, IRoxieContextLogger &logctx) const override;

    InMemoryIndex &findIndex(const char *indexSig) const
    {
        CriticalBlock b(activeCrit);
        ForEachItemIn(idx, activeIndexes)
        {
            InMemoryIndex &thisIndex = activeIndexes.item(idx);
            StringBuffer sig;
            thisIndex.toString(sig);
            if (strcmp(sig, indexSig)==0)
                return thisIndex;
        }
        throwUnexpected();
    }

    inline const char *queryFileName() const
    {
        return fileName.get();
    }

    virtual IDirectReader *createReader(const RowFilter &postFilter, bool _grouped, offset_t readPos, unsigned partNo, unsigned numParts, const ITranslatorSet *translators) const override
    {
        if (loadedIntoMemory)
            return new InMemoryDirectReader(postFilter, _grouped, readPos, fileStart, fileEnd-fileStart, baseMap, partNo, numParts, translators);
        else
            return new BufferedDirectReader(postFilter, _grouped, readPos, files, partNo, numParts, translators);
    }

    virtual void load(IFileIOArray *_files, IOutputMetaData *preloadLayout, bool preload) override
    {
        if (!loaded)
        {
            files.set(_files);
            totalSize = 0;
            fileEnd = NULL; 
            if (files)
            {
                for (unsigned idx=0; idx < files->length(); idx++)
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
            if (!files->allFormatsMatch())
            {
                IException *E = makeStringException(ROXIE_MEMORY_ERROR, "Cannot load superfile with mismatching formats into memory");
                EXCLOG(MCoperatorError, E);
                throw E;
            }
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
                for (unsigned idx=0; idx < files->length(); idx++)
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
            loadedIntoMemory = true;
        }
    }

    virtual void setKeyInfo(IPropertyTree &indexInfo) override
    {
        Owned<IPropertyTreeIterator> indexes = indexInfo.getElements("FieldSet");
        CriticalBlock b(activeCrit);
        ForEach(*indexes)
        {
            InMemoryIndex *newOrder = new InMemoryIndex(recInfo, indexes->query());
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
                if (activeIndexes)
                {
                    InMemoryIndex &firstIdx = activeIndexes.item(0);
                    CriticalUnblock ub(activeCrit);
                    newOrder->load(firstIdx);  // Load pointers from an existing index to save rescanning all records
                }
                else
                {
                    CriticalUnblock ub(activeCrit);
                    newOrder->load(fileStart, totalSize);
                }
                activeIndexes.append(*newOrder);
            }
        }
    }

    void append(InMemoryIndex &newIdx)  // For use in unit tests
    {
        CriticalBlock b(activeCrit);
        activeIndexes.append(newIdx);
    }
};

extern IInMemoryIndexManager *getEmptyIndexManager(const RtlRecord &recInfo)
{
    return new InMemoryIndexManager(recInfo, true, nullptr);
}

class InMemoryIndexCursor : implements IDirectReader, implements ISourceRowCursor, public CInterface
{
    friend class InMemoryIndexTest;
    friend class InMemoryIndexManager;

#ifdef BASED_POINTERS
    const void *base;
#endif
    t_indexentry *ptrs;
    unsigned numPtrs;
    Linked<const InMemoryIndex> index;
    Owned<KeySearcher> keySearcher;

    RowFilter indexedFields;    // defining the values we are looking for
    RowFilter &postFilter;
    unsigned cur = 0;
    bool eof;
    const PtrToOffsetMapper &baseMap;
    RtlDynRow rowInfo;

    Linked<const InMemoryIndexManager> manager;
    MemoryBuffer buf;  // Used if translating to hold on to current row;
    Linked<const ITranslatorSet> translators;
    const IDynamicTransform *translator = nullptr;
public:
    IMPLEMENT_IINTERFACE;

    InMemoryIndexCursor(const InMemoryIndexManager *_manager, const InMemoryIndex *_index,
                        const PtrToOffsetMapper &_baseMap, RowFilter &_postFilter, const RtlRecord &_recInfo,
                        const ITranslatorSet *_translators, MemoryBuffer *serializedInfo = nullptr)
    : index(_index), postFilter(_postFilter), baseMap(_baseMap), rowInfo(_recInfo), manager(_manager), translators(_translators)
    {
        ForEachItemIn(idx, index->sortFields)
        {
            unsigned fieldIdx = index->sortFields.item(idx);
            const IFieldFilter *match = postFilter.extractFilter(fieldIdx);
            if (match)
                indexedFields.addFilter(*match);
            else
                break;  // MORE - this means we never use wilds in in-memory indexes - always leave to postfilter. Is that right?
        }
        keySearcher.setown(new KeySearcher(_recInfo, index->sortFields, indexedFields, this));
        translator = translators->queryTranslator(0);  // Any one would do - we require all to match
#ifdef BASED_POINTERS
        base = index->base;
#endif
        ptrs = index->ptrs;
        numPtrs = index->numPtrs;
        postFilter.recalcFieldsRequired();
        eof = false;
        if (serializedInfo)
            deserializeCursorPos(*serializedInfo);
    }

    ~InMemoryIndexCursor()
    {
    }

    // interface IDirectReader
    virtual bool isKeyed() const override
    {
        // Could argue should return false if no indexed fields. We still use a key as it avoids prefetches for variable-size rows
        // The value is used both to decide if worth doing in parallel (where false would be more appropriate)
        // and for continuation serialization (where false would generate invalid results)
        return true;
    }

    virtual IDirectStreamReader *queryDirectStreamReader()
    {
        throwUnexpected();
    }

    virtual const byte *nextRow() override
    {
        while (!eof)
        {
            if (!keySearcher->next())
            {
                eof = true;
                break;
            }
            else if (postFilter.matches(keySearcher->queryRow()))
            {
                if (translator)
                {
                    buf.setLength(0);
                    MemoryBufferBuilder aBuilder(buf, 0);
                    translator->translate(aBuilder, unexpectedVirtualFieldCallback, keySearcher->queryRow().queryRow()); // MORE - could pass in partially-resolved RtlRow
                    return aBuilder.getSelf();
                }
                else
                    return keySearcher->queryRow().queryRow();
            }
        }
        return nullptr;
    }

    virtual void finishedRow() override
    {
    }

    virtual void reset()
    {
        cur = (unsigned) -1;
        eof = false;
    }

    virtual const byte *findNext(const RowCursor & current)
    {
        size_t high = numPtrs;
        size_t low = cur+1;

        //Find the value of low,high where all rows 0..low-1 are < search and rows low..max are >= search
        while (low<high)
        {
            size_t mid = low + (high - low) / 2;
            rowInfo.setRow(GETROW(mid), indexedFields.getNumFieldsRequired());
            int rc = current.compareNext(rowInfo);  // compare rowInfo with the row we are hoping to find
            if (rc < 0)
                low = mid + 1;  // if this row is lower than the seek row, exclude mid from the potential positions
            else
                high = mid; // otherwise exclude all above mid from the potential positions.
        }
        cur = low;
        if (low == numPtrs)
        {
            eof = true;
            return nullptr;
        }
        return (const byte *) GETROW(cur);
    }

    virtual const byte * next()
    {
        cur++;
        if (cur >= numPtrs)
        {
            eof = true;
            return nullptr;
        }
        return (const byte *) GETROW(cur);
    }
    virtual unsigned __int64 getFilePosition(const void *_ptr)
    {
        if (translator)
        {
            assertex(_ptr==buf.toByteArray());
            _ptr = keySearcher->queryRow().queryRow();
        }
        return baseMap.ptrToFilePosition(_ptr);
    }

    virtual unsigned __int64 getLocalFilePosition(const void *_ptr)
    {
        if (translator)
        {
            assertex(_ptr==buf.toByteArray());
            _ptr = keySearcher->queryRow().queryRow();
        }
        return baseMap.ptrToLocalFilePosition(_ptr);
    }

    virtual const char * queryLogicalFilename(const void * row) 
    { 
        UNIMPLEMENTED;
    }
    virtual const byte * lookupBlob(unsigned __int64 id) { UNIMPLEMENTED; }

    virtual void serializeCursorPos(MemoryBuffer &mb) const 
    {
        index->serializeCursorPos(mb);
        mb.append(cur);
        // MORE - we could save some of the state in keyCursor to avoid seeking the next row
    }

    void deserializeCursorPos(MemoryBuffer &mb)
    {
        // Note - index signature already read
        mb.read(cur);
    }
};

IDirectReader *InMemoryIndexManager::selectKey(MemoryBuffer &serializedInfo, ScoredRowFilter &postFilters, const ITranslatorSet *translators) const
{
    StringBuffer indexSig;
    serializedInfo.read(indexSig);
    InMemoryIndex &thisIndex = findIndex(indexSig);
    return new InMemoryIndexCursor(this, &thisIndex, baseMap, postFilters, recInfo, translators, &serializedInfo);
}

IDirectReader *InMemoryIndexManager::selectKey(ScoredRowFilter &filter, const ITranslatorSet *translators, IRoxieContextLogger &logctx) const
{
    if (!inMemoryKeysEnabled)
        return nullptr;
    unsigned best = 0;
    unsigned perfect = filter.getMaxScore();
    InMemoryIndex *bestIndex = nullptr;
    {
        CriticalBlock b(activeCrit);
        ForEachItemIn(idx, activeIndexes)
        {
            InMemoryIndex &thisIndex = activeIndexes.item(idx);
            if (thisIndex.maxScore() > best)
            {
                unsigned score = filter.scoreKey(thisIndex.sortFields);
                if (score > best)
                {
                    bestIndex = &thisIndex;
                    best = score;
                }
                if (score==perfect) // Perfect match
                    break;
            }
        }
    }
    if (bestIndex)
    {
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer ret;
            logctx.CTXLOG("Using key %s", bestIndex->toString(ret).str());
        }
        return new InMemoryIndexCursor(this, bestIndex, baseMap, filter, recInfo, translators);
    }
    else
        return nullptr;
}

extern IInMemoryIndexManager *createInMemoryIndexManager(const RtlRecord &recInfo, bool isOpt, const char *fileName)
{
    return new InMemoryIndexManager(recInfo, isOpt, fileName);
}

//=======================================================================================================

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class InMemoryIndexTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( InMemoryIndexTest );
        CPPUNIT_TEST(test1);
        CPPUNIT_TEST(testPtrToOffsetMapper);
    CPPUNIT_TEST_SUITE_END();

protected:
    class CDummyTranslatorSet : implements CInterfaceOf<ITranslatorSet>
    {
        virtual const IDynamicTransform *queryTranslator(unsigned subFile) const override { return nullptr; }
        virtual const IKeyTranslator *queryKeyTranslator(unsigned subFile) const override { return nullptr; }
        virtual ISourceRowPrefetcher *getPrefetcher(unsigned subFile) const override { throwUnexpected(); }
        virtual IOutputMetaData *queryActualLayout(unsigned subFile) const override { throwUnexpected(); }
        virtual int queryTargetFormatCrc() const override { throwUnexpected(); }
        virtual const RtlRecord &queryTargetFormat() const override { throwUnexpected(); }
        virtual bool isTranslating() const override { return false; }
        virtual bool isTranslatingKeyed() const override { return false; }
        virtual bool hasConsistentTranslation() const override { return true; }
    } dummyTranslator;

    void test1()
    {
        StringContextLogger logctx("dummy");
        RtlIntTypeInfo ty1(type_int|type_unsigned, sizeof(unsigned));
        RtlFieldInfo f1("f1", nullptr, &ty1);
        const RtlFieldInfo * const fields [] = {&f1, nullptr};
        RtlRecord dummy(fields, true);
        unsigned testarray[] = {1,2,2,2,4,3,8,9,0,5};
        UnsignedArray order;
        order.append(0);
        InMemoryIndex di(dummy, order);
        di.load(testarray, sizeof(testarray));

        InMemoryIndexManager indexes(dummy, false, "test1");
        indexes.append(*LINK(&di));

        unsigned searchval = 1;
        ScoredRowFilter filter;
        filter.addFilter(*createFieldFilter(0, ty1, &searchval));
        Owned<IDirectReader> d = indexes.selectKey(filter, &dummyTranslator, logctx);
        ASSERT(d->nextRow()==(const byte *) &testarray[0]);
        ASSERT(d->nextRow()==nullptr);
        ASSERT(d->nextRow()==nullptr);

        searchval = 10;
        ScoredRowFilter filter1;
        filter1.addFilter(*createFieldFilter(0, ty1, &searchval));
        d.setown(indexes.selectKey(filter1, &dummyTranslator, logctx));
        ASSERT(d->nextRow()==nullptr);
        ASSERT(d->nextRow()==nullptr);
        d.clear();

        Owned<IPropertyTree> keyInfo = createPTreeFromXMLString("<R><FieldSet><Field name='f1'/></FieldSet></R>");
        indexes.setKeyInfo(*keyInfo.get());

        ScoredRowFilter filter2;
        Owned<IValueSet> set = createValueSet(ty1);
        unsigned searchval1 = 2;
        set->addRawRange(&searchval1, &searchval1);
        unsigned searchval2 = 9;
        set->addRawRange(&searchval2, &searchval2);
        filter2.addFilter(*createFieldFilter(0, set));
        d.setown(indexes.selectKey(filter2, &dummyTranslator, logctx));
        ASSERT(*(int *) d->nextRow()==2);
        ASSERT(*(int *) d->nextRow()==2);
        ASSERT(*(int *) d->nextRow()==2);
        ASSERT(*(int *) d->nextRow()==9);
        ASSERT(d->nextRow()==nullptr);

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

