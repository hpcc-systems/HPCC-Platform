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

#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "jprop.hpp"
#include "jdebug.hpp"
#include "jlzw.hpp"
#include "junicode.hpp"
#include "eclhelper.hpp"
#include "thorcommon.ipp"
#include "eclrtl.hpp"
#include "rtlread_imp.hpp"
#include "rtlcommon.hpp"
#include "rtldynfield.hpp"
#include "eclhelper_dyn.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include <algorithm>
#ifdef _USE_NUMA
#include <numa.h>
#endif
#include "roxiemem.hpp"
#include "thorstep.hpp"
#include "roxiemem.hpp"

#define ROWAGG_PERROWOVERHEAD (sizeof(AggregateRowBuilder))

void AggregateRowBuilder::Link() const { LinkRoxieRow(this); }
bool AggregateRowBuilder::Release() const { ReleaseRoxieRow(this); return false; }  // MORE - return value is iffy

RowAggregator::RowAggregator(IHThorHashAggregateExtra &_extra, IHThorRowAggregator & _helper) : helper(_helper)
{
    comparer = _extra.queryCompareRowElement();
    hasher = _extra.queryHash();
    elementHasher = _extra.queryHashElement();
    elementComparer = _extra.queryCompareElements();
    cursor = NULL;
    eof = false;
    totalSize = overhead = 0;
}

RowAggregator::~RowAggregator()
{
    reset();
}

static CClassMeta<AggregateRowBuilder> AggregateRowBuilderMeta;

void RowAggregator::start(IEngineRowAllocator *_rowAllocator, ICodeContext *ctx, unsigned activityId)
{
    rowAllocator.set(_rowAllocator);
    rowBuilderAllocator.setown(ctx->getRowAllocatorEx(&AggregateRowBuilderMeta, activityId, roxiemem::RHFunique|roxiemem::RHFscanning|roxiemem::RHFdelayrelease));
}

void RowAggregator::reset()
{
    while (!eof)
    {
        AggregateRowBuilder *n = nextResult();
        if (n)
            ReleaseRoxieRow(n);
    }
    _releaseAll();
    eof = false;
    cursor = NULL;
    rowAllocator.clear();
    totalSize = overhead = 0;
}

AggregateRowBuilder &RowAggregator::addRow(const void * row)
{
    AggregateRowBuilder *result;
    unsigned hash = hasher->hash(row);
    void * match = find(hash, row);
    if (match)
    {
        result = static_cast<AggregateRowBuilder *>(match);
        totalSize -= result->querySize();
        size32_t sz = helper.processNext(*result, row);
        result->setSize(sz);
        totalSize += sz;
    }
    else
    {
        Owned<AggregateRowBuilder> rowBuilder = new (rowBuilderAllocator->createRow()) AggregateRowBuilder(rowAllocator, hash);
        helper.clearAggregate(*rowBuilder);
        size32_t sz = helper.processFirst(*rowBuilder, row);
        rowBuilder->setSize(sz);
        result = rowBuilder.getClear();
        addNew(result, hash);
        totalSize += sz;
        overhead += ROWAGG_PERROWOVERHEAD;
    }
    return *result;
}

void RowAggregator::mergeElement(const void * otherElement)
{
    unsigned hash = elementHasher->hash(otherElement);
    void * match = findElement(hash, otherElement);
    if (match)
    {
        AggregateRowBuilder *rowBuilder = static_cast<AggregateRowBuilder *>(match);
        totalSize -= rowBuilder->querySize();
        size32_t sz = helper.mergeAggregate(*rowBuilder, otherElement);
        rowBuilder->setSize(sz);
        totalSize += sz;
    }
    else
    {
        Owned<AggregateRowBuilder> rowBuilder = new (rowBuilderAllocator->createRow()) AggregateRowBuilder(rowAllocator, hash);
        rowBuilder->setSize(cloneRow(*rowBuilder, otherElement, rowAllocator->queryOutputMeta()));
        addNew(rowBuilder.getClear(), hash);
    }
}

const void * RowAggregator::getFindParam(const void *et) const
{
    // Slightly odd name for this function... it actually gets the comparable element
    const AggregateRowBuilder *rb = static_cast<const AggregateRowBuilder*>(et);
    return rb->row();
}

bool RowAggregator::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    if (fphash != hashFromElement(et))
        return false;
    // et = element in the table (an AggregateRowBuilder) key = new row (in input row layout).
    return comparer->docompare(key, getFindParam(et)) == 0;
}

bool RowAggregator::matchesElement(const void *et, const void * searchET) const
{
    return elementComparer->docompare(getFindParam(et), searchET) == 0;
}


AggregateRowBuilder *RowAggregator::nextResult()
{
    void *ret = next(cursor);
    if (!ret)
    {
        eof = true;
        return NULL;
    }
    cursor = ret;
    return static_cast<AggregateRowBuilder *>(ret);
}

//=====================================================================================================

void CStreamMerger::fillheap(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
{
    assertex(activeInputs == 0);
    for(unsigned i = 0; i < numInputs; i++)
        if(pullInput(i, seek, numFields, stepExtra))
            mergeheap[activeInputs++] = i;
}

void CStreamMerger::permute(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
{
    // the tree structure: element p has children p*2+1 and p*2+2, or element c has parent (unsigned)(c-1)/2
    // the heap property: no element should be smaller than its parent
    // the dedup variant: if(dedup), the top of the heap should also not be equal to either child
    // the method: establish this by starting with the parent of the bottom element and working up to the top element, sifting each down to its correct place
    if (activeInputs >= 2)
        for(unsigned p = (activeInputs-2)/2; p > 0; --p)
            siftDown(p);

    if(dedup)
        siftDownDedupTop(seek, numFields, stepExtra);
    else
        siftDown(0);
}

const void * CStreamMerger::consumeTop()
{
    unsigned top = mergeheap[0];
    if (!pullConsumes)
        consumeInput(top);
    const void *next = pending[top];
    pending[top] = NULL;
    return next;
}


bool CStreamMerger::ensureNext(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra * stepExtra)
{
    //wasCompleteMatch must be initialised from the actual row returned.  (See bug #30388)
    if (first)
    {
        fillheap(seek, numFields, stepExtra);
        permute(seek, numFields, stepExtra);
        first = false;
        if (activeInputs == 0)
            return false;
        unsigned top = mergeheap[0];
        wasCompleteMatch = pendingMatches[top];
        return true;
    }

    while (activeInputs)
    {
        unsigned top = mergeheap[0];
        const void *next = pending[top];
        if (next)
        {
            if (seek)
            {
                int c = rangeCompare->docompare(next, seek, numFields);
                if (c >= 0)
                {
                    if (stepExtra->returnMismatches() && (c > 0))
                    {
                        wasCompleteMatch = pendingMatches[top];
                        return true;
                    }
                    else
                    {
                        if (pendingMatches[top])
                            return true;
                    }
                }
            }
            else
            {
                if (pendingMatches[top])
                    return true;
            }
            skipInput(top);
        }

        if(!pullInput(top, seek, numFields, stepExtra))
            if(!promote(0))
                return false;

        // we have changed the element at the top of the heap, so need to sift it down to maintain the heap property
        if(dedup)
            siftDownDedupTop(seek, numFields, stepExtra);
        else
            siftDown(0);
    }
    return false;
}

bool CStreamMerger::ensureNext()
{ 
    bool isCompleteMatch = true;
    return ensureNext(NULL, 0, isCompleteMatch, NULL); 
}

void CStreamMerger::permute()
{ 
    permute(NULL, 0, NULL); 
}

bool CStreamMerger::promote(unsigned p)
{
    activeInputs--;
    if(activeInputs == p)
        return false;
    mergeheap[p] = mergeheap[activeInputs];
    return true;
}

void CStreamMerger::siftDownDedupTop(const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
{
    // same as siftDown(0), except that it also ensures that the top of the heap is not equal to either of its children
    if(activeInputs < 2)
        return;
    unsigned c = 1;
    int childcmp = 1;
    if(activeInputs >= 3)
    {
        childcmp = compare->docompare(pending[mergeheap[2]], pending[mergeheap[1]]);
        if(childcmp < 0)
            c = 2;
    }
    int cmp = compare->docompare(pending[mergeheap[c]], pending[mergeheap[0]]);
    if(cmp > 0)
        return;
    // the following loop ensures the correct property holds on the smaller branch, and that childcmp==0 iff the top matches the other branch
    while(cmp <= 0)
    {
        if(cmp == 0)
        {
            if(mergeheap[c] < mergeheap[0])
            {
                unsigned r = mergeheap[c];
                mergeheap[c] = mergeheap[0];
                mergeheap[0] = r;
            }
            unsigned top = mergeheap[c];
            skipInput(top);
            if(!pullInput(top, seek, numFields, stepExtra))
                if(!promote(c))
                    break;
            siftDown(c);
        }
        else
        {
            unsigned r = mergeheap[c];
            mergeheap[c] = mergeheap[0];
            mergeheap[0] = r;
            if(siftDown(c))
                break;
        }
        cmp = compare->docompare(pending[mergeheap[c]], pending[mergeheap[0]]);
    }
    // the following loop ensures the uniqueness property holds on the other branch too
    c = 3-c;
    if(activeInputs <= c)
        return;
    while(childcmp == 0)
    {
        if(mergeheap[c] < mergeheap[0])
        {
            unsigned r = mergeheap[c];
            mergeheap[c] = mergeheap[0];
            mergeheap[0] = r;
        }
        unsigned top = mergeheap[c];
        skipInput(top);
        if(!pullInput(top, seek, numFields, stepExtra))
            if(!promote(c))
                break;
        siftDown(c);
        childcmp = compare->docompare(pending[mergeheap[c]], pending[mergeheap[0]]);
    }
}

void CStreamMerger::cleanup()
{
    clearPending();
    delete [] pending;
    pending = NULL;
    delete [] pendingMatches;
    pendingMatches = NULL;
    delete [] mergeheap;
    mergeheap = NULL;
}


void CStreamMerger::clearPending()
{
    if (pending && activeInputs)
    {
        for(unsigned i = 0; i < numInputs; i++)
        {
            if (pullConsumes)
                releaseRow(pending[i]);
            pending[i] = NULL;
        }
        activeInputs = 0;
    }
    first = true;
}

CStreamMerger::CStreamMerger(bool _pullConsumes)
{
    pending = NULL;
    pendingMatches = NULL;
    mergeheap = NULL;
    compare = NULL;
    rangeCompare = NULL;
    dedup = false;
    activeInputs = 0;
    pullConsumes = _pullConsumes;
    numInputs = 0;
    first = true;
}

CStreamMerger::~CStreamMerger()
{
    //can't call cleanup() because virtual releaseRow() won't be defined.
    // NOTE: use assert rather than assertex as exceptions from within destructors are not handled well.
    assert(!pending && !mergeheap);
}

void CStreamMerger::init(ICompare * _compare, bool _dedup, IRangeCompare * _rangeCompare)
{
    compare = _compare;
    dedup = _dedup;
    rangeCompare = _rangeCompare;
}

void CStreamMerger::initInputs(unsigned _numInputs)
{
    assertex(!pending);     // cleanup should have been called before reinitializing
    numInputs = _numInputs;
    mergeheap = new unsigned[numInputs];
    pending = new const void *[numInputs];
    pendingMatches = new bool [numInputs];
    for (unsigned i = 0; i < numInputs; i++)
        pending[i] = NULL;
    activeInputs = 0;
    first = true;
}

void CStreamMerger::consumeInput(unsigned i)
{
    //should be over-ridden if pullConsumes is false;
    throwUnexpected();
}


void CStreamMerger::skipInput(unsigned i)
{
    if (!pullConsumes)
        consumeInput(i);
    releaseRow(pending[i]);
    pending[i] = NULL;
}


void CStreamMerger::primeRows(const void * * rows)
{
    assertex(first && (activeInputs == 0));
    first = false;
    for(unsigned i = 0; i < numInputs; i++)
    {
        if ((pending[i] = rows[i]) != NULL)
        {
            mergeheap[activeInputs++] = i;
            pendingMatches[i] = true;
        }
    }

    permute();
}


const void * CStreamMerger::nextRow()
{
    if (ensureNext())
        return consumeTop();
    return NULL;
}


const void * CStreamMerger::queryNextRow()
{
    if (ensureNext())
        return pending[mergeheap[0]];
    return NULL;
}

unsigned CStreamMerger::queryNextInput()
{
    if (ensureNext())
        return mergeheap[0];
    return NotFound;
}


const void * CStreamMerger::nextRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    if (ensureNext(seek, numFields, wasCompleteMatch, &stepExtra))
        return consumeTop();
    return NULL;
}


void CStreamMerger::skipRow()
{
    assertex(!first);
    skipInput(mergeheap[0]);
}

//=====================================================================================================

CThorDemoRowSerializer::CThorDemoRowSerializer(MemoryBuffer & _buffer) : buffer(_buffer)
{
    nesting = 0;
}

void CThorDemoRowSerializer::put(size32_t len, const void * ptr)
{
    buffer.append(len, ptr);
    //ok to flush if nesting == 0;
}

size32_t CThorDemoRowSerializer::beginNested(size32_t count)
{
    nesting++;
    unsigned pos = buffer.length();
    buffer.append((size32_t)0);
    return pos;
}

void CThorDemoRowSerializer::endNested(size32_t sizePos)
{
    unsigned pos = buffer.length();
    buffer.rewrite(sizePos);
    buffer.append((size32_t)(pos - (sizePos + sizeof(size32_t))));
    buffer.rewrite(pos);
    nesting--;
}



IOutputRowSerializer * CachedOutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId) const
{
    if (metaFlags & (MDFhasserialize|MDFneedserializedisk))
        return meta->createDiskSerializer(ctx, activityId);
    if (isFixedSize())
        return new CSimpleFixedRowSerializer(getFixedSize());
    return new CSimpleVariableRowSerializer(this);
}


IOutputRowDeserializer * CachedOutputMetaData::createDiskDeserializer(ICodeContext * ctx, unsigned activityId) const
{
    if (metaFlags & (MDFhasserialize|MDFneedserializedisk))
        return meta->createDiskDeserializer(ctx, activityId);
    if (isFixedSize())
        return new CSimpleFixedRowDeserializer(getFixedSize());
    throwUnexpectedX("createDiskDeserializer variable meta has no serializer");
}

IOutputRowSerializer * CachedOutputMetaData::createInternalSerializer(ICodeContext * ctx, unsigned activityId) const
{
    if (metaFlags & (MDFhasserialize|MDFneedserializeinternal))
        return meta->createInternalSerializer(ctx, activityId);
    if (isFixedSize())
        return new CSimpleFixedRowSerializer(getFixedSize());
    return new CSimpleVariableRowSerializer(this);
}


IOutputRowDeserializer * CachedOutputMetaData::createInternalDeserializer(ICodeContext * ctx, unsigned activityId) const
{
    if (metaFlags & (MDFhasserialize|MDFneedserializeinternal))
        return meta->createInternalDeserializer(ctx, activityId);
    if (isFixedSize())
        return new CSimpleFixedRowDeserializer(getFixedSize());
    throwUnexpectedX("createInternalDeserializer variable meta has no serializer");
}

void CSizingSerializer::put(size32_t len, const void * ptr)
{
    totalsize += len;
}

size32_t CSizingSerializer::beginNested(size32_t count)
{
    totalsize += sizeof(size32_t);
    return totalsize;
}

void CSizingSerializer::endNested(size32_t position)
{
}

void CMemoryRowSerializer::put(size32_t len, const void * ptr)
{
    buffer.append(len, ptr);
}

size32_t CMemoryRowSerializer::beginNested(size32_t count)
{
    nesting++;
    unsigned pos = buffer.length();
    buffer.append((size32_t)0);
    return pos;
}

void CMemoryRowSerializer::endNested(size32_t sizePos)
{
    size32_t sz = buffer.length()-(sizePos + sizeof(size32_t));
    buffer.writeDirect(sizePos,sizeof(sz),&sz);
    nesting--;
}

static void ensureClassesAreNotAbstract()
{
    MemoryBuffer temp;
    CThorStreamDeserializerSource x1(NULL);
    CThorContiguousRowBuffer x2(NULL);
    CSizingSerializer x3;
    CMemoryRowSerializer x4(temp);
}

//=====================================================================================================

//the visitor callback is used to ensure link counts for children are updated.
size32_t cloneRow(ARowBuilder & rowBuilder, const void * row, IOutputMetaData * meta)
{
    size32_t rowSize = meta->getRecordSize(row);        // TBD could be better?
    byte * self = rowBuilder.ensureCapacity(rowSize, NULL);
    memcpy(self, row, rowSize);
    if (meta->getMetaFlags() & MDFneeddestruct)
    {
        ChildRowLinkerWalker walker;
        meta->walkIndirectMembers(self, walker);
    }
    return rowSize;
}


//---------------------------------------------------------------------------------------------------

extern const char * getActivityText(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKnone:                   return "None";
    case TAKdiskwrite:              return "Disk Write";
    case TAKsort:                   return "Sort";
    case TAKdedup:                  return "Dedup";
    case TAKfilter:                 return "Filter";
    case TAKsplit:                  return "Split";
    case TAKproject:                return "Project";
    case TAKrollup:                 return "Rollup";
    case TAKiterate:                return "Iterate";
    case TAKaggregate:              return "Aggregate";
    case TAKhashaggregate:          return "Hash Aggregate";
    case TAKfirstn:                 return "Firstn";
    case TAKsample:                 return "Sample";
    case TAKdegroup:                return "Degroup";
    case TAKjoin:                   return "Join";
    case TAKhashjoin:               return "Hash Join";
    case TAKlookupjoin:             return "Lookup Join";
    case TAKselfjoin:               return "Self Join";
    case TAKkeyedjoin:              return "Keyed Join";
    case TAKgroup:                  return "Group";
    case TAKworkunitwrite:          return "Output";
    case TAKfunnel:                 return "Funnel";
    case TAKapply:                  return "Apply";
    case TAKinlinetable:            return "Inline Dataset";
    case TAKhashdistribute:         return "Hash Distribute";
    case TAKhashdedup:              return "Hash Dedup";
    case TAKnormalize:              return "Normalize";
    case TAKremoteresult:           return "Remote Result";
    case TAKpull:                   return "Pull";
    case TAKdenormalize:            return "Denormalize";
    case TAKnormalizechild:         return "Normalize Child";
    case TAKchilddataset:           return "Child Dataset";
    case TAKselectn:                return "Select Nth";
    case TAKenth:                   return "Enth";
    case TAKif:                     return "If";
    case TAKnull:                   return "Null";
    case TAKdistribution:           return "Distribution";
    case TAKcountproject:           return "Count Project";
    case TAKchoosesets:             return "Choose Sets";
    case TAKpiperead:               return "Pipe Read";
    case TAKpipewrite:              return "Pipe Write";
    case TAKcsvwrite:               return "Csv Write";
    case TAKpipethrough:            return "Pipe Through";
    case TAKindexwrite:             return "Index Write";
    case TAKchoosesetsenth:         return "Choose Sets Enth";
    case TAKchoosesetslast:         return "Choose Sets Last";
    case TAKfetch:                  return "Fetch";
    case TAKhashdenormalize:        return "Hash Denormalize";
    case TAKworkunitread:           return "Read";
    case TAKthroughaggregate:       return "Through Aggregate";
    case TAKspill:                  return "Spill";
    case TAKcase:                   return "Case";
    case TAKlimit:                  return "Limit";
    case TAKcsvfetch:               return "Csv Fetch";
    case TAKxmlwrite:               return "Xml Write";
    case TAKjsonwrite:              return "Json Write";
    case TAKparse:                  return "Parse";
    case TAKsideeffect:             return "Simple Action";
    case TAKtopn:                   return "Top N";
    case TAKmerge:                  return "Merge";
    case TAKxmlfetch:               return "Xml Fetch";
    case TAKjsonfetch:              return "Json Fetch";
    case TAKxmlparse:               return "Parse Xml";
    case TAKkeyeddistribute:        return "Keyed Distribute";
    case TAKjoinlight:              return "Lightweight Join";
    case TAKalljoin:                return "All Join";
    case TAKsoap_rowdataset:        return "SOAP dataset";
    case TAKsoap_rowaction:         return "SOAP action";
    case TAKsoap_datasetdataset:    return "SOAP dataset";
    case TAKsoap_datasetaction:     return "SOAP action";
    case TAKkeydiff:                return "Key Difference";
    case TAKkeypatch:               return "Key Patch";
    case TAKkeyeddenormalize:       return "Keyed Denormalize";
    case TAKsequential:             return "Sequential";
    case TAKparallel:               return "Parallel";
    case TAKchilditerator:          return "Child Dataset";
    case TAKdatasetresult:          return "Dataset Result";
    case TAKrowresult:              return "Row Result";
    case TAKchildif:                return "If";
    case TAKpartition:              return "Partition Distribute";
    case TAKsubgraph:               return "Sub Graph";
    case TAKlocalgraph:             return "Local Graph";
    case TAKifaction:               return "If Action";
    case TAKemptyaction:            return "Empty Action";
    case TAKskiplimit:              return "Skip Limit";
    case TAKdiskread:               return "Disk Read";
    case TAKdisknormalize:          return "Disk Normalize";
    case TAKdiskaggregate:          return "Disk Aggregate";
    case TAKdiskcount:              return "Disk Count";
    case TAKdiskgroupaggregate:     return "Disk Grouped Aggregate";
    case TAKdiskexists:             return "Disk Exists";
    case TAKindexread:              return "Index Read";   
    case TAKindexnormalize:         return "Index Normalize";
    case TAKindexaggregate:         return "Index Aggregate";
    case TAKindexcount:             return "Index Count";
    case TAKindexgroupaggregate:    return "Index Grouped Aggregate";
    case TAKindexexists:            return "Index Exists";
    case TAKchildread:              return "Child Read";
    case TAKchildnormalize:         return "Child Normalize";
    case TAKchildaggregate:         return "Child Aggregate";
    case TAKchildcount:             return "Child Count";
    case TAKchildgroupaggregate:    return "Child Grouped Aggregate";
    case TAKchildexists:            return "Child Exists";
    case TAKchildthroughnormalize:  return "Normalize";
    case TAKcsvread:                return "Csv Read";
    case TAKxmlread:                return "Xml Read";
    case TAKjsonread:               return "Json Read";
    case TAKlocalresultread:        return "Read Local Result";
    case TAKlocalresultwrite:       return "Local Result";
    case TAKcombine:                return "Combine";
    case TAKregroup:                return "Regroup";
    case TAKrollupgroup:            return "Rollup Group";
    case TAKcombinegroup:           return "Combine Group";
    case TAKlookupdenormalize:      return "Lookup Denormalize";
    case TAKalldenormalize:         return "All Denormalize";
    case TAKsmartdenormalizegroup:  return "Smart Denormalize Group";
    case TAKunknowndenormalizegroup1: return "Unknown Denormalize Group1";
    case TAKunknowndenormalizegroup2: return "Unknown Denormalize Group2";
    case TAKunknowndenormalizegroup3: return "Unknown Denormalize Group3";
    case TAKlastdenormalizegroup:    return "Last Denormalize Group";
    case TAKdenormalizegroup:       return "Denormalize Group";
    case TAKhashdenormalizegroup:   return "Hash Denormalize Group";
    case TAKlookupdenormalizegroup: return "Lookup Denormalize Group";
    case TAKkeyeddenormalizegroup:  return "Keyed Denormalize Group";
    case TAKalldenormalizegroup:    return "All Denormalize Group";
    case TAKlocalresultspill:       return "Spill Local Result";
    case TAKsimpleaction:           return "Action";
    case TAKloopcount:              return "Loop";
    case TAKlooprow:                return "Loop";
    case TAKloopdataset:            return "Loop";
    case TAKchildcase:              return "Case";
    case TAKremotegraph:            return "Remote";
    case TAKlibrarycall:            return "Library Call";
    case TAKlocalstreamread:        return "Read Input";
    case TAKprocess:                return "Process";
    case TAKgraphloop:              return "Graph";
    case TAKparallelgraphloop:      return "Graph";
    case TAKgraphloopresultread:    return "Graph Input";
    case TAKgraphloopresultwrite:   return "Graph Result";
    case TAKgrouped:                return "Grouped";
    case TAKsorted:                 return "Sorted";
    case TAKdistributed:            return "Distributed";
    case TAKnwayjoin:               return "Join";
    case TAKnwaymerge:              return "Merge";
    case TAKnwaymergejoin:          return "Merge Join";
    case TAKnwayinput:              return "Nway Input";
    case TAKnwaygraphloopresultread: return "Nway Graph Input";
    case TAKnwayselect:             return "Select Nway Input";
    case TAKnonempty:               return "Non Empty";
    case TAKcreaterowlimit:         return "OnFail Limit";
    case TAKexistsaggregate:        return "Exists";
    case TAKcountaggregate:         return "Count";
    case TAKprefetchproject:        return "Prefetch Project";
    case TAKprefetchcountproject:   return "Prefetch Count Project";
    case TAKfiltergroup:            return "Filter Group";
    case TAKmemoryspillread:        return "Read Spill";
    case TAKmemoryspillwrite:       return "Write Spill";
    case TAKmemoryspillsplit:       return "Spill";
    case TAKsection:                return "Section";
    case TAKlinkedrawiterator:      return "Child Dataset";
    case TAKnormalizelinkedchild:   return "Normalize";
    case TAKfilterproject:          return "Filtered Project";
    case TAKcatch:                  return "Catch";
    case TAKskipcatch:              return "Skip Catch";
    case TAKcreaterowcatch:         return "OnFail Catch";
    case TAKsectioninput:           return "Section Input";
    case TAKcaseaction:             return "Case Action";
    case TAKindexgroupcount:        return "Index Grouped Count";
    case TAKindexgroupexists:       return "Index Grouped Exists";
    case TAKhashdistributemerge:    return "Distribute Merge";
    case TAKselfjoinlight:          return "Lightweight Self Join";
    case TAKlastjoin:               return "Last Join";
    case TAKwhen_dataset:           return "When";
    case TAKhttp_rowdataset:        return "HTTP dataset";
    case TAKstreamediterator:       return "Streamed Dataset";
    case TAKexternalsource:         return "User Source";
    case TAKexternalsink:           return "User Output";
    case TAKexternalprocess:        return "User Proceess";
    case TAKwhen_action:            return "When";
    case TAKsubsort:                return "Sub Sort";
    case TAKdictionaryworkunitwrite:return "Dictionary Write";
    case TAKdictionaryresultwrite:  return "Dictionary Result";
    case TAKsmartjoin:              return "Smart Join";
    case TAKunknownjoin1:           return "Unknown Join1";
    case TAKunknownjoin2:           return "Unknown Join2";
    case TAKunknownjoin3:           return "Unknown Join3";
    case TAKsmartdenormalize:       return "Smart Denormalize";
    case TAKunknowndenormalize1:    return "Unknown Denormalize1";
    case TAKunknowndenormalize2:    return "Unknown Denormalize2";
    case TAKunknowndenormalize3:    return "Unknown Denormalize3";
    case TAKlastdenormalize:        return "Last Denormalize";
    case TAKselfdenormalize:        return "Self Denormalize";
    case TAKselfdenormalizegroup:   return "Self Denormalize Group";
    case TAKtrace:                  return "Trace";
    case TAKquantile:               return "Quantile";
    case TAKspillread:              return "Spill Read";
    case TAKspillwrite:             return "Spill Write";
    case TAKnwaydistribute:         return "Nway Distribute";
    }
    throwUnexpected();
}

extern bool isActivitySource(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKpiperead:
    case TAKinlinetable:
    case TAKworkunitread:
    case TAKnull:
    case TAKsideeffect:
    case TAKsoap_rowdataset:
    case TAKsoap_rowaction:
    case TAKkeydiff:
    case TAKkeypatch:
    case TAKchilditerator:
    case TAKlocalgraph:
    case TAKemptyaction:
    case TAKdiskread:
    case TAKdisknormalize:
    case TAKdiskaggregate:
    case TAKdiskcount:
    case TAKdiskgroupaggregate:
    case TAKindexread:
    case TAKindexnormalize:
    case TAKindexaggregate:
    case TAKindexcount:
    case TAKindexgroupaggregate:
    case TAKchildnormalize:
    case TAKchildaggregate:
    case TAKchildgroupaggregate:
    case TAKcsvread:
    case TAKxmlread:
    case TAKjsonread:
    case TAKlocalresultread:
    case TAKsimpleaction:
    case TAKlocalstreamread:
    case TAKgraphloopresultread:
    case TAKnwaygraphloopresultread:
    case TAKlinkedrawiterator:
    case TAKindexgroupexists:
    case TAKindexgroupcount:
    case TAKstreamediterator:
    case TAKexternalsource:
    case TAKspillread:
        return true;
    }
    return false;
}

extern bool isActivitySink(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKdiskwrite: 
    case TAKworkunitwrite:
    case TAKapply:
    case TAKremoteresult:
    case TAKdistribution:
    case TAKpipewrite:
    case TAKcsvwrite:
    case TAKindexwrite:
    case TAKxmlwrite:
    case TAKjsonwrite:
    case TAKsoap_rowaction:
    case TAKsoap_datasetaction:
    case TAKkeydiff:
    case TAKkeypatch:
    case TAKdatasetresult:
    case TAKrowresult:
    case TAKemptyaction:
    case TAKlocalresultwrite:
    case TAKgraphloopresultwrite:
    case TAKsimpleaction:
    case TAKexternalsink:
    case TAKifaction:
    case TAKparallel:
    case TAKsequential:
    case TAKwhen_action:
    case TAKdictionaryworkunitwrite:
    case TAKdictionaryresultwrite:
    case TAKspillwrite:
        return true;
    }
    return false;
}

//=====================================================================================================


// ===========================================

IRowInterfaces *createRowInterfaces(IOutputMetaData *meta, unsigned actid, unsigned heapFlags, ICodeContext *context)
{
    class cRowInterfaces: implements IRowInterfaces, public CSimpleInterface
    {
        unsigned actid;
        Linked<IOutputMetaData> meta;
        ICodeContext* context;
        Linked<IEngineRowAllocator> allocator;
        Linked<IOutputRowSerializer> serializer;
        Linked<IOutputRowDeserializer> deserializer;
        CSingletonLock allocatorlock;
        CSingletonLock serializerlock;
        CSingletonLock deserializerlock;
        unsigned heapFlags;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cRowInterfaces(IOutputMetaData *_meta,unsigned _actid, unsigned _heapFlags, ICodeContext *_context)
            : meta(_meta), heapFlags(_heapFlags)
        {
            context = _context;
            actid = _actid;
        }
        IEngineRowAllocator * queryRowAllocator()
        {
            if (allocatorlock.lock()) {
                if (!allocator&&meta) 
                    allocator.setown(context->getRowAllocatorEx(meta, actid, heapFlags));
                allocatorlock.unlock();
            }
            return allocator;
        }
        IOutputRowSerializer * queryRowSerializer()
        {
            if (serializerlock.lock()) {
                if (!serializer&&meta) 
                    serializer.setown(meta->createDiskSerializer(context,actid));
                serializerlock.unlock();
            }
            return serializer;
        }
        IOutputRowDeserializer * queryRowDeserializer()
        {
            if (deserializerlock.lock()) {
                if (!deserializer&&meta) 
                    deserializer.setown(meta->createDiskDeserializer(context,actid));
                deserializerlock.unlock();
            }
            return deserializer;
        }
        IOutputMetaData *queryRowMetaData() 
        {
            return meta;
        }
        unsigned queryActivityId() const
        {
            return actid;
        }
        ICodeContext *queryCodeContext()
        {
            return context;
        }
    };
    return new cRowInterfaces(meta,actid,heapFlags,context);
};

static NullVirtualFieldCallback nullVirtualFieldCallback;
class CRowStreamReader : public CSimpleInterfaceOf<IExtRowStream>
{
protected:
    Linked<IFileIO> fileio;
    Linked<IMemoryMappedFile> mmfile;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<IEngineRowAllocator> allocator;
    Owned<ISerialStream> strm;
    Owned<ISourceRowPrefetcher> prefetcher;
    CThorContiguousRowBuffer prefetchBuffer;
    unsigned __int64 progress = 0;
    Linked<ITranslator> translatorContainer;
    MemoryBuffer translateBuf;
    IOutputMetaData *actualFormat = nullptr;
    const IDynamicTransform *translator = nullptr;
    IVirtualFieldCallback * fieldCallback;
    RowFilter actualFilter;
    RtlDynRow *filterRow = nullptr;

    EmptyRowSemantics emptyRowSemantics;
    offset_t currentRowOffset = 0;
    bool eoi = false;
    bool eos = false;
    bool eog = false;
    bool hadMatchInGroup = false;
    offset_t bufofs = 0;
#ifdef TRACE_CREATE
    static unsigned rdnum;
#endif

    class : implements IFileSerialStreamCallback
    {
    public:
        CRC32 crc;
        void process(offset_t ofs, size32_t sz, const void *buf)
        {
            crc.tally(sz,buf);
        }
    } crccb;
    
    inline bool fieldFilterMatch(const void * buffer)
    {
        if (actualFilter.numFilterFields())
        {
            filterRow->setRow(buffer, 0);
            return actualFilter.matches(*filterRow);
        }
        else
            return true;
    }

    inline bool checkEmptyRow()
    {
        if (ers_allow == emptyRowSemantics)
        {
            byte b;
            prefetchBuffer.read(1, &b);
            prefetchBuffer.finishedRow();
            if (1 == b)
                return true;
        }
        return false;
    }
    inline void checkEog()
    {
        if (ers_eogonly == emptyRowSemantics)
        {
            byte b;
            prefetchBuffer.read(1, &b);
            eog = 1 == b;
        }
    }
    inline bool checkExitConditions()
    {
        if (prefetchBuffer.eos())
        {
            eos = true;
            return true;
        }
        if (eog)
        {
            eog = false;
            if (hadMatchInGroup)
            {
                hadMatchInGroup = false;
                return true;
            }
        }
        return false;
    }
    const byte *getNextPrefetchRow()
    {
        while (true)
        {
            ++progress;
            if (checkEmptyRow())
                return nullptr;
            currentRowOffset = prefetchBuffer.tell();
            prefetcher->readAhead(prefetchBuffer);
            bool matched = fieldFilterMatch(prefetchBuffer.queryRow());
            checkEog();
            if (matched) // NB: prefetchDone() call must be paired with a row returned from prefetchRow()
            {
                hadMatchInGroup = true;
                return prefetchBuffer.queryRow(); // NB: buffer ptr could have changed due to reading eog byte
            }
            else
                prefetchBuffer.finishedRow();
            if (checkExitConditions())
                break;
        }
        return nullptr;
    }
    const void *getNextRow()
    {
        /* NB: this is very similar to getNextPrefetchRow() above
         * with the primary difference being it is deserializing into
         * a row builder and returning finalized rows.
         */
        while (true)
        {
            ++progress;
            if (checkEmptyRow())
                return nullptr;
            currentRowOffset = prefetchBuffer.tell();
            RtlDynamicRowBuilder rowBuilder(*allocator);
            size32_t size = deserializer->deserialize(rowBuilder, prefetchBuffer);
            bool matched = fieldFilterMatch(rowBuilder.getUnfinalized());
            checkEog();
            prefetchBuffer.finishedRow();
            const void *row = rowBuilder.finalizeRowClear(size);
            if (matched)
            {
                hadMatchInGroup = true;
                return row;
            }
            ReleaseRoxieRow(row);
            if (checkExitConditions())
                break;

        }
        return nullptr;
    }
public:
    CRowStreamReader(IFileIO *_fileio, IMemoryMappedFile *_mmfile, IRowInterfaces *rowif, offset_t _ofs, offset_t _len, bool _tallycrc, EmptyRowSemantics _emptyRowSemantics, ITranslator *_translatorContainer, IVirtualFieldCallback * _fieldCallback)
        : fileio(_fileio), mmfile(_mmfile), allocator(rowif->queryRowAllocator()), prefetchBuffer(nullptr), emptyRowSemantics(_emptyRowSemantics), translatorContainer(_translatorContainer), fieldCallback(_fieldCallback)
    {
#ifdef TRACE_CREATE
        PROGLOG("CRowStreamReader %d = %p",++rdnum,this);
#endif
        if (fileio)
            strm.setown(createFileSerialStream(fileio,_ofs,_len,(size32_t)-1, _tallycrc?&crccb:NULL));
        else
            strm.setown(createFileSerialStream(mmfile,_ofs,_len,_tallycrc?&crccb:NULL));
        currentRowOffset = _ofs;
        if (translatorContainer)
        {
            actualFormat = &translatorContainer->queryActualFormat();
            translator = &translatorContainer->queryTranslator();
        }
        else
        {
            actualFormat = rowif->queryRowMetaData();
            deserializer.set(rowif->queryRowDeserializer());
        }
        prefetcher.setown(actualFormat->createDiskPrefetcher());
        if (prefetcher)
            prefetchBuffer.setStream(strm);
        if (!fieldCallback)
            fieldCallback = &nullVirtualFieldCallback;
    }

    ~CRowStreamReader()
    {
#ifdef TRACE_CREATE
        PROGLOG("~CRowStreamReader %d = %p",rdnum--,this);
#endif
        delete filterRow;
    }

    IMPLEMENT_IINTERFACE_USING(CSimpleInterfaceOf<IExtRowStream>)

    virtual void reinit(offset_t _ofs,offset_t _len,unsigned __int64 _maxrows) override
    {
        assertex(_maxrows == 0);
        eoi = false;
        eos = (_len==0);
        eog = false;
        hadMatchInGroup = false;
        bufofs = 0;
        progress = 0;
        strm->reset(_ofs,_len);
        currentRowOffset = _ofs;
    }

    virtual const void *nextRow() override
    {
        if (eog)
        {
            eog = false;
            hadMatchInGroup = false;
        }
        else if (!eos)
        {
            if (prefetchBuffer.eos())
                eos = true;
            else
            {
                if (translator)
                {
                    const byte *row = getNextPrefetchRow();
                    if (row)
                    {
                        RtlDynamicRowBuilder rowBuilder(*allocator);
                        size32_t size = translator->translate(rowBuilder, *fieldCallback, row);
                        prefetchBuffer.finishedRow();
                        return rowBuilder.finalizeRowClear(size);
                    }
                }
                else
                    return getNextRow();
            }
        }
        return nullptr;
    }

    virtual const byte *prefetchRow() override
    {
        // NB: prefetchDone() call must be paired with a row returned from prefetchRow()
        if (eog)
        {
            eog = false;
            hadMatchInGroup = false;
        }
        else if (!eos)
        {
            if (prefetchBuffer.eos())
                eos = true;
            else
            {
                const byte *row = getNextPrefetchRow();
                if (row)
                {
                    if (translator)
                    {
                        translateBuf.setLength(0);
                        MemoryBufferBuilder rowBuilder(translateBuf, 0);
                        translator->translate(rowBuilder, *fieldCallback, row);
                        row = reinterpret_cast<const byte *>(translateBuf.toByteArray());
                    }
                    return row;
                }
            }
        }
        return nullptr;
    }

    virtual void prefetchDone() override
    {
        prefetchBuffer.finishedRow();
    }

    virtual void stop() override
    {
        stop(NULL);
    }

    void clear()
    {
        strm.clear();
        fileio.clear();
    }

    virtual void stop(CRC32 *crcout) override
    {
        if (!eos) {
            eos = true;
            clear();
        }
        // NB CRC will only be right if stopped at eos
        if (crcout)
            *crcout = crccb.crc;
    }

    virtual offset_t getOffset() const override
    {
        return prefetchBuffer.tell();
    }

    virtual offset_t getLastRowOffset() const override
    {
        return currentRowOffset;
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        if (fileio)
            return fileio->getStatistic(kind);
        return 0;
    }
    virtual unsigned __int64 queryProgress() const override
    {
        return progress;
    }
    virtual void setFilters(IConstArrayOf<IFieldFilter> &filters)
    {
        if (filterRow)
        {
            delete filterRow;
            filterRow = nullptr;
            actualFilter.clear();
        }
        if (filters.ordinality())
        {
            actualFilter.appendFilters(filters);
            if (translatorContainer)
            {
                const IKeyTranslator *keyedTranslator = translatorContainer->queryKeyedTranslator();
                if (keyedTranslator)
                    keyedTranslator->translate(actualFilter);
            }
            const RtlRecord *actual = &actualFormat->queryRecordAccessor(true);
            filterRow = new RtlDynRow(*actual);
        }
    }
};

class CLimitedRowStreamReader : public CRowStreamReader
{
    unsigned __int64 maxrows;
    unsigned __int64 rownum;

public:
    CLimitedRowStreamReader(IFileIO *_fileio, IMemoryMappedFile *_mmfile, IRowInterfaces *rowif, offset_t _ofs, offset_t _len, unsigned __int64 _maxrows, bool _tallycrc, EmptyRowSemantics _emptyRowSemantics, ITranslator *translatorContainer, IVirtualFieldCallback * _fieldCallback)
        : CRowStreamReader(_fileio, _mmfile, rowif, _ofs, _len, _tallycrc, _emptyRowSemantics, translatorContainer, _fieldCallback)
    {
        maxrows = _maxrows;
        rownum = 0;
        eos = maxrows==0;
    }

    virtual void reinit(offset_t _ofs,offset_t _len,unsigned __int64 _maxrows) override
    {
        CRowStreamReader::reinit(_ofs, _len, 0);
        if (_maxrows==0)
            eos = true;
        maxrows = _maxrows;
        rownum = 0;
    }

    virtual const void *nextRow() override
    {
        const void * ret = CRowStreamReader::nextRow();
        if (++rownum==maxrows)
            eos = true;
        return ret;
    }
};

#ifdef TRACE_CREATE
unsigned CRowStreamReader::rdnum;
#endif

IExtRowStream *createRowStreamEx(IFileIO *fileIO, IRowInterfaces *rowIf, offset_t offset, offset_t len, unsigned __int64 maxrows, unsigned rwFlags, ITranslator *translatorContainer, IVirtualFieldCallback * fieldCallback)
{
    EmptyRowSemantics emptyRowSemantics = extractESRFromRWFlags(rwFlags);
    if (maxrows == (unsigned __int64)-1)
        return new CRowStreamReader(fileIO, NULL, rowIf, offset, len, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
    else
        return new CLimitedRowStreamReader(fileIO, NULL, rowIf, offset, len, maxrows, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
}

bool UseMemoryMappedRead = false;

IExtRowStream *createRowStreamEx(IFile *file, IRowInterfaces *rowIf, offset_t offset, offset_t len, unsigned __int64 maxrows, unsigned rwFlags, IExpander *eexp, ITranslator *translatorContainer, IVirtualFieldCallback * fieldCallback)
{
    bool compressed = TestRwFlag(rwFlags, rw_compress);
    EmptyRowSemantics emptyRowSemantics = extractESRFromRWFlags(rwFlags);
    if (UseMemoryMappedRead && !compressed)
    {
        PROGLOG("Memory Mapped read of %s",file->queryFilename());
        Owned<IMemoryMappedFile> mmfile = file->openMemoryMapped();
        if (!mmfile)
            return NULL;
        if (maxrows == (unsigned __int64)-1)
            return new CRowStreamReader(NULL, mmfile, rowIf, offset, len, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
        else
            return new CLimitedRowStreamReader(NULL, mmfile, rowIf, offset, len, maxrows, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
    }
    else
    {
        Owned<IFileIO> fileio;
        if (compressed)
        {
            // JCSMORE should pass in a flag for rw_compressblkcrc I think, doesn't look like it (or anywhere else)
            // checks the block crc's at the moment.
            fileio.setown(createCompressedFileReader(file, eexp, UseMemoryMappedRead));
        }
        else
            fileio.setown(file->open(IFOread));
        if (!fileio)
            return NULL;
        if (maxrows == (unsigned __int64)-1)
            return new CRowStreamReader(fileio, NULL, rowIf, offset, len, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
        else
            return new CLimitedRowStreamReader(fileio, NULL, rowIf, offset, len, maxrows, TestRwFlag(rwFlags, rw_crc), emptyRowSemantics, translatorContainer, fieldCallback);
    }
}

IExtRowStream *createRowStream(IFile *file, IRowInterfaces *rowIf, unsigned rwFlags, IExpander *eexp, ITranslator *translatorContainer, IVirtualFieldCallback * fieldCallback)
{
    return createRowStreamEx(file, rowIf, 0, (offset_t)-1, (unsigned __int64)-1, rwFlags, eexp, translatorContainer, fieldCallback);
}

// Memory map sizes can be big, restrict to 64-bit platforms.
void useMemoryMappedRead(bool on)
{
#if defined(_DEBUG) || defined(__64BIT__)
    UseMemoryMappedRead = on;
#endif
}

#define ROW_WRITER_BUFFERSIZE (0x100000)
class CRowStreamWriter : private IRowSerializerTarget, implements IExtRowWriter, public CSimpleInterface
{
    Linked<IFileIOStream> stream;
    Linked<IOutputRowSerializer> serializer;
    Linked<IEngineRowAllocator> allocator;
    CRC32 crc;
    EmptyRowSemantics emptyRowSemantics;
    bool tallycrc;
    unsigned nested;
    MemoryAttr ma;
    MemoryBuffer extbuf;  // may need to spill to disk at some point
    byte *buf;
    size32_t bufpos;
    bool autoflush;
#ifdef TRACE_CREATE
    static unsigned wrnum;
#endif

    void flushBuffer(bool final) 
    {
        try
        {
            if (bufpos) {
                stream->write(bufpos,buf);
                if (tallycrc)
                    crc.tally(bufpos,buf);
                bufpos = 0;
            }
            size32_t extpos = extbuf.length();
            if (!extpos)
                return;
            if (!final)
                extpos = (extpos/ROW_WRITER_BUFFERSIZE)*ROW_WRITER_BUFFERSIZE;
            if (extpos) {
                stream->write(extpos,extbuf.toByteArray());
                if (tallycrc)
                    crc.tally(extpos,extbuf.toByteArray());
            }
            if (extpos<extbuf.length()) {
                bufpos = extbuf.length()-extpos;
                memcpy(buf,extbuf.toByteArray()+extpos,bufpos);
            }
            extbuf.clear();
        }
        catch (IException *e)
        {
            autoflush = false; // avoid follow-on errors
            EXCLOG(e, "flushBuffer");
            throw;
        }
    }
    void streamFlush()
    {
        try
        {
            stream->flush();
        }
        catch (IException *e)
        {
            autoflush = false; // avoid follow-on errors
            EXCLOG(e, "streamFlush");
            throw;
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowStreamWriter(IFileIOStream *_stream, IOutputRowSerializer *_serializer, IEngineRowAllocator *_allocator, EmptyRowSemantics _emptyRowSemantics, bool _tallycrc, bool _autoflush)
        : stream(_stream), serializer(_serializer), allocator(_allocator), emptyRowSemantics(_emptyRowSemantics)
    {
#ifdef TRACE_CREATE
        PROGLOG("createRowWriter %d = %p",++wrnum,this);
#endif
        tallycrc = _tallycrc;
        nested = 0;
        buf = (byte *)ma.allocate(ROW_WRITER_BUFFERSIZE);
        bufpos = 0;
        autoflush = _autoflush;
    }

    ~CRowStreamWriter()
    {
#ifdef TRACE_CREATE
        PROGLOG("~createRowWriter %d = %p",wrnum--,this);
#endif
        if (autoflush)
            flush();
        else if (bufpos+extbuf.length()) {
#ifdef _DEBUG
            PrintStackReport();
#endif
            WARNLOG("CRowStreamWriter closed with %d bytes unflushed",bufpos+extbuf.length());
        }
    }

    void putRow(const void *row)
    {
        if (row)
        {
            if (ers_allow == emptyRowSemantics)
            {
                byte b = 0;
                put(1, &b);
                serializer->serialize(*this, (const byte *)row);
            }
            else
            {
                serializer->serialize(*this, (const byte *)row);
                if (ers_eogonly == emptyRowSemantics)
                {
                    byte b = 0;
                    if (bufpos<ROW_WRITER_BUFFERSIZE)
                        buf[bufpos++] = b;
                    else
                        extbuf.append(b);
                }
            }
            allocator->releaseRow(row);
        }
        else if (ers_eogonly == emptyRowSemantics) // backpatch
        {
            byte b = 1;
            if (extbuf.length())
                extbuf.writeDirect(extbuf.length()-1, sizeof(b), &b);
            else
            {
                assertex(bufpos);
                buf[bufpos-1] = b;
            }
        }
        else if (ers_allow == emptyRowSemantics)
        {
            byte b = 1;
            put(1, &b);
        }
    }

    void flush()
    {
        flushBuffer(true);
        streamFlush();
    }

    void flush(CRC32 *crcout)
    {
        flushBuffer(true);
        streamFlush();
        if (crcout)
            *crcout = crc;
    }

    offset_t getPosition()
    {
        return stream->tell()+bufpos+extbuf.length();
    }

    void put(size32_t len, const void * ptr)
    {
        // first fill buf
        for (;;) {
            if (bufpos<ROW_WRITER_BUFFERSIZE) {
                size32_t wr = ROW_WRITER_BUFFERSIZE-bufpos;
                if (wr>len)
                    wr = len;
                memcpy(buf+bufpos,ptr,wr);
                bufpos += wr;
                len -= wr;
                if (len==0)
                    break;  // quick exit
                ptr = (const byte *)ptr + wr;
            }
            if (nested) {
                // have to append to ext buffer (will need to spill to disk here if gets *too* big)
                extbuf.append(len,ptr);
                break;
            }
            else
                flushBuffer(false);
        }
    }

    size32_t beginNested(size32_t count)
    {
        if (nested++==0)
            if (bufpos==ROW_WRITER_BUFFERSIZE)
                flushBuffer(false);
        size32_t ret = bufpos+extbuf.length();
        size32_t sz = 0;
        put(sizeof(sz),&sz);
        return ret;
    }

    void endNested(size32_t pos)
    {
        size32_t sz = bufpos+extbuf.length()-(pos + sizeof(size32_t));
        size32_t wr = sizeof(size32_t); 
        byte *out = (byte *)&sz;
        if (pos<ROW_WRITER_BUFFERSIZE) {
            size32_t space = ROW_WRITER_BUFFERSIZE-pos;
            if (space>wr)
                space = wr;
            memcpy(buf+pos,out,space);
            wr -= space;
            if (wr==0) {
                --nested;
                return;  // quick exit
            }
            out += space;
            pos += space;
        }
        extbuf.writeDirect(pos-ROW_WRITER_BUFFERSIZE,wr,out);
        --nested;
    }

};

#ifdef TRACE_CREATE
unsigned CRowStreamWriter::wrnum=0;
#endif

IExtRowWriter *createRowWriter(IFile *iFile, IRowInterfaces *rowIf, unsigned flags, ICompressor *compressor, size32_t compressorBlkSz)
{
    OwnedIFileIO iFileIO;
    if (TestRwFlag(flags, rw_compress))
    {
        size32_t fixedSize = rowIf->queryRowMetaData()->querySerializedDiskMeta()->getFixedSize();
        if (fixedSize && TestRwFlag(flags, rw_grouped))
            ++fixedSize; // row writer will include a grouping byte
        ICompressedFileIO *compressedFileIO = createCompressedFileWriter(iFile, fixedSize, TestRwFlag(flags, rw_extend), TestRwFlag(flags, rw_compressblkcrc), compressor, getCompMethod(flags));
        if (compressorBlkSz)
            compressedFileIO->setBlockSize(compressorBlkSz);
        iFileIO.setown(compressedFileIO);
    }
    else
        iFileIO.setown(iFile->open((flags & rw_extend)?IFOwrite:IFOcreate));
    if (!iFileIO)
        return NULL;
    flags &= ~COMP_MASK;
    return createRowWriter(iFileIO, rowIf, flags);
}

IExtRowWriter *createRowWriter(IFileIO *iFileIO, IRowInterfaces *rowIf, unsigned flags, size32_t compressorBlkSz)
{
    if (TestRwFlag(flags, rw_compress))
        throw MakeStringException(0, "Unsupported createRowWriter flags");
    Owned<IFileIOStream> stream;
    if (TestRwFlag(flags, rw_buffered))
        stream.setown(createBufferedIOStream(iFileIO));
    else
        stream.setown(createIOStream(iFileIO));
    if (flags & rw_extend)
        stream->seek(0, IFSend);
    flags &= ~((unsigned)(rw_extend|rw_buffered));
    return createRowWriter(stream, rowIf, flags);
}

IExtRowWriter *createRowWriter(IFileIOStream *strm, IRowInterfaces *rowIf, unsigned flags)
{
    if (0 != (flags & (rw_extend|rw_buffered|COMP_MASK)))
        throw MakeStringException(0, "Unsupported createRowWriter flags");
    EmptyRowSemantics emptyRowSemantics = extractESRFromRWFlags(flags);
    Owned<CRowStreamWriter> writer = new CRowStreamWriter(strm, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), emptyRowSemantics, TestRwFlag(flags, rw_crc), TestRwFlag(flags, rw_autoflush));
    return writer.getClear();
}

class CDiskMerger : implements IDiskMerger, public CInterface
{
    IArrayOf<IFile> tempfiles;
    IRowStream **strms;
    Linked<IRecordSize> irecsize;
    StringAttr tempnamebase;
    Linked<IRowLinkCounter> linker;
    Linked<IRowInterfaces> rowInterfaces;
    
public:
    IMPLEMENT_IINTERFACE;

    CDiskMerger(IRowInterfaces *_rowInterfaces, IRowLinkCounter *_linker, const char *_tempnamebase)
        : rowInterfaces(_rowInterfaces), linker(_linker), tempnamebase(_tempnamebase)
    {
        strms = NULL;
    }
    ~CDiskMerger()
    {
        for (unsigned i=0;i<tempfiles.ordinality();i++) {
            if (strms&&strms[i])
                strms[i]->Release();
            try
            {
                tempfiles.item(i).remove();
            }
            catch (IException * e)
            {
                //Exceptions inside destructors are bad.
                EXCLOG(e);
                e->Release();
            }
        }
        free(strms);
    }
    IRowWriter *createWriteBlock()
    {
        StringBuffer tempname(tempnamebase);
        tempname.append('.').append(tempfiles.ordinality()).append('_').append((__int64)GetCurrentThreadId()).append('_').append((unsigned)GetCurrentProcessId());
        IFile *file = createIFile(tempname.str());
        tempfiles.append(*file);
        return createRowWriter(file, rowInterfaces);
    }
    void put(const void **rows,unsigned numrows)
    {
        Owned<IRowWriter> out = createWriteBlock();
        for (unsigned i=0;i<numrows;i++)
            out->putRow(rows[i]);
    }
    void putIndirect(const void ***rowptrs,unsigned numrows)
    {
        Owned<IRowWriter> out = createWriteBlock();
        for (unsigned i=0;i<numrows;i++)
            out->putRow(*(rowptrs[i]));
    }
    virtual void put(ISortedRowProvider *rows)
    {
        Owned<IRowWriter> out = createWriteBlock();
        void * row;
        while((row = rows->getNextSorted()) != NULL)
            out->putRow(row);
    }
    IRowStream *merge(ICompare *icompare, bool partdedup)
    {
        unsigned numstrms = tempfiles.ordinality();
        strms = (IRowStream **)calloc(numstrms,sizeof(IRowStream *));
        unsigned i;
        for (i=0;i<numstrms;i++) {
            strms[i] = createRowStream(&tempfiles.item(i), rowInterfaces);
        }
        if (numstrms==1) 
            return LINK(strms[0]);
        if (icompare) 
            return createRowStreamMerger(numstrms, strms, icompare, partdedup, linker);
        return createConcatRowStream(numstrms,strms);
    }
    virtual count_t mergeTo(IRowWriter *dest, ICompare *icompare, bool partdedup)
    {
        count_t count = 0;
        Owned<IRowStream> mergedStream = merge(icompare, partdedup);
        for (;;)
        {
            const void *row = mergedStream->nextRow();
            if (!row)
                return count;
            dest->putRow(row); // takes ownership
            ++count;
        }
        return count;
    }    
};

IDiskMerger *createDiskMerger(IRowInterfaces *rowInterfaces, IRowLinkCounter *linker, const char *tempnamebase)
{
    return new CDiskMerger(rowInterfaces, linker, tempnamebase);
}

//---------------------------------------------------------------------------------------------------------------------

void ActivityTimeAccumulator::addStatistics(IStatisticGatherer & builder) const
{
    if (totalCycles)
    {
        builder.addStatistic(StWhenFirstRow, firstRow);
        builder.addStatistic(StTimeElapsed, elapsed());
        builder.addStatistic(StTimeTotalExecute, cycle_to_nanosec(totalCycles));
        builder.addStatistic(StTimeFirstExecute, latency());
    }
}

void ActivityTimeAccumulator::addStatistics(CRuntimeStatisticCollection & merged) const
{
    if (totalCycles)
    {
        merged.mergeStatistic(StWhenFirstRow, firstRow);
        merged.mergeStatistic(StTimeElapsed, elapsed());
        merged.mergeStatistic(StTimeTotalExecute, cycle_to_nanosec(totalCycles));
        merged.mergeStatistic(StTimeFirstExecute, latency());
    }
}

void ActivityTimeAccumulator::merge(const ActivityTimeAccumulator & other)
{
    if (other.totalCycles)
    {
        if (totalCycles)
        {
            //Record the earliest start, the latest end, the longest latencies
            cycle_t thisLatency = latencyCycles();
            cycle_t otherLatency = other.latencyCycles();
            cycle_t maxLatency = std::max(thisLatency, otherLatency);
            if (startCycles > other.startCycles)
            {
                startCycles = other.startCycles;
                firstRow =other.firstRow;
            }
            firstExitCycles = startCycles + maxLatency;
            if (endCycles < other.endCycles)
                endCycles = other.endCycles;
            totalCycles += other.totalCycles;
        }
        else
            *this = other;
    }
}

//---------------------------------------------------------------------------------------------------------------------

//MORE: Not currently implemented for windows.
#ifdef CPU_SETSIZE
static unsigned getCpuId(const char * text, char * * next)
{
    unsigned cpu = (unsigned)strtoul(text, next, 10);
    if (*next == text)
        throw makeStringExceptionV(1, "Invalid CPU: %s", text);
    else if (cpu >= CPU_SETSIZE)
        throw makeStringExceptionV(1, "CPU %u is out of range 0..%u", cpu, CPU_SETSIZE);
    return cpu;
}
#endif

void setProcessAffinity(const char * cpuList)
{
    assertex(cpuList);
#ifdef CPU_ZERO
    cpu_set_t cpus;
    CPU_ZERO(&cpus);

    const char * cur = cpuList;
    for (;;)
    {
        char * next;
        unsigned cpu1 = getCpuId(cur, &next);
        if (*next == '-')
        {
            const char * range = next+1;
            unsigned cpu2 = getCpuId(range, &next);
            for (unsigned cpu= cpu1; cpu <= cpu2; cpu++)
                CPU_SET(cpu, &cpus);
        }
        else
            CPU_SET(cpu1, &cpus);

        if (*next == '\0')
            break;

        if (*next != ',')
            throw makeStringExceptionV(1, "Invalid cpu affinity list %s", cur);

        cur = next+1;
    }

    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpus))
        throw makeStringException(errno, "Failed to set affinity");
    DBGLOG("Process affinity set to %s", cpuList);
#endif
    clearAffinityCache();
}

void setAutoAffinity(unsigned curProcess, unsigned processPerMachine, const char * optNodes)
{
#if defined(CPU_ZERO) && defined(_USE_NUMA)
    if (processPerMachine <= 1)
        return;

    if (numa_available() == -1)
    {
        DBGLOG("Numa functions not available");
        return;
    }

    if (optNodes)
        throw makeStringException(1, "Numa node list not yet supported");

    unsigned numaMap[NUMA_NUM_NODES];
    unsigned numNumaNodes = 0;
#if defined(LIBNUMA_API_VERSION) && (LIBNUMA_API_VERSION>=2)
    //Create a bit mask to record which nodes are available to the system
    //num_all_nodes_ptr contains only nodes with associated memory - which causes issues on misconfigured systems
    struct bitmask * available_nodes = numa_allocate_nodemask();
    numa_bitmask_clearall(available_nodes);

    unsigned maxcpus = numa_num_configured_cpus();
    for (unsigned cpu=0; cpu < maxcpus; cpu++)
    {
        //Check the cpu can be used by this process.
        if (numa_bitmask_isbitset(numa_all_cpus_ptr, cpu))
        {
            int node = numa_node_of_cpu(cpu);
            if (node != -1)
                numa_bitmask_setbit(available_nodes, node);
        }
    }

    for (unsigned i=0; i<=numa_max_node(); i++)
    {
        if (numa_bitmask_isbitset(available_nodes, i))
        {
            numaMap[numNumaNodes] = i;
            numNumaNodes++;

            if (!numa_bitmask_isbitset(numa_all_nodes_ptr, i))
                DBGLOG("Numa: Potential inefficiency - node %u does not have any associated memory", i);
        }
    }

    numa_bitmask_free(available_nodes);

    DBGLOG("Affinity: Max cpus(%u) nodes(%u) actual nodes(%u), processes(%u)", maxcpus, numa_max_node()+1, numNumaNodes, processPerMachine);
#else
    //On very old versions of numa assume that all nodes are present
    for (unsigned i=0; i<=numa_max_node(); i++)
    {
        numaMap[numNumaNodes] = i;
        numNumaNodes++;
    }
#endif
    if (numNumaNodes <= 1)
        return;

    unsigned firstNode = 0;
    unsigned numNodes = 1;
    if (processPerMachine >= numNumaNodes)
    {
        firstNode = curProcess % numNumaNodes;
    }
    else
    {
        firstNode = (curProcess * numNumaNodes) / processPerMachine;
        unsigned nextNode = ((curProcess+1) *  numNumaNodes) / processPerMachine;
        numNodes = nextNode - firstNode;
    }

    if ((processPerMachine % numNumaNodes) != 0)
        DBGLOG("Affinity: %u processes will not be evenly balanced over %u numa nodes", processPerMachine, numNumaNodes);

#if defined(LIBNUMA_API_VERSION) && (LIBNUMA_API_VERSION>=2)
    //This code assumes the nodes are sensibly ordered (e.g., nodes on the same socket are next to each other), and
    //only works well when number of processes is a multiple of the number of numa nodes.  A full solution would look
    //at distances.
    struct bitmask * cpus = numa_allocate_cpumask();
    struct bitmask * nodeMask = numa_allocate_cpumask();
    for (unsigned node=0; node < numNodes; node++)
    {
        numa_node_to_cpus(numaMap[firstNode+node], nodeMask);
        //Shame there is no inbuilt union operation.
        for (unsigned cpu=0; cpu < maxcpus; cpu++)
        {
            if (numa_bitmask_isbitset(nodeMask, cpu))
                numa_bitmask_setbit(cpus, cpu);
        }
    }
    bool ok = (numa_sched_setaffinity(0, cpus) == 0);
    numa_bitmask_free(nodeMask);
    numa_bitmask_free(cpus);
#else
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    numa_node_to_cpus(numaMap[firstNode], (unsigned long *) &cpus, sizeof (cpus));
    bool ok = sched_setaffinity (0, sizeof(cpus), &cpus) != 0;
#endif

    if (!ok)
        throw makeStringExceptionV(1, "Failed to set affinity to numa node %u (id:%u)", firstNode, numaMap[firstNode]);

    DBGLOG("Process bound to numa node %u..%u (id:%u) of %u", firstNode, firstNode + numNodes - 1, numaMap[firstNode], numNumaNodes);
#endif
    clearAffinityCache();
}

void bindMemoryToLocalNodes()
{
#if defined(LIBNUMA_API_VERSION) && (LIBNUMA_API_VERSION>=2)
    numa_set_bind_policy(1);

    unsigned numNumaNodes = 0;
    for (unsigned i=0; i<=numa_max_node(); i++)
    {
        if (numa_bitmask_isbitset(numa_all_nodes_ptr, i))
            numNumaNodes++;
    }
    if (numNumaNodes <= 1)
        return;
    struct bitmask *nodes = numa_get_run_node_mask();
    numa_set_membind(nodes);
    DBGLOG("Process memory bound to numa nodemask 0x%x (of %u nodes total)", (unsigned)(*(nodes->maskp)), numNumaNodes);
    numa_bitmask_free(nodes);
#endif
}

static IOutputMetaData *_getDaliLayoutInfo(MemoryBuffer &layoutBin, IPropertyTree const &props)
{
    try
    {
        Owned<IException> error;
        bool isGrouped = props.getPropBool("@grouped", false);
        if (props.hasProp("_rtlType"))
        {
            props.getPropBin("_rtlType", layoutBin);
            try
            {
                return createTypeInfoOutputMetaData(layoutBin, isGrouped);
            }
            catch (IException *E)
            {
                EXCLOG(E);
                error.setown(E); // Save to throw later if we can't recover via ECL
            }
        }
        if (props.hasProp("ECL"))
        {
            const char *kind = props.queryProp("@kind");
            bool isIndex = (kind && streq(kind, "key"));
            StringBuffer layoutECL;
            props.getProp("ECL", layoutECL);
            MultiErrorReceiver errs;
            Owned<IHqlExpression> expr = parseQuery(layoutECL.str(), &errs);
            if (errs.errCount() == 0)
            {
                if (props.hasProp("_record_layout"))  // Some old indexes need the payload count patched in from here
                {
                    MemoryBuffer mb;
                    props.getPropBin("_record_layout", mb);
                    expr.setown(patchEclRecordDefinitionFromRecordLayout(expr, mb));
                }
                if (exportBinaryType(layoutBin, expr, isIndex))
                    return createTypeInfoOutputMetaData(layoutBin, isGrouped);
            }
        }
        if (error)
        {
            throw(error.getClear());
        }
    }
    catch (IException *E)
    {
        EXCLOG(E, "Cannot deserialize file metadata:");
        ::Release(E);
    }
    catch (...)
    {
        DBGLOG("Cannot deserialize file metadata: Unknown error");
    }
    return nullptr;
}

extern THORHELPER_API IOutputMetaData *getDaliLayoutInfo(IPropertyTree const &props)
{
    MemoryBuffer layoutBin;
    return _getDaliLayoutInfo(layoutBin, props);
}

extern THORHELPER_API bool getDaliLayoutInfo(MemoryBuffer &layoutBin, IPropertyTree const &props)
{
    Owned<IOutputMetaData> meta = _getDaliLayoutInfo(layoutBin, props);
    return nullptr != meta; // meta created to verify, but only returning layoutBin;
}

static bool getTranslators(Owned<const IDynamicTransform> &translator, Owned<const IKeyTranslator> *keyedTranslator, const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode)
{
    if (expectedCrc)
    {
        IOutputMetaData * sourceFormat = expectedFormat;
        unsigned sourceCrc = expectedCrc;
        if (mode != RecordTranslationMode::AlwaysECL)
        {
            if (publishedFormat)
            {
                sourceFormat = publishedFormat;
                sourceCrc = publishedCrc;
            }

            if (publishedCrc && expectedCrc && (publishedCrc != expectedCrc) && (RecordTranslationMode::None == mode))
                throwTranslationError(publishedFormat->queryRecordAccessor(true), expectedFormat->queryRecordAccessor(true), tracing);
        }

        //This has a very low possibility of format crcs accidentally matching, which could lead to a crashes on an untranslated files.
        if ((projectedFormat != sourceFormat) && (projectedCrc != sourceCrc))
        {
            translator.setown(createRecordTranslator(projectedFormat->queryRecordAccessor(true), sourceFormat->queryRecordAccessor(true)));

            if (!translator->canTranslate())
                throw MakeStringException(0, "Untranslatable record layout mismatch detected for file %s", tracing);

            if (translator->needsTranslate())
            {
                if (keyedTranslator && (sourceFormat != expectedFormat))
                {
                    Owned<const IKeyTranslator> _keyedTranslator = createKeyTranslator(sourceFormat->queryRecordAccessor(true), expectedFormat->queryRecordAccessor(true));
                    if (_keyedTranslator->needsTranslate())
                        keyedTranslator->swap(_keyedTranslator);
                }
            }
            else
                translator.clear();
        }
    }
    return nullptr != translator.get();
}

bool getTranslators(Owned<const IDynamicTransform> &translator, const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode)
{
    return getTranslators(translator, nullptr, tracing, expectedCrc, expectedFormat, publishedCrc, publishedFormat, projectedCrc, projectedFormat, mode);
}

bool getTranslators(Owned<const IDynamicTransform> &translator, Owned<const IKeyTranslator> &keyedTranslator, const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode)
{
    return getTranslators(translator, &keyedTranslator, tracing, expectedCrc, expectedFormat, publishedCrc, publishedFormat, projectedCrc, projectedFormat, mode);
}

ITranslator *getTranslators(const char *tracing, unsigned expectedCrc, IOutputMetaData *expectedFormat, unsigned publishedCrc, IOutputMetaData *publishedFormat, unsigned projectedCrc, IOutputMetaData *projectedFormat, RecordTranslationMode mode)
{
    Owned<const IDynamicTransform> translator;
    Owned<const IKeyTranslator> keyedTranslator;
    if (getTranslators(translator, &keyedTranslator, tracing, expectedCrc, expectedFormat, publishedCrc, publishedFormat, projectedCrc, projectedFormat, mode))
    {
        if (RecordTranslationMode::AlwaysECL == mode)
        {
            publishedFormat = expectedFormat;
            publishedCrc = expectedCrc;
        }
        else if (!publishedFormat)
            publishedFormat = expectedFormat;
        class CTranslator : public CSimpleInterfaceOf<ITranslator>
        {
            Linked<IOutputMetaData> actualFormat;
            Linked<const IDynamicTransform> translator;
            Linked<const IKeyTranslator> keyedTranslator;
        public:
            CTranslator(IOutputMetaData *_actualFormat, const IDynamicTransform *_translator, const IKeyTranslator *_keyedTranslator)
                : actualFormat(_actualFormat), translator(_translator), keyedTranslator(_keyedTranslator)
            {
            }
            virtual IOutputMetaData &queryActualFormat() const override
            {
                return *actualFormat;
            }
            virtual const IDynamicTransform &queryTranslator() const override
            {
                return *translator;
            }
            virtual const IKeyTranslator *queryKeyedTranslator() const override
            {
                return keyedTranslator;
            }
        };
        return new CTranslator(publishedFormat, translator, keyedTranslator);
    }
    else
        return nullptr;
}

#ifdef _USE_TBB
#include "tbb/task.h"

CPersistentTask::CPersistentTask(const char *name, IThreaded *_owner) : owner(_owner) {}

void CPersistentTask::start()
{
    class RunTask : public tbb::task
    {
    public:
        RunTask(CPersistentTask * _owner, tbb::task * _next) : owner(_owner), next(_next)
        {
        }
        virtual tbb::task * execute()
        {
            try
            {
                owner->owner->threadmain();
            }
            catch (IException *e)
            {
                owner->exception.setown(e);
            }
            return next;
        }
    protected:
        CPersistentTask * owner;
        tbb::task * next;
    };
    end = new (tbb::task::allocate_root()) tbb::empty_task();
    tbb::task * task = new (end->allocate_child()) RunTask(this, nullptr);
    end->set_ref_count(1+1);
    tbb::task::spawn(*task);
}

bool CPersistentTask::join(unsigned timeout, bool throwException)
{
    end->wait_for_all();
    end->destroy(*end);
    end = nullptr;
    if (throwException && exception.get())
        throw exception.getClear();
    return true;
}
#endif

