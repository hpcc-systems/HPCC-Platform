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

#include "thorstep.hpp"

#define ROWAGG_PERROWOVERHEAD (sizeof(AggregateRowBuilder))
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

void RowAggregator::start(IEngineRowAllocator *_rowAllocator)
{
    rowAllocator.set(_rowAllocator);
}

void RowAggregator::reset()
{
    while (!eof)
    {
        AggregateRowBuilder *n = nextResult();
        if (n)
            n->Release();
    }
    SuperHashTable::releaseAll();
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
        Owned<AggregateRowBuilder> rowBuilder = new AggregateRowBuilder(rowAllocator, hash);
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
        Owned<AggregateRowBuilder> rowBuilder = new AggregateRowBuilder(rowAllocator, hash);
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
    case TAKindexread:              return "Index Read";   
    case TAKindexnormalize:         return "Index Normalize";
    case TAKindexaggregate:         return "Index Aggregate";
    case TAKindexcount:             return "Index Count";
    case TAKindexgroupaggregate:    return "Index Grouped Aggregate";
    case TAKchildnormalize:         return "Child Normalize";
    case TAKchildaggregate:         return "Child Aggregate";
    case TAKchildgroupaggregate:    return "Child Grouped Aggregate";
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
    case TAKsectioninput:               return "Section Input";
    case TAKindexgroupcount:        return "Index Grouped Count";
    case TAKindexgroupexists:   return "Index Grouped Exists";
    case TAKhashdistributemerge:    return "Distribute Merge";
    case TAKselfjoinlight:          return "Lightweight Self Join";
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
    case TAKsmartdenormalize:       return "Smart Denormalize";
    case TAKsmartdenormalizegroup:  return "Smart Denormalize Group";
    case TAKselfdenormalize:        return "Self Denormalize";
    case TAKselfdenormalizegroup:   return "Self Denormalize Group";
    case TAKtrace:                  return "Trace";
    case TAKquantile:               return "Quantile";
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
        return true;
    }
    return false;
}

//=====================================================================================================


CThorContiguousRowBuffer::CThorContiguousRowBuffer(ISerialStream * _in) : in(_in)
{
    buffer = NULL;
    maxOffset = 0;
    readOffset = 0;
}

void CThorContiguousRowBuffer::doRead(size32_t len, void * ptr)
{
    ensureAccessible(readOffset + len);
    memcpy(ptr, buffer+readOffset, len);
    readOffset += len;
}


size32_t CThorContiguousRowBuffer::read(size32_t len, void * ptr)
{
    doRead(len, ptr);
    return len;
}

size32_t CThorContiguousRowBuffer::readSize()
{
    size32_t value;
    doRead(sizeof(value), &value);
    return value;
}

size32_t CThorContiguousRowBuffer::readPackedInt(void * ptr)
{
    size32_t size = sizePackedInt();
    doRead(size, ptr);
    return size;
}

size32_t CThorContiguousRowBuffer::readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len)
{
    if (len == 0)
        return 0;

    size32_t size = sizeUtf8(len);
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVStr();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}

size32_t CThorContiguousRowBuffer::readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize)
{
    size32_t size = sizeVUni();
    byte * self = target.ensureCapacity(fixedSize + size, NULL);
    doRead(size, self+offset);
    return size;
}


size32_t CThorContiguousRowBuffer::sizePackedInt()
{
    ensureAccessible(readOffset+1);
    return rtlGetPackedSizeFromFirst(buffer[readOffset]);
}

size32_t CThorContiguousRowBuffer::sizeUtf8(size32_t len)
{
    if (len == 0)
        return 0;

    //The len is the number of utf characters, size depends on which characters are included.
    size32_t nextOffset = readOffset;
    while (len)
    {
        ensureAccessible(nextOffset+1);

        for (;nextOffset < maxOffset;)
        {
            nextOffset += readUtf8Size(buffer+nextOffset);  // This function only accesses the first byte
            if (--len == 0)
                break;
        }
    }
    return nextOffset - readOffset;
}

size32_t CThorContiguousRowBuffer::sizeVStr()
{
    size32_t nextOffset = readOffset;
    loop
    {
        ensureAccessible(nextOffset+1);

        for (; nextOffset < maxOffset; nextOffset++)
        {
            if (buffer[nextOffset] == 0)
                return (nextOffset + 1) - readOffset;
        }
    }
}

size32_t CThorContiguousRowBuffer::sizeVUni()
{
    size32_t nextOffset = readOffset;
    const size32_t sizeOfUChar = 2;
    loop
    {
        ensureAccessible(nextOffset+sizeOfUChar);

        for (; nextOffset+1 < maxOffset; nextOffset += sizeOfUChar)
        {
            if (buffer[nextOffset] == 0 && buffer[nextOffset+1] == 0)
                return (nextOffset + sizeOfUChar) - readOffset;
        }
    }
}


void CThorContiguousRowBuffer::reportReadFail()
{
    throwUnexpected();
}


const byte * CThorContiguousRowBuffer::peek(size32_t maxSize)
{
    if (maxSize+readOffset > maxOffset)
        doPeek(maxSize+readOffset);
    return buffer + readOffset;
}

offset_t CThorContiguousRowBuffer::beginNested()
{
    size32_t len = readSize();
    return len+readOffset;
}

bool CThorContiguousRowBuffer::finishedNested(offset_t & endPos)
{
    return readOffset >= endPos;
}

void CThorContiguousRowBuffer::skip(size32_t size)
{ 
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipPackedInt()
{
    size32_t size = sizePackedInt();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipUtf8(size32_t len)
{
    size32_t size = sizeUtf8(len);
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVStr()
{
    size32_t size = sizeVStr();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

void CThorContiguousRowBuffer::skipVUni()
{
    size32_t size = sizeVUni();
    ensureAccessible(readOffset+size);
    readOffset += size;
}

// ===========================================

IRowInterfaces *createRowInterfaces(IOutputMetaData *meta, unsigned actid, ICodeContext *context)
{
    class cRowInterfaces: public CSimpleInterface, implements IRowInterfaces
    {
        Linked<IOutputMetaData> meta;
        ICodeContext* context;
        unsigned actid;
        Linked<IEngineRowAllocator> allocator;
        Linked<IOutputRowSerializer> serializer;
        Linked<IOutputRowDeserializer> deserializer;
        CSingletonLock allocatorlock;
        CSingletonLock serializerlock;
        CSingletonLock deserializerlock;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cRowInterfaces(IOutputMetaData *_meta,unsigned _actid, ICodeContext *_context)
            : meta(_meta)
        {
            context = _context;
            actid = _actid;
        }
        IEngineRowAllocator * queryRowAllocator()
        {
            if (allocatorlock.lock()) {
                if (!allocator&&meta) 
                    allocator.setown(context->getRowAllocator(meta, actid));
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
        unsigned queryActivityId()
        {
            return actid;
        }
        ICodeContext *queryCodeContext()
        {
            return context;
        }
    };
    return new cRowInterfaces(meta,actid,context);
};

class CRowStreamReader : public CSimpleInterface, implements IExtRowStream
{
    Linked<IFileIO> fileio;
    Linked<IMemoryMappedFile> mmfile;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<IEngineRowAllocator> allocator;
    Owned<ISerialStream> strm;
    CThorStreamDeserializerSource source;
    Owned<ISourceRowPrefetcher> prefetcher;
    CThorContiguousRowBuffer prefetchBuffer; // used if prefetcher set
    bool grouped;
    unsigned __int64 maxrows;
    unsigned __int64 rownum;
    bool eoi;
    bool eos;
    bool eog;
    offset_t bufofs;
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
    
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowStreamReader(IFileIO *_fileio, IMemoryMappedFile *_mmfile, IRowInterfaces *rowif, offset_t _ofs, offset_t _len, unsigned __int64 _maxrows, bool _tallycrc, bool _grouped)
        : fileio(_fileio), mmfile(_mmfile), allocator(rowif->queryRowAllocator()), prefetchBuffer(NULL) 
    {
#ifdef TRACE_CREATE
        PROGLOG("CRowStreamReader %d = %p",++rdnum,this);
#endif
        maxrows = _maxrows;
        grouped = _grouped;
        eoi = false;
        eos = maxrows==0;
        eog = false;
        bufofs = 0;
        rownum = 0;
        if (fileio)
            strm.setown(createFileSerialStream(fileio,_ofs,_len,(size32_t)-1, _tallycrc?&crccb:NULL));
        else
            strm.setown(createFileSerialStream(mmfile,_ofs,_len,_tallycrc?&crccb:NULL));
        prefetcher.setown(rowif->queryRowMetaData()->createDiskPrefetcher(rowif->queryCodeContext(), rowif->queryActivityId()));
        if (prefetcher)
            prefetchBuffer.setStream(strm);
        source.setStream(strm);
        deserializer.set(rowif->queryRowDeserializer());            
    }

    ~CRowStreamReader()
    {
#ifdef TRACE_CREATE
        PROGLOG("~CRowStreamReader %d = %p",rdnum--,this);
#endif
    }

    void reinit(offset_t _ofs,offset_t _len,unsigned __int64 _maxrows)
    {
        maxrows = _maxrows;
        eoi = false;
        eos = (maxrows==0)||(_len==0);
        eog = false;
        bufofs = 0;
        rownum = 0;
        strm->reset(_ofs,_len);
    }



    const void *nextRow()
    {
        if (eog) {
            eog = false;
            return NULL;
        }
        if (eos)
            return NULL;
        if (source.eos()) {
            eos = true;
            return NULL;
        }
        RtlDynamicRowBuilder rowBuilder(allocator);
        size_t size = deserializer->deserialize(rowBuilder,source);
        if (grouped && !eos) {
            byte b;
            source.read(sizeof(b),&b);
            eog = (b==1);
        }
        if (++rownum==maxrows)
            eos = true;
        return rowBuilder.finalizeRowClear(size);
    }

    const void *prefetchRow(size32_t *sz)
    {
        if (eog) 
            eog = false;
        else if (!eos) {
            if (source.eos()) 
                eos = true;
            else {
                assertex(prefetcher);
                prefetcher->readAhead(prefetchBuffer);
                const byte * ret = prefetchBuffer.queryRow();
                if (sz)
                    *sz = prefetchBuffer.queryRowSize();
                return ret;
            }
        }
        if (sz)
            sz = 0;
        return NULL;
    }

    void prefetchDone()
    {
        prefetchBuffer.finishedRow();
        if (grouped) {
            byte b;
            strm->get(sizeof(b),&b);
            eog = (b==1);
        }
    }

    virtual void stop()
    {
        stop(NULL);
    }

    void clear()
    {
        strm.clear();
        source.clearStream();
        fileio.clear();
    }


    void stop(CRC32 *crcout)
    {
        if (!eos) {
            eos = true;
            clear();
        }
        // NB CRC will only be right if stopped at eos
        if (crcout)
            *crcout = crccb.crc;
    }

    offset_t getOffset()
    {
        return source.tell();
    }


};

#ifdef TRACE_CREATE
unsigned CRowStreamReader::rdnum;
#endif

bool UseMemoryMappedRead = false;

IExtRowStream *createRowStreamEx(IFile *file, IRowInterfaces *rowIf, offset_t offset, offset_t len, unsigned __int64 maxrows, unsigned rwFlags, IExpander *eexp)
{
    bool compressed = TestRwFlag(rwFlags, rw_compress);
    if (UseMemoryMappedRead && !compressed)
    {
        PROGLOG("Memory Mapped read of %s",file->queryFilename());
        Owned<IMemoryMappedFile> mmfile = file->openMemoryMapped();
        if (!mmfile)
            return NULL;
        return new CRowStreamReader(NULL, mmfile, rowIf, offset, len, maxrows, TestRwFlag(rwFlags, rw_crc), TestRwFlag(rwFlags, rw_grouped));
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
        return new CRowStreamReader(fileio, NULL, rowIf, offset, len, maxrows, TestRwFlag(rwFlags, rw_crc), TestRwFlag(rwFlags, rw_grouped));
    }
}

IExtRowStream *createRowStream(IFile *file, IRowInterfaces *rowIf, unsigned rwFlags, IExpander *eexp)
{
    return createRowStreamEx(file, rowIf, 0, (offset_t)-1, (unsigned __int64)-1, rwFlags, eexp);
}

// Memory map sizes can be big, restrict to 64-bit platforms.
void useMemoryMappedRead(bool on)
{
#if defined(_DEBUG) || defined(__64BIT__)
    UseMemoryMappedRead = on;
#endif
}

#define ROW_WRITER_BUFFERSIZE (0x100000)
class CRowStreamWriter : public CSimpleInterface, private IRowSerializerTarget, implements IExtRowWriter
{
    Linked<IFileIOStream> stream;
    Linked<IOutputRowSerializer> serializer;
    Linked<IEngineRowAllocator> allocator;
    CRC32 crc;
    bool grouped;
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

    CRowStreamWriter(IFileIOStream *_stream,IOutputRowSerializer *_serializer,IEngineRowAllocator *_allocator,bool _grouped, bool _tallycrc, bool _autoflush)
        : stream(_stream), serializer(_serializer), allocator(_allocator)
    {
#ifdef TRACE_CREATE
        PROGLOG("createRowWriter %d = %p",++wrnum,this);
#endif
        grouped = _grouped;
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
        if (row) {
            serializer->serialize(*this,(const byte *)row);
            if (grouped) {
                byte b = 0;
                if (bufpos<ROW_WRITER_BUFFERSIZE) 
                    buf[bufpos++] = b;
                else 
                    extbuf.append(b);
            }
            allocator->releaseRow(row);
        }
        else if (grouped) { // backpatch
            byte b = 1;
            if (extbuf.length())
                extbuf.writeDirect(extbuf.length()-1,sizeof(b),&b);
            else {
                assertex(bufpos);
                buf[bufpos-1] = b;
            }
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
        loop {
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

IExtRowWriter *createRowWriter(IFile *iFile, IRowInterfaces *rowIf, unsigned flags, ICompressor *compressor)
{
    OwnedIFileIO iFileIO;
    if (TestRwFlag(flags, rw_compress))
    {
        size32_t fixedSize = rowIf->queryRowMetaData()->querySerializedDiskMeta()->getFixedSize();
        if (fixedSize && TestRwFlag(flags, rw_grouped))
            ++fixedSize; // row writer will include a grouping byte
        iFileIO.setown(createCompressedFileWriter(iFile, fixedSize, TestRwFlag(flags, rw_extend), TestRwFlag(flags, rw_compressblkcrc), compressor, TestRwFlag(flags, rw_fastlz)));
    }
    else
        iFileIO.setown(iFile->open((flags & rw_extend)?IFOwrite:IFOcreate));
    if (!iFileIO)
        return NULL;
    flags &= ~((unsigned)(rw_compress|rw_fastlz|rw_compressblkcrc));
    return createRowWriter(iFileIO, rowIf, flags);
}

IExtRowWriter *createRowWriter(IFileIO *iFileIO, IRowInterfaces *rowIf, unsigned flags)
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
    if (0 != (flags & (rw_compress|rw_fastlz|rw_extend|rw_buffered|rw_compressblkcrc)))
        throw MakeStringException(0, "Unsupported createRowWriter flags");
    Owned<CRowStreamWriter> writer = new CRowStreamWriter(strm, rowIf->queryRowSerializer(), rowIf->queryRowAllocator(), TestRwFlag(flags, rw_grouped), TestRwFlag(flags, rw_crc), TestRwFlag(flags, rw_autoflush));
    return writer.getClear();
}

class CDiskMerger : public CInterface, implements IDiskMerger
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
        loop
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




