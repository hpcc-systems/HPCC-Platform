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

//****************************************************************************
// Name:         jhtree.cpp
//
// Purpose:      
//
// Description:    
//
// Notes:        Supports only static (non-changing) files
//
//                Initially I was holding on to the root nodes, but came to find
//                that they could potentially fill the cache by themselves...
//
//                Things to play with:
//                    - try not unpacking the entire node when it is read in.
//                        break it out as needed later.
//
// History:        31-Aug-99   crs  original
//              08-Jan-00    nh  added LZW compression of nodes
//                14-feb-00     nh     added GetORDKey
//                15-feb-00     nh     fixed isolatenode and nextNode
//                12-Apr-00    jcs moved over to jhtree.dll etc.
//****************************************************************************

#include "platform.h"
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <limits.h>
#ifdef __linux__
#include <alloca.h>
#endif

#include "hlzw.h"

#include "jmutex.hpp"
#include "jhutil.hpp"
#include "jmisc.hpp"
#include "jstats.h"
#include "ctfile.hpp"
#include "jhinplace.hpp"

#include "jhtree.ipp"
#include "keybuild.hpp"
#include "bloom.hpp"
#include "eclhelper_dyn.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"
#include "eclhelper_base.hpp"
#include "jmetrics.hpp"
#include "jevent.hpp"

constexpr __uint64 defaultFetchThresholdNs = 20000; // Assume anything < 20us comes from the page cache, everything above probably went to disk

static std::atomic<CKeyStore *> keyStore(nullptr);
static unsigned defaultKeyIndexLimit = 200;
static CNodeCache *nodeCache = NULL;
static CriticalSection *initCrit = NULL;
static cycle_t fetchThresholdCycles = 0;

bool useMemoryMappedIndexes = false;
bool linuxYield = false;
bool flushJHtreeCacheOnOOM = true;
std::atomic<unsigned __int64> branchSearchCycles{0};
std::atomic<unsigned __int64> leafSearchCycles{0};

static cycle_t traceCacheLockingFrequency{0};
static cycle_t traceNodeLoadFrequency{0};
static cycle_t traceNodeLoadThreshold{0};

MODULE_INIT(INIT_PRIORITY_JHTREE_JHTREE)
{
    initCrit = new CriticalSection;
    fetchThresholdCycles = nanosec_to_cycle(defaultFetchThresholdNs);
    traceCacheLockingFrequency = millisec_to_cycle(60000);          // Report locking delays at most once per minute
    traceNodeLoadFrequency = millisec_to_cycle(60000);              // Report slow loads at most once per minute
    traceNodeLoadThreshold = millisec_to_cycle(20);                 // Report node loads that take > 5ms
    return 1;
}

MODULE_EXIT()
{
    delete initCrit;
    delete keyStore.load(std::memory_order_relaxed);
    ::Release((CInterface*)nodeCache);
    nodeCache = nullptr;
}

//#define DUMP_NODES

SegMonitorList::SegMonitorList(const RtlRecord &_recInfo) : recInfo(_recInfo)
{
    keySegCount = recInfo.getNumKeyedFields();
    reset();
}

SegMonitorList::SegMonitorList(const SegMonitorList &from, const char *fixedVals, unsigned sortFieldOffset)
: recInfo(from.recInfo), keySegCount(from.keySegCount)
{
    ForEachItemIn(idx, from.segMonitors)
    {
        IKeySegmentMonitor &seg = from.segMonitors.item(idx);
        unsigned offset = seg.getOffset();
        if (offset < sortFieldOffset)
            segMonitors.append(*createSingleKeySegmentMonitor(false, seg.queryFieldIndex(), offset, seg.getSize(), fixedVals+offset));
        else
            segMonitors.append(OLINK(seg));
    }
    recalculateCache();
    modified = false;
}

void SegMonitorList::describe(StringBuffer &out) const
{
    for (unsigned idx=0; idx <= lastRealSeg() && idx < segMonitors.length(); idx++)
    {
        auto &filter = segMonitors.item(idx);
        if (idx)
            out.append(',');
        out.appendf("%s=", recInfo.queryName(idx));
        filter.describe(out, *recInfo.queryType(idx));
    }
}

bool SegMonitorList::matchesBuffer(const void *buffer, unsigned lastSeg, unsigned &matchSeg) const
{
    if (segMonitors.length())
    {
        for (; matchSeg <= lastSeg; matchSeg++)
        {
            if (!segMonitors.item(matchSeg).matchesBuffer(buffer))
                return false;
        }
    }
    return true;
}

bool SegMonitorList::canMatch() const
{
    ForEachItemIn(idx, segMonitors)
    {
        if (segMonitors.item(idx).isEmpty())
            return false;
    }
    return true;
}

IIndexFilter *SegMonitorList::item(unsigned idx) const
{
    return &segMonitors.item(idx);
}

size32_t SegMonitorList::getSize() const
{
    unsigned lim = segMonitors.length();
    if (lim)
    {
        IKeySegmentMonitor &lastItem = segMonitors.item(lim-1);
        return lastItem.getOffset() + lastItem.getSize();
    }
    else
        return 0;
}

void SegMonitorList::checkSize(size32_t keyedSize, char const * keyname) const
{
    size32_t segSize = getSize();
    if (segSize != keyedSize)
    {
        StringBuffer err;
        err.appendf("Key size mismatch on key %s - key size is %u, expected %u", keyname, keyedSize, getSize());
        IException *e = MakeStringExceptionDirect(1000, err.str());
        EXCLOG(e, err.str());
        throw e;
    }
}

void SegMonitorList::setLow(unsigned segno, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    while (segno < lim)
        segMonitors.item(segno++).setLow(keyBuffer);
}

unsigned SegMonitorList::setLowAfter(size32_t offset, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    unsigned segno = 0;
    unsigned skipped = 0;
    while (segno < lim)
    {
        IKeySegmentMonitor &seg = segMonitors.item(segno++);
        if (seg.getOffset() >= offset)
            seg.setLow(keyBuffer);
        else if (seg.getSize()+seg.getOffset() <= offset)
            skipped++;
        else
        {
            byte *temp = (byte *) alloca(seg.getSize() + seg.getOffset());
            seg.setLow(temp);
            memcpy((byte *)keyBuffer+offset, temp+offset, seg.getSize() - (offset - seg.getOffset()));
        }
    }
    return skipped;
}

void SegMonitorList::endRange(unsigned segno, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    if (segno < lim)
        segMonitors.item(segno++).endRange(keyBuffer);
    while (segno < lim)
        segMonitors.item(segno++).setHigh(keyBuffer);
}

bool SegMonitorList::incrementKey(unsigned segno, void *keyBuffer) const
{
    // Increment the key buffer to next acceptable value
    for(;;)
    {
        if (segMonitors.item(segno).increment(keyBuffer))
        {
            setLow(segno+1, keyBuffer);
            return true;
        }
        if (!segno)
            return false;
        segno--;
    }
}

unsigned SegMonitorList::_lastRealSeg()
{
    unfiltered = false;
    unsigned seg = segMonitors.length();
    for (;;)
    {
        if (!seg)
        {
            unfiltered = true;
            return 0;
        }
        seg--;
        if (!segMonitors.item(seg).isWild()) // MORE - why not just remove them? Stepping/overrides?
            return seg;
    }
}

unsigned SegMonitorList::lastFullSeg() const
{
    // This is used to determine what part of the segmonitor list to use for a pre-count to determine if atmost/limit have been hit
    // We include everything up to the last of i) the last keyed element or ii) the last keyed,opt element that has no wild between it and a keyed element
    // NOTE - can return (unsigned) -1 if there are no full segments
    unsigned len = segMonitors.length();
    unsigned seg = 0;
    unsigned ret = (unsigned) -1;
    bool wildSeen = false;
    while (seg < len)
    {
        if (segMonitors.item(seg).isWild())
            wildSeen = true;
        else
        {
            if (!wildSeen || !segMonitors.item(seg).isOptional())
            {
                ret = seg;
                wildSeen = false;
            }
        }
        seg++;
    }
    return ret;
}

void SegMonitorList::finish(unsigned keyedSize)
{
    if (modified)
    {
        while (segMonitors.length() < keySegCount)
        {
            unsigned idx = segMonitors.length();
            size32_t offset = recInfo.getFixedOffset(idx);
            if (offset == keyedSize)
            {
                DBGLOG("SegMonitor record does not match key");  // Can happen when reading older indexes that don't save key information in metadata properly
                keySegCount = segMonitors.length();
                break;
            }
            size32_t size = recInfo.getFixedOffset(idx+1) - offset;
            segMonitors.append(*createWildKeySegmentMonitor(idx, offset, size));
        }
        size32_t segSize = getSize();
        assertex(segSize == keyedSize);
        recalculateCache();
        modified = false;
    }
}

void SegMonitorList::recalculateCache()
{
    cachedLRS = _lastRealSeg();
}

void SegMonitorList::reset()
{
    segMonitors.kill();
    modified = true;
}

// interface IIndexReadContext
void SegMonitorList::append(IKeySegmentMonitor *segment)
{
    modified = true;
    unsigned fieldIdx = segment->queryFieldIndex();
    while (segMonitors.length() < fieldIdx)
    {
        unsigned idx = segMonitors.length();
        size32_t offset = recInfo.getFixedOffset(idx);
        size32_t size = recInfo.getFixedOffset(idx+1) - offset;
        segMonitors.append(*createWildKeySegmentMonitor(idx, offset, size));
    }
    segMonitors.append(*segment);
}

void SegMonitorList::append(FFoption option, const IFieldFilter * filter)
{
    throwUnexpected();
}

///
static UnexpectedVirtualFieldCallback unexpectedFieldCallback;
class jhtree_decl CKeyLevelManager : implements IKeyManager, public CInterface
{
protected:
    IContextLogger *ctx = nullptr;
    IContextLogger *activeCtx = nullptr;
    Owned <IIndexFilterList> filter;
    IKeyCursor *keyCursor;
    ConstPointerArray activeBlobs;
    __uint64 partitionFieldMask = 0;
    unsigned indexParts = 0;
    unsigned keyedSize;     // size of non-payload part of key
    bool started = false;
    bool newFilters = false;
    bool logExcessiveSeeks = false;

    Owned<const IDynamicTransform> layoutTrans;
    MemoryBuffer buf;  // used when translating
    size32_t layoutSize = 0;
public:
    IMPLEMENT_IINTERFACE;

    CKeyLevelManager(const RtlRecord &_recInfo, IKeyIndex * _key, IContextLogger *_ctx, bool _newFilters, bool _logExcessiveSeeks)
    : ctx(_ctx), newFilters(_newFilters), logExcessiveSeeks(_logExcessiveSeeks)
    {
        if (newFilters)
            filter.setown(new IndexRowFilter(_recInfo));
        else
            filter.setown(new SegMonitorList(_recInfo));
        keyCursor = NULL;
        keyedSize = 0;
        setKey(_key);
    }

    ~CKeyLevelManager()
    {
        ::Release(keyCursor);
        releaseBlobs();
    }

    virtual unsigned numActiveKeys() const override
    {
        return keyCursor ? 1 : 0;
    }

    void setKey(IKeyIndexBase * _key)
    {
        ::Release(keyCursor);
        keyCursor = NULL;
        if (_key)
        {
            assertex(_key->numParts()==1);
            IKeyIndex *ki = _key->queryPart(0);
            keyCursor = ki->getCursor(filter, logExcessiveSeeks);
            if (keyedSize)
                assertex(keyedSize == ki->keyedSize());
            else
                keyedSize = ki->keyedSize();
            partitionFieldMask = ki->getPartitionFieldMask();
            indexParts = ki->numPartitions();

            // If TLK don't collect context information
            activeCtx = ki->isTopLevelKey() ? nullptr : ctx;
        }
    }

    virtual unsigned getPartition() override
    {
        if (partitionFieldMask)
        {
            hash64_t hash = HASH64_INIT;
            if (getBloomHash(partitionFieldMask, *filter, hash))
                return (((unsigned) hash) % indexParts) + 1;  // NOTE - the Hash distribute function that distributes the index when building will truncate to 32-bits before taking modulus - so we must too!
        }
        return 0;
    }

    virtual void setChooseNLimit(unsigned __int64 _rowLimit) override
    {
        // TODO ?
    }

    virtual void reset(bool crappyHack)
    {
        if (keyCursor)
        {
            if (!started)
            {
                started = true;
                filter->checkSize(keyedSize, keyCursor->queryName());
            }
            if (!crappyHack)
            {
                keyCursor->reset();
            }
        }
    }

    virtual void releaseSegmentMonitors()
    {
        filter->reset();
        started = false;
    }

    virtual void append(IKeySegmentMonitor *segment) 
    { 
        assertex(!newFilters && !started);
        filter->append(segment);
    }


    virtual void append(FFoption option, const IFieldFilter * fieldFilter)
    {
        assertex(newFilters && !started);
        filter->append(option, fieldFilter);
    }

    inline const byte *queryKeyBuffer() override
    {
        if(layoutTrans)
        {
            buf.setLength(0);
            MemoryBufferBuilder aBuilder(buf, 0);
            layoutSize = layoutTrans->translate(aBuilder, unexpectedFieldCallback, keyCursor->queryRecordBuffer());
            return aBuilder.getSelf();
        }
        else
            return keyCursor->queryRecordBuffer();
    }

    inline size32_t queryRowSize()
    {
        if (layoutTrans)
            return layoutSize;
        else
            return keyCursor ? keyCursor->getSize() : 0;
    }

    inline unsigned __int64 querySequence()
    {
        return keyCursor ? keyCursor->getSequence() : 0;
    }

    virtual offset_t queryFPos() const override
    {
        return keyCursor ? keyCursor->getFPos() : 0;
    }

    virtual bool lookup(bool exact)
    {
        if (keyCursor)
            return keyCursor->lookup(exact, activeCtx);
        else
            return false;
    }

    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        return keyCursor ? keyCursor->lookupSkip(seek, seekOffset, seeklen, activeCtx) : false;
    }

    unsigned __int64 getCount()
    {
        assertex(keyCursor);
        return keyCursor->getCount(activeCtx);
    }

    unsigned __int64 getCurrentRangeCount(unsigned groupSegCount)
    {
        assertex(keyCursor);
        return keyCursor->getCurrentRangeCount(groupSegCount, activeCtx);
    }

    bool nextRange(unsigned groupSegCount)
    {
        assertex(keyCursor);
        return keyCursor->nextRange(groupSegCount);
    }

    unsigned __int64 checkCount(unsigned __int64 max)
    {
        assertex(keyCursor);
        return keyCursor->checkCount(max, activeCtx);
    }

    virtual void serializeCursorPos(MemoryBuffer &mb)
    {
        keyCursor->serializeCursorPos(mb);
    }

    virtual void deserializeCursorPos(MemoryBuffer &mb)
    {
        keyCursor->deserializeCursorPos(mb, activeCtx);
    }

    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx)
    {
        const byte *ret = keyCursor->loadBlob(blobid, blobsize, ctx);
        activeBlobs.append(ret);
        return ret;
    }

    virtual void releaseBlobs()
    {
        ForEachItemIn(idx, activeBlobs)
        {
            free((void *) activeBlobs.item(idx));
        }
        activeBlobs.kill();
    }

    virtual void setLayoutTranslator(const IDynamicTransform * trans) override
    {
        layoutTrans.set(trans);
    }

    virtual void finishSegmentMonitors()
    {
        filter->finish(keyedSize);
    }

    virtual void describeFilter(StringBuffer &out) const override
    {
        filter->describe(out);
    }

    virtual void mergeStats(CRuntimeStatisticCollection & targetStats) const
    {
        if (keyCursor)
            keyCursor->mergeStats(targetStats); // merge IO stats
    }
};


///////////////////////////////////////////////////////////////////////////////

enum CacheType : unsigned
{
    CacheBranch = 0,
    CacheLeaf = 1,
    CacheBlob = 2,
    //CacheTLK?
    CacheMax = 3
};
static constexpr const char * cacheTypeText[CacheMax]  = { "branch", "leaf", "blob" };

static_assert((unsigned)CacheBranch == (unsigned)NodeBranch, "Mismatch Cache Branch");
static_assert((unsigned)CacheLeaf == (unsigned)NodeLeaf, "Mismatch Cache Leaf");
static_assert((unsigned)CacheBlob == (unsigned)NodeBlob, "Mismatch Cache Blob");

constexpr StatisticKind addStatId[CacheMax] = { StNumNodeCacheAdds, StNumLeafCacheAdds, StNumBlobCacheAdds };
constexpr StatisticKind hitStatId[CacheMax] = { StNumNodeCacheHits, StNumLeafCacheHits, StNumBlobCacheHits };
constexpr StatisticKind loadStatId[CacheMax] = { StCycleNodeLoadCycles, StCycleLeafLoadCycles, StCycleBlobLoadCycles };
constexpr StatisticKind readStatId[CacheMax] = { StCycleNodeReadCycles, StCycleLeafReadCycles, StCycleBlobReadCycles };
constexpr StatisticKind fetchStatId[CacheMax] = { StNumNodeDiskFetches, StNumLeafDiskFetches, StNumBlobDiskFetches };
constexpr StatisticKind fetchTimeId[CacheMax] = { StCycleNodeFetchCycles, StCycleLeafFetchCycles, StCycleBlobFetchCycles };

///////////////////////////////////////////////////////////////////////////////

// For some reason #pragma pack does not seem to work here. Force all elements to 8 bytes
class CKeyIdAndPos
{
public:
    unsigned __int64 keyId;
    offset_t pos;

    CKeyIdAndPos(unsigned __int64 _keyId, offset_t _pos) { keyId = _keyId; pos = _pos; }

    bool operator==(const CKeyIdAndPos &other) { return keyId == other.keyId && pos == other.pos; }
};

class CNodeCacheEntry : public CInterface
{
public:
    CriticalSection cs;
private:
    std::atomic<const CJHTreeNode *> node{nullptr};
public:
    ~CNodeCacheEntry()
    {
        ::Release(node.load());
    }
    inline bool isReady() const
    {
        return node != nullptr;
    }
    inline const CJHTreeNode *queryNode() const
    {
        return node;
    }
    inline const CJHTreeNode *getNode() const
    {
        assert(isReady());
        return ::LINK(node.load());
    }
    inline void noteReady(const CJHTreeNode *_node)
    {
        node = _node;
    }
};

class CNodeMapping : public HTMapping<CNodeCacheEntry, CKeyIdAndPos>
{
public:
    CNodeMapping(CKeyIdAndPos &fp, CNodeCacheEntry &et) : HTMapping<CNodeCacheEntry, CKeyIdAndPos>(et, fp) { }
    ~CNodeMapping() { this->et.Release(); }
    const CJHTreeNode *queryNode()
    {
        return queryElement().queryNode();
    }
    CNodeCacheEntry &query() { return queryElement(); }

//The following pointers are used to maintain the position in the LRU cache
    CNodeMapping * prev = nullptr;
    CNodeMapping * next = nullptr;
};

class DelayedCacheEntryReleaser : public IRemovedMappingCallback
{
    //This number should be high enough so that in all common cases it is not exceeded.
    //There is little downside in making it too large - other than cache locality on the stack.
    //It will depend on the number of worker threads and the variation in expanded size of the
    //different leaf nodes.  Running stresstest with 15 workers and the default compression I
    //have seen numbers as high as 30.
    static constexpr unsigned maxFixed = 40;
public:
    ~DelayedCacheEntryReleaser()
    {
        if (unlikely(recordingEvents()))
        {
            try
            {
                for (unsigned i1 = 0; i1 < numFixed; i1++)
                    noteEviction(fixedFileId[i1], fixedPending[i1]);

                ForEachItemIn(i2, pending)
                    noteEviction(pendingFileIds.item(i2), &pending.item(i2));
            }
            catch (IException * e)
            {
                EXCLOG(e, "~DelayedCacheEntryReleaser");
                e->Release();
            }
        }

        for (unsigned i=0; i < numFixed; i++)
            fixedPending[i]->Release();
    }
    virtual void noteRemoval(void * _mapping) override
    {
        CNodeMapping *mapping = reinterpret_cast<CNodeMapping *>(_mapping);
        unsigned keyId = mapping->queryFindValue().keyId;
        // Save the node onto a list, so it will be released when this object is released.
        CJHTreeNode * node = const_cast<CJHTreeNode *>(mapping->query().getNode());
        // The key id needs to be stored separately because the node does not currently contain it.
        if (numFixed < maxFixed)
        {
            fixedPending[numFixed] = node;
            fixedFileId[numFixed] = keyId;
            numFixed++;
        }
        else
        {
            pending.append(*node);
            pendingFileIds.append(keyId);
        }
    }

protected:
    void noteEviction(unsigned keyId, CJHTreeNode * node)
    {
        offset_t pos = node->getFpos();
        queryRecorder().recordIndexEviction(keyId, pos, node->getNodeType(), node->getMemSize());
    }

protected:
    CIArrayOf<CJHTreeNode> pending;
    UnsignedArray pendingFileIds;
    //Use a fixed array for a small number of allocations to avoid a heap allocation inside the critsec
    CJHTreeNode * fixedPending[maxFixed]; // deliberately uninitialized
    unsigned fixedFileId[maxFixed]; // deliberately uninitialized
    unsigned numFixed = 0;

};

typedef OwningSimpleHashTableOf<CNodeMapping, CKeyIdAndPos> CNodeTable;
class CNodeMRUSubCache final : public CMRUCacheOf<CKeyIdAndPos, CNodeCacheEntry, CNodeMapping, CNodeTable>
{
    std::atomic<size_t> sizeInMem{0};
    size_t memLimit = 0;
public:
    mutable CriticalSection lock;
    RelaxedAtomic<__uint64> numHits{0};
    RelaxedAtomic<__uint64> numAdds{0};
    RelaxedAtomic<__uint64> numDups{0};
    RelaxedAtomic<__uint64> numEvicts{0};

    size_t setMemLimit(size_t _memLimit)
    {
        size_t oldMemLimit = memLimit;
        memLimit = _memLimit;
        if (full())
            makeSpace(nullptr);
        return oldMemLimit;
    }
    virtual void makeSpace(IRemovedMappingCallback * callback)
    {
        // remove LRU until !full
        // This code could walk the list, rather than restarting at the end each time - but there are unlikely to be
        // many entries that have no associated node, and nodes could have been associated in the meantime.
        // Note that we are called inside a lock
        do
        {
            CNodeMapping *tail = mruList.tail();
            if (unlikely(!tail))
                throw makeStringExceptionV(9999, "Index cache appears full but contains no entries size=%zx limit=%zx", sizeInMem.load(), memLimit);

            //Never evict an entry that hasn't yet loaded - otherwise the sizeInMem can become inconsistent
            //When running with slow remote storage this can take a long time to be ready - so we need
            //to walk on to the next entry in the lrulist, otherwise we can run out of memory since nothing
            //would be removed.
            while (!tail->queryElement().isReady())
            {
                tail = tail->prev;
                if (!tail)
                {
                     // no pages in the cache are ready - this could possibly happen in a tiny race-window where
                     // sizes in the cache have been updated, but no nodes have yet been associated with the entries.
                    return;
                }
            }

            mruList.remove(tail);
            numEvicts.fastAdd(1);
            if (callback)
                callback->noteRemoval(tail);
            table.removeExact(tail);
        }
        while (full());
    }
    virtual bool full()
    {
        if (((size_t)-1) == memLimit) return false;
        return sizeInMem > memLimit;
    }
    virtual void elementRemoved(CNodeMapping *mapping)
    {
        const CJHTreeNode *node = mapping->queryNode();
        //Node may be null if there was a failure loading the node from disk for some reason.
        if (node)
            sizeInMem -= node->getMemSize();
    }
    void reportEntries(ICacheInfoRecorder &cacheInfo)
    {
        CriticalBlock block(lock);
        Owned<CNodeMRUSubCache::CMRUIterator> iter = getIterator();
        ForEach(*iter)
        {
            CNodeMapping &mapping = iter->query();
            const CKeyIdAndPos &key = mapping.queryFindValue();
            const CNodeCacheEntry &entry = mapping.queryElement();
            if (entry.isReady())
            {
                const CJHTreeNode *node = mapping.queryNode();
                cacheInfo.noteWarm(key.keyId, key.pos, node->getNodeDiskSize(), node->getNodeType());
            }
        }
    }
    void noteReady(const CJHTreeNode &node)
    {
        sizeInMem += node.getMemSize();
    }
    void traceState(StringBuffer & out)
    {
        //Should be safe to call outside of a critical section, but values may be inconsistent
        out.append(table.ordinality()).append(":").append((__uint64)sizeInMem);
        out.appendf(" [%" I64F "u:%" I64F "u:%" I64F "u:%" I64F "u]", numHits.load(), numAdds.load(), numDups.load(), numEvicts.load());
    }
    unsigned __int64 getStatisticValue(StatisticKind kind) const
    {
        switch (kind)
        {
        case StNumCacheAdds:
            return numAdds.load();
        case StNumCacheHits:
            return numHits.load();
        case StNumCacheDuplicates:
            return numDups.load();
        case StNumCacheEvictions:
            return numEvicts.load();
        }
        return 0;
    }
};

// Maximum cost/benefit seen for 4 buckets, which does not skew the LRU list too much
static constexpr unsigned cacheBits = 2;
static constexpr unsigned cacheBuckets = 1U << cacheBits;
static constexpr unsigned cacheShift = 32 - cacheBits;


class CNodeMRUCache
{
    using IMetric = hpccMetrics::IMetric;
    class CacheMetric final : public hpccMetrics::MetricBase
    {
    public:
        CacheMetric(const char * name, const char * desc, StatisticKind _kind, CNodeMRUCache & _cache) :
            MetricBase(name, desc, hpccMetrics::METRICS_COUNTER, SMeasureCount, hpccMetrics::MetricMetaData()),
            cache(_cache), kind(_kind)
        {
        }

        virtual __uint64 queryValue() const
        {
            return cache.getStatisticValue(kind);
        }

    protected:
        CNodeMRUCache & cache;
        StatisticKind kind;
    };

    void createCacheMetric(const char * name, const char * desc, StatisticKind kind)
    {
        std::shared_ptr<IMetric> pMetric = std::shared_ptr<IMetric>(new CacheMetric(name, desc, kind, *this));
        hpccMetrics::queryMetricsManager().addMetric(pMetric);
        metrics.push_back(std::move(pMetric));
    }

public:
    CNodeMRUCache(CacheType cacheType)
    {
        StringBuffer name, desc;
        const char *typeText = cacheTypeText[cacheType];
        name.appendf("jhtree.cache.%s.hits", typeText);
        desc.appendf("The number of cache hits on the jhtree %s cache", typeText);
        createCacheMetric(name, desc, StNumCacheHits);
        name.clear().appendf("jhtree.cache.%s.adds", typeText);
        desc.clear().appendf("The number of cache adds to the jhtree %s cache", typeText);
        createCacheMetric(name, desc, StNumCacheAdds);
        name.clear().appendf("jhtree.cache.%s.dups", typeText);
        desc.clear().appendf("The number of cache add collisions on the jhtree %s cache", typeText);
        createCacheMetric(name, desc, StNumCacheDuplicates);
        name.clear().appendf("jhtree.cache.%s.evictions", typeText);
        desc.clear().appendf("The number of nodes evicted from the jhtree %s cache", typeText);
        createCacheMetric(name, desc, StNumCacheEvictions);
    }

    inline unsigned getKeyHash(const CKeyIdAndPos & key)
    {
        return cache[0].getKeyHash(key);
    }

    __uint64 getStatisticValue(StatisticKind kind) const
    {
        __uint64 total = 0;
        for (unsigned i =0; i < cacheBuckets; i++)
            total += cache[i].getStatisticValue(kind);
        return total;
    }

    void reportEntries(ICacheInfoRecorder &cacheInfo)
    {
        for (unsigned j = 0; j < cacheBuckets; j++)
            cache[j].reportEntries(cacheInfo);
    }

    size_t setCacheMem(size_t newSize)
    {
        size_t oldV = 0;
        for (unsigned i=0; i < cacheBuckets; i++)
        {
            CriticalBlock block(cache[i].lock);
            oldV += cache[i].setMemLimit(newSize/cacheBuckets);
        }
        enabled = newSize != 0;
        return oldV;
    }

    void traceState(StringBuffer & out)
    {
        if (enabled)
        {
            for (unsigned j=0; j < cacheBuckets; j++)
            {
                if (j)
                    out.append(' ');
                cache[j].traceState(out);
            }
        }
        else
        {
            out.append("[disabled]");
        }
    }
    CNodeMRUSubCache cache[cacheBuckets];
    bool enabled = false;
protected:
    std::vector<std::shared_ptr<IMetric>> metrics;
};

class CNodeCache : public CInterface
{
private:
    CNodeMRUCache cache[CacheMax] = { CacheBranch, CacheLeaf, CacheBlob };
    std::vector<std::shared_ptr<hpccMetrics::IMetric>> metrics;
public:
    CNodeCache(size_t maxNodeMem, size_t maxLeaveMem, size_t maxBlobMem)
    {
        setNodeCacheMem(maxNodeMem);
        setLeafCacheMem(maxLeaveMem);
        setBlobCacheMem(maxBlobMem);
        // note that each index caches the last blob it unpacked so that sequential blobfetches are still ok
    }
    const CJHTreeNode *getCachedNode(const INodeLoader *key, unsigned keyID, offset_t pos, NodeType type, IContextLogger *ctx, bool isTLK);
    void getCacheInfo(ICacheInfoRecorder &cacheInfo);

    inline size_t setNodeCacheMem(size_t newSize)
    {
        return setCacheMem(newSize, CacheBranch);
    }
    inline size_t setLeafCacheMem(size_t newSize)
    {
        return setCacheMem(newSize, CacheLeaf);
    }
    inline size_t setBlobCacheMem(size_t newSize)
    {
        return setCacheMem(newSize, CacheBlob);
    }
    void clear()
    {
        for (unsigned i=0; i < CacheMax; i++)
        {
            for (unsigned j=0; j < cacheBuckets; j++)
            {
                CriticalBlock block(cache[i].cache[j].lock);
                cache[i].cache[j].kill();
            }
        }
    }
    void traceState(StringBuffer & out)
    {
        for (unsigned i=0; i < CacheMax; i++)
        {
            out.append(cacheTypeText[i]).append('(');
            cache[i].traceState(out);
            out.append(") ");
        }
    }
    void logState()
    {
        StringBuffer state;
        traceState(state);
        DBGLOG("NodeCache: %s", state.str());
    }

protected:
    size_t setCacheMem(size_t newSize, CacheType type)
    {
        return cache[type].setCacheMem(newSize);
    }
};

static inline CNodeCache *queryNodeCache()
{
    if (nodeCache) return nodeCache; // avoid crit
    CriticalBlock b(*initCrit);
    if (!nodeCache) nodeCache = new CNodeCache(100*0x100000, 50*0x100000, 0);
    return nodeCache;
}

void clearNodeCache()
{
    queryNodeCache()->clear();
}

void logNodeCacheStats(const char *prefix)
{
    if (nodeCache)
    {
        StringBuffer str(prefix);
        nodeCache->traceState(str);
        DBGLOG("%s", str.str());
    }
}

void logCacheState()
{
    if (nodeCache)
        nodeCache->logState();
    DBGLOG("Search times branch(%lluns) leaf(%lluns)", cycle_to_nanosec(branchSearchCycles), cycle_to_nanosec(leafSearchCycles));
    branchSearchCycles = 0;
    leafSearchCycles = 0;
}

inline CKeyStore *queryKeyStore()
{
    CKeyStore * value = keyStore.load(std::memory_order_acquire);
    if (value) return value; // avoid crit
    CriticalBlock b(*initCrit);
    if (!keyStore.load(std::memory_order_acquire)) keyStore = new CKeyStore;
    return keyStore;
}

unsigned setKeyIndexCacheSize(unsigned limit)
{
    return queryKeyStore()->setKeyCacheLimit(limit);
}

CKeyStore::CKeyStore() : keyIndexCache(defaultKeyIndexLimit)
{
#if 0
    mm.setown(createSharedMemoryManager("RichardsSharedMemManager", 0x100000));
    try
    {
        if (mm)
            sharedCache.setown(mm->share());
    }
    catch (IException *E)
    {
        E->Release();
    }
#endif
}

CKeyStore::~CKeyStore()
{
}

unsigned CKeyStore::getUniqId(unsigned useId, const char * filename)
{
    unsigned id;
    if (useId != (unsigned) -1)
        id = useId;
    else
        id = ++nextId;

    //This is unfortunate that this is within the critical section, but if it is not then the event stream
    //can contain references to ids before the file information is recorded.
    //
    //Index adds are unusual, so it is unlikely to cause problems in practice.
    //
    //It has to be in this function because creating a file class will generate some event data as a side-effect.
    if (unlikely(recordingEvents()))
        queryRecorder().recordFileInformation(id, filename);

    return id;
}

unsigned CKeyStore::setKeyCacheLimit(unsigned limit)
{
    synchronized block(mutex);
    return keyIndexCache.setCacheLimit(limit);
}

IKeyIndex *CKeyStore::doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, unsigned fileIdx, IMemoryMappedFile *iMappedFile, bool isTLK, size32_t blockedIOSize)
{
    // isTLK provided by caller since flags in key header unreliable. If either say it's a TLK, I believe it.
    IKeyIndex *keyIndex;
    StringBuffer fname;
    fname.append(fileName).append('/').append(crc).append('/').append(blockedIOSize);

    // MORE - holds onto the mutex way too long
    synchronized block(mutex);
    keyIndex = keyIndexCache.query(fname);
    if (NULL == keyIndex)
    {
        if (iMappedFile)
        {
            assert(!iFileIO && !part);
            keyIndex = new CMemKeyIndex(getUniqId(fileIdx, fileName), LINK(iMappedFile), fname, isTLK);
        }
        else if (iFileIO)
        {
            assert(!part);
            keyIndex = new CDiskKeyIndex(getUniqId(fileIdx, fileName), LINK(iFileIO), fname, isTLK, blockedIOSize);
        }
        else
        {
            assert(fileIdx==(unsigned) -1);
            Owned<IFile> iFile;
            if (part)
            {
                iFile.setown(part->open());
                if (NULL == iFile.get())
                    throw MakeStringException(0, "Failed to open index file %s", fileName);
            }
            else
                iFile.setown(createIFile(fileName));
            IFileIO *fio = iFile->open(IFOread);
            if (fio)
                keyIndex = new CDiskKeyIndex(getUniqId(fileIdx, fileName), fio, fname, isTLK, blockedIOSize);
            else
                throw MakeStringException(0, "Failed to open index file %s", fileName);
        }
        keyIndexCache.replace(fname, *LINK(keyIndex));
    }
    else
    {
        LINK(keyIndex);
    }
    assertex(NULL != keyIndex);
    return keyIndex;
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, IFileIO *iFileIO, unsigned fileIdx, bool isTLK, size32_t blockedIOSize)
{
    return doload(fileName, crc, NULL, iFileIO, fileIdx, NULL, isTLK, blockedIOSize);
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, size32_t blockedIOSize)
{
    return doload(fileName, crc, NULL, NULL, (unsigned) -1, iMappedFile, isTLK, blockedIOSize);
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, bool isTLK, size32_t blockedIOSize)
{
    return doload(fileName, crc, NULL, NULL, (unsigned) -1, NULL, isTLK, blockedIOSize);
}

StringBuffer &CKeyStore::getMetrics(StringBuffer &xml)
{
    xml.append(" <IndexMetrics>\n");

    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        const char *name = mapping.queryFindString();
        xml.appendf(" <Index name=\"%s\" scans=\"%d\" seeks=\"%d\"/>\n", name, index.queryScans(), index.querySeeks());
    }
    xml.append(" </IndexMetrics>\n");
    return xml;
}

//MORE: This should be called sparingly because it locks all access via the hash table
void CKeyStore::recordEventIndexInformation()
{
    if (!recordingEvents())
        return;

    EventRecorder & recorder = queryRecorder();
    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
    ForEach(*iter)
    {
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        unsigned id = index.queryId();
        const char *name = mapping.queryFindString();
        recorder.recordFileInformation(id, name);
    }
}

void CKeyStore::resetMetrics()
{
    synchronized block(mutex);

    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        index.resetCounts();
    }
}

void CKeyStore::clearCache(bool killAll)
{
    synchronized block(mutex);

    if (killAll)
    {
        clearNodeCache(); // no point in keeping old nodes cached if key store cache has been cleared
        keyIndexCache.kill();
    }
    else
    {
        StringArray goers;
        Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
        ForEach(*iter)
        {           
            CKeyIndexMapping &mapping = iter->query();
            IKeyIndex &index = mapping.query();
            if (!index.IsShared())
            {
                const char *name = mapping.queryFindString();
                goers.append(name);
            }
        }
        ForEachItemIn(idx, goers)
        {
            keyIndexCache.remove(goers.item(idx));
        }
    }
}

void CKeyStore::clearCacheEntry(const char *keyName)
{
    if (!keyName || !*keyName)
        return;  // nothing to do

    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();

    StringArray goers;
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        if (!index.IsShared())
        {
            const char *name = mapping.queryFindString();
            if (strstr(name, keyName) != 0)  // keyName doesn't have drive or part number associated with it
                goers.append(name);
        }
    }
    ForEachItemIn(idx, goers)
    {
        keyIndexCache.remove(goers.item(idx));
    }
}

void CKeyStore::clearCacheEntry(const IFileIO *io)
{
    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();

    StringArray goers;
    ForEach(*iter)
    {
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        if (!index.IsShared())
        {
            if (index.queryFileIO()==io)
                goers.append(mapping.queryFindString());
        }
    }
    ForEachItemIn(idx, goers)
    {
        keyIndexCache.remove(goers.item(idx));
    }
}

//

static void noteSeeks(IContextLogger *ctx, unsigned lseeks, unsigned lscans, unsigned lwildseeks)
{
    if (ctx)
    {
        if (lseeks) ctx->noteStatistic(StNumIndexSeeks, lseeks);
        if (lscans) ctx->noteStatistic(StNumIndexScans, lscans);
        if (lwildseeks) ctx->noteStatistic(StNumIndexWildSeeks, lwildseeks);
    }
}

static void noteSkips(IContextLogger *ctx, unsigned lskips, unsigned lnullSkips)
{
    if (ctx)
    {
        if (lskips) ctx->noteStatistic(StNumIndexSkips, lskips);
        if (lnullSkips) ctx->noteStatistic(StNumIndexNullSkips, lnullSkips);
    }
}


// CKeyIndex impl.

CKeyIndex::CKeyIndex(unsigned _iD, const char *_name) : name(_name)
{
    iD = _iD;
    cache = queryNodeCache(); // use one node cache for all key indexes;
    cache->Link();
    keyHdr = NULL;
    rootNode = nullptr;
    cachedBlobNodePos = 0;
    keySeeks.store(0);
    keyScans.store(0);
    latestGetNodeOffset = 0;
}

const CJHSearchNode *CKeyIndex::getRootNode() const
{
    offset_t rootPos = keyHdr->getRootFPos();
    Linked<CNodeCache> nodeCache = queryNodeCache();
    // The root node may be a branch or a leaf (on TLK nodes)
    NodeType type = getBranchDepth() != 0 ? NodeBranch : NodeLeaf;
    Owned<const CJHSearchNode> root = (const CJHSearchNode *) nodeCache->getCachedNode(this, iD, rootPos, type, NULL, isTopLevelKey());

    // It's not uncommon for a TLK to have a "root node" that has a single entry in it pointing to a leaf node
    // with all the info in. In such cases we can avoid a lot of cache lookups by pointing the "root" in the
    // CKeyIndex directly to the (single) leaf.
    // You might think it would be ok to do this for non TLK indexes, but it will not work without changes elsewhere.
    // This extra layer is no longer generated for TLKs unless required, but skipping it here means older 
    // indexes benefit too.
    if (root && isTopLevelKey() && !root->isLeaf() && root->getNumKeys()==1)
    {
        Owned<const CJHSearchNode> oldRoot = root;
        rootPos = root->getFPosAt(0);
        root.setown((const CJHSearchNode *) nodeCache->getCachedNode(this, iD, rootPos, NodeLeaf, NULL, true));
    }
    return root.getClear();
}

void CKeyIndex::init(KeyHdr &hdr, bool isTLK)
{
    if (isTLK)
        hdr.ktype |= HTREE_TOPLEVEL_KEY; // Once upon a time, thor did not set
    else if (hdr.ktype & HTREE_TOPLEVEL_KEY)
        isTLK = true;
    assertex((hdr.ktype & COL_PREFIX) != 0);   // We have not generated a key without COL_PREFIX set for over 20 years

    keyHdr = new CKeyHdr();
    try
    {
        keyHdr->load(hdr);
        rootNode = getRootNode();
    }
    catch (IKeyException *ke)
    {
        if (!name.get()) throw;
        StringBuffer msg;
        IKeyException *ke2 = MakeKeyException(ke->errorCode(), "%s. In key '%s' (corrupt index?)", ke->errorMessage(msg).str(), name.get());
        ke->Release();
        throw ke2;
    }
}

CKeyIndex::~CKeyIndex()
{
    ::Release(keyHdr);
    ::Release(cache);
    ::Release(rootNode);
}

CMemKeyIndex::CMemKeyIndex(unsigned _iD, IMemoryMappedFile *_io, const char *_name, bool isTLK)
    : CKeyIndex(_iD, _name)
{
    io.setown(_io);
    assertex(io->offset()==0);                  // mapped whole file
    assertex(io->length()==io->fileSize());     // mapped whole file
    KeyHdr hdr;
    if (io->length() < sizeof(hdr))
        throw MakeStringException(0, "Failed to read key header: file too small, could not read %u bytes", (unsigned) sizeof(hdr));
    memcpy(&hdr, io->base(), sizeof(hdr));

    if (hdr.ktype & USE_TRAILING_HEADER)
    {
        _WINREV(hdr.nodeSize);
        memcpy(&hdr, (io->base()+io->length()) - hdr.nodeSize, sizeof(hdr));
    }
    init(hdr, isTLK);
}

const CJHTreeNode *CMemKeyIndex::loadNode(cycle_t * fetchCycles, offset_t pos, IFileIO *) const
{
    nodesLoaded++;
    if (pos + keyHdr->getNodeSize() > io->fileSize())
    {
        IException *E = MakeStringException(errno, "Error reading node at position %" I64F "x past EOF", pos); 
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw E;
    }
    char *nodeData = (char *) (io->base() + pos);
    return CKeyIndex::_loadNode(nodeData, pos, false);
}

CDiskKeyIndex::CDiskKeyIndex(unsigned _iD, IFileIO *_io, const char *_name, bool isTLK, size32_t _blockedIOSize)
    : CKeyIndex(_iD, _name)
{
    blockedIOSize = _blockedIOSize;
    io.setown(_io);
    KeyHdr hdr;
    offset_t sizeRead = io->read(0, sizeof(hdr), &hdr);
    if (sizeRead != sizeof(hdr))
        throw MakeStringException(0, "Failed to read key '%s' header: file too small, could not read %u bytes - read %u", _name, (unsigned) sizeof(hdr), (unsigned)sizeRead);

#ifdef _DEBUG
    //In debug mode always use the trailing header if it is available to ensure that code path is tested
    if (hdr.ktype & USE_TRAILING_HEADER)
#else
    if (hdr.ktype & TRAILING_HEADER_ONLY)
#endif
    {
        _WINREV(hdr.nodeSize);
        offset_t actualSize = io->size();
        offset_t readOffset = actualSize - hdr.nodeSize;
        sizeRead = io->read(readOffset, sizeof(hdr), &hdr);
        if (sizeRead != sizeof(hdr))
            throw MakeStringException(4, "Invalid key %s: failed to read trailing key header at offset %llu, read %u", _name, readOffset, (unsigned)sizeRead);
    }
    init(hdr, isTLK);
}

const CJHTreeNode *CDiskKeyIndex::loadNode(cycle_t * fetchCycles, offset_t pos, IFileIO *useIO) const
{
    nodesLoaded++;
    if (!useIO) useIO = io;
    unsigned nodeSize = keyHdr->getNodeSize();

    //Use alloca() to allocate a buffer on the stack if the node size is small enough.
    //Often the data could be read directly from the input files's buffer and not even be copied.
    //Should we have a io->peek(pos, size) which returns a pointer if supported?
    constexpr const size_t maxStackSize = 8192; // Default node size
    MemoryAttr ma;
    char *nodeData;
    if (nodeSize <= maxStackSize)
        nodeData = (char *) alloca(nodeSize);
    else
        nodeData = (char *) ma.allocate(nodeSize);
    assertex(nodeData);

    CCycleTimer fetchTimer(fetchCycles != nullptr);
    if (useIO->read(pos, nodeSize, nodeData) != nodeSize)
    {
        IException *E = MakeStringException(errno, "Error %d reading node at position %" I64F "x", errno, pos); 
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw E;
    }
    if (fetchCycles)
        *fetchCycles = fetchTimer.elapsedCycles();
    return CKeyIndex::_loadNode(nodeData, pos, true);
}

CJHTreeNode *CKeyIndex::_createNode(const NodeHdr &nodeHdr) const
{
    if (nodeHdr.compressionType == LegacyCompression)
    {
        switch(nodeHdr.nodeType)
        {
        case NodeBranch:
            return new CJHLegacySearchNode();
        case NodeLeaf:
            if (keyHdr->isVariable())
                return new CJHVarTreeNode();
            else if (keyHdr->isRowCompressed())
                return new CJHRowCompressedNode();
            else
                return new CJHLegacySearchNode();
        case NodeBlob:
            return new CJHTreeBlobNode();
        case NodeMeta:
            return new CJHTreeMetadataNode();
        case NodeBloom:
            return new CJHTreeBloomTableNode();
        default:
            throwUnexpected();
        }
    }
    else
    {
        switch(nodeHdr.compressionType)
        {
        case SplitPayload:
            assertex(nodeHdr.nodeType== NodeLeaf);    // Should only be using the new format for leaf nodes
            return new CJHSplitSearchNode();
        case InplaceCompression:
            if (nodeHdr.nodeType == NodeLeaf)
                return new CJHInplaceLeafNode();
            if (nodeHdr.nodeType == NodeBranch)
                return new CJHInplaceBranchNode();
            UNIMPLEMENTED;
        default:
            throwUnexpected();
        }
    }
}

CJHTreeNode *CKeyIndex::_loadNode(char *nodeData, offset_t pos, bool needsCopy) const
{
    try
    {
        Owned<CJHTreeNode> ret = _createNode(*(NodeHdr *) nodeData);
        ret->load(keyHdr, nodeData, pos, needsCopy);
        return ret.getClear();
    }
    catch (IException *E)
    {
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw;
    }
    catch (...)
    {
        DBGLOG("Unknown exception in key %s, position 0x%" I64F "x", name.get(), pos);
        throw;
    }
}

bool CKeyIndex::isTopLevelKey() const
{
    return isTLK();
}

bool CKeyIndex::isFullySorted()
{
    return (keyHdr->getKeyType() & HTREE_FULLSORT_KEY) != 0;
}

__uint64 CKeyIndex::getPartitionFieldMask()
{
    return keyHdr->getPartitionFieldMask();
}

unsigned CKeyIndex::numPartitions()
{
    return keyHdr->numPartitions();
}


IKeyCursor *CKeyIndex::getCursor(const IIndexFilterList *filter, bool logExcessiveSeeks)
{
    return new CKeyCursor(*this, filter, logExcessiveSeeks, blockedIOSize);
}

const CJHSearchNode *CKeyIndex::getIndexNode(offset_t offset, NodeType type, IContextLogger *ctx) const
{ 
    latestGetNodeOffset = offset;
    //Call isTLK() rather than isTopLevelKey() so the test is inlined (rather than a virtual)
    return (CJHSearchNode *)cache->getCachedNode(this, iD, offset, type, ctx, isTLK());
}

const CJHSearchNode *CKeyIndex::getIndexNodeUsingLoader(const INodeLoader *nodeLoader, offset_t offset, NodeType type, IContextLogger *ctx) const
{
    latestGetNodeOffset = offset;
    //Call isTLK() rather than isTopLevelKey() so the test is inlined (rather than a virtual)
    return (CJHSearchNode *)cache->getCachedNode(nodeLoader, iD, offset, type, ctx, isTLK());
}

void CKeyIndex::dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw)
{
    Owned<const CJHTreeNode> node = loadNode(nullptr, pos, nullptr);
    node->dump(out, keySize(), count, isRaw);
}

bool CKeyIndex::hasSpecialFileposition() const
{
    return keyHdr->hasSpecialFileposition();
}

bool CKeyIndex::needsRowBuffer() const
{
    return keyHdr->hasSpecialFileposition() || keyHdr->isRowCompressed();
}

size32_t CKeyIndex::keySize()
{
    size32_t fileposSize = keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0;
    return keyHdr->getMaxKeyLength() + fileposSize;
}

size32_t CKeyIndex::keyedSize()
{
    return keyHdr->getNodeKeyLength();
}

bool CKeyIndex::hasPayload()
{
    return keyHdr->hasPayload();
}

const CJHTreeBlobNode *CKeyIndex::getBlobNode(offset_t nodepos, IContextLogger *ctx)
{
    Owned<const CJHTreeBlobNode> match;
    cycle_t readCycles = 0;
    cycle_t fetchCycles = 0;
    {
        CriticalBlock b(cacheCrit);
        if (nodepos != cachedBlobNodePos)
        {

            CCycleTimer blobLoadTimer;
            Owned<const CJHTreeNode> node = loadNode(&fetchCycles, nodepos, nullptr); // note - don't use the cache
            assertex(node->isBlob());
            cachedBlobNode.setown(static_cast<const CJHTreeBlobNode *>(node.getClear()));
            cachedBlobNodePos = nodepos;
            readCycles = blobLoadTimer.elapsedCycles();
        }
        match.set(cachedBlobNode);
    }

    if (ctx)
    {
        if (readCycles != 0)
        {
            ctx->noteStatistic(StNumBlobCacheAdds, 1);
            ctx->noteStatistic(StCycleBlobLoadCycles, readCycles);
            ctx->noteStatistic(StCycleBlobReadCycles, fetchCycles);
            if (fetchCycles >= fetchThresholdCycles)
            {
                ctx->noteStatistic(StNumBlobDiskFetches, 1);
                ctx->noteStatistic(StCycleBlobFetchCycles, fetchCycles);
            }
        }
        else
            ctx->noteStatistic(StNumBlobCacheHits, 1);
    }

    return match.getClear();
}

const byte *CKeyIndex::loadBlob(unsigned __int64 blobid, size32_t &blobSize, IContextLogger *ctx)
{
    offset_t nodepos = blobid & I64C(0xffffffffffff);
    size32_t offset = (size32_t) ((blobid & I64C(0xffff000000000000)) >> 44);

    Owned<const CJHTreeBlobNode> blobNode = getBlobNode(nodepos, ctx);
    size32_t sizeRemaining = blobNode->getTotalBlobSize(offset);
    blobSize = sizeRemaining;
    byte *ret = (byte *) malloc(sizeRemaining);
    byte *finger = ret;
    for (;;)
    {
        size32_t gotHere = blobNode->getBlobData(offset, finger);
        assertex(gotHere <= sizeRemaining);
        sizeRemaining -= gotHere;
        finger += gotHere;
        if (!sizeRemaining)
            break;
        blobNode.setown(getBlobNode(blobNode->getRightSib(), ctx));
        offset = 0;
    }
    return ret;
}

offset_t CKeyIndex::queryMetadataHead()
{
    offset_t ret = keyHdr->getHdrStruct()->metadataHead;
    if(ret == static_cast<offset_t>(-1)) ret = 0; // index created before introduction of metadata would have FFFF... in this space
    return ret;
}

offset_t CKeyIndex::queryFirstBranchOffset()
{
    unsigned branchDepth = getBranchDepth();
    if (branchDepth == 0) // Only a single leaf - return 0
        return 0;
    if (branchDepth == 1) // a single branch node - return the offset of the node
        return keyHdr->getRootFPos();

    Linked<const CJHSearchNode> cur = rootNode;
    for (unsigned nextBranch = 2; ; nextBranch++)
    {
        offset_t branchOffset = cur->getFPosAt(0);
        if (nextBranch == branchDepth)
            return branchOffset;
        IContextLogger * ctx = nullptr;
        cur.setown(getIndexNode(branchOffset, NodeBranch, ctx));
    }
}

void CKeyIndex::loadBloomFilters()
{
    offset_t bloomAddr = keyHdr->getHdrStruct()->bloomHead;
    if (!bloomAddr || bloomAddr == static_cast<offset_t>(-1))
    {
        bloomFiltersLoaded = true;
        return; // indexes created before introduction of bloomfilter would have FFFF... in this space
    }

    while (bloomAddr)
    {
        Owned<const CJHTreeNode> node = loadNode(nullptr, bloomAddr, nullptr);
        assertex(node->isBloom());
        CJHTreeBloomTableNode &bloomNode = *(CJHTreeBloomTableNode *)node.get();
        bloomAddr = bloomNode.get8();
        unsigned numHashes = bloomNode.get4();
        __uint64 fields =  bloomNode.get8();
        unsigned bloomTableSize = bloomNode.get4();
        MemoryBuffer bloomTable;
        bloomTable.ensureCapacity(bloomTableSize);
        for (;;)
        {
            static_cast<const CJHTreeBloomTableNode *>(node.get())->get(bloomTable);
            offset_t next = node->getRightSib();
            if (!next)
                break;
            node.setown(loadNode(nullptr, next, nullptr));
            assertex(node->isBloom());
        }
        assertex(bloomTable.length()==bloomTableSize);
        //DBGLOG("Creating bloomfilter(%d, %d) for fields %" I64F "x",numHashes, bloomTableSize, fields);
        bloomFilters.append(*new IndexBloomFilter(numHashes, bloomTableSize, (byte *) bloomTable.detach(), fields));
    }
    bloomFilters.sort(IndexBloomFilter::compare);
    bloomFiltersLoaded = true;
}

bool CKeyIndex::bloomFilterReject(const IIndexFilterList &segs) const
{
    if (segs.isUnfiltered())
        return false;
    if (!bloomFiltersLoaded)
    {
        CriticalBlock b(cacheCrit);
        if (!bloomFiltersLoaded)
            const_cast<CKeyIndex *>(this)->loadBloomFilters();
    }
    ForEachItemIn(idx, bloomFilters)
    {
        IndexBloomFilter &filter = bloomFilters.item(idx);
        if (filter.reject(segs))
            return true;
    }
    return false;
}

IPropertyTree * CKeyIndex::getMetadata()
{
    offset_t nodepos = queryMetadataHead();
    if(!nodepos)
        return NULL;
    Owned<const CJHTreeMetadataNode> node;
    StringBuffer xml;
    while(nodepos)
    {
        node.setown(QUERYINTERFACE(loadNode(nullptr, nodepos, nullptr), const CJHTreeMetadataNode));
        node->get(xml);
        nodepos = node->getRightSib();
    }
    IPropertyTree * ret;
    try
    {
        ret = createPTreeFromXMLString(xml.str());
    }
    catch(IPTreeReadException * e)
    {
        StringBuffer emsg;
        IException * wrapped = MakeStringException(e->errorAudience(), e->errorCode(), "Error retrieving XML metadata: %s", e->errorMessage(emsg).str());
        e->Release();
        throw wrapped;
    }
    return ret;
}

bool CKeyIndex::prewarmPage(offset_t offset, NodeType type)
{
    try
    {
        Owned<const CJHTreeNode> page = getIndexNode(offset, type, nullptr);
        return page != nullptr;
    }
    catch(IException *E)
    {
        ::Release(E);
    }
    return false;
}

const CJHSearchNode *CKeyIndex::locateFirstLeafNode(IContextLogger *ctx) const
{
    keySeeks++;

    offset_t leafOffset = keyHdr->getFirstLeafPos();
    if (leafOffset != (offset_t)-1)
    {
        if (leafOffset == 0)
            return nullptr;
        return getIndexNode(leafOffset, NodeLeaf, ctx);
    }

    //Unusual - an index with no elements
    if (keyHdr->getNumRecords() == 0)
        return nullptr;

    const CJHSearchNode * cur = LINK(rootNode);
    unsigned depth = 0;
    while (!cur->isLeaf())
    {
        const CJHTreeNode * prev = cur;
        depth++;
        NodeType type = (depth < getBranchDepth()) ? NodeBranch : NodeLeaf;
        cur = getIndexNode(cur->getFPosAt(0), type, ctx);
        assertex(cur);
        prev->Release();
    }
    return cur;
}

const CJHSearchNode *CKeyIndex::locateLastLeafNode(IContextLogger *ctx) const
{
    keySeeks++;
    noteSeeks(ctx, 1, 0, 0);

    //Unusual - an index with no elements
    if (keyHdr->getNumRecords() == 0)
        return nullptr;

    const CJHSearchNode * cur = LINK(rootNode);
    unsigned depth = 0;
    //First find the last leaf node pointed to by the higher level index
    while (!cur->isLeaf())
    {
        const CJHSearchNode * prev = cur;
        depth++;
        NodeType type = (depth < getBranchDepth()) ? NodeBranch : NodeLeaf;
        cur = getIndexNode(cur->nextNodeFpos(), type, ctx);
        assertex(cur);
        prev->Release();
    }

    //Now walk the leaf node siblings until there are no more.
    for (;;)
    {
        const CJHSearchNode * last = cur;
        cur = getIndexNode(cur->nextNodeFpos(), NodeLeaf, ctx);
        if (!cur)
            return last;
        ::Release(last);
    }
}

CKeyCursor::CKeyCursor(CKeyIndex &_key, const IIndexFilterList *_filter, bool _logExcessiveSeeks, size32_t _blockedIOSize)
    : key(OLINK(_key)), filter(_filter), logExcessiveSeeks(_logExcessiveSeeks)
{
    if (_blockedIOSize)
    {
        IFileIO *baseIO = const_cast<IFileIO *>(key.queryFileIO());  // I suspect createBlockedIO should take const...
        if (baseIO)
            myIO.setown(createBlockedIO(LINK(baseIO), _blockedIOSize));
    }
    nodeKey = 0;
    recordBuffer = (char *) malloc(key.keySize());  // MORE - would be nice to know real max - is it stored in metadata?
}

CKeyCursor::CKeyCursor(const CKeyCursor &from)
: key(OLINK(from.key)), filter(from.filter)
{
    nodeKey = from.nodeKey;
    node.set(from.node);
    for (unsigned i = 0; i < maxParentNodes; i++)
    {
        parents[i].set(from.parents[i]);
        parentNodeKeys[i] = from.parentNodeKeys[i];
    }
    unsigned keySize = key.keySize();
    recordBuffer = (char *) malloc(keySize);  // MORE - would be nice to know real max - is it stored in metadata?
    memcpy(recordBuffer, from.recordBuffer, key.keyedSize());  // Just make keyed portion valid initially
    fullBufferValid = false;
    eof = from.eof;
    matched = from.matched;
}


CKeyCursor::~CKeyCursor()
{
    key.Release();
    free(recordBuffer);
}

void CKeyCursor::reset()
{
    node.clear();
    matched = false;
    clearParentNodes();
    eof = key.bloomFilterReject(*filter) || !filter->canMatch();
    if (!eof)
        setLow(0);
}

bool CKeyCursor::next(IContextLogger *ctx)
{
    return _next(ctx) && node && node->getKeyAt(nodeKey, recordBuffer);
}

bool CKeyCursor::_next(IContextLogger *ctx)
{
    fullBufferValid = false;
    if (!node)
    {
        node.setown(key.locateFirstLeafNode(ctx));
        clearParentNodes();  // MORE - could do better, but may not be a lot of point?
        nodeKey = 0;
        return node && node->isKeyAt(nodeKey);
    }
    else
    {
        key.keyScans++;
        if (!node->isKeyAt(++nodeKey))
        {
            offset_t rsib = node->getRightSib();
            NodeType type = node->getNodeType();
            node.clear();
            if (rsib != 0)
            {
                node.setown(getCursorNode(rsib, type, ctx));
                if (node != NULL)
                {
                    nodeKey = 0;
                    // Update the nodekey indexes of any parent nodes that are still relevant, and clear any that are not
                    unsigned depth = key.getBranchDepth();
                    while (depth)
                    {
                        depth--;
                        if (depth < maxParentNodes && parents[depth])
                        {
                            unsigned numParentKeys = parents[depth]->getNumKeys();
                            unsigned parentNodeKey = parentNodeKeys[depth];
                            if (parentNodeKey < numParentKeys-1)
                            {
                                parentNodeKeys[depth] = parentNodeKey+1;
                                break;
                            }
                            parents[depth].clear();
                        }
                        else
                        {
                            // We can't tell which parent node info is correct. Clear them all.
                            clearParentNodes();
                            break;
                        }
                    }
                    return node->isKeyAt(0);
                }
            }
            clearParentNodes();
            return false;
        }
        else
            return true;
    }
}

const char *CKeyCursor::queryName() const
{
    return key.queryFileName();
}

size32_t CKeyCursor::getKeyedSize() const
{
    return key.keyedSize();
}

const byte *CKeyCursor::queryRecordBuffer() const
{
    assertex(node);
    if (!fullBufferValid)
    {
        node->fetchPayload(nodeKey, recordBuffer);
        fullBufferValid = true;
    }
    return (const byte *) recordBuffer;
}

const byte *CKeyCursor::queryKeyedBuffer() const
{
    return (const byte *) recordBuffer;
}

size32_t CKeyCursor::getSize()
{
    assertex(node);
    return node->getSizeAt(nodeKey);
}

offset_t CKeyCursor::getFPos() const
{
    assertex(node);
    return node->getFPosAt(nodeKey);
}

unsigned __int64 CKeyCursor::getSequence()
{
    assertex(node);
    return node->getSequence(nodeKey);
}

bool CKeyCursor::_last(IContextLogger *ctx)
{
    fullBufferValid = false;
    node.setown(key.locateLastLeafNode(ctx));
    clearParentNodes();   // Could in theory do better but unlikely to be any point
    if (node)
    {
        nodeKey = node->getNumKeys()-1;
        return true;
    }
    return false;
}

bool CKeyCursor::_gtEqual(IContextLogger *ctx)
{
    fullBufferValid = false;
    key.keySeeks++;
    unsigned lwm = 0;
    unsigned branchDepth = key.getBranchDepth();
    unsigned depth = branchDepth;
    if (node)
    {
        // When seeking forward, there are two cases worth optimizing:
        // 1. the next record is actually the one we want
        // 2. The record we want is on the current page
        unsigned numKeys = node->getNumKeys();
        if (nodeKey < numKeys-1)
        {   
            int rc = node->compareValueAt(recordBuffer, ++nodeKey);
            if (rc <= 0)
            {
                return true;
            }
            if (nodeKey < numKeys-1)
            {
                rc = node->compareValueAt(recordBuffer, numKeys-1);
                if (rc <= 0)
                {
                    lwm = nodeKey+1;
                }
            }
        }
    }
    if (!lwm)
    {
        while (depth)
        {
            depth--;
            if (depth < maxParentNodes)
            {
                const CJHSearchNode *startnode = parents[depth];
                if (startnode)
                {
                    unsigned numParentKeys = startnode->getNumKeys();
                    unsigned parentNodeKey = parentNodeKeys[depth];
                    if (parentNodeKey < numParentKeys-1)
                    {   
                        int rc = startnode->compareValueAt(recordBuffer, numParentKeys-1);
                        if (rc <= 0)
                        {
                            lwm = parentNodeKey;
                            node.set(startnode);
                            break;
                        }
                    }
                    parents[depth].clear();
                }
            }
        }
    }
    if (!lwm)
    {
        node.set(key.rootNode);
        depth = 0;
    }
    for (;;)
    {
        // first search for first GTE entry (result in b(<),a(>=))
        unsigned int a = node->locateGE(recordBuffer, lwm);
        if (node->isLeaf())
        {
            if (a<node->getNumKeys())
                nodeKey = a;
            else
            {
                offset_t nextPos = node->nextNodeFpos();  // This can happen at eof because of key peculiarity where level above reports ffff as last
                node.setown(getCursorNode(nextPos, NodeLeaf, ctx));
                nodeKey = 0;
            }
            if (node)
                return true; 
            else
                return false;
        }
        else
        {
            if (a<node->getNumKeys())
            {
                offset_t npos = node->getFPosAt(a);
                if (depth < maxParentNodes)
                {
                    parentNodeKeys[depth] = a;
                    parents[depth].swap(node);
                }
                lwm = 0;
                depth++;
                NodeType type = (depth < branchDepth) ? NodeBranch : NodeLeaf;
                node.setown(getCursorNode(npos, type, ctx));
            }
            else
                return false;
        }
    }
}

bool CKeyCursor::_ltEqual(IContextLogger *ctx)
{
    fullBufferValid = false;
    key.keySeeks++;
    matched = false;
    unsigned lwm = 0;
    unsigned branchDepth = key.getBranchDepth();
    unsigned depth = branchDepth;
    if (node)
    {
        // When seeking forward, there are two cases worth optimizing:
        // 1. next record is > src, so we return current
        // 2. The record we want is on the current page
        unsigned numKeys = node->getNumKeys();
        if (nodeKey < numKeys-1)
        {   
            int rc = node->compareValueAt(recordBuffer, ++nodeKey);
            if (rc < 0)
            {
                --nodeKey;
                return true; 
            }
            if (nodeKey < numKeys-1)
            {
                rc = node->compareValueAt(recordBuffer, numKeys-1);
                if (rc < 0)
                    lwm = nodeKey;
            }
        }
    }
    if (!lwm)
    {
        while (depth)
        {
            depth--;
            if (depth < maxParentNodes)
            {
                const CJHSearchNode *startnode = parents[depth];
                if (startnode)
                {
                    unsigned numParentKeys = startnode->getNumKeys();
                    unsigned parentNodeKey = parentNodeKeys[depth];
                    if (parentNodeKey < numParentKeys-1)
                    {   
                        int rc = startnode->compareValueAt(recordBuffer, numParentKeys-1);
                        if (rc < 0)
                        {
                            lwm = parentNodeKey;
                            node.set(startnode);
                            break;
                        }
                    }
                    parents[depth].clear();
                }
            }
        }
    }
    if (!lwm)
    {
        node.set(key.rootNode);
        depth = 0;
    }
    for (;;)
    {
        // Locate first record greater than src
        unsigned int a = node->locateGT(recordBuffer, lwm);
        if (node->isLeaf())
        {
            // record we want is the one before first record greater than src.
            if (a>0)
                nodeKey = a-1;
            else
            {
                offset_t prevPos = node->prevNodeFpos();
                node.setown(getCursorNode(prevPos, NodeLeaf, ctx));
                if (node)
                    nodeKey = node->getNumKeys()-1;
            }
            if (node)
            {
                return true; 
            }
            else
                return false;
        }
        else
        {
            // Node to look in is the first one one that ended greater than src.
            if (a==node->getNumKeys())
                a--;   // value being looked for is off the end of the index.
            offset_t npos = node->getFPosAt(a);
            if (depth < maxParentNodes)
            {
                parentNodeKeys[depth] = a;
                parents[depth].swap(node);
            }
            lwm = 0;
            depth++;
            NodeType type = (depth < branchDepth) ? NodeBranch : NodeLeaf;
            node.setown(getCursorNode(npos, type, ctx));
            if (!node)
                throw MakeStringException(0, "Invalid key %s: child node pointer should never be NULL", key.name.get());
        }
    }
}

void CKeyCursor::clearParentNodes()
{
    for (unsigned i = 0; i < maxParentNodes; i++)
    {
        parents[i].clear();
        parentNodeKeys[i] = 0;
    }
}

void CKeyCursor::serializeCursorPos(MemoryBuffer &mb)
{
    mb.append(eof);
    if (!eof)
    {
        mb.append(matched);
        if (node)
        {
            mb.append(node->getFpos());
            mb.append(nodeKey);
        }
        else
        {
            offset_t zero = 0;
            unsigned zero2 = 0;
            mb.append(zero);
            mb.append(zero2);
        }
    }
}

void CKeyCursor::deserializeCursorPos(MemoryBuffer &mb, IContextLogger *ctx)
{
    mb.read(eof);
    node.clear();
    clearParentNodes();   // we don't serialize parent node info
    if (!eof)
    {
        mb.read(matched);
        offset_t nodeAddress;
        mb.read(nodeAddress);
        mb.read(nodeKey);
        fullBufferValid = false;
        if (nodeAddress)
        {
            node.setown(getCursorNode(nodeAddress, NodeLeaf, ctx));
            if (node && recordBuffer)
                node->getKeyAt(nodeKey, recordBuffer);
        }
    }
}

const byte *CKeyCursor::loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx)
{
    return key.loadBlob(blobid, blobsize, ctx);
}

bool CKeyCursor::lookup(bool exact, IContextLogger *ctx)
{
    return _lookup(exact, filter->lastRealSeg(), filter->isUnfiltered(), ctx);
}

bool CKeyCursor::_lookup(bool exact, unsigned lastSeg, bool unfiltered, IContextLogger *ctx)
{
    if (unfiltered && !matched)
    {
        //Special case reading a file with no filter - fall into the next processing.
        matched = true;
    }
    bool ret = false;
    unsigned lwildseeks = 0;
    unsigned lseeks = 0;
    unsigned lscans = 0;
    while (!eof)
    {
        if (matched)
        {
            if (!_next(ctx))
                eof = true;
            lscans++;
        }
        else
        {
            if (!_gtEqual(ctx))
                eof = true;
            lseeks++;
        }
        if (!eof)
        {
            assertex(node);
            verifyex(node->getKeyAt(nodeKey, recordBuffer));
            unsigned i = 0;
            matched = filter->matchesBuffer(recordBuffer, lastSeg, i);
            if (matched)
            {
                ret = true;
                break;
            }
#ifdef  __linux__
            if (linuxYield)
                sched_yield();
#endif
            eof = !filter->incrementKey(i, recordBuffer);
            if (!exact)
            {
                ret = true;
                break;
            }
            if (eof)
                break;
            lwildseeks++;
        }
        else
            eof = true;
    }
    if (logExcessiveSeeks && lwildseeks > 1000 && ret)
        reportExcessiveSeeks(lwildseeks, lastSeg, ctx);
    noteSeeks(ctx, lseeks, lscans, lwildseeks);
    return ret;
}

bool CKeyCursor::lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen, IContextLogger *ctx)
{
    if (skipTo(seek, seekOffset, seeklen))
        noteSkips(ctx, 1, 0);
    else
        noteSkips(ctx, 0, 1);
    bool ret = lookup(true, ctx);
#ifdef _DEBUG
    if (doTrace(traceSmartStepping, TraceFlags::Max))
    {
        StringBuffer recstr;
        unsigned i;
        for (i = 0; i < key.keySize(); i++)
        {
            unsigned char c = ((unsigned char *) recordBuffer)[i];
            recstr.appendf("%c", isprint(c) ? c : '.');
        }
        recstr.append ("    ");
        for (i = 0; i < key.keySize(); i++)
        {
            recstr.appendf("%02x ", ((unsigned char *) recordBuffer)[i]);
        }
        if (ctx)
        {
            const CRuntimeStatisticCollection &statsCollection = ctx->queryStats();
            unsigned __int64 seeks = statsCollection.getStatisticValue(StNumIndexSeeks);
            unsigned __int64 scans = statsCollection.getStatisticValue(StNumIndexScans);
            unsigned __int64 skips = statsCollection.getStatisticValue(StNumIndexSkips);
            DBGLOG("SKIP: Got skips=%" I64F "u seeks=%" I64F "u scans=%" I64F "u : %s", skips, seeks, scans, recstr.str());
        }
        else
        {
            DBGLOG("SKIP: (no ContextLogger - not tracking stats) : %s", recstr.str());
        }
    }
#endif
    return ret;
}


unsigned __int64 CKeyCursor::getCount(IContextLogger *ctx)
{
    reset();
    unsigned __int64 result = 0;
    unsigned lastRealSeg = filter->lastRealSeg();
    bool unfiltered = filter->isUnfiltered();
    for (;;)
    {
        if (_lookup(true, lastRealSeg, unfiltered, ctx))
        {
            unsigned __int64 locount = getSequence();
            endRange(lastRealSeg);
            _ltEqual(ctx);
            result += getSequence()-locount+1;
            if (!incrementKey(lastRealSeg))
                break;
        }
        else
            break;
    }
    return result;
}

unsigned __int64 CKeyCursor::checkCount(unsigned __int64 max, IContextLogger *ctx)
{
    reset();
    unsigned __int64 result = 0;
    unsigned lastFullSeg = filter->lastFullSeg();
    bool unfiltered = filter->isUnfiltered();
    if (lastFullSeg == (unsigned) -1)
    {
        noteSeeks(ctx, 1, 0, 0);
        if (_last(ctx))
            return getSequence()+1;
        else
            return 0;
    }
    for (;;)
    {
        if (_lookup(true, lastFullSeg, unfiltered, ctx))
        {
            unsigned __int64 locount = getSequence();
            endRange(lastFullSeg);
            _ltEqual(ctx);
            result += getSequence()-locount+1;
            if (max && (result > max))
                break;
            if (!incrementKey(lastFullSeg))
                break;
        }
        else
            break;
    }
    return result;
}

unsigned __int64 CKeyCursor::getCurrentRangeCount(unsigned groupSegCount, IContextLogger *ctx)
{
    unsigned __int64 locount = getSequence();
    endRange(groupSegCount);
    _ltEqual(ctx);
    noteSeeks(ctx, 1, 0, 0);
    return getSequence()-locount+1;
}

bool CKeyCursor::nextRange(unsigned groupSegCount)
{
    matched = false;
    if (!incrementKey(groupSegCount-1))
        return false;
    return true;
}

void CKeyCursor::reportExcessiveSeeks(unsigned numSeeks, unsigned lastSeg, IContextLogger *ctx)
{
    StringBuffer recstr;
    unsigned i;
    bool printHex = false;
    for (i = 0; i < key.keySize(); i++)
    {
        unsigned char c = ((unsigned char *) recordBuffer)[i];
        if (isprint(c))
            recstr.append(c);
        else
        {
            recstr.append('.');
            printHex = true;
        }
    }
    if (printHex)
    {
        recstr.append ("\n");
        for (i = 0; i < key.keySize(); i++)
        {
            recstr.appendf("%02x ", ((unsigned char *) recordBuffer)[i]);
        }
    }
    recstr.append ("\nusing filter:\n");
    filter->describe(recstr);
    if (ctx)
        ctx->CTXLOG("%d seeks to lookup record \n%s\n in key %s", numSeeks, recstr.str(), key.queryFileName());
    else
        DBGLOG("%d seeks to lookup record \n%s\n in key %s", numSeeks, recstr.str(), key.queryFileName());
}

bool CKeyCursor::skipTo(const void *_seek, size32_t seekOffset, size32_t seeklen)
{
    // Modify the current key contents buffer as follows
    // Take bytes up to seekoffset from current buffer (i.e. leave them alone)
    // Take up to seeklen bytes from seek comparing them as I go. If I see a lower one before I see a higher one, stop.
    // If I didn't see any higher ones, return (at which point the skipto was a no-op
    // If I saw higher ones, call setLowAfter for all remaining segmonitors
    // If the current contents of buffer could not match, call incremementKey at the appropriate monitor so that it can
    // Clear the matched flag
    const byte *seek = (const byte *) _seek;
    while (seeklen)
    {
        int c = *seek - (byte) (recordBuffer[seekOffset]);
        if (c < 0)
            return false;
        else if (c>0)
        {
            memcpy(recordBuffer+seekOffset, seek, seeklen);
            break;
        }
        seek++;
        seekOffset++;
        seeklen--;
    }
    if (!seeklen) return false;

    unsigned j = setLowAfter(seekOffset + seeklen);
    bool canmatch = filter->matchesBuffer(recordBuffer, filter->lastRealSeg(), j);
    if (!canmatch)
        eof = !incrementKey(j);
    matched = false;
    return true;
}

IKeyCursor * CKeyCursor::fixSortSegs(unsigned sortFieldOffset)
{
    return new CPartialKeyCursor(*this, sortFieldOffset);
}

CPartialKeyCursor::CPartialKeyCursor(const CKeyCursor &from, unsigned sortFieldOffset)
: CKeyCursor(from)
{
    filter = filter->fixSortSegs( recordBuffer, sortFieldOffset);
}

CPartialKeyCursor::~CPartialKeyCursor()
{
    ::Release(filter);
}

//-------------------------------------------------------

IndexRowFilter::IndexRowFilter(const RtlRecord &_recInfo) : recInfo(_recInfo)
{
    keySegCount = recInfo.getNumKeyedFields();
    lastReal = 0;
    lastFull = -1;
    keyedSize = 0;
}

IndexRowFilter::IndexRowFilter(const IndexRowFilter &from, const char *fixedVals, unsigned sortFieldOffset)
: recInfo(from.recInfo), keySegCount(from.keySegCount)
{
    lastReal = 0;
    lastFull = -1;
    keyedSize = 0;
    ForEachItemIn(idx, from.filters)
    {
        auto &filter = from.filters.item(idx);
        unsigned field = filter.queryFieldIndex();
        unsigned offset = recInfo.getFixedOffset(field);
        if (offset < sortFieldOffset)
            append(FFkeyed, createFieldFilter(field, *recInfo.queryType(field), fixedVals+offset));
        else
            append(FFkeyed, LINK(&filter));  // MORE - FFopt vs FFkeyed is dodgy
    }
}


void IndexRowFilter::append(IKeySegmentMonitor *segment)
{
    throwUnexpected();
}

const IIndexFilter *IndexRowFilter::item(unsigned idx) const
{
    return &queryFilter(idx);
}

void IndexRowFilter::append(FFoption option, const IFieldFilter * filter)
{
    assertex(filter->queryType().isFixedSize());
    unsigned idx = filter->queryFieldIndex();
    while (idx > numFilterFields())
    {
        append(FFkeyed, createWildFieldFilter(numFilterFields(), *recInfo.queryType(numFilterFields())));
    }
    assertex(idx == numFilterFields());
    if (!filter->isWild())
    {
        lastReal = idx;
        if (option != FFopt || lastFull == idx-1)
            lastFull = idx;
        unfiltered = false;
    }
    keyedSize += filter->queryType().getMinSize();
    addFilter(*filter);
}

void IndexRowFilter::setLow(unsigned field, void *keyBuffer) const
{
    unsigned lim = numFilterFields();
    while (field < lim)
    {
        unsigned offset = recInfo.getFixedOffset(field);
        const IFieldFilter &filter = queryFilter(field);
        filter.setLow(keyBuffer, offset);
        field++;
    }
}

unsigned IndexRowFilter::setLowAfter(size32_t offset, void *keyBuffer) const
{
    unsigned lim = filters.length();
    unsigned field = 0;
    unsigned skipped = 0;
    unsigned fieldOffset = recInfo.getFixedOffset(field);
    while (field < lim)
    {
        unsigned nextOffset = recInfo.getFixedOffset(field+1);
        if (fieldOffset >= offset)
            filters.item(field).setLow(keyBuffer, fieldOffset);
        else if (nextOffset <= offset)
            skipped++;
        else
        {
            byte *temp = (byte *) alloca(nextOffset - fieldOffset);
            filters.item(field).setLow(temp, 0);
            memcpy((byte *)keyBuffer+offset, temp, nextOffset - offset);
        }
        field++;
        fieldOffset = nextOffset;
    }
    return skipped;
}

bool IndexRowFilter::incrementKey(unsigned segno, void *keyBuffer) const
{
    // Increment the key buffer to next acceptable value
    if (segno == (unsigned)-1)
        return false;

    for(;;)
    {
        if (queryFilter(segno).incrementKey(keyBuffer, recInfo.getFixedOffset(segno)))
        {
            setLow(segno+1, keyBuffer);
            return true;
        }
        if (!segno)
            return false;
        segno--;
    }
}

void IndexRowFilter::endRange(unsigned field, void *keyBuffer) const
{
    unsigned lim = numFilterFields();
    if (field < lim)
    {
        queryFilter(field).endRange(keyBuffer, recInfo.getFixedOffset(field));
        field++;
    }
    while (field < lim)
    {
        queryFilter(field).setHigh(keyBuffer, recInfo.getFixedOffset(field));
        field++;
    }
}

bool IndexRowFilter::isUnfiltered() const
{
    return unfiltered;
}

unsigned IndexRowFilter::lastRealSeg() const
{
    return lastReal;
}

unsigned IndexRowFilter::lastFullSeg() const
{
    return lastFull;
}

unsigned IndexRowFilter::numFilterFields() const
{
    return RowFilter::numFilterFields();
}

IIndexFilterList *IndexRowFilter::fixSortSegs(const char *fixedVals, unsigned sortFieldOffset) const
{
    return new IndexRowFilter(*this, fixedVals, sortFieldOffset);
}

void IndexRowFilter::reset()
{
    RowFilter::clear();
    lastReal = 0;
    lastFull = -1;
    keyedSize = 0;
    unfiltered = true;
}

void IndexRowFilter::checkSize(size32_t _keyedSize, char const * keyname) const
{
    if (_keyedSize != keyedSize)
    {
        StringBuffer err;
        err.appendf("Key size mismatch on key %s - key size is %u, expected %u", keyname, _keyedSize, keyedSize);
    }
}

void IndexRowFilter::recalculateCache()
{
    // Nothing to do. This probably should be moved to be local to SegMonitorList
}

void IndexRowFilter::finish(size32_t _keyedSize)
{
    while (numFilterFields() < keySegCount)
    {
        unsigned idx = numFilterFields();
        append(FFkeyed, createWildFieldFilter(idx, *recInfo.queryType(idx)));
    }
    assertex(numFilterFields() == keySegCount);
}

void IndexRowFilter::describe(StringBuffer &out) const
{
    for (unsigned idx=0; idx <= lastRealSeg() && idx < numFilterFields(); idx++)
    {
        auto &filter = queryFilter(idx);
        if (idx)
            out.append(',');
        out.appendf("%s=", recInfo.queryName(idx));
        filter.describe(out);
    }
}

bool IndexRowFilter::matchesBuffer(const void *buffer, unsigned lastSeg, unsigned &matchSeg) const
{
    if (numFilterFields())
    {
        unsigned maxSeg = lastSeg+1; // avoid unlikely problems with -1
        RtlFixedRow rowInfo(recInfo, buffer, numFilterFields());
        for (; matchSeg < maxSeg; matchSeg++)
        {
            if (!queryFilter(matchSeg).matches(rowInfo))
                return false;
        }
    }
    return true;
}

bool IndexRowFilter::canMatch() const
{
    ForEachItemIn(idx, filters)
    {
        if (filters.item(idx).isEmpty())
            return false;
    }
    return true;
}

//-------------------------------------------------------

class CLazyKeyIndex : implements IKeyIndex, public CInterface
{
    StringAttr keyfile;
    unsigned crc; 
    unsigned fileIdx;
    Linked<IDelayedFile> delayedFile;
    mutable Owned<IFileIO> iFileIO;
    mutable Owned<IKeyIndex> realKey;
    mutable CriticalSection c;
    bool isTLK;
    size32_t blockedIOSize = 0;

    inline IKeyIndex &checkOpen() const
    {
        CriticalBlock b(c);
        if (!realKey)
        {
            Owned<IMemoryMappedFile> mapped = useMemoryMappedIndexes ? delayedFile->getMappedFile() : nullptr;
            if (mapped)
                realKey.setown(queryKeyStore()->load(keyfile, crc, mapped, isTLK));
            else
            {
                iFileIO.setown(delayedFile->getFileIO());
                realKey.setown(queryKeyStore()->load(keyfile, crc, iFileIO, fileIdx, isTLK, blockedIOSize));
            }
            if (!realKey)
            {
                DBGLOG("Lazy key file %s could not be opened", keyfile.get());
                throw MakeStringException(0, "Lazy key file %s could not be opened", keyfile.get());
            }
        }
        return *realKey;
    }

public:
    IMPLEMENT_IINTERFACE;
    CLazyKeyIndex(const char *_keyfile, unsigned _crc, IDelayedFile *_delayedFile, unsigned _fileIdx, bool _isTLK, size32_t _blockedIOSize)
        : keyfile(_keyfile), crc(_crc), fileIdx(_fileIdx), delayedFile(_delayedFile), isTLK(_isTLK), blockedIOSize(_blockedIOSize)
    {}

    virtual bool IsShared() const { return CInterface::IsShared(); }

    virtual IKeyCursor *getCursor(const IIndexFilterList *filter, bool logExcessiveSeeks) override { return checkOpen().getCursor(filter, logExcessiveSeeks); }
    virtual size32_t keySize() { return checkOpen().keySize(); }
    virtual size32_t keyedSize() { return checkOpen().keyedSize(); }
    virtual bool hasPayload() { return checkOpen().hasPayload(); }
    virtual bool isTopLevelKey() const override { return checkOpen().isTopLevelKey(); }
    virtual bool isFullySorted() override { return checkOpen().isFullySorted(); }
    virtual __uint64 getPartitionFieldMask() { return checkOpen().getPartitionFieldMask(); }
    virtual unsigned numPartitions() { return checkOpen().numPartitions(); }
    virtual unsigned getFlags() { return checkOpen().getFlags(); }
    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw) { checkOpen().dumpNode(out, pos, count, isRaw); }
    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return realKey ? realKey->queryScans() : 0; }
    virtual unsigned querySeeks() { return realKey ? realKey->querySeeks() : 0; }
    virtual const char *queryFileName() const { return keyfile.get(); }
    virtual unsigned queryId() const override { return NotFound; }
    virtual offset_t queryBlobHead() { return checkOpen().queryBlobHead(); }
    virtual void resetCounts() { if (realKey) realKey->resetCounts(); }
    virtual offset_t queryLatestGetNodeOffset() const { return realKey ? realKey->queryLatestGetNodeOffset() : 0; }
    virtual offset_t queryMetadataHead() { return checkOpen().queryMetadataHead(); }
    virtual IPropertyTree * getMetadata() { return checkOpen().getMetadata(); }
    virtual unsigned getNodeSize() { return checkOpen().getNodeSize(); }
    virtual const IFileIO *queryFileIO() const override { return iFileIO; } // NB: if not yet opened, will be null
    virtual bool hasSpecialFileposition() const { return checkOpen().hasSpecialFileposition(); }
    virtual bool needsRowBuffer() const { return checkOpen().needsRowBuffer(); }
    virtual bool prewarmPage(offset_t offset, NodeType type) { return checkOpen().prewarmPage(offset, type); }
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const override
    {
        {
            CriticalBlock b(c);
            if (!realKey) return;
        }
        realKey->mergeStats(stats);
    }
    virtual offset_t queryFirstBranchOffset() override { return checkOpen().queryFirstBranchOffset(); }
};

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, IFileIO &iFileIO, unsigned fileIdx, bool isTLK, size32_t blockedIOSize)
{
    return queryKeyStore()->load(keyfile, crc, &iFileIO, fileIdx, isTLK, blockedIOSize);
}

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, bool isTLK, size32_t blockedIOSize)
{
    return queryKeyStore()->load(keyfile, crc, isTLK, blockedIOSize);
}

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, IDelayedFile &iFileIO, unsigned fileIdx, bool isTLK, size32_t blockedIOSize)
{
    return new CLazyKeyIndex(keyfile, crc, &iFileIO, fileIdx, isTLK, blockedIOSize);
}

extern jhtree_decl void clearKeyStoreCache(bool killAll)
{
    queryKeyStore()->clearCache(killAll);
}

extern jhtree_decl void clearKeyStoreCacheEntry(const char *name)
{
    queryKeyStore()->clearCacheEntry(name);
}

extern jhtree_decl void clearKeyStoreCacheEntry(const IFileIO *io)
{
    queryKeyStore()->clearCacheEntry(io);
}

extern jhtree_decl StringBuffer &getIndexMetrics(StringBuffer &ret)
{
    return queryKeyStore()->getMetrics(ret);
}


extern jhtree_decl void resetIndexMetrics()
{
    queryKeyStore()->resetMetrics();
}

void recordEventIndexInformation()
{
    queryKeyStore()->recordEventIndexInformation();
}

extern jhtree_decl size_t setNodeCacheMem(size_t cacheSize)
{
    return queryNodeCache()->setNodeCacheMem(cacheSize);
}

extern jhtree_decl size_t setLeafCacheMem(size_t cacheSize)
{
    return queryNodeCache()->setLeafCacheMem(cacheSize);
}

extern jhtree_decl size_t setBlobCacheMem(size_t cacheSize)
{
    return queryNodeCache()->setBlobCacheMem(cacheSize);
}

void setNodeFetchThresholdNs(__uint64 thresholdNs)
{
    fetchThresholdCycles = nanosec_to_cycle(thresholdNs);
}

void setIndexWarningThresholds(IPropertyTree * options)
{
    if (options->hasProp("@traceCacheLockingFrequencyNs"))
        traceCacheLockingFrequency = nanosec_to_cycle(options->getPropInt64("@traceCacheLockingFrequencyNs"));
    if (options->hasProp("@traceNodeLoadFrequencyNs"))
        traceNodeLoadFrequency = nanosec_to_cycle(options->getPropInt64("@traceNodeLoadFrequencyNs"));
    if (options->hasProp("@traceNodeLoadThresholdNs"))
        traceNodeLoadThreshold = nanosec_to_cycle(options->getPropInt64("@traceNodeLoadThresholdNs"));
}

extern jhtree_decl void getNodeCacheInfo(ICacheInfoRecorder &cacheInfo)
{
    // MORE - consider reporting root nodes of open IKeyIndexes too?
    queryNodeCache()->getCacheInfo(cacheInfo);
}

///////////////////////////////////////////////////////////////////////////////
// CNodeCache impl.
///////////////////////////////////////////////////////////////////////////////

void CNodeCache::getCacheInfo(ICacheInfoRecorder &cacheInfo)
{
    for (unsigned i = 0; i < CacheMax; i++)
        cache[i].reportEntries(cacheInfo);
}

//Use a critical section in each node to prevent multiple threads loading the same node at the same time.
//Critical sections are 40bytes on linux so < 0.5% overhead for an 8K page and trivial overhead when constructed (<10ns)
static std::atomic<cycle_t> lastLockingReportCycles{0};
static std::atomic<cycle_t> lastLoadReportCycles{0};

//Keep track of the total time that is taken to resolve a node from the cache - including loading it from disk.
class NodeCacheLookupTimer
{
public:
    inline NodeCacheLookupTimer(IContextLogger * _ctx) : ctx(_ctx)
    {
        if (ctx)
            startCycles = get_cycles_now();
    }
    inline ~NodeCacheLookupTimer()
    {
        try
        {
            if (ctx)
                ctx->noteStatistic(StCycleIndexCacheBlockedCycles, get_cycles_now() - startCycles);
        }
        catch (IException * e)
        {
            //If a query is being aborted, noteStatistic can throw an abort exception.  Throw it away instead.
            ::Release(e);
        }
    }

    //Call this function to avoid the destructor having to call get_cycles_now()
    void noteComplete(cycle_t now)
    {
        if (ctx)
        {
            ctx->noteStatistic(StCycleIndexCacheBlockedCycles, now - startCycles);
            // prevent double recording the time - compiler will likely optimize away the destructor
            ctx = nullptr;
        }
    }

private:
    IContextLogger * ctx;
    cycle_t startCycles = 0;
};

const CJHTreeNode *CNodeCache::getCachedNode(const INodeLoader *keyIndex, unsigned iD, offset_t pos, NodeType type, IContextLogger *ctx, bool isTLK)
{
    // MORE - could probably be improved - I think having the cache template separate is not helping us here
    // Also one cache per key would surely be faster, and could still use a global total
    if (!pos)
        return NULL;

    //Time how long it takes to resolve an item in the node cache, and record it as the blocked time.
    NodeCacheLookupTimer lookupTimer(ctx);

    // No benefit in caching the following, especially since they will evict useful pages
    if ((type == NodeMeta) || (type == NodeBloom))
        return keyIndex->loadNode(nullptr, pos, nullptr);

    //NOTE: TLK leaf nodes are currently cached along with branches, not with leaves.  It might be better if this was a separate cache.
    CacheType cacheType = isTLK ? CacheBranch : (CacheType)type;

    // check cacheEnabled[cacheType] avoid the critical section (and testing the flag within the critical section)
    CNodeMRUCache & typeCache = cache[cacheType];
    if (unlikely(!typeCache.enabled))
        return keyIndex->loadNode(nullptr, pos, nullptr);

    CKeyIdAndPos key(iD, pos);
    unsigned hashcode = typeCache.getKeyHash(key);
    unsigned subCache = cacheBits == 0 ? 0 : hashcode >> cacheShift;
    CNodeMRUSubCache & curCache = typeCache.cache[subCache];

    //Previously, this was implemented as:
    //  Lock, unlock.  Load the page.  Lock, check if it has been added, otherwise add.
    //Now, it is coded as:
    //  Lock, add if missing, unlock.  Lock a page-dependent-cr load() release lock.
    //There will be the same number of critical section locks, but loading a page will contend on a different lock - so it should reduce contention.
    CriticalSection & cacheLock = curCache.lock;
    Owned<CNodeCacheEntry> ownedCacheEntry; // ensure node gets cleaned up if it fails to load
    bool alreadyExists = true;
    {
        DelayedCacheEntryReleaser delayedReleaser;
        CNodeCacheEntry * cacheEntry;

        CLeavableCriticalBlock block(cacheLock);
        cacheEntry = curCache.query(hashcode, &key);
        if (likely(cacheEntry))
        {
            curCache.numHits.fastAdd(1);
            const CJHTreeNode * fastPathMatch = cacheEntry->queryNode();
            if (likely(fastPathMatch))
            {
                //Avoid linking and releasing cacheEntry if the match is already loaded.  Link the node then leave
                //the critical section asap
                fastPathMatch->Link();
                block.leave();

                //Update ctx stats outside of the critical section.
                if (unlikely(recordingEvents()))
                    queryRecorder().recordIndexLookup(iD, pos, type, true);
                if (ctx) ctx->noteStatistic(hitStatId[cacheType], 1);
                return fastPathMatch;
            }
        }
        else
        {
            cacheEntry = new CNodeCacheEntry;
            curCache.replace(key, *cacheEntry, &delayedReleaser);
            alreadyExists = false;
            curCache.numAdds.fastAdd(1);
        }

        //same as ownedcacheEntry.set(cacheEntry), but avoids a null check or two
        cacheEntry->Link();
        ownedCacheEntry.setown(cacheEntry);
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordIndexLookup(iD, pos, type, false);

    //If an exception is thrown before the node is cleanly loaded we need to remove the partially constructed
    //node from the cache otherwise it may never get loaded, and can prevent items being removed from the cache
    //note: noteStatistic() can throw an exception if a worker has been aborted...
    try
    {
        //Move the atomic increments out of the critical section - they can be relatively expensive
        if (likely(ctx))
        {
            //It is most likely that an item has been added - because if the entry already exists
            //it will only reach this point if another thread has not already loaded the node.
            if (unlikely(alreadyExists))
                ctx->noteStatistic(hitStatId[cacheType], 1);
            else
                ctx->noteStatistic(addStatId[cacheType], 1);
        }

        cycle_t startCycles = get_cycles_now();
        lookupTimer.noteComplete(startCycles);

        //The common case is that this flag has already been set (by a previous add).
        if (likely(ownedCacheEntry->isReady()))
            return ownedCacheEntry->getNode();

        cycle_t fetchCycles = 0;
        cycle_t startLoadCycles;

        //Protect loading the node contents with a critical section - so that the node will only be loaded by one thread.
        //MORE: If this was called by high and low priority threads then there is an outside possibility that it could take a
        //long time for the low priority thread to progress.  That might cause the cache to be temporarily unbounded.  Unlikely in practice.
        {
            CriticalBlock loadBlock(ownedCacheEntry->cs);
            startLoadCycles = get_cycles_now();
            if (!ownedCacheEntry->isReady())
            {
                const CJHTreeNode *node = keyIndex->loadNode(&fetchCycles, pos, nullptr);
                if (unlikely(type != node->getNodeType()))
                {
                    //This should never happen, but if it does, report as much information as possible to diagnose the issue.
                    StringBuffer msg;
                    msg.appendf("Node type mismatch for node %s@%llx (expected %s, got %s)", keyIndex->queryFileName(), pos, cacheTypeText[type], cacheTypeText[node->getNodeType()]);
                    node->Release();
                    throwUnexpectedX(msg);
                }

                //Update the associated size of the entry in the hash table before setting isReady (never evicted until isReady is set)
                curCache.noteReady(*node);
                ownedCacheEntry->noteReady(node);
            }
            else
            {
                curCache.numDups++;
            }
        }
        cycle_t endLoadCycles = get_cycles_now();
        cycle_t actualLoadCycles = endLoadCycles - startLoadCycles;

        if (unlikely(recordingEvents()))
            queryRecorder().recordIndexLoad(iD, pos, type, ownedCacheEntry->queryNode()->getMemSize(), cycle_to_nanosec(actualLoadCycles), cycle_to_nanosec(fetchCycles));

        if (actualLoadCycles > traceNodeLoadThreshold)
        {
            if ((endLoadCycles - lastLoadReportCycles) >= traceNodeLoadFrequency)
            {
                lastLoadReportCycles = endLoadCycles;
                WARNLOG("CNodeCache::getNode load(%s %x:%llu) took %lluus fetch(%lluus) size(%u)", cacheTypeText[cacheType], iD, pos,
                        cycle_to_microsec(actualLoadCycles), cycle_to_microsec(fetchCycles), ownedCacheEntry->queryNode()->getMemSize());
            }
        }

        if (ctx)
        {
            ctx->noteStatistic(loadStatId[cacheType], endLoadCycles - startCycles);
            ctx->noteStatistic(readStatId[cacheType], fetchCycles);
            if (fetchCycles >= fetchThresholdCycles)
            {
                ctx->noteStatistic(fetchStatId[cacheType], 1);
                ctx->noteStatistic(fetchTimeId[cacheType], fetchCycles);
            }
        }
        return ownedCacheEntry->getNode();
    }
    catch (...)
    {
        //Ensure any partially constructed nodes are removed from the cache
        if (!ownedCacheEntry->isReady())
        {
            CriticalBlock block(cacheLock);
            if (!ownedCacheEntry->isReady())
                curCache.remove(key);
        }
        throw;
    }
}

RelaxedAtomic<unsigned> nodesLoaded;

//------------------------------------------------------------------------------------------------

class CKeyMerger : public CKeyLevelManager
{
    unsigned *mergeheap;
    unsigned numkeys;
    unsigned activekeys;
    IArrayOf<IKeyCursor> cursorArray;
    UnsignedArray mergeHeapArray;
    UnsignedArray keyNoArray;

    IKeyCursor **cursors;
    unsigned sortFieldOffset;
    unsigned sortFromSeg;

    bool resetPending;

    inline int BuffCompare(unsigned a, unsigned b)
    {
        const byte *c1 = cursors[mergeheap[a]]->queryKeyedBuffer();
        const byte *c2 = cursors[mergeheap[b]]->queryKeyedBuffer();

        //Only compare the keyed portion, and if equal tie-break on lower input numbers having priority
        //In the future this should use the comparison functions from the type info
        int ret = memcmp(c1+sortFieldOffset, c2+sortFieldOffset, keyedSize-sortFieldOffset);
        if (!ret)
        {
            if (sortFieldOffset)
                ret = memcmp(c1, c2, sortFieldOffset);
            //If they are equal, earlier inputs have priority
            if (!ret)
                ret = a - b;
        }
        return ret;
    }

    Linked<IKeyIndexBase> keyset;

    void calculateSortSeg()
    {
        // Make sure that sortFromSeg is properly set
        sortFromSeg = (unsigned) -1;
        unsigned numFilters = filter->numFilterFields();
        for (unsigned idx = 0; idx < numFilters; idx++)
        {
            unsigned offset = filter->getFieldOffset(idx);
            if (offset == sortFieldOffset)
            {
                sortFromSeg = idx;
                break;
            }
        }
        if (sortFromSeg == (unsigned)-1)
            assertex(!"Attempting to sort from offset that is not on a segment boundary");
        assertex(resetPending == true);
    }

public:
    CKeyMerger(const RtlRecord &_recInfo, IKeyIndexSet *_keyset, unsigned _sortFieldOffset, IContextLogger *_ctx, bool _newFilters, bool _logExcessiveSeeks)
    : CKeyLevelManager(_recInfo, NULL, _ctx, _newFilters, _logExcessiveSeeks), sortFieldOffset(_sortFieldOffset)
    {
        init();
        setKey(_keyset);
    }

    CKeyMerger(const RtlRecord &_recInfo, IKeyIndex *_onekey, unsigned _sortFieldOffset, IContextLogger *_ctx, bool _newFilters, bool _logExcessiveSeeks)
    : CKeyLevelManager(_recInfo, NULL, _ctx, _newFilters, _logExcessiveSeeks), sortFieldOffset(_sortFieldOffset)
    {
        init();
        setKey(_onekey);
    }

    ~CKeyMerger()
    {
        killBuffers();
    }

    void killBuffers()
    {
        cursorArray.kill();
        keyCursor = NULL; // cursorArray owns cursors
        mergeHeapArray.kill();
        keyNoArray.kill();

        cursors = NULL;
        mergeheap = NULL;
    }

    void init()
    {
        numkeys = 0;
        activekeys = 0;
        resetPending = true;
        sortFromSeg = 0;
    }

    virtual unsigned numActiveKeys() const override
    {
        return activekeys;
    }

    virtual unsigned getPartition() override
    {
        return 0;   // If all keys share partition info (is that required?) then we can do better
    }

    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        // Rather like a lookup, except that no records below the value indicated by seek* should be returned.
        if (resetPending)
        {
            resetSort(seek, seekOffset, seeklen);
            if (!activekeys)
                return false;
#ifdef _DEBUG
            if (doTrace(traceSmartStepping, TraceFlags::Max))
                DBGLOG("SKIP: init key = %d", mergeheap[0]);
#endif
            return true;
        }
        else
        {
            if (!activekeys)
            {
#ifdef _DEBUG
                if (doTrace(traceSmartStepping, TraceFlags::Max))
                    DBGLOG("SKIP: merge done");
#endif
                return false;
            }
            unsigned key = mergeheap[0];
#ifdef _DEBUG
            if (doTrace(traceSmartStepping, TraceFlags::Max))
                DBGLOG("SKIP: merging key = %d", key);
#endif
            unsigned compares = 0;
            for (;;)
            {
                if (!CKeyLevelManager::lookupSkip(seek, seekOffset, seeklen) )
                {
                    activekeys--;
                    if (!activekeys)
                    {
                        if (ctx)
                            ctx->noteStatistic(StNumIndexMergeCompares, compares);
                        return false;
                    }
                    mergeheap[0] = mergeheap[activekeys];
                }
                /* The key associated with mergeheap[0] will have changed
                   This code restores the heap property
                */
                unsigned p = 0; /* parent */
                while (1) 
                {
                    unsigned c = p*2 + 1; /* child */
                    if ( c >= activekeys ) 
                        break;
                    /* Select smaller child */
                    if ( c+1 < activekeys && BuffCompare( c+1, c ) < 0 ) c += 1;
                    /* If child is greater or equal than parent then we are done */
                    if ( BuffCompare( c, p ) >= 0 ) 
                        break;
                    /* Swap parent and child */
                    int r = mergeheap[c];
                    mergeheap[c] = mergeheap[p];
                    mergeheap[p] = r;
                    /* child becomes parent */
                    p = c;
                }
                if (key != mergeheap[0])
                {
                    key = mergeheap[0];
                    keyCursor = cursors[key];
                }
                const byte *keyBuffer = keyCursor->queryKeyedBuffer();
                if (memcmp(seek, keyBuffer+seekOffset, seeklen) <= 0)
                {
#ifdef _DEBUG
                    if (doTrace(traceSmartStepping, TraceFlags::Max))
                    {
                        unsigned keySize = keyCursor->getKeyedSize();
                        DBGLOG("SKIP: merged key = %d", key);
                        StringBuffer recstr;
                        unsigned i;
                        for (i = 0; i < keySize; i++)
                        {
                            unsigned char c = ((unsigned char *) keyBuffer)[i];
                            recstr.appendf("%c", isprint(c) ? c : '.');
                        }
                        recstr.append ("    ");
                        for (i = 0; i < keySize; i++)
                        {
                            recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
                        }
                        if (ctx)
                        {
                            const CRuntimeStatisticCollection &statsCollection = ctx->queryStats();
                            unsigned __int64 seeks = statsCollection.getStatisticValue(StNumIndexSeeks);
                            unsigned __int64 scans = statsCollection.getStatisticValue(StNumIndexScans);
                            unsigned __int64 skips = statsCollection.getStatisticValue(StNumIndexSkips);
                            DBGLOG("SKIP: Out skips=%" I64F "u seeks=%" I64F "u scans=%" I64F "u : %s", skips, seeks, scans, recstr.str());
                        }
                        else
                        {
                            DBGLOG("SKIP: (no ContextLogger - not tracking stats) : %s", recstr.str());
                        }
                    }
#endif
                    if (ctx)
                        ctx->noteStatistic(StNumIndexMergeCompares, compares);
                    return true;
                }
                else
                {
                    compares++;
                    if (ctx && (compares == 100))
                    {
                        ctx->noteStatistic(StNumIndexMergeCompares, compares); // also checks for abort...
                        compares = 0;
                    }
                }
            }
        }
    }

    virtual void setLayoutTranslator(const IDynamicTransform * trans) override
    { 
        if (trans && trans->keyedTranslated())
            throw MakeStringException(0, "Layout translation not supported when merging key parts, as it may change sort order"); 

        // It MIGHT be possible to support translation still if all keyCursors have the same translation
        // would have to translate AFTER the merge, but that's ok
        // HOWEVER the result won't be guaranteed to be in sorted order afterwards so is there any point?
        CKeyLevelManager::setLayoutTranslator(trans);
    }

    virtual void setKey(IKeyIndexBase *_keyset)
    {
        keyset.set(_keyset);
        if (_keyset && _keyset->numParts())
        {
            IKeyIndex *ki = _keyset->queryPart(0);
            keyedSize = ki->keyedSize();
            numkeys = _keyset->numParts();
            if (sortFieldOffset > keyedSize)
                throw MakeStringException(0, "Index sort order can only include keyed fields");
        }
        else
            numkeys = 0;
        killBuffers();
    }

    void resetSort(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        activekeys = 0;
        filter->recalculateCache();
        unsigned i;
        for (i = 0; i < numkeys; i++)
        {
            Owned<IKeyCursor> cursor = keyset->queryPart(i)->getCursor(filter, logExcessiveSeeks);
            cursor->reset();
            for (;;)
            {
                bool found;
                unsigned lskips = 0;
                unsigned lnullSkips = 0;
                for (;;)
                {
                    if (seek)
                    {
                        if (cursor->skipTo(seek, seekOffset, seeklen))
                            lskips++;
                        else
                            lnullSkips++;
                    }
                    found = cursor->lookup(true, ctx);
                    if (!found || !seek || memcmp(cursor->queryKeyedBuffer() + seekOffset, seek, seeklen) >= 0)
                        break;
                }
                noteSkips(ctx, lskips, lnullSkips);
                if (found)
                {
                    IKeyCursor *mergeCursor;
                    if (sortFromSeg)
                        mergeCursor = cursor->fixSortSegs(sortFieldOffset);
                    else
                        mergeCursor = LINK(cursor);

                    keyNoArray.append(i);
                    cursorArray.append(*mergeCursor);
                    mergeHeapArray.append(activekeys++);
                    if (!sortFromSeg || !cursor->nextRange(sortFromSeg))
                        break;
                }
                else
                {
                    break;
                }
            }
        }
        if (activekeys>0) 
        {
            if (ctx)
                ctx->noteStatistic(StNumIndexMerges, activekeys);
            cursors = cursorArray.getArray();
            mergeheap = mergeHeapArray.getArray();
            /* Permute mergeheap to establish the heap property
               For each element p, the children are p*2+1 and p*2+2 (provided these are in range)
               The children of p must both be greater than or equal to p
               The parent of a child c is given by p = (c-1)/2
            */
            for (i=1; i<activekeys; i++)
            {
                int r = mergeheap[i];
                int c = i; /* child */
                while (c > 0) 
                {
                    int p = (c-1)/2; /* parent */
                    if ( BuffCompare( c, p ) >= 0 ) 
                        break;
                    mergeheap[c] = mergeheap[p];
                    mergeheap[p] = r;
                    c = p;
                }
            }
            keyCursor = cursors[mergeheap[0]];
        }
        else
        {
            keyCursor = NULL;
        }
        resetPending = false;
    }

    virtual void reset(bool crappyHack)
    {
        if (!started)
        {
            started = true;
            filter->checkSize(keyedSize, "[merger]"); //PG: not sure what keyname to use here
        }
        if (!crappyHack)
        {
            killBuffers();
            resetPending = true;
        }
        else
        {
            if (sortFieldOffset)
            {
                ForEachItemIn(idx, cursorArray)
                {
                    cursorArray.replace(*cursorArray.item(idx).fixSortSegs(sortFieldOffset), idx);
                }
            }
            keyCursor = cursors[mergeheap[0]];
            resetPending = false;
        }
    }

    virtual bool lookup(bool exact)
    {
        assertex(exact);
        if (resetPending)
        {
            resetSort(NULL, 0, 0);
            if (!activekeys)
                return false;
        }
        else
        {
            if (!activekeys)
                return false;
            unsigned key = mergeheap[0];
            if (!keyCursor->lookup(exact, ctx))
            {
                activekeys--;
                if (!activekeys)
                    return false; // MORE - does this lose a record?
                mergeheap[0] = mergeheap[activekeys];
            }

            /* The key associated with mergeheap[0] will have changed
               This code restores the heap property
            */
            unsigned p = 0; /* parent */
            while (1) 
            {
                unsigned c = p*2 + 1; /* child */
                if ( c >= activekeys ) 
                    break;
                /* Select smaller child */
                if ( c+1 < activekeys && BuffCompare( c+1, c ) < 0 ) c += 1;
                /* If child is greater or equal than parent then we are done */
                if ( BuffCompare( c, p ) >= 0 ) 
                    break;
                /* Swap parent and child */
                int r = mergeheap[c];
                mergeheap[c] = mergeheap[p];
                mergeheap[p] = r;
                /* child becomes parent */
                p = c;
            }
//          dumpMergeHeap();
            if (mergeheap[0] != key)
                keyCursor = cursors[mergeheap[0]];
        }
        return true;
    }

    virtual unsigned __int64 getCount()
    {
        assertex (!sortFieldOffset);  // we should have avoided using a stepping merger for precheck of limits, both for efficiency and because this code won't work
                                      // as the sequence numbers are not in sequence
        unsigned __int64 ret = 0;
        if (resetPending)
            resetSort(NULL, 0, 0); // This is slightly suboptimal
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            keyCursor = cursors[key];
            ret += CKeyLevelManager::getCount();
        }
        return ret;
    }

    virtual unsigned __int64 checkCount(unsigned __int64 max)
    {
        assertex (!sortFieldOffset);  // we should have avoided using a stepping merger for precheck of limits, both for efficiency and because this code won't work
                                      // as the sequence numbers are not in sequence
        unsigned __int64 ret = 0;
        if (resetPending)
            resetSort(NULL, 0, 0); // this is a little suboptimal as we will not bail out early
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            keyCursor = cursors[key];
            unsigned __int64 thisKeyCount = CKeyLevelManager::checkCount(max);
            ret += thisKeyCount;
            if (thisKeyCount > max)
                return ret;
            max -= thisKeyCount;
        }
        return ret;
    }

    virtual void serializeCursorPos(MemoryBuffer &mb)
    {
//      dumpMergeHeap();
        mb.append(activekeys);
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            mb.append(keyNoArray.item(key));
            cursors[key]->serializeCursorPos(mb);
        }
    }

    virtual void deserializeCursorPos(MemoryBuffer &mb)
    {
        mb.read(activekeys);
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned keyno;
            mb.read(keyno);
            keyNoArray.append(keyno);
            keyCursor = keyset->queryPart(keyno)->getCursor(filter, logExcessiveSeeks);
            keyCursor->deserializeCursorPos(mb, ctx);
            cursorArray.append(*keyCursor);
            mergeHeapArray.append(i);
        }
        cursors = cursorArray.getArray();
        mergeheap = mergeHeapArray.getArray();
    }

    virtual void finishSegmentMonitors()
    {
        CKeyLevelManager::finishSegmentMonitors();
        if (sortFieldOffset)
        {
            filter->checkSize(keyedSize, "[merger]"); // Ensures trailing KSM is setup
            calculateSortSeg();
        }
    }
};

extern jhtree_decl IKeyManager *createKeyMerger(const RtlRecord &_recInfo, IKeyIndexSet * _keys, unsigned _sortFieldOffset, IContextLogger *_ctx, bool _newFilters, bool _logExcessiveSeeks)
{
    return new CKeyMerger(_recInfo, _keys, _sortFieldOffset, _ctx, _newFilters, _logExcessiveSeeks);
}

extern jhtree_decl IKeyManager *createSingleKeyMerger(const RtlRecord &_recInfo, IKeyIndex * _onekey, unsigned _sortFieldOffset, IContextLogger *_ctx, bool _newFilters, bool _logExcessiveSeeks)
{
    return new CKeyMerger(_recInfo, _onekey, _sortFieldOffset, _ctx, _newFilters, _logExcessiveSeeks);
}

class CKeyIndexSet : implements IKeyIndexSet, public CInterface
{
    IPointerArrayOf<IKeyIndex> indexes;
    offset_t recordCount = 0;
    offset_t totalSize = 0;
    StringAttr origFileName;

public:
    IMPLEMENT_IINTERFACE;
    
    virtual bool IsShared() const { return CInterface::IsShared(); }
    void addIndex(IKeyIndex *i) { indexes.append(i); }
    virtual unsigned numParts() { return indexes.length(); }
    virtual IKeyIndex *queryPart(unsigned partNo) { return indexes.item(partNo); }
    virtual void setRecordCount(offset_t count) { recordCount = count; }
    virtual void setTotalSize(offset_t size) { totalSize = size; }
    virtual offset_t getRecordCount() { return recordCount; }
    virtual offset_t getTotalSize() { return totalSize; }
};

extern jhtree_decl IKeyIndexSet *createKeyIndexSet()
{
    return new CKeyIndexSet;
}

extern jhtree_decl IKeyManager *createLocalKeyManager(const RtlRecord &_recInfo, IKeyIndex *_key, IContextLogger *_ctx, bool newFilters, bool _logExcessiveSeeks)
{
    return new CKeyLevelManager(_recInfo, _key, _ctx, newFilters, _logExcessiveSeeks);
}

class CKeyArray : implements IKeyArray, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }
    IPointerArrayOf<IKeyIndexBase> keys;
    virtual IKeyIndexBase *queryKeyPart(unsigned partNo)
    {
        if (!keys.isItem(partNo))
        {
            return NULL;
        }
        IKeyIndexBase *key = keys.item(partNo);
        return key;
    }

    virtual unsigned length() { return keys.length(); }
    void addKey(IKeyIndexBase *f) { keys.append(f); }
};

extern jhtree_decl IKeyArray *createKeyArray()
{
    return new CKeyArray;
}


extern jhtree_decl IIndexLookup *createIndexLookup(IKeyManager *keyManager)
{
    class CIndexLookup : public CSimpleInterfaceOf<IIndexLookup>
    {
        Linked<IKeyManager> keyManager;
    public:
        CIndexLookup(IKeyManager *_keyManager) : keyManager(_keyManager)
        {
        }
        virtual void ensureAvailable() override { }
        virtual unsigned __int64 getCount() override
        {
            return keyManager->getCount();
        }
        virtual unsigned __int64 checkCount(unsigned __int64 limit) override
        {
            return keyManager->checkCount(limit);
        }
        virtual const void *nextKey() override
        {
            if (keyManager->lookup(true))
                return keyManager->queryKeyBuffer();
            else
                return nullptr;
        }
    };
    return new CIndexLookup(keyManager);
}


#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class TestIndexWriteArg : public CThorIndexWriteArg
{
public:
    TestIndexWriteArg(const char * _filename, const char * _compression, IOutputMetaData * _meta)
     : filename(_filename), compression(_compression), meta(_meta)
    {
    }

    virtual const char * getFileName() { return filename; }
    virtual int getSequence() { return 0; }
    virtual IOutputMetaData * queryDiskRecordSize() { return meta; }
    virtual const char * queryRecordECL() { return nullptr; }
    virtual unsigned getFlags() { return compression ? TIWcompressdefined : 0; }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src, IBlobCreator * blobs, unsigned __int64 & filepos)
    {
        UNIMPLEMENTED;
    }
    virtual unsigned getKeyedSize() { return 0; }
    virtual unsigned getMaxKeySize() { return 0; }
    virtual unsigned getFormatCrc() { return 0; }
    virtual const char * queryCompression() { return compression; }

public:
    const char * filename = nullptr;
    const char * compression = nullptr;
    IOutputMetaData * meta = nullptr;
};


class IKeyManagerSlowTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( IKeyManagerSlowTest  );
        CPPUNIT_TEST(testStepping);
        CPPUNIT_TEST(testKeys);
    CPPUNIT_TEST_SUITE_END();

    void testStepping()
    {
        buildTestKeys(false, true, false, false, nullptr, nullptr);
        {
            // We are going to treat as a 7-byte field then a 3-byte field, and request the datasorted by the 3-byte...
            Owned <IKeyIndex> index1 = createKeyIndex("keyfile1.$$$", 0, false, 0);
            Owned <IKeyIndex> index2 = createKeyIndex("keyfile2.$$$", 0, false, 0);
            Owned<IKeyIndexSet> keyset = createKeyIndexSet();
            keyset->addIndex(index1.getClear());
            keyset->addIndex(index2.getClear());
            const char *json = "{ \"ty1\": { \"fieldType\": 4, \"length\": 7 }, "
                               "  \"ty2\": { \"fieldType\": 4, \"length\": 3 }, "
                               " \"fieldType\": 13, \"length\": 10, "
                               " \"fields\": [ "
                               " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                               " { \"name\": \"f2\", \"type\": \"ty2\", \"flags\": 4 } ] "
                               "}";
            Owned<IOutputMetaData> meta = createTypeInfoOutputMetaData(json, false);
            Owned <IKeyManager> tlk1 = createKeyMerger(meta->queryRecordAccessor(true), keyset, 7, NULL, false, false);
            Owned<IStringSet> sset1 = createStringSet(7);
            sset1->addRange("0000003", "0000003");
            sset1->addRange("0000005", "0000006");
            tlk1->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            Owned<IStringSet> sset2 = createStringSet(3);
            sset2->addRange("010", "010");
            sset2->addRange("030", "033");
            Owned<IStringSet> sset3 = createStringSet(3);
            sset3->addRange("999", "XXX");
            sset3->addRange("000", "002");
            tlk1->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk1->finishSegmentMonitors();

            tlk1->reset();

            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003031", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005031", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006031", 10)==0);
            MemoryBuffer mb;
            tlk1->serializeCursorPos(mb);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003033", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005033", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006033", 10)==0);
            ASSERT(!tlk1->lookup(true)); 
            ASSERT(!tlk1->lookup(true)); 

            Owned <IKeyManager> tlk2 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL, false, false);
            tlk2->setKey(keyset);
            tlk2->deserializeCursorPos(mb);
            tlk2->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk2->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk2->finishSegmentMonitors();
            tlk2->reset(true);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000003032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000005032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000006032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000003033", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000005033", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000006033", 10)==0);
            ASSERT(!tlk2->lookup(true)); 
            ASSERT(!tlk2->lookup(true)); 

            Owned <IKeyManager> tlk3 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL, false, false);
            tlk3->setKey(keyset);
            tlk3->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk3->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk3->finishSegmentMonitors();
            tlk3->reset(false);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000003010", 10)==0);
            ASSERT(tlk3->lookupSkip("031", 7, 3)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000003031", 10)==0);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000005031", 10)==0);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000006031", 10)==0);
            ASSERT(!tlk3->lookupSkip("081", 7, 3)); 
            ASSERT(!tlk3->lookup(true)); 

            Owned <IKeyManager> tlk4 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL, false, false);
            tlk4->setKey(keyset);
            tlk4->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk4->append(createKeySegmentMonitor(false, sset3.getLink(), 1, 7, 3));
            tlk4->finishSegmentMonitors();
            tlk4->reset(false);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003999", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005999", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006999", 10)==0);
            ASSERT(!tlk4->lookup(true)); 
            ASSERT(!tlk4->lookup(true)); 

        }
        clearKeyStoreCache(true);
        removeTestKeys();
    }

    void buildTestKeys(bool variable, bool useTrailingHeader, bool noSeek, bool quickCompressed, IOutputMetaData * meta, const char * compression)
    {
        DBGLOG("buildTestKeys(variable=%d, useTrailingHeader=%d, noSeek=%d, quickCompressed=%d, compression=%s)",
               variable, useTrailingHeader, noSeek, quickCompressed, compression ? compression : "");

        buildTestKey("keyfile1.$$$", false, variable, useTrailingHeader, noSeek, quickCompressed, meta, compression);
        buildTestKey("keyfile2.$$$", true, variable, useTrailingHeader, noSeek, quickCompressed, meta, compression);
    }

    void buildTestKey(const char *filename, bool skip, bool variable, bool useTrailingHeader, bool noSeek, bool quickCompressed, IOutputMetaData * meta, const char * compression)
    {
        TestIndexWriteArg helper(filename, compression, meta);
        OwnedIFile file = createIFile(filename);
        OwnedIFileIO io = file->openShared(IFOcreate, IFSHfull);
        Owned<IFileIOStream> out = createIOStream(io);
        if (noSeek)
            out.setown(createNoSeekIOStream(out));
        unsigned maxRecSize = variable ? 18 : 10;
        unsigned keyedSize = 10;
        Owned<IKeyBuilder> builder = createKeyBuilder(out, COL_PREFIX | HTREE_FULLSORT_KEY | HTREE_COMPRESSED_KEY |
                (quickCompressed ? HTREE_QUICK_COMPRESSED_KEY : 0) |
                (variable ? HTREE_VARSIZE : 0) |
                (useTrailingHeader ? USE_TRAILING_HEADER : 0) |
                (noSeek ? TRAILING_HEADER_ONLY : 0) |
                0,
                maxRecSize, NODESIZE, keyedSize, 0, &helper, nullptr, true, false);

        char keybuf[18];
        memset(keybuf, '0', 18);
        uint32_t maxRecordSizeSeen = 0;
        for (unsigned count = 0; count < 10000; count++)
        {
            unsigned datasize = 10;
            if (variable && (count % 10)==0)
            {
                char *blob = new char[count+100000];
                byte seed = count;
                for (unsigned i = 0; i < count+100000; i++)
                {
                    blob[i] = seed;
                    seed = seed * 13 + i;
                }
                offset_t blobid = builder->createBlob(count+100000, blob);
                memcpy(keybuf + 10, &blobid, sizeof(blobid));
                delete [] blob;
                datasize += sizeof(blobid);
            }
            bool skipme = (count % 4 == 0) != skip;
            if (!skipme)
            {
                builder->processKeyData(keybuf, count*10, datasize);
                if (count==48 || count==49)
                    builder->processKeyData(keybuf, count*10, datasize);
            }
            if (datasize > maxRecordSizeSeen)
                maxRecordSizeSeen = datasize;
            unsigned idx = 9;
            for (;;)
            {
                if (keybuf[idx]=='9')
                    keybuf[idx--]='0';
                else
                {
                    keybuf[idx]++;
                    break;
                }
            }
        }
        builder->finish(nullptr, nullptr, maxRecordSizeSeen);
        out->flush();
        DBGLOG("Size %s=%llu", filename, file->size());
    }

    void removeTestKeys()
    {
        ASSERT(remove("keyfile1.$$$")==0);
        ASSERT(remove("keyfile2.$$$")==0);
    }

    void checkBlob(IKeyManager *key, unsigned size)
    {
        unsigned __int64 blobid;
        memcpy(&blobid, key->queryKeyBuffer()+10, sizeof(blobid));
        ASSERT(blobid != 0);
        size32_t blobsize;
        const byte *blob = key->loadBlob(blobid, blobsize, nullptr);
        ASSERT(blob != NULL);
        ASSERT(blobsize == size);
        byte seed = size-100000;
        for (unsigned i = 0; i < size; i++)
        {
            ASSERT(blob[i] == seed);
            seed = seed * 13 + i;
        }
        key->releaseBlobs();
    }
protected:
    void testKeys(bool variable, bool useTrailingHeader, bool noSeek, bool quickCompressed, const char * compression)
    {
        const char *json = variable ?
                "{ \"ty1\": { \"fieldType\": 4, \"length\": 10 }, "
                "  \"ty2\": { \"fieldType\": 15, \"length\": 8, \"child\": \"ty1\" }, "
                " \"fieldType\": 13, \"length\": 10, "
                " \"fields\": [ "
                " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                " { \"name\": \"f3\", \"type\": \"ty2\", \"flags\": 65551 } "  // 0x01000f i.e. payload and blob
                " ]"
                "}"
                :
                "{ \"ty1\": { \"fieldType\": 4, \"length\": 10 }, "
                " \"fieldType\": 13, \"length\": 10, "
                " \"fields\": [ "
                " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                " ] "
                "}";
        Owned<IOutputMetaData> meta = createTypeInfoOutputMetaData(json, false);
        const RtlRecord &recInfo = meta->queryRecordAccessor(true);
        buildTestKeys(variable, useTrailingHeader, noSeek, quickCompressed, meta, compression);
        {
            Owned <IKeyIndex> index1 = createKeyIndex("keyfile1.$$$", 0, false, 0);
            Owned <IKeyManager> tlk1 = createLocalKeyManager(recInfo, index1, NULL, false, false);
            Owned<IStringSet> sset1 = createStringSet(10);
            sset1->addRange("0000000001", "0000000100");
            tlk1->append(createKeySegmentMonitor(false, sset1.getClear(), 0, 0, 10));
            tlk1->finishSegmentMonitors();
            tlk1->reset();

            Owned <IKeyManager> tlk1a = createLocalKeyManager(recInfo, index1, NULL, false, false);
            Owned<IStringSet> sset1a = createStringSet(8);
            sset1a->addRange("00000000", "00000001");
            tlk1a->append(createKeySegmentMonitor(false, sset1a.getClear(), 0, 0, 8));
            tlk1a->append(createKeySegmentMonitor(false, NULL, 1, 8, 1));
            sset1a.setown(createStringSet(1));
            sset1a->addRange("0", "1");
            tlk1a->append(createKeySegmentMonitor(false, sset1a.getClear(), 2, 9, 1));
            tlk1a->finishSegmentMonitors();
            tlk1a->reset();


            Owned<IStringSet> ssetx = createStringSet(10);
            ssetx->addRange("0000000001", "0000000002");
            ASSERT(ssetx->numValues() == 2);
            ssetx->addRange("00000000AK", "00000000AL");
            ASSERT(ssetx->numValues() == 4);
            ssetx->addRange("0000000100", "0010000000");
            ASSERT(ssetx->numValues() == (unsigned) -1);
            ssetx->addRange("0000000001", "0010000000");
            ASSERT(ssetx->numValues() == (unsigned) -1);


            Owned <IKeyIndex> index2 = createKeyIndex("keyfile2.$$$", 0, false, 0);
            Owned <IKeyManager> tlk2 = createLocalKeyManager(recInfo, index2, NULL, false, false);
            Owned<IStringSet> sset2 = createStringSet(10);
            sset2->addRange("0000000001", "0000000100");
            ASSERT(sset2->numValues() == 65536);
            tlk2->append(createKeySegmentMonitor(false, sset2.getClear(), 0, 0, 10));
            tlk2->finishSegmentMonitors();
            tlk2->reset();

            Owned <IKeyManager> tlk3;
            if (!variable)
            {
                Owned<IKeyIndexSet> both = createKeyIndexSet();
                both->addIndex(index1.getLink());
                both->addIndex(index2.getLink());
                Owned<IStringSet> sset3 = createStringSet(10);
                tlk3.setown(createKeyMerger(recInfo, NULL, 0, NULL, false, false));
                tlk3->setKey(both);
                sset3->addRange("0000000001", "0000000100");
                tlk3->append(createKeySegmentMonitor(false, sset3.getClear(), 0, 0, 10));
                tlk3->finishSegmentMonitors();
                tlk3->reset();
            }

            Owned <IKeyManager> tlk2a = createLocalKeyManager(recInfo, index2, NULL, false, false);
            Owned<IStringSet> sset2a = createStringSet(10);
            sset2a->addRange("0000000048", "0000000048");
            ASSERT(sset2a->numValues() == 1);
            tlk2a->append(createKeySegmentMonitor(false, sset2a.getClear(), 0, 0, 10));
            tlk2a->finishSegmentMonitors();
            tlk2a->reset();

            Owned <IKeyManager> tlk2b = createLocalKeyManager(recInfo, index2, NULL, false, false);
            Owned<IStringSet> sset2b = createStringSet(10);
            sset2b->addRange("0000000047", "0000000049");
            ASSERT(sset2b->numValues() == 3);
            tlk2b->append(createKeySegmentMonitor(false, sset2b.getClear(), 0, 0, 10));
            tlk2b->finishSegmentMonitors();
            tlk2b->reset();

            Owned <IKeyManager> tlk2c = createLocalKeyManager(recInfo, index2, NULL, false, false);
            Owned<IStringSet> sset2c = createStringSet(10);
            sset2c->addRange("0000000047", "0000000047");
            tlk2c->append(createKeySegmentMonitor(false, sset2c.getClear(), 0, 0, 10));
            tlk2c->finishSegmentMonitors();
            tlk2c->reset();

            ASSERT_EQUAL(76U, (unsigned)tlk1->getCount());
            ASSERT_EQUAL(30U, (unsigned)tlk1a->getCount());
            ASSERT(tlk2->getCount() == 26);
            ASSERT(tlk2a->getCount() == 2);
            ASSERT(tlk2b->getCount() == 2);
            ASSERT(tlk2c->getCount() == 0);
            if (tlk3)
                ASSERT(tlk3->getCount() == 102);

// MORE -           PUT SOME TESTS IN FOR WILD SEEK STUFF

            unsigned pass;
            char buf[11];
                unsigned i;
            for (pass = 0; pass < 2; pass++)
            {
                tlk1->reset();
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000001", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000002", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000003", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000005", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000006", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000007", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000009", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000010", 10)==0);
                if (variable)
                    checkBlob(tlk1, 10+100000);

                tlk1a->reset();
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000001", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000010", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000011", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000021", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000030", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000031", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000041", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000050", 10)==0);

                tlk2->reset();
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000004", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000008", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000012", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000016", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000020", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000024", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000028", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000032", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000036", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000040", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000044", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000048", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000048", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000052", 10)==0);

                if (tlk3)
                {
                    tlk3->reset();
                    for (i = 1; i <= 100; i++)
                    {
                        ASSERT(tlk3->lookup(true)); 
                        sprintf(buf, "%010d", i);
                        ASSERT(memcmp(tlk3->queryKeyBuffer(), buf, 10)==0);
                        if (i==48 || i==49)
                        {
                            ASSERT(tlk3->lookup(true)); 
                            ASSERT(memcmp(tlk3->queryKeyBuffer(), buf, 10)==0);
                        }
                    }
                    ASSERT(!tlk3->lookup(true)); 
                    ASSERT(!tlk3->lookup(true));    
                }
            }
            tlk1->releaseSegmentMonitors();
            tlk2->releaseSegmentMonitors();
            if (tlk3)
                tlk3->releaseSegmentMonitors();
        }
        clearKeyStoreCache(true);
        removeTestKeys();
    }

    void testKeys()
    {
        try
        {
            ASSERT(sizeof(CKeyIdAndPos) == sizeof(unsigned __int64) + sizeof(offset_t));
            for (bool var : { true, false })
                for (bool trail : { false, true })
                    for (bool noseek : { false, true })
                        for (bool quick : { true, false })
                            for (const char * compression : { (const char *)nullptr, "POC", "inplace" })
                                testKeys(var, trail, noseek, quick, compression);
        }
        catch (IException * e)
        {
            StringBuffer s;
            e->errorMessage(s);
            CPPUNIT_ASSERT_MESSAGE(s.str(), false);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( IKeyManagerSlowTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( IKeyManagerSlowTest, "IKeyManagerSlowTest" );

#endif
