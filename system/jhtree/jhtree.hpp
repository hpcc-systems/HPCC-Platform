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

#ifndef _JHTREE_INCL
#define _JHTREE_INCL

#ifdef JHTREE_EXPORTS
#define jhtree_decl DECL_EXPORT
#else
#define jhtree_decl DECL_IMPORT
#endif

#include "jiface.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "errorlist.h"

enum NodeType : byte;

class BloomFilter;
interface IIndexFilterList;
interface IPropertyTree;

interface jhtree_decl IDelayedFile : public IInterface
{
    virtual IMemoryMappedFile *getMappedFile() = 0;
    virtual IFileIO *getFileIO() = 0;
};

interface jhtree_decl IKeyCursor : public IInterface
{
    virtual const char *queryName() const = 0;
    virtual size32_t getSize() = 0;  // Size of current row
    virtual size32_t getKeyedSize() const = 0;  // Size of keyed fields
    virtual void serializeCursorPos(MemoryBuffer &mb) = 0;
    virtual void deserializeCursorPos(MemoryBuffer &mb, IContextLogger *ctx) = 0;
    virtual unsigned __int64 getSequence() = 0;
    virtual offset_t getFPos() const = 0;
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx) = 0;
    virtual void reset() = 0;
    virtual bool lookup(bool exact, IContextLogger *ctx) = 0;
    virtual bool next(IContextLogger *ctx) = 0;
    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen, IContextLogger *ctx) = 0;
    virtual bool skipTo(const void *_seek, size32_t seekOffset, size32_t seeklen) = 0;
    virtual IKeyCursor *fixSortSegs(unsigned sortFieldOffset) = 0;

    virtual unsigned __int64 getCount(IContextLogger *ctx) = 0;
    virtual unsigned __int64 checkCount(unsigned __int64 max, IContextLogger *ctx) = 0;
    virtual unsigned __int64 getCurrentRangeCount(unsigned groupSegCount, IContextLogger *ctx) = 0;
    virtual bool nextRange(unsigned groupSegCount) = 0;
    virtual const byte *queryRecordBuffer() const = 0;
    virtual const byte *queryKeyedBuffer() const = 0;
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const = 0;
};

interface IKeyIndex;

interface jhtree_decl IKeyIndexBase : public IInterface
{
    virtual unsigned numParts() = 0;
    virtual IKeyIndex *queryPart(unsigned idx) = 0;
    virtual bool IsShared() const = 0;
};

interface jhtree_decl IKeyIndex : public IKeyIndexBase
{
    virtual IKeyCursor *getCursor(const IIndexFilterList *filter, bool logExcessiveSeeks) = 0;
    virtual size32_t keySize() = 0;
    virtual bool isFullySorted() = 0;
    virtual bool isTopLevelKey() const = 0;
    virtual __uint64 getPartitionFieldMask() = 0;
    virtual unsigned numPartitions() = 0;
    virtual unsigned getFlags() = 0;
    virtual void dumpNode(FILE *out, offset_t pos, unsigned rowCount, bool isRaw) = 0;
    virtual unsigned queryScans() = 0;
    virtual unsigned querySeeks() = 0;
    virtual size32_t keyedSize() = 0;
    virtual bool hasPayload() = 0;
    virtual const char *queryFileName() const = 0;
    virtual unsigned queryId() const = 0;
    virtual offset_t queryBlobHead() = 0;
    virtual void resetCounts() = 0;
    virtual offset_t queryLatestGetNodeOffset() const = 0;
    virtual offset_t queryMetadataHead() = 0;
    virtual IPropertyTree * getMetadata() = 0;

    virtual unsigned getNodeSize() = 0;
    virtual const IFileIO *queryFileIO() const = 0;
    virtual bool hasSpecialFileposition() const = 0;
    virtual bool needsRowBuffer() const = 0;
    virtual bool prewarmPage(offset_t offset, NodeType type) = 0;
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const = 0;
    virtual offset_t queryFirstBranchOffset() = 0;
};

interface IKeyArray : extends IInterface
{
    virtual bool IsShared() const = 0;
    virtual IKeyIndexBase *queryKeyPart(unsigned partNo) = 0;
    virtual unsigned length() = 0;
    virtual void addKey(IKeyIndexBase *f) = 0;
};

interface jhtree_decl IKeyIndexSet : public IKeyIndexBase
{
    virtual void addIndex(IKeyIndex *newPart) = 0;
    virtual void setRecordCount(offset_t count) = 0;
    virtual void setTotalSize(offset_t size) = 0;
    virtual offset_t getRecordCount() = 0;
    virtual offset_t getTotalSize() = 0;
};

interface ICacheInfoRecorder
{
    virtual void noteWarm(unsigned fileIdx, offset_t page, size32_t len, NodeType type) = 0;
};


extern jhtree_decl void clearKeyStoreCache(bool killAll);
extern jhtree_decl void clearKeyStoreCacheEntry(const char *name);
extern jhtree_decl void clearKeyStoreCacheEntry(const IFileIO *io);
extern jhtree_decl unsigned setKeyIndexCacheSize(unsigned limit);
extern jhtree_decl void clearNodeCache();
extern jhtree_decl void logCacheState();
// these methods return previous values
extern jhtree_decl size_t setNodeCacheMem(size_t cacheSize);
extern jhtree_decl size_t setLeafCacheMem(size_t cacheSize);
extern jhtree_decl size_t setBlobCacheMem(size_t cacheSize);
extern jhtree_decl void setNodeFetchThresholdNs(__uint64 thresholdNs);
extern jhtree_decl void setIndexWarningThresholds(IPropertyTree * options);

extern jhtree_decl void getNodeCacheInfo(ICacheInfoRecorder &cacheInfo);

extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, bool isTLK, size32_t blockedIOSize);
extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, IFileIO &ifile, unsigned fileIdx, bool isTLK, size32_t blockedIOSize);
extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, IDelayedFile &ifile, unsigned fileIdx, bool isTLK, size32_t blockedIOSize);

extern jhtree_decl bool isIndexFile(const char *fileName);
extern jhtree_decl bool isIndexFile(IFile *file);
extern jhtree_decl IKeyIndexSet *createKeyIndexSet();
extern jhtree_decl IKeyArray *createKeyArray();
extern jhtree_decl StringBuffer &getIndexMetrics(StringBuffer &);
extern jhtree_decl void resetIndexMetrics();
extern jhtree_decl void recordEventIndexInformation();

extern jhtree_decl RelaxedAtomic<unsigned> nodesLoaded;

extern std::atomic<unsigned __int64> branchSearchCycles;
extern std::atomic<unsigned __int64> leafSearchCycles;


extern jhtree_decl bool linuxYield;
extern jhtree_decl bool flushJHtreeCacheOnOOM;
extern jhtree_decl bool useMemoryMappedIndexes;
extern jhtree_decl void logNodeCacheStats(const char *prefix);


#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef char16_t UChar;
#else //_WIN32
typedef unsigned short UChar;
#endif //_WIN32
#include "rtlkey.hpp"
#include "rtlnewkey.hpp"
#include "jmisc.hpp"

class RtlRecord;
interface IDynamicTransform;

class jhtree_decl SegMonitorList : public CInterfaceOf<IIndexFilterList>
{
    unsigned cachedLRS = 0;
    bool modified = true;
    bool unfiltered = true;
    const RtlRecord &recInfo;
    unsigned keySegCount;
    IArrayOf<IKeySegmentMonitor> segMonitors;

    size32_t getSize() const;
    unsigned _lastRealSeg();
    SegMonitorList(const SegMonitorList &_from, const char *fixedVals, unsigned sortFieldOffset);
public:
    SegMonitorList(const RtlRecord &_recInfo);

    // interface IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment) override;
    virtual IIndexFilter *item(unsigned i) const override;
    virtual void append(FFoption option, const IFieldFilter * filter) override;

    // interface IIndexFilterList

    virtual void setLow(unsigned segno, void *keyBuffer) const override;
    virtual unsigned setLowAfter(size32_t offset, void *keyBuffer) const override;
    virtual bool incrementKey(unsigned segno, void *keyBuffer) const override;
    virtual void endRange(unsigned segno, void *keyBuffer) const override;
    virtual unsigned lastRealSeg() const override { assertex(!modified); return cachedLRS; }
    virtual bool isUnfiltered() const override { assertex(!modified); return unfiltered; }
    unsigned lastFullSeg() const override;
    virtual unsigned numFilterFields() const override { return segMonitors.length(); }
    virtual IIndexFilterList *fixSortSegs(const char *fixedVals, unsigned sortFieldOffset) const override
    {
        return new SegMonitorList(*this, fixedVals, sortFieldOffset);
    }
    virtual void reset() override;
    virtual void checkSize(size32_t keyedSize, char const * keyname) const override;
    virtual void recalculateCache() override;
    virtual void finish(size32_t keyedSize) override;
    virtual void describe(StringBuffer &out) const override;
    virtual bool matchesBuffer(const void *buffer, unsigned lastSeg, unsigned &matchSeg) const override;
    virtual unsigned getFieldOffset(unsigned idx) const override { return recInfo.getFixedOffset(idx); }
    virtual bool canMatch() const override;
};

interface IIndexLookup : extends IInterface // similar to a small subset of IKeyManager
{
    virtual void ensureAvailable() = 0;
    virtual const void *nextKey() = 0;
    virtual unsigned __int64 getCount() = 0;
    virtual unsigned __int64 checkCount(unsigned __int64 limit) = 0;
};

interface IKeyManager : public IInterface, extends IIndexReadContext
{
    virtual void reset(bool crappyHack = false) = 0;
    virtual void releaseSegmentMonitors() = 0;

    virtual const byte *queryKeyBuffer() = 0; //if using RLT: fpos is the translated value, so correct in a normal row
    virtual unsigned __int64 querySequence() = 0;
    virtual offset_t queryFPos() const = 0;   // filepos from current row as returned by queryKeyBuffer()
    virtual size32_t queryRowSize() = 0;      // Size of current row as returned by queryKeyBuffer()

    virtual bool lookup(bool exact) = 0;
    virtual unsigned __int64 getCount() = 0;
    virtual unsigned __int64 getCurrentRangeCount(unsigned groupSegCount) = 0;
    virtual bool nextRange(unsigned groupSegCount) = 0;
    virtual void setKey(IKeyIndexBase * _key) = 0;
    virtual void setChooseNLimit(unsigned __int64 _rowLimit) = 0; // for choosen type functionality
    virtual unsigned __int64 checkCount(unsigned __int64 limit) = 0;
    virtual void serializeCursorPos(MemoryBuffer &mb) = 0;
    virtual void deserializeCursorPos(MemoryBuffer &mb) = 0;
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx) = 0;
    virtual void releaseBlobs() = 0;

    virtual void setLayoutTranslator(const IDynamicTransform * trans) = 0;
    virtual void finishSegmentMonitors() = 0;
    virtual void describeFilter(StringBuffer &out) const = 0;

    virtual bool lookupSkip(const void *seek, size32_t seekGEOffset, size32_t seeklen) = 0;
    virtual unsigned getPartition() = 0;  // Use PARTITION() to retrieve partno, if possible, or zero to mean read all

    virtual unsigned numActiveKeys() const = 0;
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const = 0;
};

inline offset_t extractFpos(IKeyManager * manager)
{
    return manager->queryFPos();
}

class RtlRecord;

extern jhtree_decl IKeyManager *createLocalKeyManager(const RtlRecord &_recInfo, IKeyIndex * _key, IContextLogger *ctx, bool _newFilters, bool _logExcessiveSeeks);
extern jhtree_decl IKeyManager *createKeyMerger(const RtlRecord &_recInfo, IKeyIndexSet * _key, unsigned sortFieldOffset, IContextLogger *ctx, bool _newFilters, bool _logExcessiveSeeks);
extern jhtree_decl IKeyManager *createSingleKeyMerger(const RtlRecord &_recInfo, IKeyIndex * _onekey, unsigned sortFieldOffset, IContextLogger *ctx, bool _newFilters, bool _logExcessiveSeeks);

class KLBlobProviderAdapter : implements IBlobProvider
{
    IKeyManager *klManager;
    IContextLogger *ctx;
public:
    KLBlobProviderAdapter(IKeyManager *_klManager, IContextLogger *_ctx) : klManager(_klManager), ctx(_ctx) {};
    ~KLBlobProviderAdapter() 
    {
        if (klManager)
            klManager->releaseBlobs(); 
    }
    virtual const byte * lookupBlob(unsigned __int64 id) { size32_t dummy; return klManager->loadBlob(id, dummy, ctx); }
};

extern jhtree_decl bool isCompressedIndex(const char *filename);
extern jhtree_decl bool isIndexFile(IFileIO *fileIO);
extern jhtree_decl bool isIndexFile(IFile *filename);

extern jhtree_decl IIndexLookup *createIndexLookup(IKeyManager *keyManager);

#define JHTREE_KEY_NOT_SORTED JHTREE_ERROR_START

constexpr bool isIndexReadActivity(ThorActivityKind actKind)
{
    switch (actKind)
    {
        case TAKindexread:
        case TAKindexnormalize:
        case TAKindexaggregate:
        case TAKindexcount:
        case TAKindexgroupaggregate:
        case TAKindexexists:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
        case TAKkeyedjoin:
        case TAKkeyeddistribute:
        case TAKkeyeddenormalize:
        case TAKkeyeddenormalizegroup:
            return true;
        default:
            return false;
    }
}

#endif
