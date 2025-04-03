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

#ifndef _JHTREEI_INCL
#define _JHTREEI_INCL

#include "jmutex.hpp"
#include "jhutil.hpp"
#include "jqueue.tpp"
#include "ctfile.hpp"

#include "jhtree.hpp"
#include "bloom.hpp"

class CKeyIndexMapping : public OwningStringHTMapping<IKeyIndex>
{
public:
    CKeyIndexMapping(const char *fp, IKeyIndex &et) : OwningStringHTMapping<IKeyIndex>(fp, et) { }

//The following pointers are used to maintain the position in the LRU cache
    CKeyIndexMapping * prev = nullptr;
    CKeyIndexMapping * next = nullptr;
};


typedef OwningStringSuperHashTableOf<CKeyIndexMapping> CKeyIndexTable;
typedef CMRUCacheMaxCountOf<const char *, IKeyIndex, CKeyIndexMapping, CKeyIndexTable> CKeyIndexMRUCache;

typedef class MapStringToMyClass<IKeyIndex> KeyCache;

class CKeyStore
{
private:
    Mutex mutex;
    CKeyIndexMRUCache keyIndexCache;
    std::atomic<unsigned> nextId { 0x80000000 };

    unsigned getUniqId(unsigned useId, const char * filename);
    IKeyIndex *doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, unsigned fileIdx, IMemoryMappedFile *iMappedFile, bool isTLK, size32_t blockedIOSize);
public:
    CKeyStore();
    ~CKeyStore();
    IKeyIndex *load(const char *fileName, unsigned crc, bool isTLK, size32_t blockedIOSize);
    IKeyIndex *load(const char *fileName, unsigned crc, IFileIO *iFileIO, unsigned fileIdx, bool isTLK, size32_t blockedIOSize);
    IKeyIndex *load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, size32_t blockedIOSize);
    void clearCache(bool killAll);
    void clearCacheEntry(const char *name);
    void clearCacheEntry(const IFileIO *io);
    unsigned setKeyCacheLimit(unsigned limit);
    StringBuffer &getMetrics(StringBuffer &xml);
    void recordEventIndexInformation();
    void resetMetrics();
};

class CNodeCache;
enum request { LTE, GTE };

// INodeLoader impl.
interface INodeLoader
{
    virtual const CJHTreeNode *loadNode(cycle_t * fetchCycles, offset_t offset, IFileIO *useIO) const = 0;
    virtual const CJHSearchNode *locateFirstLeafNode(IContextLogger *ctx) const = 0;
    virtual const CJHSearchNode *locateLastLeafNode(IContextLogger *ctx) const = 0;
    virtual const char *queryFileName() const = 0;
};

class jhtree_decl CKeyIndex : implements IKeyIndex, implements INodeLoader, public CInterface
{
    friend class CKeyStore;
    friend class CKeyCursor;

private:
    CKeyIndex(CKeyIndex &);

protected:
    unsigned iD;
    StringAttr name;
    mutable CriticalSection cacheCrit;
    Owned<const CJHTreeBlobNode> cachedBlobNode;
    CIArrayOf<IndexBloomFilter> bloomFilters;
    std::atomic<bool> bloomFiltersLoaded = {0};
    offset_t cachedBlobNodePos;
    size32_t blockedIOSize = 0;

    CKeyHdr *keyHdr;
    CNodeCache *cache;
    const CJHSearchNode *rootNode;
    mutable RelaxedAtomic<unsigned> keySeeks;
    mutable RelaxedAtomic<unsigned> keyScans;
    mutable offset_t latestGetNodeOffset;  // NOT SAFE but only used by keydiff

    CJHTreeNode *_loadNode(char *nodeData, offset_t pos, bool needsCopy) const;
    CJHTreeNode *_createNode(const NodeHdr &hdr) const;
    const CJHSearchNode *getIndexNode(offset_t offset, NodeType type, IContextLogger *ctx) const;
    const CJHSearchNode *getIndexNodeUsingLoader(const INodeLoader *nodeLoader, offset_t offset, NodeType type, IContextLogger *ctx) const;
    const CJHTreeBlobNode *getBlobNode(offset_t nodepos, IContextLogger *ctx);

    CKeyIndex(unsigned _iD, const char *_name);
    ~CKeyIndex();
    void init(KeyHdr &hdr, bool isTLK);
    void loadBloomFilters();
    const CJHSearchNode *getRootNode() const;

    inline bool isTLK() const { return (keyHdr->getKeyType() & HTREE_TOPLEVEL_KEY) != 0; }
 
public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }

// IKeyIndex impl.
    virtual IKeyCursor *getCursor(const IIndexFilterList *filter, bool logExcessiveSeeks) override;

    virtual size32_t keySize();
    virtual bool hasPayload();
    virtual size32_t keyedSize();
    virtual bool isTopLevelKey() const override final;
    virtual bool isFullySorted() override;
    virtual __uint64 getPartitionFieldMask() override;
    virtual unsigned numPartitions() override;
    virtual unsigned getFlags() { return (unsigned char)keyHdr->getKeyType(); };

    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw);
    virtual unsigned queryId() const override { return iD; }

    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return keyScans; }
    virtual unsigned querySeeks() { return keySeeks; }
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx);
    virtual offset_t queryBlobHead() { return keyHdr->getHdrStruct()->blobHead; }
    virtual void resetCounts() { keyScans.store(0); keySeeks.store(0); }
    virtual offset_t queryLatestGetNodeOffset() const { return latestGetNodeOffset; }
    virtual offset_t queryMetadataHead();
    virtual IPropertyTree * getMetadata();

    unsigned getBranchDepth() const { return keyHdr->getHdrStruct()->hdrseq; }
    bool bloomFilterReject(const IIndexFilterList &segs) const;

    virtual unsigned getNodeSize() { return keyHdr->getNodeSize(); }
    virtual bool hasSpecialFileposition() const;
    virtual bool needsRowBuffer() const;
    virtual bool prewarmPage(offset_t page, NodeType type);
    virtual offset_t queryFirstBranchOffset() override;

 // INodeLoader impl.
    virtual const CJHTreeNode *loadNode(cycle_t * fetchCycles, offset_t offset, IFileIO *useIO) const override = 0;  // Must be implemented in derived classes
    virtual const CJHSearchNode *locateFirstLeafNode(IContextLogger *ctx) const override;
    virtual const CJHSearchNode *locateLastLeafNode(IContextLogger *ctx) const override;

    virtual void mergeStats(CRuntimeStatisticCollection & stats) const override {}

    virtual const char *queryFileName() const = 0;
};

class jhtree_decl CMemKeyIndex : public CKeyIndex
{
private:
    Linked<IMemoryMappedFile> io;
public:
    CMemKeyIndex(unsigned _iD, IMemoryMappedFile *_io, const char *_name, bool _isTLK);

    virtual const char *queryFileName() const { return name.get(); }
    virtual const IFileIO *queryFileIO() const override { return nullptr; }
// INodeLoader impl.
    virtual const CJHTreeNode *loadNode(cycle_t * fetchCycles, offset_t offset, IFileIO *useIO) const override;
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const override {}
};

class jhtree_decl CDiskKeyIndex : public CKeyIndex
{
private:
    Linked<IFileIO> io;
    void cacheNodes(CNodeCache *cache, offset_t firstnode, bool isTLK);
    
public:
    CDiskKeyIndex(unsigned _iD, IFileIO *_io, const char *_name, bool _isTLK, size32_t _blockedIOSize);

    virtual const char *queryFileName() const { return name.get(); }
    virtual const IFileIO *queryFileIO() const override { return io; }
// INodeLoader impl.
    virtual const CJHTreeNode *loadNode(cycle_t * fetchCycles, offset_t offset, IFileIO *useIO) const override;
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const override { ::mergeStats(stats, io); }
};

class jhtree_decl CKeyCursor : public CInterfaceOf<IKeyCursor>, implements INodeLoader
{
protected:
    static constexpr unsigned maxParentNodes = 4;
    CKeyIndex &key;
    const IIndexFilterList *filter;
    char *recordBuffer = nullptr;
    Owned<const CJHSearchNode> node;
    Owned<const CJHSearchNode> parents[maxParentNodes];
    unsigned int parentNodeKeys[maxParentNodes] = {0};
    unsigned int nodeKey;
    
    mutable bool fullBufferValid = false;
    bool eof=false;
    bool matched=false; //MORE - this should probably be renamed. It's tracking state from one call of lookup to the next.
    bool logExcessiveSeeks = false;
    Owned<IFileIO> myIO;  // This should be a blockedIO based on key.IO if blocking is enabled, else nullptr
public:
    CKeyCursor(CKeyIndex &_key, const IIndexFilterList *filter, bool _logExcessiveSeeks, unsigned _blockedIOSize);
    ~CKeyCursor();

    virtual const char *queryName() const override;
    virtual size32_t getSize();
    virtual size32_t getKeyedSize() const;
    virtual offset_t getFPos() const;
    virtual void serializeCursorPos(MemoryBuffer &mb);
    virtual void deserializeCursorPos(MemoryBuffer &mb, IContextLogger *ctx);
    virtual unsigned __int64 getSequence(); 
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize, IContextLogger *ctx);
    virtual void reset();
    virtual bool lookup(bool exact, IContextLogger *ctx) override;
    virtual bool next(IContextLogger *ctx) override;
    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen, IContextLogger *ctx) override;
    virtual bool skipTo(const void *_seek, size32_t seekOffset, size32_t seeklen) override;
    virtual IKeyCursor *fixSortSegs(unsigned sortFieldOffset) override;

    virtual unsigned __int64 getCount(IContextLogger *ctx) override;
    virtual unsigned __int64 checkCount(unsigned __int64 max, IContextLogger *ctx) override;
    virtual unsigned __int64 getCurrentRangeCount(unsigned groupSegCount, IContextLogger *ctx) override;
    virtual bool nextRange(unsigned groupSegCount) override;
    virtual const byte *queryRecordBuffer() const override;
    virtual const byte *queryKeyedBuffer() const override;

 // INodeLoader impl.
    virtual const CJHTreeNode *loadNode(cycle_t * fetchCycles, offset_t offset, IFileIO *useIO) const override
    {
        return key.loadNode(fetchCycles, offset, myIO);
    }
    virtual const CJHSearchNode *locateFirstLeafNode(IContextLogger *ctx) const override
    {
        return key.locateFirstLeafNode(ctx);
    }
    virtual const CJHSearchNode *locateLastLeafNode(IContextLogger *ctx) const override
    {
        return key.locateLastLeafNode(ctx);
    }
    const CJHSearchNode *getCursorNode(offset_t offset, NodeType type, IContextLogger *ctx) const
    {
        return key.getIndexNodeUsingLoader(this, offset, type, ctx);
    }

protected:
    CKeyCursor(const CKeyCursor &from);

    // Internal searching functions - set current node/nodekey/matched values
    bool _last(IContextLogger *ctx);        // Updates node/nodekey
    bool _gtEqual(IContextLogger *ctx);     // Reads recordBuffer, updates node/nodekey 
    bool _ltEqual(IContextLogger *ctx);     // Reads recordBuffer, updates node/nodekey 
    bool _next(IContextLogger *ctx);        // Updates node/nodekey 
    // if _lookup returns true, recordBuffer will contain keyed portion of result
    bool _lookup(bool exact, unsigned lastSeg, bool unfiltered, IContextLogger *ctx);

    void clearParentNodes();

    void reportExcessiveSeeks(unsigned numSeeks, unsigned lastSeg, IContextLogger *ctx);

    inline void setLow(unsigned segNo)
    {
        filter->setLow(segNo, recordBuffer);
    }
    inline unsigned setLowAfter(size32_t offset)
    {
        return filter->setLowAfter(offset, recordBuffer);
    }
    inline bool incrementKey(unsigned segno) const
    {
        return filter->incrementKey(segno, recordBuffer);
    }
    inline void endRange(unsigned segno)
    {
        filter->endRange(segno, recordBuffer);
    }
    virtual void mergeStats(CRuntimeStatisticCollection & stats) const override
    {
        key.mergeStats(stats);
    }
    virtual const char *queryFileName() const override
    {
        return key.queryFileName();
    }
};

class CPartialKeyCursor : public CKeyCursor
{
public:
    CPartialKeyCursor(const CKeyCursor &from, unsigned sortFieldOffset);
    ~CPartialKeyCursor();
};

// Specialization of a RowFilter allowing us to use them from JHTree. Allowed to assume that all keyed fields are fixed size (for now!)

class IndexRowFilter : public RowFilter, public CInterfaceOf<IIndexFilterList>
{
public:
    IndexRowFilter(const RtlRecord &_recInfo);
    virtual void append(IKeySegmentMonitor *segment) override;
    virtual const IIndexFilter *item(unsigned idx) const override;
    virtual void append(FFoption option, const IFieldFilter * filter) override;

    virtual void setLow(unsigned segno, void *keyBuffer) const override;
    virtual unsigned setLowAfter(size32_t offset, void *keyBuffer) const override;
    virtual bool incrementKey(unsigned segno, void *keyBuffer) const override;
    virtual void endRange(unsigned segno, void *keyBuffer) const override;
    virtual unsigned lastRealSeg() const override;
    virtual unsigned lastFullSeg() const override;
    virtual unsigned numFilterFields() const override;
    virtual IIndexFilterList *fixSortSegs(const char *fixedVals, unsigned sortFieldOffset) const override;
    virtual void reset() override;
    virtual void checkSize(size32_t keyedSize, char const * keyname) const override;
    virtual void recalculateCache() override;
    virtual void finish(size32_t keyedSize) override;
    virtual void describe(StringBuffer &out) const override;
    virtual bool matchesBuffer(const void *buffer, unsigned lastSeg, unsigned &matchSeg) const override;
    virtual unsigned getFieldOffset(unsigned idx) const override { return recInfo.getFixedOffset(idx); }
    virtual bool canMatch() const override;
    virtual bool isUnfiltered() const override;


protected:
    IndexRowFilter(const IndexRowFilter &_from, const char *fixedVals, unsigned sortFieldOffset);

    const RtlRecord &recInfo;
    unsigned lastReal = 0;
    unsigned lastFull = -1;
    unsigned keyedSize = 0;
    unsigned keySegCount = 0;
    bool unfiltered = true;
};

#endif
