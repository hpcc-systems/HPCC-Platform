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

typedef OwningStringHTMapping<IKeyIndex> CKeyIndexMapping;
typedef OwningStringSuperHashTableOf<CKeyIndexMapping> CKeyIndexTable;
typedef CMRUCacheMaxCountOf<const char *, IKeyIndex, CKeyIndexMapping, CKeyIndexTable> CKeyIndexMRUCache;

typedef class MapStringToMyClass<IKeyIndex> KeyCache;

class CKeyStore
{
private:
    Mutex mutex;
    CKeyIndexMRUCache keyIndexCache;
    std::atomic<unsigned> nextId { 0x80000000 };
    unsigned getUniqId(unsigned useId)
    {
        if (useId != (unsigned) -1)
            return useId;
        return ++nextId;
    }
    IKeyIndex *doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, unsigned fileIdx, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
public:
    CKeyStore();
    ~CKeyStore();
    IKeyIndex *load(const char *fileName, unsigned crc, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IFileIO *iFileIO, unsigned fileIdx, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
    void clearCache(bool killAll);
    void clearCacheEntry(const char *name);
    void clearCacheEntry(const IFileIO *io);
    unsigned setKeyCacheLimit(unsigned limit);
    StringBuffer &getMetrics(StringBuffer &xml);
    void resetMetrics();
};

class CNodeCache;
enum request { LTE, GTE };

// INodeLoader impl.
interface INodeLoader
{
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
    virtual CJHTreeNode *locateFirstNode(KeyStatsCollector &stats) = 0;
    virtual CJHTreeNode *locateLastNode(KeyStatsCollector &stats) = 0;
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
    CriticalSection blobCacheCrit;
    Owned<CJHTreeBlobNode> cachedBlobNode;
    CIArrayOf<IndexBloomFilter> bloomFilters;
    offset_t cachedBlobNodePos;

    CKeyHdr *keyHdr;
    CNodeCache *cache;
    CJHTreeNode *rootNode;
    RelaxedAtomic<unsigned> keySeeks;
    RelaxedAtomic<unsigned> keyScans;
    offset_t latestGetNodeOffset;

    CJHTreeNode *loadNode(char *nodeData, offset_t pos, bool needsCopy);
    CJHTreeNode *getNode(offset_t offset, IContextLogger *ctx);
    CJHTreeBlobNode *getBlobNode(offset_t nodepos);


    CKeyIndex(unsigned _iD, const char *_name);
    ~CKeyIndex();
    void init(KeyHdr &hdr, bool isTLK, bool allowPreload);
    void cacheNodes(CNodeCache *cache, offset_t nodePos, bool isTLK);
    void loadBloomFilters();
    
public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }

// IKeyIndex impl.
    virtual IKeyCursor *getCursor(const IIndexFilterList *filter, bool logExcessiveSeeks) override;

    virtual size32_t keySize();
    virtual bool hasPayload();
    virtual size32_t keyedSize();
    virtual bool isTopLevelKey() override;
    virtual bool isFullySorted() override;
    virtual __uint64 getPartitionFieldMask() override;
    virtual unsigned numPartitions() override;
    virtual unsigned getFlags() { return (unsigned char)keyHdr->getKeyType(); };

    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw);

    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return keyScans; }
    virtual unsigned querySeeks() { return keySeeks; }
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual offset_t queryBlobHead() { return keyHdr->getHdrStruct()->blobHead; }
    virtual void resetCounts() { keyScans.store(0); keySeeks.store(0); }
    virtual offset_t queryLatestGetNodeOffset() const { return latestGetNodeOffset; }
    virtual offset_t queryMetadataHead();
    virtual IPropertyTree * getMetadata();

    bool bloomFilterReject(const IIndexFilterList &segs) const;

    virtual unsigned getNodeSize() { return keyHdr->getNodeSize(); }
    virtual bool hasSpecialFileposition() const;
    virtual bool needsRowBuffer() const;
    virtual bool prewarmPage(offset_t page);
 
 // INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
    CJHTreeNode *locateFirstNode(KeyStatsCollector &stats);
    CJHTreeNode *locateLastNode(KeyStatsCollector &stats);
};

class jhtree_decl CMemKeyIndex : public CKeyIndex
{
private:
    Linked<IMemoryMappedFile> io;
public:
    CMemKeyIndex(unsigned _iD, IMemoryMappedFile *_io, const char *_name, bool _isTLK);

    virtual const char *queryFileName() { return name.get(); }
    virtual const IFileIO *queryFileIO() const override { return nullptr; }
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset);
};

class jhtree_decl CDiskKeyIndex : public CKeyIndex
{
private:
    Linked<IFileIO> io;
    void cacheNodes(CNodeCache *cache, offset_t firstnode, bool isTLK);
    
public:
    CDiskKeyIndex(unsigned _iD, IFileIO *_io, const char *_name, bool _isTLK, bool _allowPreload);

    virtual const char *queryFileName() { return name.get(); }
    virtual const IFileIO *queryFileIO() const override { return io; }
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset);
};

class jhtree_decl CKeyCursor : public CInterfaceOf<IKeyCursor>
{
protected:
    CKeyIndex &key;
    const IIndexFilterList *filter;
    char *keyBuffer = nullptr;
    Owned<CJHTreeNode> node;
    unsigned int nodeKey;

    bool eof=false;
    bool matched=false; //MORE - this should probably be renamed. It's tracking state from one call of lookup to the next.
    bool logExcessiveSeeks = false;
public:
    CKeyCursor(CKeyIndex &_key, const IIndexFilterList *filter, bool _logExcessiveSeeks);
    ~CKeyCursor();

    virtual bool next(char *dst, KeyStatsCollector &stats) override;
    virtual const char *queryName() const override;
    virtual size32_t getSize();
    virtual size32_t getKeyedSize() const;
    virtual offset_t getFPos(); 
    virtual void serializeCursorPos(MemoryBuffer &mb);
    virtual void deserializeCursorPos(MemoryBuffer &mb, KeyStatsCollector &stats);
    virtual unsigned __int64 getSequence(); 
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual void reset();
    virtual bool lookup(bool exact, KeyStatsCollector &stats) override;
    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen, KeyStatsCollector &stats) override;
    virtual bool skipTo(const void *_seek, size32_t seekOffset, size32_t seeklen) override;
    virtual IKeyCursor *fixSortSegs(unsigned sortFieldOffset) override;

    virtual unsigned __int64 getCount(KeyStatsCollector &stats) override;
    virtual unsigned __int64 checkCount(unsigned __int64 max, KeyStatsCollector &stats) override;
    virtual unsigned __int64 getCurrentRangeCount(unsigned groupSegCount, KeyStatsCollector &stats) override;
    virtual bool nextRange(unsigned groupSegCount) override;
    virtual const byte *queryKeyBuffer() const override;
protected:
    CKeyCursor(const CKeyCursor &from);

    bool last(char *dst, KeyStatsCollector &stats);
    bool gtEqual(const char *src, char *dst, KeyStatsCollector &stats);
    bool ltEqual(const char *src, KeyStatsCollector &stats);
    bool _lookup(bool exact, unsigned lastSeg, KeyStatsCollector &stats);
    void reportExcessiveSeeks(unsigned numSeeks, unsigned lastSeg, size32_t recSize, KeyStatsCollector &stats);

    inline void setLow(unsigned segNo)
    {
        filter->setLow(segNo, keyBuffer);
    }
    inline unsigned setLowAfter(size32_t offset)
    {
        return filter->setLowAfter(offset, keyBuffer);
    }
    inline bool incrementKey(unsigned segno) const
    {
        return filter->incrementKey(segno, keyBuffer);
    }
    inline void endRange(unsigned segno)
    {
        filter->endRange(segno, keyBuffer);
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

protected:
    IndexRowFilter(const IndexRowFilter &_from, const char *fixedVals, unsigned sortFieldOffset);

    const RtlRecord &recInfo;
    unsigned lastReal = 0;
    unsigned lastFull = 0;
    unsigned keyedSize = 0;
    unsigned keySegCount = 0;

};

#endif
