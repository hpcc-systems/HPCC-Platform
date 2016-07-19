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

interface jhtree_decl IDelayedFile : public IInterface
{
    virtual IMemoryMappedFile *queryMappedFile() = 0;
    virtual IFileIO *queryFileIO() = 0;
};

interface jhtree_decl IKeyCursor : public IInterface
{
    virtual bool next(char *dst) = 0;
    virtual bool prev(char *dst) = 0;
    virtual bool first(char *dst) = 0;
    virtual bool last(char *dst) = 0;
    virtual bool gtEqual(const char *src, char *dst, bool seekForward = false) = 0; // returns first record >= src
    virtual bool ltEqual(const char *src, char *dst, bool seekForward = false) = 0; // returns last record <= src
    virtual size32_t getSize() = 0;
    virtual offset_t getFPos() = 0;
    virtual void serializeCursorPos(MemoryBuffer &mb) = 0;
    virtual void deserializeCursorPos(MemoryBuffer &mb, char *keyBuffer) = 0;
    virtual unsigned __int64 getSequence() = 0;
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize) = 0;
    virtual void releaseBlobs() = 0;
    virtual void reset() = 0;
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
    virtual IKeyCursor *getCursor(IContextLogger *ctx) = 0;
    virtual size32_t keySize() = 0;
    virtual bool isFullySorted() = 0;
    virtual bool isTopLevelKey() = 0;
    virtual unsigned getFlags() = 0;
    virtual void dumpNode(FILE *out, offset_t pos, unsigned rowCount, bool isRaw) = 0;
    virtual unsigned queryScans() = 0;
    virtual unsigned querySeeks() = 0;
    virtual size32_t keyedSize() = 0;
    virtual bool hasPayload() = 0;
    virtual const char *queryFileName() = 0;
    virtual offset_t queryBlobHead() = 0;
    virtual void resetCounts() = 0;
    virtual offset_t queryLatestGetNodeOffset() const = 0;
    virtual offset_t queryMetadataHead() = 0;
    virtual IPropertyTree * getMetadata() = 0;
    virtual unsigned getNodeSize() = 0;
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

interface IReplicatedFile;

extern jhtree_decl void clearKeyStoreCache(bool killAll);
extern jhtree_decl void clearKeyStoreCacheEntry(const char *name);
extern jhtree_decl unsigned setKeyIndexCacheSize(unsigned limit);
extern jhtree_decl void clearNodeCache();
// these methods return previous values
extern jhtree_decl bool setNodeCachePreload(bool preload);
extern jhtree_decl size32_t setNodeCacheMem(size32_t cacheSize);
extern jhtree_decl size32_t setLeafCacheMem(size32_t cacheSize);
extern jhtree_decl size32_t setBlobCacheMem(size32_t cacheSize);


extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, bool isTLK, bool preloadAllowed);
extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, IFileIO &ifile, bool isTLK, bool preloadAllowed);
extern jhtree_decl IKeyIndex *createKeyIndex(IReplicatedFile &part, unsigned crc, bool isTLK, bool preloadAllowed);
extern jhtree_decl IKeyIndex *createKeyIndex(const char *filename, unsigned crc, IDelayedFile &ifile, bool isTLK, bool preloadAllowed);

extern jhtree_decl bool isKeyFile(const char *keyfile);
extern jhtree_decl void validateKeyFile(const char *keyfile, offset_t nodepos = 0);
extern jhtree_decl IKeyIndexSet *createKeyIndexSet();
extern jhtree_decl IKeyArray *createKeyArray();
extern jhtree_decl StringBuffer &getIndexMetrics(StringBuffer &);
extern jhtree_decl void resetIndexMetrics();

extern jhtree_decl RelaxedAtomic<unsigned> nodesLoaded;
extern jhtree_decl RelaxedAtomic<unsigned> cacheHits;
extern jhtree_decl RelaxedAtomic<unsigned> cacheAdds;
extern jhtree_decl RelaxedAtomic<unsigned> blobCacheHits;
extern jhtree_decl RelaxedAtomic<unsigned> blobCacheAdds;
extern jhtree_decl RelaxedAtomic<unsigned> leafCacheHits;
extern jhtree_decl RelaxedAtomic<unsigned> leafCacheAdds;
extern jhtree_decl RelaxedAtomic<unsigned> nodeCacheHits;
extern jhtree_decl RelaxedAtomic<unsigned> nodeCacheAdds;
extern jhtree_decl RelaxedAtomic<unsigned> preloadCacheHits;
extern jhtree_decl RelaxedAtomic<unsigned> preloadCacheAdds;
extern jhtree_decl bool logExcessiveSeeks;
extern jhtree_decl bool linuxYield;
extern jhtree_decl bool traceSmartStepping;
extern jhtree_decl bool traceJHtreeAllocations;
extern jhtree_decl bool flushJHtreeCacheOnOOM;
extern jhtree_decl bool useMemoryMappedIndexes;
extern jhtree_decl void clearNodeStats();


#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef wchar_t UChar;
#else //_WIN32
typedef unsigned short UChar;
#endif //_WIN32
#include "rtlkey.hpp"
#include "jmisc.hpp"

class jhtree_decl SegMonitorList : implements IInterface, implements IIndexReadContext, public CInterface
{
    unsigned _lastRealSeg() const;
    unsigned cachedLRS;
    unsigned mergeBarrier;
    bool modified;
public:
    IMPLEMENT_IINTERFACE;
    inline SegMonitorList() { modified = true; mergeBarrier = 0; }
    IArrayOf<IKeySegmentMonitor> segMonitors;

    void setLow(unsigned segno, void *keyBuffer) const;
    unsigned setLowAfter(size32_t offset, void *keyBuffer) const;
    bool incrementKey(unsigned segno, void *keyBuffer) const;
    void endRange(unsigned segno, void *keyBuffer) const;
    inline unsigned lastRealSeg() const { assertex(!modified); return cachedLRS; }
    unsigned lastFullSeg() const;
    bool matched(void *keyBuffer, unsigned &lastMatch) const;
    size32_t getSize() const;
    inline void setMergeBarrier(unsigned offset) { mergeBarrier = offset; }

    void checkSize(size32_t keyedSize, char const * keyname);
    void recalculateCache();
    void finish();

    // interface IIndexReadContext
    virtual void append(IKeySegmentMonitor *segment);
    virtual unsigned ordinality() const;
    virtual IKeySegmentMonitor *item(unsigned i) const;
};

class IRecordLayoutTranslator;

interface IKeyManager : public IInterface, extends IIndexReadContext
{
    virtual void reset(bool crappyHack = false) = 0;
    virtual void releaseSegmentMonitors() = 0;

    virtual const byte *queryKeyBuffer(offset_t & fpos) = 0; //if using RLT: fpos is the translated value, so correct in a normal row
    virtual offset_t queryFpos() = 0; //if using RLT: this is the untranslated fpos, so correct as the part number in TLK but not in a normal row
    virtual unsigned queryRecordSize() = 0;

    virtual bool lookup(bool exact) = 0;
    virtual unsigned __int64 getCount() = 0;
    virtual unsigned __int64 getCurrentRangeCount(unsigned groupSegCount) = 0;
    virtual bool nextRange(unsigned groupSegCount) = 0;
    virtual void setKey(IKeyIndexBase * _key) = 0;
    virtual unsigned __int64 checkCount(unsigned __int64 limit) = 0;
    virtual void serializeCursorPos(MemoryBuffer &mb) = 0;
    virtual void deserializeCursorPos(MemoryBuffer &mb) = 0;
    virtual unsigned querySeeks() const = 0;
    virtual unsigned queryScans() const = 0;
    virtual unsigned querySkips() const = 0;
    virtual unsigned queryNullSkips() const = 0;
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize) = 0;
    virtual void releaseBlobs() = 0;
    virtual void resetCounts() = 0;

    virtual void setLayoutTranslator(IRecordLayoutTranslator * trans) = 0;
    virtual void finishSegmentMonitors() = 0;

    virtual bool lookupSkip(const void *seek, size32_t seekGEOffset, size32_t seeklen) = 0;
};

extern jhtree_decl IKeyManager *createKeyManager(IKeyIndex * _key, unsigned rawSize, IContextLogger *ctx);
extern jhtree_decl IKeyManager *createKeyMerger(IKeyIndexSet * _key, unsigned rawSize, unsigned sortFieldOffset, IContextLogger *ctx);
extern jhtree_decl IKeyManager *createSingleKeyMerger(IKeyIndex * _onekey, unsigned rawSize, unsigned sortFieldOffset, IContextLogger *ctx);

class KLBlobProviderAdapter : implements IBlobProvider
{
    IKeyManager *klManager;
public:
    KLBlobProviderAdapter(IKeyManager *_klManager) : klManager(_klManager) {};
    ~KLBlobProviderAdapter() 
    {
        if (klManager)
            klManager->releaseBlobs(); 
    }
    virtual byte * lookupBlob(unsigned __int64 id) { size32_t dummy; return (byte *) klManager->loadBlob(id, dummy); }
};

extern jhtree_decl bool isCompressedIndex(const char *filename);

#endif
