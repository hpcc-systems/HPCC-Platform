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

#ifndef _JHTREEI_INCL
#define _JHTREEI_INCL

#include "jmutex.hpp"
#include "jhutil.hpp"
#include "jqueue.tpp"
#include "ctfile.hpp"

#include "jhtree.hpp"

typedef OwningStringHTMapping<IKeyIndex> CKeyIndexMapping;
typedef OwningStringSuperHashTableOf<CKeyIndexMapping> CKeyIndexTable;
typedef CMRUCacheMaxCountOf<const char *, IKeyIndex, CKeyIndexMapping, CKeyIndexTable> CKeyIndexMRUCache;

typedef class MapStringToMyClass<IKeyIndex> KeyCache;

class CKeyStore
{
private:
    Mutex mutex;
    Mutex idmutex;
    CKeyIndexMRUCache keyIndexCache;
    int nextId;
    int getUniqId() { synchronized procedure(idmutex); return ++nextId; }
    IKeyIndex *doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
public:
    CKeyStore();
    ~CKeyStore();
    IKeyIndex *load(const char *fileName, unsigned crc, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IFileIO *iFileIO, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IReplicatedFile &part, bool isTLK, bool allowPreload);
    void clearCache(bool killAll);
    void clearCacheEntry(const char *name);
    void setKeyCacheLimit(unsigned limit);
    StringBuffer &getMetrics(StringBuffer &xml);
    void resetMetrics();
};

class CNodeCache;
enum request { LTE, GTE };

// INodeLoader impl.
interface INodeLoader
{
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
};

class jhtree_decl CKeyIndex : public CInterface, implements IKeyIndex, implements INodeLoader
{
    friend class CKeyStore;
    friend class CKeyCursor;

private:
    CKeyIndex(CKeyIndex &);

protected:
    int iD;
    StringAttr name;
    CriticalSection blobCacheCrit;
    Owned<CJHTreeBlobNode> cachedBlobNode;
    offset_t cachedBlobNodePos;

    CKeyHdr *keyHdr;
    CNodeCache *cache;
    CJHTreeNode *rootNode;
    atomic_t keySeeks;
    atomic_t keyScans;
    offset_t latestGetNodeOffset;

    CJHTreeNode *loadNode(char *nodeData, offset_t pos, bool needsCopy);
    CJHTreeNode *getNode(offset_t offset, IContextLogger *ctx);
    CJHTreeBlobNode *getBlobNode(offset_t nodepos);


    CKeyIndex(int _iD, const char *_name);
    ~CKeyIndex();
    void init(KeyHdr &hdr, bool isTLK, bool allowPreload);
    void cacheNodes(CNodeCache *cache, offset_t nodePos, bool isTLK);
    
public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }

// IKeyIndex impl.
    virtual IKeyCursor *getCursor(IContextLogger *ctx);

    virtual size32_t keySize();
    virtual bool hasPayload();
    virtual size32_t keyedSize();
    virtual bool isTopLevelKey();
    virtual bool isFullySorted();
    virtual unsigned getFlags() { return (unsigned char)keyHdr->getKeyType(); };

    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw);

    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return atomic_read(&keyScans); }
    virtual unsigned querySeeks() { return atomic_read(&keySeeks); }
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual offset_t queryBlobHead() { return keyHdr->getHdrStruct()->blobHead; }
    virtual void resetCounts() { atomic_set(&keyScans, 0); atomic_set(&keySeeks, 0); }
    virtual offset_t queryLatestGetNodeOffset() const { return latestGetNodeOffset; }
    virtual offset_t queryMetadataHead();
    virtual IPropertyTree * getMetadata();
    virtual unsigned getNodeSize() { return keyHdr->getNodeSize(); }
 
 // INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
};

class jhtree_decl CMemKeyIndex : public CKeyIndex
{
private:
    Linked<IMemoryMappedFile> io;
public:
    CMemKeyIndex(int _iD, IMemoryMappedFile *_io, const char *_name, bool _isTLK);

    virtual const char *queryFileName() { return name.get(); }
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset);
};

class jhtree_decl CDiskKeyIndex : public CKeyIndex
{
private:
    Linked<IFileIO> io;
    void cacheNodes(CNodeCache *cache, offset_t firstnode, bool isTLK);
    
public:
    CDiskKeyIndex(int _iD, IFileIO *_io, const char *_name, bool _isTLK, bool _allowPreload);

    virtual const char *queryFileName() { return name.get(); }
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset);
};

class jhtree_decl CKeyCursor : public CInterface, public IKeyCursor
{
private:
    IContextLogger *ctx;
    CKeyIndex &key;
    Owned<CJHTreeNode> node;
    unsigned int nodeKey;
    ConstPointerArray activeBlobs;

    CJHTreeNode *locateFirstNode();
    CJHTreeNode *locateLastNode();

public:
    IMPLEMENT_IINTERFACE;
    CKeyCursor(CKeyIndex &_key, IContextLogger *ctx);
    ~CKeyCursor();

    virtual bool next(char *dst);
    virtual bool prev(char *dst);
    virtual bool first(char *dst);
    virtual bool last(char *dst);
    virtual bool gtEqual(const char *src, char *dst, bool seekForward);
    virtual bool ltEqual(const char *src, char *dst, bool seekForward);
    virtual size32_t getSize();
    virtual offset_t getFPos(); 
    virtual void serializeCursorPos(MemoryBuffer &mb);
    virtual void deserializeCursorPos(MemoryBuffer &mb);
    virtual unsigned __int64 getSequence(); 
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual void releaseBlobs();
    virtual void reset();
};


#endif
