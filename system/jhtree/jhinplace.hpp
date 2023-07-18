/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#ifndef JHINPLACE_HPP
#define JHINPLACE_HPP

#include "jiface.hpp"
#include "jhutil.hpp"
#include "hlzw.h"
#include "jcrc.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "ctfile.hpp"

class InplaceNodeSearcher
{
public:
    InplaceNodeSearcher() = default;
    InplaceNodeSearcher(unsigned _count, const byte * data, size32_t _keyLen, const byte * _nullRow);
    void init(unsigned _count, const byte * data, size32_t _keyLen, const byte * _nullRow);

    //Find the first row that is >= the search row
    unsigned findGE(const unsigned len, const byte * search) const;
    size32_t getValueAt(unsigned int num, char *key) const;
    int compareValueAt(const char *src, unsigned int index) const;
    int compareValueAtFallback(const char *src, unsigned int index) const;

protected:
    const byte * nodeData = nullptr;
    const byte * nullRow = nullptr;
    size32_t count = 0;
    size32_t keyLen = 0;
};

//---------------------------------------------------------------------------------------------------------------------

class PartialMatchBuilder;
class PartialMatch : public CInterface
{
public:
    PartialMatch(PartialMatchBuilder * _builder, size32_t _len, const void * _data, unsigned _rowOffset, bool _isRoot);

    bool combine(size32_t newLen, const byte * newData);
    bool removeLast();
    void serialize(MemoryBuffer & buffer);
    void serializeFirst(MemoryBuffer & out);
    bool squash();
    void trace(unsigned indent);
    byte queryFirstByte() const
    {
        dbgassertex(data.length());
        return data.bytes()[0];
    }

    size32_t getSize();
    size32_t getCount();
    const byte * queryNullRow() const;
    bool isEnd() const { return data.length() == 0; }

protected:
    bool allNextAreEnd() const;
    bool allNextAreIdentical(bool allowCache) const;
    unsigned appendRepeat(size32_t offset, size32_t copyOffset, byte repeatByte, size32_t repeatCount);
    inline void cacheSizes()
    {
        if (dirty)
            doCacheSizes();
    }

    void doCacheSizes();
    void describeSquashed(StringBuffer & out);
    size32_t getMaxOffset();
    byte getSequentialOptionFlags() const;
    bool matches(PartialMatch & other, bool ignoreLeadingByte, bool allowCache)
    {
#ifdef _DEBUG
        //Always compare newer nodes with older nodes.
        //This is because the cache of previous matches is invalidated when a new child node is added, and new items
        //are always applied to the most recent entry at a given level.
        assertex((int)(seq - other.seq) > 0);
#endif
        if (allowCache && prevMatch[ignoreLeadingByte] == &other)
            return true;
        return doMatches(other, ignoreLeadingByte, allowCache);
    }
    bool doMatches(PartialMatch & other, bool ignoreLeadingByte, bool allowCache);
    void noteDirty();

protected:
    PartialMatchBuilder * builder;
    MemoryBuffer data;
    MemoryBuffer squashedData;
    CIArrayOf<PartialMatch> next;
#ifdef _DEBUG
    unsigned seq = 0;
#endif
    unsigned rowOffset;
    PartialMatch * prevMatch[2]{ nullptr, nullptr };
    size32_t maxOffset = 0;
    size32_t size = 0;
    size32_t maxCount = 0;
    bool dirty = true;
    bool squashed = false;
    bool isRoot;
};


class PartialMatchBuilder
{
public:
    PartialMatchBuilder(size32_t _keyLen, const byte *_nullRow, bool _optimizeTrailing) : nullRow(_nullRow), keyLen(_keyLen), optimizeTrailing(_optimizeTrailing)
    {
        if (!nullRow)
            optimizeTrailing = false;
        //MORE: Options for #null bytes to include in a row, whether to squash etc.
    }

    void add(size32_t len, const void * data);
    void removeLast();
    void serialize(MemoryBuffer & out);
    void trace();
    void gatherSingleTextCounts(unsigned * counts);

    size32_t queryKeyLen() const { return keyLen; }
    const byte * queryNullRow() const { return nullRow; }
    unsigned getCount();
    unsigned getSize();

public:
    unsigned numDuplicates = 0;

protected:
    Owned<PartialMatch> root;
    const byte * nullRow = nullptr;
    size32_t keyLen;
    bool optimizeTrailing = false;
};

//---------------------------------------------------------------------------------------------------------------------

class jhtree_decl InplaceKeyBuildContext
{
public:
    ~InplaceKeyBuildContext();

public:
    ICompressHandler * compressionHandler = nullptr;
    Owned<ICompressor> compressor; // potentially shared
    StringBuffer compressionOptions;
    MemoryBuffer uncompressed;
    MemoryAttr compressed;
    const byte * nullRow = nullptr;

    //Various stats gathered when building the index
    unsigned numKeyedDuplicates = 0;
    offset_t totalKeyedSize = 0;
    offset_t totalDataSize = 0;
    offset_t numLeafNodes = 0;
    offset_t numBlockCompresses = 0;
    offset_t branchMemorySize = 0;
    offset_t leafMemorySize = 0;
    struct {
        double minCompressionThreshold = 0.95; // use uncompressed if compressed is > 95% uncompressed
        unsigned maxCompressionFactor = 25;    // Don't compress payload to less than 4% of the original by default (beause when it is read it will use lots of memory)
        bool recompress = false;
    } options;
};

//---------------------------------------------------------------------------------------------------------------------

class jhtree_decl CJHInplaceTreeNode : public CJHSearchNode
{
public:
    ~CJHInplaceTreeNode();

    virtual void load(CKeyHdr *keyHdr, const void *rawData, offset_t pos, bool needCopy) override;
    virtual int compareValueAt(const char *src, unsigned int index) const override;
    virtual int locateGE(const char * search, unsigned minIndex) const override;
    virtual int locateGT(const char * search, unsigned minIndex) const override;
    virtual bool getKeyAt(unsigned int num, char *dest) const override;

protected:
    InplaceNodeSearcher searcher;
    Owned<IRandRowExpander> rowexp;  // expander for rand rowdiff
    const byte * positionData = nullptr;
    UnsignedArray payloadOffsets;
    byte * payload = nullptr;
    unsigned __int64 firstSequence = 0;
    unsigned __int64 minPosition = 0;
    size32_t keyLen = 0;
    size32_t keyCompareLen = 0;
    unsigned __int64 positionScale = 1;
    byte sizeMask;
    byte bytesPerPosition = 0;
    bool ownedPayload = false;
};


class jhtree_decl CJHInplaceBranchNode final : public CJHInplaceTreeNode
{
public:
    virtual bool fetchPayload(unsigned int num, char *dest) const override;
    virtual size32_t getSizeAt(unsigned int num) const override;
    virtual offset_t getFPosAt(unsigned int num) const override;
    virtual unsigned __int64 getSequence(unsigned int num) const override;
};

class jhtree_decl CJHInplaceLeafNode final : public CJHInplaceTreeNode
{
public:
    virtual bool fetchPayload(unsigned int num, char *dest) const override;
    virtual size32_t getSizeAt(unsigned int num) const override;
    virtual offset_t getFPosAt(unsigned int num) const override;
    virtual unsigned __int64 getSequence(unsigned int num) const override;
};

class jhtree_decl CInplaceWriteNode : public CWriteNode
{
public:
    CInplaceWriteNode(offset_t fpos, CKeyHdr *keyHdr, bool isLeafNode);

    virtual const void *getLastKeyValue() const override { return lastKeyValue.get(); }
    virtual unsigned __int64 getLastSequence() const override { return lastSequence; }

    void saveLastKey(const void *data, size32_t size, unsigned __int64 sequence);

protected:
    MemoryAttr lastKeyValue;
    unsigned __int64 lastSequence = 0;
    size32_t keyCompareLen = 0;
};

class jhtree_decl CInplaceBranchWriteNode : public CInplaceWriteNode
{
public:
    CInplaceBranchWriteNode(offset_t fpos, CKeyHdr *keyHdr, InplaceKeyBuildContext & _ctx);

    virtual bool add(offset_t pos, const void *data, size32_t size, unsigned __int64 sequence) override;
    virtual void write(IFileIOStream *, CRC32 *crc) override;

protected:
    unsigned getDataSize();

protected:
    InplaceKeyBuildContext & ctx;
    PartialMatchBuilder builder;
    Unsigned64Array positions;
    unsigned nodeSize;
    bool scaleFposByNodeSize = true;
};


struct LeafFilepositionInfo
{
    __uint64 minPosition = 0;
    __uint64 maxPosition = 0;
    unsigned scaleFactor = 0;
    bool linear = true;
};

class jhtree_decl CInplaceLeafWriteNode : public CInplaceWriteNode
{
public:
    CInplaceLeafWriteNode(offset_t fpos, CKeyHdr *keyHdr, InplaceKeyBuildContext & _ctx);

    virtual bool add(offset_t pos, const void *data, size32_t size, unsigned __int64 sequence) override;
    virtual void write(IFileIOStream *, CRC32 *crc) override;

protected:
    unsigned getDataSize(bool includePayload);
    bool recompressAll(unsigned maxSize);

protected:
    InplaceKeyBuildContext & ctx;
    PartialMatchBuilder builder;
    KeyCompressor compressor;
    MemoryBuffer uncompressed;      // Much better if these could be shared by all nodes => refactor
    MemoryAttr compressed;
    UnsignedArray payloadLengths;
    Unsigned64Array positions;
    LeafFilepositionInfo positionInfo;
    __uint64 firstSequence = 0;
    unsigned nodeSize;
    size32_t keyLen = 0;
    size32_t firstUncompressed = 0;
    size32_t sizeCompressedPayload = 0; // Set from closed compressor
    offset_t totalUncompressedSize = 0;
    bool isVariable = false;
    bool rowCompression = false;
    bool useCompressedPayload = false;
    bool gatherUncompressed = true;
    bool openedCompressor = false;
};

class InplaceIndexCompressor : public CInterfaceOf<IIndexCompressor>
{
public:
    InplaceIndexCompressor(size32_t keyedSize, const CKeyHdr * keyHdr, IHThorIndexWriteArg * helper, const char * _compressionName);

    virtual const char *queryName() const override { return compressionName.str(); }
    virtual CWriteNode *createNode(offset_t _fpos, CKeyHdr *_keyHdr, bool isLeafNode) const override
    {
        if (isLeafNode)
            return new CInplaceLeafWriteNode(_fpos, _keyHdr, ctx);
        else
            return new CInplaceBranchWriteNode(_fpos, _keyHdr, ctx);
    }

    virtual offset_t queryBranchMemorySize() const override
    {
        return ctx.branchMemorySize;
    }
    virtual offset_t queryLeafMemorySize() const override
    {
        return ctx.leafMemorySize;
    }

protected:
    StringAttr compressionName;
    mutable InplaceKeyBuildContext ctx;
};

#endif
