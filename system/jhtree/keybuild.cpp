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

#include "keybuild.hpp"
#include "eclhelper.hpp"
#include "bloom.hpp"
#include "jmisc.hpp"

struct CRC32HTE
{
    CRC32 crc;
    offset_t startBlockPos, endBlockPos;
    offset_t size;

    CRC32HTE() : startBlockPos(0), endBlockPos(0), size(0) { }
    const void *queryStartParam() const
    {
        return (const void *) &startBlockPos;
    }
    const void *queryEndParam() const
    {
        return (const void *) &endBlockPos;
    }
};

class CRC32HT : public SuperHashTableOf<CRC32HTE, offset_t>
{
public:
    CRC32HT(void) : SuperHashTableOf<CRC32HTE, offset_t>() { }
    CRC32HT(unsigned initsize) : SuperHashTableOf<CRC32HTE, offset_t>(initsize) { }
    ~CRC32HT() { _releaseAll(); }
    CRC32HTE *find(offset_t & fp) const { return SuperHashTableOf<CRC32HTE, offset_t>::find(&fp); }
    virtual void onAdd(void *et) { }
    virtual void onRemove(void *et) { }
    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *) fp, sizeof(offset_t), 0);
    }
};

class CRC32StartHT : public CRC32HT
{
public:
    virtual unsigned getHashFromElement(const void *et) const
    {
        return hashc((const unsigned char *) ((const CRC32HTE *) et)->queryStartParam(), sizeof(offset_t), 0);
    }
    virtual const void *getFindParam(const void *et) const { return ((const CRC32HTE *)et)->queryStartParam(); }
    virtual bool matchesFindParam(const void *et, const void *fp, unsigned) const { return *(offset_t *)((const CRC32HTE *)et)->queryStartParam() == *(offset_t *)fp; }
};

class CRC32EndHT : public CRC32HT
{
public:
    virtual unsigned getHashFromElement(const void *et) const
    {
        return hashc((const unsigned char *) ((const CRC32HTE *) et)->queryEndParam(), sizeof(offset_t), 0);
    }
    virtual const void *getFindParam(const void *et) const { return ((const CRC32HTE *)et)->queryEndParam(); }
    virtual bool matchesFindParam(const void *et, const void *fp, unsigned) const { return *(offset_t *)((const CRC32HTE *)et)->queryEndParam() == *(offset_t *)fp; }
};

class CKeyBuilder : public CInterfaceOf<IKeyBuilder>
{
protected:
    unsigned keyValueSize;
    count_t records;
    unsigned levels;
    offset_t nextPos;
    Owned<CKeyHdr> keyHdr;
    CWriteNode *prevLeafNode;
    NodeInfoArray leafInfo;
    Linked<IFileIOStream> out;
    unsigned keyedSize;
    unsigned __int64 sequence;
    CRC32StartHT crcStartPosTable;
    CRC32EndHT crcEndPosTable;
    CRC32 headCRC;
    bool doCrc = false;

private:
    unsigned __int64 duplicateCount;
    __uint64 partitionFieldMask = 0;
    CWriteNode *activeNode = nullptr;
    CBlobWriteNode *activeBlobNode = nullptr;
    CIArrayOf<CWriteNodeBase> pendingNodes;
    IArrayOf<IBloomBuilder> bloomBuilders;
    IArrayOf<IRowHasher> rowHashers;
    bool enforceOrder = true;
    bool isTLK = false;

public:
    CKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, unsigned nodeSize, unsigned _keyedSize, unsigned __int64 _startSequence,  IHThorIndexWriteArg *_helper, bool _enforceOrder, bool _isTLK)
        : out(_out),
          enforceOrder(_enforceOrder),
          isTLK(_isTLK)
    {
        sequence = _startSequence;
        keyHdr.setown(new CKeyHdr());
        keyValueSize = rawSize;
        keyedSize = _keyedSize != (unsigned) -1 ? _keyedSize : rawSize;

        levels = 0;
        records = 0;
        nextPos = nodeSize; // leaving room for header
        prevLeafNode = NULL;

        assertex(nodeSize >= CKeyHdr::getSize());
        assertex(nodeSize <= 0xffff); // stored in a short in the header - we should fix that if/when we restructure header
        if (flags & TRAILING_HEADER_ONLY)
            flags |= USE_TRAILING_HEADER;
        if ((flags & (HTREE_QUICK_COMPRESSED_KEY|HTREE_VARSIZE)) == (HTREE_QUICK_COMPRESSED_KEY|HTREE_VARSIZE))
            flags &= ~HTREE_QUICK_COMPRESSED;  // Quick does not support variable-size rows
        KeyHdr *hdr = keyHdr->getHdrStruct();
        hdr->nodeSize = nodeSize;
        hdr->extsiz = 4096;
        hdr->length = keyValueSize; 
        hdr->ktype = flags;
        hdr->timeid = 0;
        hdr->clstyp = 1;  // IDX_CLOSE
        hdr->maxkbn = nodeSize-sizeof(NodeHdr);
        hdr->maxkbl = hdr->maxkbn;
        hdr->flpntr = sizeof(offset_t);
        hdr->verson = 130; // version from ctree.
        hdr->keypad = ' ';
        hdr->flflvr = 1;
        hdr->flalgn = 8;
        hdr->maxmrk = hdr->nodeSize/4; // always this in ctree.
        hdr->namlen = 255;
        hdr->defrel = 8;
        hdr->hdrseq = 0;
        hdr->fposOffset = 0;
        hdr->fileSize = 0;
        hdr->nodeKeyLength = _keyedSize;
        hdr->version = KEYBUILD_VERSION;
        hdr->blobHead = 0;
        hdr->metadataHead = 0;

        keyHdr->write(out, &headCRC);  // Reserve space for the header - we may seek back and write it properly later

        doCrc = true;
        duplicateCount = 0;
        if (_helper)
        {
            partitionFieldMask = _helper->getPartitionFieldMask();
            auto bloomInfo =_helper->queryBloomInfo();
            if (bloomInfo)
            {
                const RtlRecord &recinfo = _helper->queryDiskRecordSize()->queryRecordAccessor(true);
                while (*bloomInfo)
                {
                    bloomBuilders.append(*createBloomBuilder(*bloomInfo[0]));
                    rowHashers.append(*createRowHasher(recinfo, bloomInfo[0]->getBloomFields()));
                    bloomInfo++;
                }
            }
        }
    }

    ~CKeyBuilder()
    {
        for (;;)
        {
            CRC32HTE *et = (CRC32HTE *)crcEndPosTable.next(NULL);
            if (!et) break;
            crcEndPosTable.removeExact(et);
            delete et;
        }
    }

    void buildLevel(NodeInfoArray &thisLevel, NodeInfoArray &parents)
    {
        unsigned int leaf = 0;
        CWriteNode *node = NULL;
        node = new CWriteNode(nextPos, keyHdr, levels==0);
        nextPos += keyHdr->getNodeSize();
        while (leaf<thisLevel.ordinality())
        {
            CNodeInfo &info = thisLevel.item(leaf);
            if (!node->add(info.pos, info.value, info.size, info.sequence))
            {
                flushNode(node, parents);
                node->Release();
                node = new CWriteNode(nextPos, keyHdr, levels==0);
                nextPos += keyHdr->getNodeSize();
                verifyex(node->add(info.pos, info.value, info.size, info.sequence));
            }
            leaf++;
        }
        flushNode(node, parents);
        flushNode(NULL, parents);
        node->Release();
    }

protected:
    offset_t endLevel(bool close)
    {
        return 0;
    }

    offset_t nextLevel()
    {
        offset_t ret = endLevel(false);
        levels++;
        return 0;
    }

    void writeFileHeader(bool fixHdr, CRC32 *crc)
    {
        if (out)
        {
            out->flush();
            if (keyHdr->getKeyType() & USE_TRAILING_HEADER)
            {
                keyHdr->setPhyRec(nextPos+keyHdr->getNodeSize()-1);
                writeNode(keyHdr, out->tell());  // write a copy at end too, for use on systems that can't seek
            }
            if (!(keyHdr->getKeyType() & TRAILING_HEADER_ONLY))
            {
                out->seek(0, IFSbegin);
                keyHdr->write(out, crc);
            }
            else if (crc)
            {
                *crc = headCRC;
            }
        }
    }

    void writeNode(CWritableKeyNode *node, offset_t _nodePos)
    {
        unsigned nodeSize = keyHdr->getNodeSize();
        if (doCrc)
        {
            CRC32HTE *rollingCrcEntry1 = crcEndPosTable.find(_nodePos); // is start of this block end of another?
            offset_t endPos = _nodePos+nodeSize;
            if (rollingCrcEntry1)
            {
                crcEndPosTable.removeExact(rollingCrcEntry1); // end pos will change
                node->write(out, &rollingCrcEntry1->crc);
                rollingCrcEntry1->size += nodeSize;

                CRC32HTE *rollingCrcEntry2 = crcStartPosTable.find(endPos); // is end of this block, start of another?
                if (rollingCrcEntry2)
                {
                    crcStartPosTable.removeExact(rollingCrcEntry2); // remove completely, will join to rollingCrcEntry1
                    crcEndPosTable.removeExact(rollingCrcEntry2);
                    CRC32Merger crcMerger;
                    crcMerger.addChildCRC(rollingCrcEntry1->size, rollingCrcEntry1->crc.get(), true);
                    crcMerger.addChildCRC(rollingCrcEntry2->size, rollingCrcEntry2->crc.get(), true);

                    rollingCrcEntry1->crc.reset(~crcMerger.get());
                    rollingCrcEntry1->size += rollingCrcEntry2->size;
                    rollingCrcEntry1->endBlockPos = rollingCrcEntry2->endBlockPos;

                    delete rollingCrcEntry2;
                }
                else
                    rollingCrcEntry1->endBlockPos = endPos;
                crcEndPosTable.replace(*rollingCrcEntry1);
            }
            else
            {
                rollingCrcEntry1 = crcStartPosTable.find(endPos); // is end of this node, start of another?
                if (rollingCrcEntry1)
                {
                    crcStartPosTable.removeExact(rollingCrcEntry1); // start pos will change
                    CRC32 crcFirst;
                    node->write(out, &crcFirst);

                    CRC32Merger crcMerger;
                    crcMerger.addChildCRC(nodeSize, crcFirst.get(), true);
                    crcMerger.addChildCRC(rollingCrcEntry1->size, rollingCrcEntry1->crc.get(), true);

                    rollingCrcEntry1->crc.reset(~crcMerger.get());
                    rollingCrcEntry1->startBlockPos = _nodePos;
                    rollingCrcEntry1->size += nodeSize;
                    crcStartPosTable.replace(*rollingCrcEntry1);
                }
                else
                {
                    rollingCrcEntry1 = new CRC32HTE;
                    node->write(out, &rollingCrcEntry1->crc);
                    rollingCrcEntry1->startBlockPos = _nodePos;
                    rollingCrcEntry1->endBlockPos = _nodePos+nodeSize;
                    rollingCrcEntry1->size = nodeSize;
                    crcStartPosTable.replace(*rollingCrcEntry1);
                    crcEndPosTable.replace(*rollingCrcEntry1);
                }
            }
        }
        else
            node->write(out, nullptr);
    }

    void flushNode(CWriteNode *node, NodeInfoArray &nodeInfo)
    {   
        // Messy code, but I don't have the energy to recode right now.
        if (keyHdr->getKeyType() & TRAILING_HEADER_ONLY)
        {
            while (pendingNodes)
            {
                CWriteNodeBase &pending = pendingNodes.item(0);
                if (!prevLeafNode || pending.getFpos() > prevLeafNode->getFpos())
                    break;
                writeNode(&pending, pending.getFpos());
                pendingNodes.remove(0);
            }
        }
        if (prevLeafNode != NULL)
        {
            unsigned __int64 lastSequence = prevLeafNode->getLastSequence();
            if (node)
            {
                prevLeafNode->setRightSib(node->getFpos());
                node->setLeftSib(prevLeafNode->getFpos());
                nodeInfo.append(* new CNodeInfo(prevLeafNode->getFpos(), prevLeafNode->getLastKeyValue(), keyedSize, lastSequence));
            }
            else
                nodeInfo.append(* new CNodeInfo(prevLeafNode->getFpos(), NULL, keyedSize, lastSequence));
            if ((keyHdr->getKeyType() & TRAILING_HEADER_ONLY) != 0 && activeBlobNode && activeBlobNode->getFpos() < prevLeafNode->getFpos())
                pendingNodes.append(*prevLeafNode);
            else
            {
                writeNode(prevLeafNode, prevLeafNode->getFpos());
                prevLeafNode->Release();
            }
            prevLeafNode = NULL;
        }
        if (NULL != node)
        {
            prevLeafNode = node;
            prevLeafNode->Link();
        }
    }

    void buildTree(NodeInfoArray &children)
    {
        if (children.ordinality() != 1 || levels==0) 
        {
            // Note that we always create at least 2 levels as various places assume it
            // Also when building keys for Moxie (bias != 0), need parent level due to assumptions in DKC...
            offset_t offset = nextLevel();
            if (offset)
            {
                ForEachItemIn(idx, children)
                {
                    CNodeInfo &info = children.item(idx);
                    info.pos += offset;
                }
            }
            NodeInfoArray parentInfo;
            buildLevel(children, parentInfo);
            buildTree(parentInfo);
        }
        else
        {
            KeyHdr *hdr = keyHdr->getHdrStruct();
            hdr->nument = records;
            hdr->root = nextPos - hdr->nodeSize;
            hdr->phyrec = hdr->numrec = nextPos-1;
            hdr->maxmrk = hdr->nodeSize/4; // always this in ctree.
            hdr->namlen = 255;
            hdr->defrel = 8;
            hdr->hdrseq = levels;
        }
    }

    void finish(IPropertyTree * metadata, unsigned * fileCrc)
    {
        if (activeBlobNode && (keyHdr->getKeyType() & TRAILING_HEADER_ONLY))
        {
            pendingNodes.append(*activeBlobNode);
            activeBlobNode = nullptr;
        }
        if (NULL != activeNode)
        {
            flushNode(activeNode, leafInfo);
            activeNode->Release();
            activeNode = nullptr;
        }
        if (activeBlobNode)
        {
            writeNode(activeBlobNode, activeBlobNode->getFpos());
            activeBlobNode->Release();
            activeBlobNode = nullptr;
        }
        flushNode(NULL, leafInfo);
        if (keyHdr->getKeyType() & TRAILING_HEADER_ONLY)
        {
            ForEachItemIn(idx, pendingNodes)
            {
                CWriteNodeBase &pending = pendingNodes.item(idx);
                writeNode(&pending, pending.getFpos());
            }
            pendingNodes.kill();
        }
        buildTree(leafInfo);
        if(metadata)
        {
            assertex(strcmp(metadata->queryName(), "metadata") == 0);
            StringBuffer metaXML;
            toXML(metadata, metaXML);
            writeMetadata(metaXML.str(), metaXML.length());
        }
        ForEachItemIn(idx, bloomBuilders)
        {
            IBloomBuilder &bloomBuilder = bloomBuilders.item(idx);
            if (bloomBuilder.valid())
            {
                Owned<const BloomFilter> filter = bloomBuilder.build();
                writeBloomFilter(*filter, rowHashers.item(idx).queryFields());
            }
        }
        keyHdr->getHdrStruct()->partitionFieldMask = partitionFieldMask;
        CRC32 headerCrc;
        writeFileHeader(false, &headerCrc);

        if (fileCrc)
        {
            if (doCrc)
            {
                assertex(crcEndPosTable.count() <= 1);
                CRC32Merger crcMerger;
                crcMerger.addChildCRC(keyHdr->getNodeSize(), headerCrc.get(), true);
                CRC32HTE *rollingCrcEntry = (CRC32HTE *)crcEndPosTable.next(NULL);
                if (rollingCrcEntry)
                    crcMerger.addChildCRC(rollingCrcEntry->size, rollingCrcEntry->crc.get(), true);
                *fileCrc = crcMerger.get();
            }
            else
                *fileCrc = 0;
        }
    }

    void addLeafInfo(CNodeInfo *info)
    {
        leafInfo.append(* info);
    }

    void processKeyData(const char *keyData, offset_t pos, size32_t recsize)
    {
        records++;
        if (NULL == activeNode)
        {
            activeNode = new CWriteNode(nextPos, keyHdr, true);
            nextPos += keyHdr->getNodeSize();
        }
        else if (enforceOrder) // NB: order is indeterminate when build a TLK for a LOCAL index. duplicateCount is not calculated in this case.
        {
            int cmp = memcmp(keyData, activeNode->getLastKeyValue(), keyedSize);
            if (cmp<0)
                throw MakeStringException(JHTREE_KEY_NOT_SORTED, "Unable to build index - dataset not sorted in key order");
            if (cmp==0)
                ++duplicateCount;
        }
        if (!isTLK)
        {
            ForEachItemInRev(idx, bloomBuilders)
            {
                IBloomBuilder &bloomBuilder = bloomBuilders.item(idx);
                IRowHasher &hasher = rowHashers.item(idx);
                if (!bloomBuilder.add(hasher.hash((const byte *) keyData)))
                {
                    bloomBuilders.remove(idx);
                    rowHashers.remove(idx);
                }
            }
        }
        if (!activeNode->add(pos, keyData, recsize, sequence))
        {
            assertex(NULL != activeNode->getLastKeyValue()); // empty and doesn't fit!

            flushNode(activeNode, leafInfo);
            activeNode->Release();
            activeNode = new CWriteNode(nextPos, keyHdr, true);
            nextPos += keyHdr->getNodeSize();
            if (!activeNode->add(pos, keyData, recsize, sequence))
                throw MakeStringException(0, "Key row too large to fit within a key node (uncompressed size=%d, variable=%s, pos=%" I64F "d)", recsize, keyHdr->isVariable()?"true":"false", pos);
        }
        sequence++;
    }

    void newBlobNode()
    {
        if (keyHdr->getHdrStruct()->blobHead == 0)
            keyHdr->getHdrStruct()->blobHead = nextPos;         
        CBlobWriteNode *prevBlobNode = activeBlobNode;
        activeBlobNode = new CBlobWriteNode(nextPos, keyHdr);
        nextPos += keyHdr->getNodeSize();
        if (prevBlobNode)
        {
            activeBlobNode->setLeftSib(prevBlobNode->getFpos());
            prevBlobNode->setRightSib(activeBlobNode->getFpos());
            if (keyHdr->getKeyType() & TRAILING_HEADER_ONLY)
                pendingNodes.append(*prevBlobNode);
            else
            {
                writeNode(prevBlobNode, prevBlobNode->getFpos());
                delete(prevBlobNode);
            }
        }
    }

    virtual unsigned __int64 createBlob(size32_t size, const char * ptr)
    {
        if (!size)
            return 0;
        if (NULL == activeBlobNode)
            newBlobNode();
        unsigned __int64 head = activeBlobNode->add(ptr, size);
        if (!head)
        {
            newBlobNode();
            head = activeBlobNode->add(ptr, size);
            assertex(head);
        }
        while (size)
        {
            newBlobNode();
            unsigned __int64 chunkhead = activeBlobNode->add(ptr, size);
            assertex(chunkhead);
        }
        return head;
    }

    unsigned __int64 getDuplicateCount() { return duplicateCount; };

protected:
    void writeMetadata(char const * data, size32_t size)
    {
        assertex(keyHdr->getHdrStruct()->metadataHead == 0);
        assertex(size);
        keyHdr->getHdrStruct()->metadataHead = nextPos;
        Owned<CMetadataWriteNode> prevNode;
        while(size)
        {
            Owned<CMetadataWriteNode> node(new CMetadataWriteNode(nextPos, keyHdr));
            nextPos += keyHdr->getNodeSize();
            size32_t written = node->set(data, size);
            assertex(written);
            if(prevNode)
            {
                node->setLeftSib(prevNode->getFpos());
                prevNode->setRightSib(node->getFpos());
                writeNode(prevNode, prevNode->getFpos());
            }
            prevNode.setown(node.getClear());
        }
        writeNode(prevNode, prevNode->getFpos());
    }

    void writeBloomFilter(const BloomFilter &filter, __uint64 fields)
    {
        size32_t size = filter.queryTableSize();
        if (!size)
            return;
        auto prevBloom = keyHdr->getHdrStruct()->bloomHead;
        keyHdr->getHdrStruct()->bloomHead = nextPos;
        Owned<CBloomFilterWriteNode> prevNode;
        Owned<CBloomFilterWriteNode> node(new CBloomFilterWriteNode(nextPos, keyHdr));
        // Table info is serialized into first page. Note that we assume that it fits (would need to have a crazy-small page size for that to not be true)
        node->put8(prevBloom);
        node->put4(filter.queryNumHashes());
        node->put8(fields);
        node->put4(size);
        const byte *data = filter.queryTable();
        while (size)
        {
            nextPos += keyHdr->getNodeSize();
            size32_t written = node->set(data, size);
            assertex(written);
            if(prevNode)
            {
                node->setLeftSib(prevNode->getFpos());
                prevNode->setRightSib(node->getFpos());
                writeNode(prevNode, prevNode->getFpos());
            }
            prevNode.setown(node.getClear());
            if (!size)
                break;
            node.setown(new CBloomFilterWriteNode(nextPos, keyHdr));
        }
        writeNode(prevNode, prevNode->getFpos());
    }
};

extern jhtree_decl IKeyBuilder *createKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, unsigned nodeSize, unsigned keyFieldSize, unsigned __int64 startSequence, IHThorIndexWriteArg *helper, bool enforceOrder, bool isTLK)
{
    return new CKeyBuilder(_out, flags, rawSize, nodeSize, keyFieldSize, startSequence, helper, enforceOrder, isTLK);
}


class PartNodeInfo : public CInterface
{
public:
    PartNodeInfo(unsigned _part, NodeInfoArray & _nodes)
    {
        part = _part;
        ForEachItemIn(idx, _nodes)
            nodes.append(OLINK(_nodes.item(idx)));
    }

public:
    unsigned part;
    NodeInfoArray nodes;
};

int compareParts(CInterface * const * _left, CInterface * const * _right)
{
    PartNodeInfo * left = (PartNodeInfo *)*_left;
    PartNodeInfo * right = (PartNodeInfo *)*_right;
    return (int)(left->part - right->part);
}

extern jhtree_decl bool checkReservedMetadataName(const char *name)
{
    return strsame(name, "_nodeSize") || strsame(name, "_noSeek") || strsame(name, "_useTrailingHeader");
}
