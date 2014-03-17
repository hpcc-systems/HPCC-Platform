/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    ~CRC32HT() { CRC32HT::kill(); }
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

class CKeyBuilderBase : public CInterface
{
protected:
    unsigned rawSize;
    count_t records;
    unsigned levels;
    offset_t nextPos;
    Owned<CKeyHdr> keyHdr;
    CWriteNode *prevLeafNode;
    NodeInfoArray leafInfo;
    Linked<IFileIOStream> out;
    offset_t fileSize;
    unsigned keyedSize;
    unsigned __int64 sequence;
    CRC32StartHT crcStartPosTable;
    CRC32EndHT crcEndPosTable;
    bool doCrc, legacyCompat;

public:
    IMPLEMENT_IINTERFACE;

    CKeyBuilderBase(IFileIOStream *_out, unsigned flags, unsigned _rawSize, offset_t _fileSize, unsigned nodeSize, unsigned _keyedSize, unsigned __int64 _startSequence, bool _legacyCompat)
        : out(_out), legacyCompat(_legacyCompat)
    {
        doCrc = false;
        sequence = _startSequence;
        keyHdr.setown(new CKeyHdr());
        rawSize = _rawSize;
        keyedSize = _keyedSize != (unsigned) -1 ? _keyedSize : rawSize;

        fileSize = _fileSize;

        levels = 0;
        records = 0;
        nextPos = nodeSize; // leaving room for header
        prevLeafNode = NULL;

        assertex(nodeSize >= CKeyHdr::getSize());
        assertex(nodeSize <= 0xffff); // stored in a short in the header - we should fix that if/when we restructure header 
        KeyHdr *hdr = keyHdr->getHdrStruct();
        hdr->nodeSize = nodeSize;
        hdr->extsiz = 4096;
        hdr->length = 0; // fill in at end
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
        hdr->fileSize = fileSize;
        hdr->nodeKeyLength = _keyedSize;
        hdr->version = KEYBUILD_VERSION;
        hdr->blobHead = 0;
        hdr->metadataHead = 0;

        keyHdr->write(out);  // Reserve space for the header - we'll seek back and write it properly later
    }

    CKeyBuilderBase(CKeyHdr * chdr)
    {
        levels = 0;
        records = 0;
        prevLeafNode = NULL;

        keyHdr.set(chdr);
        KeyHdr *hdr = keyHdr->getHdrStruct();
        records = hdr->nument;
        nextPos = hdr->nodeSize; // leaving room for header
        rawSize = keyHdr->getMaxKeyLength();
        keyedSize = keyHdr->getNodeKeyLength();
    }

    ~CKeyBuilderBase()
    {
        loop
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

    void writeFileHeader(CRC32 *crc)
    {
        if (out)
        {
            out->flush();
            out->seek(0, IFSbegin);

#define LEGACY_DEFAULT_MAXLENGTH 4096
            /* For legacy compatibility, where no explicit maxlength was provided.
             * 1) Check if < default and set header to default - maintaining legacy key build/read behaviour.
             * 2) Issue warning if runtime max exceeded default - key will not be compatible with legacy system, without MAXLENGTHs being added.
             */
            if (legacyCompat && rawSize==LEGACY_DEFAULT_MAXLENGTH)
            {
                if (keyHdr->getMaxKeyLength() < LEGACY_DEFAULT_MAXLENGTH)
                    keyHdr->setMaxKeyLength(LEGACY_DEFAULT_MAXLENGTH);
                else
                    WARNLOG("Key created with legacy compatibility set, but key+payload size exceeds legacy default of 4096. Max size is: %d", keyHdr->getMaxKeyLength());
            }

            keyHdr->write(out, crc);
        }
    }

    void writeNode(CWriteNodeBase *node)
    {
        unsigned nodeSize = keyHdr->getNodeSize();
        if (doCrc)
        {
            offset_t nodePos = node->getFpos();
            CRC32HTE *rollingCrcEntry1 = crcEndPosTable.find(nodePos); // is start of this block end of another?
            nodePos += nodeSize; // update to endpos
            if (rollingCrcEntry1)
            {
                crcEndPosTable.removeExact(rollingCrcEntry1); // end pos will change
                node->write(out, &rollingCrcEntry1->crc);
                rollingCrcEntry1->size += nodeSize;

                CRC32HTE *rollingCrcEntry2 = crcStartPosTable.find(nodePos); // is end of this block, start of another?
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
                    rollingCrcEntry1->endBlockPos = nodePos;
                crcEndPosTable.replace(*rollingCrcEntry1);
            }
            else
            {
                rollingCrcEntry1 = crcStartPosTable.find(nodePos); // is end of this node, start of another?
                if (rollingCrcEntry1)
                {
                    crcStartPosTable.removeExact(rollingCrcEntry1); // start pos will change
                    CRC32 crcFirst;
                    node->write(out, &crcFirst);

                    CRC32Merger crcMerger;
                    crcMerger.addChildCRC(nodeSize, crcFirst.get(), true);
                    crcMerger.addChildCRC(rollingCrcEntry1->size, rollingCrcEntry1->crc.get(), true);

                    rollingCrcEntry1->crc.reset(~crcMerger.get());
                    rollingCrcEntry1->startBlockPos = node->getFpos();
                    rollingCrcEntry1->size += nodeSize;
                    crcStartPosTable.replace(*rollingCrcEntry1);
                }
                else
                {
                    rollingCrcEntry1 = new CRC32HTE;
                    node->write(out, &rollingCrcEntry1->crc);
                    rollingCrcEntry1->startBlockPos = node->getFpos();
                    rollingCrcEntry1->endBlockPos = node->getFpos()+nodeSize;
                    rollingCrcEntry1->size = nodeSize;
                    crcStartPosTable.replace(*rollingCrcEntry1);
                    crcEndPosTable.replace(*rollingCrcEntry1);
                }
            }
        }
        else
            node->write(out);
    }

    void flushNode(CWriteNode *node, NodeInfoArray &nodeInfo)
    {   
        // Messy code, but I don't have the energy to recode right now.
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
            writeNode(prevLeafNode);
            prevLeafNode->Release();
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
};

class CKeyBuilder : public CKeyBuilderBase, implements IKeyBuilder
{
private:
    CWriteNode *activeNode;
    CBlobWriteNode *activeBlobNode;

public:
    IMPLEMENT_IINTERFACE;

    CKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, offset_t fileSize, unsigned nodeSize, unsigned keyedSize, unsigned __int64 startSequence, bool legacyCompat)
        : CKeyBuilderBase(_out, flags, rawSize, fileSize, nodeSize, keyedSize, startSequence, legacyCompat)
    {
        doCrc = true;
        activeNode = NULL;
        activeBlobNode = NULL;
    }

public:
    void finish(unsigned *fileCrc)
    {
        finish(NULL, fileCrc);
    }

    void finish(IPropertyTree * metadata, unsigned * fileCrc)
    {
        if (NULL != activeNode)
        {
            flushNode(activeNode, leafInfo);
            activeNode->Release();
        }
        if (NULL != activeBlobNode)
        {
            writeNode(activeBlobNode);
            activeBlobNode->Release();
        }
        flushNode(NULL, leafInfo);
        buildTree(leafInfo);
        if(metadata)
        {
            assertex(strcmp(metadata->queryName(), "metadata") == 0);
            StringBuffer metaXML;
            toXML(metadata, metaXML);
            writeMetadata(metaXML.str(), metaXML.length());
        }
        CRC32 headerCrc;
        writeFileHeader(&headerCrc);

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
        if (!activeNode->add(pos, keyData, recsize, sequence))
        {
            assertex(NULL != activeNode->getLastKeyValue()); // empty and doesn't fit!

            flushNode(activeNode, leafInfo);
            activeNode->Release();
            activeNode = new CWriteNode(nextPos, keyHdr, true);
            nextPos += keyHdr->getNodeSize();
            if (!activeNode->add(pos, keyData, recsize, sequence))
                throw MakeStringException(0, "Key row too large to fit within a key node (uncompressed size=%d, variable=%s, pos=%"I64F"d)", recsize, keyHdr->isVariable()?"true":"false", pos);
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
            writeNode(prevBlobNode);
            delete(prevBlobNode);
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
                writeNode(prevNode);
            }
            prevNode.setown(node.getLink());
        }
        writeNode(prevNode);
    }
};

extern jhtree_decl IKeyBuilder *createKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, offset_t fileSize, unsigned nodeSize, unsigned keyFieldSize, unsigned __int64 startSequence, bool legacyCompat)
{
    return new CKeyBuilder(_out, flags, rawSize, fileSize, nodeSize, keyFieldSize, startSequence, legacyCompat);
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

int compareParts(CInterface * * _left, CInterface * * _right)
{
    PartNodeInfo * left = (PartNodeInfo *)*_left;
    PartNodeInfo * right = (PartNodeInfo *)*_right;
    return (int)(left->part - right->part);
}

class CKeyDesprayer : public CKeyBuilderBase, public IKeyDesprayer
{
public:
    CKeyDesprayer(CKeyHdr * _hdr, IFileIOStream * _out) : CKeyBuilderBase(_hdr)
    {
        out.set(_out);
        nextPos = out->tell();
    }
    IMPLEMENT_IINTERFACE

    virtual void addPart(unsigned idx, offset_t numRecords, NodeInfoArray & nodes)
    {
        records += numRecords;
        parts.append(* new PartNodeInfo(idx, nodes));
    }

    virtual void finish()
    {
        levels = 1; // already processed one level of index....
        parts.sort(compareParts);
        ForEachItemIn(idx, parts)
        {
            NodeInfoArray & nodes = parts.item(idx).nodes;
            ForEachItemIn(idx2, nodes)
                leafInfo.append(OLINK(nodes.item(idx2)));
        }
        buildTree(leafInfo);
        writeFileHeader(NULL);
    }

protected:
    CIArrayOf<PartNodeInfo> parts;
};


extern jhtree_decl IKeyDesprayer * createKeyDesprayer(IFile * in, IFileIOStream * out)
{
    Owned<IFileIO> io = in->open(IFOread);
    MemoryAttr buffer(sizeof(KeyHdr));
    io->read(0, sizeof(KeyHdr), (void *)buffer.get());

    Owned<CKeyHdr> hdr = new CKeyHdr;
    hdr->load(*(KeyHdr *)buffer.get());
    hdr->getHdrStruct()->nument = 0;
    return new CKeyDesprayer(hdr, out);
}
