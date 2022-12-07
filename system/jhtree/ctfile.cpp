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

#include "platform.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef __linux__
#include <alloca.h>
#endif

#include "jmisc.hpp"
#include "hlzw.h"

#include "ctfile.hpp"
#include "jstats.h"

void SwapBigEndian(KeyHdr &hdr)
{
    _WINREV(hdr.phyrec);
    _WINREV(hdr.delstk);
    _WINREV(hdr.numrec);
    _WINREV(hdr.reshdr);
    _WINREV(hdr.lstmbr);
    _WINREV(hdr.sernum);
    _WINREV(hdr.nument);
    _WINREV(hdr.root);
    _WINREV(hdr.fileid);
    _WINREV(hdr.servid);
    _WINREV(hdr.verson);
    _WINREV(hdr.nodeSize);
    _WINREV(hdr.extsiz);
    _WINREV(hdr.flmode);
    _WINREV(hdr.maxkbl);
    _WINREV(hdr.maxkbn);
//    _WINREV(hdr.updflg);
//    _WINREV(hdr.autodup);
//    _WINREV(hdr.deltyp);
//    _WINREV(hdr.keypad);
//    _WINREV(hdr.flflvr);
//    _WINREV(hdr.flalgn);
//    _WINREV(hdr.flpntr);
    _WINREV(hdr.clstyp);
    _WINREV(hdr.length);
    _WINREV(hdr.nmem);
    _WINREV(hdr.kmem);
    _WINREV(hdr.lanchr);
    _WINREV(hdr.supid);
    _WINREV(hdr.hdrpos);
    _WINREV(hdr.sihdr);
    _WINREV(hdr.timeid);
    _WINREV(hdr.suptyp);
    _WINREV(hdr.maxmrk);
    _WINREV(hdr.namlen);
    _WINREV(hdr.xflmod);
    _WINREV(hdr.defrel);
    _WINREV(hdr.hghtrn);
    _WINREV(hdr.hdrseq);
    _WINREV(hdr.tstamp);
    _WINREV(hdr.rs3[0]);
    _WINREV(hdr.rs3[1]);
    _WINREV(hdr.rs3[2]);
    _WINREV(hdr.fposOffset);
    _WINREV(hdr.fileSize);
    _WINREV(hdr.nodeKeyLength);
    _WINREV(hdr.version);
    _WINREV(hdr.blobHead);
    _WINREV(hdr.metadataHead);
    _WINREV(hdr.bloomHead);
    _WINREV(hdr.partitionFieldMask);
    _WINREV(hdr.firstLeaf);
}

inline void SwapBigEndian(NodeHdr &hdr)
{
    _WINREV(hdr.rightSib);
    _WINREV(hdr.leftSib);
    _WINREV(hdr.numKeys);
    _WINREV(hdr.keyBytes);
    _WINREV(hdr.crc32);
//   _WINREV(hdr.subType);
//   _WINREV(hdr.nodeType);
}

extern bool isCompressedIndex(const char *filename)
{
    OwnedIFile file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    unsigned __int64 size = file->size();
    if (size)
    {
        KeyHdr hdr;
        if (io->read(0, sizeof(hdr), &hdr) == sizeof(hdr))
        {
            SwapBigEndian(hdr);
            if (hdr.nodeSize && size % hdr.nodeSize == 0 && hdr.ktype & (HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY))
            {
#ifdef _DEBUG
                //In debug mode always use the trailing header if it is available to ensure that code path is tested
                if (hdr.ktype & USE_TRAILING_HEADER)
#else
                if (hdr.ktype & TRAILING_HEADER_ONLY)
#endif
                {
                    if (io->read(size-hdr.nodeSize, sizeof(hdr), &hdr) != sizeof(hdr))
                        return false;
                    SwapBigEndian(hdr);
                }
                if (hdr.root && hdr.root % hdr.nodeSize == 0 && hdr.phyrec == size-1 && hdr.ktype & (HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY))
                {
                    NodeHdr root;
                    if (io->read(hdr.root, sizeof(root), &root) == sizeof(root))
                    {
                        SwapBigEndian(root);
                        return root.leftSib==0 && root.rightSib==0;
                    }
                }
            }
        }
    }
    return false;
}

extern jhtree_decl bool isIndexFile(IFile *file)
{
    try
    {
        offset_t size = file->size();
        if (((offset_t)-1 == size) || (size <= sizeof(KeyHdr)))
            return false;
        Owned<IFileIO> io = file->open(IFOread);
        if (!io)
            return false;
        KeyHdr hdr;
        if (io->read(0, sizeof(hdr), &hdr) != sizeof(hdr))
            return false;
        SwapBigEndian(hdr);
        if (hdr.nodeSize && (size % hdr.nodeSize == 0))
        {
#ifdef _DEBUG
            //In debug mode always use the trailing header if it is available to ensure that code path is tested
            if (hdr.ktype & USE_TRAILING_HEADER)
#else
            if (hdr.ktype & TRAILING_HEADER_ONLY)
#endif
            {
                if (io->read(size-hdr.nodeSize, sizeof(hdr), &hdr) != sizeof(hdr))
                    return false;
                SwapBigEndian(hdr);

            }
            if (!hdr.root || !hdr.nodeSize || !hdr.root || size % hdr.nodeSize ||  hdr.phyrec != size-1 || hdr.root % hdr.nodeSize || hdr.root >= size)
                return false;
            return true;    // Reasonable heuristic...
        }
    }
    catch (IException *E)
    {
        E->Release();
    }
    return false;
}

extern jhtree_decl bool isIndexFile(const char *fileName)
{
    OwnedIFile iFile = createIFile(fileName);
    if (!iFile)
        return false;
    return isIndexFile(iFile);
}


// CKeyHdr
CKeyHdr::CKeyHdr()
{
    memset(&hdr, 0, sizeof(hdr));
}

void CKeyHdr::load(KeyHdr &_hdr)
{
    memcpy(&hdr, &_hdr, sizeof(hdr));
    SwapBigEndian(hdr);

    if (0xffff != hdr.version && KEYBUILD_VERSION < hdr.version)
        throw MakeKeyException(KeyExcpt_IncompatVersion, "This build is compatible with key versions <= %u. Key is version %u", KEYBUILD_VERSION, (unsigned) hdr.version);
}

unsigned int CKeyHdr::getMaxKeyLength() 
{
    return hdr.length; 
}

bool CKeyHdr::isVariable() 
{
    return (hdr.ktype & HTREE_VARSIZE) == HTREE_VARSIZE; 
}

void CWriteKeyHdr::write(IFileIOStream *out, CRC32 *crc)
{
    unsigned nodeSize = hdr.nodeSize;
    MemoryAttr ma;
    byte *buf = (byte *) ma.allocate(nodeSize); 
    memcpy(buf, &hdr, sizeof(hdr));
    memset(buf+sizeof(hdr), 0xff, nodeSize-sizeof(hdr));
    SwapBigEndian(*(KeyHdr*) buf);
    out->write(nodeSize, buf);
    if (crc)
        crc->tally(nodeSize, buf);
}

//=========================================================================================================

CNodeBase::CNodeBase()
{
    keyHdr = NULL;
    fpos = 0;
}

void CNodeBase::init(CKeyHdr *_keyHdr, offset_t _fpos)
{
    _keyHdr->Link();
    keyHdr = _keyHdr;
    memset(&hdr, 0, sizeof(hdr));
    fpos = _fpos;
}

CNodeBase::~CNodeBase()
{
    ::Release(keyHdr);
}

const char *CNodeBase::getNodeTypeName() const
{
    switch (hdr.nodeType)
    {
    case NodeBranch: return "NodeBranch";
    case NodeLeaf: return "NodeLeaf";
    case NodeBlob: return "NodeBlob";
    case NodeMeta: return "NodeMeta";
    case NodeBloom: return "NodeBloom";
    default: return "Unknown";
    }

}


//=========================================================================================================

CWriteNodeBase::CWriteNodeBase(offset_t _fpos, CKeyHdr *_keyHdr) 
{
    CNodeBase::init(_keyHdr, _fpos);
    unsigned nodeSize = keyHdr->getNodeSize();
    nodeBuf = (char *) malloc(nodeSize);
    memset(nodeBuf, 0, nodeSize);
    maxBytes = keyHdr->getMaxNodeBytes();
    keyPtr = nodeBuf + sizeof(hdr);
}

CWriteNodeBase::~CWriteNodeBase()
{
    free(nodeBuf);
}

void CWriteNodeBase::writeHdr()
{
    hdr.crc32 = crc32(nodeBuf+sizeof(hdr), hdr.keyBytes, 0);
    memcpy(nodeBuf, &hdr, sizeof(hdr));
    SwapBigEndian(*(NodeHdr *) nodeBuf);
}

void CWriteNodeBase::write(IFileIOStream *out, CRC32 *crc)
{
    if (isBlob() || (isLeaf() && (keyHdr->getKeyType() & HTREE_COMPRESSED_KEY)))
        lzwcomp.close();
    assertex(hdr.keyBytes<=maxBytes);
    writeHdr();
    out->seek(getFpos(), IFSbegin);
    out->write(keyHdr->getNodeSize(), nodeBuf);
    if (crc)
        crc->tally(keyHdr->getNodeSize(), nodeBuf);
}

//=========================================================================================================

CWriteNode::CWriteNode(offset_t _fpos, CKeyHdr *_keyHdr, bool isLeafNode) : CWriteNodeBase(_fpos, _keyHdr)
{
    keyLen = keyHdr->getMaxKeyLength();
    hdr.nodeType = isLeafNode ? NodeLeaf : NodeBranch;
    if (!isLeafNode)
    {
        keyLen = keyHdr->getNodeKeyLength();
    }
    lastKeyValue = (char *) malloc(keyLen);
    lastSequence = 0;
}

CWriteNode::~CWriteNode()
{
    free(lastKeyValue);
}

bool CWriteNode::add(offset_t pos, const void *indata, size32_t insize, unsigned __int64 sequence)
{
    char keyType = keyHdr->getKeyType();
    if (isLeaf() && !hdr.numKeys)
    {
        unsigned __int64 rsequence = sequence;
        _WINREV(rsequence);
        memcpy(keyPtr, &rsequence, sizeof(rsequence));
        keyPtr += sizeof(rsequence);
        hdr.keyBytes += sizeof(rsequence);
    }
    if (isLeaf() && (keyType & HTREE_COMPRESSED_KEY))
    {
        if (0 == hdr.numKeys)
            lzwcomp.open(keyPtr, maxBytes-hdr.keyBytes, keyHdr->isVariable(), (keyType&HTREE_QUICK_COMPRESSED_KEY)==HTREE_QUICK_COMPRESSED_KEY);
        if (0xffff == hdr.numKeys || 0 == lzwcomp.writekey(pos, (const char *)indata, insize, sequence))
        {
            lzwcomp.close();
            return false;
        }
        hdr.keyBytes = lzwcomp.buflen() + sizeof(unsigned __int64); // rsequence added above
    }
    else
    {
        if (0xffff == hdr.numKeys)
            return false;
        bool lastnode = false;
        assertex (indata);
        //assertex(insize==keyLen);
        const void *data;
        int size;

        char *result = (char *) alloca(insize+1);  // Gets bigger if no leading common!
        size = compressValue((const char *) indata, insize, result);
        data = result;

        int bytes = sizeof(pos) + size;
        if (keyHdr->isVariable())
            bytes += sizeof(KEYRECSIZE_T);
        if (hdr.keyBytes + bytes >= maxBytes)    // probably could be '>' (loses byte)
            return false;

        if (keyHdr->isVariable() && isLeaf())
        {
            KEYRECSIZE_T _insize = insize;
            _WINREV(_insize);
            memcpy(keyPtr, &_insize, sizeof(_insize));
            keyPtr += sizeof(_insize);
        }
        _WINREV(pos);
        memcpy(keyPtr, &pos, sizeof(pos));
        keyPtr += sizeof(pos);
        memcpy(keyPtr, data, size);
        keyPtr += size;
        hdr.keyBytes += bytes;
    }

    if (insize>keyLen)
        throw MakeStringException(0, "key+payload (%u) exceeds max length (%u)", insize, keyLen);
    memcpy(lastKeyValue, indata, insize);
    lastSequence = sequence;
    hdr.numKeys++;
    return true;
}

size32_t CWriteNode::compressValue(const char *keyData, size32_t size, char *result)
{
    unsigned int pack = 0;
    if (hdr.numKeys)
    {
        for (; pack<size && pack<255; pack++)
        {
            if (keyData[pack] != lastKeyValue[pack])
                break;
        }
    }

    result[0] = pack;
    memcpy(&result[1], keyData+pack, size-pack);
    return size-pack+1;
}

//=========================================================================================================

CBlobWriteNode::CBlobWriteNode(offset_t _fpos, CKeyHdr *_keyHdr) : CWriteNodeBase(_fpos, _keyHdr)
{
    hdr.nodeType = NodeBlob;
    lzwcomp.openBlob(keyPtr, maxBytes);
}

CBlobWriteNode::~CBlobWriteNode()
{
}

unsigned __int64 CBlobWriteNode::makeBlobId(offset_t nodepos, unsigned offset)
{
    assertex(nodepos != 0);
    assertex((nodepos & I64C(0xffff000000000000)) == 0);
    assertex((offset & 0xf) == 0);
    return (((unsigned __int64) offset) << 44) | nodepos;
}

unsigned __int64 CBlobWriteNode::add(const char * &data, size32_t &size)
{
    unsigned __int64 ret = makeBlobId(getFpos(), lzwcomp.getCurrentOffset());
    unsigned written = lzwcomp.writeBlob(data, size);
    if (written)
    {
        size -= written;
        data = (const char *) data + written;
        return ret;
    }
    else
        return 0;
}

//=========================================================================================================

CMetadataWriteNode::CMetadataWriteNode(offset_t _fpos, CKeyHdr *_keyHdr) : CWriteNodeBase(_fpos, _keyHdr)
{
    hdr.nodeType = NodeMeta;
}

size32_t CMetadataWriteNode::set(const char * &data, size32_t &size)
{
    unsigned short written = ((size > (maxBytes-sizeof(unsigned short))) ? (maxBytes-sizeof(unsigned short)) : size);
    _WINCPYREV2(keyPtr, &written);
    memcpy(keyPtr+sizeof(unsigned short), data, written);
    data += written;
    size -= written;
    return written;
}

//=========================================================================================================

CBloomFilterWriteNode::CBloomFilterWriteNode(offset_t _fpos, CKeyHdr *_keyHdr) : CWriteNodeBase(_fpos, _keyHdr)
{
    hdr.nodeType = NodeBloom;
}

size32_t CBloomFilterWriteNode::set(const byte * &data, size32_t &size)
{
    unsigned short written;
    _WINCPYREV2(&written, keyPtr);

    unsigned short writtenThisTime = ((size > (maxBytes-written-sizeof(unsigned short))) ? (maxBytes-written-sizeof(unsigned short)) : size);
    memcpy(keyPtr+sizeof(unsigned short)+written, data, writtenThisTime);
    data += writtenThisTime;
    size -= writtenThisTime;
    written += writtenThisTime;
    _WINCPYREV2(keyPtr, &written);
    return writtenThisTime;
}

void CBloomFilterWriteNode::put4(unsigned val)
{
    assert(sizeof(val)==4);
    unsigned short written;
    _WINCPYREV2(&written, keyPtr);

    assertex(written + sizeof(val) + sizeof(unsigned short) <= maxBytes);
    _WINCPYREV4(keyPtr+sizeof(unsigned short)+written, &val);
    written += sizeof(val);
    _WINCPYREV2(keyPtr, &written);
}

void CBloomFilterWriteNode::put8(__int64 val)
{
    assert(sizeof(val)==8);
    unsigned short written;
    _WINCPYREV2(&written, keyPtr);

    assertex(written + sizeof(val) + sizeof(unsigned short) <= maxBytes);
    _WINCPYREV8(keyPtr+sizeof(unsigned short)+written, &val);
    written += sizeof(val);
    _WINCPYREV2(keyPtr, &written);
}

//=========================================================================================================

void CJHTreeNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CNodeBase::init(_keyHdr, _fpos);
    unpack(rawData, needCopy);
}

void CJHTreeNode::dump(FILE *out, int length, unsigned rowCount, bool raw) const
{
    if (!raw)
        fprintf(out, "Node dump: fpos(%" I64F "d) type %s\n", getFpos(), getNodeTypeName());
}

CJHTreeNode::~CJHTreeNode()
{
    releaseMem(keyBuf, expandedSize);
}

void CJHTreeNode::releaseMem(void *togo, size32_t len)
{
    free(togo);
}

void *CJHTreeNode::allocMem(size32_t len)
{
    char *ret = (char *) malloc(len);
    if (!ret)
    {
        Owned<IException> E = MakeStringException(MSGAUD_operator,0, "Out of memory in CJHTreeNode::allocMem, requesting %d bytes", len);
        EXCLOG(E);
        if (flushJHtreeCacheOnOOM)
        {
            clearKeyStoreCache(false);
            ret = (char *) malloc(len);
        }
        if (!ret)
            throw E.getClear();
    }
    return ret;
}

char *CJHTreeNode::expandKeys(const void *src,size32_t &retsize)
{
    Owned<IExpander> exp = createLZWExpander(true);
    int len=exp->init(src);
    if (len==0)
    {
        retsize = 0;
        return NULL;
    }
    char *outkeys=(char *) allocMem(len);
    exp->expand(outkeys);
    retsize = len;
    return outkeys;
}

void CJHTreeNode::unpack(const void *node, bool needCopy)
{
    assertex(!keyBuf && (expandedSize == 0));
    memcpy(&hdr, node, sizeof(hdr));
    SwapBigEndian(hdr);
    __int64 maxsib = keyHdr->getHdrStruct()->phyrec;
    if (!hdr.isValid(keyHdr->getNodeSize()))
    {
        PROGLOG("hdr.nodeType=%d",(int)hdr.nodeType);
        PROGLOG("hdr.rightSib=%" I64F "d",hdr.rightSib);
        PROGLOG("hdr.leftSib=%" I64F "d",hdr.leftSib);
        PROGLOG("maxsib=%" I64F "d",maxsib);
        PROGLOG("nodeSize=%d", keyHdr->getNodeSize());
        PROGLOG("keyBytes=%d",(int)hdr.keyBytes);
        PrintStackReport();
        throw MakeStringException(0, "Htree: Corrupt key node detected");
    }
    const char *data = ((const char *) node) + sizeof(hdr);
    if (hdr.crc32)
    {
        unsigned crc = crc32(data, hdr.keyBytes, 0);
        if (hdr.crc32 != crc)
            throw MakeStringException(0, "CRC error on key node");
    }
}

//------------------------------

void CJHSearchNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    keyLen = _keyHdr->getMaxKeyLength();
    keyCompareLen = _keyHdr->getNodeKeyLength();
    CJHTreeNode::load(_keyHdr, rawData, _fpos, needCopy);
}


int CJHSearchNode::locateGE(const char * search, unsigned minIndex) const
{
#ifdef TIME_NODE_SEARCH
    CCycleTimer timer;
#endif
    unsigned int a = minIndex;
    int b = getNumKeys();
    // first search for first GTE entry (result in b(<),a(>=))
    while ((int)a<b)
    {
        int i = a+(b-a)/2;
        int rc = compareValueAt(search, i);
        if (rc>0)
            a = i+1;
        else
            b = i;
    }

#ifdef TIME_NODE_SEARCH
    unsigned __int64 elapsed = timer.elapsedCycles();
    if (isBranch())
        branchSearchCycles += elapsed;
    else
        leafSearchCycles += elapsed;
#endif
    return a;
}


int CJHSearchNode::locateGT(const char * search, unsigned minIndex) const
{
    unsigned int a = minIndex;
    int b = getNumKeys();
    // Locate first record greater than src
    while ((int)a<b)
    {
        //MORE: Note sure why the index is subtly different to the GTE version
        //I suspect no good reason, and may mess up cache locality.
        int i = a+(b+1-a)/2;
        int rc = compareValueAt(search, i-1);
        if (rc>=0)
            a = i;
        else
            b = i-1;
    }
    return a;
}

void CJHSearchNode::unpack(const void *node, bool needCopy)
{
    CJHTreeNode::unpack(node, needCopy);
    if (hdr.nodeType == NodeBranch)
        keyLen = keyHdr->getNodeKeyLength();
    keyRecLen = keyLen + sizeof(offset_t);
    char keyType = keyHdr->getKeyType();
    const char *keys = ((const char *) node) + sizeof(hdr);
    if (hdr.nodeType==NodeLeaf)
    {
        firstSequence = *(unsigned __int64 *) keys;
        keys += sizeof(unsigned __int64);
        _WINREV(firstSequence);
    }
    if (hdr.nodeType==NodeLeaf && (keyType & HTREE_COMPRESSED_KEY))
    {
        expandedSize = keyHdr->getNodeSize();
        if ((keyType & (HTREE_QUICK_COMPRESSED_KEY|HTREE_VARSIZE))==HTREE_QUICK_COMPRESSED_KEY)
            keyBuf = nullptr;
        else
            keyBuf = expandKeys(keys, expandedSize);
    }
    else
    {
        if (hdr.numKeys) 
        {
            bool handleVariable = keyHdr->isVariable() && isLeaf();
            KEYRECSIZE_T workRecLen;
            MemoryBuffer keyBufMb;
            const char *source = keys;
            char *target;
            // do first row
            if (handleVariable) {
                memcpy(&workRecLen, source, sizeof(workRecLen));
                _WINREV(workRecLen);
                size32_t tmpSz = sizeof(workRecLen) + sizeof(offset_t);
                target = (char *)keyBufMb.reserve(tmpSz+workRecLen);
                memcpy(target, source, tmpSz);
                source += tmpSz;
                target += tmpSz;
            }
            else {
                target = (char *)keyBufMb.reserveTruncate(hdr.numKeys * keyRecLen);
                workRecLen = keyRecLen - sizeof(offset_t);
                memcpy(target, source, sizeof(offset_t));
                source += sizeof(offset_t);
                target += sizeof(offset_t);
            }

            // this is where next row gets data from
            const char *prev, *next = NULL;
            unsigned prevOffset = 0;
            if (handleVariable)
                prevOffset = target-((char *)keyBufMb.bufferBase());
            else
                next = target;

            unsigned char pack1 = *source++;
            assert(0==pack1); // 1st time will be always be 0
            KEYRECSIZE_T left = workRecLen;
            while (left--) {
                *target = *source;
                source++;
                target++;
            }
            // do subsequent rows
            for (int i = 1; i < hdr.numKeys; i++) {
                if (handleVariable) {
                    memcpy(&workRecLen, source, sizeof(workRecLen));
                    _WINREV(workRecLen);
                    target = (char *)keyBufMb.reserve(sizeof(workRecLen)+sizeof(offset_t)+workRecLen);
                    size32_t tmpSz = sizeof(workRecLen)+sizeof(offset_t);
                    memcpy(target, source, tmpSz);
                    target += tmpSz;
                    source += tmpSz;
                }
                else
                {
                    memcpy(target, source, sizeof(offset_t));
                    source += sizeof(offset_t);
                    target += sizeof(offset_t);
                }
                pack1 = *source++;
                assert(pack1<=workRecLen);            
                if (handleVariable) {
                    prev = ((char *)keyBufMb.bufferBase())+prevOffset;
                    // for next
                    prevOffset = target-((char *)keyBufMb.bufferBase());
                }
                else {
                    prev = next;
                    next = target;
                }
                left = workRecLen - pack1;
                while (pack1--) {
                    *target = *prev;
                    prev++;
                    target++;
                }
                while (left--) {
                    *target = *source;
                    source++;
                    target++;
                }
            }
            expandedSize = keyBufMb.length();
            keyBuf = (char *)keyBufMb.detach();
            assertex(keyBuf);
        }
        else {
            keyBuf = NULL;
            expandedSize = 0;
        }
    }
}


offset_t CJHSearchNode::prevNodeFpos() const
{
    offset_t ll;

    if (!isLeaf())
        ll = getFPosAt(0);
    else
        ll = hdr.leftSib;
    return ll;
}

offset_t CJHSearchNode::nextNodeFpos() const
{
    offset_t ll;

    if (!isLeaf())
        ll = getFPosAt(hdr.numKeys - 1);
    else
        ll = hdr.rightSib;
    return ll;
}

int CJHSearchNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, keyBuf + index*keyRecLen + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

bool CJHSearchNode::fetchPayload(unsigned int index, char *dst) const
{
    if (index >= hdr.numKeys) return false;
    if (dst)
    {
        const char * p = keyBuf + index*keyRecLen;
        if (keyHdr->hasSpecialFileposition())
        {
            //It would make sense to have the fileposition at the start of the row from the perspective of the
            //internal representation, but that would complicate everything else which assumes the keyed
            //fields start at the beginning of the row.
            memcpy(dst+keyCompareLen, p+keyCompareLen+sizeof(offset_t), keyLen-keyCompareLen);
            memcpy(dst+keyLen, p, sizeof(offset_t));
        }
        else
        {
            memcpy(dst+keyCompareLen, p+keyCompareLen, keyLen-keyCompareLen);
        }
    }
    return true;
}

bool CJHSearchNode::getKeyAt(unsigned int index, char *dst) const
{
    if (index >= hdr.numKeys) return false;
    if (dst)
    {
        const char * p = keyBuf + index*keyRecLen;
        if (keyHdr->hasSpecialFileposition())
            p += sizeof(offset_t);
        memcpy(dst, p, keyCompareLen);
    }
    return true;
}

size32_t CJHSearchNode::getSizeAt(unsigned int index) const
{
    if (keyHdr->hasSpecialFileposition())
        return keyLen + sizeof(offset_t);
    else
        return keyLen;
}

offset_t CJHSearchNode::getFPosAt(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;

    offset_t pos;
    const char * p = keyBuf + index*keyRecLen;
    memcpy( &pos, p, sizeof(__int64));
    _WINREV(pos);
    return pos;
}

unsigned __int64 CJHSearchNode::getSequence(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;
    return firstSequence + index;
}

void CJHSearchNode::dump(FILE *out, int length, unsigned rowCount, bool raw) const
{
    CJHTreeNode::dump(out, length, rowCount, raw);
    if (rowCount==0 || rowCount > getNumKeys())
        rowCount = getNumKeys();
    char *dst = (char *) alloca(keyHdr->getMaxKeyLength() + sizeof(offset_t));
    for (unsigned int i=0; i<rowCount; i++)
    {
        getKeyAt(i, dst);
        fetchPayload(i, dst);
        if (raw)
        {
            fwrite(dst, 1, length, out);
        }
        else
        {
            offset_t pos = getFPosAt(i);
            StringBuffer s;
            appendURL(&s, dst, length, true);
            fprintf(out, "keyVal %d [%" I64F "d] = %s\n", i, pos, s.str());
        }
    }
    if (!raw)
        fprintf(out, "==========\n");
}


extern jhtree_decl void validateKeyFile(const char *filename, offset_t nodePos)
{
    OwnedIFile file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    if (!io)
        throw MakeStringException(1, "Invalid key %s: cannot open file", filename);
    unsigned __int64 size = file->size();
    if (!size)
        throw MakeStringException(2, "Invalid key %s: zero size", filename);
    KeyHdr hdr;
    if (io->read(0, sizeof(hdr), &hdr) != sizeof(hdr))
        throw MakeStringException(4, "Invalid key %s: failed to read key header", filename);
    CKeyHdr keyHdr;
    keyHdr.load(hdr);
    if (keyHdr.getKeyType() & USE_TRAILING_HEADER)
    {
        if (io->read(size - keyHdr.getNodeSize(), sizeof(hdr), &hdr) != sizeof(hdr))
            throw MakeStringException(4, "Invalid key %s: failed to read trailing key header", filename);
        keyHdr.load(hdr);
    }

    _WINREV(hdr.phyrec);
    _WINREV(hdr.root);
    _WINREV(hdr.nodeSize);
    if (hdr.phyrec != size-1)
        throw MakeStringException(5, "Invalid key %s: phyrec was %" I64F "d, expected %" I64F "d", filename, hdr.phyrec, size-1);
    if (!hdr.nodeSize || size % hdr.nodeSize)
        throw MakeStringException(3, "Invalid key %s: size %" I64F "d is not a multiple of key node size (%d)", filename, size, hdr.nodeSize);
    if (!hdr.root || hdr.root % hdr.nodeSize !=0)
        throw MakeStringException(6, "Invalid key %s: invalid root pointer %" I64F "x", filename, hdr.root);
    NodeHdr root;
    if (io->read(hdr.root, sizeof(root), &root) != sizeof(root))
        throw MakeStringException(7, "Invalid key %s: failed to read root node", filename);
    _WINREV(root.rightSib);
    _WINREV(root.leftSib);
    if (root.leftSib || root.rightSib)
        throw MakeStringException(8, "Invalid key %s: invalid root node sibling pointers 0x%" I64F "x, 0x%" I64F "x (expected 0,0)", filename, root.leftSib, root.rightSib);

    for (offset_t nodeOffset = (nodePos ? nodePos : hdr.nodeSize); nodeOffset < (nodePos ? nodePos+1 : size); nodeOffset += hdr.nodeSize)
    {
        MemoryAttr ma;
        char *buffer = (char *) ma.allocate(hdr.nodeSize);
        io->read(nodeOffset, hdr.nodeSize, buffer);

        CJHTreeNode theNode;
        theNode.load(&keyHdr, buffer, nodeOffset, true);

        NodeHdr *nodeHdr = (NodeHdr *) buffer;
        SwapBigEndian(*nodeHdr);
        if (!nodeHdr->isValid(hdr.nodeSize))
            throw MakeStringException(9, "Invalid key %s: invalid node header at position 0x%" I64F "x", filename, nodeOffset);
        if (nodeHdr->leftSib >= size || nodeHdr->rightSib >= size)
            throw MakeStringException(9, "Invalid key %s: out of range sibling pointers 0x%" I64F "x, 0x%" I64F "x at position 0x%" I64F "x", filename, nodeHdr->leftSib, nodeHdr->rightSib, nodeOffset);
        if (nodeHdr->crc32)
        {
            unsigned crc = crc32(buffer + sizeof(NodeHdr), nodeHdr->keyBytes, 0);
            if (crc != nodeHdr->crc32)
                throw MakeStringException(9, "Invalid key %s: crc mismatch at position 0x%" I64F "x", filename, nodeOffset);
        }
        else
        {
            // MORE - if we felt so inclined, we could decode the node and check records were in ascending order
        }
    }
}

//=========================================================================================================

CJHVarTreeNode::CJHVarTreeNode()
{
    recArray = NULL;
}

void CJHVarTreeNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHSearchNode::load(_keyHdr, rawData, _fpos, needCopy);
    unsigned n = getNumKeys();
    recArray = new const char * [n];
    const char *finger = keyBuf;
    for (unsigned int i=0; i<getNumKeys(); i++)
    {
        recArray[i] = finger + sizeof(KEYRECSIZE_T);
        KEYRECSIZE_T recsize = *(KEYRECSIZE_T *)finger;
        _WINREV(recsize);
        finger += recsize + sizeof(KEYRECSIZE_T) + sizeof(offset_t);
    }
}

CJHVarTreeNode::~CJHVarTreeNode()
{
    delete [] recArray;
}

int CJHVarTreeNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, recArray[index] + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

bool CJHVarTreeNode::fetchPayload(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;

    if (NULL != dst)
    {
        const char * p = recArray[num];
        KEYRECSIZE_T reclen = ((KEYRECSIZE_T *) p)[-1];
        _WINREV(reclen);
        if (keyHdr->hasSpecialFileposition())
        {
            memcpy(dst+keyCompareLen, p+keyCompareLen+sizeof(offset_t), reclen-keyCompareLen);
            memcpy(dst+reclen, p, sizeof(offset_t));
        }
        else
            memcpy(dst+keyCompareLen, p+keyCompareLen, reclen-keyCompareLen);
    }
    return true;
}

bool CJHVarTreeNode::getKeyAt(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;

    if (NULL != dst)
    {
        const char * p = recArray[num];
        KEYRECSIZE_T reclen = ((KEYRECSIZE_T *) p)[-1];
        _WINREV(reclen);
        assertex(reclen >= keyCompareLen);
        if (keyHdr->hasSpecialFileposition())
            memcpy(dst, p + sizeof(offset_t), keyCompareLen);
        else
            memcpy(dst, p, keyCompareLen);
    }
    return true;
}

size32_t CJHVarTreeNode::getSizeAt(unsigned int num) const
{
    const char * p = recArray[num];
    KEYRECSIZE_T reclen = ((KEYRECSIZE_T *) p)[-1];
    _WINREV(reclen);
    if (keyHdr->hasSpecialFileposition())
        return reclen + sizeof(offset_t);
    else
        return reclen;
}

offset_t CJHVarTreeNode::getFPosAt(unsigned int num) const
{
    if (num >= hdr.numKeys) return 0;

    const char * p = recArray[num];
    offset_t pos;
    memcpy( &pos, p, sizeof(__int64) );
    _WINREV(pos);
    return pos;
}

//=========================================================================================================

void CJHRowCompressedNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHSearchNode::load(_keyHdr, rawData, _fpos, needCopy);
    assertex(hdr.nodeType==NodeLeaf);
    char *keys = ((char *) rawData) + sizeof(hdr)+sizeof(firstSequence);
    assertex(IRandRowExpander::isRand(keys));
    rowexp.setown(createRandRDiffExpander());
    rowexp->init(keys, needCopy);
}

int CJHRowCompressedNode::compareValueAt(const char *src, unsigned int index) const
{
    return rowexp->cmpRow(src,index,keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0,keyCompareLen);
}

bool CJHRowCompressedNode::fetchPayload(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;
    if (dst)
    {
        if (keyHdr->hasSpecialFileposition())
        {
            rowexp->expandRow(dst+keyCompareLen,num,keyCompareLen+sizeof(offset_t),keyLen-keyCompareLen);
            rowexp->expandRow(dst+keyLen,num,0,sizeof(offset_t));
        }
        else
            rowexp->expandRow(dst+keyCompareLen,num,keyCompareLen,keyLen-keyCompareLen);
    }
    return true;
}

bool CJHRowCompressedNode::getKeyAt(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;
    if (dst)
    {
        if (keyHdr->hasSpecialFileposition())
        {
            rowexp->expandRow(dst,num,sizeof(offset_t),keyCompareLen);
        }
        else
            rowexp->expandRow(dst,num,0,keyCompareLen);
    }
    return true;
}

offset_t CJHRowCompressedNode::getFPosAt(unsigned int num) const
{
    if (num >= hdr.numKeys)
        return 0;
    else
    {
        offset_t pos;
        rowexp->expandRow(&pos,num,0,sizeof(pos));
        _WINREV(pos);
        return pos;
    }
}

//=========================================================================================================

CJHTreeBlobNode::CJHTreeBlobNode()
{
}

CJHTreeBlobNode::~CJHTreeBlobNode()
{
}

void CJHTreeBlobNode::unpack(const void *nodeData, bool needCopy)
{
    CJHTreeNode::unpack(nodeData, needCopy);
    const byte *data = ((const byte *) nodeData) + sizeof(hdr);
    char keyType = keyHdr->getKeyType();
    expandedSize = keyHdr->getNodeSize();
    keyBuf = expandKeys(data, expandedSize);
}

size32_t CJHTreeBlobNode::getTotalBlobSize(unsigned offset) const
{
    assertex(offset < expandedSize);
    unsigned datalen;
    memcpy(&datalen, keyBuf+offset, sizeof(datalen));
    _WINREV(datalen);
    return datalen;
}

size32_t CJHTreeBlobNode::getBlobData(unsigned offset, void *dst) const
{
    unsigned sizeHere = getTotalBlobSize(offset);
    offset += sizeof(unsigned);
    if (sizeHere > expandedSize - offset)
        sizeHere = expandedSize - offset;
    memcpy(dst, keyBuf+offset, sizeHere);
    return sizeHere;
}

void CJHTreeRawDataNode::unpack(const void *nodeData, bool needCopy)
{
    CJHTreeNode::unpack(nodeData, needCopy);
    // MORE - what about needCopy flag?
    const byte *data = ((const byte *) nodeData) + sizeof(hdr);
    unsigned short len = *reinterpret_cast<const unsigned short *>(data);
    _WINREV(len);
    expandedSize = len;
    keyBuf = (char *) allocMem(len);
    memcpy(keyBuf, data+sizeof(unsigned short), len);
}

void CJHTreeMetadataNode::get(StringBuffer & out) const
{
    out.append(expandedSize, keyBuf);
}

void CJHTreeBloomTableNode::get(MemoryBuffer & out) const
{
    out.append(expandedSize-read, keyBuf + read);
}

__int64 CJHTreeBloomTableNode::get8()
{
    __int64 ret = 0;
    assert(sizeof(ret)==8);
    assertex(expandedSize >= read + sizeof(ret));
    _WINCPYREV8(&ret, keyBuf + read);
    read += sizeof(ret);
    return ret;
}

unsigned CJHTreeBloomTableNode::get4()
{
    unsigned ret = 0;
    assert(sizeof(ret)==4);
    assertex(expandedSize >= read + sizeof(ret));
    _WINCPYREV4(&ret, keyBuf + read);
    read += sizeof(ret);
    return ret;
}


class DECL_EXCEPTION CKeyException : implements IKeyException, public CInterface
{
    int errCode;
    StringBuffer errMsg;
public:
    IMPLEMENT_IINTERFACE;

    CKeyException(int _errCode, const char *_errMsg, va_list &args) __attribute__((format(printf,3,0))) : errCode(_errCode)
    {
        if (_errMsg)
            errMsg.valist_appendf(_errMsg, args);
    }

    StringBuffer &translateCode(StringBuffer &out) const
    {
        out.append("IKeyException: ");
        switch (errCode)
        {
            case KeyExcpt_IncompatVersion:
                return out.append("Incompatible key version.");
            default:
                return out.append("UNKNOWN ERROR");
        }
    }

// IException
    int errorCode() const { return errCode; }
    StringBuffer &errorMessage(StringBuffer &out) const
    {
        return translateCode(out).append("\n").append(errMsg.str());
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

IKeyException *MakeKeyException(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IKeyException *e = new CKeyException(code, format, args);
    va_end(args);
    return e;
}

