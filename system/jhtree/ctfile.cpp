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
}

inline void SwapBigEndian(NodeHdr &hdr)
{
    _WINREV(hdr.rightSib);
    _WINREV(hdr.leftSib);
    _WINREV(hdr.numKeys);
    _WINREV(hdr.keyBytes);
    _WINREV(hdr.crc32);
//   _WINREV(hdr.memNumber);
//   _WINREV(hdr.leafFlag);
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
                if (hdr.ktype & USE_TRAILING_HEADER)
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
            if (hdr.ktype & USE_TRAILING_HEADER)
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

void CKeyHdr::write(IFileIOStream *out, CRC32 *crc)
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

unsigned int CKeyHdr::getMaxKeyLength() 
{
    return hdr.length; 
}

bool CKeyHdr::isVariable() 
{
    return (hdr.ktype & HTREE_VARSIZE) == HTREE_VARSIZE; 
}

//=========================================================================================================

CNodeBase::CNodeBase()
{
    keyHdr = NULL;
    fpos = 0;
    keyLen = 0;
    keyType = 0;
    keyCompareLen = 0;
    isVariable = false;
}

void CNodeBase::load(CKeyHdr *_keyHdr, offset_t _fpos)
{
    _keyHdr->Link();
    keyHdr = _keyHdr;
    keyLen = keyHdr->getMaxKeyLength();
    keyCompareLen = keyHdr->getNodeKeyLength();
    keyType = keyHdr->getKeyType();
    memset(&hdr, 0, sizeof(hdr));
    fpos = _fpos;
    isVariable = keyHdr->isVariable();
}

CNodeBase::~CNodeBase()
{
    keyHdr->Release();
}


//=========================================================================================================

CWriteNodeBase::CWriteNodeBase(offset_t _fpos, CKeyHdr *_keyHdr) 
{
    CNodeBase::load(_keyHdr, _fpos);
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
    if (isLeaf() && (keyType & HTREE_COMPRESSED_KEY))
        lzwcomp.close();
    assertex(hdr.keyBytes<=maxBytes);
    writeHdr();
    out->seek(getFpos(), IFSbegin);
    out->write(keyHdr->getNodeSize(), nodeBuf);
    if (crc)
        crc->tally(keyHdr->getNodeSize(), nodeBuf);
}

//=========================================================================================================

CWriteNode::CWriteNode(offset_t _fpos, CKeyHdr *_keyHdr, bool isLeaf) : CWriteNodeBase(_fpos, _keyHdr)
{
    hdr.leafFlag = isLeaf ? 1 : 0;
    if (!isLeaf)
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
            lzwcomp.open(keyPtr, maxBytes-hdr.keyBytes, isVariable, (keyType&HTREE_QUICK_COMPRESSED_KEY)==HTREE_QUICK_COMPRESSED_KEY);
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
        if (!indata)
        {
            lastnode = true;
            indata = alloca(insize);
            memset((void *) indata, 0xff, insize);
        }
        //assertex(insize==keyLen);
        const void *data;
        int size;

        if (keyType & COL_PREFIX)
        {
            char *result = (char *) alloca(insize+1);  // Gets bigger if no leading common!
            size = compressValue((const char *) indata, insize, result);
            data = result;
        }
        else
        {
            size = insize;
            data = indata;
        }

        int bytes = sizeof(pos) + size;
        if (isVariable)
            bytes += sizeof(KEYRECSIZE_T);
        if (hdr.keyBytes + bytes >= maxBytes)    // probably could be '>' (loses byte)
            return false;

        if (isVariable && isLeaf())
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
    hdr.leafFlag = 2;
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
    hdr.leafFlag = 3;
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
    hdr.leafFlag = 4;
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

CJHTreeNode::CJHTreeNode()
{
    keyBuf = NULL;
    keyRecLen = 0;
    firstSequence = 0;
    expandedSize = 0;
}

void CJHTreeNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CNodeBase::load(_keyHdr, _fpos);
    unpack(rawData, needCopy);
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

char *CJHTreeNode::expandKeys(void *src,size32_t &retsize)
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

size32_t CJHTreeNode::getNodeSize() const
{
    return keyHdr->getNodeSize();
}

void CJHTreeNode::unpack(const void *node, bool needCopy)
{
    memcpy(&hdr, node, sizeof(hdr));
    SwapBigEndian(hdr);
    __int64 maxsib = keyHdr->getHdrStruct()->phyrec;
    if (!hdr.isValid(keyHdr->getNodeSize()))
    {
        PROGLOG("hdr.leafFlag=%d",(int)hdr.leafFlag);
        PROGLOG("hdr.rightSib=%" I64F "d",hdr.rightSib);
        PROGLOG("hdr.leftSib=%" I64F "d",hdr.leftSib);
        PROGLOG("maxsib=%" I64F "d",maxsib);
        PROGLOG("nodeSize=%d", keyHdr->getNodeSize());
        PROGLOG("keyBytes=%d",(int)hdr.keyBytes);
        PrintStackReport();
        throw MakeStringException(0, "Htree: Corrupt key node detected");
    }
    if (!hdr.leafFlag)
        keyLen = keyHdr->getNodeKeyLength();
    keyRecLen = keyLen + sizeof(offset_t);
    char *keys = ((char *) node) + sizeof(hdr);
    if (hdr.crc32)
    {
        unsigned crc = crc32(keys, hdr.keyBytes, 0);
        if (hdr.crc32 != crc)
            throw MakeStringException(0, "CRC error on key node");
    }
    if (hdr.leafFlag==1)
    {
        firstSequence = *(unsigned __int64 *) keys;
        keys += sizeof(unsigned __int64);
        _WINREV(firstSequence);
    }
    if(isMetadata() || isBloom())
    {
        unsigned short len = *reinterpret_cast<unsigned short *>(keys);
        _WINREV(len);
        expandedSize = len;
        keyBuf = (char *) allocMem(len);
        memcpy(keyBuf, keys+sizeof(unsigned short), len);
    }
    else if (isLeaf() && (keyType & HTREE_COMPRESSED_KEY))
    {
        {
            MTIME_SECTION(queryActiveTimer(), "Compressed node expand");
            expandedSize = keyHdr->getNodeSize();
            bool quick = !isBlob() && (keyType&(HTREE_QUICK_COMPRESSED_KEY|HTREE_VARSIZE))==HTREE_QUICK_COMPRESSED_KEY;
            keyBuf = NULL;
            if (!quick)
                keyBuf = expandKeys(keys,expandedSize);
        }
    }
    else
    {
        int i;
        if (keyType & COL_PREFIX)
        {
            MTIME_SECTION(queryActiveTimer(), "COL_PREFIX expand");
            
            if (hdr.numKeys) {
                bool handleVariable = isVariable && isLeaf();
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
#ifdef _DEBUG
                assertex(0==pack1); // 1st time will be always be 0
#endif
                KEYRECSIZE_T left = workRecLen;
                while (left--) {
                    *target = *source;
                    source++;
                    target++;
                }
                // do subsequent rows
                for (i = 1; i < hdr.numKeys; i++) {
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
#ifdef _DEBUG
                    assertex(pack1<=workRecLen);            
#endif
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
        else
        {
            MTIME_SECTION(queryActiveTimer(), "NO compression copy");
            expandedSize = hdr.keyBytes + sizeof( __int64 );  // MORE - why is the +sizeof() there?
            keyBuf = (char *) allocMem(expandedSize);
            memcpy(keyBuf, keys, hdr.keyBytes + sizeof( __int64 ));
        }
    }
}

offset_t CJHTreeNode::prevNodeFpos() const
{
    offset_t ll;

    if (!isLeaf())
        ll = getFPosAt(0);
    else
        ll = hdr.leftSib;
    return ll;
}

offset_t CJHTreeNode::nextNodeFpos() const
{
    offset_t ll;

    if (!isLeaf())
        ll = getFPosAt(hdr.numKeys - 1);
    else
        ll = hdr.rightSib;
    return ll;
}

int CJHTreeNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, keyBuf + index*keyRecLen + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

bool CJHTreeNode::getValueAt(unsigned int index, char *dst) const
{
    if (index >= hdr.numKeys) return false;
    if (dst)
    {
        if (keyHdr->hasSpecialFileposition())
        {
            //It would make sense to have the fileposition at the start of the row from the perspective of the
            //internal representation, but that would complicate everything else which assumes the keyed
            //fields start at the beginning of the row.
            const char * p = keyBuf + index*keyRecLen;
            memcpy(dst, p + sizeof(offset_t), keyLen);
            memcpy(dst+keyLen, p, sizeof(offset_t));
        }
        else
        {
            const char * p = keyBuf + index*keyRecLen;
            memcpy(dst, p, keyLen);
        }
    }
    return true;
}

size32_t CJHTreeNode::getSizeAt(unsigned int index) const
{
    if (keyHdr->hasSpecialFileposition())
        return keyLen + sizeof(offset_t);
    else
        return keyLen;
}

offset_t CJHTreeNode::getFPosAt(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;

    offset_t pos;
    const char * p = keyBuf + index*keyRecLen;
    memcpy( &pos, p, sizeof(__int64));
    _WINREV(pos);
    return pos;
}

unsigned __int64 CJHTreeNode::getSequence(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;
    return firstSequence + index;
}

bool CJHTreeNode::contains(const char *src) const
{ // returns true if node contains key

    if (compareValueAt(src, 0)<0)
        return false;
    if (compareValueAt(src, hdr.numKeys-1)>0)
        return false;
    return true;
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
        {
            MTIME_SECTION(queryActiveTimer(), "JHTREE read index node");
            io->read(nodeOffset, hdr.nodeSize, buffer);
        }
        CJHTreeNode theNode;
        {
            MTIME_SECTION(queryActiveTimer(), "JHTREE load index node");
            theNode.load(&keyHdr, buffer, nodeOffset, true);
        }
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
    CJHTreeNode::load(_keyHdr, rawData, _fpos, needCopy);
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

bool CJHVarTreeNode::getValueAt(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;

    if (NULL != dst)
    {
        const char * p = recArray[num];
        KEYRECSIZE_T reclen = ((KEYRECSIZE_T *) p)[-1];
        _WINREV(reclen);
        if (keyHdr->hasSpecialFileposition())
        {
            memcpy(dst, p + sizeof(offset_t), reclen);
            memcpy(dst+reclen, p, sizeof(offset_t));
        }
        else
            memcpy(dst, p, reclen);
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
    CJHTreeNode::load(_keyHdr, rawData, _fpos, needCopy);
    assertex(hdr.leafFlag==1);
    char *keys = ((char *) rawData) + sizeof(hdr)+sizeof(firstSequence);
    assertex(IRandRowExpander::isRand(keys));
    rowexp.setown(createRandRDiffExpander());
    rowexp->init(keys, needCopy);
}

int CJHRowCompressedNode::compareValueAt(const char *src, unsigned int index) const
{
    return rowexp->cmpRow(src,index,keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0,keyCompareLen);
}

bool CJHRowCompressedNode::getValueAt(unsigned int num, char *dst) const
{
    if (num >= hdr.numKeys) return false;
    if (dst)
    {
        if (keyHdr->hasSpecialFileposition())
        {
            rowexp->expandRow(dst,num,sizeof(offset_t),keyLen);
            rowexp->expandRow(dst+keyLen,num,0,sizeof(offset_t));
        }
        else
            rowexp->expandRow(dst,num,0,keyLen);
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

size32_t CJHTreeBlobNode::getTotalBlobSize(unsigned offset)
{
    assertex(offset < expandedSize);
    unsigned datalen;
    memcpy(&datalen, keyBuf+offset, sizeof(datalen));
    _WINREV(datalen);
    return datalen;
}

size32_t CJHTreeBlobNode::getBlobData(unsigned offset, void *dst)
{
    unsigned sizeHere = getTotalBlobSize(offset);
    offset += sizeof(unsigned);
    if (sizeHere > expandedSize - offset)
        sizeHere = expandedSize - offset;
    memcpy(dst, keyBuf+offset, sizeHere);
    return sizeHere;
}

void CJHTreeMetadataNode::get(StringBuffer & out)
{
    out.append(expandedSize, keyBuf);
}

void CJHTreeBloomTableNode::get(MemoryBuffer & out)
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

