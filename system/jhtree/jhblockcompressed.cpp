/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "jhblockcompressed.hpp"

#include "platform.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

#ifdef __linux__
#include <alloca.h>
#endif

#include "jmisc.hpp"
#include "jset.hpp"
#include "jzstd.hpp"
#include "hlzw.h"

#include "ctfile.hpp"
#include "jstats.h"
#include "jevent.hpp"

int CJHBlockCompressedSearchNode::locateGE(const char * search, unsigned minIndex) const
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

int CJHBlockCompressedSearchNode::locateGT(const char * search, unsigned minIndex) const
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

char *CJHBlockCompressedSearchNode::expandBlock(const void *src, size32_t &decompressedSize, CompressionMethod compressionMethod)
{
    ICompressHandler * handler = queryCompressHandler(compressionMethod);
    if (!handler)
        throw makeStringExceptionV(JHTREE_KEY_UNKNOWN_COMPRESSION, "Unknown payload compression method %d", (int)compressionMethod);

    const char * options = nullptr;
    Owned<IExpander> exp = handler->getExpander(options);

    int len=exp->init(src);
    if (len==0)
    {
        decompressedSize = 0;
        return NULL;
    }
    char *outkeys=(char *) allocMem(len);
    exp->expand(outkeys);
    decompressedSize = len;
    return outkeys;
}

void CJHBlockCompressedSearchNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHSearchNode::load(_keyHdr, rawData, _fpos, needCopy);

    keyLen = keyHdr->getMaxKeyLength();
    keyCompareLen = _keyHdr->getNodeKeyLength();
    keyRecLen = keyLen + sizeof(offset_t);

    const char *keys = ((const char *) rawData) + sizeof(hdr);

    firstSequence = *(unsigned __int64 *) keys;
    keys += sizeof(unsigned __int64);
    _WINREV(firstSequence);
    
    CompressionMethod compressionMethod = *(CompressionMethod*) keys;
    keys += sizeof(CompressionMethod);

    CCycleTimer expansionTimer(true);
    keyBuf = expandBlock(keys, inMemorySize, compressionMethod);
    loadExpandTime = expansionTimer.elapsedNs();
}

int CJHBlockCompressedSearchNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, keyBuf + index*keyRecLen + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

bool CJHBlockCompressedSearchNode::fetchPayload(unsigned int index, char *dst, PayloadReference & activePayload) const
{
    if (index >= hdr.numKeys) return false;
    if (!dst) return true;

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
    return true;
}

bool CJHBlockCompressedSearchNode::getKeyAt(unsigned int index, char *dst) const
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

size32_t CJHBlockCompressedSearchNode::getSizeAt(unsigned int index) const
{
    if (keyHdr->hasSpecialFileposition())
        return keyLen + sizeof(offset_t);
    else
        return keyLen;
}

offset_t CJHBlockCompressedSearchNode::getFPosAt(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;

    offset_t pos;
    const char * p = keyBuf + index*keyRecLen;
    memcpy( &pos, p, sizeof(__int64));
    _WINREV(pos);
    return pos;
}

unsigned __int64 CJHBlockCompressedSearchNode::getSequence(unsigned int index) const
{
    if (index >= hdr.numKeys) return 0;
    return firstSequence + index;
}

void CJHBlockCompressedSearchNode::dump(FILE *out, int length, unsigned rowCount, bool raw) const
{
    CJHSearchNode::dump(out, length, rowCount, raw);

    if (rowCount==0 || rowCount > getNumKeys())
        rowCount = getNumKeys();
    char *dst = (char *) alloca(keyHdr->getMaxKeyLength() + sizeof(offset_t));

    PayloadReference activePayload;
    for (unsigned int i=0; i<rowCount; i++)
    {
        getKeyAt(i, dst);
        fetchPayload(i, dst, activePayload);
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


void CJHNewBlobNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHTreeNode::load(_keyHdr, rawData, _fpos, needCopy);
    const byte *data = ((const byte *) rawData) + sizeof(hdr);
    CompressionMethod method = (CompressionMethod)*data++;
    inMemorySize = keyHdr->getNodeSize();
    keyBuf = expandData(queryCompressHandler(method), data, inMemorySize);
}

//=========================================================================================================

CBlockCompressedWriteNode::CBlockCompressedWriteNode(offset_t _fpos, CKeyHdr *_keyHdr, bool isLeafNode, const CBlockCompressedBuildContext& ctx) : 
    CWriteNode(_fpos, _keyHdr, isLeafNode), context(ctx)
{
    hdr.compressionType = BlockCompression;
    keyLen = keyHdr->getMaxKeyLength();
    if (!isLeafNode)
    {
        keyLen = keyHdr->getNodeKeyLength();
    }
    lastKeyValue = (char *) malloc(keyLen);
    lastSequence = 0;
}

CBlockCompressedWriteNode::~CBlockCompressedWriteNode()
{
    free(lastKeyValue);
}

bool CBlockCompressedWriteNode::add(offset_t pos, const void *indata, size32_t insize, unsigned __int64 sequence)
{
    if (hdr.numKeys == 0)
    {
        unsigned __int64 rsequence = sequence;
        _WINREV(rsequence);
        memcpy(keyPtr, &rsequence, sizeof(rsequence));
        keyPtr += sizeof(rsequence);
        hdr.keyBytes += sizeof(rsequence);

        memcpy(keyPtr, &context.compressionMethod, sizeof(context.compressionMethod));
        keyPtr += sizeof(context.compressionMethod);
        hdr.keyBytes += sizeof(context.compressionMethod);

        //Adjust the fixed key size to include the fileposition field which is written by writekey.
        bool isVariable = keyHdr->isVariable();
        size32_t fixedKeySize = isVariable ? 0 : keyLen + sizeof(offset_t);

        ICompressHandler * handler = queryCompressHandler(context.compressionMethod);
        compressor.open(keyPtr, maxBytes-hdr.keyBytes, handler, context.compressionOptions, isVariable, fixedKeySize);
    }

    if (0xffff == hdr.numKeys || 0 == compressor.writekey(pos, (const char *)indata, insize))
        return false;

    if (insize>keyLen)
        throw MakeStringException(0, "key+payload (%u) exceeds max length (%u)", insize, keyLen);

    memcpy(lastKeyValue, indata, insize);
    lastSequence = sequence;
    hdr.numKeys++;
    memorySize += insize + sizeof(pos);
    return true;
}

void CBlockCompressedWriteNode::finalize()
{
    compressor.close();
    if (hdr.numKeys)
        hdr.keyBytes = compressor.buflen() + sizeof(unsigned __int64) + sizeof(CompressionMethod); // rsequence
}

//=========================================================================================================

BlockCompressedIndexCompressor::BlockCompressedIndexCompressor(unsigned keyedSize, IHThorIndexWriteArg *helper, const char* options)
{
    CompressionMethod compressionMethod = COMPRESS_METHOD_ZSTDS;
    StringBuffer compressionOptions;

    auto processOption = [this] (const char * option, const char * value)
    {
        CompressionMethod method = translateToCompMethod(option, COMPRESS_METHOD_NONE);
        if (method != COMPRESS_METHOD_NONE)
        {
            context.compressionMethod = method;
            if (!streq(value, "1"))
                context.compressionOptions.append(',').append(value);
        }
        else if (strieq(option, "compression"))
        {
            context.compressionMethod = translateToCompMethod(value, COMPRESS_METHOD_ZSTDS);
        }
        else if (strieq(option, "compressopt"))
        {
            context.compressionOptions.append(',').append(value);
        }
    };

    processOptionString(options, processOption);

    context.compressionHandler = queryCompressHandler(compressionMethod);
    if (!context.compressionHandler)
        throw MakeStringException(0, "Unknown compression method %d", (int)compressionMethod);
}

CJHBlockCompressedVarNode::CJHBlockCompressedVarNode() {}
CJHBlockCompressedVarNode::~CJHBlockCompressedVarNode()
{
    delete [] recArray;
}

void CJHBlockCompressedVarNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHBlockCompressedSearchNode::load(_keyHdr, rawData, _fpos, needCopy);
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

int CJHBlockCompressedVarNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, recArray[index] + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

bool CJHBlockCompressedVarNode::fetchPayload(unsigned int num, char *dst, PayloadReference & activePayload) const
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

bool CJHBlockCompressedVarNode::getKeyAt(unsigned int num, char *dst) const
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

size32_t CJHBlockCompressedVarNode::getSizeAt(unsigned int num) const
{
    const char * p = recArray[num];
    KEYRECSIZE_T reclen = ((KEYRECSIZE_T *) p)[-1];
    _WINREV(reclen);
    if (keyHdr->hasSpecialFileposition())
        return reclen + sizeof(offset_t);
    else
        return reclen;
}

offset_t CJHBlockCompressedVarNode::getFPosAt(unsigned int num) const
{
    if (num >= hdr.numKeys) return 0;

    const char * p = recArray[num];
    offset_t pos;
    memcpy( &pos, p, sizeof(__int64) );
    _WINREV(pos);
    return pos;
}