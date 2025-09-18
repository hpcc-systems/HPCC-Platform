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

void CJHBlockCompressedSearchNode::load(CKeyHdr *_keyHdr, const void *rawData, offset_t _fpos, bool needCopy)
{
    CJHSearchNode::load(_keyHdr, rawData, _fpos, needCopy);
    keyLen = _keyHdr->getMaxKeyLength();
    keyCompareLen = _keyHdr->getNodeKeyLength();
    if (hdr.nodeType == NodeBranch)
        keyLen = keyHdr->getNodeKeyLength();
    keyRecLen = keyLen + sizeof(offset_t);
    char keyType = keyHdr->getKeyType();
    const char *keys = ((const char *) rawData) + sizeof(hdr);
    if (hdr.nodeType==NodeLeaf)
    {
        firstSequence = *(unsigned __int64 *) keys;
        keys += sizeof(unsigned __int64);
        _WINREV(firstSequence);
    }

    CCycleTimer expansionTimer(isLeaf());
    if (hdr.nodeType==NodeLeaf && (keyType & HTREE_COMPRESSED_KEY))
    {
        inMemorySize = keyHdr->getNodeSize();
        if ((keyType & (HTREE_QUICK_COMPRESSED_KEY|HTREE_VARSIZE))==HTREE_QUICK_COMPRESSED_KEY)
            keyBuf = nullptr;
        else
            keyBuf = expandData(keys, inMemorySize);
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
            inMemorySize = keyBufMb.length();
            keyBuf = (char *)keyBufMb.detach();
            assertex(keyBuf);
        }
        else {
            keyBuf = NULL;
            inMemorySize = 0;
            if (hdr.nodeType == NodeBranch)
            {
                if ((hdr.leftSib == 0) && (hdr.rightSib == 0))
                {
                    //Sanity check to catch error where a section of the file has unexpectedly been zeroed.
                    //which is otherwise tricky to track down.
                    //This can only legally happen if there is an index with 0 entries
                    if (keyHdr->getNumRecords() != 0)
                        throw MakeStringException(0, "Zeroed index node detected at offset %llu", getFpos());
                }
            }
        }
    }

    if (isLeaf())
        loadExpandTime = expansionTimer.elapsedNs();
}

int CJHBlockCompressedSearchNode::compareValueAt(const char *src, unsigned int index) const
{
    return memcmp(src, keyBuf + index*keyRecLen + (keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0), keyCompareLen);
}

//This critical section will be shared by all legacy nodes, but it is only used when recording events, so the potential contention is not a problem.
static CriticalSection payloadExpandCs;

bool CJHBlockCompressedSearchNode::fetchPayload(unsigned int index, char *dst, PayloadReference & activePayload) const
{
    if (index >= hdr.numKeys) return false;
    if (!dst) return true;

    bool recording = recordingEvents();
    if (recording)
    {
        std::shared_ptr<byte []> sharedPayload;

        {
            CriticalBlock block(payloadExpandCs);

            sharedPayload = expandedPayload.lock();
            if (!sharedPayload)
            {
                //Allocate a dummy payload so we can track whether it is hit or not
                sharedPayload = std::shared_ptr<byte []>(new byte[1]);
                expandedPayload = sharedPayload;
            }
        }

        queryRecorder().recordIndexPayload(keyHdr->getKeyId(), getFpos(), 0, getMemSize());

        //Ensure the payload stays alive for the duration of this call, and is likely preserved until
        //the next call.  Always replacing is as efficient as conditional - since we are using a move operator.
        activePayload.data = std::move(sharedPayload);
    }

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

//=========================================================================================================

CBlockCompressedWriteNode::CBlockCompressedWriteNode(offset_t _fpos, CKeyHdr *_keyHdr, bool isLeafNode) : CWriteNode(_fpos, _keyHdr, isLeafNode)
{
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
        // For HTREE_COMPRESSED_KEY hdr.keyBytes is only updated on the first row (above),
        // and then in the finalize() call after the compressor has been closed.
        if (0 == hdr.numKeys)
        {
            bool isVariable = keyHdr->isVariable();
            //Adjust the fixed key size to include the fileposition field which is written by writekey.
            size32_t fixedKeySize = isVariable ? 0 : keyLen + sizeof(offset_t);
            bool rowCompressed = (keyType&HTREE_QUICK_COMPRESSED_KEY)==HTREE_QUICK_COMPRESSED_KEY;
            lzwcomp.open(keyPtr, maxBytes-hdr.keyBytes, isVariable, rowCompressed, fixedKeySize);
        }
        if (0xffff == hdr.numKeys || 0 == lzwcomp.writekey(pos, (const char *)indata, insize))
            return false;
    }
    else
    {
        if (0xffff == hdr.numKeys)
            return false;
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
    memorySize += insize + sizeof(pos);
    return true;
}

void CBlockCompressedWriteNode::finalize()
{
    if (isLeaf() && (keyHdr->getKeyType() & HTREE_COMPRESSED_KEY))
    {
        lzwcomp.close();
        if (hdr.numKeys)
            hdr.keyBytes = lzwcomp.buflen() + sizeof(unsigned __int64); // rsequence
    }
}

size32_t CBlockCompressedWriteNode::compressValue(const char *keyData, size32_t size, char *result)
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

