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

#ifndef JHBLOCK_COMPRESSED_HPP
#define JHBLOCK_COMPRESSED_HPP

#include "jiface.hpp"
#include "jhutil.hpp"
#include "hlzw.h"
#include "jcrc.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "ctfile.hpp"

class CJHBlockCompressedSearchNode : public CJHSearchNode
{
protected:
    size32_t keyLen = 0;
    size32_t keyCompareLen = 0;
    size32_t keyRecLen = 0;

    unsigned __int64 firstSequence = 0;

    inline size32_t getKeyLen() const { return keyLen; }

public:
//These are the key functions that need to be implemented for a node that can be searched
    inline size32_t getNumKeys() const { return hdr.numKeys; }
    virtual bool getKeyAt(unsigned int num, char *dest) const override;         // Retrieve keyed fields
    virtual bool fetchPayload(unsigned int num, char *dest, PayloadReference & activePayload) const override;     // Retrieve payload fields. Note destination is assumed to already contain keyed fields
    virtual size32_t getSizeAt(unsigned int num) const override;
    virtual offset_t getFPosAt(unsigned int num) const override;
    virtual unsigned __int64 getSequence(unsigned int num) const override;
    virtual int compareValueAt(const char *src, unsigned int index) const override;

    virtual int locateGE(const char * search, unsigned minIndex) const override;
    virtual int locateGT(const char * search, unsigned minIndex) const override;

// Loading etc
    virtual void load(CKeyHdr *keyHdr, const void *rawData, offset_t pos, bool needCopy) override;
    virtual void dump(FILE *out, int length, unsigned rowCount, bool raw) const override;

protected:
    //The following are used for faking payload events
    mutable std::weak_ptr<byte[]> expandedPayload;
};

class jhtree_decl CBlockCompressedWriteNode : public CWriteNode
{
private:
    KeyCompressor lzwcomp;
    unsigned keyLen = 0;
    size32_t memorySize = 0;
    char *lastKeyValue = nullptr;
    unsigned __int64 lastSequence = 0;

    size32_t compressValue(const char *keyData, size32_t size, char *result);
public:
    CBlockCompressedWriteNode(offset_t fpos, CKeyHdr *keyHdr, bool isLeafNode);
    ~CBlockCompressedWriteNode();

    virtual bool add(offset_t pos, const void *data, size32_t size, unsigned __int64 sequence) override;
    virtual void finalize() override;
    virtual const void *getLastKeyValue() const override { return lastKeyValue; }
    virtual unsigned __int64 getLastSequence() const override { return lastSequence; }
    virtual size32_t getMemorySize() const override { return memorySize; }
};

#endif
