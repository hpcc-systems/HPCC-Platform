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

#ifndef KEYBUILD_HPP
#define KEYBUILD_HPP

#include "ctfile.hpp"
#include "eclhelper.hpp"

class CNodeInfo : implements serializable, public CInterface
{
public:
    offset_t pos;
    void *value;
    unsigned size;
    unsigned __int64 sequence;

    IMPLEMENT_IINTERFACE;

    CNodeInfo(offset_t _pos, const void *_value, unsigned _size, unsigned __int64 _sequence)
    {
        pos = _pos;
        size = _size;
        sequence = _sequence;
        if (_value)
        {
            value = malloc(size);
            memcpy(value, _value, size);
        }
        else
            value = NULL;
    }
    CNodeInfo()
    {
        pos = 0;
        size = 0;
        value = NULL;
        sequence = 0;
    }
    ~CNodeInfo()
    {
        free(value);
    }
    void serialize(MemoryBuffer &tgt)
    {
        tgt.append(pos).append(size).append(sequence);
        if (value)
            tgt.append(true).append(size, value);
        else
            tgt.append(false);
    }
    void deserialize(MemoryBuffer &src)
    {
        src.read(pos);
        src.read(size);
        src.read(sequence);
        bool hasValue;
        src.read(hasValue);
        if (hasValue)
        {
            value = malloc(size);
            memcpy(value, src.readDirect(size), size);
        }
        else
            value = NULL;
    }
    static int compare(IInterface * const *ll, IInterface * const *rr)
    {
        CNodeInfo *l = (CNodeInfo *) *ll;
        CNodeInfo *r = (CNodeInfo *) *rr;
        if (l->pos < r->pos) 
            return -1;
        else if (l->pos == r->pos) 
            return 0;
        else
            return 1;
    }
};

typedef IArrayOf<CNodeInfo> NodeInfoArray;

interface IKeyBuilder : public IInterface
{
    virtual void finish(IPropertyTree * metadata, unsigned * crc) = 0;
    virtual void processKeyData(const char *keyData, offset_t pos, size32_t recsize) = 0;
    virtual void addLeafInfo(CNodeInfo *info) = 0;
    virtual unsigned __int64 createBlob(size32_t size, const char * _ptr) = 0;
    virtual unsigned __int64 getDuplicateCount() = 0;
};

extern jhtree_decl IKeyBuilder *createKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, unsigned nodeSize, unsigned keyFieldSize, unsigned __int64 startSequence, IHThorIndexWriteArg *helper, bool enforceOrder, bool isTLK);

interface IKeyDesprayer : public IInterface
{
    virtual void addPart(unsigned idx, offset_t numRecords, NodeInfoArray & nodes) = 0;
    virtual void finish() = 0;
};

extern jhtree_decl IKeyDesprayer * createKeyDesprayer(IFile * in, IFileIOStream * out);
extern jhtree_decl bool checkReservedMetadataName(const char *name);

#endif
