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

#ifndef KEYBUILD_HPP
#define KEYBUILD_HPP

#include "ctfile.hpp"

class CNodeInfo : public CInterface, implements serializable
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
    static int compare(IInterface **ll, IInterface **rr)
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
    virtual void finish(unsigned *crc = NULL) = 0;
    virtual void finish(IPropertyTree * metadata, unsigned * crc = NULL) = 0;
    virtual void processKeyData(const char *keyData, offset_t pos, size32_t recsize) = 0;
    virtual void addLeafInfo(CNodeInfo *info) = 0;
    virtual unsigned __int64 createBlob(size32_t size, const char * _ptr) = 0;
};

extern jhtree_decl IKeyBuilder *createKeyBuilder(IFileIOStream *_out, unsigned flags, unsigned rawSize, offset_t fileSize, unsigned nodeSize, unsigned keyFieldSize, unsigned __int64 startSequence);

interface IKeyDesprayer : public IInterface
{
    virtual void addPart(unsigned idx, offset_t numRecords, NodeInfoArray & nodes) = 0;
    virtual void finish() = 0;
};

extern jhtree_decl IKeyDesprayer * createKeyDesprayer(IFile * in, IFileIOStream * out);

#endif
