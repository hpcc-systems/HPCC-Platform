/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef rtlrecord_hpp
#define rtlrecord_hpp

#if defined(_WIN32)
#include <malloc.h>
#else
#include <alloca.h>
#endif

#include "rtlfield.hpp"

//These classe provides a relatively efficient way to access fields within a variable length record structure.
// Probably convert to an interface with various concrete implementations for varing degrees of complexity
//
// Complications:
// * ifblocks
// * nested records.
// * alien data types
//

struct ECLRTL_API RtlRecord
{
public:
    friend class RtlRow;
    RtlRecord(const RtlRecordTypeInfo & fields, bool expandFields);
    ~RtlRecord();

    void calcRowOffsets(size_t * variableOffsets, const void * _row) const;

    virtual size32_t getFixedSize() const
    {
        return numVarFields ? 0 : fixedOffsets[numFields];
    }

    size_t getOffset(size_t * variableOffsets, unsigned field) const
    {
        return fixedOffsets[field] + variableOffsets[whichVariableOffset[field]];
    }


    size_t getRecordSize(size_t * variableOffsets) const
    {
        return getOffset(variableOffsets, numFields);
    }

    virtual size32_t getMinRecordSize() const;

    inline unsigned getNumVarFields() const { return numVarFields; }
    inline const RtlTypeInfo * queryType(unsigned field) const { return fields[field]->type; }

protected:
    size_t * fixedOffsets;        // fixed portion of the field offsets + 1 extra
    unsigned * whichVariableOffset;// which variable offset should be added to the fixed
    unsigned * variableFieldIds;  // map variable field to real field id.
    unsigned numFields;
    unsigned numVarFields;
    const RtlFieldInfo * const * fields;
    const RtlFieldInfo * const * originalFields;
};

class ECLRTL_API RtlRow
{
public:
    RtlRow(const RtlRecord & _info, const void * optRow, unsigned numOffsets, size_t * _variableOffsets);

    __int64 getInt(unsigned field) const;
    void getUtf8(size32_t & resultLen, char * & result, unsigned field) const;

    size_t getOffset(unsigned field) const
    {
        return info.getOffset(variableOffsets, field);
    }

    size_t getRecordSize() const
    {
        return info.getRecordSize(variableOffsets);
    }

    void setRow(const void * _row);

protected:
    const RtlRecord & info;
    const void * row;
    size_t * variableOffsets;       // [0 + 1 entry for each variable size field ]
};

struct ECLRTL_API RtlDynRow : public RtlRow
{
public:
    RtlDynRow(const RtlRecord & _info, const void * optRow = NULL);
    ~RtlDynRow();
};

//The following template class is used from the generated code to avoid allocating the offset array
template <unsigned NUM_VARIABLE_FIELDS>
struct ECLRTL_API RtlStaticRow : RtlRow
{
public:
    RtlStaticRow(const RtlRecord & _info, const void * optRow = NULL) : RtlRow(_info, optRow, NUM_VARIABLE_FIELDS+1, off) {}
public:
    size_t off[NUM_VARIABLE_FIELDS+1];
};

class ECLRTL_API RtlRecordSize : public IRecordSize, public RtlCInterface
{
public:
    RtlRecordSize(const RtlRecordTypeInfo & fields) : offsetInformation(fields, true) {}
    RTLIMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void * row)
    {
        //Allocate a temporary offset array on the stack to avoid runtime overhead.
        unsigned numOffsets = offsetInformation.getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow offsetCalculator(offsetInformation, row, numOffsets, variableOffsets);
        return offsetCalculator.getRecordSize();
    }

    virtual size32_t getFixedSize() const
    {
        return offsetInformation.getFixedSize();
    }
    // returns 0 for variable row size
    virtual size32_t getMinRecordSize() const
    {
        return offsetInformation.getMinRecordSize();
    }

protected:
    RtlRecord offsetInformation;
};

#endif
