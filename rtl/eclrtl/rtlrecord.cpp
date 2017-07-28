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

#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jlib.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"

/*
 * Potential different implementations (for all fixed size has no penalty):
 *
 * a. when row changes, update offsets of all offset fields
 *    + access is simple, single array lookup
 *    - lots more fields are updated even if the fields aren't used.
 *
 * b. when a row changes update the next offset for all variable size fields.
 *    + minimal updates
 *    - offset access is more complex
 *
 * c. when row changes clear cache, and calculate on demand.
 *    + trivial update on row change
 *    + no cost of fields not accessed
 *    + may be required to implement ifblocks - since fields in the test expression must be evaluated before all the
 *      offsets are known.  Depends on the implementation of ifblocks!
 *    - accessing an offset will involve a loop and be more expensive
 *
 * Handling complications:
 * a. Nested rows
 *    A dilemma.  Should they be expanded?
 *    + If they are expanded then it makes it much simpler to use externally.
 *    - The nested fields have compound names storing and matching them is a complication.
 *    - The code generator doesn't expand.
 *    + It is easier to calculate offsets since all fields are supported by the same class.
 *    + Sizes for unexpanded rows would need their own size caching classes.  That is more complex!
 *    + The meta information currently processes child information.
 *
 * Not expanding is complicated if you also try and only calculate the offsets of fields in nested records once - since you end
 * up needing to call back and forth between instances and the static information.
 * However, if nested records are processed by using size(), and selections from them are processed by instantiating
 * an instance of the nested row it is all much simpler.  The cost is potentially re-evaluating sizes of nested fields.  Potentially
 * inefficient if most are fixed size, but some are variable.
 *
 * b. bitfields
 *    the last bitfield in a bitfield container has the size of the container, the others have 0 size.
 *
 * c. ifblocks
 *    Nasty.  Allowing direct access means the flag would need checking, and a field would need a pointer
 *    to its containing ifblock.  Cleanest to have a different derived implementation which was used if the record
 *    contained ifblocks which added calls to check ifblocks before size()/getValue() etc.
 *    Will require an extra row parameter to every RtlTypeInfo::getX() function.
 *    Evaluating the test expression without compiling will require expression interpreting.
 *
 * d. alien datatypes
 *    As long as the rtlField class implements the functions then it shouldn't cause any problems.  Evaluating at
 *    from a record at runtime without compiling will be tricky - requires an interpreter.
 *
 * Other
 *  Add a minSize to each field (unless already stored in the record information)
 *
 * Expression interpreting:
 *   Replace the no_select with a CHqlBoundExpr(no_select, fieldid).
 *   Evaluate (ctx [ logical->RtlFieldOffsetCalculator mapping ]).
 *   Even better if mapped direct to something that represents the base cursor so no need to search ctx.
 *   For nested selects the code would need to be consistent.
 */

static unsigned countFields(const RtlFieldInfo * const * fields, bool & containsNested)
{
    unsigned cnt = 0;
    for (;*fields;fields++)
    {
        const RtlTypeInfo * type = (*fields)->type;
        if (type->getType() == type_record)
        {
            containsNested = true;
            const RtlFieldInfo * const * nested = type->queryFields();
            if (nested)
                cnt += countFields(nested, containsNested);
        }
        else
            cnt++;
    }
    return cnt;
}


static unsigned expandNestedRows(unsigned idx, const char *prefix, const RtlFieldInfo * const * fields, const RtlFieldInfo * * target, const char * *names)
{
    for (;*fields;fields++)
    {
        const RtlFieldInfo * cur = *fields;
        const RtlTypeInfo * type = cur->type;
        if (type->getType() == type_record)
        {
            const RtlFieldInfo * const * nested = type->queryFields();
            if (nested)
            {
                StringBuffer newPrefix(prefix);
                newPrefix.append(cur->name).append('.');
                idx = expandNestedRows(idx, newPrefix.str(), nested, target, names);
            }
        }
        else
        {
            if (prefix)
            {
                StringBuffer name(prefix);
                name.append(cur->name);
                names[idx] = name.detach();
            }
            else
                names[idx] = nullptr;
            target[idx++] = cur;
        }
    }
    return idx;
}

class FieldNameToFieldNumMap
{
public:
    FieldNameToFieldNumMap(const RtlRecord &record)
    {
        unsigned numFields = record.getNumFields();
        for (unsigned idx = 0; idx < numFields;idx++)
            map.setValue(record.queryName(idx), idx);
    }
    unsigned lookup(const char *name) const
    {
        unsigned *result = map.getValue(name);
        if (result)
            return *result;
        else
            return (unsigned) -1;
    }
    MapConstStringTo<unsigned> map;  // Note - does not copy strings - they should all have sufficient lifetime
};

RtlRecord::RtlRecord(const RtlRecordTypeInfo & record, bool expandFields)
: RtlRecord(record.fields, expandFields)  // delegated constructor
{
}

RtlRecord::RtlRecord(const RtlFieldInfo * const *_fields, bool expandFields) : fields(_fields), originalFields(_fields), names(nullptr), nameMap(nullptr)
{
    //MORE: Does not cope with ifblocks.
    numVarFields = 0;
    numTables = 0;
    //Optionally expand out nested rows.
    if (expandFields)
    {
        bool containsNested = false;
        numFields = countFields(fields, containsNested);
        if (containsNested)
        {
            const RtlFieldInfo * * allocated  = new const RtlFieldInfo * [numFields+1];
            names = new const char *[numFields];
            fields = allocated;
            unsigned idx = expandNestedRows(0, nullptr, originalFields, allocated, names);
            assertex(idx == numFields);
            allocated[idx] = nullptr;
        }
    }
    else
    {
        numFields = countFields(fields);
    }
    for (unsigned i=0; i < numFields; i++)
    {
        const RtlTypeInfo *curType = queryType(i);
        if (!curType->isFixedSize())
            numVarFields++;
        if (curType->getType()==type_table || curType->getType()==type_record)
            numTables++;
    }

    fixedOffsets = new size_t[numFields + 1];
    whichVariableOffset = new unsigned[numFields + 1];
    variableFieldIds = new unsigned[numVarFields];
    if (numTables)
    {
        nestedTables = new const RtlRecord *[numTables];
        tableIds = new unsigned[numTables];
    }
    else
    {
        nestedTables = nullptr;
        tableIds = nullptr;
    }
    unsigned curVariable = 0;
    unsigned curTable = 0;
    size_t fixedOffset = 0;
    for (unsigned i=0;; i++)
    {
        whichVariableOffset[i] = curVariable;
        fixedOffsets[i] = fixedOffset;
        if (i == numFields)
            break;

        const RtlTypeInfo * curType = queryType(i);
        if (curType->isFixedSize())
        {
            size_t thisSize = curType->size(nullptr, nullptr);
            fixedOffset += thisSize;
        }
        else
        {
            variableFieldIds[curVariable] = i;
            curVariable++;
            fixedOffset = 0;
        }
        switch (curType->getType())
        {
        case type_table:
            tableIds[curTable] = i;
            nestedTables[curTable++] = new RtlRecord(curType->queryChildType()->queryFields(), expandFields);
            break;
        case type_record:
            tableIds[curTable] = i;
            nestedTables[curTable++] = new RtlRecord(curType->queryFields(), expandFields);
            break;
        }
    }
}


RtlRecord::~RtlRecord()
{
    if (names)
    {
        for (unsigned i = 0; i < numFields; i++)
        {
            free((char *) names[i]);
        }
        delete [] names;
    }
    if (fields != originalFields)
    {
        delete [] fields;
    }
    delete [] fixedOffsets;
    delete [] whichVariableOffset;
    delete [] variableFieldIds;
    delete [] tableIds;

    if (nestedTables)
    {
        for (unsigned i = 0; i < numTables; i++)
            delete nestedTables[i];
        delete [] nestedTables;
    }
    delete nameMap;
}

void RtlRecord::calcRowOffsets(size_t * variableOffsets, const void * _row, unsigned numFieldsUsed) const
{
    const byte * row = static_cast<const byte *>(_row);
    unsigned maxVarField = (numFieldsUsed>=numFields) ? numVarFields : whichVariableOffset[numFieldsUsed];
    for (unsigned i = 0; i < maxVarField; i++)
    {
        unsigned fieldIndex = variableFieldIds[i];
        size_t offset = getOffset(variableOffsets, fieldIndex);
        size_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
        variableOffsets[i+1] = offset+fieldSize;
    }
#ifdef _DEBUG
    for (unsigned i = maxVarField; i < numVarFields; i++)
    {
        variableOffsets[i+1] = 0x7fffffff;
    }
#endif
}

size32_t RtlRecord::getMinRecordSize() const
{
    if (numVarFields == 0)
        return fixedOffsets[numFields];

    size32_t minSize = 0;
    for (unsigned i=0; i < numFields; i++)
        minSize += queryType(i)->getMinSize();

    return minSize;
}

static const FieldNameToFieldNumMap *setupNameMap(const RtlRecord &record, std::atomic<const FieldNameToFieldNumMap *> &aNameMap)
{
    const FieldNameToFieldNumMap *lnameMap = new FieldNameToFieldNumMap(record);
    const FieldNameToFieldNumMap *expected = nullptr;
    if (aNameMap.compare_exchange_strong(expected, lnameMap))
        return lnameMap;
    else
    {
        // Other thread already set it while we were creating
        delete lnameMap;
        return expected;  // has been updated to the value set by other thread
    }
}

unsigned RtlRecord::getFieldNum(const char *fieldName) const
{
    // NOTE: the nameMap field cannot be declared as atomic, since the class definition is included in generated
    // code which is not (yet) compiled using C++11. If that changes then the reinterpret_cast can be removed.
    std::atomic<const FieldNameToFieldNumMap *> &aNameMap = reinterpret_cast<std::atomic<const FieldNameToFieldNumMap *> &>(nameMap);
    const FieldNameToFieldNumMap *useMap = aNameMap.load(std::memory_order_relaxed);
    if (!useMap)
        useMap = setupNameMap(*this, aNameMap);
    return useMap->lookup(fieldName);
}

const char *RtlRecord::queryName(unsigned field) const
{
    if (names && names[field])
        return names[field];
    return fields[field]->name;
}

const RtlRecord *RtlRecord::queryNested(unsigned fieldId) const
{
    // Map goes in wrong direction (for size reasons). We could replace with a hashtable or binsearch but
    // should not be enough nested tables for it to be worth it;
    for (unsigned i = 0; i < numTables; i++)
        if (tableIds[i]==fieldId)
            return nestedTables[i];
    return nullptr;
}

//---------------------------------------------------------------------------------------------------------------------

RtlRow::RtlRow(const RtlRecord & _info, const void * optRow, unsigned numOffsets, size_t * _variableOffsets) : info(_info), variableOffsets(_variableOffsets)
{
    assertex(numOffsets == info.getNumVarFields()+1);
    //variableOffset[0] is used for all fixed offset fields to avoid any special casing.
    variableOffsets[0] = 0;
    setRow(optRow);
}

__int64 RtlRow::getInt(unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlTypeInfo * type = info.queryType(field);
    return type->getInt(self + getOffset(field));
}

double RtlRow::getReal(unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlTypeInfo * type = info.queryType(field);
    return type->getReal(self + getOffset(field));
}

void RtlRow::getString(size32_t & resultLen, char * & result, unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlTypeInfo * type = info.queryType(field);
    return type->getString(resultLen, result, self + getOffset(field));
}

void RtlRow::getUtf8(size32_t & resultLen, char * & result, unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlTypeInfo * type = info.queryType(field);
    return type->getUtf8(resultLen, result, self + getOffset(field));
}

void RtlRow::setRow(const void * _row)
{
    row = _row;
    if (_row)
        info.calcRowOffsets(variableOffsets, _row);
}

void RtlRow::setRow(const void * _row, unsigned _numFields)
{
    row = _row;
    if (_row)
        info.calcRowOffsets(variableOffsets, _row, _numFields);
}

RtlDynRow::RtlDynRow(const RtlRecord & _info, const void * optRow) : RtlRow(_info, optRow, _info.getNumVarFields()+1, new size_t[_info.getNumVarFields()+1])
{
}

RtlDynRow::~RtlDynRow()
{
    delete [] variableOffsets;
}

//-----------------

static const RtlRecord *setupRecordAccessor(const COutputMetaData &meta, bool expand, std::atomic<const RtlRecord *> &aRecordAccessor)
{
    const RtlRecord *lRecordAccessor = new RtlRecord(meta.queryTypeInfo()->queryFields(), expand);
    const RtlRecord *expected = nullptr;
    if (aRecordAccessor.compare_exchange_strong(expected, lRecordAccessor))
        return lRecordAccessor;
    else
    {
        // Other thread already set it while we were creating
        delete lRecordAccessor;
        return expected;  // has been updated to the value set by other thread
    }
}

COutputMetaData::COutputMetaData()
{
    recordAccessor[0] = recordAccessor[1] = NULL;
}

COutputMetaData::~COutputMetaData()
{
    delete recordAccessor[0]; delete recordAccessor[1];
}

const RtlRecord &COutputMetaData::queryRecordAccessor(bool expand) const
{
    // NOTE: the recordAccessor field cannot be declared as atomic, since the class definition is included in generated
    // code which is not (yet) compiled using C++11. If that changes then the reinterpret_cast can be removed.
    std::atomic<const RtlRecord *> &aRecordAccessor = reinterpret_cast<std::atomic<const RtlRecord *> &>(recordAccessor[expand]);
    const RtlRecord *useAccessor = aRecordAccessor.load(std::memory_order_relaxed);
    if (!useAccessor)
        useAccessor = setupRecordAccessor(*this, expand, aRecordAccessor);
    return *useAccessor;
}

class CVariableOutputRowSerializer : public COutputRowSerializer
{
public:
    inline CVariableOutputRowSerializer(unsigned _activityId, IOutputMetaData * _meta) : COutputRowSerializer(_activityId) { meta = _meta; }

    virtual void serialize(IRowSerializerTarget & out, const byte * self)
    {
        unsigned size = meta->getRecordSize(self);
        out.put(size, self);
    }

protected:
    IOutputMetaData * meta;
};

class ECLRTL_API CSimpleSourceRowPrefetcher : public ISourceRowPrefetcher, public RtlCInterface
{
public:
    CSimpleSourceRowPrefetcher(IOutputMetaData & _meta, ICodeContext * _ctx, unsigned _activityId)
    {
        deserializer.setown(_meta.querySerializedDiskMeta()->createDiskDeserializer(_ctx, _activityId));
        rowAllocator.setown(_ctx->getRowAllocator(&_meta, _activityId));
    }

    RTLIMPLEMENT_IINTERFACE

    virtual void readAhead(IRowDeserializerSource & in)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t len = deserializer->deserialize(rowBuilder, in);
        rtlReleaseRow(rowBuilder.finalizeRowClear(len));
    }

protected:
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IEngineRowAllocator> rowAllocator;
};

//---------------------------------------------------------------------------


IOutputRowSerializer * COutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId)
{
    return new CVariableOutputRowSerializer(activityId, this);
}

ISourceRowPrefetcher * COutputMetaData::createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
{
    ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher(ctx, activityId);
    if (fetcher)
        return fetcher;
    //Worse case implementation using a deserialize
    return new CSimpleSourceRowPrefetcher(*this, ctx, activityId);
}

ISourceRowPrefetcher *COutputMetaData::defaultCreateDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
{
    if (getMetaFlags() & MDFneedserializedisk)
        return querySerializedDiskMeta()->createDiskPrefetcher(ctx, activityId);
    CSourceRowPrefetcher * fetcher = doCreateDiskPrefetcher(activityId);
    if (fetcher)
    {
        fetcher->onCreate(ctx);
        return fetcher;
    }
    return NULL;
}

IOutputRowSerializer *CFixedOutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowSerializer(activityId, fixedSize);
}

IOutputRowDeserializer *CFixedOutputMetaData::createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowDeserializer(activityId, fixedSize);
}

ISourceRowPrefetcher *CFixedOutputMetaData::createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
{
    ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher(ctx, activityId);
    if (fetcher)
        return fetcher;
    return new CFixedSourceRowPrefetcher(activityId, fixedSize);
}

IOutputRowSerializer * CActionOutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowSerializer(activityId, 0);
}

IOutputRowDeserializer * CActionOutputMetaData::createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowDeserializer(activityId, 0);
}

ISourceRowPrefetcher * CActionOutputMetaData::createDiskPrefetcher(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedSourceRowPrefetcher(activityId, 0);
}


