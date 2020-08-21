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
 *    A field in an ifblock can be viewed as a variable size field - size is either 0 or normal size
 *    depending on the condition in question. We add a flag and a pointer to the ifblock info into
 *    each contained field. Only properly supported in expanded mode.
 *    Might be cleanest to have a different derived implementation which was used if the record
 *    contained ifblocks, though this would mean we would need to make some functions virtual or else move
 *    the responsibility for knowing about ifblocks out to all customers?
 *    Added calls to check ifblocks before size()/getValue() etc. will require an extra row parameter
 *    to every RtlTypeInfo::getX() function? Not if it is required that the offsets have been set first.
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

static unsigned countFields(const RtlFieldInfo * const * fields, bool & containsNested, bool &containsXPaths, unsigned &numIfBlocks)
{
    unsigned cnt = 0;
    for (;*fields;fields++)
    {
        const RtlTypeInfo * type = (*fields)->type;
        if (type->getType() == type_record || type->getType()==type_ifblock)
        {
            containsNested = true;
            if (type->getType()==type_ifblock)
                numIfBlocks++;
            const RtlFieldInfo * const * nested = type->queryFields();
            if (nested)
                cnt += countFields(nested, containsNested, containsXPaths, numIfBlocks);
        }
        else
        {
            if (!containsXPaths && !isEmptyString((*fields)->xpath))
                containsXPaths = true;
            cnt++;
        }
    }
    return cnt;
}

class IfBlockInfo
{
public:
    IfBlockInfo(const RtlFieldInfo &_field, const IfBlockInfo *_parent, unsigned _idx, unsigned _startIdx, unsigned _prevFields)
    : field(_field), parent(_parent), idx(_idx), startIdx(_startIdx), prevFields(_prevFields)
    {
    }

    bool excluded(const byte *row, byte *conditions) const
    {
        if (conditions[idx]==2)
        {
            if (parent && parent->excluded(row, conditions))
                conditions[idx] = 0;
            else
            {
                const RtlIfBlockTypeInfo *cond = static_cast<const RtlIfBlockTypeInfo *>(field.type);
                conditions[idx] = cond->getCondition(row) ? 1 : 0;
            }
        }
        return conditions[idx]==0;
    }
    inline unsigned queryStartField() const { return startIdx; }
    inline unsigned numPrevFields() const { return prevFields; }
    inline bool matchIfBlock(const RtlIfBlockTypeInfo * ifblock) const { return ifblock == field.type; }
private:
    const RtlFieldInfo &field;
    const IfBlockInfo *parent = nullptr;  // for nested ifblocks
    unsigned idx;
    unsigned startIdx; // For ifblocks inside child records
    unsigned prevFields; // number of fields before the if block
};

class RtlCondFieldStrInfo : public RtlFieldStrInfo
{
public:
    RtlCondFieldStrInfo(const RtlFieldInfo &from, const IfBlockInfo &_ifblock, bool forcePayload)
    : RtlFieldStrInfo(from.name, from.xpath, from.type, from.flags | (forcePayload ? RFTMinifblock|RFTMdynamic|RFTMispayloadfield : RFTMinifblock|RFTMdynamic), (const char *) from.initializer),
      origField(from),ifblock(_ifblock)
    {
    }
public:
    const RtlFieldInfo &origField;
    const IfBlockInfo &ifblock;
};

static unsigned expandNestedRows(unsigned idx, unsigned startIdx, StringBuffer &prefix, StringBuffer &xPathPrefix, const RtlFieldInfo * const * fields, const RtlFieldInfo * * target, const char * *names, const char * *xpaths, const IfBlockInfo *inIfBlock, ConstPointerArrayOf<IfBlockInfo> &ifblocks, bool forcePayload)
{
    for (;*fields;fields++)
    {
        const RtlFieldInfo * cur = *fields;
        const RtlTypeInfo * type = cur->type;
        bool isIfBlock  = type->getType()==type_ifblock;
        if (isIfBlock || type->getType() == type_record)
        {
            const IfBlockInfo *nestIfBlock = inIfBlock;
            if (isIfBlock)
            {
                nestIfBlock = new IfBlockInfo(*cur, inIfBlock, ifblocks.ordinality(), startIdx, idx);
                ifblocks.append(nestIfBlock);
            }
            const RtlFieldInfo * const * nested = type->queryFields();
            if (nested)
            {
                size32_t prevPrefixLength = prefix.length();
                if (!isEmptyString(cur->name))
                    prefix.append(cur->name).append('.');

                size32_t prevXPathPrefixLength = xPathPrefix.length();
                if (xpaths)
                {
                    const char *xpath = cur->queryXPath();
                    if (!isEmptyString(xpath))
                        xPathPrefix.append(xpath).append('/');
                }
                idx = expandNestedRows(idx, isIfBlock ? startIdx : idx, prefix, xPathPrefix, nested, target, names, xpaths, nestIfBlock, ifblocks, forcePayload || (cur->flags & RFTMispayloadfield) != 0);
                prefix.setLength(prevPrefixLength);
                if (xpaths)
                    xPathPrefix.setLength(prevXPathPrefixLength);
            }
        }
        else
        {
            if (prefix)
            {
                StringBuffer name(prefix);
                name.append(cur->name);
                names[idx] = name.detach();
                if (xpaths)
                {
                    StringBuffer xpath(xPathPrefix);
                    xpath.append(cur->queryXPath());
                    xpaths[idx] = xpath.detach();
                }
            }
            else
            {
                names[idx] = nullptr;
                if (xpaths)
                    xpaths[idx] = nullptr;
            }
            if (inIfBlock && !(cur->flags & RFTMinifblock))
                target[idx++] = new RtlCondFieldStrInfo(*cur, *inIfBlock, forcePayload);
            else if (forcePayload && !(cur->flags & RFTMispayloadfield))
            {
                dbgassertex((cur->flags & RFTMdynamic) == 0);
                target[idx++] = new RtlFieldStrInfo(cur->name, cur->xpath, cur->type, cur->flags | (RFTMispayloadfield|RFTMdynamic), (const char * ) cur->initializer);
            }
            else
            {
                dbgassertex((cur->flags & RFTMdynamic) == 0);
                target[idx++] = cur;
            }
        }
    }
    return idx;
}

class FieldNameToFieldNumMap
{
public:
    FieldNameToFieldNumMap(const RtlRecord &record) : map(true)
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

RtlRecord::RtlRecord(const RtlFieldInfo * const *_fields, bool expandFields) : fields(_fields), originalFields(_fields), names(nullptr), xpaths(nullptr), nameMap(nullptr)
{
    numVarFields = 0;
    numTables = 0;
    numIfBlocks = 0;
    ifblocks = nullptr;
    //Optionally expand out nested rows.
    if (expandFields)
    {
        bool containsNested = false;
        bool containsXPaths = false;
        numFields = countFields(fields, containsNested, containsXPaths, numIfBlocks);
        if (containsNested)
        {
            ConstPointerArrayOf<IfBlockInfo> _ifblocks;
            const RtlFieldInfo * * allocated  = new const RtlFieldInfo * [numFields+1];
            names = new const char *[numFields];
            if (containsXPaths)
                xpaths = new const char *[numFields];
            fields = allocated;
            StringBuffer prefix, xPathPrefix;
            unsigned idx = expandNestedRows(0, 0, prefix, xPathPrefix, originalFields, allocated, names, xpaths, nullptr, _ifblocks, false);
            ifblocks = _ifblocks.detach();
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
        if (!curType->isFixedSize() || (fields[i]->flags & RFTMinifblock))
            numVarFields++;
        if (curType->getType()==type_table || curType->getType()==type_record || curType->getType()==type_dictionary)
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
        if (curType->isFixedSize() && !(fields[i]->flags & RFTMinifblock))
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
        case type_dictionary:
            tableIds[curTable] = i;
            nestedTables[curTable++] = new RtlRecord(curType->queryChildType()->queryFields(), expandFields);
            break;
        case type_record:
            tableIds[curTable] = i;
            nestedTables[curTable++] = new RtlRecord(curType->queryFields(), expandFields);
            break;
        }
    }

    //Zero length records cause problems (allocation, and indicating if skipped) => force the length to 1 byte so it is possible to tell
    if (numFields == 0)
        fixedOffsets[0] = 1;
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
    if (xpaths)
    {
        for (unsigned i = 0; i < numFields; i++)
        {
            free((char *) xpaths[i]);
        }
        delete [] xpaths;
    }
    if (fields != originalFields)
    {
        for (const RtlFieldInfo * const * finger = fields; *finger; finger++)
        {
            const RtlFieldInfo *thisField = *finger;
            if (thisField->flags & RFTMdynamic)
                delete thisField;
        }
        delete [] fields;
    }
    if (ifblocks)
    {
        for (unsigned i = 0; i < numIfBlocks; i++)
        {
            delete(ifblocks[i]);
        }
        //following as allocated as a ConstPointerArrayOf<IfBlockInfo>, rather than new []
        free(ifblocks);
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

unsigned RtlRecord::getNumKeyedFields() const
{
    unsigned ret = 0;
    for (const RtlFieldInfo * const * finger = originalFields; *finger; finger++)
    {
        if ((*finger)->flags & RFTMispayloadfield)
            break;
        ret++;
    }
    return ret;
}

void RtlRecord::calcRowOffsets(size_t * variableOffsets, const void * _row, unsigned numFieldsUsed) const
{
    const byte * row = static_cast<const byte *>(_row);
    unsigned maxVarField = (numFieldsUsed>=numFields) ? numVarFields : whichVariableOffset[numFieldsUsed];
    if (numIfBlocks)
    {
        byte *conditions = (byte *) alloca(numIfBlocks * sizeof(byte));
        memset(conditions, 2, numIfBlocks);    // Meaning condition not yet calculated
        for (unsigned i = 0; i < maxVarField; i++)
        {
            unsigned fieldIndex = variableFieldIds[i];
            const RtlFieldInfo *field = fields[fieldIndex];
            size_t offset = getOffset(variableOffsets, fieldIndex);
            if (field->flags & RFTMinifblock)
            {
                const RtlCondFieldStrInfo *condfield = static_cast<const RtlCondFieldStrInfo *>(field);
                unsigned startField = condfield->ifblock.queryStartField();
                const byte *childRow = row;
                if (startField)
                    childRow += getOffset(variableOffsets, startField);
                if (condfield->ifblock.excluded(childRow, conditions))
                {
                    variableOffsets[i+1] = offset; // (meaning size ends up as zero);
                    continue;
                }
            }
            size_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
            variableOffsets[i+1] = offset+fieldSize;
        }
    }
    else
    {
        size32_t varoffset = 0;
        for (unsigned i = 0; i < maxVarField; i++)
        {
            unsigned fieldIndex = variableFieldIds[i];
            size32_t offset = fixedOffsets[fieldIndex] + varoffset;
            size32_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
            varoffset = offset+fieldSize;
            variableOffsets[i+1] = varoffset;
        }
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
    {
        const RtlFieldInfo *field = fields[i];
        if (!(field->flags & RFTMinifblock))
            minSize += queryType(i)->getMinSize();
    }
    return minSize;
}

size32_t RtlRecord::deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in) const
{
    size32_t offset = 0;
    byte *conditionValues = (byte *) alloca(numIfBlocks);
    memset(conditionValues, 2, numIfBlocks);
    for (unsigned i = 0; i < numVarFields; i++)
    {
        unsigned fieldIndex = variableFieldIds[i];
        const RtlFieldInfo *field = fields[fieldIndex];
        size32_t fixedSize = fixedOffsets[fieldIndex];
        byte * self = rowBuilder.ensureCapacity(offset + fixedSize, ""); // Why not field->name?
        in.read(fixedSize, self + offset);
        if (excluded(field, self, conditionValues))
            continue;
        offset = queryType(fieldIndex)->deserialize(rowBuilder, in, offset + fixedSize);
    }
    size32_t lastFixedSize = fixedOffsets[numFields];
    byte * self = rowBuilder.ensureCapacity(offset + lastFixedSize, "");
    in.read(lastFixedSize, self + offset);
    return offset + lastFixedSize;
}

void RtlRecord::readAhead(IRowPrefetcherSource & in) const
{
    // Note - this should not and can not be used when ifblocks present - canprefetch flag should take care of that.
    if (numIfBlocks==0)
    {
        for (unsigned i=0; i < numVarFields; i++)
        {
            unsigned fieldIndex = variableFieldIds[i];
            in.skip(fixedOffsets[fieldIndex]);
            queryType(fieldIndex)->readAhead(in);
        }
        in.skip(fixedOffsets[numFields]);
    }
    else
    {
        const RtlFieldInfo * const * field;
        for (field = originalFields; *field; field++)
            (*field)->type->readAhead(in);
    }
}

int RtlRecord::compare(const byte * left, const byte * right) const
{
    //Use originalFields so that ifblocks are processed correctly
    return compareFields(originalFields, left, right, false);
}

unsigned RtlRecord::queryIfBlockLimit(const RtlIfBlockTypeInfo * ifblock) const
{
    for (unsigned i=0; i < numIfBlocks; i++)
    {
        if (ifblocks[i]->matchIfBlock(ifblock))
            return ifblocks[i]->numPrevFields();
    }
    throwUnexpected();
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

const char *RtlRecord::queryXPath(unsigned field) const // NB: returns name if no xpath
{
    if (xpaths && xpaths[field])
        return xpaths[field];
    return fields[field]->queryXPath();
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

bool RtlRecord::hasNested() const
{
    return fields != originalFields;
}

const RtlFieldInfo * RtlRecord::queryOriginalField(const char *fieldName) const
{
    // Used when setting up then checking unusual translation scenarios - doesn't need to be lightning-fast
    for (const RtlFieldInfo * const * finger = originalFields; *finger; finger++)
    {
        const char *srcName = (*finger)->name;
        if (srcName && strieq(fieldName, srcName))
            return *finger;
    }
    return nullptr;
}

const RtlFieldInfo *RtlRecord::queryOriginalField(unsigned idx) const
{
    const RtlFieldInfo *field = queryField(idx);
    if (field->flags & RFTMinifblock)
        return &static_cast<const RtlCondFieldStrInfo *>(field)->origField;
    else
        return field;
}


bool RtlRecord::excluded(const RtlFieldInfo *field, const byte *row, byte *conditionValues) const
{
    if (!field->omitable())
        return false;
    const RtlCondFieldStrInfo *condfield = static_cast<const RtlCondFieldStrInfo *>(field);
    unsigned startField = condfield->ifblock.queryStartField();
    const byte *childRow = row;
    if (startField)
        childRow += calculateOffset(row, startField);
    return condfield->ifblock.excluded(childRow, conditionValues);
}

size_t RtlRecord::getFixedOffset(unsigned field) const
{
    assert(isFixedOffset(field));
    return fixedOffsets[field];
}

bool RtlRecord::isFixedOffset(unsigned field) const
{
    return (whichVariableOffset[field]==0);
}

size32_t RtlRecord::getRecordSize(const void *_row) const
{
    if (numIfBlocks)
    {
        unsigned numOffsets = getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow sourceRow(*this, _row, numOffsets, variableOffsets);
        return sourceRow.getOffset(numFields+1);
    }
    else
    {
        size32_t size = getFixedSize();
        if (!size)
        {
            const byte * row = static_cast<const byte *>(_row);
            size32_t varoffset = 0;
            for (unsigned i = 0; i < numVarFields; i++)
            {
                unsigned fieldIndex = variableFieldIds[i];
                size32_t offset = fixedOffsets[fieldIndex] + varoffset;
                size32_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
                varoffset = offset + fieldSize;
            }
            size = fixedOffsets[numFields] + varoffset;
        }
        return size;
    }
}

size32_t RtlRecord::calculateOffset(const void *_row, unsigned field) const
{
    if (numIfBlocks)
    {
        unsigned numOffsets = getNumVarFields() + 1;
        size_t * variableOffsets = (size_t *)alloca(numOffsets * sizeof(size_t));
        RtlRow sourceRow(*this, nullptr, numOffsets, variableOffsets);
        sourceRow.setRow(_row, field);
        return sourceRow.getOffset(field);
    }
    else
    {
        const byte * row = static_cast<const byte *>(_row);
        size32_t varoffset = 0;
        unsigned varFields = whichVariableOffset[field];
        for (unsigned i = 0; i < varFields; i++)
        {
            unsigned fieldIndex = variableFieldIds[i];
            size32_t offset = fixedOffsets[fieldIndex] + varoffset;
            size32_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
            varoffset = offset + fieldSize;
        }
        return fixedOffsets[field] + varoffset;
    }
}

//---------------------------------------------------------------------------------------------------------------------

RtlRow::RtlRow(const RtlRecord & _info, const void * optRow, unsigned numOffsets, size_t * _variableOffsets) : info(_info), variableOffsets(_variableOffsets)
{
    assertex(numOffsets == info.getNumVarFields()+1);
    //variableOffset[0] is used for all fixed offset fields to avoid any special casing.
    variableOffsets[0] = 0;
    setRow(optRow);
}

size_t RtlRow::noVariableOffsets [1] = {0};

RtlRow::RtlRow(const RtlRecord & _info, const void *_row) : info(_info), variableOffsets(noVariableOffsets)
{
    row = (const byte *)_row;

}

__int64 RtlRow::getInt(unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlFieldInfo *fieldInfo = info.queryField(field);
    const RtlTypeInfo * type = fieldInfo->type;
    if (!fieldInfo->omitable() || getSize(field))
        return type->getInt(self + getOffset(field));
    else if (fieldInfo->initializer && !isVirtualInitializer(fieldInfo->initializer))
        return type->getInt(fieldInfo->initializer);
    else
        return 0;
}

double RtlRow::getReal(unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlFieldInfo *fieldInfo = info.queryField(field);
    const RtlTypeInfo * type = fieldInfo->type;
    if (!fieldInfo->omitable() || getSize(field))
        return type->getReal(self + getOffset(field));
    else if (fieldInfo->initializer && !isVirtualInitializer(fieldInfo->initializer))
        return type->getReal(fieldInfo->initializer);
    else
        return 0;
}

void RtlRow::getString(size32_t & resultLen, char * & result, unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlFieldInfo *fieldInfo = info.queryField(field);
    const RtlTypeInfo * type = fieldInfo->type;
    if (!fieldInfo->omitable() || getSize(field))
        type->getString(resultLen, result, self + getOffset(field));
    else if (fieldInfo->initializer && !isVirtualInitializer(fieldInfo->initializer))
        type->getString(resultLen, result, fieldInfo->initializer);
    else
    {
        resultLen = 0;
        result = nullptr;
    }
}

void RtlRow::getUtf8(size32_t & resultLen, char * & result, unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlFieldInfo *fieldInfo = info.queryField(field);
    const RtlTypeInfo * type = fieldInfo->type;
    if (!fieldInfo->omitable() || getSize(field))
        type->getUtf8(resultLen, result, self + getOffset(field));
    else if (fieldInfo->initializer && !isVirtualInitializer(fieldInfo->initializer))
        type->getUtf8(resultLen, result, fieldInfo->initializer);
    else
    {
        resultLen = 0;
        result = nullptr;
    }
}

void RtlRow::setRow(const void * _row, unsigned _numFieldsUsed)
{
    row = (const byte *)_row;
    if (_row)
    {
        numFieldsUsed = _numFieldsUsed;
        if (numFieldsUsed)
            info.calcRowOffsets(variableOffsets, _row, _numFieldsUsed);
#if defined(_DEBUG) && defined(TRACE_ROWOFFSETS)
        for (unsigned i = 0; i < info.getNumFields() && i < numFieldsUsed; i++)
        {
            printf("Field %d (%s) offset %d", i, info.queryName(i), (int) getOffset(i));
            if (getSize(i))
            {
                unsigned bufflen;
                rtlDataAttr buff;
                getString(bufflen, buff.refstr(), i);
                printf(" value %.*s", bufflen, buff.getstr());
            }
            printf("\n");
        }
#endif
    }
    else
        numFieldsUsed = 0;
}

void RtlRow::lazyCalcOffsets(unsigned _numFieldsUsed) const
{
    // This is a little iffy as it's not really const - but it clears up a lot of other code if you
    // treat it as if it is. Logically it kind-of is, in that we are doing lazy-evaluation of the
    // offsets but logically we are not creating information here.
    // Another alternative would be to do the lazy eval in getOffset/getRecordSize ?
    assert(row);
    if (_numFieldsUsed > numFieldsUsed)
    {
        info.calcRowOffsets(variableOffsets, row, _numFieldsUsed); // MORE - could be optimized to only calc ones not previously calculated
        numFieldsUsed = _numFieldsUsed;
    }
}

RtlFixedRow::RtlFixedRow(const RtlRecord & _info, const void *_row, unsigned _numFieldsUsed)
: RtlRow(_info, _row)
{
    numFieldsUsed = _numFieldsUsed;
    dbgassertex(info.isFixedOffset(numFieldsUsed));
}

RtlDynRow::RtlDynRow(const RtlRecord & _info, const void * optRow) : RtlRow(_info, optRow, _info.getNumVarFields()+1, new size_t[_info.getNumVarFields()+1])
{
}

RtlDynRow::~RtlDynRow()
{
    delete [] variableOffsets;
}


//---------------------------------------------------------------------------------------------------------------------

class CDefaultDeserializer : public CInterfaceOf<IOutputRowDeserializer>
{
public:
    CDefaultDeserializer(const RtlRecord & _record) : record(_record) {}

    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in)
    {
        return record.deserialize(rowBuilder, in);
    }
private:
    const RtlRecord & record;
};


class CDefaultPrefetcher : public CInterfaceOf<ISourceRowPrefetcher>
{
public:
    CDefaultPrefetcher(const RtlRecord & _record) : record(_record) {}

    virtual void readAhead(IRowPrefetcherSource & in)
    {
        record.readAhead(in);
    }
private:
    const RtlRecord & record;
};



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
    // code which does not include <atomic>. If that changes then the reinterpret_cast can be removed.
    std::atomic<const RtlRecord *> &aRecordAccessor = reinterpret_cast<std::atomic<const RtlRecord *> &>(recordAccessor[expand]);
    const RtlRecord *useAccessor = aRecordAccessor.load(std::memory_order_relaxed);
    if (!useAccessor)
        useAccessor = setupRecordAccessor(*this, expand, aRecordAccessor);
    return *useAccessor;
}

size32_t COutputMetaData::getRecordSize(const void * data)
{
    return queryRecordAccessor(true).getRecordSize(data);
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

//---------------------------------------------------------------------------


IOutputRowSerializer * COutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId)
{
    return new CVariableOutputRowSerializer(activityId, this);
}

ISourceRowPrefetcher * COutputMetaData::createDiskPrefetcher()
{
    ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher();
    if (fetcher)
        return fetcher;
    return new CDefaultPrefetcher(queryRecordAccessor(true));
}

IOutputRowDeserializer *COutputMetaData::createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
{
    return new CDefaultDeserializer(queryRecordAccessor(true));
}

ISourceRowPrefetcher *COutputMetaData::defaultCreateDiskPrefetcher()
{
    if (getMetaFlags() & MDFneedserializedisk)
        return querySerializedDiskMeta()->createDiskPrefetcher();
    CSourceRowPrefetcher * fetcher = doCreateDiskPrefetcher();
    if (fetcher)
    {
        fetcher->onCreate();
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

ISourceRowPrefetcher *CFixedOutputMetaData::createDiskPrefetcher()
{
    ISourceRowPrefetcher * fetcher = defaultCreateDiskPrefetcher();
    if (fetcher)
        return fetcher;
    return new CFixedSourceRowPrefetcher(fixedSize);
}

IOutputRowSerializer * CActionOutputMetaData::createDiskSerializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowSerializer(activityId, 0);
}

IOutputRowDeserializer * CActionOutputMetaData::createDiskDeserializer(ICodeContext * ctx, unsigned activityId)
{
    return new CFixedOutputRowDeserializer(activityId, 0);
}

ISourceRowPrefetcher * CActionOutputMetaData::createDiskPrefetcher()
{
    return new CFixedSourceRowPrefetcher(0);
}


