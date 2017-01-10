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
#include "rtlrecord.hpp"

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

static unsigned countFields(const RtlFieldInfo * const * fields)
{
    unsigned cnt = 0;
    for (;*fields;fields++)
        cnt++;
    return cnt;
}


RtlRecord::RtlRecord(const RtlRecordTypeInfo & record) : fields(record.fields)
{
    //MORE: Does not cope with ifblocks.
    numVarFields = 0;
    numFields = countFields(fields);
    for (unsigned i=0; i < numFields; i++)
    {
        if (!queryType(i)->isFixedSize())
            numVarFields++;
    }

    fixedOffsets = new size_t[numFields + 1];
    whichVariableOffset = new unsigned[numFields + 1];
    variableFieldIds = new unsigned[numVarFields];

    unsigned curVariable = 0;
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
    }
}


RtlRecord::~RtlRecord()
{
    delete [] fixedOffsets;
    delete [] whichVariableOffset;
    delete [] variableFieldIds;
}


void RtlRecord::calcRowOffsets(size_t * variableOffsets, const void * _row) const
{
    const byte * row = static_cast<const byte *>(_row);
    for (unsigned i = 0; i < numVarFields; i++)
    {
        unsigned fieldIndex = variableFieldIds[i];
        size_t offset = getOffset(variableOffsets, fieldIndex);
        size_t fieldSize = queryType(fieldIndex)->size(row + offset, row);
        variableOffsets[i+1] = offset+fieldSize;
    }
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

//---------------------------------------------------------------------------------------------------------------------

RtlRow::RtlRow(const RtlRecord & _info, const void * optRow, size_t * _variableOffsets) : info(_info), variableOffsets(_variableOffsets)
{
    //variableOffset[0] is used for all fixed offset fields to avoid any special casing.
    variableOffsets[0] = 0;
    if (optRow)
        setRow(optRow);
}

__int64 RtlRow::getInt(unsigned field) const
{
    const byte * self = reinterpret_cast<const byte *>(row);
    const RtlTypeInfo * type = info.queryType(field);
    return type->getInt(self + getOffset(field));
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


RtlDynRow::RtlDynRow(const RtlRecord & _info, const void * optRow) : RtlRow(_info, optRow, new size_t[_info.getNumVarFields()+1])
{
}

RtlDynRow::~RtlDynRow()
{
    delete [] variableOffsets;
}

//---------------------------------------------------------------------------------------------------------------------
