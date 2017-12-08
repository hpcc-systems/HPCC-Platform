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

#ifndef RTLNEWKEY_INCL
#define RTLNEWKEY_INCL
#include "eclrtl.hpp"

#include "rtlkey.hpp"
#include "rtlrecord.hpp"

BITMASK_ENUM(TransitionMask);

/*
 * The RowFilter class represents a multiple-field filter of a row.
 */

class RowFilter
{
public:
    void addFilter(IFieldFilter & filter);
    bool matches(const RtlRow & row) const;

    void extractKeyFilter(const RtlRecord & record, IArrayOf<IFieldFilter> & keyFilters) const;
    int compareRows(const RtlRow & left, const RtlRow & right) const;
    unsigned numFilterFields() const { return filters.ordinality(); }
    const IFieldFilter & queryFilter(unsigned i) const { return filters.item(i); }

protected:
    IArrayOf<IFieldFilter> filters;
};

//This class represents the current set of values which have been matched in the filter sets.
//A field can either have a valid current value, or it has an index of the next filter range which must match
//for that field.
//Note: If any field is matching a range, then all subsequent fields must be matching the lowest possible filter range
//      Therefore it is only necessary to keep track of the number of exactly matched fields, and which range the next
//      field (if any) is matched against.
class RowCursor
{
public:
    RowCursor(const RtlRecord & record, RowFilter & filter) : currentRow(record, nullptr)
    {
        filter.extractKeyFilter(record, filters);
        ForEachItemIn(i, filters)
            matchedRanges.append(0);
    }

    void selectFirst()
    {
        eos = false;
        numMatched = 0;
        nextUnmatchedRange = 0;
    }

    //Compare the incoming row against the current search row
    int compareNext(const RtlRow & candidate) const
    {
        if (nextSeekIsGT())
        {
            assertex(nextUnmatchedRange == -1U);
            assertex(numMatched != filters.ordinality());
            unsigned i;
            int c = 0;
            //Note field numMatched is not matched, but we are searching for the next highest value => loop until <= numMatched
            for (i = 0; i <= numMatched; i++)
            {
                //Use sequential searches for the values that have previously been matched
                c = queryFilter(i).compareRow(candidate, currentRow);
                if (c != 0)
                    return c;
            }

            //If the next value of the trailing field isn't known then no limit can be placed on the
            //subsequent fields.
            //If it is possible to increment a key value, then it should be going through the compareGE() code instead
            return -1;
        }
        else
        {
            unsigned i;
            for (i = 0; i < numMatched; i++)
            {
                //Use sequential searches for the values that have previously been matched
                int c = queryFilter(i).compareRow(candidate, currentRow);
                if (c != 0)
                    return c;
            }
            unsigned nextRange = nextUnmatchedRange;
            for (; i < numFilterFields(); i++)
            {
                //Compare the row against each of the potential ranges.
                int c = queryFilter(i).compareLowest(candidate, nextRange);
                if (c != 0)
                    return c;
                nextRange = 0;
            }
            return 0;
        }
    }

    //Compare with the row that is larger than the first numMatched fields.

    bool matches() const
    {
        ForEachItemIn(i, filters)
        {
            if (!filters.item(i).matches(currentRow))
                return false;
        }
        return true;
    }

    unsigned numFilterFields() const { return filters.ordinality(); }
    const RtlRow & queryRow() const { return currentRow; }
    bool setRowForward(const byte * row);
    bool nextSeekIsGT() const { return (nextUnmatchedRange == -1U); }
    bool noMoreMatches() const { return eos; }

protected:
    unsigned matchPos(unsigned i) const { return matchedRanges.item(i); }
    bool isValid(unsigned i) const { return matchPos(i) < queryFilter(i).numRanges(); }
    const IFieldFilter & queryFilter(unsigned i) const { return filters.item(i); }
    bool findNextRange(unsigned field);

protected:
    RtlDynRow currentRow;
    unsigned numMatched = 0;
    unsigned nextUnmatchedRange = 0;
    UnsignedArray matchedRanges;
    bool eos = false;
    IArrayOf<IFieldFilter> filters; // for an index must be in field order, and all values present - more thought required
};

interface ISourceRowCursor
{
public:
    //Find the first row that is forward from the search cursor
    virtual const byte * findNext(const RowCursor & current) = 0;
    //select the next row
    virtual const byte * next() = 0;
    //prepare ready for a new set of seeks
    virtual void reset() = 0;
};


#endif
