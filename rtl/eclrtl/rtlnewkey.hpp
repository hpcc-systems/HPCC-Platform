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

enum TransitionMask : byte
{
    CMPlt = 0x01,
    CMPeq = 0x02,
    CMPgt = 0x04,
    CMPmin = 0x08,      //minimum possible value
    CMPmax = 0x10,      //maximum possible value
    CMPle = CMPlt | CMPeq,
    CMPge = CMPgt | CMPeq,
    CMPminmask = CMPgt|CMPmin,
    CMPmaxmask = CMPlt|CMPmax,
};
BITMASK_ENUM(TransitionMask);

class ValueTransition;
interface RtlTypeInfo;
class RtlRow;

/*
 * The IValueSet interface represents a set of ranges of values.
 *
 * The transitions always come in pairs - an upper and lower bound.  Each bound can be inclusive or exclusive.
 */
interface IValueSet : public IInterface
{
//The following methods are used for creating a valueset
    virtual ValueTransition * createTransition(TransitionMask mask, unsigned __int64 value) const = 0;
    virtual ValueTransition * createStringTransition(TransitionMask mask, size32_t len, const char * value) const = 0;
    virtual ValueTransition * createUtf8Transition(TransitionMask mask, size32_t len, const char * value) const = 0;
    virtual void addRange(ValueTransition * loval, ValueTransition * hival) = 0;
    virtual void addAll() = 0;
    virtual void killRange(ValueTransition * loval, ValueTransition * hival) = 0;
    virtual void reset() = 0;
    virtual void invertSet() = 0;
    virtual void unionSet(const IValueSet *) = 0;
    virtual void excludeSet(const IValueSet *) = 0;
    virtual void intersectSet(const IValueSet *) = 0;
    virtual StringBuffer & serialize(StringBuffer & out) const= 0;

//The following methods are for use once the value set has been created.
    virtual unsigned numRanges() const = 0;

    //find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
    //matchRange is set to the range number, set to numRanges if there is no possible match.  Uses a binary chop
    virtual int findCandidateRange(const byte * field, unsigned & matchRange) const = 0;

    //find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
    //starts searching from curRange (which is likely to match).  Uses a sequential search.
    virtual int checkCandidateRange(const byte * field, unsigned & curRange) const = 0;

    // Does this field match any range?
    virtual bool matches(const byte * field) const = 0;

    // Does this field match this particular range?
    virtual bool matches(const byte * field, unsigned range) const = 0;

    virtual const RtlTypeInfo & queryType() const = 0;
};
extern ECLRTL_API IValueSet * createValueSet(const RtlTypeInfo & type);

interface ISetCreator
{
public:
    virtual void addRange(TransitionMask lowerMask, const StringBuffer & lower, TransitionMask upperMask, const StringBuffer & upperString) = 0;
};

/*
 * Read the textual representation of a set.
 * The format of the set is an optional comma-separated sequence of ranges.
 * Each range is specified as paren lower, upper paren, where the paren is either ( or [ depending
 * on whether the specified bound is inclusive or exclusive.
 * If only one bound is specified then it is used for both upper and lower bound (only meaningful with [] )
 *
 * ( A means values > A - exclusive
 * [ means values >= A - inclusive
 * A ) means values < A - exclusive
 * A ] means values <= A - inclusive
 * For example:
 * [A] matches just A
 * (,A),(A,) matches all but A
 * (A] of [A) are both empty ranges
 * [A,B) means A*
 * Values use the ECL syntax for constants. String constants are always utf8. Binary use d'xx' format (hexpairs)
 *
 * @param creator   The interface that wraps the set that is being created
 * @param filter    The textual representation of the set.
 */
extern ECLRTL_API void deserializeSet(ISetCreator & creator, const char * filter);

/*
 * Read the textual representation of a set.
 *
 * @param set       The target set to be updated.
 * @param filter    The textual representation of the set.
 */
extern ECLRTL_API void deserializeSet(IValueSet & set, const char * filter);


/*
 * This interface represents a filter on a particular field of the row - similar to a segment monitor.
 *
 * Example implementations include single value, sets of ranges, regex or wildcard
 */
interface IFieldFilter : public IInterface
{
public:
//Simple row matching
    virtual bool matches(const RtlRow & row) const = 0;

    virtual int compareRow(const RtlRow & left, const RtlRow & right) const = 0;

    //MORE to come to support index lookups.
    virtual unsigned numRanges() const = 0;
    virtual int findCandidateRange(const RtlRow & row, unsigned & matchRange) const = 0;
    virtual int checkCandidateRange(const RtlRow & row, unsigned & curRange) const = 0;
    virtual bool withinUpperRange(const RtlRow & row, unsigned range) const = 0; // is the row within the current upper limit?
    virtual bool matches(const RtlRow & row, unsigned range) const = 0;
};

//More types of IFieldFilter to come later
extern ECLRTL_API IFieldFilter * createFieldFilter(unsigned fieldId, IValueSet * values);
extern ECLRTL_API IFieldFilter * createWildFieldFilter(unsigned fieldId);

/*
 * The RowFilter class represents a multiple-field filter of a row.
 */
class RowFilter
{
public:
    void addFilter(IFieldFilter & filter);
    bool matches(const RtlRow & row) const;

    int compareRows(const RtlRow & left, const RtlRow & right) const;
    unsigned numFilterFields() const { return filters.ordinality(); }
    const IFieldFilter & queryFilter(unsigned i) const { return filters.item(i); }

protected:
    IArrayOf<IFieldFilter> filters; // for an index must be in field order, and all values present - more thought required
};



#endif
