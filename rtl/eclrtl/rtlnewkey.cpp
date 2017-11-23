/*##############################################################################


    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR getValue OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include <initializer_list>
#include "jlib.hpp"
#include "jsort.hpp"
#include "jexcept.hpp"
#include "rtlnewkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"

//move to jstring as part of HPCC-18547
/*
 * Read a single quoted string from in until the terminating quote is found
 *
 * @param out       The resulting string with special quote characters resolved.
 * @param in        A reference to the start of the string.  Updated to point to the end of the string.
 */
static void readString(StringBuffer &out, const char * &in)
{
    for (;;)
    {
        char c = *in++;
        if (!c)
            throw MakeStringException(0, "Invalid filter - missing closing '");
        if (c=='\'')
            break;
        if (c=='\\')
        {
            c = *in++;
            switch (c)
            {
            case '\'':
            case '\\':
                break;
            default:
                UNIMPLEMENTED; // HPCC-18547
            }
        }
        out.append(c);
    }
}

/*
 * Read a single string from in until a matching terminator is found
 *
 * @param out       The resulting string.
 * @param in        A reference to the start of the string.  Updated to point to the end of the string.
 * @param term      Characters that can serve as terminators.
 */
static void readUntilTerminator(StringBuffer & out, const char * & in, const char * terminators)
{
    const char * start = in;
    const char * pbrk = strpbrk(start, terminators);
    if (!pbrk)
        throw makeStringExceptionV(0, "Invalid filter - expected terminator '%s'", terminators);
    out.append(pbrk - start, start);
    in = pbrk;
}


void deserializeSet(ISetCreator & creator, const char * filter)
{
    while (*filter)
    {
        char startRange = *filter++;
        if (startRange != '(' && startRange != '[')
            throw MakeStringException(0, "Invalid filter string: expected [ or ( at start of range");

        StringBuffer upperString, lowerString;
        if (*filter=='\'')
        {
            filter++;
            readString(lowerString, filter);
        }
        else
            readUntilTerminator(lowerString, filter, ",])");

        if (*filter == ',')
        {
            filter++;
            if (*filter=='\'')
            {
                filter++;
                readString(upperString, filter);
            }
            else
                readUntilTerminator(upperString, filter, "])");
        }
        else
            upperString.set(lowerString);

        char endRange = *filter++;
        if (endRange != ')' && endRange != ']')
            throw MakeStringException(0, "Invalid filter string: expected ] or ) at end of range");
        if (*filter==',')
            filter++;
        else if (*filter)
            throw MakeStringException(0, "Invalid filter string: expected , between ranges");

        TransitionMask lowerMask = (startRange == '(') ? CMPgt : CMPge;
        TransitionMask upperMask = (endRange == ')') ? CMPlt : CMPle;
        creator.addRange(lowerMask, lowerString, upperMask, upperString);
    }
}

// A class wrapped for adding ranges to IValueSets
class ValueSetCreator : implements ISetCreator
{
public:
    ValueSetCreator(IValueSet & _set) : set(_set) {}

    virtual void addRange(TransitionMask lowerMask, const StringBuffer & lowerString, TransitionMask upperMask, const StringBuffer & upperString) override
    {
        Owned<ValueTransition> lower = lowerString ? set.createUtf8Transition(lowerMask, rtlUtf8Length(lowerString.length(), lowerString), lowerString) : nullptr;
        Owned<ValueTransition> upper = upperString ? set.createUtf8Transition(upperMask, rtlUtf8Length(upperString.length(), upperString), upperString) : nullptr;
        set.addRange(lower, upper);
    }

protected:
    IValueSet & set;
};



void deserializeSet(IValueSet & set, const char * filter)
{
    ValueSetCreator creator(set);
    deserializeSet(creator, filter);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * This class represents a value and a comparison condition and is used for representing ranges of values
 *
 * A lowerbound will always have the CMPgt bit set in the mask, and upper bound will have CMPlt set in the mask.
 * The value is always represented in the same way as a field of that type would be in a record.
 */

class ValueTransition : implements CInterface
{
public:
    ValueTransition(TransitionMask _mask, const RtlTypeInfo & type, const void *_value)
    {
        mask = _mask;
        dbgassertex(_value || isMinimum() || isMaximum());
        if (_value)
        {
            size32_t size = type.size((const byte *)_value, nullptr);
            value.allocateN(size, false);
            memcpy(value, _value, size);
        }
    }
    ValueTransition(TransitionMask _mask) : mask(_mask)
    {
        dbgassertex(isMaximum() || isMinimum());
    }

    MemoryBuffer &serialize(const RtlTypeInfo & type, MemoryBuffer &mb) const
    {
        mb.append((byte)mask);
        size32_t size = type.size(value, nullptr);
        memcpy(mb.reserve(size), value, size);
        return mb;
    }

    int compareRaw(const RtlTypeInfo & type, const byte * field) const
    {
        if (!value)
        {
            if (mask & CMPmin)
                return +1;
            if (mask & CMPmax)
                return -1;
        }
        return type.compare(field, value.get());
    }

    int compare(const RtlTypeInfo & type, const byte * field) const
    {
        int c = compareRaw(type, field);
        if (c == 0)
        {
            if (mask & CMPeq)
                return 0;
            //Lower bound
            if (mask & CMPgt)
                return -1;
            //Check against upper bound
            if (mask & CMPlt)
                return +1;
            throwUnexpected();
        }
        return c;
    }

    int compareRaw(const RtlTypeInfo & type, const ValueTransition & other) const
    {
        if (!value || !other.value)
        {
            if (isMinimum())
                return other.isMinimum() ? 0 : -1;

            if (isMaximum())
                return other.isMaximum() ? 0 : +1;

            if (other.isMinimum())
                return +1;

            if (other.isMaximum())
                return -1;

            throwUnexpected();
        }

        return type.compare(value, other.value);
    }


    StringBuffer & describe(const RtlTypeInfo & type, StringBuffer & out) const
    {
        if (mask & CMPgt)
        {
            if (mask & CMPeq)
                out.append("[");
            else
                out.append("(");
        }

        if (value)
        {
            size32_t len;
            rtlDataAttr text;
            type.getUtf8(len, text.refstr(), value);
            size32_t size = rtlUtf8Size(len, text.getstr());
            if (type.isNumeric())
            {
                out.append(size, text.getstr());
            }
            else
            {
                out.append("'");
                appendUtf8AsECL(out, size, text.getstr());
                out.append("'");
            }
        }

        if (mask & CMPlt)
        {
            if (mask & CMPeq)
                out.append("]");
            else
                out.append(")");
        }
        return out;
    }

    bool isPreviousBound(const RtlTypeInfo & type, const ValueTransition & other) const
    {
        if (isInclusiveBound() || other.isInclusiveBound())
        {
            if (compareRaw(type, other) == 0)
                return true;
        }
        if (isInclusiveBound() && other.isInclusiveBound())
        {
            //MORE: if isPreviousValue(other)
            //if (oneless(hival, t.getValue()))
        }
        return false;
    }

    //If this transition isn't shared then modify it directly, otherwise clone.  Always return a linked object.
    ValueTransition * modifyTransition(const RtlTypeInfo & type, TransitionMask newMask)
    {
        assertex(value);
        if (IsShared())
        {
            return new ValueTransition(newMask, type, value);
        }
        else
        {
            mask = newMask;
            return LINK(this);
        }
    }

    ValueTransition * modifyInverse(const RtlTypeInfo & type)
    {
        TransitionMask newMask = (TransitionMask)(mask ^ (CMPgt|CMPlt|CMPeq));
        return modifyTransition(type, newMask);
    }

    bool isLowerBound() const { return (mask & CMPgt) != 0; }
    bool isUpperBound() const { return (mask & CMPlt) != 0; }
    bool isInclusiveBound() const { return (mask & CMPeq) != 0; }
    bool isMinimum() const { return (mask & CMPmin) != 0; }
    bool isMaximum() const { return (mask & CMPmax) != 0; }

private:
    TransitionMask mask;
    OwnedMalloc<byte> value;
};

typedef CIArrayOf<ValueTransition> ValueTransitionArray;


//---------------------------------------------------------------------------------------------------------------------

/*
 * The ValueSet class represents a set of ranges of values.
 *
 * The transitions always come in pairs - an upper and lower bound.  Each bound can be inclusive or exclusive.
 */
class ValueSet : public CInterfaceOf<IValueSet>
{
public:
    ValueSet(const RtlTypeInfo & _type) : type(_type)
    {
    }

// Methods for creating a value set
    virtual ValueTransition * createTransition(TransitionMask mask, unsigned __int64 value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildInt(builder, 0, nullptr, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }
    virtual ValueTransition * createStringTransition(TransitionMask mask, size32_t len, const char * value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildString(builder, 0, nullptr, len, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }
    virtual ValueTransition * createUtf8Transition(TransitionMask mask, size32_t len, const char * value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildUtf8(builder, 0, nullptr, len, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }

    virtual void addRange(ValueTransition * lower, ValueTransition * upper) override
    {
        Owned<ValueTransition> minBound;
        Owned<ValueTransition> maxBound;
        if (!lower)
        {
            minBound.setown(new ValueTransition(CMPminmask));
            lower = minBound;
        }
        if (!upper)
        {
            maxBound.setown(new ValueTransition(CMPmaxmask));
            upper = maxBound;
        }

        if (!lower->isLowerBound() || !upper->isUpperBound())
            throw MakeStringException(1, "Invalid range bounds");

        //If lower > upper then it is an empty range
        int rc = lower->compareRaw(type, *upper);
        if (rc > 0)
            return;

        //Check for exclusive ranges for a single value
        if (rc == 0)
        {
            if (!lower->isInclusiveBound() || !upper->isInclusiveBound())
                return;
        }

        // binchop to find last transition > val...
        unsigned int low = 0;
        unsigned high = transitions.ordinality();
        while (low < high)
        {
            unsigned mid = low + (high - low) / 2;
            int rc = lower->compareRaw(type, transitions.item(mid));
            if (rc <= 0)
                high = mid;
            else
                low = mid+1;
        }

        unsigned idx;
        if (transitions.isItem(low))
        {
            ValueTransition & next = transitions.item(low);
            if (lower->compareRaw(type, next) == 0)
            {
                if (next.isLowerBound())
                {
                    if (!next.isInclusiveBound() && lower->isInclusiveBound())
                        transitions.replace(*LINK(lower), low);
                    idx = low+1;
                }
                else
                {
                    if (next.isInclusiveBound() || lower->isInclusiveBound())
                    {
                        transitions.remove(low);
                        idx = low;
                    }
                    else
                    {
                        transitions.add(*LINK(lower), low+1);
                        idx = low + 2;
                    }
                }
            }
            else
            {
                if (next.isLowerBound())
                {
                    transitions.add(*LINK(lower), low);
                    idx = low + 1;
                }
                else
                {
                    //previous must exist and be lower => ignore this one
                    idx = low;
                }
            }
        }
        else
        {
            transitions.append(*LINK(lower));
            transitions.append(*LINK(upper));
            return;
        }


        //Walk the remaining transitions until you find one that is higher than the current one (or run out)
        while (transitions.isItem(idx))
        {
            ValueTransition & cur = transitions.item(idx);
            int rc = cur.compareRaw(type, *upper);
            if (rc > 0)
            {
                if (cur.isUpperBound())
                    return;
                break;
            }
            if (rc == 0)
            {
                if (cur.isUpperBound())
                {
                    if (!cur.isInclusiveBound() && upper->isInclusiveBound())
                        transitions.replace(*LINK(upper), idx);
                    return;
                }

                //Upper value matches the next lower bound - could either remove the next item if either is inclusive, otherwise add
                if (cur.isInclusiveBound() || upper->isInclusiveBound())
                {
                    transitions.remove(idx);
                    return;
                }
                break;
            }
            transitions.remove(idx);
        }

        if (transitions.isItem(idx))
        {
            ValueTransition & cur = transitions.item(idx);
            assertex(cur.isLowerBound());
            if (upper->isPreviousBound(type, cur))
            {
                transitions.remove(idx);
                return;
            }
        }
        transitions.add(*LINK(upper), idx);
    }
    virtual void addAll() override
    {
        reset();
        addRange(nullptr, nullptr);
    }
    virtual void killRange(ValueTransition * lower, ValueTransition * upper) override
    {
        Owned<ValueTransition> minBound;
        Owned<ValueTransition> maxBound;
        if (!lower)
        {
            minBound.setown(new ValueTransition(CMPminmask));
            lower = minBound;
        }
        if (!upper)
        {
            maxBound.setown(new ValueTransition(CMPmaxmask));
            upper = maxBound;
        }

        if (!lower->isLowerBound() || !upper->isUpperBound())
            throw MakeStringException(1, "Invalid range bounds");

        //If lower > upper then it is an empty range
        int rc = lower->compareRaw(type, *upper);
        if (rc > 0)
            return;

        //Check for exclusive ranges for a single value
        if (rc == 0)
        {
            if (!lower->isInclusiveBound() || !upper->isInclusiveBound())
                return;
        }

        // binchop to find last transition > val...
        unsigned int low = 0;
        unsigned high = transitions.ordinality();
        while (low < high)
        {
            unsigned mid = low + (high - low) / 2;
            int rc = lower->compareRaw(type, transitions.item(mid));
            if (rc <= 0)
                high = mid;
            else
                low = mid+1;
        }

        unsigned idx = low;
        if (!transitions.isItem(low))
            return;

        //First terminate any set that overlaps the start of the range
        ValueTransition & next = transitions.item(low);
        if (lower->compareRaw(type, next) == 0)
        {
            if (next.isLowerBound())
            {
                // Range [x..y] remove [x..]
                if (!lower->isInclusiveBound() && next.isInclusiveBound())
                {
                    //[x,y] - (x,z) = [x],{ (x,y)-(x,z)
                    transitions.add(*lower->modifyTransition(type, CMPle), low+1);
                    idx = low+2;
                }
                else
                    transitions.remove(low);
            }
            else
            {
                //[x..y]-[y..z) -> [x..y)
                if (lower->isInclusiveBound() && next.isInclusiveBound())
                {
                    //Convert to an exclusive bound.  This must be different from the lower bound
                    //(otherwise it would have matched that bound 1st)
                    ValueTransition * newTransition = next.modifyTransition(type, CMPlt);
                    transitions.replace(*newTransition, low);
                    idx = low+1;
                }
                else
                    idx = low+1;
            }
        }
        else
        {
            if (next.isUpperBound())
            {
                transitions.add(*lower->modifyTransition(type, lower->isInclusiveBound() ? CMPlt : CMPle), idx);
                idx++;
            }
        }


        //Walk the remaining transitions until you find one that is higher than the current one (or run out)
        while (transitions.isItem(idx))
        {
            ValueTransition & cur = transitions.item(idx);
            int rc = cur.compareRaw(type, *upper);
            if (rc > 0)
            {
                if (cur.isUpperBound())
                    transitions.add(*upper->modifyTransition(type, upper->isInclusiveBound() ? CMPgt : CMPge), idx);
                return;
            }
            if (rc == 0)
            {
                if (cur.isUpperBound())
                {
                    //[x..y] remove [y..y)
                    if (cur.isInclusiveBound() && !upper->isInclusiveBound())
                        transitions.add(*upper->modifyTransition(type, CMPge), idx);
                    else
                        transitions.remove(idx);
                    return;
                }
                else
                {
                    if (!upper->isInclusiveBound())
                        return;
                }
            }
            transitions.remove(idx);
        }
    }
    virtual void reset() override
    {
        transitions.kill();
    }
    virtual void invertSet() override
    {
        if (transitions.empty())
        {
            addAll();
            return;
        }
        unsigned idx = 0;
        ValueTransitionArray newTransitions;
        if (!transitions.item(0).isMinimum())
            newTransitions.append(*new ValueTransition(CMPminmask));
        else
            idx++;

        bool unboundedUpper = transitions.tos().isMaximum();
        unsigned max = transitions.ordinality();
        if (unboundedUpper)
            max--;

        for (; idx < max; idx ++)
            newTransitions.append(*transitions.item(idx).modifyInverse(type));

        if (!unboundedUpper)
            newTransitions.append(*new ValueTransition(CMPmaxmask));

        transitions.swapWith(newTransitions);
    }
    virtual void unionSet(const IValueSet * _other) override
    {
        //Iterate through the ranges in other and add them to this set
        const ValueSet * other = static_cast<const ValueSet *>(_other);
        for (unsigned i=0; i < other->transitions.ordinality(); i+=2)
            addRange(&other->transitions.item(i), &other->transitions.item(i+1));
    }
    virtual void excludeSet(const IValueSet * _other) override
    {
        //Iterate through the ranges in other and add them to this set
        const ValueSet * other = static_cast<const ValueSet *>(_other);
        for (unsigned i=0; i < other->transitions.ordinality(); i+=2)
            killRange(&other->transitions.item(i), &other->transitions.item(i+1));
    }
    virtual void intersectSet(IValueSet const * _other) override
    {
        const ValueSet * other = static_cast<const ValueSet *>(_other);
        //Iterate through the ranges in other and remove them from this set
        unsigned curOther = 0;
        unsigned otherMax = other->numTransitions();
        ValueTransitionArray newTransitions;
        for (unsigned i = 0; i < numTransitions(); i+=2)
        {
            ValueTransition & lower = transitions.item(i);
            ValueTransition & upper = transitions.item(i+1);

            for (;;)
            {
                if (curOther == otherMax)
                    goto done; // break out of both loops.

                ValueTransition & otherLower = other->transitions.item(curOther);
                ValueTransition & otherUpper = other->transitions.item(curOther+1);

                //If upper is lower than the other lower bound then no rows overlap, skip this range
                int rc1 = upper.compareRaw(type, otherLower);
                if (rc1 < 0 ||
                    ((rc1 == 0) && (!upper.isInclusiveBound() || !otherLower.isInclusiveBound())))
                    break;

                //If other upper is lower than the lower bound then no rows overlap, skip other range
                int rc2 = otherUpper.compareRaw(type, lower);
                if (rc2 < 0 ||
                    ((rc2 == 0) && (!otherUpper.isInclusiveBound() || !lower.isInclusiveBound())))
                {
                    curOther += 2;
                    continue;
                }

                //Lower bound of the intersection is the higher of the two lower bounds
                int rc3 = lower.compareRaw(type, otherLower);
                if (rc3 < 0 || ((rc3 == 0) && lower.isInclusiveBound()))
                    newTransitions.append(OLINK(otherLower));
                else
                    newTransitions.append(OLINK(lower));

                //Upper bound of the intersection is the lower of the two bounds - and move onto next range that is consumed
                int rc4 = upper.compareRaw(type, otherUpper);
                if (rc4 < 0 || ((rc4 == 0) && !upper.isInclusiveBound()))
                {
                    newTransitions.append(OLINK(upper));
                    break;
                }
                else
                {
                    newTransitions.append(OLINK(otherUpper));
                    curOther += 2;
                }
            }
        }

    done:
        transitions.swapWith(newTransitions);

    }

    virtual ValueTransition * createRawTransition(TransitionMask mask, const void * value) const override
    {
        return new ValueTransition(mask, type, value);
    }

    virtual void addRawRange(const void * lower, const void * upper) override
    {
        Owned<ValueTransition> lowerBound = lower ? createRawTransition(CMPge, lower) : nullptr;
        Owned<ValueTransition> upperBound = upper ? createRawTransition(CMPle, upper) : nullptr;
        addRange(lowerBound, upperBound);
    }

    virtual void killRawRange(const void * lower, const void * upper) override
    {
        Owned<ValueTransition> lowerBound = lower ? createRawTransition(CMPge, lower) : nullptr;
        Owned<ValueTransition> upperBound = upper ? createRawTransition(CMPle, upper) : nullptr;
        killRange(lowerBound, upperBound);
    }

// Methods for using a value set
    virtual unsigned numRanges() const override;

    //find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
    //matchRange is set to the range number, set to numRanges if there is no possible match.  Uses a binary chop
    virtual int findCandidateRange(const byte * field, unsigned & matchRange) const override;

    //find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
    //starts searching from curRange (which is likely to match).  Uses a sequential search.
    virtual int checkCandidateRange(const byte * field, unsigned & curRange) const override;

    // Does this field match any range?
    virtual bool matches(const byte * field) const override;

    // Does this field match this particular range?
    virtual bool matches(const byte * field, unsigned range) const override;

    virtual StringBuffer & serialize(StringBuffer & out) const override
    {
        //Does this need to include the type information?
        return describe(out);
    }

    virtual const RtlTypeInfo & queryType() const override
    {
        return type;
    }

protected:
    inline int compareRaw(const byte * field, ValueTransition * transition) const __attribute__((always_inline))
    {
        return transition->compareRaw(type, field);
    }

    inline int compare(const byte * field, ValueTransition * transition) const __attribute__((always_inline))
    {
        return transition->compare(type, field);
    }

    ValueTransition * queryTransition(unsigned i) const { return &transitions.item(i); }
    unsigned numTransitions() const { return transitions.ordinality(); }

    StringBuffer & describe(StringBuffer & out) const
    {
        for (unsigned i = 0; i < transitions.ordinality(); i += 2)
        {
            if (i != 0)
                out.append(",");
            ValueTransition & lower = transitions.item(i);
            ValueTransition & upper = transitions.item(i+1);
            lower.describe(type, out);
            if (lower.compareRaw(type, upper) != 0)
            {
                out.append(",");
                upper.describe(type, out);
            }
            else
                out.append("]");
        }
        return out;
    }

    bool hasLowerBound() const
    {
        if (transitions.empty())
            return false;
        return transitions.item(0).isMinimum();
    }

    bool hasUpperBound() const
    {
        if (transitions.empty())
            return false;
        return transitions.tos().isMaximum();
    }
protected:
    const RtlTypeInfo & type;
    ValueTransitionArray transitions;
};

unsigned ValueSet::numRanges() const
{
    return transitions.ordinality() / 2;
}

bool ValueSet::matches(const byte * field) const
{
    //NOTE: It is guaranteed that transitions with the same value are merged if possible - which allows the loop to terminate early on equality.
    unsigned high = numRanges();
    unsigned low = 0;
    while (low < high)
    {
        unsigned mid = low + (high-low)/2;
        unsigned lower = mid * 2;
        //Using compareRaw allows early termination for equalities that actually fail to match.
        int rc = compareRaw(field, queryTransition(lower));
        if (rc < 0)
        {
            high = mid;
        }
        else if (rc == 0)
        {
            if (queryTransition(lower)->isInclusiveBound())
                return true;
            else
                return false;
        }
        else
        {
            unsigned upper = lower + 1;
            rc = compareRaw(field, queryTransition(upper));
            if (rc < 0)
                return true;
            if (rc == 0)
            {
                if (queryTransition(upper)->isInclusiveBound())
                    return true;
                else
                    return false;
            }
            low = mid+1;
        }
    }
    return false;
 }


bool ValueSet::matches(const byte * field, unsigned range) const
{
    if (range >= numRanges())
        return false;
    unsigned lower = range * 2;
    int rc = compare(field, queryTransition(lower));
    if (rc < 0)
        return false;
    if (rc == 0)
        return true;
    unsigned upper = lower + 1;
    rc = compare(field, queryTransition(upper));
    return (rc <= 0);
}


//find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
//matchRange is set to the range number, set to numRanges if there is no possible match.  Uses a binary chop
int ValueSet::findCandidateRange(const byte * field, unsigned & matchRange) const
{
    //NOTE: It is guaranteed that transitions are merged if possible - which allows the loop to terminate early.
    unsigned high = numRanges();
    unsigned low = 0;
    int rc = 0;
    while (low < high)
    {
        unsigned mid = low + (high - low) / 2;
        unsigned lower = mid * 2;
        rc = compare(field, queryTransition(lower));
        if (rc <= 0)
            high = mid;
        else
            low = mid + 1;
    }
    matchRange = low;
    return rc;
}

//find the last range where the lower bound <= the field, returns 0 if the field matches the lower bound, > 0 otherwise.
//starts searching from curRange (which is likely to match).  Uses a sequential search.
int ValueSet::checkCandidateRange(const byte * field, unsigned & curRange) const
{
    unsigned cur = curRange;
    while (cur < numRanges())
    {
        unsigned lower = cur * 2;
        int rc = compare(field, queryTransition(lower));
        if (rc >= 0)
        {
            curRange = cur;
            return rc;
        }
        cur++;
    }
    curRange = numRanges();
    return 0;
}

IValueSet * createValueSet(const RtlTypeInfo & _type) { return new ValueSet(_type); }

//---------------------------------------------------------------------------------------------------------------------

/*
 * Base implementation class for field filters.
 */
class FieldFilter : public CInterfaceOf<IFieldFilter>
{
public:
    FieldFilter(unsigned _field, const RtlTypeInfo & _type) : field(_field), type(_type) {}

//More complex index matching
    virtual int compareRow(const RtlRow & left, const RtlRow & right) const override
    {
        return type.compare(left.queryField(field), right.queryField(field));
    }

protected:
    unsigned field;
    const RtlTypeInfo & type;
};

/*
 * A field filter that can match a set of values.
 */
class SetFieldFilter : public FieldFilter
{
public:
    SetFieldFilter(unsigned _field, IValueSet * _values) : FieldFilter(_field, _values->queryType()), values(_values) {}

//Simple row matching
    virtual bool matches(const RtlRow & row) const override;

//More complex index matching
    virtual unsigned numRanges() const override;
    virtual int findCandidateRange(const RtlRow & row, unsigned & matchRange) const override;
    virtual int checkCandidateRange(const RtlRow & row, unsigned & curRange) const override;
    virtual bool withinUpperRange(const RtlRow & row, unsigned range) const override; // is the row within the current upper limit?
    virtual bool matches(const RtlRow & row, unsigned range) const override;

protected:
    Linked<IValueSet> values;
};


bool SetFieldFilter::matches(const RtlRow & row) const
{
    return values->matches(row.queryField(field));
}

unsigned SetFieldFilter::numRanges() const
{
    return values->numRanges();
}

int SetFieldFilter::findCandidateRange(const RtlRow & row, unsigned & matchRange) const
{
    UNIMPLEMENTED;
    return 0;
}

int SetFieldFilter::checkCandidateRange(const RtlRow & row, unsigned & curRange) const
{
    UNIMPLEMENTED;
    return 0;
}

bool SetFieldFilter::withinUpperRange(const RtlRow & row, unsigned range) const
{
    UNIMPLEMENTED;
    return false;
}

bool SetFieldFilter::matches(const RtlRow & row, unsigned range) const
{
    UNIMPLEMENTED;
    return 0;
}

IFieldFilter * createFieldFilter(unsigned fieldId, IValueSet * values)
{
    return new SetFieldFilter(fieldId, values);
}

IFieldFilter * createEmptyFieldFilter(unsigned fieldId, const RtlTypeInfo & type)
{
    Owned<IValueSet> values = createValueSet(type);
    return new SetFieldFilter(fieldId, values);
}

IFieldFilter * createFieldFilter(unsigned fieldId, const RtlTypeInfo & type, const void * value)
{
    Owned<IValueSet> values = createValueSet(type);
    values->addRawRange(value, value);
    return new SetFieldFilter(fieldId, values);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * A field filter that can match any value.
 */
class WildFieldFilter : public FieldFilter
{
public:
    WildFieldFilter(unsigned _field, const RtlTypeInfo & _type) : FieldFilter(_field, _type) {}

//Simple row matching
    virtual bool matches(const RtlRow & row) const override
    {
        return true;
    }

//More complex index matching
    virtual unsigned numRanges() const override
    {
        return 0;
    }

    virtual int findCandidateRange(const RtlRow & row, unsigned & matchRange) const override
    {
        matchRange = 0;
        return 0;
    }
    virtual int checkCandidateRange(const RtlRow & row, unsigned & curRange) const override
    {
        curRange = 0;
        return 0;
    }
    virtual bool withinUpperRange(const RtlRow & row, unsigned range) const override
    {
        return true;
    }
    virtual bool matches(const RtlRow & row, unsigned range) const override
    {
        return true;
    }
};

extern ECLRTL_API IFieldFilter * createWildFieldFilter(unsigned fieldId);

//---------------------------------------------------------------------------------------------------------------------


void RowFilter::addFilter(IFieldFilter & filter)
{
    //assertex(filter.queryField() == filters.ordinality()); //MORE - fill with wild filters and replace existing wild
    filters.append(filter);
}

bool RowFilter::matches(const RtlRow & row) const
{
    ForEachItemIn(i, filters)
    {
        if (!filters.item(i).matches(row))
            return false;
    }
    return true;
}

int RowFilter::compareRows(const RtlRow & left, const RtlRow & right) const
{
    ForEachItemIn(i, filters)
    {
        int rc = filters.item(i).compareRow(left, right);
        if (rc != 0)
            return rc;
    }
    return 0;
}

//---------------------------------------------------------------------------------------------------------------------


class RowCursor;
class IRowCursor
{
public:
    //Find the first row that is >= the search cursor
    virtual const byte * findFirst(RowCursor & search) = 0;
    //Find the first row that is larger than search within the first numSeekFields
    virtual const byte * findGT(const RtlRow & search, unsigned numSeekFields) = 0;
    //select the next row
    virtual const byte * next() = 0;
};

//This class represents the current set of values which have been matched in the filter sets.
class RowCursor
{
public:
    RowCursor(RowFilter & _filter) : filter(_filter) {}

    void selectFirst()
    {
        numMatched = 0;
    }

    //Compare the incoming row against the search
    int compareGE(const RtlRow & row)
    {
        unsigned i;
        for (i = 0; i < numMatched; i++)
        {
            //Use sequential searches for the values that have previously been matched
            int c = queryFilter(i).checkCandidateRange(row, matchPos(i));
            if (c != 0)
            {
                numMatched = i + 1;
                return c;
            }
        }
        for (; i < numFilterFields(); i++)
        {
            int c = queryFilter(i).findCandidateRange(row, matchPos(i));
            if (c != 0)
            {
                numMatched = i + 1;
                return c;
            }
        }
        numMatched = i;
        return 0;
    }

    int compareRows(const RtlRow & left, const RtlRow & right) const
    {
        return filter.compareRows(left, right);
    }

    bool matchesCurrent(const RtlRow & row)
    {
        unsigned i;
        for (i = 0; i < numMatched; i++)
        {
            if (!queryFilter(i).withinUpperRange(row, matchPos(i)))
            {
                //save away failure info
                return false;
            }
        }

        //Not sure which
        for (; i < numFilterFields(); i++)
        {
            int c = queryFilter(i).findCandidateRange(row, matchPos(i));
            if (!isValid(i))
            {
                numMatched = i + 1;
                //Restart searching element i-1.
                return false;
            }
            if (!queryFilter(i).matches(row, matchPos(i)))
            {
                numMatched = i + 1;
                return false;
            }
        }
        return true;
    }

    //One of the filters in the range 0..numMatches failed.
    //Walk the fields in turn checking if they still match.  The first that doesn't needs to be advanced if possible.
    //If that field cannot be advanced then we need to search for the next row that is > the current leading fields.
    //If there is Need to advance the filter to the next possibility
    //The filter doesn't match => find the next set of potential matches.
    unsigned nextCandidate(const RtlRow & row)
    {
        for (unsigned i = 0; i < numMatched; i++)
        {
            const IFieldFilter & cur = queryFilter(i);
            if (!cur.withinUpperRange(row, matchPos(i)))
            {
                //Find the range that could include the row
                cur.checkCandidateRange(row, matchPos(i));
                if (isValid(i))
                    return 0;
                if (i == 0)
                    return UINT_MAX;
                //caller needs to find a new row that is larger that the current values for fields 0..i
                //Now need to search for a value if
                //Search for a new candidate value from this filter
                return i+1;
            }
        }
        throwUnexpected();
        return false;
    }

    unsigned numFilterFields() const { return filter.numFilterFields(); }

protected:
    unsigned & matchPos(unsigned i) { return matchPositions.element(i); }
    unsigned matchPos(unsigned i) const { return matchPositions.item(i); }
    bool isValid(unsigned i) const { return matchPos(i) < queryFilter(i).numRanges(); }
    const IFieldFilter & queryFilter(unsigned i) const { return filter.queryFilter(i); }

protected:
    UnsignedArray matchPositions;
    unsigned numMatched = 0;
    RowFilter & filter;
};

//---------------------------------------------------------------------------------------------

class KeySearcher
{
public:
    KeySearcher(const RtlRecord & _info, RowFilter & _filter) : curRow(_info, nullptr), cursor(_filter)
    {
    }

    bool first()
    {
        cursor.selectFirst();
        return resolveValidRow(0);
    }

    bool next()
    {
        setCurrent(rows->next());
        if (!curRow)
            return false;
        if (cursor.matchesCurrent(curRow))
            return true;
        //MORE: Check if the row is valid for the next transition
        unsigned numSeekFields = cursor.nextCandidate(curRow);
        return resolveValidRow(numSeekFields);
    }

    bool resolveValidRow(unsigned numSeekFields)
    {
        for (;;)
        {
            //No more possible candidates
            if (numSeekFields == UINT_MAX)
                return false;

            if (numSeekFields != 0)
            {
                //Find the first row where the first numSeekFields are larger than the current values.
                setCurrent(rows->findGT(curRow, numSeekFields)); // more - return the row pointer to avoid recalculation
            }
            else
                setCurrent(rows->findFirst(cursor));
            if (!curRow)
                return false;
            if (cursor.matchesCurrent(curRow))
                return true;
            numSeekFields = cursor.nextCandidate(curRow);
        }
    }

protected:
    void setCurrent(const byte * row) { curRow.setRow(row, cursor.numFilterFields()); }

protected:
    IRowCursor * rows = nullptr;
    RtlDynRow curRow;
    RowCursor cursor;
};

//Always has an even number of transitions
//Null transitions can be used at the start and end of the ranges to map anything
//equality conditions repeat the transition
class InMemoryRowCursor : public IRowCursor
{
    virtual const byte * findFirst(RowCursor & search)
    {
        size_t high = numRows;
        size_t low = 0;
        while (low<high)
        {
            size_t mid = low + (high - low) / 2;
            seekRow.setRow(rows[mid], search.numFilterFields());
            int rc = search.compareGE(seekRow);
            if (rc>0)
                low = mid + 1;
            else
                high = mid;
        }
        cur = low;
        if (low == numRows)
            return nullptr;
        return rows[cur];
    }
    //Find the first row that is larger than search within the first numSeekFields
    //RowCursor is not the correct type
    virtual const byte * findGT(const RowCursor & order, const RtlRow & search, unsigned numSeekFields)
    {
        size_t high = numRows;
        size_t low = 0;
        while (low<high)
        {
            size_t mid = low + (high - low) / 2;
            seekRow.setRow(rows[mid], order.numFilterFields());
            int rc = order.compareRows(seekRow, search);
            if (rc >= 0)
                low = mid + 1;
            else
                high = mid;
        }
        cur = low;
        if (cur == numRows)
            return nullptr;
        return rows[cur];
    }

    virtual const byte * next() override
    {
        cur++;
        if (cur == numRows)
            return nullptr;
        return rows[cur];
    }

protected:
    size_t numRows;
    size_t cur;
    const byte * * rows;
    RtlDynRow seekRow;
};

/*
class KeyLocator
{
public:
    KeyLocator(IValueSet & _filter) : filter(_filter)
    {
    }

    bool first()
    {

        transitionIdx = 0;
        startTransition = filter.queryTransition(transitionIdx);
        if (!startTransition)
            return false;
        endTransition = filter.queryTransition(transitionIdx+1);
        return nextValidRow();
    }

    bool next()
    {
        curRow = rows->next();
        if (matches(*endTransition, curRow))
            return true;
        if (!nextTransition())
            return false;
        //MORE: Check if the row is valid for the next transition
        return nextValidRow();
    }
    bool nextValidRow()
    {
        for (;;)
        {
            if (findFirst(startTransition))
            {
                if (matches(*endTransition, curRow))
                    return true;
            }
            if (!nextTransition())
                return false;
        }
    }

 protected:
    bool nextTransition()
    {
        transitionIdx += 2;
        return setTransition();
    }

    bool setTransition()
    {
        startTransition = filter.queryTransition(transitionIdx);
        if (!startTransition)
            return false;
        endTransition = filter.queryTransition(transitionIdx+1);
        return true;
    }

    bool findFirst(const ValueTransition * transition)
    {
        assertex(transition->queryMask() & CMPgt);
        if (transition->queryMask() & CMPeq)
            curRow = rows->findGE(transition);
        else
            curRow = rows->findGT(transition);
        return curRow != nullptr;
    }

protected:
    unsigned transitionIdx = 0;
    const ValueTransition * startTransition = nullptr;
    const ValueTransition * endTransition = nullptr;
    IRowCursor * rows = nullptr;
    const byte * curRow = nullptr;
    IValueSet & filter;
};
*/

/*
Conclusions:

* Need compareLowest to be able to implement first()/ next with wildcards.  Could implement with a typed lowest transition....
* Is it actually any different from start transition for most cases -
* index read is very efficient using a single memcmp when finding the next.
* Moving the compare into the transition makes the memcmp harder - although could be optimized + allow combining.
* The string sets should work on a field.  The segmonitor should map row->field
* Could have multiple adjacent field fields once have variable length.
* Moving the compare into the transition makes index formats without decompression very tricky.  Could possibly work around it by having a byte provider interface which could be called to get the next value.
*
*
*/

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

/*
class IStdException : extends std::exception
{
Owned<IException> jException;
public:
IStdException(IException *E) : jException(E) {};
};
*/

static void addRange(IValueSet * set, const char * lower, const char * upper)
{
    Owned<ValueTransition> lowerBound = lower ? set->createUtf8Transition(CMPge, rtlUtf8Length(strlen(lower), lower), lower) : nullptr;
    Owned<ValueTransition> upperBound = upper ? set->createUtf8Transition(CMPle, rtlUtf8Length(strlen(upper), upper), upper) : nullptr;
    set->addRange(lowerBound, upperBound);
};

class ValueSetTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ValueSetTest);
        CPPUNIT_TEST(testFilter);
        CPPUNIT_TEST(testRange);
        CPPUNIT_TEST(testSerialize);
        CPPUNIT_TEST(testUnion);
        CPPUNIT_TEST(testExclude);
        CPPUNIT_TEST(testInverse);
        CPPUNIT_TEST(testIntersect);
        CPPUNIT_TEST(testStr2);
    CPPUNIT_TEST_SUITE_END();

protected:
    void checkSet(const IValueSet * set, const char * expected)
    {
        StringBuffer temp;
        set->serialize(temp);
        if (!streq(expected, temp))
            CPPUNIT_ASSERT_EQUAL(expected, temp.str());
    }

    void testRange(RtlTypeInfo & type, const char * low, const char * high, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        addRange(set, low, high);
        checkSet(set, expected);
    }

    void testRangeStr1(const char * low, const char * high, const char * expected)
    {
        RtlStringTypeInfo str1(type_string, 1);
        testRange(str1, low, high, expected);
    }

    void testRangeInt2(const char * low, const char * high, const char * expected)
    {
        RtlIntTypeInfo int2(type_int, 2);
        testRange(int2, low, high, expected);
    }

    void testRangeStrX(const char * low, const char * high, const char * expected)
    {
        RtlStringTypeInfo type(type_string|RFTMunknownsize, 0);
        testRange(type, low, high, expected);
    }

    void testRangeUniX(const char * low, const char * high, const char * expected)
    {
        RtlUnicodeTypeInfo type(type_string|RFTMunknownsize, 0, "");
        testRange(type, low, high, expected);
    }

    void testRange()
    {
        testRangeStr1("A","Z","['A','Z']");
        testRangeStr1("X","z","['X','z']");
        testRangeStr1("Z","X","");
        testRangeStr1(nullptr, "z","(,'z']");
        testRangeStr1("Z", nullptr,"['Z',)");
        testRangeStr1("A", "A","['A']");
        testRangeStr1(nullptr, nullptr,"(,)");
        testRangeStr1("é", "é","['é']"); // Test utf8 translation
        testRangeStr1("AB", "ZX","['A','Z']");

        testRangeInt2("0","1","[0,1]");
        testRangeInt2("-1","1","[-1,1]");
        testRangeInt2("-32768","32767","[-32768,32767]");
        testRangeInt2("32768","0","[-32768,0]");

        testRangeStrX("A", "Z","['A','Z']");
        testRangeStrX("AB", "ZX","['AB','ZX']");
        testRangeStrX("éèabc", "ab","");
        testRangeStrX("a'b", "éèabc", "['a\\'b','éèabc']");

        testRangeUniX("A", "Z","['A','Z']");
        testRangeUniX("AB", "ZX","['AB','ZX']");
        testRangeUniX("éèabc", "ab","");
        testRangeUniX("a'b", "éèabc", "['a\\'b','éèabc']");
    }

    void testSerialize(RtlTypeInfo & type, const char * filter, const char * expected = nullptr)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        checkSet(set, expected ? expected : filter);

    }
    void testUnion(RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        deserializeSet(*set, next);
        checkSet(set, expected);

        //test the arguments in the opposite order
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        deserializeSet(*set2, filter);
        checkSet(set, expected);

        //test the arguments in the opposite order
        Owned<IValueSet> set3a = createValueSet(type);
        Owned<IValueSet> set3b = createValueSet(type);
        deserializeSet(*set3a, next);
        deserializeSet(*set3b, filter);
        set3a->unionSet(set3b);
        checkSet(set3a, expected);
    }
    void testSerialize()
    {
        RtlStringTypeInfo str1(type_string, 1);
        RtlIntTypeInfo int2(type_int, 2);
        RtlStringTypeInfo strx(type_string|RFTMunknownsize, 0);
        testSerialize(int2, "[123]");
        testSerialize(int2, "(123,234]");
        testSerialize(int2, "[123,234)");
        testSerialize(int2, "(123,234)");
        testSerialize(int2, "(,234)");
        testSerialize(int2, "(128,)");
        testSerialize(int2, "(,)");
        testSerialize(int2, "(123,234),(456,567)");
        testSerialize(int2, "(456,567),(123,234)", "(123,234),(456,567)");

        testSerialize(str1, "['A']");
        testSerialize(str1, "[',']");
        testSerialize(str1, "['\\'']");
        testSerialize(strx, "['\\'\\'','}']");

        testSerialize(str1, "[A]", "['A']");
        testSerialize(int2, "(123]", "");
        testSerialize(int2, "[123)", "");
        testSerialize(int2, "[1,0]", "");
    }

    void testUnion()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testSerialize(int2, "[3,5],[5,7]", "[3,7]");
        testSerialize(int2, "[3,5],(5,7]", "[3,7]");
        testSerialize(int2, "[3,5),[5,7]", "[3,7]");
        testSerialize(int2, "[3,5),(5,7]");
        testSerialize(int2, "[4],[4]", "[4]");
        testSerialize(int2, "[4,5],[4]", "[4,5]");
        testSerialize(int2, "[4,5],[5]", "[4,5]");

        testUnion(int2, "[3,5]", "[,1]", "(,1],[3,5]");
        testUnion(int2, "[3,5]", "[,3]", "(,5]");
        testUnion(int2, "[3,5]", "[,4]", "(,5]");
        testUnion(int2, "[3,5]", "[,]", "(,)");
        testUnion(int2, "[3,5]", "[1,)", "[1,)");
        testUnion(int2, "[3,5]", "[3,)", "[3,)");
        testUnion(int2, "[3,5]", "[5,)", "[3,)");
        testUnion(int2, "[3,5]", "(5,)", "[3,)");
        testUnion(int2, "[3,5]", "[6,)", "[3,5],[6,)");
        testUnion(int2, "[3,5]", "(6,)", "[3,5],(6,)");

        testUnion(int2, "[4,7],[12,15]", "[1,1]", "[1],[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,4)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,4]", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,5)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,7)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,7]", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,8]", "[1,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,12)", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,12]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,14]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,15]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,16]", "[1,16]");
        testUnion(int2, "[4,7],[12,15]", "[1,17]", "[1,17]");
        testUnion(int2, "[4,7],[12,15]", "(4,5)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,4]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,5)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,16]", "[4,16]");
        testUnion(int2, "[4,7],[12,15]", "[4,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "(5,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "(7,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,8)", "[4,8),[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "[7,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "[8,8]", "[4,7],[8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,12)", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,12]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,14]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,15]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,16]", "[4,7],[8,16]");
        testUnion(int2, "[4,7],[12,15]", "[8,17]", "[4,7],[8,17]");
        testUnion(int2, "[4,7],[12,15]", "(8,11)", "[4,7],(8,11),[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,12)", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,12]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,14]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,15]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,16]", "[4,7],(8,16]");
        testUnion(int2, "[4,7],[12,15]", "(8,17]", "[4,7],(8,17]");
        testUnion(int2, "[4,7],[12,15]", "(12,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(12,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(12,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "(12,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[12,12]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[12,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[14,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[14,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[14,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[14,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[15,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[15,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[15,17)", "[4,7],[12,17)");
        testUnion(int2, "[4,7],[12,15]", "[16]", "[4,7],[12,15],[16]");
        testUnion(int2, "[4,7],[12,15]", "[16,17]", "[4,7],[12,15],[16,17]");
        testUnion(int2, "[4,7],[12,15]", "[17]", "[4,7],[12,15],[17]");

        testUnion(int2, "(4,7),(12,15)", "[1,1]", "[1],(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,4)", "[1,4),(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,4]", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,5)", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,7)", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,7]", "[1,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,8]", "[1,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,12)", "[1,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,12]", "[1,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,14]", "[1,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,15]", "[1,15]");
        testUnion(int2, "(4,7),(12,15)", "[1,16]", "[1,16]");
        testUnion(int2, "(4,7),(12,15)", "[1,17]", "[1,17]");
        testUnion(int2, "(4,7),(12,15)", "(4,5)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,7)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,12]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,4]", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,5)", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,7)", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,7]", "[4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,8]", "[4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,12)", "[4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,12]", "[4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,14]", "[4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,15]", "[4,15]");
        testUnion(int2, "(4,7),(12,15)", "[4,16]", "[4,16]");
        testUnion(int2, "(4,7),(12,15)", "[4,17]", "[4,17]");
        testUnion(int2, "(4,7),(12,15)", "(5,7)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,12]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,14]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,15]", "(4,15]");
        testUnion(int2, "(4,7),(12,15)", "(5,17]", "(4,17]");
        testUnion(int2, "(4,7),(12,15)", "(7,8]", "(4,7),(7,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,8)", "(4,7),(7,8),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,12)", "(4,7),(7,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,12]", "(4,7),(7,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,14]", "(4,7),(7,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,15]", "(4,7),(7,15]");
        testUnion(int2, "(4,7),(12,15)", "(7,17]", "(4,7),(7,17]");
        testUnion(int2, "(4,7),(12,15)", "[7,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,17]", "(4,17]");
        testUnion(int2, "(4,7),(12,15)", "[8,8]", "(4,7),[8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,12)", "(4,7),[8,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,12]", "(4,7),[8,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,14]", "(4,7),[8,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,15]", "(4,7),[8,15]");
        testUnion(int2, "(4,7),(12,15)", "[8,16]", "(4,7),[8,16]");
        testUnion(int2, "(4,7),(12,15)", "[8,17]", "(4,7),[8,17]");
        testUnion(int2, "(4,7),(12,15)", "(8,11)", "(4,7),(8,11),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,12)", "(4,7),(8,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,12]", "(4,7),(8,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,14]", "(4,7),(8,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,15]", "(4,7),(8,15]");
        testUnion(int2, "(4,7),(12,15)", "(8,16]", "(4,7),(8,16]");
        testUnion(int2, "(4,7),(12,15)", "(8,17]", "(4,7),(8,17]");
        testUnion(int2, "(4,7),(12,15)", "(12,14]", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(12,15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "(12,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "(12,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[12,12]", "(4,7),[12,15)");
        testUnion(int2, "(4,7),(12,15)", "[12,14]", "(4,7),[12,15)");
        testUnion(int2, "(4,7),(12,15)", "[12,15]", "(4,7),[12,15]");
        testUnion(int2, "(4,7),(12,15)", "[12,16]", "(4,7),[12,16]");
        testUnion(int2, "(4,7),(12,15)", "[12,17]", "(4,7),[12,17]");
        testUnion(int2, "(4,7),(12,15)", "[14,14]", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[14,15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "[14,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "[14,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "[15,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "(15,16]", "(4,7),(12,15),(15,16]");
        testUnion(int2, "(4,7),(12,15)", "[15,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[15,17)", "(4,7),(12,17)");
        testUnion(int2, "(4,7),(12,15)", "[16]", "(4,7),(12,15),[16]");
        testUnion(int2, "(4,7),(12,15)", "[16,17]", "(4,7),(12,15),[16,17]");
        testUnion(int2, "(4,7),(12,15)", "[17]", "(4,7),(12,15),[17]");
    }

    void testExclude(RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        set->excludeSet(set2);
        checkSet(set, expected);
    }

    //Tests killRange which is used by excludeSet()
    void testExclude()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testExclude(int2, "[4]", "(4,5]", "[4]");


        testExclude(int2, "[4,7],[12,15]", "[1,1]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,4)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,4]", "(4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,5)", "[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,7)", "[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,7]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(,)", "");
        testExclude(int2, "[4,7],[12,15]", "(,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(6,)", "[4,6]");
        testExclude(int2, "[4,7],[12,15]", "[1,8]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,12)", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,14]", "(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,15]", "");
        testExclude(int2, "[4,7],[12,15]", "[1,16]", "");
        testExclude(int2, "[4,7],[12,15]", "(4,5)", "[4],[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,7)", "[4],[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,7]", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,8]", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,12)", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,12]", "[4],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,4]", "(4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,5)", "[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,7)", "[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,7]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,8]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,12)", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,14]", "(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,15]", "");
        testExclude(int2, "[4,7],[12,15]", "[4,16]", "");
        testExclude(int2, "[4,7],[12,15]", "(5,7)", "[4,5],[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,7]", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,8]", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,12)", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,12]", "[4,5],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,14]", "[4,5],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,15)", "[4,5],[15]");
        testExclude(int2, "[4,7],[12,15]", "(5,15]", "[4,5]");
        testExclude(int2, "[4,7],[12,15]", "(5,17]", "[4,5]");
        testExclude(int2, "[4,7],[12,15]", "(7,8]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,8)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "(7,17]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[7,7]", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,8]", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,12)", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,17]", "[4,7)");
        testExclude(int2, "[4,7],[12,15]", "[8,8]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[8,16]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[8,17]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "(8,11)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(8,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(8,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(12,14]", "[4,7],[12],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(12,15]", "[4,7],[12]");
        testExclude(int2, "[4,7],[12,15]", "(12,16]", "[4,7],[12]");
        testExclude(int2, "[4,7],[12,15]", "[12,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[12,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[12,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[12,16]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[14,14]", "[4,7],[12,14),(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[14,15]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[14,16]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[14,17]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[15]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,16]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,17]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,17)", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[16]", "[4,7],[12,15]");
        testExclude(int2, "(4,7),(12,15)", "[1,1]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,4)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,4]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,14]", "(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,15]", "");
        testExclude(int2, "(4,7),(12,15)", "[1,16]", "");
        testExclude(int2, "(4,7),(12,15)", "(4,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,4]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,14]", "(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,15]", "");
        testExclude(int2, "(4,7),(12,15)", "(5,6)", "(4,5],[6,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,7)", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,7]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,8]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,12)", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,12]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,14]", "(4,5],(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,15]", "(4,5]");
        testExclude(int2, "(4,7),(12,15)", "(5,17]", "(4,5]");
        //return;
        testExclude(int2, "(4,7),(12,15)", "(7,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,8)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(7,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[7,7]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,11)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(12,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(12,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(12,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[12,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[13]", "(4,7),(12,13),(13,15)");
        testExclude(int2, "(4,7),(12,15)", "[12,14)", "(4,7),[14,15)");
        testExclude(int2, "(4,7),(12,15)", "[12,15)", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[12,16)", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[14,14]", "(4,7),(12,14),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[14,15]", "(4,7),(12,14)");
        testExclude(int2, "(4,7),(12,15)", "[14,16]", "(4,7),(12,14)");
        testExclude(int2, "(4,7),(12,15)", "[15]", "(4,7),(12,15)");
    }

    void testInverse(RtlTypeInfo & type, const char * filter, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        set->invertSet();
        checkSet(set, expected);
    }

    //Tests killRange which is used by excludeSet()
    void testInverse()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testInverse(int2, "[4]", "(,4),(4,)");
        testInverse(int2, "[4,5]", "(,4),(5,)");
        testInverse(int2, "(4,5)", "(,4],[5,)");
        testInverse(int2, "(,5)", "[5,)");
        testInverse(int2, "[6,)", "(,6)");
        testInverse(int2, "", "(,)");
        testInverse(int2, "(,)", "");
        testInverse(int2, "[4,5],(8,9),[12,14)", "(,4),(5,8],[9,12),[14,)");
    }

    void testIntersect(RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        set->intersectSet(set2);
        checkSet(set, expected);

        //Test the opposite way around
        Owned<IValueSet> seta = createValueSet(type);
        deserializeSet(*seta, filter);
        Owned<IValueSet> setb = createValueSet(type);
        deserializeSet(*setb, next);
        setb->intersectSet(seta);
        checkSet(set, expected);
    }

    void testIntersect()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testIntersect(int2, "", "[1,2],(3,6)", "");
        testIntersect(int2, "(,)", "[1,2],(3,6)", "[1,2],(3,6)");
        testIntersect(int2, "(,)", "", "");
        testIntersect(int2, "", "", "");
        testIntersect(int2, "(,)", "(,)", "(,)");

        testIntersect(int2, "(3,7),[10,20]", "[2]", "");
        testIntersect(int2, "(3,7),[10,20]", "[3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[4]", "[4]");
        testIntersect(int2, "(3,7),[10,20]", "[6]", "[6]");
        testIntersect(int2, "(3,7),[10,20]", "[7]", "");
        testIntersect(int2, "(3,7),[10,20]", "[10]", "[10]");
        testIntersect(int2, "(3,7),[10,20]", "[20]", "[20]");
        testIntersect(int2, "(3,7),[10,20]", "[21]", "");

        testIntersect(int2, "(3,7),[10,20]", "[2,3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[2,5]", "(3,5]");
        testIntersect(int2, "(3,7),[10,20]", "[2,7]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[2,8]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[2,10]", "(3,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[2,15]", "(3,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[2,20]", "(3,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[2,25]", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[3,3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[3,5]", "(3,5]");
        testIntersect(int2, "(3,7),[10,20]", "[3,7]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[3,8]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[3,10]", "(3,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[3,15]", "(3,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[3,20]", "(3,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[3,25]", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[5,7]", "[5,7)");
        testIntersect(int2, "(3,7),[10,20]", "[5,8]", "[5,7)");
        testIntersect(int2, "(3,7),[10,20]", "[5,10]", "[5,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[5,15]", "[5,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[5,20]", "[5,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[5,25]", "[5,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[7,8]", "");
        testIntersect(int2, "(3,7),[10,20]", "[7,10]", "[10]");
        testIntersect(int2, "(3,7),[10,20]", "[7,15]", "[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[7,20]", "[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[7,25]", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[10,15]", "[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[10,20]", "[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[10,25]", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[15,20]", "[15,20]");
        testIntersect(int2, "(3,7),[10,20]", "[15,25]", "[15,20]");

        testIntersect(int2, "(3,7),[10,20]", "[20,25]", "[20]");

        testIntersect(int2, "(3,7),[10,20]", "(2,3)", "");
        testIntersect(int2, "(3,7),[10,20]", "(2,5)", "(3,5)");
        testIntersect(int2, "(3,7),[10,20]", "(2,7)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,8)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,10)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,15)", "(3,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(2,20)", "(3,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(2,25)", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(3,5)", "(3,5)");
        testIntersect(int2, "(3,7),[10,20]", "(3,7)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,8)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,10)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,15)", "(3,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(3,20)", "(3,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(3,25)", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(5,7)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,8)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,10)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,15)", "(5,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(5,20)", "(5,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(5,25)", "(5,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(7,8)", "");
        testIntersect(int2, "(3,7),[10,20]", "(7,10)", "");
        testIntersect(int2, "(3,7),[10,20]", "(7,15)", "[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(7,20)", "[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(7,25)", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(10,15)", "(10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(10,20)", "(10,20)");

        testIntersect(int2, "(3,7),[10,20]", "(15,20)", "(15,20)");
        testIntersect(int2, "(3,7),[10,20]", "(15,25)", "(15,20]");

        testIntersect(int2, "(3,7),[10,20]", "(20,25)", "");

        testIntersect(int2, "(3,5),[7,10),[15,20),(30,32),[37]", "(4,7],(9,12),[13,31],(36,)", "(4,5),[7],(9,10),[15,20),(30,31],[37]");
    }
    void testStr2()
    {
        RtlStringTypeInfo str1(type_string, 1);
        Owned<IValueSet> az = createValueSet(str1);
        addRange(az, "A", "Z");
        checkSet(az, "['A','Z']");
        Owned<IValueSet> dj = createValueSet(str1);
        addRange(dj, "D", "J");
        checkSet(dj, "['D','J']");
        Owned<IValueSet> hz = createValueSet(str1);
        addRange(hz, "H", "Z");
        Owned<IValueSet> jk = createValueSet(str1);
        addRange(jk, "J", "K");
        Owned<IValueSet> kj = createValueSet(str1);
        addRange(kj, "K", "J");
        checkSet(kj, "");

    }
    // id:int2 extra:string padding:! name:string2
    // Keep in sorted order so they can be reused for index testing
    const char *testRows[6] = {
            "\002\000\000\000\000\000!AB",
            "\001\000\004\000\000\000MARK!GH",
            "\000\001\003\000\000\000FRY!JH",
            "\001\001\003\000\000\000MAR!AC",
            "\002\001\003\000\000\000MAS!JH",
            "\003\001\004\000\000\000MASK!JH",
    };
    //testFilter("id=[1,3];name=(,GH)", { false, true, false, false, false });
    void testFilter(const char * originalFilter, const std::initializer_list<bool> & expected)
    {
        const byte * * rows = reinterpret_cast<const byte * *>(testRows);
        RtlIntTypeInfo int2(type_int, 2);
        RtlStringTypeInfo str1(type_string, 2);
        RtlStringTypeInfo str2(type_string, 2);
        RtlStringTypeInfo strx(type_string|RFTMunknownsize, 0);
        RtlFieldInfo id("id", nullptr, &int2);
        RtlFieldInfo extra("extra", nullptr, &strx);
        RtlFieldInfo padding("padding", nullptr, &str1);
        RtlFieldInfo name("name", nullptr, &str2);
        const RtlFieldInfo * fields[] = { &id, &extra, &padding, &name, nullptr };
        RtlRecordTypeInfo recordType(type_record, 4, fields);
        RtlRecord record(recordType, true);

        RowFilter cursor;
        const char * filter = originalFilter;
        while (filter && *filter)
        {
            StringBuffer next;
            const char * semi = strchr(filter, ';');
            if (semi)
            {
                next.append(semi-filter, filter);
                filter = semi+1;
            }
            else
            {
                next.append(filter);
                filter = nullptr;
            }

            const char * equal = strchr(next, '=');
            assertex(equal);
            StringBuffer fieldName(equal-next, next);
            unsigned fieldNum = record.getFieldNum(fieldName);
            assertex(fieldNum != (unsigned) -1);
            const RtlTypeInfo *fieldType = record.queryType(fieldNum);
            Owned<IValueSet> set = createValueSet(*fieldType);
            deserializeSet(*set, equal+1);
            cursor.addFilter(*new SetFieldFilter(fieldNum, set));
        }

        RtlDynRow row(record, nullptr);
        assertex((expected.end() - expected.begin()) == (unsigned)_elements_in(testRows));
        const bool * curExpected = expected.begin();
        for (unsigned i= 0; i < _elements_in(testRows); i++)
        {
            row.setRow(rows[i]);
            if (cursor.matches(row) != curExpected[i])
            {
                printf("Failure to match row %u filter '%s'\n", i, originalFilter);
                CPPUNIT_ASSERT_EQUAL(curExpected[i], cursor.matches(row));
            }
        }
    };

    void testFilter()
    {
        testFilter("", { true, true, true, true, true, true });
        testFilter("id=[1]", { false, true, false, false, false, false });
        testFilter("id=[1,2]", { true, true, false, false, false, false });
        testFilter("id=(1,2]", { true, false, false, false, false, false });
        testFilter("id=[1,3];name=(,GH)", { true, false, false, false, false, false });
        testFilter("id=[1,3];name=(,GH)", { true, false, false, false, false, false });
        testFilter("extra=['MAR','MAS']", { false, true, false, true, true, false });
        testFilter("extra=('MAR','MAS')", { false, true, false, false, false, false });
        testFilter("id=(,257]", { true, true, true, true, false, false });
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ValueSetTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ValueSetTest, "ValueSetTest");

#endif
