/*##############################################################################


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
#include "jdebug.hpp"
#include "jsort.hpp"
#include "jexcept.hpp"
#include "rtlnewkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"
#include "rtlnewkey.hpp"

/*
 * Read a single quoted string from in until the terminating quote is found
 *
 * @param out       The resulting string with embedded escaped quote characters resolved.
 * @param in        A reference to the start of the string.  Updated to point to the end of the string.
 *
 */
static void readString(StringBuffer &out, const char * &in)
{
    const char *start = in;
    // Find terminating quote, skipping any escaped ones
    for (;;)
    {
        char c = *in++;
        if (!c)
            throw MakeStringException(0, "Invalid filter - missing closing '");
        if (c=='\'')
            break;
        if (c=='\\')
            c = *in++;
    }
    StringBuffer errmsg;
    unsigned errpos;
    if (!checkUnicodeLiteral(start, in-start-1, errpos, errmsg))
        throw makeStringExceptionV(0, "Invalid filter - %s", errmsg.str());
    rtlDataAttr temp;
    size32_t newlen = 0;  // NOTE - this will be in codepoints, not bytes
    rtlCodepageToUtf8XUnescape(newlen, temp.refstr(), in-start-1, start, "UTF-8");
    size32_t newsize = rtlUtf8Size(newlen, temp.getstr());
    out.append(newsize, temp.getstr());
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
        Owned<IValueTransition> lower = lowerString ? set.createUtf8Transition(lowerMask, rtlUtf8Length(lowerString.length(), lowerString), lowerString) : nullptr;
        Owned<IValueTransition> upper = upperString ? set.createUtf8Transition(upperMask, rtlUtf8Length(upperString.length(), upperString), upperString) : nullptr;
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

class ValueTransition : implements CInterfaceOf<IValueTransition>
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
    ValueTransition(const RtlTypeInfo & type, MemoryBuffer & in)
    {
        byte inmask;
        in.read(inmask);
        if (!(inmask & CMPnovalue))
        {
            mask = (TransitionMask)inmask;
            size32_t size = type.size(in.readDirect(0), nullptr);
            value.allocateN(size, false);
            in.read(size, value);
        }
        else
        {
            mask = (TransitionMask)(inmask & ~CMPnovalue);
        }
    }
    ValueTransition(TransitionMask _mask) : mask(_mask)
    {
        dbgassertex(isMaximum() || isMinimum());
    }

    bool equals(const RtlTypeInfo & type, const ValueTransition & other) const
    {
        if (mask != other.mask)
            return false;
        if (value && other.value)
        {
            return type.compare(value, other.value) == 0;
        }
        else
            return !value && !other.value;
    }

    MemoryBuffer & serialize(const RtlTypeInfo & type, MemoryBuffer & out) const
    {
        byte outmask = mask;
        if (value)
        {
            size32_t size = type.size(value, nullptr);
            out.append(outmask);
            out.append(size, value);
        }
        else
        {
            outmask |= CMPnovalue;
            out.append(outmask);
        }
        return out;
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

typedef IArrayOf<ValueTransition> ValueTransitionArray;


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

    ValueSet(const RtlTypeInfo & _type, MemoryBuffer & in) : type(_type)
    {
        unsigned cnt;
        in.readPacked(cnt);
        for (unsigned i = 0; i < cnt; i++)
            transitions.append(*new ValueTransition(type, in));
    }

// Methods for creating a value set
    virtual IValueTransition * createTransition(TransitionMask mask, unsigned __int64 value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildInt(builder, 0, nullptr, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }
    virtual IValueTransition * createStringTransition(TransitionMask mask, size32_t len, const char * value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildString(builder, 0, nullptr, len, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }
    virtual IValueTransition * createUtf8Transition(TransitionMask mask, size32_t len, const char * value) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildUtf8(builder, 0, nullptr, len, value);
        return new ValueTransition(mask, type, buff.toByteArray());
    }

    virtual void addRange(IValueTransition * _lower, IValueTransition * _upper) override
    {
        ValueTransition * lower = static_cast<ValueTransition *>(_lower);
        ValueTransition * upper = static_cast<ValueTransition *>(_upper);
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
    virtual void killRange(IValueTransition * _lower, IValueTransition * _upper) override
    {
        ValueTransition * lower = static_cast<ValueTransition *>(_lower);
        ValueTransition * upper = static_cast<ValueTransition *>(_upper);
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

    virtual bool equals(const IValueSet & _other) const override
    {
        const ValueSet & other = static_cast<const ValueSet &>(_other);
        if (!type.equivalent(&other.type))
            return false;
        if (transitions.ordinality() != other.transitions.ordinality())
            return false;
        ForEachItemIn(i, transitions)
        {
            if (!transitions.item(i).equals(type, other.transitions.item(i)))
                return false;
        }
        return true;
    }

// Methods for using a value set
    virtual bool isWild() const override;
    virtual unsigned numRanges() const override;

    virtual int compareLowest(const byte * field, unsigned range) const override;

    virtual int compareHighest(const byte * field, unsigned range) const override;

    virtual int findForwardMatchRange(const byte * field, unsigned & matchRange) const override;

    // Does this field match any range?
    virtual bool matches(const byte * field) const override;

    // Does this field match this particular range?
    virtual bool matches(const byte * field, unsigned range) const override;

    virtual StringBuffer & serialize(StringBuffer & out) const override
    {
        //Does this need to include the type information?
        return describe(out);
    }

    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override
    {
        out.appendPacked((unsigned)transitions.ordinality());
        ForEachItemIn(i, transitions)
            transitions.item(i).serialize(type, out);
        return out;
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
        return !transitions.item(0).isMinimum();
    }

    bool hasUpperBound() const
    {
        if (transitions.empty())
            return false;
        return !transitions.tos().isMaximum();
    }
protected:
    const RtlTypeInfo & type;
    ValueTransitionArray transitions;
};

unsigned ValueSet::numRanges() const
{
    return transitions.ordinality() / 2;
}

bool ValueSet::isWild() const
{
    if (transitions.ordinality() != 2)
        return false;

    return queryTransition(0)->isMinimum() && queryTransition(1)->isMaximum();
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


int ValueSet::compareLowest(const byte * field, unsigned range) const
{
    return compare(field, queryTransition(range*2));
}

int ValueSet::compareHighest(const byte * field, unsigned range) const
{
    return compare(field, queryTransition(range*2+1));
}


/*
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
int ValueSet::checkNextCandidateRange(const byte * field, unsigned & curRange) const
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
*/

//If field lies within a range return true and set matchRange to the index of the range.
//If field is outside a range return false and set matchRange to the index of the next range, i.e. min(range.lower) where range.lower > field
//Find the largest transition value that is <= field, and then check if within upper limit
//Could have a version that starts from the previous match to shorten the binary chop, or use a linear search instead
int ValueSet::findForwardMatchRange(const byte * field, unsigned & matchRange) const
{
    unsigned ranges = numRanges();
    unsigned high = ranges;
    unsigned low = 0;
    //Find the largest transition that is <= the search value
    while (high - low > 1)
    {
        unsigned mid = low + (high - low) / 2;
        unsigned lower = mid * 2;
        int rc = compare(field, queryTransition(lower));
        if (rc < 0)
        {
            // field is less than transition, so transition with value < search must be before.
            high = mid; // exclude mid and all transitions after it
        }
        else if (rc > 0)
        {
            // field is greater than transition, so transition with value > search must be this or after.
            low = mid; // exclude mid-1 and all transitions before it
        }
        else
        {
            matchRange = mid;
            return true;
        }
    }
    //rc is comparison from element mid.
    if (low != ranges)
    {
        if (compare(field, queryTransition(low*2)) >= 0)
        {
            int rc = compare(field, queryTransition(low*2+1));
            if (rc <= 0)
            {
                matchRange = low;
                return true;
            }
            low++;
        }
    }

    matchRange = low;
    return false;
}

IValueSet * createValueSet(const RtlTypeInfo & _type) { return new ValueSet(_type); }
IValueSet * createValueSet(const RtlTypeInfo & _type, MemoryBuffer & in) { return new ValueSet(_type, in); }

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

    virtual unsigned queryFieldIndex() const override
    {
        return field;
    }

    virtual const RtlTypeInfo & queryType() const override
    {
        return type;
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
    virtual bool isEmpty() const override;
    virtual bool isWild() const override;

//More complex index matching
    virtual unsigned numRanges() const override;
    virtual int compareLowest(const RtlRow & left, unsigned range) const override;
    virtual int compareHighest(const RtlRow & left, unsigned range) const override;
    virtual int findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const override;

    virtual unsigned queryScore() const override;
    virtual IFieldFilter *remap(unsigned newField) const override { return new SetFieldFilter(newField, values); }
    virtual StringBuffer & serialize(StringBuffer & out) const override;
    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override;
protected:
    Linked<IValueSet> values;
};


bool SetFieldFilter::matches(const RtlRow & row) const
{
    return values->matches(row.queryField(field));
}

bool SetFieldFilter::isEmpty() const
{
    return values->numRanges() == 0;
}

bool SetFieldFilter::isWild() const
{
    return values->isWild();
}

unsigned SetFieldFilter::numRanges() const
{
    return values->numRanges();
}

int SetFieldFilter::compareLowest(const RtlRow & left, unsigned range) const
{
    return values->compareLowest(left.queryField(field), range);
}

int SetFieldFilter::compareHighest(const RtlRow & left, unsigned range) const
{
    return values->compareHighest(left.queryField(field), range);
}

int SetFieldFilter::findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const
{
    return values->findForwardMatchRange(row.queryField(field), matchRange);
}

unsigned SetFieldFilter::queryScore() const
{
    // MORE - the score should probably depend on the number and nature of ranges too.
    unsigned score = type.getMinSize();
    if (!score)
        score = 5;   // Arbitrary guess for average field length in a variable size field
    return score;
}

StringBuffer & SetFieldFilter::serialize(StringBuffer & out) const
{
    out.append('=');
    return values->serialize(out);
}
MemoryBuffer & SetFieldFilter::serialize(MemoryBuffer & out) const
{
    out.append('=');
    return values->serialize(out);
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
    virtual bool isEmpty() const override
    {
        return false;
    }
    virtual bool isWild() const override
    {
        return true;
    }

//More complex index matching
    virtual unsigned numRanges() const override
    {
        return 1;
    }

    virtual int compareLowest(const RtlRow & left, unsigned range) const override
    {
        //Should pass through to compare using the type.
        //return type.compareLowest(left.queryField(fieldId));
        //MORE
        return +1;
    }

    virtual int compareHighest(const RtlRow & left, unsigned range) const override
    {
        //Should pass through to compare using the type.
        //return type.compareLowest(left.queryField(fieldId));
        //MORE
        return -1;
    }

    virtual int findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const override
    {
        matchRange = 0;
        return true;
    }
    virtual unsigned queryScore() const override
    {
        return 0;
    }
    virtual IFieldFilter *remap(unsigned newField) const override { return new WildFieldFilter(newField, type); }

    virtual StringBuffer & serialize(StringBuffer & out) const override
    {
        return out.append('*');
    }

    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override
    {
        return out.append('*');
    }
};

IFieldFilter * createWildFieldFilter(unsigned fieldId, const RtlTypeInfo & type)
{
    //MORE: Is it worth special casing, or would a SetFieldFilter of null..null be just as good?
    return new WildFieldFilter(fieldId, type);
}

//---------------------------------------------------------------------------------------------------------------------

//Note, the fieldId could be serialized within the string, but it is needed to determine the type, and
//passing it in allows this code to be decoupled from the type serialization code.
IFieldFilter * deserializeFieldFilter(unsigned fieldId, const RtlTypeInfo & type, const char * src)
{
    switch (*src)
    {
    case '*':
        return createWildFieldFilter(fieldId, type);
    case '=':
        {
            Owned<IValueSet> values = createValueSet(type);
            deserializeSet(*values, src+1);
            return createFieldFilter(fieldId, values);
        }
    }

    UNIMPLEMENTED_X("Unknown Field Filter");
}

IFieldFilter * deserializeFieldFilter(unsigned fieldId, const RtlTypeInfo & type, MemoryBuffer & in)
{
    char kind;
    in.read(kind);
    switch (kind)
    {
    case '*':
        return createWildFieldFilter(fieldId, type);
    case '=':
        {
            Owned<IValueSet> values = createValueSet(type, in);
            return createFieldFilter(fieldId, values);
        }
    }

    UNIMPLEMENTED_X("Unknown Field Filter");
}

//---------------------------------------------------------------------------------------------------------------------

static int compareFieldFilters(IInterface * const * left, IInterface * const * right)
{
    IFieldFilter * leftFilter = static_cast<IFieldFilter *>(*left);
    IFieldFilter * rightFilter = static_cast<IFieldFilter *>(*right);
    return leftFilter->queryFieldIndex() - rightFilter->queryFieldIndex();
}

void RowFilter::addFilter(const IFieldFilter & filter)
{
    //assertex(filter.queryField() == filters.ordinality()); //MORE - fill with wild filters and replace existing wild
    filters.append(filter);
    unsigned fieldNum = filter.queryFieldIndex();
    if (fieldNum >= numFieldsRequired)
        numFieldsRequired = fieldNum+1;
}

bool RowFilter::matches(const RtlRow & row) const
{
    row.lazyCalcOffsets(numFieldsRequired);
    ForEachItemIn(i, filters)
    {
        if (!filters.item(i).matches(row))
            return false;
    }
    return true;
}

void RowFilter::appendFilters(IConstArrayOf<IFieldFilter> & _filters)
{
    ForEachItemIn(i, _filters)
    {
        addFilter(OLINK(_filters.item(i)));
    }
}

void RowFilter::extractKeyFilter(const RtlRecord & record, IConstArrayOf<IFieldFilter> & keyFilters) const
{
    if (!filters)
        return;

    // for an index must be in field order, and all values present
    IConstArrayOf<IFieldFilter> temp;
    ForEachItemIn(i, filters)
        temp.append(OLINK(filters.item(i)));
    temp.sort(compareFieldFilters);

    unsigned maxField = temp.tos().queryFieldIndex();
    unsigned curIdx=0;
    for (unsigned field = 0; field <= maxField; field++)
    {
        const IFieldFilter & cur = temp.item(curIdx);
        if (field == cur.queryFieldIndex())
        {
            keyFilters.append(OLINK(cur));
            curIdx++;
        }
        else
            keyFilters.append(*createWildFieldFilter(field, *record.queryType(field)));
    }
}

void RowFilter::extractMemKeyFilter(const RtlRecord & record, const UnsignedArray &sortOrder, IConstArrayOf<IFieldFilter> & keyFilters) const
{
    if (!filters)
        return;

    // for in-memory index, we want filters in the same order as the sort fields, with wilds added
    ForEachItemIn(idx, sortOrder)
    {
        unsigned sortField = sortOrder.item(idx);
        bool needWild = true;
        ForEachItemIn(fidx, filters)
        {
            const IFieldFilter &filter = filters.item(fidx);
            if (filter.queryFieldIndex()==sortField)
            {
                keyFilters.append(OLINK(filter));
                needWild = false;
                break;
            }
        }
        if (needWild)
            keyFilters.append(*createWildFieldFilter(sortField, *record.queryType(sortField)));
    }
}

const IFieldFilter *RowFilter::findFilter(unsigned fieldNum) const
{
    ForEachItemIn(i, filters)
    {
        const IFieldFilter &field = filters.item(i);
        if (field.queryFieldIndex() == fieldNum)
            return &field;
    }
    return nullptr;
}

const IFieldFilter *RowFilter::extractFilter(unsigned fieldNum)
{
    ForEachItemIn(i, filters)
    {
        const IFieldFilter &field = filters.item(i);
        if (field.queryFieldIndex() == fieldNum)
        {
            filters.remove(i, true);
            return &field;
        }
    }
    return nullptr;
}

void RowFilter::remove(unsigned idx)
{
    filters.remove(idx);
}

void RowFilter::clear()
{
    filters.kill();
    numFieldsRequired = 0;
}

void RowFilter::recalcFieldsRequired()
{
    numFieldsRequired = 0;
    ForEachItemIn(i, filters)
    {
        const IFieldFilter &field = filters.item(i);
        if (field.queryFieldIndex() >= numFieldsRequired)
            numFieldsRequired = field.queryFieldIndex()+1;
    }
}

void RowFilter::remapField(unsigned filterIdx, unsigned newFieldNum)
{
    filters.replace(*filters.item(filterIdx).remap(newFieldNum), filterIdx);
}


//---------------------------------------------------------------------------------------------------------------------

bool RowCursor::setRowForward(const byte * row)
{
    currentRow.setRow(row, numFieldsRequired);

    unsigned field = 0;
    //Now check which of the fields matches, and update matchedRanges to indicate
    //MORE: Add an optimization:
    //First of all check all of the fields that were previously matched.  If the previous matches are in the same range,
    //then the next field must either be in the same range, or a following range.
    /*
    for (; field < numMatched; field++)
    {
        const IFieldFilter & filter = queryFilter(field);
        if (!filter.withinUpperRange(currentRow, matchPos(field)))
        {
            //This field now either matches a later range, or doesn't match at all.
            //Find the new range for the current field that could include the row
            cur.checkNextCandidateRange(currentRow, matchPos(i));
            if (isValid(i))
                return 0;
            //Finished processing
            if (i == 0)
                return UINT_MAX;
            //caller needs to find a new row that is larger that the current values for fields 0..i
            //Now need to search for a value if
            //Search for a new candidate value from this filter
            return i+1;
        }
        //if (!filter.withinUpperRange(currentRow,
    }
    */

    for (; field < filters.ordinality(); field++)
    {
        const IFieldFilter & filter = queryFilter(field);
        //If the field is within a range return true and the range.  If outside the range return the next range.
        unsigned matchRange;
        bool matched = filter.findForwardMatchRange(currentRow, matchRange);
        if (!matched)
        {
            if (matchRange >= filter.numRanges())
                return findNextRange(field);

            nextUnmatchedRange = matchRange;
            break;
        }
        matchedRanges.replace(matchRange, field);
    }

    numMatched = field;
    return numMatched == filters.ordinality();
}


//The filter for field "field" has been exhausted, work out which value should be compared next
bool RowCursor::findNextRange(unsigned field)
{
    unsigned matchRange;
    for (;;)
    {
        if (field == 0)
        {
            eos = true;
            return false;
        }

        field = field-1;
        matchRange = matchedRanges.item(field);
        const IFieldFilter & filter = queryFilter(field);
        //If the field value is less than the upper bound of the current range, search for the next value above
        //the current value
        if (filter.compareHighest(currentRow, matchRange) < 0)
        {
            numMatched = field;

            //MORE: Optimize the case where the field can be incremented - if so other fields can compare against lowest
            //if (filter.incrementValue(currentRow))
            //    nextUnmatchedRange = 0;
            //else
            nextUnmatchedRange = -1U;
            return false;
        }

        matchRange++;
        if (matchRange < filter.numRanges())
            break;

        field--;
    }

    nextUnmatchedRange = matchRange;
    numMatched = field-1;
    return false;
}


//---------------------------------------------------------------------------------------------

class InMemoryRows
{
public:
    InMemoryRows(size_t _countRows, const byte * * _rows, const RtlRecord & _record)
     : countRows(_countRows), rows(_rows), record(_record)
    {
    }

    const byte * queryRow(unsigned i) const { return rows[i]; }
    size_t numRows() const { return countRows; }
    const RtlRecord & queryRecord() const { return record; }

protected:
    size_t countRows;
    const byte * * rows;
    const RtlRecord & record;
};

//Always has an even number of transitions
//Null transitions can be used at the start and end of the ranges to map anything
//equality conditions repeat the transition
class InMemoryRowCursor : public ISourceRowCursor
{
public:
    InMemoryRowCursor(InMemoryRows & _source) : source(_source), seekRow(_source.queryRecord())
    {
    }

    virtual const byte * findNext(const RowCursor & search) override
    {
        size_t numRows = source.numRows();
        if (numRows == 0)
            return nullptr;

        size_t high = numRows;
        size_t low = 0; // Could be cur

        bool scanOnNext = false;
        if (cur != 0 && scanOnNext)
        {
            //MORE: The next match is likely to be close, so first of all look for a match in the next few rows
            //An always searching forwards, so can guarantee that it follows cur > low
        }
        //Find the value of low,high where all rows 0..low-1 are < search and rows low..max are >= search
        while (low<high)
        {
            size_t mid = low + (high - low) / 2;
            seekRow.setRow(source.queryRow(mid), search.numFilterFields());
            int rc = search.compareNext(seekRow);  // compare seekRow with the row we are hoping to find
            if (rc < 0)
                low = mid + 1;  // if this row is lower than the seek row, exclude mid from the potential positions
            else
                high = mid; // otherwise exclude all above mid from the potential positions.
        }
        cur = low;
        if (low == numRows)
            return nullptr;
        return source.queryRow(cur);
    }

    virtual const byte * next() override
    {
        cur++;
        if (cur == source.numRows())
            return nullptr;
        return source.queryRow(cur);
    }

    virtual void reset() override
    {
        cur = 0;
        seekRow.setRow(nullptr);
    }

protected:
    size_t cur = 0;
    InMemoryRows & source;
    RtlDynRow seekRow;
};

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

//Scan a set of rows to find the matches - used to check that the keyed operations are correct
class RowScanner
{
public:
    RowScanner(const RtlRecord & _info, RowFilter & _filter, const PointerArray & _rows) : rows(_rows), curRow(_info, nullptr), filter(_filter)
    {
    }

    bool first()
    {
        return resolveNext(0);
    }

    bool next()
    {
        return resolveNext(curIndex+1);
    }

    bool resolveNext(unsigned next)
    {
        while (next < rows.ordinality())
        {
            curRow.setRow(rows.item(next));
            if (filter.matches(curRow))
            {
                curIndex = next;
                return true;
            }
            next++;
        }
        curIndex = next;
        return false;
    }

    const RtlRow & queryRow() const { return curRow; }

protected:
    const PointerArray & rows;
    RtlDynRow curRow;
    RowFilter & filter;
    unsigned curIndex = 0;
};



static void addRange(IValueSet * set, const char * lower, const char * upper)
{
    Owned<IValueTransition> lowerBound = lower ? set->createUtf8Transition(CMPge, rtlUtf8Length(strlen(lower), lower), lower) : nullptr;
    Owned<IValueTransition> upperBound = upper ? set->createUtf8Transition(CMPle, rtlUtf8Length(strlen(upper), upper), upper) : nullptr;
    set->addRange(lowerBound, upperBound);
};

class RawRowCompare : public ICompare
{
public:
    RawRowCompare(const RtlRecord & _record) : record(_record) {}

    virtual int docompare(const void * left,const void * right) const
    {
        return record.compare((const byte *)left, (const byte *)right);
    }

    const RtlRecord & record;
};


class ValueSetTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ValueSetTest);
        CPPUNIT_TEST(testKeyed2);
        CPPUNIT_TEST(testRange);
        CPPUNIT_TEST(testSerialize);
        CPPUNIT_TEST(testUnion);
        CPPUNIT_TEST(testExclude);
        CPPUNIT_TEST(testInverse);
        CPPUNIT_TEST(testIntersect);
        CPPUNIT_TEST(testStr2);
        CPPUNIT_TEST(testFilter);
        CPPUNIT_TEST(testKeyed1);
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

        MemoryBuffer mb;
        set->serialize(mb);
        Owned<IValueSet> newset = createValueSet(type, mb);
        checkSet(newset, expected ? expected : filter);
        CPPUNIT_ASSERT(set->equals(*newset));

        StringBuffer s;
        set->serialize(s);
        Owned<IValueSet> newset2 = createValueSet(type);
        deserializeSet(*newset2, s);
        CPPUNIT_ASSERT(set->equals(*newset2));
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
        RtlUtf8TypeInfo utf8(type_utf8|RFTMunknownsize, 0, nullptr);
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
        testSerialize(str1, "['\\u0041']", "['A']");
        testSerialize(str1, "['\\n']");
        testSerialize(utf8, "['\\u611b']", "['愛']");
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
            "\001\000\004\000\000\000MARK!GH",
            "\002\000\000\000\000\000!AB",
            "\000\001\003\000\000\000FRY!JH",
            "\001\001\003\000\000\000MAR!AC",
            "\002\001\003\000\000\000MAS!JH",
            "\003\002\004\000\000\000MASK!JH",
    };

    const RtlIntTypeInfo int2 = RtlIntTypeInfo(type_int, 2);
    const RtlIntTypeInfo int4 = RtlIntTypeInfo(type_int, 4);
    const RtlStringTypeInfo str1 = RtlStringTypeInfo(type_string, 1);
    const RtlStringTypeInfo str2 = RtlStringTypeInfo(type_string, 2);
    const RtlStringTypeInfo strx = RtlStringTypeInfo(type_string|RFTMunknownsize, 0);
    const RtlFieldInfo id = RtlFieldInfo("id", nullptr, &int2);
    const RtlFieldInfo extra = RtlFieldInfo("extra", nullptr, &strx);
    const RtlFieldInfo padding = RtlFieldInfo("padding", nullptr, &str1);
    const RtlFieldInfo name = RtlFieldInfo("name", nullptr, &str2);
    const RtlFieldInfo * const fields[5] = { &id, &extra, &padding, &name, nullptr };
    const RtlRecordTypeInfo recordType = RtlRecordTypeInfo(type_record, 4, fields);
    const RtlRecord record = RtlRecord(recordType, true);

    void processFilter(RowFilter & cursor, const char * originalFilter, const RtlRecord & searchRecord)
    {
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
            unsigned fieldNum = searchRecord.getFieldNum(fieldName);
            assertex(fieldNum != (unsigned) -1);
            const RtlTypeInfo *fieldType = searchRecord.queryType(fieldNum);
            Owned<IValueSet> set = createValueSet(*fieldType);
            deserializeSet(*set, equal+1);
            cursor.addFilter(*new SetFieldFilter(fieldNum, set));
        }
    }

    //testFilter("id=[1,3];name=(,GH)", { false, true, false, false, false });
    void testFilter(const char * originalFilter, const std::initializer_list<bool> & expected)
    {
        const byte * * rows = reinterpret_cast<const byte * *>(testRows);
        RowFilter cursor;
        processFilter(cursor, originalFilter, record);

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
        testFilter("id=[1]", { true, false, false, false, false, false });
        testFilter("id=[1],[2],[4],[6],[12],[23],[255],[256],[300],[301],[320]", { true, true, true, false, false, false });
        testFilter("id=[1,2]", { true, true, false, false, false, false });
        testFilter("id=(1,2]", { false, true, false, false, false, false });
        testFilter("id=[1,3];name=(,GH)", { false, true, false, false, false, false });
        testFilter("id=[1,3];name=(,GH]", { true, true, false, false, false, false });
        testFilter("extra=['MAR','MAS']", { true, false, false, true, true, false });
        testFilter("extra=('MAR','MAS')", { true, false, false, false, false, false });
        testFilter("id=(,257]", { true, true, true, true, false, false });
    }


    void testKeyed(const char * originalFilter, const char * expected)
    {
        const byte * * rows = reinterpret_cast<const byte * *>(testRows);
        RowFilter filter;
        processFilter(filter, originalFilter, record);

        InMemoryRows source(_elements_in(testRows), rows, record);
        InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
        KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);

        StringBuffer matches;
        while (searcher.next())
        {
            searcher.queryRow().lazyCalcOffsets(1);  // In unkeyed case we may not have calculated field 0 offset (though it is always going to be 0).
            matches.append(searcher.queryRow().getInt(0)).append("|");
        }

        if (!streq(matches, expected))
        {
            printf("Failure to match expected keyed filter '%s' (%s, %s)\n", originalFilter, expected, matches.str());
            CPPUNIT_ASSERT(streq(matches, expected));
        }
    }

    void testKeyed1()
    {
        testKeyed("extra=['MAR','MAS']", "1|257|258|");
        testKeyed("","1|2|256|257|258|515|");
        testKeyed("id=[1,2]","1|2|");
        testKeyed("id=[1],[256]","1|256|");
        testKeyed("id=[1],[256,280]","1|256|257|258|");
        testKeyed("id=[1],[256,280],[1000],[1023]","1|256|257|258|");
        testKeyed("id=[1],[2],[4],[6],[12],[23],[255],[256],[300],[301],[320]","1|2|256|");
        testKeyed("extra=['MAR','MAS']", "1|257|258|");
        testKeyed("extra=('MAR','MAS')", "1|");
        testKeyed("name=['AB','AC']", "2|257|");
    }

    void generateOrderedRows(PointerArray & rows, const RtlRecord & rowRecord)
    {
        //Generate rows with 3 fields.   Try and ensure:
        //  each trailing field starts with 0, non zero.
        //  the third field has significant distribution in the number of elements for each value of field2
        //  duplicates occur in the full keyed values.
        //  sometimes the next value in sequence happens to match a trailing filter condition e.g. field3=1
        //Last field First field x ranges from 0 to n1
        //Second field
        //Second field y ranges from 0 .. n2, and is included if (x + y) % 3 != 0 and (x + y) % 5 != 0
        //Third field is sparse from 0..n3.  m = ((x + y) % 11 ^2 + 1;  if (n3 + x *2 + y) % m = 0 then it is included
        unsigned n = 100000;
        unsigned f1 = 0;
        unsigned f2 = 0;
        unsigned f3 = 0;
        unsigned numf2 = 1;
        unsigned countf2 = 0;
        unsigned numf3 = 1;
        unsigned countf3 = 0;
        MemoryBuffer buff;
        for (unsigned i = 0; i < n; i++)
        {
            buff.setLength(0);
            MemoryBufferBuilder builder(buff, 0);
            size32_t offset = 0;
            offset = rowRecord.queryType(0)->buildInt(builder, offset, nullptr, f1);
            offset = rowRecord.queryType(1)->buildInt(builder, offset, nullptr, f2);
            offset = rowRecord.queryType(2)->buildInt(builder, offset, nullptr, f3);

            byte * row = new byte[offset];
            memcpy(row, buff.bufferBase(), offset);
            rows.append(row);

            unsigned pf2 = f2;
            unsigned pf3 = f3;
            if (++countf3 == numf3)
            {
                f2++;
                if (++countf2 == numf2)
                {
                    f1++;
                    f2 = i % 2;
                    numf2 = i % 23;
                    if (numf2 == 0)
                    {
                        f1++;
                        numf2 = (i % 21) + 1;
                    }
                    countf2 = 0;
                }

                f3 = i % 7;
                countf3 = 0;
                numf3 = i % 9;
                if (numf3 == 0)
                {
                    f3++;
                    numf3 = (i % 11) + 1;
                }
            }

            if (i % 5)
                f3++;
        }

        //Sort the rows - to allow different field types to be used.
        RawRowCompare compareRow(rowRecord);
        qsortvec(rows.getArray(), rows.ordinality(), compareRow);
    }

    void traceRow(const RtlRow & row)
    {
        printf("%u %u %u", (unsigned)row.getInt(0), (unsigned)row.getInt(1), (unsigned)row.getInt(2));
    }

    const RtlFieldInfo f1 = RtlFieldInfo("f1", nullptr, &int2);
    const RtlFieldInfo f2 = RtlFieldInfo("f2", nullptr, &int2);
    const RtlFieldInfo f3 = RtlFieldInfo("f3", nullptr, &int2);
    const RtlFieldInfo * const testFields[4] = { &f1, &f2, &f3, nullptr };
    const RtlRecordTypeInfo testRecordType = RtlRecordTypeInfo(type_record, 6, testFields);
    const RtlRecord testRecord = RtlRecord(testRecordType, true);

    void timeKeyedScan(const PointerArray & rows, const RtlRecord & searchRecord, const char * filterText)
    {
        RowFilter filter;
        processFilter(filter, filterText, searchRecord);

        CCycleTimer timeKeyed;
        unsigned countKeyed = 0;
        {
            InMemoryRows source(rows.ordinality(), (const byte * *)rows.getArray(), searchRecord);
            InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
            KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);

            while (searcher.next())
            {
                countKeyed++;
            }
        }
        unsigned __int64 keyedMs = timeKeyed.elapsedNs();

        CCycleTimer timeScan;
        unsigned countScan = 0;
        {
            RowScanner scanner(searchRecord, filter, rows);

            bool hasSearch = scanner.first();
            while (hasSearch)
            {
                countScan++;
                hasSearch = scanner.next();
            }
        }
        unsigned __int64 scanMs = timeScan.elapsedNs();
        CPPUNIT_ASSERT_EQUAL(countScan, countKeyed);

        printf("[%s] %u matches keyed(%" I64F "u) scan(%" I64F "u) (%.3f)\n", filterText, countScan, keyedMs, scanMs, (double)keyedMs/scanMs);
    }


    void testKeyed(const PointerArray & rows, const RtlRecord & searchRecord, const char * filterText)
    {
        RowFilter filter;
        processFilter(filter, filterText, searchRecord);

        InMemoryRows source(rows.ordinality(), (const byte * *)rows.getArray(), searchRecord);
        InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
        KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);
        RowScanner scanner(source.queryRecord(), filter, rows);

        unsigned count = 0;
        bool hasSearch = searcher.next();
        bool hasScan = scanner.first();
        while (hasSearch && hasScan)
        {
            count++;
            if (searchRecord.compare(searcher.queryRow().queryRow(), scanner.queryRow().queryRow()) != 0)
                break;

            hasSearch = searcher.next();
            hasScan = scanner.next();
        }
        if (hasSearch || hasScan)
        {
            printf("[%s] Keyed: ", filterText);
            if (hasSearch)
                traceRow(searcher.queryRow());
            else
                printf("<missing>");
            printf(" Scan: ");
            if (hasScan)
                traceRow(scanner.queryRow());
            else
                printf("<missing>");
            printf("\n");
        }
        else
        {
            const bool compareTiming = true;
            if (compareTiming)
                timeKeyedScan(rows, searchRecord, filterText);
            else
                printf("[%s] %u matches\n", filterText, count);
        }
    }


    void testKeyed2()
    {
        PointerArray rows;
        generateOrderedRows(rows, testRecord);

        testKeyed(rows, testRecord, "");
        testKeyed(rows, testRecord, "f1=[5]");
        testKeyed(rows, testRecord, "f1=[0]");
        testKeyed(rows, testRecord, "f2=[1]");
        testKeyed(rows, testRecord, "f3=[1]");
        testKeyed(rows, testRecord, "f3=[4]");
        testKeyed(rows, testRecord, "f3=[1,3]");
        testKeyed(rows, testRecord, "f3=[1],[2],[3]");
        testKeyed(rows, testRecord, "f1=[21];f2=[20];f3=[4]");
        testKeyed(rows, testRecord, "f1=[7];f3=[5]");
        testKeyed(rows, testRecord, "f1=[7,];f3=[,5]");

        ForEachItemIn(i, rows)
            delete [] (byte *)rows.item(i);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ValueSetTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ValueSetTest, "ValueSetTest");

#endif
