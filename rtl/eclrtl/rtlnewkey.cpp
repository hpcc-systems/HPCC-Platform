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
#include <algorithm>

#include "jlib.hpp"
#include "jdebug.hpp"
#include "jsort.hpp"
#include "jexcept.hpp"
#include "rtlnewkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"
#include "rtlnewkey.hpp"
#include "rtlfield.hpp"
#include "rtldynfield.hpp"

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

//Allow trailing :<length> to specify a substring match
static void readSubLength(size32_t & subLength, const char * &in)
{
    const char * filter = in;
    if (*filter == ':')
    {
        char * next;
        subLength = strtoul(filter+1, &next, 10);
        in = next;
    }
    else
        subLength = MatchFullString;
}


void readFieldFromFieldFilter(StringBuffer & fieldText, const char * & src)
{
    readUntilTerminator(fieldText, src, "=*:!");
}

void deserializeSet(ISetCreator & creator, const char * filter)
{
    while (*filter)
    {
        char startRange = *filter++;
        if (startRange != '(' && startRange != '[')
            throw MakeStringException(0, "Invalid filter string: expected [ or ( at start of range");

        StringBuffer upperString, lowerString;
        size32_t lowerSubLength = MatchFullString;
        size32_t upperSubLength = MatchFullString;
        if (*filter=='\'')
        {
            filter++;
            readString(lowerString, filter);
        }
        else
            readUntilTerminator(lowerString, filter, ":,])");
        readSubLength(lowerSubLength, filter);

        if (*filter == ',')
        {
            filter++;
            if (*filter=='\'')
            {
                filter++;
                readString(upperString, filter);
            }
            else
                readUntilTerminator(upperString, filter, ":])");
            readSubLength(upperSubLength, filter);
        }
        else
        {
            upperString.set(lowerString);
            upperSubLength = lowerSubLength;
        }

        char endRange = *filter++;
        if (endRange != ')' && endRange != ']')
            throw MakeStringException(0, "Invalid filter string: expected ] or ) at end of range");
        if (*filter==',')
            filter++;
        else if (*filter)
            throw MakeStringException(0, "Invalid filter string: expected , between ranges");

        TransitionMask lowerMask = (startRange == '(') ? CMPgt : CMPge;
        TransitionMask upperMask = (endRange == ')') ? CMPlt : CMPle;
        creator.addRange(lowerMask, lowerString, upperMask, upperString, lowerSubLength, upperSubLength);
    }
}

// A class wrapped for adding ranges to IValueSets
class ValueSetCreator : implements ISetCreator
{
public:
    ValueSetCreator(IValueSet & _set) : set(_set) {}

    virtual void addRange(TransitionMask lowerMask, const StringBuffer & lowerString, TransitionMask upperMask, const StringBuffer & upperString, size32_t lowerSubLength, size32_t upperSubLength) override
    {
        Owned<IValueTransition> lower = lowerString ? set.createUtf8Transition(lowerMask, rtlUtf8Length(lowerString.length(), lowerString), lowerString, lowerSubLength) : nullptr;
        Owned<IValueTransition> upper = upperString ? set.createUtf8Transition(upperMask, rtlUtf8Length(upperString.length(), upperString), upperString, upperSubLength) : nullptr;
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
 * A link counted version of an RtlTypeInfo
 */
class SharedRtlTypeInfo : public CInterface
{
public:
    SharedRtlTypeInfo(const RtlTypeInfo * _type) : type(_type) {}
    ~SharedRtlTypeInfo() { type->doDelete(); }

    const RtlTypeInfo * const type;
};

void getStretchedValue(MemoryBuffer & target, const RtlTypeInfo & newType, const RtlTypeInfo & oldType, const byte * value)
{
    MemoryBufferBuilder builder(target, 0);
    translateScalar(builder, 0, nullptr, newType, oldType, value);
}

//---------------------------------------------------------------------------------------------------------------------

static bool incrementBuffer(byte *buf, size32_t size)
{
    int i = size;
    while (i--)
    {
        buf[i]++;
        if (buf[i]!=0)
            return true;
    }
    return false;
}

//---------------------------------------------------------------------------------------------------------------------

bool needToApplySubString(const RtlTypeInfo & type, const byte * value, size32_t subLength)
{
    if (subLength == MatchFullString)
        return false;

    return type.compareRange(subLength, value, MatchFullString, value) != 0;
}

void applySubString(TransitionMask & mask, const RtlTypeInfo & type, MemoryBuffer & buff, const byte * value, size32_t subLength)
{
    //Unusual: the string being searched against is larger than the substring being applied.  Truncate it to
    //the substring length, and then change it back to the field string length
    FieldTypeInfoStruct info;
    info.fieldType = (type.fieldType & ~RFTMunknownsize);
    info.length = subLength;
    Owned<SharedRtlTypeInfo> subType(new SharedRtlTypeInfo(info.createRtlTypeInfo()));
    const RtlTypeInfo & truncType = *subType->type;

    MemoryBuffer resized;
    getStretchedValue(resized, truncType, type, value);

    MemoryBuffer stretched;
    getStretchedValue(stretched, type, truncType, resized.bytes());

    //Work around for a problem comparing data values
    if (type.compare((const byte *)stretched.toByteArray(), value) != 0)
    {
        // x >= 'ab' becomes x > 'a' => clear the equal item
        // x < 'ab' becomes x <= 'a' => set the equal item
        if (mask & CMPlt)
            mask |= CMPeq;
        else if (mask & CMPgt)
            mask &= ~CMPeq;
    }
    //value may have come from buff, so only modify the output when the function is finished.
    stretched.swapWith(buff);
}

void checkSubString(TransitionMask & mask, const RtlTypeInfo & type, MemoryBuffer & buff, size32_t subLength)
{
    if (buff.length() == 0)
        return;

    // Treat myfield[1..-1] as [1..0] - same as substring implementation.
    if ((int)subLength < 0)
        subLength =0;

    const byte * value = (const byte *)buff.toByteArray();
    if (needToApplySubString(type, value, subLength))
        applySubString(mask, type, buff, value, subLength);
}


/*
 * This class represents a value and a comparison condition and is used for representing ranges of values
 *
 * A lowerbound will always have the CMPgt bit set in the mask, and upper bound will have CMPlt set in the mask.
 * The value is always represented in the same way as a field of that type would be in a record.
 */

class ValueTransition : implements CInterfaceOf<IValueTransition>
{
public:
    ValueTransition(TransitionMask _mask, const RtlTypeInfo & type, const void *_value, size32_t _subLength)
    : mask(_mask), subLength(_subLength)
    {
        // Treat myfield[1..-1] as [1..0] - same as substring implementation.
        if ((int)subLength < 0)
            subLength = 0;

        dbgassertex(_value || isMinimum() || isMaximum());
        if (_value)
        {
            size32_t size = type.size((const byte *)_value, nullptr);
            value.allocateN(size, false);
            memcpy(value, _value, size);
        }
        //A subset >= length of the field is discarded
        if (matchSubString() && type.isFixedSize())
        {
            if (subLength >= type.getMinSize())
                subLength = MatchFullString;
        }
    }
    ValueTransition(const RtlTypeInfo & type, MemoryBuffer & in)
    {
        byte inmask;
        in.read(inmask);

        mask = (TransitionMask)(inmask & ~(CMPnovalue|CMPsubstring));
        if (!(inmask & CMPnovalue))
        {
            size32_t size = type.size(in.readDirect(0), nullptr);
            value.allocateN(size, false);
            in.read(size, value);
        }

        if (inmask & CMPsubstring)
            in.readPacked(subLength);
    }
    ValueTransition(TransitionMask _mask) : mask(_mask)
    {
        dbgassertex(isMaximum() || isMinimum());
    }

    bool equals(const RtlTypeInfo & type, const ValueTransition & other) const
    {
        if (mask != other.mask)
            return false;
        if (subLength != other.subLength)
            return false;
        if (value && other.value)
        {
            return type.compare(value, other.value) == 0;
        }
        else
            return !value && !other.value;
    }

    const byte *queryValue() const
    {
        return value;
    }

    MemoryBuffer & serialize(const RtlTypeInfo & type, MemoryBuffer & out) const
    {
        byte outmask = mask;
        if (!value)
            outmask |= CMPnovalue;
        if (matchSubString())
            outmask |= CMPsubstring;

        out.append(outmask);
        if (value)
        {
            size32_t size = type.size(value, nullptr);
            out.append(size, value);
        }
        if (matchSubString())
            out.appendPacked(subLength);
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
        if (matchSubString())
            return type.compareRange(subLength, field, subLength, value.get());

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

        if (matchSubString() || other.matchSubString())
        {
            size32_t minLength = std::min(subLength, other.subLength);
            int compare = type.compareRange(minLength, value, minLength, other.value);
            if ((compare == 0) && (subLength != other.subLength))
            {
                //If the start matches, but the lengths are different, whether the shorter string is a lower bound is key
                if (subLength < other.subLength)
                {
                    //E.g. compre 'a:1' with 'aa'.
                    if (isLowerBound())
                        return -1;
                    else
                        return +1;
                }
                else
                {
                    //E.g. Compare 'aa' with 'a:1'.
                    if (other.isLowerBound())
                        return +1;
                    else
                        return -1;
                }
            }
            return compare;
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
                //MORE: If the field type is a string or data it may contain characters that are not technically supported in utf8 e.g. \0
                appendUtf8AsECL(out, size, text.getstr());
                out.append("'");
            }
            if (matchSubString())
                out.append(":").append(subLength);
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
            return new ValueTransition(newMask, type, value, subLength);
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

    ValueTransition * cast(const RtlTypeInfo & newType, const RtlTypeInfo & oldType)
    {
        if (!value)
            return LINK(this);
        if (matchSubString())
            throwUnexpected();

        MemoryBuffer resized;
        getStretchedValue(resized, newType, oldType, value);

        MemoryBuffer recast;
        getStretchedValue(recast, oldType, newType, resized.bytes());

        TransitionMask newMask = mask;
        if (oldType.compare(value, recast.bytes()) != 0)
        {
            // x >= 'ab' becomes x > 'a' => clear the equal item
            // x < 'ab' becomes x <= 'a' => set the equal item
            if (newMask & CMPlt)
                newMask |= CMPeq;
            else if (newMask & CMPgt)
                newMask &= ~CMPeq;
        }
        return new ValueTransition(newMask, newType, resized.toByteArray(), MatchFullString);
    }

    bool isLowerBound() const { return (mask & CMPgt) != 0; }
    bool isUpperBound() const { return (mask & CMPlt) != 0; }
    bool isInclusiveBound() const { return (mask & CMPeq) != 0; }
    bool isMinimum() const { return (mask & CMPmin) != 0; }
    bool isMaximum() const { return (mask & CMPmax) != 0; }
    bool matchSubString() const { return (subLength != MatchFullString); }

    void setLow(void * buffer, size32_t offset, const RtlTypeInfo &type) const
    {
        byte *dst = ((byte *) buffer) + offset;
        type.setLowBound(dst, value, subLength, isInclusiveBound());
    }
    void setHigh(void * buffer, size32_t offset, const RtlTypeInfo &type) const
    {
        //Variable size fields cannot sensibly implement this function
        byte *dst = ((byte *) buffer) + offset;
        type.setHighBound(dst, value, subLength, isInclusiveBound());
    }
private:
    TransitionMask mask;
    size32_t subLength = MatchFullString; //MORE: Substrings could use a derived class to avoid the space
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
        return new ValueTransition(mask, type, buff.toByteArray(), MatchFullString);
    }
    virtual IValueTransition * createStringTransition(TransitionMask mask, size32_t len, const char * value, size32_t subLength) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildString(builder, 0, nullptr, len, value);
        checkSubString(mask, type, buff, subLength);
        return new ValueTransition(mask, type, buff.toByteArray(), subLength);
    }
    virtual IValueTransition * createUtf8Transition(TransitionMask mask, size32_t len, const char * value, size32_t subLength) const override
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildUtf8(builder, 0, nullptr, len, value);
        checkSubString(mask, type, buff, subLength);
        return new ValueTransition(mask, type, buff.toByteArray(), subLength);
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
        return new ValueTransition(mask, type, value, MatchFullString);
    }

    virtual ValueTransition * createRawTransitionEx(TransitionMask mask, const void * rawvalue, size32_t subLength) const override
    {
        // Treat myfield[1..-1] as [1..0] - same as substring implementation.
        if ((int)subLength < 0)
            subLength =0;

        const byte * value = (const byte *)rawvalue;
        if (likely(!needToApplySubString(type, value, subLength)))
            return new ValueTransition(mask, type, value, subLength);

        MemoryBuffer tempBuffer;
        applySubString(mask, type, tempBuffer, value, subLength);
        return new ValueTransition(mask, type, tempBuffer.toByteArray(), subLength);
    }

    virtual void addRawRange(const void * lower, const void * upper) override
    {
        addRawRangeEx(lower, upper, MatchFullString);
    }

    virtual void killRawRange(const void * lower, const void * upper) override
    {
        killRawRangeEx(lower, upper, MatchFullString);
    }

    virtual void addRawRangeEx(const void * lower, const void * upper, size32_t subLength) override
    {
        Owned<ValueTransition> lowerBound = lower ? createRawTransitionEx(CMPge, lower, subLength) : nullptr;
        Owned<ValueTransition> upperBound = upper ? createRawTransitionEx(CMPle, upper, subLength) : nullptr;
        addRange(lowerBound, upperBound);
    }

    virtual void killRawRangeEx(const void * lower, const void * upper, size32_t subLength) override
    {
        Owned<ValueTransition> lowerBound = lower ? createRawTransitionEx(CMPge, lower, subLength) : nullptr;
        Owned<ValueTransition> upperBound = upper ? createRawTransitionEx(CMPle, upper, subLength) : nullptr;
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

    virtual IValueSet * cast(const RtlTypeInfo & newType) const
    {
        Owned<IValueSet> newSet = new ValueSet(newType);
        unsigned max = transitions.ordinality();
        for (unsigned i=0; i < max; i += 2)
        {
            Owned<IValueTransition> newLower = transitions.item(i).cast(newType, type);
            Owned<IValueTransition> newUpper = transitions.item(i+1).cast(newType, type);
            newSet->addRange(newLower, newUpper);
        }
        return newSet.getClear();
    }

// Methods for using a value set
    virtual bool isWild() const override;
    virtual const void *querySingleValue() const override;

    virtual unsigned numRanges() const override;

    virtual int compareLowest(const byte * field, unsigned range) const override;

    virtual int compareHighest(const byte * field, unsigned range) const override;

    virtual int findForwardMatchRange(const byte * field, unsigned & matchRange) const override;   // why int - seems to return bool

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

    virtual void setLow(void *buffer, size32_t offset, const RtlTypeInfo &parentType) const override
    {
        dbgassertex(type.isFixedSize() && parentType.isFixedSize());
        queryTransition(0)->setLow(buffer, offset, type);
        if (&parentType != &type)
        {
            unsigned fullSize = parentType.getMinSize();
            unsigned subSize = type.getMinSize();
            assertex(subSize <= fullSize);
            memset(((byte *) buffer) + offset + subSize, 0, fullSize-subSize);
        }
    }

    virtual bool incrementKey(void *buffer, size32_t offset, const RtlTypeInfo &parentType) const override
    {
        dbgassertex(type.isFixedSize() && parentType.isFixedSize());
        byte *ptr = ((byte *) buffer) + offset;
        bool ok = incrementBuffer(ptr, type.getMinSize());
        if (ok)
        {
            unsigned nextRange;
            bool res = findForwardMatchRange(ptr, nextRange);
            if (!res)
            {
                if (nextRange == numRanges())
                    return false;
                queryTransition(nextRange*2)->setLow(buffer, offset, type);
                if (&parentType != &type)
                {
                    unsigned fullSize = parentType.getMinSize();
                    unsigned subSize = type.getMinSize();
                    dbgassertex(subSize <= fullSize);
                    memset(((byte *) buffer) + offset + subSize, 0, fullSize-subSize);
                }
            }
        }
        return ok;
    }

    virtual void endRange(void *buffer, size32_t offset, const RtlTypeInfo &parentType) const override
    {
        dbgassertex(type.isFixedSize() && parentType.isFixedSize());
        byte *ptr = ((byte *) buffer) + offset;
        unsigned nextRange;
        bool res = findForwardMatchRange(ptr, nextRange);
        assertex(res);
        queryTransition(nextRange*2+1)->setHigh(buffer, offset, type);
        if (&parentType != &type)
        {
            unsigned fullSize = parentType.getMinSize();
            unsigned subSize = type.getMinSize();
            dbgassertex(subSize <= fullSize);
            memset(((byte *) buffer) + offset + subSize, 0xff, fullSize-subSize);
        }
    }

    virtual void setHigh(void *buffer, size32_t offset, const RtlTypeInfo &parentType) const override
    {
        dbgassertex(type.isFixedSize() && parentType.isFixedSize());
        queryTransition(numTransitions()-1)->setHigh(buffer, offset, type);
        if (&parentType != &type)
        {
            unsigned fullSize = parentType.getMinSize();
            unsigned subSize = type.getMinSize();
            dbgassertex(subSize <= fullSize);
            memset(((byte *) buffer) + offset + subSize, 0xff, fullSize-subSize);
        }
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

const void *ValueSet::querySingleValue() const
{
    if (transitions.ordinality() == 2)
    {
        ValueTransition *  lower = queryTransition(0);
        ValueTransition *  upper = queryTransition(1);

        if (lower->isInclusiveBound() && !lower->matchSubString() && upper->isInclusiveBound() && !upper->matchSubString())
        {
            if (type.compare(queryTransition(0)->queryValue(), queryTransition(1)->queryValue())==0)
                return queryTransition(0)->queryValue();
        }
    }
    return nullptr;
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

    virtual bool getBloomHash(hash64_t &hash) const override
    {
        return false;
    }

    virtual unsigned queryScore() const override
    {
        // MORE - the score should probably depend on the number and nature of ranges too.
        unsigned score = type.getMinSize();
        if (!score)
            score = 5;   // Arbitrary guess for average field length in a variable size field
        return score;
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
    SetFieldFilter(unsigned _field, const RtlTypeInfo & fieldType, IValueSet * _values) : FieldFilter(_field, fieldType), values(_values) {}

//Simple row matching
    virtual bool matches(const RtlRow & row) const override;
    virtual bool isEmpty() const override;
    virtual bool isWild() const override;

//More complex index matching
    virtual unsigned numRanges() const override;
    virtual int compareLowest(const RtlRow & left, unsigned range) const override;
    virtual int compareHighest(const RtlRow & left, unsigned range) const override;
    virtual int findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const override;

    virtual IFieldFilter *remap(unsigned newField) const override { return new SetFieldFilter(newField, values); }
    virtual StringBuffer & serialize(StringBuffer & out) const override;
    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override;

// jhtree
    virtual void setLow(void *buffer, size32_t offset) const override;
    virtual bool incrementKey(void *buffer, size32_t offset) const override;
    virtual void endRange(void *buffer, size32_t offset) const override;
    virtual void setHigh(void *buffer, size32_t offset) const override;

// Human-readable description for tracing/debugging
    virtual StringBuffer &describe(StringBuffer &out) const override;

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

StringBuffer &SetFieldFilter::describe(StringBuffer &out) const
{
    // MORE - could consider abbreviating very large sets...
    return values->serialize(out);
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

StringBuffer & SetFieldFilter::serialize(StringBuffer & out) const
{
    out.append(field).append('=');
    return values->serialize(out);
}
MemoryBuffer & SetFieldFilter::serialize(MemoryBuffer & out) const
{
    out.appendPacked(field).append('=');
    return values->serialize(out);
}

void SetFieldFilter::setLow(void *buffer, size32_t offset) const
{
    dbgassertex(!isEmpty());
    values->setLow(buffer, offset, type);
}

bool SetFieldFilter::incrementKey(void *buffer, size32_t offset) const
{
    return values->incrementKey(buffer, offset, type);
}

void SetFieldFilter::endRange(void *buffer, size32_t offset) const
{
    values->endRange(buffer, offset, type);
}
void SetFieldFilter::setHigh(void *buffer, size32_t offset) const
{
    values->setHigh(buffer, offset, type);
}

//---------------------------------------------------------------------------------------------------------------------

class SingleFieldFilter : public FieldFilter
{
public:
    SingleFieldFilter(unsigned _field, const RtlTypeInfo & fieldType, const byte * _value);
    ~SingleFieldFilter();

//Simple row matching
    virtual bool matches(const RtlRow & row) const override;
    virtual bool isEmpty() const override { return false; }
    virtual bool isWild() const override { return false; }

//More complex index matching
    virtual unsigned numRanges() const override { return 1; };
    virtual int compareLowest(const RtlRow & left, unsigned range) const override;
    virtual int compareHighest(const RtlRow & left, unsigned range) const override;
    virtual int findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const override;

    virtual IFieldFilter *remap(unsigned newField) const override { return new SingleFieldFilter(newField, type, value); }
    virtual StringBuffer & serialize(StringBuffer & out) const override;
    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override;

// jhtree
    virtual bool getBloomHash(hash64_t &hash) const override;
    virtual void setLow(void *buffer, size32_t offset) const override;
    virtual bool incrementKey(void *buffer, size32_t offset) const override;
    virtual void endRange(void *buffer, size32_t offset) const override;
    virtual void setHigh(void *buffer, size32_t offset) const override;

    // Human-readable description for tracing/debugging
    virtual StringBuffer &describe(StringBuffer &out) const override;

protected:
    const byte *value;
};

SingleFieldFilter::SingleFieldFilter(unsigned _field, const RtlTypeInfo & fieldType, const byte * _value)
: FieldFilter(_field, fieldType)
{
    size32_t size = type.size(_value, nullptr);
    byte *val = (byte *) malloc(size);
    memcpy(val, _value, size);
    value = val;
}
SingleFieldFilter::~SingleFieldFilter()
{
    free((void *) value);
}

bool SingleFieldFilter::matches(const RtlRow & row) const
{
    return type.compare(row.queryField(field), value) == 0;
}

int SingleFieldFilter::compareLowest(const RtlRow & row, unsigned range) const
{
    dbgassertex(!range);
    return type.compare(row.queryField(field), value);
}

int SingleFieldFilter::compareHighest(const RtlRow & row, unsigned range) const
{
    dbgassertex(!range);
    return type.compare(row.queryField(field), value);
}

bool SingleFieldFilter::getBloomHash(hash64_t &hash) const
{
    if (!value)
        return false;
    hash = rtlHash64Data(type.size(value, nullptr), value, hash);
    return true;
}

int SingleFieldFilter::findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const
{
    int rc = type.compare(row.queryField(field), value);
    if (rc==0)
    {
        matchRange = 0;
        return true;
    }
    else if (rc < 0)
    {
        matchRange = 0;
    }
    else
    {
        matchRange = 1;
    }
    return false;
}

StringBuffer & SingleFieldFilter::serialize(StringBuffer & out) const
{
    out.append(field).append("=[");
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
    return out.append("]");
}

StringBuffer &SingleFieldFilter::describe(StringBuffer &out) const
{
    size32_t size;
    rtlDataAttr text;
    type.getString(size, text.refstr(), value);
    if (type.isNumeric())
        return out.append(size, text.getstr());
    else
        return out.appendf("'%*s'", size, text.getstr());
}


MemoryBuffer & SingleFieldFilter::serialize(MemoryBuffer & out) const
{
    out.appendPacked(field).append('=');
    out.appendPacked(1);  // Signifying a single transition - which in turn means a SingleFieldFilter not a set.
    size32_t size = type.size(value, nullptr);
    out.append(size, value);
    return out;
}

void SingleFieldFilter::setLow(void *buffer, size32_t offset) const
{
    memcpy(((byte *)buffer) + offset, value, type.size(value, nullptr));
}

bool SingleFieldFilter::incrementKey(void *buffer, size32_t offset) const
{
    // Set to next permitted value above current
    byte *ptr = ((byte *)buffer) + offset;
    if (type.compare(ptr, value) < 0)
    {
        memcpy(ptr, value, type.size(value, nullptr));
        return true;
    }
    else
        return false;
}

void SingleFieldFilter::endRange(void *buffer, size32_t offset) const
{
    // Set to last permitted value in the range that includes current (which is asserted to be valid)
    dbgassertex(type.compare(((byte *)buffer) + offset, value)==0);
}
void SingleFieldFilter::setHigh(void *buffer, size32_t offset) const
{
    memcpy(((byte *)buffer) + offset, value, type.size(value, nullptr));
}

IFieldFilter * createFieldFilter(unsigned fieldId, IValueSet *values)
{
    const void *single = values->querySingleValue();
    if (single)
        return new SingleFieldFilter(fieldId, values->queryType(), (const byte *) single);
    else if (values->isWild())
        return createWildFieldFilter(fieldId, values->queryType());
    else
        return new SetFieldFilter(fieldId, values);
}

IFieldFilter * createEmptyFieldFilter(unsigned fieldId, const RtlTypeInfo & type)
{
    Owned<IValueSet> values = createValueSet(type);
    return new SetFieldFilter(fieldId, values);
}

IFieldFilter * createFieldFilter(unsigned fieldId, const RtlTypeInfo & type, const void * value)
{
    return new SingleFieldFilter(fieldId, type, (const byte *) value);
}


//---------------------------------------------------------------------------------------------------------------------

/*
 * A field filter that can match a set of values.
 */
class SubStringFieldFilter : public SetFieldFilter
{
public:
    SubStringFieldFilter(unsigned _field, const RtlTypeInfo & _fieldType, SharedRtlTypeInfo * _subType, IValueSet * _values)
    : SetFieldFilter(_field, _fieldType, _values), subType(_subType)
    {
        subLength = subType->type->length;
    }

    virtual StringBuffer & serialize(StringBuffer & out) const override;
    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override;

protected:
    Linked<SharedRtlTypeInfo> subType;
    size32_t subLength;
};

StringBuffer & SubStringFieldFilter::serialize(StringBuffer & out) const
{
    out.append(field).append(':').append(subLength).append("=");
    return values->serialize(out);
}

MemoryBuffer & SubStringFieldFilter::serialize(MemoryBuffer & out) const
{
    out.appendPacked(field).append(':').append(subLength);
    return values->serialize(out);
}


class FixedSubStringFieldFilter : public SubStringFieldFilter
{
public:
    FixedSubStringFieldFilter(unsigned _field, const RtlTypeInfo & _fieldType, SharedRtlTypeInfo * _subType, IValueSet * _values)
    : SubStringFieldFilter(_field, _fieldType, _subType, _values)
    {
    }

    virtual IFieldFilter *remap(unsigned newField) const override { return new SubStringFieldFilter(newField, type, subType, values); }
};



class VariableSubStringFieldFilter : public SubStringFieldFilter
{
public:
    VariableSubStringFieldFilter(unsigned _field, const RtlTypeInfo & _fieldType, SharedRtlTypeInfo * _subType, IValueSet * _values)
    : SubStringFieldFilter(_field, _fieldType, _subType, _values),
      maxTempLength(subLength * 4) //Maximum expansion from length to size is 4 bytes for utf8
    {
    }

//Simple row matching
    virtual bool matches(const RtlRow & row) const override;

//More complex index matching
    virtual int compareLowest(const RtlRow & left, unsigned range) const override;
    virtual int compareHighest(const RtlRow & left, unsigned range) const override;
    virtual int findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const override;

    virtual IFieldFilter *remap(unsigned newField) const override { return new VariableSubStringFieldFilter(newField, type, subType, values); }

protected:
    const unsigned maxTempLength;
};


bool VariableSubStringFieldFilter::matches(const RtlRow & row) const
{
    const byte * ptr = row.queryField(field);
    size32_t len = *reinterpret_cast<const size32_t *>(ptr);
    if (likely(len >= subLength))
        return values->matches(ptr + sizeof(size32_t));

    //Clone and expand the string to the expected length
    byte * temp = (byte *)alloca(maxTempLength);
    RtlStaticRowBuilder builder(temp, maxTempLength);
    translateScalar(builder, 0, nullptr, *subType->type, type, ptr);
    return values->matches(temp);
}

int VariableSubStringFieldFilter::compareLowest(const RtlRow & left, unsigned range) const
{
    const byte * ptr = left.queryField(field);
    size32_t len = *reinterpret_cast<const size32_t *>(ptr);
    if (likely(len >= subLength))
        return values->compareLowest(ptr + sizeof(size32_t), range);

    //Clone and expand the string to the expected length
    byte * temp = (byte *)alloca(maxTempLength);
    RtlStaticRowBuilder builder(temp, maxTempLength);
    translateScalar(builder, 0, nullptr, *subType->type, type, ptr);
    return values->compareLowest(temp, range);
}

int VariableSubStringFieldFilter::compareHighest(const RtlRow & left, unsigned range) const
{
    const byte * ptr = left.queryField(field);
    size32_t len = *reinterpret_cast<const size32_t *>(ptr);
    if (likely(len >= subLength))
        return values->compareHighest(ptr + sizeof(size32_t), range);

    //Clone and expand the string to the expected length
    byte * temp = (byte *)alloca(maxTempLength);
    RtlStaticRowBuilder builder(temp, maxTempLength);
    translateScalar(builder, 0, nullptr, *subType->type, type, ptr);
    return values->compareHighest(temp, range);
}

int VariableSubStringFieldFilter::findForwardMatchRange(const RtlRow & row, unsigned & matchRange) const
{
    const byte * ptr = row.queryField(field);
    size32_t len = *reinterpret_cast<const size32_t *>(ptr);
    if (likely(len >= subLength))
        return values->findForwardMatchRange(ptr + sizeof(size32_t), matchRange);

    //Clone and expand the string to the expected length
    byte * temp = (byte *)alloca(maxTempLength);
    RtlStaticRowBuilder builder(temp, maxTempLength);
    translateScalar(builder, 0, nullptr, *subType->type, type, ptr);
    return values->findForwardMatchRange(temp, matchRange);
}


IFieldFilter * createSubStringFieldFilter(unsigned fieldId, size32_t subLength, IValueSet * values)
{
    const RtlTypeInfo & type = values->queryType();
    if ((int) subLength <= 0)
    {
        //Does the set include a blank value?
        MemoryBuffer blank;
        MemoryBufferBuilder builder(blank, 0);
        type.buildString(builder, 0, nullptr, 0, "");

        bool valuesMatchBlank = values->matches(blank.bytes());
        if (valuesMatchBlank)
            return createWildFieldFilter(fieldId, type);
        else
            return createEmptyFieldFilter(fieldId, type);
    }

    //Check for substring that doesn't truncate the field.
    if (type.isFixedSize())
    {
        size32_t fieldLength = type.length;
        if (subLength >= fieldLength)
            return createFieldFilter(fieldId, values);
    }

    switch (type.getType())
    {
    case type_string:
    case type_qstring:
    case type_unicode:
    case type_utf8:
    case type_data:
        break;
    default:
        throw MakeStringException(2, "Invalid type for substring filter");
    }

    //Created a truncated type
    FieldTypeInfoStruct info;
    info.fieldType = (type.fieldType & ~RFTMunknownsize);
    info.length = subLength;
    Owned<SharedRtlTypeInfo> subType(new SharedRtlTypeInfo(info.createRtlTypeInfo()));

    //Create a new set of values truncated to the appropriate length.
    Owned<IValueSet> newValues = values->cast(*subType->type);
    if (type.isFixedSize())
    {
        //The standard compare will only look at the first subLength characters.
        return new FixedSubStringFieldFilter(fieldId, type, subType, newValues);
    }

    //Check that the temporary buffer that *might* be required is a sensible size for storing on the stack
    if (subLength > 256)
        throw MakeStringException(3, "Substring [1..%u] range is too large for a variable size field", subLength);

    //Use a class which will expand the string to the sub length if it is shorter
    return new VariableSubStringFieldFilter(fieldId, type, subType, newValues);
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
        return out.append(field).append('*');
    }

    virtual StringBuffer & describe(StringBuffer & out) const override
    {
        return out.append('*');
    }

    virtual MemoryBuffer & serialize(MemoryBuffer & out) const override
    {
        return out.appendPacked(field).append('*');
    }

    virtual void setLow(void *buffer, size32_t offset) const
    {
        byte *ptr = ((byte*) buffer) + offset;
        assert(type.isFixedSize());
        memset(ptr, 0x0, type.getMinSize());
    }
    virtual bool incrementKey(void *buffer, size32_t offset) const override
    {
        byte *ptr = ((byte*) buffer) + offset;
        assert(type.isFixedSize());
        return incrementBuffer(ptr, type.getMinSize());
    }
    virtual void endRange(void *buffer, size32_t offset) const
    {
        setHigh(buffer, offset);
    }
    virtual void setHigh(void *buffer, size32_t offset) const
    {
        byte *ptr = ((byte*) buffer) + offset;
        assert(type.isFixedSize());
        memset(ptr, 0xff, type.getMinSize());
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
    case '!':
        //This syntax is added as a convenience when processing from a user supplied string
        if (src[1] == '=')
        {
            Owned<IValueSet> values = createValueSet(type);
            deserializeSet(*values, src+2);
            values->invertSet();
            return createFieldFilter(fieldId, values);
        }
        break;
    case '=':
        {
            Owned<IValueSet> values = createValueSet(type);
            deserializeSet(*values, src+1);
            return createFieldFilter(fieldId, values);
        }
    case ':':
        {
            char * end;
            size32_t subLength = strtoul(src+1, &end, 10);
            //This could support !, but this is a legacy syntax : is now specified as part of the set
            if (*end != '=')
                UNIMPLEMENTED_X("Expected =");

            Owned<IValueSet> set = createValueSet(type);
            deserializeSet(*set, end+1);
            return createSubStringFieldFilter(fieldId, subLength, set);
        }
    }

    UNIMPLEMENTED_X("Unknown Field Filter");
}

IFieldFilter * deserializeFieldFilter(const RtlRecord & record, const char * src)
{
    StringBuffer fieldText;
    readFieldFromFieldFilter(fieldText, src);
    unsigned fieldNum;
    if (isdigit(fieldText.str()[0]))
        fieldNum = atoi(fieldText.str());
    else
        fieldNum = record.getFieldNum(fieldText);
    if (((unsigned)-1) == fieldNum)
        return nullptr;
    else
        return deserializeFieldFilter(fieldNum, *record.queryType(fieldNum), src);
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
            unsigned numTransitions;
            size32_t pos = in.getPos();
            in.readPacked(numTransitions);
            if (numTransitions==1)
            {
                size32_t size = type.size(in.readDirect(0), nullptr);
                return new SingleFieldFilter(fieldId, type, (const byte *) in.readDirect(size));
            }
            else
            {
                in.reset(pos);
                Owned<IValueSet> values = createValueSet(type, in);
                return createFieldFilter(fieldId, values);
            }
        }
    case ':':
        {
            size32_t subLength;
            in.read(subLength);

            //Created a truncated type
            FieldTypeInfoStruct info;
            info.fieldType = (type.fieldType & ~RFTMunknownsize);
            info.length = subLength;
            Owned<SharedRtlTypeInfo> subType(new SharedRtlTypeInfo(info.createRtlTypeInfo()));

            //The serialized set is already truncated to the appropriate length.
            Owned<IValueSet> values = createValueSet(*subType->type, in);
            if (type.isFixedSize())
            {
                return new FixedSubStringFieldFilter(fieldId, type, subType, values);
            }

            return new VariableSubStringFieldFilter(fieldId, type, subType, values);
        }
    }

    throwUnexpectedX("Unknown Field Filter");
}

IFieldFilter * deserializeFieldFilter(const RtlRecord & searchRecord, MemoryBuffer & in)
{
    unsigned fieldNum;
    in.readPacked(fieldNum);
    return deserializeFieldFilter(fieldNum, *searchRecord.queryType(fieldNum), in);
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
    filters.append(filter);
    unsigned fieldNum = filter.queryFieldIndex();
    if (fieldNum >= numFieldsRequired)
        numFieldsRequired = fieldNum+1;
}

const IFieldFilter & RowFilter::addFilter(const RtlRecord & record, const char * filterText)
{
    IFieldFilter & filter = *deserializeFieldFilter(record, filterText);
    filters.append(filter);
    unsigned fieldNum = filter.queryFieldIndex();
    if (fieldNum >= numFieldsRequired)
        numFieldsRequired = fieldNum+1;
    return filter;
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

void RowFilter::appendFilters(const IConstArrayOf<IFieldFilter> & _filters)
{
    ForEachItemIn(i, _filters)
    {
        addFilter(OLINK(_filters.item(i)));
    }
}

void RowFilter::createSegmentMonitors(IIndexReadContext *irc)
{
    ForEachItemIn(i, filters)
        irc->append(FFkeyed, LINK(&filters.item(i)));
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

RowFilter & RowFilter::clear()
{
    filters.kill();
    numFieldsRequired = 0;
    return *this;
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
    }

    nextUnmatchedRange = matchRange;
    numMatched = field-1;
    return false;
}


//---------------------------------------------------------------------------------------------

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

