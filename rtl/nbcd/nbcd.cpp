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

#include "platform.h"
#include "nbcd.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"

#ifdef _WIN32
 #define NOMEMCPY volatile          // stop VC++ doing a stupid optimization
#else
 #define NOMEMCPY
#endif

static double Pow10[] = { 1, 10, 100, 1000, 10000, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,1e11,1e12,1e13,1e14,1e15,1e16 };
static int signMap[16] = { 0,0,0,0,0,0,0,0,0,0,+1,-1,+1,-1,+1,+1 };

Decimal & Decimal::abs()
{
    negative = false;
    return *this;
}


Decimal & Decimal::add(const Decimal & other)
{
    if (negative == other.negative)
        return addDigits(other);
    else
        return subtractDigits(other);
}



Decimal & Decimal::addDigits(const Decimal & other)
{
    extendRange(other);
    byte oLo = other.lsb;
    byte oHi = other.msb;
    byte hi = msb;

    unsigned idx;
    byte carry = 0;
    for (idx = oLo; (idx <= oHi); idx++)
    {
        digits[idx] += other.digits[idx] + carry;
        carry = 0;
        if (digits[idx] > 9)
        {
            carry++;
            digits[idx] -= 10;
        }
    }

    for (;carry && idx <= hi; idx++)
    {
        digits[idx]++;
        carry = 0;
        if (digits[idx] > 9)
        {
            carry = 1;
            digits[idx] -= 10;
        }
    }

    if (carry && hi != lastDigit)
    {
        digits[++hi] = carry;
        msb = hi;
    }
    return *this;
}

int Decimal::compareNull() const
{
    byte idx;
    for (idx = lsb; idx <= msb; idx++)
    {
        if (digits[idx]) return negative ? -1 : +1;
    }
    return 0;
}


int Decimal::compare(const Decimal & other) const
{
    int lo1, hi1, lo2, hi2;
    clip(lo1, hi1);
    other.clip(lo2, hi2);

    //First check for zero comparison..
    if (lo1 > hi1)
    {
        if (lo2 > hi2)
            return 0;
        return other.negative ? +1 : -1;
    }
    if (lo2 > hi2)
        return negative ? -1 : +1;

    if (negative ^ other.negative)
        return negative ? -1 : +1;

    if (hi1 != hi2)
        return (hi1 > hi2) ^ negative ? +1 : -1;

    int limit = lo1 < lo2 ? lo2 : lo1;
    for (;hi1 >= limit; hi1--)
    {
        int diff = digits[hi1] - other.digits[hi1];
        if (diff != 0)
            return (diff > 0) ^ negative ? +1 : -1;
    }

    if (lo1 == lo2)
        return 0;
    if (lo1 < lo2)
        return negative ? -1 : +1;
    return negative ? +1 : -1;
}


Decimal & Decimal::divide(const Decimal & other)
{
    //NB: Round towards zero
    int lo1, hi1, lo2, hi2;
    clip(lo1, hi1);
    other.clip(lo2, hi2);

    int nd1 = hi1+1-lo1;
    int nd2 = hi2+1-lo2;
    int hi = (hi1-hi2)+zeroDigit; // how many digits will there be in the result
    int iters = hi+1;
    if (hi < 0)
    {
        setZero();
        return *this;
    }
    if (hi2 < lo2)
    {
        //Division by zero defined to return 0 instead of throw an exception
        setZero();
        return *this;
    }

    lsb = 0;
    msb = hi >= maxDigits ? maxDigits-1 : hi;

    const byte spare = 3;
    byte temp[maxDigits*2 + 3];
    unsigned numeratorDigits = (hi + 1) + nd2;
    memset(temp, 0, numeratorDigits+spare);             // ensure two zero in msb, and below lsb.  Also 2 zeros for looking 2 bytes ahead..

    byte * numerator = temp+spare;
    if (numeratorDigits > (unsigned)nd1)
        memcpy(numerator + numeratorDigits - 1 - nd1, digits+lo1, nd1);
    else
        memcpy(numerator, digits + hi1 + 1 - (numeratorDigits-1), numeratorDigits-1);

    unsigned divisor01 = other.digits[hi2] * 10;
    if (hi2 != lo2)
        divisor01 += other.digits[hi2-1];

    //MORE: Terminate early for exact divide..
    const byte * divisor = other.digits + lo2;
    for (int iter = iters; iter--; )
    {
        //The following guess for q is never too small, may be 1 too large
        byte * curNumerator = numerator + iter;
        unsigned numerator012 = curNumerator[nd2] * 100 + curNumerator[nd2-1] * 10 + curNumerator[nd2-2];
        unsigned q = numerator012 / divisor01;
        if (q == 10) q--;

        if (q)
        {
            unsigned carry = 0;
            for (int i = 0; i < nd2; i++)
            {
                int next = 90 + curNumerator[i] - divisor[i] * q - carry;
                div_t values = div(next, 10);
                carry = 9 - values.quot;
                curNumerator[i] = values.rem;
            }
            carry -= curNumerator[nd2];
            if (carry)
            {
                q--;
                assertex(carry==1);
                carry = 0;
                for (int i = 0; i < nd2; i++)
                {
                    byte next = curNumerator[i] + divisor[i] + carry;
                    carry = 0;
                    if (next >= 10)
                    {
                        next -= 10;
                        carry = 1;
                    }
                    curNumerator[i] = next;
                }
                assertex(carry);
            }
        }
        if (iter < maxDigits)
            digits[iter] = q;
    }
    //MORE: This should really calculate the next digit, and conditionally round the least significant digit.
    if (true)
    {
        //The following guess for q is never too small, may be 1 too large
        byte * curNumerator = numerator-1;
        unsigned numerator012 = curNumerator[nd2] * 100 + curNumerator[nd2-1] * 10 + curNumerator[nd2-2];
        unsigned q = numerator012 / divisor01;
        if (q == 5)
        {
            unsigned carry = 0;
            for (int i = 0; i < nd2; i++)
            {
                int next = 90 + curNumerator[i] - divisor[i] * q - carry;
                carry = 9 - next / 10;
            }
            carry -= curNumerator[nd2];
            if (carry)
                q--;
        }

        if (q >= 5)
        {
            for (int roundDigit=0; roundDigit < iters; roundDigit++)
            {
                unsigned next = digits[roundDigit]+1;
                if (next == 10)
                    next = 0;
                digits[roundDigit] = next;
                if (next != 0)
                    break;
            }
        }
    }

    negative ^= other.negative;
    return *this;
}


void Decimal::extendRange(byte oLsb, byte oMsb)
{
    byte index;
    if (lsb > oLsb)
    {
        for (index = oLsb; index != lsb; index++)
            digits[index] =0;
        lsb = oLsb;
    }
    if (msb < oMsb)
    {
        for (index = msb+1; index <= oMsb; index++)
            digits[index] = 0;
        msb = oMsb;
    }
}


bool Decimal::isZero() const
{
    //NB: Round towards zero
    int lo, hi;
    clip(lo, hi);
    return (hi < lo);
}

Decimal & Decimal::modulus(const Decimal & other)
{
    Decimal left(*this);
    left.divide(other).truncate(0).multiply(other);
    return subtract(left);
}


Decimal & Decimal::multiply(const Decimal & other)
{
    int low1, high1, low2, high2, lowt, hight;

    clip(low1, high1);
    other.clip(low2, high2);
    lowt = low1+low2-zeroDigit;
    if (lowt < 0) lowt = 0;
    hight = high1 + high2 - zeroDigit;
    if (hight >= maxDigits) hight = maxDigits-1;
    else if (hight < 0)
    {
        if (hight < -1)
        {
            setZero();
            return *this;
        }
        hight = 0;
    }


    unsigned temp[maxDigits*2];
    _clear(temp);
//  memset(temp+low1+low2, 0, (high1+high2-low1-low2+2)*sizeof(unsigned));  // only need to clear part of the target we're adding to.

    //More: could copy across 1st time round - might be worth it.
    const byte * digits1 = digits;
    const byte * digits2 = other.digits;
    for (int i = low1; i <= high1; i++)
    {
        byte next = digits1[i];
        if (next)
        {
            for (int j=low2; j <= high2; j++)
                temp[i+j] += next * digits2[j];
        }
    }

    //Now copy the results, taking care of the carries
    unsigned carry = 0;
    int j;
    for (j = low1+low2 - zeroDigit; j < lowt; j++)
    {
        unsigned next = temp[j+zeroDigit]+carry;
        //Round the least significant digit
        if (j+1 == lowt)
            next += 5;
        carry = next / 10;
    }

    for (j = lowt; j <= hight; j++)
    {
        div_t next = div(temp[j+zeroDigit]+carry, 10);
        digits[j] = next.rem;
        carry = next.quot;
    }
    while ((hight < maxDigits-1) && (carry != 0))
    {
        digits[++hight] = carry % 10;
        carry = carry / 10;
    }

    lsb = lowt;
    msb = hight;
    negative ^= other.negative;
    return *this;
}


Decimal & Decimal::negate()
{
    negative = !negative;
    return *this;
}

Decimal & Decimal::power(unsigned value)
{
    if (value == 0)
        setInt(1);
    else
        doPower(value);
    return *this;
}


Decimal & Decimal::power(int value)
{
    if ( value >= 0)
        return power((unsigned)value);

#if 1
    //This probably gives slightly more expected results, but both suffer from rounding errors.
    Decimal reciprocal;
    reciprocal.setInt(1);
    reciprocal.divide(*this);
    set(reciprocal);
    doPower((unsigned)-value);
    return *this;
#else
    doPower((unsigned)-value);
    Decimal reciprocal;
    reciprocal.setInt(1);
    reciprocal.divide(*this);
    set(reciprocal);
    return *this;
#endif
}


Decimal & Decimal::incLSD()
{
    unsigned index = lsb;
    while (index <= msb)
    {
        if (++digits[index] != 10)
        {
            lsb = index;
            return *this;
        }
        digits[index] = 0;
        index++;
    }
    digits[++msb] = 1;
    return *this;
}


Decimal & Decimal::round(int places)
{
    //out of range - either 0 or overflow
    if (places < -maxPrecision)
    {
        setZero();
        return *this;
    }
    if (zeroDigit - places <= lsb)
        return *this;

    lsb = zeroDigit - places;
    if (lsb > msb)
    {
        digits[lsb] = 0;
        if ((lsb == msb+1) && digits[msb] >= 5)
            digits[lsb]++;
        msb = lsb;
        return *this;
    }
    if (digits[lsb-1] < 5)
        return *this;

    return incLSD();
}


Decimal & Decimal::roundup(int places)
{
    if ((places >= maxPrecision) || (zeroDigit - places <= lsb))
        return *this;

    unsigned lower = lsb;
    lsb = zeroDigit - places;
    for (unsigned i=lower; i < lsb; i++)
    {
        if (digits[i])
            return incLSD();
    }
    return *this;
}


void Decimal::getPrecision(unsigned & digits, unsigned & precision)
{
    //Ensures digits>=precision && precision >= 0
    unsigned top = msb >= zeroDigit ? msb+1 : zeroDigit;
    unsigned low = lsb >= zeroDigit ? zeroDigit : lsb;
    digits = (top == low) ? 1 : top - low;
    precision = zeroDigit-low;
}

void Decimal::getClipPrecision(unsigned & digits, unsigned & precision)
{
    int lo, hi;
    clip(lo, hi);

    if (lo > hi)
    {
        digits = 1;
        precision = 0;
    }
    else
    {
        //Ensures digits>=precision && precision >= 0
        unsigned top = hi >= zeroDigit ? hi+1 : zeroDigit;
        unsigned low = lo >= zeroDigit ? zeroDigit : lo;
        digits = (top == low) ? 1 : top - low;
        precision = zeroDigit-low;
    }
}

Decimal & Decimal::setPrecision(byte numDigits, byte precision)
{
    unsigned char newhigh = zeroDigit + numDigits - precision - 1;
    unsigned char newlow = zeroDigit - precision;
    if (msb > newhigh)
        msb = newhigh;
    if (lsb < newlow)
        lsb = newlow;
    if (lsb > msb)
    {
        lsb = msb;
        digits[lsb] = 0;
    }
    return *this;
}


Decimal & Decimal::subtract(const Decimal & other)
{
    if (negative != other.negative)
        return addDigits(other);
    else
        return subtractDigits(other);
}


Decimal & Decimal::subtractDigits(const Decimal & other)
{
    extendRange(other);
    byte oLo = other.lsb;
    byte oHi = other.msb;
    byte hi = msb;

    unsigned idx;
    byte carry = 0;
    for (idx = oLo; (idx <= oHi); idx++)
    {
        int next = digits[idx] - (other.digits[idx] + carry);
        carry = 0;
        if (next < 0)
        {
            carry++;
            next += 10;
        }
        digits[idx] = next;
    }

    for (;carry && idx <= hi; idx++)
    {
        digits[idx]--;
        carry = 0;
        if (digits[idx] == 255)
        {
            carry = 1;
            digits[idx] += 10;
        }
    }

    if (carry)
    {
        //underflow => complement the result and add 1
        negative = !negative;
        carry = 1;
        for (idx = lsb; idx <= hi; idx++)
        {
            byte next = 9 - digits[idx] + carry;
            carry = 0;
            if (next == 10)
            {
                carry = 1;
                next -= 10;
            }
            digits[idx] = next;
        }
        assertex(!carry);
    }
    return *this;
}

Decimal & Decimal::truncate(int places)
{
    //out of range - either 0 or overflow
    if (places <= -maxIntegerDigits)
    {
        setZero();
        return *this;
    }

    if (zeroDigit - places > lsb)
    {
        lsb = zeroDigit - places;
        if (lsb > msb)
        {
            digits[lsb] = 0;
            msb = lsb;
        }
    }
    return *this;
}


size32_t Decimal::getStringLength() const
{
    int lo, hi;
    clip(lo, hi);

    if (lo > hi) // (lo == hi) && (digits[lo] == 0))        
        return 1;
    byte top = (hi < zeroDigit) ? zeroDigit-1 : hi;
    byte bottom = (lo > zeroDigit) ? zeroDigit : lo;

    unsigned outLen = (top + 1 - bottom);
    if (negative)           outLen++;       // '-'
    if (lo < zeroDigit) outLen++;       // '.'
    if (hi < zeroDigit) outLen++;       // '0'
    return outLen;
}


void Decimal::getCString(size32_t length, char * buffer) const
{
    unsigned len = getStringLength();
    if (len >= length)
    {
        memset(buffer, '*', length-1);
        buffer[length-1] = 0;
        return;
    }

    unsigned written = doGetString(buffer);
    assertex(len == written);
    buffer[len] = 0;
}

char * Decimal::getCString() const
{
    unsigned len = getStringLength();
    char * buffer = (char *)malloc(len+1);
    unsigned written = doGetString(buffer);
    assertex(len == written);
    buffer[len] = 0;
    return buffer;
}


void Decimal::getDecimal(byte length, byte precision, void * buffer, byte signs) const
{
    doGetDecimal(length, 2*length-1, precision, buffer);
    byte sign = negative ? (signs & 0x0f) : (signs >> 4);
    ((byte *)buffer)[length-1] |= sign;
}


__int64 Decimal::getInt64() const
{
    unsigned __int64 value = getUInt64();
    if (negative)
        return -(__int64)value;
    return (__int64)value;
}


int Decimal::getInt() const
{
    unsigned int value = getUInt();
    if (negative)
        return -(int) value;
    return (int) value;
}


double Decimal::getReal() const
{
    int lo, hi;
    clip(lo, hi);

    double total = 0;
    byte sigDigits = 16; // Only worth taking 16 places over
    int i;
    for (i = hi; sigDigits && i >= lo; i--, sigDigits-- )
        total = total * 10 + digits[i];
    i++;

    if ( i > zeroDigit )
    {
        unsigned amount = i - zeroDigit;
        while ( amount > 15 )
        {
            total *= 1e16;
            amount -= 16;
        }
        total *= Pow10[ amount ];
    }
    else if ( i < zeroDigit )
    {
        unsigned amount = zeroDigit - i;
        while ( amount > 15 )
        {
            total /= 1e16;
            amount -= 16;
        }
        total /= Pow10[ amount ];
    }

    if (negative )
        return -total;
    else
        return total;
}


void Decimal::getString(size32_t length, char * buffer) const
{
    unsigned len = getStringLength();
    if (len > length)
    {
        memset(buffer, '*', length);
        return;
    }

    unsigned written = doGetString(buffer);
    assertex(len == written);
    memset(buffer+len, ' ', length-len);
}


void Decimal::getStringX(size32_t & length, char * & buffer) const
{
    unsigned len = getStringLength();
    buffer = (char *)malloc(len);
    unsigned written = doGetString(buffer);
    assertex(len == written);
    length = len;
}


void Decimal::getUDecimal(byte length, byte precision, void * buffer) const
{
    doGetDecimal(length, 2*length, precision, buffer);
}


unsigned int Decimal::getUInt() const
{
    const unsigned hi = msb;
    unsigned int value = 0;
    if (hi >= zeroDigit)
    {
        value = digits[hi];
        unsigned lo = lsb;
        if (lo < zeroDigit)
        {
            for (unsigned idx = hi-1; idx >= zeroDigit; idx--)
                value = value * 10 + digits[idx];
        }
        else
        {
            unsigned idx;
            for (idx = hi-1; idx >= lo; idx--)
                value = value * 10 + digits[idx];
            for (;idx >= zeroDigit;idx--)
                value *= 10;
        }
    }
    return value;
}


unsigned __int64 Decimal::getUInt64() const
{
    //MORE: This isn't the most efficient way of doing it - see num2str in jutil for some hints.
    const unsigned hi = msb;
    unsigned __int64 value = 0;
    if (hi >= zeroDigit)
    {
        value = digits[hi];
        unsigned lo = lsb;
        if (lo < zeroDigit)
        {
            for (unsigned idx = hi-1; idx >= zeroDigit; idx--)
                value = value * 10 + digits[idx];
        }
        else
        {
            unsigned idx;
            for (idx = hi-1; idx >= lo; idx--)
                value = value * 10 + digits[idx];
            for (;idx >= zeroDigit;idx--)
                value *= 10;
        }
    }
    return value;
}


void Decimal::overflow()
{
}


void Decimal::set(const Decimal & value)
{
    memcpy(this, &value, sizeof(*this));
}

void Decimal::setCString(const char * buffer)
{
    const char * cur = buffer;
    while (*cur == ' ')
        cur++;

    negative = false;
    if (*cur == '-')
    {
        negative = true;
        cur++;
    }

    const char * start = cur;
    while (isdigit(*cur))
        cur++;

    unsigned numDigits = (cur-start);
    if (numDigits > maxIntegerDigits)
    {
        overflow();
        numDigits = maxIntegerDigits;
    }

    int idx;
    for (idx = 0; (unsigned)idx < numDigits; idx++)
        digits[zeroDigit+idx] = cur[-(idx+1)] - '0'; // careful - if idx is unsigned, -(idx+1) is not sign-extended and fails if int size is not pointer size
    msb = zeroDigit+(numDigits-1);

    if (*cur == '.')
    {
        cur++;

        const char * start = cur;
        const char * limit = cur + maxPrecision;
        byte * digit = digits + zeroDigit;
        while ((cur < limit) && (isdigit(*cur)))
            *--digit = *cur++ - '0';
        lsb = zeroDigit - (cur-start);
    }
    else
        lsb = zeroDigit;
}


void Decimal::setDecimal(byte length, byte precision, const void * _buffer)
{
    const byte * buffer = (const byte *)_buffer;
    lsb = zeroDigit - precision;
    msb = lsb + length*2 - 2;
    unsigned idx = 0;
    while (msb > maxDigits)
    {
        msb -= 2;
        idx++;
    }
    unsigned cur = msb;
    if (msb == maxDigits)
    {
        msb--;
        cur = msb;
        digits[cur--] = buffer[idx] & 0x0f;
        idx++;
    }
    for (; (int)idx < length-1; idx++)
    {
        byte next = buffer[idx];
        digits[cur--] = next >> 4;
        digits[cur--] = next & 0x0f;
    }
    byte next = buffer[idx];
    digits[cur--] = next >> 4;
    negative = (signMap[next & 0x0f] == -1);
}


void Decimal::setInt64(__int64 value)
{
    if (value >= 0)
        setUInt64((unsigned __int64)value);
    else
    {
        setUInt64((unsigned __int64)-value);
        negative = true;
    }
}


void Decimal::setInt(int value)
{
    if (value >= 0)
        setUInt((unsigned int)value);
    else
    {
        setUInt((unsigned int)-value);
        negative = true;
    }
}

void Decimal::setReal(double value)
{
    setZero();

    int    dec;
    int    sign;
    char digitText[DOUBLE_SIG_DIGITS+2];
    if (!safe_ecvt(sizeof(digitText), digitText, value, DOUBLE_SIG_DIGITS, &dec, &sign))
        return;

    int len = DOUBLE_SIG_DIGITS;
    int hi = zeroDigit - 1 + dec;
    int lo  = hi - (len -1);

    const char * finger = digitText;
    //Number too big - should it create a maximum value? or truncate as it currently does.
    if (hi >= maxDigits)  // Most of this work is dealing with out of range cases
    {
        if (lo >= maxDigits)
            return;
        
        finger += (hi - (maxDigits-1));
        hi = maxDigits-1;
    }
    
    if (lo < 0)
    {
        if (hi < 0)
            return;
        
        lo = 0;
    }
    
    msb = hi;
    lsb  = lo;
    
    for ( int i = hi; i >= lo; i-- )
    {
        byte next = *finger++ - '0';
        if (next < 10)
            digits[i] = next;
        else
        {
            //infinity????
            setZero();
            return;
        }
    }

    if (sign)
        negative = true;
}


void Decimal::setString(size32_t length, const char * buffer)
{
    const char * limit = buffer+length;
    const char * cur = buffer;
    while ((cur < limit) && (*cur == ' '))
        cur++;

    negative = false;
    if ((cur < limit) && (*cur == '-'))
    {
        negative = true;
        cur++;
    }

    const char * start = cur;
    while ((cur < limit) && (isdigit(*cur)))
        cur++;

    unsigned numDigits = (cur-start);
    if (numDigits > maxIntegerDigits)
    {
        overflow();
        numDigits = maxIntegerDigits;
    }

    int idx;
    for (idx = 0; idx < (int)numDigits; idx++)
        digits[zeroDigit+idx] = cur[-(idx+1)] - '0'; // careful - if idx is unsigned, -(idx+1) is not sign-extended and fails if int size is not pointer size
    msb = zeroDigit+(numDigits-1);

    if ((cur < limit) && (*cur == '.'))
    {
        cur++;

        const char * start = cur;
        if (limit-cur > maxPrecision)
            limit = cur + maxPrecision;
        byte * digit = digits + zeroDigit;
        while ((cur < limit) && (isdigit(*cur)))
            *--digit = *cur++ - '0';
        lsb = zeroDigit - (cur-start);
    }
    else
        lsb = zeroDigit;
}


void Decimal::setUInt(unsigned int value)
{
    negative = false;
    lsb = zeroDigit;
    unsigned idx = zeroDigit;
    while (value > 9)
    {
        unsigned int next = value / 10;
        digits[idx++] = value - next*10;
        value = next;
    }
    digits[idx] = value;
    msb = idx;
}


void Decimal::setUInt64(unsigned __int64 value)
{
    negative = false;
    //MORE: This isn't the most efficient way of doing it - see num2str in jutil for some hints.
    lsb = zeroDigit;
    unsigned idx = zeroDigit;
    while (value > 9)
    {
        unsigned __int64 next = value / 10;
        digits[idx++] = (byte)(value - next*10);
        value = next;
    }
    digits[idx] = (byte)value;
    msb = idx;
}


void Decimal::setUDecimal(byte length, byte precision, const void * _buffer)
{
    const byte * buffer = (const byte *)_buffer;
    lsb = zeroDigit - precision;
    msb = lsb + length*2 - 1;
    unsigned cur = msb;
    for (unsigned idx=0; idx < length; idx++)
    {
        byte next = buffer[idx];
        digits[cur--] = next >> 4;
        digits[cur--] = next & 0x0f;
    }
    negative = false;
}

//-- helper functions:

void Decimal::clip(int & newLsb, int & newMsb) const
{
    int lo = lsb;
    int hi = msb;
    while (lo < hi && digits[lo] == 0)
        lo++;
    while (hi >= lo && digits[hi] == 0)
        hi--;
    newLsb = lo;
    newMsb = hi;
}

void Decimal::clip(int & newLsb, int & newMsb, unsigned minLsb, unsigned maxMsb) const
{
    clip(newLsb, newMsb);
    if (newLsb < (int)minLsb)
        newLsb = minLsb;
    if (newMsb > (int)maxMsb)
        newMsb = maxMsb;
    if (newMsb < newLsb)
        newMsb = newLsb-1;
}

unsigned Decimal::doGetString(char * buffer) const
{
    char * cur = buffer;
    int lo, hi;
    clip(lo, hi);

    if (lo > hi) // || (lo == hi) && (digits[hi] == 0))
    {
        *cur = '0';
        return 1;
    }

    if (negative)
        *cur++ = '-';

    int idx;
    if (hi < zeroDigit)
    {
        *cur++ = '0';
        *cur++ = '.';
        for (idx = zeroDigit-1; idx > hi; idx--)
            *cur++ = '0';

        for (;idx >= lo; idx--)
            *cur++ = '0' + digits[idx];
    }
    else
    {
        if (lo < zeroDigit)
        {
            for (idx = hi; idx >= zeroDigit; idx--)
                *cur++ = '0' + digits[idx];

            *cur++ = '.';
            for (; idx >= lo; idx--)
                *cur++ = '0' + digits[idx];
        }
        else
        {
            for (idx = hi; idx >= lo; idx--)
                *cur++ = '0' + digits[idx];

            for (; idx >= zeroDigit; idx--)
                *cur++ = '0';
        }
    }
    return cur-buffer;
}


void Decimal::doGetDecimal(byte length, byte maxDigits, byte precision, void * buffer) const
{
    int tgtHighPos = zeroDigit+maxDigits-precision-1;
    int tgtLowPos = tgtHighPos - length*2 + 1;
    int lowPos, highPos;
    clip(lowPos, highPos, zeroDigit-precision, zeroDigit+maxDigits-precision-1);
    if ((lowPos > highPos) || (highPos > tgtHighPos))
    {
        //zero or overflow...
        memset(buffer, 0, length);
        return;
    }

    int copyHighPos = highPos;
    if (tgtLowPos > copyHighPos)
        copyHighPos = tgtLowPos;
    else if (tgtHighPos < copyHighPos)
        copyHighPos = tgtHighPos;       // overflow....
    int copyLowPos = lowPos;
    if (tgtLowPos > copyLowPos)
        copyLowPos = tgtLowPos;

    NOMEMCPY byte * tgt = (byte *)buffer;
    //First pad with zeros.
    if (tgtHighPos > copyHighPos)
    {
        unsigned zeros = tgtHighPos-copyHighPos;
        while (zeros >= 2)
        {
            *tgt++ = 0;
            zeros -= 2;
        }
        if (zeros != 0)
            *tgt++ = digits[copyHighPos--];
    }

    //Now will with characters
    while (copyLowPos < copyHighPos)
    {
        byte next = digits[copyHighPos];
        byte next2 = digits[copyHighPos-1];
        *tgt++ = (byte)(next << 4) | next2;
        copyHighPos -= 2;
    }
    if (copyLowPos == copyHighPos)
    {
        *tgt++ = digits[copyLowPos] << 4;
        copyLowPos -= 2;
    }
    while (copyLowPos > tgtLowPos)
    {
        *tgt++ = 0;
        copyLowPos -= 2;
    }
}

void Decimal::doPower(unsigned value)
{
    if (value == 1)
        return;

    if (value & 1)
    {
        Decimal saved(*this);
        doPower(value >> 1);
        multiply(*this);
        multiply(saved);
    }
    else
    {
        doPower(value >> 1);
        multiply(*this);
    }
}

void Decimal::setZero()
{
    negative = false;
    lsb = msb = zeroDigit;
    digits[zeroDigit] = 0;
}


//---------------------------------------------------------------------------


bool dec2Bool(size32_t bytes, const void * _data)
{
    const byte * data = (const byte *)_data;
    //ignore the sign
    if (data[--bytes] & 0xf0)
        return true;
    while (bytes--)
        if (*data++)
            return true;
    return false;
}


bool udec2Bool(size32_t bytes, const void * _data)
{
    const byte * data = (const byte *)_data;
    while (bytes--)
        if (*data++)
            return true;
    return false;
}


int decCompareDecimal(size32_t bytes, const void * _left, const void * _right)
{
    const byte * left = (const byte *)_left;
    const byte * right = (const byte *)_right;

    bytes--;
    byte signLeft = left[bytes] & 0x0f;
    byte signRight = right[bytes] & 0x0f;
    if (signMap[signLeft] != signMap[signRight])
    {
        int res = signMap[signLeft] - signMap[signRight];
        // could be +0 and -0......
        if ((left[bytes] & 0xf0) || (right[bytes] & 0xf0))
            return res;
        while (bytes--)
            if (left[bytes] || right[bytes])
                return res;
        return 0;
    }

    bool numbersAreNegative = (signMap[signLeft] == -1);
    while (bytes--)
    {
        byte l = *left++;
        byte r = *right++;
        int res = l - r;
        if (res)
            return numbersAreNegative ? -res : res;
    }

    int res = (*left & 0xf0) - (*right & 0xf0);
    if (res)
        return numbersAreNegative ? -res : res;
    return 0;
}


int decCompareUDecimal(size32_t bytes, const void * _left, const void * _right)
{
    const byte * left = (const byte *)_left;
    const byte * right = (const byte *)_right;

    while (bytes--)
    {
        byte l = *left++;
        byte r = *right++;
        int res = l - r;
        if (res)
            return res;
    }
    return 0;
}


bool decValid(bool isSigned, unsigned digits, const void * data)
{
    if (data && digits)
    {
        if ((isSigned && digits % 2 == 0) || (!isSigned && digits % 2 != 0))
        {
            if (*((byte*)data)&0xf0)
                return false;
            digits++;
        }
        unsigned bytes = isSigned ? digits/2+1 : (digits+1)/2;
        byte *dp = (byte * )data;
        if (isSigned)
        {
            byte sign = dp[--bytes] & 0x0f;
            // allow 0x0F and 0x0C for positive and 0x0D for negative signs
            if (!(sign == 0x0f || sign == 0x0d || sign == 0x0c))
                return false;
            if ((byte)(dp[bytes] & 0xf0u) > 0x90u)
                return false;
        }
        // This code assumes 32bit registers.
        unsigned dwords = bytes / 4;
        bytes %= 4;
        while(bytes--)
        {
            byte b = dp[dwords*4 + bytes];
            if (((b&0xF0u) > 0x90u) || ((b&0x0Fu) > 0x09u))
                return false;
        }
        // see http://www.cs.uiowa.edu/~jones/bcd/bcd.html for an explanation of this code
        unsigned __int32  *wp = (unsigned __int32 *)data;
        while (dwords--)
        {
            unsigned __int32 l = wp[dwords];
            if ((unsigned __int32 )(l&0xF0000000) > 0x90000000u)
                return false;
            __int32 l1 = l  + 0x66666666;
            __int32 l2 = l1 ^ l;
            __int32 l3 = l2 & 0x11111110;
            if (l3)
                return false;
        }
    }
    return true;
}


