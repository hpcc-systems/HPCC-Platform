/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jlib.hpp"
#include "rtlbcd.hpp"
#include "nbcd.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

static CriticalSection bcdCriticalSection;
static Decimal stack[32];
static unsigned curStack;

//---------------------------------------------------------------------------------------------------------------------

void DecLock()
{
    bcdCriticalSection.enter();
}

void DecUnlock()
{
    bcdCriticalSection.leave();
}

unsigned DecMarkStack()
{
    return curStack;
}

void DecReleaseStack(unsigned mark)
{
    curStack = mark;
}

//---------------------------------------------------------------------------------------------------------------------

static void setDivideByZero(Decimal & tos, DBZaction dbz)
{
    switch (dbz)
    {
    case DBZfail:
        rtlFailDivideByZero();
        break;
    case DBZnan:
    case DBZzero:
        tos.setZero();
        break;
    default:
        throwUnexpected();
    }
}



nbcd_decl void DecAbs()
{
    stack[curStack-1].abs();
}

nbcd_decl void DecAdd()
{
    curStack--;
    stack[curStack-1].add(stack[curStack]);
}

nbcd_decl int DecCompareNull()
{
    curStack--;
    return stack[curStack].compareNull();
}

nbcd_decl int DecDistinct()
{
    curStack -= 2;
    return stack[curStack].compare(stack[curStack+1]);
}

nbcd_decl int DecDistinctR()
{
    curStack -= 2;
    return stack[curStack+1].compare(stack[curStack]);
}

nbcd_decl void DecDivide(byte dbz)
{
    curStack--;
    if (stack[curStack].isZero())
        setDivideByZero(stack[curStack-1], (DBZaction)dbz);
    else
        stack[curStack-1].divide(stack[curStack]);
}

nbcd_decl void DecDivideR(byte dbz)
{
    DecSwap();
    DecDivide(dbz);
}

nbcd_decl void DecDup()
{
    stack[curStack].set(stack[curStack-1]);
    curStack++;
}

nbcd_decl void DecSetPrecision(unsigned char declen, unsigned char prec)
{
    stack[curStack-1].round(prec).setPrecision(declen, prec);
}

nbcd_decl void DecSub()
{
    curStack--;
    stack[curStack-1].subtract(stack[curStack]);
}

nbcd_decl void DecSubR()
{
    DecSwap();
    DecSub();
}

nbcd_decl void DecInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getPrecision(digits, prec);
}

nbcd_decl void DecClipInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getClipPrecision(digits, prec);
}

nbcd_decl void DecLongPower(long pow)
{
    stack[curStack-1].power((int)pow);
}

nbcd_decl void DecUlongPower(unsigned long pow)
{
    stack[curStack-1].power((unsigned)pow);
}

nbcd_decl void  DecModulus(byte dbz)
{
    curStack--;
    if (stack[curStack].isZero())
        setDivideByZero(stack[curStack-1], (DBZaction)dbz);
    else
        stack[curStack-1].modulus(stack[curStack]);
}

nbcd_decl void DecMul()
{
    curStack--;
    stack[curStack-1].multiply(stack[curStack]);
}

nbcd_decl void DecNegate()
{
    stack[curStack-1].negate();
}

nbcd_decl void DecPopCString (unsigned length, char * buffer)
{
    stack[--curStack].getCString(length, buffer);
}

nbcd_decl char * DecPopCStringX()
{
    return stack[--curStack].getCString();
}

nbcd_decl __int64 DecPopInt64()
{
    return stack[--curStack].getInt64();
}

nbcd_decl void DecPopDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getDecimal(declen, prec, buffer);
}

nbcd_decl void DecPopUDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getUDecimal(declen, prec, buffer);
}

nbcd_decl int    DecPopLong()
{
    return stack[--curStack].getInt();
}

nbcd_decl unsigned long DecPopUlong()
{
    return stack[--curStack].getUInt();
}

nbcd_decl double DecPopReal()
{
    return stack[--curStack].getReal();
}

nbcd_decl unsigned DecPopString( unsigned length, char * buffer)
{
    stack[--curStack].getString(length, buffer);
    return length;  // significant length??
}

nbcd_decl void DecPopStringX( unsigned & length, char * & buffer)
{
    stack[--curStack].getStringX(length, buffer);
}

nbcd_decl void DecPushCString(const char *s)
{
    stack[curStack++].setCString(s);
}

nbcd_decl void DecPushInt64(__int64 value)
{
    stack[curStack++].setInt64(value);
}

nbcd_decl void DecPushUInt64(unsigned __int64 value)
{
    stack[curStack++].setUInt64(value);
}

nbcd_decl void DecPushLong( long value)
{
    stack[curStack++].setInt(value);
}

nbcd_decl void DecPushDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setDecimal(declen, prec, buffer);
}

nbcd_decl void DecPushUDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setUDecimal(declen, prec, buffer);
}

nbcd_decl void DecPushReal( double value )
{
    stack[curStack++].setReal(value);
}

nbcd_decl void DecPushString(unsigned length, const char * text)
{
    stack[curStack++].setString(length, text);
}

nbcd_decl void DecPushUlong( unsigned long value)
{
    stack[curStack++].setUInt(value);
}

nbcd_decl void DecRound()
{
    stack[curStack-1].round(0);
}

nbcd_decl void DecRoundUp()
{
    stack[curStack-1].roundup(0);
}

nbcd_decl void DecRoundTo(unsigned places)
{
    stack[curStack-1].round(places);
}

nbcd_decl void DecSwap()
{
    char temp[sizeof(Decimal)];
    memcpy(&temp, &stack[curStack-1], sizeof(Decimal));
    memcpy(&stack[curStack-1], &stack[curStack-2], sizeof(Decimal));
    memcpy(&stack[curStack-2], &temp, sizeof(Decimal));
}


nbcd_decl void DecTruncate()
{
    stack[curStack-1].truncate(0);
}

nbcd_decl void DecTruncateAt(unsigned places)
{
    stack[curStack-1].truncate(places);
}

nbcd_decl bool DecValid(bool isSigned, unsigned digits, const void * data)
{
    return decValid(isSigned, digits, data);
}

nbcd_decl bool DecValidTos()
{
    return stack[--curStack].isValid();
}

nbcd_decl bool Dec2Bool(size32_t bytes, const void * data)
{
    return dec2Bool(bytes, data);
}

nbcd_decl bool UDec2Bool(size32_t bytes, const void * data)
{
    return udec2Bool(bytes, data);
}

nbcd_decl int DecCompareDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareDecimal(bytes, _left, _right);
}

nbcd_decl int    DecCompareUDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareUDecimal(bytes, _left, _right);
}

// internal

