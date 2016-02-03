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
#include "jlib.hpp"
#include "rtlbcd.hpp"
#include "nbcd.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

#ifdef __APPLE__
//Apple does not currently support thread_local (very strange), so need to use __thread.
static __thread Decimal stack[32];
static __thread unsigned curStack = 0;
#else
//gcc needs to use thread_local because it doesn't support __thread on arrays of constexpr objects
static thread_local Decimal stack[32];
static thread_local unsigned curStack = 0;
#endif

//---------------------------------------------------------------------------------------------------------------------

//These functions are retained to that old work units will load, and then report a version mismatch, rather than a
//confusing unresolved symbol error.
void DecLock()
{
    throwUnexpected();
}

void DecUnlock()
{
    throwUnexpected();
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



void DecAbs()
{
    stack[curStack-1].abs();
}

void DecAdd()
{
    curStack--;
    stack[curStack-1].add(stack[curStack]);
}

int DecCompareNull()
{
    curStack--;
    return stack[curStack].compareNull();
}

int DecDistinct()
{
    curStack -= 2;
    return stack[curStack].compare(stack[curStack+1]);
}

int DecDistinctR()
{
    curStack -= 2;
    return stack[curStack+1].compare(stack[curStack]);
}

void DecDivide(byte dbz)
{
    curStack--;
    if (stack[curStack].isZero())
        setDivideByZero(stack[curStack-1], (DBZaction)dbz);
    else
        stack[curStack-1].divide(stack[curStack]);
}

void DecDivideR(byte dbz)
{
    DecSwap();
    DecDivide(dbz);
}

void DecDup()
{
    stack[curStack].set(stack[curStack-1]);
    curStack++;
}

void DecSetPrecision(unsigned char declen, unsigned char prec)
{
    stack[curStack-1].round(prec).setPrecision(declen, prec);
}

void DecSub()
{
    curStack--;
    stack[curStack-1].subtract(stack[curStack]);
}

void DecSubR()
{
    DecSwap();
    DecSub();
}

void DecInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getPrecision(digits, prec);
}

void DecClipInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getClipPrecision(digits, prec);
}

void DecLongPower(long pow)
{
    stack[curStack-1].power((int)pow);
}

void DecUlongPower(unsigned long pow)
{
    stack[curStack-1].power((unsigned)pow);
}

void DecModulus(byte dbz)
{
    curStack--;
    if (stack[curStack].isZero())
        setDivideByZero(stack[curStack-1], (DBZaction)dbz);
    else
        stack[curStack-1].modulus(stack[curStack]);
}

void DecMul()
{
    curStack--;
    stack[curStack-1].multiply(stack[curStack]);
}

void DecNegate()
{
    stack[curStack-1].negate();
}

void DecPopCString (unsigned length, char * buffer)
{
    stack[--curStack].getCString(length, buffer);
}

char * DecPopCStringX()
{
    return stack[--curStack].getCString();
}

__int64 DecPopInt64()
{
    return stack[--curStack].getInt64();
}

void DecPopDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getDecimal(declen, prec, buffer);
}

void DecPopUDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getUDecimal(declen, prec, buffer);
}

int DecPopLong()
{
    return stack[--curStack].getInt();
}

unsigned long DecPopUlong()
{
    return stack[--curStack].getUInt();
}

double DecPopReal()
{
    return stack[--curStack].getReal();
}

unsigned DecPopString( unsigned length, char * buffer)
{
    stack[--curStack].getString(length, buffer);
    return length;  // significant length??
}

void DecPopStringX( unsigned & length, char * & buffer)
{
    stack[--curStack].getStringX(length, buffer);
}

void DecPushCString(const char *s)
{
    stack[curStack++].setCString(s);
}

void DecPushInt64(__int64 value)
{
    stack[curStack++].setInt64(value);
}

void DecPushUInt64(unsigned __int64 value)
{
    stack[curStack++].setUInt64(value);
}

void DecPushLong( long value)
{
    stack[curStack++].setInt(value);
}

void DecPushDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setDecimal(declen, prec, buffer);
}

void DecPushUDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setUDecimal(declen, prec, buffer);
}

void DecPushReal( double value )
{
    stack[curStack++].setReal(value);
}

void DecPushString(unsigned length, const char * text)
{
    stack[curStack++].setString(length, text);
}

void DecPushUlong( unsigned long value)
{
    stack[curStack++].setUInt(value);
}

void DecRound()
{
    stack[curStack-1].round(0);
}

void DecRoundUp()
{
    stack[curStack-1].roundup(0);
}

void DecRoundTo(unsigned places)
{
    stack[curStack-1].round(places);
}

void DecSwap()
{
    char temp[sizeof(Decimal)];
    memcpy(&temp, &stack[curStack-1], sizeof(Decimal));
    memcpy(&stack[curStack-1], &stack[curStack-2], sizeof(Decimal));
    memcpy(&stack[curStack-2], &temp, sizeof(Decimal));
}


void DecTruncate()
{
    stack[curStack-1].truncate(0);
}

void DecTruncateAt(unsigned places)
{
    stack[curStack-1].truncate(places);
}

bool DecValid(bool isSigned, unsigned digits, const void * data)
{
    return decValid(isSigned, digits, data);
}

bool DecValidTos()
{
    return stack[--curStack].isValid();
}

bool Dec2Bool(size32_t bytes, const void * data)
{
    return dec2Bool(bytes, data);
}

bool UDec2Bool(size32_t bytes, const void * data)
{
    return udec2Bool(bytes, data);
}

int DecCompareDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareDecimal(bytes, _left, _right);
}

int DecCompareUDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareUDecimal(bytes, _left, _right);
}

// internal

