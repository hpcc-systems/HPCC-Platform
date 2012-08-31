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
#include "nbcd.hpp"
#include "jlib.hpp"
#include "bcd.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

CriticalSection bcdCriticalSection;

nbcd_decl void _fastcall  DecLock()
{
    bcdCriticalSection.enter();
}

nbcd_decl void _fastcall  DecUnlock()
{
    bcdCriticalSection.leave();
}

static Decimal stack[32];
unsigned  curStack;


nbcd_decl void _fastcall  DecAbs()
{
    stack[curStack-1].abs();
}

nbcd_decl void _fastcall  DecAdd()
{
    curStack--;
    stack[curStack-1].add(stack[curStack]);
}

nbcd_decl int _fastcall  DecCompareNull()
{
    curStack--;
    return stack[curStack].compareNull();
}

nbcd_decl int _fastcall  DecDistinct()
{
    curStack -= 2;
    return stack[curStack].compare(stack[curStack+1]);
}

nbcd_decl int _fastcall  DecDistinctR()
{
    curStack -= 2;
    return stack[curStack+1].compare(stack[curStack]);
}

nbcd_decl void _fastcall  DecDivide()
{
    curStack--;
    stack[curStack-1].divide(stack[curStack]);
}

nbcd_decl void _fastcall  DecDivideR()
{
    DecSwap();
    DecDivide();
}

nbcd_decl void _fastcall  DecDup()
{
    stack[curStack].set(stack[curStack-1]);
    curStack++;
}

nbcd_decl void _fastcall  DecSetPrecision(unsigned char declen, unsigned char prec)
{
    stack[curStack-1].round(prec).setPrecision(declen, prec);
}

nbcd_decl void _fastcall  DecSub()
{
    curStack--;
    stack[curStack-1].subtract(stack[curStack]);
}

nbcd_decl void _fastcall  DecSubR()
{
    DecSwap();
    DecSub();
}

nbcd_decl void _fastcall  DecInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getPrecision(digits, prec);
}

nbcd_decl void _fastcall  DecClipInfo (unsigned & digits, unsigned & prec)
{
    stack[curStack-1].getClipPrecision(digits, prec);
}

nbcd_decl void _fastcall  DecLongPower(long pow)
{
    stack[curStack-1].power((int)pow);
}

nbcd_decl void _fastcall  DecUlongPower(unsigned long pow)
{
    stack[curStack-1].power((unsigned)pow);
}

nbcd_decl void  _fastcall  DecModulus()
{
    curStack--;
    stack[curStack-1].modulus(stack[curStack]);
}

nbcd_decl void _fastcall  DecMul()
{
    curStack--;
    stack[curStack-1].multiply(stack[curStack]);
}

nbcd_decl void _fastcall  DecNegate()
{
    stack[curStack-1].negate();
}

nbcd_decl void _fastcall  DecPopCString (unsigned length, char * buffer)
{
    stack[--curStack].getCString(length, buffer);
}

nbcd_decl char * _fastcall  DecPopCStringX()
{
    return stack[--curStack].getCString();
}

nbcd_decl __int64 _fastcall  DecPopInt64()
{
    return stack[--curStack].getInt64();
}

nbcd_decl void _fastcall  DecPopDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getDecimal(declen, prec, buffer);
}

nbcd_decl void _fastcall  DecPopUDecimal(void * buffer,unsigned char declen,unsigned char prec)
{
    stack[--curStack].round(prec).getUDecimal(declen, prec, buffer);
}

nbcd_decl int    _fastcall  DecPopLong()
{
    return stack[--curStack].getInt();
}

nbcd_decl unsigned long _fastcall  DecPopUlong()
{
    return stack[--curStack].getUInt();
}

nbcd_decl double _fastcall  DecPopReal()
{
    return stack[--curStack].getReal();
}

nbcd_decl unsigned _fastcall  DecPopString( unsigned length, char * buffer)
{
    stack[--curStack].getString(length, buffer);
    return length;  // significant length??
}

nbcd_decl void _fastcall  DecPopStringX( unsigned & length, char * & buffer)
{
    stack[--curStack].getStringX(length, buffer);
}

nbcd_decl void _fastcall  DecPushCString(const char *s)
{
    stack[curStack++].setCString(s);
}

nbcd_decl void _fastcall  DecPushInt64(__int64 value)
{
    stack[curStack++].setInt64(value);
}

nbcd_decl void _fastcall  DecPushUInt64(unsigned __int64 value)
{
    stack[curStack++].setUInt64(value);
}

nbcd_decl void _fastcall  DecPushLong( long value)
{
    stack[curStack++].setInt(value);
}

nbcd_decl void _fastcall  DecPushDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setDecimal(declen, prec, buffer);
}

nbcd_decl void _fastcall  DecPushUDecimal(const void * buffer,unsigned char declen,unsigned char prec)
{
    stack[curStack++].setUDecimal(declen, prec, buffer);
}

nbcd_decl void _fastcall  DecPushReal( double value )
{
    stack[curStack++].setReal(value);
}

nbcd_decl void _fastcall  DecPushString(unsigned length, const char * text)
{
    stack[curStack++].setString(length, text);
}

nbcd_decl void _fastcall  DecPushUlong( unsigned long value)
{
    stack[curStack++].setUInt(value);
}

nbcd_decl void _fastcall  DecRound()
{
    stack[curStack-1].round(0);
}

nbcd_decl void _fastcall  DecRoundUp()
{
    stack[curStack-1].roundup(0);
}

nbcd_decl void _fastcall  DecRoundTo(unsigned places)
{
    stack[curStack-1].round(places);
}

nbcd_decl void _fastcall  DecSwap()
{
    char temp[sizeof(Decimal)];
    memcpy(&temp, &stack[curStack-1], sizeof(Decimal));
    memcpy(&stack[curStack-1], &stack[curStack-2], sizeof(Decimal));
    memcpy(&stack[curStack-2], &temp, sizeof(Decimal));
}


nbcd_decl void _fastcall  DecTruncate()
{
    stack[curStack-1].truncate(0);
}

nbcd_decl void _fastcall  DecTruncateAt(unsigned places)
{
    stack[curStack-1].truncate(places);
}

nbcd_decl bool _fastcall  DecValid(bool isSigned, unsigned digits, const void * data)
{
    return decValid(isSigned, digits, data);
}

nbcd_decl bool _fastcall  Dec2Bool(size32_t bytes, const void * data)
{
    return dec2Bool(bytes, data);
}

nbcd_decl bool _fastcall  UDec2Bool(size32_t bytes, const void * data)
{
    return udec2Bool(bytes, data);
}

nbcd_decl int _fastcall  DecCompareDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareDecimal(bytes, _left, _right);
}

nbcd_decl int    _fastcall  DecCompareUDecimal(size32_t bytes, const void * _left, const void * _right)
{
    return decCompareUDecimal(bytes, _left, _right);
}

// internal

nbcd_decl void * _fastcall  DecSaveStack( void )            { UNIMPLEMENTED; }
nbcd_decl void _fastcall  DecRestoreStack( void * )     { UNIMPLEMENTED; }
