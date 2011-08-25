/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

static TempDecimal stack[32];
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
    char temp[sizeof(TempDecimal)];
    memcpy(&temp, &stack[curStack-1], sizeof(TempDecimal));
    memcpy(&stack[curStack-1], &stack[curStack-2], sizeof(TempDecimal));
    memcpy(&stack[curStack-2], &temp, sizeof(TempDecimal));
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
