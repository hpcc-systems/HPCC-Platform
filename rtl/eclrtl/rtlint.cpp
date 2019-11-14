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
#include <math.h>
#include <stdio.h>
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "jptree.hpp"
#include "eclrtl.hpp"
#include "rtlbcd.hpp"

//#define _FAST_AND_LOOSE_

#ifdef _FAST_AND_LOOSE_

#if __BYTE_ORDER == __LITTLE_ENDIAN
//These could read beyond a page boundary....
int rtlReadInt3(const void * data)                      { return ((*(int *)data << 8) >> 8); }
__int64 rtlReadInt5(const void * data)                  { return ((*(__int64 *)data << 24) >> 24); }
__int64 rtlReadInt6(const void * data)                  { return ((*(__int64 *)data << 16) >> 16); }
__int64 rtlReadInt7(const void * data)                  { return ((*(__int64 *)data << 8) >> 8); }
unsigned rtlReadUInt3(const void * data)                { return (*(unsigned *)data & 0xFFFFFF); }
unsigned __int64 rtlReadUInt5(const void * data)        { return (*(unsigned __int64 *)data & 0xFFFFFFFFFF); }
unsigned __int64 rtlReadUInt6(const void * data)        { return (*(unsigned __int64 *)data & 0xFFFFFFFFFFFF); }
unsigned __int64 rtlReadUInt7(const void * data)        { return (*(unsigned __int64 *)data & 0xFFFFFFFFFFFFFF); }
#else
//Probably have problems of misaligned access as well.....  
int rtlReadInt3(const void * data)                      { return (*(int *)data >> 8); }
__int64 rtlReadInt5(const void * data)                  { return (*(__int64 *)data >> 24); }
__int64 rtlReadInt6(const void * data)                  { return (*(__int64 *)data >> 16); }
__int64 rtlReadInt7(const void * data)                  { return (*(__int64 *)data >> 8); }
unsigned rtlReadUInt3(const void * data)                { return (*(unsigned *)data >> 8); }
unsigned __int64 rtlReadUInt5(const void * data)        { return (*(unsigned __int64 *)data >> 24); }
unsigned __int64 rtlReadUInt6(const void * data)        { return (*(unsigned __int64 *)data >> 16); }
unsigned __int64 rtlReadUInt7(const void * data)        { return (*(unsigned __int64 *)data >> 8); }
#endif

#else

#if __BYTE_ORDER == __LITTLE_ENDIAN

int rtlReadInt3(const void * _data)                     
{ 
    const unsigned short * usdata = (const unsigned short *)_data;
    const signed char * scdata = (const signed char *)_data;
    return ((unsigned)(int)scdata[2] << 16) | ((int)usdata[0]);
}

unsigned rtlReadUInt3(const void * _data)
{ 
    const unsigned short * usdata = (const unsigned short *)_data;
    const unsigned char * ucdata = (const unsigned char *)_data;
    return ((unsigned)ucdata[2] << 16) | ((unsigned)usdata[0]);
}

//no sign extension issues, so same functions can be used for signed and unsigned.
void rtlWriteInt3(void * _data, unsigned value)                     
{ 
    unsigned short * usdata = (unsigned short *)_data;
    unsigned char * scdata = (unsigned char *)_data;
    scdata[2] = value >> 16;
    usdata[0] = value;
}

void rtlWriteInt5(void * _data, unsigned __int64 value)     
{ 
    unsigned * udata = (unsigned *)_data;
    unsigned char * ucdata = (unsigned char *)_data;
    ucdata[4] = (unsigned char) (value>>32);
    udata[0] = (unsigned)value;
}

void rtlWriteInt6(void * _data, unsigned __int64 value)     
{ 
    unsigned * udata = (unsigned *)_data;
    unsigned short * usdata = (unsigned short *)_data;
    usdata[2] = (unsigned short) (value>>32);
    udata[0] = (unsigned)value;
}

void rtlWriteInt7(void * _data, unsigned __int64 value)     
{ 
    unsigned * udata = (unsigned *)_data;
    unsigned short * usdata = (unsigned short *)_data;
    unsigned char * ucdata = (unsigned char *)_data;
    ucdata[6] = (unsigned char) (value>>48);
    usdata[2] = (unsigned short) (value>>32);
    udata[0] = (unsigned)value;
}

#ifdef _WIN32
//MSVC seems to optimize these much better than you would expect.... in release
__int64 rtlReadInt5(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    const signed char * scdata = (const signed char *)_data;
    LARGE_INTEGER x;
    x.HighPart = (int)scdata[4];
    x.LowPart = udata[0];
    return x.QuadPart;
}

__int64 rtlReadInt6(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const signed short * ssdata = (const signed short *)_data;
    LARGE_INTEGER x;
    x.HighPart = (int)ssdata[2];
    x.LowPart = udata[0];
    return x.QuadPart;
}

__int64 rtlReadInt7(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    const signed char * scdata = (const signed char *)_data;
    LARGE_INTEGER x;
    x.HighPart = ((int)scdata[6] << 16) | usdata[2];
    x.LowPart = udata[0];
    return x.QuadPart;
}


unsigned __int64 rtlReadUInt5(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned char * ucdata = (const unsigned char *)_data;
    LARGE_INTEGER x;
    x.HighPart = (int)ucdata[4];
    x.LowPart = udata[0];
    return x.QuadPart;
}

unsigned __int64 rtlReadUInt6(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    LARGE_INTEGER x;
    x.HighPart = (int)usdata[2];
    x.LowPart = udata[0];
    return x.QuadPart;
}

unsigned __int64 rtlReadUInt7(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    const unsigned char * ucdata = (const unsigned char *)_data;
    LARGE_INTEGER x;
    x.HighPart = ((unsigned)ucdata[6] << 16) | usdata[2];
    x.LowPart = udata[0];
    return x.QuadPart;
}

#else

__int64 rtlReadInt5(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const signed char * scdata = (const signed char *)_data;
    return ((__uint64)(__int64)scdata[4] << 32) | ((__int64)udata[0]);
}

__int64 rtlReadInt6(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const signed short * ssdata = (const signed short *)_data;
    return ((__uint64)(__int64)ssdata[2] << 32) | ((__int64)udata[0]);
}

__int64 rtlReadInt7(const void * _data)     
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    const signed char * scdata = (const signed char *)_data;
    return ((__uint64)(__int64)scdata[6] << 48) | ((__int64)usdata[2] << 32) | ((__int64)udata[0]);
}

unsigned __int64 rtlReadUInt5(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned char * ucdata = (const unsigned char *)_data;
    return ((unsigned __int64)ucdata[4] << 32) | ((unsigned __int64)udata[0]);
}

unsigned __int64 rtlReadUInt6(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    return ((unsigned __int64)usdata[2] << 32) | ((unsigned __int64)udata[0]);
}

unsigned __int64 rtlReadUInt7(const void * _data)       
{ 
    const unsigned * udata = (const unsigned *)_data;
    const unsigned short * usdata = (const unsigned short *)_data;
    const unsigned char * ucdata = (const unsigned char *)_data;
    return ((unsigned __int64)ucdata[6] << 48) | ((unsigned __int64)usdata[2] << 32) | ((unsigned __int64)udata[0]);
}

#endif


int rtlReadSwapInt2(const void * _data)                     
{ 
    short temp;
    _cpyrev2(&temp, _data);
    return temp;
}

int rtlReadSwapInt3(const void * _data)                     
{ 
    const unsigned char * scdata = (const unsigned char *)_data;
    int temp = scdata[0] << 16;
    _cpyrev2(&temp, scdata+1);
    return temp;
}

int rtlReadSwapInt4(const void * _data)                     
{ 
    int temp;
    _cpyrev4(&temp, _data);
    return temp;
}

__int64 rtlReadSwapInt5(const void * _data)                     
{ 
    const unsigned char * scdata = (const unsigned char *)_data;
    __int64 temp = ((__uint64)scdata[0]) << 32;
    _cpyrev4(&temp, scdata+1);
    return temp;
}

__int64 rtlReadSwapInt6(const void * _data)                     
{ 
    const signed char * scdata = (const signed char *)_data;
    __int64 temp = ((__uint64)scdata[0]) << 40;
    _cpyrev5(&temp, scdata+1);
    return temp;
}

__int64 rtlReadSwapInt7(const void * _data)                     
{ 
    const signed char * scdata = (const signed char *)_data;
    __int64 temp = ((__uint64)scdata[0]) << 48;
    _cpyrev6(&temp, scdata+1);
    return temp;
}

__int64 rtlReadSwapInt8(const void * _data)                     
{ 
    __int64 temp;
    _cpyrev8(&temp, _data);
    return temp;
}

unsigned rtlReadSwapUInt2(const void * _data)                       
{ 
    unsigned short temp;
    _cpyrev2(&temp, _data);
    return temp;
}

unsigned rtlReadSwapUInt3(const void * _data)
{ 
    unsigned temp = 0;
    _cpyrev3(&temp, _data);
    return temp;
}

unsigned rtlReadSwapUInt4(const void * _data)                       
{ 
    unsigned temp;
    _cpyrev4(&temp, _data);
    return temp;
}

unsigned __int64 rtlReadSwapUInt5(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev5(&temp, _data);
    return temp;
}

unsigned __int64 rtlReadSwapUInt6(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev6(&temp, _data);
    return temp;
}

unsigned __int64 rtlReadSwapUInt7(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev7(&temp, _data);
    return temp;
}

unsigned __int64 rtlReadSwapUInt8(const void * _data)                       
{ 
    unsigned __int64 temp;
    _cpyrev8(&temp, _data);
    return temp;
}

//no sign extension issues, so same functions can be used for signed and unsigned.
void rtlWriteSwapInt2(void * _data, unsigned value)
{
    _cpyrev2(_data, &value);
}

void rtlWriteSwapInt3(void * _data, unsigned value)                     
{ 
    _cpyrev3(_data, &value);
}

void rtlWriteSwapInt4(void * _data, unsigned value)
{
    _cpyrev4(_data, &value);
}

void rtlWriteSwapInt5(void * _data, unsigned __int64 value)     
{ 
    _cpyrev5(_data, &value);
}

void rtlWriteSwapInt6(void * _data, unsigned __int64 value)     
{ 
    _cpyrev6(_data, &value);
}

void rtlWriteSwapInt7(void * _data, unsigned __int64 value)     
{ 
    _cpyrev7(_data, &value);
}

void rtlWriteSwapInt8(void * _data, unsigned __int64 value)
{
    _cpyrev8(_data, &value);
}


//The following functions are identical to the rtlReadSwapIntX functions for little endian.
//big endian functions would be different.
short rtlRevInt2(const void * _data)
{ 
    short temp;
    _cpyrev2(&temp, _data);
    return temp;
}

int rtlRevInt3(const void * _data)                      
{ 
    const signed char * scdata = (const signed char *)_data;
    int temp = scdata[0] << 16;
    _cpyrev2(&temp, scdata+1);
    return temp;
}

int rtlRevInt4(const void * _data)
{ 
    int temp;
    _cpyrev4(&temp, _data);
    return temp;
}

__int64 rtlRevInt5(const void * _data)                      
{ 
    const signed char * scdata = (const signed char *)_data;
    __int64 temp = ((__int64)scdata[0]) << 32;
    _cpyrev4(&temp, scdata+1);
    return temp;
}

__int64 rtlRevInt6(const void * _data)                      
{ 
    const signed char * scdata = (const signed char *)_data;
    __int64 temp = ((__int64)scdata[0]) << 40;
    _cpyrev5(&temp, scdata+1);
    return temp;
}

__int64 rtlRevInt7(const void * _data)                      
{ 
    const signed char * scdata = (const signed char *)_data;
    __int64 temp = ((__int64)scdata[0]) << 48;
    _cpyrev6(&temp, scdata+1);
    return temp;
}

__int64 rtlRevInt8(const void * _data)
{ 
    __int64 temp;
    _cpyrev8(&temp, _data);
    return temp;
}

unsigned short rtlRevUInt2(const void * _data)
{ 
    unsigned short temp;
    _cpyrev2(&temp, _data);
    return temp;
}

unsigned rtlRevUInt3(const void * _data)
{ 
    unsigned temp = 0;
    _cpyrev3(&temp, _data);
    return temp;
}

unsigned rtlRevUInt4(const void * _data)
{ 
    unsigned temp;
    _cpyrev4(&temp, _data);
    return temp;
}

unsigned __int64 rtlRevUInt5(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev5(&temp, _data);
    return temp;
}

unsigned __int64 rtlRevUInt6(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev6(&temp, _data);
    return temp;
}

unsigned __int64 rtlRevUInt7(const void * _data)
{ 
    unsigned __int64 temp = 0;
    _cpyrev7(&temp, _data);
    return temp;
}

unsigned __int64 rtlRevUInt8(const void * _data)
{ 
    unsigned __int64 temp;
    _cpyrev8(&temp, _data);
    return temp;
}

#else
//Big endian form
#error "Big endian implementation of rtlReadIntX functions need implementing...."
#endif

unsigned rtlCastUInt3(unsigned value)
{
    return (value & 0xffffff);
}

unsigned __int64 rtlCastUInt5(unsigned __int64 value)
{
    return (value & I64C(0xffffffffff));
}

unsigned __int64 rtlCastUInt6(unsigned __int64 value)
{
    return (value & I64C(0xffffffffffff));
}

unsigned __int64 rtlCastUInt7(unsigned __int64 value)
{
    return (value & I64C(0xffffffffffffff));
}

signed rtlCastInt3(signed value)
{
    return ((signed) ((unsigned) value << 8)) >> 8;
}

__int64 rtlCastInt5(__int64 value)
{
    return (__int64) ((__uint64) value << 24) >> 24;
}

__int64 rtlCastInt6(__int64 value)
{
    return (__int64) ((__uint64) value << 16) >> 16;
}

__int64 rtlCastInt7(__int64 value)
{
    return (__int64) ((__uint64) value << 8) >> 8;
}


unsigned __int64 rtlReadUInt(const void * self, unsigned length)
{
    switch (length)
    {
    case 1: return rtlReadUInt1(self);
    case 2: return rtlReadUInt2(self);
    case 3: return rtlReadUInt3(self);
    case 4: return rtlReadUInt4(self);
    case 5: return rtlReadUInt5(self);
    case 6: return rtlReadUInt6(self);
    case 7: return rtlReadUInt7(self);
    case 8: return rtlReadUInt8(self);
    }
    rtlFailUnexpected();
    return 0;
}

__int64 rtlReadInt(const void * self, unsigned length)
{
    switch (length)
    {
    case 1: return rtlReadInt1(self);
    case 2: return rtlReadInt2(self);
    case 3: return rtlReadInt3(self);
    case 4: return rtlReadInt4(self);
    case 5: return rtlReadInt5(self);
    case 6: return rtlReadInt6(self);
    case 7: return rtlReadInt7(self);
    case 8: return rtlReadInt8(self);
    }
    rtlFailUnexpected();
    return 0;
}

unsigned __int64 rtlReadSwapUInt(const void * self, unsigned length)
{
    switch (length)
    {
    case 1: return rtlReadSwapUInt1(self);
    case 2: return rtlReadSwapUInt2(self);
    case 3: return rtlReadSwapUInt3(self);
    case 4: return rtlReadSwapUInt4(self);
    case 5: return rtlReadSwapUInt5(self);
    case 6: return rtlReadSwapUInt6(self);
    case 7: return rtlReadSwapUInt7(self);
    case 8: return rtlReadSwapUInt8(self);
    }
    rtlFailUnexpected();
    return 0;
}

__int64 rtlReadSwapInt(const void * self, unsigned length)
{
    switch (length)
    {
    case 1: return rtlReadSwapInt1(self);
    case 2: return rtlReadSwapInt2(self);
    case 3: return rtlReadSwapInt3(self);
    case 4: return rtlReadSwapInt4(self);
    case 5: return rtlReadSwapInt5(self);
    case 6: return rtlReadSwapInt6(self);
    case 7: return rtlReadSwapInt7(self);
    case 8: return rtlReadSwapInt8(self);
    }
    rtlFailUnexpected();
    return 0;
}

void rtlWriteInt(void * self, __int64 val, unsigned length)
{
    switch (length)
    {
    case 1: rtlWriteInt1(self, (unsigned)val); break;
    case 2: rtlWriteInt2(self, (unsigned)val); break;
    case 3: rtlWriteInt3(self, (unsigned)val); break;
    case 4: rtlWriteInt4(self, (unsigned)val); break;
    case 5: rtlWriteInt5(self, val); break;
    case 6: rtlWriteInt6(self, val); break;
    case 7: rtlWriteInt7(self, val); break;
    case 8: rtlWriteInt8(self, val); break;
    default:
        rtlFailUnexpected();
        break;
    }
}

void rtlWriteSwapInt(void * self, __int64 val, unsigned length)
{
    switch (length)
    {
    case 1: rtlWriteSwapInt1(self, (unsigned)val); break;
    case 2: rtlWriteSwapInt2(self, (unsigned)val); break;
    case 3: rtlWriteSwapInt3(self, (unsigned)val); break;
    case 4: rtlWriteSwapInt4(self, (unsigned)val); break;
    case 5: rtlWriteSwapInt5(self, val); break;
    case 6: rtlWriteSwapInt6(self, val); break;
    case 7: rtlWriteSwapInt7(self, val); break;
    case 8: rtlWriteSwapInt8(self, val); break;
    default:
        rtlFailUnexpected();
        break;
    }
}

#endif
