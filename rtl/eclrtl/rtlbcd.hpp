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

#ifndef _RTLBCD_HPP_
#define _RTLBCD_HPP_

#include "eclrtl.hpp"

ECLRTL_API void  DecAbs( void );         // make top of stack absolute
ECLRTL_API void  DecAdd( void );         // add top two values on stack and replace with result
ECLRTL_API int   DecCompareNull( void ); // compare top of stack to NULL, returns result, takes value of stack
ECLRTL_API int   DecDistinct( void );    // compares second to top value on stack, removes them and returns result
ECLRTL_API int   DecDistinctR( void );   // compares top to second value on stack, removes them and returns result
ECLRTL_API void  DecDivide(byte dbz);      // divide second by top value on stack and replace with result
ECLRTL_API void  DecDivideR(byte dbz);     // divide top by second value on stack and replace with result
ECLRTL_API void  DecDup( void );         // duplicate value on top of decimal stack
ECLRTL_API void  DecSetPrecision(unsigned char declen, unsigned char prec); // changes length and precision of top value on stack
ECLRTL_API void  DecSub( void );         // substract top from second value on stack and replace with result
ECLRTL_API void  DecSubR( void );        // substract second from top value on stack and replace with result
ECLRTL_API void  DecInfo(unsigned & digits, unsigned & prec); // returns the digits and precision of top value on stack
ECLRTL_API void  DecClipInfo(unsigned & digits, unsigned & prec);
ECLRTL_API void  DecLongPower(long pow);   // calculates top of stack to the power of long and replaces with result
ECLRTL_API void  DecModulus(byte dbz);   // modulus second by top value on stack and replace with result
ECLRTL_API void  DecMul( void );         // multiply values top and second on the stack and replace with result
ECLRTL_API void  DecNegate( void );      // negate value on top of the decimal stack
ECLRTL_API void  DecPopCString (unsigned, char *); // Pops decimal accumulator into CString
ECLRTL_API char * DecPopCStringX();       // Pop decimal accumulator into CString allocated on heap
ECLRTL_API __int64 DecPopInt64( void );    // Pops decimal accumulator into __Int64
ECLRTL_API void  DecPopDecimal(void * tgt,unsigned char declen,unsigned char prec); // Pops decimal value of the stack
ECLRTL_API void  DecPopUDecimal(void * tgt,unsigned char declen,unsigned char prec); // Pops unsigned decimal value of the stack
ECLRTL_API int   DecPopLong( void );       // Pops decimal accumulator into long
ECLRTL_API unsigned long  DecPopUlong( void );    // Pops decimal accumulator into unsigned long
ECLRTL_API double         DecPopReal( void );     // Pops decimal accumulator into real (double)
ECLRTL_API unsigned     DecPopString( unsigned, char * );   // Pops decimal accumulator into string
ECLRTL_API void  DecPopStringX( unsigned &, char * & );   // Pops decimal accumulator into string, determines size and allocates string
ECLRTL_API void  DecPushCString(const char *s);  // Pushes CString onto decimal stack
ECLRTL_API void  DecPushInt64(__int64 ); // Pushes __Int64 onto decimal stack
ECLRTL_API void  DecPushUInt64(unsigned __int64 ); // Pushes unsigned __Int64 onto decimal stack
ECLRTL_API void  DecPushLong( long );    // Pushes long value onto decimal stack
ECLRTL_API void  DecPushDecimal(const void *,unsigned char declen,unsigned char prec); // Pushes decimal value onto the stack
ECLRTL_API void  DecPushUDecimal(const void *,unsigned char declen,unsigned char prec); // Pushes unsigned decimal value onto the stack
ECLRTL_API void  DecPushReal( double d );  // pushes real (double) onto decimal stack
ECLRTL_API void  DecPushString(unsigned, const char * ); // Pushes string onto decimal stack
ECLRTL_API void  DecPushUlong( unsigned long ); // Pushes unsigned long value onto decimal stack
ECLRTL_API void  DecRestoreStack( void * ); // Restore decimal stack
ECLRTL_API void  DecRound( void );       // round value on top of decimal stack
ECLRTL_API void  DecRoundUp( void );       // round value on top of decimal stack
ECLRTL_API void  DecRoundTo( unsigned places );       // round value on top of decimal stack
ECLRTL_API void  * DecSaveStack( void );   // Save decimal stack
ECLRTL_API void  DecSwap( void );          // swap top and second values on decimal stack
ECLRTL_API void  DecTruncate( void );       // truncate value on top of decimal stack
ECLRTL_API void  DecTruncateAt(unsigned places);       // truncate value on top of decimal stack
ECLRTL_API void  DecUlongPower(unsigned long pow); // calculates top of stack to the power of unsigned long and replaces with result

ECLRTL_API bool  DecValid(bool isSigned, unsigned digits, const void * data);
ECLRTL_API bool  DecValidTos();
ECLRTL_API bool  Dec2Bool(size32_t bytes, const void * data);
ECLRTL_API bool  UDec2Bool(size32_t bytes, const void * data);

ECLRTL_API int   DecCompareDecimal(size32_t bytes, const void * _left, const void * _right);
ECLRTL_API int   DecCompareUDecimal(size32_t bytes, const void * _left, const void * _right);

// internal
void                     AddBytes(unsigned dest,unsigned src,unsigned num); 
ECLRTL_API char  DecClip(void *);
ECLRTL_API void  DecRoundPos(void *,int by);
ECLRTL_API void  SetMAccum(unsigned char c);

ECLRTL_API void  DecLock();
ECLRTL_API void  DecUnlock();
ECLRTL_API unsigned DecMarkStack();
ECLRTL_API void DecReleaseStack(unsigned mark);

//No longer a critical section (since stack is thread_local), but prevents problems with exceptions.
class ECLRTL_API BcdCriticalBlock
{
public:
    inline BcdCriticalBlock()       { mark = DecMarkStack(); }
    inline ~BcdCriticalBlock()      { DecReleaseStack(mark); }

protected:
    unsigned mark;
};


#endif
