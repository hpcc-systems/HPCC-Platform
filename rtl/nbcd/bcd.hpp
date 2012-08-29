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

#ifndef _BCD_
#define _BCD_
#include "platform.h"

#ifdef _WIN32
 #ifdef NBCD_EXPORTS
  #define nbcd_decl __declspec(dllexport)
 #else
  #define nbcd_decl __declspec(dllimport)
 #endif
#else
 #define nbcd_decl
#endif

#ifndef _WIN32
#define _fastcall
#define __fastcall
#endif

nbcd_decl void  _fastcall  DecAbs( void );         // make top of stack absolute
nbcd_decl void  _fastcall  DecAdd( void );         // add top two values on stack and replace with result
nbcd_decl int   _fastcall  DecCompareNull( void ); // compare top of stack to NULL, returns result, takes value of stack
nbcd_decl int   _fastcall  DecDistinct( void );    // compares second to top value on stack, removes them and returns result
nbcd_decl int   _fastcall  DecDistinctR( void );   // compares top to second value on stack, removes them and returns result
nbcd_decl void  _fastcall  DecDivide( void );      // divide second by top value on stack and replace with result
nbcd_decl void  _fastcall  DecDivideR( void );     // divide top by second value on stack and replace with result
nbcd_decl void  _fastcall  DecDup( void );         // duplicate value on top of decimal stack
nbcd_decl void  _fastcall  DecSetPrecision(unsigned char declen, unsigned char prec); // changes length and precision of top value on stack
nbcd_decl void  _fastcall  DecSub( void );         // substract top from second value on stack and replace with result
nbcd_decl void  _fastcall  DecSubR( void );        // substract second from top value on stack and replace with result
nbcd_decl void  _fastcall  DecInfo(unsigned & digits, unsigned & prec); // returns the digits and precision of top value on stack
nbcd_decl void  _fastcall  DecClipInfo(unsigned & digits, unsigned & prec);
nbcd_decl void  _fastcall  DecLongPower(long pow);   // calculates top of stack to the power of long and replaces with result
nbcd_decl void  _fastcall  DecModulus( void );   // modulus second by top value on stack and replace with result
nbcd_decl void  _fastcall  DecMul( void );         // multiply values top and second on the stack and replace with result
nbcd_decl void  _fastcall  DecNegate( void );      // negate value on top of the decimal stack
nbcd_decl void  _fastcall  DecPopCString (unsigned, char *); // Pops decimal accumulator into CString
nbcd_decl char * _fastcall  DecPopCStringX();       // Pop decimal accumulator into CString allocated on heap
nbcd_decl __int64 _fastcall  DecPopInt64( void );    // Pops decimal accumulator into __Int64 
nbcd_decl void  _fastcall  DecPopDecimal(void * tgt,unsigned char declen,unsigned char prec); // Pops decimal value of the stack
nbcd_decl void  _fastcall  DecPopUDecimal(void * tgt,unsigned char declen,unsigned char prec); // Pops unsigned decimal value of the stack
nbcd_decl int   _fastcall  DecPopLong( void );       // Pops decimal accumulator into long
nbcd_decl unsigned long  _fastcall  DecPopUlong( void );    // Pops decimal accumulator into unsigned long
nbcd_decl double         _fastcall  DecPopReal( void );     // Pops decimal accumulator into real (double)
nbcd_decl unsigned     _fastcall  DecPopString( unsigned, char * );   // Pops decimal accumulator into string
nbcd_decl void  _fastcall  DecPopStringX( unsigned &, char * & );   // Pops decimal accumulator into string, determines size and allocates string
nbcd_decl void  _fastcall  DecPushCString(const char *s);  // Pushes CString onto decimal stack
nbcd_decl void  _fastcall  DecPushInt64(__int64 ); // Pushes __Int64 onto decimal stack
nbcd_decl void  _fastcall  DecPushUInt64(unsigned __int64 ); // Pushes unsigned __Int64 onto decimal stack
nbcd_decl void  _fastcall  DecPushLong( long );    // Pushes long value onto decimal stack 
nbcd_decl void  _fastcall  DecPushDecimal(const void *,unsigned char declen,unsigned char prec); // Pushes decimal value onto the stack
nbcd_decl void  _fastcall  DecPushUDecimal(const void *,unsigned char declen,unsigned char prec); // Pushes unsigned decimal value onto the stack
nbcd_decl void  _fastcall  DecPushReal( double d );  // pushes real (double) onto decimal stack
nbcd_decl void  _fastcall  DecPushString(unsigned, const char * ); // Pushes string onto decimal stack
nbcd_decl void  _fastcall  DecPushUlong( unsigned long ); // Pushes unsigned long value onto decimal stack
nbcd_decl void  _fastcall  DecRestoreStack( void * ); // Restore decimal stack
nbcd_decl void  _fastcall  DecRound( void );       // round value on top of decimal stack
nbcd_decl void  _fastcall  DecRoundUp( void );       // round value on top of decimal stack
nbcd_decl void  _fastcall  DecRoundTo( unsigned places );       // round value on top of decimal stack
nbcd_decl void  * _fastcall  DecSaveStack( void );   // Save decimal stack
nbcd_decl void  _fastcall  DecSwap( void );          // swap top and second values on decimal stack 
nbcd_decl void  _fastcall  DecTruncate( void );       // truncate value on top of decimal stack
nbcd_decl void  _fastcall  DecTruncateAt(unsigned places);       // truncate value on top of decimal stack
nbcd_decl void  _fastcall  DecUlongPower(unsigned long pow); // calculates top of stack to the power of unsigned long and replaces with result

nbcd_decl void  _fastcall  DecLock();
nbcd_decl void  _fastcall  DecUnlock();
nbcd_decl bool  _fastcall  DecValid(bool isSigned, unsigned digits, const void * data);
nbcd_decl bool  _fastcall  Dec2Bool(size32_t bytes, const void * data);
nbcd_decl bool  _fastcall  UDec2Bool(size32_t bytes, const void * data);

nbcd_decl int   _fastcall  DecCompareDecimal(size32_t bytes, const void * _left, const void * _right);
nbcd_decl int   _fastcall  DecCompareUDecimal(size32_t bytes, const void * _left, const void * _right);

// internal
void                     AddBytes(unsigned dest,unsigned src,unsigned num); 
nbcd_decl char  _fastcall  DecClip(void *);       
nbcd_decl void  _fastcall  DecRoundPos(void *,int by);        
nbcd_decl void  _fastcall  SetMAccum(unsigned char c);                       

struct nbcd_decl BcdCriticalBlock
{
    inline BcdCriticalBlock()       { DecLock(); }
    inline ~BcdCriticalBlock()      { DecUnlock(); }
};


#endif
