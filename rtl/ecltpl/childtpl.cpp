$?doNotIncludeInGeneratedCode$
/*##############################################################################
## Copyright © 2011 HPCC Systems.  All rights reserved.
############################################################################## */

$?$/* Template for generating a child module for query */
#ifdef _WIN32
#define ECL_API __declspec(dllexport)
#define LOCAL_API
#define SERVICE_API __declspec(dllimport)
#define RTL_API __declspec(dllimport)
#define BCD_API __declspec(dllimport)
#else
  #ifdef USE_VISIBILITY
    #define ECL_API __attribute__ ((visibility("default")))
    #define LOCAL_API __attribute__ ((visibility("hidden")))
  #else
    #define ECL_API
    #define LOCAL_API
  #endif
  #define SERVICE_API
  #define RTL_API
  #define BCD_API
#endif

#include <string.h>
#include <malloc.h>
#include <stdlib.h>
#include <ctype.h>
#include <math.h>

#define CHEAP_UCHAR_DEF
#if __WIN32
typedef wchar_t UChar;
#else //__WIN32
typedef unsigned short UChar;
#endif //__WIN32

#ifdef _WIN32
typedef unsigned int size32_t; // avoid pulling in platform.h (which pulls in windows.h etc) just for this...
#else
#include "platform.h"
#endif

#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"

extern __declspec(dllimport) void _fastcall DecLock();
extern __declspec(dllimport) void _fastcall DecUnlock();
struct BcdCriticalBlock
{
    BcdCriticalBlock()      { DecLock(); }
    ~BcdCriticalBlock()     { DecUnlock(); }
};

@include@
@prototype@

#include "$headerName$"

@helper@
