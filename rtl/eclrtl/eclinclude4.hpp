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

#ifndef eclinclude4_incl
#define eclinclude4_incl

// Gather all the includes used by generated code into a single file so that we can
// take advantage of precompiled headers more easily

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
#include <stdlib.h>
#include <ctype.h>
#include <math.h>

#define CHEAP_UCHAR_DEF
#if _WIN32
typedef char16_t UChar;
#else //_WIN32
typedef unsigned short UChar;
#endif //_WIN32

#ifdef _WIN32
typedef unsigned int size32_t; // avoid pulling in platform.h (which pulls in windows.h etc) just for this...
typedef unsigned __int64 hash64_t;
#define DECL_EXPORT __declspec(dllexport)
#define DECL_IMPORT __declspec(dllimport)
#define DECL_LOCAL
#define DECL_EXCEPTION
#else
#include "platform.h"
#endif

#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"
#include "rtlbcd.hpp"
#include "rtlrecord.hpp"

#endif
