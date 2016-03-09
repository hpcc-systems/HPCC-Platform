/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#ifndef eclregex_incl
#define eclregex_incl

#ifndef CHEAP_UCHAR_DEF
#ifndef U_OVERRIDE_CXX_ALLOCATION
#define U_OVERRIDE_CXX_ALLOCATION 0 // Enabling this forces all allocation of ICU objects to ICU's heap, but is incompatible with jmemleak
#endif //U_OVERRIDE_CXX_ALLOCATION
#include "unicode/utf.h"
#endif //CHEAP_UCHAR_DEF

#if defined(_WIN32)&&!defined(ECLRTL_LOCAL)
#ifdef ECLRTL_EXPORTS
#define ECLRTL_API __declspec(dllexport)
#else
#define ECLRTL_API __declspec(dllimport)
#endif
#else
#define ECLRTL_API
#endif

#ifdef _MSC_VER
#undef __attribute__ // in case platform.h has been included
#define __attribute__(param) /* do nothing */
#endif

//-----------------------------------------------------------------------------

// RegEx Compiler for ansii  strings (uses BOOST or std::regex)
struct IStrRegExprFindInstance
{
    virtual bool found() const = 0;
    virtual void getMatchX(size32_t & outlen, char * & out, unsigned n = 0) const = 0;
};

struct ICompiledStrRegExpr
{
    virtual void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const = 0;
    virtual IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const = 0;
    virtual void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _srcLen, const char * _search) = 0;
};

// RegEx Compiler for unicode strings
struct IUStrRegExprFindInstance
{
    virtual bool found() const = 0;
    virtual void getMatchX(size32_t & outlen, UChar * & out, unsigned n = 0) const = 0;
};

struct ICompiledUStrRegExpr
{
    virtual void replace(size32_t & outlen, UChar * & out, size32_t slen, UChar const * str, size32_t rlen, UChar const * replace) const = 0;
    virtual IUStrRegExprFindInstance * find(const UChar * str, size32_t from, size32_t len) const = 0;
    virtual void getMatchSet(bool  & __isAllResult, size32_t & __resultBytes, void * & __result, size32_t _srcLen, const UChar * _search) = 0;
};

ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive);
ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiled);
ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * compiled);

ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive);
ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiled);
ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * compiled);


#endif
