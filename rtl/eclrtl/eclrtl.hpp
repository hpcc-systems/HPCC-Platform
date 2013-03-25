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

#ifndef eclrtl_incl
#define eclrtl_incl

#include "jscm.hpp"

// Define CHEAP_UCHAR_DEF and typedef UChar yourself, before including this file, to avoid including ICU's headers
// This is cheaper when only unicode support used is through these interfaces, esp. in generated code
// Will not work if you're including any other ICU libraries in this module!

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

#ifndef I64C
#ifdef _WIN32
#define I64C(n) n##i64
#else
#define I64C(n) n##LL
#endif
#endif

typedef unsigned char byte;
interface IEngineRowAllocator;
interface IOutputMetaData;
interface IOutputRowSerializer;
interface IOutputRowDeserializer;
interface IAtom;
interface IRecordSize;
interface IException;
class StringBuffer;
class MemoryBuffer;

enum DBZaction { DBZnone, DBZzero, DBZnan, DBZfail }; // Different actions on divide by zero

//-----------------------------------------------------------------------------

// RegEx Compiler for ansii  strings (uses BOOST)
interface IStrRegExprFindInstance
{
    virtual bool found() const = 0;
    virtual void getMatchX(size32_t & outlen, char * & out, unsigned n = 0) const = 0;
};

interface ICompiledStrRegExpr
{
    virtual void replace(size32_t & outlen, char * & out, size32_t slen, char const * str, size32_t rlen, char const * replace) const = 0;
    virtual IStrRegExprFindInstance * find(const char * str, size32_t from, size32_t len, bool needToKeepSearchString) const = 0;
};

// RegEx Compiler for unicode strings
interface IUStrRegExprFindInstance
{
    virtual bool found() const = 0;
    virtual void getMatchX(size32_t & outlen, UChar * & out, unsigned n = 0) const = 0;
};

interface ICompiledUStrRegExpr
{
    virtual void replace(size32_t & outlen, UChar * & out, size32_t slen, UChar const * str, size32_t rlen, UChar const * replace) const = 0;
    virtual IUStrRegExprFindInstance * find(const UChar * str, size32_t from, size32_t len) const = 0;
};

//-----------------------------------------------------------------------------

ECLRTL_API void * rtlMalloc(size32_t size);
ECLRTL_API void rtlFree(void * x);
ECLRTL_API void * rtlRealloc(void * _ptr, size32_t size);
ECLRTL_API __int64 rtlRound(double x);
ECLRTL_API double rtlRoundTo(double x, int places);
ECLRTL_API __int64 rtlRoundDown(double x);
ECLRTL_API __int64 rtlRoundUp(double x);
ECLRTL_API void rtlIntFormat(unsigned & len, char * & target, __int64 value, unsigned width, unsigned flags);
ECLRTL_API void rtlRealFormat(unsigned & tlen, char * & target, double value, unsigned width, unsigned places);
ECLRTL_API void holeIntFormat(size32_t maxlen, char * target, __int64 value, unsigned width, unsigned flags);
ECLRTL_API void holeRealFormat(size32_t maxlen, char * target, double value, unsigned width, unsigned places);

ECLRTL_API void rtlUInt4ToStr(size32_t l, char * t, unsigned val);
ECLRTL_API void rtlUInt8ToStr(size32_t l, char * t, unsigned __int64 val);
ECLRTL_API void rtlInt4ToStr(size32_t l, char * t, int val);
ECLRTL_API void rtlInt8ToStr(size32_t l, char * t, __int64 val);

ECLRTL_API void rtlUInt4ToStrX(size32_t & l, char * & t, unsigned val);
ECLRTL_API void rtlUInt8ToStrX(size32_t & l, char * & t, unsigned __int64 val);
ECLRTL_API void rtlInt4ToStrX(size32_t & l, char * & t, int val);
ECLRTL_API void rtlInt8ToStrX(size32_t & l, char * & t, __int64 val);

ECLRTL_API void rtlUInt4ToVStr(size32_t l, char * t, unsigned val);
ECLRTL_API void rtlUInt8ToVStr(size32_t l, char * t, unsigned __int64 val);
ECLRTL_API void rtlInt4ToVStr(size32_t l, char * t, int val);
ECLRTL_API void rtlInt8ToVStr(size32_t l, char * t, __int64 val);

ECLRTL_API char * rtlUInt4ToVStrX(unsigned val);
ECLRTL_API char * rtlUInt8ToVStrX(unsigned __int64 val);
ECLRTL_API char * rtlInt4ToVStrX(int val);
ECLRTL_API char * rtlInt8ToVStrX(__int64 val);

ECLRTL_API double rtlStrToReal(size32_t l, const char * t);
ECLRTL_API double rtlVStrToReal(const char * t);
ECLRTL_API double rtlEStrToReal(size32_t l, const char * t);
ECLRTL_API double rtlUnicodeToReal(size32_t l, UChar const * t);

ECLRTL_API void rtlRealToStr(size32_t l, char * t, double val);
ECLRTL_API void rtlRealToStrX(size32_t & l, char * & t, double val);
ECLRTL_API void rtlRealToVStr(size32_t l, char * t, double val);
ECLRTL_API char * rtlRealToVStrX(double val);

ECLRTL_API void rtlRealToStr(size32_t l, char * t, float val);
ECLRTL_API void rtlRealToStrX(size32_t & l, char * & t, float val);
ECLRTL_API void rtlRealToVStr(size32_t l, char * t, float val);
ECLRTL_API char * rtlRealToVStrX(float val);

ECLRTL_API void rtlStrToStrX(unsigned & tlen, char * & tgt, unsigned slen, const void * src);
ECLRTL_API void rtlStrToDataX(unsigned & tlen, void * & tgt, unsigned slen, const void * src);
ECLRTL_API char * rtlStrToVStrX(unsigned slen, const void * src);
ECLRTL_API char * rtlEStrToVStrX(unsigned slen, const char * src);
ECLRTL_API void rtlEStrToStrX(unsigned & tlen, char * & tgt, unsigned slen, const char * src);
ECLRTL_API void rtlStrToEStrX(unsigned & tlen, char * & tgt, unsigned slen, const char * src);

ECLRTL_API unsigned rtlStrToUInt4(size32_t l, const char * t);
ECLRTL_API unsigned __int64 rtlStrToUInt8(size32_t l, const char * t);
ECLRTL_API int rtlStrToInt4(size32_t l, const char * t);
ECLRTL_API __int64 rtlStrToInt8(size32_t l, const char * t);
ECLRTL_API bool rtlStrToBool(size32_t l, const char * t);
ECLRTL_API __int64 rtlUnicodeToInt8(size32_t l, UChar const * tl);
ECLRTL_API bool rtlUnicodeToBool(size32_t l, UChar const * t);

ECLRTL_API unsigned rtlEStrToUInt4(size32_t l, const char * t);
ECLRTL_API unsigned __int64 rtlEStrToUInt8(size32_t l, const char * t);
ECLRTL_API int rtlEStrToInt4(size32_t l, const char * t);
ECLRTL_API __int64 rtlEStrToInt8(size32_t l, const char * t);

ECLRTL_API unsigned rtlVStrToUInt4(const char * t);
ECLRTL_API unsigned __int64 rtlVStrToUInt8(const char * t);
ECLRTL_API int rtlVStrToInt4(const char * t);
ECLRTL_API __int64 rtlVStrToInt8(const char * t);
ECLRTL_API bool rtlVStrToBool(const char * t);

ECLRTL_API int rtlSearchTableStringN(unsigned count, char * * table, unsigned width, const char * search);
ECLRTL_API int rtlSearchTableVStringN(unsigned count, char * * table, const char * search);

ECLRTL_API int rtlNewSearchDataTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search);
ECLRTL_API int rtlNewSearchEStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search);
ECLRTL_API int rtlNewSearchQStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search);
ECLRTL_API int rtlNewSearchStringTable(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search);

ECLRTL_API int rtlNewSearchUnicodeTable(unsigned count, unsigned elemlen, UChar * * table, unsigned width, const UChar * search, const char * locale);
ECLRTL_API int rtlNewSearchVUnicodeTable(unsigned count, UChar * * table, const UChar * search, const char * locale);
ECLRTL_API int rtlNewSearchUtf8Table(unsigned count, unsigned elemlen, char * * table, unsigned width, const char * search, const char * locale);


ECLRTL_API int rtlSearchTableInteger8(unsigned count, __int64 * table, __int64 search);
ECLRTL_API int rtlSearchTableUInteger8(unsigned count, unsigned __int64 * table, unsigned __int64 search);
ECLRTL_API int rtlSearchTableInteger4(unsigned count, int * table, int search);
ECLRTL_API int rtlSearchTableUInteger4(unsigned count, unsigned * table, unsigned search);

ECLRTL_API int searchTableStringN(unsigned count, const char * * table, unsigned width, const char * search);
ECLRTL_API unsigned rtlCrc32(unsigned len, const void * buffer, unsigned crc);

//These functions should have an X in the name
ECLRTL_API void rtlConcat(unsigned & tlen, char * * tgt, ...);
ECLRTL_API void rtlConcatVStr(char * * tgt, ...);
ECLRTL_API void rtlConcatUnicode(unsigned & tlen, UChar * * tgt, ...);
ECLRTL_API void rtlConcatVUnicode(UChar * * tgt, ...);
ECLRTL_API void rtlConcatExtend(unsigned & tlen, char * & tgt, unsigned slen, const char * src);

ECLRTL_API void rtlConcatStrF(unsigned tlen, void * tgt, int fill, ...);
ECLRTL_API void rtlConcatVStrF(unsigned tlen, char * tgt, ...);
ECLRTL_API void rtlConcatUnicodeF(unsigned tlen, UChar * tgt, ...);
ECLRTL_API void rtlConcatVUnicodeF(unsigned tlen, UChar * tgt, ...);

ECLRTL_API char * rtlCreateQuotedString(unsigned _len_tgt,char * tgt);

ECLRTL_API unsigned rtlConcatStrToStr(unsigned tlen, char * tgt, unsigned idx, unsigned slen, const char * src);
ECLRTL_API unsigned rtlConcatVStrToStr(unsigned tlen, char * tgt, unsigned idx, const char * src);
ECLRTL_API void rtlConcatStrToVStr(unsigned tlen, void * _tgt, unsigned slen, const void * src);
ECLRTL_API void rtlConcatVStrToVStr(unsigned tlen, void * _tgt, const char * src);
ECLRTL_API unsigned rtlConcatUnicodeToUnicode(unsigned tlen, UChar * tgt, unsigned idx, unsigned slen, UChar const * src);
ECLRTL_API unsigned rtlConcatVUnicodeToUnicode(unsigned tlen, UChar * tgt, unsigned idx, UChar const * src);

ECLRTL_API void rtlSubDataFX(unsigned & tlen, void * & tgt, unsigned slen, const void * src, unsigned from);
ECLRTL_API void rtlSubDataFT(unsigned tlen, void * tgt, unsigned slen, const void * src, unsigned from, unsigned to);
ECLRTL_API void rtlSubDataFTX(unsigned & tlen, void * & tgt, unsigned slen, const void * src, unsigned from, unsigned to);
ECLRTL_API void rtlSubQStrFX(unsigned & tlen, char * & tgt, unsigned slen, const char * _src, unsigned from);
ECLRTL_API void rtlSubQStrFT(unsigned tlen, char * tgt, unsigned slen, const char * src, unsigned from, unsigned to);
ECLRTL_API void rtlSubQStrFTX(unsigned & tlen, char * & tgt, unsigned slen, const char * _src, unsigned from, unsigned to);
ECLRTL_API void rtlSubStrFTX(unsigned & tlen, char * & tgt, unsigned slen, const char * _src, unsigned from, unsigned to);
ECLRTL_API void rtlSubStrFX(unsigned & tlen, char * & tgt, unsigned slen, const char * _src, unsigned from);
ECLRTL_API void rtlSubStrFT(unsigned tlen, char * tgt, unsigned slen, const char * src, unsigned from, unsigned to);
ECLRTL_API void rtlUnicodeSubStrFTX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src, unsigned from, unsigned to);
ECLRTL_API void rtlUnicodeSubStrFX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src, unsigned from);
ECLRTL_API void rtlESpaceFill(unsigned tlen, char * tgt, unsigned idx);
ECLRTL_API void rtlSpaceFill(unsigned tlen, char * tgt, unsigned idx);
ECLRTL_API void rtlZeroFill(unsigned tlen, char * tgt, unsigned idx);
ECLRTL_API void rtlNullTerminate(unsigned tlen, char * tgt, unsigned idx);
ECLRTL_API void rtlUnicodeSpaceFill(unsigned tlne, UChar * tgt, unsigned idx);
ECLRTL_API void rtlUnicodeNullTerminate(unsigned tlen, UChar * tgt, unsigned idx);

ECLRTL_API void rtlStringToLower(size32_t l, char * t);
ECLRTL_API void rtlStringToUpper(size32_t l, char * t);
ECLRTL_API void rtlUnicodeToLower(size32_t l, UChar * t, char const * locale);
ECLRTL_API void rtlUnicodeToUpper(size32_t l, UChar * t, char const * locale);
ECLRTL_API void rtlUnicodeToLowerX(size32_t & lenout, UChar * & out, size32_t l, const UChar * t, char const * locale);
ECLRTL_API unsigned rtlTrimDataLen(size32_t l, const void * _t);            // trims 0 bytes.
ECLRTL_API unsigned rtlTrimStrLen(size32_t l, const char * t);
ECLRTL_API unsigned rtlTrimUnicodeStrLen(size32_t l, UChar const * t);
ECLRTL_API unsigned rtlTrimUtf8StrLen(size32_t l, const char * t);
ECLRTL_API unsigned rtlTrimVStrLen(const char * t);
ECLRTL_API unsigned rtlTrimVUnicodeStrLen(UChar const * t);
ECLRTL_API int rtlCompareStrStr(unsigned l1, const char * p1, unsigned l2, const char * p2);
ECLRTL_API int rtlCompareVStrVStr(const char * p1, const char * p2);
ECLRTL_API int rtlCompareStrBlank(unsigned l1, const char * p1);
ECLRTL_API int rtlCompareDataData(unsigned l1, const void * p1, unsigned l2, const void * p2);
ECLRTL_API int rtlCompareEStrEStr(unsigned l1, const char * p1, unsigned l2, const char * p2);
ECLRTL_API int rtlCompareUnicodeUnicode(unsigned l1, UChar const * p1, unsigned l2, UChar const * p2, char const * locale); // l1,2 in UChars, i.e. bytes/2
ECLRTL_API int rtlCompareUnicodeUnicodeStrength(unsigned l1, UChar const * p1, unsigned l2, UChar const * p2, char const * locale, unsigned strength); // strength should be between 1 (primary) and 5 (identical)
ECLRTL_API int rtlCompareVUnicodeVUnicode(UChar const * p1, UChar const * p2, char const * locale);
ECLRTL_API int rtlCompareVUnicodeVUnicodeStrength(UChar const * p1, UChar const * p2, char const * locale, unsigned strength);
ECLRTL_API void rtlKeyUnicodeX(unsigned & tlen, void * & tgt, unsigned slen, const UChar * src, const char * locale);
ECLRTL_API void rtlKeyUnicodeStrengthX(unsigned & tlen, void * & tgt, unsigned slen, const UChar * src, const char * locale, unsigned strength);
ECLRTL_API bool rtlGetNormalizedUnicodeLocaleName(unsigned len, char const * in, char * out);

ECLRTL_API int rtlPrefixDiffStr(unsigned l1, const char * p1, unsigned l2, const char * p2);
ECLRTL_API int rtlPrefixDiffUnicode(unsigned l1, const UChar * p1, unsigned l2, const UChar * p2, const char * locale);

ECLRTL_API void rtlTrimRight(unsigned &tlen, char * &tgt, unsigned slen, const char * src); // YMA
ECLRTL_API void rtlTrimUnicodeRight(unsigned &tlen, UChar * &tgt, unsigned slen, UChar const * src);
ECLRTL_API void rtlTrimUtf8Right(unsigned &tlen, char * &tgt, unsigned slen, char const * src);
ECLRTL_API void rtlTrimVRight(unsigned &tlen, char * &tgt, const char * src); // YMA
ECLRTL_API void rtlTrimVUnicodeRight(unsigned &tlen, UChar * &tgt, UChar const * src);
ECLRTL_API void rtlTrimLeft(unsigned &tlen, char * &tgt, unsigned slen, const char * src); // YMA
ECLRTL_API void rtlTrimUnicodeLeft(unsigned &tlen, UChar * &tgt, unsigned slen, UChar const * src);
ECLRTL_API void rtlTrimUtf8Left(unsigned &tlen, char * &tgt, unsigned slen, const char * src);
ECLRTL_API void rtlTrimVLeft(unsigned &tlen, char * &tgt, const char * src); // YMA
ECLRTL_API void rtlTrimVUnicodeLeft(unsigned &tlen, UChar * &tgt, UChar const * src);
ECLRTL_API void rtlTrimBoth(unsigned &tlen, char * &tgt, unsigned slen, const char * src); // YMA
ECLRTL_API void rtlTrimUnicodeBoth(unsigned &tlen, UChar * &tgt, unsigned slen, UChar const * src);
ECLRTL_API void rtlTrimUtf8Both(unsigned &tlen, char * &tgt, unsigned slen, const char * src);
ECLRTL_API void rtlTrimVBoth(unsigned &tlen, char * &tgt, const char * src); // YMA
ECLRTL_API void rtlTrimVUnicodeBoth(unsigned &tlen, UChar * &tgt, UChar const * src);
ECLRTL_API void rtlTrimAll(unsigned &tlen, char * &tgt, unsigned slen, const char * src); // YMA
ECLRTL_API void rtlTrimUnicodeAll(unsigned &tlen, UChar * &tgt, unsigned slen, UChar const * src);
ECLRTL_API void rtlTrimUtf8All(unsigned &tlen, char * &tgt, unsigned slen, const char * src);
ECLRTL_API void rtlTrimVAll(unsigned &tlen, char * &tgt, const char * src); // YMA
ECLRTL_API void rtlTrimVUnicodeAll(unsigned &tlen, UChar * &tgt, UChar const * src);
ECLRTL_API unsigned rtlTrimStrLenNonBlank(size32_t l, const char * t);
ECLRTL_API unsigned rtlTrimVStrLenNonBlank(const char * t);

ECLRTL_API void rtlAssignTrimLeftV(size32_t tlen, char * tgt, unsigned slen, const char * src);
ECLRTL_API void rtlAssignTrimVLeftV(size32_t tlen, char * tgt, const char * src);
ECLRTL_API void rtlAssignTrimRightV(size32_t tlen, char * tgt, unsigned slen, const char * src);
ECLRTL_API void rtlAssignTrimVRightV(size32_t tlen, char * tgt, const char * src);
ECLRTL_API void rtlAssignTrimBothV(size32_t tlen, char * tgt, unsigned slen, const char * src);
ECLRTL_API void rtlAssignTrimVBothV(size32_t tlen, char * tgt, const char * src);
ECLRTL_API void rtlAssignTrimAllV(size32_t tlen, char * tgt, unsigned slen, const char * src);
ECLRTL_API void rtlAssignTrimVAllV(size32_t tlen, char * tgt, const char * src);

ECLRTL_API void rtlAssignTrimUnicodeLeftV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src);
ECLRTL_API void rtlAssignTrimVUnicodeLeftV(size32_t tlen, UChar * tgt, const UChar * src);
ECLRTL_API void rtlAssignTrimUnicodeRightV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src);
ECLRTL_API void rtlAssignTrimVUnicodeRightV(size32_t tlen, UChar * tgt, const UChar * src);
ECLRTL_API void rtlAssignTrimUnicodeBothV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src);
ECLRTL_API void rtlAssignTrimVUnicodeBothV(size32_t tlen, UChar * tgt, const UChar * src);
ECLRTL_API void rtlAssignTrimUnicodeAllV(size32_t tlen, UChar * tgt, unsigned slen, const UChar * src);
ECLRTL_API void rtlAssignTrimVUnicodeAllV(size32_t tlen, UChar * tgt, const UChar * src);

ECLRTL_API bool rtlDataToBool(unsigned tlen, const void * tgt);
ECLRTL_API void rtlBoolToData(unsigned tlen, void * tgt, bool src);
ECLRTL_API void rtlBoolToStr(unsigned tlen, void * tgt, bool src);
ECLRTL_API void rtlBoolToVStr(char * tgt, int src);
ECLRTL_API void rtlBoolToStrX(unsigned & tlen, char * & tgt, bool src);
ECLRTL_API char * rtlBoolToVStrX(bool src);
ECLRTL_API void rtlStrToData(unsigned tlen, void * tgt, unsigned slen, const void * src);
ECLRTL_API void rtlStrToStr(unsigned tlen, void * tgt, unsigned slen, const void * src);
ECLRTL_API void rtlDataToData(unsigned tlen, void * tgt, unsigned slen, const void * src);
ECLRTL_API void rtlEStrToEStr(unsigned tlen, void * tgt, unsigned slen, const void * src);
ECLRTL_API void rtlEStrToVStr(unsigned tlen, void * tgt, unsigned slen, const char * src);
ECLRTL_API void rtlStrToVStr(unsigned tlen, void * tgt, unsigned slen, const void * src);
ECLRTL_API void rtlVStrToData(unsigned tlen, void * tgt, const char * src);
ECLRTL_API void rtlVStrToStr(unsigned tlen, void * tgt, const char * src);
ECLRTL_API void rtlVStrToVStr(unsigned tlen, void * tgt, const char * src);
ECLRTL_API void rtlEStrToStr(unsigned outlen, char *out, unsigned inlen, const char *in);
ECLRTL_API void rtlStrToEStr(unsigned outlen, char *out, unsigned inlen, const char *in);
ECLRTL_API void rtlCodepageToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlCodepageToVUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlVCodepageToUnicode(unsigned outlen, UChar * out, char const * in, char const * codepage);
ECLRTL_API void rtlVCodepageToVUnicode(unsigned outlen, UChar * out, char const * in, char const * codepage);
ECLRTL_API void rtlCodepageToUnicodeUnescape(unsigned outlen, UChar * out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlUnicodeToCodepage(unsigned outlen, char * out, unsigned inlen, UChar const * in, char const * codepage);
ECLRTL_API void rtlUnicodeToData(unsigned outlen, void * out, unsigned inlen, UChar const * in);
ECLRTL_API void rtlUnicodeToVCodepage(unsigned outlen, char * out, unsigned inlen, UChar const * in, char const * codepage);
ECLRTL_API void rtlVUnicodeToCodepage(unsigned outlen, char * out, UChar const * in, char const * codepage);
ECLRTL_API void rtlVUnicodeToData(unsigned outlen, void * out, UChar const * in);
ECLRTL_API void rtlVUnicodeToVCodepage(unsigned outlen, char * out, UChar const * in, char const * codepage);
ECLRTL_API void rtlCodepageToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API UChar * rtlCodepageToVUnicodeX(unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlVCodepageToUnicodeX(unsigned & outlen, UChar * & out, char const * in, char const * codepage);
ECLRTL_API UChar * rtlVCodepageToVUnicodeX(char const * in, char const * codepage);
ECLRTL_API void rtlCodepageToUnicodeXUnescape(unsigned & outlen, UChar * & out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlUnicodeToCodepageX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in, char const * codepage);
ECLRTL_API void rtlUnicodeToDataX(unsigned & outlen, void * & out, unsigned inlen, UChar const * in);
ECLRTL_API char * rtlUnicodeToVCodepageX(unsigned inlen, UChar const * in, char const * codepage);
ECLRTL_API void rtlVUnicodeToCodepageX(unsigned & outlen, char * & out, UChar const * in, char const * codepage);
ECLRTL_API char * rtlVUnicodeToVCodepageX(UChar const * in, char const * codepage);
ECLRTL_API void rtlStrToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in);
ECLRTL_API void rtlUnicodeToStr(unsigned outlen, char * out, unsigned inlen, UChar const * in);
ECLRTL_API void rtlStrToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in);
ECLRTL_API void rtlUnicodeToStrX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in);
ECLRTL_API void rtlUnicodeToEscapedStrX(unsigned & outlen, char * & out, unsigned inlen, UChar const * in);
ECLRTL_API void rtlUnicodeToQuotedUTF8X(unsigned & outlen, char * & out, unsigned inlen, UChar const * in);
ECLRTL_API bool rtlCodepageToCodepage(unsigned outlen, char * out, unsigned inlen, char const * in, char const * outcodepage, char const * incodepage); //returns success, false probably means illegal input or overflow
ECLRTL_API int rtlSingleUtf8ToCodepage(char * out, unsigned inlen, char const * in, char const * outcodepage); //returns number of trailbytes on character in UTF8 if valid, -1 error (illegal input or overflow (this routine assumes output is single character, which can break if outcodepage uses multibyte sequences, so don't do that))

ECLRTL_API void rtlCreateOrder(void * tgt, const void * src, unsigned num, unsigned width, const void * compare);
ECLRTL_API unsigned rtlRankFromOrder(unsigned index, unsigned num, const void * order);
ECLRTL_API unsigned rtlRankedFromOrder(unsigned index, unsigned num, const void * order) ;

ECLRTL_API void rtlEcho(unsigned len, const char * src);    // useful for testing.

const unsigned int HASH32_INIT = 0x811C9DC5;
const unsigned __int64 HASH64_INIT = I64C(0xcbf29ce484222325);

ECLRTL_API unsigned rtlHashData( unsigned length, const void *_k, unsigned initval);
ECLRTL_API unsigned rtlHashString( unsigned length, const char *_k, unsigned initval);
ECLRTL_API unsigned rtlHashUnicode(unsigned length, UChar const * k, unsigned initval);
ECLRTL_API unsigned rtlHashUtf8(unsigned length, const char * k, unsigned initval);
ECLRTL_API unsigned rtlHashVStr(const char * k, unsigned initval);
ECLRTL_API unsigned rtlHashVUnicode(UChar const * k, unsigned initval);
ECLRTL_API unsigned rtlHashDataNC( unsigned length, const void * _k, unsigned initval);
ECLRTL_API unsigned rtlHashVStrNC(const char * k, unsigned initval);

ECLRTL_API hash64_t rtlHash64Data(size32_t len, const void *buf, hash64_t hval);
ECLRTL_API hash64_t rtlHash64VStr(const char *str, hash64_t hval);
ECLRTL_API hash64_t rtlHash64Unicode(unsigned length, UChar const * k, hash64_t initval);
ECLRTL_API hash64_t rtlHash64Utf8(unsigned length, const char * k, hash64_t initval);
ECLRTL_API hash64_t rtlHash64VUnicode(UChar const * k, hash64_t initval);

ECLRTL_API unsigned rtlHash32Data(size32_t len, const void *buf, unsigned hval);
ECLRTL_API unsigned rtlHash32VStr(const char *str, unsigned hval);
ECLRTL_API unsigned rtlHash32Unicode(unsigned length, UChar const * k, unsigned initval);
ECLRTL_API unsigned rtlHash32Utf8(unsigned length, const char * k, unsigned initval);
ECLRTL_API unsigned rtlHash32VUnicode(UChar const * k, unsigned initval);

ECLRTL_API unsigned rtlCrcData( unsigned length, const void *_k, unsigned initval);
ECLRTL_API unsigned rtlCrcUnicode(unsigned length, UChar const * k, unsigned initval);
ECLRTL_API unsigned rtlCrcUtf8(unsigned length, const char * k, unsigned initval);
ECLRTL_API unsigned rtlCrcVStr( const char * k, unsigned initval);
ECLRTL_API unsigned rtlCrcVUnicode(UChar const * k, unsigned initval);

ECLRTL_API unsigned rtlRandom();
ECLRTL_API void rtlSeedRandom(unsigned value);
ECLRTL_API void rtlFail(int code, const char *msg);
ECLRTL_API void rtlSysFail(int code, const char *msg);
ECLRTL_API void rtlFailUnexpected();
ECLRTL_API void rtlFailOnAssert();
ECLRTL_API void rtlFailDivideByZero();

ECLRTL_API void rtlReportFieldOverflow(unsigned size, unsigned max, const char * name);
ECLRTL_API void rtlReportRowOverflow(unsigned size, unsigned max);
ECLRTL_API void rtlCheckFieldOverflow(unsigned size, unsigned max, const char * name);
ECLRTL_API void rtlCheckRowOverflow(unsigned size, unsigned max);

inline int rtlReadInt1(const void * data) { return *(signed char *)data; }
inline int rtlReadInt2(const void * data) { return *(signed short *)data; }
ECLRTL_API int rtlReadInt3(const void * data);
inline int rtlReadInt4(const void * data) { return *(signed int *)data; }
ECLRTL_API __int64 rtlReadInt5(const void * data);
ECLRTL_API __int64 rtlReadInt6(const void * data);
ECLRTL_API __int64 rtlReadInt7(const void * data);
inline __int64 rtlReadInt8(const void * data) { return *(signed __int64 *)data; }
ECLRTL_API __int64 rtlReadInt(const void * data, unsigned length);

inline unsigned int rtlReadUInt1(const void * data) { return *(unsigned char *)data; }
inline unsigned int rtlReadUInt2(const void * data) { return *(unsigned short *)data; }
ECLRTL_API unsigned rtlReadUInt3(const void * data);
inline unsigned int rtlReadUInt4(const void * data) { return *(unsigned int *)data; }
ECLRTL_API unsigned __int64 rtlReadUInt5(const void * data);
ECLRTL_API unsigned __int64 rtlReadUInt6(const void * data);
ECLRTL_API unsigned __int64 rtlReadUInt7(const void * data);
inline unsigned __int64 rtlReadUInt8(const void * data) { return *(unsigned __int64 *)data; }
ECLRTL_API unsigned __int64 rtlReadUInt(const void * data, unsigned length);

//MORE: Change if reverse endian, or if alignment issues.
inline size32_t rtlReadSize32t(void * data) { return *(const size32_t *)data; }

inline void rtlWriteInt1(void * data, unsigned value) { *(unsigned char *)data = value; }
inline void rtlWriteInt2(void * data, unsigned value) { *(unsigned short *)data = value; }
ECLRTL_API void rtlWriteInt3(void * data, unsigned value);
inline void rtlWriteInt4(void * data, unsigned value) { *(unsigned *)data = value; }
ECLRTL_API void rtlWriteInt5(void * data, unsigned __int64 value);
ECLRTL_API void rtlWriteInt6(void * data, unsigned __int64 value);
ECLRTL_API void rtlWriteInt7(void * data, unsigned __int64 value);
inline void rtlWriteInt8(void * data, unsigned value) { *(unsigned __int64 *)data = value; }
inline void rtlWriteSize32t(void * data, unsigned value) { *(size32_t *)data = value; }
ECLRTL_API void rtlWriteInt(void * self, __int64 val, unsigned length);

inline int rtlReadSwapInt1(const void * data) { return *(signed char *)data; }
ECLRTL_API int rtlReadSwapInt2(const void * data);
ECLRTL_API int rtlReadSwapInt3(const void * data);
ECLRTL_API int rtlReadSwapInt4(const void * data);
ECLRTL_API __int64 rtlReadSwapInt5(const void * data);
ECLRTL_API __int64 rtlReadSwapInt6(const void * data);
ECLRTL_API __int64 rtlReadSwapInt7(const void * data);
ECLRTL_API __int64 rtlReadSwapInt8(const void * data);
ECLRTL_API __int64 rtlReadSwapInt(const void * data, unsigned length);

inline unsigned int rtlReadSwapUInt1(const void * data) { return *(unsigned char *)data; }
ECLRTL_API unsigned rtlReadSwapUInt2(const void * data);
ECLRTL_API unsigned rtlReadSwapUInt3(const void * data);
ECLRTL_API unsigned rtlReadSwapUInt4(const void * data);
ECLRTL_API unsigned __int64 rtlReadSwapUInt5(const void * data);
ECLRTL_API unsigned __int64 rtlReadSwapUInt6(const void * data);
ECLRTL_API unsigned __int64 rtlReadSwapUInt7(const void * data);
ECLRTL_API unsigned __int64 rtlReadSwapUInt8(const void * data);
ECLRTL_API unsigned __int64 rtlReadSwapUInt(const void * data, unsigned length);

ECLRTL_API void rtlWriteSwapInt3(void * data, unsigned value);
ECLRTL_API void rtlWriteSwapInt5(void * data, unsigned __int64 value);
ECLRTL_API void rtlWriteSwapInt6(void * data, unsigned __int64 value);
ECLRTL_API void rtlWriteSwapInt7(void * data, unsigned __int64 value);

ECLRTL_API short rtlRevInt2(const void * data);
ECLRTL_API int rtlRevInt3(const void * data);
ECLRTL_API int rtlRevInt4(const void * data);
ECLRTL_API __int64 rtlRevInt5(const void * data);
ECLRTL_API __int64 rtlRevInt6(const void * data);
ECLRTL_API __int64 rtlRevInt7(const void * data);
ECLRTL_API __int64 rtlRevInt8(const void * data);
ECLRTL_API unsigned short rtlRevUInt2(const void * data);
ECLRTL_API unsigned rtlRevUInt3(const void * data);
ECLRTL_API unsigned rtlRevUInt4(const void * data);
ECLRTL_API unsigned __int64 rtlRevUInt5(const void * data);
ECLRTL_API unsigned __int64 rtlRevUInt6(const void * data);
ECLRTL_API unsigned __int64 rtlRevUInt7(const void * data);
ECLRTL_API unsigned __int64 rtlRevUInt8(const void * data);

ECLRTL_API unsigned rtlCastUInt3(unsigned value);
ECLRTL_API unsigned __int64 rtlCastUInt5(unsigned __int64 value);
ECLRTL_API unsigned __int64 rtlCastUInt6(unsigned __int64 value);
ECLRTL_API unsigned __int64 rtlCastUInt7(unsigned __int64 value);
ECLRTL_API signed rtlCastInt3(signed value);
ECLRTL_API __int64 rtlCastInt5(__int64 value);
ECLRTL_API __int64 rtlCastInt6(__int64 value);
ECLRTL_API __int64 rtlCastInt7(__int64 value);

inline unsigned rtliCastUInt3(unsigned value)                 { return (value & 0xffffff); }
inline unsigned __int64 rtliCastUInt5(unsigned __int64 value) { return (value & I64C(0xffffffffff)); }
inline unsigned __int64 rtliCastUInt6(unsigned __int64 value) { return (value & I64C(0xffffffffffff)); }
inline unsigned __int64 rtliCastUInt7(unsigned __int64 value) { return (value & I64C(0xffffffffffffff)); }
inline signed rtliCastInt3(signed value)   { return (value << 8) >> 8; }
inline __int64 rtliCastInt5(__int64 value) { return (value << 24) >> 24; }
inline __int64 rtliCastInt6(__int64 value) { return (value << 16) >> 16; }
inline __int64 rtliCastInt7(__int64 value) { return (value << 8) >> 8; }

ECLRTL_API unsigned __int64 rtlGetPackedUnsigned(const void * _ptr);
ECLRTL_API void rtlSetPackedUnsigned(void * _ptr, unsigned __int64 value);
ECLRTL_API size32_t rtlGetPackedSize(const void * _ptr);
ECLRTL_API __int64 rtlGetPackedSigned(const void * ptr);
ECLRTL_API void rtlSetPackedSigned(void * ptr, __int64 value);
ECLRTL_API size32_t rtlGetPackedSizeFromFirst(byte first);

ECLRTL_API void rtlReleaseRow(const void * row);
ECLRTL_API void * rtlLinkRow(const void * row);
ECLRTL_API void rtlReleaseRowset(unsigned count, byte * * rowset);
ECLRTL_API byte * * rtlLinkRowset(byte * * rowset);

ECLRTL_API void ensureRtlLoaded();      // call this to create a static link to the rtl...

ECLRTL_API void outputXmlString(unsigned len, const char *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlBool(bool field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlData(unsigned len, const void *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlInt(__int64 field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlReal(double field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlDecimal(const void *field, unsigned digits, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlUDecimal(const void *field, unsigned digits, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlBeginNested(const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlEndNested(const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlSetAll(StringBuffer &out);

ECLRTL_API void outputXmlAttrString(unsigned len, const char *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrData(unsigned len, const void *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrBool(bool field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrInt(__int64 field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrUInt(unsigned __int64 field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrReal(double field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputXmlAttrUtf8(unsigned len, const char *field, const char *fieldname, StringBuffer &out);

ECLRTL_API void outputJsonDecimal(const void *field, unsigned digits, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputJsonUDecimal(const void *field, unsigned digits, unsigned precision, const char *fieldname, StringBuffer &out);
ECLRTL_API void outputJsonUnicode(unsigned len, const UChar *field, const char *fieldname, StringBuffer &out);

ECLRTL_API void deserializeRaw(unsigned size, void *record, MemoryBuffer & in);
ECLRTL_API void deserializeDataX(size32_t & len, void * & data, MemoryBuffer &in);
ECLRTL_API void deserializeStringX(size32_t & len, char * & data, MemoryBuffer &in);
ECLRTL_API char * deserializeCStringX(MemoryBuffer &in);
ECLRTL_API void deserializeSet(bool & isAll, size32_t & len, void * & data, MemoryBuffer &in);
ECLRTL_API void deserializeUnicodeX(size32_t & len, UChar * & data, MemoryBuffer &in);
ECLRTL_API void deserializeUtf8X(size32_t & len, char * & data, MemoryBuffer &in);
ECLRTL_API UChar * deserializeVUnicodeX(MemoryBuffer &in);
ECLRTL_API void deserializeQStrX(size32_t & len, char * & data, MemoryBuffer &out);
ECLRTL_API void deserializeRowsetX(size32_t & count, byte * * & data, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, MemoryBuffer &in);
ECLRTL_API void deserializeGroupedRowsetX(size32_t & count, byte * * & data, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, MemoryBuffer &in);
ECLRTL_API void deserializeDictionaryX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, MemoryBuffer &in);

ECLRTL_API byte * rtlDeserializeRow(IEngineRowAllocator * rowAllocator, IOutputRowDeserializer * deserializer, const void * src);
ECLRTL_API byte * rtlDeserializeBufferRow(IEngineRowAllocator * rowAllocator, IOutputRowDeserializer * deserializer, MemoryBuffer & buffer);

ECLRTL_API void serializeRaw(unsigned size, const void *record, MemoryBuffer &out);
ECLRTL_API void serializeDataX(size32_t len, const void * data, MemoryBuffer &out);
ECLRTL_API void serializeStringX(size32_t len, const char * data, MemoryBuffer &out);
ECLRTL_API void serializeCStringX(const char * data, MemoryBuffer &out);
ECLRTL_API void serializeSet(bool isAll, size32_t len, const void * data, MemoryBuffer &in);
ECLRTL_API void serializeUnicodeX(size32_t len, const UChar * data, MemoryBuffer &out);
ECLRTL_API void serializeUtf8X(size32_t len, const char * data, MemoryBuffer &out);
ECLRTL_API void serializeQStrX(size32_t len, const char * data, MemoryBuffer &out);
ECLRTL_API void serializeRowsetX(size32_t count, byte * * data, IOutputRowSerializer * serializer, MemoryBuffer &out);
ECLRTL_API void serializeGroupedRowsetX(size32_t count, byte * * data, IOutputRowSerializer * serializer, MemoryBuffer &out);
ECLRTL_API void serializeRow(const void * row, IOutputRowSerializer * serializer, MemoryBuffer & out);
ECLRTL_API void serializeDictionaryX(size32_t count, byte * * rows, IOutputRowSerializer * serializer, MemoryBuffer & buffer);

ECLRTL_API void serializeFixedString(unsigned len, const char *field, MemoryBuffer &out);
ECLRTL_API void serializeLPString(unsigned len, const char *field, MemoryBuffer &out);
ECLRTL_API void serializeVarString(const char *field, MemoryBuffer &out);
ECLRTL_API void serializeBool(bool field, MemoryBuffer &out);
ECLRTL_API void serializeFixedData(unsigned len, const void *field, MemoryBuffer &out);
ECLRTL_API void serializeLPData(unsigned len, const void *field, MemoryBuffer &out);
ECLRTL_API void serializeInt1(signed char field, MemoryBuffer &out);
ECLRTL_API void serializeInt2(signed short field, MemoryBuffer &out);
ECLRTL_API void serializeInt3(signed int field, MemoryBuffer &out);
ECLRTL_API void serializeInt4(signed int field, MemoryBuffer &out);
ECLRTL_API void serializeInt5(signed __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeInt6(signed __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeInt7(signed __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeInt8(signed __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeUInt1(unsigned char field, MemoryBuffer &out);
ECLRTL_API void serializeUInt2(unsigned short field, MemoryBuffer &out);
ECLRTL_API void serializeUInt3(unsigned int field, MemoryBuffer &out);
ECLRTL_API void serializeUInt4(unsigned int field, MemoryBuffer &out);
ECLRTL_API void serializeUInt5(unsigned __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeUInt6(unsigned __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeUInt7(unsigned __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeUInt8(unsigned __int64 field, MemoryBuffer &out);
ECLRTL_API void serializeReal4(float field, MemoryBuffer &out);
ECLRTL_API void serializeReal8(double field, MemoryBuffer &out);

//These maths functions can all have out of range arguments....
ECLRTL_API double rtlLog(double x);
ECLRTL_API double rtlLog10(double x);
ECLRTL_API double rtlSqrt(double x);
ECLRTL_API double rtlACos(double x);
ECLRTL_API double rtlASin(double x);

ECLRTL_API bool rtlIsValidReal(unsigned size, const void * data);
ECLRTL_API double rtlCreateRealNull();

ECLRTL_API unsigned rtlQStrLength(unsigned size);
ECLRTL_API unsigned rtlQStrSize(unsigned length);

ECLRTL_API void rtlStrToQStr(size32_t outlen, char * out, size32_t inlen, const void *in);
ECLRTL_API void rtlStrToQStrX(size32_t & outlen, char * & out, size32_t inlen, const void *in);
ECLRTL_API void rtlQStrToData(size32_t outlen, void * out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToDataX(size32_t & outlen, void * & out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToStr(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToQStr(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToStrX(size32_t & outlen, char * & out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToQStrX(size32_t & outlen, char * & out, size32_t inlen, const char *in);
ECLRTL_API void rtlQStrToVStr(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlStrToQStrNX(size32_t & outlen, char * & out, size32_t inlen, const void *in, size32_t logicalLength);
ECLRTL_API int rtlCompareQStrQStr(size32_t llen, const void * left, size32_t rlen, const void * right);
ECLRTL_API void rtlDecPushQStr(size32_t len, const void * data);
ECLRTL_API bool rtlQStrToBool(size32_t inlen, const char * in);

ECLRTL_API void rtlUnicodeToUnicode(size32_t outlen, UChar * out, size32_t inlen, UChar const *in);
ECLRTL_API void rtlUnicodeToVUnicode(size32_t outlen, UChar * out, size32_t inlen, UChar const *in);
ECLRTL_API void rtlVUnicodeToUnicode(size32_t outlen, UChar * out, UChar const *in);
ECLRTL_API void rtlVUnicodeToVUnicode(size32_t outlen, UChar * out, UChar const *in);
ECLRTL_API void rtlUnicodeToUnicodeX(unsigned & tlen, UChar * & tgt, unsigned slen, UChar const * src);
ECLRTL_API UChar * rtlUnicodeToVUnicodeX(unsigned slen, UChar const * src);
ECLRTL_API void rtlVUnicodeToUnicodeX(unsigned & tlen, UChar * & tgt, UChar const * src);
ECLRTL_API UChar * rtlVUnicodeToVUnicodeX(UChar const * src);
ECLRTL_API void rtlDecPushUnicode(size32_t len, UChar const * data);
ECLRTL_API unsigned rtlUnicodeStrlen(UChar const * str);
ECLRTL_API void rtlUnicodeStrcpy(UChar * tgt, UChar const * src);

ECLRTL_API void rtlStrToVUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in);

ECLRTL_API unsigned rtlUtf8Size(const void * data);
ECLRTL_API unsigned rtlUtf8Size(unsigned len, const void * data);
ECLRTL_API unsigned rtlUtf8Length(unsigned size, const void * data);
ECLRTL_API unsigned rtlUtf8Char(const void * data);
ECLRTL_API void rtlUtf8ToData(size32_t outlen, void * out, size32_t inlen, const char *in);
ECLRTL_API void rtlUtf8ToDataX(size32_t & outlen, void * & out, size32_t inlen, const char *in);
ECLRTL_API void rtlUtf8ToStr(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlUtf8ToStrX(size32_t & outlen, char * & out, size32_t inlen, const char *in);
ECLRTL_API char * rtlUtf8ToVStr(size32_t inlen, const char *in);
ECLRTL_API void rtlDataToUtf8(size32_t outlen, char * out, size32_t inlen, const void *in);
ECLRTL_API void rtlDataToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const void *in);
ECLRTL_API void rtlStrToUtf8(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlStrToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const char *in);
ECLRTL_API void rtlUtf8ToUtf8(size32_t outlen, char * out, size32_t inlen, const char *in);
ECLRTL_API void rtlUtf8ToUtf8X(size32_t & outlen, char * & out, size32_t inlen, const char *in);
ECLRTL_API int rtlCompareUtf8Utf8(size32_t llen, const char * left, size32_t rlen, const char * right, const char * locale);
ECLRTL_API int rtlCompareUtf8Utf8Strength(size32_t llen, const char * left, size32_t rlen, const char * right, const char * locale, unsigned strength);
ECLRTL_API void rtlDecPushUtf8(size32_t len, const void * data);
ECLRTL_API bool rtlUtf8ToBool(size32_t inlen, const char * in);
ECLRTL_API __int64 rtlUtf8ToInt(size32_t inlen, const char * in);
ECLRTL_API double rtlUtf8ToReal(size32_t inlen, const char * in);
ECLRTL_API void rtlCodepageToUtf8(unsigned outlen, char * out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlCodepageToUtf8X(unsigned & outlen, char * & out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlUtf8ToCodepage(unsigned outlen, char * out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlUtf8ToCodepageX(unsigned & outlen, char * & out, unsigned inlen, char const * in, char const * codepage);
ECLRTL_API void rtlUnicodeToUtf8X(unsigned & outlen, char * & out, unsigned inlen, const UChar * in);
ECLRTL_API void rtlUnicodeToUtf8(unsigned outlen, char * out, unsigned inlen, const UChar * in);
ECLRTL_API void rtlUtf8ToUnicodeX(unsigned & outlen, UChar * & out, unsigned inlen, char const * in);
ECLRTL_API void rtlUtf8ToUnicode(unsigned outlen, UChar * out, unsigned inlen, char const * in);
ECLRTL_API void rtlUtf8SubStrFTX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from, unsigned to);
ECLRTL_API void rtlUtf8SubStrFX(unsigned & tlen, char * & tgt, unsigned slen, char const * src, unsigned from);
ECLRTL_API void rtlUtf8SubStrFT(unsigned tlen, char * tgt, unsigned slen, char const * src, unsigned from, unsigned to);
ECLRTL_API void rtlUtf8ToLower(size32_t l, char * t, char const * locale);
ECLRTL_API void rtlConcatUtf8(unsigned & tlen, char * * tgt, ...);
ECLRTL_API unsigned rtlConcatUtf8ToUtf8(unsigned tlen, char * tgt, unsigned idx, unsigned slen, const char * src);
ECLRTL_API void rtlUtf8SpaceFill(unsigned tlne, char * tgt, unsigned idx);


ECLRTL_API ICompiledStrRegExpr * rtlCreateCompiledStrRegExpr(const char * regExpr, bool isCaseSensitive);
ECLRTL_API void rtlDestroyCompiledStrRegExpr(ICompiledStrRegExpr * compiled);
ECLRTL_API void rtlDestroyStrRegExprFindInstance(IStrRegExprFindInstance * compiled);


ECLRTL_API ICompiledUStrRegExpr * rtlCreateCompiledUStrRegExpr(const UChar * regExpr, bool isCaseSensitive);
ECLRTL_API void rtlDestroyCompiledUStrRegExpr(ICompiledUStrRegExpr * compiled);
ECLRTL_API void rtlDestroyUStrRegExprFindInstance(IUStrRegExprFindInstance * compiled);


ECLRTL_API void rtlCreateRange(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str, byte fill, byte pad);
ECLRTL_API void rtlCreateRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);
ECLRTL_API void rtlCreateRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);

ECLRTL_API void rtlCreateDataRangeLow(size32_t & outlen, void * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const void * str);
ECLRTL_API void rtlCreateDataRangeHigh(size32_t & outlen, void * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const void * str);
ECLRTL_API void rtlCreateStrRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);
ECLRTL_API void rtlCreateStrRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);
ECLRTL_API void rtlCreateQStrRangeLow(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);
ECLRTL_API void rtlCreateQStrRangeHigh(size32_t & outlen, char * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const char * str);
ECLRTL_API void rtlCreateUnicodeRangeLow(size32_t & outlen, UChar * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const UChar * str);
ECLRTL_API void rtlCreateUnicodeRangeHigh(size32_t & outlen, UChar * & out, unsigned fieldLen, unsigned compareLen, size32_t len, const UChar * str);

ECLRTL_API unsigned rtlCountRows(size32_t len, const void * data, IRecordSize * rs);
ECLRTL_API unsigned rtlCountToSize(unsigned count, const void * data, IRecordSize * rs);
ECLRTL_API void rtlSetToSetX(bool & outIsAll, size32_t & outLen, void * & outData, bool inIsAll, size32_t inLen, void * inData);
ECLRTL_API void rtlAppendSetX(bool & outIsAll, size32_t & outLen, void * & outData, bool leftIsAll, size32_t leftLen, void * leftData, bool rightIsAll, size32_t rightLen, void * rightData);

// rtlCodepageConvert uses a target buffer provided by the user (and returns the length actually used)
// rtlCodepageConvertX allocates the target buffer itself (user must free)
// If you can guess an upper bound on the target length, the rtlCodepageConvert will be more efficient.
// I believe, for example, that when converting from UTF-8 to any single byte encoding, the target cannot be longer than the source.
// If you can't guess an upper bound, rtlCodepageConvertX comes in two forms, with and without preflighting.
// Switching preflighting on optimizes for memory, the returned buffer is exactly the required size.
// Switching preflighting off optimizes for speed, the returned buffer is the maximum possible size (unicode size * max bytes per char).
// For acceptible names of codepages, see system/icu/include/codepages.txt
ECLRTL_API void * rtlOpenCodepageConverter(char const * sourceName, char const * targetName, bool & failed);
ECLRTL_API void rtlCloseCodepageConverter(void * converter);
ECLRTL_API unsigned rtlCodepageConvert(void * converter, unsigned targetLength, char * target, unsigned sourceLength, char const * source, bool & failed);
ECLRTL_API void rtlCodepageConvertX(void * converter, unsigned & targetLength, char * & target, unsigned sourceLength, char const * source, bool & failed, bool preflight);

ECLRTL_API void xmlDecodeStrX(size32_t & outLen, char * & out, size32_t inLen, const char * in);
ECLRTL_API void xmlDecodeUStrX(size32_t & outLen, UChar * & out, size32_t inLen, const UChar * in);
ECLRTL_API void xmlEncodeStrX(size32_t & outLen, char * & out, size32_t inLen, const char * in, unsigned flags);
ECLRTL_API void xmlEncodeUStrX(size32_t & outLen, UChar * & out, size32_t inLen, const UChar * in, unsigned flags);

ECLRTL_API bool rtlCsvStrToBool(size32_t l, const char * t);

ECLRTL_API void rtlHashMd5Init(size32_t sizestate, void * _state);
ECLRTL_API void rtlHashMd5Data(size32_t len, const void *buf, size32_t sizestate, void * _state);
ECLRTL_API void rtlHashMd5Finish(void * out, size32_t sizestate, void * _state);


ECLRTL_API void rtlExtractTag(size32_t & outLen, char * & out, const char * text, const char * tag, const char * rootTag);
ECLRTL_API void rtlExceptionExtract(size32_t & outLen, char * & out, const char * text, const char * tag);
ECLRTL_API void rtlExceptionExtract(size32_t & outLen, char * & out, IException * e, const char * tag);
ECLRTL_API void rtlAddExceptionTag(StringBuffer & errorText, const char * tag, const char * value);

ECLRTL_API int rtlQueryLocalFailCode(IException * e);
ECLRTL_API void rtlGetLocalFailMessage(size32_t & len, char * & text, IException * e, const char * tag);
ECLRTL_API void rtlFreeException(IException * e);

ECLRTL_API IAtom * rtlCreateFieldNameAtom(const char * name);

//Test functions:
ECLRTL_API void rtlTestGetPrimes(size32_t & len, void * & data);
ECLRTL_API void rtlTestFibList(bool & outAll, size32_t & outSize, void * & outData, bool inAll, size32_t inSize, const void * inData);
ECLRTL_API unsigned rtlTick();
ECLRTL_API unsigned rtlDelayReturn(unsigned value, unsigned sleepTime);

ECLRTL_API bool rtlGPF();

//-----------------------------------------------------------------------------

interface IEmbedFunctionContext : extends IInterface
{
    virtual void bindBooleanParam(const char *name, bool val) = 0;
    virtual void bindDataParam(const char *name, size32_t len, const void *val) = 0;
    virtual void bindRealParam(const char *name, double val) = 0;
    virtual void bindSignedParam(const char *name, __int64 val) = 0;
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val) = 0;
    virtual void bindStringParam(const char *name, size32_t len, const char *val) = 0;
    virtual void bindVStringParam(const char *name, const char *val) = 0;
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val) = 0;
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val) = 0;

    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData) = 0;

    virtual bool getBooleanResult() = 0;
    virtual void getDataResult(size32_t &len, void * &result) = 0;
    virtual double getRealResult() = 0;
    virtual __int64 getSignedResult() = 0;
    virtual unsigned __int64 getUnsignedResult() = 0;
    virtual void getStringResult(size32_t &len, char * &result) = 0;
    virtual void getUTF8Result(size32_t &chars, char * &result) = 0;
    virtual void getUnicodeResult(size32_t &chars, UChar * &result) = 0;
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize) = 0;

    virtual void importFunction(size32_t len, const char *function) = 0;
    virtual void compileEmbeddedScript(size32_t len, const char *script) = 0;
    virtual void callFunction() = 0;
};

interface IEmbedContext : extends IInterface
{
    virtual IEmbedFunctionContext *createFunctionContext(bool isImport, const char *options) = 0;
    // MORE - add syntax checked here!
};

#endif
