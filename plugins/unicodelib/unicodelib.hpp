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

#ifndef UNICODELIB_INCL
#define UNICODELIB_INCL

#ifdef _WIN32
#define UNICODELIB_CALL _cdecl
#else
#define UNICODELIB_CALL
#endif

#ifdef UNICODELIB_EXPORTS
#define UNICODELIB_API DECL_EXPORT
#else
#define UNICODELIB_API DECL_IMPORT
#endif

#ifndef CHEAP_UCHAR_DEF
#ifndef U_OVERRIDE_CXX_ALLOCATION
#define U_OVERRIDE_CXX_ALLOCATION 0 // Enabling this forces all allocation of ICU objects to ICU's heap, but is incompatible with jmemleak
#endif //U_OVERRIDE_CXX_ALLOCATION
#include "unicode/utf.h"
#endif //CHEAP_UCHAR_DEF

#include "hqlplugins.hpp"

/* For CompareAtStrength calls, strength is an integer between 1 and 5. Loosely, the levels are defined as follows.
   - Strength 1 ignores accents and cases, and differentiates only between different letters.
   - Strength 2 ignores cases, but differentiates between accents.
   - Strength 3 differentiates between accents and cases, but ignores e.g. differences between Hiragana and Katakana
   - Strength 4 differentiates between accents and cases and e.g. Hiragana/Katakana, but ignores e.g. Hebrew cantellation marks
   - Strength 5 differentiates between all strings whose NFD (canonically decomposed) forms are non-identical

   For FindAtStrength, the matching relationship is not symmetrical, e.g. when searching for an accented letter, we get hits from
   unaccented versions only at strength 1, but when searching for an unaccented letter, we get hits from accented versions at strengths 1--4.

   The default (as used by the built-in ECL unicode comparison) is strength 3.
   We take CompareIgnoreCase to mean compare at strength 2.

   See http://www-124.ibm.com/icu/userguide/Collate_Concepts.html#Comparison_Levels for more information, or ecl/regress/ul.hql for examples.
*/

extern "C" {
UNICODELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
UNICODELIB_API void setPluginContext(IPluginContext * _ctx);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeFilterOut(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeFilter(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeSubsOut(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned newCharLen, UChar const * newChar);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeSubs(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned newCharLen, UChar const * newChar);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeRepad(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned tLen);
UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeFind(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance);
UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleFind(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance, char const * localename);
UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleFindAtStrength(unsigned srcLen, UChar const * src, unsigned hitLen, UChar const * hit, unsigned instance, char const * localename, char strength);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeExtract(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned instance);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeExtract50(UChar *tgt, unsigned srcLen, UChar const * src, unsigned instance);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToLowerCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToUpperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToProperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToLowerCase80(UChar * tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToUpperCase80(UChar * tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeToProperCase80(UChar * tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToLowerCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToUpperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToProperCase(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToLowerCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToUpperCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleToProperCase80(UChar * tgt, unsigned srcLen, UChar const * src, char const * localename);
UNICODELIB_API int UNICODELIB_CALL ulUnicodeCompareIgnoreCase(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2);
UNICODELIB_API int UNICODELIB_CALL ulUnicodeCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char strength);
UNICODELIB_API int UNICODELIB_CALL ulUnicodeLocaleCompareIgnoreCase(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char const * localename);
UNICODELIB_API int UNICODELIB_CALL ulUnicodeLocaleCompareAtStrength(unsigned src1Len, UChar const * src1, unsigned src2Len, UChar const * src2, char const * localename, char strength);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeReverse(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeFindReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeFindReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindAtStrengthReplace(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename, char strength);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleFindAtStrengthReplace80(UChar * tgt, unsigned srcLen, UChar const * src, unsigned stokLen, UChar const * stok, unsigned rtokLen, UChar const * rtok, char const * localename, char strength);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanAccents(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces(unsigned & tgtLen, UChar * & tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces25(UChar * tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeCleanSpaces80(UChar * tgt, unsigned srcLen, UChar const * src);
UNICODELIB_API bool UNICODELIB_CALL ulUnicodeWildMatch(unsigned srcLen, UChar const * src, unsigned patLen, UChar const * pat, bool noCase);
UNICODELIB_API bool UNICODELIB_CALL ulUnicodeContains(unsigned srcLen, UChar const * src, unsigned patLen, UChar const * pat, bool noCase);
UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleEditDistance(unsigned leftLen, UChar const * left, unsigned rightLen, UChar const * right,char const * localename);
UNICODELIB_API bool UNICODELIB_CALL ulUnicodeLocaleEditDistanceWithinRadius(unsigned leftLen, UChar const * left, unsigned rightLen, UChar const * right, unsigned radius,char const * localename);
UNICODELIB_API unsigned UNICODELIB_CALL ulUnicodeLocaleWordCount(unsigned textLen, UChar const * text,char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleGetNthWord(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned n, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleExcludeNthWord(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned n, char const * localename);
UNICODELIB_API void UNICODELIB_CALL ulUnicodeLocaleTranslate(unsigned & tgtLen, UChar * & tgt, unsigned textLen, UChar const * text, unsigned searLen, UChar const * sear, unsigned replLen, UChar * repl, char const * localename);
}

#endif
