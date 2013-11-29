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

#ifndef STRINGLIB_INCL
#define STRINGLIB_INCL

#ifdef _WIN32
#define STRINGLIB_CALL _cdecl
#ifdef STRINGLIB_EXPORTS
#define STRINGLIB_API __declspec(dllexport)
#else
#define STRINGLIB_API __declspec(dllimport)
#endif
#else
#define STRINGLIB_CALL
#define STRINGLIB_API
#endif

#include "platform.h"
#include "hqlplugins.hpp"

extern "C" {

#ifdef STRINGLIB_EXPORTS
STRINGLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
STRINGLIB_API void setPluginContext(IPluginContext * _ctx);
#endif

STRINGLIB_API void STRINGLIB_CALL slStringFilterOut(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned hitLen, const char * hit);
STRINGLIB_API void STRINGLIB_CALL slStringFilter(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned hitLen, const char * hit);
STRINGLIB_API void STRINGLIB_CALL slStringSubsOut(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned hitLen, const char * hit, unsigned newCharLen, const char * newChar);
STRINGLIB_API void STRINGLIB_CALL slStringSubs(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned hitLen, const char * hit, unsigned newCharLen, const char * newChar);
STRINGLIB_API void STRINGLIB_CALL slStringRepad(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned tLen);
STRINGLIB_API void STRINGLIB_CALL slStringTranslate(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned hitLen, const char * hit, unsigned mappingLen, const char * mapping);
STRINGLIB_API unsigned STRINGLIB_CALL slStringFind(unsigned srcLen, const char * src, unsigned hitLen, const char * hit, unsigned instance);
STRINGLIB_API unsigned STRINGLIB_CALL slStringFind2(unsigned srcLen, const char * src, unsigned hitLen, const char * hit);
STRINGLIB_API unsigned STRINGLIB_CALL slStringFindCount(unsigned srcLen, const char * src, unsigned hitLen, const char * hit);
STRINGLIB_API void STRINGLIB_CALL slStringExtract(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned instance);
STRINGLIB_API void STRINGLIB_CALL slStringExtract50(char *tgt, unsigned srcLen, const char * src, unsigned instance);
STRINGLIB_API char * STRINGLIB_CALL slGetDateYYYYMMDD(void);
STRINGLIB_API void STRINGLIB_CALL slGetDateYYYYMMDD2(char * ret);
STRINGLIB_API char * STRINGLIB_CALL slGetBuildInfo(void);
STRINGLIB_API void STRINGLIB_CALL slGetBuildInfo100(char *tgt);
STRINGLIB_API void STRINGLIB_CALL slData2String(size32_t & __ret_len,char * & __ret_str,unsigned _len_y, const void * y);
STRINGLIB_API void STRINGLIB_CALL slString2Data(size32_t & __ret_len,void * & __ret_str,unsigned _len_src,const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToLowerCase(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToUpperCase(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToProperCase(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToCapitalCase(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToTitleCase(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToLowerCase80(char * tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringToUpperCase80(char * tgt, unsigned srcLen, const char * src);
STRINGLIB_API int STRINGLIB_CALL slStringCompareIgnoreCase (unsigned src1Len, const char * src1, unsigned src2Len, const char * src2);
STRINGLIB_API void STRINGLIB_CALL slStringReverse (unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringFindReplace(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned stokLen, const char * stok, unsigned rtokLen, const char * rtok);
STRINGLIB_API void STRINGLIB_CALL slStringFindReplace80(char * tgt, unsigned srcLen, const char * src, unsigned stokLen, const char * stok, unsigned rtokLen, const char * rtok);
STRINGLIB_API void STRINGLIB_CALL slStringCleanSpaces(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr);
STRINGLIB_API void STRINGLIB_CALL slStringCleanSpaces25(char *__ret_str,unsigned _len_instr,const char * instr);
STRINGLIB_API void STRINGLIB_CALL slStringCleanSpaces80(char *__ret_str,unsigned _len_instr,const char * instr);
STRINGLIB_API bool STRINGLIB_CALL slStringWildMatch(unsigned srcLen, const char * src, unsigned patLen, const char * pat, bool noCase);
STRINGLIB_API bool STRINGLIB_CALL slStringWildExactMatch(unsigned srcLen, const char * src, unsigned patLen, const char * pat, bool noCase);
STRINGLIB_API bool STRINGLIB_CALL slStringContains(unsigned srcLen, const char * src, unsigned sampleLen, const char * sample, bool noCase);
STRINGLIB_API void STRINGLIB_CALL slStringExtractMultiple(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned __int64 mask);
STRINGLIB_API unsigned STRINGLIB_CALL slEditDistanceV2(unsigned leftLen, const char * left, unsigned rightLen, const char * right);
STRINGLIB_API bool STRINGLIB_CALL slEditDistanceWithinRadiusV2(unsigned leftLen, const char * left, unsigned rightLen, const char * right, unsigned radius);
STRINGLIB_API void STRINGLIB_CALL slStringGetNthWord(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned n);
STRINGLIB_API unsigned STRINGLIB_CALL slStringWordCount(unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringExcludeLastWord(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
STRINGLIB_API void STRINGLIB_CALL slStringExcludeNthWord(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned n);
STRINGLIB_API unsigned STRINGLIB_CALL slCountWords(size32_t lenSrc, const char * src, size32_t lenSeparator, const char * separator, bool allowBlankItems);
STRINGLIB_API void STRINGLIB_CALL slSplitWords(bool & __isAllResult, size32_t & __lenResult, void * & __result, size32_t lenSrc, const char * src, size32_t lenSeparator, const char * separator, bool allowBlankItems);
STRINGLIB_API void STRINGLIB_CALL slCombineWords(size32_t & __lenResult, void * & __result, bool isAllSrc, size32_t lenSrc, const char * src, size32_t lenSeparator, const char * separator, bool allowBlankItems);
STRINGLIB_API unsigned STRINGLIB_CALL slStringToDate(size32_t lenS, const char * s, const char * fmtin);
STRINGLIB_API unsigned STRINGLIB_CALL slMatchDate(size32_t lenS, const char * s, bool isAllFormats, unsigned lenFormats, const void * _formats);
STRINGLIB_API void STRINGLIB_CALL slFormatDate(size32_t & __lenResult, char * & __result, unsigned date, const char * format);
STRINGLIB_API void STRINGLIB_CALL slStringRepeat(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src, unsigned n);
}

namespace nsStringlib {
extern STRINGLIB_API unsigned editDistanceV3(unsigned leftLen, const char * left, unsigned rightLen, const char * right, unsigned radius);
}
#endif
