#include "platform.h"
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include "dmetaphone.hpp"
#include "metaphone.h"

#define DMETAPHONE_VERSION "DMETAPHONE 1.1.05"

static const char * compatibleVersions[] = {
    "DMETAPHONE 1.1.05 [0e64c86ec1d5771d4ce0abe488a98a2a]",
    "DMETAPHONE 1.1.05",
    NULL };

DMETAPHONE_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = DMETAPHONE_VERSION;
    pb->moduleName = "lib_metaphone";
    pb->ECL = NULL;  // Definition is in lib_metaphone.ecllib
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "Metaphone library";
    return true;
}

namespace nsDmetaphone {

IPluginContext * parentCtx = NULL;

}

using namespace nsDmetaphone;

DMETAPHONE_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }


DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph2);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph2);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    __ret_len = strlen((char*) metaph) + strlen((char*) metaph2);
    __ret_str = (char *) CTXMALLOC(parentCtx, __ret_len+1);
    strcpy(__ret_str, (char*) metaph);
    strcat(__ret_str, (char*) metaph2);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1_20(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 20);
    size32_t metaph_len = strlen((char*) metaph);
    strncpy(__ret_str, (char*) metaph, (metaph_len > 20)?20:metaph_len);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2_20(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 20);
    size32_t metaph2_len = strlen((char*) metaph2);
    strncpy(__ret_str, (char*) metaph2, (metaph2_len > 20)?20:metaph2_len);
}

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth_40(char * __ret_str,unsigned _len_instr,const char * instr)
{
    cString metaph;
    cString metaph2;
    MString ms;
    ms.Set(instr, _len_instr);
    ms.DoubleMetaphone(metaph, metaph2);
    memset(__ret_str, ' ', 40);
    size32_t metaph_len = strlen((char*) metaph);
    strncpy(__ret_str, (char*) metaph, (metaph_len > 20)?20:metaph_len);
    size32_t metaph2_len = strlen((char*) metaph2);
    strncpy(__ret_str+metaph_len, (char*) metaph2, (metaph2_len > 20)?20:metaph2_len);
}
