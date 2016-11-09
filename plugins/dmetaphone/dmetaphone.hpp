#ifndef DMETAPHONE_INCL
#define DMETAPHONE_INCL

#ifdef _WIN32
#define DMETAPHONE_CALL _cdecl
#else
#define DMETAPHONE_CALL
#endif

#ifdef DMETAPHONE_EXPORTS
#define DMETAPHONE_API DECL_EXPORT
#else
#define DMETAPHONE_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"

extern "C" {

#ifdef DMETAPHONE_EXPORTS
DMETAPHONE_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
DMETAPHONE_API void setPluginContext(IPluginContext * _ctx);
#endif

DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr);
DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr);
DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth(size32_t & __ret_len,char * & __ret_str,unsigned _len_instr,const char * instr);
DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone1_20(char * __ret_str,unsigned _len_instr,const char * instr);
DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphone2_20(char * __ret_str,unsigned _len_instr,const char * instr);
DMETAPHONE_API void DMETAPHONE_CALL mpDMetaphoneBoth_40(char * __ret_str,unsigned _len_instr,const char * instr);
}

#endif
