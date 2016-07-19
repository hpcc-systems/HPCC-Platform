/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

#ifndef ECL_MEMCACHED_INCL
#define ECL_MEMCACHED_INCL

#ifdef _WIN32
#define ECL_MEMCACHED_CALL _cdecl
#else
#define ECL_MEMCACHED_CALL
#endif

#ifdef ECL_MEMCACHED_EXPORTS
#define ECL_MEMCACHED_API DECL_EXPORT
#else
#define ECL_MEMCACHED_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"
#include "eclhelper.hpp"

extern "C"
{
    ECL_MEMCACHED_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
    ECL_MEMCACHED_API void setPluginContext(IPluginContext * _ctx);
}

//NB: LIBMEMCACHED_API already used by libmemcached
extern "C++"
{
namespace MemCachedPlugin {
    //--------------------------SET----------------------------------------
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, bool value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, signed __int64 value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, unsigned __int64 value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, double value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSetUtf8(ICodeContext * _ctx, const char * key, size32_t valueLength, const char * value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, size32_t valueLength, const char * value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet    (ICodeContext * _ctx, const char * key, size32_t valueLength, const UChar * value, const char * options, const char * partitionKey, unsigned __int64 expire);
    ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSetData(ICodeContext * _ctx, const char * key, size32_t valueLength, const void * value, const char * options, const char * partitionKey, unsigned __int64 expire);
    //--------------------------GET----------------------------------------
    ECL_MEMCACHED_API bool             ECL_MEMCACHED_CALL MGetBool  (ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API signed __int64   ECL_MEMCACHED_CALL MGetInt8  (ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API unsigned __int64 ECL_MEMCACHED_CALL MGetUint8 (ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API double           ECL_MEMCACHED_CALL MGetDouble(ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MGetUtf8  (ICodeContext * _ctx, size32_t & valueLength, char * & returnValue, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MGetStr   (ICodeContext * _ctx, size32_t & valueLength, char * & returnValue, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MGetUChar (ICodeContext * _ctx, size32_t & valueLength, UChar * & returnValue, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * key, const char * options, const char * partitionKey);
    //--------------------------------AUXILLARIES---------------------------
    ECL_MEMCACHED_API bool             ECL_MEMCACHED_CALL MExists (ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API const char *     ECL_MEMCACHED_CALL MKeyType(ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MClear  (ICodeContext * _ctx, const char * options);
    ECL_MEMCACHED_API void             ECL_MEMCACHED_CALL MDelete (ICodeContext * _ctx, const char * key, const char * options, const char * partitionKey);
}//close namespace
}
#endif
