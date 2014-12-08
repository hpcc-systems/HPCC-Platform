/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#ifndef ECL_REDIS_ASYNC_INCL
#define ECL_REDIS_ASYNC_INCL

#include "redisplugin.hpp"

extern "C++"
{
namespace Async {
    //------------------------ASYNC--GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RGetBool  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL RGetInt8  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetUint8 (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API double           ECL_REDIS_CALL RGetDouble(ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * options, const char * key, unsigned __int64 database);
    //------------------------ASYNC--SET----------------------------------------
    ECL_REDIS_API void ECL_REDIS_CALL RSetBool (ICodeContext * _ctx, const char * options, const char * key, bool value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetInt  (ICodeContext * _ctx, const char * options, const char * key, signed __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUInt (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetReal (ICodeContext * _ctx, const char * options, const char * key, double value, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUtf8 (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetStr  (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUChar(ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const UChar * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetData (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const void * value, unsigned __int64 database, unsigned expire);
}

namespace Lock {
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetLockObject(ICodeContext * ctx, const char * options, const char * key);
    ECL_REDIS_API bool ECL_REDIS_CALL RMissThenLock(ICodeContext * ctx, unsigned __int64 keyPtr, unsigned __int64 database);
    //------------------------LOCKING--GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RGetBool  (ICodeContext * _ctx, unsigned __int64 keyPtr);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL RGetInt8  (ICodeContext * _ctx, unsigned __int64 keyPtr);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetUint8 (ICodeContext * _ctx, unsigned __int64 keyPtr);
    ECL_REDIS_API double           ECL_REDIS_CALL RGetDouble(ICodeContext * _ctx, unsigned __int64 keyPtr);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 keyPtr);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 keyPtr);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, unsigned __int64 keyPtr);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, unsigned __int64 keyPtr);
    //------------------------LOCKING--SET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RSetBool (ICodeContext * _ctx, unsigned __int64 keyPtr, bool value, unsigned expire);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL RSetInt  (ICodeContext * _ctx, unsigned __int64 keyPtr, signed __int64 value, unsigned expire);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RSetUInt (ICodeContext * _ctx, unsigned __int64 keyPtr, unsigned __int64 value, unsigned expire);
    ECL_REDIS_API double           ECL_REDIS_CALL RSetReal (ICodeContext * _ctx, unsigned __int64 keyPtr, double value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL RSetUtf8 (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 keyPtr, size32_t valueLength, const char * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL RSetStr  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 keyPtr, size32_t valueLength, const char * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL RSetUChar(ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, unsigned __int64 keyPtr, size32_t valueLength, const UChar * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL RSetData (ICodeContext * _ctx, size32_t & returnLength, void * & returnValue, unsigned __int64 keyPtr, size32_t valueLength, const void * value, unsigned expire);
}
}
#endif
