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
    //------------------------ASYNC--GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL AsncRGetBool  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL AsncRGetInt8  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL AsncRGetUint8 (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API double           ECL_REDIS_CALL AsncRGetDouble(ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL AsncRGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL AsncRGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL AsncRGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL AsncRGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * options, const char * key, unsigned __int64 database);
    //------------------------ASYNC--SET----------------------------------------
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetBool (ICodeContext * _ctx, const char * options, const char * key, bool value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetInt  (ICodeContext * _ctx, const char * options, const char * key, signed __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetUInt (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetReal (ICodeContext * _ctx, const char * options, const char * key, double value, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetUtf8 (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetStr  (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetUChar(ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const UChar * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL AsncRSetData (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const void * value, unsigned __int64 database, unsigned expire);

    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetLockObject(ICodeContext * ctx, const char * options, const char * key);
    ECL_REDIS_API bool ECL_REDIS_CALL RMissThenLock(ICodeContext * ctx, unsigned __int64 lockObject, unsigned __int64 database);
    //------------------------LOCKING--GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL LockingRGetBool  (ICodeContext * _ctx, unsigned __int64 lockObject);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL LockingRGetInt8  (ICodeContext * _ctx, unsigned __int64 lockObject);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL LockingRGetUint8 (ICodeContext * _ctx, unsigned __int64 lockObject);
    ECL_REDIS_API double           ECL_REDIS_CALL LockingRGetDouble(ICodeContext * _ctx, unsigned __int64 lockObject);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 lockObject);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 lockObject);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, unsigned __int64 lockObject);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, unsigned __int64 lockObject);
    //------------------------LOCKING--SET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL LockingRSetBool (ICodeContext * _ctx, unsigned __int64 lockObject, bool value, unsigned expire);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL LockingRSetInt  (ICodeContext * _ctx, unsigned __int64 lockObject, signed __int64 value, unsigned expire);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL LockingRSetUInt (ICodeContext * _ctx, unsigned __int64 lockObject, unsigned __int64 value, unsigned expire);
    ECL_REDIS_API double           ECL_REDIS_CALL LockingRSetReal (ICodeContext * _ctx, unsigned __int64 lockObject, double value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRSetUtf8 (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 lockObject, size32_t valueLength, const char * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRSetStr  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 lockObject, size32_t valueLength, const char * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRSetUChar(ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, unsigned __int64 lockObject, size32_t valueLength, const UChar * value, unsigned expire);
    ECL_REDIS_API void             ECL_REDIS_CALL LockingRSetData (ICodeContext * _ctx, size32_t & returnLength, void * & returnValue, unsigned __int64 lockObject, size32_t valueLength, const void * value, unsigned expire);
}
#endif
