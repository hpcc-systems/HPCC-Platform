/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef ECL_REDIS_SYNC_INCL
#define ECL_REDIS_SYNC_INCL

#include "redisplugin.hpp"

extern "C++"
{
namespace RedisPlugin {
    //--------------------------SET----------------------------------------
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetBool (ICodeContext * _ctx, const char * key, bool value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetInt  (ICodeContext * _ctx, const char * key, signed __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUInt (ICodeContext * _ctx, const char * key, unsigned __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetReal (ICodeContext * _ctx, const char * key, double value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUtf8 (ICodeContext * _ctx, const char * key, size32_t valueLength, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetStr  (ICodeContext * _ctx, const char * key, size32_t valueLength, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUChar(ICodeContext * _ctx, const char * key, size32_t valueLength, const UChar * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void ECL_REDIS_CALL SyncRSetData (ICodeContext * _ctx, const char * key, size32_t valueLength, const void * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    //--------------------------GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL SyncRGetBool  (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL SyncRGetInt8  (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRGetUint8 (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API double           ECL_REDIS_CALL SyncRGetDouble(ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncRGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncRGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncRGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncRGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);

    //--------------------------------AUXILLARIES---------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RExist  (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL RClear  (ICodeContext * _ctx, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL RDel    (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL RPersist(ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL RExpire (ICodeContext * _ctx, const char * key, const char * options, unsigned expire, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize (ICodeContext * _ctx, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);

    //--------------------------SET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL SyncLockRSetBool (ICodeContext * _ctx, const char * key, bool value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL SyncLockRSetInt  (ICodeContext * _ctx, const char * key, signed __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncLockRSetUInt (ICodeContext * _ctx, const char * key, unsigned __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API double           ECL_REDIS_CALL SyncLockRSetReal (ICodeContext * _ctx, const char * key, double value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRSetUtf8 (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRSetStr  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRSetUChar(ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * key, size32_t valueLength, const UChar * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRSetData (ICodeContext * _ctx, size32_t & returnLength, void * & returnValue, const char * key, size32_t valueLength, const void * value, const char * options, unsigned __int64 database, unsigned expire, const char * pswd, unsigned timeout);
    //--------------------------GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL SyncLockRGetBool  (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL SyncLockRGetInt8  (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncLockRGetUint8 (ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API double           ECL_REDIS_CALL SyncLockRGetDouble(ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);
    ECL_REDIS_API void             ECL_REDIS_CALL SyncLockRGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * pswd, unsigned timeout);

    ECL_REDIS_API bool ECL_REDIS_CALL SyncLockRMissThenLock(ICodeContext * _ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout);
}
}
#endif
