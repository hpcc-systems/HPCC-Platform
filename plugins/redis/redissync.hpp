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

namespace RedisPlugin
{
class SyncConnection : public Connection
{
public :
    SyncConnection(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * pswd, unsigned __int64 _timeout);
    SyncConnection(ICodeContext * ctx, RedisServer * _server, unsigned __int64 database, const char * pswd);
    ~SyncConnection()
    {
        if (context)
            redisFree(context);
    }
    static SyncConnection * createConnection(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * pswd, unsigned __int64 _timeout);

    //set
    template <class type> void set(ICodeContext * ctx, const char * key, type value, unsigned expire);
    template <class type> void set(ICodeContext * ctx, const char * key, size32_t valueLength, const type * value, unsigned expire);
    //get
    template <class type> void get(ICodeContext * ctx, const char * key, type & value);
    template <class type> void get(ICodeContext * ctx, const char * key, size_t & valueLength, type * & value);
    void getVoidPtrLenPair(ICodeContext * ctx, const char * key, size_t & valueLength, void * & value);
    void persist(ICodeContext * ctx, const char * key);
    void expire(ICodeContext * ctx, const char * key, unsigned _expire);
    void del(ICodeContext * ctx, const char * key);
    void clear(ICodeContext * ctx);
    unsigned __int64 dbSize(ICodeContext * ctx);
    bool exists(ICodeContext * ctx, const char * key);

protected :
    void connect(ICodeContext * ctx, unsigned __int64 _database, const char * pswd);
    void selectDB(ICodeContext * ctx, unsigned __int64 _database);
    void authenticate(ICodeContext * ctx, const char * pswd);

    virtual void updateTimeout(unsigned __int64 _timeout);
    virtual void assertOnError(const redisReply * reply, const char * _msg);
    virtual void assertConnection();
    virtual void logServerStats(ICodeContext * ctx);

protected :
    redisContext * context;
};
}//close namespace

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
}
}
#endif
