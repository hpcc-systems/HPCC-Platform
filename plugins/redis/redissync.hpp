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

#ifndef ECL_REDIS_SYNC_INCL
#define ECL_REDIS_SYNC_INCL

#include "redisplugin.hpp"

namespace Sync
{
class Connection : public RedisPlugin::Connection
{
public :
    Connection(ICodeContext * ctx, const char * _options, unsigned __int64 database);
    ~Connection()
    {
        if (context)
            redisFree(context);
    }

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
    void clear(ICodeContext * ctx, unsigned when);
    unsigned __int64 dbSize(ICodeContext * ctx);
    bool exist(ICodeContext * ctx, const char * key);

protected :
    virtual void selectDB(ICodeContext * ctx);
    virtual void assertOnError(const redisReply * reply, const char * _msg);
    virtual void assertConnection();
    virtual void logServerStats(ICodeContext * ctx);
    virtual bool logErrorOnFail(ICodeContext * ctx, const redisReply * reply, const char * _msg);

protected :
    redisContext * context;
};
}//close namespace

extern "C++"
{
namespace Sync {
    //--------------------------SET----------------------------------------
    ECL_REDIS_API void ECL_REDIS_CALL RSetBool (ICodeContext * _ctx, const char * options, const char * key, bool value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetInt  (ICodeContext * _ctx, const char * options, const char * key, signed __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUInt (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetReal (ICodeContext * _ctx, const char * options, const char * key, double value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUtf8 (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetStr  (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetUChar(ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const UChar * value, unsigned __int64 database, unsigned expire);
    ECL_REDIS_API void ECL_REDIS_CALL RSetData (ICodeContext * _ctx, const char * options, const char * key, size32_t valueLength, const void * value, unsigned __int64 database, unsigned expire);
    //--------------------------GET----------------------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RGetBool  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API signed __int64   ECL_REDIS_CALL RGetInt8  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetUint8 (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API double           ECL_REDIS_CALL RGetDouble(ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUtf8  (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetStr   (ICodeContext * _ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetUChar (ICodeContext * _ctx, size32_t & returnLength, UChar * & returnValue, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RGetData  (ICodeContext * _ctx,size32_t & returnLength, void * & returnValue, const char * options, const char * key, unsigned __int64 database);

    //--------------------------------AUXILLARIES---------------------------
    ECL_REDIS_API bool             ECL_REDIS_CALL RExist  (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RClear  (ICodeContext * _ctx, const char * options, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RDel    (ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RPersist(ICodeContext * _ctx, const char * options, const char * key, unsigned __int64 database);
    ECL_REDIS_API void             ECL_REDIS_CALL RExpire (ICodeContext * _ctx, const char * options, const char * key, unsigned expire, unsigned __int64 database);
    ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize (ICodeContext * _ctx, const char * options, unsigned __int64 database);
}
}
#endif
