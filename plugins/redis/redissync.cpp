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

#include "platform.h"
#include "eclrtl.hpp"
#include "jstring.hpp"
#include "redissync.hpp"

namespace RedisPlugin {
static CriticalSection crit;

static Owned<SyncConnection> cachedConnection;

SyncConnection::SyncConnection(ICodeContext * ctx, const char * _options, unsigned __int64 _database) : Connection(ctx, _options)
{
   context = redisConnectWithTimeout(server->getIp(), server->getPort(), REDIS_TIMEOUT);
   assertConnection();
   redisSetTimeout(context, REDIS_TIMEOUT);
   selectDB(ctx, _database);
   init(ctx);
}
SyncConnection::SyncConnection(ICodeContext * ctx, RedisServer * _server, unsigned __int64 _database) : Connection(ctx, _server)
{
   context = redisConnectWithTimeout(server->getIp(), server->getPort(), REDIS_TIMEOUT);
   assertConnection();
   redisSetTimeout(context, REDIS_TIMEOUT);
   selectDB(ctx, _database);
   init(ctx);
}
SyncConnection * SyncConnection::createConnection(ICodeContext * ctx, const char * options, unsigned __int64 _database)
{
    CriticalBlock block(crit);
    if (!cachedConnection)
    {
        cachedConnection.setown(new SyncConnection(ctx, options, _database));
        return LINK(cachedConnection);
    }

    //Parse now to use if identical rather than parsing to compare and then again in SyncConnection constructor
    Owned<RedisServer> requestedServer = new RedisServer(ctx, options);
    if (cachedConnection->isSameConnection(ctx, requestedServer.get()))
    {
        cachedConnection->selectDB(ctx, _database);
        return LINK(cachedConnection);
    }

    cachedConnection.setown(new SyncConnection(ctx, requestedServer.getClear(), _database));
    return LINK(cachedConnection);
}
void SyncConnection::selectDB(ICodeContext * ctx, unsigned __int64 _database)
{
    if (database == _database)
        return;
    database = _database;
    VStringBuffer cmd("SELECT %llu", database);
    OwnedReply reply = createReply(redisCommand(context, cmd.str()));
    assertOnError(reply->query(), "'SELECT' request failed ");
}
void SyncConnection::logServerStats(ICodeContext * ctx)
{
    OwnedReply reply = createReply(redisCommand(context, "INFO ALL"));
    assertOnError(reply->query(), "'INFO ALL' request failed");
    StringBuffer stats("Redis Plugin : Server stats - ");
    stats.newline().append(reply->query()->str).newline();
    ctx->logString(stats.str());
    alreadyInitialized = true;
}
bool SyncConnection::logErrorOnFail(ICodeContext * ctx, const redisReply * reply, const char * _msg)
{
    if (!reply)
    {
        assertConnection();
        assertOnError(NULL, _msg);
    }
    else if (reply->type == REDIS_REPLY_STRING)
        return false;

    VStringBuffer msg("Redis Plugin: %s%s", _msg, reply->str);
    ctx->logString(msg.str());
    return true;
}
void SyncConnection::assertOnError(const redisReply * reply, const char * _msg)
{
    if (!reply)
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: %s%s", _msg, "no 'reply' nor connection error");
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        VStringBuffer msg("Redis Plugin: %s%s", _msg, reply->str);
        rtlFail(0, msg.str());
    }
}
void SyncConnection::assertConnection()
{
    if (!context)
        rtlFail(0, "Redis Plugin: 'redisConnect' failed - no error available.");
    else if (context->err)
    {
        VStringBuffer msg("Redis Plugin: Connection failed - %s for %s:%u", context->errstr, ip(), port());
        rtlFail(0, msg.str());
    }
}

void SyncConnection::clear(ICodeContext * ctx, unsigned when)
{
    //NOTE: flush is the actual cache flush/clear/delete and not an io buffer flush.
    OwnedReply reply = createReply(redisCommand(context, "FLUSHDB"));//NOTE: FLUSHDB deletes current database where as FLUSHALL deletes all dbs.
    //NOTE: documented as never failing, but in case
    assertOnError(reply->query(), "'Clear' request failed - ");
}
void SyncConnection::del(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = createReply(redisCommand(context, "DEL %b", key, strlen(key)));
    assertOnError(reply->query(), "'Del' request failed - ");
}
void SyncConnection::persist(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = createReply(redisCommand(context, "PERSIST %b", key, strlen(key)));
    assertOnError(reply->query(), "'Persist' request failed - ");
}
void SyncConnection::expire(ICodeContext * ctx, const char * key, unsigned _expire)
{
    OwnedReply reply = createReply(redisCommand(context, "DEL %b %u", key, strlen(key), _expire));
    assertOnError(reply->query(), "'Expire' request failed - ");
}
bool SyncConnection::exists(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = createReply(redisCommand(context, "EXISTS %b", key, strlen(key)));
    assertOnError(reply->query(), "'Exists' request failed - ");
    return (reply->query()->integer != 0);
}
unsigned __int64 SyncConnection::dbSize(ICodeContext * ctx)
{
    OwnedReply reply = createReply(redisCommand(context, "DBSIZE"));
    assertOnError(reply->query(), "'DBSIZE' request failed - ");
    return reply->query()->integer;
}
//-------------------------------------------SET-----------------------------------------
//--OUTER--
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, type value, unsigned __int64 database, unsigned expire)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, _options, database);
    master->set(ctx, key, value, expire);
}
//Set pointer types
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueLength, const type * value, unsigned __int64 database, unsigned expire)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, _options, database);
    master->set(ctx, key, valueLength, value, expire);
}
//--INNER--
template<class type> void SyncConnection::set(ICodeContext * ctx, const char * key, type value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining
    const char * msg = setFailMsg;

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);

    OwnedReply reply = createReply(redisCommand(context, cmd.str(), key, strlen(key), _value, sizeof(type)));
    assertOnError(reply->query(), msg);
}
template<class type> void SyncConnection::set(ICodeContext * ctx, const char * key, size32_t valueLength, const type * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    const char * msg = setFailMsg;

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);
    OwnedReply reply = createReply(redisCommand(context, cmd.str(), key, strlen(key), _value, (size_t)valueLength));
    assertOnError(reply->query(), msg);
}
//-------------------------------------------GET-----------------------------------------
//--OUTER--
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->get(ctx, key, returnValue);
}
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnLength, type * & returnValue, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->get(ctx, key, returnLength, returnValue);
}
void SyncRGetVoidPtrLenPair(ICodeContext * ctx, const char * options, const char * key, size_t & returnLength, void * & returnValue, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->getVoidPtrLenPair(ctx, key, returnLength, returnValue);
}
//--INNER--
template<class type> void SyncConnection::get(ICodeContext * ctx, const char * key, type & returnValue)
{
    OwnedReply reply = createReply(redisCommand(context, "GET %b", key, strlen(key)));

    StringBuffer keyMsg = getFailMsg;
    assertOnError(reply->query(), appendIfKeyNotFoundMsg(reply->query(), key, keyMsg));

    size_t returnSize = reply->query()->len;
    if (sizeof(type)!=returnSize)
    {
        VStringBuffer msg("RedisPlugin: ERROR - Requested type of different size (%uB) from that stored (%uB).", (unsigned)sizeof(type), (unsigned)returnSize);

        rtlFail(0, msg.str());
    }
    memcpy(&returnValue, reply->query()->str, returnSize);
}
template<class type> void SyncConnection::get(ICodeContext * ctx, const char * key, size_t & returnLength, type * & returnValue)
{
    OwnedReply reply = createReply(redisCommand(context, "GET %b", key, strlen(key)));

    StringBuffer keyMsg = getFailMsg;
    assertOnError(reply->query(), appendIfKeyNotFoundMsg(reply->query(), key, keyMsg));

    returnLength = reply->query()->len;
    size_t returnSize = returnLength*sizeof(type);

    returnValue = reinterpret_cast<type*>(cpy(reply->query()->str, returnSize));
}
void SyncConnection::getVoidPtrLenPair(ICodeContext * ctx, const char * key, size_t & returnLength, void * & returnValue)
{
    OwnedReply reply = createReply(redisCommand(context, "GET %b", key, strlen(key)));
    StringBuffer keyMsg = getFailMsg;
    assertOnError(reply->query(), appendIfKeyNotFoundMsg(reply->query(), key, keyMsg));

    returnLength = reply->query()->len;
    returnValue = reinterpret_cast<void*>(cpy(reply->query()->str, reply->query()->len));
}

//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL RClear(ICodeContext * ctx, const char * options, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->clear(ctx, 0);
}
ECL_REDIS_API bool ECL_REDIS_CALL RExist(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    return master->exists(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RDel(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->del(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RPersist(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->persist(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RExpire(ICodeContext * ctx, const char * options, const char * key, unsigned _expire, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    master->expire(ctx, key, _expire*unitExpire);
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize(ICodeContext * ctx, const char * options, unsigned __int64 database)
{
    Owned<SyncConnection> master = SyncConnection::createConnection(ctx, options, database);
    return master->dbSize(ctx);
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetStr(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, valueLength, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUChar(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const UChar * value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, (valueLength)*sizeof(UChar), value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetInt(ICodeContext * ctx, const char * options, const char * key, signed __int64 value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUInt(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetReal(ICodeContext * ctx, const char * options, const char * key, double value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetBool(ICodeContext * ctx, const char * options, const char * key, bool value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetData(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const void * value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, valueLength, value, database, expire);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUtf8(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire)
{
    SyncRSet(ctx, options, key, rtlUtf8Size(valueLength, value), value, database, expire);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL SyncRGetBool(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    bool value;
    SyncRGet(ctx, options, key, value, database);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncRGetDouble(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    double value;
    SyncRGet(ctx, options, key, value, database);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncRGetInt8(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    signed __int64 value;
    SyncRGet(ctx, options, key, value, database);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRGetUint8(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    unsigned __int64 value;
    SyncRGet(ctx, options, key, value, database);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t _returnLength;
    SyncRGet(ctx, options, key, _returnLength, returnValue, database);
    returnLength = static_cast<size32_t>(_returnLength);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * options, const char * key, unsigned __int64 database)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database);
    returnLength = static_cast<size32_t>(_returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t returnSize;
    SyncRGet(ctx, options, key, returnSize, returnValue, database);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t _returnLength;
    SyncRGetVoidPtrLenPair(ctx, options, key, _returnLength, returnValue, database);
    returnLength = static_cast<size32_t>(_returnLength);
}
}//close namespace
