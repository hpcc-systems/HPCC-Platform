/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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
#include "jthread.hpp"
#include "eclrtl.hpp"
#include "jstring.hpp"
#include "redis.hpp"
#include "hiredis/hiredis.h"

#define REDIS_VERSION "redis plugin 1.0.0"
ECL_REDIS_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = REDIS_VERSION;
    pb->moduleName = "lib_redis";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for the C API hiredis\n";
    return true;
}

namespace RedisPlugin {

class Connection;
static const char * REDIS_LOCK_PREFIX = "redis_ecl_lock";
static __thread Connection * cachedConnection;
static __thread ThreadTermFunc threadHookChain;
static __thread bool threadHooked;

static void * allocateAndCopy(const void * src, size_t size)
{
    return memcpy(rtlMalloc(size), src, size);
}
static StringBuffer & appendExpire(StringBuffer & buffer, unsigned expire)
{
    if (expire > 0)
        buffer.append(" PX ").append(expire);
    return buffer;
}
class Reply : public CInterface
{
public :
    inline Reply() : reply(NULL) { };
    inline Reply(void * _reply) : reply((redisReply*)_reply) { }
    inline Reply(redisReply * _reply) : reply(_reply) { }
    inline ~Reply()
    {
        if (reply)
            freeReplyObject(reply);
    }

    static Reply * createReply(void * _reply) { return new Reply(_reply); }
    inline const redisReply * query() const { return reply; }
    void setClear(redisReply * _reply)
    {
        if (reply)
            freeReplyObject(reply);
        reply = _reply;
    }

private :
    redisReply * reply;
};
typedef Owned<RedisPlugin::Reply> OwnedReply;

class TimeoutHandler
{
public :
    TimeoutHandler(unsigned _timeout) : timeout(_timeout), t0(msTick()) { }
    inline void reset(unsigned _timeout) { timeout = _timeout; t0 = msTick(); }
    unsigned timeLeft() const { unsigned timeLeft = timeout - (msTick() - t0); return timeLeft > 0 ? timeLeft : 0; }

private :
    unsigned timeout;
    unsigned t0;
};

class Connection : public CInterface
{
public :
    Connection(ICodeContext * ctx, const char * options, int database, const char * password, unsigned _timeout);
    Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, unsigned _serverIpPortPasswordHash, int _database, const char * password, unsigned _timeout);
    ~Connection()
    {
        if (context)
            redisFree(context);
    }
    static Connection * createConnection(ICodeContext * ctx, const char * options, int database, const char * password, unsigned _timeout);

    //set
    template <class type> void set(ICodeContext * ctx, const char * key, type value, unsigned expire);
    template <class type> void set(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire);
    //get
    template <class type> void get(ICodeContext * ctx, const char * key, type & value);
    template <class type> void get(ICodeContext * ctx, const char * key, size_t & valueSize, type * & value);

    //-------------------------------LOCKING------------------------------------------------
    void lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, unsigned expire);
    void lockGet(ICodeContext * ctx, const char * key, size_t & valueSize, char * & value, const char * password, unsigned expire);
    void unlock(ICodeContext * ctx, const char * key);
    //--------------------------------------------------------------------------------------

    void persist(ICodeContext * ctx, const char * key);
    void expire(ICodeContext * ctx, const char * key, unsigned _expire);
    void del(ICodeContext * ctx, const char * key);
    void clear(ICodeContext * ctx);
    unsigned __int64 dbSize(ICodeContext * ctx);
    bool exists(ICodeContext * ctx, const char * key);

protected :
    void redisSetTimeout();
    void redisConnectWithTimeout();
    unsigned timeLeft();
    void parseOptions(ICodeContext * ctx, const char * _options);
    void connect(ICodeContext * ctx, int _database, const char * password);
    void selectDB(ICodeContext * ctx, int _database);
    void resetContextErr();
    void readReply(Reply * reply);
    void readReplyAndAssert(Reply * reply, const char * msg);
    void readReplyAndAssertWithKey(Reply * reply, const char * msg, const char * key);
    void assertKey(const redisReply * reply, const char * key);
    void assertAuthorization(const redisReply * reply);
    void assertOnError(const redisReply * reply, const char * _msg);
    void assertOnCommandError(const redisReply * reply, const char * cmd);
    void assertOnCommandErrorWithDatabase(const redisReply * reply, const char * cmd);
    void assertOnCommandErrorWithKey(const redisReply * reply, const char * cmd, const char * key);
    void assertConnection();
    void updateTimeout(unsigned _timeout);
    void * redisCommand(redisContext * context, const char * format, ...);
    static unsigned hashServerIpPortPassword(ICodeContext * ctx, const char * _options, const char * password);
    bool isSameConnection(ICodeContext * ctx, const char * _options, const char * password) const;

    //-------------------------------LOCKING------------------------------------------------
    void handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire);
    void handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password, unsigned expire);
    void encodeChannel(StringBuffer & channel, const char * key) const;
    bool noScript(const redisReply * reply) const;
    bool lock(ICodeContext * ctx, const char * key, const char * channel, unsigned expire);
    //--------------------------------------------------------------------------------------

protected :
    redisContext * context;
    StringAttr options;
    StringAttr ip;
    unsigned serverIpPortPasswordHash;
    int port;
    int database; //NOTE: redis stores the maximum number of dbs as an 'int'.
    TimeoutHandler timeout;
};

//The following class is here to ensure destruction of the cachedConnection within the main thread
//as this is not handled by the thread hook mechanism.
static class MainThreadCachedConnection
{
public :
    MainThreadCachedConnection() { }
    ~MainThreadCachedConnection()
    {
        if (cachedConnection)
        {
            cachedConnection->Release();
            cachedConnection = NULL;
        }
    }
} mainThread;

static void releaseContext()
{
    if (cachedConnection)
    {
        cachedConnection->Release();
        cachedConnection = NULL;
    }
    if (threadHookChain)
    {
        (*threadHookChain)();
        threadHookChain = NULL;
    }
    threadHooked = false;
}
Connection::Connection(ICodeContext * ctx, const char * _options, int _database, const char * password, unsigned _timeout)
  : database(0), timeout(_timeout), port(0), serverIpPortPasswordHash(hashServerIpPortPassword(ctx, _options, password))
{
    options.set(_options, strlen(_options));
    parseOptions(ctx, _options);
    connect(ctx, _database, password);
}
Connection::Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, unsigned _serverIpPortPasswordHash, int _database, const char * password, unsigned _timeout)
  : database(0), timeout(_timeout), serverIpPortPasswordHash(_serverIpPortPasswordHash), port(_port)
{
    options.set(_options, strlen(_options));
    ip.set(_ip, strlen(_ip));
    connect(ctx, _database, password);
}
unsigned Connection::timeLeft()
{
    unsigned timeLeft = timeout.timeLeft();
    if (timeLeft == 0)
        rtlFail(0, "RedisPlugin: function timed out before sending command to redis.");
    return timeLeft;
}
void Connection::redisSetTimeout()
{
    unsigned _timeLeft = timeLeft();
    struct timeval to = { _timeLeft/1000, (_timeLeft%1000)*1000 };
    assertex(context);
    if (::redisSetTimeout(context, to) != REDIS_OK)
    {
        if (context->err)
        {
            VStringBuffer msg("RedisPlugin: failed to set timeout - %s", context->errstr);
            rtlFail(0, msg.str());
        }
        else
            rtlFail(0, "RedisPlugin: failed to set timeout - no message available");
    }
}
void Connection::redisConnectWithTimeout()
{
    unsigned _timeLeft = timeLeft();
    struct timeval to = { _timeLeft/1000, (_timeLeft%1000)*1000 };
    context = ::redisConnectWithTimeout(ip.str(), port, to);
    assertConnection();
}
void Connection::updateTimeout(unsigned _timeout)
{
    timeout.reset(_timeout);
    redisSetTimeout();// NOTE: possibly not not needed.
}
void Connection::connect(ICodeContext * ctx, int _database, const char * password)
{
    redisConnectWithTimeout();

    //The following is the dissemination of the two methods authenticate(ctx, password) & selectDB(ctx, _database)
    //such that they may be pipelined to save an extra round trip to the server and back.
    if (password && *password)
        redisAppendCommand(context, "AUTH %b", password, strlen(password));

    if (database != _database)
    {
        VStringBuffer cmd("SELECT %d", _database);
        redisAppendCommand(context, cmd.str());
    }

    //Now read replies.
    OwnedReply reply = new Reply();
    if (password && *password)
        readReplyAndAssert(reply, "server authentication failed");

    if (database != _database)
    {
        readReplyAndAssert(reply, "request to SELECT database failed");
        database = _database;
    }
}
void * Connection::redisCommand(redisContext * context, const char * format, ...)
{
    //Copied from https://github.com/redis/hiredis/blob/master/hiredis.c ~line:1008 void * redisCommand(redisContext * context, const char * format, ...)
    //with redisSetTimeout(); added.
    va_list parameters;
    void * reply = NULL;
    va_start(parameters, format);
    redisSetTimeout();
    reply = ::redisvCommand(context, format, parameters); //MORE: could wrap and link 'reply' here rather than by caller.
    va_end(parameters);
    return reply;
}
bool Connection::isSameConnection(ICodeContext * ctx, const char * _options, const char * password) const
{
    return (hashServerIpPortPassword(ctx, _options, password) == serverIpPortPasswordHash);
}
unsigned Connection::hashServerIpPortPassword(ICodeContext * ctx, const char * _options, const char * password)
{
    return hashc((const unsigned char*)_options, strlen(_options), hashc((const unsigned char*)password, strlen(password), 0));
}
void Connection::parseOptions(ICodeContext * ctx, const char * _options)
{
    StringArray optionStrings;
    optionStrings.appendList(_options, " ");
    ForEachItemIn(idx, optionStrings)
    {
        const char *opt = optionStrings.item(idx);
        if (strncmp(opt, "--SERVER=", 9) == 0)
        {
            opt += 9;
            StringArray splitPort;
            splitPort.appendList(opt, ":");
            if (splitPort.ordinality()==2)
            {
                ip.set(splitPort.item(0));
                port = atoi(splitPort.item(1));
            }
        }
        else
        {
            VStringBuffer err("RedisPlugin: unsupported option string %s", opt);
            rtlFail(0, err.str());
        }
    }
    if (ip.isEmpty())
    {
        ip.set("localhost");
        port = 6379;
        if (ctx)
        {
            VStringBuffer msg("Redis Plugin: WARNING - using default server (%s:%d)", ip.str(), port);
            ctx->logString(msg.str());
        }
    }
}
void Connection::resetContextErr()
{
    if (context)
        context->err = REDIS_OK;
}
void Connection::readReply(Reply * reply)
{
    redisReply * nakedReply = NULL;
    redisSetTimeout();
    redisGetReply(context, (void**)&nakedReply);
    assertex(reply);
    reply->setClear(nakedReply);
}
void Connection::readReplyAndAssert(Reply * reply, const char * msg)
{
    readReply(reply);
    assertex(reply);
    assertOnError(reply->query(), msg);
}
void Connection::readReplyAndAssertWithKey(Reply * reply, const char * msg, const char * key)
{
    readReply(reply);
    assertex(reply);
    assertOnCommandErrorWithKey(reply->query(), msg, key);
}
Connection * Connection::createConnection(ICodeContext * ctx, const char * options, int _database, const char * password, unsigned _timeout)
{
    if (!cachedConnection)
    {
        cachedConnection = new Connection(ctx, options, _database, password, _timeout);
        if (!threadHooked)
        {
            threadHookChain = addThreadTermFunc(releaseContext);
            threadHooked = true;
        }
        return LINK(cachedConnection);
    }

    if (cachedConnection->isSameConnection(ctx, options, password))
    {
        //MORE: should perhaps check that the connection has not expired (think hiredis REDIS_KEEPALIVE_INTERVAL is defaulted to 15s).
        cachedConnection->resetContextErr();//reset the context err to allow reuse when an error previously occurred.
        cachedConnection->assertConnection();
        cachedConnection->updateTimeout(_timeout);
        cachedConnection->selectDB(ctx, _database);
        return LINK(cachedConnection);
    }

    cachedConnection->Release();
    cachedConnection = NULL;
    cachedConnection = new Connection(ctx, options, _database, password, _timeout);
    return LINK(cachedConnection);
}
void Connection::selectDB(ICodeContext * ctx, int _database)
{
    if (database == _database)
        return;
    database = _database;
    VStringBuffer cmd("SELECT %d", database);
    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str()));
    assertOnCommandError(reply->query(), "SELECT");
}
void Connection::assertOnError(const redisReply * reply, const char * _msg)
{
    if (!reply)//MORE: should this be assertex(reply) instead?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: %s - %s", _msg, "neither 'reply' nor connection error available");
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        VStringBuffer msg("Redis Plugin: %s - %s", _msg, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertOnCommandErrorWithKey(const redisReply * reply, const char * cmd, const char * key)
{
    if (!reply)//MORE: should this be assertex(reply) instead?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: ERROR - %s '%s' on database %d failed with neither 'reply' nor connection error available", cmd, key, database);
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        VStringBuffer msg("Redis Plugin: ERROR - %s '%s' on database %d failed : %s", cmd, key, database, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertOnCommandErrorWithDatabase(const redisReply * reply, const char * cmd)
{
    if (!reply)//assertex(reply)?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: ERROR - %s on database %d failed with neither 'reply' nor connection error available", cmd, database);
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        VStringBuffer msg("Redis Plugin: ERROR - %s on database %d failed : %s", cmd, database, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertOnCommandError(const redisReply * reply, const char * cmd)
{
    if (!reply)//assertex(reply)?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: ERROR - %s failed with neither 'reply' nor connection error available", cmd);
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        VStringBuffer msg("Redis Plugin: ERROR - %s failed : %s", cmd, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertAuthorization(const redisReply * reply)
{
    if (reply && reply->str && ( strncmp(reply->str, "NOAUTH", 6) == 0 || strncmp(reply->str, "ERR operation not permitted", 27) == 0 ))
    {
        VStringBuffer msg("Redis Plugin: server authentication failed - %s", reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertKey(const redisReply * reply, const char * key)
{
    if (reply && reply->type == REDIS_REPLY_NIL)
    {
        VStringBuffer msg("Redis Plugin: ERROR - the requested key '%s' does not exist on database %d", key, database);
        rtlFail(0, msg.str());
    }
}
void Connection::assertConnection()
{
    if (!context)
        rtlFail(0, "Redis Plugin: 'redisConnect' failed - no error available.");
    else if (context->err)
    {
        VStringBuffer msg("Redis Plugin: Connection failed - %s for %s:%u", context->errstr, ip.str(),  port);
        rtlFail(0, msg.str());
    }
}
void Connection::clear(ICodeContext * ctx)
{
    //NOTE: flush is the actual cache flush/clear/delete and not an io buffer flush.
    OwnedReply reply = Reply::createReply(redisCommand(context, "FLUSHDB"));//NOTE: FLUSHDB deletes current database where as FLUSHALL deletes all dbs.
    //NOTE: documented as never failing, but in case
    assertOnCommandErrorWithDatabase(reply->query(), "FlushDB");
}
void Connection::del(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "DEL %b", key, strlen(key)));
    assertOnCommandErrorWithKey(reply->query(), "Del", key);
}
void Connection::persist(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "PERSIST %b", key, strlen(key)));
    assertOnCommandErrorWithKey(reply->query(), "Persist", key);
}
void Connection::expire(ICodeContext * ctx, const char * key, unsigned _expire)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "PEXPIRE %b %u", key, strlen(key), _expire));
    assertOnCommandErrorWithKey(reply->query(), "Expire", key);
}
bool Connection::exists(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "EXISTS %b", key, strlen(key)));
    assertOnCommandErrorWithKey(reply->query(), "Exists", key);
    return (reply->query()->integer != 0);
}
unsigned __int64 Connection::dbSize(ICodeContext * ctx)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "DBSIZE"));
    assertOnCommandErrorWithDatabase(reply->query(), "DBSIZE");
    return reply->query()->integer;
}
//-------------------------------------------SET-----------------------------------------
//--OUTER--
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, type value, int database, unsigned expire, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->set(ctx, key, value, expire);
}
//Set pointer types
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const type * value, int database, unsigned expire, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->set(ctx, key, valueSize, value, expire);
}
//--INNER--
template<class type> void Connection::set(ICodeContext * ctx, const char * key, type value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);

    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str(), key, strlen(key), _value, sizeof(type)));
    assertOnCommandErrorWithKey(reply->query(), "SET", key);
}
template<class type> void Connection::set(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);
    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str(), key, strlen(key), _value, (size_t)valueSize));
    assertOnCommandErrorWithKey(reply->query(), "SET", key);
}
//-------------------------------------------GET-----------------------------------------
//--OUTER--
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, int database, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->get(ctx, key, returnValue);
}
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, type * & returnValue, int database, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->get(ctx, key, returnSize, returnValue);
}
//--INNER--
template<class type> void Connection::get(ICodeContext * ctx, const char * key, type & returnValue)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "GET %b", key, strlen(key)));

    assertOnError(reply->query(), "GET");
    assertKey(reply->query(), key);

    size_t returnSize = reply->query()->len;
    if (sizeof(type)!=returnSize)
    {
        VStringBuffer msg("RedisPlugin: ERROR - Requested type of different size (%uB) from that stored (%uB).", (unsigned)sizeof(type), (unsigned)returnSize);
        rtlFail(0, msg.str());
    }
    memcpy(&returnValue, reply->query()->str, returnSize);
}
template<class type> void Connection::get(ICodeContext * ctx, const char * key, size_t & returnSize, type * & returnValue)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "GET %b", key, strlen(key)));

    assertOnError(reply->query(), "GET");
    assertKey(reply->query(), key);

    returnSize = reply->query()->len;
    returnValue = reinterpret_cast<type*>(allocateAndCopy(reply->query()->str, returnSize));
}
//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL RClear(ICodeContext * ctx, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->clear(ctx);
}
ECL_REDIS_API bool ECL_REDIS_CALL RExist(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    return master->exists(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RDel(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->del(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RPersist(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->persist(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RExpire(ICodeContext * ctx, const char * key, const char * options, int database, unsigned _expire, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->expire(ctx, key, _expire);
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize(ICodeContext * ctx, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    return master->dbSize(ctx);
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetStr(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUChar(ICodeContext * ctx, const char * key, size32_t valueLength, const UChar * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, (valueLength)*sizeof(UChar), value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetInt(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUInt(ICodeContext * ctx, const char * key, unsigned __int64 value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetReal(ICodeContext * ctx, const char * key, double value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetBool(ICodeContext * ctx, const char * key, bool value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetData(ICodeContext * ctx, const char * key, size32_t valueSize, const void * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUtf8(ICodeContext * ctx, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncRSet(ctx, options, key, rtlUtf8Size(valueLength, value), value, database, expire, password, timeout);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL SyncRGetBool(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    bool value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncRGetDouble(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    double value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncRGetInt8(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    signed __int64 value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRGetUint8(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    unsigned __int64 value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    size_t returnSize;
    SyncRGet(ctx, options, key, returnSize, returnValue, database, password, timeout);
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    size_t returnSize;
    SyncRGet(ctx, options, key, returnSize, returnValue, database, password, timeout);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetData(ICodeContext * ctx, size32_t & returnSize, void * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
//----------------------------------LOCK------------------------------------------
//-----------------------------------SET-----------------------------------------
//Set pointer types
void SyncLockRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const char * value, int database, unsigned expire, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->lockSet(ctx, key, valueSize, value, expire);
}
//--INNER--
void Connection::lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    handleLockOnSet(ctx, key, _value, (size_t)valueSize, expire);
}
//-------------------------------------------GET-----------------------------------------
//--OUTER--
void SyncLockRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, char * & returnValue, int database, unsigned expire, const char * password, unsigned _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->lockGet(ctx, key, returnSize, returnValue, password, expire);
}
//--INNER--
void Connection::lockGet(ICodeContext * ctx, const char * key, size_t & returnSize, char * & returnValue, const char * password, unsigned expire)
{
    MemoryAttr retVal;
    handleLockOnGet(ctx, key, &retVal, password, expire);
    returnSize = retVal.length();
    returnValue = reinterpret_cast<char*>(retVal.detach());
}
//---------------------------------------------------------------------------------------
void Connection::encodeChannel(StringBuffer & channel, const char * key) const
{
    channel.append(REDIS_LOCK_PREFIX).append("_").append(key).append("_").append(database);
}
bool Connection::lock(ICodeContext * ctx, const char * key, const char * channel, unsigned expire)
{
    StringBuffer cmd("SET %b %b NX PX ");
    cmd.append(expire);

    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str(), key, strlen(key), channel, strlen(channel)));
    assertOnError(reply->query(), cmd.append(" of the key '").append(key).append("' failed"));

    return (reply->query()->type == REDIS_REPLY_STATUS && strcmp(reply->query()->str, "OK") == 0);
}
void Connection::unlock(ICodeContext * ctx, const char * key)
{
    //WATCH key, if altered between WATCH and EXEC abort all commands inbetween
    redisAppendCommand(context, "WATCH %b", key, strlen(key));
    redisAppendCommand(context, "GET %b", key, strlen(key));

    //Read replies
    OwnedReply reply = new Reply();
    readReplyAndAssertWithKey(reply.get(), "manual unlock", key);//WATCH reply
    readReplyAndAssertWithKey(reply.get(), "manual unlock", key);//GET reply

    //check if locked
    if (strncmp(reply->query()->str, REDIS_LOCK_PREFIX, strlen(REDIS_LOCK_PREFIX)) == 0)
    {
        //MULTI - all commands between MULTI and EXEC are considered an atomic transaction on the server
        redisAppendCommand(context, "MULTI");//MULTI
        redisAppendCommand(context, "DEL %b", key, strlen(key));//DEL
        redisAppendCommand(context, "EXEC");//EXEC
#if(0)//Quick draw! You have 10s to manually send (via redis-cli) "set testlock foobar". The second myRedis.Exists('testlock') in redislockingtest.ecl should now return TRUE.
        sleep(10);
#endif
        readReplyAndAssertWithKey(reply.get(), "manual unlock", key);//MULTI reply
        readReplyAndAssertWithKey(reply.get(), "manual unlock", key);//DEL reply
        readReplyAndAssertWithKey(reply.get(), "manual unlock", key);//EXEC reply
    }
    //If the above is aborted, let the lock expire.
}
void Connection::handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password, unsigned expire)
{
    //NOTE: This routine can only return an empty string under one condition, that which indicates to the caller that the key was successfully locked.

    StringBuffer channel;
    encodeChannel(channel, key);

    //Query key and set lock if non existent
    if (lock(ctx, key, channel.str(), expire))
        return;

#if(0)//Test empty string handling by deleting the lock/value, and thus GET returns REDIS_REPLY_NIL as the reply type and an empty string.
    {
    OwnedReply pubReply = Reply::createReply(redisCommand(context, "DEL %b", key, strlen(key)));
    assertOnError(pubReply->query(), "del fail");
    }
#endif

    //SUB before GET
    //Requires separate connection from GET so that the replies are not mangled. This could be averted
    Owned<Connection> subConnection = new Connection(ctx, options.str(), ip.str(), port, serverIpPortPasswordHash, database, password, timeLeft());
    OwnedReply reply = Reply::createReply(redisCommand(subConnection->context, "SUBSCRIBE %b", channel.str(), (size_t)channel.length()));
    assertOnCommandErrorWithKey(reply->query(), "GET", key);
    if (reply->query()->type == REDIS_REPLY_ARRAY && strcmp("subscribe", reply->query()->element[0]->str) != 0 )
    {
        VStringBuffer msg("Redis Plugin: ERROR - GET '%s' on database %d failed : failed to register SUB", key, database);
        rtlFail(0, msg.str());
    }

#if(0)//Test publish before GET.
    {
    OwnedReply pubReply = Reply::createReply(redisCommand(context, "PUBLISH %b %b", channel.str(), (size_t)channel.length(), "foo", (size_t)3));
    assertOnError(pubReply->query(), "pub fail");
    }
#endif

    //Now GET
    reply->setClear((redisReply*)redisCommand(context, "GET %b", key, strlen(key)));
    assertOnCommandErrorWithKey(reply->query(), "GET", key);

#if(0)//Test publish after GET.
    {
    OwnedReply pubReply = Reply::createReply(redisCommand(context, "PUBLISH %b %b", channel.str(), (size_t)channel.length(), "foo", (size_t)3));
    assertOnError(pubReply->query(), "pub fail");
    }
#endif

    //Only return an actual value, i.e. neither the lock value nor an empty string. The latter is unlikely since we know that lock()
    //failed, indicating that the key existed. If this is an actual value, it is however, possible for it to have been DELeted in the interim.
    if (reply->query()->type != REDIS_REPLY_NIL && reply->query()->str && strncmp(reply->query()->str, REDIS_LOCK_PREFIX, strlen(REDIS_LOCK_PREFIX)) != 0)
    {
        retVal->set(reply->query()->len, reply->query()->str);
        return;
    }
    else
    {
        //Check that the lock was set by this plugin and thus that we subscribed to the expected channel.
        if (reply->query()->str && strcmp(reply->query()->str, channel.str()) !=0 )
        {
            VStringBuffer msg("Redis Plugin: ERROR - the key '%s', on database %d, is locked with a channel ('%s') different to that subscribed to (%s).", key, database, reply->query()->str, channel.str());
            rtlFail(0, msg.str());
            //MORE: In theory, it is possible to recover at this stage by subscribing to the channel that the key was actually locked with.
            //However, we may have missed the massage publication already or by then, but could SUB again in case we haven't.
            //More importantly and furthermore, the publication (in SetAndPublish<type>) will only publish to the channel encoded by
            //this plugin, rather than the string retrieved as the lock value (the value of the locked key).
        }
#if(0)//Added to allow for manual pub testing via redis-cli
        struct timeval to = { 10, 0 };//10secs
        ::redisSetTimeout(subConnection->context, to);
#endif
        //Locked so SUBSCRIBE
        redisReply * nakedReply = NULL;
        subConnection->redisSetTimeout();
        bool err = redisGetReply(subConnection->context, (void**)&nakedReply);
        reply->setClear(nakedReply);
        if (err != REDIS_OK)
            rtlFail(0, "RedisPlugin: ERROR - GET timed out.");
        assertOnCommandErrorWithKey(nakedReply, "GET", key);
        if (nakedReply->type == REDIS_REPLY_ARRAY && strcmp("message", nakedReply->element[0]->str) == 0)
        {
            //We are about to return a value, to conform with other Get<type> functions, fail if the key did not exist.
            //Since the value is sent via a published message, there is no direct reply struct so assume that an empty
            //string is equivalent to a non-existent key.
            //More importantly, it is paramount that this routine only return an empty string under one condition, that
            //which indicates to the caller that the key was successfully locked.
            //NOTE: it is possible for an empty message to have been PUBLISHed.
            if (nakedReply->element[2]->len > 0)
            {
                retVal->set(nakedReply->element[2]->len, nakedReply->element[2]->str);//return the published value rather than another (WATCHed) GET.
                return;
            }
            VStringBuffer msg("Redis Plugin: ERROR - the requested key '%s' does not exist on database %d", key, database);
            rtlFail(0, msg.str());
        }
    }
    throwUnexpected();
}
void Connection::handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire)
{
    //Due to locking logic surfacing into ECL, any locking.set (such as this is) assumes that they own the lock and therefore go ahead and set regardless.
    StringBuffer channel;
    encodeChannel(channel, key);

    if (size > 29)//c.f. 1st note below.
    {
        OwnedReply replyContainer = new Reply();
        if (expire == 0)
        {
            const char * luaScriptSHA1 = "2a4a976d9bbd806756b2c7fc1e2bc2cb905e68c3"; //NOTE: update this if luaScript is updated!
            replyContainer->setClear((redisReply*)redisCommand(context, "EVALSHA %b %d %b %b %b", luaScriptSHA1, (size_t)40, 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size));
            if (noScript(replyContainer->query()))
            {
                const char * luaScript = "redis.call('SET', KEYS[1], ARGV[2]) redis.call('PUBLISH', ARGV[1], ARGV[2]) return";//NOTE: MUST update luaScriptSHA1 if luaScript is updated!
                replyContainer->setClear((redisReply*)redisCommand(context, "EVAL %b %d %b %b %b", luaScript, strlen(luaScript), 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size));
            }
        }
        else
        {
            const char * luaScriptWithExpireSHA1 = "6f6bc88ccea7c6853ccc395eaa7abd8cb91fb2d8"; //NOTE: update this if luaScriptWithExpire is updated!
            replyContainer->setClear((redisReply*)redisCommand(context, "EVALSHA %b %d %b %b %b %d", luaScriptWithExpireSHA1, (size_t)40, 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size, expire));
            if (noScript(replyContainer->query()))
            {
                const char * luaScriptWithExpire = "redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) redis.call('PUBLISH', ARGV[1], ARGV[2]) return";//NOTE: MUST update luaScriptWithExpireSHA1 if luaScriptWithExpire is updated!
                replyContainer->setClear((redisReply*)redisCommand(context, "EVAL %b %d %b %b %b %d", luaScriptWithExpire, strlen(luaScriptWithExpire), 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size, expire));
            }
        }
        assertOnCommandErrorWithKey(replyContainer->query(), "SET", key);
    }
    else
    {
        StringBuffer cmd("SET %b %b");
        RedisPlugin::appendExpire(cmd, expire);
        redisAppendCommand(context, "MULTI");
        redisAppendCommand(context, cmd.str(), key, strlen(key), value, size);//SET
        redisAppendCommand(context, "PUBLISH %b %b", channel.str(), (size_t)channel.length(), value, size);//PUB
        redisAppendCommand(context, "EXEC");

        //Now read and assert replies
        OwnedReply reply = new Reply();
        readReplyAndAssertWithKey(reply, "SET", key);//MULTI reply
        readReplyAndAssertWithKey(reply, "SET", key);//SET reply
        readReplyAndAssertWithKey(reply, "PUB for the key", key);//PUB reply
        readReplyAndAssertWithKey(reply, "SET", key);//EXEC reply
    }

    //NOTE: When setting and publishing the data with a pipelined MULTI-SET-PUB-EXEC, the data is sent twice, once with the SET and again with the PUBLISH.
    //To prevent this, send the data to the server only once with a server-side lua script that then sets and publishes the data from the server.
    //However, there is a transmission overhead for this method that may still be larger than sending the data twice if it is small enough.
    //multi-set-pub-exec (via strings) has a transmission length of - "MULTI SET" + key + value + "PUBLISH" + channel + value  = 5 + 3 + key + 7 + value + channel + value + 4
    //The lua script (assuming the script already exists on the server) a length of - "EVALSHA" + digest + "1" + key + channel + value = 7 + 40 + 1 + key + channel + value
    //Therefore, they have same length when: 19 + value = 48 => value = 29.

    //NOTE: Pipelining the above commands may not be the expected behaviour, instead only PUBLISH upon a successful SET. Doing both regardless, does however ensure
    //(assuming only the SET fails) that any subscribers do in fact get their requested key-value even if the SET fails. This may not be expected behaviour
    //as it is now possible for the key-value to NOT actually exist in the cache though it was retrieved via a redis plugin get function. This is documented in the README.
    //Further more, it is possible that the locked value and thus the channel stored within the key is not that expected, i.e. computed via encodeChannel() (e.g.
    //if set by a non-conforming external client/process). It is however, possible to account for this via using a GETSET instead of just the SET. This returns the old
    //value stored, this can then be checked if it is a lock (i.e. has at least the "redis_key_lock prefix"), if it doesn't, PUB on the channel from encodeChannel(),
    //otherwise PUB on the value retrieved from GETSET or possibly only if it at least has the prefix "redis_key_lock".
    //This would however, prevent the two commands from being pipelined, as the GETSET would need to return before publishing. It would also mean sending the data twice.
}
bool Connection::noScript(const redisReply * reply) const
{
    return (reply && reply->type == REDIS_REPLY_ERROR && strncmp(reply->str, "NOSCRIPT", 8) == 0);
}
//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    SyncLockRSet(ctx, options, key, valueLength, value, database, expire, password, timeout);
    returnLength = valueLength;
    returnValue = (char*)allocateAndCopy(value, valueLength);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue, const char * key, size32_t valueLength, const UChar * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    unsigned valueSize = (valueLength)*sizeof(UChar);
    SyncLockRSet(ctx, options, key, valueSize, (char*)value, database, expire, password, timeout);
    returnLength= valueLength;
    returnValue = (UChar*)allocateAndCopy(value, valueSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout)
{
    unsigned valueSize = rtlUtf8Size(valueLength, value);
    SyncLockRSet(ctx, options, key, valueSize, value, database, expire, password, timeout);
    returnLength = valueLength;
    returnValue = (char*)allocateAndCopy(value, valueSize);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, expire, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire)
{
    size_t returnSize;
    char  * _returnValue;
    SyncLockRGet(ctx, options, key, returnSize, _returnValue, database, expire, password, timeout);
    returnValue = (UChar*)_returnValue;
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire)
{
    size_t returnSize;
    SyncLockRGet(ctx, options, key, returnSize, returnValue, database, expire, password, timeout);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRUnlock(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->unlock(ctx, key);
}
}//close namespace
