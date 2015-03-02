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

StringBuffer & appendExpire(StringBuffer & buffer, unsigned expire)
{
    if (expire > 0)
        buffer.append(" EX ").append(expire/1000000);
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

class Connection : public CInterface
{
public :
    Connection(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * password, unsigned __int64 _timeout);
    Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, unsigned _serverIpPortPasswordHash, unsigned __int64 _database, const char * password, unsigned __int64 _timeout);
    ~Connection()
    {
        if (context)
            redisFree(context);
    }
    static Connection * createConnection(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * password, unsigned __int64 _timeout);
    void parseOptions(ICodeContext * ctx, const char * _options);
    //set
    template <class type> void set(ICodeContext * ctx, const char * key, type value, unsigned expire);
    template <class type> void set(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire);
    //get
    template <class type> void get(ICodeContext * ctx, const char * key, type & value);
    template <class type> void get(ICodeContext * ctx, const char * key, size_t & valueSize, type * & value);

    //-------------------------------LOCKING------------------------------------------------
    //set
    template <class type> void lockSet(ICodeContext * ctx, const char * key, type value, unsigned expire);
    template <class type> void lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire);
    //get
    void lockGet(ICodeContext * ctx, const char * key, MemoryAttr * retValue, const char * password);
    template <class type> void lockGet(ICodeContext * ctx, const char * key, size_t & valueSize, type * & value, const char * password);
    bool missThenLock(ICodeContext * ctx, const char * key);
    void encodeChannel(StringBuffer & channel, const char * key) const;
    bool lock(ICodeContext * ctx, const char * key, const char * channel);
    void unlock(ICodeContext * ctx, const char * key);
    void handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password);
    void handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire);
    //--------------------------------------------------------------------------------------

    void persist(ICodeContext * ctx, const char * key);
    void expire(ICodeContext * ctx, const char * key, unsigned _expire);
    void del(ICodeContext * ctx, const char * key);
    void clear(ICodeContext * ctx);
    unsigned __int64 dbSize(ICodeContext * ctx);
    bool exists(ICodeContext * ctx, const char * key);

protected :
    void connect(ICodeContext * ctx, unsigned __int64 _database, const char * password);
    void selectDB(ICodeContext * ctx, unsigned __int64 _database);
    void authenticate(ICodeContext * ctx, const char * password);
    void resetContextErr();
    void readReply(Reply * reply);
    void readReplyAndAssert(Reply * reply, const char * msg, const char * key);
    void assertKey(const redisReply * reply, const char * key);
    void assertOnError(const redisReply * reply, const char * _msg);
    void assertOnCommandError(const redisReply * reply, const char * cmd);
    void assertOnCommandErrorWithDatabase(const redisReply * reply, const char * cmd);
    void assertOnCommandErrorWithKey(const redisReply * reply, const char * cmd, const char * key);
    void assertConnection();
    void updateTimeout(unsigned __int64 _timeout);
    void logServerStats(ICodeContext * ctx);

    void * allocateAndCopy(const char * src, size_t size);
    void init(ICodeContext * ctx);
    bool isSameConnection(ICodeContext * ctx, const char * password) const;

protected :
    StringAttr options;
    StringAttr ip;
    unsigned serverIpPortPasswordHash;
    int port;
    redisContext * context;
    unsigned __int64 timeout;
    unsigned __int64 database;
    bool alreadyInitialized;
};

//The following class is here to ensure destruction of the cachedConnection within the main thread
//as this is not handled by the thread hook mechanism.
static class mainThreadCachedConnection
{
public :
    mainThreadCachedConnection() { }
    ~mainThreadCachedConnection()
    {
        if (cachedConnection)
            cachedConnection->Release();
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
}
Connection::Connection(ICodeContext * ctx, const char * _options, unsigned __int64 _database, const char * password, unsigned __int64 _timeout)
  : alreadyInitialized(false), database(0), timeout(_timeout)
{
    serverIpPortPasswordHash = hashc((const unsigned char*)password, strlen(password), 0);
    serverIpPortPasswordHash = hashc((const unsigned char*)_options, strlen(_options), serverIpPortPasswordHash);
    options.set(_options, strlen(_options));
    parseOptions(ctx, _options);
    connect(ctx, _database, password);
}
Connection::Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, unsigned _serverIpPortPasswordHash, unsigned __int64 _database, const char * password, unsigned __int64 _timeout)
  : alreadyInitialized(false), database(0), timeout(_timeout), serverIpPortPasswordHash(_serverIpPortPasswordHash), port(_port)
{
    options.set(_options, strlen(_options));
    ip.set(_ip, strlen(_ip));
    connect(ctx, _database, password);
}
void Connection::connect(ICodeContext * ctx, unsigned __int64 _database, const char * password)
{
    struct timeval to = { timeout/1000000, timeout%1000000 };
    context = redisConnectWithTimeout(ip.str(), port, to);
    redisSetTimeout(context, to);
    assertConnection();
    authenticate(ctx, password);
    selectDB(ctx, _database);
    init(ctx);
}
bool Connection::isSameConnection(ICodeContext * ctx, const char * password) const
{
    unsigned hash = hashc((const unsigned char*)options.str(), options.length(), hashc((const unsigned char*)password, strlen(password), 0));
    return (serverIpPortPasswordHash == hash);
}
void * Connection::allocateAndCopy(const char * src, size_t size)
{
    void * value = rtlMalloc(size);
    return memcpy(value, src, size);
}
void Connection::init(ICodeContext * ctx)
{
    logServerStats(ctx);
    alreadyInitialized = true;
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
    return;
}
void Connection::authenticate(ICodeContext * ctx, const char * password)
{
    if (password && *password)
    {
        OwnedReply reply = Reply::createReply(redisCommand(context, "AUTH %b", password, strlen(password)));
        assertOnError(reply->query(), "server authentication failed");
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
    redisGetReply(context, (void**)&nakedReply);
    assertex(reply);
    reply->setClear(nakedReply);
}
void Connection::readReplyAndAssert(Reply * reply, const char * msg, const char * key)
{
    readReply(reply);
    assertex(reply);
    assertOnCommandErrorWithKey(reply->query(), msg, key);
}
Connection * Connection::createConnection(ICodeContext * ctx, const char * options, unsigned __int64 _database, const char * password, unsigned __int64 _timeout)
{
    if (!cachedConnection)
    {
        cachedConnection = new Connection(ctx, options, _database, password, _timeout);
        threadHookChain = addThreadTermFunc(releaseContext);
        return LINK(cachedConnection);
    }

    if (cachedConnection->isSameConnection(ctx, password))
    {
        //MORE: need to check that the connection has not expired (think hiredis REDIS_KEEPALIVE_INTERVAL is defaulted to 15s).
        //At present updateTimeout calls assertConnection.
        cachedConnection->resetContextErr();//reset the context err to allow reuse when an error previously occurred.
        cachedConnection->updateTimeout(_timeout);
        cachedConnection->selectDB(ctx, _database);
        return LINK(cachedConnection);
    }

    cachedConnection->Release();
    cachedConnection = new Connection(ctx, options, _database, password, _timeout);
    return LINK(cachedConnection);
}
void Connection::selectDB(ICodeContext * ctx, unsigned __int64 _database)
{
    if (database == _database)
        return;
    database = _database;
    VStringBuffer cmd("SELECT %" I64F "u", database);
    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str()));
    assertOnCommandError(reply->query(), "SELECT");
}
void Connection::updateTimeout(unsigned __int64 _timeout)
{
    if (timeout == _timeout)
        return;
    assertConnection();
    timeout = _timeout;
    struct timeval to = { timeout/1000000, timeout%1000000 };
    assertex(context);
    if (redisSetTimeout(context, to) != REDIS_OK)
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
void Connection::logServerStats(ICodeContext * ctx)
{
    OwnedReply reply = Reply::createReply(redisCommand(context, "INFO"));
    assertOnError(reply->query(), "'INFO' request failed");
    StringBuffer stats("Redis Plugin : Server stats - ");
    stats.newline().append(reply->query()->str).newline();
    ctx->logString(stats.str());
}
void Connection::assertOnError(const redisReply * reply, const char * _msg)
{
    if (!reply)//assertex(reply)?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: %s - %s", _msg, "neither 'reply' nor connection error available");
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        if (strncmp(reply->str, "NOAUTH", 6) == 0)
        {
            VStringBuffer msg("Redis Plugin: server authentication failed - %s", reply->str);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin: %s - %s", _msg, reply->str);
            rtlFail(0, msg.str());
        }
    }
}
void Connection::assertOnCommandErrorWithKey(const redisReply * reply, const char * cmd, const char * key)
{
    if (!reply)//assertex(reply)?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: ERROR - %s '%s' on database %" I64F "u failed with neither 'reply' nor connection error available", cmd, key, database);
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        if (strncmp(reply->str, "NOAUTH", 6) == 0)
        {
            VStringBuffer msg("Redis Plugin: server authentication failed - %s", reply->str);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin: ERROR - %s '%s' on database %" I64F "u failed : %s", cmd, key, database, reply->str);
            rtlFail(0, msg.str());
        }
    }
}
void Connection::assertOnCommandErrorWithDatabase(const redisReply * reply, const char * cmd)
{
    if (!reply)//assertex(reply)?
    {
        //There should always be a context error if no reply error
        assertConnection();
        VStringBuffer msg("Redis Plugin: ERROR - %s on database %" I64F "u failed with neither 'reply' nor connection error available", cmd, database);
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        if (strncmp(reply->str, "NOAUTH", 6) == 0)
        {
            VStringBuffer msg("Redis Plugin: server authentication failed - %s", reply->str);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin: ERROR - %s on database %" I64F "u failed : %s", cmd, database, reply->str);
            rtlFail(0, msg.str());
        }
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
        if (strncmp(reply->str, "NOAUTH", 6) == 0)
        {
            VStringBuffer msg("Redis Plugin: server authentication failed - %s", reply->str);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin: ERROR - %s failed : %s", cmd, reply->str);
            rtlFail(0, msg.str());
        }
    }
}
void Connection::assertKey(const redisReply * reply, const char * key)
{
    if (reply && reply->type == REDIS_REPLY_NIL)
    {
        VStringBuffer msg("Redis Plugin: ERROR - the requested key '%s' does not exist on database %" I64F "u", key, database);
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
    OwnedReply reply = Reply::createReply(redisCommand(context, "EXPIRE %b %u", key, strlen(key), _expire/1000000));
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
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, type value, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->set(ctx, key, value, expire);
}
//Set pointer types
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const type * value, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 _timeout)
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
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, unsigned __int64 database, const char * password, unsigned __int64 _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->get(ctx, key, returnValue);
}
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, type * & returnValue, unsigned __int64 database, const char * password, unsigned __int64 _timeout)
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
ECL_REDIS_API void ECL_REDIS_CALL RClear(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->clear(ctx);
}
ECL_REDIS_API bool ECL_REDIS_CALL RExist(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    return master->exists(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RDel(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->del(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RPersist(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->persist(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL RExpire(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, unsigned _expire, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->expire(ctx, key, _expire);
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize(ICodeContext * ctx, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    return master->dbSize(ctx);
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetStr(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUChar(ICodeContext * ctx, const char * key, size32_t valueSize, const UChar * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, (valueSize)*sizeof(UChar), value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetInt(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUInt(ICodeContext * ctx, const char * key, unsigned __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetReal(ICodeContext * ctx, const char * key, double value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetBool(ICodeContext * ctx, const char * key, bool value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetData(ICodeContext * ctx, const char * key, size32_t valueSize, const void * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUtf8(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncRSet(ctx, options, key, rtlUtf8Size(valueSize, value), value, database, expire, password, timeout);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL SyncRGetBool(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    bool value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncRGetDouble(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    double value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncRGetInt8(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    signed __int64 value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRGetUint8(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    unsigned __int64 value;
    SyncRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUChar(ICodeContext * ctx, size32_t & returnSize, UChar * & returnValue,  const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUtf8(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(rtlUtf8Length(_returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetData(ICodeContext * ctx, size32_t & returnSize, void * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
//----------------------------------LOCK------------------------------------------
//-----------------------------------SET-----------------------------------------
//--OUTER--
template<class type> void SyncLockRSet(ICodeContext * ctx, const char * _options, const char * key, type value, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->lockSet(ctx, key, value, expire);
}
//Set pointer types
template<class type> void SyncLockRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const type * value, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, _options, database, password, _timeout);
    master->lockSet(ctx, key, valueSize, value, expire);
}
//--INNER--
template<class type> void Connection::lockSet(ICodeContext * ctx, const char * key, type value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining
    handleLockOnSet(ctx, key, _value, sizeof(type), expire);
}
template<class type> void Connection::lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    handleLockOnSet(ctx, key, _value, (size_t)valueSize, expire);
}
//-------------------------------------------GET-----------------------------------------
//--OUTER--
template<class type> void SyncLockRGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, unsigned __int64 database, const char * password, unsigned __int64 _timeout)
{
    MemoryAttr retVal;
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->lockGet(ctx, key, &retVal, password);

    size_t returnSize = retVal.length();
    if (sizeof(type)!=returnSize)
    {
        VStringBuffer msg("RedisPlugin: ERROR - Requested type of different size (%uB) from that stored (%uB).", (unsigned)sizeof(type), (unsigned)returnSize);
        rtlFail(0, msg.str());
    }
    returnValue = *(static_cast<const type*>(retVal.get()));
}
template<class type> void SyncLockRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, type * & returnValue, unsigned __int64 database, const char * password, unsigned __int64 _timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, _timeout);
    master->lockGet(ctx, key, returnSize, returnValue, password);
}
//--INNER--
void Connection::lockGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password)
{
    handleLockOnGet(ctx, key, retVal, password);
}
template<class type> void Connection::lockGet(ICodeContext * ctx, const char * key, size_t & returnSize, type * & returnValue, const char * password)
{
    MemoryAttr retVal;
    handleLockOnGet(ctx, key, &retVal, password);
    returnSize = retVal.length();
    returnValue = reinterpret_cast<type*>(retVal.detach());
}
bool Connection::missThenLock(ICodeContext * ctx, const char * key)
{
    StringBuffer channel;
    encodeChannel(channel, key);
    return lock(ctx, key, channel);
}
void Connection::encodeChannel(StringBuffer & channel, const char * key) const
{
    channel.append(REDIS_LOCK_PREFIX).append("_").append(key).append("_").append(database).append("_").append(ip.str()).append("_").append(port);
}
bool Connection::lock(ICodeContext * ctx, const char * key, const char * channel)
{
    StringBuffer cmd("SET %b %b NX EX ");
    cmd.append(timeout/1000000);

    OwnedReply reply = Reply::createReply(redisCommand(context, cmd.str(), key, strlen(key), channel, strlen(channel)));
    assertOnError(reply->query(), cmd.append(" of the key '").append(key).append("' failed"));

    if (reply->query()->type == REDIS_REPLY_STATUS && strcmp(reply->query()->str, "OK") == 0)
        return true;
    return false;
}
void Connection::unlock(ICodeContext * ctx, const char * key)
{
    //WATCH key, if altered between WATCH and EXEC abort all commands inbetween
    redisAppendCommand(context, "WATCH %b", key, strlen(key));
    redisAppendCommand(context, "GET %b", key, strlen(key));

    //Read replies
    OwnedReply reply = new Reply();
    readReplyAndAssert(reply.get(), "manual unlock", key);//WATCH reply
    readReplyAndAssert(reply.get(), "manual unlock", key);//GET reply

    //check if locked
    if (strncmp(reply->query()->str, REDIS_LOCK_PREFIX, strlen(REDIS_LOCK_PREFIX)) == 0)
    {
        //MULTI - all commands between MULTI and EXEC are considered an atomic transaction on the server
        redisAppendCommand(context, "MULTI");//MULTI
        redisAppendCommand(context, "DEL %b", key, strlen(key));//DEL
        redisAppendCommand(context, "EXEC");//EXEC
#if(0)//Quick draw! You have 10s to manually (via redis-cli) send "set testlock foobar". The second myRedis.Exists('testlock') in redislockingtest.ecl should now return TRUE.
        sleep(10);
#endif
        readReplyAndAssert(reply.get(), "manual unlock", key);//MULTI reply
        readReplyAndAssert(reply.get(), "manual unlock", key);//DEL reply
        readReplyAndAssert(reply.get(), "manual unlock", key);//EXEC reply
    }
    //If the above is aborted, let the lock expire.
}
void Connection::handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password)
{
    StringBuffer channel;
    encodeChannel(channel, key);

    //SUB before GET
    //Requires separate connection from GET so that the replies are not mangled. This could be averted
    Owned<Connection> subConnection = new Connection(ctx, options.str(), ip.str(), port, serverIpPortPasswordHash, database, password, timeout);
    OwnedReply reply = Reply::createReply(redisCommand(subConnection->context, "SUBSCRIBE %b", channel.str(), channel.length()));
    assertOnCommandErrorWithKey(reply->query(), "GET", key);
    if (reply->query()->type == REDIS_REPLY_ARRAY && strcmp("subscribe", reply->query()->element[0]->str) != 0 )
    {
        VStringBuffer msg("Redis Plugin: ERROR - GET '%s' on database %" I64F "u failed : failed to register SUB", key, database);
        rtlFail(0, msg.str());
    }

#if(0)
    {
    OwnedReply pubReply = Reply::createReply(redisCommand(context, "PUBLISH %b %b", channel.str(), channel.length(), "foo", 3));
    assertOnError(pubReply->query(), "pub fail");
    }
#endif

    //Now GET
    reply->setClear((redisReply*)redisCommand(context, "GET %b", key, strlen(key)));
    assertOnCommandErrorWithKey(reply->query(), "GET", key);
    assertKey(reply->query(), key);

#if(0)
    {
    OwnedReply pubReply = Reply::createReply(redisCommand(context, "PUBLISH %b %b", channel.str(), channel.length(), "foo", 3));
    assertOnError(pubReply->query(), "pub fail");
    }
#endif

    //Check if returned value is locked
    if (strncmp(reply->query()->str, REDIS_LOCK_PREFIX, strlen(REDIS_LOCK_PREFIX)) != 0)
    {
        //Not locked so return value
        retVal->set(reply->query()->len, reply->query()->str);
        return;
    }
    else
    {
#if(0)//Added to allow for manual pub testing via redis-cli
        struct timeval to = { 10, 0 };//10secs
        redisSetTimeout(subConnection->context, to);
#endif
        //Locked so SUBSCRIBE
        redisReply * nakedReply = NULL;
        bool err = redisGetReply(subConnection->context, (void**)&nakedReply);
        reply->setClear(nakedReply);
        if (err != REDIS_OK)
            rtlFail(0, "RedisPlugin: ERROR - GET timed out.");
        assertOnCommandErrorWithKey(nakedReply, "GET", key);
        if (nakedReply->type == REDIS_REPLY_ARRAY && strcmp("message", nakedReply->element[0]->str) == 0)
        {
            retVal->set(nakedReply->element[2]->len, nakedReply->element[2]->str);//return the published value rather than another (WATCHed) GET.
            return;
        }
    }
    throwUnexpected();
}
void Connection::handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire)
{
    StringBuffer cmd("SET %b %b");
    RedisPlugin::appendExpire(cmd, expire);

    //Due to locking logic surfacing into ECL, any locking.set (such as this is) assumes that they own the lock and therefore go ahead and set regardless.
    //It is possible for a process/call to 'own' a lock and store this info in the LockObject, however, this prevents sharing between clients.
    redisAppendCommand(context, cmd.str(), key, strlen(key), value, size);//SET
    StringBuffer channel;
    encodeChannel(channel, key);
    redisAppendCommand(context, "PUBLISH %b %b", channel.str(), channel.length(), value, size);//PUB

    //Now read and assert replies
    OwnedReply replyContainer = new Reply();
    readReplyAndAssert(replyContainer, "SET", key);//SET reply
    readReplyAndAssert(replyContainer, "PUB for the key", key);//PUB reply
}
//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, size32_t valueSize, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, valueSize, value, expire,  database, password, timeout);
    returnSize = valueSize;
    returnValue = (char*)memcpy(rtlMalloc(valueSize), value, valueSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUChar(ICodeContext * ctx, size32_t & returnSize, UChar * & returnValue, const char * key, size32_t valueSize, const UChar * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, (valueSize)*sizeof(UChar), value, expire, database, password, timeout);
    returnSize = valueSize;
    returnValue = (UChar*)memcpy(rtlMalloc(valueSize), value, valueSize);
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncLockRSetInt(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, value, expire, database, password, timeout);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncLockRSetUInt(ICodeContext * ctx, const char * key, unsigned __int64 value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, value, expire, database, password, timeout);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncLockRSetReal(ICodeContext * ctx, const char * key, double value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, value, expire, database, password, timeout);
    return value;
}
ECL_REDIS_API bool ECL_REDIS_CALL SyncLockRSetBool(ICodeContext * ctx, const char * key, bool value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, value, expire, database, password, timeout);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetData(ICodeContext * ctx, size32_t & returnSize, void * & returnValue, const char * options, const char * key, size32_t valueSize, const void * value, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, valueSize, value, expire, database, password, timeout);
    returnSize = valueSize;
    returnValue = memcpy(rtlMalloc(valueSize), value, valueSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUtf8(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, size32_t valueSize, const char * value, const char * options, unsigned __int64 database, unsigned expire, const char * password, unsigned __int64 timeout)
{
    SyncLockRSet(ctx, options, key, rtlUtf8Size(valueSize, value), value, expire, database, password, timeout);
    returnSize = valueSize;
    returnValue = (char*)memcpy(rtlMalloc(valueSize), value, valueSize);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL SyncLockRGetBool(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    bool value;
    SyncLockRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncLockRGetDouble(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    double value;
    SyncLockRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncLockRGetInt8(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    signed __int64 value;
    SyncLockRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncLockRGetUint8(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    unsigned __int64 value;
    SyncLockRGet(ctx, options, key, value, database, password, timeout);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUChar(ICodeContext * ctx, size32_t & returnSize, UChar * & returnValue,  const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUtf8(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(rtlUtf8Length(_returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetData(ICodeContext * ctx, size32_t & returnSize, void * & returnValue, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API bool ECL_REDIS_CALL SyncLockRMissThenLock(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    return master->missThenLock(ctx, key);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRUnlock(ICodeContext * ctx, const char * key, const char * options, unsigned __int64 database, const char * password, unsigned __int64 timeout)
{
    Owned<Connection> master = Connection::createConnection(ctx, options, database, password, timeout);
    master->unlock(ctx, key);
}
}//close namespace
