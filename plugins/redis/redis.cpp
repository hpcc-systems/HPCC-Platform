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
#include "jmutex.hpp"
#include "redis.hpp"
extern "C"
{
#include "hiredis/hiredis.h"
}

#define REDIS_VERSION "redis plugin 1.0.0"
ECL_REDIS_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = REDIS_VERSION;
    pb->moduleName = "lib_redis";
    pb->ECL = nullptr;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for the C API hiredis";
    return true;
}

namespace RedisPlugin {

class Connection;

static const char * REDIS_LOCK_PREFIX = "redis_ecl_lock";
static __thread Connection * cachedConnection = nullptr;
static __thread Connection * cachedPubConnection = nullptr;//database should always = 0
static __thread Connection * cachedSubscriptionConnection = nullptr;

#define NO_CONNECTION_CACHING 0
#define ALLOW_CONNECTION_CACHING 1
#define CACHE_ALL_CONNECTIONS 2
#define INTERNAL_TIMEOUT -2
#define DUMMY_IP 0
#define DUMMY_PORT 0

static CriticalSection critsec;
static __thread bool threadHooked = false;
static int connectionCachingLevel = ALLOW_CONNECTION_CACHING;
static std::atomic<bool> connectionCachingLevelChecked(false);
static bool cacheSubConnections = true;
static std::atomic<bool> cacheSubConnectionsOptChecked(false);
static int unsubscribeTimeout = 100;//ms
static std::atomic<bool> unsubscribeTimeoutChecked(false);
static unsigned unsubscribeReadAttempts = 2;//The max number of possible socket read attempts when wanting the desired unsubscribe confirmation, otherwise give up.
static std::atomic<bool> unsubscribeReadAttemptsChecked(false);

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
    inline Reply() { };
    inline Reply(void * _reply) : reply((redisReply*)_reply) { }
    inline Reply(redisReply * _reply) : reply(_reply) { }
    inline ~Reply()
    {
        if (reply)
            freeReplyObject(reply);
    }

    static inline Reply * createReply(void * _reply) { return new Reply(_reply); }
    inline const redisReply * query() const { return reply; }
    inline void setClear(void * _reply) { setClear((redisReply*)_reply); }
    void setClear(redisReply * _reply);

private :
    redisReply * reply = nullptr;
};
void Reply::setClear(redisReply * _reply)
{
    if (reply == _reply)
        return;

    if (reply)
        freeReplyObject(reply);
    reply = _reply;
}
typedef Owned<RedisPlugin::Reply> OwnedReply;

class TimeoutHandler
{
public :
    TimeoutHandler(unsigned _timeout) : timeout(_timeout), t0(msTick()) { }
    inline void reset(unsigned _timeout) { timeout = _timeout; t0 = msTick(); }
    unsigned timeLeft() const;
    inline unsigned getTimeout() const { return timeout; }

private :
    unsigned timeout;
    unsigned t0;
};
unsigned TimeoutHandler::timeLeft() const
{
    //This function is ambiguous and the caller must disambiguate timeout == 0 from timeLeft == 0
    if (timeout)
    {
        unsigned dt = msTick() - t0;
        if (dt < timeout)
            return timeout - dt;
    }
    return 0;
}

class Connection : public CInterface
{
    friend class ConnectionContainer;
public :
    Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, bool parseOptions, int _database, const char * password, unsigned _timeout, bool selectDB);
    ~Connection() { freeContext(); }
    static Connection * createConnection(ICodeContext * ctx, Connection * & _cachedConnection, const char * options, const char * _ip, int _port, bool parseOptions, int _database, const char * password, unsigned _timeout, bool cachedConnectionRequested, bool isSubscription = false);

    //set
    template <class type> void setKey(ICodeContext * ctx, const char * key, type value, unsigned expire);
    template <class type> void setKey(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire);
    void setIntKey(ICodeContext * ctx, const char * key, signed __int64 value, unsigned expire, bool _unsigned);
    void setRealKey(ICodeContext * ctx, const char * key, double value, unsigned expire);

    //get
    template <class type> void getKey(ICodeContext * ctx, const char * key, type & value);
    template <class type> void getKey(ICodeContext * ctx, const char * key, size_t & valueSize, type * & value);
    template <class type> void getNumericKey(ICodeContext * ctx, const char * key, type & value);
    signed __int64 returnInt(const char * key, const char * cmd, const redisReply * reply);

    //-------------------------------LOCKING------------------------------------------------
    void lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, unsigned expire);
    void lockGet(ICodeContext * ctx, const char * key, size_t & valueSize, char * & value, const char * password, unsigned expire);
    void unlock(ICodeContext * ctx, const char * key);
    //--------------------------------------------------------------------------------------

    //-------------------------------PUB/SUB------------------------------------------------
    unsigned __int64 publish(ICodeContext * ctx, const char * keyOrChannel, size32_t messageSize, const char * message, int _database, bool lockedKey);
    void subAndWaitForSinglePub(ICodeContext * ctx, const char * keyOrChannel, size_t & messageSize, char * & message, int _database, bool lockedKey);
    //--------------------------------------------------------------------------------------

    void persist(ICodeContext * ctx, const char * key);
    void expire(ICodeContext * ctx, const char * key, unsigned _expire);
    void del(ICodeContext * ctx, const char * key);
    void clear(ICodeContext * ctx);
    unsigned __int64 dbSize(ICodeContext * ctx);
    bool exists(ICodeContext * ctx, const char * key);
    signed __int64 incrBy(ICodeContext * ctx, const char * key, signed __int64 value);

protected : //Specific to subscribed connections
    void subscribe(ICodeContext * ctx, const char * channel);
    void unsubscribe();
    bool isCorrectChannel(const redisReply * reply, const char * op) const;
    int redisSetUnsubscribeTimeout();
    static int getUnsubscribeTimeout();
    static unsigned getUnsubscribeReadAttempts();

protected :
    void freeContext();
    int redisSetTimeout();
    int setTimeout(unsigned _timeout);
    inline unsigned timeLeft() const { return timeout.timeLeft(); }
    void assertTimeout(int state);
    void redisConnect();
    void doParseOptions(ICodeContext * ctx, const char * _options);
    void connect(ICodeContext * ctx, int _database, const char * password, bool selectDB);
    inline bool isCachedConnection() const { return (this == cachedConnection) || (this == cachedPubConnection) || (this == cachedSubscriptionConnection); }
    void selectDB(ICodeContext * ctx, int _database);
    void readReply(Reply * reply);
    void readReplyAndAssert(Reply * reply, const char * msg);
    void readReplyAndAssertWithCmdMsg(Reply * reply, const char * msg, const char * key = nullptr);
    void assertKey(const redisReply * reply, const char * key);
    void assertAuthorization(const redisReply * reply);
    void assertOnError(const redisReply * reply, const char * _msg);
    void assertOnErrorWithCmdMsg(const redisReply * reply, const char * cmd, const char * key = nullptr);
    void assertConnection(const char * _msg);
    void assertConnectionWithCmdMsg(const char * cmd, const char * key = nullptr);
    __declspec(noreturn) void fail(const char * cmd, const char * errmsg, const char * key = nullptr) __attribute__((noreturn));
    void * redisCommand(const char * format, ...);
    void fromStr(const char * str, const char * key, double & ret);
    void fromStr(const char * str, const char * key, signed __int64 & ret);
    void fromStr(const char * str, const char * key, unsigned __int64 & ret);
    static unsigned hashServerIpPortPassword(ICodeContext * ctx, const char * _options, const char * password);
    static bool canCacheConnections(bool cachedConnectionRequested, bool isSubscription);
    static int getConnectionCachingLevel();
    static bool getCacheSubConnections();
    int writeBufferToSocket();
    bool isSameConnection(ICodeContext * ctx, const char * _options, const char * password) const;
    void reset(ICodeContext * ctx, unsigned _database, const char * password, unsigned _timeout, bool selectDB);

    //-------------------------------LOCKING------------------------------------------------
    void handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire);
    void handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password, unsigned expire);
    const char * encodeChannel(StringBuffer & buffer, const char * keyOrChannel, int _database, bool lockedKey) const;
    bool noScript(const redisReply * reply) const;
    bool lock(ICodeContext * ctx, const char * key, const char * channel, unsigned expire);
    //--------------------------------------------------------------------------------------

protected :
    redisContext * context = nullptr;
    StringAttr options;
    StringAttr ip; //The default is set in parseOptions as "localhost"
    unsigned serverIpPortPasswordHash = 0;
    int port = 6379; //Default redis-server port
    TimeoutHandler timeout;
    int database = 0; //NOTE: redis stores the maximum number of dbs as an 'int'.

    StringAttr channel;
    bool subscribed = false;
};
class ConnectionContainer : public CInterface
{
public :
    ConnectionContainer() { }
    ConnectionContainer(Connection * _connection)
    {
        connection.setown(_connection);
    }
    ~ConnectionContainer()
    {
        if (connection)
            connection->unsubscribe();
    }
    inline Connection * operator -> () const { return connection.get(); }
    inline void setown(Connection * _connection) { connection.setown(_connection); }
    __declspec(noreturn) void handleException(IException * error) __attribute__((noreturn))
    {
        if (connection)
            connection->freeContext();
        throw error;
    }

    Owned<Connection> connection;
};
static bool releaseAllCachedContexts(bool isPooled)
{
    if (cachedConnection)
    {
        cachedConnection->Release();
        cachedConnection = nullptr;
    }
    if (cachedPubConnection)
    {
        cachedPubConnection->Release();
        cachedPubConnection = nullptr;
    }
    if (cachedSubscriptionConnection)
    {
        cachedSubscriptionConnection->Release();
        cachedSubscriptionConnection = nullptr;
    }
    threadHooked = false;
    return false;
}
//The following class is here to ensure destruction of the cachedConnection within the main thread
//as this is not handled by the thread hook mechanism.
static class MainThreadCachedConnection
{
public :
    MainThreadCachedConnection() { }
    ~MainThreadCachedConnection() { releaseAllCachedContexts(false); }
} mainThread;

Connection::Connection(ICodeContext * ctx, const char * _options, const char * _ip, int _port, bool parseOptions, int _database, const char * password, unsigned _timeout, bool selectDB)
  : timeout(_timeout)
{
    serverIpPortPasswordHash = hashServerIpPortPassword(ctx, _options, password);
    options.set(_options, strlen(_options));

    if (parseOptions || !_ip || !_port)//parseOptions(true) is intended to be used when passing a valid ip & port but check just in case.
        doParseOptions(ctx, _options);
    else
    {
        port = _port;
        ip.set(_ip, strlen(_ip));
    }
    connect(ctx, _database, password, selectDB);
}
void Connection::redisConnect()
{
    freeContext();
    if (timeout.getTimeout() == 0)
        context = ::redisConnect(ip.str(), port);
    else
    {
        unsigned timeStillLeft = timeLeft();
        if (timeStillLeft == 0)
            rtlFail(0, "Redis Plugin: ERROR - function timed out internally.");
        struct timeval to = { (time_t) (timeStillLeft/1000), (suseconds_t) ((timeStillLeft%1000)*1000) };
        context = ::redisConnectWithTimeout(ip.str(), port, to);
    }
    assertConnection("connection");
}
void Connection::connect(ICodeContext * ctx, int _database, const char * password, bool selectDB)
{
    redisConnect();

    //The following is the dissemination of the two methods authenticate(ctx, password) & selectDB(ctx, _database)
    //such that they may be pipelined to save an extra round trip to the server and back.
    if (password && *password)
        redisAppendCommand(context, "AUTH %b", password, strlen(password));

    if (selectDB && (database != _database))
    {
        VStringBuffer cmd("SELECT %d", _database);
        redisAppendCommand(context, cmd.str());
    }

    //Now read replies.
    OwnedReply reply = new Reply();
    if (password && *password)
        readReplyAndAssert(reply, "server authentication");

    if (selectDB && (database != _database))
    {
        VStringBuffer cmd("SELECT %d", _database);
        readReplyAndAssertWithCmdMsg(reply, cmd.str());
        database = _database;
    }
}
void * Connection::redisCommand(const char * format, ...)
{
    //Copied from https://github.com/redis/hiredis/blob/master/hiredis.c ~line:1008 void * redisCommand(redisContext * context, const char * format, ...)
    //with redisSetTimeout(); added.
    va_list parameters;
    void * reply = nullptr;
    va_start(parameters, format);
    assertTimeout(redisSetTimeout());
    reply = ::redisvCommand(context, format, parameters);
    va_end(parameters);
    return reply;
}
int Connection::setTimeout(unsigned _timeout)
{
    struct timeval to = { (time_t) (_timeout/1000), (suseconds_t) ((_timeout%1000)*1000) };
    if (!context)
        return REDIS_ERR;
    return ::redisSetTimeout(context, to);//NOTE: ::redisSetTimeout sets the socket timeout and therefore 0 => forever
}
int Connection::redisSetTimeout()
{
    unsigned timeStillLeft = timeLeft();
    if (timeStillLeft == 0 && timeout.getTimeout() != 0)
        return INTERNAL_TIMEOUT;
    return setTimeout(timeStillLeft);
}
int Connection::redisSetUnsubscribeTimeout()
{
    unsigned timeout = getUnsubscribeTimeout();
    if (timeout == 0)//0 => no timeout => use normal timeout
        return redisSetTimeout();

    unsigned timeStillLeft =  timeLeft();
    if (timeStillLeft == 0)
        return INTERNAL_TIMEOUT;

    unsigned tmp = unsubscribeTimeout < timeStillLeft ? unsubscribeTimeout : timeStillLeft;
    return setTimeout(tmp);
}
bool Connection::isSameConnection(ICodeContext * ctx, const char * _options, const char * password) const
{
    return (hashServerIpPortPassword(ctx, _options, password) == serverIpPortPasswordHash);
}
unsigned Connection::hashServerIpPortPassword(ICodeContext * ctx, const char * _options, const char * password)
{
    return hashc((const unsigned char*)_options, strlen(_options), hashc((const unsigned char*)password, strlen(password), 0));
}
void Connection::reset(ICodeContext * ctx, unsigned _database, const char * password, unsigned _timeout, bool selectDB)
{
    timeout.reset(_timeout);
    if (!context || context->err != REDIS_OK)
    {
        database = 0;
        connect(ctx, _database, password, selectDB);
    }
}
void Connection::doParseOptions(ICodeContext * ctx, const char * _options)
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
            VStringBuffer err("Redis Plugin: ERROR - unsupported option string '%s'", opt);
            rtlFail(0, err.str());
        }
    }
    if (ip.isEmpty())
    {
        ip.set("localhost");
        if (ctx)
        {
            VStringBuffer msg("Redis Plugin: WARNING - using default cache (%s:%d)", ip.str(), port);
            ctx->logString(msg.str());
        }
    }
}
void Connection::freeContext()
{
    subscribed = false;
    channel.clear();
    if(context)
    {
        redisFree(context);
        context = nullptr;
        database = 0;
    }
}
const char * Connection::encodeChannel(StringBuffer & buffer, const char * keyOrChannel, int _database, bool lockedKey) const
{
    if (lockedKey)
        buffer.append(REDIS_LOCK_PREFIX).append("_").append(keyOrChannel).append("_").append(_database);
    else
        buffer.set(keyOrChannel);
    return buffer.str();
}
bool Connection::isCorrectChannel(const redisReply * reply, const char * op) const
{
    if (!reply || channel.isEmpty())
        return false;
    return (reply->type == REDIS_REPLY_ARRAY)
            && (reply->elements > 1)
            && (strcmp(op, reply->element[0]->str) == 0)
            && (strcmp(channel.str(), reply->element[1]->str) == 0);
}
void Connection::subscribe(ICodeContext * ctx, const char * _channel)
{
    assert(!subscribed || channel.isEmpty());//If this is a reused subscriptionConnection then this implies that it was not correctly unsubscribed/reset.
    channel.set(_channel);

    OwnedReply reply = Reply::createReply(redisCommand("SUBSCRIBE %b", channel.str(), (size_t)channel.length()));
    /* It is possible for the subscription to succeed in being registered by the server but fails in receiving the associated reply.
     * Set early to prevent not unsubscribing when failing.
     */
    subscribed = true;
    assertOnErrorWithCmdMsg(reply->query(), "SUBSCRIBE", channel.str());
    if (!isCorrectChannel(reply->query(), "subscribe"))
        fail("SUBSCRIBE", "failed to register SUB", channel.str());
}
void Connection::subAndWaitForSinglePub(ICodeContext * ctx, const char * keyOrChannel, size_t & messageSize, char * & message, int _database, bool lockedKey)
{
    StringBuffer channelBuffer;
    encodeChannel(channelBuffer, keyOrChannel, _database, lockedKey);
    subscribe(ctx, channelBuffer.str());

    //Now wait for published message
    OwnedReply reply = new Reply();
    readReply(reply);
    assertOnErrorWithCmdMsg(reply->query(), "SUBSCRIBE", channel.str());

    if (isCorrectChannel(reply->query(), "message"))
    {
        if (reply->query()->element[2]->len > 0)
        {
            messageSize = (size_t)reply->query()->element[2]->len;
            message = reinterpret_cast<char*>(allocateAndCopy(reply->query()->element[2]->str, messageSize));
        }
        else
        {
            messageSize = 0;
            message = nullptr;
        }
        unsubscribe();
        return;
    }
    throwUnexpected();
}
int Connection::writeBufferToSocket()
{
    int done = 0;
    if (context->flags & REDIS_BLOCK)
    {
       /* Write until done */
       while (!done)
       {
           if (redisBufferWrite(context, &done) == REDIS_ERR)
               return REDIS_ERR;
       }
    }
    return REDIS_OK;
}
unsigned Connection::getUnsubscribeReadAttempts()
{
    if (!unsubscribeReadAttemptsChecked)//unsubscribeReadAttemptsChecked is std:atomic<bool>. Test to guard against unnecessary critical section
    {
        CriticalBlock block(critsec);
        if (!unsubscribeReadAttemptsChecked)
        {
            const char * tmp = getenv("HPCC_REDIS_PLUGIN_UNSUBSCRIBE_READ_ATTEMPTS");//ms
            if (tmp && *tmp)
                unsubscribeReadAttempts = atoi(tmp);
            unsubscribeReadAttemptsChecked = true;
        }
    }
    return unsubscribeReadAttempts;
}
void Connection::unsubscribe()
{
    /* redisContext has both an output buffer (context->obuf) and an input buffer (context->reader).
     * redisCommand writes the command to the output buffer and then calls redisGetReply. This will first try to read unconsumed replies from the input buffer.
     * If there are none then it will flush the output buffer to the socket and wait for a (or multiples of) redisReply from the socket which writes this to the input buffer.
     * This is undesired behaviour here as it will not send the UNSUBSCRIBE command until all replies are consumed on the input buffer. Furthermore, there could be
     * spurious commands still in the output buffer preceding the UNSIBSCRIBE, though in the current plugin implementation there shouldn't be, unless in a failing state.
     * To solve these two (client) issues both buffers are cleared before unsubscribing. However, the same applies for the server side output buffer and more importantly the
     * client socket file descriptor as the redis server pushes published messages to clients. In order to reuse the connection for subsequent subscriptions this unsubscribe
     * must be confirmed and in order to do this all (now) spurious preceding replies must first be consumed. Because a single socket read will read more than a single
     * redis reply (if >1 exists waiting on the server), we can switch subsequent reads to just the input buffer. The point here is that we want to limit the number of
     * possible socket reads and thus possible waits. We then only make a maximum of HPCC_REDIS_PLUGIN_UNSUBSCRIBE_READ_ATTEMPTS socket reads.
     */

    if (!subscribed)
        return;

    //return early
    if (!context || (context->err != REDIS_OK) || channel.isEmpty())
    {
        freeContext();
        return;
    }

    /* hiredis v0.12.0 is broken (hiredis.h includes read.h but read.h is not installed), and <=v0.11.0 sds.h is neither installed nor included in hiredis.h.
     *
     * SubConnections are only used to sub and unsub. They only sub to a single channel at a time (currently) so only an un-flushed UNSUBSCRIBE may still be
     * still be present in the output buffer. If so it will either be for the same channel or another and thus is benign in nature as long as the channel is
     * is also confirmed in the reply. This implies that it should be ok to not clear the output buffer. Further more, connection caching is only used for
     * hiredis >= v0.13.0 which is where this is preferably needed.
     */
#if HIREDIS_MAJOR >= 0 && 12 >= MIN_HIREDIS_MINOR && HIREDIS_PATCH >= 1
    //Empty output buffer as it may contain previous and now unwanted commands. Since the subscription connections only sub and unsub, this should already be empty.
    if (*context->obuf != '\0')//obuf is a redis sds string (chr*) containing a header with the actual pointer pointing directly to string buffer post header.
    {
        sdsfree(context->obuf);//free/clear current buffer
        context->obuf = sdsempty();//setup new one ready for writing
    }
#endif

    //Empty input buffer as it may contain previous and now unwanted replies
    if (context->reader->len > 0)
    {
        redisReaderFree(context->reader);
        context->reader = redisReaderCreate();
    }

    //Write command to buffer, set timeout, and write to socket.
    bool cmdAppendOK = redisAppendCommand(context, "UNSUBSCRIBE %b", channel.str(), channel.length());
    if ((cmdAppendOK != REDIS_OK) || (redisSetUnsubscribeTimeout() != REDIS_OK) || (writeBufferToSocket() != REDIS_OK))
    {
        freeContext();
        return;
    }

    OwnedReply reply = new Reply();
    for (unsigned i = 0; i < getUnsubscribeReadAttempts(); i++)
    {
        if (redisSetUnsubscribeTimeout() != REDIS_OK)
        {
            freeContext();
            return;
        }
        redisReply * nakedReply = nullptr;
        redisGetReply(context, (void**)&nakedReply);
        reply->setClear(nakedReply);
        if (!reply->query())
        {
            freeContext();
            return;
        }
        if (isCorrectChannel(reply->query(), "unsubscribe"))
        {
            channel.clear();
            subscribed = false;
            return;
        }

        /* Whilst the input buffer was cleared the same may not be true for that server side.
         * We can read as many replies from the reader as we like but only 'unsubscribeReadAttempts'
         * from the socket.
         */
        bool replyErrorFound = (reply->query()->type == REDIS_REPLY_ERROR);
        for(;;)
        {
            redisReply * nakedReply = nullptr;
            if (redisReaderGetReply(context->reader, (void**)&nakedReply) != REDIS_OK)
            {
                freeContext();
                return;
            }
            if (!nakedReply)
                break;
            reply->setClear(nakedReply);

            if (nakedReply->type == REDIS_REPLY_ERROR)
            {
                replyErrorFound = true;
                continue;
            }
            if (isCorrectChannel(reply->query(), "unsubscribe"))
            {
                channel.clear();
                subscribed = false;
                return;
            }
        }

        /* If a reply error was encountered there is no way to know if it was
         * associated with the unsubscribe attempt of from a previous and unwanted
         * reply, at this point. Either it was, in which case we need to clear up or read
         * over from the socket again. The latter is not acceptable in case it *was* associated
         * and thus there are no more replies and therefore not wasting time for the read to time out.
         */
        if (replyErrorFound)
        {
            freeContext();
            return;
        }
    }
    freeContext();
}
void Connection::assertTimeout(int state)
{
    switch(state)
    {
    case REDIS_OK :
        return;
    case REDIS_ERR :
        assertConnection("request to set timeout");
        break;
    case INTERNAL_TIMEOUT :
        rtlFail(0, "Redis Plugin: ERROR - function timed out internally.");
    }
}
void Connection::readReply(Reply * reply)
{
    redisReply * nakedReply = nullptr;
    assertTimeout(redisSetTimeout());
    redisGetReply(context, (void**)&nakedReply);
    reply->setClear(nakedReply);
}
void Connection::readReplyAndAssert(Reply * reply, const char * msg)
{
    readReply(reply);
    assertOnError(reply->query(), msg);
}
void Connection::readReplyAndAssertWithCmdMsg(Reply * reply, const char * msg, const char * key)
{
    readReply(reply);
    assertOnErrorWithCmdMsg(reply->query(), msg, key);
}
int Connection::getConnectionCachingLevel()
{
    //Fetch connection caching level
    if (!connectionCachingLevelChecked)//connectionCachingLevelChecked is std:atomic<bool>. Test to guard against unnecessary critical section
    {
        CriticalBlock block(critsec);
        if (!connectionCachingLevelChecked)
        {
            const char * tmp = getenv("HPCC_REDIS_PLUGIN_CONNECTION_CACHING_LEVEL"); // 0 = NO_CONNECTION_CACHING, 1 = ALLOW_CONNECTION_CACHING, 2 = CACHE_ALL_CONNECTIONS
            //connectionCachingLevel is already defaulted to ALLOW_CONNECTION_CACHING
            if (tmp && *tmp)
                connectionCachingLevel = atoi(tmp); //don't bother range checking
            connectionCachingLevelChecked = true;
        }
    }
    return connectionCachingLevel;
}
bool Connection::getCacheSubConnections()
{
    if (!cacheSubConnectionsOptChecked)//cacheSubConnectionsOptChecked is std:atomic<bool>. Test to guard against unnecessary critical section
    {
        CriticalBlock block(critsec);
        if (!cacheSubConnectionsOptChecked)
        {
            const char * tmp = getenv("HPCC_REDIS_PLUGIN_CACHE_SUB_CONNECTIONS");
            //cacheSubConnections is already defaulted to true;
            if (tmp && *tmp)
            {
                //less ops to check on & true however, making cacheSubConnections(false) if not met would do also for misspelled versions.
                //Since the default is ON, misspelling on or true means/does nothing.
                if (*tmp == '0' || (stricmp("off", tmp) == 0) || (stricmp("false", tmp) == 0))
                    cacheSubConnections = false;
            }
            cacheSubConnectionsOptChecked= true;
        }
    }
    return cacheSubConnections;
}
int Connection::getUnsubscribeTimeout()
{
    if (!unsubscribeTimeoutChecked)//unsubscribeTimeoutChecked is std:atomic<bool>. Test to guard against unnecessary critical section
    {
        CriticalBlock block(critsec);
        if (!unsubscribeTimeoutChecked)
        {
            const char * tmp = getenv("HPCC_REDIS_PLUGIN_UNSUBSCRIBE_TIMEOUT");//ms
            if (tmp && *tmp)
                unsubscribeTimeout = atoi(tmp);
            unsubscribeTimeoutChecked = true;
        }
    }
    return unsubscribeTimeout;
}
bool Connection::canCacheConnections(bool cachedConnectionRequested, bool isSubscription)
{
#if HIREDIS_VERSION_OK_FOR_CACHING
    switch (getConnectionCachingLevel())
    {
    case CACHE_ALL_CONNECTIONS :
        return true;
    case NO_CONNECTION_CACHING :
        return false;
    }

    if (isSubscription)
        return cachedConnectionRequested && getCacheSubConnections();

    return cachedConnectionRequested;
#endif
    return false;
}
static void addThreadHook()
{
    if (!threadHooked)
    {
        addThreadTermFunc(releaseAllCachedContexts);
        threadHooked = true;
    }
}
Connection * Connection::createConnection(ICodeContext * ctx,  Connection * & _cachedConnection, const char * _options, const char * _ip, int _port, bool parseOptions, int _database, const char * password, unsigned _timeout, bool cachedConnectionRequested, bool isSubscription)
{
    if (canCacheConnections(cachedConnectionRequested, isSubscription))
    {
        if (!_cachedConnection)
        {
           _cachedConnection = new Connection(ctx, _options, _ip, _port, parseOptions, _database, password, _timeout, !isSubscription);
            addThreadHook();
            return LINK(_cachedConnection);
        }

        if (_cachedConnection->isSameConnection(ctx, _options, password))
        {
            //MORE: should perhaps check that the connection has not expired (think hiredis REDIS_KEEPALIVE_INTERVAL is defaulted to 15s).
            _cachedConnection->reset(ctx, _database, password, _timeout, !isSubscription);//If the context had been previously freed, this will reconnect and selectDB in connect().
            _cachedConnection->selectDB(ctx, _database);//If the context is still present selectDB here.
            return LINK(_cachedConnection);
        }

        _cachedConnection->Release();
        _cachedConnection = nullptr;
        _cachedConnection = new Connection(ctx, _options, _ip, _port, parseOptions, _database, password, _timeout, !isSubscription);
        return LINK(_cachedConnection);
    }
    else
        return new Connection(ctx, _options, _ip, _port, parseOptions, _database, password, _timeout, !isSubscription);
}
void Connection::selectDB(ICodeContext * ctx, int _database)
{
    if (database == _database || subscribed)
        return;
    database = _database;
    VStringBuffer cmd("SELECT %d", database);
    OwnedReply reply = Reply::createReply(redisCommand(cmd.str()));
    assertOnErrorWithCmdMsg(reply->query(), cmd.str());
}
void Connection::fail(const char * cmd, const char * errmsg, const char * key)
{
    if (key)
    {
        VStringBuffer msg("Redis Plugin: ERROR - %s '%s' on database %d for %s:%d failed : %s", cmd, key, database, ip.str(), port, errmsg);
        rtlFail(0, msg.str());
    }
    VStringBuffer msg("Redis Plugin: ERROR - %s on database %d for %s:%d failed : %s", cmd, database, ip.str(), port, errmsg);
    rtlFail(0, msg.str());
}
void Connection::assertOnError(const redisReply * reply, const char * _msg)
{
    if (!reply)
    {
        assertConnection(_msg);
        throwUnexpected();
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        VStringBuffer msg("Redis Plugin: %s - %s", _msg, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertOnErrorWithCmdMsg(const redisReply * reply, const char * cmd, const char * key)
{
    if (!reply)
    {
        assertConnectionWithCmdMsg(cmd, key);
        throwUnexpected();
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        assertAuthorization(reply);
        fail(cmd, reply->str, key);
    }
}
void Connection::assertAuthorization(const redisReply * reply)
{
    if (reply && reply->str && ( strncmp(reply->str, "NOAUTH", 6) == 0 || strncmp(reply->str, "ERR operation not permitted", 27) == 0 ))
    {
        VStringBuffer msg("Redis Plugin: ERROR - authentication for %s:%d failed : %s", ip.str(), port, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertKey(const redisReply * reply, const char * key)
{
    if (reply && reply->type == REDIS_REPLY_NIL)
    {
        VStringBuffer msg("Redis Plugin: ERROR - the requested key '%s' does not exist on database %d on %s:%d", key, database, ip.str(), port);
        rtlFail(0, msg.str());
    }
}
void Connection::assertConnectionWithCmdMsg(const char * cmd, const char * key)
{
    if (!context)
        fail(cmd, "neither 'reply' nor connection error available", key);
    else if (context->err)
        fail(cmd, context->errstr, key);
}
void Connection::assertConnection(const char * _msg)
{
    if (!context)
    {
        VStringBuffer msg("Redis Plugin: ERROR - %s for %s:%d failed : neither 'reply' nor connection error available", _msg, ip.str(), port);
        rtlFail(0, msg.str());
    }
    else if (context->err)
    {
        VStringBuffer msg("Redis Plugin: ERROR - %s for %s:%d failed : %s", _msg, ip.str(), port, context->errstr);
        rtlFail(0, msg.str());
    }
}
void Connection::clear(ICodeContext * ctx)
{
    //NOTE: flush is the actual cache flush/clear/delete and not an io buffer flush.
    OwnedReply reply = Reply::createReply(redisCommand("FLUSHDB"));//NOTE: FLUSHDB deletes current database where as FLUSHALL deletes all dbs.
    //NOTE: documented as never failing, but in case
    assertOnErrorWithCmdMsg(reply->query(), "FlushDB");
}
void Connection::del(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand("DEL %b", key, strlen(key)));
    assertOnErrorWithCmdMsg(reply->query(), "Del", key);
}
void Connection::persist(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand("PERSIST %b", key, strlen(key)));
    assertOnErrorWithCmdMsg(reply->query(), "Persist", key);
}
void Connection::expire(ICodeContext * ctx, const char * key, unsigned _expire)
{
    OwnedReply reply = Reply::createReply(redisCommand("PEXPIRE %b %u", key, strlen(key), _expire));
    assertOnErrorWithCmdMsg(reply->query(), "Expire", key);
}
bool Connection::exists(ICodeContext * ctx, const char * key)
{
    OwnedReply reply = Reply::createReply(redisCommand("EXISTS %b", key, strlen(key)));
    assertOnErrorWithCmdMsg(reply->query(), "Exists", key);
    return (reply->query()->integer != 0);
}
unsigned __int64 Connection::dbSize(ICodeContext * ctx)
{
    OwnedReply reply = Reply::createReply(redisCommand("DBSIZE"));
    assertOnErrorWithCmdMsg(reply->query(), "DBSIZE");
    return reply->query()->integer;
}
signed __int64 Connection::incrBy(ICodeContext * ctx, const char * key, signed __int64 value)
{
    OwnedReply reply = Reply::createReply(redisCommand("INCRBY %b %" I64F "d", key, strlen(key), value));
    return returnInt(key, "INCRBY", reply->query());
}
//-------------------------------------------SET-----------------------------------------
void Connection::setIntKey(ICodeContext * ctx, const char * key, signed __int64 value, unsigned expire, bool _unsigned)
{
    StringBuffer cmd("SET %b %" I64F);
    if (_unsigned)
        cmd.append("u");
    else
        cmd.append("d");
    appendExpire(cmd, expire);

    OwnedReply reply = Reply::createReply(redisCommand(cmd.str(), key, strlen(key), value));
    assertOnErrorWithCmdMsg(reply->query(), "SET", key);
}
void Connection::setRealKey(ICodeContext * ctx, const char * key, double value, unsigned expire)
{
    StringBuffer cmd("SET %b %.16g");
    appendExpire(cmd, expire);
    OwnedReply reply = Reply::createReply(redisCommand(cmd.str(), key, strlen(key), value));
    assertOnErrorWithCmdMsg(reply->query(), "SET", key);
}
//--OUTER--
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, type value, int database, unsigned expire, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, _options, DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->setKey(ctx, key, value, expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
//Set pointer types
template<class type> void SyncRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const type * value, int database, unsigned expire, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, _options,  DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->setKey(ctx, key, valueSize, value, expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
//--INNER--
template<class type> void Connection::setKey(ICodeContext * ctx, const char * key, type value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);

    OwnedReply reply = Reply::createReply(redisCommand(cmd.str(), key, strlen(key), _value, sizeof(type)));
    assertOnErrorWithCmdMsg(reply->query(), "SET", key);
}
template<class type> void Connection::setKey(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining

    StringBuffer cmd("SET %b %b");
    appendExpire(cmd, expire);
    OwnedReply reply = Reply::createReply(redisCommand(cmd.str(), key, strlen(key), _value, (size_t)valueSize));
    assertOnErrorWithCmdMsg(reply->query(), "SET", key);
}
//-------------------------------------------GET-----------------------------------------
signed __int64 Connection::returnInt(const char * key, const char * cmd, const redisReply * reply)
{
    assertOnErrorWithCmdMsg(reply, cmd, key);
    assertKey(reply, key);
    if (reply->type == REDIS_REPLY_INTEGER)
        return reply->integer;

    fail(cmd, "expected RESP integer from redis", key);
    throwUnexpected(); //stop compiler complaining
}
//--OUTER--
template<class type> void SyncRGetNumeric(ICodeContext * ctx, const char * options, const char * key, type & returnValue, int database, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options,  DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->getNumericKey(ctx, key, returnValue);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, int database, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options,  DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->getKey(ctx, key, returnValue);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
template<class type> void SyncRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, type * & returnValue, int database, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options,  DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->getKey(ctx, key, returnSize, returnValue);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
void Connection::fromStr(const char * str, const char * key, double & ret)
{
    char * end = nullptr;
    ret = strtod(str, &end);
    if (errno == ERANGE)
        fail("GetReal", "value returned out of range", key);
}
void Connection::fromStr(const char * str, const char * key, signed __int64 & ret)
{
    char* end = nullptr;
    ret = strtoll(str, &end, 10);
    if (errno == ERANGE)
        fail("GetInteger", "value returned out of range", key);
}
void Connection::fromStr(const char * str, const char * key, unsigned __int64 & ret)
{
    char* end = nullptr;
    ret = strtoull(str, &end, 10);
    if (errno == ERANGE)
        fail("GetUnsigned", "value returned out of range", key);
}
//--INNER--
template<class type> void Connection::getNumericKey(ICodeContext * ctx, const char * key, type & returnValue)
{
    OwnedReply reply = Reply::createReply(redisCommand("GET %b", key, strlen(key)));

    assertOnErrorWithCmdMsg(reply->query(), "GET", key);
    assertKey(reply->query(), key);
    fromStr(reply->query()->str, key, returnValue);
}
template<class type> void Connection::getKey(ICodeContext * ctx, const char * key, type & returnValue)
{
    OwnedReply reply = Reply::createReply(redisCommand("GET %b", key, strlen(key)));

    assertOnErrorWithCmdMsg(reply->query(), "GET", key);
    assertKey(reply->query(), key);

    size_t returnSize = reply->query()->len;
    if (sizeof(type)!=returnSize)
    {
        VStringBuffer msg("requested type of different size (%uB) from that stored (%uB)", (unsigned)sizeof(type), (unsigned)returnSize);
        fail("GET", msg.str(), key);
    }
    memcpy(&returnValue, reply->query()->str, returnSize);
}
template<class type> void Connection::getKey(ICodeContext * ctx, const char * key, size_t & returnSize, type * & returnValue)
{
    OwnedReply reply = Reply::createReply(redisCommand("GET %b", key, strlen(key)));

    assertOnErrorWithCmdMsg(reply->query(), "GET", key);
    assertKey(reply->query(), key);

    returnSize = reply->query()->len;
    returnValue = reinterpret_cast<type*>(allocateAndCopy(reply->query()->str, returnSize));
}
unsigned __int64 Connection::publish(ICodeContext * ctx, const char * keyOrChannel, size32_t messageSize, const char * message, int _database, bool lockedKey)
{
    StringBuffer channel;
    encodeChannel(channel, keyOrChannel, _database, lockedKey);

    OwnedReply reply = Reply::createReply(redisCommand("PUBLISH %b %b", channel.str(), (size_t)channel.length(), message, (size_t)messageSize));
    assertOnErrorWithCmdMsg(reply->query(), "PUBLISH", channel.str());
    if (reply->query()->type == REDIS_REPLY_INTEGER)
    {
        if (reply->query()->integer >= 0)
            return (unsigned __int64)reply->query()->integer;
        else
            throwUnexpected();
    }
    throwUnexpected();
}
//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRPub(ICodeContext * ctx, const char * keyOrChannel, size32_t messageSize, const char * message, const char * options, int database, const char * password, unsigned timeout, bool lockedKey, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedPubConnection, options, DUMMY_IP, DUMMY_PORT, true, 0, password, timeout, cachedConnectionRequested));
        return master->publish(ctx, keyOrChannel, messageSize, message, database, lockedKey);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSub(ICodeContext * ctx, size32_t & messageSize, char * & message, const char * keyOrChannel, const char * options, int database, const char * password, unsigned timeout, bool lockedKey, bool cachedConnectionRequested)
{
    size_t _messageSize = 0;
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedSubscriptionConnection, options, DUMMY_IP, DUMMY_PORT, true, 0, password, timeout, cachedConnectionRequested, true));
        master->subAndWaitForSinglePub(ctx, keyOrChannel, _messageSize, message, database, lockedKey);
        messageSize = static_cast<size32_t>(_messageSize);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL RClear(ICodeContext * ctx, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->clear(ctx);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API bool ECL_REDIS_CALL RExist(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        return master->exists(ctx, key);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL RDel(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->del(ctx, key);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL RPersist(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->persist(ctx, key);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL RExpire(ICodeContext * ctx, const char * key, const char * options, int database, unsigned _expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->expire(ctx, key, _expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RDBSize(ICodeContext * ctx, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        return master->dbSize(ctx);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncRINCRBY(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        return master->incrBy(ctx, key, value);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetStr(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout, cachedConnectionRequested);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUChar(ICodeContext * ctx, const char * key, size32_t valueLength, const UChar * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncRSet(ctx, options, key, (valueLength)*sizeof(UChar), value, database, expire, password, timeout, cachedConnectionRequested);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetInt(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->setIntKey(ctx, key, value, expire, false);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUInt(ICodeContext * ctx, const char * key, unsigned __int64 value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->setIntKey(ctx, key, value, expire, true);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetReal(ICodeContext * ctx, const char * key, double value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->setRealKey(ctx, key, value, expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetBool(ICodeContext * ctx, const char * key, bool value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncRSet(ctx, options, key, value, database, expire, password, timeout, cachedConnectionRequested);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetData(ICodeContext * ctx, const char * key, size32_t valueSize, const void * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncRSet(ctx, options, key, valueSize, value, database, expire, password, timeout, cachedConnectionRequested);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRSetUtf8(ICodeContext * ctx, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncRSet(ctx, options, key, rtlUtf8Size(valueLength, value), value, database, expire, password, timeout, cachedConnectionRequested);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL SyncRGetBool(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    bool value;
    SyncRGet(ctx, options, key, value, database, password, timeout, cachedConnectionRequested);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL SyncRGetDouble(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    double value;
    SyncRGetNumeric(ctx, options, key, value, database, password, timeout, cachedConnectionRequested);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL SyncRGetInt8(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    signed __int64 value;
    SyncRGetNumeric(ctx, options, key, value, database, password, timeout, cachedConnectionRequested);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL SyncRGetUint8(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    unsigned __int64 value;
    SyncRGetNumeric(ctx, options, key, value, database, password, timeout, cachedConnectionRequested);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout, cachedConnectionRequested);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    size_t returnSize;
    SyncRGet(ctx, options, key, returnSize, returnValue, database, password, timeout, cachedConnectionRequested);
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    size_t returnSize;
    SyncRGet(ctx, options, key, returnSize, returnValue, database, password, timeout, cachedConnectionRequested);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncRGetData(ICodeContext * ctx, size32_t & returnSize, void * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    size_t _returnSize;
    SyncRGet(ctx, options, key, _returnSize, returnValue, database, password, timeout, cachedConnectionRequested);
    returnSize = static_cast<size32_t>(_returnSize);
}
//----------------------------------LOCK------------------------------------------
//-----------------------------------SET-----------------------------------------
//Set pointer types
void SyncLockRSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueSize, const char * value, int database, unsigned expire, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, _options, DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->lockSet(ctx, key, valueSize, value, expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
//--INNER--
void Connection::lockSet(ICodeContext * ctx, const char * key, size32_t valueSize, const char * value, unsigned expire)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    handleLockOnSet(ctx, key, _value, (size_t)valueSize, expire);
}
//-------------------------------------------GET-----------------------------------------
//--OUTER--
void SyncLockRGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnSize, char * & returnValue, int database, unsigned expire, const char * password, unsigned _timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, _timeout, cachedConnectionRequested));
        master->lockGet(ctx, key, returnSize, returnValue, password, expire);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
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
bool Connection::lock(ICodeContext * ctx, const char * key, const char * channel, unsigned expire)
{
    if (expire == 0)
        fail("GetOrLock<type>", "invalid value for 'expire', persistent locks not allowed.", key);
    StringBuffer cmd("SET %b %b NX PX ");
    cmd.append(expire);

    OwnedReply reply = Reply::createReply(redisCommand(cmd.str(), key, strlen(key), channel, strlen(channel)));
    assertOnErrorWithCmdMsg(reply->query(), cmd.str(), key);

    return (reply->query()->type == REDIS_REPLY_STATUS && strcmp(reply->query()->str, "OK") == 0);
}
void Connection::unlock(ICodeContext * ctx, const char * key)
{
    //WATCH key, if altered between WATCH and EXEC abort all commands inbetween
    redisAppendCommand(context, "WATCH %b", key, strlen(key));
    redisAppendCommand(context, "GET %b", key, strlen(key));

    //Read replies
    OwnedReply reply = new Reply();
    readReplyAndAssertWithCmdMsg(reply.get(), "manual unlock", key);//WATCH reply
    readReplyAndAssertWithCmdMsg(reply.get(), "manual unlock", key);//GET reply

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
        readReplyAndAssertWithCmdMsg(reply.get(), "manual unlock", key);//MULTI reply
        readReplyAndAssertWithCmdMsg(reply.get(), "manual unlock", key);//DEL reply
        readReplyAndAssertWithCmdMsg(reply.get(), "manual unlock", key);//EXEC reply
    }
    else
    {
        reply->setClear(redisCommand("UNWATCH"));
        //Close connection upon failing to UNWATCH.
        if (!reply->query() || (reply->query()->type != REDIS_REPLY_STATUS) || (strcmp(reply->query()->str, "OK") != 0))
            freeContext();
    }
}
void Connection::handleLockOnGet(ICodeContext * ctx, const char * key, MemoryAttr * retVal, const char * password, unsigned expire)
{
    //NOTE: This routine can only return an empty string under one condition, that which indicates to the caller that the key was successfully locked.

    StringBuffer channel;
    encodeChannel(channel, key, database, true);

    //Query key and set lock if non existent
    if (lock(ctx, key, channel.str(), expire))
        return;

#if(0)//Test empty string handling by deleting the lock/value, and thus GET returns REDIS_REPLY_NIL as the reply type and an empty string.
    {
    OwnedReply pubReply = Reply::createReply(redisCommand("DEL %b", key, strlen(key)));
    assertOnError(pubReply->query(), "del fail");
    }
#endif

    //SUB before GET
    //Requires separate connection from GET so that the replies are not mangled. This could be averted but is not worth it.
    int _timeLeft = (int) timeLeft();//createConnection requires a timeout value, so create it here.
    if (_timeLeft == 0 && timeout.getTimeout() != 0)//Disambiguate between zero time left and timeout = 0 => infinity.
        rtlFail(0, "Redis Plugin: ERROR - function timed out internally.");

    ConnectionContainer subscriptionConnection;
    try
    {
        subscriptionConnection.setown(createConnection(ctx, cachedSubscriptionConnection, options.str(), ip.str(), port, false, 0, password, _timeLeft, canCacheConnections(isCachedConnection(), true), true));
        subscriptionConnection->subscribe(ctx, channel.str());

#if(0)//Test publish before GET.
        {
        OwnedReply pubReply = Reply::createReply(redisCommand("PUBLISH %b %b", channel.str(), (size_t)channel.length(), "foo", (size_t)3));
        assertOnError(pubReply->query(), "pub fail");
        }
#endif

        //Now GET
        OwnedReply getReply = Reply::createReply((redisReply*)redisCommand("GET %b", key, strlen(key)));
        assertOnErrorWithCmdMsg(getReply->query(), "GetOrLock<type>", key);

#if(0)//Test publish after GET.
        {
        OwnedReply pubReply = Reply::createReply(redisCommand("PUBLISH %b %b", channel.str(), (size_t)channel.length(), "foo", (size_t)3));
        assertOnError(pubReply->query(), "pub fail");
        }
#endif

        //Only return an actual value, i.e. neither the lock value nor an empty string. The latter is unlikely since we know that lock()
        //failed, indicating that the key existed. If this is an actual value, it is however, possible for it to have been DELeted in the interim.
        if (getReply->query()->type != REDIS_REPLY_NIL && getReply->query()->str && strncmp(getReply->query()->str, REDIS_LOCK_PREFIX, strlen(REDIS_LOCK_PREFIX)) != 0)
        {
            retVal->set(getReply->query()->len, getReply->query()->str);
            return;
        }
        else
        {
            //Check that the lock was set by this plugin and thus that we subscribed to the expected channel.
            if (getReply->query()->str && strcmp(getReply->query()->str, channel.str()) !=0 )
            {
                VStringBuffer msg("key locked with a channel ('%s') different to that subscribed to (%s).", getReply->query()->str, channel.str());
                fail("GetOrLock<type>", msg.str(), key);
                //MORE: In theory, it is possible to recover at this stage by subscribing to the channel that the key was actually locked with.
                //However, we may have missed the massage publication already or by then, but could SUB again in case we haven't.
                //More importantly and furthermore, the publication (in SetAndPublish<type>) will only publish to the channel encoded by
                //this plugin, rather than the string retrieved as the lock value (the value of the locked key).
            }
            getReply.clear();

#if(0)//Added to allow for manual pub testing via redis-cli
            struct timeval to = { 10, 0 };//10secs
            ::redisSetTimeout(subscriptionConnection->context, to);
#endif

            OwnedReply subReply = new Reply();
            subscriptionConnection->readReply(subReply);
            subscriptionConnection->assertOnErrorWithCmdMsg(subReply->query(), "GetOrLock<type>", key);

            if (subscriptionConnection->isCorrectChannel(subReply->query(), "message"))
            {
                //We are about to return a value, to conform with other Get<type> functions, fail if the key did not exist.
                //Since the value is sent via a published message, there is no direct reply struct so assume that an empty
                //string is equivalent to a non-existent key.
                //More importantly, it is paramount that this routine only return an empty string under one condition, that
                //which indicates to the caller that the key was successfully locked.
                //NOTE: it is possible for an empty message to have been PUBLISHed.
                if (subReply->query()->element[2]->len > 0)
                {
                    retVal->set(subReply->query()->element[2]->len, subReply->query()->element[2]->str);//return the published value rather than another (WATCHed) GET.
                    return;
                }
                //fail that key does not exist
                redisReply fakeReply;
                fakeReply.type = REDIS_REPLY_NIL;
                assertKey(&fakeReply, key);
            }
        }
        throwUnexpected();
    }
    catch (IException * error)
    {
        subscriptionConnection.handleException(error);
    }
}
void Connection::handleLockOnSet(ICodeContext * ctx, const char * key, const char * value, size_t size, unsigned expire)
{
    //Due to locking logic surfacing into ECL, any locking.set (such as this is) assumes that they own the lock and therefore go ahead and set regardless.
    StringBuffer channel;
    encodeChannel(channel, key, database, true);

    if (size > 29)//c.f. 1st note below.
    {
        OwnedReply replyContainer = new Reply();
        if (expire == 0)
        {
            const char * luaScriptSHA1 = "2a4a976d9bbd806756b2c7fc1e2bc2cb905e68c3"; //NOTE: update this if luaScript is updated!
            replyContainer->setClear(redisCommand("EVALSHA %b %d %b %b %b", luaScriptSHA1, (size_t)40, 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size));
            if (noScript(replyContainer->query()))
            {
                const char * luaScript = "redis.call('SET', KEYS[1], ARGV[2]) redis.call('PUBLISH', ARGV[1], ARGV[2]) return";//NOTE: MUST update luaScriptSHA1 if luaScript is updated!
                replyContainer->setClear(redisCommand("EVAL %b %d %b %b %b", luaScript, strlen(luaScript), 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size));
            }
        }
        else
        {
            const char * luaScriptWithExpireSHA1 = "6f6bc88ccea7c6853ccc395eaa7abd8cb91fb2d8"; //NOTE: update this if luaScriptWithExpire is updated!
            replyContainer->setClear(redisCommand("EVALSHA %b %d %b %b %b %d", luaScriptWithExpireSHA1, (size_t)40, 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size, expire));
            if (noScript(replyContainer->query()))
            {
                const char * luaScriptWithExpire = "redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) redis.call('PUBLISH', ARGV[1], ARGV[2]) return";//NOTE: MUST update luaScriptWithExpireSHA1 if luaScriptWithExpire is updated!
                replyContainer->setClear(redisCommand("EVAL %b %d %b %b %b %d", luaScriptWithExpire, strlen(luaScriptWithExpire), 1, key, strlen(key), channel.str(), (size_t)channel.length(), value, size, expire));
            }
        }
        assertOnErrorWithCmdMsg(replyContainer->query(), "SET", key);
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
        readReplyAndAssertWithCmdMsg(reply, "SET", key);//MULTI reply
        readReplyAndAssertWithCmdMsg(reply, "SET", key);//SET reply
        readReplyAndAssertWithCmdMsg(reply, "PUB for the key", key);//PUB reply
        readReplyAndAssertWithCmdMsg(reply, "SET", key);//EXEC reply
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
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    SyncLockRSet(ctx, options, key, valueLength, value, database, expire, password, timeout, cachedConnectionRequested);
    returnLength = valueLength;
    returnValue = (char*)allocateAndCopy(value, valueLength);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue, const char * key, size32_t valueLength, const UChar * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    unsigned valueSize = (valueLength)*sizeof(UChar);
    SyncLockRSet(ctx, options, key, valueSize, (char*)value, database, expire, password, timeout, cachedConnectionRequested);
    returnLength= valueLength;
    returnValue = (UChar*)allocateAndCopy(value, valueSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRSetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, size32_t valueLength, const char * value, const char * options, int database, unsigned expire, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    unsigned valueSize = rtlUtf8Size(valueLength, value);
    SyncLockRSet(ctx, options, key, valueSize, value, database, expire, password, timeout, cachedConnectionRequested);
    returnLength = valueLength;
    returnValue = (char*)allocateAndCopy(value, valueSize);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetStr(ICodeContext * ctx, size32_t & returnSize, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire, bool cachedConnectionRequested)
{
    size_t _returnSize;
    SyncLockRGet(ctx, options, key, _returnSize, returnValue, database, expire, password, timeout, cachedConnectionRequested);
    returnSize = static_cast<size32_t>(_returnSize);
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire, bool cachedConnectionRequested)
{
    size_t returnSize;
    char  * _returnValue;
    SyncLockRGet(ctx, options, key, returnSize, _returnValue, database, expire, password, timeout, cachedConnectionRequested);
    returnValue = (UChar*)_returnValue;
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, int database, const char * password, unsigned timeout, unsigned expire, bool cachedConnectionRequested)
{
    size_t returnSize;
    SyncLockRGet(ctx, options, key, returnSize, returnValue, database, expire, password, timeout, cachedConnectionRequested);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL SyncLockRUnlock(ICodeContext * ctx, const char * key, const char * options, int database, const char * password, unsigned timeout, bool cachedConnectionRequested)
{
    ConnectionContainer master;
    try
    {
        master.setown(Connection::createConnection(ctx, cachedConnection, options, DUMMY_IP, DUMMY_PORT, true, database, password, timeout, cachedConnectionRequested));
        master->unlock(ctx, key);
    }
    catch (IException * error)
    {
        master.handleException(error);
    }
}
}//close namespace
