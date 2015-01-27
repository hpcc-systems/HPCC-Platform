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
#include "hiredis/adapters/libev.h"

#include "platform.h"
#include "eclrtl.hpp"
#include "jstring.hpp"
#include "jsem.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"
#include "jmutex.hpp"
#include "redisplugin.hpp"
#include "redissync.hpp"
#include "redisasync.hpp"

#include "hiredis/async.h"

namespace Lock
{
static CriticalSection crit;
static const char * REDIS_LOCK_PREFIX = "redis_ecl_lock";// needs to be a large random value uniquely individual per client
static const unsigned REDIS_LOCK_EXPIRE = 60; //(secs)
static const unsigned REDIS_MAX_LOCKS = 9999;

class KeyLock : public CInterface
{
public :
    KeyLock(ICodeContext * ctx, const char * _options, const char * _key, const char * _channel, unsigned __int64 _database);
    ~KeyLock();

    inline const char * getKey()     const { return key.str(); }
    inline const char * getOptions() const { return options.str(); }
    inline const char * getChannel()  const { return channel.str(); }
    inline unsigned __int64 getDatabase()  const { return database; }

private :
    StringAttr options; //shouldn't be needed, tidy isSameConnection to pass 'master' & 'port'
    StringAttr master;
    int port;
    unsigned __int64 database;
    StringAttr key;
    StringAttr channel;
};
KeyLock::KeyLock(ICodeContext * ctx, const char * _options, const char * _key, const char * _channel, unsigned __int64 _database)
{
    options.set(_options);
    key.set(_key);
    channel.set(_channel);
    database =_database;
    RedisPlugin::parseOptions(ctx, _options, master, port);
}
KeyLock::~KeyLock()
{
    redisContext * context = redisConnectWithTimeout(master, port, RedisPlugin::REDIS_TIMEOUT);
    CriticalBlock block(crit);
    OwnedReply reply = RedisPlugin::createReply(redisCommand(context, getCmd, key.str(), strlen(key.str())));
    const char * channel2 = reply->query()->str;

    if (strcmp(channel, channel2) == 0)
        OwnedReply reply = RedisPlugin::createReply(redisCommand(context, "DEL %b", key.str(), strlen(key.str())));

    if (context)
        redisFree(context);
}
}//close Lock Namespace

namespace Async
{
static CriticalSection crit;
static const unsigned REDIS_TIMEOUT = 2000;//ms
static const ev_tstamp EV_TIMEOUT = REDIS_TIMEOUT/1000.;//secs
class SubContainer;

class ReturnValue : public CInterface
{
public :
    ReturnValue() : size(0), value(NULL) { }
    ~ReturnValue()
    {
        if (value)
            free(value);
    }

    void cpy(size_t _size, const char * _value) { size = _size; value = (char*)malloc(size); memcpy(value, _value, size); }
    inline size_t getSize() const { return size; }
    inline const char * str() const { return value; }
    char * getStr() { char * tmp = value; value = NULL; size = 0; return tmp; }
    inline bool isEmpty() const { return size == 0; }//!value||!*value; }

private :
    size_t size;
    char * value;
};
class Connection : public RedisPlugin::Connection
{
public :
    Connection(ICodeContext * ctx, const char * _options, unsigned __int64 _database);
    ~Connection();

    //get
    void get(ICodeContext * ctx, const char * key, ReturnValue * retVal, const char * channel);
    //template <class type> void get(ICodeContext * ctx, const char * key, type & value, const char * channel);
    template <class type> void get(ICodeContext * ctx, const char * key, size_t & valueLength, type * & value, const char * channel);
    void getVoidPtrLenPair(ICodeContext * ctx, const char * key, size_t & valueLength, void * & value, const char * channel);
    //set
    template<class type> void set(ICodeContext * ctx, const char * key, type value, unsigned expire, const char * channel);
    template<class type> void set(ICodeContext * ctx, const char * key, size32_t valueLength, const type * value, unsigned expire, const char * channel);

    bool missThenLock(ICodeContext * ctx, Lock::KeyLock * keyPtr);
    static void assertContextErr(const redisAsyncContext * context);

protected :
    virtual void selectDb(ICodeContext * ctx);
    virtual void assertOnError(const redisReply * reply, const char * _msg);
    virtual void assertConnection();
    void createAndAssertConnection(ICodeContext * ctx);
    void assertRedisErr(int reply, const char * _msg);
    void handleLockForGet(ICodeContext * ctx, const char * key, const char * channel, ReturnValue * retVal);
    void handleLockForSet(ICodeContext * ctx, const char * key, const char * value, size_t size, const char * channel, unsigned expire);
    void subscribe(const char * channel, StringAttr & value);
    void unsubscribe(const char * channel);
    void attachLibev();
    bool lock(const char * key, const char * channel);
    void handleLoop(struct ev_loop * evLoop, ev_tstamp timeout);

    //callbacks
    static void assertCallbackError(const redisAsyncContext * context, const redisReply * reply, const char * _msg);
    static void connectCB(const redisAsyncContext *c, int status);
    static void disconnectCB(const redisAsyncContext *c, int status);
    static void subCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void pubCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void setCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void getCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void unsubCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void setLockCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void selectCB(redisAsyncContext * context, void * _reply, void * privdata);
    static void timeoutCB(struct ev_loop * evLoop, ev_timer *w, int revents);

protected :
    redisAsyncContext * context;
};

class SubContainer : public Async::Connection
{
public :
    SubContainer(ICodeContext * ctx, const char * options, const char * _channel, unsigned __int64 _database);
    SubContainer(ICodeContext * ctx, const char * options, const char * _channel, redisCallbackFn * _callback, unsigned __int64 _database);
    ~SubContainer()
    {
        unsubscribe();
    }

    inline void setCB(redisCallbackFn * _callback) { callback = _callback; }
    inline redisCallbackFn * getCB() { return callback; }
    inline void setChannel(const char * _channel) { channel.set(_channel); }
    inline const char * getChannel() { return channel.str(); }
    inline void setMessage(const char * _message) { message.set(_message); }
    inline const char * getMessage() { return message.str(); }
    inline void wait(unsigned timeout) { msgSem.wait(timeout); }
    inline void signal() { msgSem.signal(); }
    inline void subActivated() { subActiveSem.signal(); }
    inline void subActivationWait(unsigned timeout) { subActiveSem.wait(timeout); }
    void subscribe(struct ev_loop * evLoop);
    void unsubscribe();

    void stopEvLoop()
    {
        CriticalBlock block(crit);
        context->ev.delRead((void*)context->ev.data);
        context->ev.delWrite((void*)context->ev.data);
    }
    //callback
    static void subCB(redisAsyncContext * context, void * _reply, void * privdata);

protected :
    Semaphore msgSem;
    Semaphore subActiveSem;
    redisCallbackFn * callback;
    StringAttr message;
    StringAttr channel;
};

class SubscriptionThread : implements IThreaded, implements IInterface, public SubContainer
{
public :
    SubscriptionThread(ICodeContext * ctx, const char * options, const char * channel, unsigned __int64 _database) : SubContainer(ctx, options, channel, _database), thread("SubscriptionThread", (IThreaded*)this)
    {
        evLoop = NULL;
    }
    virtual ~SubscriptionThread()
    {
        stopEvLoop();
        wait(REDIS_TIMEOUT);//give stopEvLoop time to complete
        thread.stopped.signal();
        thread.join();
    }

    void start()
    {
        thread.start();
        subActivationWait(REDIS_TIMEOUT);//wait for subscription to be acknowledged by redis
    }

    IMPLEMENT_IINTERFACE;

private :
    void main()
    {
        evLoop = ev_loop_new(0);
        subscribe(evLoop);
        signal();
    }

private :
    CThreaded  thread;
    struct ev_loop * evLoop;
};
SubContainer::SubContainer(ICodeContext * ctx, const char * options, const char * _channel, unsigned __int64 _database) : Connection(ctx, options, _database)
{
    channel.set(_channel);
    callback = subCB;
}
SubContainer::SubContainer(ICodeContext * ctx, const char * options, const char * _channel, redisCallbackFn * _callback, unsigned __int64 _database) : Connection(ctx, options,_database )
{
    channel.set(_channel);
    callback = _callback;
}
Connection::Connection(ICodeContext * ctx, const char * _options, unsigned __int64 _database) : RedisPlugin::Connection(ctx, _options, _database)
{
    createAndAssertConnection(ctx);
    //could log server stats here, however async connections are not cached and therefore book keeping of only doing so for new servers may not be worth it.
}
void Connection::selectDb(ICodeContext * ctx)
{
    if (database == 0)
        return;
    attachLibev();
    VStringBuffer cmd("SELECT %llu", database);
    assertRedisErr(redisAsyncCommand(context, selectCB, NULL, cmd.str()), "SELECT (lock) buffer write error");
    handleLoop(EV_DEFAULT_ EV_TIMEOUT);
}
Connection::~Connection()
{
    if (context)
    {
        //redis can auto disconnect upon certain errors, disconnectCB is called to handle this and is automatically
        //disconnected after this freeing the context
        if (context->err == REDIS_OK)
            redisAsyncDisconnect(context);
    }
}
void Connection::createAndAssertConnection(ICodeContext * ctx)
{
    context = NULL;
    context = redisAsyncConnect(master.str(), port);
    assertConnection();
    context->data = (void*)this;
    assertRedisErr(redisAsyncSetConnectCallback(context, connectCB), "failed to set connect callback");
    assertRedisErr(redisAsyncSetDisconnectCallback(context, disconnectCB), "failed to set disconnect callback");
    selectDb(ctx);
}
void Connection::assertConnection()
{
    if (!context)
        rtlFail(0, "Redis Plugin: async context mem alloc fail.");
    assertContextErr(context);
}
void Connection::assertContextErr(const redisAsyncContext * context)
{
    if (context && context->err)
    {
        const Connection * connection = (const Connection*)context->data;
        if (connection)
        {
            VStringBuffer msg("Redis Plugin : failed to create connection context for %s:%d - %s", connection->getMaster(), connection->getPort(), context->errstr);
            rtlFail(0, msg.str());
        }
        {
            VStringBuffer msg("Redis Plugin : failed to create connection context - %s", context->errstr);
            rtlFail(0, msg.str());
        }
    }
}
void Connection::connectCB(const redisAsyncContext * context, int status)
{
    if (status != REDIS_OK)
    {
        if (context->data)
        {
            const Connection * connection = (Connection *)context->data;
            VStringBuffer msg("Redis Plugin : failed to connect to %s:%d - %s", connection->master.str(), connection->port, context->errstr);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin : failed to connect - %s", context->errstr);
            rtlFail(0, msg.str());
        }
    }
}
void Connection::disconnectCB(const redisAsyncContext * context, int status)
{
    if (status != REDIS_OK)//&& context->err != 2)//err = 2:  ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this contex
    {
        if (context->data)
        {
            const Connection  * connection = (Connection*)context->data;
            VStringBuffer msg("Redis Plugin : server (%s:%d) forced disconnect - %s", connection->master.str(), connection->port, context->errstr);
            rtlFail(0, msg.str());
        }
        else
        {
            VStringBuffer msg("Redis Plugin : server forced disconnect - %s", context->errstr);
            rtlFail(0, msg.str());
        }
    }
}
Connection * createConnection(ICodeContext * ctx, const char * options, unsigned __int64 database)
{
    return new Connection(ctx, options, database);
}
void Connection::assertRedisErr(int reply, const char * _msg)
{
    if (reply != REDIS_OK)
    {
        VStringBuffer msg("Redis Plugin: %s", _msg);
        rtlFail(0, msg.str());
    }
}
void Connection::assertOnError(const redisReply * reply, const char * _msg)
{
    if (!reply)
    {
        assertConnection();
        //There should always be a connection error
        VStringBuffer msg("Redis Plugin: %s%s", _msg, "no 'reply' nor connection error");
        rtlFail(0, msg.str());
    }
    else if (reply->type == REDIS_REPLY_ERROR)
    {
        VStringBuffer msg("Redis Plugin: %s%s", _msg, reply->str);
        rtlFail(0, msg.str());
    }
}
void Connection::assertCallbackError(const redisAsyncContext * context, const redisReply * reply, const char * _msg)
{
    if (reply && reply->type == REDIS_REPLY_ERROR)
    {
        VStringBuffer msg("Redis Plugin: %s%s", _msg, reply->str);
        rtlFail(0, msg.str());
    }
    assertContextErr(context);
}
void Connection::timeoutCB(struct ev_loop * evLoop, ev_timer *w, int revents)
{

    rtlFail(0, "Redis Plugin : async operation timed out");
}
void Connection::handleLoop(struct ev_loop * evLoop, ev_tstamp timeout)
{
    ev_timer timer;
    ev_timer_init(&timer, timeoutCB, timeout, 0.);
    ev_timer_again(evLoop, &timer);
    ev_run(evLoop, 0);
}

//Async callbacks-----------------------------------------------------------------
void SubContainer::subCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    if (_reply == NULL || privdata == NULL)
        return;

    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "callback fail");

    if (reply->type == REDIS_REPLY_ARRAY)
    {
        Async::SubContainer * holder = (Async::SubContainer*)privdata;
        if (strcmp("subscribe", reply->element[0]->str) == 0 )
            holder->subActivated();
        else if (strcmp("message", reply->element[0]->str) == 0 )
        {
            holder->setMessage(reply->element[2]->str);
            const char * channel = reply->element[1]->str;
            redisAsyncCommand(context, NULL, NULL, "UNSUBSCRIBE %b", channel, strlen(channel));
            redisAsyncHandleWrite(context);
            context->ev.delRead((void*)context->ev.data);
        }
    }
}
void Connection::subCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    if (_reply == NULL)
        return;

    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "callback fail");

    if (reply->type == REDIS_REPLY_ARRAY && strcmp("message", reply->element[0]->str) == 0 )
    {
        ((StringAttr*)privdata)->set(reply->element[2]->str);
        const char * channel = reply->element[1]->str;
        redisAsyncCommand(context, NULL, NULL, "UNSUBSCRIBE %b", channel, strlen(channel));
        redisLibevDelRead((void*)context->ev.data);
    }
}
void Connection::unsubCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "get callback fail");
    redisLibevDelRead((void*)context->ev.data);
}
void Connection::getCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "get callback fail");

    ReturnValue * retVal = (ReturnValue*)privdata;
    retVal->cpy((size_t)reply->len, reply->str);
    redisLibevDelRead((void*)context->ev.data);
}
void Connection::setCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "set callback fail");
    redisLibevDelRead((void*)context->ev.data);
}
void Connection::setLockCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "set lock callback fail");

    switch(reply->type)
    {
    case REDIS_REPLY_STATUS :
        *(bool*)privdata = strcmp(reply->str, "OK") == 0;
        break;
    case REDIS_REPLY_NIL :
        *(bool*)privdata = false;
    }
    redisLibevDelRead((void*)context->ev.data);
}
void Connection::selectCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "select callback fail");
    redisLibevDelRead((void*)context->ev.data);
}

void Connection::pubCB(redisAsyncContext * context, void * _reply, void * privdata)
{
    redisReply * reply = (redisReply*)_reply;
    assertCallbackError(context, reply, "get callback fail");
    redisLibevDelRead((void*)context->ev.data);
}
void Connection::unsubscribe(const char * channel)
{
    attachLibev();
    assertRedisErr(redisAsyncCommand(context, NULL, NULL, "UNSUBSCRIBE %b", channel, strlen(channel)), "UNSUBSCRIBE buffer write error");
    redisAsyncHandleWrite(context);
}
void Connection::subscribe(const char * channel, StringAttr & value)
{
    attachLibev();
    assertRedisErr(redisAsyncCommand(context, subCB, (void*)&value, "SUBSCRIBE %b", channel, strlen(channel)), "SUBSCRIBE buffer write error");
    handleLoop(EV_DEFAULT_ EV_TIMEOUT);
}
void SubContainer::subscribe(struct ev_loop * evLoop)
{
    assertRedisErr(redisLibevAttach(evLoop, context), "failure to attach to libev");
    assertRedisErr(redisAsyncCommand(context, callback, (void*)this, "SUBSCRIBE %b", channel.str(), channel.length()), "SUBSCRIBE buffer write error");
    handleLoop(evLoop, EV_TIMEOUT);
}
void SubContainer::unsubscribe()
{
    attachLibev();
    assertRedisErr(redisAsyncCommand(context, NULL, NULL, "UNSUBSCRIBE %b", channel.str(), channel.length()), "UNSUBSCRIBE buffer write error");
    redisAsyncHandleWrite(context);
}
bool Connection::missThenLock(ICodeContext * ctx, Lock::KeyLock * keyPtr)
{
    return lock(keyPtr->getKey(), keyPtr->getChannel());
}
void Connection::attachLibev()
{
    if (context->ev.data)
        return;
    assertRedisErr(redisLibevAttach(EV_DEFAULT_ context), "failure to attach to libev");
}
bool Connection::lock(const char * key, const char * channel)
{
    StringBuffer cmd("SET %b %b NX EX ");
    cmd.append(Lock::REDIS_LOCK_EXPIRE);

    bool locked = false;
    attachLibev();
    assertRedisErr(redisAsyncCommand(context, setLockCB, (void*)&locked, cmd.str(), key, strlen(key), channel, strlen(channel)), "SET NX (lock) buffer write error");
    handleLoop(EV_DEFAULT_ EV_TIMEOUT);
    return locked;
}
void Connection::handleLockForGet(ICodeContext * ctx, const char * key, const char * channel, ReturnValue * retVal)
{
    bool ignoreLock = (channel == NULL);//function was not passed with channel => called from non-locking async routines
    Owned<SubscriptionThread> subThread;//thread to hold subscription event loop
    if (!ignoreLock)
    {
        subThread.set(new SubscriptionThread(ctx, options.str(), channel, database));
        subThread->start();//subscribe and wait for 1st callback that redis received sub. Do not block for main message callback this is the point of the thread.
    }

    attachLibev();
    assertRedisErr(redisAsyncCommand(context, getCB, (void*)retVal, "GET %b", key, strlen(key)), "GET buffer write error");
    handleLoop(EV_DEFAULT_ EV_TIMEOUT);

    if (ignoreLock)
        return;//with value just retrieved regardless of success (handled by caller)

    if (retVal->isEmpty())//cache miss
    {
        if (lock(key, channel))
            return;//race winner
        else
            subThread->wait(REDIS_TIMEOUT);//race losers
    }
    else //contents found
    {
        //check if already locked
        if (strncmp(retVal->str(), Lock::REDIS_LOCK_PREFIX, strlen(Lock::REDIS_LOCK_PREFIX)) == 0 )
            subThread->wait(REDIS_TIMEOUT);//locked so lets subscribe for value
        else
            return;//normal GET
    }
}
void Connection::handleLockForSet(ICodeContext * ctx, const char * key, const char * value, size_t size, const char * channel, unsigned expire)
{
    StringBuffer cmd(setCmd);
    RedisPlugin::appendExpire(cmd, expire);
    attachLibev();
    if(channel)//acknowledge locks i.e. for use as the race winner and lock owner
    {
        //Due to locking logic surfacing into ECL, any locking.set (such as this is) assumes that they own the lock and therefore just go ahead and set
        //It is possible for a process/call to 'own' a lock and store this info in the LockObject, however, this prevents sharing between clients.
        assertRedisErr(redisAsyncCommand(context, setCB, NULL, cmd.str(), key, strlen(key), value, size), "SET buffer write error");
        handleLoop(EV_DEFAULT_ EV_TIMEOUT);//not theoretically necessary as subscribers receive value from published message. In addition,
        //the logic stated above allows for any other client to set and therefore as soon as this is set it can be instantly altered.
        //However, this is here as libev handles socket io to/from redis.
        assertRedisErr(redisAsyncCommand(context, pubCB, NULL, "PUBLISH %b %b", channel, strlen(channel), value, size), "PUBLISH buffer write error");
        handleLoop(EV_DEFAULT_ EV_TIMEOUT);//this only waits to ensure redis received pub cmd. MORE: reply contains number of subscribers on that channel - utilise this?
    }
    else
    {
        ReturnValue retVal;
        //This branch represents a normal async get i.e. no lockObject present and no channel
        //There are two primary options that could be taken    1) set regardless of lock (publish if it was locked)
        //                                                     2) Only set if not locked
        //(1)
        StringBuffer cmd2("GETSET %b %b");
        RedisPlugin::appendExpire(cmd2, expire);
        //obtain channel
        assertRedisErr(redisAsyncCommand(context, getCB, (void*)&retVal, cmd2.str(), key, strlen(key), value, size), "SET buffer write error");
        handleLoop(EV_DEFAULT_ EV_TIMEOUT);
        if (strncmp(retVal.str(), Lock::REDIS_LOCK_PREFIX, strlen(Lock::REDIS_LOCK_PREFIX)) == 0 )
        {
            assertRedisErr(redisAsyncCommand(context, pubCB, NULL, "PUBLISH %b %b", channel, strlen(channel), value, size), "PUBLISH buffer write error");
            handleLoop(EV_DEFAULT_ EV_TIMEOUT);//again not necessary, could just call redisAsyncHandleWrite(context);
        }
    }
}
//-------------------------------------------GET-----------------------------------------
//---OUTER---
template<class type> void RGet(ICodeContext * ctx, const char * options, const char * key, type & returnValue, const char * channel, unsigned __int64 database)
{
    Owned<Async::Connection> master = Async::createConnection(ctx, options, database);
    ReturnValue retVal;
    master->get(ctx, key, &retVal, channel);
    StringBuffer keyMsg = getFailMsg;

    size_t returnSize = retVal.getSize();
    if (sizeof(type)!=returnSize)
    {
        VStringBuffer msg("RedisPlugin: ERROR - Requested type of different size (%uB) from that stored (%uB).", (unsigned)sizeof(type), (unsigned)returnSize);
        rtlFail(0, msg.str());
    }
    memcpy(&returnValue, retVal.str(), returnSize);
}
template<class type> void RGet(ICodeContext * ctx, const char * options, const char * key, size_t & returnLength, type * & returnValue, const char * channel, unsigned __int64 database)
{
    Owned<Async::Connection> master = Async::createConnection(ctx, options, database);
    master->get(ctx, key, returnLength, returnValue, channel);
}
void RGetVoidPtrLenPair(ICodeContext * ctx, const char * options, const char * key, size_t & returnLength, void * & returnValue, const char * channel, unsigned __int64 database)
{
    Owned<Async::Connection> master = Async::createConnection(ctx, options, database);
    master->getVoidPtrLenPair(ctx, key, returnLength, returnValue, channel);
}
//---INNER---
void Connection::get(ICodeContext * ctx, const char * key, ReturnValue * retVal, const char * channel )
{
    handleLockForGet(ctx, key, channel, retVal);
}
template<class type> void Connection::get(ICodeContext * ctx, const char * key, size_t & returnSize, type * & returnValue, const char * channel)
{
    ReturnValue retVal;
    handleLockForGet(ctx, key, channel, &retVal);

    returnSize = retVal.getSize();
    returnValue = reinterpret_cast<type*>(retVal.getStr());
}
void Connection::getVoidPtrLenPair(ICodeContext * ctx, const char * key, size_t & returnSize, void * & returnValue, const char * channel )
{
    ReturnValue retVal;
    handleLockForGet(ctx, key, channel, &retVal);

    returnSize = retVal.getSize();
    returnValue = reinterpret_cast<void*>(cpy(retVal.str(), returnSize));
}
//-------------------------------------------SET-----------------------------------------
//---OUTER---
template<class type> void RSet(ICodeContext * ctx, const char * _options, const char * key, type value, unsigned expire, const char * channel, unsigned __int64 database)
{
    Owned<Connection> master = Async::createConnection(ctx, _options, database);
    master->set(ctx, key, value, expire, channel);
}
//Set pointer types
template<class type> void RSet(ICodeContext * ctx, const char * _options, const char * key, size32_t valueLength, const type * value, unsigned expire, const char * channel, unsigned __int64 database)
{
    Owned<Connection> master = Async::createConnection(ctx, _options, database);
    master->set(ctx, key, valueLength, value, expire, channel);
}
//---INNER---
template<class type> void Connection::set(ICodeContext * ctx, const char * key, type value, unsigned expire, const char * channel)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining
    handleLockForSet(ctx, key, _value, sizeof(type), channel, expire);
}
template<class type> void Connection::set(ICodeContext * ctx, const char * key, size32_t valueSize, const type * value, unsigned expire, const char * channel)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    handleLockForSet(ctx, key, _value, (size_t)valueSize, channel, expire);
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL RSetStr(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, valueLength, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetUChar(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const UChar * value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, (valueLength)*sizeof(UChar), value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetInt(ICodeContext * ctx, const char * options, const char * key, signed __int64 value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetUInt(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetReal(ICodeContext * ctx, const char * options, const char * key, double value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetBool(ICodeContext * ctx, const char * options, const char * key, bool value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetData(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const void * value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, valueLength, value, expire, NULL, database);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetUtf8(ICodeContext * ctx, const char * options, const char * key, size32_t valueLength, const char * value, unsigned __int64 database, unsigned expire)
{
    Async::RSet(ctx, options, key, rtlUtf8Size(valueLength, value), value, expire, NULL, database);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL RGetBool(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    bool value;
    Async::RGet(ctx, options, key, value, NULL, database);
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL RGetDouble(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    double value;
    Async::RGet(ctx, options, key, value, NULL, database);
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL RGetInt8(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    signed __int64 value;
    Async::RGet(ctx, options, key, value, NULL, database);
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetUint8(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    unsigned __int64 value;
    Async::RGet(ctx, options, key, value, NULL, database);
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL RGetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t _returnLength;
    Async::RGet(ctx, options, key, _returnLength, returnValue, NULL, database);
    returnLength = static_cast<size32_t>(_returnLength);
}
ECL_REDIS_API void ECL_REDIS_CALL RGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * options, const char * key, unsigned __int64 database)
{
    size_t returnSize = 0;
    Async::RGet(ctx, options, key, returnSize, returnValue, NULL, database);
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL RGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t returnSize = 0;
    Async::RGet(ctx, options, key, returnSize, returnValue, NULL, database);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL RGetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, const char * options, const char * key, unsigned __int64 database)
{
    size_t _returnLength = 0;
    Async::RGet(ctx, options, key, _returnLength, returnValue, NULL, database);
    returnLength = static_cast<size32_t>(_returnLength);
}
}//close Async namespace
//----------------------------------------------------------------------------------------------------------------------------------------------------
namespace Lock {
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetLockObject(ICodeContext * ctx, const char * options, const char * key, unsigned __int64 database)
{
    Owned<KeyLock> keyPtr;
    StringBuffer channel;
    channel.set(Lock::REDIS_LOCK_PREFIX);
    channel.append("_").append(key).append("_");
    channel.append("_").append(database).append("_");

   // CriticalBlock block(crit);
    //srand(unsigned int seed);
    channel.append(rand()%REDIS_MAX_LOCKS+1);

    keyPtr.set(new Lock::KeyLock(ctx, options, key, channel.str(), database));
    return reinterpret_cast<unsigned long long>(keyPtr.get());
}
ECL_REDIS_API bool ECL_REDIS_CALL RMissThenLock(ICodeContext * ctx, unsigned __int64 _keyPtr)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    const char * channel = keyPtr->getChannel();
    if (!keyPtr || strlen(channel) == 0)
    {
        VStringBuffer msg("Redis Plugin : ERROR 'Locking.ExistLockSub' called without sufficient LockObject.");
        rtlFail(0, msg.str());
    }
    const char * options = keyPtr->getOptions();
    Owned<Async::Connection> master = Async::createConnection(ctx, options, keyPtr->getDatabase());
    return master->missThenLock(ctx, keyPtr);
}
//-----------------------------------SET------------------------------------------
ECL_REDIS_API void ECL_REDIS_CALL RSetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 _keyPtr, size32_t valueLength, const char * value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), valueLength, value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = valueLength;
    memcpy(&returnValue, value, returnLength);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue, unsigned __int64 _keyPtr, size32_t valueLength, const UChar * value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), (valueLength)*sizeof(UChar), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = valueLength;
    memcpy(&returnValue, value, returnLength);
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL RSetInt(ICodeContext * ctx, unsigned __int64 _keyPtr, signed __int64 value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API  unsigned __int64 ECL_REDIS_CALL RSetUInt(ICodeContext * ctx, unsigned __int64 _keyPtr, unsigned __int64 value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL RSetReal(ICodeContext * ctx, unsigned __int64 _keyPtr, double value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API bool ECL_REDIS_CALL RSetBool(ICodeContext * ctx, unsigned __int64 _keyPtr,  bool value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL RSetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, unsigned __int64 _keyPtr, size32_t valueLength, const void * value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), valueLength, value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = valueLength;
    memcpy(&returnValue, value, returnLength);
}
ECL_REDIS_API void ECL_REDIS_CALL RSetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 _keyPtr, size32_t valueLength, const char * value, unsigned expire)
{
    KeyLock * keyPtr = (KeyLock*)_keyPtr;
    Async::RSet(ctx, keyPtr->getOptions(), keyPtr->getKey(), rtlUtf8Size(valueLength, value), value, expire, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = valueLength;
    memcpy(&returnValue, value, returnLength);
}
//-------------------------------------GET----------------------------------------
ECL_REDIS_API bool ECL_REDIS_CALL RGetBool(ICodeContext * ctx, unsigned __int64 _keyPtr)
{
    bool value;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API double ECL_REDIS_CALL RGetDouble(ICodeContext * ctx, unsigned __int64 _keyPtr)
{
    double value;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API signed __int64 ECL_REDIS_CALL RGetInt8(ICodeContext * ctx, unsigned __int64 _keyPtr)
{
    signed __int64 value;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API unsigned __int64 ECL_REDIS_CALL RGetUint8(ICodeContext * ctx, unsigned __int64 _keyPtr)
{
    unsigned __int64 value;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), value, keyPtr->getChannel(), keyPtr->getDatabase());
    return value;
}
ECL_REDIS_API void ECL_REDIS_CALL RGetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 _keyPtr)
{
    size_t _returnLength;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), _returnLength, returnValue, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = static_cast<size32_t>(_returnLength);
}
ECL_REDIS_API void ECL_REDIS_CALL RGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  unsigned __int64 _keyPtr)
{
    size_t returnSize = 0;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), returnSize, returnValue, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = static_cast<size32_t>(returnSize/sizeof(UChar));
}
ECL_REDIS_API void ECL_REDIS_CALL RGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, unsigned __int64 _keyPtr)
{
    size_t returnSize = 0;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), returnSize, returnValue, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_REDIS_API void ECL_REDIS_CALL RGetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, unsigned __int64 _keyPtr)
{
    size_t _returnLength = 0;
    Lock::KeyLock * keyPtr = (Lock::KeyLock*)_keyPtr;
    Async::RGet(ctx, keyPtr->getOptions(), keyPtr->getKey(), _returnLength, returnValue, keyPtr->getChannel(), keyPtr->getDatabase());
    returnLength = static_cast<size32_t>(_returnLength);
}
}//close Lock namespace
