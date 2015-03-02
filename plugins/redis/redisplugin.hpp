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

#ifndef ECL_REDIS_INCL
#define ECL_REDIS_INCL

#ifdef _WIN32
#define ECL_REDIS_CALL _cdecl
#ifdef ECL_REDIS_EXPORTS
#define ECL_REDIS_API __declspec(dllexport)
#else
#define ECL_REDIS_API __declspec(dllimport)
#endif
#else
#define ECL_REDIS_CALL
#define ECL_REDIS_API
#endif

#include "jhash.hpp"
#include "hqlplugins.hpp"
#include "eclhelper.hpp"
#include "jexcept.hpp"
#include "hiredis/hiredis.h"

extern "C"
{
    ECL_REDIS_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
    ECL_REDIS_API void setPluginContext(IPluginContext * _ctx);
}

class StringBuffer;

namespace RedisPlugin {
StringBuffer & appendExpire(StringBuffer & buffer, unsigned expire);

class RedisServer : public CInterface
{
public :
    RedisServer(ICodeContext * ctx, const char * _options, const char * pswd)
    {
        serverIpPortPasswordHash = hashc((const unsigned char*)pswd, strlen(pswd), 0);
        serverIpPortPasswordHash = hashc((const unsigned char*)_options, strlen(_options), serverIpPortPasswordHash);
        options.set(_options);
        parseOptions(ctx, _options);
    }
    bool isSame(ICodeContext * ctx, const char * password) const
    {
        unsigned hash = hashc((const unsigned char*)options.str(), options.length(), hashc((const unsigned char*)password, strlen(password), 0));
        return (serverIpPortPasswordHash == hash);
    }
    const char * getIp() { return ip.str(); }
    int getPort() { return port; }
    void parseOptions(ICodeContext * ctx, const char * _options);

private :
    unsigned serverIpPortPasswordHash;
    StringAttr options;
    StringAttr ip;
    int port;
};
class Connection : public CInterface
{
public :
    Connection(ICodeContext * ctx, const char * _options, const char * pswd, unsigned __int64 _timeout);
    Connection(ICodeContext * ctx, RedisServer * _server,  const char * pswd, unsigned __int64 _timeout);

    const char * ip() const { return server->getIp(); }
    int port() const { return server->getPort(); }
    unsigned __int64 getTimeout() const { return timeout; }
    bool isSameConnection(ICodeContext * ctx, const char * password) const;

protected :
    virtual void assertOnError(const redisReply * reply, const char * _msg) { }
    virtual void assertConnection() { }
    virtual void logServerStats(ICodeContext * ctx) { }
    virtual void updateTimeout(unsigned __int64 _timeout) { }

    void * allocateAndCopy(const char * src, size_t size);
    void init(ICodeContext * ctx);

protected :
    Owned<RedisServer> server;
    unsigned __int64 timeout;
    unsigned __int64 database;
    bool alreadyInitialized;
};

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

}//close namespace

#endif
