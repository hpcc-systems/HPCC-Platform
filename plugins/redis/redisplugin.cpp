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
#include "redisplugin.hpp"

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
IPluginContext * parentCtx = NULL;

StringBuffer & appendExpire(StringBuffer & buffer, unsigned expire)
{
    if (expire > 0)
        buffer.append(" EX ").append(expire*unitExpire);
    return buffer;
}

Reply * createReply(void * _reply) { return new Reply(_reply); }

const char * Reply::typeToStr() const
{
    switch (reply->type)
    {
    case REDIS_REPLY_STATUS :
        return "REDIS_REPLY_STATUS";
    case REDIS_REPLY_INTEGER :
        return "REDIS_REPLY_INTEGER";
    case REDIS_REPLY_NIL :
            return "REDIS_REPLY_NIL";
    case REDIS_REPLY_STRING :
            return "REDIS_REPLY_STRING";
    case REDIS_REPLY_ERROR :
            return "REDIS_REPLY_ERROR";
    case REDIS_REPLY_ARRAY :
            return "REDIS_REPLY_ARRAY";
    default :
        return "UKNOWN";
    }
}

static CriticalSection crit;
static Owned<Connection> cachedConnection;

ECL_REDIS_API void setPluginContext(IPluginContext * ctx) { parentCtx = ctx; }

void RedisServer::parseOptions(ICodeContext * ctx, const char * _options)
{
    StringArray optionStrings;
    optionStrings.appendList(_options, " ");
    ForEachItemIn(idx, optionStrings)
    {
        const char *opt = optionStrings.item(idx);
        if (strncmp(opt, "--SERVER=", 9) ==0)
        {
            opt += 9;
            StringArray splitPort;
            splitPort.appendList(opt, ":");
            if (splitPort.ordinality()==2)
            {
                ip.set(splitPort.item(0));
                port = atoi(splitPort.item(1));
                return;
            }
        }
        else
        {
            VStringBuffer err("RedisPlugin: unsupported option string %s", opt);
            rtlFail(0, err.str());
        }
    }
    ip.set("localhost");
    port = 6379;
    if (!ctx)
        return;
    VStringBuffer msg("Redis Plugin: WARNING - using default server (%s:%d)", ip.str(), port);
    ctx->logString(msg.str());
}
Connection::Connection(ICodeContext * ctx, const char * _options) : alreadyInitialized(false), database(0)
{
    server.set(new RedisServer(ctx, _options));
}
Connection::Connection(ICodeContext * ctx, RedisServer * _server) : alreadyInitialized(false), database(0)
{
    server.setown(_server);
}
bool Connection::isSameConnection(ICodeContext * ctx, const RedisServer * requestedServer) const
{
    return server->isSame(ctx, requestedServer);
}
void * Connection::cpy(const char * src, size_t size)
{
    void * value = rtlMalloc(size);
    return memcpy(value, src, size);
}
const char * Connection::appendIfKeyNotFoundMsg(const redisReply * reply, const char * key, StringBuffer & target) const
{
    if (reply && reply->type == REDIS_REPLY_NIL)
        target.append("(key: '").append(key).append("') ");
    return target.str();
}
void Connection::init(ICodeContext * ctx)
{
    logServerStats(ctx);
}
}//close namespace


