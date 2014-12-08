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

StringBuffer & appendExpire(StringBuffer & buffer, unsigned expire)
{
    if (expire > 0)
        buffer.append(" EX ").append(expire*unitExpire);
    return buffer;
}

void RedisServer::parseOptions(ICodeContext * ctx, const char * _options)
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
Connection::Connection(ICodeContext * ctx, const char * _options, const char * pswd, unsigned __int64 _timeout) : alreadyInitialized(false), database(0), timeout(_timeout)
{
    server.set(new RedisServer(ctx, _options, pswd));
}
Connection::Connection(ICodeContext * ctx, RedisServer * _server) : alreadyInitialized(false), database(0), timeout(0)
{
    server.setown(_server);
}
bool Connection::isSameConnection(ICodeContext * ctx, unsigned hash) const
{
    return server->isSame(ctx, hash);
}
void * Connection::allocateAndCopy(const char * src, size_t size)
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
    alreadyInitialized = true;
}
}//close namespace


