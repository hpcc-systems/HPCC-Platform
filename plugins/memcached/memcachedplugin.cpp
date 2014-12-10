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
#include "memcachedplugin.hpp"
#include "eclrtl.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include "workunit.hpp"
#include <libmemcached/memcached.hpp>
#include <libmemcached/util.h>

#define MEMCACHED_VERSION "memcached plugin 1.0.0"

ECL_MEMCACHED_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = MEMCACHED_VERSION;
    pb->moduleName = "lib_memcached";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for the C/C++ API libmemcached (http://libmemcached.org/)\n";
    return true;
}

namespace MemCachedPlugin {
IPluginContext * parentCtx = NULL;
static const unsigned unitExpire = 86400;//1 day (secs)

enum eclDataType {
    ECL_BOOLEAN,
    ECL_DATA,
    ECL_INTEGER,
    ECL_REAL,
    ECL_STRING,
    ECL_UTF8,
    ECL_UNICODE,
    ECL_UNSIGNED,
    ECL_NONE
};

const char * enumToStr(eclDataType type)
{
    switch(type)
    {
    case ECL_BOOLEAN:
        return "BOOLEAN";
    case ECL_INTEGER:
        return "INTEGER";
    case ECL_UNSIGNED:
        return "UNSIGNED";
    case ECL_REAL:
        return "REAL";
    case ECL_STRING:
        return "STRING";
    case ECL_UTF8:
        return "UTF8";
    case ECL_UNICODE:
        return "UNICODE";
    case ECL_DATA:
        return "DATA";
    case ECL_NONE:
        return "Nonexistent";
    default:
        return "UNKNOWN";
    }
}

class MCached : public CInterface
{
public :
    MCached(ICodeContext * ctx, const char * servers);
    ~MCached();

    //set
    template <class type> bool set(ICodeContext * ctx, const char * partitionKey, const char * key, type value, unsigned expire, eclDataType eclType);
    template <class type> bool set(ICodeContext * ctx, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned expire, eclDataType eclType);
    //get
    template <class type> void get(ICodeContext * ctx, const char * partitionKey, const char * key, type & value, eclDataType eclType);
    template <class type> void get(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & valueLength, type * & value, eclDataType eclType);
    void getVoidPtrLenPair(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & valueLength, void * & value, eclDataType eclType);

    bool clear(ICodeContext * ctx, unsigned when);
    bool exist(ICodeContext * ctx, const char * key, const char * partitionKey);
    eclDataType getKeyType(const char * key, const char * partitionKey);

    bool isSameConnection(const char * _servers) const;

private :
    void checkServersUp(ICodeContext * ctx);
    void assertOnError(memcached_return_t error, const char * msgSuffix = "");
    const char * keyNotFoundMsg(memcached_return_t error, const char * key, StringBuffer & target) const;
    void connect(ICodeContext * ctx);
    bool reportErrorOnFail(ICodeContext * ctx, memcached_return_t error);
    void reportKeyTypeMismatch(ICodeContext * ctx, const char * key, uint32_t flag, eclDataType eclType);
    void * cpy(const char * src, size_t length);
    void logServerStats(ICodeContext * ctx);
    void init(ICodeContext * ctx);
    void invokePoolSecurity(ICodeContext * ctx);
    void invokeConnectionSecurity(ICodeContext * ctx);
    void setPoolSettings();
    void assertPool();//For internal purposes to insure correct order of the above processes and instantiation.

private :
    memcached_st * connection;
    memcached_pool_st * pool;
    StringAttr options;
    bool alreadyInitialized;
    unsigned typeMismatchCount;
};

#define OwnedMCached Owned<MemCachedPlugin::MCached>

#define MAX_TYPEMISMATCHCOUNT 10

static CriticalSection crit;
static OwnedMCached cachedConnection;

MCached * createConnection(ICodeContext * ctx, const char * servers)
{
    CriticalBlock block(crit);
    if (!cachedConnection)
    {
        cachedConnection.setown(new MemCachedPlugin::MCached(ctx, servers));
        return LINK(cachedConnection);
    }

    if (cachedConnection->isSameConnection(servers))
        return LINK(cachedConnection);

    cachedConnection.setown(new MemCachedPlugin::MCached(ctx, servers));
    return LINK(cachedConnection);
}

//-------------------------------------------SET-----------------------------------------
template<class type> bool MSet(ICodeContext * ctx, const char * _servers, const char * partitionKey, const char * key, type value, unsigned expire, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, _servers);
    bool success = serverPool->set(ctx, partitionKey, key, value, expire, eclType);
    return success;
}
//Set pointer types
template<class type> bool MSet(ICodeContext * ctx, const char * _servers, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned expire, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, _servers);
    bool success = serverPool->set(ctx, partitionKey, key, valueLength, value, expire, eclType);
    return success;
}
//-------------------------------------------GET-----------------------------------------
template<class type> void MGet(ICodeContext * ctx, const char * servers, const char * partitionKey, const char * key, type & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, servers);
    serverPool->get(ctx, partitionKey, key, returnValue, eclType);
}
template<class type> void MGet(ICodeContext * ctx, const char * servers, const char * partitionKey, const char * key, size_t & returnLength, type * & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, servers);
    serverPool->get(ctx, partitionKey, key, returnLength, returnValue, eclType);
}
void MGetVoidPtrLenPair(ICodeContext * ctx, const char * servers, const char * partitionKey, const char * key, size_t & returnLength, void * & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, servers);
    serverPool->getVoidPtrLenPair(ctx, partitionKey, key, returnLength, returnValue, eclType);
}
}//close namespace

//----------------------------------SET----------------------------------------
template<class type> bool MemCachedPlugin::MCached::set(ICodeContext * ctx, const char * partitionKey, const char * key, type value, unsigned expire, eclDataType eclType)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        return !reportErrorOnFail(ctx, memcached_set_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), _value, sizeof(value), (time_t)(expire*unitExpire), (uint32_t)eclType));
    else
        return !reportErrorOnFail(ctx, memcached_set(connection, key, strlen(key), _value, sizeof(value), (time_t)(expire*unitExpire), (uint32_t)eclType));
}
template<class type> bool MemCachedPlugin::MCached::set(ICodeContext * ctx, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned expire, eclDataType eclType)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        return !reportErrorOnFail(ctx, memcached_set_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), _value, (size_t)(valueLength), (time_t)(expire*unitExpire), (uint32_t)eclType));
    else
        return !reportErrorOnFail(ctx, memcached_set(connection, key, strlen(key), _value, (size_t)(valueLength), (time_t)(expire*unitExpire), (uint32_t)eclType));
}
//----------------------------------GET----------------------------------------
template<class type> void MemCachedPlugin::MCached::get(ICodeContext * ctx, const char * partitionKey, const char * key, type & returnValue, eclDataType eclType)
{
    uint32_t flag = 0;
    size_t returnLength = 0;
    memcached_return_t error;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnLength, &flag, &error));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnLength, &flag, &error));

    StringBuffer keyMsg;
    assertOnError(error, keyNotFoundMsg(error, key, keyMsg));
    reportKeyTypeMismatch(ctx, key, flag, eclType);

    if (sizeof(type)!=returnLength)
    {
        VStringBuffer msg("MemCachedPlugin: ERROR - Requested type of different size (%uB) from that stored (%uB). Check logs for more information.", (unsigned)sizeof(type), (unsigned)returnLength);
        rtlFail(0, msg.str());
    }
    memcpy(&returnValue, value, returnLength);
}
template<class type> void MemCachedPlugin::MCached::get(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & returnLength, type * & returnValue, eclDataType eclType)
{
    uint32_t flag = 0;
    memcached_return_t error;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnLength, &flag, &error));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnLength, &flag, &error));

    StringBuffer keyMsg;
    assertOnError(error, keyNotFoundMsg(error, key, keyMsg));
    reportKeyTypeMismatch(ctx, key, flag, eclType);

    returnValue = reinterpret_cast<type*>(cpy(value, returnLength));
}
void MemCachedPlugin::MCached::getVoidPtrLenPair(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & returnLength, void * & returnValue, eclDataType eclType)
{
    uint32_t flag = 0;
    size_t returnValueLength = 0;
    memcached_return_t error;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnValueLength, &flag, &error));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnValueLength, &flag, &error));

    StringBuffer keyMsg;
    assertOnError(error, keyNotFoundMsg(error, key, keyMsg));
    reportKeyTypeMismatch(ctx, key, flag, eclType);

    returnLength = (size32_t)(returnValueLength);
    returnValue = reinterpret_cast<void*>(cpy(value, returnLength));
}

ECL_MEMCACHED_API void setPluginContext(IPluginContext * ctx) { MemCachedPlugin::parentCtx = ctx; }

MemCachedPlugin::MCached::MCached(ICodeContext * ctx, const char * _options)
{
    alreadyInitialized = false;
    connection = NULL;
    pool = NULL;
    options.set(_options);
    typeMismatchCount = 0;

#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    memcached_st *memc = memcached_create(NULL);
    memcached_return_t error;
    memcached_server_st *servers = NULL;
    try
    {
        unsigned pool_min = 1;
        unsigned pool_max = 1;
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
                unsigned port;
                if (splitPort.ordinality()==2)
                    port = atoi(splitPort.item(1));
                else
                    port = 11211;
                servers = memcached_server_list_append(NULL, splitPort.item(0), port, &error);
                assertOnError(error, "memcached_server_list_append failed");
            }
            else if (strncmp(opt, "--POOL-MIN=", 11) ==0)
                pool_min = atoi(opt+11);
            else if (strncmp(opt, "--POOL-MAX=", 11) ==0)
                pool_max = atoi(opt+11);
            else
            {
                VStringBuffer err("MemCachedPlugin: unsupported option string %s", opt);
                rtlFail(0, err.str());
            }
        }
        if (!servers)
            rtlFail(0, "No servers specified");
        error = memcached_server_push(memc, servers);
        memcached_server_list_free(servers);
        assertOnError(error, "memcached_server_push failed");
        pool = memcached_pool_create(memc, pool_min, pool_max);  // takes ownership of memc
    }
    catch (...)
    {
        if (servers)
            memcached_server_list_free(servers);
        if (memc)
            memcached_free(memc);
        throw;
    }
#else
    pool = memcached_pool(_options, strlen(_options));
#endif
    assertPool();

    setPoolSettings();
    invokePoolSecurity(ctx);
    connect(ctx);
    checkServersUp(ctx);
}
//-----------------------------------------------------------------------------
MemCachedPlugin::MCached::~MCached()
{
    if (pool)
    {
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
        memcached_pool_push(pool, connection);
#else
        memcached_pool_release(pool, connection);
#endif
        connection = NULL;//For safety (from changing this destructor) as not implicit in either the above or below.
        memcached_st *memc = memcached_pool_destroy(pool);
        if (memc)
            memcached_free(memc);
    }
    else if (connection)//This should never be needed but just in case.
    {
        memcached_free(connection);
    }
}

bool MemCachedPlugin::MCached::isSameConnection(const char * _servers) const
{
    if (!_servers)
        return false;

    return stricmp(options.get(), _servers) == 0;
}

void MemCachedPlugin::MCached::assertPool()
{
    if (!pool)
    {
        StringBuffer msg = "Memcached Plugin: Failed to instantiate server pool with:";
        msg.newline().append(options);
        rtlFail(0, msg.str());
    }
}

void * MemCachedPlugin::MCached::cpy(const char * src, size_t length)
{
    void * value = rtlMalloc(length);
    return memcpy(value, src, length);
}

void MemCachedPlugin::MCached::checkServersUp(ICodeContext * ctx)
{
    memcached_return_t error;
    char * args = NULL;

    OwnedMalloc<memcached_stat_st> stats;
    stats.setown(memcached_stat(connection, args, &error));
    assertex(stats);

    unsigned int numberOfServers = memcached_server_count(connection);
    unsigned int numberOfServersDown = 0;
    for (unsigned i = 0; i < numberOfServers; ++i)
    {
        if (stats[i].pid == -1)//perhaps not the best test?
        {
            numberOfServersDown++;
            VStringBuffer msg("Memcached Plugin: Failed connecting to entry %u\nwithin the server list: %s", i+1, options.str());
            ctx->addWuException(msg.str(), WRN_FROM_PLUGIN, ExceptionSeverityWarning, "");
        }
    }
    if (numberOfServersDown == numberOfServers)
        rtlFail(0,"Memcached Plugin: Failed connecting to ALL servers. Check memcached on all servers and \"memcached -B ascii\" not used.");

    //check memcached version homogeneity
    for (unsigned i = 0; i < numberOfServers-1; ++i)
    {
        if (strcmp(stats[i].version, stats[i+1].version) != 0)
            ctx->addWuException("Memcached Plugin: Inhomogeneous versions of memcached across servers.", WRN_FROM_PLUGIN, ExceptionSeverityInformation, "");
    }
}

bool MemCachedPlugin::MCached::reportErrorOnFail(ICodeContext * ctx, memcached_return_t error)
{
    if (error == MEMCACHED_SUCCESS)
        return false;

    VStringBuffer msg("Memcached Plugin: %s", memcached_strerror(connection, error));
    ctx->addWuException(msg.str(), ERR_FROM_PLUGIN, ExceptionSeverityInformation, "");
    return true;
}

void MemCachedPlugin::MCached::assertOnError(memcached_return_t error, const char * msgSuffix)
{
    if (error != MEMCACHED_SUCCESS)
    {
        VStringBuffer msg("Memcached Plugin: %s%s", memcached_strerror(connection, error), msgSuffix);
        rtlFail(0, msg.str());
    }
}

const char * MemCachedPlugin::MCached::keyNotFoundMsg(memcached_return_t error, const char * key, StringBuffer & target) const
{
    target.clear();
    if (error == MEMCACHED_NOTFOUND)
    {
        target = " (key: '";
        target.append(key).append("') ");
    }
    return target.str();
}

bool MemCachedPlugin::MCached::clear(ICodeContext * ctx, unsigned when)
{
    //NOTE: memcached_flush is the actual cache flush/clear/delete and not an io buffer flush.
    return !reportErrorOnFail(ctx, memcached_flush(connection, (time_t)(when)));
}

bool MemCachedPlugin::MCached::exist(ICodeContext * ctx, const char * key, const char * partitionKey)
{
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    throw makeStringException(0, "memcached_exist not supported in this version of libmemcached");
#else
    memcached_return_t error;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        error = memcached_exist_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key));
    else
        error = memcached_exist(connection, key, strlen(key));

    if (error == MEMCACHED_SUCCESS)
        return true;
    else if (error == MEMCACHED_NOTFOUND)
        return false;

    reportErrorOnFail(ctx, error);
    return false;
#endif
}

MemCachedPlugin::eclDataType MemCachedPlugin::MCached::getKeyType(const char * key, const char * partitionKey)
{
    size_t returnValueLength;
    uint32_t flag;
    memcached_return_t error;

    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnValueLength, &flag, &error);
    else
        memcached_get(connection, key, strlen(key), &returnValueLength, &flag, &error);

    if (error == MEMCACHED_SUCCESS)
        return (MemCachedPlugin::eclDataType)(flag);
    else if (error == MEMCACHED_NOTFOUND)
        return ECL_NONE;
    else
    {
        StringBuffer msg = "Memcached Plugin: ";
        rtlFail(0, msg.append(memcached_strerror(connection, error)).str());
    }
}

void MemCachedPlugin::MCached::reportKeyTypeMismatch(ICodeContext * ctx, const char * key, uint32_t flag, eclDataType eclType)
{
    if (flag && eclType != ECL_DATA && flag != eclType)
    {
        VStringBuffer msg("Memcached Plugin: The requested key '%s' is of type %s, not %s as requested.", key, enumToStr((eclDataType)(flag)), enumToStr(eclType));
        if (++typeMismatchCount <= MAX_TYPEMISMATCHCOUNT)
            ctx->logString(msg.str());//NOTE: logging locally, rather than calling ctx->addWuException, to prevent flooding the WU if this is called multiple times by every node
    }
}

void MemCachedPlugin::MCached::logServerStats(ICodeContext * ctx)
{
    //NOTE: errors are ignored here so that at least some info is reported, such as non-connection related libmemcached version numbers
    memcached_return_t error;
    char * args = NULL;

    OwnedMalloc<memcached_stat_st> stats;
    stats.setown(memcached_stat(connection, args, &error));

    OwnedMalloc<char*> keys;
    keys.setown(memcached_stat_get_keys(connection, stats, &error));

    unsigned int numberOfServers = memcached_server_count(connection);
    for (unsigned int i = 0; i < numberOfServers; ++i)
    {
        StringBuffer statsStr;
        unsigned j = 0;
        do
        {
            OwnedMalloc<char> value;
            value.setown(memcached_stat_get_value(connection, &stats[i], keys[j], &error));
            statsStr.newline().append("libmemcached server stat - ").append(keys[j]).append(":").append(value);
        } while (keys[++j]);
        statsStr.newline().append("libmemcached client stat - libmemcached version:").append(memcached_lib_version());
        ctx->logString(statsStr.str());
    }
}

void MemCachedPlugin::MCached::init(ICodeContext * ctx)
{
    logServerStats(ctx);
}

void MemCachedPlugin::MCached::setPoolSettings()
{
    assertPool();
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_HASH_WITH_PREFIX_KEY, 1));//key set in invokeConnectionSecurity. Only hashed with keys and not partitionKeys
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_KETAMA, 1));//NOTE: alias of MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA amongst others.
    memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_USE_UDP, 0);  // Note that this fails on early versions of libmemcached, so ignore result
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_SERVER_FAILURE_LIMIT, 1));
#if (LIBMEMCACHED_VERSION_HEX>=0x50000)
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_REMOVE_FAILED_SERVERS, 1));
#endif
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_NO_BLOCK, 0));
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT, 100));//units of ms MORE: What should I set this to or get from?
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 0));// Buffering does not work with the ecl runtime paradigm
}

void MemCachedPlugin::MCached::invokePoolSecurity(ICodeContext * ctx)
{
    assertPool();
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1));
}

void MemCachedPlugin::MCached::invokeConnectionSecurity(ICodeContext * ctx)
{
    //NOTE: Whether to assert or just report? This depends on when this is called. If before checkServersUp() and
    //a server is down, it will cause the following to fail if asserted with only a 'poor' libmemcached error message.
    //Reporting means that these 'security' measures may not be carried out. Moving checkServersUp() to here is probably the best
    //soln. however, this comes with extra overhead.
    reportErrorOnFail(ctx, memcached_verbosity(connection, (uint32_t)(0)));
    //reportErrorOnFail(ctx, memcached_callback_set(connection, MEMCACHED_CALLBACK_PREFIX_KEY, "ecl"));//NOTE: MEMCACHED_CALLBACK_PREFIX_KEY is an alias of MEMCACHED_CALLBACK_NAMESPACE
}

void MemCachedPlugin::MCached::connect(ICodeContext * ctx)
{
    assertPool();
    if (connection)
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
        memcached_pool_push(pool, connection);
#else
        memcached_pool_release(pool, connection);
#endif
    memcached_return_t error;
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    connection = memcached_pool_pop(pool, (struct timespec *)0 , &error);
#else
    connection = memcached_pool_fetch(pool, (struct timespec *)0 , &error);
#endif
    invokeConnectionSecurity(ctx);

    if (!alreadyInitialized)//Do this now rather than after assert. Better to have something even if it could be jiberish.
    {
        init(ctx);//doesn't necessarily initialize anything, instead outputs specs etc for debugging
        alreadyInitialized = true;
    }
    assertOnError(error);
}

//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MClear(ICodeContext * ctx, const char * servers)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, servers);
    bool returnValue = serverPool->clear(ctx, 0);
    return returnValue;
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MExist(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, servers);
    bool returnValue = serverPool->exist(ctx, key, partitionKey);
    return returnValue;
}
ECL_MEMCACHED_API const char * ECL_MEMCACHED_CALL MKeyType(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, servers);
    const char * keyType = enumToStr(serverPool->getKeyType(key, partitionKey));
    return keyType;
}
//-----------------------------------SET------------------------------------------
//NOTE: These were all overloaded by 'value' type, however; this caused problems since ecl implicitly casts and doesn't type check.
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, size32_t valueLength, const char * value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, valueLength, value, expire, MemCachedPlugin::ECL_STRING);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, size32_t valueLength, const UChar * value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, (valueLength)*sizeof(UChar), value, expire, MemCachedPlugin::ECL_UNICODE);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, signed __int64 value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, value, expire, MemCachedPlugin::ECL_INTEGER);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, unsigned __int64 value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, value, expire, MemCachedPlugin::ECL_UNSIGNED);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, double value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, value, expire, MemCachedPlugin::ECL_REAL);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * servers, const char * key, bool value, const char * partitionKey, unsigned expire)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, value, expire, MemCachedPlugin::ECL_BOOLEAN);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSetData(ICodeContext * ctx, const char * servers, const char * key, size32_t valueLength, const void * value, const char * partitionKey, unsigned expire)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, valueLength, value, expire, MemCachedPlugin::ECL_DATA);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MSetUtf8(ICodeContext * ctx, const char * servers, const char * key, size32_t valueLength, const char * value, const char * partitionKey, unsigned expire /* = 0 (ECL default)*/)
{
    return MemCachedPlugin::MSet(ctx, servers, partitionKey, key, rtlUtf8Size(valueLength, value), value, expire, MemCachedPlugin::ECL_UTF8);
}
//-------------------------------------GET----------------------------------------
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MGetBool(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    bool value;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, value, MemCachedPlugin::ECL_BOOLEAN);
    return value;
}
ECL_MEMCACHED_API double ECL_MEMCACHED_CALL MGetDouble(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    double value;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, value, MemCachedPlugin::ECL_REAL);
    return value;
}
ECL_MEMCACHED_API signed __int64 ECL_MEMCACHED_CALL MGetInt8(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    signed __int64 value;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, value, MemCachedPlugin::ECL_INTEGER);
    return value;
}
ECL_MEMCACHED_API unsigned __int64 ECL_MEMCACHED_CALL MGetUint8(ICodeContext * ctx, const char * servers, const char * key, const char * partitionKey)
{
    unsigned __int64 value;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, value, MemCachedPlugin::ECL_UNSIGNED);
    return value;
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * servers, const char * key, const char * partitionKey)
{
    size_t _returnLength;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, _returnLength, returnValue, MemCachedPlugin::ECL_STRING);
    returnLength = static_cast<size32_t>(_returnLength);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * servers, const char * key, const char * partitionKey)
{
    size_t _returnSize;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, _returnSize, returnValue, MemCachedPlugin::ECL_UNICODE);
    returnLength = static_cast<size32_t>(_returnSize/sizeof(UChar));
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * servers, const char * key, const char * partitionKey)
{
    size_t returnSize;
    MemCachedPlugin::MGet(ctx, servers, partitionKey, key, returnSize, returnValue, MemCachedPlugin::ECL_UTF8);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, const char * servers, const char * key, const char * partitionKey)
{
    size_t _returnLength;
    MemCachedPlugin::MGetVoidPtrLenPair(ctx, servers, partitionKey, key, _returnLength, returnValue, MemCachedPlugin::ECL_DATA);
    returnLength = static_cast<size32_t>(_returnLength);
}
