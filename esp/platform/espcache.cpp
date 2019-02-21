/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "espcontext.hpp"
#include "espcache.hpp"


#ifdef USE_LIBMEMCACHED
#include <libmemcached/memcached.hpp>
#include <libmemcached/util.h>

class ESPMemCached : implements IEspCache, public CInterface
{
    memcached_st* connection = nullptr;
    memcached_pool_st* pool = nullptr;
    StringAttr options;
    bool initialized = false;
    CriticalSection cacheCrit;

    void setPoolSettings();
    void connect();
    bool checkServersUp();

    void assertOnError(memcached_return_t rc, const char * _msg);
    void assertPool();

    virtual bool checkAndGet(const char* groupID, const char* cacheID, StringBuffer& out);

public :
    IMPLEMENT_IINTERFACE;
    ESPMemCached();
    ~ESPMemCached();

    virtual bool cacheResponse(const char* cacheID, const unsigned cacheSeconds, const char* content, const char* contentType);
    virtual bool readResponseCache(const char* cacheID, StringBuffer& content, StringBuffer& contentType);

    bool init(const char * _options);
    ESPCacheResult exists(const char* groupID, const char* cacheID);
    ESPCacheResult get(const char* groupID, const char* cacheID, StringBuffer& out);
    ESPCacheResult set(const char* groupID, const char* cacheID, const char* value, unsigned __int64 expireSec);
    void remove(const char* groupID, const char* cacheID);
    void flush(unsigned when);
};

ESPMemCached::ESPMemCached()
{
#if (LIBMEMCACHED_VERSION_HEX < 0x01000010)
    VStringBuffer msg("ESPMemCached: libmemcached version '%s' incompatible with min version>=1.0.10", LIBMEMCACHED_VERSION_STRING);
    ESPLOG(LogNormal, "%s", msg.str());
#endif
}

ESPMemCached::~ESPMemCached()
{
    if (pool)
    {
        memcached_pool_release(pool, connection);
        connection = nullptr;//For safety (from changing this destructor) as not implicit in either the above or below.
        memcached_st *memc = memcached_pool_destroy(pool);
        if (memc)
            memcached_free(memc);
    }
    else if (connection)//This should never be needed but just in case.
    {
        memcached_free(connection);
    }
}

bool ESPMemCached::init(const char * _options)
{
    CriticalBlock block(cacheCrit);

    if (initialized)
        return initialized;

    if (!isEmptyString(_options))
        options.set(_options);
    else
        options.set("--SERVER=127.0.0.1");
    pool = memcached_pool(options.get(), options.length());
    assertPool();

    setPoolSettings();
    connect();
    if (connection)
        initialized = checkServersUp();
    return initialized;
}

bool ESPMemCached::cacheResponse(const char* cacheID, unsigned cacheSeconds, const char* content, const char* contentType)
{
    VStringBuffer contentTypeID("ContentType_%s", cacheID);

    CriticalBlock block(cacheCrit);
    ESPCacheResult ret = set("ESPResponse", cacheID, content, cacheSeconds);
    if (ret != ESPCacheSuccess)
        return false;
    return set("ESPResponse", contentTypeID.str(), contentType, cacheSeconds) == ESPCacheSuccess;
}

bool ESPMemCached::readResponseCache(const char* cacheID, StringBuffer& content, StringBuffer& contentType)
{
    VStringBuffer contentTypeID("ContentType_%s", cacheID);

    CriticalBlock block(cacheCrit);
    if (!checkAndGet("ESPResponse", cacheID, content))
        return false;
    return checkAndGet("ESPResponse", contentTypeID.str(), contentType);
}

void ESPMemCached::setPoolSettings()
{
    assertPool();
    const char * msg = "memcached_pool_behavior_set failed - ";
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_KETAMA, 1), msg);//NOTE: alias of MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA amongst others.
    memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_USE_UDP, 0);  // Note that this fails on early versions of libmemcached, so ignore result
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_NO_BLOCK, 0), msg);
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT, 1000), msg);//units of ms.
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_SND_TIMEOUT, 1000000), msg);//units of mu-s.
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_RCV_TIMEOUT, 1000000), msg);//units of mu-s.
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 0), msg);
    assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1), "memcached_pool_behavior_set failed - ");
}

void ESPMemCached::connect()
{
    assertPool();
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    if (connection)
        memcached_pool_push(pool, connection);
    memcached_return_t rc;
    connection = memcached_pool_pop(pool, (struct timespec *)0 , &rc);
#else
    if (connection)
        memcached_pool_release(pool, connection);
    memcached_return_t rc;
    connection = memcached_pool_fetch(pool, (struct timespec *)0 , &rc);
#endif
    assertOnError(rc, "memcached_pool_pop failed - ");
}

bool ESPMemCached::checkServersUp()
{
    memcached_return_t rc;
    char* args = nullptr;
    OwnedMalloc<memcached_stat_st> stats;
    stats.setown(memcached_stat(connection, args, &rc));

    unsigned int numberOfServers = memcached_server_count(connection);
    if (numberOfServers < 1)
    {
        ESPLOG(LogMin,"ESPMemCached: no server connected.");
        return false;
    }

    unsigned int numberOfServersDown = 0;
    for (unsigned i = 0; i < numberOfServers; ++i)
    {
        if (stats[i].pid == -1)//perhaps not the best test?
        {
            numberOfServersDown++;
            VStringBuffer msg("ESPMemCached: Failed connecting to entry %u\nwithin the server list: %s", i+1, options.str());
            ESPLOG(LogMin, "%s", msg.str());
        }
    }
    if (numberOfServersDown == numberOfServers)
    {
        ESPLOG(LogMin,"ESPMemCached: Failed connecting to ALL servers. Check memcached on all servers and \"memcached -B ascii\" not used.");
        return false;
    }

    //check memcached version homogeneity
    for (unsigned i = 0; i < numberOfServers-1; ++i)
    {
        if (!streq(stats[i].version, stats[i+1].version))
            OWARNLOG("ESPMemCached: Inhomogeneous versions of memcached across servers.");
    }
    return true;
}

ESPCacheResult ESPMemCached::exists(const char* groupID, const char* cacheID)
{
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    throw makeStringException(0, "memcached_exist not supported in this version of libmemcached");
#endif

    memcached_return_t rc;
    size_t groupIDLength = strlen(groupID);
    if (groupIDLength)
        rc = memcached_exist_by_key(connection, groupID, groupIDLength, cacheID, strlen(cacheID));
    else
        rc = memcached_exist(connection, cacheID, strlen(cacheID));

    if (rc != MEMCACHED_NOTFOUND)
        assertOnError(rc, "'Exists' request failed - ");
    return rc == MEMCACHED_NOTFOUND ? ESPCacheNotFound : (rc == MEMCACHED_SUCCESS ? ESPCacheSuccess : ESPCacheError);
}

ESPCacheResult ESPMemCached::get(const char* groupID, const char* cacheID, StringBuffer& out)
{
    uint32_t flag = 0;
    size_t returnLength;
    memcached_return_t rc;

    OwnedMalloc<char> value;
    size_t groupIDLength = strlen(groupID);
    if (groupIDLength)
        value.setown(memcached_get_by_key(connection, groupID, groupIDLength, cacheID, strlen(cacheID), &returnLength, &flag, &rc));
    else
        value.setown(memcached_get(connection, cacheID, strlen(cacheID), &returnLength, &flag, &rc));

    if (value)
        out.set(value);

    StringBuffer msg = "'Get' request failed - ";
    if (rc == MEMCACHED_NOTFOUND)
        msg.append("(cacheID: '").append(cacheID).append("') ");
    assertOnError(rc, msg.str());
    return rc == MEMCACHED_NOTFOUND ? ESPCacheNotFound : (rc == MEMCACHED_SUCCESS ? ESPCacheSuccess : ESPCacheError);
}

bool ESPMemCached::checkAndGet(const char* groupID, const char* cacheID, StringBuffer& item)
{
    ESPCacheResult ret = exists(groupID, cacheID);
    if ((ret != ESPCacheSuccess) && (ret != ESPCacheNotFound))
        return false;
    if (ret == ESPCacheSuccess)
        get(groupID, cacheID, item);
    return true;
}

ESPCacheResult ESPMemCached::set(const char* groupID, const char* cacheID, const char* value, unsigned __int64 expireSec)
{
    memcached_return_t rc;
    size_t groupIDLength = strlen(groupID);
    if (groupIDLength)
        rc = memcached_set_by_key(connection, groupID, groupIDLength, cacheID, strlen(cacheID), value, strlen(value), (time_t)expireSec, 0);
    else
        rc = memcached_set(connection, cacheID, strlen(cacheID), value, strlen(value), (time_t)expireSec, 0);
    
    assertOnError(rc, "'Set' request failed - ");
    return rc == MEMCACHED_NOTFOUND ? ESPCacheNotFound : (rc == MEMCACHED_SUCCESS ? ESPCacheSuccess : ESPCacheError);
}

void ESPMemCached::remove(const char* groupID, const char* cacheID)
{
    memcached_return_t rc;
    size_t groupIDLength = strlen(groupID);
    if (groupIDLength)
        rc = memcached_delete_by_key(connection, groupID, groupIDLength, cacheID, strlen(cacheID), (time_t)0);
    else
        rc = memcached_delete(connection, cacheID, strlen(cacheID), (time_t)0);
    assertOnError(rc, "'Delete' request failed - ");
}

void ESPMemCached::flush(unsigned when)
{
    //NOTE: memcached_flush is the actual cache flush/clear/delete and not an io buffer flush.
    assertOnError(memcached_flush(connection, (time_t)(when)), "'Clear' request failed - ");
}

void ESPMemCached::assertOnError(memcached_return_t rc, const char * _msg)
{
    if (rc != MEMCACHED_SUCCESS)
    {
        VStringBuffer msg("ESPMemCached: %s%s", _msg, memcached_strerror(connection, rc));
        ESPLOG(LogNormal, "%s", msg.str());
    }
}

void ESPMemCached::assertPool()
{
    if (!pool)
    {
        StringBuffer msg = "ESPMemCached: Failed to instantiate server pool with:";
        msg.newline().append(options);
        ESPLOG(LogNormal, "%s", msg.str());
    }
}
#endif //USE_LIBMEMCACHED

extern esp_http_decl IEspCache* createESPCache(const char* setting)
{
#ifdef USE_LIBMEMCACHED
    Owned<ESPMemCached> espCache = new ESPMemCached();
    if (espCache->init(setting))
        return espCache.getClear();
#endif
    return nullptr;
}
