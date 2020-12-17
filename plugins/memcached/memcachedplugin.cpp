/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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
#include "jthread.hpp"
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
    pb->description = "ECL plugin library for the C/C++ API libmemcached (http://libmemcached.org/)";
    return true;
}

namespace MemCachedPlugin {

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
    MCached(ICodeContext * ctx, const char * _options);
    ~MCached();

    //set
    template <class type> void set(ICodeContext * ctx, const char * partitionKey, const char * key, type value, unsigned __int64 expire, eclDataType eclType);
    template <class type> void set(ICodeContext * ctx, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned __int64 expire, eclDataType eclType);
    //get
    template <class type> void get(ICodeContext * ctx, const char * partitionKey, const char * key, type & value, eclDataType eclType);
    template <class type> void get(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & valueLength, type * & value, eclDataType eclType);
    void getVoidPtrLenPair(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & valueLength, void * & value, eclDataType eclType);

    void clear(ICodeContext * ctx, unsigned when);
    bool exists(ICodeContext * ctx, const char * key, const char * partitionKey);
    void deleteKey(ICodeContext * ctx, const char * key, const char * partitionKey);
    eclDataType getKeyType(const char * key, const char * partitionKey);

    bool isSameConnection(const char * _options) const;

private :
    void checkServersUp(ICodeContext * ctx);
    void assertOnError(memcached_return_t rc, const char * _msg);
    const char * appendIfKeyNotFoundMsg(memcached_return_t rc, const char * key, StringBuffer & target) const;
    void connect(ICodeContext * ctx);
    bool logErrorOnFail(ICodeContext * ctx, memcached_return_t rc, const char * _msg);
    void reportKeyTypeMismatch(ICodeContext * ctx, const char * key, uint32_t flag, eclDataType eclType);
    void * cpy(const char * src, size_t length);
    void setPoolSettings();
    void assertPool();//For internal purposes to insure correct order of the above processes and instantiation.

private :
    memcached_st * connection;
    memcached_pool_st * pool;
    StringAttr options;
    unsigned typeMismatchCount;
};

typedef Owned<MCached> OwnedMCached;

static const unsigned MAX_TYPEMISMATCHCOUNT = 10;

static __thread MCached * cachedConnection;
static __thread bool threadHooked;

//The following class is here to ensure destruction of the cachedConnection within the main thread
//as this is not handled by the thread hook mechanism.
static class mainThreadCachedConnection
{
public :
    mainThreadCachedConnection() { }
    ~mainThreadCachedConnection()
    {
        if (cachedConnection)
        {
            cachedConnection->Release();
            cachedConnection = NULL;
        }
    }
} mainThread;

static bool releaseContext(bool isPooled)
{
    if (cachedConnection)
    {
        cachedConnection->Release();
        cachedConnection = NULL;
    }
    threadHooked = false;
    return false;
}
MCached * createConnection(ICodeContext * ctx, const char * options)
{
    if (!cachedConnection)
    {
        cachedConnection = new MemCachedPlugin::MCached(ctx, options);
        if (!threadHooked)
        {
            addThreadTermFunc(releaseContext);
            threadHooked = true;
        }
        return LINK(cachedConnection);
    }

    if (cachedConnection->isSameConnection(options))
        return LINK(cachedConnection);

    cachedConnection->Release();
    cachedConnection = NULL;
    cachedConnection = new MemCachedPlugin::MCached(ctx, options);
    return LINK(cachedConnection);
}

//-------------------------------------------SET-----------------------------------------
template<class type> void MSet(ICodeContext * ctx, const char * _options, const char * partitionKey, const char * key, type value, unsigned __int64 expire, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, _options);
    serverPool->set(ctx, partitionKey, key, value, expire, eclType);
}
//Set pointer types
template<class type> void MSet(ICodeContext * ctx, const char * _options, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned __int64 expire, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, _options);
    serverPool->set(ctx, partitionKey, key, valueLength, value, expire, eclType);
}
//-------------------------------------------GET-----------------------------------------
template<class type> void MGet(ICodeContext * ctx, const char * options, const char * partitionKey, const char * key, type & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, options);
    serverPool->get(ctx, partitionKey, key, returnValue, eclType);
}
template<class type> void MGet(ICodeContext * ctx, const char * options, const char * partitionKey, const char * key, size_t & returnLength, type * & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, options);
    serverPool->get(ctx, partitionKey, key, returnLength, returnValue, eclType);
}
void MGetVoidPtrLenPair(ICodeContext * ctx, const char * options, const char * partitionKey, const char * key, size_t & returnLength, void * & returnValue, eclDataType eclType)
{
    OwnedMCached serverPool = createConnection(ctx, options);
    serverPool->getVoidPtrLenPair(ctx, partitionKey, key, returnLength, returnValue, eclType);
}

//----------------------------------SET----------------------------------------
template<class type> void MemCachedPlugin::MCached::set(ICodeContext * ctx, const char * partitionKey, const char * key, type value, unsigned __int64 expire, eclDataType eclType)
{
    const char * _value = reinterpret_cast<const char *>(&value);//Do this even for char * to prevent compiler complaining
    size_t partitionKeyLength = strlen(partitionKey);
    const char * msg = "'Set' request failed - ";
    if (partitionKeyLength)
        assertOnError(memcached_set_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), _value, sizeof(value), (time_t)expire, (uint32_t)eclType), msg);
    else
        assertOnError(memcached_set(connection, key, strlen(key), _value, sizeof(value), (time_t)expire, (uint32_t)eclType), msg);
}
template<class type> void MemCachedPlugin::MCached::set(ICodeContext * ctx, const char * partitionKey, const char * key, size32_t valueLength, const type * value, unsigned __int64 expire, eclDataType eclType)
{
    const char * _value = reinterpret_cast<const char *>(value);//Do this even for char * to prevent compiler complaining
    size_t partitionKeyLength = strlen(partitionKey);
    const char * msg = "'Set' request failed - ";
    if (partitionKeyLength)
        assertOnError(memcached_set_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), _value, (size_t)(valueLength), (time_t)expire, (uint32_t)eclType), msg);
    else
        assertOnError(memcached_set(connection, key, strlen(key), _value, (size_t)(valueLength), (time_t)expire, (uint32_t)eclType), msg);
}
//----------------------------------GET----------------------------------------
template<class type> void MemCachedPlugin::MCached::get(ICodeContext * ctx, const char * partitionKey, const char * key, type & returnValue, eclDataType eclType)
{
    uint32_t flag = 0;
    size_t returnLength = 0;
    memcached_return_t rc;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnLength, &flag, &rc));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnLength, &flag, &rc));

    StringBuffer keyMsg("'Get<type>' request failed - ");
    assertOnError(rc, appendIfKeyNotFoundMsg(rc, key, keyMsg));
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
    memcached_return_t rc;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnLength, &flag, &rc));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnLength, &flag, &rc));

    StringBuffer keyMsg("'Get<type>' request failed - ");
    assertOnError(rc, appendIfKeyNotFoundMsg(rc, key, keyMsg));
    reportKeyTypeMismatch(ctx, key, flag, eclType);

    returnValue = reinterpret_cast<type*>(cpy(value, returnLength));
}
void MemCachedPlugin::MCached::getVoidPtrLenPair(ICodeContext * ctx, const char * partitionKey, const char * key, size_t & returnLength, void * & returnValue, eclDataType eclType)
{
    uint32_t flag = 0;
    size_t returnValueLength = 0;
    memcached_return_t rc;

    OwnedMalloc<char> value;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnValueLength, &flag, &rc));
    else
        value.setown(memcached_get(connection, key, strlen(key), &returnValueLength, &flag, &rc));

    StringBuffer keyMsg("'Get<type>' request failed - ");
    assertOnError(rc, appendIfKeyNotFoundMsg(rc, key, keyMsg));
    reportKeyTypeMismatch(ctx, key, flag, eclType);

    returnLength = (size32_t)(returnValueLength);
    returnValue = reinterpret_cast<void*>(cpy(value, returnLength));
}

MemCachedPlugin::MCached::MCached(ICodeContext * ctx, const char * _options) : connection(NULL), pool(NULL), typeMismatchCount(0)
{
#if (LIBMEMCACHED_VERSION_HEX < 0x01000010)
    StringBuffer msg("Memcached Plugin: libmemcached version '");
    msg.append(LIBMEMCACHED_VERSION_STRING).append("' incompatible with min version>=1.0.10");
    rtlFail(0, msg.str());
#endif

    options.set(_options);
    pool = memcached_pool(_options, strlen(_options));
    assertPool();

    setPoolSettings();
    connect(ctx);
    checkServersUp(ctx);
}
//-----------------------------------------------------------------------------
MemCachedPlugin::MCached::~MCached()
{
    if (pool)
    {

        memcached_pool_release(pool, connection);
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

bool MemCachedPlugin::MCached::isSameConnection(const char * _options) const
{
    if (!_options)
        return false;

    return stricmp(options.get(), _options) == 0;
}

void MemCachedPlugin::MCached::assertPool()
{
    if (!pool)
    {
        StringBuffer msg("Memcached Plugin: Failed to instantiate server pool with:");
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
    memcached_return_t rc;
    char * args = NULL;

    OwnedMalloc<memcached_stat_st> stats;
    stats.setown(memcached_stat(connection, args, &rc));
    assertex(stats);

    unsigned int numberOfServers = memcached_server_count(connection);
    unsigned int numberOfServersDown = 0;
    for (unsigned i = 0; i < numberOfServers; ++i)
    {
        if (stats[i].pid == -1)//perhaps not the best test?
        {
            numberOfServersDown++;
            VStringBuffer msg("Memcached Plugin: Failed connecting to entry %u\nwithin the server list: %s", i+1, options.str());
            ctx->logString(msg.str());
        }
    }
    if (numberOfServersDown == numberOfServers)
        rtlFail(0,"Memcached Plugin: Failed connecting to ALL servers. Check memcached on all servers and \"memcached -B ascii\" not used.");

    //check memcached version homogeneity
    for (unsigned i = 0; i < numberOfServers-1; ++i)
    {
        if (strcmp(stats[i].version, stats[i+1].version) != 0)
            ctx->logString("Memcached Plugin: Inhomogeneous versions of memcached across servers.");
    }
}

bool MemCachedPlugin::MCached::logErrorOnFail(ICodeContext * ctx, memcached_return_t rc, const char * _msg)
{
    if (rc == MEMCACHED_SUCCESS)
        return false;

    VStringBuffer msg("Memcached Plugin: %s%s", _msg, memcached_strerror(connection, rc));
    ctx->logString(msg.str());
    return true;
}

void MemCachedPlugin::MCached::assertOnError(memcached_return_t rc, const char * _msg)
{
    if (rc != MEMCACHED_SUCCESS)
    {
        VStringBuffer msg("Memcached Plugin: %s%s", _msg, memcached_strerror(connection, rc));
        rtlFail(0, msg.str());
    }
}

const char * MemCachedPlugin::MCached::appendIfKeyNotFoundMsg(memcached_return_t rc, const char * key, StringBuffer & target) const
{
    if (rc == MEMCACHED_NOTFOUND)
        target.append("(key: '").append(key).append("') ");
    return target.str();
}

void MemCachedPlugin::MCached::clear(ICodeContext * ctx, unsigned when)
{
    //NOTE: memcached_flush is the actual cache flush/clear/delete and not an io buffer flush.
    assertOnError(memcached_flush(connection, (time_t)(when)), "'Clear' request failed - ");
}

bool MemCachedPlugin::MCached::exists(ICodeContext * ctx, const char * key, const char * partitionKey)
{
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    throw makeStringException(0, "memcached_exist not supported in this version of libmemcached");
#else
    memcached_return_t rc;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        rc = memcached_exist_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key));
    else
        rc = memcached_exist(connection, key, strlen(key));

    if (rc == MEMCACHED_NOTFOUND)
        return false;
    else
    {
        assertOnError(rc, "'Exists' request failed - ");
        return true;
    }
#endif
}
void MemCachedPlugin::MCached::deleteKey(ICodeContext * ctx, const char * key, const char * partitionKey)
{
    memcached_return_t rc;
    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        rc = memcached_delete_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), (time_t)0);
    else
        rc = memcached_delete(connection, key, strlen(key), (time_t)0);
    assertOnError(rc, "'Delete' request failed - ");
}
MemCachedPlugin::eclDataType MemCachedPlugin::MCached::getKeyType(const char * key, const char * partitionKey)
{
    size_t returnValueLength;
    uint32_t flag;
    memcached_return_t rc;

    size_t partitionKeyLength = strlen(partitionKey);
    if (partitionKeyLength)
        memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnValueLength, &flag, &rc);
    else
        memcached_get(connection, key, strlen(key), &returnValueLength, &flag, &rc);

    if (rc == MEMCACHED_SUCCESS)
        return (MemCachedPlugin::eclDataType)(flag);
    else if (rc == MEMCACHED_NOTFOUND)
        return ECL_NONE;
    else
    {
        VStringBuffer msg("Memcached Plugin: 'KeyType' request failed - %s", memcached_strerror(connection, rc));
        rtlFail(0, msg.str());
    }
}

void MemCachedPlugin::MCached::reportKeyTypeMismatch(ICodeContext * ctx, const char * key, uint32_t flag, eclDataType eclType)
{
    if (flag && eclType != ECL_DATA && flag != eclType)
    {
        VStringBuffer msg("Memcached Plugin: The requested key '%s' is of type %s, not %s as requested.", key, enumToStr((eclDataType)(flag)), enumToStr(eclType));
        if (++typeMismatchCount <= MAX_TYPEMISMATCHCOUNT)
            ctx->logString(msg.str());
    }
}

void MemCachedPlugin::MCached::setPoolSettings()
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

void MemCachedPlugin::MCached::connect(ICodeContext * ctx)
{
    assertPool();
    if (connection)
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
        memcached_pool_push(pool, connection);
#else
        memcached_pool_release(pool, connection);
#endif
    memcached_return_t rc;
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
    connection = memcached_pool_pop(pool, (struct timespec *)0 , &rc);
#else
    connection = memcached_pool_fetch(pool, (struct timespec *)0 , &rc);
#endif
    assertOnError(rc, "memcached_pool_pop failed - ");
}

//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MClear(ICodeContext * ctx, const char * options)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, options);
    serverPool->clear(ctx, 0);
}
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MExists(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, options);
    return serverPool->exists(ctx, key, partitionKey);
}
ECL_MEMCACHED_API const char * ECL_MEMCACHED_CALL MKeyType(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, options);
    const char * keyType = enumToStr(serverPool->getKeyType(key, partitionKey));
    return keyType;
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MDelete(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    OwnedMCached serverPool = MemCachedPlugin::createConnection(ctx, options);
    serverPool->deleteKey(ctx, key, partitionKey);
}
//-----------------------------------SET------------------------------------------
//NOTE: These were all overloaded by 'value' type, however; this caused problems since ecl implicitly casts and doesn't type check.
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, size32_t valueLength, const char * value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, valueLength, value, expire, MemCachedPlugin::ECL_STRING);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, size32_t valueLength, const UChar * value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, (valueLength)*sizeof(UChar), value, expire, MemCachedPlugin::ECL_UNICODE);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, signed __int64 value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, value, expire, MemCachedPlugin::ECL_INTEGER);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, unsigned __int64 value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, value, expire, MemCachedPlugin::ECL_UNSIGNED);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, double value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, value, expire, MemCachedPlugin::ECL_REAL);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSet(ICodeContext * ctx, const char * key, bool value, const char * options, const char * partitionKey, unsigned __int64 expire)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, value, expire, MemCachedPlugin::ECL_BOOLEAN);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSetData(ICodeContext * ctx, const char * key, size32_t valueLength, const void * value, const char * options, const char * partitionKey, unsigned __int64 expire)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, valueLength, value, expire, MemCachedPlugin::ECL_DATA);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MSetUtf8(ICodeContext * ctx, const char * key, size32_t valueLength, const char * value, const char * options, const char * partitionKey, unsigned __int64 expire /* = 0 (ECL default)*/)
{
    MemCachedPlugin::MSet(ctx, options, partitionKey, key, rtlUtf8Size(valueLength, value), value, expire, MemCachedPlugin::ECL_UTF8);
}
//-------------------------------------GET----------------------------------------
ECL_MEMCACHED_API bool ECL_MEMCACHED_CALL MGetBool(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    bool value;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, value, MemCachedPlugin::ECL_BOOLEAN);
    return value;
}
ECL_MEMCACHED_API double ECL_MEMCACHED_CALL MGetDouble(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    double value;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, value, MemCachedPlugin::ECL_REAL);
    return value;
}
ECL_MEMCACHED_API signed __int64 ECL_MEMCACHED_CALL MGetInt8(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    signed __int64 value;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, value, MemCachedPlugin::ECL_INTEGER);
    return value;
}
ECL_MEMCACHED_API unsigned __int64 ECL_MEMCACHED_CALL MGetUint8(ICodeContext * ctx, const char * key, const char * options, const char * partitionKey)
{
    unsigned __int64 value;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, value, MemCachedPlugin::ECL_UNSIGNED);
    return value;
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetStr(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, const char * partitionKey)
{
    size_t _returnLength;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, _returnLength, returnValue, MemCachedPlugin::ECL_STRING);
    returnLength = static_cast<size32_t>(_returnLength);
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetUChar(ICodeContext * ctx, size32_t & returnLength, UChar * & returnValue,  const char * key, const char * options, const char * partitionKey)
{
    size_t _returnSize;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, _returnSize, returnValue, MemCachedPlugin::ECL_UNICODE);
    returnLength = static_cast<size32_t>(_returnSize/sizeof(UChar));
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetUtf8(ICodeContext * ctx, size32_t & returnLength, char * & returnValue, const char * key, const char * options, const char * partitionKey)
{
    size_t returnSize;
    MemCachedPlugin::MGet(ctx, options, partitionKey, key, returnSize, returnValue, MemCachedPlugin::ECL_UTF8);
    returnLength = static_cast<size32_t>(rtlUtf8Length(returnSize, returnValue));
}
ECL_MEMCACHED_API void ECL_MEMCACHED_CALL MGetData(ICodeContext * ctx, size32_t & returnLength, void * & returnValue, const char * key, const char * options, const char * partitionKey)
{
    size_t _returnLength;
    MemCachedPlugin::MGetVoidPtrLenPair(ctx, options, partitionKey, key, _returnLength, returnValue, MemCachedPlugin::ECL_DATA);
    returnLength = static_cast<size32_t>(_returnLength);
}
}//close namespace
