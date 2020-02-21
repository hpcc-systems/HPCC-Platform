/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
#include "mysql.h"
#include "mysqld_error.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield.hpp"
#include "rtlembed.hpp"
#include "roxiemem.hpp"
#include "nbcd.hpp"

#if (MYSQL_VERSION_ID >= 80000)
  typedef bool my_bool;
#endif

__declspec(noreturn) static void UNSUPPORTED(const char *feature) __attribute__((noreturn));

static unsigned mysqlCacheCheckPeriod = 10000;
static unsigned mysqlCacheTimeoutPeriod = 60000;
static unsigned mysqlConnectionCacheSize = 10;


static void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in mysql plugin", feature);
}

static const char * compatibleVersions[] = {
    "MySQL Embed Helper 1.0.0",
    NULL };

static const char *version = "MySQL Embed Helper 1.0.0";

extern "C" DECL_EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = version;
    pb->moduleName = "mysql";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "MySQL Embed Helper";
    return true;
}

extern "C" DECL_EXPORT void setPluginContextEx(IPluginContextEx * _ctx)
{
    mysqlCacheCheckPeriod = _ctx->ctxGetPropInt("@mysqlCacheCheckPeriod", 10000);
    mysqlCacheTimeoutPeriod = _ctx->ctxGetPropInt("@mysqlCacheTimeoutPeriod", 60000);
    mysqlConnectionCacheSize = _ctx->ctxGetPropInt("@mysqlConnectionCacheSize", 10);
}

namespace mysqlembed {

__declspec(noreturn) static void failx(const char *msg, ...) __attribute__((format(printf, 1, 2), noreturn));
__declspec(noreturn) static void fail(const char *msg) __attribute__((noreturn));

static void failx(const char *message, ...)
{
    va_list args;
    va_start(args,message);
    StringBuffer msg;
    msg.append("mysql: ").valist_appendf(message,args);
    va_end(args);
    rtlFail(0, msg.str());
}

static void fail(const char *message)
{
    StringBuffer msg;
    msg.append("mysql: ").append(message);
    rtlFail(0, msg.str());
}


// Wrappers to MySQL structures that require corresponding releases

class MySQLConnection;

static __thread MySQLConnection *threadCachedConnection = nullptr;

enum MySQLOptionParamType
{
    ParamTypeNone,
    ParamTypeString,
    ParamTypeUInt,
    ParamTypeULong,
    ParamTypeBool
};

struct MySQLOptionDefinition
{
    const char *name;
    enum mysql_option option;
    MySQLOptionParamType paramType;
};

#define addoption(a,b) { #a, a, b }

MySQLOptionDefinition options[] =
{
    addoption(MYSQL_OPT_COMPRESS, ParamTypeNone),
    addoption(MYSQL_OPT_CONNECT_TIMEOUT, ParamTypeUInt),
#if (MYSQL_VERSION_ID < 80000)
    addoption(MYSQL_OPT_GUESS_CONNECTION, ParamTypeNone),
    addoption(MYSQL_OPT_SSL_VERIFY_SERVER_CERT, ParamTypeBool),
    addoption(MYSQL_OPT_USE_EMBEDDED_CONNECTION, ParamTypeNone),
    addoption(MYSQL_OPT_USE_REMOTE_CONNECTION, ParamTypeNone),
    addoption(MYSQL_SECURE_AUTH, ParamTypeBool),
    addoption(MYSQL_SET_CLIENT_IP, ParamTypeString),
#endif
    addoption(MYSQL_OPT_LOCAL_INFILE, ParamTypeUInt),
    addoption(MYSQL_OPT_NAMED_PIPE, ParamTypeNone),
    addoption(MYSQL_OPT_PROTOCOL, ParamTypeUInt),
    addoption(MYSQL_OPT_READ_TIMEOUT, ParamTypeUInt),
    addoption(MYSQL_OPT_RECONNECT, ParamTypeBool),
    addoption(MYSQL_OPT_USE_RESULT, ParamTypeNone),
    addoption(MYSQL_OPT_WRITE_TIMEOUT, ParamTypeUInt),
    addoption(MYSQL_READ_DEFAULT_FILE, ParamTypeString),
    addoption(MYSQL_READ_DEFAULT_GROUP, ParamTypeString),
    addoption(MYSQL_REPORT_DATA_TRUNCATION, ParamTypeBool),
    addoption(MYSQL_SET_CHARSET_DIR, ParamTypeString),
    addoption(MYSQL_SET_CHARSET_NAME, ParamTypeString),
    addoption(MYSQL_SHARED_MEMORY_BASE_NAME, ParamTypeString),
#if MYSQL_VERSION_ID >= 50507
    addoption(MYSQL_DEFAULT_AUTH, ParamTypeString),
    addoption(MYSQL_PLUGIN_DIR, ParamTypeString),
#endif
#if (MYSQL_VERSION_ID >= 50601)
    addoption(MYSQL_OPT_BIND, ParamTypeString),
#endif
#if (MYSQL_VERSION_ID >= 50603)
    addoption(MYSQL_OPT_SSL_CA, ParamTypeString),
    addoption(MYSQL_OPT_SSL_CAPATH, ParamTypeString),
    addoption(MYSQL_OPT_SSL_CERT, ParamTypeString),
    addoption(MYSQL_OPT_SSL_CIPHER, ParamTypeString),
    addoption(MYSQL_OPT_SSL_CRL, ParamTypeString),
    addoption(MYSQL_OPT_SSL_CRLPATH, ParamTypeString),
    addoption(MYSQL_OPT_SSL_KEY, ParamTypeString),
#endif
#if (MYSQL_VERSION_ID >= 50606)
    addoption(MYSQL_SERVER_PUBLIC_KEY, ParamTypeString),
#endif
#if (MYSQL_VERSION_ID >= 50527 && MYSQL_VERSION_ID < 50600) || MYSQL_VERSION_ID >= 50607
    addoption(MYSQL_ENABLE_CLEARTEXT_PLUGIN, ParamTypeBool),
#endif
    addoption(MYSQL_INIT_COMMAND, ParamTypeString),
#if (MYSQL_VERSION_ID >= 50610)
    addoption(MYSQL_OPT_CAN_HANDLE_EXPIRED_PASSWORDS, ParamTypeBool),
#endif
#if (MYSQL_VERSION_ID >= 50703 && MYSQL_VERSION_ID < 80000)
    addoption(MYSQL_OPT_SSL_ENFORCE, ParamTypeBool),
#endif
#if (MYSQL_VERSION_ID >= 50709)
    addoption(MYSQL_OPT_MAX_ALLOWED_PACKET, ParamTypeULong),
    addoption(MYSQL_OPT_NET_BUFFER_LENGTH, ParamTypeULong),
#endif
#if (MYSQL_VERSION_ID >= 50710)
    addoption(MYSQL_OPT_TLS_VERSION, ParamTypeString),
#endif
#if (MYSQL_VERSION_ID >= 50711)
    addoption(MYSQL_OPT_SSL_MODE, ParamTypeUInt),
#endif
    { nullptr, (enum mysql_option) 0, ParamTypeNone }
};

static MySQLOptionDefinition &lookupOption(const char *optName)
{
    for (MySQLOptionDefinition *optDef = options; optDef->name != nullptr; optDef++)
    {
        if (stricmp(optName, optDef->name)==0)
            return *optDef;
    }
    failx("Unknown option %s", optName);
}

class MySQLConnectionCloserThread : public Thread
{
    virtual int run() override;
public:
    static Semaphore closing;
} *connectionCloserThread = nullptr;


class MySQLConnection : public CInterface
{
public:
    MySQLConnection(MYSQL *_conn, const char *_cacheOptions, bool _threadCached, bool _globalCached) : conn(_conn), threadCached(_threadCached), globalCached(_globalCached)
    {
        if (_cacheOptions && (threadCached || globalCached))
            cacheOptions = strdup(_cacheOptions);
        else
            cacheOptions = nullptr;
        created = msTick();
    }
    ~MySQLConnection()
    {
        if (conn)
        {
            if (threadCached || globalCached)
            {
                Owned<MySQLConnection> cacheEntry = new MySQLConnection(*this); // Note - takes ownership of this->cacheOptions and this->conn
                cacheOptions = NULL;
                conn = NULL;
                if (threadCached)
                    setThreadCache(cacheEntry.getLink());
                else // globalCached
                {
                    CriticalBlock b(globalCacheCrit);
                    if (globalCachedConnections.length()==mysqlConnectionCacheSize)
                    {
                        MySQLConnection &goer = globalCachedConnections.popGet();
                        goer.globalCached = false;  // Make sure we don't recache it!
                        goer.Release();
                    }
                    globalCachedConnections.add(*cacheEntry.getClear(), 0);
                    if (!connectionCloserThread)
                    {
                        connectionCloserThread = new MySQLConnectionCloserThread;
                        connectionCloserThread->start();
                    }
                }
            }
            else
            {
                mysql_close(conn);
                free((char *) cacheOptions);
            }
        }
    }
    inline operator MYSQL *() const
    {
        return conn;
    }
    inline bool matches (const char *_options)
    {
        return _options && cacheOptions && streq(_options, cacheOptions);
    }

    static MySQLConnection *findCachedConnection(const char *options, bool bypassCache)
    {
        const char *server = "localhost";
        const char *user = "";
        const char *password = "";
        const char *database = "";
        bool hasMySQLOpt = false;
        bool threadCache = false;
        bool globalCache = true;
        unsigned port = 0;
        StringArray opts;
        opts.appendList(options, ",");
        ForEachItemIn(idx, opts)
        {
            const char *opt = opts.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
                val++;
                if (stricmp(optName, "server")==0)
                    server = val;   // Note that lifetime of val is adequate for this to be safe
                else if (stricmp(optName, "port")==0)
                    port = atoi(val);
                else if (stricmp(optName, "user")==0)
                    user = val;
                else if (stricmp(optName, "password")==0)
                    password = val;
                else if (stricmp(optName, "database")==0)
                    database = val;
                else if (stricmp(optName, "cache")==0)
                {
                    if (clipStrToBool(val) || strieq(val, "thread"))
                    {
                        threadCache = true;
                        globalCache = false;
                    }
                    else if (strieq(val, "global"))
                        globalCache = true;
                    else if (strieq(val, "none") || strieq(val, "false") || strieq(val, "off") || strieq(val, "0"))
                        globalCache = false;
                    else
                        failx("Unknown cache option %s", val);
                }
                else if (strnicmp(optName, "MYSQL_", 6)==0)
                    hasMySQLOpt = true;
                else
                    failx("Unknown option %s", optName.str());
            }
        }
        if (!bypassCache)
        {
            if (threadCache)
            {
                if (threadCachedConnection && threadCachedConnection->matches(options))
                {
                    MySQLConnection *ret = threadCachedConnection;
                    threadCachedConnection = nullptr;
                    return ret;
                }
            }
            else if (globalCache)
            {
                CriticalBlock b(globalCacheCrit);
                ForEachItemIn(idx, globalCachedConnections)
                {
                    MySQLConnection &cached = globalCachedConnections.item(idx);
                    if (cached.matches(options))
                    {
                        globalCachedConnections.remove(idx, true);
                        return &cached;
                    }
                }
            }
        }
        MySQLConnection::clearThreadCache();
        Owned<MySQLConnection> newConn = new MySQLConnection(mysql_init(NULL), options, threadCache, globalCache);
        if (hasMySQLOpt)
        {
            ForEachItemIn(idx, opts)
            {
                const char *opt = opts.item(idx);
                if (strnicmp(opt, "MYSQL_", 6)==0)
                {
                    const char *val = strchr(opt, '=');
                    StringBuffer optName(opt);
                    if (val)
                    {
                        optName.setLength(val-opt);
                        val++;
                    }
                    MySQLOptionDefinition &optDef = lookupOption(optName);
                    int rc;
                    if (optDef.paramType == ParamTypeNone)
                    {
                        if (val)
                            failx("Option %s does not take a value", optName.str());
                        rc = mysql_options(*newConn, optDef.option, nullptr);
                    }
                    else
                    {
                        if (!val)
                            failx("Option %s requires a value", optName.str());
                        switch (optDef.paramType)
                        {
                        case ParamTypeString:
                            rc = mysql_options(*newConn, optDef.option, val);
                            break;
                        case ParamTypeUInt:
                            {
                                unsigned int oval = strtoul(val, nullptr, 10);
                                rc = mysql_options(*newConn, optDef.option, (const char *) &oval);
                                break;
                            }
                        case ParamTypeULong:
                            {
                                unsigned long oval = strtoul(val, nullptr, 10);
                                rc = mysql_options(*newConn, optDef.option, (const char *) &oval);
                                break;
                            }
                        case ParamTypeBool:
                            {
                                my_bool oval = clipStrToBool(val);
                                rc = mysql_options(*newConn, optDef.option, (const char *) &oval);
                                break;
                            }
                        }
                    }
                    if (rc)
                        failx("Failed to set option %s (%s)", optName.str(), mysql_error(*newConn));
                }
            }
        }
        if (!mysql_real_connect(*newConn, server, user, password, database, port, NULL, 0))
            failx("Failed to connect (%s)", mysql_error(*newConn));
        return newConn.getClear();
    }

    static void clearThreadCache()
    {
        ::Release(threadCachedConnection);
        threadCachedConnection = NULL;
    }

    static void setThreadCache(MySQLConnection *connection)
    {
        clearThreadCache();
        threadCachedConnection = connection;
    }

    bool wasCached() const
    {
        return reusing;
    }

    MySQLConnection *reopen()
    {
        threadCached = false;
        globalCached = false;
        return findCachedConnection(cacheOptions, true);
    }

    static void retireCache(unsigned maxAge)
    {
        CriticalBlock b(globalCacheCrit);
        unsigned now = msTick();
        ForEachItemInRev(idx, globalCachedConnections)
        {
            MySQLConnection &cached = globalCachedConnections.item(idx);
            if (!maxAge || (now - cached.created > maxAge))
            {
                cached.globalCached = false;  // Make sure we don't re-add it!
                globalCachedConnections.remove(idx);
            }
        }
    }

private:
    MySQLConnection(const MySQLConnection &from)
    {
        conn = from.conn;  // Taking over ownership
        cacheOptions = from.cacheOptions;  // Taking over ownership
        threadCached = from.threadCached;
        globalCached = from.globalCached;
        reusing = true;
        created = msTick();
    }

    static CIArrayOf<MySQLConnection> globalCachedConnections;
    static CriticalSection globalCacheCrit;

    MYSQL *conn;
    const char *cacheOptions;  // Not done as a StringAttr, in order to avoid reallocation when recaching after use (see copy constructor above)
    unsigned created;
    bool threadCached;
    bool globalCached;
    bool reusing = false;
};

CIArrayOf<MySQLConnection> MySQLConnection::globalCachedConnections;
CriticalSection MySQLConnection::globalCacheCrit;

Semaphore MySQLConnectionCloserThread::closing;

int MySQLConnectionCloserThread::run()
{
    for (;;)
    {
        if (closing.wait(mysqlCacheCheckPeriod))
        {
            break;
        }
        MySQLConnection::retireCache(mysqlCacheTimeoutPeriod);
    }
    return 0;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    if (connectionCloserThread)
    {
        MySQLConnectionCloserThread::closing.signal();
        connectionCloserThread->join();
        connectionCloserThread->Release();
    }
    MySQLConnection::retireCache(0);
}


class MySQLResult : public CInterface
{
public:
    MySQLResult(MYSQL_RES *_res) : res(_res)
    {
    }
    ~MySQLResult()
    {
        if (res)
            mysql_free_result(res);
    }
    inline operator MYSQL_RES *() const
    {
        return res;
    }
private:
    MySQLResult(const MySQLResult &);
    MYSQL_RES *res;
};

class MySQLStatement : public CInterface
{
public:
    MySQLStatement(MYSQL_STMT *_stmt) : stmt(_stmt)
    {
    }
    ~MySQLStatement()
    {
        if (stmt)
            mysql_stmt_close(stmt);
    }
    inline operator MYSQL_STMT *() const
    {
        return stmt;
    }
private:
    MySQLStatement(const MySQLStatement &);
    MYSQL_STMT *stmt;
};

class MySQLBindingArray
{
public:
    MySQLBindingArray()
    {
        columns = 0;
        bindinfo = NULL;
        is_null = NULL;;
        error = NULL;
        lengths = NULL;
    }
    void init(unsigned count)
    {
        columns = count;
        if (columns)
        {
            bindinfo = new MYSQL_BIND [columns];
            is_null = new my_bool [columns];
            error = new my_bool [columns];
            lengths = new unsigned long [columns];
            memset(bindinfo, 0, columns * sizeof(bindinfo[0]));
            memset(is_null, 0, columns * sizeof(is_null[0]));
            memset(error, 0, columns * sizeof(error[0]));
            memset(lengths, 0, columns * sizeof(lengths[0]));
            for (int i = 0; i < columns; i++)
            {
                bindinfo[i].is_null = &is_null[i];
                bindinfo[i].length = &lengths[i];
                bindinfo[i].error = &error[i];
            }
        }
    }
    void bindResults(MYSQL_RES *res)
    {
        init(mysql_num_fields(res));
        for (int i = 0; i < columns; i++)
        {
            MYSQL_FIELD *col = mysql_fetch_field_direct(res, i);
            switch (col->type)
            {
            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                bindinfo[i].buffer_type = MYSQL_TYPE_STRING;
                bindinfo[i].buffer_length = 100;  // MORE - is there a better guess?
                break;
            case MYSQL_TYPE_TIMESTAMP:
            case MYSQL_TYPE_DATETIME:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATE:
                bindinfo[i].buffer_type = col->type;
                bindinfo[i].buffer_length = sizeof(MYSQL_TIME);
                break;
            default:
                bindinfo[i].buffer_type = col->type;
                bindinfo[i].buffer_length = col->length;
                break;
            }
            bindinfo[i].buffer = rtlMalloc(bindinfo[i].buffer_length);
        }
    }
    ~MySQLBindingArray()
    {
        for (int i = 0; i < columns; i++)
        {
            rtlFree(bindinfo[i].buffer);
        }
        delete [] bindinfo;
        delete [] is_null;
        delete [] error;
        delete [] lengths;
    }
    inline int numColumns() const
    {
        return columns;
    }
    inline MYSQL_BIND &queryColumn(int colIdx, const char *name) const
    {
        if (colIdx >= columns)
        {
            VStringBuffer error("No matching bound column for parameter %d", colIdx);
            if (name)
                error.appendf(" (%s)", name);
            fail(error);
        }
        return bindinfo[colIdx];
    }
    inline MYSQL_BIND *queryBindings() const
    {
        return bindinfo;
    }
private:
    MYSQL_BIND *bindinfo;
    my_bool *is_null;
    my_bool *error;
    unsigned long *lengths;
    int columns;
};

class MySQLPreparedStatement : public CInterface
{
public:
    MySQLPreparedStatement(MySQLConnection *_conn, MySQLStatement *_stmt)
    : conn(_conn), stmt(_stmt)
    {
        // Create bindings for input parameters
        inputBindings.init(mysql_stmt_param_count(*stmt));
        // Bindings for results are created after the execute, as they are not always available until then (e.g. when calling a stored procedure)
        if (mysql_stmt_errno(*stmt))
            fail(mysql_stmt_error(*stmt));
    }
    MySQLPreparedStatement(MySQLConnection *_conn, const char *_query, unsigned _len)
    : conn(_conn)
    {
        // Used for cases with no parameters or result, that are not supported in prepared query protocol - e.g. DROP PROCEDURE
        query.set(_query, _len);
        inputBindings.init(0);
    }
    ~MySQLPreparedStatement()
    {
        stop();
    }
    inline void stop()
    {
        res.clear();
        stmt.clear();
    }
    bool next()
    {
        if (!stmt)
            return false;
        int rc = mysql_stmt_fetch(*stmt);
        if (rc == MYSQL_NO_DATA)
            return false;
        else if (rc)
            fail(mysql_stmt_error(*stmt));
        else
            return true;
    }
    void execute()
    {
        if (stmt && *stmt)
        {
            // NOTE - we ignore all but the first result from stored procedures
#if MYSQL_VERSION_ID >= 50500
            while (mysql_stmt_next_result(*stmt)==0)
            {
                // Ignore and discard any additional results for the current row - typically the status result from a stored procedure call
                mysql_stmt_free_result(*stmt);
            }
#endif
            if (inputBindings.numColumns() && mysql_stmt_bind_param(*stmt, inputBindings.queryBindings()))
                fail(mysql_stmt_error(*stmt));
            if (mysql_stmt_execute(*stmt))
                fail(mysql_stmt_error(*stmt));
            res.setown(new MySQLResult(mysql_stmt_result_metadata(*stmt)));
            if (*res)
            {
                resultBindings.bindResults(*res);
                /* Bind the result buffers */
                if (mysql_stmt_bind_result(*stmt, resultBindings.queryBindings()))
                    fail(mysql_stmt_error(*stmt));
            }
        }
        else
        {
            if (mysql_query(*conn, query))
                fail(mysql_error(*conn));
        }
    }
    inline const MySQLBindingArray &queryResultBindings() const
    {
        return resultBindings;
    }
    inline const MySQLBindingArray &queryInputBindings() const
    {
        return inputBindings;
    }
    inline bool hasResult() const
    {
        return *res != NULL;
    }
protected:
    Linked<MySQLConnection> conn;
    Linked<MySQLStatement> stmt;
    Owned<MySQLResult> res;
    MySQLBindingArray inputBindings;
    MySQLBindingArray resultBindings;
    StringAttr query;
};

// Conversions from MySQL values to ECL data

__declspec(noreturn) static void typeError(const char *expected, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const RtlFieldInfo *field)
{
    VStringBuffer msg("mysql: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", field->name);
    rtlFail(0, msg.str());
}

static bool isInteger(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
        return true;
    default:
        return false;
    }
}

static bool isDateTime(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
        return true;
    default:
        return false;
    }
}

static bool isString(enum_field_types type)
{
    switch (type)
    {
    case MYSQL_TYPE_BIT:  // Slightly dubious but MySQL seems to represent them as string fields in their gui...
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VAR_STRING:
        return true;
    default:
        return false;
    }
}


static unsigned __int64 getDateTimeValue(const MYSQL_BIND &bound)
{
    const MYSQL_TIME * time = (const MYSQL_TIME *) bound.buffer;
    switch (bound.buffer_type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
        //What format should this be?  Possibly a timestamp_t
        return (unsigned __int64)((time->year * 10000) + (time->month * 100) + (time->day)) * 1000000 +
               (time->hour * 10000) + (time->minute * 100) + (time->second);
    case MYSQL_TYPE_DATE:
        return (time->year * 10000) + (time->month * 100) + (time->day);
    case MYSQL_TYPE_TIME:
        return (time->hour * 10000) + (time->minute * 100) + (time->second);
    default:
        throwUnexpected();
    }
}

static void getDateTimeText(const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    const MYSQL_TIME * time = (const MYSQL_TIME *) bound.buffer;
    char temp[20];
    switch (bound.buffer_type)
    {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
        _snprintf(temp, sizeof(temp), "%4u-%02u-%02u %02u:%02u:%02u", time->year, time->month, time->day, time->hour, time->minute, time->second);
        break;
    case MYSQL_TYPE_DATE:
        _snprintf(temp, sizeof(temp), "%4u-%02u-%02u", time->year, time->month, time->day);
        break;
    case MYSQL_TYPE_TIME:
        _snprintf(temp, sizeof(temp), "%02u:%02u:%02u", time->hour, time->minute, time->second);
        break;
    default:
        throwUnexpected();
    }
    rtlStrToStrX(chars, result, strlen(temp), temp);
}

static bool getBooleanResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.boolResult;
    }
    if (!isInteger(bound.buffer_type))
        typeError("boolean", field);
    return rtlReadUInt(bound.buffer, *bound.length) != 0;
}

static void getDataResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, void * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlStrToDataX(chars, result, p.resultChars, p.stringResult);
        return;
    }
    if (isString(bound.buffer_type))
        rtlStrToDataX(chars, result, *bound.length, bound.buffer);   // This feels like it may not work to me - will preallocate rather larger than we want
    else
        typeError("blob", field);
}

static __int64 getSignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound);
static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound);

static double getRealResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.doubleResult;
    }
    if (bound.buffer_type == MYSQL_TYPE_BIT)
        return (double) getUnsignedResult(field, bound);
    if (isInteger(bound.buffer_type))
    {
        if (bound.is_unsigned)
            return (double) getUnsignedResult(field, bound);
        else
            return (double) getSignedResult(field, bound);
    }
    else if (bound.buffer_type == MYSQL_TYPE_FLOAT)
        return * (float *) bound.buffer;
    else if (bound.buffer_type == MYSQL_TYPE_DOUBLE)
        return * (double *) bound.buffer;
    else
        typeError("double", field);
}

static __int64 getSignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.intResult;
    }
    if (isDateTime(bound.buffer_type))
        return getDateTimeValue(bound);
    if (bound.buffer_type == MYSQL_TYPE_BIT)
    {
        // These are stored as big-endian values it seems...
        return (__int64) rtlReadSwapUInt(bound.buffer, *bound.length);
    }
    if (isInteger(bound.buffer_type))
    {
        if (bound.is_unsigned)
            return (__int64) rtlReadUInt(bound.buffer, *bound.length);
        else
            return rtlReadInt(bound.buffer, *bound.length);
    }
    else
        typeError("integer", field);
}

static unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const MYSQL_BIND &bound)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        return p.uintResult;
    }
    if (isDateTime(bound.buffer_type))
        return getDateTimeValue(bound);
    if (bound.buffer_type == MYSQL_TYPE_BIT)
        return rtlReadSwapUInt(bound.buffer, *bound.length);
    if (!isInteger(bound.buffer_type))
        typeError("integer", field);
    if (bound.is_unsigned)
        return rtlReadUInt(bound.buffer, *bound.length);
    else
        return (unsigned __int64) rtlReadInt(bound.buffer, *bound.length);
}

static void getStringResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlStrToStrX(chars, result, p.resultChars, p.stringResult);
        return;
    }

    if (isDateTime(bound.buffer_type))
    {
        getDateTimeText(bound, chars, result);
        return;
    }
    if (!isString(bound.buffer_type))
        typeError("string", field);

    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToStrX(chars, result, numchars, text);
}

static void getUTF8Result(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, char * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
        return;
    }

    if (isDateTime(bound.buffer_type))
    {
        getDateTimeText(bound, chars, result);
        return;
    }
    if (!isString(bound.buffer_type))
        typeError("string", field);

    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToUtf8X(chars, result, numchars, text);
}

static void getUnicodeResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, size32_t &chars, UChar * &result)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
        return;
    }
    if (bound.buffer_type != MYSQL_TYPE_STRING && bound.buffer_type != MYSQL_TYPE_VAR_STRING)
        typeError("string", field);
    const char *text = (const char *) bound.buffer;
    unsigned long bytes = *bound.length;
    unsigned numchars = rtlUtf8Length(bytes, text);  // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
    rtlUtf8ToUnicodeX(chars, result, numchars, text);
}

static void getDecimalResult(const RtlFieldInfo *field, const MYSQL_BIND &bound, Decimal &value)
{
    if (*bound.is_null)
    {
        NullFieldProcessor p(field);
        value.set(p.decimalResult);
        return;
    }
    size32_t chars;
    rtlDataAttr result;
    mysqlembed::getStringResult(field, bound, chars, result.refstr());
    value.setString(chars, result.getstr());
    if (field)
    {
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
        value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
    }
}

static void createBindBuffer(MYSQL_BIND & bindInfo, enum_field_types sqlType, unsigned size)
{
    if (size)
    {
        if (!bindInfo.buffer)
        {
            bindInfo.buffer_type = sqlType;
            bindInfo.buffer = rtlMalloc(size);
        }
        else
            assertex(bindInfo.buffer_type == sqlType);
    }
    else
    {
        // Buffer is reallocated each time - caller is responsible for it.
        bindInfo.buffer_type = sqlType;
        rtlFree(bindInfo.buffer);
        bindInfo.buffer = NULL;
    }
}

// A MySQLRowBuilder object is used to construct an ECL row from a MySQL row

class MySQLRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    MySQLRowBuilder(const MySQLBindingArray &_resultInfo)
    : resultInfo(_resultInfo), colIdx(-1)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getBooleanResult(field, nextField(field));
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        mysqlembed::getDataResult(field, nextField(field), len, result);
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getRealResult(field, nextField(field));
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getSignedResult(field, nextField(field));
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        return mysqlembed::getUnsignedResult(field, nextField(field));
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        mysqlembed::getStringResult(field, nextField(field), chars, result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        mysqlembed::getUTF8Result(field, nextField(field), chars, result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        mysqlembed::getUnicodeResult(field, nextField(field), chars, result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        mysqlembed::getDecimalResult(field, nextField(field), value);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        UNSUPPORTED("SET fields");
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        UNSUPPORTED("Nested datasets");
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    const MYSQL_BIND &nextField(const RtlFieldInfo * field)
    {
        if (colIdx < resultInfo.numColumns())
            colIdx++;
        else
            fail("Too many fields in ECL output row");
        const MYSQL_BIND &column = resultInfo.queryColumn(colIdx,field->name);
        if (*column.error)
            failx("Error fetching column %s", field->name);
        return column;
    }
    const MySQLBindingArray &resultInfo;
    int colIdx;
};

// Bind MySQL variables from an ECL record

class MySQLRecordBinder : public CInterfaceOf<IFieldProcessor>
{
public:
    MySQLRecordBinder(const RtlTypeInfo *_typeInfo, const MySQLBindingArray &_bindings, int _firstParam)
      : typeInfo(_typeInfo), bindings(_bindings), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
    {
    }
    int numFields()
    {
        int count = 0;
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields++)
            count++;
        return count;
    }
    void processRow(const byte *row)
    {
        thisParam = firstParam;
        typeInfo->process(row, row, &dummyField, *this);   // Bind the variables for the current row
    }
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlStrToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_TINY, sizeof(value));
        * (bool *) bindInfo.buffer = value;
        bindInfo.is_unsigned = true;
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        size32_t bytes;
        void *data;
        rtlStrToDataX(bytes, data, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_BLOB, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_LONGLONG, sizeof(value));
        * (__int64 *) bindInfo.buffer = value;
        bindInfo.is_unsigned = false;
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_LONGLONG, sizeof(value));
        * (unsigned __int64 *) bindInfo.buffer = value;
        bindInfo.is_unsigned = true;
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_DOUBLE, sizeof(value));
        * (double *) bindInfo.buffer = value;
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        char *data;
        val.setDecimal(digits, precision, value);
        val.getStringX(bytes, data);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        char *data;
        val.setUDecimal(digits, precision, value);
        val.getStringX(bytes, data);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processString(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUtf8ToUtf8X(utf8chars, utf8, len, value);
        MYSQL_BIND &bindInfo = createBindBuffer(MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        UNSUPPORTED("SET fields");
        return false;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        UNSUPPORTED("Nested datasets");
        return false;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        throwUnexpected();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    MYSQL_BIND &createBindBuffer(enum_field_types sqlType, unsigned size)
    {
        MYSQL_BIND &bindInfo = bindings.queryColumn(thisParam++, NULL);
        mysqlembed::createBindBuffer(bindInfo, sqlType, size);
        return bindInfo;
    }
    const RtlTypeInfo *typeInfo;
    const MySQLBindingArray &bindings;
    int firstParam;
    RtlFieldStrInfo dummyField;
    int thisParam;
};

//

class MySQLDatasetBinder : public MySQLRecordBinder
{
public:
    MySQLDatasetBinder(IRowStream * _input, const RtlTypeInfo *_typeInfo, const MySQLBindingArray &_bindings, int _firstParam)
      : input(_input), MySQLRecordBinder(_typeInfo, _bindings, _firstParam)
    {
    }
    bool bindNext()
    {
        roxiemem::OwnedConstRoxieRow nextRow = (const byte *) input->ungroupedNextRow();
        if (!nextRow)
            return false;
        processRow((const byte *) nextRow.get());   // Bind the variables for the current row
        return true;
    }
    void executeAll(MySQLPreparedStatement *stmtInfo)
    {
        while (bindNext())
        {
            stmtInfo->execute();
        }
    }
protected:
    Owned<IRowStream> input;
};

// A MySQL function that returns a dataset will return a MySQLRowStream object that can be
// interrogated to return each row of the result in turn

class MySQLRowStream : public CInterfaceOf<IRowStream>
{
public:
    MySQLRowStream(MySQLDatasetBinder *_inputStream, MySQLPreparedStatement *_stmtInfo, IEngineRowAllocator *_resultAllocator)
    : inputStream(_inputStream), stmtInfo(_stmtInfo), resultAllocator(_resultAllocator)
    {
        executePending = true;
        eof = false;
    }
    virtual const void *nextRow()
    {
        // A little complex when streaming data in as well as out - want to execute for every input record
        if (eof)
            return NULL;
        for (;;)
        {
            if (executePending)
            {
                executePending = false;
                if (inputStream && !inputStream->bindNext())
                {
                    noteEOF();
                    return NULL;
                }
                stmtInfo->execute();
            }
            if (stmtInfo->next())
                break;
            if (inputStream)
                executePending = true;
            else
            {
                noteEOF();
                return NULL;
            }
        }
        RtlDynamicRowBuilder rowBuilder(resultAllocator);
        MySQLRowBuilder mysqlRowBuilder(stmtInfo->queryResultBindings());
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, mysqlRowBuilder);
        return rowBuilder.finalizeRowClear(len);
    }
    virtual void stop()
    {
        resultAllocator.clear();
        stmtInfo->stop();
    }

protected:
    void noteEOF()
    {
        if (!eof)
        {
            eof = true;
            stop();
        }
    }
    Linked<MySQLDatasetBinder> inputStream;
    Linked<MySQLPreparedStatement> stmtInfo;
    Linked<IEngineRowAllocator> resultAllocator;
    bool executePending;
    bool eof;
};

// Each call to a MySQL function will use a new MySQLEmbedFunctionContext object

static bool mysqlInitialized = false;
static __thread bool mysqlThreadInitialized = false;
static CriticalSection initCrit;

static bool terminateMySqlThread(bool isPooled)
{
    MySQLConnection::clearThreadCache();
    mysql_thread_end();
    mysqlThreadInitialized = false;  // In case it was a threadpool thread...
    return false;
}

static void initializeMySqlThread()
{
    if (!mysqlThreadInitialized)
    {
        {
            CriticalBlock b(initCrit);
            if (!mysqlInitialized)
            {
                mysqlInitialized = true;
                mysql_library_init(0, NULL, NULL);
            }
        }
        mysql_thread_init();
        addThreadTermFunc(terminateMySqlThread);
        mysqlThreadInitialized = true;
    }
}

class MySQLEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    MySQLEmbedFunctionContext(const IThorActivityContext *_ctx, unsigned _flags, const char *options)
      : flags(_flags), nextParam(0), activityCtx(_ctx)
    {
        initializeMySqlThread();
        conn.setown(MySQLConnection::findCachedConnection(options, false));
    }
    virtual bool getBooleanResult()
    {
        bool ret = mysqlembed::getBooleanResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getDataResult(size32_t &len, void * &result)
    {
        mysqlembed::getDataResult(NULL, getScalarResult(), len, result);
        checkSingleRow();
    }
    virtual double getRealResult()
    {
        double ret = mysqlembed::getRealResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual __int64 getSignedResult()
    {
        __int64 ret = mysqlembed::getSignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        unsigned __int64 ret = mysqlembed::getUnsignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getStringResult(size32_t &chars, char * &result)
    {
        mysqlembed::getStringResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        mysqlembed::getUTF8Result(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        mysqlembed::getUnicodeResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getDecimalResult(Decimal &value)
    {
        mysqlembed::getDecimalResult(NULL, getScalarResult(), value);
        checkSingleRow();
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        UNSUPPORTED("SET results");
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new MySQLRowStream(inputStream, stmtInfo, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        lazyExecute();
        if (!stmtInfo->hasResult())
            typeError("row", NULL);
        MySQLRowStream stream(NULL, stmtInfo, _resultAllocator);
        roxiemem::OwnedConstRoxieRow ret = stream.nextRow();
        roxiemem::OwnedConstRoxieRow ret2 = stream.nextRow();
        stream.stop();
        if (ret ==  NULL || ret2 != NULL)  // Check for exactly one returned row
            typeError("row", NULL);
        return (byte *) ret.getClear();
    }
    virtual size32_t getTransformResult(ARowBuilder & rowBuilder)
    {
        lazyExecute();
        if (!stmtInfo->next())
            typeError("row", NULL);
        MySQLRowBuilder mysqlRowBuilder(stmtInfo->queryResultBindings());
        const RtlTypeInfo *typeInfo = rowBuilder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t ret = typeInfo->build(rowBuilder, 0, &dummyField, mysqlRowBuilder);
        if (stmtInfo->next())
            typeError("row", NULL);  // Check that a single row was returned
        return ret;
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val) override
    {
        MySQLRecordBinder binder(metaVal.queryTypeInfo(), stmtInfo->queryInputBindings(), nextParam);
        binder.processRow(val);
        nextParam += binder.numFields();
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        // We only support a single dataset parameter...
        if (inputStream)
        {
            fail("At most one dataset parameter supported");
        }
        inputStream.setown(new MySQLDatasetBinder(LINK(val), metaVal.queryTypeInfo(), stmtInfo->queryInputBindings(), nextParam));
        nextParam += inputStream->numFields();
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_TINY, sizeof(val));
        * (bool *) bindInfo.buffer = val;
        bindInfo.is_unsigned = true;
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        size32_t bytes;
        void *data;
        rtlStrToDataX(bytes, data, len, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_BLOB, 0);
        bindInfo.buffer = data;
        bindInfo.buffer_length = bytes;
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_FLOAT, sizeof(val));
        * (float *) bindInfo.buffer = val;
    }
    virtual void bindRealParam(const char *name, double val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_DOUBLE, sizeof(val));
        * (double *) bindInfo.buffer = val;
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        bindSignedParam(name, val);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_LONGLONG, sizeof(val));
        * (__int64 *) bindInfo.buffer = val;
        bindInfo.is_unsigned = false;
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        bindUnsignedParam(name, val);
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_LONGLONG, sizeof(val));
        * (unsigned __int64 *) bindInfo.buffer = val;
        bindInfo.is_unsigned = true;
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlStrToUtf8X(utf8chars, utf8, len, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUtf8ToUtf8X(utf8chars, utf8, chars, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, chars, val);
        MYSQL_BIND &bindInfo = findParameter(name, MYSQL_TYPE_STRING, 0);
        bindInfo.buffer = utf8;
        bindInfo.buffer_length = rtlUtf8Size(utf8chars, utf8);
        bindInfo.length = &bindInfo.buffer_length;
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
    {
        UNSUPPORTED("SET parameters");  // MySQL does support sets, so MIGHT be possible...
    }
    virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
    {
        return NULL;
    }
    virtual void paramWriterCommit(IInterface *writer)
    {
    }
    virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
    {
    }

    virtual void importFunction(size32_t lenChars, const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t chars, const char *script)
    {
        StringBuffer scriptStr;
        size32_t len;
        if (activityCtx)
        {
            rtlSubstituteActivityContext(scriptStr, activityCtx, chars, script);
            script = scriptStr.str();
            len = scriptStr.length();
        }
        else
            len = rtlUtf8Size(chars, script);
        for (;;)
        {
            Owned<MySQLStatement> stmt  = new MySQLStatement(mysql_stmt_init(*conn));
            if (!*stmt)
                fail("failed to create statement");
            if (mysql_stmt_prepare(*stmt, script, len))
            {
                int rc = mysql_stmt_errno(*stmt);
                if (rc == ER_UNSUPPORTED_PS)
                {
                    // Some functions are not supported in prepared statements, but are still handy to be able to call
                    // So long as they have no bound vars and no return value, we can probably call them ok
                    if ((flags & (EFnoreturn|EFnoparams)) == (EFnoreturn|EFnoparams))
                    {
                        stmtInfo.setown(new MySQLPreparedStatement(conn, script, len));
                        break;
                    }
                    fail(mysql_stmt_error(*stmt));
                }
                // If we get an error, it could be that the cached connection is stale - retry
                if (conn->wasCached())
                {
                    conn.setown(conn->reopen());
                    continue;
                }
                fail(mysql_stmt_error(*stmt));
            }
            stmtInfo.setown(new MySQLPreparedStatement(conn, stmt));
            break;
        }
    }
    virtual void callFunction()
    {
        if (nextParam != stmtInfo->queryInputBindings().numColumns())
            failx("Not enough parameters supplied (%d parameters supplied, but statement has %d bound columns)", nextParam, stmtInfo->queryInputBindings().numColumns());
        // We actually do the execute later, when the result is fetched
        // Unless, there is no expected result, in that case execute query now
        if (flags & EFnoreturn)
            lazyExecute();
    }
    virtual void loadCompiledScript(size32_t chars, const void *_script) override
    {
        throwUnexpected();
    }
    virtual void enter() override {}
    virtual void reenter(ICodeContext *codeCtx) override {}
    virtual void exit() override {}
protected:
    void lazyExecute()
    {
        if (inputStream)
            inputStream->executeAll(stmtInfo);
        else
            stmtInfo->execute();
    }
    const MYSQL_BIND &getScalarResult()
    {
        lazyExecute();
        if (!stmtInfo->hasResult() || stmtInfo->queryResultBindings().numColumns() != 1)
            typeError("scalar", NULL);
        if (!stmtInfo->next())
            typeError("scalar", NULL);
        return stmtInfo->queryResultBindings().queryColumn(0, NULL);
    }
    void checkSingleRow()
    {
        if (stmtInfo->next())
            typeError("scalar", NULL);
    }
    inline MYSQL_BIND &findParameter(const char *name, enum_field_types sqlType, unsigned size)
    {
        // Everything is positional in MySQL
        MYSQL_BIND &bindInfo = stmtInfo->queryInputBindings().queryColumn(nextParam++, name);
        createBindBuffer(bindInfo, sqlType, size);
        return bindInfo;
    }
    Owned<MySQLConnection> conn;
    Owned<MySQLPreparedStatement> stmtInfo;
    Owned<MySQLDatasetBinder> inputStream;
    const IThorActivityContext *activityCtx;
    unsigned flags;
    int nextParam;
};

class MySQLEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
    {
        return createFunctionContextEx(nullptr, nullptr, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
    {
        if (flags & EFimport)
            UNSUPPORTED("IMPORT");
        else
            return new MySQLEmbedFunctionContext(activityCtx, flags, options);
    }
    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
    {
        throwUnexpected();
    }
};

extern DECL_EXPORT IEmbedContext* getEmbedContext()
{
    return new MySQLEmbedContext();
}

extern DECL_EXPORT bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
