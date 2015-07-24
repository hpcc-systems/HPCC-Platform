/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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
#include "cassandra.h"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "hqlplugins.hpp"
#include "deftype.hpp"
#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlembed.hpp"
#include "roxiemem.hpp"
#include "nbcd.hpp"
#include "jsort.hpp"
#include "jptree.hpp"
#include "jregexp.hpp"

#include "workunit.hpp"
#include "workunit.ipp"

#include "cassandraembed.hpp"

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

static const char * compatibleVersions[] = {
    "Cassandra Embed Helper 1.0.0",
    NULL };

static const char *version = "Cassandra Embed Helper 1.0.0";

extern "C" EXPORT bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
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
    pb->moduleName = "cassandra";
    pb->ECL = NULL;
    pb->flags = PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "Cassandra Embed Helper";
    return true;
}

namespace cassandraembed {

extern void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "UNSUPPORTED feature: %s not supported in Cassandra plugin", feature);
}

static void logCallBack(const CassLogMessage *message, void *data)
{
    DBGLOG("cassandra: %s - %s", cass_log_level_string(message->severity), message->message);
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    cass_log_set_callback(logCallBack, NULL);
    cass_log_set_level(CASS_LOG_WARN);
    return true;
}

extern void failx(const char *message, ...)
{
    va_list args;
    va_start(args,message);
    StringBuffer msg;
    msg.append("cassandra: ").valist_appendf(message,args);
    va_end(args);
    rtlFail(0, msg.str());
}

extern void fail(const char *message)
{
    StringBuffer msg;
    msg.append("cassandra: ").append(message);
    rtlFail(0, msg.str());
}

void check(CassError rc)
{
    if (rc != CASS_OK)
    {
        fail(cass_error_desc(rc));
    }
}

// Wrappers to Cassandra structures that require corresponding releases

void CassandraCluster::setOptions(const StringArray &options)
{
    const char *contact_points = "localhost";
    const char *user = "";
    const char *password = "";
    ForEachItemIn(idx, options)
    {
        const char *opt = options.item(idx);
        const char *val = strchr(opt, '=');
        if (val)
        {
            StringBuffer optName(val-opt, opt);
            val++;
            if (stricmp(optName, "contact_points")==0 || stricmp(optName, "server")==0)
                contact_points = val;   // Note that lifetime of val is adequate for this to be safe
            else if (stricmp(optName, "user")==0)
                user = val;
            else if (stricmp(optName, "password")==0)
                password = val;
            else if (stricmp(optName, "keyspace")==0)
                keyspace.set(val);
            else if (stricmp(optName, "batch")==0)
            {
                if (stricmp(val, "LOGGED")==0)
                    batchMode = CASS_BATCH_TYPE_LOGGED;
                else if (stricmp(val, "UNLOGGED")==0)
                    batchMode = CASS_BATCH_TYPE_UNLOGGED;
                else if (stricmp(val, "COUNTER")==0)
                    batchMode = CASS_BATCH_TYPE_COUNTER;
            }
            else if (stricmp(optName, "pageSize")==0)
                pageSize = getUnsignedOption(val, "pageSize");
            else if (stricmp(optName, "maxFutures")==0)
                maxFutures=getUnsignedOption(val, "maxFutures");
            else if (stricmp(optName, "maxRetries")==0)
                maxRetries=getUnsignedOption(val, "maxRetries");
            else if (stricmp(optName, "port")==0)
            {
                unsigned port = getUnsignedOption(val, "port");
                checkSetOption(cass_cluster_set_port(cluster, port), "port");
            }
            else if (stricmp(optName, "protocol_version")==0)
            {
                unsigned protocol_version = getUnsignedOption(val, "protocol_version");
                checkSetOption(cass_cluster_set_protocol_version(cluster, protocol_version), "protocol_version");
            }
            else if (stricmp(optName, "num_threads_io")==0)
            {
                unsigned num_threads_io = getUnsignedOption(val, "num_threads_io");
                cass_cluster_set_num_threads_io(cluster, num_threads_io);  // No status return
            }
            else if (stricmp(optName, "queue_size_io")==0)
            {
                unsigned queue_size_io = getUnsignedOption(val, "queue_size_io");
                checkSetOption(cass_cluster_set_queue_size_io(cluster, queue_size_io), "queue_size_io");
            }
            else if (stricmp(optName, "core_connections_per_host")==0)
            {
                unsigned core_connections_per_host = getUnsignedOption(val, "core_connections_per_host");
                checkSetOption(cass_cluster_set_core_connections_per_host(cluster, core_connections_per_host), "core_connections_per_host");
            }
            else if (stricmp(optName, "max_connections_per_host")==0)
            {
                unsigned max_connections_per_host = getUnsignedOption(val, "max_connections_per_host");
                checkSetOption(cass_cluster_set_max_connections_per_host(cluster, max_connections_per_host), "max_connections_per_host");
            }
            else if (stricmp(optName, "max_concurrent_creation")==0)
            {
                unsigned max_concurrent_creation = getUnsignedOption(val, "max_concurrent_creation");
                checkSetOption(cass_cluster_set_max_concurrent_creation(cluster, max_concurrent_creation), "max_concurrent_creation");
            }
            else if (stricmp(optName, "pending_requests_high_water_mark")==0)
            {
                unsigned pending_requests_high_water_mark = getUnsignedOption(val, "pending_requests_high_water_mark");
                checkSetOption(cass_cluster_set_pending_requests_high_water_mark(cluster, pending_requests_high_water_mark), "pending_requests_high_water_mark");
            }
            else if (stricmp(optName, "pending_requests_low_water_mark")==0)
            {
                unsigned pending_requests_low_water_mark = getUnsignedOption(val, "pending_requests_low_water_mark");
                checkSetOption(cass_cluster_set_pending_requests_low_water_mark(cluster, pending_requests_low_water_mark), "pending_requests_low_water_mark");
            }
            else if (stricmp(optName, "max_concurrent_requests_threshold")==0)
            {
                unsigned max_concurrent_requests_threshold = getUnsignedOption(val, "max_concurrent_requests_threshold");
                checkSetOption(cass_cluster_set_max_concurrent_requests_threshold(cluster, max_concurrent_requests_threshold), "max_concurrent_requests_threshold");
            }
            else if (stricmp(optName, "connect_timeout")==0)
            {
                unsigned connect_timeout = getUnsignedOption(val, "connect_timeout");
                cass_cluster_set_connect_timeout(cluster, connect_timeout);
            }
            else if (stricmp(optName, "request_timeout")==0)
            {
                unsigned request_timeout = getUnsignedOption(val, "request_timeout");
                cass_cluster_set_request_timeout(cluster, request_timeout);
            }
            else if (stricmp(optName, "load_balance_round_robin")==0)
            {
                cass_bool_t enable = getBoolOption(val, "load_balance_round_robin");
                if (enable==cass_true)
                    cass_cluster_set_load_balance_round_robin(cluster);
            }
            else if (stricmp(optName, "load_balance_dc_aware")==0)
            {
                StringArray lbargs;
                lbargs.appendList(val, "|");
                if (lbargs.length() != 3)
                    failx("Invalid value '%s' for option %s - expected 3 subvalues (separate with |)", val, optName.str());
                unsigned usedPerRemote = getUnsignedOption(lbargs.item(2), "load_balance_dc_aware");
                cass_bool_t allowRemote = getBoolOption(lbargs.item(2), "load_balance_dc_aware");
                checkSetOption(cass_cluster_set_load_balance_dc_aware(cluster, lbargs.item(0), usedPerRemote, allowRemote), "load_balance_dc_aware");
            }
            else if (stricmp(optName, "token_aware_routing")==0)
            {
                cass_bool_t enable = getBoolOption(val, "token_aware_routing");
                cass_cluster_set_token_aware_routing(cluster, enable);
            }
            else if (stricmp(optName, "latency_aware_routing")==0)
            {
                cass_bool_t enable = getBoolOption(val, "latency_aware_routing");
                cass_cluster_set_latency_aware_routing(cluster, enable);
            }
            else if (stricmp(optName, "latency_aware_routing_settings")==0)
            {
                StringArray subargs;
                subargs.appendList(val, "|");
                if (subargs.length() != 5)
                    failx("Invalid value '%s' for option %s - expected 5 subvalues (separate with |)", val, optName.str());
                cass_double_t exclusion_threshold = getDoubleOption(subargs.item(0), "exclusion_threshold");
                cass_uint64_t scale_ms = getUnsigned64Option(subargs.item(1), "scale_ms");
                cass_uint64_t retry_period_ms = getUnsigned64Option(subargs.item(2), "retry_period_ms");
                cass_uint64_t update_rate_ms = getUnsigned64Option(subargs.item(3), "update_rate_ms");
                cass_uint64_t min_measured = getUnsigned64Option(subargs.item(4), "min_measured");
                cass_cluster_set_latency_aware_routing_settings(cluster, exclusion_threshold, scale_ms, retry_period_ms, update_rate_ms, min_measured);
            }
            else if (stricmp(optName, "tcp_nodelay")==0)
            {
                cass_bool_t enable = getBoolOption(val, "tcp_nodelay");
                cass_cluster_set_tcp_nodelay(cluster, enable);
            }
            else if (stricmp(optName, "tcp_keepalive")==0)
            {
                StringArray subargs;
                subargs.appendList(val, "|");
                if (subargs.length() != 2)
                    failx("Invalid value '%s' for option %s - expected 2 subvalues (separate with |)", val, optName.str());
                cass_bool_t enabled = getBoolOption(subargs.item(0), "enabled");
                unsigned delay_secs = getUnsignedOption(subargs.item(0), "delay_secs");
                cass_cluster_set_tcp_keepalive(cluster, enabled, delay_secs);
            }
            else
                failx("Unrecognized option %s", optName.str());
        }
    }
    cass_cluster_set_contact_points(cluster, contact_points);
    if (*user || *password)
        cass_cluster_set_credentials(cluster, user, password);
}

void CassandraCluster::checkSetOption(CassError rc, const char *name)
{
    if (rc != CASS_OK)
    {
        failx("While setting option %s: %s", name, cass_error_desc(rc));
    }
}
cass_bool_t CassandraCluster::getBoolOption(const char *val, const char *option)
{
    return strToBool(val) ? cass_true : cass_false;
}
unsigned CassandraCluster::getUnsignedOption(const char *val, const char *option)
{
    char *endp;
    long value = strtoul(val, &endp, 0);
    if (endp==val || *endp != '\0' || value > UINT_MAX || value < 0)
        failx("Invalid value '%s' for option %s", val, option);
    return (unsigned) value;
}
unsigned CassandraCluster::getDoubleOption(const char *val, const char *option)
{
    char *endp;
    double value = strtod(val, &endp);
    if (endp==val || *endp != '\0')
        failx("Invalid value '%s' for option %s", val, option);
    return value;
}
__uint64 CassandraCluster::getUnsigned64Option(const char *val, const char *option)
{
    // MORE - could check it's all digits (with optional leading spaces...), if we cared.
    return rtlVStrToUInt8(val);
}

//------------------

void CassandraFuture::wait(const char *why) const
{
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    if(rc != CASS_OK)
    {
        const char *message;
        size_t length;
        cass_future_error_message(future, &message, &length);
        VStringBuffer err("cassandra: failed to %s (%.*s)", why, (int) length, message);
#ifdef _DEBUG
        DBGLOG("%s", err.str());
#endif
        rtlFail(0, err.str());
    }
}

void CassandraSession::set(CassSession *_session)
{
    if (session)
    {
        CassandraFuture close_future(cass_session_close(session));
        cass_future_wait(close_future);
        cass_session_free(session);
    }
    session = _session;
}

//----------------------

CassandraRetryingFuture::CassandraRetryingFuture(CassSession *_session, CassStatement *_statement, Semaphore *_limiter, unsigned _retries)
: session(_session), statement(_statement), retries(_retries), limiter(_limiter), future(NULL)
{
    execute();
}

CassandraRetryingFuture::~CassandraRetryingFuture()
{
    if (future)
        cass_future_free(future);
}

void CassandraRetryingFuture::wait(const char *why)
{
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    if(rc != CASS_OK)
    {
        switch (rc)
        {
        case CASS_ERROR_LIB_NO_HOSTS_AVAILABLE: // MORE - are there others we should retry?
            if (retry(why))
                break;
            // fall into
        default:
            const char *message;
            size_t length;
            cass_future_error_message(future, &message, &length);
            VStringBuffer err("cassandra: failed to %s (%.*s)", why, (int) length, message);
            rtlFail(0, err.str());
        }
    }
}

bool CassandraRetryingFuture::retry(const char *why)
{
    for (int i = 0; i < retries; i++)
    {
        execute();
        cass_future_wait(future);
        CassError rc = cass_future_error_code(future);
        if(rc == CASS_OK)
            return true;
    }
    return false;
}

void CassandraRetryingFuture::execute()
{
    if (limiter)
        limiter->wait();
    future = cass_session_execute(session, statement);
    if (limiter)
        cass_future_set_callback(future, signaller, this); // Note - this will call the callback if the future has already completed
}

void CassandraRetryingFuture::signaller(CassFuture *future, void *data)
{
    CassandraRetryingFuture *self = (CassandraRetryingFuture *) data;
    if (self && self->limiter)
        self->limiter->signal();
}

//----------------------

CassandraStatementInfo::CassandraStatementInfo(CassandraSession *_session, CassandraPrepared *_prepared, unsigned _numBindings, CassBatchType _batchMode, unsigned pageSize, unsigned _maxFutures, unsigned _maxRetries)
    : session(_session), prepared(_prepared), numBindings(_numBindings), batchMode(_batchMode), semaphore(NULL), maxFutures(_maxFutures), maxRetries(_maxRetries)
{
    assertex(prepared && *prepared);
    statement.setown(new CassandraStatement(cass_prepared_bind(*prepared)));
    if (pageSize)
        cass_statement_set_paging_size(*statement, pageSize);
    inBatch = false;
}
CassandraStatementInfo::~CassandraStatementInfo()
{
    stop();
    futures.kill();
    delete semaphore;
}
void CassandraStatementInfo::stop()
{
    iterator.clear();
    result.clear();
    prepared.clear();
}
bool CassandraStatementInfo::next()
{
    loop
    {
        if (!iterator)
        {
            if (result)
                iterator.setown(new CassandraIterator(cass_iterator_from_result(*result)));
            else
                return false;
        }
        if (cass_iterator_next(*iterator))
            return true;
        iterator.clear();
        if (!cass_result_has_more_pages(*result))
        {
            result.clear();
            break;
        }
        cass_statement_set_paging_state(*statement, *result);
        result.setown(new CassandraFutureResult(cass_session_execute(*session, *statement)));
    }
    return false;
}
void CassandraStatementInfo::startStream()
{
    if (batchMode != (CassBatchType) -1)
        batch.setown(new CassandraBatch(cass_batch_new(batchMode)));
    else
        semaphore = new Semaphore(maxFutures ? maxFutures : 100);
    statement.setown(new CassandraStatement(cass_prepared_bind(*prepared)));
    inBatch = true;
}
void CassandraStatementInfo::endStream()
{
    if (batch)
    {
        result.setown(new CassandraFutureResult (cass_session_execute_batch(*session, *batch)));
        assertex (rowCount() == 0);
    }
    else
    {
        ForEachItemIn(idx, futures)
        {
            futures.item(idx).wait("endStream");
        }
    }
}
void CassandraStatementInfo::execute()
{
    assertex(statement && *statement);
    if (batch)
    {
        check(cass_batch_add_statement(*batch, *statement));
        statement.setown(new CassandraStatement(cass_prepared_bind(*prepared)));
    }
    else if (inBatch)
    {
        futures.append(*new CassandraRetryingFuture(*session, statement->getClear(), semaphore, maxRetries));
        statement.setown(new CassandraStatement(cass_prepared_bind(*prepared)));
    }
    else
    {
        result.setown(new CassandraFutureResult(cass_session_execute(*session, *statement)));
    }
}

// Conversions from Cassandra values to ECL data

static const char *getTypeName(CassValueType type)
{
    switch (type)
    {
    case CASS_VALUE_TYPE_CUSTOM: return "CUSTOM";
    case CASS_VALUE_TYPE_ASCII: return "ASCII";
    case CASS_VALUE_TYPE_BIGINT: return "BIGINT";
    case CASS_VALUE_TYPE_BLOB: return "BLOB";
    case CASS_VALUE_TYPE_BOOLEAN: return "BOOLEAN";
    case CASS_VALUE_TYPE_COUNTER: return "COUNTER";
    case CASS_VALUE_TYPE_DECIMAL: return "DECIMAL";
    case CASS_VALUE_TYPE_DOUBLE: return "DOUBLE";
    case CASS_VALUE_TYPE_FLOAT: return "FLOAT";
    case CASS_VALUE_TYPE_INT: return "INT";
    case CASS_VALUE_TYPE_TEXT: return "TEXT";
    case CASS_VALUE_TYPE_TIMESTAMP: return "TIMESTAMP";
    case CASS_VALUE_TYPE_UUID: return "UUID";
    case CASS_VALUE_TYPE_VARCHAR: return "VARCHAR";
    case CASS_VALUE_TYPE_VARINT: return "VARINT";
    case CASS_VALUE_TYPE_TIMEUUID: return "TIMEUUID";
    case CASS_VALUE_TYPE_INET: return "INET";
    case CASS_VALUE_TYPE_LIST: return "LIST";
    case CASS_VALUE_TYPE_MAP: return "MAP";
    case CASS_VALUE_TYPE_SET: return "SET";
    default: return "UNKNOWN";
    }
}
static void typeError(const char *expected, const CassValue *value, const RtlFieldInfo *field) __attribute__((noreturn));

static void typeError(const char *expected, const CassValue *value, const RtlFieldInfo *field)
{
    VStringBuffer msg("cassandra: type mismatch - %s expected", expected);
    if (field)
        msg.appendf(" for field %s", str(field->name));
    if (value)
        msg.appendf(", received %s", getTypeName(cass_value_type(value)));
    rtlFail(0, msg.str());
}

extern bool isInteger(const CassValueType t)
{
    switch (t)
    {
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_INT:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_VARINT:
        return true;
    default:
        return false;
    }
}

extern bool isString(CassValueType t)
{
    switch (t)
    {
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
    case CASS_VALUE_TYPE_ASCII:
        return true;
    default:
        return false;
    }
}

// when extracting elements of a set, field will point at the SET info- we want to get the typeInfo for the element type
static const RtlTypeInfo *getFieldBaseType(const RtlFieldInfo *field)
{
   const RtlTypeInfo *type = field->type;
   if ((type->fieldType & RFTMkind) == type_set)
       return type->queryChildType();
   else
       return type;
}

static int getNumFields(const RtlTypeInfo *record)
{
    int count = 0;
    const RtlFieldInfo * const *fields = record->queryFields();
    assertex(fields);
    while (*fields++)
        count++;
    return count;
}

extern bool getBooleanResult(const RtlFieldInfo *field, const CassValue *value)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        return p.boolResult;
    }
    if (cass_value_type(value) != CASS_VALUE_TYPE_BOOLEAN)
        typeError("boolean", value, field);
    cass_bool_t output;
    check(cass_value_get_bool(value, &output));
    return output != cass_false;
}

extern void getDataResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, void * &result)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        rtlStrToDataX(chars, result, p.resultChars, p.stringResult);
        return;
    }
    // We COULD require that the field being retrieved is a blob - but Cassandra seems happy to use any field here, and
    // it seems like it could be more useful to support anything
    // if (cass_value_type(value) != CASS_VALUE_TYPE_BLOB)
    //     typeError("blob", value, field);
    const cass_byte_t *bytes;
    size_t size;
    check(cass_value_get_bytes(value, &bytes, &size));
    rtlStrToDataX(chars, result, size, bytes);
}

extern double getRealResult(const RtlFieldInfo *field, const CassValue *value)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        return p.doubleResult;
    }
    else if (isInteger(cass_value_type(value)))
        return (double) getSignedResult(field, value);
    else switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_FLOAT:
    {
        cass_float_t output_f;
        check(cass_value_get_float(value, &output_f));
        return output_f;
    }
    case CASS_VALUE_TYPE_DOUBLE:
    {
        cass_double_t output_d;
        check(cass_value_get_double(value, &output_d));
        return output_d;
    }
    default:
        typeError("double", value, field);
    }
}

extern __int64 getSignedResult(const RtlFieldInfo *field, const CassValue *value)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        return p.intResult;
    }
    switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_INT:
    {
        cass_int32_t output;
        check(cass_value_get_int32(value, &output));
        return output;
    }
    case CASS_VALUE_TYPE_TIMESTAMP:
    case CASS_VALUE_TYPE_BIGINT:
    case CASS_VALUE_TYPE_COUNTER:
    case CASS_VALUE_TYPE_VARINT:
    {
        cass_int64_t output;
        check(cass_value_get_int64(value, &output));
        return output;
    }
    default:
        typeError("integer", value, field);
    }
}

extern  unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const CassValue *value)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        return p.uintResult;
    }
    return (__uint64) getSignedResult(field, value);
}

extern void getStringResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, char * &result)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        rtlStrToStrX(chars, result, p.resultChars, p.stringResult);
        return;
    }
    switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_ASCII:
    {
        const char *output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        rtlStrToStrX(chars, result, length, output);
        break;
    }
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
    {
        const char *output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        unsigned numchars = rtlUtf8Length(length, output);
        rtlUtf8ToStrX(chars, result, numchars, output);
        break;
    }
    default:
        typeError("string", value, field);
    }
}

extern void getUTF8Result(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, char * &result)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
        return;
    }
    switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_ASCII:
    {
        const char *output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        rtlStrToUtf8X(chars, result, length, output);
        break;
    }
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
    {
        const char * output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        unsigned numchars = rtlUtf8Length(length, output);
        rtlUtf8ToUtf8X(chars, result, numchars, output);
        break;
    }
    default:
        typeError("string", value, field);
    }
}

extern void getUnicodeResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, UChar * &result)
{
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
        return;
    }
    switch (cass_value_type(value))
    {
    case CASS_VALUE_TYPE_ASCII:
    {
        const char * output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        rtlStrToUnicodeX(chars, result, length, output);
        break;
    }
    case CASS_VALUE_TYPE_VARCHAR:
    case CASS_VALUE_TYPE_TEXT:
    {
        const char * output;
        size_t length;
        check(cass_value_get_string(value, &output, &length));
        unsigned numchars = rtlUtf8Length(length, output);
        rtlUtf8ToUnicodeX(chars, result, numchars, output);
        break;
    }
    default:
        typeError("string", value, field);
    }
}

extern void getDecimalResult(const RtlFieldInfo *field, const CassValue *value, Decimal &result)
{
    // Note - Cassandra has a decimal type, but it's not particularly similar to the ecl one. Map to string for now, as we do in MySQL
    if (cass_value_is_null(value))
    {
        NullFieldProcessor p(field);
        result.set(p.decimalResult);
        return;
    }
    size32_t chars;
    rtlDataAttr tempStr;
    cassandraembed::getStringResult(field, value, chars, tempStr.refstr());
    result.setString(chars, tempStr.getstr());
    if (field)
    {
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
        result.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
    }
}

// A CassandraRowBuilder object is used to construct an ECL row from a Cassandra row

class CassandraRowBuilder : public CInterfaceOf<IFieldSource>
{
public:
    CassandraRowBuilder(const CassandraStatementInfo *_stmtInfo)
    : stmtInfo(_stmtInfo), colIdx(0), numIteratorFields(0), nextIteratedField(0)
    {
    }
    virtual bool getBooleanResult(const RtlFieldInfo *field)
    {
        return cassandraembed::getBooleanResult(field, nextField(field));
    }
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        cassandraembed::getDataResult(field, nextField(field), len, result);
    }
    virtual double getRealResult(const RtlFieldInfo *field)
    {
        return cassandraembed::getRealResult(field, nextField(field));
    }
    virtual __int64 getSignedResult(const RtlFieldInfo *field)
    {
        return cassandraembed::getSignedResult(field, nextField(field));
    }
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field)
    {
        return cassandraembed::getUnsignedResult(field, nextField(field));
    }
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        cassandraembed::getStringResult(field, nextField(field), chars, result);
    }
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        cassandraembed::getUTF8Result(field, nextField(field), chars, result);
    }
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        cassandraembed::getUnicodeResult(field, nextField(field), chars, result);
    }
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        cassandraembed::getDecimalResult(field, nextField(field), value);
    }

    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false;
        iterator.setown(new CassandraIterator(cass_iterator_from_collection(nextField(field))));
    }
    virtual bool processNextSet(const RtlFieldInfo * field)
    {
        numIteratorFields = 1;
        return *iterator && cass_iterator_next(*iterator); // If field was NULL, we'll have a NULL iterator (representing an empty set/list)
        // Can't distinguish empty set from NULL field, so assume the former (rather than trying to deliver the default value for the set field)
    }
    virtual void processBeginDataset(const RtlFieldInfo * field)
    {
        numIteratorFields = getNumFields(field->type->queryChildType());
        switch (numIteratorFields)
        {
        case 1:
            iterator.setown(new CassandraIterator(cass_iterator_from_collection(nextField(field))));
            break;
        case 2:
            iterator.setown(new CassandraIterator(cass_iterator_from_map(nextField(field))));
            break;
        default:
            UNSUPPORTED("Nested datasets with > 2 fields");
        }
    }
    virtual void processBeginRow(const RtlFieldInfo * field)
    {
    }
    virtual bool processNextRow(const RtlFieldInfo * field)
    {
        nextIteratedField = 0;
        return *iterator && cass_iterator_next(*iterator); // If field was NULL, we'll have a NULL iterator (representing an empty set/list/map)
        // Can't distinguish empty set from NULL field, so assume the former (rather than trying to deliver the default value for the set field)
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        iterator.clear();
        numIteratorFields = 0;
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        iterator.clear();
        numIteratorFields = 0;
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    const CassValue *nextField(const RtlFieldInfo * field)
    {
        const CassValue *ret;
        if (iterator)
        {
            switch (numIteratorFields)
            {
            case 1:
                ret = cass_iterator_get_value(*iterator);
                break;
            case 2:
                if (nextIteratedField==0)
                    ret = cass_iterator_get_map_key(*iterator);
                else
                    ret = cass_iterator_get_map_value(*iterator);
                nextIteratedField++;
                break;
            default:
                throwUnexpected();
            }
        }
        else
            ret = cass_row_get_column(stmtInfo->queryRow(), colIdx++);
        if (!ret)
            failx("Too many fields in ECL output row, reading field %s", str(field->name));
        return ret;
    }
    const CassandraStatementInfo *stmtInfo;
    Owned<CassandraIterator> iterator;
    int colIdx;
    int numIteratorFields;
    int nextIteratedField;
};

// Bind Cassandra columns from an ECL record

class CassandraRecordBinder : public CInterfaceOf<IFieldProcessor>
{
public:
    CassandraRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, const CassandraStatementInfo *_stmtInfo, int _firstParam)
      : logctx(_logctx), typeInfo(_typeInfo), stmtInfo(_stmtInfo), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
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
        rtlDataAttr utfText;
        rtlStrToUtf8X(utf8chars, utfText.refstr(), len, value);
        if (collection)
            checkBind(cass_collection_append_string_n(*collection, utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                      field);
        else
            checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(),
                                                 checkNextParam(field),
                                                 utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                      field);
    }
    virtual void processBool(bool value, const RtlFieldInfo * field)
    {
        if (collection)
            checkBind(cass_collection_append_bool(*collection, value ? cass_true : cass_false), field);
        else
            checkBind(cass_statement_bind_bool(stmtInfo->queryStatement(), checkNextParam(field), value ? cass_true : cass_false), field);
    }
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        if (collection)
            checkBind(cass_collection_append_bytes(*collection, (const cass_byte_t*) value, len), field);
        else
            checkBind(cass_statement_bind_bytes(stmtInfo->queryStatement(), checkNextParam(field), (const cass_byte_t*) value, len), field);
    }
    virtual void processInt(__int64 value, const RtlFieldInfo * field)
    {
        if (getFieldBaseType(field)->size(NULL,NULL)>4)
        {
            if (collection)
                checkBind(cass_collection_append_int64(*collection, value), field);
            else
                checkBind(cass_statement_bind_int64(stmtInfo->queryStatement(), checkNextParam(field), value), field);
        }
        else
        {
            if (collection)
                checkBind(cass_collection_append_int32(*collection, value), field);
            else
                checkBind(cass_statement_bind_int32(stmtInfo->queryStatement(), checkNextParam(field), value), field);
        }
    }
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        UNSUPPORTED("UNSIGNED columns");
    }
    virtual void processReal(double value, const RtlFieldInfo * field)
    {
        if (getFieldBaseType(field)->size(NULL,NULL)>4)
        {
            if (collection)
                checkBind(cass_collection_append_double(*collection, value), field);
            else
                checkBind(cass_statement_bind_double(stmtInfo->queryStatement(), checkNextParam(field), value), field);
        }
        else
        {
            if (collection)
                checkBind(cass_collection_append_float(*collection, (float) value), field);
            else
                checkBind(cass_statement_bind_float(stmtInfo->queryStatement(), checkNextParam(field), (float) value), field);
        }
    }
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        Decimal val;
        size32_t bytes;
        rtlDataAttr decText;
        val.setDecimal(digits, precision, value);
        val.getStringX(bytes, decText.refstr());
        processUtf8(bytes, decText.getstr(), field);
    }
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        UNSUPPORTED("UNSIGNED decimals");
    }
    virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
    {
        size32_t utf8chars;
        rtlDataAttr utfText;
        rtlUnicodeToUtf8X(utf8chars, utfText.refstr(), chars, value);
        if (collection)
            checkBind(cass_collection_append_string_n(*collection, utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                      field);
        else
            checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(),
                                                   checkNextParam(field),
                                                   utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                      field);
    }
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processUtf8(charCount, text.getstr(), field);
    }
    virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
    {
        if (collection)
            checkBind(cass_collection_append_string_n(*collection, value, rtlUtf8Size(chars, value)), field);
        else
            checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(), checkNextParam(field), value, rtlUtf8Size(chars, value)), field);
    }

    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
    {
        if (isAll)
            UNSUPPORTED("SET(ALL)");
        collection.setown(new CassandraCollection(cass_collection_new(CASS_COLLECTION_TYPE_SET, numElements)));
        return true;
    }
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
    {
        // If there's a single field, assume we are mapping to a SET/LIST
        // If there are two, assume it's a MAP
        // Otherwise, fail
        int numFields = getNumFields(field->type->queryChildType());
        if (numFields < 1 || numFields > 2)
        {
            UNSUPPORTED("Nested datasets with > 2 fields");
        }
        collection.setown(new CassandraCollection(cass_collection_new(numFields==1 ? CASS_COLLECTION_TYPE_SET : CASS_COLLECTION_TYPE_MAP, numRows)));
        return true;
    }
    virtual bool processBeginRow(const RtlFieldInfo * field)
    {
        return true;
    }
    virtual void processEndSet(const RtlFieldInfo * field)
    {
        checkBind(cass_statement_bind_collection(stmtInfo->queryStatement(), checkNextParam(field), *collection), field);
        collection.clear();
    }
    virtual void processEndDataset(const RtlFieldInfo * field)
    {
        checkBind(cass_statement_bind_collection(stmtInfo->queryStatement(), checkNextParam(field), *collection), field);
        collection.clear();
    }
    virtual void processEndRow(const RtlFieldInfo * field)
    {
    }
protected:
    inline unsigned checkNextParam(const RtlFieldInfo * field)
    {
        if (logctx.queryTraceLevel() > 4)
            logctx.CTXLOG("Binding %s to %d", str(field->name), thisParam);
        return thisParam++;
    }
    inline void checkBind(CassError rc, const RtlFieldInfo * field)
    {
        if (rc != CASS_OK)
        {
            failx("While binding parameter %s: %s", str(field->name), cass_error_desc(rc));
        }
    }
    const RtlTypeInfo *typeInfo;
    const CassandraStatementInfo *stmtInfo;
    Owned<CassandraCollection> collection;
    const IContextLogger &logctx;
    int firstParam;
    RtlFieldStrInfo dummyField;
    int thisParam;
};

//
class CassandraDatasetBinder : public CassandraRecordBinder
{
public:
    CassandraDatasetBinder(const IContextLogger &_logctx, IRowStream * _input, const RtlTypeInfo *_typeInfo, const CassandraStatementInfo *_stmt, int _firstParam)
      : input(_input), CassandraRecordBinder(_logctx, _typeInfo, _stmt, _firstParam)
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
    void executeAll(CassandraStatementInfo *stmtInfo)
    {
        stmtInfo->startStream();
        while (bindNext())
        {
            stmtInfo->execute();
        }
        stmtInfo->endStream();
    }
protected:
    Owned<IRowStream> input;
};

// A Cassandra function that returns a dataset will return a CassandraRowStream object that can be
// interrogated to return each row of the result in turn

class CassandraRowStream : public CInterfaceOf<IRowStream>
{
public:
    CassandraRowStream(CassandraDatasetBinder *_inputStream, CassandraStatementInfo *_stmtInfo, IEngineRowAllocator *_resultAllocator)
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
        loop
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
        CassandraRowBuilder cassandraRowBuilder(stmtInfo);
        const RtlTypeInfo *typeInfo = resultAllocator->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, cassandraRowBuilder);
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
    Linked<CassandraDatasetBinder> inputStream;
    Linked<CassandraStatementInfo> stmtInfo;
    Linked<IEngineRowAllocator> resultAllocator;
    bool executePending;
    bool eof;
};

// Each call to a Cassandra function will use a new CassandraEmbedFunctionContext object

class CassandraEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
{
public:
    CassandraEmbedFunctionContext(const IContextLogger &_logctx, unsigned _flags, const char *options)
      : logctx(_logctx), flags(_flags), nextParam(0), numParams(0)
    {
        StringArray opts;
        opts.appendList(options, ",");
        cluster.setown(new CassandraCluster(cass_cluster_new()));
        cluster->setOptions(opts);
        session.setown(new CassandraSession(cass_session_new()));
        CassandraFuture future(cluster->keyspace.isEmpty() ? cass_session_connect(*session, *cluster) : cass_session_connect_keyspace(*session, *cluster, cluster->keyspace));
        future.wait("connect");
    }
    virtual bool getBooleanResult()
    {
        bool ret = cassandraembed::getBooleanResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getDataResult(size32_t &len, void * &result)
    {
        cassandraembed::getDataResult(NULL, getScalarResult(), len, result);
        checkSingleRow();
    }
    virtual double getRealResult()
    {
        double ret = cassandraembed::getRealResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual __int64 getSignedResult()
    {
        __int64 ret = cassandraembed::getSignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual unsigned __int64 getUnsignedResult()
    {
        unsigned __int64 ret = cassandraembed::getUnsignedResult(NULL, getScalarResult());
        checkSingleRow();
        return ret;
    }
    virtual void getStringResult(size32_t &chars, char * &result)
    {
        cassandraembed::getStringResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUTF8Result(size32_t &chars, char * &result)
    {
        cassandraembed::getUTF8Result(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getUnicodeResult(size32_t &chars, UChar * &result)
    {
        cassandraembed::getUnicodeResult(NULL, getScalarResult(), chars, result);
        checkSingleRow();
    }
    virtual void getDecimalResult(Decimal &value)
    {
        cassandraembed::getDecimalResult(NULL, getScalarResult(), value);
        checkSingleRow();
    }
    virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
    {
        CassandraIterator iterator(cass_iterator_from_collection(getScalarResult()));
        rtlRowBuilder out;
        byte *outData = NULL;
        size32_t outBytes = 0;
        while (cass_iterator_next(iterator))
        {
            const CassValue *value = cass_iterator_get_value(iterator);
            assertex(value);
            if (elemSize != UNKNOWN_LENGTH)
            {
                out.ensureAvailable(outBytes + elemSize);
                outData = out.getbytes() + outBytes;
            }
            switch ((type_t) elemType)
            {
            case type_int:
                rtlWriteInt(outData, cassandraembed::getSignedResult(NULL, value), elemSize);
                break;
            case type_unsigned:
                rtlWriteInt(outData, cassandraembed::getUnsignedResult(NULL, value), elemSize);
                break;
            case type_real:
                if (elemSize == sizeof(double))
                    * (double *) outData = cassandraembed::getRealResult(NULL, value);
                else
                {
                    assertex(elemSize == sizeof(float));
                    * (float *) outData = (float) cassandraembed::getRealResult(NULL, value);
                }
                break;
            case type_boolean:
                assertex(elemSize == sizeof(bool));
                * (bool *) outData = cassandraembed::getBooleanResult(NULL, value);
                break;
            case type_string:
            case type_varstring:
            {
                rtlDataAttr str;
                size32_t lenBytes;
                cassandraembed::getStringResult(NULL, value, lenBytes, str.refstr());
                if (elemSize == UNKNOWN_LENGTH)
                {
                    if (elemType == type_string)
                    {
                        out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                        outData = out.getbytes() + outBytes;
                        * (size32_t *) outData = lenBytes;
                        rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, str.getstr());
                        outBytes += lenBytes + sizeof(size32_t);
                    }
                    else
                    {
                        out.ensureAvailable(outBytes + lenBytes + 1);
                        outData = out.getbytes() + outBytes;
                        rtlStrToVStr(0, outData, lenBytes, str.getstr());
                        outBytes += lenBytes + 1;
                    }
                }
                else
                {
                    if (elemType == type_string)
                        rtlStrToStr(elemSize, outData, lenBytes, str.getstr());
                    else
                        rtlStrToVStr(elemSize, outData, lenBytes, str.getstr());  // Fixed size null terminated strings... weird.
                }
                break;
            }
            case type_unicode:
            case type_utf8:
            {
                rtlDataAttr str;
                size32_t lenChars;
                cassandraembed::getUTF8Result(NULL, value, lenChars, str.refstr());
                const char * text =  str.getstr();
                size32_t lenBytes = rtlUtf8Size(lenChars, text);
                if (elemType == type_utf8)
                {
                    assertex (elemSize == UNKNOWN_LENGTH);
                    out.ensureAvailable(outBytes + lenBytes + sizeof(size32_t));
                    outData = out.getbytes() + outBytes;
                    * (size32_t *) outData = lenChars;
                    rtlStrToStr(lenBytes, outData+sizeof(size32_t), lenBytes, text);
                    outBytes += lenBytes + sizeof(size32_t);
                }
                else
                {
                    if (elemSize == UNKNOWN_LENGTH)
                    {
                        // You can't assume that number of chars in utf8 matches number in unicode16 ...
                        size32_t numchars16;
                        rtlDataAttr unicode16;
                        rtlUtf8ToUnicodeX(numchars16, unicode16.refustr(), lenChars, text);
                        out.ensureAvailable(outBytes + numchars16*sizeof(UChar) + sizeof(size32_t));
                        outData = out.getbytes() + outBytes;
                        * (size32_t *) outData = numchars16;
                        rtlUnicodeToUnicode(numchars16, (UChar *) (outData+sizeof(size32_t)), numchars16, unicode16.getustr());
                        outBytes += numchars16*sizeof(UChar) + sizeof(size32_t);
                    }
                    else
                        rtlUtf8ToUnicode(elemSize / sizeof(UChar), (UChar *) outData, lenChars, text);
                }
                break;
            }
            default:
                fail("type mismatch - unsupported return type");
            }
            if (elemSize != UNKNOWN_LENGTH)
                outBytes += elemSize;
        }
        __isAllResult = false;
        __resultBytes = outBytes;
        __result = out.detachdata();
    }
    virtual IRowStream *getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        return new CassandraRowStream(inputStream, stmtInfo, _resultAllocator);
    }
    virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        if (!stmtInfo->hasResult() || stmtInfo->rowCount() != 1)
            typeError("row", NULL, NULL);
        CassandraRowStream stream(NULL, stmtInfo, _resultAllocator);
        roxiemem::OwnedConstRoxieRow ret = stream.nextRow();
        stream.stop();
        if (ret ==  NULL)  // Check for exactly one returned row
            typeError("row", NULL, NULL);
        return (byte *) ret.getClear();
    }
    virtual size32_t getTransformResult(ARowBuilder & rowBuilder)
    {
        if (!stmtInfo->hasResult() || stmtInfo->rowCount() != 1)
            typeError("row", NULL, NULL);
        if (!stmtInfo->next())
            fail("Failed to read row");
        CassandraRowBuilder cassandraRowBuilder(stmtInfo);
        const RtlTypeInfo *typeInfo = rowBuilder.queryAllocator()->queryOutputMeta()->queryTypeInfo();
        assertex(typeInfo);
        RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
        return typeInfo->build(rowBuilder, 0, &dummyField, cassandraRowBuilder);
    }
    virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, byte *val)
    {
        CassandraRecordBinder binder(logctx, metaVal.queryTypeInfo(), stmtInfo, nextParam);
        binder.processRow(val);
        nextParam += binder.numFields();
    }
    virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
    {
        // We only support a single dataset parameter...
        // MORE - look into batch?
        if (inputStream)
        {
            fail("At most one dataset parameter supported");
        }
        inputStream.setown(new CassandraDatasetBinder(logctx, LINK(val), metaVal.queryTypeInfo(), stmtInfo, nextParam));
        nextParam += inputStream->numFields();
    }

    virtual void bindBooleanParam(const char *name, bool val)
    {
        checkBind(cass_statement_bind_bool(stmtInfo->queryStatement(), checkNextParam(name), val ? cass_true : cass_false), name);
    }
    virtual void bindDataParam(const char *name, size32_t len, const void *val)
    {
        checkBind(cass_statement_bind_bytes(stmtInfo->queryStatement(), checkNextParam(name), (const cass_byte_t*) val, len), name);
    }
    virtual void bindFloatParam(const char *name, float val)
    {
        checkBind(cass_statement_bind_float(stmtInfo->queryStatement(), checkNextParam(name), val), name);
    }
    virtual void bindRealParam(const char *name, double val)
    {
        checkBind(cass_statement_bind_double(stmtInfo->queryStatement(), checkNextParam(name), val), name);
    }
    virtual void bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        if (size > 4)
            checkBind(cass_statement_bind_int64(stmtInfo->queryStatement(), checkNextParam(name), val), name);
        else
            checkBind(cass_statement_bind_int32(stmtInfo->queryStatement(), checkNextParam(name), val), name);
    }
    virtual void bindSignedParam(const char *name, __int64 val)
    {
        bindSignedSizeParam(name, 8, val);
    }
    virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        UNSUPPORTED("UNSIGNED columns");
    }
    virtual void bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        UNSUPPORTED("UNSIGNED columns");
    }
    virtual void bindStringParam(const char *name, size32_t len, const char *val)
    {
        size32_t utf8chars;
        rtlDataAttr utfText;
        rtlStrToUtf8X(utf8chars, utfText.refstr(), len, val);
        checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(),
                                             checkNextParam(name),
                                             utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                  name);
    }
    virtual void bindVStringParam(const char *name, const char *val)
    {
        bindStringParam(name, strlen(val), val);
    }
    virtual void bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(), checkNextParam(name), val, rtlUtf8Size(chars, val)), name);
    }
    virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        size32_t utf8chars;
        rtlDataAttr utfText;
        rtlUnicodeToUtf8X(utf8chars, utfText.refstr(), chars, val);
        checkBind(cass_statement_bind_string_n(stmtInfo->queryStatement(),
                                                 checkNextParam(name),
                                                 utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr())),
                  name);
    }
    virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, void *setData)
    {
        if (isAll)
            UNSUPPORTED("SET(ALL)");
        type_t typecode = (type_t) elemType;
        const byte *inData = (const byte *) setData;
        const byte *endData = inData + totalBytes;
        int numElems;
        if (elemSize == UNKNOWN_LENGTH)
        {
            numElems = 0;
            // Will need 2 passes to work out how many elements there are in the set :(
            while (inData < endData)
            {
                int thisSize;
                switch (elemType)
                {
                case type_varstring:
                    thisSize = strlen((const char *) inData) + 1;
                    break;
                case type_string:
                    thisSize = * (size32_t *) inData + sizeof(size32_t);
                    break;
                case type_unicode:
                    thisSize = (* (size32_t *) inData) * sizeof(UChar) + sizeof(size32_t);
                    break;
                case type_utf8:
                    thisSize = rtlUtf8Size(* (size32_t *) inData, inData + sizeof(size32_t)) + sizeof(size32_t);
                    break;
                default:
                    fail("Unsupported parameter type");
                    break;
                }
                inData += thisSize;
                numElems++;
            }
            inData = (const byte *) setData;
        }
        else
            numElems = totalBytes / elemSize;
        CassandraCollection collection(cass_collection_new(CASS_COLLECTION_TYPE_SET, numElems));
        while (inData < endData)
        {
            size32_t thisSize = elemSize;
            CassError rc;
            switch (typecode)
            {
            case type_int:
                if (elemSize > 4)
                    rc = cass_collection_append_int64(collection, rtlReadInt(inData, elemSize));
                else
                    rc = cass_collection_append_int32(collection, rtlReadInt(inData, elemSize));
                break;
            case type_unsigned:
                UNSUPPORTED("UNSIGNED columns");
                break;
            case type_varstring:
            {
                size32_t numChars = strlen((const char *) inData);
                if (elemSize == UNKNOWN_LENGTH)
                    thisSize = numChars + 1;
                size32_t utf8chars;
                rtlDataAttr utfText;
                rtlStrToUtf8X(utf8chars, utfText.refstr(), numChars, (const char *) inData);
                rc = cass_collection_append_string_n(collection, utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr()));
                break;
            }
            case type_string:
            {
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = * (size32_t *) inData;
                    inData += sizeof(size32_t);
                }
                size32_t utf8chars;
                rtlDataAttr utfText;
                rtlStrToUtf8X(utf8chars, utfText.refstr(), thisSize, (const char *) inData);
                rc = cass_collection_append_string_n(collection, utfText.getstr(), rtlUtf8Size(utf8chars, utfText.getstr()));
                break;
            }
            case type_real:
                if (elemSize == sizeof(double))
                    rc = cass_collection_append_double(collection, * (double *) inData);
                else
                    rc = cass_collection_append_float(collection, * (float *) inData);
                break;
            case type_boolean:
                assertex(elemSize == sizeof(bool));
                rc = cass_collection_append_bool(collection, *(bool*)inData ? cass_true : cass_false);
                break;
            case type_unicode:
            {
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = (* (size32_t *) inData) * sizeof(UChar); // NOTE - it's in chars...
                    inData += sizeof(size32_t);
                }
                unsigned unicodeChars;
                rtlDataAttr unicode;
                rtlUnicodeToUtf8X(unicodeChars, unicode.refstr(), thisSize / sizeof(UChar), (const UChar *) inData);
                size32_t sizeBytes = rtlUtf8Size(unicodeChars, unicode.getstr());
                rc = cass_collection_append_string_n(collection, unicode.getstr(), sizeBytes);
                break;
            }
            case type_utf8:
            {
                assertex (elemSize == UNKNOWN_LENGTH);
                size32_t numChars = * (size32_t *) inData;
                inData += sizeof(size32_t);
                thisSize = rtlUtf8Size(numChars, inData);
                rc = cass_collection_append_string_n(collection, (const char *) inData, thisSize);
                break;
            }
            case type_data:
                if (elemSize == UNKNOWN_LENGTH)
                {
                    thisSize = * (size32_t *) inData;
                    inData += sizeof(size32_t);
                }
                rc = cass_collection_append_bytes(collection, (const cass_byte_t*) inData, thisSize);
                break;
            }
            checkBind(rc, name);
            inData += thisSize;
        }
        checkBind(cass_statement_bind_collection(stmtInfo->queryStatement(),
                                                 checkNextParam(name),
                                                 collection),
                  name);
    }

    virtual void importFunction(size32_t lenChars, const char *text)
    {
        throwUnexpected();
    }
    virtual void compileEmbeddedScript(size32_t chars, const char *_script)
    {
        // Incoming script is not necessarily null terminated. Note that the chars refers to utf8 characters and not bytes.
        size32_t len = rtlUtf8Size(chars, _script);
        queryString.set(_script, len);
        const char *script = queryString.get(); // Now null terminated
        if ((flags & (EFnoreturn|EFnoparams)) == (EFnoreturn|EFnoparams))
        {
            loop
            {
                const char *nextScript = findUnquoted(script, ';');
                if (!nextScript)
                {
                    // script should be pointing at only trailing whitespace, else it's a "missing ;" error
                    break;
                }
                CassandraStatement statement(cass_statement_new_n(script, nextScript-script, 0));
                CassandraFuture future(cass_session_execute(*session, statement));
                future.wait("execute statement");
                script = nextScript;
            }
        }
        else
        {
            // MORE - can cache this, perhaps, if script is same as last time?
            CassandraFuture future(cass_session_prepare(*session, script));
            future.wait("prepare statement");
            Owned<CassandraPrepared> prepared = new CassandraPrepared(cass_future_get_prepared(future), NULL);
            if ((flags & EFnoparams) == 0)
                numParams = countBindings(script);
            else
                numParams = 0;
            stmtInfo.setown(new CassandraStatementInfo(session, prepared, numParams, cluster->batchMode, cluster->pageSize, cluster->maxFutures, cluster->maxRetries));
        }
    }
    virtual void callFunction()
    {
        // Does not seem to be a way to check number of parameters expected...
        // if (nextParam != cass_statement_bind_count(stmtInfo))
        //    fail("Not enough parameters");
        try
        {
            if (stmtInfo && !stmtInfo->hasResult())
                lazyExecute();
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            msg.appendf(" (processing query %s)", queryString.get());
            throw makeStringException(E->errorCode(), msg);
        }
    }
protected:
    void lazyExecute()
    {
        if (inputStream)
            inputStream->executeAll(stmtInfo);
        else
            stmtInfo->execute();
    }
    const CassValue *getScalarResult()
    {
        if (!stmtInfo->next())
            typeError("scalar", NULL, NULL);
        if (cass_row_get_column(stmtInfo->queryRow(), 1))
            typeError("scalar", NULL, NULL);
        const CassValue *result = cass_row_get_column(stmtInfo->queryRow(), 0);
        if (!result)
            typeError("scalar", NULL, NULL);
        return result;
    }
    void checkSingleRow()
    {
        if (stmtInfo->rowCount() != 1)
            typeError("scalar", NULL, NULL);
    }
    unsigned countBindings(const char *query)
    {
        unsigned queryCount = 0;
        while ((query = findUnquoted(query, '?')) != NULL)
            queryCount++;
        return queryCount;
    }
    const char *findUnquoted(const char *query, char searchFor)
    {
        // Note - returns pointer to char AFTER the first occurrence of searchFor outside of quotes

        char inStr = '\0';
        char ch;
        while ((ch = *query++) != 0)
        {
            if (ch == inStr)
                inStr = false;
            else switch (ch)
            {
            case '\'':
            case '"':
                inStr = ch;
                break;
            case '\\':
                if (inStr && *query)
                    query++;
                break;
            case '/':
                if (!inStr)
                {
                    if (*query=='/')
                    {
                        while (*query && *query != '\n')
                            query++;
                    }
                    else if (*query=='*')
                    {
                        query++;
                        loop
                        {
                            if (!*query)
                                fail("Unterminated comment in query string");
                            if (*query=='*' && query[1]=='/')
                            {
                                query+= 2;
                                break;
                            }
                            query++;
                        }
                    }
                }
                break;
            default:
                if (!inStr && ch==searchFor)
                    return query;
                break;
            }
        }
        return NULL;
    }
    inline unsigned checkNextParam(const char *name)
    {
        if (nextParam == numParams)
            failx("Too many parameters supplied: No matching ? for parameter %s", name);
        return nextParam++;
    }
    inline void checkBind(CassError rc, const char *name)
    {
        if (rc != CASS_OK)
        {
            failx("While binding parameter %s: %s", name, cass_error_desc(rc));
        }
    }
    Owned<CassandraCluster> cluster;
    Owned<CassandraSession> session;
    Owned<CassandraStatementInfo> stmtInfo;
    Owned<CassandraDatasetBinder> inputStream;
    const IContextLogger &logctx;
    unsigned flags;
    unsigned nextParam;
    unsigned numParams;
    StringAttr queryString;

};

class CassandraEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options)
    {
        return createFunctionContextEx(NULL, flags, options);
    }
    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext * ctx, unsigned flags, const char *options)
    {
        if (flags & EFimport)
            UNSUPPORTED("IMPORT");
        else
            return new CassandraEmbedFunctionContext(ctx ? ctx->queryContextLogger() : queryDummyContextLogger(), flags, options);
    }
};

extern IEmbedContext* getEmbedContext()
{
    return new CassandraEmbedContext();
}

extern bool syntaxCheck(const char *script)
{
    return true; // MORE
}

} // namespace
