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

#ifndef _CASSANDRAEMBED_INCL
#define _CASSANDRAEMBED_INCL

namespace cassandraembed {

extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
extern void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
extern void fail(const char *msg) __attribute__((noreturn));
extern void check(CassError rc);
extern bool isInteger(const CassValueType t);
extern bool isString(CassValueType t);

// Wrappers to Cassandra structures that require corresponding releases

class CassandraCluster : public CInterface
{
public:
    inline CassandraCluster(CassCluster *_cluster) : cluster(_cluster), batchMode((CassBatchType) -1), pageSize(0)
    {
    }
    void setOptions(const StringArray &options);
    inline ~CassandraCluster()
    {
        if (cluster)
            cass_cluster_free(cluster);
    }
    inline operator CassCluster *() const
    {
        return cluster;
    }
private:
    void checkSetOption(CassError rc, const char *name);
    cass_bool_t getBoolOption(const char *val, const char *option);
    unsigned getUnsignedOption(const char *val, const char *option);
    unsigned getDoubleOption(const char *val, const char *option);
    __uint64 getUnsigned64Option(const char *val, const char *option);
    CassandraCluster(const CassandraCluster &);
    CassCluster *cluster;
public:
    // These are here as convenient to set from same options string. They are really properties of the session
    // or query rather than the cluster, but we have one session per cluster so we get away with it at the moment.
    CassBatchType batchMode;
    unsigned pageSize;
    StringAttr keyspace;
};

class CassandraFuture : public CInterface
{
public:
    inline CassandraFuture(CassFuture *_future) : future(_future)
    {
    }
    inline ~CassandraFuture()
    {
        if (future)
            cass_future_free(future);
    }
    inline operator CassFuture *() const
    {
        return future;
    }
    void wait(const char *why) const;
    inline void set(CassFuture *_future)
    {
        if (future)
            cass_future_free(future);
        future = _future;
    }
protected:
    CassandraFuture(const CassandraFuture &);
    CassFuture *future;
};

class CassandraFutureResult : public CassandraFuture
{
public:
    inline CassandraFutureResult(CassFuture *_future) : CassandraFuture(_future)
    {
        result = NULL;
    }
    inline ~CassandraFutureResult()
    {
        if (result)
            cass_result_free(result);
    }
    inline operator const CassResult *() const
    {
        if (!result)
        {
            wait("FutureResult");
            result = cass_future_get_result(future);
        }
        return result;
    }
private:
    CassandraFutureResult(const CassandraFutureResult &);
    mutable const CassResult *result;

};

class CassandraSession : public CInterface
{
public:
    inline CassandraSession() : session(NULL) {}
    inline CassandraSession(CassSession *_session) : session(_session)
    {
    }
    inline ~CassandraSession()
    {
        set(NULL);
    }
    void set(CassSession *_session);
    inline operator CassSession *() const
    {
        return session;
    }
private:
    CassandraSession(const CassandraSession &);
    CassSession *session;
};

class CassandraBatch : public CInterface
{
public:
    inline CassandraBatch(CassBatch *_batch) : batch(_batch)
    {
    }
    inline ~CassandraBatch()
    {
        if (batch)
            cass_batch_free(batch);
    }
    inline operator CassBatch *() const
    {
        return batch;
    }
private:
    CassandraBatch(const CassandraBatch &);
    CassBatch *batch;
};

class CassandraPrepared : public CInterfaceOf<IInterface>
{
public:
    inline CassandraPrepared(const CassPrepared *_prepared, const char *_queryString)
    : prepared(_prepared), queryString(_queryString)
    {
    }
    inline ~CassandraPrepared()
    {
        if (prepared)
            cass_prepared_free(prepared);
    }
    inline operator const CassPrepared *() const
    {
        return prepared;
    }
    inline const char *queryQueryString() const
    {
        return queryString;
    }
private:
    CassandraPrepared(const CassandraPrepared &);
    StringAttr queryString;
    const CassPrepared *prepared;
};

class CassandraStatement : public CInterface
{
public:
    inline CassandraStatement(CassStatement *_statement) : statement(_statement)
    {
    }
    inline CassandraStatement(const char *simple) : statement(cass_statement_new(simple, 0))
    {
    }
    inline CassandraStatement(const CassandraPrepared *_prepared) : statement(cass_prepared_bind(*_prepared))
    {
        const char *queryString = _prepared->queryQueryString(); // Only set when tracing..
        if (queryString)
            query.set(queryString);
        _prepared->Release();
    }
    inline ~CassandraStatement()
    {
        if (statement)
            cass_statement_free(statement);
    }
    operator CassStatement *() const
    {
        if (query.length())
            DBGLOG("Executing %s", query.str());
        return statement;
    }
    void bindBool(unsigned idx, cass_bool_t value)
    {
        if (query.length())
            traceBind(idx, "%s", value ? "true" : "false");
        check(cass_statement_bind_bool(statement, idx, value));
    }
    void bindInt32(unsigned idx, __int32 value)
    {
        if (query.length())
            traceBind(idx, "%d", value);
        check(cass_statement_bind_int32(statement, idx, value));
    }
    void bindInt64(unsigned idx, __int64 value)
    {
        if (query.length())
            traceBind(idx, "%" I64F "d", value);
        check(cass_statement_bind_int64(statement, idx, value));
    }
    void bindString(unsigned idx, const char *value)
    {
        if (query.length())
        {
            unsigned l = strlen(value);
            if (l > 100)
                l = 100;
            traceBind(idx, "'%.*s'", l, value);
        }
        check(cass_statement_bind_string(statement, idx, value));
    }
    void bindString_n(unsigned idx, const char *value, unsigned len)
    {
        if (strlen(value)<len)
        {
            if (query.length())
                traceBind(idx, "'%s'", value);
            check(cass_statement_bind_string(statement, idx, value));
        }
        else
        {
            if (query.length())
                traceBind(idx, "'%.*s'", len>100?100:len, value);
            check(cass_statement_bind_string_n(statement, idx, value, len));
        }
    }
    void bindBytes(unsigned idx, const cass_byte_t *value, unsigned len)
    {
        if (query.length())
            traceBind(idx, "(bytes)");
        check(cass_statement_bind_bytes(statement, idx, value, len));
    }
    void bindCollection(unsigned idx, const CassCollection *value)
    {
        if (query.length())
            traceBind(idx, "(collection)");
        check(cass_statement_bind_collection(statement, idx, value));
    }
private:
    void traceBind(unsigned idx, const char *format, ...)
    {
        assert(query.length());
        StringBuffer bound;
        va_list args;
        va_start(args, format);
        bound.valist_appendf(format, args);
        va_end(args);
        const char *pos = query;
        do
        {
            pos = strchr(pos, '?');
            assert(pos);
            if (!pos)
                return;
            pos++;
        } while (idx--);
        query.insert(pos-query, bound);
    }
    CassandraStatement(const CassandraStatement &);
    StringBuffer query;
    CassStatement *statement;
};

class CassandraResult : public CInterfaceOf<IInterface>
{
public:
    inline CassandraResult(const CassResult *_result) : result(_result)
    {
    }
    inline ~CassandraResult()
    {
        if (result)
            cass_result_free(result);
    }
    inline operator const CassResult *() const
    {
        return result;
    }
private:
    CassandraResult(const CassandraResult &);
    const CassResult *result;
};

class CassandraIterator : public CInterfaceOf<IInterface>
{
public:
    inline CassandraIterator(CassIterator *_iterator) : iterator(_iterator)
    {
    }
    inline ~CassandraIterator()
    {
        if (iterator)
            cass_iterator_free(iterator);
    }
    inline void set(CassIterator *_iterator)
    {
        if (iterator)
            cass_iterator_free(iterator);
        iterator = _iterator;
    }
    inline operator CassIterator *() const
    {
        return iterator;
    }
protected:
    CassandraIterator(const CassandraIterator &);
    CassIterator *iterator;
};

class CassandraCollection : public CInterface
{
public:
    inline CassandraCollection(CassCollection *_collection) : collection(_collection)
    {
    }
    inline ~CassandraCollection()
    {
        if (collection)
            cass_collection_free(collection);
    }
    inline operator CassCollection *() const
    {
        return collection;
    }
private:
    CassandraCollection(const CassandraCollection &);
    CassCollection *collection;
};

class CassandraStatementInfo : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CassandraStatementInfo(CassandraSession *_session, CassandraPrepared *_prepared, unsigned _numBindings, CassBatchType _batchMode, unsigned pageSize);
    ~CassandraStatementInfo();
    void stop();
    bool next();
    void startStream();
    void endStream();
    void execute();
    inline size_t rowCount() const
    {
        return cass_result_row_count(*result);
    }
    inline bool hasResult() const
    {
        return result != NULL;
    }
    inline const CassRow *queryRow() const
    {
        assertex(iterator && *iterator);
        return cass_iterator_get_row(*iterator);
    }
    inline CassStatement *queryStatement() const
    {
        assertex(statement && *statement);
        return *statement;
    }
protected:
    Linked<CassandraSession> session;
    Linked<CassandraPrepared> prepared;
    Owned<CassandraBatch> batch;
    Owned<CassandraStatement> statement;
    Owned<CassandraFutureResult> result;
    Owned<CassandraIterator> iterator;
    unsigned numBindings;
    CassBatchType batchMode;
};

extern bool getBooleanResult(const RtlFieldInfo *field, const CassValue *value);
extern void getDataResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, void * &result);
extern __int64 getSignedResult(const RtlFieldInfo *field, const CassValue *value);
extern unsigned __int64 getUnsignedResult(const RtlFieldInfo *field, const CassValue *value);
extern double getRealResult(const RtlFieldInfo *field, const CassValue *value);
extern void getStringResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, char * &result);
extern void getUTF8Result(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, char * &result);
extern void getUnicodeResult(const RtlFieldInfo *field, const CassValue *value, size32_t &chars, UChar * &result);
extern void getDecimalResult(const RtlFieldInfo *field, const CassValue *value, Decimal &result);

} // namespace
#endif
