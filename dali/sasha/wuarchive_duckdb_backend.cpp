/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
#include <duckdb.h>
#include <cctype>
#include <string>
#include <vector>
#include "jbuff.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jstring.hpp"
#include "salds.hpp"
#include "wuarchivepublisher.hpp"
#include "wuarchive_sql.hpp"
#include "wuarchive_sql_errors.hpp"

// Keep this equal to ArchivedWuRecord::version; bump both when the record or its DuckDB mapping/schema changes.
static constexpr unsigned duckDbArchivedWuRecordVersion = 1;

static_assert(duckDbArchivedWuRecordVersion == ArchivedWuRecord::version,
    "ArchivedWuRecord version mismatch: update ensureArchiveSchemaDuckDb and publishArchivedWuRecordDuckDb");

static void throwDuckDbError(const char * message)
{
    throw makeStringExceptionV(ARCHIVE_SQL_ERROR_QUERY_EXEC, "ARCHIVE SQL: DuckDB error: %s", message ? message : "unknown error");
}

static void execDuckDb(duckdb_connection connection, const char * sql)
{
    duckdb_result result{};
    OwnedPtrCustomFree<duckdb_result, duckdb_destroy_result> ownedResult(&result);
    if (duckdb_query(connection, sql, &result) != DuckDBSuccess)
        throwDuckDbError(duckdb_result_error(&result));
}

class CDuckDbStatement
{
    duckdb_prepared_statement statement = nullptr;

    void checkBind(duckdb_state state)
    {
        if (state != DuckDBSuccess)
            throwDuckDbError(duckdb_prepare_error(statement));
    }

public:
    CDuckDbStatement(duckdb_connection connection, const char * sql)
    {
        if (duckdb_prepare(connection, sql, &statement) != DuckDBSuccess)
        {
            StringAttr error(statement ? duckdb_prepare_error(statement) : "prepare failed");
            if (statement)
                duckdb_destroy_prepare(&statement);
            throwDuckDbError(error.get());
        }
    }

    ~CDuckDbStatement()
    {
        if (statement)
            duckdb_destroy_prepare(&statement);
    }

    void bindText(idx_t index, const char * value)
    {
        checkBind(duckdb_bind_varchar(statement, index, value ? value : ""));
    }

    void bindInt64(idx_t index, int64_t value)
    {
        checkBind(duckdb_bind_int64(statement, index, value));
    }

    void bindUInt64(idx_t index, uint64_t value)
    {
        checkBind(duckdb_bind_uint64(statement, index, value));
    }

    void bindBoolean(idx_t index, bool value)
    {
        checkBind(duckdb_bind_boolean(statement, index, value));
    }

    void clearBindings()
    {
        checkBind(duckdb_clear_bindings(statement));
    }

    void execute()
    {
        duckdb_result result{};
        OwnedPtrCustomFree<duckdb_result, duckdb_destroy_result> ownedResult(&result);
        if (duckdb_execute_prepared(statement, &result) != DuckDBSuccess)
            throwDuckDbError(duckdb_result_error(&result));
    }
};

static void ensureArchiveSchemaDuckDb(duckdb_connection connection)
{
    execDuckDb(connection,
        "CREATE TABLE IF NOT EXISTS workunits ("
        "    wuid VARCHAR PRIMARY KEY,"
        "    cluster VARCHAR NOT NULL,"
        "    owner VARCHAR NOT NULL,"
        "    job_name VARCHAR NOT NULL,"
        "    state VARCHAR NOT NULL,"
        "    is_protected BOOLEAN NOT NULL DEFAULT FALSE,"
        "    wu_action BIGINT NOT NULL DEFAULT 0,"
        "    compile_cost UBIGINT NULL,"
        "    execute_cost UBIGINT NULL,"
        "    file_access_cost UBIGINT NULL,"
        "    cost_saving_potential UBIGINT NULL,"
        "    total_cluster_time UBIGINT NULL,"
        "    failure_desc VARCHAR NOT NULL DEFAULT '',"
        "    archived_at VARCHAR NOT NULL"
        ")");

    execDuckDb(connection,
        "CREATE TABLE IF NOT EXISTS workunit_files ("
        "    wuid VARCHAR NOT NULL,"
        "    file_name VARCHAR NOT NULL,"
        "    use_count UBIGINT NOT NULL DEFAULT 1,"
        "    is_super BOOLEAN NOT NULL DEFAULT FALSE,"
        "    PRIMARY KEY (wuid, file_name)"
        ")");

    execDuckDb(connection,
        "CREATE TABLE IF NOT EXISTS workunit_errors ("
        "    wuid VARCHAR NOT NULL,"
        "    sequence_no UBIGINT NOT NULL,"
        "    error_message VARCHAR NOT NULL,"
        "    PRIMARY KEY (wuid, sequence_no)"
        ")");

    execDuckDb(connection,
        "CREATE TABLE IF NOT EXISTS workunit_application_values ("
        "    wuid VARCHAR NOT NULL,"
        "    sequence_no UBIGINT NOT NULL,"
        "    application VARCHAR NOT NULL,"
        "    name VARCHAR NOT NULL,"
        "    value VARCHAR NOT NULL,"
        "    PRIMARY KEY (wuid, sequence_no)"
        ")");

    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_cluster_state ON workunits(cluster, state, archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_owner ON workunits(owner, archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_job_name ON workunits(job_name, archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_state ON workunits(state, archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_execute_cost ON workunits(execute_cost)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_file_access_cost ON workunits(file_access_cost)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_protected ON workunits(is_protected, archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_archived_at ON workunits(archived_at, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_file_name ON workunit_files(file_name, wuid)");
    execDuckDb(connection, "CREATE INDEX IF NOT EXISTS ix_super ON workunit_files(is_super, wuid)");
}

static void publishArchivedWuRecordDuckDb(duckdb_connection connection, const ArchivedWuRecord & record)
{
    execDuckDb(connection, "BEGIN TRANSACTION");
    try
    {
        CDuckDbStatement upsertWU(connection,
            "INSERT INTO workunits "
            "    (wuid, cluster, owner, job_name, state, is_protected, wu_action, compile_cost, execute_cost, "
            "     file_access_cost, cost_saving_potential, total_cluster_time, failure_desc, archived_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (wuid) DO UPDATE SET "
            "    cluster=excluded.cluster, owner=excluded.owner, job_name=excluded.job_name, state=excluded.state, "
            "    is_protected=excluded.is_protected, wu_action=excluded.wu_action, "
            "    compile_cost=excluded.compile_cost, execute_cost=excluded.execute_cost, "
            "    file_access_cost=excluded.file_access_cost, cost_saving_potential=excluded.cost_saving_potential, "
            "    total_cluster_time=excluded.total_cluster_time, failure_desc=excluded.failure_desc, archived_at=excluded.archived_at");
        upsertWU.bindText(1, record.wuid.get());
        upsertWU.bindText(2, record.clusterLc.get());
        upsertWU.bindText(3, record.ownerLc.get());
        upsertWU.bindText(4, record.jobNameLc.get());
        upsertWU.bindText(5, record.state.get());
        upsertWU.bindBoolean(6, record.isProtected);
        upsertWU.bindInt64(7, record.WUAction);
        upsertWU.bindUInt64(8, record.compileCost);
        upsertWU.bindUInt64(9, record.executeCost);
        upsertWU.bindUInt64(10, record.fileAccessCost);
        upsertWU.bindUInt64(11, record.costSavingPotential);
        upsertWU.bindUInt64(12, record.totalClusterTime);
        upsertWU.bindText(13, record.failureDesc.get());

        StringBuffer archivedAt;
        assertex(!record.archivedAt.isNull());
        unsigned year, month, day, hour, minute, second, nano;
        record.archivedAt.getDate(year, month, day);
        record.archivedAt.getTime(hour, minute, second, nano);
        archivedAt.appendf("%04u-%02u-%02u %02u:%02u:%02u.%06u", year, month, day, hour, minute, second, nano / 1000);
        upsertWU.bindText(14, archivedAt.str());
        upsertWU.execute();

        CDuckDbStatement delFiles(connection,
            "DELETE FROM workunit_files "
            "WHERE wuid=?");
        delFiles.bindText(1, record.wuid.get());
        delFiles.execute();

        CDuckDbStatement insFile(connection,
            "INSERT INTO workunit_files "
            "    (wuid, file_name, use_count, is_super) "
            "VALUES (?, ?, ?, ?)");
        for (const ArchivedWuFileRecord & file : record.files)
        {
            if (file.fileNameLc.isEmpty())
                continue;
            insFile.clearBindings();
            insFile.bindText(1, record.wuid.get());
            insFile.bindText(2, file.fileNameLc.get());
            insFile.bindUInt64(3, file.useCount);
            insFile.bindBoolean(4, file.isSuper);
            insFile.execute();
        }

        CDuckDbStatement delErrors(connection,
            "DELETE FROM workunit_errors "
            "WHERE wuid=?");
        delErrors.bindText(1, record.wuid.get());
        delErrors.execute();

        CDuckDbStatement insError(connection,
            "INSERT INTO workunit_errors "
            "    (wuid, sequence_no, error_message) "
            "VALUES (?, ?, ?)");
        for (const ArchivedWuErrorRecord & error : record.errors)
        {
            if (error.errorMessage.isEmpty())
                continue;
            insError.clearBindings();
            insError.bindText(1, record.wuid.get());
            insError.bindUInt64(2, error.sequenceNo);
            insError.bindText(3, error.errorMessage.get());
            insError.execute();
        }

        CDuckDbStatement delApplicationValues(connection,
            "DELETE FROM workunit_application_values "
            "WHERE wuid=?");
        delApplicationValues.bindText(1, record.wuid.get());
        delApplicationValues.execute();

        CDuckDbStatement insApplicationValue(connection,
            "INSERT INTO workunit_application_values "
            "    (wuid, sequence_no, application, name, value) "
            "VALUES (?, ?, ?, ?, ?)");
        unsigned sequenceNo = 0;
        for (const ArchivedWuApplicationValueRecord & applicationValue : record.applicationValues)
        {
            insApplicationValue.clearBindings();
            insApplicationValue.bindText(1, record.wuid.get());
            insApplicationValue.bindUInt64(2, ++sequenceNo);
            insApplicationValue.bindText(3, applicationValue.application.get());
            insApplicationValue.bindText(4, applicationValue.name.get());
            insApplicationValue.bindText(5, applicationValue.value.get());
            insApplicationValue.execute();
        }

        execDuckDb(connection, "COMMIT");
        PROGLOG("ARCHIVE SQL: DuckDB publish committed for %s (files=%u errors=%u)", record.wuid.get(), static_cast<unsigned>(record.files.size()), static_cast<unsigned>(record.errors.size()));
    }
    catch (...)
    {
        try
        {
            execDuckDb(connection, "ROLLBACK");
        }
        catch (IException * e)
        {
            VStringBuffer context("ARCHIVE SQL: DuckDB rollback failed for %s", record.wuid.get());
            EXCLOG(e, context.str());
            e->Release();
        }
        catch (...)
        {
            OERRLOG("ARCHIVE SQL: DuckDB rollback failed for %s: unknown exception", record.wuid.get());
        }
        throw;
    }
}

class CDuckDbArchiveSqlConnection : public CSimpleInterfaceOf<IArchiveSqlConnection>
{
    duckdb_database database = nullptr;
    duckdb_connection connection = nullptr;

public:
    explicit CDuckDbArchiveSqlConnection(const SqlConnectionInfo & info)
    {
        StringBuffer databasePath;
        getLdsPath("", databasePath);
        addPathSepChar(databasePath).append(info.database);

        duckdb_config config = nullptr;
        if (duckdb_create_config(&config) != DuckDBSuccess)
            throw makeStringException(ARCHIVE_SQL_ERROR_SQL_CONNECT, "ARCHIVE SQL: DuckDB configuration creation failed");
        OwnedPtrCustomFree<duckdb_config, duckdb_destroy_config> ownedConfig(&config);

        char * error = nullptr;
        duckdb_state status = duckdb_open_ext(databasePath.str(), &database, config, &error);
        OwnedPtrCustomFree<void, duckdb_free> ownedError(error);
        if (status != DuckDBSuccess)
            throw makeStringExceptionV(ARCHIVE_SQL_ERROR_SQL_CONNECT, "ARCHIVE SQL: DuckDB open %s failed: %s", databasePath.str(), error ? error : "database open failed");

        if (duckdb_connect(database, &connection) != DuckDBSuccess)
        {
            duckdb_close(&database);
            throw makeStringExceptionV(ARCHIVE_SQL_ERROR_SQL_CONNECT, "ARCHIVE SQL: DuckDB connection to %s failed", databasePath.str());
        }
    }

    ~CDuckDbArchiveSqlConnection()
    {
        if (connection)
            duckdb_disconnect(&connection);
        if (database)
            duckdb_close(&database);
    }

    virtual void ensureSchema() override
    {
        ensureArchiveSchemaDuckDb(connection);
    }

    virtual void publish(const ArchivedWuRecord & record) override
    {
        publishArchivedWuRecordDuckDb(connection, record);
    }
};

class CDuckDbArchiveSqlBackend : public IArchiveSqlBackend
{
public:
    virtual const char * queryName() const override
    {
        return "duckdb";
    }

    virtual IArchiveSqlConnection * connect(const SqlConnectionInfo & info) override
    {
        return new CDuckDbArchiveSqlConnection(info);
    }
};

IArchiveSqlBackend & queryArchiveSqlBackendDuckdb()
{
    static CDuckDbArchiveSqlBackend backend;
    return backend;
}
