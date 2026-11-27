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
#include "jexcept.hpp"
#include "jlog.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "wuarchivepublisher.hpp"
#include "wuarchive_sql.hpp"
#include "wuarchive_sql_errors.hpp"

class CSqlArchivedWuPublisher : public CSimpleInterfaceOf<IArchivedWuPublisher>
{
    static constexpr unsigned publishRetries = 3;
    IArchiveSqlBackend & backend;
    StringAttr database;
    Owned<IArchiveSqlConnection> conn;
    bool schemaEnsured = false;
    CriticalSection crit{SYNC_LOCATION};

    void connect()
    {
        DBGLOG("ARCHIVE SQL: opening %s database %s", backend.queryName(), database.get());
        SqlConnectionInfo info;
        info.database = database;

        Owned<IArchiveSqlConnection> newConn = backend.connect(info);
        if (!schemaEnsured)
        {
            newConn->ensureSchema();
            schemaEnsured = true;
        }
        conn.setown(newConn.getClear());
        //PROGLOG("ARCHIVE SQL: connected via %s to %s", backend.queryName(), database.get());
    }

    void ensureConnected()
    {
        if (conn)
            return;
        connect();
    }

public:
    CSqlArchivedWuPublisher(IArchiveSqlBackend & _backend, const char * _database)
        : backend(_backend), database(_database)
    {
    }

    virtual void publish(const ArchivedWuRecord & record) override
    {
        CriticalBlock block(crit);
        Owned<IException> exception;

        for (unsigned attempt = 0; attempt < publishRetries; ++attempt)
        {
            try
            {
                ensureConnected();
                conn->publish(record);
                return; // Successfully published, exit the retry loop
            }
            catch (IException * e)
            {
                EXCLOG(e);
                exception.setown(e);
            }
            conn.clear(); // Clear the connection to re-connect on the next attempt
        }
        DBGLOG("ARCHIVE SQL: publish failed for %s, reconnecting to %s database %s", record.wuid.get(), backend.queryName(), database.get());
        throw exception.getClear();
    }
};

IArchivedWuPublisher * createSqlArchivedWuPublisher(const IPropertyTree * sqlConfig)
{
    assertex(sqlConfig);

    const char * backendName = sqlConfig->queryProp("@backend", "duckdb");
    if (!strieq(backendName, "duckdb"))
        throw makeStringExceptionV(ARCHIVE_SQL_ERROR_CONFIG, "ARCHIVE SQL: unsupported archiveSql backend '%s'; only duckdb is supported", backendName);

    IArchiveSqlBackend & backend = queryArchiveSqlBackendDuckdb();

    constexpr const char * database = "wuarchive.duckdb";

    PROGLOG("ARCHIVE SQL: configured %s publisher for %s", backend.queryName(), database);

    return new CSqlArchivedWuPublisher(backend, database);
}
