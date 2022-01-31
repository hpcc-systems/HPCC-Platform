/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems.

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

#include "LoggingErrors.hpp"
#include "cassandralogagent.hpp"

static const int defaultMaxTriesGTS = -1;

static void setCassandraLogAgentOption(StringArray& opts, const char* opt, const char* val)
{
    if (opt && *opt && val)
    {
        VStringBuffer optstr("%s=%s", opt, val);
        opts.append(optstr);
    }
}

static const CassValue* getSingleResult(const CassResult* result)
{
    const CassRow* row = cass_result_first_row(result);
    return row ? cass_row_get_column(row, 0) : NULL;
}

bool CCassandraLogAgent::init(const char* name, const char* type, IPropertyTree* cfg, const char* process)
{
    if (!name || !*name || !type || !*type)
        throw MakeStringException(-1, "Name or type not specified for CassandraLogAgent");

    if (!cfg)
        throw MakeStringException(-1, "Unable to find configuration for log agent %s:%s", name, type);

    agentName.set(name);
    const char* servicesConfig = cfg->queryProp("@services");
    if (isEmptyString(servicesConfig))
        throw MakeStringException(-1,"No Logging Service defined for %s", agentName.get());
    setServices(servicesConfig);

    IPropertyTree* cassandra = cfg->queryBranch("Cassandra");
    if(!cassandra)
        throw MakeStringException(-1, "Unable to find Cassandra settings for log agent %s:%s", name, type);

    readDBCfg(cassandra, dbServer, dbUserID, dbPassword);

    if (hasService(LGSTUpdateLOG))
    {
        //Read information about data mapping for every log groups
        readLogGroupCfg(cfg, defaultLogGroup, logGroups);
        if (defaultLogGroup.isEmpty())
            throw MakeStringException(-1,"LogGroup not defined");

        //Read mapping between log sources and log groups
        readLogSourceCfg(cfg, logSourceCount, logSourcePath, logSources);
    }

    //Read transactions settings
    if (hasService(LGSTGetTransactionSeed))
    {
        readTransactionCfg(cfg);
        maxTriesGTS = cfg->getPropInt("MaxTriesGTS", defaultMaxTriesGTS);
    }

    //Setup Cassandra
    initKeySpace();
    return true;
}

void CCassandraLogAgent::initKeySpace()
{
    //Initialize Cassandra Cluster Session
    cassSession.setown(new CassandraClusterSession(cass_cluster_new()));
    if (!cassSession)
        throw MakeStringException(-1,"Unable to create cassandra cassSession session");

    setSessionOptions(NULL);

    //ensure defaultDB
    ensureDefaultKeySpace();

    //ensure transSeed tables
    ensureTransSeedTable();

    //Read logging transaction seed
    queryTransactionSeed(loggingTransactionApp.get(), loggingTransactionSeed);
}

void CCassandraLogAgent::ensureDefaultKeySpace()
{
    CassandraSession s(cass_session_new());
    CassandraFuture future1(cass_session_connect(s, cassSession->queryCluster()));
    future1.wait("connect without keyspace");

    VStringBuffer st("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };",
        defaultDB.str());
    CassandraStatement statement(cass_statement_new(st.str(), 0));
    CassandraFuture future2(cass_session_execute(s, statement));
    future2.wait("execute");

    s.set(NULL);
}

void CCassandraLogAgent::ensureTransSeedTable()
{
    //Create transaction seed table as needed
    StringBuffer transSeedTableKeys;
    StringArray transSeedTableColumnNames, transSeedTableColumnTypes;
    transSeedTableColumnNames.append("id");
    transSeedTableColumnTypes.append("int");
    transSeedTableColumnNames.append("application");
    transSeedTableColumnTypes.append("varchar");
    transSeedTableKeys.set("application"); //primary keys

    //The defaultDB has transactions table.
    setSessionOptions(defaultDB.str());
    cassSession->connect();
    createTable(defaultDB.str(), transactionTable.str(), transSeedTableColumnNames, transSeedTableColumnTypes, transSeedTableKeys.str());

    unsigned id = 0;
    VStringBuffer st("SELECT id FROM %s LIMIT 1;", transactionTable.str());
    if (!executeSimpleSelectStatement(st.str(), id))
    {
        st.setf("INSERT INTO %s (id, application) values ( 10000, '%s');",
            transactionTable.str(), loggingTransactionApp.get());
        executeSimpleStatement(st.str());

        if (!strieq(defaultTransactionApp.get(), loggingTransactionApp.get()))
        {
            st.setf("INSERT INTO %s (id, application) values ( 10000, '%s');",
                transactionTable.str(), defaultTransactionApp.get());
            executeSimpleStatement(st.str());
        }
    }
    cassSession->disconnect();
}

void CCassandraLogAgent::queryTransactionSeed(const char* appName, StringBuffer& seed)
{
    CriticalBlock b(transactionSeedCrit);

    unsigned seedInt = 0;
    VStringBuffer st("SELECT id FROM %s WHERE application = '%s'", transactionTable.str(), appName);
    setSessionOptions(defaultDB.str()); //Switch to defaultDB since it may not be the current keyspace.
    cassSession->connect();
    executeSimpleSelectStatement(st.str(), seedInt);
    seed.setf("%d", seedInt);

    //update transactions for the next seed
    VStringBuffer updateQuery("UPDATE %s SET id=%d WHERE application = '%s'",
        transactionTable.str(), ++seedInt,  appName);
    executeSimpleStatement(updateQuery.str());
    cassSession->disconnect();
}

void CCassandraLogAgent::setSessionOptions(const char *keyspace)
{
    StringArray opts;
    setCassandraLogAgentOption(opts, "contact_points", dbServer.str());
    if (!dbUserID.isEmpty())
    {
        setCassandraLogAgentOption(opts, "user", dbUserID.str());
        if (!dbPassword.isEmpty())
            setCassandraLogAgentOption(opts, "password", dbPassword.str());
    }
    if (keyspace && *keyspace)
        setCassandraLogAgentOption(opts, "keyspace", keyspace);
    cassSession->setOptions(opts);
}

void CCassandraLogAgent::createTable(const char *dbName, const char *tableName, StringArray& columnNames, StringArray& columnTypes, const char* keys)
{
    StringBuffer fields;
    ForEachItemIn(i, columnNames)
        fields.appendf("%s %s,", columnNames.item(i), columnTypes.item(i));

    VStringBuffer createTableSt("CREATE TABLE IF NOT EXISTS %s.%s (%s PRIMARY KEY (%s));", dbName, tableName, fields.str(), keys);
    executeSimpleStatement(createTableSt.str());
}

void CCassandraLogAgent::addField(CLogField& logField, const char* name, StringBuffer& value, StringBuffer& fields, StringBuffer& values)
{
    const char* fieldType = logField.getType();
    if(strieq(fieldType, "int"))
    {
        appendFieldInfo(logField.getMapTo(), value, fields, values, false);
        return;
    }

    if(strieq(fieldType, "raw"))
    {
        appendFieldInfo(logField.getMapTo(), value, fields, values, true);;
        return;
    }

    if(strieq(fieldType, "varchar") || strieq(fieldType, "text"))
    {
        if(fields.length() != 0)
            fields.append(',');
        fields.append(logField.getMapTo());

        if(values.length() != 0)
            values.append(',');
        values.append('\'');

        const char* str = value.str();
        int length = value.length();
        for(int i = 0; i < length; i++)
        {
            unsigned char c = str[i];
            if(c == '\t' || c == '\n' || c== '\r')
                values.append(' ');
            else if(c == '\'')
                values.append('"');
            else if(c < 32 || c > 126)
                values.append('?');
            else
                values.append(c);
        }
        values.append('\'');
        return;
    }

    DBGLOG("Unknown format %s", fieldType);
}

void CCassandraLogAgent::setUpdateLogStatement(const char* dbName, const char* tableName,
        const char* fields, const char* values, StringBuffer& statement)
{
    statement.setf("INSERT INTO %s.%s (%s, date_added) values (%s, toUnixTimestamp(now()));", dbName, tableName, fields, values);
}

void CCassandraLogAgent::executeSimpleStatement(const char* st)
{
    bool trace = !queryLogMsgManager()->rejectsCategory(LegacyMsgCatMin);
    CassandraStatement statement(cassSession->prepareStatement(st, trace));
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
}

void CCassandraLogAgent::executeUpdateLogStatement(StringBuffer& st)
{
    cassSession->connect();
    CassandraFuture futurePrep(cass_session_prepare_n(cassSession->querySession(), st.str(), st.length()));
    futurePrep.wait("prepare statement");

    Owned<CassandraPrepared> prepared = new CassandraPrepared(cass_future_get_prepared(futurePrep), NULL);
    CassandraStatement statement(prepared.getClear());
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
    cassSession->disconnect();
}

bool CCassandraLogAgent::executeSimpleSelectStatement(const char* st, unsigned& resultValue)
{
    bool trace = !queryLogMsgManager()->rejectsCategory(LegacyMsgCatMin);
    CassandraStatement statement(cassSession->prepareStatement(st, trace));
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
    CassandraResult result(cass_future_get_result(future));
    if (cass_result_row_count(result) == 0)
        return false;

    resultValue = getUnsignedResult(NULL, getSingleResult(result));
    return true;
}

extern "C"
{
CASSANDRALOGAGENT_API IEspLogAgent* newLoggingAgent()
{
    return new CCassandraLogAgent();
}
}

#ifdef SET_LOGTABLE
//Keep this for now just in case. We may remove after a few releases.
void CCassandraLogAgent::ensureKeySpace()
{
    CassandraSession s(cass_session_new());
    CassandraFuture future(cass_session_connect(s, cassSession->queryCluster()));
    future.wait("connect without keyspace");

    VStringBuffer createKeySpace("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };",
        cassSession->queryKeySpace());
    executeSimpleStatement(createKeySpace.str());
    s.set(NULL);

    //prepare transSeedTable
    StringBuffer transSeedTableKeys;
    StringArray transSeedTableColumnNames, transSeedTableColumnTypes;
    transSeedTableColumnNames.append("id");
    transSeedTableColumnTypes.append("int");
    transSeedTableColumnNames.append("application");
    transSeedTableColumnTypes.append("varchar");
    transSeedTableColumnNames.append("update_time");
    transSeedTableColumnTypes.append("timestamp");
    transSeedTableKeys.set("application");

    cassSession->connect();
    createTable("transactions", transSeedTableColumnNames, transSeedTableColumnTypes, transSeedTableKeys.str());

    //prepare log tables
    ForEachItemIn(i, logDBTables)
    {
        CDBTable& table = logDBTables.item(i);

        StringBuffer logTableKeys;
        StringArray logTableColumnNames, logTableColumnTypes;

        DBFieldMap* fieldMap = table.getFieldMap();
        StringArray& logTableColumnNameArray = fieldMap->getMapToNames();
        logTableColumnNames.append("log_id");
        logTableColumnTypes.append("varchar");
        ForEachItemIn(ii, logTableColumnNameArray)
        {
            logTableColumnNames.append(logTableColumnNameArray.item(ii));
            logTableColumnTypes.append(fieldMap->getMapToTypes().item(ii));
        }
        logTableColumnNames.append("date_added");
        logTableColumnTypes.append("timestamp");
        logTableKeys.set("log_id");
        createTable(table.getTableName(), logTableColumnNames, logTableColumnTypes, logTableKeys.str());
    }
    initTransSeedTable();
    cassSession->disconnect();
}
#endif
