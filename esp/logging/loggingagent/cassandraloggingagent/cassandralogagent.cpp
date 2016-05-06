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
#include "esploggingservice_esp.ipp"
#include "cassandralogagent.hpp"

const int DefaultMaxTriesGTS = -1;
const char* const DefaultloggingTransactionApp = "CassandraloggingTransaction";
const char* const DefaultTransactionApp = "DefauleTransaction";

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

void ensureInputString(const char* input, bool lowerCase, StringBuffer& inputStr, int code, const char* msg)
{
    inputStr.set(input).trim();
    if (inputStr.isEmpty())
        throw MakeStringException(code, "%s", msg);
    if (lowerCase)
        inputStr.toLowerCase();
}

void CLogTable::loadMappings(IPropertyTree& fieldList)
{
    StringBuffer name, mapTo, fieldType, defaultValue;
    Owned<IPropertyTreeIterator> itr = fieldList.getElements("Field");
    ForEach(*itr)
    {
        IPropertyTree &map = itr->query();

        ensureInputString(map.queryProp("@name"), false, name, -1, "Field @name required");
        ensureInputString(map.queryProp("@mapto"), true, mapTo, -1, "Field @mapto required");
        ensureInputString(map.queryProp("@type"), true, fieldType, -1, "Field @type required");
        defaultValue = map.queryProp("@default");
        defaultValue.trim();

        Owned<CLogField> field = new CLogField(name.str(), mapTo.str(), fieldType.str());
        if (!defaultValue.isEmpty())
            field->setDefault(defaultValue.str());
        logFields.append(*field.getClear());
    }
}

void CLogGroup::loadMappings(IPropertyTree& fieldList)
{
    StringBuffer tableName;
    Owned<IPropertyTreeIterator> itr = fieldList.getElements("Fieldmap");
    ForEach(*itr)
    {
        ensureInputString(itr->query().queryProp("@table"), true, tableName, -1, "Fieldmap @table required");

        Owned<CLogTable> table = new CLogTable(tableName.str());
        table->loadMappings(itr->query());
        CIArrayOf<CLogField>& logFields = table->getLogFields();
        if (logFields.length() < 1)
            throw MakeStringException(-1,"No Fieldmap for %s", tableName.str());

        logTables.append(*table.getClear());
    }
}

bool CCassandraLogAgent::init(const char* name, const char* type, IPropertyTree* cfg, const char* process)
{
    if (!name || !*name || !type || !*type)
        throw MakeStringException(-1, "Name or type not specified for CassandraLogAgent");

    if (!cfg)
        throw MakeStringException(-1, "Unable to find configuration for log agent %s:%s", name, type);

    IPropertyTree* cassandra = cfg->queryBranch("Cassandra");
    if(!cassandra)
        throw MakeStringException(-1, "Unable to find Cassandra settings for log agent %s:%s", name, type);

    agentName.set(name);
    ensureInputString(cassandra->queryProp("@server"), true, dbServer, -1, "Cassandra server required");
    ensureInputString(cassandra->queryProp("@dbName"), true, defaultDB, -1, "Database name required");

    const char* userID = cassandra->queryProp("@dbUser");
    if (userID && *userID)
    {
        const char* encodedPassword = cassandra->queryProp("@dbPassWord");
        if (!encodedPassword || !*encodedPassword)
            throw MakeStringException(-1, "Cassandra Database password required");

        decrypt(dbPassword, encodedPassword);
        dbUserID.set(userID);
    }

    {
        //Read information about data mapping
        StringBuffer groupName;
        Owned<IPropertyTreeIterator> iter = cfg->getElements("LogGroup");
        ForEach(*iter)
        {
            ensureInputString(iter->query().queryProp("@name"), true, groupName, -1, "LogGroup @name required");
            Owned<CLogGroup> logGroup = new CLogGroup(groupName.str());
            logGroup->loadMappings(iter->query());
            logGroups.setValue(groupName.str(), logGroup);
            if (defaultLogGroup.isEmpty())
                defaultLogGroup.set(groupName.str());
        }
    }

    logSourceCount = 0;
    Owned<IPropertyTreeIterator> iter2 = cfg->getElements("LogSourceMap/LogSource");
    ForEach(*iter2)
    {
        StringBuffer name, groupName, dbName;
        ensureInputString(iter2->query().queryProp("@name"), false, name, -1, "LogSource @name required");
        ensureInputString(iter2->query().queryProp("@maptologgroup"), true, groupName, -1, "LogSource @maptologgroup required");
        ensureInputString(iter2->query().queryProp("@maptodb"), true, dbName, -1, "LogSource @maptodb required");
        Owned<CLogSource> logSource = new CLogSource(name.str(), groupName.str(), dbName.str());
        logSources.setValue(name.str(), logSource);
        logSourceCount++;
    }

    maxTriesGTS = cfg->getPropInt("MaxTriesGTS", DefaultMaxTriesGTS);
    loggingTransactionApp.set(cfg->hasProp("loggingTransaction") ? cfg->queryProp("loggingTransaction") : DefaultloggingTransactionApp);
    defaultTransactionApp.set(cfg->hasProp("defaultTransaction") ? cfg->queryProp("defaultTransaction") : DefaultTransactionApp);
    loggingTransactionCount = 0;

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

    StringArray opts;
    setCassandraLogAgentOption(opts, "contact_points", dbServer.str());
    if (!dbUserID.isEmpty())
    {
        setCassandraLogAgentOption(opts, "user", dbUserID.str());
        setCassandraLogAgentOption(opts, "password", dbPassword.str());
    }

    cassSession->setOptions(opts);

    //prepare defaultDB
    ensureKeySpace();

    //prepare transSeed tables
    initTransSeedTable();

    //Read logging transaction seed
    queryTransactionSeed(loggingTransactionApp.get(), transactionSeed);
}

void CCassandraLogAgent::ensureKeySpace()
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

void CCassandraLogAgent::initTransSeedTable()
{
    //Create transaction seed table as needed
    StringBuffer transSeedTableKeys;
    StringArray transSeedTableColumnNames, transSeedTableColumnTypes;
    transSeedTableColumnNames.append("id");
    transSeedTableColumnTypes.append("int");
    transSeedTableColumnNames.append("agent_name");
    transSeedTableColumnTypes.append("varchar");
    transSeedTableColumnNames.append("application");
    transSeedTableColumnTypes.append("varchar");
    transSeedTableColumnNames.append("update_time");
    transSeedTableColumnTypes.append("timestamp");
    transSeedTableKeys.set("application, agent_name"); //primary keys

    setKeySpace(defaultDB.str());
    cassSession->connect();
    createTable(defaultDB.str(), "transactions", transSeedTableColumnNames, transSeedTableColumnTypes, transSeedTableKeys.str());

    unsigned transactionCount = 0;
    if (executeSimpleSelectStatement("SELECT COUNT(*) FROM transactions", transactionCount) == 0)
    {
        VStringBuffer st("INSERT INTO transactions (id, agent_name, application, update_time) values ( 10000, '%s', '%s', toUnixTimestamp(now()));", agentName.get(), loggingTransactionApp.get());
        executeSimpleStatement(st.str());

        st.setf("INSERT INTO transactions (id, agent_name, application, update_time) values ( 10000, '%s', '%s', toUnixTimestamp(now()));", agentName.get(), defaultTransactionApp.get());
        executeSimpleStatement(st.str());
    }
    cassSession->disconnect();
}

void CCassandraLogAgent::queryTransactionSeed(const char* appName, StringBuffer& seed)
{
    CriticalBlock b(transactionSeedCrit);

    unsigned seedInt = 0;
    VStringBuffer st("SELECT id FROM transactions WHERE agent_name ='%s' AND application = '%s'", agentName.get(), appName);
    setKeySpace(defaultDB.str()); //Switch to defaultDB since it may not be the current keyspace.
    cassSession->connect();
    executeSimpleSelectStatement(st.str(), seedInt);
    seed.setf("%d", seedInt);

    //update transactions for the next seed
    VStringBuffer updateQuery("UPDATE transactions SET id=%d WHERE agent_name ='%s' AND application = '%s'", ++seedInt, agentName.get(), appName);
    executeSimpleStatement(updateQuery.str());
    cassSession->disconnect();
}

bool CCassandraLogAgent::getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    unsigned retry = 1;
    while (1)
    {
        try
        {
            const char* appName = req.getApplication();
            if (!appName || !*appName)
                appName = defaultTransactionApp.get();

            StringBuffer logSeed;
            queryTransactionSeed(appName, logSeed);

            if (!logSeed.length())
                throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to get TransactionSeed");

            resp.setSeedId(logSeed.str());
            resp.setStatusCode(0);
            return true;
        }
        catch (IException* e)
        {
            StringBuffer errorStr, errorMessage;
            errorMessage.append("Failed to get TransactionSeed: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
            ERRLOG("%s -- try %d", errorMessage.str(), retry);
            e->Release();
            if (retry < maxTriesGTS)
            {
                Sleep(retry*3000);
                retry++;
            }
            else
            {
                resp.setStatusCode(-1);
                resp.setStatusMessage(errorMessage.str());
                break;
            }
        }
    }
    return false;
}

bool CCassandraLogAgent::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp)
{
    try
    {
        StringBuffer requestBuf = req.getUpdateLogRequest();
        if (requestBuf.isEmpty())
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log request.");

        StringBuffer logDB, logSource;
        requestBuf.insert(0, "<LogRequest>");
        requestBuf.append("</LogRequest>");
        Owned<IPropertyTree> logRequestTree = createPTreeFromXMLString(requestBuf.length(), requestBuf.str());
        if (!logRequestTree)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log request.");

        CLogGroup* logGroup = checkLogSource(logRequestTree, logSource, logDB);
        if (!logGroup)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Log Group %s undefined.", logSource.str());

        StringBuffer logID;
        getLoggingTransactionID(logID);

        CIArrayOf<CLogTable>& logTables = logGroup->getLogTables();
        setKeySpace(logDB.str());
        ForEachItemIn(i, logTables)
        {
            CLogTable& table = logTables.item(i);

            StringBuffer cqlStatement;
            if(!buildUpdateLogStatement(logRequestTree, logDB.str(), table, logID, cqlStatement))
                throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed in creating SQL statement.");

            if (getEspLogLevel() >= LogMax)
                DBGLOG("CQL: %s\n", cqlStatement.str());

            cassSession->connect();
            executeSimpleStatement(cqlStatement);
            cassSession->disconnect();
        }
        resp.setStatusCode(0);
        return true;
    }
    catch (IException* e)
    {
        StringBuffer errorStr, errorMessage;
        errorMessage.append("Failed to update log: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
        ERRLOG("%s", errorMessage.str());
        e->Release();
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorMessage.str());
    }
    return false;
}

CLogGroup* CCassandraLogAgent::checkLogSource(IPropertyTree* logRequest, StringBuffer& source, StringBuffer& logDB)
{
    if (logSourceCount == 0)
    {//if no log source is configured, use default Log Group and DB
        logDB.set(defaultDB.str());
        return logGroups.getValue(defaultLogGroup.get());
    }
    source = logRequest->queryProp("Source");
    if (source.isEmpty())
        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log Source from request.");
    CLogSource* logSource = logSources.getValue(source.str());
    if (!logSource)
        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Log Source %s undefined.", source.str());

    logDB.set(logSource->getDBName());
    return logGroups.getValue(logSource->getGroupName());
}

void CCassandraLogAgent::getLoggingTransactionID(StringBuffer& id)
{
    CriticalBlock b(uniqueIDCrit);
    id.set(transactionSeed.str()).append("-").append(++loggingTransactionCount);
}

bool CCassandraLogAgent::buildUpdateLogStatement(IPropertyTree* logRequest, const char* logDB, CLogTable& table, StringBuffer& logID, StringBuffer& cqlStatement)
{
    StringBuffer fields, values;
    BoolHash handledFields;
    CIArrayOf<CLogField>& logFields = table.getLogFields();
    ForEachItemIn(i, logFields) //Go through data items to be logged
    {
        CLogField& logField = logFields.item(i);

        StringBuffer colName = logField.getMapTo();
        bool* found = handledFields.getValue(colName.str());
        if (found && *found)
            continue;

        StringBuffer path = logField.getName();
        if (path.charAt(path.length() - 1) == ']')
        {//Attr filter. Separate the last [] from the path.
            const char* pTr = path.str();
            const char* ppTr = strrchr(pTr, '[');
            if (!ppTr)
                continue;

            StringBuffer attr;
            attr.set(ppTr+1);
            attr.setLength(attr.length() - 1);
            path.setLength(ppTr - pTr);

            StringBuffer colValue;
            Owned<IPropertyTreeIterator> itr = logRequest->getElements(path.str());
            ForEach(*itr)
            {//Log the first valid match just in case more than one matches.
                IPropertyTree& ppTree = itr->query();
                colValue.set(ppTree.queryProp(attr.str()));
                if (colValue.length())
                {
                    addField(logField, colName.str(), colValue, fields, values);
                    handledFields.setValue(colName.str(), true);
                    break;
                }
            }
            continue;
        }

        Owned<IPropertyTreeIterator> itr = logRequest->getElements(path.str());
        ForEach(*itr)
        {
            IPropertyTree& ppTree = itr->query();

            StringBuffer colValue;
            if (ppTree.hasChildren()) //This is a tree branch.
                toXML(&ppTree, colValue);
            else
                ppTree.getProp(NULL, colValue);

            if (colValue.length())
            {
                addField(logField, colName.str(), colValue, fields, values);
                handledFields.setValue(colName.str(), true);
                break;
            }
        }
    }

    //add any default fields that may be required but not in request.
    addMissingFields(logFields, handledFields, fields, values);
    appendFieldInfo("log_id", logID, fields, values, true);

    cqlStatement.setf("INSERT INTO %s.%s (%s, date_added) values (%s, toUnixTimestamp(now()));",
        logDB, table.getTableName(), fields.str(), values.str());
    return true;
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

void CCassandraLogAgent::appendFieldInfo(const char* field, StringBuffer& value, StringBuffer& fields, StringBuffer& values, bool quoted)
{
    if(values.length() != 0)
        values.append(',');
    if (quoted)
        values.append('\'').append(value.length(), value.str()).append('\'');
    else
        values.append(value.length(), value.str());

    if(fields.length() != 0)
        fields.append(',');
    fields.append(field);
}

void CCassandraLogAgent::addMissingFields(CIArrayOf<CLogField>& logFields, BoolHash& handledFields, StringBuffer& fields, StringBuffer& values)
{
    ForEachItemIn(i, logFields) //Go through data items to be logged
    {
        CLogField& logField = logFields.item(i);
        const char* colName = logField.getMapTo();
        bool* found = handledFields.getValue(colName);
        if (found && *found)
            continue;
        StringBuffer value = logField.getDefault();
        if (!value.isEmpty())
            addField(logField, colName, value, fields, values);
    }
}

void CCassandraLogAgent::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    return; //No filter in CCassandraLogAgent for now
}

void CCassandraLogAgent::setKeySpace(const char *keyspace)
{
    StringArray opts;
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

void CCassandraLogAgent::executeSimpleStatement(const char* st)
{
    CassandraStatement statement(cassSession->prepareStatement(st, getEspLogLevel()>LogNormal));
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
}

void CCassandraLogAgent::executeSimpleStatement(StringBuffer& st)
{
    CassandraFuture futurePrep(cass_session_prepare_n(cassSession->querySession(), st.str(), st.length()));
    futurePrep.wait("prepare statement");

    Owned<CassandraPrepared> prepared = new CassandraPrepared(cass_future_get_prepared(futurePrep), NULL);
    CassandraStatement statement(prepared.getClear());
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
}

unsigned CCassandraLogAgent::executeSimpleSelectStatement(const char* st, unsigned& resultValue)
{
    CassandraStatement statement(cassSession->prepareStatement(st, getEspLogLevel()>LogNormal));
    CassandraFuture future(cass_session_execute(cassSession->querySession(), statement));
    future.wait("execute");
    CassandraResult result(cass_future_get_result(future));
    resultValue = getUnsignedResult(NULL, getSingleResult(result));
    return resultValue;
}

extern "C"
{
CASSABDRALOGAGENT_API IEspLogAgent* newLoggingAgent()
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
