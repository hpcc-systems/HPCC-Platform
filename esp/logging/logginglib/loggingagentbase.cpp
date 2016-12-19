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
#include "loggingagentbase.hpp"

static const char* const defaultTransactionTable = "transactions";
static const char* const defaultTransactionAppName = "accounting_log";
static const char* const defaultLoggingTransactionAppName = "logging_transaction";

void CDBLogAgentBase::readDBCfg(IPropertyTree* cfg, StringBuffer& server, StringBuffer& dbUser, StringBuffer& dbPassword)
{
    ensureInputString(cfg->queryProp("@server"), true, server, -1, "Database server required");
    ensureInputString(cfg->queryProp("@dbName"), true, defaultDB, -1, "Database name required");

    transactionTable.set(cfg->hasProp("@dbTableName") ? cfg->queryProp("@dbTableName") : defaultTransactionTable);
    dbUser.set(cfg->queryProp("@dbUser"));
    const char* encodedPassword = cfg->queryProp("@dbPassWord");
    if(encodedPassword && *encodedPassword)
        decrypt(dbPassword, encodedPassword);
}

void CDBLogAgentBase::readTransactionCfg(IPropertyTree* cfg)
{
    //defaultTransactionApp: if no APP name is given, which APP name (TableName) should be used to get a transaction seed?
    //loggingTransactionApp: the TableName used to get a transaction seed for this logging agent
    defaultTransactionApp.set(cfg->hasProp("defaultTransaction") ? cfg->queryProp("defaultTransaction") : defaultTransactionAppName);
    loggingTransactionApp.set(cfg->hasProp("loggingTransaction") ? cfg->queryProp("loggingTransaction") : defaultLoggingTransactionAppName);
    loggingTransactionCount = 0;
}

bool CDBLogAgentBase::getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    bool bRet = false;
    StringBuffer appName = req.getApplication();
    appName.trim();
    if (appName.length() == 0)
        appName = defaultTransactionApp.get();

    unsigned retry = 1;
    while (1)
    {
        try
        {
            StringBuffer logSeed;
            queryTransactionSeed(appName, logSeed);
            if (!logSeed.length())
                throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to get TransactionSeed");

            resp.setSeedId(logSeed.str());
            resp.setStatusCode(0);
            bRet = true;
            break;
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
    return bRet;
}

bool CDBLogAgentBase::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp)
{
    unsigned startTime = (getEspLogLevel()>=LogNormal) ? msTick() : 0;
    bool ret = false;
    try
    {
        const char* updateLogReq = req.getUpdateLogRequest();
        if (!updateLogReq || !*updateLogReq)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log request.");

        StringBuffer requestBuf, logDB, logSource;
        requestBuf.append("<LogRequest>").append(updateLogReq).append("</LogRequest>");
        Owned<IPropertyTree> logRequestTree = createPTreeFromXMLString(requestBuf.length(), requestBuf.str());
        if (!logRequestTree)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log request.");

        CLogGroup* logGroup = checkLogSource(logRequestTree, logSource, logDB);
        if (!logGroup)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Log Group %s undefined.", logSource.str());

        StringBuffer logID;
        getLoggingTransactionID(logID);

        CIArrayOf<CLogTable>& logTables = logGroup->getLogTables();
        ForEachItemIn(i, logTables)
        {
            CLogTable& table = logTables.item(i);

            StringBuffer updateDBStatement;
            if(!buildUpdateLogStatement(logRequestTree, logDB.str(), table, logID, updateDBStatement))
                throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed in creating SQL statement.");
            if (getEspLogLevel()>=LogNormal)
                DBGLOG("LAgent UpdateLog BuildStat %d done: %dms\n", i, msTick() -  startTime);

            if (getEspLogLevel() >= LogMax)
                DBGLOG("UpdateLog: %s\n", updateDBStatement.str());

            executeUpdateLogStatement(updateDBStatement);
            if (getEspLogLevel()>=LogNormal)
                DBGLOG("LAgent UpdateLog ExecStat %d done: %dms\n", i, msTick() -  startTime);
        }
        resp.setStatusCode(0);
        ret = true;
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
    if (getEspLogLevel()>=LogNormal)
        DBGLOG("LAgent UpdateLog total=%dms\n", msTick() -  startTime);
    return ret;
}

CLogGroup* CDBLogAgentBase::checkLogSource(IPropertyTree* logRequest, StringBuffer& source, StringBuffer& logDB)
{
    if (logSourceCount == 0)
    {//if no log source is configured, use default Log Group and DB
        logDB.set(defaultDB.str());
        source.set(defaultLogGroup.get());
        return logGroups.getValue(defaultLogGroup.get());
    }
    source = logRequest->queryProp(logSourcePath.get());
    if (source.isEmpty())
        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log Source from request.");
    CLogSource* logSource = logSources.getValue(source.str());
    if (!logSource)
        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Log Source %s undefined.", source.str());

    logDB.set(logSource->getDBName());
    return logGroups.getValue(logSource->getGroupName());
}

void CDBLogAgentBase::getLoggingTransactionID(StringBuffer& id)
{
    id.set(loggingTransactionSeed.str()).append("-").append(++loggingTransactionCount);
}

bool CDBLogAgentBase::buildUpdateLogStatement(IPropertyTree* logRequest, const char* logDB,
    CLogTable& table, StringBuffer& logID, StringBuffer& updateDBStatement)
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

    setUpdateLogStatement(logDB, table.getTableName(), fields.str(), values.str(), updateDBStatement);
    return true;
}

void CDBLogAgentBase::appendFieldInfo(const char* field, StringBuffer& value, StringBuffer& fields, StringBuffer& values, bool quoted)
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

void CDBLogAgentBase::addMissingFields(CIArrayOf<CLogField>& logFields, BoolHash& handledFields, StringBuffer& fields, StringBuffer& values)
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

void CDBLogAgentBase::getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID)
{
    //Not implemented
}

void CDBLogAgentBase::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    //No filter in CDBSQLLogAgent
}
