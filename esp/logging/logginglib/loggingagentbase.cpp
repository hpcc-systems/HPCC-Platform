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
#include "loggingagentbase.hpp"

static const char* const defaultTransactionTable = "transactions";
static const char* const defaultTransactionAppName = "accounting_log";
static const char* const defaultLoggingTransactionAppName = "logging_transaction";

void CLogContentFilter::readAllLogFilters(IPropertyTree* cfg)
{
    bool groupFilterRead = false;
    VStringBuffer xpath("Filters/Filter[@type='%s']", espLogContentGroupNames[ESPLCGBackEndResp]);
    IPropertyTree* filter = cfg->queryBranch(xpath.str());
    if (filter && filter->hasProp("@value"))
    {
        logBackEndResp = filter->getPropBool("@value");
        groupFilterRead = true;
    }

    xpath.setf("Filters/Filter[@type='%s']", espLogContentGroupNames[ESPLCGBackEndReq]);
    filter = cfg->queryBranch(xpath.str());
    if (filter && filter->hasProp("@value"))
    {
        logBackEndReq = filter->getPropBool("@value");
        groupFilterRead = true;
    }

    for (unsigned i = 0; i < ESPLCGBackEndReq; i++)
    {
        if (readLogFilters(cfg, i))
            groupFilterRead = true;
    }

    if (!groupFilterRead)
    {
        groupFilters.clear();
        readLogFilters(cfg, ESPLCGAll);
    }
}

bool CLogContentFilter::readLogFilters(IPropertyTree* cfg, unsigned groupID)
{
    Owned<CESPLogContentGroupFilters> espLogContentGroupFilters = new CESPLogContentGroupFilters((ESPLogContentGroup) groupID);
    StringBuffer xpath;
    if (groupID != ESPLCGAll)
        xpath.appendf("Filters/Filter[@type='%s']", espLogContentGroupNames[groupID]);
    else
        xpath.append("Filters/Filter");
    Owned<IPropertyTreeIterator> filters = cfg->getElements(xpath.str());
    ForEach(*filters)
    {
        IPropertyTree &filter = filters->query();
        StringBuffer value = filter.queryProp("@value");
        if (!value.length())
            continue;

        //clean "//"
        unsigned idx = value.length()-1;
        while (idx)
        {
            if ((value.charAt(idx-1) == '/') && (value.charAt(idx) == '/'))
                value.remove(idx, 1);
            idx--;
        }

        //clean "/*" at the end
        while ((value.length() > 1) && (value.charAt(value.length()-2) == '/') && (value.charAt(value.length()-1) == '*'))
            value.setLength(value.length() - 2);

        if (value.length() && !streq(value.str(), "*") && !streq(value.str(), "/") && !streq(value.str(), "*/"))
        {
            espLogContentGroupFilters->addFilter(value.str());
        }
        else
        {
            espLogContentGroupFilters->clearFilters();
            break;
        }
    }

    bool hasFilter = espLogContentGroupFilters->getFilterCount() > 0;
    if (hasFilter)
        groupFilters.append(*espLogContentGroupFilters.getClear());
    return hasFilter;
}

void CLogContentFilter::addLogContentBranch(StringArray& branchNames, IPropertyTree* contentToLogBranch, IPropertyTree* updateLogRequestTree)
{
    IPropertyTree* pTree = updateLogRequestTree;
    unsigned numOfBranchNames = branchNames.length();
    unsigned i = 0;
    while (i < numOfBranchNames)
    {
        const char* branchName = branchNames.item(i);
        if (branchName && *branchName)
            pTree = ensurePTree(pTree, branchName);
        i++;
    }
    pTree->addPropTree(contentToLogBranch->queryName(), LINK(contentToLogBranch));
}

void CLogContentFilter::filterAndAddLogContentBranch(StringArray& branchNamesInFilter, unsigned idx,
    StringArray& branchNamesInLogContent, IPropertyTree* originalLogContentBranch, IPropertyTree* updateLogRequestTree, bool& logContentEmpty)
{
    Owned<IPropertyTreeIterator> contentItr = originalLogContentBranch->getElements(branchNamesInFilter.item(idx));
    ForEach(*contentItr)
    {
        IPropertyTree& contentToLogBranch = contentItr->query();
        if (idx == branchNamesInFilter.length() - 1)
        {
            addLogContentBranch(branchNamesInLogContent, &contentToLogBranch, updateLogRequestTree);
            logContentEmpty = false;
        }
        else
        {
            branchNamesInLogContent.append(contentToLogBranch.queryName());
            filterAndAddLogContentBranch(branchNamesInFilter, idx+1, branchNamesInLogContent, &contentToLogBranch,
                updateLogRequestTree, logContentEmpty);
            branchNamesInLogContent.remove(branchNamesInLogContent.length() - 1);
        }
    }
}

void CLogContentFilter::filterLogContentTree(StringArray& filters, IPropertyTree* originalContentTree, IPropertyTree* newLogContentTree, bool& logContentEmpty)
{
    ForEachItemIn(i, filters)
    {
        const char* logContentFilter = filters.item(i);
        if(!logContentFilter || !*logContentFilter)
            continue;

        StringArray branchNamesInFilter, branchNamesInLogContent;
        branchNamesInFilter.appendListUniq(logContentFilter, "/");
        filterAndAddLogContentBranch(branchNamesInFilter, 0, branchNamesInLogContent, originalContentTree, newLogContentTree, logContentEmpty);
    }
}

IEspUpdateLogRequestWrap* CLogContentFilter::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    const char* logContent = req->getUpdateLogRequest();
    Owned<IPropertyTree> logRequestTree = req->getLogRequestTree();
    Owned<IPropertyTree> updateLogRequestTree = createPTree("UpdateLogRequest");

    StringBuffer source;
    if (groupFilters.length() < 1)
    {//No filter
        if (logRequestTree)
        {
            updateLogRequestTree->addPropTree(logRequestTree->queryName(), LINK(logRequestTree));
        }
        else if (logContent && *logContent)
        {
            Owned<IPropertyTree> pTree = createPTreeFromXMLString(logContent);
            source = pTree->queryProp("Source");
            updateLogRequestTree->addPropTree(pTree->queryName(), LINK(pTree));
        }
        else
        {
            Owned<IPropertyTree> espContext = req->getESPContext();
            Owned<IPropertyTree> userContext = req->getUserContext();
            Owned<IPropertyTree> userRequest = req->getUserRequest();
            const char* userResp = req->getUserResponse();
            const char* logDatasets = req->getLogDatasets();
            const char* backEndReq = req->getBackEndRequest();
            const char* backEndResp = req->getBackEndResponse();
            if (!espContext && !userContext && !userRequest && (!userResp || !*userResp) && (!backEndResp || !*backEndResp))
                throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log content");
            source = userContext->queryProp("Source");

            StringBuffer espContextXML, userContextXML, userRequestXML;
            IPropertyTree* logContentTree = ensurePTree(updateLogRequestTree, "LogContent");
            if (espContext)
            {
                logContentTree->addPropTree(espContext->queryName(), LINK(espContext));
            }
            if (userContext)
            {
                IPropertyTree* pTree = ensurePTree(logContentTree, espLogContentGroupNames[ESPLCGUserContext]);
                pTree->addPropTree(userContext->queryName(), LINK(userContext));
            }
            if (userRequest)
            {
                IPropertyTree* pTree = ensurePTree(logContentTree, espLogContentGroupNames[ESPLCGUserReq]);
                pTree->addPropTree(userRequest->queryName(), LINK(userRequest));
            }
            if (!isEmptyString(userResp))
            {
                IPropertyTree* pTree = ensurePTree(logContentTree, espLogContentGroupNames[ESPLCGUserResp]);
                Owned<IPropertyTree> userRespTree = createPTreeFromXMLString(userResp);
                pTree->addPropTree(userRespTree->queryName(), LINK(userRespTree));
            }
            if (!isEmptyString(logDatasets))
            {
                IPropertyTree* pTree = ensurePTree(logContentTree, espLogContentGroupNames[ESPLCGLogDatasets]);
                Owned<IPropertyTree> logDatasetTree = createPTreeFromXMLString(logDatasets);
                pTree->addPropTree(logDatasetTree->queryName(), LINK(logDatasetTree));
            }
            if (!isEmptyString(backEndReq))
                logContentTree->addProp(espLogContentGroupNames[ESPLCGBackEndReq], backEndReq);
            if (!isEmptyString(backEndResp))
                logContentTree->addProp(espLogContentGroupNames[ESPLCGBackEndResp], backEndResp);
        }
    }
    else
    {
        bool logContentEmpty = true;
        IPropertyTree* logContentTree = ensurePTree(updateLogRequestTree, "LogContent");
        if (logRequestTree)
        {
            filterLogContentTree(groupFilters.item(0).getFilters(), logRequestTree, logContentTree, logContentEmpty);
        }
        else if (logContent && *logContent)
        {
            Owned<IPropertyTree> originalContentTree = createPTreeFromXMLString(logContent);
            source = originalContentTree->queryProp("Source");
            filterLogContentTree(groupFilters.item(0).getFilters(), originalContentTree, logContentTree, logContentEmpty);
        }
        else
        {
            for (unsigned group = 0; group < ESPLCGBackEndResp; group++)
            {
                Owned<IPropertyTree> originalContentTree;
                if (group == ESPLCGESPContext)
                    originalContentTree.setown(req->getESPContext());
                else if (group == ESPLCGUserContext)
                {
                    originalContentTree.setown(req->getUserContext());
                    source = originalContentTree->queryProp("Source");
                }
                else if (group == ESPLCGUserReq)
                    originalContentTree.setown(req->getUserRequest());
                else if (group == ESPLCGLogDatasets)
                {
                    const char* logDatasets = req->getLogDatasets();
                    if (logDatasets && *logDatasets)
                        originalContentTree.setown(createPTreeFromXMLString(logDatasets));
                }
                else //group = ESPLCGUserResp
                {
                    const char* resp = req->getUserResponse();
                    if (!resp || !*resp)
                        continue;
                    originalContentTree.setown(createPTreeFromXMLString(resp));
                }
                if (!originalContentTree)
                    continue;

                IPropertyTree* newContentTree = ensurePTree(logContentTree, espLogContentGroupNames[group]);
                bool hasFilters = false;
                ForEachItemIn(i, groupFilters)
                {
                    CESPLogContentGroupFilters& filtersGroup = groupFilters.item(i);
                    if (filtersGroup.getGroup() == group)
                    {
                        if (group != ESPLCGESPContext)//For non ESPLCGESPContext, we want to keep the root of original tree.
                            newContentTree = ensurePTree(newContentTree, originalContentTree->queryName());
                        filterLogContentTree(filtersGroup.getFilters(), originalContentTree, newContentTree, logContentEmpty);
                        hasFilters =  true;
                        break;
                    }
                }

                if (!hasFilters)
                {
                    newContentTree->addPropTree(originalContentTree->queryName(), LINK(originalContentTree));
                    logContentEmpty = false;
                }
            }
            if (logBackEndReq)
            {
                const char* request = req->getBackEndRequest();
                if (!isEmptyString(request))
                {
                    logContentTree->addProp(espLogContentGroupNames[ESPLCGBackEndReq], request);
                    logContentEmpty = false;
                }
            }
            if (logBackEndResp)
            {
                const char* resp = req->getBackEndResponse();
                if (resp && *resp)
                {
                    logContentTree->addProp(espLogContentGroupNames[ESPLCGBackEndResp], resp);
                    logContentEmpty = false;
                }
            }
        }
        if (logContentEmpty)
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log content");
    }
    if (!source.isEmpty())
        updateLogRequestTree->addProp("LogContent/Source", source.str());

    const char* option = req->getOption();
    if (option && *option)
        updateLogRequestTree->addProp("Option", option);

    StringBuffer updateLogRequestXML;
    toXML(updateLogRequestTree, updateLogRequestXML);
    ESPLOG(LogMax, "filtered content and option: <%s>", updateLogRequestXML.str());

    return new CUpdateLogRequestWrap(req->getGUID(), req->getOption(), updateLogRequestXML.str());
}

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
    if (!hasService(LGSTGetTransactionSeed))
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "%s: no getTransactionSeed service configured", agentName.get());

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
            OERRLOG("%s -- try %d", errorMessage.str(), retry);
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
    if (!hasService(LGSTUpdateLOG))
        throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "%s: no updateLog service configured", agentName.get());

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

            ESPLOG(LogNormal, "LAgent UpdateLog BuildStat %d done: %dms\n", i, msTick() -  startTime);
            ESPLOG(LogMax, "UpdateLog: %s\n", updateDBStatement.str());

            executeUpdateLogStatement(updateDBStatement);
            ESPLOG(LogNormal, "LAgent UpdateLog ExecStat %d done: %dms\n", i, msTick() -  startTime);
        }
        resp.setStatusCode(0);
        ret = true;
    }
    catch (IException* e)
    {
        StringBuffer errorStr, errorMessage;
        errorMessage.append("Failed to update log: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
        IERRLOG("%s", errorMessage.str());
        e->Release();
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorMessage.str());
    }
    ESPLOG(LogNormal, "LAgent UpdateLog total=%dms\n", msTick() -  startTime);
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
    if (table.getEnableLogID()) {
        appendFieldInfo("log_id", logID, fields, values, true);
    }

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

IEspUpdateLogRequestWrap* CDBLogAgentBase::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    //No filter in CDBSQLLogAgent
    return req;
}
