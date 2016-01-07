/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include "httpclient.hpp"
#include "ws_loggingservice_esp.ipp"
#include "LoggingErrors.hpp"
#include "loggingcommon.hpp"
#include "loggingagentbase.hpp"
#include "loggingagent.hpp"

const int DefaultMaxTriesGTS = -1;
const char* const PropESPServer = "ESPServer";
const char* const PropServerUrl = "@url";
const char* const PropServerUserID = "@user";
const char* const PropServerPassword = "@password";
const char* const PropServerWaitingSeconds = "MaxServerWaitingSeconds";
const char* const MaxTriesGTS = "MaxTriesGTS";

bool CESPServerLoggingAgent::init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
{
    if (!cfg)
        return false;

    IPropertyTree* espServer = cfg->queryBranch(PropESPServer);
    if(!espServer)
        throw MakeStringException(-1,"Unable to find ESPServer settings for log agent %s:%s", name, type);

    const char* url = espServer->queryProp(PropServerUrl);
    if (url && *url)
        serverUrl.set(url);

    const char* userID = espServer->queryProp(PropServerUserID);
    const char* password = espServer->queryProp(PropServerPassword);
    if (userID && *userID && password && *password)
    {
        serverUserID.set(userID);
        decrypt(serverPassword, password);
    }
    maxServerWaitingSeconds = cfg->getPropInt(PropServerWaitingSeconds);
    maxGTSRetries = cfg->getPropInt(MaxTriesGTS, DefaultMaxTriesGTS);

    readAllLogFilters(cfg);
    return true;
}

void CESPServerLoggingAgent::readAllLogFilters(IPropertyTree* cfg)
{
    bool groupFilterRead = false;
    VStringBuffer xpath("Filters/Filter[@type='%s']", espLogContentGroupNames[ESPLCGBackEndResp]);
    IPropertyTree* filter = cfg->queryBranch(xpath.str());
    if (filter && filter->hasProp("@value"))
    {
        logBackEndResp = filter->getPropBool("@value");
        groupFilterRead = true;
    }

    for (unsigned i = 0; i < ESPLCGBackEndResp; i++)
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

bool CESPServerLoggingAgent::readLogFilters(IPropertyTree* cfg, unsigned groupID)
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

bool CESPServerLoggingAgent::getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    bool bRet = false;
    StringBuffer soapreq(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
        " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
        " <soap:Body>");
    soapreq.append("<GetTransactionSeedRequest/>");
    soapreq.append("</soap:Body></soap:Envelope>");

    unsigned retry = 1;
    while (1)
    {
        try
        {
            int statusCode = 0;
            StringBuffer statusMessage, transactionSeed;
            if (!getTransactionSeed(soapreq, statusCode, statusMessage, transactionSeed))
                throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed,"Failed to get TransactionSeed");

            resp.setSeedId(transactionSeed.str());
            resp.setStatusCode(statusCode);
            if (statusMessage.length())
                resp.setStatusMessage(statusMessage.str());
            bRet = true;
            break;
        }
        catch (IException* e)
        {
            StringBuffer errorStr, errorMessage;
            errorMessage.append("Failed to get TransactionSeed: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
            ERRLOG("%s -- try %d", errorMessage.str(), retry);
            e->Release();
            if (retry < maxGTSRetries)
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

bool CESPServerLoggingAgent::getTransactionSeed(StringBuffer& soapreq, int& statusCode, StringBuffer& statusMessage, StringBuffer& seedID)
{
    StringBuffer status, response;
    if (!sendHTTPRequest(soapreq, response, status) || !response.length() || !status.length())
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to send Transaction Seed request to %s", serverUrl.str());

    if (!strieq(status, "200 OK"))
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "%s", status.str());

    Owned<IPropertyTree> pTree = createPTreeFromXMLString(response.str());
    if (!pTree)
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to read response from %s", serverUrl.str());

    statusCode = pTree->getPropInt("soap:Body/GetTransactionSeedResponse/StatusCode");
    statusMessage.set(pTree->queryProp("soap:Body/GetTransactionSeedResponse/StatusMessage"));
    seedID.set(pTree->queryProp("soap:Body/GetTransactionSeedResponse/SeedId"));

    if (statusCode || !seedID.length())
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to get Transaction Seed from %s", serverUrl.str());
    return true;
}

bool CESPServerLoggingAgent::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp)
{
    try
    {
        StringBuffer soapreq(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
            " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
            " <soap:Body>"
            );
        soapreq.append(req.getUpdateLogRequest());
        soapreq.append("</soap:Body></soap:Envelope>");

        StringBuffer status, respStr;
        if (sendHTTPRequest(soapreq, respStr, status) && status.length() && strieq(status, "200 OK"))
            resp.setStatusCode(0);
        else if (status.length() && !strieq(status, "200 OK"))
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "%s", status.str());
        else if (respStr.length())
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "%s", respStr.str());
        else
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to send update log request to %s", serverUrl.str());
    }
    catch (IException* e)
    {//retry will be in update log queue.
        StringBuffer errorStr, errorMessage;
        errorMessage.append("Failed to update log: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
        ERRLOG("%s", errorMessage.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorMessage.str());
        e->Release();
    }
    return true;
}

void CESPServerLoggingAgent::addLogContentBranch(StringArray& branchNames, IPropertyTree* contentToLogBranch, IPropertyTree* updateLogRequestTree)
{
    IPropertyTree* pTree = updateLogRequestTree;
    unsigned numOfBranchNames = branchNames.length();
    if (numOfBranchNames > 0)
    {
        unsigned i = 0;
        while (i < numOfBranchNames)
        {
            const char* branchName = branchNames.item(i);
            if (branchName && *branchName)
                pTree = ensurePTree(pTree, branchName);
            i++;
        }
    }
    pTree->addPropTree(contentToLogBranch->queryName(), LINK(contentToLogBranch));
}

void CESPServerLoggingAgent::filterAndAddLogContentBranch(StringArray& branchNamesInFilter, unsigned idx,
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

void CESPServerLoggingAgent::filterLogContentTree(StringArray& filters, IPropertyTree* originalContentTree, IPropertyTree* newLogContentTree, bool& logContentEmpty)
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

void CESPServerLoggingAgent::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    const char* logContent = req->getUpdateLogRequest();
    Owned<IPropertyTree> updateLogRequestTree = createPTree("UpdateLogRequest");

    if (groupFilters.length() < 1)
    {//No filter
        if (logContent && *logContent)
        {
            Owned<IPropertyTree> pTree = createPTreeFromXMLString(logContent);
            updateLogRequestTree->addPropTree(pTree->queryName(), LINK(pTree));
        }
        else
        {
            Owned<IPropertyTree> espContext = req->getESPContext();
            Owned<IPropertyTree> userContext = req->getUserContext();
            Owned<IPropertyTree> userRequest = req->getUserRequest();
            const char* userResp = req->getUserResponse();
            const char* backEndResp = req->getBackEndResponse();
            if (!espContext && !userContext && !userRequest && (!userResp || !*userResp) && (!backEndResp || !*backEndResp))
                throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "Failed to read log content");

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
            if (userResp && *userResp)
            {
                IPropertyTree* pTree = ensurePTree(logContentTree, espLogContentGroupNames[ESPLCGUserResp]);
                Owned<IPropertyTree> userRespTree = createPTreeFromXMLString(userResp);
                pTree->addPropTree(userRespTree->queryName(), LINK(userRespTree));
            }
            if (backEndResp && *backEndResp)
                logContentTree->addProp(espLogContentGroupNames[ESPLCGBackEndResp], backEndResp);
        }
    }
    else
    {
        bool logContentEmpty = true;
        IPropertyTree* logContentTree = ensurePTree(updateLogRequestTree, "LogContent");
        if (logContent && *logContent)
        {
            Owned<IPropertyTree> originalContentTree = createPTreeFromXMLString(logContent);
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
                    originalContentTree.setown(req->getUserContext());
                else if (group == ESPLCGUserReq)
                    originalContentTree.setown(req->getUserRequest());
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
    const char* option = req->getOption();
    if (option && *option)
        updateLogRequestTree->addProp("Option", option);

    StringBuffer updateLogRequestXML;
    toXML(updateLogRequestTree, updateLogRequestXML);
    DBGLOG("filtered content and option: <%s>", updateLogRequestXML.str());
    req->clearOriginalContent();
    req->setUpdateLogRequest(updateLogRequestXML.str());
}

bool CESPServerLoggingAgent::sendHTTPRequest(StringBuffer& req, StringBuffer &resp, StringBuffer &status)
{
    Owned<IHttpClientContext> httpctx = getHttpClientContext();
    Owned <IHttpClient> httpclient = httpctx->createHttpClient(NULL, serverUrl.str());
    if (serverUserID.length() && serverPassword.length())
    {
        httpclient->setUserID(serverUserID.str());
        httpclient->setPassword(serverPassword.str());
    }
    httpclient->setTimeOut(maxServerWaitingSeconds);

    return !httpclient->sendRequest("POST", "text/xml", req, resp, status);
}

extern "C"
{
ESPSERVERLOGGINGAGENT_API IEspLogAgent* newLoggingAgent()
{
    return new CESPServerLoggingAgent();
}
}
