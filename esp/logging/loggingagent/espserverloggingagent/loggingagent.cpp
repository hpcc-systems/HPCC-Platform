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

static const int DefaultMaxTriesGTS = -1;
static const char* const PropESPServer = "ESPServer";
static const char* const PropServerUrl = "@url";
static const char* const PropServerUserID = "@user";
static const char* const PropServerPassword = "@password";
static const char* const PropServerWaitingSeconds = "MaxServerWaitingSeconds";
static const char* const PropMaxTransIDLength = "MaxTransIDLength";
static const char* const PropMaxTransIDSequenceNumber = "MaxTransIDSequenceNumber";
static const char* const PropMaxTransSeedTimeoutMinutes = "MaxTransSeedTimeoutMinutes";
static const char* const PropTransactionSeedType = "TransactionSeedType";
static const char* const PropAlternativeTransactionSeedType = "AlternativeTransactionSeedType";
static const char* const DefaultTransactionSeedType =  "-";
static const char* const DefaultAlternativeTransactionSeedType =  "-X"; //local
static const char* const MaxTriesGTS = "MaxTriesGTS";
static const char* const appESPServerLoggingAgent = "ESPServerLoggingAgent";

bool CESPServerLoggingAgent::init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
{
    if (!cfg)
        return false;

    agentName.set(name);
    const char* servicesConfig = cfg->queryProp("@services");
    if (isEmptyString(servicesConfig))
        throw MakeStringException(-1,"No Logging Service defined for %s", agentName.get());
    setServices(servicesConfig);

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

    if (hasService(LGSTUpdateLOG))
    {
        BoolHash uniqueGroupNames;
        StringBuffer sourceName, groupName, dbName;
        Owned<IPropertyTreeIterator> iter = cfg->getElements("LogSourceMap/LogSource");
        ForEach(*iter)
        {
            ensureInputString(iter->query().queryProp("@name"), false, sourceName, -1, "LogSource @name required");
            ensureInputString(iter->query().queryProp("@maptologgroup"), true, groupName, -1, "LogSource @maptologgroup required");
            ensureInputString(iter->query().queryProp("@maptodb"), true, dbName, -1, "LogSource @maptodb required");
            Owned<CLogSource> logSource = new CLogSource(sourceName.str(), groupName.str(), dbName.str());
            logSources.setValue(sourceName.str(), logSource);

            bool* found = uniqueGroupNames.getValue(groupName.str());
            if (!found || !*found)
            {
                uniqueGroupNames.setValue(groupName.str(), true);

                unsigned maxSeq = 0;
                unsigned maxLength = 0;
                unsigned seedExpiredSeconds = 0;
                VStringBuffer xpath("LogGroup/[@name='%s']", groupName.str());
                IPropertyTree* logGroup = cfg->queryBranch(xpath.str());
                if (logGroup)
                {
                    maxLength = logGroup->getPropInt(PropMaxTransIDLength, 0);
                    maxSeq = logGroup->getPropInt(PropMaxTransIDSequenceNumber, 0),
                    seedExpiredSeconds = 60 * logGroup->getPropInt(PropMaxTransSeedTimeoutMinutes, 0);
                }

                if (!hasService(LGSTGetTransactionSeed) && !hasService(LGSTGetTransactionID))
                    continue;

                StringBuffer transactionSeed, statusMessage;
                getTransactionSeed(groupName.str(), transactionSeed, statusMessage);
                if (transactionSeed.length() > 0)
                {
                    Owned<CTransIDBuilder> entry = new CTransIDBuilder(transactionSeed.str(), false, transactionSeedType.get(),
                        maxLength, maxSeq, seedExpiredSeconds);
                    transIDMap.setValue(groupName.str(), entry);
                    if (iter->query().getPropBool("@default", false))
                        defaultGroup.set(groupName.str());
                }
                else
                    PROGLOG("Failed to get TransactionSeed for <%s>", groupName.str());
            }
        }
        logContentFilter.readAllLogFilters(cfg);
    }

    if (!hasService(LGSTGetTransactionSeed) && !hasService(LGSTGetTransactionID))
        return true;

    maxGTSRetries = cfg->getPropInt(MaxTriesGTS, DefaultMaxTriesGTS);
    transactionSeedType.set(cfg->hasProp(PropTransactionSeedType) ? cfg->queryProp(PropTransactionSeedType) :
        DefaultTransactionSeedType);
    alternativeTransactionSeedType.set(cfg->hasProp(PropAlternativeTransactionSeedType) ?
        cfg->queryProp(PropAlternativeTransactionSeedType) : DefaultAlternativeTransactionSeedType);

    StringBuffer localTransactionSeed;
    createLocalTransactionSeed(localTransactionSeed);
    Owned<CTransIDBuilder> localTransactionEntry = new CTransIDBuilder(localTransactionSeed.str(), true, alternativeTransactionSeedType.get(),
        cfg->getPropInt(PropMaxTransIDLength, 0), cfg->getPropInt(PropMaxTransIDSequenceNumber, 0),
        60 * cfg->getPropInt(PropMaxTransSeedTimeoutMinutes, 0));
    transIDMap.setValue(appESPServerLoggingAgent, localTransactionEntry);

    return true;
}



void CESPServerLoggingAgent::createLocalTransactionSeed(StringBuffer& transactionSeed)
{
    unsigned ip = queryHostIP().iphash();
    unsigned mstick6char = ((unsigned)usTick() & 0xFFFFFF);
    unsigned processId2char = ((unsigned)GetCurrentProcessId()) & 0xF;
    unsigned threadId1char = ((unsigned) (memsize_t) GetCurrentThreadId()) & 0xF;
    transactionSeed.setf("%02X%06X%X%X", ip, mstick6char, processId2char, threadId1char);
}

bool CESPServerLoggingAgent::getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    if (!hasService(LGSTGetTransactionSeed))
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "%s: no getTransactionSeed service configured", agentName.get());

    StringBuffer statusMessage, transactionSeed;
    int statusCode = getTransactionSeed(req.getApplication(), transactionSeed, statusMessage);
    resp.setStatusCode(statusCode);
    resp.setSeedId(transactionSeed.str());
    if (statusMessage.length())
        resp.setStatusMessage(statusMessage.str());
    return (statusCode != -2);
}

int CESPServerLoggingAgent::getTransactionSeed(const char* appName, StringBuffer& transactionSeed, StringBuffer& statusMessage)
{
    StringBuffer soapreq(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
        " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\">"
        " <soap:Body>");
    if (!appName || !*appName)
        soapreq.append("<GetTransactionSeedRequest/>");
    else
        soapreq.append("<GetTransactionSeedRequest><Application>").append(appName).append("</Application></GetTransactionSeedRequest>");
    soapreq.append("</soap:Body></soap:Envelope>");

    unsigned retry = 1;
    int statusCode = 0;
    while (1)
    {
        try
        {
            if (!getTransactionSeed(soapreq, statusCode, statusMessage, transactionSeed))
                throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed,"Failed to get TransactionSeed");
            break;
        }
        catch (IException* e)
        {
            StringBuffer errorStr;
            statusMessage.set("Failed to get TransactionSeed: error code ").append(e->errorCode()).append(", error message ").append(e->errorMessage(errorStr));
            OERRLOG("%s -- try %d", statusMessage.str(), retry);
            e->Release();
            if (retry >= maxGTSRetries)
            {
                statusCode = -2;
                break;
            }
            Sleep(retry*3000);
            retry++;
        }
    }

    return statusCode;
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

    //statusCode: 0 = success, -1 = failed after retries
    statusCode = pTree->getPropInt("soap:Body/GetTransactionSeedResponse/StatusCode");
    statusMessage.set(pTree->queryProp("soap:Body/GetTransactionSeedResponse/StatusMessage"));
    seedID.set(pTree->queryProp("soap:Body/GetTransactionSeedResponse/SeedId"));

    if (statusCode || !seedID.length())
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to get Transaction Seed from %s", serverUrl.str());
    return true;
}

void CESPServerLoggingAgent::resetTransSeed(CTransIDBuilder *builder, const char* groupName)
{
    StringBuffer transactionSeed, statusMessage;
    if (builder->isLocalSeed())
        createLocalTransactionSeed(transactionSeed);
    else
    {
        int statusCode = getTransactionSeed(groupName, transactionSeed, statusMessage);
        if (!transactionSeed.length() || (statusCode != 0))
        {
            StringBuffer msg = "Failed to get Transaction Seed for ";
            msg.append(groupName).append(". statusCode: ").append(statusCode);
            if (!statusMessage.isEmpty())
                msg.append(", ").append(statusMessage.str());
            throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "%s", msg.str());
        }
    }

    builder->resetTransSeed(transactionSeed.str(), builder->isLocalSeed() ? alternativeTransactionSeedType.get() : transactionSeedType.get());
};

void CESPServerLoggingAgent::getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID)
{
    if (!hasService(LGSTGetTransactionID))
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "%s: no getTransactionID service configured", agentName.get());

    const char* groupName = nullptr;
    CTransIDBuilder* transIDBuilder = nullptr;
    StringAttr* source = nullptr;
    if (transFields)
        source = transFields->getValue(sTransactionMethod);
    if (source)
    {
        CLogSource* logSource = logSources.getValue(source->get());
        if (logSource)
        {
            groupName = logSource->getGroupName();
            transIDBuilder = transIDMap.getValue(groupName);
        }
    }
    if (!transIDBuilder && (defaultGroup.length() != 0))
    {
        groupName = defaultGroup.str();
        transIDBuilder = transIDMap.getValue(groupName);
    }
    if (!transIDBuilder)
    {
        groupName = appESPServerLoggingAgent;
        transIDBuilder = transIDMap.getValue(appESPServerLoggingAgent);
    }
    if (!transIDBuilder) //This should not happen.
        throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "Failed to getTransactionID");

    if (!transIDBuilder->checkMaxSequenceNumber() || !transIDBuilder->checkTimeout())
        resetTransSeed(transIDBuilder, groupName);

    transIDBuilder->getTransID(transFields, transactionID);
    if (!transIDBuilder->checkMaxLength(transactionID.length()))
    {
        resetTransSeed(transIDBuilder, groupName);
        transIDBuilder->getTransID(transFields, transactionID);
    }
    return;
}

bool CESPServerLoggingAgent::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp)
{
    try
    {
        if (!hasService(LGSTUpdateLOG))
            throw MakeStringException(EspLoggingErrors::UpdateLogFailed, "%s: no updateLog service configured", agentName.get());

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
        IERRLOG("%s", errorMessage.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorMessage.str());
        e->Release();
    }
    return true;
}

IEspUpdateLogRequestWrap* CESPServerLoggingAgent::filterLogContent(IEspUpdateLogRequestWrap* req)
{
    return logContentFilter.filterLogContent(req);
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
