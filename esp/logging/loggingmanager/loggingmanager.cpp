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

#include "LoggingErrors.hpp"
#include "loggingcommon.hpp"
#include "loggingmanager.hpp"

CLoggingManager::~CLoggingManager(void)
{
    for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
    {
        loggingAgentThreads[x]->stop();
        loggingAgentThreads[x]->Release();
    }

    loggingAgentThreads.clear();
}

//Called when Logging manager is created. Create logging agents based on settings
bool CLoggingManager::init(IPropertyTree* cfg, const char* service)
{
    if (!cfg)
    {
        ERRLOG(EspLoggingErrors::ConfigurationFileEntryError, "Logging Manager setting not found for %s", service);
        return false;
    }

    Owned<IPTreeIterator> loggingAgentSettings = cfg->getElements("LogAgent");
    ForEach(*loggingAgentSettings)
    {
        IPropertyTree& loggingAgentTree = loggingAgentSettings->query();
        const char* agentName = loggingAgentTree.queryProp("@name");
        const char* agentType = loggingAgentTree.queryProp("@type");
        const char* agentPlugin = loggingAgentTree.queryProp("@plugin");
        if (!agentName || !*agentName || !agentPlugin || !*agentPlugin)
            continue;

        IEspLogAgent* loggingAgent = loadLoggingAgent(agentName, agentPlugin, service, cfg);
        if (!loggingAgent)
        {
            ERRLOG(-1, "Failed to create logging agent for %s", agentName);
            continue;
        }
        loggingAgent->init(agentName, agentType, &loggingAgentTree, service);
        IUpdateLogThread* logThread = createUpdateLogThread(&loggingAgentTree, service, agentName, loggingAgent);
        if(!logThread)
            throw MakeStringException(-1, "Failed to create update log thread for %s", agentName);
        loggingAgentThreads.push_back(logThread);
    }

    return !loggingAgentThreads.empty();
}

typedef IEspLogAgent* (*newLoggingAgent_t_)();

IEspLogAgent* CLoggingManager::loadLoggingAgent(const char* name, const char* dll, const char* service, IPropertyTree* cfg)
{
    StringBuffer plugin;
    plugin.append(SharedObjectPrefix).append(dll).append(SharedObjectExtension);
    HINSTANCE loggingAgentLib = LoadSharedObject(plugin.str(), true, false);
    if(!loggingAgentLib)
        throw MakeStringException(EspLoggingErrors::LoadLoggingLibraryError, "can't load library %s", plugin.str());

    newLoggingAgent_t_ xproc = (newLoggingAgent_t_)GetSharedProcedure(loggingAgentLib, "newLoggingAgent");
    if (!xproc)
        throw MakeStringException(EspLoggingErrors::LoadLoggingLibraryError, "procedure newLoggingAgent of %s can't be loaded", plugin.str());

    return (IEspLogAgent*) xproc();
}

bool CLoggingManager::updateLog(const char* option, const char* logContent, StringBuffer& status)
{
    bool bRet = false;
    try
    {
        Owned<IEspUpdateLogRequestWrap> req =  new CUpdateLogRequestWrap(NULL, option, logContent);
        Owned<IEspUpdateLogResponse> resp =  createUpdateLogResponse();
        bRet = updateLog(*req, *resp, status);
    }
    catch (IException* e)
    {
        e->errorMessage(status);
        status.insert(0, "Failed to update log: ");
        ERRLOG("%s", status.str());
        e->Release();
    }

    return bRet;
}

bool CLoggingManager::updateLog(const char* option, IEspContext& espContext, IPropertyTree* userContext, IPropertyTree* userRequest,
        const char* backEndResp, const char* userResp, StringBuffer& status)
{
    bool bRet = false;
    try
    {
        short port;
        StringBuffer sourceIP;
        espContext.getServAddress(sourceIP, port);
        Owned<IPropertyTree> espContextTree = createPTree("ESPContext");
        espContextTree->addProp("SourceIP", sourceIP.str());
        const char* userId = espContext.queryUserId();
        if (userId && *userId)
            espContextTree->addProp("UserName", userId);

        Owned<IEspUpdateLogRequestWrap> req =  new CUpdateLogRequestWrap(NULL, option, espContextTree.getClear(), LINK(userContext), LINK(userRequest),
            backEndResp, userResp);
        Owned<IEspUpdateLogResponse> resp =  createUpdateLogResponse();
        bRet = updateLog(*req, *resp, status);
    }
    catch (IException* e)
    {
        e->errorMessage(status);
        status.insert(0, "Failed to update log: ");
        ERRLOG("%s", status.str());
        e->Release();
    }
    return bRet;
}

bool CLoggingManager::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp, StringBuffer& status)
{
    bool bRet = updateLog(req, resp);
    if (bRet)
        status.set("Log request has been sent.");
    else
    {
        const char* statusMsg = resp.getStatusMessage();
        if (statusMsg && *statusMsg)
            status.setf("Failed to update log: %s", statusMsg);
        else
            status.set("Failed to update log");
    }
    return bRet;
}

bool CLoggingManager::updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp)
{
    bool bRet = false;
    try
    {
        for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
        {
            IUpdateLogThread* loggingThread = loggingAgentThreads[x];
            if (loggingThread->hasService(LGSTUpdateLOG))
            {
                loggingThread->queueLog(&req);
                bRet = true;
            }
        }
    }
    catch (IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
        ERRLOG("Failed to update log: %s",errorStr.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorStr.str());
        e->Release();
    }
    return bRet;
}

bool CLoggingManager::getTransactionSeed(StringBuffer& transactionSeed, StringBuffer& status)
{
    bool bRet = false;
    try
    {
        Owned<IEspGetTransactionSeedRequest> req =  createGetTransactionSeedRequest();
        Owned<IEspGetTransactionSeedResponse> resp =  createGetTransactionSeedResponse();
        transactionSeed.set("Seed");

        bRet = getTransactionSeed(*req, *resp);
        if (bRet && !resp->getStatusCode())
        {
            const char* seed = resp->getSeedId();
            if (!seed || !*seed)
                status.set("Failed to get Transaction Seed");
            else
            {
                transactionSeed.set(seed);
                status.set("Transaction Seed returned.");
                bRet = true;
            }
        }
        else
        {
            const char* statusMsg = resp->getStatusMessage();
            if (statusMsg && *statusMsg)
                status.setf("Failed to get Transaction Seed: %s", statusMsg);
            else
                status.set("Failed to get Transaction Seed");
        }
    }
    catch (IException* e)
    {
        e->errorMessage(status);
        status.insert(0, "Failed to get Transaction Seed: ");
        ERRLOG("%s",status.str());
        e->Release();
    }

    return bRet;
}


bool CLoggingManager::getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    bool bRet = false;
    try
    {
        for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
        {
            IUpdateLogThread* loggingThread = loggingAgentThreads[x];
            if (!loggingThread->hasService(LGSTGetTransactionSeed))
                continue;

            IEspLogAgent* loggingAgent = loggingThread->getLogAgent();
            bRet = loggingAgent->getTransactionSeed(req, resp);
            if (bRet)
                break;
        }
    }
    catch (IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
        ERRLOG("Failed to get Transaction Seed: %s",errorStr.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorStr.str());
        e->Release();
    }

    return bRet;
}

extern "C"
{
LOGGINGMANAGER_API ILoggingManager* newLoggingManager()
{
    return new CLoggingManager();
}
}
