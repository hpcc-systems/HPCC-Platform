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

#pragma warning (disable : 4786)

#include "LoggingErrors.hpp"
#include "logthread.hpp"
#include "loggingservice.hpp"

#define WSLOGGING_ACCESS    "WsLoggingAccess"

typedef IEspLogAgent* (*newLogAgent_t_)();

bool CWsLoggingServiceEx::init(const char* service, const char* type, IPropertyTree* cfg, const char* process)
{
    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> pServiceNode = cfg->getPropTree(xpath.str());
    if (!pServiceNode)
        throw MakeStringException(-1, "No settings found for service %s", service);

    Owned<IPropertyTreeIterator> logAgents = pServiceNode->getElements("LogAgent");
    if (!logAgents)
        throw MakeStringException(-1, "No logAgent is defined for service %s", service);

    ForEach(*logAgents)
    {
        IPropertyTree& ptree = logAgents->query();
        const char* agentName = ptree.queryProp("@name");
        const char* agentType = ptree.queryProp("@type");
        const char* agentPlugin = ptree.queryProp("@plugin");
        if (!agentName || !*agentName || !agentPlugin || !*agentPlugin)
            continue;

        IEspLogAgent* logAgent = loadLoggingAgent(agentName, agentPlugin);
        if (!logAgent)
        {
            OERRLOG(-1, "Failed to create logging agent for %s", agentName);
            continue;
        }
        logAgent->init(agentName, agentType, &ptree, process);
        IUpdateLogThread* logThread = createUpdateLogThread(&ptree, service, agentName, logAgent);
        if(!logThread)
            throw MakeStringException(-1, "Failed to create update log thread for %s", agentName);

        loggingAgentThreads.push_back(logThread);
    }

    return true;
}

IEspLogAgent* CWsLoggingServiceEx::loadLoggingAgent(const char* name, const char* dll)
{
    StringBuffer realName;
    // add suffix and prefix if needed
    realName.append(SharedObjectPrefix).append(dll).append(SharedObjectExtension);
    HINSTANCE loggingAgentLib = LoadSharedObject(realName.str(), true, false);
    if(!loggingAgentLib)
        throw MakeStringException(EspLoggingErrors::LoadLoggingLibraryError, "can't load library %s", realName.str());

    newLogAgent_t_ xproc = (newLogAgent_t_)GetSharedProcedure(loggingAgentLib, "newLoggingAgent");
    if (!xproc)
        throw MakeStringException(EspLoggingErrors::LoadLoggingLibraryError, "procedure newLoggingAgent of %s can't be loaded", realName.str());

    return (IEspLogAgent*) xproc();
}

bool CWsLoggingServiceEx::onUpdateLog(IEspContext& context, IEspUpdateLogRequest& req, IEspUpdateLogResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(WSLOGGING_ACCESS, SecAccess_Write, EspLoggingErrors::WSLoggingAccessDenied, "WsLoggingService::UpdateLog: Permission denied.");

        context.addTraceSummaryTimeStamp(LogMin, "startQLog");
        for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
        {
            IUpdateLogThread* loggingThread = loggingAgentThreads[x];
            if (!loggingThread->hasService(LGSTUpdateLOG))
                continue;
            loggingThread->queueLog(&req);
        }
        context.addTraceSummaryTimeStamp(LogMin, "endQLog");
        resp.setStatusCode(0);
        resp.setStatusMessage("Log will be updated.");
    }
    catch (IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
        OERRLOG("Failed to update log: cannot add to log queue: %s",errorStr.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorStr.str());
        e->Release();
    }
    return true;
}

bool CWsLoggingServiceEx::onGetTransactionSeed(IEspContext& context, IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp)
{
    bool bRet = false;
    try
    {
        context.ensureFeatureAccess(WSLOGGING_ACCESS, SecAccess_Read, EspLoggingErrors::WSLoggingAccessDenied, "WsLoggingService::GetTransactionSeed: Permission denied.");

        LOGServiceType serviceType = LGSTGetTransactionSeed;
        for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
        {
            IUpdateLogThread* loggingThread = loggingAgentThreads[x];
            if (!loggingThread->hasService(serviceType))
                continue;

            IEspLogAgent* loggingAgent = loggingThread->getLogAgent();
            bRet = loggingAgent->getTransactionSeed(req, resp);
            break;
        }
    }
    catch (IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
        errorStr.insert(0, "Failed to get Transaction Seed: ");
        OERRLOG("%s", errorStr.str());
        resp.setStatusCode(-1);
        resp.setStatusMessage(errorStr.str());
        e->Release();
    }
    return bRet;
}
