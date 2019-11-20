/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.

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

#include "ws_decoupledloggingservice.hpp"

#include "jlib.hpp"

#include "LoggingErrors.hpp"
#include "loggingcommon.hpp"
#include "loggingagentbase.hpp"
#include "exception_util.hpp"

typedef IEspLogAgent* (*newLoggingAgent_t_)();

IEspLogAgent* CWSDecoupledLogEx::loadLoggingAgent(const char* name, const char* dll, const char* service, IPropertyTree* cfg)
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

void CWSDecoupledLogEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    if (!cfg)
        throw MakeStringException(-1, "Can't initialize CWSDecoupledLogEx, cfg is NULL");

    espProcess.set(process);

    StringBuffer xpath;
    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    IPropertyTree *serviceCFG = cfg->queryPropTree(xpath.str());
    Owned<IPTreeIterator> agentGroupSettings = serviceCFG->getElements("LoggingAgentGroup");
    ForEach(*agentGroupSettings)
    {
        IPropertyTree& agentGroupTree = agentGroupSettings->query();
        const char* groupName = agentGroupTree.queryProp("@name");
        if (isEmptyString(groupName))
            continue;

        const char* tankFileDir = agentGroupTree.queryProp("FailSafeLogsDir");
        if (isEmptyString(tankFileDir))
            throw MakeStringException(-1, "Can't initialize CWSDecoupledLogEx, FailSafeLogsDir is NULL for LoggingAgentGroup %s", groupName);

        Owned<WSDecoupledLogAgentGroup> group = new WSDecoupledLogAgentGroup(groupName, tankFileDir, agentGroupTree.queryProp("FailSafeLogsMask"));
        Owned<IPTreeIterator> loggingAgentSettings = agentGroupTree.getElements("LogAgent");
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
                OERRLOG(-1, "Failed to create logging agent for %s", agentName);
                continue;
            }
            loggingAgent->init(agentName, agentType, &loggingAgentTree, service);
            loggingAgent->initVariants(&loggingAgentTree);
            Owned<IUpdateLogThread> logThread = createUpdateLogThread(&loggingAgentTree, service, agentName, tankFileDir, loggingAgent);
            if(!logThread)
                throw MakeStringException(-1, "Failed to create update log thread for %s", agentName);

            CLogRequestReader* logRequestReader = logThread->getLogRequestReader();
            if (!logRequestReader)
                throw MakeStringException(-1, "CLogRequestReader not found for %s.", agentName);

            if (group->getLoggingAgentThread(agentName))
                throw MakeStringException(-1, "%s: >1 logging agents are named as %s.", groupName, agentName);
            group->addLoggingAgentThread(agentName, logThread);
        }
        logGroups.insert({groupName, group});
    }
}

bool CWSDecoupledLogEx::onGetLogAgentSetting(IEspContext& context, IEspGetLogAgentSettingRequest& req, IEspGetLogAgentSettingResponse& resp)
{
    try
    {
        IArrayOf<IEspLogAgentGroupSetting> groupSettingResp;
        IArrayOf<IConstLogAgentGroup>& groups = req.getGroups();
        if (!groups.ordinality())
        {
            for (auto ml : logGroups)
                getSettingsForLoggingAgentsInGroup(ml.second, nullptr, groupSettingResp);
        }
        else
        {
            ForEachItemIn(i, groups)
            {
                IConstLogAgentGroup& g = groups.item(i);
                const char* gName = g.getGroupName();
                if (isEmptyString(gName))
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Group name not specified.");

                auto match = logGroups.find(gName);
                if (match != logGroups.end())
                {
                    StringArray& agentNames = g.getAgentNames();
                    getSettingsForLoggingAgentsInGroup(match->second, &agentNames, groupSettingResp);
                }
                else
                {
                    Owned<IEspLogAgentGroupSetting> groupSetting = createLogAgentGroupSetting();
                    groupSetting->setGroupName(gName);
                    groupSetting->setGroupStatus("NotFound");
                    groupSettingResp.append(*groupSetting.getClear());
                }
            }
        }
        resp.setSettings(groupSettingResp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDecoupledLogEx::onPauseLog(IEspContext& context, IEspPauseLogRequest& req, IEspPauseLogResponse& resp)
{
    try
    {
        IArrayOf<IEspLogAgentGroupStatus> groupStatusResp;
        bool pause = req.getPause();
        IArrayOf<IConstLogAgentGroup>& groups = req.getGroups();
        if (!groups.ordinality())
        {
            for (auto ml : logGroups)
                pauseLoggingAgentsInGroup(ml.second, nullptr, pause, groupStatusResp);
        }
        else
        {
            ForEachItemIn(i, groups)
            {
                IConstLogAgentGroup& g = groups.item(i);
                const char* gName = g.getGroupName();
                if (isEmptyString(gName))
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Group name not specified.");

                auto match = logGroups.find(gName);
                if (match != logGroups.end())
                {
                    StringArray& agentNames = g.getAgentNames();
                    pauseLoggingAgentsInGroup(match->second, &agentNames, pause, groupStatusResp);
                }
                else
                {
                    Owned<IEspLogAgentGroupStatus> groupStatus = createLogAgentGroupStatus();
                    groupStatus->setGroupName(gName);
                    groupStatus->setGroupStatus("NotFound");
                    groupStatusResp.append(*groupStatus.getClear());
                }
            }
        }
        resp.setStatuses(groupStatusResp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWSDecoupledLogEx::getSettingsForLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames,
    IArrayOf<IEspLogAgentGroupSetting>& groupSettingResp)
{
    IArrayOf<IEspLogAgentSetting> agentSettingResp;
    if (!agentNames || !agentNames->ordinality())
        getSettingsForAllLoggingAgentsInGroup(group, agentSettingResp);
    else
    {
        ForEachItemIn(j, *agentNames)
        {
            const char* agentName = agentNames->item(j);
            if (isEmptyString(agentName))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: logging agent name not specified.", group->getName());

            getLoggingAgentSettings(agentName, group->getLoggingAgentThread(agentName), agentSettingResp);
        }
    }

    Owned<IEspLogAgentGroupSetting> groupSetting = createLogAgentGroupSetting();
    groupSetting->setGroupName(group->getName());
    groupSetting->setGroupStatus("Found");
    const char* tankFileDir = group->getTankFileDir();
    const char* tankFileMask = group->getTankFileMask();
    groupSetting->setTankFileDir(tankFileDir);
    if (!isEmptyString(tankFileMask))
        groupSetting->setTankFileMask(tankFileMask);
    groupSetting->setAgentSettings(agentSettingResp);
    groupSettingResp.append(*groupSetting.getClear());
}

void CWSDecoupledLogEx::pauseLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames, bool pause,
    IArrayOf<IEspLogAgentGroupStatus>& groupStatusResp)
{
    IArrayOf<IEspLogAgentStatus> agentStatusResp;
    if (!agentNames || !agentNames->ordinality())
        pauseAllLoggingAgentsInGroup(group, pause, agentStatusResp);
    else
    {
        ForEachItemIn(j, *agentNames)
        {
            const char* agentName = agentNames->item(j);
            if (isEmptyString(agentName))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: logging agent name not specified.", group->getName());

            pauseLoggingAgent(agentName, group->getLoggingAgentThread(agentName), pause, agentStatusResp);
        }
    }

    Owned<IEspLogAgentGroupStatus> groupStatus = createLogAgentGroupStatus();
    groupStatus->setGroupName(group->getName());
    groupStatus->setGroupStatus("Found");
    groupStatus->setAgentStatuses(agentStatusResp);
    groupStatusResp.append(*groupStatus.getClear());
}

void CWSDecoupledLogEx::pauseAllLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, bool pause, IArrayOf<IEspLogAgentStatus>& agentStatusResp)
{
    std::map<std::string, Owned<IUpdateLogThread>>&  agentThreadMap = group->getLoggingAgentThreads();
    for (auto mt : agentThreadMap)
        pauseLoggingAgent(mt.first.c_str(), mt.second, pause, agentStatusResp);
}

void CWSDecoupledLogEx::pauseLoggingAgent(const char* agentName, IUpdateLogThread* agentThread, bool pause, IArrayOf<IEspLogAgentStatus>& agentStatusResp)
{
    Owned<IEspLogAgentStatus> agentStatus = createLogAgentStatus();
    agentStatus->setAgentName(agentName);

    if (!agentThread)
        agentStatus->setStatus("NotFound");
    else
    {
        agentThread->getLogRequestReader()->setPause(pause);
        agentStatus->setStatus(pause ? "Pausing" : "Resuming");
    }
    agentStatusResp.append(*agentStatus.getClear());
}

void CWSDecoupledLogEx::getSettingsForAllLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, IArrayOf<IEspLogAgentSetting>& agentSettingResp)
{
    std::map<std::string, Owned<IUpdateLogThread>>&  agentThreadMap = group->getLoggingAgentThreads();
    for (auto mt : agentThreadMap)
        getLoggingAgentSettings(mt.first.c_str(), mt.second, agentSettingResp);
}

void CWSDecoupledLogEx::getLoggingAgentSettings(const char* agentName, IUpdateLogThread* agentThread, IArrayOf<IEspLogAgentSetting>& agentSettingResp)
{
    Owned<IEspLogAgentSetting> agentSetting = createLogAgentSetting();
    agentSetting->setAgentName(agentName);

    if (!agentThread)
        agentSetting->setAgentStatus("NotFound");
    else
    {
        CLogRequestReaderSettings* settings = agentThread->getLogRequestReader()->getSettings();
        if (!settings)
            agentSetting->setAgentStatus("SettingsNotFound");
        else
        {
            agentSetting->setAgentStatus("Found");
            agentSetting->setAckedFileList(settings->ackedFileList);
            agentSetting->setAckedLogRequestFile(settings->ackedLogRequestFile);
            agentSetting->setWaitSeconds(settings->waitSeconds);
            agentSetting->setPendingLogBufferSize(settings->pendingLogBufferSize);
        }
    }
    agentSettingResp.append(*agentSetting.getClear());
}

IUpdateLogThread* WSDecoupledLogAgentGroup::getLoggingAgentThread(const char* name)
{
    if (isEmptyString(name))
        return nullptr;

    auto match = loggingAgentThreads.find(name);
    if (match == loggingAgentThreads.end())
        return nullptr;
    return match->second;
}
