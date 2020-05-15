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

            ILogRequestReader* logRequestReader = logThread->getLogRequestReader();
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
    LogAgentAction action;
    action.type = CLogAgentActions_GetSettings;

    CLogAgentActionResults results;
    WSDecoupledLogGetSettings act(action, results);
    act.doAction(context, logGroups, req.getGroups());
    resp.setSettings(results.queryGroupSettings());

    return true;
}

bool CWSDecoupledLogEx::onPauseLog(IEspContext& context, IEspPauseLogRequest& req, IEspPauseLogResponse& resp)
{
    LogAgentAction action;
    action.type = req.getPause() ? CLogAgentActions_Pause : CLogAgentActions_Resume;

    CLogAgentActionResults results;
    WSDecoupledLogPause act(action, results);
    act.doAction(context, logGroups, req.getGroups());

    resp.setStatuses(results.queryGroupStatus());

    return true;
}

bool CWSDecoupledLogEx::onGetAckedLogFiles(IEspContext& context, IEspGetAckedLogFilesRequest& req, IEspGetAckedLogFilesResponse& resp)
{
    LogAgentAction action;
    action.type = CLogAgentActions_GetAckedLogFileNames;

    CLogAgentActionResults results;
    WSDecoupledLogGetAckedLogFileNames act(action, results);
    act.doAction(context, logGroups, req.getGroups());

    resp.setAckedLogFilesInGroups(results.queryTankFilesInGroup());

    return true;
}

bool CWSDecoupledLogEx::onCleanAckedFiles(IEspContext& context, IEspCleanAckedFilesRequest& req, IEspCleanAckedFilesResponse& resp)
{
    try
    {
        const char* groupName = req.getGroupName();
        if (isEmptyString(groupName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Group name not specified.");
        auto match = logGroups.find(groupName);
        if (match == logGroups.end())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Group %s not found.", groupName);

        LogAgentAction action;
        action.type = CLogAgentActions_CleanAckedLogFiles;
        action.fileNames = &req.getFileNames();
        if (!action.fileNames->length())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        CLogAgentActionResults results;
        WSDecoupledLogCleanAckedLogFiles act(action, results);
        act.doActionInGroup(match->second, nullptr);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void WSDecoupledLogAction::doAction(IEspContext& context, std::map<std::string, Owned<WSDecoupledLogAgentGroup>>& allGroups,
    IArrayOf<IConstLogAgentGroup>& groupsReq)
{
    try
    {
        if (groupsReq.ordinality())
        {
            checkGroupInput(allGroups, groupsReq);
            ForEachItemIn(i, groupsReq)
            {
                IConstLogAgentGroup& g = groupsReq.item(i);
                auto match = allGroups.find(g.getGroupName());
                StringArray& agentNames = g.getAgentNames();
                doActionInGroup(match->second, &agentNames);
            }
        }
        else
        {
            for (auto ml : allGroups)
            {
                doActionInGroup(ml.second, nullptr);
            }
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
}

void WSDecoupledLogAction::checkGroupInput(std::map<std::string, Owned<WSDecoupledLogAgentGroup>>& allGroups,
    IArrayOf<IConstLogAgentGroup>& groupsReq)
{
    ForEachItemIn(i, groupsReq)
    {
        IConstLogAgentGroup& g = groupsReq.item(i);
        const char* gName = g.getGroupName();
        if (isEmptyString(gName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Group name not specified.");

        auto match = allGroups.find(gName);
        if (match == allGroups.end())
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Group %s not found.", gName);

        StringArray& agentNames = g.getAgentNames();
        ForEachItemIn(j, agentNames)
        {
            const char* agentName = agentNames.item(j);
            if (isEmptyString(agentName))
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "%s: logging agent name not specified.", gName);

            if (!match->second->getLoggingAgentThread(agentName))
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "%s: logging agent %s not found.", gName, agentName);
        }
    }
}

void WSDecoupledLogAction::doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames)
{
    if (!agentNames || !agentNames->ordinality())
    {
        std::map<std::string, Owned<IUpdateLogThread>>& agentThreadMap = group->getLoggingAgentThreads();
        for (auto mt : agentThreadMap)
        {
            if (!doActionForAgent(mt.first.c_str(), mt.second))
                break;
        }
    }
    else
    {
        ForEachItemIn(j, *agentNames)
        {
            const char* agentName = agentNames->item(j);
            if (!doActionForAgent(agentName, group->getLoggingAgentThread(agentName)))
                break;
        }
    }
}

void WSDecoupledLogGetSettings::doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames)
{
    groupSetting.setown(createLogAgentGroupSetting());
    groupSetting->setGroupName(group->getName());
    const char* tankFileDir = group->getTankFileDir();
    const char* tankFileMask = group->getTankFileMask();
    groupSetting->setTankFileDir(tankFileDir);
    if (!isEmptyString(tankFileMask))
        groupSetting->setTankFileMask(tankFileMask);

    WSDecoupledLogAction::doActionInGroup(group, agentNames);

    results.appendGroupSetting(groupSetting.getClear());
}

bool WSDecoupledLogGetSettings::doActionForAgent(const char* agentName, IUpdateLogThread* agentThread)
{
    Owned<IEspLogAgentSetting> agentSetting = createLogAgentSetting();
    agentSetting->setAgentName(agentName);

    CLogRequestReaderSettings* settings = agentThread->getLogRequestReader()->getSettings();
    if (!settings)
        agentSetting->setAgentStatus("SettingsNotFound");
    else
    {
        agentSetting->setAgentStatus("SettingsFound");
        agentSetting->setAckedFileList(settings->ackedFileList);
        agentSetting->setAckedLogRequestFile(settings->ackedLogRequestFile);
        agentSetting->setWaitSeconds(settings->waitSeconds);
        agentSetting->setPendingLogBufferSize(settings->pendingLogBufferSize);
    }

    IArrayOf<IConstLogAgentSetting>& agentSettings = groupSetting->getAgentSettings();
    agentSettings.append(*agentSetting.getClear());
    return true;
}

void WSDecoupledLogPause::doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames)
{
    groupStatus.setown(createLogAgentGroupStatus());
    groupStatus->setGroupName(group->getName());

    WSDecoupledLogAction::doActionInGroup(group, agentNames);

    results.appendGroupStatus(groupStatus.getClear());
}

bool WSDecoupledLogPause::doActionForAgent(const char* agentName, IUpdateLogThread* agentThread)
{
    agentThread->getLogRequestReader()->setPause((action.type == CLogAgentActions_Pause) ? true : false);

    IArrayOf<IConstLogAgentStatus>& agentStatusInGroup = groupStatus->getAgentStatuses();
    Owned<IEspLogAgentStatus> aStatus = createLogAgentStatus();
    aStatus->setAgentName(agentName);
    if (action.type == CLogAgentActions_Pause)
        aStatus->setStatus("Pausing");
    else
        aStatus->setStatus("Resuming");
    agentStatusInGroup.append(*aStatus.getClear());
    return true;
}

void WSDecoupledLogGetAckedLogFileNames::doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames)
{
    tankFilesInGroup.setown(createLogAgentGroupTankFiles());
    tankFilesInGroup->setGroupName(group->getName());
    tankFilesInGroup->setTankFileDir(group->getTankFileDir());

    WSDecoupledLogAction::doActionInGroup(group, agentNames);

    results.appendGroupTankFiles(tankFilesInGroup.getClear());
}

bool WSDecoupledLogGetAckedLogFileNames::doActionForAgent(const char* agentName, IUpdateLogThread* agentThread)
{
    StringArray& ackedFiles = tankFilesInGroup->getTankFileNames();
    //The ackedFiles stores the tank files which have been acked for all logging agents
    //in an agent group. At the beginning, it is empty. The reportAckedLogFiles() will
    //be called for the 1st logging agent. The ackedFiles is filled with the acked tank
    //files in the 1st logging agent. If the ackedFiles is still empty, this method
    //returns false and the outside loop for other logging agents in the group will be
    //stopped. If the ackedFiles is not empty, the outside loop calls this method for
    //the rest of logging agents in the group. For those logging agents, the
    //removeUnknownAckedLogFiles() will be called because ackedFiles.length() != 0.
    //In the removeUnknownAckedLogFiles(), if any file inside the ackedFiles has not
    //been acked in that agent, the file should be removed from the ackedFiles. After
    //the removeUnknownAckedLogFiles() call, if the ackedFiles is empty, the outside
    //loop for the rest of logging agents in the group will be stopped.
    if (!ackedFiles.length())
        agentThread->getLogRequestReader()->reportAckedLogFiles(ackedFiles);
    else
        agentThread->getLogRequestReader()->removeUnknownAckedLogFiles(ackedFiles);
    return !ackedFiles.empty();
}

bool WSDecoupledLogCleanAckedLogFiles::doActionForAgent(const char* agentName, IUpdateLogThread* agentThread)
{
    agentThread->getLogRequestReader()->cleanAckedLogFiles(*action.fileNames);
    return true;
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
