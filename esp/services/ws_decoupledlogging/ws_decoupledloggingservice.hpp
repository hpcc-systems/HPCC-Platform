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

#ifndef _ESPWIZ_ws_decoupledlogging_HPP__
#define _ESPWIZ_ws_decoupledlogging_HPP__

#include "ws_decoupledlogging_esp.ipp"
#include "espp.hpp"
#include "environment.hpp"
#include "logthread.hpp"

struct LogAgentAction
{
    CLogAgentActions type;
    StringArray* fileNames = nullptr;
};

class CLogAgentActionResults : public CSimpleInterfaceOf<IInterface>
{
    IArrayOf<IEspLogAgentGroupStatus> groupStatus;
    IArrayOf<IEspLogAgentGroupSetting> groupSettings;
    IArrayOf<IEspLogAgentGroupTankFiles> tankFilesInGroups;

public:
    CLogAgentActionResults() {};

    IArrayOf<IEspLogAgentGroupStatus>& queryGroupStatus() { return groupStatus; }
    void appendGroupStatus(IEspLogAgentGroupStatus* status) { groupStatus.append(*status); }
    IArrayOf<IEspLogAgentGroupSetting>& queryGroupSettings() { return groupSettings; }
    void appendGroupSetting(IEspLogAgentGroupSetting* setting) { groupSettings.append(*setting); }
    IArrayOf<IEspLogAgentGroupTankFiles>& queryTankFilesInGroup() { return tankFilesInGroups; }
    void appendGroupTankFiles(IEspLogAgentGroupTankFiles* groupTankFiles) { tankFilesInGroups.append(*groupTankFiles); }
};

class CWSDecoupledLogSoapBindingEx : public CWSDecoupledLogSoapBinding
{
public:

    CWSDecoupledLogSoapBindingEx(http_soap_log_level level=hsl_none) : CWSDecoupledLogSoapBinding(level) { };

    CWSDecoupledLogSoapBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level=hsl_none)
        : CWSDecoupledLogSoapBinding(cfg, bindname, procname, level) { };
};

class WSDecoupledLogAgentGroup : public CSimpleInterfaceOf<IInterface>
{
    StringAttr name;
    StringAttr tankFileDir, tankFileMask;
    std::map<std::string, Owned<IUpdateLogThread>> loggingAgentThreads;

public:
    WSDecoupledLogAgentGroup(const char* _name, const char* _tankFileDir, const char* _tankFileMask)
        : name(_name), tankFileDir(_tankFileDir), tankFileMask(_tankFileMask) {}

    const char* getName() { return name.get(); }
    const char* getTankFileDir() { return tankFileDir.get(); }
    const char* getTankFileMask() { return tankFileMask.get(); }

    std::map<std::string, Owned<IUpdateLogThread>>& getLoggingAgentThreads() { return loggingAgentThreads; }
    void addLoggingAgentThread(const char* name, IUpdateLogThread* thread) { loggingAgentThreads.insert({name, thread}); }
    IUpdateLogThread* getLoggingAgentThread(const char* name);
};

class WSDecoupledLogAction : public CSimpleInterfaceOf<IInterface>
{
    void checkGroupInput(std::map<std::string, Owned<WSDecoupledLogAgentGroup>>& allGroups, IArrayOf<IConstLogAgentGroup>& groupsReq);

protected:
    LogAgentAction& action;
    CLogAgentActionResults& results;

public:
    WSDecoupledLogAction(LogAgentAction& _action, CLogAgentActionResults& _results)
        : action(_action), results(_results) {}

    void doAction(IEspContext& context, std::map<std::string, Owned<WSDecoupledLogAgentGroup>>& _allGroups,
        IArrayOf<IConstLogAgentGroup>& _groups);
    virtual bool doActionForAgent(const char* agentName, IUpdateLogThread* agentThread) = 0;
    virtual void doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames);
};

class WSDecoupledLogGetSettings : public WSDecoupledLogAction
{
    Owned<IEspLogAgentGroupSetting> groupSetting;

public:
    WSDecoupledLogGetSettings(LogAgentAction& _action, CLogAgentActionResults& _results)
        : WSDecoupledLogAction(_action, _results) {}

    virtual bool doActionForAgent(const char* agentName, IUpdateLogThread* agentThread);
    virtual void doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames);
};

class WSDecoupledLogPause : public WSDecoupledLogAction
{
    Owned<IEspLogAgentGroupStatus> groupStatus;

public:
    WSDecoupledLogPause(LogAgentAction& _action, CLogAgentActionResults& _results)
        : WSDecoupledLogAction(_action, _results) {}

    virtual bool doActionForAgent(const char* agentName, IUpdateLogThread* agentThread);
    virtual void doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames);
};

class WSDecoupledLogGetAckedLogFileNames : public WSDecoupledLogAction
{
    Owned<IEspLogAgentGroupTankFiles> tankFilesInGroup;

public:
    WSDecoupledLogGetAckedLogFileNames(LogAgentAction& _action, CLogAgentActionResults& _results)
        : WSDecoupledLogAction(_action, _results) {}

    virtual bool doActionForAgent(const char* agentName, IUpdateLogThread* agentThread);
    virtual void doActionInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames);
};

class WSDecoupledLogCleanAckedLogFiles : public WSDecoupledLogAction
{
public:
    WSDecoupledLogCleanAckedLogFiles(LogAgentAction& _action, CLogAgentActionResults& _results)
        : WSDecoupledLogAction(_action, _results) {}

    virtual bool doActionForAgent(const char* agentName, IUpdateLogThread* agentThread);
};

class CWSDecoupledLogEx : public CWSDecoupledLog
{
    StringAttr espProcess;
    IEspContainer* container;
    std::map<std::string, Owned<WSDecoupledLogAgentGroup>> logGroups;

    IEspLogAgent* loadLoggingAgent(const char* name, const char* dll, const char* service, IPropertyTree* cfg);

public:
    IMPLEMENT_IINTERFACE;

    virtual void setContainer(IEspContainer* _container)
    {
        container = _container;
    }

    virtual void init(IPropertyTree* cfg, const char* process, const char* service);
    virtual bool onGetLogAgentSetting(IEspContext& context, IEspGetLogAgentSettingRequest& req, IEspGetLogAgentSettingResponse& resp);
    virtual bool onPauseLog(IEspContext& context, IEspPauseLogRequest& req, IEspPauseLogResponse& resp);
    virtual bool onGetAckedLogFiles(IEspContext& context, IEspGetAckedLogFilesRequest& req, IEspGetAckedLogFilesResponse& resp);
    virtual bool onCleanAckedFiles(IEspContext& context, IEspCleanAckedFilesRequest& req, IEspCleanAckedFilesResponse& resp);
};

#endif //_ESPWIZ_ws_decoupledlogging_HPP__
