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

class CWSDecoupledLogEx : public CWSDecoupledLog
{
    StringAttr espProcess;
    IEspContainer* container;
    std::map<std::string, Owned<WSDecoupledLogAgentGroup>> logGroups;

    IEspLogAgent* loadLoggingAgent(const char* name, const char* dll, const char* service, IPropertyTree* cfg);
    void pauseLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames, bool pause,
        IArrayOf<IEspLogAgentGroupStatus>& groupStatusResp);
    void pauseAllLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, bool pause, IArrayOf<IEspLogAgentStatus>& agentStatusResp);
    void getSettingsForLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, StringArray* agentNames,
        IArrayOf<IEspLogAgentGroupSetting>& groupSettingResp);
    void getSettingsForAllLoggingAgentsInGroup(WSDecoupledLogAgentGroup* group, IArrayOf<IEspLogAgentSetting>& agentSettingResp);
    void pauseLoggingAgent(const char* agentName, IUpdateLogThread* agentThread, bool pause, IArrayOf<IEspLogAgentStatus>& agentStatusResp);
    void getLoggingAgentSettings(const char* agentName, IUpdateLogThread* agentThread, IArrayOf<IEspLogAgentSetting>& agentSettingResp);

public:
    IMPLEMENT_IINTERFACE;

    virtual void setContainer(IEspContainer* _container)
    {
        container = _container;
    }

    virtual void init(IPropertyTree* cfg, const char* process, const char* service);
    virtual bool onGetLogAgentSetting(IEspContext& context, IEspGetLogAgentSettingRequest& req, IEspGetLogAgentSettingResponse& resp);
    virtual bool onPauseLog(IEspContext& context, IEspPauseLogRequest& req, IEspPauseLogResponse& resp);
};

#endif //_ESPWIZ_ws_decoupledlogging_HPP__
