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

#if !defined(__LOGGINGMANAGER_HPP__)
#define __LOGGINGMANAGER_HPP__

#include <vector>

#include "loggingcommon.hpp"
#include "loggingagentbase.hpp"
#include "logthread.hpp"
#include "loggingmanager.h"
#include "LogFailSafe.hpp"

#ifdef LOGGINGMANAGER_EXPORTS
    #define LOGGINGMANAGER_API DECL_EXPORT
#else
    #define LOGGINGMANAGER_API DECL_IMPORT
#endif

class CEspLogEntry : implements IEspLogEntry, public CInterface
{
    Owned<IEspContext> espContext;
    StringAttr option, logContent, backEndReq, backEndResp, userResp, logDatasets;
    Owned<IPropertyTree> userContextTree;
    Owned<IPropertyTree> userRequestTree;
    Owned<IPropertyTree> logInfoTree;
    Owned<IPropertyTree> scriptValuesTree;
    Owned<IInterface> extraLog;

public:
    IMPLEMENT_IINTERFACE;

    CEspLogEntry(void) { };

    void setOwnEspContext(IEspContext* ctx) { espContext.setown(ctx); };
    void setOwnUserContextTree(IPropertyTree* tree) { userContextTree.setown(tree); };
    void setOwnUserRequestTree(IPropertyTree* tree) { userRequestTree.setown(tree); };
    void setOwnLogInfoTree(IPropertyTree* tree) { logInfoTree.setown(tree); };
    void setOwnScriptValuesTree(IPropertyTree* tree) override { scriptValuesTree.setown(tree); };
    void setOwnExtraLog(IInterface* extra) { extraLog.setown(extra); };
    void setOption(const char* ptr) { option.set(ptr); };
    void setLogContent(const char* ptr) { logContent.set(ptr); };
    void setBackEndReq(const char* ptr) { backEndReq.set(ptr); };
    void setBackEndResp(const char* ptr) { backEndResp.set(ptr); };
    void setUserResp(const char* ptr) { userResp.set(ptr); };
    void setLogDatasets(const char* ptr) { logDatasets.set(ptr); };

    IEspContext* getEspContext() { return espContext; };
    IPropertyTree* getUserContextTree() { return userContextTree; };
    IPropertyTree* getUserRequestTree() { return userRequestTree; };
    IPropertyTree* getLogInfoTree() { return logInfoTree; };
    IPropertyTree* getScriptValuesTree() override { return scriptValuesTree; };
    IInterface* getExtraLog() { return extraLog; };
    const char* getOption() { return option.get(); };
    const char* getLogContent() { return logContent.get(); };
    const char* getBackEndReq() { return backEndReq.get(); };
    const char* getBackEndResp() { return backEndResp.get(); };
    const char* getUserResp() { return userResp.get(); };
    const char* getLogDatasets() { return logDatasets.get(); };
};


class CLoggingManager : implements ILoggingManager, public CInterface
{
    typedef std::vector<IUpdateLogThread*> LOGGING_AGENTTHREADS;
    LOGGING_AGENTTHREADS  loggingAgentThreads;
    bool oneTankFile = false, decoupledLogging = false, initialized = false;
    Owned<ILogFailSafe> logFailSafe;
    CLogContentFilter logContentFilter;
    unsigned serviceMask = 0;

    inline void setServiceMaskService(LOGServiceType service) { serviceMask |= (1 << service); }
    IEspLogAgent* loadLoggingAgent(const char* name, const char* dll, const char* type, IPropertyTree* cfg);
    bool updateLogImpl(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    bool saveToTankFile(IEspUpdateLogRequestWrap& req, CLogRequestInFile* reqInFile);
    unsigned serializeLogRequestContent(IEspUpdateLogRequestWrap* request, const char* GUID, StringBuffer& logData);

    bool updateLog(IEspContext* espContext, IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp, StringBuffer& status);
    bool updateLog(IEspContext* espContext, const char* option, IPropertyTree* userContext, IPropertyTree* userRequest, IPropertyTree *scriptValues,
        const char* backEndReq, const char* backEndResp, const char* userResp, const char* logDatasets, StringBuffer& status);
    bool updateLog(IEspContext* espContext, const char* option, const char* logContent, StringBuffer& status);
    bool updateLog(IEspContext* espContext, const char* option, IPropertyTree* logInfo, IInterface* extraLog, StringBuffer& status);

public:
    IMPLEMENT_IINTERFACE;

    CLoggingManager(void) { initialized = false; };
    virtual ~CLoggingManager(void);

    virtual bool init(IPropertyTree* cfg, const char* service);

    virtual IEspLogEntry* createLogEntry();
    bool hasService(LOGServiceType service) const override;
    virtual bool updateLog(IEspContext* espContext, IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual bool updateLog(IEspLogEntry* entry, StringBuffer& status);
    virtual bool getTransactionSeed(StringBuffer& transactionSeed, StringBuffer& status);
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual bool getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID, StringBuffer& status);
};

#endif // !defined(__LOGGINGMANAGER_HPP__)
