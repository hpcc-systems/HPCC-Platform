/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

#if !defined(__LOGGINGMANAGER_H__)
#define __LOGGINGMANAGER_H__

#include "jiface.hpp"
#include "esp.hpp"
#include "loggingagentbase.hpp"
#include "pluginloader.hpp"

#define LOGGINGMANAGERLIB "loggingmanager"
#define LOGGINGDBSINGLEINSERT "SingleInsert"

interface IEspLogEntry  : implements IInterface
{
    virtual void setOwnEspContext(IEspContext* ctx)  = 0;
    virtual void setOwnUserContextTree(IPropertyTree* tree) = 0;
    virtual void setOwnUserRequestTree(IPropertyTree* tree) = 0;
    virtual void setOwnLogInfoTree(IPropertyTree* tree) = 0;
    virtual void setOwnScriptValuesTree(IPropertyTree* extra) = 0;
    virtual void setOwnExtraLog(IInterface* extra) = 0;
    virtual void setOption(const char* ptr) = 0;
    virtual void setLogContent(const char* ptr) = 0;
    virtual void setBackEndReq(const char* ptr) = 0;
    virtual void setBackEndResp(const char* ptr) = 0;
    virtual void setUserResp(const char* ptr) = 0;
    virtual void setLogDatasets(const char* ptr) = 0;

    virtual IEspContext* getEspContext() = 0;
    virtual IPropertyTree* getUserContextTree() = 0;
    virtual IPropertyTree* getUserRequestTree() = 0;
    virtual IPropertyTree* getLogInfoTree() = 0;
    virtual IPropertyTree* getScriptValuesTree() = 0;
    virtual IInterface* getExtraLog() = 0;
    virtual const char* getOption() = 0;
    virtual const char* getLogContent() = 0;
    virtual const char* getBackEndReq() = 0;
    virtual const char* getBackEndResp() = 0;
    virtual const char* getUserResp() = 0;
    virtual const char* getLogDatasets() = 0;
};

interface ILoggingManager : implements IInterface
{
    virtual bool init(IPropertyTree* loggingConfig, const char* service) = 0;
    virtual IEspLogEntry* createLogEntry() = 0;
    virtual bool hasService(LOGServiceType service) const = 0;
    virtual bool updateLog(IEspLogEntry* entry, StringBuffer& status) = 0;
    virtual bool updateLog(IEspContext* espContext, IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp) = 0;
    virtual bool getTransactionSeed(StringBuffer& transactionSeed, StringBuffer& status) = 0;
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp) = 0;
    virtual bool getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID, StringBuffer& status) = 0;
};

typedef ILoggingManager* (*newLoggingManager_t_)();

/**
 * CLogManagerLoader is a simple wrapper of a single instantation of TPluginLoader
 * for the creation of ILoggingManager instances.
 */
class CLoggingManagerLoader
{
    TPluginLoader<newLoggingManager_t_> m_loader;
public:
    CLoggingManagerLoader(const char* libraryDefault, const char* entryPointDefault, const char* libraryXPath, const char* entryPointXPath)
        : m_loader(isEmptyString(libraryDefault) ? LOGGINGMANAGERLIB : libraryDefault, isEmptyString(entryPointDefault) ? "newLoggingManager" : entryPointDefault, libraryXPath, entryPointXPath)
    {
    }

    ILoggingManager* create(const IPTree& configuration)
    {
        return m_loader.create<ILoggingManager>(configuration, [&](newLoggingManager_t_ entryPoint) { return entryPoint(); });
    }
};

#endif // !defined(__LOGGINGMANAGER_H__)
