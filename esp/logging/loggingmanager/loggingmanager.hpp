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

#ifdef WIN32
    #ifdef LOGGINGMANAGER_EXPORTS
        #define LOGGINGMANAGER_API __declspec(dllexport)
    #else
        #define LOGGINGMANAGER_API __declspec(dllimport)
    #endif
#else
    #define LOGGINGMANAGER_API
#endif


class CLoggingManager : public CInterface, implements ILoggingManager
{
    typedef std::vector<IUpdateLogThread*> LOGGING_AGENTTHREADS;
    LOGGING_AGENTTHREADS  loggingAgentThreads;

    IEspLogAgent* loadLoggingAgent(const char* name, const char* dll, const char* type, IPropertyTree* cfg);
    bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp, StringBuffer& status);

public:
    IMPLEMENT_IINTERFACE;

    CLoggingManager(void) {};
    virtual ~CLoggingManager(void);

    virtual bool init(IPropertyTree* cfg, const char* service);

    virtual bool updateLog(const char* option, IEspContext& espContext, IPropertyTree* userContext, IPropertyTree* userRequest,
        const char* backEndResp, const char* userResp, StringBuffer& status);
    virtual bool updateLog(const char* option, const char* logContent, StringBuffer& status);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual bool getTransactionSeed(StringBuffer& transactionSeed, StringBuffer& status);
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
};

#endif // !defined(__LOGGINGMANAGER_HPP__)
