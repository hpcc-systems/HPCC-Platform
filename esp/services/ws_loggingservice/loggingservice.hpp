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

#ifndef _WSLOGGINGSERVICE_HPP__
#define _WSLOGGINGSERVICE_HPP__

#include <vector>
#include "loggingcommon.hpp"
#include "logthread.hpp"
#include "loggingagentbase.hpp"

class CWsLoggingServiceEx : public CWsLoggingService
{
    typedef std::vector<IUpdateLogThread*> LOGGING_AGENTTHREADS;
    LOGGING_AGENTTHREADS  loggingAgentThreads;

    unsigned loggingServiceThreadPoolSize;
    unsigned loggingServiceThreadWaitingTimeOut;
    unsigned loggingServiceThreadPoolStackSize;
    Owned<IThreadPool> loggingServiceThreadPool;

    IEspLogAgent* loadLoggingAgent(const char* name, const char* dll);

public:
    IMPLEMENT_IINTERFACE;

    CWsLoggingServiceEx() {};
    virtual ~CWsLoggingServiceEx()
    {
        for (unsigned int x = 0; x < loggingAgentThreads.size(); x++)
        {
            loggingAgentThreads[x]->stop();
            loggingAgentThreads[x]->Release();
        }

        loggingAgentThreads.clear();
    };

    virtual bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);

    //interface IEspLoggingService
    bool onGetTransactionSeed(IEspContext& context, IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    bool onUpdateLog(IEspContext& context, IEspUpdateLogRequest& req, IEspUpdateLogResponse& resp);
};

#endif //_WSLOGGINGSERVICE_HPP__
