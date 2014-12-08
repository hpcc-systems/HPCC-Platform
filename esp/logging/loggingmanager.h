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

#if !defined(__LOGGINGMANAGER_H__)
#define __LOGGINGMANAGER_H__

#include "jiface.hpp"
#include "esp.hpp"
#include "ws_loggingservice_esp.ipp"

#define LOGGINGMANAGERLIB "loggingmanager"
#define LOGGINGDBSINGLEINSERT "SingleInsert"

interface ILoggingManager : implements IInterface
{
    virtual bool init(IPropertyTree* loggingConfig, const char* service) = 0;
    virtual bool updateLog(const char* option, const char* logContent, StringBuffer& status) = 0;
    virtual bool updateLog(IEspUpdateLogRequest& req, IEspUpdateLogResponse& resp) = 0;
    virtual bool getTransactionSeed(StringBuffer& transactionSeed, StringBuffer& status) = 0;
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp) = 0;
};

typedef ILoggingManager* (*newLoggingManager_t_)();

#endif // !defined(__LOGGINGMANAGER_H__)
