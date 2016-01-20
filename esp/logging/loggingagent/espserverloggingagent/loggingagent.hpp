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

#ifndef __ESPSERVERLOGGINGAGENT__HPP__
#define __ESPSERVERLOGGINGAGENT__HPP__

#include "loggingcommon.hpp"

#ifdef WIN32
    #ifdef ESPSERVERLOGGINGAGENT_EXPORTS
        #define ESPSERVERLOGGINGAGENT_API __declspec(dllexport)
    #else
        #define ESPSERVERLOGGINGAGENT_API __declspec(dllimport)
    #endif
#else
    #define ESPSERVERLOGGINGAGENT_API
#endif

enum ESPLogContentGroup
{
    ESPLCGESPContext = 0,
    ESPLCGUserContext = 1,
    ESPLCGUserReq = 2,
    ESPLCGUserResp = 3,
    ESPLCGBackEndResp = 4,
    ESPLCGAll = 5
};

static const char * const espLogContentGroupNames[] = { "ESPContext", "UserContext", "UserRequest", "UserResponse", "BackEndResponse", "", NULL };

class CESPLogContentGroupFilters : public CInterface, implements IInterface
{
    ESPLogContentGroup group;
    StringArray filters;

public:
    IMPLEMENT_IINTERFACE;

    CESPLogContentGroupFilters(ESPLogContentGroup _group) : group(_group) {};
    ESPLogContentGroup getGroup() { return group; };
    StringArray& getFilters() { return filters; };
    void clearFilters() { filters.clear(); };
    unsigned getFilterCount() { return filters.length(); };
    void addFilter(const char* filter)
    {
        if (filter && *filter)
            filters.append(filter);
    };
};

class CESPServerLoggingAgent : public CInterface, implements IEspLogAgent
{
    StringBuffer serviceName, loggingAgentName;
    StringBuffer serverUrl, serverUserID, serverPassword;
    unsigned maxServerWaitingSeconds; //time out value for HTTP connection to logging server
    unsigned maxGTSRetries;
    StringArray     logContentFilters;
    IArrayOf<CESPLogContentGroupFilters> groupFilters;
    bool logBackEndResp;

    void readAllLogFilters(IPropertyTree* cfg);
    bool readLogFilters(IPropertyTree* cfg, unsigned groupID);
    void filterLogContentTree(StringArray& filters, IPropertyTree* originalContentTree, IPropertyTree* newLogContentTree, bool& logContentEmpty);
    void filterAndAddLogContentBranch(StringArray& branchNamesInFilter, unsigned idx, StringArray& branchNamesInLogContent,
        IPropertyTree* in, IPropertyTree* updateLogRequestTree, bool& logContentEmpty);
    void addLogContentBranch(StringArray& branchNames, IPropertyTree* contentToLogBranch, IPropertyTree* updateLogRequestTree);
    bool sendHTTPRequest(StringBuffer& req, StringBuffer& resp, StringBuffer& status);
    bool getTransactionSeed(StringBuffer& soapreq, int& statusCode, StringBuffer& statusMessage, StringBuffer& seedID);

public:
    IMPLEMENT_IINTERFACE;

    CESPServerLoggingAgent() {};
    virtual ~CESPServerLoggingAgent() {};

    bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);

    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual void filterLogContent(IEspUpdateLogRequestWrap* req);
};

#endif //__ESPSERVERLOGGINGAGENT__HPP__
