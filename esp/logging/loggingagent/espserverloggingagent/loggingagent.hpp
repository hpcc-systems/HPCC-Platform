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
#include "datafieldmap.hpp"

#ifdef ESPSERVERLOGGINGAGENT_EXPORTS
    #define ESPSERVERLOGGINGAGENT_API DECL_EXPORT
#else
    #define ESPSERVERLOGGINGAGENT_API DECL_IMPORT
#endif

enum ESPLogContentGroup
{
    ESPLCGESPContext = 0,
    ESPLCGUserContext = 1,
    ESPLCGUserReq = 2,
    ESPLCGUserResp = 3,
    ESPLCGLogDatasets = 4,
    ESPLCGBackEndResp = 5,
    ESPLCGAll = 6
};

static const char * const espLogContentGroupNames[] = { "ESPContext", "UserContext", "UserRequest", "UserResponse",
    "LogDatasets", "BackEndResponse", "", NULL };

class CTransIDBuilder : public CInterface, implements IInterface
{
    StringAttr seed;
    bool localSeed;
    unsigned __int64 seq;
    void add(StringAttrMapping* transIDFields, const char* key, StringBuffer& id)
    {
        StringAttr* value = transIDFields->getValue(key);
        if (value)
            id.append(value->get()).append('-');
        else
        {
            const char* ptr = key;
            if (strlen(key) > 11) //skip the "transaction" prefix of the key
                ptr += 11;
            id.append('?').append(ptr).append('-');
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    CTransIDBuilder(const char* _seed, bool _localSeed) : seed(_seed), localSeed(_localSeed), seq(0) { };
    virtual ~CTransIDBuilder() {};

    virtual const char* getTransSeed() { return seed.get(); };
    virtual void getTransID(StringAttrMapping* transIDFields, StringBuffer& id)
    {
        id.clear();

        if (transIDFields)
        {
            add(transIDFields, sTransactionDateTime, id);
            add(transIDFields, sTransactionMethod, id);
            add(transIDFields, sTransactionIdentifier, id);
        }
        if (localSeed)
            id.append(seed.get()).append("-X").append(++seq);
        else
            id.append(seed.get()).append('-').append(++seq);
    };
};

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
    StringBuffer serviceName, loggingAgentName, defaultGroup;
    StringBuffer serverUrl, serverUserID, serverPassword;
    unsigned maxServerWaitingSeconds; //time out value for HTTP connection to logging server
    unsigned maxGTSRetries;
    StringArray     logContentFilters;
    CIArrayOf<CESPLogContentGroupFilters> groupFilters;
    bool logBackEndResp;
    MapStringToMyClass<CTransIDBuilder> transIDMap;
    MapStringToMyClass<CLogSource> logSources;

    void readAllLogFilters(IPropertyTree* cfg);
    bool readLogFilters(IPropertyTree* cfg, unsigned groupID);
    void filterLogContentTree(StringArray& filters, IPropertyTree* originalContentTree, IPropertyTree* newLogContentTree, bool& logContentEmpty);
    void filterAndAddLogContentBranch(StringArray& branchNamesInFilter, unsigned idx, StringArray& branchNamesInLogContent,
        IPropertyTree* in, IPropertyTree* updateLogRequestTree, bool& logContentEmpty);
    void addLogContentBranch(StringArray& branchNames, IPropertyTree* contentToLogBranch, IPropertyTree* updateLogRequestTree);
    bool sendHTTPRequest(StringBuffer& req, StringBuffer& resp, StringBuffer& status);
    int getTransactionSeed(const char* source, StringBuffer& transactionSeed, StringBuffer& statusMessage);
    bool getTransactionSeed(StringBuffer& soapreq, int& statusCode, StringBuffer& statusMessage, StringBuffer& seedID);

    virtual void createLocalTransactionSeed(StringBuffer& transactionSeed);

public:
    IMPLEMENT_IINTERFACE;

    CESPServerLoggingAgent() {};
    virtual ~CESPServerLoggingAgent() {};

    bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);

    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual void getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual void filterLogContent(IEspUpdateLogRequestWrap* req);
};

#endif //__ESPSERVERLOGGINGAGENT__HPP__
