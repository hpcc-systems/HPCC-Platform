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

class CESPServerLoggingAgent : public CInterface, implements IEspLogAgent
{
    StringBuffer serviceName, loggingAgentName, defaultGroup;
    StringBuffer serverUrl, serverUserID, serverPassword;
    unsigned maxServerWaitingSeconds; //time out value for HTTP connection to logging server
    unsigned maxGTSRetries;
    CLogContentFilter logContentFilter;
    MapStringToMyClass<CTransIDBuilder> transIDMap;
    MapStringToMyClass<CLogSource> logSources;
    StringAttr transactionSeedType;
    StringAttr alternativeTransactionSeedType;

    bool sendHTTPRequest(StringBuffer& req, StringBuffer& resp, StringBuffer& status);
    int getTransactionSeed(const char* source, StringBuffer& transactionSeed, StringBuffer& statusMessage);
    bool getTransactionSeed(StringBuffer& soapreq, int& statusCode, StringBuffer& statusMessage, StringBuffer& seedID);
    void resetTransSeed(CTransIDBuilder *builder, const char* groupName);

    virtual void createLocalTransactionSeed(StringBuffer& transactionSeed);

public:
    IMPLEMENT_IINTERFACE;

    CESPServerLoggingAgent() {};
    virtual ~CESPServerLoggingAgent() {};

    bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);

    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual void getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual IEspUpdateLogRequestWrap* filterLogContent(IEspUpdateLogRequestWrap* req);
};

#endif //__ESPSERVERLOGGINGAGENT__HPP__
