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

#pragma warning (disable : 4786)

#ifndef _LOGGINGAGENT_HPP__
#define _LOGGINGAGENT_HPP__

#include "jiface.hpp"
#include "esp.hpp"
#include "datafieldmap.hpp"
#include "ws_loggingservice_esp.ipp"
#include "loggingcommon.hpp"
#include "LoggingErrors.hpp"

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

#define UPDATELOGTHREADWAITINGTIME 3000

//The 'TransactionXYZ' is expected for the following key strings
//used in CTransIDBuilder::getTransID() -> add().
static const char* sTransactionDateTime = "TransactionDateTime";
static const char* sTransactionMethod = "TransactionMethod";
static const char* sTransactionIdentifier = "TransactionIdentifier";

class CTransIDBuilder : public CInterface, implements IInterface
{
    StringAttr seed, seedType;
    bool localSeed;
    unsigned __int64 seq = 0;

    unsigned maxLength = 0;
    unsigned maxSeq = 0;
    unsigned seedExpiredSeconds = 0;
    time_t createTime;

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
    CTransIDBuilder(const char* _seed, bool _localSeed, const char* _seedType, unsigned _maxLength, unsigned _maxSeq, unsigned _seedExpiredSeconds)
        : seed(_seed), localSeed(_localSeed), seedType(_seedType), maxLength(_maxLength), maxSeq(_maxSeq), seedExpiredSeconds(_seedExpiredSeconds)
    {
        CDateTime now;
        now.setNow();
        createTime = now.getSimple();
    };
    virtual ~CTransIDBuilder() {};

    bool checkMaxSequenceNumber() { return (maxSeq == 0) || (seq < maxSeq); };
    bool checkMaxLength(unsigned length) { return (maxLength == 0) || (length <= maxLength); };
    bool checkTimeout()
    {
        if (seedExpiredSeconds ==0)
            return true;

        CDateTime now;
        now.setNow();
        return now.getSimple() < createTime + seedExpiredSeconds;
    };
    bool isLocalSeed() { return localSeed; };
    void resetTransSeed(const char* newSeed, const char* newSeedType)
    {
        if (isEmptyString(newSeed))
            throw MakeStringException(EspLoggingErrors::GetTransactionSeedFailed, "TransactionSeed cannot be empty.");
        seed.set(newSeed);
        seedType.set(newSeedType);
        seq = 0;

        CDateTime now;
        now.setNow();
        createTime = now.getSimple();
    };

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
        id.append(seed.get());
        if (seedType.length())
            id.append(seedType.get());
        id.append(++seq);
    };
};

interface IEspUpdateLogRequestWrap : extends IInterface
{
    virtual const char* getGUID()=0;
    virtual const char* getOption()=0;
    virtual const char* getUpdateLogRequest()=0;
    virtual IPropertyTree* getESPContext()=0;
    virtual IPropertyTree* getUserContext()=0;
    virtual IPropertyTree* getUserRequest()=0;
    virtual IPropertyTree* getLogRequestTree()=0;
    virtual IInterface* getExtraLog()=0;
    virtual const char* getBackEndResponse()=0;
    virtual const char* getUserResponse()=0;
    virtual const char* getLogDatasets()=0;
    virtual const bool getNoResend()=0;
    virtual void setGUID(const char* val)=0;
    virtual void setOption(const char* val)=0;
    virtual void setUpdateLogRequest(const char* val)=0;
    virtual void setESPContext(IPropertyTree* val)=0;
    virtual void setUserContext(IPropertyTree* val)=0;
    virtual void setUserRequest(IPropertyTree* val)=0;
    virtual void setLogRequestTree(IPropertyTree* val)=0;
    virtual void setExtraLog(IInterface* val)=0;
    virtual void setBackEndResponse(const char* val)=0;
    virtual void setUserResponse(const char* val)=0;
    virtual void setLogDatasets(const char* val)=0;
    virtual unsigned incrementRetryCount() = 0;
    virtual void setNoResend(bool val)=0;
    virtual void clearOriginalContent() = 0;
};

class CUpdateLogRequestWrap : implements IEspUpdateLogRequestWrap, public CInterface
{
    StringAttr  GUID;
    StringAttr  option;
    StringAttr  updateLogRequest;
    Owned<IPropertyTree> espContext;
    Owned<IPropertyTree> userContext;
    Owned<IPropertyTree> userRequest;
    Owned<IPropertyTree> logRequestTree;
    Owned<IInterface> extraLog;
    StringAttr  backEndResponse;
    StringAttr  userResponse;
    StringAttr  logDatasets;
    unsigned    retryCount;
    bool        noResend = false;;

public:
    IMPLEMENT_IINTERFACE;

    CUpdateLogRequestWrap(const char* _GUID, const char* _option, const char* _updateLogRequest)
        : GUID(_GUID), option(_option), updateLogRequest(_updateLogRequest), retryCount(0) {};
    CUpdateLogRequestWrap(const char* _GUID, const char* _option, IPropertyTree* _espContext,
        IPropertyTree*_userContext, IPropertyTree*_userRequest, const char *_backEndResponse, const char *_userResponse,
        const char *_logDatasets)
        : GUID(_GUID), option(_option), backEndResponse(_backEndResponse), userResponse(_userResponse),
        logDatasets(_logDatasets), retryCount(0)
    {
        userContext.setown(_userContext);
        espContext.setown(_espContext);
        userRequest.setown(_userRequest);
    };
    CUpdateLogRequestWrap(const char* _GUID, const char* _option, IPropertyTree* _logInfo,
        IInterface* _extraLog) : GUID(_GUID), option(_option), retryCount(0)
    {
        logRequestTree.setown(_logInfo);
        extraLog.setown(_extraLog);
    };

    void clearOriginalContent()
    {
        espContext.clear();
        userRequest.clear();
        userContext.clear();
        userResponse.clear();
        logDatasets.clear();
        backEndResponse.clear();
        updateLogRequest.clear();
        logRequestTree.clear();
        extraLog.clear();
    };

    const char* getGUID() {return GUID.get();};
    const char* getOption() {return option.get();};
    const char* getUpdateLogRequest() {return updateLogRequest.get();};
    IPropertyTree* getESPContext() {return espContext.getLink();};
    IPropertyTree* getUserContext() {return userContext.getLink();};
    IPropertyTree* getUserRequest() {return userRequest.getLink();};
    IPropertyTree* getLogRequestTree() {return logRequestTree.getLink();};
    IInterface* getExtraLog() {return extraLog.getLink();};
    const char* getBackEndResponse() {return backEndResponse.get();};
    const char* getUserResponse() {return userResponse.get();};
    const char* getLogDatasets() {return logDatasets.get();};
    const bool getNoResend() {return noResend;};
    void setGUID(const char* val) {GUID.set(val);};
    void setOption(const char* val) {option.set(val);};
    void setUpdateLogRequest(const char* val) {updateLogRequest.set(val);};
    void setESPContext(IPropertyTree* val) {espContext.setown(val);};
    void setUserContext(IPropertyTree* val) {userContext.setown(val);};
    void setUserRequest(IPropertyTree* val) {userRequest.setown(val);};
    void setLogRequestTree(IPropertyTree* val) {logRequestTree.setown(val);};
    void setExtraLog(IInterface* val) {extraLog.setown(val);};
    void setBackEndResponse(const char* val) {backEndResponse.set(val);};
    void setUserResponse(const char* val) {userResponse.set(val);};
    void setLogDatasets(const char* val) {logDatasets.set(val);};
    unsigned incrementRetryCount() { retryCount++; return retryCount;};
    void setNoResend(bool val) { noResend = val; };
};

interface IEspLogAgent : extends IInterface
{
    virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process) = 0;
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp) = 0;
    virtual void getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID) = 0;
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp) = 0;
    virtual IEspUpdateLogRequestWrap* filterLogContent(IEspUpdateLogRequestWrap* req) = 0;
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

class LOGGINGCOMMON_API CLogContentFilter : public CInterface
{
    bool            logBackEndResp;
    StringArray     logContentFilters;
    CIArrayOf<CESPLogContentGroupFilters> groupFilters;

    bool readLogFilters(IPropertyTree* cfg, unsigned groupID);
    void filterLogContentTree(StringArray& filters, IPropertyTree* originalContentTree, IPropertyTree* newLogContentTree, bool& logContentEmpty);
    void filterAndAddLogContentBranch(StringArray& branchNamesInFilter, unsigned idx, StringArray& branchNamesInLogContent,
        IPropertyTree* in, IPropertyTree* updateLogRequestTree, bool& logContentEmpty);
    void addLogContentBranch(StringArray& branchNames, IPropertyTree* contentToLogBranch, IPropertyTree* updateLogRequestTree);
public:
    IMPLEMENT_IINTERFACE;

    CLogContentFilter() {};

    void readAllLogFilters(IPropertyTree* cfg);
    IEspUpdateLogRequestWrap* filterLogContent(IEspUpdateLogRequestWrap* req);
};

class LOGGINGCOMMON_API CDBLogAgentBase : public CInterface, implements IEspLogAgent
{
protected:
    StringBuffer defaultDB, transactionTable, loggingTransactionSeed;
    StringAttr defaultLogGroup, defaultTransactionApp, loggingTransactionApp, logSourcePath;

    unsigned logSourceCount, loggingTransactionCount, maxTriesGTS;
    MapStringToMyClass<CLogGroup> logGroups;
    MapStringToMyClass<CLogSource> logSources;

    void readDBCfg(IPropertyTree* cfg, StringBuffer& server, StringBuffer& dbUser, StringBuffer& dbPassword);
    void readTransactionCfg(IPropertyTree* cfg);
    bool buildUpdateLogStatement(IPropertyTree* logRequest, const char* logDB, CLogTable& table, StringBuffer& logID, StringBuffer& cqlStatement);
    void appendFieldInfo(const char* field, StringBuffer& value, StringBuffer& fields, StringBuffer& values, bool quoted);
    void addMissingFields(CIArrayOf<CLogField>& logFields, BoolHash& HandledFields, StringBuffer& fields, StringBuffer& values);
    CLogGroup* checkLogSource(IPropertyTree* logRequest, StringBuffer& source, StringBuffer& logDB);
    void getLoggingTransactionID(StringBuffer& id);

    virtual void addField(CLogField& logField, const char* name, StringBuffer& value, StringBuffer& fields, StringBuffer& values) = 0;
    virtual void queryTransactionSeed(const char* appName, StringBuffer& seed) = 0;
    virtual void executeUpdateLogStatement(StringBuffer& statement) = 0;
    virtual void setUpdateLogStatement(const char* dbName, const char* tableName,
        const char* fields, const char* values, StringBuffer& statement) = 0;
public:
    IMPLEMENT_IINTERFACE;

    CDBLogAgentBase() {};
    virtual ~CDBLogAgentBase() {};

    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual void getTransactionID(StringAttrMapping* transFields, StringBuffer& transactionID);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual IEspUpdateLogRequestWrap* filterLogContent(IEspUpdateLogRequestWrap* req);
};
#endif  //_LOGGINGAGENT_HPP__
