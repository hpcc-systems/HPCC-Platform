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

#pragma warning (disable : 4786)

#ifndef _LOGGINGAGENT_HPP__
#define _LOGGINGAGENT_HPP__

#include "jiface.hpp"
#include "esp.hpp"
#include "ws_loggingservice_esp.ipp"

#define UPDATELOGTHREADWAITINGTIME 3000

interface IEspUpdateLogRequestWrap : extends IInterface
{
    virtual const char* getGUID()=0;
    virtual const char* getOption()=0;
    virtual const char* getUpdateLogRequest()=0;
    virtual IPropertyTree* getESPContext()=0;
    virtual IPropertyTree* getUserContext()=0;
    virtual IPropertyTree* getUserRequest()=0;
    virtual const char* getBackEndResponse()=0;
    virtual const char* getUserResponse()=0;
    virtual void setGUID(const char* val)=0;
    virtual void setOption(const char* val)=0;
    virtual void setUpdateLogRequest(const char* val)=0;
    virtual void setESPContext(IPropertyTree* val)=0;
    virtual void setUserContext(IPropertyTree* val)=0;
    virtual void setUserRequest(IPropertyTree* val)=0;
    virtual void setBackEndResponse(const char* val)=0;
    virtual void setUserResponse(const char* val)=0;
    virtual unsigned incrementRetryCount() = 0;
    virtual void clearOriginalContent() = 0;
};

class CUpdateLogRequestWrap : public CInterface, implements IEspUpdateLogRequestWrap
{
    StringAttr  GUID;
    StringAttr  option;
    StringAttr  updateLogRequest;
    Owned<IPropertyTree> espContext;
    Owned<IPropertyTree> userContext;
    Owned<IPropertyTree> userRequest;
    StringAttr  backEndResponse;
    StringAttr  userResponse;
    unsigned    retryCount;

public:
    IMPLEMENT_IINTERFACE;

    CUpdateLogRequestWrap(const char* _GUID, const char* _option, const char* _updateLogRequest)
        : GUID(_GUID), option(_option), updateLogRequest(_updateLogRequest), retryCount(0) {};
    CUpdateLogRequestWrap(const char* _GUID, const char* _option, IPropertyTree* _espContext,
        IPropertyTree*_userContext, IPropertyTree*_userRequest, const char *_backEndResponse, const char *_userResponse)
        : GUID(_GUID), option(_option), backEndResponse(_backEndResponse), userResponse(_userResponse), retryCount(0)
    {
        userContext.setown(_userContext);
        espContext.setown(_espContext);
        userRequest.setown(_userRequest);
    };
    ~CUpdateLogRequestWrap()
    {
        espContext.clear();
        userRequest.clear();
        userContext.clear();
    };
    void clearOriginalContent()
    {
        espContext.clear();
        userRequest.clear();
        userContext.clear();
        userResponse.clear();
        backEndResponse.clear();
        updateLogRequest.clear();
    };

    const char* getGUID() {return GUID.get();};
    const char* getOption() {return option.get();};
    const char* getUpdateLogRequest() {return updateLogRequest.get();};
    IPropertyTree* getESPContext() {return espContext.getLink();};
    IPropertyTree* getUserContext() {return userContext.getLink();};
    IPropertyTree* getUserRequest() {return userRequest.getLink();};
    const char* getBackEndResponse() {return backEndResponse.get();};
    const char* getUserResponse() {return userResponse.get();};
    void setGUID(const char* val) {GUID.set(val);};
    void setOption(const char* val) {option.set(val);};
    void setUpdateLogRequest(const char* val) {updateLogRequest.set(val);};
    void setESPContext(IPropertyTree* val) {espContext.setown(val);};
    void setUserContext(IPropertyTree* val) {userContext.setown(val);};
    void setUserRequest(IPropertyTree* val) {userRequest.setown(val);};
    void setBackEndResponse(const char* val) {backEndResponse.set(val);};
    void setUserResponse(const char* val) {userResponse.set(val);};
    unsigned incrementRetryCount() { retryCount++; return retryCount;};
};

interface IEspLogAgent : extends IInterface
{
    virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process) = 0;
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp) = 0;
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp) = 0;
    virtual void filterLogContent(IEspUpdateLogRequestWrap* req) = 0;
};

#endif  //_LOGGINGAGENT_HPP__
