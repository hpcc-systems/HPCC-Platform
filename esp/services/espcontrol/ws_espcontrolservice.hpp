/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef _ESPWIZ_ws_espcontrol_HPP__
#define _ESPWIZ_ws_espcontrol_HPP__

#include "ws_espcontrol_esp.ipp"
#include "espp.hpp"
#include "environment.hpp"

class CWSESPControlEx : public CWSESPControl
{
    StringAttr espProcess;
    MapStringTo<int> sessionTimeoutMinutesMap;
    IEspContainer* m_container;

    const char* readSessionTimeStamp(int t, StringBuffer& str);
    float readSessionTimeoutMin(int sessionTimeoutMinutes, int lastAccessed);
    IRemoteConnection* querySDSConnection(const char* xpath, unsigned mode, unsigned timeout);
    IRemoteConnection* querySDSConnectionForESPSession(unsigned mode, unsigned timeout);
    const char* setSessionXPath(bool allSessions, const char* _id, const char* _userID, const char* _fromIP, StringBuffer& xPath);
    IEspSession* setSessionInfo(IPropertyTree* espSessionTree, unsigned port, IEspSession* session);
    void cleanSessions(bool allSessions, const char* _id, const char* _userID, const char* _fromIP);
    void setSessionTimeout(int timeoutMinutes, IPropertyTree& session);

public:
    IMPLEMENT_IINTERFACE;

    virtual void setContainer(IEspContainer * container)
    {
        m_container = container;
    }

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual bool onSetLogging(IEspContext &context, IEspSetLoggingRequest &req, IEspSetLoggingResponse &resp);
    virtual bool onSessionQuery(IEspContext& context, IEspSessionQueryRequest& req, IEspSessionQueryResponse& resp);
    virtual bool onSessionInfo(IEspContext& context, IEspSessionInfoRequest& req, IEspSessionInfoResponse& resp);
    virtual bool onCleanSession(IEspContext& context, IEspCleanSessionRequest& req, IEspCleanSessionResponse& resp);
    virtual bool onSetSessionTimeout(IEspContext& context, IEspSetSessionTimeoutRequest& req, IEspSetSessionTimeoutResponse& resp);
    virtual bool onDetachBindingsFromDali(IEspContext& context, IEspDetachBindingsFromDaliRequest& req, IEspDetachBindingsFromDaliResponse& resp);
    virtual bool onAttachBindingsToDali(IEspContext& context, IEspAttachBindingsToDaliRequest& req, IEspAttachBindingsToDaliResponse& resp);
};

#endif //_ESPWIZ_ws_espcontrol_HPP__
