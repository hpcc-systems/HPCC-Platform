/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _HTTPSERVICE_HPP__
#define _HTTPSERVICE_HPP__

//Jlib
#include "jiface.hpp"
#include "jstring.hpp"
#include "jsocket.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "http/platform/httptransport.ipp"
#include "SOAP/Platform/soapbind.hpp"
#include "SOAP/Platform/soapmessage.hpp"

#include "espsession.ipp"
#include "jhash.hpp"

typedef enum espAuthState_
{
    authUnknown,
    authRequired,
    authProvided,
    authSucceeded,
    authPending,
    authTaskDone,
    authFailed
} EspAuthState;

struct EspAuthRequest
{
    IEspContext* ctx;
    EspHttpBinding* authBinding;
    IProperties* requestParams;
    StringBuffer httpPath, httpMethod, serviceName, methodName;
    sub_service stype = sub_serv_unknown;
    bool isSoapPost;
};

interface IRemoteConnection;
class CEspHttpServer : implements IHttpServerService, public CInterface
{
    CriticalSection critDaliSession;
protected:
    ISocket&                m_socket;
    Owned<CHttpRequest>     m_request;
    Owned<CHttpResponse>    m_response;
    CEspApplicationPort*    m_apport;
    Owned<IEspRpcBinding>   m_defaultBinding;

    bool m_viewConfig;
    int m_MaxRequestEntityLength;
    int lastSessionCleanUpTime = 0;

    int unsupported();
    EspHttpBinding* getBinding();
    EspAuthState checkUserAuth();
    void readAuthRequest(EspAuthRequest& req);
    EspAuthState preCheckAuth(EspAuthRequest& authReq);
    EspAuthState checkUserAuthPerRequest(EspAuthRequest& authReq);
    EspAuthState checkUserAuthPerSession(EspAuthRequest& authReq);
    EspAuthState authNewSession(EspAuthRequest& authReq, const char* _userName, const char* _password, const char* sessionStartURL);
    EspAuthState authExistingSession(EspAuthRequest& req, unsigned sessionID);
    void logoutSession(EspAuthRequest& authReq, unsigned sessionID, IPropertyTree* domainSessions);
    void askUserLogin(EspAuthRequest& authReq);
    EspAuthState handleAuthFailed(bool sessionAuth, EspAuthRequest& authReq);
    EspHttpBinding* getEspHttpBinding(EspAuthRequest& req);
    bool isAuthRequiredForBinding(EspAuthRequest& req);
    void authOptionalGroups(EspAuthRequest& req);
    unsigned createHTTPSession(EspHttpBinding* authBinding, const char* userID, const char* loginURL);
    void timeoutESPSessions(EspHttpBinding* authBinding, IPropertyTree* espSessions);
    void addCookie(const char* cookieName, const char *cookieValue, int maxAgeSec);
    void clearCookie(const char* cookieName);
    unsigned readCookie(const char* cookieName);
    const char* readCookie(const char* cookieName, StringBuffer& cookieValue);
    void createGetSessionTimeoutResponse(StringBuffer& resp, ESPSerializationFormat format, IPropertyTree* sessionTree);
    void sendMessage(const char* msg, const char* msgType);
    IRemoteConnection* getSDSConnection(const char* xpath, unsigned mode, unsigned timeout);

public:
    IMPLEMENT_IINTERFACE;

    CEspHttpServer(ISocket& sock, CEspApplicationPort* apport, bool viewConfig, int maxRequestEntityLength);
    virtual ~CEspHttpServer();

    //IEspService
    bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
    {
        return true;
    }
    bool rootAuth(IEspContext* ctx);

    virtual int processRequest();

    virtual int onPost();
    virtual int onGet();
    virtual int onOptions();

    virtual int onGetFile(CHttpRequest* request, CHttpResponse* response, const char *path);
    virtual int onGetXslt(CHttpRequest* request, CHttpResponse* response, const char *path);

    virtual int onGetBuildSoapRequest(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetApplicationFrame(CHttpRequest* request, CHttpResponse* response, IEspContext* ctx);
    virtual int onGetTitleBar(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetNavWindow(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetDynNavData(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetNavEvent(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetMainWindow(CHttpRequest* request, CHttpResponse* response);
#ifdef _USE_OPENLDAP
    virtual int onUpdatePasswordInput(CHttpRequest* request, CHttpResponse* response);
    virtual int onUpdatePassword(CHttpRequest* request, CHttpResponse* response);
#endif


    virtual const char * getServiceType() {return "HttpServer";};
};


#endif
