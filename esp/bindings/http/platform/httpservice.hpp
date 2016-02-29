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
    authUpdatePassword,
    authFailed
} EspAuthState;

enum HTTPSessionState
{
    HTTPSS_new,
    HTTPSS_active
};

class CEspHttpServer : public CInterface, implements IHttpServerService
{
protected:
    ISocket&                m_socket;
    Owned<CHttpRequest>     m_request;
    Owned<CHttpResponse>    m_response;
    CEspApplicationPort*    m_apport;
    Owned<IEspRpcBinding>   m_defaultBinding;

    bool m_viewConfig;
    int m_MaxRequestEntityLength;

    int unsupported();
    EspHttpBinding* getBinding();
#ifdef _USE_OPENLDAP
    EspAuthState preCheckAuth(const char* reqMethod, const char* serviceName, sub_service stype, const char* serviceMethod);
    EspAuthState checkUserAuth();
    EspAuthState doSessionAuth(IEspContext* ctx, EspHttpBinding* authBinding, unsigned sessionID,
        const char* httpPath, const char* httpMethod, const char* serviceName, const char* methodName, sub_service stype);
    void postSessionAuth(IEspContext* ctx, unsigned sessionID, time_t accessTime, void* _conn, IPropertyTree* sessionTree);
    void askUserLogOn(unsigned sessionID, void* conn);
    void handleAuthFailed(IEspContext* ctx, bool sessionAuth, EspHttpBinding* authBinding,
        void* conn, unsigned sessionID, const char* redirectURL);
    void handlePasswordExpired(bool sessionAuth);
    bool handleUserLogOut(EspHttpBinding* authBinding, unsigned sessionID);
    EspHttpBinding* getEspHttpBinding(IEspContext* ctx, const char* httpMethod, const char* serviceName,
        sub_service stype, bool isSoapPost);
    bool isAuthRequiredForBinding(IEspContext* ctx, EspHttpBinding* authBinding, const char* httpPath);
    void authOptionalGroups(IEspContext* ctx, EspHttpBinding* authBinding, const char* httpMethod, const char* serviceName, sub_service stype);
    unsigned createHTTPSession(EspHttpBinding* authBinding, IPropertyTree* domainSessions, const char* loginURL);
    bool deleteHTTPSession(EspHttpBinding* authBinding, unsigned sessionID);
    IPropertyTree* readAndCleanDomainSessions(void* conn, time_t accessTime);
    void addCookie(const char* cookieName, const char *cookieValue, unsigned maxAgeSec);
    void clearCookie(const char* cookieName);
    unsigned readSessionIDFromCookie();
#endif

public:
    IMPLEMENT_IINTERFACE;

    CEspHttpServer(ISocket& sock, bool viewConfig, int maxRequestEntityLength);
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
