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

#ifndef _HTTPCLIENT_IPP__
#define _HTTPCLIENT_IPP__

#include "httpclient.hpp"
#include "securesocket.hpp"
#include "espsession.ipp"
#include "persistent.hpp"

//#define COOKIE_HANDLING

class CHttpClientContext : public CInterface, implements IHttpClientContext
{
    friend class CHttpClient;
private:
    Owned<ISecureSocketContext> m_ssctx;
    Owned<IPropertyTree> m_config;
    CriticalSection m_sscrit;
    Owned<IPersistentHandler> m_persistentHandler;

#ifdef COOKIE_HANDLING
    ReadWriteLock m_rwlock;
    IArrayOf<CEspCookie> m_cookies;
protected:
    IArrayOf<CEspCookie>& queryCookies() {return m_cookies;}
    //TODO: need to replace old cookies that match the new cookie.
    void addCookie(CEspCookie* cookie)
    {
        if(!cookie)
            return;
        const char* name = cookie->getName();
        const char* host = cookie->getHost();
        if(!name || !host)
            return;
        {
            WriteLockBlock wblock(m_rwlock);
            ForEachItemInRev(x, m_cookies)
            {
                CEspCookie* c = &m_cookies.item(x);
                if(!c)
                    continue;
                if(stricmp(c->getName(), name) == 0 && stricmp(c->getHost(), host) == 0)
                {
                    m_cookies.remove(x);
                }
            }
            m_cookies.append(*LINK(cookie));
        }
    }
#endif

public:
    IMPLEMENT_IINTERFACE;

    CHttpClientContext();
    CHttpClientContext(IPropertyTree* config);
    virtual ~CHttpClientContext();
    virtual IHttpClient* createHttpClient(const char* proxy, const char* url);
};

class CHttpClient : implements IHttpClient, public CInterface
{

#ifdef COOKIE_HANDLING
    friend class CHttpClientContext;
protected:
    IArrayOf<CEspCookie> m_request_cookies;
    IArrayOf<CEspCookie> m_response_cookies;
    CHttpClientContext* m_context;
#endif

private:
    StringAttr m_protocol;
    StringAttr m_host; 
    int        m_port;
    StringAttr m_path;
    StringAttr m_url;
    StringBuffer m_proxy;
    ISocket*   m_socket;
    bool          m_disableKeepAlive;

    unsigned m_connectTimeoutMs = HTTP_CLIENT_DEFAULT_CONNECT_TIMEOUT;
    unsigned m_readTimeoutSecs = 0;

    StringAttr m_userid;
    StringAttr m_password;
    StringAttr m_realm;
    ISecureSocketContext *m_ssctx;
    IPersistentHandler* m_persistentHandler;
    bool m_isPersistentSocket;
    int m_numRequests;
    SocketEndpoint m_ep;

    virtual int connect(StringBuffer& errmsg);

public:
    IMPLEMENT_IINTERFACE;

    CHttpClient(const char *proxy, const char* url);
    virtual ~CHttpClient();
    virtual void setSsCtx(ISecureSocketContext* ctx);
    virtual void disableKeepAlive() { m_disableKeepAlive = true; }

    virtual int sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response);
    virtual int sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent = false);
    virtual int sendRequest(IProperties *headers, const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent = false);

    virtual int postRequest(ISoapMessage &req, ISoapMessage& resp);

    virtual void setProxy(const char* proxy);

    virtual void setUserID(const char* userid);
    virtual void setPassword(const char* password);
    virtual void setRealm(const char* realm);
    virtual void setConnectTimeOutMs(unsigned timeout) override;
    virtual void setTimeOut(unsigned int timeout);
    virtual void setPersistentHandler(IPersistentHandler* handler) { m_persistentHandler = handler; }
};

#endif
