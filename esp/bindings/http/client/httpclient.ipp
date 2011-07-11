/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef _HTTPCLIENT_IPP__
#define _HTTPCLIENT_IPP__

#include "httpclient.hpp"
#include "securesocket.hpp"
#include "espsession.ipp"

//#define COOKIE_HANDLING

class CHttpClientContext : public CInterface, implements IHttpClientContext
{
    friend class CHttpClient;
private:
    Owned<ISecureSocketContext> m_ssctx;
    Owned<IPropertyTree> m_config;

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

class CHttpClient : public CInterface, implements IHttpClient
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
    unsigned int m_timeout;

    StringAttr m_userid;
    StringAttr m_password;
    StringAttr m_realm;
    ISecureSocketContext *m_ssctx;

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
    virtual void setTimeOut(unsigned int timeout);
};

#endif
