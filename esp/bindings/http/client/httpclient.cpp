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

#pragma warning(disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

//ESP Bindings
#include "httpclient.ipp"
#include "http/platform/httptransport.ipp"
#include "securesocket.hpp"
#include "bindutil.hpp"
#include "espplugin.ipp"
#include "SOAP/Platform/soapmessage.hpp"

/*************************************************************************
     CHttpClient Implementation
**************************************************************************/
#define URL_MAX  512
#define HTTP_CLIENT_DEFAULT_CONNECT_TIMEOUT 3000

CHttpClientContext::CHttpClientContext()
{
}

CHttpClientContext::CHttpClientContext(IPropertyTree* config) : m_config(config)
{
}

CHttpClientContext::~CHttpClientContext()
{
}

IHttpClient* CHttpClientContext::createHttpClient(const char* proxy, const char* url)
{
    CHttpClient* client = new CHttpClient(proxy, url);

    if(url != NULL && Utils::strncasecmp(url, "HTTPS://", 8) == 0)
    {
        if(m_ssctx.get() == NULL)
        {
            if(m_config.get() == NULL)
            {
                createSecureSocketContext_t xproc = NULL;
                IEspPlugin *pplg = loadPlugin(SSLIB);
                if (pplg)
                    xproc = (createSecureSocketContext_t) pplg->getProcAddress("createSecureSocketContext");
                else
                    throw MakeStringException(-1, "dll/shared-object %s can't be loaded", SSLIB);

                if (xproc)
                    m_ssctx.setown(xproc(ClientSocket));
                else
                    throw MakeStringException(-1, "procedure createSecureSocketContext can't be loaded");
            }
            else
            {
                createSecureSocketContextEx2_t xproc = NULL;
                IEspPlugin *pplg = loadPlugin(SSLIB);
                if (pplg)
                    xproc = (createSecureSocketContextEx2_t) pplg->getProcAddress("createSecureSocketContextEx2");
                else
                    throw MakeStringException(-1, "dll/shared-object %s can't be loaded", SSLIB);

                if (xproc)
                    m_ssctx.setown(xproc(m_config.get(),ClientSocket));
                else
                    throw MakeStringException(-1, "procedure createSecureSocketContext can't be loaded");

            }
            if(m_ssctx.get() == NULL)
                throw MakeStringException(-1, "SecureSocketContext can't be created");

        }
        client->setSsCtx(m_ssctx.get());
    }

#ifdef COOKIE_HANDLING
    client->m_context = this;
    if(url && *url)
    {
        ReadLockBlock rblock(m_rwlock);
        StringBuffer host, protocol, user, passwd, port, path;
        Utils::SplitURL(url, protocol, user, passwd, host, port, path); 
        if(host.length() > 0)
        {
            ForEachItemIn(x, m_cookies)
            {
                CEspCookie* cookie = &m_cookies.item(x);
                if(!cookie)
                    continue;
                const char* chost = cookie->getHost();
                if(chost && stricmp(chost, host.str()) == 0)
                {
                    //TODO: is it better to clone the cookie?
                    client->m_request_cookies.append(*LINK(cookie));
                }
            }
        }
    }
#endif

    return client;
}




CHttpClient::CHttpClient(const char *proxy, const char* url) : m_proxy(proxy), m_url(url), m_disableKeepAlive(false), m_timeout(0)
{
    StringBuffer protocol,username,password, host, port, path;
    Utils::SplitURL(url, protocol,username,password, host, port, path);

    m_protocol.set(protocol.str());
    m_host.set(host.str());

    if(port.length() > 0)
        m_port = atoi(port.str());
    else
        if(Utils::strncasecmp(url, "HTTPS://", 8) == 0)
            m_port = 443;
        else
            m_port = 80;

    m_path.set(path.str());

    m_socket = NULL;
}

CHttpClient::~CHttpClient()
{
    if(m_socket)
    {
        try
        {
            m_socket->shutdown();
            m_socket->close();
            m_socket->Release();
        }
        catch(...)
        {
        }
    }
}

void CHttpClient::setSsCtx(ISecureSocketContext* ctx)
{
    m_ssctx = ctx;
}

void CHttpClient::setUserID(const char* userid)
{
    m_userid.set(userid);
}

void CHttpClient::setPassword(const char* password)
{
    m_password.set(password);
}

void CHttpClient::setRealm(const char* realm)
{
    m_realm.set(realm);
}

void CHttpClient::setProxy(const char* proxy)
{
    m_proxy.clear().append(proxy);
}

void CHttpClient::setTimeOut(unsigned int timeout)
{
    m_timeout =  timeout;
}

int CHttpClient::connect(StringBuffer& errmsg)
{
    SocketEndpoint ep;
    
    if(m_proxy.length() == 0)
    {
        if(m_host.length() <= 0)
            throw MakeStringException(-1, "host not specified");
        if (!ep.set(m_host.get(), m_port))
        {
            errmsg.appendf("Bad host name/ip: %s", m_host.get());
            ERRLOG("%s", errmsg.str());
            return -1;
        }
        //TODO: should it be 443 for HTTPS??
        if (ep.port==0)
            ep.port=80;
    }
    else
    {
        if (!ep.set(m_proxy.str()))
        {
            errmsg.appendf("Bad proxy name/ip: %s", m_proxy.str());
            ERRLOG("%s", errmsg.str());
            return -1;
        }
        //TODO: should it be 443 for HTTPS??
        if (ep.port==0)
            ep.port=80;
    }

    try
    {
        if(m_timeout)
            m_socket = ISocket::connect_timeout(ep,m_timeout);
        else
            m_socket = ISocket::connect_timeout(ep, HTTP_CLIENT_DEFAULT_CONNECT_TIMEOUT);

        if(strcmp(m_protocol.get(), "HTTPS") == 0)
        {
            ISecureSocket* securesocket = m_ssctx->createSecureSocket(m_socket);
            int res = securesocket->secure_connect();
            if(res < 0)
            {
                m_socket->shutdown();
                m_socket->close();
                m_socket->Release();
                m_socket = NULL;
            }
            else
            {
                m_socket = securesocket;
            }
        }
    }
    catch(IException *e)
    {
        StringBuffer url;
        ERRLOG("Error connecting to %s", ep.getUrlStr(url).str());
        DBGLOG(e);
        e->Release();
        m_socket = NULL;
        return -1;
    }
    catch(...)
    {
        StringBuffer url;
        ERRLOG("Unknown exception connecting to %s", ep.getUrlStr(url).str());
        m_socket = NULL;
        return -1;
    }

    if(m_socket == NULL)
    {
        StringBuffer urlstr;
        DBGLOG(">>Can't connect to %s", ep.getUrlStr(urlstr).str());
        return -1;
    }

    return 0;
}

int CHttpClient::sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response)
{
    StringBuffer errmsg;
    if(connect(errmsg) < 0)
    {
        response.append(errmsg);
        return -1;
    }

    Owned<CHttpRequest> httprequest;
    Owned<CHttpResponse> httpresponse;

    httprequest.setown(new CHttpRequest(*m_socket));
    httpresponse.setown(new CHttpResponse(*m_socket));
    
    httprequest->setMethod(method);
    httprequest->setVersion("HTTP/1.1");

    if(m_proxy.length() <= 0)
    {
        httprequest->setPath(m_path.get());
    }
    else
    {
        httprequest->setPath(m_url.get());
    }

    httprequest->setHost(m_host.get());
    httprequest->setPort(m_port);
    
    httprequest->setContentType(contenttype);

    if(m_userid.length() > 0)
    {
        StringBuffer uidpair;
        uidpair.append(m_userid.get()).append(":").append(m_password.get());
        StringBuffer result;
        Utils::base64encode(uidpair.str(), uidpair.length(), result);
        StringBuffer authhdr("Basic ");

        //Remove the \n from the end of the encoded string.
        //Should it even be there??
        result.setCharAt(result.length() - 1,0);

        authhdr.append(result.str());
        httprequest->addHeader("Authorization", authhdr.str());
    }
    if(m_realm.length() > 0)
    {
        StringBuffer authheader;
        authheader.append("Basic realm=\"").append(m_realm).append("\"");
        httprequest->addHeader("WWW-Authenticate", authheader.str());
    }

    if (getEspLogLevel()>LogNormal)
    {
        DBGLOG("Content type: %s", contenttype);
        DBGLOG("Request content: %s", request.str());
    }

    httprequest->setContent(request.str());

#ifdef COOKIE_HANDLING
    ForEachItemIn(x, m_request_cookies)
    {
        CEspCookie* cookie = &m_request_cookies.item(x);
        if(cookie)
            httprequest->addCookie(LINK(cookie));
    }
#endif

    if(m_timeout)
        httpresponse->setTimeOut(m_timeout);

    httprequest->send();
    httpresponse->receive(false, NULL);  // MORE - pass in IMultiException if we want to see exceptions (which are not fatal)

#ifdef COOKIE_HANDLING
    if(m_context)
    {
        IArrayOf<CEspCookie>& cookies = httpresponse->queryCookies();
        ForEachItemIn(x, cookies)
        {
            CEspCookie* cookie = &cookies.item(x);
            if(!cookie)
                continue;
            cookie->setHost(m_host.get());
            m_context->addCookie(cookie);
        }
    }
#endif

    httpresponse->getContent(response);


    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response content: %s", response.str());

    return 0;
}


int CHttpClient::sendRequest(IProperties *headers, const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent)
{
    StringBuffer errmsg;
    if(connect(errmsg) < 0)
    {
        response.append(errmsg);
        return -1;
    }

    Owned<CHttpRequest> httprequest;
    Owned<CHttpResponse> httpresponse;

    httprequest.setown(new CHttpRequest(*m_socket));
    httpresponse.setown(new CHttpResponse(*m_socket));
    
    httprequest->setMethod(method);
    httprequest->setVersion("HTTP/1.1");

    if(m_proxy.length() <= 0)
    {
        httprequest->setPath(m_path.get());
    }
    else
    {
        httprequest->setPath(m_url.get());
    }

    httprequest->setHost(m_host.get());
    httprequest->setPort(m_port);
    
    httprequest->setContentType(contenttype);

    if (headers)
    {
        Owned<IPropertyIterator> iter = headers->getIterator();
        ForEach(*iter.get())
        {
            const char *key = iter->getPropKey();
            if (key && *key)
            {
                const char *value = headers->queryProp(key);
                if (value && *value)
                    httprequest->addHeader(key, value);
            }
        }
    }

    if(m_userid.length() > 0)
    {
        StringBuffer uidpair;
        uidpair.append(m_userid.get()).append(":").append(m_password.get());
        StringBuffer result;
        Utils::base64encode(uidpair.str(), uidpair.length(), result);
        StringBuffer authhdr("Basic ");

        //Remove the \n from the end of the encoded string.
        //Should it even be there??
        result.setCharAt(result.length() - 1,0);

        authhdr.append(result.str());
        httprequest->addHeader("Authorization", authhdr.str());
    }
    if(m_realm.length() > 0)
    {
        StringBuffer authheader;
        authheader.append("Basic realm=\"").append(m_realm).append("\"");
        httprequest->addHeader("WWW-Authenticate", authheader.str());
    }

    if (getEspLogLevel()>LogNormal)
    {
        DBGLOG("Content type: %s", contenttype);
        DBGLOG("Request content: %s", request.str());
    }

    httprequest->setContent(request.str());

    Owned<IMultiException> me = MakeMultiException();
    //httprequest->sendWithoutContentType();

#ifdef COOKIE_HANDLING
    ForEachItemIn(x, m_request_cookies)
    {
        CEspCookie* cookie = &m_request_cookies.item(x);
        if(cookie)
            httprequest->addCookie(LINK(cookie));
    }
#endif

    if(m_timeout)
        httpresponse->setTimeOut(m_timeout);

    httprequest->send();
    httpresponse->receive(alwaysReadContent, me);  // MORE - pass in IMultiException if we want to see exceptions (which are not fatal)

#ifdef COOKIE_HANDLING
    if(m_context)
    {
        IArrayOf<CEspCookie>& cookies = httpresponse->queryCookies();
        ForEachItemIn(x, cookies)
        {
            CEspCookie* cookie = &cookies.item(x);
            if(!cookie)
                continue;
            cookie->setHost(m_host.get());
            m_context->addCookie(cookie);
        }
    }
#endif

    httpresponse->getContent(response);
    httpresponse->getStatus(responseStatus);

//const char* dstxml = "c:\\debug02.jpg";
//int ofile = open(dstxml, _O_WRONLY | _O_CREAT | _O_BINARY);
//if(ofile != -1)
//{
//write(ofile, response.str(), response.length());
//close(ofile);
//}
    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response content: %s", response.str());

    return 0;
}

int CHttpClient::sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent)
{
    return sendRequest(NULL, method, contenttype, request, response, responseStatus, alwaysReadContent);
}

// since an element may have namespace specified in its tag, don't look for trailing '>' 
// in its start tag
static const char* getElementText(const char* str, const char* beginTag/*like '<A'*/, const char* endTag/*like '</A>'*/,
                                  int& textLen)
{
    const char* element = strstr(str, beginTag);//element points to '<A...'
    if (element)
    {
      const char* endStartTag = strchr(element, '>');/* > of start tag <A...>*/
        const char* beginEndTag = strstr(element, endTag);
      if (endStartTag && beginEndTag && endStartTag++ < beginEndTag)
      {
         textLen = beginEndTag - endStartTag;
         return endStartTag;
      }
    }
   textLen = 0;
   return NULL;
}

static void parseSoapFault(const char* content, StringBuffer& msg)
{
    const char* start = strstr(content, ":Fault");//match any namespace like 'soap' or 'soapenv' etc. before :Fault
    if (start)
    {
        start += 8;
        msg.append("SOAP fault:");

      int textLen;
      const char* elementText;
      elementText = getElementText(start, "<faultcode", "</faultcode>", textLen);
        if (elementText)
        {
            msg.append(" code=");
            msg.append(textLen, elementText);
            msg.append(".");
        }
      elementText = getElementText(start, "<faultstring", "</faultstring>", textLen);
        if (elementText)
        {
            msg.append(" string=");
            msg.append(textLen, elementText);
            msg.append(".");
        }
      elementText = getElementText(start, "<detail", "</detail>", textLen);
        if (elementText)
        {
            msg.append(" detail=");
            msg.append(textLen, elementText);
            msg.append(".");
        }
    }
}

int CHttpClient::postRequest(ISoapMessage &req, ISoapMessage& resp)
{
    CSoapRequest& request = *(dynamic_cast<CSoapRequest*>(&req));
    const char* requeststr = request.get_text();
    CSoapResponse& response = *(dynamic_cast<CSoapResponse*>(&resp));

    if(&request == NULL || &response == NULL)
        throw MakeStringException(-1, "request or response is NULL");

    StringBuffer errmsg;
    if(connect(errmsg) < 0)
    {
        response.set_status(SOAP_CONNECTION_ERROR);
        response.set_err(errmsg);
        return -1;
    }

    Owned<CHttpRequest> httprequest(new CHttpRequest(*m_socket));
    Owned<CHttpResponse> httpresponse(new CHttpResponse(*m_socket));

    
    httprequest->setMethod("POST");
    httprequest->setVersion("HTTP/1.1");

    if(m_proxy.length() <= 0)
    {
        httprequest->setPath(m_path.get());
    }
    else
    {
        httprequest->setPath(m_url.get());
    }

    httprequest->setHost(m_host.get());
    httprequest->setPort(m_port);
    
    if(strlen(request.get_content_type()) > 0)
        httprequest->setContentType(request.get_content_type());

    const char* soapaction = request.get_soapaction();
    if(soapaction != NULL && strlen(soapaction) > 0)
    {
        httprequest->addHeader("SOAPAction", soapaction);
    }
    if(m_userid.length() > 0)
    {
        StringBuffer uidpair;
        uidpair.append(m_userid.get()).append(":").append(m_password.get());
        StringBuffer result;
        Utils::base64encode(uidpair.str(), uidpair.length(), result);
        StringBuffer authhdr("Basic ");

        //Remove the \n from the end of the encoded string.
        //Should it even be there??
        result.setCharAt(result.length() - 1,0);

        authhdr.append(result.str());
        httprequest->addHeader("Authorization", authhdr.str());
        if(m_proxy.length())
            httprequest->addHeader("Proxy-Authorization", authhdr.str());
    }
    if(m_realm.length() > 0)
    {
        StringBuffer authheader;
        authheader.append("Basic realm=\"").append(m_realm).append("\"");
        httprequest->addHeader("WWW-Authenticate", authheader.str());
    }

    if (m_disableKeepAlive)
        httprequest->addHeader("Connection", "close");
    httprequest->setContentType(HTTP_TYPE_TEXT_XML);
    httprequest->setContent(requeststr);

    if (getEspLogLevel()>LogNormal)
        DBGLOG("http request content = %s", requeststr);

#ifdef COOKIE_HANDLING
    ForEachItemIn(x, m_request_cookies)
    {
        CEspCookie* cookie = &m_request_cookies.item(x);
        if(cookie)
            httprequest->addCookie(LINK(cookie));
    }
#endif
    httprequest->send();
    Owned<IMultiException> me = MakeMultiException();
    httpresponse->receive(true, me);

#ifdef COOKIE_HANDLING
    if(m_context)
    {
        IArrayOf<CEspCookie>& cookies = httpresponse->queryCookies();
        ForEachItemIn(x, cookies)
        {
            CEspCookie* cookie = &cookies.item(x);
            if(!cookie)
                continue;
            cookie->setHost(m_host.get());
            m_context->addCookie(cookie);
        }
    }
#endif

    StringBuffer status;
    httpresponse->getStatus(status);

    int statusCode = atoi(status.str());
    char statusClass = '0';
    if(status.length() > 0)
        statusClass = status.charAt(0);

    errmsg.clear().appendf("HTTP Status %s", status.str());
    if(statusClass == '2')
    {
        response.set_status(SOAP_OK);
    }
    else if(statusClass == '3')
    {
        response.set_status(SOAP_SERVER_ERROR);
        response.set_err(errmsg.str());
        return -1;
    }
    else if(statusClass == '4')
    {
        if(statusCode == HTTP_STATUS_UNAUTHORIZED_CODE || 
            statusCode == HTTP_STATUS_FORBIDDEN_CODE ||
            statusCode == HTTP_STATUS_NOT_ALLOWED_CODE ||
            statusCode == HTTP_STATUS_PROXY_AUTHENTICATION_REQUIRED_CODE)
            response.set_status(SOAP_AUTHENTICATION_ERROR);
        else
            response.set_status(SOAP_CLIENT_ERROR);

        response.set_err(errmsg.str());
        DBGLOG("SOAP_CLIENT_ERROR: %s", errmsg.str());
        return -1;
    }
    else if(statusClass == '5')
    {
        response.set_status(SOAP_SERVER_ERROR);

        StringBuffer content;
        parseSoapFault(httpresponse->getContent(content),errmsg);

        response.set_err(errmsg.str());
        DBGLOG("SOAP_SERVER_ERROR: %s", errmsg.str());

        return -1;
    }
    else
    {
        DBGLOG("%s", errmsg.str());

        StringBuffer msg;
        if (me->ordinality())
        {
            aindex_t count = me->ordinality();
            for (aindex_t i = 0; i < count; i++)
            {
                IException& ex = me->item(i);

                int errCode = ex.errorCode();
                StringBuffer buf;
                msg.appendf("errorCode = %d\t message = %s\n", errCode, ex.errorMessage(buf).str());
            }

        }
        DBGLOG("SOAP_RPC_ERROR = %s", msg.str());
        response.set_status(SOAP_RPC_ERROR);
        response.set_err(msg);
        return -1;
    }

    StringBuffer contenttype;
    httpresponse->getContentType(contenttype);
    response.set_content_type(contenttype.str());
    StringBuffer content;
    httpresponse->getContent(content);

    if (getEspLogLevel()>LogNormal)
    {
        if(httpresponse->isTextMessage())
            DBGLOG("http response content = %s", content.str());
    }
   
    response.set_text(content.str());
            
    // parse soap fault
    parseSoapFault(content,errmsg.clear());
    if (errmsg.length())
        response.set_err(errmsg);

    return 0;
}

IHttpClientContext* getHttpClientContext()
{
    static Owned<CHttpClientContext> theHttpClientContext;

    if(theHttpClientContext.get() == NULL)
    {
        theHttpClientContext.setown(new CHttpClientContext());
    }
    return theHttpClientContext.getLink();
}

IHttpClientContext* createHttpClientContext(IPropertyTree* config)
{
    return new CHttpClientContext(config);
}

