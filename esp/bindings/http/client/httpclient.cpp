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

#pragma warning(disable : 4786)


//Jlib
#include "jliball.hpp"

//ESP Bindings
#include "httpclient.ipp"
#include "http/platform/httptransport.hpp"
#include "securesocket.hpp"
#include "bindutil.hpp"
#include "espplugin.ipp"
#include "SOAP/Platform/soapmessage.hpp"

/*************************************************************************
     CHttpClient Implementation
**************************************************************************/
#define URL_MAX  512

CHttpClientContext::CHttpClientContext()
{
    initPersistentHandler();
}

CHttpClientContext::CHttpClientContext(IPropertyTree* config) : m_config(config)
{
    initPersistentHandler();
}

void CHttpClientContext::initPersistentHandler()
{
    m_persistentHandler.setown(createPersistentHandler(nullptr, DEFAULT_MAX_PERSISTENT_IDLE_TIME, DEFAULT_MAX_PERSISTENT_REQUESTS, static_cast<PersistentLogLevel>(getEspLogLevel()), true));
}

CHttpClientContext::~CHttpClientContext()
{
}

IHttpClient* CHttpClientContext::createHttpClient(const char* proxy, const char* url)
{
    CHttpClient* client = new CHttpClient(proxy, url);

    if(url != NULL && Utils::strncasecmp(url, "HTTPS://", 8) == 0)
    {
        CriticalBlock b(m_sscrit);
        if(m_ssctx.get() == NULL)
        {
            StringBuffer libName;
            IEspPlugin *pplg = loadPlugin(libName.append(SharedObjectPrefix).append(SSLIB).append(SharedObjectExtension));
            if (!pplg)
                throw MakeStringException(-1, "dll/shared-object %s can't be loaded", libName.str());

            if(m_config.get() == NULL)
            {
                createSecureSocketContext_t xproc = NULL;
                xproc = (createSecureSocketContext_t) pplg->getProcAddress("createSecureSocketContext");
                if (xproc)
                    m_ssctx.setown(xproc(ClientSocket));
                else
                    throw MakeStringException(-1, "procedure createSecureSocketContext can't be loaded");
            }
            else
            {
                createSecureSocketContextEx2_t xproc = NULL;
                xproc = (createSecureSocketContextEx2_t) pplg->getProcAddress("createSecureSocketContextEx2");
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

    client->setPersistentHandler(m_persistentHandler);

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


CHttpClient::CHttpClient(const char *proxy, const char* url) : m_proxy(proxy), m_url(url), m_disableKeepAlive(false), m_isPersistentSocket(false), m_numRequests(0), m_persistable(false)
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
    if (m_socket)
    {
        Owned<ISocket> forRelease(m_socket);
        if (m_isPersistentSocket)
        {
            m_persistentHandler->doneUsing(m_socket, !m_disableKeepAlive && m_persistable, m_numRequests>1?(m_numRequests-1):0);
        }
        else if (!m_disableKeepAlive && m_persistable)
        {
            m_persistentHandler->add(m_socket, &m_ep);
        }
        else
        {
            try
            {
                m_socket->shutdown();
                m_socket->close();
            }
            catch(...)
            {
            }
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

void CHttpClient::setConnectTimeOutMs(unsigned timeout)
{
    m_connectTimeoutMs =  timeout;
}

void CHttpClient::setTimeOut(unsigned int timeout)
{
    m_readTimeoutSecs =  timeout;
}

int CHttpClient::connect(StringBuffer& errmsg, bool forceNewConnection)
{
    if (m_socket != nullptr)
    {
        if(m_isPersistentSocket)
            m_persistentHandler->doneUsing(m_socket, false, 0);
        close();
    }

    SocketEndpoint ep;

    if(m_proxy.length() == 0)
    {
        if(m_host.length() <= 0)
            throw MakeStringException(-1, "host not specified");
        if (!ep.set(m_host.get(), m_port))
        {
            errmsg.appendf("Bad host name/ip: %s", m_host.get());
            UERRLOG("%s", errmsg.str());
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
            UERRLOG("%s", errmsg.str());
            return -1;
        }
        //TODO: should it be 443 for HTTPS??
        if (ep.port==0)
            ep.port=80;
    }
    m_ep = ep;

    if(m_persistentHandler->inDoNotReuseList(&ep))
        m_disableKeepAlive = true;

    Linked<ISocket> pSock = (m_disableKeepAlive || forceNewConnection)?nullptr:m_persistentHandler->getAvailable(&ep);
    if(pSock)
    {
        m_isPersistentSocket = true;
        m_socket = pSock.getLink();
    }
    else
    {
        m_isPersistentSocket = false;
        try
        {
            m_socket = ISocket::connect_timeout(ep, m_connectTimeoutMs);

            if(strcmp(m_protocol.get(), "HTTPS") == 0)
            {
                ISecureSocket* securesocket = m_ssctx->createSecureSocket(m_socket);
                int res = securesocket->secure_connect();
                if(res < 0)
                {
                    close();
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
            UERRLOG("Error connecting to %s", ep.getUrlStr(url).str());
            DBGLOG(e);
            e->Release();
            m_socket = nullptr;
            return -1;
        }
        catch(...)
        {
            StringBuffer url;
            UERRLOG("Unknown exception connecting to %s", ep.getUrlStr(url).str());
            m_socket = nullptr;
            return -1;
        }
    }
    if(m_socket == nullptr)
    {
        StringBuffer urlstr;
        DBGLOG(">>Can't connect to %s", ep.getUrlStr(urlstr).str());
        return -1;
    }

    return 0;
}

void CHttpClient::close()
{
    if (m_socket == nullptr)
        return;
    m_socket->shutdown();
    m_socket->close();
    m_socket->Release();
    m_socket = nullptr;
}

int CHttpClient::sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response)
{
    HttpClientErrCode ret = sendRequest(method, contenttype, request, response, false);
    if (ret == HttpClientErrCode::PeerClosed)
        ret = sendRequest(method, contenttype, request, response, true);
    return static_cast<int>(ret);
}

HttpClientErrCode CHttpClient::sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, bool forceNewConnection)
{
    StringBuffer errmsg;
    if (connect(errmsg, forceNewConnection) < 0)
    {
        response.append(errmsg);
        return HttpClientErrCode::Error;
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
        JBASE64_Encode(uidpair.str(), uidpair.length(), result, false);
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

    httprequest->send();

    if (m_readTimeoutSecs)
        httpresponse->setTimeOut(m_readTimeoutSecs);
    int ret = httpresponse->receive(false, NULL);  // MORE - pass in IMultiException if we want to see exceptions (which are not fatal)
    if (ret < 0 && m_isPersistentSocket && httpresponse->getPeerClosed())
        return HttpClientErrCode::PeerClosed;
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

    m_persistable = httpresponse->getPersistentEligible();
    m_numRequests++;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response content: %s", response.str());

    return HttpClientErrCode::OK;
}

void copyHeaders(CHttpMessage &copyTo, CHttpMessage &copyFrom)
{
    if (copyFrom.queryHeaders().ordinality())
    {
        ForEachItemIn(i, copyFrom.queryHeaders())
        {
            StringArray pair;
            pair.appendList(copyFrom.queryHeaders().item(i), ":", true);
            const char *name = pair.item(0);

            switch (*name)
            {
            case 'H':
            case 'h':
                if (strieq(name, "Host"))
                    continue;
                break;
            case 'C':
            case 'c':
                if (strnicmp(name, "Content-", 8)==0)
                {
                    if (strieq(name+8, "Length") || strieq(name+8, "Type"))
                        continue;
                }
                break;
            default:
                break;
            }
            copyTo.setHeader(name, pair.item(1));
        }
    }
}

void copyCookies(CHttpMessage &copyTo, CHttpMessage &copyFrom, const char *host)
{
    IArrayOf<CEspCookie>& cookies = copyFrom.queryCookies();
    ForEachItemIn(x, cookies)
    {
        CEspCookie* cookie = &cookies.item(x);
        if(!cookie)
            continue;
        cookie->setHost(host); //unfortunately changes copyFrom cookie... should make a true copy of cookie
        copyTo.addCookie(cookie);
    }
}

int CHttpClient::proxyRequest(IHttpMessage *request, IHttpMessage *response)
{
    HttpClientErrCode ret = proxyRequest(request, response, false);
    if (ret == HttpClientErrCode::PeerClosed)
        ret = proxyRequest(request, response, true);
    return static_cast<int>(ret);
}

HttpClientErrCode CHttpClient::proxyRequest(IHttpMessage *request, IHttpMessage *response, bool forceNewConnection)
{
    CHttpRequest *forwardRequest = static_cast<CHttpRequest*>(request);
    assertex(forwardRequest != nullptr);

    StringBuffer forwardFor;
    if (forwardRequest->getHeader("HPCC-Forward-For", forwardFor).length())
        throw MakeStringExceptionDirect(-1, "Only one HPCC-Forward-For hop currently allowed");

    CHttpResponse *forwardResponse = static_cast<CHttpResponse*>(response);
    assertex(forwardResponse != nullptr);

    StringBuffer errmsg;
    if (connect(errmsg, forceNewConnection) < 0)
    {
        forwardResponse->setContent(errmsg);
        forwardResponse->setContentType(HTTP_TYPE_TEXT_PLAIN);
        return HttpClientErrCode::Error;
    }

    Owned<CHttpRequest> httprequest = new CHttpRequest(*m_socket);
    Owned<CHttpResponse> httpresponse = new CHttpResponse(*m_socket);

    httprequest->setMethod(forwardRequest->queryMethod());
    httprequest->setVersion("HTTP/1.1");

    if(m_proxy.length() <= 0)
        httprequest->setPath(m_path.get());
    else
        httprequest->setPath(m_url.get());

    httprequest->setHost(m_host.get());
    httprequest->setPort(m_port);

    copyHeaders(*httprequest, *forwardRequest);

    httprequest->setHeader("HPCC-Forward-For", "true"); //For now limit to one hop, can support multi hpcc forward for hops in future, but why?

    StringBuffer contentType;
    forwardRequest->getContentType(contentType);

    httprequest->setContentType(contentType);
    httprequest->setContent(forwardRequest->queryContent());

    if (getEspLogLevel()>LogNormal)
    {
        StringBuffer s;
        DBGLOG("Content type: %s", forwardRequest->getContentType(s).str());
        DBGLOG("Request content: %s", forwardRequest->queryContent());
    }

    Owned<IMultiException> me = MakeMultiException();

    copyCookies(*httprequest, *forwardRequest, m_host);

    httprequest->send();

    if (m_readTimeoutSecs)
        httpresponse->setTimeOut(m_readTimeoutSecs);
    int ret = httpresponse->receive(false, nullptr);
    if (ret < 0 && m_isPersistentSocket && httpresponse->getPeerClosed())
        return HttpClientErrCode::PeerClosed;

    copyCookies(*forwardResponse, *httpresponse, m_host);
    copyHeaders(*forwardResponse, *httpresponse);

    StringBuffer responseStatus;
    StringBuffer responseContentType;

    forwardResponse->setStatus(httpresponse->getStatus(responseStatus));
    forwardResponse->setContent(httpresponse->queryContent());
    forwardResponse->setContentType(httpresponse->getContentType(responseContentType));

    m_persistable = httpresponse->getPersistentEligible();
    m_numRequests++;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response content: %s", httpresponse->queryContent());

    return HttpClientErrCode::OK;
}

int CHttpClient::sendRequest(IProperties *headers, const char* method, const char* contenttype, StringBuffer& content, StringBuffer& responseContent, StringBuffer& responseStatus, bool alwaysReadContent)
{
    HttpClientErrCode ret = sendRequest(headers, method, contenttype, content, responseContent, responseStatus, alwaysReadContent, false);
    if (ret == HttpClientErrCode::PeerClosed)
        ret = sendRequest(headers, method, contenttype, content, responseContent, responseStatus, alwaysReadContent, true);
    return static_cast<int>(ret);
}

HttpClientErrCode CHttpClient::sendRequest(IProperties *headers, const char* method, const char* contenttype, StringBuffer& content, StringBuffer& responseContent, StringBuffer& responseStatus, bool alwaysReadContent, bool forceNewConnection)
{
    StringBuffer errmsg;
    if (connect(errmsg, forceNewConnection) < 0)
    {
        responseContent.append(errmsg);
        return HttpClientErrCode::Error;
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
        JBASE64_Encode(uidpair.str(), uidpair.length(), result, false);
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
        DBGLOG("Request content: %s", content.str());
    }

    httprequest->setContent(content.str());

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

    httprequest->send();

    if (m_readTimeoutSecs)
        httpresponse->setTimeOut(m_readTimeoutSecs);
    int ret = httpresponse->receive(alwaysReadContent, me);  // MORE - pass in IMultiException if we want to see exceptions (which are not fatal)
    if (ret < 0 && m_isPersistentSocket && httpresponse->getPeerClosed())
        return HttpClientErrCode::PeerClosed;

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

    httpresponse->getContent(responseContent);
    httpresponse->getStatus(responseStatus);

    m_persistable = httpresponse->getPersistentEligible();
    m_numRequests++;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response content: %s", responseContent.str());

    return HttpClientErrCode::OK;
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
        start = strchr(start, '>');
    if (start)
    {
        start += 1;
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
    HttpClientErrCode ret = postRequest(req, resp, false);
    if (ret == HttpClientErrCode::PeerClosed)
        ret = postRequest(req, resp, true);
    return static_cast<int>(ret);
}

HttpClientErrCode CHttpClient::postRequest(ISoapMessage &req, ISoapMessage& resp, bool forceNewConnection)
{
    CSoapRequest& request = *(dynamic_cast<CSoapRequest*>(&req));
    const char* requeststr = request.get_text();
    CSoapResponse& response = *(dynamic_cast<CSoapResponse*>(&resp));

    StringBuffer errmsg;
    if (connect(errmsg, forceNewConnection) < 0 || !m_socket)
    {
        response.set_status(SOAP_CONNECTION_ERROR);
        response.set_err(errmsg);
        return HttpClientErrCode::Error;
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
        JBASE64_Encode(uidpair.str(), uidpair.length(), result, false);
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

    if (m_readTimeoutSecs)
        httpresponse->setTimeOut(m_readTimeoutSecs);
    int ret = httpresponse->receive(true, me);
    if (ret < 0 && m_isPersistentSocket && httpresponse->getPeerClosed())
        return HttpClientErrCode::PeerClosed;

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
        return HttpClientErrCode::Error;
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
        UERRLOG("SOAP_CLIENT_ERROR: %s", errmsg.str());
        return HttpClientErrCode::Error;
    }
    else if(statusClass == '5')
    {
        response.set_status(SOAP_SERVER_ERROR);

        StringBuffer content;
        parseSoapFault(httpresponse->getContent(content),errmsg);

        response.set_err(errmsg.str());
        UERRLOG("SOAP_SERVER_ERROR: %s", errmsg.str());

        return HttpClientErrCode::Error;
    }
    else
    {
        UERRLOG("%s", errmsg.str());

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
        UERRLOG("SOAP_RPC_ERROR = %s", msg.str());
        response.set_status(SOAP_RPC_ERROR);
        response.set_err(msg);
        return HttpClientErrCode::Error;
    }

    m_persistable = httpresponse->getPersistentEligible();
    m_numRequests++;

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

    return HttpClientErrCode::OK;
}

static Owned<CHttpClientContext> theHttpClientContext;
static CriticalSection httpCrit;

IHttpClientContext* getHttpClientContext()
{
    CriticalBlock b(httpCrit);
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

