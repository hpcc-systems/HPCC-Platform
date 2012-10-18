/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

#include "espcontext.hpp"
#include "esphttp.hpp"

//ESP Bindings
#include "http/platform/httpservice.hpp"
#include "http/platform/httptransport.hpp"

#include "htmlpage.hpp"

/***************************************************************************
 *              CEspHttpServer Implementation
 ***************************************************************************/
CEspHttpServer::CEspHttpServer(ISocket& sock, bool viewConfig, int maxRequestEntityLength):m_socket(sock), m_MaxRequestEntityLength(maxRequestEntityLength)
{
    IEspContext* ctx = createEspContext();
    m_request.setown(new CHttpRequest(sock));
    m_request->setMaxRequestEntityLength(maxRequestEntityLength);
    m_response.setown(new CHttpResponse(sock));
    m_request->setOwnContext(ctx);
    m_response->setOwnContext(LINK(ctx));
    m_viewConfig=viewConfig;
}

CEspHttpServer::CEspHttpServer(ISocket& sock, CEspApplicationPort* apport, bool viewConfig, int maxRequestEntityLength):m_socket(sock), m_MaxRequestEntityLength(maxRequestEntityLength)
{
    IEspContext* ctx = createEspContext();
    m_request.setown(new CHttpRequest(sock));
    m_request->setMaxRequestEntityLength(maxRequestEntityLength);
    m_response.setown(new CHttpResponse(sock));
    m_request->setOwnContext(ctx);
    m_response->setOwnContext(LINK(ctx));
    m_apport = apport;
    if (apport->getDefaultBinding())
        m_defaultBinding.set(apport->getDefaultBinding()->queryBinding());
    m_viewConfig=viewConfig;
}

CEspHttpServer::~CEspHttpServer()
{
    try
    {
        IEspContext* ctx = m_request->queryContext();
        if (ctx)
        {
            //Request is logged only when there is an exception or the request time is very long.
            //If the flag of 'EspLogRequests' is set or the log level > LogNormal, the Request should
            //has been logged and it should not be logged here.
            ctx->setProcessingTime();
            if ((ctx->queryHasException() || (ctx->queryProcessingTime() > getSlowProcessingTime())) &&
                !getEspLogRequests() && (getEspLogLevel() <= LogNormal))
            {
                StringBuffer logStr;
                logStr.appendf("%s %s", m_request->queryMethod(), m_request->queryPath());

                const char* paramStr = m_request->queryParamStr();
                if (paramStr && *paramStr)
                    logStr.appendf("?%s", paramStr);

                if (m_request->isSoapMessage())
                {
                    StringBuffer requestStr;
                    m_request->getContent(requestStr);

                    if (requestStr.length() > 0)
                    {
                        bool trimSpace = true;
                        StringBuffer requestBuf;
                        const char* s = requestStr.str();
                        while (s && *s)
                        {
                            if ((s[0] != '\r') && (s[0] != '\n'))
                            {
                                if (s[0] != ' ')
                                {
                                    requestBuf.append(s[0]);
                                    trimSpace = false;
                                }
                                else if (!trimSpace)
                                {
                                    requestBuf.append(s[0]);
                                }
                            }
                            else
                            {
                                trimSpace = true;
                            }

                            s++;
                        }

                        if (requestBuf.length() > 0)
                        {
                            logStr.newline();
                            logStr.append(requestBuf.str());
                        }
                    }
                }
                DBGLOG("Request[%s]", logStr.str());
            }
        }

        m_request.clear();
        m_response.clear();
    }
    catch (...)
    {
        ERRLOG("In CEspHttpServer::~CEspHttpServer() -- Unknown Exception.");
    }
}

typedef enum espAuthState_
{
    authUnknown,
    authRequired,
    authProvided,
    authSucceeded,
    authPending,
    authFailed
} EspAuthState;


bool CEspHttpServer::rootAuth(IEspContext* ctx)
{
    if (!m_apport->rootAuthRequired())
        return true;

    bool ret=false;
    EspHttpBinding* thebinding=NULL;
    int ordinality=m_apport->getBindingCount();
    if (ordinality==1)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(0);
        thebinding = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
    }
    else if (m_defaultBinding)
    {
        thebinding=dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());
    }

    if (thebinding)
    {
        thebinding->populateRequest(m_request.get());
        if(!thebinding->authRequired(m_request.get()) || thebinding->doAuth(ctx))
            ret=true;
        else
        {
            ISecUser *user = ctx->queryUser();
            if (user && user->getAuthenticateStatus() == AS_PASSWORD_EXPIRED)
            {
                DBGLOG("ESP password expired for %s", user->getName());
                m_response->setContentType(HTTP_TYPE_TEXT_PLAIN);
                m_response->setContent("Your ESP password has expired");
                m_response->send();
            }
            else
            {
                DBGLOG("User authentication required");
                m_response->sendBasicChallenge(thebinding->getChallengeRealm(), true);
            }
        }
    }

    return ret;
}

const char* getSubServiceDesc(sub_service stype)
{
#define DEF_CASE(s) case s: return #s;
    switch(stype)
    {
    DEF_CASE(sub_serv_unknown)
    DEF_CASE(sub_serv_root)
    DEF_CASE(sub_serv_main)
    DEF_CASE(sub_serv_service)
    DEF_CASE(sub_serv_method)
    DEF_CASE(sub_serv_files)
    DEF_CASE(sub_serv_itext)
    DEF_CASE(sub_serv_iframe)
    DEF_CASE(sub_serv_content)
    DEF_CASE(sub_serv_result)
    DEF_CASE(sub_serv_index)
    DEF_CASE(sub_serv_index_redirect)
    DEF_CASE(sub_serv_form)
    DEF_CASE(sub_serv_xform)
    DEF_CASE(sub_serv_query)
    DEF_CASE(sub_serv_instant_query)
    DEF_CASE(sub_serv_soap_builder)
    DEF_CASE(sub_serv_wsdl)
    DEF_CASE(sub_serv_xsd)
    DEF_CASE(sub_serv_config)
    DEF_CASE(sub_serv_php)
    DEF_CASE(sub_serv_getversion)
    DEF_CASE(sub_serv_reqsamplexml)
    DEF_CASE(sub_serv_respsamplexml)
    DEF_CASE(sub_serv_file_upload)

    default: return "invalid-type";
    }
} 

static bool authenticateOptionalFailed(IEspContext& ctx, IEspHttpBinding* binding)
{
#ifdef ENABLE_NEW_SECURITY
    if (ctx.queryRequestParameters()->hasProp("internal"))
    {
        ISecUser* user = ctx.queryUser();
        if(!user || user->getStatus()==SecUserStatus_Inhouse || user->getStatus()==SecUserStatus_Unknown)
            return false;

        ERRLOG("User %s trying to access unauthorized feature: internal", user->getName() ? user->getName() : ctx.queryUserId());
        return true;
    }
    // TODO: handle binding specific optionals
#elif !defined(DISABLE_NEW_SECURITY)
#error Please include esphttp.hpp in this file.
#endif

    return false;
}

int CEspHttpServer::processRequest()
{
    try
    {
        if (m_request->receive(NULL)==-1) // MORE - pass in IMultiException if we want to see exceptions (which are not fatal)
            return -1;
    }
    catch(IEspHttpException* e)
    {
        m_response->sendException(e);
        e->Release();
        return 0;
    }
    catch (IException *e)
    {
        DBGLOG(e);
        e->Release();
        return 0;
    }
    catch (...)
    {
        DBGLOG("Unknown Exception - reading request [CEspHttpServer::processRequest()]");
        return 0;
    }

    try
    {
        
        EspHttpBinding* thebinding=NULL;
        
        StringBuffer method;
        m_request->getMethod(method);

        EspAuthState authState=authUnknown;
        sub_service stype=sub_serv_unknown;
        
        StringBuffer pathEx;
        StringBuffer serviceName;
        StringBuffer methodName;
        m_request->getEspPathInfo(stype, &pathEx, &serviceName, &methodName, false);
        ESPLOG(LogNormal,"sub service type: %s. parm: %s", getSubServiceDesc(stype), m_request->queryParamStr());

        m_request->updateContext();
        IEspContext* ctx = m_request->queryContext();
        ctx->setServiceName(serviceName.str());

        bool isSoapPost=(stricmp(method.str(), POST_METHOD) == 0 && m_request->isSoapMessage());
        if (!isSoapPost)
        {
            StringBuffer peerStr, pathStr;
            const char *userid=ctx->queryUserId();
            DBGLOG("%s %s, from %s@%s", method.str(), m_request->getPath(pathStr).str(), (userid) ? userid : "unknown", m_request->getPeer(peerStr).str());

            if (m_apport->rootAuthRequired() && (!ctx->queryUserId() || !*ctx->queryUserId()))
            {
                thebinding = dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());
                StringBuffer realmbuf;
                if(thebinding)
                {   
                    realmbuf.append(thebinding->getChallengeRealm());
                }

                if(realmbuf.length() == 0)
                    realmbuf.append("ESP");
                DBGLOG("User authentication required");
                m_response->sendBasicChallenge(realmbuf.str(), true);
                return 0;
            }
        }

        if (!stricmp(method.str(), GET_METHOD))
        {
            if (stype==sub_serv_root)
            {
                if (!rootAuth(ctx))
                    return 0;
                // authenticate optional groups
                if (authenticateOptionalFailed(*ctx,NULL))
                    throw createEspHttpException(401,"Unauthorized Access","Unauthorized Access");

                return onGetApplicationFrame(m_request.get(), m_response.get(), ctx);
            }

            if (!stricmp(serviceName.str(), "esp"))
            {
                if (!methodName.length())
                    return 0;
                if (!rootAuth(ctx))
                    return 0;
                if (methodName.charAt(methodName.length()-1)=='_')
                    methodName.setCharAt(methodName.length()-1, 0);
                if (!stricmp(methodName.str(), "files"))
                    return onGetFile(m_request.get(), m_response.get(), pathEx.str());
                else if (!stricmp(methodName.str(), "xslt"))
                    return onGetXslt(m_request.get(), m_response.get(), pathEx.str());
                else if (!stricmp(methodName.str(), "body"))
                    return onGetMainWindow(m_request.get(), m_response.get());
                else if (!stricmp(methodName.str(), "frame"))
                    return onGetApplicationFrame(m_request.get(), m_response.get(), ctx);
                else if (!stricmp(methodName.str(), "titlebar"))
                    return onGetTitleBar(m_request.get(), m_response.get());
                else if (!stricmp(methodName.str(), "nav"))
                    return onGetNavWindow(m_request.get(), m_response.get());
                else if (!stricmp(methodName.str(), "navdata"))
                    return onGetDynNavData(m_request.get(), m_response.get());
                else if (!stricmp(methodName.str(), "navmenuevent"))
                    return onGetNavEvent(m_request.get(), m_response.get());
                else if (!stricmp(methodName.str(), "soapreq"))
                    return onGetBuildSoapRequest(m_request.get(), m_response.get());
#ifdef _USE_OPENLDAP
                else if (strieq(methodName.str(), "updatepasswordinput"))
                    return onUpdatePasswordInput(m_request.get(), m_response.get());
#endif
            }
        }
#ifdef _USE_OPENLDAP
        else if (strieq(method.str(), POST_METHOD) && strieq(serviceName.str(), "esp") && (methodName.length() > 0) && strieq(methodName.str(), "updatepassword"))
        {
            if (!rootAuth(ctx))
                return 0;
            return onUpdatePassword(m_request.get(), m_response.get());
        }
#endif

        if(m_apport != NULL)
        {
            int ordinality=m_apport->getBindingCount();
            bool isSubService = false;
            if (ordinality>0)
            {
                if (ordinality==1)
                {
                    CEspBindingEntry *entry = m_apport->queryBindingItem(0);
                    thebinding = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;

                    if (thebinding && !isSoapPost && !thebinding->isValidServiceName(*ctx, serviceName.str()))
                        thebinding=NULL;
                }
                else
                {
                    EspHttpBinding* lbind=NULL;
                    for(int index=0; !thebinding && index<ordinality; index++)
                    {
                        CEspBindingEntry *entry = m_apport->queryBindingItem(index);
                        lbind = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
                        if (lbind)
                        {
                            if (!thebinding && lbind->isValidServiceName(*ctx, serviceName.str()))
                            {
                                thebinding=lbind;
                                StringBuffer bindSvcName;
                                if (stricmp(serviceName, lbind->getServiceName(bindSvcName)))
                                    isSubService = true;
                            }                           
                        }
                    }
                }
                if (!thebinding && m_defaultBinding)
                    thebinding=dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());
                if (thebinding)
                {
                    StringBuffer servName(ctx->queryServiceName(NULL));
                    if (!servName.length())
                    {
                        thebinding->getServiceName(servName);
                        ctx->setServiceName(servName.str());
                    }
                    
                    thebinding->populateRequest(m_request.get());
                    if(thebinding->authRequired(m_request.get()) && !thebinding->doAuth(ctx))
                    {
                        authState=authRequired;
                        if(isSoapPost)
                        {
                            authState = authPending;
                            ctx->setToBeAuthenticated(true);
                        }
                    }
                    else
                        authState = authSucceeded;
                }
            }
                    
            if (authState==authRequired)
            {
                ISecUser *user = ctx->queryUser();
                if (user && user->getAuthenticateStatus() == AS_PASSWORD_EXPIRED)
                {
                    DBGLOG("ESP password expired for %s", user->getName());
                    m_response->setContentType(HTTP_TYPE_TEXT_PLAIN);
                    m_response->setContent("Your ESP password has expired");
                    m_response->send();
                }
                else
                {
                    DBGLOG("User authentication required");
                    StringBuffer realmbuf;
                    if(thebinding)
                        realmbuf.append(thebinding->getChallengeRealm());
                    if(realmbuf.length() == 0)
                        realmbuf.append("ESP");
                    m_response->sendBasicChallenge(realmbuf.str(), !isSoapPost);
                }
                return 0;
            }

            // authenticate optional groups
            if (authenticateOptionalFailed(*ctx,thebinding))
                throw createEspHttpException(401,"Unauthorized Access","Unauthorized Access");

            if (thebinding!=NULL)
            {
                if(stricmp(method.str(), POST_METHOD)==0)
                    thebinding->handleHttpPost(m_request.get(), m_response.get());
                else if(!stricmp(method.str(), GET_METHOD)) 
                {
                    if (stype==sub_serv_index_redirect)
                    {
                        StringBuffer url;
                        if (isSubService) 
                        {
                            StringBuffer qSvcName;
                            thebinding->qualifySubServiceName(*ctx,serviceName,NULL, qSvcName, NULL);
                            url.append(qSvcName);
                        }
                        else
                            thebinding->getServiceName(url);
                        url.append('/');
                        const char* parms = m_request->queryParamStr();
                        if (parms && *parms)
                            url.append('?').append(parms);
                        m_response->redirect(*m_request.get(),url);
                    }
                    else
                        thebinding->onGet(m_request.get(), m_response.get());
                }
                else
                    unsupported();
            }
            else
            {
                if(!stricmp(method.str(), POST_METHOD))
                    onPost();
                else if(!stricmp(method.str(), GET_METHOD))
                    onGet();
                else
                    unsupported();
            }
            ctx->addTraceSummaryTimeStamp("handleHttp");
        }
    }
    catch(IEspHttpException* e)
    {
        m_response->sendException(e);
        e->Release();
        return 0;
    }
    catch (IException *e)
    {
        DBGLOG(e);
        e->Release();
        return 0;
    }
    catch (...)
    {
        StringBuffer content_type;
        unsigned len = m_request->getContentLength();
        DBGLOG("Unknown Exception - processing request");
        DBGLOG("METHOD: %s, PATH: %s, TYPE: %s, CONTENT-LENGTH: %d", m_request->queryMethod(), m_request->queryPath(), m_request->getContentType(content_type).str(), len);
        if (len)
            m_request->logMessage(LOGCONTENT, "HTTP request content received:\n");
        return 0;
    }

    return 0;
}

int CEspHttpServer::onGetApplicationFrame(CHttpRequest* request, CHttpResponse* response, IEspContext* ctx)
{
    time_t modtime = 0;

    IProperties *params = request->queryParameters();
    const char *inner=(params)?params->queryProp("inner") : NULL;
    StringBuffer ifmodifiedsince;
    request->getHeader("If-Modified-Since", ifmodifiedsince);

    if (inner&&*inner&&ifmodifiedsince.length())
    {
        response->setStatus(HTTP_STATUS_NOT_MODIFIED);
        response->send();
    }
    else
    {
        StringBuffer html;
        m_apport->getAppFrameHtml(modtime, inner, html, ctx);
        response->setContent(html.length(), html.str());
        response->setContentType("text/html; charset=UTF-8");
        response->setStatus(HTTP_STATUS_OK);

        const char *timestr=ctime(&modtime);
        response->addHeader("Last-Modified", timestr);
        response->send();
    }

    return 0;
}

int CEspHttpServer::onGetTitleBar(CHttpRequest* request, CHttpResponse* response)
{
    bool rawXml = request->queryParameters()->hasProp("rawxml_");
    StringBuffer m_headerHtml(m_apport->getTitleBarHtml(*request->queryContext(), rawXml));
    response->setContent(m_headerHtml.length(), m_headerHtml.str());
    response->setContentType(rawXml ? HTTP_TYPE_APPLICATION_XML_UTF8 : "text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return 0;
}

int CEspHttpServer::onGetNavWindow(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer navContent;
    StringBuffer navContentType;
    m_apport->getNavBarContent(*request->queryContext(), navContent, navContentType, request->queryParameters()->hasProp("rawxml_"));
    response->setContent(navContent.length(), navContent.str());
    response->setContentType(navContentType.str());
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return 0;
}

int CEspHttpServer::onGetDynNavData(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer navContent;
    StringBuffer navContentType;
    bool         bVolatile;
    m_apport->getDynNavData(*request->queryContext(), request->queryParameters(), navContent, navContentType, bVolatile);
    if (bVolatile)
        response->addHeader("Cache-control",  "max-age=0");
    response->setContent(navContent.length(), navContent.str());
    response->setContentType(navContentType.str());
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return 0;
}

int CEspHttpServer::onGetNavEvent(CHttpRequest* request, CHttpResponse* response)
{
    m_apport->onGetNavEvent(*request->queryContext(), request, response);
    return 0;
}

int CEspHttpServer::onGetBuildSoapRequest(CHttpRequest* request, CHttpResponse* response)
{
    m_apport->onBuildSoapRequest(*request->queryContext(), request, response);
    return 0;
}

#ifdef _USE_OPENLDAP
int CEspHttpServer::onUpdatePasswordInput(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer html;
    m_apport->onUpdatePasswordInput(*request->queryContext(), html);
    response->setContent(html.length(), html.str());
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);

    response->send();

    return 0;
}

int CEspHttpServer::onUpdatePassword(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer html;
    m_apport->onUpdatePassword(*request->queryContext(), request, html);
    response->setContent(html.length(), html.str());
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);

    response->send();
    return 0;
}
#endif

int CEspHttpServer::onGetMainWindow(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer url("../?main");
    double ver = request->queryContext()->getClientVersion();
    if (ver>0)
        url.appendf("&ver_=%g", ver);
    response->redirect(*request, url);
    return 0;
}

inline void make_env_var(StringArray &env, StringBuffer &var, const char *name, const char *value)
{
    env.append(var.clear().append(name).append('=').append(value).str());
}

inline void make_env_var(StringArray &env, StringBuffer &var, const char *name, const StringBuffer &value)
{
    env.append(var.clear().append(name).append('=').append(value).str());
}

inline void make_env_var(StringArray &env, StringBuffer &var, const char *name, int value)
{
    env.append(var.clear().append(name).append('=').append(value).str());
}

bool skipHeader(const char *name)
{
    if (!stricmp(name, "CONTENT_LENGTH"))
        return true;
    else if (!strcmp(name, "AUTHORIZATION"))
        return true;
    else if (!strcmp(name, "CONTENT_TYPE"))
        return true;
    return false;
}

typedef enum _cgi_resp_state
{
    cgi_resp_hname,
    cgi_resp_hval,
    cgi_resp_body
} cgi_resp_state;


int CEspHttpServer::onRunCGI(CHttpRequest* request, CHttpResponse* response, const char *path)
{
    char cwd[1024];
    if (!GetCurrentDirectory(1024, cwd)) {
        ERRLOG("onRunCGI: Current directory path too big, setting local path to null");
        cwd[0] = 0;
    }
    StringBuffer docRoot(cwd);
    docRoot.append("/files");
    StringBuffer script(docRoot);
    script.append("/").append(path);

    StringArray env;
    StringBuffer var;

    make_env_var(env, var, "SERVER_SOFTWARE", "ESP/1.0");
    make_env_var(env, var, "GATEWAY_INTERFACE", "CGI/1.1");
    make_env_var(env, var, "SERVER_PROTOCOL", "HTTP/1.0");
    make_env_var(env, var, "DOCUMENT_ROOT", docRoot);
    make_env_var(env, var, "SCRIPT_FILENAME", script);
    make_env_var(env, var, "REQUEST_METHOD", request->queryMethod());
    make_env_var(env, var, "PATH", getenv("PATH"));
    make_env_var(env, var, "QUERY_STRING", request->queryParamStr());
    make_env_var(env, var, "REDIRECT_STATUS", 1);

    ISocket *sock=request->getSocket();
    if (sock)
    {
        char sname[512]={0};
        int sport = sock->name(sname, 512);
        SocketEndpoint ep;
        sock->getPeerEndpoint(ep);
        StringBuffer ipstr;
        ep.getIpText(ipstr);
        make_env_var(env, var, "REMOTE_ADDRESS", ipstr.str());
        make_env_var(env, var, "REMOTE_PORT", ep.port);
        make_env_var(env, var, "SERVER_PORT", sport);
    }

    IEspContext *ctx=request->queryContext();
    if (ctx)
    {
        StringBuffer userstr(ctx->queryUserId());
        if (userstr.length())
            make_env_var(env, var, "REMOTE_USER", userstr);
    }
    make_env_var(env, var, "REQUEST_URI", request->queryPath());
    make_env_var(env, var, "SCRIPT_NAME", request->queryPath());
    

    StringBuffer hostIpStr;
    queryHostIP().getIpText(hostIpStr);
    
    make_env_var(env, var, "SERVER_ADDR", hostIpStr);
    make_env_var(env, var, "SERVER_NAME", hostIpStr);
    
    if (!stricmp(request->queryMethod(), "POST"))
    {
        if (request->getContentLength())
        {
            StringBuffer type;
            make_env_var(env, var, "CONTENT_TYPE", request->getContentType(type).str());
            make_env_var(env, var, "CONTENT_LENGTH", request->getContentLength());
        }
    }

    make_env_var(env, var, "SCRIPT_FILENAME", script);

    StringArray &headers=request->queryHeaders();
    ForEachItemIn(iheader, headers)
    {
        const char* header = headers.item(iheader);
        if (header)
        {
            const char* colon = strchr(header, ':');
            if(colon)
            {
                StringBuffer hname(colon-header, header);
                hname.toUpperCase().replace('-','_');
                if (!skipHeader(hname.str()))
                {
                    const char *finger=colon+1;
                    while (*finger==' ') finger++;
                    StringBuffer hstr;
                    hstr.append(("HTTP_")).append(hname).append('=').append(finger);
                    env.append(hstr.str());
                    DBGLOG("%s", hstr.str());
                }
            }
        }
    }
    
    make_env_var(env, var, "XXX", "222 Running the script => ");

    StringBuffer in;
    StringBuffer out;
    callExternalProgram("php-cgi.exe", in, out, &env);
    const char *start=out.str();
    cgi_resp_state rstate=cgi_resp_hname;

    StringBuffer hname;
    StringBuffer hval;
    const char *finger;
    for(finger=start; *finger!=0 && rstate!=cgi_resp_body; finger++)
    {
        switch (*finger)
        {
        case ':':
            if (rstate==cgi_resp_hname)
                rstate=cgi_resp_hval;
            else if (rstate==cgi_resp_hval)
                hval.append(':');
            break;
        case '\r':
            if (!strncmp(finger, "\r\n\r\n", 4))
            {
                finger+=3;
                rstate=cgi_resp_body;
            }
            else
            {
                if (hname.length() && hval.length())
                {
                    if (!stricmp(hname.str(), "content-type"))
                        response->setContentType(strchr(hval.str(), '/') ? hval.str() : "text/html");
                    else
                        response->setHeader(hname.str(), hval.str());
                }
                hname.clear();
                hval.clear();
                rstate=cgi_resp_hname;
            }
            break;

        case '\n':
            if (finger[1]=='\n')
            {
                finger++;
                rstate=cgi_resp_body;
            }
            else
            {
                if (hname.length() && hval.length())
                {
                    if (!stricmp(hname.str(), "content-type"))
                        response->setContentType(strchr(hval.str(), '/') ? hval.str() : "text/html");
                    else
                        response->setHeader(hname.str(), hval.str());
                }
                hname.clear();
                hval.clear();
                rstate=cgi_resp_hname;
            }
            break;
        default:
            if (rstate==cgi_resp_hname)
                hname.append(*finger);
            else if (rstate==cgi_resp_hval)
                hval.append(*finger);
            break;
        }
    }

    response->setContent(out.length()-(finger-start), finger);
    response->send();
    return 0;
}

int CEspHttpServer::onGetFile(CHttpRequest* request, CHttpResponse* response, const char *path)
{
        if (!request || !response || !path)
            return -1;
        
        int pathlen=strlen(path);
        if (pathlen>5 && !stricmp(path+pathlen-4, ".php"))
            return onRunCGI(request, response, path);
        
        StringBuffer mimetype;
        MemoryBuffer content;

        StringBuffer filepath(getCFD());
        filepath.append("files/");
        filepath.append(path);
        if (httpContentFromFile(filepath.str(), mimetype, content))
        {
            response->setContent(content.length(), content.toByteArray());
            response->setContentType(mimetype.str());

            //if file being requested is an image file then set its expiration to a year
            //so browser does not keep reloading it
            const char* pchExt = strrchr(path, '.');
            if (pchExt && (!stricmp(++pchExt, "gif") || !stricmp(pchExt, "jpg") || !stricmp(pchExt, "png")))
                response->addHeader("Cache-control",  "max-age=31536000");
        }
        else
        {
            DBGLOG("Get File %s: file not found", filepath.str());
            response->setStatus(HTTP_STATUS_NOT_FOUND);
        }
        response->send();
        return 0;
}

int CEspHttpServer::onGetXslt(CHttpRequest* request, CHttpResponse* response, const char *path)
{
        if (!request || !response || !path)
            return -1;
        
        StringBuffer mimetype;
        MemoryBuffer content;

        StringBuffer filepath;
        if (httpContentFromFile(filepath.append(getCFD()).append("smc_xslt/").append(path).str(), mimetype, content) || httpContentFromFile(filepath.clear().append(getCFD()).append("xslt/").append(path).str(), mimetype, content))
        {
            response->setContent(content.length(), content.toByteArray());
            response->setContentType(mimetype.str());
        }
        else
        {
            DBGLOG("Get XSLT %s: file not found", filepath.str());
            response->setStatus(HTTP_STATUS_NOT_FOUND);
        }

        response->send();
        return 0;
}


int CEspHttpServer::unsupported()
{
    HtmlPage page("Enterprise Services Platform");
    StringBuffer espHeader;

    espHeader.append("<table border=\"0\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" bgcolor=\"#000000\" height=\"108\">");
    espHeader.append("<tr><td width=\"24%\" height=\"24\" bgcolor=\"#000000\"><img border=\"0\" src=\"esp/files_/logo.gif\" width=\"258\" height=\"108\" /></td></tr>");
    espHeader.append("<tr><td width=\"24%\" height=\"24\" bgcolor=\"#AA0000\"><p align=\"center\" /><b><font color=\"#FFFFFF\" size=\"5\">Enterprise Services Platform</font></b></td></tr>");
    espHeader.append("</table>");

    page.appendContent(new CHtmlText(espHeader.str()));

    page.appendContent(new CHtmlHeader(H1, "Unsupported http method"));

    StringBuffer content;
    page.getHtml(content);

    m_response->setVersion(HTTP_VERSION);
    m_response->setContent(content.length(), content.str());
    m_response->setContentType("text/html; charset=UTF-8");
    m_response->setStatus(HTTP_STATUS_OK);

    m_response->send();

    return 0;
}

int CEspHttpServer::onPost()
{
    HtmlPage page("Enterprise Services Platform");
    StringBuffer espHeader;

    espHeader.append("<table border=\"0\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" bgcolor=\"#000000\" height=\"108\">");
    espHeader.append("<tr><td width=\"24%\" height=\"24\" bgcolor=\"#000000\"><img border=\"0\" src=\"esp/files_/logo.gif\" width=\"258\" height=\"108\" /></td></tr>");
    espHeader.append("<tr><td width=\"24%\" height=\"24\" bgcolor=\"#AA0000\"><p align=\"center\" /><b><font color=\"#FFFFFF\" size=\"5\">Enterprise Services Platform</font></b></td></tr>");
    espHeader.append("</table>");

    page.appendContent(new CHtmlText(espHeader.str()));

    page.appendContent(new CHtmlHeader(H1, "Invalid POST"));

    StringBuffer content;
    page.getHtml(content);

    m_response->setVersion(HTTP_VERSION);
    m_response->setContent(content.length(), content.str());
    m_response->setContentType("text/html; charset=UTF-8");
    m_response->setStatus(HTTP_STATUS_OK);

    m_response->send();

    return 0;
}

int CEspHttpServer::onGet()
{   
    if (m_request && m_request->queryParameters()->hasProp("config_") && m_viewConfig)
    {
        StringBuffer mimetype;
        MemoryBuffer content;
        httpContentFromFile("esp.xml", mimetype, content);

        m_response->setVersion(HTTP_VERSION);
        m_response->setContent(content.length(), content.toByteArray());
        m_response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
        m_response->setStatus(HTTP_STATUS_OK);
        m_response->send();
    }
    else
    {
        HtmlPage page("Enterprise Services Platform");
        page.appendContent(new CHtmlHeader(H1, "Available Services:"));

        CHtmlList * list = (CHtmlList *)page.appendContent(new CHtmlList);
        EspHttpBinding* lbind=NULL;
        int ordinality=m_apport->getBindingCount();

        double ver = m_request->queryContext()->getClientVersion();
        for(int index=0; index<ordinality; index++)
        {
            CEspBindingEntry *entry = m_apport->queryBindingItem(index);
            lbind = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
            if (lbind)
            {
                StringBuffer srv, srvLink;
                lbind->getServiceName(srv);
                srvLink.appendf("/%s", srv.str());
                if (ver)
                    srvLink.appendf("?ver_=%g", ver);
                
                list->appendContent(new CHtmlLink(srv.str(), srvLink.str()));
            }
        }

        StringBuffer content;
        page.getHtml(content);

        m_response->setVersion(HTTP_VERSION);
        m_response->setContent(content.length(), content.str());
        m_response->setContentType("text/html; charset=UTF-8");
        m_response->setStatus(HTTP_STATUS_OK);

        m_response->send();
    }

    return 0;
}

