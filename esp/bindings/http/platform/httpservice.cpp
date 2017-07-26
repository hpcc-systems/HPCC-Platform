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

#pragma warning (disable : 4786)

#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl DECL_EXPORT
#else
    #define esp_http_decl DECL_IMPORT
#endif
//Jlib
#include "jliball.hpp"

#include "espcontext.hpp"
#include "esphttp.hpp"

//ESP Bindings
#include "http/platform/httpsecurecontext.hpp"
#include "http/platform/httpservice.hpp"
#include "http/platform/httptransport.hpp"

#include "htmlpage.hpp"
#include "dasds.hpp"

/***************************************************************************
 *              CEspHttpServer Implementation
 ***************************************************************************/

CEspHttpServer::CEspHttpServer(ISocket& sock, CEspApplicationPort* apport, bool viewConfig, int maxRequestEntityLength):m_socket(sock), m_MaxRequestEntityLength(maxRequestEntityLength)
{
    m_request.setown(new CHttpRequest(sock));
    IEspContext* ctx = createEspContext(createHttpSecureContext(m_request.get()));
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

                DBGLOG("Request[%s]", logStr.str());
                if (m_request->isSoapMessage())
                {
                    StringBuffer requestStr;
                    m_request->getContent(requestStr);
                    if (requestStr.length())
                        m_request->logSOAPMessage(requestStr.str(), NULL);
                }
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

bool CEspHttpServer::rootAuth(IEspContext* ctx)
{
    if (!m_apport->rootAuthRequired())
        return true;

    bool ret=false;
    EspHttpBinding* thebinding=getBinding();
    if (thebinding)
    {
        thebinding->populateRequest(m_request.get());
        if(!thebinding->authRequired(m_request.get()) || thebinding->doAuth(ctx))
            ret=true;
        else
        {
            ISecUser *user = ctx->queryUser();
            if (user && user->getAuthenticateStatus() == AS_PASSWORD_VALID_BUT_EXPIRED)
            {
                m_response->redirect(*m_request.get(), "/esp/updatepasswordinput");
                ret = true;
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
    DEF_CASE(sub_serv_getversion)
    DEF_CASE(sub_serv_reqsamplexml)
    DEF_CASE(sub_serv_respsamplexml)
    DEF_CASE(sub_serv_respsamplejson)
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

EspHttpBinding* CEspHttpServer::getBinding()
{
    EspHttpBinding* thebinding=NULL;
    int ordinality=m_apport->getBindingCount();
    if (ordinality==1)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(0);
        thebinding = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
    }
    else if (m_defaultBinding)
        thebinding=dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());
    return thebinding;
}

//CORS allow headers for interoperability, we do not rely on this for security since
//that only means treating the browser as a trusted entity.  We need to be diligent and secure
//for every request whether it comes from a cross domain browser or any other source

void checkSetCORSAllowOrigin(CHttpRequest *req, CHttpResponse *resp)
{
    StringBuffer origin;
    req->getHeader("Origin", origin);
    if (origin.length())
        resp->setHeader("Access-Control-Allow-Origin", "*");
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
        ctx->setHTTPMethod(method.str());
        ctx->setServiceMethod(methodName.str());

        StringBuffer peerStr, pathStr;
        const char *userid=ctx->queryUserId();
        ESPLOG(LogMin, "%s %s, from %s@%s", method.str(), m_request->getPath(pathStr).str(), (userid) ? userid : "unknown", m_request->getPeer(peerStr).str());

        authState = checkUserAuth();
        if ((authState == authTaskDone) || (authState == authFailed))
            return 0;

        if (!stricmp(method.str(), GET_METHOD))
        {
            if (stype==sub_serv_root)
            {
                return onGetApplicationFrame(m_request.get(), m_response.get(), ctx);
            }

            if (!stricmp(serviceName.str(), "esp"))
            {
                if (!methodName.length())
                    return 0;

                if (methodName.charAt(methodName.length()-1)=='_')
                    methodName.setCharAt(methodName.length()-1, 0);
                if (!stricmp(methodName.str(), "files"))
                {
                    if (!getTxSummaryResourceReq())
                        ctx->cancelTxSummary();
                    checkInitEclIdeResponse(m_request, m_response);
                    return onGetFile(m_request.get(), m_response.get(), pathEx.str());
                }
                else if (!stricmp(methodName.str(), "xslt"))
                {
                    if (!getTxSummaryResourceReq())
                        ctx->cancelTxSummary();
                    return onGetXslt(m_request.get(), m_response.get(), pathEx.str());
                }
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
            }
        }

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

                    bool isSoapPost=(strieq(method.str(), POST_METHOD) && m_request->isSoapMessage());
                    if (thebinding && !isSoapPost && !thebinding->isValidServiceName(*ctx, serviceName.str()))
                        thebinding=NULL;
                }
                else
                {
                    EspHttpBinding* lbind=NULL;
                    for (int index=0; !thebinding && index<ordinality; index++)
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
            }

            if(strieq(method.str(), OPTIONS_METHOD))
                return onOptions();

            checkSetCORSAllowOrigin(m_request, m_response);

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
            ctx->addTraceSummaryTimeStamp(LogMin, "handleHttp");
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
        __int64 len = m_request->getContentLength();
        DBGLOG("Unknown Exception - processing request");
        DBGLOG("METHOD: %s, PATH: %s, TYPE: %s, CONTENT-LENGTH: %" I64F "d", m_request->queryMethod(), m_request->queryPath(), m_request->getContentType(content_type).str(), len);
        if (len > 0)
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
        CEspBindingEntry* entry = m_apport->getDefaultBinding();
        if(entry)
        {
            EspHttpBinding *httpbind = dynamic_cast<EspHttpBinding *>(entry->queryBinding());
            if(httpbind)
            {
                const char *page = httpbind->getRootPage(ctx);
                if(page && *page)
                    return onGetFile(request, response, page);
            }
        }

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
    unsigned returnCode = m_apport->onUpdatePassword(*request->queryContext(), request, html);
    if (returnCode == 0)
    {
        EspHttpBinding* binding = getBinding();
        StringBuffer authorizationHeader;
        request->getHeader("Authorization", authorizationHeader);
        //If the request has the "Authorization" header, the request is from the AuthPerRequest type.
        if (binding && authorizationHeader.isEmpty())
        {
            AuthType domainAuthType = binding->getDomainAuthType();
            if ((domainAuthType == AuthPerSessionOnly) || (domainAuthType == AuthTypeMixed))
            {//A session can only be set for those 2 auth types.
                StringBuffer urlCookie;
                readCookie(SESSION_START_URL_COOKIE, urlCookie);
                unsigned sessionID = createHTTPSession(binding, request->getParameters()->queryProp("username"), urlCookie.isEmpty() ? "/" : urlCookie.str());
                m_request->queryContext()->setSessionToken(sessionID);
                VStringBuffer sessionIDStr("%u", sessionID);
                addCookie(binding->querySessionIDCookieName(), sessionIDStr.str(), binding->getSessionTimeoutSeconds());
                clearCookie(SESSION_START_URL_COOKIE);
            }
        }
    }
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

inline void make_env_var(StringArray &env, StringBuffer &var, const char *name, __int64 value)
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

static void httpGetFile(CHttpRequest* request, CHttpResponse* response, const char *urlpath, const char *filepath)
{
    StringBuffer mimetype, etag, lastModified;
    MemoryBuffer content;
    bool modified = true;
    request->getHeader("If-None-Match", etag);
    request->getHeader("If-Modified-Since", lastModified);

    if (httpContentFromFile(filepath, mimetype, content, modified, lastModified, etag))
    {
        response->CheckModifiedHTTPContent(modified, lastModified.str(), etag.str(), mimetype.str(), content);
    }
    else
    {
        DBGLOG("Get File %s: file not found", filepath);
        response->setStatus(HTTP_STATUS_NOT_FOUND);
    }
    response->send();
}

static void httpGetDirectory(CHttpRequest* request, CHttpResponse* response, const char *urlpath, const char *dirpath, bool top, const StringBuffer &tail)
{
    Owned<IPropertyTree> tree = createPTree("directory", ipt_none);
    tree->setProp("@path", urlpath);
    Owned<IDirectoryIterator> dir = createDirectoryIterator(dirpath, NULL);
    ForEach(*dir)
    {
        IPropertyTree *entry = tree->addPropTree(dir->isDir() ? "directory" : "file", createPTree(ipt_none));
        StringBuffer s;
        entry->setProp("name", dir->getName(s));
        if (!dir->isDir())
            entry->setPropInt64("size", dir->getFileSize());
        CDateTime cdt;
        dir->getModifiedTime(cdt);
        entry->setProp("modified", cdt.getString(s.clear(), false));
    }

    const char *fmt = request->queryParameters()->queryProp("format");
    StringBuffer out;
    StringBuffer contentType;
    if (!fmt || strieq(fmt,"html"))
    {
        contentType.set("text/html");
        out.append("<!DOCTYPE html><html><body>");
        if (!top)
            out.appendf("<a href='%s'>..</a><br/>", tail.length() ? "." : "..");

        Owned<IPropertyTreeIterator> it = tree->getElements("*");
        ForEach(*it)
        {
            IPropertyTree &e = it->query();
            const char *href=e.queryProp("name");
            if (tail.length())
                out.appendf("<a href='%s/%s'>%s</a><br/>", tail.str(), href, href);
            else
                out.appendf("<a href='%s'>%s</a><br/>", href, href);
        }
        out.append("</body></html>");
    }
    else if (strieq(fmt, "json"))
    {
        contentType.set("application/json");
        toJSON(tree, out);
    }
    else if (strieq(fmt, "xml"))
    {
        contentType.set("application/xml");
        toXML(tree, out);
    }
    response->setStatus(HTTP_STATUS_OK);
    response->setContentType(contentType);
    response->setContent(out);
    response->send();
}

static bool checkHttpPathStaysWithinBounds(const char *path)
{
    if (!path || !*path)
        return true;
    int depth = 0;
    StringArray nodes;
    nodes.appendList(path, "/");
    ForEachItemIn(i, nodes)
    {
        const char *node = nodes.item(i);
        if (!*node || streq(node, ".")) //empty or "." doesn't advance
            continue;
        if (!streq(node, ".."))
            depth++;
        else
        {
            depth--;
            if (depth<0)  //only really care that the relative http path doesn't position itself above its own root node
                return false;
        }
    }
    return true;
}

int CEspHttpServer::onGetFile(CHttpRequest* request, CHttpResponse* response, const char *urlpath)
{
        if (!request || !response || !urlpath)
            return -1;

        StringBuffer basedir(getCFD());
        basedir.append("files/");

        if (!checkHttpPathStaysWithinBounds(urlpath))
        {
            DBGLOG("Get File %s: attempted access outside of %s", urlpath, basedir.str());
            response->setStatus(HTTP_STATUS_NOT_FOUND);
            response->send();
            return 0;
        }

        StringBuffer ext;
        StringBuffer tail;
        splitFilename(urlpath, NULL, NULL, &tail, &ext);

        bool top = !urlpath || !*urlpath;
        StringBuffer httpPath;
        request->getPath(httpPath).str();
        if (httpPath.charAt(httpPath.length()-1)=='/')
            tail.clear();
        else if (top)
            tail.set("./files");

        StringBuffer fullpath;
        makeAbsolutePath(urlpath, basedir.str(), fullpath);
        if (!checkFileExists(fullpath) && !checkFileExists(fullpath.toUpperCase()) && !checkFileExists(fullpath.toLowerCase()))
        {
            DBGLOG("Get File %s: file not found", urlpath);
            response->setStatus(HTTP_STATUS_NOT_FOUND);
            response->send();
            return 0;
        }

        if (isDirectory(fullpath))
            httpGetDirectory(request, response, urlpath, fullpath, top, tail);
        else
            httpGetFile(request, response, urlpath, fullpath);
        return 0;
}

int CEspHttpServer::onGetXslt(CHttpRequest* request, CHttpResponse* response, const char *path)
{
        if (!request || !response || !path)
            return -1;
        
        StringBuffer mimetype, etag, lastModified;
        MemoryBuffer content;
        bool modified = true;
        request->getHeader("If-None-Match", etag);
        request->getHeader("If-Modified-Since", lastModified);

        VStringBuffer filepath("%ssmc_xslt/%s", getCFD(), path);
        if (httpContentFromFile(filepath.str(), mimetype, content, modified, lastModified.clear(), etag) ||
            httpContentFromFile(filepath.clear().append(getCFD()).append("xslt/").append(path).str(), mimetype, content, modified, lastModified.clear(), etag))
        {
            response->CheckModifiedHTTPContent(modified, lastModified.str(), etag.str(), mimetype.str(), content);
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

int CEspHttpServer::onOptions()
{
    m_response->setVersion(HTTP_VERSION);
    m_response->setStatus(HTTP_STATUS_OK);

    //CORS allow headers for interoperability, we do not rely on this for security since
    //that only means treating the browser as a trusted entity.  We need to be diligent and secure
    //for every request whether it comes from a cross domain browser or any other source
    StringBuffer allowHeaders;
    m_request->getHeader("Access-Control-Request-Headers", allowHeaders);
    if (allowHeaders.length())
        m_response->setHeader("Access-Control-Allow-Headers", allowHeaders);
    m_response->setHeader("Access-Control-Allow-Origin", "*");
    m_response->setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
    m_response->setHeader("Access-Control-Max-Age", "86400"); //arbitrary 24 hours
    m_response->setContentType("text/plain");
    m_response->setContent("");

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
        StringBuffer mimetype, etag, lastModified;
        MemoryBuffer content;
        bool modified = true;
        m_request->getHeader("If-None-Match", etag);
        m_request->getHeader("If-Modified-Since", lastModified);
        httpContentFromFile("esp.xml", mimetype, content, modified, lastModified, etag);
        m_response->setVersion(HTTP_VERSION);
        m_response->CheckModifiedHTTPContent(modified, lastModified.str(), etag.str(), HTTP_TYPE_APPLICATION_XML_UTF8, content);
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

EspAuthState CEspHttpServer::checkUserAuth()
{
    EspAuthRequest authReq;
    readAuthRequest(authReq);
    if (authReq.httpPath.isEmpty())
        throw MakeStringException(-1, "URL query string cannot be empty.");

    if (!authReq.authBinding)
        throw MakeStringException(-1, "Cannot find ESP HTTP Binding");

    ESPLOG(LogMax, "checkUserAuth: %s %s", m_request->isSoapMessage() ? "SOAP" : "HTTP", authReq.httpMethod.isEmpty() ? "??" : authReq.httpMethod.str());

    //The preCheckAuth() does not return authUnknown when:
    //No authentication is required for the ESP binding;
    //Or no authentication is required for certain situations of not rootAuthRequired();
    //Or a user is trying to access some resources for displaying login/logout pages;
    //Or this is a user request for updating password.
    EspAuthState authState = preCheckAuth(authReq);
    if (authState != authUnknown)
        return authState;

    StringBuffer authorizationHeader;
    m_request->getHeader("Authorization", authorizationHeader);

    StringBuffer servName(authReq.ctx->queryServiceName(nullptr));
    if (servName.isEmpty())
    {
        authReq.authBinding->getServiceName(servName);
        authReq.ctx->setServiceName(servName.str());
    }

    AuthType domainAuthType = authReq.authBinding->getDomainAuthType();
    authReq.ctx->setDomainAuthType(domainAuthType);
    if (authorizationHeader.isEmpty() && domainAuthType != AuthPerRequestOnly)
    {//Try session based authentication now.
        EspAuthState authState = checkUserAuthPerSession(authReq);
        if (authState != authUnknown)
            return authState;
    }
    if (domainAuthType != AuthPerSessionOnly)
    {// BasicAuthentication or SOAP calls
        EspAuthState authState = checkUserAuthPerRequest(authReq);
        if (authState != authUnknown)
            return authState;
    }

    //HTTP authentication failed. Send out a login page or 401.
    bool authSession = (domainAuthType == AuthPerSessionOnly) || ((domainAuthType == AuthTypeMixed) && authorizationHeader.isEmpty());
    return handleAuthFailed(authSession, authReq);
}

//Read authentication related information into EspAuthRequest.
void CEspHttpServer::readAuthRequest(EspAuthRequest& req)
{
    StringBuffer pathEx;
    m_request->getEspPathInfo(req.stype, &pathEx, &req.serviceName, &req.methodName, false);
    m_request->getMethod(req.httpMethod);
    m_request->getPath(req.httpPath);//m_httpPath

    req.isSoapPost = (strieq(req.httpMethod.str(), POST_METHOD) && m_request->isSoapMessage());
    req.ctx = m_request->queryContext();
    req.authBinding = getEspHttpBinding(req);
    req.requestParams = m_request->queryParameters();
}

EspHttpBinding* CEspHttpServer::getEspHttpBinding(EspAuthRequest& authReq)
{
    if (strieq(authReq.httpMethod.str(), GET_METHOD) && ((authReq.stype == sub_serv_root)
            || (!authReq.serviceName.isEmpty() && strieq(authReq.serviceName.str(), "esp"))))
        return getBinding();

    if(!m_apport)
        return nullptr;

    int ordinality=m_apport->getBindingCount();
    if (ordinality < 1)
        return nullptr;

    EspHttpBinding* espHttpBinding = nullptr;
    if (ordinality==1)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(0);
        espHttpBinding = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
        //If there is only one binding on the port, we allow SOAP calls to work if they go
        //to http://IP:Port without any service name on the path. Even without specifying
        //the service, if the request matches a method, the method will run. So, the espHttpBinding
        //is set to nullptr only if !authReq.isSoapPost.
        if (!authReq.isSoapPost && espHttpBinding && !espHttpBinding->isValidServiceName(*authReq.ctx, authReq.serviceName.str()))
            espHttpBinding=nullptr;
        return espHttpBinding;
    }

    for (unsigned index=0; index<ordinality; index++)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(index);
        EspHttpBinding* lbind = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : nullptr;
        if (lbind && lbind->isValidServiceName(*authReq.ctx, authReq.serviceName.str()))
        {
            espHttpBinding=lbind;
            break;
        }
    }

    if (!espHttpBinding && m_defaultBinding)
        espHttpBinding=dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());

    return espHttpBinding;
}

EspAuthState CEspHttpServer::preCheckAuth(EspAuthRequest& authReq)
{
    if (!isAuthRequiredForBinding(authReq))
        return authSucceeded;

    if (!m_apport->rootAuthRequired() && strieq(authReq.httpMethod.str(), GET_METHOD) &&
        ((authReq.stype == sub_serv_root) || (!authReq.serviceName.isEmpty() && strieq(authReq.serviceName.str(), "esp"))))
        return authSucceeded;

#ifdef _USE_OPENLDAP
    if (!authReq.httpMethod.isEmpty() && !authReq.serviceName.isEmpty() && !authReq.methodName.isEmpty() && strieq(authReq.serviceName.str(), "esp"))
    {
        if (strieq(authReq.httpMethod.str(), POST_METHOD) && strieq(authReq.methodName.str(), "updatepassword"))
        {
            EspHttpBinding* thebinding = getBinding();
            if (thebinding)
                thebinding->populateRequest(m_request.get());
            onUpdatePassword(m_request.get(), m_response.get());
            return authTaskDone;
        }
        if (strieq(authReq.httpMethod.str(), GET_METHOD) && strieq(authReq.methodName.str(), "updatepasswordinput"))//process before authentication check
        {
            StringBuffer userID;
            readCookie(SESSION_ID_TEMP_COOKIE, userID);
            if (!userID.isEmpty()) //For session auth, the cookie has the userID.
                authReq.ctx->setUserID(userID.str());
            onUpdatePasswordInput(m_request.get(), m_response.get());
            return authTaskDone;
        }
    }
#endif

    if ((authReq.authBinding->getDomainAuthType() != AuthPerRequestOnly) && authReq.authBinding->isDomainAuthResources(authReq.httpPath.str()))
        return authSucceeded;//Give the permission to send out some pages used for login or logout.

    return authUnknown;
}

bool CEspHttpServer::isAuthRequiredForBinding(EspAuthRequest& authReq)
{
    IAuthMap* authmap = authReq.authBinding->queryAuthMAP();
    if (!authmap) //No auth requirement
        return false;

    const char* authMethod = authReq.authBinding->queryAuthMethod();
    if (isEmptyString(authMethod) || strieq(authMethod, "none"))
        return false;

    ISecResourceList* rlist = authmap->getResourceList(authReq.httpPath.str());
    if(!rlist) //No auth requirement for the httpPath.
        return false;

    authReq.ctx->setAuthenticationMethod(authMethod);
    authReq.ctx->setResources(rlist);

    return true;
}

EspAuthState CEspHttpServer::checkUserAuthPerSession(EspAuthRequest& authReq)
{
    ESPLOG(LogMax, "checkUserAuthPerSession");

    unsigned sessionID = readCookie(authReq.authBinding->querySessionIDCookieName());
    if (sessionID > 0)
        return authExistingSession(authReq, sessionID);//Check session based authentication using this session ID.

    StringBuffer urlCookie;
    readCookie(SESSION_START_URL_COOKIE, urlCookie);
    if (strieq(authReq.httpPath.str(), authReq.authBinding->queryLoginURL()))
    {//This is a request to ask for a login page.
        if (urlCookie.isEmpty())
            addCookie(SESSION_START_URL_COOKIE, "/", 0); //Will be redirected to / after authenticated.
        return authSucceeded;
    }

    if (authReq.serviceName.isEmpty() || authReq.methodName.isEmpty() || !strieq(authReq.serviceName.str(), "esp") || !strieq(authReq.methodName.str(), "login"))
        return authUnknown;

    const char* userName = (authReq.requestParams) ? authReq.requestParams->queryProp("username") : NULL;
    const char* password = (authReq.requestParams) ? authReq.requestParams->queryProp("password") : NULL;
    if (!isEmptyString(userName) && !isEmptyString(password))
        return authNewSession(authReq, userName, password, urlCookie.isEmpty() ? "/" : urlCookie.str());

    if (authReq.isSoapPost) //from SOAP Test page
        sendMessage("Authentication failed: empty user name or password.", "text/html; charset=UTF-8");
    else //from other page
        askUserLogin(authReq);
    return authFailed;
}

EspAuthState CEspHttpServer::checkUserAuthPerRequest(EspAuthRequest& authReq)
{
    ESPLOG(LogMax, "checkUserAuthPerRequest");

    authReq.authBinding->populateRequest(m_request.get());
    if (authReq.authBinding->doAuth(authReq.ctx))
    {//We do pass the authentication per the request
        // authenticate optional groups. Do we still need?
        authOptionalGroups(authReq);

        StringBuffer userName, peer;
        ESPLOG(LogNormal, "Authenticated for %s@%s", authReq.ctx->getUserID(userName).str(), m_request->getPeer(peer).str());
        return authSucceeded;
    }
    if (!authReq.isSoapPost)
        return authUnknown;

    //If SoapPost, username/password may be in soap:Header which is not in HTTP header.
    //The doAuth() may check them inside CSoapService::processHeader() later.
    authReq.ctx->setToBeAuthenticated(true);
    return authPending;
}

void CEspHttpServer::sendMessage(const char* msg, const char* msgType)
{
    if (!isEmptyString(msg))
        m_response->setContent(msg);
    m_response->setContentType(msgType);
    m_response->setStatus(HTTP_STATUS_OK);
    m_response->send();
}

EspAuthState CEspHttpServer::authNewSession(EspAuthRequest& authReq, const char* _userName, const char* _password, const char* sessionStartURL)
{
    StringBuffer peer;
    m_request->getPeer(peer);

    ESPLOG(LogMax, "authNewSession for %s@%s", _userName, peer.str());

    authReq.ctx->setUserID(_userName);
    authReq.ctx->setPassword(_password);
    authReq.authBinding->populateRequest(m_request.get());
    if (!authReq.authBinding->doAuth(authReq.ctx))
    {
        ESPLOG(LogMin, "Authentication failed for %s@%s", _userName, peer.str());
        return handleAuthFailed(true, authReq);
    }

    // authenticate optional groups
    authOptionalGroups(authReq);

    unsigned sessionID = createHTTPSession(authReq.authBinding, _userName, sessionStartURL);
    authReq.ctx->setSessionToken(sessionID);

    ESPLOG(LogMax, "Authenticated for %s@%s", _userName, peer.str());

    VStringBuffer sessionIDStr("%u", sessionID);
    addCookie(authReq.authBinding->querySessionIDCookieName(), sessionIDStr.str(), authReq.authBinding->getSessionTimeoutSeconds());
    clearCookie(SESSION_START_URL_COOKIE);
    m_response->redirect(*m_request, sessionStartURL);

    return authSucceeded;
}

void CEspHttpServer::createGetSessionTimeoutResponse(StringBuffer& resp, ESPSerializationFormat format, IPropertyTree* sessionTree)
{
    //The timeoutAt is a time stamp for when the session should be timed out.
    //The 0 is used to indicate that the session is already timed out.
    __int64 timeoutAt = sessionTree ? sessionTree->getPropInt64(PropSessionTimeoutAt, 0) : 0;
    bool timeoutByAdmin = sessionTree ? sessionTree->getPropBool(PropSessionTimeoutByAdmin, false) : false;
    if (format == ESPSerializationJSON)
    {
        resp.set("{ ");
        resp.append(" \"GetSessionTimeoutResponse\": { ");
        resp.appendf("\"TimeoutAt\": %lld", timeoutAt);
        if (timeoutByAdmin)
            resp.appendf(", \"TimeoutByAdmin\": true");
        resp.append(" }");
        resp.append(" }");
    }
    else
    {
        resp.set("<GetSessionTimeoutResponse>");
        resp.appendf("<TimeoutAt>%lld</TimeoutAt>", timeoutAt);
        if (timeoutByAdmin)
            resp.append("<TimeoutByAdmin>true</TimeoutByAdmin>");
        resp.append("</GetSessionTimeoutResponse>");
    }
}

EspAuthState CEspHttpServer::authExistingSession(EspAuthRequest& authReq, unsigned sessionID)
{
    ESPLOG(LogMax, "authExistingSession: %s<%u>", PropSessionID, sessionID);

    Owned<IRemoteConnection> conn = getSDSConnection(authReq.authBinding->queryESPSessionSDSPath(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    IPropertyTree* espSessions = conn->queryRoot();
    if (authReq.authBinding->getSessionTimeoutSeconds() >= 0)
    {
        CDateTime now;
        now.setNow();
        time_t timeNow = now.getSimple();
        if (timeNow - lastSessionCleanUpTime >= authReq.authBinding->getCheckSessionTimeoutSeconds())
        {
            lastSessionCleanUpTime = timeNow;
            timeoutESPSessions(authReq.authBinding, espSessions);
        }
    }

    VStringBuffer xpath("%s[@port=\"%d\"]/%s[%s='%u']", PathSessionApplication, authReq.authBinding->getPort(), PathSessionSession, PropSessionID, sessionID);
    IPropertyTree* sessionTree = espSessions->queryBranch(xpath.str());
    if (!authReq.serviceName.isEmpty() && !authReq.methodName.isEmpty() &&
        strieq(authReq.serviceName.str(), "esp") && strieq(authReq.methodName.str(), "get_session_timeout"))
    {
        StringBuffer content;
        ESPSerializationFormat respFormat = m_request->queryContext()->getResponseFormat();
        createGetSessionTimeoutResponse(content, respFormat, sessionTree);
        sendMessage(content.str(), (respFormat == ESPSerializationJSON) ? "application/json" : "text/xml");
        return authTaskDone;
    }

    if (!sessionTree)
    {
        ESPLOG(LogMin, "Authentication failed: session:<%u> not found", sessionID);
        if (authReq.isSoapPost) //from SOAP Test page
            sendMessage("Session expired. Please close this page and login again.", "text/html; charset=UTF-8");
        else
            askUserLogin(authReq);
        return authFailed;
    }

    authOptionalGroups(authReq);

    //The UserID has to be set before the populateRequest() because the UserID is used to create the user object.
    //After the user object is created, we may call addSessionToken().
    StringAttr userName = sessionTree->queryProp(PropSessionUserID);
    authReq.ctx->setUserID(userName.str());
    authReq.authBinding->populateRequest(m_request.get());
    authReq.ctx->setSessionToken(sessionID);

    ESPLOG(LogMax, "Authenticated for %s<%u> %s@%s", PropSessionID, sessionID, userName.str(), sessionTree->queryProp(PropSessionNetworkAddress));
    if (!authReq.serviceName.isEmpty() && !authReq.methodName.isEmpty() && strieq(authReq.serviceName.str(), "esp") && strieq(authReq.methodName.str(), "logout"))
        logoutSession(authReq, sessionID, espSessions);
    else
    {
        //The "ECLWatchAutoRefresh" returns a flag: '1' means that the request is generated by a UI auto refresh action and '0' means not.
        StringBuffer autoRefresh;
        m_request->getParameter("ECLWatchAutoRefresh", autoRefresh);

        CDateTime now;
        now.setNow();
        time_t createTime = now.getSimple();
        sessionTree->setPropInt64(PropSessionLastAccessed, createTime);
        if (!sessionTree->getPropBool(PropSessionTimeoutByAdmin, false) && (autoRefresh.isEmpty() || strieq(autoRefresh.str(), "0")))
        {
            time_t timeoutAt = createTime + authReq.authBinding->getSessionTimeoutSeconds();
            sessionTree->setPropInt64(PropSessionTimeoutAt, timeoutAt);
            ESPLOG(LogMin, "Updated %s for (/%s/%s) : %ld", PropSessionTimeoutAt, authReq.serviceName.isEmpty() ? "" : authReq.serviceName.str(),
                authReq.methodName.isEmpty() ? "" : authReq.methodName.str(), timeoutAt);
        }
        ///authReq.ctx->setAuthorized(true);
        VStringBuffer sessionIDStr("%u", sessionID);
        addCookie(authReq.authBinding->querySessionIDCookieName(), sessionIDStr.str(), authReq.authBinding->getSessionTimeoutSeconds());
    }

    return authSucceeded;
}

void CEspHttpServer::logoutSession(EspAuthRequest& authReq, unsigned sessionID, IPropertyTree* espSessions)
{
    //delete this session before logout
    VStringBuffer path("%s[@port=\"%d\"]", PathSessionApplication, authReq.authBinding->getPort());
    IPropertyTree* sessionTree = espSessions->queryBranch(path.str());
    if (sessionTree)
    {
        ICopyArrayOf<IPropertyTree> toRemove;
        path.setf("%s[%s='%d']", PathSessionSession, PropSessionID, sessionID);
        Owned<IPropertyTreeIterator> it = sessionTree->getElements(path.str());
        ForEach(*it)
            toRemove.append(it->query());
        ForEachItemIn(i, toRemove)
            sessionTree->removeTree(&toRemove.item(i));
    }
    else
        ESPLOG(LogMin, "Cann't find session tree: %s[@port=\"%d\"]", PathSessionApplication, authReq.authBinding->getPort());

    ///authReq.ctx->setAuthorized(true);

    clearCookie(authReq.authBinding->querySessionIDCookieName());
    const char* logoutURL = authReq.authBinding->queryLogoutURL();
    if (!isEmptyString(logoutURL))
        m_response->redirect(*m_request, authReq.authBinding->queryLogoutURL());
    else
        sendMessage(nullptr, "text/html; charset=UTF-8");
}

EspAuthState CEspHttpServer::handleAuthFailed(bool sessionAuth, EspAuthRequest& authReq)
{
    ISecUser *user = authReq.ctx->queryUser();
    if (user && user->getAuthenticateStatus() == AS_PASSWORD_VALID_BUT_EXPIRED)
    {
        ESPLOG(LogMin, "ESP password expired for %s. Asking update ...", authReq.ctx->queryUserId());
        if (sessionAuth) //For session auth, store the userid to cookie for the updatepasswordinput form.
            addCookie(SESSION_ID_TEMP_COOKIE, authReq.ctx->queryUserId(), 0);
        m_response->redirect(*m_request.get(), "/esp/updatepasswordinput");
        return authSucceeded;
    }

    if (user && (user->getAuthenticateStatus() == AS_PASSWORD_EXPIRED))
        ESPLOG(LogMin, "ESP password expired for %s", authReq.ctx->queryUserId());

    if (!sessionAuth)
    {
        ESPLOG(LogMin, "Authentication failed: send BasicAuthentication.");
        m_response->sendBasicChallenge(authReq.authBinding->getChallengeRealm(), true);
    }
    else
    {
        ESPLOG(LogMin, "Authentication failed: call askUserLogin.");
        askUserLogin(authReq);
    }
    return authFailed;
}

void CEspHttpServer::askUserLogin(EspAuthRequest& authReq)
{
    StringBuffer urlCookie;
    readCookie(SESSION_START_URL_COOKIE, urlCookie);
    if (urlCookie.isEmpty())
    {
        StringBuffer sessionStartURL = authReq.httpPath;
        if (authReq.requestParams && authReq.requestParams->hasProp("__querystring"))
            sessionStartURL.append("?").append(authReq.requestParams->queryProp("__querystring"));
        if (!sessionStartURL.isEmpty() && streq(sessionStartURL.str(), "/WsSMC/"))
            sessionStartURL.set("/");

        const char* loginURL = authReq.authBinding->queryLoginURL();
        if (strieq(loginURL, sessionStartURL))
            sessionStartURL.set("/");

        addCookie(SESSION_START_URL_COOKIE, sessionStartURL.str(), 0); //time out when browser is closed
    }
    m_response->redirect(*m_request, authReq.authBinding->queryLoginURL());
}

unsigned CEspHttpServer::createHTTPSession(EspHttpBinding* authBinding, const char* userID, const char* sessionStartURL)
{
    CDateTime now;
    now.setNow();
    time_t createTime = now.getSimple();

    StringBuffer peer, sessionIDStr;
    VStringBuffer idStr("%s_%ld", m_request->getPeer(peer).str(), createTime);
    unsigned sessionID = hashc((unsigned char *)idStr.str(), idStr.length(), 0);
    sessionIDStr.append(sessionID);

    VStringBuffer xpath("%s[%s='%u']", PathSessionSession, PropSessionID, sessionID);
    Owned<IRemoteConnection> conn = getSDSConnection(authBinding->querySessionSDSPath(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    IPropertyTree* domainSessions = conn->queryRoot();
    IPropertyTree* sessionTree = domainSessions->queryBranch(xpath.str());
    if (sessionTree)
    {
        sessionTree->setPropInt64(PropSessionLastAccessed, createTime);
        if (!sessionTree->getPropBool(PropSessionTimeoutByAdmin, false))
            sessionTree->setPropInt64(PropSessionTimeoutAt, createTime + authBinding->getSessionTimeoutSeconds());
        return sessionID;
    }
    ESPLOG(LogMax, "New sessionID <%d> at <%ld> in createHTTPSession()", sessionID, createTime);

    IPropertyTree* ptree = domainSessions->addPropTree(PathSessionSession);
    ptree->setProp(PropSessionNetworkAddress, peer.str());
    ptree->setPropInt64(PropSessionID, sessionID);
    ptree->setPropInt64(PropSessionExternalID, hashc((unsigned char *)sessionIDStr.str(), sessionIDStr.length(), 0));
    ptree->setProp(PropSessionUserID, userID);
    ptree->setPropInt64(PropSessionCreateTime, createTime);
    ptree->setPropInt64(PropSessionLastAccessed, createTime);
    ptree->setPropInt64(PropSessionTimeoutAt, createTime + authBinding->getSessionTimeoutSeconds());
    ptree->setProp(PropSessionLoginURL, sessionStartURL);
    return sessionID;
}

void CEspHttpServer::timeoutESPSessions(EspHttpBinding* authBinding, IPropertyTree* espSessions)
{
    //Removing HTTPSessions if timed out
    CDateTime now;
    now.setNow();
    time_t timeNow = now.getSimple();

    Owned<IPropertyTreeIterator> iter1 = espSessions->getElements(PathSessionApplication);
    ForEach(*iter1)
    {
        ICopyArrayOf<IPropertyTree> toRemove;
        Owned<IPropertyTreeIterator> iter2 = iter1->query().getElements(PathSessionSession);
        ForEach(*iter2)
        {
            IPropertyTree& item = iter2->query();
            if (timeNow >= item.getPropInt64(PropSessionTimeoutAt, 0))
                toRemove.append(item);
        }
        ForEachItemIn(i, toRemove)
            iter1->query().removeTree(&toRemove.item(i));
    }
}

void CEspHttpServer::authOptionalGroups(EspAuthRequest& authReq)
{
    if (strieq(authReq.httpMethod.str(), GET_METHOD) && (authReq.stype==sub_serv_root) && authenticateOptionalFailed(*authReq.ctx, nullptr))
        throw MakeStringException(-1, "Unauthorized Access to service root");
    if ((!strieq(authReq.httpMethod.str(), GET_METHOD) || !strieq(authReq.serviceName.str(), "esp")) && authenticateOptionalFailed(*authReq.ctx, authReq.authBinding))
        throw MakeStringException(-1, "Unauthorized Access: %s %s", authReq.httpMethod.str(), authReq.serviceName.str());
}

IRemoteConnection* CEspHttpServer::getSDSConnection(const char* xpath, unsigned mode, unsigned timeout)
{
    Owned<IRemoteConnection> globalLock = querySDS().connect(xpath, myProcessSession(), RTM_LOCK_READ, SESSION_SDS_LOCK_TIMEOUT);
    if (!globalLock)
        throw MakeStringException(-1, "Unable to connect to ESP Session information in dali %s", xpath);
    return globalLock.getClear();
}

void CEspHttpServer::addCookie(const char* cookieName, const char *cookieValue, int maxAgeSec)
{
    CEspCookie* cookie = new CEspCookie(cookieName, cookieValue);
    if (maxAgeSec > 0)
    {
        char expiresTime[64];
        time_t tExpires;
        time(&tExpires);
        tExpires += maxAgeSec;
#ifdef _WIN32
        struct tm *gmtExpires;
        gmtExpires = gmtime(&tExpires);
        strftime(expiresTime, 64, "%a, %d %b %Y %H:%M:%S GMT", gmtExpires);
#else
        struct tm gmtExpires;
        gmtime_r(&tExpires, &gmtExpires);
        strftime(expiresTime, 64, "%a, %d %b %Y %H:%M:%S GMT", &gmtExpires);
#endif //_WIN32

        cookie->setExpires(expiresTime);
    }
    cookie->setHTTPOnly(true);
    cookie->setSameSite("Lax");
    m_response->addCookie(cookie);
}

void CEspHttpServer::clearCookie(const char* cookieName)
{
    CEspCookie* cookie = new CEspCookie(cookieName, "");
    cookie->setExpires("Thu, 01 Jan 1970 00:00:01 GMT");
    m_response->addCookie(cookie);
    m_response->addHeader(cookieName,  "max-age=0");
}

unsigned CEspHttpServer::readCookie(const char* cookieName)
{
    CEspCookie* sessionIDCookie = m_request->queryCookie(cookieName);
    if (sessionIDCookie)
    {
        StringBuffer sessionIDStr = sessionIDCookie->getValue();
        if (sessionIDStr.length())
            return atoi(sessionIDStr.str());
    }
    return 0;
}

const char* CEspHttpServer::readCookie(const char* cookieName, StringBuffer& cookieValue)
{
    CEspCookie* sessionIDCookie = m_request->queryCookie(cookieName);
    if (sessionIDCookie)
        cookieValue.append(sessionIDCookie->getValue());
    return cookieValue.str();
}
