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
#include "dasds.hpp"

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
#ifdef _USE_OPENLDAP
        authState = checkUserAuth();
        if ((authState == authUpdatePassword) || (authState == authFailed))
            return 0;
#endif

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
                    checkInitEclIdeResponse(m_request, m_response);
                    return onGetFile(m_request.get(), m_response.get(), pathEx.str());
                }
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
            }

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

#ifdef _USE_OPENLDAP
#define SESSION_TIMEOUT (600) // 10 Mins
const char* SESSION_COOKIE = "ESPSessionID";

EspAuthState CEspHttpServer::checkUserAuth()
{
    sub_service stype=sub_serv_unknown;
    StringBuffer httpPath, httpMethod, pathEx, serviceName, methodName;
    m_request->getEspPathInfo(stype, &pathEx, &serviceName, &methodName, false);
    m_request->getMethod(httpMethod);

    EspAuthState authState = preCheckAuth(httpMethod.str(), serviceName.str(), stype, methodName.str());
    if (authState != authUnknown)
        return authState;

    IEspContext* ctx = m_request->queryContext();
    bool isSoapPost = (strieq(httpMethod.str(), POST_METHOD) && m_request->isSoapMessage());
    EspHttpBinding* authBinding = getEspHttpBinding(ctx, httpMethod.str(), serviceName.str(), stype, isSoapPost);
    if (!authBinding)
        throw MakeStringException(-1, "Cannot find ESP HTTP Binding");

    m_request->getPath(httpPath);//m_httpPath
    if(!httpPath.length())
        throw MakeStringException(-1, "URL query string cannot be empty.");

    if (!isAuthRequiredForBinding(ctx, authBinding, httpPath.str()))
        return authSucceeded;

    if (strieq(httpPath.str(), "/esp/files/userlogon.html"))
    {
        if (readSessionIDFromCookie())
            return authSucceeded;

        DBGLOG("Authentication failed: no session ID found for userlogon.html.");
        askUserLogOn(createHTTPSession(authBinding, NULL, "/"), NULL);
        return authSucceeded;
    }

    StringBuffer servName(ctx->queryServiceName(NULL));
    if (!servName.length())
    {
        authBinding->getServiceName(servName);
        ctx->setServiceName(servName.str());
    }

    AuthType domainAuthType = authBinding->getDomainAuthType();
    ctx->setDomainAuthType(domainAuthType);
    if (domainAuthType != AuthPerRequestOnly)
    {
        if (getEspLogLevel()>LogNormal)
            DBGLOG("Check sessionAuth.");
        unsigned sessionID = readSessionIDFromCookie();
        if (sessionID > 0)
            return doSessionAuth(ctx, authBinding, sessionID, httpPath.str(), httpMethod.str(),
                serviceName.str(), methodName.str(), stype);
    }
    if (domainAuthType != AuthPerSessionOnly)// BasicAuthentication
    {
        if (getEspLogLevel()>LogNormal)
            DBGLOG("Check reqAuth");
        authBinding->populateRequest(m_request.get());
        if (authBinding->doAuth(ctx))
        {
            // authenticate optional groups. Do we still need?
            authOptionalGroups(ctx, authBinding, httpMethod.str(), serviceName.str(), stype);
            StringBuffer userName, peer;
            DBGLOG("Authenticated for %s@%s", ctx->getUserID(userName).str(), m_request->getPeer(peer).str());
            return authSucceeded;
        }
    }

    if ((domainAuthType == AuthPerRequestOnly) || ((domainAuthType == AuthTypeMixed) && (strieq(httpMethod.str(), POST_METHOD))))
    {
        handleAuthFailed(ctx, false, authBinding, NULL, 0, NULL);
        return authFailed;
    }

    //Store the original url for redirecting after login
    StringBuffer redirectURL = httpPath;
    IProperties* params = m_request->queryParameters();
    if (params && params->hasProp("__querystring"))
        redirectURL.append("?").append(params->queryProp("__querystring"));
    handleAuthFailed(ctx, true, authBinding, NULL, 0, redirectURL.str());
    return authFailed;
}

EspAuthState CEspHttpServer::preCheckAuth(const char* httpMethod, const char* serviceName, sub_service stype, const char* serviceMethod)
{
    if (!m_apport->rootAuthRequired() && strieq(httpMethod, GET_METHOD) &&
        ((stype == sub_serv_root) || (serviceName && strieq(serviceName, "esp"))))
            return authSucceeded;

    if (httpMethod && serviceName && serviceMethod && strieq(serviceName, "esp"))
    {
        if (strieq(httpMethod, POST_METHOD) && strieq(serviceMethod, "updatepassword"))
        {
            EspHttpBinding* thebinding = getBinding();
            if (thebinding)
                thebinding->populateRequest(m_request.get());
            onUpdatePassword(m_request.get(), m_response.get());
            return authUpdatePassword;
        }
        if (strieq(httpMethod, GET_METHOD) && strieq(serviceMethod, "updatepasswordinput"))//process before authentication check
        {
            onUpdatePasswordInput(m_request.get(), m_response.get());
            return authUpdatePassword;
        }
    }

    StringBuffer httpPath;
    m_request->getPath(httpPath);//m_httpPath
    if(httpPath.length() && (strieq(httpPath.str(), "/favicon.ico") || strieq(httpPath.str(), "/esp/files/userlogout.html")))
        return authSucceeded;

    return authUnknown;
}

EspHttpBinding* CEspHttpServer::getEspHttpBinding(IEspContext* ctx, const char* httpMethod,
    const char* serviceName, sub_service stype, bool isSoapPost)
{
    if (strieq(httpMethod, GET_METHOD) && ((stype == sub_serv_root) || (serviceName && strieq(serviceName, "esp"))))
        return getBinding();

    if(!m_apport)
        return NULL;

    int ordinality=m_apport->getBindingCount();
    if (ordinality < 1)
        return NULL;

    EspHttpBinding* espHttpBinding = NULL;
    if (ordinality==1)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(0);
        espHttpBinding = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
        //TODO: Why check !isSoapPost if (ordinality==1)?
        //The isValidServiceName() returns false except for ws_ecl and esdl.
        if (!isSoapPost && espHttpBinding && !espHttpBinding->isValidServiceName(*ctx, serviceName))
            espHttpBinding=NULL;
        return espHttpBinding;
    }

    for(unsigned index=0; index<ordinality; index++)
    {
        CEspBindingEntry *entry = m_apport->queryBindingItem(index);
        EspHttpBinding* lbind = (entry) ? dynamic_cast<EspHttpBinding*>(entry->queryBinding()) : NULL;
        if (lbind && lbind->isValidServiceName(*ctx, serviceName))
        {
            espHttpBinding=lbind;
            break;
        }
    }

    if (!espHttpBinding && m_defaultBinding)
        espHttpBinding=dynamic_cast<EspHttpBinding*>(m_defaultBinding.get());

    return espHttpBinding;
}

bool CEspHttpServer::isAuthRequiredForBinding(IEspContext* ctx, EspHttpBinding* authBinding, const char* httpPath)
{
    IAuthMap* authmap = authBinding->queryAuthMAP();
    if(!authmap) //No auth requirement
        return false;

    const char* authMethod = authBinding->queryAuthMethod();
    if (!authMethod || !*authMethod || strieq(authMethod, "none"))
        return false;

    ISecResourceList* rlist = authmap->getResourceList(httpPath);
    if(!rlist) //No auth requirement for the httpPath.
        return false;

    ctx->setAuthenticationMethod(authMethod);
    ctx->setResources(rlist);

    return true;
}

EspAuthState CEspHttpServer::doSessionAuth(IEspContext* ctx, EspHttpBinding* authBinding, unsigned sessionID,
    const char* httpPath, const char* httpMethod, const char* serviceName, const char* methodName, sub_service stype)
{
    if (getEspLogLevel()>LogNormal)
        DBGLOG("sessionID<%d>", sessionID);

    StringBuffer redirectURL = httpPath;
    IProperties* params = m_request->queryParameters();
    if (params && params->hasProp("__querystring"))
        redirectURL.append("?").append(params->queryProp("__querystring"));

    CDateTime now;
    now.setNow();
    time_t accessTime = now.getSimple();
    Owned<IRemoteConnection> conn = querySDS().connect(authBinding->getDomainSessionSDSPath(), myProcessSession(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Failed to connect SDS DomainSession.");

    IPropertyTree* domainSessions = readAndCleanDomainSessions(conn, accessTime);
    VStringBuffer xpath("Session[@id='%d']", sessionID);
    IPropertyTree* sessionTree = domainSessions->getBranch(xpath.str());
    if (!sessionTree)
    {
        if (getEspLogLevel()>LogNormal)
            DBGLOG("Authentication failed: session:<%d> not found", sessionID);
        askUserLogOn(createHTTPSession(authBinding, domainSessions, redirectURL), conn);
        return authFailed;
    }

    HTTPSessionState sessionState = (HTTPSessionState) sessionTree->getPropInt("@state");
    if (sessionState == HTTPSS_new)
    {
        StringBuffer userName, password;
        userName = (params) ? params->queryProp("username") : NULL;
        password = (params) ? params->queryProp("password") : NULL;
        if (!userName.length() || !password.length())
        {
            if (getEspLogLevel()>LogNormal)
                DBGLOG("Authentication failed: invalid credential for session:<%d>", sessionID);
            handleAuthFailed(ctx, true, authBinding, conn, sessionID, redirectURL.str());
            return authFailed;
        }

        ctx->setUserID(userName.str());
        ctx->setPassword(password.str());
        authBinding->populateRequest(m_request.get());
        if (!authBinding->doAuth(ctx))
        {
            if (getEspLogLevel()>LogNormal)
                DBGLOG("Authentication failed for session:<%d>", sessionID);
            handleAuthFailed(ctx, true, authBinding, conn, sessionID, redirectURL.str());
            return authFailed;
        }

        // authenticate optional groups
        authOptionalGroups(ctx, authBinding, httpMethod, serviceName, stype);

        redirectURL.set(sessionTree->queryProp("@loginurl"));
        sessionTree->setProp("@userid", userName.str());
        sessionTree->addPropInt("@state", HTTPSS_active);
    }
    else
        authOptionalGroups(ctx, authBinding, httpMethod, serviceName, stype);

    postSessionAuth(ctx, sessionID, accessTime, conn, sessionTree);
    if (sessionState == HTTPSS_new)
    {
        if (redirectURL.length())
            m_response->redirect(*m_request, redirectURL.str());
        else
            m_response->redirect(*m_request, "/");
    }
    else if (methodName && strieq(methodName, "logout"))
        handleUserLogOut(authBinding, sessionID); //What if the logout failed?

    return authSucceeded;
}

void CEspHttpServer::askUserLogOn(unsigned sessionID, void* _conn)
{
    if (_conn)
    {
        IRemoteConnection* conn = (IRemoteConnection*) _conn;
        conn->commit();
        conn->close();
    }

    VStringBuffer sessionIDStr("%d", sessionID);
    addCookie(SESSION_COOKIE, sessionIDStr.str(), SESSION_TIMEOUT);
    m_response->redirect(*m_request, "/esp/files/userlogon.html");
}

void CEspHttpServer::handleAuthFailed(IEspContext* ctx, bool sessionAuth, EspHttpBinding* authBinding,
    void* conn, unsigned sessionID, const char* redirectURL)
{
    ISecUser *user = ctx->queryUser();
    if (user && (user->getAuthenticateStatus() == AS_PASSWORD_EXPIRED || user->getAuthenticateStatus() == AS_PASSWORD_VALID_BUT_EXPIRED))
    {
        DBGLOG("ESP password expired for %s", ctx->queryUserId());
        handlePasswordExpired(sessionAuth);
        return;
    }

    if (!sessionAuth)
    {
        DBGLOG("Authentication failed: send BasicAuthentication.");
        m_response->sendBasicChallenge(authBinding->getChallengeRealm(), true);
        return;
    }

    DBGLOG("Authentication failed: askUserLogOn.");
    if (conn)
        askUserLogOn(sessionID, conn);
    else
        askUserLogOn(createHTTPSession(authBinding, NULL, redirectURL), NULL);
}

void CEspHttpServer::handlePasswordExpired(bool sessionAuth)
{
    if (sessionAuth)
        m_response->redirect(*m_request.get(), "/esp/updatepasswordinput");
    else
    {
        Owned<IMultiException> me = MakeMultiException();
        me->append(*MakeStringException(-1, "Your ESP password has expired."));
        m_response->handleExceptions(NULL, me, "ESP Authentication", "PasswordExpired", NULL);
    }
    return;
}

void CEspHttpServer::postSessionAuth(IEspContext* ctx, unsigned sessionID, time_t accessTime,
    void* _conn, IPropertyTree* sessionTree)
{
    if (!_conn)
        throw MakeStringException(-1, "Invalid SDS connection.");

    StringBuffer userName = sessionTree->queryProp("@userid");
    StringBuffer netAddr = sessionTree->queryProp("@netAddr");
    sessionTree->setPropInt64("@lastaccessed", accessTime);
    IRemoteConnection* conn = (IRemoteConnection*) _conn;
    conn->commit();
    conn->close();

    ctx->setUserID(userName.str());
    ///ctx->setAuthorized(true);
    VStringBuffer sessionIDStr("%d", sessionID);
    addCookie(SESSION_COOKIE, sessionIDStr.str(), SESSION_TIMEOUT);
    DBGLOG("Authenticated for %s@%s", userName.str(), netAddr.str());
}

bool CEspHttpServer::handleUserLogOut(EspHttpBinding* authBinding, unsigned sessionID)
{
    deleteHTTPSession(authBinding, sessionID);
    clearCookie(SESSION_COOKIE);
    m_response->redirect(*m_request, "/esp/files/userlogout.html");
    return true;
}

unsigned CEspHttpServer::createHTTPSession(EspHttpBinding* authBinding, IPropertyTree* domainSessions, const char* loginURL)
{
    CDateTime now;
    now.setNow();
    time_t createTime = now.getSimple();

    StringBuffer peer;
    VStringBuffer idStr("%s_%ld", m_request->getPeer(peer).str(), createTime);
    unsigned sessionID = hashc((unsigned char *)idStr.str(), idStr.length(), 0);
    if (getEspLogLevel()>LogNormal)
        DBGLOG("New sessionID <%d> for <%ld> in createHTTPSession()", sessionID, createTime);

    Owned<IPropertyTree> ptree = createPTree();
    ptree->addProp("@netaddr", peer.str());
    ptree->addProp("@loginurl", loginURL);
    ptree->addPropInt("@id", sessionID);
    ptree->addPropInt("@state", HTTPSS_new);
    ptree->setPropInt64("@createtime", createTime);
    ptree->setPropInt64("@lastaccessed", createTime);

    if (domainSessions)
    {
        domainSessions->addPropTree("Session", LINK(ptree));
        return sessionID;
    }

    Owned<IRemoteConnection> conn = querySDS().connect(authBinding->getDomainSessionSDSPath(), myProcessSession(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Failed to connect SDS DomainSession.");

    IPropertyTree* root = conn->queryRoot();
    if (!root)
        throw MakeStringException(-1, "Failed to get SDS DomainSession.");

    root->addPropTree("Session", LINK(ptree));
    return sessionID;
}

bool CEspHttpServer::deleteHTTPSession(EspHttpBinding* authBinding, unsigned sessionID)
{
    if (sessionID == 0)
        return false;

    Owned<IRemoteConnection> conn = querySDS().connect(authBinding->getDomainSessionSDSPath(), myProcessSession(), RTM_LOCK_WRITE, SESSION_SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1, "Failed to connect SDS DomainSession.");

    IPropertyTree* root = conn->queryRoot();
    if (!root)
        throw MakeStringException(-1, "Failed to get SDS DomainSession.");

    VStringBuffer path("Session[@id='%d']", sessionID);
    IPropertyTree* ptree = root->getBranch(path.str());
    if (!ptree)
    {
        DBGLOG("Failed to delete session:<%d> not found", sessionID);
        return false;
    }

    root->removeTree(ptree);
    conn->commit();
    conn->close();
    return true;
}

IPropertyTree* CEspHttpServer::readAndCleanDomainSessions(void* _conn, time_t accessTime)
{
    if (!_conn)
        throw MakeStringException(-1, "Invalid SDS connection.");

    IRemoteConnection* conn = (IRemoteConnection*) _conn;
    IPropertyTree* root = conn->queryRoot();
    if (!root)
        throw MakeStringException(-1, "Failed to get SDS DomainSession.");

    //Removing HTTPSessions if timed out
    Owned<IPropertyTreeIterator> iter = root->getElements("Session");
    ForEach(*iter)
    {
        IPropertyTree& item = iter->query();
        if (accessTime - item.getPropInt64("@lastaccessed", 0) >= SESSION_TIMEOUT)
            root->removeTree(&item);
    }
    return root;
}


void CEspHttpServer::addCookie(const char* cookieName, const char *cookieValue, unsigned maxAgeSec)
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
    m_response->addCookie(cookie);
}

void CEspHttpServer::clearCookie(const char* cookieName)
{
    CEspCookie* cookie = new CEspCookie(cookieName, "");
    cookie->setExpires("Thu, 01 Jan 1970 00:00:01 GMT");
    m_response->addCookie(cookie);
    m_response->addHeader(cookieName,  "max-age=0");
}

unsigned CEspHttpServer::readSessionIDFromCookie()
{
    CEspCookie* sessionIDCookie = m_request->queryCookie(SESSION_COOKIE);
    if (sessionIDCookie)
    {
        StringBuffer sessionIDStr = sessionIDCookie->getValue();
        if (sessionIDStr.length())
            return atoi(sessionIDStr.str());
    }
    return 0;
}

void CEspHttpServer::authOptionalGroups(IEspContext* ctx, EspHttpBinding* authBinding,
    const char* httpMethod, const char* serviceName, sub_service stype)
{
    if (strieq(httpMethod, GET_METHOD) && (stype==sub_serv_root) && authenticateOptionalFailed(*ctx, NULL))
        throw MakeStringException(-1, "Unauthorized Access");
    if ((!strieq(httpMethod, GET_METHOD) || !strieq(serviceName, "esp")) && authenticateOptionalFailed(*ctx, authBinding))
        throw MakeStringException(-1, "Unauthorized Access");
}

#endif
