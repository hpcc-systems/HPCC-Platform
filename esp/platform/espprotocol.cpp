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

#ifdef _WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

#include "platform.h"
#include "espprotocol.hpp"
#include "espbinding.hpp"
#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

typedef IXslProcessor * (*getXslProcessor_func)();

static atomic_t gActiveRequests;

long ActiveRequests::getCount()
{
    return atomic_read(&gActiveRequests);
}

void ActiveRequests::inc()
{
    atomic_inc(&gActiveRequests);
}

void ActiveRequests::dec()
{
    atomic_dec(&gActiveRequests);
}

CEspApplicationPort::CEspApplicationPort(bool viewcfg) : bindingCount(0), defBinding(-1), viewConfig(viewcfg), rootAuth(false), navWidth(165), navResize(false), navScroll(false)
{
    build_ver = getBuildVersion();

    hxsl = LoadSharedObject(SharedObjectPrefix "xmllib" SharedObjectExtension, true, false);
    if (!hxsl)
        DBGLOG("Loading xmllib shared library failed!");
    else
    {
        getXslProcessor_func xslfactory = (getXslProcessor_func) GetSharedProcedure(hxsl, "getXslProcessor");
        if (!xslfactory)
            DBGLOG("Loading procedure from xmllib shared library failed!");
        else
            xslp.setown(xslfactory());
    }
}

void CEspApplicationPort::appendBinding(CEspBindingEntry* entry, bool isdefault)
{
    bindings[bindingCount]=entry;
    if (isdefault)
        defBinding=bindingCount;
    bindingCount++;
    EspHttpBinding *httpbind = dynamic_cast<EspHttpBinding *>(entry->queryBinding());
    if (httpbind)
    {
        if (!rootAuth)
            rootAuth = httpbind->rootAuthRequired();
        int width=0;
        bool resizable=false;
        bool scroll=false;
        httpbind->getNavSettings(width, resizable, scroll);
        if (navWidth<width)
            navWidth=width;
        if (!navResize)
            navResize=resizable;
        if (!navScroll)
            navScroll=scroll;
    }
}


const StringBuffer &CEspApplicationPort::getAppFrameHtml(time_t &modified, const char *inner, StringBuffer &html, IEspContext* ctx)
{
    CEspBindingEntry* bindingentry = getDefaultBinding();
    if(bindingentry)
    {
        EspHttpBinding *httpbind = dynamic_cast<EspHttpBinding *>(bindingentry->queryBinding());
        if(httpbind)
        {
            const char* rootpage = httpbind->getRootPage(ctx);
            if(rootpage && *rootpage)
            {
                html.loadFile(StringBuffer(getCFD()).append(rootpage).str());
                return html;
            }
        }
    }

    if (!xslp)
       throw MakeStringException(0,"Error - CEspApplicationPort XSLT processor not initialized");

    bool embedded_url=(inner&&*inner);

    StringBuffer params;
    bool needRefresh = true;
    if (!getUrlParams(ctx->queryRequestParameters(), params))
    {
        if (params.length()==0)
            needRefresh = false;
        if (ctx->getClientVersion()>0)
        {
            params.appendf("%cver_=%g", params.length()?'&':'?', ctx->getClientVersion());
            needRefresh = true;
        }
    }

    if (needRefresh || embedded_url || !appFrameHtml.length())
    {
        int passwordDaysRemaining = -1;//-1 means dont display change password screen
#ifdef _USE_OPENLDAP
        ISecUser* user = ctx->queryUser();
        ISecManager* secmgr = ctx->querySecManager();
        if(user && secmgr)
        {
            passwordDaysRemaining = user->getPasswordDaysRemaining();//-1 if expired, -2 if never expires
            int passwordExpirationDays = (int)secmgr->getPasswordExpirationWarningDays();
            if (passwordDaysRemaining==-2 || passwordDaysRemaining > passwordExpirationDays)
                passwordDaysRemaining = -1;
        }
#endif
        StringBuffer xml;
        StringBuffer encoded_inner;
        if(inner && *inner)
            encodeXML(inner, encoded_inner);

        // replace & with &amps;
        params.replaceString("&","&amp;");

        xml.appendf("<EspApplicationFrame title=\"%s\" navWidth=\"%d\" navResize=\"%d\" navScroll=\"%d\" inner=\"%s\" params=\"%s\" passwordDays=\"%d\"/>",
            getESPContainer()->getFrameTitle(), navWidth, navResize, navScroll, (inner&&*inner) ? encoded_inner.str() : "?main", params.str(), passwordDaysRemaining);

        Owned<IXslTransform> xform = xslp->createXslTransform();
        xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/appframe.xsl").str());
        xform->setXmlSource(xml.str(), xml.length()+1);
        xform->transform( (needRefresh || embedded_url) ? html.clear() : appFrameHtml.clear());
    }

    if (!needRefresh && !embedded_url)
        html.clear().append(appFrameHtml.str());

    static time_t startup_time = time(NULL);
    modified = startup_time;
    return html;
}

const StringBuffer &CEspApplicationPort::getTitleBarHtml(IEspContext& ctx, bool rawXml)
{
    if (xslp)
    {
        StringBuffer titleBarXml;
        const char* user = ctx.queryUserId();
                if (!user || !*user)
            titleBarXml.appendf("<EspHeader><BuildVersion>%s</BuildVersion><ConfigAccess>%d</ConfigAccess>"
                "<LoginId>&lt;nobody&gt;</LoginId><NoUser>1</NoUser></EspHeader>", build_ver, viewConfig);
                else
            titleBarXml.appendf("<EspHeader><BuildVersion>%s</BuildVersion><ConfigAccess>%d</ConfigAccess>"
                "<LoginId>%s</LoginId></EspHeader>", build_ver, viewConfig, user);

        if (rawXml)
        {
            titleBarHtml.set(titleBarXml);
        }
        else
        {
            Owned<IXslTransform> xform = xslp->createXslTransform();
            xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/espheader.xsl").str());
            xform->setXmlSource(titleBarXml.str(), titleBarXml.length()+1);
            xform->transform(titleBarHtml.clear());
        }
    }
    return titleBarHtml;
}

const StringBuffer &CEspApplicationPort::getNavBarContent(IEspContext &context, StringBuffer &content, StringBuffer &contentType, bool rawxml)
{
    if (xslp)
    {
        Owned<IPropertyTree> navtree=createPTree("EspNavigationData");
        int count = getBindingCount();
        for (int idx = 0; idx<count; idx++)
            bindings[idx]->queryBinding()->getNavigationData(context, *navtree.get());

        StringBuffer xml;
        buildNavTreeXML(navtree.get(), xml);
        if (rawxml)
        {
            content.swapWith(xml);
            contentType.clear().append(HTTP_TYPE_APPLICATION_XML_UTF8);
        }
        else
        {
            const char* viewType = navtree->queryProp("@viewType");

            Owned<IXslTransform> xform = xslp->createXslTransform();

            StringBuffer xslsource;
            if (viewType && *viewType)
            {
                xslsource.append(getCFD()).appendf("./xslt/%s.xsl", stricmp(viewType, "tree") != 0 ? viewType: "navigation");
            }
            else
            {
                xslsource.append(getCFD()).append("./xslt/nav.xsl");

            }
            xform->loadXslFromFile(xslsource.str());


            xform->setXmlSource(xml.str(), xml.length()+1);
            xform->transform(content);
            contentType.clear().append("text/html; charset=UTF-8");
        }
    }
    return content;
}

const StringBuffer &CEspApplicationPort::getDynNavData(IEspContext &context, IProperties *params, StringBuffer &content,
                                                       StringBuffer &contentType, bool& bVolatile)
{
    Owned<IPropertyTree> navtree=createPTree("EspDynNavData");
    bVolatile = false;
    int count = getBindingCount();
    for (int idx = 0; idx<count; idx++)
        bindings[idx]->queryBinding()->getDynNavData(context, params, *navtree.get());

    if (!bVolatile)
        bVolatile = navtree->getPropBool("@volatile", false);
    contentType.clear().append(HTTP_TYPE_APPLICATION_XML_UTF8);
    return toXML(navtree.get(), content.clear());
}

int CEspApplicationPort::onGetNavEvent(IEspContext &context, IHttpMessage* request, IHttpMessage* response)
{
    int handled=0;
    int count = getBindingCount();
    for (int idx = 0; !handled && idx<count; idx++)
    {
        handled = bindings[idx]->queryBinding()->onGetNavEvent(context, request, response);
    }
    return handled;
}

int CEspApplicationPort::onBuildSoapRequest(IEspContext &context, IHttpMessage* ireq, IHttpMessage* iresp)
{
    CHttpRequest *request=dynamic_cast<CHttpRequest*>(ireq);
    CHttpResponse *response=dynamic_cast<CHttpResponse*>(iresp);

    int handled=0;
    int count = getBindingCount();
    for (int idx = 0; !handled && idx<count; idx++)
    {
        //if (bindings[idx]->queryBinding()->isValidServiceName(context, ))
    }
    return handled;
}

void CEspApplicationPort::buildNavTreeXML(IPropertyTree* navtree, StringBuffer& xmlBuf, bool insideFolder)
{
    if (!navtree)
        return;

    //Find out the menu items which do not request a specific position
    //Also find out the maximum position being requested
    unsigned positionMax = 0;
    StringArray itemsGroup1;
    Owned<IPropertyTreeIterator> items = navtree->getElements("*");
    ForEach(*items)
    {
        IPropertyTree &item = items->query();
        unsigned position = (unsigned) item.getPropInt("@relPosition", 0);
        if (position > positionMax)
        {
            positionMax = position;
        }
        else if (position < 1)
        {//if the item does not request a position, add it to the 'itemsGroup1'.
            StringBuffer itemXML;
            if (!insideFolder)
                buildNavTreeXML(&item, itemXML, true);
            else
                toXML(&item, itemXML);

            itemsGroup1.append(itemXML);
        }
    }

    xmlBuf.appendf("<%s", navtree->queryName());
    Owned<IAttributeIterator> attrs = navtree->getAttributes();
    ForEach(*attrs)
    {
        const char *attrname = attrs->queryName()+1;
        const char *attrvaluee = attrs->queryValue();
        if (attrname && *attrname && attrvaluee && *attrvaluee)
            xmlBuf.appendf(" %s=\"%s\"", attrname, attrvaluee);
    }
    xmlBuf.append(">\n");

    unsigned positionInGroup1 = 0;
    unsigned itemCountInGroup1 = itemsGroup1.length();

    //append the menu items based on the position requested
    unsigned position = 1;
    while (position <= positionMax)
    {
        bool foundOne = false;

        //process the item(s) which asks for this position
        StringBuffer xPath;
        xPath.appendf("*[@relPosition=%d]", position);
        Owned<IPropertyTreeIterator> items1 = navtree->getElements(xPath.str());
        ForEach(*items1)
        {
            IPropertyTree &item = items1->query();

            StringBuffer itemXML;
            if (!insideFolder)
                buildNavTreeXML(&item, itemXML, true);
            else
                toXML(&item, itemXML);
            xmlBuf.append(itemXML.str());

            foundOne = true;
        }

        //If no one asks for this position, pick one from the itemsGroup1
        if (!foundOne && (positionInGroup1 < itemCountInGroup1))
        {
            StringBuffer itemXML = itemsGroup1.item(positionInGroup1);
            xmlBuf.append(itemXML.str());
            positionInGroup1++;
        }

        position++;
    }

    //Check any item left inside the itemsGroup1 and append it into the xml
    while (positionInGroup1 < itemCountInGroup1)
    {
        StringBuffer itemXML = itemsGroup1.item(positionInGroup1);
        xmlBuf.append(itemXML.str());
        positionInGroup1++;
    }

    xmlBuf.appendf("</%s>\n", navtree->queryName());
}

IPropertyTree *CEspBinding::ensureNavFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *menuname, bool sort, unsigned relPosition)
{
    StringBuffer xpath;
    xpath.appendf("Folder[@name=\"%s\"]", name);

    IPropertyTree *ret = root.queryPropTree(xpath.str());
    if (!ret)
    {
        ret=createPTree("Folder");
        ret->addProp("@name", name);
        ret->addProp("@tooltip", tooltip);
        ret->setProp("@menu", menuname);
        if (sort)
            ret->addPropBool("@sort", true);
        ret->addPropInt("@relPosition", relPosition);

        root.addPropTree("Folder", ret);
    }

    return ret;
}

IPropertyTree *CEspBinding::ensureNavMenu(IPropertyTree &root, const char *name)
{
    StringBuffer xpath;
    xpath.appendf("Menu[@name=\"%s\"]", name);

    IPropertyTree *ret = root.queryPropTree(xpath.str());
    if (!ret)
    {
        ret=createPTree("Menu");
        ret->addProp("@name", name);
        root.addPropTree("Menu", ret);
    }
    return ret;
}

IPropertyTree *CEspBinding::ensureNavMenuItem(IPropertyTree &root, const char *name, const char *tooltip, const char *action)
{
    StringBuffer xpath;
    xpath.appendf("MenuItem[@name=\"%s\"]", name);
    IPropertyTree *ret = root.queryPropTree(xpath.str());
    if (!ret)
    {
        ret=createPTree("MenuItem");
        ret->addProp("@name", name);
        ret->addProp("@tooltip", tooltip);
        ret->addProp("@action", action);
        root.addPropTree("MenuItem", ret);
    }
    return ret;
}

IPropertyTree *CEspBinding::ensureNavDynFolder(IPropertyTree &root, const char *name, const char *tooltip, const char *params, const char *menuname)
{
    StringBuffer xpath;
    xpath.appendf("DynamicFolder[@name=\"%s\"]", name);

    IPropertyTree *ret = root.queryPropTree(xpath.str());
    if (!ret)
    {
        ret=createPTree("DynamicFolder");
        ret->addProp("@name", name);
        ret->addProp("@tooltip", tooltip);
        ret->addProp("@params", params);
        ret->setProp("@menu", menuname);
        root.addPropTree("DynamicFolder", ret);
    }
    return ret;
}

IPropertyTree *CEspBinding::ensureNavLink(IPropertyTree &folder, const char *name, const char *path, const char *tooltip, const char *menuname, const char *navPath, unsigned relPosition, bool force)
{
    StringBuffer xpath;
    xpath.appendf("Link[@name=\"%s\"]", name);

    bool addNew = true;
    IPropertyTree *ret = folder.queryPropTree(xpath.str());
    if (ret)
    {
        bool forced = ret->getPropBool("@force");
        if (forced || !force)
            return ret;

        addNew = false;
    }

    if (addNew)
        ret=createPTree("Link");

    ret->setProp("@name", name);
    ret->setProp("@tooltip", tooltip);
    ret->setProp("@path", path);
    ret->setProp("@menu", menuname);
    ret->setProp("@navPath", navPath);
    ret->setPropInt("@relPosition", relPosition);
    ret->setPropBool("@force", force);

    if (addNew)
        folder.addPropTree("Link", ret);

    return ret;
}


IPropertyTree *CEspBinding::addNavException(IPropertyTree &folder, const char *message/*=NULL*/, int code/*=0*/, const char *source/*=NULL*/)
{
    IPropertyTree *ret = folder.addPropTree("Exception", createPTree());
    ret->addProp("@message", message ? message : "Unknown exception");
    ret->setPropInt("@code", code);
    ret->setProp("@source", source);
    return ret;
}

void CEspBinding::getNavigationData(IEspContext &context, IPropertyTree & data)
{
    IEspWsdlSections *wsdl = dynamic_cast<IEspWsdlSections *>(this);
    if (wsdl)
    {
        StringBuffer serviceName, params;
        wsdl->getServiceName(serviceName);
        if (!getUrlParams(context.queryRequestParameters(), params))
        {
            if (context.getClientVersion()>0)
                params.appendf("%cver_=%g", params.length()?'&':'?', context.getClientVersion());
        }

        IPropertyTree *folder=createPTree("Folder");
        folder->addProp("@name", serviceName.str());
        folder->addProp("@info", serviceName.str());

        StringBuffer encodedparams;
        if (params.length())
            encodeUtf8XML(params.str(), encodedparams, 0);

        folder->addProp("@urlParams", encodedparams);
        if (showSchemaLinks())
            folder->addProp("@showSchemaLinks", "true");

        if (params.length())
            params.setCharAt(0,'&'); //the entire params string will follow the initial param: "?form"

        MethodInfoArray methods;
        wsdl->getQualifiedNames(context, methods);
        ForEachItemIn(idx, methods)
        {
            CMethodInfo &method = methods.item(idx);
            IPropertyTree *link=createPTree("Link");
            link->addProp("@name", method.m_label.str());
            link->addProp("@info", method.m_label.str());
            StringBuffer path;
            path.appendf("../%s/%s?form%s", serviceName.str(), method.m_label.str(),params.str());
            link->addProp("@path", path.str());

            folder->addPropTree("Link", link);
        }

        data.addPropTree("Folder", folder);
    }
}

void CEspBinding::getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data)
{
}

#ifdef _USE_OPENLDAP
void CEspApplicationPort::onUpdatePasswordInput(IEspContext &context, StringBuffer& html)
{
    StringBuffer xml;
    if (context.queryUserId())
        xml.appendf("<UpdatePassword><username>%s</username><Code>-1</Code></UpdatePassword>", context.queryUserId());
    else
        xml.appendf("<UpdatePassword><Code>2</Code><Massage>Can't find user in esp context. Please check if the user was properly logged in.</Massage></UpdatePassword>");
    Owned<IXslTransform> xform = xslp->createXslTransform();
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/passwordupdate.xsl").str());
    xform->setXmlSource(xml.str(), xml.length()+1);
    xform->transform( html);
    return;
}

void CEspApplicationPort::onUpdatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& html)
{
    StringBuffer xml, message;
    unsigned returnCode = updatePassword(context, request, message);

    if (context.queryUserId())
        xml.appendf("<UpdatePassword><username>%s</username>", context.queryUserId());
    else
        xml.appendf("<UpdatePassword><username/>");
    xml.appendf("<Code>%d</Code><Message>%s</Message></UpdatePassword>", returnCode, message.str());

    Owned<IXslTransform> xform = xslp->createXslTransform();
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/passwordupdate.xsl").str());
    xform->setXmlSource(xml.str(), xml.length()+1);
    xform->transform( html);
    return;
}

unsigned CEspApplicationPort::updatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& message)
{
    ISecManager* secmgr = context.querySecManager();
    if(!secmgr)
    {
        message.append("Security manager is not found. Please check if the system authentication is set up correctly.");
        return 2;
    }

    ISecUser* user = context.queryUser();
    if(!user)
    {
        message.append("Can't find user in esp context. Please check if the user was properly logged in.");
        return 2;
    }

    const char* oldpass1 = context.queryPassword();
    if (!oldpass1)
    {
        message.append("Existing password missing from request.");
        return 2;
    }

    CHttpRequest *httpRequest=dynamic_cast<CHttpRequest*>(request);
    IProperties *params = httpRequest->getParameters();
    if (!params)
    {
        message.append("No parameter is received. Please check user input.");
        return 1;
    }

    const char* username = params->queryProp("username");
    const char* oldpass = params->queryProp("oldpass");
    const char* newpass1 = params->queryProp("newpass1");
    const char* newpass2 = params->queryProp("newpass2");
    if(!username || !streq(username, user->getName()))
    {
        message.append("Incorrect username has been received.");
        return 1;
    }
    if(!oldpass || !streq(oldpass, oldpass1))
    {
        message.append("Old password doesn't match credentials in use.");
        return 1;
    }
    if(!streq(newpass1, newpass2))
    {
        message.append("Password re-entry doesn't match.");
        return 1;
    }
    if(streq(oldpass, newpass1))
    {
        message.append("New password can't be the same as current password.");
        return 1;
    }

    bool returnFlag = false;
    try
    {
        returnFlag = secmgr->updateUserPassword(*user, newpass1, oldpass);//provide the entered current password, not the cached one
    }
    catch(IException* e)
    {
        StringBuffer emsg;
        e->errorMessage(emsg);
        message.append(emsg.str());
        return 2;
    }

    if(!returnFlag)
    {
        message.append("Failed in changing password.");
        return 2;
    }

    message.append("Your password has been changed successfully.");
    return 0;
}
#endif

CEspProtocol::CEspProtocol()
{
   m_viewConfig=false;
   m_MaxRequestEntityLength = DEFAULT_MAX_REQUEST_ENTITY_LENGTH;
}

CEspProtocol::~CEspProtocol()
{
    clear();
}

bool CEspProtocol::notifySelected(ISocket *sock,unsigned selected)
{
    return true;
}

const char * CEspProtocol::getProtocolName()
{
    return "ESP Protocol";
}


void CEspProtocol::addBindingMap(ISocket *sock, IEspRpcBinding* binding, bool isdefault)
{
    CEspBindingEntry *entry = new CEspBindingEntry(sock, binding);

    char name[256];
    int port = sock->name(name, 255);

    CApplicationPortMap::iterator apport_it = m_portmap.find(port);

    CEspApplicationPort *apport=NULL;

    if (apport_it!=m_portmap.end())
    {
        apport = (*apport_it).second;
        apport->appendBinding(entry, isdefault);
    }
    else
    {
        apport = new CEspApplicationPort(m_viewConfig);
        apport->appendBinding(entry, isdefault);

        CApplicationPortMap::value_type vt(port, apport);
        m_portmap.insert(vt);
    }
}

CEspApplicationPort* CEspProtocol::queryApplicationPort(int port)
{
    CApplicationPortMap::iterator apport_it = m_portmap.find(port);
    return (apport_it != m_portmap.end()) ? (*apport_it).second : NULL;
}
