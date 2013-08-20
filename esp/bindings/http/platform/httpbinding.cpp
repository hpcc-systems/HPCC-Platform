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

#pragma warning(disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
//#include "IAEsp.hpp"

//ESP Core
#include "espthread.hpp"
#include "espbinding.hpp"

#include "bindutil.hpp"

#include "espcontext.hpp"

#include "httpbinding.hpp"
#include "htmlpage.hpp"
#include  "seclib.hpp"
#include "../../../system/security/shared/secloader.hpp"
#include  "../../SOAP/Platform/soapmessage.hpp"
#include "xmlvalidator.hpp"
#include "xsdparser.hpp"

#define FILE_UPLOAD     "FileUploadAccess"

static HINSTANCE getXmlLib()
{
    const char* name = SharedObjectPrefix "xmllib" SharedObjectExtension;
    HINSTANCE xmllib = LoadSharedObject(name,true,false);
    if (!LoadSucceeded(xmllib))
        throw MakeStringException(-1,"load %s failed with code %d", name, GetSharedObjectError());
    return xmllib;
}

static IXslProcessor* getXmlLibXslProcessor()
{
    static Owned<IXslProcessor> processor;
    if (!processor)
    {
        typedef IXslProcessor* (*getXslProcessor_t)();
        getXslProcessor_t f = (getXslProcessor_t)GetSharedProcedure(getXmlLib(), "getXslProcessor");
        processor.setown(f());
    }
    return processor.get();
}

static IXmlValidator* getXmlLibXmlValidator()
{
    static Owned<IXmlValidator> v;
    if (!v)
    {
        typedef IXmlDomParser* (*getDomParser_t)();
        getDomParser_t f = (getDomParser_t)GetSharedProcedure(getXmlLib(), "getXmlDomParser");
        Owned<IXmlDomParser> d = f();
        v.setown(d->createXmlValidator());
    }
    return v.get();
}

static IXmlSchema* createXmlSchema(const char* schema)
{
    const char* name = SharedObjectPrefix "xmllib" SharedObjectExtension;
    HINSTANCE xmllib = LoadSharedObject(name,true,false);
    if (!LoadSucceeded(xmllib))
        throw MakeStringException(-1,"load %s failed with code %d", name, GetSharedObjectError());
    typedef IXmlSchema* (*XmlSchemaCreator)(const char*);
    XmlSchemaCreator creator = (XmlSchemaCreator)GetSharedProcedure(xmllib, "createXmlSchemaFromString");
    if (!creator)
        throw MakeStringException(-1,"load XmlSchema factory failed: createXmlSchemaFromString()");
    
    return creator(schema);
}

EspHttpBinding::EspHttpBinding(IPropertyTree* tree, const char *bindname, const char *procname)
{
    Owned<IPropertyTree> proc_cfg = getProcessConfig(tree, procname);
    m_viewConfig = proc_cfg ? proc_cfg->getPropBool("@httpConfigAccess") : false;   
    m_formOptions = proc_cfg ? proc_cfg->getPropBool("@formOptionsAccess") : false;
    m_roxieOption = proc_cfg ? proc_cfg->getPropBool("@roxieTestAccess") : false;
    m_includeSoapTest = true;
    m_configFile.set(tree ? tree->queryProp("@config") : "esp.xml");
    Owned<IPropertyTree> bnd_cfg = getBindingConfig(tree, bindname, procname);
    m_wsdlVer=0.0;

    // get the config default version
    const char* defVersion = bnd_cfg->queryProp("@defaultServiceVersion");
    if (defVersion && *defVersion)
        m_defaultSvcVersion.set(defVersion);

    if(!bnd_cfg)
    {
        m_filespath.append(getCFD()).append("files/");
    }
    else
    {
        if (!bnd_cfg->getProp("@baseFilesPath", m_filespath))
            m_filespath.append(getCFD()).append("./files/");

        m_host.append(bnd_cfg->queryProp("@netAddress"));
        m_port = bnd_cfg->getPropInt("@port");

        const char *realm = bnd_cfg->queryProp("@realm");
        if (realm)
            m_realm.append(realm);
        else
        {
            StringBuffer xpath;
            xpath.appendf("EspBinding[@port='%d']", m_port);
            Owned<IPropertyTreeIterator> bindings = proc_cfg->getElements(xpath.str());
            for (bindings->first(); bindings->isValid(); bindings->next())
            {
                const char *type = bindings->query().queryProp("@type");
                if (type && !stricmp(type, "ws_smcSoapBinding"))
                {
                    m_realm.append("EclWatch");
                    break;
                }
            }

            if (!m_realm.length())
            {
                const char *srvName = bnd_cfg->queryProp("@service");
                if (srvName)
                {
                    Owned<IPropertyTree> srv_cfg = getServiceConfig(tree, srvName, procname);
                    if (srv_cfg)
                    {
                        realm = srv_cfg->queryProp("@type");
                        const char *ssfile=srv_cfg->queryProp("@subservices");
                        if (ssfile && checkFileExists(ssfile))
                            m_subservices.setown(createPTreeFromXMLFile(ssfile, ipt_caseInsensitive));
                    }
                }
                
                m_realm.append((realm) ? realm : "EspServices");
            }
        }

        m_wsdlAddress.append(bnd_cfg->queryProp("@wsdlServiceAddress"));
        if (!m_wsdlAddress.length())
        {
            const char *host=getHost();
            if (!host || !(*host) || !strcmp(host, ".") || !strcmp(host, "0.0.0.0"))
                queryHostIP().getIpText(m_wsdlAddress);
            else
                m_wsdlAddress.append(host);
            if (!strchr(m_wsdlAddress.str(), ':') && m_port!=80 && m_port!=443)
                m_wsdlAddress.append(':').append(m_port);
        }
        if (strnicmp(m_wsdlAddress.str(), "http", 4))
            m_wsdlAddress.insert(0, (m_port!=443) ? "http://" : "https://");
        
        Owned<IPropertyTree> authcfg = bnd_cfg->getPropTree("Authenticate");
        if(authcfg != NULL)
        {
            authcfg->getProp("@type", m_authtype);
            authcfg->getProp("@method", m_authmethod);
            PROGLOG("Authenticate method=%s", m_authmethod.str());
            if(stricmp(m_authmethod.str(), "LdapSecurity") == 0)
            {
                StringBuffer lsname;
                authcfg->getProp("@config", lsname);
                Owned<IPropertyTree> lscfg = bnd_cfg->getPropTree(StringBuffer(".//ldapSecurity[@name=").appendf("\"%s\"]", lsname.str()).str());
                if(lscfg == NULL)
                {
                    Owned<IPropertyTree> process_config = getProcessConfig(tree, procname);
                    if(process_config.get() != NULL)
                        lscfg.setown(process_config->getPropTree(StringBuffer("ldapSecurity[@name=").appendf("\"%s\"]", lsname.str()).str()));
                    if(lscfg == NULL)
                    {
                        ERRLOG("can't find bnd_cfg for LdapSecurity %s", lsname.str());
                        throw MakeStringException(-1, "can't find bnd_cfg for LdapSecurity %s", lsname.str());
                    }
                }

                m_secmgr.setown(SecLoader::loadSecManager("LdapSecurity", "EspHttpBinding", LINK(lscfg)));
                if(m_secmgr.get() == NULL)
                {
                    throw MakeStringException(-1, "error generating SecManager");
                }
                
                StringBuffer basednbuf;
                authcfg->getProp("@resourcesBasedn", basednbuf);
                m_secmgr->setExtraParam("resourcesBasedn", basednbuf.str());
                basednbuf.clear();
                authcfg->getProp("@workunitsBasedn", basednbuf);
                m_secmgr->setExtraParam("workunitsBasedn", basednbuf.str());

                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
                m_feature_authmap.setown(m_secmgr->createFeatureMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "CommonSecurity") == 0)
            {
                StringBuffer csname;
                bnd_cfg->getProp(".//Authenticate/@config", csname);
                IPropertyTree* cscfg = bnd_cfg->getPropTree(StringBuffer(".//CommonSecurity[@name=").appendf("\"%s\"]", csname.str()).str());
                if(cscfg == NULL)
                {
                    IPropertyTree* process_config = getProcessConfig(tree, procname);
                    if(process_config != NULL)
                        cscfg = process_config->getPropTree(StringBuffer("CommonSecurity[@name=").appendf("\"%s\"]", csname.str()).str());
                    if(cscfg == NULL)
                        throw MakeStringException(-1, "can't find cfg for CommonSecurity %s", csname.str());
                }
                
                m_secmgr.setown(SecLibLoader::loadSecManager("CommonSecurity", "EspHttpBinding", cscfg));
                if(m_secmgr.get() == NULL)
                {
                    throw MakeStringException(-1, "error generating SecManager for CommonSecurity");
                }
                
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "AccurintSecurity") == 0)
            {
                StringBuffer csname;
                bnd_cfg->getProp(".//Authenticate/@config", csname);
                Owned<IPropertyTree> cscfg = bnd_cfg->getPropTree(StringBuffer(".//AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str());
                if(cscfg == NULL)
                {
                    Owned<IPropertyTree> process_config = getProcessConfig(tree, procname);
                    if(process_config.get() != NULL)
                        cscfg.setown(process_config->getPropTree(StringBuffer("AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str()));
                    if(cscfg == NULL)
                        throw MakeStringException(-1, "can't find cfg for AccurintSecurity %s", csname.str());
                }
                StringBuffer iproaming;
                if(proc_cfg.get())
                    proc_cfg->getProp("@enableIPRoaming", iproaming);
                if(iproaming.length() > 0 && (strcmp(iproaming.str(), "1") == 0 || stricmp(iproaming.str(), "true") == 0))
                    cscfg->setPropBool("@enableIPRoaming", true);
                StringBuffer otp;
                if(proc_cfg.get())
                    proc_cfg->getProp("@enableOTP", otp);
                if(otp.length() > 0 && (strcmp(otp.str(), "1") == 0 || stricmp(otp.str(), "true") == 0))
                    cscfg->setPropBool("@enableOTP", true);
                
                m_secmgr.setown(SecLibLoader::loadSecManager("AccurintSecurity", "EspHttpBinding", cscfg));
                if(m_secmgr.get() == NULL)
                {
                    throw MakeStringException(-1, "error generating SecManager for AccurintSecurity");
                }
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
                m_feature_authmap.setown(m_secmgr->createFeatureMap(authcfg));
                m_setting_authmap.setown(m_secmgr->createSettingMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "AccurintAccess") == 0)
            {
                
                StringBuffer csname;
                bnd_cfg->getProp(".//Authenticate/@config", csname);
                Owned<IPropertyTree> cscfg = bnd_cfg->getPropTree(StringBuffer(".//AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str());
                if(cscfg == NULL)
                {
                    Owned<IPropertyTree> process_config = getProcessConfig(tree, procname);
                    if(process_config.get() != NULL)
                        cscfg.setown(process_config->getPropTree(StringBuffer("AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str()));
                    if(cscfg == NULL)
                        throw MakeStringException(-1, "can't find cfg for AccurintSecurity %s", csname.str());
                }
                
                m_secmgr.setown(SecLibLoader::loadSecManager("AccurintAccess", "EspHttpBinding", cscfg));
                if(m_secmgr.get() == NULL)
                {
                    throw MakeStringException(-1, "error generating SecManager for AccurintSecurity");
                }
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
                m_feature_authmap.setown(m_secmgr->createFeatureMap(authcfg));
                //m_setting_authmap.setown(m_secmgr->createSettingMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "RemoteNSSecurity") == 0)
            {
                
                StringBuffer csname;
                //TODO - USe a different set of configuration settings.
                bnd_cfg->getProp(".//Authenticate/@config", csname);
                Owned<IPropertyTree> cscfg = bnd_cfg->getPropTree(StringBuffer(".//AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str());
                if(cscfg == NULL)
                {
                    Owned<IPropertyTree> process_config = getProcessConfig(tree, procname);
                    if(process_config.get() != NULL)
                        cscfg.setown(process_config->getPropTree(StringBuffer("AccurintSecurity[@name=").appendf("\"%s\"]", csname.str()).str()));
                    if(cscfg == NULL)
                        throw MakeStringException(-1, "can't find cfg for AccurintSecurity %s", csname.str());
                }
                
                m_secmgr.setown(SecLibLoader::loadSecManager("RemoteNSSecurity", "EspHttpBinding", cscfg));
                if(m_secmgr.get() == NULL)
                {
                    throw MakeStringException(-1, "error generating SecManager for CommonSecurity");
                }
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
                m_feature_authmap.setown(m_secmgr->createFeatureMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "UserDefined") == 0)
            {
                m_secmgr.setown(SecLoader::loadSecManager("Default", "EspHttpBinding", NULL));
                m_authmap.setown(SecLoader::loadTheDefaultAuthMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "Local") == 0)
            {
                m_secmgr.setown(SecLoader::loadSecManager("Local", "EspHttpBinding", NULL));
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
            }
            else if(stricmp(m_authmethod.str(), "htpasswd") == 0)
            {
                Owned<IPropertyTree> cfg;
                Owned<IPropertyTree> process_config = getProcessConfig(tree, procname);
                if(process_config.get() != NULL)
                    cfg.setown(process_config->getPropTree("htpasswdSecurity"));
                if(cfg == NULL)
                {
                    ERRLOG("can't find htpasswdSecurity in configuration");
                    throw MakeStringException(-1, "can't find htpasswdSecurity in configuration");
                }

                m_secmgr.setown(SecLoader::loadSecManager("htpasswd", "EspHttpBinding", LINK(cfg)));
                m_authmap.setown(m_secmgr->createAuthMap(authcfg));
            }
            IRestartManager* restartManager = dynamic_cast<IRestartManager*>(m_secmgr.get());
            if(restartManager!=NULL)
            {
                IRestartHandler* pHandler = dynamic_cast<IRestartHandler*>(getESPContainer());
                if(pHandler!=NULL)
                    restartManager->setRestartHandler(pHandler);
            }
        }
    }

    if(m_secmgr.get())
    {
        const char* desc = m_secmgr->getDescription();
        if(desc && *desc)
            m_challenge_realm.appendf("ESP (Authentication: %s)", desc);
    }
    if(m_challenge_realm.length() == 0)
        m_challenge_realm.append("ESP");
}

StringBuffer &EspHttpBinding::generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &ns)
{
    ns.append("urn:hpccsystems:ws:");
    if (serv && *serv)
        ns.appendLower(strlen(serv), serv);
    return ns;
}

void EspHttpBinding::getSchemaLocation( IEspContext &context, CHttpRequest* request, StringBuffer &schemaLocation )
{
    const char* svcName = request->queryServiceName();
    const char* method = request->queryServiceMethod();
    if ( !svcName || !(*svcName) )
        return;

    StringBuffer host;
    const char* wsdlAddr = request->queryParameters()->queryProp("__wsdl_address");
    if (wsdlAddr && *wsdlAddr)
        host.append(wsdlAddr);
    else
    {
        host.append(request->queryHost());
        if (request->getPort()>0)
          host.append(":").append(request->getPort());
    }
    schemaLocation.appendf("%s/%s/%s?xsd&amp;ver_=%g", host.str(), svcName, method ? method : "", context.getClientVersion());
}

int EspHttpBinding::getMethodDescription(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    StringBuffer key(method);
    StringAttr *descrStr=desc_map.getValue(key.toUpperCase().str());
    page.append(descrStr ? descrStr->get() : "No description available");
    return 0;
}

int EspHttpBinding::getMethodHelp(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    StringBuffer key(method);
    StringAttr *helpStr=help_map.getValue(key.toUpperCase().str());
    page.append(helpStr ? helpStr->get() : "No Help available");
    return 0;
}

bool EspHttpBinding::qualifySubServiceName(IEspContext &context, const char *servname, const char *methname, StringBuffer &servQName, StringBuffer *methQName)
{
    if (m_subservices)
    {
        StringBuffer xpath;
        xpath.appendf("SubService[@name='%s']/@name", servname);
        const char *qname=m_subservices->queryProp(xpath.str());
        if (qname)
        {
            servQName.clear().append(qname);
            return qualifyMethodName(context, methname, methQName);
        }
    }
    return false;
}

bool EspHttpBinding::hasSubService(IEspContext &context, const char *name)
{
    if (m_subservices)
    {
        StringBuffer xpath;
        xpath.appendf("SubService[@name='%s']", name);
        return m_subservices->hasProp(xpath.str());
    }
    return false;
}


void EspHttpBinding::setRequestPath(const char *path)
{
    m_reqPath.clear();
    m_reqPath.append(path);
}

bool EspHttpBinding::rootAuthRequired()
{
    if(!m_authmap.get())
        return false;

    if(!stricmp(m_authmethod.str(), "UserDefined") && m_authmap->shouldAuth("/"))
        return true;

    Owned<ISecResourceList> rlist = m_authmap->getResourceList("/");
    if (rlist.get() == NULL)
        return false;

    return true;
}


bool EspHttpBinding::authRequired(CHttpRequest *request)
{
    StringBuffer path;
    request->getPath(path);
    if(path.length() == 0)
    {
        throw MakeStringException(-1, "Path is empty for http request");
    }
    if(m_authmap.get() == NULL)
        return false;

    if(stricmp(m_authmethod.str(), "UserDefined") == 0 && m_authmap->shouldAuth(path.str()))
        return true;

    ISecResourceList* rlist = m_authmap->getResourceList(path.str());
    if(rlist == NULL)
        return false;
    request->queryContext()->setResources(rlist);

    return true;
}

void EspHttpBinding::populateRequest(CHttpRequest *request)
{
    IEspContext* ctx = request->queryContext();

    ctx->setSecManger(m_secmgr.getLink());
    ctx->setFeatureAuthMap(m_feature_authmap.getLink());

    StringBuffer userid, password,realm,peer;
    ctx->getUserID(userid);

    if(m_secmgr.get() == NULL)
    {
        return;
    }


    ISecUser *user = m_secmgr->createUser(userid.str());
    if(user == NULL)
    {
        WARNLOG("Couldn't create ISecUser object for %s", userid.str());
        return;
    }

    ctx->getPassword(password);
    user->credentials().setPassword(password.str());

    ctx->getRealm(realm);
    user->setRealm(realm.str());

    ctx->getPeer(peer);
    user->setPeer(peer.str());

    ctx->setUser(user);

    CEspCookie* cookie = request->queryCookie("SEQUEST");
    if(cookie)
    {
        const char* val = cookie->getValue();
        if(val && *val)
            user->setProperty("SEQUEST", val);
    }
    cookie = request->queryCookie("OTP2FACTOR");
    if(cookie)
    {
        const char* val = cookie->getValue();
        if(val && *val)
            user->setProperty("OTP2FACTOR", val);
    }

    if(m_setting_authmap.get() != NULL)
    {
        ISecResourceList* settinglist = m_setting_authmap->getResourceList("*");
        if(settinglist == NULL)
            return ;
        if (getEspLogLevel()>=LogMax)
        {
            StringBuffer s;
            DBGLOG("Set security settings: %s", settinglist->toString(s).str());
        }
        ctx->setSecuritySettings(settinglist);
    }
    return ;
}

bool EspHttpBinding::doAuth(IEspContext* ctx)
{
    if(m_authtype.length() == 0 || stricmp(m_authtype.str(), "Basic") == 0)
    {
        return basicAuth(ctx);
    }

    return false;
}

bool EspHttpBinding::basicAuth(IEspContext* ctx)
{
    StringBuffer userid;
    ctx->getUserID(userid);
    if(userid.length() == 0)
    {
        return false;
    }

    if(stricmp(m_authmethod.str(), "UserDefined") == 0)
        return true;

    ISecUser *user = ctx->queryUser();
    if(user == NULL)
    {
        WARNLOG("Can't find user in context");
        ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", "Access Denied: No username provided");
        return false;
    }

    if(m_secmgr.get() == NULL)
    {
        WARNLOG("No mechanism established for authentication");
        return false;
    }

    ISecResourceList* rlist = ctx->queryResources();
    if(rlist == NULL)
        return false;

    bool authenticated = m_secmgr->authorize(*user, rlist);
    if(!authenticated)
    {
        if (user->getAuthenticateStatus() == AS_PASSWORD_EXPIRED || user->getAuthenticateStatus() == AS_PASSWORD_VALID_BUT_EXPIRED)
            ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", "ESP password is expired");
        else
            ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", "Access Denied: User or password invalid");
        return false;
    }
    bool authorized = true;
    for(int i = 0; i < rlist->count(); i++)
    {
        ISecResource* curres = rlist->queryResource(i);
        if(curres != NULL)
        {
            int access = (int)curres->getAccessFlags();
            int required = (int)curres->getRequiredAccessFlags();
            if(access < required)
            {
                const char *desc=curres->getDescription();
                ESPLOG(LogMin, "Access for user '%s' denied to: %s. Access=%d, Required=%d", user->getName(), desc?desc:"<no-desc>", access, required);
                ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: Not Authorized", "Resource: %s [%s]", curres->getName(), (desc) ? desc : "");
                authorized = false;
                break;
            }
        }
    }

    if(authorized==false)
        return false;

    ISecPropertyList* securitySettings = ctx->querySecuritySettings();
    if(securitySettings == NULL)
        return authorized;

    m_secmgr->updateSettings(*user,securitySettings);

    ctx->addTraceSummaryTimeStamp("basicAuth");
    return authorized;
}
    
void EspHttpBinding::handleHttpPost(CHttpRequest *request, CHttpResponse *response)
{
    if(request->isSoapMessage()) 
    {
        request->queryParameters()->setProp("__wsdl_address", m_wsdlAddress.str());
        onSoapRequest(request, response);
    }
    else if(request->isFormSubmission())
        onPostForm(request, response);
    else
        onPost(request, response);
}

int EspHttpBinding::onGet(CHttpRequest* request, CHttpResponse* response)
{
    IEspContext& context = *request->queryContext();

    // At this time, the request is already received and fully passed, and
    // the user authenticated
    LogLevel level = getEspLogLevel(&context);
    if (level >= LogNormal)
        DBGLOG("EspHttpBinding::onGet");
    
    response->setVersion(HTTP_VERSION);
    response->addHeader("Expires", "0");

    response->setStatus(HTTP_STATUS_OK);
    
    sub_service sstype = sub_serv_unknown;

    StringBuffer pathEx;
    StringBuffer serviceName;
    StringBuffer methodName;
    StringBuffer paramStr;
    
    request->getEspPathInfo(sstype, &pathEx, &serviceName, &methodName, false);

    // adjust version if necessary
    if (m_defaultSvcVersion.get() && !context.queryRequestParameters()->queryProp("ver_"))
    {
        switch(sstype)
        {
        case sub_serv_root:
        case sub_serv_main:
        case sub_serv_index:
        case sub_serv_xform:
        case sub_serv_xsd:
        case sub_serv_wsdl:
        case sub_serv_soap_builder:
        case sub_serv_reqsamplexml:
        case sub_serv_respsamplexml:
            context.setClientVersion(atof(m_defaultSvcVersion));

        default:
            break;
        }
    }

    switch (sstype)
    {
        case sub_serv_root:
        case sub_serv_main:
            return onGetRoot(context, request, response);
        case sub_serv_config:
            return onGetConfig(context, request, response);
        case sub_serv_getversion:
            return onGetVersion(context, request, response, serviceName.str());
        case sub_serv_index:
            return onGetIndex(context, request, response, serviceName.str());
        case sub_serv_files:
            checkInitEclIdeResponse(request, response);
            return onGetFile(context, request, response, pathEx.str());
        case sub_serv_itext:
            return onGetItext(context, request, response, pathEx.str());
        case sub_serv_iframe:
            return onGetIframe(context, request, response, pathEx.str());
        case sub_serv_content:
            return onGetContent(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_method:
            return onGetService(context, request, response, serviceName.str(), methodName.str(), pathEx.str());
        case sub_serv_form:
            return onGetForm(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_xform:
            return onGetXForm(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_result:
            return onGetResult(context, request, response, serviceName.str(), methodName.str(), pathEx.str());
        case sub_serv_wsdl:
            return onGetWsdl(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_xsd:
            return onGetXsd(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_instant_query:
            return onGetInstantQuery(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_soap_builder:
            return onGetSoapBuilder(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_reqsamplexml:
            return onGetReqSampleXml(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_respsamplexml:
            return onGetRespSampleXml(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_query:
            return onGetQuery(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_file_upload:
            return onStartUpload(context, request, response, serviceName.str(), methodName.str());
        default:
            return onGetNotFound(context, request,  response, serviceName.str());
    }

    return 0;
}

int EspHttpBinding::onGetStaticIndex(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, const char *serv)
{
    return onGetNotFound(context, request,  response, serv);
}



#if 1

//============================================================
// Filter out XML by schema using Schema parser without indent

static void setStartTag(IPTree* in, IXmlType* type, const char* tag,StringBuffer& out)
{
    int nAttrs = type->getAttrCount();
    out.appendf("<%s", tag);
    for (int attr=0; attr<nAttrs; attr++)
    {
        IXmlAttribute* att = type->queryAttr(attr);
        VStringBuffer prop("@%s", att->queryName());
        const char* val = in->queryProp(prop);
        if (val && *val)
        {
            StringBuffer encoded;
            encodeXML(val, encoded);
            out.appendf(" %s=\"%s\"", att->queryName(), encoded.str());
        }
    }
    out.append('>');
}

static void filterXmlBySchema(IPTree* in, IXmlType* type, const char* tag, StringBuffer& out)
{
    assertex(type);

    if (type->isComplexType())
    {
        setStartTag(in,type,tag,out);

        int flds = type->getFieldCount();
        for (int i=0; i<flds; i++)
        {
            const char* fldName = type->queryFieldName(i);
            IPTree* fld = in ? in->queryBranch(fldName) : NULL;
            filterXmlBySchema(fld,type->queryFieldType(i),fldName,out);
        }
        if (flds==0)
        {
            if (type->getSubType() == SubType_Complex_SimpleContent) // the first field is baseType
            {
                const char* val = in->queryProp(".");
                if (val && *val) {
                    StringBuffer encoded;
                    encodeXML(val, encoded);
                    out.append(encoded);
                }
            }
        }
        out.appendf("</%s>",tag);
    }
    else if (type->isArray())
    {
        setStartTag(in,type,tag,out);
        //check xml first to decide items, if 0, check schema, generate 1 item by default
        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
        {
            VStringBuffer s("*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");
            out.append(s);
            ERRLOG("%s", s.str());
            return;
        }

        bool hasChild = false;
        if (in)
        {
            Owned<IPTreeIterator> it = in->getElements(itemName);
            for (it->first(); it->isValid(); it->next())
            {
                hasChild = true;
                filterXmlBySchema(&it->query(), itemType, itemName, out);
            }
        }

        if (!hasChild)
        {
            filterXmlBySchema(NULL, itemType, itemName, out);
        }
        out.appendf("</%s>",tag);
    }
    else // simple type
    {
        setStartTag(in,type,tag,out);
        if (in)
        {
            const char* value = in->queryProp(NULL);
            if (value)
                encodeUtf8XML(value,out);
        }
        out.appendf("</%s>", tag);
    }
}

static void filterXmlBySchema(StringBuffer& in, StringBuffer& schema, StringBuffer& out)
{
    Owned<IXmlSchema> sp = createXmlSchema(schema);
    Owned<IPTree> tree = createPTreeFromXMLString(in);

    //VStringBuffer name("tns:%s", tree->queryName());
    const char* name = tree->queryName();
    IXmlType* type = sp->queryElementType(name);
    if (!type)
    {
        StringBuffer method(strlen(name)-7, name);
        type = sp->queryElementType(method);
    }

    if (type)
        filterXmlBySchema(tree,type,name,out);
    else 
    {
        const char* value = tree->queryProp(NULL);
        DBGLOG("Unknown xml tag ignored: <%s>%s</%s>", name, value?value:"", name);
    }
}

// new way to generate soap message
void EspHttpBinding::getSoapMessage(StringBuffer& soapmsg, IEspContext& ctx, CHttpRequest* request, const char *serv, const char *method)
{
    StringBuffer reqName(serv);
    reqName.append("Request");  
    Owned<IRpcMessage> msg = new CRpcMessage(reqName.str());
    msg->setContext(&ctx);

    Owned<IRpcRequestBinding> rpcreq = createReqBinding(ctx, request, serv, method);
    rpcreq->serialize(*msg);

    StringBuffer req, schema, filtered;
    msg->marshall(req, NULL);

    getSchema(schema,ctx,request,serv,method,false);
    filterXmlBySchema(req,schema,filtered);
    
    StringBuffer ns;
    soapmsg.appendf(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
          " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\""
          " xmlns=\"%s\">"
        " <soap:Body>%s </soap:Body></soap:Envelope>",
        generateNamespace(ctx, request, serv, method, ns).str(), filtered.str()
        );
}

#else

//=========================================================
// Filter out XML by schema using Schema parser with indent

static void filterXmlBySchema(IPTree* in, IXmlType* type, const char* tag, StringBuffer& out,int indent)
{
    assertex(type);

    if (type->isComplexType())
    {
        out.pad(indent).appendf("<%s>\n", tag);
        int flds = type->getFieldCount();
        for (int i=0; i<flds; i++)
        {
            const char* fldName = type->queryFieldName(i);
            IPTree* fld = in ? in->queryBranch(fldName) : NULL;
            filterXmlBySchema(fld,type->queryFieldType(i),fldName,out,indent+1);
        }
        out.pad(indent).appendf("</%s>\n",tag);
    }
    else if (type->isArray())
    {
        out.pad(indent).appendf("<%s>\n", tag);
        //check xml first to decide items, if 0, check schema, generate 1 item by default
        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
        {
            VStringBuffer s("*** Invalid array definition: tag=%s, indent=%d, itemName=%s", tag, indent,itemName?itemName:"NULL");
            out.append(s);
            ERRLOG(s);
            return;
        }

        bool hasChild = false;
        if (in)
        {
            Owned<IPTreeIterator> it = in->getElements(itemName);
            for (it->first(); it->isValid(); it->next())
            {
                hasChild = true;
                filterXmlBySchema(&it->query(), itemType, itemName, out,indent+1);
            }
        }

        if (!hasChild)
        {
            filterXmlBySchema(NULL, itemType, itemName, out,indent+1);
        }
        out.pad(indent).appendf("</%s>\n",tag);
    }
    else // simple type
    {
        out.pad(indent).appendf("<%s>", tag);
        if (in)
        {
            const char* value = in->queryProp(NULL);
            if (value)
                encodeUtf8XML(value,out);
        }
        out.appendf("</%s>\n", tag);
    }
}

static void filterXmlBySchema(StringBuffer& in, StringBuffer& schema, StringBuffer& out,int indent)
{
    Owned<IXmlSchema> sp = createXmlSchema(schema);
    Owned<IPTree> tree = createPTreeFromXMLString(in);

    //VStringBuffer name("tns:%s", tree->queryName());
    const char* name = tree->queryName();
    IXmlType* type = sp->queryElementType(name);
    if (!type)
    {
        StringBuffer method(strlen(name)-7, name);
        type = sp->queryElementType(method);
    }

    if (type)
        filterXmlBySchema(tree,type,name,out,indent);
    else 
    {
        const char* value = tree->queryProp(NULL);
        DBGLOG("Unknown xml tag ignored: <%s>%s</%s>", name, value?value:"", name);
    }
}

void EspHttpBinding::getSoapMessage(StringBuffer& soapmsg, IEspContext& ctx, CHttpRequest* request, const char *serv, const char *method)
{
    StringBuffer reqName(serv);
    reqName.append("Request");
    Owned<IRpcMessage> msg = new CRpcMessage(reqName.str());

    Owned<IRpcRequestBinding> rpcreq = createReqBinding(ctx, request, serv, method);
    rpcreq->serialize(*msg);

    // use schema to filter out fields that are internal, not belong to the version etc
    StringBuffer req,schema,filtered;
    msg->marshall(req, NULL);
    getSchema(schema,ctx,request,serv,method,false);
    //DBGLOG("Schema: %s", schema.str());
    filterXmlBySchema(req,schema,filtered,2);
    
    soapmsg.appendf(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
            " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\""
            " xmlns=\"urn:hpccsystems:ws:");
    if (serv && *serv)
        soapmsg.appendLower(strlen(serv), serv);
    soapmsg.appendf("\">\n <soap:Body>\n%s </soap:Body>\n</soap:Envelope>", filtered.str());
}

#endif



int EspHttpBinding::onGetSoapBuilder(IEspContext &context, CHttpRequest* request, CHttpResponse* response,  const char *serv, const char *method)
{   
    StringBuffer soapmsg, serviceQName, methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName)
        || methodQName.length()==0)
        throw createEspHttpException(HTTP_STATUS_BAD_REQUEST_CODE, "Bad Request", HTTP_STATUS_BAD_REQUEST);

    getSoapMessage(soapmsg,context,request,serviceQName,methodQName);
    
    //put all URL parameters into dest
    
    StringBuffer params;    
    const char* excludes[] = {"soap_builder_",NULL};
    
    getEspUrlParams(context,params,excludes);

    VStringBuffer dest("%s/%s?%s", serviceQName.str(), methodQName.str(), params.str()); 

    VStringBuffer header("SOAPAction: \"%s\"\n", dest.str()); 
    header.append("Content-Type: text/xml; charset=UTF-8");

    IXslProcessor* xslp = getXmlLibXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform();
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/soap_page.xsl").str());
    xform->setXmlSource("<xml/>", 6);

    // params
    xform->setStringParameter("serviceName", serviceQName.str());
    xform->setStringParameter("methodName", methodQName.str());
    xform->setStringParameter("soapbody", soapmsg.str());
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    VStringBuffer url("%s?%s", methodQName.str(), params.str()); 
    xform->setStringParameter("destination", url.str());
        
    StringBuffer page;
    xform->transform(page);     

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

int EspHttpBinding::onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    return onGetNotFound(context, request,  response, serv);
}
int EspHttpBinding::onGetResult(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method, const char *pathex)
{
    return onGetNotFound(context, request,  response, serv);
}
int EspHttpBinding::onGetResultPresentation(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method, StringBuffer &xmlResult)
{
    return onGetNotFound(context, request,  response, serv);
}
int EspHttpBinding::onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *path)
{
    return onGetNotFound(context, request,  response, NULL);
}

int EspHttpBinding::onGetItext(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *path)
{
    StringBuffer title;
    request->getParameter("text", title);
    StringBuffer content;
    if (checkInitEclIdeResponse(request, response))
        content.append("<!DOCTYPE html>"); //may be safe for all browsers? but better to be safe for now?
    content.append("<html><head>");
    if(title.length() > 0)
        content.appendf("<title>%s</title>", title.str());
    content.appendf("</head><body>"
        "<img src=\"files_/img/topurl.png\" onClick=\"top.location.href=parent.frames['imain'].location.href\" alt=\"No Frames\" width=\"13\" height=\"15\">"
        "&nbsp;&nbsp;<font size=+1><b>%s</b></font></body></html>", title.str());
    response->setContent(content.length(), content.str());
    response->setContentType("text/html");
    response->send();
    return 0;
}
int EspHttpBinding::onGetIframe(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *path)
{
    StringBuffer title;
    request->getParameter("esp_iframe_title", title);
    StringBuffer content;
    if (checkInitEclIdeResponse(request, response))
        content.append("<!DOCTYPE html>"); //may be safe for all browsers? but better to be safe for now?
    content.append("<html><head>");
    if(title.length() > 0)
        content.appendf("<title>%s</title>", title.str());
    content.append("</head>");
    
    content.append("<frameset rows=\"35,*\" FRAMEPADDING=\"0\" PADDING=\"0\" SPACING=\"0\" FRAMEBORDER=\"0\">");
    content.appendf("<frame src=\"itext?text=%s\" name=\"ititle\" target=\"imain\" scrolling=\"no\"/>", title.str());
    
    //content.appendf("<frame name=\"imain\" target=\"main\" src=\"../%s?%s\" scrolling=\"auto\" frameborder=\"0\" noresize=\"noresize\"/>", path, request->queryParamStr());   
    StringBuffer inner;
    request->getParameter("inner", inner);
    StringBuffer plainText;
    request->getParameter("PlainText", plainText);
    if (plainText.length() > 0)
        inner.appendf("&PlainText=%s", plainText.str());
            
    ESPLOG(LogNormal,"Inner: %s", inner.str());
    ESPLOG(LogNormal,"Param: %s", request->queryParamStr());
    content.appendf("<frame name=\"imain\" target=\"main\" src=\"%s\" scrolling=\"auto\" frameborder=\"0\" noresize=\"noresize\"/>", inner.str());
    content.append("</frameset>");
    content.append("</body></html>");
    
    response->setContent(content.length(), content.str());
    response->setContentType("text/html");
    response->send();
    return 0;
}
int EspHttpBinding::onGetContent(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    return onGetNotFound(context, request,  response, serv);
}

int EspHttpBinding::onGetConfig(IEspContext &context, CHttpRequest* request, CHttpResponse* response)
{
    ISecUser* user = context.queryUser();
    if (m_viewConfig || (user && (user->getStatus()==SecUserStatus_Inhouse)))
    {
        ESPLOG(LogNormal, "Get config file: %s", m_configFile.get());

        StringBuffer content;
        xmlContentFromFile(m_configFile, "/esp/xslt/xmlformatter.xsl", content);
        response->setContent(content.str());
        response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
        response->setStatus(HTTP_STATUS_OK);
        response->send();
        return 0;
    }
    DBGLOG("Config access denied");
    return onGetNotFound(context, request, response, NULL);
}


int EspHttpBinding::onGetVersion(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service)
{
    StringBuffer verxml;
    StringBuffer srvQName;
    qualifyServiceName(context, service, NULL, srvQName, NULL);
    verxml.appendf("<VersionInfo><Service>%s</Service><Version>%.3f</Version></VersionInfo>", srvQName.str(), m_wsdlVer);
    
    response->setContent(verxml.str());
    response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return 0;
}

int EspHttpBinding::onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    return getWsdlOrXsd(context,request,response,service,method,false);
}

int EspHttpBinding::onGetWsdl(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    return getWsdlOrXsd(context,request,response,service,method,true);
}

bool EspHttpBinding::getSchema(StringBuffer& schema, IEspContext &ctx, CHttpRequest* req, const char *service, const char *method, bool standalone) 
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(ctx, service, method, serviceQName, &methodQName))
        return false;

    const char *sqName = serviceQName.str();
    const char *mqName = methodQName.str();

    Owned<IPropertyTree> namespaces = createPTree();
    appendSchemaNamespaces(namespaces, ctx, req, service, method);
    Owned<IPropertyTreeIterator> nsiter = namespaces->getElements("namespace");

    StringBuffer nstr;
    generateNamespace(ctx, req, sqName, mqName, nstr);
    schema.appendf("<xsd:schema elementFormDefault=\"qualified\" targetNamespace=\"%s\" ", nstr.str());
    if (standalone)
        schema.appendf(" xmlns:tns=\"%s\"  xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"", nstr.str());
    ForEach(*nsiter)
    {
        IPropertyTree &ns = nsiter->query();
        schema.appendf(" xmlns:%s=\"%s\"", ns.queryProp("@nsvar"), ns.queryProp("@ns"));
    }
    schema.append(">\n");
    ForEach(*nsiter)
    {
        IPropertyTree &ns = nsiter->query();
        if (ns.hasProp("@import"))  
            schema.appendf("<xsd:import namespace=\"%s\" schemaLocation=\"%s\"/>", ns.queryProp("@ns"), ns.queryProp("@location"));
    }
    

    schema.append(
        "<xsd:complexType name=\"EspException\">"
            "<xsd:all>"
                "<xsd:element name=\"Code\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Audience\" type=\"xsd:string\" minOccurs=\"0\"/>"
                "<xsd:element name=\"Source\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Message\" type=\"xsd:string\" minOccurs=\"0\"/>"
            "</xsd:all>"
        "</xsd:complexType>\n"
        "<xsd:complexType name=\"ArrayOfEspException\">"
            "<xsd:sequence>"
                "<xsd:element name=\"Source\" type=\"xsd:string\"  minOccurs=\"0\"/>"
                "<xsd:element name=\"Exception\" type=\"tns:EspException\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>"
            "</xsd:sequence>"
        "</xsd:complexType>\n"
        "<xsd:element name=\"Exceptions\" type=\"tns:ArrayOfEspException\"/>\n"
    );

    if (ctx.queryOptions()&ESPCTX_WSDL_EXT)
    {
        schema.append(          
            "<xsd:complexType name=\"EspSecurityInfo\">"
                "<xsd:all>"
                    "<xsd:element name=\"UsernameToken\" minOccurs=\"0\">"
                        "<xsd:complexType>"
                            "<xsd:all>"
                                "<xsd:element name=\"Username\" minOccurs=\"0\"/>"
                                "<xsd:element name=\"Password\" minOccurs=\"0\"/>"
                            "</xsd:all>"
                        "</xsd:complexType>"
                    "</xsd:element>"
                    "<xsd:element name=\"RealmToken\" minOccurs=\"0\">"
                        "<xsd:complexType>"
                            "<xsd:all>"
                                "<xsd:element name=\"Realm\" minOccurs=\"0\"/>"
                            "</xsd:all>"
                        "</xsd:complexType>"
                    "</xsd:element>"
                "</xsd:all>"
              "</xsd:complexType>"
            "<xsd:element name=\"Security\" type=\"tns:EspSecurityInfo\"/>\n"
        );
    }

    bool mda=(req->queryParameters()->getPropInt("mda")!=0);
    getXsdDefinition(ctx, req, schema, sqName, mqName, mda);

    schema.append("<xsd:element name=\"string\" nillable=\"true\" type=\"xsd:string\" />\n");
    schema.append("</xsd:schema>");

    return true;
}

int EspHttpBinding::getWsdlOrXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method, bool isWsdl)
{
    bool mda=(request->queryParameters()->getPropInt("mda")!=0);
    try
    {
        StringBuffer serviceQName;
        StringBuffer methodQName;

        if (!qualifyServiceName(context, service, method, serviceQName, &methodQName))
        {
            response->setStatus(HTTP_STATUS_NOT_FOUND);
        }
        else
        {
            const char *sqName = serviceQName.str();
            const char *mqName = methodQName.str();
            StringBuffer ns;
            generateNamespace(context, request, serviceQName.str(), methodQName.str(), ns);

            StringBuffer content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if (context.queryRequestParameters()->hasProp("display"))
                content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");
            else if (isWsdl && context.queryRequestParameters()->hasProp("wsdlviewer"))
                content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/wsdl-viewer.xsl\"?>");
            if (isWsdl)
            {
                content.appendf("<definitions xmlns=\"http://schemas.xmlsoap.org/wsdl/\" xmlns:soap=\"http://schemas.xmlsoap.org/wsdl/soap/\" xmlns:http=\"http://schemas.xmlsoap.org/wsdl/http/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\""
                    " xmlns:mime=\"http://schemas.xmlsoap.org/wsdl/mime/\" xmlns:tns=\"%s\""
                    " targetNamespace=\"%s\">", ns.str(), ns.str());
                content.append("<types>");
            }

            getSchema(content,context,request,service,method,!isWsdl);

            if (isWsdl)
            {
                content.append("</types>");

                getWsdlMessages(context, request, content, sqName, mqName, mda);
                getWsdlPorts(context, request, content, sqName, mqName, mda);
                getWsdlBindings(context, request, content, sqName, mqName, mda);
                
                StringBuffer location(m_wsdlAddress.str());
                if (request->queryParameters()->hasProp("wsdl_destination_path"))
                    location.append(request->queryParameters()->queryProp("wsdl_destination_path"));
                else
                    location.append('/').append(sqName).appendf("?ver_=%g", context.getClientVersion());
                
                if (request->queryParameters()->hasProp("encode_results"))
                {
                    const char *encval = request->queryParameters()->queryProp("encode_results");
                    location.append("&amp;").appendf("encode_=%s", (encval && *encval) ? encval : "1");
                }

                content.appendf("<service name=\"%s\">", sqName);
                content.appendf("<port name=\"%sServiceSoap\" binding=\"tns:%sServiceSoap\">", sqName, sqName);
                content.appendf("<soap:address location=\"%s\"/>", location.str());
                content.append("</port>");
                content.append("</service>");
                content.append("</definitions>");
            }

            response->setContent(content.length(), content.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
            response->setStatus(HTTP_STATUS_OK);
        }
    }
    catch (IException *e)
    {
        return onGetException(context, request, response, *e);
    }

    response->send();
    return 0;
}

static void genSampleXml(StringStack& parent, IXmlType* type, StringBuffer& out, const char* tag, const char* ns=NULL)
{
    assertex(type!=NULL);

    const char* typeName = type->queryName();

    if (type->isComplexType())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive
        
        out.appendf("<%s", tag);
        if (ns)
            out.append(' ').append(ns);
        for (unsigned i=0; i<type->getAttrCount(); i++)
        {
            IXmlAttribute* attr = type->queryAttr(i);
            out.appendf(" %s='", attr->queryName());
            attr->getSampleValue(out);
            out.append('\'');
        }
        out.append('>');
        if (typeName)
            parent.push_back(typeName);
        
        int flds = type->getFieldCount();
        
        switch (type->getSubType())
        {
        case SubType_Complex_SimpleContent:
            assertex(flds==0);
            type->queryFieldType(0)->getSampleValue(out,tag);
            break;

        default:
            for (int idx=0; idx<flds; idx++)
                genSampleXml(parent,type->queryFieldType(idx),out,type->queryFieldName(idx));
            break;
        }

        if (typeName)
            parent.pop_back();      
        out.appendf("</%s>",tag);
    }
    else if (type->isArray())
    {
        if (typeName && std::find(parent.begin(),parent.end(),typeName) != parent.end())
            return; // recursive

        const char* itemName = type->queryFieldName(0);
        IXmlType*   itemType = type->queryFieldType(0);
        if (!itemName || !itemType)
            throw MakeStringException(-1,"*** Invalid array definition: tag=%s, itemName=%s", tag, itemName?itemName:"NULL");

        StringBuffer item;
        if (typeName)
            parent.push_back(typeName);
        genSampleXml(parent,itemType,item,itemName);
        if (typeName)
            parent.pop_back();      

        // gen two items
        out.appendf("<%s>%s%s</%s>", tag,item.str(),item.str(),tag);
    }
    else // simple type
    {
        out.appendf("<%s>", tag);
        type->getSampleValue(out,NULL);
        out.appendf("</%s>", tag);
    }
}

void EspHttpBinding::generateSampleXml(bool isRequest, IEspContext &context, CHttpRequest* request, CHttpResponse* response,    const char *serv, const char *method)
{
    StringBuffer serviceQName, methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
        return;

    MethodInfoArray info;
    getQualifiedNames(context, info);
    StringBuffer element;
    for (unsigned i=0; i<info.length(); i++)
    {
        CMethodInfo& m = info.item(i);
        if (stricmp(m.m_label, methodQName)==0)
        {
            element.set(isRequest ? m.m_requestLabel : m.m_responseLabel);
            break;
        }
    }

    if (!element.length())
        element.append(methodQName.str()).append(isRequest ? "Request" : "Response");

    StringBuffer schemaXml;

    getSchema(schemaXml,context,request,serv,method,false);
    Owned<IXmlSchema> schema = createXmlSchema(schemaXml);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType(element);
        if (type)
        {
            StringBuffer content;
            StringStack parent;
            StringBuffer nsdecl("xmlns=\"");

            content.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if (context.queryRequestParameters()->hasProp("display"))
                content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

            genSampleXml(parent,type, content, element, generateNamespace(context, request, serviceQName.str(), methodQName.str(), nsdecl).append('\"').str());
            response->setContent(content.length(), content.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return;
        }
    }

    throw MakeStringException(-1,"Unknown type: %s", element.str());
}

void EspHttpBinding::generateSampleXmlFromSchema(bool isRequest, IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method, const char * schemaxml)
{
    StringBuffer serviceQName, methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
        return;

    MethodInfoArray info;
    getQualifiedNames(context, info);
    StringBuffer element;
    for (unsigned i=0; i<info.length(); i++)
    {
        CMethodInfo& m = info.item(i);
        if (stricmp(m.m_label, methodQName)==0)
        {
            element.set(isRequest ? m.m_requestLabel : m.m_responseLabel);
            break;
        }
    }

    if (!element.length())
        element.append(methodQName.str()).append(isRequest ? "Request" : "Response");

    StringBuffer schemaXmlbuff(schemaxml);

    Owned<IXmlSchema> schema = createXmlSchema(schemaXmlbuff);
    if (schema.get())
    {
        IXmlType* type = schema->queryElementType(element);
        if (type)
        {
            StringBuffer content;
            StringStack parent;
            StringBuffer nsdecl("xmlns=\"");

            content.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if (context.queryRequestParameters()->hasProp("display"))
                content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

            genSampleXml(parent,type, content, element, generateNamespace(context, request, serviceQName.str(), methodQName.str(), nsdecl).append('\"').str());
            response->setContent(content.length(), content.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return;
        }
    }

    throw MakeStringException(-1,"Unknown type: %s", element.str());
}

int EspHttpBinding::onGetReqSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    generateSampleXml(true, ctx, request, response, serv, method);
    return 0;
}

int EspHttpBinding::onGetRespSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    generateSampleXml(false, ctx, request, response, serv, method);
    return 0;
}

int EspHttpBinding::onStartUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringArray fileNames;
    Owned<IMultiException> me = MakeMultiException("FileSpray::UploadFile()");
    try
    {
        if (!ctx.validateFeatureAccess(FILE_UPLOAD, SecAccess_Full, false))
            throw MakeStringException(-1, "Permission denied.");

        StringBuffer netAddress, path;
        request->getParameter("NetAddress", netAddress);
        request->getParameter("Path", path);

        if ((netAddress.length() < 1) || (path.length() < 1))
            throw MakeStringException(-1, "Upload destination not specified.");

        request->readContentToFiles(netAddress, path, fileNames);
        return onFinishUpload(ctx, request, response, serv, method, fileNames, NULL);
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*MakeStringExceptionDirect(-1, "Unknown Exception"));
    }
    return onFinishUpload(ctx, request, response, serv, method, fileNames, me);
}

int EspHttpBinding::onFinishUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response,    const char *serv, const char *method, StringArray& fileNames, IMultiException *me)
{
    response->setContentType("text/html; charset=UTF-8");
    StringBuffer content(
        "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
            "<head>"
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
                "<title>Enterprise Services Platform</title>"
            "</head>"
            "<body>"
                "<div id=\"DropzoneFileData\">");
    if (!me || (me->ordinality()==0))
        content.append("<br/><b>File has been uploaded.</b>");
    else
    {
        StringBuffer msg;
        WARNLOG("Exception(s) in EspHttpBinding::onFinishUpload - %s", me->errorMessage(msg).append('\n').str());
        content.appendf("<br/><b>%s</b>", msg.str());
    }
    content.append("</div>"
            "</body>"
        "</html>");

    response->setContent(content.str());
    response->send();

    return 0;
}


int EspHttpBinding::getWsdlMessages(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    bool allMethods = (method==NULL || !*method);
    
    MethodInfoArray methods;
    getQualifiedNames(context, methods);
    int count=methods.ordinality();
    for (int indx=0; indx<count; indx++)
    {
        CMethodInfo &info = methods.item(indx);
        {
            if (allMethods || !Utils::strcasecmp(method, info.m_label.str()))
            {
                content.appendf("<message name=\"%sSoapIn\">", info.m_label.str());
                if (context.queryOptions()&ESPCTX_WSDL_EXT)
                    content.appendf("<part name=\"security\" element=\"tns:Security\"/>");
                content.appendf("<part name=\"parameters\" element=\"tns:%s\"/>", info.m_requestLabel.str());
                content.append("</message>");

                content.appendf("<message name=\"%sSoapOut\">", info.m_label.str());
                content.appendf("<part name=\"parameters\" element=\"tns:%s\"/>", info.m_responseLabel.str());
                content.append("</message>");
            }
        }
    }
    content.append("<message name=\"EspSoapFault\">"
        "<part name=\"parameters\" element=\"tns:Exceptions\"/>"
        "</message>");

    return 0;
}

int EspHttpBinding::getWsdlPorts(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    StringBuffer serviceName;
    StringBuffer methName;
    qualifyServiceName(context, service, method, serviceName, &methName);

    content.appendf("<portType name=\"%sServiceSoap\">", serviceName.str());

    bool allMethods = (method==NULL || !*method);
    MethodInfoArray methods;
    getQualifiedNames(context, methods);
    int count=methods.ordinality();
    for (int indx=0; indx<count; indx++)
    {
        CMethodInfo &info = methods.item(indx);
        {
            if (allMethods || !Utils::strcasecmp(method, info.m_label.str()))
            {
                content.appendf("<operation name=\"%s\">", info.m_label.str());
                content.appendf("<input message=\"tns:%sSoapIn\"/>", info.m_label.str());
                content.appendf("<output message=\"tns:%sSoapOut\"/>", info.m_label.str());
                content.appendf("<fault name=\"excfault\" message=\"tns:EspSoapFault\"/>");
                content.append("</operation>");
                if (!allMethods) // no need to continue
                    break;
            }
        }
    }
    content.append("</portType>");
    return 0;
}


int EspHttpBinding::getWsdlBindings(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda)
{
    StringBuffer serviceName;
    StringBuffer methName;
    qualifyServiceName(context, service, method, serviceName, &methName);

    content.appendf("<binding name=\"%sServiceSoap\" type=\"tns:%sServiceSoap\">", serviceName.str(), serviceName.str());
    content.append("<soap:binding transport=\"http://schemas.xmlsoap.org/soap/http\" style=\"document\"/>");
    
    bool allMethods = (method==NULL || !*method);
    MethodInfoArray methods;
    getQualifiedNames(context, methods);
    int count=methods.ordinality();
    for (int indx=0; indx<count; indx++)
    {
        CMethodInfo &info = methods.item(indx);
        {
            if (allMethods || !Utils::strcasecmp(method, info.m_label.str()))
            {
                content.appendf("<operation name=\"%s\">", info.m_label.str());
                //content.appendf("<soap:operation soapAction=\"%s/%s?ver_=%g\" style=\"document\"/>", serviceName.str(), info.m_label.str(), getWsdlVersion());
                content.appendf("<soap:operation soapAction=\"%s/%s?ver_=%g\" style=\"document\"/>", serviceName.str(), info.m_label.str(), context.getClientVersion());
                content.append("<input>");
                if (context.queryOptions()&ESPCTX_WSDL_EXT)
                    content.appendf("<soap:header message=\"tns:%sSoapIn\" part=\"security\" use=\"literal\"/>", info.m_label.str());
                content.append("<soap:body use=\"literal\"/></input>");
                content.append("<output><soap:body use=\"literal\"/></output>");
                content.append("<fault  name=\"excfault\"><soap:fault name=\"excfault\" use=\"literal\"/></fault>");
        
                content.append("</operation>");
                if (!allMethods) // no need to continue
                    break;
            }
        }
    }
    content.append("</binding>");
    return 0;
}

int EspHttpBinding::onGetNotFound(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, const char *service)
{
    HtmlPage page("Enterprise Services Platform");
    page.appendContent(new CHtmlHeader(H1, "Page Not Found"));

    StringBuffer content;
    page.getHtml(content);

    response->setContent(content.length(), content.str());
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_NOT_FOUND);

    response->send();

    return 0;
}

int EspHttpBinding::onGetException(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, IException &e)
{
    HtmlPage page("Enterprise Services Platform");

    StringBuffer msg("Exception: ");
    msg.appendf(" %d ", e.errorCode());
    e.errorMessage(msg);

    page.appendContent(new CHtmlHeader(H1, msg.str()));

    StringBuffer content;
    page.getHtml(content);

    response->setContent(content.length(), content.str());
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);

    response->send();

    return 0;
}

int EspHttpBinding::onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
{
    StringBuffer serviceName;
    getServiceName(serviceName);
    DBGLOG("CWsADLSoapBindingEx::onGetRoot");

    StringBuffer htmlStr;
    htmlStr.appendf(
        "<html>"
            "<head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"><title>%s</title></head>"
            "<body><p align=\"center\"><font size=\"55\" color=\"#999999\">%s</font></p></body>"
        "</html>", serviceName.str(), serviceName.str());
    response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
    response->setContent(htmlStr.length(), (const char*) htmlStr.str());
    response->send();

    return 0;
}

int EspHttpBinding::onGetIndex(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, const char *service)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(context, service, "", serviceQName, &methodQName))
    {
        return onGetNotFound(context, request,  response, service);
    }
    else
    {
        double ver = context.getClientVersion();
        if (ver<=0)
            ver = getWsdlVersion();
        HtmlPage page("Enterprise Services Platform");

        page.appendContent(new CHtmlHeader(H1, serviceQName));

        const char* build_ver = getBuildVersion();
        VStringBuffer build("Version: %g, Build: %s", ver, build_ver);
        page.appendContent(new CHtmlHeader(H2, build));

        StringBuffer urlParams;
        if (!getUrlParams(context.queryRequestParameters(),urlParams))
            urlParams.appendf("%cver_=%g",(urlParams.length()>0) ? '&' : '?', ver);

        StringBuffer wsLink;
        CHtmlList * list=NULL;
        MethodInfoArray methods;
        getQualifiedNames(context, methods);

        if (supportGeneratedForms() || request->queryParameters()->hasProp("list_forms"))
        {
            page.appendContent(new CHtmlText("The following operations are supported:<br/>"));

            //links to the form pages
            list = (CHtmlList *)page.appendContent(new CHtmlList);
            StringBuffer urlFormParams(urlParams);
            urlFormParams.append(urlFormParams.length()>0 ? "&form" : "?form");
            for(int i = 0, tot = methods.length(); i < tot; ++i)
            {
                CMethodInfo &info = methods.item(i);
                {
                    wsLink.clear().appendf("%s%s", methods.item(i).m_label.str(),urlFormParams.str());
                    list->appendContent(new CHtmlLink(methods.item(i).m_label.str(), wsLink.str()));
                }
            }
        }
        
        page.appendContent(new CHtmlText("<br/>For a formal definition, please review the "));      
        urlParams.append(urlParams.length()>0 ? "&wsdl" : "?wsdl");

        wsLink.clear().appendf("%s", urlParams.str());
        page.appendContent(new CHtmlLink("WSDL Definition", wsLink.str()));

        page.appendContent(new CHtmlText(" for this service, <br/><br/>or the individual WSDL definitions for each operation: "));

        //links to the individual WSDL pages
        list = (CHtmlList *)page.appendContent(new CHtmlList);
        for(int i = 0, tot = methods.length(); i < tot; ++i)
        {
            CMethodInfo &info = methods.item(i);
            {
                wsLink.clear().appendf("%s%s", info.m_label.str(),urlParams.str());
                list->appendContent(new CHtmlLink(info.m_label.str(), wsLink.str()));
            }
        }

        StringBuffer content;
        page.getHtml(content);

        response->setContent(content.length(), content.str());
        response->setContentType("text/html; charset=UTF-8");
        response->setStatus(HTTP_STATUS_OK);

        response->send();
    }

    return 0;
}

// Interestingly, only single quote needs to HTML escape.
// ", <, >, & don't need escape.
void EspHttpBinding::escapeSingleQuote(StringBuffer& src, StringBuffer& escaped)
{
    for (const char* p = src.str(); *p!=0; p++)
    {
        if (*p == '\'')
            escaped.append("&apos;");
        else 
            escaped.append(*p);
    }
}

int EspHttpBinding::onGetXForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
    {
        return onGetNotFound(context, request,  response, serv);
    }
    else
    {
        StringBuffer page;
        IXslProcessor* xslp = getXmlLibXslProcessor();

        // get schema
        StringBuffer schema;
        context.addOptions(ESPCTX_ALL_ANNOTATION);
        getSchema(schema, context, request, serv, method, true);
        //DBGLOG("Schema: %s", schema.str());

        Owned<IXslTransform> xform = xslp->createXslTransform();
        xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/gen_form.xsl").str());
        xform->setXmlSource(schema.str(), schema.length()+1);

        // params
        xform->setStringParameter("serviceName", serviceQName);
        StringBuffer version;
        version.appendf("%g",context.getClientVersion());
        xform->setStringParameter("serviceVersion", version);

        StringBuffer methodExt(methodQName);
        if (context.queryRequestParameters()->hasProp("json"))
            methodExt.append(".json");
        xform->setStringParameter("methodName", methodExt);

        // pass params to form (excluding form and __querystring)
        StringBuffer params;
        if (!getUrlParams(context.queryRequestParameters(),params))
            params.appendf("%cver_=%g",(params.length()>0) ? '&' : '?', context.getClientVersion());
        xform->setStringParameter("queryParams", params.str());
        
        StringBuffer tmp,escaped;
        getMethodHelp(context, serviceQName, methodQName, tmp);
        escapeSingleQuote(tmp,escaped);
        xform->setStringParameter("methodHelp", escaped);

        getMethodDescription(context, serviceQName.str(), methodQName.str(), tmp.clear());
        escapeSingleQuote(tmp,escaped.clear());
        xform->setStringParameter("methodDesc", escaped);

        xform->setParameter("formOptionsAccess", m_formOptions?"1":"0");
        xform->setParameter("includeSoapTest", m_includeSoapTest?"1":"0");
        xform->setParameter("includeRoxieTest", m_roxieOption?"1":"0");

        // set the prop noDefaultValue param
        IProperties* props = context.queryRequestParameters();
        bool formInitialized = false;
        if (props) {
            Owned<IPropertyIterator> it = props->getIterator();
            for (it->first(); it->isValid(); it->next()) {
                const char* key = it->getPropKey();
                if (*key=='.') {
                    formInitialized = true;
                    break;
                }
            }
        }
        xform->setParameter("noDefaultValue", formInitialized ? "1" : "0");

        MethodInfoArray info;
        getQualifiedNames(context, info);
        for (unsigned i=0; i<info.length(); i++)
        {
            CMethodInfo& m = info.item(i);
            if (stricmp(m.m_label, methodQName)==0)
            {
                xform->setStringParameter("requestLabel", m.m_requestLabel);
                break;
            }
        }
        xform->transform(page);     
        response->setContentType("text/html");
        response->setContent(page.str());
    }

    response->send();

    return 0;
}

int EspHttpBinding::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
    {
        return onGetNotFound(context, request,  response, serv);
    }
    else
    {
        StringBuffer page;

        page.append(
            "<html>"
                "<head>"
                    "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                    "<title>ESP Service form</title>"
                    "<script language=\"JavaScript\" src=\"files_/calendar_xs.js\"></script>"
                    "<script language=\"JavaScript\" src=\"files_/hint.js\"></script>"
                "</head>"
                "<body>"
                    //"<table border=\"0\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" bgcolor=\"#000000\" height=\"108\">"
                    //  "<tr>"
                    //      "<td width=\"24%\" height=\"24\" bgcolor=\"#000000\">"
                    //          "<img border=\"0\" src=\"/");
                    //          page.append(serviceQName.str());
                    //          page.append("/files_/logo.gif\" width=\"258\" height=\"108\" />"
                    //      "</td>"
                    //  "</tr>"
                    //  "<tr>"
                    //      "<td width=\"24%\" height=\"24\" bgcolor=\"#AA0000\">"
                    //          "<p align=\"center\" />"
                    //          "<b>"
                    //              "<font color=\"#FFFFFF\" size=\"5\">Enterprise Services Platform<sup><font size=\"2\">TM</font></sup></font>"
                    //          "</b>"
                    //      "</td>"
                    //  "</tr>"
                    //"</table>"
                    //"<br />"
                    //"<br />"
                    "<p align=\"center\" />"
                    "<table cellSpacing=\"0\" cellPadding=\"1\" width=\"90%\" bgColor=\"#666666\" border=\"0\">"
                        "<tbody>"
                            "<tr align=\"middle\" bgColor=\"#666666\">"
                                "<td height=\"23\">"
                                    "<p align=\"left\">"
                                        "<font color=\"#efefef\">");
                                page.appendf("<b>%s [Version %g]</b>", serviceQName.str(), context.getClientVersion());
                                page.append("</font>"
                                    "</p>"
                                "</td>"
                            "</tr>"
                            "<tr bgColor=\"#ffcc66\">"
                                "<td height=\"3\">"
                                    "<p align=\"left\">");
                                            
                                page.appendf("<b>&gt; %s</b>", methodQName.str());
                                page.append("</p>"
                                "</td>"
                            "</tr>"
                            "<TR bgColor=\"#666666\">"
                                "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                    "<TBODY>"
                                        "<TR>"
                                            "<TD vAlign=\"center\" align=\"left\">"
                                                "<p align=\"left\"><br />");
                                                getMethodDescription(context, serviceQName.str(), methodQName.str(), page);
                                                page.append("<br/></p>"
                                            "</TD>"
                                        "</TR>"
                                    "</TBODY>"
                                "</TABLE>"
                            "</TR>"
                            "<TR bgColor=\"#666666\">"
                                "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                    "<TBODY>"
                                        "<TR>"
                                            "<TD vAlign=\"center\" align=\"left\">"
                                                "<p align=\"left\"><br />");
                                                getMethodHelp(context, serviceQName.str(), methodQName.str(), page);
                                                page.append("<br/><br/></p>"
                                            "</TD>"
                                        "</TR>"
                                    "</TBODY>"
                                "</TABLE>"
                            "</TR>"
                            "<TR bgColor=\"#666666\">"
                                "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                    "<TBODY>"
                                        "<TR>"
                                            "<TD vAlign=\"center\" align=\"left\">");
                                                getMethodHtmlForm(context, request, serviceQName.str(), methodQName.str(), page, true);
                                                page.append("</TD>"
                                        "</TR>"
                                    "</TBODY>"
                                "</TABLE>"
                            "</TR>"
                        "</tbody>"
                    "</table>"
                    "<BR />"
                "</body>"
            "</html>");

        response->setContent(page.str());
        response->setContentType("text/html");
    }
    
    response->send();

    return 0;
}

                          
int EspHttpBinding::formatHtmlResultSet(IEspContext &context, const char *serv, const char *method, const char *resultsXml, StringBuffer &html)
{
    Owned<IPropertyTree> ptree = createPTreeFromXMLString(resultsXml);
    
    Owned<IPropertyTreeIterator> exceptions = ptree->getElements("//Exception");
    ForEach(*exceptions.get())
    {
        IPropertyTree &xcpt = exceptions->query();
        html.appendf("<br/>Exception: <br/>Reported by: %s <br/>Message: %s <br/>", 
            xcpt.queryProp("Source"), xcpt.queryProp("Message"));
    }
    
    Owned<IPropertyTreeIterator> datasets = ptree->getElements("//Dataset");
    ForEach(*datasets.get())
    {
        html.append("<table bordercolor=\"#808080\" border=\"1\" cellspacing=\"0\" cellpadding=\"0\" ><tr><td>");
        html.append("<table border=\"1\" bordercolor=\"#808080\" cellspacing=\"0\" cellpadding=\"0\" >");

        IPropertyTree &dset = datasets->query();

        //column headers
        IPropertyTree *row1 = dset.queryPropTree("Row[1]");
        if (row1)
        {
            html.append("<tr><td bgcolor=\"#808080\"><font face=\"Verdana\" size=\"2\">&nbsp;</font></td>");
            
            Owned<IPropertyTreeIterator> columns = row1->getElements("*");
            ForEach(*columns.get())
            {
                const char *title = columns->query().queryName();
                html.appendf("<td align=\"center\" bgcolor=\"#808080\"><font face=\"Verdana\" size=\"2\" color=\"#E0E0E0\"><b>%s</b></font></td>", (title!=NULL) ? title : "&nbsp;");
            }                       
                    
            html.append("</tr>");
        }

        Owned<IPropertyTreeIterator> datarows = dset.getElements("Row");
        if (datarows)
        {
            int count=1;
            ForEach(*datarows.get())
            {
                IPropertyTree &row = datarows->query();
                Owned<IPropertyTreeIterator> columns = row.getElements("*");
                
                html.appendf("<td align=\"center\" bgcolor=\"#808080\"><font face=\"Verdana\" size=\"2\" color=\"#E0E0E0\"><b>%d</b></font></td>", count++);
                ForEach(*columns.get())
                {
                    const char *value = columns->query().queryProp(NULL);
                    html.appendf("<td align=\"center\" bgcolor=\"#E0E0E0\"><font face=\"Verdana\" size=\"2\" color=\"#000000\">%s</font></td>", (value!=NULL) ? value : "&nbsp;");
                }                       
                        
                html.append("</tr>");
            }
        }
    
        html.append("</td></tr></table></table>");
    }
    
    return 0;
}


int EspHttpBinding::formatResultsPage(IEspContext &context, const char *serv, const char *method, StringBuffer &results, StringBuffer &page)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    qualifyServiceName(context, serv, method, serviceQName, &methodQName);

    page.append(
        "<html>"
            "<head>"
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                "<title>ESP Service form</title>"
            "</head>"
            "<body>"
                //"<table border=\"0\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" bgcolor=\"#000000\" height=\"108\">"
                //  "<tr>"
                //      "<td width=\"24%\" height=\"24\" bgcolor=\"#000000\">"
                //          "<img border=\"0\" src=\"/");
                //          page.append(serviceQName.str());
                //          page.append("/files_/logo.gif\" width=\"258\" height=\"108\" />"
                //      "</td>"
                //  "</tr>"
                //  "<tr>"
                //      "<td width=\"24%\" height=\"24\" bgcolor=\"#AA0000\">"
                //          "<p align=\"center\" />"
                //          "<b>"
                //              "<font color=\"#FFFFFF\" size=\"5\">Enterprise Services Platform<sup><font size=\"2\">TM</font></sup></font>"
                //          "</b>"
                //      "</td>"
                //  "</tr>"
                //"</table>"
                //"<br />"
                //"<br />"
                "<p align=\"center\" />"
                "<table cellSpacing=\"0\" cellPadding=\"1\" width=\"90%\" bgColor=\"#666666\" border=\"0\">"
                    "<tbody>"
                        "<tr align=\"middle\" bgColor=\"#666666\">"
                            "<td height=\"23\">"
                                "<p align=\"left\">"
                                    "<font color=\"#efefef\">");
                            page.appendf("<b>%s</b>", serviceQName.str());
                            page.append("</font>"
                                "</p>"
                            "</td>"
                        "</tr>"
                        "<tr bgColor=\"#ffcc66\">"
                            "<td height=\"3\">"
                                "<p align=\"left\">");
                                        
                            page.appendf("<b>&gt; %s Results</b>", methodQName.str());
                            page.append("</p>"
                            "</td>"
                        "</tr>"
                        "<TR bgColor=\"#666666\">"
                            "<TABLE cellSpacing=\"0\" width=\"90%\" bgColor=\"#efefef\" border=\"0\">"
                                "<TBODY>"
                                    "<TR>"
                                        "<TD vAlign=\"center\" align=\"left\">");
                                            page.append(results);
                                            page.append("</TD>"
                                    "</TR>"
                                "</TBODY>"
                            "</TABLE>"
                        "</TR>"
                    "</tbody>"
                "</table>"
                "<BR />"
            "</body>"
        "</html>");

    return 0;
}

bool EspHttpBinding::setContentFromFile(IEspContext &context, CHttpResponse &resp, const char *filepath)
{
    StringBuffer mimetype, etag, lastModified;
    MemoryBuffer content;
    bool modified = false;
    if (httpContentFromFile(filepath, mimetype, content, modified, lastModified, etag))
    {
        resp.setContent(content.length(), content.toByteArray());
        resp.setContentType(mimetype.str());
        return true;
    }
    return false;
}

void EspHttpBinding::onBeforeSendResponse(IEspContext& context, CHttpRequest* request, MemoryBuffer& content, 
                                      const char *serviceName, const char* methodName)
{
    IProperties* params = request->queryParameters();
    if (params->hasProp("esp_validate"))
        validateResponse(context, request, content, serviceName, methodName);
    else if (params->hasProp("esp_sort_result"))
        sortResponse(context, request, content, serviceName, methodName);
}

void EspHttpBinding::validateResponse(IEspContext& context, CHttpRequest* request, MemoryBuffer& content, 
                                      const char *service, const char* method)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;

    if (!qualifyServiceName(context, service, method, serviceQName, &methodQName))
        return;
            
    StringBuffer xml,xsd;
    
    // name space
    StringBuffer ns;
    generateNamespace(context, request, serviceQName.str(), methodQName.str(), ns);

    // XML
    Owned<IPropertyTree> tree; 
    try {
        tree.setown(createPTreeFromXMLString(content.length(), content.toByteArray()));
        // format it for better error message
        toXML(tree, xml, 1);

        //Hack: add default name space to XML
        const char* end = strstr(xml, "<?xml");
        if (end) 
        {
            end = strstr(end+5, "?>");
            if (end)
                end = strchr(end+2, '>');
            if (!end)
                throw MakeStringException(-1,"Invalid response XML in processing instruction");
        } 
        else
        {
            end = strchr(xml, '>');
            if (!end)
                throw MakeStringException(-1,"Can not find the root node");
            if (*(end-1)=='/')   // root is like: <xxx />
                end--;
        }

        int offset = end ? end-xml.str() : 0;
        StringBuffer tag(offset, xml.str());
        if (!strstr(tag, " xmlns="))
        {
            StringBuffer defNS;
            defNS.appendf(" xmlns=\"%s\"", ns.str());
            xml.insert(offset, defNS.str());
        }
    } catch (IException* e) {
        StringBuffer msg;
        DBGLOG("Unexpected error: parsing XML: %s", e->errorMessage(msg).str());
    }

    // schema
    getSchema(xsd,context,request,serviceQName,methodQName,true);
            
    // validation
    if (getEspLogLevel()>LogMax)
        DBGLOG("[VALIDATE] xml: %s\nxsd: %s\nns: %s",xml.str(), xsd.str(), ns.str());

    IXmlValidator* v = getXmlLibXmlValidator();

    v->setXmlSource(xml, strlen(xml));
    v->setSchemaSource(xsd, strlen(xsd));
    v->setTargetNamespace(ns);

    Owned<IPropertyTree> result = createPTree("ModifiedResponse");
    IPropertyTree* e = createPTree();
    try {
        v->validate();
        e->appendProp("Result", "No error found");
    } catch (IMultiException* me) {
        for (unsigned i=0; i<me->ordinality(); i++)
        {
            IException& item = me->item(i);
            StringBuffer s;
            e->addProp("Error", item.errorMessage(s).str());
        }
        me->Release();
    }
    result->addPropTree("SchemaValidation", e);

    result->addPropTree("OriginalResponse", LINK(tree));

    StringBuffer temp;
    toXML(result,temp,1);
    unsigned len = temp.length(); // This has to be done before temp.detach is called!
    content.setBuffer(len, temp.detach(), true);
}

void EspHttpBinding::sortResponse(IEspContext& context, CHttpRequest* request, MemoryBuffer& content, 
                                      const char *serviceName, const char* methodName)
{
    ESPLOG(LogNormal,"Sorting Response XML...");

    try {
        StringBuffer result;
        StringBuffer respXML(content.length(), content.toByteArray());

        //sorting
        IXslProcessor* xslp = getXmlLibXslProcessor();

        Owned<IXslTransform> xform = xslp->createXslTransform();
        xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/dict_sort.xsl").str());

        xform->setXmlSource(respXML,respXML.length());
        xform->transform(result);
        if (getEspLogLevel()>LogNormal)
            DBGLOG("XML sorted: %s", result.str());
        unsigned len = result.length();
        content.setBuffer(len, result.detach(), true);      
    } catch (IException* e) {
        StringBuffer msg;
        DBGLOG("Unexpected error: parsing XML: %s", e->errorMessage(msg).str());
    }
}
