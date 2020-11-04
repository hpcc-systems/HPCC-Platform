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

#include "esphttp.hpp"

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
//#include "IAEsp.hpp"

//ESP Core
#include "espthread.hpp"
#include "espbinding.hpp"

#include "bindutil.hpp"

#include "httpbinding.hpp"
#include "htmlpage.hpp"
#include  "seclib.hpp"
#include "../../../system/security/shared/secloader.hpp"
#include  "../../SOAP/Platform/soapmessage.hpp"
#include  "../../SOAP/Platform/soapbind.hpp"
#include "xmlvalidator.hpp"
#include "xsdparser.hpp"
#include "espsecurecontext.hpp"
#include "jsonhelpers.hpp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "workunit.hpp"

#define FILE_UPLOAD     "FileUploadAccess"
#define DEFAULT_HTTP_PORT 80

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
    m_includeSoapTest = true;
    m_includeJsonTest = true;
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

        m_host.set(bnd_cfg->queryProp("@netAddress"));
        m_port = bnd_cfg->getPropInt("@port");

        const char *realm = bnd_cfg->queryProp("@realm");
        if (realm)
            m_realm.set(realm);
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
                    m_realm.set("EclWatch");
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
                
                m_realm.set((realm) ? realm : "EspServices");
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
            //Instantiate a Security Manager
            m_authtype.set(authcfg->queryProp("@type"));
            m_authmethod.set(authcfg->queryProp("@method"));
            if (!m_authmethod.isEmpty())
            {
                Owned<IPropertyTree> secMgrCfg;
                if(proc_cfg.get() != NULL)
                {
                    VStringBuffer sb("SecurityManagers/SecurityManager[@name='%s']", m_authmethod.str());
                    Owned<IPropertyTree> smTree;
                    smTree.setown(proc_cfg->getPropTree(sb.str()));
                    if (smTree && smTree->hasProp("@type"))
                        secMgrCfg.setown(smTree->getPropTree(smTree->queryProp("@type")));
                }

                if (secMgrCfg)
                {
                    //This is a Pluggable Security Manager
                    m_secmgr.setown(SecLoader::loadPluggableSecManager<ISecManager>(bindname, bnd_cfg, secMgrCfg));
                    m_authmap.setown(m_secmgr->createAuthMap(authcfg));
                    m_feature_authmap.setown(m_secmgr->createFeatureMap(authcfg));
                    m_setting_authmap.setown(m_secmgr->createSettingMap(authcfg));
                }
                else
                {
                    //Legacy Security Manager
                    if(stricmp(m_authmethod.str(), "LdapSecurity") == 0)
                    {
                        StringBuffer lsname;
                        authcfg->getProp("@config", lsname);
                        Owned<IPropertyTree> lscfg = bnd_cfg->getPropTree(StringBuffer(".//ldapSecurity[@name=").appendf("\"%s\"]", lsname.str()).str());
                        if(lscfg == NULL)
                        {
                            if(proc_cfg.get() != NULL)
                                lscfg.setown(proc_cfg->getPropTree(StringBuffer("ldapSecurity[@name=").appendf("\"%s\"]", lsname.str()).str()));
                            if(lscfg == NULL)
                            {
                                OERRLOG("can't find bnd_cfg for LdapSecurity %s", lsname.str());
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
                        m_setting_authmap.setown(m_secmgr->createSettingMap(authcfg));
                    }
                    else if(stricmp(m_authmethod.str(), "Local") == 0)
                    {
                        m_secmgr.setown(SecLoader::loadSecManager("Local", "EspHttpBinding", NULL));
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

    setUnrestrictedSSTypes();

    //Even for non-session based environment, the sessionIDCookieName may be used to
    //remove session related cookies cached in some browser page.
    sessionIDCookieName.setf("%s%d", SESSION_ID_COOKIE, m_port);
    if (!m_secmgr.get() || !daliClientActive())
    {
        if (!daliClientActive())
        {
            if (proc_cfg->hasProp("AuthDomains"))
                throw MakeStringException(-1, "ESP cannot have an 'AuthDomains' setting because no dali connection is available.");
            if (bnd_cfg->hasProp("@authDomain"))
                throw MakeStringException(-1, "ESP Binding %s cannot have an '@authDomain' setting because no dali connection is available.", bindname);
        }

        domainAuthType = AuthPerRequestOnly;
        Owned<IPropertyTree> authcfg = proc_cfg->getPropTree("Authentication");
        if (authcfg)
        {
            const char* authmethod = authcfg->queryProp("@method");
            if (authmethod && strieq(authmethod, "userNameOnly"))
            {
                //The @getUserNameUnrestrictedResources contains URLs which may be used by the getUserNameURL page.
                //For example, an icon file on the getUserNameURL page.
                const char* unrestrictedResources = authcfg->queryProp("@getUserNameUnrestrictedResources");
                if (!isEmptyString(unrestrictedResources))
                    readUnrestrictedResources(unrestrictedResources);
                else
                    readUnrestrictedResources(DEFAULT_UNRESTRICTED_RESOURCES);

                const char* getUserNameURL = authcfg->queryProp("@getUserNameURL");
                if (!isEmptyString(getUserNameURL))
                    loginURL.set(getUserNameURL);
                else
                    loginURL.set(DEFAULT_GET_USER_NAME_URL);
                const char* _loginLogoURL = authcfg->queryProp("@loginLogoURL");
                if (!isEmptyString(_loginLogoURL))
                    loginLogoURL.set(_loginLogoURL);
                else
                    loginLogoURL.set(DEFAULT_LOGIN_LOGO_URL);
                domainAuthType = AuthUserNameOnly;
            }
        }
        return;
    }

    processName.set(procname);
    const char* authDomain = bnd_cfg->queryProp("@authDomain");
    if (!isEmptyString(authDomain))
        domainName.set(authDomain);
    else
        domainName.set("default");

    readAuthDomainCfg(proc_cfg);

    if ((domainAuthType == AuthPerSessionOnly) || (domainAuthType == AuthTypeMixed))
    {
        espSessionSDSPath.setf("%s/%s[@name=\"%s\"]", PathSessionRoot, PathSessionProcess, processName.get());
        sessionSDSPath.setf("%s/%s[@port=\"%d\"]/", espSessionSDSPath.str(), PathSessionApplication, m_port);
        checkSessionTimeoutSeconds = proc_cfg->getPropInt("@checkSessionTimeoutSeconds", ESP_CHECK_SESSION_TIMEOUT);
    }

    setABoolHash(proc_cfg->queryProp("@urlAlias"), serverAlias);
}

static int compareLength(char const * const *l, char const * const *r) { return strlen(*l) - strlen(*r); }

void EspHttpBinding::readAuthDomainCfg(IPropertyTree* procCfg)
{
    VStringBuffer xpath("AuthDomains/AuthDomain[@domainName=\"%s\"]", domainName.get());
    IPropertyTree* authDomainTree = procCfg->queryPropTree(xpath);
    if (authDomainTree)
    {
        const char* authType = authDomainTree->queryProp("@authType");
        if (isEmptyString(authType) || strieq(authType, "AuthTypeMixed"))
            domainAuthType = AuthTypeMixed;
        else if (strieq(authType, "AuthPerSessionOnly"))
            domainAuthType = AuthPerSessionOnly;
        else
        {
            domainAuthType = AuthPerRequestOnly;
            return;
        }

        int clientSessionTimeoutMinutes = authDomainTree->getPropInt("@clientSessionTimeoutMinutes", ESP_SESSION_TIMEOUT);
        if (clientSessionTimeoutMinutes < 0)
            clientSessionTimeoutSeconds = ESP_SESSION_NEVER_TIMEOUT;
        else
            clientSessionTimeoutSeconds = clientSessionTimeoutMinutes * 60;

        //The serverSessionTimeoutMinutes is used to clean the sessions by ESP server after the sessions have been timed out on ESP clients.
        //Considering possible network delay, serverSessionTimeoutMinutes should be greater than clientSessionTimeoutMinutes.
        int serverSessionTimeoutMinutes = authDomainTree->getPropInt("@serverSessionTimeoutMinutes", 0);
        if ((serverSessionTimeoutMinutes < 0) || (clientSessionTimeoutMinutes < 0))
            serverSessionTimeoutSeconds = ESP_SESSION_NEVER_TIMEOUT;
        else
            serverSessionTimeoutSeconds = serverSessionTimeoutMinutes * 60;
        if (serverSessionTimeoutSeconds < clientSessionTimeoutSeconds)
            serverSessionTimeoutSeconds = 2 * clientSessionTimeoutSeconds;

        //The @unrestrictedResources contains URLs which may be used before a user is authenticated.
        //For example, an icon file on the login page.
        const char* unrestrictedResources = authDomainTree->queryProp("@unrestrictedResources");
        if (!isEmptyString(unrestrictedResources))
            readUnrestrictedResources(unrestrictedResources);

        const char* _loginURL = authDomainTree->queryProp("@logonURL");
        if (!isEmptyString(_loginURL))
            loginURL.set(_loginURL);
        else
            loginURL.set(DEFAULT_LOGIN_URL);
        const char* _loginLogoURL = authDomainTree->queryProp("@loginLogoURL");
        if (!isEmptyString(_loginLogoURL))
            loginLogoURL.set(_loginLogoURL);
        else
            loginLogoURL.set(DEFAULT_LOGIN_LOGO_URL);

        const char* _logoutURL = authDomainTree->queryProp("@logoutURL");
        if (!isEmptyString(_logoutURL))
        {
            logoutURL.set(_logoutURL);
            domainAuthResources.setValue(logoutURL.get(), true);
        }

        //Read pre-configured 'invalidURLsAfterAuth'. Separate the comma separated string to a
        //list. Store them into BoolHash for quick lookup.
        setABoolHash(authDomainTree->queryProp("@invalidURLsAfterAuth"), invalidURLsAfterAuth);
    }
    else
    {//old environment.xml
        domainAuthType = AuthTypeMixed;
        readUnrestrictedResources(DEFAULT_UNRESTRICTED_RESOURCES);
        loginURL.set(DEFAULT_LOGIN_URL);
    }

    if (!loginURL.isEmpty())
        setABoolHash(loginURL.get(), invalidURLsAfterAuth);
    setABoolHash("/esp/login", invalidURLsAfterAuth);
    domainAuthResourcesWildMatch.sortCompare(compareLength);
}

void EspHttpBinding::readUnrestrictedResources(const char* resources)
{
    StringArray resourceArray;
    resourceArray.appendListUniq(resources, ",");
    ForEachItemIn(i, resourceArray)
    {
        const char* resource = resourceArray.item(i);
        if (isEmptyString(resource))
            continue;
        if (isWildString(resource))
            domainAuthResourcesWildMatch.append(resource);
        else
            domainAuthResources.setValue(resource, true);
    }
}

//Set the subservice types (wsdl, xsd, etc) which do not need user authentication.
void EspHttpBinding::setUnrestrictedSSTypes()
{
    unrestrictedSSTypes.insert(sub_serv_wsdl);
    unrestrictedSSTypes.insert(sub_serv_xsd);
    unrestrictedSSTypes.insert(sub_serv_reqsamplexml);
    unrestrictedSSTypes.insert(sub_serv_respsamplexml);
    unrestrictedSSTypes.insert(sub_serv_reqsamplejson);
    unrestrictedSSTypes.insert(sub_serv_respsamplejson);
}

bool EspHttpBinding::isUnrestrictedSSType(sub_service ss) const
{
    auto search = unrestrictedSSTypes.find(ss);
    return (search != unrestrictedSSTypes.end()); 
}

//Check whether the url is valid or not for redirect after authentication.
bool EspHttpBinding::canRedirectAfterAuth(const char* url) const
{
    if (isEmptyString(url))
        return false;

    bool* found = invalidURLsAfterAuth.getValue(url);
    return (!found || !*found);
}

//Use the origin header to check whether the request is a CORS request or not.
bool EspHttpBinding::isCORSRequest(const char* originHeader)
{
    if (isEmptyString(originHeader))
        return false;

    const char* ptr = nullptr;
    if (strnicmp(originHeader, "http://", 7) == 0)
        ptr = originHeader + 7;
    else if (strnicmp(originHeader, "https://", 8) == 0)
        ptr = originHeader + 8;
    else
        return true;

    StringBuffer ipStr; //ip or network alias
    while(*ptr && *ptr != ':')
    {
        ipStr.append(*ptr);
        ptr++;
    }

    IpAddress ip(ipStr.str());
    if (ip.ipequals(queryHostIP()))
        return false;

    int port = 0;
    if (*ptr && *ptr == ':')
    {
        ptr++;
        while(*ptr && isdigit(*ptr))
        {
            port = 10*port + (*ptr-'0');
            ptr++;
        }
    }
    if (port == 0)
        port = DEFAULT_HTTP_PORT;

    if (port != getPort())
        return true;


    bool* found = serverAlias.getValue(ipStr.str());
    return (!found || !*found);
}

void EspHttpBinding::setABoolHash(const char* csv, BoolHash& hash) const
{
    if (isEmptyString(csv))
        return;

    StringArray aList;
    aList.appendListUniq(csv, ",");
    ForEachItemIn(i, aList)
    {
        const char* s = aList.item(i);
        bool* found = hash.getValue(s);
        if (!found || !*found)
            hash.setValue(s, true);
    }
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

bool EspHttpBinding::isMethodInService(IEspContext& context, const char *servname, const char *methname)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;
    if (!qualifyServiceName(context, servname, methname, serviceQName, &methodQName))
        return false;

    if (methodQName.length() > 0)
        return true;

    return isMethodInSubService(context, servname, methname);
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


    ISecUser *user = m_secmgr->createUser(userid.str(), ctx->querySecureContext());
    if(user == NULL)
    {
        UWARNLOG("Couldn't create ISecUser object for %s", userid.str());
        return;
    }

    ctx->getPassword(password);
    user->credentials().setPassword(password.str());

    ctx->getRealm(realm);
    user->setRealm(realm.str());

    ctx->getPeer(peer);
    user->setPeer(peer.str());

    ctx->setUser(user);

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
        ctx->setAuthError(EspAuthErrorEmptyUserID);
        ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", "Access Denied: No username provided");
        return false;
    }

    if(stricmp(m_authmethod.str(), "UserDefined") == 0)
        return true;

    ISecUser *user = ctx->queryUser();
    if(user == NULL)
    {
        UWARNLOG("Can't find user in context");
        ctx->setAuthError(EspAuthErrorUserNotFoundInContext);
        ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", "Access Denied: No username provided");
        return false;
    }

    //Check if the password is a "real" password, or a workunit distributed access token
    const char * pwd = user->credentials().getPassword();
    if (isWorkunitDAToken(pwd))
    {
        wuTokenStates state = verifyWorkunitDAToken(user->getName(), pwd);//throws if cannot open workunit
        if (state == wuTokenValid)
        {
            user->setAuthenticateStatus(AS_AUTHENTICATED);
        }
        else
        {
            user->setAuthenticateStatus(AS_INVALID_CREDENTIALS);
            const char * reason = state == wuTokenInvalid ? "WUToken Workunit Token invalid" : "WUToken Workunit Inactive";
            ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", reason);
            ctx->setAuthError(EspAuthErrorNotAuthenticated);
            ctx->setRespMsg(reason);
            user->credentials().setPassword(nullptr);
            return false;
        }
    }

    if(m_secmgr.get() == NULL)
    {
        UWARNLOG("No mechanism established for authentication");
        ctx->setAuthError(EspAuthErrorNoAuthMechanism);
        return false;
    }

    ISecResourceList* rlist = ctx->queryResources();
    if(rlist == NULL)
    {
        UWARNLOG("No Security Resource");
        ctx->setAuthError(EspAuthErrorEmptySecResource);
        return false;
    }

    bool authenticated = m_secmgr->authorize(*user, rlist, ctx->querySecureContext());
    if(!authenticated)
    {
        VStringBuffer err("User %s : ", user->getName());
        switch (user->getAuthenticateStatus())
        {
        case AS_PASSWORD_EXPIRED :
        case AS_PASSWORD_VALID_BUT_EXPIRED :
            err.append("Password expired");
            break;
        case AS_ACCOUNT_DISABLED :
            err.append("Account disabled");
            break;
        case AS_ACCOUNT_EXPIRED :
            err.append("Account expired");
            break;
        case AS_ACCOUNT_LOCKED :
            err.append("Account locked");
            break;
        case AS_INVALID_CREDENTIALS :
        default:
            err.append("Access Denied: User or password invalid");
        }
        ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authentication", err.str());
        ctx->setAuthError(EspAuthErrorNotAuthenticated);
        ctx->setRespMsg(err.str());
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
                VStringBuffer msg("Access for user '%s' denied to: %s. Access=%d, Required=%d", user->getName(), desc?desc:"<no-desc>", access, required);
                ESPLOG(LogMin, "%s", msg.str());
                ctx->AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: Not Authorized", "Resource: %s [%s]", curres->getName(), (desc) ? desc : "");
                ctx->setAuthError(EspAuthErrorNotAuthorized);
                ctx->setRespMsg(msg.str());
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

    m_secmgr->updateSettings(*user,securitySettings, ctx->querySecureContext());

    ctx->addTraceSummaryTimeStamp(LogMin, "basicAuth");
    return authorized;
}

bool EspHttpBinding::queryCacheSeconds(const char *method, unsigned& cacheSeconds)
{
    StringBuffer key(method);
    unsigned* value = cacheSecondsMap.getValue(key.toUpperCase().str());
    if (!value)
        return false;
    cacheSeconds = *value;
    return true;
}

bool EspHttpBinding::queryCacheGlobal(const char *method)
{
    StringBuffer key(method);
    bool* cacheGlobal = cacheGlobalMap.getValue(key.toUpperCase().str());
    return cacheGlobal && *cacheGlobal;
}

const char* EspHttpBinding::createESPCacheID(CHttpRequest* request, StringBuffer& cacheID)
{
    StringBuffer idStr, msgType;
    if(request->isSoapMessage())
        msgType.set("SOAP");
    else if(request->isFormSubmission())
        msgType.set("FORM");

    if (!queryCacheGlobal(request->queryServiceMethod()))
    {
        const char* userID = request->queryContext()->queryUserId();
        if (!isEmptyString(userID))
            idStr.append(userID).append("_");
    }
    if (!msgType.isEmpty())
        idStr.append(msgType.str()).append("_");
    idStr.appendf("%s_%s_%s", request->queryServiceName(), request->queryServiceMethod(), request->queryAllParameterString());
    cacheID.append(hashc((unsigned char *)idStr.str(), idStr.length(), 0));
    return cacheID.str();
}

bool EspHttpBinding::sendFromESPCache(IEspCache* cacheClient, CHttpRequest* request, CHttpResponse* response, const char* cacheID)
{
    StringBuffer content, contentType;
    if (!cacheClient->readResponseCache(cacheID, content.clear(), contentType.clear()))
        ESPLOG(LogMax, "Failed to read from ESP Cache for %s.", request->queryServiceMethod());
    if (content.isEmpty() || contentType.isEmpty())
        return false;

    ESPLOG(LogMax, "Sending from ESP Cache for %s.", request->queryServiceMethod());
    response->setContentType(contentType.str());
    response->setContent(content.str());
    response->send();
    return true;
}

void EspHttpBinding::addToESPCache(IEspCache* cacheClient, CHttpRequest* request, CHttpResponse* response, const char* cacheID, unsigned cacheSeconds)
{
    StringBuffer content, contentType;
    response->getContent(content);
    response->getContentType(contentType);
    if (cacheClient->cacheResponse(cacheID, cacheSeconds, content.str(), contentType.str()))
        ESPLOG(LogMax, "AddTo ESP Cache for %s.", request->queryServiceMethod());
    else
        ESPLOG(LogMax, "Failed to add ESP Cache for %s.", request->queryServiceMethod());
}

void EspHttpBinding::clearCacheByGroupID(const char *ids)
{
    if (isEmptyString(ids))
        return;

    IEspContainer *espContainer = getESPContainer();
    if (!espContainer->hasCacheClient())
        return;

    ESPLOG(LogMax, "clearCacheByGroupID %s.", ids);
    StringArray errorMsgs;
    espContainer->clearCacheByGroupID(ids, errorMsgs);
    if (errorMsgs.length() > 0)
    {
        ForEachItemIn(i, errorMsgs)
            DBGLOG("%s", errorMsgs.item(i));
    }
}

void EspHttpBinding::handleHttpPost(CHttpRequest *request, CHttpResponse *response)
{
    StringBuffer cacheID;
    unsigned cacheSeconds = 0;
    IEspCache *cacheClient = nullptr;
    IEspContext &context = *request->queryContext();

    IEspContainer *espContainer = getESPContainer();
    if (espContainer->hasCacheClient() && (cacheMethods > 0)
        && queryCacheSeconds(request->queryServiceMethod(), cacheSeconds)) //ESP cache is needed for this method
    {
        cacheClient = (IEspCache*) espContainer->queryCacheClient(getCacheGroupID(request->queryServiceMethod()));
        if (cacheClient)
            createESPCacheID(request, cacheID);
        if (!cacheID.isEmpty() && !context.queryRequestParameters()->queryProp("dirty_cache")
            && sendFromESPCache(cacheClient, request, response, cacheID.str()))
            return;
    }

    if (request->isSoapMessage() || request->isJsonMessage())
    {
        request->queryParameters()->setProp("__wsdl_address", m_wsdlAddress.str());
        if(request->isJsonMessage())
            context.setResponseFormat(ESPSerializationJSON);
        onSoapRequest(request, response);
    }
    else if(request->isFormSubmission())
        onPostForm(request, response);
    else
        onPost(request, response);

    if (!cacheID.isEmpty())
        addToESPCache(cacheClient, request, response, cacheID.str(), cacheSeconds);
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
        case sub_serv_respsamplejson:
        case sub_serv_reqsamplejson:
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
        case sub_serv_json_builder:
            return onGetJsonBuilder(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_reqsamplexml:
            return onGetReqSampleXml(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_respsamplexml:
            return onGetRespSampleXml(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_respsamplejson:
            return onGetRespSampleJson(context, request, response, serviceName.str(), methodName.str());
        case sub_serv_reqsamplejson:
            return onGetReqSampleJson(context, request, response, serviceName.str(), methodName.str());
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
            IERRLOG("%s", s.str());
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

static void filterXmlBySchema(StringBuffer& in, StringBuffer& schema, const char* name, StringBuffer& out)
{
    Owned<IXmlSchema> sp = createXmlSchema(schema);
    Owned<IPTree> tree = createPTreeFromXMLString(in);

    //VStringBuffer name("tns:%s", tree->queryName());
    IXmlType* type = sp->queryElementType(name);
    if (!type)
    {
        name = tree->queryName();
        type = sp->queryElementType(name);
        if (!type)
        {
            StringBuffer method(name);
            if (method.length() > 7)
                method.setLength(method.length()-7);
            type = sp->queryElementType(method);
        }
    }

    if (type)
        filterXmlBySchema(tree,type,name,out);
    else 
    {
        const char* value = tree->queryProp(NULL);
        UWARNLOG("Unknown xml tag ignored: <%s>%s</%s>", name, value?value:"", name);
    }
}

void EspHttpBinding::getXMLMessageTag(IEspContext& ctx, bool isRequest, const char *method, StringBuffer& tag)
{
    MethodInfoArray info;
    getQualifiedNames(ctx, info);
    for (unsigned i=0; i<info.length(); i++)
    {
        CMethodInfo& m = info.item(i);
        if (!stricmp(m.m_label, method))
        {
            tag.set(isRequest ? m.m_requestLabel : m.m_responseLabel);
            break;
        }
    }
    if (!tag.length())
        tag.append(method).append(isRequest ? "Request" : "Response");
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

    StringBuffer req, tag, schema, filtered;
    msg->marshall(req, NULL);

    getSchema(schema,ctx,request,serv,method,false);
    getXMLMessageTag(ctx, true, method, tag);
    filterXmlBySchema(req,schema,tag.str(),filtered);
    
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

void EspHttpBinding::getJsonMessage(StringBuffer& jsonmsg, IEspContext& ctx, CHttpRequest* request, const char *serv, const char *method)
{
    ESPSerializationFormat orig_format = ctx.getResponseFormat();
    ctx.setResponseFormat(ESPSerializationJSON);
    Owned<IRpcRequestBinding> rpcreq = createReqBinding(ctx, request, serv, method);
    CSoapRequestBinding* reqbind = dynamic_cast<CSoapRequestBinding*>(rpcreq.get());
    if (reqbind)
    {
        StringBuffer mime;
        reqbind->appendContent(&ctx, jsonmsg, mime);
    }
    ctx.setResponseFormat(orig_format);
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
            IERRLOG(s);
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
    const char* authMethod = context.getAuthenticationMethod();
    if (authMethod && !strieq(authMethod, "none") && ((context.getDomainAuthType() == AuthPerSessionOnly) || (context.getDomainAuthType() == AuthTypeMixed)))
    {
        xform->setParameter("showLogout", "1");
        const char* userId = context.queryUserId();
        if (!isEmptyString(userId))
            xform->setStringParameter("username", userId);
    }

    StringBuffer page;
    xform->transform(page);     

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

int EspHttpBinding::onGetJsonBuilder(IEspContext &context, CHttpRequest* request, CHttpResponse* response,  const char *serv, const char *method)
{
    StringBuffer jsonmsg, serviceQName, methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName)
        || methodQName.length()==0)
        throw createEspHttpException(HTTP_STATUS_BAD_REQUEST_CODE, "Bad Request", HTTP_STATUS_BAD_REQUEST);

    getJsonMessage(jsonmsg,context,request,serviceQName,methodQName);

    //put all URL parameters into dest

    StringBuffer params;
    const char* excludes[] = {"json_builder_",NULL};
    getEspUrlParams(context,params,excludes);

    StringBuffer header("Content-Type: application/json");

    Owned<IXslProcessor> xslp = getXslProcessor();
    Owned<IXslTransform> xform = xslp->createXslTransform();
    xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/wsecl3_jsontest.xsl").str());

    StringBuffer encodedMsg;
    StringBuffer srcxml;
    srcxml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><srcxml><jsonreq><![CDATA[");
    srcxml.append(encodeJSON(encodedMsg, jsonmsg.str())); //encode the whole thing for javascript embedding
    srcxml.append("]]></jsonreq></srcxml>");
    xform->setXmlSource(srcxml.str(), srcxml.length());
    xform->setStringParameter("showhttp", "true()");

    // params
    xform->setStringParameter("pageName", "JSON Test");
    xform->setStringParameter("serviceName", serviceQName.str());
    xform->setStringParameter("methodName", methodQName.str());
    xform->setStringParameter("header", header.str());
    xform->setStringParameter("jsonbody", encodedMsg.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer destination;
    destination.appendf("%s?%s", methodQName.str(), params.str());
    xform->setStringParameter("destination", destination.str());
    const char* authMethod = context.getAuthenticationMethod();
    if (authMethod && !strieq(authMethod, "none") && ((context.getDomainAuthType() == AuthPerSessionOnly) || (context.getDomainAuthType() == AuthTypeMixed)))
    {
        xform->setParameter("showLogout", "1");
        const char* userId = context.queryUserId();
        if (!isEmptyString(userId))
            xform->setStringParameter("username", userId);
    }

    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

int EspHttpBinding::onFeaturesAuthorize(IEspContext &context, MapStringTo<SecAccessFlags> & pmap, const char *serviceName, const char *methodName)
{
    if (!context.validateFeaturesAccess(pmap, false))
    {
        StringBuffer features;
        HashIterator iter(pmap);
        int index = 0;
        ForEach(iter)
        {
            IMapping &cur = iter.query();
            const char * key = (const char *)cur.getKey();
            SecAccessFlags val = *pmap.getValue(key);
            features.appendf("%s%s:%s", (index++ == 0 ? "" : ", "), key, getSecAccessFlagName(val));
        }
        throw MakeStringException(-1, "%s::%s access denied - Required features: %s.", serviceName, methodName, features.str());
    }
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
        if (getESPContainer() && getESPContainer()->queryApplicationConfig())
        {
            ESPLOG(LogNormal, "Get config generated during application init");
            StringBuffer content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if (context.queryRequestParameters()->hasProp("display"))
                content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");
            toXML(getESPContainer()->queryApplicationConfig(), content);
            response->setContent(content.str());
            response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return 0;
        }

        ESPLOG(LogNormal, "Get config file: %s", m_configFile.get());

        StringBuffer content;
        xmlContentFromFile(m_configFile, "/esp/xslt/xmlformatter.xsl", content);
        response->setContent(content.str());
        response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
        response->setStatus(HTTP_STATUS_OK);
        response->send();
        return 0;
    }
    OERRLOG("Config access denied");
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
            return onGetNotFound(context, request,  response, service);
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

void EspHttpBinding::generateSampleXml(bool isRequest, IEspContext &context, CHttpRequest* request, StringBuffer &content, const char *serv, const char *method)
{
    StringBuffer schemaXml, element;
    getXMLMessageTag(context, isRequest, method, element);
    getSchema(schemaXml,context,request,serv,method,false);
    Owned<IXmlSchema> schema;
    IXmlType* type = nullptr;
    try
    {
        schema.setown(createXmlSchema(schemaXml));
        if (!schema)
        {
            content.appendf("<Error>generateSampleXml schema error: %s::%s</Error>", serv, method);
            return;
        }

        type = schema->queryElementType(element);
        if (!type)
        {
            content.appendf("<Error>generateSampleXml unknown type: %s in %s::%s</Error>", element.str(), serv, method);
            return;
        }
    }
    catch (IException *E)
    {
        StringBuffer msg;
        content.appendf("<Error>generateSampleXml Exception: %s in %s::%s</Error>", E->errorMessage(msg).str(), serv, method);
        E->Release();
        return;
    }
    StringStack parent;
    StringBuffer nsdecl("xmlns=\"");
    genSampleXml(parent,type, content, element, generateNamespace(context, request, serv, method, nsdecl).append('\"').str());
}



void EspHttpBinding::generateSampleXml(bool isRequest, IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer serviceName;
    StringBuffer methodName;
    if (!qualifyServiceName(context, serv, method, serviceName, (method) ? &methodName : nullptr))
        return;

    StringBuffer content;
    if (method && *method)
        generateSampleXml(isRequest, context, request, content, serviceName, methodName);
    else
    {
        MethodInfoArray methods;
        getQualifiedNames(context, methods);

        content.appendf("<Examples><%s>\n", isRequest ? "Requests" : "Responses");
        ForEachItemIn(i, methods)
            generateSampleXml(isRequest, context, request, content, serviceName.str(), methods.item(i).m_label.str());
        content.appendf("\n</%s></Examples>", isRequest ? "Requests" : "Responses");
    }

    response->setContent(content.length(), content.str());
    response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return;
}

void EspHttpBinding::generateSampleJson(bool isRequest, IEspContext &context, CHttpRequest* request, StringBuffer &content, const char *serv, const char *method)
{
    StringBuffer schemaXml, element;
    getXMLMessageTag(context, isRequest, method, element);
    getSchema(schemaXml,context,request,serv,method,false);

    Owned<IXmlSchema> schema;
    IXmlType* type = nullptr;
    try
    {
        schema.setown(createXmlSchema(schemaXml));
        if (!schema)
        {
            content.appendf("{\"Error\": \"generateSampleJson schema error: %s::%s\"}", serv, method);
            return;
        }

        type = schema->queryElementType(element);
        if (!type)
        {
            content.appendf("{\"Error\": \"generateSampleJson unknown type: %s in %s::%s\"}", element.str(), serv, method);
            return;
        }
    }
    catch (IException *E)
    {
        StringBuffer msg;
        content.appendf("{\"Error\": \"generateSampleJson unknown type: %s in %s::%s\"}", E->errorMessage(msg).str(), serv, method);
        E->Release();
        return;
    }

    StringArray parentTypes;
    JsonHelpers::buildJsonMsg(parentTypes, type, content, element, NULL, REQSF_ROOT|REQSF_SAMPLE_DATA|REQSF_FORMAT);
}

void EspHttpBinding::generateSampleJson(bool isRequest, IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer serviceName;
    StringBuffer methodName;
    if (!qualifyServiceName(context, serv, method, serviceName, (method) ? &methodName : nullptr))
        return;

    StringBuffer content;
    if (method && *method)
        generateSampleJson(isRequest, context, request, content, serviceName, methodName);
    else
    {
        MethodInfoArray methods;
        getQualifiedNames(context, methods);
        content.appendf("{\"Examples\": {\"%s\": [\n", isRequest ? "Request" : "Response");
        ForEachItemIn(i, methods)
        {
            delimitJSON(content, true);
            generateSampleJson(isRequest, context, request, content, serviceName.str(), methods.item(i).m_label.str());
        }
        content.append("]}}\n");
    }

    response->setContent(content.length(), content.str());
    response->setContentType(HTTP_TYPE_TEXT_PLAIN_UTF8);
    response->setStatus(HTTP_STATUS_OK);
    response->send();
    return;
}

void EspHttpBinding::generateSampleXmlFromSchema(bool isRequest, IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method, const char * schemaxml)
{
    StringBuffer serviceQName, methodQName;

    if (!qualifyServiceName(context, serv, method, serviceQName, &methodQName))
        return;

    StringBuffer element, schemaXmlbuff(schemaxml);
    getXMLMessageTag(context, isRequest, methodQName.str(), element);

    StringBuffer content("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    if (context.queryRequestParameters()->hasProp("display"))
        content.append("<?xml-stylesheet type=\"text/xsl\" href=\"/esp/xslt/xmlformatter.xsl\"?>");

    if (!methodQName || !*methodQName)
    {
        StringBuffer label(isRequest ? "Requests" : "Responses");
        content.appendf("<Examples><%s>", label.str());
        MethodInfoArray methods;
        getQualifiedNames(context, methods);
        ForEachItemIn(i, methods)
        {
            generateSampleXml(isRequest, context, request, content, serviceQName.str(), methods.item(i).m_label.str());
        }
        content.appendf("</%s></Examples>", label.str());
    }
    else
    {
        Owned<IXmlSchema> schema = createXmlSchema(schemaXmlbuff);
        if (!schema.get())
            throw MakeStringException(-1, "Could not create XML Schema");

        IXmlType* type = schema->queryElementType(element);
        if (!type)
            throw MakeStringException(-1, "Unknown type: %s", element.str());

        StringStack parent;
        StringBuffer nsdecl("xmlns=\"");
        genSampleXml(parent,type, content, element, generateNamespace(context, request, serviceQName.str(), methodQName.str(), nsdecl).append('\"').str());
    }

    response->setContent(content.length(), content.str());
    response->setContentType(HTTP_TYPE_APPLICATION_XML_UTF8);
    response->setStatus(HTTP_STATUS_OK);
    response->send();
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


int EspHttpBinding::onGetReqSampleJson(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    generateSampleJson(true, ctx, request, response, serv, method);
    return 0;
}

int EspHttpBinding::onGetRespSampleJson(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    generateSampleJson(false, ctx, request, response, serv, method);
    return 0;
}

int EspHttpBinding::onStartUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringArray fileNames, files;
    Owned<IMultiException> me = MakeMultiException("FileSpray::UploadFile()");
    try
    {
        if (!ctx.validateFeatureAccess(FILE_UPLOAD, SecAccess_Full, false))
            throw MakeStringException(-1, "Permission denied.");

        StringBuffer netAddress, path;
        request->getParameter("NetAddress", netAddress);
        request->getParameter("Path", path);
        if (((netAddress.length() < 1) || (path.length() < 1)))
            request->readUploadFileContent(fileNames, files);
        else
            request->readContentToFiles(netAddress, path, fileNames);

        return onFinishUpload(ctx, request, response, serv, method, fileNames, files, NULL);
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*MakeStringExceptionDirect(-1, "Unknown Exception"));
    }

    //There is an exception. So close socket connection right after sending the response to avoid more file upload.
    response->setPersistentEligible(false);

    return onFinishUpload(ctx, request, response, serv, method, fileNames, files, me);
}

int EspHttpBinding::onFinishUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response,    const char *serv, const char *method,
                                   StringArray& fileNames, StringArray& files, IMultiException *me)
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
        IWARNLOG("Exception(s) in EspHttpBinding::onFinishUpload - %s", me->errorMessage(msg).append('\n').str());
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

        page.appendContent(new CHtmlText("For complete sets of example messages:<br/>"));
        list = (CHtmlList *)page.appendContent(new CHtmlList);
        list->appendContent(new CHtmlLink("XML Requests", wsLink.set(urlParams).append("&reqxml_").str()));
        list->appendContent(new CHtmlLink("XML Responses", wsLink.set(urlParams).append("&respxml_").str()));
        IProperties *parms = request->queryParameters();
        if (parms->hasProp("include_jsonreqs_"))
            list->appendContent(new CHtmlLink("JSON Requests", wsLink.set(urlParams).append("&reqjson_").str()));

        list->appendContent(new CHtmlLink("JSON Responses", wsLink.set(urlParams).append("&respjson_").str()));

        page.appendContent(new CHtmlText("The following operations are supported:<br/>"));

        //links to the form pages
        list = (CHtmlList *)page.appendContent(new CHtmlList);
        StringBuffer urlFormParams(urlParams);
        urlFormParams.append(urlFormParams.length()>0 ? "&form" : "?form");
        for(int i = 0, tot = methods.length(); i < tot; ++i)
        {
            CMethodInfo &method = methods.item(i);
            {
                wsLink.setf("%s%s", method.m_label.str(),urlFormParams.str());
                list->appendContent(new CHtmlLink(method.m_label.str(), wsLink.str()));
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

        Owned<IXslTransform> xform = xslp->createXslTransform();
        xform->loadXslFromFile(StringBuffer(getCFD()).append("./xslt/gen_form.xsl").str());
        xform->setXmlSource(schema.str(), schema.length()+1);
        const char* authMethod = context.getAuthenticationMethod();
        if (authMethod && !strieq(authMethod, "none") && ((context.getDomainAuthType() == AuthPerSessionOnly) || (context.getDomainAuthType() == AuthTypeMixed)))
        {
            xform->setParameter("showLogout", "1");
            const char* userId = context.queryUserId();
            if (!isEmptyString(userId))
                xform->setStringParameter("username", userId);
        }

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
        xform->setParameter("includeJsonTest", m_includeJsonTest?"1":"0");

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
        IERRLOG("Unexpected error: parsing XML: %s", e->errorMessage(msg).str());
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
        IERRLOG("Unexpected error: parsing XML: %s", e->errorMessage(msg).str());
    }
}
