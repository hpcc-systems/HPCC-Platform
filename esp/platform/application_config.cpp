/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "jliball.hpp"

// We can use "work" as the first parameter when debugging.
#define ESP_SINGLE_PROCESS

//ESP Core
#include "espp.hpp"
#include "espcfg.ipp"
#include "esplog.hpp"
#include "espcontext.hpp"

enum class LdapType { LegacyAD, AzureAD };

static void appendPTreeFromYamlFile(IPropertyTree *tree, const char *file, bool overwriteAttr)
{
    Owned<IPropertyTree> appendTree = createPTreeFromYAMLFile(file);
    if (appendTree->hasProp("esp"))
        mergeConfiguration(*tree, *appendTree->queryPropTree("esp"), nullptr, overwriteAttr);
    else
        mergeConfiguration(*tree, *appendTree, nullptr, overwriteAttr);
}

IPropertyTree *loadApplicationConfig(const char *application, const char* argv[])
{
    Owned<IPropertyTree> applicationConfig = createPTree(application);
    IPropertyTree *defaultConfig = applicationConfig->addPropTree("esp");

    char sepchar = getPathSepChar(hpccBuildInfo.componentDir);

    StringBuffer path(hpccBuildInfo.componentDir);
    addPathSepChar(path, sepchar).append("applications").append(sepchar).append("common").append(sepchar);
    if (checkDirExists(path))
    {
        Owned<IDirectoryIterator> common_dir = createDirectoryIterator(path, "*.yaml", false, false);
        ForEach(*common_dir)
            appendPTreeFromYamlFile(defaultConfig, common_dir->query().queryFilename(), true);
    }

    path.set(hpccBuildInfo.componentDir);
    addPathSepChar(path, sepchar).append("applications").append(sepchar).append(application).append(sepchar);
    if (!checkDirExists(path))
        throw MakeStringException(-1, "Can't find esp application %s (dir %s)", application, path.str());
    Owned<IDirectoryIterator> application_dir = createDirectoryIterator(path, "*.yaml", false, false);
    ForEach(*application_dir)
        appendPTreeFromYamlFile(defaultConfig, application_dir->query().queryFilename(), true);

    //apply provided config to the application
    Owned<IPropertyTree> config = loadConfiguration(defaultConfig, argv, "esp", "ESP", nullptr, nullptr);

    return config.getClear();
}

static void copyAttributes(IPropertyTree *target, IPropertyTree *src)
{
    Owned<IAttributeIterator> aiter = src->getAttributes();
    ForEach (*aiter)
        target->addProp(aiter->queryName(), aiter->queryValue());
}

static void copyDirectories(IPropertyTree *target, IPropertyTree *src)
{
    if (!src || !target)
        return;
    Owned<IAttributeIterator> aiter = src->getAttributes();
    ForEach (*aiter)
    {
        IPropertyTree *entry = target->addPropTree("Category");
        entry->setProp("@name", aiter->queryName()+1);
        entry->setProp("@dir", aiter->queryValue());
    }
}

void addAuthDomains(IPropertyTree *appEsp, IPropertyTree *legacyEsp)
{
    IPropertyTree *authDomainTree = appEsp->queryPropTree("authDomain");
    if (authDomainTree)
    {
        IPropertyTree *legacyAuthDomains = legacyEsp->addPropTree("AuthDomains");
        IPropertyTree *legacyAuthDomain = legacyAuthDomains->addPropTree("AuthDomain");
        copyAttributes(legacyAuthDomain, authDomainTree);
        return;
    }

    VStringBuffer AuthDomains(
        "<AuthDomains>"
            "<AuthDomain authType='AuthPerRequestOnly' clientSessionTimeoutMinutes='120' domainName='default'"
                " invalidURLsAfterAuth='/esp/login' loginLogoURL='/esp/files/eclwatch/img/Loginlogo.png'"
                " logonURL='/esp/files/Login.html' logoutURL='' serverSessionTimeoutMinutes='240'"
                " unrestrictedResources='/favicon.ico,/esp/files/*,/esp/xslt/*'/>"
        "</AuthDomains>");
    legacyEsp->addPropTree("AuthDomains", createPTreeFromXMLString(AuthDomains));
}

bool addLdapSecurity(IPropertyTree *legacyEsp, IPropertyTree *appEsp, StringBuffer &bindAuth, LdapType ldapType)
{
    StringBuffer path(hpccBuildInfo.componentDir);
    char sepchar = getPathSepChar(hpccBuildInfo.componentDir);
    addPathSepChar(path, sepchar).append("applications").append(sepchar).append("common").append(sepchar).append("ldap").append(sepchar).append("ldap.yaml");
    if (checkFileExists(path))
        appendPTreeFromYamlFile(appEsp, path.str(), false);

    IPropertyTree *appLdap = appEsp->queryPropTree("ldap");
    if (!appLdap)
        throw MakeStringException(-1, "Can't find application LDAP settings.  To run without security set 'auth: none'");

    if (!appLdap->hasProp("@ldapAddress"))
        throw MakeStringException(-1, "LDAP not configured (Missing 'ldapAddress').  To run without security set 'auth: none'");

    IPropertyTree *legacyLdap = legacyEsp->addPropTree("ldapSecurity");
    copyAttributes(legacyLdap, appLdap);

    StringAttr configname(appLdap->queryProp("@objname"));
    if (!legacyLdap->hasProp("@name"))
        legacyLdap->setProp("@name", configname.str());

    StringAttr resourcesBasedn(appLdap->queryProp("@resourcesBasedn"));
    StringAttr workunitsBasedn(appLdap->queryProp("@workunitsBasedn"));
    bindAuth.setf("<Authenticate method='LdapSecurity' config='%s' resourcesBasedn='%s' workunitsBasedn='%s'/>", configname.str(), resourcesBasedn.str(), workunitsBasedn.str());

    VStringBuffer authenticationXml("<Authentication ldapConnections='10' ldapServer='%s' method='ldaps' passwordExpirationWarningDays='10'/>", configname.str());
    legacyEsp->addPropTree("Authentication", createPTreeFromXMLString(authenticationXml));
    return true;
}

bool addAuthNZSecurity(const char *name, IPropertyTree *legacyEsp, IPropertyTree *appEsp, StringBuffer &bindAuth)
{
    IPropertyTree *authNZ = appEsp->queryPropTree("authNZ");
    if (!authNZ)
        throw MakeStringException(-1, "Can't find application AuthNZ section.  To run without security set 'auth: none'");
    authNZ = authNZ->queryPropTree(name);
    if (!authNZ)
        throw MakeStringException(-1, "Can't find application %s AuthNZ settings.  To run without security set 'auth: none'", name);
    IPropertyTree *appSecMgr = authNZ->queryPropTree("SecurityManager");
    if (!appSecMgr)
    {
        appSecMgr = authNZ;
    }
    const char *method = appSecMgr->queryProp("@name");
    const char *tag = appSecMgr->queryProp("@type");
    if (isEmptyString(tag))
        throw MakeStringException(-1, "SecurityManager type attribute required.  To run without security set 'auth: none'");

    IPropertyTree *legacy = legacyEsp->addPropTree("SecurityManagers");
    legacy = legacy->addPropTree("SecurityManager");
    copyAttributes(legacy, appSecMgr);
    legacy = legacy->addPropTree(tag);
    mergePTree(legacy, authNZ); //extra info clean up later
    legacy->removeProp("SecurityManager"); //already copied these attributes above, don't need this as a child

    bindAuth.setf("<Authenticate method='%s'/>", method ? method : "unknown");
    return true;
}

//  auth "none" must be explicit, default to secure mode, don't want to accidentally turn off security
bool addSecurity(IPropertyTree *legacyEsp, IPropertyTree *appEsp, StringBuffer &bindAuth)
{
    const char *auth = appEsp->queryProp("@auth");
    if (isEmptyString(auth))
        throw MakeStringException(-1, "'auth' attribute required.  To run without security set 'auth: none'");
    if (streq(auth, "none"))
        return false;

    addAuthDomains(appEsp, legacyEsp);

    if (streq(auth, "ldap"))
        return addLdapSecurity(legacyEsp, appEsp, bindAuth, LdapType::LegacyAD);
    if (streq(auth, "azure_ldap"))
        return addLdapSecurity(legacyEsp, appEsp, bindAuth, LdapType::AzureAD);
    return addAuthNZSecurity(auth, legacyEsp, appEsp, bindAuth);
}

void bindAuthResources(IPropertyTree *legacyAuthenticate, IPropertyTree *app, const char *service, const char *auth)
{
    IPropertyTree *appAuth = nullptr;
    if (isEmptyString(auth) || streq(auth, "ldap") || streq(auth, "azure_ldap"))
        appAuth = app->queryPropTree("ldap");
    else if (streq(auth, "none"))
        return;
    else
    {
        appAuth = app->queryPropTree("authNZ");
        if (!appAuth)
            return;
        appAuth = appAuth->queryPropTree(auth);
        if (!appAuth)
            return;
        const char *useResourceMapsFrom= appAuth->queryProp("@useResourceMapsFrom");
        if (!isEmptyString(useResourceMapsFrom))
            appAuth= app->queryPropTree(useResourceMapsFrom);
    }
    if (!appAuth)
        throw MakeStringException(-1, "Can't find application Auth settings.  To run without security set 'auth: none'");
    IPropertyTree *root_access = appAuth->queryPropTree("root_access");
    if (root_access)//root_access (feature map, auth map) not required for simple security managers
    {
        StringAttr required(root_access->queryProp("@required"));
        StringAttr description(root_access->queryProp("@description"));
        StringAttr resource(root_access->queryProp("@resource"));
        VStringBuffer locationXml("<Location path='/' resource='%s' required='%s' description='%s'/>", resource.str(), required.str(), description.str());
        legacyAuthenticate->addPropTree("Location", createPTreeFromXMLString(locationXml));
    }

    VStringBuffer featuresPath("resource_map/%s/Feature", service);
    Owned<IPropertyTreeIterator> features = appAuth->getElements(featuresPath);
    ForEach(*features)
        legacyAuthenticate->addPropTree("Feature", LINK(&features->query()));

    VStringBuffer settingsPath("resource_map/%s/Setting", service);
    Owned<IPropertyTreeIterator> settings = appAuth->getElements(settingsPath);
    ForEach(*settings)
        legacyAuthenticate->addPropTree("Setting", LINK(&settings->query()));
}

void bindService(IPropertyTree *legacyEsp, IPropertyTree *app, const char *service, const char *protocol, const char *netAddress, unsigned port, const char *bindAuth, const char *wsdlAddress, int seq)
{
    VStringBuffer xpath("binding_plugins/@%s", service);
    const char *binding_plugin = app->queryProp(xpath);

    VStringBuffer bindingXml("<EspBinding name='%s_binding' service='%s_service' protocol='%s' type='%s_http' plugin='%s' netAddress='%s' wsdlServiceAddress='%s' port='%d'/>", service, service, protocol, service, binding_plugin, netAddress, wsdlAddress, port);
    IPropertyTree *bindingEntry = legacyEsp->addPropTree("EspBinding", createPTreeFromXMLString(bindingXml));
    if (seq==0)
        bindingEntry->setProp("@defaultBinding", "true");

    if (!isEmptyString(bindAuth))
    {
        const char *auth = app->queryProp("@auth");
        IPropertyTree *authenticate = bindingEntry->addPropTree("Authenticate", createPTreeFromXMLString(bindAuth));
        if (authenticate)
            bindAuthResources(authenticate, app, service, auth);
    }

    Owned<IPropertyTreeIterator> corsAllowed = app->getElements("corsAllowed");
    if (corsAllowed->first())
    {
        IPropertyTree *cors = bindingEntry->addPropTree("cors");
        ForEach(*corsAllowed)
            cors->addPropTree("allowed", createPTreeFromIPT(&corsAllowed->query()));
    }

    // bindingInfo has sub-elements named for services like 'esdl' or 'eclwatch'.
    // The elements under each service section are merged into the bindingEntry
    // for matching services.
    xpath.setf("bindingInfo/%s", service);
    IPTree *branch = app->queryBranch(xpath.str());
    if (branch)
        mergePTree(bindingEntry, branch);
}

static void mergeServicePTree(IPropertyTree *target, IPropertyTree *toMerge)
{
    Owned<IAttributeIterator> aiter = toMerge->getAttributes();
    ForEach (*aiter)
    {
        //make uppercase attributes into elements to match legacy ESP config
        const char *xpath = aiter->queryName();
        if (xpath && xpath[0]=='@' && isupper(xpath[1]))
            xpath++;
        target->addProp(xpath, aiter->queryValue());
    }
    Owned<IPropertyTreeIterator> iter = toMerge->getElements("*");
    ForEach (*iter)
    {
        IPropertyTree &e = iter->query();
        target->addPropTree(e.queryName(), LINK(&e));
    }
}

void addService(IPropertyTree *legacyEsp, IPropertyTree *app, const char *application, const char *service, const char *protocol, const char *netAddress, unsigned port, const char *bindAuth, const char *wsdlAddress, int seq)
{
    VStringBuffer plugin_xpath("service_plugins/@%s", service);
    const char *service_plugin = app->queryProp(plugin_xpath);
    VStringBuffer serviceXml("<EspService name='%s_service' type='%s' plugin='%s'/>", service, service, service_plugin);

    VStringBuffer config_xpath("%s/%s", application, service);
    IPropertyTree *serviceConfig = app->queryPropTree(config_xpath);
    IPropertyTree *serviceEntry = legacyEsp->addPropTree("EspService", createPTreeFromXMLString(serviceXml));

    if (serviceConfig && serviceEntry)
        mergeServicePTree(serviceEntry, serviceConfig);

    bindService(legacyEsp, app, service, protocol, netAddress, port, bindAuth, wsdlAddress, seq);
}

bool addProtocol(IPropertyTree *legacyEsp, IPropertyTree *app)
{
    const char *maxReqLenStr = app->queryProp("@maxRequestEntityLength");
    if (isEmptyString(maxReqLenStr))
        maxReqLenStr = "8M";
    offset_t maxReqLen = friendlyStringToSize(maxReqLenStr);

    bool useTls = app->getPropBool("@tls", true);
    if (useTls)
    {
        VStringBuffer protocolXml("<EspProtocol name='https' type='secure_http_protocol' plugin='esphttp' maxRequestEntityLength='%llu'/>", maxReqLen);
        IPropertyTree *protocol = legacyEsp->addPropTree("EspProtocol", createPTreeFromXMLString(protocolXml));
        IPropertyTree *tls = app->queryPropTree("tls_config");
        if (protocol && tls)
        {
            Owned<IAttributeIterator> aiter = tls->getAttributes();
            ForEach (*aiter)
                tls->addProp(aiter->queryName()+1, aiter->queryValue()); //attributes to elements to match legacy config
            Owned<IPropertyTree> temp;
            const char *instance = app->queryProp("@instance");
            if (instance)
            {
                StringBuffer xml;
                toXML(tls, xml);
                xml.replaceString("{$instance}", instance);
                temp.setown(createPTreeFromXMLString(xml));
                tls = temp.get();
            }
            mergePTree(protocol, tls);
        }
    }
    else
    {
        VStringBuffer protocolXml("<EspProtocol name='http' type='http_protocol' plugin='esphttp' maxRequestEntityLength='%llu'/>", maxReqLen);
        legacyEsp->addPropTree("EspProtocol", createPTreeFromXMLString(protocolXml));
    }
    return useTls;
}

void addServices(IPropertyTree *legacyEsp, IPropertyTree *appEsp, const char *application, const char *auth, bool tls)
{
    Owned<IPropertyTreeIterator> services = appEsp->getElements("application/services");
    int port = appEsp->getPropInt("service/@port", appEsp->getPropInt("@port", 8880));
    const char *netAddress = appEsp->queryProp("@netAddress");
    if (!netAddress)
        netAddress = ".";
    const char *wsdlAddress = appEsp->queryProp("service/@wsdlAddress");
    if (!wsdlAddress)
        wsdlAddress = "";
    int seq=0;
    ForEach(*services)
        addService(legacyEsp, appEsp, application, services->query().queryProp("."), tls ? "https" : "http", netAddress, port, auth, wsdlAddress, seq++);
}

void addBindingToServiceResource(IPropertyTree *service, const char *name, const char *serviceType, unsigned port,
    const char *baseDN, const char *workunitsBaseDN)
{
    IPropertyTree *resourcesTree = service->queryPropTree("Resources");
    if (!resourcesTree)
        resourcesTree = service->addPropTree("Resources", createPTree("Resources"));
    IPropertyTree *bindingTree = resourcesTree->addPropTree("Binding", createPTree("Binding"));
    bindingTree->setProp("@name", name);
    bindingTree->setProp("@service", serviceType);
    bindingTree->setPropInt("@port", port);
    bindingTree->setProp("@basedn", baseDN);
    bindingTree->setProp("@workunitsBasedn", workunitsBaseDN);
}

const char *getLDAPBaseDN(IPropertyTree &service, IPropertyTree *ldapAuthTree, const char *xpath)
{
    const char *resourcesBasedn = service.queryProp(xpath);
    if (!isEmptyString(resourcesBasedn))
        return resourcesBasedn;

    if (!ldapAuthTree)
        return nullptr;

    VStringBuffer authTreeXPath("ldap/%s", xpath);
    return ldapAuthTree->queryProp(authTreeXPath);
}

void setLDAPSecurityInWSAccess(IPropertyTree *legacyEsp, IPropertyTree *legacyLdap)
{
    IPropertyTree *wsAccessService = legacyEsp->queryPropTree("EspService[@type='ws_access']");
    if (!wsAccessService)
        throw makeStringException(-1, "Missing configuration for EspService 'ws_access'");

    IPropertyTree *wsSMCService = legacyEsp->queryPropTree("EspService[@type='WsSMC']");
    if (!wsSMCService)
        throw makeStringException(-1, "Missing configuration for EspService 'WsSMC'");

    const char *fileBaseDN = legacyLdap->queryProp("@filesBasedn");
    if (!isEmptyString(fileBaseDN))
    {
        IPropertyTree *filesTree = wsAccessService->queryPropTree("Files");
        if (!filesTree)
            filesTree = wsAccessService->addPropTree("Files", createPTree("Files"));
        filesTree->setProp("@basedn", fileBaseDN);
    }

    VStringBuffer xpath("EspBinding[@service='%s']", wsSMCService->queryProp("@name"));
    Owned<IPropertyTreeIterator> bindings = legacyEsp->getElements(xpath);
    ForEach(*bindings)
    {
        IPropertyTree &binding = bindings->query();
        IPropertyTree *authTree = binding.queryPropTree("Authenticate");
        const char *baseDN = authTree->queryProp("@resourcesBasedn");
        const char *workunitsBaseDN = authTree->queryProp("@workunitsBasedn");
        addBindingToServiceResource(wsAccessService, binding.queryProp("@name"), "WsSMC",
            binding.getPropInt("@port"), baseDN, workunitsBaseDN);
    }

    //Now, setLDAPSecurity for the esp applications other than eclwatch.
    char sepchar = getPathSepChar(hpccBuildInfo.componentDir);
    Owned<IPropertyTreeIterator> services = getGlobalConfigSP()->getElements("services[@class='esp'][@public='true']");
    ForEach(*services)
    {
        IPropertyTree &service = services->query();
        const char *type = service.queryProp("@type");
        if (strieq(type, "eclwatch"))
            continue;

        StringBuffer path(hpccBuildInfo.componentDir);
        addPathSepChar(path, sepchar).append("applications").append(sepchar).append(type).append(sepchar);
        path.append("ldap_authorization_map.yaml");
        Owned<IPropertyTree> authTree = createPTreeFromYAMLFile(path);
        if (!authTree)
            IERRLOG("Failed to read %s for EspService %s", path.str(), type);
        const char *baseDN = getLDAPBaseDN(service, authTree, "@resourcesBasedn");
        const char *workunitsBaseDN = getLDAPBaseDN(service, authTree, "@workunitsBasedn");
        if (!isEmptyString(baseDN) || !isEmptyString(workunitsBaseDN))
            addBindingToServiceResource(wsAccessService, type, type, 0, //port number unknown. Seem not used. Set to 0 for now.
                baseDN, workunitsBaseDN);
    }
}

void copyTraceFlags(IPropertyTree *legacyEsp, IPropertyTree *appEsp)
{
    IPropertyTree *traceFlags = appEsp->queryPropTree("traceFlags");
    if (traceFlags)
    {
        IPropertyTree *legacyTraceFlags = legacyEsp->addPropTree("TraceFlags");
        copyAttributes(legacyTraceFlags, traceFlags);
    }
}

IPropertyTree *buildApplicationLegacyConfig(const char *application, const char* argv[])
{
    Owned<IPropertyTree> appEspConfig = loadApplicationConfig(application, argv);

    Owned<IPropertyTree> legacy = createPTreeFromXMLString("<Environment><Software><EspProcess/><Directories name='HPCCSystems'/></Software></Environment>");
    IPropertyTree *legacyEsp = legacy->queryPropTree("Software/EspProcess");
    copyAttributes(legacyEsp, appEspConfig);
    if (!legacyEsp->hasProp("@name") && legacyEsp->hasProp("@instance"))
        legacyEsp->setProp("@name", legacyEsp->queryProp("@instance"));

    if (!legacyEsp->hasProp("@directory"))
    {
        const char *componentName = legacyEsp->queryProp("@name");
        VStringBuffer s("%s/%s", hpccBuildInfo.runtimeDir, isEmptyString(componentName) ? application : componentName);
        legacyEsp->setProp("@directory", s.str());
    }
    if (!legacyEsp->hasProp("@componentfilesDir"))
        legacyEsp->setProp("@componentfilesDir", hpccBuildInfo.componentDir);

    bool tls = addProtocol(legacyEsp, appEspConfig);

    StringBuffer bindAuth;
    addSecurity(legacyEsp, appEspConfig, bindAuth);
    addServices(legacyEsp, appEspConfig, application, bindAuth, tls);

    IPropertyTree *legacyLdap = legacyEsp->queryPropTree("ldapSecurity");
    if (legacyLdap && strieq(application, "eclwatch"))
        setLDAPSecurityInWSAccess(legacyEsp, legacyLdap);

    IPropertyTree *legacyDirectories = legacy->queryPropTree("Software/Directories");
    IPropertyTree *appDirectories = appEspConfig->queryPropTree("directories");
    copyDirectories(legacyDirectories, appDirectories);

    copyTraceFlags(legacyEsp, appEspConfig);
    return legacy.getClear();
}
