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
#include "build-config.h"

static void appendPTreeFromYamlFile(IPropertyTree *tree, const char *file)
{
    Owned<IPropertyTree> appendTree = createPTreeFromYAMLFile(file);
    if (appendTree->hasProp("esp"))
        mergeConfiguration(*tree, *appendTree->queryPropTree("esp"), nullptr);
    else
        mergeConfiguration(*tree, *appendTree, nullptr);
}

IPropertyTree *loadApplicationConfig(const char *application, const char* argv[])
{
    char sepchar = getPathSepChar(COMPONENTFILES_DIR);
    StringBuffer path(COMPONENTFILES_DIR);
    addPathSepChar(path, sepchar).append("applications").append(sepchar).append(application).append(sepchar);
    if (!checkDirExists(path))
        throw MakeStringException(-1, "Can't find esp application %s (dir %s)", application, path.str());

    Owned<IPropertyTree> applicationConfig = createPTree(application);
    IPropertyTree *defaultConfig = applicationConfig->addPropTree("esp");
    Owned<IDirectoryIterator> dir = createDirectoryIterator(path, "*.yaml", false, false);
    ForEach(*dir)
        appendPTreeFromYamlFile(defaultConfig, dir->query().queryFilename());

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

//returns true if ldap is enabled.
//  only support ldap or "none" for now.
//  "none" must be explicit, don't want to accidentally turn off security
bool addSecurity(IPropertyTree *legacyEsp, IPropertyTree *appEsp, StringBuffer &bindAuth)
{
    const char *auth = appEsp->queryProp("@auth");
    if (auth && streq(auth, "none"))
        return false;
    const char *ldapAddress = appEsp->queryProp("@ldapAddress");
    if (isEmptyString(ldapAddress))
        throw MakeStringException(-1, "LDAP not configured.  To run without security set auth=none");
    IPropertyTree *appLdap = appEsp->queryPropTree("ldap");
    if (!appLdap)
        throw MakeStringException(-1, "can't fine application LDAP settings.  To run without security set auth=none");

    IPropertyTree *legacyLdap = legacyEsp->addPropTree("ldapSecurity");
    copyAttributes(legacyLdap, appLdap);
    legacyLdap->setProp("@ldapAddress", ldapAddress);

    StringAttr configname(appLdap->queryProp("@objname"));
    if (!legacyLdap->hasProp("@name"))
        legacyLdap->setProp("@name", configname.str());

    StringAttr resourcesBasedn(appLdap->queryProp("@resourcesBasedn"));
    StringAttr workunitsBasedn(appLdap->queryProp("@workunitsBasedn"));
    bindAuth.setf("<Authenticate method='LdapSecurity' config='%s' resourcesBasedn='%s' workunitsBasedn='%s'/>", configname.str(), resourcesBasedn.str(), workunitsBasedn.str());

    VStringBuffer authenticationXml("<Authentication htpasswdFile='/etc/HPCCSystems/.htpasswd' ldapAuthMethod='kerberos' ldapConnections='10' ldapServer='%s' method='ldaps' passwordExpirationWarningDays='10'/>", configname.str());
    legacyEsp->addPropTree("Authentication", createPTreeFromXMLString(authenticationXml));

    return true;
}

void bindAuthResources(IPropertyTree *legacyAuthenticate, IPropertyTree *app, const char *service)
{
    IPropertyTree *appLdap = app->queryPropTree("ldap");
    if (!appLdap)
        throw MakeStringException(-1, "can't find application LDAP settings.  To run without security set auth=none");
    IPropertyTree *root_access = appLdap->queryPropTree("root_access");
    StringAttr required(root_access->queryProp("@required"));
    StringAttr description(root_access->queryProp("@description"));
    StringAttr resource(root_access->queryProp("@resource"));
    VStringBuffer locationXml("<Location path='/' resource='%s' required='%s' description='%s'/>", resource.str(), required.str(), description.str());
    legacyAuthenticate->addPropTree("Location", createPTreeFromXMLString(locationXml));

    VStringBuffer featuresPath("resource_map/%s/Feature", service);
    Owned<IPropertyTreeIterator> features = appLdap->getElements(featuresPath);
    ForEach(*features)
        legacyAuthenticate->addPropTree("Feature", LINK(&features->query()));
}

void bindService(IPropertyTree *legacyEsp, IPropertyTree *app, const char *service, const char *protocol, const char *netAddress, unsigned port, const char *auth, int seq)
{
    VStringBuffer xpath("binding_plugins/@%s", service);
    const char *binding_plugin = app->queryProp(xpath);

    VStringBuffer bindingXml("<EspBinding name='%s_binding' service='%s_service' protocol='%s' type='%s_http' plugin='%s' netAddress='%s' port='%d'/>", service, service, protocol, service, binding_plugin, netAddress, port);
    IPropertyTree *bindingEntry = legacyEsp->addPropTree("EspBinding", createPTreeFromXMLString(bindingXml));
    if (seq==0)
        bindingEntry->setProp("@defaultBinding", "true");

    if (!isEmptyString(auth))
    {
        IPropertyTree *authenticate = bindingEntry->addPropTree("Authenticate", createPTreeFromXMLString(auth));
        if (authenticate)
            bindAuthResources(authenticate, app, service);
    }
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

void addService(IPropertyTree *legacyEsp, IPropertyTree *app, const char *application, const char *service, const char *protocol, const char *netAddress, unsigned port, const char *auth, int seq)
{
    VStringBuffer plugin_xpath("service_plugins/@%s", service);
    const char *service_plugin = app->queryProp(plugin_xpath);
    VStringBuffer serviceXml("<EspService name='%s_service' type='%s' plugin='%s'/>", service, service, service_plugin);

    VStringBuffer config_xpath("%s/%s", application, service);
    IPropertyTree *serviceConfig = app->queryPropTree(config_xpath);
    IPropertyTree *serviceEntry = legacyEsp->addPropTree("EspService", createPTreeFromXMLString(serviceXml));

    if (serviceConfig && serviceEntry)
        mergeServicePTree(serviceEntry, serviceConfig);

    bindService(legacyEsp, app, service, protocol, netAddress, port, auth, seq);
}

bool addProtocol(IPropertyTree *legacyEsp, IPropertyTree *app)
{
    bool useTls = app->getPropBool("@tls", true);
    if (useTls)
    {
        StringBuffer protocolXml("<EspProtocol name='https' type='secure_http_protocol' plugin='esphttp' maxRequestEntityLength='60000000'/>");
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
        StringBuffer protocolXml("<EspProtocol name='http' type='http_protocol' plugin='esphttp' maxRequestEntityLength='60000000'/>");
        legacyEsp->addPropTree("EspProtocol", createPTreeFromXMLString(protocolXml));
    }
    return useTls;
}

void addServices(IPropertyTree *legacyEsp, IPropertyTree *appEsp, const char *application, const char *auth, bool tls)
{
    Owned<IPropertyTreeIterator> services = appEsp->getElements("application/services");
    int port = appEsp->getPropInt("@port", 8880);
    const char *netAddress = appEsp->queryProp("@netAddress");
    if (!netAddress)
        netAddress = ".";
    int seq=0;
    ForEach(*services)
        addService(legacyEsp, appEsp, application, services->query().queryProp("."), tls ? "https" : "http", netAddress, port, auth, seq++);
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
        VStringBuffer s("%s/%s", RUNTIME_DIR, isEmptyString(componentName) ? application : componentName);
        legacyEsp->setProp("@directory", s.str());
    }
    if (!legacyEsp->hasProp("@componentfilesDir"))
        legacyEsp->setProp("@componentfilesDir", COMPONENTFILES_DIR);

    bool tls = addProtocol(legacyEsp, appEspConfig);

    StringBuffer auth;
    addSecurity(legacyEsp, appEspConfig, auth);
    addServices(legacyEsp, appEspConfig, application, auth, tls);

    IPropertyTree *legacyDirectories = legacy->queryPropTree("Software/Directories");
    IPropertyTree *appDirectories = appEspConfig->queryPropTree("directories");
    copyDirectories(legacyDirectories, appDirectories);

    return legacy.getClear();
}
