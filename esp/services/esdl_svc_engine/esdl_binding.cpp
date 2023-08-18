/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "esdl_binding.hpp"
#include "params2xml.hpp"
#include "wsexcept.hpp"
#include "httpclient.hpp"

#include "rtlformat.hpp"

//for dali communication
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"

#include "jsonhelpers.hpp"
#include "eclhelper.hpp"    //IXMLWriter
#include "thorxmlwrite.hpp" //JSON WRITER
#include "workunit.hpp"
#include "wuwebview.hpp"
#include "jsmartsock.ipp"
#include "esdl_monitor.hpp"
#include "EsdlAccessMapGenerator.hpp"

#include "loggingagentbase.hpp"
#include "httpclient.ipp"
#include "esdl_summary_profile.hpp"

class EsdlSvcReporter : public EsdlDefReporter
{
protected:
    void reportSelf(Flags flag, const char* component, const char* level, const char* msg) const override
    {
        const LogMsgCategory* catPtr = nullptr;
        switch (flag & ReportCategoryMask)
        {
        case ReportDisaster:  catPtr = &MCoperatorDisaster; break;
        case ReportAError:    catPtr = &MCauditError; break;
        case ReportIError:    catPtr = &MCdebugError; break;
        case ReportOError:    catPtr = &MCoperatorError; break;
        case ReportUError:    catPtr = &MCuserError; break;
        case ReportIWarning:  catPtr = &MCdebugWarning; break;
        case ReportOWarning:  catPtr = &MCoperatorWarning; break;
        case ReportUWarning:  catPtr = &MCuserWarning; break;
        case ReportDProgress: catPtr = &MCdebugProgress; break;
        case ReportOProgress: catPtr = &MCoperatorProgress; break;
        case ReportUProgress: catPtr = &MCuserProgress; break;
        case ReportDInfo:     catPtr = &MCdebugInfo; break;
        case ReportOInfo:     catPtr = &MCoperatorInfo; break;
        case ReportUInfo:     catPtr = &MCuserInfo; break;
        default: return;
        }
        LOG(*catPtr, "%s [%s]: %s", level, component, msg);
    }
};

static const char* Scope_EsdlService = "EsdlService";
static const char* Scope_EsdlMethod = "EsdlMethod";
static const char* Scope_BindingService = "BindingService";
static const char* Scope_BindingMethod = "BindingMethod";
static const char* Var_Service = "service";
static const char* Var_Method = "method";
#define ANNOTATION_AUTH_FEATURE "auth_feature"

// Config flag to return the schemaLocation on non-Soap Fault method responses
#define ANNOTATION_RETURNSCHEMALOCATION_ONOK "returnSchemaLocationOnOK"

// The scope mapper and level mapper instances could be static service members, but enabling the
// declaration of these types in the header requires inclusion of headers that unnecessarily affect
// other projects that include this class' header.
static EsdlAccessMapScopeMapper g_scopeMapper({Scope_EsdlService, Scope_EsdlMethod, Scope_BindingService, Scope_BindingMethod});
static EsdlAccessMapLevelMapper g_levelMapper;
// The reporter helper provides for construction and initialization of a shared IEsdlDefReporter
// instance. If in the future, however, it is decided that service instances need control of the
// reporter flags, the helper would be replaced by a local instance of a reporter.
struct ReporterHelper
{
    ReporterHelper()
    {
        m_reporter.setown(new EsdlSvcReporter());
        m_reporter->setFlags(IEsdlDefReporter::ReportMethod | IEsdlDefReporter::ReportUserAudience | IEsdlDefReporter::ReportErrorClass, true);
    }

    IEsdlDefReporter* getLink() const
    {
        return m_reporter.getLink();
    }

    Owned<IEsdlDefReporter> m_reporter;
};
static ReporterHelper g_reporterHelper;

/*
 * trim xpath at first instance of element
 */
bool trimXPathToParentSDSElement(const char *element, const char * xpath, StringBuffer & parentNodeXPath)
{
    if (!element || !*element)
        return false;
    if (!xpath || !*xpath)
        return false;

    int len = strlen(element);
    StringArray paths;
    paths.appendList(xpath, "/");

    ForEachItemIn(idx, paths)
    {
        if (idx > 0)
            parentNodeXPath.append("/");

        parentNodeXPath.append(paths.item(idx));
        if (strncmp(element, paths.item(idx), len)==0)
            return true;
    }

    return false;
}

IPropertyTree * fetchESDLBindingFromStateFile(const char *process, const char *bindingName, const char * stateFileName)
{
    try
    {
        Owned<IPTree> esdlState = createPTreeFromXMLFile(stateFileName);
        if (esdlState)
        {
            const char * restoredBindingTS = esdlState->queryProp("@StateSaveTimems");
            ESPLOG(LogNormal, "ESDL State restored from local state store: %s created epoch: %s", bindingName, restoredBindingTS);
            return esdlState->getPropTree("EsdlBinding/Binding");
        }
        else
        {
            ESPLOG(LogNormal, "Failed to load DESDL binding state from local file: '%s' for ESP binding %s.%s", stateFileName, process, bindingName);
        }
    }
    catch (...)
    {
        ESPLOG(LogNormal, "Failed to load DESDL binding state from local file: '%s' for ESP binding %s.%s", stateFileName, process, bindingName);
    }

    return nullptr;
}

void saveState(const char * esdlDefinition, IPropertyTree * esdlBinding, const char * stateFileFullPath)
{
    StringBuffer dESDLState;
    dESDLState.setf( "<ESDLBindingState StateSaveTimems='%d'><EsdlDefinition>%s</EsdlDefinition>", msTick(), esdlDefinition ? esdlDefinition : "");
    if (esdlBinding)
    {
        dESDLState.append("<EsdlBinding>");
        toXML(esdlBinding, dESDLState);
        dESDLState.append("</EsdlBinding>");
    }

    dESDLState.append("</ESDLBindingState>");

#ifdef _DEBUG
fprintf(stdout, "\nESDL State to be stored:\n%s", dESDLState.str());
#endif

    Owned<IPropertyTree> serviceESDLDef = createPTreeFromXMLString(dESDLState.str(), ipt_caseInsensitive);

    // Saving to tmp file first, this should prob be an atomic action
    StringBuffer tempname;
    makeTempCopyName(tempname, stateFileFullPath);
    saveXML(tempname, serviceESDLDef);
    renameFile(stateFileFullPath, tempname, true);
}

void clearState(const char * stateFileFullPath)
{
    saveState("", NULL, stateFileFullPath);
}

bool EsdlServiceImpl::loadLoggingManager(Owned<ILoggingManager>& manager, IPTree* configuration)
{
    static CLoggingManagerLoader loader(nullptr, nullptr, nullptr, nullptr);
    if (!configuration)
    {
        manager.clear();
    }
    else
    {
        manager.setown(loader.create(*configuration));
        if (!manager)
            throw MakeStringException(-1, "ESDL Service %s could not load logging manager", m_espServiceName.str());
        manager->init(configuration, m_espServiceName);
    }
    m_bGenerateLocalTrxId = (!loggingManager() || !loggingManager()->hasService(LGSTGetTransactionID));
    return true;
}

template <typename file_loader_t>
inline bool EsdlServiceImpl::initMaskingEngineDirectory(const char* dir, const char* mask, file_loader_t loader)
{
    bool failed = false;
    Owned<IDirectoryIterator> files = createDirectoryIterator(dir, mask, false, false);
    ForEach(*files)
    {
        const char* filename = files->query().queryFilename();
        Owned<IPropertyTree> ptree = loader(filename, 0, ptr_ignoreWhiteSpace, nullptr);
        if (ptree)
        {
            if (!initMaskingEngineEmbedded(m_oStaticMaskingEngine, ptree, true))
                failed = true;
        }
    }
    return !failed;
}

bool EsdlServiceImpl::initMaskingEngineEmbedded(Owned<IDataMaskingEngine>& engine, const IPropertyTree* ptree, bool required)
{
    if (!ptree)
        return !required;
    bool created = false;
    if (!engine)
    {
        engine.setown(new DataMasking::CEngine());
        created = true;
    }
    size_t addedProfiles = engine->loadProfiles(*ptree);
    switch (addedProfiles)
    {
    case size_t(-1):
        if (created)
            engine.clear();
        return false;
    case 0:
        if (created)
            engine.clear();
        return !required;
    default:
        DBGLOG("initMaskingEngineEmbedded added %zu profiles to engine %p", addedProfiles, engine.get());
        return true;
    }
}

bool EsdlServiceImpl::initMaskingEngineDirectory(const char* dir)
{
    bool failed = false;
    if (!isEmptyString(dir))
    {
        if (!initMaskingEngineDirectory(dir, "*.xml", createPTreeFromXMLFile))
            failed = true;
    }
    return !failed;
}

void EsdlServiceImpl::init(const IPropertyTree *cfg,
                           const char *process,
                           const char *service)
{
    m_espServiceName.set(service);
    m_espProcName.set(process);
    m_bGenerateLocalTrxId = true;

    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]", process);
    IPropertyTree * espcfg = cfg->queryPropTree(xpath);
    if (!espcfg)
        throw MakeStringException(ERR_ESDL_BINDING_INTERNERR, "Could not access ESP process configuration: esp process '%s' service name '%s'", process, service);

    xpath.setf("EspService[@name=\"%s\"]", service);
    IPropertyTree * srvcfg = espcfg->queryPropTree(xpath);
    if (srvcfg)
    {
        //This is treated as the actual service name -sigh
        m_espServiceType.set(srvcfg->queryProp("@type"));
        if (m_espServiceType.length() <= 0)
            throw MakeStringException(-1, "Could not determine ESDL service configuration type: esp process '%s' service name '%s'", process, service);

        loadLoggingManager(m_oStaticLoggingManager, srvcfg->queryPropTree("LoggingManager"));

        bool initMaskingFailed = false;
        if (!initMaskingEngineDirectory(espcfg->queryProp("@maskingProfileDir")))
            initMaskingFailed = true;
        if (!initMaskingEngineEmbedded(m_oStaticMaskingEngine, srvcfg, false))
            initMaskingFailed = true;
        if (initMaskingFailed)
            throw makeStringExceptionV(-1, "ESDL service '%s' failed to load masking configuration", m_espServiceName.str());

        m_usesURLNameSpace = false;
        m_namespaceScheme.set(srvcfg->queryProp("@namespaceScheme"));
        if (m_namespaceScheme.isEmpty())
            m_namespaceScheme.set(espcfg->queryProp("@namespaceScheme"));

        if (streq(m_namespaceScheme.str(), NAMESPACE_SCHEME_CONFIG_VALUE_SCAPPS))
            m_serviceNameSpaceBase.set(SCAPPS_NAMESPACE_BASE);
        else if (srvcfg->hasProp("@namespaceBase"))
            m_serviceNameSpaceBase.set(srvcfg->queryProp("@namespaceBase")).trim();
        else
            m_serviceNameSpaceBase.set(DEFAULT_ESDLBINDING_URN_BASE);

        m_usesURLNameSpace = strstr(m_serviceNameSpaceBase, "://") != nullptr;

        const char* defaultFeatureAuth = srvcfg->queryProp("@defaultFeatureAuth");
        if (isEmptyString(defaultFeatureAuth))
            m_defaultFeatureAuth.append("${service}Access:full");
        else if (!strieq(defaultFeatureAuth, "no_default"))
            m_defaultFeatureAuth.append(defaultFeatureAuth);

        xpath.setf("EspBinding[@service=\"%s\"]", service); //get this service's binding cfg
        m_oEspBindingCfg.set(espcfg->queryPropTree(xpath.str()));

        xpath.setf("CustomBindingParameters/CustomBindingParameter[@key=\"UseDefaultEnterpriseTxSummaryProfile\"]/@value");
        bool useDefaultSummaryProfile = m_oEspBindingCfg->getPropBool(xpath.str());
        // Possible future update is to read profile settings from configuration
        if(useDefaultSummaryProfile)
            m_txSummaryProfile.setown(new CTxSummaryProfileEsdl);
    }
    else
        throw MakeStringException(-1, "Could not access ESDL service configuration: esp process '%s' service name '%s'", process, service);
}

void EsdlServiceImpl::configureJavaMethod(const char *method, IPropertyTree &entry, const char *classPath)
{
    const char *javaScopedMethod = entry.queryProp("@javamethod");
    if (!javaScopedMethod || !*javaScopedMethod)
    {
        UWARNLOG("ESDL binding - found java target method \"%s\" without java method defined.", method);
        return;
    }

    StringArray javaNodes;
    javaNodes.appendList(javaScopedMethod, ".");
    if (javaNodes.length()!=3) //adf: may become more flexible?
    {
        UWARNLOG("ESDL binding - target method \"%s\", configured java method currently must be of the form 'package.class.method', found (%s).", method, javaScopedMethod);
        return;
    }

    const char *javaPackage = javaNodes.item(0);
    const char *javaClass = javaNodes.item(1);
    const char *javaMethod = javaNodes.item(2);

    VStringBuffer javaScopedClass("%s.%s", javaPackage, javaClass);
    entry.setProp("@javaclass", javaScopedClass);

    if (!javaServiceMap.getValue(javaScopedClass))
    {
        VStringBuffer classPathOption("classpath=%s", classPath);
        try
        {
            Owned<IEmbedServiceContext> srvctx = ensureJavaEmbeded().createServiceContext(javaScopedClass, EFimport, classPathOption);
            if (srvctx)
                javaServiceMap.setValue(javaScopedClass, srvctx.getClear());
            else
            {
                //Log error, but try again next reload, in case the java class is fixed
                UWARNLOG("ESDL binding - failed to load java class %s for target method %s", javaScopedClass.str(), method);
            }
        }
        catch (IException *E)
        {
            DBGLOG(E, "DynamicESDL-JavaMethod:");
            javaExceptionMap.setValue(javaScopedClass, E);
            E->Release();
        }
    }
}

void EsdlServiceImpl::configureCppMethod(const char *method, IPropertyTree &entry, IEspPlugin*& plugin)
{
    const char *pluginName = entry.queryProp("@plugin");
    if (!pluginName || !*pluginName)
    {
        DBGLOG("C++ plugin name is missing for method %s", method);
        return;
    }
    const char *cppMethod = entry.queryProp("@method");
    if (!cppMethod || !*cppMethod)
    {
        DBGLOG("C++ method name is missing for method %s", method);
        return;
    }

    Owned<IEspPlugin> pluginHolder;
    if (!plugin)
    {
        DBGLOG("Loading plugin %s", pluginName);
        pluginHolder.setown(loadPlugin(pluginName));
        plugin = pluginHolder.get();
        if (!plugin || !plugin->isLoaded())
        {
            DBGLOG("C++ plugin %s could not be loaded for esdl method %s", pluginName, method);
            return;
        }
        cppPluginMap.remove(pluginName);
        cppPluginMap.setValue(pluginName, plugin);
    }

    cpp_service_method_t xproc = (cpp_service_method_t) plugin->getProcAddress(cppMethod);
    if (!xproc)
    {
        DBGLOG("C++ method %s could not be loaded from plugin %s in esdl method %s", cppMethod, pluginName, method);
        return;
    }
    cppProcMap.remove(method);
    cppProcMap.setValue(method, xproc);
}

void EsdlServiceImpl::configureUrlMethod(const char *method, IPropertyTree &entry)
{
    const char *url = entry.queryProp("@url");
    if (!url || !*url)
    {
        UWARNLOG("ESDL binding - found target method \"%s\" without target url!", method);
        return;
    }

    if (!entry.hasProp("@queryname"))
    {
        UWARNLOG("ESDL binding - found target method \"%s\" without target query!", method);
        return;
    }

    StringBuffer protocol, name, pw, path, iplist, ops;
    EsdlBindingImpl::splitURLList(url, protocol, name, pw, iplist, path, ops);

    entry.setProp("@prot", protocol);
    // Setting the path here overrides any existing path attribute value. Only do it when
    // the url attribute explicitly includes a non-empty value.
    if (path.length() > 0 && !streq(path, "/"))
        entry.setProp("@path", path);

    try
    {
        Owned<ISmartSocketFactory> sf = createSmartSocketFactory(iplist, true);

        connMap.remove(method);
        connMap.setValue(method, sf.get());
    }
    catch(IException* ie)
    {
        ESPLOG(LogMin,"DESDL: Error while setting up connection for method \"%s\" verify its configuration.", method);
        StringBuffer msg;
        ie->errorMessage(msg);
        ESPLOG(LogMin,"%s",msg.str());
        connMap.remove(method);
        ie->Release();
    }
}

void EsdlServiceImpl::handleTransformError(StringAttr &serviceError, MapStringTo<StringAttr, const char *> &methodErrors, IException *e, const char *service, const char *method)
{
    VStringBuffer msg("Encountered error while fetching transforms for service '%s'", service);
    if (!isEmptyString(method))
        msg.appendf(" method '%s'", method);
    msg.append(": ");
    if (e)
        e->errorMessage(msg);
    IERRLOG("%s", msg.str());

    if (!isEmptyString(method))
        methodErrors.setValue(method, msg.str());
    else
        serviceError.set(msg.str());
}

enum class scriptXmlChildMode { raised, lowered, kept };

StringBuffer &toScriptXML(IPropertyTree *tree, const StringArray &raise, const StringArray &lower, StringBuffer &xml, int indent);

StringBuffer &toScriptXMLNamedChildren(IPropertyTree *tree, const char *name, const StringArray &raise, const StringArray &lower, bool excludeNotKept, StringBuffer &xml, int indent)
{
    Owned<IPropertyTreeIterator> children = tree->getElements(name);
    ForEach(*children)
    {
        IPropertyTree &child = children->query();
        const char *name = child.queryName();
        if (!excludeNotKept || (!raise.contains(name) && !lower.contains(name)))
            toScriptXML(&child, raise, lower, xml, indent + 2);
    }
    return xml;
}

StringBuffer &toScriptXMLChildren(IPropertyTree *tree, scriptXmlChildMode mode, const StringArray &raise, const StringArray &lower, StringBuffer &xml, int indent)
{
    switch (mode)
    {
    case scriptXmlChildMode::kept:
        return toScriptXMLNamedChildren(tree, "*", raise, lower, true, xml, indent);
    case scriptXmlChildMode::raised:
    {
        ForEachItemIn(i, raise)
            toScriptXMLNamedChildren(tree, raise.item(i), raise, lower, false, xml, indent);
        return xml;
    }
    case scriptXmlChildMode::lowered:
    {
        ForEachItemIn(i, lower)
            toScriptXMLNamedChildren(tree, lower.item(i), raise, lower, false, xml, indent);
        return xml;
    }
    default:
        return xml;
    }
}

StringBuffer &toScriptXML(IPropertyTree *tree, const StringArray &raise, const StringArray &lower, StringBuffer &xml, int indent)
{
    const char *name = tree->queryName();
    if (!name)
        name = "__unnamed__";

    appendXMLOpenTag(xml.pad(indent), name, nullptr, false);

    Owned<IAttributeIterator> it = tree->getAttributes(true);
    ForEach(*it)
        appendXMLAttr(xml, it->queryName()+1, it->queryValue(), nullptr, true);

    if (!tree->hasChildren())
        return xml.append("/>\n");

    xml.append(">\n");

    toScriptXMLChildren(tree, scriptXmlChildMode::raised, raise, lower, xml, indent);
    toScriptXMLChildren(tree, scriptXmlChildMode::kept, raise, lower, xml, indent);
    toScriptXMLChildren(tree, scriptXmlChildMode::lowered, raise, lower, xml, indent);

    return appendXMLCloseTag(xml.pad(indent), name).append("\n");
}

//Need to fix up serialized PTREE order for backward compatability of scripts

StringBuffer &toScriptXML(IPropertyTree *tree, StringBuffer &xml, int indent)
{
    StringArray raise;
    raise.append("xsdl:param");
    raise.append("xsdl:variable");

    StringArray lower;
    lower.append("xsdl:otherwise");

    return toScriptXML(tree, raise, lower, xml, indent);

}

StringBuffer &buildScriptXml(IPropertyTree *cfgParent, StringBuffer &xml)
{
    if (!cfgParent)
        return xml;
    appendXMLOpenTag(xml, "Scripts", nullptr, false);
    appendXMLAttr(xml, "xmlns:xsdl", "urn:hpcc:esdl:script", nullptr, true);
    appendXMLAttr(xml, "xmlns:es", "urn:hpcc:esdl:script", nullptr, true);
    xml.append('>');
    IPropertyTree *crt = cfgParent->queryPropTree("xsdl:CustomRequestTransform");
    if (crt)
        toScriptXML(crt, xml, 2);
    IPropertyTree *transforms = cfgParent->queryPropTree("Transforms");
    if (transforms)
        toScriptXML(transforms, xml, 2);
    appendXMLCloseTag(xml, "Scripts");
    xml.replaceString("&apos;", "'");
    return xml;
}

void EsdlServiceImpl::addTransforms(IPropertyTree *cfgParent, const char *service, const char *method, bool removeCfgEntries)
{
    try
    {
        StringBuffer xml;
        const char *scriptXml = nullptr;
        if (cfgParent->hasProp("Scripts"))
            scriptXml = cfgParent->queryProp("Scripts");
        else
            scriptXml = buildScriptXml(cfgParent, xml).str();
        if (!isEmptyString(scriptXml))
            m_transforms->addMethodTransforms(method ? method : "", scriptXml, nonLegacyTransforms);
        if (removeCfgEntries)
        {
            cfgParent->removeProp("Scripts");
            cfgParent->removeProp("Transforms");
            cfgParent->removeProp("xsdl:CustomRequestTransform");
        }
    }
    catch (IException *e)
    {
        handleTransformError(m_serviceScriptError, m_methodScriptErrors, e, service, method);
        e->Release();
    }
    catch (...)
    {
        handleTransformError(m_serviceScriptError, m_methodScriptErrors, nullptr, service, method);
    }
}

void EsdlServiceImpl::configureTargets(IPropertyTree *cfg, const char *service)
{
    VStringBuffer xpath("Definition[@esdlservice='%s']", service);
    IPropertyTree *definition_cfg = cfg->queryPropTree(xpath);
    if (nullptr == definition_cfg)
    {
        DBGLOG("ESDL Binding: While configuring method targets: service definition not found for %s", service);
        return;
    }

    IEsdlDefService* svcDef = m_esdl->queryService(service);
    if (!svcDef)
        throw MakeStringException(-1, "ESDL binding - service '%s' not in definition!", service);

    // The static configuration defines a default namespace generator.
    // The ESDL definition may override the default with an explicit pattern.
    // The ESDL binding may override the ESDL definition with an explicit pattern.
    // The ESDL binding may override the ESDL definition by reverting to the default generator.
    bool bindingNamespaces = false;
    bool definitionNamespaces = false;
    Owned<String> serviceNS;
    const char* ns = nullptr;
    m_explicitNamespaces.kill();
    if (!isEmptyString(ns = definition_cfg->queryProp("@namespace")))
    {
        if (!strieq(ns, "default"))
        {
            serviceNS.setown(new String(ns));
            m_explicitNamespaces.setValue("", serviceNS.getLink());
            bindingNamespaces = true;
        }
    }
    else if (!isEmptyString(ns = svcDef->queryServiceNamespace()))
    {
        serviceNS.setown(new String(ns));
        m_explicitNamespaces.setValue("", serviceNS.getLink());
        definitionNamespaces = true;
    }

    IPropertyTree *target_cfg = definition_cfg->queryPropTree("Methods");
    DBGLOG("ESDL Binding: configuring method targets for esdl service %s", service);

    if (target_cfg)
    {
        Owned<String> methodsNS;
        if (bindingNamespaces && !isEmptyString(ns = target_cfg->queryProp("@namespace")))
            methodsNS.setown(new String(ns));
        else if (definitionNamespaces && !isEmptyString(ns = svcDef->queryMethodsNamespace()))
            methodsNS.setown(new String(ns));

        addTransforms(target_cfg, service, nullptr, true);

        m_pServiceMethodTargets.setown(createPTree(ipt_caseInsensitive));
        Owned<IPropertyTreeIterator> itns = target_cfg->getElements("Method");

        ForEach(*itns)
            m_pServiceMethodTargets->addPropTree("Target", createPTreeFromIPT(&itns->query()));

        StringBuffer classPath;
        const IProperties &envConf = queryEnvironmentConf();
        if (envConf.hasProp("classpath"))
            envConf.getProp("classpath", classPath);
        else
            classPath.append(hpccBuildInfo.installDir).append(PATHSEPCHAR).append("classes");

        MapStringToMyClass<IEspPlugin> localCppPluginMap;

        const char*      svcAuthDef  = svcDef->queryProp(ANNOTATION_AUTH_FEATURE);
        const char*      svcAuthBnd  = definition_cfg->queryProp("@" ANNOTATION_AUTH_FEATURE);
        m_methodAccessMaps.kill();

        m_returnSchemaLocationOnOK = definition_cfg->getPropBool("@" ANNOTATION_RETURNSCHEMALOCATION_ONOK);

        Owned<IPropertyTreeIterator> iter = m_pServiceMethodTargets->getElements("Target");
        ForEach(*iter)
        {
            IPropertyTree &methodCfg = iter->query();
            const char *method = methodCfg.queryProp("@name");
            if (!method || !*method)
                throw MakeStringException(-1, "ESDL binding - found %s target method entry without name!", service);
            IEsdlDefMethod* mthDef = svcDef->queryMethodByName(method);
            if (!mthDef)
                throw MakeStringException(-1, "ESDL binding - found %s target method entry '%s' not in definition!", service, method);

            StringBuffer key(method);
            key.toLowerCase();
            if ((bindingNamespaces && !isEmptyString(ns = methodCfg.queryProp("@namespace"))) || (definitionNamespaces && !isEmptyString(ns = mthDef->queryMethodNamespace())))
                m_explicitNamespaces.setValue(key, new String(ns));
            else if (methodsNS)
                m_explicitNamespaces.setValue(key, methodsNS.getLink());
            else
                m_explicitNamespaces.setValue(key, serviceNS.getLink());

            Owned<MethodAccessMap> authMap(new MethodAccessMap());
            EsdlAccessMapReporter  reporter(*authMap.get(), g_reporterHelper.getLink());
            EsdlAccessMapGenerator generator(g_scopeMapper, g_levelMapper, reporter);
            generator.setVariable(Var_Service, service);
            generator.setVariable(Var_Method, method);
            generator.insertScope(Scope_EsdlService, svcAuthDef);
            generator.insertScope(Scope_EsdlMethod, mthDef->queryMetaData(ANNOTATION_AUTH_FEATURE));
            generator.insertScope(Scope_BindingService, svcAuthBnd);
            generator.insertScope(Scope_BindingMethod, methodCfg.queryProp("@" ANNOTATION_AUTH_FEATURE));
            if (!m_defaultFeatureAuth.isEmpty())
                generator.setDefaultSecurity(m_defaultFeatureAuth);
            if (generator.generateMap())
                m_methodAccessMaps.setValue(mthDef->queryMethodName(), authMap.getLink());

            m_methodScriptErrors.remove(method);
            m_transforms->removeMethod(method);

            addTransforms(&methodCfg, service, method, true);

            const char *type = methodCfg.queryProp("@querytype");
            if (type && strieq(type, "java"))
                configureJavaMethod(method, methodCfg, classPath);
            else if (type && strieq(type, "cpp"))
            {
                const char* pluginName = methodCfg.queryProp("@plugin");
                if (!pluginName || !*pluginName)
                    DBGLOG("C++ plugin name is missing for method %s", method);
                else
                {
                    IEspPlugin* plugin = localCppPluginMap.getValue(pluginName);
                    bool loaded = (plugin != nullptr);
                    configureCppMethod(method, methodCfg, plugin);
                    if (!loaded && plugin != nullptr)
                        localCppPluginMap.setValue(pluginName, plugin);
                }
            }
            else if (type && strieq(type, "script"))
                DBGLOG("Purely scripted service method %s", method);
            else
                configureUrlMethod(method, methodCfg);
            DBGLOG("Method %s configured", method);
        }
        m_transforms->bindFunctionCalls();
    }
    else
        DBGLOG("ESDL Binding: While configuring method targets: method configuration not found for %s.", service);
}

void EsdlServiceImpl::configureLogging(IPropertyTree* cfg)
{
    loadLoggingManager(m_oDynamicLoggingManager, cfg);
}

void EsdlServiceImpl::configureMasking(IPropertyTree* cfg)
{
    m_oDynamicMaskingEngine.clear();
    if (!initMaskingEngineEmbedded(m_oDynamicMaskingEngine, cfg, false))
        throw makeStringException(-1, "failure loading masking configuration");
}

String* EsdlServiceImpl::getExplicitNamespace(const char* method) const
{
    StringBuffer tmp(method);
    Owned<String>* ns = m_explicitNamespaces.getValue(tmp.toLowerCase());
    return (ns ? ns->getLink() : nullptr);
}

#define ROXIEREQ_FLAGS (ESDL_TRANS_START_AT_ROOT | ESDL_TRANS_ROW_OUT | ESDL_TRANS_TRIM | ESDL_TRANS_OUTPUT_XMLTAG)
#define ESDLREQ_FLAGS (ESDL_TRANS_START_AT_ROOT | ESDL_TRANS_TRIM | ESDL_TRANS_OUTPUT_XMLTAG)
#define ESDLDEP_FLAGS (DEPFLAG_COLLAPSE | DEPFLAG_ARRAYOF)

enum EsdlMethodImplType
{
    EsdlMethodImplUnknown,
    EsdlMethodImplRoxie,
    EsdlMethodImplWsEcl,
    EsdlMethodImplProxy,
    EsdlMethodImplJava,
    EsdlMethodImplCpp,
    EsdlMethodImplScript
};

inline EsdlMethodImplType getEsdlMethodImplType(const char *querytype)
{
    if (querytype)
    {
        if (strieq(querytype, "roxie"))
            return EsdlMethodImplRoxie;
        if (strieq(querytype, "wsecl"))
            return EsdlMethodImplWsEcl;
        if (strieq(querytype, "proxy"))
            return EsdlMethodImplProxy;
        if (strieq(querytype, "java"))
            return EsdlMethodImplJava;
        if (strieq(querytype, "cpp"))
            return EsdlMethodImplCpp;
        if (strieq(querytype, "script"))
            return EsdlMethodImplScript;
    }
    return EsdlMethodImplRoxie;
}

static inline bool isPublishedQuery(EsdlMethodImplType implType)
{
    return (implType==EsdlMethodImplRoxie || implType==EsdlMethodImplWsEcl);
}

IEsdlScriptContext* EsdlServiceImpl::checkCreateEsdlServiceScriptContext(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *originalRequest)
{
    IEsdlTransformEntryPointMap *serviceEPm = m_transforms->queryMethod("");
    IEsdlTransformEntryPointMap *methodEPm = m_transforms->queryMethod(mthdef.queryMethodName());
    if (!serviceEPm && !methodEPm)
        return nullptr;

    Owned<IEsdlScriptContext> scriptContext = createEsdlScriptContext(&context, m_transforms->queryFunctionRegister(mthdef.queryMethodName()), LINK(maskingEngine()));

    scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "service", srvdef.queryName());
    scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "method", mthdef.queryMethodName());
    scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request_type", mthdef.queryRequestType());
    scriptContext->setAttribute(ESDLScriptCtxSection_ESDLInfo, "request", mthdef.queryRequestType());  //this could diverge from request_type in the future

    if (originalRequest)
        scriptContext->setContent(ESDLScriptCtxSection_OriginalRequest, originalRequest);
    if (tgtcfg)
        scriptContext->setContent(ESDLScriptCtxSection_TargetConfig, tgtcfg);
    if (m_oEspBindingCfg)
        scriptContext->setContent(ESDLScriptCtxSection_BindingConfig, m_oEspBindingCfg.get());

    scriptContext->enableMasking(nullptr, 0);
    return scriptContext.getClear();
}

void EsdlServiceImpl::runPostEsdlScript(IEspContext &context,
                                         IEsdlScriptContext *scriptContext,
                                         IEsdlDefService &srvdef,
                                         IEsdlDefMethod &mthdef,
                                         StringBuffer &content,
                                         unsigned txResultFlags,
                                         const char *ns,
                                         const char *schema_location)
{
    if (scriptContext)
    {
        IEsdlTransformSet *serviceIRTs = m_transforms->queryMethodEntryPoint("", ESDLScriptEntryPoint_InitialEsdlResponse);
        IEsdlTransformSet *methodIRTs = m_transforms->queryMethodEntryPoint(mthdef.queryName(), ESDLScriptEntryPoint_InitialEsdlResponse);

        if (serviceIRTs || methodIRTs)
        {
            scriptContext->setContent(ESDLScriptCtxSection_InitialESDLResponse, content.str());

            context.addTraceSummaryTimeStamp(LogNormal, "srt-modifytrans");
            processServiceAndMethodTransforms(scriptContext, {serviceIRTs, methodIRTs}, ESDLScriptCtxSection_InitialESDLResponse, ESDLScriptCtxSection_ModifiedESDLResponse);
            scriptContext->toXML(content.clear(), ESDLScriptCtxSection_ModifiedESDLResponse);
            context.addTraceSummaryTimeStamp(LogNormal, "end-modifytrans");

            //have to make a second ESDL pass (this stage is always a basic response format, never an HPCC/Roxie format)
            StringBuffer out;
            m_pEsdlTransformer->process(context, EsdlResponseMode, srvdef.queryName(), mthdef.queryName(), out, content.str(), txResultFlags, ns, schema_location);
            content.swapWith(out);
        }
    }

}

void EsdlServiceImpl::runServiceScript(IEspContext &context,
                                         IEsdlScriptContext *scriptContext,
                                         IEsdlDefService &srvdef,
                                         IEsdlDefMethod &mthdef,
                                         const char *reqcontent,
                                         StringBuffer &respcontent,
                                         unsigned txResultFlags,
                                         const char *ns,
                                         const char *schema_location)
{
    if (scriptContext)
    {
        IEsdlTransformSet *serviceSTs = m_transforms->queryMethodEntryPoint("", ESDLScriptEntryPoint_ScriptedService);
        IEsdlTransformSet *methodSTs = m_transforms->queryMethodEntryPoint(mthdef.queryName(), ESDLScriptEntryPoint_ScriptedService);

        if (serviceSTs || methodSTs)
        {
            scriptContext->setContent(ESDLScriptCtxSection_ScriptRequest, reqcontent);

            context.addTraceSummaryTimeStamp(LogNormal, "srt-script-service");
            VStringBuffer emptyResponse("<%s/>", mthdef.queryResponseType());
            scriptContext->setContent(ESDLScriptCtxSection_ScriptResponse, emptyResponse.str());
            processServiceAndMethodTransforms(scriptContext, {serviceSTs, methodSTs}, ESDLScriptCtxSection_ScriptRequest, ESDLScriptCtxSection_ScriptResponse);
            scriptContext->toXML(respcontent.clear(), ESDLScriptCtxSection_ScriptResponse);
            context.addTraceSummaryTimeStamp(LogNormal, "end-script-service");
        }
    }

}

void EsdlServiceImpl::handleServiceRequest(IEspContext &context,
                                           Owned<IEsdlScriptContext> &scriptContext,
                                           IEsdlDefService &srvdef,
                                           IEsdlDefMethod &mthdef,
                                           Owned<IPropertyTree> &tgtcfg,
                                           Owned<IPropertyTree> &tgtctx,
                                           const char *ns,
                                           const char *schema_location,
                                           IPropertyTree *req,
                                           StringBuffer &out,
                                           StringBuffer &logdata,
                                           StringBuffer &origResp,
                                           StringBuffer &soapmsg,
                                           unsigned int flags)
{
    const char *mthName = mthdef.queryName();
    context.addTraceSummaryValue(LogMin, "method", mthName, TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
    const char* srvName = srvdef.queryName();

    if (m_serviceScriptError.length()) //checked further along in shared code, but might as well avoid extra overhead
    {
        VStringBuffer msg("%s::%s disabled due to ESDL Script error(s). [%s]. Review transform template in configuration.", srvdef.queryName(), mthName, m_serviceScriptError.str());
        throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "%s", msg.str());

    }

    Owned<MethodAccessMap>* authMap = m_methodAccessMaps.getValue(mthdef.queryMethodName());
    if (authMap != nullptr && authMap->get() != nullptr)
    {
        MethodAccessMap& methaccessmap = *authMap->get();
        if (methaccessmap.ordinality() > 0 && !context.validateFeaturesAccess(methaccessmap, false))
        {
            StringBuffer features;
            HashIterator iter(methaccessmap);
            int index = 0;
            ForEach(iter)
            {
                IMapping &cur = iter.query();
                const char * key = (const char *)cur.getKey();
                features.appendf("%s%s:%s", (index++ == 0 ? "" : ", "), key, getSecAccessFlagName(*methaccessmap.getValue(key)));
            }
            const char * user = context.queryUserId();
            throw MakeStringException(401, "Insufficient priviledge to run function (%s) access denied for user (%s) - %s", mthName, (user && *user) ? user : "Anonymous", features.str());
        }
    }

    StringBuffer trxid;
    if (!m_bGenerateLocalTrxId)
    {
        if (loggingManager())
        {
            context.addTraceSummaryTimeStamp(LogNormal, "srt-trxid");
            StringBuffer wsaddress;
            short int port;
            context.getServAddress(wsaddress, port);
            VStringBuffer uniqueId("%s:%d-%u", wsaddress.str(), port, (unsigned) (memsize_t) GetCurrentThreadId());

            StringAttrMapping trxidbasics;
            StringBuffer creationTime;
            creationTime.setf("%u", context.queryCreationTime());

            trxidbasics.setValue(sTransactionDateTime, creationTime.str());
            trxidbasics.setValue(sTransactionMethod, mthName);
            trxidbasics.setValue(sTransactionIdentifier, uniqueId.str());

            StringBuffer trxidstatus;
            if (!loggingManager()->getTransactionID(&trxidbasics,trxid, trxidstatus))
                ESPLOG(LogMin,"DESDL: Logging Agent generated Transaction ID failed: %s", trxidstatus.str());
            context.addTraceSummaryTimeStamp(LogNormal, "end-trxid");
        }
        else
            ESPLOG(LogMin,"DESDL: Transaction ID could not be fetched from logging manager!");
    }

    if (!trxid.length())                       //either there's no logging agent providing trxid, or it failed to generate an id
        generateTransactionId(context, trxid); //in that case, the failure is logged and we generate a local trxid

    if (trxid.length())
        context.setTransactionID(trxid.str());
    else
        ESPLOG(LogMin,"DESDL: Transaction ID could not be generated!");

    // In the future we could instantiate a profile from the service configuration.
    // For now use a profile created on service initialization.
    CTxSummary* txSummary = context.queryTxSummary();
    if(txSummary)
        txSummary->setProfile(m_txSummaryProfile);

    EsdlMethodImplType implType = EsdlMethodImplUnknown;

    if(stricmp(mthName, "echotest")==0 || mthdef.hasProp("EchoTest"))
    {
        handleEchoTest(mthdef.queryName(),req,out,context.getResponseFormat());
        return;
    }
    else if
    (stricmp(mthName, "ping")==0 || mthdef.hasProp("Ping"))
    {
        handlePingRequest(srvName, out, flags);
        return;
    }
    else
    {
        if (!m_pServiceMethodTargets)
            throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "Service methods not configured!");

        VStringBuffer xpath("Target[@name=\"%s\"]", mthName);
        tgtcfg.setown(m_pServiceMethodTargets->getPropTree(xpath.str()));

        if (!tgtcfg)
            throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESDL", "Target not configured for method: %s", mthName );

        StringAttr *crtErrorMessage = m_methodScriptErrors.getValue(mthName);
        if (crtErrorMessage && !crtErrorMessage->isEmpty())
        {
            VStringBuffer msg("%s::%s disabled due to ESDL Script error(s). [%s]. Review transform template in configuration.", srvName, mthName, crtErrorMessage->str());
            throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "%s", msg.str());
        }

        implType = getEsdlMethodImplType(tgtcfg->queryProp("@querytype"));

        bool use_numeric_bool = srvdef.getPropBool("numeric_bool");
        unsigned txResultFlags = ESDL_TRANS_OUTPUT_ROOT;
        if (use_numeric_bool)
            txResultFlags |= ESDL_TRANS_NUMERIC_BOOLEAN;

        if (implType==EsdlMethodImplJava)
        {
            const char *javaPackage = srvdef.queryName();
            const char *javaScopedClass = tgtcfg->queryProp("@javaclass");
            const char *javaScopedMethod = tgtcfg->queryProp("@javamethod");

            Linked<IEmbedServiceContext> srvctx = javaServiceMap.getValue(javaScopedClass);
            if (!srvctx)
            {
                StringBuffer errmsg;
                errmsg.appendf("Java class %s not loaded for method %s", javaScopedClass, mthName);
                Linked<IException> exception = javaExceptionMap.getValue(javaScopedClass);
                if(exception)
                {
                    errmsg.append(". Cause: ");
                    exception->errorMessage(errmsg);
                }
                throw makeWsException(ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, "ESDL", "%s", errmsg.str());
            }

            // Note that at present all methods called are assumed to be non-static (this was all that was supported prior to 7.2.0)
            // We could extend edsl with a flag to indicate a static method (and remove the '@' in the signature below if it was set)
            // Or we could remove the entire signature (the : and following in the line below) and have the plugin deduce it automatically - this does
            // have a small performance penalty though.

            //"WsWorkunits.WsWorkunitsService.WUAbort:@(LWsWorkunits/EsdlContext;LWsWorkunits/WUAbortRequest;)LWsWorkunits/WUAbortResponse;";
            VStringBuffer signature("%s:@(L%s/EsdlContext;L%s/%s;)L%s/%s;", javaScopedMethod, javaPackage, javaPackage, mthdef.queryRequestType(), javaPackage, mthdef.queryResponseType());

            Owned<IEmbedFunctionContext> javactx;
            javactx.setown(srvctx->createFunctionContext(signature));
            if (!javactx)
                throw makeWsException(ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, "ESDL", "Java method %s could not be loaded from class %s in esdl method %s", tgtcfg->queryProp("@javamethod"), javaScopedClass, mthName);

            Owned<IXmlWriterExt> writer = dynamic_cast<IXmlWriterExt *>(javactx->bindParamWriter(m_esdl, javaPackage, "EsdlContext", "context"));
             if (writer)
             {
                if (context.queryUserId())
                    writer->outputCString(context.queryUserId(), "username");
                javactx->paramWriterCommit(writer);
             }

             writer.setown(dynamic_cast<IXmlWriterExt *>(javactx->bindParamWriter(m_esdl, javaPackage, mthdef.queryRequestType(), "request")));
             context.addTraceSummaryTimeStamp(LogNormal, "srt-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
             m_pEsdlTransformer->process(context, EsdlRequestMode, srvdef.queryName(), mthdef.queryName(), *req, writer, 0, NULL);
             context.addTraceSummaryTimeStamp(LogNormal, "end-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

             javactx->paramWriterCommit(writer);
             javactx->callFunction();

             Owned<IXmlWriterExt> javaRespWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);
             javactx->writeResult(m_esdl, srvdef.queryName(), mthdef.queryResponseType(), javaRespWriter);
             origResp.set(javaRespWriter->str());

             Owned<IXmlWriterExt> finalRespWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
             m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), finalRespWriter, logdata, txResultFlags, ns, schema_location);
             // TODO: Modify processHPCCResult to return record count
             out.append(finalRespWriter->str());
        }
        else if (implType==EsdlMethodImplCpp)
        {
            Owned<IXmlWriterExt> reqWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);

            //Preprocess Request
            StringBuffer reqcontent;
            unsigned xflags = (isPublishedQuery(implType)) ? ROXIEREQ_FLAGS : ESDLREQ_FLAGS;
            context.addTraceSummaryTimeStamp(LogNormal, "srt-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
            m_pEsdlTransformer->process(context, EsdlRequestMode, srvdef.queryName(), mthdef.queryName(), *req, reqWriter.get(), xflags, NULL);
            context.addTraceSummaryTimeStamp(LogNormal, "end-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

            reqcontent.set(reqWriter->str());
            context.addTraceSummaryTimeStamp(LogNormal, "serialized-xmlreq");

            cpp_service_method_t xproc = nullptr;
            cpp_service_method_t* xprocp = cppProcMap.getValue(mthName);
            if (xprocp)
                xproc = *xprocp;
            if (!xproc)
                throw makeWsException(ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, "ESDL", "C++ plugin or method not loaded for method %s", mthName);

            StringBuffer ctxbuf;
            ctxbuf.append("<EsdlContext>");
            if (context.queryUserId())
                ctxbuf.appendf("<username>%s</username>", context.queryUserId());
            ctxbuf.append("</EsdlContext>");

            context.addTraceSummaryTimeStamp(LogNormal, "srt-cppcall");
            xproc(ctxbuf.str(), reqcontent.str(), origResp);
            context.addTraceSummaryTimeStamp(LogNormal, "end-cppcall");

            context.addTraceSummaryTimeStamp(LogNormal, "srt-procres", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
            Owned<IXmlWriterExt> finalRespWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
            m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), finalRespWriter, logdata, txResultFlags, ns, schema_location);
            // TODO: Modify processHPCCResult to return record count
            context.addTraceSummaryTimeStamp(LogNormal, "end-procres", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

            out.append(finalRespWriter->str());
        }
        else
        {
            //Future: support transforms for all transaction types by moving scripts and script processing up
            scriptContext.setown(checkCreateEsdlServiceScriptContext(context, srvdef, mthdef, tgtcfg, req));

            Owned<IXmlWriterExt> reqWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);

            //Preprocess Request
            StringBuffer reqcontent;
            unsigned xflags = (isPublishedQuery(implType)) ? ROXIEREQ_FLAGS : ESDLREQ_FLAGS;
            context.addTraceSummaryTimeStamp(LogNormal, "srt-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
            int recordCount = m_pEsdlTransformer->process(context, EsdlRequestMode, srvdef.queryName(), mthdef.queryName(), *req, reqWriter.get(), xflags, NULL);
            context.addTraceSummaryTimeStamp(LogNormal, "end-reqproc", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
            context.addTraceSummaryValue(LogNormal, "custom_fields.recCount", recordCount, TXSUMMARY_GRP_ENTERPRISE);

            if(isPublishedQuery(implType) || implType==EsdlMethodImplScript)
                tgtctx.setown(createTargetContext(context, tgtcfg.get(), srvdef, mthdef, req));

            reqcontent.set(reqWriter->str());
            context.addTraceSummaryTimeStamp(LogNormal, "serialized-xmlreq");

            if (implType==EsdlMethodImplScript)
                runServiceScript(context, scriptContext, srvdef, mthdef, reqcontent, origResp, txResultFlags, ns, schema_location);
            else
                handleFinalRequest(context, scriptContext, tgtcfg, tgtctx, srvdef, mthdef, ns, reqcontent, origResp, isPublishedQuery(implType), implType==EsdlMethodImplProxy, soapmsg);
            context.addTraceSummaryTimeStamp(LogNormal, "end-HFReq", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

            if (isPublishedQuery(implType))
            {
                context.addTraceSummaryTimeStamp(LogNormal, "srt-procres", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
                Owned<IXmlWriterExt> respWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
                m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), respWriter.get(), logdata, txResultFlags, ns, schema_location);
                context.addTraceSummaryTimeStamp(LogNormal, "end-procres", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
                out.append(respWriter->str());
                runPostEsdlScript(context, scriptContext, srvdef, mthdef, out, txResultFlags, ns, schema_location);
            }
            else if(implType==EsdlMethodImplProxy)
                getSoapBody(out, origResp);
            else
            {
                m_pEsdlTransformer->process(context, EsdlResponseMode, srvdef.queryName(), mthdef.queryName(), out, origResp.str(), txResultFlags, ns, schema_location);
                runPostEsdlScript(context, scriptContext, srvdef, mthdef, out, txResultFlags, ns, schema_location);
                logdata.set("<LogDatasets/>");
            }
        }
    }

    ESPLOG(LogMax,"Customer Response: %s", out.str());
}

bool EsdlServiceImpl::handleResultLogging(IEspContext &espcontext, IEsdlScriptContext *scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree * reqcontext, IPropertyTree * request,const char *rawreq, const char * rawresp, const char * finalresp, const char * logdata)
{
    StringBuffer temp;
    if (scriptContext)
    {
        espcontext.addTraceSummaryTimeStamp(LogNormal, "custom_fields.srt-preLogScripts", TXSUMMARY_GRP_ENTERPRISE);
        IEsdlTransformSet *servicePLTs = m_transforms->queryMethodEntryPoint("", ESDLScriptEntryPoint_PreLogging);
        IEsdlTransformSet *methodPLTs = m_transforms->queryMethodEntryPoint(mthdef.queryName(), ESDLScriptEntryPoint_PreLogging);

        scriptContext->appendContent(ESDLScriptCtxSection_LogData, "LogDatasets", logdata);

        if (servicePLTs || methodPLTs)
        {
            processServiceAndMethodTransforms(scriptContext, {servicePLTs, methodPLTs}, ESDLScriptCtxSection_LogData, nullptr);
            scriptContext->toXML(temp, ESDLScriptCtxSection_LogData);
            logdata = temp.str();
        }
        espcontext.addTraceSummaryTimeStamp(LogNormal, "custom_fields.end-preLogScripts", TXSUMMARY_GRP_ENTERPRISE);
    }

    bool success = true;
    espcontext.addTraceSummaryTimeStamp(LogNormal, "srt-resLogging", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
    if (loggingManager())
    {
        Owned<IEspLogEntry> entry = loggingManager()->createLogEntry();
        entry->setOption(LOGGINGDBSINGLEINSERT);
        entry->setOwnEspContext(LINK(&espcontext));
        entry->setOwnUserContextTree(LINK(reqcontext));
        entry->setOwnUserRequestTree(LINK(request));
        entry->setUserResp(finalresp);
        entry->setBackEndReq(rawreq);
        entry->setBackEndResp(rawresp);
        entry->setLogDatasets(logdata);
        if (scriptContext)
            entry->setOwnScriptValuesTree(scriptContext->createPTreeFromSection(ESDLScriptCtxSection_Logging));
        StringBuffer logresp;
        success = loggingManager()->updateLog(entry, logresp);
        ESPLOG(LogMin,"ESDLService: Attempted to log ESP transaction: %s", logresp.str());
    }
    espcontext.addTraceSummaryTimeStamp(LogNormal, "end-resLogging", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
    return success;
}

void EsdlServiceImpl::getSoapBody(StringBuffer& out,StringBuffer& soapresp)
{
    int len = soapresp.length();
    if(len<=0)
        return;

    const char* begin = soapresp.str();
    const char* end = begin+len;
    const char* finger = begin;

    //find the envelope
    while(finger && finger<=end-9 && strnicmp(finger,":envelope",9)!=0)
        finger++;

    //no envelope so return as is
    if(finger==end-9)
    {
        out.append(soapresp.str());
        return;
    }

    //find the body
    while(finger && finger<=end-5 && strnicmp(finger,":body",5)!=0)
        finger++;

    //if for some reason invalid soap, return
    if(finger==end-5)
    {
        ESPLOG(LogMax,"EsdlServiceImpl - Invalid Soap Response: %s",soapresp.str());
        return;
    }

    //find the end bracket incase there is namespace
    while(finger && *finger!='>')
        finger++;

    const char* start=finger+1;

    //now find the end of the body
    finger = soapresp.str() + soapresp.length() - 6;
    while(finger&& finger>=start && strnicmp(finger,":body>",6)!=0)
        finger--;

    if(finger==start)
    {
        ESPLOG(LogMax,"EsdlServiceImpl - Invalid Soap Response: %s",soapresp.str());
        return;
    }

    while(finger>start && *finger!='<')
        finger--;

    out.clear().append(finger-start,start);
}

void EsdlServiceImpl::getSoapError( StringBuffer& out,
                                    StringBuffer& soapresp,
                                    const char * starttxt,
                                    const char * endtxt)

{
    int len = soapresp.length();
    if(len<=0)
        return;

    const char* begin = soapresp.str();
    const char* end = begin+len;
    const char* finger = begin;

    const unsigned int envlen = 9;
    const unsigned int textlen = strlen(starttxt);
    const unsigned int textlenend = strlen(endtxt);

    //find the envelope
    while(finger && finger<=end-envlen && strnicmp(finger,":envelope",envlen)!=0)
        finger++;

    //no envelope so return as is
    if(finger==end-envlen)
    {
        out.append(soapresp.str());
        return;
    }

    //find the Reason
    while(finger && finger<end-textlen && strnicmp(finger,starttxt,textlen)!=0)        ++finger;

    //if for some reason invalid soap, return
    if(finger==end-textlen)
    {
        ESPLOG(LogMax,"EsdlServiceImpl - Could not find Reason, trying for other tags.: %s",soapresp.str());
        return;
    }

    //find the end bracket incase there is namespace
    while(finger && *finger!='>')        finger++;

    const char* start=finger+1;

    //now find the end of the Reason
    finger = soapresp.str() + soapresp.length() - textlenend;
    while(finger&& finger>=start && strnicmp(finger,endtxt,textlenend)!=0)
        finger--;

    if(finger==start)
    {
        ESPLOG(LogMax,"EsdlServiceImpl - Could not find Reason, trying for other tags.: %s",soapresp.str());
        return;
    }

    while(finger>start && *finger!='<')
        finger--;

    out.clear().append(finger-start,start);
}

void EsdlServiceImpl::handleFinalRequest(IEspContext &context,
                                         IEsdlScriptContext *scriptContext,
                                         Owned<IPropertyTree> &tgtcfg,
                                         Owned<IPropertyTree> &tgtctx,
                                         IEsdlDefService &srvdef,
                                         IEsdlDefMethod &mthdef,
                                         const char *ns,
                                         StringBuffer& req,
                                         StringBuffer &out,
                                         bool isroxie,
                                         bool isproxy,
                                         StringBuffer& soapmsg)
{
    const char *tgtUrl = tgtcfg->queryProp("@url");
    if (!isEmptyString(tgtUrl))
    {
        prepareFinalRequest(context, scriptContext, tgtcfg, tgtctx, srvdef, mthdef, isroxie, ns, req, soapmsg);
        sendTargetSOAP(context, tgtcfg.get(), soapmsg.str(), out, isproxy, NULL);
    }
    else
    {
        ESPLOG(LogMax,"No target URL configured for %s",mthdef.queryMethodName());
        throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP",
                   "No target URL configured for %s!", mthdef.queryMethodName());
    }

    auto echoInsertionPoint = strstr(out, "</Result></Results>");

    if (echoInsertionPoint)
    {
        try
        {
            const char *queryname = (isroxie ? tgtcfg->queryProp("@queryname") : nullptr);
            const char *requestname = mthdef.queryRequestType();
            const char *qname = nullptr;
            const char *content = nullptr;

            StringBuffer encodedContent;
            StringBuffer echoDataset;

            XmlPullParser xpp(soapmsg.str(), soapmsg.length());
            StartTag stag;
            EndTag etag;
            int type = XmlPullParser::END_DOCUMENT;

            echoDataset.ensureCapacity(soapmsg.length());
            xpp.setSupportNamespaces(false); // parser throws exceptions when it encounters undefined namespace prefixes, such as CRT's 'xsdl'
            while ((type = xpp.next()) != XmlPullParser::END_DOCUMENT)
            {
                switch (type)
                {
                case XmlPullParser::START_TAG:
                    xpp.readStartTag(stag);
                    qname = stag.getQName();
                    if (streq(qname, "soap:Envelope"))
                        echoDataset << "<Dataset name=\"DesdlSoapRequestEcho\">";
                    else if (streq(qname, "soap:Body"))
                        echoDataset << "<Row>";
                    else if (queryname && streq(qname, queryname))
                        echoDataset << "<roxierequest name=\"" << queryname << "\">";
                    else if (streq(qname, requestname))
                        echoDataset << "<esprequest name=\"" << requestname << "\">";
                    else
                    {
                        echoDataset << '<';
                        if (strncmp(qname, "xsdl:", 5) == 0)
                            echoDataset << "xsdl_" << qname + 5; // eliminate problematic namespace prefix
                        else
                            echoDataset << qname;
                        for (int idx = 0; idx < stag.getLength(); idx++)
                        {
                            encodeXML(stag.getValue(idx), encodedContent.clear());
                            echoDataset << ' ' << stag.getRawName(idx) << "=\"" << encodedContent << '"';
                        }
                        echoDataset << '>';
                    }
                    break;

                case XmlPullParser::END_TAG:
                    xpp.readEndTag(etag);
                    qname = etag.getQName();
                    if (streq(qname, "soap:Envelope"))
                        echoDataset << "</Dataset>";
                    else if (streq(qname, "soap:Body"))
                        echoDataset << "</Row>";
                    else if (queryname && streq(qname, queryname))
                        echoDataset << "</roxierequest>";
                    else if (streq(qname, requestname))
                        echoDataset << "</esprequest>";
                    else if (strncmp(qname, "xsdl:", 5) == 0)
                        echoDataset << "</xsdl_" << qname + 5 << '>'; // eliminate problematic namespace prefix
                    else
                        echoDataset << "</" << qname << '>';
                    break;

                case XmlPullParser::CONTENT:
                    content = xpp.readContent();
                    if (!isEmptyString(content))
                    {
                        encodeXML(content, encodedContent.clear());
                        echoDataset << encodedContent;
                    }
                    break;
                }
            }
            out.insert(echoInsertionPoint - out.str(), echoDataset);
        }
        catch (XmlPullParserException& xppe)
        {
            IERRLOG("Unable to echo transformed request to response: %s", xppe.what());
        }
        catch (...)
        {
            IERRLOG("Unable to echo transformed request to response");
        }
    }

    if (scriptContext)
    {
        IEsdlTransformSet *serviceIRTs = m_transforms->queryMethodEntryPoint("", ESDLScriptEntryPoint_BackendResponse);
        IEsdlTransformSet *methodIRTs = m_transforms->queryMethodEntryPoint(mthdef.queryName(), ESDLScriptEntryPoint_BackendResponse);

        context.addTraceSummaryTimeStamp(LogNormal, "srt-resptrans");

        scriptContext->setContent(ESDLScriptCtxSection_InitialResponse, out.str());
        if (serviceIRTs || methodIRTs)
        {
            processServiceAndMethodTransforms(scriptContext, {serviceIRTs, methodIRTs}, ESDLScriptCtxSection_InitialResponse, ESDLScriptCtxSection_PreESDLResponse);
            scriptContext->toXML(out.clear(), ESDLScriptCtxSection_PreESDLResponse);
        }

        context.addTraceSummaryTimeStamp(LogNormal, "end-resptrans");
    }
}

void EsdlServiceImpl::handleEchoTest(const char *mthName,
                                     IPropertyTree *req,
                                     StringBuffer &out,
                                     ESPSerializationFormat format)
{
    const char* valueIn = req->queryProp("ValueIn");
    StringBuffer encoded;
    if (format == ESPSerializationJSON)
    {
        encodeJSON(encoded, valueIn);
        out.appendf("{\n\t\"%sResponse\":\n{\t\t\"ValueOut\": \"%s\"\n\t\t}\n}", mthName, encoded.str());
    }
    else
    {
        encodeXML(valueIn, encoded);
        out.appendf("<%sResponse><ValueOut>%s</ValueOut></%sResponse>", mthName, encoded.str(), mthName);
    }
}

void EsdlServiceImpl::handlePingRequest(const char *srvName, StringBuffer &out, unsigned int flags)
{
    if (flags & ESDL_BINDING_RESPONSE_JSON)
        out.appendf("\"%sPingResponse\": {}", srvName);
    else
        out.appendf("<%sPingResponse></%sPingResponse>", srvName, srvName);
}

void EsdlServiceImpl::generateTargetURL(IEspContext & context,
                                     IPropertyTree *srvinfo,
                                     StringBuffer & url,
                                     bool isproxy
                                     )
{
    if (!srvinfo)
        throw MakeStringException(-1, "Could not generate target URL, invalid service info.");

    StringBuffer name(srvinfo->queryProp("@name"));

    ISmartSocketFactory *sconn = connMap.getValue(name);
    if (!sconn)
        throw MakeStringException(-1, "Could not create smartsocket.");

    url.set(srvinfo->queryProp("@prot"));
    if (url.length() <= 0)
        url.append("HTTP");
    url.append("://");

    sconn->getUrlStr(url, true);

    if(srvinfo->hasProp("@path"))  //Append the server path
    {
        const char * path = srvinfo->queryProp("@path");
        if (*path != '/')
            url.append("/");
        url.append(path);
    }

    if(isproxy)
    {
        const char* path = context.getContextPath();
        if(path&&*path)
            url.append(path);

        IProperties* params = context.queryRequestParameters();
        if(params)
        {
            const char* qparams = params->queryProp("__querystring");
            if(qparams && *qparams)
                url.appendf("?%s",qparams);
        }
    }
}


void EsdlServiceImpl::sendTargetSOAP(IEspContext & context,
                                     IPropertyTree *srvinfo,
                                     const char * req,
                                     StringBuffer &resp,
                                     bool isproxy,
                                     const char * targeturl)
{
    StringBuffer url;

    if (!srvinfo)
        throw MakeStringException(-1, "Empty method config detected.");

    if (!targeturl || !*targeturl)
        generateTargetURL(context, srvinfo,url, isproxy);
    else
        url.set(targeturl);

    if (url.length() <= 0)
        throw MakeStringException(-1, "Empty URL detected.");

    const char * querytype = srvinfo->queryProp("@querytype");
    const char * pw = srvinfo->queryProp("@password");
    const char * username = srvinfo->queryProp("@username");
    const char * pwc = srvinfo->queryProp("@pwc");
    unsigned int maxWaitSeconds = srvinfo->getPropInt("@maxWaitSeconds");

    Owned<IHttpClientContext> httpctx = getHttpClientContext();
    Owned <IHttpClient> httpclient = httpctx->createHttpClient(NULL, url.str());
    httpclient->setTimeOut(maxWaitSeconds);

    StringBuffer password;
    if (pwc)
        password.append(pwc);
    else if (pw)
        decrypt(password, pw);

    if (username && *username && password.length())
    {
        httpclient->setUserID(username);
        httpclient->setPassword(password.str());
    }

    Owned<IProperties> headers = createProperties();
    headers->setProp(HTTP_HEADER_HPCC_GLOBAL_ID, context.getGlobalId());
    headers->setProp(HTTP_HEADER_HPCC_CALLER_ID, context.getLocalId());
    StringBuffer status;
    StringBuffer clreq(req);

    ESPLOG(LogMax,"OUTGOING Request target: %s", url.str());
    ESPLOG(LogMax,"OUTGOING Request: %s", clreq.str());
    {
        EspTimeSection timing("Calling out to query");
        context.addTraceSummaryTimeStamp(LogMin, "startcall", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
        httpclient->sendRequest(headers, "POST", "text/xml", clreq, resp, status, true);
        context.addTraceSummaryTimeStamp(LogMin, "endcall", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
    }

    context.addTraceSummaryValue(LogMin, "custom_fields.ext_resp_len", resp.length(), TXSUMMARY_GRP_ENTERPRISE);

    if (status.length()==0)
    {
        UERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status not reported, response content: %s", resp.str());
        throw makeWsException( ERR_ESDL_BINDING_UNAVAIL, WSERR_CLIENT, "ESP", "Internal Server Unavailable");
    }
    else if (strncmp("500", status.str(), 3)==0)  //process internal service errors.
    {
        UERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status: %s", status.str());
        StringBuffer out;
        getSoapError(out,resp,":Text", ":Text>");

        if( out.length() == 0 )
            getSoapError(out,resp,"<Error>", "</Error>");  //Try Error

        if( out.length() == 0 )
            getSoapError(out,resp,"<faultstring>", "</faultstring>");  //Try Error

        if( out.length() > 0){
            throw makeWsException( 3403, WSERR_SERVER, "ESP", "SoapCallStatus:%s", out.str());
        }

        throw makeWsException( 3402, WSERR_SERVER, "ESP", "SoapCallStatus:%s", status.str());
    }
    else if (strncmp("200", status.str(), 3)!=0)
    {
        UERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status: %s", status.str());
        throw makeWsException( 3402, WSERR_SERVER, "ESP", "SoapCallStatus:%s", status.str());
    }

    ESPLOG(LogMax,"INCOMING Response: %s", resp.str());
    if (querytype!=NULL && strieq(querytype, "WSECL"))
    {
        StringBuffer decoded;
        decodeXML(resp.str(), decoded);
        ESPLOG(LogMax,"SOAP Decoded Response: %s", decoded.str());
        resp.swapWith(decoded);
    }
}

void EsdlServiceImpl::getTargetResponseFile(IEspContext & context,
                                            IPropertyTree *srvinfo,
                                            const char * req,
                                            StringBuffer &resp)
{
    const char * respfile = srvinfo->queryProp("@respfile");

    ESPLOG(LogMax,"Response file: %s", respfile);
    if (respfile)
        resp.loadFile(respfile);
}

/*
    Builds up the request string into the 'reqStr' buffer in a format
    suitable for submission to back-end service.
 */

void EsdlServiceImpl::prepareFinalRequest(IEspContext &context,
                                        IEsdlScriptContext *scriptContext,
                                        Owned<IPropertyTree> &tgtcfg,
                                        Owned<IPropertyTree> &tgtctx,
                                        IEsdlDefService &srvdef,
                                        IEsdlDefMethod &mthdef,
                                        bool isroxie,
                                        const char* ns,
                                        StringBuffer &reqcontent,
                                        StringBuffer &reqProcessed)
{
    const char *mthName = mthdef.queryName();

    reqProcessed.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">");

    if(isroxie)
    {
        const char *tgtQueryName =  tgtcfg->queryProp("@queryname");
        if (!isEmptyString(tgtQueryName))
        {
            reqProcessed.append("<soap:Body><").append(tgtQueryName).append(">");
            reqProcessed.appendf("<_TransactionId>%s</_TransactionId>", context.queryTransactionID());

            if (tgtctx)
                toXML(tgtctx.get(), reqProcessed);

            reqProcessed.append(reqcontent.str());
            reqProcessed.append("</").append(tgtQueryName).append("></soap:Body>");

            // Transform gateways
            auto cfgGateways = tgtcfg->queryBranch("Gateways");
            if (cfgGateways)
            {
                auto baseXpath = cfgGateways->queryProp("@legacyTransformTarget");
                if (!isEmptyString(baseXpath))
                {
                    Owned<IPTree> gws = createPTree("gateways", 0);
                    // Temporarily add the closing </soap:Envelope> tag so we have valid
                    // XML to transform the gateways
                    Owned<IPTree> soapTree = createPTreeFromXMLString(reqProcessed.append("</soap:Envelope>"), ipt_ordered);
                    StringBuffer xpath(baseXpath);
                    StringBuffer rowName(cfgGateways->queryProp("@legacyRowName"));

                    if (rowName.isEmpty())
                        rowName.append("row");
                    EsdlBindingImpl::transformGatewaysConfig(tgtcfg, gws, rowName);
                    xpath.replaceString("{$query}", tgtQueryName);
                    xpath.replaceString("{$method}", mthName);
                    xpath.replaceString("{$service}", srvdef.queryName());
                    xpath.replaceString("{$request}", mthdef.queryRequestType());
                    mergePTree(ensurePTree(soapTree, xpath), gws);
                    toXML(soapTree, reqProcessed.clear());
                    // Remove the </soap:Envelope> tag so it can be unconditionally added
                    // when the gateways don't require processing
                    reqProcessed.setLength(strstr(reqProcessed, "</soap:Envelope>") - reqProcessed.str());
                    reqProcessed.trim();
                }
            }
        }
        else
        {
            // Use WsException here because both callers throw this type of exception
            throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_SERVER, "ESP",
                        "EsdlServiceImpl::processFinalRequest() target query name missing");
        }
    }
    else
    {
        StringBuffer headers;
        processHeaders(context, srvdef, mthdef, ns, reqcontent, headers);
        if (headers.length() > 0 )
            reqProcessed.append("<soap:Header>").append(headers).append("</soap:Header>");

        processRequest(context, srvdef, mthdef, ns, reqcontent);
        reqProcessed.append("<soap:Body>").append(reqcontent).append("</soap:Body>");
    }

    reqProcessed.append("</soap:Envelope>");

    // Process Custom Request Transforms

    if (m_serviceScriptError.length())
        throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "%s::%s disabled due to service-level Custom Transform errors. [%s]. Review transform template in configuration.", srvdef.queryName(), mthName, m_serviceScriptError.str());

    StringAttr *crtErrorMessage = m_methodScriptErrors.getValue(mthName);
    if (crtErrorMessage && !crtErrorMessage->isEmpty())
        throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "%s::%s disabled due to ESDL Script error(s). [%s]. Review transform template in configuration.", srvdef.queryName(), mthName, crtErrorMessage->str());

    if (scriptContext)
    {
        IEsdlTransformSet *serviceBRTs = m_transforms->queryMethodEntryPoint("", ESDLScriptEntryPoint_BackendRequest);
        IEsdlTransformSet *methodBRTs = m_transforms->queryMethodEntryPoint(mthName, ESDLScriptEntryPoint_BackendRequest);

        context.addTraceSummaryTimeStamp(LogNormal, "srt-custreqtrans");
        scriptContext->setContent(ESDLScriptCtxSection_ESDLRequest, reqProcessed.str());
        if (serviceBRTs || methodBRTs)
        {
            processServiceAndMethodTransforms(scriptContext, {serviceBRTs, methodBRTs}, ESDLScriptCtxSection_ESDLRequest, ESDLScriptCtxSection_FinalRequest);
            scriptContext->toXML(reqProcessed.clear(), ESDLScriptCtxSection_FinalRequest);
        }

        context.addTraceSummaryTimeStamp(LogNormal, "end-custreqtrans");
    }
}

EsdlServiceImpl::~EsdlServiceImpl()
{
    for (auto& item : connMap)
    {
        ISmartSocketFactory* sf = static_cast<ISmartSocketFactory*>(item.getValue());
        if(sf)
            sf->stop();
    }
}

EsdlBindingImpl::EsdlBindingImpl()
{
        m_pESDLService = nullptr;
        m_isAttached = true;
}

EsdlBindingImpl::EsdlBindingImpl(IPropertyTree* cfg, IPropertyTree* esdlArchive, const char *binding,  const char *process) : CHttpSoapBinding(cfg, binding, process)
{
    m_pCentralStore.setown(getEsdlCentralStore(true));
    m_bindingName.set(binding);
    m_processName.set(process);

    m_pESDLService = nullptr;
    m_isAttached = true;

    try
    {
        char currentDirectory[_MAX_DIR];
        if (!getcwd(currentDirectory, sizeof(currentDirectory)))
            throw makeWsException( ERR_ESDL_BINDING_UNEXPECTED, WSERR_CLIENT,  "ESP", "getcwd failed");

        m_esdlStateFilesLocation.setf("%s%cDESDL_STATE",currentDirectory, PATHSEPCHAR);
        recursiveCreateDirectory(m_esdlStateFilesLocation.str());

        m_esdlStateFilesLocation.appendf("%c%s-%s.xml", PATHSEPCHAR, m_processName.str(), m_bindingName.str());
    }
    catch (...)
    {
        DBGLOG("Detected error creating ESDL State store: %s", m_esdlStateFilesLocation.str());
    }

    try
    {
        if (esdlArchive)
            m_esdlBndCfg.set(esdlArchive->queryPropTree("Binding"));
        else
            m_esdlBndCfg.setown(fetchESDLBinding(process, binding, m_esdlStateFilesLocation));

        if (!m_esdlBndCfg.get())
            DBGLOG("ESDL Binding: Could not fetch ESDL binding %s for ESP Process %s", binding, process);
#ifdef _DEBUG
        else
        {
            StringBuffer cfgtext;
            toXML(m_esdlBndCfg.get(), cfgtext);
            DBGLOG("ESDL binding configuration:\n%s", cfgtext.str());
        }
#endif

        if (m_esdlBndCfg.get())
        {
            m_bindingId.set(m_esdlBndCfg->queryProp("@id"));
            IEsdlMonitor* monitor = queryEsdlMonitor();
            monitor->registerBinding(m_bindingId.get(), this);
            DBGLOG("ESDL binding %s properly registered", m_bindingId.str());
       }

        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]", process, binding);
        m_bndCfg.set(cfg->queryPropTree(xpath.str()));

        if (!m_bndCfg)
            DBGLOG("ESDL Binding: Warning could not fetch ESDL bind information for this ESP Binding %s", binding);

        m_esdl.setown(createNewEsdlDefinition());
        m_xsdgen.setown(createEsdlDefinitionHelper());
    }
    catch(IException* e)
    {
       StringBuffer msg;
       e->errorMessage(msg);
       DBGLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: %s", msg.str());
    }
    catch (...)
    {
        DBGLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: Could Not load Binding %s information from Dali!", binding);
    }
}

IPropertyTree* EsdlBindingImpl::fetchESDLBinding(const char *process, const char *bindingName, const char * stateFileName)
{
    if (!m_pCentralStore)
        return nullptr;
    if(isAttached())
    {
        Owned<IPropertyTree> esdlBinding = m_pCentralStore->fetchBinding(process, bindingName);
        return esdlBinding.getClear();
    }
    else
    {
        return fetchESDLBindingFromStateFile(process, bindingName, stateFileName);
    }
}

bool EsdlBindingImpl::loadLocalDefinitions(IPropertyTree *esdlArchive, const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * esdl_binding, StringBuffer & loadedServiceName)
{
    if (!esdl || !esdl_binding || !esdlArchive)
        return false;

    //Loading first ESDL definition encountered, informed that espServiceName is to be treated as arbitrary
    IPropertyTree * esdl_binding_definition = esdl_binding->queryPropTree("Definition[1]");

    if (esdl_binding_definition)
    {
        try
        {
            const char * id = esdl_binding_definition->queryProp("@id");
            PROGLOG("Loading esdl definition for ID '%s'", id);
            loadedServiceName.set(esdl_binding_definition->queryProp("@esdlservice"));
            IEsdlShare* esdlshare = queryEsdlShare();
            Linked<IEsdlDefinition> shareddef = esdlshare->lookup(id);
            if (shareddef)
            {
                PROGLOG("Found esdl definition %s in shared cache, use it directly.", id);
                esdl.set(shareddef);
                return true;
            }
            else
                PROGLOG("Esdl definition %s not found in shared cache, loading it from archive", id);

            StringBuffer esdlXML;
            if (esdlArchive)
            {
                esdlXML.set(esdlArchive->queryProp("Definitions"));
                if (esdlXML.isEmpty())
                    throw MakeStringException(-1, "Could not load ESDL definition: '%s' assigned to esp service name '%s'", id, espServiceName);
            }

#ifdef _DEBUG
            DBGLOG("\nESDL Definition to be loaded:\n%s", esdlXML.str());
#endif
            esdl->addDefinitionFromXML(esdlXML, id);

            if (strcmp(loadedServiceName.str(), espServiceName)!=0)
                DBGLOG("ESDL Binding: ESP service %s now based off of ESDL Service def: %s", espServiceName, loadedServiceName.str());

            esdlshare->add(id, esdl.get());
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("Error while loading ESDL definitions: %s", msg.str());
            e->Release();
        }
    }
    else
    {
        DBGLOG("ESDL Binding: Could not find any ESDL definition while loading ESDL Binding: %s", esdl_binding->queryProp("@id"));
        return false;
    }

    return true;
}

/* if the target ESDL binding contains an ESDL service definition matching this espServiceName, load it.
 * Otherwise, load the first definition available, and report it via the loadedServiceName
 */
bool EsdlBindingImpl::loadStoredDefinitions(const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName, const char * stateFileName)
{
    if (!esdl || !config || !m_pCentralStore)
        return false;

    //Loading first ESDL definition encountered, informed that espServiceName is to be treated as arbitrary
    IPropertyTree * esdlDefinitionConfig = config->queryPropTree("Definition[1]");

    bool stateRestored = false;
    if (esdlDefinitionConfig)
    {
        try
        {
            const char * id = esdlDefinitionConfig->queryProp("@id");
            PROGLOG("Loading esdl definition for ID %s", id);
            loadedServiceName.set(esdlDefinitionConfig->queryProp("@esdlservice"));
            IEsdlShare* esdlshare = queryEsdlShare();
            Linked<IEsdlDefinition> shareddef = esdlshare->lookup(id);
            if (shareddef)
            {
                PROGLOG("Found esdl definition %s in shared cache, use it directly.", id);
                esdl.set(shareddef);
                return true;
            }
            else
                PROGLOG("Esdl definition %s not found in shared cache, loading it from store", id);
            StringBuffer esdlXML;
            m_pCentralStore->fetchDefinitionXML(id, esdlXML);
            if (!esdlXML.length())
            {
                Owned<IPropertyTree> esdlDefintion;
                PROGLOG("ESDL Binding: Could not load ESDL definition: '%s' from Dali, attempting to load from local state store", id);
                Owned<IPTree> pt = createPTreeFromXMLFile(stateFileName);

                if (pt)
                    esdlDefintion.set(pt->queryPropTree("EsdlDefinition"));

                if (!esdlDefintion)
                    throw MakeStringException(-1, "Could not load ESDL definition: '%s' assigned to esp service name '%s'", id, espServiceName);

                stateRestored = true;
                toXML(esdlDefintion, esdlXML);
            }

#ifdef _DEBUG
            DBGLOG("\nESDL Definition to be loaded:\n%s", esdlXML.str());
#endif
            esdl->addDefinitionFromXML(esdlXML, id);

            if (strcmp(loadedServiceName.str(), espServiceName)!=0)
                DBGLOG("ESDL Binding: ESP service %s now based off of ESDL Service def: %s", espServiceName, loadedServiceName.str());

            if (!stateRestored)
            {
                try
                {
                    saveState(esdlXML.str(), config, stateFileName);
                }
                catch (IException *E)
                {
                    StringBuffer message;
                    message.setf("Error saving DESDL state file for ESP service %s", espServiceName);
                    // If we can't save the DESDL state for this binding, then tough. Carry on without it. Changes will not survive an unexpected roxie restart
                    EXCLOG(E, message);
                    E->Release();
                }
                catch (...)
                {
                    // If we can't save the DESDL state for this binding, then tough. Carry on without it. Changes will not survive an unexpected roxie restart
                    DBGLOG("Error saving DESDL state file for ESP service %s", espServiceName);
                }
            }
            esdlshare->add(id, esdl.get());
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("Error while loading ESDL definitions: %s", msg.str());
            e->Release();
        }
    }
    else
    {
        DBGLOG("ESDL Binding: Could not find any ESDL definition while loading ESDL Binding: %s", config->queryProp("@id"));
        return false;
    }

    return true;
}

void EsdlBindingImpl::saveDESDLState()
{
    try
    {
        Owned<IEsdlDefObjectIterator> it = m_esdl->getDependencies(m_espServiceName.str(), "", 0, NULL, 0);

        StringBuffer serviceESXDL;
        m_xsdgen->toXML(*it, serviceESXDL, 0, NULL, 0);

        saveState(serviceESXDL.str(), m_esdlBndCfg, m_esdlStateFilesLocation.str());
    }
    catch (IException *E)
    {
        StringBuffer message;
        message.setf("Error saving DESDL state file for ESP service %s", m_espServiceName.str());
        // If we can't save the DESDL state for this binding, then tough. Carry on without it. Changes will not survive an unexpected roxie restart
        EXCLOG(E, message);
        E->Release();
    }
    catch (...)
    {
        // If we can't save the DESDL state for this binding, then tough. Carry on without it. Changes will not survive an unexpected roxie restart
        StringBuffer message;
        message.setf("Error saving DESDL state file for ESP service %s", m_espServiceName.str());
    }
}

bool EsdlBindingImpl::reloadDefinitionsFromCentralStore(IPropertyTree * esdlBndCng, StringBuffer & loadedname)
{
    if (!m_pCentralStore)
        return false;
    if (esdlBndCng == nullptr)
        esdlBndCng = m_esdlBndCfg.get();
    if ( m_pESDLService )
    {
        Owned<IEsdlDefinition> tempESDLDef = createNewEsdlDefinition();

        if (!loadStoredDefinitions(m_espServiceName.get(), tempESDLDef, esdlBndCng, loadedname, m_esdlStateFilesLocation.str()))
        {
            OERRLOG("Failed to reload ESDL definitions");
            return false;
        }

        DBGLOG("Definitions reloaded, will update ESDL definition object");
        CriticalBlock b(configurationLoadCritSec);

        m_esdl.setown(tempESDLDef.getClear());
        m_pESDLService->setEsdlTransformer(createEsdlXFormer(m_esdl));

        return true;
    }

    OERRLOG("Cannot reload definitions because the service implementation is not available");
    return false;
}

bool EsdlBindingImpl::reloadBindingFromCentralStore(const char* bindingId)
{
    if (!m_pCentralStore)
        return false;
    if(!bindingId || !*bindingId)
        return false;
    if(m_bindingId.length() == 0 || strcmp(m_bindingId.str(), bindingId) != 0)
        m_bindingId.set(bindingId);
    try
    {
        DBGLOG("Reloading binding %s...", bindingId);
        if ( m_pESDLService)
        {
            Owned<IPropertyTree> tempEsdlBndCfg;
            DBGLOG("Fetching ESDL binding information from dali based on ESP binding (%s)", bindingId);
            tempEsdlBndCfg.setown(m_pCentralStore->fetchBinding(bindingId));

            if (!tempEsdlBndCfg.get())
            {
                clearDESDLState();
                clearState(m_esdlStateFilesLocation.str());
                return false;
            }

            StringBuffer loadedname;
            if (!reloadDefinitionsFromCentralStore(tempEsdlBndCfg.get(), loadedname))
                return false;

            IEsdlDefService *srvdef = m_esdl->queryService(loadedname);

            if (srvdef)
                initEsdlServiceInfo(*srvdef);

            configureProxies(tempEsdlBndCfg, loadedname);

            m_pESDLService->m_espServiceType.set(loadedname);
            m_pESDLService->configureTargets(tempEsdlBndCfg, loadedname);
            m_pESDLService->configureLogging(tempEsdlBndCfg->queryPropTree("Definition[1]/LoggingManager"));
            m_pESDLService->configureMasking(tempEsdlBndCfg->queryPropTree("Definition[1]/Masking"));
            m_esdlBndCfg.setown(tempEsdlBndCfg.getClear());
        }
        else
            DBGLOG("Could not reload binding %s because service implementation object not available.", bindingId);

    }
    catch(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        IERRLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: %s", msg.str());
        e->Release();
    }
    catch (...)
    {
        IERRLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: Could Not load Binding %s information from Dali!", bindingId);
    }
    return true;
}

void EsdlBindingImpl::configureProxies(IPropertyTree *cfg, const char *service)
{
    VStringBuffer xpath("Definition[@esdlservice='%s']/Methods", service);
    IPropertyTree *target_cfg = cfg->queryPropTree(xpath.str());

    if (!target_cfg)
    {
        DBGLOG("ESDL Binding proxies: esdl service %s methods configuration not found", service);
        return;
    }

    Owned<IPropertyTreeIterator> proxies = target_cfg->getElements("Proxy");
    if (!proxies->first()) //no proxies at all
        return;

    m_proxyInfo.setown(createPTree(ipt_caseInsensitive));
    IPropertyTree *serviceTree = m_proxyInfo->addPropTree(service);

    //method definitions win, these methods will not be proxied
    Owned<IPropertyTreeIterator> methods = target_cfg->getElements("Method");
    ForEach(*methods)
        serviceTree->addPropTree("Method", createPTreeFromIPT(&methods->query()));

    ForEach(*proxies)
        serviceTree->addPropTree("Proxy", createPTreeFromIPT(&proxies->query()));
}

void EsdlBindingImpl::clearBindingState()
{
    clearDESDLState();
    clearState(m_esdlStateFilesLocation.str());
}

void EsdlBindingImpl::addService(IPropertyTree *esdlArchive, const char * name,
                                 const char * host,
                                 unsigned short port,
                                 IEspService & service)
{
    //The name passed in, is actually the @type attribute from the EspService configuration pointed to by this binding.
    CriticalBlock b(configurationLoadCritSec);

    //this has already been done by CEsdlSvcEngineSoapBindingEx::addService
    if (!m_pESDLService)
        m_pESDLService = dynamic_cast<EsdlServiceImpl*>(&service);

    m_espServiceName.set(name);
    DBGLOG("ESDL Binding: Adding service '%s' on %s port %d on %s binding.", name, (host&&*host)?host:"", port, m_bindingName.get());
    if ( m_pESDLService)
    {
        if (m_esdlBndCfg)
        {
            if (m_esdl)
            {
                StringBuffer loadedservicename;
                bool loaded = false;
                if (esdlArchive)
                    loaded = loadLocalDefinitions(esdlArchive, name, m_esdl, m_esdlBndCfg, loadedservicename);
                else
                    loaded = loadStoredDefinitions(name, m_esdl, m_esdlBndCfg, loadedservicename, m_esdlStateFilesLocation.str());

                if (!loaded)
                {
                    DBGLOG("ESDL Binding: Error adding ESP service '%s': Could not fetch ESDL definition", name);
                    return;
                }

                m_pESDLService->setEsdlTransformer(createEsdlXFormer(m_esdl));

                IEsdlDefService *srvdef = m_esdl->queryService(name);
                if (!srvdef)
                {
                    DBGLOG("ESP Service %s has been assigned to ESDL service definition %s", name, loadedservicename.str());
                    srvdef = m_esdl->queryService(loadedservicename.str());
                    name = loadedservicename.str();

                    m_espServiceName.set(name);
                    m_pESDLService->m_espServiceType.set(name);
                    m_pESDLService->m_esdl.set(m_esdl);
                }

                if (srvdef)
                    initEsdlServiceInfo(*srvdef);

                configureProxies(m_esdlBndCfg, name);

                m_pESDLService->configureTargets(m_esdlBndCfg, name);
                m_pESDLService->configureLogging(m_esdlBndCfg->queryPropTree("Definition[1]/LoggingManager"));
                m_pESDLService->configureMasking(m_esdlBndCfg->queryPropTree("Definition[1]/Masking"));
                CEspBinding::addService(name, host, port, service);
            }
            else
                DBGLOG("ESDL Binding: Error adding service '%s': ESDL definition objectnot available", name);
        }
        else
            DBGLOG("ESDL Binding: Error adding service '%s': ESDL binding configuration not available", name);
    }
    else
        DBGLOG("ESDL Binding: Error adding service '%s': service implementation object not available", name);

}

void EsdlBindingImpl::initEsdlServiceInfo(IEsdlDefService &srvdef)
{
    const char *verstr = srvdef.queryProp("version");
    if(verstr && *verstr)
        m_defaultSvcVersion.set(verstr);

    verstr = srvdef.queryProp("default_client_version");
    if (verstr && *verstr)
    {
        if (atof(verstr) > atof(m_defaultSvcVersion.str()))
            m_defaultSvcVersion.set(verstr);
    }

    if(m_defaultSvcVersion.length() > 0)
        setWsdlVersion(atof(m_defaultSvcVersion.str()));

    //superclass binding sets up wsdladdress
    //setWsdlAddress(bndcfg->queryProp("@wsdlServiceAddress"));

    // XSL Parameters are added as expressions, so any string
    // values must be quoted when setting the property value

    IProperties *xsdparams = createProperties(false);
    xsdparams->setProp( "all_annot_Param", "true()" );
    xsdparams->setProp( "no_exceptions_inline", "true()" );
    m_xsdgen->setTransformParams(EsdlXslToXsd, xsdparams);

    IProperties *wsdlparams = createProperties(false);
    wsdlparams->setProp( "location", StringBuffer().append('\'').append(getWsdlAddress()).append('\'').str() );
    wsdlparams->setProp( "create_wsdl", "true()");
    wsdlparams->setProp( "all_annot_Param", "true()" );
    wsdlparams->setProp( "no_exceptions_inline", "true()" );
    m_xsdgen->setTransformParams(EsdlXslToWsdl, wsdlparams);

    StringBuffer xsltpath(getCFD());
    xsltpath.append("xslt/esxdl2xsd.xslt");
    m_xsdgen->loadTransform(xsltpath, xsdparams, EsdlXslToXsd );
    m_xsdgen->loadTransform(xsltpath, wsdlparams, EsdlXslToWsdl );

}

void EsdlBindingImpl::getSoapMessage(StringBuffer& soapmsg,
                                     IEspContext& context,
                                     CHttpRequest* request,
                                     const char *service,
                                     const char *method)
{
    IEsdlDefMethod *mthdef = NULL;
    if (m_esdl)
    {
        IEsdlDefService *srvdef = m_esdl->queryService(service);

        if (srvdef)
        {
            mthdef = srvdef->queryMethodByName(method);
            if (mthdef)
            {
                StringBuffer ns;
                generateNamespace(context, request, srvdef->queryName(), mthdef->queryName(), ns);
                soapmsg.appendf(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
                      " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\""
                      " xmlns=\"%s\">"
                    " <soap:Body>", ns.str());

                params2xml(m_esdl, srvdef->queryName(), mthdef->queryName(), EsdlTypeRequest, request->queryParameters(), soapmsg, 0, context.getClientVersion());
                soapmsg.append("</soap:Body></soap:Envelope>");
            }
        }
    }
}

static void returnSocket(CHttpResponse *response)
{
    if (response && response->querySocketReturner())
        response->querySocketReturner()->returnSocket();
}

int EsdlBindingImpl::onGetInstantQuery(IEspContext &context,
                                       CHttpRequest* request,
                                       CHttpResponse* response,
                                       const char *serviceName,
                                       const char *methodName)
{
    StringBuffer xmlstr;
    StringBuffer source;
    StringBuffer orderstatus;

    context.addTraceSummaryTimeStamp(LogMin, "reqRecvd", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
    Owned<IMultiException> me = MakeMultiException(source.appendf("EsdlBindingImpl::%s()", methodName).str());

    IEsdlDefMethod *mthdef = NULL;
    if (!m_esdl)
    {
        me->append(*MakeStringException(-1, "ESDL definition for service %s has not been loaded", serviceName));
    }
    else
    {
        IEsdlDefService *srvdef = m_esdl->queryService(serviceName);
        context.addTraceSummaryTimeStamp(LogMax, "esdlServDefCrtd");
        if (!srvdef)
            me->append(*MakeStringException(-1, "Service %s definiton not found", serviceName));
        else
        {
            mthdef = srvdef->queryMethodByName(methodName);
            if (!mthdef)
                me->append(*MakeStringException(-1, "Method %s definiton not found", methodName));
            else
            {
                try
                {
                    params2xml(m_esdl, srvdef->queryName(), mthdef->queryName(), EsdlTypeRequest, request->queryParameters(), xmlstr, 0, context.getClientVersion());
                    ESPLOG(LogMax,"params reqxml: %s", xmlstr.str());

                    StringBuffer out;
                    StringBuffer logdata;

                    Owned<IPropertyTree> tgtcfg;
                    Owned<IPropertyTree> tgtctx;
                    Owned<IPropertyTree> req_pt = createPTreeFromXMLString(xmlstr.length(), xmlstr.str(), false);

                    StringBuffer ns, schemaLocation;
                    generateNamespace(context, request, srvdef->queryName(), mthdef->queryName(), ns);

                    if(m_pESDLService->m_returnSchemaLocationOnOK)
                        getSchemaLocation(context, request, ns, schemaLocation);

                    context.setESDLBindingID(m_bindingId.get());

                    StringBuffer origResp;
                    StringBuffer soapmsg;
                    Owned<IEsdlScriptContext> scriptContext;
                    m_pESDLService->handleServiceRequest(context, scriptContext, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(), schemaLocation.str(), req_pt.get(), out, logdata, origResp, soapmsg, 0);

                    response->setContent(out.str());

                    if (context.getResponseFormat() == ESPSerializationJSON)
                        response->setContentType(HTTP_TYPE_JSON);
                    else
                      response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
                    response->setStatus(HTTP_STATUS_OK);
                    context.addTraceSummaryTimeStamp(LogMin, "custom_fields.RspSndSt", TXSUMMARY_GRP_ENTERPRISE);
                    response->send();
                    returnSocket(response);

                    unsigned timetaken = msTick() - context.queryCreationTime();
                    context.addTraceSummaryTimeStamp(LogMin, "respSent", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

                    ESPLOG(LogMax,"EsdlBindingImpl:onGetInstantQuery response: %s", out.str());

                    m_pESDLService->handleResultLogging(context, scriptContext, *srvdef, *mthdef, tgtctx.get(), req_pt.get(), soapmsg.str(), origResp.str(), out.str(), logdata.str());
                    context.addTraceSummaryTimeStamp(LogMin, "respLogged");

                    return 0;
                }
                catch (IMultiException* mex)
                {
                    me->append(*mex);
                    mex->Release();
                }
                catch (IException* e)
                {
                    me->append(*e);
                }
                catch (...)
                {
                    me->append(*MakeStringException(-1, "Unknown Exception"));
                }
            }
        }
    }

    if (me->ordinality())
    {
        StringBuffer xml;
        me->serialize(xml);

        response->setContent(xml.str());
        response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
        response->setStatus(HTTP_STATUS_OK);
        response->send();

        ESPLOG(LogMax,"EsdlBindingImpl:onGetInstantQuery response: %s", xml.str());
    }

    return 0;
}

static bool getSoapMethodInfo(const char * xmlin,
                              StringBuffer &method,
                              StringBuffer &ns)
{
    StartTag stag;
    int type;

    XmlPullParser xpp;

    xpp.setSupportNamespaces(true);
    xpp.setInput(xmlin, strlen(xmlin));

    method.clear();
    ns.clear();

    while (true)
    {
        type = xpp.next();
        switch(type)
        {
            case XmlPullParser::START_TAG:
            {
                xpp.readStartTag(stag);
                if (strieq(stag.getLocalName(), "Body"))
                {
                    while (true)
                    {
                        type = xpp.next();
                        switch(type)
                        {
                            case XmlPullParser::START_TAG:{
                                xpp.readStartTag(stag);
                                ns.append(stag.getUri());
                                method.append(stag.getLocalName());
                                return true;
                            }
                            case XmlPullParser::END_TAG: //no start tag after body means no method
                            case XmlPullParser::END_DOCUMENT: //bad xml?
                                return false;
                        }
                    }
                }
                break;
            }
            case XmlPullParser::END_DOCUMENT:
                return false;
        }
    }
    return false;
}

void parseNamespace(const char *ns, StringBuffer &service, StringBuffer &method, StringArray &opts, StringBuffer &version, const char * namespacebase)
{
    if (ns && !strnicmp(ns, namespacebase, strlen(namespacebase)))
    {
        const char *str=ns+strlen(namespacebase);
        if (*str==':' || *str=='/')
            str++;
        while (*str && !strchr(":(@/", *str))
            service.append(*str++);
        if (*str==':' || *str=='/')
        {
            str++;
            while (*str && !strchr(":(@/", *str))
                method.append(*str++);
            while (*str && !strchr("(@", *str))
                str++;
        }
        if (*str=='(')
        {
            do
            {
                str++;
                StringBuffer val;
                while (*str && !strchr(",)@", *str))
                    val.append(*str++);
                if (val.length())
                    opts.append(val.str());
            }
            while (*str==',');
            if (*str==')')
                str++;
        }
        if (*str=='@')
            if (strnicmp(str, "@ver=", 5)==0)
            {
                str+=5;
                for (; (*str=='.' || (*str >='0' && *str<='9')); str++)
                    version.append(*str);
            }
    }
}

bool EsdlBindingImpl::checkForMethodProxy(const char *service, const char *method, StringBuffer &forwardTo, bool &resetForwardedFor)
{
    if (!m_proxyInfo || !method || !*method)
        return false;

    IPropertyTree *proxyService = m_proxyInfo->queryPropTree(service);
    if (!proxyService)
        return false;

    VStringBuffer xpath("Method[@name='%s']", method);
    if (proxyService->hasProp(xpath)) //if method is configured locally, don't proxy
        return false;

    xpath.setf("Proxy[@method='%s']", method); //exact (non-wild) match wins
    IPropertyTree *exactProxy = proxyService->queryPropTree(xpath);
    if (exactProxy)
    {
        forwardTo.set(exactProxy->queryProp("@forwardTo"));
        resetForwardedFor = exactProxy->getPropBool("@resetForwardedFor", resetForwardedFor);
        return true;
    }

    size_t len = strlen(method);
    if (len && (*method=='_' || method[len-1]=='_')) //methods beginning or ending in '_' can't be wild card selected (these are generally internal methods, use explicit match string to override)
        return false;

    Owned<IPropertyTreeIterator> proxies = proxyService->getElements("Proxy");
    ForEach(*proxies)
    {
        const char *wildname = proxies->query().queryProp("@method");
        if (isWildString(wildname) && WildMatch(method, wildname, true))
        {
            forwardTo.set(proxies->query().queryProp("@forwardTo"));
            resetForwardedFor = proxies->query().getPropBool("@resetForwardedFor", false);
            return true;
        }
    }
    return false;
}

int EsdlBindingImpl::HandleSoapRequest(CHttpRequest* request,
                                       CHttpResponse* response)
{
    IEspContext *ctx = request->queryContext();
    if(ctx->toBeAuthenticated()) //no HTTP basic auth info?
    {
        ctx->setAuthStatus(AUTH_STATUS_FAIL);
        response->sendBasicChallenge("ESP", false);
        return 0;
    }

    const char * methodName = NULL;
    const char * resptype = NULL;
    const char * reqtype = NULL;

    const char *in = NULL;
    StringBuffer xmlout;

    try
    {
        ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestStart", TXSUMMARY_GRP_ENTERPRISE);
        ctx->addTraceSummaryValue(LogMin, "app.name", m_processName.get(), TXSUMMARY_GRP_ENTERPRISE);
        ctx->addTraceSummaryValue(LogMin, "custom_fields.esp_service_type", "desdl", TXSUMMARY_GRP_ENTERPRISE);

        in = request->queryContent();
        if (!in || !*in)
            throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT,  "ESP", "SOAP content not found" );

        StringBuffer reqname;
        StringBuffer ns;
        StringBuffer schemaLocation;

        IEsdlDefMethod *mthdef=NULL;
        if (getSoapMethodInfo(in, reqname, ns))
        {
            ctx->addTraceSummaryValue(LogMin, "custom_fields.soap", reqname.str(), TXSUMMARY_GRP_ENTERPRISE);
            if (m_proxyInfo)
            {
                StringBuffer proxyMethod(reqname);
                size32_t len = reqname.length()-7;
                if (len>0 && strieq(reqname.str()+len, "Request"))
                    proxyMethod.setLength(len);
                StringBuffer proxyAddress;
                bool resetForwardedFor = false;
                if (checkForMethodProxy(request->queryServiceName(), proxyMethod, proxyAddress, resetForwardedFor))
                {
                    ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestEnd", TXSUMMARY_GRP_ENTERPRISE);
                    return forwardProxyMessage(proxyAddress, request, response, resetForwardedFor);
                }
            }

            StringBuffer nssrv;
            StringBuffer nsmth;
            StringBuffer nsver;
            StringArray nsopts;

            parseNamespace(ns.str(), nssrv, nsmth, nsopts, nsver, m_pESDLService->m_serviceNameSpaceBase.str());

            IProperties *qps=ctx->queryRequestParameters();
            if (nsopts.ordinality())
            {
                ForEachItemIn(x, nsopts)
                    qps->setProp(nsopts.item(x), "1");
            }
            if (nsver.length())
                ctx->setClientVersion(atof(nsver.str()));

            if(!m_esdl)
                throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "ESDL definition for service %s has not been loaded.", request->queryServiceName());

            IEsdlDefService *srvdef = NULL;
            if (nssrv.length())
            {
                srvdef = m_esdl->queryService(nssrv.str());
                if (!srvdef)
                    throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Web service matching namespace not found: %s", request->queryServiceName());
            }
            else
            {
                srvdef = m_esdl->queryService(request->queryServiceName());
                if (!srvdef)
                    throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Requested web service not found: %s", request->queryServiceName());
            }

            /*
             * Attempt to identify target method based on request name first
             * If that fails, look-up by method name.
             * Mimics soap binding behavior.
             */
            mthdef = srvdef->queryMethodByRequest(reqname.str());
            if (!mthdef)
            {
               mthdef = srvdef->queryMethodByName(reqname.str());
            }

            if (mthdef)
            {
                methodName = mthdef->queryName();
                if (nsmth.length() && stricmp(nsmth.str(), methodName))
                    throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, request->queryServiceName(), "Namespace operation didn't match ESP method");

                resptype = mthdef->queryResponseType();
                reqtype = mthdef->queryRequestType();
            }
            else
                throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, request->queryServiceName(),
                            "EsdlBindingImpl could not ascertain target method based on XML request structure" );

            StringBuffer reqName;
            //Owned<IPropertyTree> soap = createPTreeFromXMLString(in, ipt_none,(PTreeReaderOptions) (ipt_caseInsensitive | ptr_ignoreWhiteSpace | ptr_noRoot | ptr_ignoreNameSpaces));
            Owned<IPropertyTree> soap = createPTreeFromXMLString(in, ipt_none,(PTreeReaderOptions) (ipt_caseInsensitive | ptr_ignoreWhiteSpace | ptr_ignoreNameSpaces));

            VStringBuffer xpath("Body/%s", reqname.str());
            IPropertyTree *pt = soap->queryPropTree(xpath.str());

            if (!pt)
                throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, request->queryServiceName(), "EsdlBindingImpl could not process SOAP request" );

            StringBuffer baseout;
            StringBuffer logdata;

            Owned<IPropertyTree> tgtcfg;
            Owned<IPropertyTree> tgtctx;

            // Echo back the reqeust namespace, don't generate it here
            if(m_pESDLService->m_returnSchemaLocationOnOK)
                getSchemaLocation(*ctx, request, ns, schemaLocation);

            ctx->setESDLBindingID(m_bindingId.get());

            StringBuffer origResp;
            StringBuffer soapmsg;
            Owned<IEsdlScriptContext> scriptContext;
            m_pESDLService->handleServiceRequest(*ctx, scriptContext, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(), schemaLocation.str(), pt, baseout, logdata, origResp, soapmsg, 0);

            StringBuffer out;
            out.append(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                    "<soap:Body>");

            out.append(baseout.str());
            out.append(
                    "</soap:Body>"
                "</soap:Envelope>");

            response->setContent(out.str());
            response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
            response->setStatus(HTTP_STATUS_OK);
            ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.RspSndSt", TXSUMMARY_GRP_ENTERPRISE);
            response->send();
            returnSocket(response);

            unsigned timetaken = msTick() - ctx->queryCreationTime();
            ctx->addTraceSummaryTimeStamp(LogMin, "respSent", TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);

            m_pESDLService->handleResultLogging(*ctx, scriptContext, *srvdef, *mthdef, tgtctx.get(), pt, soapmsg.str(), origResp.str(), out.str(), logdata.str());

            ESPLOG(LogMax,"EsdlBindingImpl:HandleSoapRequest response: %s", xmlout.str());
        }
        else
            throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, request->queryServiceName(), "EsdlBindingImpl could not process SOAP request" );
    }
    catch (XmlPullParserException& exml)
    {
        StringBuffer text;
        text.appendf("Parsing xml error: %s.", exml.getMessage().c_str());
        ESPLOG(LogMax, "%s\n", text.str());
        ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestEnd", TXSUMMARY_GRP_ENTERPRISE);
        throw makeWsException(  ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, "Esp", "%s", text.str() );
    }
    catch (IException *e)
    {
        const char * source = request->queryServiceName();
        StringBuffer msg;
        ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestEnd", TXSUMMARY_GRP_ENTERPRISE);
        ctx->addTraceSummaryValue(LogMin, "custom_fields._soap_call_error_msg", e->errorMessage(msg).str(), TXSUMMARY_GRP_ENTERPRISE);
        handleSoapRequestException(e, source);
    }
    catch(...)
    {
        StringBuffer text;
        text.append( "EsdlBindingImpl could not process SOAP request" );
        ESPLOG(LogMax,"%s", text.str());
        ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestEnd", TXSUMMARY_GRP_ENTERPRISE);
        throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_SERVER, "ESP", "%s", text.str() );
    }

    ctx->addTraceSummaryTimeStamp(LogMin, "custom_fields.soapRequestEnd", TXSUMMARY_GRP_ENTERPRISE);
    return 0;
}

class CNSVariableSubstitutionHelper : public CInterfaceOf<IVariableSubstitutionHelper>
{
    IEspContext&      m_context;
    IEsdlDefinition&  m_esdl;
    StringBuffer      m_service;
    StringBuffer      m_method;
public:
    CNSVariableSubstitutionHelper(IEspContext& context, IEsdlDefinition& esdl, const char* service, const char* method)
        : m_context(context)
        , m_esdl(esdl)
        , m_service(service)
        , m_method(method)
    {
        m_service.toLowerCase();
        m_method.toLowerCase();
    }
    virtual bool findVariable(const char* name, StringBuffer &value) override
    {
        StringBuffer tmp;
        bool         required = true;
        if (!isEmptyString(name))
        {
            if (streq(name, "service"))
            {   // service name, lowercase
                tmp.append(m_service);
            }
            else if (streq(name, "esdl-service"))
            {   // service name, possible mixed case
                IEsdlDefService* esdlService = m_esdl.queryService(m_service);
                if (esdlService)
                    tmp.append(esdlService->queryName());
            }
            else if (streq(name, "method"))
            {   // method name, lowercase
                if (!m_method.isEmpty())
                    tmp.append(m_method);
            }
            else if (streq(name, "esdl-method"))
            {   // methodname, possible mixed case
                IEsdlDefService* esdlService = m_esdl.queryService(m_service);
                IEsdlDefMethod* esdlMethod = (esdlService ? esdlService->queryMethodByName(m_method) : nullptr);
                if (esdlMethod)
                    tmp.append(esdlMethod->queryName());
            }
            else if (streq(name, "optionals"))
            {   // optional esdl optionals (e.g., "internal"), lowercase
                IProperties* params = m_context.queryRequestParameters();
                Owned<IPropertyIterator> esdlOptionals = m_esdl.queryOptionals()->getIterator();
                ForEach(*esdlOptionals)
                {
                    const char* key = esdlOptionals->getPropKey();
                    if (params->hasProp(key))
                    {
                        if (!tmp.isEmpty())
                            tmp.append(',');
                        else
                            tmp.append('(');
                        tmp.append(key);
                    }
                }
                if (!tmp.isEmpty())
                    tmp.toLowerCase().append(')');
                required = false;
            }
            else if (streq(name, "version"))
            {   // client version number
                tmp.appendf("%g", m_context.getClientVersion());
            }
        }
        if (required && tmp.isEmpty())
            throw makeStringExceptionV(ERR_ESDL_BINDING_INTERNERR, "Could not generate namespace with {$%s}, ensure ESDL definition is correctly configured", name);
        value.append(tmp);
        return true;
    }
};

StringBuffer & EsdlBindingImpl::generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char * method, StringBuffer & ns)
{
    Owned<String> explicitNS(m_pESDLService->getExplicitNamespace(method));
    if (explicitNS)
    {
        CNSVariableSubstitutionHelper helper(context, *m_esdl, serv, method);
        return replaceVariables(ns.clear(), explicitNS->str(), false, &helper, "{$", "}");
    }

    if (m_pESDLService->m_serviceNameSpaceBase.isEmpty())
        throw MakeStringExceptionDirect(ERR_ESDL_BINDING_INTERNERR, "Could not generate namespace, ensure namespace base is correctly configured.");

    if (streq(m_pESDLService->m_namespaceScheme.str(), NAMESPACE_SCHEME_CONFIG_VALUE_SCAPPS))
    {
        //Need proper case for ESDL service name
        IEsdlDefService *esdlService = m_esdl->queryService(serv);
        const char *esdlServiceName = (esdlService) ? esdlService->queryName() : nullptr;
        if (!esdlServiceName || !*esdlServiceName)
            throw MakeStringExceptionDirect(ERR_ESDL_BINDING_INTERNERR, "Could not generate namespace, ensure ESDL definition is correctly configured.");
        ns.set(SCAPPS_NAMESPACE_BASE).append(esdlServiceName);
        return ns;
    }

    ns.appendf("%s%c%s", m_pESDLService->m_serviceNameSpaceBase.str(), m_pESDLService->m_usesURLNameSpace ? '/' : ':', serv);

    if (method && *method)
        ns.append(m_pESDLService->m_usesURLNameSpace ? '/': ':').append(method);

    StringBuffer ns_optionals;
    IProperties *params = context.queryRequestParameters();
    if (m_esdl)
    {
        Owned<IPropertyIterator> esdl_optionals = m_esdl->queryOptionals()->getIterator();
        ForEach(*esdl_optionals)
        {
            const char *key = esdl_optionals->getPropKey();
            if (params->hasProp(key))
            {
                if (ns_optionals.length())
                    ns_optionals.append(',');
                ns_optionals.append(key);
            }
        }
        if (ns_optionals.length())
            ns.append('(').append(ns_optionals.str()).append(')');
    }
    ns.append("@ver=").appendf("%g", context.getClientVersion());
    return ns.toLowerCase();
}


int EsdlBindingImpl::onJavaPlugin(IEspContext &context,
                               CHttpRequest* request,
                               CHttpResponse* response,
                               const char *serviceName,
                               const char *methodName)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;
    StringBuffer out;

    Owned<CSoapFault> soapFault;

    if (!serviceName || !*serviceName)
        serviceName = m_espServiceName.get();

    if (!m_esdl || !qualifyServiceName(context, serviceName, methodName, serviceQName, &methodQName))
    {
        response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        out.set("The service has not been properly loaded.");
    }
    else
    {
        StringBuffer ns;
        generateNamespace(context, request, serviceName, methodName, ns);

        try
        {
            Owned<IEsdlDefObjectIterator> it = m_esdl->getDependencies(serviceName, methodName, context.getClientVersion(), context.queryRequestParameters(), 0);
            m_xsdgen->toMicroService( *it, out, EsdlXslToJavaServiceBase, context.queryRequestParameters(), 0);
        }
        catch (IException *E)
        {
            throw makeWsException(*E, WSERR_CLIENT , "ESDL");
        }
        catch (...)
        {
            throw makeWsException(ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT , "ESDL", "Could not generate JavaPlugin for this service." );
        }
        response->setStatus(HTTP_STATUS_OK);
    }

    response->setContent(out.str());
    response->setContentType(HTTP_TYPE_TEXT_PLAIN_UTF8);
    response->send();

    return 0;
}

inline bool hasHttpPrefix(const char *addr)
{
    if (hasPrefix(addr, "http", false))
    {
        const char *finger = addr + 4;
        if (*finger=='s' || *finger=='S')
            finger++;
        if (*finger==':')
            return true;
    }
    return false;
}

int EsdlBindingImpl::forwardProxyMessage(const char *addr, CHttpRequest* request, CHttpResponse* response, bool resetForwardedFor)
{
    Owned<IHttpClientContext> httpctx = getHttpClientContext();

    StringBuffer url;
    if (!hasHttpPrefix(addr))
        url.append("http://");
    url.append(addr).append(request->queryPath());

    const char *paramstr = request->queryParamStr();
    if (paramstr && *paramstr)
        url.append('?').append(paramstr);

    DBGLOG("Forwarding request to %s", url.str());
    Owned<IHttpClient> cl = httpctx->createHttpClient(nullptr, url);
    cl->proxyRequest(request, response, resetForwardedFor);

    StringBuffer status;
    DBGLOG("Forwarded request status %s", response->getStatus(status).str());

    int statusCode = atoi(status.str());
    if (statusCode == HTTP_STATUS_UNAUTHORIZED_CODE)
        response->sendBasicChallenge(getChallengeRealm(), false);
    else
        response->send();

    return 0;
}

int EsdlBindingImpl::onGet(CHttpRequest* request, CHttpResponse* response)
{
    Owned<IMultiException> me = MakeMultiException("DynamicESDL");

    try
    {
        IEspContext *context = request->queryContext();
        IProperties *parms = request->queryParameters();

        parms->setProp("include_jsonreqs_", "1");

        const char *thepath = request->queryPath();

        StringBuffer root;
        firstPathNode(thepath, root);

        if (!strieq(root, "esdl"))
        {
            if (m_proxyInfo)
            {
                sub_service sstype = sub_serv_unknown;
                request->getEspPathInfo(sstype);

                switch (sstype)
                {
                //only proxy actual service method calls, not meta service calls
                case sub_serv_method:
                case sub_serv_query:
                case sub_serv_instant_query:
                {
                    StringBuffer method;
                    nextPathNode(thepath, method);
                    StringBuffer proxyAddress;
                    bool resetForwardedFor = false;
                    if (checkForMethodProxy(root, method, proxyAddress, resetForwardedFor))
                        return forwardProxyMessage(proxyAddress, request, response, resetForwardedFor);
                    break;
                }
                default:
                    break;
                }
            }
            return EspHttpBinding::onGet(request, response);
        }

        StringBuffer action;
        nextPathNode(thepath, action);
        if(!strieq(action, "plugin"))
            return EspHttpBinding::onGet(request, response);
        StringBuffer language;
        nextPathNode(thepath, language);
        if (!strieq(language, "java"))
            throw MakeStringException(-1, "Unsupported embedded language %s", language.str());

        StringBuffer servicename;
        nextPathNode(thepath, servicename);
        if (!servicename.length())
            throw MakeStringExceptionDirect(-1, "Service name required to generate Java plugin code");

        StringBuffer methodname;
        nextPathNode(thepath, methodname);

        return onJavaPlugin(*context, request, response, servicename, methodname);
    }
    catch (IMultiException* mex)
    {
        me->append(*mex);
        mex->Release();
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*MakeStringExceptionDirect(-1, "Unknown Exception"));
    }

    response->handleExceptions(getXslProcessor(), me, "DynamicESDL", "", StringBuffer(getCFD()).append("./smc_xslt/exceptions.xslt").str(), false);
    return 0;
}


int EsdlBindingImpl::onGetXsd(IEspContext &context,
                              CHttpRequest* request,
                              CHttpResponse* response,
                              const char *serviceName,
                              const char *methodName)
{
    StringBuffer out;
    if (!serviceName || !*serviceName)
        serviceName = m_espServiceName.get();

    context.queryXslParameters()->setProp("all_annot_Param", "false()");

    if (!getSchema(out, context, request, serviceName, methodName, false))
    {
        response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        out.set("The service has not been properly loaded.");
    }
    else
        response->setStatus(HTTP_STATUS_OK);

    response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
    response->setContent(out.str());
    response->send();
    return 0;
}

int EsdlBindingImpl::onGetWsdl(IEspContext &context,
                               CHttpRequest* request,
                               CHttpResponse* response,
                               const char *serviceName,
                               const char *methodName)
{
    StringBuffer serviceQName;
    StringBuffer methodQName;
    StringBuffer out;

    Owned<CSoapFault> soapFault;

    if (!serviceName || !*serviceName)
        serviceName = m_espServiceName.get();

    if (!m_esdl || !qualifyServiceName(context, serviceName, methodName, serviceQName, &methodQName))
    {
        response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        out.set("The service has not been properly loaded.");
    }
    else
    {
        StringBuffer ns;
        generateNamespace(context, request, serviceName, methodName, ns);

        try
        {
            Owned<IEsdlDefObjectIterator> it = m_esdl->getDependencies(serviceName, methodName, context.getClientVersion(), context.queryRequestParameters(), ESDLDEP_FLAGS);
            IProperties* overrideParams = context.queryXslParameters();
            overrideParams->setProp("all_annot_Param", "false()");
            m_xsdgen->toWSDL( *it, out, EsdlXslToWsdl, context.getClientVersion(), context.queryRequestParameters(), ns.str(), ESDLDEP_FLAGS, overrideParams);
        }
        catch (...)
        {
            throw makeWsException(ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT , "ESP", "Could not generate WSDL for this service." );
        }
        response->setStatus(HTTP_STATUS_OK);
    }

    response->setContent(out.str());
    response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
    response->send();

    return 0;
}

bool EsdlBindingImpl::getSchema(StringBuffer& schema,
                                IEspContext &ctx,
                                CHttpRequest* req,
                                const char *service,
                                const char *method,
                                bool standalone)
{

    StringBuffer ns;
    if (!service || !*service)
        return false;
    if (!m_esdl)
        return false;

    generateNamespace(ctx, req, service, method, ns);

    Owned<IEsdlDefObjectIterator> it = m_esdl->getDependencies(service, method, ctx.getClientVersion(), ctx.queryRequestParameters(), ESDLDEP_FLAGS);
    m_xsdgen->toXSD( *it, schema, EsdlXslToXsd, ctx.getClientVersion(), ctx.queryRequestParameters(), ns.str(), ESDLDEP_FLAGS, ctx.queryXslParameters());
    ESPLOG(LogMax,"EsdlBindingImpl::getSchema schema: %s", schema.str());
    return true;
}

int EsdlBindingImpl::getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods)
{
    if (!m_esdl)
    {
        ESPLOG(LogMax,"ESDL definition for service not loaded");
        return -1;
    }

    double ver = ctx.getClientVersion();
    if (ver<=0)
        ver = m_defaultSvcVersion.get() ? atof(m_defaultSvcVersion.get()) : 0;
    if (ver<=0)
        ver = getWsdlVersion();

    //Rodrigo, here we might need to fetch all associated services
    const char *servname=queryServiceType();
    IEsdlDefService *srv = m_esdl->queryService(servname);
    if (!srv)
    {
        ESPLOG(LogNone,"Service (%s) not found", servname);
        return -1;
    }

    Owned<IEsdlDefMethodIterator> it = srv->getMethods();
    ForEach(*it)
    {
        IEsdlDefMethod &mth = it->query();
        const char* optional = mth.queryProp("optional");

        if (mth.checkVersion(ver) && (optional ? ctx.checkOptional(optional) : true))
        {
            CMethodInfo *mthinfo = new CMethodInfo(mth.queryMethodName(), mth.queryRequestType(), mth.queryResponseType());
            methods.append(*mthinfo);
        }
    }
    return methods.ordinality();
}

int EsdlBindingImpl::getMethodProperty(IEspContext &context, const char *serv, const char *method, StringBuffer &page, const char *propname, const char *dfault)
{
    if (!serv || !*serv || !method || !*method)
        return 0;

    if (!m_esdl)
    {
        ESPLOG(LogMax,"EsdlBindingImpl::getMethodProperty - ESDL definition for service not loaded");
        return 0;
    }

    IEsdlDefService *srv = m_esdl->queryService(serv);
    if (!srv)
    {
        ESPLOG(LogMax,"EsdlBindingImpl::getMethodProperty - service (%s) not found", serv);
        return 0;
    }

    IEsdlDefMethod *mth = srv->queryMethodByName(method);
    if (!mth)
    {
        ESPLOG(LogMax,"EsdlBindingImpl::getMethodProperty - service (%s) method (%s) not found", serv, method);
        return 0;
    }

    const char *value = mth->queryProp(propname);
    page.append(value ? value : dfault);
    return 0;
}

int EsdlBindingImpl::getMethodDescription(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    return getMethodProperty(context, serv, method, page, ESDL_METHOD_DESCRIPTION, "No description available");
}
int EsdlBindingImpl::getMethodHelp(IEspContext &context, const char *serv, const char *method, StringBuffer &page)
{
    return getMethodProperty(context, serv, method, page, ESDL_METHOD_HELP, "No Help available");
}

bool EsdlBindingImpl::qualifyServiceName(IEspContext &context,
                                        const char *servname,
                                        const char *methname,
                                        StringBuffer &servQName,
                                        StringBuffer *methQName)
{
    if (!m_esdl)
    {
        ESPLOG(LogMax,"ESDL definition for service not loaded");
        return false;
    }

    IEsdlDefService *srv = m_esdl->queryService(servname);
    if (!srv)
    {
        ESPLOG(LogMax,"Service (%s) not found", servname);
        return false;
    }

    servQName.clear().append(srv->queryName());
    if (methname && *methname && methQName)
    {
        methQName->clear();
        IEsdlDefMethod *mth = srv->queryMethodByName(methname);
        if (mth)
            methQName->append(mth->queryName());
    }
    else if (methQName != NULL)
        methQName->clear();

    return servQName.length()? true : false;
}

bool EsdlBindingImpl::qualifyMethodName(IEspContext &context,
                                        const char *methname,
                                        StringBuffer *methQName)
{
    if (!m_esdl)
    {
        ESPLOG(LogMax,"ESDL definition for service not loaded");
        return false;
    }

    IEsdlDefService *srv = m_esdl->queryService(queryServiceType());
    if (!srv)
        throw makeWsException(  ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESP", "Service definition not found");

    IEsdlDefMethod *mth = srv->queryMethodByName(methname);
    if (mth && methQName)
    {
        methQName->clear();
        if (mth)
            methQName->append(mth->queryName());
    }

    return (mth && (!methQName || methQName->length()));
}

int EsdlBindingImpl::onGetXForm(IEspContext &context,
                                CHttpRequest* request,
                                CHttpResponse* response,
                                const char *serv,
                                const char *method)
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

        Owned<IXslProcessor> xslp  = getXslProcessor();

        // get schema
        StringBuffer schema;
        context.addOptions(ESPCTX_ALL_ANNOTATION);
        getSchema(schema, context, request, serv, method, true);
        ESPLOG(LogMax,"Schema: %s", schema.str());

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
        EspHttpBinding::escapeSingleQuote(tmp,escaped);
        xform->setStringParameter("methodHelp", escaped);

        getMethodDescription(context, serviceQName.str(), methodQName.str(), tmp.clear());
        EspHttpBinding::escapeSingleQuote(tmp,escaped.clear());
        xform->setStringParameter("methodDesc", escaped);

        // By default, even if the config property is missing, include the Roxie Test button
        if (m_esdlBndCfg->getPropBool("@includeRoxieTest", true)==false)
            xform->setParameter("includeRoxieTest", "0");
        else
            xform->setParameter("includeRoxieTest", "1");
        xform->setParameter("includeJsonTest", "1");
        xform->setParameter("includeJsonReqSample", "1");

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
        const char* authMethod = context.getAuthenticationMethod();
        if (authMethod && !strieq(authMethod, "none") && ((context.getDomainAuthType() == AuthPerSessionOnly) || (context.getDomainAuthType() == AuthTypeMixed)))
            xform->setParameter("showLogout", "1");

        xform->transform(page);
        response->setContentType("text/html");
        response->setContent(page.str());
    }

    response->send();

    return 0;
}

int EsdlBindingImpl::getJsonTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response)
{
    const char * servname = request->queryServiceName();
    const char * methname = request->queryServiceMethod();
    IProperties *parms = context.queryRequestParameters();

    StringBuffer jsonmsg("{\n");
    try
    {
        getRequestContent(context, jsonmsg, request, servname, methname, NULL, ESDL_BINDING_REQUEST_JSON);
    }
    catch (IException *e)
    {
        JsonHelpers::appendJSONException(jsonmsg, e);
    }
    catch (...)
    {
        JsonHelpers::appendJSONExceptionItem(jsonmsg, -1, "Error Processing JSON request");
    }
    jsonmsg.append("\n}");

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
    xform->setStringParameter("serviceName", servname);
    xform->setStringParameter("methodName", methname);
    xform->setStringParameter("header", header.str());

    ISecUser* user = context.queryUser();
    bool inhouse = user && (user->getStatus()==SecUserStatus_Inhouse);
    xform->setParameter("inhouseUser", inhouse ? "true()" : "false()");

    StringBuffer destination;
    destination.appendf("%s?%s", methname, params.str());
    xform->setStringParameter("destination", destination.str());

    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

void EsdlBindingImpl::handleJSONPost(CHttpRequest *request, CHttpResponse *response)
{
    IEspContext *ctx = request->queryContext();
    ctx->addTraceSummaryValue(LogNormal, "Esdl Binding", "JSONPost");

    StringBuffer jsonresp;

    try
    {
        const char * methodName = request->queryServiceMethod();
        const char * serviceName = request->queryServiceName();

        if (m_proxyInfo)
        {
            StringBuffer proxyAddress;
            bool resetForwardedFor = false;
            if (checkForMethodProxy(serviceName, methodName, proxyAddress, resetForwardedFor))
            {
                forwardProxyMessage(proxyAddress, request, response, resetForwardedFor);
                return;
            }
        }

        StringBuffer content(request->queryContent());
        if (getEspLogLevel()>LogNormal)
            DBGLOG("EsdlBinding::%s::%s: JSON request: %s", serviceName, methodName, content.str());

        Owned<IPropertyTree> contentTree = createPTreeFromJSONString(content.str());
        if (!contentTree)
            throw MakeStringException(-1, "EsdlBinding::%s::%s: Could not process JSON request", serviceName, methodName);

        StringBuffer requestName;
        if (stricmp(methodName, "ping") == 0)
            requestName.append(serviceName);
        requestName.append(methodName).append("Request");

        Owned<IPropertyTree> reqTree = contentTree->getBranch(requestName.str());
        if (!reqTree)
            throw MakeStringException(-1, "EsdlBinding::%s::%s: Could not find \"%s\" section in JSON request", serviceName, methodName, requestName.str());

        if (!m_esdl)
            throw MakeStringException(-1, "EsdlBinding::%s: Service definition has not been loaded", serviceName);
        IEsdlDefService *srvdef = m_esdl->queryService(serviceName);

        if (!srvdef)
            throw MakeStringException(-1, "EsdlBinding::%s: Service definition not found", serviceName);
        IEsdlDefMethod *mthdef = srvdef->queryMethodByName(methodName);
        if (!mthdef)
            throw MakeStringException(-1, "EsdlBinding::%s::%s: Method definition not found", serviceName, methodName);
        jsonresp.append("{");
        StringBuffer logdata; //RODRIGO: What are we doing w/ the logdata?

        Owned<IPropertyTree> tgtcfg;
        Owned<IPropertyTree> tgtctx;

        StringBuffer ns, schemaLocation;
        generateNamespace(*ctx, request, serviceName, methodName, ns);
        if(m_pESDLService->m_returnSchemaLocationOnOK)
            getSchemaLocation(*ctx, request, ns, schemaLocation);

        ctx->setESDLBindingID(m_bindingId.get());
        StringBuffer origResp;
        StringBuffer soapmsg;
        Owned<IEsdlScriptContext> scriptContext;
        m_pESDLService->handleServiceRequest(*ctx, scriptContext, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(), schemaLocation.str(), reqTree.get(), jsonresp, logdata, origResp, soapmsg, ESDL_BINDING_RESPONSE_JSON);

        jsonresp.append("}");

        response->setContent(jsonresp.str());
        response->setContentType("application/json");
        response->setStatus("200 OK");
        response->send();
        returnSocket(response);

        m_pESDLService->handleResultLogging(*ctx, scriptContext, *srvdef, *mthdef, tgtctx.get(), reqTree, soapmsg.str(), origResp.str(), jsonresp.str(), logdata.str());

        if (getEspLogLevel()>LogNormal)
            DBGLOG("json response: %s", jsonresp.str());
        return;
    }
    catch (IWsException * iwse)
    {
        JsonHelpers::appendJSONException(jsonresp.set("{"), iwse);
        jsonresp.append('}');
        iwse->Release();
    }
    catch (IException *e)
    {
        JsonHelpers::appendJSONException(jsonresp.set("{"), e);
        jsonresp.append("\n}");
        e->Release();
    }
    catch (...)
    {
        JsonHelpers::appendJSONExceptionItem(jsonresp.set("{"), -1, "Error Processing JSON request");
        jsonresp.append("\n}");
    }

    response->setContent(jsonresp.str());
    response->setContentType("application/json");
    response->setStatus("200 OK");
    response->send();
}

void EsdlBindingImpl::handleHttpPost(CHttpRequest *request, CHttpResponse *response)
{
    StringBuffer headerContentType;
    request->getContentType(headerContentType);

    sub_service sstype = sub_serv_unknown;

    StringBuffer pathEx;
    StringBuffer serviceName;
    StringBuffer methodName;
    StringBuffer paramStr;

    request->getEspPathInfo(sstype, &pathEx, &serviceName, &methodName, false);

    if (m_proxyInfo)
    {
        switch (sstype)
        {
        //only proxy actual service method calls, not meta service calls
        case sub_serv_method:
        case sub_serv_query:
        case sub_serv_instant_query:
        {
            StringBuffer proxyAddress;
            bool resetForwardedFor = false;
            if (checkForMethodProxy(serviceName, methodName, proxyAddress, resetForwardedFor)) //usually won't catch SOAP requests, so will test again later when method is known.
            {
                forwardProxyMessage(proxyAddress, request, response, resetForwardedFor);
                return;
            }
            break;
        }
        default:
            break;
        }
    }

    request->queryContext()->addTraceSummaryTimeStamp(LogMin, "custom_fields.HttpPostStart", TXSUMMARY_GRP_ENTERPRISE);
    switch (sstype)
    {
        case sub_serv_roxie_builder:
             onGetRoxieBuilder( request, response, serviceName.str(), methodName.str());
             break;
        case sub_serv_json_builder:
             getJsonTestForm(*request->queryContext(), request, response);
             break;
        default:
        {
            IProperties* props = request->queryParameters();

            if(props && props->hasProp("_RoxieRequest"))
            {
                onRoxieRequest(request, response, methodName.str());
            }
            else
            {
                StringBuffer ct;
                request->getContentType(ct);

                if (!strnicmp(ct.str(), "application/json", 16))
                   handleJSONPost(request, response);
                else
                   EspHttpBinding::handleHttpPost(request, response);
            }
            break;
        }
    }
    request->queryContext()->addTraceSummaryTimeStamp(LogMin, "custom_fields.HttpPostEnd", TXSUMMARY_GRP_ENTERPRISE);
}

int EsdlBindingImpl::onRoxieRequest(CHttpRequest* request, CHttpResponse* response, const char * method)
{

    IEspContext *ctx = request->queryContext();

    StringBuffer roxieRequest;
    StringBuffer roxieResponse;
    StringBuffer roxieUrl;

    Owned<IMultiException> me = MakeMultiException("Roxie Test Page");

    request->getContent(roxieRequest);

    if( roxieRequest.length() == 0 )
    {
        response->setStatus(HTTP_STATUS_BAD_REQUEST);
        response->setContent("<error><content>ROXIE Request is empty</content></error>");
        response->send();
        return -1;
    }

    IProperties* params = request->queryParameters();

    params->getProp("roxie-url",roxieUrl);

    response->setContentType("text/html; charset=UTF-8");

    try
    {
        VStringBuffer xpath("Target[@name=\"%s\"]", method);
        Owned<IPropertyTree> srvinfo = m_pESDLService->m_pServiceMethodTargets->getPropTree(xpath.str());

        if (!srvinfo)
        {
            response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        }
        else
        {
            m_pESDLService->sendTargetSOAP(*ctx,srvinfo,roxieRequest.str(),roxieResponse, false, roxieUrl.str());
            response->setContent(roxieResponse.str());
            response->setStatus(HTTP_STATUS_OK);
        }

        response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
        response->send();
    }
    catch (IException *e)
    {
        me->append(*e);
    }
    catch(...)
    {
        me->append(*MakeStringException(-1, "ESP could not process SOAP request"));
    }

    if (me->ordinality())
    {
        StringBuffer xml;
        me->serialize(xml);

        response->setContent(xml.str());
        response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
        response->setStatus(HTTP_STATUS_NOT_ALLOWED);
        response->send();

        ESPLOG(LogMax,"Roxie Test response: %s", xml.str());
    }

    return 0;
}

/*
 * Splits URLLists
 * URLList should be in this format: [(HTTP|HTTPS)://][username':'password'@']ip':'port1['|'iprange:port2]['/'path['?'options]
 *
 * Returns:
 *  protocol = ["HTTP"|"HTTPS"]. "HTTP" if none found.
 *  userName = "username". Blank if none found
 *  password = "password". Blank if none found
 *  ipPortListBody = Content between usernam:password@ and /path. Assumed to be syntactically correct, no validation performed.
 *  path = "/path". "/" if none found.
 *  options = "urlop1&urlopt2...". "" if none found.
 */

void EsdlBindingImpl::splitURLList(const char* urlList, StringBuffer& protocol,StringBuffer& userName,StringBuffer& password, StringBuffer& ipPortListBody, StringBuffer& path, StringBuffer& options)
{
    if(!urlList || !*urlList)
        throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Invalid URL List.");

    int protlen = 0;
    if(strnicmp(urlList, "HTTP://",7) == 0)
    {
        protocol.set("HTTP");
        protlen = 7;
    }
    else if(strnicmp(urlList, "HTTPS://", 8) == 0)
    {
        protocol.set("HTTPS");
        protlen = 8;
    }
    else
    {
        protocol.set("HTTP");
        protlen = 0;
    }

    const char *finger=urlList+protlen;
    const char *lastDelimiter = finger;

    path.append("/");

    while (*finger && *finger != '@' && *finger != '/')
       finger++;

    if (*finger && *finger == '@')
    {
       const char *tmpDelimiter = lastDelimiter;
       while (*tmpDelimiter && *tmpDelimiter != ':' && tmpDelimiter != finger)
           tmpDelimiter++;

       if (*tmpDelimiter == ':')
       {
           userName.append(tmpDelimiter-lastDelimiter, lastDelimiter);
           password.append((finger-1)-tmpDelimiter, tmpDelimiter+1);
       }
       else if (tmpDelimiter == finger)
           userName.append(finger-lastDelimiter, lastDelimiter);
       else
           throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Invalid URL List, missing user:pass delimiter %s", urlList);

       lastDelimiter = finger+1;
       if (!*lastDelimiter)
           throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Invalid URL List %s", urlList);

    }

    while (*finger && *finger != '/')
       finger++;

    ipPortListBody.append(finger-lastDelimiter, lastDelimiter);
    if (!*finger)
       return;
    lastDelimiter = finger+1;

    while (*finger && *finger != '?')
       finger++;

    path.append(finger-lastDelimiter, lastDelimiter);
    if (!*finger)
        return;
    lastDelimiter = finger+1;

    while (*finger)
          finger++;

    options.append(finger-lastDelimiter, lastDelimiter);
}

void EsdlBindingImpl::getRequestContent(IEspContext &context, StringBuffer & req, CHttpRequest* request, const char * servicename, const char * methodname, const char *ns, unsigned flags)
{
    if ( m_pESDLService)
    {
       StringBuffer xmlstr;
       params2xml(m_esdl, servicename, methodname, EsdlTypeRequest, request->queryParameters(), xmlstr, flags, context.getClientVersion());
       ESPLOG(LogMax,"params reqxml: %s", xmlstr.str());

       Owned<IPropertyTree> tgtctx;
       Owned<IPropertyTree> req_pt = createPTreeFromXMLString(xmlstr.length(), xmlstr.str(), false);

       Owned<IXmlWriterExt> writer = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_REQUEST_JSON) ? WTJSONRootless : WTStandard);

       m_pESDLService->m_pEsdlTransformer->process(context, EsdlRequestMode, m_espServiceName.get(), methodname, *req_pt.get(), writer.get(), flags, ns);

       req.append(writer->str());
    }
}

int EsdlBindingImpl::onGetRoxieBuilder(CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    if (! m_pESDLService || !m_esdl)
        return(0);

    Owned<IPropertyTree> srvinfo;
    StringBuffer strUrl;
    StringBuffer strQueryName;

    StringBuffer strUsername;
    StringBuffer strPassword;
    StringBuffer xmlstr;
    StringBuffer roxieUrl;

    StringBuffer roxiemsg, serviceQName, methodQName;
    IEspContext * context = request->queryContext();
    context->setTransactionID("ROXIETEST-NOTRXID");
    IEsdlDefService *defsrv = m_esdl->queryService(queryServiceType());
    IEsdlDefMethod *defmth;

    if(defsrv)
        defmth = defsrv->queryMethodByName(method);
    else
        throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP", "Could not find service method: %s", method);
    const char * mthname = defmth->queryName();
    const char * srvname = defsrv->queryName();

    if (!qualifyServiceName(*context, srvname, mthname, serviceQName, &methodQName) || methodQName.length()==0)
    {
        roxiemsg.append("Cannot qualify service name");
    }
    else
    {
        if ( m_pESDLService)
        {
            VStringBuffer xpath("Target[@name=\"%s\"]", mthname);
            Owned<IPropertyTree> tgtcfg = m_pESDLService->m_pServiceMethodTargets->getPropTree(xpath.str());

            if (tgtcfg)
            {
                const char *tgtQueryName =  tgtcfg->queryProp("@queryname");

                if (tgtQueryName && *tgtQueryName)
                {
                    m_pESDLService->generateTargetURL(*context,tgtcfg, roxieUrl, false);

                    Owned<IPropertyTree> tgtctx;
                    Owned<IPropertyTree> req_pt = createPTreeFromXMLString(xmlstr.length(), xmlstr.str(), false);

                    StringBuffer ns,schema;
                    generateNamespace(*context, request, srvname, mthname, ns);

                    StringBuffer reqcontent;
                    getRequestContent(*context, reqcontent, request, srvname, mthname, ns, ROXIEREQ_FLAGS);

                    tgtctx.setown( m_pESDLService->createTargetContext(*context, tgtcfg, *defsrv, *defmth, req_pt));

                    Owned<IEsdlScriptContext> scriptContext = m_pESDLService->checkCreateEsdlServiceScriptContext(*context, *defsrv, *defmth, tgtcfg.get(), req_pt);
                    m_pESDLService->prepareFinalRequest(*context, scriptContext, tgtcfg, tgtctx, *defsrv, *defmth, true, ns.str(), reqcontent, roxiemsg);
                }
                else
                {
                    StringBuffer msg;
                    msg.set("Roxie Builder: Could not find target query for method: ").append(method);
                    throw createEspHttpException(HTTP_STATUS_NOT_FOUND_CODE, msg.str(), HTTP_STATUS_NOT_FOUND);
                }
            }
            else
            {
                StringBuffer msg;
                msg.set("Roxie Builder: Could not find configuration for method: ").append(method);
                throw createEspHttpException(HTTP_STATUS_NOT_FOUND_CODE, msg.str(), HTTP_STATUS_NOT_FOUND);
            }
        }
        else
        {
            StringBuffer msg;
            msg.set("Roxie Builder: Could not find configuration for service: ").append(serv);
            throw createEspHttpException(HTTP_STATUS_NOT_FOUND_CODE, msg.str(), HTTP_STATUS_NOT_FOUND);
        }
    }

    VStringBuffer version("%g",context->getClientVersion());
    VStringBuffer dest("%s/%s?ver_=%s", serviceQName.str(), methodQName.str(), version.str());

    VStringBuffer header("Content-Type: %s; charset=UTF-8", HTTP_TYPE_TEXT_XML);

    /*StringBuffer auth, abuf;
    abuf.appendf("%s:%s", strUsername.str(), strPassword.str());
    JBASE64_Encode(abuf.str(), abuf.length(), auth, false);
    header.appendf("Authorization: Basic %s\r\n", auth.str());
    */

    Owned<IXslProcessor> xslp  = getXslProcessor();
    Owned<IXslTransform> trans = xslp->createXslTransform();

    Owned<IXslTransform> xform = xslp->createXslTransform();

    StringBuffer xsltpath(getCFD());
    xsltpath.append("xslt/roxie_page.xsl");
    xform->loadXslFromFile(xsltpath.str());
    xform->setXmlSource("<xml/>", 6);

    // params
    xform->setStringParameter("serviceName", serviceQName.str());
    xform->setStringParameter("methodName", methodQName.str());
    xform->setStringParameter("roxiebody", roxiemsg.str());
    xform->setStringParameter("header", header.str());

    xform->setStringParameter("roxieUrl", roxieUrl.str());

    VStringBuffer url("%s?ver_=%s", methodQName.str(), version.str());
    xform->setStringParameter("destination", url.str());

    StringBuffer page;
    xform->transform(page);

    response->setContent(page);
    response->setContentType("text/html; charset=UTF-8");
    response->setStatus(HTTP_STATUS_OK);
    response->send();

    return 0;
}

void EsdlBindingImpl::getSoapMessage(StringBuffer& out,StringBuffer& soapresp,
                                    const char * starttxt,const char * endtxt)

{
    int len = soapresp.length();
    if(len<=0)
        return;

    const char* begin = soapresp.str();
    const char* end = begin+len;
    const char* finger = begin;

    const unsigned int envlen = 9;
    const unsigned int textlen = strlen(starttxt);
    const unsigned int textlenend = strlen(endtxt);

    //find the envelope
    while(finger && finger<=end-envlen && strnicmp(finger,":envelope",envlen)!=0)
        finger++;

    //no envelope so return as is
    if(finger==end-envlen)
    {
        out.append(soapresp.str());
        return;
    }

    //find the Reason
    while(finger && finger<end-textlen && strnicmp(finger,starttxt,textlen)!=0)
        ++finger;

    //if for some reason invalid soap, return
    if(finger==end-textlen)
    {
        ESPLOG(LogMax,"getSoapMessage - Could not find start text, trying for other tags.: %s",soapresp.str());
        return;
    }

    //find the end bracket incase there is namespace
    while(finger && *finger!='>')
        finger++;

    const char* start=finger+1;

    //now find the end of the Reason
    finger = soapresp.str() + soapresp.length() - textlenend;
    while(finger&& finger>=start && strnicmp(finger,endtxt,textlenend)!=0)
        finger--;

    if(finger==start)
    {
        ESPLOG(LogMax,"EsdlServiceImpl - Could not find end text, trying for other tags.: %s",soapresp.str());
        return;
    }

    while(finger>start && *finger!='<')
        finger--;

    out.clear().append(finger-start,start);
}

void EsdlBindingImpl::handleSoapRequestException(IException *e, const char *)
{
    throw e;
}

int EsdlBindingImpl::onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *pathex)
{
    if(request == NULL || response == NULL)
        return -1;
    StringBuffer mimetype;
    MemoryBuffer content;

    StringBuffer filepath;
    getBaseFilePath(filepath);
    if (strchr("\\/", filepath.charAt(filepath.length()-1))==NULL)
        filepath.append("/");
    filepath.append(pathex);
    response->httpContentFromFile(filepath.str());
    response->send();
    return 0;
}

int EsdlBindingImpl::onGetReqSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    return onGetSampleXml(true, ctx, request, response, serv, method);
}

int EsdlBindingImpl::onGetRespSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    return onGetSampleXml(false, ctx, request, response, serv, method);
}

int EsdlBindingImpl::onGetSampleXml(bool isRequest, IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringBuffer schema;
    if (!getSchema(schema, ctx, request, serv, method, true))
    {
        response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        response->setContent("The service has not been properly loaded.");
    }

    EspHttpBinding::generateSampleXmlFromSchema(isRequest, ctx, request, response, serv, method, schema.str());

    return 0;
}

void EsdlBindingImpl::transformGatewaysConfig( IPropertyTree* srvcfg, IPropertyTree* forRoxie, const char* altElementName )
{
    // Do we need to handle 'local FQDN'? It doesn't appear to be in the
    // Gateway element. Not sure where it's set but the RemoteNSClient
    // references it in RemoteNSFactory.cpp translateGateway() and modifies
    // resulting gateway URL if it's set.
    if( srvcfg && forRoxie)
    {
        Owned<IPropertyTreeIterator> cfgIter = srvcfg->getElements("Gateways/Gateway");
        cfgIter->first();
        if( cfgIter->isValid() )
        {
            const char* treeName = (!isEmptyString(altElementName) ? altElementName : "Gateway");

            while( cfgIter->isValid() )
            {
                IPropertyTree& cfgGateway = cfgIter->query();

                StringBuffer url, service;
                if( makeURL( url, cfgGateway ) )
                {
                    cfgGateway.getProp("@name", service);
                    service.toLowerCase();

                    Owned<IPropertyTree> gw = createPTree(treeName, false);
                    gw->addProp("ServiceName", service.str());
                    gw->addProp("URL", url.str());

                    forRoxie->addPropTree(treeName, gw.getLink());
                }
                else
                {
                    DBGLOG( "transformGatewaysConfig: Gateways/Gateway url in config for service='%s'", service.str() );
                }

                cfgIter->next();
            }
        }
    }
}

bool EsdlBindingImpl::makeURL( StringBuffer& url, IPropertyTree& cfg )
{
    bool result = false;

    // decrypt password
    // encode password
    // construct gateway URL including password

    StringBuffer decryptPass, password;
    StringBuffer user;
    const char* pw = NULL;
    const char* usr = NULL;

    pw = cfg.queryProp("@password");
    if( pw )
    {
        decrypt( decryptPass, pw );
        encodeUrlUseridPassword( password, decryptPass.str() );
    }

    usr = cfg.queryProp("@username");
    if( usr )
    {
        encodeUrlUseridPassword( user, usr );
    }

    bool roxieClient = cfg.getPropBool("@roxieClient", true);
    const char* cfgURL = cfg.queryProp("@url");

    if( cfgURL && *cfgURL )
    {
        StringBuffer protocol, name, pw, host, port, path;
        Utils::SplitURL( cfgURL, protocol, name, pw, host, port, path );

        if( protocol.length()>0 && host.length()>0 )
        {
            StringBuffer roxieURL;
            url.append( protocol );
            url.append( "://" );

            if( roxieClient && user.length()>0 && password.length()>0 )
            {
                url.appendf("%s:%s@", user.str(), password.str());
            }

            url.append(host);

            if( port.length()>0 )
            {
                url.appendf(":%s", port.str());
            }

            if( path.length()>0 )
            {
                url.append(path);
            }

            result = true;
        }
    }

    return result;
}

bool EsdlBindingImpl::usesESDLDefinition(const char * name, int version)
{
    if (!name || !*name)
        return false;
    if (version <= 0)
        return false;
    VStringBuffer id("%s.%d", name, version);

    return usesESDLDefinition(id.str());
}

bool EsdlBindingImpl::usesESDLDefinition(const char * id)
{
    CriticalBlock b(configurationLoadCritSec);
    return !id || !m_esdl ? false : m_esdl->hasXMLDefintionLoaded(id);
}
