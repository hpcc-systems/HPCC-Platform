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
#ifndef _EsdlBinding_HPP__
#define _EsdlBinding_HPP__

#include "esdl_script.hpp"
#include "esdl_def.hpp"
#include "esdl_transformer.hpp"
#include "esdl_def_helper.hpp"
#include "jsmartsock.hpp"
#include "dasds.hpp"
#include "jptree.hpp"
#include "xsdparser.hpp"
#include "loggingmanager.h"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "dautils.hpp"
#include "esdl_store.hpp"
#include "esdl_monitor.hpp"
#include "espplugin.ipp"
#include "datamaskingengine.hpp"
#include "txsummary.hpp"

static const char* ESDL_METHOD_DESCRIPTION="description";
static const char* ESDL_METHOD_HELP="help";

#define SDS_LOCK_TIMEOUT_DESDL (30*1000)

#include "SOAP/Platform/soapbind.hpp"

#define ERR_ESDL_BINDING_AUTHERROR  401
#define ERR_ESDL_BINDING_BADACCOUNT 402
#define ERR_ESDL_BINDING_BADREQUEST 403
#define ERR_ESDL_BINDING_BADABI     410
#define ERR_ESDL_BINDING_BADBRAND   411
#define ERR_ESDL_BINDING_BADBROKER  412
#define ERR_ESDL_BINDING_TESTCASE   413
#define ERR_ESDL_BINDING_BADVERSION 414
#define ERR_ESDL_BINDING_UNEXPECTED 498
#define ERR_ESDL_BINDING_UNAVAIL    499
#define ERR_ESDL_BINDING_INTERNERR  999

//#define ESDL_TRANS_START_AT_ROOT
#define ESDL_BINDING_RESPONSE_XML   0x02000
#define ESDL_BINDING_RESPONSE_JSON  0x04000
#define ESDL_BINDING_REQUEST_XML    0x08000
#define ESDL_BINDING_REQUEST_JSON   0x10000

#define CACHED_RESULT_NAME  "_cached_result"
#define REQ_REF_NUM_NAME    "_req_ref_num"
#define MCACHE_OBJECT_KEY   "_mcache_object_key_"

#ifdef LINK_STATICALLY  // May be useful for debugging...
namespace javaembed { IEmbedContext* getEmbedContext(); }
#endif

typedef int (*cpp_service_method_t)(const char* CtxXML, const char* ReqXML, StringBuffer& RespXML);

class EsdlServiceImpl : public CInterface, implements IEspService
{
private:
    inline Owned<ILoggingManager>& loggingManager() { return m_oDynamicLoggingManager ? m_oDynamicLoggingManager : m_oStaticLoggingManager; }
    inline Owned<IDataMaskingEngine>& maskingEngine() { return m_oDynamicMaskingEngine ? m_oDynamicMaskingEngine : m_oStaticMaskingEngine; }
    IEspContainer *container = nullptr;
    Owned<IEsdlTransformMethodMap> m_transforms = createEsdlTransformMethodMap();
    bool nonLegacyTransforms = false;

    MapStringToMyClass<ISmartSocketFactory> connMap;
    MapStringToMyClass<IEmbedServiceContext> javaServiceMap;
    MapStringToMyClass<IException> javaExceptionMap;
    MapStringToMyClass<IEspPlugin> cppPluginMap;
    MapStringTo<cpp_service_method_t, cpp_service_method_t> cppProcMap;
    Owned<ILoggingManager> m_oDynamicLoggingManager;
    Owned<ILoggingManager> m_oStaticLoggingManager;
    bool m_bGenerateLocalTrxId = false;
    StringAttr m_serviceScriptError;
    using MethodAccessMap = MapStringTo<SecAccessFlags>;
    using MethodAccessMaps = MapStringTo<Owned<MethodAccessMap> >;
    MethodAccessMaps            m_methodAccessMaps;
    StringBuffer                m_defaultFeatureAuth;
    MapStringTo<Owned<String> > m_explicitNamespaces;
    Owned<ITxSummaryProfile>    m_txSummaryProfile;
    Owned<IDataMaskingEngine>   m_oDynamicMaskingEngine;
    Owned<IDataMaskingEngine>   m_oStaticMaskingEngine;

#ifndef LINK_STATICALLY
    Owned<ILoadedDllEntry> javaPluginDll;
#endif
    Owned<IEmbedContext> javaplugin;

public:
    StringBuffer                m_espServiceType;
    StringBuffer                m_espServiceName;
    StringBuffer                m_espProcName;
    Owned<IPropertyTree>        m_oEspBindingCfg;
    Owned<IPropertyTree>        m_pServiceMethodTargets;
    Owned<IEsdlTransformer>     m_pEsdlTransformer;
    Owned<IEsdlDefinition>      m_esdl;
    StringBuffer                m_serviceNameSpaceBase;
    StringAttr                  m_namespaceScheme;
    bool                        m_usesURLNameSpace{false};
    bool                        m_returnSchemaLocationOnOK{false};

    using TransformErrorMap = MapStringTo<StringAttr, const char *>;
    TransformErrorMap m_methodScriptErrors;

public:
    IMPLEMENT_IINTERFACE;
    EsdlServiceImpl()
    {
    }

    virtual ~EsdlServiceImpl();

    virtual const char * getServiceType()
    {
        return m_espServiceType.str();
    }
    IEmbedContext &ensureJavaEmbeded()
    {
        if (!javaplugin)
        {
#ifdef LINK_STATICALLY  // May be useful for debugging...
            javaplugin.setown(javaembed::getEmbedContext());
#else
            javaPluginDll.setown(createDllEntry("javaembed", false, NULL, false));
            if (!javaPluginDll)
                throw makeStringException(0, "Failed to load javaembed plugin");
            GetEmbedContextFunction pf = (GetEmbedContextFunction) javaPluginDll->getEntry("getEmbedContextDynamic");
            if (!pf)
                throw makeStringException(0, "Failed to load javaembed plugin");
            javaplugin.setown(pf());
#endif
            }
        return *javaplugin;
    }

    virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
    {
        init(cfg, process, name);
        return true;
    }

    virtual void setContainer(IEspContainer * container_)
    {
        container = container_;
    }

    virtual void setEsdlTransformer(IEsdlTransformer * transformer)
    {
        m_pEsdlTransformer.setown(transformer);
    }

    virtual void clearDESDLState()
    {
        if(m_pEsdlTransformer)
            m_pEsdlTransformer.clear();
        if(m_pServiceMethodTargets)
            m_pServiceMethodTargets.clear();
        m_methodScriptErrors.kill();
    }

    virtual bool loadLoggingManager(Owned<ILoggingManager>& manager, IPTree* configuration);
    virtual void init(const IPropertyTree *cfg, const char *process, const char *service);
    virtual void configureTargets(IPropertyTree *cfg, const char *service);
    virtual void configureLogging(IPropertyTree *cfg);
    virtual void configureMasking(IPropertyTree *cfg);
    void configureJavaMethod(const char *method, IPropertyTree &entry, const char *classPath);
    void configureCppMethod(const char *method, IPropertyTree &entry, IEspPlugin*& plugin);
    void configureUrlMethod(const char *method, IPropertyTree &entry);
    String* getExplicitNamespace(const char* method) const;

    void handleTransformError(StringAttr &serviceError, TransformErrorMap &methodErrors, IException *e, const char *service, const char *method);
    void addTransforms(IPropertyTree *cfgParent, const char *service, const char *method, bool removeCfgIEntries);

    IEsdlScriptContext* checkCreateEsdlServiceScriptContext(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *origReq);
    void runPostEsdlScript(IEspContext &context, IEsdlScriptContext *scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer &out, unsigned txResultFlags, const char *ns, const char *schema_location);
    void runServiceScript(IEspContext &context, IEsdlScriptContext *scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *reqcontent, StringBuffer &out, unsigned txResultFlags, const char *ns, const char *schema_location);

    virtual void handleServiceRequest(IEspContext &context, Owned<IEsdlScriptContext> &scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, const char *ns, const char *schema_location, IPropertyTree *req, StringBuffer &out, StringBuffer &logdata, StringBuffer &origResp, StringBuffer &soapmsg, unsigned int flags);
    virtual void generateTransactionId(IEspContext & context, StringBuffer & trxid)=0;
    void generateTargetURL(IEspContext & context, IPropertyTree *srvinfo, StringBuffer & url, bool isproxy);
    void sendTargetSOAP(IEspContext & context, IPropertyTree *srvinfo, const char * req, StringBuffer &resp, bool isproxy,const char * targeturl);
    virtual IPropertyTree *createTargetContext(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt){return NULL;}
    virtual IPropertyTree *createInsContextForRoxieTest(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt){return NULL;}
    virtual void getTargetResponseFile(IEspContext & context, IPropertyTree *srvinfo, const char * req, StringBuffer &resp);
    virtual void esdl_log(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *tgtctx, IPropertyTree *req_pt, const char *xmlresp, const char *logdata, unsigned int timetaken){}
    virtual void processHeaders(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer &req, StringBuffer &headers){};
    virtual void processRequest(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer &req) {};
    void prepareFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, bool isroxie, const char* ns, StringBuffer &reqcontent, StringBuffer &reqProcessed);
    virtual void createServersList(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer &servers) {};
    virtual bool handleResultLogging(IEspContext &espcontext, IEsdlScriptContext *scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree * reqcontext, IPropertyTree * request,  const char * rawreq, const char * rawresp, const char * finalresp, const char * logdata);
    void handleEchoTest(const char *mthName, IPropertyTree *req, StringBuffer &soapResp, ESPSerializationFormat format);
    void handlePingRequest(const char *srvName, StringBuffer &out, unsigned int flags);
    virtual void handleFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer& req, StringBuffer &out, bool isroxie, bool isproxy, StringBuffer &rawreq);
    void getSoapBody(StringBuffer& out,StringBuffer& soapresp);
    void getSoapError(StringBuffer& out,StringBuffer& soapresp,const char *,const char *);

    /**
     * @brief Adjust the contents of the target configuration on a per-transaction basis.
     *
     * If changes to the shared method configuration are required on a per-transaction basis,
     * replace the shared configuration with a copy that contains the updates. This must be
     * invoked before `createTargetContext` or `checkCreateEsdlServceScriptContext`, both of which
     * use the target configuration.
     *
     * Gateway elements may require updates on a per-transaction basis. Specifically, the use
     * of local secret references must be updated with each transaction because the secret
     * values may have changed since a previous use.
     *
     * @param tgtcfg the shared configuration on input; either the unchanged shared configuration
     *               or modified copy on output
     */
    void adjustTargetConfig(Owned<IPTree>& tgtcfg) const;

    virtual bool unsubscribeServiceFromDali() override {return true;}
    virtual bool subscribeServiceToDali() override {return false;}
    virtual bool attachServiceToDali() override {return false;}
    virtual bool detachServiceFromDali() override {return false;}

protected:
    static constexpr const char* gwLocalSecretPrefix = "local-secret:";
    static constexpr const size_t gwLocalSecretPrefixLength = strlen(gwLocalSecretPrefix);

    /**
     * @brief Possibly construct a new gateway URL value by resolving a given connection secret
     *        identifier.
     *
     * The gateway element is expected to contain a non-empty value for property `@url`. This value
     * must begin with gwLocalSecretPrefix and be followed by a secret identification in the form
     * of `[ vault-id ":" ] secret-name`. This is assumed to identify connection secret known only
     * to the ESP, exceptions will be thrown on failure:
     *
     * - `gateway` must contain property `@url`; and
     * - property '@url` must begin with gwLocalSecretPrefix; and
     * - property `@url` must include a secret name, and may include a vault identifier; and
     * - the optional vault identifier and required secret name combination must identify a secret
     *   in the `esp` category; and
     * - the secret must grant the requested `permission`, when given, by defining a Boolean
     *   property with the permission name set to true; and
     * - the secret must define a non-empty `url` property; and
     * - the secret must define either a non-empty `username` property or a Boolean property
     *   `insecure` set to to true; and
     * - the secret may define a `password` property only if a non-empty `username` property is
     *   also defined.
     *
     * Use of local secrets in gateway configurations is preferred to the use of inline URLs, but
     * is not considered a best practice. Although it does remove user credentials from the
     * configuration file, it increases exposure of the secret data and precludes the use of
     * secret-defined tokens and certificates for connection security.
     *
     * @param gateway    property tree that must contain at least a `@url` property
     * @param permission name of local secret property controlling use of the secret in a gateway
     */
    void resolveGatewayLocalSecret(IPTree& gateway, const char* permission) const;

    /**
     * @brief Possibly construct a new gateway URL value by removing embedded user credentials and
     *        inserting separately configured values.
     *
     * The gateway element must contain an `@url` property, and may contain `@username` and
     * `@password` properties. A password must not be specified in the absence of a username.
     *
     * Use of inline URLs is strongly discouraged. Secure connections are not possible without
     * including user credentials in the configuration. Support is provided for backward
     * compatibility purposes only.
     *
     * Support for a `@roxieClient` gateway property is obsolete. When used with gateway
     * configurations, its effect would be to either require both username and password or prevent
     * the use of inline credentials. DESDL configurations should define credentials when needed,
     * and omit them when not. 
     *
     * @param gateway property tree that should contain at least a `@url` property
     */
    void resolveGatewayInlineURL(IPTree &gateway) const;

    /**
     * @brief Helper function to assemble a URL with optional inline user credentials.
     * 
     * @param url      a base URL on input; a possibly modified URL on output
     * @param username optional user identifier
     * @param password optional user password
     */
    void updateURLCredentials(StringBuffer& url, const char* username, const char* password) const;

    /**
     * @brief Implementation of legacy gateway transformation invoked only during preparation of
     *        published requests.
     *
     * This can be deprecated once scripts can access resolved URL values.
     *
     * @param srvcfg         the target configuration containing all gateways
     * @param forRoxie       the transformed gateway structure
     * @param altElementName configurable element name used in the transformed gateway structure
     */
    void transformGatewaysConfig( IPropertyTree* srvcfg, IPropertyTree* forRoxie, const char* altElementName = nullptr ) const;

private:
    bool initMaskingEngineDirectory(const char* dir);
    template <typename file_loader_t>
    bool initMaskingEngineDirectory(const char* dir, const char* mask, file_loader_t loader);
    bool initMaskingEngineEmbedded(Owned<IDataMaskingEngine>& engine, const IPropertyTree* ptree, bool required);
};

#define DEFAULT_ESDLBINDING_URN_BASE "urn:hpccsystems:ws"
#define SCAPPS_NAMESPACE_BASE "http://webservices.seisint.com/"

#define NAMESPACE_SCHEME_CONFIG_VALUE_SCAPPS "scapps"

class EsdlBindingImpl : public CHttpSoapBinding
{
private:
    Owned<IPropertyTree>                    m_bndCfg;
    Owned<IPropertyTree>                    m_esdlBndCfg;
    Owned<IPropertyTree>                    m_proxyInfo;

    Owned<IEsdlDefinition>                  m_esdl;
    Owned<IEsdlDefinitionHelper>            m_xsdgen;

    StringArray                             m_esdlDefinitions;
    StringAttr                              m_bindingName;
    StringAttr                              m_processName;
    StringAttr                              m_espServiceName; //previously held the esdl service name, we are now
                                                              //supporting mismatched ESP Service name assigned to a different named ESDL service definition
    Owned<IEsdlStore>                       m_pCentralStore;
    CriticalSection                         configurationLoadCritSec;
    CriticalSection                         detachCritSec;
    StringBuffer                            m_esdlStateFilesLocation;
    bool                                    m_isAttached;
    StringAttr                              m_bindingId;

    virtual void clearDESDLState()
    {
        if (m_esdl)
            m_esdl.clear();
        if(m_esdlBndCfg)
            m_esdlBndCfg.clear();
        if (m_pESDLService)
            m_pESDLService->clearDESDLState();
        if (m_proxyInfo)
            m_proxyInfo.clear();

        //prob need to un-initesdlservinfo as well.
        ESPLOG(LogNormal, "Warning binding %s.%s is being un-loaded!", m_processName.get(), m_bindingName.get());
    }

public:
    EsdlServiceImpl * m_pESDLService;
    IMPLEMENT_IINTERFACE;

    EsdlBindingImpl();
    EsdlBindingImpl(IPropertyTree* cfg, IPropertyTree *esdlArchive, const char *bindname=NULL, const char *procname=NULL);

    virtual ~EsdlBindingImpl()
    {
    }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response);

    bool checkForMethodProxy(const char *service, const char *method, StringBuffer &address, bool &resetForwardedFor);
    int forwardProxyMessage(const char *addr, CHttpRequest* request, CHttpResponse* response, bool resetForwardedFor);

    virtual void initEsdlServiceInfo(IEsdlDefService &srvdef);

    void addService(IPropertyTree *esdlArchive, const char * name, const char * host, unsigned short port, IEspService & service);

    int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response,    const char *serviceName, const char *methodName);
    int HandleSoapRequest(CHttpRequest* request, CHttpResponse* response);

    void handleHttpPost(CHttpRequest *request, CHttpResponse *response);
    void handleJSONPost(CHttpRequest *request, CHttpResponse *response);

    int onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);
    virtual bool getSchema(StringBuffer& schema, IEspContext &ctx, CHttpRequest* req, const char *service, const char *method, bool standalone) override;
    int onGetWsdl(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);
    int onJavaPlugin(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);

    int onGetXForm(IEspContext &context, CHttpRequest* request,   CHttpResponse* response, const char *serv, const char *method);
    virtual int onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *pathex);
    int getJsonTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response);
    void getRequestContent(IEspContext &context, StringBuffer & req, CHttpRequest* request, const char * servicename, const char * methodname, const char *ns, unsigned flags);
    void setXslProcessor(IInterface *xslp){}

    int getMethodProperty(IEspContext &context, const char *serv, const char *method, StringBuffer &page, const char *propname, const char *dfault);

    virtual int getMethodDescription(IEspContext &context, const char *serv, const char *method, StringBuffer &page) override;
    virtual int getMethodHelp(IEspContext &context, const char *serv, const char *method, StringBuffer &page) override;

    int getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods);

    StringBuffer & getServiceName(StringBuffer & resp)
    {
        return resp.append( m_pESDLService->getServiceType());
    }
    bool isValidServiceName(IEspContext &context, const char *name)
    {
        return strieq(name,  m_pESDLService->getServiceType());
    }
    bool qualifyServiceName(IEspContext &context, const char *servname, const char *methname, StringBuffer &servQName, StringBuffer *methQName);
    virtual bool qualifyMethodName(IEspContext &context, const char *methname, StringBuffer *methQName);
    void getSoapMessage(StringBuffer& soapmsg, IEspContext& ctx, CHttpRequest* request, const char *serv, const char *method);
    virtual const char *queryServiceType()=0;
    virtual StringBuffer &generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &ns);
    virtual int onGetReqSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);
    virtual int onGetRespSampleXml(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);
    virtual void handleSoapRequestException(IException *e, const char *source);

    int onGetSampleXml(bool isRequest, IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);
    static void splitURLList(const char* urlList, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password, StringBuffer& ipportlistbody, StringBuffer& path, StringBuffer& options);

    bool usesESDLDefinition(const char * name, int version);
    bool usesESDLDefinition(const char * id);
    virtual bool isDynamicBinding() const override { return true; }
    virtual bool isBound() const override { return (m_esdlBndCfg.get() != nullptr); }
    virtual unsigned getCacheMethodCount(){return 0;}
    bool reloadBindingFromCentralStore(const char* bindingId);
    bool reloadDefinitionsFromCentralStore(IPropertyTree * esdlBndCng, StringBuffer & loadedname);
    void configureProxies(IPropertyTree *cfg, const char *service);

    void clearBindingState();

    virtual bool subscribeBindingToDali() override
    {
        CriticalBlock b(detachCritSec);
        if (!m_pCentralStore)
            return false;
        if(m_isAttached)
            return true;
        m_pCentralStore->attachToBackend();
        queryEsdlMonitor()->subscribe();
        m_isAttached = true;
        if(m_bindingId.length() != 0)
        {
            ESPLOG(LogNormal, "Requesting reload of ESDL binding %s...", m_bindingId.get());
            reloadBindingFromCentralStore(m_bindingId.get());
        }
        return true;
    }

    virtual bool unsubscribeBindingFromDali() override
    {
        CriticalBlock b(detachCritSec);
        if (!m_pCentralStore)
            return true;
        if(!m_isAttached)
            return true;
        m_isAttached = false;
        m_pCentralStore->detachFromBackend();
        queryEsdlMonitor()->unsubscribe();
        return true;
    }

    bool detachBindingFromDali() override
    {
        return unsubscribeBindingFromDali();
    }

    virtual bool canDetachFromDali() override
    {
        return true;
    }

    void setIsAttached(bool isattached)
    {
        CriticalBlock b(detachCritSec);
        m_isAttached = isattached;
    }

    bool isAttached()
    {
        CriticalBlock b(detachCritSec);
        return m_isAttached;
    }

private:
    int onGetRoxieBuilder(CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);
    int onRoxieRequest(CHttpRequest* request, CHttpResponse* response, const char *  method);
    void getSoapMessage(StringBuffer& out,StringBuffer& soapresp,const char * starttxt,const char * endtxt);

    void saveDESDLState();
    IPropertyTree * fetchESDLBinding(const char *process, const char *bindingName, const char * stateFileName);
    bool loadStoredDefinitions(const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName, const char * stateFileName);
    bool loadLocalDefinitions(IPropertyTree *esdlArchive, const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName);

};

#endif //_EsdlBinding_HPP__
