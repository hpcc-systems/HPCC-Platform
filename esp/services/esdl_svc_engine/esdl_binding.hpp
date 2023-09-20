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
    /**
     * @brief Cache of secrets accessed as part of transaction processing.
     *
     * In the context of a single transaction, multiple requests for the same secret should always
     * return the same secret data for each request. Cached data does not expire and there is no
     * option to reload data.
     *
     * TODO: Refactor to support additional use cases
     *       - Provide ESDL script operation support so mysql (now) and http-post-xml (eventually)
     *         can benefit secret reuse.
     *       - Encapsulate non-jsecrets access to support non-standard secret sources, unless
     *         jsecrets is changed to provide this encapsulation.
     */
    class TransactionSecrets
    {
    private:
        using Key = std::tuple<std::string, std::string, std::string>;
        using Cache = std::map<Key, Owned<IPTree>>;
        Cache cache;
    public:
        IPTree* getVaultSecret(const char* category, const char* vaultId, const char* name);
        IPTree* getSecret(const char* category, const char* name);
    protected:
        IPTree* lookup(const char* category, const char* vaultId, const char* name) const;
        void store(IPTree& secret, const char* category, const char* vaultId, const char* name);
    };

private:
    /**
     * @brief Abstraction of a per-transaction gateway updater.
     *
     * During service load, the method configurations are parsed for Gateways/Gateway elements.
     * Matching elements containing both `@name` and `@url` properties may be updated when
     * preparing backend service requests. Updates may inject user credentials into a configured
     * URL or assemble a new URL from a secret.
     *
     * Each instance contains the information and logic to update a single Gateway property tree
     * as needed. Only one instance per gateay should be created per transaction. No instance
     * should be created for gateways that do not require updates.
     */
    interface IGatewayUpdater : public IInterface
    {
        virtual void updateGateway(IPTree& gw, const char* requiredUsage) = 0;
    };
    using GatewayUpdaters = std::map<std::string, Owned<IGatewayUpdater>>;

    /**
     * @brief Abstraction of a per-service-method gateway update handler.
     *
     * During service load, the method configurations are parsed for Gateways/Gateway elements.
     * Matching elements containing both `@name` and `@url` properties may be updated when
     * preparing backend service requests. Updates may inject user credentials into a configured
     * URL or assemble a new URL from a secret.
     *
     * Implementations are expected to retain all gateway configuration values at load time to
     * enable an `IGatewayUpdater` instance to update a copy of the gateway element during
     * backend request preparation.
     *
     * - If the `Gateway` element is a self-contained gateway specification, the retained data
     *   may be the updated values. Inline URLs fit this description.
     * - If the `Gateway` element refers to external data that could change between service load
     *   and a transaction, the retained data must be sufficient to retrieve current values on
     *   demand. Local secrets fit this description.
     *
     * Implementations may assume that each gateway configuration will include both `@name` and
     *  `@url` properties. No other assumptions are valid.
     */
    interface IUpdatableGateway : public IInterface
    {
        virtual IGatewayUpdater* getUpdater(GatewayUpdaters& updaters, TransactionSecrets& secrets) const = 0;
    };
    using UpdatableGateways = std::map<std::string, Owned<IUpdatableGateway>>;

    /**
     * @brief Abstract extension of `IUpdatableGateway` with some standardized behaviors.
     *
     * - Subclasses supply the updater map key used to locate an existing updater or store a new
     *   updater. It is assumed the key given is the value of `Gateway/@name`. This value must not
     *   be null.
     * - An existing updater in a given map of updaters will always be reused before creating a
     *   new updater.
     * - Insertion (or replacement) of user credentials in a URL string is available to any
     *   subclass that needs it..
     *
     * All extensions must implement `getUpdater(TransactionSecrets&)`. In some cases this may entail creation
     * of a new updater instance. In other cases a single instance may be reused.
     */
    class CUpdatableGateway : public CInterfaceOf<IUpdatableGateway>
    {
    public:
        virtual IGatewayUpdater* getUpdater(GatewayUpdaters& updaters, TransactionSecrets& secrets) const override;
    protected:
        std::string updatersKey;
    protected:
        CUpdatableGateway(const char* gwName);
        virtual IGatewayUpdater* getUpdater(TransactionSecrets& secrets) const = 0;
        bool updateURLCredentials(StringBuffer& url, const char* username, const char* password) const;
    };
    
    /**
     * @brief Concrete extension of both `CUpdatableGateway` and `IGatewayUpdater` for handling
     *        legecy URL gateways
     *
     * - User credentials embedded in `Gateway/@url` are dropped.
     * - `Gateway/@username` may specify a user name to embed in an updated URL.
     * - If `Gateway/@username` is given, `Gateway/@password` may specify an exncrypted password
     *   value to be decrypted and embedded in an updated URL.
     * - It is an error to specify `Gateway/@password` without `Gateway/@username`.
     *
     * Support is provided for backward compatibility. Use of these secrets is strongly discouraged
     * and should be avoided whenever possible.
     */
    class CLegacyUrlGateway : public CUpdatableGateway, public IGatewayUpdater
    {
    protected: // CUpdatableGateway
        virtual CLegacyUrlGateway* getUpdater(TransactionSecrets&) const override;
    public: // IGatewayUpdater
        virtual void updateGateway(IPTree& gw, const char*) override;
    protected:
        StringBuffer url;
    public:
        IMPLEMENT_IINTERFACE_USING(CUpdatableGateway);
        CLegacyUrlGateway(const IPTree& gw, const char* gwName, const char* gwUrl);
    };
    
    /**
     * @brief Concrete extension of `CUpdatableGateway` for handling local secrets that is always
     *        extended with secret usage-specific logic.
     *
     * - Ensures `Gateway/@url` matches the pattern `"local-secret:" [ vault-id ":" ] secret-name".`
     * - Defines a standard updater separating dynamically changing secrets from the configuration.
     * - Enforces a requirement for a local secret to explicitly allow its data to be shared with
     *   a backend roxie service.
     *
     * Support is provided as an alternative to inline definitions when secret names cannot be
     * passed to the target roxie for resolution. Use is discouraged unless it is unavoidable.
     */
    class CLocalSecretGateway : public CUpdatableGateway
    {
    protected:
        class CUpdater : public CInterfaceOf<IGatewayUpdater>
        {
            friend class CLocalSecretGateway;
        public: // IGatewayUpdater
            virtual void updateGateway(IPTree& gw, const char* requiredUsage) override;
        protected:
            Linked<const CLocalSecretGateway> entry;
            Owned<IPTree> secret;
        public:
            CUpdater(const CLocalSecretGateway& _entry, TransactionSecrets& secrets);
        };
    protected: // CUpdatableGateway
        virtual IGatewayUpdater* getUpdater(TransactionSecrets& secrets) const override;
    protected:
        StringBuffer secretId;
        StringAttr   vaultId;
        StringBuffer secretName;
    protected:
        CLocalSecretGateway(const IPTree& gw, const char* gwName, const char* gwUrl, const char* classPrefix);
    protected:
        virtual void doUpdate(IPTree& gw, const IPTree& secret, const char* requiredUsage) const;
    };

    /**
     * @brief Concete extension of `CLocalSecretGateway` to construct a new `Gateway/@url` value
     *        from secret-defined values.
     *
     * - A secret name must begin with "http-connect-". Configurations should omit this prefix,
     *   but its presence is acceptable.
     * - A secret must satisfy the requirements defined in `CLocalSecretGateway`.
     * - A secret must define a non-empty `url` property.
     * - A secret must define either a non-empty `username` property or set the `omitCredentials`
     *   property to true.
     * - A secret may define a `password` property if `username` is also defined.
     * - A secret must not define `username` and set `omitCredentials` to true.
     * - A secret must not define a `password` property if `username` is not defined.
     */
    class CHttpConnectGateway : public CLocalSecretGateway
    {
    protected: // CLocalSecretGateway
        void doUpdate(IPTree& gw, const IPTree& secret, const char* requiredUsage) const override;
    public:
        CHttpConnectGateway(const IPTree& gw, const char* gwName, const char* gwUrl);
    };

    /**
     * @brief Storage of updateable gateway handlers for a single method.
     *
     * Gateway updates may occur in either or both of two locations within a backend
     * service request. The storage layout must enable support for both locations.
     *
     * 1. The optional inclusion of a method's binding definition that does not exclude defined
     *    gateways, also known as the target context. Target context inclusion must update all
     *    gateways referring to local secrets. Target context inclusion must not, for backward
     *    compatibility, update any inline gateways unless `Gateways/@updateInline` is true.
     * 2. The optional request for "legacy gateway transformation". Legacy transformations must
     *    update all local secret and inline gateways when `Gateways/@legacyTransformTarget` is
     *    not empty.
     */
    struct GatewaysCacheEntry
    {
        UpdatableGateways targetContext;
        UpdatableGateways legacyTransform;
    };
    using GatewaysCache = std::map<std::string, GatewaysCacheEntry>;

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
    GatewaysCache m_methodGatewaysCache;

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
    
    /**
     * @brief Assemble a SOAP request to be sent to a backend service.
     *
     * Assembly includes construction of an initial SOAP request and processing backend request
     * ESDL script entry point transforms, if any exist.
     *
     * Initial request construction of all backend requests wraps the given request content in a
     * SOAP envelope and SOAP body. For published requests, where `isroxie` is true, updatable
     * gateways are updated prior to inclusion with the `tgtctx` configuration and as part of
     * legacy gateway transformations.
     *
     * Support for inline URLs in the target configuration, and context which is a subset of the
     * configuration, is provided for backward compatibility. Use is strongly discouraged.
     *
     * Support for local secrets in the target configuration is offered for use cases where the
     * ESP cannot identify a connection secret known to the target service. It is an improvement
     * on inline URLs, but still involves risk by disclosing secret data.
     *
     * It is a best practice for all Gateay URLs to be defined in terms of secrets known to the
     * target service, eliminating the need for any additional data disclosures in the request.
     *
     * @param context 
     * @param scriptContext 
     * @param tgtcfg 
     * @param tgtctx 
     * @param srvdef 
     * @param mthdef 
     * @param isroxie 
     * @param ns 
     * @param reqcontent 
     * @param reqProcessed 
     */
    void prepareFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, bool isroxie, const char* ns, StringBuffer &reqcontent, StringBuffer &reqProcessed);
    virtual void createServersList(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer &servers) {};
    virtual bool handleResultLogging(IEspContext &espcontext, IEsdlScriptContext *scriptContext, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree * reqcontext, IPropertyTree * request,  const char * rawreq, const char * rawresp, const char * finalresp, const char * logdata);
    void handleEchoTest(const char *mthName, IPropertyTree *req, StringBuffer &soapResp, ESPSerializationFormat format);
    void handlePingRequest(const char *srvName, StringBuffer &out, unsigned int flags);
    virtual void handleFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer& req, StringBuffer &out, bool isroxie, bool isproxy, StringBuffer &rawreq);
    void getSoapBody(StringBuffer& out,StringBuffer& soapresp);
    void getSoapError(StringBuffer& out,StringBuffer& soapresp,const char *,const char *);

    virtual bool unsubscribeServiceFromDali() override {return true;}
    virtual bool subscribeServiceToDali() override {return false;}
    virtual bool attachServiceToDali() override {return false;}
    virtual bool detachServiceFromDali() override {return false;}

protected:
    static constexpr const char* gwLocalSecretPrefix = "local-secret:";
    static constexpr const size_t gwLocalSecretPrefixLength = strlen(gwLocalSecretPrefix);

    /**
     * @brief Factory method to create an updatable gateway handler for a gateway referencing a
     *        local secret.
     * 
     * @param gw 
     * @param gwName 
     * @param gwUrl 
     * @return IUpdatableGateway* 
     */
    IUpdatableGateway* createLocalSecretGateway(const IPTree& gw, const char* gwName, const char* gwUrl) const;

    /**
     * @brief Factory method to create an updatable gateway handler for a gateway defined inline.
     * 
     * @param gw 
     * @param gwName 
     * @param gwUrl 
     * @return IUpdatableGateway* 
     */
    IUpdatableGateway* createInlineGateway(const IPTree& gw, const char* gwName, const char* gwUrl) const;

    /**
     * @brief Update all iterated nodes, as needed.
     * 
     * @param gwIt       property tree nodes, presumed to be `Gateway` nodes, for possible updates
     * @param updatables set of update handlers to be applied to nodes
     * @param updaters   cache of updaters used in the current transaction
     * @param secrets    cache of secrets used in the current transaction
     */
    void applyGatewayUpdates(IPTreeIterator& gwIt, const UpdatableGateways& updatables, GatewayUpdaters& updaters, TransactionSecrets& secrets) const;

    /**
     * @brief Implementation of legacy gateway transformation invoked only during preparation of
     *        published requests.
     *
     * @param inputs         iterator of all gateways
     * @param forRoxie       the transformed gateway structure
     * @param altElementName configurable element name used in the transformed gateway structure
     */
    void transformGatewaysConfig( IPTreeIterator* inputs, IPropertyTree* forRoxie, const char* altElementName = nullptr ) const;

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
