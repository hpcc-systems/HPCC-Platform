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
    IEspContainer *container;
    MapStringToMyClass<ISmartSocketFactory> connMap;
    MapStringToMyClass<IEmbedServiceContext> javaServiceMap;
    MapStringToMyClass<IException> javaExceptionMap;
    MapStringToMyClass<IEspPlugin> cppPluginMap;
    MapStringTo<cpp_service_method_t, cpp_service_method_t> cppProcMap;
    Owned<ILoggingManager> m_oLoggingManager;
    bool m_bGenerateLocalTrxId;
    MapStringToMyClass<IEsdlCustomTransform> m_customRequestTransformMap;
    Owned<IEsdlCustomTransform> m_serviceLevelRequestTransform;
    bool m_serviceLevelCrtFail = false;
    using MethodAccessMap = MapStringTo<SecAccessFlags>;
    using MethodAccessMaps = MapStringTo<Owned<MethodAccessMap> >;
    MethodAccessMaps            m_methodAccessMaps;
    StringBuffer                m_defaultFeatureAuth;

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
    bool                        m_usesURLNameSpace;
    MapStringTo<StringAttr, const char *> m_methodCRTransformErrors;

public:
    IMPLEMENT_IINTERFACE;
    EsdlServiceImpl(){}

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
        m_methodCRTransformErrors.kill();
    }

    virtual bool loadLogggingManager();
    virtual void init(const IPropertyTree *cfg, const char *process, const char *service);
    virtual void configureTargets(IPropertyTree *cfg, const char *service);
    void configureJavaMethod(const char *method, IPropertyTree &entry, const char *classPath);
    void configureCppMethod(const char *method, IPropertyTree &entry, IEspPlugin*& plugin);
    void configureUrlMethod(const char *method, IPropertyTree &entry);
    void addServiceLevelRequestTransform(IPropertyTree *customRequestTransform);
    void addMethodLevelRequestTransform(const char *method, IPropertyTree &methodCfg, IPropertyTree *customRequestTransform);

    IEsdlScriptContext* checkCreateEsdlServiceScriptContext(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg);

    virtual void handleServiceRequest(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, const char *ns, const char *schema_location, IPropertyTree *req, StringBuffer &out, StringBuffer &logdata, unsigned int flags);
    virtual void generateTransactionId(IEspContext & context, StringBuffer & trxid)=0;
    void generateTargetURL(IEspContext & context, IPropertyTree *srvinfo, StringBuffer & url, bool isproxy);
    void sendTargetSOAP(IEspContext & context, IPropertyTree *srvinfo, const char * req, StringBuffer &resp, bool isproxy,const char * targeturl);
    virtual IPropertyTree *createTargetContext(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt){return NULL;}
    virtual IPropertyTree *createInsContextForRoxieTest(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt){return NULL;}
    virtual void getTargetResponseFile(IEspContext & context, IPropertyTree *srvinfo, const char * req, StringBuffer &resp);
    virtual void esdl_log(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *tgtctx, IPropertyTree *req_pt, const char *xmlresp, const char *logdata, unsigned int timetaken){}
    virtual void processHeaders(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer &req, StringBuffer &headers){};
    virtual void processRequest(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer &req) {};
    virtual void processResponse(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer &resp) {};
    void prepareFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, bool isroxie, const char* ns, StringBuffer &reqcontent, StringBuffer &reqProcessed);
    virtual void createServersList(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer &servers) {};
    virtual bool handleResultLogging(IEspContext &espcontext, IEsdlScriptContext *scriptContext, IPropertyTree * reqcontext, IPropertyTree * request,  const char * rawreq, const char * rawresp, const char * finalresp, const char * logdata);
    void handleEchoTest(const char *mthName, IPropertyTree *req, StringBuffer &soapResp, ESPSerializationFormat format);
    void handlePingRequest(const char *srvName, StringBuffer &out, unsigned int flags);
    virtual void handleFinalRequest(IEspContext &context, IEsdlScriptContext *scriptContext, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer& req, StringBuffer &out, bool isroxie, bool isproxy, StringBuffer &rawreq);
    void getSoapBody(StringBuffer& out,StringBuffer& soapresp);
    void getSoapError(StringBuffer& out,StringBuffer& soapresp,const char *,const char *);

    virtual bool unsubscribeServiceFromDali() override {return true;}
    virtual bool subscribeServiceToDali() override {return false;}
    virtual bool attachServiceToDali() override {return false;}
    virtual bool detachServiceFromDali() override {return false;}
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
    StringBuffer                            m_staticNamespace;
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
    static void transformGatewaysConfig( IPropertyTree* srvcfg, IPropertyTree* forRoxie, const char* altElementName = nullptr );
    static bool makeURL( StringBuffer& url, IPropertyTree& cfg );

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
    bool loadDefinitions(const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName, const char * stateFileName);
    bool loadLocalDefinitions(IPropertyTree *esdlArchive, const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName);

};

#endif //_EsdlBinding_HPP__
