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

static const char* ESDL_DEFS_ROOT_PATH="/ESDL/Definitions/";
static const char* ESDL_DEF_PATH="/ESDL/Definitions/Definition";
static const char* ESDL_DEF_ENTRY="Definition";

static const char* ESDL_BINDINGS_ROOT_PATH="/ESDL/Bindings/";
static const char* ESDL_BINDING_PATH="/ESDL/Bindings/Binding";
static const char* ESDL_BINDING_ENTRY="Binding";
static const char* ESDL_METHOD_DESCRIPTION="description";
static const char* ESDL_METHOD_HELP="help";

#define SDS_LOCK_TIMEOUT (30*1000) // 5mins, 30s a bit short

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

class EsdlServiceImpl : public CInterface, implements IEspService
{
private:
    IEspContainer *container;
    MapStringToMyClass<ISmartSocketFactory> connMap;
    MapStringToMyClass<IEmbedServiceContext> javaServiceMap;
    Owned<ILoggingManager> loggingManager;
#ifndef LINK_STATICALLY
    Owned<ILoadedDllEntry> javaPluginDll;
#endif
    Owned<IEmbedContext> javaplugin;


public:
    StringBuffer                m_espServiceType;
    StringBuffer                m_espServiceName;
    StringBuffer                m_espProcName;
    Owned<IPropertyTree>        m_pServiceConfig;
    Owned<IPropertyTree>        m_pServiceMethodTargets;
    Owned<IEsdlTransformer>     m_pEsdlTransformer;
    Owned<IEsdlDefinition>      m_esdl;

public:
    IMPLEMENT_IINTERFACE;
    EsdlServiceImpl()  {}

    ~EsdlServiceImpl();

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
            javaPluginDll.setown(createDllEntry("javaembed", false, NULL));
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
    }

    virtual bool loadLogggingManager();
    virtual void init(const IPropertyTree *cfg, const char *process, const char *service);
    virtual void configureTargets(IPropertyTree *cfg, const char *service);
    void configureJavaMethod(const char *method, IPropertyTree &entry, const char *classPath);
    void configureUrlMethod(const char *method, IPropertyTree &entry);

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
    virtual void createServersList(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, StringBuffer &servers) {};
    virtual bool handleResultLogging(IEspContext &espcontext, IPropertyTree * reqcontext, IPropertyTree * request,  const char * rawresp, const char * finalresp);
    void handleEchoTest(const char *mthName, IPropertyTree *req, StringBuffer &soapResp, unsigned flags=0);
    virtual void handleFinalRequest(IEspContext &context, Owned<IPropertyTree> &tgtcfg, Owned<IPropertyTree> &tgtctx, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, const char *ns, StringBuffer& req, StringBuffer &out, bool isroxie, bool isproxy);
    void getSoapBody(StringBuffer& out,StringBuffer& soapresp);
    void getSoapError(StringBuffer& out,StringBuffer& soapresp,const char *,const char *);
};

//RODRIGO: BASE URN should be configurable.
#define ESDLBINDING_URN_BASE "urn:hpccsystems:ws"

class EsdlBindingImpl : public CHttpSoapBinding
{
private:
    //==========================================================================================
    // the following class implements notification handler for subscription to dali for environment
    // updates by other clients.
    //==========================================================================================
    class CESDLBindingSubscription : public CInterface, implements ISDSSubscription
    {
    private :
        CriticalSection daliSubscriptionCritSec;
        SubscriptionId sub_id;
        EsdlBindingImpl * thisBinding;

    public:
        CESDLBindingSubscription(EsdlBindingImpl * binding)
        {
            thisBinding = binding;
            VStringBuffer fullBindingPath("/ESDL/Bindings/Binding[@id=\'%s.%s\']",thisBinding->m_processName.get(),thisBinding->m_bindingName.get());
            CriticalBlock b(daliSubscriptionCritSec);
            try
            {
                sub_id = querySDS().subscribe(fullBindingPath.str(), *this, true);
            }
            catch (IException *E)
            {
                // failure to subscribe implies dali is down... is this ok??
                // Is this bad enough to halt the esp load process??
                E->Release();
            }
        }

        virtual ~CESDLBindingSubscription()
        {
            unsubscribe();
        }

        void unsubscribe()
        {
            CriticalBlock b(daliSubscriptionCritSec);
            try
            {
                if (sub_id)
                {
                    querySDS().unsubscribe(sub_id);
                    sub_id = 0;
                }
            }
            catch (IException *E)
            {
                E->Release();
            }
            sub_id = 0;
        }

        IMPLEMENT_IINTERFACE;
        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL);
    };

    class CESDLDefinitionSubscription : public CInterface, implements ISDSSubscription
    {
    private :
        CriticalSection daliSubscriptionCritSec;
        SubscriptionId sub_id;
        EsdlBindingImpl * thisBinding;

        public:
        CESDLDefinitionSubscription(EsdlBindingImpl * binding)
        {
            thisBinding = binding;
            //for some reason subscriptions based on xpaths with attributes don't seem to work correctly
            //fullBindingPath.set("/ESDL/Bindings/Binding[@EspBinding=\'WsAccurint\'][@EspProcess=\'myesp\']");

            CriticalBlock b(daliSubscriptionCritSec);
            try
            {
                sub_id = querySDS().subscribe(ESDL_DEF_PATH, *this, true);
            }
            catch (IException *E)
            {
                // failure to subscribe implies dali is down... is this ok??
                // Is this bad enough to halt the esp load process??
                E->Release();
            }
        }

        virtual ~CESDLDefinitionSubscription()
        {
            unsubscribe();
        }

        void unsubscribe()
        {
            CriticalBlock b(daliSubscriptionCritSec);
            try
            {
                if (sub_id)
                {
                    querySDS().unsubscribe(sub_id);
                    sub_id = 0;
                }
            }
            catch (IException *E)
            {
                E->Release();
            }
            sub_id = 0;
        }

        IMPLEMENT_IINTERFACE;
        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL);
    };

    Owned<IPropertyTree>                    m_bndCfg;
    Owned<IPropertyTree>                    m_esdlBndCfg;

    Owned<IEsdlDefinition>                  m_esdl;
    Owned<IEsdlDefinitionHelper>            m_xsdgen;

    StringArray                             m_esdlDefinitions;
    StringAttr                              m_bindingName;
    StringAttr                              m_processName;
    StringAttr                              m_espServiceName; //previously held the esdl service name, we are now
                                                              //supporting mismatched ESP Service name assigned to a different named ESDL service definition
    Owned<CESDLBindingSubscription>         m_pBindingSubscription;
    Owned<CESDLDefinitionSubscription>      m_pDefinitionSubscription;
    CriticalSection                         configurationLoadCritSec;

    virtual void clearDESDLState()
    {
        if (m_esdl)
            m_esdl.clear();
        if(m_esdlBndCfg)
            m_esdlBndCfg.clear();
        if (m_pESDLService)
            m_pESDLService->clearDESDLState();
        //prob need to un-initesdlservinfo as well.
        DBGLOG("Warning binding %s.%s is being un-loaded!", m_processName.get(), m_bindingName.get());
    }

public:
    EsdlServiceImpl * m_pESDLService;
    //CIArrayOf<EsdlServiceImpl> m_esdlServices;
    IMPLEMENT_IINTERFACE;

    EsdlBindingImpl();
    EsdlBindingImpl(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL);

    ~EsdlBindingImpl()
    {
    }

    virtual int onGet(CHttpRequest* request, CHttpResponse* response);

    virtual void initEsdlServiceInfo(IEsdlDefService &srvdef);

    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service);

    int onGetInstantQuery(IEspContext &context, CHttpRequest* request, CHttpResponse* response,    const char *serviceName, const char *methodName);
    int HandleSoapRequest(CHttpRequest* request, CHttpResponse* response);

    void handleHttpPost(CHttpRequest *request, CHttpResponse *response);
    void handleJSONPost(CHttpRequest *request, CHttpResponse *response);

    int onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);
    virtual bool getSchema(StringBuffer& schema, IEspContext &ctx, CHttpRequest* req, const char *service, const char *method, bool standalone);
    int onGetWsdl(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);
    int onJavaPlugin(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *serviceName, const char *methodName);

    int onGetXForm(IEspContext &context, CHttpRequest* request,   CHttpResponse* response, const char *serv, const char *method);
    virtual int onGetFile(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *pathex);
    int getJsonTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response);
    void getRequestContent(IEspContext &context, StringBuffer & req, CHttpRequest* request, const char * servicename, const char * methodname, const char *ns, unsigned flags);
    void setXslProcessor(IInterface *xslp){}

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
    static void transformGatewaysConfig( IPropertyTree* srvcfg, IPropertyTree* forRoxie );
    static bool makeURL( StringBuffer& url, IPropertyTree& cfg );

    bool usesESDLDefinition(const char * name, int version);
    bool usesESDLDefinition(const char * id);

private:
    int onGetRoxieBuilder(CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method);
    int onRoxieRequest(CHttpRequest* request, CHttpResponse* response, const char *  method);
    bool getRoxieConfig(StringBuffer & queryName, StringBuffer & url, StringBuffer & username, StringBuffer & password, const char *method);
    void getSoapMessage(StringBuffer& out,StringBuffer& soapresp,const char * starttxt,const char * endtxt);

    bool reloadBinding(const char *binding, const char *process);
    bool reloadDefinitions(IPropertyTree * esdlBndCng);
};

#endif //_EsdlBinding_HPP__
