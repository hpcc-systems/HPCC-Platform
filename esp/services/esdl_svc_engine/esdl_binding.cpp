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
#include "build-config.h"
#include "jsmartsock.ipp"
#include "esdl_monitor.hpp"

#include "loggingagentbase.hpp"
#include "httpclient.ipp"

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

bool EsdlServiceImpl::loadLogggingManager()
{
    if (!m_oLoggingManager)
    {
        StringBuffer realName;
        realName.append(SharedObjectPrefix).append(LOGGINGMANAGERLIB).append(SharedObjectExtension);

        HINSTANCE loggingManagerLib = LoadSharedObject(realName.str(), true, false);

        if(loggingManagerLib == NULL)
        {
            ESPLOG(LogNormal,"ESP service %s: cannot load logging manager library(%s)", m_espServiceName.str(), realName.str());
            return false;
        }

        newLoggingManager_t_ xproc = NULL;
        xproc = (newLoggingManager_t_)GetSharedProcedure(loggingManagerLib, "newLoggingManager");

        if (!xproc)
        {
            ESPLOG(LogNormal,"ESP service %s: procedure newLogggingManager of %s can't be loaded\n", m_espServiceName.str(), realName.str());
            return false;
        }

        m_oLoggingManager.setown((ILoggingManager*) xproc());
    }

    return true;
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

        IPropertyTree* loggingConfig = srvcfg->queryPropTree("LoggingManager");
        if (loggingConfig)
        {
            ESPLOG(LogMin, "ESP Service %s attempting to load configured logging manager.", service);
            if (loadLogggingManager())
            {
                m_oLoggingManager->init(loggingConfig, service);
                m_bGenerateLocalTrxId = false;
            }
            else
                throw MakeStringException(-1, "ESDL Service %s could not load logging manager", service);
        }
        else
            ESPLOG(LogNormal, "ESP Service %s is not attached to any logging manager.", service);

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

        m_usesURLNameSpace = startsWith(m_serviceNameSpaceBase.str(), "http://") ? true : false;

        xpath.setf("EspBinding[@service=\"%s\"]", service); //get this service's binding cfg
        m_oEspBindingCfg.set(espcfg->queryPropTree(xpath.str()));
    }
    else
        throw MakeStringException(-1, "Could not access ESDL service configuration: esp process '%s' service name '%s'", process, service);
}

void EsdlServiceImpl::configureJavaMethod(const char *method, IPropertyTree &entry, const char *classPath)
{
    const char *javaScopedMethod = entry.queryProp("@javamethod");
    if (!javaScopedMethod || !*javaScopedMethod)
    {
        DBGLOG("ESDL binding - found java target method \"%s\" without java method defined.", method);
        return;
    }

    StringArray javaNodes;
    javaNodes.appendList(javaScopedMethod, ".");
    if (javaNodes.length()!=3) //adf: may become more flexible?
    {
        DBGLOG("ESDL binding - target method \"%s\", configured java method currently must be of the form 'package.class.method', found (%s).", method, javaScopedMethod);
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
                DBGLOG("ESDL binding - failed to load java class %s for target method %s", javaScopedClass.str(), method);
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

void EsdlServiceImpl::configureUrlMethod(const char *method, IPropertyTree &entry)
{
    const char *url = entry.queryProp("@url");
    if (!url || !*url)
    {
        DBGLOG("ESDL binding - found target method \"%s\" without target url!", method);
        return;
    }

    if (!entry.hasProp("@queryname"))
    {
        DBGLOG("ESDL binding - found target method \"%s\" without target query!", method);
        return;
    }

    StringBuffer protocol, name, pw, path, iplist, ops;
    EsdlBindingImpl::splitURLList(url, protocol, name, pw, iplist, path, ops);

    entry.setProp("@prot", protocol);
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

void EsdlServiceImpl::addServiceLevelRequestTransform(IPropertyTree *customRequestTransform)
{
    if (!customRequestTransform)
        return;

    try
    {
        m_serviceLevelRequestTransform.setown(new CEsdlCustomTransform(*customRequestTransform));
    }
    catch(IException* e)
    {
        m_serviceLevelCrtFail = true;
        StringBuffer msg;
        e->errorMessage(msg);
        ERRLOG("Service Level Custom Request Transform could not be processed!!: \n\t%s", msg.str());
        e->Release();
    }
    catch (...)
    {
        m_serviceLevelCrtFail = true;
        ERRLOG("Service Level Custom Request Transform could not be processed!!");
    }
}

void EsdlServiceImpl::addMethodLevelRequestTransform(const char *method, IPropertyTree &methodCfg, IPropertyTree *customRequestTransform)
{
    if (!method || !*method || !customRequestTransform)
        return;

    try
    {
        Owned<CEsdlCustomTransform> crt = new CEsdlCustomTransform(*customRequestTransform);
        m_customRequestTransformMap.setValue(method, crt.get());
    }
    catch(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        VStringBuffer errmsg("Custom Request Transform for method %s could not be processed!!: %s", method, msg.str());

        m_methodCRTransformErrors.setValue(method, errmsg.str());
        ERRLOG("%s", errmsg.str());
        e->Release();
    }
    catch (...)
    {
        VStringBuffer errmsg("Custom Request Transform for method %s could not be processed!!", method);
        m_methodCRTransformErrors.setValue(method, errmsg.str());
        ERRLOG("%s", errmsg.str());
    }
}

static void ensureMergeOrderedPTree(Owned<IPropertyTree> &target, IPropertyTree *src)
{
    if (!src)
        return;
    if (!target)
        target.setown(createPTree(ipt_ordered));
    mergePTree(target, src);
}

void EsdlServiceImpl::configureTargets(IPropertyTree *cfg, const char *service)
{
    VStringBuffer xpath("Definition[@esdlservice='%s']/Methods", service);
    IPropertyTree *target_cfg = cfg->queryPropTree(xpath.str());
    DBGLOG("ESDL Binding: configuring method targets for esdl service %s", service);

    if (target_cfg)
    {
        Owned<IPropertyTree> serviceCrt;
        try
        {
            ensureMergeOrderedPTree(serviceCrt, target_cfg->queryPropTree("xsdl:CustomRequestTransform"));
            Owned<IPropertyTreeIterator> transforms =  target_cfg->getElements("Transforms/xsdl:CustomRequestTransform");
            ForEach(*transforms)
                ensureMergeOrderedPTree(serviceCrt, &transforms->query());
        }
        catch (IPTreeException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            VStringBuffer errmsg("Encountered error while fetching \"xsdl:CustomRequestTransform\" from service \"%s\" ESDL configuration: %s ", service, msg.str());

            m_serviceLevelCrtFail = true;
            ERRLOG("%s", errmsg.str());
            e->Release();
        }

        addServiceLevelRequestTransform(serviceCrt);

        m_pServiceMethodTargets.setown(createPTree(ipt_caseInsensitive));
        Owned<IPropertyTreeIterator> itns = target_cfg->getElements("Method");

        ForEach(*itns)
            m_pServiceMethodTargets->addPropTree("Target", createPTreeFromIPT(&itns->query()));


        StringBuffer classPath;
        const IProperties &envConf = queryEnvironmentConf();
        if (envConf.hasProp("classpath"))
            envConf.getProp("classpath", classPath);
        else
            classPath.append(INSTALL_DIR).append(PATHSEPCHAR).append("classes");

        Owned<IPropertyTreeIterator> iter = m_pServiceMethodTargets->getElements("Target");
        ForEach(*iter)
        {
            IPropertyTree &methodCfg = iter->query();
            const char *method = methodCfg.queryProp("@name");
            if (!method || !*method)
                throw MakeStringException(-1, "ESDL binding - found target method entry without name!");

            m_methodCRTransformErrors.remove(method);
            m_customRequestTransformMap.remove(method);

            Owned<IPropertyTree> methodCrt;
            try
            {
                ensureMergeOrderedPTree(methodCrt, methodCfg.queryPropTree("xsdl:CustomRequestTransform"));
                Owned<IPropertyTreeIterator> transforms =  methodCfg.getElements("Transforms/xsdl:CustomRequestTransform");
                ForEach(*transforms)
                    ensureMergeOrderedPTree(methodCrt, &transforms->query());
            }
            catch (IException *e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                VStringBuffer errmsg("Encountered error while fetching 'xsdl:CustomRequestTransform' from service '%s', method '%s' ESDL configuration: %s ", service, method, msg.str());

                m_methodCRTransformErrors.setValue(method, errmsg.str());
                ERRLOG("%s", errmsg.str());
                e->Release();
            }

            addMethodLevelRequestTransform(method, methodCfg, methodCrt);

            const char *type = methodCfg.queryProp("@querytype");
            if (type && strieq(type, "java"))
                configureJavaMethod(method, methodCfg, classPath);
            else
                configureUrlMethod(method, methodCfg);
            DBGLOG("Method %s configured", method);
        }
    }
    else
        DBGLOG("ESDL Binding: While configuring method targets: method configuration not found for %s.", service);
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
    EsdlMethodImplJava
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
    }
    return EsdlMethodImplRoxie;
}

static inline bool isPublishedQuery(EsdlMethodImplType implType)
{
    return (implType==EsdlMethodImplRoxie || implType==EsdlMethodImplWsEcl);
}

void EsdlServiceImpl::handleServiceRequest(IEspContext &context,
                                           IEsdlDefService &srvdef,
                                           IEsdlDefMethod &mthdef,
                                           Owned<IPropertyTree> &tgtcfg,
                                           Owned<IPropertyTree> &tgtctx,
                                           const char *ns,
                                           const char *schema_location,
                                           IPropertyTree *req,
                                           StringBuffer &out,
                                           StringBuffer &logdata,
                                           unsigned int flags)
{
    const char *mthName = mthdef.queryName();
    context.addTraceSummaryValue(LogMin, "method", mthName);


    IEsdlCustomTransform *crt = m_customRequestTransformMap.getValue(mthdef.queryMethodName());
    if (!crt)
    {
        if (m_serviceLevelCrtFail)
            throw MakeStringException(-1, "%s::%s disabled due to Custom Transform errors. Review transform template in configuration.", srvdef.queryName(), mthName);
        crt = m_serviceLevelRequestTransform;
    }

    MapStringTo<SecAccessFlags>&  methaccessmap = const_cast<MapStringTo<SecAccessFlags>&>(mthdef.queryAccessMap());
    if (methaccessmap.ordinality() > 0)
    {
        if (!context.validateFeaturesAccess(methaccessmap, false))
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
            throw MakeStringException(-1, "%s::%s access denied for user '%s' - Method requires: '%s'.", srvdef.queryName(), mthName, (user && *user) ? user : "Anonymous", features.str());
        }
    }

    StringBuffer trxid;
    if (!m_bGenerateLocalTrxId)
    {
        if (m_oLoggingManager)
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
            if (!m_oLoggingManager->getTransactionID(&trxidbasics,trxid, trxidstatus))
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

    StringBuffer origResp;
    EsdlMethodImplType implType = EsdlMethodImplUnknown;

    if(stricmp(mthName, "echotest")==0 || mthdef.hasProp("EchoTest"))
    {
        handleEchoTest(mthdef.queryName(),req,out,context.getResponseFormat());
        return;
    }
    else if
    (stricmp(mthName, "ping")==0 || mthdef.hasProp("Ping"))
    {
        handlePingRequest(mthdef.queryName(),out,context.getResponseFormat());
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

        StringAttr *crtErrorMessage = m_methodCRTransformErrors.getValue(mthName);
        if (crtErrorMessage && !crtErrorMessage->isEmpty())
            throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_CLIENT, "ESDL", "%s", crtErrorMessage->str());

        implType = getEsdlMethodImplType(tgtcfg->queryProp("@querytype"));

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

            //"WsWorkunits.WsWorkunitsService.WUAbort:(LWsWorkunits/EsdlContext;LWsWorkunits/WUAbortRequest;)LWsWorkunits/WUAbortResponse;";
            VStringBuffer signature("%s:(L%s/EsdlContext;L%s/%s;)L%s/%s;", javaScopedMethod, javaPackage, javaPackage, mthdef.queryRequestType(), javaPackage, mthdef.queryResponseType());

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
             context.addTraceSummaryTimeStamp(LogNormal, "srt-reqproc");
             m_pEsdlTransformer->process(context, EsdlRequestMode, srvdef.queryName(), mthdef.queryName(), *req, writer, 0, NULL);
             context.addTraceSummaryTimeStamp(LogNormal, "end-reqproc");

             javactx->paramWriterCommit(writer);
             javactx->callFunction();

             Owned<IXmlWriterExt> javaRespWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);
             javactx->writeResult(m_esdl, srvdef.queryName(), mthdef.queryResponseType(), javaRespWriter);
             origResp.set(javaRespWriter->str());

             Owned<IXmlWriterExt> finalRespWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
             m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), finalRespWriter, logdata, ESDL_TRANS_OUTPUT_ROOT, ns, schema_location);

             out.append(finalRespWriter->str());
        }
        else
        {
            Owned<IXmlWriterExt> reqWriter = createIXmlWriterExt(0, 0, NULL, WTStandard);

            //Preprocess Request
            StringBuffer reqcontent;
            unsigned xflags = (isPublishedQuery(implType)) ? ROXIEREQ_FLAGS : ESDLREQ_FLAGS;
            context.addTraceSummaryTimeStamp(LogNormal, "srt-reqproc");
            m_pEsdlTransformer->process(context, EsdlRequestMode, srvdef.queryName(), mthdef.queryName(), *req, reqWriter.get(), xflags, NULL);
            context.addTraceSummaryTimeStamp(LogNormal, "end-reqproc");

            if(isPublishedQuery(implType))
                tgtctx.setown(createTargetContext(context, tgtcfg.get(), srvdef, mthdef, req));

            reqcontent.set(reqWriter->str());
            context.addTraceSummaryTimeStamp(LogNormal, "serialized-xmlreq");

            if (crt)
            {
                context.addTraceSummaryTimeStamp(LogNormal, "srt-custreqtrans");
                crt->processTransform(&context, reqcontent, m_oEspBindingCfg.get());
                context.addTraceSummaryTimeStamp(LogNormal, "end-custreqtrans");
            }

            handleFinalRequest(context, tgtcfg, tgtctx, srvdef, mthdef, ns, reqcontent, origResp, isPublishedQuery(implType), implType==EsdlMethodImplProxy);
            context.addTraceSummaryTimeStamp(LogNormal, "end-HFReq");

            if (isPublishedQuery(implType))
            {
                context.addTraceSummaryTimeStamp(LogNormal, "srt-procres");
                Owned<IXmlWriterExt> respWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
                m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), respWriter.get(), logdata, ESDL_TRANS_OUTPUT_ROOT, ns, schema_location);
                context.addTraceSummaryTimeStamp(LogNormal, "end-procres");

                out.append(respWriter->str());
            }
            else if(implType==EsdlMethodImplProxy)
                getSoapBody(out, origResp);
            else
                m_pEsdlTransformer->process(context, EsdlResponseMode, srvdef.queryName(), mthdef.queryName(), out, origResp.str(), ESDL_TRANS_OUTPUT_ROOT, ns, schema_location);
        }
    }

    context.addTraceSummaryTimeStamp(LogNormal, "srt-resLogging");
    handleResultLogging(context, tgtctx.get(), req,  origResp.str(), out.str(), logdata.str());
    context.addTraceSummaryTimeStamp(LogNormal, "end-resLogging");
    ESPLOG(LogMax,"Customer Response: %s", out.str());
}

bool EsdlServiceImpl::handleResultLogging(IEspContext &espcontext, IPropertyTree * reqcontext, IPropertyTree * request,const char * rawresp, const char * finalresp, const char * logdata)
{
    bool success = true;
    if (m_oLoggingManager)
    {
        Owned<IEspLogEntry> entry = m_oLoggingManager->createLogEntry();
        entry->setOption(LOGGINGDBSINGLEINSERT);
        entry->setOwnEspContext(LINK(&espcontext));
        entry->setOwnUserContextTree(LINK(reqcontext));
        entry->setOwnUserRequestTree(LINK(request));
        entry->setUserResp(finalresp);
        entry->setBackEndResp(rawresp);
        entry->setLogDatasets(logdata);

        StringBuffer logresp;
        success = m_oLoggingManager->updateLog(entry, logresp);
        ESPLOG(LogMin,"ESDLService: Attempted to log ESP transaction: %s", logresp.str());
    }

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
                                         Owned<IPropertyTree> &tgtcfg,
                                         Owned<IPropertyTree> &tgtctx,
                                         IEsdlDefService &srvdef,
                                         IEsdlDefMethod &mthdef,
                                         const char *ns,
                                         StringBuffer& req,
                                         StringBuffer &out,
                                         bool isroxie,
                                         bool isproxy)
{
    StringBuffer soapmsg;
    soapmsg.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">");


    if(isroxie)
    {
        const char *tgtQueryName = tgtcfg->queryProp("@queryname");
        if (tgtQueryName && *tgtQueryName)
        {
            soapmsg.append("<soap:Body><").append(tgtQueryName).append(">");
            soapmsg.appendf("<_TransactionId>%s</_TransactionId>", context.queryTransactionID());

            if (tgtctx)
                toXML(tgtctx.get(), soapmsg);

            soapmsg.append(req.str());
            soapmsg.append("</").append(tgtQueryName).append("></soap:Body>");
        }
        else
            throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_SERVER, "ESP",
                        "EsdlServiceImpl::handleFinalRequest() target query name missing");
    }
    else
    {
        StringBuffer headers;
        processHeaders(context, srvdef, mthdef, ns, req,headers);
        if (headers.length() > 0 )
            soapmsg.append("<soap:Header>").append(headers).append("</soap:Header>");

        processRequest(context, srvdef, mthdef, ns, req);
        soapmsg.append("<soap:Body>").append(req).append("</soap:Body>");
    }
    soapmsg.append("</soap:Envelope>");

    const char *tgtUrl = tgtcfg->queryProp("@url");
    if (tgtUrl && *tgtUrl)
        sendTargetSOAP(context, tgtcfg.get(), soapmsg.str(), out, isproxy, NULL);
    else
    {
        ESPLOG(LogMax,"No target URL configured for %s",mthdef.queryMethodName());
        throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, "ESP",
                   "No target URL configured for %s!", mthdef.queryMethodName());
    }

    processResponse(context,srvdef,mthdef,ns,out);

}

void EsdlServiceImpl::handleEchoTest(const char *mthName,
                                     IPropertyTree *req,
                                     StringBuffer &out,
                                     ESPSerializationFormat format)
{
    const char* valueIn = req->queryProp("ValueIn");
    StringBuffer encoded;
    if (format == ESPSerializationJSON)
        out.appendf("{\n\t\"%sResponse\":\n{\t\t\"ValueOut\": \"%s\"\n\t\t}\n}", mthName, encodeJSON(encoded,valueIn).str());
    else
        out.appendf("<%sResponse><ValueOut>%s</ValueOut></%sResponse>", mthName, encodeXML(valueIn,encoded), mthName);
}

void EsdlServiceImpl::handlePingRequest(const char *mthName,StringBuffer &out,ESPSerializationFormat format)
{
    if (format == ESPSerializationJSON)
        out.appendf("{\"%sPingResponse\": {}}", mthName);
    else
        out.appendf("<%sResponse></%sResponse>", mthName, mthName);
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

    StringBuffer status;
    StringBuffer clreq(req);

    ESPLOG(LogMax,"OUTGOING Request target: %s", url.str());
    ESPLOG(LogMax,"OUTGOING Request: %s", clreq.str());
    {
        EspTimeSection timing("Calling out to query");
        context.addTraceSummaryTimeStamp(LogMin, "startcall");
        httpclient->sendRequest("POST", "text/xml", clreq, resp, status,true);
        context.addTraceSummaryTimeStamp(LogMin, "endcall");
    }

    if (status.length()==0)
    {
        ERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status not reported, response content: %s", resp.str());
        throw makeWsException( ERR_ESDL_BINDING_UNAVAIL, WSERR_CLIENT, "ESP", "Internal Server Unavailable");
    }
    else if (strncmp("500", status.str(), 3)==0)  //process internal service errors.
    {
        ERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status: %s", status.str());
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
        ERRLOG("EsdlBindingImpl::sendTargetSOAP sendRequest() status: %s", status.str());
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

EsdlBindingImpl::EsdlBindingImpl(IPropertyTree* cfg, const char *binding,  const char *process) : CHttpSoapBinding(cfg, binding, process)
{
    m_pCentralStore.setown(createEsdlCentralStore());
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

/* if the target ESDL binding contains an ESDL service definition matching this espServiceName, load it.
 * Otherwise, load the first definition available, and report it via the loadedServiceName
 */
bool EsdlBindingImpl::loadDefinitions(const char * espServiceName, Owned<IEsdlDefinition>& esdl, IPropertyTree * config, StringBuffer & loadedServiceName, const char * stateFileName)
{
    if (!esdl || !config)
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
    if (esdlBndCng == nullptr)
        esdlBndCng = m_esdlBndCfg.get();
    if ( m_pESDLService )
    {
        Owned<IEsdlDefinition> tempESDLDef = createNewEsdlDefinition();

        if (!loadDefinitions(m_espServiceName.get(), tempESDLDef, esdlBndCng, loadedname, m_esdlStateFilesLocation.str()))
        {
            DBGLOG("Failed to reload ESDL definitions");
            return false;
        }

        DBGLOG("Definitions reloaded, will update ESDL definition object");
        CriticalBlock b(configurationLoadCritSec);

        m_esdl.setown(tempESDLDef.getClear());
        m_pESDLService->setEsdlTransformer(createEsdlXFormer(m_esdl));

        return true;
    }

    DBGLOG("Cannot reload definitions because the service implementation is not available");
    return false;
}

bool EsdlBindingImpl::reloadBindingFromCentralStore(const char* bindingId)
{
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
            m_esdlBndCfg.setown(tempEsdlBndCfg.getClear());
        }
        else
            DBGLOG("Could not reload binding %s because service implementation object not available.", bindingId);

    }
    catch(IException* e)
    {
       StringBuffer msg;
       e->errorMessage(msg);
       DBGLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: %s", msg.str());
       e->Release();
    }
    catch (...)
    {
        DBGLOG("Exception caught in EsdlBindingImpl::EsdlBindingImpl: Could Not load Binding %s information from Dali!", bindingId);
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

void EsdlBindingImpl::addService(const char * name,
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
                if (!loadDefinitions(name, m_esdl, m_esdlBndCfg, loadedservicename, m_esdlStateFilesLocation.str()))
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

    m_staticNamespace.set(srvdef.queryStaticNamespace());

    //superclass binding sets up wsdladdress
    //setWsdlAddress(bndcfg->queryProp("@wsdlServiceAddress"));

    IProperties *xsdparams = createProperties(false);
    xsdparams->setProp( "all_annot_Param", 1 );
    m_xsdgen->setTransformParams(EsdlXslToXsd, xsdparams);

    IProperties *wsdlparams = createProperties(false);
    wsdlparams->setProp( "location", getWsdlAddress());
    wsdlparams->setProp( "create_wsdl", "true()");
    wsdlparams->setProp( "all_annot_Param", 1 );
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

int EsdlBindingImpl::onGetInstantQuery(IEspContext &context,
                                       CHttpRequest* request,
                                       CHttpResponse* response,
                                       const char *serviceName,
                                       const char *methodName)
{
    StringBuffer xmlstr;
    StringBuffer source;
    StringBuffer orderstatus;

    context.addTraceSummaryTimeStamp(LogMin, "reqRecvd");
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

                    getSchemaLocation(context, request, schemaLocation);
                    m_pESDLService->handleServiceRequest(context, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(), schemaLocation.str(), req_pt.get(), out, logdata, 0);

                    response->setContent(out.str());

                    if (context.getResponseFormat() == ESPSerializationJSON)
                        response->setContentType(HTTP_TYPE_JSON);
                    else
                      response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
                    response->setStatus(HTTP_STATUS_OK);
                    response->send();

                    unsigned timetaken = msTick() - context.queryCreationTime();
                    context.addTraceSummaryTimeStamp(LogMin, "respSent");

                    ESPLOG(LogMax,"EsdlBindingImpl:onGetInstantQuery response: %s", out.str());

                    m_pESDLService->esdl_log(context, *srvdef, *mthdef, tgtcfg.get(), tgtctx.get(), req_pt.get(), out.str(), logdata.str(), timetaken);
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

bool EsdlBindingImpl::checkForMethodProxy(const char *service, const char *method, StringBuffer &forwardTo)
{
    if (!m_proxyInfo || !method || !*method)
        return false;

    IPropertyTree *proxyService = m_proxyInfo->queryPropTree(service);
    if (!proxyService)
        return false;

    VStringBuffer xpath("Method[@name='%s']", method);
    if (proxyService->hasProp(xpath)) //if method is configured locally, don't proxy
        return false;

    xpath.setf("Proxy[@method='%s']/@forwardTo", method); //exact (non-wild) match wins
    forwardTo.set(proxyService->queryProp(xpath));
    if (forwardTo.length())
        return true;

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
        in = request->queryContent();
        if (!in || !*in)
            throw makeWsException( ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT,  "ESP", "SOAP content not found" );

        StringBuffer reqname;
        StringBuffer ns;
        StringBuffer schemaLocation;

        IEsdlDefMethod *mthdef=NULL;
        if (getSoapMethodInfo(in, reqname, ns))
        {
            if (m_proxyInfo)
            {
                StringBuffer proxyMethod(reqname);
                size32_t len = reqname.length()-7;
                if (len>0 && strieq(reqname.str()+len, "Request"))
                    proxyMethod.setLength(len);
                StringBuffer proxyAddress;
                if (checkForMethodProxy(request->queryServiceName(), proxyMethod, proxyAddress))
                    return forwardProxyMessage(proxyAddress, request, response);
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

            ns.clear();
            generateNamespace(*ctx, request, srvdef->queryName(), mthdef->queryName(), ns);
            getSchemaLocation(*ctx, request, schemaLocation);

             m_pESDLService->handleServiceRequest(*ctx, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(),
                                       schemaLocation.str(), pt, baseout, logdata, 0);

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
            response->send();

            unsigned timetaken = msTick() - ctx->queryCreationTime();
            ctx->addTraceSummaryTimeStamp(LogMin, "respSent");

             m_pESDLService->esdl_log(*ctx, *srvdef, *mthdef, tgtcfg.get(), tgtctx.get(), pt, baseout.str(), logdata.str(), timetaken);

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

        throw makeWsException(  ERR_ESDL_BINDING_BADREQUEST, WSERR_SERVER, "Esp", "%s", text.str() );
    }
    catch (IException *e)
    {
        const char * source = request->queryServiceName();
        handleSoapRequestException(e, source);
    }
    catch(...)
    {
        StringBuffer text;
        text.append( "EsdlBindingImpl could not process SOAP request" );
        ESPLOG(LogMax,"%s", text.str());
        throw makeWsException( ERR_ESDL_BINDING_INTERNERR, WSERR_SERVER, "ESP", "%s", text.str() );
    }

    return 0;
}

StringBuffer & EsdlBindingImpl::generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char * method, StringBuffer & ns)
{
    if (!m_staticNamespace.isEmpty())
        return ns.set(m_staticNamespace);

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
            m_xsdgen->toJavaService( *it, out, EsdlXslToJavaServiceBase, context.queryRequestParameters(), 0);
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

int EsdlBindingImpl::forwardProxyMessage(const char *addr, CHttpRequest* request, CHttpResponse* response)
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
    cl->proxyRequest(request, response);

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
                    if (checkForMethodProxy(root, method, proxyAddress))
                        return forwardProxyMessage(proxyAddress, request, response);
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
            m_xsdgen->toWSDL( *it, out, EsdlXslToWsdl, context.getClientVersion(), context.queryRequestParameters(), ns.str(), ESDLDEP_FLAGS);
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
    m_xsdgen->toXSD( *it, schema, EsdlXslToXsd, ctx.getClientVersion(), ctx.queryRequestParameters(), ns.str(), ESDLDEP_FLAGS);
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
        {
            methQName->append(mth->queryName());
            addMethodDescription(methQName->str(), mth->queryProp(ESDL_METHOD_DESCRIPTION));
            addMethodHelp(methQName->str(), mth->queryProp(ESDL_METHOD_HELP));
        }
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
    const char* excludes[] = {"soap_builder_",NULL};
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
    xform->setStringParameter("destination", methname);

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
            if (checkForMethodProxy(serviceName, methodName, proxyAddress))
            {
                forwardProxyMessage(proxyAddress, request, response);
                return;
            }
        }

        StringBuffer content(request->queryContent());
        if (getEspLogLevel()>LogNormal)
            DBGLOG("EsdlBinding::%s::%s: JSON request: %s", serviceName, methodName, content.str());

        Owned<IPropertyTree> contentTree = createPTreeFromJSONString(content.str());
        if (contentTree)
        {
            StringBuffer reqestName(methodName);
            reqestName.append("Request");

            Owned<IPropertyTree> reqTree = contentTree->getBranch(reqestName.str());
            if (!reqTree)
                throw MakeStringException(-1, "EsdlBinding::%s::%s: Could not find \"%s\" section in JSON request", serviceName, methodName, reqestName.str());

            if (!m_esdl)
            {
                throw MakeStringException(-1, "EsdlBinding::%s: Service definition has not been loaded", serviceName);
            }
            else
            {
                IEsdlDefService *srvdef = m_esdl->queryService(serviceName);

                if (!srvdef)
                    throw MakeStringException(-1, "EsdlBinding::%s: Service definition not found", serviceName);
                else
                {
                    IEsdlDefMethod *mthdef = srvdef->queryMethodByName(methodName);
                    if (!mthdef)
                        throw MakeStringException(-1, "EsdlBinding::%s::%s: Method definition not found", serviceName, methodName);
                    else
                    {
                        jsonresp.append("{");
                        StringBuffer logdata; //RODRIGO: What are we doing w/ the logdata?

                        Owned<IPropertyTree> tgtcfg;
                        Owned<IPropertyTree> tgtctx;

                        StringBuffer ns, schemaLocation;
                        generateNamespace(*ctx, request, serviceName, methodName, ns);
                        getSchemaLocation(*ctx, request, schemaLocation);

                        m_pESDLService->handleServiceRequest(*ctx, *srvdef, *mthdef, tgtcfg, tgtctx, ns.str(), schemaLocation.str(), reqTree.get(), jsonresp, logdata, ESDL_BINDING_RESPONSE_JSON);

                        jsonresp.append("}");
                    }
                }
            }

            if (getEspLogLevel()>LogNormal)
                DBGLOG("json response: %s", jsonresp.str());
        }
        else
            throw MakeStringException(-1, "EsdlBinding::%s::%s: Could not process JSON request", serviceName, methodName);
    }
    catch (IWsException * iwse)
    {
        JsonHelpers::appendJSONException(jsonresp.set("{"), iwse);
        jsonresp.append('}');
    }
    catch (IException *e)
    {
        JsonHelpers::appendJSONException(jsonresp.set("{"), e);
        jsonresp.append("\n}");
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
            if (checkForMethodProxy(serviceName, methodName, proxyAddress)) //usually won't catch SOAP requests, so will test again later when method is known.
            {
                forwardProxyMessage(proxyAddress, request, response);
                return;
            }
            break;
        }
        default:
            break;
        }
    }

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
                    roxiemsg.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">");
                    roxiemsg.append("<soap:Body><").append(tgtQueryName).append(">");

                    roxiemsg.appendf("<_TransactionId>%s</_TransactionId>", context->queryTransactionID());
                    if (tgtctx)
                        toXML(tgtctx.get(), roxiemsg);

                    roxiemsg.append(reqcontent.str());
                    roxiemsg.append("</").append(tgtQueryName).append("></soap:Body>");
                    roxiemsg.append("</soap:Envelope>");
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
    JBASE64_Encode(abuf.str(), abuf.length(), auth);
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

void EsdlBindingImpl::transformGatewaysConfig( IPropertyTree* srvcfg, IPropertyTree* forRoxie )
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
            while( cfgIter->isValid() )
            {
                IPropertyTree& cfgGateway = cfgIter->query();

                StringBuffer url, service;
                if( makeURL( url, cfgGateway ) )
                {
                    cfgGateway.getProp("@name", service);
                    service.toLowerCase();

                    Owned<IPropertyTree> gw = createPTree("Gateway", false);
                    gw->addProp("ServiceName", service.str());
                    gw->addProp("URL", url.str());

                    forRoxie->addPropTree("Gateway", gw.getLink());
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
