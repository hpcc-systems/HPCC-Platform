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

#pragma warning( disable : 4786 )

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"

//ESP Core
#include "espthread.hpp"

//ESP Bindings
#include "SOAP/Platform/soapbind.hpp"
#include "SOAP/client/soapclient.hpp"
#include "http/platform/httpprot.hpp"
#include "http/platform/httpservice.hpp"
#include "SOAP/Platform/soapservice.hpp"
#include "SOAP/Platform/soapmessage.hpp"
#include "SOAP/xpp/xjx/xjxpp.hpp"
#include "jmetrics.hpp"


static auto pSoapRequestCount = hpccMetrics::registerCounterMetric("esp.soap_requests.received", "Number of JSON and SOAP POST requests received", SMeasureCount);


#define ESP_FACTORY DECL_EXPORT

CSoapBinding::CSoapBinding()
{
}

CSoapBinding::~CSoapBinding()
{
}

//IEspRpcBinding
const char * CSoapBinding::getRpcType()
{
   return "soap";
}


const char * CSoapBinding::getTransportType()
{
   return "unknown";
}

int CSoapBinding::processRequest(IRpcMessage* rpc_call, IRpcMessage* rpc_response)
{
    return 0;
}

CHttpSoapBinding::CHttpSoapBinding():EspHttpBinding(NULL, NULL, NULL)
{
    log_level_=hsl_none;
}

CHttpSoapBinding::CHttpSoapBinding(IPropertyTree* cfg, const char *bindname, const char *procname, http_soap_log_level level)
: EspHttpBinding(cfg, bindname, procname)
{
    log_level_=level;
}

CHttpSoapBinding::~CHttpSoapBinding()
{
}

const char * CHttpSoapBinding::getTransportType()
{
    return "http";
}

static CSoapFault* makeSoapFault(CHttpRequest* request, IMultiException* me, const char *ns, const char* schemaLocation)
{
    if (!isEmptyString(schemaLocation))
    {
        StringBuffer ns_ext;
        appendXMLAttr(ns_ext, "xmlns", ns, nullptr, true);
        appendXMLAttr(ns_ext, "schemaLocation", schemaLocation, "xsi", true);
        return new CSoapFault(me, ns_ext);
    }

    return new CSoapFault(me);
}

int CHttpSoapBinding::onSoapRequest(CHttpRequest* request, CHttpResponse* response)
{
    pSoapRequestCount->inc(1);
    IEspContext* ctx = request->queryContext();
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        int errcode = 0;
        StringBuffer msgbuf;
        try
        {
            return HandleSoapRequest(request,response);
        }
        catch (IMultiException* mex)
        {
            errcode = mex->errorCode();
            mex->serializeJSON(msgbuf, 0, true, true, true);
            StringBuffer errMessage;
            ctx->addTraceSummaryValue(LogMin, "msg", mex->errorMessage(errMessage).str(), TXSUMMARY_GRP_ENTERPRISE);
            mex->Release();
        }
        catch (IException* e)
        {
            errcode = e->errorCode();
            Owned<IMultiException> mex = MakeMultiException("Esp");
            mex->append(*e); // e is owned by mex
            mex->serializeJSON(msgbuf, 0, true, true, true);
            StringBuffer errMessage;
            ctx->addTraceSummaryValue(LogMin, "msg", e->errorMessage(errMessage).str(), TXSUMMARY_GRP_ENTERPRISE);
        }
        catch (...)
        {
            errcode = 500;
            Owned<IMultiException> mex = MakeMultiException("Esp");
            mex->append(*MakeStringException(500, "Internal Server Error"));
            mex->serializeJSON(msgbuf, 0, true, true, true);
            ctx->addTraceSummaryValue(LogMin, "msg", "Internal Server Error", TXSUMMARY_GRP_ENTERPRISE);
        }
        SetHTTPErrorStatus(errcode, response);
        response->setContentType(HTTP_TYPE_JSON);
        response->setContent(msgbuf.str());
    }
    else
    {
        Owned<CSoapFault> soapFault;
        try
        {
            return HandleSoapRequest(request,response);
        }
        catch (IMultiException* mex)
        {
            StringBuffer ns;
            StringBuffer schemaLocation;
            generateNamespace(*request->queryContext(), request, request->queryServiceName(), request->queryServiceMethod(), ns);
            getSchemaLocation(*ctx, request, ns, schemaLocation);
            soapFault.setown(makeSoapFault(request,mex, ns.str(), schemaLocation.str()));
            //SetHTTPErrorStatus(mex->errorCode(),response);
            SetHTTPErrorStatus(500,response);
            StringBuffer errMessage;
            ctx->addTraceSummaryValue(LogMin, "msg", mex->errorMessage(errMessage).str(), TXSUMMARY_GRP_ENTERPRISE);
            VStringBuffer fault("F%d", mex->errorCode());
            ctx->addTraceSummaryValue(LogMin, "custom_fields.soapFaultCode", fault.str(), TXSUMMARY_GRP_ENTERPRISE);
            mex->Release();
        }
        catch (IException* e)
        {
            StringBuffer ns;
            StringBuffer schemaLocation;
            generateNamespace(*request->queryContext(), request, request->queryServiceName(), request->queryServiceMethod(), ns);
            getSchemaLocation(*ctx, request, ns, schemaLocation);
            Owned<IMultiException> mex = MakeMultiException("Esp");
            mex->append(*e); // e is owned by mex
            soapFault.setown(makeSoapFault(request,mex, ns.str(), schemaLocation.str()));
            SetHTTPErrorStatus(500,response);
            StringBuffer errMessage;
            ctx->addTraceSummaryValue(LogMin, "msg", e->errorMessage(errMessage).str(), TXSUMMARY_GRP_ENTERPRISE);
            VStringBuffer fault("F%d", mex->errorCode());
            ctx->addTraceSummaryValue(LogMin, "custom_fields.soapFaultCode", fault.str(), TXSUMMARY_GRP_ENTERPRISE);
        }
        catch (...)
        {
            soapFault.setown(new CSoapFault(500,"Internal Server Error"));
            SetHTTPErrorStatus(500,response);
            ctx->addTraceSummaryValue(LogMin, "msg", "Internal Server Error", TXSUMMARY_GRP_ENTERPRISE);
        }
        //response->setContentType(soapFault->get_content_type());
        response->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
        response->setContent(soapFault->get_text());
    }
    response->send();
    return -1;
}

int CHttpSoapBinding::HandleSoapRequest(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer requeststr;
    request->getContent(requeststr);
    if (requeststr.length() == 0)
        throw MakeStringException(-1, "Content read is empty");

    IEspContext* ctx = request->queryContext();

    Owned<CSoapService> soapservice;
    Owned<CSoapRequest> soaprequest;
    Owned<CSoapResponse> soapresponse;

    soapservice.setown(new CSoapService(this));
    soaprequest.setown(new CSoapRequest);
    soaprequest->set_text(requeststr.str());

    StringBuffer contenttype;
    request->getContentType(contenttype);
    soaprequest->set_content_type(contenttype.str());
    soaprequest->setContext(ctx);

    CMimeMultiPart* multipart = request->queryMultiPart();
    if (multipart != nullptr)
        soaprequest->setOwnMultiPart(LINK(multipart));

    soapresponse.setown(new CSoapResponse);

    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        soaprequest->setHttpReq(request);
        soapresponse->setHttpResp(response);
    }

    soapservice->processRequest(*soaprequest.get(), *soapresponse.get());

    //For JSON the response would have been sent except for certain errors, which will be thrown below
    if (!soapresponse->getHttpResp() || !soapresponse->getHttpResp()->getRespSent())
    {
        response->setVersion(HTTP_VERSION);
        int status = soapresponse->get_status();
        if (status == SOAP_OK)
            response->setStatus(HTTP_STATUS_OK);
        else if (status == SOAP_SERVER_ERROR || status == SOAP_RPC_ERROR || status == SOAP_CONNECTION_ERROR)
        {
            StringBuffer msg("Internal Server Error");
            const char* detail = soapresponse->get_err();
            if (detail && *detail)
                msg.appendf(" [%s]", detail);
            throw MakeStringExceptionDirect(500, msg.str());
        }
        else if (status == SOAP_CLIENT_ERROR || status == SOAP_REQUEST_TYPE_ERROR)
        {
            StringBuffer msg("Bad Request");
            const char* detail = soapresponse->get_err();
            if (detail && *detail)
                msg.appendf(" [%s]", detail);
            throw MakeStringExceptionDirect(400, msg.str());
        }
        else if (status == SOAP_AUTHENTICATION_REQUIRED)
            response->sendBasicChallenge(m_challenge_realm.str(), false);
        else if (status == SOAP_AUTHENTICATION_ERROR)
        {
            throw MakeStringExceptionDirect(401,"Unauthorized Access");
        }
        else
            response->setStatus(HTTP_STATUS_OK);

        response->setContentType(soapresponse->get_content_type());
        response->setContent(soapresponse->get_text());
        response->send();
    }

    return 0;
}


static IPropertyTree *createSecClientConfig(const char *clientCertPath, const char *clientPrivateKey, const char *caCertsPath, bool acceptSelfSigned)
{
    Owned<IPropertyTree> info = createPTree();

    if (!isEmptyString(clientCertPath))
    {
        info->setProp("certificate", clientCertPath);
        if (!isEmptyString(clientPrivateKey))
            info->setProp("privatekey", clientPrivateKey);
    }

    IPropertyTree *verify = ensurePTree(info, "verify");
    if (!isEmptyString(caCertsPath))
    {
        IPropertyTree *ca = ensurePTree(verify, "ca_certificates");
        ca->setProp("@path", caCertsPath);
    }
    verify->setPropBool("@enable", true);
    verify->setPropBool("@accept_selfsigned", acceptSelfSigned);
    verify->setProp("trusted_peers", "anyone");

    return info.getClear();
}

void CSoapRequestBinding::post(const char *proxy, const char* url, IRpcResponseBinding& response, const char *soapaction)
{
    CRpcCall rpccall;
    CRpcResponse rpcresponse;

    rpccall.set_url(url);
    rpccall.setProxy(proxy);

    serialize(*static_cast<IRpcMessage*>(&rpccall));
    
    CSoapClient soapclient; //to add support for handling cookies soapclient(false);
    if (connectTimeoutMs_)
        soapclient.setConnectTimeoutMs(connectTimeoutMs_);
    if (readTimeoutSecs_)
        soapclient.setReadTimeoutSecs(readTimeoutSecs_);
    if (mtls_secret_.length())
        soapclient.setMtlsSecretName(mtls_secret_);
    if (client_cert_.length() || ca_certs_.length() || accept_self_signed_)
        soapclient.setSecureSocketConfig(createSecClientConfig(client_cert_, client_priv_key_, ca_certs_, accept_self_signed_));

    soapclient.setUsernameToken(soap_getUserId(), soap_getPassword(), soap_getRealm());

    int result = soapclient.postRequest(soapaction, rpccall, rpcresponse);

    if(result == SOAP_OK)
    {
        response.setRpcState(RPC_MESSAGE_OK);
        response.unserialize(rpcresponse, NULL, NULL);
    }
    else if(result == SOAP_CONNECTION_ERROR)
    {
        response.setRpcState(RPC_MESSAGE_CONNECTION_ERROR);
    }
    else
    {
        response.setRpcState(RPC_MESSAGE_ERROR);
    }
}

void CSoapComplexType::appendContent(IEspContext* ctx, MemoryBuffer& buffer, StringBuffer& mimetype)
{
    StringBuffer content;
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        const char *jsonp = ctx->queryRequestParameters()->queryProp("jsonp");
        if (jsonp && *jsonp)
            content.append(jsonp).append('(');
        content.append('{');
        serializeStruct(ctx, content, (const char *)NULL);
        content.append('}');
        if (jsonp && *jsonp)
            content.append(");");
        mimetype.set("application/json");
    }
    else
    {
        buffer.append(38, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        serializeStruct(ctx, content, (const char *)NULL);
        mimetype.set("text/xml; charset=UTF-8");
    }

    buffer.append(content.length(), content.str());
}

void CSoapComplexType::appendContent(IEspContext* ctx, StringBuffer& buffer, StringBuffer& mimetype)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        const char *jsonp = ctx->queryRequestParameters()->queryProp("jsonp");
        if (jsonp && *jsonp)
            buffer.append(jsonp).append('(');
        buffer.append('{');
        serializeStruct(ctx, buffer, (const char *)nullptr);
        buffer.append('}');
        if (jsonp && *jsonp)
            buffer.append(");");
        mimetype.set("application/json");
    }
    else
    {
        buffer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        serializeStruct(ctx, buffer, (const char *)nullptr);
        mimetype.set("text/xml; charset=UTF-8");
    }
}

inline void open_element(IEspContext *ctx, StringBuffer &xml, const char *name, const char *uri, const char *prefix)
{
    if (!name || !*name)
        return;
    xml.append("<");
    if (prefix && *prefix)
        xml.append(prefix).append(':');
    xml.append(name);
    if (uri && *uri)
    {
        xml.append("xmlns");
        if (prefix && *prefix)
            xml.append(':').append(prefix);
        xml.append("=\"").append(uri).append('\"');
    }
}

inline void start_child_attributes(IEspContext *ctx, StringBuffer &xml, const char *name)
{
}

inline void start_child_elements(IEspContext *ctx, StringBuffer &xml, const char *name)
{
    if (!name || !*name)
        return;
    xml.append('>');
}

inline void close_element(IEspContext *ctx, StringBuffer &xml, const char *name, const char *prefix)
{
    if (!name || !*name)
        return;
    xml.append("</");
    if (prefix && *prefix)
        xml.append(prefix).append(':');
    xml.append(name).append('>');
}

void CSoapComplexType::serializeJSONStruct(IEspContext* ctx, StringBuffer& s, const char *name)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        appendJSONNameOrDelimit(s, name);
        s.append("{");
        serializeContent(ctx, s);
        s.append("}");
        return;
    }
}

void CSoapComplexType::serializeStruct(IEspContext* ctx, StringBuffer& s, const char *name)
{
    const char *tag = (name && *name) ? name : getRootName();
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return serializeJSONStruct(ctx, s, tag);

    open_element(ctx, s, tag, getNsURI(), getNsPrefix());
    start_child_attributes(ctx, s, name);
    serializeAttributes(ctx, s);
    start_child_elements(ctx, s, tag);
    serializeContent(ctx, s);
    close_element(ctx, s, tag, getNsPrefix());
}

void CSoapComplexType::serializeItem(IEspContext* ctx, StringBuffer& s, const char *name)
{
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        return serializeJSONStruct(ctx, s, NULL);
    serializeStruct(ctx, s, name);
}

void CSoapResponseBinding::handleExceptions(IMultiException *me, const char *serv, const char *meth)
{
    if (me->ordinality() > 0)
    {
        StringBuffer text;
        me->errorMessage(text);
        text.append('\n');
        IWARNLOG("Exception(s) in %s::%s - %s", serv, meth, text.str());

        IArrayOf<IException>& exceptions = me->getArray();
        ForEachItemIn(i, exceptions)
            noteException(*LINK(&exceptions.item(i)));
    }
}


void SetHTTPErrorStatus(int ErrorCode,CHttpResponse* response)
{
    switch(ErrorCode)
    {
    case 204:
        response->setStatus(HTTP_STATUS_NO_CONTENT);
        break;
    case 301:
        response->setStatus(HTTP_STATUS_MOVED_PERMANENTLY);
        break;
    case 302:
        response->setStatus(HTTP_STATUS_REDIRECT);
        break;
    case 303:
        response->setStatus(HTTP_STATUS_REDIRECT_POST);
        break;
    case 400:
        response->setStatus(HTTP_STATUS_BAD_REQUEST);
        break;
    case 401:
        response->setStatus(HTTP_STATUS_UNAUTHORIZED);
        break;
    case 403:
        response->setStatus(HTTP_STATUS_FORBIDDEN);
        break;
    case 404:
        response->setStatus(HTTP_STATUS_NOT_FOUND);
        break;
    case 405:
        response->setStatus(HTTP_STATUS_NOT_ALLOWED);
        break;
    case 501:
        response->setStatus(HTTP_STATUS_NOT_IMPLEMENTED);
        break;
    default:
        response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);
    }
}
