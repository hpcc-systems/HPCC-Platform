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

#pragma warning( disable : 4786)

#include "esphttp.hpp"

#include <stdlib.h>

//ESP Bindings
#include "espcontext.hpp"
#include "http/client/httpclient.hpp"
#include "SOAP/client/soapclient.hpp"
#include "bindutil.hpp"

#include <memory>

int CSoapClient::setUsernameToken(const char* username, const char* password, const char* realm)
{
    m_username.set(username);
    m_password.set(password);
    m_realm.set(realm);
    return 0;
}

// Under most cases, use this one
int CSoapClient::postRequest(IRpcMessage& rpccall, IRpcMessage& rpcresponse)
{
    return postRequest(NULL, NULL, rpccall, rpcresponse);
}

// If you want to set a soapaction http header, use this one.
int CSoapClient::postRequest(const char* soapaction, IRpcMessage& rpccall, StringBuffer& resp)
{
    return postRequest(NULL, soapaction, rpccall, resp);
}

// If you want to set a soapaction http header, use this one.
int CSoapClient::postRequest(const char* soapaction, IRpcMessage& rpccall, StringBuffer& resp, IRpcMessageArray *headers)
{
    return postRequest(NULL, soapaction, rpccall, resp, headers);
}

int CSoapClient::postRequest(const char* soapaction, IRpcMessage& rpccall, IRpcMessage& rpcresponse)
{
    return postRequest(NULL, soapaction, rpccall, rpcresponse);
}

// If you want a content-type that is different from "soap/application", use this one. This should not be necessary
// unless the server side doesn't follow the standard.
int CSoapClient::postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf)
{
    return postRequest(contenttype, soapaction, rpccall, responsebuf, NULL, NULL);
}

int CSoapClient::postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf, IRpcMessageArray *headers)
{
    return postRequest(contenttype, soapaction, rpccall, responsebuf, NULL, headers);
}

int CSoapClient::postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf, CMimeMultiPart* resp_multipart,  IRpcMessageArray *headers)
{
    CRpcCall* call = (CRpcCall *)&rpccall;

    CBody* req_body = new CBody;
    req_body->add_rpcmessage(call);

    CHeader* req_header = NULL;
    if(m_username.length() > 0)
    {
        req_header = new CHeader;
        IRpcMessage* oneblock = new CRpcMessage("Security");
        oneblock->set_ns("wsse");
        oneblock->add_value("", "wsse", "UsernameToken", "", "");
        oneblock->add_value("UsernameToken", "wsse", "Username", "", m_username.get());
        oneblock->add_value("UsernameToken", "wsse", "Password", "", m_password.get());
        oneblock->add_value("RealmToken",    "wsse", "Realm", "", m_realm.get());
        req_header->addHeaderBlock(oneblock);
    }

    if (headers)
    {
        if (!req_header)
            req_header = new CHeader;
        
        ForEachItemIn(idx, *headers)
        {
            req_header->addHeaderBlock(&headers->item(idx));
            headers->remove(idx, true);
        }
    }

    // Form request
    Owned<CEnvelope> req_envelope;
    req_envelope.setown(new CEnvelope(req_header, req_body));
    Owned<CMimeMultiPart> multipart;
    multipart.setown(new CMimeMultiPart("1.0", "", "MIME_boundary", "text/xml", "soaproot"));
    req_envelope->marshall(multipart);

    StringBuffer requeststr;
    StringBuffer contenttypestr;

    if(multipart->getBodyCount() == 1)
    {
        CMimeBodyPart* rootpart = multipart->queryRootPart();
        rootpart->getContent(requeststr);
    }
    else
    {
        multipart->serialize(contenttypestr, requeststr);
    }

    if (getEspLogLevel(rpccall.queryContext())>LogNormal)
    {
        DBGLOG("Content type: %s", contenttypestr.str());
    }

    Owned<CSoapRequest> soap_request;
    soap_request.setown(new CSoapRequest);

    //Use the provided contenttype to overwrite the default content type.
    if(contenttype != NULL && strlen(contenttype) > 0)
        soap_request->set_content_type(contenttype);
    else
        soap_request->set_content_type(contenttypestr.str());

    soap_request->set_text(requeststr.str());
    if(soapaction != NULL && strlen(soapaction) > 0)
        soap_request->set_soapaction(soapaction);

    Owned<CSoapResponse> soap_response;
    soap_response.setown(new CSoapResponse);

    //Send request and get response
    if(m_transportclient.get() == NULL)
    {
        Owned<IHttpClientContext> httpctx = getHttpClientContext();
        Owned<IHttpClient> httpclient = httpctx->createHttpClient(call->getProxy(), call->get_url());
        if (m_disableKeepAlive)
            httpclient->disableKeepAlive();
        if(m_username.length() > 0)
        {
            httpclient->setUserID(m_username.get());
            httpclient->setPassword(m_password.get());
        }
        if(m_realm.length() > 0)
        {
            httpclient->setRealm(m_realm.get());
        }

        if (m_connectTimeoutMs)
            httpclient->setConnectTimeOutMs(m_connectTimeoutMs);
        if (m_readTimeoutSecs)
            httpclient->setTimeOut(m_readTimeoutSecs);

        m_transportclient.setown(httpclient.getClear());
    }

    if (!soap_request.get() || !soap_response.get())
        throw MakeStringException(-1, "request or response is NULL");
    m_transportclient->postRequest(*soap_request.get(), *soap_response.get());

    int retstatus = soap_response->get_status();
    if(retstatus != SOAP_OK)
    {
        StringBuffer errmsg = "SOAP error";

        if(retstatus == SOAP_CLIENT_ERROR)
            errmsg = "SOAP client error";
        else if(retstatus == SOAP_SERVER_ERROR)
            errmsg = "SOAP server error";
        else if(retstatus == SOAP_RPC_ERROR)
            errmsg = "SOAP rpc error";
        else if(retstatus == SOAP_CONNECTION_ERROR)
            errmsg = "SOAP Connection error";
        else if(retstatus == SOAP_REQUEST_TYPE_ERROR)
            errmsg = "SOAP request type error";
        else if(retstatus == SOAP_AUTHENTICATION_ERROR)
            errmsg = "SOAP authentication error";

        const char *errText = soap_response->get_err();
        if (errText && *errText)
        {
            if (retstatus == SOAP_SERVER_ERROR) //check for special case of VERY ugly soapfault string
            {
                const char *faultString = strstr(errText, "string=[");
                if (faultString)
                {
                    faultString += 8;
                    const char *endString = strchr(faultString, ']');
                    if (endString)
                    {
                        errmsg.clear().append(endString-faultString, faultString);
                        errText = nullptr;
                    }
                }
            }
            if (errText)
                errmsg.appendf("[%s]", errText);
        }

        throw MakeStringException(retstatus, "%s", errmsg.str());
    }

#if defined(DEBUG_HTTP_)
    DBGLOG("response content type = %s", soap_response->get_content_type());
#endif
    if(!Utils::strncasecmp(soap_response->get_content_type(), "application/soap", strlen("application/soap")) || !Utils::strncasecmp(soap_response->get_content_type(), "text/xml", strlen("text/xml")))
    {
        responsebuf.append(soap_response->get_text());
    }
    else if(!Utils::strncasecmp(soap_response->get_content_type(), "Multipart/Related", strlen("Multipart/Related")))
    {
        CMimeMultiPart* parts = resp_multipart ? resp_multipart : new CMimeMultiPart("1.0", "Multipart/Related", "", "", "");
        parts->unserialize(soap_response->get_content_type(), soap_response->get_text_length(), soap_response->get_text());
        CMimeBodyPart* rootpart = parts->queryRootPart();
        rootpart->getContent(responsebuf);
        if (!resp_multipart)
            delete parts;
    }
    else
    {
        UWARNLOG("SOAP Response type %s not supported", soap_response->get_content_type());
        return SOAP_REQUEST_TYPE_ERROR;
    }

    if(responsebuf.length() == 0)
    {
        throw MakeStringException(-1, "Empty SOAP message received");
    }

    return SOAP_OK;
}

int CSoapClient::postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, IRpcMessage& rpcresponse)
{
    StringBuffer responsebuf;
    Owned<CMimeMultiPart> parts = new CMimeMultiPart("1.0", "Multipart/Related", "", "", "");
    int rt = postRequest(contenttype, soapaction, rpccall, responsebuf, parts);
    if (rt != SOAP_OK)
        return rt;

//  DBGLOG("response SoapClient got from soap server = \n%s", responsebuf.str());
    std::unique_ptr<XmlPullParser> xpp(new XmlPullParser());
    int bufSize = responsebuf.length();
    xpp->setSupportNamespaces(true);
    xpp->setInput(responsebuf.str(), bufSize);

    int type;
    StartTag stag;
    EndTag etag;

    Owned<CEnvelope> res_envelope;
    res_envelope.setown(new CEnvelope);

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
    {
        if(type == XmlPullParser::START_TAG) {
            xpp->readStartTag(stag);
            if(!stricmp(stag.getLocalName(), SOAP_ENVELOPE_NAME))
            {
                res_envelope->unmarshall(xpp.get());
                break;
            }
        }
    }

    CBody* res_body = res_envelope->get_body();

    CRpcResponse* response = (CRpcResponse *)&rpcresponse;
    res_body->nextRpcMessage(response);
    response->unmarshall(xpp.get(), parts.get());

    return SOAP_OK;
}

int CSoapClient::postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf)
{
    return postRequest(rpccall, responsebuf, NULL);
}

int CSoapClient::postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf, IRpcMessageArray *headers)
{
    CRpcCall* call = (CRpcCall *)&rpccall;

    CBody* req_body = new CBody;
    req_body->add_rpcmessage(call);

    CHeader* req_header = NULL;
    if(m_username.length() > 0)
    {
        req_header = new CHeader;
        IRpcMessage* oneblock = new CRpcMessage("Security");
        oneblock->set_ns("wsse");
        oneblock->add_value("", "wsse", "UsernameToken", "", "");
        oneblock->add_value("UsernameToken", "wsse", "Username", "", m_username.get());
        oneblock->add_value("UsernameToken", "wsse", "Password", "", m_password.get());
        oneblock->add_value("RealmToken",    "wsse", "Realm", "", m_realm.get());
        req_header->addHeaderBlock(oneblock);
    }

    if (headers)
    {
        if (!req_header)
            req_header = new CHeader;
        ForEachItemIn(idx, *headers)
        {
            req_header->addHeaderBlock(&headers->item(idx));
            headers->remove(idx, true);
        }
    }

    // Form request
    Owned<CEnvelope> req_envelope;
    req_envelope.setown(new CEnvelope(req_header, req_body));
    Owned<CMimeMultiPart> multipart;
    multipart.setown(new CMimeMultiPart("1.0", "", "MIME_boundary", "text/xml", "soaproot"));
    req_envelope->marshall(multipart);

    StringBuffer requeststr;
    StringBuffer contenttypestr;

    if(multipart->getBodyCount() == 1)
    {
        CMimeBodyPart* rootpart = multipart->queryRootPart();
        rootpart->getContent(requeststr);
    }
    else
    {
        multipart->serialize(contenttypestr, requeststr);
    }

    Owned<CSoapRequest> soap_request = new CSoapRequest();

    soap_request->set_content_type(contenttypestr.str());
    soap_request->set_text(requeststr.str());
#if defined(DEBUG_HTTP_)
    DBGLOG("SOAP Request content: %s", requeststr.str());
#endif

    Owned<CSoapResponse> soap_response = new CSoapResponse();

    //Send request and get response
    if(m_transportclient.get() == NULL)
    {
        Owned<IHttpClientContext> httpctx = getHttpClientContext();
        IHttpClient* httpclient = httpctx->createHttpClient(call->getProxy(), call->get_url());
        m_transportclient.setown(httpclient);
        if (m_disableKeepAlive)
            httpclient->disableKeepAlive();

        if(m_username.length() > 0)
        {
            httpclient->setUserID(m_username.get());
            httpclient->setPassword(m_password.get());
        }
        if(m_realm.length() > 0)
        {
            httpclient->setRealm(m_realm.get());
        }
    }

    if (!soap_request.get() || !soap_response.get())
        throw MakeStringException(-1, "request or response is NULL");
    m_transportclient->postRequest(*soap_request.get(), *soap_response.get());

    int retstatus = soap_response->get_status();
    if(retstatus != SOAP_OK)
    {
        const char* errmsg = "SOAP error";
        if(retstatus == SOAP_CLIENT_ERROR)
            errmsg = "SOAP client error";
        else if(retstatus == SOAP_SERVER_ERROR)
            errmsg = "SOAP server error";
        else if(retstatus == SOAP_RPC_ERROR)
            errmsg = "SOAP rpc error";
        else if(retstatus == SOAP_CONNECTION_ERROR)
            errmsg = "SOAP Connection error";
        else if(retstatus == SOAP_REQUEST_TYPE_ERROR)
            errmsg = "SOAP request type error";
        else if(retstatus == SOAP_AUTHENTICATION_ERROR)
            errmsg = "SOAP authentication error";

        throw MakeStringException(retstatus, "%s", errmsg);
    }

    if(soap_response->get_text_length() == 0)
    {
        throw MakeStringException(-1, "Empty SOAP message received");
    }

    StringBuffer& resptext = soap_response->query_text();
    responsebuf.swapWith(resptext);

    return SOAP_OK;
}
