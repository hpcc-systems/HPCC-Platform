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

//ESP Bindings
#include "platform.h"
#include <xpp/XmlPullParser.h>
#include <xjx/xjxpp.hpp>
#include "SOAP/Platform/soapservice.hpp"
#include "http/platform/httpservice.hpp"
#include "bindutil.hpp"

#include "authenticate.hpp"
#include <memory>

using namespace std;
using namespace xpp;

#ifdef _DEBUG
//#define DEBUG_SOAP_
#endif

int CSoapService::processHeader(CHeader* header, IEspContext* ctx)
{
    int num = header->getNumBlocks();

    if(ctx == NULL)
        return 0;

    int returnValue = 0;
    bool authenticated = !ctx->toBeAuthenticated();
    for (int i = 0; i < num; i++)
    {
        IRpcMessage* oneblock = header->getHeaderBlock(i);
        if(oneblock == NULL)
            continue;
        if(strcmp(oneblock->get_name(), "Security") == 0)
        {
            bool encodeXML = oneblock->getEncodeXml();
            oneblock->setEncodeXml(false);
            StringBuffer username, password,realm;
            oneblock->get_value("UsernameToken/Username", username);
            oneblock->get_value("UsernameToken/Password", password);
            oneblock->get_value("RealmToken/Realm", realm);
            oneblock->setEncodeXml(encodeXML);
            //DBGLOG("username=%s, password=%s", username.str(), password.str());
            if(username.length() > 0)
            {
                ctx->setUserID(username.str());
                ctx->setPassword(password.str());
                if(realm.length()>0)
                    ctx->setRealm(realm.str());
                
                ISecManager* secmgr = ctx->querySecManager();
                if(secmgr != NULL)
                {
                    ISecUser *user = ctx->queryUser();
                    if(user==NULL)
                    {
                        user = secmgr->createUser(username.str());
                        ctx->setUser(user);
                    }
                    if(user == NULL)
                    {
                        IWARNLOG("Couldn't create ISecUser object for %s", username.str());
                    }
                    user->setName(username.str());
                    user->credentials().setPassword(password.str());
                    if(realm.length()>0)
                        user->setRealm(realm.str());
                }

                if(ctx->toBeAuthenticated())
                {
                    if(stricmp(m_soapbinding->getTransportType(), "http") == 0)
                    {
                        EspHttpBinding* httpbinding = dynamic_cast<EspHttpBinding*>(m_soapbinding.get());
                        authenticated = httpbinding->doAuth(ctx);
                    }
                    else
                    {
                        authenticated = false;
                    }
                    if(!authenticated)
                        returnValue = SOAP_AUTHENTICATION_ERROR;
                    break;
                }
            }
        }
    }

    if (returnValue == 0)
    {
        if (authenticated)
        {
            if (ctx->toBeAuthenticated())
                ctx->setAuthStatus(AUTH_STATUS_OK); //May be changed to AUTH_STATUS_NOACCESS if failed in feature level authorization.
            return 0;
        }
        returnValue = SOAP_AUTHENTICATION_REQUIRED;
    }

    StringBuffer peerStr;
    ctx->getPeer(peerStr);
    const char* userId = ctx->queryUserId();
    VStringBuffer msg("SOAP request from %s@%s.", (userId&&*userId)?userId:"unknown", (peerStr.length()>0)?peerStr.str():"unknown");
    if (returnValue == SOAP_AUTHENTICATION_ERROR)
        msg.append(" User authentication failed");
    else
        msg.append(" User authentication required");
    ctx->setAuthStatus(AUTH_STATUS_FAIL);
    DBGLOG("%s", msg.str());

    return returnValue;
}

int CSoapService::processRequest(ISoapMessage &req, ISoapMessage& resp)
{
    CSoapRequest& request = *(dynamic_cast<CSoapRequest*>(&req));
    CSoapResponse& response = *(dynamic_cast<CSoapResponse*>(&resp));

    IEspContext* ctx = req.queryContext();
    
    StringBuffer requeststr;

    Owned<CMimeMultiPart> multipart;
    if(Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_SOAP, strlen(HTTP_TYPE_SOAP))==0 || Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_TEXT_XML, strlen(HTTP_TYPE_TEXT_XML))==0)
    {
        requeststr.append(request.get_text());
    }
    else if (Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_JSON, strlen(HTTP_TYPE_JSON))==0)
    {
        requeststr.append(request.get_text());
    }
    else if(!Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_MULTIPART_RELATED, strlen(HTTP_TYPE_MULTIPART_RELATED)))
    {
        multipart.setown(LINK(request.queryMultiPart()));
        CMimeBodyPart* rootpart = multipart->queryRootPart();
        if(rootpart != NULL)
            rootpart->getContent(requeststr);
        else
            throw MakeStringException(-1, "MultiPart root is NULL");
    }
    else
    {
        throw MakeStringException(-1, "Request type %s not supported", request.get_content_type());
    }
    
    OwnedPtr<XJXPullParser> xpp;
    int bufSize = requeststr.length();
    //Parse the content
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
        xpp.setown(new CJsonPullParser(true));
    else
    {
        XmlPullParser* ptr = nullptr;
        xpp.setown((ptr = new XmlPullParser()));
        ptr->setSupportNamespaces(true);
    }
    xpp->setInput(requeststr.str(), bufSize);

    int type; 
    StartTag stag;
    EndTag etag;
    StringBuffer peerStr;
    ctx->getPeer(peerStr);
    Owned<CRpcResponse> rpc_response;
    rpc_response.setown(new CRpcResponse);
    rpc_response->setContext(req.queryContext());
    rpc_response->setHttpResp(response.getHttpResp());
    Owned<CRpcCall>rpc_call;
    rpc_call.setown(new CRpcCall);
    rpc_call->setContext(req.queryContext());
    rpc_call->setHttpReq(request.getHttpReq());
    
    if (ctx && ctx->getResponseFormat()==ESPSerializationJSON)
    {
        rpc_call->preunmarshall(xpp.get());
        rpc_call->unmarshall(xpp.get(), multipart.get());
        const char* userId = ctx->queryUserId();
        DBGLOG("JSON method <%s> from %s@%s.", rpc_call->get_name(),  (userId&&*userId)?userId:"unknown",
            (peerStr.length()>0)?peerStr.str():"unknown");
        ctx->setHTTPMethod("JSON");
    }
    else
    {
        Owned<CEnvelope> req_envelope;
        req_envelope.setown(new CEnvelope);

        while ((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
        {
            if (type == XmlPullParser::START_TAG) {
                xpp->readStartTag(stag);
                if (!stricmp(stag.getLocalName(), SOAP_ENVELOPE_NAME))
                {
                    req_envelope->unmarshall(xpp.get());
                    break;
                }
            }
        }

        CHeader* req_header = req_envelope->get_header();
        if (req_header != NULL)
        {
            // As headers are normally for common uses like authentication and routing, let's process it here
            // instead of in binding.
            int ret = processHeader(req_header, ctx);
            if (ret != 0 )
            {
                response.set_status(ret);
                return 0;
            }
        }

        CBody* req_body = req_envelope->get_body();
        const char* userId = ctx->queryUserId();
        try {
            req_body->nextRpcMessage(rpc_call.get());
            rpc_call->unmarshall(xpp.get(), multipart.get());
        } catch (XmlPullParserException& e) {
            response.set_status(SOAP_CLIENT_ERROR);
            response.set_err(e.getMessage().c_str());
            DBGLOG("SOAP request from %s@%s. Parsing xml error: %s. Offending XML: [%s]", (userId&&*userId)?userId:"unknown",
                (peerStr.length()>0)?peerStr.str():"unknown", e.getMessage().c_str(), requeststr.str());
            return 0;
        } catch (...) {
            response.set_status(SOAP_CLIENT_ERROR);
            response.set_err("Unknown error when parsing soap body XML");
            IERRLOG("SOAP request from %s@%s. Unknown error when parsing: %s",  (userId&&*userId)?userId:"unknown",
                (peerStr.length()>0)?peerStr.str():"unknown", requeststr.str());
            return 0;
        }

        DBGLOG("SOAP method <%s> from %s@%s.", rpc_call->get_name(),  (userId&&*userId)?userId:"unknown",
            (peerStr.length()>0)?peerStr.str():"unknown");
        ctx->setHTTPMethod("SOAP");
    }

    ctx->setServiceMethod(rpc_call->get_name());

    // call the rpc and set the response
    if(m_soapbinding != NULL)
        m_soapbinding->processRequest(rpc_call, rpc_response);

    if (!response.getHttpResp() || !response.getHttpResp()->getRespSent())
    {
        response.set_status(rpc_response->get_status());
        response.set_err(rpc_response->get_err());

        //JSON content would've been sent, except certain errors, which don't need the following
        if (!(ctx && ctx->getResponseFormat()==ESPSerializationJSON))
        {
            Owned<CBody> res_body = new CBody;
            res_body->add_rpcmessage(rpc_response.get());

            Owned<CEnvelope> res_envelope;
            res_envelope.setown(new CEnvelope(NULL, res_body.getLink()));
            Owned<CMimeMultiPart> resp_multipart;
            resp_multipart.setown(new CMimeMultiPart("1.0", "", "MIME_boundary", "text/xml", "soaproot"));
            res_envelope->marshall(resp_multipart);

            StringBuffer contenttype;
            StringBuffer responsestr;
            resp_multipart->serialize(contenttype, responsestr);

            response.set_content_type(contenttype.str());
            response.set_text(responsestr.str());
        }
    }

    return 0;
}

