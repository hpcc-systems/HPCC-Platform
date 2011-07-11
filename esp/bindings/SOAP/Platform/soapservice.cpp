/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#pragma warning( disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//ESP Bindings
#include "platform.h"
#include <xpp/XmlPullParser.h>
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
                        WARNLOG("Couldn't create ISecUser object for %s", username.str());
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
                    {           
                        StringBuffer peerStr;
                        ctx->getPeer(peerStr);
                        DBGLOG("User authentication failed for soap request from %s", peerStr.str());
                        return SOAP_AUTHENTICATION_ERROR;
                    }
                    return 0;
                }
            }
        }
    }

    StringBuffer peerStr;
    ctx->getPeer(peerStr);
    const char* userId = ctx->queryUserId();
    DBGLOG("SOAP request from %s@%s", (userId&&*userId)?userId:"unknown", (peerStr.length()>0)?peerStr.str():"unknown");

    if(!authenticated)
    {
        DBGLOG("User authentication required");
        return SOAP_AUTHENTICATION_REQUIRED;
    }

    return 0;
}

int CSoapService::processRequest(ISoapMessage &req, ISoapMessage& resp)
{
    ESP_TIME_SECTION("CSoapService::processRequest()");

    CSoapRequest& request = *(dynamic_cast<CSoapRequest*>(&req));
    CSoapResponse& response = *(dynamic_cast<CSoapResponse*>(&resp));

    IEspContext* ctx = req.queryContext();
    
    StringBuffer requeststr;

    Owned<CMimeMultiPart> multipart;
    if(Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_SOAP, strlen(HTTP_TYPE_SOAP))==0 || Utils::strncasecmp(request.get_content_type(), HTTP_TYPE_TEXT_XML, strlen(HTTP_TYPE_TEXT_XML))==0)
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
    
    //Parse the content
    auto_ptr<XmlPullParser> xpp(new XmlPullParser());
    int bufSize = requeststr.length();
    xpp->setSupportNamespaces(true);
    xpp->setInput(requeststr.str(), bufSize);

    int type; 
    StartTag stag;
    EndTag etag;

    Owned<CEnvelope> req_envelope;
    req_envelope.setown(new CEnvelope);

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT) 
    {
        if(type == XmlPullParser::START_TAG) {
            xpp->readStartTag(stag);
            if(!stricmp(stag.getLocalName(), SOAP_ENVELOPE_NAME))
            {
                req_envelope->unmarshall(xpp.get());
                break;
            }
        }
    }
    
    CHeader* req_header = req_envelope->get_header();
    if(req_header != NULL)
    {
        // As headers are normally for common uses like authentication and routing, let's process it here
        // instead of in binding.
        int ret = processHeader(req_header, ctx);
        if(ret != 0 )
        {
            response.set_status(ret);
            return 0;
        }
    }

    CBody* req_body = req_envelope->get_body();
    Owned<CRpcResponse> rpc_response;
    rpc_response.setown(new CRpcResponse);
    rpc_response->setContext(req.queryContext());
    Owned<CRpcCall>rpc_call;
    rpc_call.setown(new CRpcCall);
    rpc_call->setContext(req.queryContext());
    
    try {
        req_body->nextRpcMessage(rpc_call.get());
        rpc_call->unmarshall(xpp.get(), multipart.get());
    } catch (XmlPullParserException& e) {
        response.set_status(SOAP_CLIENT_ERROR);
        response.set_err(e.getMessage().c_str());
        DBGLOG("parsing xml error: %s. Offending XML: [%s]", e.getMessage().c_str(), requeststr.str());
        return 0;
    } catch (...) {
        response.set_status(SOAP_CLIENT_ERROR);
        response.set_err("Unknown error when parsing soap body XML");
        ERRLOG("unknown error when parsing: %s", requeststr.str());
        return 0;
    }

    DBGLOG("SOAP method <%s>", rpc_call->get_name());

    // call the rpc and set the response
    if(m_soapbinding != NULL)
        m_soapbinding->processRequest(rpc_call, rpc_response);
        
    response.set_status(rpc_response->get_status());
    response.set_err(rpc_response->get_err());

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

    return 0;
}

