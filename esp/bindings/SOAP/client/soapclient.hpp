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

#ifndef _SOAPCLIENT_HPP__
#define _SOAPCLIENT_HPP__

//Jlib
#include "jiface.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"

class esp_http_decl CSoapClient : implements ISoapClient, public CInterface
{
private:
    StringAttr                  m_username;
    StringAttr                  m_password;
    StringAttr                  m_realm;
    bool                        m_disableKeepAlive;
    Owned<ITransportClient> m_transportclient;
    int postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, 
                         StringBuffer& responsebuf, CMimeMultiPart* resp_multipart, IRpcMessageArray *headers=NULL);

public:
    IMPLEMENT_IINTERFACE

    CSoapClient(){m_disableKeepAlive=false;}
    CSoapClient(ITransportClient* transportclient){m_transportclient.setown(transportclient); m_disableKeepAlive=false;}
    virtual ~CSoapClient() {};
    virtual int setUsernameToken(const char* userid, const char* password, const char* realm);
    virtual void disableKeepAlive() { m_disableKeepAlive = true;}

    // Under most cases, use this one
    virtual int postRequest(IRpcMessage& rpccall, IRpcMessage& rpcresponse);

    // If you want to set a soapaction http header, use this one.
    virtual int postRequest(const char* soapaction, IRpcMessage& rpccall, IRpcMessage& rpcresponse);
    virtual int postRequest(const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf);
    virtual int postRequest(const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf, IRpcMessageArray *headers);

    // If you want a content-type that is different from "soap/application", use this one. This should not be necessary
    // unless the server side doesn't follow the standard.
    virtual int postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, IRpcMessage& rpcresponse);
    virtual int postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf);
    virtual int postRequest(const char* contenttype, const char* soapaction, IRpcMessage& rpccall, StringBuffer& responsebuf, IRpcMessageArray *headers);

    virtual int postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf);
    virtual int postRequest(IRpcMessage & rpccall, StringBuffer & responsebuf, IRpcMessageArray *headers);

    virtual const char * getServiceType() {return "EspSoapClient";};
};

#endif
