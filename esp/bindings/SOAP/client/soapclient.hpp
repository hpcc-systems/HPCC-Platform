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

#ifndef _SOAPCLIENT_HPP__
#define _SOAPCLIENT_HPP__

#ifndef esp_http_decl
    #define esp_http_decl
#endif

//Jlib
#include "jiface.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"

class esp_http_decl CSoapClient : public CInterface, implements ISoapClient
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
    IMPLEMENT_IINTERFACE;

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
