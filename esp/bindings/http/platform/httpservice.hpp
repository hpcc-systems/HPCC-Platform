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

#ifndef _HTTPSERVICE_HPP__
#define _HTTPSERVICE_HPP__

//Jlib
#include "jiface.hpp"
#include "jstring.hpp"
#include "jsocket.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"

//ESP Bindings
#include "http/platform/httptransport.ipp"
#include "SOAP/Platform/soapbind.hpp"
#include "SOAP/Platform/soapmessage.hpp"

#include "espsession.ipp"


class CEspHttpServer : public CInterface, implements IHttpServerService
{
protected:
    ISocket&                m_socket;
    Owned<CHttpRequest>     m_request;
    Owned<CHttpResponse>    m_response;
    CEspApplicationPort*    m_apport;
    Owned<IEspRpcBinding>   m_defaultBinding;

    bool m_viewConfig;
    int m_MaxRequestEntityLength;

    int unsupported();
public:
    IMPLEMENT_IINTERFACE;

    CEspHttpServer(ISocket& sock, bool viewConfig, int maxRequestEntityLength);
    CEspHttpServer(ISocket& sock, CEspApplicationPort* apport, bool viewConfig, int maxRequestEntityLength);
    virtual ~CEspHttpServer();

    //IEspService
    bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
    {
        return true;
    }
    bool rootAuth(IEspContext* ctx);

    virtual int processRequest();

    virtual int onPost();
    virtual int onGet();

    virtual int onGetFile(CHttpRequest* request, CHttpResponse* response, const char *path);
    virtual int onGetXslt(CHttpRequest* request, CHttpResponse* response, const char *path);

    virtual int onGetBuildSoapRequest(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetApplicationFrame(CHttpRequest* request, CHttpResponse* response, IEspContext* ctx);
    virtual int onGetTitleBar(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetNavWindow(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetDynNavData(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetNavEvent(CHttpRequest* request, CHttpResponse* response);
    virtual int onGetMainWindow(CHttpRequest* request, CHttpResponse* response);
    virtual int onRunCGI(CHttpRequest* request, CHttpResponse* response, const char *path);



    virtual const char * getServiceType() {return "HttpServer";};
};


#endif
