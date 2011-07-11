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

#pragma warning(disable : 4786)

#ifdef _WIN32
#define WS_ECL_CLIENT_API _declspec (dllexport)
#else
#define WS_ECL_CLIENT_API
#endif

//Jlib
#include "jliball.hpp"

//CRT
#include <stdio.h>

//SCM Interfaces
#include "ws_ecl_client.hpp"

//ESP Bindings
#include "ws_ecl_client_bind.hpp"

#include "client_ws_ecl.ipp"

#ifdef _WIN32
#include <process.h>
#endif


Owned<IClientWsEclService> g_theClient;

CClientWsEclService::CClientWsEclService()
{
    m_url[0] = 0;
    m_reqId = 0;
}

CClientWsEclService::~CClientWsEclService()
{
}

void CClientWsEclService::addServiceUrl(const char * url)
{
    strcpy(m_url, url);
}

void CClientWsEclService::removeServiceUrl(const char *url)
{
}

const char* CClientWsEclService::queryServiceUrl()
{
    return m_url;
}

IClientWsEclRequest * CClientWsEclService::createRequest(const char *method)
{
    CClientWsEclRequest* request = new CClientWsEclRequest(method);
    request->setUrl(m_url);

    return request;
}

IClientWsEclResp* CClientWsEclService::search(IClientWsEclRequest* request)
{
    if(strlen(m_url) == 0)
    {
        throw MakeStringException(-1, "url not set");
    }

    CClientWsEclRequest* eclrequest = dynamic_cast<CClientWsEclRequest*>(request);
    Owned<CClientWsEclResponse> eclresponse = new CClientWsEclResponse;
    eclresponse->setRequestId(m_reqId);
    
    m_reqId++;
    
    eclrequest->post(m_url, *eclresponse);

    return eclresponse.getClear();
}


IClientWsEclResp* CClientWsEclService::searchEx(IClientWsEclRequest* request, const char *user, const char *pw, const char *realm)
{
    return searchEx(request,m_url, user, pw, realm);
}

IClientWsEclResp* CClientWsEclService::searchEx(IClientWsEclRequest* request,const char* URL, const char *user, const char *pw, const char *realm)
{
    if(strlen(URL) == 0)
        throw MakeStringException(-1, "url not set");

    CClientWsEclRequest* eclrequest = dynamic_cast<CClientWsEclRequest*>(request);
    Owned<CClientWsEclResponse> eclresponse = new CClientWsEclResponse;
    eclresponse->setRequestId(m_reqId);
    m_reqId++;

    eclrequest->post(URL, *eclresponse, user, pw, realm);

    return eclresponse.getClear();
}

IClientWsEclResp* CClientWsEclService::sendHttpRequest(IClientWsEclRequest* request, const char* method, const char* URL, 
                                                                                    const char *user, const char *pw, const char *realm,
                                                                                    const char* httpPostVariableName, bool encodeHttpPostBody)
{
    if(strlen(URL) == 0)
        throw MakeStringException(-1, "url not set");

    CClientWsEclRequest* eclrequest = dynamic_cast<CClientWsEclRequest*>(request);
    Owned<CClientWsEclResponse> eclresponse = new CClientWsEclResponse;
    eclresponse->setRequestId(m_reqId);
    m_reqId++;

    eclrequest->sendHttpRequest(*eclresponse, method, URL, user, pw, realm, httpPostVariableName, encodeHttpPostBody);
    return eclresponse.getClear();
}

void CClientWsEclService::searchAsync(IClientWsEclRequest* request, IClientWsEclEvents& events)
{
    if(strlen(m_url) == 0)
    {
        throw MakeStringException(-1, "url not set");
    }

    CClientWsEclRequest* eclrequest = dynamic_cast<CClientWsEclRequest*>(request);
    eclrequest->setRequestId(m_reqId);
    eclrequest->setEvents(&events);

    m_reqId++;
    
    //Released in new thread.
    eclrequest->Link();
    DBGLOG("Starting query thread...");
#ifdef _WIN32
    _beginthread(CClientWsEclRequest::eclWorkerThread, 0, (void *)(eclrequest));
#else
    UNIMPLEMENTED;
#endif
}

WS_ECL_CLIENT_API IClientWsEclService * getWsEclClient()
{
   if (!g_theClient)
      g_theClient.setown(new CClientWsEclService);

   return g_theClient.getLink();
}

WS_ECL_CLIENT_API IClientWsEclService * createWsEclClient()
{
   return new CClientWsEclService();
}
