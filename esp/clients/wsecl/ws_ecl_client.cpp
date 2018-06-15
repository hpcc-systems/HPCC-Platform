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

#pragma warning(disable : 4786)

#define WS_ECL_CLIENT_API DECL_EXPORT

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
    strncpy(m_url, url, sizeof(m_url));
    m_url[sizeof(m_url)-1] = 0;
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
