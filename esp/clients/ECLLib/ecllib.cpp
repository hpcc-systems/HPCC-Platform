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
#define ECL_CLIENT_API _declspec (dllexport)
#else
#define ECL_CLIENT_API
#endif

//Jlib
#include "jliball.hpp"

//CRT
#include <stdio.h>

//SCM Interfaces
#include "ecllib.hpp"

//ESP Bindings
#include "soap/ecl/eclbind.hpp"

#include "eclclient.hpp"
#include <process.h>


Owned<IClientInstantEcl> g_theEclClient;

CClientInstantEcl::CClientInstantEcl()
{
    m_url[0] = 0;
    m_reqId = 0;
}

CClientInstantEcl::~CClientInstantEcl()
{
}

void CClientInstantEcl::addServiceUrl(const char * url)
{
    strcpy(m_url, url);
}

void CClientInstantEcl::removeServiceUrl(const char *url)
{
}

IClientInstantEclRequest * CClientInstantEcl::createRequest()
{
    CInstantEclRequest* request = new CInstantEclRequest;
    request->setUrl(m_url);

    return request;
}

IClientInstantEclResp* CClientInstantEcl::runEcl(IClientInstantEclRequest* request)
{
    if(strlen(m_url) == 0)
    {
        throw MakeStringException(-1, "url not set");
    }

    CInstantEclRequest* eclrequest = dynamic_cast<CInstantEclRequest*>(request);
    CInstantEclResponse* eclresponse = new CInstantEclResponse;
    eclresponse->setRequestId(m_reqId);
    
    m_reqId++;
    
    eclrequest->post(m_url, *eclresponse);
    return eclresponse;
}

void CClientInstantEcl::runEclAsync(IClientInstantEclRequest* request, IClientInstantEclEvents& events)
{
    if(strlen(m_url) == 0)
    {
        throw MakeStringException(-1, "url not set");
    }

    CInstantEclRequest* eclrequest = dynamic_cast<CInstantEclRequest*>(request);
    eclrequest->setReqId(m_reqId);
    eclrequest->setEvents(&events);

    m_reqId++;
    
    //Released in new thread.
    eclrequest->Link();
    DBGLOG("Starting query thread...");
    _beginthread(CInstantEclRequest::eclWorkerThread, 0, (void *)(eclrequest));
}

ECL_CLIENT_API IClientInstantEcl * createInstantEclClient()
{
   if (!g_theEclClient)
      g_theEclClient.setown(new CClientInstantEcl);

   return g_theEclClient.getLink();
}

