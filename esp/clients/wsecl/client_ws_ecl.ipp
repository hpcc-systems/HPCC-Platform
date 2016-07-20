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

#ifndef _WS_ECL_CLIENT_HPP__
#define _WS_ECL_CLIENT_HPP__

#include "ws_ecl_client.hpp"

class CClientWsEclService : implements IClientWsEclService, public CInterface
{
private:
    char m_url[256];
    long m_reqId;
protected:
    
public:
    IMPLEMENT_IINTERFACE;

    CClientWsEclService();
    virtual ~CClientWsEclService();

    virtual void        addServiceUrl(const char * url);
    virtual void        removeServiceUrl(const char * url);
    virtual const char* queryServiceUrl();


    virtual IClientWsEclRequest* createRequest(const char *methodName);
    virtual IClientWsEclResp* search(IClientWsEclRequest* request);
    virtual IClientWsEclResp* searchEx(IClientWsEclRequest* request, const char *user, const char *pw, const char *relm);
    virtual IClientWsEclResp* searchEx(IClientWsEclRequest* request,const char* URL, const char *user, const char *pw, const char *relm);
    virtual IClientWsEclResp* sendHttpRequest( IClientWsEclRequest* request,const char* method, const char* URL, const char *user, 
                                                                        const char *pw, const char *realm, const char* httpPostVariableName, bool encodeHttpPostBody);

    virtual void searchAsync(IClientWsEclRequest* request, IClientWsEclEvents &events);

};



#endif
