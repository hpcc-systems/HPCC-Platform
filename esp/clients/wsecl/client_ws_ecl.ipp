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

#ifndef _WS_ECL_CLIENT_HPP__
#define _WS_ECL_CLIENT_HPP__

#include "ws_ecl_client.hpp"

class CClientWsEclService : public CInterface,
   implements IClientWsEclService
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
