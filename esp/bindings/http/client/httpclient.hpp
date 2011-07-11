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

#ifndef _HTTPCLIENT_HPP__
#define _HTTPCLIENT_HPP__

#include "esphttp.hpp"
#include "soapesp.hpp"

interface IHttpClient : extends ITransportClient
{
    virtual void setProxy(const char* proxy) = 0;
    virtual void setUserID(const char* userid) = 0;
    virtual void setPassword(const char* password) = 0;
    virtual void setRealm(const char* realm) = 0;
    virtual void setTimeOut(unsigned int timeout) = 0;
    virtual void disableKeepAlive() = 0;

    virtual int sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response) = 0;
    virtual int sendRequest(const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent = false) = 0;
    virtual int sendRequest(IProperties *headers, const char* method, const char* contenttype, StringBuffer& request, StringBuffer& response, StringBuffer& responseStatus, bool alwaysReadContent = false) = 0;
    virtual int postRequest(ISoapMessage & request, ISoapMessage & response) = 0;

};

interface IHttpClientContext : extends IInterface
{
    virtual IHttpClient* createHttpClient(const char* proxy, const char* url) = 0;
};


ESPHTTP_API IHttpClientContext* getHttpClientContext();
ESPHTTP_API IHttpClientContext* createHttpClientContext(IPropertyTree* config);

#endif
