/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
