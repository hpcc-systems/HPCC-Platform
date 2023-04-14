/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include "httpsecurecontext.hpp"
#define MAX_ADDRESS_LEN 256

class CHttpSecureContext : extends CEspBaseSecureContext
{
private:
    CHttpRequest* m_request;

protected:
    IEspContext* queryContext() const
    {
        return (m_request ? m_request->queryContext() : NULL);
    }

public:
    CHttpSecureContext(CHttpRequest* request)
        : m_request(request)
    {
    }

    virtual ~CHttpSecureContext()
    {
    }

    virtual const char* getProtocol() const override
    {
        return "http";
    }

    virtual bool getProp(int type, const char* name, StringBuffer& value) override
    {
        bool result = false;

        switch (type)
        {
        case HTTP_PROPERTY_TYPE_COOKIE:
            result = getCookie(name, value);
            break;
        case HTTP_PROPERTY_TYPE_HEADER:
            result = getHeader(name, value);
            break;
        case HTTP_PROPERTY_TYPE_REMOTE_ADDRESS:
            result = getRemoteAddress(value);
            break;
        default:
            break;
        }

        return result;
    }

private:
    bool getCookie(const char* name, StringBuffer& value)
    {
        bool result = false;

        if (name && *name)
        {
            CEspCookie* cookie = m_request->queryCookie(name);

            if (cookie)
            {
                value.set(cookie->getValue());
                result = true;
            }
        }

        return result;
    }

    bool getHeader(const char* name, StringBuffer& value)
    {
        if (name && *name)
        {
            value.clear();

            if (m_request->getHeader(name, value).length())
                return true;
        }

        return false;
    }

    bool getRemoteAddress(StringBuffer& value)
    {
        char address[MAX_ADDRESS_LEN] = {0,};
        int  port = m_request->getSocket()->peer_name(address, MAX_ADDRESS_LEN);
        bool result = (*address != 0);

        if (result)
            value.set(address);

        return result;
    }
};

IEspSecureContextEx* createHttpSecureContext(CHttpRequest* request)
{
    return new CHttpSecureContext(request);
}
