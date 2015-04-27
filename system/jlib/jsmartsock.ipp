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


#ifndef JSMARTSOCK_IPP
#define JSMARTSOCK_IPP

#include "jsmartsock.hpp"
#include "jlog.hpp"


struct SmartSocketEndpoint
{
    SocketEndpoint ep;
    StringBuffer name;
    unsigned lastHostUpdate;
    bool status;

    SmartSocketEndpoint(const char *_name,unsigned short port=0) : ep(_name, port)
    {
        if (ep.isNull()) 
            throw MakeStringException(-1,"SmartSocketEndpoint resolution failed for '%s' %d",_name,port);
        StringBuffer ipStr;
        ep.getIpText(ipStr);
        if (strcmp(ipStr.str(), _name)!=0)
            name.append(_name);
        lastHostUpdate=msTick();
        status = true;
    }

    void checkHost(unsigned interval)
    {
        if (interval!=((unsigned) -1))
        {
            if (name.length() && ((msTick()-lastHostUpdate)>(interval*1000)))
            {
                ep.set(name, ep.port);
                lastHostUpdate=msTick();
            }
        }
    }
};


class SmartSocketEndpointArray : public SafePointerArrayOf<SmartSocketEndpoint> {};

class jlib_decl CSmartSocketFactory: public Thread,
    implements ISmartSocketFactory
{
    SmartSocketEndpointArray sockArray;
    Mutex lock;

    unsigned nextEndpointIndex;
    bool retry;
    unsigned retryInterval;
    unsigned dnsInterval;

    void shuffleEndpoints();
    SmartSocketEndpoint *findEndpoint(SocketEndpoint &ep);

public:
    IMPLEMENT_IINTERFACE;

    CSmartSocketFactory(const char *_socklist, bool _retry = false, unsigned _retryInterval = 60, unsigned _dnsInterval = (unsigned)-1);
    ~CSmartSocketFactory();
    int run();

    bool getStatus(SocketEndpoint &ep);
    void setStatus(SocketEndpoint &ep, bool status);

    ISmartSocket *connect();

    ISmartSocket *connect_timeout( unsigned timeoutms = 0);

    ISmartSocket *connectNextAvailableSocket();

    SmartSocketEndpoint *nextSmartEndpoint();
    SocketEndpoint& nextEndpoint();

    virtual void stop();

    virtual void resolveHostnames();
};


class jlib_thrown_decl SmartSocketException: public CInterface, public ISmartSocketException
{
public:
    IMPLEMENT_IINTERFACE;

    SmartSocketException(int code, const char *_msg) : errcode(code), msg(_msg) { };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("CSmartSocket: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };

private:
    int errcode;
    StringAttr msg;
};

ISmartSocketException *createSmartSocketException(int errorCode, const char *msg)
{
    return new SmartSocketException(errorCode, msg);
}

#endif

