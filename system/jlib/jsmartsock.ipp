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


inline SmartSocketEndpoint *Array__Member2Param(SmartSocketEndpoint *&src){ return src; }
inline void Array__Assign(SmartSocketEndpoint * & dest, SmartSocketEndpoint * src){ dest = src; }
inline bool Array__Equal(SmartSocketEndpoint * const & m, SmartSocketEndpoint * const & p){ return m==p; }
inline void Array__Destroy(SmartSocketEndpoint *item){delete item;}
MAKEArrayOf(SmartSocketEndpoint*, SmartSocketEndpoint*, SmartSocketEndpointArray);


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

