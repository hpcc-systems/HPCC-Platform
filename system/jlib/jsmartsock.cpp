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


#include "jsmartsock.ipp"
#include "jdebug.hpp"

ISmartSocketException *createSmartSocketException(int errorCode, const char *msg)
{
    return new SmartSocketException(errorCode, msg);
}

class SmartSocketListParser
{
public:
    SmartSocketListParser(const char * text)
    {
        fullText = strdup(text);
    }

    ~SmartSocketListParser()
    {
        free(fullText);
    }

    unsigned getSockets(SmartSocketEndpointArray &array, unsigned defport=0)
    {
        // IPV6TBD

        char *copyFullText = strdup(fullText);
        unsigned port = defport;

        char *saveptr;
        char *ip = strtok_r(copyFullText, "|", &saveptr);
        while (ip != NULL)
        {
            char *p = strchr(ip, ':');

            if (p)
            {
                *p = 0;
                p++;
                port = atoi(p);
            }

            if (isdigit(*ip))
            {
                char *dash = strrchr(ip, '-');
                if (dash)
                {
                    *dash = 0;
                    int last = atoi(dash+1);
                    char *dot = strrchr(ip, '.');
                    *dot = 0;
                    int first = atoi(dot+1);
                    for (int i = first; i <= last; i++)
                    {
                        StringBuffer t;
                        t.append(ip).append('.').append(i);
                        array.append(new SmartSocketEndpoint(t.str(), port));
                    }
                }
                else
                {
                    array.append(new SmartSocketEndpoint(ip, port));
                }
            }
            else
            {
                array.append(new SmartSocketEndpoint(ip, port));
            }
            ip = strtok_r(NULL, "|", &saveptr);
        }

        free(copyFullText);
        return array.ordinality();
    }

private:
    char *fullText;
};


class jlib_decl CSmartSocket: implements ISmartSocket, public CInterface
{
    ISocket *sock;
    SocketEndpoint ep;
    CSmartSocketFactory *factory;

public:
    IMPLEMENT_IINTERFACE;

    CSmartSocket(ISocket *_sock, SocketEndpoint &_ep, CSmartSocketFactory *_factory);
    ~CSmartSocket();

    // ISmartSocket
    ISocket *querySocket() { return (sock); }

    // subset of ISocket
    void read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                        unsigned timeout = WAIT_FOREVER);
    void read(void* buf, size32_t size);

    size32_t write(void const* buf, size32_t size);

    void close();
};


CSmartSocket::CSmartSocket(ISocket *_sock, SocketEndpoint &_ep, CSmartSocketFactory *_factory) : sock(_sock), ep(_ep), factory(_factory)
{
};


CSmartSocket::~CSmartSocket()
{
    if (sock)
        sock->Release();
};

void CSmartSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeout)
{
    try
    {
        sock->read(buf, min_size, max_size, size_read, timeout);
    }
    catch (IException *)
    {
        factory->setStatus(ep, false);

        if (sock != NULL)
        {
            sock->Release();
            sock = NULL;
        }

        throw;
    }
}

void CSmartSocket::read(void* buf, size32_t size)
{
    try
    {
        sock->read(buf, size);
    }
    catch (IException *)
    {
        factory->setStatus(ep, false);

        if (sock != NULL)
        {
            sock->Release();
            sock = NULL;
        }

        throw;
    }
}
    
size32_t CSmartSocket::write(void const* buf, size32_t size)
{
    try
    {
        return sock->write(buf, size);
    }
    catch (IException *)
    {
        factory->setStatus(ep, false);

        if (sock != NULL)
        {
            sock->Release();
            sock = NULL;
        }

        throw;
    }
}


void CSmartSocket::close()
{
    try
    {
        sock->close();
        sock->Release();
        sock = NULL;
    }
    catch (IException *)
    {
        factory->setStatus(ep, false);

        if (sock != NULL)
        {
            sock->Release();
            sock = NULL;
        }

        throw;
    }
}


CSmartSocketFactory::CSmartSocketFactory(const char *_socklist, bool _retry, unsigned _retryInterval, unsigned _dnsInterval)
{
    PROGLOG("CSmartSocketFactory::CSmartSocketFactory(%s)",_socklist?_socklist:"NULL");
    SmartSocketListParser slp(_socklist);
    if (slp.getSockets(sockArray) == 0)
        throw createSmartSocketException(0, "no endpoints defined");

    shuffleEndpoints();

    nextEndpointIndex = 0;
    dnsInterval=_dnsInterval;

    retry = _retry;
    if (retry)
    {
        retryInterval = _retryInterval;
        this->start();
    }
}


CSmartSocketFactory::~CSmartSocketFactory()
{
    stop();
}

void CSmartSocketFactory::stop()
{
    retry = false;
    this->join();
}

void CSmartSocketFactory::resolveHostnames() {
    for(unsigned i=0; i < sockArray.ordinality(); i++) {
        SmartSocketEndpoint *ep=sockArray.item(i);
        
        SmartSocketEndpoint resolveEP=*ep;

        resolveEP.ep.set(resolveEP.name.str(), resolveEP.ep.port);

        {
            synchronized block(lock);
            *ep=resolveEP;
        }
    }   
}

void CSmartSocketFactory::shuffleEndpoints()
{
    Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
    random->seed((unsigned)get_cycles_now());

    unsigned i = sockArray.ordinality();
    while (i > 1)
    {
        unsigned j = random->next() % i;
        i--;
        sockArray.swap(i, j);
    }
}


SmartSocketEndpoint *CSmartSocketFactory::nextSmartEndpoint()
{
    SmartSocketEndpoint *ss=sockArray.item(nextEndpointIndex);
    if (retry)
    {
        unsigned startEndpoint = nextEndpointIndex;
        while (!ss || !ss->status)
        {
            ++nextEndpointIndex %= sockArray.ordinality();
            if (startEndpoint == nextEndpointIndex)
                throw createSmartSocketException(0, "no endpoints are alive");
            ss = sockArray.item(nextEndpointIndex);
        }
    }
    ++nextEndpointIndex %= sockArray.ordinality();

    return ss;
}

SocketEndpoint& CSmartSocketFactory::nextEndpoint()
{
    SmartSocketEndpoint *ss=nextSmartEndpoint();
    if (!ss)
        throw createSmartSocketException(0, "smartsocket failed to get nextEndpoint");
    return (ss->ep);
}

ISmartSocket *CSmartSocketFactory::connect_timeout( unsigned timeoutms)
{
    SmartSocketEndpoint *ss = nextSmartEndpoint();
    if (!ss)
        throw createSmartSocketException(0, "smartsocket failed to get nextEndpoint");

    ISocket *sock = NULL;
    SocketEndpoint ep;
    try 
    {
        {
            synchronized block(lock);
            ss->checkHost(dnsInterval);
            ep = ss->ep;
        }
        if (timeoutms)
            sock = ISocket::connect_timeout(ep, timeoutms);
        else
            sock = ISocket::connect(ep);

        return new CSmartSocket(sock, ep, this);
    }
    catch (IException *e)
    {
        StringBuffer s("CSmartSocketFactory::connect ");
        ep.getUrlStr(s);
        EXCLOG(e,s.str());
        ss->status=false;
        if (sock)
            sock->Release();
        throw;
    }
}

ISmartSocket *CSmartSocketFactory::connect()
{
    return connect_timeout(0);
}

ISmartSocket *CSmartSocketFactory::connectNextAvailableSocket()
{
    while(1)
    {
        try 
        {
            return connect_timeout(1000);  // 1 sec
        }
        catch (ISmartSocketException *e)
        {
            throw e;
        }
        catch (IException *e)
        {
            e->Release();   //keep trying
        }
    }
    return NULL;  // should never get here, but make the compiler happy
}

int CSmartSocketFactory::run()
{
    unsigned idx;

    while (retry)
    {
        for(unsigned secs = 0; (secs < retryInterval) && retry; secs++)
            Sleep(1000);

        if(!retry)
            break;

        for (idx = 0; idx < sockArray.ordinality(); idx++)
        {
            SmartSocketEndpoint *ss=sockArray.item(idx);
            if (ss && !ss->status)
            {
                try
                {
                    synchronized block(lock);
                    ss->checkHost(dnsInterval);
                    Owned <ISocket> testSock = ISocket::connect_timeout(ss->ep, 1000);  // 1 sec
                    testSock->close();
                    ss->status = true;
                }
                catch (IException *e)
                {
                    // still bad - keep set to false
                    e->Release();
                }
            }
        }
    }

    return 0;
}


SmartSocketEndpoint *CSmartSocketFactory::findEndpoint(SocketEndpoint &ep)
{
    for (unsigned idx = 0; idx < sockArray.ordinality(); idx++)
    {
        SmartSocketEndpoint *ss=sockArray.item(idx);
        if (ss && ss->ep.equals(ep))
            return ss;
    }
    return NULL;
}


bool CSmartSocketFactory::getStatus(SocketEndpoint &ep)
{
    SmartSocketEndpoint *ss=findEndpoint(ep);
    return (ss && ss->status);
}


void CSmartSocketFactory::setStatus(SocketEndpoint &ep, bool status)
{
    SmartSocketEndpoint *ss=findEndpoint(ep);
    if (ss)
        ss->status=status;
}


ISmartSocketFactory *createSmartSocketFactory(const char *_socklist, bool _retry, unsigned _retryInterval, unsigned _dnsInterval) {
    return new CSmartSocketFactory(_socklist, _retry, _retryInterval, _dnsInterval);
}
