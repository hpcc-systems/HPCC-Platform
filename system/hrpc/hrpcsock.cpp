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

// TCP/IP transport for HRPC
#include "platform.h"

#include "hrpc.hpp"
#include "hrpc.ipp"

#include "hrpcsock.hpp"
#include "jsocket.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

class HRPCsockettransport: public CInterface, implements IHRPCtransport 
{
public:
    IMPLEMENT_IINTERFACE;

    static CriticalSection critsect;

    HRPCsockettransport()
    {
        connectcount = 0;
        datasock = NULL;
        serversock = NULL;
        delsock = NULL;
        hostname = NULL;
        hostport = 0;
        timeout = (unsigned)WAIT_FOREVER;
        timeoutms = 0;
        connecttimeout = 60000;
        stopped = 1;
    }
    ~HRPCsockettransport()
    {
        Disconnect();
        StopListening();
        Unbind();
        if (delsock)
            delsock->Release();
        if (datasock)
            datasock->Release();
        if (serversock)
            serversock->Release();
        free(hostname);
        
    }
    virtual void Transmit(HRPCbuffer &c) // assume packet header at head
    {
        if (!datasock) {
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); // already disconnected
        }
        size32_t len = c.len();
        HRPCpacketheader *h=HRPCpacketheader::read(c);
        size32_t sz = c.len(); // length left
        h->size = sz;
        h->winrev();
        // 
        try {
            datasock->write(h,len); 
        }
        catch (IJSOCK_Exception *e) {
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_not_opened:       hrpcerr = HRPCERR_transport_not_open;   break;
            case JSOCKERR_timeout_expired:  hrpcerr = HRPCERR_call_timeout;         break;
            case JSOCKERR_broken_pipe:      hrpcerr = HRPCERR_lost_connection;      break;
            case JSOCKERR_graceful_close:   hrpcerr = HRPCERR_lost_connection;      break;
            default:
                throw;
            }

            // automatically disconnect on the above exceptions
            try {
                Disconnect();
            }
            catch (IJSOCK_Exception *e2) {  // ignore fails 
                e2->Release();
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        c.readptr(sz);
    }
    
    int readdata(void *p,size32_t sz,int timeo=-1)
    {
        if (!datasock) {
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); // already disconnected
        }
        size32_t retsz;
        try {
            if (timeo != -1) { // timeout overridden
                datasock->readtms(p,sz,sz,retsz,(unsigned)timeo);
            }
            else if ((timeout==(unsigned)WAIT_FOREVER)||!timeoutms) {
                datasock->read(p,sz,sz,retsz,timeout);
            }
            else {
                datasock->readtms(p,sz,sz,retsz,timeout*1000+timeoutms);
            }
        }
        catch (IJSOCK_Exception *e) {
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_not_opened:       hrpcerr = HRPCERR_transport_not_open;   break;
            case JSOCKERR_broken_pipe:      hrpcerr = HRPCERR_lost_connection;      break;
            case JSOCKERR_graceful_close:   hrpcerr = HRPCERR_lost_connection;      break;
            case JSOCKERR_timeout_expired:  hrpcerr = HRPCERR_call_timeout;         break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        return 1;
    }


    virtual void Receive(HRPCbuffer &c,int timeo)
    {
        c.ensure(sizeof(HRPCpacketheader));
        HRPCpacketheader *h=(HRPCpacketheader *)c.writeptr(sizeof(HRPCpacketheader));
        if (readdata(h,sizeof(HRPCpacketheader),timeo)) {
            h->winrev();
            unsigned sz=h->size;
            if (h->size==0)
                return;
            c.ensure(h->size);
            // h now invalid
            readdata(c.writeptr(sz), sz, timeo);
        }
    }

    virtual void Bind(int port,int qsize)
    {
        if (serversock) {
            serversock->Release();
            serversock = NULL;
        }
        try {
            serversock = ISocket::create(port,qsize);
        }
        catch (IJSOCK_Exception *e) {
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_port_in_use:          hrpcerr = HRPCERR_transport_port_in_use;    break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        assertex(serversock);
    }

    virtual void Bind(SocketEndpoint &endpoint,int qsize)
    {
        if (serversock) {
            serversock->Release();
            serversock = NULL;
        }
        try {
            StringBuffer ipname;
            endpoint.getIpText(ipname);
            serversock = ISocket::create_ip(endpoint.port,ipname.str(),qsize);
        }
        catch (IJSOCK_Exception *e) {
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_port_in_use:          hrpcerr = HRPCERR_transport_port_in_use;    break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        assertex(serversock);
    }

    virtual void Unbind()
    {
        if (serversock) {
            ISocket *sock = serversock;
            serversock = NULL;
            try {
                sock->close();
            }
            catch (IJSOCK_Exception *) {
                sock->Release();
                throw; // TBD
            }
            sock->Release();
        }
    }


    virtual bool Listen()
    {
        assertex(serversock);   // only on server
        assertex(stopped);
        Disconnect();
        stopped = 0;
        try {
            datasock = serversock->accept(true);
            if (datasock==NULL) {
                stopped = 1;
                return false;
            }
        }
        catch (IJSOCK_Exception *e)
        {
            if (stopped) {
                // check error here TBD
                e->Release();
                return false;
            }
            stopped = 1;
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_not_opened:           hrpcerr = HRPCERR_transport_not_open;   break;
            case JSOCKERR_invalid_access_mode:  hrpcerr = HRPCERR_internal;             break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        stopped = 1;
        return true;
    }

    virtual IHRPCtransport * Accept()
    {
        assertex(serversock);   // only on server
        assertex(stopped);
        Disconnect();
        stopped = 0;
        ISocket *sock=NULL;
        try {
            sock = serversock->accept();
        }
        catch (IJSOCK_Exception *e)
        {
            if (stopped) {
                // check error here TBD
                e->Release();
                return NULL;
            }
            stopped = 1;
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_not_opened:           hrpcerr = HRPCERR_transport_not_open;   break;
            case JSOCKERR_invalid_access_mode:  hrpcerr = HRPCERR_internal;             break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        stopped = 1;
        HRPCsockettransport *newtr = new HRPCsockettransport;
        newtr->datasock = sock;
        return newtr;
    }

    virtual void StopListening()
    {
        CriticalBlock critblock(critsect);
        if (!stopped) {
            stopped = 1;
            assertex(serversock);   // only on server
            assertex(!datasock);
            try {
                serversock->cancel_accept();
            }
            catch (IJSOCK_Exception *e)
            {   // should be no exceptions
                THROWHRPCEXCEPTIONEXC(HRPCERR_internal,e);
                e->Release();
            }
        }
    }

    void SetServerAddr(const char *ip,int portno)
    {
        free(hostname);
        if (ip&&*ip) {
            hostname = strdup(ip);
        }
        else {
            hostname = NULL;
        }
        if (portno)
            hostport = portno;
    }

    void SetServerAddr(SocketEndpoint &endpoint)
    {
        free(hostname);
        StringBuffer ipname;
        endpoint.getIpText(ipname);
        hostname = strdup(ipname.str());
        hostport = endpoint.port;
    }

    void GetServerAddr(SocketEndpoint &ep)
    {
        ep.set(hostname,hostport);
    }

    void SetName(const char *ip)
    {
        SetServerAddr(ip,0);
    }

    void Connect(int to)
    {
        assertex(serversock==NULL); // only on client
        assertex(hostname!=NULL);
        Disconnect();
        try {
            unsigned timeout = (to<0)?connecttimeout:to;
            SocketEndpoint ep(hostname,hostport);
            datasock = ISocket::connect_wait(ep,timeout);
        }
        catch (IJSOCK_Exception *e)
        {
            int hrpcerr;
            switch (e->errorCode()) {
            case JSOCKERR_bad_address:          hrpcerr = HRPCERR_bad_address;          break;
            case JSOCKERR_connection_failed:    hrpcerr = HRPCERR_connection_failed;    break;
            default:
                throw;
            }
            THROWHRPCEXCEPTIONEXC(hrpcerr,e);
            e->Release();
        }
        assertex(datasock);
    }

    void Attach(ISocket *sock)
    {
        assertex(serversock==NULL); // only on client
        assertex(hostname!=NULL);
        Disconnect();
        assertex(sock);
        datasock = LINK(sock);
    }


    void Disconnect()
    {
        CriticalBlock critblock(critsect);
        if (datasock ) {
            try {
                datasock->shutdown();
                datasock->close();
            }
            catch (IJSOCK_Exception *e)
            {
                if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close))
                {
                    // only system errors
                    ISocket * localsock = datasock;
                    ::Release(delsock);
                    delsock = datasock;
                    datasock = NULL;
                    throw;
                }
                e->Release();
            }
            ISocket * localsock = datasock;
            ::Release(delsock);
            delsock = datasock;
            datasock = NULL;
        }
    }

    bool  Name(char *name,size32_t namemax)
    {
        CriticalBlock critblock(critsect);
        name[0] = 0;
        if (datasock) {
            try {
                datasock->name(name,namemax);
            }
            catch (IJSOCK_Exception *e)
            {
                int hrpcerr;
                switch (e->errorCode()) {
                case JSOCKERR_not_opened:           hrpcerr = HRPCERR_transport_not_open;   break;
                default:
                    throw;
                }
                THROWHRPCEXCEPTIONEXC(hrpcerr,e);
                e->Release();
            }
        }
        return name[0]!=0;
    }

    bool  PeerName(char *name,size32_t namemax)
    {
        CriticalBlock critblock(critsect);
        name[0] = 0;
        if (datasock) {
            try {
                datasock->peer_name(name,namemax);
            }
            catch (IJSOCK_Exception *e)
            {
                int hrpcerr;
                switch (e->errorCode()) {
                case JSOCKERR_not_opened:           hrpcerr = HRPCERR_transport_not_open;   break;
                default:
                    throw;
                }
                THROWHRPCEXCEPTIONEXC(hrpcerr,e);
                e->Release();
            }
        }
        return name[0]!=0;
    }

    virtual void SetTimeout(int secs,int msecs)
    {
        if (secs<0) {
            timeout = (unsigned)WAIT_FOREVER;
            timeoutms = 0;
        }
        else {
            timeout = (unsigned)secs;
            timeoutms = (unsigned)msecs;
        }
    }

    virtual void SetConnectTimeout(unsigned timeout)
    {
        connecttimeout = timeout?timeout:(1000*60*60*24);

    }
    
    virtual Mutex &Sync()
    {
        return mx;
    }


protected:

    int      connectcount;
    ISocket *datasock;
    ISocket *serversock;
    ISocket *delsock;
    char    *hostname;
    unsigned short hostport;
    unsigned timeout;
    unsigned timeoutms;
    unsigned connecttimeout;
    int      stopped;
    Mutex mx;
};

CriticalSection HRPCsockettransport::critsect;


IHRPCtransport *MakeTcpTransport(   const char *target, // NULL for server, "" for not yet named
                                    int port,
                                    int listenqsize )
{
    HRPCsockettransport *t = new HRPCsockettransport;
    try {
        if (target) {
            t->SetServerAddr(target,port);
        }
        else if (port) {
            t->Bind(port,listenqsize);
        }
    }
    catch (...) {
        t->Release();
        t = NULL;
        throw;
    }
    return t;
}

bool getTcpTarget(IHRPCtransport *transport,SocketEndpoint &ep)
{
    HRPCsockettransport * ct = QUERYINTERFACE(transport, HRPCsockettransport);
    if (!ct)
        return false;
    ct->GetServerAddr(ep);
    return true;
}


IHRPCtransport *MakeClientTcpTransport( SocketEndpoint &endpoint )
{
    HRPCsockettransport *t = new HRPCsockettransport;
    t->SetServerAddr(endpoint);
    return t;
}

IHRPCtransport *MakeServerTcpTransport(int port, int listenqsize )
{
    HRPCsockettransport *t = new HRPCsockettransport;
    try {
        t->Bind(port,listenqsize);
    }
    catch (...) {
        t->Release();
        t = NULL;
        throw;
    }
    return t;
}

IHRPCtransport *MakeServerTcpTransport(SocketEndpoint &endpoint, int listenqsize )
{
    HRPCsockettransport *t = new HRPCsockettransport;
    try {
        t->Bind(endpoint,listenqsize);
    }
    catch (...) {
        t->Release();
        t = NULL;
        throw;
    }
    return t;
}




  
