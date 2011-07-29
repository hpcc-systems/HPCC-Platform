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

// MP transport for HRPC
#include "platform.h"

#include "hrpc.hpp"
#include "hrpc.ipp"
#include "hrpcmp.hpp" 

#include "jsocket.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

#include <mpbase.hpp> 
#include <mpcomm.hpp> 

interface IMpTransportState : extends IInterface
{
public:
    virtual void Connect(int timeout)=0;
    virtual void Transmit(CMessageBuffer& mb)=0;
    virtual void Receive(CMessageBuffer& mb,int timeoutms=-1)=0;
    virtual bool Listen()=0;
    virtual void Cancel()=0;
    virtual bool getInfo(bool peer,SocketEndpoint &ep, mptag_t &tag)=0;
    virtual void Disconnect()=0;
};

class MpTransportStateCommon: public CInterface, implements IMpTransportState
{
protected:
    static CriticalSection critsect;

    mptag_t     servertag;
    mptag_t     clienttag;
    bool        cancelled;

public:
    IMPLEMENT_IINTERFACE;

    MpTransportStateCommon()
    {
        servertag = TAG_NULL;
        clienttag = TAG_NULL;
        cancelled = false;
    }

    bool connected()
    {
        return ((servertag!=TAG_NULL)&&(clienttag!=TAG_NULL));
    }

    void Disconnect()
    {
        servertag = TAG_NULL;
        clienttag = TAG_NULL;
    }

    void Cancel()
    {
        cancelled = true;
    }

    void timedout()
    {
        if (cancelled)
            THROWHRPCEXCEPTION(HRPCERR_internal);               
        THROWHRPCEXCEPTION(HRPCERR_call_timeout);
    }

};

CriticalSection MpTransportStateCommon::critsect;

class MpIntraClientState: public MpTransportStateCommon
{
    Owned<ICommunicator>    comm;
    rank_t                  serverrank;
    mptag_t                 connecttag;
public:
    MpIntraClientState(ICommunicator* _comm, rank_t _serverrank, mptag_t _connecttag)
    {
        comm.set(_comm); 
        serverrank = _serverrank;
        comm->verifyConnection(serverrank);
        connecttag = _connecttag;
    }

    void Connect(int timeout)
    {
        Disconnect();
        clienttag = createReplyTag();                   
        CMessageBuffer mb;
        serializeMPtag(mb,clienttag);
        comm->sendRecv(mb,serverrank, connecttag, timeout);  
        deserializeMPtag(mb,servertag);
    }

    void Transmit(CMessageBuffer &mb)
    {
        assertex(connected());
        verifyex(comm->send(mb,serverrank, servertag));
    }

    void Receive(CMessageBuffer &mb,int timeout)
    {
        //client only receives from server
        assertex(servertag!=TAG_NULL);
        if (!comm->recv(mb,serverrank, clienttag, NULL ,timeout)) 
            THROWHRPCEXCEPTION(HRPCERR_call_timeout);
    }

    bool getInfo(bool peer,SocketEndpoint &ep, mptag_t &tag)
    {
        CriticalBlock critblock(critsect);
        if (!connected())
            return false;
        if (peer) {
            ep = comm->queryGroup().queryNode(serverrank).endpoint();
            tag = servertag; 
        }
        else {
            ep = queryMyNode()->endpoint();
            tag = clienttag; 
        }
        return true;
    }

    bool  Listen()
    {   
        assertex(!"only on server");
        return false;
    }

};

class MpIntraServerState: public MpTransportStateCommon
{
    Owned<ICommunicator>    comm;
    mptag_t                 listentag;
    rank_t                  clientrank;
    bool                    listening;
public:

    MpIntraServerState(ICommunicator* _comm, mptag_t _listentag)
    {
        listening = false;
        comm.set(_comm); 
        clientrank = RANK_NULL;
        listentag = _listentag;
    }

    void Transmit(CMessageBuffer &mb)
    {
        if (!connected())
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); 
        assertex(clienttag!=TAG_NULL);
        comm->send(mb,clientrank,clienttag);
    }

    void Receive(CMessageBuffer &mb,int timeout)
    {
        if (!connected())
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); 
        if (!comm->recv(mb,clientrank, servertag,NULL, timeout))
            timedout();
    }

    bool  Listen()
    {
        CriticalBlock critblock(critsect);
        Disconnect();
        if (!cancelled) {
            listening = true;
            critsect.leave();
            CMessageBuffer mb;
            if (comm->recv(mb, RANK_ALL, listentag, &clientrank, MP_WAIT_FOREVER)) {
                critsect.enter();
                deserializeMPtag(mb,clienttag);
                servertag = createReplyTag();                   
                serializeMPtag(mb.clear(),servertag);
                comm->reply(mb);
            }
            else {
                critsect.enter();
                cancelled = true;
            }
            listening = false;
        }
        return !cancelled;
    }

    void Cancel()
    {
        CriticalBlock critblock(critsect);
        MpTransportStateCommon::Cancel();
        if (listening)
            comm->cancel(RANK_ALL, listentag);
    }

    bool getInfo(bool peer,SocketEndpoint &ep, mptag_t &tag)
    {
        CriticalBlock critblock(critsect);
        if (!connected())
            return false;
        if (peer) {
            ep = comm->queryGroup().queryNode(clientrank).endpoint();
            tag = clienttag; 
        }
        else {
            ep = queryMyNode()->endpoint();
            tag = servertag; 
        }
        return true;
    }

    void Connect(int timeout)
    {
        assertex(!"only on client");
    }

};

class MpInterClientState: public MpTransportStateCommon
{
    Owned<INode>            servernode;
    mptag_t                 connecttag;
public:
    MpInterClientState(INode* _servernode, mptag_t _connecttag)
    {
        servernode.set(_servernode);
        queryWorldCommunicator().verifyConnection(servernode);
        connecttag = _connecttag;
    }

    void Connect(int timeout)
    {
        Disconnect();
        clienttag = createReplyTag();               
        CMessageBuffer mbs;
        serializeMPtag(mbs,clienttag);
        queryWorldCommunicator().sendRecv(mbs,servernode,connecttag,timeout); 
        deserializeMPtag(mbs,servertag);
    }

    void Transmit(CMessageBuffer& mb)
    {
        assertex(connected());
        verifyex(queryWorldCommunicator().send(mb,servernode, servertag));
    }

    void Receive(CMessageBuffer &mb,int timeout)
    {
        assertex(connected());
        if (!queryWorldCommunicator().recv(mb,servernode, clienttag, NULL ,timeout)) 
            THROWHRPCEXCEPTION(HRPCERR_call_timeout);
    }

    bool getInfo(bool peer,SocketEndpoint &ep, mptag_t &tag)
    {
        CriticalBlock critblock(critsect);
        if (!connected())
            return false;
        if (peer) {
            ep = servernode->endpoint();
            tag = servertag; 
        }
        else {
            ep = queryMyNode()->endpoint();
            tag = clienttag; 
        }
        return true;
    }


    bool  Listen()
    {
        assertex(!"only on server");
        return false;
    }

};


class MpInterServerState: public MpTransportStateCommon
{
    Owned<INode>            clientnode;
    mptag_t                 listentag;
    bool                    listening;
public:
    MpInterServerState(mptag_t _listentag)
    {
        listening = false;
        listentag = _listentag;
    }

    void Transmit(CMessageBuffer &mb)
    {
        if (!connected())
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); 
        queryWorldCommunicator().send(mb,clientnode,clienttag);
    }

    void Receive(CMessageBuffer &mb,int timeout)
    {
        if (!connected())
            THROWHRPCEXCEPTION(HRPCERR_transport_not_open); 
        assertex(clienttag!=TAG_NULL);
        if (!queryWorldCommunicator().recv(mb,clientnode, servertag,NULL, timeout))
            timedout();
    }

    bool getInfo(bool peer,SocketEndpoint &ep, mptag_t &tag)
    {
        CriticalBlock critblock(critsect);
        if (!connected())
            return false;
        if (peer) {
            ep = clientnode->endpoint();
            tag = clienttag; 
        }
        else {
            ep = queryMyNode()->endpoint();
            tag = servertag; 
        }
        return true;
    }


    bool  Listen()
    {
        CriticalBlock critblock(critsect);
        Disconnect();
        if (!cancelled) {
            listening = true;
            CMessageBuffer mb;
            INode *node;
            critsect.leave();
            if (queryWorldCommunicator().recv(mb, NULL, listentag, &node, MP_WAIT_FOREVER)) {
                critsect.enter();
                clientnode.setown(node);
                deserializeMPtag(mb,clienttag);
                servertag = createReplyTag();                   
                serializeMPtag(mb.clear(),servertag);
                queryWorldCommunicator().reply(mb);
            }
            else {
                critsect.enter();
                cancelled = true;
            }
            listening = false;
        }
        return !cancelled;
    }

    void Cancel()
    {
        CriticalBlock critblock(critsect);
        MpTransportStateCommon::Cancel();
        if (listening)
            queryWorldCommunicator().cancel(NULL, listentag);
    }

    void Connect(int timeout)
    {
        assertex(!"only on client");
    }

};

class HRPCmptransport: public CInterface, implements IHRPCtransport 
{

protected:
    Owned<IMpTransportState>    state;
    CMessageBuffer              mb;

    int                         timeoutms;          
    int                         connecttimeoutms;           
    Mutex                       mx;

public:
    IMPLEMENT_IINTERFACE;

    HRPCmptransport(IMpTransportState* s)
    {
        state.setown(s);
        timeoutms = MP_WAIT_FOREVER;
        connecttimeoutms = MP_WAIT_FOREVER;
    }

    ~HRPCmptransport()
    {
        Disconnect();
        StopListening();
    }


    //Transmit data to rank (as declared in MakeMpTransport)
    void Transmit(HRPCbuffer &c) // assume packet header at head
    {
        size32_t len = c.len(); // complete left
        HRPCpacketheader* h=(HRPCpacketheader*)c.readptr(len); 
        size32_t sz = c.len(); // length left
        h->size = sz;
        h->winrev();
        mb.clear().append(len).append(len,h);
        state->Transmit(mb);
        c.readptr(sz); //...
    }

    
    //Receive will fill HRPCbuffer with what is heard (recieved message)
    void Receive(HRPCbuffer &c,int timeo)
    {
        mb.clear();
        size32_t size;
        if (timeo != -1) { // timeout overridden
            state->Receive(mb,timeo);
        }
        else 
            state->Receive(mb,timeoutms);
        mb.read(size);
        c.ensure(size);

        HRPCpacketheader *h=(HRPCpacketheader *)c.writeptr(size);

        mb.read(size,h);

        h->winrev();
    }

    //block until there is a message recieved by this node
    bool Listen()
    {   
        return state->Listen();
    }

    void StopListening()
    {
        state->Cancel();
    }

    void Disconnect()
    {
        state->Disconnect();
    }

    virtual IHRPCtransport * Accept()
    {
        if (Listen()==0)  //hrpcmp: state shared
            return NULL;    
        state->Link();
        return new HRPCmptransport(state);
    }


    bool  getName(bool peer, char *name, size32_t namemax)
    {
        if (namemax==0)
            return false;
        mptag_t tag;
        SocketEndpoint ep;
        if (!state->getInfo(peer,ep,tag)) {
            name[0] = 0;
            return false;
        }
        StringBuffer str;
        ep.getUrlStr(str).appendf("{%d}",(int)tag); // we'll use proper tag name when available
        size32_t l = str.length();
        if (l>=namemax)
            l = namemax-1;
        memcpy(name,str.str(),l);
        name[l] = 0;
        return true;
    }

    bool Name(char *name,size32_t namemax)
    {
        return getName(false,name,namemax);
    }

    bool PeerName(char *name,size32_t namemax)
    {
        return getName(true,name,namemax);
    }


    void Connect(int to)
    {
        if (to<=0)
            to = connecttimeoutms;
        state->Connect(to);
    }

    void SetConnectTimeout(unsigned timeoutms)
    {
        connecttimeoutms = timeoutms;
        if (connecttimeoutms<1000*60)
            connecttimeoutms = 1000*60;
    }

    void SetName(const char *name)
    {
        assertex(!"SetName not supported in MP HRPC");
    }
    
    void Attach(ISocket *sock)
    {
        assertex(!"Attach not supported in MP HRPC");

    }

    void SetTimeout(int secs,int msecs)  
    {
        if (secs<0) 
            timeoutms = MP_WAIT_FOREVER;
        else 
            timeoutms = secs*1000+msecs;
    }
    
    Mutex &Sync()  
    {
        return mx;
    }


};


HRPCmptransport* MakeHRPCmptransport(MpTransportStateCommon* s)
{
    return (new HRPCmptransport(s));
}

// Intra Communication
IHRPCtransport *MakeClientMpTransport( ICommunicator *comm, rank_t rank, mptag_t tag)
{
    if ((comm==NULL))
        THROWHRPCEXCEPTION(HRPCERR_bad_address);
    // line below: check valid rank value is between 0 and ordinatlity-1
    unsigned count  = comm->queryGroup().ordinality();
    if (rank>=count)
        THROWHRPCEXCEPTION(HRPCERR_bad_address);     
    MpIntraClientState * s = new MpIntraClientState(comm,rank,tag);
    return MakeHRPCmptransport(s);
};

IHRPCtransport *MakeServerMpTransport( ICommunicator *comm, mptag_t tag )
{
    if (comm==NULL)
        THROWHRPCEXCEPTION(HRPCERR_bad_address);
    MpIntraServerState * s = new MpIntraServerState(comm,tag);
    return MakeHRPCmptransport(s);
};
                                                                        
// Inter (using InterCommunicator)
IHRPCtransport *MakeClientMpInterTransport( INode *node, mptag_t tag)
{
    if (node==NULL)
        THROWHRPCEXCEPTION(HRPCERR_bad_address);
    MpInterClientState * s = new MpInterClientState(node,tag);
    return MakeHRPCmptransport(s);
};

IHRPCtransport *MakeServerMpInterTransport(mptag_t tag)
{
    MpInterServerState * s = new MpInterServerState(tag);
    return MakeHRPCmptransport(s);
};
