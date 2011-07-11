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

#ifndef HRPC_HPP
#define HRPC_HPP

#include "platform.h"

#include "jexcept.hpp"
#include "jiface.hpp"

// HRPC runtime

#include "hrpcbuff.hpp" // inline buffer handling

class Mutex;

struct HRPCmoduleid
{
    char            modname[8];
    unsigned char   version;
};

class ISocket; // for attachConnect shortcut

interface IHRPCtransport: extends IInterface
{
public:
    virtual void Transmit(HRPCbuffer &c)=0;
    virtual void Receive(HRPCbuffer &c,int timeoutms=-1)=0;
    virtual bool Listen()=0;
    virtual void StopListening()=0;
    virtual void Connect(int timeoutms=-1)=0;
    virtual void Disconnect()=0;
    virtual bool Name(char *name,size32_t namemax)=0;
    virtual bool PeerName(char *name,size32_t namemax)=0;
    virtual void SetName(const char *name)=0;
    virtual void SetConnectTimeout(unsigned timeoutms)=0; // for connection
    virtual void SetTimeout(int secs,int msecs=0)=0; // for data transfer
    virtual IHRPCtransport *Accept()=0;  // used as an alternative to Listen
    virtual Mutex &Sync()=0;             // used for serializing access to transport
    virtual void Attach(ISocket *sock)=0;    // internal use to set socket used
};



class HRPCstub;
class HRPCmodule;


class HRPCserver : public CInterface, extends IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    HRPCserver(IHRPCtransport *it);
    HRPCserver();
    ~HRPCserver();
    void AttachStub(HRPCstub *);
    void DetachStub(HRPCstub *);
    void Listen(IHRPCtransport *it=NULL);
    void Run(IHRPCtransport *it); // transport obtained by Accept
    void Stop();
    int  CallerName(char *name,size32_t namemax) { if (transport) return transport->PeerName(name,namemax); return 0; }
    Mutex &Sync() { return transport->Sync(); }
    bool HandleAllExceptionsRemote(bool set=TRUE) { bool ret=returnexceptions; returnexceptions=set; return ret; }

protected: friend class HRPCcommon;
    bool returnexceptions;
protected: friend class HRPCstub;
    int async;
    HRPCbuffer inbuff;
    HRPCbuffer retbuff;
protected: friend class HRPCmodule;
    HRPCstub *stubs;
private:
    IHRPCtransport * transport;
    void SetTransport(IHRPCtransport *);
    void DoRun(size32_t base);
    enum { server_closed, server_listening, server_recv, server_instub } state;
};


struct HRPCcallframe;

class HRPCcommon // proxy and stub
{
public:
   HRPCcommon();
   virtual ~HRPCcommon();
    void StopServer() { if (_server) _server->Stop(); }
    void SetCallTimeLimit(long timemsecs) { timelimit = timemsecs; }
    bool HandleAllCallbackExceptionsRemote(bool set=TRUE) { bool ret=cbreturnexceptions; cbreturnexceptions=set; return ret; }
protected:
    void _returnOK(HRPCbuffer &b);
    virtual void _callbackstub(HRPCbuffer &buff,HRPCbuffer &obuff,int fn)=0;
    virtual void _stub(HRPCbuffer &buff,HRPCbuffer &obuff,int fn)=0;
    int cbnest;
    bool cbreturnexceptions;

protected: friend class HRPCserver;
    HRPCserver      *_server;
    HRPCmoduleid    *_id;
    void doproxy(HRPCcallframe &frame,int fn,IHRPCtransport *tr,HRPCbuffer &buff,HRPCbuffer &rbuff,int cb,int locked);
    void _sendexception(HRPCbuffer &rbuff,unsigned char exctype,int errcode,const char *str);
    long timelimit;

};


enum TRYCONNECTSTATE { TCfalse, TClocked, TCunlocked };

class HRPCmodule: public HRPCcommon
{
public:
    HRPCmodule();
    ~HRPCmodule();
    void UseTransport(IHRPCtransport *t) { if (lockcount) termconnection();
                                           assertex(!connected);
                                           if (t) { t->Link(); sync = &t->Sync(); } else sync = NULL;
                                           if (transport) transport->Release();
                                           transport = t;
                                         }
    void RenameTransport(const char *n)  { if (lockcount) termconnection();
                                           assertex(!connected);
                                           assertex(transport);
                                           transport->SetName(n);
                                         }
    void LockConnection()                { assertex(transport); lockcount++; }
    void UnlockConnection()              { assertex(transport); if (--lockcount==0) termconnection(); }
    bool ConnectionLocked()          { return lockcount>0; }
    bool TryConnect(int secs,bool raiseexcept=false,bool unlock=false);
    bool AttachConnect(ISocket *sock,bool leaveunlocked);   // allows a preconnected socket to be used (internal to MultiConnect)
    IHRPCtransport *queryTransport()     { return transport; }
protected:
    void _proxy(HRPCcallframe &frame,int fn);
    HRPCbuffer inbuff;
    HRPCbuffer retbuff;
protected: friend struct HRPCcallframe;
    Mutex *sync;
private:
    int lockcount;
    bool connected;
    TRYCONNECTSTATE tryconnected;
    IHRPCtransport *transport;
    void doconnect(int connect);
    void  callcallbackstub(int fn,HRPCbuffer  &rbuff);
    void _returnnull(HRPCbuffer &buff);
    void _stub(HRPCbuffer &buff,HRPCbuffer &obuff,int fn) { assertex(!"should not be called"); }
    void termconnection();
};

class HRPCstub : public HRPCcommon
{
public:
    HRPCstub();
    ~HRPCstub();
protected:
    void _callbackproxy(HRPCcallframe &frame,int fn);
    void _returnasync(HRPCbuffer &buff);
    HRPCbuffer cbbuff;
    HRPCbuffer cbretbuff;
    int  CallerName(char *name,size32_t namemax) { return _server->CallerName(name,namemax); }
    int  SetCallbackModule(HRPCmodule &m);
                                            
private:
    void callstub(int fn,HRPCbuffer  &rbuff);
    void _callbackstub(HRPCbuffer &buff,HRPCbuffer &obuff,int fn) { assertex(!"should not be called"); }
protected: friend class HRPCserver;
    HRPCstub        *_next;
};


struct HRPCcallframe
{
    HRPCcallframe(Mutex *m,HRPCbuffer &c);
    ~HRPCcallframe();
    HRPCbuffer &buff;
    size32_t        mark;
private:
    Mutex *sync;
};


extern "C" {

interface HRPCI_Clarion_Module
{
    virtual void _stdcall Link() const=0;
    virtual int _stdcall Release() const=0;
    virtual void _stdcall FreeMem(void *ref)=0;
};

interface HRPCI_Clarion_Transport
{
    virtual void _stdcall Link() const =0;
    virtual int _stdcall Release() const =0;
    virtual IHRPCtransport * _stdcall GetTransport()=0;
};

}



// ----------------------------------------------

enum HRPC_ERROR_CODES {
        HRPCERR_ok                      = 0,
        HRPCERR_lost_connection         = -1,
        HRPCERR_call_timeout            = -2,
        HRPCERR_transport_not_open      = -3,
        HRPCERR_internal                = -4,
        HRPCERR_bad_address             = -5,
        HRPCERR_connection_failed       = -6,
        HRPCERR_transport_port_in_use   = -7,
        HRPCERR_module_not_found        = -8,
        HRPCERR_mismatched_hrpc_version = -9
};


/*
Exceptions Raised:

IHRPCtransport
    Transmit
        HRPCERR_lost_connection,HRPCERR_transport_not_open
    Receive
        HRPCERR_lost_connection,HRPCERR_transport_not_open
    Listen
        HRPCERR_transport_not_open,HRPCERR_internal
    StopListening
        HRPCERR_internal
    Connect
        HRPCERR_connection_failed,HRPCERR_bad_address
    Name
        HRPCERR_transport_not_open
    PeerName
        HRPCERR_transport_not_open
    SetName
    Accept
        HRPCERR_transport_not_open,HRPCERR_internal


HRPCserver:
    Listen
        HRPCERR_lost_connection
    Run
        HRPCERR_lost_connection
    Stop

RPC (i.e. client side remote procedure call)
    HRPCERR_call_timeout
    HRPCERR_mismatched_hrpc_version
        


*/

interface IHRPC_Exception: extends IException
{
};

// Exceptions raised on server and passed back to client
interface IRemoteException:  extends IException
{
    enum RemoteExceptionKind
    {
        EX_STANDARD =1, // IException
        EX_STRING   =2, // const char *
        EX_HARDWARE =3, // e.g. GPFs etc
        EX_OTHER    =4  // other exception i.e. (...)
    };
    virtual RemoteExceptionKind kind()=0;
};


#endif
