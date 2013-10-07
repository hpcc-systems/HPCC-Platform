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

#include "platform.h"

#include "hrpc.hpp"
#include "hrpc.ipp"
#include <time.h>
#ifdef _VER_C5
    #undef _HRPCTRACE
#else
    #include <ctype.h>
    #include <stdarg.h>
#endif

#include "jmutex.hpp"
#include "jsocket.hpp"

#define RETURNEXCEPTIONSDEFAULT true   // set false to stop exceptions being returned accross RPC


#ifdef _HRPCTRACE

int started=0;
static const char *trcfile="hrpctrc.txt";
#include <stdio.h> 
#include <time.h>
#include "jdebug.hpp"

void HRPCtrace(const char *fmt, ...)
{
    static char buf[0x4000];
    va_list args;
    va_start(args, fmt);
    vsprintf(buf, fmt, args);
    va_end(args);
    DBGLOG("%s",buf);
}


#else 
inline void HRPCtrace(const char *, ...) {}
#endif


class HRPCException: public CInterface, public IHRPC_Exception
{
public:
    IMPLEMENT_IINTERFACE;
    HRPCException(int code) : errcode(code) { modname[0]=0; };
    HRPCException(int code,HRPCmoduleid &id) : errcode(code) 
    { 
        memcpy(modname,id.modname,sizeof(id.modname));
        modname[sizeof(id.modname)]=0; 
        modver = id.version;
    };
    
    HRPCException(int code,IException *e) : errcode(code) 
    {
        if (e)
            e->errorMessage(excstr);
        modname[0] = 0;
    }

    int errorCode() const
    {
       return errcode;
    }
    
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        if (modname[0]) {
            str.append(modname);
            str.append(".");
            str.append(modver);
            str.append(": ");
        }
        switch (errcode) {
        case HRPCERR_ok:                        str.append("ok"); break;
        case HRPCERR_lost_connection:           str.append("lost connection"); break;
        case HRPCERR_call_timeout:              str.append("call timeout"); break;
        case HRPCERR_transport_not_open:        str.append("transport not open"); break;
        case HRPCERR_internal:                  str.append("internal"); break;
        case HRPCERR_bad_address:               str.append("bad address"); break;
        case HRPCERR_connection_failed:         str.append("connection failed"); break;
        case HRPCERR_transport_port_in_use:     str.append("transport port in use"); break;
        case HRPCERR_module_not_found:          str.append("module not found or mismatched version"); break;
        default:                                str.append("unknown (").append(errcode).append(")");
        }
        if (excstr.length()) {
            str.append(" [");
            str.append(excstr.toCharArray());
            str.append(']');
        }
        return str;
    }   

    MessageAudience errorAudience() const 
    { 
        switch (errcode) {
        case HRPCERR_lost_connection:           
        case HRPCERR_connection_failed:         
        case HRPCERR_transport_port_in_use:     
            return MSGAUD_operator; 
        }
        return MSGAUD_user; 
    }
private:
    int     errcode;
    char    modname[sizeof(HRPCmoduleid)]; // use version for NULL
    StringBuffer excstr;
    unsigned modver;
};


IHRPC_Exception *MakeHRPCexception(int code)
{
    return new HRPCException(code);
}

IHRPC_Exception *MakeHRPCexception(int code,HRPCmoduleid &id)
{
    return new HRPCException(code,id);
}

IHRPC_Exception *MakeHRPCexception(int code,IException *e)
{
    return new HRPCException(code,e);
}

class CRemoteException: public CInterface, implements IRemoteException
{
public:
    IMPLEMENT_IINTERFACE;
    CRemoteException(int code,const char *str,int kind) : msg(str),errcode(code),errkind((RemoteExceptionKind)kind) {};
    
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const { str.append(msg); return str;}   
    MessageAudience errorAudience() const { return MSGAUD_user; }
    RemoteExceptionKind      kind() { return errkind; }
    static IRemoteException *Make(int code,const char *str,int kind) { return new CRemoteException(code,str,kind); }

protected:
    int                     errcode;
    RemoteExceptionKind     errkind;
    StringAttr msg;
};  

struct Csaveseh
{
    bool save;
    Csaveseh(bool e) 
    {
        save = e;
        if (e) 
            EnableSEHtoExceptionMapping();
    }
    ~Csaveseh() 
    {
        if (save) 
            DisableSEHtoExceptionMapping();
    }
};


HRPCcommon::HRPCcommon()
{
    _server = NULL;
    cbnest = 0;
    timelimit = 0;
    cbreturnexceptions = RETURNEXCEPTIONSDEFAULT;
}

HRPCcommon::~HRPCcommon()
{
}



HRPCmodule::HRPCmodule()
{
    lockcount = 0;
    connected = false;
    transport = NULL;
    tryconnected = TCfalse;
    sync = NULL;
}

HRPCmodule::~HRPCmodule()
{
    termconnection();
    ::Release(transport);
}

void HRPCmodule::termconnection()
{ 
    // forced termination of connection - does not fail if connection has already gone down
    if (connected && transport) {
        transport->Sync().lock(); // just in case
        connected=false; 
        try {
            _returnnull(retbuff);
        }
        catch (IException *e) {
            e->Release();
        }
        try {
            transport->Disconnect(); 
        }
        catch (IException *e) {
            e->Release();
        }
        transport->Sync().unlock();
    } 
}


HRPCstub::HRPCstub()
{
}

HRPCstub::~HRPCstub()
{
    if (_server)
        _server->DetachStub(this);
}

void HRPCcommon::_returnOK(HRPCbuffer &buff)
{
    buff.rewrite();
    HRPCpacketheader *h = HRPCpacketheader::write(buff);
    h->flags = HRPCendian|HRPCpacketheader::PFreturn;
}

void HRPCmodule::_returnnull(HRPCbuffer &buff)
{
    buff.rewrite();
    HRPCpacketheader *h = HRPCpacketheader::write(buff);
    h->flags = HRPCendian|HRPCpacketheader::PFreturn;
    assertex(transport);
    transport->Transmit(buff);
}


void HRPCstub::_returnasync(HRPCbuffer &buff)
{
    buff.rewrite();
    HRPCpacketheader *h = HRPCpacketheader::write(buff);
    h->flags = HRPCendian|HRPCpacketheader::PFreturn;
    assertex(_server);
    assertex(_server->transport);
    _server->transport->Transmit(buff);
    _server->async = 1;
}

static void dosendexception(HRPCbuffer &rbuff,unsigned char exctype,int errcode,const char *modname,const char *str)
{
    // assumes ReturnOK not done
    rbuff.rewrite();
    StringBuffer msg;
    if (modname) {
        unsigned i;
        for (i=0;i<8;i++) {
            if (modname[i]) 
                msg.append(modname[i]);
            else
                break;
        }
    }
    msg.append('[');
    GetHostName(msg).append(':').append(str).append(']');
    HRPCpacketheader *h = HRPCpacketheader::write(rbuff);
    h->flags = HRPCendian|HRPCpacketheader::PFexception;
    _WINREV(errcode);
    rbuff.write(&errcode,sizeof(errcode));
    rbuff.write(&exctype,sizeof(exctype));
    rbuff.writestr(msg.str());
}
        
void HRPCcommon::_sendexception(HRPCbuffer &rbuff,unsigned char exctype,int errcode,const char *str)
{
    dosendexception(rbuff,exctype,errcode,_id?_id->modname:NULL,str);
}


struct SafeInc
{
    int &val;
    SafeInc(int &v) : val(v) { val++; }
    ~SafeInc() { val--; }
};
void HRPCcommon::doproxy(HRPCcallframe &frame,int fn,IHRPCtransport *tr,HRPCbuffer &buff,HRPCbuffer &rbuff,int cb,int locked)
{
    unsigned duetime = 0;
    if (timelimit)
        duetime = msTick()+timelimit;
    size32_t base=buff.markwrite();
    HRPCtrace(">doproxy in fn=%d base=%d cb=%d locked=%d\n",fn,base,cb,locked);
    HRPCpacketheader *h=(HRPCpacketheader *)buff.buffptr(frame.mark);
    h->function = fn; 
    memcpy(&h->module,_id,sizeof(h->module));
    h->flags = HRPCendian;
    if (cb||cbnest) {
        h->flags |= HRPCpacketheader::PFcallback;
    }
    else if (locked) {
        h->flags |= HRPCpacketheader::PFlocked;
    }
    HRPCtrace(" doproxy pre-transmit(1)\n");
    tr->Transmit(buff);
    HRPCtrace(" doproxy post-transmit(1)\n");
    SafeInc si(cbnest);
    while (1) {
        buff.releasewrite(base);
        int timeleft=-1;
        HRPCtrace(" doproxy pre-receive\n");
        if (timelimit) {
            timeleft = (duetime-msTick());
            if (timeleft<=0) {
                THROWHRPCEXCEPTION(HRPCERR_call_timeout);
            }
        }
        tr->Receive(buff,timeleft);
        HRPCtrace(" doproxy post-receive\n");
        h = HRPCpacketheader::read(buff);
        if (h->flags&HRPCpacketheader::PFexception) {
            int errcode;
            buff.read(&errcode,sizeof(errcode));
            _WINREV(errcode);
            unsigned char exctype;
            buff.read(&exctype,sizeof(exctype));
            throw(CRemoteException::Make(errcode,buff.readstrptr(),exctype));
        }   
        else if (h->flags&HRPCpacketheader::PFcallback) {
            Csaveseh saveseh(cbreturnexceptions);
            try {
                if (cb) {
                    HRPCtrace(">pre stub %d\n",h->function);
                    _stub(buff,rbuff,h->function);
                    HRPCtrace("<post stub %d\n",h->function);
                }
                else {
                    HRPCtrace(">pre callbackstub %d\n",h->function);
                    _callbackstub(buff,rbuff,h->function);
                    HRPCtrace("<post callbackstub %d\n",h->function);
                }
            }
            catch (IHRPC_Exception *e) { // don't pass back HRPC exceptions
                throw e;
            }
            catch (ISEH_Exception *e) {
                StringBuffer msg;
                _sendexception(rbuff,IRemoteException::EX_HARDWARE,e->errorCode(),e->errorMessage(msg).toCharArray());
                e->Release();
            }
            catch (IException *e) {
                StringBuffer msg;
                _sendexception(rbuff,IRemoteException::EX_STANDARD,e->errorCode(),e->errorMessage(msg).toCharArray());
                e->Release();
            }
            catch (const char *s) {
                _sendexception(rbuff,IRemoteException::EX_STRING,-1,s);
            }
            catch (...) {
                if (!cbreturnexceptions)
                    throw;
                _sendexception(rbuff,IRemoteException::EX_OTHER,-1,"Unknown Remote Exception");
            }

            HRPCtrace(" doproxy pre-transmit(2)\n");
            tr->Transmit(rbuff);
            HRPCtrace(" doproxy post-transmit(2)\n");
        }
        else
            break;
    }
    timelimit = 0;
    HRPCtrace("<doproxy out\n");
}

bool HRPCmodule::TryConnect(int msecs,bool raiseex,bool leaveunlocked)
{
    if (!transport||!msecs)
        return false;
    sync->lock();
    bool ret = true;
    if (connected||(cbnest!=0)) {
        // test link here?
        sync->unlock();
    }
    else {
        ret = false;
        try {
            transport->Connect(msecs);
            ret = true;
            if (leaveunlocked) {
                sync->unlock();
                tryconnected = TCunlocked;
            }
            else
                tryconnected = TClocked;

        }
        catch (IHRPC_Exception *e) { 
            sync->unlock();
            if (raiseex) 
                throw e;
        }
        catch (IException *e) {
            sync->unlock();
            if (raiseex) 
                throw e;
            e->Release();
        }
        catch (...) {
            sync->unlock();
            if (raiseex) 
                throw;
        }
    }
    return ret;
}

bool HRPCmodule::AttachConnect(ISocket *sock,bool leaveunlocked)
{
    if (!transport)
        return false;
    sync->lock();
    bool ret = true;
    if (connected||(cbnest!=0)) {
        // test link here?
        sync->unlock();
    }
    else {
        ret = false;
        transport->Attach(sock);
        ret = true;
        if (leaveunlocked) {
            sync->unlock();
            tryconnected = TCunlocked;
        }
        else
            tryconnected = TClocked;

    }
    return ret;
}


void HRPCmodule::_proxy(HRPCcallframe &frame,int fn)
{
//  synchronized proc(*sync); // not required callframe locks
    assertex(&frame.buff==&inbuff);
    assertex(transport);
    bool doconn = (!connected)&&(cbnest==0);
    bool locked = ConnectionLocked();

    if (tryconnected!=TCfalse) {
        if (tryconnected==TClocked)
            sync->unlock(); // STILL LOCKED BY THE FRAME
        tryconnected = TCfalse;
        connected = true;
    }
    else if (doconn) {
        transport->Connect();
        connected = true;
    }
    try {
        doproxy(frame,fn,transport,inbuff,retbuff,0,locked);
    }
    catch (IHRPC_Exception *e) { // always disconnect
        transport->Disconnect();
        connected = false;
        throw e;
    }
    catch (...) {
        if (doconn&&!locked) {
            transport->Disconnect();
            connected = false;
        }
        throw;
    }
    if (doconn&&!locked) {
        transport->Disconnect();
        connected = false;
    }
}

void HRPCstub::_callbackproxy(HRPCcallframe &frame,int fn)
{
    assertex(_server);
    assertex(&frame.buff==&cbbuff);
    assertex(_server->transport);
    doproxy(frame,fn,_server->transport,cbbuff,cbretbuff,1,0);
}

int HRPCstub::SetCallbackModule(HRPCmodule &m)
{
    // assumes transport set up
    char name[32];
    if (CallerName(name,sizeof(name))) {
        m.RenameTransport(name);
        return 1;
    }
    return 0;
}



HRPCserver::HRPCserver(IHRPCtransport *it)
{
    transport = LINK(it);
    state = server_closed;
    stubs = NULL;
    returnexceptions = RETURNEXCEPTIONSDEFAULT;
}

HRPCserver::HRPCserver()
{
    transport = NULL;
    state = server_closed;
    stubs = NULL;
    returnexceptions = RETURNEXCEPTIONSDEFAULT;
}

HRPCserver::~HRPCserver()
{
    while (stubs!=NULL) {
        DetachStub(stubs);
    }
    ::Release(transport);
}

void HRPCserver::AttachStub(HRPCstub *s)
{
    s->_next = stubs;
    stubs = s;
    s->_server = this;
}

void HRPCserver::DetachStub(HRPCstub *s)
{
    if (!s)
        return;
    s->_server = NULL;
    HRPCstub *prev=NULL;
    HRPCstub *sp=stubs;
    while (sp) {
        if (s==sp) {
            if (prev)
                prev->_next = sp->_next;
            else
                stubs = sp->_next;
        }
        prev = sp;
        sp = sp->_next;
    }
}

void HRPCserver::Listen(IHRPCtransport * trans)
{
    if (trans) {
        ::Release(transport);
        transport = LINK(trans);
    }
    size32_t base=inbuff.markwrite();
    assertex(state==server_closed); // recursion check (TBD?)
    state=server_listening;
    try {
        while ((state!=server_closed) && transport->Listen()) {

#ifdef _DEBUG_MEM
            _CrtMemState start, end, diff;  
            PrintLog("\n====================== Memory leak check point starts ======================");
            _CrtMemCheckpoint(&start);
#endif

            DoRun(base);

#ifdef _DEBUG_MEM
            _CrtMemCheckpoint(&end);
            if (_CrtMemDifference(&diff,&start,&end))
            {
                fprintf(stderr,"\n*********** Memory leak detected! **********");
                PrintLog("Memory leak detected!");
                _CrtMemDumpStatistics(&diff);
            }
            PrintLog("\n====================== Memory leak check point ends ========================");
#endif

            if (state!=server_closed) {
                state = server_listening;
            }
        }
    }
    catch (...) {
        if (trans!=NULL) {
            transport->Release();
            transport = NULL;
        }
        state = server_closed;
        inbuff.releasewrite(base);
        throw;
    }
    if (trans!=NULL) {
        transport->Release();
        transport = NULL;
    }
    inbuff.releasewrite(base);
    state = server_closed;
}

void HRPCserver::DoRun(size32_t base)
{
    HRPCtrace(">DoRun enter\n");
    Csaveseh saveseh(returnexceptions);
    state = server_recv;
    int lockcontinue=0;
    do {
        HRPCtrace(" DoRun pre-receive base=%d lockcontinue=%d\n",base,lockcontinue);
        transport->Receive(inbuff);
        HRPCtrace(" DoRun post-receive \n");
        HRPCpacketheader *head=HRPCpacketheader::read(inbuff);
        if (head->flags&HRPCpacketheader::PFlocked) {
            lockcontinue = 1;
        }
        else {
            if (lockcontinue)
                break;
        }
        HRPCstub *sp=stubs;
        while (sp) {
            if (memcmp(sp->_id,&head->module,sizeof(head->module))==0) {
                async=0;
                if (returnexceptions) {
                    try {
                        state = server_instub;
                        HRPCtrace(" DoRun pre-stub \n");
                        sp->_stub(inbuff,retbuff,head->function);
                        HRPCtrace(" DoRun post-stub \n");
                        if (state==server_closed) {
                            lockcontinue = 0;
                        }
                        else {
                            state = server_recv;
                        }
                    }
                    catch (IHRPC_Exception *e)
                    { // one of mine!
                        throw e;
                    }
                    catch (ISEH_Exception *e) {
                        if (async) throw e;
                        StringBuffer msg;
                        sp->_sendexception(retbuff,IRemoteException::EX_HARDWARE,e->errorCode(),e->errorMessage(msg).toCharArray());
                        e->Release();
                    }
                    catch (IException *e) {
                        if (async) throw e;
                        StringBuffer msg;
                        sp->_sendexception(retbuff,IRemoteException::EX_STANDARD,e->errorCode(),e->errorMessage(msg).toCharArray());
                        e->Release();
                    }
                    catch (const char *s) {
                        if (async) throw s;
                        sp->_sendexception(retbuff,IRemoteException::EX_STRING,-1,s);
                    }
                    catch (...) {
                        if (async) throw;
                        sp->_sendexception(retbuff,IRemoteException::EX_OTHER,-1,"Unknown Remote Exception");
                    }
                }
                else {
                    state = server_instub;
                    HRPCtrace(" DoRun pre-stub \n");
                    sp->_stub(inbuff,retbuff,head->function);
                    HRPCtrace(" DoRun post-stub \n");
                    if (state==server_closed) {
                        lockcontinue = 0;
                    }
                    else {
                        state = server_recv;
                    }
                }


                HRPCtrace(" Dorun pre-transmit async=%d\n",async);
                if (!async) {
                    transport->Transmit(retbuff);
                    HRPCtrace(" Dorun post-transmit\n");
                }
                break;
            }
            sp = sp->_next;
        }
        if (sp==NULL) {
            IHRPC_Exception *e=MakeHRPCexception(HRPCERR_module_not_found,head->module);
            StringBuffer msg;
            dosendexception(retbuff,IRemoteException::EX_STANDARD,e->errorCode(),NULL,e->errorMessage(msg).toCharArray());
            e->Release();
            transport->Transmit(retbuff);
        }
        
        inbuff.releasewrite(base);
    } while (lockcontinue);
    transport->Disconnect();
    HRPCtrace("<DoRun exit\n");
}

void HRPCserver::Run(IHRPCtransport * trans)
{
    assertex(trans);
    ::Release(transport);
    transport = LINK(trans);
    size32_t base=inbuff.markwrite();
    assertex(state==server_closed); // recursion check (TBD?)
    DoRun(base);
    state = server_closed;
    ::Release(transport);
    transport = NULL;
}

void HRPCserver::Stop()
{
    Sleep(0);
    IHRPCtransport *trans=transport;
    try {
        if (trans) {
            trans->Link();
            if (state==server_listening) {
                trans->StopListening();
            }
            else if (state==server_recv) {
                trans->Disconnect();
                trans->StopListening();
            }
            else
                state = server_closed;
        }
        else
            state = server_closed;
    }
    catch (...) {
        PrintLog("HRPCserver::Stop Fault");
    }
    ::Release(trans);
}


HRPCcallframe::HRPCcallframe(Mutex *m,HRPCbuffer &c) : buff(c)
{
    sync = m;
    if (sync)
        sync->lock();
    mark = buff.markwrite();
    HRPCpacketheader::write(buff);
}

HRPCcallframe::~HRPCcallframe()
{
    buff.releasewrite(mark);
    if (sync)
        sync->unlock();
}


