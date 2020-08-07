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

// todo look at IRemoteFileServer stop


#include "platform.h"
#include "limits.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"

#include "securesocket.hpp"
#include "portlist.h"
#include "jsocket.hpp"
#include "jencrypt.hpp"
#include "jlzw.hpp"
#include "jset.hpp"
#include "jhtree.hpp"

#include "remoteerr.hpp"
#include <atomic>
#include <string>

#include "dafscommon.hpp"
#include "rmtclient_impl.hpp"


#define SOCKET_CACHE_MAX 500


#if SIMULATE_PACKETLOSS
static ISocket *dummyReadWrite::timeoutreadsock = null; // used to trigger
#endif

void cleanupDaFsSocket(ISocket *sock)
{
    if (!sock)
        return;
    try
    {
        sock->shutdown();
    }
    catch (IException *e)
    {
        e->Release();
    }
    try
    {
        sock->close();
    }
    catch (IException *e)
    {
        e->Release();
    }
}


///////////////////////////


static const unsigned defaultDafsConnectTimeoutSeconds=100;
static const unsigned defaultDafsConnectRetries=2;
static const unsigned defaultDafsMaxRecieveTimeSeconds=0;
static const unsigned defaultDafsConnectFailRetrySeconds=10;

static unsigned dafsConnectTimeoutMs = defaultDafsConnectTimeoutSeconds * 1000;
static unsigned dafsConnectRetries = defaultDafsConnectRetries;
static unsigned dafsMaxReceiveTimeMs = defaultDafsMaxRecieveTimeSeconds * 1000;
static unsigned dafsConnectFailRetryTimeMs = defaultDafsConnectFailRetrySeconds * 1000;

MODULE_INIT(INIT_PRIORITY_DAFSCLIENT)
{
    const IProperties &confProps = queryEnvironmentConf();
    dafsConnectTimeoutMs = confProps.getPropInt("dafsConnectTimeoutSeconds", defaultDafsConnectTimeoutSeconds) * 1000;
    dafsConnectRetries = confProps.getPropInt("dafsConnectRetries", defaultDafsConnectRetries);
    dafsMaxReceiveTimeMs = confProps.getPropInt("dafsMaxReceiveTimeSeconds", defaultDafsMaxRecieveTimeSeconds);
    dafsConnectFailRetryTimeMs = confProps.getPropInt("daFsConnectFailRetrySeconds", defaultDafsConnectFailRetrySeconds) * 1000;
    return true;
}


//Security and default port attributes
static class _securitySettings
{
public:
    DAFSConnectCfg  connectMethod;
    unsigned short  daFileSrvPort;
    unsigned short  daFileSrvSSLPort;
    const char *    certificate;
    const char *    privateKey;
    const char *    passPhrase;

    _securitySettings()
    {
        queryDafsSecSettings(&connectMethod, &daFileSrvPort, &daFileSrvSSLPort, &certificate, &privateKey, &passPhrase);
    }
} securitySettings;


static CriticalSection              secureContextCrit;
static Owned<ISecureSocketContext>  secureContextServer;
static Owned<ISecureSocketContext>  secureContextClient;

#ifdef _USE_OPENSSL
static ISecureSocket *createSecureSocket(ISocket *sock, SecureSocketType type)
{
    {
        CriticalBlock b(secureContextCrit);
        if (type == ServerSocket)
        {
            if (!secureContextServer)
                secureContextServer.setown(createSecureSocketContextEx(securitySettings.certificate, securitySettings.privateKey, securitySettings.passPhrase, type));
        }
        else if (!secureContextClient)
            secureContextClient.setown(createSecureSocketContext(type));
    }
    int loglevel = SSLogNormal;
#ifdef _DEBUG
    loglevel = SSLogMax;
#endif
    if (type == ServerSocket)
        return secureContextServer->createSecureSocket(sock, loglevel);
    else
        return secureContextClient->createSecureSocket(sock, loglevel);
}
#else
static ISecureSocket *createSecureSocket(ISocket *sock, SecureSocketType type)
{
    throwUnexpected();
}
#endif

void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime)
{
    dafsConnectTimeoutMs = maxconnecttime;
    dafsMaxReceiveTimeMs = maxreadtime;
}


struct sRFTM
{
    CTimeMon *timemon;
    sRFTM(unsigned limit) {  timemon = limit ? new CTimeMon(limit) : NULL; }
    ~sRFTM() { delete timemon; }
};



// used by testsocket only
RemoteFileCommandType queryRemoteStreamCmd()
{
    return RFCStreamReadTestSocket;
}


#define CLIENT_TIMEOUT      (1000*60*60*12)     // long timeout in case zombies
#define CLIENT_INACTIVEWARNING_TIMEOUT (1000*60*60*12) // time between logging inactive clients
#define SERVER_TIMEOUT      (1000*60*5)         // timeout when waiting for dafilesrv to reply after command
                                                // (increased when waiting for large block)
#ifdef SIMULATE_PACKETLOSS
#define NORMAL_RETRIES      (1)
#define LENGTHY_RETRIES     (1)
#else
#define NORMAL_RETRIES      (3)
#define LENGTHY_RETRIES     (12)
#endif

#ifdef _DEBUG
byte traceFlags=0x30;
#else
byte traceFlags=0x20;
#endif


#define TF_TRACE (traceFlags&1)
#define TF_TRACE_PRE_IO (traceFlags&2)
#define TF_TRACE_FULL (traceFlags&4)
#define TF_TRACE_CLIENT_CONN (traceFlags&8)
#define TF_TRACE_TREE_COPY (traceFlags&0x10)
#define TF_TRACE_CLIENT_STATS (traceFlags&0x20)



unsigned mapDafilesrvixCodes(unsigned err)
{
    // old Solaris dali/remote/daliservix.cpp uses
    // different values for these error codes.
    switch (err)
    {
        case 8200:
            return RFSERR_InvalidCommand;
        case 8201:
            return RFSERR_NullFileIOHandle;
        case 8202:
            return RFSERR_InvalidFileIOHandle;
        case 8203:
            return RFSERR_TimeoutFileIOHandle;
        case 8204:
            return RFSERR_OpenFailed;
        case 8205:
            return RFSERR_ReadFailed;
        case 8206:
            return RFSERR_WriteFailed;
        case 8207:
            return RFSERR_RenameFailed;
        case 8208:
            return RFSERR_SetReadOnlyFailed;
        case 8209:
            return RFSERR_GetDirFailed;
        case 8210:
            return RFSERR_MoveFailed;
    }
    return err;
}


typedef byte OnceKey[16];

static void genOnce(OnceKey &key)
{
    static __int64 inc=0;
    *(unsigned *)&key[0] = getRandom();
    *(__int64 *)&key[4] = ++inc;
    *(unsigned *)&key[12] = getRandom();
}

static void mergeOnce(OnceKey &key,size32_t sz,const void *data)
{
    assertex(sz<=sizeof(OnceKey));
    const byte *p = (const byte *)data;
    while (sz)
        key[--sz] ^= *(p++);
}

//---------------------------------------------------------------------------

class DECL_EXCEPTION CDafsException: public IDAFS_Exception, public CInterface
{
    int     errcode;
    StringAttr msg;
public:
    IMPLEMENT_IINTERFACE;

    CDafsException(int code,const char *_msg)
        : errcode(code), msg(_msg)
    {
    };

    int errorCode() const
    {
        return errcode;
    }

    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append(msg);
    }
    MessageAudience errorAudience() const
    {
        return MSGAUD_user;
    }
};

IDAFS_Exception *createDafsException(int code, const char *msg)
{
    return new CDafsException(code, msg);
}

IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args) __attribute__((format(printf,2,0)));

IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new CDafsException(code, eStr);
}

IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...) __attribute__((format(printf,2,3)));
IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IDAFS_Exception *ret = createDafsExceptionVA(code, format, args);
    va_end(args);
    return ret;
}

void setDafsEndpointPort(SocketEndpoint &ep)
{
    // odd kludge (don't do this at home)
    byte ipb[4];
    if (ep.getNetAddress(sizeof(ipb),&ipb)==sizeof(ipb)) {
        if ((ipb[0]==255)&&(ipb[1]==255)) {
            ep.port = (((unsigned)ipb[2])<<8)+ipb[3];
            ep.ipset(queryLocalIP());
        }
    }
    if (ep.port==0)
    {
        if ( (securitySettings.connectMethod == SSLNone) || (securitySettings.connectMethod == UnsecureFirst) )
            ep.port = securitySettings.daFileSrvPort;
        else
            ep.port = securitySettings.daFileSrvSSLPort;
    }
}


void sendDaFsBuffer(ISocket * socket, MemoryBuffer & src, bool testSocketFlag)
{
    unsigned length = src.length() - sizeof(unsigned);
    byte * buffer = (byte *)src.toByteArray();
    if (TF_TRACE_FULL)
        PROGLOG("sendDaFsBuffer size %d, data = %d %d %d %d",length, (int)buffer[4],(int)buffer[5],(int)buffer[6],(int)buffer[7]);
    if (testSocketFlag)
        length |= 0x80000000;
    _WINCPYREV(buffer, &length, sizeof(unsigned));
    SOCKWRITE(socket)(buffer, src.length());
}

size32_t receiveDaFsBufferSize(ISocket * socket, unsigned numtries,CTimeMon *timemon)
{
    unsigned timeout = SERVER_TIMEOUT;
    if (numtries==0) {
        numtries = 1;
        timeout = 10*1000;  // 10s
    }
    while (numtries--) {
        try {
            if (timemon) {
                unsigned remaining;
                if (timemon->timedout(&remaining)||(remaining<10))
                    remaining = 10;
                if (remaining<timeout)
                    timeout = remaining;
            }
            size32_t szread;
            size32_t gotLength;
            SOCKREADTMS(socket)(&gotLength, sizeof(gotLength), sizeof(gotLength), szread, timeout);
            _WINREV(gotLength);
            if (TF_TRACE_FULL)
                PROGLOG("receiveDaFsBufferSize %d",gotLength);
            return gotLength;
        }
        catch (IJSOCK_Exception *e) {
            if ((numtries==0)||(e->errorCode()!=JSOCKERR_timeout_expired)||(timemon&&timemon->timedout())) {
                throw;
            }
            StringBuffer err;
            char peername[256];
            socket->peer_name(peername,sizeof(peername)-1);
            WARNLOG("Remote connection %s: %s",peername,e->errorMessage(err).str()); // why no peername
            e->Release();
            Sleep(500+getRandom()%1000); // ~1s
        }
    }
    return 0;
}

void flushDaFsSocket(ISocket *socket)
{
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    size32_t totread=0;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        char buf[16*1024];
        for (;;)
        {
            size32_t szread;
            SOCKREADTMS(socket)(buf, 1, sizeof(buf), szread, 1000*30);
            totread += szread;
        }
    }
    catch (IJSOCK_Exception *e)
    {
        if (totread)
            PROGLOG("%d bytes discarded",totread);
        if (e->errorCode()!=JSOCKERR_timeout_expired)
            EXCLOG(e,"flush");
        e->Release();
    }
}

void receiveDaFsBuffer(ISocket * socket, MemoryBuffer & tgt, unsigned numtries, size32_t maxsz)
    // maxsz is a guess at a resonable upper max to catch where protocol error
{
    sRFTM tm(dafsMaxReceiveTimeMs);
    size32_t gotLength = receiveDaFsBufferSize(socket, numtries,tm.timemon);
    Owned<IException> exc;
    if (gotLength)
    {
        size32_t origlen = tgt.length();
        try
        {
            if (gotLength>maxsz)
            {
                StringBuffer msg;
                msg.appendf("receiveBuffer maximum block size exceeded %d/%d",gotLength,maxsz);
                throw createDafsException(DAFSERR_protocol_failure,msg.str());
            }
            unsigned timeout = SERVER_TIMEOUT*(numtries?numtries:1);
            if (tm.timemon)
            {
                unsigned remaining;
                if (tm.timemon->timedout(&remaining)||(remaining<10))
                    remaining = 10;
                if (remaining<timeout)
                    timeout = remaining;
            }
            size32_t szread;
            SOCKREADTMS(socket)((gotLength<4000)?tgt.reserve(gotLength):tgt.reserveTruncate(gotLength), gotLength, gotLength, szread, timeout);
        }
        catch (IException *e)
        {
            exc.setown(e);
        }

        if (exc.get())
        {
            tgt.setLength(origlen);
            EXCLOG(exc, "receiveDaFsBuffer");
            PrintStackReport();
            if (JSOCKERR_timeout_expired != exc->errorCode())
            {
                if (!tm.timemon||!tm.timemon->timedout())
                    flushDaFsSocket(socket);
            }
            IJSOCK_Exception *JSexc = dynamic_cast<IJSOCK_Exception *>(exc.get());
            if (JSexc != nullptr)
                throw LINK(JSexc);
            else
                throw exc.getClear();
        }
    }
    tgt.setEndian(__BIG_ENDIAN);
}


//---------------------------------------------------------------------------


struct CConnectionRec
{
    SocketEndpoint ep;
    unsigned tick;
    IArrayOf<ISocket> socks;            // relies on isShared
};

static class CConnectionTable: public SuperHashTableOf<CConnectionRec,SocketEndpoint>
{

    void onAdd(void *) {}

    void onRemove(void *e)
    {
        CConnectionRec *r=(CConnectionRec *)e;
        delete r;
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CConnectionRec &elem=*(const CConnectionRec *)e;
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const CConnectionRec &elem=*(const CConnectionRec *)p;
        return (void *)&elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CConnectionRec *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CConnectionRec,SocketEndpoint);

    unsigned numsockets;

public:
    static CriticalSection crit;

    CConnectionTable()
    {
        numsockets = 0;
    }
    ~CConnectionTable() {
        _releaseAll();
    }

    ISocket *lookup(const SocketEndpoint &ep)
    {
        // always called from crit block
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (r) {
            ForEachItemIn(i,r->socks) {
                ISocket *s = &r->socks.item(i);
                if (!QUERYINTERFACE(s, CInterface)->IsShared()) {
                    r->tick = msTick();
                    s->Link();
                    return s;
                }
            }
        }
        return NULL;
    }

    void addLink(SocketEndpoint &ep,ISocket *sock)
    {
        // always called from crit block
        while (numsockets>=SOCKET_CACHE_MAX) {
            // find oldest
            CConnectionRec *c = NULL;
            unsigned oldest = 0;
            CConnectionRec *old = NULL;
            unsigned oldi;
            unsigned now = msTick();
            for (;;) {
                c = (CConnectionRec *)SuperHashTableOf<CConnectionRec,SocketEndpoint>::next(c);
                if (!c)
                    break;
                ForEachItemIn(i,c->socks) {
                    ISocket *s = &c->socks.item(i);
                    if (!QUERYINTERFACE(s, CInterface)->IsShared()) { // candidate to remove
                        unsigned t = now-c->tick;
                        if (t>oldest) {
                            oldest = t;
                            old = c;
                            oldi = i;
                        }
                    }
                }
            }
            if (!old)
                return;
            old->socks.remove(oldi);
            numsockets--;
        }
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (!r) {
            r = new CConnectionRec;
            r->ep = ep;
            SuperHashTableOf<CConnectionRec,SocketEndpoint>::add(*r);
        }
        sock->Link();
        r->socks.append(*sock);
        numsockets++;
        r->tick = msTick();
    }

    void remove(const SocketEndpoint &ep, ISocket *sock)
    {
        // always called from crit block
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (r)
            if (r->socks.zap(*sock)&&numsockets)
                numsockets--;

    }
} *ConnectionTable = NULL;


CriticalSection CConnectionTable::crit;

void clientSetDaliServixSocketCaching(bool on)
{
    CriticalBlock block(CConnectionTable::crit);
    if (on) {
        if (!ConnectionTable)
            ConnectionTable = new CConnectionTable;
    }
    else {
        delete ConnectionTable;
        ConnectionTable = NULL;
    }
}

ISocket *getConnectionTableSocket(const SocketEndpoint &ep)
{
    CriticalBlock block(CConnectionTable::crit);
    if (!ConnectionTable)
        return nullptr;
    return ConnectionTable->lookup(ep);
}

void removeConnectionTableSocket(const SocketEndpoint &ep, ISocket *socket)
{
    CriticalBlock block(CConnectionTable::crit);
    if (ConnectionTable)
        ConnectionTable->remove(ep, socket);
}

void clientAddSocketToCache(SocketEndpoint &ep,ISocket *socket)
{
    CriticalBlock block(CConnectionTable::crit);
    if (ConnectionTable)
        ConnectionTable->addLink(ep,socket);
}




//---------------------------------------------------------------------------

void CRemoteBase::connectSocket(SocketEndpoint &ep, unsigned connectTimeoutMs, unsigned connectRetries)
{
    if (!connectTimeoutMs)
        connectTimeoutMs = dafsConnectTimeoutMs;

    unsigned connectAttempts = ((INFINITE == connectRetries) ? dafsConnectRetries : connectRetries) + 1;

    sRFTM tm(connectTimeoutMs);

    {
        CriticalBlock block(lastFailEpCrit);
        if (ep.equals(lastfailep))
        {
            if (msTick()-lastfailtime<dafsConnectFailRetryTimeMs)
            {
                StringBuffer msg("Failed to connect (host marked down) to dafilesrv/daliservix on ");
                ep.getUrlStr(msg);
                throw createDafsException(DAFSERR_connection_failed,msg.str());
            }
            lastfailep.set(NULL);
            connectAttempts = 1;    // on probation
        }
    }
    while (connectAttempts--)
    {
        StringBuffer eps;
        if (TF_TRACE_CLIENT_CONN)
        {
            ep.getUrlStr(eps);
            if (ep.port == securitySettings.daFileSrvSSLPort)
                PROGLOG("Connecting SECURE to %s", eps.str());
            else
                PROGLOG("Connecting to %s", eps.str());
            //PrintStackReport();
        }
        bool ok = true;
        try
        {
            if (tm.timemon)
            {
                unsigned remaining;
                if (tm.timemon->timedout(&remaining))
                    throwJSocketException(JSOCKERR_connection_failed);
                socket.setown(ISocket::connect_timeout(ep,remaining));
            }
            else
                socket.setown(ISocket::connect(ep));
            if (ep.port == securitySettings.daFileSrvSSLPort)
            {
#ifdef _USE_OPENSSL
                Owned<ISecureSocket> ssock;
                try
                {
                    ssock.setown(createSecureSocket(socket.getClear(), ClientSocket));
                    int status = ssock->secure_connect();
                    if (status < 0)
                        throw createDafsException(DAFSERR_connection_failed, "Failure to establish secure connection");
                    socket.setown(ssock.getLink());
                }
                catch (IException *e)
                {
                    cleanupDaFsSocket(ssock);
                    ssock.clear();
                    cleanupDaFsSocket(socket);
                    socket.clear();
                    StringBuffer eMsg;
                    e->errorMessage(eMsg);
                    e->Release();
                    throw createDafsException(DAFSERR_connection_failed, eMsg.str());
                }
#else
                throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
            }
        }
        catch (IJSOCK_Exception *e)
        {
            ok = false;
            if (!connectAttempts||(tm.timemon&&tm.timemon->timedout()))
            {
                if (e->errorCode()==JSOCKERR_connection_failed)
                {
                    {
                        CriticalBlock block(lastFailEpCrit);
                        lastfailep.set(ep);
                        lastfailtime = msTick();
                    }
                    e->Release();
                    StringBuffer msg("Failed to connect (setting host down) to dafilesrv/daliservix on ");
                    ep.getUrlStr(msg);
                    throw createDafsException(DAFSERR_connection_failed,msg.str());
                }
                throw;
            }
            StringBuffer err;
            WARNLOG("Remote file connect %s",e->errorMessage(err).str());
            e->Release();
        }
        if (ok)
        {
            if (TF_TRACE_CLIENT_CONN)
                PROGLOG("Connected to %s",eps.str());
            break;
        }
        bool timeExpired = false;
        unsigned sleeptime = getRandom()%3000+1000;
        if (tm.timemon)
        {
            unsigned remaining;
            if (tm.timemon->timedout(&remaining))
                timeExpired = true;
            else
            {
                if (remaining/2<sleeptime)
                    sleeptime = remaining/2;
            }
        }
        if (!timeExpired)
        {
            Sleep(sleeptime);       // prevent multiple retries beating
            if (ep.port == securitySettings.daFileSrvSSLPort)
                PROGLOG("Retrying SECURE connect");
            else
                PROGLOG("Retrying connect");
        }
    }

    clientAddSocketToCache(ep, socket);
}

void CRemoteBase::killSocket(SocketEndpoint &tep)
{
    // NB: always called with CRemoteBase::crit locked
    try
    {
        Owned<ISocket> s = socket.getClear();
        if (!s)
            return;
        removeConnectionTableSocket(tep, s);
    }
    catch (IJSOCK_Exception *e)
    {
        e->Release();   // ignore errors closing
    }
    Sleep(getRandom()%1000*5+500);      // prevent multiple beating
}

void CRemoteBase::sendRemoteCommand(MemoryBuffer & src, MemoryBuffer & reply, bool retry, bool lengthy, bool handleErrCode)
{
    CriticalBlock block(crit);  // serialize commands on same file
    SocketEndpoint tep(ep);
    setDafsEndpointPort(tep);
    unsigned nretries = retry?3:0;
    Owned<IJSOCK_Exception> firstexc;   // when retrying return first error if fails
    for (;;)
    {
        try
        {
            if (socket)
            {
                sendDaFsBuffer(socket, src);
                receiveDaFsBuffer(socket, reply, lengthy?LENGTHY_RETRIES:NORMAL_RETRIES);
                break;
            }
        }
        catch (IJSOCK_Exception *e)
        {
            if (!nretries--)
            {
                if (firstexc)
                {
                    e->Release();
                    e = firstexc.getClear();
                }
                killSocket(tep);
                throw e;
            }
            StringBuffer str;
            e->errorMessage(str);
            WARNLOG("Remote File: %s, retrying (%d)",str.str(),nretries);
            if (firstexc)
                e->Release();
            else
                firstexc.setown(e);
            killSocket(tep);
        }
        socket.setown(getConnectionTableSocket(tep));
        if (socket)
        {
            // validate existing socket by sending an 'exists' command with short time out
            // (use exists for backward compatibility)
            bool ok = false;
            try
            {
                MemoryBuffer sendbuf;
                initSendBuffer(sendbuf);
                MemoryBuffer replybuf;
                sendbuf.append((RemoteFileCommandType)RFCexists).append(filename);
                sendDaFsBuffer(socket, sendbuf);
                receiveDaFsBuffer(socket, replybuf, 0, 1024);
                ok = true;
            }
            catch (IException *e)
            {
                e->Release();
            }
            if (!ok)
                killSocket(tep);
        }

        if (!socket)
        {
            bool doConnect = true;
            if (connectMethod == SSLFirst || connectMethod == UnsecureFirst)
            {
                // MCK - could maintain a list of 100 or so previous endpoints and if connection failed
                // then mark port down for a delay (like 15 min above) to avoid having to try every time ...
                try
                {
                    connectSocket(tep, 5000, 0);
                    doConnect = false;
                }
                catch (IDAFS_Exception *e)
                {
                    if (e->errorCode() == DAFSERR_connection_failed)
                    {
                        unsigned prevPort = tep.port;
                        if (prevPort == securitySettings.daFileSrvSSLPort)
                            tep.port = securitySettings.daFileSrvPort;
                        else
                            tep.port = securitySettings.daFileSrvSSLPort;
                        WARNLOG("Connect failed on port %d, retrying on port %d", prevPort, tep.port);
                        doConnect = true;
                        e->Release();
                    }
                    else
                        throw e;
                }
            }
            if (doConnect)
                connectSocket(tep);
        }
    }

    if (!handleErrCode)
        return;
    unsigned errCode;
    reply.read(errCode);
    if (errCode)
    {
        // old Solaris daliservix.cpp error code conversion
        if ( (errCode >= 8200) && (errCode <= 8210) )
            errCode = mapDafilesrvixCodes(errCode);
        StringBuffer msg;
        if (filename.get())
            msg.append(filename);
        ep.getUrlStr(msg.append('[')).append("] ");
        size32_t pos = reply.getPos();
        if (pos<reply.length())
        {
            size32_t len = reply.length()-pos;
            const byte *rest = reply.readDirect(len);
            if (errCode==RFSERR_InvalidCommand)
            {
                const char *s = (const char *)rest;
                const char *e = (const char *)rest+len;
                while (*s&&(s!=e))
                    s++;
                msg.append(s-(const char *)rest,(const char *)rest);
            }
            else if (len&&(rest[len-1]==0))
                msg.append((const char *)rest);
            else
            {
                msg.appendf("extra data[%d]",len);
                for (unsigned i=0;(i<16)&&(i<len);i++)
                    msg.appendf(" %2x",(int)rest[i]);
            }
        }
        // NB: could append getRFSERRText for all error codes
        else if (errCode == RFSERR_GetDirFailed)
            msg.append(RFSERR_GetDirFailed_Text);
        else
            msg.append("ERROR #").append(errCode);
#ifdef _DEBUG
        ERRLOG("%s",msg.str());
        PrintStackReport();
#endif
        throw createDafsException(errCode,msg.str());
    }
}

void CRemoteBase::sendRemoteCommand(MemoryBuffer & src, bool retry)
{
    MemoryBuffer reply;
    sendRemoteCommand(src, reply, retry);
}

CRemoteBase::CRemoteBase(const SocketEndpoint &_ep, const char * _filename)
    : filename(_filename)
{
    ep = _ep;
    connectMethod = securitySettings.connectMethod;
}

CRemoteBase::CRemoteBase(const SocketEndpoint &_ep, DAFSConnectCfg _connectMethod, const char * _filename)
    : filename(_filename)
{
    ep = _ep;
    connectMethod = _connectMethod;
}

void CRemoteBase::disconnect()
{
    CriticalBlock block(crit);
    Owned<ISocket> s = socket.getClear();
    if (s)
    {
        SocketEndpoint tep(ep);
        setDafsEndpointPort(tep);
        removeConnectionTableSocket(tep, s);
    }
}

// IDaFsConnection impl.
void CRemoteBase::close(int handle)
{
    if (handle)
    {
        try
        {
            MemoryBuffer sendBuffer;
            initSendBuffer(sendBuffer);
            sendBuffer.append((RemoteFileCommandType)RFCcloseIO).append(handle);
            sendRemoteCommand(sendBuffer,false);
        }
        catch (IDAFS_Exception *e)
        {
            if ((e->errorCode()!=RFSERR_InvalidFileIOHandle)&&(e->errorCode()!=RFSERR_NullFileIOHandle))
                throw;
            e->Release();
        }
    }
}

void CRemoteBase::send(MemoryBuffer &sendMb, MemoryBuffer &reply)
{
    sendRemoteCommand(sendMb, reply);
}

unsigned CRemoteBase::getVersion(StringBuffer &ver)
{
    unsigned ret;
    MemoryBuffer sendBuffer;
    initSendBuffer(sendBuffer);
    sendBuffer.append((RemoteFileCommandType)RFCgetver);
    sendBuffer.append((unsigned)RFCgetver);
    MemoryBuffer replyBuffer;
    try
    {
        sendRemoteCommand(sendBuffer, replyBuffer, true, false, false);
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
        return 0;
    }
    unsigned errCode;
    replyBuffer.read(errCode);
    if (errCode==RFSERR_InvalidCommand)
    {
        ver.append("DS V1.0");
        return 10;
    }
    else if (errCode==0)
        ret = 11;
    else if (errCode<0x10000)
        return 0;
    else
        ret = errCode-0x10000;

    StringAttr vers;
    replyBuffer.read(vers);
    ver.append(vers);
    return ret;
}

const SocketEndpoint &CRemoteBase::queryEp() const
{
    return ep;
}

SocketEndpoint CRemoteBase::lastfailep;
unsigned CRemoteBase::lastfailtime;
CriticalSection CRemoteBase::lastFailEpCrit;



IDaFsConnection *createDaFsConnection(const SocketEndpoint &ep, DAFSConnectCfg connectMethod, const char *tracing)
{
    return new CRemoteBase(ep, connectMethod, tracing);
}


/////////////////////////

ISocket *checkSocketSecure(ISocket *socket)
{
    if (securitySettings.connectMethod == SSLNone)
        return LINK(socket);

    char pname[256];
    pname[0] = 0;
    int pport = socket->peer_name(pname, sizeof(pname)-1);

    if ( (pport == securitySettings.daFileSrvSSLPort) && (!socket->isSecure()) )
    {
#ifdef _USE_OPENSSL
        Owned<ISecureSocket> ssock;
        try
        {
            ssock.setown(createSecureSocket(LINK(socket), ClientSocket));
            int status = ssock->secure_connect();
            if (status < 0)
                throw createDafsException(DAFSERR_connection_failed, "Failure to establish secure connection");
            return ssock.getClear();
        }
        catch (IException *e)
        {
            cleanupDaFsSocket(ssock);
            ssock.clear();
            cleanupDaFsSocket(socket);
            StringBuffer eMsg;
            e->errorMessage(eMsg);
            e->Release();
            throw createDafsException(DAFSERR_connection_failed, eMsg.str());
        }
#else
        throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
    }

    return LINK(socket);
}

ISocket *connectDafs(SocketEndpoint &ep, unsigned timeoutms)
{
    Owned<ISocket> socket;

    if ( (securitySettings.connectMethod == SSLNone) || (securitySettings.connectMethod == SSLOnly) )
    {
        socket.setown(ISocket::connect_timeout(ep, timeoutms));
        return checkSocketSecure(socket);
    }

    // SSLFirst or UnsecureFirst ...

    unsigned newtimeout = timeoutms;
    if (newtimeout > 5000)
        newtimeout = 5000;

    int conAttempts = 2;
    while (conAttempts > 0)
    {
        conAttempts--;
        bool connected = false;
        try
        {
            socket.setown(ISocket::connect_timeout(ep, newtimeout));
            connected = true;
            newtimeout = timeoutms;
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode() == JSOCKERR_connection_failed)
            {
                e->Release();
                if (ep.port == securitySettings.daFileSrvSSLPort)
                    ep.port = securitySettings.daFileSrvPort;
                else
                    ep.port = securitySettings.daFileSrvSSLPort;
                if (!conAttempts)
                    throw;
            }
            else
                throw;
        }

        if (connected)
        {
            if (ep.port == securitySettings.daFileSrvSSLPort)
            {
                try
                {
                    return checkSocketSecure(socket);
                }
                catch (IDAFS_Exception *e)
                {
                    connected = false;
                    if (e->errorCode() == DAFSERR_connection_failed)
                    {
                        // worth logging to help identify any ssl config issues ...
                        StringBuffer errmsg;
                        e->errorMessage(errmsg);
                        WARNLOG("%s", errmsg.str());
                        e->Release();
                        ep.port = securitySettings.daFileSrvPort;
                        if (!conAttempts)
                            throw;
                    }
                    else
                        throw;
                }
            }
            else
                return socket.getClear();
        }
    }

    throw createDafsException(DAFSERR_connection_failed, "Failed to establish connection with DaFileSrv");
}

unsigned short getActiveDaliServixPort(const IpAddress &ip)
{
    if (ip.isNull())
        return 0;
    SocketEndpoint ep(0, ip);
    setDafsEndpointPort(ep);
    try {
        Owned<ISocket> socket = connectDafs(ep, 10000);
        return ep.port;
    }
    catch (IException *e)
    {
        e->Release();
    }
    return 0;
}

bool testDaliServixPresent(const IpAddress &ip)
{
    return getActiveDaliServixPort(ip) != 0;
}

unsigned getDaliServixVersion(const IpAddress &ip,StringBuffer &ver)
{
    SocketEndpoint ep(0,ip);
    return getDaliServixVersion(ep,ver);
}

unsigned getDaliServixVersion(const SocketEndpoint &_ep,StringBuffer &ver)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return 0;
    try
    {
        Owned<ISocket> socket = connectDafs(ep, 10000);
        return getRemoteVersion(socket,ver);
    }
    catch (IException *e)
    {
        EXCLOG(e,"getDaliServixVersion");
        e->Release();
    }
    return 0;
}

unsigned getRemoteVersion(IDaFsConnection &daFsConnection, StringBuffer &ver)
{
    return daFsConnection.getVersion(ver);
}

unsigned getRemoteVersion(ISocket *origSock, StringBuffer &ver)
{
    // used to have a global critical section here
    if (!origSock)
        return 0;

    Owned<ISocket> socket = checkSocketSecure(origSock);

    unsigned ret;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        receiveDaFsBuffer(socket, reply, 1 ,4096);
        unsigned errCode;
        reply.read(errCode);
        if (errCode==RFSERR_InvalidCommand)
        {
            ver.append("DS V1.0");
            return 10;
        }
        else if (errCode==0)
            ret = 11;
        else if (errCode<0x10000)
            return 0;
        else
            ret = errCode-0x10000;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
        return 0;
    }
    StringAttr vers;
    reply.read(vers);
    ver.append(vers);
    return ret;
}

unsigned getCachedRemoteVersion(IDaFsConnection &daFsConnection)
{
    /* JCSMORE - add a SocketEndpoint->version cache
     * Idea being, that clients will want to determine version and differentiate what they send
     * But do not want the cost of asking each time!
     * So have a 'getRemoteVersion' call ask once and store version, so next time it returns cached answer.
     *
     * May want to have timeout on cache entries, but can be long. Don't expect remote side to change often within lifetime of client.
     */

    // JCSMORE TBD (properly!)

    // 1st check ep in cache using:
    //   daFsConnect.queryEp()
    // else

    StringBuffer ver;
    return daFsConnection.getVersion(ver);
}

unsigned getCachedRemoteVersion(const SocketEndpoint &ep, bool secure)
{
    // 1st check ep in cache
    // else
    DAFSConnectCfg connMethod = secure ? SSLOnly : SSLNone;
    Owned<IDaFsConnection> daFsConnection = createDaFsConnection(ep, connMethod, "getversion");
    return getCachedRemoteVersion(*daFsConnection);
}


/////////////////////////


//////////////

extern unsigned stopRemoteServer(ISocket * socket)
{
    // used to have a global critical section here
    if (!socket)
        return 0;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCstop);
    sendbuf.append((unsigned)RFCstop);
    MemoryBuffer replybuf;
    unsigned errCode = RFSERR_InvalidCommand;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        receiveDaFsBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        replybuf.read(errCode);
    }
    catch (IJSOCK_Exception *e)
    {
        if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
            EXCLOG(e);
        else
            errCode = 0;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return errCode;
}

int setDafsTrace(ISocket * socket,byte flags)
{
    if (!socket)
    {
        byte ret = traceFlags;
        traceFlags = flags;
        return ret;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCsettrace).append(flags);
    MemoryBuffer replybuf;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        receiveDaFsBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}

int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg)
{
    assertex(socket);
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCsetthrottle2).append((unsigned)throttleClass).append(throttleLimit);
    sendbuf.append(throttleDelayMs).append(throttleCPULimit).append(queueLimit);
    MemoryBuffer replybuf;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        receiveDaFsBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        if (retcode && errMsg && replybuf.remaining())
            replybuf.read(*errMsg);
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}

int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr)
{
    assertex(socket);
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetinfo).append(level);
    MemoryBuffer replybuf;
    try
    {
        sendDaFsBuffer(socket, sendbuf);
        receiveDaFsBuffer(socket, replybuf, 1);
        int retcode;
        replybuf.read(retcode);
        if (retcode==0)
        {
            StringAttr s;
            replybuf.read(s);
            retstr.append(s);
        }
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}


struct CDafsOsCacheEntry
{
    SocketEndpoint ep;
    DAFS_OS os;
    time_t at;
};

class CDafsOsCache: public SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>
{
    void onAdd(void *) {}

    void onRemove(void *et)
    {
        CDafsOsCacheEntry *e = (CDafsOsCacheEntry *)et;
        delete e;
    }
    unsigned getHashFromElement(const void *e) const
    {
        const CDafsOsCacheEntry &elem=*(const CDafsOsCacheEntry *)e;        
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const CDafsOsCacheEntry &elem=*(const CDafsOsCacheEntry *)p;        
        return (void *)&elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CDafsOsCacheEntry *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CDafsOsCacheEntry,SocketEndpoint);

public:
    static CriticalSection crit;

    CDafsOsCache() 
    {
    }
    ~CDafsOsCache()
    {
        SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::_releaseAll();
    }
    DAFS_OS lookup(const SocketEndpoint &ep,ISocket *sock)
    {
        CriticalBlock block(crit);
        CDafsOsCacheEntry *r = SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::find(&ep);
        bool needupdate=false;
        unsigned t = (unsigned)time(NULL);
        if (!r) {
            r = new CDafsOsCacheEntry;
            r->ep = ep;
            needupdate = true;
            SuperHashTableOf<CDafsOsCacheEntry,SocketEndpoint>::add(*r);
        }
        else
            needupdate = (t-r->at>60*5);        // update every 5 mins
        if (needupdate) {
            r->os = DAFS_OSunknown;
            StringBuffer ver;
            unsigned ret;
            if (sock)
                ret = getRemoteVersion(sock,ver);
            else
                ret = getDaliServixVersion(ep,ver);
            if (ret!=0) { // if cross-os needs dafilesrv
                if (strstr(ver.str(),"Linux")!=NULL)
                    r->os = DAFS_OSlinux;
                else if (strstr(ver.str(),"Windows")!=NULL)
                    r->os = DAFS_OSwindows;
                else if (strstr(ver.str(),"Solaris")!=NULL)
                    r->os = DAFS_OSsolaris;
            }
            r->at = t;
        }
        return r->os;
    }
};


CriticalSection CDafsOsCache::crit;


DAFS_OS getDaliServixOs(const SocketEndpoint &ep,ISocket *socket)
{
#ifdef _DEBUG
    if (ep.isLocal())
#ifdef _WIN32
        return DAFS_OSwindows;
#else
        return DAFS_OSlinux;
#endif
#endif
    static CDafsOsCache cache;
    return cache.lookup(ep,socket);
}



DAFS_OS getDaliServixOs(const SocketEndpoint &ep)
{
    return getDaliServixOs(ep,NULL);
}


extern DAFSCLIENT_API int setDafileSvrTraceFlags(const SocketEndpoint &_ep,byte flags)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return -3;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return setDafsTrace(socket, flags);
    }
    catch (IException *e)
    {
        EXCLOG(e,"setDafileSvrTraceFlags");
        e->Release();
    }
    return -2;
}

extern DAFSCLIENT_API int setDafileSvrThrottleLimit(const SocketEndpoint &_ep, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return -3;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return setDafsThrottleLimit(socket, throttleClass, throttleLimit, throttleDelayMs, throttleCPULimit, queueLimit, errMsg);
    }
    catch (IException *e)
    {
        EXCLOG(e,"setDafileSvrThrottleLimit");
        e->Release();
    }
    return -2;
}

extern DAFSCLIENT_API int getDafileSvrInfo(const SocketEndpoint &_ep, unsigned level, StringBuffer &retstr)
{
    SocketEndpoint ep(_ep);
    setDafsEndpointPort(ep);
    if (ep.isNull())
        return false;
    try {
        Owned<ISocket> socket = connectDafs(ep, 5000);
        return getDafsInfo(socket, level, retstr);
    }
    catch (IException *e)
    {
        EXCLOG(e,"getDafileSvrInfo");
        e->Release();
    }
    return -2;
}

