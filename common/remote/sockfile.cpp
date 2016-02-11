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
#include "sockfile.hpp"
#include "portlist.h"
#include "jsocket.hpp"
#include "jencrypt.hpp"
#include "jset.hpp"

#include "remoteerr.hpp"
#include <atomic>

#define SOCKET_CACHE_MAX 500

#ifdef _DEBUG
//#define SIMULATE_PACKETLOSS 1
#endif

#define TREECOPYTIMEOUT   (60*60*1000)     // 1Hr (I guess could take longer for big file but at least will stagger)
#define TREECOPYPOLLTIME  (60*1000*5)      // for tracing that delayed
#define TREECOPYPRUNETIME (24*60*60*1000)  // 1 day

#if SIMULATE_PACKETLOSS

#define TESTING_FAILURE_RATE_LOST_SEND  10 // per 1000
#define TESTING_FAILURE_RATE_LOST_RECV  10 // per 1000
#define DUMMY_TIMEOUT_MAX (1000*10)

static bool errorSimulationOn = true;
static ISocket *timeoutreadsock = NULL; // used to trigger 


struct dummyReadWrite
{
    class X
    {
        dummyReadWrite *parent;
    public:
        X(dummyReadWrite *_parent)
        {
            parent = _parent;
        }
        ~X()
        {
            delete parent;
        }
    };

    class TimeoutSocketException: public CInterface, public IJSOCK_Exception
    {
    public:
        IMPLEMENT_IINTERFACE;

        TimeoutSocketException()
        {
        }

        virtual ~TimeoutSocketException()
        {
        }

        int errorCode() const { return JSOCKERR_timeout_expired; }
        StringBuffer &  errorMessage(StringBuffer &str) const
        {
            return str.append("timeout expired");
        }
        MessageAudience errorAudience() const 
        { 
            return MSGAUD_user; 
        }
    };


    ISocket *sock;

    dummyReadWrite(ISocket *_sock)
    {
        sock = _sock;
    }

    void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, time_t timeout)
    {
        X x(this);
        unsigned t = msTick();
        unsigned r = getRandom();
        bool timeoutread = (timeoutreadsock==sock);
        timeoutreadsock=NULL;
        if (!timeoutread)
            sock->readtms(buf, min_size, max_size, size_read, timeout);
        if (timeoutread||(errorSimulationOn&&(TESTING_FAILURE_RATE_LOST_RECV>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_RECV))) {
            PrintStackReport();
            if (timeoutread)
                PROGLOG("** Simulate timeout");
            else
                PROGLOG("** Simulate Packet loss (size %d,%d)",min_size,max_size);
            if (timeout>DUMMY_TIMEOUT_MAX)
                timeout = DUMMY_TIMEOUT_MAX;
            t = msTick()-t;
            if (t<timeout)
                Sleep(timeout-t);
            IJSOCK_Exception *e = new TimeoutSocketException;
            throw e;
        }
    }
    size32_t write(void const* buf, size32_t size)
    {
        X x(this);
        timeoutreadsock=NULL;
        unsigned r = getRandom();
        if (errorSimulationOn&&(TESTING_FAILURE_RATE_LOST_SEND>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_SEND)) {
            PrintStackReport();
            PROGLOG("** Simulate Packet loss (size %d)",size);
            timeoutreadsock=sock;
            return size; 
        }
        return sock->write(buf,size);
    }
};



#define SOCKWRITE(sock) (new dummyReadWrite(sock))->write
#define SOCKREADTMS(sock) (new dummyReadWrite(sock))->readtms
#else
#define SOCKWRITE(sock) sock->write
#define SOCKREADTMS(sock) sock->readtms
#endif

// backward compatible modes
typedef enum { compatIFSHnone, compatIFSHread, compatIFSHwrite, compatIFSHexec, compatIFSHall} compatIFSHmode;

static const char *VERSTRING= "DS V1.9"       // dont forget FILESRV_VERSION in header
#ifdef _WIN32
"Windows ";
#else
"Linux ";
#endif

typedef unsigned char RemoteFileCommandType;
typedef int RemoteFileIOHandle;

static unsigned maxConnectTime = 0;
static unsigned maxReceiveTime = 0;

//Security and default port attributes
static class _securitySettings
{
public:
    bool            useSSL;
    unsigned short  daFileSrvPort;
    const char *    certificate;
    const char *    privateKey;

    _securitySettings()
    {
        querySecuritySettings(&useSSL, &daFileSrvPort, &certificate, &privateKey);
    }
} securitySettings;


static CriticalSection              secureContextCrit;
static Owned<ISecureSocketContext>  secureContext;

#ifdef USE_OPENSSL
static ISecureSocket *createSecureSocket(ISocket *sock,SecureSocketType type)
{
    {
        CriticalBlock b(secureContextCrit);
        if (!secureContext)
        {
            if (securitySettings.certificate)
                secureContext.setown(createSecureSocketContextEx(securitySettings.certificate,securitySettings.privateKey, NULL, type));
            else
                secureContext.setown(createSecureSocketContext(type));
        }
    }
#ifdef _DEBUG
    return secureContext->createSecureSocket(sock, SSLogMax);
#else
    return secureContext->createSecureSocket(sock);
#endif
}
#endif

void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime)
{
    maxConnectTime = maxconnecttime;
    maxReceiveTime = maxreadtime;
}


struct sRFTM        
{
    CTimeMon *timemon;
    sRFTM(unsigned limit) {  timemon = limit ? new CTimeMon(limit) : NULL; }
    ~sRFTM() { delete timemon; }
};


const char *remoteServerVersionString() { return VERSTRING; }

static bool AuthenticationEnabled = true;

bool enableDafsAuthentication(bool on)
{
    bool ret = AuthenticationEnabled;
    AuthenticationEnabled = on;
    return ret;
}


#define CLIENT_TIMEOUT      (1000*60*60*12)     // long timeout in case zombies
#define CLIENT_INACTIVEWARNING_TIMEOUT (1000*60*60*12) // time between logging inactive clients
#define SERVER_TIMEOUT      (1000*60*5)         // timeout when waiting for dafilesrv to reply after command
                                                // (increased when waiting for large block)
#define DAFS_CONNECT_FAIL_RETRY_TIME (1000*60*15)
#ifdef SIMULATE_PACKETLOSS
#define NORMAL_RETRIES      (1)
#define LENGTHY_RETRIES     (1)
#else
#define NORMAL_RETRIES      (3)
#define LENGTHY_RETRIES     (12)
#endif

#ifdef _DEBUG
static byte traceFlags=0x30;
#else
static byte traceFlags=0x20;
#endif

#define TF_TRACE (traceFlags&1)
#define TF_TRACE_PRE_IO (traceFlags&2)
#define TF_TRACE_FULL (traceFlags&4)
#define TF_TRACE_CLIENT_CONN (traceFlags&8)
#define TF_TRACE_TREE_COPY (traceFlags&0x10)
#define TF_TRACE_CLIENT_STATS (traceFlags&0x20)



enum {
    RFCopenIO,                                      // 0
    RFCcloseIO,
    RFCread,
    RFCwrite,
    RFCsize,
    RFCexists,
    RFCremove,
    RFCrename,
    RFCgetver,
    RFCisfile,
    RFCisdirectory,                                 // 10
    RFCisreadonly,
    RFCsetreadonly,
    RFCgettime,
    RFCsettime,
    RFCcreatedir,
    RFCgetdir,
    RFCstop,
    RFCexec,
    RFCkill,
    RFCredeploy,                                    // 20
    RFCgetcrc,
    RFCmove,
// 1.5 features below
    RFCsetsize,
    RFCextractblobelements,
    RFCcopy,
    RFCappend,
    RFCmonitordir,
    RFCsettrace,
    RFCgetinfo,
    RFCfirewall,    // not used currently          // 30
    RFCunlock,  
    RFCunlockreply,
    RFCinvalid,
    RFCcopysection,
// 1.7e
    RFCtreecopy,
// 1.7e - 1
    RFCtreecopytmp,
// 1.8
    RFCsetthrottle, // legacy version
// 1.9
    RFCsetthrottle2,
    RFCsetfileperms,
    RFCmax,
    RFCunknown = 255 // 0 would have been more sensible, but can't break backward compatibility
};

#define RFCText(cmd) #cmd

const char *RFCStrings[] =
{
    RFCText(RFCopenIO),
    RFCText(RFCcloseIO),
    RFCText(RFCread),
    RFCText(RFCwrite),
    RFCText(RFCsize),
    RFCText(RFCexists),
    RFCText(RFCremove),
    RFCText(RFCrename),
    RFCText(RFCgetver),
    RFCText(RFCisfile),
    RFCText(RFCisdirectory),
    RFCText(RFCisreadonly),
    RFCText(RFCsetreadonly),
    RFCText(RFCgettime),
    RFCText(RFCsettime),
    RFCText(RFCcreatedir),
    RFCText(RFCgetdir),
    RFCText(RFCstop),
    RFCText(RFCexec),
    RFCText(RFCkill),
    RFCText(RFCredeploy),
    RFCText(RFCgetcrc),
    RFCText(RFCmove),
    RFCText(RFCsetsize),
    RFCText(RFCextractblobelements),
    RFCText(RFCcopy),
    RFCText(RFCappend),
    RFCText(RFCmonitordir),
    RFCText(RFCsettrace),
    RFCText(RFCgetinfo),
    RFCText(RFCfirewall),
    RFCText(RFCunlock),
    RFCText(RFCunlockreply),
    RFCText(RFCinvalid),
    RFCText(RFCcopysection),
    RFCText(RFCtreecopy),
    RFCText(RFCtreecopytmp),
    RFCText(RFCsetthrottle), // legacy version
    RFCText(RFCsetthrottle2),
    RFCText(RFCsetfileperms),
    RFCText(RFCunknown),
};
static const char *getRFCText(RemoteFileCommandType cmd)
{
    if (cmd > RFCmax)
        cmd = RFCmax;
    return RFCStrings[cmd];
}

#define ThrottleText(throttleClass) #throttleClass
const char *ThrottleStrings[] =
{
    ThrottleText(ThrottleStd),
    ThrottleText(ThrottleSlow),
};

// very high upper limits that configure can't exceed
#define THROTTLE_MAX_LIMIT 1000000
#define THROTTLE_MAX_DELAYMS 3600000
#define THROTTLE_MAX_CPUTHRESHOLD 100
#define THROTTLE_MAX_QUEUELIMIT 10000000

static const char *getThrottleClassText(ThrottleClass throttleClass) { return ThrottleStrings[throttleClass]; }

typedef enum { ACScontinue, ACSdone, ACSerror} AsyncCommandStatus;



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

class CDafsException: public CInterface, public IDAFS_Exception
{
    StringAttr msg;
    int     errcode;
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

static IDAFS_Exception *createDafsException(int code,const char *msg)
{
    return new CDafsException(code,msg);
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
        ep.port = securitySettings.daFileSrvPort;
    }
}


inline MemoryBuffer & initSendBuffer(MemoryBuffer & buff)
{
    buff.setEndian(__BIG_ENDIAN);       // transfer as big endian...
    buff.append((unsigned)0);           // reserve space for length prefix
    return buff;
}

inline void sendBuffer(ISocket * socket, MemoryBuffer & src)
{
    unsigned length = src.length() - sizeof(unsigned);
    byte * buffer = (byte *)src.toByteArray();
    if (TF_TRACE_FULL)
        PROGLOG("sendBuffer size %d, data = %d %d %d %d",length, (int)buffer[4],(int)buffer[5],(int)buffer[6],(int)buffer[7]);
    _WINCPYREV(buffer, &length, sizeof(unsigned));
    SOCKWRITE(socket)(buffer, src.length());
}

inline size32_t receiveBufferSize(ISocket * socket, unsigned numtries=NORMAL_RETRIES,CTimeMon *timemon=NULL)
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
                PROGLOG("receiveBufferSized %d",gotLength);
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

static void flush(ISocket *socket)
{
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    size32_t totread=0;
    try {
        sendBuffer(socket, sendbuf);
        char buf[1024];
        loop {
            Sleep(1000);    // breathe
            size32_t szread;
            SOCKREADTMS(socket)(buf, 1, sizeof(buf), szread, 1000*60);
            totread += szread;
        }
    }
    catch (IJSOCK_Exception *e) {
        if (totread)
            PROGLOG("%d bytes discarded",totread);
        if (e->errorCode()!=JSOCKERR_timeout_expired)
            EXCLOG(e,"flush");
        e->Release();
    }
}

inline void receiveBuffer(ISocket * socket, MemoryBuffer & tgt, unsigned numtries=1, size32_t maxsz=0x7fffffff)
    // maxsz is a guess at a resonable upper max to catch where protocol error
{
    sRFTM tm(maxReceiveTime);
    size32_t gotLength = receiveBufferSize(socket, numtries,tm.timemon);
    if (gotLength) {
        size32_t origlen = tgt.length();
        try {
            if (gotLength>maxsz) {
                StringBuffer msg;
                msg.appendf("receiveBuffer maximum block size exceeded %d/%d",gotLength,maxsz);
                PrintStackReport();
                throw createDafsException(DAFSERR_protocol_failure,msg.str());
            }
            unsigned timeout = SERVER_TIMEOUT*(numtries?numtries:1);
            if (tm.timemon) {
                unsigned remaining;
                if (tm.timemon->timedout(&remaining)||(remaining<10))
                    remaining = 10;
                if (remaining<timeout)
                    timeout = remaining;
            }
            size32_t szread;
            SOCKREADTMS(socket)((gotLength<4000)?tgt.reserve(gotLength):tgt.reserveTruncate(gotLength), gotLength, gotLength, szread, timeout);
        }
        catch (IJSOCK_Exception *e) {
            if (e->errorCode()!=JSOCKERR_timeout_expired) {
                EXCLOG(e,"receiveBuffer(1)");
                PrintStackReport();
                if (!tm.timemon||!tm.timemon->timedout())
                    flush(socket);
            }
            else {
                EXCLOG(e,"receiveBuffer");
                PrintStackReport();
            }
            tgt.setLength(origlen);
            throw;
        }
        catch (IException *e) {
            EXCLOG(e,"receiveBuffer(2)");
            PrintStackReport();
            if (!tm.timemon||!tm.timemon->timedout())
                flush(socket);
            tgt.setLength(origlen);
            throw;
        }

    }
    tgt.setEndian(__BIG_ENDIAN);
}


struct CConnectionRec
{
    SocketEndpoint ep;
    unsigned tick;
    IArrayOf<ISocket> socks;            // relies on isShared
};

//---------------------------------------------------------------------------
// Local mount redirect

struct CLocalMountRec: public CInterface
{
    IpAddress ip;
    StringAttr dir;             // dir path on remote ip
    StringAttr local;           // local dir path
};

static CIArrayOf<CLocalMountRec> localMounts;
static CriticalSection           localMountCrit;

void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (dir==NULL) { // remove all matching mount
            if (!mountdir)
                return;
            if (strcmp(mount.local,mountdir)==0) 
                localMounts.remove(i);
        }
        else if (mount.ip.ipequals(ip)&&(strcmp(mount.dir,dir)==0)) {
            if (mountdir) {
                mount.local.set(mountdir);
                return;
            }
            else
                localMounts.remove(i);
        }
    }
    if (dir&&mountdir) {
        CLocalMountRec &mount = *new CLocalMountRec;
        mount.ip.ipset(ip);
        mount.dir.set(dir);
        mount.local.set(mountdir);
        localMounts.append(mount);
    }
}

IFile *createFileLocalMount(const IpAddress &ip, const char * filename)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (mount.ip.ipequals(ip)) {
            size32_t bl = mount.dir.length();
            if (isPathSepChar(mount.dir[bl-1]))
                bl--;
            if ((memcmp((void *)filename,(void *)mount.dir.get(),bl)==0)&&(isPathSepChar(filename[bl])||!filename[bl])) { // match
                StringBuffer locpath(mount.local);
                if (filename[bl])
                    addPathSepChar(locpath).append(filename+bl+1);
                locpath.replace((PATHSEPCHAR=='\\')?'/':'\\',PATHSEPCHAR);
                return createIFile(locpath.str());
            }
        }
    }
    return NULL;
}


//---------------------------------------------------------------------------


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
        releaseAll(); 
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
            loop {
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

    void remove(SocketEndpoint &ep,ISocket *sock)
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

//---------------------------------------------------------------------------

// TreeCopy

#define TREECOPY_CACHE_SIZE 50

struct CTreeCopyItem: public CInterface
{
    StringAttr net;
    StringAttr mask;
    offset_t sz;                // original size
    CDateTime dt;               // original date
    RemoteFilenameArray loc;    // locations for file - 0 is original
    Owned<IBitSet> busy;    
    unsigned lastused;

    CTreeCopyItem(RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt)
        : net(_net), mask(_mask)
    {
        loc.append(orig);
        dt.set(_dt);
        sz = _sz;
        busy.setown(createThreadSafeBitSet());
        lastused = msTick();
    }
    bool equals(const RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt) 
    {
        if (!orig.equals(loc.item(0)))
            return false;
        if (strcmp(_net,net)!=0)
            return false;
        if (strcmp(_mask,mask)!=0)
            return false;
        if (sz!=_sz)
            return false;
        return (dt.equals(_dt,false));
    }
};

static CIArrayOf<CTreeCopyItem>  treeCopyArray;
static CriticalSection           treeCopyCrit;
static unsigned                  treeCopyWaiting=0;
static Semaphore                 treeCopySem;   

#define DEBUGSAMEIP false


//---------------------------------------------------------------------------

class CRemoteBase: public CInterface
{

    Owned<ISocket>          socket;
    static  SocketEndpoint  lastfailep;
    static unsigned         lastfailtime;
    bool                    useSSL;
    void connectSocket(SocketEndpoint &ep)
    {
        sRFTM tm(maxConnectTime);
        // called in CConnectionTable::crit
        unsigned retries = 3;
        if (ep.equals(lastfailep)) {
            if (msTick()-lastfailtime<DAFS_CONNECT_FAIL_RETRY_TIME) {
                StringBuffer msg("Failed to connect to dafilesrv/daliservix on ");
                ep.getUrlStr(msg);
                throw createDafsException(DAFSERR_connection_failed,msg.str());
            }
            lastfailep.set(NULL);
            retries = 1;    // on probation
        }
        while(retries--) {
            CriticalUnblock unblock(CConnectionTable::crit); // allow others to connect
            StringBuffer eps;
            if (TF_TRACE_CLIENT_CONN) {
                ep.getUrlStr(eps);
                PROGLOG("Connecting %sto %s",useSSL?"SECURE ":"",eps.str());
                //PrintStackReport();
            }
            bool ok = true;
            try {
                if (tm.timemon) {
                    unsigned remaining;
                    tm.timemon->timedout(&remaining);
                    socket.setown(ISocket::connect_timeout(ep,remaining));
                }
                else
                    socket.setown(ISocket::connect(ep));
                if (useSSL)
                {
#ifdef USE_OPENSSL
                    Owned<ISecureSocket> ssock = createSecureSocket(socket.getClear(), ClientSocket);
                    int status = ssock->secure_connect();
                    if (status < 0)
                        throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection");
                    socket.setown(ssock.getLink());
#else
                    throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
                }
            }
            catch (IJSOCK_Exception *e) {
                ok = false;
                if (!retries||(tm.timemon&&tm.timemon->timedout())) {
                    if (e->errorCode()==JSOCKERR_connection_failed) {
                        lastfailep.set(ep);
                        lastfailtime = msTick();
                        e->Release();
                        StringBuffer msg("Failed to connect to dafilesrv/daliservix on ");
                        ep.getUrlStr(msg);
                        throw createDafsException(DAFSERR_connection_failed,msg.str());
                    }
                    throw;
                }
                StringBuffer err;
                WARNLOG("Remote file connect %s",e->errorMessage(err).str());
                e->Release();
            }
            if (ok) {
                if (TF_TRACE_CLIENT_CONN) {
                    PROGLOG("Connected to %s",eps.str());
                }
                if (AuthenticationEnabled) {
                    try {
                        sendAuthentication(ep); // this will log error
                        break;
                    }
                    catch (IJSOCK_Exception *e) {
                        StringBuffer err;
                        WARNLOG("Remote file authenticate %s for %s ",e->errorMessage(err).str(),ep.getUrlStr(eps.clear()).str());
                        e->Release();
                        if (!retries)
                            break;
                    }
                }
                else
                    break;
            }
            unsigned sleeptime = getRandom()%3000+1000;
            if (tm.timemon) {
                unsigned remaining;
                tm.timemon->timedout(&remaining);
                if (remaining/2<sleeptime)
                    sleeptime = remaining/2;
            }
            Sleep(sleeptime);       // prevent multiple retries beating
            PROGLOG("Retrying %sconnect",useSSL?"SECURE ":"");
        }
        if (ConnectionTable)
            ConnectionTable->addLink(ep,socket);
    }

    void killSocket(SocketEndpoint &tep)
    {
        CriticalBlock block2(CConnectionTable::crit); // this is nested with crit
        if (socket) {
            try {
                Owned<ISocket> s = socket.getClear();
                if (ConnectionTable)
                    ConnectionTable->remove(tep,s);
            }
            catch (IJSOCK_Exception *e) {
                e->Release();   // ignore errors closing
            }
            Sleep(getRandom()%1000*5+500);      // prevent multiple beating
        }
    }

protected: friend class CRemoteFileIO;


    StringAttr          filename;
    CriticalSection     crit;
    SocketEndpoint      ep;


    void sendRemoteCommand(MemoryBuffer & src, MemoryBuffer & reply, bool retry=true, bool lengthy=false)
    {
        CriticalBlock block(crit);  // serialize commands on same file
        SocketEndpoint tep(ep);
        setDafsEndpointPort(tep);
        unsigned nretries = retry?3:0;
        Owned<IJSOCK_Exception> firstexc;   // when retrying return first error if fails
        loop {
            try {
                if (socket) {
                    sendBuffer(socket, src);
                    receiveBuffer(socket, reply, lengthy?LENGTHY_RETRIES:NORMAL_RETRIES);
                    break;
                }
            }
            catch (IJSOCK_Exception *e) {
                if (!nretries--) {
                    if (firstexc) {
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
            CriticalBlock block2(CConnectionTable::crit); // this is nested with crit
            if (ConnectionTable) {

                socket.setown(ConnectionTable->lookup(tep));
                if (socket) {
                    // validate existing socket by sending an 'exists' command with short time out
                    // (use exists for backward compatibility)
                    bool ok = false;
                    try {
                        MemoryBuffer sendbuf;
                        initSendBuffer(sendbuf);
                        MemoryBuffer replybuf;
                        sendbuf.append((RemoteFileCommandType)RFCexists).append(filename);
                        sendBuffer(socket, sendbuf);
                        receiveBuffer(socket, replybuf, 0, 1024);
                        ok = true;
                    }
                    catch (IException *e) {
                        e->Release();
                    }
                    if (!ok)
                        killSocket(tep);
                }
            }
            if (!socket) {
                bool tryNonSecure = true;
                if (useSSL) {
                    try {
                        connectSocket(tep);//first try secure connect
                        tryNonSecure = false;
                    }
                    catch (IException *e) {

                        StringBuffer s;
                        e->errorMessage(s);
                        e->Release();
                        WARNLOG("Secure connect failed, retrying on legacy port (%s)",s.str());

                        useSSL = false;
                        tep.port = DAFILESRV_PORT;//retry on nonsecure port
                        tryNonSecure = true;
                    }
                }

                if (tryNonSecure)
                    connectSocket(tep);
            }
        }

        unsigned errCode;
        reply.read(errCode);
        if (errCode) {

            StringBuffer msg;
            if (filename.get())
                msg.append(filename);
            ep.getUrlStr(msg.append('[')).append("] ");
            size32_t pos = reply.getPos();
            if (pos<reply.length()) {
                size32_t len = reply.length()-pos;
                const byte *rest = reply.readDirect(len);
                if (errCode==RFSERR_InvalidCommand) {
                    const char *s = (const char *)rest;
                    const char *e = (const char *)rest+len;
                    while (*s&&(s!=e))
                        s++;
                    msg.append(s-(const char *)rest,(const char *)rest);
                }
                else if (len&&(rest[len-1]==0))
                    msg.append((const char *)rest);
                else {
                    msg.appendf("extra data[%d]",len);
                    for (unsigned i=0;(i<16)&&(i<len);i++)
                        msg.appendf(" %2x",(int)rest[i]);
                }
            }
            else if(errCode == 8209)
                msg.append("Failed to open directory.");
            else
                msg.append("ERROR #").append(errCode);
#ifdef _DEBUG
            ERRLOG("%s",msg.str());
            PrintStackReport();
#endif
            throw createDafsException(errCode,msg.str());
        }
    }

    void sendRemoteCommand(MemoryBuffer & src, bool retry)
    {
        MemoryBuffer reply;
        sendRemoteCommand(src, reply, retry);
    }

    void throwUnauthenticated(const IpAddress &ip,const char *user,unsigned err=0)
    {
        if (err==0)
            err = RFSERR_AuthenticateFailed;
        StringBuffer msg;
        msg.appendf("Authentication for %s on ",user);
        ip.getIpText(msg);
        msg.append(" failed");
        throw createDafsException(err, msg.str());
    }

    void sendAuthentication(const IpAddress &serverip)
    {
        // send my sig
        // first send my sig which if stream unencrypted will get returned as a bad command
        OnceKey oncekey;
        genOnce(oncekey);
        MemoryBuffer sendbuf;
        initSendBuffer(sendbuf);
        MemoryBuffer replybuf;
        MemoryBuffer encbuf; // because aesEncrypt clears input
        sendbuf.append((RemoteFileCommandType)RFCunlock).append(sizeof(oncekey),&oncekey);
        try {
            sendBuffer(socket, sendbuf);
            receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        }
        catch (IException *e)
        {
            EXCLOG(e,"Remote file - sendAuthentication(1)");
            throw;
        }
        unsigned errCode;
        replybuf.read(errCode);
        if (errCode!=0)  // no authentication required
            return;
        SocketEndpoint ep;
        ep.setLocalHost(0);
        byte ipdata[16];
        size32_t ipds = ep.getNetAddress(sizeof(ipdata),&ipdata);
        mergeOnce(oncekey,ipds,&ipdata);
        StringBuffer username;
        StringBuffer password;
        IPasswordProvider * pp = queryPasswordProvider();
        if (pp)
            pp->getPassword(serverip, username, password);
        if (!username.length())
            username.append("sds_system");      // default account (note if exists should have restricted access!)
        if (!password.length())
            password.append("sds_man");
        if (replybuf.remaining()<=sizeof(size32_t))
            throwUnauthenticated(serverip,username.str());
        size32_t bs;
        replybuf.read(bs);
        if (replybuf.remaining()<bs)
            throwUnauthenticated(serverip,username.str());
        MemoryBuffer skeybuf;
        aesDecrypt(&oncekey,sizeof(oncekey),replybuf.readDirect(bs),bs,skeybuf);
        if (skeybuf.remaining()<sizeof(OnceKey))
            throwUnauthenticated(serverip,username.str());
        OnceKey sokey;
        skeybuf.read(sizeof(OnceKey),&sokey);
        // now we have the key to use to send user/password
        MemoryBuffer tosend;
        tosend.append((byte)2).append(username).append(password);
        initSendBuffer(sendbuf.clear());
        sendbuf.append((RemoteFileCommandType)RFCunlockreply);
        aesEncrypt(&sokey, sizeof(oncekey), tosend.toByteArray(), tosend.length(), encbuf);
        sendbuf.append(encbuf.length());
        sendbuf.append(encbuf);
        try {
            sendBuffer(socket, sendbuf);
            receiveBuffer(socket, replybuf.clear(), NORMAL_RETRIES, 1024);
        }
        catch (IException *e)
        {
            EXCLOG(e,"Remote file - sendAuthentication(2)");
            throw;
        }
        replybuf.read(errCode);
        if (errCode==0)  // suceeded!
            return;
        throwUnauthenticated(serverip,username.str(),errCode);
    }

public:
    SocketEndpoint  &queryEp() { return ep; };


    CRemoteBase(const SocketEndpoint &_ep, const char * _filename)
        : filename(_filename)
    {
        ep = _ep;
        useSSL = securitySettings.useSSL;
    }

    void connect()
    {
        CriticalBlock block(crit);
        CriticalBlock block2(CConnectionTable::crit); // this shouldn't ever block
        if (AuthenticationEnabled) {
            SocketEndpoint tep(ep);
            setDafsEndpointPort(tep);
            connectSocket(tep);
        }
    }

    void disconnect()
    {
        CriticalBlock block(crit);
        CriticalBlock block2(CConnectionTable::crit); // this shouldn't ever block
        if (socket)
        {
            ISocket *s = socket.getClear();
            if (ConnectionTable)
            {
                SocketEndpoint tep(ep);
                setDafsEndpointPort(tep);
                ConnectionTable->remove(tep,s);
            }
            ::Release(s);
        }
    }

    const char *queryLocalName()
    {
        return filename;
    }

};

SocketEndpoint  CRemoteBase::lastfailep;
unsigned CRemoteBase::lastfailtime;


//---------------------------------------------------------------------------

class CRemoteDirectoryIterator : public CInterface, implements IDirectoryDifferenceIterator
{
    Owned<IFile>    cur;
    bool            curvalid;
    bool            curisdir;
    StringAttr      curname;
    CDateTime       curdt;
    __int64         cursize;
    StringAttr      dir;
    SocketEndpoint  ep;
    byte            *flags;
    unsigned        numflags;
    unsigned        curidx;
    unsigned        mask;

    MemoryBuffer buf;
public:
    static CriticalSection      crit;

    CRemoteDirectoryIterator(SocketEndpoint &_ep,const char *_dir)
        : dir(_dir)
    {
        // an extended difference iterator starts with 2 (for bwd compatibility)
        ep = _ep;
        curisdir = false;
        curvalid = false;
        cursize = 0;
        curidx = (unsigned)-1;
        mask = 0;
        numflags = 0;
        flags = NULL;
    }

    bool appendBuf(MemoryBuffer &_buf)
    {
        buf.setSwapEndian(_buf.needSwapEndian());
        byte hdr;
        _buf.read(hdr);
        if (hdr==2) {
            _buf.read(numflags);
            flags = (byte *)malloc(numflags);
            _buf.read(numflags,flags);
        }
        else {
            buf.append(hdr);
            flags = NULL;
            numflags = 0;
        }
        size32_t rest = _buf.length()-_buf.getPos();
        const byte *rb = (const byte *)_buf.readDirect(rest);
        bool ret = true;
        // At the last byte of the rb (rb[rest-1]) is the stream live flag
        //  True if the stream has more data
        //  False at the end of stream
        // The previous byte (rb[rest-2]) is the flag to signal there are more
        // valid entries in this block
        //  True if there are valid directory entry follows this flag
        //  False if there are no more valid entry in this block aka end of block
        // If there is more data in the stream, the end of block flag should be removed
        if (rest&&(rb[rest-1]!=0)) 
        {
            rest--; // remove stream live flag
            if(rest && (0 == rb[rest-1]))
            	rest--; //Remove end of block flag
            ret = false;  // continuation
        }
        buf.append(rest,rb);
        return ret;
    }

    ~CRemoteDirectoryIterator()
    {
        free(flags);
    }

    IMPLEMENT_IINTERFACE

    bool first()
    {
        curidx = (unsigned)-1;
        buf.reset();
        return next();
    }
    bool next()
    {
        loop {
            curidx++;
            cur.clear();
            curdt.clear();
            curname.clear();
            cursize = 0;
            curisdir = false;
            if (buf.getPos()>=buf.length())
                return false;
            byte isValidEntry;
            buf.read(isValidEntry);
            curvalid = isValidEntry!=0;
            if (!curvalid) 
                return false;
            buf.read(curisdir);
            buf.read(cursize);
            curdt.deserialize(buf);
            buf.read(curname);
            // kludge for bug in old linux jlibs
            if (strchr(curname,'\\')&&(getPathSepChar(dir)=='/')) {
                StringBuffer temp(curname);
                temp.replace('\\','/');
                curname.set(temp.str());
            }
            if ((mask==0)||(getFlags()&mask))
                break;
        }
        return true;
    }

    bool isValid() 
    { 
        return curvalid; 
    }
    IFile & query() 
    { 
        if (!cur) {
            StringBuffer full(dir);
            addPathSepChar(full).append(curname);
            if (ep.isNull())
                cur.setown(createIFile(full.str()));
            else {
                RemoteFilename rfn;
                rfn.setPath(ep,full.str());
                cur.setown(createIFile(rfn));
            }
        }
        return *cur; 
    }
    StringBuffer &getName(StringBuffer &buf)
    {
        return buf.append(curname);
    }
    bool isDir() 
    { 
        return curisdir; 
    }

    __int64 getFileSize()
    {
        if (curisdir)
            return -1;
        return cursize;
    }


    bool getModifiedTime(CDateTime &ret)
    {
        ret = curdt;
        return true;
    }

    void setMask(unsigned _mask)
    {
        mask = _mask;
    }
    
    virtual unsigned getFlags()
    {
        if (flags&&(curidx<numflags))
            return flags[curidx];
        return 0;
    }

    static bool serialize(MemoryBuffer &mb,IDirectoryIterator *iter, size32_t bufsize, bool first)
    {
        bool ret = true;
        byte b=1;
        StringBuffer tmp;
        if (first ? iter->first() : iter->next()) {
            loop {
                mb.append(b);
                bool isdir = iter->isDir();
                __int64 sz = isdir?0:iter->getFileSize();
                CDateTime dt;
                iter->getModifiedTime(dt);
                iter->getName(tmp.clear());
                mb.append(isdir).append(sz);
                dt.serialize(mb);
                mb.append(tmp.str());
                if (bufsize&&(mb.length()>=bufsize-1)) {
                    ret = false;
                    break;
                }
                if (!iter->next())
                    break;
            }
        }               
        b = 0;
        mb.append(b);   
        return ret;
    }

    static void serializeDiff(MemoryBuffer &mb,IDirectoryDifferenceIterator *iter)
    {
        // bit slow
        MemoryBuffer flags;
        ForEach(*iter) 
            flags.append((byte)iter->getFlags());
        if (flags.length()) {
            byte b = 2;
            mb.append(b).append((unsigned)flags.length()).append(flags);
        }
        serialize(mb,iter,0,true);
    }

    void serialize(MemoryBuffer &mb,bool isdiff)
    {
        byte b;
        if (isdiff&&numflags&&flags) {
            b = 2;
            mb.append(b).append(numflags).append(numflags,flags);
        }
        serialize(mb,this,0,true);
    }

};

class CCritTable;
class CEndpointCS : public CriticalSection, public CInterface
{
    CCritTable &table;
    const SocketEndpoint ep;
public:
    CEndpointCS(CCritTable &_table, const SocketEndpoint &_ep) : table(_table), ep(_ep) { }
    const void *queryFindParam() const { return &ep; }

    virtual void beforeDispose();
};

class CCritTable : private SimpleHashTableOf<CEndpointCS, const SocketEndpoint>
{
    typedef SimpleHashTableOf<CEndpointCS, const SocketEndpoint> PARENT;
    CriticalSection crit;
public:
    CEndpointCS *getCrit(const SocketEndpoint &ep)
    {
        CriticalBlock b(crit);
        Linked<CEndpointCS> clientCrit = find(ep);
        if (!clientCrit || !clientCrit->isAlive()) // if !isAlive(), then it is in the process of being destroyed/removed.
        {
            clientCrit.setown(new CEndpointCS(*this, ep));
            replace(*clientCrit); // NB table doesn't own
        }
        return clientCrit.getClear();
    }
    unsigned getHashFromElement(const void *e) const
    {
        const CEndpointCS &elem=*(const CEndpointCS *)e;
        return getHashFromFindParam(elem.queryFindParam());
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    void removeExact(CEndpointCS *clientCrit)
    {
        CriticalBlock b(crit);
        PARENT::removeExact(clientCrit); // NB may not exist, could have been replaced if detected !isAlive() in getCrit()
    }
} *dirCSTable;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    dirCSTable = new CCritTable;
    return true;
}
MODULE_EXIT()
{
    delete dirCSTable;
}

void CEndpointCS::beforeDispose()
{
    table.removeExact(this);
}


class CRemoteFile : public CRemoteBase, implements IFile
{
    StringAttr remotefilename;
    unsigned flags;
public:
    IMPLEMENT_IINTERFACE
    CRemoteFile(const SocketEndpoint &_ep, const char * _filename)
        : CRemoteBase(_ep, _filename)
    {
        flags = ((unsigned)IFSHread)|((S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)<<16);
    }

    bool exists()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCexists).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
    {
        CDateTime dummyTime;
        if (!createTime)
            createTime = &dummyTime;
        if (!modifiedTime)
            modifiedTime = &dummyTime;
        if (!accessedTime)
            accessedTime = &dummyTime;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgettime).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        bool ok;
        replyBuffer.read(ok);
        if (ok) {
            createTime->deserialize(replyBuffer);
            modifiedTime->deserialize(replyBuffer);
            accessedTime->deserialize(replyBuffer);
        }
        return ok;
    }

    bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsettime).append(filename);
        if (createTime) {
            sendBuffer.append((bool)true);
            createTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (modifiedTime) {
            sendBuffer.append((bool)true);
            modifiedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (accessedTime) {
            sendBuffer.append((bool)true);
            accessedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    fileBool isDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisdirectory).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }


    fileBool isFile()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisfile).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    fileBool isReadOnly()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisreadonly).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);
        
        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    IFileIO * open(IFOmode mode,IFEflags extraFlags=IFEnone);
    IFileIO * openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags=IFEnone);
    IFileAsyncIO * openAsync(IFOmode mode) { return NULL; } // not supported

    const char * queryFilename()
    {
        if (remotefilename.isEmpty()) {
            RemoteFilename rfn;
            rfn.setPath(ep,filename);
            StringBuffer path;
            rfn.getRemotePath(path);
            remotefilename.set(path);
        }
        return remotefilename.get();
    }

    void resetLocalFilename(const char *name)
    {
        remotefilename.clear();
        filename.set(name);
    }

    bool remove()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCremove).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    void rename(const char *newname)
    {
    // currently ignores directory on newname (in future versions newname will be required to be tail only and not full path)
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        path.append(splitDirTail(newname,newdir));
        if (newdir.length()&&(strcmp(newdir.str(),path.str())!=0)) 
            WARNLOG("CRemoteFile::rename passed full path '%s' that may not to match original directory '%s'",newname,path.str());
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCrename).append(filename).append(path);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(path);
        remotefilename.clear();
    }

    void move(const char *newname)
    {
        // like rename except between directories
        // first create replote path
        if (!newname||!*newname)
            return;
        RemoteFilename destrfn;
        if (isPathSepChar(newname[0])&&isPathSepChar(newname[1])) {
            destrfn.setRemotePath(newname);
            if (!destrfn.queryEndpoint().ipequals(ep)) {
                StringBuffer msg;
                msg.appendf("IFile::move %s to %s, destination node must match source node", queryFilename(), newname);
                throw createDafsException(RFSERR_MoveFailed,msg.str());
            }
        }
        else
            destrfn.setPath(ep,newname);
        StringBuffer dest;
        newname = destrfn.getLocalPath(dest).str();
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        const char *newtail = splitDirTail(newname,newdir);
        if (strcmp(newdir.str(),path.str())==0) {
            path.append(newtail);
            newname = path;
            sendBuffer.append((RemoteFileCommandType)RFCrename);    // use rename if we can (supported on older dafilesrv)
        }
        else 
            sendBuffer.append((RemoteFileCommandType)RFCmove);
        sendBuffer.append(filename).append(newname);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(newname);
        remotefilename.clear();
    }

    void setReadOnly(bool set)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetreadonly).append(filename).append(set);
        sendRemoteCommand(sendBuffer, replyBuffer);
    }

    void setFilePermissions(unsigned fPerms)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetfileperms).append(filename).append(fPerms);
        sendRemoteCommand(sendBuffer, replyBuffer);
    }

    offset_t size()
    {
#if 1 // faster method (consistant with IFile)
        // do this by using dir call (could be improved with new function but this not *too* bad)
        if (isSpecialPath(filename))
            return 0;   // queries deemed to always exist (though don't know size).
                        // if needed to get size I guess could use IFileIO method and cache (bit of pain though)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return (offset_t)-1;
        return (offset_t) iter->getFileSize();
#else
        IFileIO * io = open(IFOread);
        offset_t length = (offset_t)-1;
        if (io)
        {   
            length = io->size();
            io->Release();
        }
        return length;
#endif
    }

    bool createDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcreatedir).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    virtual IDirectoryIterator *directoryFiles(const char *mask,bool sub,bool includedirs)
    {
        if (mask&&!*mask)
            return createDirectoryIterator("",""); // NULL iterator

        CRemoteDirectoryIterator *ret = new CRemoteDirectoryIterator(ep, filename);
        byte stream=1;

        Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
        CriticalBlock block(*crit);
        loop {
            MemoryBuffer sendBuffer;
            initSendBuffer(sendBuffer);
            MemoryBuffer replyBuffer;
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(filename).append(mask?mask:"").append(includedirs).append(sub).append(stream);
            sendRemoteCommand(sendBuffer, replyBuffer);
            if (ret->appendBuf(replyBuffer))
                break;
            stream = 2;
        }
        return ret;
    }

    IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) // returns NULL if timed out
    {
        // abortsem not yet supported
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCmonitordir).append(filename).append(mask?mask:"").append(includedirs).append(sub);
        sendBuffer.append(checkinterval).append(timeout);
        __int64 cancelid=0; // not yet used
        sendBuffer.append(cancelid);
        byte isprev=(prev!=NULL)?1:0;
        sendBuffer.append(isprev);
        if (prev) 
            CRemoteDirectoryIterator::serialize(sendBuffer,prev,0,true);
        sendRemoteCommand(sendBuffer, replyBuffer);
        byte status;
        replyBuffer.read(status);
        if (status==1) {
            CRemoteDirectoryIterator *iter = new CRemoteDirectoryIterator(ep, filename);
            iter->appendBuf(replyBuffer);
            return iter;
        }
        return NULL;
    }

    bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime)
    {
        // do this by using dir call (could be improved with new function but this not *too* bad)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return false;
        isdir = iter->isDir();
        size = (offset_t) iter->getFileSize();
        iter->getModifiedTime(modtime);
        return true;
    }



    bool setCompression(bool set)
    {
        assertex(!"Need to implement compress()");
        return false;
    }

    offset_t compressedSize()
    {
        assertex(!"Need to implement actualSize()");
        return (offset_t)-1;
    }

    void serialize(MemoryBuffer &tgt)
    {
        throwUnexpected();
    }

    void deserialize(MemoryBuffer &src)
    {
        throwUnexpected();
    }

    unsigned getCRC()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgetcrc).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer, true, true);
        
        unsigned crc;
        replyBuffer.read(crc);
        return crc;
    }

    void setCreateFlags(unsigned short cflags)
    {
        flags &= 0xffff;
        flags |= ((unsigned)cflags<<16);
    }

    unsigned short getCreateFlags()
    {
        return (unsigned short)(flags>>16);
    }

    void setShareMode(IFSHmode shmode)
    {
        flags &= ~(IFSHfull|IFSHread);
        flags |= (unsigned)(shmode&(IFSHfull|IFSHread));
    }

    unsigned short getShareMode()
    {
        return (unsigned short)(flags&0xffff);
    }

    void remoteExtractBlobElements(const char * prefix, ExtractedBlobArray & extracted)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        sendBuffer.append((RemoteFileCommandType)RFCextractblobelements).append(prefix).append(filename);
        MemoryBuffer replyBuffer;
        sendRemoteCommand(sendBuffer, replyBuffer, true, true); // handles error code
        unsigned n;
        replyBuffer.read(n);
        for (unsigned i=0;i<n;i++) {
            ExtractedBlobInfo *item = new ExtractedBlobInfo;
            item->deserialize(replyBuffer);
            extracted.append(*item);
        }
    }

    bool copySectionAsync(const char *uuid,const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, unsigned timeout)
    {
        // now if we get here is it can be assumed the source file is local to where we send the command
        StringBuffer tos;
        dest.getRemotePath(tos);
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcopysection).append(uuid).append(queryLocalName()).append(tos).append(toOfs).append(fromOfs).append(size).append(timeout);
        sendRemoteCommand(sendBuffer, replyBuffer);
        unsigned status;
        replyBuffer.read(status);
        if (progress) {
            offset_t sizeDone;
            offset_t totalSize;
            replyBuffer.read(sizeDone).read(totalSize);
            progress->onProgress(sizeDone,totalSize);
        }
        return (AsyncCommandStatus)status!=ACScontinue; // should only otherwise be done as errors raised by exception
    }

    void copySection(const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, CFflags copyFlags=CFnone)
    {
        StringBuffer uuid;
        genUUID(uuid,true);
        unsigned timeout = 60*1000; // check every minute
        while(!copySectionAsync(uuid.str(),dest,toOfs,fromOfs,size,progress,timeout));
    }

    void copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags=CFnone);

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs, memsize_t len, bool write)
    {
        return NULL;
    }

    void treeCopyTo(IFile *dest,IpSubNet &subnet,IpAddress &resfrom,bool usetmp,CFflags copyFlags=CFnone)
    {
        resfrom.ipset(NULL);
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)(usetmp?RFCtreecopytmp:RFCtreecopy));
        RemoteFilename rfn;
        rfn.setPath(ep,filename);
        rfn.serialize(sendBuffer);
        const char *d = dest->queryFilename();
        if (!isAbsolutePath(d)) 
            throw MakeStringException(-1,"treeCopyFile destination '%s' is not an absolute path", d);
        rfn.setRemotePath(d);
        rfn.serialize(sendBuffer);
        StringBuffer tmp;
        subnet.getNetText(tmp);
        sendBuffer.append(tmp);
        subnet.getMaskText(tmp.clear());
        sendBuffer.append(tmp);
        unsigned status=1;
        try {
            sendRemoteCommand(sendBuffer, replyBuffer);
            replyBuffer.read(status);
        }
        catch (IDAFS_Exception *e) {
            if (e->errorCode()!=RFSERR_InvalidCommand) 
                throw;
            e->Release();
            status = (unsigned)-1;
        }
        if (status==-1) {
            resfrom.ipset(ep);
            StringBuffer tmp;
            WARNLOG("dafilesrv on %s does not support treeCopyTo - falling back to copyTo",resfrom.getIpText(tmp).str());
            copyTo(dest,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
            status = 0;
        }
        else if (status==0)
            resfrom.ipdeserialize(replyBuffer);
    }



};

void clientCacheFileConnect(SocketEndpoint &_ep,unsigned timeout)
{
    if (!timeout) {
        SocketEndpoint ep(_ep);
        setDafsEndpointPort(ep);
        Owned<CRemoteFile> cfile = new CRemoteFile(ep, "null");
        cfile->connect();
        return; // frees file and adds its socket to cache
    }
    // timeout needed so start a thread (that may become orphaned)
    class cThread: public Thread
    {
        SocketEndpoint ep;
    public:
        cThread(SocketEndpoint &_ep)
            : Thread("cacheFileConnect")
        {
            ep = _ep;
        }
        int run()
        {
            try {
                clientCacheFileConnect(ep,0);
            }
            catch (IException *e) {
                CriticalBlock block(sect);
                except.setown(e);
            }
            return 0;
        }
        Owned<IException> except;
        CriticalSection sect;
        
    } *thread;
    thread = new cThread(_ep);
    thread->start();
    IException *e =NULL;
    if (!thread->join(timeout)) {
        StringBuffer msg("Timed out connecting to ");
        _ep.getUrlStr(msg);
        e =  createDafsException(RFSERR_AuthenticateFailed,msg.str());
    }
    {
        CriticalBlock block(thread->sect);
        if (!e&&thread->except) 
            e = thread->except.getClear();
    }
    thread->Release();
    if (e)
        throw e;
}

void clientAddSocketToCache(SocketEndpoint &ep,ISocket *socket)
{
    CriticalBlock block(CConnectionTable::crit); 
    if (ConnectionTable)
        ConnectionTable->addLink(ep,socket);
}


IFile * createRemoteFile(SocketEndpoint &ep, const char * filename)
{
    IFile *ret = createFileLocalMount(ep,filename);
    if (ret)
        return ret;
    return new CRemoteFile(ep, filename);
}


void clientDisconnectRemoteFile(IFile *file)
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (cfile)
        cfile->disconnect();
}

bool clientResetFilename(IFile *file, const char *newname) // returns false if not remote
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (!cfile) 
        return false;
    cfile->resetLocalFilename(newname);
    return true;
}



extern bool clientAsyncCopyFileSection(const char *uuid,
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toOfs,                     // -1 created file and copies to start
                        offset_t fromOfs,
                        offset_t size,
                        ICopyFileProgress *progress,
                        unsigned timeout)       // returns true when done
{
    CRemoteFile *cfile = QUERYINTERFACE(from,CRemoteFile);
    if (!cfile) {
        // local - do sync
        from->copySection(to,toOfs,fromOfs,size,progress);
        return true;
    }
    return cfile->copySectionAsync(uuid,to,toOfs,fromOfs, size, progress, timeout);

}




//---------------------------------------------------------------------------

class CRemoteFileIO : public CInterface, implements IFileIO
{
protected:
    Linked<CRemoteFile> parent;
    RemoteFileIOHandle  handle;
    std::atomic<cycle_t> ioReadCycles;
    std::atomic<cycle_t> ioWriteCycles;
    std::atomic<__uint64> ioReadBytes;
    std::atomic<__uint64> ioWriteBytes;
    std::atomic<__uint64> ioReads;
    std::atomic<__uint64> ioWrites;
    IFOmode mode;
    compatIFSHmode compatmode;
    IFEflags extraFlags;
    bool disconnectonexit;
public:
    IMPLEMENT_IINTERFACE
    CRemoteFileIO(CRemoteFile *_parent)
        : parent(_parent), ioReadCycles(0), ioWriteCycles(0), ioReadBytes(0), ioWriteBytes(0), ioReads(0), ioWrites(0)

    {
        handle = 0;
        disconnectonexit = false;
    }

    ~CRemoteFileIO()
    {
        if (handle) {
            try {
                close();
            }
            catch (IException *e) {
                StringBuffer s;
                e->errorMessage(s);
                WARNLOG("CRemoteFileIO close file: %s",s.str());
                e->Release();
            }
        }
        if (disconnectonexit)
            parent->disconnect();
    }

    void close()
    {
        if (handle) {
            try {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCcloseIO).append(handle);
                parent->sendRemoteCommand(sendBuffer,false);
            }
            catch (IDAFS_Exception *e) {
                if ((e->errorCode()!=RFSERR_InvalidFileIOHandle)&&(e->errorCode()!=RFSERR_NullFileIOHandle))
                    throw;
                e->Release();
            }
            handle = 0;
        }
    }

    bool open(IFOmode _mode,compatIFSHmode _compatmode,IFEflags _extraFlags=IFEnone)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char *localname = parent->queryLocalName();
        localname = skipSpecialPath(localname);
        // also send _extraFlags
        // then also send sMode, cFlags
        unsigned short sMode = parent->getShareMode();
        unsigned short cFlags = parent->getCreateFlags();
        sendBuffer.append((RemoteFileCommandType)RFCopenIO).append(localname).append((byte)_mode).append((byte)_compatmode).append((byte)_extraFlags).append(sMode).append(cFlags);
        parent->sendRemoteCommand(sendBuffer, replyBuffer);

        replyBuffer.read(handle);
        if (!handle)
            return false;
        switch (_mode) {
        case IFOcreate:
            mode = IFOwrite;
            break;
        case IFOcreaterw:
            mode = IFOreadwrite;
            break;
        default:
            mode = _mode;
            break;
        }
        compatmode = _compatmode;
        extraFlags = _extraFlags;
        return true;
    }

    bool reopen() 
    {
        StringBuffer s;
        PROGLOG("Attempting reopen of %s on %s",parent->queryLocalName(),parent->queryEp().getUrlStr(s).str());
        if (open(mode,compatmode,extraFlags)) {
            return true;
        }
        return false;

    }


    offset_t size()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsize).append(handle);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false);                      
        // Retry using reopen TBD

        offset_t ret;
        replyBuffer.read(ret);
        return ret;
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        switch (kind)
        {
        case StCycleDiskReadIOCycles:
            return ioReadCycles.load(std::memory_order_relaxed);
        case StCycleDiskWriteIOCycles:
            return ioWriteCycles.load(std::memory_order_relaxed);
        case StTimeDiskReadIO:
            return cycle_to_nanosec(ioReadCycles.load(std::memory_order_relaxed));
        case StTimeDiskWriteIO:
            return cycle_to_nanosec(ioWriteCycles.load(std::memory_order_relaxed));
        case StSizeDiskRead:
            return ioReadBytes.load(std::memory_order_relaxed);
        case StSizeDiskWrite:
            return ioWriteBytes.load(std::memory_order_relaxed);
        case StNumDiskReads:
            return ioReads.load(std::memory_order_relaxed);
        case StNumDiskWrites:
            return ioWrites.load(std::memory_order_relaxed);
        }
        return 0;
    }

    size32_t read(offset_t pos, size32_t len, void * data)
    {
        size32_t got;
        MemoryBuffer replyBuffer;
        CCycleTimer timer;
        const void *b = doRead(pos,len,replyBuffer,got,data);
        ioReadCycles.fetch_add(timer.elapsedCycles());
        ioReadBytes.fetch_add(got);
        ++ioReads;
        if (b!=data)
            memcpy(data,b,got);
        return got;
    }

    virtual void flush()
    {
    }

    const void *doRead(offset_t pos, size32_t len, MemoryBuffer &replyBuffer, size32_t &got, void *dstbuf)
    {
        unsigned tries=0;
        loop {
            try {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                replyBuffer.clear();
                sendBuffer.append((RemoteFileCommandType)RFCread).append(handle).append(pos).append(len);
                parent->sendRemoteCommand(sendBuffer, replyBuffer,false);
                // kludge dafilesrv versions <= 1.5e don't return error correctly 
                if (replyBuffer.length()>len+sizeof(size32_t)+sizeof(unsigned)) {
                    size32_t save = replyBuffer.getPos();
                    replyBuffer.reset(len+sizeof(size32_t)+sizeof(unsigned));
                    unsigned errCode;
                    replyBuffer.read(errCode);
                    if (errCode) {
                        StringBuffer msg;
                        parent->ep.getUrlStr(msg.append('[')).append("] ");
                        if (replyBuffer.getPos()<replyBuffer.length()) {
                            StringAttr s;
                            replyBuffer.read(s);
                            msg.append(s);
                        }
                        else
                            msg.append("ERROR #").append(errCode);
                        throw createDafsException(errCode, msg.str());
                    }
                    else
                        replyBuffer.reset(save);
                }
                replyBuffer.read(got);
                if ((got>replyBuffer.remaining())||(got>len)) {
                    PROGLOG("Read beyond buffer %d,%d,%d",got,replyBuffer.remaining(),len);
                    throw createDafsException(RFSERR_ReadFailed, "Read beyond buffer");
                }
                return replyBuffer.readDirect(got);
            }
            catch (IJSOCK_Exception *e) {
                EXCLOG(e,"CRemoteFileIO::read");
                if (++tries>3)
                    throw;
                WARNLOG("Retrying read of %s (%d)",parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                    throw exc.getClear();
            }
        }
        got = 0;
        return NULL;
    }



    size32_t write(offset_t pos, size32_t len, const void * data)
    {
        unsigned tries=0;
        size32_t ret = 0;
        CCycleTimer timer;
        loop {
            try {
                MemoryBuffer replyBuffer;
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCwrite).append(handle).append(pos).append(len).append(len, data);
                parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
                replyBuffer.read(ret);
                break;
            }
            catch (IJSOCK_Exception *e) {
                EXCLOG(e,"CRemoteFileIO::write");
                if (++tries>3)
                    throw;
                WARNLOG("Retrying write(%" I64F "d,%d) of %s (%d)",pos,len,parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                    throw exc.getClear();

            }
        }
        ioWriteCycles.fetch_add(timer.elapsedCycles());
        ioWriteBytes.fetch_add(ret);
        ++ioWrites;
        if ((ret==(size32_t)-1) || (ret < len))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"write failed, disk full?");
        return ret;
    }

    offset_t appendFile(IFile *file,offset_t pos,offset_t len)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char * fname = file->queryFilename();
        sendBuffer.append((RemoteFileCommandType)RFCappend).append(handle).append(fname).append(pos).append(len);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true); // retry not safe
        
        offset_t ret;
        replyBuffer.read(ret);

        if ((ret==(offset_t)-1) || (ret < len))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"append failed, disk full?");    // though could be file missing TBD
        return ret;
    }


    void setSize(offset_t size) 
    { 
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetsize).append(handle).append(size);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
        // retry using reopen TBD


    }

    void setDisconnectOnExit(bool set) { disconnectonexit = set; }
};

void clientDisconnectRemoteIoOnExit(IFileIO *fileio,bool set)
{
    CRemoteFileIO *cfileio = QUERYINTERFACE(fileio,CRemoteFileIO);
    if (cfileio)
        cfileio->setDisconnectOnExit(set);
}



IFileIO * CRemoteFile::openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags)
{
    assertex(((unsigned)shmode&0xffffffc7)==0);
    compatIFSHmode compatmode;
    unsigned fileflags = (flags>>16) &  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH);
    if (fileflags&S_IXUSR)                      // this is bit hit and miss but backward compatible
        compatmode = compatIFSHexec;
    else if (fileflags&(S_IWGRP|S_IWOTH))
        compatmode = compatIFSHall;
    else if (shmode&IFSHfull)
        compatmode = compatIFSHwrite;
    else if (((shmode&(IFSHread|IFSHfull))==0) && ((fileflags&(S_IRGRP|S_IROTH))==0))
        compatmode = compatIFSHnone;
    else
        compatmode = compatIFSHread;
    Owned<CRemoteFileIO> res = new CRemoteFileIO(this);
    if (res->open(mode,compatmode,extraFlags))
        return res.getClear();
    return NULL;
}

IFileIO * CRemoteFile::open(IFOmode mode,IFEflags extraFlags)
{
    return openShared(mode,(IFSHmode)(flags&(IFSHread|IFSHfull)),extraFlags);
}

//---------------------------------------------------------------------------

void CRemoteFile::copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags)
{
    CRemoteFile *dstfile = QUERYINTERFACE(dest,CRemoteFile);
    if (dstfile&&!dstfile->queryEp().isLocal()) {
        StringBuffer tmpname;
        Owned<IFile> destf;
        RemoteFilename dest;
        if (usetmp) {
            makeTempCopyName(tmpname,dstfile->queryLocalName());
            dest.setPath(dstfile->queryEp(),tmpname.str());
        }
        else
            dest.setPath(dstfile->queryEp(),dstfile->queryLocalName());
        destf.setown(createIFile(dest));
        try {
            // following may fail if new dafilesrv not deployed on src
            copySection(dest,(offset_t)-1,0,(offset_t)-1,progress,copyFlags);
            if (usetmp) {
                StringAttr tail(pathTail(dstfile->queryLocalName()));
                dstfile->remove();
                destf->rename(tail);
            }
            return;
        }
        catch (IException *e)
        {
            StringBuffer s;
            s.appendf("Remote File Copy (%d): ",e->errorCode());
            e->errorMessage(s);
            s.append(", retrying local");
            WARNLOG("%s",s.str());
            e->Release();
        }
        // delete dest
        try {
            destf->remove();
        }
        catch (IException *e)
        {
            EXCLOG(e,"Remote File Copy, Deleting temporary file");
            e->Release();
        }
    }
    // assumption if we get here that source remote, dest local (or equiv)
    class cIntercept: implements ICopyFileIntercept
    {
        MemoryAttr ma;
        MemoryBuffer mb;
        virtual offset_t copy(IFileIO *from, IFileIO *to, offset_t ofs, size32_t sz)
        {
            if (ma.length()<sz)
                ma.allocate(sz);    // may be not used
            void *buf = ma.bufferBase();
            size32_t got;
            CRemoteFileIO *srcio = QUERYINTERFACE(from,CRemoteFileIO);
            const void *dst;
            if (srcio) 
                dst = srcio->doRead(ofs,sz,mb.clear(),got,buf);
            else {
                // shouldn't ever get here if source remote
                got = from->read(ofs, sz, buf);
                dst = buf;
            }
            if (got != 0)
                to->write(ofs, got, dst);
            return got;
        }
    } intercept;
    doCopyFile(dest,this,buffersize,progress,&intercept,usetmp,copyFlags);
}


unsigned getRemoteVersion(ISocket * socket, StringBuffer &ver)
{
    // used to have a global critical section here
    if (!socket)
        return 0;
    unsigned ret;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, reply, 1 ,4096);
        unsigned errCode;
        reply.read(errCode);
        if (errCode==RFSERR_InvalidCommand) {
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
    catch (IException *e) {
        EXCLOG(e);
        ::Release(e);
        return 0;
    }
    StringAttr vers;
    reply.read(vers);
    ver.append(vers);
    return ret;
}


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
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        replybuf.read(errCode);
    }
    catch (IJSOCK_Exception *e) { 
        if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
            EXCLOG(e);
        else
            errCode = 0;
    }
    catch (IException *e) {
        EXCLOG(e);
        ::Release(e);
    }
    return errCode;
}

int remoteExec(ISocket * socket, const char *cmdline, const char *workdir,bool sync,
                size32_t insize, void *inbuf, MemoryBuffer *outbuf)
{
    if (!socket)
        return -1;
    bool hasoutput = (outbuf!=NULL);
    if (!inbuf)
        insize = 0;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCexec).append(cmdline).append(workdir).append(sync).
               append(hasoutput).append(insize);
    if (insize)
        sendbuf.append(insize, inbuf);
    MemoryBuffer replybuf;
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, LENGTHY_RETRIES); // we don't know how long program will take really - assume <1hr
        int retcode;
        unsigned phandle;
        size32_t outsz;
        replybuf.read(retcode).read(phandle).read(outsz);
        if (outsz&&outbuf)
            replybuf.read(outsz,outbuf->reserve(outsz));
        return retcode;
    }
    catch (IException *e) {
        EXCLOG(e);
        ::Release(e);
    }

    return -1;
}

int setDafsTrace(ISocket * socket,byte flags)
{
    if (!socket) {
        byte ret = traceFlags;
        traceFlags = flags;
        return ret;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCsettrace).append(flags);
    MemoryBuffer replybuf;
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        return retcode;
    }
    catch (IException *e) {
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
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        if (retcode && errMsg && replybuf.remaining())
            replybuf.read(*errMsg);
        return retcode;
    }
    catch (IException *e) {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}

int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr)
{
    if (!socket) {
        retstr.append(VERSTRING);
        return 0;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetinfo).append(level);
    MemoryBuffer replybuf;
    try {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, 1);
        int retcode;
        replybuf.read(retcode);
        if (retcode==0) {
            StringAttr s;
            replybuf.read(s);
            retstr.append(s);
        }
        return retcode;
    }
    catch (IException *e) {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}



void remoteExtractBlobElements(const SocketEndpoint &ep,const char * prefix, const char * filename, ExtractedBlobArray & extracted)
{
    Owned<CRemoteFile> file = new CRemoteFile (ep,filename);
    file->remoteExtractBlobElements(prefix, extracted);
}

//====================================================================================================


class CAsyncCommandManager
{
    class CAsyncJob: public CInterface
    {
        class cThread: public Thread
        {
            CAsyncJob *parent;
        public:
            cThread(CAsyncJob *_parent)
                : Thread("CAsyncJob")
            {
                parent = _parent;
            }
            int run()
            {
                int ret = -1;
                try {
                    ret = parent->run();
                    parent->setDone();
                }
                catch (IException *e)
                {
                    parent->setException(e);
                }
                parent->signal();
                return ret;
            }
        } *thread;
        StringAttr uuid;
        CAsyncCommandManager &parent;
    public:
        CAsyncJob(CAsyncCommandManager &_parent, const char *_uuid)
            : parent(_parent), uuid(_uuid)
        {
            thread = new cThread(this);
            hash = hashc((const byte *)uuid.get(),uuid.length(),~0U);
        }
        ~CAsyncJob()
        {
            thread->join();
            thread->Release();
        }
        static void destroy(CAsyncJob *j)
        {
            j->Release();
        }
        void signal()
        {
            parent.signal();
        }
        void start()
        {
            parent.wait();
            thread->start();
        }
        void join()
        {
            thread->join();
        }
        static unsigned getHash(const char *key)
        {
            return hashc((const byte *)key,strlen(key),~0U);
        }
        static CAsyncJob* create(const char *key) { assertex(!"CAsyncJob::create not implemented"); return NULL; }
        unsigned hash;
        bool eq(const char *key)
        {
            return stricmp(key,uuid.get())==0;
        }
        virtual int run()=0;
        virtual void setException(IException *e)=0;
        virtual void setDone()=0;
    };

    class CAsyncCopySection: public CAsyncJob
    {
        Owned<IFile> src;
        RemoteFilename dst;
        offset_t toOfs;
        offset_t fromOfs;
        offset_t size;
        CFPmode mode; // not yet supported
        CriticalSection sect;
        offset_t done;
        offset_t total;
        Semaphore finished;
        AsyncCommandStatus status;
        Owned<IException> exc;
    public:
        CAsyncCopySection(CAsyncCommandManager &parent, const char *_uuid, const char *fromFile, const char *toFile, offset_t _toOfs, offset_t _fromOfs, offset_t _size)
            : CAsyncJob(parent, _uuid)
        {
            status = ACScontinue;
            src.setown(createIFile(fromFile));
            dst.setRemotePath(toFile);
            toOfs = _toOfs;
            fromOfs = _fromOfs;
            size = _size;
            mode = CFPcontinue;
            done = 0;
            total = (offset_t)-1;
        }
        AsyncCommandStatus poll(offset_t &_done, offset_t &_total,unsigned timeout)
        {
            if (timeout&&finished.wait(timeout)) 
                finished.signal();      // may need to call again
            CriticalBlock block(sect);
            if (exc) 
                throw exc.getClear();
            _done = done;
            _total = total;
            return status;
        }
        int run()
        {
            class cProgress: implements ICopyFileProgress
            {
                CriticalSection &sect;
                CFPmode &mode;
                offset_t &done;
                offset_t &total;
            public:
                cProgress(CriticalSection &_sect,offset_t &_done,offset_t &_total,CFPmode &_mode)
                    : sect(_sect), done(_done), total(_total), mode(_mode)
                {
                }
                CFPmode onProgress(offset_t sizeDone, offset_t totalSize)
                {
                    CriticalBlock block(sect);
                    done = sizeDone;
                    total = totalSize;
                    return mode;
                }
            } progress(sect,total,done,mode);
            src->copySection(dst,toOfs, fromOfs, size, &progress);  // exceptions will be handled by base class
            return 0;
        }
        void setException(IException *e)
        {
            EXCLOG(e,"CAsyncCommandManager::CAsyncJob"); 
            CriticalBlock block(sect);
            if (exc.get())
                e->Release();
            else
                exc.setown(e);
            status = ACSerror;
        }
        void setDone()
        {
            CriticalBlock block(sect);
            finished.signal();
            status = ACSdone;
        }
    };

    CMinHashTable<CAsyncJob> jobtable;
    CriticalSection sect;
    Semaphore threadsem;
    unsigned limit;

public:
    CAsyncCommandManager(unsigned _limit) : limit(_limit)
    {
        if (limit) // 0 == unbound
            threadsem.signal(limit); // max number of async jobs
    }
    void join()
    {
        CriticalBlock block(sect);
        unsigned i;
        CAsyncJob *j=jobtable.first(i);
        while (j) {
            j->join();
            j=jobtable.next(i);
        }
    }

    void signal()
    {
        if (limit)
            threadsem.signal();
    }

    void wait()
    {
        if (limit)
            threadsem.wait();
    }

    AsyncCommandStatus copySection(const char *uuid, const char *fromFile, const char *toFile, offset_t toOfs, offset_t fromOfs, offset_t size, offset_t &done, offset_t &total, unsigned timeout)
    {
        // return 0 if continuing, 1 if done
        CAsyncCopySection * job;
        Linked<CAsyncJob> cjob;
        {
            CriticalBlock block(sect);
            cjob.set(jobtable.find(uuid,false));
            if (cjob) {
                job = QUERYINTERFACE(cjob.get(),CAsyncCopySection);
                if (!job) {
                    throw MakeStringException(-1,"Async job ID mismatch");  
                }
            }
            else {
                job = new CAsyncCopySection(*this, uuid, fromFile, toFile, toOfs, fromOfs, size);
                cjob.setown(job);
                jobtable.add(cjob.getLink());
                cjob->start();
            }
        }
        AsyncCommandStatus ret = ACSerror;
        Owned<IException> rete;
        try {
            ret = job->poll(done,total,timeout);
        }
        catch (IException * e) {
            rete.setown(e);
        }
        if ((ret!=ACScontinue)||rete.get()) {
            job->join();
            CriticalBlock block(sect);
            jobtable.remove(job);
            if (rete.get())
                throw rete.getClear();
        }
        return ret;
    }
};


//====================================================================================================



inline void appendErr(MemoryBuffer &reply, RemoteFileCommandType e)
{
    reply.append((unsigned)e).append(getRFCText(e));
}
inline void appendErr2(MemoryBuffer &reply, RemoteFileCommandType e, unsigned v)
{
    StringBuffer msg;
    msg.append(getRFCText(e)).append(':').append(v);
    reply.append((unsigned)e).append(msg.str());
}
inline void appendErr3(MemoryBuffer &reply, RemoteFileCommandType e, int code, const char *errMsg)
{
    StringBuffer msg;
    msg.appendf("ERROR: %s(%d) '%s'", getRFCText(e), code, errMsg?errMsg:"");
    // some errors are RemoteFileCommandType, some are RFSERR_*
    // RFCOpenIO needs remapping to non-zero for client to know its an error
    if ((RemoteFileCommandType)e == RFCopenIO)
        e = (RemoteFileCommandType)RFSERR_OpenFailed;
    reply.append((unsigned)e);
    reply.append(msg.str());
}



#define MAPCOMMAND(c,p) case c: { ret = this->p(msg, reply) ; break; }
#define MAPCOMMANDCLIENT(c,p,client) case c: { ret = this->p(msg, reply, client); break; }
#define MAPCOMMANDCLIENTTHROTTLE(c,p,client,throttler) case c: { ret = this->p(msg, reply, client, throttler); break; }
#define MAPCOMMANDSTATS(c,p,stats) case c: { ret = this->p(msg, reply, stats); break; }
#define MAPCOMMANDCLIENTSTATS(c,p,client,stats) case c: { ret = this->p(msg, reply, client, stats); break; }

static unsigned ClientCount = 0;
static unsigned MaxClientCount = 0;
static CriticalSection ClientCountSect;

#define DEFAULT_THROTTLOG_LOG_INTERVAL_SECS 60 // log total throttled delay period


class CClientStats : public CInterface
{
    SpinLock spin;
public:
    CClientStats(const char *_client) : client(_client) { count = 0; bRead = 0; bWritten = 0; }
    const char *queryFindString() const { return client; }
    inline void addRead(unsigned len)
    {
        SpinBlock b(spin); // rare event, but we should change to a atomic<__int64> for >= c++11
        bRead += len;
    }
    inline void addWrite(unsigned len)
    {
        SpinBlock b(spin); // rare event, but we should change to a atomic<__int64> for >= c++11
        bWritten += len;
    }

    StringAttr client;
    unsigned __int64 count;
    unsigned __int64 bRead;
    unsigned __int64 bWritten;
};
class CClientStatsTable : public OwningStringSuperHashTableOf<CClientStats>
{
    typedef OwningStringSuperHashTableOf<CClientStats> PARENT;
    CriticalSection crit;
    unsigned cmdStats[RFCmax];

    static int compareElement(void* const *ll, void* const *rr)
    {
        const CClientStats *l = (const CClientStats *) *ll;
        const CClientStats *r = (const CClientStats *) *rr;
        if (l->count == r->count)
            return 0;
        else if (l->count<r->count)
            return 1;
        else
            return -1;
    }
public:
    CClientStatsTable()
    {
        memset(&cmdStats[0], 0, sizeof(cmdStats));
    }
    ~CClientStatsTable()
    {
        kill();
    }
    CClientStats *getClientReference(RemoteFileCommandType cmd, const char *client)
    {
        CriticalBlock b(crit);
        CClientStats *stats = PARENT::find(client);
        if (!stats)
        {
            stats = new CClientStats(client);
            PARENT::replace(*stats);
        }
        if (cmd<RFCmax) // i.e. ignore duff command (which will be traced), but still record client connected
            cmdStats[cmd]++;
        ++stats->count;
        return LINK(stats);
    }
    StringBuffer &getInfo(StringBuffer &info, unsigned level=1)
    {
        CriticalBlock b(crit);
        unsigned __int64 totalCmds = 0;
        for (unsigned c=0; c<RFCmax; c++)
            totalCmds += cmdStats[c];
        unsigned totalClients = PARENT::ordinality();
        info.appendf("Commands processed = %" I64F "u, unique clients = %u", totalCmds, totalClients);
        if (totalCmds)
        {
            info.append("Command stats:").newline();
            for (unsigned c=0; c<RFCmax; c++)
            {
                unsigned __int64 count = cmdStats[c];
                if (count)
                    info.append(getRFCText(c)).append(": ").append(count).newline();
            }
        }
        if (totalClients)
        {
            SuperHashIteratorOf<CClientStats> iter(*this);
            PointerArrayOf<CClientStats> elements;
            ForEach(iter)
            {
                CClientStats &elem = iter.query();
                elements.append(&elem);
            }
            elements.sort(&compareElement);
            if (level < 10)
            {
                // list up to 10 clients ordered by # of commands processed
                unsigned max=elements.ordinality();
                if (max>10)
                    max = 10; // cap
                info.append("Top 10 clients:").newline();
                for (unsigned e=0; e<max; e++)
                {
                    const CClientStats &element = *elements.item(e);
                    info.appendf("Client %s - %" I64F "d requests handled, bytes read = %" I64F "d, bytes written = % " I64F "d",
                            element.client.get(), element.count, element.bRead, element.bWritten).newline();
                }
            }
            else // list all
            {
                info.append("All clients:").newline();
                ForEachItemIn(e, elements)
                {
                    const CClientStats &element = *elements.item(e);
                    info.appendf("Client %s - %" I64F "d requests handled, bytes read = %" I64F "d, bytes written = % " I64F "d",
                            element.client.get(), element.count, element.bRead, element.bWritten).newline();
                }
            }
        }
        return info;
    }
    void reset()
    {
        CriticalBlock b(crit);
        memset(&cmdStats[0], 0, sizeof(cmdStats));
        kill();
    }
};

class CRemoteFileServer : public CInterface, implements IRemoteFileServer
{
    class CThrottler;
    class CRemoteClientHandler : public CInterface, implements ISocketSelectNotify
    {
    public:
        CRemoteFileServer *parent;
        Owned<ISocket> socket;
        StringAttr peerName;
        Owned<IAuthenticatedUser> user;
        MemoryBuffer msg;
        bool selecthandled;
        size32_t left;
        IArrayOf<IFileIO>   openfiles;      // kept in sync with handles
        Owned<IDirectoryIterator> opendir;
        StringAttrArray     opennames;      // for debug
        IntArray            handles;
        unsigned            lasttick, lastInactiveTick;
        atomic_t            &globallasttick;
        unsigned            previdx;        // for debug


        IMPLEMENT_IINTERFACE;

        CRemoteClientHandler(CRemoteFileServer *_parent,ISocket *_socket,IAuthenticatedUser *_user,atomic_t &_globallasttick)
            : socket(_socket), user(_user), globallasttick(_globallasttick)
        {
            previdx = (unsigned)-1;
            StringBuffer peerBuf;
            char name[256];
            name[0] = 0;
            int port = socket->peer_name(name,sizeof(name)-1);
            if (port>=0)
            {
                peerBuf.append(name);
                if (port)
                    peerBuf.append(':').append(port);
                peerName.set(peerBuf);
            }
            else
            {
                /* There's a possibility the socket closed before got here, in which case, peer name is unavailable
                 * May potentially be unavailable for other reasons also.
                 * Must be set, as used in client stats HT.
                 * If socket closed, the handler will start up but notice closed and quit
                 */
                peerName.set("UNKNOWN PEER NAME");
            }
            {
                CriticalBlock block(ClientCountSect);
                if (++ClientCount>MaxClientCount)
                    MaxClientCount = ClientCount;
                if (TF_TRACE_CLIENT_CONN)
                {
                    StringBuffer s;
                    s.appendf("Connecting(%p) [%d,%d] to ",this,ClientCount,MaxClientCount);
                    s.append(peerName);
                    PROGLOG("%s", s.str());
                }
            }
            parent = _parent;
            left = 0;
            msg.setEndian(__BIG_ENDIAN);
            selecthandled = false;
            touch();
        }
        ~CRemoteClientHandler()
        {
            {
                CriticalBlock block(ClientCountSect);
                ClientCount--;
                if (TF_TRACE_CLIENT_CONN) {
                    PROGLOG("Disconnecting(%p) [%d,%d] ",this,ClientCount,MaxClientCount);
                }
            }
            ISocket *sock = socket.getClear();
            try {
                sock->Release();
            }
            catch (IException *e) {
                EXCLOG(e,"~CRemoteClientHandler");
                e->Release();
            }
        }
        bool notifySelected(ISocket *sock,unsigned selected)
        {
            if (TF_TRACE_FULL)
                PROGLOG("notifySelected(%p)",this);
            if (sock!=socket)
                WARNLOG("notifySelected - invalid socket passed");
            size32_t avail = (size32_t)socket->avail_read();
            if (avail)
                touch();
            if (left==0)
            {
                try
                {
                    left = avail?receiveBufferSize(socket):0;
                }
                catch (IException *e)
                {
                    EXCLOG(e,"notifySelected(1)");
                    e->Release();
                    left = 0;
                }
                if (left)
                {
                    avail = (size32_t)socket->avail_read();
                    try
                    {
                        msg.ensureCapacity(left);
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e,"notifySelected(2)");
                        e->Release();
                        left = 0;
                        // if too big then corrupted packet so read avail to try and consume
                        char fbuf[1024];
                        while (avail)
                        {
                            size32_t rd = avail>sizeof(fbuf)?sizeof(fbuf):avail;
                            try
                            {
                                socket->read(fbuf, rd); // don't need timeout here
                                avail -= rd;
                            }
                            catch (IException *e)
                            {
                                EXCLOG(e,"notifySelected(2) flush");
                                e->Release();
                                break;
                            }
                        }
                        avail = 0;
                        left = 0;
                    }
                }
            }
            size32_t toread = left>avail?avail:left;
            if (toread)
            {
                try
                {
                    socket->read(msg.reserve(toread), toread);  // don't need timeout here
                }
                catch (IException *e)
                {
                    EXCLOG(e,"notifySelected(3)");
                    e->Release();
                    toread = left;
                    msg.clear();
                }
            }
            if (TF_TRACE_FULL)
                PROGLOG("notifySelected %d,%d",toread,left);
            if ((left!=0)&&(avail==0))
            {
                WARNLOG("notifySelected: Closing mid packet, %d remaining", left);
                toread = left;
                msg.clear();
            }
            left -= toread;
            if (left==0)
            {
                // DEBUG
                parent->notify(this, msg); // consumes msg
            }
            return false;
        }

        void logPrevHandle()
        {
            if (previdx<opennames.ordinality())
                PROGLOG("Previous handle(%d): %s",handles.item(previdx),opennames.item(previdx).text.get());
        }

        bool throttleCommand(MemoryBuffer &msg)
        {
            RemoteFileCommandType cmd = RFCunknown;
            Owned<IException> e;
            try
            {
                msg.read(cmd);
                parent->throttleCommand(cmd, msg, this);
                return true;
            }
            catch (IException *_e)
            {
                e.setown(_e);
            }
            /* processCommand() will handle most exception and replies,
             * but if throttleCommand fails before it gets that far, this will handle
             */
            MemoryBuffer reply;
            initSendBuffer(reply);
            StringBuffer s;
            e->errorMessage(s);
            appendErr3(reply, cmd, e->errorCode(), s.str());
            parent->appendError(cmd, this, cmd, reply);
            sendBuffer(socket, reply);
            return false;
        }

        void processCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CThrottler *throttler)
        {
            MemoryBuffer reply;
            parent->processCommand(cmd, msg, initSendBuffer(reply), this, throttler);
            sendBuffer(socket, reply);
        }

        bool immediateCommand() // returns false if socket closed or failure
        {
            MemoryBuffer msg;
            msg.setEndian(__BIG_ENDIAN);
            touch();
            size32_t avail = (size32_t)socket->avail_read();
            if (avail==0)
                return false;
            receiveBuffer(socket, msg, 5);   // shouldn't timeout as data is available
            touch();
            if (msg.length()==0)
                return false;
            return throttleCommand(msg);
        }

        void process(MemoryBuffer &msg)
        {
            if (selecthandled)
                throttleCommand(msg);
            else
            {
                // msg only used/filled if process() has been triggered by notify()
                while (parent->threadRunningCount()<=parent->targetActiveThreads) // if too many threads add to select handler
                {
                    int w;
                    try
                    {
                        w = socket->wait_read(1000);
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e, "CRemoteClientHandler::main wait_read error");
                        e->Release();
                        parent->onCloseSocket(this,1);
                        return;
                    }
                    if (w==0)
                        break;
                    if ((w<0)||!immediateCommand())
                    {
                        if (w<0)
                            WARNLOG("CRemoteClientHandler::main wait_read error");
                        parent->onCloseSocket(this,1);
                        return;
                    }
                }

                /* This is a bit confusing..
                 * The addClient below, adds this request to a selecthandler handled by another thread
                 * and passes ownership of 'this' (CRemoteClientHandler)
                 *
                 * When notified, the selecthandler will launch a new pool thread to handle the request
                 * If the pool thread limit is hit, the selecthandler will be blocked [ see comment in CRemoteFileServer::notify() ]
                 *
                 * Either way, a thread pool slot is occupied when processing a request.
                 * Blocked threads, will be blocked for up to 1 minute (as defined by createThreadPool call)
                 * IOW, if there are lots of incoming clients that can't be serviced by the CThrottler limit,
                 * a large number of pool threads will build up after a while.
                 *
                 * The CThrottler mechanism, imposes a further hard limit on how many concurrent request threads can be active.
                 * If the thread pool had an absolute limit (instead of just introducing a delay), then I don't see the point
                 * in this additional layer of throttling..
                 */
                selecthandled = true;
                parent->addClient(this);    // add to select handler
                // NB: this (CRemoteClientHandler) is now linked by the selecthandler and owned by the 'clients' list
            }
        }

        bool timedOut()
        {
            return (msTick()-lasttick)>CLIENT_TIMEOUT;
        }

        bool inactiveTimedOut()
        {
            unsigned ms = msTick();
            if ((ms-lastInactiveTick)>CLIENT_INACTIVEWARNING_TIMEOUT)
            {
                lastInactiveTick = ms;
                return true;
            }
            return false;
        }

        void touch()
        {
            lastInactiveTick = lasttick = msTick();
            atomic_set(&globallasttick,lasttick);
        }

        const char *queryPeerName()
        {
            return peerName;
        }

        bool getInfo(StringBuffer &str)
        {
            str.append("client(");
            const char *name = queryPeerName();
            bool ok;
            if (name)
            {
                ok = true;
                str.append(name);
            }
            else
                ok = false;
            unsigned ms = msTick();
            str.appendf("): last touch %d ms ago (%d, %d)",ms-lasttick,lasttick,ms);
            ForEachItemIn(i,handles) {
                str.appendf("\n  %d: ",handles.item(i));
                str.append(opennames.item(i).text);
            }
            return ok;
        }
    };

    class CThrottleQueueItem : public CSimpleInterface
    {
    public:
        RemoteFileCommandType cmd;
        Linked<CRemoteClientHandler> client;
        MemoryBuffer msg;
        CCycleTimer timer;
        CThrottleQueueItem(RemoteFileCommandType _cmd, MemoryBuffer &_msg, CRemoteClientHandler *_client) : cmd(_cmd), client(_client)
        {
            msg.swapWith(_msg);
        }
    };

    class CThrottler
    {
        CRemoteFileServer &owner;
        Semaphore sem;
        CriticalSection crit, configureCrit;
        StringAttr title;
        unsigned limit, delayMs, cpuThreshold, queueLimit;
        unsigned disabledLimit;
        unsigned __int64 totalThrottleDelay;
        CCycleTimer totalThrottleDelayTimer;
        QueueOf<CThrottleQueueItem, false> queue;
        unsigned statsIntervalSecs;

    public:
        CThrottler(CRemoteFileServer &_owner, const char *_title) : owner(_owner), title(_title)
        {
            totalThrottleDelay = 0;
            limit = 0;
            delayMs = DEFAULT_STDCMD_THROTTLEDELAYMS;
            cpuThreshold = DEFAULT_STDCMD_THROTTLECPULIMIT;
            disabledLimit = 0;
            queueLimit = DEFAULT_STDCMD_THROTTLEQUEUELIMIT;
            statsIntervalSecs = DEFAULT_STDCMD_THROTTLECPULIMIT;
        }
        ~CThrottler()
        {
            loop
            {
                Owned<CThrottleQueueItem> item = queue.dequeue();
                if (!item)
                    break;
            }
        }
        unsigned queryLimit() const { return limit; }
        unsigned queryDelayMs() const { return delayMs; };;
        unsigned queryCpuThreshold() const { return cpuThreshold; }
        unsigned queryQueueLimit() const { return queueLimit; }
        StringBuffer &getInfoSummary(StringBuffer &info)
        {
            info.appendf("Throttler(%s) - limit=%u, delayMs=%u, cpuThreshold=%u, queueLimit=%u", title.get(), limit, delayMs, cpuThreshold, queueLimit).newline();
            unsigned elapsedSecs = totalThrottleDelayTimer.elapsedMs()/1000;
            time_t simple;
            time(&simple);
            simple -= elapsedSecs;

            CDateTime dt;
            dt.set(simple);
            StringBuffer dateStr;
            dt.getTimeString(dateStr, true);
            info.appendf("Throttler(%s): statistics since %s", title.get(), dateStr.str()).newline();
            info.appendf("Total delay of %0.2f seconds", ((double)totalThrottleDelay)/1000).newline();
            info.appendf("Requests currently queued: %u", queue.ordinality());
            return info;
        }
        void getInfo(StringBuffer &info)
        {
            CriticalBlock b(crit);
            getInfoSummary(info).newline();
        }
        void configure(unsigned _limit, unsigned _delayMs, unsigned _cpuThreshold, unsigned _queueLimit)
        {
            if (_limit > THROTTLE_MAX_LIMIT || _delayMs > THROTTLE_MAX_DELAYMS || _cpuThreshold > THROTTLE_MAX_CPUTHRESHOLD || _queueLimit > THROTTLE_MAX_QUEUELIMIT)
                throw MakeStringException(0, "Throttler(%s), rejecting configure command: limit=%u (max permitted=%u), delayMs=%u (max permitted=%u), cpuThreshold=%u (max permitted=%u), queueLimit=%u (max permitted=%u)",
                                              title.str(), _limit, THROTTLE_MAX_LIMIT, _delayMs, THROTTLE_MAX_DELAYMS, _cpuThreshold,
                                              THROTTLE_MAX_CPUTHRESHOLD, _queueLimit, THROTTLE_MAX_QUEUELIMIT);
            CriticalBlock b(configureCrit);
            int delta = 0;
            if (_limit)
            {
                if (disabledLimit) // if transitioning from disabled to some throttling
                {
                    assertex(0 == limit);
                    delta = _limit - disabledLimit; // + or -
                    disabledLimit = 0;
                }
                else
                    delta = _limit - limit; // + or -
            }
            else if (0 == disabledLimit)
            {
                PROGLOG("Throttler(%s): disabled, previous limit: %u", title.get(), limit);
                /* disabling - set limit immediately to let all new transaction through.
                 * NB: the semaphore signals are not consumed in this case, because transactions could be waiting on it.
                 * Instead the existing 'limit' is kept in 'disabledLimit', so that if/when throttling is
                 * re-enabled, it is used as a basis for increasing or consuming the semaphore signal count.
                 */
                disabledLimit = limit;
                limit = 0;
            }
            if (delta > 0)
            {
                PROGLOG("Throttler(%s): Increasing limit from %u to %u", title.get(), limit, _limit);
                sem.signal(delta);
                limit = _limit;
                // NB: If throttling was off, this doesn't effect transactions in progress, i.e. will only throttle new transactions coming in.
            }
            else if (delta < 0)
            {
                PROGLOG("Throttler(%s): Reducing limit from %u to %u", title.get(), limit, _limit);
                // NB: This is not expected to take long
                CCycleTimer timer;
                while (delta < 0)
                {
                    if (sem.wait(1000))
                        ++delta;
                    else
                        PROGLOG("Throttler(%s): Waited %0.2f seconds so far for up to a maximum of %u (previous limit) transactions to complete, %u completed", title.get(), ((double)timer.elapsedMs())/1000, limit, -delta);
                }
                limit = _limit;
                // NB: doesn't include transactions in progress, i.e. will only throttle new transactions coming in.
            }
            if (_delayMs != delayMs)
            {
                PROGLOG("Throttler(%s): New delayMs=%u, previous: %u", title.get(), _delayMs, delayMs);
                delayMs = _delayMs;
            }
            if (_cpuThreshold != cpuThreshold)
            {
                PROGLOG("Throttler(%s): New cpuThreshold=%u, previous: %u", title.get(), _cpuThreshold, cpuThreshold);
                cpuThreshold = _cpuThreshold;
            }
            if (((unsigned)-1) != _queueLimit && _queueLimit != queueLimit)
            {
                PROGLOG("Throttler(%s): New queueLimit=%u%s, previous: %u", title.get(), _queueLimit, 0==_queueLimit?"(disabled)":"", queueLimit);
                queueLimit = _queueLimit;
            }
        }
        void setStatsInterval(unsigned _statsIntervalSecs)
        {
            if (_statsIntervalSecs != statsIntervalSecs)
            {
                PROGLOG("Throttler(%s): New statsIntervalSecs=%u, previous: %u", title.get(), _statsIntervalSecs, statsIntervalSecs);
                statsIntervalSecs = _statsIntervalSecs;
            }
        }
        void take(RemoteFileCommandType cmd) // cmd for info. only
        {
            loop
            {
                if (sem.wait(delayMs))
                    return;
                PROGLOG("Throttler(%s): transaction delayed [cmd=%s]", title.get(), getRFCText(cmd));
            }
        }
        void release()
        {
            sem.signal();
        }
        StringBuffer &getStats(StringBuffer &stats, bool reset)
        {
            CriticalBlock b(crit);
            getInfoSummary(stats);
            if (reset)
            {
                totalThrottleDelayTimer.reset();
                totalThrottleDelay = 0;
            }
            return stats;
        }
        void addCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CRemoteClientHandler *client)
        {
            CCycleTimer timer;
            Owned<IException> exception;
            bool hadSem = true;
            if (!sem.wait(delayMs))
            {
                CriticalBlock b(crit);
                if (!sem.wait(0)) // check hasn't become available
                {
                    unsigned cpu = getLatestCPUUsage();
                    if (getLatestCPUUsage()<cpuThreshold)
                    {
                        /* Allow to proceed, despite hitting throttle limit because CPU < threshold
                         * NB: The overall number of threads is still capped by the thread pool.
                         */
                        unsigned ms = timer.elapsedMs();
                        totalThrottleDelay += ms;
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s] for : %u milliseconds, proceeding as cpu(%u)<throttleCPULimit(%u)", title.get(), getRFCText(cmd), cpu, ms, cpuThreshold);
                        hadSem = false;
                    }
                    else
                    {
                        if (queueLimit && queue.ordinality()>=queueLimit)
                            throw MakeStringException(0, "Throttler(%s), the maxiumum number of items are queued (%u), rejecting new command[%s]", title.str(), queue.ordinality(), getRFCText(cmd));
                        queue.enqueue(new CThrottleQueueItem(cmd, msg, client)); // NB: takes over ownership of 'client' from running thread
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s], queuing (%u queueud), [client=%p, sock=%u]", title.get(), getRFCText(cmd), queue.ordinality(), client, client->socket->OShandle());
                        return;
                    }
                }
            }

            /* Guarantee that sem is released.
             * Should normally release on clean exit when queue is empty.
             */
            struct ReleaseSem
            {
                Semaphore *sem;
                ReleaseSem(Semaphore *_sem) { sem = _sem; }
                ~ReleaseSem() { if (sem) sem->signal(); }
            } releaseSem(hadSem?&sem:NULL);

            /* Whilst holding on this throttle slot (i.e. before signalling semaphore back), process
             * queued items. NB: other threads that are finishing will do also.
             * Queued items are processed 1st, then the current request, then anything that was queued when handling current request
             * Throttle slot (semaphore) is only given back when no more to do.
             */
            Linked<CRemoteClientHandler> currentClient;
            MemoryBuffer currentMsg;
            unsigned ms;
            loop
            {
                RemoteFileCommandType currentCmd;
                {
                    CriticalBlock b(crit);
                    Owned<CThrottleQueueItem> item = queue.dequeue();
                    if (item)
                    {
                        currentCmd = item->cmd;
                        currentClient.setown(item->client.getClear());
                        currentMsg.swapWith(item->msg);
                        ms = item->timer.elapsedMs();
                    }
                    else
                    {
                        if (NULL == client) // previously handled and queue empty
                        {
                            /* Commands are only queued if semaphore is exhaused (checked inside crit)
                             * so only signal the semaphore inside the crit, after checking if there are no queued items
                             */
                            if (hadSem)
                            {
                                releaseSem.sem = NULL;
                                sem.signal();
                            }
                            break;
                        }
                        currentCmd = cmd;
                        currentClient.set(client); // process current request after dealing with queue
                        currentMsg.swapWith(msg);
                        ms = timer.elapsedMs();
                        client = NULL;
                    }
                }
                if (ms >= 1000)
                {
                    if (ms>delayMs)
                        PROGLOG("Throttler(%s): transaction delayed [cmd=%s] for : %u seconds", title.get(), getRFCText(currentCmd), ms/1000);
                }
                {
                    CriticalBlock b(crit);
                    totalThrottleDelay += ms;
                }
                try
                {
                    currentClient->processCommand(currentCmd, currentMsg, this);
                }
                catch (IException *e)
                {
                    EXCLOG(e, "addCommand: processCommand failed");
                    e->Release();
                }
            }
        }
    };

    // temporarily release a throttler slot
    class CThrottleReleaseBlock
    {
        CThrottler &throttler;
        RemoteFileCommandType cmd;
    public:
        CThrottleReleaseBlock(CThrottler &_throttler, RemoteFileCommandType _cmd) : throttler(_throttler), cmd(_cmd)
        {
            throttler.release();
        }
        ~CThrottleReleaseBlock()
        {
            throttler.take(cmd);
        }
    };

    int                 lasthandle;
    CriticalSection     sect;
    Owned<ISocket>      acceptsock;
    Owned<ISocket>      rejectsock;//used to immediately reject nonsecure connection requests when in secure mode
    Owned<ISocketSelectHandler> selecthandler;
    Owned<IThreadPool>  threads;    // for commands
    bool stopping;
    unsigned clientcounttick;
    unsigned closedclients;
    CAsyncCommandManager asyncCommandManager;
    CThrottler stdCmdThrottler, slowCmdThrottler;
    CClientStatsTable clientStatsTable;
    atomic_t globallasttick;
    unsigned targetActiveThreads;

    int getNextHandle()
    {
        // called in sect critical block
        loop {
            if (lasthandle==INT_MAX)
                lasthandle = 1;
            else
                lasthandle++;
            unsigned idx1;
            unsigned idx2;
            if (!findHandle(lasthandle,idx1,idx2))
                return lasthandle;
        }
    }

    bool findHandle(int handle,unsigned &clientidx,unsigned &handleidx)
    {
        // called in sect critical block
        clientidx = (unsigned)-1;
        handleidx = (unsigned)-1;
        ForEachItemIn(i,clients) {
            CRemoteClientHandler &client = clients.item(i);
            ForEachItemIn(j,client.handles) {
                if (client.handles.item(j)==handle) {
                    handleidx = j;
                    clientidx = i;
                    return true;
                }
            }
        }
        return false;
    }

    class cCommandProcessor: public CInterface, implements IPooledThread
    {
        Owned<CRemoteClientHandler> client;
        MemoryBuffer msg;

    public:
        IMPLEMENT_IINTERFACE;

        struct cCommandProcessorParams
        {
            cCommandProcessorParams() { msg.setEndian(__BIG_ENDIAN); }
            CRemoteClientHandler *client;
            MemoryBuffer msg;
        };

        void init(void *_params)
        {
            cCommandProcessorParams &params = *(cCommandProcessorParams *)_params;
            client.setown(params.client);
            msg.swapWith(params.msg);
        }

        void main()
        {
            // idea is that initially we process commands inline then pass over to select handler
            try
            {
                client->process(msg);
            }
            catch (IException *e)
            {
                // suppress some errors
                EXCLOG(e,"cCommandProcessor::main");
                e->Release();
            }
            try
            {
                client.clear();
            }
            catch (IException *e)
            {
                // suppress some more errors clearing client
                EXCLOG(e,"cCommandProcessor::main(2)");
                e->Release();
            }
        }
        bool stop()
        {
            return true;
        }
        bool canReuse()
        {
            return false; // want to free owned socket
        }
    };

    IArrayOf<CRemoteClientHandler> clients;

    class cImpersonateBlock
    {
        CRemoteClientHandler &client;
    public:
        cImpersonateBlock(CRemoteClientHandler &_client)
            : client(_client)
        {
            if (client.user.get()) {
                if (TF_TRACE)
                    PROGLOG("Impersonate user: %s",client.user->username());
                client.user->impersonate();
            }
        }
        ~cImpersonateBlock()
        {
            if (client.user.get()) {
                if (TF_TRACE)
                    PROGLOG("Stop impersonating user: %s",client.user->username());
                client.user->revert();
            }
        }
    };

#define IMPERSONATE_USER(client) cImpersonateBlock ublock(client)

public:

    IMPLEMENT_IINTERFACE

    CRemoteFileServer(unsigned maxThreads, unsigned maxThreadsDelayMs, unsigned maxAsyncCopy)
        : stdCmdThrottler(*this, "stdCmdThrotlter"), slowCmdThrottler(*this, "slowCmdThrotlter"), asyncCommandManager(maxAsyncCopy)
    {
        lasthandle = 0;
        selecthandler.setown(createSocketSelectHandler(NULL));

        stdCmdThrottler.configure(DEFAULT_STDCMD_PARALLELREQUESTLIMIT, DEFAULT_STDCMD_THROTTLEDELAYMS, DEFAULT_STDCMD_THROTTLECPULIMIT, DEFAULT_STDCMD_THROTTLEQUEUELIMIT);
        slowCmdThrottler.configure(DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT, DEFAULT_SLOWCMD_THROTTLEDELAYMS, DEFAULT_SLOWCMD_THROTTLECPULIMIT, DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT);

        unsigned targetMinThreads=maxThreads*20/100; // 20%
        if (0 == targetMinThreads) targetMinThreads = 1;
        targetActiveThreads=maxThreads*80/100; // 80%
        if (0 == targetActiveThreads) targetActiveThreads = 1;

        class CCommandFactory : public CSimpleInterfaceOf<IThreadFactory>
        {
            CRemoteFileServer &parent;
        public:
            CCommandFactory(CRemoteFileServer &_parent) : parent(_parent) { }
            virtual IPooledThread *createNew()
            {
                return parent.createCommandProcessor();
            }
        };
        Owned<IThreadFactory> factory = new CCommandFactory(*this); // NB: pool links factory, so takes ownership
        threads.setown(createThreadPool("CRemoteFileServerPool", factory, NULL, maxThreads, maxThreadsDelayMs,
#ifdef __64BIT__
            0, // Unlimited stack size
#else
            0x10000,
#endif
        INFINITE,targetMinThreads));
        threads->setStartDelayTracing(60); // trace amount delayed every minute.

        PROGLOG("CRemoteFileServer: maxThreads = %u, maxThreadsDelayMs = %u, maxAsyncCopy = %u", maxThreads, maxThreadsDelayMs, maxAsyncCopy);

        stopping = false;
        clientcounttick = msTick();
        closedclients = 0;
        atomic_set(&globallasttick,msTick());
    }

    ~CRemoteFileServer()
    {
#ifdef _DEBUG
        PROGLOG("Exiting CRemoteFileServer");
#endif
        asyncCommandManager.join();
        clients.kill();
#ifdef _DEBUG
        PROGLOG("Exited CRemoteFileServer");
#endif
    }

    //MORE: The file handles should timeout after a while, and accessing an old (invalid handle)
    // should throw a different exception
    bool checkFileIOHandle(MemoryBuffer &reply, int handle, IFileIO *&fileio, bool del=false)
    {
        CriticalBlock block(sect);
        fileio = NULL;
        if (handle<=0) {
            appendErr(reply, (RemoteFileCommandType)RFSERR_NullFileIOHandle);
            return false;
        }
        unsigned clientidx;
        unsigned handleidx;
        if (findHandle(handle,clientidx,handleidx)) {
            CRemoteClientHandler &client = clients.item(clientidx);
            if (del) {
                client.handles.remove(handleidx);
                client.openfiles.remove(handleidx);
                client.opennames.remove(handleidx);
                client.previdx = (unsigned)-1;
            }
            else {
               fileio = &client.openfiles.item(handleidx);
               client.previdx = handleidx;
            }
            return true;
        }
        appendErr(reply, (RemoteFileCommandType)RFSERR_InvalidFileIOHandle);
        return false;
    }

    void onCloseSocket(CRemoteClientHandler *client, int which) 
    {
        if (!client)
            return;
        CriticalBlock block(sect);
#ifdef _DEBUG
        StringBuffer s(client->queryPeerName());
        PROGLOG("onCloseSocket(%d) %s",which,s.str());
#endif
        if (client->socket)
        {
            try
            {
                /* JCSMORE - shouldn't this really be dependent on whether selecthandled=true
                 * It has not been added to the selecthandler
                 * Harmless, but wasteful if so.
                 */
                selecthandler->remove(client->socket);
            }
            catch (IException *e) {
                EXCLOG(e,"CRemoteFileServer::onCloseSocket.1");
                e->Release();
            }
        }
        try {
            clients.zap(*client);
        }
        catch (IException *e) {
            EXCLOG(e,"CRemoteFileServer::onCloseSocket.2");
            e->Release();
        }
    }


    bool cmdOpenFileIO(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        Owned<StringAttrItem> name = new StringAttrItem;
        byte mode;
        byte share;
        msg.read(name->text).read(mode).read(share);  
        // also try to recv extra byte
        byte extra = 0;
        unsigned short sMode = IFUnone;
        unsigned short cFlags = IFUnone;
        if (msg.remaining() >= sizeof(byte))
        {
            msg.read(extra);
            // and then try to recv extra sMode, cFlags (always sent together)
            if (msg.remaining() >= (sizeof(sMode) + sizeof(cFlags)))
                msg.read(sMode).read(cFlags);
        }
        IFEflags extraFlags = (IFEflags)extra;
        // none => nocache for remote (hint)
        // can revert to previous behavior with conf file setting "allow_pgcache_flush=false"
        if (extraFlags == IFEnone)
            extraFlags = IFEnocache;
        Owned<IFile> file = createIFile(name->text);
        switch ((compatIFSHmode)share) {
        case compatIFSHnone:
            file->setCreateFlags(S_IRUSR|S_IWUSR);
            file->setShareMode(IFSHnone);
            break;
        case compatIFSHread:
            file->setShareMode(IFSHread);
            break;
        case compatIFSHwrite:
            file->setShareMode(IFSHfull);
            break;
        case compatIFSHexec:
            file->setCreateFlags(S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
            break;
        case compatIFSHall:
            file->setCreateFlags(S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH); // bit excessive
            file->setShareMode(IFSHfull);
            break;
        }
        // use sMode, cFlags if sent
        if (sMode != IFUnone && cFlags != IFUnone)
        {
            file->setCreateFlags(cFlags);
            file->setShareMode((IFSHmode)sMode);
        }
        if (TF_TRACE_PRE_IO)
            PROGLOG("before open file '%s',  (%d,%d,%d,%d,0%o)",name->text.get(),(int)mode,(int)share,extraFlags,sMode,cFlags);
        IFileIO *fileio = file->open((IFOmode)mode,extraFlags);
        int handle;
        if (fileio) {
            CriticalBlock block(sect);
            handle = getNextHandle();
            client.previdx = client.opennames.ordinality();
            client.handles.append(handle);
            client.openfiles.append(*fileio);
            client.opennames.append(*name.getLink());
        }
        else
            handle = 0;
        reply.append(RFEnoerror);
        reply.append(handle);
        if (TF_TRACE)
            PROGLOG("open file '%s',  (%d,%d) handle = %d",name->text.get(),(int)mode,(int)share,handle);
        return true;
    }

    bool cmdCloseFileIO(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        msg.read(handle);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio, true))
            return false;
        if (TF_TRACE)
            PROGLOG("close file,  handle = %d",handle);
        reply.append(RFEnoerror);
        return true;
    }

    bool cmdRead(MemoryBuffer & msg, MemoryBuffer & reply, CClientStats &stats)
    {
        int handle;
        __int64 pos;
        size32_t len;
        msg.read(handle).read(pos).read(len);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;

        //arrange it so we read directly into the reply buffer...
        unsigned posOfErr = reply.length();
        reply.append((unsigned)RFEnoerror);
        size32_t numRead;
        unsigned posOfLength = reply.length();
        if (TF_TRACE_PRE_IO)
            PROGLOG("before read file,  handle = %d, toread = %d",handle,len);
        void * data;
        {
            reply.reserve(sizeof(numRead));
            data = reply.reserve(len);
        }
        try {
            numRead = fileio->read(pos,len,data);
        }
        catch (IException *e)
        {
            reply.setWritePos(posOfErr);
            StringBuffer s;
            e->errorMessage(s);
            appendErr3(reply, (RemoteFileCommandType)RFSERR_ReadFailed,e->errorCode(),s.str());
            e->Release();
            return false;
        }
        stats.addRead(len);
        if (TF_TRACE)
            PROGLOG("read file,  handle = %d, pos = %" I64F "d, toread = %d, read = %d",handle,pos,len,numRead);
        {
            reply.setLength(posOfLength + sizeof(numRead) + numRead);
            reply.writeEndianDirect(posOfLength,sizeof(numRead),&numRead);
        }
        return true;
    }

    bool cmdSize(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        msg.read(handle);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;
        __int64 size = fileio->size();
        reply.append((unsigned)RFEnoerror).append(size);
        if (TF_TRACE)
            PROGLOG("size file,  handle = %d, size = %" I64F "d",handle,size);
        return true;
    }

    bool cmdSetSize(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        offset_t size;
        msg.read(handle).read(size);
        IFileIO *fileio;
        if (TF_TRACE)
            PROGLOG("set size file,  handle = %d, size = %" I64F "d",handle,size);
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;
        fileio->setSize(size);
        reply.append((unsigned)RFEnoerror);
        return true;
    }


    bool cmdWrite(MemoryBuffer & msg, MemoryBuffer & reply, CClientStats &stats)
    {
        int handle;
        __int64 pos;
        size32_t len;
        msg.read(handle).read(pos).read(len);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;

        const byte *data = (const byte *)msg.readDirect(len);
        if (TF_TRACE_PRE_IO)
            PROGLOG("before write file,  handle = %d, towrite = %d",handle,len);
        size32_t numWritten = fileio->write(pos,len,data);
        stats.addWrite(numWritten);
        if (TF_TRACE)
            PROGLOG("write file,  handle = %d, towrite = %d, written = %d",handle,len,numWritten);
        reply.append((unsigned)RFEnoerror).append(numWritten);
        return true;
    }

    bool cmdExists(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("exists,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        bool e = file->exists();
        reply.append((unsigned)RFEnoerror).append(e);
        return true;
    }


    bool cmdRemove(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("remove,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        bool e = file->remove();
        reply.append((unsigned)RFEnoerror).append(e);
        return true;
    }

    bool cmdGetVer(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        if (TF_TRACE)
            PROGLOG("getVer");
        if (msg.getPos()+sizeof(unsigned)>msg.length())
            reply.append((unsigned)RFEnoerror);
        else
            reply.append((unsigned)FILESRV_VERSION+0x10000);
        reply.append(VERSTRING);
        return true;
    }

    bool cmdRename(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("rename,  '%s' to '%s'",fromname.get(),toname.get());
        Owned<IFile> file=createIFile(fromname);
        file->rename(toname);
        reply.append((unsigned)RFEnoerror);
        return true;
        return false;
    }

    bool cmdMove(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("move,  '%s' to '%s'",fromname.get(),toname.get());
        Owned<IFile> file=createIFile(fromname);
        file->move(toname);
        reply.append((unsigned)RFEnoerror);
        return true;
        return false;
    }

    bool cmdCopy(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr fromname;
        msg.read(fromname);
        StringAttr toname;
        msg.read(toname);
        if (TF_TRACE)
            PROGLOG("copy,  '%s' to '%s'",fromname.get(),toname.get());
        copyFile(toname, fromname);
        reply.append((unsigned)RFEnoerror);
        return true;
    }

    bool cmdAppend(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client, CClientStats &stats)
    {
        IMPERSONATE_USER(client);
        int handle;
        __int64 pos;
        __int64 len;
        StringAttr srcname;
        msg.read(handle).read(srcname).read(pos).read(len);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;

        Owned<IFile> file = createIFile(srcname.get());
        __int64 written = fileio->appendFile(file,pos,len);
        stats.addWrite(written);
        if (TF_TRACE)
            PROGLOG("append file,  handle = %d, file=%s, pos = %" I64F "d len = %" I64F "d written = %" I64F "d",handle,srcname.get(),pos,len,written);
        reply.append((unsigned)RFEnoerror).append(written);
        return true;
    }

    bool cmdIsFile(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isFile,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isFile();
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdIsDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isDir,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isDirectory();
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdIsReadOnly(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isReadOnly,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = (unsigned)file->isReadOnly();
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdSetReadOnly(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        bool set;
        msg.read(name).read(set);

        if (TF_TRACE)
            PROGLOG("setReadOnly,  '%s' %d",name.get(),(int)set);
        Owned<IFile> file=createIFile(name);
        file->setReadOnly(set);
        reply.append((unsigned)RFEnoerror);
        return true;
    }

    bool cmdSetFilePerms(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        unsigned fPerms;
        msg.read(name).read(fPerms);
        if (TF_TRACE)
            PROGLOG("setFilePerms,  '%s' 0%o",name.get(),fPerms);
        Owned<IFile> file=createIFile(name);
        file->setFilePermissions(fPerms);
        reply.append((unsigned)RFEnoerror);
        return true;
    }

    bool cmdGetTime(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("getTime,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        CDateTime createTime;
        CDateTime modifiedTime;
        CDateTime accessedTime;
        bool ret = file->getTime(&createTime,&modifiedTime,&accessedTime);
        reply.append((unsigned)RFEnoerror).append(ret);
        if (ret) {
            createTime.serialize(reply);
            modifiedTime.serialize(reply);
            accessedTime.serialize(reply);
        }
        return true;
    }

    bool cmdSetTime(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        bool creategot;
        CDateTime createTime;
        bool modifiedgot;
        CDateTime modifiedTime;
        bool accessedgot;
        CDateTime accessedTime;
        msg.read(name);
        msg.read(creategot);
        if (creategot) 
            createTime.deserialize(msg);
        msg.read(modifiedgot);
        if (modifiedgot) 
            modifiedTime.deserialize(msg);
        msg.read(accessedgot);
        if (accessedgot) 
            accessedTime.deserialize(msg);

        if (TF_TRACE)
            PROGLOG("setTime,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);

        bool ret = file->setTime(creategot?&createTime:NULL,modifiedgot?&modifiedTime:NULL,accessedgot?&accessedTime:NULL);
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdCreateDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("CreateDir,  '%s'",name.get());
        Owned<IFile> dir=createIFile(name);
        bool ret = dir->createDirectory();
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdGetDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        StringAttr mask;
        bool includedir;
        bool sub;
        byte stream = 0;
        msg.read(name).read(mask).read(includedir).read(sub);
        if (msg.remaining()>=sizeof(byte)) {
            msg.read(stream);
            if (stream==1)
                client.opendir.clear();
        }

        if (TF_TRACE)
            PROGLOG("GetDir,  '%s', '%s'",name.get(),mask.get());
        Owned<IFile> dir=createIFile(name);

        Owned<IDirectoryIterator> iter;
        if (stream>1)
            iter.set(client.opendir);
        else {
            iter.setown(dir->directoryFiles(mask.length()?mask.get():NULL,sub,includedir));
            if (stream != 0)
                client.opendir.set(iter);
        }
        if (!iter) {
            reply.append((unsigned)RFSERR_GetDirFailed);
            return false;
        }
        reply.append((unsigned)RFEnoerror);
        if (CRemoteDirectoryIterator::serialize(reply,iter,stream?0x100000:0,stream<2)) {
            if (stream != 0)
                client.opendir.clear();
        }
        else {
            bool cont=true;
            reply.append(cont);
        }
        return true;
    }

    bool cmdMonitorDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        StringAttr mask;
        bool includedir;
        bool sub;
        unsigned checkinterval;
        unsigned timeout;
        __int64 cancelid; // not yet used
        msg.read(name).read(mask).read(includedir).read(sub).read(checkinterval).read(timeout).read(cancelid);
        byte isprev;
        msg.read(isprev);
        Owned<IDirectoryIterator> prev;
        if (isprev==1) {
            SocketEndpoint ep;
            CRemoteDirectoryIterator *di = new CRemoteDirectoryIterator(ep,name);
            di->appendBuf(msg);
            prev.setown(di);
        }
        if (TF_TRACE)
            PROGLOG("MonitorDir,  '%s' '%s'",name.get(),mask.get());
        Owned<IFile> dir=createIFile(name);
        Owned<IDirectoryDifferenceIterator> iter=dir->monitorDirectory(prev,mask.length()?mask.get():NULL,sub,includedir,checkinterval,timeout);
        reply.append((unsigned)RFEnoerror);
        byte state = (iter.get()==NULL)?0:1;
        reply.append(state);
        if (state==1)
            CRemoteDirectoryIterator::serializeDiff(reply,iter);
        return true;
    }

    bool cmdCopySection(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr uuid;
        StringAttr fromFile;
        StringAttr toFile;
        offset_t toOfs;
        offset_t fromOfs;
        offset_t size;
        offset_t sizeDone=0;
        offset_t totalSize=(offset_t)-1;
        unsigned timeout;
        msg.read(uuid).read(fromFile).read(toFile).read(toOfs).read(fromOfs).read(size).read(timeout);
        AsyncCommandStatus status = asyncCommandManager.copySection(uuid,fromFile,toFile,toOfs,fromOfs,size,sizeDone,totalSize,timeout);
        reply.append((unsigned)RFEnoerror).append((unsigned)status).append(sizeDone).append(totalSize);
        return true;
    }

    static void treeCopyFile(RemoteFilename &srcfn, RemoteFilename &dstfn, const char *net, const char *mask, IpAddress &ip, bool usetmp, CThrottler *throttler, CFflags copyFlags=CFnone)
    {
        unsigned start = msTick();
        Owned<IFile> dstfile = createIFile(dstfn);
        // the following is really to check the dest node is up and working (otherwise not much point in continuing!)
        if (dstfile->exists())
            PROGLOG("TREECOPY overwriting '%s'",dstfile->queryFilename());
        Owned<IFile> srcfile = createIFile(srcfn);
        unsigned lastmin = 0;
        if (!srcfn.queryIP().ipequals(dstfn.queryIP())) {
            CriticalBlock block(treeCopyCrit);
            loop {
                CDateTime dt;
                offset_t sz;
                try {
                    sz = srcfile->size();
                    if (sz==(offset_t)-1) {
                        if (TF_TRACE_TREE_COPY)
                            PROGLOG("TREECOPY source not found '%s'",srcfile->queryFilename());
                        break;
                    }
                    srcfile->getTime(NULL,&dt,NULL);
                }
                catch (IException *e) {
                    EXCLOG(e,"treeCopyFile(1)");
                    e->Release();
                    break;
                }
                Linked<CTreeCopyItem> tc;
                unsigned now = msTick();
                ForEachItemInRev(i1,treeCopyArray) {
                    CTreeCopyItem &item = treeCopyArray.item(i1);
                    // prune old entries (not strictly needed buf I think better)
                    if (now-item.lastused>TREECOPYPRUNETIME)
                        treeCopyArray.remove(i1);
                    else if (!tc.get()&&item.equals(srcfn,net,mask,sz,dt)) {
                        tc.set(&item);
                        item.lastused = now;
                    }
                }
                if (!tc.get()) {
                    if (treeCopyArray.ordinality()>=TREECOPY_CACHE_SIZE)
                        treeCopyArray.remove(0);
                    tc.setown(new CTreeCopyItem(srcfn,net,mask,sz,dt));
                    treeCopyArray.append(*tc.getLink());
                }
                ForEachItemInRev(cand,tc->loc) { // rev to choose copied locations first (maybe optional?)
                    if (!tc->busy->testSet(cand)) {
                        // check file accessible and matches
                        if (!cand&&dstfn.equals(tc->loc.item(cand)))  // hmm trying to overwrite existing, better humor
                            continue;
                        bool ok = true;
                        Owned<IFile> rmtfile = createIFile(tc->loc.item(cand));
                        if (cand) { // only need to check if remote
                            try {
                                if (rmtfile->size()!=sz)
                                    ok = false;
                                else {
                                    CDateTime fdt;
                                    rmtfile->getTime(NULL,&fdt,NULL);
                                    ok = fdt.equals(dt);
                                }
                            }
                            catch (IException *e) {
                                EXCLOG(e,"treeCopyFile(2)");
                                e->Release();
                                ok = false;
                            }
                        }
                        if (ok) { // if not ok leave 'busy'
                            // finally lets try and copy!
                            try {
                                if (TF_TRACE_TREE_COPY)
                                    PROGLOG("TREECOPY(started) %s to %s",rmtfile->queryFilename(),dstfile->queryFilename());
                                {
                                    CriticalUnblock unblock(treeCopyCrit); // note we have tc linked
                                    rmtfile->copyTo(dstfile,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
                                }
                                if (TF_TRACE_TREE_COPY)
                                    PROGLOG("TREECOPY(done) %s to %s",rmtfile->queryFilename(),dstfile->queryFilename());
                                tc->busy->set(cand,false);
                                if (treeCopyWaiting)
                                    treeCopySem.signal((treeCopyWaiting>1)?2:1);
                                // add to known locations
                                tc->busy->set(tc->loc.ordinality(),false); // prob already is clear
                                tc->loc.append(dstfn);
                                ip.ipset(tc->loc.item(cand).queryIP());
                                return;
                            }
                            catch (IException *e) {
                                if (cand==0) {
                                    tc->busy->set(0,false); // don't leave busy
                                    if (treeCopyWaiting)
                                        treeCopySem.signal();
                                    throw;      // what more can we do!
                                }
                                EXCLOG(e,"treeCopyFile(3)");
                                e->Release();
                            }
                        }
                    }
                }
                // all locations busy
                if (msTick()-start>TREECOPYTIMEOUT) {
                    WARNLOG("Treecopy %s wait timed out", srcfile->queryFilename());
                    break;
                }
                treeCopyWaiting++; // note this isn't precise - just indication
                {
                    CriticalUnblock unblock(treeCopyCrit);
                    if (throttler)
                    {
                        CThrottleReleaseBlock block(*throttler, RFCtreecopy);
                        treeCopySem.wait(TREECOPYPOLLTIME);
                    }
                    else
                        treeCopySem.wait(TREECOPYPOLLTIME);
                }
                treeCopyWaiting--;
                if ((msTick()-start)/10*1000!=lastmin) {
                    lastmin = (msTick()-start)/10*1000;
                    PROGLOG("treeCopyFile delayed: %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
                }
            }
        }
        else if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY source on same node as destination");
        if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY(started,fallback) %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
        try {
            GetHostIp(ip);
            srcfile->copyTo(dstfile,DEFAULT_COPY_BLKSIZE,NULL,usetmp,copyFlags);
        }
        catch (IException *e) {
            EXCLOG(e,"TREECOPY(done,fallback)");
            throw;
        }
        if (TF_TRACE_TREE_COPY)
            PROGLOG("TREECOPY(done,fallback) %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
    }

    bool cmdTreeCopy(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client, CThrottler *throttler, bool usetmp=false)
    {
        IMPERSONATE_USER(client);
        RemoteFilename src;
        src.deserialize(msg);
        RemoteFilename dst;
        dst.deserialize(msg);
        StringAttr net;
        StringAttr mask;
        msg.read(net).read(mask);
        IpAddress ip;
        treeCopyFile(src,dst,net,mask,ip,usetmp,throttler);
        unsigned status = 0;
        reply.append((unsigned)RFEnoerror).append((unsigned)status);
        ip.ipserialize(reply);
        return true;
    }

    bool cmdTreeCopyTmp(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client, CThrottler *throttler)
    {
        return cmdTreeCopy(msg, reply, client, throttler, true);
    }


    bool cmdGetCRC(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("getCRC,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        unsigned ret = file->getCRC();
        reply.append((unsigned)RFEnoerror).append(ret);
        return true;
    }

    bool cmdStop(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        PROGLOG("Abort request received");
        stopping = true;
        if (acceptsock) 
            acceptsock->cancel_accept();
        if (rejectsock)
            rejectsock->cancel_accept();
        reply.append((unsigned)RFEnoerror);
        return false;
    }

    bool cmdExec(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        StringAttr cmdline;
        StringAttr workdir;
        bool sync;
        bool hasoutput;
        size32_t insize;
        MemoryAttr inbuf;
        msg.read(cmdline).read(workdir).read(sync).read(hasoutput).read(insize);
        if (insize) 
            msg.read(insize, inbuf.allocate(insize));
        Owned<IPipeProcess> pipe = createPipeProcess();
        int retcode=-1;
        HANDLE phandle=(HANDLE)0;
        MemoryBuffer outbuf;
        if (pipe->run("EXEC",cmdline,workdir,insize!=0,hasoutput)) {
            if (insize) {
                pipe->write(insize, inbuf.get());
                pipe->closeInput();
            }
            if (hasoutput) {
                byte buf[4096];
                loop {
                    size32_t read = pipe->read(sizeof(buf),buf);
                    if (!read)
                        break;
                    outbuf.append(read,buf);
                }
            }
            if (sync)
                retcode = pipe->wait();
            else {
                phandle = pipe->getProcessHandle(); 
                retcode = 0;
            }
        }
        size32_t outsz = outbuf.length();
        reply.append(retcode).append((unsigned)phandle).append(outsz);
        if (outsz)
            reply.append(outbuf);
        return true;
    }

    bool cmdSetTrace(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        byte flags;
        msg.read(flags);
        int retcode=-1;
        if (flags!=255) {   // escape
            retcode = traceFlags;
            traceFlags = flags;
        }
        reply.append(retcode);
        return true;
    }

    bool cmdGetInfo(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        unsigned level=1;
        if (msg.remaining() >= sizeof(unsigned))
            msg.read(level);
        StringBuffer retstr;
        getInfo(retstr, level);
        reply.append(0).append(retstr.str());
        return true;
    }

    bool cmdFirewall(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        // TBD
        StringBuffer retstr;
        getInfo(retstr);
        reply.append(0).append(retstr.str());
        return true;
    }

    bool cmdExtractBlobElements(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr prefix;
        StringAttr filename;
        msg.read(prefix).read(filename);
        RemoteFilename rfn;
        rfn.setLocalPath(filename);
        ExtractedBlobArray extracted;
        extractBlobElements(prefix, rfn, extracted);
        unsigned n = extracted.ordinality();
        reply.append((unsigned)RFEnoerror).append(n);
        for (unsigned i=0;i<n;i++)
            extracted.item(i).serialize(reply);
        return true;
    }

    bool cmdRedeploy(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        return false; // TBD
    }

    bool cmdKill(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        // TBD
        appendErr2(reply, (RemoteFileCommandType)RFSERR_InvalidCommand, RFCkill);
        return false;
    }

    bool cmdUnknown(MemoryBuffer & msg, MemoryBuffer & reply,RemoteFileCommandType cmd)
    {
        appendErr2(reply, (RemoteFileCommandType)RFSERR_InvalidCommand, cmd);
        return false;
    }

    bool cmdUnlock(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        // this is an attempt to authenticate when we haven't got authentication turned on
        if (TF_TRACE_CLIENT_STATS)
        {
            StringBuffer s(client.queryPeerName());
            PROGLOG("Connect from %s",s.str());
        }
        appendErr2(reply, (RemoteFileCommandType)RFSERR_InvalidCommand, RFCunlock);
        return false;
    }

    // legacy version
    bool cmdSetThrottle(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        unsigned limit, delayMs, cpuThreshold;
        msg.read(limit);
        msg.read(delayMs);
        msg.read(cpuThreshold);
        stdCmdThrottler.configure(limit, delayMs, cpuThreshold, (unsigned)-1);
        reply.append((unsigned)RFEnoerror);
        return true;
    }

    bool cmdSetThrottle2(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        unsigned throttleClass, limit, delayMs, cpuThreshold, queueLimit;
        msg.read(throttleClass);
        msg.read(limit);
        msg.read(delayMs);
        msg.read(cpuThreshold);
        msg.read(queueLimit);
        setThrottle((ThrottleClass)throttleClass, limit, delayMs, cpuThreshold, queueLimit);
        reply.append((unsigned)RFEnoerror);
        return true;
    }

    void appendError(RemoteFileCommandType cmd, CRemoteClientHandler *client, unsigned ret, MemoryBuffer &reply)
    {
        if (reply.length()>=sizeof(unsigned)*2)
        {
            reply.reset();
            unsigned z;
            unsigned e;
            reply.read(z).read(e);
            StringBuffer err("ERR(");
            err.append(e).append(") ");
            if (client)
            {
                const char *peer = client->queryPeerName();
                if (peer)
                    err.append(peer);
            }
            if (e&&(reply.getPos()<reply.length()))
            {
                StringAttr es;
                reply.read(es);
                err.append(" : ").append(es);
            }
            reply.reset();
            if (cmd!=RFCunlock)
                PROGLOG("%s",err.str());    // supress authentication logging
            if (client)
                client->logPrevHandle();
        }
    }

    void throttleCommand(RemoteFileCommandType cmd, MemoryBuffer &msg, CRemoteClientHandler *client)
    {
        switch(cmd)
        {
            case RFCexec:
            case RFCgetcrc:
            case RFCcopy:
            case RFCappend:
            case RFCtreecopy:
            case RFCtreecopytmp:
                slowCmdThrottler.addCommand(cmd, msg, client);
                return;
            case RFCcloseIO:
            case RFCopenIO:
            case RFCread:
            case RFCsize:
            case RFCwrite:
            case RFCexists:
            case RFCremove:
            case RFCrename:
            case RFCgetver:
            case RFCisfile:
            case RFCisdirectory:
            case RFCisreadonly:
            case RFCsetreadonly:
            case RFCsetfileperms:
            case RFCgettime:
            case RFCsettime:
            case RFCcreatedir:
            case RFCgetdir:
            case RFCmonitordir:
            case RFCstop:
            case RFCextractblobelements:
            case RFCkill:
            case RFCredeploy:
            case RFCmove:
            case RFCsetsize:
            case RFCsettrace:
            case RFCgetinfo:
            case RFCfirewall:
            case RFCunlock:
                stdCmdThrottler.addCommand(cmd, msg, client);
                return;
            // NB: The following commands are still bound by the the thread pool
            case RFCsetthrottle: // legacy version
            case RFCsetthrottle2:
            case RFCcopysection: // slightly odd, but has it's own limit
            default:
            {
                client->processCommand(cmd, msg, NULL);
                break;
            }
        }
    }

    bool processCommand(RemoteFileCommandType cmd, MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler *client, CThrottler *throttler)
    {
        Owned<CClientStats> stats = clientStatsTable.getClientReference(cmd, client->queryPeerName());
        bool ret = true;
        try
        {
            switch(cmd)
            {
                MAPCOMMANDSTATS(RFCread, cmdRead, *stats);
                MAPCOMMANDSTATS(RFCwrite, cmdWrite, *stats);
                MAPCOMMANDCLIENTSTATS(RFCappend, cmdAppend, *client, *stats);
                MAPCOMMAND(RFCcloseIO, cmdCloseFileIO);
                MAPCOMMANDCLIENT(RFCopenIO, cmdOpenFileIO, *client);
                MAPCOMMAND(RFCsize, cmdSize);
                MAPCOMMANDCLIENT(RFCexists, cmdExists, *client);
                MAPCOMMANDCLIENT(RFCremove, cmdRemove, *client);
                MAPCOMMANDCLIENT(RFCrename, cmdRename, *client);
                MAPCOMMAND(RFCgetver, cmdGetVer);
                MAPCOMMANDCLIENT(RFCisfile, cmdIsFile, *client);
                MAPCOMMANDCLIENT(RFCisdirectory, cmdIsDir, *client);
                MAPCOMMANDCLIENT(RFCisreadonly, cmdIsReadOnly, *client);
                MAPCOMMANDCLIENT(RFCsetreadonly, cmdSetReadOnly, *client);
                MAPCOMMANDCLIENT(RFCsetfileperms, cmdSetFilePerms, *client);
                MAPCOMMANDCLIENT(RFCgettime, cmdGetTime, *client);
                MAPCOMMANDCLIENT(RFCsettime, cmdSetTime, *client);
                MAPCOMMANDCLIENT(RFCcreatedir, cmdCreateDir, *client);
                MAPCOMMANDCLIENT(RFCgetdir, cmdGetDir, *client);
                MAPCOMMANDCLIENT(RFCmonitordir, cmdMonitorDir, *client);
                MAPCOMMAND(RFCstop, cmdStop);
                MAPCOMMANDCLIENT(RFCexec, cmdExec, *client);
                MAPCOMMANDCLIENT(RFCextractblobelements, cmdExtractBlobElements, *client);
                MAPCOMMAND(RFCkill, cmdKill);
                MAPCOMMAND(RFCredeploy, cmdRedeploy); // only Windows
                MAPCOMMANDCLIENT(RFCgetcrc, cmdGetCRC, *client);
                MAPCOMMANDCLIENT(RFCmove, cmdMove, *client);
                MAPCOMMANDCLIENT(RFCcopy, cmdCopy, *client);
                MAPCOMMAND(RFCsetsize, cmdSetSize);
                MAPCOMMAND(RFCsettrace, cmdSetTrace);
                MAPCOMMAND(RFCgetinfo, cmdGetInfo);
                MAPCOMMAND(RFCfirewall, cmdFirewall);
                MAPCOMMANDCLIENT(RFCunlock, cmdUnlock, *client);
                MAPCOMMANDCLIENT(RFCcopysection, cmdCopySection, *client);
                MAPCOMMANDCLIENTTHROTTLE(RFCtreecopy, cmdTreeCopy, *client, &slowCmdThrottler);
                MAPCOMMANDCLIENTTHROTTLE(RFCtreecopytmp, cmdTreeCopyTmp, *client, &slowCmdThrottler);
                MAPCOMMAND(RFCsetthrottle, cmdSetThrottle); // legacy version
                MAPCOMMAND(RFCsetthrottle2, cmdSetThrottle2);
            default:
                ret = cmdUnknown(msg,reply,cmd);
                break;
            }
        }
        catch (IException *e)
        {
            ret = false;
            StringBuffer s;
            e->errorMessage(s);
            appendErr3(reply, cmd, e->errorCode(), s.str());
            e->Release();
        }
        if (!ret) // append error string
            appendError(cmd, client, cmd, reply);
        return ret;
    }

    IPooledThread *createCommandProcessor()
    {
        return new cCommandProcessor();
    }

    void run(SocketEndpoint &listenep, bool useSSL)
    {
        if (listenep.isNull())
            acceptsock.setown(ISocket::create(listenep.port));
        else {
            StringBuffer ips;
            listenep.getIpText(ips);
            acceptsock.setown(ISocket::create_ip(listenep.port,ips.str()));
        }
        if (useSSL) {
            if (!securitySettings.certificate)
                throw createDafsException(DAFSERR_connection_failed,"SSL Certificate information not found in environment.conf");
            if (listenep.port <= 0)
            {
                assertex(FALSE);//should never get here
                listenep.port = securitySettings.daFileSrvPort;
            }
            //Create unsecure socket to reject non-ssl client requests
            if (listenep.isNull())
                rejectsock.setown(ISocket::create(DAFILESRV_PORT));
            else
            {
                StringBuffer ips;
                listenep.getIpText(ips);
                rejectsock.setown(ISocket::create_ip(DAFILESRV_PORT,ips.str()));
            }
        }
#ifdef _DEBUG
        StringBuffer sb;
        listenep.getUrlStr(sb);
        DBGLOG("Server accepting %sfrom %s", useSSL?"SECURE ":"", sb.str());
#endif
        selecthandler->start();

        UnsignedArray readSocks;
        if (useSSL)
        {
            readSocks.append(acceptsock->OShandle());
            readSocks.append(rejectsock->OShandle());
        }

        loop {
            Owned<ISocket> sock;
            bool sockavail = false;
            try {
                if (!useSSL)
                    sockavail = acceptsock->wait_read(1000*60*1)!=0;
                else
                {
                    UnsignedArray waitingSocks;
                    //SSL Enabled. Listen for non SSL connection on DAFILESRV_PORT and reject them
                    int numReady = wait_read_multiple(readSocks, 1000*60*1, waitingSocks);
                    if (numReady)
                    {
                        for (int idx = 0; idx < numReady; idx++)
                        {
                            if (waitingSocks.item(idx) == rejectsock->OShandle())
                            {
                                //Unsecure connection attemped, reject !
                                Owned<ISocket> s;
                                s.setown(rejectsock->accept(true));
                                IpAddress ip;
                                StringBuffer sb;
                                s->getPeerAddress(ip);
                                ip.getIpText(sb);
                                DBGLOG("Rejecting nonsecure connect from %s",sb.str());
                                s->close();
                            }
                            else
                            {
                                sockavail = true;
                            }
                        }
                    }
                }
#if 0
                if (!sockavail) {
                    JSocketStatistics stats;
                    getSocketStatistics(stats);
                    StringBuffer s;
                    getSocketStatisticsString(stats,s);
                    PROGLOG( "Socket statistics : \n%s\n",s.str());
                }
#endif
            }
            catch (IException *e) {
                EXCLOG(e,"CRemoteFileServer(1)");
                e->Release();
                // not sure what to do so just accept
                sockavail = true;
            }
            if (stopping)
                break;
            if (sockavail) {
                try {
                    sock.setown(acceptsock->accept(true));
                    if (useSSL)
                    {
#ifdef USE_OPENSSL
                        Owned<ISecureSocket> ssock = createSecureSocket(sock.getClear(), ServerSocket);
                        int status = ssock->secure_accept();
                        if (status < 0)
                            throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection");
                        sock.setown(ssock.getLink());
#else
                        throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
                    }
                    if (!sock||stopping)
                        break;
                    runClient(sock.getClear());
                }
                catch (IException *e) {
                    EXCLOG(e,"CRemoteFileServer");
                    e->Release();
                    sock.clear();
                    if (!QUERYINTERFACE(e, IJSOCK_Exception))
                        break;
                }
            }
            else
                checkTimeout();
        }
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("CRemoteFileServer:run exiting");
        selecthandler->stop(true);
    }

    void processUnauthenticatedCommand(RemoteFileCommandType cmd, ISocket *socket, MemoryBuffer &msg)
    {
        // these are unauthenticated commands
        if (cmd != RFCgetver)
            cmd = RFCinvalid;
        MemoryBuffer reply;
        processCommand(cmd, msg, initSendBuffer(reply), NULL, NULL);
        sendBuffer(socket, reply);
    }

    bool checkAuthentication(ISocket *socket, IAuthenticatedUser *&ret)
    {
        ret = NULL;
        if (!AuthenticationEnabled)
            return true;
        MemoryBuffer reqbuf;
        MemoryBuffer reply;
        MemoryBuffer encbuf; // because aesEncrypt clears input
        initSendBuffer(reply);
        receiveBuffer(socket, reqbuf, 1);
        RemoteFileCommandType typ=0;
        if (reqbuf.remaining()<sizeof(RemoteFileCommandType))
            return false;
        reqbuf.read(typ);
        if (typ!=RFCunlock) {
            processUnauthenticatedCommand(typ,socket,reqbuf);
            return false;
        }
        if (reqbuf.remaining()<sizeof(OnceKey))
            return false;
        OnceKey oncekey;
        reqbuf.read(sizeof(oncekey),&oncekey);
        IpAddress ip;
        socket->getPeerAddress(ip);
        byte ipdata[16];
        size32_t ipds = ip.getNetAddress(sizeof(ipdata),&ipdata);
        mergeOnce(oncekey,sizeof(ipdata),&ipdata); // this is clients key
        OnceKey mykey;
        genOnce(mykey);
        reply.append((unsigned)0); // errcode
        aesEncrypt(&oncekey,sizeof(oncekey),&mykey,sizeof(oncekey),encbuf);
        reply.append(encbuf.length()).append(encbuf);
        sendBuffer(socket, reply); // send my oncekey
        reqbuf.clear();
        receiveBuffer(socket, reqbuf, 1);
        if (reqbuf.remaining()>sizeof(RemoteFileCommandType)+sizeof(size32_t)) {
            reqbuf.read(typ);
            if (typ==RFCunlockreply) {
                size32_t bs;
                reqbuf.read(bs);
                if (bs<=reqbuf.remaining()) {
                    MemoryBuffer userbuf;
                    aesDecrypt(&mykey,sizeof(mykey),reqbuf.readDirect(bs),bs,userbuf);
                    byte n;
                    userbuf.read(n);
                    if (n>=2) {
                        StringAttr user;
                        StringAttr password;
                        userbuf.read(user).read(password);
                        Owned<IAuthenticatedUser> iau = createAuthenticatedUser();
                        if (iau->login(user,password)) {
                            initSendBuffer(reply.clear());
                            reply.append((unsigned)0);
                            sendBuffer(socket, reply); // send OK
                            ret = iau;
                            return true;
                        }
                    }
                }
            }
        }
        reply.clear();
        appendErr(reply, (RemoteFileCommandType)RFSERR_AuthenticateFailed);
        sendBuffer(socket, reply); // send OK
        return false;
    }

    void runClient(ISocket *sock)
    {
        cCommandProcessor::cCommandProcessorParams params;
        IAuthenticatedUser *user=NULL;
        bool authenticated = false;
        try {
            if (checkAuthentication(sock,user))
                authenticated = true;
        }
        catch (IException *e) {
            e->Release();
        }
        if (!authenticated) {
            try {
                sock->Release();
            }
            catch (IException *e) {
                e->Release();
            }
            return;
        }
        params.client = new CRemoteClientHandler(this,sock,user,globallasttick);
        {
            CriticalBlock block(sect);
            clients.append(*LINK(params.client));
        }
        // NB: This could be blocked, by thread pool limit
        threads->start(&params);
    }

    void stop()
    {
        // stop accept loop
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("CRemoteFileServer::stop");
        if (acceptsock)
            acceptsock->cancel_accept();
        if (rejectsock)
            rejectsock->cancel_accept();
        threads->stopAll();
        threads->joinAll(true,60*1000);
    }

    bool notify(CRemoteClientHandler *_client, MemoryBuffer &msg)
    {
        Linked<CRemoteClientHandler> client;
        client.set(_client);
        if (TF_TRACE_FULL)
            PROGLOG("notify %d", msg.length());
        if (msg.length())
        {
            if (TF_TRACE_FULL)
                PROGLOG("notify CRemoteClientHandler(%p), msg length=%u", _client, msg.length());
            cCommandProcessor::cCommandProcessorParams params;
            params.client = client.getClear();
            params.msg.swapWith(msg);

            /* This can block because the thread pool is full and therefore block the selecthandler
             * This is akin to the main server blocking post accept() for the same reason.
             */
            threads->start(&params);
        }
        else
            onCloseSocket(client,3);    // removes owned handles

        return false;
    }

    void addClient(CRemoteClientHandler *client)
    {
        if (client&&client->socket)
            selecthandler->add(client->socket,SELECTMODE_READ,client);
    }

    void checkTimeout()
    {
        if (msTick()-clientcounttick>1000*60*60) {
            CriticalBlock block(ClientCountSect);
            if (TF_TRACE_CLIENT_STATS && (ClientCount || MaxClientCount))
                PROGLOG("Client count = %d, max = %d", ClientCount, MaxClientCount);
            clientcounttick = msTick();
            MaxClientCount = ClientCount;
            if (closedclients) {
                if (TF_TRACE_CLIENT_STATS)
                    PROGLOG("Closed client count = %d",closedclients);
                closedclients = 0;
            }
        }
        CriticalBlock block(sect);
        ForEachItemInRev(i,clients) {
            CRemoteClientHandler &client = clients.item(i);
            if (client.timedOut()) {
                StringBuffer s;
                bool ok = client.getInfo(s);    // will spot duff sockets
                if (ok&&(client.handles.ordinality()!=0))  {
                    if (TF_TRACE_CLIENT_CONN && client.inactiveTimedOut())
                        WARNLOG("Inactive %s",s.str());
                }
                else {
#ifndef _DEBUG
                    if (TF_TRACE_CLIENT_CONN)
#endif
                        PROGLOG("Timing out %s",s.str());
                    closedclients++;
                    onCloseSocket(&client,4);   // removes owned handles
                }
            }
        }
    }

    void getInfo(StringBuffer &info, unsigned level=1)
    {
        {
            CriticalBlock block(ClientCountSect);
            info.append(VERSTRING).append('\n');
            info.appendf("Client count = %d\n",ClientCount);
            info.appendf("Max client count = %d",MaxClientCount);
        }
        CriticalBlock block(sect);
        ForEachItemIn(i,clients)
        {
            info.newline().append(i).append(": ");
            clients.item(i).getInfo(info);
        }
        info.newline().appendf("Running threads: %u", threadRunningCount());
        info.newline();
        stdCmdThrottler.getInfo(info);
        info.newline();
        slowCmdThrottler.getInfo(info);
        clientStatsTable.getInfo(info, level);
    }

    unsigned threadRunningCount()
    {
        if (!threads)
            return 0;
        return threads->runningCount();
    }

    unsigned idleTime()
    {
        unsigned t = (unsigned)atomic_read(&globallasttick);
        return msTick()-t;
    }

    void setThrottle(ThrottleClass throttleClass, unsigned limit, unsigned delayMs, unsigned cpuThreshold, unsigned queueLimit)
    {
        switch (throttleClass)
        {
            case ThrottleStd:
                stdCmdThrottler.configure(limit, delayMs, cpuThreshold, queueLimit);
                break;
            case ThrottleSlow:
                slowCmdThrottler.configure(limit, delayMs, cpuThreshold, queueLimit);
                break;
            default:
            {
                StringBuffer availableClasses("{ ");
                for (unsigned c=0; c<ThrottleClassMax; c++)
                {
                    availableClasses.append(c).append(" = ").append(getThrottleClassText((ThrottleClass)c));
                    if (c+1<ThrottleClassMax)
                        availableClasses.append(", ");
                }
                availableClasses.append(" }");
                throw MakeStringException(0, "Unknown throttle class: %u, available classes are: %s", (unsigned)throttleClass, availableClasses.str());
            }
        }
    }

    StringBuffer &getStats(StringBuffer &stats, bool reset)
    {
        CriticalBlock block(sect);
        stdCmdThrottler.getStats(stats, reset).newline();
        slowCmdThrottler.getStats(stats, reset);
        if (reset)
            clientStatsTable.reset();
        return stats;
    }
};


IRemoteFileServer * createRemoteFileServer(unsigned maxThreads, unsigned maxThreadsDelayMs, unsigned maxAsyncCopy)
{
#if SIMULATE_PACKETLOSS
    errorSimulationOn = false;
#endif
    return new CRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy);
}

