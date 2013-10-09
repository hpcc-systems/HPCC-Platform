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

// todo look at IRemoteFileServer stop


#include "platform.h"
#include "limits.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"

#include "sockfile.hpp"
#include "portlist.h"
#include "jsocket.hpp"
#include "jencrypt.hpp"
#include "jset.hpp"

#include "remoteerr.hpp"

#define SOCKET_CACHE_MAX 500

#define MAX_THREADS             100
#define TARGET_MIN_THREADS      20
#define TARGET_ACTIVE_THREADS   80

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

static const char *VERSTRING= "DS V1.7e - 7 "       // dont forget FILESRV_VERSION in header
#ifdef _WIN32
"Windows ";
#else
"Linux ";
#endif

typedef unsigned char RemoteFileCommandType;
typedef int RemoteFileIOHandle;

static unsigned maxConnectTime = 0;
static unsigned maxReceiveTime = 0;

void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime)
{
    maxConnectTime = maxconnecttime;
    maxReceiveTime = maxreadtime;
}


struct sRFTM        
{
    CTimeMon *timemon;
    sRFTM() {  timemon = maxReceiveTime?new CTimeMon(maxReceiveTime):NULL; }
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
    RFCmax,
    };


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

class CThrottler
{
    Semaphore &sem;
    bool got;
public:
    CThrottler(Semaphore &_sem) : sem(_sem), got(false)
    {
        take();
    }
    ~CThrottler()
    {
        release();
    }
    bool take()
    {
        assertex(!got);
        got = false;
        loop {
            if (sem.wait(5000)) {
                got = true;
                break;
            }
            unsigned cpu = getLatestCPUUsage();
            PROGLOG("Throttler stalled (%d%% cpu)",cpu);
            if (getLatestCPUUsage()<75) 
                break;
        }
        return got;
    }
    bool release()
    {
        if (got)
        {
            got = false;
            sem.signal();
            return true;
        }
        return false;
    }
};

// temporarily release a throttler slot
class CThrottleReleaseBlock
{
    CThrottler &throttler;
    bool had;
public:
    CThrottleReleaseBlock(CThrottler &_throttler) : throttler(_throttler)
    {
        had = throttler.release();
    }
    ~CThrottleReleaseBlock()
    {
        if (had)
            throttler.take();
    }
};

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
        ep.port = DAFILESRV_PORT;
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
    sRFTM tm;
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
        busy.setown(createBitSet());
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

static void treeCopyFile(RemoteFilename &srcfn, RemoteFilename &dstfn, const char *net, const char *mask, IpAddress &ip, bool usetmp, CThrottler *throttler)
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
                                rmtfile->copyTo(dstfile,0x100000,NULL,usetmp);
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
                    CThrottleReleaseBlock block(*throttler);
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
        srcfile->copyTo(dstfile,0x100000,NULL,usetmp);
    }
    catch (IException *e) {
        EXCLOG(e,"TREECOPY(done,fallback)");
        throw;
    }
    if (TF_TRACE_TREE_COPY)
        PROGLOG("TREECOPY(done,fallback) %s to %s",srcfile->queryFilename(),dstfile->queryFilename());
}



//---------------------------------------------------------------------------

class CRemoteBase: public CInterface
{

    Owned<ISocket>          socket;
    static  SocketEndpoint  lastfailep;
    static unsigned         lastfailtime;

    void connectSocket(SocketEndpoint &ep)
    {
        sRFTM tm;
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
                PROGLOG("Connecting to %s",eps.str());
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
            PROGLOG("Retrying connect");
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
        ISocket *s = socket.getClear();
        if (ConnectionTable) {
            SocketEndpoint tep(ep);
            setDafsEndpointPort(tep);
            ConnectionTable->remove(tep,s);
        }
        ::Release(s);
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
        flags = ((unsigned)IFSHread)|((S_IRUSR|S_IWUSR)<<16);
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

    IFileIO * open(IFOmode mode);
    IFileIO * openShared(IFOmode mode,IFSHmode shmode);
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

    void setCreateFlags(unsigned cflags)
    {
        flags |= (cflags<<16);
    }

    void setShareMode(IFSHmode shmode)
    {
        flags &= ~(IFSHfull|IFSHread);
        flags |= (unsigned)(shmode&(IFSHfull|IFSHread));
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

    void copySection(const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress)
    {
        StringBuffer uuid;
        genUUID(uuid,true);
        unsigned timeout = 60*1000; // check every minute
        while(!copySectionAsync(uuid.str(),dest,toOfs,fromOfs,size,progress,timeout));
    }

    void copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp);

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs, memsize_t len, bool write)
    {
        return NULL;
    }

    void treeCopyTo(IFile *dest,IpSubNet &subnet,IpAddress &resfrom,bool usetmp)
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
            copyTo(dest,0x100000,NULL,usetmp);
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
    IFOmode mode;
    compatIFSHmode compatmode;
    bool disconnectonexit;
public:
    IMPLEMENT_IINTERFACE
    CRemoteFileIO(CRemoteFile *_parent)
        : parent(_parent)
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

    bool open(IFOmode _mode,compatIFSHmode _compatmode) 
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char *localname = parent->queryLocalName();
        localname = skipSpecialPath(localname);
        sendBuffer.append((RemoteFileCommandType)RFCopenIO).append(localname).append((byte)_mode).append((byte)_compatmode);
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
        }
        compatmode = _compatmode;
        return true;
    }

    bool reopen() 
    {
        StringBuffer s;
        PROGLOG("Attempting reopen of %s on %s",parent->queryLocalName(),parent->queryEp().getUrlStr(s).str());
        if (open(mode,compatmode)) {
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


    size32_t read(offset_t pos, size32_t len, void * data)
    {
        size32_t got;
        MemoryBuffer replyBuffer;
        const void *b = doRead(pos,len,replyBuffer,got,data);
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
                WARNLOG("Retrying write(%"I64F"d,%d) of %s (%d)",pos,len,parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                    throw exc.getClear();

            }
        }
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



IFileIO * CRemoteFile::openShared(IFOmode mode,IFSHmode shmode)
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
    if (res->open(mode,compatmode))
        return res.getClear();
    return NULL;
}

IFileIO * CRemoteFile::open(IFOmode mode)
{
    return openShared(mode,(IFSHmode)(flags&(IFSHread|IFSHfull)));
}

//---------------------------------------------------------------------------

void CRemoteFile::copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp)
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
            copySection(dest,(offset_t)-1,0,(offset_t)-1,progress);
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
    doCopyFile(dest,this,buffersize,progress,&intercept,usetmp);
}


unsigned getRemoteVersion(ISocket * socket, StringBuffer &ver)
{
    static CriticalSection sect;
    CriticalBlock block(sect);
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
    static CriticalSection sect;
    CriticalBlock block(sect);
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


int getDafsInfo(ISocket * socket,StringBuffer &retstr)
{
    if (!socket) {
        retstr.append(VERSTRING);
        return 0;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetinfo);
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
                parent->threadsem.signal();
                return ret;
            }
        } *thread;
        StringAttr uuid;
    public:
        Semaphore &threadsem;
        CAsyncJob(const char *_uuid,Semaphore &_threadsem)
            : uuid(_uuid),threadsem(_threadsem)
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
        void start()
        {
            threadsem.wait();
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
        CAsyncCopySection(const char *_uuid, const char *fromFile, const char *toFile, offset_t _toOfs, offset_t _fromOfs, offset_t _size, Semaphore &threadsem)
            : CAsyncJob(_uuid,threadsem)
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

public:

    CAsyncCommandManager()
    {
        threadsem.signal(10); // max number of async jobs
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
                job = new CAsyncCopySection(uuid, fromFile, toFile, toOfs, fromOfs, size, threadsem);
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



#define throwErr3(e,v,s) { StringBuffer msg; \
                           msg.appendf("ERROR: %s(%d) '%s'",#e,v,s?s:""); \
                           reply.append(e); reply.append(msg.str()); } 

#define throwErr(e)     { reply.append(e).append(#e); }
#define throwErr2(e,v)      { StringBuffer tmp; tmp.append(#e).append(':').append(v); reply.append(e).append(tmp.str()); }



#define MAPCOMMAND(c,p) case c: { ret =  this->p(msg, reply) ; break; }
#define MAPCOMMANDCLIENT(c,p,client) case c: { ret = this->p(msg, reply, client); break; }
#define MAPCOMMANDCLIENTTHROTTLER(c,p,client,throttler) case c: { ret = this->p(msg, reply, client, throttler); break; }

static unsigned ClientCount = 0;
static unsigned MaxClientCount = 0;
static CriticalSection ClientCountSect;


class CRemoteFileServer : public CInterface, implements IRemoteFileServer, implements IThreadFactory
{
    int                 lasthandle;
    CriticalSection     sect;
    Owned<ISocket>      acceptsock;
    Owned<ISocketSelectHandler> selecthandler;
    Owned<IThreadPool>  threads;    // for commands
    bool stopping;
    unsigned clientcounttick;
    unsigned closedclients;
    CAsyncCommandManager asyncCommandManager;
    Semaphore throttlesem;
    atomic_t globallasttick;

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

    struct CRemoteClientHandler: public CInterface, implements ISocketSelectNotify
    {
        CRemoteFileServer *parent;
        Owned<ISocket> socket;
        Owned<IAuthenticatedUser> user;
        MemoryBuffer buf;
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
            CriticalBlock block(ClientCountSect);
            if (++ClientCount>MaxClientCount) 
                MaxClientCount = ClientCount;
            if (TF_TRACE_CLIENT_CONN) {
                StringBuffer s;
                s.appendf("Connecting(%x) [%d,%d] to ",(unsigned)(long)this,ClientCount,MaxClientCount);
                peerName(s);
                PROGLOG("%s",s.str());
            }
            parent = _parent;
            left = 0;
            buf.setEndian(__BIG_ENDIAN); 
            selecthandled = false;
            touch();
        }
        ~CRemoteClientHandler()
        {   
            {
                CriticalBlock block(ClientCountSect);
                ClientCount--;
                if (TF_TRACE_CLIENT_CONN) {
                    PROGLOG("Disconnecting(%x) [%d,%d] ",(unsigned)(long)this,ClientCount,MaxClientCount);
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
                PROGLOG("notifySelected(%x)",(unsigned)(long)this);
            if (sock!=socket) 
                WARNLOG("notifySelected - invalid socket passed");
            size32_t avail = (size32_t)socket->avail_read();
            if (avail)
                touch();
            if (left==0) {
                try {
                    left = avail?receiveBufferSize(socket):0;
                }
                catch (IException *e) {
                    EXCLOG(e,"notifySelected(1)");
                    e->Release();
                    left = 0;
                }
                if (left) {
                    avail = (size32_t)socket->avail_read();
                    try {
                        buf.ensureCapacity(left);
                    }
                    catch (IException *e) {
                        EXCLOG(e,"notifySelected(2)");
                        e->Release();
                        left = 0;
                        // if too big then corrupted packet so read avail to try and consume
                        char fbuf[1024];
                        while (avail) {
                            size32_t rd = avail>sizeof(fbuf)?sizeof(fbuf):avail;
                            try {
                                socket->read(fbuf, rd); // don't need timeout here
                                avail -= rd;
                            }
                            catch (IException *e) {
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
            if (toread) {
                try {
                    socket->read(buf.reserve(toread), toread);  // don't need timeout here
                }
                catch (IException *e) {
                    EXCLOG(e,"notifySelected(3)");
                    e->Release();
                    toread = left;
                    buf.clear();
                }
            }
            if (TF_TRACE_FULL)
                PROGLOG("notifySelected %d,%d",toread,left);
            if ((left!=0)&&(avail==0)) {
                WARNLOG("notifySelected: Closing mid packet, %d remaining", left);
                toread = left;
                buf.clear();
            }
            left -= toread;
            if (left==0)  {
                // DEBUG
                parent->notify(this);
            }
            return false;
        }

        void logPrevHandle()
        {
            if (previdx<opennames.ordinality()) 
                PROGLOG("Previous handle(%d): %s",handles.item(previdx),opennames.item(previdx).text.get());
        }


        void processCommand()
        {
            CThrottler throttler(parent->throttleSem());
            MemoryBuffer reply;
            RemoteFileCommandType cmd;
            buf.read(cmd);
            parent->dispatchCommand(cmd, buf, initSendBuffer(reply), this, &throttler);
            buf.clear();
            sendBuffer(socket, reply);
        }

        bool immediateCommand() // returns false if socket closed or failure
        {
            try {
                buf.clear();
                touch();
                size32_t avail = (size32_t)socket->avail_read();
                if (avail==0)
                    return false;
                receiveBuffer(socket,buf, 5);   // shouldn't timeout as data is available
                touch();
                if (buf.length()==0)
                    return false;
                processCommand();
            }
            catch (IException *e) {
                EXCLOG(e,"CRemoteClientHandler::immediateCommand");
                e->Release();
                buf.clear();
                return false;
            }
            return true;
        }

        void process()
        {
            if (selecthandled) 
                processCommand(); // buffer already filled
            else {
                while (parent->threadRunningCount()<=TARGET_ACTIVE_THREADS) { // if too many threads add to select handler
                    int w = socket->wait_read(1000);
                    if (w==0)
                        break;
                    if ((w<0)||!immediateCommand()) {
                        if (w<0) 
                            WARNLOG("CRemoteClientHandler::main wait_read error");
                        parent->onCloseSocket(this,1);
                        return;
                    }
                }
                selecthandled = true;
                parent->addClient(this);    // add to select handler
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

        bool peerName(StringBuffer &buf)
        {
            if (socket) {
                char name[256];
                name[0] = 0;
                int port = socket->peer_name(name,sizeof(name)-1);
                if (port>=0) {
                    buf.append(name);
                    if (port)
                        buf.append(':').append(port);
                    return true;
                }
            }
            return false;
        }

        bool getInfo(StringBuffer &str) 
        {
            str.append("client(");
            bool ok = peerName(str);
            unsigned ms = msTick();
            str.appendf("): last touch %d ms ago (%d, %d)",ms-lasttick,lasttick,ms);
            ForEachItemIn(i,handles) {
                str.appendf("\n  %d: ",handles.item(i));
                str.append(opennames.item(i).text);
            }
            return ok;
        }
    
    };

    class cCommandProcessor: public CInterface, implements IPooledThread
    {
        Owned<CRemoteClientHandler> client;

    public:
        IMPLEMENT_IINTERFACE;

        struct cCommandProcessorParams
        {
            CRemoteClientHandler *client; 
        };

        void init(void *_params)
        {
            cCommandProcessorParams &params = *(cCommandProcessorParams *)_params;
            client.setown(params.client);
        }

        void main()
        {
            // idea is that initially we process commands inline then pass over to select handler
            try {
                client->process();
            }
            catch (IException *e) {
                // suppress some errors
                EXCLOG(e,"cCommandProcessor::main");
                e->Release();
            }
            try {
                client.clear();
            }
            catch (IException *e) {
                // suppress some more errors clearing client
                EXCLOG(e,"cCommandProcessor::main(2)");
            }
        }
        bool stop()
        {
            return true;
        }
        bool canReuse()
        {
            return false; // want to free owned osocke
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

    CRemoteFileServer()
    {
        throttlesem.signal(10);
        lasthandle = 0;
        selecthandler.setown(createSocketSelectHandler(NULL));
        threads.setown(createThreadPool("CRemoteFileServerPool",this,NULL,MAX_THREADS,60*1000,
#ifdef __64BIT__
            0,
#else
            0x10000,
#endif

        INFINITE,TARGET_MIN_THREADS));
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
            throwErr(RFSERR_NullFileIOHandle);
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
        throwErr(RFSERR_InvalidFileIOHandle);
        return false;
    }

    void onCloseSocket(CRemoteClientHandler *client, int which) 
    {
        if (!client)
            return;
        CriticalBlock block(sect);
#ifdef _DEBUG
        StringBuffer s;
        client->peerName(s);
        PROGLOG("onCloseSocket(%d) %s",which,s.str());
#endif
        if (client->socket) {
            try {
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
        try {
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
            if (TF_TRACE_PRE_IO)
                PROGLOG("before open file '%s',  (%d,%d)",name->text.get(),(int)mode,(int)share);
            IFileIO *fileio = file->open((IFOmode)mode);
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
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_OpenFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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



    bool cmdRead(MemoryBuffer & msg, MemoryBuffer & reply)
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
            throwErr3(RFSERR_ReadFailed,e->errorCode(),s.str());
            e->Release();
            return false;
        }
        if (TF_TRACE)
            PROGLOG("read file,  handle = %d, pos = %"I64F"d, toread = %d, read = %d",handle,pos,len,numRead);
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
            PROGLOG("size file,  handle = %d, size = %"I64F"d",handle,size);
        return true;
    }

    bool cmdSetSize(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        offset_t size;
        msg.read(handle).read(size);
        IFileIO *fileio;
        if (TF_TRACE)
            PROGLOG("set size file,  handle = %d, size = %"I64F"d",handle,size);
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;
        fileio->setSize(size);
        reply.append((unsigned)RFEnoerror);
        return true;
    }


    bool cmdWrite(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        int handle;
        __int64 pos;
        size32_t len;
        msg.read(handle).read(pos).read(len);
        IFileIO *fileio;
        if (!checkFileIOHandle(reply, handle, fileio))
            return false;

        const byte *data = (const byte *)msg.readDirect(len);
        try {
            if (TF_TRACE_PRE_IO)
                PROGLOG("before write file,  handle = %d, towrite = %d",handle,len);
            size32_t numWritten = fileio->write(pos,len,data);
            if (TF_TRACE)
                PROGLOG("write file,  handle = %d, towrite = %d, written = %d",handle,len,numWritten);
            reply.append((unsigned)RFEnoerror).append(numWritten);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_WriteFailed,e->errorCode(),s.str());
            e->Release();
        }

        return false;
    }

    bool cmdExists(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("exists,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        try {
            bool e = file->exists();
            reply.append((unsigned)RFEnoerror).append(e);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_ExistsFailed,e->errorCode(),s.str());
            e->Release();
        }

        return false;
    }


    bool cmdRemove(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("remove,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        try {
            bool e = file->remove();
            reply.append((unsigned)RFEnoerror).append(e);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_RemoveFailed,e->errorCode(),s.str());
            e->Release();
        }

        return false;
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
        try {
            file->rename(toname);
            reply.append((unsigned)RFEnoerror);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_RenameFailed,e->errorCode(),s.str());
            e->Release();
        }

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
        try {
            file->move(toname);
            reply.append((unsigned)RFEnoerror);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_MoveFailed,e->errorCode(),s.str());
            e->Release();
        }

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
        try {
            copyFile(toname, fromname);
            reply.append((unsigned)RFEnoerror);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_CopyFailed,e->errorCode(),s.str());
            e->Release();
        }

        return false;
        
    }

    bool cmdAppend(MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler &client)
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

        try {
            Owned<IFile> file = createIFile(srcname.get());
            __int64 written = fileio->appendFile(file,pos,len);
            if (TF_TRACE)
                PROGLOG("append file,  handle = %d, file=%s, pos = %"I64F"d len = %"I64F"d written = %"I64F"d",handle,srcname.get(),pos,len,written);
            reply.append((unsigned)RFEnoerror).append(written);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_AppendFailed,e->errorCode(),s.str());
            e->Release();
        }

        return false;
        
    }

    bool cmdIsFile(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isFile,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        try {
            unsigned ret = (unsigned)file->isFile();
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_IsFileFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
    }

    bool cmdIsDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isDir,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        try {
            unsigned ret = (unsigned)file->isDirectory();
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_IsDirectoryFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
    }

    bool cmdIsReadOnly(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("isReadOnly,  '%s'",name.get());
        Owned<IFile> file=createIFile(name);
        try {
            unsigned ret = (unsigned)file->isReadOnly();
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_IsReadOnlyFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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
        try {
            file->setReadOnly(set);
            reply.append((unsigned)RFEnoerror);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_SetReadOnlyFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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
        try {
            bool ret = file->getTime(&createTime,&modifiedTime,&accessedTime);
            reply.append((unsigned)RFEnoerror).append(ret);
            if (ret) {
                createTime.serialize(reply);
                modifiedTime.serialize(reply);
                accessedTime.serialize(reply);
            }
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_GetTimeFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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

        try {
            bool ret = file->setTime(creategot?&createTime:NULL,modifiedgot?&modifiedTime:NULL,accessedgot?&accessedTime:NULL);
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_SetTimeFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
    }

    bool cmdCreateDir(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        StringAttr name;
        msg.read(name);
        if (TF_TRACE)
            PROGLOG("CreateDir,  '%s'",name.get());
        Owned<IFile> dir=createIFile(name);

        try {
            bool ret = dir->createDirectory();
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_CreateDirFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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
        try {
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
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_GetDirFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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
        try {
            Owned<IFile> dir=createIFile(name);
            Owned<IDirectoryDifferenceIterator> iter=dir->monitorDirectory(prev,mask.length()?mask.get():NULL,sub,includedir,checkinterval,timeout);
            reply.append((unsigned)RFEnoerror);
            byte state = (iter.get()==NULL)?0:1;
            reply.append(state);
            if (state==1)
                CRemoteDirectoryIterator::serializeDiff(reply,iter);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_GetDirFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
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
        try {
            AsyncCommandStatus status = asyncCommandManager.copySection(uuid,fromFile,toFile,toOfs,fromOfs,size,sizeDone,totalSize,timeout);
            reply.append((unsigned)RFEnoerror).append((unsigned)status).append(sizeDone).append(totalSize);
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_CopySectionFailed,e->errorCode(),s.str());
            e->Release();
        }
        return true;
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
        try {
            IpAddress ip;
            treeCopyFile(src,dst,net,mask,ip,usetmp,throttler);
            unsigned status = 0;
            reply.append((unsigned)RFEnoerror).append((unsigned)status);
            ip.ipserialize(reply);
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_TreeCopyFailed,e->errorCode(),s.str());
            e->Release();
        }
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
        try {
            unsigned ret = file->getCRC();
            reply.append((unsigned)RFEnoerror).append(ret);
            return true;
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_GetCrcFailed,e->errorCode(),s.str());
            e->Release();
        }
        return false;
    }

    bool cmdStop(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        PROGLOG("Abort request received");
        stopping = true;
        if (acceptsock) 
            acceptsock->cancel_accept();
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
        StringBuffer retstr;
        int retcode = getInfo(retstr);
        reply.append(retcode).append(retstr.str());
        return true;
    }

    bool cmdFirewall(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        // TBD
        StringBuffer retstr;
        int retcode = getInfo(retstr);
        reply.append(retcode).append(retstr.str());
        return true;
    }

    bool cmdExtractBlobElements(MemoryBuffer &msg, MemoryBuffer &reply, CRemoteClientHandler &client)
    {
        IMPERSONATE_USER(client);
        try {
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
        }
        catch (IException *e) {
            StringBuffer s;
            e->errorMessage(s);
            throwErr3(RFSERR_ExtractBlobElementsFailed,e->errorCode(),s.str());
            e->Release();
        }
        return true;
    }

    bool cmdRedeploy(MemoryBuffer &msg, MemoryBuffer &reply)
    {
        return false; // TBD
    }

    bool cmdKill(MemoryBuffer & msg, MemoryBuffer & reply)
    {
        // TBD
        throwErr2(RFSERR_InvalidCommand,(unsigned)RFCkill);
        return false;
    }

    bool cmdUnknown(MemoryBuffer & msg, MemoryBuffer & reply,RemoteFileCommandType cmd)
    {
        throwErr2(RFSERR_InvalidCommand,(unsigned)cmd);
        return false;
    }

    bool cmdUnlock(MemoryBuffer & msg, MemoryBuffer & reply,CRemoteClientHandler &client)
    {
        // this is an attempt to authenticate when we haven't got authentication turned on
        StringBuffer s;
        client.peerName(s);
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("Connect from %s",s.str());
        throwErr2(RFSERR_InvalidCommand,(unsigned)RFCunlock);
        return false;
    }

    bool dispatchCommand(RemoteFileCommandType cmd, MemoryBuffer & msg, MemoryBuffer & reply, CRemoteClientHandler *client, CThrottler *throttler)
    {
        bool ret = true;
        switch(cmd) {
            MAPCOMMAND(RFCcloseIO, cmdCloseFileIO);
            MAPCOMMANDCLIENT(RFCopenIO, cmdOpenFileIO, *client);
            MAPCOMMAND(RFCread, cmdRead);
            MAPCOMMAND(RFCsize, cmdSize);
            MAPCOMMAND(RFCwrite, cmdWrite);
            MAPCOMMANDCLIENT(RFCexists, cmdExists, *client);
            MAPCOMMANDCLIENT(RFCremove, cmdRemove, *client);
            MAPCOMMANDCLIENT(RFCrename, cmdRename, *client);
            MAPCOMMAND(RFCgetver, cmdGetVer);
            MAPCOMMANDCLIENT(RFCisfile, cmdIsFile, *client);
            MAPCOMMANDCLIENT(RFCisdirectory, cmdIsDir, *client);
            MAPCOMMANDCLIENT(RFCisreadonly, cmdIsReadOnly, *client);
            MAPCOMMANDCLIENT(RFCsetreadonly, cmdSetReadOnly, *client);
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
            MAPCOMMANDCLIENT(RFCappend, cmdAppend, *client);
            MAPCOMMAND(RFCsetsize, cmdSetSize);
            MAPCOMMAND(RFCsettrace, cmdSetTrace);
            MAPCOMMAND(RFCgetinfo, cmdGetInfo);
            MAPCOMMAND(RFCfirewall, cmdFirewall);
            MAPCOMMANDCLIENT(RFCunlock, cmdUnlock, *client);
            MAPCOMMANDCLIENT(RFCcopysection, cmdCopySection, *client);
            MAPCOMMANDCLIENTTHROTTLER(RFCtreecopy, cmdTreeCopy, *client, throttler);
            MAPCOMMANDCLIENTTHROTTLER(RFCtreecopytmp, cmdTreeCopyTmp, *client, throttler);

        default:
            ret = cmdUnknown(msg,reply,cmd);
        }
        if (!ret) { // append error string
            if (reply.length()>=sizeof(unsigned)*2) {
                reply.reset();
                unsigned z;
                unsigned e;
                reply.read(z).read(e);
                StringBuffer err("ERR(");
                err.append(e).append(") ");
                if (client&&(client->peerName(err)))
                    err.append(" : ");
                if (e&&(reply.getPos()<reply.length())) {
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
        return ret;
    }


    virtual IPooledThread *createNew()
    {
        return new cCommandProcessor();
    }


    void run(SocketEndpoint &listenep)
    {
        if (listenep.isNull())
            acceptsock.setown(ISocket::create(listenep.port));

        else {
            StringBuffer ips;
            listenep.getIpText(ips);
            acceptsock.setown(ISocket::create_ip(listenep.port,ips.str()));
        }
        selecthandler->start();
        loop {
            Owned<ISocket> sock;
            bool sockavail;
            try {
                sockavail = acceptsock->wait_read(1000*60*1)!=0;
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
                    if (!sock||stopping)
                        break;
                }
                catch (IException *e) {
                    EXCLOG(e,"CRemoteFileServer");
                    e->Release();
                    break;
                }
                runClient(sock.getClear());
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
        dispatchCommand(cmd, msg, initSendBuffer(reply), NULL, NULL);
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
        throwErr(RFSERR_AuthenticateFailed);
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
            params.client->Link();
            clients.append(*params.client);
        }
        threads->start(&params);
    }

    void stop()
    {
        // stop accept loop
        if (TF_TRACE_CLIENT_STATS)
            PROGLOG("CRemoteFileServer::stop");
        if (acceptsock) 
            acceptsock->cancel_accept();
        threads->stopAll();
        threads->joinAll(true,60*1000);
    }

    bool notify(CRemoteClientHandler *_client)
    {
        Linked<CRemoteClientHandler> client;
        client.set(_client);
        if (TF_TRACE_FULL)
            PROGLOG("notify %d",client->buf.length());
        if (client->buf.length()) {
            cCommandProcessor::cCommandProcessorParams params;
            params.client = client.getClear();
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

    int getInfo(StringBuffer &str)
    {
        {
            CriticalBlock block(ClientCountSect);
            str.append(VERSTRING).append('\n');
            str.appendf("Client count = %d\n",ClientCount);
            str.appendf("Max client count = %d",MaxClientCount);
        }
        CriticalBlock block(sect);
        ForEachItemIn(i,clients) {
            str.append('\n').append(i).append(": ");
            clients.item(i).getInfo(str);
        }
        return 0;
    }

    unsigned threadRunningCount()
    {
        if (!threads)
            return 0;
        return threads->runningCount();
    }

    Semaphore &throttleSem()
    {
        return throttlesem;
    }

    unsigned idleTime()
    {
        unsigned t = (unsigned)atomic_read(&globallasttick);
        return msTick()-t;
    }

};



IRemoteFileServer * createRemoteFileServer()
{
#if SIMULATE_PACKETLOSS
    errorSimulationOn = false;
#endif
    return new CRemoteFileServer();
}

