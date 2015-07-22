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

#define mp_decl __declspec(dllexport)

/* TBD
    lost packet disposal
    synchronous send
    connection protocol (HRPC);
    look at all timeouts
*/

#include "platform.h"
#include "portlist.h"
#include "jlib.hpp"
#include <limits.h>

#include "jsocket.hpp"
#include "jmutex.hpp"
#include "jutil.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"

#include "mpcomm.hpp"
#include "mpbuff.hpp"
#include "mputil.hpp"
#include "mplog.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

//#define _TRACE
//#define _FULLTRACE
#define _TRACEORPHANS


#define REFUSE_STALE_CONNECTION


#define MP_PROTOCOL_VERSION    0x102                   
#define MP_PROTOCOL_VERSIONV6   0x202                   // extended for IPV6

// These should really be configurable
#define CANCELTIMEOUT       1000             // 1 sec
#define CONNECT_TIMEOUT          (5*60*1000) // 5 mins
#define CONNECT_READ_TIMEOUT     (10*1000)   // 10 seconds. NB: used by connect readtms loop (see loopCnt)
#define CONNECT_TIMEOUT_INTERVAL 1000        // 1 sec
#define CONNECT_RETRYCOUNT       180         // Overall max connect time is = CONNECT_RETRYCOUNT * CONNECT_READ_TIMEOUT
#define CONNECT_TIMEOUT_MINSLEEP 2000        // random range: CONNECT_TIMEOUT_MINSLEEP to CONNECT_TIMEOUT_MAXSLEEP milliseconds
#define CONNECT_TIMEOUT_MAXSLEEP 5000

#define CONFIRM_TIMEOUT          (90*1000) // 1.5 mins
#define CONFIRM_TIMEOUT_INTERVAL 5000 // 5 secs
#define TRACESLOW_THRESHOLD      1000 // 1 sec

#define VERIFY_DELAY            (1*60*1000)  // 1 Minute
#define VERIFY_TIMEOUT          (1*60*1000)  // 1 Minute

#define DIGIT1 U64C(0x10000000000) // (256ULL*256ULL*256ULL*65536ULL)
#define DIGIT2 U64C(0x100000000)   // (256ULL*256ULL*65536ULL)
#define DIGIT3 U64C(0x1000000)     // (256ULL*65536ULL)
#define DIGIT4 U64C(0x10000)       // (65536ULL)

#define _TRACING

static  CriticalSection verifysect;
static  CriticalSection childprocesssect;
static  UnsignedArray childprocesslist;

// IPv6 TBD

struct SocketEndpointV4
{
    byte ip[4];
    unsigned short port;
    SocketEndpointV4() {};
    SocketEndpointV4(const SocketEndpoint &val) { set(val); }
    void set(const SocketEndpoint &val)
    {
        port = val.port;
        if (val.getNetAddress(sizeof(ip),&ip)!=sizeof(ip))
            IPV6_NOT_IMPLEMENTED();
    }
    void get(SocketEndpoint &val)   
    {
        val.setNetAddress(sizeof(ip),&ip);
        val.port = port;
    }
};

class PacketHeader // standard packet header - no virtuals 
{
public:
    static unsigned nextseq;
    static unsigned lasttick;

    void initseq()
    {
        sequence = msTick();
        lasttick = sequence;
        if (sequence-nextseq>USHRT_MAX)
            sequence = nextseq++;
        else
            nextseq = sequence+1;
    }

    PacketHeader(size32_t _size, SocketEndpoint &_sender, SocketEndpoint &_target, mptag_t _tag, mptag_t _replytag)
    {
        size = _size;
        tag = _tag;
        sender.set(_sender);
        target.set(_target);
        replytag = _replytag;
        flags = 0;
        version = MP_PROTOCOL_VERSION;
        initseq();
    }
    PacketHeader() {}
    size32_t        size;                                                   // 0    total packet size
    mptag_t         tag;                                                    // 4    packet tag (kind)
    unsigned short  version;                                                // 8   protocol version
    unsigned short  flags;                                                  // 10   flags
    SocketEndpointV4  sender;                                               // 12   who sent
    SocketEndpointV4  target;                                               // 18   who destined for
    mptag_t         replytag;                                               // 24   used for reply
    unsigned        sequence;                                               // 28   packet type dependant
                                                                            // Total 32

    void setMessageFields(CMessageBuffer &mb)
    {
        SocketEndpoint ep;
        sender.get(ep);
        mb.init(ep,tag,replytag);
    }
};

class PacketHeaderV6 : public PacketHeader
{
    unsigned senderex[4];                                                   // 32
    unsigned targetex[4];                                                   // 48
                                                                            // total 64
    void setMessageFields(CMessageBuffer &mb)
    {
        SocketEndpoint ep;
        ep.setNetAddress(sizeof(senderex),&senderex);
        ep.port = sender.port;
        mb.init(ep,tag,replytag);
    }

};
    

unsigned PacketHeader::nextseq=0;
unsigned PacketHeader::lasttick=0;


#define MINIMUMPACKETSIZE       sizeof(PacketHeader) 
#define MAXDATAPERPACKET        50000

struct MultiPacketHeader
{
    mptag_t tag;
    size32_t ofs;
    size32_t size;
    unsigned idx;
    unsigned numparts;
    size32_t total;
    StringBuffer &getDetails(StringBuffer &out) const
    {
        out.append("MultiPacketHeader: ");
        out.append("tag=").append((unsigned)tag);
        out.append(",ofs=").append(ofs);
        out.append(",size=").append(size);
        out.append(",idx=").append(idx);
        out.append(",numparts=").append(numparts);
        out.append(",total=").append(total);
        return out;
    }
};


// 

class CMPException: public CInterface, public IMP_Exception
{
public:
    IMPLEMENT_IINTERFACE;

    CMPException(MessagePassingError err,const SocketEndpoint &ep) : error(err), endpoint(ep) 
    {
    }

    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        StringBuffer tmp;
        switch (error) {
        case MPERR_ok:                          str.append("OK"); break;
        case MPERR_connection_failed:           str.appendf("MP connect failed (%s)",endpoint.getUrlStr(tmp).str()); break;
        case MPERR_process_not_in_group:        str.appendf("Current process not in Communicator group"); break;
        case MPERR_protocol_version_mismatch:   str.appendf("Protocol version mismatch (%s)",endpoint.getUrlStr(tmp).str()); break;
        case MPERR_link_closed:                 str.appendf("MP link closed (%s)",endpoint.getUrlStr(tmp).str()); break;
        }
        return str;
    }
    int             errorCode() const { return error; }
    MessageAudience errorAudience() const 
    { 
        return MSGAUD_user; 
    }
    virtual const SocketEndpoint &queryEndpoint() const { return endpoint; }
private:
    MessagePassingError error;
    SocketEndpoint endpoint;
};




class CBufferQueueNotify
{
public:
    virtual bool notify(CMessageBuffer *)=0;
    virtual bool notifyClosed(SocketEndpoint &closedep)=0; // called when connection closed
};


class CBufferQueueWaiting
{
public:
    enum QWenum { QWcontinue, QWdequeue, QWprobe };
    Semaphore sem;
    CBufferQueueNotify &waiting;
    bool probe;
    CBufferQueueWaiting(CBufferQueueNotify& _waiting,bool _probe) : waiting(_waiting) { probe = _probe; }
    QWenum notify(CMessageBuffer *b)
    {
        // check this for DLL unloaded TBD
        if (waiting.notify(b)) {
            sem.signal();
            return probe?QWprobe:QWdequeue;
        }
        return QWcontinue;
    }
    QWenum notifyClosed(SocketEndpoint &ep)
    {
        // check this for DLL unloaded TBD
        if (waiting.notifyClosed(ep)) {
            sem.signal();
            return QWdequeue;
        }
        return QWcontinue;
    }

};

typedef CopyReferenceArrayOf<CBufferQueueWaiting> CWaitingArray;

class CBufferQueue
{
    QueueOf<CMessageBuffer, false> received;
    CWaitingArray waiting;
    CriticalSection sect;

public:

    CBufferQueue()
    {
    }

    void enqueue(CMessageBuffer *b)
    {
        CriticalBlock block(sect);
        unsigned iter=0;
        loop {
            ForEachItemIn(i,waiting) {
                CBufferQueueWaiting::QWenum r = waiting.item(i).notify(b);
                if (r!=CBufferQueueWaiting::QWcontinue) {
                    waiting.remove(i);
                    if (r==CBufferQueueWaiting::QWdequeue)
                        return;
                    //CBufferQueueWaiting::QWprobe
                    break;
                }
            }
            if (b->getReplyTag() != TAG_CANCEL) 
                break;
            if (iter++==10) {
                delete b;
                return;
            }
            CriticalUnblock unblock(sect);
            Sleep(CANCELTIMEOUT/10);            // to avoid race conditions (cancel eventually times out)
        }
        received.enqueue(b);
    }

    bool wait(CBufferQueueNotify &nfy,bool probe,CTimeMon &tm)
    {
        CriticalBlock block(sect);
        bool probegot = false;
        ForEachQueueItemIn(i,received) {
            if (nfy.notify(received.item(i))) {
                if (probe) {
                    probegot = true;
                }
                else {
                    received.dequeue(i);
                    return true;
                }
            }
        }
        if (probegot)
            return true;
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        CBufferQueueWaiting qwaiting(nfy,probe);
        waiting.append(qwaiting);
        sect.leave();
        bool ok = qwaiting.sem.wait(remaining);
        sect.enter();
        if (!ok) {
            ok = qwaiting.sem.wait(0);
            if (!ok)
                waiting.zap(qwaiting);
        }
        return ok;
    }

    unsigned flush(CBufferQueueNotify &nfy)
    {
        unsigned count = 0;
        CriticalBlock block(sect);
        ForEachQueueItemInRev(i,received) {
            if (nfy.notify(received.item(i))) {
                count++;
                delete received.dequeue(i);
            }
        }
        return count;
    }

    void notifyClosed(SocketEndpoint &ep)
    {
        CriticalBlock block(sect);
        ForEachItemInRev(i,waiting) {
            CBufferQueueWaiting::QWenum r = waiting.item(i).notifyClosed(ep);
            if (r!=CBufferQueueWaiting::QWcontinue) {
                waiting.remove(i);
            }
        }
    }

    StringBuffer &getReceiveQueueDetails(StringBuffer &buf)
    {
        CriticalBlock block(sect);
        ForEachQueueItemIn(i,received) {
            received.item(i)->getDetails(buf).append('\n');
        }
        return buf;
    }
};

static UnsignedShortArray freetags;
static unsigned nextfreetag=0;

unsigned short generateDynamicTag()
{
    if (freetags.ordinality())
        return freetags.popGet();
    return nextfreetag++;
}

void releaseDynamicTag(unsigned short tag)
{
    freetags.append(tag);
}



class CMPServer;
class CMPChannel;


class CMPConnectThread: public Thread
{
    bool running;
    ISocket *listensock;
    CMPServer *parent;
    void checkSelfDestruct(void *p,size32_t sz);
public:
    CMPConnectThread(CMPServer *_parent, unsigned port);
    ~CMPConnectThread()
    {
        ::Release(listensock);
    }
    int run();
    void start(unsigned short port);
    void stop()
    {
        if (running) {
            running = false;
            listensock->cancel_accept();
            if (!join(1000*60*5))   // should be pretty instant
                printf("CMPConnectThread::stop timed out\n");
        }
    }
};

class PingPacketHandler;
class PingReplyPacketHandler;
class MultiPacketHandler;
class BroadcastPacketHandler;
class ForwardPacketHandler;
class UserPacketHandler;
class CMPNotifyClosedThread;

static class CMPServer: private SuperHashTableOf<CMPChannel,SocketEndpoint>
{
    unsigned short              port;
    ISocketSelectHandler        *selecthandler;
    CMPConnectThread            *connectthread;
    CBufferQueue                receiveq;
    CMPNotifyClosedThread       *notifyclosedthread;
public:
    static CriticalSection  serversect;
    static int                      servernest;
    static bool                     serverpaused;
    bool checkclosed;

// packet handlers
    PingPacketHandler           *pingpackethandler;         // TAG_SYS_PING
    PingReplyPacketHandler      *pingreplypackethandler;    // TAG_SYS_PING_REPLY
    ForwardPacketHandler        *forwardpackethandler;      // TAG_SYS_FORWARD
    MultiPacketHandler          *multipackethandler;        // TAG_SYS_MULTI
    BroadcastPacketHandler      *broadcastpackethandler;    // TAG_SYS_BCAST
    UserPacketHandler           *userpackethandler;         // default


    CMPServer(unsigned _port);
    ~CMPServer();
    void start();
    void stop();
    unsigned short getPort() { return port; }
    void setPort(unsigned short _port) { port = _port; }
    CMPChannel &lookup(const SocketEndpoint &remoteep);
    ISocketSelectHandler &querySelectHandler() { return *selecthandler; };
    CBufferQueue &getReceiveQ() { return receiveq; }
    bool recv(CMessageBuffer &mbuf, const SocketEndpoint *ep, mptag_t tag, CTimeMon &tm);
    void flush(mptag_t tag);
    unsigned probe(const SocketEndpoint *ep, mptag_t tag, CTimeMon &tm, SocketEndpoint &sender);
    void cancel(const SocketEndpoint *ep, mptag_t tag);
    bool nextChannel(CMPChannel *&c);
    void addConnectionMonitor(IConnectionMonitor *monitor);
    void removeConnectionMonitor(IConnectionMonitor *monitor);
    void notifyClosed(SocketEndpoint &ep);
    StringBuffer &getReceiveQueueDetails(StringBuffer &buf) 
    {
        return receiveq.getReceiveQueueDetails(buf);
    }   
    void removeChannel(CMPChannel *c) { if (c) removeExact(c); }
protected:
    void onAdd(void *);
    void onRemove(void *e);
    unsigned getHashFromElement(const void *e) const;
    unsigned getHashFromFindParam(const void *fp) const;
    const void * getFindParam(const void *p) const;
    bool matchesFindParam(const void * et, const void *fp, unsigned fphash) const;

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CMPChannel,SocketEndpoint);


} *MPserver=NULL;
int CMPServer::servernest=0;
bool CMPServer::serverpaused=false;
CriticalSection CMPServer::serversect;

byte RTsalt=0xff;

mptag_t createReplyTag()
{
    // these are short-lived so a simple increment will do (I think this is OK!)
    mptag_t ret;
    {
        static CriticalSection sect;
        CriticalBlock block(sect);
        static int rettag=(int)TAG_REPLY_BASE;  // NB negative
        if (RTsalt==0xff) {
            RTsalt = (byte)(getRandom()%16);
            rettag = (int)TAG_REPLY_BASE-RTsalt;
        }
        if (rettag>(int)TAG_REPLY_BASE) {           // wrapped
            rettag = (int)TAG_REPLY_BASE-RTsalt;
        }
        ret = (mptag_t)rettag;
        rettag -= 16;
    }
    if (MPserver)
        MPserver->flush(ret);
    return ret;
}

void checkTagOK(mptag_t tag)
{
    if ((int)tag<=(int)TAG_REPLY_BASE) {
        int dif = (int)TAG_REPLY_BASE-(int)tag;
        if (dif%16!=RTsalt) {
            ERRLOG("**Invalid MP tag used");
            PrintStackReport();
        }
    }
}

//===========================================================================

class CMPNotifyClosedThread: public Thread
{
    IArrayOf<IConnectionMonitor> connectionmonitors;
    CriticalSection conmonsect;
    SimpleInterThreadQueueOf<INode, false> workq;
    bool stopping;  
    CMPServer *parent;
    CriticalSection stopsect;
public:
    CMPNotifyClosedThread(CMPServer *_parent)
        : Thread("CMPNotifyClosedThread")
    {
        parent = _parent;
        stopping = false;
    }

    ~CMPNotifyClosedThread()
    {
        IArrayOf<IConnectionMonitor> todelete;
        CriticalBlock block(conmonsect);
        while (connectionmonitors.ordinality())
            todelete.append(connectionmonitors.popGet());
    }

    void addConnectionMonitor(IConnectionMonitor *monitor)
    {
        if (monitor)
            connectionmonitors.append(*LINK(monitor));
    }
    void removeConnectionMonitor(IConnectionMonitor *monitor)
    {
        // called in critical section CMPServer::sect
        if (monitor) {
            CriticalBlock block(conmonsect);
            connectionmonitors.zap(*monitor);
        }
    }
    int run()
    {
        loop {
            try {
                Owned<INode> node = workq.dequeue();
                if (node->endpoint().isNull())
                    break;
                SocketEndpoint ep = node->endpoint();
                parent->getReceiveQ().notifyClosed(ep); 
                IArrayOf<IConnectionMonitor> toclose;
                {
                    CriticalBlock block(conmonsect);
                    ForEachItemIn(i1,connectionmonitors) {
                        toclose.append(*LINK(&connectionmonitors.item(i1)));
                    }
                }
                ForEachItemIn(i,toclose) {
                    toclose.item(i).onClose(ep);
                }
            }
            catch (IException *e) {
                FLLOG(MCoperatorWarning, unknownJob, e,"MP writepacket");
                e->Release();
            }
        }
        return 0;
    }
    void stop()
    {
        {
            CriticalBlock block(stopsect);
            if (!stopping) {
                stopping = true;
                SocketEndpoint ep;
                workq.enqueue(createINode(ep));
            }
        }
        while (!join(1000*60*3))
            PROGLOG("CMPNotifyClosedThread join failed");
    }
    void notify(SocketEndpoint &ep)
    {
        CriticalBlock block(stopsect);
        if (!stopping&&!ep.isNull()) {
            if (workq.ordinality()>100)
                PROGLOG("MP: %d waiting to close",workq.ordinality());
            workq.enqueue(createINode(ep));
        }
    }
};


void traceSlowReadTms(const char *msg, ISocket *sock, void *dst, size32_t minSize, size32_t maxSize, size32_t &sizeRead, unsigned timeoutMs, unsigned timeoutChkIntervalMs)
{
    dbgassertex(timeoutChkIntervalMs < timeoutMs);
    StringBuffer epStr;
    CCycleTimer readTmsTimer;
    unsigned intervalTimeoutMs = timeoutChkIntervalMs;
    loop
    {
        try
        {
            sock->readtms(dst, minSize, maxSize, sizeRead, intervalTimeoutMs);
            break;
        }
        catch (IJSOCK_Exception *e)
        {
            if (JSOCKERR_timeout_expired != e->errorCode())
                throw;
            unsigned elapsedMs = readTmsTimer.elapsedMs();
            if (elapsedMs >= timeoutMs)
                throw;
            unsigned remainingMs = timeoutMs-elapsedMs;
            if (remainingMs < timeoutChkIntervalMs)
                intervalTimeoutMs = remainingMs;
            if (0 == epStr.length())
            {
                SocketEndpoint ep;
                sock->getPeerEndpoint(ep);
                ep.getUrlStr(epStr);
            }
            WARNLOG("%s %s, stalled for %d ms so far", msg, epStr.str(), elapsedMs);
        }
    }
    if (readTmsTimer.elapsedMs() >= TRACESLOW_THRESHOLD)
    {
        if (0 == epStr.length())
        {
            SocketEndpoint ep;
            sock->getPeerEndpoint(ep);
            ep.getUrlStr(epStr);
        }
        WARNLOG("%s %s, took: %d ms", msg, epStr.str(), readTmsTimer.elapsedMs());
    }
}

class CMPPacketReader;

class CMPChannel: public CInterface
{
    ISocket *channelsock;
    CMPServer *parent;
    Mutex sendmutex;
    Semaphore sendwaitingsig;
    unsigned sendwaiting;               // number waiting on sendwaitingsem (for multi/single clashes to resolve)
    CriticalSection connectsect;
    CMPPacketReader *reader;
    bool master;                        // i.e. connected originally
    mptag_t multitag;                   // current multi send in progress
    bool closed;
    IArrayOf<ISocket> keptsockets;
    CriticalSection attachsect;
    unsigned __int64 attachaddrval;
    SocketEndpoint attachep;
    atomic_t attachchk;

protected: friend class CMPServer;
    SocketEndpoint remoteep;
    SocketEndpoint localep;         // who the other end thinks I am
protected: friend class CMPPacketReader;
    unsigned lastxfer;  
#ifdef _FULLTRACE
    unsigned startxfer; 
    unsigned numiter;
#endif


    bool connect(CTimeMon &tm)
    {
        // must be called from connectsect
        // also in sendmutex

        ISocket *newsock=NULL;
        unsigned retrycount = CONNECT_RETRYCOUNT;
        unsigned remaining;

        while (!channelsock) {
            try {
                StringBuffer str;
#ifdef _TRACE
                LOG(MCdebugInfo(100), unknownJob, "MP: connecting to %s",remoteep.getUrlStr(str).str());
#endif
                if (((int)tm.timeout)<0)
                    remaining = CONNECT_TIMEOUT;
                else if (tm.timedout(&remaining)) {
#ifdef _FULLTRACE
                    PROGLOG("MP: connect timed out 1");
#endif
                    return false;
                }
                if (remaining<10000)
                    remaining = 10000; // 10s min granularity for MP
                newsock = ISocket::connect_timeout(remoteep,remaining);
                newsock->set_keep_alive(true);
#ifdef _FULLTRACE
                LOG(MCdebugInfo(100), unknownJob, "MP: connect after socket connect, retrycount = %d", retrycount);
#endif

                SocketEndpointV4 id[2];
                SocketEndpoint hostep;
                hostep.setLocalHost(parent->getPort());
                id[0].set(hostep);
                id[1].set(remoteep);

                unsigned __int64 addrval = DIGIT1*id[0].ip[0] + DIGIT2*id[0].ip[1] + DIGIT3*id[0].ip[2] + DIGIT4*id[0].ip[3] + id[0].port;
#ifdef _TRACE
                PROGLOG("MP: connect addrval = %" I64F "u", addrval);
#endif

                newsock->write(&id[0],sizeof(id)); 

#ifdef _FULLTRACE
                StringBuffer tmp1;
                id[0].getUrlStr(tmp1);
                tmp1.append(' ');
                id[1].getUrlStr(tmp1);
                LOG(MCdebugInfo(100), unknownJob, "MP: connect after socket write %s",tmp1.str());
#endif

                size32_t reply = 0;
                size32_t rd = 0;

#ifdef _TRACE
                LOG(MCdebugInfo(100), unknownJob, "MP: connect after socket write, waiting for read");
#endif

                // Wait for connection reply but also check for A<->B deadlock (where both processes are here
                // waiting for other side to send confirm) and decide who stops waiting based on address.
                // To be compatible with older versions of mplib which will not do this,
                // loop with short wait time and release CS to allow other side to proceed
                StringBuffer epStr;
                unsigned startMs = msTick();
                unsigned loopCnt = CONNECT_READ_TIMEOUT / CONNECT_TIMEOUT_INTERVAL + 1;
#ifdef _TRACE
                PROGLOG("MP: loopCnt start = %u", loopCnt);
#endif
                while (loopCnt-- > 0)
                {
                    {
                        CriticalBlock block(attachsect);
#ifdef _TRACE
                        PROGLOG("MP: connect got attachsect, attachchk = %d, loopCnt = %u", atomic_read(&attachchk), loopCnt);
#endif
                        if (atomic_read(&attachchk) > 0)
                        {
                            if (remoteep.equals(attachep))
                            {
#ifdef _TRACE
                                PROGLOG("MP: deadlock situation [] attachaddrval = %" I64F "u addrval = %" I64F "u", attachaddrval, addrval);
#endif
                                if (attachaddrval < addrval)
                                    break;
                            }
                        }
                    }

                    rd = 0;

                    try
                    {
                        newsock->readtms(&reply,sizeof(reply),sizeof(reply),rd,CONNECT_TIMEOUT_INTERVAL);
                    }
                    catch (IException *e)
                    {
#ifdef _TRACE
                        PROGLOG("MP: loop exception code = %d, loopCnt = %u", e->errorCode(), loopCnt);
#endif
                        if ( (e->errorCode() != JSOCKERR_timeout_expired) ||
                             ((e->errorCode() == JSOCKERR_timeout_expired) && (loopCnt == 0)) )
                        {
                                if (tm.timedout(&remaining))
                                {
#ifdef _FULLTRACE
                                    EXCLOG(e,"MP: connect timed out 3");
#endif
                                    e->Release();
                                    newsock->Release();
                                    return false;
                                }
#ifdef _TRACE
                                EXCLOG(e, "MP: Failed to connect");
#endif
                                e->Release();
                                if ((retrycount--==0)||(tm.timeout==MP_ASYNC_SEND))
                                {   // don't bother retrying on async send
                                    throw new CMPException(MPERR_connection_failed,remoteep);
                                }
#ifdef _TRACE
                                LOG(MCdebugInfo(100), unknownJob, "MP: Retrying connection to %s, %d attempts left",remoteep.getUrlStr(str).toCharArray(),retrycount+1);
#endif
                        }
                        else
                        {
                            if (0 == epStr.length())
                            {
                                SocketEndpoint ep;
                                newsock->getPeerEndpoint(ep);
                                ep.getUrlStr(epStr);
                            }
                            WARNLOG("MP: connect to: %s, stalled for %d ms so far", epStr.str(), msTick()-startMs);
                            e->Release();
                        }
                    }
#ifdef _FULLTRACE
                    PROGLOG("MP: rd = %d", rd);
#endif
                    if (rd != 0)
                        break;
                }

#ifdef _TRACE
                LOG(MCdebugInfo(100), unknownJob, "MP: connect after socket read rd=%u, reply=%u, sizeof(id)=%lu", rd, reply, sizeof(id));
#endif

                if (reply!=0)
                {
                    unsigned elapsedMs = msTick() - startMs;
                    if (elapsedMs >= TRACESLOW_THRESHOLD)
                    {
                        if (0 == epStr.length())
                        {
                            SocketEndpoint ep;
                            newsock->getPeerEndpoint(ep);
                            ep.getUrlStr(epStr);
                        }
                        WARNLOG("MP: connect to: %s, took: %d ms", epStr.str(), elapsedMs);
                    }

                    assertex(reply==sizeof(id));    // how can this fail?
                    if (attachSocket(newsock,remoteep,hostep,true,NULL,addrval))
                    {
                        newsock->Release();
                        newsock = NULL;
#ifdef _TRACE
                        LOG(MCdebugInfo(100), unknownJob, "MP: connected to %s",str.str());
#endif
                        lastxfer = msTick();
                        closed = false;
                        break;
                    }
                }

            }
            catch (IException *e)
            {
                if (tm.timedout(&remaining)) {
#ifdef _FULLTRACE
                    EXCLOG(e,"MP: connect timed out 2");
#endif
                    e->Release();
                    return false;
                }
#ifdef _TRACE
                EXCLOG(e, "MP: Failed to connect");
#endif
                e->Release();
                if ((retrycount--==0)||(tm.timeout==MP_ASYNC_SEND)) {   // don't bother retrying on async send
                    IMP_Exception *e=new CMPException(MPERR_connection_failed,remoteep);
                    throw e;
                }
#ifdef _TRACE
                StringBuffer str;
                str.clear();
                LOG(MCdebugInfo(100), unknownJob, "MP: Retrying connection to %s, %d attempts left",remoteep.getUrlStr(str).str(),retrycount+1);
#endif
            }

            ::Release(newsock);
            newsock = NULL;

            {
                CriticalUnblock unblock(connectsect); // to avoid connecting philosopher problem
#ifdef _FULLTRACE
                PROGLOG("MP: before sleep");
#endif
                // check often if channelsock was created from accept thread
                Sleep(50);
                unsigned totalt = CONNECT_TIMEOUT_MINSLEEP + getRandom() % (CONNECT_TIMEOUT_MAXSLEEP-CONNECT_TIMEOUT_MINSLEEP);
                unsigned startt = msTick();
                unsigned deltat = 0;
                while (deltat < totalt)
                {
                    {
                        CriticalBlock block(connectsect);
                        if (channelsock)
                            break;
                    }
                    deltat = msTick() - startt;
                    Sleep(50);
                }
#ifdef _FULLTRACE
                PROGLOG("MP: after sleep");
#endif
            }

        }
        return true;
    }


public:

    Semaphore pingsem;

    CMPChannel(CMPServer *_parent,SocketEndpoint &_remoteep);
    ~CMPChannel();

    void reset();

    bool attachSocket(ISocket *newsock,const SocketEndpoint &_remoteep,const SocketEndpoint &_localep,bool ismaster,size32_t *confirm, unsigned __int64 addrval=0);

    bool writepacket(const void *hdr,size32_t hdrsize,const void *hdr2,size32_t hdr2size,const void *body,size32_t bodysize,CTimeMon &tm)
    {
        Linked<ISocket> dest;
        {
            CriticalBlock block(connectsect);
            if (closed) {
#ifdef _FULLTRACE
                LOG(MCdebugInfo(100), unknownJob, "WritePacket closed on entry");
                PrintStackReport();
#endif
                IMP_Exception *e=new CMPException(MPERR_link_closed,remoteep);
                throw e;
            }
            if (!channelsock) {
                if (!connect(tm)) {
#ifdef _FULLTRACE
                    LOG(MCdebugInfo(100), unknownJob, "WritePacket connect failed");
#endif
                    return false;
                }
            }
            dest.set(channelsock);
        }
        try {
#ifdef _FULLTRACE
            unsigned t1 = msTick();
#endif
            if ((tm.timeout!=MP_ASYNC_SEND)&&(tm.timeout!=MP_WAIT_FOREVER)) {
//              if (tm.timeout!=MP_ASYNC_SEND) {
                unsigned remaining;
                if (tm.timedout(&remaining))
                    return false;
                if (channelsock->wait_write(remaining)==0) {
                    return false;
                }
                if (tm.timedout())
                    return false;
            }
            // exception checking TBD
#ifdef _FULLTRACE
            StringBuffer ep1;
            StringBuffer ep2;
            LOG(MCdebugInfo(100), unknownJob, "WritePacket(target=%s,(%d,%d,%d))",remoteep.getUrlStr(ep1).str(),hdrsize,hdr2size,bodysize);
            unsigned t2 = msTick();
#endif
            unsigned n = 0;
            const void *bufs[3];
            size32_t sizes[3];
            if (hdrsize) {
                bufs[n] = hdr;
                sizes[n++] = hdrsize;
            }
            if (hdr2size) {
                bufs[n] = hdr2;
                sizes[n++] = hdr2size;
            }
            if (bodysize) {
                bufs[n] = body;
                sizes[n++] = bodysize;
            }
            if (!dest) {
                LOG(MCdebugInfo(100), unknownJob, "MP Warning: WritePacket unexpected NULL socket");
                return false;
            }
            dest->write_multiple(n,bufs,sizes);  
            lastxfer = msTick();
#ifdef _FULLTRACE
            LOG(MCdebugInfo(100), unknownJob, "WritePacket(timewaiting=%d,timesending=%d)",t2-t1,lastxfer-t2);
#endif
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, unknownJob, e,"MP writepacket");
            closeSocket();
            throw;
        }
        return true;
    }

    bool writepacket(const void *hdr,size32_t hdrsize,const void *body,size32_t bodysize,CTimeMon &tm)
    {
        return writepacket(hdr,hdrsize,NULL,0,body,bodysize,tm);
    }

    bool writepacket(const void *hdr,size32_t hdrsize,CTimeMon &tm)
    {
        return writepacket(hdr,hdrsize,NULL,0,NULL,0,tm);
    }

    bool sendPing(CTimeMon &tm);
    bool sendPingReply(unsigned timeout,bool identifyself);

    bool verifyConnection(CTimeMon &tm,bool allowconnect)
    {
        {
            CriticalBlock block(connectsect);
            if (!channelsock&&allowconnect)
                return connect(tm);
            if (closed||!channelsock)
                return false;
            if ((msTick()-lastxfer)<VERIFY_DELAY) 
                return true;
        }
        StringBuffer ep;
        remoteep.getUrlStr(ep);
        loop {
            CTimeMon pingtm(1000*60);
            if (sendPing(pingtm)) 
                break;
            {
                CriticalBlock block(connectsect);
                if (closed||!channelsock)
                    return false;
            }
            if (tm.timedout()) {
                LOG(MCdebugInfo(100), unknownJob, "MP: verify, ping failed to %s",ep.str());
                closeSocket();
                return false;
            }
            LOG(MCdebugInfo(100), unknownJob, "MP: verify, ping failed to %s, retrying",ep.str());
            unsigned remaining;
            if (!pingtm.timedout(&remaining)&&remaining) 
                Sleep(remaining);
        }
        return true;
    }


    bool send(MemoryBuffer &mb, mptag_t tag, mptag_t replytag, CTimeMon &tm, bool reply);


    void closeSocket(bool keepsocket=false)
    {
        ISocket *s;
        bool socketfailed = false;
        {
            CriticalBlock block(connectsect);
            if (!channelsock) 
                return;
            lastxfer = msTick();
            closed = true;
            if (parent)
                parent->checkclosed = true;
            s=channelsock;
            channelsock = NULL;
            {
                CriticalBlock block(attachsect);
                attachaddrval = 0;
                attachep.set(NULL);
                atomic_set(&attachchk, 0);
            }
            if (!keepsocket) {
                try {
                    s->shutdown();
                }
                catch (IException *) { 
                    socketfailed = true; // ignore if the socket has been closed
                }
            }
            parent->querySelectHandler().remove(s);
        }
        parent->notifyClosed(remoteep);
        if (socketfailed) {
            try {
                s->Release();
            }
            catch (IException *) { 
                // ignore
            }
        }
        else if (keepsocket) {
            // hopefully shouldn't get too many of these! (this is a kludge to prevent closing off wrong socket)
            if (keptsockets.ordinality()>10)
                keptsockets.remove(0);
            keptsockets.append(*s);
        }
        else {
            try {
                s->close();
            }
            catch (IException *) { 
                socketfailed = true; // ignore if the socket has been closed
            }
            s->Release();
        }
    }


    CMPServer &queryServer() { return *parent; }

    void monitorCheck();

    StringBuffer & queryEpStr(StringBuffer &s)
    {
        return remoteep.getUrlStr(s);
    }

    bool isClosed()
    {
        return closed;
    }

    bool isConnected()
    {
        return !closed&&(channelsock!=NULL);
    }
};

// Message Handlers (not done as interfaces for speed reasons

class UserPacketHandler // default
{
    CMPServer *server;
public:
    UserPacketHandler(CMPServer *_server)
    {
        server = _server;
    }

    void handle(CMessageBuffer *msg) // takes ownership of message buffer
    {
        server->getReceiveQ().enqueue(msg);
    }
    bool send(CMPChannel *channel,PacketHeader &hdr,MemoryBuffer &mb, CTimeMon &tm)
    {
#ifdef _FULLTRACE
        StringBuffer ep1;
        StringBuffer ep2;
        LOG(MCdebugInfo(100), unknownJob, "MP: send(target=%s,sender=%s,tag=%d,replytag=%d,size=%d)",hdr.target.getUrlStr(ep1).str(),hdr.sender.getUrlStr(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
#endif
        return channel->writepacket(&hdr,sizeof(hdr),mb.toByteArray(),mb.length(),tm);
    }
};

class PingPacketHandler // TAG_SYS_PING
{
public:
    void handle(CMPChannel *channel,bool identifyself)
    {
        channel->sendPingReply(CONFIRM_TIMEOUT,identifyself); 
    }
    bool send(CMPChannel *channel,PacketHeader &hdr,CTimeMon &tm)
    {
        return channel->writepacket(&hdr,sizeof(hdr),tm);
    }
};

class PingReplyPacketHandler // TAG_SYS_PING_REPLY
{
public:
    void handle(CMPChannel *channel)
    {
        channel->pingsem.signal();
    }
    bool send(CMPChannel *channel,PacketHeader &hdr,MemoryBuffer &mb,CTimeMon &tm)
    {
        return channel->writepacket(&hdr,sizeof(hdr),mb.toByteArray(),mb.length(),tm);
    }
};



class CMultiPacketReceiver: public CInterface
{   // assume each sender only sends one multi-message per channel
public:
    SocketEndpoint sender;
    MultiPacketHeader info;
    CMessageBuffer *msg;
    byte * ptr;
};

class MultiPacketHandler // TAG_SYS_MULTI
{
    CIArrayOf<CMultiPacketReceiver> inprogress; // should be ok as not many in progress hopefully (TBD orphans)
    CriticalSection sect;
    unsigned lastErrMs;

    void logError(unsigned code, MultiPacketHeader &mhdr, CMessageBuffer &msg, MultiPacketHeader *otherMhdr)
    {
        unsigned ms = msTick();
        if ((ms-lastErrMs) > 1000) // avoid logging too much
        {
            StringBuffer errorMsg("sender=");
            msg.getSender().getUrlStr(errorMsg).newline();
            errorMsg.append("This header: ");
            mhdr.getDetails(errorMsg).newline();
            if (otherMhdr)
            {
                errorMsg.append("Other header: ");
                otherMhdr->getDetails(errorMsg).newline();
            }
            msg.getDetails(errorMsg);
            LOG(MCerror, unknownJob, "MultiPacketHandler: protocol error (%d) %s", code, errorMsg.str());
        }
        lastErrMs = ms;
    }
public:
    MultiPacketHandler() : lastErrMs(0)
    {
    }
    CMessageBuffer *handle(CMessageBuffer * msg)
    {
        if (!msg) 
            return NULL;
        CriticalBlock block(sect);
        MultiPacketHeader mhdr;
        msg->read(sizeof(mhdr),&mhdr);
        CMultiPacketReceiver *recv=NULL;
        ForEachItemIn(i,inprogress) {
            CMultiPacketReceiver &mpr = inprogress.item(i);
            if ((mpr.info.tag==mhdr.tag)&&mpr.sender.equals(msg->getSender())) {
                recv = &mpr;
                break;
            }
        }
        if (mhdr.idx==0) {
            if ((mhdr.ofs!=0)||(recv!=NULL)) {
                logError(1, mhdr, *msg, recv?&recv->info:NULL);
                delete msg;
                return NULL;
            }
            recv = new CMultiPacketReceiver;
            recv->msg = new CMessageBuffer();           
            recv->msg->init(msg->getSender(),mhdr.tag,msg->getReplyTag());
            recv->ptr = (byte *)recv->msg->reserveTruncate(mhdr.total);
            recv->sender = msg->getSender();
            recv->info = mhdr;
            inprogress.append(*recv);
        }
        else {
            if ((recv==NULL)||(mhdr.ofs==0)||
                 (recv->info.ofs+recv->info.size!=mhdr.ofs)||
                 (recv->info.idx+1!=mhdr.idx)||
                 (recv->info.total!=mhdr.total)||
                 (mhdr.ofs+mhdr.size>mhdr.total)) {
                logError(2, mhdr, *msg, recv?&recv->info:NULL);
                delete msg;
                return NULL;
            }
        }
        msg->read(mhdr.size,recv->ptr+mhdr.ofs);
        delete msg;
        msg = NULL;
        recv->info = mhdr;
        if (mhdr.idx+1==mhdr.numparts) {
            if (mhdr.ofs+mhdr.size!=mhdr.total) {
                logError(3, mhdr, *msg, NULL);
                return NULL;
            }
            msg = recv->msg;
            inprogress.remove(i);
        }
        return msg;
    }
    bool send(CMPChannel *channel,PacketHeader &hdr,MemoryBuffer &mb, CTimeMon &tm, Mutex &sendmutex)
    {
        // must not adjust mb
#ifdef _FULLTRACE
        StringBuffer ep1;
        StringBuffer ep2;
        LOG(MCdebugInfo(100), unknownJob, "MP: multi-send(target=%s,sender=%s,tag=%d,replytag=%d,size=%d)",hdr.target.getUrlStr(ep1).str(),hdr.sender.getUrlStr(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
#endif
        PacketHeader outhdr;
        outhdr = hdr;
        outhdr.tag = TAG_SYS_MULTI;
        MultiPacketHeader mhdr;
        mhdr.total = hdr.size-sizeof(hdr);      
        mhdr.numparts = (mhdr.total+MAXDATAPERPACKET-1)/MAXDATAPERPACKET;
        mhdr.size = mhdr.total/mhdr.numparts;
        mhdr.tag = hdr.tag;
        mhdr.ofs = 0;
        mhdr.idx = 0;
        const byte *p = (const byte *)mb.toByteArray();
        unsigned i=0;
        loop {
            if (i+1==mhdr.numparts) 
                mhdr.size = mhdr.total-mhdr.ofs;
#ifdef _FULLTRACE
            LOG(MCdebugInfo(100), unknownJob, "MP: multi-send block=%d, num blocks=%d, ofs=%d, size=%d",i,mhdr.numparts,mhdr.ofs,mhdr.size);
#endif
            outhdr.initseq();
            outhdr.size = sizeof(outhdr)+sizeof(mhdr)+mhdr.size;
            if (!channel->writepacket(&outhdr,sizeof(outhdr),&mhdr,sizeof(mhdr),p,mhdr.size,tm)) {
#ifdef _FULLTRACE
                LOG(MCdebugInfo(100), unknownJob, "MP: multi-send failed");
#endif
                return false;
            }
            i++;
            if (i==mhdr.numparts)
                break;
            sendmutex.unlock(); // allow other messages to interleave
            sendmutex.lock();
            mhdr.idx++;
            mhdr.ofs += mhdr.size;
            p += mhdr.size;
        }
        return true;
    }

};

class BroadcastPacketHandler // TAG_SYS_BCAST
{
public:
    CMessageBuffer *handle(CMessageBuffer * msg)
    {
        delete msg;
        return NULL;
    }
};

class ForwardPacketHandler // TAG_SYS_FORWARD
{
public:
    CMessageBuffer *handle(CMessageBuffer * msg)
    {
        delete msg;
        return NULL;
    }
};


// --------------------------------------------------------

class CMPPacketReader: public CInterface, public ISocketSelectNotify
{
    CMessageBuffer *activemsg;
    byte * activeptr;
    size32_t remaining;
    byte *dataptr;
    CMPChannel *parent;
    CriticalSection sect;
public:
    IMPLEMENT_IINTERFACE;

    CMPPacketReader(CMPChannel *_parent)
    {
        init(_parent);
    }

    void init(CMPChannel *_parent)
    {
        parent = _parent;
        activemsg = NULL;
    }

    void shutdown()
    {
        CriticalBlock block(sect);
        parent = NULL;
    }

    bool notifySelected(ISocket *sock,unsigned selected)
    {
        if (!parent)
            return false;
        try {
            // try and mop up all data on socket 
            
            size32_t sizeavail = sock->avail_read(); 
            if (sizeavail==0) {
                // graceful close
                Linked<CMPChannel> pc;
                {
                    CriticalBlock block(sect);
                    if (parent) {
                        pc.set(parent);     // don't want channel to disappear during call
                        parent = NULL;
                    }
                }
                if (pc) 
                    pc->closeSocket();
                return false;
            }
            do {
                parent->lastxfer = msTick();
#ifdef _FULLTRACE
                parent->numiter++;
#endif
                if (!activemsg) { // no message in progress
                    PacketHeader hdr; // header for active message
#ifdef _FULLTRACE
                    parent->numiter = 1;
                    parent->startxfer = msTick();
#endif
                    // assumes packet header will arrive in one go
                    if (sizeavail<sizeof(hdr)) {
#ifdef _FULLTRACE
                        LOG(MCdebugInfo(100), unknownJob, "Selected stalled on header %d %d",sizeavail,sizeavail-sizeof(hdr));
#endif
                        size32_t szread;
                        sock->read(&hdr,sizeof(hdr),sizeof(hdr),szread,60); // I don't *really* want to block here but not much else can do
                    }
                    else
                        sock->read(&hdr,sizeof(hdr));
                    if (hdr.version/0x100 != MP_PROTOCOL_VERSION/0x100) {
                        // TBD IPV6 here
                        SocketEndpoint ep;
                        hdr.sender.get(ep);
                        IMP_Exception *e=new CMPException(MPERR_protocol_version_mismatch,ep);
                        throw e;
                    }
                    if (sizeavail<=sizeof(hdr))
                        sizeavail = sock->avail_read(); 
                    else
                        sizeavail -= sizeof(hdr);
#ifdef _FULLTRACE
                    StringBuffer ep1;
                    StringBuffer ep2;
                    LOG(MCdebugInfo(100), unknownJob, "MP: ReadPacket(sender=%s,target=%s,tag=%d,replytag=%d,size=%d)",hdr.sender.getUrlStr(ep1).str(),hdr.target.getUrlStr(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
#endif
                    remaining = hdr.size-sizeof(hdr);
                    activemsg = new CMessageBuffer(remaining); // will get from low level IO at some stage
                    activeptr = (byte *)activemsg->reserveTruncate(remaining);
                    hdr.setMessageFields(*activemsg);
                }
                
                size32_t toread = sizeavail;
                if (toread>remaining)
                    toread = remaining;
                if (toread) {
                    sock->read(activeptr,toread);
                    remaining -= toread;
                    sizeavail -= toread;
                    activeptr += toread;
                }
                if (remaining==0) { // we have the packet so process

#ifdef _FULLTRACE
                    LOG(MCdebugInfo(100), unknownJob, "MP: ReadPacket(timetaken = %d,select iterations=%d)",msTick()-parent->startxfer,parent->numiter);
#endif
                    do {
                        switch (activemsg->getTag()) {
                        case TAG_SYS_MULTI:
                             activemsg = parent->queryServer().multipackethandler->handle(activemsg); // activemsg in/out
                             break;
                        case TAG_SYS_PING:
                             parent->queryServer().pingpackethandler->handle(parent,false); //,activemsg); 
                             delete activemsg;
                             activemsg = NULL;
                             break;
                        case TAG_SYS_PING_REPLY:
                             parent->queryServer().pingreplypackethandler->handle(parent); 
                             delete activemsg;
                             activemsg = NULL;
                             break;
                        case TAG_SYS_BCAST:
                             activemsg = parent->queryServer().broadcastpackethandler->handle(activemsg); 
                             break;
                        case TAG_SYS_FORWARD:
                             activemsg = parent->queryServer().forwardpackethandler->handle(activemsg); 
                             break;
                        default:
                             parent->queryServer().userpackethandler->handle(activemsg); // takes ownership
                             activemsg = NULL;
                        }
                    } while (activemsg);
                }
                if (!sizeavail)
                    sizeavail = sock->avail_read();
            } while (sizeavail);
            return false; // ok
        }
        catch (IException *e) {
            if (e->errorCode()!=JSOCKERR_graceful_close)
                FLLOG(MCoperatorWarning, unknownJob, e,"MP(Packet Reader)");
            e->Release();
        }
        // error here, so close socket (ignore error as may be closed already)
        try {
            if(parent)
                parent->closeSocket();
        }
        catch (IException *e) {
            e->Release();
        }
        parent = NULL;
        return false;
    }
};


CMPChannel::CMPChannel(CMPServer *_parent,SocketEndpoint &_remoteep) 
{
    channelsock = NULL;
    parent = _parent;
    remoteep = _remoteep;
    localep.set(parent->getPort());
    multitag = TAG_NULL;
    reader = new CMPPacketReader(this);
    closed = false;
    master = false;
    sendwaiting = 0;
    attachaddrval = 0;
    attachep.set(NULL);
    atomic_set(&attachchk, 0);
}

void CMPChannel::reset()
{
    reader->shutdown(); // clear as early as possible
    closeSocket();
    reader->Release();
    channelsock = NULL;
    multitag = TAG_NULL;
    reader = new CMPPacketReader(this);
    closed = false;
    master = false;
    sendwaiting = 0;
    attachaddrval = 0;
    attachep.set(NULL);
    atomic_set(&attachchk, 0);
}


CMPChannel::~CMPChannel()
{
    reader->shutdown(); // clear as early as possible
    closeSocket();
    reader->Release();
}

bool CMPChannel::attachSocket(ISocket *newsock,const SocketEndpoint &_remoteep,const SocketEndpoint &_localep,bool ismaster,size32_t *confirm, unsigned __int64 addrval) // takes ownership if succeeds
{
    struct attachdTor
    {
        atomic_t &attchk;
        attachdTor(atomic_t &_attchk) : attchk(_attchk) { }
        ~attachdTor() { atomic_dec(&attchk); }
    } attachChk (attachchk);

#ifdef _FULLTRACE       
    PROGLOG("MP: attachSocket on entry, ismaster = %d, confirm = %p, channelsock = %p, addrval = %" I64F "u", ismaster, confirm, channelsock, addrval);
#endif

    {
        CriticalBlock block(attachsect);
        attachaddrval = addrval;
        attachep = _remoteep;
        atomic_inc(&attachchk);
    }

    CriticalBlock block(connectsect);

#ifdef _FULLTRACE       
    PROGLOG("MP: attachSocket got connectsect, channelsock = %p", channelsock);
#endif

    // resolution to stop clash i.e. A sends to B at exactly same time B sends to A

    if (channelsock) {

        if (_remoteep.port==0)
            return false;

        StringBuffer ep1;
        StringBuffer ep2;
        _localep.getUrlStr(ep1);
        _remoteep.getUrlStr(ep2);
        LOG(MCdebugInfo(100), unknownJob, "MP: Possible clash between %s->%s %d(%d)",ep1.str(),ep2.str(),(int)ismaster,(int)master);

        try {
            if (ismaster!=master) {
                if (ismaster) {
                    LOG(MCdebugInfo(100), unknownJob, "MP: resolving socket attach clash (master)");
                    return false;
                }
                else {
                    Sleep(50);  // give the other side some time to close
                    CTimeMon tm(10000);
                    if (verifyConnection(tm,false)) {
                        LOG(MCdebugInfo(100), unknownJob, "MP: resolving socket attach clash (verified)");
                        return false;
                    }
                }
            }
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, unknownJob, e,"MP attachsocket(1)");
            e->Release();
        }
        try {
            LOG(MCdebugInfo(100), unknownJob, "Message Passing - removing stale socket to %s",ep2.str());
            CriticalUnblock unblock(connectsect);
            closeSocket(true);
#ifdef REFUSE_STALE_CONNECTION
            if (!ismaster)
                return false;
#endif
            Sleep(100); // pause to allow close socket triggers to run
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, unknownJob, e,"MP attachsocket(2)");
            e->Release();
        }

    }

    if (confirm)
        newsock->write(confirm,sizeof(*confirm)); // confirm while still in connectsect

    closed = false;
    reader->init(this);
    channelsock = LINK(newsock);

#ifdef _FULLTRACE       
    PROGLOG("MP: attachSocket before select add");
#endif

    parent->querySelectHandler().add(channelsock,SELECTMODE_READ,reader);

#ifdef _FULLTRACE       
    PROGLOG("MP: attachSocket after select add");
#endif

    localep = _localep;
    master = ismaster;

    return true;
}

bool CMPChannel::send(MemoryBuffer &mb, mptag_t tag, mptag_t replytag, CTimeMon &tm, bool reply)
{   
    // note must not adjust mb
    assertex(tag!=TAG_NULL);
    assertex(tm.timeout);
    const byte *msg = (const byte *)mb.toByteArray();
    size32_t msgsize = mb.length();
    PacketHeader hdr(msgsize+sizeof(PacketHeader),localep,remoteep,tag,replytag);
    if (closed||(reply&&!isConnected())) {  // flag error if has been disconnected
#ifdef _FULLTRACE
        LOG(MCdebugInfo(100), unknownJob, "CMPChannel::send closed on entry %d",(int)closed);
        PrintStackReport();
#endif
        IMP_Exception *e=new CMPException(MPERR_link_closed,remoteep);
        throw e;
    }

    bool ismulti = (msgsize>MAXDATAPERPACKET);
    // pre-condition - ensure no clashes
    loop {
        sendmutex.lock();
        if (ismulti) {
            if (multitag==TAG_NULL)     // don't want to interleave with other multi send
                break;
        }
        else if (multitag!=tag)         // don't want to interleave with another of same tag
            break;
        sendwaiting++;
        sendmutex.unlock();
        sendwaitingsig.wait();
    }


    struct Cpostcondition // can we start using eiffel
    {
        Mutex &sendmutex;
        unsigned &sendwaiting;
        Semaphore &sendwaitingsig;
        mptag_t *multitag;

        Cpostcondition(Mutex &_sendmutex,unsigned &_sendwaiting,Semaphore &_sendwaitingsig,mptag_t *_multitag)
            : sendmutex(_sendmutex),sendwaiting(_sendwaiting),sendwaitingsig(_sendwaitingsig)
        {
            multitag = _multitag;
        }

        ~Cpostcondition() 
        { 
            if (multitag)
                *multitag = TAG_NULL; 
            if (sendwaiting) {
                sendwaitingsig.signal(sendwaiting);
                sendwaiting = 0;
            }
            sendmutex.unlock();
        }
    } postcond(sendmutex,sendwaiting,sendwaitingsig,ismulti?&multitag:NULL); 

    if (ismulti)
        return parent->multipackethandler->send(this,hdr,mb,tm,sendmutex);
    return parent->userpackethandler->send(this,hdr,mb,tm);
}

bool CMPChannel::sendPing(CTimeMon &tm)
{
    unsigned remaining;
    tm.timedout(&remaining);
    if (!sendmutex.lockWait(remaining))
        return false;
    SocketEndpoint myep(parent->getPort());
    PacketHeader hdr(sizeof(PacketHeader),myep,remoteep,TAG_SYS_PING,TAG_SYS_PING_REPLY);
    bool ret = false;
    try {
        ret = parent->pingpackethandler->send(this,hdr,tm)&&!tm.timedout(&remaining);
    }
    catch (IException *e) {         
        FLLOG(MCoperatorWarning, unknownJob, e,"MP ping(1)");
        e->Release();
    }
    sendmutex.unlock();
    if (ret)
        ret = pingsem.wait(remaining);
    return ret;
}

bool CMPChannel::sendPingReply(unsigned timeout,bool identifyself)
{
    CTimeMon mon(timeout);
    unsigned remaining;
    mon.timedout(&remaining);
    if (!sendmutex.lockWait(remaining))
        return false;
    SocketEndpoint myep(parent->getPort());
    MemoryBuffer mb;
    if (identifyself) {
#ifdef _WIN32
        mb.append(GetCommandLine());
#endif
    }
    PacketHeader hdr(mb.length()+sizeof(PacketHeader),myep,remoteep,TAG_SYS_PING_REPLY,TAG_NULL);

    bool ret;
    try {
        ret = parent->pingreplypackethandler->send(this,hdr,mb,mon);
    }
    catch (IException *e) {         
        FLLOG(MCoperatorWarning, unknownJob, e,"MP ping reply(1)");
        e->Release();
        ret = false;
    }
    sendmutex.unlock();
    return ret;
}
    
// --------------------------------------------------------
CMPConnectThread::CMPConnectThread(CMPServer *_parent, unsigned port)
    : Thread("MP Connection Thread")
{
    parent = _parent;
    if (!port)
    {
        // need to connect early to resolve clash
        Owned<IPropertyTree> env = getHPCCEnvironment();
        unsigned minPort, maxPort;
        if (env)
        {
            minPort = env->getPropInt("EnvSettings/ports/mpStart", MP_START_PORT);
            maxPort = env->getPropInt("EnvSettings/ports/mpEnd", MP_END_PORT);
        }
        else
        {
            minPort = MP_START_PORT;
            maxPort = MP_END_PORT;
        }
        assertex(maxPort >= minPort);
        Owned<IJSOCK_Exception> lastErr;
        unsigned numPorts = maxPort - minPort + 1;
        for (unsigned retries = 0; retries < numPorts * 3; retries++)
        {
            port = minPort + getRandom() % numPorts;
            try
            {
                listensock = ISocket::create(port, 16);  // better not to have *too* many waiting
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                if (e->errorCode()!=JSOCKERR_port_in_use)
                    throw;
                lastErr.setown(e);
            }
        }
        if (!listensock)
            throw lastErr.getClear();
    }
    else 
        listensock = NULL;  // delay create till running
    parent->setPort(port);
#ifdef _TRACE
    LOG(MCdebugInfo(100), unknownJob, "MP Connect Thread Init Port = %d", port);
#endif
    running = false;
}

void CMPConnectThread::checkSelfDestruct(void *p,size32_t sz)
{
    byte *b = (byte *)p;
    while (sz--) 
        if (*(b++)!=0xff)
            return;
    // Panic!
    PROGLOG("MP Self destruct invoked");
    try {
        if (listensock) {
            listensock->close();
            listensock->Release();
            listensock=NULL;
        }
    }
    catch (...)
    {
        PROGLOG("MP socket close failure");
    }
    // Kill registered child processes
    PROGLOG("MP self destruct exit");
    queryLogMsgManager()->flushQueue(10*1000);
#ifdef _WIN32
    ForEachItemIn(i,childprocesslist) 
        TerminateProcess((HANDLE)childprocesslist.item(i), 1);
    TerminateProcess(GetCurrentProcess(), 1);
#else
    ForEachItemIn(i,childprocesslist) 
        ::kill((HANDLE)childprocesslist.item(i), SIGTERM);
    ::kill(getpid(), SIGTERM);
#endif
    _exit(1);   

}

void CMPConnectThread::start(unsigned short port)
{
    if (!listensock)
        listensock = ISocket::create(port,16);  
    running = true;
    Thread::start();
}

int CMPConnectThread::run()
{
#ifdef _TRACE
    LOG(MCdebugInfo(100), unknownJob, "MP: Connect Thread Starting - accept loop");
#endif
    while (running) {
        ISocket *sock=NULL;
        try {
            sock=listensock->accept(true);
#ifdef _FULLTRACE       
            StringBuffer s;
            SocketEndpoint ep1;
            sock->getPeerEndpoint(ep1);
            PROGLOG("MP: Connect Thread: socket accepted from %s",ep1.getUrlStr(s).str());
#endif
        }
        catch (IException *e)
        {
            LOG(MCdebugInfo, unknownJob, e,"MP accept failed");
            throw; // error handling TBD
        }
        if (sock) {
            try {
                sock->set_keep_alive(true);
                size32_t rd;
                SocketEndpoint _remoteep;
                SocketEndpoint hostep;
                SocketEndpointV4 id[2];
                traceSlowReadTms("MP: initial accept packet from", sock, &id[0], sizeof(id), sizeof(id), rd, CONFIRM_TIMEOUT, CONFIRM_TIMEOUT_INTERVAL);
                if (rd != sizeof(id))
                {
                    StringBuffer errMsg("MP Connect Thread: invalid number of connection bytes serialized from ");
                    SocketEndpoint ep;
                    sock->getPeerEndpoint(ep);
                    ep.getUrlStr(errMsg);
                    FLLOG(MCoperatorWarning, unknownJob, "%s", errMsg.str());
                    sock->close();
                    continue;
                }
                id[0].get(_remoteep);
                id[1].get(hostep);

                unsigned __int64 addrval = DIGIT1*id[0].ip[0] + DIGIT2*id[0].ip[1] + DIGIT3*id[0].ip[2] + DIGIT4*id[0].ip[3] + id[0].port;
#ifdef _TRACE
                PROGLOG("MP: Connect Thread: addrval = %" I64F "u", addrval);
#endif

                if (_remoteep.isNull() || hostep.isNull())
                {
                    // JCSMORE, I think _remoteep really must/should match a IP of this local host
                    StringBuffer errMsg("MP Connect Thread: invalid remote and/or host ep serialized from ");
                    SocketEndpoint ep;
                    sock->getPeerEndpoint(ep);
                    ep.getUrlStr(errMsg);
                    FLLOG(MCoperatorWarning, unknownJob, "%s", errMsg.str());
                    sock->close();
                    continue;
                }
#ifdef _FULLTRACE       
                StringBuffer tmp1;
                _remoteep.getUrlStr(tmp1);
                tmp1.append(' ');
                hostep.getUrlStr(tmp1);
                PROGLOG("MP: Connect Thread: after read %s",tmp1.str());
#endif
                checkSelfDestruct(&id[0],sizeof(id));
                if (!parent->lookup(_remoteep).attachSocket(sock,_remoteep,hostep,false,&rd,addrval)) {
#ifdef _FULLTRACE       
                    PROGLOG("MP Connect Thread: lookup failed");
#endif
                }
                else {
#ifdef _TRACE
                    StringBuffer str1;
                    StringBuffer str2;
                    LOG(MCdebugInfo(100), unknownJob, "MP Connect Thread: connected to %s",_remoteep.getUrlStr(str1).str());
#endif
                }
#ifdef _FULLTRACE       
                PROGLOG("MP: Connect Thread: after write");
#endif
            }
            catch (IException *e)
            {
                FLLOG(MCoperatorWarning, unknownJob, e,"MP Connect Thread: Failed to make connection(1)");
                sock->close();
                e->Release();
            }
            try {
                sock->Release();
            }
            catch (IException *e)
            {
                FLLOG(MCoperatorWarning, unknownJob, e,"MP sock release failed");
                e->Release();
            }
        }
        else {
            if (running)
                LOG(MCdebugInfo(100), unknownJob, "MP Connect Thread accept returned NULL");
        }
    }
#ifdef _TRACE
    LOG(MCdebugInfo(100), unknownJob, "MP Connect Thread Stopping");
#endif
    return 0;
}

// --------------------------------------------------------

class CMPChannelIterator
{
    CMPServer &parent;
    CMPChannel *cur;
public:
    CMPChannelIterator(CMPServer &_parent)
        : parent(_parent)
    {
        cur = NULL;
    }

    bool first()
    {
        cur = NULL;
        return parent.nextChannel(cur);
    }

    bool next()
    {
        return cur&&parent.nextChannel(cur);
    }

    bool isValid()
    {
        return cur!=NULL;
    }

    CMPChannel &query() 
    { 
        return *cur; 
    }

};



//-----------------------------------------------------------------------------------

CMPChannel &CMPServer::lookup(const SocketEndpoint &endpoint)
{
    // there is an assumption here that no removes will be done within this loop
    CriticalBlock block(serversect);
    SocketEndpoint ep = endpoint;
    CMPChannel *e=find(ep);
    // Check for freed channels
    if (e&&e->isClosed()&&(msTick()-e->lastxfer>30*1000))
        e->reset();
    if (checkclosed) {
        checkclosed = false;
        CMPChannel *c = NULL;
        loop { 
            c = next(c);
            if (!c) {
                break;
            }
            if (c->isClosed()&&(msTick()-c->lastxfer>30*1000)) {
                removeExact(c);
                c = NULL;
            }
        }
        e=find(ep);
    }
    if (!e) {
        e = new CMPChannel(this,ep);
        add(*e);
    }
    return *e;
}


CMPServer::CMPServer(unsigned _port)
{
    port = 0;   // connectthread tells me what port it actually connected on
    checkclosed = false;
    connectthread = new CMPConnectThread(this, _port);
    selecthandler = createSocketSelectHandler();
    pingpackethandler = new PingPacketHandler;              // TAG_SYS_PING
    pingreplypackethandler = new PingReplyPacketHandler;    // TAG_SYS_PING_REPLY
    forwardpackethandler = new ForwardPacketHandler;        // TAG_SYS_FORWARD
    multipackethandler = new MultiPacketHandler;            // TAG_SYS_MULTI
    broadcastpackethandler = new BroadcastPacketHandler;    // TAG_SYS_BCAST
    userpackethandler = new UserPacketHandler(this);        // default
    notifyclosedthread = new CMPNotifyClosedThread(this);
    notifyclosedthread->start();
    initMyNode(port); // NB port set by connectthread constructor
    selecthandler->start();
}

CMPServer::~CMPServer()
{
#ifdef _TRACEORPHANS
    StringBuffer buf;
    getReceiveQueueDetails(buf);
    if (buf.length())
        LOG(MCdebugInfo(100), unknownJob, "MP: Orphan check\n%s",buf.str());
#endif
    releaseAll();
    selecthandler->Release();
    notifyclosedthread->stop();
    notifyclosedthread->Release();
    connectthread->Release();
    
    delete pingpackethandler;
    delete pingreplypackethandler;
    delete forwardpackethandler;
    delete multipackethandler;
    delete broadcastpackethandler;
    delete userpackethandler;
}


bool CMPServer::recv(CMessageBuffer &mbuf, const SocketEndpoint *ep, mptag_t tag, CTimeMon &tm)
{
    checkTagOK(tag);
    class Cnfy: public CBufferQueueNotify
    {
    public:
        bool aborted;
        CMessageBuffer *result;
        const SocketEndpoint *ep;
        SocketEndpoint closedEp; // used if receiving on RANK_ALL
        mptag_t tag;
        Cnfy(const SocketEndpoint *_ep,mptag_t _tag) { ep = _ep; tag = _tag; result = NULL; aborted=false; }
        bool notify(CMessageBuffer *msg)
        {
            if ((tag==TAG_ALL)||(tag==msg->getTag())) {
                SocketEndpoint senderep = msg->getSender();
                if ((ep==NULL)||ep->equals(senderep)||senderep.isNull()) {
                    if (msg->getReplyTag()==TAG_CANCEL)
                        delete msg;
                    else
                        result = msg;
                    return true;
                }
            }
            return false;
        }
        bool notifyClosed(SocketEndpoint &_closedEp) // called when connection closed
        {
            if (NULL == ep) { // ep is NULL if receiving on RANK_ALL
                closedEp = _closedEp;
                ep = &closedEp; // used for abort info
                aborted = true;
                return true;
            }
            else if (ep->equals(_closedEp)) {
                aborted = true;
                return true;
            }
            return false;
        }
    } nfy(ep,tag);
    if (receiveq.wait(nfy,false,tm)&&nfy.result) {
        mbuf.transferFrom(*nfy.result);
        delete nfy.result;
        return true;
    }
    if (nfy.aborted) {
#ifdef _FULLTRACE
        LOG(MCdebugInfo(100), unknownJob, "CMPserver::recv closed on notify");
        PrintStackReport();
#endif
        IMP_Exception *e=new CMPException(MPERR_link_closed,*nfy.ep);
        throw e;
    }
    return false;
}

void CMPServer::flush(mptag_t tag)
{
    class Cnfy: public CBufferQueueNotify
    {
    public:
        mptag_t tag;
        Cnfy(mptag_t _tag) { tag = _tag; }
        bool notify(CMessageBuffer *msg)
        {
            return (tag==TAG_ALL)||(tag==msg->getTag());
        }
        bool notifyClosed(SocketEndpoint &closedep) // called when connection closed
        {
            return false;
        }
    } nfy(tag);
    unsigned count = receiveq.flush(nfy);
    if (count) 
        PROGLOG("CMPServer::flush(%d) discarded %u buffers",(int)tag,count);
}

void CMPServer::cancel(const SocketEndpoint *ep, mptag_t tag)
{
    CMessageBuffer *cancelmsg = new CMessageBuffer(0);
    SocketEndpoint send;
    if (ep)
        send = *ep;
    cancelmsg->init(send,tag,TAG_CANCEL);
    getReceiveQ().enqueue(cancelmsg);
}

unsigned CMPServer::probe(const SocketEndpoint *ep, mptag_t tag,CTimeMon &tm,SocketEndpoint &sender)
{
    class Cnfy: public CBufferQueueNotify
    {
    public:
        bool aborted;
        SocketEndpoint &sender;
        const SocketEndpoint *ep;
        mptag_t tag;
        bool cancel;
        unsigned count;
        Cnfy(const SocketEndpoint *_ep,mptag_t _tag,SocketEndpoint &_sender) : sender(_sender) 
        { 
            ep = _ep; 
            tag = _tag; 
            cancel = false; 
            aborted = false; 
            count = 0;
        }
        bool notify(CMessageBuffer *msg)
        {
            if (((tag==TAG_ALL)||(tag==msg->getTag()))&&
                ((ep==NULL)||ep->equals(msg->getSender()))) {
                if (count==0) {
                    sender = msg->getSender();
                    cancel = (msg->getReplyTag()==TAG_CANCEL);
                }
                count++;
                return true;
            }
            return false;
        }
        bool notifyClosed(SocketEndpoint &closedep) // called when connection closed
        {
            if (ep&&ep->equals(closedep)) {
                aborted = true;
                return true;
            }
            return false;
        }
    } nfy(ep,tag,sender);
    if (receiveq.wait(nfy,true,tm)) {
        return nfy.cancel?0:nfy.count;
    }
    if (nfy.aborted) {
#ifdef _FULLTRACE
        LOG(MCdebugInfo(100), unknownJob, "CMPserver::probe closed on notify");
        PrintStackReport();
#endif
        IMP_Exception *e=new CMPException(MPERR_link_closed,*ep);
        throw e;
    }
    return 0;
}

void CMPServer::start()
{
    connectthread->start(getPort());    
}

void CMPServer::stop()
{
    selecthandler->stop(true); 
    connectthread->stop();
    CMPChannel *c = NULL;
    loop { 
        c = (CMPChannel *)next(c);
        if (!c)
            break;
        c->closeSocket();
    }
}



void CMPServer::addConnectionMonitor(IConnectionMonitor *monitor)
{
    // called in critical section CMPServer::sect
    notifyclosedthread->addConnectionMonitor(monitor);
}

void CMPServer::removeConnectionMonitor(IConnectionMonitor *monitor)
{
    // called in critical section CMPServer::sect
    notifyclosedthread->removeConnectionMonitor(monitor);
}


void CMPServer::onAdd(void *)
{
    // not used
}

void CMPServer::onRemove(void *e)
{
    CMPChannel &elem=*(CMPChannel *)e;      
    elem.Release();
}

unsigned CMPServer::getHashFromElement(const void *e) const
{
    const CMPChannel &elem=*(const CMPChannel *)e;      
    return elem.remoteep.hash(0);
}

unsigned CMPServer::getHashFromFindParam(const void *fp) const
{
    return ((const SocketEndpoint*)fp)->hash(0);
}

const void * CMPServer::getFindParam(const void *p) const
{
    const CMPChannel &elem=*(const CMPChannel *)p;      
    return &elem.remoteep;
}

bool CMPServer::matchesFindParam(const void * et, const void *fp, unsigned) const
{
    return ((CMPChannel *)et)->remoteep.equals(*(SocketEndpoint *)fp);
}

bool CMPServer::nextChannel(CMPChannel *&cur)
{
    CriticalBlock block(serversect);
    cur = (CMPChannel *)SuperHashTableOf<CMPChannel,SocketEndpoint>::next(cur);
    return cur!=NULL;
}

void CMPServer::notifyClosed(SocketEndpoint &ep)
{
#ifdef _TRACE
    StringBuffer url;
    LOG(MCdebugInfo(100), unknownJob, "MP: CMPServer::notifyClosed %s",ep.getUrlStr(url).str());
#endif
    notifyclosedthread->notify(ep);
}

// --------------------------------------------------------


class CInterCommunicator: public CInterface, public IInterCommunicator
{
    CMPServer *parent;

public:
    IMPLEMENT_IINTERFACE;

    bool send (CMessageBuffer &mbuf, INode *dst, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER)
    {
        if (!dst)
            return false;
        size32_t msgsize = mbuf.length();
        if (dst->equals(queryMyNode())) {
            CMessageBuffer *msg = new CMessageBuffer();
            mptag_t reply = mbuf.getReplyTag();
            msg->transferFrom(mbuf);
            msg->init(dst->endpoint(),tag,reply);
            parent->getReceiveQ().enqueue(msg);
            mbuf.clear(); // for consistent semantics
            return true;
        }

        CTimeMon tm(timeout);
        CMPChannel &channel = parent->lookup(dst->endpoint());
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        if (!channel.send(mbuf,tag,mbuf.getReplyTag(),tm,false))
            return false;
        mbuf.clear(); // for consistent semantics
        return true;
    }

    bool verifyConnection(INode *node, unsigned timeout)
    {
        CriticalBlock block(verifysect);
        CTimeMon tm(timeout);
        CMPChannel &channel = parent->lookup(node->endpoint());
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        return channel.verifyConnection(tm,true);
    }

    void verifyAll(StringBuffer &log)
    {
        CMPChannelIterator iter(*parent);
        if (iter.first()) {
            do {
                CriticalBlock block(verifysect);
                CTimeMon tm(5000);
                CMPChannel &channel = iter.query();
                if (!channel.isClosed()) {
                    channel.queryEpStr(log).append(' ');
                    if (channel.verifyConnection(tm,false))
                        log.append("OK\n");
                    else
                        log.append("FAILED\n");
                }
            }
            while (iter.next());
        }

    }

    bool verifyAll(IGroup *group,bool duplex, unsigned timeout)
    {
        CriticalBlock block(verifysect);
        CTimeMon tm(timeout);
        rank_t myrank = group->rank();
        {
            ForEachNodeInGroup(rank,*group) {
                bool doverify;
                if (duplex)
                    doverify = (myrank!=rank);
                else if ((rank&1)==(myrank&1))
                    doverify = (myrank>rank);
                else
                    doverify = (myrank<rank);
                if (doverify) {
                    CMPChannel &channel = parent->lookup(group->queryNode(rank).endpoint());
                    unsigned remaining;
                    if (tm.timedout(&remaining)) {
                        return false;
                    }
                    if (!channel.verifyConnection(tm,true)) {
                        return false;
                    }
                }
            }
        }
        if (!duplex) {
            ForEachNodeInGroup(rank,*group) {
                bool doverify = ((rank&1)==(myrank&1))?(myrank<rank):(myrank>rank);
                if (doverify) {
                    CMPChannel &channel = parent->lookup(group->queryNode(rank).endpoint());
                    while (!channel.verifyConnection(tm,false)) {
                        unsigned remaining;
                        if (tm.timedout(&remaining))
                            return false;
                        CriticalUnblock unblock(verifysect);
                        Sleep(100);
                    }
                }
            }
        }
        return true;
    }


    unsigned probe(INode *src, mptag_t tag, INode **sender=NULL, unsigned timeout=0)
    {
        if (sender)
            *sender = NULL;
        SocketEndpoint res;
        CTimeMon tm(timeout);
        unsigned ret = parent->probe(src?&src->endpoint():NULL,tag,tm,res);
        if (ret!=0) {
            if (sender) 
                *sender = createINode(res);
            return ret;
        }
        return 0;
    }
    
    bool recv(CMessageBuffer &mbuf, INode *src, mptag_t tag, INode **sender=NULL, unsigned timeout=MP_WAIT_FOREVER)
    {
        if (sender)
            *sender = NULL;
        CTimeMon tm(timeout);
        loop
        {
            try
            {
                if (parent->recv(mbuf,src?&src->endpoint():NULL,tag,tm))
                {
                    if (sender)
                        *sender = createINode(mbuf.getSender());
                    return true;
                }
                return false;
            }
            catch (IMP_Exception *e)
            {
                if (MPERR_link_closed != e->errorCode())
                    throw;
                const SocketEndpoint &ep = e->queryEndpoint();
                if (src && (ep == src->endpoint()))
                    throw;
                // ignoring closed endpoint
                e->Release();
                // loop around and recv again
            }
        }
    }


    void flush(mptag_t tag)
    {
        parent->flush(tag);
    }


    bool sendRecv(CMessageBuffer &mbuff, INode *dst, mptag_t dsttag,  unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(dst);
        mptag_t replytag = createReplyTag();
        CTimeMon tm(timeout);
        mbuff.setReplyTag(replytag);
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        if (!send(mbuff,dst,dsttag,remaining)||tm.timedout(&remaining))
            return false;
        mbuff.clear();
        return recv(mbuff,dst,replytag,NULL,remaining);
    }

    bool reply(CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER)
    {
        Owned<INode> dst(createINode(mbuff.getSender()));
        return send(mbuff,dst,mbuff.getReplyTag(),timeout);
    }

    void cancel(INode *src, mptag_t tag)
    {
        parent->cancel(src?&src->endpoint():NULL,tag);
    }


    void disconnect(INode *node)
    {
        CriticalBlock block(verifysect);
        CMPChannel &channel = parent->lookup(node->endpoint());
        channel.closeSocket();
        parent->removeChannel(&channel);
    }

    CInterCommunicator(CMPServer *_parent)
    {
        parent = _parent;
    }
    ~CInterCommunicator()
    {
    }

};




class CCommunicator: public CInterface, public ICommunicator
{
    IGroup *group;
    CMPServer *parent;
    bool outer;

    const SocketEndpoint &queryEndpoint(rank_t rank)
    {
        return group->queryNode(rank).endpoint();
    }

    CMPChannel &queryChannel(rank_t rank)
    {
        return parent->lookup(queryEndpoint(rank));
    }

public:
    IMPLEMENT_IINTERFACE;


    bool send (CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout)
    { 
        // send does not corrupt mbuf
        if (dstrank==RANK_NULL)
            return false;
        rank_t myrank = group->rank();
        if (dstrank==myrank) {
            CMessageBuffer *msg = mbuf.clone();
            // change sender
            msg->init(queryMyNode()->endpoint(),tag,mbuf.getReplyTag());
            parent->getReceiveQ().enqueue(msg);
        }
        else {
            CTimeMon tm(timeout);
            rank_t endrank;
            if (dstrank==RANK_ALL) {
                send(mbuf,myrank,tag,timeout);
                dstrank = RANK_ALL_OTHER;
            }
            if (dstrank==RANK_ALL_OTHER) {
                dstrank = 0;
                endrank = group->ordinality()-1;
            }
            else if (dstrank==RANK_RANDOM) {
                if (group->ordinality()>1) {
                    do {
                        dstrank = getRandom()%group->ordinality();
                    } while (dstrank==myrank);
                }
                else {
                    assertex(myrank!=0);
                    dstrank = 0;
                }
                endrank = dstrank;
            }
            else
                endrank = dstrank;
            for (;dstrank<=endrank;dstrank++) {
                if (dstrank!=myrank) {
                    CMPChannel &channel = queryChannel(dstrank);
                    unsigned remaining;
                    if (tm.timedout(&remaining))
                        return false;
                    if (!channel.send(mbuf,tag,mbuf.getReplyTag(),tm,false))
                        return false;
                }
            }
        }
        return true;
    }

    void barrier(void)
    {
#ifdef _TRACE
        PrintLog("MP: barrier enter");
#endif

        /*
         * Use the dissemination algorithm described in:
         * Debra Hensgen, Raphael Finkel, and Udi Manbet, "Two Algorithms for Barrier Synchronization,"
         * International Journal of Parallel Programming, 17(1):1-17, 1988.
         * It uses ceiling(lgp) steps. In step k, 0 <= k <= (ceiling(lgp)-1),
         * process i sends to process (i + 2^k) % p and receives from process (i - 2^k + p) % p.
         */

        int myrank = group->rank();
        int numranks = group->ordinality();
        CMessageBuffer mb;
        rank_t r;

        int mask = 0x1;
        while (mask < numranks)
        {
            int dst = (myrank + mask) % numranks;
            int src = (myrank - mask + numranks) % numranks;

#ifdef _TRACE
            PrintLog("MP: barrier: send to %d, recv from %d", dst, src);
#endif

            // NOTE: MPI method MUST use sendrecv so as to not send/recv deadlock ...

            mb.clear();
            mb.append("MPTAG_BARRIER");
            bool oks = send(mb,dst,MPTAG_BARRIER,120000);
            mb.clear();
            bool okr = recv(mb,src,MPTAG_BARRIER,&r);

            if (!oks && !okr)
            {
                PrintLog("MP: barrier: Error sending or recving");
                break;
            }

            mask <<= 1;
        }

#ifdef _TRACE
        PrintLog("MP: barrier leave");
#endif
    }

    bool verifyConnection(rank_t rank,  unsigned timeout)
    {
        CriticalBlock block(verifysect);
        assertex(rank!=RANK_RANDOM);
        assertex(rank!=RANK_ALL);
        CTimeMon tm(timeout);
        CMPChannel &channel = queryChannel(rank);
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        return channel.verifyConnection(tm,true);
    }

    bool verifyAll(bool duplex, unsigned timeout)
    {
        CriticalBlock block(verifysect);
        CTimeMon tm(timeout);
        rank_t myrank = group->rank();
        {
            ForEachNodeInGroup(rank,*group) {
                bool doverify;
                if (duplex)
                    doverify = (rank!=myrank);
                else if ((rank&1)==(myrank&1))
                    doverify = (myrank>rank);
                else
                    doverify = (myrank<rank);
                if (doverify) {
                    CMPChannel &channel = queryChannel(rank);
                    unsigned remaining;
                    if (tm.timedout(&remaining)) {
                        return false;
                    }
                    if (!channel.verifyConnection(tm,true)) 
                        return false;
                }
            }
        }
        if (!duplex) {
            ForEachNodeInGroup(rank,*group) {
                bool doverify = ((rank&1)==(myrank&1))?(myrank<rank):(myrank>rank);
                if (doverify) {
                    CMPChannel &channel = queryChannel(rank);
                    while (!channel.verifyConnection(tm,false)) {
                        unsigned remaining;
                        if (tm.timedout(&remaining))
                            return false;
                        CriticalUnblock unblock(verifysect);
                        Sleep(100);
                    }
                }
            }
        }
        return true;
    }

    unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=0)
    {
        assertex(srcrank!=RANK_NULL);
        SocketEndpoint res;
        CTimeMon tm(timeout);
        unsigned ret = parent->probe((srcrank==RANK_ALL)?NULL:&queryEndpoint(srcrank),tag,tm,res);
        if (ret!=0) {
            if (sender)
                *sender = group->rank(res);
            return ret;
        }
        if (sender)
            *sender = RANK_NULL;
        return 0;
    }

    bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(srcrank!=RANK_NULL);
        const SocketEndpoint *srcep=NULL;
        if (srcrank==RANK_ALL) {
            if (!outer&&(group->ordinality()==1))               // minor optimization (useful in Dali)
                srcep = &queryEndpoint(0);
        }
        else
            srcep = &queryEndpoint(srcrank);
        CTimeMon tm(timeout);
        loop
        {
            try
            {
                if (parent->recv(mbuf,(srcrank==RANK_ALL)?NULL:&queryEndpoint(srcrank),tag,tm))
                {
                    if (sender)
                        *sender = group->rank(mbuf.getSender());
                    return true;
                }
                if (sender)
                    *sender = RANK_NULL;
                return false;
            }
            catch (IMP_Exception *e)
            {
                if (MPERR_link_closed != e->errorCode())
                    throw;
                const SocketEndpoint &ep = e->queryEndpoint();
                if (RANK_NULL != group->rank(ep))
                    throw;
                // ignoring closed endpoint from outside the communicator group
                e->Release();
                // loop around and recv again
            }
        }
    }
    
    void flush(mptag_t tag)
    {
        parent->flush(tag);
    }

    IGroup &queryGroup() { return *group; }
    IGroup *getGroup()  { return LINK(group); }

    bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex((sendrank!=RANK_NULL)&&(sendrank!=RANK_ALL));
        if (sendrank==RANK_RANDOM) {
            if (group->ordinality()>1) {
                do {
                    sendrank = getRandom()%group->ordinality();
                } while (sendrank==group->rank());
            }
            else {
                assertex(group->rank()!=0);
                sendrank = 0;
            }
        }
        mptag_t replytag = createReplyTag();
        CTimeMon tm(timeout);
        mbuff.setReplyTag(replytag);
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        if (!send(mbuff,sendrank,sendtag,remaining)||tm.timedout(&remaining))
            return false;
        mbuff.clear();
        return recv(mbuff,sendrank,replytag,NULL,remaining);
    }

    bool reply(CMessageBuffer &mbuf, unsigned timeout=MP_WAIT_FOREVER)
    {
        mptag_t replytag = mbuf.getReplyTag();
        rank_t dstrank = group->rank(mbuf.getSender());
        if (dstrank!=RANK_NULL) {
            if (send (mbuf, dstrank, replytag,timeout)) {
                mbuf.setReplyTag(TAG_NULL);
                return true;
            }
            return false;
        }
            
        CTimeMon tm(timeout);
        CMPChannel &channel = parent->lookup(mbuf.getSender());
        unsigned remaining;
        if (tm.timedout(&remaining)) {
            return false;
        }
        if (channel.send(mbuf,replytag,TAG_NULL,tm, true)) {
            mbuf.setReplyTag(TAG_NULL);
            return true;
        }
        return false;
    }

    void cancel(rank_t srcrank, mptag_t tag)
    {
        assertex(srcrank!=RANK_NULL);
        parent->cancel((srcrank==RANK_ALL)?NULL:&queryEndpoint(srcrank),tag);
    }

    void disconnect(INode *node)
    {
        CriticalBlock block(verifysect);
        CMPChannel &channel = parent->lookup(node->endpoint());
        channel.closeSocket();
        parent->removeChannel(&channel);
    }

    CCommunicator(CMPServer *_parent,IGroup *_group, bool _outer)
    {
        outer = _outer;
        parent = _parent;
        group = LINK(_group); 
    }
    ~CCommunicator()
    {
        group->Release();
    }

};


ICommunicator *createCommunicator(IGroup *group,bool outer)
{
    assertex(MPserver!=NULL);
    return new CCommunicator(MPserver,group,outer);
}

static IInterCommunicator *worldcomm=NULL;

IInterCommunicator &queryWorldCommunicator()
{
    CriticalBlock block(CMPServer::serversect);
    assertex(MPserver!=NULL);
    if (!worldcomm)
        worldcomm = new CInterCommunicator(MPserver);
    return *worldcomm;
}

void startMPServer(unsigned port, bool paused)
{
    assertex(sizeof(PacketHeader)==32);
    CriticalBlock block(CMPServer::serversect); 
    if (CMPServer::servernest==0)
    {
        if (!CMPServer::serverpaused)
        {
            delete MPserver;
            MPserver = new CMPServer(port);
        }
        if (paused)
        {
            CMPServer::serverpaused = true;
            return;
        }
        queryLogMsgManager()->setPort(MPserver->getPort());
        MPserver->start();
        CMPServer::serverpaused = false;
    }
    CMPServer::servernest++;
}


void stopMPServer()
{
    CriticalBlock block(CMPServer::serversect);
    if (--CMPServer::servernest==0) {
        stopLogMsgReceivers();
#ifdef _TRACE
        LOG(MCdebugInfo(100), unknownJob, "MP: Stopping MP Server");
#endif
        CriticalUnblock unblock(CMPServer::serversect);
        assertex(MPserver!=NULL);
        MPserver->stop();
        delete MPserver;
        MPserver = NULL;
        ::Release(worldcomm);
        worldcomm = NULL;
        initMyNode(0);
#ifdef _TRACE
        LOG(MCdebugInfo(100), unknownJob, "MP: Stopped MP Server");
#endif
    }
}

extern mp_decl void addMPConnectionMonitor(IConnectionMonitor *monitor)
{
    CriticalBlock block(CMPServer::serversect);
    assertex(MPserver);
    MPserver->addConnectionMonitor(monitor);
}

extern mp_decl void removeMPConnectionMonitor(IConnectionMonitor *monitor)
{
    CriticalBlock block(CMPServer::serversect);
    if (MPserver)
        MPserver->removeConnectionMonitor(monitor);
}

StringBuffer &getReceiveQueueDetails(StringBuffer &buf)
{
    CriticalBlock block(CMPServer::serversect);
    if (MPserver)
        MPserver->getReceiveQueueDetails(buf);
    return buf;
}



void registerSelfDestructChildProcess(HANDLE handle)
{
    CriticalBlock block(childprocesssect);
    if (handle!=(HANDLE)-1)
        childprocesslist.append((unsigned)handle);
}

void unregisterSelfDestructChildProcess(HANDLE handle)
{
    CriticalBlock block(childprocesssect);
    if (handle!=(HANDLE)-1) 
        childprocesslist.zap((unsigned)handle);
}
