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

#define mp_decl DECL_EXPORT

/* TBD
    lost packet disposal
    synchronous send
    connection protocol (HRPC);
    look at all timeouts
*/

#include <future>
#include <vector>
#include <list>
#include <array>

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
#include "jsecrets.hpp"

#include "mpcomm.hpp"
#include "mpbuff.hpp"
#include "mputil.hpp"
#include "mplog.hpp"

#include "securesocket.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

//#define _TRACE
//#define _FULLTRACE

//#define _TRACEMPSERVERNOTIFYCLOSED
#define _TRACEORPHANS


#define REFUSE_STALE_CONNECTION


#define MP_PROTOCOL_VERSION    0x102                   
#define MP_PROTOCOL_VERSIONV6   0x202                   // extended for IPV6

// These should really be configurable
#define CANCELTIMEOUT       1000             // 1 sec
#define CONNECT_TIMEOUT          (5*60*1000) // 5 mins
#define CONNECT_READ_TIMEOUT     (90*1000)   // 90 seconds. NB: used by connect readtms loop (see loopCnt)
#define CONNECT_READEXC_TIMEOUT  (10*1000)   // 10 seconds. NB: to read exception info after confirm
#define CONNECT_TIMEOUT_INTERVAL 1000        // 1 sec
#define CONNECT_RETRYCOUNT       180         // Overall max connect time is = CONNECT_RETRYCOUNT * CONNECT_READ_TIMEOUT
#define CONNECT_TIMEOUT_MINSLEEP 2000        // random range: CONNECT_TIMEOUT_MINSLEEP to CONNECT_TIMEOUT_MAXSLEEP milliseconds
#define CONNECT_TIMEOUT_MAXSLEEP 5000

#define PING_CONFIRM_TIMEOUT     (90*1000) // 1.5 mins
#define HEADER_CONFIRM_TIMEOUT   (90*1000) // 1.5 mins
#define TRACESLOW_THRESHOLD      1000 // 1 sec

#define VERIFY_DELAY            (1*60*1000)  // 1 Minute
#define VERIFY_TIMEOUT          (1*60*1000)  // 1 Minute

#define DIGIT1 U64C(0x10000000000) // (256ULL*256ULL*256ULL*65536ULL)
#define DIGIT2 U64C(0x100000000)   // (256ULL*256ULL*65536ULL)
#define DIGIT3 U64C(0x1000000)     // (256ULL*65536ULL)
#define DIGIT4 U64C(0x10000)       // (65536ULL)

#define _TRACING

static  CriticalSection childprocesssect;
#ifdef _WIN32
static  Unsigned64Array childprocesslist;
#else
static  UnsignedArray childprocesslist;
#endif

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
    StringBuffer & getEndpointHostText(StringBuffer &val)
    {
        SocketEndpoint s;
        this->get(s);
        return s.getEndpointHostText(val);
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

#if 0
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
#endif

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

class DECL_EXCEPTION CMPException: public IMP_Exception, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CMPException(MessagePassingError err,const SocketEndpoint &ep, const char *_msg = nullptr) : error(err), endpoint(ep), msg(_msg)
    {
    }

    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        StringBuffer tmp;
        switch (error) {
        case MPERR_ok:                          str.append("OK"); break;
        case MPERR_connection_failed:           str.appendf("MP connect failed (%s)",endpoint.getEndpointHostText(tmp).str()); break;
        case MPERR_process_not_in_group:        str.appendf("Current process not in Communicator group"); break;
        case MPERR_protocol_version_mismatch:   str.appendf("Protocol version mismatch (%s)",endpoint.getEndpointHostText(tmp).str()); break;
        // process crashes (segv, etc.) often cause this exception which is logged and can be misleading
        // change it from "MP link closed" to something more helpful
        case MPERR_link_closed:                 str.appendf("Unexpected process termination (ep:%s)",endpoint.getEndpointHostText(tmp).str()); break;
        }
        if (msg.length())
            str.append(" - ").append(msg);
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
    StringAttr msg;
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
        for (;;) {
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

bool check_kernparam(const char *path, int *value)
{
#ifdef __linux__
    FILE *f = fopen(path,"r");
    char res[32];
    char *r = 0;
    if (f) {
        r = fgets(res, sizeof(res), f);
        fclose(f);
        if (r) {
            *value = atoi(r);
            return true;
        }
    }
#endif
    return false;
}

bool check_somaxconn(int *val)
{
    return check_kernparam("/proc/sys/net/core/somaxconn", val);
}

class CMPServer;
class CMPChannel;


static CriticalSection portProbeCS;
static cycle_t portProbeLastLog = 0;
enum CloseType { CloseType_graceful, CloseType_error, CloseType_timeout, CloseType_COUNT };
static std::array<unsigned __int64, CloseType_COUNT> portProbeCloseCounts = {};
static std::array<unsigned __int64, CloseType_COUNT> portProbeCloseCycles = {};
static cycle_t oneMinCycles = 0; // initialized in trackPortProbe 1st time


static void trackPortProbe(cycle_t createTimeCycles, const char *peerEndpointTest, CloseType closeType)
{
    // this should be based on a logging feature flag
    cycle_t nowCycles = get_cycles_now();
    cycle_t elapsedCycles = nowCycles - createTimeCycles;
    CLeavableCriticalBlock b(portProbeCS);
    portProbeCloseCounts[closeType]++;
    portProbeCloseCycles[closeType] += elapsedCycles;
    if ((0 == portProbeLastLog))
    {
        portProbeLastLog = nowCycles;
        oneMinCycles = queryOneSecCycles()*60;
    }
    else
    {
        cycle_t cyclesSinceLastLog = nowCycles - portProbeLastLog;
        if (cyclesSinceLastLog >= oneMinCycles) // log max every minute
        {
            unsigned __int64 numGracefulClose = portProbeCloseCounts[CloseType_graceful];
            unsigned __int64 numError = portProbeCloseCounts[CloseType_error];
            unsigned __int64 numTimeout = portProbeCloseCounts[CloseType_timeout];
            cycle_t cyclesGracefulClose = portProbeCloseCycles[CloseType_graceful];
            cycle_t cyclesError = portProbeCloseCycles[CloseType_error];
            cycle_t cyclesTimeout = portProbeCloseCycles[CloseType_timeout];

            portProbeLastLog = nowCycles;

            b.leave(); // leave crit before logging

            unsigned __int64 totalProbes = numGracefulClose + numError + numTimeout;
            DBGLOG("Port probes: %" I64F "u [graceful=%" I64F "u (%" I64F "u ms),error=%" I64F "u (%" I64F "u ms),timedout=%" I64F "u (%" I64F "u ms). Last: %s, type=%s, time: %" I64F "u",
                totalProbes,
                numGracefulClose, cycle_to_millisec(cyclesGracefulClose),
                numError, cycle_to_millisec(cyclesError),
                numTimeout, cycle_to_millisec(cyclesTimeout),
                peerEndpointTest, CloseType_graceful==closeType?"graceful":CloseType_error==closeType?"error":"timeout", cycle_to_millisec(elapsedCycles));
        }
    }
}

// Legacy header sent id[2] only (but legacy clients are no longer supported since 9.6.4, i.e. they will fail during connection process)
struct ConnectHdr
{
    ConnectHdr(const SocketEndpoint &hostEp, const SocketEndpoint &remoteEp, unsigned __int64 role)
    {
        id[0].set(hostEp);
        id[1].set(remoteEp);

        hdr.size = sizeof(PacketHeader);
        hdr.tag = TAG_SYS_BCAST;
        hdr.flags = 0;
        hdr.version = MP_PROTOCOL_VERSION;
        setRole(role);
    }
    ConnectHdr()
    {
    }
    SocketEndpointV4 id[2];
    PacketHeader hdr;
    inline void setRole(unsigned __int64 role)
    {
        hdr.replytag = (mptag_t) (role >> 32);
        hdr.sequence = (unsigned) (role & 0xffffffff);
    }
    inline unsigned __int64 getRole() const
    {
        return (((unsigned __int64)hdr.replytag)<<32) | ((unsigned __int64)hdr.sequence);
    }
};


class CMPConnectThread: public Thread
{
    class CConnectSelectHandler
    {
        CMPConnectThread &owner;
        Owned<ISocketSelectHandler> selectHandler;
        unsigned mode = SELECTMODE_READ;
    public:
        class CSocketHandler : public CInterfaceOf<ISocketSelectNotify>
        {
            CConnectSelectHandler &selectHandler;
            Owned<ISocket> sock;
            StringBuffer peerHostText, peerEndpointText;
            ConnectHdr hdr;
            cycle_t createTime = 0;
            size32_t readSoFar = 0;
            CriticalSection crit;
            bool closedOrHandled = false;
        public:
            CSocketHandler(CConnectSelectHandler &_selectHandler, ISocket *_sock) : selectHandler(_selectHandler), sock(_sock)
            {
                createTime = get_cycles_now();
                SocketEndpoint peerEP;
                sock->getPeerEndpoint(peerEP);
                peerEP.getHostText(peerHostText); // always used by handleAcceptedSocket
                peerEndpointText.append(peerHostText); // only used if tracing an error
                if (peerEP.port)
                    peerEndpointText.append(':').append(peerEP.port);
            }
            ISocket *querySocket()
            {
                return sock;
            }
            ConnectHdr &queryHdr()
            {
                return hdr;
            }
            cycle_t queryCreateTime() const
            {
                return createTime;
            }
            size32_t queryReadSoFar() const
            {
                return readSoFar;
            }
            const char *queryPeerHostText() const
            {
                return peerHostText;
            }
            const char *queryPeerEndpointText() const
            {
                return peerEndpointText;
            }
            bool closeIfTimedout(cycle_t now)
            {
                if (cycle_to_millisec(now - createTime) >= HEADER_CONFIRM_TIMEOUT)
                {
                    // will block any pending notifySelected on this socket
                    CriticalBlock b(crit);
                    if (!closedOrHandled)
                    {
                        closedOrHandled = true;
                        return true;
                    }
                }
                return false;
            }
            // ISocketSelectNotify impl.
            virtual bool notifySelected(ISocket *sock, unsigned selected) override
            {
                CLeavableCriticalBlock b(crit);
                if (closedOrHandled)
                    return false;
                size32_t rd = 0;
                void *p = (byte *)&hdr + readSoFar;

                Owned<IJSOCK_Exception> exception;
                try
                {
                    sock->readtms(p, 0, sizeof(ConnectHdr)-readSoFar, rd, 60000); // long enough!
                    readSoFar += rd;
                    if (sizeof(ConnectHdr) == readSoFar)
                    {
                        closedOrHandled = true;
                        // process() will remove itself from handler, and need to avoid it doing so while in 'crit'
                        // since the maintenance thread could also be tyring to manipulate handlers and calling closeIfTimedout()
                        b.leave();
                        selectHandler.process(*this);
                    }
                }
                catch (IJSOCK_Exception *e)
                {
                    exception.setown(e);
                }
                if (exception)
                    selectHandler.close(*this, exception);

                return false;
            }
        };
    private:
        // NB: Linked vs Owned, because methods will implicitly construct an object of this type
        // which can be problematic/confusing, for example if Owned and std::list->remove is called
        // with a pointer, it will auto instantiate a OWned<CSocketHandler> and cause -ve leak.
        std::list<Linked<CSocketHandler>> handlers;

        CriticalSection handlersCS;

        std::thread maintenanceThread;
        Semaphore maintenanceSem;

        void clearupSocketHandlers()
        {
            std::vector<Owned<CSocketHandler>> toClose;
            {
                cycle_t nowCycles = get_cycles_now();
                CriticalBlock b(handlersCS);
                auto it = handlers.begin();
                while (true)
                {
                    if (it == handlers.end())
                        break;
                    CSocketHandler *socketHandler = *it;
                    if (socketHandler->closeIfTimedout(nowCycles))
                    {
                        toClose.push_back(LINK(socketHandler));
                        it = handlers.erase(it);
                    }
                    else
                        ++it;
                }
            }
            for (auto &socketHandler: toClose)
            {
                try
                {
                    Owned<IJSOCK_Exception> e = createJSocketException(JSOCKERR_timeout_expired, "Connect timeout expired", __FILE__, __LINE__);
                    close(*socketHandler, e);
                }
                catch (IException *e)
                {
                    EXCLOG(e, "CConnectSelectHandler::maintenanceFunc");
                    e->Release();
                }
            }
        }
    public:
        CConnectSelectHandler(CMPConnectThread &_owner) : owner(_owner)
        {
            selectHandler.setown(createSocketSelectHandler());
            selectHandler->start();

            auto maintenanceFunc = [&]
            {
                while (owner.running)
                {
                    if (maintenanceSem.wait(10000)) // check every 10s
                        break;
                    clearupSocketHandlers();
                }
            };
            maintenanceThread = std::thread(maintenanceFunc);
        }
        ~CConnectSelectHandler()
        {
            maintenanceSem.signal();
            maintenanceThread.join();
        }
        void add(ISocket *sock)
        {
            while (true)
            {
                unsigned numHandlers;
                {
                    CriticalBlock b(handlersCS);
                    numHandlers = handlers.size();
                }
                if (numHandlers < owner.maxListenHandlerSockets)
                    break;
                DBGLOG("Too many handlers (%u), waiting for some to be processed (max limit: %u)", numHandlers, owner.maxListenHandlerSockets);
                MilliSleep(1000);
            }

            Owned<CSocketHandler> socketHandler = new CSocketHandler(*this, LINK(sock));

            size_t numHandlers;
            {
                CriticalBlock b(handlersCS);
                selectHandler->add(sock, mode, socketHandler); // NB: sock and handler linked by select handler
                handlers.emplace_back(socketHandler);
                numHandlers = handlers.size();
            }
            if (0 == (numHandlers % 100)) // for info. log at each 100 boundary
                DBGLOG("handlers = %u", (unsigned)numHandlers);
        }
        void close(CSocketHandler &socketHandler, IJSOCK_Exception *exception)
        {
            if (socketHandler.queryReadSoFar()) // read something
            {
                VStringBuffer errMsg("MP Connect Thread: invalid number of connection bytes serialized from: %s", socketHandler.queryPeerEndpointText());
                FLLOG(MCoperatorWarning, "%s", errMsg.str());
            }
            int exceptionCode = exception->errorCode();
            switch (exceptionCode)
            {
                case JSOCKERR_timeout_expired:
                    trackPortProbe(socketHandler.queryCreateTime(), socketHandler.queryPeerEndpointText(), CloseType_timeout);
                    break;
                case JSOCKERR_graceful_close:
                    trackPortProbe(socketHandler.queryCreateTime(), socketHandler.queryPeerEndpointText(), CloseType_graceful);
                    break;
                default:
                    trackPortProbe(socketHandler.queryCreateTime(), socketHandler.queryPeerEndpointText(), CloseType_error);
                    break;
            }

            Linked<CSocketHandler> handler = &socketHandler;
            {
                CriticalBlock b(handlersCS);
                selectHandler->remove(socketHandler.querySocket());
                handlers.remove(&socketHandler);
            }
            handler->querySocket()->close();
        }
        void process(CSocketHandler &socketHandler)
        {
            Linked<CSocketHandler> handler = &socketHandler;
            {
                CriticalBlock b(handlersCS);
                selectHandler->remove(socketHandler.querySocket());
                handlers.remove(&socketHandler);
            }

            if (owner.threadPool)
                owner.threadPool->start(handler.getClear());
            else
                owner.handleAcceptedSocket(handler.getClear());
        }
    };
    std::atomic<bool> running;
    bool listen;
    ISocket *listensock;
    CMPServer *parent;
    int mpSoMaxConn;
    unsigned acceptThreadPoolSize = 0;
    unsigned maxListenHandlerSockets = 60000; // what is a sensible default limit?
    Owned<IThreadPool> threadPool;

    Owned<IAllowListHandler> allowListCallback;
    void checkSelfDestruct(void *p,size32_t sz);

    Owned<ISecureSocketContext> secureContextServer;

public:
    CMPConnectThread(CMPServer *_parent, unsigned port, bool _listen);
    ~CMPConnectThread()
    {
        ::Release(listensock);
    }
    int run();
    void startPort(unsigned short port);
    void stop()
    {
        if (running)
        {
            running = false;
            listensock->cancel_accept();

            // ensure CMPConnectThread::run() has exited, and is not accepting more sockets
            if (!join(1000*60*5))   // should be pretty instant
                printf("CMPConnectThread::stop timed out\n");

            if (listen && acceptThreadPoolSize)
            {
                if (!threadPool->joinAll(true, 1000*60*5))
                    printf("CMPConnectThread::stop threadPool->joinAll timed out\n");
            }
        }
    }
    void installAllowListCallback(IAllowListHandler *_allowListCallback)
    {
        allowListCallback.set(_allowListCallback);
    }
    IAllowListHandler *queryAllowListCallback() const
    {
        return allowListCallback;
    }
    bool handleAcceptedSocket(CConnectSelectHandler::CSocketHandler *handler);
};

class PingPacketHandler;
class PingReplyPacketHandler;
class MultiPacketHandler;
class BroadcastPacketHandler;
class ForwardPacketHandler;
class UserPacketHandler;
class CMPNotifyClosedThread;

typedef SuperHashTableOf<CMPChannel,SocketEndpoint> CMPChannelHT;
class CMPServer: private CMPChannelHT, implements IMPServer
{
    byte RTsalt;
    ISocketSelectHandler        *selecthandler;
    CMPConnectThread            *connectthread;
    CBufferQueue                receiveq;
    CMPNotifyClosedThread       *notifyclosedthread;
    CriticalSection sect;
protected:
    unsigned __int64            role;
    unsigned short              port;
public:
    bool checkclosed;
    bool tryReopenChannel = false;
    bool useTLS = false;
    unsigned mpTraceLevel = 0;
    bool dumpQueue = true;

// packet handlers
    PingPacketHandler           *pingpackethandler;         // TAG_SYS_PING
    PingReplyPacketHandler      *pingreplypackethandler;    // TAG_SYS_PING_REPLY
    ForwardPacketHandler        *forwardpackethandler;      // TAG_SYS_FORWARD
    MultiPacketHandler          *multipackethandler;        // TAG_SYS_MULTI
    BroadcastPacketHandler      *broadcastpackethandler;    // TAG_SYS_BCAST
    UserPacketHandler           *userpackethandler;         // default

    IMPLEMENT_IINTERFACE_USING(CMPChannelHT);

    CMPServer(unsigned __int64 _role, unsigned _port, bool _listen);
    ~CMPServer();
    void start();
    virtual void stop();
    unsigned short getPort() const { return port; }
    unsigned __int64 getRole() const { return role; }
    void setPort(unsigned short _port) { port = _port; }
    CMPChannel *lookup(const SocketEndpoint &remoteep);
    ISocketSelectHandler &querySelectHandler() { return *selecthandler; };
    CBufferQueue &getReceiveQ() { return receiveq; }
    void checkTagOK(mptag_t tag);
    bool recv(CMessageBuffer &mbuf, const SocketEndpoint *ep, mptag_t tag, CTimeMon &tm);
    void flush(mptag_t tag);
    unsigned probe(const SocketEndpoint *ep, mptag_t tag, CTimeMon &tm, SocketEndpoint &sender);
    void cancel(const SocketEndpoint *ep, mptag_t tag);
    bool nextChannel(CMPChannel *&c);
    void addConnectionMonitor(IConnectionMonitor *monitor);
    void removeConnectionMonitor(IConnectionMonitor *monitor);
    void notifyClosed(SocketEndpoint &ep, bool trace);
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

    CriticalSection replyTagSect;
    int rettag;
    INode *myNode;

public:
    virtual mptag_t createReplyTag()
    {
        // these are short-lived so a simple increment will do (I think this is OK!)
        mptag_t ret;
        {
            CriticalBlock block(replyTagSect);
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
        flush(ret);
        return ret;
    }
    virtual ICommunicator *createCommunicator(IGroup *group, bool outer);
    virtual INode *queryMyNode()
    {
        return myNode;
    }
    virtual void setOpt(MPServerOpts opt, const char *value)
    {
        switch (opt)
        {
            case mpsopt_channelreopen:
            {
                bool tf = (nullptr != value) ? strToBool(value) : false;
                PROGLOG("Setting ChannelReopen = %s", tf ? "true" : "false");
                tryReopenChannel = tf;
                break;
            }
            default:
                // ignore
                break;
        }
    }
    virtual void installAllowListCallback(IAllowListHandler *allowListCallback) override
    {
        connectthread->installAllowListCallback(allowListCallback);
    }
    virtual IAllowListHandler *queryAllowListCallback() const override
    {
        return connectthread->queryAllowListCallback();
    }
};

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
        for (;;) {
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
                FLLOG(MCoperatorWarning, e, "MP writepacket");
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


class CMPPacketReader;

class CMPChannel: public CInterface
{
    ISocket *channelsock = nullptr;
    CMPServer *parent;
    TimedMutex sendmutex;
    Semaphore sendwaitingsig;
    unsigned sendwaiting = 0;           // number waiting on sendwaitingsem (for multi/single clashes to resolve)
    CriticalSection connectsect;
    CMPPacketReader *reader;
    bool master = false;                // i.e. connected originally
    mptag_t multitag = TAG_NULL;        // current multi send in progress
    bool closed = false;
    IArrayOf<ISocket> keptsockets;
    CriticalSection attachsect;
    unsigned __int64 attachaddrval = 0;
    SocketEndpoint attachep, attachPeerEp;
    std::atomic<unsigned> attachchk;

protected: friend class CMPServer;
    SocketEndpoint remoteep;
    SocketEndpoint localep;         // who the other end thinks I am
protected: friend class CMPPacketReader;
    unsigned lastxfer;  
#ifdef _FULLTRACE
    unsigned startxfer; 
    unsigned numiter;
#endif
    Owned<ISecureSocketContext> secureContextClient;

    bool checkReconnect(CTimeMon &tm)
    {
        if (!parent->tryReopenChannel)
            return false;
        ::Release(channelsock);
        channelsock = nullptr;
        if (connect(tm))
            return true;
        WARNLOG("Failed to reconnect");
        return false;
    }
    bool connect(CTimeMon &tm)
    {
        // must be called from connectsect
        // also in sendmutex

        Owned<ISocket> newsock;
        unsigned retrycount = CONNECT_RETRYCOUNT;
        unsigned remaining;
        Owned<IException> exitException;

        while (!channelsock)
        {
            try
            {
                StringBuffer str;
#ifdef _TRACE
                LOG(MCdebugInfo, "MP: connecting to %s role: %" I64F "u", remoteep.getEndpointHostText(str).str(), parent->getRole());
#endif
                if (((int)tm.timeout)<0)
                    remaining = CONNECT_TIMEOUT;
                else if (tm.timedout(&remaining))
                {
#ifdef _FULLTRACE
                    PROGLOG("MP: connect timed out 1");
#endif
                    return false;
                }
                if (remaining<10000)
                    remaining = 10000; // 10s min granularity for MP
                newsock.setown(ISocket::connect_timeout(remoteep,remaining));

#if defined(_USE_OPENSSL)
                if (parent->useTLS)
                {
                    Owned<ISecureSocket> ssock = secureContextClient->createSecureSocket(newsock.getClear());
                    int tlsTraceLevel = SSLogMin;
                    if (parent->mpTraceLevel >= MPVerboseMsgThreshold)
                        tlsTraceLevel = SSLogMax;
                    int status = ssock->secure_connect(tlsTraceLevel);
                    if (status < 0)
                    {
                        ssock->close();
                        exitException.setown(new CMPException(MPERR_connection_failed, remoteep));
                        throw exitException.getLink();
                    }
                    newsock.setown(ssock.getClear());
                }
#endif // OPENSSL

                newsock->set_keep_alive(true);

#ifdef _FULLTRACE
                LOG(MCdebugInfo, "MP: connect after socket connect, retrycount = %d", retrycount);
#endif

                SocketEndpoint hostep;
                hostep.setLocalHost(parent->getPort());
                ConnectHdr connectHdr(hostep, remoteep, parent->getRole());

                unsigned __int64 addrval = DIGIT1*connectHdr.id[0].ip[0] + DIGIT2*connectHdr.id[0].ip[1] + DIGIT3*connectHdr.id[0].ip[2] + DIGIT4*connectHdr.id[0].ip[3] + connectHdr.id[0].port;
#ifdef _TRACE
                PROGLOG("MP: connect addrval = %" I64F "u", addrval);
#endif

                newsock->write(&connectHdr,sizeof(connectHdr));

#ifdef _FULLTRACE
                StringBuffer tmp1;
                connectHdr.id[0].getEndpointHostText(tmp1);
                tmp1.append(' ');
                connectHdr.id[1].getEndpointHostText(tmp1);
                LOG(MCdebugInfo, "MP: connect after socket write %s",tmp1.str());
#endif

                size32_t rd = 0;

#ifdef _TRACE
                LOG(MCdebugInfo, "MP: connect after socket write, waiting for read");
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
                rd = 0;
                byte replyBuf[sizeof(size32_t)];
                size32_t totRead = 0;
                size32_t maxRead = sizeof(size32_t);
                while (loopCnt-- > 0)
                {
                    {
                        CriticalBlock block(attachsect);
#ifdef _TRACE
                        PROGLOG("MP: connect got attachsect, attachchk = %d, loopCnt = %u", attachchk.load(), loopCnt);
#endif
                        if (attachchk > 0)
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

                    try
                    {
                        // read 4 bytes, value should be sizeof(ConnectHdr.id) [12] or sizeof(connectHdr) [44] or larger (exception)
                        // if its an exception or legacy and not in allowlist the other side closes its socket after sending this msg ...

                        size32_t amtRead = 0;
                        newsock->readtms(&replyBuf[totRead], 1, maxRead, amtRead, CONNECT_TIMEOUT_INTERVAL);
                        totRead += amtRead;
                        if (totRead == sizeof(size32_t))
                        {
                            rd = totRead;
                            break;
                        }
                        maxRead -= amtRead;
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
                                return false;
                            }
#ifdef _TRACE
                            EXCLOG(e, "MP: Failed to connect");
#endif
                            if ((retrycount--==0)||(tm.timeout==MP_ASYNC_SEND))
                            {   // don't bother retrying on async send
                                e->Release();
                                throw new CMPException(MPERR_connection_failed,remoteep);
                            }

                            // if other side closes, connect again
                            if (e->errorCode() == JSOCKERR_graceful_close)
                            {
                                LOG(MCdebugInfo, "MP: Retrying (other side closed connection, probably due to clash)");
                                e->Release();
                                break;
                            }

                            e->Release();

#ifdef _TRACE
                            LOG(MCdebugInfo, "MP: Retrying connection to %s, %d attempts left",remoteep.getEndpointHostText(str).str(),retrycount+1);
#endif
                        }
                        else
                        {
                            // interval read timed out ...
                            if (0 == epStr.length())
                            {
                                SocketEndpoint ep;
                                newsock->getPeerEndpoint(ep);
                                ep.getEndpointHostText(epStr);
                            }
                            WARNLOG("MP: connect to: %s, stalled for %d ms so far", epStr.str(), msTick()-startMs);
                            e->Release();
                        }
                    }
                } // while loopcnt

                /* NB: legacy clients that don't handle the exception deserialization here
                 * will see reply as success, so no clean error,
                 * but will fail shortly afterwards since server connection is closed
                 */
                if (rd == sizeof(size32_t))
                {
                    size32_t replyVal;
                    memcpy(&replyVal, (size32_t *)replyBuf, sizeof(size32_t));
#ifdef _TRACE
                    LOG(MCdebugInfo, "MP: connect after socket read replyVal=%u, sizeof(connectHdr)=%lu", replyVal, sizeof(connectHdr));
#endif
                    if (replyVal > sizeof(ConnectHdr))
                    {
                        // read exception message ...
                        MemoryBuffer replyMb;
                        void *replyMem = replyMb.ensureCapacity(replyVal);

                        size32_t amtRead = 0;
                        try
                        {
                            newsock->readtms(replyMem, replyVal, replyVal, amtRead, CONNECT_READEXC_TIMEOUT);
                        }
                        catch (IException *e)
                        {
                            if (0 == epStr.length())
                            {
                                SocketEndpoint ep;
                                newsock->getPeerEndpoint(ep);
                                ep.getEndpointHostText(epStr);
                            }
                            StringBuffer allowExcStr;
                            exitException.setown(makeStringExceptionV(-99, "Error '%s' reading Allowlist exception from: %s", e->errorMessage(allowExcStr).str(), epStr.str()));
                            e->Release();
                            throw exitException.getLink();
                        }
                        replyMb.setLength(amtRead);
                        exitException.setown(deserializeException(replyMb));
                        throw exitException.getLink();
                    }

                    unsigned elapsedMs = msTick() - startMs;
                    if (elapsedMs >= TRACESLOW_THRESHOLD)
                    {
                        if (0 == epStr.length())
                        {
                            SocketEndpoint ep;
                            newsock->getPeerEndpoint(ep);
                            ep.getEndpointHostText(epStr);
                        }
                        WARNLOG("MP: connect to: %s, took: %d ms", epStr.str(), elapsedMs);
                    }

                    if (attachSocket(newsock,remoteep,hostep,true,NULL,addrval))
                    {
#ifdef _TRACE
                        LOG(MCdebugInfo, "MP: connected to %s",str.str());
#endif
                        lastxfer = msTick();
                        closed = false;
                        break;
                    }
                }

            }
            catch (IException *e)
            {
                if (exitException)
                    throw;
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
                LOG(MCdebugInfo, "MP: Retrying connection to %s, %d attempts left",remoteep.getEndpointHostText(str).str(),retrycount+1);
#endif
            }

            newsock.clear();

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
#ifdef _TRACELINKCLOSED
                LOG(MCdebugInfo, "WritePacket closed on entry");
                PrintStackReport();
#endif
                if (!checkReconnect(tm))
                    throw new CMPException(MPERR_link_closed,remoteep);
            }
            if (!channelsock) {
                if (!connect(tm)) {
#ifdef _FULLTRACE
                    LOG(MCdebugInfo, "WritePacket connect failed");
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
            LOG(MCdebugInfo, "WritePacket(target=%s,(%d,%d,%d))",remoteep.getEndpointHostText(ep1).str(),hdrsize,hdr2size,bodysize);
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
                LOG(MCdebugInfo, "MP Warning: WritePacket unexpected NULL socket");
                return false;
            }
            dest->write_multiple(n,bufs,sizes);  
            lastxfer = msTick();
#ifdef _FULLTRACE
            LOG(MCdebugInfo, "WritePacket(timewaiting=%d,timesending=%d)",t2-t1,lastxfer-t2);
#endif
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, e,"MP writepacket");
            closeSocket(false, true);
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
        remoteep.getEndpointHostText(ep); 
        for (;;) {
            CTimeMon pingtm(1000*60);
            if (sendPing(pingtm)) 
                break;
            {
                CriticalBlock block(connectsect);
                if (closed||!channelsock)
                    return false;
            }
            if (tm.timedout()) {
                LOG(MCdebugInfo, "MP: verify, ping failed to %s",ep.str());
                closeSocket();
                return false;
            }
            LOG(MCdebugInfo, "MP: verify, ping failed to %s, retrying",ep.str());
            unsigned remaining;
            if (!pingtm.timedout(&remaining)&&remaining) 
                Sleep(remaining);
        }
        return true;
    }


    bool send(MemoryBuffer &mb, mptag_t tag, mptag_t replytag, CTimeMon &tm, bool reply);


    void closeSocket(bool keepsocket=false, bool trace=false)
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
            channelsock = nullptr;
            {
                CriticalBlock block(attachsect);
                attachaddrval = 0;
                attachep.set(nullptr);
                attachPeerEp.set(nullptr);
                attachchk = 0;
            }
            if (!keepsocket) {
                try {
                    s->shutdown();
                }
                catch (IException *e) {
                    socketfailed = true; // ignore if the socket has been closed
                    WARNLOG("closeSocket() : Ignoring shutdown error");
                    e->Release();
                }
            }
            parent->querySelectHandler().remove(s);
        }
        parent->notifyClosed(remoteep, trace);
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
        return remoteep.getEndpointHostText(s);
    }

    bool isClosed()
    {
        return closed;
    }

    bool isConnected()
    {
        return !closed&&(channelsock!=NULL);
    }

    const SocketEndpoint &queryPeerEp() const { return attachPeerEp; }
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
        LOG(MCdebugInfo, "MP: send(target=%s,sender=%s,tag=%d,replytag=%d,size=%d)",hdr.target.getEndpointHostText(ep1).str(),hdr.sender.getEndpointHostText(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
#endif
        return channel->writepacket(&hdr,sizeof(hdr),mb.toByteArray(),mb.length(),tm);
    }
};

class PingPacketHandler // TAG_SYS_PING
{
public:
    void handle(CMPChannel *channel,bool identifyself)
    {
        channel->sendPingReply(PING_CONFIRM_TIMEOUT, identifyself);
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
            msg.getSender().getEndpointHostText(errorMsg).newline();
            errorMsg.append("This header: ");
            mhdr.getDetails(errorMsg).newline();
            if (otherMhdr)
            {
                errorMsg.append("Other header: ");
                otherMhdr->getDetails(errorMsg).newline();
            }
            msg.getDetails(errorMsg);
            LOG(MCerror, "MultiPacketHandler: protocol error (%d) %s", code, errorMsg.str());
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
    bool send(CMPChannel *channel,PacketHeader &hdr,MemoryBuffer &mb, CTimeMon &tm, TimedMutex &sendmutex)
    {
        // must not adjust mb
#ifdef _FULLTRACE
        StringBuffer ep1;
        StringBuffer ep2;
        LOG(MCdebugInfo, "MP: multi-send(target=%s,sender=%s,tag=%d,replytag=%d,size=%d)",hdr.target.getEndpointHostText(ep1).str(),hdr.sender.getEndpointHostText(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
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
        for (;;) {
            if (i+1==mhdr.numparts) 
                mhdr.size = mhdr.total-mhdr.ofs;
#ifdef _FULLTRACE
            LOG(MCdebugInfo, "MP: multi-send block=%d, num blocks=%d, ofs=%d, size=%d",i,mhdr.numparts,mhdr.ofs,mhdr.size);
#endif
            outhdr.initseq();
            outhdr.size = sizeof(outhdr)+sizeof(mhdr)+mhdr.size;
            if (!channel->writepacket(&outhdr,sizeof(outhdr),&mhdr,sizeof(mhdr),p,mhdr.size,tm)) {
#ifdef _FULLTRACE
                LOG(MCdebugInfo, "MP: multi-send failed");
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


static PeriodicTimer periodicTimer(10*60*1000, false); // 10 minutes
static std::atomic<__uint64> mpProtocolErrors{0};
// --------------------------------------------------------

class CMPPacketReader: public ISocketSelectNotify, public CInterface
{
    CMessageBuffer *activemsg = nullptr;
    byte * activeptr = nullptr;
    size32_t remaining = 0;
    CMPChannel *parent = nullptr;
    CriticalSection sect;
    bool gotPacketHdr = false;
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

    bool notifySelected(ISocket *sock, unsigned selected)
    {
        if (!parent)
            return false;
        bool closeSocket = false; // if a graceful close is hit, this will be set and will fall through to close socket
        bool suppressException = false;
        try
        {
            while (true) // NB: breaks out if blocked (if (remaining) ..)
            {
                // try and mop up all data on socket
                parent->lastxfer = msTick();
#ifdef _FULLTRACE
                parent->numiter++;
#endif
                if (!activemsg) // no message in progress
                {
#ifdef _FULLTRACE
                    parent->numiter = 1;
                    parent->startxfer = msTick();
#endif
                    remaining = sizeof(PacketHeader);
                    gotPacketHdr = false;
                    activemsg = new CMessageBuffer(remaining);
                    activeptr = (byte *)activemsg->bufferBase();
                }
                size32_t szRead;
                if (!gotPacketHdr)
                {
                    CCycleTimer timer;
                    closeSocket = readtmsAllowClose(sock, activeptr, 0, remaining, szRead, timer.remainingMs(60000));
                    remaining -= szRead;
                    activeptr += szRead;
                    if (remaining) // only possible if blocked.
                        break; // wait for next notification

                    PacketHeader &hdr = *(PacketHeader *)activemsg->bufferBase();
                    if (hdr.version/0x100 != MP_PROTOCOL_VERSION/0x100)
                    {
                        // TBD IPV6 here
                        mpProtocolErrors++;
                        SocketEndpoint ep;
                        sock->getPeerEndpoint(ep);
                        if (periodicTimer.hasElapsed())
                        {
                            VStringBuffer packetHdrBytes("[%" I64F "u incidents to date]. Packet Header: ", mpProtocolErrors.load());
                            hexdump2string((byte const *)&hdr, sizeof(hdr), packetHdrBytes);
                            throw new CMPException(MPERR_protocol_version_mismatch, ep, packetHdrBytes.str());
                        }
                        else
                        {
                            suppressException = true;
                            throw new CMPException(MPERR_protocol_version_mismatch, ep);
                        }
                    }
                    hdr.setMessageFields(*activemsg);
    #ifdef _FULLTRACE
                    StringBuffer ep1;
                    StringBuffer ep2;
                    LOG(MCdebugInfo, "MP: ReadPacket(sender=%s,target=%s,tag=%d,replytag=%d,size=%d)",hdr.sender.getEndpointHostText(ep1).str(),hdr.target.getEndpointHostText(ep2).str(),hdr.tag,hdr.replytag,hdr.size);
    #endif
                    remaining = hdr.size-sizeof(hdr);
                    activeptr = (byte *)activemsg->clear().reserveTruncate(remaining);
                    gotPacketHdr = true;
                }

                if (!closeSocket && remaining)
                {
                    closeSocket = readtmsAllowClose(sock, activeptr, 0, remaining, szRead, WAIT_FOREVER);
                    remaining -= szRead;
                    activeptr += szRead;
                }
                if (remaining) // only possible if blocked.
                    break; // wait for next notification

#ifdef _FULLTRACE
                LOG(MCdebugInfo, "MP: ReadPacket(timetaken = %d,select iterations=%d)",msTick()-parent->startxfer,parent->numiter);
#endif
                do
                {
                    switch (activemsg->getTag())
                    {
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
                }
                while (activemsg);
                if (closeSocket)
                    break;
            }
        }
        catch (IException *e)
        {
            if (!suppressException && e->errorCode()!=JSOCKERR_graceful_close)
                FLLOG(MCoperatorWarning, e, "MP(Packet Reader)");
            e->Release();
            closeSocket = true; // NB: this select handler will removed and not be notified again
        }

        if (closeSocket)
        {
            // here due to error or graceful close, so close socket (ignore error as may be closed already)
            try
            {
                Linked<CMPChannel> pc;
                {
                    CriticalBlock block(sect);
                    if (parent)
                    {
                        pc.set(parent);     // don't want channel to disappear during call
                        parent = NULL;
                    }
                }
                if (pc)
                    pc->closeSocket(false, true);
            }
            catch (IException *e)
            {
                e->Release();
            }
        }
        return false;
    }
};


CMPChannel::CMPChannel(CMPServer *_parent,SocketEndpoint &_remoteep) : parent(_parent), remoteep(_remoteep)
{
    localep.set(parent->getPort());
    reader = new CMPPacketReader(this);
    attachep.set(nullptr);
    attachchk = 0;
    lastxfer = msTick();

#if defined(_USE_OPENSSL)
    if (parent->useTLS)
        secureContextClient.setown(createSecureSocketContextSecret("local", ClientSocket));
#endif
}

void CMPChannel::reset()
{
    reader->shutdown(); // clear as early as possible
    closeSocket(false, true);
    reader->Release();
    channelsock = nullptr;
    multitag = TAG_NULL;
    reader = new CMPPacketReader(this);
    closed = false;
    master = false;
    sendwaiting = 0;
    attachaddrval = 0;
    attachep.set(nullptr);
    attachPeerEp.set(nullptr);
    attachchk = 0;
    lastxfer = msTick();
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
        std::atomic<unsigned> &attchk;
        attachdTor(std::atomic<unsigned> &_attchk) : attchk(_attchk) { }
        ~attachdTor() { --attchk; }
    } attachChk (attachchk);

#ifdef _FULLTRACE       
    PROGLOG("MP: attachSocket on entry, ismaster = %d, confirm = %p, channelsock = %p, addrval = %" I64F "u", ismaster, confirm, channelsock, addrval);
#endif

    {
        CriticalBlock block(attachsect);
        attachaddrval = addrval;
        attachep = _remoteep;
        if (newsock)
            newsock->getPeerEndpoint(attachPeerEp);
        ++attachchk;
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
        _localep.getEndpointHostText(ep1);
        _remoteep.getEndpointHostText(ep2);
        LOG(MCdebugInfo, "MP: Possible clash between %s->%s %d(%d)",ep1.str(),ep2.str(),(int)ismaster,(int)master);

        try {
            if (ismaster!=master) {
                if (ismaster) {
                    LOG(MCdebugInfo, "MP: resolving socket attach clash (master)");
                    return false;
                }
                else {
                    Sleep(50);  // give the other side some time to close
                    CTimeMon tm(10000);
                    if (verifyConnection(tm,false)) {
                        LOG(MCdebugInfo, "MP: resolving socket attach clash (verified)");
                        return false;
                    }
                }
            }
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, e,"MP attachsocket(1)");
            e->Release();
        }
        try {
            LOG(MCdebugInfo, "Message Passing - removing stale socket to %s",ep2.str());
            CriticalUnblock unblock(connectsect);
            closeSocket(true, true);
#ifdef REFUSE_STALE_CONNECTION
            if (!ismaster)
                return false;
#endif
            Sleep(100); // pause to allow close socket triggers to run
        }
        catch (IException *e) {
            FLLOG(MCoperatorWarning, e,"MP attachsocket(2)");
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

    parent->querySelectHandler().add(channelsock, SELECTMODE_READ, reader);

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
    size32_t msgsize = mb.length();
    PacketHeader hdr(msgsize+sizeof(PacketHeader),localep,remoteep,tag,replytag);
    if (closed||(reply&&!isConnected()))  // flag error if has been disconnected
    {
#ifdef _TRACELINKCLOSED
        LOG(MCdebugInfo, "CMPChannel::send closed on entry %d",(int)closed);
        PrintStackReport();
#endif
        if (!checkReconnect(tm))
            throw new CMPException(MPERR_link_closed,remoteep);
    }

    bool ismulti = (msgsize>MAXDATAPERPACKET);
    // pre-condition - ensure no clashes
    for (;;)
    {
        sendmutex.lock();
        if (ismulti)
        {
            if (multitag==TAG_NULL)     // don't want to interleave with other multi send
            {
                multitag = tag;
                break;
            }
        }
        else if (multitag!=tag)         // don't want to interleave with another of same tag
            break;

        /* NB: block clashing multi packet sends until current one is done,
         * but note that the multipackethandler-send() temporarily releases the sendmutex,
         * between packets, to allow other tags to interleave
         */
        sendwaiting++;
        sendmutex.unlock();
        sendwaitingsig.wait();
    }


    struct Cpostcondition // can we start using eiffel
    {
        TimedMutex &sendmutex;
        unsigned &sendwaiting;
        Semaphore &sendwaitingsig;
        mptag_t *multitag;

        Cpostcondition(TimedMutex &_sendmutex,unsigned &_sendwaiting,Semaphore &_sendwaitingsig,mptag_t *_multitag)
            : sendmutex(_sendmutex),sendwaiting(_sendwaiting),sendwaitingsig(_sendwaitingsig)
        {
            multitag = _multitag;
        }

        ~Cpostcondition() 
        { 
            if (multitag)
                *multitag = TAG_NULL;
            if (sendwaiting)
            {
                sendwaitingsig.signal(sendwaiting);
                sendwaiting = 0;
            }
            sendmutex.unlock();
        }
    } postcond(sendmutex, sendwaiting, sendwaitingsig, (ismulti && (multitag != TAG_NULL)) ? &multitag : nullptr);

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
        FLLOG(MCoperatorWarning, e,"MP ping(1)");
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
        FLLOG(MCoperatorWarning, e,"MP ping reply(1)");
        e->Release();
        ret = false;
    }
    sendmutex.unlock();
    return ret;
}

static constexpr unsigned defaultAcceptThreadPoolSize = 100;
// --------------------------------------------------------
CMPConnectThread::CMPConnectThread(CMPServer *_parent, unsigned port, bool _listen)
    : Thread("MP Connection Thread")
{
    parent = _parent;
    listen = _listen;
    mpSoMaxConn = 0;
#ifndef _CONTAINERIZED
    Owned<IPropertyTree> env = getHPCCEnvironment();
    if (env)
    {
        if (listen)
        {
            mpSoMaxConn = env->getPropInt("EnvSettings/mpSoMaxConn", 0);
            if (!mpSoMaxConn)
                mpSoMaxConn = env->getPropInt("EnvSettings/ports/mpSoMaxConn", 0);
            acceptThreadPoolSize = env->getPropInt("EnvSettings/acceptThreadPoolSize", defaultAcceptThreadPoolSize);
        }
        unsigned mpTraceLevel = env->getPropInt("EnvSettings/mpTraceLevel", 0);
        switch (mpTraceLevel)
        {
            case 0:
                parent->mpTraceLevel = InfoMsgThreshold;
                break;
            case 1:
                parent->mpTraceLevel = DebugMsgThreshold;
                break;
            case 2:
                parent->mpTraceLevel = ExtraneousMsgThreshold;
                break;
            default:
                parent->mpTraceLevel = MPVerboseMsgThreshold;
                break;
        }
    }
#else
    parent->mpTraceLevel = getComponentConfigSP()->getPropInt("logging/@detail", InfoMsgThreshold);
    if (listen)
    {
        mpSoMaxConn = getConfigInt("expert/@mpSoMaxConn", 0);
        acceptThreadPoolSize = getConfigInt("expert/@acceptThreadPoolSize", defaultAcceptThreadPoolSize);
    }
#endif

    if (mpSoMaxConn)
    {
        int kernSoMaxConn = 0;
        bool soMaxCheck = check_somaxconn(&kernSoMaxConn);
        if (soMaxCheck && (mpSoMaxConn > kernSoMaxConn))
            WARNLOG("MP: kernel listen queue backlog setting (somaxconn=%d) is lower than environment mpSoMaxConn (%d) setting and should be increased", kernSoMaxConn, mpSoMaxConn);
    }
    if (!mpSoMaxConn && listen)
        mpSoMaxConn = DEFAULT_LISTEN_QUEUE_SIZE;
    if (!port)
    {
        // need to connect early to resolve clash
        unsigned minPort, maxPort;
        minPort = MP_START_PORT;
        maxPort = MP_END_PORT;
#ifndef _CONTAINERIZED
        if (env)
        {
            minPort = env->getPropInt("EnvSettings/mpStart", 0);
            if (!minPort)
                minPort = env->getPropInt("EnvSettings/ports/mpStart", MP_START_PORT);
            maxPort = env->getPropInt("EnvSettings/mpEnd", 0);
            if (!maxPort)
                maxPort = env->getPropInt("EnvSettings/ports/mpEnd", MP_END_PORT);
        }
#endif
        assertex(maxPort >= minPort);
        Owned<IJSOCK_Exception> lastErr;
        // mck - if not listening then could ignore port range and
        //       let OS select an unused port ...
        unsigned numPorts = maxPort - minPort + 1;
        for (unsigned retries = 0; retries < numPorts * 3; retries++)
        {
            port = minPort + getRandom() % numPorts;
            try
            {
                listensock = ISocket::create(port, mpSoMaxConn);
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
    LOG(MCdebugInfo, "MP Connect Thread Init Port = %d", port);
#endif
    running = false;

#if defined(_USE_OPENSSL)
    if (parent->useTLS)
        secureContextServer.setown(createSecureSocketContextSecretSrv("local", nullptr, true));
#endif
    PROGLOG("MP TLS: %s acceptThreadPoolSize: %u", parent->useTLS ? "on" : "off", acceptThreadPoolSize);
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
            shutdownAndCloseNoThrow(listensock);
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

void CMPConnectThread::startPort(unsigned short port)
{
    if (!listensock)
        listensock = ISocket::create(port, mpSoMaxConn);
    if (!listen)
        return;
    running = true;
    if (acceptThreadPoolSize)
    {
        class CMPConnectThreadFactory : public CInterfaceOf<IThreadFactory>
        {
            CMPConnectThread &owner;
        public:
            CMPConnectThreadFactory(CMPConnectThread &_owner) : owner(_owner)
            {
            }
        // IThreadFactory
            IPooledThread *createNew() override
            {
                class CMPConnectionThread : public CInterfaceOf<IPooledThread>
                {
                    CMPConnectThread &owner;
                    Owned<CConnectSelectHandler::CSocketHandler> handler;
                public:
                    CMPConnectionThread(CMPConnectThread &_owner) : owner(_owner)
                    {
                    }
                // IPooledThread
                    virtual void init(void *param) override
                    {
                        handler.setown((CConnectSelectHandler::CSocketHandler *)param);
                    }
                    virtual void threadmain() override
                    {
                        owner.handleAcceptedSocket(handler.getClear());
                    }
                    virtual bool stop() override
                    {
                        return true;
                    }
                    virtual bool canReuse() const override
                    {
                        return true;
                    }
                };
                return new CMPConnectionThread(owner);
            }
        };
        Owned<IThreadFactory> factory = new CMPConnectThreadFactory(*this);
        threadPool.setown(createThreadPool("MPConnectPool", factory, false, nullptr, acceptThreadPoolSize, INFINITE));
    }
    Thread::start(false);
}

bool CMPConnectThread::handleAcceptedSocket(CConnectSelectHandler::CSocketHandler *_handler)
{
    Owned<CConnectSelectHandler::CSocketHandler> handler = _handler;
    ISocket *sock = handler->querySocket();
    ConnectHdr &connectHdr = handler->queryHdr();
    try
    {
        if (allowListCallback)
        {
            StringBuffer responseText; // filled if denied, NB: if amount sent is > sizeof(ConnectHdr) we can differentiate exception from success
            if (!allowListCallback->isAllowListed(handler->queryPeerHostText(), connectHdr.getRole(), &responseText))
            {
                Owned<IException> e = makeStringException(-1, responseText);
                OWARNLOG(e, nullptr);

                // NB: from 9.6 legacy clients are no longer supported (legacy clients in this context are older than 7.4.2)
                MemoryBuffer mb;
                DelayedSizeMarker marker(mb);
                serializeException(e, mb);
                marker.write();
                sock->write(mb.toByteArray(), mb.length());

                sock->close();
                return false; // did not timeout
            }
        }

        SocketEndpoint _remoteep;
        SocketEndpoint hostep;
        connectHdr.id[0].get(_remoteep);
        connectHdr.id[1].get(hostep);

        unsigned __int64 addrval = DIGIT1*connectHdr.id[0].ip[0] + DIGIT2*connectHdr.id[0].ip[1] + DIGIT3*connectHdr.id[0].ip[2] + DIGIT4*connectHdr.id[0].ip[3] + connectHdr.id[0].port;
#ifdef _TRACE
        PROGLOG("MP: Connect Thread: addrval = %" I64F "u", addrval);
#endif

        if (_remoteep.isNull() || hostep.isNull())
        {
            StringBuffer errMsg;
            SocketEndpointV4 zeroTest[2];
            memset(zeroTest, 0x0, sizeof(zeroTest));
            if (memcmp(connectHdr.id, zeroTest, sizeof(connectHdr.id)))
            {
                // JCSMORE, I think _remoteep really must/should match a IP of this local host
                errMsg.appendf("MP Connect Thread: invalid remote and/or host ep serialized from %s", handler->queryPeerEndpointText());
                FLLOG(MCoperatorWarning, "%s", errMsg.str());
            }
            else if (parent->mpTraceLevel >= MPVerboseMsgThreshold)
            {
                // all zeros msg received
                errMsg.appendf("MP Connect Thread: connect with empty msg received, assumed port monitor check from %s", handler->queryPeerEndpointText());
                PROGLOG("%s", errMsg.str());
            }
            sock->close();
            return false;
        }
#ifdef _FULLTRACE       
        StringBuffer tmp1;
        _remoteep.getEndpointHostText(tmp1);
        tmp1.append(' ');
        hostep.getEndpointHostText(tmp1);
        PROGLOG("MP: Connect Thread: after read %s",tmp1.str());
#endif
        checkSelfDestruct(&connectHdr.id[0],sizeof(connectHdr.id));
        Owned<CMPChannel> channel = parent->lookup(_remoteep);
        size32_t rd = sizeof(connectHdr);
        if (!channel->attachSocket(sock,_remoteep,hostep,false,&rd,addrval))
        {
#ifdef _FULLTRACE       
            PROGLOG("MP Connect Thread: lookup failed");
#endif
        }
        else
        {
#ifdef _TRACE
            StringBuffer str1;
            StringBuffer str2;
            LOG(MCdebugInfo, "MP Connect Thread: connected to %s",_remoteep.getEndpointHostText(str1).str());
#endif
        }
#ifdef _FULLTRACE       
        PROGLOG("MP: Connect Thread: after write");
#endif
    }
    catch (IException *e)
    {
        FLLOG(MCoperatorWarning, e,"MP Connect Thread: Failed to make connection(1)");
        sock->close();
        e->Release();
    }
    return false;
}

int CMPConnectThread::run()
{
#ifdef _TRACE
    LOG(MCdebugInfo, "MP: Connect Thread Starting - accept loop");
#endif
    Owned<IException> exception;

    CConnectSelectHandler connectSelectHandler(*this);
    while (running)
    {
        Owned<ISocket> sock;
        try
        {
            sock.setown(listensock->accept(true));
        }
        catch (IException *e)
        {
            exception.setown(e);
        }
        if (sock)
        {
#if defined(_USE_OPENSSL)
            if (parent->useTLS)
            {
                Owned<ISecureSocket> ssock = secureContextServer->createSecureSocket(sock.getClear());
                int tlsTraceLevel = SSLogMin;
                if (parent->mpTraceLevel >= MPVerboseMsgThreshold)
                    tlsTraceLevel = SSLogMax;
                int status = ssock->secure_accept(tlsTraceLevel);
                if (status < 0)
                {
                    ssock->close();
                    PROGLOG("MP Connect Thread: failed to accept secure connection");
                    continue;
                }
                sock.setown(ssock.getClear());
            }
#endif // OPENSSL

#ifdef _FULLTRACE
            StringBuffer s;
            SocketEndpoint ep1;
            sock->getPeerEndpoint(ep1);
            PROGLOG("MP: Connect Thread: socket accepted from %s",ep1.getEndpointHostText(s).str());
#endif
            sock->set_keep_alive(true);

            // NB: creates a CSocketHandler that is added to the select handler.
            // it will manage the handling of the incoming ConnectHdr header only.
            // After that, the socket will be removed from the connectSelectHamndler,
            // a CMPChannel will be estalbished, and the socket will be added to the MP CMPPacketReader select handler.
            // See handleAcceptedSocket.
            connectSelectHandler.add(sock);
        }
        else
        {
            if (running)
            {
                if (exception)
                {
                    constexpr unsigned sleepSecs = 5;
                    // Log and pause for a few seconds, because accept loop may have failed due to handle exhaustion.
                    VStringBuffer msg("MP accept failed. Accept loop will be paused for %u seconds", sleepSecs);
                    EXCLOG(exception, msg.str());
                    exception.clear();
                    MilliSleep(sleepSecs * 1000);
                }
                else // not sure this can ever happen (no exception, still running, and sock==nullptr)
                    LOG(MCdebugInfo, "MP Connect Thread accept returned NULL");
            }
        }
    }
#ifdef _TRACE
    LOG(MCdebugInfo, "MP Connect Thread Stopping");
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

CMPChannel *CMPServer::lookup(const SocketEndpoint &endpoint)
{
    // there is an assumption here that no removes will be done within this loop
    CriticalBlock block(sect);
    SocketEndpoint ep = endpoint;
    CMPChannel *e=find(ep);
    // Check for freed channels
    if (e&&e->isClosed()&&(msTick()-e->lastxfer>30*1000))
        e->reset();
    if (checkclosed) {
        checkclosed = false;
        CMPChannel *c = NULL;
        for (;;) {
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
    return LINK(e);
}


CMPServer::CMPServer(unsigned __int64 _role, unsigned _port, bool _listen)
{
    RTsalt=0xff;
    role = _role;
    port = 0;   // connectthread tells me what port it actually connected on
    checkclosed = false;
    useTLS = queryMtls();

    // If !_listen, CMPConnectThread binds a port but does not actually start
    // running, it is used as a unique IP:port required in MP INode/IGroup internals
    // for MP clients that do not need to accept connections.

    connectthread = new CMPConnectThread(this, _port, _listen);
    selecthandler = createSocketSelectHandler();
    pingpackethandler = new PingPacketHandler;              // TAG_SYS_PING
    pingreplypackethandler = new PingReplyPacketHandler;    // TAG_SYS_PING_REPLY
    forwardpackethandler = new ForwardPacketHandler;        // TAG_SYS_FORWARD
    multipackethandler = new MultiPacketHandler;            // TAG_SYS_MULTI
    broadcastpackethandler = new BroadcastPacketHandler;    // TAG_SYS_BCAST
    userpackethandler = new UserPacketHandler(this);        // default
    notifyclosedthread = new CMPNotifyClosedThread(this);
    notifyclosedthread->start(false);
    selecthandler->start();
    rettag = (int)TAG_REPLY_BASE; // NB negative

    SocketEndpoint ep(port); // NB port set by connectthread constructor
    myNode = createINode(ep);
}

CMPServer::~CMPServer()
{
#ifdef _TRACEORPHANS
    if (dumpQueue)
    {
        StringBuffer buf;
        getReceiveQueueDetails(buf);
        if (buf.length())
            LOG(MCdebugInfo, "MP: Orphan check\n%s",buf.str());
    }
#endif
    _releaseAll();
    selecthandler->stop(true);
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
    ::Release(myNode);
}

void CMPServer::checkTagOK(mptag_t tag)
{
    if ((int)tag<=(int)TAG_REPLY_BASE) {
        int dif = (int)TAG_REPLY_BASE-(int)tag;
        if (dif%16!=RTsalt) {
            ERRLOG("**Invalid MP tag used");
            PrintStackReport();
        }
    }
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
                const SocketEndpoint &senderep = msg->getSender();
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
#ifdef _TRACELINKCLOSED
        LOG(MCdebugInfo, "CMPserver::recv closed on notify");
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
        DBGLOG("CMPServer::flush(%d) discarded %u buffers",(int)tag,count);
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
#ifdef _TRACELINKCLOSED
        LOG(MCdebugInfo, "CMPserver::probe closed on notify");
        PrintStackReport();
#endif
        IMP_Exception *e=new CMPException(MPERR_link_closed,*ep);
        throw e;
    }
    return 0;
}

void CMPServer::start()
{
    connectthread->startPort(getPort());
}

void CMPServer::stop()
{
    selecthandler->stop(true);
    connectthread->stop();
    CMPChannel *c = NULL;
    for (;;) {
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
    CriticalBlock block(sect);
    cur = (CMPChannel *)SuperHashTableOf<CMPChannel,SocketEndpoint>::next(cur);
    return cur!=NULL;
}

void CMPServer::notifyClosed(SocketEndpoint &ep, bool trace)
{
#ifdef _TRACEMPSERVERNOTIFYCLOSED
    if (trace)
    {
        StringBuffer url;
        LOG(MCdebugInfo, "MP: CMPServer::notifyClosed %s",ep.getEndpointHostText(url).str());
        PrintStackReport();
    }
#endif
    notifyclosedthread->notify(ep);
}

// --------------------------------------------------------


class CInterCommunicator: public IInterCommunicator, public CInterface
{
    CMPServer *parent;
    CriticalSection verifysect;

public:
    IMPLEMENT_IINTERFACE;

    bool send (CMessageBuffer &mbuf, INode *dst, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER)
    {
        if (!dst)
            return false;
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
        Owned<CMPChannel> channel = parent->lookup(dst->endpoint());
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        if (!channel->send(mbuf,tag,mbuf.getReplyTag(),tm,false))
            return false;
        mbuf.clear(); // for consistent semantics
        return true;
    }

    bool verifyConnection(INode *node, unsigned timeout)
    {
        CriticalBlock block(verifysect);
        CTimeMon tm(timeout);
        Owned<CMPChannel> channel = parent->lookup(node->endpoint());
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        return channel->verifyConnection(tm,true);
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
        rank_t myrank = group->rank(parent->queryMyNode());
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
                    Owned<CMPChannel> channel = parent->lookup(group->queryNode(rank).endpoint());
                    unsigned remaining;
                    if (tm.timedout(&remaining)) {
                        return false;
                    }
                    if (!channel->verifyConnection(tm,true)) {
                        return false;
                    }
                }
            }
        }
        if (!duplex) {
            ForEachNodeInGroup(rank,*group) {
                bool doverify = ((rank&1)==(myrank&1))?(myrank<rank):(myrank>rank);
                if (doverify) {
                    Owned<CMPChannel> channel = parent->lookup(group->queryNode(rank).endpoint());
                    while (!channel->verifyConnection(tm,false)) {
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
        for (;;)
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
        mptag_t replytag = parent->createReplyTag();
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
        Owned<CMPChannel> channel = parent->lookup(node->endpoint());
        channel->closeSocket();
        parent->removeChannel(channel);
    }

    CInterCommunicator(CMPServer *_parent)
    {
        parent = _parent;
    }
    ~CInterCommunicator()
    {
    }

};




class CCommunicator: public ICommunicator, public CInterface
{
    IGroup *group;
    CMPServer *parent;
    bool outer;
    rank_t myrank;
    CriticalSection verifysect;

    const SocketEndpoint &queryEndpoint(rank_t rank)
    {
        return group->queryNode(rank).endpoint();
    }

    CMPChannel *getChannel(rank_t rank)
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
        if (dstrank==myrank) {
            CMessageBuffer *msg = mbuf.clone();
            // change sender
            msg->init(parent->queryMyNode()->endpoint(),tag,mbuf.getReplyTag());
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
                    Owned<CMPChannel> channel = getChannel(dstrank);
                    unsigned remaining;
                    if (tm.timedout(&remaining))
                        return false;
                    if (!channel->send(mbuf,tag,mbuf.getReplyTag(),tm,false))
                        return false;
                }
            }
        }
        return true;
    }

    void barrier(void)
    {
#ifdef _TRACE
        DBGLOG("MP: barrier enter");
#endif

        /*
         * Use the dissemination algorithm described in:
         * Debra Hensgen, Raphael Finkel, and Udi Manbet, "Two Algorithms for Barrier Synchronization,"
         * International Journal of Parallel Programming, 17(1):1-17, 1988.
         * It uses ceiling(lgp) steps. In step k, 0 <= k <= (ceiling(lgp)-1),
         * process i sends to process (i + 2^k) % p and receives from process (i - 2^k + p) % p.
         */

        int numranks = group->ordinality();
        CMessageBuffer mb;
        rank_t r;

        int mask = 0x1;
        while (mask < numranks)
        {
            int dst = (myrank + mask) % numranks;
            int src = (myrank - mask + numranks) % numranks;

#ifdef _TRACE
            DBGLOG("MP: barrier: send to %d, recv from %d", dst, src);
#endif

            // NOTE: MPI method MUST use sendrecv so as to not send/recv deadlock ...

            mb.clear();
            mb.append("MPTAG_BARRIER");
            bool oks = send(mb,dst,MPTAG_BARRIER,120000);
            mb.clear();
            bool okr = recv(mb,src,MPTAG_BARRIER,&r);

            if (!oks && !okr)
            {
                DBGLOG("MP: barrier: Error sending or recving");
                break;
            }

            mask <<= 1;
        }

#ifdef _TRACE
        DBGLOG("MP: barrier leave");
#endif
    }

    bool verifyConnection(rank_t rank,  unsigned timeout, bool allowConnect=true)
    {
        CriticalBlock block(verifysect);
        assertex(rank!=RANK_RANDOM);
        assertex(rank!=RANK_ALL);
        CTimeMon tm(timeout);
        Owned<CMPChannel> channel = getChannel(rank);
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        return channel->verifyConnection(tm,allowConnect);
    }

    bool verifyAll(bool duplex, unsigned totalTimeout, unsigned perConnectionTimeout)
    {
        CriticalBlock block(verifysect);
        CTimeMon totalTM(totalTimeout);

        Semaphore sem;
        sem.signal(getAffinityCpus());
        std::atomic<bool> abort{false};
        
        auto verifyConnWithConnect = [&](unsigned rank, unsigned timeout)
        {
            CTimeMon tm(timeout);
            Owned<CMPChannel> channel = getChannel(rank);
            return channel->verifyConnection(tm, true);    
        };

        auto verifyConnWithoutConnect = [&](unsigned rank, unsigned timeout)
        {
            CTimeMon tm(timeout);
            while (true)
            {
                Owned<CMPChannel> channel = getChannel(rank);
                if (channel->verifyConnection(tm, false))
                    return true;
                if (abort || tm.timedout())
                    return false;
                Sleep(100);
            }
        };

        auto threadedVerifyConnectFunc = [&](rank_t rank, std::function<bool (unsigned rank, unsigned timeout)> connectFunc)
        {
            // NB: running because took (via wait()) a semaphore slot, restore it at end of scope
            struct RestoreSlot
            {
                Semaphore &sem;
                RestoreSlot(Semaphore &_sem) : sem(_sem) { }
                ~RestoreSlot() { sem.signal(); }
            } restoreSlot(sem);

            unsigned timeoutMs;
            if (totalTM.timedout(&timeoutMs) || abort)
                return false;
            if (perConnectionTimeout && (perConnectionTimeout < timeoutMs))
                timeoutMs = perConnectionTimeout;

            if (!connectFunc(rank, timeoutMs))
            {
                abort = true; // ensure verifyFunc knows before release slot, to prevent other thread being launched
                return false;
            }
            return true;
        };

        auto verifyFunc = [&](std::function<bool (unsigned rank)> isRankToVerifyFunc, std::function<bool (unsigned rank, unsigned timeout)> connectFunc)
        {
            std::vector<std::future<bool>> results;
            for (rank_t rank=0; rank<group->ordinality(); rank++)
            {
                if (isRankToVerifyFunc(rank))
                {
                    // check timeout before and after sem.wait
                    // NB: sem.wait if successful, takes a slot which is restored by the thread when it is done
                    unsigned remaining;
                    if (totalTM.timedout(&remaining) || !sem.wait(remaining) || totalTM.timedout(&remaining))
                    {
                        abort = true;
                        break;
                    }
                    else if (abort)
                        break;
                    results.push_back(std::async(std::launch::async, threadedVerifyConnectFunc, rank, connectFunc));
                }
            }
            bool res = true;
            for (auto &f: results)
            {
                if (!f.get())
                    res = false;
            }
            return res && !abort;
        };

        if (duplex)
            return verifyFunc([this](rank_t rank) { return rank != myrank; }, verifyConnWithConnect);
        else
        {
            if (!verifyFunc([this](rank_t rank) { return ((rank&1)==(myrank&1)) ? (myrank > rank) : (myrank < rank); }, verifyConnWithConnect))
                return false;

            return verifyFunc([this](rank_t rank) { return ((rank&1)==(myrank&1)) ? (myrank < rank) : (myrank > rank); }, verifyConnWithoutConnect);
        }
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
        for (;;)
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
                } while (sendrank==myrank);
            }
            else {
                assertex(myrank!=0);
                sendrank = 0;
            }
        }
        mptag_t replytag = parent->createReplyTag();
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
        Owned<CMPChannel> channel = parent->lookup(mbuf.getSender());
        unsigned remaining;
        if (tm.timedout(&remaining)) {
            return false;
        }
        if (channel->send(mbuf,replytag,TAG_NULL,tm, true)) {
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
        Owned<CMPChannel> channel = parent->lookup(node->endpoint());
        channel->closeSocket();
        parent->removeChannel(channel);
    }

    virtual const SocketEndpoint &queryChannelPeerEndpoint(const SocketEndpoint &sender) const override
    {
        Owned<CMPChannel> channel = parent->lookup(sender);
        assertex(channel);
        return channel->queryPeerEp();
    }

    CCommunicator(CMPServer *_parent,IGroup *_group, bool _outer)
    {
        outer = _outer;
        parent = _parent;
        group = LINK(_group); 
        myrank = group->rank(parent->queryMyNode());
    }
    ~CCommunicator()
    {
        group->Release();
    }
};


// Additional CMPServer methods

ICommunicator *CMPServer::createCommunicator(IGroup *group, bool outer)
{
    return new CCommunicator(this,group,outer);
}

///////////////////////////////////

IMPServer *startNewMPServer(unsigned port, bool listen)
{
    assertex(sizeof(PacketHeader)==32);
    CMPServer *mpServer = new CMPServer(0, port, listen);
    mpServer->start();
    return mpServer;
}


class CGlobalMPServer : public CMPServer
{
    int nestLevel;
    bool paused;
    IInterCommunicator *worldcomm;
public:
    static CriticalSection sect;

    CGlobalMPServer(unsigned __int64 _role, unsigned _port, bool _listen) : CMPServer(_role, _port, _listen)
    {
        worldcomm = NULL;
        nestLevel = 0;
    }
    ~CGlobalMPServer()
    {
        ::Release(worldcomm);
        worldcomm = nullptr;
    }
    IInterCommunicator &queryWorldCommunicator()
    {
        if (!worldcomm)
            worldcomm = new CInterCommunicator(this);
        return *worldcomm;
    }
    unsigned incNest() { return ++nestLevel; }
    unsigned decNest() { return --nestLevel; }
    unsigned queryNest() { return nestLevel; }
    bool isPaused() const { return paused; }
    void setPaused(bool onOff) { paused = onOff; }
    void setDumpQueue(bool onOff) { dumpQueue = onOff; }
};
CriticalSection CGlobalMPServer::sect;
static CGlobalMPServer *globalMPServer;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    globalMPServer = NULL;
    return true;
}
MODULE_EXIT()
{
    CGlobalMPServer * savedMPServer = globalMPServer;
    globalMPServer = nullptr;
    ::Release(savedMPServer);
}

void startMPServer(unsigned __int64 role, unsigned port, bool paused, bool listen)
{
    assertex(sizeof(PacketHeader)==32);
    CriticalBlock block(CGlobalMPServer::sect);
    if (NULL == globalMPServer)
    {
        globalMPServer = new CGlobalMPServer(role, port, listen);
        initMyNode(globalMPServer->getPort());
    }
    if (0 == globalMPServer->queryNest())
    {
        if (paused)
        {
            globalMPServer->setPaused(paused);
            return;
        }
        queryLogMsgManager()->setPort(globalMPServer->getPort());
        globalMPServer->start();
        globalMPServer->setPaused(false);
    }
    globalMPServer->incNest();
}

void startMPServer(unsigned port, bool paused, bool listen)
{
    startMPServer(0, port, paused, listen);
}

void stopMPServer(bool dumpQueue)
{
    CGlobalMPServer *_globalMPServer = NULL;
    {
        CriticalBlock block(CGlobalMPServer::sect);
        if (NULL == globalMPServer)
            return;
        if (0 == globalMPServer->decNest())
        {
            stopLogMsgReceivers();
#ifdef _TRACE
            LOG(MCdebugInfo, "MP: Stopping MP Server");
#endif
            _globalMPServer = globalMPServer;
            globalMPServer = NULL;
        }
    }
    if (NULL == _globalMPServer)
        return;
    _globalMPServer->setDumpQueue(dumpQueue);
    _globalMPServer->stop();
    _globalMPServer->Release();
#ifdef _TRACE
    LOG(MCdebugInfo, "MP: Stopped MP Server");
#endif
    CriticalBlock block(CGlobalMPServer::sect);
    initMyNode(0);
}

bool hasMPServerStarted()
{
    CriticalBlock block(CGlobalMPServer::sect);
    return globalMPServer != NULL;
}

IInterCommunicator &queryWorldCommunicator()
{
    CriticalBlock block(CGlobalMPServer::sect);
    assertex(globalMPServer);
    return globalMPServer->queryWorldCommunicator();
}

mptag_t createReplyTag()
{
    assertex(globalMPServer);
    return globalMPServer->createReplyTag();
}

ICommunicator *createCommunicator(IGroup *group, bool outer)
{
    assertex(globalMPServer);
    return globalMPServer->createCommunicator(group, outer);
}

StringBuffer &getReceiveQueueDetails(StringBuffer &buf)
{
    CriticalBlock block(CGlobalMPServer::sect);
    if (globalMPServer)
        globalMPServer->getReceiveQueueDetails(buf);
    return buf;
}

void addMPConnectionMonitor(IConnectionMonitor *monitor)
{
    CriticalBlock block(CGlobalMPServer::sect);
    assertex(globalMPServer);
    globalMPServer->addConnectionMonitor(monitor);
}

void removeMPConnectionMonitor(IConnectionMonitor *monitor)
{
    CriticalBlock block(CGlobalMPServer::sect);
    if (globalMPServer)
        globalMPServer->removeConnectionMonitor(monitor);
}

IMPServer *getMPServer()
{
    CriticalBlock block(CGlobalMPServer::sect);
    assertex(globalMPServer);
    return LINK(globalMPServer);
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
