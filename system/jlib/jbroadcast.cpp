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


#include "jsocket.hpp"
#include "jthread.hpp"
#include "jexcept.hpp"
#include "jbuff.hpp"
#include "jlog.hpp"
#include "jfile.hpp"

#include "jbroadcast.ipp"

// valid udp multicast range - 224.0.0.0 through 239.255.255.255

#define DEFAULT_UNICAST_PCENT 10
#define DEFAULT_UNICAST_LIMIT 8
#define NACK_SINGLETON 0x80000000
#define NACK_ENDMARKER 0x40000000
#define PACKET_QUEUE_LIMIT 0 // disabled for now

//#define TRACKLASTACK
//#define COUNTRESENDS

#define UDP_SEND_SIZE (128*1024)
#define UDP_RECV_SIZE (128*1024)

#define DEFAULT_POLL_DELAY 1000
#define MAX_POLL_REPLY_DELAY 1000

static CriticalSection *recvServerCrit, *jobIdCrit;
static CMCastRecvServer *mcastRecvServer = NULL;

static unsigned unicastPcent = DEFAULT_UNICAST_PCENT;
static unsigned unicastLimit = DEFAULT_UNICAST_LIMIT;
static bool useUniCast = true;
static unsigned tracingPeriod = 0; // periodic tracing disabled by default
static unsigned pollDelay = DEFAULT_POLL_DELAY;
static unsigned nextJobId;

MODULE_INIT(INIT_PRIORITY_JBROADCAST)
{
    recvServerCrit = new CriticalSection;
    jobIdCrit = new CriticalSection;
    return true;
}

MODULE_EXIT()
{
    delete recvServerCrit;
    delete jobIdCrit;
    if (mcastRecvServer) mcastRecvServer->Release();
}

static unsigned getNextJobId()
{
    CriticalBlock b(*jobIdCrit);
    return nextJobId++;
}

static int pktOrderFunc(unsigned *m1, unsigned *m2)
{
    return *m1-*m2;
}

static int pktRevOrderFunc(unsigned *m1, unsigned *m2)
{
    return *m2-*m1;
}

static int dataPktOrderFunc(CInterface **dataPacket1, CInterface **dataPacket2)
{
    return (*((CDataPacket **)dataPacket2))->header->id-(*((CDataPacket **)dataPacket1))->header->id;
}

class CUniq
{
public:
    CUniq()
    {
        reset();
    }
    void reset()
    {
        nextFree = bctag_DYNAMIC;
        freeList.kill();
    }
    inline __int64 getNextFree() { return nextFree; }
    inline __int64 getId()
    {
        return freeList.ordinality() ? freeList.pop() : nextFree++;
    }
    inline void freeId(__int64 id)
    {
        assertex(id<nextFree);
        freeList.append(id);
    }
private:
    __int64 nextFree;
    Int64Array freeList;
};

static CUniq uniqIds;

bctag_t jlib_decl allocBcTag()
{
    return (bctag_t) uniqIds.getId(); 
}

void jlib_decl freeBcTag(bctag_t tag)
{
    uniqIds.freeId(tag);
}

//
// CMCastRecvServer impl.
//

CMCastRecvServer::CMCastRecvServer(const char *_broadcastRoot, unsigned _groupMember, SocketEndpoint &_mcastEp, unsigned _ackPort) : broadcastRoot(_broadcastRoot), groupMember(_groupMember), mcastEp(_mcastEp), ackPort(_ackPort)
{
    stopped = false;
    start();
}

void CMCastRecvServer::stop()
{
    stopped = true;
    sock->close();
    join();
    ackSock->close();
}

void CMCastRecvServer::registerReceiver(CMCastReceiver &receiver)
{
    CriticalBlock b(receiversCrit);
    receiver.Link();
    receivers.replace(receiver);
}

void CMCastRecvServer::deregisterReceiver(CMCastReceiver &receiver)
{
    CriticalBlock b(receiversCrit);
    receivers.removeExact(&receiver);
}

CMCastReceiver *CMCastRecvServer::getReceiver(bctag_t tag)
{
    CriticalBlock b(receiversCrit);
    CMCastReceiver *receiver = receivers.find(tag);
    return LINK(receiver);
}

int CMCastRecvServer::run()
{
    SocketEndpoint ackEp(broadcastRoot);
    ackEp.port = ackPort;
    StringBuffer s;
    ackEp.getIpText(s);
    ackSock.setown(ISocket::udp_connect(ackEp.port, s.str()));
    ackSock->set_send_buffer_size(UDP_SEND_SIZE);

    StringBuffer ipStr;
    mcastEp.getIpText(ipStr);
    sock.setown(ISocket::multicast_create(mcastEp.port, ipStr.str()));
    sock->set_receive_buffer_size(UDP_RECV_SIZE);
    SocketEndpoint ep(ipStr.str());
    sock->join_multicast_group(ep);

    
    MemoryBuffer mbAck;
    MCAckPacketHeader *ackPacket = (MCAckPacketHeader *)mbAck.reserveTruncate(MC_ACK_PACKET_SIZE);

    ackPacket->node = groupMember;

    LOG(MCdebugProgress(10), unknownJob, "Running as client %d connected to server %s", groupMember, broadcastRoot.get());

    unsigned *nackList = (unsigned *)(((byte *)ackPacket)+sizeof(MCAckPacketHeader));
    const unsigned *nackUpper = (unsigned *)((byte *)ackPacket)+MC_ACK_PACKET_SIZE-sizeof(unsigned);

    Owned<CDataPacket> dataPacket = new CDataPacket();

    CTimeMon logTm(10000), logTmCons(5000), logTmPoll(5000), logTmOld(5000), logTmNoRecv(5000);
    loop
    {
        try
        {
            unsigned startTime = msTick();
            loop
            {
                try
                {
                    size32_t szRead;
                    sock->read(dataPacket->header, sizeof(MCPacketHeader), MC_PACKET_SIZE, szRead, 5000);
                    break;
                }
                catch (IException *e)
                {
                    if (JSOCKERR_timeout_expired != e->errorCode())
                        throw;
                    else e->Release();
                    LOG(MCdebugProgress(1), unknownJob, "Waiting on packet read socket (waited=%d)", msTick()-startTime);
                }
            }
            if (stopped) break;
            if (MCPacket_Stop == dataPacket->header->cmd)
            {
                stopped = true;
                break;
            }
            ackPacket->tag = dataPacket->header->tag;
            ackPacket->jobId = dataPacket->header->jobId;

            if (oldJobIds.find(dataPacket->header->jobId))
            {
                if (MCPacket_Poll == dataPacket->header->cmd)
                {
                    ackPacket->ackDone = true;
                    MilliSleep(MAX_POLL_REPLY_DELAY/(groupMember+1));
                    ackSock->write(ackPacket, sizeof(MCAckPacketHeader));
                }

                if (tracingPeriod && logTmOld.timedout())
                {
                    LOG(MCdebugProgress(1), unknownJob, "Old job polled=%s", MCPacket_Poll == dataPacket->header->cmd?"true":"false");
                    logTmOld.reset(tracingPeriod);
                }
            }
            else
            {
                CMCastReceiver *receiver = getReceiver(dataPacket->header->tag);
                if (receiver)
                {
                    if (MCPacket_Poll == dataPacket->header->cmd)
                    {
                        size32_t sz;
                        bool res = receiver->buildNack(ackPacket, sz, dataPacket->header->total);
                        MilliSleep(MAX_POLL_REPLY_DELAY/(groupMember+1));
                        ackSock->write(ackPacket, sz);
                        if (tracingPeriod && logTmPoll.timedout())
                        {
                            LOG(MCdebugProgress(1), unknownJob, "Send nack back sz=%d, res=%s, done=%s", sz, res?"true":"false", ackPacket->ackDone?"true":"false");
                            logTmPoll.reset(tracingPeriod);
                        }
                    }
                    else
                    {
                        unsigned total = dataPacket->header->total;
                        bool done;
                        if (receiver->packetReceived(*dataPacket, done)) // if true, packet consumed
                        {
                            unsigned level;
                            if (tracingPeriod && logTmCons.timedout())
                            {
                                level = 1;
                                logTmCons.reset(5000);
                            } else level = 110;
                            LOG(MCdebugProgress(level), unknownJob, "Pkt %d taken by receiver", dataPacket->header->id);
                            if (done)
                            {
                                LOG(MCdebugProgress(10), unknownJob, "Client (tag=%x, jobId=%d) received all %d packets", dataPacket->header->tag, dataPacket->header->jobId, dataPacket->header->total);
                                oldJobIds.replace(* new CUIntValue(dataPacket->header->jobId));
                            }
                            // JCSMORE should use packet pool.
                            // init new packet
                            dataPacket.setown(new CDataPacket());
                        }
                        else if (tracingPeriod && logTm.timedout())
                        {
                            LOG(MCdebugProgress(150), unknownJob, "throwing away packet %d", dataPacket->header->id);
                            logTm.reset(tracingPeriod);
                        }

                        if (!done)
                        {
                            size32_t sz;
                            if (receiver->buildNack(ackPacket, sz, total))
                                ackSock->write(ackPacket, sz);
                        }
                    }
                }
                else if (tracingPeriod && logTmNoRecv.timedout())
                {
                    LOG(MCdebugProgress(1), unknownJob, "No Receiver tag=%d", dataPacket->header->tag);
                    logTmNoRecv.reset(tracingPeriod);
                }
            }
        }
        catch (IException *e)
        {
            pexception("Client Exception",e);
            break;
        }
    }

    PROGLOG("Receive server stopping, aborting receivers");
    {
        CriticalBlock b(receiversCrit);
        SuperHashIteratorOf<CMCastReceiver> iter(receivers);
        ForEach (iter)
            iter.query().stop();
    }

    return 0;
}

CMCastRecvServer *queryMCastRecvServer()
{
    return mcastRecvServer;
}

void startMCastRecvServer(const char *broadcastRoot, unsigned groupMember, const char *mcastIp, unsigned mcastPort, unsigned ackPort)
{
    SocketEndpoint mcastEp(mcastIp, mcastPort);
    startMCastRecvServer(broadcastRoot, groupMember, mcastEp, ackPort);
}

void startMCastRecvServer(const char *broadcastRoot, unsigned groupMember, SocketEndpoint &mcastEp, unsigned ackPort)
{
    CriticalBlock b(*recvServerCrit);
    assertex(!mcastRecvServer);
    mcastRecvServer = new CMCastRecvServer(broadcastRoot, groupMember, mcastEp, ackPort);
}

void stopMCastRecvServer()
{
    CriticalBlock b(*recvServerCrit);
    if (mcastRecvServer)
    {
        mcastRecvServer->stop();
        mcastRecvServer = NULL;
    }
}

// 


CMCastReceiver::CMCastReceiver(bctag_t _tag) : tag(_tag), nextPacket(0), eosHit(false), aborted(false)
{
    { CriticalBlock b(*recvServerCrit);
        if (!mcastRecvServer)
            throw MakeStringException(0, "Multicast receive server not running");
    }
    logTmRecv.reset(tracingPeriod);
    logTmCons.reset(tracingPeriod);
    mcastRecvServer->registerReceiver(*this);
}

CMCastReceiver::~CMCastReceiver()
{
    assertex(mcastRecvServer);
    mcastRecvServer->deregisterReceiver(*this);
}

bool CMCastReceiver::packetReceived(CDataPacket &dataPacket, bool &complete)
{
    if (dataPacket.header->total == pktsReceived.ordinality())
        complete = true;
    else if (NotFound == pktsReceived.bSearch(dataPacket.header->id, pktOrderFunc))
    {
        bool ret = false;
        bool isNew;
        pktsReceived.bAdd(dataPacket.header->id, pktOrderFunc, isNew);
        assertex(isNew);

        CriticalBlock b(crit);
        bool overflow = PACKET_QUEUE_LIMIT && (dataPackets.ordinality() > PACKET_QUEUE_LIMIT);
        if (!overflow || dataPacket.header->id < dataPackets.item(0).header->id)
        {
            // process packet
            unsigned level;
            if (tracingPeriod && logTmRecv.timedout())
            {
                level = 1;
                logTmRecv.reset(tracingPeriod);
            }
            else level = 110;
            LOG(MCdebugProgress(level), unknownJob, "\nReceived \n"
                "packet id      = %d\n"
                "packet length  = %d\n"
                , dataPacket.header->id, dataPacket.header->length);

            {
                dataPacket.Link();
                CInterface *_dataPacket = &dataPacket;
                dataPackets.bAdd(_dataPacket, dataPktOrderFunc, isNew);
            }
            assertex(isNew);

            if (overflow)
            {
                LOG(MCdebugProgress(50), unknownJob, "Overflow, removed traling packet %d", dataPackets.item(0).header->id);
                unsigned pos = pktsReceived.bSearch(dataPackets.item(0).header->id, pktOrderFunc);
                assertex(NotFound != pos);
                dataPackets.remove(0);
                pktsReceived.remove(pos);
            }
    
            receivedSem.signal();

            ret = true;
        }
        complete = (dataPacket.header->total == pktsReceived.ordinality());
        return ret;
    }
    return false;
}

bool CMCastReceiver::buildNack(MCAckPacketHeader *ackPacket, size32_t &sz, unsigned total)
{
    unsigned *nackList = (unsigned *)(((byte *)ackPacket)+sizeof(MCAckPacketHeader));
    const unsigned *nackUpper = (unsigned *)((byte *)ackPacket)+MC_ACK_PACKET_SIZE-sizeof(unsigned);
    unsigned *nList = nackList;
    unsigned nackStart = (unsigned)-1, prev = (unsigned)-1;
    unsigned nackEnd;
    unsigned p;
    for (p=0; p<pktsReceived.ordinality(); p++)
    {
        unsigned pkt = pktsReceived.item(p);
        
        if (prev != pkt-1)
        {
            nackStart = prev+1;
            if (pkt == nackStart+1)
            {
                nackEnd = nackStart;
                nackStart |= NACK_SINGLETON;
            }
            else
                nackEnd = pkt-1;
            *nList++ = nackStart;
            if (0 == (nackStart & NACK_SINGLETON))
            {
                assertex((unsigned)-1 != nackEnd);
                *nList++ = nackEnd;
            }
        }

        prev = pkt;

        assertex(nList<=nackUpper);
    }
    nackEnd = prev;
    if (total)
    {
        if (nackEnd == total-1)
            ackPacket->ackDone = true;
        else
        {
            ackPacket->ackDone = false;
            *nList++ = nackEnd+1;
            *nList++ = total-1;
        }
    }
    else
        ackPacket->ackDone = false;

    if (nList != nackList)
    {
        *(nList++) = NACK_ENDMARKER;
        sz = (size32_t)sizeof(MCAckPacketHeader)+(((byte *)nList)-((byte *)nackList));
        return true;
    }
    else
    {
        sz = 0;
        return false;
    }
}

void CMCastReceiver::reset()
{
    dataPackets.kill();
    pktsReceived.kill();
    nextPacket = 0;
    eosHit = false;
}

// IBroadcastReceiver impl.
bool CMCastReceiver::eos()
{
    return eosHit;
}

void CMCastReceiver::stop()
{
    aborted = true;
    receivedSem.signal();
}

bool CMCastReceiver::read(MemoryBuffer &mb)
{
    loop
    {
        receivedSem.wait();
        if (aborted) 
        {
            aborted = false;
            return false;
        }
        bool hadSome=false;
        loop
        {
            Linked<CDataPacket> dataPacket;
            { CriticalBlock b(crit);
                if (dataPackets.ordinality() && nextPacket==dataPackets.tos().header->id)
                {
                    dataPacket.set(&dataPackets.tos());
                    dataPackets.pop();
                    hadSome = true;
                }
                else break;
            }
            if (dataPacket)
            {
                mb.append(dataPacket->header->length, dataPacket->queryData());
                unsigned level;
                if (tracingPeriod && logTmCons.timedout())
                {
                    level = 1;
                    logTmCons.reset(tracingPeriod);
                }
                else level = 110;
                LOG(MCdebugProgress(level), unknownJob, "Pkt %d consumed", dataPacket->header->id);
                if (nextPacket == dataPacket->header->total-1)
                {
                    eosHit = true;
                    return true;
                }
                nextPacket++;
            }
        }
        if (hadSome) return true;
    }
    return true;
}

//
//
// CAckProcessor impl.
//
struct TagJobIdTuple
{
    TagJobIdTuple(bctag_t _tag, unsigned _jobId) : tag(_tag), jobId(_jobId) { }
    bctag_t tag;
    unsigned jobId;
    bool operator ==(TagJobIdTuple &other) { return other.tag == tag && other.jobId == jobId; }
};

class CAckProcessor : public CInterface
{
    CriticalSection crit;
    CPktNodeTable pktNodeTable, nodeAckTable;
    UnsignedArray nackOrder;
    CUIntTable clientsDone;
    unsigned nodes;
    TagJobIdTuple tagJobId;
#ifdef TRACKLASTACK
    unsigned *nodeLastAckTimes;
#endif

public:
    CAckProcessor(bctag_t tag, unsigned jobId, unsigned _nodes) : tagJobId( tag, jobId), nodes(_nodes)
    {
#ifdef TRACKLASTACK
        nodeLastAckTimes = (unsigned *)malloc(nodes * sizeof(unsigned));
        memset(nodeLastAckTimes, 0, nodes * sizeof(unsigned));
#endif
    }

    const void *queryFindParam() const
    {
        return (const void *) &tagJobId;
    }

    void clear()
    {
        pktNodeTable.kill();
        nackOrder.kill();
        nodeAckTable.kill();
        clientsDone.kill();
    }

    void initNackTable(unsigned packets)
    {
        pktNodeTable.reinit(packets);
        while (packets--)
        {
            if (packets < 10 && packets%3==0) continue;
            CUIntTableItem *nodeMap = new CUIntTableItem(packets);
            pktNodeTable.add(* nodeMap);
            nackOrder.append(packets);
            unsigned node;
            for (node=0; node<nodes; node++)
                nodeMap->add(* new CUIntValue(node));
        }
    }

    void addNackAll(unsigned pkt)
    {
        unsigned n = nodes;
        while (n--)
            addNack(pkt, n);
    }

    void addNack(unsigned pkt, unsigned node)
    {
        CUIntTableItem *nodeMap = pktNodeTable.find(pkt);
        if (!nodeMap)
        {
            nodeMap = new CUIntTableItem(pkt);
            pktNodeTable.add(* nodeMap);
            bool isNew;
            nackOrder.bAdd(pkt, pktRevOrderFunc, isNew);
            assertex(isNew);
        }
        if (!nodeMap->find(node))
        {
            nodeMap->add(* new CUIntValue(node));
            LOG(MCdebugProgress(150), unknownJob, "New NACK pkt = %d from node %d", pkt, node);
        }
    }

    void deleteNack(unsigned node, unsigned pkt)
    {
        CriticalBlock b(crit);
        if (pktNodeTable.remove(&pkt))
        {
            unsigned pos = nackOrder.bSearch(pkt, pktRevOrderFunc);
            assertex(pos != NotFound);
            nackOrder.remove(pos);
        }
    }

    CUIntTableItem *detachPktNack()
    {
        // detach first nacked pkt.
        CriticalBlock b(crit);
        if (!nackOrder.ordinality()) return NULL;
        unsigned nackPkt = nackOrder.pop();
        CUIntTableItem *map = (CUIntTableItem *)pktNodeTable.find(nackPkt);
        assertex(map);
        map->Link();
        pktNodeTable.removeExact(map);
        return map;
    }

#ifdef TRACKLASTACK
    unsigned queryLastAckMs(unsigned node)
    {
        return nodeLastAckTimes[node];
    }
#endif

    unsigned queryAcked(unsigned node)
    {
        CriticalBlock b(crit);
        CUIntTableItem *pktAcks = nodeAckTable.find(node);
        if (!pktAcks) return (unsigned)-1;
        return pktAcks->count();
    }

    bool queryClientDone(unsigned node)
    {
        CriticalBlock b(crit);
        if (clientsDone.find(node))
            return true;
        return false;
    }

    virtual int handleAck(MCAckPacketHeader *ackPacket)
    {
        if (ackPacket->ackDone)
        {
            CriticalBlock b(crit);
            clientsDone.replace(* new CUIntValue(ackPacket->node));
            LOG(MCdebugProgress(150), unknownJob, "Node %d signalled done", ackPacket->node);
        }
        else
        {
#ifdef TRACKLASTACK
            nodeLastAckTimes[ackPacket->node] = msTick();
#endif
            const unsigned *nackList = (unsigned *)(((byte *)ackPacket)+sizeof(MCAckPacketHeader));
            unsigned prevEnd = 0;
        StringBuffer msg;
            loop
            {
                unsigned nackStart = *nackList++, nackEnd;
                if (nackStart == NACK_ENDMARKER)
                    break;
                if (nackStart & NACK_SINGLETON)
                {
                    nackStart &= ~NACK_SINGLETON;
                    nackEnd = nackStart;
                }
                else
                    nackEnd = *nackList++;

                // fill-in implicit acked from node (prevEnd->nackStart)
                CriticalBlock b(crit);
                CUIntTableItem *ackedPkts = nodeAckTable.find(ackPacket->node);
                if (!ackedPkts)
                {
                    ackedPkts = new CUIntTableItem(ackPacket->node);
                    nodeAckTable.add(* ackedPkts);
                }
                unsigned pkt;
                for (pkt = prevEnd; pkt<nackStart; pkt++)
                {
                    if (!ackedPkts->find(pkt))
                    {
                        ackedPkts->add(* new CUIntValue(pkt));
                        deleteNack(ackPacket->node, pkt);
                    }
                }

                if (prevEnd<nackStart) msg.appendf(", ACK[%d-%d]", prevEnd, nackStart-1);

                for (pkt = nackStart; pkt<=nackEnd; pkt++)
                    addNack(pkt, ackPacket->node);

                msg.appendf(", NACK[%d-%d]", nackStart, nackEnd);

                prevEnd = nackEnd+1;
            }

            if (0 ==ackPacket->node)
                LOG(MCdebugProgress(55), unknownJob, "ACK/NACK node=%d, %s", ackPacket->node, msg.str());
        }
        return 0;
    }
};

static int CIOrderFunc(CInterface **cI1, CInterface **cI2)
{
    return (*((CCountedItem **)cI2))->count-(*((CCountedItem **)cI1))->count;
}

static CriticalSection gBCInUseCrit;
static bool groupBroadCastInUse = false;
typedef ThreadSafeOwningSimpleHashTableOf<CAckProcessor, TagJobIdTuple> CAckProcessorTable;
class CMCastBroadcaster : public CInterface, implements IBroadcast
{
    unsigned nodes, ackPort;
    SocketEndpointArray eps;
    Owned<ISocket> mcastSock;
    IArrayOf<ISocket> unicastSocks;

    Owned<ISocket> ackSock;
    bool stopped;
    CAckProcessorTable ackProcessorTable;
    class CThreaded : public Thread
    {
        CMCastBroadcaster &mcB;
    public:
        CThreaded(CMCastBroadcaster &_mcB) : Thread("CMCastBroadcaster"), mcB(_mcB) { start(); }
        virtual int run() { mcB.main(); return 1; }
    } *threaded;
public:
    IMPLEMENT_IINTERFACE;

    CMCastBroadcaster(SocketEndpointArray &_eps, SocketEndpoint &mcastEp, unsigned _ackPort)
    {
        {
            CriticalBlock b(gBCInUseCrit);
            assertex(!groupBroadCastInUse);
            groupBroadCastInUse = true;
        }
        srand((unsigned) time(NULL));
        nextJobId = rand();
        ackPort = _ackPort;
        CloneArray(eps, _eps);
        nodes = eps.ordinality();   
        StringBuffer ipStr;
        mcastEp.getIpText(ipStr);
        mcastSock.setown(ISocket::multicast_connect(mcastEp.port,ipStr.str(),5));
        mcastSock->set_send_buffer_size(UDP_SEND_SIZE);
        ArrayIteratorOf<SocketEndpointArray, SocketEndpoint> iter(eps);
        ForEach (iter)
        {
            StringBuffer ipStr;
            iter.query().getIpText(ipStr);          
            unicastSocks.append(*ISocket::udp_connect(mcastEp.port, ipStr.str()));
        }
        stopped = false;
        threaded = new CThreaded(*this);
    }

    ~CMCastBroadcaster()
    {
        CriticalBlock b(gBCInUseCrit);
        groupBroadCastInUse = false;
        if (threaded) { stop(); threaded->join(); threaded->Release(); }
    }

    void main()
    {
        ackSock.setown(ISocket::udp_create(ackPort));
        ackSock->set_receive_buffer_size(UDP_RECV_SIZE);
        try
        {
            MemoryBuffer mb;
            MCAckPacketHeader *ackPacket = (MCAckPacketHeader *)mb.reserveTruncate(MC_ACK_PACKET_SIZE);
            loop
            {
                size32_t szRead;
                ackSock->read(ackPacket, sizeof(MCAckPacketHeader), MC_ACK_PACKET_SIZE, szRead, WAIT_FOREVER);
                TagJobIdTuple tagJobId(ackPacket->tag, ackPacket->jobId);
                CAckProcessor *ackProcessor = ackProcessorTable.find(tagJobId);
                if (ackProcessor)
                    ackProcessor->handleAck(ackPacket);
            }
        }
        catch (IException *e)
        {
            if (JSOCKERR_graceful_close != e->errorCode())
                LOG(MCwarning, unknownJob, e, "Ack handler");
        }
    }

    virtual bool send(bctag_t tag, unsigned size, const void *data)
    {
        unsigned packets = 0;

        unsigned remaining = size;
        unsigned offset = 0;

#ifdef _DEBUG
        MTimeSection * mt = new MTimeSection(defaultTimer, "ServerBroadcast", "SERVER BROADCAST"); // MORE is ServerBroadcast a scope (where) or a name (what)?
#endif

        unsigned maxDataSz = MC_PACKET_SIZE-sizeof(MCPacketHeader);
        MemoryBuffer headerMb;
        MCPacketHeader *header = (MCPacketHeader *)headerMb.reserveTruncate(MC_PACKET_SIZE);
        byte *pktData = ((byte*)header) + sizeof(MCPacketHeader);

        header->total = size / maxDataSz; 
        if (size % maxDataSz) header->total++;
        header->cmd = MCPacket_None;
        header->tag = tag;
        unsigned jobId = getNextJobId();
        header->jobId = jobId;
    
        CAckProcessor *ackProcessor = new CAckProcessor(tag, jobId, nodes);
        struct ScopedTableElem
        {
            ScopedTableElem(CAckProcessorTable &_table, CAckProcessor &_processor) : table(_table), processor(_processor) { table.replace(processor); } 
            ~ScopedTableElem() { table.removeExact(&processor); }
            CAckProcessorTable &table;
            CAckProcessor &processor;
        } scopedTableElem(ackProcessorTable, *ackProcessor);

        LOG(MCdebugProgress(30), unknownJob, "Broadcasting");
        byte *dataPtr = (byte *) data;
        packets = (remaining+maxDataSz-1)/maxDataSz;
        ackProcessor->initNackTable(packets);
        LOG(MCdebugProgress(20), unknownJob, "Broadcasting %d packets", packets);

        unsigned lastPoll = msTick();
        CCountTable resendPktTable, resendNodeTable;

        CTimeMon logTm(5000);
        while (!stopped)
        {
            LOG(MCdebugProgress(30), unknownJob, "Resending cycle");
            while (!stopped)
            {
                Owned<CUIntTableItem> nodeMap = ackProcessor->detachPktNack();
                if (!nodeMap) break;
                unsigned pkt = nodeMap->queryPacket();
                assertex(pkt<packets);
                header->id = pkt;
                header->length = (pkt+1==packets) ? size-(maxDataSz*pkt) : maxDataSz;
                header->offset = maxDataSz*pkt;
                header->total = packets;
                header->cmd = MCPacket_None;
                    
                // refill pktData
                memcpy(pktData, ((byte*)data)+header->offset, header->length);

                bool unicast = useUniCast && (nodeMap->count() <= unicastLimit || (nodeMap->count() * 100 / nodes < unicastPcent));
                unsigned level;
                if (logTm.timedout())
                {
                    level = 1;
                    logTm.reset(5000);
                }
                else level = 100;
                unsigned nodeCount = nodeMap->count();
#ifdef COUNTRESENDS
                unsigned resendCount = resendPktTable.incItem(pkt);
                LOG(MCdebugProgress(level), unknownJob, "Resending packet %s (%d nodes request) %d (resent %d times)", unicast?"UNICAST":"MULTICAST", nodeMap->count(), pkt, resendCount);
#endif
                if (unicast)
                {
#ifdef COUNTRESENDS
                    StringBuffer nodeList;
#endif
                    SuperHashIteratorOf<CUIntValue> iter(*nodeMap);
                    if (iter.first())
                    {
                        loop
                        {
                            unsigned node = iter.query().queryValue();
                            unicastSocks.item(node).write(header, sizeof(MCPacketHeader)+header->length);
#ifdef COUNTRESENDS
                            resendNodeTable.incItem(node);
                            nodeList.append(node);
#endif
                            if (!iter.next()) break;
#ifdef COUNTRESENDS
                            nodeList.append(", ");
#endif
                        }
#ifdef COUNTRESENDS
                        LOG(MCdebugProgress(level), unknownJob, "Unicasted to nodes: %s", nodeList.str());
#endif
                    }
                }
                else
                {
#ifdef COUNTRESENDS
                    SuperHashIteratorOf<CUIntValue> iter(*nodeMap);
                    ForEach (iter)
                    {
                        unsigned node = iter.query().queryValue();
                        resendNodeTable.incItem(-node);
                    }
#endif
                    mcastSock->write(header, sizeof(MCPacketHeader)+header->length);
                }
            }
            if (stopped) break;

            // run out of nacks - but not all acked - poll for more nacks.
            StringBuffer ackStr("Ack counts (total=");
            ackStr.append(packets).append("; ");
            unsigned node;
            LOG(MCdebugProgress(30), unknownJob, "Polling");
            UnsignedArray needPolling;
            for (node=0; node<nodes; node++)
            {
                unsigned ackedPkts = ackProcessor->queryAcked(node);
                if (packets != ackedPkts && !ackProcessor->queryClientDone(node))
                {
                    needPolling.append(node);
                    ackStr.append("n(").append(node).append("):").append((unsigned)-1==ackedPkts?0:ackedPkts).append(' ');
                }
            }
            if (needPolling.ordinality())
            {
                unsigned diff = msTick()-lastPoll;
                if (diff < pollDelay)
                {
                    DBGLOG("Sleeping %d ms", pollDelay-diff);
                    MilliSleep(pollDelay-diff);
                }
                header->cmd = MCPacket_Poll;
                header->total = packets;
                bool unicastPoll = useUniCast && (needPolling.ordinality() <= unicastLimit || (needPolling.ordinality() * 100 / nodes < unicastPcent));
                StringBuffer pollStr("Polled ");
                if (unicastPoll)
                {
                    pollStr.append("node list : ");
                    ForEachItemIn(n, needPolling)
                    {
                        unsigned node = needPolling.item(n);
                        pollStr.append(node).append("(");
                        eps.item(node).getUrlStr(pollStr);
                        pollStr.append(") ");
#ifdef TRACKLASTACK
                        unsigned lastMs = ackProcessor->queryLastAckMs(node);
                        if (lastMs)
                            pollStr.append('[').append(msTick()-lastMs).append("] ");
#endif
                        unicastSocks.item(node).write(header, sizeof(MCPacketHeader));
                    }
                }
                else // mcast
                {
                    pollStr.append(" ALL (mcast) nodes");
                    mcastSock->write(header, sizeof(MCPacketHeader));
                }
                LOG(MCdebugProgress(30), unknownJob, "%s", pollStr.str());
                LOG(MCdebugProgress(40), unknownJob, "%s", ackStr.str());
                lastPoll = msTick();
            }
            else
            {
                LOG(MCdebugProgress(10), unknownJob, "Done");
                bool allDone = true;
                LOG(MCdebugProgress(30), unknownJob, "Checking Done");
                for (node=0; node<nodes; node++)
                {
                    if (!ackProcessor->queryClientDone(node))
                    {
                        allDone = false;
                        break;
                    }

                }
                if (allDone)
                    break; //finished!
                else
                {
                    header->cmd = MCPacket_Poll;
                    header->total = packets;
                    mcastSock->write(header, sizeof(MCPacketHeader));
                }
            }
        }

#ifdef _DEBUG
        delete mt;
#endif
#ifdef COUNTRESENDS
        // timings/stats

        LOG(MCdebugProgress(110), unknownJob, "Packet resend history");
        SuperHashIteratorOf<CCountedItem> pktIter(resendPktTable);
        CopyCIArrayOf<CCountedItem> list;
        ForEach (pktIter)
        {
            CCountedItem &rv = pktIter.query();
            list.append(rv);
        }
        list.sort(CIOrderFunc);
        ForEachItemIn(ci1, list)
        {
            CCountedItem &rv = list.item(ci1);
            LOG(MCdebugProgress(110), unknownJob, "Resent packet %d, %d times", rv.queryValue(), rv.count);
        }
        list.kill();
        LOG(MCdebugProgress(110), unknownJob, "Node resend history");
        SuperHashIteratorOf<CCountedItem> nodeIter(resendNodeTable);
        ForEach (nodeIter)
        {
            CCountedItem &rv = nodeIter.query();
            list.append(rv);
        }
        list.sort(CIOrderFunc);
        ForEachItemIn(ci2, list)
        {
            CCountedItem &rv = list.item(ci2);
            int node = rv.queryValue();
            if (node < 0)
                LOG(MCdebugProgress(110), unknownJob, "Multicast to all nodes due (in part) to node %d, %d times", -node, rv.count);
            else
                LOG(MCdebugProgress(110), unknownJob, "Node %d was resent to %d times", node, rv.count);
        }
#endif

        LOG(MCdebugProgress(10), unknownJob, "All %d packets from all %d nodes acknowledged", packets, nodes);
#ifdef _DEBUG
        StringBuffer str;
        defaultTimer->getTimings(str);
        LOG(MCdebugProgress(10), unknownJob, "%s", str.str());
#endif
        return !stopped;
    }

    virtual void stop()
    {
        if (!stopped)
        {
            stopped = true;
            if (ackSock) ackSock->close();
        }
    }

    virtual void stopClients()
    {
        // TBD
        unsigned attempts = 5;
        while (attempts--)
        {
            PROGLOG("Sending stop via mcast");
            MCPacketHeader header;
            header.cmd = MCPacket_Stop;
            mcastSock->write(&header, sizeof(MCPacketHeader));
            MilliSleep(1000);
        }
    }
};

IBroadcast *createGroupBroadcast(SocketEndpointArray &eps, const char *mcastIp, unsigned mcastPort, unsigned ackPort)
{
    SocketEndpoint mcastEp(mcastIp, mcastPort);
    return createGroupBroadcast(eps, mcastEp, ackPort);
}

IBroadcast *createGroupBroadcast(SocketEndpointArray &eps, SocketEndpoint &mcastEp, unsigned ackPort)
{
    return new CMCastBroadcaster(eps, mcastEp, ackPort);
}

IBroadcastReceiver *createGroupBroadcastReceiver(bctag_t tag)
{
    return new CMCastReceiver(tag);
}

void setBroadcastOpt(bcopt_t opt, unsigned value)
{
    switch (opt)
    {
        case bcopt_pollDelay:
            pollDelay = value;
            break;
        case bcopt_useUniCast:
            useUniCast = 0!=value;
            break;
        case bcopt_unicastLimit:
            unicastLimit = value;
            break;
        case bcopt_unicastPcent:
            unicastPcent = value;
            break;
        default:
            assertex(!"Unknown broadcast option");
    }
}
