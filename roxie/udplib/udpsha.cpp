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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "jsocket.hpp"
#include "jlog.hpp"
#include "roxie.hpp"
#include "roxiemem.hpp"
#include "portlist.h"

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

using roxiemem::DataBuffer;
using roxiemem::IDataBufferManager;

IDataBufferManager *bufferManager;

bool udpTraceFlow = false;
bool udpTraceTimeouts = false;
unsigned udpTraceLevel = 0;
unsigned udpFlowSocketsSize = 131072;
unsigned udpLocalWriteSocketSize = 1024000;
unsigned udpStatsReportInterval = 60000;

unsigned multicastTTL = 1;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    bufferManager = roxiemem::createDataBufferManager(roxiemem::DATA_ALIGNMENT_SIZE);
    return true;
}

MODULE_EXIT()
{ 
    bufferManager->Release();
}


const IpAddress ServerIdentifier::getIpAddress() const
{
    IpAddress ret;
    ret.setIP4(netAddress);
    return ret;
}

bool ServerIdentifier::isMe() const
{
    return *this==myNode;
}

ServerIdentifier myNode;

//---------------------------------------------------------------------------------------------

void queue_t::set_queue_size(unsigned _limit)
{
    limit = _limit;
}

queue_t::queue_t(unsigned _limit)
{
    set_queue_size(_limit);
}


queue_t::~queue_t() 
{
    while (head)
    {
        auto p = head;
        head = head->msgNext;
        ::Release(p);
    }
}

unsigned queue_t::available()
{
    CriticalBlock b(c_region);
    if (count < limit)
        return limit - count;
    return 0;
}

int queue_t::free_slots() 
{
    int res=0;
    while (res <= 0)
    {
        c_region.enter();
        res = limit - count;
        if (res <= 0)
            signal_free_sl++;
        c_region.leave();
        if (res <= 0)
        {
            while (!free_sl.wait(3000))
            {
                if (udpTraceLevel >= 1)
                    DBGLOG("queue_t::free_slots blocked for 3 seconds waiting for free_sl semaphore");
            }
        }
    }
    return res;
}

void queue_t::interrupt()
{
    data_avail.interrupt();
}

void queue_t::pushOwn(DataBuffer *buf)
{
    // Could probably be done lock-free, which given one thread using this is high priority might avoid some
    // potential priority-inversion issues. Or we might consider using PI-aware futexes here?
    assert(!buf->msgNext);
    {
        CriticalBlock b(c_region);
        if (tail)
        {
            assert(head);
            assert(!tail->msgNext);
            tail->msgNext = buf;
        }
        else
        {
            assert(!head);
            head = buf;
        }
        tail = buf;
        count++;
#ifdef _DEBUG
        if (count > limit)
            DBGLOG("queue_t::pushOwn set count to %u", count);
#endif
    }
    data_avail.signal();
}

DataBuffer *queue_t::pop(bool block)
{
    if (!data_avail.wait(block ? INFINITE : 0))
        return nullptr;
    DataBuffer *ret = nullptr;
    unsigned signalFreeSlots = 0;
    {
        CriticalBlock b(c_region);
        if (!count)
            return nullptr;
        count--;
        ret = head;
        head = head->msgNext;
        if (!head)
        {
            assert(!count);
            tail = nullptr;
        }
        ret->msgNext = nullptr;
        if (count < limit && signal_free_sl)
        {
            signal_free_sl--;
            signalFreeSlots++;
        }
    }
    if (signalFreeSlots)
        free_sl.signal(signalFreeSlots);
    return ret;
}


unsigned queue_t::removeData(const void *key, PKT_CMP_FUN pkCmpFn)
{
    unsigned removed = 0;
    unsigned signalFreeSlots = 0;
    {
        CriticalBlock b(c_region);
        if (count)
        {
            DataBuffer *prev = nullptr;
            DataBuffer *finger = head;
            while (finger)
            {
                if (!key || !pkCmpFn || pkCmpFn((const void*) finger, key))
                {
                    auto temp = finger;
                    finger = finger->msgNext;
                    if (prev==nullptr)
                    {
                        assert(head==temp);
                        head = finger;
                    }
                    else
                        prev->msgNext = finger;
                    if (temp==tail)
                        tail = prev;
                    ::Release(temp);
                    count--;
                    if (count < limit && signal_free_sl)
                    {
                        signal_free_sl--;
                        signalFreeSlots++;
                    }
                    removed++;
                }
                else
                {
                    prev = finger;
                    finger = finger->msgNext;
                }
            }
        }
    }
    if (signalFreeSlots)
        free_sl.signal(signalFreeSlots);
    return removed;
}


bool queue_t::dataQueued(const void *key, PKT_CMP_FUN pkCmpFn)
{
    CriticalBlock b(c_region);
    DataBuffer *finger = head;
    while (finger)
    {
        if (pkCmpFn((const void*) finger, key))
            return true;
        finger = finger->msgNext;
    }
    return false;
}


#ifndef _WIN32
#define HOSTENT hostent
#include <netdb.h>
#endif

int check_set(const char *path, int value)
{
#ifdef __linux__
    FILE *f = fopen(path,"r");
    char res[32];
    char *r = 0;
    int si = 0;
    if (f) {
        r = fgets(res, sizeof(res), f);
        fclose(f);
    }
    if (r)
        si = atoi(r);
    if (!si)
    {
        OWARNLOG("WARNING: Failed to read value for %s", path);
        return 0;
    }
    else if (si<value)
        return -1;
#endif
    return 0;
}

int check_max_socket_read_buffer(int size) {
    return check_set("/proc/sys/net/core/rmem_max", size);
}
int check_max_socket_write_buffer(int size) {
    return check_set("/proc/sys/net/core/wmem_max", size);
}

#if defined( __linux__) || defined(__APPLE__)
void setLinuxThreadPriority(int level)
{
    pthread_t self = pthread_self();
    int policy;
    sched_param param;
    int rc;
    if (( rc = pthread_getschedparam(self, &policy, &param)) != 0) 
        DBGLOG("pthread_getschedparam error: %d", rc);
    if (level < 0)
        UNIMPLEMENTED;
    else if (!level)
    {
        param.sched_priority = 0;
        policy = SCHED_OTHER;
    }
    else
    {
        policy = SCHED_RR;
        param.sched_priority = level;
    }
    if(( rc = pthread_setschedparam(self, policy, &param)) != 0) 
        DBGLOG("pthread_setschedparam error: %d policy=%i pr=%i id=%" I64F "i TID=%i", rc, policy, param.sched_priority, (unsigned __int64) self, threadLogID());
    else
        DBGLOG("priority set id=%" I64F "i policy=%i pri=%i TID=%i", (unsigned __int64) self, policy, param.sched_priority, threadLogID());
}
#endif


extern UDPLIB_API void queryMemoryPoolStats(StringBuffer &memStats)
{
    if (bufferManager)
        bufferManager->poolStats(memStats);
}

RelaxedAtomic<unsigned> packetsOOO;

bool PacketTracker::noteSeen(UdpPacketHeader &hdr)
{
    bool resent = false;
    sequence_t seq = hdr.sendSeq;
    if (hdr.pktSeq & UDP_PACKET_RESENT)
        resent = true;
    // Four cases: less than lastUnseen, equal to, within TRACKER_BITS of, or higher
    // Be careful to think about wrapping. Less than and higher can't really be distinguished, but we treat resent differently from original
    bool duplicate = false;
    unsigned delta = seq - base;
    if (udpTraceLevel > 5)
    {
        DBGLOG("PacketTracker::noteSeen %" SEQF "u: delta %d", hdr.sendSeq, delta);
        dump();
    }
    if (delta < TRACKER_BITS)
    {
        unsigned idx = (seq / 64) % TRACKER_DWORDS;
        unsigned bit = seq % 64;
        __uint64 bitm = U64C(1)<<bit;
        duplicate = (seen[idx] & bitm) != 0;
        seen[idx] |= bitm;
        if (seq==base)
        {
            while (seen[idx] & bitm)
            {
                // Important to update in this order, so that during the window where they are inconsistent we have
                // false negatives rather than false positives
                seen[idx] &= ~bitm;
                base++;
                idx = (base / 64) % TRACKER_DWORDS;
                bit = base % 64;
                bitm = U64C(1)<<bit;
            }
        }
        // calculate new hwm, with some care for wrapping
        if ((int) (seq - hwm) > 0)
            hwm = seq;
        else if (!resent)
            packetsOOO++;
    }
    else if (resent)
        // Don't treat a resend that goes out of range as indicative of a restart - it probably just means
        // that the resend was not needed and the original moved things on when it arrived
        duplicate = true;
    else
    {
        // We've gone forwards too far to track, or backwards because server restarted
        // We have taken steps to try to avoid the former...
        // In theory could try to preserve SOME information in the former case, but as it shouldn't happen, can we be bothered?
#ifdef _DEBUG
        if (udpResendLostPackets)
        {
            DBGLOG("Received packet %" SEQF "u will cause loss of information in PacketTracker", seq);
            dump();
        }
        //assert(false);
#endif
        memset(seen, 0, sizeof(seen));
        base = seq+1;
        hwm = seq;
    }
    return duplicate;
}

const PacketTracker PacketTracker::copy() const
{
    // This is called within a critical section. Would be better if we could avoid having to do so,
    // but we want to be able to read a consistent set of values
    PacketTracker ret;
    ret.base = base;
    ret.hwm = hwm;
    memcpy(ret.seen, seen, sizeof(seen));
    return ret;
}

bool PacketTracker::hasSeen(sequence_t seq) const
{
    // Accessed only on sender side where these are not modified, so no need for locking
    // Careful about wrapping!
    unsigned delta = seq - base;
    if (udpTraceLevel > 5)
    {
       DBGLOG("PacketTracker::hasSeen - have I seen %" SEQF "u, %d", seq, delta);
       dump();
    }
    if (delta < TRACKER_BITS)
    {
        unsigned idx = (seq / 64) % TRACKER_DWORDS;
        unsigned bit = seq % 64;
        return (seen[idx] & (U64C(1)<<bit)) != 0;
    }
    else if (delta > INT_MAX)  // Or we could just make delta a signed int? But code above would have to check >0
        return true;
    else
        return false;
}

bool PacketTracker::canRecord(sequence_t seq) const
{
    // Careful about wrapping!
    unsigned delta = seq - base;
    if (udpTraceLevel > 5)
    {
       DBGLOG("PacketTracker::hasSeen - can I record %" SEQF "u, %d", seq, delta);
       dump();
    }
    return (delta < TRACKER_BITS);
}

bool PacketTracker::hasGaps() const
{
    return base!=hwm+1;
}

void PacketTracker::dump() const
{
    DBGLOG("PacketTracker base=%" SEQF "u, hwm=%" SEQF "u, seen[0]=%" I64F "x", base, hwm, seen[0]);
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class PacketTrackerTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PacketTrackerTest);
    CPPUNIT_TEST(testNoteSeen);
    CPPUNIT_TEST(testReplay);
    CPPUNIT_TEST_SUITE_END();

    void testNoteSeen()
    {
        PacketTracker p;
        UdpPacketHeader hdr;
        hdr.pktSeq = 0;
        // Some simple tests
        CPPUNIT_ASSERT(!p.hasSeen(0));
        CPPUNIT_ASSERT(!p.hasSeen(1));
        hdr.sendSeq = 0;
        CPPUNIT_ASSERT(!p.noteSeen(hdr));
        CPPUNIT_ASSERT(p.hasSeen(0));
        CPPUNIT_ASSERT(!p.hasSeen(1));
        CPPUNIT_ASSERT(!p.hasSeen(2000));
        CPPUNIT_ASSERT(!p.hasSeen(2001));
        hdr.pktSeq = UDP_PACKET_RESENT;
        CPPUNIT_ASSERT(p.noteSeen(hdr));
        hdr.pktSeq = 0;
        hdr.sendSeq = 2000;
        CPPUNIT_ASSERT(!p.noteSeen(hdr));
        CPPUNIT_ASSERT(p.hasSeen(0));
        CPPUNIT_ASSERT(p.hasSeen(1));
        CPPUNIT_ASSERT(p.hasSeen(2000));
        CPPUNIT_ASSERT(!p.hasSeen(2001));
        hdr.sendSeq = 0;
        CPPUNIT_ASSERT(!p.noteSeen(hdr));
        CPPUNIT_ASSERT(p.hasSeen(0));
        CPPUNIT_ASSERT(!p.hasSeen(1));
        CPPUNIT_ASSERT(!p.hasSeen(2000));
        CPPUNIT_ASSERT(!p.hasSeen(2001));

        PacketTracker p2;
        hdr.sendSeq = 1;
        CPPUNIT_ASSERT(!p2.noteSeen(hdr));
        CPPUNIT_ASSERT(!p2.hasSeen(0));
        CPPUNIT_ASSERT(p2.hasSeen(1));
        hdr.sendSeq = TRACKER_BITS-1;  // This is the highest value we can record without losing information
        CPPUNIT_ASSERT(!p2.noteSeen(hdr));
        CPPUNIT_ASSERT(!p2.hasSeen(0));
        CPPUNIT_ASSERT(p2.hasSeen(1));
        CPPUNIT_ASSERT(p2.hasSeen(TRACKER_BITS-1));
        CPPUNIT_ASSERT(!p2.hasSeen(TRACKER_BITS));
        CPPUNIT_ASSERT(!p2.hasSeen(TRACKER_BITS+1));
        hdr.sendSeq = TRACKER_BITS;
        p2.noteSeen(hdr);
        CPPUNIT_ASSERT(p2.hasSeen(0));
        CPPUNIT_ASSERT(p2.hasSeen(1));
        CPPUNIT_ASSERT(p2.hasSeen(TRACKER_BITS-1));
        CPPUNIT_ASSERT(p2.hasSeen(TRACKER_BITS));
        CPPUNIT_ASSERT(!p2.hasSeen(TRACKER_BITS+1));
        CPPUNIT_ASSERT(!p2.hasSeen(TRACKER_BITS+2));
    }

    void t(PacketTracker &p, sequence_t seq, unsigned pseq)
    {
        UdpPacketHeader hdr;
        hdr.sendSeq = seq;
        hdr.pktSeq = pseq;
        if (seq==29)
            CPPUNIT_ASSERT(p.noteSeen(hdr) == false);
        else
            p.noteSeen(hdr);
    }

    void testReplay()
    {
        PacketTracker p;
        t(p,1,0x1);
        t(p,2,0x2);
        t(p,3,0x3);
        t(p,4,0x4);
        t(p,5,0x5);
        t(p,6,0x6);
        t(p,7,0x7);
        t(p,8,0x8);
        t(p,9,0x9);
        t(p,11,0xb);
        t(p,12,0xc);
        t(p,13,0xd);
        t(p,14,0xe);
        t(p,15,0xf);
        t(p,16,0x10);
        t(p,17,0x11);
        t(p,18,0x12);
        t(p,19,0x13);
        t(p,20,0x14);
        t(p,21,0x15);
        t(p,22,0x16);
        t(p,23,0x17);
        t(p,24,0x18);
        t(p,25,0x19);
        t(p,26,0x1a);
        t(p,27,0x1b);
        t(p,28,0x1c);
        t(p,50,0x40000032);
        t(p,51,0x40000033);
        t(p,52,0x40000034);
        t(p,53,0x40000035);
        t(p,54,0x40000036);
        t(p,55,0x40000037);
        t(p,56,0x40000038);
        t(p,57,0x40000039);
        t(p,58,0x4000003a);
        t(p,59,0x4000003b);
        t(p,60,0x4000003c);
        t(p,61,0x4000003d);
        t(p,62,0xc0000000);
        t(p,63,0x4000003e);
        t(p,64,0x4000003f);
        t(p,65,0x40000040);
        t(p,66,0x40000041);
        t(p,67,0x40000042);
        t(p,68,0x40000043);
        t(p,69,0x40000044);
        t(p,70,0x40000045);
        t(p,71,0x40000046);
        t(p,72,0x40000047);
        t(p,73,0x40000048);
        t(p,74,0x40000049);
        t(p,75,0x4000004a);
        t(p,76,0x4000004b);
        t(p,77,0x4000004c);
        t(p,78,0x4000004d);
        t(p,79,0x4000004e);
        t(p,80,0x4000004f);
        t(p,81,0x40000050);
        t(p,82,0x40000051);
        t(p,83,0x40000052);
        t(p,84,0x40000053);
        t(p,85,0x40000054);
        t(p,86,0x40000055);
        t(p,87,0x40000056);
        t(p,88,0x40000057);
        t(p,89,0x40000058);
        t(p,90,0x40000059);
        t(p,91,0x4000005a);
        t(p,92,0x4000005b);
        t(p,93,0x4000005c);
        t(p,0,0x40000000);
        t(p,1,0x40000001);
        t(p,2,0x40000002);
        t(p,3,0x40000003);
        t(p,4,0x40000004);
        t(p,5,0x40000005);
        t(p,6,0x40000006);
        t(p,7,0x40000007);
        t(p,8,0x40000008);
        t(p,9,0x40000009);
        t(p,10,0x4000000a);
        t(p,11,0x4000000b);
        t(p,12,0x4000000c);
        t(p,13,0x4000000d);
        t(p,14,0x4000000e);
        t(p,15,0x4000000f);
        t(p,16,0x40000010);
        t(p,17,0x40000011);
        t(p,18,0x40000012);
        t(p,19,0x40000013);
        t(p,20,0x40000014);
        t(p,21,0x40000015);
        t(p,22,0x40000016);
        t(p,23,0x40000017);
        t(p,24,0x40000018);
        t(p,25,0x40000019);
        t(p,26,0x4000001a);
        t(p,27,0x4000001b);
        t(p,28,0x4000001c);
        t(p,29,0x4000001d);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( PacketTrackerTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( PacketTrackerTest, "PacketTrackerTest" );

#endif

/*
Crazy thoughts on network-wide flow control

Avoid sending data that clashes with other outbound or inbound data
    is outbound really an issue?
    if only inbound, should be easier
        can have each inbound node police its own, for a start
            udplib already tries to do this
        when sending permission to send, best to pick someone that is not sending to anyone else
            udplib already tries to do this
            but it can still lead to idleness - id node 1 sending to node 2, and node2 to node 1, node3 can't find anyone idle.


If you do need global:
  Every bit of data getting sent (perhaps over a certain size threshold?) gets permission from central traffic cop
  Outbound packet says source node, target node size
  Reply says source,target,size
  Cop allows immediately if nothing inflight between those pairs
  Cop assumes completion 
  Cop redundancy
   - a backup cop is listening in?
     - use multicast for requests and replies?
   - no reply implies what?
   - backup cop just needs heartbeat from active cop
   - permission expires
   - multiple cops for blocks of targets?
    - but I want global view of who is sending 


*/

/* Simulating traffic flow

N threads that simulate behaviour of agents
fake write socket that
- accepts data pushed to it
- moves it to a read socket
fake read socket that
- accepts packets from write sockets
- discards when full (but tells you)
- delivers packets to consumer

*/

#ifdef SOCKET_SIMULATION
bool isUdpTestMode = false;


CSimulatedWriteSocket* CSimulatedWriteSocket::udp_connect(const SocketEndpoint &ep)
{
    return new CSimulatedWriteSocket(ep);
}

size32_t CSimulatedWriteSocket::write(void const* buf, size32_t size)
{
    CriticalBlock b(CSimulatedReadSocket::allReadersCrit);
    CSimulatedReadSocket *dest = CSimulatedReadSocket::connectSimulatedSocket(destEp);
    if (dest)
        dest->writeSimulatedPacket(buf, size);
    else
    {
        StringBuffer s;
        DBGLOG("Write to disconnected socket %s", destEp.getUrlStr(s).str());
    }
    return size;
}

std::map<SocketEndpoint, CSimulatedReadSocket *> CSimulatedReadSocket::allReaders;
CriticalSection CSimulatedReadSocket::allReadersCrit;

CSimulatedReadSocket::CSimulatedReadSocket(const SocketEndpoint &_me) : me(_me)
{
    StringBuffer s;
    DBGLOG("Creating fake socket %s", me.getUrlStr(s).str());
    CriticalBlock b(allReadersCrit);
    allReaders[me] = this;
}

CSimulatedReadSocket::~CSimulatedReadSocket()
{
    StringBuffer s;
    DBGLOG("Closing fake socket %s", me.getUrlStr(s).str());
    CriticalBlock b(allReadersCrit);
    allReaders.erase(me);
}

CSimulatedReadSocket* CSimulatedReadSocket::udp_create(const SocketEndpoint &_me)
{
    return new CSimulatedReadSocket(_me);
}

CSimulatedReadSocket* CSimulatedReadSocket::connectSimulatedSocket(const SocketEndpoint &ep)
{
    CriticalBlock b(allReadersCrit);
    return allReaders[ep];
}

void CSimulatedReadSocket::writeSimulatedPacket(void const* buf, size32_t size)
{
    {
        CriticalBlock b(crit);
        if (size+used > max)
        {
            DBGLOG("Lost packet");
            return;
        }
        packetSizes.push(size);
        packets.push(memcpy(malloc(size), buf, size));
        used += size;
    }
//    StringBuffer s; DBGLOG("Signalling available data on %s", me.getUrlStr(s).str());
    avail.signal();
}

void CSimulatedReadSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs)
{
    unsigned tms = timeoutsecs == WAIT_FOREVER ? WAIT_FOREVER : timeoutsecs * 1000;
    readtms(buf, min_size, max_size, size_read, tms);
}

void CSimulatedReadSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                     unsigned timeout)
{
    size_read = 0;
    if (!timeout || wait_read(timeout))
    {
        CriticalBlock b(crit);
        const void *thisData = packets.front();
        unsigned thisSize = packetSizes.front();
        if (thisSize > max_size)
        {
            assert(false);
            UNIMPLEMENTED;  // Partial packet read not supported yet - add if needed
        }
        else
        {
            packets.pop();
            packetSizes.pop();
            used -= thisSize;
        }
        size_read = thisSize;
        memcpy(buf, thisData, thisSize);
        free((void *) thisData);
    }
    else
        throw makeStringException(JSOCKERR_timeout_expired, "");
}

int CSimulatedReadSocket::wait_read(unsigned timeout)
{
    bool ret = avail.wait(timeout);
//    if (me.port==9001)
  //      MilliSleep(1);
    return ret;
}

#ifdef _USE_CPPUNIT

class SimulatedUdpStressTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(SimulatedUdpStressTest);
    CPPUNIT_TEST(simulateTraffic);
    CPPUNIT_TEST_SUITE_END();

    Owned<IDataBufferManager> dbm;
    bool initialized = false;

    void testInit()
    {
        if (!initialized)
        {
            udpTraceLevel = 1;
            udpTraceTimeouts = true;
            udpResendLostPackets = true;
            udpRequestToSendTimeout = 1000;
            udpRequestToSendAckTimeout = 1000;
            udpMaxPendingPermits = 10;
            isUdpTestMode = true;
            roxiemem::setTotalMemoryLimit(false, false, false, 20*1024*1024, 0, NULL, NULL);
            dbm.setown(roxiemem::createDataBufferManager(roxiemem::DATA_ALIGNMENT_SIZE));
            initialized = true;
        }
    }

    void simulateTraffic()
    {
        testInit();
        myNode.setIp(IpAddress("1.2.3.4"));
        Owned<IReceiveManager> rm = createReceiveManager(CCD_DATA_PORT, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, 50, 50, false);
        printf("Start test\n");
        asyncFor(20, 20, [](unsigned i)
        {
            unsigned header = 0;
            IpAddress pretendIP(VStringBuffer("8.8.8.%d", i));
            // Note - this is assuming we send flow on the data port (that option defaults true in roxie too)
            Owned<ISendManager> sm = createSendManager(CCD_DATA_PORT, CCD_DATA_PORT, CCD_CLIENT_FLOW_PORT, 100, 3, pretendIP, nullptr, false);
            Owned<IMessagePacker> mp = sm->createMessagePacker(0, 0, &header, sizeof(header), myNode, 0);
            for (unsigned i = 0; i < 10000; i++)
            {
                void *buf = mp->getBuffer(500, false);
                memset(buf, i, 500);
                mp->putBuffer(buf, 500, false);
            }
            mp->flush();
            Sleep(1000);
        });
        printf("End test\n");
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( SimulatedUdpStressTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( SimulatedUdpStressTest, "SimulatedUdpStressTest" );

#endif
#endif
