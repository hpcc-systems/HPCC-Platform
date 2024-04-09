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

// All exported udp configuration options - these values provide the default values
#ifdef TEST_DROPPED_PACKETS
bool udpDropDataPackets = false;
unsigned udpDropDataPacketsPercent = 0;
unsigned udpDropFlowPackets[flowType::max_flow_cmd] = {};
unsigned flowPacketsSent[flowType::max_flow_cmd] = {};
#endif

bool udpTraceFlow = false;
bool udpTraceTimeouts = false;

unsigned udpTraceLevel = 0;
unsigned udpFlowSocketsSize = 131072;
unsigned udpLocalWriteSocketSize = 1024000;
unsigned udpStatsReportInterval = 60000;

unsigned udpOutQsPriority = 0;
unsigned udpSendTraceThresholdMs = 50;

unsigned udpMaxPermitDeadTimeouts = 5;  // How many permit grants are allowed to expire (with no flow message) until request is ignored
unsigned udpRequestDeadTimeout = 10000; // Timeout for sender getting no response to request to send before assuming that the receiver is dead.


//The following control the timeouts within the udp layer.  All timings are in milliseconds, but I suspect some of these should possibly be sub-millisecond
//The following timeouts are described in more detail in a comment at the head of udptrr.cpp
unsigned udpFlowAckTimeout = 2;         // [sender] the maximum time that it is expected to take to receive an acknowledgement of a flow message (when one is sent) - should be small
unsigned updDataSendTimeout = 20;       // [sender+receiver] how long to receive the maximum amount of data, ~100 packets of 8K should take 10ms on a 1Gb network. Timeout for assuming send_complete has been lost
unsigned udpRequestTimeout = 20;        // [sender] A reasonable expected time between a request for a permit until the permit is granted - used as a timeout to guard against an ok_to_send has been lost.
unsigned udpPermitTimeout = 50;         // [receiver] How long is a grant expected to last before it is assumed lost?
unsigned udpResendDelay = 0;            // [sender+receiver] How long should elapse after a data packet has been sent before we assume it is lost.
                                        // 0 means they are unlikely to be lost, so worth resending as soon as it appears to be missing - trading duplicate packets for delays (good if allowasync=false)

unsigned udpMaxPendingPermits = 10;     // This seems like a reasonable compromise - each sender will be able to send up to 20% of the input queue each request.
unsigned udpMaxClientPercent = 600;     // What percentage of (queueSize/maxPendingPermits) should be granted to each sender.
unsigned udpMinSlotsPerSender = 1;      // The smallest number of slots to assign to a sender
bool udpResendAllMissingPackets = true; // If set do not limit the number of missing packets sent to the size of the permit.
bool udpResendLostPackets = true;       // is the code to resend lost data packets enabled?
bool udpAssumeSequential = false;       // If a data packet with a later sequence has been received is it reasonable to assume it has been lost?
bool udpAdjustThreadPriorities = false; // Adjust the priorities for the UDP receiving and sending threads so they have priority.
                                        // Enabling tends to cause a big rise in context switches from other threads, so disabled by default
bool udpAllowAsyncPermits = false;      // Allow requests to send more data to overtake the data packets that are being sent.
bool udpRemoveDuplicatePermits = true;
bool udpEncryptOnSendThread = false;

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

void queue_t::interrupt()
{
    data_avail.interrupt();
}

void queue_t::doEnqueue(DataBuffer *buf)
{
    // Must currently be called within a critical section.  Does not signal - that should be done outside the CS.
    // Could probably be done lock-free, which given one thread using this is high priority might avoid some
    // potential priority-inversion issues. Or we might consider using PI-aware futexes here?
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
    count.fastAdd(1); // inside a critical section, so no need for atomic inc.
}

void queue_t::pushOwnWait(DataBuffer * buf)
{
    assert(!buf->msgNext);
    for (;;)
    {
        {
            CriticalBlock b(c_region);

            if (count < limit)
            {
                doEnqueue(buf);
                break;  // signal outside the critical section, rather than here, so the waiting thread can progress
            }
            signal_free_sl++;
        }

        unsigned delay = 3;
        while (!free_sl.wait(delay * 1000))
        {
            if (udpTraceLevel >= 1)
                DBGLOG("queue_t::pushOwnWait blocked for %u seconds waiting for free_sl semaphore [%u/%u]", delay, count.load(), limit);
            delay *= 2;
        }
    }
    data_avail.signal();
}

void queue_t::pushOwn(DataBuffer *buf)
{
    // Could probably be done lock-free, which given one thread using this is high priority might avoid some
    // potential priority-inversion issues. Or we might consider using PI-aware futexes here?
    assert(!buf->msgNext);
    {
        buf->changeState(roxiemem::DBState::unowned, roxiemem::DBState::queued, __func__);
        CriticalBlock b(c_region);
        doEnqueue(buf);
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
        if (unlikely(!count))
            return nullptr;
        count.fastAdd(-1); // inside a critical section => not atomic
        ret = head;
        head = ret->msgNext;
        if (!head)
        {
            assert(!count);
            tail = nullptr;
        }
        if (count < limit && signal_free_sl)
        {
            signal_free_sl--;
            signalFreeSlots++;
        }
    }
    ret->msgNext = nullptr;
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
                    count.fastAdd(-1);
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
    if (!udpAdjustThreadPriorities)
        return;

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
    else if (resent && base)
        // Don't treat a resend that goes out of range as indicative of a restart - it probably just means
        // that the resend was not needed and the original moved things on when it arrived. Unless base is 0
        // in which case it probably means I restarted
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

//---------------------------------------------------------------------------------------------------------------------

void sanityCheckUdpSettings(unsigned receiveQueueSize, unsigned sendQueueSize, unsigned numSenders, __uint64 networkSpeedBitsPerSecond)
{
    unsigned maxDataPacketSize = 0x2000;    // assume jumbo frames roxiemem::DATA_ALIGNMENT_SIZE;
    __uint64 bytesPerSecond = networkSpeedBitsPerSecond / 10;

    unsigned __int64 minPacketTimeNs = (maxDataPacketSize * U64C(1000000000)) / bytesPerSecond;
    unsigned __int64 minLatencyNs = 50000;
    unsigned maxSlotsPerClient = (udpMaxPendingPermits == 1) ? receiveQueueSize : (udpMaxClientPercent * receiveQueueSize) / (udpMaxPendingPermits * 100);
    unsigned __int64 minTimeForAllPackets = receiveQueueSize * minPacketTimeNs;
    //The data for a permit may arrive after the data from all the other senders => need to take the entire queue into account
    unsigned __int64 minTimeForPermitPackets = minTimeForAllPackets;

    auto trace = [](const char * title, unsigned value, unsigned __int64 minValue, unsigned maxFactor)
    {
        DBGLOG("%s: %u [%u..%u]  us: %u [%u..%u]", title, value, (unsigned)(minValue/1000000), (unsigned)(minValue*maxFactor/1000000),
                                                          value*1000, (unsigned)(minValue/1000), (unsigned)(minValue*maxFactor/1000));
    };

    //MORE: Allow the udpReceiverSize to be defined and finish implementing the following, with some comments to describe the thinking
    // All in milliseconds
    if (udpTraceTimeouts || udpTraceLevel >= 1)
    {
        DBGLOG("udpAssumeSequential: %s", boolToStr(udpAssumeSequential));
        DBGLOG("udpResendLostPackets: %s", boolToStr(udpResendLostPackets));
        DBGLOG("udpResendAllMissingPackets: %s", boolToStr(udpResendAllMissingPackets));
        DBGLOG("udpAdjustThreadPriorities: %s", boolToStr(udpAdjustThreadPriorities));
        DBGLOG("udpAllowAsyncPermits: %s", boolToStr(udpAllowAsyncPermits));
        trace("udpFlowAckTimeout", udpFlowAckTimeout, minLatencyNs*2, 20);
        trace("updDataSendTimeout", updDataSendTimeout, minTimeForAllPackets, 10);
        trace("udpPermitTimeout", udpPermitTimeout, 2 * minLatencyNs + minTimeForPermitPackets, 10);
        trace("udpRequestTimeout", udpRequestTimeout, (2 * minLatencyNs + minTimeForPermitPackets) * 2 / 5, 10);
        trace("udpResendDelay", udpResendDelay, minTimeForAllPackets, 10);
        DBGLOG("udpMaxPendingPermits: %u [%u..%u]", udpMaxPendingPermits, udpMaxPendingPermits, udpMaxPendingPermits);
        DBGLOG("udpMaxClientPercent: %u [%u..%u]", udpMaxClientPercent, 100, udpMaxPendingPermits * 100);
        DBGLOG("udpMaxPermitDeadTimeouts: %u [%u..%u]", udpMaxPermitDeadTimeouts, 2, 10);
        DBGLOG("udpRequestDeadTimeout: %u [%u..%u]", udpRequestDeadTimeout, 10000, 120000);
        DBGLOG("udpMinSlotsPerSender: %u [%u..%u]", udpMinSlotsPerSender, 1, 5);
        DBGLOG("Queue sizes: send(%u) receive(%u)", sendQueueSize, receiveQueueSize);
    }

    // Some sanity checks
    if (!udpResendLostPackets)
        WARNLOG("udpResendLostPackets is currently disabled - only viable on a very reliable network");
    if (udpAllowAsyncPermits)
    {
        if (udpResendDelay == 0)
            ERRLOG("udpResendDelay of 0 should not be used if udpAllowAsyncPermits=true");
    }
    else
    {
        if (udpResendDelay != 0)
            WARNLOG("udpResendDelay of 0 is recommended if udpAllowAsyncPermits=false");
    }
    if (udpFlowAckTimeout == 0)
    {
        ERRLOG("udpFlowAckTimeout should not be set to 0");
        udpFlowAckTimeout = 1;
    }
    if (udpRequestTimeout == 0)
    {
        ERRLOG("udpRequestTimeout should not be set to 0");
        udpFlowAckTimeout = 10;
    }
    if (udpMaxPendingPermits > receiveQueueSize)
        throwUnexpectedX("udpMaxPendingPermits > receiveQueueSize");
    if (maxSlotsPerClient == 0)
        throwUnexpectedX("maxSlotsPerClient == 0");

    if (udpFlowAckTimeout * 10 > udpRequestTimeout)
        WARNLOG("udpFlowAckTimeout should be significantly smaller than udpRequestTimeout");
    if (udpRequestTimeout >= udpPermitTimeout)
        WARNLOG("udpRequestTimeout should be lower than udpPermitTimeout, otherwise dropped ok_to_send will not be spotted early enough");
    if (udpMaxPendingPermits == 1)
        WARNLOG("udpMaxPendingPermits=1: only one sender can send at a time");
    if (udpMaxClientPercent < 100)
        ERRLOG("udpMaxClientPercent should be >= 100");
    else if (maxSlotsPerClient > receiveQueueSize)
        ERRLOG("maxSlotsPerClient * udpMaxClientPercent exceeds the queue size => all slots will be initially allocated to the first sender");
    if (udpMinSlotsPerSender > 10)
        ERRLOG("udpMinSlotsPerSender of %u is higher than recommended", udpMinSlotsPerSender);
}

//---------------------------------------------------------------------------------------------------------------------
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
bool udpTestUseUdpSockets = true;
bool udpTestSocketJitter = false;
unsigned udpTestSocketDelay = 0;
bool udpTestVariableDelay = false;


static CriticalSection allWriteSocketsCrit;
static ICopyArrayOf<CSimulatedQueueWriteSocket> allWriteSockets;

class DelayedSocketWriter : public Thread
{
public:
    virtual int run() override
    {
        while (running)
        {
            unsigned shortestDelay = udpTestSocketDelay;
            unsigned now = msTick();
            {
                CriticalBlock b(allWriteSocketsCrit);
                ForEachItemIn(idx, allWriteSockets)
                {
                    CSimulatedQueueWriteSocket &ws = allWriteSockets.item(idx);
                    shortestDelay = std::min(shortestDelay, ws.writeDelayed(now));
                }
            }
            MilliSleep(shortestDelay);
        }
        return 0;
    }
    virtual void start(bool inheritThreadContext) override
    {
        running = true;
        Thread::start(inheritThreadContext);
    }
    void stop()
    {
        running = false;
        join();
    }
private:
    std::atomic<bool> running = { false };
} delayedWriter;

CSimulatedQueueWriteSocket::CSimulatedQueueWriteSocket(const SocketEndpoint &ep) : destEp(ep), delay(udpTestSocketDelay), jitter(udpTestSocketJitter)
{
    if (delay)
    {
        CriticalBlock b(allWriteSocketsCrit);
        if (!allWriteSockets.length())
            delayedWriter.start(false);
        allWriteSockets.append(*this);
    }
}

CSimulatedQueueWriteSocket::~CSimulatedQueueWriteSocket()
{
    if (delay)
    {
        CriticalBlock b(allWriteSocketsCrit);
        allWriteSockets.zap(*this);
        if (!allWriteSockets.length())
            delayedWriter.stop();
    }
}

CSimulatedQueueWriteSocket* CSimulatedQueueWriteSocket::udp_connect(const SocketEndpoint &ep)
{
    return new CSimulatedQueueWriteSocket(ep);
}

unsigned CSimulatedQueueWriteSocket::writeDelayed(unsigned now)
{
    CriticalBlock b(crit);
    while (dueTimes.size())
    {
        int delay = dueTimes.front() - now;
        if (delay > 0)
            return delay;
        unsigned jitteredSize = 0;
        const void *jitteredBuff = nullptr;
        if (jitter && dueTimes.size()>1 && rand() % 100 == 0)
        {
            jitteredSize = packetSizes.front();
            jitteredBuff = packets.front();
            dueTimes.pop();
            packets.pop();
            packetSizes.pop();
        }
        CriticalBlock b(CSimulatedQueueReadSocket::allReadersCrit);
        CSimulatedQueueReadSocket *dest = CSimulatedQueueReadSocket::connectSimulatedSocket(destEp);
        if (dest)
        {
            dest->writeOwnSimulatedPacket(packets.front(), packetSizes.front());
            if (jitteredBuff)
                dest->writeOwnSimulatedPacket(jitteredBuff, jitteredSize);
        }
        else
        {
            StringBuffer s;
            free((void *) packets.front());
            if (jitteredBuff)
                free((void *) jitteredBuff);
            DBGLOG("Write to disconnected socket %s", destEp.getEndpointHostText(s).str());
        }
        dueTimes.pop();
        packets.pop();
        packetSizes.pop();
    }
    return (unsigned) -1;
}

size32_t CSimulatedQueueWriteSocket::write(void const* buf, size32_t size)
{
    if (delay)
    {
        CriticalBlock b(crit);
        packetSizes.push(size);
        packets.push(memcpy(malloc(size), buf, size));
        dueTimes.push(msTick() + delay * (udpTestVariableDelay && size>200 ? 1 : 3));
    }
    else
    {
        CriticalBlock b(CSimulatedQueueReadSocket::allReadersCrit);
        CSimulatedQueueReadSocket *dest = CSimulatedQueueReadSocket::connectSimulatedSocket(destEp);
        if (dest)
            dest->writeSimulatedPacket(buf, size);
        else
        {
            StringBuffer s;
            DBGLOG("Write to disconnected socket %s", destEp.getEndpointHostText(s).str());
        }
    }
    return size;
}

std::map<SocketEndpoint, CSimulatedQueueReadSocket *> CSimulatedQueueReadSocket::allReaders;
CriticalSection CSimulatedQueueReadSocket::allReadersCrit;

CSimulatedQueueReadSocket::CSimulatedQueueReadSocket(const SocketEndpoint &_me) : me(_me)
{
    StringBuffer s;
    DBGLOG("Creating fake socket %s", me.getEndpointHostText(s).str());
    CriticalBlock b(allReadersCrit);
    allReaders[me] = this;
}

CSimulatedQueueReadSocket::~CSimulatedQueueReadSocket()
{
    StringBuffer s;
    DBGLOG("Closing fake socket %s", me.getEndpointHostText(s).str());
    CriticalBlock b(allReadersCrit);
    allReaders.erase(me);
}

CSimulatedQueueReadSocket* CSimulatedQueueReadSocket::udp_create(const SocketEndpoint &_me)
{
    return new CSimulatedQueueReadSocket(_me);
}

CSimulatedQueueReadSocket* CSimulatedQueueReadSocket::connectSimulatedSocket(const SocketEndpoint &ep)
{
    CriticalBlock b(allReadersCrit);
    return allReaders[ep];
}

void CSimulatedQueueReadSocket::writeSimulatedPacket(void const* buf, size32_t size)
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
//    StringBuffer s; DBGLOG("Signalling available data on %s", me.getEndpointHostText(s).str());
    avail.signal();
}

void CSimulatedQueueReadSocket::writeOwnSimulatedPacket(void const* buf, size32_t size)
{
    {
        CriticalBlock b(crit);
        if (size+used > max)
        {
            DBGLOG("Lost packet");
            free((void *) buf);
            return;
        }
        packetSizes.push(size);
        packets.push(buf);
        used += size;
    }
//    StringBuffer s; DBGLOG("Signalling available data on %s", me.getEndpointHostText(s).str());
    avail.signal();
}

void CSimulatedQueueReadSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs, bool suppresGCIfMinSize)
{
    unsigned tms = timeoutsecs == WAIT_FOREVER ? WAIT_FOREVER : timeoutsecs * 1000;
    readtms(buf, min_size, max_size, size_read, tms);
}

void CSimulatedQueueReadSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read,
                     unsigned timeout, bool suppresGCIfMinSize)
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

int CSimulatedQueueReadSocket::wait_read(unsigned timeout)
{
    bool ret = avail.wait(timeout);
    return ret;
}


//------------------------------------------------------------------------------------------------------------------------------

//Hash the ip and port and map that to a local port - complain if more than one combination is hashed to the same port.
constexpr unsigned basePort = 9010;
constexpr unsigned maxPorts = 980;
static std::atomic_bool connected[maxPorts];
unsigned getMappedSocketPort(const SocketEndpoint & ep)
{
    unsigned hash = ep.hash(0x31415926);
    return basePort + hash % maxPorts;
}

CSimulatedUdpReadSocket::CSimulatedUdpReadSocket(const SocketEndpoint &_me)
{
    port = getMappedSocketPort(_me);
    if (connected[port-basePort].exchange(true))
        throw makeStringException(0, "Two ip/ports mapped to the same port - improve the hash (or change maxPorts)!");
    realSocket.setown(ISocket::udp_create(port));
}

CSimulatedUdpReadSocket::~CSimulatedUdpReadSocket()
{
    connected[port-basePort].exchange(false);
}

size32_t CSimulatedUdpReadSocket::get_receive_buffer_size() { return realSocket->get_receive_buffer_size(); }
void CSimulatedUdpReadSocket::set_receive_buffer_size(size32_t sz) { realSocket->set_receive_buffer_size(sz); }
void CSimulatedUdpReadSocket::read(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeoutsecs, bool suppresGCIfMinSize)
{
    realSocket->read(buf, min_size, max_size, size_read, timeoutsecs);
}
void CSimulatedUdpReadSocket::readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, unsigned timeout, bool suppresGCIfMinSize)
{
    realSocket->readtms(buf, min_size, max_size, size_read, timeout);
}
int CSimulatedUdpReadSocket::wait_read(unsigned timeout)
{
    return realSocket->wait_read(timeout);
}
void CSimulatedUdpReadSocket::close()
{
    realSocket->close();
}

CSimulatedUdpReadSocket* CSimulatedUdpReadSocket::udp_create(const SocketEndpoint &_me) { return new CSimulatedUdpReadSocket(_me); }

CSimulatedUdpWriteSocket::CSimulatedUdpWriteSocket( const SocketEndpoint &ep)
{
    unsigned port = getMappedSocketPort(ep);
    SocketEndpoint localEp(port, queryLocalIP());
    realSocket.setown(ISocket::udp_connect(localEp));
}

CSimulatedUdpWriteSocket*  CSimulatedUdpWriteSocket::udp_connect( const SocketEndpoint &ep) { return new CSimulatedUdpWriteSocket(ep); }

size32_t CSimulatedUdpWriteSocket::write(void const* buf, size32_t size)
{
    return realSocket->write(buf, size);
}
void CSimulatedUdpWriteSocket::set_send_buffer_size(size32_t sz)
{
    realSocket->set_send_buffer_size(sz);
}
void CSimulatedUdpWriteSocket::close()
{
    realSocket->close();
}

#endif
