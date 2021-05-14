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
#include "udptrs.hpp"
#include "udpipmap.hpp"

#include "jsocket.hpp"
#include "jlog.hpp"
#include "jencrypt.hpp"
#include "jsecrets.hpp"
#include "roxie.hpp"
#ifdef _WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <math.h>
#include <atomic>

unsigned udpOutQsPriority = 0;
unsigned udpMaxRetryTimedoutReqs = 0; // 0 means off (keep retrying forever)
unsigned udpRequestToSendTimeout = 0; // value in milliseconds - 0 means calculate from query timeouts
unsigned udpRequestToSendAckTimeout = 10; // value in milliseconds

#ifdef _DEBUG
//#define TEST_DROPPED_PACKETS
#endif

using roxiemem::DataBuffer;
/*
 *
 * There are 3 threads running to manage the data transfer from agent back to server:
 * send_resend_flow
 *   - checks periodically that nothing is waiting for a "request to send" that timed out
 * send_receive_flow
 *   - waits on socket receiving "ok_to_send" packets from servers
 *   - updates state of relevant receivers
 *   - pushes permission tokens to a queue
 * send_data
 *   - waits on queue of permission tokens
 *   - broadcasts "busy"
 *   - writes data to server
 *   - broadcasts "no longer "
 *   - sends "completed" or "completed but I want to send more" flow message to server
 *
 * Queueing up data packets is done by the agent worker threads.
 * *

 *
 * Data races to watch for
 * 1. Two agent threads add data at same time - only one should sent rts (use atomic inc for the count)
 * 2. We check for timeout and resend rts or fail just as permission comes in
 *    - resend rts is harmless ?
 *    - fail is acceptable
 * 3. After sending data, we need to decide whether to set state to 'pending' (and send rts) or empty. If we read count, decide it's zero
 *    and then (before we set state) someone adds data (and sends rts), we must not set state to empty. CAS to set state empty only if
 *    it's sending_data perhaps?
 * 4. While sending data, someone adds new data. They need to send rts and set state to pending whether empty or sending_data
 * 5. Do we need sending_data state? Is it the same as empty, really? Is empty the same as 'count==0' ? Do we even need state?
 *    - send rts whenever incrementing count from zero
 *    - resend rts if count is non-zero and timed out
 *    - resend rts if we send data but there is some remaining
 */

// UdpResentList keeps a copy of up to TRACKER_BITS previously sent packets so we can send them again
RelaxedAtomic<unsigned> packetsResent;
RelaxedAtomic<unsigned> flowRequestsSent;
RelaxedAtomic<unsigned> flowPermitsReceived;
RelaxedAtomic<unsigned> dataPacketsSent;

unsigned udpResendTimeout;  // in millseconds
bool udpResendLostPackets;
bool udpAssumeSequential;

static unsigned lastResentReport = 0;
static unsigned lastPacketsResent = 0;
static unsigned lastFlowRequestsSent = 0;
static unsigned lastFlowPermitsReceived = 0;
static unsigned lastDataPacketsSent = 0;

class UdpResendList
{
private:
    DataBuffer *entries[TRACKER_BITS] = { nullptr };
    unsigned timeSent[TRACKER_BITS] = { 0 };
    sequence_t first = 0;
    unsigned count = 0;  // number of non-null entries
public:
    void append(DataBuffer *buf)
    {
        UdpPacketHeader *header = (UdpPacketHeader*) buf->data;
        sequence_t seq = header->sendSeq;
        header->pktSeq |= UDP_PACKET_RESENT;
        if (!count)
        {
            first = seq;
        }
        else if (seq - first >= TRACKER_BITS)
        {
            // This shouldn't happen if we have steps in place to block ending new until we are sure old have been delivered.
            throwUnexpected();
        }
        unsigned idx = seq % TRACKER_BITS;
        assert(entries[idx] == nullptr);
        entries[idx] = buf;
        timeSent[idx] = msTick();
        count++;
    }

    // This function does two things:
    // 1. Updates the circular buffer to release any packets that are confirmed delivered
    // 2. Appends any packets that need resending to the toSend list

    void noteRead(const PacketTracker &seen, std::vector<DataBuffer *> &toSend, unsigned space, unsigned nextSendSequence)
    {
        if (!count)
            return;
        unsigned now = msTick();
        sequence_t seq = first;
        unsigned checked = 0;
        bool released = false;
        while (checked < count && space)
        {
            unsigned idx = seq % TRACKER_BITS;
            if (entries[idx])
            {
                UdpPacketHeader *header = (UdpPacketHeader*) entries[idx]->data;
                assert(seq == header->sendSeq);
                if (seen.hasSeen(header->sendSeq))
                {
                    ::Release(entries[idx]);
                    entries[idx] = nullptr;
                    count--;
                    released = true;
                }
                else
                {
                    // The current table entry is not marked as seen by receiver. Should we resend it?
                    if (now-timeSent[idx] >= udpResendTimeout ||    // Note that this will block us from sending newer packets, if we have reached limit of tracking.
                        (udpAssumeSequential && (int)(seq - seen.lastSeen()) < 0))  // so we (optionally) assume any packet not received that is EARLIER than one that HAS been received is lost.
                    {
                        if (udpTraceLevel > 1)
                            DBGLOG("Resending %" SEQF "u last sent %u ms ago", seq, now-timeSent[idx]);
                        timeSent[idx] = now;
                        packetsResent++;
                        toSend.push_back(entries[idx]);
                        space--;
                    }
                    checked++;
                }
            }
            seq++;
        }
        if (released && count)
        {
            while (entries[first % TRACKER_BITS] == nullptr)
                first++;
        }
    }
    unsigned firstTracked() const
    {
        assert(count);                    // Meaningless to call this if count is 0
        return first;
    }
    unsigned numActive() const
    {
        return count;
    }
    bool canRecord(unsigned seq) const
    {
        return (count==0 || seq - first < TRACKER_BITS);
    }

};

class UdpReceiverEntry : public IUdpReceiverEntry
{
    UdpReceiverEntry() = delete;
    UdpReceiverEntry ( const UdpReceiverEntry & ) = delete;
private:
    queue_t *output_queue = nullptr;
    bool    initialized = false;
    const bool isLocal = false;
    const bool encrypted = false;
    ISocket *send_flow_socket = nullptr;
    ISocket *data_socket = nullptr;
    const unsigned numQueues;
    int     current_q = 0;
    int     currentQNumPkts = 0;         // Current Queue Number of Consecutive Processed Packets.
    int     *maxPktsPerQ = nullptr;      // to minimise power function re-calc for every packet

    void sendRequest(UdpRequestToSendMsg &msg)
    {
        try
        {
            if (udpTraceLevel > 3 || udpTraceFlow)
            {
                StringBuffer s;
                DBGLOG("UdpSender: sending flowType::%s msg %" SEQF "u flowSeq %" SEQF "u to node=%s", flowType::name(msg.cmd), msg.sendSeq, msg.flowSeq, ip.getIpText(s).str());
            }
            send_flow_socket->write(&msg, sizeof(UdpRequestToSendMsg));
            flowRequestsSent++;
        }
        catch(IException *e)
        {
            StringBuffer s;
            DBGLOG("UdpSender: sendRequest write failed - %s", e->errorMessage(s).str());
            e->Release();
        }
        catch (...)
        {
            DBGLOG("UdpSender: sendRequest write failed - unknown error");
        }
    }

    const IpAddress sourceIP;
    UdpResendList *resendList = nullptr;
public:
    const IpAddress ip;
    unsigned timeouts = 0;      // Number of consecutive timeouts
    std::atomic<unsigned> requestExpiryTime = { 0 };  // Updated by send_flow thread, read by send_resend thread and send_data thread

    static bool comparePacket(const void *pkData, const void *key)
    {
        UdpPacketHeader *dataHdr = (UdpPacketHeader*) ((DataBuffer*)pkData)->data;
        UdpPacketHeader *keyHdr = (UdpPacketHeader*) key;
        return ( (dataHdr->ruid == keyHdr->ruid) && (dataHdr->msgId == keyHdr->msgId) );
    }

    std::atomic<unsigned> packetsQueued = { 0 };
    std::atomic<sequence_t> nextSendSequence = {0};
    std::atomic<sequence_t> activeFlowSequence = {0};
    CriticalSection activeCrit;

    void sendDone(unsigned packets)
    {
        //This function has a potential race condition with requestToSendNew:
        //packetsQueued must be checked within the critical section to ensure that requestToSend hasn't been called
        //between retrieving the count and entering the critical section, otherwise this function will set
        //requestExpiryTime to 0 (and indicate the operation is done)even though there packetsQueued is non-zero.
        CriticalBlock b(activeCrit);
        bool dataRemaining;
        if (resendList)
            dataRemaining = (packetsQueued.load(std::memory_order_relaxed) && resendList->canRecord(nextSendSequence)) || resendList->numActive();
        else
            dataRemaining = packetsQueued.load(std::memory_order_relaxed);
        // If dataRemaining says 0, but someone adds a row in this window, the request_to_send will be sent BEFORE the send_completed
        // So long as receiver handles that, are we good?
        UdpRequestToSendMsg msg;
        msg.packets = packets;                      // Note this is how many we sent
        msg.sendSeq = nextSendSequence;
        msg.sourceNode = sourceIP;
        if (dataRemaining && requestExpiryTime)  // requestExpiryTime will be non-zero UNLESS someone called abort() just before I got here
        {
            msg.flowSeq = activeFlowSequence++;
            msg.cmd = flowType::request_to_send_more;
            requestExpiryTime = msTick() + udpRequestToSendAckTimeout;
        }
        else
        {
            msg.flowSeq = activeFlowSequence;
            msg.cmd = flowType::send_completed;
            requestExpiryTime = 0;
        }
        sendRequest(msg);
        timeouts = 0;
    }

    void requestToSendNew()
    {
        //See comment in sendDone() on a potential race condition.
        CriticalBlock b(activeCrit);
        // This is called from data thread when new data added to a previously-empty list
        if (!requestExpiryTime)
        {
            // If there's already an active request - no need to create a new one
            UdpRequestToSendMsg msg;
            msg.cmd = flowType::request_to_send;
            msg.packets = 0;
            msg.sendSeq = nextSendSequence;
            msg.flowSeq = ++activeFlowSequence;
            msg.sourceNode = sourceIP;
            requestExpiryTime = msTick() + udpRequestToSendAckTimeout;
            sendRequest(msg);
        }
    }

    void resendRequestToSend()
    {
        // This is called from timeout thread when a previously-send request has had no response
        timeouts++;
        if (udpTraceLevel || udpTraceFlow || udpTraceTimeouts)
        {
            StringBuffer s;
            EXCLOG(MCoperatorError,"ERROR: UdpSender: timed out %i times (max=%i) waiting ok_to_send msg from node=%s",
                   timeouts, udpMaxRetryTimedoutReqs, ip.getIpText(s).str());
        }
        // 0 (zero) value of udpMaxRetryTimedoutReqs means NO limit on retries
        CriticalBlock b(activeCrit);
        if (udpMaxRetryTimedoutReqs && (timeouts >= udpMaxRetryTimedoutReqs))
        {
            abort();
            return;
        }
        if (requestExpiryTime)
        {
            UdpRequestToSendMsg msg;
            msg.cmd = flowType::request_to_send;
            msg.packets = 0;
            msg.sendSeq = nextSendSequence;
            msg.flowSeq = activeFlowSequence;
            msg.sourceNode = sourceIP;
            requestExpiryTime = msTick() + udpRequestToSendAckTimeout;
            sendRequest(msg);
        }
    }

    void requestAcknowledged()
    {
        CriticalBlock b(activeCrit);
        if (requestExpiryTime)
            requestExpiryTime = msTick() + udpRequestToSendTimeout;
    }

    unsigned sendData(const UdpPermitToSendMsg &permit, TokenBucket *bucket)
    {
#ifdef _DEBUG
        // Consistency check
        if (permit.destNode.getIpAddress().ipcompare(ip) != 0)
        {
            StringBuffer p, s;
            DBGLOG("UdpFlow: permit ip %s does not match receiver table ip %s", permit.destNode.getTraceText(p).str(), ip.getIpText(s).str());
            printStackReport();
        }
#endif
        if (permit.flowSeq != activeFlowSequence)
        {
            if (udpTraceLevel>1 || udpTraceFlow)
            {
                StringBuffer s;
                DBGLOG("UdpFlow: ignoring out-of-date permit_to_send seq %" SEQF "u (expected %" SEQF "u) to node %s", permit.flowSeq, activeFlowSequence+0, permit.destNode.getTraceText(s).str());
            }
            return 0;
        }
        unsigned maxPackets = permit.max_data;
        std::vector<DataBuffer *> toSend;
        unsigned totalSent = 0;
        unsigned resending = 0;
        if (resendList)
        {
            resendList->noteRead(permit.seen, toSend, maxPackets, nextSendSequence.load(std::memory_order_relaxed));
            resending = toSend.size();
            maxPackets -= resending;
            // Don't send any packet that would end up overwriting an active packet in our resend list
            if (resendList->numActive())
            {
                unsigned inflight = nextSendSequence - resendList->firstTracked();
                assert(inflight <= TRACKER_BITS);
                if (maxPackets > TRACKER_BITS-inflight)
                {
                    maxPackets = TRACKER_BITS-inflight;
                    if (udpTraceLevel>2 || maxPackets == 0)
                        DBGLOG("Can't send more than %d new packets or we will overwrite unreceived packets (%u in flight, %u active %u resending now)", maxPackets, inflight, resendList->numActive(), resending);
                    // Note that this may mean we can't send any packets, despite having asked for permission to do so
                    // We will keep on asking.
                }
            }
        }
        if (udpTraceLevel>2)
            DBGLOG("Resending %u packets", (unsigned) toSend.size());
        while (maxPackets && packetsQueued.load(std::memory_order_relaxed))
        {
            DataBuffer *buffer = popQueuedData();
            if (!buffer)
                break;  // Suggests data was aborted before we got to pop it
            UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
            header->sendSeq = nextSendSequence++;
            toSend.push_back(buffer);
            maxPackets--;
            totalSent += header->length;
#if defined(__linux__) || defined(__APPLE__)
            if (isLocal && (totalSent> 100000))  // Avoids sending too fast to local node, for reasons lost in the mists of time
                break;
#endif
        }
        MemoryBuffer encryptBuffer;
        for (DataBuffer *buffer: toSend)
        {
            UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
            unsigned length = header->length;
            if (bucket)
            {
                MTIME_SECTION(queryActiveTimer(), "bucket_wait");
                bucket->wait((length / 1024)+1);
            }
            try
            {
#ifdef TEST_DROPPED_PACKETS
                if (((header->pktSeq & UDP_PACKET_RESENT)==0) && (header->pktSeq==0 || header->pktSeq==10 || ((header->pktSeq&UDP_PACKET_COMPLETE) != 0)))
                    DBGLOG("Deliberately dropping packet %" SEQF "u", header->sendSeq);
                else
#endif
                if (encrypted)
                {
                    encryptBuffer.clear();
                    encryptBuffer.append(sizeof(UdpPacketHeader), header);    // We don't encrypt the header
                    length -= sizeof(UdpPacketHeader);
                    const char *data = buffer->data + sizeof(UdpPacketHeader);
                    const MemoryAttr &udpkey = getSecretUdpKey(true);
                    aesEncrypt(udpkey.get(), udpkey.length(), data, length, encryptBuffer);
                    header->length = encryptBuffer.length();
                    encryptBuffer.writeDirect(0, sizeof(UdpPacketHeader), header);   // Only really need length updating
                    assert(length <= DATA_PAYLOAD);
                    if (udpTraceLevel > 5)
                        DBGLOG("ENCRYPT: Writing %u bytes to data socket", encryptBuffer.length());
                    data_socket->write(encryptBuffer.toByteArray(), encryptBuffer.length());
                }
                else
                    data_socket->write(buffer->data, length);
                dataPacketsSent++;
            }
            catch(IException *e)
            {
                StringBuffer s;
                DBGLOG("UdpSender: write exception - write(%p, %u) - %s", buffer->data, length, e->errorMessage(s).str());
                e->Release();
            } 
            catch(...)
            {
                DBGLOG("UdpSender: write exception - unknown exception");
            }
            if (resendList)
            {
                if (resending)
                    resending--;   //Don't add the ones I am resending back onto list - they are still there!
                else
                    resendList->append(buffer);
            }
            else
                ::Release(buffer);
        }
        sendDone(toSend.size());
        return totalSent;
    }

    bool dataQueued(const UdpPacketHeader &key)
    {
        // Used when a retry packet is received, to determine whether the query is in fact completed
        // but just stuck in transit queues
        if (packetsQueued.load(std::memory_order_relaxed))
        {
            for (unsigned i = 0; i < numQueues; i++)
            {
                if (output_queue[i].dataQueued(&key, &comparePacket))
                    return true;
            }
        }
        return false;
    }

    bool removeData(void *key, PKT_CMP_FUN pkCmpFn) 
    {
        // Used after receiving an abort, to avoid sending data that is no longer required
        // Note that we don't attempt to remove packets that have already been sent from the resend list
        unsigned removed = 0;
        if (packetsQueued.load(std::memory_order_relaxed))
        {
            for (unsigned i = 0; i < numQueues; i++)
            {
                removed += output_queue[i].removeData(key, pkCmpFn);
            }
            packetsQueued -= removed;
        }
        return removed > 0;
    }

    void abort()
    {
        // Called if too many timeouts on a request to send

        if (udpTraceLevel > 3)
        {
            StringBuffer s;
            DBGLOG("UdpSender: abort sending queued data to node=%s", ip.getIpText(s).str());
        }
        timeouts = 0;
        requestExpiryTime = 0;
        removeData(nullptr, nullptr);
    }

    inline void pushData(unsigned queue, DataBuffer *buffer)
    {
        output_queue[queue].free_slots();     // block until at least one free space
        output_queue[queue].pushOwn(buffer);
        if (!packetsQueued++)
            requestToSendNew();
    }

    DataBuffer *popQueuedData() 
    {
        DataBuffer *buffer;
        for (unsigned i = 0; i < numQueues; i++)
        {
            if (udpOutQsPriority)
            {
                buffer = output_queue[current_q].pop(false);
                if (!buffer)
                {
                    if (udpTraceLevel >= 5)
                        DBGLOG("UdpSender: ---------- Empty Q %d", current_q);
                    currentQNumPkts = 0;
                    current_q = (current_q + 1) % numQueues;
                }
                else
                {
                    currentQNumPkts++;
                    if (udpTraceLevel >= 5)
                        DBGLOG("UdpSender: ---------- Packet from Q %d", current_q);
                    if (currentQNumPkts >= maxPktsPerQ[current_q])
                    {
                        currentQNumPkts = 0;
                        current_q = (current_q + 1) % numQueues;
                    }
                    packetsQueued--;
                    return buffer;
                }
            }
            else
            {
                current_q = (current_q + 1) % numQueues;
                buffer = output_queue[current_q].pop(false);
                if (buffer)
                {
                    packetsQueued--;
                    return buffer;
                }
            }
        }
        // If we get here, it suggests we were told to get a buffer but no queue has one.
        // This should be rare but possible if data gets removed following an abort, as
        // there is a window in abort() between the remove and the decrement of packetsQueued.
        return nullptr;
    }

    UdpReceiverEntry(const IpAddress _ip, unsigned _numQueues, unsigned _queueSize, unsigned _sendFlowPort, unsigned _dataPort, bool _encrypted)
    : ip (_ip), sourceIP(myNode.getIpAddress()), numQueues(_numQueues), isLocal(_ip.isLocal()), encrypted(_encrypted)
    {
        assert(!initialized);
        assert(numQueues > 0);
        if (!ip.isNull())
        {
            try 
            {
                SocketEndpoint sendFlowEp(_sendFlowPort, ip);
                SocketEndpoint dataEp(_dataPort, ip);
                send_flow_socket = ISocket::udp_connect(sendFlowEp);
                data_socket = ISocket::udp_connect(dataEp);
                if (isLocal)
                {
                    data_socket->set_send_buffer_size(udpLocalWriteSocketSize);
                    if (udpTraceLevel > 0)
                        DBGLOG("UdpSender: sendbuffer set for local socket (size=%d)", udpLocalWriteSocketSize);
                }
            }
            catch(IException *e) 
            {
                StringBuffer error, ipstr;
                DBGLOG("UdpSender: udp_connect failed %s %s", ip.getIpText(ipstr).str(), e->errorMessage(error).str());
                throw;
            }
            catch(...) 
            {
                StringBuffer ipstr;
                DBGLOG("UdpSender: udp_connect failed %s %s", ip.getIpText(ipstr).str(), "Unknown error");
                throw;
            }
            output_queue = new queue_t[numQueues];
            maxPktsPerQ = new int[numQueues];
            for (unsigned j = 0; j < numQueues; j++) 
            {
                output_queue[j].set_queue_size(_queueSize);
                maxPktsPerQ[j] = (int) pow((double)udpOutQsPriority, (double)numQueues - j - 1);
            }
            initialized = true;
            if (udpTraceLevel > 0)
            {
                StringBuffer ipStr;
                DBGLOG("UdpSender: added entry for ip=%s to receivers table - send_flow_port=%d", ip.getIpText(ipStr).str(), _sendFlowPort);
            }
        }
        if (udpResendLostPackets)
        {
            DBGLOG("UdpSender: created resend list with %u entries", TRACKER_BITS);
            resendList = new UdpResendList;
        }
        else
            DBGLOG("UdpSender: resend list disabled");
    }

    ~UdpReceiverEntry()
    {
        if (send_flow_socket) send_flow_socket->Release();
        if (data_socket) data_socket->Release();
        if (output_queue) delete [] output_queue;
        if (maxPktsPerQ) delete [] maxPktsPerQ;
        delete resendList;
    }

};

class CSendManager : implements ISendManager, public CInterface
{
    class StartedThread : public Thread
    {
    private:
        Semaphore started;
        virtual int run()
        {
            started.signal();
            return doRun();
        }
    protected:
        bool running;
    public:
        StartedThread(const char *name) : Thread(name)
        {
            running = false;
        }

        ~StartedThread()
        {
            running = false;
            join();
        }

        virtual void start()
        {
            running = true;
            Thread::start();
            started.wait();
        }

        virtual int doRun() = 0;
    };

    class send_resend_flow : public StartedThread
    {
        // Check if any senders have timed out
        CSendManager &parent;
        Semaphore terminated;

        virtual int doRun() override
        {
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_resend_flow started");
            unsigned timeout = udpRequestToSendTimeout;
            while (running)
            {
                if (terminated.wait(timeout) || !running)
                    break;

                unsigned now = msTick();
                timeout = udpRequestToSendTimeout;
                for (auto&& dest: parent.receiversTable)
                {
#ifdef _DEBUG
                    // Consistency check
                    UdpReceiverEntry &receiverInfo = parent.receiversTable[dest.ip];
                    if (&receiverInfo != &dest)
                    {
                        StringBuffer s;
                        DBGLOG("UdpSender: table entry %s does not find itself", dest.ip.getIpText(s).str());
                        printStackReport();

                    }
#endif
                    unsigned expireTime = dest.requestExpiryTime;
                    if (expireTime)
                    {
                        int timeToGo = expireTime-now;
                        if (timeToGo <= 0)
                            dest.resendRequestToSend();
                        else if ((unsigned) timeToGo < timeout)
                            timeout = timeToGo;
                    }
                }
                if (udpStatsReportInterval && (now-lastResentReport > udpStatsReportInterval))
                {
                    // MORE - some of these should really be tracked per destination
                    lastResentReport = now;
                    if (packetsResent > lastPacketsResent)
                    {
                        DBGLOG("%u more packets resent by this agent (%u total)", packetsResent-lastPacketsResent, packetsResent-0);
                        lastPacketsResent = packetsResent;
                    }
                    if (flowRequestsSent > lastFlowRequestsSent)
                    {
                        DBGLOG("%u more flow request packets sent by this agent (%u total)", flowRequestsSent - lastFlowRequestsSent, flowRequestsSent-0);
                        lastFlowRequestsSent = flowRequestsSent;
                    }
                    if (flowPermitsReceived > lastFlowPermitsReceived)
                    {
                        DBGLOG("%u more flow control packets recived by this agent (%u total)", flowPermitsReceived - lastFlowPermitsReceived, flowPermitsReceived-0);
                        lastFlowPermitsReceived = flowPermitsReceived;
                    }
                    if (dataPacketsSent > lastDataPacketsSent)
                    {
                        DBGLOG("%u more data packets sent by this agent (%u total)", dataPacketsSent - lastDataPacketsSent, dataPacketsSent-0);
                        lastDataPacketsSent = dataPacketsSent;
                    }
                }
            }
            return 0;
        }

    public:
        send_resend_flow(CSendManager &_parent)
            : StartedThread("UdpLib::send_resend_flow"), parent(_parent)
        {
            start();
        }

        ~send_resend_flow()
        {
            running = false;
            terminated.signal();
            join();
        }

    };

    class send_receive_flow : public StartedThread 
    {
        CSendManager &parent;
        int      receive_port;
        Owned<ISocket> flow_socket;
        
    public:
        send_receive_flow(CSendManager &_parent, int r_port) : StartedThread("UdpLib::send_receive_flow"), parent(_parent)
        {
            receive_port = r_port;
            if (check_max_socket_read_buffer(udpFlowSocketsSize) < 0) 
                throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
            flow_socket.setown(ISocket::udp_create(receive_port));
            flow_socket->set_receive_buffer_size(udpFlowSocketsSize);
            size32_t actualSize = flow_socket->get_receive_buffer_size();
            DBGLOG("UdpSender: rcv_flow_socket created port=%d sockbuffsize=%d actualsize=%d", receive_port, udpFlowSocketsSize, actualSize);
            start();
        }
        
        ~send_receive_flow() 
        {
            running = false;
            if (flow_socket) 
                flow_socket->close();
            join();
        }
        
        virtual int doRun() 
        {
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_receive_flow started");
#ifdef __linux__
            setLinuxThreadPriority(2);
#endif
            while(running) 
            {
                UdpPermitToSendMsg f = { flowType::ok_to_send, 0, { } };
                unsigned readsize = udpResendLostPackets ? sizeof(UdpPermitToSendMsg) : offsetof(UdpPermitToSendMsg, seen);
                while (running) 
                {
                    try 
                    {
                        unsigned int res;
                        flow_socket->read(&f, readsize, readsize, res, 5);
                        flowPermitsReceived++;
                        assert(res==readsize);
                        switch (f.cmd)
                        {
                        case flowType::ok_to_send:
                            if (udpTraceLevel > 2 || udpTraceFlow)
                            {
                                StringBuffer s;
                                DBGLOG("UdpSender: received ok_to_send msg max %d packets from node=%s seq %" SEQF "u", f.max_data, f.destNode.getTraceText(s).str(), f.flowSeq);
                            }
                            parent.data->ok_to_send(f);
                            break;

                        case flowType::request_received:
                            if (udpTraceLevel > 2 || udpTraceFlow)
                            {
                                StringBuffer s;
                                DBGLOG("UdpSender: received request_received msg from node=%s seq %" SEQF "u", f.destNode.getTraceText(s).str(), f.flowSeq);
                            }
                            parent.receiversTable[f.destNode].requestAcknowledged();
                            break;

                        default: 
                            DBGLOG("UdpSender: received unknown flow message type=%d", f.cmd);
                        }
                    }
                    catch (IException *e) 
                    {
                        if (running && e->errorCode() != JSOCKERR_timeout_expired)
                        {
                            StringBuffer s;
                            DBGLOG("UdpSender: send_receive_flow::read failed port=%i %s", receive_port, e->errorMessage(s).str());
                        }
                        e->Release();
                    }
                    catch (...) 
                    {
                        if (running)   
                            DBGLOG("UdpSender: send_receive_flow::unknown exception");
                        MilliSleep(0);
                    }
                }
            }
            return 0;
        }
    };

    class send_data : public StartedThread 
    {
        CSendManager &parent;
        simple_queue<UdpPermitToSendMsg> send_queue;
        Linked<TokenBucket> bucket;

    public:
        send_data(CSendManager &_parent, TokenBucket *_bucket)
            : StartedThread("UdpLib::send_data"), parent(_parent), bucket(_bucket), send_queue(100) // MORE - send q size should be configurable and/or related to size of cluster?
        {
            if (check_max_socket_write_buffer(udpLocalWriteSocketSize) < 0) 
                throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max write buffer is less than %i", udpLocalWriteSocketSize);
            start();
        }
        
        ~send_data()
        {
            running = false;
            UdpPermitToSendMsg dummy = {};
            send_queue.push(dummy);
            join();
        }   

        bool ok_to_send(const UdpPermitToSendMsg &msg) 
        {
            if (send_queue.push(msg, 15)) 
                return true;
            else 
            {
                StringBuffer s;
                DBGLOG("UdpSender: push() failed - ignored ok_to_send msg - node=%s, maxData=%u", msg.destNode.getTraceText(s).str(), msg.max_data);
                return false;
            }
        }

        virtual int doRun() 
        {
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_data started");
        #ifdef __linux__
            setLinuxThreadPriority(1); // MORE - windows? Is this even a good idea? Must not send faster than receiver can pull off the socket
        #endif
            UdpPermitToSendMsg permit;
            while (running) 
            {
                send_queue.pop(permit);
                if (!running)
                    return 0;

                UdpReceiverEntry &receiverInfo = parent.receiversTable[permit.destNode];
                unsigned payload = receiverInfo.sendData(permit, bucket);
                
                if (udpTraceLevel > 2)
                {
                    StringBuffer s;
                    DBGLOG("UdpSender: sent %u bytes to node=%s under permit %" SEQF "u", payload, permit.destNode.getTraceText(s).str(), permit.flowSeq);
                }
            }
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_data stopped");
            return 0;
        }
    };

    friend class send_resend_flow;
    friend class send_receive_flow;
    friend class send_data;

    unsigned numQueues;

    IpAddress myIP;
    IpMapOf<UdpReceiverEntry> receiversTable;
    send_resend_flow  *resend_flow;
    send_receive_flow *receive_flow;
    send_data         *data;
    Linked<TokenBucket> bucket;
    bool encrypted;
    
    std::atomic<unsigned> msgSeq{0};

    inline unsigned getNextMessageSequence()
    {
        unsigned res;
        do
        {
            res = ++msgSeq;
        } while (unlikely(!res));
        return res;
    }
        
public:
    IMPLEMENT_IINTERFACE;

    CSendManager(int server_flow_port, int data_port, int client_flow_port, int q_size, int _numQueues, const IpAddress &_myIP, TokenBucket *_bucket, bool _encrypted)
        : bucket(_bucket),
          myIP(_myIP),
          receiversTable([_numQueues, q_size, server_flow_port, data_port, _encrypted](const ServerIdentifier ip) { return new UdpReceiverEntry(ip.getIpAddress(), _numQueues, q_size, server_flow_port, data_port, _encrypted);}),
          encrypted(_encrypted)
    {
#ifndef _WIN32
        setpriority(PRIO_PROCESS, 0, -3);
#endif
        numQueues = _numQueues;
        data = new send_data(*this, bucket);
        resend_flow = new send_resend_flow(*this);
        receive_flow = new send_receive_flow(*this, client_flow_port);
    }


    ~CSendManager() 
    {
        delete resend_flow;
        delete receive_flow;
        delete data;
    }

    // Interface ISendManager

    virtual void writeOwn(IUdpReceiverEntry &receiver, DataBuffer *buffer, unsigned len, unsigned queue) override
    {
        // NOTE: takes ownership of the DataBuffer
        assert(queue < numQueues);
        assert(buffer);
        static_cast<UdpReceiverEntry &>(receiver).pushData(queue, buffer);
    }

    virtual IMessagePacker *createMessagePacker(ruid_t ruid, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue) override
    {
        return ::createMessagePacker(ruid, sequence, messageHeader, headerSize, *this, receiversTable[destNode], myIP, getNextMessageSequence(), queue, encrypted);
    }

    virtual bool dataQueued(ruid_t ruid, unsigned msgId, const ServerIdentifier &destNode) override
    {
        UdpPacketHeader pkHdr;
        pkHdr.ruid = ruid;
        pkHdr.msgId = msgId;
        return receiversTable[destNode].dataQueued(pkHdr);
    }

    virtual bool abortData(ruid_t ruid, unsigned msgId, const ServerIdentifier &destNode)
    {
        UdpPacketHeader pkHdr;
        pkHdr.ruid = ruid;
        pkHdr.msgId = msgId;
        return receiversTable[destNode].removeData((void*) &pkHdr, &UdpReceiverEntry::comparePacket);
    }

    virtual bool allDone() 
    {
        // Used for some timing tests only
        for (auto&& dest: receiversTable)
        {
            if (dest.packetsQueued.load(std::memory_order_relaxed))
                return false;
        }
        return true;
    }

};

ISendManager *createSendManager(int server_flow_port, int data_port, int client_flow_port, int queue_size_pr_server, int queues_pr_server, TokenBucket *rateLimiter, bool encryptionInTransit)
{
    assertex(!myNode.getIpAddress().isNull());
    return new CSendManager(server_flow_port, data_port, client_flow_port, queue_size_pr_server, queues_pr_server, myNode.getIpAddress(), rateLimiter, encryptionInTransit);
}

class CMessagePacker : implements IMessagePacker, public CInterface
{
    ISendManager   &parent;
    IUdpReceiverEntry &receiver;
    UdpPacketHeader package_header;
    DataBuffer     *part_buffer;
    const unsigned data_buffer_size;
    unsigned data_used;
    void *mem_buffer;
    unsigned mem_buffer_size;
    unsigned totalSize;
    bool            packed_request;
    MemoryBuffer    metaInfo;
    bool            last_message_done;
    int             queue_number;

public:
    IMPLEMENT_IINTERFACE;

    CMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue, bool _encrypted)
        : parent(_parent), receiver(_receiver), data_buffer_size(DATA_PAYLOAD - sizeof(UdpPacketHeader) - (_encrypted ? 16 : 0))

    {
        queue_number = _queue;

        package_header.length = 0;          // filled in with proper value later
        package_header.metalength = 0;
        package_header.ruid = ruid;
        package_header.msgId = msgId;
        package_header.pktSeq = 0;
        package_header.node.setIp(_sourceNode);
        package_header.msgSeq = _msgSeq;

        packed_request = false;
        part_buffer = bufferManager->allocate();
        assertex(data_buffer_size >= headerSize + sizeof(unsigned short));
        *(unsigned short *) (&part_buffer->data[sizeof(UdpPacketHeader)]) = headerSize;
        memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+sizeof(unsigned short)], messageHeader, headerSize);
        data_used = headerSize + sizeof(unsigned short);
        mem_buffer = 0;
        mem_buffer_size = 0;
        last_message_done = false;
        totalSize = 0;
    }

    ~CMessagePacker()
    {
        if (part_buffer)
            part_buffer->Release();
        if (mem_buffer) free (mem_buffer);
    }

    virtual void *getBuffer(unsigned len, bool variable) override
    {
        if (variable)
            len += sizeof(RecordLengthType);
        if (data_buffer_size < len)
        {
            // Won't fit in one, so allocate temp location
            if (mem_buffer_size < len)
            {
                free(mem_buffer);
                mem_buffer = checked_malloc(len, ROXIE_MEMORY_ERROR);
                mem_buffer_size = len;
            }
            packed_request = false;
            if (variable)
                return ((char *) mem_buffer) + sizeof(RecordLengthType);
            else
                return mem_buffer;
        }

        if (part_buffer && ((data_buffer_size - data_used) < len))
            flush(false); // Note that we never span records that are small enough to fit - this can result in significant wastage if record just over DATA_PAYLOAD/2

        if (!part_buffer)
        {
            part_buffer = bufferManager->allocate();
        }
        packed_request = true;
        if (variable)
            return &part_buffer->data[data_used + sizeof(UdpPacketHeader) + sizeof(RecordLengthType)];
        else
            return &part_buffer->data[data_used + sizeof(UdpPacketHeader)];
    }

    virtual void putBuffer(const void *buf, unsigned len, bool variable) override
    {
        if (variable)
        {
            assertex(len < MAX_RECORD_LENGTH);
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        totalSize += len;
        if (packed_request)
        {
            assert(len <= (data_buffer_size - data_used));
            data_used += len;
        }
        else
        {
            while (len)
            {
                if (!part_buffer)
                {
                    part_buffer = bufferManager->allocate();
                    data_used = 0;
                }
                unsigned chunkLen = data_buffer_size - data_used;
                if (chunkLen > len)
                    chunkLen = len;
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], buf, chunkLen);
                data_used += chunkLen;
                len -= chunkLen;
                buf = &(((char*)buf)[chunkLen]);
                if (len)
                    flush(false);
            }
        }
    }

    virtual void sendMetaInfo(const void *buf, unsigned len) override {
        metaInfo.append(len, buf);
    }

    virtual void flush() override { flush(true); }

    virtual unsigned size() const override
    {
        return totalSize;
    }
private:

    void flush(bool last_msg)
    {
        if (!last_message_done && last_msg)
        {
            last_message_done = true;
            if (!part_buffer)
                part_buffer = bufferManager->allocate();
            const char *metaData = metaInfo.toByteArray();
            unsigned metaLength = metaInfo.length();
            unsigned maxMetaLength = data_buffer_size - data_used;
            while (metaLength > maxMetaLength)
            {
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, maxMetaLength);
                put_package(part_buffer, data_used, maxMetaLength);
                metaLength -= maxMetaLength;
                metaData += maxMetaLength;
                data_used = 0;
                maxMetaLength = data_buffer_size;
                part_buffer = bufferManager->allocate();
            }
            if (metaLength)
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, metaLength);
            package_header.pktSeq |= UDP_PACKET_COMPLETE;
            put_package(part_buffer, data_used, metaLength);
        }
        else if (part_buffer)
        {
            // Just flush current - used when no room for current row
            if (data_used)
                put_package(part_buffer, data_used, 0); // buffer released in put_package
            else
                part_buffer->Release(); // If NO data in buffer, release buffer back to pool
        }
        part_buffer = 0;
        data_used = 0;
    }

    void put_package(DataBuffer *dataBuff, unsigned datalength, unsigned metalength)
    {
        package_header.length = datalength + metalength + sizeof(UdpPacketHeader);
        package_header.metalength = metalength;
        memcpy(dataBuff->data, &package_header, sizeof(package_header));
        parent.writeOwn(receiver, dataBuff, package_header.length, queue_number);
        package_header.pktSeq++;
    }


};


extern UDPLIB_API IMessagePacker *createMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue, bool _encrypted)
{
    return new CMessagePacker(ruid, msgId, messageHeader, headerSize, _parent, _receiver, _sourceNode, _msgSeq, _queue, _encrypted);
}
