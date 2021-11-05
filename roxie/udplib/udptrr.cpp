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

#include <string>
#include <map>
#include <queue>

#include "jthread.hpp"
#include "jlog.hpp"
#include "jisem.hpp"
#include "jsocket.hpp"
#include "udplib.hpp"
#include "udptrr.hpp"
#include "udptrs.hpp"
#include "udpipmap.hpp"
#include "udpmsgpk.hpp"
#include "roxiemem.hpp"
#include "roxie.hpp"

#ifdef _WIN32
#include <io.h>
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include <thread>

using roxiemem::DataBuffer;
using roxiemem::IRowManager;

unsigned udpMaxPendingPermits = 1;

RelaxedAtomic<unsigned> flowPermitsSent = {0};
RelaxedAtomic<unsigned> flowRequestsReceived = {0};
RelaxedAtomic<unsigned> dataPacketsReceived = {0};
static unsigned lastFlowPermitsSent = 0;
static unsigned lastFlowRequestsReceived = 0;
static unsigned lastDataPacketsReceived = 0;

// The code that redirects flow messages from data socket to flow socket relies on the assumption tested here
static_assert(sizeof(UdpRequestToSendMsg) < sizeof(UdpPacketHeader), "Expected UDP rts size to be less than packet header");

class CReceiveManager : implements IReceiveManager, public CInterface
{
    /*
     * The ReceiveManager has several threads:
     * 1. receive_receive_flow (priority 3)
     *     - waits for packets on flow port
     *     - maintains list of nodes that have pending requests
     *     - sends ok_to_send to one sender (or more) at a time
     * 2. receive_data (priority 4)
     *     - reads data packets off data socket
     *     - runs at v. high priority
     *     - used to have an option to perform collation on this thread but a bad idea:
     *        - can block (ends up in memory manager via attachDataBuffer).
     *        - Does not apply back pressure
     *     - Just enqueues them. We don't give permission to send more than the queue can hold, but it's a soft limit
     * 3. PacketCollator (standard priority)
     *     - dequeues packets
     *     - collates packets
     *
     */

    /*
     * Handling lost packets
     *
     * We try to make lost packets unlikely by telling agents when to send (and making sure they don't send unless
     * there's a good chance that socket buffer will have room). But we can't legislate for network issues.
     *
     * What packets can be lost?
     * 1. Data packets - handled via sliding window of resendable packets (or by retrying whole query after a timeout, of resend logic disabled)
     * 2. RequestToSend - the sender's resend thread checks periodically. There's a short initial timeout for getting a reply (either "request_received"
     *    or "okToSend"), then a longer timeout for actually sending.
     * 3. OkToSend - there is a timeout after which the permission is considered invalid (based on how long it SHOULD take to send them).
     *    The requestToSend retry mechanism would then make sure retried.
     *    MORE - if I don't get a response from OkToSend I should assume lost and requeue it.
     * 4. complete - covered by same timeout as okToSend. A lost complete will mean incoming data to that node stalls for the duration of this timeout,
     *
     */
    class UdpSenderEntry  // one per node in the system
    {
        // This is created the first time a message from a previously unseen IP arrives, and remains alive indefinitely
        // Note that the various members are accessed by different threads, but no member is accessed from more than one thread
        // (except where noted) so protection is not required

        // Note that UDP ordering rules mean we can't guarantee that we don't see a "request_to_send" for the next transfer before
        // we see the "complete" for the current one. Even if we were sure network stack would not reorder, these come from different
        // threads on the sender side and the order is not 100% guaranteed, so we need to cope with it.

        // We also need to recover gracefully (and preferably quickly) if any flow or data messages go missing. Currently the sender
        // will resend the rts if no ok_to_send within timeout, but there may be a better way?

    public:
        // Used only by receive_flow thread
        IpAddress dest;
        ISocket *flowSocket = nullptr;
        UdpSenderEntry *prevSender = nullptr;  // Used to form list of all senders that have outstanding requests
        UdpSenderEntry *nextSender = nullptr;  // Used to form list of all senders that have outstanding requests
        flowType::flowCmd state = flowType::send_completed;    // Meaning I'm not on any queue
        sequence_t flowSeq = 0;                // the sender's most recent flow sequence number
        sequence_t sendSeq = 0;                // the sender's most recent sequence number from request-to-send, representing sequence number of next packet it will send
        unsigned timeouts = 0;                 // How many consecutive timeouts have happened on the current request
        unsigned requestTime = 0;              // When we received the active requestToSend
        unsigned timeStamp = 0;                // When we last sent okToSend

    private:
        // Updated by receive_data thread, read atomically by receive_flow
        mutable CriticalSection psCrit;
        PacketTracker packetsSeen;

    public:

        UdpSenderEntry(const IpAddress &_dest, unsigned port) : dest(_dest)
        {
            SocketEndpoint ep(port, dest);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
                flowSocket = CSimulatedWriteSocket::udp_connect(ep);
            else
#endif
                flowSocket = ISocket::udp_connect(ep);
        }

        ~UdpSenderEntry()
        {
            if (flowSocket)
            {
                flowSocket->close();
                flowSocket->Release();
            }
        }
        bool noteSeen(UdpPacketHeader &hdr)
        {
            if (udpResendLostPackets)
            {
                CriticalBlock b(psCrit);
                return packetsSeen.noteSeen(hdr);
            }
            else
                return false;
        }

        bool canSendAny() const
        {
            // We can send some if (a) the first available new packet is less than TRACKER_BITS above the first unreceived packet or
            // (b) we are assuming arrival in order, and there are some marked seen that are > first unseen OR
            // (c) the oldest in-flight packet has expired
            if (!udpResendLostPackets)
                return true;
            {
                CriticalBlock b(psCrit);
                if (packetsSeen.canRecord(sendSeq))
                    return true;
                if (udpAssumeSequential && packetsSeen.hasGaps())
                    return true;
            }
            if (msTick()-requestTime > udpResendTimeout)
                return true;
            return false;
        }

        void acknowledgeRequest(const IpAddress &returnAddress, sequence_t _flowSeq, sequence_t _sendSeq)
        {
            if (flowSeq==_flowSeq)
            {
                // It's a duplicate request-to-send - ignore it? Or assume it means they lost our ok-to-send ? MORE - probably depends on state
                if (udpTraceLevel || udpTraceFlow)
                {
                    StringBuffer s;
                    DBGLOG("UdpFlow: ignoring duplicate requestToSend %" SEQF "u from node %s", _flowSeq, dest.getIpText(s).str());
                }
                return;
            }
            flowSeq = _flowSeq;
            sendSeq = _sendSeq;
            requestTime = msTick();
            timeouts = 0;
            try
            {
                UdpPermitToSendMsg msg;
                msg.cmd = flowType::request_received;
                msg.flowSeq = _flowSeq;
                msg.destNode = returnAddress;
                msg.max_data = 0;
                if (udpResendLostPackets)
                {
                    CriticalBlock b(psCrit);
                    msg.seen = packetsSeen.copy();
                }

                if (udpTraceLevel > 3 || udpTraceFlow)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: sending request_received msg seq %" SEQF "u to node=%s", _flowSeq, dest.getIpText(ipStr).str());
                }
                flowSocket->write(&msg, udpResendLostPackets ? sizeof(UdpPermitToSendMsg) : offsetof(UdpPermitToSendMsg, seen));
                flowPermitsSent++;

            }
            catch(IException *e)
            {
                StringBuffer d, s;
                DBGLOG("UdpReceiver: acknowledgeRequest failed node=%s %s", dest.getIpText(d).str(), e->errorMessage(s).str());
                e->Release();
            }
        }

        void requestToSend(unsigned maxTransfer, const IpAddress &returnAddress)
        {
            try
            {
                UdpPermitToSendMsg msg;
                msg.cmd = maxTransfer ? flowType::ok_to_send : flowType::request_received;
                msg.flowSeq = flowSeq;
                msg.destNode = returnAddress;
                msg.max_data = maxTransfer;
                if (udpResendLostPackets)
                {
                    CriticalBlock b(psCrit);
                    msg.seen = packetsSeen.copy();
                }
                if (udpTraceLevel > 3 || udpTraceFlow)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: sending ok_to_send %u msg seq %" SEQF "u to node=%s", maxTransfer, flowSeq, dest.getIpText(ipStr).str());
                }
                flowSocket->write(&msg, udpResendLostPackets ? sizeof(UdpPermitToSendMsg) : offsetof(UdpPermitToSendMsg, seen));
                flowPermitsSent++;
            }
            catch(IException *e)
            {
                StringBuffer d, s;
                DBGLOG("UdpReceiver: requestToSend failed node=%s %s", dest.getIpText(d).str(), e->errorMessage(s).str());
                e->Release();
            }
        }

    };

    class SenderList
    {
        UdpSenderEntry *head = nullptr;
        UdpSenderEntry *tail = nullptr;
        unsigned numEntries = 0;

        void checkListIsValid(UdpSenderEntry *lookfor)
        {
#ifdef _DEBUG
            UdpSenderEntry *prev = nullptr;
            UdpSenderEntry *finger = head;
            unsigned length = 0;
            while (finger)
            {
                if (finger==lookfor)
                    lookfor = nullptr;
                prev = finger;
                finger = finger->nextSender;
                length++;
            }
            assert(prev == tail);
            assert(lookfor==nullptr);
            assert(numEntries==length);
#endif
        }
    public:
        unsigned length() const { return numEntries; }
        operator UdpSenderEntry *() const
        {
            return head;
        }
        void append(UdpSenderEntry *sender)
        {
            if (tail)
            {
                tail->nextSender = sender;
                sender->prevSender = tail;
                tail = sender;
            }
            else
            {
                head = tail = sender;
            }
            numEntries++;
            checkListIsValid(sender);
        }
        void remove(UdpSenderEntry *sender)
        {
            if (sender->prevSender)
                sender->prevSender->nextSender = sender->nextSender;
            else
                head = sender->nextSender;
            if (sender->nextSender)
                sender->nextSender->prevSender = sender->prevSender;
            else
                tail = sender->prevSender;
            sender->prevSender = nullptr;
            sender->nextSender = nullptr;
            numEntries--;
            checkListIsValid(nullptr);
        }
    };

    IpMapOf<UdpSenderEntry> sendersTable;

    class receive_receive_flow : public Thread 
    {
        CReceiveManager &parent;
        Owned<ISocket> flow_socket;
        const unsigned flow_port;
        const unsigned maxSlotsPerSender;
        std::atomic<bool> running = { false };
        
        SenderList pendingRequests;     // List of people wanting permission to send
        SenderList pendingPermits;      // List of people given permission to send

        void enqueueRequest(UdpSenderEntry *requester, sequence_t flowSeq, sequence_t sendSeq)
        {
            switch (requester->state)
            {
            case flowType::ok_to_send:
                pendingPermits.remove(requester);
                // Fall through
            case flowType::send_completed:
                pendingRequests.append(requester);
                requester->state = flowType::request_to_send;
                break;
            case flowType::request_to_send:
                // Perhaps the sender never saw our permission? Already on queue...
                break;
            default:
                // Unexpected state, should never happen!
                DBGLOG("ERROR: Unexpected state %s in enqueueRequest", flowType::name(requester->state));
                throwUnexpected();
                break;
            }
            requester->acknowledgeRequest(myNode.getIpAddress(), flowSeq, sendSeq);  // Acknowledge receipt of the request

        }

        void okToSend(UdpSenderEntry *requester, unsigned slots)
        {
            switch (requester->state)
            {
            case flowType::request_to_send:
                pendingRequests.remove(requester);
                // Fall through
            case flowType::send_completed:
                pendingPermits.append(requester);
                requester->state = flowType::ok_to_send;
                break;
            case flowType::ok_to_send:
                // Perhaps the sender never saw our permission? Already on queue...
                break;
            default:
                // Unexpected state, should never happen!
                DBGLOG("ERROR: Unexpected state %s in okToSend", flowType::name(requester->state));
                throwUnexpected();
                break;
            }
            requester->timeStamp = msTick();
            requester->requestToSend(slots, myNode.getIpAddress());
        }

        void noteDone(UdpSenderEntry *requester, const UdpRequestToSendMsg &msg)
        {
            switch (requester->state)
            {
            case flowType::request_to_send:
                // A bit unexpected but will happen if our previous permission timed out and we pushed to back of the requests queue
                pendingRequests.remove(requester);
                break;
            case flowType::ok_to_send:
                pendingPermits.remove(requester);
                break;
            case flowType::send_completed:
                DBGLOG("Duplicate completed message received: msg %s flowSeq %" SEQF "u sendSeq %" SEQF "u. Ignoring", flowType::name(msg.cmd), msg.flowSeq, msg.sendSeq);
                break;
            default:
                // Unexpected state, should never happen! Ignore.
                DBGLOG("ERROR: Unexpected state %s in noteDone", flowType::name(requester->state));
                break;
            }
            requester->state = flowType::send_completed;
        }

    public:
        receive_receive_flow(CReceiveManager &_parent, unsigned flow_p, unsigned _maxSlotsPerSender)
        : Thread("UdpLib::receive_receive_flow"), parent(_parent), flow_port(flow_p), maxSlotsPerSender(_maxSlotsPerSender)
        {
        }
        
        ~receive_receive_flow() 
        {
            if (running)
            {
                running = false;
                if (flow_socket)
                    flow_socket->close();
                join();
            }
        }

        virtual void start()
        {
            running = true;
            if (check_max_socket_read_buffer(udpFlowSocketsSize) < 0)
                throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
                flow_socket.setown(CSimulatedReadSocket::udp_create(SocketEndpoint(flow_port, myNode.getIpAddress())));
            else
#endif
                flow_socket.setown(ISocket::udp_create(flow_port));
            flow_socket->set_receive_buffer_size(udpFlowSocketsSize);
            size32_t actualSize = flow_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: receive_receive_flow created port=%d sockbuffsize=%d actual %d", flow_port, udpFlowSocketsSize, actualSize);
            Thread::start();
        }

        void doFlowRequest(const UdpRequestToSendMsg &msg)
        {
            flowRequestsReceived++;
            if (udpTraceLevel > 5 || udpTraceFlow)
            {
                StringBuffer ipStr;
                DBGLOG("UdpReceiver: received %s msg flowSeq %" SEQF "u sendSeq %" SEQF "u from node=%s", flowType::name(msg.cmd), msg.flowSeq, msg.sendSeq, msg.sourceNode.getTraceText(ipStr).str());
            }
            UdpSenderEntry *sender = &parent.sendersTable[msg.sourceNode];
            switch (msg.cmd)
            {
            case flowType::request_to_send:
                enqueueRequest(sender, msg.flowSeq, msg.sendSeq);
                break;

            case flowType::send_completed:
                noteDone(sender, msg);
                break;

            case flowType::request_to_send_more:
                noteDone(sender, msg);
                enqueueRequest(sender, msg.flowSeq+1, msg.sendSeq);
                break;

            default:
                DBGLOG("UdpReceiver: received unrecognized flow control message cmd=%i", msg.cmd);
            }
        }

        unsigned checkPendingRequests()
        {
            unsigned timeout = 5000;   // The default timeout is 5 seconds if nothing is waiting for response...
            if (pendingPermits)
            {
                unsigned now = msTick();
                for (UdpSenderEntry *finger = pendingPermits; finger != nullptr; )
                {
                    if (now - finger->timeStamp >= udpRequestToSendAckTimeout)
                    {
                        if (udpTraceLevel || udpTraceFlow || udpTraceTimeouts)
                        {
                            StringBuffer s;
                            DBGLOG("permit to send %" SEQF "u to node %s timed out after %u ms, rescheduling", finger->flowSeq, finger->dest.getIpText(s).str(), udpRequestToSendAckTimeout);
                        }
                        UdpSenderEntry *next = finger->nextSender;
                        pendingPermits.remove(finger);
                        if (++finger->timeouts > udpMaxRetryTimedoutReqs && udpMaxRetryTimedoutReqs != 0)
                        {
                            if (udpTraceLevel || udpTraceFlow || udpTraceTimeouts)
                            {
                                StringBuffer s;
                                DBGLOG("permit to send %" SEQF "u to node %s timed out %u times - abandoning", finger->flowSeq, finger->dest.getIpText(s).str(), finger->timeouts);
                            }
                        }
                        else
                        {
                            // Put it back on the queue (at the back)
                            finger->timeStamp = now;
                            pendingRequests.append(finger);
                            finger->state = flowType::request_to_send;
                        }
                        finger = next;
                    }
                    else
                    {
                        timeout = finger->timeStamp + udpRequestToSendAckTimeout - now;
                        break;
                    }
                }
            }
            unsigned slots = parent.input_queue->available();
            bool anyCanSend = false;
            for (UdpSenderEntry *finger = pendingRequests; finger != nullptr; finger = finger->nextSender)
            {
                if (pendingPermits.length()>=udpMaxPendingPermits)
                    break;
                if (!slots) // || slots<minSlotsPerSender)
                {
                    timeout = 1;   // Slots should free up very soon!
                    break;
                }
                // If requester would not be able to send me any (because of the ones in flight) then wait

                if (finger->canSendAny())
                {
                    unsigned requestSlots = slots;
                    if (requestSlots>maxSlotsPerSender)
                        requestSlots = maxSlotsPerSender;
                    okToSend(finger, requestSlots);
                    slots -= requestSlots;
                    if (timeout > udpRequestToSendAckTimeout)
                        timeout = udpRequestToSendAckTimeout;
                    anyCanSend = true;
                }
                else
                {
                    if (udpTraceFlow)
                    {
                        StringBuffer s;
                        DBGLOG("Sender %s can't be given permission to send yet as resend buffer full", finger->dest.getIpText(s).str());
                    }
                }
            }
            if (slots && pendingRequests.length() && pendingPermits.length()<udpMaxPendingPermits && !anyCanSend)
            {
                if (udpTraceFlow)
                {
                    StringBuffer s;
                    DBGLOG("All senders blocked by resend buffers");
                }
                timeout = 1; // Hopefully one of the senders should unblock soon
            }
            return timeout;
        }

        virtual int run() override
        {
            DBGLOG("UdpReceiver: receive_receive_flow started");
        #ifdef __linux__
            setLinuxThreadPriority(3);
        #else
            adjustPriority(1);
        #endif
            UdpRequestToSendMsg msg;
            unsigned timeout = 5000;
            while (running)
            {
                try
                {
                    if (udpTraceLevel > 5 || udpTraceFlow)
                    {
                        DBGLOG("UdpReceiver: wait_read(%u)", timeout);
                    }
                    bool dataAvail = flow_socket->wait_read(timeout);
                    if (dataAvail)
                    {
                        const unsigned l = sizeof(msg);
                        unsigned int res ;
                        flow_socket->readtms(&msg, l, l, res, 0);
                        assert(res==l);
                        doFlowRequest(msg);
                    }
                    timeout = checkPendingRequests();
                }
                catch (IException *e)
                {
                    if (running)
                    {
                        StringBuffer s;
                        DBGLOG("UdpReceiver: failed %i %s", flow_port, e->errorMessage(s).str());
                    }
                    e->Release();
                }
                catch (...)
                {
                    DBGLOG("UdpReceiver: receive_receive_flow::run unknown exception");
                }
            }
            return 0;
        }

    };

    class receive_data : public Thread 
    {
        CReceiveManager &parent;
        ISocket *receive_socket = nullptr;
        std::atomic<bool> running = { false };
        Semaphore started;
        
    public:
        receive_data(CReceiveManager &_parent) : Thread("UdpLib::receive_data"), parent(_parent)
        {
            unsigned ip_buffer = parent.input_queue_size*DATA_PAYLOAD*2;
            if (ip_buffer < udpFlowSocketsSize) ip_buffer = udpFlowSocketsSize;
            if (check_max_socket_read_buffer(ip_buffer) < 0) 
                throw MakeStringException(ROXIE_UDP_ERROR, "System socket max read buffer is less than %u", ip_buffer);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
                receive_socket = CSimulatedReadSocket::udp_create(SocketEndpoint(parent.data_port, myNode.getIpAddress()));
            else
#endif
                receive_socket = ISocket::udp_create(parent.data_port);
            receive_socket->set_receive_buffer_size(ip_buffer);
            size32_t actualSize = receive_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: rcv_data_socket created port=%d requested sockbuffsize=%d actual sockbuffsize=%d", parent.data_port, ip_buffer, actualSize);
            running = false;
        }

        virtual void start()
        {
            running = true;
            Thread::start();
            started.wait();
        }
        
        ~receive_data()
        {
            DBGLOG("Total data packets seen = %u OOO(%u) Requests(%u) Permits(%u)", dataPacketsReceived.load(), packetsOOO.load(), flowRequestsReceived.load(), flowRequestsSent.load());

            running = false;
            if (receive_socket)
                receive_socket->close();
            join();
            ::Release(receive_socket);
        }

        virtual int run() 
        {
            DBGLOG("UdpReceiver: receive_data started");
        #ifdef __linux__
            setLinuxThreadPriority(4);
        #else
            adjustPriority(2);
        #endif
            started.signal();
            unsigned lastOOOReport = 0;
            unsigned lastPacketsOOO = 0;
            unsigned timeout = 5000;
            DataBuffer *b = nullptr;
            while (running) 
            {
                try 
                {
                    unsigned int res;
                    bool dataAvail = receive_socket->wait_read(timeout);
                    if (dataAvail)
                    {
                        if (!b)
                            b = bufferManager->allocate();
                        receive_socket->readtms(b->data, 1, DATA_PAYLOAD, res, 0);
                        if (res!=sizeof(UdpRequestToSendMsg))
                        {
                            dataPacketsReceived++;
                            UdpPacketHeader &hdr = *(UdpPacketHeader *) b->data;
                            assert(hdr.length == res && hdr.length > sizeof(hdr));
                            UdpSenderEntry *sender = &parent.sendersTable[hdr.node];
                            if (sender->noteSeen(hdr))
                            {
                                if (udpTraceLevel > 5) // don't want to interrupt this thread if we can help it
                                {
                                    StringBuffer s;
                                    DBGLOG("UdpReceiver: discarding unwanted resent packet %" SEQF "u %x from %s", hdr.sendSeq, hdr.pktSeq, hdr.node.getTraceText(s).str());
                                }
                                hdr.node.clear();  // Used to indicate a duplicate that collate thread should discard. We don't discard on this thread as don't want to do anything that requires locks...
                            }
                            else
                            {
                                if (udpTraceLevel > 5) // don't want to interrupt this thread if we can help it
                                {
                                    StringBuffer s;
                                    DBGLOG("UdpReceiver: %u bytes received packet %" SEQF "u %x from %s", res, hdr.sendSeq, hdr.pktSeq, hdr.node.getTraceText(s).str());
                                }
                            }
                            parent.input_queue->pushOwn(b);
                            b = nullptr;
                        }
                        else
                        {
                            assert(parent.data_port==parent.receive_flow_port);
                            parent.receive_flow->doFlowRequest(*(UdpRequestToSendMsg *) b->data);
                            timeout = parent.receive_flow->checkPendingRequests();
                        }
                    }
                    else
                        timeout = parent.receive_flow->checkPendingRequests();
                    if (udpStatsReportInterval)
                    {
                        unsigned now = msTick();
                        if (now-lastOOOReport > udpStatsReportInterval)
                        {
                            lastOOOReport = now;
                            if (packetsOOO > lastPacketsOOO)
                            {
                                DBGLOG("%u more packets received out-of-order by this server (%u total)", packetsOOO-lastPacketsOOO, packetsOOO-0);
                                lastPacketsOOO = packetsOOO;
                            }
                            if (flowRequestsReceived > lastFlowRequestsReceived)
                            {
                                DBGLOG("%u more flow requests received by this server (%u total)", flowRequestsReceived-lastFlowRequestsReceived, flowRequestsReceived-0);
                                lastFlowRequestsReceived = flowRequestsReceived;
                            }
                            if (flowPermitsSent > lastFlowPermitsSent)
                            {
                                DBGLOG("%u more flow permits sent by this server (%u total)", flowPermitsSent-lastFlowPermitsSent, flowPermitsSent-0);
                                lastFlowPermitsSent = flowPermitsSent;
                            }
                            if (dataPacketsReceived > lastDataPacketsReceived)
                            {
                                DBGLOG("%u more data packets received by this server (%u total)", dataPacketsReceived-lastDataPacketsReceived, dataPacketsReceived-0);
                                lastDataPacketsReceived = dataPacketsReceived;
                            }
                        }
                    }
                }
                catch (IException *e) 
                {
                    if (running && e->errorCode() != JSOCKERR_timeout_expired)
                    {
                        StringBuffer s;
                        DBGLOG("UdpReceiver: receive_data::run read failed port=%u - Exp: %s", parent.data_port,  e->errorMessage(s).str());
                        MilliSleep(1000); // Give a chance for mem free
                    }
                    e->Release();
                }
                catch (...) 
                {
                    DBGLOG("UdpReceiver: receive_data::run unknown exception port %u", parent.data_port);
                    MilliSleep(1000);
                }
            }
            ::Release(b);
            return 0;
        }
    };

    class CPacketCollator : public Thread
    {
        CReceiveManager &parent;
    public:
        CPacketCollator(CReceiveManager &_parent) : Thread("CPacketCollator"), parent(_parent) {}

        virtual int run() 
        {
            DBGLOG("UdpReceiver: CPacketCollator::run");
            parent.collatePackets();
            return 0;
        }
    } collatorThread;


    friend class receive_receive_flow;
    friend class receive_send_flow;
    friend class receive_data;
    friend class ReceiveFlowManager;
    
    queue_t              *input_queue;
    int                  input_queue_size;
    receive_receive_flow *receive_flow;
    receive_data         *data;
    
    int                  receive_flow_port;
    int                  data_port;

    std::atomic<bool> running = { false };
    bool encrypted = false;

    typedef std::map<ruid_t, CMessageCollator*> uid_map;
    uid_map         collators;
    SpinLock collatorsLock; // protects access to collators map

  public:
    IMPLEMENT_IINTERFACE;
    CReceiveManager(int server_flow_port, int d_port, int client_flow_port, int queue_size, int m_slot_pr_client, bool _encrypted)
        : collatorThread(*this), encrypted(_encrypted), sendersTable([client_flow_port](const ServerIdentifier ip) { return new UdpSenderEntry(ip.getIpAddress(), client_flow_port);})
    {
#ifndef _WIN32
        setpriority(PRIO_PROCESS, 0, -15);
#endif
        receive_flow_port = server_flow_port;
        data_port = d_port;
        input_queue_size = queue_size;
        input_queue = new queue_t(queue_size);
        data = new receive_data(*this);
        receive_flow = new receive_receive_flow(*this, server_flow_port, m_slot_pr_client);

        running = true;
        collatorThread.start();
        data->start();
        if (data_port != receive_flow_port)
            receive_flow->start();
        MilliSleep(15);
    }

    ~CReceiveManager() 
    {
        running = false;
        input_queue->interrupt();
        collatorThread.join();
        delete data;
        delete receive_flow;
        delete input_queue;
    }

    virtual void detachCollator(const IMessageCollator *msgColl) 
    {
        ruid_t ruid = msgColl->queryRUID();
        if (udpTraceLevel >= 2) DBGLOG("UdpReceiver: detach %p %u", msgColl, ruid);
        {
            SpinBlock b(collatorsLock);
            collators.erase(ruid);
        }
        msgColl->Release();
    }

    void collatePackets()
    {
        while(running) 
        {
            try
            {
                DataBuffer *dataBuff = input_queue->pop(true);
                collatePacket(dataBuff);
            }
            catch (IException * e)
            {
                //An interrupted semaphore exception is expected at closedown - anything else should be reported
                if (!dynamic_cast<InterruptedSemaphoreException *>(e))
                    EXCLOG(e);
                e->Release();
            }
        }
    }

    void collatePacket(DataBuffer *dataBuff)
    {
        const UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;

        if (udpTraceLevel >= 4) 
        {
            StringBuffer s;
            DBGLOG("UdpReceiver: CPacketCollator - unQed packet - ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X len=%d node=%s",
                pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->length, pktHdr->node.getTraceText(s).str());
        }

        Linked <CMessageCollator> msgColl;
        bool isDefault = false;
        {
            SpinBlock b(collatorsLock);
            try
            {
                msgColl.set(collators[pktHdr->ruid]);
                if (!msgColl)
                {
                    msgColl.set(collators[RUID_DISCARD]);
                    isDefault = true;
                    unwantedDiscarded++;
                }
            }
            catch (IException *E)
            {
                EXCLOG(E);
                E->Release();
            }
            catch (...)
            {
                IException *E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception caught in CPacketCollator::run");
                EXCLOG(E);
                E->Release();
            }
        }
        if (udpTraceLevel && isDefault && !isUdpTestMode)
        {
            StringBuffer s;
            DBGLOG("UdpReceiver: CPacketCollator NO msg collator found - using default - ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%s", pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->node.getTraceText(s).str());
        }
        if (msgColl && msgColl->attach_databuffer(dataBuff))
            dataBuff = nullptr;
        else
            dataBuff->Release();
    }

    virtual IMessageCollator *createMessageCollator(IRowManager *rowManager, ruid_t ruid)
    {
        CMessageCollator *msgColl = new CMessageCollator(rowManager, ruid, encrypted);
        if (udpTraceLevel > 2)
            DBGLOG("UdpReceiver: createMessageCollator %p %u", msgColl, ruid);
        {
            SpinBlock b(collatorsLock);
            collators[ruid] = msgColl;
        }
        msgColl->Link();
        return msgColl;
    }
};

IReceiveManager *createReceiveManager(int server_flow_port, int data_port, int client_flow_port,
                                      int udpQueueSize, unsigned maxSlotsPerSender,
                                      bool encrypted)
{
    assertex (maxSlotsPerSender <= (unsigned) udpQueueSize);
    assertex (maxSlotsPerSender <= (unsigned) TRACKER_BITS);
    return new CReceiveManager(server_flow_port, data_port, client_flow_port, udpQueueSize, maxSlotsPerSender, encrypted);
}

/*
Thoughts on flow control / streaming:
1. The "continuation packet" mechanism does have some advantages
    - easy recovery from agent failures
    - agent recovers easily from Roxie server failures
    - flow control is simple (but is it effective?)

2. Abandoning continuation packet in favour of streaming would give us the following issues:
    - would need some flow control to stop getting ahead of a Roxie server that consumed slowly
        - flow control is non trivial if you want to avoid tying up a agent thread and want agent to be able to recover from Roxie server failure
    - Need to work out how to do GSS - the nextGE info needs to be passed back in the flow control?
    - can't easily recover from agent failures if you already started processing
        - unless you assume that the results from agent are always deterministic and can retry and skip N
    - potentially ties up a agent thread for a while 
        - do we need to have a larger thread pool but limit how many actually active?

3. Order of work
    - Just adding streaming while ignoring flow control and continuation stuff (i.e. we still stop for permission to continue periodically)
        - Shouldn't make anything any _worse_ ...
            - except that won't be able to recover from a agent dying mid-stream (at least not without some considerable effort)
                - what will happen then?
            - May also break server-side caching (that no-one has used AFAIK). Maybe restrict to nohits as we change....
    - Add some flow control
        - would prevent agent getting too far ahead in cases that are inadequately flow-controlled today
        - shouldn't make anything any worse...
    - Think about removing continuation mechanism from some cases

Per Gavin, streaming would definitely help for the lowest frequency term.  It may help for the others as well if it avoided any significant start up costs - e.g., opening the indexes,
creating the segment monitors, creating the various cursors, and serialising the context (especially because there are likely to be multiple cursors).

To add streaming:
    - Need to check for meta availability other than when first received
        - when ? 
    - Need to cope with a getNext() blocking without it causing issues
     - perhaps should recode getNext() of variable-size rows first?

More questions:
    - Can we afford the memory for the resend info?
        - Save maxPacketsPerSender per sender ?
    - are we really handling restart and sequence wraparound correctly?
    - what about server-side caching? Makes it hard
        - but maybe we should only cache tiny replies anyway....

Problems found while testing implemetnation:
    - the unpacker cursor read code is crap
    - there is a potential to deadlock when need to make a callback agent->server during a streamed result (indexread5 illustrates)
        - resolution callback code doesn't really need to be query specific - could go to the default handler
        - but other callbacks - ALIVE, EXCEPTION, and debugger are not so clear
    - It's not at all clear where to move the code for processing metadata
    - callback paradigm would solve both - but it has to be on a client thread (e.g. from within call to next()).

    The following are used in "pseudo callback" mode:
    #define ROXIE_DEBUGREQUEST 0x3ffffff7u
    #define ROXIE_DEBUGCALLBACK 0x3ffffff8u
    #define ROXIE_PING 0x3ffffff9u
        - goes to own handler anyway
    #define ROXIE_TRACEINFO 0x3ffffffau
        - could go in meta? Not time critical. Could all go to single handler? (a bit hard since we want to intercept for caller...)
    #define ROXIE_FILECALLBACK 0x3ffffffbu
        - could go to single handler
    #define ROXIE_ALIVE 0x3ffffffcu
        - currently getting delayed a bit too much potentially if downstream processing is slow? Do I even need it if streaming?
    #define ROXIE_KEYEDLIMIT_EXCEEDED 0x3ffffffdu
        - could go in metadata of standard response
    #define ROXIE_LIMIT_EXCEEDED 0x3ffffffeu
        - ditto
    #define ROXIE_EXCEPTION   0x3fffffffu
        - ditto
    And the continuation metadata.

    What if EVERYTHING was a callback? - here's an exception... here's some more rows... here's some tracing... here's some continuation metadata
    Somewhere sometime I need to marshall from one thread to another though (maybe more than once unless I can guarantee callback is always very fast)

    OR (is it the same) everything is metadata ? Metadata can contain any of the above information (apart from rows - or maybe they are just another type)
    If I can't deal quickly with a packet of information, I queue it up? Spanning complicates things though. I need to be able to spot complete portions of metadata
    (and in kind-of the same way I need to be able to spot complete rows of data even when they span multiple packets.) I think data is really a bit different from the rest -
    you expect it to be continuous and you want the others to interrupt the flow.

    If continuation info was restricted to a "yes/no" (i.e. had to be continued on same node as started on) could have simple "Is there any continuation" bit. Others are sent in their 
    own packets so are a little different. Does that make it harder to recover? Not sure that it does really (just means that the window at which a failure causes a problem starts earlier).
    However it may be an issue tying up agent thread for a while (and do we know when to untie it if the Roxie server abandons/restarts?)

    Perhaps it makes sense to pause at this point (with streaming disabled and with retry mechanism optional)





*/
