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
#include <algorithm>

#include "jthread.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
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

/*

The UDP transport layer uses the following timeouts:

Timeouts:
    udpFlowAckTimeout   - the maximum time that it is expected to take to receive an acknowledgement of a flow message (when one is
                          sent) - should be small
                          => timeout for request to send before re-requesting
                          [sender] resend the request to send
    updDataSendTimeout  - the maximum time that is expected to take to send the data once a permit has been granted.
                          => timeout for assuming send_complete has been lost
                          [sender] the timeout before re-requesting a request-to-send after sending a block of data
                                   (unused if permits are asynchronous)
                          [receiver] Used to estimate a timeout if there are active requests and not enough active slots
    udpRequestTimeout   - A reasonable expected time between a request for a permit until the permit is granted
                          => timeout for guarding against an ok_to_send being lost.
                          [sender] if no permit granted within timeout re-request to send
    udpPermitTimeout    - the maximum time that it is expected to take to send and receive the data once a permit is granted.
                          => Timeout for a permit before it is assumed lost
                          [receiver] If rts received while permit is active, permit is resent.  If not complete within timeout,
                                     revoke the permit.
    udpResendDelay      - the time that should have elapsed before a missing data packet is resent
                          (I think this only makes sense if a new permit can be granted before all the data has been received,
                          so the request to send more is sent to the flow port.)
                          0 means they are unlikely to be lost, so worth resending as soon as it appears to be missing - trading
                          duplicate packets for delays
                          [sender] minimum time to elapse from initial send before sending packets that are assumed lost
                          [receiver] minimum time to elapse before receiver assumes that a sender will send missing packets.
Also:
    udpMaxPermitDeadTimeouts   - How many permit grants are allowed to expire (with no flow message) until sender is assumed down
    udpRequestDeadTimeout      - Timeout for sender getting no response to request to send before assuming that the receiver is dead


General flow:
-------------
The general flow is as follows:

On the sender:
* When data becomes available (and none was previously present)
  - Send a request to send to the receiver flow port.  Set timeout to ack timeout.

* When receive "request_received"
  - Set timeout to udpRequestTimeout

* When receive ok_to_send, add the permit to a permit queue.
  - Mark target as permit pending (to avoid resending requests)

* Periodically:
  If permit requested, timeout has expired, and permit not received, then resubmit te request (with ack timeout)

* When a permit is popped from the queue
  - gather packets to resend that are not recorded as received in the header sent by the receiver
  - gather any extra data packets to send up to the permit size.
  - Send a begin_send [to the <flow> port - so the permit is adjusted early]
  - Send the data packets
  - if no more data (and nothing in the resend list) send send_completed to data port
    if async permits:
        send  send_completed to data port, and request_to_send to the flow port (timeout = udpFlowAckTimeout).
    else
        send request_to_send_more to the data port, and set re-request timeout to udpDataSendTimeout.

On the receiver:
* When receive request_to_send:
  - If the flowSeq matches an active permit resend the ok_to_send
  - otherwise add to requests list (if not already present) and send an acknowledgement
  - check to grant new permits

* When receive begin_send
  - Adjust the permit to the actual number of packets being sent
  - check to grant new permits

* When receive send_completed:
  - remove from permits list (and free up the permit)
  - check to grant new permits

* When receive request_to_send_more:
  - Treat as send_complete, followed by a request_to_send.

Behaviour on lost flow messages
-------------------------------
* request_to_send.
  - sender will re-request fairly quickly
  - receiver needs no special support
  => delay of ack timeout for this sender to start sending data

* request_received
  - sender will re-request fairly quickly
  - receiver needs to acknowledge duplicate requests to send (but ignore requests with a lower flow id, since they have arrived out of order)
  => extra flow message, but no delay since receiver will still go ahead with allocating permits.

* ok_to_send
  - sender will re-request if not received within a time limit
  - receiver will remove permit after timeout, and grant new permits
  - if a receiver gets a request to send for an active permit, it resends the ok_to_send (but retains the original timeout)
  => the available permits will be reduced by the number allocated to the sender until the permit expires.
     If multiple permits are not supported no data will be received by this node.
     if (udpRequestTimeout < udpPermitTimeout) the sender will re-request the permit, and potentially be re-sent ok_to_send

* send_start
  - allocated permits are not reduced as quickly as they could be
  => input queue will not have as much data sent to it, (reducing the number of parallel sends?)

* send_completed
  - allocated permit will last longer than it needs to.
  => similar to ok_to_send: reduced permits and delay in receiving any extra data if only a single permit is allowed.
     any miscalulated permits will be returned when permit expires.

* request_to_send_more
  - allocated permit will last longer than it needs to
  - sender will eventually send a new request-to-send after the DataSend timeout
  => reduced permits and no data received for a while if a single permit

* data packet
  - the next flow message from the receiver will contain details of which packets have been received.
  - the next permit will possibly be used to send some of the missing packets (see suggestions for changes from current)
  => collator will not be able to combine and pass data stream onto the activities.  Receiver memory consumption will go up.

Reordering problems:

- send_started processed after data received - fewer permits issued and possible over-commit on the number of permits, take care it cannot persist
- send_completed processed after next request_to_send - ignored because flow seq is previous seq
- request_received processed after ok_to_send - permit still pushed, unlikely to cause any problems.

Timeout problems
----------------
For each of the timeouts, what happens if they are set too high, or too low, and what is an estimate for a "good" value?  In general, setting a
timeout too high will affect the time to recover from lost packets.  Setting a timeout too low may cause catestrophic degredation under load -
because the extra flow messages or data packets will reduce the overall capacity.

* udpFlowAckTimeout
  Too high: delay in sending data if a request_to_send/request_received is lost
  Too low: flow control cannot acknowledge quickly enough, and receiver flow control is flooded.
  Suggestion: Should keep low, but avoid any risk of flooding.  10 times the typical time to process a request?

* updDataSendTimeout
  Too high: lost send_begin will reduce the number of permits, lost request_to_send_more will delay the sender
  Too low: permits will expire while the data is being transferred - slots will be over-committed.
           sender will potentially re-request to send before all the data has been sent over the wire
           which will cause all "missing" packets to be resent if udpResendDelay is low.
  Suggestion: If multiple permits, probably better to be too high than too low.
              E.g. The time to send and receive the data for all/half the slots?

* udpRequestTimeout
  Too high:  if (>udPermitTimeout) then sender will need to wait for the permit to be regranted
  Too low:   could flood receiver with requests if it is very busy
  Suggestion: A fraction of the udpPermitTimeout (e.g. 1/2) so a missing ok_to_send will be spotted without losing the permit

* udpPermitTimeout
  Too high: Lost ok_to_send messages will reduce the number of permits for a long time
  Too low: Receiver will be flooded with requests to send when large numbers of nodes want to send.
  Suggestion: Better to be too low than too high.  Similar to udpDataSendTimeout?  10 * the ack timeout?
              (If lower than the udpDataSendTimeout then the permit could be resent)

* udpResendDelay
  Too high: Missing packets will take a long time to be resent.
  Too low: If large proportion of packets reordered or dropped by the network packets will be sent unnecessarily
  Suggestion: Set it to lower than the permit timeout, I'm not sure we want to run on a network where that many packets are being lost!

Timeouts for assuming sender/receiver is dead
Note: A sender/receiver is never really considered dead - it only affects active requests.  If a new request is
      received then the communication will be restarted.

* udpMaxPermitDeadTimeouts
  Too high: The number of available permits will be artificially reduced for too long.  Another reason for supporting multiple permits.
  Too low: Unlikely to cause many problems.  The sender should re-request anyway.  (Currently doesn't throw away any data)
  Suggestion: ~5?.  Better to err low.  Enough so that the loss of a ok_to_send or start/complete are unlikely within the time.

* udpRequestDeadTimeout
  Too high: Packets held in memory for too long, lots of re-requests sent (and ignored)
  Too low: Valid data may be lost when the network gets busy.
  Suggestion: 10s?? Better to err high, but I suspect this is much too high....  50x the Ack timeout should really be enough (which would be 100ms)

Other udp settings
------------------
* udpResendLostPackets
  Enable the code to support resending missing packets.  On a completely reliable network, disabling it would
  reduce the time that blocks were held onto in the sender, reducing the maximum memory footprint.

* udpMaxPendingPermits
  How many permits can be granted at the same time.  Larger numbers will cope better with large numbers of senders
  sending small amounts of data.  Small numbers will increase the number of packets each sender can send.  only 1 or 2 are not recommended...

* udpMaxClientPercent
  The base number of slots allocated to each sender is (queueSize/maxPendingPermits).  This allows a larger proportion
  to be initially granted (on the assumption that many senders will then update the actual number in use with a
  smaller number).

* udpMinSlotsPerSender
  The smallest number of slots to assign to a sender, defaults to 1.  Could increase to prevent lots of small permits being granted.

* udpAssumeSequential
  If the sender has received a later sequence data packet, then resend a non-received packet - regardless of the udpResendDelay

* udpResendAllMissingPackets
  Do no limit the number of missing packets sent to the size of the permit - send all that are ok to end.  The rationale is that
  it is best to get all missing packets sent as quickly as possible to avoid the receiver being blocked.

* udpAllowAsyncPermits
  If set it allows a new permit to be send before the receiver has read all the data for the previous permit.  This allows multiple
  permits to be issued to the same client at the same time (the data could have been sent but not yet received, or the completed
  message from a pervious permit may have been lost.)

* udpStatsReportInterval

* udpAdjustThreadPriorities
  Used for experimentation.  Unlikely to be useful to set to false.

* udpTraceFlow

* udpTraceTimeouts

* udpTraceLevel

* udpOutQsPriority

* udpFlowSocketsSize

* udpLocalWriteSocketSize

Behaviour on lost servers
-------------------------

Receiver:
- Each time a permit expires (i.e. no completed message) the number of timeouts is increased.
- If it exceeds a limit then the request is removed.
- The number of timeouts is reset whenever any message is received from the sender.
=> If a dead sender isn't spotted then the number of active permits may be artificially reduced while the sender is granted a
  permit.  If a single sender has all the permits then the node will not receive any data while that sender has a permit.

Sender:
- Each time a request to send is sent the number of timeouts is increased
- If the number exceeds the timeout threshold all pending data for that target is thrown away and the timeout is reset
- Timeout count is reset when an acknowledgement or permit is received from the receiver

Conclusions
-----------
There are a few conclusions that are worth recording

* We need to support multiple permits, otherwise lost send_completed or ok_to_send flow messages will lead to periods when
  no data is being received.  (Unless we add acknowledgements for those messages.)
* Separate data and flow ports are necessary - otherwise flow requests from other senders will be held up by data.
  send_completed and request_to_send_more should be sent on the data port though (so they don't overtake the data and release the
  permits too early).
* keep the send_completed message - rather than using the seq bit from the last packet (if only for the case where no data is sent)

Which socket should flow messages go to?
----------------------------------------
Most messages need to go the flow port, otherwise they will be held up by the data packets.  There are a couple of
messages that are less obvious:

send_start:
  flow socket is likely to be best - because the message will not be held up by data packets being sent by *other*
  roxie nodes.

send_completed:
  data socket makes sense since it indicates that the sender has finished sending data.  There is no advantage (and
  some disadvantage) to it arriving early.

request_to_send:
  send to the flow socket - otherwise a sender would need to wait for data from this or other senders before permission
  could be granted.

request_to_send_more:
  Really two messages (send_complete and request_to_send), and only used if asynchronous permits are disabled.  It needs
  to go to the data socket for the same reason as send_completed.

Questions/suggestions/future
----------------------------
- What should the relative priorities of the receive flow and data threads be? [ I think should probably be the same ]
- Should the receiver immediately send a permit of 0 blocks to the receiver on send_complete/request_to_send_more to ensure missing
  blocks are sent as quickly as possible (and sender memory is freed up)? [ I suspect yes if request_to_send goes to the data port ]
- Should ok_to_send also have an acknowledgement? [ The udpRequestTimeout provides a mechanism for spotting missing packets ]
- Switch to using ns for the timeout values - so more detailed response timings can be gathered/reported

Supporting multiple permits:
----------------------------

The aim is to allow multiple senders to stream packets at the same time.  The code should aim to not allocate more
permits than there are currently slots available on the receive queue, but a slight temporary over-commit is not
a problem.
The algorithm needs to be resilient when flow control messages are lost.

Approach:

* Add a SendPermit member to the UdpSender class, including a numPackets reservation
* When a permit is granted, set numPackets to the number of slots
* Before a client sends a sequence of data packets it sends a start_send flow control message with the number of packets it is sending.
* When the receiver gets that flow control message it sets numPackets to the number of packets being sent.
* When the receiver receives a non-duplicate data packet it decrements numPackets.
* The slots allocated in a permit are limited by the queue space and the sum of all numPackets values for the active permits. (Ignore overcommit)

This is then further extended to allow multiple permits per sender....

Supporting Asynchronous request_to_send
---------------------------------------

Each sender can have up to MaxPermitsPerSender permits active at the same time.  They are revoked when a completed
message is received (for that permit, or a later permit).  The permits are implemented as an array within the sender
to avoid any dynamic memory allocation.

The main difference for asynchronous requests is that instead of sending request_to_send_more to the data socket, it is split
into two messages - send_complete sent to the data port, and a request_to_send_more to the flow port.

What is the trade off for asynchronous requests?
- Synchronous requests are received after the data, so it makes sense for udpResendDelay to be 0 (i.e. send immediately).
  This means that missing data packets are likely to be sent much more quickly.
- Asynchronous requests allow a sender to start sending more data before the previous data has been read.  When
  there is a single sender this will significantly reduce the latency.
- Asynchronous requests also ensure that requests to send more are not held up by data being sent by other nodes.

It *might* be possible to treat the permits as a circular buffer, but I don't think it would significantly
improve the efficiency.  (Minor when allocating a permit.)

* If a sender has a permit for 0 packets it should only send appropriate "missing" packets, possibly none.

Race conditions:

  update of flowid on flow thread may clash with access to conditional decrement from the data thread.
  - fewer problems if check is prevPermits>0 rather than != 0
  - will eventually (quickly?) recover since no data will be sent, and the done will clear the counters

  request_to_send while previous data has not yet been read and processed
  - as long as subsequent sends don't send any new data, eventually a send_complete will get through, allowing more data to be sent.


*/
using roxiemem::DataBuffer;
using roxiemem::IRowManager;

RelaxedAtomic<unsigned> flowPermitsSent = {0};
RelaxedAtomic<unsigned> flowRequestsReceived = {0};
RelaxedAtomic<unsigned> dataPacketsReceived = {0};
static unsigned lastFlowPermitsSent = 0;
static unsigned lastFlowRequestsReceived = 0;
static unsigned lastDataPacketsReceived = 0;

// The code that redirects flow messages from data socket to flow socket relies on the assumption tested here
static_assert(sizeof(UdpRequestToSendMsg) < sizeof(UdpPacketHeader), "Expected UDP rts size to be less than packet header");


// The following enum is used for the current state of each sender within the udp receiving code
enum class ReceiveState {
    idle,           // no activity from the sender - wating for a request to send
    requested,      // permit to be send has been requested but not granted (other permits may have been granted)
                    // this must be the state if the sender is on the pendingRequests list
    granted,        // at least one permit granted and NO pending request, waiting for data to be sent
    max
};
constexpr const char * receiveStateNameText[(unsigned)ReceiveState::max] = { "idle", "requested", "granted" };
const char * receiveStateName(ReceiveState idx) { return receiveStateNameText[(unsigned)idx]; }


template <class T>
class LinkedListOf
{
    T *head = nullptr;
    T *tail = nullptr;
    unsigned numEntries = 0;

    void checkListIsValid(T *lookfor)
    {
#ifdef _DEBUG
        T *prev = nullptr;
        T *finger = head;
        unsigned length = 0;
        while (finger)
        {
            if (finger==lookfor)
                lookfor = nullptr;
            prev = finger;
            finger = finger->next;
            length++;
        }
        assert(prev == tail);
        assert(lookfor==nullptr);
        assert(numEntries==length);
#endif
    }
public:
    unsigned length() const { return numEntries; }
    operator T *() const
    {
        return head;
    }
    void append(T *sender)
    {
        assertex(!sender->next && (sender != tail));
        if (tail)
        {
            tail->next = sender;
            sender->prev = tail;
            tail = sender;
        }
        else
        {
            head = tail = sender;
        }
        numEntries++;
        checkListIsValid(sender);
    }
    void remove(T *sender)
    {
        if (sender->prev)
            sender->prev->next = sender->next;
        else
            head = sender->next;
        if (sender->next)
            sender->next->prev = sender->prev;
        else
            tail = sender->prev;
        sender->prev = nullptr;
        sender->next = nullptr;
        numEntries--;
        checkListIsValid(nullptr);
    }
};


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
    class UdpSenderEntry;
    class SendPermit
    {
    public:
        SendPermit * prev = nullptr;
        SendPermit * next = nullptr;
        UdpSenderEntry * owner = nullptr;
        std::atomic<unsigned> flowSeq{0};   // The flow id of the request to send data.  Atomic since read from data thead.
        std::atomic<unsigned> numPackets{0};// Updated by receive_data thread, read atomically by receive_flow
        std::atomic<unsigned> sendSeq{0};   // the send sequence when the request - will be <= all datapackets sent for that permit
        unsigned permitTime = 0;            // when was the permit issued?

    public:
        bool isActive() const
        {
            //NOTE: a flowSeq if 0 is not a valid flowSeq (sender ensures that it is never used)
            return flowSeq.load(std::memory_order_acquire) != 0;
        }

        //How many are reserved - never return < 0 to avoid race condition where permit is being expired when a data packet for that permit
        //arrives.
        unsigned getNumReserved() const
        {
            int permits = numPackets.load(std::memory_order_acquire);
            return (unsigned)std::max(permits, 0);
        }

        void grantPermit(unsigned _flowSeq, unsigned _sendSeq, unsigned num, unsigned _permitTime)
        {
            flowSeq = _flowSeq;
            sendSeq = _sendSeq;
            numPackets.store(num, std::memory_order_release);
            permitTime = _permitTime;
        }

        void revokePermit()
        {
            flowSeq = 0;
            sendSeq = 0;
            permitTime = 0;
            numPackets.store(0, std::memory_order_release);
        }
    };

    //Increasing this number, increases the number of concurrent permits a sender may have (and its resilience to lost flow messages),
    //but also increases the processing cost since code often iterates through all the permits.  2..4 likely to be good values.
    static constexpr unsigned MaxPermitsPerSender = 4;

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
        UdpSenderEntry *prev = nullptr;  // Used to form list of all senders that have outstanding requests
        UdpSenderEntry *next = nullptr;  // Used to form list of all senders that have outstanding requests
        ReceiveState state = ReceiveState::idle; // Meaning I'm not on any queue
        sequence_t flowSeq = 0;                // the sender's most recent flow sequence number
        sequence_t sendSeq = 0;                // the sender's most recent sequence number from request-to-send, representing
                                               // sequence number of next packet it will send
        unsigned timeouts = 0;                 // How many consecutive timeouts have happened on the current request
        unsigned requestTime = 0;              // When we received the active requestToSend
        unsigned lastPermitTime = 0;           // When was the last permit granted?
        unsigned numPermits = 0;               // How many permits allocated?

        mutable CriticalSection psCrit;
        PacketTracker packetsSeen;

        SendPermit permits[MaxPermitsPerSender];
        SendPermit * lastDataPermit = permits;   // optimize data packet->permit mapping.  Initialise by pointing at the first permit

    public:

        UdpSenderEntry(const IpAddress &_dest, unsigned port) : dest(_dest)
        {
            SocketEndpoint ep(port, dest);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
                if (udpTestUseUdpSockets)
                    flowSocket = CSimulatedUdpWriteSocket::udp_connect(ep);
                else
                    flowSocket = CSimulatedQueueWriteSocket::udp_connect(ep);
            else
#endif
                flowSocket = ISocket::udp_connect(ep);

            for (SendPermit & permit : permits)
                permit.owner = this;
        }

        ~UdpSenderEntry()
        {
            if (flowSocket)
            {
                shutdownAndCloseNoThrow(flowSocket);
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
            if (!udpResendLostPackets || (udpResendDelay == 0))
                return true;
            {
                CriticalBlock b(psCrit);
                if (packetsSeen.canRecord(sendSeq))
                    return true;
                if (udpAssumeSequential && packetsSeen.hasGaps())
                    return true;
            }

            //The best approximation to the oldest-inflight packet - because permits may have expired...
            return (msTick()-lastPermitTime > udpResendDelay);
        }

        void acknowledgeRequest(const IpAddress &returnAddress, sequence_t _flowSeq, sequence_t _sendSeq)
        {
            if (flowSeq==_flowSeq)
            {
                // It's a duplicate request-to-send - either they lost the request_received, or the ok_to_send (which has timed out)
                // whichever is the case we should resend the acknowledgement to prevent the sender flooding us with requests
                if (udpTraceLevel >= 2 || udpTraceFlow)
                {
                    StringBuffer s;
                    DBGLOG("UdpFlow: Duplicate requestToSend %" SEQF "u from node %s", _flowSeq, dest.getHostText(s).str());
                }
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

                if (udpTraceFlow)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: sending request_received msg seq %" SEQF "u to node=%s", _flowSeq, dest.getHostText(ipStr).str());
                }
#ifdef TEST_DROPPED_PACKETS
                flowPacketsSent[msg.cmd]++;
                if (udpDropFlowPackets[msg.cmd] && flowPacketsSent[msg.cmd]%udpDropFlowPackets[msg.cmd]==0)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: deliberately dropping request_received msg seq %" SEQF "u to node=%s", _flowSeq, dest.getHostText(ipStr).str());
                }
                else
#endif
                flowSocket->write(&msg, udpResendLostPackets ? sizeof(UdpPermitToSendMsg) : offsetof(UdpPermitToSendMsg, seen));
                flowPermitsSent++;

            }
            catch(IException *e)
            {
                StringBuffer d, s;
                DBGLOG("UdpReceiver: acknowledgeRequest failed node=%s %s", dest.getHostText(d).str(), e->errorMessage(s).str());
                e->Release();
            }
        }

        void sendPermitToSend(unsigned maxTransfer, const IpAddress &returnAddress)
        {
            try
            {
                UdpPermitToSendMsg msg;
                msg.cmd = flowType::ok_to_send;
                msg.flowSeq = flowSeq;
                msg.destNode = returnAddress;
                msg.max_data = maxTransfer;
                if (udpResendLostPackets)
                {
                    CriticalBlock b(psCrit);
                    msg.seen = packetsSeen.copy();
                }
                if (udpTraceFlow)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: sending ok_to_send %u msg seq %" SEQF "u to node=%s", maxTransfer, flowSeq, dest.getHostText(ipStr).str());
                }
#ifdef TEST_DROPPED_PACKETS
                flowPacketsSent[msg.cmd]++;
                if (udpDropFlowPackets[msg.cmd] && flowPacketsSent[msg.cmd]%udpDropFlowPackets[msg.cmd]==0)
                {
                    StringBuffer ipStr;
                    DBGLOG("UdpReceiver: deliberately dropping ok_to_send %u msg seq %" SEQF "u to node=%s", maxTransfer, flowSeq, dest.getHostText(ipStr).str());
                }
                else
#endif
                flowSocket->write(&msg, udpResendLostPackets ? sizeof(UdpPermitToSendMsg) : offsetof(UdpPermitToSendMsg, seen));
                flowPermitsSent++;
            }
            catch(IException *e)
            {
                StringBuffer d, s;
                DBGLOG("UdpReceiver: requestToSend failed node=%s %s", dest.getHostText(d).str(), e->errorMessage(s).str());
                e->Release();
            }
        }

        // code to track the number of permits - all functions are called from the flow control thread, except for decPermit() from the data thread
        // need to be careful about concurent modifications. The exact number isn't critical, but
        // we should never return a -ve number.  Simplest to implement by checking in getNumReserved() rather than using a cas in decPermit()
        // How many permits outstanding for a given flowSeq?
        inline unsigned getNumReserved(unsigned flowSeq) const
        {
            for (const SendPermit & permit : permits)
            {
                if (permit.flowSeq == flowSeq)
                    return permit.getNumReserved();
            }
            return 0;
        }

        //Total reservations outstanding for the sender
        inline unsigned getTotalReserved() const
        {
            unsigned total = 0;
            for (const SendPermit & permit : permits)
            {
                total += permit.getNumReserved();
            }
            return total;
        }

        inline bool hasActivePermit() const
        {
            return (numPermits != 0);
        }

        bool hasUnusedPermit() const
        {
            return (numPermits != MaxPermitsPerSender);
        }

        inline SendPermit * queryPermit(unsigned flowSeq)
        {
            for (SendPermit & permit : permits)
            {
                if (permit.flowSeq == flowSeq)
                    return &permit;
            }
            return nullptr;
        }

        inline SendPermit & allocatePermit(unsigned permitTime, unsigned num)
        {
            for (SendPermit & permit : permits)
            {
                if (!permit.isActive())
                {
                    numPermits++;
                    lastPermitTime = permitTime;
                    permit.grantPermit(flowSeq, sendSeq, num, permitTime);
                    return permit;
                }
            }
            throwUnexpected();
        }

        void revokePermit(SendPermit & permit)
        {
            permit.revokePermit();
            numPermits--;
        }

        inline void updateNumReserved(unsigned flowSeq, unsigned num)
        {
            for (SendPermit & permit : permits)
            {
                if (permit.flowSeq == flowSeq)
                {
                    permit.numPackets.store(num, std::memory_order_release);
                    return;
                }
            }
        }
        inline void decPermit(unsigned msgSeq)
        {
            if (lastDataPermit->isActive())
            {
                //If the message sequence is still larger than the lastDataPermit sequence, then the permit will not have been reallocated, so ok to decrement
                if ((int)(msgSeq - lastDataPermit->sendSeq) >= 0)
                {
                    lastDataPermit->numPackets.fetch_sub(1, std::memory_order_acq_rel);
                    return;
                }
            }

            //Although this is a bit more work than matching by flowSeq it shouldn't be too inefficient.
            SendPermit * bestPermit = nullptr;
            int bestDelta = INT_MAX;
            for (SendPermit & permit : permits)
            {
                if (permit.isActive())
                {
                    int delta =  (int)msgSeq - permit.sendSeq;
                    //Check if this message sequence could belong to this permit (sequence number is larger)
                    if (delta >= 0)
                    {
                        if (delta < bestDelta)
                        {
                            bestPermit = &permit;
                            bestDelta = delta;
                        }
                    }
                }
            }
            if (bestPermit)
            {
                bestPermit->numPackets.fetch_sub(1, std::memory_order_acq_rel);
                lastDataPermit = bestPermit;
            }
        }
    };

    using SenderList = LinkedListOf<UdpSenderEntry>;
    using PermitList = LinkedListOf<SendPermit>;

    IpMapOf<UdpSenderEntry> sendersTable;

    class UdpRdTracker : public TimeDivisionTracker<6, false>
    {
    public:
        enum
        {
            other,
            waiting,
            allocating,
            processing,
            pushing,
            checkingPending
        };

        UdpRdTracker(const char *name, unsigned reportIntervalSeconds) : TimeDivisionTracker<6, false>(name, reportIntervalSeconds)
        {
            stateNames[other] = "other";
            stateNames[waiting] = "waiting";
            stateNames[allocating] = "allocating";
            stateNames[processing] = "processing";
            stateNames[pushing] = "pushing";
            stateNames[checkingPending] = "checking pending";
        }

    };

    class receive_receive_flow : public Thread 
    {
        CReceiveManager &parent;
        Owned<ISocket> flow_socket;
        const unsigned flow_port;
        const unsigned maxSlotsPerSender;
        const unsigned maxPermits;                  // Must be provided in the constructor
        std::atomic<bool> running = { false };
        SenderList pendingRequests;     // List of senders requesting permission to send
        PermitList pendingPermits;      // List of active permits
        UdpRdTracker timeTracker;
    private:
        void noteRequest(UdpSenderEntry *requester, sequence_t flowSeq, sequence_t sendSeq)
        {
            //Check for a permit that is still live, if found it is likely to ok_to_send was lost.
            SendPermit * permit = requester->queryPermit(flowSeq);
            if (permit)
            {
                //if present resend the ok_to_send with the size that was granted
                unsigned slots = permit->getNumReserved();
                requester->sendPermitToSend(slots, myNode.getIpAddress());
                return;
            }

            //One of
            //a) A new request has arrived
            //b) The sender has restarted
            //   The receiver will eventually time out the old permits, and a new ok_to_send will be sent.
            //c) Messages have been received out of order (e.g. request_to_send_more after a request_to_send?)
            //   Almost impossible - it would need to be a very delayed resend.  The sender will ignore, and resend
            //   a new request_to_send if necessary.
            switch (requester->state)
            {
            case ReceiveState::granted:
            case ReceiveState::idle:
                pendingRequests.append(requester);
                requester->state = ReceiveState::requested;
                break;
            case ReceiveState::requested:
                // Perhaps the sender never saw our acknowledgement? Already on queue... resend an acknowledgement
                break;
            default:
                // Unexpected state, should never happen!
                ERRLOG("ERROR: Unexpected state %s in noteRequest", receiveStateName(requester->state));
                throwUnexpected();
                break;
            }
            requester->acknowledgeRequest(myNode.getIpAddress(), flowSeq, sendSeq);  // Acknowledge receipt of the request
        }

        void grantPermit(UdpSenderEntry *requester, unsigned slots)
        {
            //State must be 'requested' if it is on the pendingRequests list
            if (requester->state != ReceiveState::requested)
            {
                // Unexpected state, should never happen!
                ERRLOG("ERROR: Unexpected state %s in grantPermit", receiveStateName(requester->state));
                requester->state = ReceiveState::requested; // Allow the system to recover.....
                printStackReport();
            }

            pendingRequests.remove(requester);

            unsigned now = msTick();
            SendPermit & permit = requester->allocatePermit(now, slots);
            pendingPermits.append(&permit);
            requester->state = ReceiveState::granted;
            requester->requestTime = now;
            requester->sendPermitToSend(slots, myNode.getIpAddress());
        }

        void noteDone(UdpSenderEntry *requester, const UdpRequestToSendMsg &msg)
        {
            const unsigned flowSeq = msg.flowSeq;
            SendPermit * permit = requester->queryPermit(flowSeq);

            //A completed message, on the data flow, may often be received after the next request to send.
            //If so it should not update the state, but it should clear all grants with a flowid <= the new flowid
            //since all the data will have been sent. (If it has not been received it is either lost or OOO (unlikely).)
            for (SendPermit & permit : requester->permits)
            {
                if (permit.isActive() && ((int)(permit.flowSeq - flowSeq) <= 0))
                {
                    pendingPermits.remove(&permit);
                    requester->revokePermit(permit);
                }
            }

            //If it matches the current flowSeq, then we can assume everything is complete, otherwise leave the state as it is
            if (flowSeq != requester->flowSeq)
                return;

            switch (requester->state)
            {
            case ReceiveState::requested:
                // A bit unexpected but will happen if the permission timed out and the request was added to the requests queue
                pendingRequests.remove(requester);
                break;
            case ReceiveState::granted:
                break;
            case ReceiveState::idle:
                DBGLOG("Duplicate completed message received: msg %s flowSeq %" SEQF "u sendSeq %" SEQF "u. Ignoring", flowType::name(msg.cmd), msg.flowSeq, msg.sendSeq);
                break;
            default:
                // Unexpected state, should never happen! Ignore.
                ERRLOG("ERROR: Unexpected state %s in noteDone", receiveStateName(requester->state));
                break;
            }
            requester->state = ReceiveState::idle;
        }

    public:
        receive_receive_flow(CReceiveManager &_parent, unsigned flow_p, unsigned _maxSlotsPerSender)
        : Thread("UdpLib::receive_receive_flow"), parent(_parent), flow_port(flow_p), maxSlotsPerSender(_maxSlotsPerSender), maxPermits(_parent.input_queue_size),
          timeTracker("receive_receive_flow", 60)
        {
        }
        
        ~receive_receive_flow() 
        {
            if (running)
            {
                running = false;
                shutdownAndCloseNoThrow(flow_socket);
                join();
            }
        }

        virtual void start(bool inheritThreadContext) override
        {
            running = true;
            if (check_max_socket_read_buffer(udpFlowSocketsSize) < 0)
                throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
                if (udpTestUseUdpSockets)
                    flow_socket.setown(CSimulatedUdpReadSocket::udp_create(SocketEndpoint(flow_port, myNode.getIpAddress())));
                else
                    flow_socket.setown(CSimulatedQueueReadSocket::udp_create(SocketEndpoint(flow_port, myNode.getIpAddress())));
            else
#endif
                flow_socket.setown(ISocket::udp_create(flow_port));
            flow_socket->set_receive_buffer_size(udpFlowSocketsSize);
            size32_t actualSize = flow_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: receive_receive_flow created port=%d sockbuffsize=%d actual %d", flow_port, udpFlowSocketsSize, actualSize);
            Thread::start(inheritThreadContext);
        }

        void doFlowRequest(const UdpRequestToSendMsg &msg)
        {
            flowRequestsReceived++;
            if (udpTraceFlow)
            {
                StringBuffer ipStr;
                DBGLOG("UdpReceiver: received %s msg flowSeq %" SEQF "u sendSeq %" SEQF "u from node=%s", flowType::name(msg.cmd), msg.flowSeq, msg.sendSeq, msg.sourceNode.getTraceText(ipStr).str());
            }
            UdpSenderEntry *sender = &parent.sendersTable[msg.sourceNode];
            unsigned flowSeq = msg.flowSeq;
            switch (msg.cmd)
            {
            case flowType::request_to_send:
                noteRequest(sender, flowSeq, msg.sendSeq);
                break;

            case flowType::send_start:
                // Could potentially go up if the sender sends more missing packets than the receiver granted, or if
                // the permit has timed out.
                sender->updateNumReserved(msg.flowSeq, msg.packets);
                break;

            case flowType::send_completed:
                noteDone(sender, msg);
                break;

            case flowType::request_to_send_more:
            {
                noteDone(sender, msg);
                unsigned nextFlowSeq = std::max(flowSeq+1, 1U); // protect against a flowSeq of 0
                noteRequest(sender, nextFlowSeq, msg.sendSeq);
                break;
            }

            default:
                DBGLOG("UdpReceiver: received unrecognized flow control message cmd=%i", msg.cmd);
            }
        }

        unsigned checkPendingRequests()
        {
            unsigned timeout = 5000;   // The default timeout is 5 seconds if nothing is waiting for response...
            unsigned permitsIssued = 0;
            if (pendingPermits)
            {
                unsigned now = msTick();
                //First remove any expired permits (stored in expiry-order in the permit list)
                SendPermit *finger = pendingPermits;
                while (finger)
                {
                    unsigned elapsed = now - finger->permitTime;
                    if (elapsed >= udpPermitTimeout)
                    {
                        UdpSenderEntry * sender = finger->owner;
                        if (udpTraceFlow || udpTraceTimeouts)
                        {
                            StringBuffer s;
                            DBGLOG("permit %" SEQF "u to node %s (%u packets) timed out after %u ms, rescheduling", sender->flowSeq, sender->dest.getHostText(s).str(), sender->getTotalReserved(), elapsed);
                        }

                        SendPermit *next = finger->next;
                        pendingPermits.remove(finger);
                        sender->revokePermit(*finger);

                        if (++sender->timeouts > udpMaxPermitDeadTimeouts && udpMaxPermitDeadTimeouts != 0)
                        {
                            if (udpTraceFlow || udpTraceTimeouts)
                            {
                                StringBuffer s;
                                DBGLOG("permit to send %" SEQF "u to node %s timed out %u times - abandoning", sender->flowSeq, sender->dest.getHostText(s).str(), sender->timeouts);
                            }

                            //Currently this is benign.  If the sender really is alive it will send another request.
                            //Should this have a more significant effect and throw away any data that has been received from that sender??
                            //Only change the state if there are no other active permits.  Only the last request will be re-sent.
                            if (!sender->hasActivePermit() && (sender->state != ReceiveState::requested))
                                sender->state = ReceiveState::idle;
                        }
                        else if (sender->state != ReceiveState::requested)
                        {
                            // Put it back on the request queue (at the back) - even if there are other active permits
                            pendingRequests.append(sender);
                            sender->state = ReceiveState::requested;
                        }
                        finger = next;
                    }
                    else
                    {
                        timeout = udpPermitTimeout - elapsed;
                        break;
                    }
                }

                // Sum the number of reserved slots assigned to active permits
                while (finger)
                {
                    permitsIssued += finger->getNumReserved();
                    finger = finger->next;
                }
            }

            // Aim is to issue enough permits to use all available the space in the queue.  Adjust available by the
            // number already issued (avoid underflow if over-committed).
            unsigned slots = parent.input_queue->available();
            if (slots >= permitsIssued)
                slots -= permitsIssued;
            else
                slots = 0;

            bool anyCanSend = false;
            bool anyCannotSend = false;
            //Note, iterate if slots==0 so that timeout code is processed.
            for (UdpSenderEntry *finger = pendingRequests; finger != nullptr; finger = finger->next)
            {
                if (pendingPermits.length()>=udpMaxPendingPermits)
                    break;

                if (slots < udpMinSlotsPerSender)
                {
                    //The number of slots may increase if (a) data is read off the input queue, or (b) a send_start adjusts the number of permits
                    //(b) will result on a read on this thread so no need to adjust timeout.
                    //(a) requires some data to be read, so assume a tenth of the time to read all data
                    const unsigned udpWaitForSlotTimeout = std::max(updDataSendTimeout/10, 1U);
                    if (timeout > udpWaitForSlotTimeout)
                        timeout = udpWaitForSlotTimeout;   // Slots should free up very soon!
                    break;
                }

                // If requester would not be able to send me any (because of the ones in flight) then wait
                if (finger->canSendAny())
                {
                    //If multiple done messages are lost and an rts has been processed there may be no permits free
                    //
                    //The transfer will recover once the permit expires. Could consider expiring the oldest permit, but it
                    //is possible that the data is still in transit, and the done may be about to appear soon.
                    //Waiting is likely to be the better option
                    if (finger->hasUnusedPermit())
                    {
                        unsigned requestSlots = slots;
                        //If already 2 outstanding permits, grant a new permit for 0 slots to send any missing packets, but nothing else.
                        if (requestSlots>maxSlotsPerSender)
                            requestSlots = maxSlotsPerSender;
                        if (requestSlots > maxPermits-permitsIssued)
                            requestSlots = maxPermits-permitsIssued;
                        grantPermit(finger, requestSlots);
                        slots -= requestSlots;
                        anyCanSend = true;
                        if (timeout > udpPermitTimeout)
                            timeout = udpPermitTimeout;
                    }
                    else
                    {
                        //Sender has a request to send, but all permits are active - suggests a previous done has been lost/not received yet
                        //Do not set anyCannotSend - because a permit being freed will be triggered by a flow message - so no need to
                        //adjust the timeout.  (A different situation from waiting for a data packet to allow a sender to proceed.)
                        if (udpTraceFlow)
                        {
                            StringBuffer s;
                            DBGLOG("Sender %s can't be given permission to send yet as all permits active", finger->dest.getHostText(s).str());
                        }
                    }
                }
                else
                {
                    anyCannotSend = true;
                    if (udpTraceFlow)
                    {
                        StringBuffer s;
                        DBGLOG("Sender %s can't be given permission to send yet as resend buffer full", finger->dest.getHostText(s).str());
                    }
                }
            }

            if (anyCannotSend && !anyCanSend)
            {
                // A very unusual situation - all potential readers cannot send any extra packets because there are significant missing packets
                if (udpTraceFlow)
                {
                    StringBuffer s;
                    DBGLOG("All senders blocked by resend buffers");
                }

                //Hard to tell what should happen to the timeout - try again when the resend timeout will allow missing packets to be sent
                unsigned missingPacketTimeout = std::max(udpResendDelay, 1U);
                if (timeout > missingPacketTimeout)
                    timeout = missingPacketTimeout; // Hopefully one of the senders should unblock soon
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
            unsigned lastReport = 0;
            unsigned suppressed = 0;
            while (running)
            {
                try
                {
                    if (udpTraceFlow)
                    {
                        DBGLOG("UdpReceiver: wait_read(%u)", timeout);
                    }
                    UdpRdTracker::TimeDivision d(timeTracker, UdpRdTracker::waiting);
                    bool dataAvail = flow_socket->wait_read(timeout);
                    if (dataAvail)
                    {
                        const unsigned l = sizeof(msg);
                        unsigned int res ;
                        flow_socket->readtms(&msg, l, l, res, 0);
                        assert(res==l);
                        d.switchState(UdpRdTracker::processing);
                        doFlowRequest(msg);
                    }
                    d.switchState(UdpRdTracker::checkingPending);
                    timeout = checkPendingRequests();
                }
                catch (IException *e)
                {
                    if (running)
                    {
                        unsigned now = msTick();
                        if (lastReport-now < 60000)
                            suppressed++;
                        else
                        {
                            StringBuffer s;
                            DBGLOG("UdpReceiver: failed %u time(s) %i %s", suppressed+1, flow_port, e->errorMessage(s).str());
                            lastReport = now;
                            suppressed = 0;
                        }
                    }
                    e->Release();
                }
                catch (...)
                {
                    unsigned now = msTick();
                    if (lastReport-now < 60000)
                        suppressed++;
                    else
                    {
                        DBGLOG("UdpReceiver: receive_receive_flow::run unknown exception (%u failure(s) noted)", suppressed+1);
                        lastReport = now;
                        suppressed = 0;
                    }
                }
            }
            return 0;
        }

    };

    class receive_data : public Thread 
    {
        CReceiveManager &parent;
        ISocket *receive_socket = nullptr;
        ISocket *selfFlowSocket = nullptr;
        std::atomic<bool> running = { false };
        Semaphore started;
        UdpRdTracker timeTracker;
        
    public:
        receive_data(CReceiveManager &_parent) : Thread("UdpLib::receive_data"), parent(_parent), timeTracker("receive_data", 60)
        {
            unsigned ip_buffer = parent.input_queue_size*DATA_PAYLOAD*2;
            if (ip_buffer < udpFlowSocketsSize) ip_buffer = udpFlowSocketsSize;
            if (check_max_socket_read_buffer(ip_buffer) < 0) 
                throw MakeStringException(ROXIE_UDP_ERROR, "System socket max read buffer is less than %u", ip_buffer);
#ifdef SOCKET_SIMULATION
            if (isUdpTestMode)
            {
                if (udpTestUseUdpSockets)
                {
                    receive_socket = CSimulatedUdpReadSocket::udp_create(SocketEndpoint(parent.data_port, myNode.getIpAddress()));
                    selfFlowSocket = CSimulatedUdpWriteSocket::udp_connect(SocketEndpoint(parent.receive_flow_port, myNode.getIpAddress()));
                }
                else
                {
                    receive_socket = CSimulatedQueueReadSocket::udp_create(SocketEndpoint(parent.data_port, myNode.getIpAddress()));
                    selfFlowSocket = CSimulatedQueueWriteSocket::udp_connect(SocketEndpoint(parent.receive_flow_port, myNode.getIpAddress()));
                }
            }
            else
#endif
            {
                receive_socket = ISocket::udp_create(parent.data_port);
                selfFlowSocket = ISocket::udp_connect(SocketEndpoint(parent.receive_flow_port, myNode.getIpAddress()));
            }
            receive_socket->set_receive_buffer_size(ip_buffer);
            size32_t actualSize = receive_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: rcv_data_socket created port=%d requested sockbuffsize=%d actual sockbuffsize=%d", parent.data_port, ip_buffer, actualSize);
            running = false;
        }

        virtual void start(bool inheritThreadContext) override
        {
            running = true;
            Thread::start(inheritThreadContext);
            started.wait();
        }

        ~receive_data()
        {
            DBGLOG("Total data packets seen = %u OOO(%u) Requests(%u) Permits(%u)", dataPacketsReceived.load(), packetsOOO.load(), flowRequestsReceived.load(), flowRequestsSent.load());

            running = false;
            shutdownAndCloseNoThrow(receive_socket);
            shutdownAndCloseNoThrow(selfFlowSocket);
            join();
            ::Release(receive_socket);
            ::Release(selfFlowSocket);
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
            unsigned lastOOOReport = msTick();
            unsigned lastPacketsOOO = 0;
            unsigned lastUnwantedDiscarded = 0;
            unsigned timeout = 5000;
            roxiemem::IDataBufferManager * udpBufferManager = bufferManager;
            DataBuffer *b = udpBufferManager->allocate();
            while (running) 
            {
                try 
                {
                    unsigned int res;
                    UdpPacketHeader &hdr = *(UdpPacketHeader *) b->data;
                    while (true)
                    {
                        //Read at least the size of the smallest packet we can receive
                        //static assert to check we are reading the smaller of the two possible packet types
                        static_assert(sizeof(UdpRequestToSendMsg) <= sizeof(UdpPacketHeader));
                        {
                            UdpRdTracker::TimeDivision d(timeTracker, UdpRdTracker::waiting);
                            receive_socket->readtms(b->data, sizeof(UdpRequestToSendMsg), DATA_PAYLOAD, res, timeout);
                        }
                        //Even if a UDP packet is not split, very occasionally only some of the data may be present for the read.
                        //Slightly horribly this packet could be one of two different formats(!)
                        //  a UdpRequestToSendMsg, which has a 2 byte command at the start of the header, with a maximum value of max_flow_cmd
                        //  a UdpPacketHeader which has a 2 byte length.  This length must be > sizeof(UdpPacketHeader).
                        //Since max_flow_cmd < sizeof(UdpPacketHeader) this can be used to distinguish a true data packet(!)
                        static_assert(flowType::max_flow_cmd < sizeof(UdpPacketHeader)); // assert to check the above comment is correct

                        if (hdr.length >= sizeof(UdpPacketHeader))
                        {
                            if (res == hdr.length)
                                break;

                            //Very rare situation - log it so that there is some evidence that it is occurring
                            OWARNLOG("Received partial network packet - %u bytes out of %u received", res, hdr.length);

                            //Because we are reading UDP datgrams rather than tcp packets, if we failed to read the whole datagram
                            //the rest of the datgram is lost - you cannot call readtms to read the rest of the datagram.
                            //Therefore throw this incomplete datagram away and allow the resend mechanism to retransmit it.
                            continue;
                        }

                        //Sanity check
                        assertex(res == sizeof(UdpRequestToSendMsg));

                        //Sending flow packets (eg send_completed) to the data thread ensures they do not overtake the data
                        //Redirect them to the flow thread to process them.
                        selfFlowSocket->write(b->data, res);
                    }
                    {
                        UdpRdTracker::TimeDivision d(timeTracker, UdpRdTracker::processing);
                        dataPacketsReceived++;
                        UdpSenderEntry *sender = &parent.sendersTable[hdr.node];
                        if (sender->noteSeen(hdr))
                        {
                            // We should perhaps track how often this happens, but it's not the same as unwantedDiscarded
                            hdr.node.clear();  // Used to indicate a duplicate that collate thread should discard. We don't discard on this thread as don't want to do anything that requires locks...
                        }
                        else
                        {
                            //Decrease the number of active reservations to balance having received a new data packet (otherwise they will be double counted)
                            sender->decPermit(hdr.msgSeq);
                            if (udpTraceLevel > 5) // don't want to interrupt this thread if we can help it
                            {
                                StringBuffer s;
                                DBGLOG("UdpReceiver: %u bytes received packet %" SEQF "u %x from %s", res, hdr.sendSeq, hdr.pktSeq, hdr.node.getTraceText(s).str());
                            }
                        }
                        d.switchState(UdpRdTracker::pushing);
                        parent.input_queue->pushOwn(b);
                        d.switchState(UdpRdTracker::allocating);
                        b = udpBufferManager->allocate();
                    }
                    if (udpStatsReportInterval)
                    {
                        unsigned now = msTick();
                        if (now-lastOOOReport > udpStatsReportInterval)
                        {
                            lastOOOReport = now;
                            if (unwantedDiscarded > lastUnwantedDiscarded)
                            {
                                DBGLOG("%u more unwanted packets discarded by this server (%u total)", unwantedDiscarded - lastUnwantedDiscarded, unwantedDiscarded-0);
                                lastUnwantedDiscarded = unwantedDiscarded;
                            }
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
    receive_receive_flow *receive_flow;
    receive_data         *data;
    
    const int             input_queue_size;
    const int             receive_flow_port;
    const int             data_port;

    std::atomic<bool> running = { false };
    bool encrypted = false;

    typedef std::map<ruid_t, CMessageCollator*> uid_map;
    uid_map         collators;
    CriticalSection collatorsLock; // protects access to collators map

public:
    IMPLEMENT_IINTERFACE;
    CReceiveManager(int server_flow_port, int d_port, int client_flow_port, int queue_size, bool _encrypted)
        : collatorThread(*this), encrypted(_encrypted),
        sendersTable([client_flow_port](const ServerIdentifier ip) { return new UdpSenderEntry(ip.getIpAddress(), client_flow_port);}),
        input_queue_size(queue_size), receive_flow_port(server_flow_port), data_port(d_port)
    {
#ifndef _WIN32
        if (udpAdjustThreadPriorities)
            setpriority(PRIO_PROCESS, 0, -15);
#endif
        assertex(data_port != receive_flow_port);
        input_queue = new queue_t(queue_size);
        data = new receive_data(*this);

        //NOTE: If all slots are allocated to a single client, then if that server goes down it will prevent any data being received from
        //any other sender for the udpPermitTimeout period
        unsigned maxSlotsPerClient = (udpMaxPendingPermits == 1) ? queue_size : (udpMaxClientPercent * queue_size) / (udpMaxPendingPermits * 100);
        assertex(maxSlotsPerClient != 0);
        if (maxSlotsPerClient > queue_size)
            maxSlotsPerClient = queue_size;
        if (udpResendLostPackets && maxSlotsPerClient > TRACKER_BITS)
            maxSlotsPerClient = TRACKER_BITS;
        receive_flow = new receive_receive_flow(*this, server_flow_port, maxSlotsPerClient);

        running = true;
        collatorThread.start(false);
        data->start(false);
        receive_flow->start(false);
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
        {
            CriticalBlock b(collatorsLock);
            collators.erase(ruid);
        }
        msgColl->Release();
    }

    void collatePackets()
    {
        UdpRdTracker timeTracker("collatePackets", 60);
        while(running) 
        {
            try
            {
                UdpRdTracker::TimeDivision d(timeTracker, UdpRdTracker::waiting);
                DataBuffer *dataBuff = input_queue->pop(true);
                d.switchState(UdpRdTracker::processing);    
                dataBuff->changeState(roxiemem::DBState::queued, roxiemem::DBState::unowned, __func__);
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
        Linked <CMessageCollator> msgColl;
        bool isDefault = false;
        {
            try
            {
                CriticalBlock b(collatorsLock);
                msgColl.set(collators[pktHdr->ruid]);
                if (!msgColl)
                {
                    // We only send single-packet messages to the default collator
                    if ((pktHdr->pktSeq & UDP_PACKET_COMPLETE) != 0 && (pktHdr->pktSeq & UDP_PACKET_SEQUENCE_MASK) == 0)
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
        if (udpTraceLevel>=5 && isDefault && !isUdpTestMode)
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
        {
            CriticalBlock b(collatorsLock);
            collators[ruid] = msgColl;
        }
        msgColl->Link();
        return msgColl;
    }
};

IReceiveManager *createReceiveManager(int server_flow_port, int data_port, int client_flow_port,
                                      int udpQueueSize, bool encrypted)
{
    return new CReceiveManager(server_flow_port, data_port, client_flow_port, udpQueueSize, encrypted);
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
