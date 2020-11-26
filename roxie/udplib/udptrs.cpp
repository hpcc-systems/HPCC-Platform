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
bool udpSnifferEnabled = false;

using roxiemem::DataBuffer;
// MORE - why use DataBuffers on output side?? We could use zeroCopy techniques if we had a dedicated memory area.
// But using them on this side means we guarantee that the packets fit into databuffers on the other side... But so would matching their size

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
 * 1. Two agent threads add data at same time - only one should sent rts (use atomic_inc for the count)
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

class UdpReceiverEntry : public IUdpReceiverEntry
{
private:
    queue_t *output_queue = nullptr;
    bool    initialized = false;
    const bool isLocal = false;
    ISocket *send_flow_socket = nullptr;
    ISocket *data_socket = nullptr;
    const unsigned numQueues;
    int     current_q = 0;
    int     currentQNumPkts = 0;         // Current Queue Number of Consecutive Processed Packets.
    int     *maxPktsPerQ = nullptr;      // to minimise power function re-calc for every packet

    void sendRequest(flowType::flowCmd cmd, unsigned packets )
    {
        UdpRequestToSendMsg msg = { cmd, static_cast<unsigned short>(packets), sourceIP };
        try
        {
            if (udpTraceLevel > 3)
            {
                StringBuffer s;
                DBGLOG("UdpSender: sending flowType::%s msg to node=%s", flowType::name(cmd), ip.getIpText(s).str());
            }
            send_flow_socket->write(&msg, sizeof(UdpRequestToSendMsg));
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
public:
    const IpAddress ip;
    unsigned timeouts = 0;      // Number of consecutive timeouts
    unsigned requestExpiryTime = 0;

    static bool comparePacket(const void *pkData, const void *key)
    {
        UdpPacketHeader *dataHdr = (UdpPacketHeader*) ((DataBuffer*)pkData)->data;
        UdpPacketHeader *keyHdr = (UdpPacketHeader*) key;
        return ( (dataHdr->ruid == keyHdr->ruid) && (dataHdr->msgId == keyHdr->msgId) );
    }

    std::atomic<unsigned> packetsQueued = { 0 };

    void sendDone(unsigned packets)
    {
        bool dataRemaining = packetsQueued.load(std::memory_order_relaxed);
        // If dataRemaining says 0, but someone adds a row in this window, the request_to_send will be sent BEFORE the send_completed
        // So long as receiver handles that, are we good?
        if (dataRemaining)
        {
            requestExpiryTime = msTick() + udpRequestToSendAckTimeout;
            sendRequest(flowType::request_to_send_more, packets);
        }
        else
        {
            requestExpiryTime = 0;
            sendRequest(flowType::send_completed, packets);
        }
        timeouts = 0;
    }

    void requestToSend()
    {
        requestExpiryTime = msTick() + udpRequestToSendAckTimeout;
        sendRequest(flowType::request_to_send, 0);
    }

    void requestAcknowledged()
    {
        if (requestExpiryTime)
            requestExpiryTime = msTick() + udpRequestToSendTimeout;
    }

    // MORE - consider where/if we need critsecs in here!

    unsigned sendData(const UdpPermitToSendMsg &permit, TokenBucket *bucket)
    {
        requestExpiryTime = 0;
        unsigned maxPackets = permit.max_data;
        std::vector<DataBuffer *> toSend;
        unsigned totalSent = 0;
        while (toSend.size() < maxPackets && packetsQueued.load(std::memory_order_relaxed))
        {
            DataBuffer *buffer = popQueuedData();
            if (!buffer)
                break;  // Suggests data was aborted before we got to pop it
            UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
            toSend.push_back(buffer);
            totalSent += header->length;
#if defined(__linux__) || defined(__APPLE__)
            if (isLocal && (totalSent> 100000))  // Avoids sending too fast to local node, for reasons lost in the mists of time
                break;
#endif
        }
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
                data_socket->write(buffer->data, length);
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
        output_queue[queue].pushOwn(buffer);
        if (!packetsQueued++)
            requestToSend();
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

    UdpReceiverEntry(const IpAddress &_ip, const IpAddress &_sourceIP, unsigned _numQueues, unsigned _queueSize, unsigned _sendFlowPort, unsigned _dataPort)
    : ip (_ip), sourceIP(_sourceIP), numQueues(_numQueues), isLocal(_ip.isLocal())
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
    }

    ~UdpReceiverEntry()
    {
        if (send_flow_socket) send_flow_socket->Release();
        if (data_socket) data_socket->Release();
        if (output_queue) delete [] output_queue;
        if (maxPktsPerQ) delete [] maxPktsPerQ;
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
                    unsigned expireTime = dest.requestExpiryTime;
                    if (expireTime)
                    {
                        if (expireTime <= now)
                        {
                            dest.timeouts++;
                            {
                                StringBuffer s;
                                EXCLOG(MCoperatorError,"ERROR: UdpSender: timed out %i times (max=%i) waiting ok_to_send msg from node=%s",
                                        dest.timeouts, udpMaxRetryTimedoutReqs, dest.ip.getIpText(s).str());
                            }
                            // 0 (zero) value of udpMaxRetryTimedoutReqs means NO limit on retries
                            if (udpMaxRetryTimedoutReqs && (dest.timeouts >= udpMaxRetryTimedoutReqs))
                                dest.abort();
                            else
                                dest.requestToSend();
                        }
                        else if (expireTime-now < timeout)
                            timeout = expireTime-now;
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
                while (running) 
                {
                    try 
                    {
                        unsigned int res ;
                        flow_socket->read(&f, sizeof(f), sizeof(f), res, 5);
                        assert(res==sizeof(f));
                        switch (f.cmd)
                        {
                        case flowType::ok_to_send:
                            if (udpTraceLevel > 1) 
                            {
                                StringBuffer s;
                                DBGLOG("UdpSender: received ok_to_send msg max %d packets from node=%s", f.max_data, f.destNode.getTraceText(s).str());
                            }
                            parent.data->ok_to_send(f);
                            break;

                        case flowType::request_received:
                            if (udpTraceLevel > 1)
                            {
                                StringBuffer s;
                                DBGLOG("UdpSender: received request_received msg from node=%s", f.destNode.getTraceText(s).str());
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
        ISocket     *sniffer_socket;
        SocketEndpoint ep;
        simple_queue<UdpPermitToSendMsg> send_queue;
        Linked<TokenBucket> bucket;

        void send_sniff(sniffType::sniffCmd busy)
        {
            sniff_msg msg = { busy, parent.myIP};
            try 
            {
                if (!sniffer_socket) 
                {
                    sniffer_socket = ISocket::multicast_connect(ep, multicastTTL);
                    if (udpTraceLevel > 1)
                    {
                        StringBuffer url;
                        DBGLOG("UdpSender: multicast_connect ok to %s", ep.getUrlStr(url).str());
                    }
                }
                sniffer_socket->write(&msg, sizeof(msg));
                if (udpTraceLevel > 1)
                    DBGLOG("UdpSender: sent busy=%d multicast msg", busy);
            }
            catch(IException *e) 
            {
                StringBuffer s;
                StringBuffer url;
                DBGLOG("UdpSender: multicast_connect or write failed ep=%s - %s", ep.getUrlStr(url).str(), e->errorMessage(s).str());
                e->Release();
            }
            catch(...) 
            {
                StringBuffer url;
                DBGLOG("UdpSender: multicast_connect or write unknown exception - ep=%s", ep.getUrlStr(url).str());
                if (sniffer_socket) 
                {
                    sniffer_socket->Release();
                    sniffer_socket = NULL;
                }
            }
        }

    public:
        send_data(CSendManager &_parent, int s_port, const IpAddress &snif_ip, TokenBucket *_bucket)
            : StartedThread("UdpLib::send_data"), parent(_parent), bucket(_bucket), ep(s_port, snif_ip), send_queue(100) // MORE - send q size should be configurable and/or related to size of cluster?
        {
            sniffer_socket = NULL;
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
            if (sniffer_socket) 
                sniffer_socket->Release();
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

                if (udpSnifferEnabled)
                    send_sniff(sniffType::busy);
                UdpReceiverEntry &receiverInfo = parent.receiversTable[permit.destNode];
                unsigned payload = receiverInfo.sendData(permit, bucket);
                if (udpSnifferEnabled)
                    send_sniff(sniffType::idle);
                
                if (udpTraceLevel > 1) 
                {
                    StringBuffer s;
                    DBGLOG("UdpSender: sent %u bytes to node=%s", payload, permit.destNode.getTraceText(s).str());
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

    IpMapOf<UdpReceiverEntry> receiversTable;
    send_resend_flow  *resend_flow;
    send_receive_flow *receive_flow;
    send_data         *data;
    Linked<TokenBucket> bucket;
    IpAddress myIP;
    
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

    CSendManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int q_size, int _numQueues, const IpAddress &_myIP, TokenBucket *_bucket)
        : bucket(_bucket),
          myIP(_myIP),
          receiversTable([_myIP, _numQueues, q_size, server_flow_port, data_port](const ServerIdentifier &ip) { return new UdpReceiverEntry(ip.getIpAddress(), _myIP, _numQueues, q_size, server_flow_port, data_port);})
    {
#ifndef _WIN32
        setpriority(PRIO_PROCESS, 0, -3);
#endif
        numQueues = _numQueues;
        data = new send_data(*this, sniffer_port, sniffer_multicast_ip, bucket);
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
        static_cast<UdpReceiverEntry &>(receiver).pushData(queue, buffer);
    }

    virtual IMessagePacker *createMessagePacker(ruid_t ruid, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue) override
    {
        return ::createMessagePacker(ruid, sequence, messageHeader, headerSize, *this, receiversTable[destNode], myIP, getNextMessageSequence(), queue);
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

ISendManager *createSendManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size_pr_server, int queues_pr_server, TokenBucket *rateLimiter)
{
    assertex(!myNode.getIpAddress().isNull());
    return new CSendManager(server_flow_port, data_port, client_flow_port, sniffer_port, sniffer_multicast_ip, queue_size_pr_server, queues_pr_server, myNode.getIpAddress(), rateLimiter);
}

class CMessagePacker : implements IMessagePacker, public CInterface
{
    ISendManager   &parent;
    IUdpReceiverEntry &receiver;
    UdpPacketHeader package_header;
    DataBuffer     *part_buffer;
    unsigned data_buffer_size;
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

    CMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue)
        : parent(_parent), receiver(_receiver)
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
        data_buffer_size = DATA_PAYLOAD - sizeof(UdpPacketHeader);
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
        if (DATA_PAYLOAD - sizeof(UdpPacketHeader) < len)
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
            data_buffer_size = DATA_PAYLOAD - sizeof(UdpPacketHeader);
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
                    data_buffer_size = DATA_PAYLOAD - sizeof(UdpPacketHeader);
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
            unsigned maxMetaLength = DATA_PAYLOAD - (sizeof(UdpPacketHeader) + data_used);
            while (metaLength > maxMetaLength)
            {
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, maxMetaLength);
                put_package(part_buffer, data_used, maxMetaLength);
                metaLength -= maxMetaLength;
                metaData += maxMetaLength;
                data_used = 0;
                maxMetaLength = DATA_PAYLOAD - sizeof(UdpPacketHeader);
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
        data_buffer_size = 0;
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


extern UDPLIB_API IMessagePacker *createMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue)
{
    return new CMessagePacker(ruid, msgId, messageHeader, headerSize, _parent, _receiver, _sourceNode, _msgSeq, _queue);
}
