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

#undef new
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

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

using roxiemem::DataBuffer;
using roxiemem::IRowManager;

unsigned udpRetryBusySenders = 0; // seems faster with 0 than 1 in my testing on small clusters and sustained throughput
unsigned udpInlineCollationPacketLimit;
bool udpInlineCollation = false;
bool udpSendCompletedInData = false;

class CReceiveManager : public CInterface, implements IReceiveManager
{
    class ReceiveFlowManager : public Thread 
    {
    private:    
        CReceiveManager &parent;

        class UdpSenderEntry  // one per node in the system
        {
            SpinLock resendInfoLock;
            unsigned lastSeen;      // Highest sequence ever seen
            unsigned missingCount;  // Number of values in missing table
            unsigned missingIndex;  // Pointer to start of values in missing circular buffer
            unsigned missingTableSize;
            unsigned *missing;
            unsigned destNodeIndex;
            unsigned myNodeIndex;
            ISocket *flowSocket;

        public:
            unsigned nextIndex;     // Used to form list of all senders that have outstanding requests
            
            UdpSenderEntry() 
            {
                nextIndex = (unsigned) -1;
                flowSocket = NULL;
                lastSeen = 0;
                missingCount = 0;
                missingIndex = 0;
                missingTableSize = 0;
                missing = NULL;
                destNodeIndex = (unsigned) -1;
                myNodeIndex = (unsigned) -1;
            }

            ~UdpSenderEntry() 
            {
                if (flowSocket) 
                {
                    flowSocket->close();
                    flowSocket->Release();
                }
                delete [] missing;
            }

            void init(unsigned _destNodeIndex, unsigned _myNodeIndex, unsigned port, unsigned _missingTableSize)
            {
                assert(!flowSocket);
                destNodeIndex = _destNodeIndex;
                myNodeIndex = _myNodeIndex;
                SocketEndpoint ep(port, getNodeAddress(destNodeIndex));
                flowSocket = ISocket::udp_connect(ep);
                missingTableSize = _missingTableSize;
                missing = new unsigned[_missingTableSize];
            }

            void noteRequest(const UdpRequestToSendMsg &msg)
            {
                // MORE - if we never go idle, we never get to see these in udpSendCompletedInData mode. Is that a big deal?
                SpinBlock b(resendInfoLock);
                if (msg.lastSequenceAvailable < lastSeen)
                {
                    // must have wrapped or restarted. Reinitialize my view of sender missing packets.
                    // MORE - if a restart (rather than a wrap) should also clear out any collators still hanging on for a reply
                    // Should I on a wrap too? 
                    if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                        DBGLOG("Resetting missing list as sender %u seems to have restarted or wrapped", msg.sourceNodeIndex); 
                    lastSeen = 0;
                    missingCount = 0;
                    missingIndex = 0;
                }
                else
                {
                    // abandon hope of receiving any that sender says they no longer have
                    while (missingCount && missing[missingIndex] < msg.firstSequenceAvailable)
                    {
                        if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                            DBGLOG("Abandoning missing message %u from sender %u - sender no longer has it", missing[missingIndex], msg.sourceNodeIndex); 
                        atomic_inc(&packetsAbandoned);
                        missingCount--;
                        missingIndex++;
                        if (missingIndex == missingTableSize)
                            missingIndex = 0;
                    }
                }
            }

            void noteReceived(const UdpPacketHeader &msg)
            {
                // MORE - what happens when we wrap?
                // MORE - what happens when a sender restarts? Probably similar to wrap...
                // Receiver restart is ok
                SpinBlock b(resendInfoLock);
                unsigned thisSequence = msg.udpSequence;
                if (thisSequence > lastSeen)
                {
                    lastSeen++;
                    if (lastSeen != thisSequence)
                    {
                        // Everything between lastSeen and just received needs adding to missing table.
                        if (thisSequence - lastSeen > missingTableSize)
                        {
                            if (lastSeen==1)
                            {
                                // assume it's receiver restart and just ignore the missing values
                                DBGLOG("Assuming receiver restart (lastseen==%u, thisSequence==%u, index==%u)", lastSeen, thisSequence, msg.nodeIndex); 
                                lastSeen = thisSequence;
                                return;
                            }
                            if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                                DBGLOG("Big gap in UDP packet sequence (%u-%u) from node %u - some ignored!", lastSeen, thisSequence-1, msg.nodeIndex); 
                            lastSeen = thisSequence - missingTableSize; // avoids wasting CPU cycles writing more values than will fit after long interruption, receiver restart, or on corrupt packet
                        }
                        while (lastSeen < thisSequence)
                        {
                            if (checkTraceLevel(TRACE_RETRY_DATA, 3))
                                DBGLOG("Adding packet sequence %u to missing list for node %u", lastSeen, msg.nodeIndex); 
                            missing[(missingIndex + missingCount) % missingTableSize] = lastSeen;
                            if (missingCount < missingTableSize)
                                missingCount++;
                            else
                            {
                                if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                                    DBGLOG("missing table overflow - packets will be lost"); 
                            }
                            lastSeen++;
                        }
                    }
                }
                else 
                {
                    if (checkTraceLevel(TRACE_RETRY_DATA, 2))
                        DBGLOG("Out-of-sequence packet received %u", thisSequence); 
                    // Hopefully filling in a missing value, and USUALLY in sequence so no point binchopping
                    for (unsigned i = 0; i < missingCount; i++)
                    {
                        unsigned idx = (missingIndex + i) % missingTableSize;
                        if (missing[idx] == thisSequence)
                        {
                            if (checkTraceLevel(TRACE_RETRY_DATA, 2))
                                DBGLOG("Formerly missing packet sequence received %u", thisSequence); 
                            while (i)
                            {
                                // copy up ones that are still missing
                                unsigned idx2 = idx ? idx-1 : missingTableSize-1;
                                missing[idx] = missing[idx2];
                                idx = idx2;
                                i--;
                            }
                            missingIndex++;
                            missingCount--;
                            break;
                        }
                        else if (missing[idx] < thisSequence)
                        {
                            if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                                DBGLOG("Unexpected packet sequence received %u - ignored", thisSequence); 
                            break;
                        }
                        else // (missing[idx] > thisSequence)
                        {
                            if (!i)
                            {
                                if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                                    DBGLOG("Unrequested missing packet received %u - ignored", thisSequence); 
                                break;
                            }
                        }
                    }
                }
            }

            void requestToSend(unsigned maxTransfer)
            {
                try
                {
                    UdpPermitToSendMsg msg;
                    {
                        SpinBlock block(resendInfoLock);
                        msg.hdr.length = sizeof(UdpPermitToSendMsg::MsgHeader) + missingCount*sizeof(msg.missingSequences[0]);
                        msg.hdr.cmd = flow_t::ok_to_send;

                        msg.hdr.destNodeIndex = myNodeIndex;
                        msg.hdr.max_data = maxTransfer;
                        msg.hdr.lastSequenceSeen = lastSeen;
                        msg.hdr.missingCount = missingCount;
                        for (unsigned i = 0; i < missingCount; i++)
                        {
                            unsigned idx = (missingIndex + i) % missingTableSize;
                            msg.missingSequences[i] = missing[idx];
                            if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                                DBGLOG("Requesting resend of packet %d", missing[idx]);
                        }
#ifdef CRC_MESSAGES
                        msg.hdr.crc = msg.calcCRC();
#endif
                    }
                    if (checkTraceLevel(TRACE_RETRY_DATA, 5))
                    {
                        StringBuffer s;
                        DBGLOG("requestToSend %s", msg.toString(s).str());
                    }
                    flowSocket->write(&msg, msg.hdr.length);
                }
                catch(IException *e) 
                {
                    StringBuffer s;
                    DBGLOG("UdpReceiver: send_acknowledge failed node=%u %s", destNodeIndex, e->errorMessage(s).toCharArray());
                    e->Release();
                }
            }

        } *sendersTable;

        unsigned maxSenders;
        unsigned firstRequest;
        unsigned lastRequest;
        unsigned maxSlotsPerSender;
        bool running;

        SpinLock receiveFlowLock;  // Protecting the currentTransfer variable and the chain of active transfers

        unsigned currentTransfer;
        Semaphore requestPending;
        Semaphore transferComplete;

    public:     
        ReceiveFlowManager(CReceiveManager &_parent, unsigned _maxSenders, unsigned _maxSlotsPerSender)
         : Thread("UdpLib::ReceiveFlowManager"), parent(_parent)
        {
            firstRequest = (unsigned) -1;
            lastRequest = (unsigned) -1;
            currentTransfer = (unsigned) -1;
            running = false;
            maxSenders = _maxSenders;
            maxSlotsPerSender = _maxSlotsPerSender;
            sendersTable = new UdpSenderEntry[maxSenders];
            unsigned missingTableSize = maxSlotsPerSender;
            if (missingTableSize > MAX_RESEND_TABLE_SIZE)
                missingTableSize = MAX_RESEND_TABLE_SIZE;
            for (unsigned i = 0; i < maxSenders; i++)
            {
                sendersTable[i].init(i, parent.myNodeIndex, parent.send_flow_port, missingTableSize);
            }
        }

        ~ReceiveFlowManager() 
        {
            running = false;
            requestPending.signal();
            transferComplete.signal();
            join();
            delete [] sendersTable;
        }

        unsigned send_acknowledge() 
        {
            int timeout = 1;
            unsigned max_transfer;
            UdpSenderEntry *sender = NULL;
            {
                SpinBlock b(receiveFlowLock);
                if (firstRequest != (unsigned) -1) 
                {
                    assert(firstRequest < maxSenders);

                    //find first non-busy sender, and move it to front of sendersTable request chain
                    int retry = udpRetryBusySenders; 
                    unsigned finger = firstRequest;
                    unsigned prev = -1;
                    loop
                    {
                        if (udpSnifferEnabled && parent.sniffer->is_busy(finger))
                        {
                            prev = finger;
                            finger = sendersTable[finger].nextIndex;
                            if (finger==(unsigned)-1)
                            {
                                if (retry--)
                                {
                                    if (udpTraceLevel > 4)
                                        DBGLOG("UdpReceive: All senders busy");
                                    MilliSleep(1);
                                    finger = firstRequest;
                                    prev = -1;
                                }
                                else
                                    break; // give up and use first anyway
                            }
                        }
                        else
                        {
                            if (finger != firstRequest) 
                            {
                                if (finger == lastRequest) 
                                    lastRequest = prev;
                                assert(prev != -1);
                                sendersTable[prev].nextIndex = sendersTable[finger].nextIndex;
                                sendersTable[finger].nextIndex = firstRequest;
                                firstRequest = finger;
                            }
                            break;
                        }
                    }
                
                    if (udpInlineCollation)
                        max_transfer = udpInlineCollationPacketLimit;
                    else
                        max_transfer = parent.input_queue->free_slots();
                    if (max_transfer > maxSlotsPerSender) 
                        max_transfer = maxSlotsPerSender;
                    timeout = ((max_transfer * DATA_PAYLOAD) / 100) + 10; // in ms assuming mtu package size with 100x margin on 100 Mbit network // MORE - hideous!

                    currentTransfer = firstRequest;
                    sender = &sendersTable[firstRequest];
                     //indicate not in queue (MORE - what if wanted to send > allowing?? Do we know how much it wanted to send?)
                    if (firstRequest==lastRequest)
                        lastRequest = (unsigned) -1;
                    firstRequest = sender->nextIndex;
                    sender->nextIndex = (unsigned) -1;
                }
            }
            if (sender)
                sender->requestToSend(max_transfer);
            return timeout;
        }

        void request(const UdpRequestToSendMsg &msg)
        {
            unsigned index = msg.sourceNodeIndex;
            assertex(index < maxSenders);
            UdpSenderEntry &sender = sendersTable[index];
            {
                SpinBlock b(receiveFlowLock);
                if ((lastRequest == index) || (sender.nextIndex != (unsigned) -1))
                {
                    DBGLOG("UdpReceiver: received duplicate request_to_send msg from node=%d", index);
                    return;
                }
                // Chain it onto list
                if (firstRequest != (unsigned) -1) 
                    sendersTable[lastRequest].nextIndex = index;
                else 
                    firstRequest = index;
                lastRequest = index;
            }   
            sender.noteRequest(msg);
            requestPending.signal();
        }

        void requestMore(unsigned index) // simpler version of request since we don't get the extra info
        {
            assertex(index < maxSenders);
            UdpSenderEntry &sender = sendersTable[index];
            {
                SpinBlock b(receiveFlowLock);
                if ((lastRequest == index) || (sender.nextIndex != (unsigned) -1))
                {
                    DBGLOG("UdpReceiver: received duplicate request_to_send msg from node=%d", index);
                    return;
                }
                // Chain it onto list
                if (firstRequest != (unsigned) -1) 
                    sendersTable[lastRequest].nextIndex = index;
                else 
                    firstRequest = index;
                lastRequest = index;
            }   
            requestPending.signal();
        }

        void completed(unsigned index) 
        {
            assert(index < maxSenders);
            bool isCurrent;
            {
                SpinBlock b(receiveFlowLock);
                isCurrent = (index == currentTransfer);
            }
            if (isCurrent)
                transferComplete.signal();
            else 
                DBGLOG("UdpReceiver: completed msg from node %u is not for current transfer (%u) ", index, currentTransfer);
        }

        inline void noteReceived(const UdpPacketHeader &msg)
        {
            sendersTable[msg.nodeIndex].noteReceived(msg);
        }

        virtual void start()
        {
            running = true;
            Thread::start();
        }

        virtual int run() 
        {
            DBGLOG("UdpReceiver: ReceiveFlowManager started");
            while (running)
            {
                requestPending.wait();
                unsigned maxTime = send_acknowledge();
                if (!transferComplete.wait(maxTime) && udpTraceLevel > 0)
                {
                    DBGLOG("UdpReceiver: transfer timed out after %d ms from node=%u", maxTime, currentTransfer);
                    // MORE - a timeout here means everything stalled... look into when it can happen!
                }

            }
            return 0;
        }
    };

    class receive_sniffer : public Thread
    {
        struct SnifferEntry {
            time_t          timeStamp;
            char            busy;
            SnifferEntry() { timeStamp = 0; busy = 0; }
        } *snifferTable;
        
        ISocket     *sniffer_socket;
        unsigned snifferPort;
        IpAddress snifferIP;
        CReceiveManager &parent;
        bool        running;
        
        inline void update(unsigned index, char busy)
        {
            if (udpTraceLevel > 5)
                DBGLOG("UdpReceive: sniffer sets is_busy[%d} to %d", index, busy);
            snifferTable[index].busy = busy;
            if (busy) 
                time(&snifferTable[index].timeStamp);
        }

    public:
        receive_sniffer(CReceiveManager &_parent, unsigned _snifferPort, const IpAddress &_snifferIP, unsigned numNodes)
          : Thread("udplib::receive_sniffer"), parent(_parent), snifferPort(_snifferPort), snifferIP(_snifferIP), running(false)
        {
            snifferTable = new SnifferEntry[numNodes];
            sniffer_socket = ISocket::multicast_create(snifferPort, snifferIP);
            if (check_max_socket_read_buffer(udpFlowSocketsSize) < 0) {
                if (!enableSocketMaxSetting)
                    throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
                check_set_max_socket_read_buffer(udpFlowSocketsSize);
            }
            sniffer_socket->set_receive_buffer_size(udpFlowSocketsSize);
            if (udpTraceLevel)
            {
                StringBuffer ipStr;
                snifferIP.getIpText(ipStr);
                DBGLOG("UdpReceiver: receive_sniffer port open %s:%i", ipStr.str(), snifferPort);
            }
        }

        ~receive_sniffer() 
        {
            running = false;
            if (sniffer_socket) sniffer_socket->close();
            join();
            if (sniffer_socket) sniffer_socket->Release();
            delete [] snifferTable;
        }

        bool is_busy(unsigned index) 
        {
            if (snifferTable[index].busy)
            {
                time_t now;
                time(&now);
                return (now - snifferTable[index].timeStamp) < 10;
            }
            else
                return false;
        }

        virtual int run() 
        {
            DBGLOG("UdpReceiver: sniffer started");
            while (running) 
            {
                try 
                {
                    unsigned int res;
                    sniff_msg msg;
                    sniffer_socket->read(&msg, 1, sizeof(msg), res, 5);
                    update(msg.nodeIndex, msg.cmd == flow_t::busy);
                }
                catch (IException *e) 
                {
                    if (running && e->errorCode() != JSOCKERR_timeout_expired)
                    {
                        StringBuffer s;
                        DBGLOG("UdpReceiver: receive_sniffer::run read failed %s", e->errorMessage(s).toCharArray());
                        MilliSleep(1000);
                    }
                    e->Release();
                }
                catch (...) 
                {
                    DBGLOG("UdpReceiver: receive_sniffer::run unknown exception port %u", parent.data_port);
                    if (sniffer_socket) {
                        sniffer_socket->Release();
                        sniffer_socket = ISocket::multicast_create(snifferPort, snifferIP);
                    }
                    MilliSleep(1000);
                }
            }
            return 0;
        }

        virtual void start()
        {
            if (udpSnifferEnabled)
            {
                running = true;
                Thread::start();
            }
        }
    };

    class receive_receive_flow : public Thread 
    {
        Owned<ISocket> flow_socket;
        int         flow_port;
        CReceiveManager &parent;
        bool        running;
        
    public:
        receive_receive_flow(CReceiveManager &_parent, int flow_p) : Thread("UdpLib::receive_receive_flow"), parent(_parent)
        {
            flow_port = flow_p;
            if (check_max_socket_read_buffer(udpFlowSocketsSize) < 0) 
            {
                if (!enableSocketMaxSetting)
                    throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
                check_set_max_socket_read_buffer(udpFlowSocketsSize);
            }
            flow_socket.setown(ISocket::udp_create(flow_port));
            flow_socket->set_receive_buffer_size(udpFlowSocketsSize);
            size32_t actualSize = flow_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: rcv_flow_socket created port=%d sockbuffsize=%d actual %d", flow_port, udpFlowSocketsSize, actualSize);
        }
        
        ~receive_receive_flow() 
        {
            running = false;
            if (flow_socket) 
                flow_socket->close();
            join();
        }

        virtual void start()
        {
            running = true;
            Thread::start();
        }

        virtual int run() 
        {
            DBGLOG("UdpReceiver: receive_receive_flow started");
        #ifdef __linux__
            setLinuxThreadPriority(3);
        #else
            adjustPriority(1);
        #endif
            UdpRequestToSendMsg f;
            while (running) 
            {
                try 
                {
                    int l = sizeof(f);
                    unsigned int res ;
                    flow_socket->read(&f, 1, l, res, 5);
                    switch (f.cmd) 
                    {
                    case flow_t::request_to_send:
                        if (udpTraceLevel > 5)
                            DBGLOG("UdpReceiver: received request_to_send msg from node=%u", f.sourceNodeIndex);
                        parent.manager->request(f);
                        break;

                    case flow_t::send_completed:
                        if (udpTraceLevel > 5)
                            DBGLOG("UdpReceiver: received send_completed msg from node=%u", f.sourceNodeIndex);
                        parent.manager->completed(f.sourceNodeIndex);
                        break;

                    case flow_t::request_to_send_more:
                        if (udpTraceLevel > 5)
                            DBGLOG("UdpReceiver: received request_to_send_more msg from node=%u", f.sourceNodeIndex);
                        parent.manager->completed(f.sourceNodeIndex);
                        parent.manager->request(f);
                        break;

                    default:
                        DBGLOG("UdpReceiver: reveived unrecognized flow control message cmd=%i", f.cmd);
                    }
                }
                catch (IException *e)  
                {
                    if (running && e->errorCode() != JSOCKERR_timeout_expired)
                    {
                        StringBuffer s;
                        DBGLOG("UdpReceiver: failed %i %s", flow_port, e->errorMessage(s).toCharArray());
                    }
                    e->Release();
                }
                catch (...) {
                    DBGLOG("UdpReceiver: receive_receive_flow::run unknown exception");
                    MilliSleep(15);
                }
            }
            return 0;
        }
    };

    class receive_data : public Thread 
    {
        CReceiveManager &parent;
        ISocket *receive_socket;
        bool running;
        Semaphore started;
        
    public:
        receive_data(CReceiveManager &_parent) : Thread("UdpLib::receive_data"), parent(_parent)
        {
            unsigned ip_buffer = parent.input_queue_size*DATA_PAYLOAD;
            if (ip_buffer < udpFlowSocketsSize) ip_buffer = udpFlowSocketsSize;
            if (check_max_socket_read_buffer(ip_buffer) < 0) 
            {
                if (!enableSocketMaxSetting)
                    throw MakeStringException(ROXIE_UDP_ERROR, "System socket max read buffer is less than %u", ip_buffer);
                check_set_max_socket_read_buffer(ip_buffer);
            }
            receive_socket = ISocket::udp_create(parent.data_port);
            receive_socket->set_receive_buffer_size(ip_buffer);
            size32_t actualSize = receive_socket->get_receive_buffer_size();
            DBGLOG("UdpReceiver: rcv_data_socket created port=%d requested sockbuffsize=%d actual sockbuffsize=%d", parent.data_port, ip_buffer, actualSize);
        }

        virtual void start()
        {
            running = true;
            Thread::start();
            started.wait();
        }
        
        ~receive_data()
        {
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
            DataBuffer *b = NULL;
            started.signal();
            while (running) 
            {
                try 
                {
                    unsigned int res;
                    b = bufferManager->allocate();
                    receive_socket->read(b->data, 1, DATA_PAYLOAD, res, 5);
                    UdpPacketHeader &hdr = *(UdpPacketHeader *) b->data;
                    unsigned flowBits = hdr.udpSequence & UDP_SEQUENCE_BITS;
                    hdr.udpSequence &= ~UDP_SEQUENCE_BITS;
                    if (flowBits & UDP_SEQUENCE_COMPLETE)
                    {
                        parent.manager->completed(hdr.nodeIndex);
                    }
                    if (flowBits & UDP_SEQUENCE_MORE)
                    {
                        parent.manager->requestMore(hdr.nodeIndex); // MORE - what about the rest of request info?
                    }
                    if (udpTraceLevel > 5) // don't want to interrupt this thread if we can help it
                        DBGLOG("UdpReceiver: %u bytes received, node=%u", res, hdr.nodeIndex);

                    if (udpInlineCollation)
                        parent.collatePacket(b);
                    else
                        parent.input_queue->pushOwn(b);
                    b = NULL;
                }
                catch (IException *e) 
                {
                    ::Release(b);
                    b = NULL;
                    if (running && e->errorCode() != JSOCKERR_timeout_expired)
                    {
                        StringBuffer s;
                        DBGLOG("UdpReceiver: receive_data::run read failed port=%u - Exp: %s", parent.data_port,  e->errorMessage(s).toCharArray());
                        MilliSleep(1000); // Give a chance for mem free
                    }
                    e->Release();
                }
                catch (...) 
                {
                    ::Release(b);
                    b = NULL;
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
    friend class receive_sniffer;
    
    queue_t              *input_queue;
    int                  input_queue_size;
    receive_receive_flow *receive_flow;
    receive_data         *data;
    ReceiveFlowManager   *manager;
    receive_sniffer      *sniffer;
    unsigned myNodeIndex;
    
    int                  send_flow_port;
    int                  receive_flow_port;
    int                  data_port;

    bool running;

    typedef std::map<ruid_t, IMessageCollator*> uid_map;

    Linked<IMessageCollator> defaultMessageCollator;
    uid_map         collators; // MORE - more sensible to use a jlib mapping I would have thought
    SpinLock collatorsLock; // protects access to collators map and defaultMessageCollator

public:
    IMPLEMENT_IINTERFACE;
    CReceiveManager(int server_flow_port, int d_port, int client_flow_port, int snif_port, const IpAddress &multicast_ip, int queue_size, int m_slot_pr_client, unsigned _myNodeIndex)
        : collatorThread(*this)
    {
#ifndef _WIN32
        setpriority(PRIO_PROCESS, 0, -15);
#endif
        myNodeIndex = _myNodeIndex;
        receive_flow_port = server_flow_port;
        send_flow_port = client_flow_port;
        data_port = d_port;
        input_queue_size = queue_size;
        input_queue = new queue_t(queue_size);
        data = new receive_data(*this);
        manager = new ReceiveFlowManager(*this, getNumNodes(), m_slot_pr_client);
        receive_flow = new receive_receive_flow(*this, server_flow_port);
        if (udpSnifferEnabled)
            sniffer = new receive_sniffer(*this, snif_port, multicast_ip, getNumNodes());

        running = true;
        collatorThread.start();
        data->start();
        manager->start();
        receive_flow->start();
        if (udpSnifferEnabled)
            sniffer->start();
        MilliSleep(15);
    }

    ~CReceiveManager() 
    {
        running = false;
        input_queue->interrupt();
        collatorThread.join();
        delete data;
        delete receive_flow;
        delete manager;
        delete sniffer;
        delete input_queue;
    }

    virtual void detachCollator(const IMessageCollator *msgColl) 
    {
        ruid_t ruid = msgColl->queryRUID();
        if (udpTraceLevel >= 2) DBGLOG("UdpReceiver: detach %p %u", msgColl, ruid);
        SpinBlock b(collatorsLock);
        collators.erase(ruid); 
        msgColl->Release();
    }

    virtual void setDefaultCollator(IMessageCollator *msgColl)
    {
        if (udpTraceLevel>=5) DBGLOG("UdpReceiver: setDefaultCollator");
        SpinBlock b(collatorsLock);
        defaultMessageCollator.set(msgColl);
    }

    void collatePackets()
    {
        unsigned lastDiscardedMsgSeq = 0;
        while(running) 
        {
            DataBuffer *dataBuff = input_queue->pop();
            collatePacket(dataBuff);
        }
    }

    void collatePacket(DataBuffer *dataBuff)
    {
        const UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        manager->noteReceived(*pktHdr);

        if (udpTraceLevel >= 4) 
            DBGLOG("UdpReceiver: CPacketCollator - unQed packet - ruid="RUIDF" id=0x%.8X mseq=%u pkseq=0x%.8X len=%d node=%u",
                pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->length, pktHdr->nodeIndex);

        Linked <IMessageCollator> msgColl;
        {
            SpinBlock b(collatorsLock);
            try
            {
                msgColl.set(collators[pktHdr->ruid]);
                if (!msgColl)
                {
                    if (udpTraceLevel)
                        DBGLOG("UdpReceiver: CPacketCollator NO msg collator found - using default - ruid="RUIDF" id=0x%.8X mseq=%u pkseq=0x%.8X node=%u", pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->nodeIndex);
                    msgColl.set(defaultMessageCollator); // MORE - if we get a header, we can send an abort.
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
        if (msgColl) 
        {
            if (msgColl->add_package(dataBuff)) 
            {
                dataBuff = 0;
            }
        }
        else
        {
            // MORE - tell the slave to stop sending?
//              if (udpTraceLevel > 1 && lastDiscardedMsgSeq != pktHdr->msgSeq)
//              DBGLOG("UdpReceiver: CPacketCollator NO msg collator found - discarding packet - ruid="RUIDF" id=0x%.8X mseq=%u pkseq=0x%.8X node=%u", pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->nodeIndex);
//          lastDiscardedMsgSeq = pktHdr->msgSeq;
        }
        if (dataBuff) 
        {   
            dataBuff->Release();
            atomic_inc(&unwantedDiscarded);
        }
    }

    virtual IMessageCollator *createMessageCollator(IRowManager *rowManager, ruid_t ruid)
    {
        IMessageCollator *msgColl = createCMessageCollator(rowManager, ruid);
        if (udpTraceLevel >= 2) DBGLOG("UdpReceiver: createMessageCollator %p %u", msgColl, ruid);
        SpinBlock b(collatorsLock);
        collators[ruid] = msgColl;
        msgColl->Link();
        return msgColl;
    }
};

IReceiveManager *createReceiveManager(int server_flow_port, int data_port, int client_flow_port,
                                      int sniffer_port, const IpAddress &sniffer_multicast_ip,
                                      int udpQueueSize, unsigned maxSlotsPerSender,
                                      unsigned myNodeIndex)
{
    assertex (maxSlotsPerSender <= udpQueueSize);
    return new CReceiveManager(server_flow_port, data_port, client_flow_port, sniffer_port, sniffer_multicast_ip, udpQueueSize, maxSlotsPerSender, myNodeIndex);
}

/*
Thoughts on flow control / streaming:
1. The "continuation packet" mechanism does have some advantages
    - easy recovery from slave failures
    - slave recovers easily from Roxie server failures
    - flow control is simple (but is it effective?)

2. Abandoning continuation packet in favour of streaming would give us the following issues:
    - would need some flow control to stop getting ahead of a Roxie server that consumed slowly
        - flow control is non trivial if you want to avoid tying up a slave thread and want slave to be able to recover from Roxie server failure
    - Need to work out how to do GSS - the nextGE info needs to be passed back in the flow control?
    - can't easily recover from slave failures if you already started processing
        - unless you assume that the results from slave are always deterministic and can retry and skip N
    - potentially ties up a slave thread for a while 
        - do we need to have a larger thread pool but limit how many actually active?

3. Order of work
    - Just adding streaming while ignoring flow control and continuation stuff (i.e. we still stop for permission to continue periodically)
        - Shouldn't make anything any _worse_ ...
            - except that won't be able to recover from a slave dying mid-stream (at least not without some considerable effort)
                - what will happen then?
            - May also break server-side caching (that no-one has used AFAIK). Maybe restrict to nohits as we change....
    - Add some flow control
        - would prevent slave getting too far ahead in cases that are inadequately flow-controlled today
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
    - there is a potential to deadlock when need to make a callback slave->server during a streamed result (indexread5 illustrates)
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
    However it may be an issue tying up slave thread for a while (and do we know when to untie it if the Roxie server abandons/restarts?)

    Perhaps it makes sense to pause at this point (with streaming disabled and with retry mechanism optional)





*/
