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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
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

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

unsigned udpOutQsPriority = 0;
unsigned udpMaxRetryTimedoutReqs = 0; // 0 means off (keep retrying forever)
unsigned udpRequestToSendTimeout = 5; // value in sec
bool udpSnifferEnabled = true;

#ifdef _DEBUG
//#define _SIMULATE_LOST_PACKETS
#endif

using roxiemem::DataBuffer;
// MORE - why use DataBuffers on output side?? We could use zeroCopy techniques if we had a dedicated memory area.

class UdpReceiverEntry 
{
    queue_t *output_queue;
    bool    initialized;

    // Circular buffer of packets available for resending
    unsigned retryDataCount;
    unsigned retryDataIdx;
    unsigned maxRetryData;
    DataBuffer **retryData;

public:
    ISocket *send_flow_socket;
    ISocket *data_socket;
    unsigned numQueues;
    int     current_q;
    int     currentQNumPkts;   // Current Queue Number of Consecutive Processed Packets.
    int     *maxPktsPerQ;      // to minimise power function re-calc for evey packet

    SpinLock lock;
    unsigned maxUdpSequence;

    unsigned nextUdpSequence()
    {
        SpinBlock b(lock); // MORE - is this needed?
        unsigned ret = ++maxUdpSequence;
        if (ret & UDP_SEQUENCE_BITS) // overflowed
            ret = 1;
        return ret;
    }

    // MORE - consider where we need critsecs in here!

    void sendRequest(unsigned myNodeIndex, flow_t::flowmsg_t cmd)
    {
        unsigned minUdpSequence;
        SpinBlock b(lock);
        {
            if (retryDataCount)
                minUdpSequence = ((UdpPacketHeader *) retryData[retryDataIdx]->data)->udpSequence;
            else
                minUdpSequence = maxUdpSequence;
        }

        UdpRequestToSendMsg msg = {sizeof(UdpRequestToSendMsg), cmd, myNodeIndex, 0, minUdpSequence, maxUdpSequence};
        try 
        {
            send_flow_socket->write(&msg, msg.length);
        }
        catch(IException *e) 
        {
            StringBuffer s;
            DBGLOG("UdpSender: sendRequest write failed - %s", e->errorMessage(s).toCharArray());
            e->Release();
        }
        catch (...) 
        {
            DBGLOG("UdpSender: sendRequest write failed - unknown error");
        }
    }

    unsigned cleanRetryData(const UdpPermitToSendMsg &permit, PointerArray &retries)
    {
        // Any saved packets < lastReceived that are not listed as missing can be deleted
        SpinBlock b(lock);
        unsigned totalData = 0;
        if (checkTraceLevel(TRACE_RETRY_DATA, 3))
        {
            unsigned minUdpSequence;
            if (retryDataCount)
                minUdpSequence = ((UdpPacketHeader *) retryData[retryDataIdx]->data)->udpSequence;
            else
                minUdpSequence = maxUdpSequence;
            StringBuffer permitStr;
            permit.toString(permitStr);
            DBGLOG("UdpSender: cleanRetryData (%s), total %u available between %u and %u", permitStr.str(), retryDataCount, minUdpSequence, maxUdpSequence); 
        }
        unsigned lastReceived = permit.hdr.lastSequenceSeen;
        unsigned missingIndex = 0;
        unsigned missingCount = permit.hdr.missingCount;
        unsigned i = 0;
        if (maxRetryData)
        {
            while (i < retryDataCount && retries.length() < permit.hdr.max_data)
            {
                unsigned idx = (retryDataIdx + i) % maxRetryData;
                DataBuffer *buffer = retryData[idx];
                if (buffer)
                {
                    UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
                    unsigned thisSequence = header->udpSequence;
                    if (thisSequence > lastReceived)
                        break;

                    if (!missingCount || thisSequence < permit.missingSequences[missingIndex])
                    {
                        ::Release(buffer);
                        retryData[idx] = NULL;
                        if (i)
                            i++; // MORE - leaves holes - is this smart? Alternatively could close up... Should be rare anyway
                        else
                        {
                            retryDataIdx = (retryDataIdx + 1) % maxRetryData;
                            retryDataCount--; 
                        }               
                    }
                    else if (thisSequence == permit.missingSequences[missingIndex])
                    {
                        totalData += header->length;
                        retries.append(buffer);
                        i++;
                        missingIndex++;
                        missingCount--;
                    }
                    else
                    {
                        missingIndex++;
                        missingCount--;
                    }
                }
                else
                {
                    if (i)
                        i++;
                    else
                    {
                        // Removing leading nulls
                        retryDataCount--;
                        retryDataIdx = (retryDataIdx + 1) % maxRetryData;
                    }
                }
            }
        }
        if (checkTraceLevel(TRACE_RETRY_DATA, 3))
            DBGLOG("UdpSender: cleanRetryData found %u to resend total size %u, total %u still available", retries.length(), totalData, retryDataCount);
        return totalData;
    }

    unsigned sendData(const UdpPermitToSendMsg &permit, bool isLocal, TokenBucket *bucket, bool &moreRequested, unsigned &maxPackets)
    {
        moreRequested = false;
        maxPackets = permit.hdr.max_data;
        PointerArray toSend;
        unsigned totalSent = cleanRetryData(permit, toSend);
        while (toSend.length() < maxPackets && dataQueued())
        {
            DataBuffer *buffer = popQueuedData();
            if (buffer) // Aborted slave queries leave NULL records on queue
            {
                UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
                toSend.append(buffer);
                totalSent += header->length;
#ifdef __linux__
                if (isLocal && (totalSent> 100000)) 
                    break;
#endif
            }
        }
        maxPackets = toSend.length();
        for (unsigned idx = 0; idx < maxPackets; idx++)
        {
            DataBuffer *buffer = (DataBuffer *) toSend.item(idx);
            UdpPacketHeader *header = (UdpPacketHeader*) buffer->data;
            bool isRetry = (header->udpSequence != 0);
            if (isRetry)
            {
                if (checkTraceLevel(TRACE_RETRY_DATA, 1))
                    DBGLOG("UdpSender: Resending packet to destination node %u sequence %u", permit.hdr.destNodeIndex, header->udpSequence);
                atomic_inc(&packetsRetried);
            }
            else
                header->udpSequence = nextUdpSequence();
            unsigned length = header->length;
            if (bucket)
            {
                MTIME_SECTION(timer, "bucket_wait");
                bucket->wait((length / 1024)+1);
            }
            try
            {
                if (udpSendCompletedInData)
                {
                    if (idx == maxPackets-1)
                    {
                         // MORE - is this safe ? Any other thread looking at the data right now? Don't _think_ so...
                        if (false && dataQueued()) // Causes some problems because no flow control info gets through at all
                        {
                            moreRequested = true;
                            header->udpSequence |= (UDP_SEQUENCE_COMPLETE|UDP_SEQUENCE_MORE);
                        }
                        else
                            header->udpSequence |= UDP_SEQUENCE_COMPLETE;
                    }
                }
#ifdef _SIMULATE_LOST_PACKETS
                if (isRetry || (header->udpSequence % 100) != 0)
#endif
                data_socket->write(buffer->data, length);
                header->udpSequence &= ~UDP_SEQUENCE_BITS;
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
            if (!isRetry && maxRetryData)
            {
                unsigned slot = (retryDataIdx + retryDataCount) % maxRetryData;
                if (retryDataCount < maxRetryData)
                    retryDataCount++;
                else
                {
                    if (udpTraceLevel > 0)
                        DBGLOG("Overflow in resend packet buffer for destination node %u - discarding packet sequence %u", permit.hdr.destNodeIndex, header->udpSequence);
                    ::Release(retryData[slot]);
                }
                retryData[slot] = buffer;
            }
            else
            {
                ::Release(buffer);
            }
        }
        return totalSent;
    }

    bool dataQueued()
    {
        for (unsigned i = 0; i < numQueues; i++) 
        {
            if (!output_queue[i].empty()) 
                return true;
        }
        return false;
    }

    bool dataQueued(void *key, PKT_CMP_FUN pkCmpFn) 
    {
        for (unsigned i = 0; i < numQueues; i++) 
        {
            if (output_queue[i].dataQueued(key, pkCmpFn)) 
                return true;
        }
        return false;
    }

    bool removeData(void *key, PKT_CMP_FUN pkCmpFn) 
    {
        bool anyRemoved = false;
        for (unsigned i = 0; i < numQueues; i++) 
        {
            if (output_queue[i].removeData(key, pkCmpFn)) 
                anyRemoved = true;
        }
        return anyRemoved;
    }

    inline void pushData(unsigned queue, DataBuffer *buffer)
    {
        output_queue[queue].pushOwn(buffer);
    }

    DataBuffer *popQueuedData() 
    {
        DataBuffer *buffer;
        while (1) 
        {
            for (unsigned i = 0; i < numQueues; i++) 
            {
                if (udpOutQsPriority) 
                {
                    if (output_queue[current_q].empty()) 
                    {
                        if (udpTraceLevel >= 5)
                            DBGLOG("UdpSender: ---------- Empty Q %d", current_q);
                        currentQNumPkts = 0;
                        current_q = (current_q + 1) % numQueues;
                    }
                    else 
                    {
                        buffer = output_queue[current_q].pop();
                        currentQNumPkts++;
                        if (udpTraceLevel >= 5) 
                            DBGLOG("UdpSender: ---------- Packet from Q %d", current_q);
                        if (currentQNumPkts >= maxPktsPerQ[current_q]) 
                        {
                            currentQNumPkts = 0;
                            current_q = (current_q + 1) % numQueues;
                        }
                        return buffer;
                    }
                }
                else 
                {
                    current_q = (current_q + 1) % numQueues;
                    if (!output_queue[current_q].empty()) 
                    {
                        return output_queue[current_q].pop();
                    }
                }
            }
            MilliSleep(10);
            DBGLOG("UdpSender: ------------- this code should never execute --------------- ");
        }
    }

    UdpReceiverEntry() 
    {
        send_flow_socket = data_socket = NULL;
        numQueues = 0;
        current_q = 0;
        initialized = false;
        output_queue = 0;
        currentQNumPkts = 0;
        maxPktsPerQ = 0;
        maxUdpSequence = 0;
        retryDataCount = 0;
        retryDataIdx = 0;
        retryData = NULL;
    }

    void init(unsigned destNodeIndex, unsigned _numQueues, unsigned queueSize, unsigned _maxRetryData, unsigned sendFlowPort, unsigned dataPort, bool isLocal)
    {
        assert(!initialized);
        maxRetryData = _maxRetryData;
        numQueues = _numQueues;
        const IpAddress &ip = getNodeAddress(destNodeIndex);
        if (!ip.isNull())
        {
            try 
            {
                SocketEndpoint sendFlowEp(sendFlowPort, ip);
                SocketEndpoint dataEp(dataPort, ip);
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
                output_queue[j].set_queue_size(queueSize);
                maxPktsPerQ[j] = (int) pow((double)udpOutQsPriority, (double)numQueues - j - 1);
            }
            if (maxRetryData)
            {
                retryData = new DataBuffer *[maxRetryData];
                memset(retryData, 0, maxRetryData * sizeof(DataBuffer *));
            }
            initialized = true;
            if (udpTraceLevel > 0)
            {
                StringBuffer ipStr;
                DBGLOG("UdpSender: added entry for ip=%s to receivers table at index=%d - send_flow_port=%d", ip.getIpText(ipStr).str(), destNodeIndex, sendFlowPort);
            }
        }
    }

    ~UdpReceiverEntry()
    {
        if (retryData) delete[] retryData; // MORE - should I release the values pointed to as well - not a big deal as quitting anyway...
        if (send_flow_socket) send_flow_socket->Release();
        if (data_socket) data_socket->Release();
        if (output_queue) delete [] output_queue;
        if (maxPktsPerQ) delete [] maxPktsPerQ;
    }

};

class CSendManager : public CInterface, implements ISendManager
{
    friend class send_send_flow;
    class CMessagePacker : public CInterface, implements IMessagePacker
    {
        CSendManager   &parent;
        unsigned        destNodeIndex;
        UdpPacketHeader package_header;
        DataBuffer     *part_buffer;
        unsigned data_buffer_size;
        unsigned data_used;
        void *mem_buffer;
        unsigned mem_buffer_size;
        unsigned totalSize;
        bool            packed_request;
        unsigned        requested_size;
        MemoryBuffer    metaInfo;
        bool            last_message_done;
        bool            aborted;
        int             queue_number;
        
    public:
        IMPLEMENT_IINTERFACE;

        CMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, CSendManager &_parent, unsigned _destNode, unsigned _sourceNode, unsigned _msgSeq, unsigned _queue) 
            : parent(_parent)
        {
            queue_number = _queue;
            destNodeIndex = _destNode;
            
            package_header.length = 0;          // filled in with proper value later
            package_header.metalength = 0;
            package_header.ruid = ruid;
            package_header.msgId = msgId;
            package_header.pktSeq = 0;
            package_header.nodeIndex = _sourceNode;
            package_header.msgSeq = _msgSeq;
            package_header.udpSequence = 0; // these are allocated when transmitted
            
            aborted = false;
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

            if (udpTraceLevel >= 40) 
                DBGLOG("UdpSender: CMessagePacker::CMessagePacker - ruid="RUIDF" id=0x%.8x mseq=%u node=%u queue=%d", ruid, msgId, _msgSeq, destNodeIndex, _queue);
            
        }

        ~CMessagePacker() 
        {
            if (part_buffer)
                part_buffer->Release();
            if (mem_buffer) free (mem_buffer);
            
            if (udpTraceLevel >= 40) 
            {
                DBGLOG("UdpSender: CMessagePacker::~CMessagePacker - ruid="RUIDF" id=0x%.8x mseq=%u pktSeq=%x node=%u",
                    package_header.ruid, package_header.msgId, package_header.msgSeq, package_header.pktSeq, destNodeIndex);
            }
        }

        virtual void abort() 
        {
            aborted = true;
        }
        
        virtual void *getBuffer(unsigned len, bool variable) 
        {
            if (variable)
                len += sizeof(RecordLengthType);
            if (DATA_PAYLOAD - sizeof(UdpPacketHeader) < len) 
            {
                // Won't fit in one, so allocate temp location
                if (mem_buffer_size < len) 
                {
                    free(mem_buffer);
                    mem_buffer = malloc(len);
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
        
        virtual void putBuffer(const void *buf, unsigned len, bool variable) 
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

        virtual void sendMetaInfo(const void *buf, unsigned len) {
            metaInfo.append(len, buf);
        }

        virtual void flush(bool last_msg = false) 
        {
            assert(!aborted);
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
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, metaLength);
                package_header.pktSeq |= 0x80000000;
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
            parent.writeOwn(destNodeIndex, dataBuff, package_header.length, queue_number);

            if (udpTraceLevel >= 50) 
            {
                if (package_header.length==991)
                    DBGLOG("NEarly");
                DBGLOG("UdpSender: CMessagePacker::put_package Qed packet - ruid="RUIDF" id=0x%.8X mseq=%u pkseq=0x%.8X len=%u node=%u queue=%d",
                    package_header.ruid, package_header.msgId, package_header.msgSeq,
                    package_header.pktSeq, package_header.length, destNodeIndex, queue_number);
            }
            package_header.pktSeq++;
        }

        virtual bool dataQueued()
        {
            return(parent.dataQueued(package_header.ruid, package_header.msgId, destNodeIndex));
        }

        virtual unsigned size() const
        {
            return totalSize;
        }
    };

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


    class send_send_flow : public StartedThread 
    {
        /*
        I don't like this code much at all
        Looping round all every time status of any changes seems like a bad thing especially as scale goes up
        Even though these look like a bitmap they are not used as such presently
         - as a result, if data_added() is called while state is completed, we lose the request I think
         - probably get away with it because of the dataqueued check

        doRun() uses state bits without protection

        A count of number pending for each might be better than just a flag

        Circular buffers to give a list of which ones are in a given state would speed up the processing in the thread?
         - normally MANY in pending (and order is interesting)
         - normally few in any other state (only 1 if thread keeping up), order not really very interesting
         - Want to keep expense on caller threads low (at the moment just set flag and sometimes signal)
          - in particular don't lock while processing the chain
         - Never need to be in >1 chain
        msTick() probably better than time() for detecting timeouts

        */
        enum bits { new_request = 1, pending_request = 2, sending_data = 4, completed = 8, completed_more = 16 };

        unsigned target_count;

        char *state;
        unsigned char *timeouts;   // Number of consecutive timeouts
        time_t *request_time;

        CriticalSection cr;
        Semaphore       sem;
        CSendManager &parent;

        virtual int doRun() 
        {
            // MORE - this is reading the state values unprotected
            // Not sure that this represents any issue in practice...
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_send_flow started - node=%u", parent.myNodeIndex);

            while (running) 
            {
                bool idle = false;
                if (sem.wait(1000)) 
                {
                    if (udpTraceLevel > 4)
                        DBGLOG("UdpSender: send_send_flow::doRun signal received");
                }
                else
                    idle = true;
                if (!running) return 0;

                time_t now;
                time(&now);

                // I don't really like this loop. Could keep a circular buffer of ones with non-zero state?
                // In a typical heavy load scenario most will be pending
                // Really two separate FIFOs - pending and active. Except that stuff pulled off pending in arbitrary order
                // Separate lists for each state (don't need one for sending) ?

                for (unsigned i = 0; i < target_count; i++)
                {
                    switch (state[i]) // MORE - should really protect it?
                    {
                    case completed:
                        done(i, false);
                        break;
                    case completed_more:
                        done(i, true);
                        break;
                    case pending_request: 
                        if ( (now - request_time[i]) < udpRequestToSendTimeout) // MORE - should really protect it?
                            break;
                        timeouts[i]++;
                        EXCLOG(MCoperatorError,"ERROR: UdpSender: timed out %i times (max=%i) waiting ok_to_send msg from node=%d timed out after=%i sec max=%i sec",
                                timeouts[i], udpMaxRetryTimedoutReqs,   
                                i, (int) (now - request_time[i]), udpRequestToSendTimeout);
                        // 0 (zero) value of udpMaxRetryTimedoutReqs means NO limit on retries
                        if (udpMaxRetryTimedoutReqs && (timeouts[i] >= udpMaxRetryTimedoutReqs))
                        {
                            abort(i);
                            break;
                        }
                        // fall into...

                    case new_request:
                        sendRequest(i);
                        break;
                    default:
                        if (idle && parent.dataQueued(i))
                        {
                            EXCLOG(MCoperatorError, "State is idle but data is queued - should not happen (index = %u). Attempting recovery.", i);
                            data_added(i);
                        }
                    }
                }
            }
            return 0;
        }
        
        void done(unsigned index, bool moreRequested)
        {
            bool dataRemaining;
            {
                CriticalBlock b(cr);
                dataRemaining = parent.dataQueued(index);
                if (dataRemaining)
                {
                    state[index] = pending_request;
                    time(&request_time[index]); 
                }
                else
                {
                    state[index] = 0;
                    timeouts[index] = 0;
                }
            }

            if (udpTraceLevel > 3) 
                DBGLOG("UdpSender: sending send_completed msg to node=%u, dataRemaining=%d", index, dataRemaining);
            if (udpSendCompletedInData)
            {
                if (dataRemaining)
                {
                    // MORE - we indicate the more to send via a bit already - don't need this unless we never go idle
                    // though there is a possible race to consider
                    if (!moreRequested)
                        parent.sendRequest(index, flow_t::request_to_send); 
                }
            }
            else
                parent.sendRequest(index, dataRemaining ? flow_t::request_to_send_more : flow_t::send_completed);
        }

        void sendRequest(unsigned index) 
        {
            if (udpTraceLevel > 3) 
                DBGLOG("UdpSender: sending request_to_send msg to node=%u", index);
            CriticalBlock b(cr);
            parent.sendRequest(index, flow_t::request_to_send);
            state[index] = pending_request;
            time(&request_time[index]); 
        }

        void abort(unsigned index) 
        {
            if (udpTraceLevel > 3) 
                DBGLOG("UdpSender: abort sending queued data to node=%u", index);
            
            CriticalBlock b(cr);
            state[index] = 0;
            timeouts[index] = 0;
            parent.abortData(index);
        }

    public:
        send_send_flow(CSendManager &_parent, unsigned numNodes) 
            : StartedThread("UdpLib::send_send_flow"), parent(_parent)
        {
            target_count = numNodes;
            state = new char [target_count];
            memset(state, 0, target_count);
            timeouts = new unsigned char [target_count];
            memset(timeouts, 0, target_count);
            request_time = new time_t [target_count];
            memset(request_time, 0, sizeof(time_t) * target_count);
            start();
        }

        ~send_send_flow() 
        {
            running = false;
            sem.signal();
            join();
            delete [] state;
            delete [] timeouts;
            delete [] request_time;
        }

        void clear_to_send_received(unsigned index) 
        {
            CriticalBlock b(cr);
            state[index] = sending_data;
        }

        void send_done(unsigned index, bool moreRequested) 
        {
            CriticalBlock b(cr);
            state[index] = moreRequested ? completed_more : completed;
            sem.signal();
        }

        void data_added(unsigned index) 
        {
            CriticalBlock b(cr);
            // MORE - this looks wrong. If I add data while sending, may get delayed until next time I have data to send?? Why declare as bitmap if not going to use it?
            // Because done() checks to see if any data pending and re-calls data_added, we get away with it
            // Using bits sounds more sensible though?
            // Actually be careful, since a send may not send ALL the data - you'd still need to call data_added if that happened. Maybe as it is is ok.
            if (!state[index]) // MORE - should just test the bit?
            {
                state[index] = new_request;
                if (udpTraceLevel > 3) 
                    DBGLOG("UdpSender: state set to new_request for node=%u", index);
                sem.signal();
            }
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
            {
                if (!enableSocketMaxSetting)
                    throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max read buffer is less than %i", udpFlowSocketsSize);
                check_set_max_socket_read_buffer(udpFlowSocketsSize);
            }
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
                UdpPermitToSendMsg f;
                while (running) 
                {
                    try 
                    {
                        unsigned int res ;
                        flow_socket->read(&f, 1, sizeof(f), res, 5);
                        assertex(res == f.hdr.length);
#ifdef CRC_MESSAGES
                        assertex(f.hdr.crc == f.calcCRC());
#endif
                        switch (f.hdr.cmd)
                        {
                        case flow_t::ok_to_send:
                            if (udpTraceLevel > 1) 
                                DBGLOG("UdpSender: received ok_to_send msg max %d packets from node=%u (length %u)", f.hdr.max_data, f.hdr.destNodeIndex, res);
                            parent.data->ok_to_send(f);
                            break;

                        default: 
                            DBGLOG("UdpSender: received unknown flow message type=%d", f.hdr.cmd);
                        }
                    }
                    catch (IException *e) 
                    {
                        if (running && e->errorCode() != JSOCKERR_timeout_expired)
                        {
                            StringBuffer s;
                            DBGLOG("UdpSender: send_receive_flow::read failed port=%i %s", receive_port, e->errorMessage(s).toCharArray());
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

        void send_sniff(bool busy)
        {
            sniff_msg msg = {sizeof(sniff_msg), busy ? flow_t::busy : flow_t::idle, parent.myNodeIndex};
            try 
            {
                if (!sniffer_socket) 
                {
                    sniffer_socket = ISocket::multicast_connect(ep, 3);
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
                DBGLOG("UdpSender: multicast_connect or write failed ep=%s - %s", ep.getUrlStr(url).str(), e->errorMessage(s).toCharArray());
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
            {
                if (!enableSocketMaxSetting)
                    throw MakeStringException(ROXIE_UDP_ERROR, "System Socket max write buffer is less than %i", udpLocalWriteSocketSize);
                check_set_max_socket_write_buffer(udpLocalWriteSocketSize);
            }
            start();
        }
        
        ~send_data()
        {
            running = false;
            UdpPermitToSendMsg dummy;
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
                DBGLOG("UdpSender: push() failed - ignored ok_to_send msg - index=%u, maxData=%u", msg.hdr.destNodeIndex, msg.hdr.max_data);
                return false;
            }
        }

        virtual int doRun() 
        {
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_data started");
        #ifdef __linux__
            setLinuxThreadPriority(1); // MORE - windows?
        #endif
            UdpPermitToSendMsg permit;
            while (running) 
            {
                send_queue.pop(permit);
                if (!running)
                    return 0;

                if (udpSnifferEnabled)
                    send_sniff(true);
                parent.send_flow->clear_to_send_received(permit.hdr.destNodeIndex);
                UdpReceiverEntry &receiverInfo = parent.receiversTable[permit.hdr.destNodeIndex];
                bool moreRequested;
                unsigned maxPackets;
                unsigned payload = receiverInfo.sendData(permit, (parent.myNodeIndex == permit.hdr.destNodeIndex), bucket, moreRequested, maxPackets);
                if (udpSendCompletedInData && !maxPackets)
                    parent.sendRequest(permit.hdr.destNodeIndex, flow_t::send_completed);
                parent.send_flow->send_done(permit.hdr.destNodeIndex, moreRequested);
                if (udpSnifferEnabled)
                    send_sniff(false);
                
                if (udpTraceLevel > 1) 
                    DBGLOG("UdpSender: sent %u bytes to node=%d", payload, permit.hdr.destNodeIndex);
                
            }
            if (udpTraceLevel > 0)
                DBGLOG("UdpSender: send_data stopped");
            return 0;
        }
    };

    friend class send_send_flow;
    friend class send_receive_flow;
    friend class send_data;

    unsigned numNodes;
    int               receive_flow_port;
    int               send_flow_port;
    int               data_port;
    unsigned          myNodeIndex;
    unsigned numQueues;

    UdpReceiverEntry  *receiversTable;
    send_send_flow    *send_flow;
    send_receive_flow *receive_flow;
    send_data         *data;
    Linked<TokenBucket> bucket;
    
    SpinLock msgSeqLock;
    unsigned msgSeq;

    static bool comparePacket(void *pkData, void *key) 
    {
        UdpPacketHeader *dataHdr = (UdpPacketHeader*) ((DataBuffer*)pkData)->data;
        UdpPacketHeader *keyHdr = (UdpPacketHeader*) key;
        return ( (dataHdr->ruid == keyHdr->ruid) && (dataHdr->msgId == keyHdr->msgId) );
    }

    inline unsigned getNextMessageSequence()
    {
        SpinBlock b(msgSeqLock);
        unsigned res = ++msgSeq;
        if (!res)
            res = ++msgSeq;
        return res;
    }
        
public:
    IMPLEMENT_IINTERFACE;

    CSendManager(int server_flow_port, int d_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int q_size, int _numQueues, unsigned maxRetryData, unsigned _myNodeIndex, TokenBucket *_bucket)
        : bucket(_bucket)
    {
#ifndef _WIN32
        setpriority(PRIO_PROCESS, 0, -3);
#endif
        numNodes = getNumNodes();
        receive_flow_port = client_flow_port;
        send_flow_port = server_flow_port;
        data_port = d_port;
        myNodeIndex = _myNodeIndex;
        numQueues = _numQueues;
        receiversTable = new UdpReceiverEntry[numNodes];
        if (maxRetryData > MAX_RESEND_TABLE_SIZE)
            maxRetryData = MAX_RESEND_TABLE_SIZE;
        for (unsigned i = 0; i < numNodes; i++)
            receiversTable[i].init(i, numQueues, q_size, maxRetryData, send_flow_port, data_port, i==myNodeIndex);

        data = new send_data(*this, sniffer_port, sniffer_multicast_ip, bucket);
        send_flow = new send_send_flow(*this, numNodes);
        receive_flow = new send_receive_flow(*this, client_flow_port);
        msgSeq = 0;
    }


    ~CSendManager() 
    {
        delete []receiversTable;
        delete send_flow;
        delete receive_flow;
        delete data;
    }

    void writeOwn(unsigned destNodeIndex, DataBuffer *buffer, unsigned len, unsigned queue)
    {
        // NOTE: takes ownership of the DataBuffer
        assert(queue < numQueues);
        assert(destNodeIndex < numNodes);
        receiversTable[destNodeIndex].pushData(queue, buffer);
        send_flow->data_added(destNodeIndex);
    }

    inline void sendRequest(unsigned destIndex, flow_t::flowmsg_t cmd)
    {
        receiversTable[destIndex].sendRequest(myNodeIndex, cmd);
    }

    bool dataQueued(unsigned destIndex) 
    {
        return receiversTable[destIndex].dataQueued();
    }

    bool abortData(unsigned destIndex)
    {
        return receiversTable[destIndex].removeData(NULL, NULL);
    }

    // Interface ISendManager

    virtual IMessagePacker *createMessagePacker(ruid_t ruid, unsigned sequence, const void *messageHeader, unsigned headerSize, unsigned destNodeIndex, int queue)
    {
        if (destNodeIndex >= numNodes)
            throw MakeStringException(ROXIE_UDP_ERROR, "createMessagePacker: invalid destination node index %i", destNodeIndex);
        return new CMessagePacker(ruid, sequence, messageHeader, headerSize, *this, destNodeIndex, myNodeIndex, getNextMessageSequence(), queue);
    }

    virtual bool dataQueued(ruid_t ruid, unsigned msgId, unsigned destIndex)
    {
        UdpPacketHeader   pkHdr;
        pkHdr.ruid = ruid;
        pkHdr.msgId = msgId;
        return receiversTable[destIndex].dataQueued((void*) &pkHdr, &comparePacket);
    }

    virtual bool abortData(ruid_t ruid, unsigned msgId, unsigned destIndex)
    {
        UdpPacketHeader pkHdr;
        pkHdr.ruid = ruid;
        pkHdr.msgId = msgId;
        return receiversTable[destIndex].removeData((void*) &pkHdr, &comparePacket);
    }

    virtual bool allDone() 
    {
        for (unsigned i = 0; i < numNodes; i++) 
        {
            if (receiversTable[i].dataQueued())
                return false;
        }
        return true;
    }

};

ISendManager *createSendManager(int server_flow_port, int data_port, int client_flow_port, int sniffer_port, const IpAddress &sniffer_multicast_ip, int queue_size_pr_server, int queues_pr_server, unsigned maxRetryData, TokenBucket *rateLimiter, unsigned myNodeIndex)
{
    return new CSendManager(server_flow_port, data_port, client_flow_port, sniffer_port, sniffer_multicast_ip, queue_size_pr_server, queues_pr_server, maxRetryData, myNodeIndex, rateLimiter);
}


