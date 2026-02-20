/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include <unordered_map>
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

#include "socketutils.hpp"

using roxiemem::DataBuffer;
using roxiemem::IRowManager;


    class TcpReadTracker : public TimeDivisionTracker<6, false>
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

        TcpReadTracker(const char *name, unsigned reportIntervalSeconds) : TimeDivisionTracker<6, false>(name, reportIntervalSeconds)
        {
            stateNames[other] = "other";
            stateNames[waiting] = "waiting";
            stateNames[allocating] = "allocating";
            stateNames[processing] = "processing";
            stateNames[pushing] = "pushing";
            stateNames[checkingPending] = "checking pending";
        }

    };


class CTcpReceiveManager : implements IReceiveManager, public CInterface
{
    queue_t              *input_queue;
    const int             data_port;

    std::atomic<bool> running = { false };
    bool encrypted = false;
    const bool collateDirectly = true;

    typedef std::unordered_map<ruid_t, CMessageCollator*> uid_map;
    uid_map         collators;
    CriticalSection collatorsLock; // protects access to collators map
    roxiemem::IDataBufferManager * udpBufferManager;

    class CTcpPacketCollator : public Thread
    {
        CTcpReceiveManager &parent;
    public:
        CTcpPacketCollator(CTcpReceiveManager &_parent) : Thread("CTcpPacketCollator"), parent(_parent) {}

        virtual int run()
        {
            DBGLOG("TcpReceiver: CPacketCollator::run");
            parent.collatePackets();
            return 0;
        }
    } collatorThread;

    class PacketListener : public CSocketConnectionListener
    {
    public:
        PacketListener(CTcpReceiveManager & _receiver)
        : CSocketConnectionListener(0, false, 0, 0), receiver(_receiver)
        {
            maxInitialReadSize = roxiemem::DATA_ALIGNMENT_SIZE * 4;
        }

        virtual bool onlyProcessFirstRead() const override
        {
            return false;
        }

        virtual unsigned getMessageSize(const void * header) const override
        {
            const UdpPacketHeader * udpHeader = (const UdpPacketHeader *)header;
            return udpHeader->length;
        }

        virtual CReadSocketHandler *createSocketHandler(ISocket *sock) override
        {
            return new CReadSocketHandler(*this, asyncReader, sock, sizeof(UdpPacketHeader), maxInitialReadSize);
        }

        virtual void processMessage(const void * data, size32_t len) override
        {
            receiver.processMessage(data, len);
        }

    protected:
        CTcpReceiveManager & receiver;
        size32_t maxInitialReadSize;
    } listener;


public:
    IMPLEMENT_IINTERFACE;
    CTcpReceiveManager(int server_flow_port, int d_port, int client_flow_port, int queue_size, bool _encrypted)
        : data_port(d_port), encrypted(_encrypted),
          collatorThread(*this), listener(*this)
    {
        running = true;
        input_queue = new queue_t(queue_size);
        udpBufferManager = bufferManager; // ugly global variable...
        if (!collateDirectly)
            collatorThread.start(false);
        listener.startPort(data_port);
    }

    ~CTcpReceiveManager()
    {
        running = false;
        listener.stop();
        input_queue->interrupt();
        if (!collateDirectly)
            collatorThread.join();
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
        TcpReadTracker timeTracker("collatePackets", 60);
        while(running)
        {
            try
            {
                TcpReadTracker::TimeDivision d(timeTracker, TcpReadTracker::waiting);
                DataBuffer *dataBuff = input_queue->pop(true);
                d.switchState(TcpReadTracker::processing);
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

    void processMessage(const void * data, size32_t len)
    {
        const UdpPacketHeader * header = (const UdpPacketHeader *)data;
        unsigned packetLength = header->length;
        dbgassertex(packetLength <= roxiemem::DATA_ALIGNMENT_SIZE);

        DataBuffer * buffer = udpBufferManager->allocate();
        memcpy(buffer->data, header, packetLength);
        if (collateDirectly)
            collatePacket(buffer);
        else
            input_queue->pushOwn(buffer);
    }

};

IReceiveManager *createTcpReceiveManager(int server_flow_port, int data_port, int client_flow_port,
                                      int udpQueueSize, bool encrypted)
{
    return new CTcpReceiveManager(server_flow_port, data_port, client_flow_port, udpQueueSize, encrypted);
}
