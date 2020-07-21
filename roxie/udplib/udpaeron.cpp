/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include <map>
#include "jexcept.hpp"
#include "jqueue.tpp"
#include "udplib.hpp"
#include "udpipmap.hpp"
#include "udpmsgpk.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
#include "roxie.hpp"
#ifdef _USE_AERON
#include <Aeron.h>

extern "C" {
#include "aeronmd.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_driver_context.h"
#include "util/aeron_properties_util.h"
}

// Configurable variables  // MORE - add relevant code to Roxie
bool useEmbeddedAeronDriver = true;
unsigned aeronConnectTimeout = 5000;
unsigned aeronPollFragmentsLimit = 10;
unsigned aeronIdleSleepMs = 1;

unsigned aeronMtuLength = 0;
unsigned aeronSocketRcvbuf = 0;
unsigned aeronSocketSndbuf = 0;
unsigned aeronInitialWindow = 0;

extern UDPLIB_API void setAeronProperties(const IPropertyTree *config)
{
    useEmbeddedAeronDriver = config->getPropBool("@aeronUseEmbeddedDriver", true);
    aeronConnectTimeout = config->getPropInt("@aeronConnectTimeout", 5000);
    aeronPollFragmentsLimit = config->getPropInt("@aeronPollFragmentsLimit", 10);
    aeronIdleSleepMs = config->getPropInt("@aeronIdleSleepMs", 1);

    aeronMtuLength = config->getPropInt("@aeronMtuLength", 0);
    aeronSocketRcvbuf = config->getPropInt("@aeronSocketRcvbuf", 0);
    aeronSocketSndbuf = config->getPropInt("@aeronSocketSndbuf", 0);
    aeronInitialWindow = config->getPropInt("@aeronInitialWindow", 0);

}

static std::thread aeronDriverThread;
static InterruptableSemaphore driverStarted;

std::atomic<bool> aeronDriverRunning = { false };

extern UDPLIB_API void stopAeronDriver()
{
    aeronDriverRunning = false;
    if (aeronDriverThread.joinable())
        aeronDriverThread.join();
}

void sigint_handler(int signal)
{
    stopAeronDriver();
}

void termination_hook(void *state)
{
    stopAeronDriver();
}

inline bool is_running()
{
    return aeronDriverRunning;
}

int startAeronDriver()
{
    aeron_driver_context_t *context = nullptr;
    aeron_driver_t *driver = nullptr;
    try
    {
        if (aeron_driver_context_init(&context) < 0)
            throw makeStringExceptionV(MSGAUD_operator, -1, "AERON: error initializing context (%d) %s", aeron_errcode(), aeron_errmsg());

        context->termination_hook_func = termination_hook;
        context->dirs_delete_on_start = true;
        context->warn_if_dirs_exist = false;
        context->term_buffer_sparse_file = false;

        if (aeronMtuLength) context->mtu_length = aeronMtuLength;
        if (aeronSocketRcvbuf) context->socket_rcvbuf = aeronSocketRcvbuf;
        if (aeronSocketSndbuf) context->socket_sndbuf = aeronSocketSndbuf;
        if (aeronInitialWindow) context->initial_window_length = aeronInitialWindow;

        if (aeron_driver_init(&driver, context) < 0)
            throw makeStringExceptionV(MSGAUD_operator, -1, "AERON: error initializing driver (%d) %s", aeron_errcode(), aeron_errmsg());

        if (aeron_driver_start(driver, true) < 0)
            throw makeStringExceptionV(MSGAUD_operator, -1, "AERON: error starting driver (%d) %s", aeron_errcode(), aeron_errmsg());

        driverStarted.signal();
        aeronDriverRunning = true;
        while (is_running())
        {
            aeron_driver_main_idle_strategy(driver, aeron_driver_main_do_work(driver));
        }
        aeron_driver_close(driver);
        aeron_driver_context_close(context);
    }
    catch (IException *E)
    {
        aeron_driver_close(driver);
        aeron_driver_context_close(context);
        driverStarted.interrupt(E);
    }
    catch (...)
    {
        aeron_driver_close(driver);
        aeron_driver_context_close(context);
        driverStarted.interrupt(makeStringException(0, "failed to start Aeron (unknown exception)"));
    }
    return 0;
}

class CRoxieAeronReceiveManager : public CInterfaceOf<IReceiveManager>
{
private:
    typedef std::map<ruid_t, CMessageCollator*> uid_map;
    uid_map         collators;
    SpinLock collatorsLock; // protects access to collators map

    std::shared_ptr<aeron::Aeron> aeron;
    std::shared_ptr<aeron::Subscription> loSub;
    std::shared_ptr<aeron::Subscription> hiSub;
    std::shared_ptr<aeron::Subscription> slaSub;
    std::thread receiveThread;
    std::atomic<bool> running = { true };
    const std::chrono::duration<long, std::milli> idleSleepMs;
public:
    CRoxieAeronReceiveManager(const SocketEndpoint &myEndpoint)
    : idleSleepMs(aeronIdleSleepMs)
    {
        if (useEmbeddedAeronDriver && !is_running())
        {
            aeronDriverThread = std::thread([]() { startAeronDriver(); });
            driverStarted.wait();
        }
        aeron::Context context;

        if (udpTraceLevel)
        {
            context.newSubscriptionHandler(
                [](const std::string& channel, std::int32_t streamId, std::int64_t correlationId)
                {
                    DBGLOG("AeronReceiver: Subscription: %s %" I64F "d %d", channel.c_str(), (__int64) correlationId, streamId);
                });
            context.availableImageHandler([](aeron::Image &image)
                {
                    DBGLOG("AeronReceiver: Available image correlationId=%" I64F "d, sessionId=%d at position %" I64F "d from %s", (__int64) image.correlationId(), image.sessionId(), (__int64) image.position(), image.sourceIdentity().c_str());
                });
            context.unavailableImageHandler([](aeron::Image &image)
                {
                   DBGLOG("AeronReceiver: Unavailable image correlationId=%" I64F "d, sessionId=%d at position %" I64F "d from %s", (__int64) image.correlationId(), image.sessionId(), (__int64) image.position(), image.sourceIdentity().c_str());
                });
        }
        aeron = aeron::Aeron::connect(context);
        loSub = addSubscription(myEndpoint, 0);
        hiSub = addSubscription(myEndpoint, 1);
        slaSub = addSubscription(myEndpoint, 2);
        aeron::fragment_handler_t handler = [this](const aeron::AtomicBuffer& buffer, aeron::util::index_t offset, aeron::util::index_t length, const aeron::Header& header)
        {
            collatePacket(buffer.buffer() + offset, length);
        };

        receiveThread = std::thread([this, handler]()
        {
            while (running)
            {
                int fragmentsRead = slaSub->poll(handler, aeronPollFragmentsLimit);
                if (!fragmentsRead)
                    fragmentsRead = hiSub->poll(handler, aeronPollFragmentsLimit);
                if (!fragmentsRead)
                    fragmentsRead = loSub->poll(handler, aeronPollFragmentsLimit);
                if (!fragmentsRead)
                    std::this_thread::sleep_for(idleSleepMs);
            }
        });
    }
    ~CRoxieAeronReceiveManager()
    {
        running = false;
        receiveThread.join();
    }

    void collatePacket( std::uint8_t *buffer, aeron::util::index_t length)
    {
        const UdpPacketHeader *pktHdr = (UdpPacketHeader*) buffer;
        assert(pktHdr->length == length);

        if (udpTraceLevel >= 4)
        {
            StringBuffer s;
            DBGLOG("AeronReceiver: CPacketCollator - unQed packet - ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X len=%d node=%s",
                pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->length, pktHdr->node.getTraceText(s).str());
        }

        Linked <CMessageCollator> msgColl;
        bool isDefault = false; // Don't trace inside the spinBlock!
        {
            SpinBlock b(collatorsLock);
            try
            {
                msgColl.set(collators[pktHdr->ruid]);
                if (!msgColl)
                {
                    msgColl.set(collators[RUID_DISCARD]);
                    // We could consider sending an abort to the agent, but it should have already been done by ccdserver code
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
        if (udpTraceLevel && isDefault)
        {
            StringBuffer s;
            DBGLOG("AeronReceiver: CPacketCollator NO msg collator found - using default - ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%s", pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->node.getTraceText(s).str());
        }
        if (msgColl)
            msgColl->attach_data(buffer, length);
    }

    // Note - some of this code could be in a common base class with udpreceivemanager, but hope to kill that at some point
    virtual IMessageCollator *createMessageCollator(roxiemem::IRowManager *rowManager, ruid_t ruid) override
    {
        CMessageCollator *msgColl = new CMessageCollator(rowManager, ruid);
        if (udpTraceLevel >= 2)
            DBGLOG("AeronReceiver: createMessageCollator %p %u", msgColl, ruid);
        {
            SpinBlock b(collatorsLock);
            collators[ruid] = msgColl;
        }
        msgColl->Link();
        return msgColl;
    }

    virtual void detachCollator(const IMessageCollator *msgColl) override
    {
        ruid_t ruid = msgColl->queryRUID();
        if (udpTraceLevel >= 2)
            DBGLOG("AeronReceiver: detach %p %u", msgColl, ruid);
        {
            SpinBlock b(collatorsLock);
            collators.erase(ruid);
        }
        msgColl->Release();
    }

private:

    std::shared_ptr<aeron::Subscription> addSubscription(const SocketEndpoint &myEndpoint, int queue)
    {
        StringBuffer channel("aeron:udp?endpoint=");
        myEndpoint.getUrlStr(channel);
        std::int64_t id = aeron->addSubscription(channel.str(), queue);
        std::shared_ptr<aeron::Subscription> subscription = aeron->findSubscription(id);
        while (!subscription)
        {
            std::this_thread::yield();
            subscription = aeron->findSubscription(id);
        }
        return subscription;
    }
};

class UdpAeronReceiverEntry : public IUdpReceiverEntry
{
private:
    std::shared_ptr<aeron::Aeron> aeron;
    unsigned numQueues;
    std::vector<std::shared_ptr<aeron::Publication>> publications;
    const IpAddress dest;

public:
    UdpAeronReceiverEntry(const IpAddress &_ip, unsigned _dataPort, std::shared_ptr<aeron::Aeron> _aeron, unsigned _numQueues)
    : dest(_ip), aeron(_aeron), numQueues(_numQueues)
    {
        StringBuffer channel("aeron:udp?endpoint=");
        dest.getIpText(channel);
        channel.append(':').append(_dataPort);
        for (unsigned queue = 0; queue < numQueues; queue++)
        {
            if (udpTraceLevel)
                DBGLOG("AeronSender: Creating publication to channel %s for queue %d", channel.str(), queue);
            std::int64_t id = aeron->addPublication(channel.str(), queue);
            std::shared_ptr<aeron::Publication> publication = aeron->findPublication(id);
            // Wait for the publication to be valid
            while (!publication)
            {
                std::this_thread::yield();
                publication = aeron->findPublication(id);
            }
            if ((unsigned) publication->maxPayloadLength() < DATA_PAYLOAD)
                throw makeStringExceptionV(ROXIE_AERON_ERROR, "AeronSender: maximum payload %u too small (%u required)", (unsigned) publication->maxPayloadLength(), (unsigned) DATA_PAYLOAD);
            if (udpTraceLevel <= 4)
                DBGLOG("AeronSender: Publication maxima: %d %d", publication->maxPayloadLength(), publication->maxMessageLength());
            publications.push_back(publication);
            // Wait for up to 5 seconds to connect to a subscriber
            unsigned start = msTick();
            while (!publication->isConnected())
            {
                Sleep(10);
                if (msTick()-start > aeronConnectTimeout)
                    throw makeStringExceptionV(ROXIE_PUBLICATION_NOT_CONNECTED, "AeronSender: Publication not connected to channel %s after %d seconds ", channel.str(), aeronConnectTimeout);
            }
        }
    }
    void write(roxiemem::DataBuffer *buffer, unsigned len, unsigned queue)
    {
        unsigned backoff = 1;
        aeron::concurrent::AtomicBuffer srcBuffer(reinterpret_cast<std::uint8_t *>(&buffer->data), len);
        for (;;)
        {
            const std::int64_t result = publications[queue]->offer(srcBuffer, 0, len);
            if (result < 0)
            {
                if (aeron::BACK_PRESSURED == result || aeron::ADMIN_ACTION == result)
                {
                    // MORE - experiment with best policy. spinning without delay may be appropriate too, depending on cpu availability
                    // and whether data write thread is high priority
                    MilliSleep(backoff-1);  // MilliSleep(0) just does a threadYield
                    if (backoff < 256)
                        backoff = backoff*2;
                    continue;
                }
                StringBuffer target;
                dest.getIpText(target);
                if (aeron::NOT_CONNECTED == result)
                    throw makeStringExceptionV(ROXIE_PUBLICATION_NOT_CONNECTED, "AeronSender: Offer failed because publisher is not connected to subscriber %s", target.str());
                else if (aeron::PUBLICATION_CLOSED == result)
                    throw makeStringExceptionV(ROXIE_PUBLICATION_CLOSED, "AeronSender: Offer failed because publisher is closed sending to %s", target.str());
                else
                    throw makeStringExceptionV(ROXIE_AERON_ERROR, "AeronSender: Offer failed for unknown reason %" I64F "d sending to %s", (__int64) result, target.str());
            }
            break;
        }
    }
};

class CRoxieAeronSendManager : public CInterfaceOf<ISendManager>
{
    std::shared_ptr<aeron::Aeron> aeron;
    const unsigned dataPort = 0;
    const unsigned numQueues = 0;
    IpMapOf<UdpAeronReceiverEntry> receiversTable;
    const IpAddress myIP;

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
    CRoxieAeronSendManager(unsigned _dataPort, unsigned _numQueues, const IpAddress &_myIP)
    : dataPort(_dataPort),
      numQueues(_numQueues),
      receiversTable([this](const IpAddress &ip) { return new UdpAeronReceiverEntry(ip, dataPort, aeron, numQueues);}),
      myIP(_myIP)
    {
        if (useEmbeddedAeronDriver && !is_running())
        {
            aeronDriverThread = std::thread([]() { startAeronDriver(); });
            driverStarted.wait();
        }
        aeron::Context context;
        if (udpTraceLevel)
            context.newPublicationHandler(
                [](const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t correlationId)
                {
                    DBGLOG("AeronSender: Publication %s, correlation %" I64F "d, streamId %d, sessionId %d", channel.c_str(), (__int64) correlationId, streamId, sessionId);
                });

        aeron = aeron::Aeron::connect(context);
    }
    virtual void writeOwn(IUdpReceiverEntry &receiver, roxiemem::DataBuffer *buffer, unsigned len, unsigned queue) override
    {
        assert(queue < numQueues);
        static_cast<UdpAeronReceiverEntry &>(receiver).write(buffer, len, queue);
        buffer->Release();
    }
    virtual IMessagePacker *createMessagePacker(ruid_t id, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue) override;
    virtual bool dataQueued(ruid_t ruid, unsigned sequence, const ServerIdentifier &destNode) override { return false; }
    virtual bool abortData(ruid_t ruid, unsigned sequence, const ServerIdentifier &destNode) override { return false; }
    virtual bool allDone() override { return true; }
};

IMessagePacker *CRoxieAeronSendManager::createMessagePacker(ruid_t ruid, unsigned sequence, const void *messageHeader, unsigned headerSize, const ServerIdentifier &destNode, int queue)
{
    const IpAddress &dest = destNode.getNodeAddress();
    return ::createMessagePacker(ruid, sequence, messageHeader, headerSize, *this, receiversTable[dest], myIP, getNextMessageSequence(), queue);
}

extern UDPLIB_API IReceiveManager *createAeronReceiveManager(const SocketEndpoint &ep)
{
    return new CRoxieAeronReceiveManager(ep);
}

extern UDPLIB_API ISendManager *createAeronSendManager(unsigned dataPort, unsigned numQueues, const IpAddress &myIP)
{
    return new CRoxieAeronSendManager(dataPort, numQueues, myIP);
}

#else

extern UDPLIB_API void setAeronProperties(const IPropertyTree *config)
{
}

extern UDPLIB_API IReceiveManager *createAeronReceiveManager(const SocketEndpoint &ep)
{
    UNIMPLEMENTED;
}

extern UDPLIB_API ISendManager *createAeronSendManager(unsigned dataPort, unsigned numQueues, const IpAddress &myIP)
{
    UNIMPLEMENTED;
}

extern UDPLIB_API void stopAeronDriver()
{
}
#endif
