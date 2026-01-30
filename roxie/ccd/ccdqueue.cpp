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

#include <platform.h>
#include <jlib.hpp>
#include <jio.hpp>
#include <jqueue.tpp>
#include <jsocket.hpp>
#include <jlog.hpp>
#include "jisem.hpp"
#include "jencrypt.hpp"
#include "jsecrets.hpp"

#include "udplib.hpp"
#include "udptopo.hpp"
#include "udpsha.hpp"
#include "ccd.hpp"
#include "ccddebug.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"

#include "socketutils.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>
#endif

using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedConstRoxieRow;
using roxiemem::IRowManager;
using roxiemem::DataBuffer;

//============================================================================================

RoxiePacketHeader::RoxiePacketHeader(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence)
{
    packetlength = sizeof(RoxiePacketHeader);
    init(_remoteId, _uid, _channel, _overflowSequence);
}

RoxiePacketHeader::RoxiePacketHeader(const RoxiePacketHeader &source, unsigned _activityId, unsigned subChannel) : serverId(source.serverId)
{
    // Used to create the header to send a callback to originating server or an IBYTI to a buddy
    activityId = _activityId;
    uid.store(source.uid);
    queryHash = source.queryHash;
    channel = source.channel;
    overflowSequence = source.overflowSequence;
    continueSequence = source.continueSequence;
    if (_activityId >= ROXIE_ACTIVITY_SPECIAL_FIRST && _activityId <= ROXIE_ACTIVITY_SPECIAL_LAST)
        overflowSequence |= OUTOFBAND_SEQUENCE; // Need to make sure it is not treated as dup of actual reply in the udp layer
    retries = getSubChannelMask(subChannel) | (source.retries & ~ROXIE_RETRIES_MASK);
    memcpy(subChannels, source.subChannels, sizeof(subChannels));
    packetlength = sizeof(RoxiePacketHeader);
}

unsigned RoxiePacketHeader::getSubChannelMask(unsigned subChannel)
{
    return SUBCHANNEL_MASK << (SUBCHANNEL_BITS * subChannel);
}

unsigned RoxiePacketHeader::priorityHash() const
{
    // Used to determine which agent to act as primary and which as secondary for a given packet (thus spreading the load)
    // It's important that we do NOT include channel (since that would result in different values for the different agents responding to a broadcast)
    // We also don't include continueSequence since we'd prefer continuations to go the same way as original
    unsigned hash = serverId.hash();
    hash = hashc((const unsigned char *) &uid, sizeof(uid), hash);
    hash += overflowSequence; // MORE - is this better than hashing?
    return hash;
}

void RoxiePacketHeader::copy(const RoxiePacketHeader &oh)
{
    // used for saving away info for later matching by match, without having to lock
    overflowSequence = oh.overflowSequence;
    continueSequence = oh.continueSequence;
    serverId = oh.serverId;
    channel = oh.channel;
    uid.store(oh.uid);
    // MORE - would it be safer, maybe even faster to copy the rest too?
}

void RoxiePacketHeader::clear()
{
    // used for saving away kill packets for later matching by match
    uid = RUID_NONE;  // Will never match a queued packet
}

bool RoxiePacketHeader::matchPacket(const RoxiePacketHeader &oh) const
{
    // used when matching up a kill packet against a pending one...
    // DO NOT compare activityId - they are not supposed to match, since 0 in activityid identifies ibyti!
    return
        oh.uid==uid &&
        (oh.overflowSequence & ~OUTOFBAND_SEQUENCE) == (overflowSequence & ~OUTOFBAND_SEQUENCE) &&
        oh.continueSequence == continueSequence &&
        oh.serverId==serverId &&
        oh.channel==channel;
}

void RoxiePacketHeader::init(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence)
{
    retries = 0;
    activityId = _remoteId.activityId;
    queryHash = _remoteId.queryHash;
    uid = _uid;
    serverId = myNode;
    channel = _channel;
    overflowSequence = _overflowSequence;
    continueSequence = 0;
    clearSubChannels();
    filler = 0; // keeps valgrind happy
}

void RoxiePacketHeader::clearSubChannels()
{
    for (unsigned idx = 0; idx < MAX_SUBCHANNEL; idx++)
        subChannels[idx].clear();
}

StringBuffer &RoxiePacketHeader::toString(StringBuffer &ret) const
{
    const IpAddress serverIP = serverId.getIpAddress();
    ret.append("activityId=");
    switch (activityId & ~ROXIE_PRIORITY_MASK)
    {
    case 0: ret.append("IBYTI"); break;
    case ROXIE_UNLOAD: ret.append("ROXIE_UNLOAD"); break;
    case ROXIE_TRACEINFO: ret.append("ROXIE_TRACEINFO"); break;
    case ROXIE_DEBUGREQUEST: ret.append("ROXIE_DEBUGREQUEST"); break;
    case ROXIE_DEBUGCALLBACK: ret.append("ROXIE_DEBUGCALLBACK"); break;
    case ROXIE_FILECALLBACK: ret.append("ROXIE_FILECALLBACK"); break;
    case ROXIE_ALIVE: ret.append("ROXIE_ALIVE"); break;
    case ROXIE_KEYEDLIMIT_EXCEEDED: ret.append("ROXIE_KEYEDLIMIT_EXCEEDED"); break;
    case ROXIE_LIMIT_EXCEEDED: ret.append("ROXIE_LIMIT_EXCEEDED"); break;
    case ROXIE_EXCEPTION: ret.append("ROXIE_EXCEPTION"); break;
    default:
        ret.appendf("%u", (activityId & ~(ROXIE_ACTIVITY_FETCH | ROXIE_PRIORITY_MASK)));
        if (activityId & ROXIE_ACTIVITY_FETCH)
            ret.appendf(" (fetch part)");
        break;
    }
    ret.appendf(" uid=" RUIDF " pri=", uid.load());
    switch (activityId & ROXIE_PRIORITY_MASK)
    {
        case ROXIE_SLA_PRIORITY: ret.append("SLA"); break;
        case ROXIE_HIGH_PRIORITY: ret.append("HIGH"); break;
        case ROXIE_LOW_PRIORITY: ret.append("LOW"); break;
        case ROXIE_BG_PRIORITY: ret.append("BG"); break;
        default: ret.append("???"); break;
    }
    ret.appendf(" queryHash=%" I64F "x ch=%u seq=%d cont=%d server=", queryHash, channel, overflowSequence, continueSequence);
    serverIP.getHostText(ret);
    if (retries)
    {
        if (retries==QUERY_ABORTED)
            ret.append(" retries=QUERY_ABORTED");
        else
        {
            if (retries & ROXIE_RETRIES_MASK)
                ret.appendf(" retries=%04x", retries);
            if (retries & ROXIE_FASTLANE)
                ret.appendf(" FASTLANE");
            if (retries & ROXIE_BROADCAST)
                ret.appendf(" BROADCAST");
        }
    }
    ret.append(" subchannels=");
    for (unsigned idx = 0; idx < MAX_SUBCHANNEL; idx++)
    {
        if (subChannels[idx].isNull())
            break;
        if (idx)
            ret.append(',');
        subChannels[idx].getTraceText(ret);
        if (subChannels[idx].isMe())
        {
            ret.append("(me)");
        }
    }
    return ret;
}

bool RoxiePacketHeader::allChannelsFailed()
{
    unsigned mask = (1 << (getNumAgents(channel) * SUBCHANNEL_BITS)) - 1;
    return (retries & mask) == mask;
}

bool RoxiePacketHeader::retry()
{
    bool worthRetrying = false;
    unsigned mask = SUBCHANNEL_MASK;
    unsigned numAgents = getNumAgents(channel);
    for (unsigned subChannel = 0; subChannel < numAgents; subChannel++)
    {
        unsigned subRetries = (retries & mask) >> (subChannel * SUBCHANNEL_BITS);
        if (subRetries != SUBCHANNEL_MASK)
            subRetries++;
        if (subRetries != SUBCHANNEL_MASK)
            worthRetrying = true;
        retries = (retries & ~mask) | (subRetries << (subChannel * SUBCHANNEL_BITS));
        mask <<= SUBCHANNEL_BITS;
    }
    return worthRetrying;
}

void RoxiePacketHeader::setException(unsigned subChannel)
{
    retries |= SUBCHANNEL_MASK << (SUBCHANNEL_BITS * subChannel);
}

unsigned RoxiePacketHeader::thisChannelRetries(unsigned subChannel)
{
    unsigned shift = SUBCHANNEL_BITS * subChannel;
    unsigned mask = SUBCHANNEL_MASK << shift;
    return (retries & mask) >> shift;
}

//============================================================================================

unsigned getReplicationLevel(unsigned channel)
{
    if (!channel)
        return 0;
    Owned<const ITopologyServer> topology = getTopology();
    return topology->queryChannelInfo(channel).replicationLevel();
}

static bool anyChannelHasReplicas()
{
    // In containerized environments, replicas are potentially dynamic - so always assume they may exist
    if (isContainerized())
        return true;

    // Check if any configured channel has more than one agent (replicas)
    // This is used to determine if IBYTI infrastructure is needed
    Owned<const ITopologyServer> topology = getTopology();
    const std::vector<unsigned> channels = topology->queryChannels();
    for (unsigned channel : channels)
    {
        if (channel != 0) // Skip channel 0 (broadcast channel)
        {
            const SocketEndpointArray &eps = topology->queryAgents(channel);
            if (eps.ordinality() > 1)
                return true;
        }
    }
    return false;
}

//============================================================================================

// A callback interface for processing Roxie worker requests
interface IRoxieWorkerRequestReceiver
{
    virtual void processMessage(const void * data, size32_t length) = 0;
};

interface IRoxieWorkerCommunicator : public IInterface
{
    virtual void startListening(IRoxieWorkerRequestReceiver & _receiver) = 0;
    virtual void stopListening() = 0;
    virtual size32_t queryMaxPacketSize() const = 0;
    virtual size32_t sendToWorker(const void * data, size32_t len, const SocketEndpoint &ep) = 0;
};


static Owned<IRoxieWorkerCommunicator> workerCommunicator;


//------------------------------------------------------------------------------------------------------------

class RoxieUdpWorkerCommunicator : public Thread, implements IRoxieWorkerCommunicator
{
public:
    IMPLEMENT_IINTERFACE_USING(Thread)

    RoxieUdpWorkerCommunicator() : Thread("RoxieUdpWorkerListener")
    {
        const char *desc = "UDP";
        workerRequestSocket.setown(ISocket::udp_create(ccdMulticastPort));
        workerRequestSocket->set_receive_buffer_size(udpMulticastBufferSize);
        size32_t actualSize = workerRequestSocket->get_receive_buffer_size();

        if (actualSize < udpMulticastBufferSize)
        {
            DBGLOG("Roxie: %s socket buffer size could not be set (requested=%d actual %d)", desc, udpMulticastBufferSize, actualSize);
            throwUnexpected();
        }
        if (doTrace(TraceFlags::Always))
            DBGLOG("Roxie: %s socket created port=%d sockbuffsize=%d actual %d", desc, ccdMulticastPort, udpMulticastBufferSize, actualSize);

        maxPacketSize = workerRequestSocket->get_max_send_size();
        if ((maxPacketSize==0) || (maxPacketSize>65535))
            maxPacketSize = 65535;
    }

    ~RoxieUdpWorkerCommunicator()
    {
        join();
    }

    int run()
    {
// Raise the priority so ibyti's get through in a timely fashion
#if defined( __linux__) || defined(__APPLE__) || defined(EMSCRIPTEN)
        setLinuxThreadPriority(3);
#else
        adjustPriority(1);
#endif
        if (traceLevel)
            DBGLOG("RoxieSocketQueueManager::run() starting: doIbytiDelay=%s minIbytiDelay=%u initIbytiDelay=%u",
                    doIbytiDelay?"YES":"NO", minIbytiDelay, initIbytiDelay);

        MemoryBuffer mb;
        for (;;)
        {
            mb.clear();
            try
            {
                // NOTE - this thread needs to do as little as possible - just read packets and queue them up - otherwise we can get packet loss due to buffer overflow
                // DO NOT put tracing on this thread except at very high tracelevels!
                void * buffer = mb.reserve(maxPacketSize);

                unsigned l;
                workerRequestSocket->readtms(buffer, sizeof(RoxiePacketHeader), maxPacketSize, l, defaultTimeout);

                receiver->processMessage(buffer, l);
            }
            catch (IException *E)
            {
                if (running)
                {
                    // MORE: Maybe we should utilize IException::errorCode - not just text ??
                    if (E->errorCode()==JSOCKERR_timeout_expired)
                        E->Release();
                    else
                    {
                        EXCLOG(E, "Exception reading or processing roxie packet");
                        E->Release();
                        // MORE: Protect with try logic, in case udp_create throws exception ?
                        //       What to do if create fails (ie exception is caught) ?
                        if (workerRequestSocket)
                        {
                            //This is not thread safe - what happens if a thread is sending at the same time that
                            //the socket is recreated?
                            workerRequestSocket->close();

                            Owned<ISocket> newWorkerRequestSocket = ISocket::udp_create(ccdMulticastPort);
                            newWorkerRequestSocket->set_receive_buffer_size(udpMulticastBufferSize);
                            workerRequestSocket.swap(newWorkerRequestSocket);
                        }
                    }
                }
                else
                {
                    E->Release();
                    break; // exit the processing loop
                }
            }
        }
        return 0;
    }

    virtual size32_t queryMaxPacketSize() const override
    {
        return maxPacketSize;
    }

    virtual void startListening(IRoxieWorkerRequestReceiver & _receiver) override
    {
        assertex(!running);
        receiver = &_receiver;
        running = true;
        start(false);
    }

    virtual void stopListening() override
    {
        if (running)
        {
            shutdownAndCloseNoThrow(workerRequestSocket);
            join();
            receiver = nullptr;
            running = false;
        }
    }

    virtual size32_t sendToWorker(const void * data, size32_t len, const SocketEndpoint &ep)
    {
        return workerRequestSocket->udp_write_to(ep, data, len);
    }

protected:
    Owned<ISocket> workerRequestSocket;
    IRoxieWorkerRequestReceiver * receiver{nullptr};
    size32_t maxPacketSize = 0;
    std::atomic<bool> running = { false };
    unsigned defaultTimeout = 5000;
};


//------------------------------------------------------------------------------------------------------------

class RoxieTcpListener : public CSocketConnectionListener
{
public:
    RoxieTcpListener(IRoxieWorkerRequestReceiver & _receiver)
     : CSocketConnectionListener(0, false, 0, 0), receiver(_receiver)
    {
    }

    virtual bool onlyProcessFirstRead() const override
    {
        return false;
    }

    virtual unsigned getMessageSize(const void * header) const override
    {
        const RoxiePacketHeader * packetHeader = (const RoxiePacketHeader *)header;
        return packetHeader->packetlength;
    }

    virtual CReadSocketHandler *createSocketHandler(ISocket *sock) override
    {
        //Header size is 64B, max variable to read is 64K
        size32_t maxInitialReadSize = 0x10000; // 64K
        return new CReadSocketHandler(*this, asyncReader, sock, sizeof(RoxiePacketHeader), maxInitialReadSize);
    }

    virtual void processMessage(const void * data, size32_t len) override
    {
        receiver.processMessage(data, len);
    }

protected:
    IRoxieWorkerRequestReceiver & receiver;
};

class RoxieTcpWorkerCommunicator : public CInterfaceOf<IRoxieWorkerCommunicator>
{
public:
    RoxieTcpWorkerCommunicator() : sender(true) {}

    virtual size32_t queryMaxPacketSize() const override
    {
        return maxPacketSize;
    }

    virtual void startListening(IRoxieWorkerRequestReceiver & _receiver) override
    {
        assertex(!running);
        running = true;
        listener.reset(new RoxieTcpListener(_receiver));
        listener->startPort(ccdMulticastPort);
    }

    virtual void stopListening() override
    {
        if (running)
        {
            listener->stop();
            listener.reset(nullptr);
            running = false;
        }
    }

    virtual size32_t sendToWorker(const void * data, size32_t len, const SocketEndpoint &ep)
    {
        CSocketTarget * sock = sender.queryWorkerSocket(ep);
        return sock->writeSync(data, len);
    }

protected:
    std::unique_ptr<RoxieTcpListener> listener;
    CTcpSender sender;
    size32_t maxPacketSize = 0x40000; // Allow up to 256KB.
    std::atomic<bool> running = { false };
};


//---------------------------------------------------------------------------------------------------------------------

void openWorkerRequestSocket()
{
    if (!workerCommunicator)
    {
        if (useTcpTransport)
            workerCommunicator.setown(new RoxieTcpWorkerCommunicator());
        else
            workerCommunicator.setown(new RoxieUdpWorkerCommunicator());
    }
}

void closeWorkerRequestSockets()
{
    workerCommunicator.clear();
}

static bool channelWrite(RoxiePacketHeader &buf, bool includeSelf)
{
    size32_t minwrote = 0;
    // In the containerized system, the list of subchannel IPs is captured in the packet header to ensure everyone is using the
    // same snapshot of the topology state.
    // If the subchannel IPs are not set, fill them in now. If they are set, use them.
    if (buf.subChannels[0].isNull())
    {
        Owned<const ITopologyServer> topo = getTopology();
        const SocketEndpointArray &eps = topo->queryAgents(buf.channel);
        if (!eps.ordinality())
            throw makeStringExceptionV(0, "No agents available for channel %d", buf.channel);
        if (buf.channel==0)
        {
            // Note that we expand any writes on channel 0 here, since we need to capture the server's view of what agents are on each channel
            bool allOk = true;
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer header;
                DBGLOG("Translating packet sent to channel 0: %s", buf.toString(header).str());
            }
            for (unsigned channel = 0; channel < numChannels; channel++)
            {
                buf.channel = channel+1;
                if (!channelWrite(buf, true))
                    allOk = false;
                buf.clearSubChannels();
            }
            buf.channel = 0;
            return allOk;
        }

        unsigned hdrHashVal = buf.priorityHash();
        unsigned numAgents = eps.ordinality();
        unsigned subChannel = (hdrHashVal % numAgents);

        for (unsigned idx = 0; idx < MAX_SUBCHANNEL; idx++)
        {
            if (idx == numAgents)
                break;
            buf.subChannels[idx].setIp(eps.item(subChannel));
            subChannel++;
            if (subChannel == numAgents)
                subChannel = 0;
        }
    }
    else
    {
        assert(buf.channel != 0);
    }
    for (unsigned subChannel = 0; subChannel < MAX_SUBCHANNEL; subChannel++)
    {
        if (buf.subChannels[subChannel].isNull())
            break;
        if (includeSelf || !buf.subChannels[subChannel].isMe())
        {
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer s, header;
                DBGLOG("Writing %d bytes to subchannel %d (%s) %s", buf.packetlength, subChannel, buf.subChannels[subChannel].getTraceText(s).str(), buf.toString(header).str());
            }
            SocketEndpoint ep(ccdMulticastPort, buf.subChannels[subChannel].getIpAddress());
            size32_t wrote = workerCommunicator->sendToWorker(&buf, buf.packetlength, ep);
            if (!subChannel || wrote < minwrote)
                minwrote = wrote;
            if (delaySubchannelPackets)
                MilliSleep(100);
        }
        else if (doTrace(traceRoxiePackets))
        {
            StringBuffer s, header;
            DBGLOG("NOT writing %d bytes to subchannel %d (%s) %s", buf.packetlength, subChannel, buf.subChannels[subChannel].getTraceText(s).str(), buf.toString(header).str());
        }
    }

    return minwrote==buf.packetlength;
}

//============================================================================================

class CRoxieQueryPacketBase : public CInterface
{
protected:
    RoxiePacketHeader *data;
    const byte *traceInfo;
    unsigned traceLength;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryPacketBase(const void *_data, int lengthRemaining, bool ownData) : data((RoxiePacketHeader *) _data)
    {
        if (!ownData)
        {
            data = (RoxiePacketHeader *) malloc(lengthRemaining);
            memcpy(data, _data, lengthRemaining);
        }
        assertex(lengthRemaining >= (int) sizeof(RoxiePacketHeader));
        data->packetlength = lengthRemaining;
        const byte *finger = (const byte *) (data + 1);
        lengthRemaining -= sizeof(RoxiePacketHeader);
        if (data->activityId == ROXIE_FILECALLBACK || data->activityId == ROXIE_DEBUGCALLBACK || data->retries == QUERY_ABORTED)
        {
            traceInfo = NULL;
            traceLength = 0;
        }
        else
        {
            assertex(lengthRemaining > 1);

            traceInfo = finger;
            const byte * endTraceInfo = finger + lengthRemaining;
            byte traceFlags = *finger++;
            if (traceFlags & LOGGING_TRACELEVELSET)
                finger++;
            if (traceFlags & LOGGING_DEBUGGERACTIVE)
            {
                assertex(finger + sizeof(unsigned short) <= endTraceInfo);
                unsigned short debugLen = *(unsigned short *) finger;
                assertex(finger + sizeof(unsigned short) + debugLen <= endTraceInfo);
                finger += debugLen + sizeof(unsigned short);
            }
            if (traceFlags & LOGGING_TRACEID)
            {
                assertex(finger + bytesTraceId + bytesSpanId <= endTraceInfo);
                finger += bytesTraceId + bytesSpanId;
            }

            for (;;)
            {
                assertex(finger != endTraceInfo);
                byte next = *finger++;
                if (next == 0)
                    break;
            }
            traceLength = finger - traceInfo;
            lengthRemaining = endTraceInfo - finger;
        }
    }

    ~CRoxieQueryPacketBase()
    {
        free(data);
    }

};

class CRoxieQueryPacket : public CRoxieQueryPacketBase, implements IRoxieQueryPacket
{
protected:
    const byte *continuationData = nullptr;
    unsigned continuationLength = 0;
    const byte *smartStepInfoData = nullptr;
    unsigned smartStepInfoLength = 0;
    const byte *contextData = nullptr;
    unsigned contextLength = 0;
    std::atomic<unsigned> timeFirstSent = 0;
    std::atomic<bool> acknowledged = false;
    unsigned resends = 0;
    
public:
    IMPLEMENT_IINTERFACE;
    CRoxieQueryPacket(const void *_data, int length, bool ownData) : CRoxieQueryPacketBase(_data, length, ownData)
    {
        const byte *finger = (const byte *) (data + 1) + traceLength;
        int lengthRemaining = length - sizeof(RoxiePacketHeader) - traceLength;
        if (data->activityId == ROXIE_FILECALLBACK || data->activityId == ROXIE_DEBUGCALLBACK || data->retries == QUERY_ABORTED)
        {
            continuationData = NULL;
            continuationLength = 0;
            smartStepInfoData = NULL;
            smartStepInfoLength = 0;
        }
        else
        {
            if (data->continueSequence & ~CONTINUE_SEQUENCE_SKIPTO)
            {
                assertex(lengthRemaining >= (int) sizeof(unsigned));
                continuationLength = *(unsigned *) finger;
                continuationData = finger + sizeof(unsigned);
                finger = continuationData + continuationLength;
                lengthRemaining -= continuationLength + sizeof(unsigned);
            }
            if (data->continueSequence & CONTINUE_SEQUENCE_SKIPTO)
            {
                assertex(lengthRemaining >= (int) sizeof(unsigned));
                smartStepInfoLength = *(unsigned *) finger;
                smartStepInfoData = finger + sizeof(unsigned);
                finger = smartStepInfoData + smartStepInfoLength;
                lengthRemaining -= smartStepInfoLength + sizeof(unsigned);
            }
        }
        assertex(lengthRemaining >= 0);
        contextData = finger;
        contextLength = lengthRemaining;
        noteTimeSent();
    }

    virtual RoxiePacketHeader &queryHeader() const
    {
        return  *data;
    }

    virtual const byte *queryTraceInfo() const
    {
        return traceInfo;
    }

    virtual unsigned getTraceLength() const
    {
        return traceLength;
    }

    virtual const void *queryContinuationData() const
    {
        return continuationData;
    }

    virtual unsigned getContinuationLength() const
    {
        return continuationLength;
    }

    virtual const byte *querySmartStepInfoData() const
    {
        return smartStepInfoData;
    }

    virtual unsigned getSmartStepInfoLength() const
    {
        return smartStepInfoLength;
    }

    virtual const void *queryContextData() const
    {
        return contextData;
    }

    virtual unsigned getContextLength() const
    {
        return contextLength;
    }

    virtual IRoxieQueryPacket *clonePacket(unsigned channel) const
    {
        unsigned length = data->packetlength;
        RoxiePacketHeader *newdata = (RoxiePacketHeader *) malloc(length);
        memcpy(newdata, data, length);
        newdata->channel = channel;
        newdata->retries |= ROXIE_BROADCAST;
        return createRoxiePacket(newdata, length, true);
    }

    virtual IRoxieQueryPacket *insertSkipData(size32_t skipDataLen, const void *skipData) const
    {
        assertex((data->continueSequence & CONTINUE_SEQUENCE_SKIPTO) == 0); // Should not already be any skipto info in the source packet

        unsigned newDataSize = data->packetlength + sizeof(unsigned) + skipDataLen;
        char *newdata = (char *) malloc(newDataSize);
        unsigned headSize = sizeof(RoxiePacketHeader);
        if (traceLength)
            headSize += traceLength;
        if (data->continueSequence & ~CONTINUE_SEQUENCE_SKIPTO)
            headSize += sizeof(unsigned) + continuationLength;
        memcpy(newdata, data, headSize); // copy in leading part of old data
        ((RoxiePacketHeader *) newdata)->continueSequence |= CONTINUE_SEQUENCE_SKIPTO; // set flag indicating new data is present
        *(unsigned *) (newdata + headSize) = skipDataLen; // add length field for new data
        memcpy(newdata + headSize + sizeof(unsigned), skipData, skipDataLen); // copy in new data
        memcpy(newdata + headSize + sizeof(unsigned) + skipDataLen, ((char *) data) + headSize, data->packetlength - headSize); // copy in remaining old data
        return createRoxiePacket(newdata, newDataSize, true);
    }

    virtual ISerializedRoxieQueryPacket *serialize() const override
    {
        unsigned length = data->packetlength;
        if (!encryptInTransit)
            return createSerializedRoxiePacket(data, length, false);

        MemoryBuffer mb;
        const byte *plainData = (const byte *) (data+1);
        plainData += traceLength;
        unsigned plainLen = length - sizeof(RoxiePacketHeader) - traceLength;
        mb.append(sizeof(RoxiePacketHeader)+traceLength, data);  // Header and traceInfo are unencrypted
        const MemoryAttr &udpkey = getSecretUdpKey(true);
        aesEncrypt(udpkey.get(), udpkey.length(), plainData, plainLen, mb);   // Encrypt everything else
        RoxiePacketHeader *newHeader = (RoxiePacketHeader *) mb.toByteArray();
        newHeader->packetlength = mb.length();
        return createSerializedRoxiePacket(mb);
    }

    virtual void noteTimeSent() override
    {
        acknowledged = false;
        timeFirstSent = msTick();
        if (!timeFirstSent)
            timeFirstSent = 1;
    }

    virtual void setAcknowledged() override
    {
        acknowledged = true;
    }

    virtual void clearAcknowledged() override
    {
        acknowledged = false;
    }

    virtual bool isAcknowledged() const override
    {
        return acknowledged;
    }

    virtual bool resendNeeded(unsigned now) override
    {
        bool ret = timeFirstSent && !acknowledged && now-timeFirstSent > packetAcknowledgeTimeout*(resends+1);
        if (ret)
            resends++;
        return ret;
    }
};

// CNocryptRoxieQueryPacket implements both serialized and deserialized packet interfaces, to avoid additional copy operations when
// using localAgent mode.

class CNocryptRoxieQueryPacket: public CRoxieQueryPacket, implements ISerializedRoxieQueryPacket
{
    unsigned __int64 enqueuedTime = 0;
public:
    IMPLEMENT_IINTERFACE;
    CNocryptRoxieQueryPacket(const void *_data, int length, bool ownData) : CRoxieQueryPacket(_data, length, ownData)
    {
    }

    virtual RoxiePacketHeader &queryHeader() const
    {
        return CRoxieQueryPacket::queryHeader();
    }

    virtual const byte *queryTraceInfo() const
    {
        return traceInfo;
    }

    virtual unsigned getTraceLength() const
    {
        return traceLength;
    }

    virtual ISerializedRoxieQueryPacket *cloneSerializedPacket(unsigned channel) const
    {
        unsigned length = data->packetlength;
        RoxiePacketHeader *newdata = (RoxiePacketHeader *) malloc(length);
        memcpy(newdata, data, length);
        newdata->channel = channel;
        newdata->retries |= ROXIE_BROADCAST;
        return new CNocryptRoxieQueryPacket(newdata, length, true);
    }

    virtual ISerializedRoxieQueryPacket *serialize() const override
    {
        return const_cast<CNocryptRoxieQueryPacket *>(LINK(this));
    }

    virtual IRoxieQueryPacket *deserialize() const override
    {
        return const_cast<CNocryptRoxieQueryPacket *>(LINK(this));
    }

    virtual unsigned __int64 queryIBYTIDelayTime() const override { return 0; }
    virtual unsigned __int64 queryEnqueuedTimeStamp() const override { return enqueuedTime; }
    virtual void noteQueued(unsigned __int64 _IBYTIdelay) override
    {
        enqueuedTime = nsTick();
    }
};

class CSerializedRoxieQueryPacket : public CRoxieQueryPacketBase, implements ISerializedRoxieQueryPacket
{
    unsigned __int64 IBYTIdelay = 0;
    unsigned __int64 enqueuedTime = 0;
public:
    IMPLEMENT_IINTERFACE;
    CSerializedRoxieQueryPacket(const void *_data, int length, bool ownData) : CRoxieQueryPacketBase(_data, length, ownData)
    {
    }

    virtual RoxiePacketHeader &queryHeader() const
    {
        return  *data;
    }

    virtual const byte *queryTraceInfo() const
    {
        return traceInfo;
    }

    virtual unsigned getTraceLength() const
    {
        return traceLength;
    }

    virtual ISerializedRoxieQueryPacket *cloneSerializedPacket(unsigned channel) const
    {
        unsigned length = data->packetlength;
        RoxiePacketHeader *newdata = (RoxiePacketHeader *) malloc(length);
        memcpy(newdata, data, length);
        newdata->channel = channel;
        newdata->retries |= ROXIE_BROADCAST;
        return new CSerializedRoxieQueryPacket(newdata, length, true);
    }

    virtual IRoxieQueryPacket *deserialize() const override
    {
        unsigned length = data->packetlength;
        MemoryBuffer mb;
        if (encryptInTransit)
        {
            const byte *encryptedData = (const byte *) (data+1);
            encryptedData += traceLength;
            unsigned encryptedLen = length - sizeof(RoxiePacketHeader) - traceLength;
            mb.append(sizeof(RoxiePacketHeader)+traceLength, data);         // Header and traceInfo are unencrypted
            const MemoryAttr &udpkey = getSecretUdpKey(true);
            aesDecrypt(udpkey.get(), udpkey.length(), encryptedData, encryptedLen, mb);  // Decrypt everything else
            RoxiePacketHeader *newHeader = (RoxiePacketHeader *) mb.toByteArray();
            newHeader->packetlength = mb.length();
        }
        else
        {
            mb.append(length, data);
        }
        return createRoxiePacket(mb);
    }
    virtual unsigned __int64 queryIBYTIDelayTime() const override { return IBYTIdelay; }
    virtual unsigned __int64 queryEnqueuedTimeStamp() const override { return enqueuedTime; }
    virtual void noteQueued(unsigned __int64 _IBYTIdelay) override
    {
        IBYTIdelay = _IBYTIdelay;
        enqueuedTime = nsTick();
    }

};

extern IRoxieQueryPacket *createRoxiePacket(void *_data, unsigned _len, bool ownData)
{
    if (!encryptInTransit)
        return new CNocryptRoxieQueryPacket(_data, _len, ownData);
    if ((unsigned short)_len != _len)
    {
        StringBuffer s;
        RoxiePacketHeader *header = (RoxiePacketHeader *) _data;
        header->toString(s);
        if (ownData)
            free(_data);
        throw MakeStringException(ROXIE_PACKET_ERROR, "Packet length %d exceeded maximum sending packet %s", _len, s.str());
    }
    return new CRoxieQueryPacket(_data, _len, ownData);
}

extern IRoxieQueryPacket *createRoxiePacket(MemoryBuffer &m)
{
    unsigned length = m.length(); // don't make assumptions about evaluation order of parameters...
    return createRoxiePacket(m.detachOwn(), length, true);
}

static IRoxieQueryPacket *deserializeCallbackPacket(const void * data, size32_t length)
{
    // Direct decryption of special packets - others are only decrypted after being dequeued
    if (encryptInTransit)
    {
        RoxiePacketHeader *header = (RoxiePacketHeader *) data;
        assertex(header != nullptr);
        assertex(header->activityId == ROXIE_FILECALLBACK || header->activityId == ROXIE_DEBUGCALLBACK);
        assertex(length >= header->packetlength);
        unsigned encryptedLen = header->packetlength - sizeof(RoxiePacketHeader);
        const void *encryptedData = (const void *)(header+1);
        const MemoryAttr &udpkey = getSecretUdpKey(true);

        MemoryBuffer decrypted;
        decrypted.ensureCapacity(length);       // May be up to 16 bytes smaller...
        decrypted.append(sizeof(RoxiePacketHeader), header);
        aesDecrypt(udpkey.get(), udpkey.length(), encryptedData, encryptedLen, decrypted);
        unsigned length = decrypted.length();
        RoxiePacketHeader *newHeader = (RoxiePacketHeader *) decrypted.detachOwn();
        newHeader->packetlength = length;
        return createRoxiePacket(newHeader, length, true);
    }
    else
    {
        return createRoxiePacket((void*)data, length, false);
    }
}

extern ISerializedRoxieQueryPacket *createSerializedRoxiePacket(void * data, unsigned length, bool ownData)
{
    if (encryptInTransit)
        return new CSerializedRoxieQueryPacket(data, length, ownData);
    else
        return new CNocryptRoxieQueryPacket(data, length, ownData);
}

extern ISerializedRoxieQueryPacket *createSerializedRoxiePacket(MemoryBuffer &m)
{
    unsigned length = m.length(); // don't make assumptions about evaluation order of parameters...
    if (encryptInTransit)
        return new CSerializedRoxieQueryPacket(m.detachOwn(), length, true);
    else
        return new CNocryptRoxieQueryPacket(m.detachOwn(), length, true);
}

//=================================================================================

AgentContextLogger::AgentContextLogger()
{
    GetHostIp(ip);
    set(NULL);
}

AgentContextLogger::AgentContextLogger(ISerializedRoxieQueryPacket *packet)
{
    GetHostIp(ip);
    set(packet);
}

void AgentContextLogger::set(ISerializedRoxieQueryPacket *packet)
{
    anyOutput = false;
    intercept = false;
    debuggerActive = false;
    checkingHeap = false;
    aborted = false;
    stats.reset();
    start = msTick();
    if (packet)
    {
        CriticalBlock b(crit); // Why?
        RoxiePacketHeader &header = packet->queryHeader();
        const byte *traceInfo = packet->queryTraceInfo();
        StringBuffer s;
        if (traceInfo)
        {
            //Extract the extra meta information serialized in queryChannelBuffer() in ccdserver.cpp
            const byte * endTraceInfo = traceInfo + packet->getTraceLength();
            unsigned char loggingFlags = *traceInfo++;
            if (loggingFlags & LOGGING_INTERCEPTED)
                intercept = true;
            if (loggingFlags & LOGGING_TRACELEVELSET)
            {
                // legacy: avoid null byte here in case anyone still thinks there's just a null-terminated string
                ctxTraceLevel = (*traceInfo++ - 1);
            }
            if (loggingFlags & LOGGING_BLIND)
                blind = true;
            if (loggingFlags & LOGGING_CHECKINGHEAP)
                checkingHeap = true;
            if (loggingFlags & LOGGING_DEBUGGERACTIVE)
            {
                assertex(traceInfo + sizeof(unsigned short) <= endTraceInfo);
                debuggerActive = true;
                unsigned short debugLen = *(unsigned short *) traceInfo;
                assertex(traceInfo + sizeof(unsigned short) + debugLen <= endTraceInfo);
                traceInfo += debugLen + sizeof(unsigned short);
            }
            if (loggingFlags & LOGGING_TRACEID)
            {
                assertex(traceInfo + bytesTraceId + bytesSpanId <= endTraceInfo);

                char traceId[lenTraceId+1];
                convertDataToHex(lenTraceId, traceId, bytesTraceId, (const char *) traceInfo);
                traceId[lenTraceId] = 0;
                traceInfo += bytesTraceId;

                char spanId[lenSpanId+1];
                convertDataToHex(lenSpanId, spanId, bytesSpanId, (const char *) traceInfo);
                spanId[lenSpanId] = 0;
                traceInfo += bytesSpanId;

                agentSpan.setown(createPseudoSpan(traceId, spanId));
                setActiveSpan(agentSpan);
            }

            // Passing the wuid via the logging context prefix is a lot of a hack...
            if (loggingFlags & LOGGING_WUID)
            {
                unsigned wuidLen = 0;
                while (traceInfo + wuidLen < endTraceInfo)
                {
                    if (traceInfo[wuidLen]=='@'||traceInfo[wuidLen]==':')
                        break;
                    wuidLen++;
                }
                wuid.set((const char *) traceInfo, wuidLen);
            }

            s.append(endTraceInfo - traceInfo, (const char *) traceInfo);
            s.append("|");
        }
        channel = header.channel;
        ip.getHostText(s);
        s.append(':').append(channel);
        StringContextLogger::set(s.str());
        if (intercept || mergeAgentStatistics)
        {
            RoxiePacketHeader newHeader(header, ROXIE_TRACEINFO, 0);  // subchannel not relevant
            output.setown(ROQ->createOutputStream(newHeader, false, *this));
        }
    }
    else
    {
        StringContextLogger::set("");
        channel = 0;
        agentSpan.clear();
        setActiveSpan(nullptr);
    }
}


void AgentContextLogger::putStatProcessed(unsigned subGraphId, unsigned actId, unsigned idx, unsigned processed, unsigned strands) const
{
    if (output && mergeAgentStatistics)
    {
        MemoryBuffer buf;
        buf.append((char) LOG_CHILDCOUNT); // A special log entry for the stats
        buf.append(subGraphId);
        buf.append(actId);
        buf.append(idx);
        buf.append(processed);
        buf.append(strands);
    }
}

void AgentContextLogger::putStats(unsigned subGraphId, unsigned actId, const CRuntimeStatisticCollection &stats) const
{
    if (output && mergeAgentStatistics)
    {
        MemoryBuffer buf;
        buf.append((char) LOG_CHILDSTATS); // A special log entry for the stats
        buf.append(subGraphId);
        buf.append(actId);
        if (stats.serialize(buf))
        {
            unsigned len = buf.length();
            void *ret = output->getBuffer(len, true);
            memcpy(ret, buf.toByteArray(), len);
            output->putBuffer(ret, len, true);
            anyOutput = true;
        }
    }
}

void AgentContextLogger::flush()
{
    if (output)
    {
        CriticalBlock b(crit);
        if (mergeAgentStatistics)
        {
            MemoryBuffer buf;
            buf.append((char) LOG_STATVALUES); // A special log entry for the stats
            if (stats.serialize(buf))
            {
                unsigned len = buf.length();
                void *ret = output->getBuffer(len, true);
                memcpy(ret, buf.toByteArray(), len);
                output->putBuffer(ret, len, true);
                anyOutput = true;
            }
        }
        ForEachItemIn(idx, log)
        {
            MemoryBuffer buf;
            LogItem &logItem = log.item(idx);
            logItem.serialize(buf);
            unsigned len = buf.length();
            void *ret = output->getBuffer(len, true);
            memcpy(ret, buf.toByteArray(), len);
            output->putBuffer(ret, len, true);
            anyOutput = true;
        }
        log.kill();
        if (anyOutput)
            output->flush();
         output.clear();
    }
}


//=================================================================================

static SpinLock onDemandQueriesCrit;
static MapXToMyClass<hash64_t, hash64_t, IQueryFactory> onDemandQueryCache;

void sendUnloadMessage(hash64_t hash, const char *id, const IRoxieContextLogger &logctx)
{
    RemoteActivityId unloadId(ROXIE_UNLOAD, hash);
    RoxiePacketHeader header(unloadId, RUID_DISCARD, 0, 0);

    MemoryBuffer mb;
    mb.append(sizeof(RoxiePacketHeader), &header);
    mb.append((char) LOGGING_NONE);
    mb.append(id);
    if (doTrace(traceRoxiePackets))
        DBGLOG("UNLOAD sent for query %s", id);
    Owned<IRoxieQueryPacket> packet = createRoxiePacket(mb);
    ROQ->sendPacket(packet, logctx);
}

void doUnload(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx)
{
    const RoxiePacketHeader &header = packet->queryHeader();
    unsigned channelNo = header.channel;
    if (logctx.queryTraceLevel())
        logctx.CTXLOG("Unload received for channel %d", channelNo);
    hash64_t hashValue = header.queryHash;
    hashValue = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
    SpinBlock b(onDemandQueriesCrit);
    onDemandQueryCache.remove(hashValue);
}

void cacheOnDemandQuery(hash64_t hashValue, unsigned channelNo, IQueryFactory *query)
{
    hashValue = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
    SpinBlock b(onDemandQueriesCrit);
    onDemandQueryCache.setValue(hashValue, query);
}

//=================================================================================

struct PingRecord
{
    unsigned tick;
    IpAddress senderIP;
    unsigned __int64 currentTopoHash;
    unsigned __int64 originalTopoHash;
};

//=================================================================================


class IBYTIbuffer
{
    // This class is used to track a finite set of recently-received IBYTI messages, that may have arrived before the messages they refer to
    // It is accessed ONLY from the main reader thread and as such does not need to be threadsafe (but does need to be fast).
    // We use a circular buffer, and don't bother removing anything (just treat old items as expired). If the buffer overflows we will end up
    // discarding the oldest tracked orphaned IBYTI - but that's ok, no worse than if we hadn't tracked them at all.
public:
    IBYTIbuffer(unsigned _numOrphans) : numOrphans(_numOrphans)
    {
        assertex(numOrphans);
        orphans = new RoxiePacketHeader[numOrphans];
        tail = 0;
    }
    ~IBYTIbuffer()
    {
        delete [] orphans;
    }
    void noteOrphan(const RoxiePacketHeader &hdr)
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing

        unsigned now = msTick();
        // We could trace that the buffer may be too small, if (orphans[tail].activityId >= now)
        orphans[tail].copy(hdr);
        orphans[tail].activityId = now + IBYTIbufferLifetime;
        tail++;
        if (tail == numOrphans)
            tail = 0;
    }
    bool lookup(const RoxiePacketHeader &hdr) const
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing

        unsigned now = msTick();
        unsigned lookat = tail;
        do
        {
            if (!lookat)
                lookat = numOrphans;
            lookat--;
            if ((int) (orphans[lookat].activityId - now) < 0)   // Watch out for wrapping
                break;    // expired;
            if (orphans[lookat].matchPacket(hdr))
                return true;
        } while (lookat != tail);
        return false;
    }
private:
    RoxiePacketHeader *orphans = nullptr;
    unsigned tail = 0;
    unsigned numOrphans = 0;
};

//=================================================================================
//
// RoxieQueue - holds pending transactions on a roxie agent

class RoxieQueue : public CInterface, implements IThreadFactory
{
    Owned <IThreadPool> workers;
    QueueOf<ISerializedRoxieQueryPacket, true> waiting;
    Semaphore available;
    CriticalSection availCrit;    // Semaphore post may be slow with a lot of waiters - this crit may be used to limit to a single waiter
    CriticalSection qcrit;
    unsigned numWorkers;
    RelaxedAtomic<unsigned> started;
    std::atomic<unsigned> idle;
    IBYTIbuffer *myIBYTIbuffer = nullptr;

    void noteQueued()
    {
        // NOTE - there is a small race condition here - if idle is 1 but two enqueue's happen
        // close enough together that the signal has not yet caused idle to come back down to zero, then the
        // desired new thread may not be created. It's unlikely, and it's benign in that the query is still
        // processed and the thread will be created next time the HWM is reached.
        if (started < numWorkers && idle==0)
        {
            workers->start(this);
            started++;
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    RoxieQueue(unsigned _numWorkers, const char *qname, bool enableIBYTI)
    {
        numWorkers = _numWorkers;
        StringBuffer tname("RoxieWorkers");
        if (qname && *qname)
            tname.appendf(" (%s)", qname);
        workers.setown(createThreadPool(tname.str(), this, false, nullptr, numWorkers));
        if (traceThreadStartDelay)
            workers->setStartDelayTracing(60);
        if (qname && *qname)
        {
            if (streq(qname, "BG"))
                workers->setNiceValue(adjustBGThreadNiceValue);
        }
        started = 0;
        idle = 0;
        // Only allocate IBYTIbuffer if we have replicas - otherwise it's wasted memory
        if (IBYTIbufferSize && enableIBYTI)
            myIBYTIbuffer = new IBYTIbuffer(IBYTIbufferSize);
    }

    ~RoxieQueue()
    {
        delete myIBYTIbuffer;
    }


    virtual IPooledThread *createNew();

    void start()
    {
        if (prestartAgentThreads)
        {
            while (started < numWorkers)
            {
                workers->start(this);
                started++;
            }
        }
    }

    IPooledThreadIterator *running()
    {
        return workers->running();
    }

    void stopAll()
    {
        workers->stopAll(true);
        signal(workers->runningCount());
    }

    void join()
    {
        workers->joinAll(true);
        workers.clear();  // Breaks a cyclic reference count that would stop us from releasing RoxieReceiverThread otherwise
    }

    void enqueue(ISerializedRoxieQueryPacket *x, unsigned __int64 IBYTIdelay)
    {
        {
            CriticalBlock qc(qcrit);
            x->noteQueued(IBYTIdelay);
            waiting.enqueue(x);
        }
        noteQueued();
        available.signal();
    }

    void enqueueUnique(ISerializedRoxieQueryPacket *x, unsigned subChannel, unsigned __int64 IBYTIdelay)
    {
        RoxiePacketHeader &header = x->queryHeader();
        bool found = false;
        {
            CriticalBlock qc(qcrit);
            unsigned len = waiting.ordinality();
            unsigned i;
            for (i = 0; i < len; i++)
            {
                ISerializedRoxieQueryPacket *queued = waiting.item(i);
                if (queued && queued->queryHeader().matchPacket(header))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                x->noteQueued(IBYTIdelay);
                waiting.enqueue(x);
            }
        }
        if (found)
        {
            if (traceLevel)
            {
                StringBuffer xx;
                AgentContextLogger l(x);
                l.CTXLOG("Ignored retry on subchannel %u for queued activity %s", subChannel, header.toString(xx).str());
            }
            x->Release();
        }
        else
        {
            available.signal();
            noteQueued();
            if (doTrace(traceIBYTI, TraceFlags::Max))
            {
                AgentContextLogger l(x);
                StringBuffer xx; 
                l.CTXLOG("enqueued %s", header.toString(xx).str());
            }
        }
    }

    bool remove(RoxiePacketHeader &x)
    {
        ISerializedRoxieQueryPacket *found = nullptr;
        {
            CriticalBlock qc(qcrit);
            unsigned len = waiting.ordinality();
            unsigned i;
            for (i = 0; i < len; i++)
            {
                ISerializedRoxieQueryPacket *queued = waiting.item(i);
                if (queued)
                {
                    if (queued->queryHeader().matchPacket(x))
                    {
                        waiting.set(i, NULL);
                        found = queued;
                        break;
                    }
                }
            }
        }
        if (found)
        {
#ifdef _DEBUG
            RoxiePacketHeader &header = found->queryHeader();
            AgentContextLogger l(found);
            StringBuffer xx;
            l.CTXLOG("discarded %s", header.toString(xx).str());
#endif
            found->Release();
            return true;
        }
        else
            return false;
    }

    void wait()
    {
        idle++;
        {
            CLeavableCriticalBlock b(availCrit, limitWaitingWorkers);
            available.wait();
        }
        idle--;
    }

    void signal(unsigned num)
    {
        available.signal(num);
    }

    ISerializedRoxieQueryPacket *dequeue()
    {
        CriticalBlock qc(qcrit);
        return waiting.dequeue();
    }

    void noteOrphanIBYTI(const RoxiePacketHeader &hdr)
    {
        if (myIBYTIbuffer)
            myIBYTIbuffer->noteOrphan(hdr);
    }

    bool lookupOrphanIBYTI(const RoxiePacketHeader &hdr) const
    {
        if (myIBYTIbuffer)
            return myIBYTIbuffer->lookup(hdr);
        else
            return false;
    }
};

class CRoxieWorker : public CInterface, implements IPooledThread
{
    RoxieQueue *queue;
    CriticalSection actCrit;
    std::atomic<bool> stopped;
    std::atomic<bool> abortLaunch;
    std::atomic<bool> workerThreadBusy;
    Owned<IRoxieAgentActivity> activity;
    Owned<IRoxieQueryPacket> packet;
    AgentContextLogger logctx;
    RoxiePacketHeader packetHeader;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieWorker()
    {
        queue = NULL;
        stopped = false;
        workerThreadBusy = false;
        abortLaunch = false;
    }
    virtual void init(void *_r) override
    {
        queue = (RoxieQueue *) _r;
        stopped = false;
        workerThreadBusy = false;
        abortLaunch = false;
    }
    virtual bool canReuse() const override
    {
        return true;
    }
    virtual bool stop() override
    {
        stopped = true;
        return true; 
    }
    inline void setActivity(IRoxieAgentActivity *act)
    {
        Owned<IRoxieAgentActivity> temp(act);
        {
            CriticalBlock b(actCrit);
            activity.swap(temp);
        }
    }
    inline void setPacket(IRoxieQueryPacket *p)
    {
        Owned<IRoxieQueryPacket> temp(p);
        CriticalBlock b(actCrit);
        if (p)
        {
            packet.swap(temp);
            packetHeader.copy(p->queryHeader());
        }
        else
        {
            packetHeader.clear();
            packet.swap(temp);
        }
    }
    inline bool match(RoxiePacketHeader &h)
    {
        // There is a window between getting packet from queue and being able to match it. 
        // This could cause some deduping to fail, but it does not matter if it does (so long as it is rare!)
        return packetHeader.matchPacket(h);
    }

    bool checkAbort(RoxiePacketHeader &h, bool checkRank, bool &queryFound, bool &preActivity)
    {
        if (packetHeader.matchPacket(h))
        {
            CriticalBlock b(actCrit);
            if (!packetHeader.matchPacket(h))
                return false;
            queryFound = true;
            abortLaunch = true;
            if (activity) 
            {
                // Try to stop/abort a job after it starts only if IBYTI comes from a higher priority agent 
                // (more primary in the rank). The agents with higher rank will hold the lower bits of the retries field in IBYTI packet).
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer x;
                    DBGLOG("Deciding whether to abort: checkRank=%um respondingSub=%u, mySub=%u", checkRank, h.getRespondingSubChannel(), h.mySubChannel());
                }
                if (!checkRank || h.getRespondingSubChannel() < h.mySubChannel())
                {
                    if (doTrace(traceRoxiePackets))
                        DBGLOG("Decided to abort running activity based on ranks");
                    activity->abort();
                    return true;
                }
                else 
                {
                    return false;
                }
            }
            else if (workerThreadBusy)  // packet set and workerThreadBusy false should never happen...
            {
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer x;
                    DBGLOG("Decided to abort: preactivity");
                }
                preActivity = true;
                // NOTE - it's possible for this to happen after the activity is actually committed to starting even though the activity is not set
                // In such cases we can and should let it continue (because in such cases I have already sent an IBYTI by this point, I may have caused buddy to abort)
                // We will count (and trace) this as a successful IBYTI even if it is actually potentially too late, but that does not really matter.
                return true;
            }
        }
        return false;
    }

    void throwRemoteException(IException *E, IRoxieAgentActivity *activity, IRoxieQueryPacket *packet, bool isUser)
    {
        try 
        {
            if (activity && (doTrace(traceRoxiePackets)))
            {
                StringBuffer act;
                activity->toString(act);
                logctx.CTXLOG("throwRemoteException, activity %s, isUser=%d", act.str(), (int) isUser);
                if (!isUser)
                    EXCLOG(E, "throwRemoteException");
            }
            
            RoxiePacketHeader &header = packet->queryHeader();
            unsigned mySubChannel = header.mySubChannel();
            // I failed to do the query, but already sent out IBYTI - resend it so someone else can try
            if (!isUser)
            {
                StringBuffer s;
                s.append("Exception in agent for packet ");
                header.toString(s);
                logctx.logOperatorException(E, NULL, 0, "%s", s.str());
                header.setException(mySubChannel);
                if (!header.allChannelsFailed() && !localAgent)
                {
                    if (doTrace(traceRoxiePackets)) 
                        logctx.CTXLOG("resending packet from agent in case others want to try it");
                    ROQ->sendPacket(packet, logctx);
                }
            }
            RoxiePacketHeader newHeader(header, ROXIE_EXCEPTION, mySubChannel);
            if (isUser)
                newHeader.retries = (unsigned short) -1;
            Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
            StringBuffer message("<Exception>");
            message.appendf("<Code>%d</Code><Message>", E->errorCode());
            StringBuffer err;
            E->errorMessage(err);
            encodeXML(err.str(), message);
            message.append("</Message></Exception>");
            unsigned len = message.length();
            void *ret = output->getBuffer(len+1, true);
            memcpy(ret, message.str(), len+1);
            output->putBuffer(ret, len+1, true);
            output->flush();
            E->Release();
        }   
        catch (IException *EInE)
        {
            EXCLOG(EInE, "Exception during throwRemoteException");
            E->Release();
            EInE->Release();
        }
        catch (...)
        {
            logctx.CTXLOG("Unknown Exception during throwRemoteException");
            E->Release();
        }
    }

    void doActivity()
    {
        RoxiePacketHeader &header = packet->queryHeader();
        unsigned channel = header.channel;
        unsigned mySubChannel = header.mySubChannel();

        try
        {   
            bool debugging = logctx.queryDebuggerActive();
            if (debugging)
            {
                if (mySubChannel)
                    abortLaunch = true;  // when debugging, we always run on primary only...
            }

            if (abortLaunch)
            {
                workerThreadBusy = false;  // Keep order - before setActivity below
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer x;
                    logctx.CTXLOG("Stop before processing - activity aborted %s", header.toString(x).str());
                }
                return;
            }

            CCycleTimer workerTimer;
            hash64_t queryHash = header.queryHash;
            Owned<IQueryFactory> queryFactory = getQueryFactory(queryHash, channel);
            if (!queryFactory && (logctx.queryWuid() || topology->hasProp("@workunit")))
            {
                Owned <IRoxieDaliHelper> daliHelper = connectToDali();
                const char * wuid = logctx.queryWuid();
                if (!wuid)
                    wuid = topology->queryProp("@workunit");
                Owned<IConstWorkUnit> wu = daliHelper->attachWorkunit(wuid);
                queryFactory.setown(createAgentQueryFactoryFromWu(wu, channel));
                if (queryFactory)
                    cacheOnDemandQuery(queryHash, channel, queryFactory);
            }
            if (!queryFactory)
            {
                StringBuffer hdr;
                IException *E = MakeStringException(MSGAUD_operator, ROXIE_UNKNOWN_QUERY, "Roxie agent received request for unregistered query: %s", header.toString(hdr).str());
                EXCLOG(E, "doActivity");
                throwRemoteException(E, activity, packet, false);
                return;
            }

            unsigned activityId = header.activityId & ~ROXIE_PRIORITY_MASK;
            Owned <IAgentActivityFactory> factory = queryFactory->getAgentActivityFactory(activityId);
            assertex(factory);
            setActivity(factory->createActivity(logctx, packet));
            if (!debugging)
                ROQ->sendIbyti(header, logctx, mySubChannel);
            Owned<IMessagePacker> output = activity->process();
            stat_type elapsedNs = workerTimer.elapsedNs();
            logctx.setStatistic(StTimeAgentProcess, elapsedNs);
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer x;
                logctx.CTXLOG("done processing %s", header.toString(x).str());
            }
            if (output)
            {
                if (doTrace(traceRoxiePackets))
                    DBGLOG("Activity completed successfully");

                workerThreadBusy = false; // Keep order - before setActivity below
                setActivity(NULL);  // Ensures all stats are merged from child queries etc
                logctx.flush();
                output->flush();
            }
        }
        catch (IUserException *E)
        {
            throwRemoteException(E, activity, packet, true);
        }
        catch (IException *E)
        {
            if (E->errorCode()!=ROXIE_ABORT_ERROR)
                throwRemoteException(E, activity, packet, false);
            else
                E->Release();
        }
        catch (...)
        {
            throwRemoteException(MakeStringException(ROXIE_MULTICAST_ERROR, "Unknown exception"), activity, packet, false);
        }
        workerThreadBusy = false; // Keep order - before setActivity below
        setActivity(NULL);
    }

    virtual void threadmain() override
    {
        while (!stopped)
        {
            try
            {
                for (;;)
                {
                    queue->wait();
                    if (stopped)
                        break;
                    abortLaunch = false;
                    workerThreadBusy = true;
                    Owned<ISerializedRoxieQueryPacket> next = queue->dequeue();
                    if (next)
                    {
                        JobNameScope jobName;
                        logctx.set(next);
                        const char * wuid = logctx.queryWuid();
                        if (wuid)
                            jobName.set(wuid);
                        logctx.setStatistic(StTimeAgentQueue, nsTick()-next->queryEnqueuedTimeStamp());
                        logctx.setStatistic(StTimeIBYTIDelay, next->queryIBYTIDelayTime());
                        setPacket(next->deserialize());
                        next.clear();
                        RoxiePacketHeader &header = packet->queryHeader();
                        if (doTrace(traceRoxiePackets, TraceFlags::Max))
                        {
                            StringBuffer x;
                            logctx.CTXLOG("dequeued %s", header.toString(x).str());
                        }
                        if ((header.activityId & ~ROXIE_PRIORITY_MASK) == ROXIE_UNLOAD)
                        {
                            doUnload(packet, logctx);
                        }
                        else if ((header.activityId & ~ROXIE_PRIORITY_MASK) == ROXIE_DEBUGREQUEST)
                        {
                            // MORE - we need to make sure only executed on primary, and that the proxyId (== pointer to DebugGraphManager) is still valid.
                            // It may be that there is not a lot of point using the pointer - may as well use an non-reused ID and look it up in a global hash table of active ones
                            doDebugRequest(packet, logctx);
                        }
                        else if (header.channel)
                            doActivity();
                        else
                            throwUnexpected(); // channel 0 requests translated earlier now
                    }
                    workerThreadBusy = false;
                    {
                        setPacket(nullptr);
                        logctx.set(NULL);
                    }
                }
            }
            catch(IException *E)
            {
                EXCLOG(E);
                if (packet)
                {
                    throwRemoteException(E, NULL, packet, false);
                    setPacket(nullptr);
                }
                else
                    E->Release();
            }
            catch(...)
            {
                Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception in Roxie worker thread");
                EXCLOG(E);
                if (packet)
                {
                    throwRemoteException(E.getClear(), NULL, packet, false);
                    setPacket(nullptr);
                }
            }
        }
    }
};

IPooledThread *RoxieQueue::createNew()
{
    return new CRoxieWorker;
}

//=================================================================================

class CallbackEntry : implements IPendingCallback, public CInterface
{
    const RoxiePacketHeader &header;
    StringAttr lfn;
    InterruptableSemaphore ready;
    MemoryBuffer data;
    bool gotData;
public:
    IMPLEMENT_IINTERFACE;
    CallbackEntry(const RoxiePacketHeader &_header, const char *_lfn) : header(_header), lfn(_lfn) 
    {
        gotData = false;
    }

    virtual bool wait(unsigned msecs)
    {
        return ready.wait(msecs);
    }

    virtual MemoryBuffer &queryData()
    {
        return data;
    }

    bool matches(RoxiePacketHeader &cand, const char *_lfn)
    {
        return (cand.matchPacket(header) && (!_lfn|| stricmp(_lfn, lfn)==0));
    }

    void doFileCallback(unsigned _len, const void *_data, bool aborted)
    {
        // MORE - make sure we call this for whole query abort as well as for callback abort
        if (aborted)
            ready.interrupt(MakeStringException(0, "Interrupted"));
        else if (!gotData)
        {
            gotData = true;
            data.append(_len, _data);
            ready.signal();
        }
    }
};

class RoxieReceiverBase : implements IRoxieOutputQueueManager, public CInterface
{
protected:
    bool enableIBYTI;
    RoxieQueue slaQueue;
    RoxieQueue hiQueue;
    RoxieQueue loQueue;
    RoxieQueue bgQueue;
    unsigned numWorkers;

public:
    IMPLEMENT_IINTERFACE

    RoxieReceiverBase(unsigned _numWorkers)
     : enableIBYTI(anyChannelHasReplicas()),
       slaQueue(_numWorkers, "SLA", enableIBYTI), hiQueue(_numWorkers, "HIGH", enableIBYTI),
       loQueue(_numWorkers, "LOW", enableIBYTI), bgQueue(_numWorkers, "BG", enableIBYTI), numWorkers(_numWorkers)
    {
        if (!enableIBYTI)
            LOG(MCoperatorInfo, "IBYTI tracking disabled because channels do not have replicas");
    }

    virtual void start() 
    {
        loQueue.start();
        hiQueue.start();
        slaQueue.start();
        bgQueue.start(); // NB BG thread priority can be adjusted
    }

    virtual void stop() 
    {
        loQueue.stopAll();
        hiQueue.stopAll();
        slaQueue.stopAll();
        bgQueue.stopAll();
    }

    virtual void join()  
    { 
        loQueue.join();
        hiQueue.join();
        slaQueue.join();
        bgQueue.join();
    }

    IArrayOf<CallbackEntry> callbacks;
    CriticalSection callbacksCrit;

    virtual IPendingCallback *notePendingCallback(const RoxiePacketHeader &header, const char *lfn)
    {
        CriticalBlock b(callbacksCrit);
        CallbackEntry *callback = new CallbackEntry(header, lfn);
        callbacks.append(*callback);
        return callback;
    }

    virtual void removePendingCallback(IPendingCallback *goer)
    {
        if (goer)
        {
            CriticalBlock b(callbacksCrit);
            callbacks.zap(static_cast<CallbackEntry &>(*goer));
        }
    }

protected:
    void doFileCallback(IRoxieQueryPacket *packet)
    {
        // This is called on the main agent reader thread so needs to be as fast as possible to avoid lost packets
        const char *lfn;
        const char *data;
        unsigned len;
        RoxiePacketHeader &header = packet->queryHeader();
        if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK)
        {
            lfn = (const char *) packet->queryContextData();
            unsigned namelen = strlen(lfn) + 1;
            data = lfn + namelen;
            len = packet->getContextLength() - namelen;
        }
        else
        {
            lfn = data = NULL; // used when query aborted
            len = 0;
        }
        CriticalBlock b(callbacksCrit);
        ForEachItemIn(idx, callbacks)
        {
            CallbackEntry &c = callbacks.item(idx);
            if (c.matches(header, lfn))
            {
                if (doTrace(traceRoxieFiles, TraceFlags::Max))
                    DBGLOG("File callback return matched a waiting query"); 
                c.doFileCallback(len, data, header.retries==QUERY_ABORTED);
            }
        }
    }
};

#ifdef _MSC_VER
#pragma warning ( push )
#pragma warning ( disable: 4355 )
#endif

static void throwPacketTooLarge(IRoxieQueryPacket *x, unsigned maxPacketSize)
{
    StringBuffer traceInfoText;
    unsigned traceLength = x->getTraceLength();
    if (traceLength)
    {
        const byte *traceInfo = x->queryTraceInfo();
        unsigned char loggingFlags = *traceInfo;
        traceInfo++;
        traceLength--;
        if (loggingFlags & LOGGING_TRACELEVELSET)
        {
            traceInfo++;
            traceLength--;
        }

        //Append the rest of the context - but ensure any control characters are converted into something printable
        encodeJSON(traceInfoText, traceLength, (const char *) traceInfo);
    }
    throw MakeStringException(ROXIE_PACKET_ERROR, "Maximum packet length %d exceeded sending packet %s (context length %u, continuation length %u, smart step length %u, trace length %u, total length %u",
                            maxPacketSize, traceInfoText.str(),
                            x->getContextLength(), x->getContinuationLength(), x->getSmartStepInfoLength(), x->getTraceLength(), x->queryHeader().packetlength);
}

class RoxieThrottledPacketSender : public Thread
{
    TokenBucket &bucket;
    InterruptableSemaphore queued;
    Semaphore started;
    SafeQueueOf<IRoxieQueryPacket, false> queue;

    class DECL_EXCEPTION StoppedException: public IException, public CInterface
    {
    public:
        IMPLEMENT_IINTERFACE;
        int             errorCode() const { return 0; }
        StringBuffer &  errorMessage(StringBuffer &str) const { return str.append("Stopped"); }     
        MessageAudience errorAudience() const { return MSGAUD_user; }
    };

    void enqueue(IRoxieQueryPacket *packet)
    {
        packet->Link();
        queue.enqueue(packet);
        queued.signal();
    }

    IRoxieQueryPacket *dequeue()
    {
        queued.wait();
        return queue.dequeue();
    }

public:
    RoxieThrottledPacketSender(TokenBucket &_bucket)
        : Thread("RoxieThrottledPacketSender"), bucket(_bucket)
    {
        start(false);
        started.wait();
    }

    ~RoxieThrottledPacketSender()
    {
        stop();
        join();
    }

    virtual int run()
    {
        started.signal();
        for (;;)
        {
            try
            {
                Owned<IRoxieQueryPacket> packet = dequeue();
                unsigned length = packet->queryHeader().packetlength;
                {
                    MTIME_SECTION(queryActiveTimer(), "bucket_wait");
                    bucket.wait((length / 1024) + 1);
                }
                Owned<ISerializedRoxieQueryPacket> serialized = packet->serialize();
                if (!channelWrite(serialized->queryHeader(), true))
                    DBGLOG("Roxie packet write wrote too little");
                packet->noteTimeSent();
            }
            catch (StoppedException *E)
            {
                E->Release();
                break;
            }
            catch (IException *E)
            {
                EXCLOG(E);
                E->Release();
            }
            catch (...)
            {
            }
        }
        return 0;
    }

    void sendPacket(IRoxieQueryPacket *x, const IRoxieContextLogger &logctx)
    {
        enqueue(x);
    }

    void stop()
    {
//      bucket.stop();
        queued.interrupt(new StoppedException);
    }
};

//------------------------------------------------------------------------------------------------------------

class DelayedPacketQueue
{
    // Used to keep a list of all recently-received packets where we are not primary subchannel. There is one queue per subchannel level
    // It is accessed ONLY from the main reader thread and does not need to be threadsafe (but does need to be fast)
    // We use a doubly-linked list (not std::list as not quite flexible enough).

    class DelayedPacketEntry
    {
        DelayedPacketEntry() = delete;
        DelayedPacketEntry(const DelayedPacketEntry&) = delete;
    public:
        DelayedPacketEntry(ISerializedRoxieQueryPacket *_packet, unsigned _waitExpires)
        : packet(_packet), waitExpires(_waitExpires)
        {
        }
        ~DelayedPacketEntry()
        {
            if (prev)
                prev->next = next;
            if (next)
                next->prev = prev;
        }
        bool matches(const RoxiePacketHeader &ibyti) const
        {
            return packet->queryHeader().matchPacket(ibyti);
        }
        ISerializedRoxieQueryPacket *getClear()
        {
            return packet.getClear();
        }
        StringBuffer & describe(StringBuffer &ret) const
        {
            return packet->queryHeader().toString(ret);
        }

        Owned<ISerializedRoxieQueryPacket> packet;
        DelayedPacketEntry *next = nullptr;
        DelayedPacketEntry *prev = nullptr;

        unsigned waitExpires = 0;
    };

public:
    DelayedPacketQueue() = default;
    DelayedPacketQueue(const DelayedPacketQueue&) = delete;
    ~DelayedPacketQueue()
    {
        while (head)
            removeEntry(head);
    }
    bool doIBYTI(const RoxiePacketHeader &ibyti)
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing

        DelayedPacketEntry *finger = head;
        while (finger)
        {
            if (finger->matches(ibyti))
            {
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("IBYTI removing delayed packet %s", finger->describe(s).str());
                }
                removeEntry(finger);
                return true;
            }
            finger = finger->next;
        }
        return false;
    }

    void append(ISerializedRoxieQueryPacket *packet, unsigned expires)
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing
        // Goes on the end. But percolate the expiry time backwards

        packet->noteQueued(0);
        DelayedPacketEntry *newEntry = new DelayedPacketEntry(packet, expires);
        if (doTrace(traceRoxiePackets))
        {
            StringBuffer s;
            DBGLOG("Adding delayed packet %s expires in %u ms", packet->queryHeader().toString(s).str(), expires - msTick());
        }
        newEntry->prev = tail;
        if (tail)
        {
            tail->next = newEntry;
            for (DelayedPacketEntry *finger = tail; finger != nullptr;)
            {
                if ((int) (finger->waitExpires - expires) <= 0)
                    break;
                finger->waitExpires = expires;
                finger = finger->prev;
            }
        }
        else
            head = newEntry;
        tail = newEntry;
    }

    // Move any that we are done waiting for our buddy onto the active queue
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue, RoxieQueue &bgQueue)
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing

        DelayedPacketEntry *finger = head;
        while (finger)
        {
            if (((int) (finger->waitExpires - now)) <= 0)   // Oddly coded to handle wrapping
            {
                ISerializedRoxieQueryPacket *packet = finger->getClear();
                const RoxiePacketHeader &header = packet->queryHeader();
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("No IBYTI received in time for delayed packet %s - enqueuing", header.toString(s).str());
                }
                unsigned __int64 IBYTIdelay = nsTick()-packet->queryEnqueuedTimeStamp();
                switch (header.activityId & ROXIE_PRIORITY_MASK)
                {
                    case ROXIE_SLA_PRIORITY: slaQueue.enqueue(packet, IBYTIdelay); break;
                    case ROXIE_HIGH_PRIORITY: hiQueue.enqueue(packet, IBYTIdelay); break;
                    case ROXIE_LOW_PRIORITY: loQueue.enqueue(packet, IBYTIdelay); break;
                    default: bgQueue.enqueue(packet, IBYTIdelay); break;
                }
                for (unsigned subChannel = 0; subChannel < MAX_SUBCHANNEL; subChannel++)
                {
                    if (header.subChannels[subChannel].isMe() || header.subChannels[subChannel].isNull())
                        break;
                    noteNodeSick(header.subChannels[subChannel]);
                }

                DelayedPacketEntry *goer = finger;
                finger = finger->next;
                removeEntry(goer);
            }
            else
                break;
        }
    }

    // How long until the next time we want to call checkExpires() ?
    unsigned getNextExpiryTime(unsigned earliestExpiryTime) const
    {
        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing
        if (head)
        {
            int delay = (int) (head->waitExpires - earliestExpiryTime);
            if (delay <= 0)
                return head->waitExpires;
        }

        return earliestExpiryTime;
    }

private:
    void removeEntry(DelayedPacketEntry *goer)
    {
        if (goer==head)
            head = goer->next;
        if (goer==tail)
            tail = goer->prev;
        delete goer;
    }

    DelayedPacketEntry *head = nullptr;
    DelayedPacketEntry *tail = nullptr;

};

//------------------------------------------------------------------------------------------------------------

class DelayedPacketQueueChannel : public CInterface
{
    // Manages a set of DelayedPacketQueues, one for each supported subchannel level.
    DelayedPacketQueueChannel() = delete;
    DelayedPacketQueueChannel(const DelayedPacketQueueChannel&) = delete;
public:
    DelayedPacketQueueChannel(unsigned _channel) : channel(_channel)
    {
    }
    inline unsigned queryChannel() const { return channel; }
    inline DelayedPacketQueue &queryQueue(unsigned subchannel)
    {
        assertex(subchannel);  // Subchannel 0 means primary and is never delayed
        subchannel -= 1;
        if (subchannel > maxSeen)
            maxSeen = subchannel;
        return queues[subchannel];
    }
    unsigned getNextExpiryTime(unsigned earliestExpiryTime) const
    {
        for (unsigned queue = 0; queue <= maxSeen; queue++)
            earliestExpiryTime = queues[queue].getNextExpiryTime(earliestExpiryTime);
        return earliestExpiryTime;
    }
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue, RoxieQueue &bgQueue)
    {
        for (unsigned queue = 0; queue <= maxSeen; queue++)
        {
            queues[queue].checkExpired(now, slaQueue, hiQueue, loQueue, bgQueue);
        }
    }
private:
    DelayedPacketQueue queues[MAX_SUBCHANNEL-1];   // Note - primary subchannel is not included
    unsigned channel = 0;
    unsigned maxSeen = 0;
};

class DelayedPacketQueueManager
{

public:
    DelayedPacketQueueManager() = default;
    DelayedPacketQueueManager(const DelayedPacketQueueManager&) = delete;
    inline DelayedPacketQueue &queryQueue(unsigned channel, unsigned subchannel)
    {
        // Note - there are normally no more than a couple of channels on a single agent.
        // If that were to change we could make this a fixed size array

        // RoxieSocketQueueManager::ibytiCrit must be locked while this is executing - could possibly be a different crit just for the channel list.
        // or even better if the channels were fixed and preallocated.
        ForEachItemIn(idx, channels)
        {
            DelayedPacketQueueChannel &i = channels.item(idx);
            if (i.queryChannel() == channel)
                return i.queryQueue(subchannel);
        }
        channels.append(*new DelayedPacketQueueChannel(channel));
        return channels.tos().queryQueue(subchannel);
    }
    unsigned getNextExpiryTime(unsigned earliestExpiryTime) const
    {
        ForEachItemIn(idx, channels)
            earliestExpiryTime = channels.item(idx).getNextExpiryTime(earliestExpiryTime);

        return earliestExpiryTime;
    }
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue, RoxieQueue &bgQueue)
    {
        ForEachItemIn(idx, channels)
        {
            channels.item(idx).checkExpired(now, slaQueue, hiQueue, loQueue, bgQueue);
        }
    }
private:
    CIArrayOf<DelayedPacketQueueChannel> channels;
};

//---------------------------------------------------------------------------------------------------------------------

static constexpr unsigned maxDelayTimeout = 0x3fffffff;

class RoxieSocketQueueManager : public RoxieReceiverBase, public IRoxieWorkerRequestReceiver
{
protected:
    Linked<ISendManager> sendManager;
    Linked<IReceiveManager> receiveManager;
    Owned<RoxieThrottledPacketSender> throttledPacketSendManager;
    Owned<TokenBucket> bucket;
    unsigned maxPacketSize = 0;
    StringContextLogger logctx;
    DelayedPacketQueueManager delayed;
    CriticalSection ibytiCrit; // Protect the ibyti structures against concurrent access - could reduce granularity later
    class WorkerReceiverTracker : public TimeDivisionTracker<6, false>
    {
    public:
        enum
        {
            other,
            waiting,
            allocating,
            processing,
            pushing,
            checkingRunning
        };

        WorkerReceiverTracker(const char *name, unsigned reportIntervalSeconds) : TimeDivisionTracker<6, false>(name, reportIntervalSeconds)
        {
            stateNames[other] = "other";
            stateNames[waiting] = "waiting";
            stateNames[allocating] = "allocating";
            stateNames[processing] = "processing";
            stateNames[pushing] = "pushing";
            stateNames[checkingRunning] = "checking running";
        }
    } timeTracker;

    class DelayedPacketProcessor : public Thread
    {
    public:
        DelayedPacketProcessor(RoxieSocketQueueManager & _parent) : Thread("DelayedPacketProcessor"), parent(_parent)
        {
        }

        virtual int run() override
        {
            firstWakeTime = msTick() + maxDelayTimeout;
            for (;;)
            {
                unsigned wake = firstWakeTime.load();
                unsigned now = msTick();
                //If there are no packets ready to process immediately then wait until the first is ready,
                //or we are signalled that an earlier packet has now been queued, or we are terminating.
                if ((int)(wake - now) > 0)
                {
                    unsigned timeout = wake - now;
                    //Arbitrary limit
                    if (timeout > 5000)
                        timeout = 5000;
                    wakeEarly.wait(timeout);

                    wake = firstWakeTime.load();
                    now = msTick();
                }
                if (abort)
                    break;

                //Could there be any delayed packets which are now ready to be processed?
                if ((int)(wake - now) <= 0)
                {
                    //Exchange the wake time for a large time in the future, so that any packets that
                    //are added in the meantime will cleanly adjust the wake time
                    while (!firstWakeTime.compare_exchange_weak(wake, now + maxDelayTimeout))
                    {
                    }

                    // Update the wake time if lower than current
                    unsigned newWakeTime = parent.processDelayedPackets(now);
                    unsigned curWakeTime = firstWakeTime.load();
                    while ((int)(newWakeTime - curWakeTime) < 0)
                    {
                        if (firstWakeTime.compare_exchange_weak(curWakeTime, newWakeTime))
                            break;
                    }
                }
            }
            abort = false;
            return 0;
        }

        void stop()
        {
            abort = true;
            wakeEarly.signal();
        }

        void noteNewDelayedPacket(unsigned newWakeTime)
        {
            //If the new packet has an earlier wake time than the lowest we have, then update the lowerst wake time
            //and signal the semaphore to wake the processor thread - so it can adjust the sleep time.
            unsigned curWakeTime = firstWakeTime.load();
            while ((int)(newWakeTime - curWakeTime) < 0)
            {
                if (firstWakeTime.compare_exchange_weak(curWakeTime, newWakeTime))
                {
                    wakeEarly.signal();
                    break;
                }
            }
        }

    protected:
        RoxieSocketQueueManager & parent;
        Semaphore wakeEarly;
        std::atomic<unsigned> firstWakeTime{0};
        std::atomic<bool> abort{false};

    } delayedPacketProcessor;

public:
    RoxieSocketQueueManager(unsigned _numWorkers) : RoxieReceiverBase(_numWorkers),
        logctx("RoxieSocketQueueManager"), timeTracker("WorkerUdpReader", 60),delayedPacketProcessor(*this)
    {
        maxPacketSize = workerCommunicator->queryMaxPacketSize();
    }

    virtual void sendPacket(IRoxieQueryPacket *x, const IRoxieContextLogger &logctx)
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendPacket");
        RoxiePacketHeader &header = x->queryHeader();

        unsigned length = x->queryHeader().packetlength;
        assertex (header.activityId & ~ROXIE_PRIORITY_MASK);
        if (doTrace(traceRoxiePackets))
        {
            StringBuffer s;
            switch (header.retries & ROXIE_RETRIES_MASK)
            {
            case (QUERY_ABORTED & ROXIE_RETRIES_MASK):
                logctx.CTXLOG("Aborting packet size=%d: %s", length, header.toString(s).str());
                break;
            default:
                logctx.CTXLOG("Resending packet size=%d: %s", length, header.toString(s).str());
                break;
            case 0:
                if (doTrace(traceRoxiePackets, TraceFlags::Max))
                    logctx.CTXLOG("Sending packet size=%d: %s", length, header.toString(s).str());
                break;
            }
        }
        if (length > maxPacketSize)
            throwPacketTooLarge(x, maxPacketSize);

        if (throttledPacketSendManager)
            throttledPacketSendManager->sendPacket(x, logctx);
        else
        {
            x->noteTimeSent();
            Owned <ISerializedRoxieQueryPacket> serialized = x->serialize();
            if (!channelWrite(serialized->queryHeader(), true))
                logctx.CTXLOG("Roxie packet write wrote too little");
        }
    }

    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx, unsigned subChannel) override
    {
        if (!header.hasBuddies())
            return;

        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendIbyti");
        RoxiePacketHeader ibytiHeader(header, header.activityId & ROXIE_PRIORITY_MASK, subChannel);
    
        if (doTrace(traceIBYTI))
        {
            StringBuffer s; logctx.CTXLOG("Sending IBYTI packet %s", ibytiHeader.toString(s).str());
        }
        channelWrite(ibytiHeader, false);  // don't send to self
    }

    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbort");
        RoxiePacketHeader abortHeader(header, header.activityId & ROXIE_PRIORITY_MASK, 0);  // subChannel irrelevant - we are about to overwrite retries anyway
        abortHeader.retries = QUERY_ABORTED;
        if (doTrace(traceRoxiePackets))
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT packet %s", abortHeader.toString(s).str());
        }
        try
        {
            if (!channelWrite(abortHeader, true))
                logctx.CTXLOG("sendAbort wrote too little");
        }
        catch (IException *E)
        {
            EXCLOG(E);
            E->Release();
        }
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK, 0); // subChannel irrelevant - we are about to overwrite retries anyway
        abortHeader.retries = QUERY_ABORTED;
        abortHeader.packetlength += strlen(lfn)+1;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (doTrace(traceRoxieFiles))
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        Owned<ISerializedRoxieQueryPacket> serialized = packet->serialize();
        if (!channelWrite(serialized->queryHeader(), true))
            logctx.CTXLOG("sendAbortCallback wrote too little");
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx)
    {
        unsigned qnum = outOfBand ? 0 : ((header.retries & ROXIE_FASTLANE) || !fastLaneQueue) ? 1 : 2;
        if (doTrace(traceRoxiePackets, TraceFlags::Max))
        {
            StringBuffer s; logctx.CTXLOG("Creating Output Stream for reply packet on Q=%d - %s", qnum, header.toString(s).str());
        }
        return sendManager->createMessagePacker(header.uid, header.getSequenceId(), &header, sizeof(RoxiePacketHeader), header.serverId, qnum);
    }

    virtual bool replyPending(RoxiePacketHeader &header)
    {
        return sendManager->dataQueued(header.uid, header.getSequenceId(), header.serverId);
    }

    virtual bool abortCompleted(RoxiePacketHeader &header)
    {
        return sendManager->abortData(header.uid, header.getSequenceId(), header.serverId);
    }

    bool abortRunning(RoxiePacketHeader &header, RoxieQueue &queue, bool checkRank, bool &preActivity)
    {
        bool queryFound = false;
        bool ret = false;
        Owned<IPooledThreadIterator> wi = queue.running();
        ForEach(*wi)
        {
            CRoxieWorker &w = (CRoxieWorker &) wi->query();
            if (w.checkAbort(header, checkRank, queryFound, preActivity))
            {
                ret = true;
                break;
            }
            else if (queryFound)
            {
                return false;
            }
        }
        if (!checkRank)
        {
            if (doTrace(traceRoxiePackets))
                DBGLOG("discarding data for aborted query");
            ROQ->abortCompleted(header);
        }
        return ret;
    }

    void doIbyti(RoxiePacketHeader &header, RoxieQueue &queue)
    {
        assert(!localAgent);
        bool preActivity = false;
        unsigned mySubChannel = header.mySubChannel();

        if (unlikely(header.retries == QUERY_ABORTED))
        {
            bool foundInQ = false;
            foundInQ = mySubChannel != 0 && delayed.queryQueue(header.channel, mySubChannel).doIBYTI(header);
            if (!foundInQ)
                foundInQ = queue.remove(header);
            if (!foundInQ)
                abortRunning(header, queue, false, preActivity);

            if (doTrace(traceRoxiePackets))
            {
                StringBuffer s; 
                DBGLOG("Abort activity %s", header.toString(s).str());
            }
            return;
        }

        unsigned subChannel = header.getRespondingSubChannel();
        if (subChannel == mySubChannel)
        {
            if (doTrace(traceRoxiePackets))
                DBGLOG("doIBYTI packet was from self");
        }
        else
        {
            noteNodeHealthy(header.subChannels[subChannel]);
            bool foundInQ = mySubChannel != 0 && delayed.queryQueue(header.channel, mySubChannel).doIBYTI(header);
            if (!foundInQ)
                foundInQ = queue.remove(header);  // Check on list waiting for a free worker
            if (foundInQ)
            {
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("Removed activity from Q : %s", header.toString(s).str());
                }
                return;
            }
            if (abortRunning(header, queue, true, preActivity))
            {
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("Aborted running activity : %s", header.toString(s).str());
                }
                return;
            }
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer s;
                DBGLOG("doIBYTI packet was too late (or too early, or too low priority) : %s", header.toString(s).str());
            }

            queue.noteOrphanIBYTI(header);
        }
    }

    void processMessage(const void * data, size32_t length, RoxiePacketHeader &header, RoxieQueue &queue)
    {
        CriticalBlock block(ibytiCrit);

        // NOTE - this thread needs to do as little as possible - just read packets and queue them up - otherwise we can get packet loss due to buffer overflow
        // DO NOT put tracing on this thread except at very high tracelevels!
        if ((header.activityId & ~ROXIE_PRIORITY_MASK) == 0)
            doIbyti(header, queue);
        else
        {
            if (!header.channel)
            {
                // Turn broadcast packet (channel 0), as early as possible, into non-0 channel packets.
                // So retries and other communication with Roxie server (which uses non-0 channel numbers) will not cause double work or confusion.
                // Unfortunately this is bad news for dropping packets
                // In SUBCHANNELS_IN_HEADER mode this translation has been done on server before sending, except for some control messages like UNLOAD?

                Owned<const ITopologyServer> topology = getTopology();
                const std::vector<unsigned> channels = topology->queryChannels();
                Owned<ISerializedRoxieQueryPacket> packet = createSerializedRoxiePacket((void *)data, length, false);
                for (unsigned i = 1; i < channels.size(); i++)
                    queue.enqueue(packet->cloneSerializedPacket(channels[i]), 0);
                header.retries |= ROXIE_BROADCAST;
                header.channel = channels[0];
                queue.enqueue(packet.getClear(), 0);   // NOTE - This would be wrong in NEW_IBYTI mode - should add to delayed instead if we are not primary... 
                return;
            }

            if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK )
            {
                Owned<IRoxieQueryPacket> packet = deserializeCallbackPacket(data, length);
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("ROXIE_CALLBACK %s", header.toString(s).str());
                }
                doFileCallback(packet);
            }
            // Skip orphan IBYTI lookup if no buddies - IBYTI packets won't be sent without replicas
            else if (header.hasBuddies() && queue.lookupOrphanIBYTI(header))
            {
                if (doTrace(traceRoxiePackets))
                {
                    StringBuffer s;
                    DBGLOG("doIBYTI packet was too early : %s", header.toString(s).str());
                }
            }
            else
            {
                unsigned mySubchannel = header.mySubChannel();
                Owned<ISerializedRoxieQueryPacket> packet = createSerializedRoxiePacket((void *)data, length, false);
                unsigned retries = header.thisChannelRetries(mySubchannel);
                if (retries >= SUBCHANNEL_MASK)
                    return; // I already failed unrecoverably on this request - ignore it
                if (acknowledgeAllRequests && (header.activityId & ~ROXIE_PRIORITY_MASK) < ROXIE_ACTIVITY_SPECIAL_FIRST)
                {
#ifdef DEBUG
                    if (testAgentFailure & 0x1 && !retries)
                        return;
#endif
                    if (doTrace(traceRoxiePackets))
                    {
                        StringBuffer buf;
                        DBGLOG("Sending ROXIE_ALIVE to acknowledge request %s", header.toString(buf).str());
                    }
                    RoxiePacketHeader newHeader(header, ROXIE_ALIVE, mySubchannel);
                    Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                    output->flush();
                }
                if (retries)
                {
                    // MORE - is this fast enough? By the time I am seeing retries I may already be under load. Could move onto a separate thread
                    assertex(header.channel); // should never see a retry on channel 0
                    // Send back an out-of-band immediately, to let Roxie server know that channel is still active
                    if (!(testAgentFailure & 0x800) && !acknowledgeAllRequests)
                    {
                        RoxiePacketHeader newHeader(header, ROXIE_ALIVE, mySubchannel);
                        Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                        output->flush();
                    }

                    // If it's a retry, look it up against already running, or output stream, or input queue
                    // if found, send an IBYTI and discard retry request

                    bool alreadyRunning = false;
                    {
                        WorkerReceiverTracker::TimeDivision division(timeTracker, WorkerReceiverTracker::checkingRunning);

                        Owned<IPooledThreadIterator> wi = queue.running();
                        ForEach(*wi)
                        {
                            CRoxieWorker &w = (CRoxieWorker &) wi->query();
                            if (w.match(header))
                            {
                                alreadyRunning = true;
                                ROQ->sendIbyti(header, logctx, mySubchannel);
                                if (doTrace(traceRoxiePackets, TraceFlags::Max))
                                {
                                    StringBuffer xx; logctx.CTXLOG("Ignored retry on subchannel %u for running activity %s", mySubchannel, header.toString(xx).str());
                                }
                                break;
                            }
                        }

                        if (!alreadyRunning && checkCompleted && ROQ->replyPending(header))
                        {
                            alreadyRunning = true;
                            ROQ->sendIbyti(header, logctx, mySubchannel);
                            if (doTrace(traceRoxiePackets, TraceFlags::Max))
                            {
                                StringBuffer xx; logctx.CTXLOG("Ignored retry on subchannel %u for completed activity %s", mySubchannel, header.toString(xx).str());
                            }
                        }
                    }

                    if (!alreadyRunning)
                    {
                        if (doTrace(traceRoxiePackets, TraceFlags::Max))
                        {
                            StringBuffer xx; logctx.CTXLOG("Retry %d received on subchannel %u for %s", retries+1, mySubchannel, header.toString(xx).str());
                        }
                        WorkerReceiverTracker::TimeDivision division(timeTracker, WorkerReceiverTracker::pushing);

                        // It's debatable whether we should delay for the primary here - they had one chance already...
                        // But then again, so did we, assuming the timeout is longer than the IBYTIdelay
                        unsigned delay = 0;
                        if (mySubchannel != 0)  // i.e. I am not the primary here and there are replicas (no delay if no buddies)
                        {
                            for (unsigned subChannel = 0; subChannel < mySubchannel; subChannel++)
                                delay += getIbytiDelay(header.subChannels[subChannel]);
                        }
                        if (delay)
                        {
                            unsigned expiryTime = msTick() + delay;
                            delayed.queryQueue(header.channel, mySubchannel).append(packet.getClear(), expiryTime);
                            delayedPacketProcessor.noteNewDelayedPacket(expiryTime);
                        }
                        else
                            queue.enqueueUnique(packet.getClear(), mySubchannel, 0);
                    }
                }
                else // first time (not a retry).
                {
                    WorkerReceiverTracker::TimeDivision division(timeTracker, WorkerReceiverTracker::pushing);
                    unsigned delay = 0;
                    if (mySubchannel != 0 && (header.activityId & ~ROXIE_PRIORITY_MASK) < ROXIE_ACTIVITY_SPECIAL_FIRST)  // i.e. I am not the primary here, there are replicas, and never delay special
                    {
                        for (unsigned subChannel = 0; subChannel < mySubchannel; subChannel++)
                            delay += getIbytiDelay(header.subChannels[subChannel]);
                    }
                    if (delay)
                    {
                        unsigned expiryTime = msTick() + delay;
                        delayed.queryQueue(header.channel, mySubchannel).append(packet.getClear(), expiryTime);
                        delayedPacketProcessor.noteNewDelayedPacket(expiryTime);
                    }
                    else
                        queue.enqueue(packet.getClear(), 0);
                }
            }
        }
    }

    virtual void processMessage(const void * data, size32_t length) override
    {
        try
        {
            RoxiePacketHeader &header = *(RoxiePacketHeader *) data;
            if (length != header.packetlength)
                DBGLOG("sock->read returned %u but packetlength was %u", length, header.packetlength);
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer s;
                DBGLOG("Read roxie packet: %s", header.toString(s).str());
            }

            WorkerReceiverTracker::TimeDivision division(timeTracker, WorkerReceiverTracker::processing);
            switch (header.activityId & ROXIE_PRIORITY_MASK)
            {
                case ROXIE_SLA_PRIORITY: processMessage(data, length, header, slaQueue); break;
                case ROXIE_HIGH_PRIORITY: processMessage(data, length, header, hiQueue); break;
                case ROXIE_LOW_PRIORITY: processMessage(data, length, header, loQueue); break;
                default: processMessage(data, length, header, bgQueue); break;
            }
        }
        catch (IException *E)
        {
            if (roxiemem::memPoolExhausted())
            {
                //MORE: I think this should probably be based on the error code instead.
                EXCLOG(E, "Exception reading or processing roxie packet");
                E->Release();
                MilliSleep(1000); // Give a chance for mem free
            }
            else
                throw;
        }
    }

    unsigned processDelayedPackets(unsigned now)
    {
        CriticalBlock block(ibytiCrit);
        delayed.checkExpired(now, slaQueue, hiQueue, loQueue, bgQueue);
        return delayed.getNextExpiryTime(now + maxDelayTimeout);
    }

    void start() 
    {
        RoxieReceiverBase::start();
        timeTracker.reset(WorkerReceiverTracker::waiting);
        // Only start delayed packet processor thread if there are replicas
        // Without replicas, packets are never delayed for IBYTI
        if (enableIBYTI)
            delayedPacketProcessor.start(false);
        workerCommunicator->startListening(*this);
    }

    void stop() 
    {
        workerCommunicator->stopListening();
        delayedPacketProcessor.stop();
        delayedPacketProcessor.join();
        RoxieReceiverBase::stop();
    }

    virtual IReceiveManager *queryReceiveManager()
    {
        return receiveManager;
    }
};

class RoxieUdpSocketQueueManager : public RoxieSocketQueueManager
{
public:
    RoxieUdpSocketQueueManager(unsigned _numWorkers, bool encryptionInTransit) : RoxieSocketQueueManager(_numWorkers)
    {
        unsigned udpQueueSize = topology->getPropInt("@udpQueueSize", UDP_QUEUE_SIZE);
        unsigned udpSendQueueSize = topology->getPropInt("@udpSendQueueSize", UDP_SEND_QUEUE_SIZE);

        if (topology->getPropInt("@sendMaxRate", 0))
        {
            unsigned sendMaxRate = topology->getPropInt("@sendMaxRate");
            unsigned sendMaxRatePeriod = topology->getPropInt("@sendMaxRatePeriod", 1);
            bucket.setown(new TokenBucket(sendMaxRate, sendMaxRatePeriod, sendMaxRate));
            throttledPacketSendManager.setown(new RoxieThrottledPacketSender(*bucket));
        }

        unsigned serverFlowPort = topology->getPropInt("@serverFlowPort", CCD_SERVER_FLOW_PORT);
        bool udpSendFlowOnDataPort = topology->getPropBool("@udpSendFlowOnDataPort", true);
        unsigned dataPort = topology->getPropInt("@dataPort", CCD_DATA_PORT);
        unsigned clientFlowPort = topology->getPropInt("@clientFlowPort", CCD_CLIENT_FLOW_PORT);
        bool useIOUringForSend = topology->getPropBool("@workerSendUseIOUring", false);
        receiveManager.setown(createReceiveManager(serverFlowPort, dataPort, clientFlowPort, udpQueueSize, encryptionInTransit));
        sendManager.setown(createSendManager(udpSendFlowOnDataPort ? dataPort : serverFlowPort, dataPort, clientFlowPort, udpSendQueueSize, fastLaneQueue ? 3 : 2, myNode.getIpAddress(), bucket, encryptionInTransit, useIOUringForSend));
    }

    virtual void abortPendingData(const SocketEndpoint &ep) override
    {
        sendManager->abortAll(ep);
    }

};

#ifdef _MSC_VER
#pragma warning( pop )
#endif


//==================================================================================================

void * CDummyMessagePacker::getBuffer(unsigned len, bool variable)
{
    if (variable)
    {
        char *ret = (char *) data.ensureCapacity(len + sizeof(RecordLengthType));
        return ret + sizeof(RecordLengthType);
    }
    else
    {
        return data.ensureCapacity(len);
    }
}

void CDummyMessagePacker::putBuffer(const void *buf, unsigned len, bool variable)
{
    if (variable)
    {
        buf = ((char *) buf) - sizeof(RecordLengthType);
        *(RecordLengthType *) buf = len;
        len += sizeof(RecordLengthType);
    }
    data.setWritePos(lastput + len);
    lastput += len;
}

void CDummyMessagePacker::flush()
{
}

void CDummyMessagePacker::sendMetaInfo(const void *buf, unsigned len)
{
    throwUnexpected();
}

unsigned CDummyMessagePacker::size() const
{
    return lastput;
}

interface ILocalMessageCollator : extends IMessageCollator
{
    virtual bool attachDataBuffers(const ArrayOf<roxiemem::OwnedDataBuffer> &buffers) = 0;
    virtual void enqueueMessage(bool outOfBand, unsigned totalSize, IMessageResult *result) = 0;
};

interface ILocalReceiveManager : extends IReceiveManager
{
    virtual ILocalMessageCollator *lookupCollator(ruid_t id) = 0;
};

class LocalMessagePacker : public CInterfaceOf<IMessagePacker>
{
protected:
    unsigned lastput = 0;
    MemoryBuffer data;
    MemoryBuffer meta;
    MemoryBuffer header;
    Linked<ILocalReceiveManager> rm;
    ruid_t id;
    bool outOfBand;

public:
    LocalMessagePacker(RoxiePacketHeader &_header, bool _outOfBand, ILocalReceiveManager *_rm) : rm(_rm), outOfBand(_outOfBand)
    {
        id = _header.uid;
        header.append(sizeof(RoxiePacketHeader), &_header);
    }

    void * getBuffer(unsigned len, bool variable) override
    {
        if (variable)
        {
            char *ret = (char *) data.ensureCapacity(len + sizeof(RecordLengthType));
            return ret + sizeof(RecordLengthType);
        }
        else
        {
            return data.ensureCapacity(len);
        }
    }

    void putBuffer(const void *buf, unsigned len, bool variable) override
    {
        if (variable)
        {
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        data.setWritePos(lastput + len);
        lastput += len;
    }

    unsigned size() const
    {
        return lastput;
    }

    virtual void flush() override;

    virtual void sendMetaInfo(const void *buf, unsigned len) override
    {
        meta.append(len, buf);
    }

};

class LocalBlockedMessagePacker : public CInterfaceOf<IMessagePacker>
{
protected:
    unsigned lastput = 0;
    MemoryBuffer meta;
    MemoryBuffer header;
    Linked<ILocalReceiveManager> rm;
    ruid_t id;
    bool outOfBand;
    const unsigned dataBufferSize = DATA_PAYLOAD - sizeof(unsigned short);
    bool packed = false;
    unsigned tempBufferSize = 0;  // MORE - use a MemoryBuffer?
    void *tempBuffer = nullptr;
    unsigned dataPos = 0;
    unsigned bufferRemaining = 0;
    DataBuffer *currentBuffer = nullptr;
    ArrayOf<roxiemem::OwnedDataBuffer> buffers;
    unsigned totalDataLen = 0;
    DataBuffer *bulkBuffers[16];  // Cache for bulk-allocated buffers
    unsigned bulkBufferCount = 0;
    unsigned bulkBufferIndex = 0;

    DataBuffer *allocateNextBuffer()
    {
        // Try to use a bulk-allocated buffer first
        if (bulkBufferIndex < bulkBufferCount)
        {
            return bulkBuffers[bulkBufferIndex++];
        }
        else
        {
            // Try to allocate a new bulk pool
            if (bufferManager->allocateBlock(8, bulkBuffers))
            {
                bulkBufferCount = 8;
                bulkBufferIndex = 0;
                return bulkBuffers[bulkBufferIndex++];
            }
            else if (bufferManager->allocateBlock(4, bulkBuffers))
            {
                bulkBufferCount = 4;
                bulkBufferIndex = 0;
                return bulkBuffers[bulkBufferIndex++];
            }
            else
            {
                // Fall back to single allocation
                return bufferManager->allocate();
            }
        }
    }

public:
    LocalBlockedMessagePacker(RoxiePacketHeader &_header, bool _outOfBand, ILocalReceiveManager *_rm) : rm(_rm), outOfBand(_outOfBand)
    {
        id = _header.uid;
        header.append(sizeof(RoxiePacketHeader), &_header);
    }
    ~LocalBlockedMessagePacker()
    {
        free(tempBuffer);
        // Release any remaining bulk-allocated buffers
        while (bulkBufferIndex < bulkBufferCount)
        {
            bulkBuffers[bulkBufferIndex++]->Release();
        }
    }

    void * getBuffer(unsigned len, bool variable) override
    {
        if (variable)
            len += sizeof(RecordLengthType);
        if (dataBufferSize < len)
        {
            // Won't fit in one, so allocate temp location
            // This code stolen from UDP layer - we could redo using a MemoryBuffer...
            packed = false;
            if (tempBufferSize < len)
            {
                free(tempBuffer);
                tempBuffer = checked_malloc(len, ROXIE_MEMORY_ERROR);
                tempBufferSize = len;
            }
            if (variable)
                return ((char *) tempBuffer) + sizeof(RecordLengthType);
            else
                return tempBuffer;
        }
        else
        {
            // Will fit in one, though not necessarily the current one...
            packed = true;
            if (currentBuffer && (bufferRemaining < len))
            {
                // Note that we never span records that are small enough to fit in one buffer - this can result in significant wastage if record just over DATA_PAYLOAD/2                
                *(unsigned short *) &currentBuffer->data = dataPos - sizeof(unsigned short);
                buffers.append(currentBuffer);
                currentBuffer = nullptr;
            }
            if (!currentBuffer)
            {
                currentBuffer = allocateNextBuffer();
                dataPos = sizeof (unsigned short);
                bufferRemaining = dataBufferSize;
            }
            if (variable)
                return &currentBuffer->data[dataPos + sizeof(RecordLengthType)];
            else
                return &currentBuffer->data[dataPos];
        }
    }

    void putBuffer(const void *buf, unsigned len, bool variable) override
    {
        if (variable)
        {
            assertex(len < MAX_RECORD_LENGTH);
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        totalDataLen += len;
        if (packed)
        {
            assert(len <= bufferRemaining);
            dataPos += len;
            bufferRemaining -= len;
        }
        else
        {
            //row length should not be split between packets (see assert in CLocalBlockedMessageUnpackCursor::getNext())
            //Therefore ensure the first block of data written is large enough to include the whole length
            unsigned minSpace = variable ? sizeof(RecordLengthType) : 0;
            while (len)
            {
                if (!currentBuffer)
                {
                    currentBuffer = allocateNextBuffer();
                    dataPos = sizeof (unsigned short);
                    bufferRemaining = dataBufferSize;
                }
                unsigned chunkLen = bufferRemaining;
                if (chunkLen > len)
                    chunkLen = len;
                if (chunkLen < minSpace)
                    chunkLen = 0;
                memcpy(&currentBuffer->data[dataPos], buf, chunkLen);
                dataPos += chunkLen;
                len -= chunkLen;
                buf = &(((char*)buf)[chunkLen]);
                bufferRemaining -= chunkLen;
                if (len)
                {
                    *(unsigned short *) &currentBuffer->data = dataPos - sizeof(unsigned short);
                    buffers.append(currentBuffer);
                    currentBuffer = nullptr;
                }

                //Set minSpace to 0 because we have either written the length, or are writing to the start of a
                //new block, so the length will no span a block boundary.
                minSpace = 0;
            }
        }
    }

    unsigned size() const
    {
        return lastput;
    }

    virtual void flush() override;

    virtual void sendMetaInfo(const void *buf, unsigned len) override
    {
        meta.append(len, buf);
    }

};

class CLocalMessageUnpackCursor : implements IMessageUnpackCursor, public CInterface
{
    // Note that the data is owned by the CLocalMessageCursor that created me,
    // and will be released by it when it dies
    void *data;
    unsigned datalen;
    unsigned pos;
    Linked<IRowManager> rowManager;
public:
    IMPLEMENT_IINTERFACE;
    CLocalMessageUnpackCursor(IRowManager *_rowManager, void *_data, unsigned _datalen)
        : rowManager(_rowManager)
    {
        datalen = _datalen;
        data = _data;
        pos = 0;
    }

    ~CLocalMessageUnpackCursor()
    {
    }

    virtual bool atEOF() const
    {
        return datalen==pos;
    }

    virtual bool isSerialized() const
    {
        // NOTE: tempting to think that we could avoid serializing in localAgent case, but have to be careful about the lifespan of the rowManager...
        return true;
    }

    virtual RecordLengthType *getNextLength() override
    {
        if (pos==datalen)
            return NULL;
        assertex(pos + sizeof(RecordLengthType) <= datalen);
        void * cur = ((char *) data) + pos;
        pos += sizeof(RecordLengthType);
        return (RecordLengthType *) cur;
    }

    virtual const void * getNext(int length)
    {
        if (pos==datalen)
            return NULL;
        assertex(pos + length <= datalen);
        void * cur = ((char *) data) + pos;
        pos += length;
        void * ret = rowManager->allocate(length, 0);
        memcpy(ret, cur, length);
        //No need for finalize since only contains plain data.
        return ret;
    }
};

class CLocalBlockedMessageUnpackCursor : implements IMessageUnpackCursor, public CInterface
{
    Linked<IRowManager> rowManager;
    const ArrayOf<roxiemem::OwnedDataBuffer> &buffers; // Owned by the CLocalBlockedMessageCursor that created me
    const byte *currentBuffer = nullptr;
    unsigned currentBufferRemaining = 0;
    unsigned bufferIdx = 0;
public:
    IMPLEMENT_IINTERFACE;
    CLocalBlockedMessageUnpackCursor(IRowManager *_rowManager, const ArrayOf<roxiemem::OwnedDataBuffer> &_buffers) 
        : rowManager(_rowManager), buffers(_buffers)
    {
        if (buffers.length())
        {
            currentBuffer = (const byte *) buffers.item(0).get()->data;
            currentBufferRemaining = *(unsigned short *) currentBuffer;
            currentBuffer += sizeof(unsigned short);
        }
    }

    ~CLocalBlockedMessageUnpackCursor()
    {
    }

    virtual bool atEOF() const
    {
        return currentBuffer != nullptr;
    }

    virtual bool isSerialized() const
    {
        // NOTE: tempting to think that we could avoid serializing in localAgent case, but have to be careful about the lifespan of the rowManager...
        return true;
    }

    virtual const void * getNext(int length)
    {
        if (!currentBuffer) 
            return nullptr;
        if ((currentBufferRemaining) >= (unsigned) length)
        {
            // Simple case - no need to copy
            const void *res = currentBuffer;
            currentBuffer += length;
            currentBufferRemaining -= length;
            checkNext();
            LinkRoxieRow(res);
            return res;
        }   
        char *currResLoc = (char*)rowManager->allocate(length, 0);
        const void *res = currResLoc;
        while (length && currentBuffer) 
        {
            // Spans more than one block - allocate and copy
            unsigned cpyLen = currentBufferRemaining;
            if (cpyLen > (unsigned) length) cpyLen = length;
            memcpy(currResLoc, currentBuffer, cpyLen);
            length -= cpyLen;
            currResLoc += cpyLen;
            currentBuffer += cpyLen;
            currentBufferRemaining -= cpyLen;
            checkNext();
        }
        assertex(!length);  // fail if not enough data available
        return res;
    }

    virtual RecordLengthType *getNextLength() override
    {
        if (!currentBuffer) 
            return nullptr;
        assertex (currentBufferRemaining >= sizeof(RecordLengthType));
        RecordLengthType *res = (RecordLengthType *) currentBuffer;
        currentBuffer += sizeof(RecordLengthType);
        currentBufferRemaining -= sizeof(RecordLengthType);
        checkNext(); // Note that length is never separated from data... but length can be zero so still need to do this...
        return res;
    }

    void checkNext()
    {
        if (!currentBufferRemaining)
        {
            if (buffers.isItem(bufferIdx+1))
            {
                bufferIdx++;
                currentBuffer = (const byte *) &buffers.item(bufferIdx).get()->data;
                currentBufferRemaining = *(unsigned short *) currentBuffer;
                currentBuffer += sizeof(unsigned short);
            }
            else
            {
                currentBuffer = nullptr;
            }
        }
    }
};

class CLocalMessageResult : implements IMessageResult, public CInterface
{
    void *data;
    void *meta;
    RoxiePacketHeader *header;
    unsigned datalen, metalen, headerlen;
public:
    IMPLEMENT_IINTERFACE;
    CLocalMessageResult(void *_data, unsigned _datalen, void *_meta, unsigned _metalen, RoxiePacketHeader *_header, unsigned _headerlen)
    {
        datalen = _datalen;
        metalen = _metalen;
        headerlen = _headerlen;
        data = _data;
        meta = _meta;
        header = _header;
    }

    ~CLocalMessageResult()
    {
        free(data);
        free(meta);
        free(header);
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const
    {
        return new CLocalMessageUnpackCursor(rowMgr, data, datalen);
    }

    virtual const RoxiePacketHeader *getMessageHeader(unsigned &length) const
    {
        length = headerlen;
        return header;
    }

    virtual const void *getMessageMetadata(unsigned &length) const
    {
        length = metalen;
        return meta;
    }

    virtual void discard() const
    {
    }

};

class CLocalBlockedMessageResult : implements IMessageResult, public CInterface
{
    ArrayOf<roxiemem::OwnedDataBuffer> buffers;
    void *meta;
    RoxiePacketHeader *header;
    unsigned metalen, headerlen;
public:
    IMPLEMENT_IINTERFACE;
    CLocalBlockedMessageResult(ArrayOf<roxiemem::OwnedDataBuffer> &_buffers, void *_meta, unsigned _metalen, RoxiePacketHeader *_header, unsigned _headerlen)
    {
        buffers.swapWith(_buffers);
        metalen = _metalen;
        headerlen = _headerlen;
        meta = _meta;
        header = _header;
    }

    ~CLocalBlockedMessageResult()
    {
        free(meta);
        free(header);
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const
    {
        return new CLocalBlockedMessageUnpackCursor(rowMgr, buffers);
    }

    virtual const RoxiePacketHeader *getMessageHeader(unsigned &length) const
    {
        length = headerlen;
        return header;
    }

    virtual const void *getMessageMetadata(unsigned &length) const
    {
        length = metalen;
        return meta;
    }

    virtual void discard() const
    {
    }

};

class CLocalMessageCollator : implements ILocalMessageCollator, public CInterface
{
    InterruptableSemaphore sem;
    QueueOf<IMessageResult, false> pending;
    CriticalSection crit;
    Linked<IRowManager> rowManager; // Linked to ensure it lives longer than me
    Linked<ILocalReceiveManager> receiveManager;
    ruid_t id;
    unsigned totalBytesReceived;
    bool memLimitExceeded = false;

public:
    IMPLEMENT_IINTERFACE;
    CLocalMessageCollator(IRowManager *_rowManager, ruid_t _ruid);
    ~CLocalMessageCollator();

    virtual ruid_t queryRUID() const 
    {
        return id;
    }

    virtual IMessageResult* getNextResult(unsigned time_out, bool &anyActivity)
    {
        if (memLimitExceeded)
        {
            DBGLOG("LocalCollator: CLocalMessageCollator::getNextResult() throwing memory limit exceeded exception");
            throw MakeStringException(0, "memory limit exceeded");
        }
        anyActivity = false;
        if (!sem.wait(time_out))
            return NULL;
        anyActivity = true;
        CriticalBlock c(crit);
        return pending.dequeue();
    }

    virtual void interrupt(IException *E)
    {
        sem.interrupt(E);
    }

    virtual void enqueueMessage(bool outOfBand, unsigned totalSize, IMessageResult *result) override
    {
        CriticalBlock c(crit);
        if (outOfBand)
            pending.enqueueHead(result);
        else
            pending.enqueue(result);
        sem.signal();
        totalBytesReceived += totalSize;
    }

    virtual bool attachDataBuffers(const ArrayOf<roxiemem::OwnedDataBuffer> &buffers) override
    {
        if (memLimitExceeded)
            return false;
        ForEachItemIn(idx, buffers)
        {
            roxiemem::OwnedDataBuffer dataBuffer = buffers.item(idx);
            if (!dataBuffer.get()->attachToRowMgr(rowManager))
            {
                memLimitExceeded = true;
                interrupt(MakeStringException(0, "memory limit exceeded"));
                return(false);
            }
        }
        return true;
    }

    virtual unsigned queryBytesReceived() const
    {
        return totalBytesReceived;
    }
    virtual unsigned queryDuplicates() const
    {
        return 0;
    }
    virtual unsigned queryResends() const
    {
        return 0;
    }
};

class RoxieLocalReceiveManager : implements ILocalReceiveManager, public CInterface
{
    MapXToMyClass<ruid_t, ruid_t, ILocalMessageCollator> collators;
    CriticalSection crit;

public:
    IMPLEMENT_IINTERFACE;
    RoxieLocalReceiveManager()
    {
    }

    virtual IMessageCollator *createMessageCollator(IRowManager *manager, ruid_t ruid)
    {
        ILocalMessageCollator *collator = new CLocalMessageCollator(manager, ruid);
        CriticalBlock b(crit);
        collators.setValue(ruid, collator);
        return collator;
    }

    virtual void detachCollator(const IMessageCollator *collator)
    {
        ruid_t id = collator->queryRUID();
        CriticalBlock b(crit);
        collators.setValue(id, NULL);
    }

    virtual ILocalMessageCollator *lookupCollator(ruid_t id)
    {
        CriticalBlock b(crit);
        ILocalMessageCollator *ret = collators.getValue(id);
        if (!ret)
            ret = collators.getValue(RUID_DISCARD);
        return LINK(ret);
    }

};

void LocalMessagePacker::flush()
{
    // MORE - I think this means we don't send anything until whole message available in localAgent mode, which
    // may not be optimal.
    data.setLength(lastput);
    Owned<ILocalMessageCollator> collator = rm->lookupCollator(id);
    if (collator)
    {
        unsigned datalen = data.length();
        unsigned metalen = meta.length();
        unsigned headerlen = header.length();
        collator->enqueueMessage(outOfBand, datalen+metalen+headerlen, new CLocalMessageResult(data.detach(), datalen, meta.detach(), metalen, (RoxiePacketHeader *) header.detach(), headerlen));
    }
    // otherwise Roxie server is no longer interested and we can simply discard
}

void LocalBlockedMessagePacker::flush()
{
    if (currentBuffer && (dataPos > sizeof(unsigned short)))
    {
        *(unsigned short *) &currentBuffer->data = dataPos - sizeof(unsigned short);
        buffers.append(currentBuffer);
        currentBuffer = nullptr;
    }
    Owned<ILocalMessageCollator> collator = rm->lookupCollator(id);
    if (collator)
    {
        unsigned metalen = meta.length();
        unsigned headerlen = header.length();
        // NOTE - takes ownership of buffers and leaves it empty
        if (collator->attachDataBuffers(buffers))
            collator->enqueueMessage(outOfBand, totalDataLen+metalen+headerlen, new CLocalBlockedMessageResult(buffers, meta.detach(), metalen, (RoxiePacketHeader *) header.detach(), headerlen));
    }
    // otherwise Roxie server is no longer interested and we can simply discard
}

CLocalMessageCollator::CLocalMessageCollator(IRowManager *_rowManager, ruid_t _ruid) 
    : rowManager(_rowManager), id(_ruid)
{
    totalBytesReceived = 0;
}

CLocalMessageCollator::~CLocalMessageCollator()
{
    IMessageResult *goer;
    for (;;)
    {
        goer = pending.dequeue();
        if (!goer)
            break;
        goer->Release();
    }
}


class RoxieLocalQueueManager : public RoxieReceiverBase
{
    Linked<RoxieLocalReceiveManager> receiveManager;

public:
    RoxieLocalQueueManager(unsigned _numWorkers) : RoxieReceiverBase(_numWorkers)
    {
        receiveManager.setown(new RoxieLocalReceiveManager);
    }
        
    virtual void sendPacket(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx) override
    {
        RoxiePacketHeader &header = packet->queryHeader();
        unsigned retries = header.thisChannelRetries(0);
        if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK )
        {
            if (doTrace(traceRoxiePackets))
            {
                StringBuffer s; 
                DBGLOG("ROXIE_CALLBACK %s", header.toString(s).str());
            }
            // MORE - do we need to encrypt these?
            doFileCallback(packet);
        }
        else if (retries < SUBCHANNEL_MASK)
        {
            if (retries)
            {
                // Send back an out-of-band immediately, to let Roxie server know that channel is still active
                RoxiePacketHeader newHeader(header, ROXIE_ALIVE, 0);
                Owned<IMessagePacker> output = createOutputStream(newHeader, true, logctx);
                output->flush();
                return; // No point sending the retry in localAgent mode
            }
            RoxieQueue *targetQueue;
            switch (header.activityId & ROXIE_PRIORITY_MASK)
            {
                case ROXIE_SLA_PRIORITY: targetQueue = &slaQueue; break;
                case ROXIE_HIGH_PRIORITY: targetQueue = &hiQueue; break;
                case ROXIE_LOW_PRIORITY: targetQueue = &loQueue; break;
                default: targetQueue = &bgQueue; break;
            }
            Owned<ISerializedRoxieQueryPacket> serialized = packet->serialize();
            if (header.channel)
            {
                targetQueue->enqueue(serialized.getClear(), 0);
            }
            else
            {
                // Turn broadcast packet (channel 0), as early as possible, into non-0 channel packets.
                // So retries and other communication with Roxie server (which uses non-0 channel numbers) will not cause double work or confusion.
                for (unsigned i = 1; i < numChannels; i++)
                    targetQueue->enqueue(serialized->cloneSerializedPacket(i+1), 0);
                header.retries |= ROXIE_BROADCAST;
                header.channel = 1;
                targetQueue->enqueue(serialized.getClear(), 0);
            }
        }
    }

    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx, unsigned subChannel) override
    {
        // Don't do IBYTI's when local agent - no buddy to talk to anyway
    }

    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieLocalQueueManager::sendAbort");
        RoxiePacketHeader abortHeader(header, header.activityId & ROXIE_PRIORITY_MASK, 0);
        abortHeader.retries = QUERY_ABORTED;
        if (doTrace(traceRoxiePackets, TraceFlags::Max))
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT packet %s", abortHeader.toString(s).str());
        }
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader);
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieLocalQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK, 0);
        abortHeader.retries = QUERY_ABORTED;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (doTrace(traceRoxieFiles))
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx) override
    {
        if (blockedLocalAgent)
            return new LocalBlockedMessagePacker(header, outOfBand, receiveManager);
        else
            return new LocalMessagePacker(header, outOfBand, receiveManager);
    }

    virtual IReceiveManager *queryReceiveManager() override
    {
        return receiveManager;
    }

    virtual bool replyPending(RoxiePacketHeader &header) override
    {
        // MORE - should really have some code here! But returning true is a reasonable approximation.
        return true;
    }

    virtual bool abortCompleted(RoxiePacketHeader &header) override
    {
        // MORE - should really have some code here!
        return false;
    }

    virtual void abortPendingData(const SocketEndpoint &ep) override
    {
        // Shouldn't ever happen, I think
    }

};


extern IRoxieOutputQueueManager *createOutputQueueManager(unsigned numWorkers, bool encrypted)
{
    if (localAgent)
        return new RoxieLocalQueueManager(numWorkers);
    else
        return new RoxieUdpSocketQueueManager(numWorkers, encrypted);

}

//================================================================================================================================

class PacketDiscarder : public Thread, implements IPacketDiscarder
{
    bool aborted;
    Owned<IRowManager> rowManager;
    Owned<IMessageCollator> mc;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    PacketDiscarder()
    {
        aborted = false;
    };

    ~PacketDiscarder()
    {
        if (mc)
            ROQ->queryReceiveManager()->detachCollator(mc);
        mc.clear();
    }

    virtual int run()
    {
        Owned<StringContextLogger> logctx = new StringContextLogger("PacketDiscarder");
        rowManager.setown(roxiemem::createRowManager(0, NULL, *logctx, NULL, false));
        mc.setown(ROQ->queryReceiveManager()->createMessageCollator(rowManager, RUID_DISCARD));
        try
        {
            while (!aborted)
            {
                bool anyActivity = false;
                Owned<IMessageResult> mr = mc->getNextResult(5000, anyActivity);
                if (mr)
                {
                    if (doTrace(traceRoxiePackets))
                        DBGLOG("Discarding unwanted message");
                    unsigned headerLen;
                    const RoxiePacketHeader &header = *(const RoxiePacketHeader *) mr->getMessageHeader(headerLen);
                    if (headerLen)
                    {
                        switch (header.activityId)
                        {
                            case ROXIE_FILECALLBACK:
                            {
                                Owned<IMessageUnpackCursor> callbackData = mr->getCursor(rowManager);
                                RecordLengthType *rowlen = callbackData->getNextLength();
                                if (rowlen)
                                {
                                    OwnedConstRoxieRow row = callbackData->getNext(*rowlen);
                                    const char *rowdata = (const char *) row.get();
                                    // bool isOpt = * (bool *) rowdata;
                                    // bool isLocal = * (bool *) (rowdata+1);
                                    ROQ->sendAbortCallback(header, rowdata+2, *logctx);
                                }
                                else
                                    DBGLOG("Unrecognized format in discarded file callback");
                                break;
                            }
                            // MORE - ROXIE_ALIVE perhaps should go here too? debug callbacks? Actually any standard query results should too (though by the time I see them here it's too late (that may change once start streaming)
                        }
                    }
                    else
                        DBGLOG("Unwanted message had no header?!");
                }
            }
        }
        catch (IException * E)
        {
            if (!aborted || QUERYINTERFACE(E, InterruptedSemaphoreException) == NULL)
                EXCLOG(E);
            ::Release(E);
        }
        return 0;
    }

    virtual void start() override
    {
        Thread::start(false);
    }

    virtual void stop()
    {
        if (mc)
            mc->interrupt();
        aborted = true;
        join();
    }

};

IPacketDiscarder *createPacketDiscarder()
{
    IPacketDiscarder *packetDiscarder = new PacketDiscarder;
    packetDiscarder->start();
    return packetDiscarder;
}
