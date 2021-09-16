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
#ifdef TIME_PACKETS
    tick = 0;
#endif
    init(_remoteId, _uid, _channel, _overflowSequence);
}

RoxiePacketHeader::RoxiePacketHeader(const RoxiePacketHeader &source, unsigned _activityId, unsigned subChannel) : serverId(source.serverId)
{
    // Used to create the header to send a callback to originating server or an IBYTI to a buddy
    activityId = _activityId;
    uid = source.uid;
    queryHash = source.queryHash;
    channel = source.channel;
    overflowSequence = source.overflowSequence;
    continueSequence = source.continueSequence;
    if (_activityId >= ROXIE_ACTIVITY_SPECIAL_FIRST && _activityId <= ROXIE_ACTIVITY_SPECIAL_LAST)
        overflowSequence |= OUTOFBAND_SEQUENCE; // Need to make sure it is not treated as dup of actual reply in the udp layer
    retries = getSubChannelMask(subChannel) | (source.retries & ~ROXIE_RETRIES_MASK);
#ifdef TIME_PACKETS
    tick = source.tick;
#endif
#ifdef SUBCHANNELS_IN_HEADER
    memcpy(subChannels, source.subChannels, sizeof(subChannels));
#endif
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
    if (traceLevel > 9)
    {
        StringBuffer s;
        DBGLOG("Calculating hash: %s hash was %d", toString(s).str(), hash);
    }
    return hash;
}

void RoxiePacketHeader::copy(const RoxiePacketHeader &oh)
{
    // used for saving away kill packets for later matching by match
    uid = oh.uid;
    overflowSequence = oh.overflowSequence;
    continueSequence = oh.continueSequence;
    serverId = oh.serverId;
    channel = oh.channel;
    // MORE - would it be safer, maybe even faster to copy the rest too?
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
#ifdef SUBCHANNELS_IN_HEADER
    clearSubChannels();
#endif
#ifdef TIME_PACKETS
    tick = 0;
#else
    filler = 0; // keeps valgrind happy
#endif
}

#ifdef SUBCHANNELS_IN_HEADER
void RoxiePacketHeader::clearSubChannels()
{
    for (unsigned idx = 0; idx < MAX_SUBCHANNEL; idx++)
        subChannels[idx].clear();
}
#endif

StringBuffer &RoxiePacketHeader::toString(StringBuffer &ret) const
{
    const IpAddress serverIP = serverId.getIpAddress();
    ret.append("activityId=");
    switch(activityId & ~ROXIE_PRIORITY_MASK)
    {
    case 0: ret.append("IBYTI"); break;
    case ROXIE_UNLOAD: ret.append("ROXIE_UNLOAD"); break;
    case ROXIE_PING: ret.append("ROXIE_PING"); break;
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
    ret.appendf(" uid=" RUIDF " pri=", uid);
    switch(activityId & ROXIE_PRIORITY_MASK)
    {
        case ROXIE_SLA_PRIORITY: ret.append("SLA"); break;
        case ROXIE_HIGH_PRIORITY: ret.append("HIGH"); break;
        case ROXIE_LOW_PRIORITY: ret.append("LOW"); break;
        default: ret.append("???"); break;
    }
    ret.appendf(" queryHash=%" I64F "x ch=%u seq=%d cont=%d server=", queryHash, channel, overflowSequence, continueSequence);
    serverIP.getIpText(ret);
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
#ifdef SUBCHANNELS_IN_HEADER
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
#endif
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

//============================================================================================

// This function maps a agent number to the multicast ip used to talk to it.

IpAddress multicastBase("239.1.1.1");           // TBD IPv6 (need IPv6 multicast addresses?
IpAddress multicastLast("239.1.5.254");

const IpAddress &getChannelIp(IpAddress &ip, unsigned _channel)
{
    // need to be careful to avoid the .0's and the .255's (not sure why...)
    ip = multicastBase;
    if (!ip.ipincrement(_channel,1,254,1,0xffff)
        ||(ip.ipcompare(multicastLast)>0))
        throw MakeStringException(ROXIE_MULTICAST_ERROR, "Out-of-range multicast channel %d", _channel);
    return ip;
}

static Owned<ISocket> multicastSocket;

void joinMulticastChannel(unsigned channel)
{
    IpAddress multicastIp;
    getChannelIp(multicastIp, channel);
    SocketEndpoint ep(ccdMulticastPort, multicastIp);
    StringBuffer epStr;
    ep.getUrlStr(epStr);
    if (!multicastSocket->join_multicast_group(ep))
        throw MakeStringException(ROXIE_MULTICAST_ERROR, "Failed to join multicast channel %d (%s)", channel, epStr.str());
    if (traceLevel)
        DBGLOG("Joined multicast channel %d (%s)", channel, epStr.str());
}

static SocketEndpointArray multicastEndpoints;  // indexed by channel

void setMulticastEndpoints(unsigned numChannels)
{
    for (unsigned channel = 0; channel <= numChannels; channel++)  // NOTE - channel 0 is special, and numChannels does not include it
    {
        IpAddress multicastIp;
        getChannelIp(multicastIp, channel);
        multicastEndpoints.append(SocketEndpoint(ccdMulticastPort, multicastIp));
    }
}


void openMulticastSocket()
{
    if (!multicastSocket)
    {
        multicastSocket.setown(ISocket::udp_create(ccdMulticastPort));
        if (multicastTTL)
        {
            multicastSocket->set_ttl(multicastTTL);
            DBGLOG("Roxie: multicastTTL: %u", multicastTTL);
        }
        else
            DBGLOG("Roxie: multicastTTL not set");
        multicastSocket->set_receive_buffer_size(udpMulticastBufferSize);
        size32_t actualSize = multicastSocket->get_receive_buffer_size();
        if (actualSize < udpMulticastBufferSize)
        {
            DBGLOG("Roxie: multicast socket buffer size could not be set (requested=%d actual %d", udpMulticastBufferSize, actualSize);
            throwUnexpected();
        }
        if (traceLevel)
            DBGLOG("Roxie: multicast socket created port=%d sockbuffsize=%d actual %d", ccdMulticastPort, udpMulticastBufferSize, actualSize);
        if (roxieMulticastEnabled && !localAgent)
        {
            Owned<const ITopologyServer> topology = getTopology();
            for (unsigned channel : topology->queryChannels())
            {
                assertex(channel);
                joinMulticastChannel(channel);
            }
            joinMulticastChannel(0); // all agents also listen on channel 0

        }
    }
}

void closeMulticastSockets()
{
    multicastSocket.clear();
}

static bool channelWrite(RoxiePacketHeader &buf, bool includeSelf)
{
    size32_t minwrote = 0;
    if (roxieMulticastEnabled)
    {
        return multicastSocket->udp_write_to(multicastEndpoints.item(buf.channel), &buf, buf.packetlength) == buf.packetlength;
    }
    else
    {
#ifdef SUBCHANNELS_IN_HEADER
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
                if (traceRoxiePackets)
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
                if (traceRoxiePackets)
                {
                    StringBuffer s, header;
                    DBGLOG("Writing %d bytes to subchannel %d (%s) %s", buf.packetlength, subChannel, buf.subChannels[subChannel].getTraceText(s).str(), buf.toString(header).str());
                }
                SocketEndpoint ep(ccdMulticastPort, buf.subChannels[subChannel].getIpAddress());
                size32_t wrote = multicastSocket->udp_write_to(ep, &buf, buf.packetlength);
                if (!subChannel || wrote < minwrote)
                    minwrote = wrote;
                if (delaySubchannelPackets)
                    MilliSleep(100);
            }
            else if (traceRoxiePackets)
            {
                StringBuffer s, header;
                DBGLOG("NOT writing %d bytes to subchannel %d (%s) %s", buf.packetlength, subChannel, buf.subChannels[subChannel].getTraceText(s).str(), buf.toString(header).str());
            }
        }
#else
        Owned<const ITopologyServer> topo = getTopology();
        const SocketEndpointArray &eps = topo->queryAgents(buf.channel);
        if (!eps.ordinality())
            throw makeStringExceptionV(0, "No agents available for channel %d", buf.channel);
        ForEachItemIn(idx, eps)
        {
            size32_t wrote = multicastSocket->udp_write_to(eps.item(idx), &buf, buf.packetlength);
            if (!idx || wrote < minwrote)
                minwrote = wrote;
        }
#endif
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

    CRoxieQueryPacketBase(const void *_data, int lengthRemaining) : data((RoxiePacketHeader *) _data)
    {
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
            lengthRemaining--;
            if (*finger++ & LOGGING_DEBUGGERACTIVE)
            {
                assertex(lengthRemaining >= (int) sizeof(unsigned short));
                unsigned short debugLen = *(unsigned short *) finger;
                finger += debugLen + sizeof(unsigned short);
                lengthRemaining -= debugLen + sizeof(unsigned short);
            }
            for (;;)
            {
                assertex(lengthRemaining>0);
                if (!*finger)
                {
                    lengthRemaining--;
                    finger++;
                    break;
                }
                lengthRemaining--;
                finger++;
            }
            traceLength = finger - traceInfo;
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
    
public:
    IMPLEMENT_IINTERFACE;
    CRoxieQueryPacket(const void *_data, int length) : CRoxieQueryPacketBase(_data, length)
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
        return createRoxiePacket(newdata, length);
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
        return createRoxiePacket(newdata, newDataSize);
    }

    virtual ISerializedRoxieQueryPacket *serialize() const override
    {
        unsigned length = data->packetlength;
        MemoryBuffer mb;
        if (encryptInTransit)
        {
            const byte *plainData = (const byte *) (data+1);
            plainData += traceLength;
            unsigned plainLen = length - sizeof(RoxiePacketHeader) - traceLength;
            mb.append(sizeof(RoxiePacketHeader)+traceLength, data);  // Header and traceInfo are unencrypted
            const MemoryAttr &udpkey = getSecretUdpKey(true);
            aesEncrypt(udpkey.get(), udpkey.length(), plainData, plainLen, mb);   // Encrypt everything else
            RoxiePacketHeader *newHeader = (RoxiePacketHeader *) mb.toByteArray();
            newHeader->packetlength = mb.length();
        }
        else
        {
            mb.append(length, data);
        }
        return createSerializedRoxiePacket(mb);
    }
};

// CNocryptRoxieQueryPacket implements both serialized and deserialized packet interfaces, to avoid additional copy operations when
// using localAgent mode.

class CNocryptRoxieQueryPacket: public CRoxieQueryPacket, implements ISerializedRoxieQueryPacket
{
public:
    IMPLEMENT_IINTERFACE;
    CNocryptRoxieQueryPacket(const void *_data, int length) : CRoxieQueryPacket(_data, length)
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
        return new CNocryptRoxieQueryPacket(newdata, length);
    }

    virtual ISerializedRoxieQueryPacket *serialize() const override
    {
        return const_cast<CNocryptRoxieQueryPacket *>(LINK(this));
    }

    virtual IRoxieQueryPacket *deserialize() const override
    {
        return const_cast<CNocryptRoxieQueryPacket *>(LINK(this));
    }
};

class CSerializedRoxieQueryPacket : public CRoxieQueryPacketBase, implements ISerializedRoxieQueryPacket
{
public:
    IMPLEMENT_IINTERFACE;
    CSerializedRoxieQueryPacket(const void *_data, int length) : CRoxieQueryPacketBase(_data, length)
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
        return new CSerializedRoxieQueryPacket(newdata, length);
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

};

extern IRoxieQueryPacket *createRoxiePacket(void *_data, unsigned _len)
{
    if (!encryptInTransit)
        return new CNocryptRoxieQueryPacket(_data, _len);
    if ((unsigned short)_len != _len)
    {
        StringBuffer s;
        RoxiePacketHeader *header = (RoxiePacketHeader *) _data;
        header->toString(s);
        free(_data);
        throw MakeStringException(ROXIE_PACKET_ERROR, "Packet length %d exceeded maximum sending packet %s", _len, s.str());
    }
    return new CRoxieQueryPacket(_data, _len);
}

extern IRoxieQueryPacket *createRoxiePacket(MemoryBuffer &m)
{
    unsigned length = m.length(); // don't make assumptions about evaluation order of parameters...
    return createRoxiePacket(m.detachOwn(), length);
}

extern IRoxieQueryPacket *deserializeCallbackPacket(MemoryBuffer &m)
{
    // Direct decryption of special packets - others are only decrypted after being dequeued
    if (encryptInTransit)
    {
        RoxiePacketHeader *header = (RoxiePacketHeader *) m.toByteArray();
        assertex(header != nullptr);
        assertex(header->activityId == ROXIE_FILECALLBACK || header->activityId == ROXIE_DEBUGCALLBACK);
        assertex(m.length() >= header->packetlength);
        unsigned encryptedLen = header->packetlength - sizeof(RoxiePacketHeader);
        const void *encryptedData = (const void *)(header+1);
        MemoryBuffer decrypted;
        decrypted.append(sizeof(RoxiePacketHeader), header);
        decrypted.ensureCapacity(encryptedLen);  // May be up to 16 bytes smaller...
        const MemoryAttr &udpkey = getSecretUdpKey(true);
        aesDecrypt(udpkey.get(), udpkey.length(), encryptedData, encryptedLen, decrypted);
        unsigned length = decrypted.length();
        RoxiePacketHeader *newHeader = (RoxiePacketHeader *) decrypted.detachOwn();
        newHeader->packetlength = length;
        return createRoxiePacket(newHeader, length);
    }
    else
    {
        unsigned length = m.length(); // don't make assumptions about evaluation order of parameters...
        return createRoxiePacket(m.detachOwn(), length);
    }
}

extern ISerializedRoxieQueryPacket *createSerializedRoxiePacket(MemoryBuffer &m)
{
    unsigned length = m.length(); // don't make assumptions about evaluation order of parameters...
    return new CSerializedRoxieQueryPacket(m.detachOwn(), length);
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
            unsigned traceLength = packet->getTraceLength();
            unsigned char loggingFlags = *traceInfo;
            if (loggingFlags & LOGGING_FLAGSPRESENT) // should always be true.... but this flag is handy to avoid flags byte ever being NULL
            {
                traceInfo++;
                traceLength--;
                if (loggingFlags & LOGGING_INTERCEPTED)
                    intercept = true;
                if (loggingFlags & LOGGING_TRACELEVELSET)
                {
                    ctxTraceLevel = (*traceInfo++ - 1); // avoid null byte here in case anyone still thinks there's just a null-terminated string
                    traceLength--;
                }
                if (loggingFlags & LOGGING_BLIND)
                    blind = true;
                if (loggingFlags & LOGGING_CHECKINGHEAP)
                    checkingHeap = true;
                if (loggingFlags & LOGGING_DEBUGGERACTIVE)
                {
                    assertex(traceLength > sizeof(unsigned short));
                    debuggerActive = true;
                    unsigned short debugLen = *(unsigned short *) traceInfo;
                    traceInfo += debugLen + sizeof(unsigned short);
                    traceLength -= debugLen + sizeof(unsigned short);
                }
                // Passing the wuid via the logging context prefix is a lot of a hack...
                if (loggingFlags & LOGGING_WUID)
                {
                    unsigned wuidLen = 0;
                    while (wuidLen < traceLength)
                    {
                        if (traceInfo[wuidLen]=='@'||traceInfo[wuidLen]==':')
                            break;
                        wuidLen++;
                    }
                    wuid.set((const char *) traceInfo, wuidLen);
                }
            }
            s.append(traceLength, (const char *) traceInfo);
            s.append("|");
        }
        channel = header.channel;
        ip.getIpText(s);
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
    RoxiePacketHeader header(unloadId, 0, 0, 0);

    MemoryBuffer mb;
    mb.append(sizeof(RoxiePacketHeader), &header);
    mb.append((char) LOGGING_FLAGSPRESENT);
    mb.append(id);
    if (traceLevel > 1)
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
};

void doPing(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx)
{
    const RoxiePacketHeader &header = packet->queryHeader();
    const IpAddress serverIP = header.serverId.getIpAddress();
    unsigned contextLength = packet->getContextLength();
    if (contextLength != sizeof(PingRecord))
    {
        StringBuffer s;
        throw MakeStringException(ROXIE_UNKNOWN_SERVER, "Unexpected data size %d (expected %d) in PING: %s", contextLength, (unsigned) sizeof(PingRecord), header.toString(s).str());
    }
    const PingRecord *data = (const PingRecord *) packet->queryContextData();
    if (!serverIP.ipequals(data->senderIP))
    {
        StringBuffer s;
        throw MakeStringException(ROXIE_UNKNOWN_SERVER, "Message received from unknown Roxie server %s", header.toString(s).str());
    }
    RoxiePacketHeader newHeader(header, ROXIE_PING, 0);  // subchannel not relevant
    Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
    void *ret = output->getBuffer(contextLength, false);
    memcpy(ret, data, contextLength);
    output->putBuffer(ret, contextLength, false);
    output->flush();
}

//=================================================================================

static ThreadId roxiePacketReaderThread = 0;

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
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
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
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
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
    CriticalSection qcrit;
    unsigned headRegionSize;
    unsigned numWorkers;
    RelaxedAtomic<unsigned> started;
    std::atomic<unsigned> idle;
    IBYTIbuffer *myIBYTIbuffer = nullptr;

    void noteQueued()
    {
        maxQueueLength.store_max(++queueLength);
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

    RoxieQueue(unsigned _headRegionSize, unsigned _numWorkers)
    {
        headRegionSize = _headRegionSize;
        numWorkers = _numWorkers;
        workers.setown(createThreadPool("RoxieWorkers", this, NULL, numWorkers));
        started = 0;
        idle = 0;
        if (IBYTIbufferSize)
            myIBYTIbuffer = new IBYTIbuffer(IBYTIbufferSize);
    }

    ~RoxieQueue()
    {
        delete myIBYTIbuffer;
    }


    virtual IPooledThread *createNew();
    void abortChannel(unsigned channel);

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

    void enqueue(ISerializedRoxieQueryPacket *x)
    {
        {
#ifdef TIME_PACKETS
            x->queryHeader().tick = msTick();
#endif
            CriticalBlock qc(qcrit);
            waiting.enqueue(x);
            noteQueued();
        }
        available.signal();
    }

    void enqueueUnique(ISerializedRoxieQueryPacket *x, unsigned subChannel)
    {
        RoxiePacketHeader &header = x->queryHeader();
#ifdef TIME_PACKETS
        header.tick = msTick();
#endif
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
                waiting.enqueue(x);
        }
        if (found)
        {
            if (traceLevel > 0)
            {
                StringBuffer xx;
                AgentContextLogger l(x);
                l.CTXLOG("Ignored retry on subchannel %u for queued activity %s", subChannel, header.toString(xx).str());
            }
            if (!subChannel)
                retriesIgnoredPrm++;
            else
                retriesIgnoredSec++;
            x->Release();
        }
        else
        {
            available.signal();
            noteQueued();
            if (traceLevel > 10)
            {
                AgentContextLogger l(x);
                StringBuffer xx; 
                l.CTXLOG("enqueued %s", header.toString(xx).str());
            }
        }
    }

    bool remove(RoxiePacketHeader &x)
    {
        unsigned scanLength = 0;
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
                    scanLength++;
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
            queueLength--;
            if (scanLength > maxScanLength)
                maxScanLength = scanLength;
            totScanLength += scanLength;
            totScans++;
            return true;
        }
        else
            return false;
    }

    void wait()
    {
        idle++;
        available.wait();
        idle--;
    }

    void signal(unsigned num)
    {
        available.signal(num);
    }

    ISerializedRoxieQueryPacket *dequeue()
    {
        CriticalBlock qc(qcrit);
        unsigned lim = waiting.ordinality();
        if (lim)
        {
            if (headRegionSize)
            {
                if (lim > headRegionSize)
                    lim = headRegionSize;
                return waiting.dequeue(fastRand() % lim);
            }
            return waiting.dequeue();
        }
        else
            return NULL;
    }

    unsigned getHeadRegionSize() const
    {
        return headRegionSize;
    }

    unsigned setHeadRegionSize(unsigned newsize)
    {
        unsigned ret = headRegionSize;
        headRegionSize = newsize;
        return ret;
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
#ifndef NEW_IBYTI
    Semaphore ibytiSem;
#endif
    bool stopped;
    bool abortJob;
    bool busy;
    Owned<IRoxieAgentActivity> activity;
    Owned<IRoxieQueryPacket> packet;
    Owned<const ITopologyServer> topology;
    AgentContextLogger logctx;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieWorker()
    {
        queue = NULL;
        stopped = false;
        busy = false;
        abortJob = false;
    }
    virtual void init(void *_r) override
    {
        queue = (RoxieQueue *) _r;
        stopped = false;
        busy = false;
        abortJob = false;
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
        CriticalBlock b(actCrit);
        activity.setown(act);
    }
    inline bool match(RoxiePacketHeader &h)
    {
        // There is a window between getting packet from queue and being able to match it. 
        // This could cause some deduping to fail, but it does not matter if it does (so long as it is rare!)
        CriticalBlock b(actCrit);
        return packet && packet->queryHeader().matchPacket(h);
    }

    void abortChannel(unsigned channel)
    {
        CriticalBlock b(actCrit);
        if (packet && packet->queryHeader().channel==channel)
        {
            abortJob = true;
#ifndef NEW_IBYTI
            if (doIbytiDelay) 
                ibytiSem.signal();
#endif
            if (activity) 
                activity->abort();
        }
    }

    bool checkAbort(RoxiePacketHeader &h, bool checkRank, bool &queryFound, bool &preActivity)
    {
        CriticalBlock b(actCrit);
        if (packet && packet->queryHeader().matchPacket(h))
        {
            queryFound = true;
            abortJob = true;
#ifndef NEW_IBYTI
            if (doIbytiDelay)
                ibytiSem.signal();
#endif
            if (activity) 
            {
                // Try to stop/abort a job after it starts only if IBYTI comes from a higher priority agent 
                // (more primary in the rank). The agents with higher rank will hold the lower bits of the retries field in IBYTI packet).
#ifdef SUBCHANNELS_IN_HEADER
                if (!checkRank || h.getRespondingSubChannel() < h.mySubChannel())
#else
                if (!checkRank || topology->queryChannelInfo(h.channel).otherAgentHasPriority(h.priorityHash(), h.getRespondingSubChannel()))
#endif
                {
                    activity->abort();
                    return true;
                }
                else 
                {
                    return false;
                }
            }
            if (busy) 
            {
                preActivity = true;
                return true;
            }
        }
        return false;
    }

    void throwRemoteException(IException *E, IRoxieAgentActivity *activity, IRoxieQueryPacket *packet, bool isUser)
    {
        try 
        {
            if (activity && (logctx.queryTraceLevel() > 1))
            {
                StringBuffer act;
                activity->toString(act);
                logctx.CTXLOG("throwRemoteException, activity %s, isUser=%d", act.str(), (int) isUser);
                if (!isUser)
                    EXCLOG(E, "throwRemoteException");
            }
            
            RoxiePacketHeader &header = packet->queryHeader();
#ifdef SUBCHANNELS_IN_HEADER
            unsigned mySubChannel = header.mySubChannel();
#else
            unsigned mySubChannel = topology->queryChannelInfo(header.channel).subChannel();
#endif
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
                    if (logctx.queryTraceLevel() > 1) 
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
        hash64_t queryHash = packet->queryHeader().queryHash;
        unsigned activityId = packet->queryHeader().activityId & ~ROXIE_PRIORITY_MASK;
        Owned<IQueryFactory> queryFactory = getQueryFactory(queryHash, channel);
#ifdef SUBCHANNELS_IN_HEADER
        unsigned mySubChannel = header.mySubChannel();
#else
        unsigned numAgents = topology->queryAgents(channel).ordinality();
        unsigned mySubChannel = topology->queryChannelInfo(channel).subChannel();
#endif
        if (!queryFactory && logctx.queryWuid())
        {
            Owned <IRoxieDaliHelper> daliHelper = connectToDali();
            Owned<IConstWorkUnit> wu = daliHelper->attachWorkunit(logctx.queryWuid(), NULL);
            queryFactory.setown(createAgentQueryFactoryFromWu(wu, channel));
            if (queryFactory)
                cacheOnDemandQuery(queryHash, channel, queryFactory);
        }
        if (!queryFactory)
        {
            StringBuffer hdr;
            IException *E = MakeStringException(MSGAUD_operator, ROXIE_UNKNOWN_QUERY, "Roxie agent received request for unregistered query: %s", packet->queryHeader().toString(hdr).str());
            EXCLOG(E, "doActivity");
            throwRemoteException(E, activity, packet, false);
            return;
        }
        try
        {   
            bool debugging = logctx.queryDebuggerActive();
            if (debugging)
            {
                if (mySubChannel)
                    abortJob = true;  // when debugging, we always run on primary only...
            }
#ifndef NEW_IBYTI
#ifdef SUBCHANNELS_IN_HEADER
            else if (doIbytiDelay && mySubChannel)
            {
                unsigned delay = 0;
                for (unsigned subChannel = 0; subChannel < mySubChannel; subChannel++)
                    delay += getIbytiDelay(header.subChannels[subChannel].getIpAddress());
                unsigned start = 0;
                if (traceRoxiePackets)
                {
                    StringBuffer x;
                    DBGLOG("YES myTurnToDelay subchannel=%u delay=%u %s", mySubChannel, delay, header.toString(x).str());
                    start = msTick();
                }
                if (delay)
                    ibytiSem.wait(delay);
                if (traceRoxiePackets)
                {
                    StringBuffer x;
                    DBGLOG("Delay done, abortJob=%d, elapsed=%d", (int) abortJob, msTick()-start);
                }
                if (!abortJob)
                {
                    for (unsigned subChannel = 0; subChannel < mySubChannel; subChannel++)
                        noteNodeSick(header.subChannels[subChannel]);
                }
            }
#else
            else if (doIbytiDelay && (numAgents > 1))
            {
                unsigned hdrHashVal = header.priorityHash();
                unsigned primarySubChannel = (hdrHashVal % numAgents);
                if (primarySubChannel != mySubChannel)
                {
                    unsigned delay = topology->queryChannelInfo(channel).getIbytiDelay(primarySubChannel);
                    if (logctx.queryTraceLevel() > 6)
                    {
                        StringBuffer x;
                        logctx.CTXLOG("YES myTurnToDelayIBYTI subchannel=%u delay=%u hash=%u %s", mySubChannel, delay, hdrHashVal, header.toString(x).str());
                    }
                    
                    // MORE: if we are dealing with a query that was on channel 0, we may want a longer delay 
                    // (since the theory about duplicated work not mattering when cluster is idle does not hold up)

                    if (delay)
                    {
                        ibytiSem.wait(delay);
                        if (!abortJob)
                            topology->queryChannelInfo(channel).noteChannelsSick(primarySubChannel);
                        if (logctx.queryTraceLevel() > 8)
                        {
                            StringBuffer x;
                            logctx.CTXLOG("Buddy did%s send IBYTI, updated delay : %s",
                                abortJob ? "" : " NOT", header.toString(x).str());
                        }
                    }
                }
                else
                {
#ifndef NO_IBYTI_DELAYS_COUNT
                    if (!mySubChannel)
                        ibytiNoDelaysPrm++;
                    else
                        ibytiNoDelaysSec++;
#endif
                    if (logctx.queryTraceLevel() > 6)
                    {
                        StringBuffer x;
                        logctx.CTXLOG("NOT myTurnToDelayIBYTI subchannel=%u hash=%u %s", mySubChannel, hdrHashVal, header.toString(x).str());
                    }
                }
            }
#endif
#endif
            if (abortJob)
            {
                CriticalBlock b(actCrit);
                busy = false;  // Keep order - before setActivity below
                if (logctx.queryTraceLevel() > 5)
                {
                    StringBuffer x;
                    logctx.CTXLOG("Stop before processing - activity aborted %s", header.toString(x).str());
                }
                return;
            }
            if (!debugging)
                ROQ->sendIbyti(header, logctx, mySubChannel);
            activitiesStarted++;
            Owned <IAgentActivityFactory> factory = queryFactory->getAgentActivityFactory(activityId);
            assertex(factory);
            setActivity(factory->createActivity(logctx, packet));
            Owned<IMessagePacker> output = activity->process();
            if (logctx.queryTraceLevel() > 5)
            {
                StringBuffer x;
                logctx.CTXLOG("done processing %s", header.toString(x).str());
            }
            if (output)
            {
                activitiesCompleted++;
                busy = false; // Keep order - before setActivity below
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
        busy = false; // Keep order - before setActivity below
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
                    agentsActive++;
                    maxAgentsActive.store_max(agentsActive);
                    abortJob = false;
                    busy = true;
#ifndef NEW_IBYTI
                    if (doIbytiDelay) 
                        ibytiSem.reinit(0U); // Make sure sem is is in no-signaled state
#endif
                    Owned<ISerializedRoxieQueryPacket> next = queue->dequeue();
                    if (next)
                    {
                        logctx.set(next);
                        packet.setown(next->deserialize());
                        next.clear();
                        queueLength--;
                        RoxiePacketHeader &header = packet->queryHeader();
#ifdef TIME_PACKETS
                        {
                            unsigned now = msTick();
                            unsigned packetWait = now-header.tick;
                            header.tick = now;
                            packetWaitMax.store_max(packetWait);
                            packetWaitElapsed += packetWait;
                            packetWaitCount++;
                        }
#endif
                        topology.setown(getTopology());
                        if (logctx.queryTraceLevel() > 10)
                        {
                            StringBuffer x;
                            logctx.CTXLOG("dequeued %s", header.toString(x).str());
                        }
                        if ((header.activityId & ~ROXIE_PRIORITY_MASK) == ROXIE_UNLOAD)
                        {
                            doUnload(packet, logctx);
                        }
                        else if ((header.activityId & ~ROXIE_PRIORITY_MASK) == ROXIE_PING)
                        {
                            doPing(packet, logctx);
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

#ifdef TIME_PACKETS
                        {
                            unsigned now = msTick();
                            unsigned packetRun = now-header.tick;
                            packetRunMax.store_max(packetRun);
                            packetRunElapsed += packetRun;
                            packetRunCount++;
                        }
#endif
                    }
                    busy = false;
                    {
                        CriticalBlock b(actCrit);
                        packet.clear();
                        topology.clear();
                        logctx.set(NULL);
                    }
                    agentsActive--;
                }
            }
            catch(IException *E)
            {
                CriticalBlock b(actCrit);
                EXCLOG(E);
                if (packet)
                {
                    throwRemoteException(E, NULL, packet, false);
                    packet.clear();
                }
                else
                    E->Release();
                topology.clear();
            }
            catch(...)
            {
                CriticalBlock b(actCrit);
                Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception in Roxie worker thread");
                EXCLOG(E);
                if (packet)
                {
                    throwRemoteException(E.getClear(), NULL, packet, false);
                    packet.clear();
                }
                topology.clear();
            }
        }
    }
};

IPooledThread *RoxieQueue::createNew()
{
    return new CRoxieWorker;
}

void RoxieQueue::abortChannel(unsigned channel)
{
    Owned<IPooledThreadIterator> wi = workers->running();
    ForEach(*wi)
    {
        CRoxieWorker &w = (CRoxieWorker &) wi->query();
        w.abortChannel(channel);
    }
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
    RoxieQueue slaQueue;
    RoxieQueue hiQueue;
    RoxieQueue loQueue;
    unsigned numWorkers;

public:
    IMPLEMENT_IINTERFACE;

    RoxieReceiverBase(unsigned _numWorkers) : slaQueue(headRegionSize, _numWorkers), hiQueue(headRegionSize, _numWorkers), loQueue(headRegionSize, _numWorkers), numWorkers(_numWorkers)
    {
    }

    virtual unsigned getHeadRegionSize() const
    {
        return loQueue.getHeadRegionSize();
    }

    virtual void setHeadRegionSize(unsigned newSize)
    {
        slaQueue.setHeadRegionSize(newSize);
        hiQueue.setHeadRegionSize(newSize);
        loQueue.setHeadRegionSize(newSize);
    }

    virtual void start() 
    {
        loQueue.start();
        hiQueue.start();
        slaQueue.start();
    }

    virtual void stop() 
    {
        loQueue.stopAll();
        hiQueue.stopAll();
        slaQueue.stopAll();
    }

    virtual void join()  
    { 
        loQueue.join();
        hiQueue.join();
        slaQueue.join();
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
                if (traceLevel > 10)
                    DBGLOG("callback return matched a waiting query"); 
                c.doFileCallback(len, data, header.retries==QUERY_ABORTED);
            }
        }
    }
};

#ifdef _MSC_VER
#pragma warning ( push )
#pragma warning ( disable: 4355 )
#endif

class RoxieThrottledPacketSender : public Thread
{
    TokenBucket &bucket;
    InterruptableSemaphore queued;
    Semaphore started;
    unsigned maxPacketSize;
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
    RoxieThrottledPacketSender(TokenBucket &_bucket, unsigned _maxPacketSize)
        : Thread("RoxieThrottledPacketSender"), bucket(_bucket), maxPacketSize(_maxPacketSize)
    {
        start();
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
                packetsSent++;
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
        RoxiePacketHeader &header = x->queryHeader();

        unsigned length = x->queryHeader().packetlength;
        assertex (header.activityId & ~ROXIE_PRIORITY_MASK);
        switch (header.retries & ROXIE_RETRIES_MASK)
        {
        case (QUERY_ABORTED & ROXIE_RETRIES_MASK):
            {
                StringBuffer s;
                logctx.CTXLOG("Aborting packet size=%d: %s", length, header.toString(s).str());
            }
            break;
        default:
            {
                StringBuffer s;
                logctx.CTXLOG("Resending packet size=%d: %s", length, header.toString(s).str());
            }
            break; 
        case 0:
            if (logctx.queryTraceLevel() > 8)
            {
                StringBuffer s;
                logctx.CTXLOG("Sending packet size=%d: %s", length, header.toString(s).str());
            }
            break;
        }
        if (length > maxPacketSize)
        {
            StringBuffer s;
            throw MakeStringException(ROXIE_PACKET_ERROR, "Maximum packet length %d exceeded sending packet %s", maxPacketSize, header.toString(s).str());
        }
        enqueue(x);
    }

    void stop()
    {
//      bucket.stop();
        queued.interrupt(new StoppedException);
    }
};

//------------------------------------------------------------------------------------------------------------
#ifdef NEW_IBYTI

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
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
        DelayedPacketEntry *finger = head;
        while (finger)
        {
            if (finger->matches(ibyti))
            {
                if (traceRoxiePackets)
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
        // Goes on the end. But percolate the expiry time backwards
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
        DelayedPacketEntry *newEntry = new DelayedPacketEntry(packet, expires);
        if (traceRoxiePackets)
        {
            StringBuffer s;
            DBGLOG("Adding delayed packet %s", packet->queryHeader().toString(s).str());
        }
        newEntry->prev = tail;
        if (tail)
        {
            tail->next = newEntry;
            for (DelayedPacketEntry *finger = tail; finger != nullptr; finger = finger->prev)
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
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue)
    {
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
        DelayedPacketEntry *finger = head;
        while (finger)
        {
            if (((int) (finger->waitExpires - now)) <= 0)   // Oddly coded to handle wrapping
            {
                ISerializedRoxieQueryPacket *packet = finger->getClear();
                const RoxiePacketHeader &header = packet->queryHeader();
                if (traceRoxiePackets)
                {
                    StringBuffer s;
                    DBGLOG("No IBYTI received yet for delayed packet %s", header.toString(s).str());
                }
                if (header.activityId & ROXIE_SLA_PRIORITY)
                    slaQueue.enqueue(packet);
                else if (header.activityId & ROXIE_HIGH_PRIORITY)
                    hiQueue.enqueue(packet);
                else
                    loQueue.enqueue(packet);
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
    unsigned timeout(unsigned now) const
    {
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
        if (head)
        {
            int delay = (int) (head->waitExpires - now);
            if (delay <= 0)
                return 0;
            else
                return (unsigned) delay;
        }
        else
            return (unsigned) -1;
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
    unsigned timeout(unsigned now) const
    {
        unsigned min = (unsigned) -1;
        for (unsigned queue = 0; queue <= maxSeen; queue++)
        {
            unsigned t = queues[queue].timeout(now);
            if (t < min)
                min = t;
        }
        return min;
    }
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue)
    {
        for (unsigned queue = 0; queue <= maxSeen; queue++)
        {
            queues[queue].checkExpired(now, slaQueue, hiQueue, loQueue);
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
        assert(GetCurrentThreadId()==roxiePacketReaderThread);
        ForEachItemIn(idx, channels)
        {
            DelayedPacketQueueChannel &i = channels.item(idx);
            if (i.queryChannel() == channel)
                return i.queryQueue(subchannel);
        }
        channels.append(*new DelayedPacketQueueChannel(channel));
        return channels.tos().queryQueue(subchannel);
    }
    unsigned timeout(unsigned now) const
    {
        unsigned ret = (unsigned) -1;
        ForEachItemIn(idx, channels)
        {
            unsigned t = channels.item(idx).timeout(now);
            if (t < ret)
                ret = t;
        }
        return ret;
    }
    void checkExpired(unsigned now, RoxieQueue &slaQueue, RoxieQueue &hiQueue, RoxieQueue &loQueue)
    {
        ForEachItemIn(idx, channels)
        {
            channels.item(idx).checkExpired(now, slaQueue, hiQueue, loQueue);
        }
    }
private:
    CIArrayOf<DelayedPacketQueueChannel> channels;
};
#endif

//------------------------------------------------------------------------------------------------------------

class RoxieSocketQueueManager : public RoxieReceiverBase
{
protected:
    Linked<ISendManager> sendManager;
    Linked<IReceiveManager> receiveManager;
    Owned<RoxieThrottledPacketSender> throttledPacketSendManager;
    Owned<TokenBucket> bucket;
    unsigned maxPacketSize = 0;
    std::atomic<bool> running = { false };
#ifdef NEW_IBYTI
    DelayedPacketQueueManager delayed;
#endif

    class ReceiverThread : public Thread
    {
        RoxieSocketQueueManager &parent;
    public:
        ReceiverThread(RoxieSocketQueueManager &_parent) : Thread("RoxieSocketQueueManager"), parent(_parent) {}
        int run()
        {
            // Raise the priority so ibyti's get through in a timely fashion
#if defined( __linux__) || defined(__APPLE__)
            setLinuxThreadPriority(3);
#else
            adjustPriority(1);
#endif
            roxiePacketReaderThread = GetCurrentThreadId();
            return parent.run();
        }
    } readThread;

public:
    RoxieSocketQueueManager(unsigned _numWorkers) : RoxieReceiverBase(_numWorkers), readThread(*this)
    {
        maxPacketSize = multicastSocket->get_max_send_size();
        if ((maxPacketSize==0)||(maxPacketSize>65535))
            maxPacketSize = 65535;
    }

    virtual void sendPacket(IRoxieQueryPacket *x, const IRoxieContextLogger &logctx)
    {
        if (throttledPacketSendManager)
            throttledPacketSendManager->sendPacket(x, logctx);
        else
        {
            MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendPacket");
            RoxiePacketHeader &header = x->queryHeader();

            unsigned length = x->queryHeader().packetlength;
            assertex (header.activityId & ~ROXIE_PRIORITY_MASK);
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
                if (logctx.queryTraceLevel() > 8)
                    logctx.CTXLOG("Sending packet size=%d: %s", length, header.toString(s).str());
                break;
            }
            if (length > maxPacketSize)
            {
                StringBuffer s;
                throw MakeStringException(ROXIE_PACKET_ERROR, "Maximum packet length %d exceeded sending packet %s", maxPacketSize, header.toString(s).str());
            }
            Owned <ISerializedRoxieQueryPacket> serialized = x->serialize();
            if (!channelWrite(serialized->queryHeader(), true))
                logctx.CTXLOG("Roxie packet write wrote too little");
            packetsSent++;
        }
    }

    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx, unsigned subChannel) override
    {
#ifdef SUBCHANNELS_IN_HEADER
        if (!header.hasBuddies())
            return;
#endif
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendIbyti");
        RoxiePacketHeader ibytiHeader(header, header.activityId & ROXIE_PRIORITY_MASK, subChannel);
    
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Sending IBYTI packet %s", ibytiHeader.toString(s).str());
        }
        channelWrite(ibytiHeader, false);  // don't send to self
        ibytiPacketsSent++;
    }

    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbort");
        RoxiePacketHeader abortHeader(header, header.activityId & ROXIE_PRIORITY_MASK, 0);  // subChannel irrelevant - we are about to overwrite retries anyway
        abortHeader.retries = QUERY_ABORTED;
        if (logctx.queryTraceLevel() > 8)
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
        abortsSent++;
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK, 0); // subChannel irrelevant - we are about to overwrite retries anyway
        abortHeader.retries = QUERY_ABORTED;
        abortHeader.packetlength += strlen(lfn)+1;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        Owned<ISerializedRoxieQueryPacket> serialized = packet->serialize();
        if (!channelWrite(serialized->queryHeader(), true))
            logctx.CTXLOG("sendAbortCallback wrote too little");
        abortsSent++;
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx)
    {
        unsigned qnum = outOfBand ? 0 : ((header.retries & ROXIE_FASTLANE) || !fastLaneQueue) ? 1 : 2;
        if (logctx.queryTraceLevel() > 8)
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
                ret = false;
                break;
            }
        }
        if (!checkRank)
        {
            if (traceLevel > 8)
                DBGLOG("discarding data for aborted query");
            ROQ->abortCompleted(header);
        }
        return ret;
    }

    void doIbyti(RoxiePacketHeader &header, RoxieQueue &queue)
    {
        assert(!localAgent);
        bool preActivity = false;
#ifdef SUBCHANNELS_IN_HEADER
        unsigned mySubChannel = header.mySubChannel();
#else
        Owned<const ITopologyServer> topology = getTopology();
        const ChannelInfo &channelInfo = topology->queryChannelInfo(header.channel);
        unsigned mySubChannel = channelInfo.subChannel();
#endif

        if (header.retries == QUERY_ABORTED)
        {
            bool foundInQ = false;
#ifdef NEW_IBYTI
            foundInQ = mySubChannel != 0 && delayed.queryQueue(header.channel, mySubChannel).doIBYTI(header);
#endif
            if (!foundInQ)
                foundInQ = queue.remove(header);
            if (!foundInQ)
                abortRunning(header, queue, false, preActivity);

            if (traceRoxiePackets || traceLevel > 10)
            {
                StringBuffer s; 
                DBGLOG("Abort activity %s", header.toString(s).str());
            }
        }
        else
        {
            ibytiPacketsReceived++;
            unsigned subChannel = header.getRespondingSubChannel();
            if (subChannel == mySubChannel)
            {
                if (traceRoxiePackets || traceLevel > 10)
                    DBGLOG("doIBYTI packet was from self");
                ibytiPacketsFromSelf++;
            }
            else
            {
#ifndef SUBCHANNELS_IN_HEADER
                channelInfo.noteChannelHealthy(subChannel);
#else
                noteNodeHealthy(header.subChannels[subChannel]);
#endif
                bool foundInQ = false;
#ifdef NEW_IBYTI
                foundInQ = mySubChannel != 0 && delayed.queryQueue(header.channel, mySubChannel).doIBYTI(header);
#endif
                if (!foundInQ)
                    foundInQ = queue.remove(header);
                if (foundInQ)
                {
                    if (traceRoxiePackets || traceLevel > 10)
                    {
                        StringBuffer s; 
                        DBGLOG("Removed activity from Q : %s", header.toString(s).str());
                    }
                    ibytiPacketsWorked++;
                    return;
                }
                if (abortRunning(header, queue, true, preActivity))
                {
                    if (traceRoxiePackets || traceLevel > 10)
                    {
                        StringBuffer s;
                        DBGLOG("Aborted running activity : %s", header.toString(s).str());
                    }
                    if (preActivity)
                        ibytiPacketsWorked++;
                    else 
                        ibytiPacketsHalfWorked++;
                    return;
                }               
                if (traceRoxiePackets || traceLevel > 10)
                {
                    StringBuffer s;
                    DBGLOG("doIBYTI packet was too late (or too early) : %s", header.toString(s).str());
                }
                ibytiPacketsTooLate++; // meaning either I started and reserve the right to finish, or I finished already
                if (IBYTIbufferSize)
                    queue.noteOrphanIBYTI(header);
            }
        }
    }

    void processMessage(MemoryBuffer &mb, RoxiePacketHeader &header, RoxieQueue &queue)
    {
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
            // In SUBCHANNELS_IN_HEADER mode this translation has been done on server before sending, except for some control messages like PING or UNLOAD

                Owned<const ITopologyServer> topology = getTopology();
                const std::vector<unsigned> channels = topology->queryChannels();
                Owned<ISerializedRoxieQueryPacket> packet = createSerializedRoxiePacket(mb);
                for (unsigned i = 1; i < channels.size(); i++)
                    queue.enqueue(packet->cloneSerializedPacket(channels[i]));
                header.channel = channels[0];
                queue.enqueue(packet.getClear());
                return;
            }
#ifdef SUBCHANNELS_IN_HEADER
            unsigned mySubchannel = header.mySubChannel();
#else
            Owned<const ITopologyServer> topology = getTopology();
            unsigned mySubchannel = topology->queryChannelInfo(header.channel).subChannel();
#endif
            if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK )
            {
                Owned<IRoxieQueryPacket> packet = deserializeCallbackPacket(mb);
                if (traceLevel > 10)
                {
                    StringBuffer s;
                    DBGLOG("ROXIE_CALLBACK %s", header.toString(s).str());
                }
                doFileCallback(packet);
            }
            else if (IBYTIbufferSize && queue.lookupOrphanIBYTI(header))
            {
                if (traceRoxiePackets || traceLevel > 10)
                {
                    StringBuffer s;
                    DBGLOG("doIBYTI packet was too early : %s", header.toString(s).str());
                }
                ibytiPacketsTooLate--;
                ibytiPacketsTooEarly++;
            }
            else
            {
                Owned<ISerializedRoxieQueryPacket> packet = createSerializedRoxiePacket(mb);
                AgentContextLogger logctx(packet);
                unsigned retries = header.thisChannelRetries(mySubchannel);
                if (retries)
                {
                    // MORE - is this fast enough? By the time I am seeing retries I may already be under load. Could move onto a separate thread
                    assertex(header.channel); // should never see a retry on channel 0
                    if (retries >= SUBCHANNEL_MASK)
                        return; // someone sent a failure or something - ignore it

                    // Send back an out-of-band immediately, to let Roxie server know that channel is still active
                    if (!(testAgentFailure & 0x800))
                    {
                        RoxiePacketHeader newHeader(header, ROXIE_ALIVE, mySubchannel);
                        Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                        output->flush();
                    }

                    // If it's a retry, look it up against already running, or output stream, or input queue
                    // if found, send an IBYTI and discard retry request

                    if (!mySubchannel)
                        retriesReceivedPrm++;
                    else
                        retriesReceivedSec++;
                    bool alreadyRunning = false;
                    Owned<IPooledThreadIterator> wi = queue.running();
                    ForEach(*wi)
                    {
                        CRoxieWorker &w = (CRoxieWorker &) wi->query();
                        if (w.match(header))
                        {
                            alreadyRunning = true;
                            if (!mySubchannel)
                                retriesIgnoredPrm++;
                            else
                                retriesIgnoredSec++;
                            ROQ->sendIbyti(header, logctx, mySubchannel);
                            if (logctx.queryTraceLevel() > 10)
                            {
                                StringBuffer xx; logctx.CTXLOG("Ignored retry on subchannel %u for running activity %s", mySubchannel, header.toString(xx).str());
                            }
                            break;
                        }
                    }
                    if (!alreadyRunning && checkCompleted && ROQ->replyPending(header))
                    {
                        alreadyRunning = true;
                        if (!mySubchannel)
                            retriesIgnoredPrm++;
                        else 
                            retriesIgnoredSec++;
                        ROQ->sendIbyti(header, logctx, mySubchannel);
                        if (logctx.queryTraceLevel() > 10)
                        {
                            StringBuffer xx; logctx.CTXLOG("Ignored retry on subchannel %u for completed activity %s", mySubchannel, header.toString(xx).str());
                        }
                    }
                    if (!alreadyRunning)
                    {
                        if (logctx.queryTraceLevel() > 10)
                        {
                            StringBuffer xx; logctx.CTXLOG("Retry %d received on subchannel %u for %s", retries+1, mySubchannel, header.toString(xx).str());
                        }
                        queue.enqueueUnique(packet.getClear(), mySubchannel);
                    }
                }
                else // first time (not a retry).
                {
#ifdef NEW_IBYTI
                    if (mySubchannel != 0)  // i.e. I am not the primary here
                    {
                        unsigned delay = 0;
                        for (unsigned subChannel = 0; subChannel < mySubchannel; subChannel++)
                            delay += getIbytiDelay(header.subChannels[subChannel]);
                        delayed.queryQueue(header.channel, mySubchannel).append(packet.getClear(), msTick()+delay);
                    }
                    else
#endif
                        queue.enqueue(packet.getClear());
                }
            }
        }
    }

    int run()
    {
        if (traceLevel) 
            DBGLOG("RoxieSocketQueueManager::run() starting: doIbytiDelay=%s minIbytiDelay=%u initIbytiDelay=%u",
                    doIbytiDelay?"YES":"NO", minIbytiDelay, initIbytiDelay);

        for (;;)
        {
            MemoryBuffer mb;
            try
            {
                // NOTE - this thread needs to do as little as possible - just read packets and queue them up - otherwise we can get packet loss due to buffer overflow
                // DO NOT put tracing on this thread except at very high tracelevels!
#ifdef NEW_IBYTI
                unsigned timeout = delayed.timeout(msTick());
                if (timeout>5000)
                    timeout = 5000;
#else
                unsigned timeout = 5000;
#endif
                unsigned l;
                multicastSocket->readtms(mb.reserve(maxPacketSize), sizeof(RoxiePacketHeader), maxPacketSize, l, timeout);
                mb.setLength(l);
                packetsReceived++;
                RoxiePacketHeader &header = *(RoxiePacketHeader *) mb.toByteArray();
                if (l != header.packetlength)
                    DBGLOG("sock->read returned %d but packetlength was %d", l, header.packetlength);
                if (traceRoxiePackets || traceLevel > 10)
                {
                    StringBuffer s;
                    DBGLOG("Read roxie packet: %s", header.toString(s).str());
                }
                if (header.activityId & ROXIE_SLA_PRIORITY)
                    processMessage(mb, header, slaQueue);
                else if (header.activityId & ROXIE_HIGH_PRIORITY)
                    processMessage(mb, header, hiQueue);
                else
                    processMessage(mb, header, loQueue);
            }
            catch (IException *E)
            {
                if (running)
                {
                    // MORE: Maybe we should utilize IException::errorCode - not just text ??
                    if (E->errorCode()==JSOCKERR_timeout_expired)
                        E->Release();
                    else if (roxiemem::memPoolExhausted()) 
                    {
                        //MORE: I think this should probably be based on the error code instead.

                        EXCLOG(E, "Exception reading or processing roxie packet");
                        E->Release();
                        MilliSleep(1000); // Give a chance for mem free
                    }
                    else 
                    {
                        EXCLOG(E, "Exception reading or processing roxie packet");
                        E->Release();
                        // MORE: Protect with try logic, in case udp_create throws exception ?
                        //       What to do if create fails (ie exception is caught) ?
                        if (multicastSocket)
                        {
                            multicastSocket->close();
                            multicastSocket.clear();
                            openMulticastSocket();
                        }
                    }
                
                }
                else
                {
                    E->Release();
                    break;
                }
            }
#ifdef NEW_IBYTI
            delayed.checkExpired(msTick(), slaQueue, hiQueue, loQueue);
#endif
        }
        return 0;
    }

    void start() 
    {
        RoxieReceiverBase::start();
        running = true;
        readThread.start(); 
    }

    void stop() 
    {
        if (running)
        {
            running = false;
            multicastSocket->close();
        }
        RoxieReceiverBase::stop();
    }

    void join()  
    { 
        readThread.join();
        RoxieReceiverBase::join();
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
        unsigned udpMaxSlotsPerClient = topology->getPropInt("@udpMaxSlotsPerClient", 0x7fffffff);
        if (topology->getPropInt("@sendMaxRate", 0))
        {
            unsigned sendMaxRate = topology->getPropInt("@sendMaxRate");
            unsigned sendMaxRatePeriod = topology->getPropInt("@sendMaxRatePeriod", 1);
            bucket.setown(new TokenBucket(sendMaxRate, sendMaxRatePeriod, sendMaxRate));
            throttledPacketSendManager.setown(new RoxieThrottledPacketSender(*bucket, maxPacketSize));
        }

        if (udpMaxSlotsPerClient > udpQueueSize)
            udpMaxSlotsPerClient = udpQueueSize;
        if (udpResendLostPackets && udpMaxSlotsPerClient > TRACKER_BITS)
            udpMaxSlotsPerClient = TRACKER_BITS;
        unsigned serverFlowPort = topology->getPropInt("@serverFlowPort", CCD_SERVER_FLOW_PORT);
        unsigned dataPort = topology->getPropInt("@dataPort", CCD_DATA_PORT);
        unsigned clientFlowPort = topology->getPropInt("@clientFlowPort", CCD_CLIENT_FLOW_PORT);
        receiveManager.setown(createReceiveManager(serverFlowPort, dataPort, clientFlowPort, udpQueueSize, udpMaxSlotsPerClient, encryptionInTransit));
        sendManager.setown(createSendManager(serverFlowPort, dataPort, clientFlowPort, udpSendQueueSize, fastLaneQueue ? 3 : 2, bucket, encryptionInTransit));
    }

    virtual void abortPendingData(const SocketEndpoint &ep) override
    {
        sendManager->abortAll(ep);
    }

};

class RoxieAeronSocketQueueManager : public RoxieSocketQueueManager
{
public:
    RoxieAeronSocketQueueManager(unsigned _numWorkers, bool encryptionInTransit) : RoxieSocketQueueManager(_numWorkers)
    {
        unsigned dataPort = topology->getPropInt("@dataPort", CCD_DATA_PORT);
        SocketEndpoint ep(dataPort, myNode.getIpAddress());
        receiveManager.setown(createAeronReceiveManager(ep, encryptionInTransit));
        assertex(!myNode.getIpAddress().isNull());
        sendManager.setown(createAeronSendManager(dataPort, fastLaneQueue ? 3 : 2, myNode.getIpAddress(), encryptionInTransit));
    }

    virtual void abortPendingData(const SocketEndpoint &ep) override
    {
    }

};


#ifdef _MSC_VER
#pragma warning( pop )
#endif


//==================================================================================================

interface ILocalMessageCollator : extends IMessageCollator
{
    virtual void enqueueMessage(bool outOfBand, void *data, unsigned datalen, void *meta, unsigned metalen, void *header, unsigned headerlen) = 0;
};

interface ILocalReceiveManager : extends IReceiveManager
{
    virtual ILocalMessageCollator *lookupCollator(ruid_t id) = 0;
};



class LocalMessagePacker : public CDummyMessagePacker
{
    MemoryBuffer meta;
    MemoryBuffer header;
    Linked<ILocalReceiveManager> rm;
    ruid_t id;
    bool outOfBand;

public:
    IMPLEMENT_IINTERFACE;
    LocalMessagePacker(RoxiePacketHeader &_header, bool _outOfBand, ILocalReceiveManager *_rm) : rm(_rm), outOfBand(_outOfBand)
    {
        id = _header.uid;
        header.append(sizeof(RoxiePacketHeader), &_header);
    }

    virtual void flush() override;

    virtual void sendMetaInfo(const void *buf, unsigned len) override
    {
        meta.append(len, buf);
    }

};

class CLocalMessageUnpackCursor : implements IMessageUnpackCursor, public CInterface
{
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

class CLocalMessageResult : implements IMessageResult, public CInterface
{
    void *data;
    void *meta;
    void *header;
    unsigned datalen, metalen, headerlen;
    unsigned pos;
public:
    IMPLEMENT_IINTERFACE;
    CLocalMessageResult(void *_data, unsigned _datalen, void *_meta, unsigned _metalen, void *_header, unsigned _headerlen)
    {
        datalen = _datalen;
        metalen = _metalen;
        headerlen = _headerlen;
        data = _data;
        meta = _meta;
        header = _header;
        pos = 0;
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

    virtual const void *getMessageHeader(unsigned &length) const
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

    virtual void enqueueMessage(bool outOfBand, void *data, unsigned datalen, void *meta, unsigned metalen, void *header, unsigned headerlen)
    {
        CriticalBlock c(crit);
        if (outOfBand)
            pending.enqueueHead(new CLocalMessageResult(data, datalen, meta, metalen, header, headerlen));
        else
            pending.enqueue(new CLocalMessageResult(data, datalen, meta, metalen, header, headerlen));
        sem.signal();
        totalBytesReceived += datalen + metalen + headerlen;
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
    Owned<StringContextLogger> logctx;

public:
    IMPLEMENT_IINTERFACE;
    RoxieLocalReceiveManager() : logctx(new StringContextLogger("RoxieLocalReceiveManager"))
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
        collator->enqueueMessage(outOfBand, data.detach(), datalen, meta.detach(), metalen, header.detach(), headerlen);
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
            if (traceLevel > 5)
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
            if (header.activityId & ROXIE_SLA_PRIORITY)
                targetQueue = &slaQueue;
            else if (header.activityId & ROXIE_HIGH_PRIORITY)
                targetQueue = &hiQueue;
            else
                targetQueue = &loQueue;

            Owned<ISerializedRoxieQueryPacket> serialized = packet->serialize();
            if (header.channel)
            {
                targetQueue->enqueue(serialized.getClear());
            }
            else
            {
                // Turn broadcast packet (channel 0), as early as possible, into non-0 channel packets.
                // So retries and other communication with Roxie server (which uses non-0 channel numbers) will not cause double work or confusion.
                for (unsigned i = 1; i < numChannels; i++)
                    targetQueue->enqueue(serialized->cloneSerializedPacket(i+1));
                header.channel = 1;
                targetQueue->enqueue(serialized.getClear());
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
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT packet %s", abortHeader.toString(s).str());
        }
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader);
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
        abortsSent++;
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) override
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieLocalQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK, 0);
        abortHeader.retries = QUERY_ABORTED;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
        abortsSent++;
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx) override
    {
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
    else if (useAeron)
        return new RoxieAeronSocketQueueManager(numWorkers, encrypted);
    else
        return new RoxieUdpSocketQueueManager(numWorkers, encrypted);

}

//================================================================================================================================

class PacketDiscarder : public Thread, implements IPacketDiscarder
{
    bool aborted;
    Owned<IRowManager> rowManager; // not completely sure I need one... maybe I do
    Owned<IMessageCollator> mc;

public:
    IMPLEMENT_IINTERFACE;

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
                    if (traceLevel > 4)
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
                                OwnedConstRoxieRow len = callbackData->getNext(sizeof(RecordLengthType));
                                if (len)
                                {
                                    RecordLengthType *rowlen = (RecordLengthType *) len.get();
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
                else if (!anyActivity)
                {
                    // to avoid leaking partial unwanted packets, we clear out mc periodically...
                    ROQ->queryReceiveManager()->detachCollator(mc);
                    mc.setown(ROQ->queryReceiveManager()->createMessageCollator(rowManager, RUID_DISCARD));
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

    virtual void start()
    {
        Thread::start();
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


//================================================================================================================================

// There are various possibly interesting ways to reply to a ping:
// Reply as soon as receive, or put it on the queue like other messages?
// Reply for every channel, or just once for every agent?
// Should I send on channel 0 or round-robin the channels?
// My gut feeling is that knowing what channels are responding is useful so should reply on every unsuspended channel, 
// and that the delay caused by queuing system is an interesting part of what we want to measure (though nice to know minimum possible too)

unsigned pingInterval = 60;

class PingTimer : public Thread
{
    bool aborted;
    Owned<IRowManager> rowManager;
    Owned<IMessageCollator> mc;
    StringContextLogger logctx;

    void sendPing(unsigned priorityMask)
    {
        try
        {
            RemoteActivityId pingId(ROXIE_PING | priorityMask, 0);
            RoxiePacketHeader header(pingId, 0, 0, 0);

            MemoryBuffer mb;
            mb.append(sizeof(RoxiePacketHeader), &header);
            mb.append((char) LOGGING_FLAGSPRESENT);
            mb.append("PING");

            PingRecord data;
            data.senderIP.ipset(myNode.getIpAddress());
            data.tick = usTick();
            mb.append(sizeof(PingRecord), &data);
            if (traceLevel > 1)
                DBGLOG("PING sent");
            Owned<IRoxieQueryPacket> packet = createRoxiePacket(mb);
            ROQ->sendPacket(packet, logctx);
        }
        catch (IException *E)
        {
            EXCLOG(E);
            E->Release();
        }
    }

public:
    PingTimer() : logctx("PingTimer")
    {
        aborted = false;
    };

    ~PingTimer()
    {
        if (mc)
            ROQ->queryReceiveManager()->detachCollator(mc);
        mc.clear();
    }

    virtual int run()
    {
        rowManager.setown(roxiemem::createRowManager(1, NULL, queryDummyContextLogger(), NULL, false));
        mc.setown(ROQ->queryReceiveManager()->createMessageCollator(rowManager, RUID_PING));
        unsigned pingsReceived = 0;
        unsigned pingsElapsed = 0;
        sendPing(ROXIE_HIGH_PRIORITY);
        while (!aborted)
        {
            bool anyActivity = false;
            Owned<IMessageResult> mr = mc->getNextResult(pingInterval*1000, anyActivity);
            if (mr)
            {
                unsigned headerLen;
                const RoxiePacketHeader *header = (const RoxiePacketHeader *) mr->getMessageHeader(headerLen);
                Owned<IMessageUnpackCursor> mu = mr->getCursor(rowManager);
                PingRecord *answer = (PingRecord *) mu->getNext(sizeof(PingRecord));
                if (answer && mu->atEOF() && headerLen==sizeof(RoxiePacketHeader))
                {
                    unsigned elapsed = usTick() - answer->tick;
                    pingsReceived++;
                    pingsElapsed += elapsed;
                    if (traceLevel > 10)
                        DBGLOG("PING reply channel=%d, time %d", header->channel, elapsed); // DBGLOG is slower than the pings so be careful!
                }
                else
                    DBGLOG("PING reply, garbled result");
                ReleaseRoxieRow(answer);
            }
            else if (!anyActivity)
            {
                if (!pingsReceived && roxieMulticastEnabled)
                    DBGLOG("PING: NO replies received! Please check multicast settings, and that your network supports multicast.");
                else if (traceLevel)
                    DBGLOG("PING: %d replies received, average delay %uus", pingsReceived, pingsReceived ? pingsElapsed / pingsReceived : 0);
                pingsReceived = 0;
                pingsElapsed = 0;
                sendPing(ROXIE_HIGH_PRIORITY);  // MORE - we could think about alternating the priority or sending pings on high and low at the same time...
            }
        }
        return 0;
    }

    void stop()
    {
        if (mc)
            mc->interrupt();
        aborted = true;
    }

    static CriticalSection crit;
} *pingTimer;

CriticalSection PingTimer::crit;

extern void startPingTimer()
{
    CriticalBlock b(PingTimer::crit);
    if (!pingTimer)
    {
        pingTimer = new PingTimer();
        pingTimer->start();
    }
}

extern void stopPingTimer()
{
    CriticalBlock b(PingTimer::crit);
    if (pingTimer)
    {
        pingTimer->stop();
        pingTimer->join();
        delete pingTimer;
        pingTimer = NULL;
    }
}

