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
#include "udplib.hpp"
#include "ccd.hpp"
#include "ccddebug.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>
#endif

CriticalSection ibytiCrit; // CAUTION - not safe to use spinlocks as real-time thread accesses
CriticalSection queueCrit;
unsigned channels[MAX_CLUSTER_SIZE];
unsigned channelCount;
unsigned subChannels[MAX_CLUSTER_SIZE];
unsigned numSlaves[MAX_CLUSTER_SIZE];
unsigned replicationLevel[MAX_CLUSTER_SIZE];
unsigned IBYTIDelays[MAX_CLUSTER_SIZE]; // MORE: this will cover only 2 slaves per channel, change to cover all. 

SpinLock suspendCrit;
bool suspendedChannels[MAX_CLUSTER_SIZE];

using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedConstRoxieRow;
using roxiemem::IRowManager;
using roxiemem::DataBuffer;

//============================================================================================

// This function maps a slave number to the multicast ip used to talk to it.

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

Owned<ISocket> multicastSocket;
SocketEndpointArray *slaveEndpoints; // indexed by channel

bool isSlaveEndpoint(unsigned channel, const IpAddress &slaveIp)
{
    SocketEndpointArray &eps = slaveEndpoints[channel];
    ForEachItemIn(idx, eps)
    {
        if (eps.item(idx).ipequals(slaveIp))
            return true;
    }
    return false;
}

void joinMulticastChannel(unsigned channel)
{
    if (roxieMulticastEnabled && !localSlave)
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
        Owned<IPropertyTreeIterator> it = ccdChannels->getElements("RoxieSlaveProcess");
        ForEach(*it)
        {
            unsigned channel = it->query().getPropInt("@channel", 0);
            assertex(channel);
            joinMulticastChannel(channel);
        }
        joinMulticastChannel(0); // all slaves also listen on channel 0
    }
}

void addEndpoint(unsigned channel, const IpAddress &slaveIp, unsigned port)
{
    if (!slaveEndpoints)
        slaveEndpoints = new SocketEndpointArray[numChannels + 1];
    IpAddress multicastIp;
    if (roxieMulticastEnabled)
        getChannelIp(multicastIp, channel);
    else
        multicastIp = slaveIp;
    if (!isSlaveEndpoint(channel, multicastIp))
    {
        SocketEndpoint &ep = *new SocketEndpoint(ccdMulticastPort, multicastIp);
        slaveEndpoints[channel].append(ep);
    }
    if (channel)
        addEndpoint(0, slaveIp, port);
}

void closeMulticastSockets()
{
    delete[] slaveEndpoints;
    slaveEndpoints = NULL;
    multicastSocket.clear();
}

size32_t channelWrite(unsigned channel, void const* buf, size32_t size)
{
    size32_t minwrote = 0;
    SocketEndpointArray &eps = slaveEndpoints[channel]; // if multicast is enabled, this will have a single multicast endpoint in it.
    assertex(eps.ordinality());
    ForEachItemIn(idx, eps)
    {
        size32_t wrote = multicastSocket->udp_write_to(eps.item(idx), buf, size);
        if (!idx || wrote < minwrote)
            minwrote = wrote;
    }
    return minwrote;
}

// #define TEST_SLAVE_FAILURE

//============================================================================================

StringBuffer &RoxiePacketHeader::toString(StringBuffer &ret) const
{
    const IpAddress &serverIP = getNodeAddress(serverIdx);
    ret.appendf("uid=" RUIDF " activityId=", uid);
    switch(activityId & ~ROXIE_PRIORITY_MASK)
    {
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
    ret.append(" pri=");
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
    return ret;
}

class CRoxieQueryPacket : public CInterface, implements IRoxieQueryPacket
{
protected:
    RoxiePacketHeader *data;
    const byte *continuationData; 
    unsigned continuationLength;
    const byte *smartStepInfoData; 
    unsigned smartStepInfoLength;
    const byte *contextData;
    unsigned contextLength;
    const byte *traceInfo;
    unsigned traceLength;
    
public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryPacket(const void *_data, int lengthRemaining) : data((RoxiePacketHeader *) _data)
    {
        assertex(lengthRemaining >= sizeof(RoxiePacketHeader));
        data->packetlength = lengthRemaining;
        const byte *finger = (const byte *) (data + 1);
        lengthRemaining -= sizeof(RoxiePacketHeader);
        if (data->activityId == ROXIE_FILECALLBACK || data->activityId == ROXIE_DEBUGCALLBACK || data->retries == QUERY_ABORTED)
        {
            continuationData = NULL;
            continuationLength = 0;
            smartStepInfoData = NULL;
            smartStepInfoLength = 0;
            traceInfo = NULL;
            traceLength = 0;
        }
        else
        {
            if (data->continueSequence & ~CONTINUE_SEQUENCE_SKIPTO)
            {
                assertex(lengthRemaining >= sizeof(unsigned short));
                continuationLength = *(unsigned short *) finger;
                continuationData = finger + sizeof(unsigned short);
                finger = continuationData + continuationLength;
                lengthRemaining -= continuationLength + sizeof(unsigned short);
            }
            else
            {
                continuationData = NULL;
                continuationLength = 0;
            }
            if (data->continueSequence & CONTINUE_SEQUENCE_SKIPTO)
            {
                assertex(lengthRemaining >= sizeof(unsigned short));
                smartStepInfoLength = *(unsigned short *) finger;
                smartStepInfoData = finger + sizeof(unsigned short);
                finger = smartStepInfoData + smartStepInfoLength;
                lengthRemaining -= smartStepInfoLength + sizeof(unsigned short);
            }
            else
            {
                smartStepInfoData = NULL;
                smartStepInfoLength = 0;
            }
            assertex(lengthRemaining > 1);
            traceInfo = finger;
            lengthRemaining--;
            if (*finger++ & LOGGING_DEBUGGERACTIVE)
            {
                assertex(lengthRemaining >= sizeof(unsigned short)); 
                unsigned short debugLen = *(unsigned short *) finger;
                finger += debugLen + sizeof(unsigned short);
                lengthRemaining -= debugLen + sizeof(unsigned short);
            }
            loop
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
        assertex(lengthRemaining >= 0);
        contextData = finger;
        contextLength = lengthRemaining;
    }

    ~CRoxieQueryPacket()
    {
        free(data);
    }

    virtual RoxiePacketHeader &queryHeader() const
    {
        return  *data;
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

    virtual const byte *queryTraceInfo() const
    {
        return traceInfo;
    }

    virtual unsigned getTraceLength() const
    {
        return traceLength;
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

        unsigned newDataSize = data->packetlength + sizeof(unsigned short) + skipDataLen;
        char *newdata = (char *) malloc(newDataSize);
        unsigned headSize = sizeof(RoxiePacketHeader);
        if (data->continueSequence & ~CONTINUE_SEQUENCE_SKIPTO)
            headSize += sizeof(unsigned short) + continuationLength;
        memcpy(newdata, data, headSize); // copy in leading part of old data
        ((RoxiePacketHeader *) newdata)->continueSequence |= CONTINUE_SEQUENCE_SKIPTO; // set flag indicating new data is present
        *(unsigned short *) (newdata + headSize) = skipDataLen; // add length field for new data
        memcpy(newdata + headSize + sizeof(unsigned short), skipData, skipDataLen); // copy in new data
        memcpy(newdata + headSize + sizeof(unsigned short) + skipDataLen, ((char *) data) + headSize, data->packetlength - headSize); // copy in remaining old data
        return createRoxiePacket(newdata, newDataSize);
    }

    virtual unsigned hash() const
    {
        // This is used for Roxie server-side caching. The hash includes some of the header and all of the payload.
        unsigned hash = 0;
        if (continuationLength)
            hash = hashc((const unsigned char *) continuationData, continuationLength, hash);
        if (smartStepInfoLength)
            hash = hashc((const unsigned char *) smartStepInfoData, smartStepInfoLength, hash);
        // NOTE - don't hash the trace info!
        hash = hashc((const unsigned char *) contextData, contextLength, hash);
        hash = hashc((const unsigned char *) &data->channel, sizeof(data->channel), hash);
        hash = hashc((const unsigned char *) &data->overflowSequence, sizeof(data->overflowSequence), hash); 
        hash = hashc((const unsigned char *) &data->continueSequence, sizeof(data->continueSequence), hash); 
        // MORE - sequence fields should always be zero for anything we are caching I think... (?)
        // Note - no point hashing activityId (as cache is local to one activity) or serverIP (likewise)
        return hash;
    }

    virtual bool cacheMatch(const IRoxieQueryPacket *c) const 
    {
        // note - this checks whether it's a repeat from Roxie server's point-of-view
        // So fields that are compared are the same as the ones that are hashed....
        RoxiePacketHeader &h = c->queryHeader();
        if (data->channel == h.channel && data->overflowSequence == h.overflowSequence && data->continueSequence == h.continueSequence)
        {
            if (continuationLength) // note - we already checked that sequences match
            {
                if (continuationLength != c->getContinuationLength())
                    return false;
                if (memcmp(continuationData,c->queryContinuationData(),continuationLength)!=0)
                    return false;
            }
            if (smartStepInfoLength)
            {
                if (smartStepInfoLength != c->getSmartStepInfoLength())
                    return false;
                if (memcmp(smartStepInfoData,c->querySmartStepInfoData(),smartStepInfoLength)!=0)
                    return false;
            }
            // NOTE - trace info NOT compared
            if (contextLength == c->getContextLength() && memcmp(contextData, c->queryContextData(), contextLength)==0)
                return true;
        }
        return false;
    }
};

extern IRoxieQueryPacket *createRoxiePacket(void *_data, unsigned _len)
{
    if ((unsigned short)_len != _len && !localSlave)
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

//=================================================================================

SlaveContextLogger::SlaveContextLogger()
{
    GetHostIp(ip);
    set(NULL);
}

SlaveContextLogger::SlaveContextLogger(IRoxieQueryPacket *packet)
{
    GetHostIp(ip);
    set(packet);
}

void SlaveContextLogger::set(IRoxieQueryPacket *packet)
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
        CriticalBlock b(crit);
        RoxiePacketHeader &header = packet->queryHeader();
        const byte *traceInfo = packet->queryTraceInfo();
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
            // Passing the wuid via the logging context prefix is a bit of a hack...
            if (loggingFlags & LOGGING_WUID)
            {
                unsigned wuidLen = 0;
                while (wuidLen < traceLength)
                {
                    if (traceInfo[wuidLen]=='@')
                        break;
                    wuidLen++;
                }
                wuid.set((const char *) traceInfo, wuidLen);
            }
        }
        channel = header.channel;
        StringBuffer s(traceLength, (const char *) traceInfo);
        s.append("|");
        ip.getIpText(s);
        s.append(':').append(channel);
        StringContextLogger::set(s.str());
        if (intercept || mergeSlaveStatistics)
        {
            RoxiePacketHeader newHeader(header, ROXIE_TRACEINFO);
            output.setown(ROQ->createOutputStream(newHeader, true, *this));
        }
    }
    else
    {
        StringContextLogger::set("");
        channel = 0;
    }
}


void SlaveContextLogger::putStatProcessed(unsigned subGraphId, unsigned actId, unsigned idx, unsigned processed) const
{
    if (output && mergeSlaveStatistics)
    {
        MemoryBuffer buf;
        buf.append((char) LOG_CHILDCOUNT); // A special log entry for the stats
        buf.append(subGraphId);
        buf.append(actId);
        buf.append(idx);
        buf.append(processed);
    }
}

void SlaveContextLogger::putStats(unsigned subGraphId, unsigned actId, const CRuntimeStatisticCollection &stats) const
{
    if (output && mergeSlaveStatistics)
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

void SlaveContextLogger::flush()
{
    if (output)
    {
        CriticalBlock b(crit);
        if (mergeSlaveStatistics)
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
            output->flush(true);
         output.clear();
    }
}

//=================================================================================

unsigned getIbytiDelay(unsigned channel, const RoxiePacketHeader &header)
{
    // MORE - adjust delay according to whether it's a retry, whether it was a broadcast etc
    CriticalBlock b(ibytiCrit);
    return IBYTIDelays[channel];
}

void resetIbytiDelay(unsigned channel)
{
    unsigned prevVal;
    {
        CriticalBlock b(ibytiCrit);
        prevVal = IBYTIDelays[channel];
        IBYTIDelays[channel] = initIbytiDelay;
    }
    if (traceLevel > 8 && prevVal != initIbytiDelay)
        DBGLOG("Reset IBYTI delay value for channel %u from %u to %u", channel, prevVal, initIbytiDelay);
}

void decIbytiDelay(unsigned channel, unsigned factor = 2)
{
    unsigned prevVal, newVal;
    {
        CriticalBlock b(ibytiCrit);
        prevVal = IBYTIDelays[channel];
        IBYTIDelays[channel] /= factor;
        if (IBYTIDelays[channel] < minIbytiDelay)
            IBYTIDelays[channel] = minIbytiDelay;
        newVal = IBYTIDelays[channel];
    }
    if (traceLevel > 8 && prevVal != newVal)
        DBGLOG("Dec IBYTI delay value for channel %u from %u to %u (factor=%u)", channel, prevVal, newVal, factor);
}

//=================================================================================

static SpinLock onDemandQueriesCrit;
static MapXToMyClass<hash64_t, hash64_t, IQueryFactory> onDemandQueryCache;

void sendUnloadMessage(hash64_t hash, const char *id, const IRoxieContextLogger &logctx)
{
    unsigned packetSize = sizeof(RoxiePacketHeader) + sizeof(char) + strlen(id) + 1;
    void *packetData = malloc(packetSize);
    RoxiePacketHeader *header = (RoxiePacketHeader *) packetData;
    RemoteActivityId unloadId(ROXIE_UNLOAD, hash);
    header->init(unloadId, 0, 0, 0);

    char *finger = (char *) (header + 1);
    *finger++ = (char) LOGGING_FLAGSPRESENT;
    strcpy(finger, id);
    finger += strlen(id)+1;
    if (traceLevel > 1)
        DBGLOG("UNLOAD sent for query %s", id);
    Owned<IRoxieQueryPacket> packet = createRoxiePacket(packetData, packetSize);
    ROQ->sendPacket(packet, logctx);
}

void doUnload(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx)
{
    const RoxiePacketHeader &header = packet->queryHeader();
    unsigned channelNo = header.channel;
    logctx.CTXLOG("Unload received for channel %d", channelNo);
    hash64_t hashValue = header.queryHash;
    SpinBlock b(onDemandQueriesCrit);
    onDemandQueryCache.remove(hashValue+channelNo);
}

void cacheOnDemandQuery(hash64_t hashValue, unsigned channelNo, IQueryFactory *query)
{
    SpinBlock b(onDemandQueriesCrit);
    onDemandQueryCache.setValue(hashValue+channelNo, query);
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
    const IpAddress &serverIP = getNodeAddress(header.serverIdx);
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
    RoxiePacketHeader newHeader(header, ROXIE_PING);
    Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
    void *ret = output->getBuffer(contextLength, false);
    memcpy(ret, data, contextLength);
    output->putBuffer(ret, contextLength, false);
    output->flush(true);
}

//=================================================================================
//
// RoxieQueue - holds pending transactions on a roxie agent

class RoxieQueue : public CInterface
{
    QueueOf<IRoxieQueryPacket, true> waiting;
    Semaphore available;
    CriticalSection qcrit;
    unsigned headRegionSize;

public:
    IMPLEMENT_IINTERFACE;

    RoxieQueue(unsigned _headRegionSize)
    {
        headRegionSize = _headRegionSize;
    }

    void enqueue(IRoxieQueryPacket *x)
    {
        {
            CriticalBlock qc(qcrit);
#ifdef TIME_PACKETS
            header.tick = msTick();
#endif
            waiting.enqueue(x);

            CriticalBlock b(counterCrit);
            queueLength++;
            if (queueLength>maxQueueLength)
                maxQueueLength = queueLength;
        }
        available.signal();
    }

    void enqueueUnique(IRoxieQueryPacket *x)
    {
        {
            CriticalBlock qc(qcrit);
            RoxiePacketHeader &header = x->queryHeader();
#ifdef TIME_PACKETS
            header.tick = msTick();
#endif
            unsigned len = waiting.ordinality();
            unsigned i;
            for (i = 0; i < len; i++)
            {
                IRoxieQueryPacket *queued = waiting.item(i);
                if (queued && queued->queryHeader().matchPacket(header))
                {
                    bool primChannel = true;
                    if (subChannels[header.channel] != 1) primChannel = false;
                    if (traceLevel > 0)
                    {
                        StringBuffer xx; 
                        SlaveContextLogger l(x);
                        l.CTXLOG("Ignored retry on %s channel for queued activity %s", primChannel?"primary":"secondary", header.toString(xx).str());
                    }
                    if (primChannel) 
                        atomic_inc(&retriesIgnoredPrm);
                    else 
                        atomic_inc(&retriesIgnoredSec);
                    x->Release();
                    return;
                }
            }
            if (traceLevel > 10)
            {
                SlaveContextLogger l(x);
                StringBuffer xx; 
                l.CTXLOG("enqueued %s", header.toString(xx).str());
            }
            waiting.enqueue(x);
            CriticalBlock b(counterCrit);
            queueLength++;
            if (queueLength>maxQueueLength)
                maxQueueLength = queueLength;
        }
        available.signal();
    }

    bool remove(RoxiePacketHeader &x)
    {
        CriticalBlock qc(qcrit);
        unsigned len = waiting.ordinality();
        unsigned i;
        unsigned scanLength = 0;
        for (i = 0; i < len; i++)
        {
            IRoxieQueryPacket *queued = waiting.item(i);
            if (queued)
            {
                scanLength++;
                if (queued->queryHeader().matchPacket(x))
                {
#ifdef _DEBUG
                    RoxiePacketHeader &header = queued->queryHeader();
                    SlaveContextLogger l(queued);
                    StringBuffer xx; 
                    l.CTXLOG("discarded %s", header.toString(xx).str());
#endif
                    // Already done in doIBYTI()...queue.remove() !!!!! atomic_inc(&ibytiPacketsWorked);
                    waiting.set(i, NULL);
                    queued->Release();
                    CriticalBlock b(counterCrit);
                    queueLength--;
                    if (scanLength > maxScanLength)
                        maxScanLength = scanLength;
                    totScanLength += scanLength;
                    totScans++;
                    if (totScans)
                        meanScanLength = totScanLength / totScans;
                    return true;
                }
            }
        }
        return false;
    }

    void wait()
    {
        available.wait();
    }

    void signal(unsigned num)
    {
        available.signal(num);
    }

    IRoxieQueryPacket *dequeue()
    {
        CriticalBlock qc(qcrit);
        unsigned lim = waiting.ordinality();
        if (lim)
        {
            if (headRegionSize)
            {
                if (lim > headRegionSize)
                    lim = headRegionSize;
                return waiting.dequeue(rand() % lim);
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
};

class CRoxieWorker : public CInterface, implements IPooledThread
{
    RoxieQueue *queue;
    CriticalSection actCrit;
    Semaphore ibytiSem;
    bool stopped;
    bool abortJob;
    bool busy;
    Owned<IRoxieSlaveActivity> activity;
    Owned<IRoxieQueryPacket> packet;
    SlaveContextLogger logctx;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieWorker()
    {
        queue = NULL;
        stopped = false;
        busy = false;
        abortJob = false;
    }
    void init(void *_r) 
    {
        queue = (RoxieQueue *) _r;
        stopped = false;
        busy = false;
        abortJob = false;
    }
    bool canReuse()
    {
        return true;
    }
    bool stop()
    {
        stopped = true;
        return true; 
    }
    inline void setActivity(IRoxieSlaveActivity *act)
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
            if (doIbytiDelay) 
                ibytiSem.signal();
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
            if (doIbytiDelay) 
                ibytiSem.signal();
            if (activity) 
            {
                // Try to stop/abort a job after it starts only if IBYTI comes from a higher priority slave 
                // (more primary in the rank). The slaves with higher rank will hold the lower bits of the retries field in IBYTI packet).
                if (!checkRank || ((h.retries & ROXIE_RETRIES_MASK) < h.getSubChannelMask(h.channel))) 
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

    void throwRemoteException(IException *E, IRoxieSlaveActivity *activity, IRoxieQueryPacket *packet, bool isUser)
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
            // I failed to do the query, but already sent out IBYTI - resend it so someone else can try
            if (!isUser)
            {
                StringBuffer s;
                s.append("Exception in slave for packet ");
                header.toString(s);
                logctx.logOperatorException(E, NULL, 0, "%s", s.str());
                header.setException();
                if (!header.allChannelsFailed() && !localSlave)
                {
                    if (logctx.queryTraceLevel() > 1) 
                        logctx.CTXLOG("resending packet from slave in case others want to try it");
                    ROQ->sendPacket(packet, logctx);
                }
            }
            RoxiePacketHeader newHeader(header, ROXIE_EXCEPTION);
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
            output->flush(true);
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
        if (!queryFactory && logctx.queryWuid())
        {
            Owned <IRoxieDaliHelper> daliHelper = connectToDali();
            Owned<IConstWorkUnit> wu = daliHelper->attachWorkunit(logctx.queryWuid(), NULL);
            queryFactory.setown(createSlaveQueryFactoryFromWu(wu, channel));
            if (queryFactory)
                cacheOnDemandQuery(queryHash, channel, queryFactory);
        }
        if (!queryFactory)
        {
            StringBuffer hdr;
            IException *E = MakeStringException(MSGAUD_operator, ROXIE_UNKNOWN_QUERY, "Roxie slave received request for unregistered query: %s", packet->queryHeader().toString(hdr).str());
            EXCLOG(E, "doActivity");
            throwRemoteException(E, activity, packet, false);
            return;
        }
        try
        {   
            if (logctx.queryTraceLevel() > 8) 
            {
                StringBuffer x;
                logctx.CTXLOG("IBYTI delay controls : doIbytiDelay=%s numslaves=%u subchnl=%u : %s",
                    doIbytiDelay?"YES":"NO", 
                    numSlaves[channel], subChannels[channel],
                    header.toString(x).str());
            }
            bool debugging = logctx.queryDebuggerActive();
            if (debugging)
            {
                if (subChannels[channel] != 1) 
                    abortJob = true;  // when debugging, we always run on primary only...
            }
            else if (doIbytiDelay && (numSlaves[channel] > 1)) 
            {
                bool primChannel = true;
                if (subChannels[channel] != 1) 
                    primChannel = false;
                bool myTurnToDelayIBYTI =  true;  // all slaves will delay, except one
                unsigned hdrHashVal = header.priorityHash();
                if ((((hdrHashVal % numSlaves[channel]) + 1) == subChannels[channel]))
                    myTurnToDelayIBYTI =  false;

                if (myTurnToDelayIBYTI) 
                {
                    unsigned delay = getIbytiDelay(channel, header);
                    if (logctx.queryTraceLevel() > 6)
                    {
                        StringBuffer x;
                        logctx.CTXLOG("YES myTurnToDelayIBYTI channel=%s delay=%u hash=%u %s", primChannel?"primary":"secondary", delay, hdrHashVal, header.toString(x).str());
                    }
                    
                    // MORE: this code puts the penalty on all slaves on this channel,
                    //       change it to have one for each slave on every channel.
                    //       NOT critical for the time being with 2 slaves per channel
                    // MORE: if we are dealing with a query that was on channel 0, we may want a longer delay 
                    // (since the theory about duplicated work not mattering when cluster is idle does not hold up)

                    if (delay)
                    {
                        ibytiSem.wait(delay);
                        if (abortJob)
                            resetIbytiDelay(channel); // we know there is an active buddy on the channel...
                        else
                            decIbytiDelay(channel);
                        if (logctx.queryTraceLevel() > 8)
                        {
                            StringBuffer x;
                            logctx.CTXLOG("Buddy did%s send IBYTI, updated delay=%u : %s", 
                                abortJob ? "" : " NOT", IBYTIDelays[channel], header.toString(x).str());
                        }
                    }
                }
                else {
#ifndef NO_IBYTI_DELAYS_COUNT
                    if (primChannel) atomic_inc(&ibytiNoDelaysPrm);
                    else atomic_inc(&ibytiNoDelaysSec);
#endif
                    if (logctx.queryTraceLevel() > 6)
                    {
                        StringBuffer x;
                        logctx.CTXLOG("NOT myTurnToDelayIBYTI channel=%s hash=%u %s", primChannel?"primary":"secondary", hdrHashVal, header.toString(x).str());
                    }
                }
            }
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
                ROQ->sendIbyti(header, logctx);
            atomic_inc(&activitiesStarted);
            Owned <ISlaveActivityFactory> factory = queryFactory->getSlaveActivityFactory(activityId);
            assertex(factory);
            setActivity(factory->createActivity(logctx, packet));
#ifdef TEST_SLAVE_FAILURE
            bool skip = false;
            if (testSlaveFailure) 
            {
                // Meaning of each byte in testSlaveFailure
                // bits 1 -> 4  : test cast type (4 bits)
                // bits 5 -> 11 : test case Freq  (7 bits)
                // bit 12 :     : 1 Dot NOT Send ROXIE_ALIVE message  - 0: send it
                // bits 13 -> 32 : test case parameter (20 bits), if any
                unsigned testCaseType = (testSlaveFailure & 0x0000000F);
                unsigned testCaseFreq = (testSlaveFailure & 0x000007F0) >> 4;
                unsigned testCaseParm = (testSlaveFailure & 0xFFFFF000) >> 12;

                if (testCaseFreq && (atomic_read(&activitiesStarted) % testCaseFreq == 0)) 
                {
                    StringBuffer x;
                    logctx.CTXLOG("------------ TestSlaveFailure to do the following (testCase=%u freq=%u tot=%u parm=%u ROXIE_ALIVE is %s - val=0x%.8X) for %s", 
                        testCaseType, testCaseFreq, (unsigned) atomic_read(&activitiesStarted), testCaseParm, (testSlaveFailure & 0x800) ? "OFF" : "ON",
                        testSlaveFailure, header.toString(x).str());
                    switch (testCaseType)
                    {
                    case 1: 
                        if (testCaseParm == 0) testCaseParm = 10000;
                        logctx.CTXLOG("--------------- Sleeping for %u ms - testCase=%u", testCaseParm, testCaseType);
                        Sleep(testCaseParm);
                        break;
                    case 2: 
                        logctx.CTXLOG("--------------- Skip processing - testCase=%u ------", testCaseType);
                        skip = true;
                        break;
                    case 3: 
                        logctx.CTXLOG("--------------- Throwing Exception String number %u NOW - testCase=%u  -----", ROXIE_FILE_ERROR, testCaseType);
                        throw MakeStringException(ROXIE_FILE_ERROR, "Simulate File Exception in slave NOW");
                        break;
                    case 4:
                        if (numSlaves[channel] == 1) 
                        {
                            logctx.CTXLOG("--------------- Setting numSlaves[channel=%u] to 2 to force one way to act as two way for ibyti logic testing - testCase=%u ------", channel, testCaseType);
                            numSlaves[channel] = 2;
                        }
                        testSlaveFailure = 0;
                        break;
                    }
                }
            }
            if (!skip)
            {
#endif
                Owned<IMessagePacker> output = activity->process();
                if (logctx.queryTraceLevel() > 5)
                {
                    StringBuffer x;
                    logctx.CTXLOG("done processing %s", header.toString(x).str());
                }
                if (output)
                {
                    atomic_inc(&activitiesCompleted);
                    busy = false; // Keep order - before setActivity below
                    setActivity(NULL);  // Ensures all stats are merged from child queries etc
                    logctx.flush();
                    output->flush(true);
                }
#ifdef TEST_SLAVE_FAILURE
            }
#endif
        }
        catch (IUserException *E)
        {
            throwRemoteException(E, activity, packet, true);
        }
        catch (IException *E)
        {
            if (E->errorCode()!=ROXIE_ABORT_ERROR)
                throwRemoteException(E, activity, packet, false);
        }
        catch (...)
        {
            throwRemoteException(MakeStringException(ROXIE_MULTICAST_ERROR, "Unknown exception"), activity, packet, false);
        }
        busy = false; // Keep order - before setActivity below
        setActivity(NULL);
    }

    void main()
    {
        while (!stopped)
        {
            try
            {
                loop
                {
                    queue->wait();
                    if (stopped)
                        break;
                    {
                        CriticalBlock b(counterCrit);
                        slavesActive++;
                        if (slavesActive > maxSlavesActive)
                            maxSlavesActive = slavesActive;
                    }
                    abortJob = false;
                    busy = true;
                    if (doIbytiDelay) 
                        ibytiSem.reinit(0U); // Make sure sem is is in no-signaled state
                    
                    {
                        CriticalBlock b(queueCrit);
                        packet.setown(queue->dequeue());
                    }
                    if (packet)
                    {
                        {
                            CriticalBlock b(counterCrit);
                            queueLength--;
                        }
                        RoxiePacketHeader &header = packet->queryHeader();
                        logctx.set(packet);
#ifdef TIME_PACKETS
                        {
                            unsigned now = msTick();
                            unsigned packetWait = now-header.tick;
                            header.tick = now;
                            CriticalBlock b(counterCrit);
                            if (packetWait > packetWaitMax)
                                packetWaitMax = packetWait;
                            packetWaitElapsed += packetWait;
                            atomic_inc(&packetWaitCount);
                        }
#endif
                        if (logctx.queryTraceLevel() > 10)
                        {
                            StringBuffer x;
                            logctx.CTXLOG("dequeued %s", header.toString(x).str());
                        }
                        if (ROQ->checkSuspended(header, logctx))
                        {
                            StringBuffer s;
                            logctx.CTXLOG("Ignoring packet for suspended channel %d: %s", header.channel, header.toString(s).str());
                        }
                        else if ((header.activityId & ~ROXIE_PRIORITY_MASK) == ROXIE_UNLOAD)
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
                            CriticalBlock b(counterCrit);
                            if (packetRun > packetRunMax)
                                packetRunMax = packetRun;
                            packetRunElapsed += packetRun;
                            atomic_inc(&packetRunCount);
                        }
#endif
                    }
                    busy = false;
                    {
                        CriticalBlock b(actCrit);
                        packet.clear();
                        logctx.set(NULL);
                    }
                    {
                        CriticalBlock b(counterCrit);
                        slavesActive--;
                    }
                }
            }
            catch(...)
            {
                CriticalBlock b(actCrit);
                Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception in Roxie worker thread");
                EXCLOG(E);
                if (packet)
                    throwRemoteException(E.getClear(), NULL, packet, false);
                packet.clear();
            }
        }
    }
};

//=================================================================================

class CallbackEntry : public CInterface, implements IPendingCallback
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

class RoxieReceiverBase : public CInterface, implements IThreadFactory, implements IRoxieOutputQueueManager
{
protected:
#ifdef ROXIE_SLA_LOGIC
    RoxieQueue slaQueue;
    Owned <IThreadPool> slaWorkers;
#endif
    RoxieQueue hiQueue;
    Owned <IThreadPool> hiWorkers;
    RoxieQueue loQueue;
    Owned <IThreadPool> loWorkers;
    unsigned numWorkers;

    void abortChannel(unsigned channel, IThreadPool *workers)
    {
        Owned<IPooledThreadIterator> wi = workers->running();
        ForEach(*wi)
        {
            CRoxieWorker &w = (CRoxieWorker &) wi->query();
            w.abortChannel(channel);
        }
    }

public:
    IMPLEMENT_IINTERFACE;

#ifdef ROXIE_SLA_LOGIC
    RoxieReceiverBase(unsigned _numWorkers) : numWorkers(_numWorkers), slaQueue(headRegionSize), hiQueue(headRegionSize), loQueue(headRegionSize)
#else
    RoxieReceiverBase(unsigned _numWorkers) : numWorkers(_numWorkers), hiQueue(headRegionSize), loQueue(headRegionSize)
#endif
    {
        loWorkers.setown(createThreadPool("RoxieLoWorkers", this, NULL, numWorkers));
        hiWorkers.setown(createThreadPool("RoxieHiWorkers", this, NULL, numWorkers));
#ifdef ROXIE_SLA_LOGIC
        slaWorkers.setown(createThreadPool("RoxieSLAWorkers", this, NULL, numWorkers));
#endif
        CriticalBlock b(ccdChannelsCrit);
        Owned<IPropertyTreeIterator> it = ccdChannels->getElements("RoxieSlaveProcess");
        ForEach(*it)
        {
            unsigned channel = it->query().getPropInt("@channel", 0);
            unsigned subChannel = it->query().getPropInt("@subChannel", 0);
            assertex(channel <= numChannels);
            assertex(subChannels[channel] == 0);
            assertex(subChannel != 0);
            subChannels[channel] = subChannel;
            IBYTIDelays[channel] = initIbytiDelay;
            channels[channelCount++] = channel;
        }
    }

    virtual bool checkSuspended(const RoxiePacketHeader &header, const IRoxieContextLogger &logctx)
    {
        bool suspended;
        {
            SpinBlock b(suspendCrit);
            suspended = suspendedChannels[header.channel];
        }
        if (suspended)
        {
            try 
            {
                RoxiePacketHeader newHeader(header, ROXIE_EXCEPTION);
                Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                StringBuffer message;
                message.appendf("<Exception><Code>%d</Code><Message>Channel %d is suspended</Message></Exception>", ROXIE_CHANNEL_SUSPENDED, header.channel);
                unsigned len = message.length();
                void *ret = output->getBuffer(len+1, true);
                memcpy(ret, message.str(), len+1);
                output->putBuffer(ret, len+1, true);
                output->flush(true);
            }   
            catch (IException *EInE)
            {
                EXCLOG(EInE, "Exception during checkSuspended");
                EInE->Release();
            }
            catch (...)
            {
                logctx.CTXLOG("Unknown Exception during checkSuspended");
            }
        }
        return suspended;
    }

    virtual bool suspendChannel(unsigned channel, bool suspend, const IRoxieContextLogger &logctx)
    {
        assertex(channel < MAX_CLUSTER_SIZE);
        bool prev;
        {
            SpinBlock b(suspendCrit);
            prev = suspendedChannels[channel];
            suspendedChannels[channel] = suspend;
        }
        if (suspend && subChannels[channel] && !prev)
        {
            logctx.CTXLOG("ERROR: suspending channel %d - aborting active queries", channel);
#ifdef ROXIE_SLA_LOGIC
            abortChannel(channel, slaWorkers);
#endif
            abortChannel(channel, hiWorkers);
            abortChannel(channel, loWorkers);
        }
        return prev;
    }

    virtual unsigned getHeadRegionSize() const
    {
        return loQueue.getHeadRegionSize();
    }

    virtual void setHeadRegionSize(unsigned newSize)
    {
#ifdef ROXIE_SLA_LOGIC
        slaQueue.setHeadRegionSize(newSize);
#endif
        hiQueue.setHeadRegionSize(newSize);
        loQueue.setHeadRegionSize(newSize);
    }

    virtual void start() 
    {
        for (unsigned i = 0; i < numWorkers; i++)
        {
            // MORE - why would we have same number of each?
            // MORE - All workers (hi or low) have same sys priority, same number of workers,
            //        and same queue size ... What can make a query marked high priority
            //        get prioity of resource over low one ?
            // MORE: I think we may want to have one set of worker threads that gets jobs of 2 (or 3 with SLA)
            //       queues (sla, high, and low). These workers will give higher priority for jobs
            //       on higher priority queues but without starving low ones... something similar to what
            //       I implemented in UDP output queues.
            //       For the time being, I will keep as is, and just add SLA queue and workers.
            loWorkers->start(&loQueue);
            hiWorkers->start(&hiQueue);
#ifdef ROXIE_SLA_LOGIC
            slaWorkers->start(&slaQueue);
#endif
        }
    }

    virtual void stop() 
    {
        loWorkers->stopAll(true);
        loQueue.signal(loWorkers->runningCount()); // MORE  - looks like a race here... interruptableSemaphore would be better
        hiWorkers->stopAll(true);
        hiQueue.signal(hiWorkers->runningCount());
#ifdef ROXIE_SLA_LOGIC
        slaWorkers->stopAll(true);
        slaQueue.signal(slaWorkers->runningCount());
#endif
    }

    virtual void join()  
    { 
#ifdef ROXIE_SLA_LOGIC
        slaWorkers->joinAll(true);
#endif
        hiWorkers->joinAll(true);
        loWorkers->joinAll(true);
        loWorkers.clear();  // Breaks a cyclic reference count that would stop us from releasing RoxieReceiverThread otherwise
        hiWorkers.clear();
#ifdef ROXIE_SLA_LOGIC
        slaWorkers.clear();
#endif
    }

    virtual IPooledThread *createNew()
    {
        return new CRoxieWorker;
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
        // This is called on the main slave reader thread so needs to be as fast as possible to avoid lost packets
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

    class StoppedException: public CInterface, public IException
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
        loop
        {
            try
            {
                Owned<IRoxieQueryPacket> packet = dequeue();
                RoxiePacketHeader &header = packet->queryHeader();
                unsigned length = packet->queryHeader().packetlength;

                {
                    MTIME_SECTION(queryActiveTimer(), "bucket_wait");
                    bucket.wait((length / 1024) + 1);
                }
                if (channelWrite(header.channel, &header, length) != length)
                    DBGLOG("multicast write wrote too little");
                atomic_inc(&packetsSent);
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

    virtual void sendPacket(IRoxieQueryPacket *x, const IRoxieContextLogger &logctx)
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

class RoxieSocketQueueManager : public RoxieReceiverBase
{
    unsigned maxPacketSize;
    bool running;
    Linked<ISendManager> sendManager;
    Linked<IReceiveManager> receiveManager;
    Owned<TokenBucket> bucket;

    class ReceiverThread : public Thread
    {
        RoxieSocketQueueManager &parent;
    public:
        ReceiverThread(RoxieSocketQueueManager &_parent) : parent(_parent), Thread("RoxieSocketQueueManager") {}
        int run()
        {
            // Raise the priority so ibyti's get through in a timely fashion
#ifdef __linux__
            setLinuxThreadPriority(3);
#else
            adjustPriority(1);
#endif
            return parent.run();
        }
    } readThread;

public:
    RoxieSocketQueueManager(unsigned snifferChannel, unsigned _numWorkers) : RoxieReceiverBase(_numWorkers), readThread(*this)
    {
        int udpQueueSize = topology->getPropInt("@udpQueueSize", UDP_QUEUE_SIZE);
        int udpSendQueueSize = topology->getPropInt("@udpSendQueueSize", UDP_SEND_QUEUE_SIZE);
        int udpMaxSlotsPerClient = topology->getPropInt("@udpMaxSlotsPerClient", 0x7fffffff);
#ifdef _DEBUG
        bool udpResendEnabled = topology->getPropBool("@udpResendEnabled", false);
#else
        bool udpResendEnabled = false;  // As long as it is known to be broken, we don't want it accidentally enabled in any release version
#endif
        maxPacketSize = multicastSocket->get_max_send_size();
        if ((maxPacketSize==0)||(maxPacketSize>65535))
            maxPacketSize = 65535;
        if (topology->getPropInt("@sendMaxRate", 0))
        {
            unsigned sendMaxRate = topology->getPropInt("@sendMaxRate");
            unsigned sendMaxRatePeriod = topology->getPropInt("@sendMaxRatePeriod", 1);
            bucket.setown(new TokenBucket(sendMaxRate, sendMaxRatePeriod, sendMaxRate));
            throttledPacketSendManager.setown(new RoxieThrottledPacketSender(*bucket, maxPacketSize));
        }

        IpAddress snifferIp;
        getChannelIp(snifferIp, snifferChannel);
        if (udpMaxSlotsPerClient > udpQueueSize)
            udpMaxSlotsPerClient = udpQueueSize;
        unsigned serverFlowPort = topology->getPropInt("@serverFlowPort", CCD_SERVER_FLOW_PORT);
        unsigned dataPort = topology->getPropInt("@dataPort", CCD_DATA_PORT);
        unsigned clientFlowPort = topology->getPropInt("@clientFlowPort", CCD_CLIENT_FLOW_PORT);
        unsigned snifferPort = topology->getPropInt("@snifferPort", CCD_SNIFFER_PORT);
        receiveManager.setown(createReceiveManager(serverFlowPort, dataPort, clientFlowPort, snifferPort, snifferIp, udpQueueSize, udpMaxSlotsPerClient, myNodeIndex));
        sendManager.setown(createSendManager(serverFlowPort, dataPort, clientFlowPort, snifferPort, snifferIp, udpSendQueueSize, fastLaneQueue ? 3 : 2, udpResendEnabled ? udpMaxSlotsPerClient : 0, bucket, myNodeIndex));
        running = false;
    }

    CriticalSection crit;
    Owned<RoxieThrottledPacketSender> throttledPacketSendManager;

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
            // MORE - crashes have been observed after exceptions here - mechanism not yet clear nor reproducible
            if (length > maxPacketSize)
            {
                StringBuffer s;
                throw MakeStringException(ROXIE_PACKET_ERROR, "Maximum packet length %d exceeded sending packet %s", maxPacketSize, header.toString(s).str());
            }

            CriticalBlock c(crit); // is this needed or was it just protecting multicast array? prevent interleaving?
            if (channelWrite(header.channel, &header, length) != length)
                logctx.CTXLOG("multicast write wrote too little");
            atomic_inc(&packetsSent);
        }
    }

    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx)
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendIbyti");
        RoxiePacketHeader ibytiHeader(header, header.activityId & ROXIE_PRIORITY_MASK);
    
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Sending IBYTI packet %s", ibytiHeader.toString(s).str());
        }

        CriticalBlock c(crit); // Not sure we really need this? Preventing interleave on writes? Should sock manage it?
        if (channelWrite(header.channel, &ibytiHeader, sizeof(RoxiePacketHeader)) != sizeof(RoxiePacketHeader))
            logctx.CTXLOG("sendIbyti wrote too little");

        atomic_inc(&ibytiPacketsSent);
    }

    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx)
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbort");
        RoxiePacketHeader abortHeader(header, header.activityId & ROXIE_PRIORITY_MASK);
        abortHeader.retries = QUERY_ABORTED;
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT packet %s", abortHeader.toString(s).str());
        }
        CriticalBlock c(crit); // Not sure we really need this? Preventing interleave on writes? Should sock manage it?
        if (channelWrite(header.channel, &abortHeader, sizeof(RoxiePacketHeader)) != sizeof(RoxiePacketHeader))
            logctx.CTXLOG("sendAbort wrote too little");

        atomic_inc(&abortsSent);
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx) 
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieSocketQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK);
        abortHeader.retries = QUERY_ABORTED;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        CriticalBlock c(crit); // Not sure we really need this? Preventing interleave on writes? Should sock manage it?
        if (channelWrite(header.channel, data.toByteArray(), data.length()) != data.length())
            logctx.CTXLOG("tr->write wrote too little");
        atomic_inc(&abortsSent);
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx)
    {
        unsigned qnum = outOfBand ? 0 : ((header.retries & ROXIE_FASTLANE) || !fastLaneQueue) ? 1 : 2;
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Creating Output Stream for reply packet on Q=%d - %s", qnum, header.toString(s).str());
        }
        return sendManager->createMessagePacker(header.uid, header.getSequenceId(), &header, sizeof(RoxiePacketHeader), header.serverIdx, qnum);
    }

    virtual bool replyPending(RoxiePacketHeader &header)
    {
        return sendManager->dataQueued(header.uid, header.getSequenceId(), header.serverIdx);
    }

    virtual bool abortCompleted(RoxiePacketHeader &header)
    {
        return sendManager->abortData(header.uid, header.getSequenceId(), header.serverIdx);
    }

    bool abortRunning(RoxiePacketHeader &header, IThreadPool *workers, bool checkRank, bool &preActivity)
    {
        bool queryFound = false;
        bool ret = false;
        Owned<IPooledThreadIterator> wi = workers->running();
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

    void doIbyti(RoxiePacketHeader &header, RoxieQueue &queue, IThreadPool *workers)
    {
        assertex(!localSlave);
        atomic_inc(&ibytiPacketsReceived);
        bool preActivity = false;

        if (traceLevel > 10)
        {
            IpAddress peer;
            StringBuffer s, s1;
            multicastSocket->getPeerAddress(peer).getIpText(s);
            header.toString(s1);
            DBGLOG("doIBYTI %s from %s", s1.str(), s.str());
            DBGLOG("header.retries=%x header.getSubChannelMask(header.channel)=%x", header.retries, header.getSubChannelMask(header.channel));
        }
        
        if (header.retries == QUERY_ABORTED)
        {
            abortRunning(header, workers, false, preActivity);
            queue.remove(header);

            if (traceLevel > 10)
            {
                StringBuffer s; 
                DBGLOG("Abort activity %s", header.toString(s).str());
            }
        }
        else
        {
            if ((header.retries & ROXIE_RETRIES_MASK) == header.getSubChannelMask(header.channel))
            {
                if (traceLevel > 10)
                    DBGLOG("doIBYTI packet was from self");
                atomic_inc(&ibytiPacketsFromSelf);
            }
            else
            {
                resetIbytiDelay(header.channel);
                bool foundInQ;
                {
                    CriticalBlock b(queueCrit);
                    foundInQ = queue.remove(header);
                }
                if (foundInQ) {
                    if (traceLevel > 10)
                    {
                        StringBuffer s; 
                        DBGLOG("Removed activity from Q : %s", header.toString(s).str());
                    }
                    atomic_inc(&ibytiPacketsWorked);
                    return;
                }
                if (abortRunning(header, workers, true, preActivity))
                {
                    if (preActivity)
                        atomic_inc(&ibytiPacketsWorked); // MORE - may want to have a diff counter for this (not in queue but in IBYTI wait or before) 
                    else 
                        atomic_inc(&ibytiPacketsHalfWorked);
                    return;
                }               
                if (traceLevel > 10)
                    DBGLOG("doIBYTI packet was too late");
                atomic_inc(&ibytiPacketsTooLate); // meaning either I started and reserve the right to finish, or I finished already
            }
        }
    }

    void processMessage(MemoryBuffer &mb, RoxiePacketHeader &header, RoxieQueue &queue, IThreadPool *workers)
    {
        // NOTE - this thread needs to do as little as possible - just read packets and queue them up - otherwise we can get packet loss due to buffer overflow
        // DO NOT put tracing on this thread except at very high tracelevels!

        if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK )
        {
            Owned<IRoxieQueryPacket> packet = createRoxiePacket(mb);
            if (traceLevel > 10)
            {
                StringBuffer s; 
                DBGLOG("ROXIE_CALLBACK %s", header.toString(s).str());
            }
            doFileCallback(packet);
        }
        else if ((header.activityId & ~ROXIE_PRIORITY_MASK) == 0)
            doIbyti(header, queue, workers); // MORE - check how fast this is!
        else
        {
            Owned<IRoxieQueryPacket> packet = createRoxiePacket(mb);
            SlaveContextLogger logctx(packet);
            unsigned retries = header.thisChannelRetries();
            if (retries)
            {
                // MORE - is this fast enough? By the time I am seeing retries I may already be under load. Could move onto a separate thread
                assertex(header.channel); // should never see a retry on channel 0
                if (retries >= SUBCHANNEL_MASK)
                    return; // someone sent a failure or something - ignore it

                // Send back an out-of-band immediately, to let Roxie server know that channel is still active
                if (!(testSlaveFailure & 0x800))
                {
                    RoxiePacketHeader newHeader(header, ROXIE_ALIVE);
                    Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                    output->flush(true);
                }

                // If it's a retry, look it up against already running, or output stream, or input queue
                // if found, send an IBYTI and discard retry request
                
                bool primChannel = true;
                if (subChannels[header.channel] != 1) primChannel = false;
                if (primChannel) 
                    atomic_inc(&retriesReceivedPrm); 
                else  
                    atomic_inc(&retriesReceivedSec); 
                bool alreadyRunning = false;
                Owned<IPooledThreadIterator> wi = workers->running();
                ForEach(*wi)
                {
                    CRoxieWorker &w = (CRoxieWorker &) wi->query();
                    if (w.match(header))
                    {
                        alreadyRunning = true;
                        if (primChannel) 
                            atomic_inc(&retriesIgnoredPrm);
                        else 
                            atomic_inc(&retriesIgnoredSec);
                        ROQ->sendIbyti(header, logctx);
                        if (logctx.queryTraceLevel() > 10)
                        {
                            StringBuffer xx; logctx.CTXLOG("Ignored retry on %s channel for running activity %s", primChannel?"primary":"secondary", header.toString(xx).str());
                        }
                        break;
                    }
                } 
                if (!alreadyRunning && checkCompleted && ROQ->replyPending(header))
                {
                    alreadyRunning = true;
                    if (primChannel) 
                        atomic_inc(&retriesIgnoredPrm); 
                    else 
                        atomic_inc(&retriesIgnoredSec);
                    ROQ->sendIbyti(header, logctx);
                    if (logctx.queryTraceLevel() > 10)
                    {
                        StringBuffer xx; logctx.CTXLOG("Ignored retry on %s channel for completed activity %s", primChannel?"primary":"secondary", header.toString(xx).str());
                    }
                }
                if (!alreadyRunning)
                {
                    if (logctx.queryTraceLevel() > 10)
                    {
                        StringBuffer xx; logctx.CTXLOG("Retry %d received on %s channel for %s", retries+1, primChannel?"primary":"secondary", header.toString(xx).str());
                    }
                    queue.enqueueUnique(packet.getClear());
                }
            }
            else // first time (not a retry). 
            {
                if (header.channel)
                {
                    queue.enqueue(packet.getClear());
                }
                else
                {
                    // Turn broadcast packet (channel 0), as early as possible, into non-0 channel packets.
                    // So retries and other communication with Roxie server (which uses non-0 channel numbers) will not cause double work or confusion.
                    // Unfortunately this is bad news for dropping packets
                    for (unsigned i = 1; i < channelCount; i++)
                        queue.enqueue(packet->clonePacket(channels[i]));
                    header.channel = channels[0];
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
                unsigned l;
                multicastSocket->read(mb.reserve(maxPacketSize), sizeof(RoxiePacketHeader), maxPacketSize, l, 5);
                mb.setLength(l);
                atomic_inc(&packetsReceived);
                RoxiePacketHeader &header = *(RoxiePacketHeader *) mb.toByteArray();
                if (l != header.packetlength)
                    DBGLOG("sock->read returned %d but packetlength was %d", l, header.packetlength);
                if (traceLevel > 10)
                {
                    StringBuffer s;
                    DBGLOG("Read from multicast: %s", header.toString(s).str());
                }
#ifdef ROXIE_SLA_LOGIC
                if (header.activityId & ROXIE_SLA_PRIORITY)
                    processMessage(mb, header, slaQueue, slaWorkers);
                else
#endif
                if (header.activityId & ROXIE_HIGH_PRIORITY)
                    processMessage(mb, header, hiQueue, hiWorkers);
                else
                    processMessage(mb, header, loQueue, loWorkers);
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

                        EXCLOG(E, "Exception reading or processing multicast msg");
                        E->Release();
                        MilliSleep(1000); // Give a chance for mem free
                    }
                    else 
                    {
                        EXCLOG(E, "Exception reading or processing multicast msg");
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

    virtual void flush(bool last_message);

    virtual void sendMetaInfo(const void *buf, unsigned len)
    {
        meta.append(len, buf);
    }

};

class CLocalMessageUnpackCursor : public CInterface, implements IMessageUnpackCursor
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
        // NOTE: tempting to think that we could avoid serializing in localSlave case, but have to be careful about the lifespan of the rowManager...
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

class CLocalMessageResult : public CInterface, implements IMessageResult
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

class CLocalMessageCollator : public CInterface, implements ILocalMessageCollator
{
    InterruptableSemaphore sem;
    QueueOf<IMessageResult, false> pending;
    CriticalSection crit;
    Linked<IRowManager> rowManager;
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

    virtual bool add_package(DataBuffer *dataBuff)
    {
        throwUnexpected(); // internal use in udp layer...
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
};

class RoxieLocalReceiveManager : public CInterface, implements ILocalReceiveManager
{
    MapXToMyClass<ruid_t, ruid_t, ILocalMessageCollator> collators;
    CriticalSection crit;
    Owned<StringContextLogger> logctx;
    Linked<IMessageCollator> defaultCollator;

public:
    IMPLEMENT_IINTERFACE;
    RoxieLocalReceiveManager() : logctx(new StringContextLogger("RoxieLocalReceiveManager"))
    {
    }

    virtual IMessageCollator *createMessageCollator(IRowManager *manager, ruid_t ruid)
    {
        IMessageCollator *collator = new CLocalMessageCollator(manager, ruid);
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

    virtual void setDefaultCollator(IMessageCollator *collator)
    {
        CriticalBlock b(crit);
        defaultCollator.set(collator);
    }

    virtual ILocalMessageCollator *lookupCollator(ruid_t id)
    {
        CriticalBlock b(crit);
        IMessageCollator *ret = collators.getValue(id);
        if (!ret)
            ret = defaultCollator;
        return LINK(QUERYINTERFACE(ret, ILocalMessageCollator));
    }

};

void LocalMessagePacker::flush(bool last_message)
{
    data.setLength(lastput);
    if (last_message)
    {
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
}

CLocalMessageCollator::CLocalMessageCollator(IRowManager *_rowManager, ruid_t _ruid) 
    : rowManager(_rowManager), id(_ruid)
{
    totalBytesReceived = 0;
}

CLocalMessageCollator::~CLocalMessageCollator()
{
    IMessageResult *goer;
    loop
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
    RoxieLocalQueueManager(unsigned snifferChannel, unsigned _numWorkers) : RoxieReceiverBase(_numWorkers)
    {
        receiveManager.setown(new RoxieLocalReceiveManager);
    }
        
    virtual bool suspendChannel(unsigned channel, bool suspend, const IRoxieContextLogger &logctx)
    {
        if (suspend)
            UNIMPLEMENTED;
        return false;
    }

    virtual void sendPacket(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx)
    {
        RoxiePacketHeader &header = packet->queryHeader();
        unsigned retries = header.thisChannelRetries();
        if (header.activityId == ROXIE_FILECALLBACK || header.activityId == ROXIE_DEBUGCALLBACK )
        {
            if (traceLevel > 5)
            {
                StringBuffer s; 
                DBGLOG("ROXIE_CALLBACK %s", header.toString(s).str());
            }
            doFileCallback(packet);
        }
        else if (retries < SUBCHANNEL_MASK)
        {
            if (retries)
            {
                // Send back an out-of-band immediately, to let Roxie server know that channel is still active
                RoxiePacketHeader newHeader(header, ROXIE_ALIVE);
                Owned<IMessagePacker> output = createOutputStream(newHeader, true, logctx);
                output->flush(true);
                return; // No point sending the retry in localSlave mode
            }
            RoxieQueue *targetQueue;
#ifdef ROXIE_SLA_LOGIC
            if (header.activityId & ROXIE_SLA_PRIORITY)
                targetQueue = &slaQueue;
            else
#endif
            if (header.activityId & ROXIE_HIGH_PRIORITY)
                targetQueue = &hiQueue;
            else
                targetQueue = &loQueue;

            if (header.channel)
            {
                targetQueue->enqueue(LINK(packet));
            }
            else
            {
                // Turn broadcast packet (channel 0), as early as possible, into non-0 channel packets.
                // So retries and other communication with Roxie server (which uses non-0 channel numbers) will not cause double work or confusion.
                for (unsigned i = 0; i < channelCount; i++)
                {
                    targetQueue->enqueue(packet->clonePacket(channels[i]));
                }
            }
        }
    }

    virtual void sendIbyti(RoxiePacketHeader &header, const IRoxieContextLogger &logctx)
    {
        // Don't do IBYTI's when local slave - no buddy to talk to anyway
    }

    virtual void sendAbort(RoxiePacketHeader &header, const IRoxieContextLogger &logctx)
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieLocalQueueManager::sendAbort");
        RoxiePacketHeader abortHeader(header, header.activityId & ROXIE_PRIORITY_MASK);
        abortHeader.retries = QUERY_ABORTED;
        if (logctx.queryTraceLevel() > 8)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT packet %s", abortHeader.toString(s).str());
        }
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader);
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
        atomic_inc(&abortsSent);
    }

    virtual void sendAbortCallback(const RoxiePacketHeader &header, const char *lfn, const IRoxieContextLogger &logctx)
    {
        MTIME_SECTION(queryActiveTimer(), "RoxieLocalQueueManager::sendAbortCallback");
        RoxiePacketHeader abortHeader(header, ROXIE_FILECALLBACK);
        abortHeader.retries = QUERY_ABORTED;
        MemoryBuffer data;
        data.append(sizeof(abortHeader), &abortHeader).append(lfn);
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s; logctx.CTXLOG("Sending ABORT FILECALLBACK packet %s for file %s", abortHeader.toString(s).str(), lfn);
        }
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(data);
        sendPacket(packet, logctx);
        atomic_inc(&abortsSent);
    }

    virtual IMessagePacker *createOutputStream(RoxiePacketHeader &header, bool outOfBand, const IRoxieContextLogger &logctx)
    {
        return new LocalMessagePacker(header, outOfBand, receiveManager);
    }

    virtual IReceiveManager *queryReceiveManager()
    {
        return receiveManager;
    }

    virtual bool replyPending(RoxiePacketHeader &header)
    {
        // MORE - should really have some code here! But returning true is a reasonable approximation.
        return true;
    }

    virtual bool abortCompleted(RoxiePacketHeader &header)
    {
        // MORE - should really have some code here!
        return false;
    }



};

IRoxieOutputQueueManager *ROQ;

extern IRoxieOutputQueueManager *createOutputQueueManager(unsigned snifferChannel, unsigned numWorkers)
{
    if (localSlave)
        return new RoxieLocalQueueManager(snifferChannel, numWorkers);
    else
        return new RoxieSocketQueueManager(snifferChannel, numWorkers);

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
        rowManager.setown(roxiemem::createRowManager(1, NULL, *logctx, NULL));
        mc.setown(ROQ->queryReceiveManager()->createMessageCollator(rowManager, RUID_DISCARD));
        ROQ->queryReceiveManager()->setDefaultCollator(mc);
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
                ROQ->queryReceiveManager()->setDefaultCollator(mc);
            }
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
// Reply for every channel, or just once for every slave?
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
        unsigned packetSize = sizeof(RoxiePacketHeader) + sizeof(char) + strlen("PING") + 1 + sizeof(PingRecord);
        void *packetData = malloc(packetSize);
        RoxiePacketHeader *header = (RoxiePacketHeader *) packetData;
        RemoteActivityId pingId(ROXIE_PING | priorityMask, 0);
        header->init(pingId, 0, 0, 0);

        char *finger = (char *) (header + 1);
        *finger++ = (char) LOGGING_FLAGSPRESENT;
        strcpy(finger, "PING");
        finger += strlen("PING")+1;
        if (traceLevel > 1)
            DBGLOG("PING sent");

        PingRecord data;
        data.senderIP.ipset(getNodeAddress(myNodeIndex));
        data.tick = usTick();
        memcpy(finger, &data, sizeof(PingRecord));
        Owned<IRoxieQueryPacket> packet = createRoxiePacket(packetData, packetSize);
        ROQ->sendPacket(packet, logctx);
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
        rowManager.setown(roxiemem::createRowManager(1, NULL, queryDummyContextLogger(), NULL));
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
                if (traceLevel)
                    DBGLOG("PING: %d replies received, average delay %d", pingsReceived, pingsReceived ? pingsElapsed / pingsReceived : 0);
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

