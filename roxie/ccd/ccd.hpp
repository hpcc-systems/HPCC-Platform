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

#ifndef _CCD_INCL
#define _CCD_INCL

#include "jexcept.hpp"
#include "jsocket.hpp"
#include "jptree.hpp"
#include "udplib.hpp"
#include "portlist.h"
#include "thorsoapcall.hpp"
#include "thorxmlwrite.hpp"
#include "jlog.hpp"
#include "jstats.h"
#include "roxie.hpp"
#include "roxiedebug.ipp"
#include "eclrtl.hpp"

#ifdef _WIN32
#ifdef CCD_EXPORTS
#define CCD_API __declspec(dllexport)
#else
#define CCD_API __declspec(dllimport)
#endif
#else
#define CCD_API
#endif

#define PARALLEL_EXECUTE

#define RE_FailedToLoadProcedure    0x1000
#define RE_FailedToLoadSharedObject 0x2000

#define MAXTRACELEVEL 100     // don't want traceLevel+1 to wrap to 0 in lsb
#define MAX_CLUSTER_SIZE 1024
#define UDP_QUEUE_SIZE 100
#define UDP_SEND_QUEUE_SIZE 50

#define ROXIE_STATEFILE_VERSION 2

extern IException *MakeRoxieException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
extern Owned<ISocket> multicastSocket;
extern size32_t channelWrite(unsigned channel, void const* buf, size32_t size);
void addEndpoint(unsigned channel, const IpAddress &slaveIp, unsigned port);
void joinMulticastChannel(unsigned channel);

extern unsigned channels[MAX_CLUSTER_SIZE];     // list of all channel numbers for this node
extern unsigned channelCount;                   // number of channels this node is doing
extern unsigned subChannels[MAX_CLUSTER_SIZE];  // maps channel numbers to subChannels for this node
extern bool suspendedChannels[MAX_CLUSTER_SIZE];// indicates suspended channels for this node
extern unsigned numSlaves[MAX_CLUSTER_SIZE];    // number of slaves listening on this channel

extern unsigned myNodeIndex;
#define OUTOFBAND_SEQUENCE    0x8000        // indicates an out-of-band reply
#define OVERFLOWSEQUENCE_MAX 0x7fffu        // Max value before we want to wrap (to avoid collision with flag)
#define CONTINUE_SEQUENCE_SKIPTO  0x8000    // flag in continueSequence field indicating presence of skipTo data
#define CONTINUESEQUENCE_MAX 0x7fffu        // Max value before we want to wrap (to avoid collision with flag)

#define ROXIE_SLA_PRIORITY 0x40000000    // mask in activityId indicating it goes SLA priority queue
#define ROXIE_HIGH_PRIORITY 0x80000000   // mask in activityId indicating it goes on the fast queue
#define ROXIE_LOW_PRIORITY 0x00000000    // mask in activityId indicating it goes on the slow queue (= default)
#ifdef ROXIE_SLA_LOGIC
#define ROXIE_PRIORITY_MASK (ROXIE_SLA_PRIORITY | ROXIE_HIGH_PRIORITY | ROXIE_LOW_PRIORITY)
#else
#define ROXIE_PRIORITY_MASK (ROXIE_HIGH_PRIORITY | ROXIE_LOW_PRIORITY )
#endif  

#define ROXIE_ACTIVITY_FETCH 0x20000000    // or'ed into activityId for fetch part of full keyed join activities

// Status information returned in the activityId field of the header:
// note - any of these also also set sequence top bit to ensure not regarded as dup.
#define ROXIE_ACTIVITY_SPECIAL_FIRST    0x3ffffff0u
#define ROXIE_UNLOAD 0x3ffffff6u
#define ROXIE_DEBUGREQUEST 0x3ffffff7u
#define ROXIE_DEBUGCALLBACK 0x3ffffff8u
#define ROXIE_PING 0x3ffffff9u
#define ROXIE_TRACEINFO 0x3ffffffau
#define ROXIE_FILECALLBACK 0x3ffffffbu
#define ROXIE_ALIVE 0x3ffffffcu
#define ROXIE_KEYEDLIMIT_EXCEEDED 0x3ffffffdu
#define ROXIE_LIMIT_EXCEEDED 0x3ffffffeu
#define ROXIE_EXCEPTION   0x3fffffffu
#define ROXIE_ACTIVITY_SPECIAL_LAST    0x3fffffffu


#define SUBCHANNEL_MASK 3
#define SUBCHANNEL_BITS 2    // allows for up to 7-way redundancy in a 16-bit short retries flag, high bits used for indicators/flags
//#define TIME_PACKETS

#define ROXIE_FASTLANE      0x8000u         // mask in retries indicating slave reply goes on the fast queue
#define ROXIE_BROADCAST     0x4000u         // mask in retries indicating original request was a broadcast
#define ROXIE_RETRIES_MASK  (~(ROXIE_FASTLANE|ROXIE_BROADCAST)) // retries bits mask
#define QUERY_ABORTED       0xffffu         // special value for retries to indicate abandoned query

#ifdef _DEBUG
#define MAX_DEBUGREQUEST_RETRIES 1
#define DEBUGREQUEST_TIMEOUT 500000
#else
#define MAX_DEBUGREQUEST_RETRIES 3
#define DEBUGREQUEST_TIMEOUT 5000
#endif

#define ROXIE_DALI_CONNECT_TIMEOUT 5000

class RemoteActivityId
{
public:
    hash64_t queryHash;
    unsigned activityId; 

    inline bool isHighPriority() const { return (activityId & ROXIE_PRIORITY_MASK) == ROXIE_HIGH_PRIORITY; }
    inline bool isSLAPriority() const { return (activityId & ROXIE_PRIORITY_MASK) == ROXIE_SLA_PRIORITY; }

    inline RemoteActivityId(unsigned _activityId, hash64_t _queryHash) 
        : activityId(_activityId), queryHash(_queryHash)
    {
    }
    inline MemoryBuffer &serialize(MemoryBuffer &out) const
    {
        return out.append(activityId).append(queryHash);
    }
    inline RemoteActivityId(MemoryBuffer &in)
    {
        in.read(activityId);
        in.read(queryHash);
    }
};

class RoxiePacketHeader
{
private:
    RoxiePacketHeader(const RoxiePacketHeader &source);

public:
    unsigned short packetlength;
    unsigned short retries;         // how many retries on this query, the high bits are used as flags, see above
    unsigned short overflowSequence;// Used if more than one packet-worth of data from server - eg keyed join. We don't mind if we wrap...
    unsigned short continueSequence;// Used if more than one chunk-worth of data from slave. We don't mind if we wrap 
    unsigned activityId;            // identifies the helper factory to be used (activityId in graph)
    hash64_t queryHash;             // identifies the query

    ruid_t uid;                     // unique id
    unsigned serverIdx;             // final result (server) destination 
#ifdef TIME_PACKETS
    unsigned tick;
#endif
    unsigned short channel;         // multicast family to send on

    inline RoxiePacketHeader(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence)
    {
        packetlength = sizeof(RoxiePacketHeader);
#ifdef TIME_PACKETS
        tick = 0;
#endif
        init(_remoteId, _uid, _channel, _overflowSequence);
    }

    RoxiePacketHeader(const RoxiePacketHeader &source, unsigned _activityId)
    {
        // Used to create the header to send a callback to originating server or an IBYTI to a buddy
        activityId = _activityId;
        uid = source.uid;
        queryHash = source.queryHash;
        serverIdx = source.serverIdx;
        channel = source.channel;
        overflowSequence = source.overflowSequence;
        continueSequence = source.continueSequence;
        if (_activityId >= ROXIE_ACTIVITY_SPECIAL_FIRST && _activityId <= ROXIE_ACTIVITY_SPECIAL_LAST)
            overflowSequence |= OUTOFBAND_SEQUENCE; // Need to make sure it is not treated as dup of actual reply in the udp layer
        retries = getSubChannelMask(channel) | (source.retries & ~ROXIE_RETRIES_MASK);
#ifdef TIME_PACKETS
        tick = source.tick;
#endif
        packetlength = sizeof(RoxiePacketHeader);
    }

    static unsigned getSubChannelMask(unsigned channel)
    {
        unsigned subChannel = subChannels[channel] - 1;
        return SUBCHANNEL_MASK << (SUBCHANNEL_BITS * subChannel);
    }

    inline unsigned getSequenceId() const
    {
        return (((unsigned) overflowSequence) << 16) | (unsigned) continueSequence;
    }

    inline unsigned priorityHash() const
    {
        // Used to determine which slave to act as primary and which as secondary for a given packet (thus spreading the load)
        // It's important that we do NOT include channel (since that would result in different values for the different slaves responding to a broadcast)
        // We also don't include continueSequence since we'd prefer continuations to go the same way as original
        unsigned hash = hashc((const unsigned char *) &serverIdx, sizeof(serverIdx), 0);
        hash = hashc((const unsigned char *) &uid, sizeof(uid), hash);
        hash += overflowSequence; // MORE - is this better than hashing?
        if (traceLevel > 9)
        {
            StringBuffer s;
            DBGLOG("Calculating hash: %s hash was %d", toString(s).str(), hash);
        }
        return hash;
    }

    inline bool matchPacket(const RoxiePacketHeader &oh) const
    {
        // used when matching up a kill packet against a pending one...
        // DO NOT compare activityId - they are not supposed to match, since 0 in activityid identifies ibyti!
        return 
            oh.uid==uid && 
            (oh.overflowSequence & ~OUTOFBAND_SEQUENCE) == (overflowSequence & ~OUTOFBAND_SEQUENCE) && 
            oh.continueSequence == continueSequence && 
            oh.serverIdx==serverIdx && 
            oh.channel==channel;
    }

    void init(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence)
    {
        retries = 0;
        activityId = _remoteId.activityId;
        queryHash = _remoteId.queryHash;
        uid = _uid;
        serverIdx = myNodeIndex;
        channel = _channel;
        overflowSequence = _overflowSequence;
        continueSequence = 0;
    }

    StringBuffer &toString(StringBuffer &ret) const;

    bool allChannelsFailed() 
    {
        unsigned mask = (1 << (numSlaves[channel] * SUBCHANNEL_BITS)) - 1;
        return (retries & mask) == mask;
    }

    bool retry()
    {
        bool worthRetrying = false;
        unsigned mask = SUBCHANNEL_MASK;
        for (unsigned subChannel = 0; subChannel < numSlaves[channel]; subChannel++)
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

    inline void noteAlive(unsigned mask)
    {
        retries = (retries & ~mask);
    }

    inline void noteException(unsigned mask)
    {
        retries = (retries | mask);
    }

    inline void setException()
    {
        unsigned subChannel = subChannels[channel] - 1;
        retries |= SUBCHANNEL_MASK << (SUBCHANNEL_BITS * subChannel);
    }

    unsigned thisChannelRetries()
    {
        unsigned shift = SUBCHANNEL_BITS * (subChannels[channel] - 1);
        unsigned mask = SUBCHANNEL_MASK << shift;
        return (retries & mask) >> shift;
    }
};

interface IRoxieQueryPacket : extends IInterface
{
    virtual RoxiePacketHeader &queryHeader() const = 0;
    virtual const void *queryContinuationData() const = 0;
    virtual unsigned getContinuationLength() const = 0;
    virtual const byte *querySmartStepInfoData() const = 0;
    virtual unsigned getSmartStepInfoLength() const = 0;
    virtual const byte *queryTraceInfo() const = 0;
    virtual unsigned getTraceLength() const = 0;
    virtual const void *queryContextData() const = 0;
    virtual unsigned getContextLength() const = 0;

    virtual IRoxieQueryPacket *clonePacket(unsigned channel) const = 0;
    virtual unsigned hash() const = 0;
    virtual bool cacheMatch(const IRoxieQueryPacket *) const = 0; // note - this checks whether it's a repeat from server's point-of-view
    virtual IRoxieQueryPacket *insertSkipData(size32_t skipDataLen, const void *skipData) const = 0;
};

interface IQueryDll;

// Global configuration info
extern bool isMonitor;
extern bool shuttingDown;
extern unsigned numChannels;
extern unsigned numActiveChannels;
extern unsigned callbackRetries;
extern unsigned callbackTimeout;
extern unsigned lowTimeout;
extern unsigned highTimeout;
extern unsigned slaTimeout;
extern unsigned headRegionSize;
extern CriticalSection ccdChannelsCrit;
extern IPropertyTree* ccdChannels;
extern IPropertyTree* topology;
extern StringArray allQuerySetNames;

extern bool crcResources;
extern bool logFullQueries;
extern bool blindLogging;
extern bool debugPermitted;
extern bool useRemoteResources;
extern bool checkFileDate;
extern bool lazyOpen;
extern bool localSlave;
extern bool doIbytiDelay;
extern unsigned initIbytiDelay;
extern unsigned minIbytiDelay;
extern bool copyResources;
extern bool chunkingHeap;
extern unsigned keyedJoinFlowLimit;
extern unsigned perChannelFlowLimit;
extern unsigned parallelLoopFlowLimit;
extern unsigned numServerThreads;
extern unsigned numRequestArrayThreads;
extern unsigned readTimeout;
extern unsigned indexReadChunkSize;
extern unsigned smartSteppingChunkRows;
extern SocketEndpoint ownEP;
extern unsigned maxBlockSize;
extern unsigned maxLockAttempts;
extern SocketEndpointArray allRoxieServers;
extern bool enableHeartBeat;
extern bool checkVersion;
extern unsigned memoryStatsInterval;
extern unsigned pingInterval;
extern unsigned socketCheckInterval;
extern memsize_t defaultMemoryLimit;
extern unsigned defaultTimeLimit[3];
extern unsigned defaultWarnTimeLimit[3];
extern bool deleteUnneededFiles;
extern bool checkPrimaries;
extern bool pretendAllOpt;
extern ClientCertificate clientCert;
extern bool useHardLink;
extern unsigned maxFileAge[2];
extern unsigned minFilesOpen[2];
extern unsigned maxFilesOpen[2];
extern unsigned restarts;
extern bool checkCompleted;
extern unsigned preabortKeyedJoinsThreshold;
extern unsigned preabortIndexReadsThreshold;
extern bool traceStartStop;
extern bool traceServerSideCache;
extern bool timeActivities;
extern int defaultCheckingHeap;
extern unsigned watchActivityId;
extern unsigned testSlaveFailure;
extern unsigned dafilesrvLookupTimeout;
extern bool fastLaneQueue;
extern unsigned mtu_size;
extern StringBuffer fileNameServiceDali;
extern StringBuffer roxieName;
extern bool trapTooManyActiveQueries;
extern bool allowRoxieOnDemand;
extern unsigned maxEmptyLoopIterations;
extern unsigned maxGraphLoopIterations;
extern HardwareInfo hdwInfo;
extern unsigned parallelAggregate;
extern bool inMemoryKeysEnabled;
extern unsigned __int64 minFreeDiskSpace;
extern unsigned serverSideCacheSize;
extern bool probeAllRows;
extern bool steppingEnabled;
extern bool simpleLocalKeyedJoins;
extern bool enableKeyDiff;
extern bool enableForceKeyDiffCopy;
extern bool useTreeCopy;
extern PTreeReaderOptions defaultXmlReadFlags;
extern bool mergeSlaveStatistics;
extern bool roxieMulticastEnabled;   // enable use of multicast for sending requests to slaves
extern bool preloadOnceData;

extern unsigned udpMulticastBufferSize;
extern size32_t diskReadBufferSize;

extern bool nodeCachePreload;
extern unsigned nodeCacheMB;
extern unsigned leafCacheMB;
extern unsigned blobCacheMB;

struct PartNoType
{
    unsigned short partNo;  // _n_of_400
    unsigned short fileNo;  // superkey file number
};

extern unsigned statsExpiryTime;
extern time_t startupTime;
extern unsigned miscDebugTraceLevel;
extern bool fieldTranslationEnabled;

extern unsigned defaultParallelJoinPreload;
extern unsigned defaultConcatPreload;
extern unsigned defaultFetchPreload;
extern unsigned defaultFullKeyedJoinPreload;
extern unsigned defaultKeyedJoinPreload;
extern unsigned defaultPrefetchProjectPreload;

extern StringBuffer logDirectory;
extern StringBuffer pluginDirectory;
extern StringBuffer pluginsList;
extern StringBuffer queryDirectory;
extern StringBuffer baseDataDirectory;
extern StringBuffer codeDirectory;
extern StringBuffer tempDirectory;

#undef UNIMPLEMENTED
#undef throwUnexpected
extern void doUNIMPLEMENTED(unsigned line, const char *file);
#define UNIMPLEMENTED { doUNIMPLEMENTED(__LINE__, __FILE__); throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "UNIMPLEMENTED"); }
#define throwUnexpected()          throw MakeStringException(ROXIE_INTERNAL_ERROR, "Internal Error at %s(%d)", __FILE__, __LINE__)

extern IRoxieQueryPacket *createRoxiePacket(void *data, unsigned length);
extern IRoxieQueryPacket *createRoxiePacket(MemoryBuffer &donor); // note: donor is empty after call
extern void dumpBuffer(const char *title, const void *buf, unsigned recSize);

inline unsigned getBondedChannel(unsigned partNo)
{
    return ((partNo - 1) % numChannels) + 1;
}

extern void FatalError(const char *format, ...)  __attribute__((format(printf, 1, 2)));
extern unsigned getNextInstanceId();
extern void closedown();

#define LOGGING_INTERCEPTED     0x01
#define LOGGING_TIMEACTIVITIES  0x02
#define LOGGING_DEBUGGERACTIVE  0x04
#define LOGGING_BLIND           0x08
#define LOGGING_TRACELEVELSET   0x10
#define LOGGING_FLAGSPRESENT    0x40

class LogItem : public CInterface
{
    friend class SlaveContextLogger;

    TracingCategory category;
    StringAttr prefix;
    StringAttr text;
    unsigned time;
    unsigned channel;
    unsigned statCode;
    unsigned __int64 statValue;
    unsigned statCount;

public:
    LogItem(TracingCategory _category, const char *_prefix, unsigned _time, unsigned _channel, const char *_text) 
        : category(_category), prefix(_prefix), time(_time), channel(_channel), text(_text)
    {
        statCode = 0;
        statValue = 0;
        statCount = 0;
    }

    LogItem(TracingCategory _category, unsigned _channel, unsigned _statCode, unsigned __int64 _statValue, unsigned _count) 
        : category(_category), channel(_channel), statCode(_statCode), statValue(_statValue), statCount(_count)
    {
        time = 0;
    }
    
    inline bool isStatistics() const
    {
        return category==LOG_STATVALUES;
    }

    inline unsigned getStatCode() const
    {
        return statCode;
    }

    inline unsigned __int64 getStatValue() const
    {
        return statValue;
    }

    inline unsigned __int64 getStatCount() const
    {
        return statCount;
    }

    LogItem(MemoryBuffer &buf)
    {
        char c; buf.read(c); category = (TracingCategory) c;
        buf.read(channel);
        if (category==LOG_STATVALUES)
        {
            time = 0;
            buf.read(statCode);
            buf.read(statValue);
            buf.read(statCount);
        }
        else
        {
            buf.read(prefix);
            buf.read(text);
            buf.read(time);
            statCode = 0;
            statValue = 0;
            statCount = 0;
        }
    }

    void serialize(MemoryBuffer &buf)
    {
        buf.append((char) category);
        buf.append(channel);
        if (category==LOG_STATVALUES)
        {
            buf.append(statCode);
            buf.append(statValue);
            buf.append(statCount);
        }
        else
        {
            buf.append(prefix);
            buf.append(text);
            buf.append(time);
        }
    }

    static const char *getCategoryString(TracingCategory c)
    {
        switch (c)
        {
        case LOG_TRACING: return "TRACE";
        case LOG_ERROR: return "ERROR";
        case LOG_TIMING: return "TIMING";
        case LOG_STATISTICS: return "STATISTICS";
        case LOG_STATVALUES: return "STATVALUES";
        default: return "UNKNOWN";
        }
    }

    void toXML(StringBuffer &out)
    {
        out.append("<Log><Category>").append(getCategoryString(category)).append("</Category>");
        out.append("<Channel>").append(channel).append("</Channel>");
        out.append("<Time>").append(time/1000).append('.').appendf("%03d", time % 1000).append("</Time>");
        if (prefix)
        {
            out.append("<Prefix>");
            encodeXML(prefix, out);
            out.append("</Prefix>");
        }
        if (text)
        {
            out.append("<Text>");
            encodeXML(text, out);
            out.append("</Text>");
        }
        out.append("</Log>\n");
    }

    void outputXML(IXmlStreamFlusher &out)
    {
        StringBuffer b;
        toXML(b);
        out.flushXML(b, true);
    }

};

extern void putStatsValue(IPropertyTree *node, const char *statName, const char *statType, unsigned __int64 val);
extern void putStatsValue(StringBuffer &reply, const char *statName, const char *statType, unsigned __int64 val);

class StatsCollector : public CInterface, implements IInterface
{
    unsigned __int64 *cumulative;
    unsigned *counts;
    mutable SpinLock lock;
    bool aborted;

    inline void init()
    {
        if (!cumulative)
        {
            cumulative = new unsigned __int64[STATS_SIZE];
            counts = new unsigned [STATS_SIZE];
            memset(cumulative, 0, STATS_SIZE * sizeof(cumulative[0]));
            memset(counts, 0, STATS_SIZE * sizeof(counts[0]));
        }
    }

    inline static const char *getStatCombineModeName(StatisticCombineType type)
    {
        switch(type)
        {
            case STATSMODE_COMBINE_SUM: return "sum";
            case STATSMODE_COMBINE_MAX: return "max";
            case STATSMODE_COMBINE_MIN: return "min";
            default:
                throwUnexpected();
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    StatsCollector()
    {
        // CAUTION: this object is reused by threadpooling - so be sure to update reset() method too!
        cumulative = NULL;
        counts = NULL;
        aborted = false;
        // CAUTION: this object is reused by threadpooling - so be sure to update reset() method too!
    }
    
    ~StatsCollector()
    {
        if (cumulative) delete [] cumulative;
        if (counts) delete [] counts;
    }

    void noteStatistic(unsigned statIdx, unsigned __int64 value, unsigned count)
    {
        SpinBlock b(lock);
        if (aborted)
            throw MakeStringException(ROXIE_ABORT_ERROR, "Roxie server requested abort for running activity");
        init();
        assert (statIdx < STATS_SIZE);
        switch (getStatCombineMode(statIdx))
        {
        case STATSMODE_COMBINE_SUM:
            cumulative[statIdx] += value;
            break;
        case STATSMODE_COMBINE_MAX:
            if (!counts[statIdx] || cumulative[statIdx] <= value)
                cumulative[statIdx] = value;
            break;
        case STATSMODE_COMBINE_MIN:
            if (!counts[statIdx] || cumulative[statIdx] >= value)
                cumulative[statIdx] = value;
            break;
        }
        counts[statIdx] += count;
    }

    void merge(const StatsCollector &from)
    {
        SpinBlock b(from.lock);
        if (from.cumulative)
        {
            for (unsigned i = 0; i < STATS_SIZE; i++)
            {
                if (from.counts[i])
                    noteStatistic(i, from.cumulative[i], from.counts[i]);
            }
        }
    }

    void dumpStats(const IRoxieContextLogger &logctx) const
    {
        SpinBlock b(lock);
        if (cumulative)
        {
            for (unsigned i = 0; i < STATS_SIZE; i++)
            {
                if (counts[i])
                {
                    StringBuffer prefix, text;
                    logctx.getLogPrefix(prefix);
                    text.appendf("%s - %"I64F"d (%d instances)", getStatName(i), cumulative[i], counts[i]);
                    logctx.CTXLOGa(LOG_STATISTICS, prefix.str(), text.str());
                }
            }
        }
    }

    void toXML(StringBuffer &reply) const
    {
        SpinBlock b(lock);
        if (cumulative)
        {
            for (unsigned i = 0; i < STATS_SIZE; i++)
            {
                if (counts[i])
                {
                    putStatsValue(reply, getStatName(i), getStatCombineModeName(getStatCombineMode(i)), counts[i]);
                }
            }
        }
    }

    void getNodeProgressInfo(IPropertyTree &node) const
    {
        SpinBlock b(lock);
        if (cumulative)
        {
            for (unsigned i = 0; i < STATS_SIZE; i++)
            {
                if (counts[i])
                {
                    putStatsValue(&node, getStatShortName(i), getStatCombineModeName(getStatCombineMode(i)), counts[i]);
                }
            }
        }
    }


    void cascade(unsigned channel, const IRoxieContextLogger &logctx) const
    {
        SpinBlock b(lock);
        if (cumulative)
        {
            for (unsigned i = 0; i < STATS_SIZE; i++)
            {
                if (counts[i])
                {
                    logctx.CTXLOGl(new LogItem(LOG_STATVALUES, channel, i, cumulative[i], counts[i]));
                }
            }
        }
    }

    void reset()
    {
        SpinBlock b(lock);
        if (cumulative) delete [] cumulative;
        if (counts) delete [] counts;
        cumulative = NULL;
        counts = NULL;
        aborted = false;
    }

    void requestAbort()
    {
        SpinBlock b(lock);
        aborted = true;
    }
};

class ContextLogger : public CInterface, implements IRoxieContextLogger
{
protected:
    mutable CriticalSection crit;
    unsigned start;
    unsigned ctxTraceLevel;
    mutable StatsCollector stats;
    unsigned channel;
public: // Not very clean but I don't care
    bool intercept;
    bool blind;
    mutable CIArrayOf<LogItem> log;
public:
    IMPLEMENT_IINTERFACE;

    ContextLogger() 
    {
        ctxTraceLevel = traceLevel;
        intercept = false;
        blind = false;
        start = msTick();
        channel = 0;
    }

    void outputXML(IXmlStreamFlusher &out)
    {
        CriticalBlock b(crit);
        ForEachItemIn(idx, log)
        {
            log.item(idx).outputXML(out);
        }
    };

    virtual void CTXLOG(const char *format, ...) const  __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, format);
        CTXLOGva(format, args);
        va_end(args);
    }
    virtual void CTXLOGva(const char *format, va_list args) const
    {
        StringBuffer prefix, text;
        getLogPrefix(prefix);
        text.valist_appendf(format, args);
        DBGLOG("[%s] %s", prefix.str(), text.str());
        if (intercept)
        {
            CriticalBlock b(crit);
            log.append(* new LogItem(LOG_TRACING, prefix, msTick() - start, channel, text));
            flush(false, false);
        }
    }
    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const
    {
        if (category == LOG_TRACING)
            DBGLOG("[%s] %s", prefix, text);
        else
            DBGLOG("[%s] %s: %s", prefix, LogItem::getCategoryString(category), text);
        if (intercept)
        {
            CriticalBlock b(crit);
            log.append(* new LogItem(category, prefix, msTick() - start, channel, text));
            flush(false, false);
        }
    }
    virtual void logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const  __attribute__((format(printf, 5, 6)))
    {
        va_list args;
        va_start(args, format);
        CTXLOGaeva(E, file, line, 0, format, args);
        va_end(args);
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const
    {
        CTXLOGaeva(E, file, line, 0, format, args);
    }

    virtual void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const __attribute__((format(printf, 6, 7)))
    {
        va_list args;
        va_start(args, format);
        CTXLOGaeva(E, file, line, prefix, format, args);
        va_end(args);
    }

    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const
    {
        StringBuffer text;
        text.append("ERROR");
        if (E)
            text.append(": ").append(E->errorCode());
        if (file)
            text.appendf(": %s(%d) ", file, line);
        if (E)
            E->errorMessage(text.append(": "));
        if (format)
        {
            text.append(": ").valist_appendf(format, args);
        }
        LOG(MCoperatorProgress, unknownJob, "[%s] %s", prefix, text.str());
        if (intercept)
        {
            CriticalBlock b(crit);
            log.append(* new LogItem(LOG_ERROR, prefix, msTick() - start, channel, text));
            flush(false, false);
        }
    }
    virtual void CTXLOGl(LogItem *logItem) const
    {
        // NOTE - we don't actually print anything to logfile here - was already printed on slave
        log.append(*logItem);
        flush(false, false);
    }

    void setIntercept(bool _intercept)
    {
        intercept = _intercept;
    }

    void setBlind(bool _blind)
    {
        blind = _blind;
    }

    void setTraceLevel(unsigned _traceLevel)
    {
        ctxTraceLevel = _traceLevel;
    }

    virtual void flush(bool closing, bool aborted) const
    {
    }

    void dumpStats() const
    {
        stats.dumpStats(*this);
    }

    virtual bool isIntercepted() const
    {
        return intercept;
    }

    virtual bool isBlind() const
    {
        return blind;
    }

    virtual void noteStatistic(unsigned statCode, unsigned __int64 value, unsigned count) const
    {
        stats.noteStatistic(statCode, value, count);
    }

    virtual unsigned queryTraceLevel() const
    {
        return ctxTraceLevel;
    }
};

class StringContextLogger : public ContextLogger
{
    StringAttr id;
public:
    StringContextLogger(const char *_id) : id(_id)
    {
    }
    StringContextLogger()
    {
    }
    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
    {
        return ret.append(id);  
    }
    void set(const char *_id)
    {
        stats.reset();
        id.set(_id);
    }
};

class SimpleContextLogger : public ContextLogger
{
    unsigned instanceId;
public:
    SimpleContextLogger(unsigned _instanceId) : instanceId(_instanceId)
    {
    }
    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
    {
        return ret.append(instanceId);  
    }
};

class SlaveContextLogger : public StringContextLogger
{
    mutable Owned<IMessagePacker> output;
    mutable bool anyOutput;
    bool traceActivityTimes;
    bool debuggerActive;
    IpAddress ip;
    StringAttr wuid;
public:
    SlaveContextLogger();
    SlaveContextLogger(IRoxieQueryPacket *packet);
    void set(IRoxieQueryPacket *packet);
    virtual void flush(bool closing, bool aborted) const;
    inline bool queryTraceActivityTimes() const { return traceActivityTimes; }
    inline bool queryDebuggerActive() const { return debuggerActive; }
    inline void setDebuggerActive(bool _active) { debuggerActive = _active; }
    inline const StatsCollector &queryStats() const 
    {
        return stats;
    }
    inline void requestAbort()
    {
        stats.requestAbort();
    }
    inline const char *queryWuid()
    {
        return wuid.get();
    }
};
#endif
