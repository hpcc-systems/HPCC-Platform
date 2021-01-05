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
#include "jset.hpp"
#include "roxie.hpp"
#include "roxiedebug.ipp"
#include "eclrtl.hpp"
#include "rtlformat.hpp"
#include "rtldynfield.hpp"
#include "workunit.hpp"

#ifdef CCD_EXPORTS
#define CCD_API DECL_EXPORT
#else
#define CCD_API DECL_IMPORT
#endif

#define PARALLEL_EXECUTE
#define PARALLEL_PERSISTANT_THREADS

#define MAXTRACELEVEL 100     // don't want traceLevel+1 to wrap to 0 in lsb
#define MAX_CLUSTER_SIZE 1024
#define UDP_QUEUE_SIZE 100
#define UDP_SEND_QUEUE_SIZE 50

#define ROXIE_STATEFILE_VERSION 2

// Have not yet tested impact of new IBYTI handling in non-containerized systems

#ifdef _CONTAINERIZED
#define NEW_IBYTI
#endif

#if defined(_CONTAINERIZED) || defined (NEW_IBYTI)
// Both containerized mode and new IBYTI mode assume subchannels are passed in header.
// It SHOULD also work, and may be beneficial, in non-containerized systems but has not as yet been confirmed.
#define SUBCHANNELS_IN_HEADER
#endif

extern IException *MakeRoxieException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
void openMulticastSocket();

void setMulticastEndpoints(unsigned numChannels);


#define OUTOFBAND_SEQUENCE    0x8000        // indicates an out-of-band reply
#define OVERFLOWSEQUENCE_MAX 0x7fffu        // Max value before we want to wrap (to avoid collision with flag)
#define CONTINUE_SEQUENCE_SKIPTO  0x8000    // flag in continueSequence field indicating presence of skipTo data
#define CONTINUESEQUENCE_MAX 0x7fffu        // Max value before we want to wrap (to avoid collision with flag)

#define ROXIE_SLA_PRIORITY 0x40000000    // mask in activityId indicating it goes SLA priority queue
#define ROXIE_HIGH_PRIORITY 0x80000000   // mask in activityId indicating it goes on the fast queue
#define ROXIE_LOW_PRIORITY 0x00000000    // mask in activityId indicating it goes on the slow queue (= default)
#define ROXIE_PRIORITY_MASK (ROXIE_SLA_PRIORITY | ROXIE_HIGH_PRIORITY | ROXIE_LOW_PRIORITY)

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
#define MAX_SUBCHANNEL  7    // (16-2) / SUBCHANNEL_BITS

//#define TIME_PACKETS

#define ROXIE_FASTLANE      0x8000u         // mask in retries indicating agent reply goes on the fast queue
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

#define ROXIE_DALI_CONNECT_TIMEOUT 30000
#define ABORT_POLL_PERIOD 5000

class RemoteActivityId
{
public:
    hash64_t queryHash;
    unsigned activityId;

    inline bool isHighPriority() const { return (activityId & ROXIE_PRIORITY_MASK) == ROXIE_HIGH_PRIORITY; }
    inline bool isSLAPriority() const { return (activityId & ROXIE_PRIORITY_MASK) == ROXIE_SLA_PRIORITY; }

    inline RemoteActivityId(unsigned _activityId, hash64_t _queryHash)
        : queryHash(_queryHash), activityId(_activityId)
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

extern bool localAgent;

class RoxiePacketHeader
{
private:
    RoxiePacketHeader(const RoxiePacketHeader &source) =  delete;

public:
    unsigned packetlength = 0;
    unsigned short retries = 0;         // how many retries on this query, the high bits are used as flags, see above
    unsigned short overflowSequence = 0;// Used if more than one packet-worth of data from server - eg keyed join. We don't mind if we wrap...
    unsigned short continueSequence = 0;// Used if more than one chunk-worth of data from agent. We don't mind if we wrap
    unsigned short channel = 0;         // multicast family to send on
    unsigned activityId = 0;            // identifies the helper factory to be used (activityId in graph)
    hash64_t queryHash = 0;             // identifies the query

    ruid_t uid = 0;                     // unique id
    ServerIdentifier serverId;
#ifdef SUBCHANNELS_IN_HEADER
    ServerIdentifier subChannels[MAX_SUBCHANNEL];
#endif
#ifdef TIME_PACKETS
    unsigned tick = 0;
#endif
    RoxiePacketHeader() = default;

    RoxiePacketHeader(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence);
    RoxiePacketHeader(const RoxiePacketHeader &source, unsigned _activityId, unsigned _subChannel);

    static unsigned getSubChannelMask(unsigned subChannel);
    unsigned priorityHash() const;
    void copy(const RoxiePacketHeader &oh);
    bool matchPacket(const RoxiePacketHeader &oh) const;
    void init(const RemoteActivityId &_remoteId, ruid_t _uid, unsigned _channel, unsigned _overflowSequence);
    StringBuffer &toString(StringBuffer &ret) const;
    bool allChannelsFailed();
    bool retry();
    void setException(unsigned subChannel);
    unsigned thisChannelRetries(unsigned subChannel);

    unsigned getRespondingSubChannel() const // NOTE - 0 based
    {
        dbgassertex(retries);
        unsigned bitpos = countTrailingUnsetBits((unsigned) retries);
        return bitpos / SUBCHANNEL_BITS;
    }

#ifdef SUBCHANNELS_IN_HEADER
    unsigned mySubChannel() const // NOTE - 0 based
    {
        if (localAgent)
            return 0;
        for (unsigned idx = 0; idx < MAX_SUBCHANNEL; idx++)
        {
            if (subChannels[idx].isMe())
                return idx;
        }
        throwUnexpected();
    }

    bool hasBuddies() const
    {
        if (localAgent)
            return false;
        if (subChannels[1].isNull())
        {
            assert(subChannels[0].isMe());
            return false;
        }
        return true;
    }

    void clearSubChannels();
#endif

    inline unsigned getSequenceId() const
    {
        return (((unsigned) overflowSequence) << 16) | (unsigned) continueSequence;
    }

    inline void noteAlive(unsigned mask)
    {
        retries = (retries & ~mask);
    }

    inline void noteException(unsigned mask)
    {
        retries = (retries | mask);
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
    virtual IRoxieQueryPacket *insertSkipData(size32_t skipDataLen, const void *skipData) const = 0;
};

interface IQueryDll;

// Global configuration info
extern bool shuttingDown;
extern unsigned numChannels;
extern unsigned callbackRetries;
extern unsigned callbackTimeout;
extern unsigned lowTimeout;
extern unsigned highTimeout;
extern unsigned slaTimeout;
extern unsigned headRegionSize;
extern unsigned ccdMulticastPort;
extern IPropertyTree *topology;
extern MapStringTo<int> *preferredClusters;
extern StringArray allQuerySetNames;

extern bool alwaysTrustFormatCrcs;
extern bool allFilesDynamic;
extern bool lockSuperFiles;
extern bool logFullQueries;
extern bool blindLogging;
extern bool debugPermitted;
extern bool useRemoteResources;
extern bool checkFileDate;
extern bool lazyOpen;
extern bool useAeron;
extern bool ignoreOrphans;
extern bool doIbytiDelay;
extern bool copyResources;
extern bool chunkingHeap;
extern unsigned perChannelFlowLimit;
extern unsigned parallelLoopFlowLimit;
extern unsigned numServerThreads;
extern unsigned numRequestArrayThreads;
extern unsigned readTimeout;
extern unsigned indexReadChunkSize;
extern SocketEndpoint debugEndpoint;
extern unsigned maxBlockSize;
extern unsigned maxLockAttempts;
extern bool enableHeartBeat;
extern bool checkVersion;
extern unsigned memoryStatsInterval;
extern unsigned pingInterval;
extern unsigned socketCheckInterval;
extern memsize_t defaultMemoryLimit;
extern unsigned defaultTimeLimit[3];
extern unsigned defaultWarnTimeLimit[3];
extern unsigned defaultThorConnectTimeout;
extern bool pretendAllOpt;
extern ClientCertificate clientCert;
extern bool useHardLink;
extern unsigned maxFileAge[2];
extern unsigned minFilesOpen[2];
extern unsigned maxFilesOpen[2];
extern RelaxedAtomic<unsigned> restarts;
extern bool checkCompleted;
extern bool prestartAgentThreads;
extern unsigned preabortKeyedJoinsThreshold;
extern unsigned preabortIndexReadsThreshold;
extern bool traceStartStop;
extern bool traceRoxiePackets;
extern bool delaySubchannelPackets;
extern bool traceTranslations;
extern bool defaultTimeActivities;
extern bool defaultTraceEnabled;
extern unsigned IBYTIbufferSize;
extern unsigned IBYTIbufferLifetime;
extern unsigned defaultTraceLimit;
extern unsigned watchActivityId;
extern unsigned testAgentFailure;
extern unsigned dafilesrvLookupTimeout;
extern bool fastLaneQueue;
extern unsigned mtu_size;
extern StringBuffer fileNameServiceDali;
extern StringBuffer roxieName;
extern bool trapTooManyActiveQueries;
extern unsigned maxEmptyLoopIterations;
extern unsigned maxGraphLoopIterations;
extern HardwareInfo hdwInfo;
extern unsigned parallelAggregate;
extern bool inMemoryKeysEnabled;
extern unsigned __int64 minFreeDiskSpace;
extern bool probeAllRows;
extern bool steppingEnabled;
extern bool simpleLocalKeyedJoins;
extern bool enableKeyDiff;
extern PTreeReaderOptions defaultXmlReadFlags;
extern bool mergeAgentStatistics;
extern bool defaultNoSeekBuildIndex;
extern unsigned parallelLoadQueries;
extern bool adhocRoxie;
extern bool alwaysFailOnLeaks;

#ifdef _CONTAINERIZED
static constexpr bool roxieMulticastEnabled = false;
#else
extern bool roxieMulticastEnabled;   // enable use of multicast for sending requests to agents
#endif
extern bool preloadOnceData;
extern bool reloadRetriesFailed;
extern bool selfTestMode;
extern bool defaultCollectFactoryStatistics;
extern bool oneShotRoxie;

extern int backgroundCopyClass;
extern int backgroundCopyPrio;

extern unsigned roxiePort;     // If listening on multiple, this is the first. Used for lock cascading

extern unsigned udpMulticastBufferSize;
extern size32_t diskReadBufferSize;

extern bool nodeCachePreload;
extern unsigned nodeCacheMB;
extern unsigned leafCacheMB;
extern unsigned blobCacheMB;

extern Owned<IPerfMonHook> perfMonHook;

struct PartNoType
{
    unsigned short partNo;  // _n_of_400
    unsigned short fileNo;  // superkey file number
};

extern unsigned statsExpiryTime;
extern time_t startupTime;
extern unsigned miscDebugTraceLevel;
extern RecordTranslationMode fieldTranslationEnabled;

extern unsigned defaultParallelJoinPreload;
extern unsigned defaultConcatPreload;
extern unsigned defaultFetchPreload;
extern unsigned defaultFullKeyedJoinPreload;
extern unsigned defaultKeyedJoinPreload;
extern unsigned defaultPrefetchProjectPreload;
extern unsigned defaultStrandBlockSize;
extern unsigned defaultForceNumStrands;
extern unsigned defaultHeapFlags;

extern bool defaultCheckingHeap;
extern bool defaultDisableLocalOptimizations;

extern unsigned agentQueryReleaseDelaySeconds;
extern unsigned coresPerQuery;

extern unsigned cacheReportPeriodSeconds;

extern StringBuffer logDirectory;
extern StringBuffer pluginDirectory;
extern StringBuffer pluginsList;
extern StringBuffer queryDirectory;
extern StringBuffer codeDirectory;
extern StringBuffer tempDirectory;

#undef UNIMPLEMENTED
#undef throwUnexpected
extern void doUNIMPLEMENTED(unsigned line, const char *file);
#define UNIMPLEMENTED { doUNIMPLEMENTED(__LINE__, __FILE__); throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "UNIMPLEMENTED"); }
#define throwUnexpected()          throw MakeStringException(ROXIE_INTERNAL_ERROR, "Internal Error at %s(%d)", sanitizeSourceFile(__FILE__), __LINE__)

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
extern void saveTopology();

#define LOGGING_INTERCEPTED     0x01
#define LOGGING_TIMEACTIVITIES  0x02
#define LOGGING_DEBUGGERACTIVE  0x04
#define LOGGING_BLIND           0x08
#define LOGGING_TRACELEVELSET   0x10
#define LOGGING_CHECKINGHEAP    0x20
#define LOGGING_FLAGSPRESENT    0x40
#define LOGGING_WUID            0x80

class LogItem : public CInterface
{
    friend class AgentContextLogger;

    TracingCategory category;
    StringAttr prefix;
    StringAttr text;
    unsigned time;
    unsigned channel;

public:
    LogItem(TracingCategory _category, const char *_prefix, unsigned _time, unsigned _channel, const char *_text)
        : category(_category), prefix(_prefix), text(_text), time(_time), channel(_channel)
    {
    }

    LogItem(MemoryBuffer &buf)
    {
        char c; buf.read(c); category = (TracingCategory) c;
        buf.read(channel);
        buf.read(prefix);
        buf.read(text);
        buf.read(time);
    }

    void serialize(MemoryBuffer &buf)
    {
        buf.append((char) category);
        buf.append(channel);
        buf.append(prefix);
        buf.append(text);
        buf.append(time);
    }

    static const char *getCategoryString(TracingCategory c)
    {
        switch (c)
        {
        case LOG_TRACING: return "TRACE";
        case LOG_ERROR: return "ERROR";
        case LOG_STATVALUES: return "STATVALUES";
        case LOG_CHILDSTATS: return "CHILDSTATS";
        case LOG_CHILDCOUNT: return "CHILDCOUNT";
        default: return "UNKNOWN";
        }
    }

    void outputXML(IXmlStreamFlusher &out)
    {
        StringBuffer b;
        Owned<IXmlWriterExt> writer = createIXmlWriterExt(0, 0, NULL, WTStandard);
        writeXML(*writer);
        b.append(writer->str()); //flush can detach pointer
        out.flushXML(b, true);
    }

    void writeXML(IXmlWriter &writer)
    {
        writer.outputBeginNested("Log", true);
        writer.outputCString(getCategoryString(category), "Category");
        writer.outputUInt(channel, sizeof(channel), "Channel");
        StringBuffer s;
        s.append(time/1000).append('.').appendf("%03d", time % 1000);
        writer.outputCString(s, "Time");
        if (prefix)
            writer.outputCString(prefix, "Prefix");
        if (text)
            writer.outputCString(text, "Text");
        writer.outputEndNested("Log");
    }

};

// MORE - this code probably should be commoned up with some of the new stats code
extern void putStatsValue(IPropertyTree *node, const char *statName, const char *statType, unsigned __int64 val);
extern void putStatsValue(StringBuffer &reply, const char *statName, const char *statType, unsigned __int64 val);

class ContextLogger : implements IRoxieContextLogger, public CInterface
{
protected:
    mutable CriticalSection crit;
    mutable CriticalSection statsCrit;
    unsigned start;
    unsigned ctxTraceLevel;
    mutable CRuntimeStatisticCollection stats;
    unsigned channel;
public: // Not very clean but I don't care
    bool intercept;
    bool blind;
    mutable bool aborted;
    mutable CIArrayOf<LogItem> log;
private:
    StringAttr globalIdHeader = "HPCC-Global-Id";
    StringAttr callerIdHeader = "HPCC-Caller-Id";
    StringAttr globalId;
    StringAttr callerId;
    StringBuffer localId;
    ContextLogger(const ContextLogger &);  // Disable copy constructor
public:
    IMPLEMENT_IINTERFACE;

    ContextLogger() : stats(allStatistics)
    {
        ctxTraceLevel = traceLevel;
        intercept = false;
        blind = false;
        start = msTick();
        channel = 0;
        aborted = false;
        if ( topology && topology->hasProp("@httpGlobalIdHeader"))
            setHttpIdHeaders(topology->queryProp("@httpGlobalIdHeader"), topology->queryProp("@httpCallerIdHeader"));
    }

    void outputXML(IXmlStreamFlusher &out)
    {
        CriticalBlock b(crit);
        ForEachItemIn(idx, log)
        {
            log.item(idx).outputXML(out);
        }
    };

    void writeXML(IXmlWriter &writer)
    {
        CriticalBlock b(crit);
        writer.outputBeginArray("Log");
        ForEachItemIn(idx, log)
        {
            log.item(idx).writeXML(writer);
        }
        writer.outputEndArray("Log");
    };

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
        }
    }
    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const  __attribute__((format(printf,6,0)))
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
        }
    }
    virtual void CTXLOGl(LogItem *logItem) const
    {
        // NOTE - we don't actually print anything to logfile here - was already printed on agent
        CriticalBlock b(crit);
        log.append(*logItem);
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

    StringBuffer &getStats(StringBuffer &s) const
    {
        CriticalBlock block(statsCrit);
        return stats.toStr(s);
    }

    virtual bool isIntercepted() const
    {
        return intercept;
    }

    virtual bool isBlind() const
    {
        return blind;
    }

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const
    {
        if (aborted)
            throw MakeStringException(ROXIE_ABORT_ERROR, "Roxie server requested abort for running activity");
        stats.addStatisticAtomic(kind, value);
    }

    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
        CriticalBlock block(statsCrit);
        stats.merge(from);
    }
    virtual void gatherStats(CRuntimeStatisticCollection & merged) const override
    {
        merged.merge(stats);
    }

    virtual unsigned queryTraceLevel() const
    {
        return ctxTraceLevel;
    }
    void reset()
    {
        stats.reset();
    }
    virtual void setGlobalId(const char *id, SocketEndpoint &ep, unsigned pid) override
    {
        globalId.set(id);
        appendGloballyUniqueId(localId.clear());
    }
    virtual void setCallerId(const char *id) override
    {
        callerId.set(id);
    }
    virtual const char *queryGlobalId() const
    {
        return globalId.get();
    }
    virtual const char *queryCallerId() const override
    {
        return callerId.get();
    }
    virtual const char *queryLocalId() const
    {
        return localId.str();
    }
    virtual void setHttpIdHeaders(const char *global, const char *caller)
    {
        if (global && *global)
            globalIdHeader.set(global);
        if (caller && *caller)
            callerIdHeader.set(caller);
    }
    virtual const char *queryGlobalIdHttpHeader() const
    {
        return globalIdHeader.str();
    }
    virtual const char *queryCallerIdHttpHeader() const
    {
        return callerIdHeader.str();
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
        reset();
        id.set(_id);
    }
    virtual bool collectingDetailedStatistics() const { return false; }
};

class AgentContextLogger : public StringContextLogger
{
    Owned<IMessagePacker> output;
    mutable bool anyOutput; // messy
    bool debuggerActive;
    bool checkingHeap;
    IpAddress ip;
    StringAttr wuid;
public:
    AgentContextLogger();
    AgentContextLogger(IRoxieQueryPacket *packet);
    void set(IRoxieQueryPacket *packet);
    void putStatProcessed(unsigned subGraphId, unsigned actId, unsigned idx, unsigned processed, unsigned strands) const;
    void putStats(unsigned subGraphId, unsigned actId, const CRuntimeStatisticCollection &stats) const;
    void flush();
    inline bool queryDebuggerActive() const { return debuggerActive; }
    virtual void gatherStats(CRuntimeStatisticCollection & merged) const override
    {
        merged.merge(stats);
    }
    inline const char *queryWuid()
    {
        return wuid.get();
    }
    inline void abort()
    {
        aborted = true;
    }
};

extern int CCD_API roxie_main(int argc, const char *argv[], const char * defaultYaml);
#endif
