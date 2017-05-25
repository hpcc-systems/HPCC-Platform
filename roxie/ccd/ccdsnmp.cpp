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

#include <algorithm>
#include "jlog.hpp"
#include "jtime.hpp"
#include "jptree.hpp"
#include "jqueue.tpp"
#include "mpbase.hpp"
#include "math.h"
#include "ccdsnmp.hpp"
#include "jhtree.hpp"
#include "thirdparty.h"
#include "roxiemem.hpp"
#include "udplib.hpp"

#define DEFAULT_PULSE_INTERVAL 30

atomic_t queryCount;
RoxieQueryStats unknownQueryStats;
RoxieQueryStats loQueryStats;
RoxieQueryStats hiQueryStats;
RoxieQueryStats slaQueryStats;
RoxieQueryStats combinedQueryStats;
atomic_t retriesIgnoredPrm;
atomic_t retriesIgnoredSec;
atomic_t retriesNeeded;
atomic_t retriesReceivedPrm;
atomic_t retriesReceivedSec;
atomic_t retriesSent;
atomic_t rowsIn;
atomic_t ibytiPacketsFromSelf;
atomic_t ibytiPacketsSent;
atomic_t ibytiPacketsWorked;
atomic_t ibytiPacketsHalfWorked;
atomic_t ibytiPacketsReceived;
atomic_t ibytiPacketsTooLate;
atomic_t ibytiNoDelaysPrm;
atomic_t ibytiNoDelaysSec;
atomic_t packetsSent;
atomic_t packetsReceived;
atomic_t resultsReceived;
atomic_t indexRecordsRead;
atomic_t postFiltered;
atomic_t abortsSent;
atomic_t activitiesStarted;
atomic_t activitiesCompleted;
atomic_t diskReadStarted;
atomic_t diskReadCompleted;
atomic_t globalSignals;
atomic_t globalLocks;
atomic_t numFilesToProcess;

CriticalSection counterCrit;
unsigned queueLength;
unsigned maxQueueLength;
unsigned rowsOut;
unsigned maxScanLength;
unsigned totScanLength;
unsigned totScans;
unsigned meanScanLength;

#ifdef TIME_PACKETS
unsigned __int64 packetWaitElapsed; 
unsigned packetWaitMax;
atomic_t packetWaitCount;
unsigned __int64 packetRunElapsed; 
unsigned packetRunMax;
atomic_t packetRunCount;
#endif

unsigned lastQueryDate = 0;
unsigned lastQueryTime = 0;
unsigned slavesActive = 0;
unsigned maxSlavesActive = 0;

#define addMetric(a, b) doAddMetric(a, #a, b)

interface INamedMetric : extends IInterface
{
    virtual long getValue() = 0;
    virtual bool isCumulative() = 0;
    virtual void resetValue() = 0;
};

interface ITimerCallback : extends IInterface
{
    virtual void onTimer() = 0;
};

class AtomicMetric : implements INamedMetric, public CInterface
{
    atomic_t &counter;
    bool cumulative;
public:
    IMPLEMENT_IINTERFACE;
    AtomicMetric(atomic_t &_counter, bool _cumulative)
    : counter(_counter), cumulative(_cumulative)
    {
    }
    virtual long getValue() 
    {
        return atomic_read(&counter);
    }
    virtual bool isCumulative()
    {
        return cumulative;
    }
    virtual void resetValue()
    {
        if (cumulative)
            atomic_set(&counter, 0);
    }
};

class RelaxedAtomicMetric : implements INamedMetric, public CInterface
{
    RelaxedAtomic<unsigned> &counter;
    const bool cumulative;
public:
    IMPLEMENT_IINTERFACE;
    RelaxedAtomicMetric(RelaxedAtomic<unsigned> &_counter, bool _cumulative)
    : counter(_counter), cumulative(_cumulative)
    {
    }
    virtual long getValue()
    {
        return counter.load();
    }
    virtual bool isCumulative()
    {
        return cumulative;
    }
    virtual void resetValue()
    {
        if (cumulative)
            counter.store(0);
    }
};

class CounterMetric : implements INamedMetric, public CInterface
{
protected:
    unsigned &counter;
    bool cumulative;
public:
    IMPLEMENT_IINTERFACE;
    CounterMetric(unsigned &_counter, bool _cumulative)
    : counter(_counter), cumulative(_cumulative)
    {
    }
    virtual long getValue() 
    {
        CriticalBlock c(counterCrit);
        return counter;
    }
    virtual bool isCumulative()
    {
        return cumulative;
    }
    virtual void resetValue()
    {
        if (cumulative)
        {
            CriticalBlock c(counterCrit);
            counter = 0;
        }
    }
};

typedef unsigned (*AccessorFunction)();

class FunctionMetric : implements INamedMetric, public CInterface
{
    AccessorFunction accessor;
public:
    IMPLEMENT_IINTERFACE;
    FunctionMetric(AccessorFunction _accessor)
    : accessor(_accessor)
    {
    }
    virtual long getValue()
    {
        return accessor();
    }
    virtual bool isCumulative() { return false; }
    virtual void resetValue() { }

};

class UserMetric : implements INamedMetric, public CInterface
{
protected:
    Owned <IUserMetric> metric;

public:
    IMPLEMENT_IINTERFACE;
    UserMetric(const char *name, const char *regex) 
    {
        metric.setown(createUserMetric(name, regex));
    }
    virtual long getValue() 
    {
        return (long) metric->queryCount();
    }
    virtual bool isCumulative() { return true; }
    virtual void resetValue()
    {
        metric->reset();
    }

};

class TickProvider : public Thread
{
    IArrayOf<ITimerCallback> listeners;
    CriticalSection crit;
    Semaphore stopped;

    void doTicks()
    {
        CriticalBlock c(crit);
        ForEachItemIn(idx, listeners)
        {
            listeners.item(idx).onTimer();
        }
    }

public:
    TickProvider() : Thread("TickProvider") 
    {
    }
    int run()
    {
        for (;;)
        {
            if (stopped.wait(10000))
                break;
            doTicks();
        }
        return 0;
    }

    void addListener(ITimerCallback *l)
    {
        listeners.append(*LINK(l));
    }

    void stop()
    {
        stopped.signal();
        join();
    }
};

class IntervalMetric : implements INamedMetric, implements ITimerCallback, public CInterface
{
    Linked<INamedMetric> base;
    CriticalSection crit;
    unsigned lastSnapshotTime;
    long lastSnapshotValue;
    unsigned minInterval;
    long value;

    void takeSnapshot()
    {
        CriticalBlock c(crit);
        unsigned now = msTick();
        unsigned period = now - lastSnapshotTime;
        if (period >= minInterval)
        {
            long newValue = base->getValue();
            value = ((newValue - lastSnapshotValue) * 1000) / period;
            lastSnapshotTime = now;
            lastSnapshotValue = newValue;
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    IntervalMetric(INamedMetric *_base, unsigned _minInterval=1000) : base(_base), minInterval(_minInterval)
    {
        lastSnapshotTime = msTick();
        lastSnapshotValue = 0;
        value = 0;
    }
    virtual void onTimer()
    {
        takeSnapshot();
    }
    virtual long getValue() 
    {
        takeSnapshot();
        return value;
    }
    virtual bool isCumulative() { return false; }

    virtual void resetValue()
    {
        CriticalBlock c(crit);
        lastSnapshotTime = msTick();
        lastSnapshotValue = 0;
        value = 0;
    }

};

class RatioMetric : implements INamedMetric, public CInterface
{
    atomic_t &counter;
    unsigned __int64 &elapsed;
public:
    IMPLEMENT_IINTERFACE;
    RatioMetric(atomic_t &_counter, unsigned __int64 &_elapsed) : counter(_counter) , elapsed(_elapsed) {}
    virtual long getValue() 
    {
        CriticalBlock c(counterCrit);
        unsigned count = atomic_read(&counter);
        if (count)
            return (unsigned) (elapsed / count);
        else
            return 0;
    }
    virtual bool isCumulative() { return true; }

    virtual void resetValue()
    {
        CriticalBlock c(counterCrit);
        atomic_set(&counter, 0);
    }

};

class UnsignedRatioMetric : implements INamedMetric, public CInterface
{
    unsigned &counter;
    unsigned __int64 &elapsed;
public:
    IMPLEMENT_IINTERFACE;
    UnsignedRatioMetric(unsigned &_counter, unsigned __int64 &_elapsed) : counter(_counter) , elapsed(_elapsed) {}
    virtual long getValue() 
    {
        CriticalBlock c(counterCrit);
        if (counter)
            return (unsigned) (elapsed / counter);
        else
            return 0;
    }
    virtual bool isCumulative() { return true; }

    virtual void resetValue()
    {
        CriticalBlock c(counterCrit);
        counter = 0;
    }

};

class CRoxieMetricsManager : implements IRoxieMetricsManager, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CRoxieMetricsManager();
    ~CRoxieMetricsManager();

    virtual long getValue(const char * name);
    void dumpMetrics();
    StringBuffer &getMetrics(StringBuffer &xml);
    void resetMetrics();

    void doAddMetric(atomic_t &counter, const char *name, unsigned interval);
    void doAddMetric(RelaxedAtomic<unsigned> &counter, const char *name, unsigned interval);
    void doAddMetric(unsigned &counter, const char *name, unsigned interval);
    void doAddMetric(INamedMetric *n, const char *name, unsigned interval);
    void doAddMetric(AccessorFunction function, const char *name, unsigned interval);
    void addRatioMetric(atomic_t &counter, const char *name, unsigned __int64 &elapsed);
    void addRatioMetric(unsigned &counter, const char *name, unsigned __int64 &elapsed);
    void addUserMetric(const char *name, const char *regex);

private:
    MapStringToMyClassViaBase<INamedMetric, INamedMetric> metricMap;
    bool started;

    TickProvider ticker;
};

void RoxieQueryStats::addMetrics(CRoxieMetricsManager *snmpManager, const char *prefix, unsigned interval)
{
    StringBuffer name;
    snmpManager->doAddMetric(count, name.clear().append(prefix).append("QueryCount"), interval);
    snmpManager->doAddMetric(failedCount, name.clear().append(prefix).append("QueryFailed"), interval);
    snmpManager->doAddMetric(active, name.clear().append(prefix).append("QueryActive"), 0);
    snmpManager->doAddMetric(maxTime, name.clear().append(prefix).append("QueryMaxTime"), 0);
    snmpManager->doAddMetric(minTime, name.clear().append(prefix).append("QueryMinTime"), 0);
    snmpManager->addRatioMetric(count, name.clear().append(prefix).append("QueryAverageTime"), totalTime);
}

using roxiemem::getHeapAllocated;
using roxiemem::getHeapPercentAllocated;
using roxiemem::getDataBufferPages;
using roxiemem::getDataBuffersActive;

CRoxieMetricsManager::CRoxieMetricsManager()
{
    started = false;
    addMetric(maxQueueLength, 0);
    addMetric(queryCount, 1000);
    unknownQueryStats.addMetrics(this, "unknown", 1000);
    loQueryStats.addMetrics(this, "lo", 1000);
    hiQueryStats.addMetrics(this, "hi", 1000);
    slaQueryStats.addMetrics(this, "sla", 1000);
    combinedQueryStats.addMetrics(this, "all", 1000);
    addMetric(retriesIgnoredPrm, 1000);
    addMetric(retriesIgnoredSec, 1000);
    addMetric(retriesNeeded, 1000);
    addMetric(retriesReceivedPrm, 1000);
    addMetric(retriesReceivedSec, 1000);
    addMetric(retriesSent, 1000);
    addMetric(rowsIn, 1000);
    addMetric(rowsOut, 1000);
    addMetric(ibytiPacketsFromSelf, 1000);
    addMetric(ibytiPacketsSent, 1000);
    addMetric(ibytiPacketsWorked, 1000);
    addMetric(ibytiPacketsHalfWorked, 1000);
    addMetric(ibytiPacketsReceived, 1000);
    addMetric(ibytiPacketsTooLate, 1000);
#ifndef NO_IBYTI_DELAYS_COUNT
    addMetric(ibytiNoDelaysPrm, 1000);
    addMetric(ibytiNoDelaysSec, 1000);
#endif
    addMetric(packetsReceived, 1000);
    addMetric(packetsSent, 1000);
    addMetric(resultsReceived, 1000);
    addMetric(slavesActive, 0);
    addMetric(maxSlavesActive, 0);
    addMetric(indexRecordsRead, 1000);
    addMetric(postFiltered, 1000);
    addMetric(abortsSent, 0);
    addMetric(activitiesStarted, 1000);
    addMetric(activitiesCompleted, 1000);
    addMetric(diskReadStarted, 0);
    addMetric(diskReadCompleted, 0);
    addMetric(globalSignals, 0);
    addMetric(globalLocks, 0);
    addMetric(restarts, 0);

    addMetric(nodesLoaded, 1000);
    addMetric(cacheHits, 1000);
    addMetric(cacheAdds, 1000);
    addMetric(leafCacheHits, 1000);
    addMetric(leafCacheAdds, 1000);
    addMetric(nodeCacheHits, 1000);
    addMetric(nodeCacheAdds, 1000);
    addMetric(preloadCacheHits, 0);
    addMetric(preloadCacheAdds, 0);
    addMetric(unwantedDiscarded, 1000);
    addMetric(packetsRetried, 1000);
    addMetric(packetsAbandoned, 1000);

    addMetric(getHeapAllocated, 0);
    addMetric(getHeapPercentAllocated, 0);
    addMetric(getDataBufferPages, 0);
    addMetric(getDataBuffersActive, 0);
    
    addMetric(maxScanLength, 0);
    addMetric(totScanLength, 0);
    addMetric(totScans, 0);
    addMetric(meanScanLength, 0);
    addMetric(lastQueryDate, 0);
    addMetric(lastQueryTime, 0);

#ifdef TIME_PACKETS
    addMetric(packetWaitMax, 0);
    addMetric(packetRunMax, 0);
    addRatioMetric(packetRunCount, "packetRunAverage", packetRunElapsed);
    addRatioMetric(packetWaitCount, "packetWaitAverage", packetWaitElapsed);
#endif
    addMetric(numFilesToProcess, 0);
    ticker.start();
}

void CRoxieMetricsManager::doAddMetric(atomic_t &counter, const char *name, unsigned interval)
{
    doAddMetric(new AtomicMetric(counter, interval != 0), name, interval);
}

void CRoxieMetricsManager::doAddMetric(RelaxedAtomic<unsigned> &counter, const char *name, unsigned interval)
{
    doAddMetric(new RelaxedAtomicMetric(counter, interval != 0), name, interval);
}

void CRoxieMetricsManager::doAddMetric(unsigned &counter, const char *name, unsigned interval)
{
    doAddMetric(new CounterMetric(counter, interval != 0), name, interval);
}

void CRoxieMetricsManager::doAddMetric(INamedMetric *n, const char *name, unsigned interval)
{
    if (interval)
    {
        StringBuffer fname(name);
        fname.append("/s");
        IntervalMetric *im = new IntervalMetric(n, interval);
        ticker.addListener(im);
        metricMap.setValue(fname.str(), im);
        im->Release();
    }
    metricMap.setValue(name, n);
    n->Release();
}

void CRoxieMetricsManager::doAddMetric(AccessorFunction function, const char *name, unsigned interval)
{
    assertex(interval==0);
    doAddMetric(new FunctionMetric(function), name, interval);
}

void CRoxieMetricsManager::addRatioMetric(unsigned &counter, const char *name, unsigned __int64 &elapsed)
{
    doAddMetric(new UnsignedRatioMetric(counter, elapsed), name, 0);
}

void CRoxieMetricsManager::addUserMetric(const char *name, const char *regex)
{
    doAddMetric(new UserMetric(name, regex), name, 0);
}

long CRoxieMetricsManager::getValue(const char * name)
{
    long ret = 0;
    INamedMetric *m = metricMap.getValue(name);
    if (m)
        ret = m->getValue();
#ifdef _DEBUG
    DBGLOG("getValue(%s) returning %ld", name, ret);
#endif
    return ret;
}


CRoxieMetricsManager::~CRoxieMetricsManager()
{
    ticker.stop();
    if (started)
        dumpMetrics();
}

void CRoxieMetricsManager::dumpMetrics()
{
    HashIterator metrics(metricMap);
    ForEach(metrics)
    {           
        IMapping &cur = metrics.query();
        INamedMetric *m = (INamedMetric *) *metricMap.mapToValue(&cur);
        if (m->isCumulative())
        {
            const char *name = (const char *) cur.getKey();
            long val = m->getValue();
            DBGLOG("TOTALS: %s = %ld", name, val);
        }
    }
}

StringBuffer &CRoxieMetricsManager::getMetrics(StringBuffer &xml)
{
    xml.append("<Metrics>\n");
    HashIterator metrics(metricMap);
    ForEach(metrics)
    {           
        IMapping &cur = metrics.query();
        INamedMetric *m = (INamedMetric *) *metricMap.mapToValue(&cur);
        const char *name = (const char *) cur.getKey();
        long val = m->getValue();
        xml.appendf(" <Metric name=\"%s\" value=\"%ld\"/>\n", name, val);
    }
    xml.append("</Metrics>\n");
    return xml;
}

void CRoxieMetricsManager::resetMetrics()
{
    HashIterator metrics(metricMap);
    ForEach(metrics)
    {           
        IMapping &cur = metrics.query();
        INamedMetric *m = (INamedMetric *) *metricMap.mapToValue(&cur);
        m->resetValue();
    }
}

IRoxieMetricsManager *createRoxieMetricsManager()
{
    return new CRoxieMetricsManager();
}

Owned<IRoxieMetricsManager> roxieMetrics;

// Requirements for query stats: Want to be able to ask:
// 1.   Average response time for query since ....??last restart of roxie??(or 
//      maybe last 24 hours)
// 2.   Average response time of query in last hours
// 3.   Quantity of searches on each query since........??last restart of roxie?? 
//      (or maybe last 24 hours)
// 4.   Quantity of searches in last hour
// 5.   Breakout of number of searches for each query with certain standard deviations

// Implementation notes: In production usage at present there may be up to 100000 queries on a given node per day, 
// - it is likely that this will increase and that roxie may be restarted less frequently if we start loading data 
// on-the-fly.
// We probably need a mechanism of concentric ring buffers to ensure that we don't end up using excessive memory
// OR we could log the info to disk (roll every hour?) (so restarts would not destroy) and scan it when asked... can cache results if we want.
// The info is already in the log file...
// A separate process tailing the log file?

// If we do the internal concentric buffers route:
// If hour (or minute, or minute/5, or whatever minimum granularity we pick) has changed since last noted a query,
// shuffle...
// we will be able to give results for any hour (x:00 to x+1:00), day (midnight to midnight), 5 minute period, etc
// as well as current hour-so-far, current day-so-far 
// and also last 5 minutes, last 60 minutes.
// OR - just use a fixed size circular buffer and discard info once full. Anyone needing to know aggregated info 
// about more than the last (say) 64k queries will have to start grepping logs.
// Probably better to use expanding array with max size as may queries will not get anywhere near.
// A binchop to find start-time/end-time ...
// OR each time we fill up we aggregate all records over 2 hours old into a hour-based array. 8760 elements if stay up for a year...
// Anything asking about a time period that starts > 2 hours ago will give approximate answer (and indicate that it did so).
// We can arrange so that all info < 2 hours old is in one circular buffer, all info > 2 hours old is aggregated into hourly slices
// The last of the hourly slices is then incomplete. OR is it easier to aggregate as we go and discard data > 1 hour old?
// Probably.
// If we have per-hour aggregate data available indefinitely, and last-hour data available in full, we should be ok

// If we want to be able to see other stats with same detail as time, we will need to abstract this out a bit more.

#define NUM_BUCKETS 16 //<32,<64,<125,<250,<500,<1s,<2,<4,<8,<16,<32,<64s,<128s,<256s,more
#define SLOT_LENGTH 3600 // in seconds. Granularity of aggregation

class CQueryStatsAggregator : public CInterface, implements IQueryStatsAggregator
{
    class QueryStatsRecord : public CInterface
    {
        // one of these per query in last hour...
    private:
        time_t startTime; // more interesting than end-time
        unsigned elapsedTimeMs;
        unsigned memUsed;
        unsigned slavesReplyLen;
        unsigned bytesOut;
        bool failed;

    public:
        QueryStatsRecord(time_t _startTime, bool _failed, unsigned _elapsedTimeMs, unsigned _memused, unsigned _slavesReplyLen, unsigned _bytesOut)
        {
            startTime = _startTime;
            failed = _failed;
            elapsedTimeMs = _elapsedTimeMs;
            memUsed = _memused;
            slavesReplyLen = _slavesReplyLen;
            bytesOut = _bytesOut;
        }

        bool expired(time_t timeNow, unsigned expirySeconds)
        {
            return difftime(timeNow, startTime) > expirySeconds;
        }

        bool inRange(time_t from, time_t to)
        {
            return difftime(startTime, from) >= 0 && difftime(to, startTime) > 0;
        }

        inline bool isOlderThan(time_t otherTime)
        {
            return difftime(startTime, otherTime) < 0;
        }

        static int compareTime(CInterface * const *_l, CInterface* const *_r)
        {
            QueryStatsRecord *l = (QueryStatsRecord *)*_l;
            QueryStatsRecord *r = (QueryStatsRecord *)*_r;
            return l->elapsedTimeMs - r->elapsedTimeMs;
        }

        static void getStats(IPropertyTree &result, CIArrayOf<QueryStatsRecord> &useStats, time_t from, time_t to)
        {
            QueryStatsAggregateRecord aggregator(from, to);
            ForEachItemIn(idx, useStats)
            {
                QueryStatsRecord &r = useStats.item(idx); 
                aggregator.noteQuery(r.failed, r.elapsedTimeMs, r.memUsed, r.slavesReplyLen, r.bytesOut);
            }
            aggregator.getStats(result, false);

            // Add in the exact percentiles
            if (useStats.length())
            {
                unsigned percentile97Pos = (useStats.length() * 97) / 100;
                useStats.sort(QueryStatsRecord::compareTime);
                result.setPropInt("percentile97", useStats.item(percentile97Pos).elapsedTimeMs);
            }
            else
                result.setPropInt("percentile97", 0);
        }

        static bool checkOlder(const void *_left, const void *_right)
        {
            QueryStatsRecord *left = (QueryStatsRecord *) _left;
            QueryStatsRecord *right = (QueryStatsRecord *) _right;
            return left->isOlderThan(right->startTime);
        }

    };
    
    class QueryStatsAggregateRecord : public CInterface
    {
        // one of these per hour...
    private:

        unsigned countTotal;
        unsigned countFailed;
        unsigned __int64 totalTimeMs;
        unsigned __int64 totalTimeMsSquared;
        unsigned __int64 totalMemUsed;
        unsigned __int64 totalSlavesReplyLen;
        unsigned __int64 totalBytesOut;
        unsigned maxTimeMs;
        unsigned minTimeMs;
        time_t startTime;
        time_t endTime;
        unsigned buckets[NUM_BUCKETS];

    public:
        QueryStatsAggregateRecord(time_t _startTime, time_t _endTime)
        {
            startTime = _startTime;
            endTime = _endTime;
            countTotal = 0;
            countFailed = 0;
            totalTimeMs = 0;
            totalTimeMsSquared = 0;
            totalMemUsed = 0;
            totalSlavesReplyLen = 0;
            totalBytesOut = 0;
            maxTimeMs = 0;
            minTimeMs = 0;
            memset(buckets, 0, sizeof(buckets));
        }

        bool inRange(time_t from, time_t to)
        {
            return (difftime(startTime, from) >= 0 && difftime(to, endTime) > 0);
        }

        bool matches(time_t queryTime)
        {
            return (difftime(queryTime, startTime) >= 0 && difftime(queryTime, endTime) < 0);
        }

        bool older(time_t queryTime)
        {
            return difftime(queryTime, endTime) >= 0;
        }

        void mergeStats(QueryStatsAggregateRecord &other)
        {
            // NOTE - we could (if we understood stats) try to interpolate when requested time ranges do not include
            // the whole of the block being merged. But I think it's better and easier to return stats for the full time periods
            // and return indication of what the time period actually being reported is.
            startTime = std::min(startTime, other.startTime);
            endTime = std::max(endTime, other.endTime);
            totalTimeMs += other.totalTimeMs;
            totalTimeMsSquared += other.totalTimeMsSquared;
            totalMemUsed += other.totalMemUsed;
            totalSlavesReplyLen += other.totalSlavesReplyLen;
            totalBytesOut += other.totalBytesOut;
            maxTimeMs = std::max(maxTimeMs, other.maxTimeMs);
            if (other.countTotal)
                minTimeMs = countTotal ? std::min(minTimeMs, other.minTimeMs) : other.minTimeMs;
            // NOTE - update coutTotal AFTER minTimeMs or the check for zero is wrong.
            countTotal += other.countTotal;
            countFailed += other.countFailed;
            for (unsigned bucketIndex = 0; bucketIndex < NUM_BUCKETS; bucketIndex++)
            {
                buckets[bucketIndex] += other.buckets[bucketIndex];
            }
            
        }

        void noteQuery(bool failed, unsigned elapsedTimeMs, unsigned memUsed, unsigned slavesReplyLen, unsigned bytesOut)
        {
            totalTimeMs += elapsedTimeMs;
            unsigned __int64 timeSquared = elapsedTimeMs;
            timeSquared *= timeSquared;
            totalTimeMsSquared += timeSquared;
            totalMemUsed += memUsed;
            totalSlavesReplyLen += slavesReplyLen;
            totalBytesOut += bytesOut;
            if (elapsedTimeMs > maxTimeMs)
                maxTimeMs = elapsedTimeMs;
            if (countTotal==0 || elapsedTimeMs < minTimeMs)
                minTimeMs = elapsedTimeMs;
            unsigned bucketIdx;
            if (elapsedTimeMs <= 32)
                bucketIdx = 0;
            else if (elapsedTimeMs <= 64)
                bucketIdx = 1;
            else
            {
                bucketIdx = 2;
                unsigned bucketMax = 125;
                while (elapsedTimeMs > bucketMax)
                {
                    bucketIdx++;
                    if (bucketIdx == NUM_BUCKETS-1)
                        break;
                    bucketMax *= 2;
                }
            }
            buckets[bucketIdx]++;
            countTotal++;
            if (failed)
                countFailed++;
        }

        void getStats(IPropertyTree &result, bool estimatePercentiles)
        {
            CDateTime dt;
            StringBuffer s;
            dt.set(startTime);
            result.setProp("startTime", dt.getString(s.clear(), true).str());
            dt.set(endTime);
            result.setProp("endTime", dt.getString(s.clear(), true).str());
            result.setPropInt64("countTotal", countTotal);
            result.setPropInt64("countFailed", countFailed);
            result.setPropInt64("averageTimeMs", countTotal ? totalTimeMs/countTotal : 0);
            result.setPropInt64("averageMemUsed", countTotal ? totalMemUsed/countTotal : 0);
            result.setPropInt64("averageSlavesReplyLen", countTotal ? totalSlavesReplyLen/countTotal : 0);
            result.setPropInt64("averageBytesOut", countTotal ? totalBytesOut/countTotal : 0);
            // MORE - do something funky and statistical using totalTimeMsSquared
            result.setPropInt("maxTimeMs", maxTimeMs);
            result.setPropInt("minTimeMs", minTimeMs);
            if (estimatePercentiles)
            {
                // We can tell which bucket the 97th percentile is in...
                unsigned percentile97 = (unsigned) (((countTotal * 97.0) / 100.0)+0.5);
                unsigned belowMe = 0;
                unsigned bucketLimit = 32;
                for (unsigned bucketIndex = 0; bucketIndex < NUM_BUCKETS; bucketIndex++)
                {
                    belowMe += buckets[bucketIndex];
                    if (belowMe >= percentile97)
                    {
                        if (bucketLimit > maxTimeMs)
                            bucketLimit = maxTimeMs; 
                        result.setPropInt("percentile97", bucketLimit);
                        result.setPropBool("percentile97/@estimate", true);
                        break;
                    }
                    if (bucketLimit == 64)
                        bucketLimit = 125;
                    else
                        bucketLimit += bucketLimit;
                }
            }
        }
    };

    QueueOf<QueryStatsRecord, false> recent;
    CIArrayOf<QueryStatsAggregateRecord> aggregated; // stored with most recent first
    unsigned expirySeconds;  // time to keep exact info (rather than just aggregated)
    StringAttr queryName;
    SpinLock lock; // MORE: This could be held this for a while.  Is this significant?  Should it be a CriticalSection?

    QueryStatsAggregateRecord &findAggregate(time_t startTime)
    {
        unsigned idx = 0;
        while (aggregated.isItem(idx))
        {
            QueryStatsAggregateRecord &thisSlot = aggregated.item(idx);
            if (thisSlot.matches(startTime))
                return thisSlot; // This is the most common case!
            else if (thisSlot.older(startTime))
                break;
            idx++;
        }
        time_t slotStart;
        time_t slotEnd;
        calcSlotStartTime(startTime, SLOT_LENGTH, slotStart, slotEnd);
        QueryStatsAggregateRecord *newSlot = new QueryStatsAggregateRecord(slotStart, slotEnd);
        aggregated.add(*newSlot, idx);
        return *newSlot;
    }

    static void calcSlotStartTime(time_t queryTime, unsigned slotLengthSeconds, time_t &slotStart, time_t &slotEnd)
    {
        assertex (slotLengthSeconds == 3600); // Haven't written any code to support anything else yet!
        struct tm queryTimeExpanded;
        localtime_r(&queryTime, &queryTimeExpanded);
        queryTimeExpanded.tm_min = 0;
        queryTimeExpanded.tm_sec = 0;
        slotStart = mktime(&queryTimeExpanded);
        queryTimeExpanded.tm_sec = slotLengthSeconds;
        slotEnd = mktime(&queryTimeExpanded);
    }
    
    static CQueryStatsAggregator globalStatsAggregator;
    static SpinLock queryStatsCrit;

public:
    static CIArrayOf<CQueryStatsAggregator> queryStatsAggregators;
    virtual void Link(void) const { CInterface::Link(); }
    virtual bool Release(void) const    
    {
        if (CInterface::Release())
            return true;
        SpinBlock b(queryStatsCrit);
        if (!IsShared())
        {
            queryStatsAggregators.zap(* const_cast<CQueryStatsAggregator*>(this));
            return true;
        }
        return false;
    }

    CQueryStatsAggregator(const char *_queryName, unsigned _expirySeconds)
        : queryName(_queryName)
    {
        expirySeconds = _expirySeconds;

        SpinBlock b(queryStatsCrit); // protect the global list
        queryStatsAggregators.append(*LINK(this));
    }
    ~CQueryStatsAggregator()
    {
        while (recent.ordinality())
        {
            recent.dequeue()->Release();
        }
    }
    static IPropertyTree *getAllQueryStats(bool includeQueries, time_t from, time_t to)
    {
        Owned<IPTree> result = createPTree("QueryStats", ipt_fast);
        if (includeQueries)
        {
            SpinBlock b(queryStatsCrit);
            ForEachItemIn(idx, queryStatsAggregators)
            {
                CQueryStatsAggregator &thisQuery = queryStatsAggregators.item(idx);
                result->addPropTree("Query", thisQuery.getStats(from, to));
            }
        }
        result->addPropTree("Global", globalStatsAggregator.getStats(from, to));
        return result.getClear();
    }

    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsedTimeMs, unsigned memUsed, unsigned slavesReplyLen, unsigned bytesOut)
    {
        time_t timeNow;
        time(&timeNow);
        SpinBlock b(lock);
        if (expirySeconds)
        {
            QueryStatsRecord *statsRec = new QueryStatsRecord(startTime, failed, elapsedTimeMs, memUsed, slavesReplyLen, bytesOut);
            recent.enqueue(statsRec, QueryStatsRecord::checkOlder);
        }
        // Now remove any that have expired
        if (expirySeconds != (unsigned) -1)
        {
            while (recent.ordinality() && recent.head()->expired(timeNow, expirySeconds))
            {
                recent.dequeue()->Release();
            }
        }

        QueryStatsAggregateRecord &aggregator = findAggregate(startTime);
        aggregator.noteQuery(failed, elapsedTimeMs, memUsed, slavesReplyLen, bytesOut);
    }

    virtual IPropertyTree *getStats(time_t from, time_t to)
    {
        time_t timeNow;
        time(&timeNow);
        Owned<IPropertyTree> result = createPTree("Query", ipt_fast);
        result->setProp("@id", queryName);
        if (expirySeconds && difftime(timeNow, from) <= expirySeconds)
        {
            // we can calculate exactly
            CIArrayOf<QueryStatsRecord> useStats;
            {
                SpinBlock b(lock); // be careful not to take too much time in here! If it gets to take a while, we will have to rethink
                ForEachQueueItemIn(idx, recent)
                {
                    QueryStatsRecord *rec = recent.item(idx);
                    if (rec->inRange(from, to))
                    {
                        rec->Link();
                        useStats.append(*rec);
                    }
                }
                // Spinlock is released here, and we process the useStats array at our leisure...
            }
            QueryStatsRecord::getStats(*result, useStats, from, to);
        }
        else // use aggregate stats - result will be inexact
        {
            QueryStatsAggregateRecord aggregator(from, to);
            {
                SpinBlock b(lock);
                ForEachItemInRev(idx, aggregated)
                {
                    QueryStatsAggregateRecord &thisSlot = aggregated.item(idx);
                    if (thisSlot.inRange(from, to))
                        aggregator.mergeStats(thisSlot);
                    else if (thisSlot.older(from))
                        break;
                }
                // Spinlock is released here, and we process the aggregator at our leisure...
            }
            aggregator.getStats(*result, true);
        }
        return result.getClear();
    }
    static inline IQueryStatsAggregator *queryGlobalStatsAggregator()
    {
        return &globalStatsAggregator;
    }
};

CIArrayOf<CQueryStatsAggregator> CQueryStatsAggregator::queryStatsAggregators;
CQueryStatsAggregator CQueryStatsAggregator::globalStatsAggregator(NULL, SLOT_LENGTH);
SpinLock CQueryStatsAggregator::queryStatsCrit; //MORE: Should probably be a critical section

IQueryStatsAggregator *queryGlobalQueryStatsAggregator()
{
    return CQueryStatsAggregator::queryGlobalStatsAggregator();
}

IQueryStatsAggregator *createQueryStatsAggregator(const char *_queryName, unsigned _expirySeconds)
{
    return new CQueryStatsAggregator(_queryName, _expirySeconds);
}

IPropertyTree *getAllQueryStats(bool includeQueries, time_t from, time_t to)
{
     return CQueryStatsAggregator::getAllQueryStats(includeQueries, from, to);
}

//=======================================================================================================

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

class StatsAggregatorTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( StatsAggregatorTest );
        CPPUNIT_TEST(test1);
    CPPUNIT_TEST_SUITE_END();

protected:
    void test1()
    {
        struct tm tmStruct;
        tmStruct.tm_isdst = 0;
        tmStruct.tm_hour = 12;
        tmStruct.tm_min = 34;
        tmStruct.tm_sec = 56;
        tmStruct.tm_mday = 14;
        tmStruct.tm_mon = 3;
        tmStruct.tm_year = 2005-1900;
        Owned<IQueryStatsAggregator> s = createQueryStatsAggregator("TestQuery", 0);
        // MORE - scope for much more testing here...
        for (unsigned i = 0; i < 100; i++)
        {
            s->noteQuery(mktime(&tmStruct), false, i, 8000, 10000, 55);
            tmStruct.tm_sec++;
        }
        tmStruct.tm_hour = 11;
        s->noteQuery(mktime(&tmStruct), false, 80000, 4000, 5000, 66);

        tmStruct.tm_hour = 0; 
        tmStruct.tm_min = 0;
        tmStruct.tm_sec = 0;
        time_t start = mktime(&tmStruct);
        tmStruct.tm_hour = 24; 
        time_t end = mktime(&tmStruct);
        {
            Owned<IPropertyTree> p = s->getStats(start, end);
            StringBuffer stats; 
            toXML(p, stats);
            DBGLOG("%s", stats.str());
        }
        {
            Owned<IPropertyTree> p = getAllQueryStats(true, start, end);
            StringBuffer stats; 
            toXML(p, stats);
            DBGLOG("%s", stats.str());
        }
        s.clear();
    }
};


CPPUNIT_TEST_SUITE_REGISTRATION( StatsAggregatorTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( StatsAggregatorTest, "StatsAggregatorTest" );

#endif
