/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include <algorithm>
#include "jlog.hpp"
#include "jtime.hpp"
#include "jptree.hpp"
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

class AtomicMetric : public CInterface, implements INamedMetric
{
    atomic_t &counter;
public:
    IMPLEMENT_IINTERFACE;
    AtomicMetric(atomic_t &_counter) : counter(_counter) {}
    virtual long getValue() 
    {
        return atomic_read(&counter);
    }
    virtual bool isCumulative() { return true; }

    virtual void resetValue() { atomic_set(&counter, 0); }

};

class CounterMetric : public CInterface, implements INamedMetric
{
protected:
    unsigned &counter;
public:
    IMPLEMENT_IINTERFACE;
    CounterMetric(unsigned &_counter) : counter(_counter) {}
    virtual long getValue() 
    {
        CriticalBlock c(counterCrit);
        return counter;
    }
    virtual bool isCumulative() { return true; }

    virtual void resetValue()
    {
        CriticalBlock c(counterCrit);
        counter = 0;
    }

};

class UserMetric : public CInterface, implements INamedMetric
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
        loop
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

class IntervalMetric : public CInterface, implements INamedMetric, implements ITimerCallback
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

class RatioMetric : public CInterface, implements INamedMetric
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

class UnsignedRatioMetric : public CInterface, implements INamedMetric
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

class CRoxieMetricsManager : public CInterface, implements IRoxieMetricsManager
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
    void doAddMetric(unsigned &counter, const char *name, unsigned interval);
    void doAddMetric(INamedMetric *n, const char *name, unsigned interval);
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
    snmpManager->doAddMetric(active, name.clear().append(prefix).append("QueryActive"), interval);
    snmpManager->doAddMetric(maxTime, name.clear().append(prefix).append("Max"), interval);
    snmpManager->doAddMetric(minTime, name.clear().append(prefix).append("Min"), interval);
    snmpManager->addRatioMetric(count, name.clear().append(prefix).append("Average"), totalTime);
}
using roxiemem::dataBufferPages;
using roxiemem::dataBuffersActive;

CRoxieMetricsManager::CRoxieMetricsManager()
{
    started = false;
    addMetric(maxQueueLength, 0);
    addMetric(queryCount, 1000);
    unknownQueryStats.addMetrics(this, "unknown", 1000);
    loQueryStats.addMetrics(this, "lo", 1000);
    hiQueryStats.addMetrics(this, "hi", 1000);
    slaQueryStats.addMetrics(this, "sla", 1000);
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
    addMetric(activitiesStarted, 0);
    addMetric(activitiesCompleted, 0);
    addMetric(diskReadStarted, 0);
    addMetric(diskReadCompleted, 0);
    addMetric(globalSignals, 0);
    addMetric(globalLocks, 0);
    addMetric(restarts, 0);

    addMetric(nodesLoaded, 0);
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

    addMetric(dataBufferPages, 0);
    addMetric(dataBuffersActive, 0);
    
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
    doAddMetric(new AtomicMetric(counter), name, interval);
}

void CRoxieMetricsManager::doAddMetric(unsigned &counter, const char *name, unsigned interval)
{
    doAddMetric(new CounterMetric(counter), name, interval);
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

void CRoxieMetricsManager::addRatioMetric(atomic_t &counter, const char *name, unsigned __int64 &elapsed)
{
    doAddMetric(new RatioMetric(counter, elapsed), name, 0);
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
        if (m->isCumulative())
        {
            const char *name = (const char *) cur.getKey();
            m->resetValue();
        }
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
// If hour (or minute, or minute/5, or whatever minumum granularity we pick) has changed since last noted a query, 
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
// If we have per-hour aggregate data available indefinately, and last-hour data available in full, we should be ok

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

        static int compareTime(CInterface **_l, CInterface**_r)
        {
            QueryStatsRecord *l = *(QueryStatsRecord **) _l;
            QueryStatsRecord *r = *(QueryStatsRecord **) _r;
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
            assertex(endTime > startTime);
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

    CIArrayOf<QueryStatsRecord> recent;
    CIArrayOf<QueryStatsAggregateRecord> aggregated; // stored with most recent first
    unsigned expirySeconds;  // time to keep exact info (rather than just agregated)
    StringAttr queryName;
    SpinLock lock;

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
    
    static CIArrayOf<CQueryStatsAggregator> allStats;
    static SpinLock allStatsCrit;

public:
    virtual void Link(void) const { CInterface::Link(); }
    virtual bool Release(void) const    
    {
        if (CInterface::Release())
            return true;
        SpinBlock b(allStatsCrit);
        if (!IsShared())
        {
            allStats.zap(* const_cast<CQueryStatsAggregator*>(this));
            return true;
        }
        return false;
    }

    CQueryStatsAggregator(const char *_queryName, unsigned _expirySeconds)
        : queryName(_queryName)
    {
        SpinBlock b(allStatsCrit);
        expirySeconds = _expirySeconds;
        allStats.append(*LINK(this));
    }

    static IPropertyTree *getAllQueryStats(time_t from, time_t to)
    {
        Owned<IPTree> result = createPTree("QueryStats");
        SpinBlock b(allStatsCrit);
        ForEachItemIn(idx, allStats)
        {
            CQueryStatsAggregator &thisQuery = allStats.item(idx);
            result->addPropTree("Query", thisQuery.getStats(from, to));
        }
        return result.getClear();
    }

    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsedTimeMs, unsigned memUsed, unsigned slavesReplyLen, unsigned bytesOut)
    {
        time_t timeNow;
        time(&timeNow);
        QueryStatsRecord *statsRec = new QueryStatsRecord(startTime, failed, elapsedTimeMs, memUsed, slavesReplyLen, bytesOut);

        SpinBlock b(lock);
        if (expirySeconds)
        {
            unsigned pos = recent.length();
            while (pos && !recent.item(pos-1).isOlderThan(startTime))
                pos--;
            recent.add(*statsRec, pos);
        }

        // Now remove any that have expired
        // MORE - a circular buffer would be more efficient
        if (expirySeconds != (unsigned) -1)
        {
            while (recent.isItem(0) && recent.item(0).expired(timeNow, expirySeconds))
                recent.remove(0);
        }

        QueryStatsAggregateRecord &aggregator = findAggregate(startTime);
        aggregator.noteQuery(failed, elapsedTimeMs, memUsed, slavesReplyLen, bytesOut);
    }

    virtual IPropertyTree *getStats(time_t from, time_t to)
    {
        time_t timeNow;
        time(&timeNow);
        Owned<IPropertyTree> result = createPTree("Query");
        result->setProp("@id", queryName);
        if (expirySeconds && difftime(timeNow, from) <= expirySeconds)
        {
            // we can calculate exactly
            CIArrayOf<QueryStatsRecord> useStats;
            {
                SpinBlock b(lock); // be careful not to take too much time in here! If it gets to take a while, we will have to rethink
                ForEachItemIn(idx, recent)
                {
                    QueryStatsRecord &rec = recent.item(idx);
                    if (rec.inRange(from, to))
                    {
                        rec.Link();
                        useStats.append(rec);
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
};

CIArrayOf<CQueryStatsAggregator> CQueryStatsAggregator::allStats;
SpinLock CQueryStatsAggregator::allStatsCrit;

IQueryStatsAggregator *createQueryStatsAggregator(const char *_queryName, unsigned _expirySeconds)
{
    return new CQueryStatsAggregator(_queryName, _expirySeconds);
}

IPropertyTree *getAllQueryStats(time_t from, time_t to)
{
     return CQueryStatsAggregator::getAllQueryStats(from, to);
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
            Owned<IPropertyTree> p = getAllQueryStats(start, end);
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
