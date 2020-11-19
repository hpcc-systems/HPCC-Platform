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

#ifndef CCDSNMP_HPP
#define CCDSNMP_HPP

#include "ccd.hpp"

class CRoxieMetricsManager;

class RoxieQueryStats
{
public:
    RelaxedAtomic<unsigned> count;
    RelaxedAtomic<unsigned> failedCount;
    RelaxedAtomic<unsigned> active;
    RelaxedAtomic<unsigned __int64> totalTime;
    RelaxedAtomic<unsigned> maxTime;
    RelaxedAtomic<unsigned> minTime;

public:
    RoxieQueryStats()
    {
        count = 0;
        failedCount = 0;
        active = 0;
        totalTime = 0;
        maxTime = 0;
        minTime = (unsigned) -1;
    }

    inline void noteActive()
    {
        active++;
    }

    inline void noteComplete()
    {
        active--;
    }

    void noteQuery(bool failed, unsigned elapsedms)
    {
        totalTime += elapsedms;
        maxTime.store_max(elapsedms);
        minTime.store_min(elapsedms);
        count++;
        if (failed)
            failedCount++;
        active--;
    }

    void addMetrics(CRoxieMetricsManager *mgr, const char *prefix, unsigned interval);
};

interface IQueryStatsAggregator : public IInterface
{
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsedTimeMs, unsigned memUsed, unsigned agentsReplyLen, unsigned bytesOut) = 0;
    virtual IPropertyTree *getStats(time_t from, time_t to) = 0;
};

extern IQueryStatsAggregator *queryGlobalQueryStatsAggregator();
extern IQueryStatsAggregator *createQueryStatsAggregator(const char *queryName, unsigned expirySeconds);
extern IPropertyTree *getAllQueryStats(bool includeQueries, bool rawStats, time_t from, time_t to);
extern IPropertyTree *getQueryRawStats(const char *queryID, time_t from, time_t to);

extern RelaxedAtomic<unsigned> queryCount;
extern RoxieQueryStats unknownQueryStats;
extern RoxieQueryStats loQueryStats;
extern RoxieQueryStats hiQueryStats;
extern RoxieQueryStats slaQueryStats;
extern RoxieQueryStats combinedQueryStats;
extern RelaxedAtomic<unsigned> retriesIgnoredPrm;
extern RelaxedAtomic<unsigned> retriesIgnoredSec;
extern RelaxedAtomic<unsigned> retriesNeeded;
extern RelaxedAtomic<unsigned> retriesReceivedPrm;
extern RelaxedAtomic<unsigned> retriesReceivedSec;
extern RelaxedAtomic<unsigned> retriesSent;
extern RelaxedAtomic<unsigned> rowsIn;
extern RelaxedAtomic<unsigned> ibytiPacketsFromSelf;
extern RelaxedAtomic<unsigned> ibytiPacketsSent;
extern RelaxedAtomic<unsigned> ibytiPacketsWorked;
extern RelaxedAtomic<unsigned> ibytiPacketsHalfWorked;
extern RelaxedAtomic<unsigned> ibytiPacketsReceived;
extern RelaxedAtomic<unsigned> ibytiPacketsTooLate;
extern RelaxedAtomic<unsigned> ibytiPacketsTooEarly;
extern RelaxedAtomic<unsigned> ibytiNoDelaysPrm;
extern RelaxedAtomic<unsigned> ibytiNoDelaysSec;
extern RelaxedAtomic<unsigned> packetsReceived;
extern RelaxedAtomic<unsigned> packetsSent;
extern RelaxedAtomic<unsigned> resultsReceived;
extern RelaxedAtomic<unsigned> indexRecordsRead;
extern RelaxedAtomic<unsigned> postFiltered;
extern RelaxedAtomic<unsigned> abortsSent;
extern RelaxedAtomic<unsigned> activitiesStarted;
extern RelaxedAtomic<unsigned> activitiesCompleted;
extern RelaxedAtomic<unsigned> diskReadStarted;
extern RelaxedAtomic<unsigned> diskReadCompleted;
extern RelaxedAtomic<unsigned> globalSignals;
extern RelaxedAtomic<unsigned> globalLocks;

extern RelaxedAtomic<unsigned> maxAgentsActive;
extern RelaxedAtomic<unsigned> agentsActive;
extern RelaxedAtomic<unsigned> rowsOut;
extern RelaxedAtomic<unsigned> queueLength;
extern RelaxedAtomic<unsigned> maxQueueLength;
extern RelaxedAtomic<unsigned> maxScanLength;
extern RelaxedAtomic<unsigned> totScanLength;
extern RelaxedAtomic<unsigned> totScans;
extern RelaxedAtomic<unsigned> numFilesToProcess;

#ifdef TIME_PACKETS
extern RelaxedAtomic<unsigned __int64> packetWaitElapsed;
extern RelaxedAtomic<unsigned> packetWaitMax;
extern RelaxedAtomic<unsigned> packetWaitCount;
extern RelaxedAtomic<unsigned __int64> packetRunElapsed;
extern RelaxedAtomic<unsigned> packetRunMax;
extern RelaxedAtomic<unsigned> packetRunCount;
#endif

extern RelaxedAtomic<unsigned> lastQueryDate;
extern RelaxedAtomic<unsigned> lastQueryTime;

interface IRoxieMetricsManager : extends IInterface
{
    virtual void dumpMetrics() = 0;
    virtual void addUserMetric(const char *name, const char *regex) = 0;
    virtual StringBuffer &getMetrics(StringBuffer &) = 0;
    virtual void resetMetrics() = 0;
};

IRoxieMetricsManager  *createRoxieMetricsManager();
extern Owned<IRoxieMetricsManager> roxieMetrics;

#endif
