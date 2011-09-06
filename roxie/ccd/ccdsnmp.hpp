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

#ifndef CCDSNMP_HPP
#define CCDSNMP_HPP

#include "ccd.hpp"

class CRoxieMetricsManager;

extern CriticalSection counterCrit;

class RoxieQueryStats
{
public:
    unsigned count;
    unsigned failedCount;
    atomic_t active;
    unsigned __int64 totalTime;
    unsigned maxTime;
    unsigned minTime; 

public:
    RoxieQueryStats()
    {
        count = 0;
        failedCount = 0;
        atomic_set(&active, 0);
        totalTime = 0;
        maxTime = 0;
        minTime = 0; 
    }

    inline void noteActive()
    {
        atomic_inc(&active);
    }

    inline void noteComplete()
    {
        atomic_dec(&active);
    }

    void noteQuery(bool failed, unsigned elapsedms)
    {
        CriticalBlock b(counterCrit);
        totalTime += elapsedms;
        if (elapsedms > maxTime)
            maxTime = elapsedms;
        if (!count || elapsedms < minTime)
            minTime = elapsedms;
        count++;
        if (failed)
            failedCount++;
        atomic_dec(&active);
    }

    void addMetrics(CRoxieMetricsManager *mgr, const char *prefix, unsigned interval);
};

interface IQueryStatsAggregator : public IInterface
{
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsedTimeMs, unsigned memUsed, unsigned slavesReplyLen, unsigned bytesOut) = 0;
    virtual IPropertyTree *getStats(time_t from, time_t to) = 0;
};
extern IQueryStatsAggregator *createQueryStatsAggregator(const char *queryName, unsigned expirySeconds);
extern IPropertyTree *getAllQueryStats(time_t from, time_t to);

extern atomic_t queryCount;
extern RoxieQueryStats unknownQueryStats;
extern RoxieQueryStats loQueryStats;
extern RoxieQueryStats hiQueryStats;
extern RoxieQueryStats slaQueryStats;
extern atomic_t retriesIgnoredPrm;
extern atomic_t retriesIgnoredSec;
extern atomic_t retriesNeeded;
extern atomic_t retriesReceivedPrm;
extern atomic_t retriesReceivedSec;
extern atomic_t retriesSent;
extern atomic_t rowsIn;
extern atomic_t ibytiPacketsFromSelf;
extern atomic_t ibytiPacketsSent;
extern atomic_t ibytiPacketsWorked;
extern atomic_t ibytiPacketsHalfWorked;
extern atomic_t ibytiPacketsReceived;
extern atomic_t ibytiPacketsTooLate;
extern atomic_t ibytiNoDelaysPrm;
extern atomic_t ibytiNoDelaysSec;
extern atomic_t packetsReceived;
extern atomic_t packetsSent;
extern atomic_t resultsReceived;
extern atomic_t indexRecordsRead;
extern atomic_t postFiltered;
extern atomic_t abortsSent;
extern atomic_t activitiesStarted;
extern atomic_t activitiesCompleted;
extern atomic_t diskReadStarted;
extern atomic_t diskReadCompleted;
extern atomic_t globalSignals;
extern atomic_t globalLocks;

extern unsigned maxSlavesActive;
extern unsigned slavesActive;
extern unsigned rowsOut;
extern unsigned queueLength;
extern unsigned maxQueueLength;
extern unsigned maxScanLength;
extern unsigned totScanLength;
extern unsigned totScans;
extern unsigned meanScanLength;
extern atomic_t numFilesToProcess;

#ifdef TIME_PACKETS
extern unsigned __int64 packetWaitElapsed;
extern atomic_t packetWaitCount;
extern unsigned packetWaitMax;
extern unsigned __int64 packetRunElapsed;
extern atomic_t packetRunCount;
extern unsigned packetRunMax;
#endif

extern unsigned lastQueryDate;
extern unsigned lastQueryTime;

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
