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
#include "thdemonserver.hpp"

#include "jmutex.hpp"
#include "jmisc.hpp"
#include "daaudit.hpp"
#include "daclient.hpp"

#include "thormisc.hpp"
#include "thorport.hpp"
#include "thcompressutil.hpp"
#include "thgraphmaster.ipp"
#include "thgraphmanager.hpp"
#include "thwatchdog.hpp"
#include "jstats.h"


extern graph_decl const StatisticsMapping graphStatistics;
class DeMonServer : public CSimpleInterface, implements IDeMonServer
{
private:
    Mutex mutex;
    unsigned lastReport;
    unsigned reportRate;
    CIArrayOf<CGraphBase> activeGraphs;
    UnsignedArray graphStarts;
    double thorManagerRate = 0;
    double thorWorkerRate = 0;
    unsigned numberOfMachines = 0;
    cost_type costLimit = 0;
    cost_type workunitCost = 0;
    Owned<IStatisticsCache> statisticsCache;
    std::vector<Linked<CGraphBase>> finishedGraphs;

    void doGetStats(IStatisticGatherer & stats, CGraphBase *graph)
    {
        ((CMasterGraph *)graph)->getStats(stats);
    }
    void doGetGraphContents(IStatisticGatherer & stats, CGraphBase *graph)
    {
        if (graph->hasProgress()) // if there have ever been any progress, ensure they are republished
            doGetStats(stats, graph);
        Owned<IThorGraphIterator> graphIter = graph->getChildGraphIterator();
        ForEach (*graphIter)
            doGetGraphStats(stats, &graphIter->query());
    }
    void doGetGraphStats(IStatisticGatherer & stats, CGraphBase *graph)
    {
        try
        {
            if (graph->queryParentActivityId() && !graph->containsActivities())
            {
                StatsActivityScope activity(stats, graph->queryParentActivityId());
                StatsChildGraphScope subgraph(stats, graph->queryGraphId());
                doGetGraphContents(stats, graph);
            }
            else
            {
                StatsSubgraphScope subgraph(stats, graph->queryGraphId());
                doGetGraphContents(stats, graph);
            }
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, thorJob, "Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
    void reportStatus(IWorkUnit *wu, CGraphBase &graph, unsigned startTime, bool finished, bool success=true)
    {
        const char *graphname = graph.queryJob().queryGraphName();
        unsigned wfid = graph.queryJob().getWfid();
        StringBuffer timer, graphScope;
        formatGraphTimerLabel(timer, graphname, 0, graph.queryGraphId());
        formatGraphTimerScope(graphScope, wfid, graphname, 0, graph.queryGraphId());
        unsigned duration = msTick()-startTime;
        updateWorkunitStat(wu, SSTsubgraph, graphScope, StTimeElapsed, timer, milliToNano(duration));
        if (costLimit || finished)
        {
            const cost_type sgCost = money2cost_type(calcCost(thorManagerRate, duration) + calcCost(thorWorkerRate, duration) * numberOfMachines);
            cost_type costDiskAccess = graph.getDiskAccessCost();
            if (finished)
            {
                if (sgCost)
                    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, graphScope, StCostExecute, NULL, sgCost, 1, 0, StatsMergeReplace);
                if (costDiskAccess)
                    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, graphScope, StCostFileAccess, NULL, costDiskAccess, 1, 0, StatsMergeReplace);
            }

            const cost_type totalCost = workunitCost + sgCost + costDiskAccess;
            if (costLimit>0 && totalCost > costLimit)
            {
                LOG(MCwarning, thorJob, "ABORT job cost exceeds limit");
                graph.fireException(MakeThorException(TE_CostExceeded, "Job cost exceeds limit"));
            }
        }
        if (finished)
        {
            if (memcmp(graphname,"graph",5)==0)
                graphname+=5;
            LOG(MCauditInfo,",Timing,ThorGraph,%s,%s,%s,%u,1,%d,%s,%s,%s",
                queryServerStatus().queryProperties()->queryProp("@thorname"),
                wu->queryWuid(),
                graphname,
                (unsigned)graph.queryGraphId(),
                duration,
                success?"SUCCESS":"FAILED",
                queryServerStatus().queryProperties()->queryProp("@nodeGroup"),
                queryServerStatus().queryProperties()->queryProp("@queue"));
            queryServerStatus().queryProperties()->removeProp("@graph");
            queryServerStatus().queryProperties()->removeProp("@subgraph");
            queryServerStatus().queryProperties()->removeProp("@sg_duration");
        }
        else
        {
            queryServerStatus().queryProperties()->setProp("@graph", graph.queryJob().queryGraphName());
            queryServerStatus().queryProperties()->setPropInt("@subgraph", (int)graph.queryGraphId());
            queryServerStatus().queryProperties()->setPropInt("@sg_duration", (duration+59999)/60000); // round it up
        }
    }

    void gatherActiveGraphStats(bool finished, bool success=true)
    {
        if (activeGraphs.ordinality())
        {
            ForEachItemIn (g, activeGraphs)
            {
                CGraphBase &graph = activeGraphs.item(g);
                gatherGraphStats(&graph, finished);
            }
        }
    }
    void reportActiveGraphs(bool finished, bool success=true)
    {
        if (activeGraphs.ordinality())
        {
            try
            {
                CJobBase & activeJob = activeGraphs.item(0).queryJob();
                IConstWorkUnit &currentWU = activeJob.queryWorkUnit();
                const char *graphName = ((CJobMaster &)activeJob).queryGraphName();
                unsigned wfid = activeJob.getWfid();
                ForEachItemIn (g, activeGraphs)
                {
                    CGraphBase &graph = activeGraphs.item(g);
                    Owned<IWUGraphStats> stats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), wfid, graph.queryGraphId(), false);
                    statisticsCache->mergeInto(wfid, graph.queryGraphId(), stats->queryStatsBuilder());
                }
                Owned<IWorkUnit> wu = &currentWU.lock();
                ForEachItemIn (g2, activeGraphs)
                {
                    CGraphBase &graph = activeGraphs.item(g2);
                    unsigned startTime = graphStarts.item(g2);
                    reportStatus(wu, graph, startTime, finished, success);
                }
                queryServerStatus().commitProperties();
            }
            catch (IException *E)
            {
                StringBuffer s;
                LOG(MCwarning, thorJob, "Failed to update progress information: %s", E->errorMessage(s).str());
                E->Release();
            }
        }
    }
    void reportCompletedGraphs()
    {
        for (auto graph: finishedGraphs)
        {
            try
            {
                IConstWorkUnit &currentWU = graph->queryJob().queryWorkUnit();
                const char *graphName = graph->queryJob().queryGraphName();
                unsigned wfid = graph->queryJob().getWfid();
                Owned<IWUGraphStats> stats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), wfid, graph->queryGraphId(), false);
                statisticsCache->mergeInto(wfid, graph->queryGraphId(), stats->queryStatsBuilder(), true);
                Owned<IWorkUnit> wu = &currentWU.lock();
                queryServerStatus().commitProperties();
            }
            catch (IException *e)
            {
                StringBuffer s;
                LOG(MCwarning, thorJob, "Failed to update graph stats information: %s", e->errorMessage(s).str());
                e->Release();
            }
        }
        finishedGraphs.clear();
    }
    void gatherGraphStats(CGraphBase *graph, bool finished)
    {
        try
        {
            unsigned wfid = graph->queryJob().getWfid();
            if (finished)
                finishedGraphs.push_back(LINK(graph));
            Owned<IStatisticGatherer> gatherer = createStatsAggregatorGather(statisticsCache, wfid, graph->queryGraphId());
            doGetGraphStats(*gatherer, graph);
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, thorJob, "Failed to update graph stats: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
    void reportGraphStats(CGraphBase *graph)
    {
        try
        {
            IConstWorkUnit &currentWU = graph->queryJob().queryWorkUnit();
            const char *graphName = ((CJobMaster &)activeGraphs.item(0).queryJob()).queryGraphName();
            unsigned wfid = graph->queryJob().getWfid();
            {
                Owned<IWUGraphStats> stats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), wfid, graph->queryGraphId(), false);
                statisticsCache->mergeInto(wfid, graph->queryGraphId(), stats->queryStatsBuilder());
            }
            Owned<IWorkUnit> wu = &currentWU.lock();
            queryServerStatus().commitProperties();
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, thorJob, "Failed to update graph stats information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
    void reportGraphStatus(CGraphBase *graph, bool finished, bool success, unsigned startTime, unsigned __int64 startTimeStamp)
    {
        try
        {
            IConstWorkUnit &currentWU = graph->queryJob().queryWorkUnit();
            Owned<IWorkUnit> wu = &currentWU.lock();
            if (startTimeStamp)
            {
                StringBuffer graphScope;
                const char *graphname = graph->queryJob().queryGraphName();
                unsigned wfid = graph->queryJob().getWfid();
                formatGraphTimerScope(graphScope, wfid, graphname, 0, graph->queryGraphId());
                wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, graphScope, StWhenStarted, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);
            }
            reportStatus(wu, *graph, startTime, finished, success);

            queryServerStatus().commitProperties();
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, thorJob, "Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    DeMonServer()
    {
        lastReport = msTick();
        reportRate = globals->getPropInt("@watchdogProgressInterval", 30);
        thorManagerRate = getThorManagerRate();
        thorWorkerRate = getThorWorkerRate();
        statisticsCache.setown(createStatisticsCache());
    }

    virtual void takeHeartBeat(MemoryBuffer &progressMb)
    {
        synchronized block(mutex);
        if (0 == activeGraphs.ordinality())
        {
            StringBuffer urlStr;
            LOG(MCdebugProgress, thorJob, "heartbeat packet received with no active graphs");
            return;
        }
        size32_t compressedProgressSz = progressMb.remaining();
        if (compressedProgressSz)
        {
            MemoryBuffer uncompressedMb;
            ThorExpand(progressMb.readDirect(compressedProgressSz), compressedProgressSz, uncompressedMb);
            do
            {
                graph_id graphId;
                unsigned slave;
                uncompressedMb.read(slave);
                uncompressedMb.read(graphId);
                CMasterGraph *graph = NULL;
                ForEachItemIn(g, activeGraphs) if (activeGraphs.item(g).queryGraphId() == graphId) graph = (CMasterGraph *)&activeGraphs.item(g);
                if (!graph)
                {
                    LOG(MCdebugProgress, thorJob, "heartbeat received from unknown graph %" GIDPF "d", graphId);
                    break;
                }
                if (!graph->deserializeStats(slave, uncompressedMb))
                {
                    LOG(MCdebugProgress, thorJob, "heartbeat error in graph %" GIDPF "d", graphId);
                    break;
                }
            }
            while (uncompressedMb.remaining());
        }
        unsigned now=msTick();
        if (now-lastReport > 1000*reportRate)
        {
            gatherActiveGraphStats(false);
            reportActiveGraphs(false);
            reportCompletedGraphs();
            lastReport = msTick();
        }
    }
    void startGraph(CGraphBase *graph)
    {
        synchronized block(mutex);

        IConstWorkUnit & wu =  graph->queryJob().queryWorkUnit();
        workunitCost = aggregateCost(&wu);

        Owned<const IPropertyTree> costs = getCostsConfiguration();
        double softLimit = 0.0, hardLimit = 0.0;
        if (costs)
        {
            softLimit = costs->getPropReal("@limit");
            hardLimit = costs->getPropReal("@hardlimit");
        }
        double tmpcostLimit = wu.getDebugValueReal("maxCost", softLimit);
        if (hardLimit && ((tmpcostLimit == 0) || (tmpcostLimit > hardLimit)))
            costLimit = money2cost_type(hardLimit);
        else
            costLimit = money2cost_type(tmpcostLimit);
        numberOfMachines = queryNodeClusterWidth() / globals->getPropInt("@numWorkersPerPod", 1); // Number of Pods or physical machines
        activeGraphs.append(*LINK(graph));
        unsigned startTime = msTick();
        graphStarts.append(startTime);
        reportGraphStatus(graph, false, true, startTime, getTimeStampNowValue());
        const char *graphname = graph->queryJob().queryGraphName();
        if (memcmp(graphname,"graph",5)==0)
            graphname+=5;
        LOG(MCauditInfo,",Progress,Thor,StartSubgraph,%s,%s,%s,%u,%s,%s",
                queryServerStatus().queryProperties()->queryProp("@thorname"),
                graph->queryJob().queryWuid(),
                graphname,
                (unsigned)graph->queryGraphId(), queryServerStatus().queryProperties()->queryProp("@nodeGroup"), queryServerStatus().queryProperties()->queryProp("@queue"));
    }
    void endGraph(CGraphBase *graph, bool success)
    {
        synchronized block(mutex);
        unsigned g = activeGraphs.find(*graph);
        if (NotFound != g)
        {
            unsigned startTime = graphStarts.item(g);
            gatherGraphStats(graph, false);
            reportGraphStatus(graph, true, success, startTime, 0);
            activeGraphs.remove(g);
            graphStarts.remove(g);
        }
    }
    void endGraphs()
    {
        synchronized block(mutex);
        gatherActiveGraphStats(true, false);
        reportActiveGraphs(true, false);
        activeGraphs.kill();
    }
};


IDeMonServer *createDeMonServer()
{
    return new DeMonServer();
}
