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


/**
 * GraphStatsCollection collects stats from graph hierarchies and writes them to the 
 * workunit.  It also calls StatisticAggregator to extract statistic kinds for aggregation.
 */
namespace GraphStatsCollection
{
    void gatherScopedGraphStats(IStatisticGatherer & stats, CGraphBase *graph);

    // Recursively gathers statistics from a graph and all its child graphs.
    void gatherGraphContents(IStatisticGatherer & stats, CGraphBase *graph)
    {
        // Gather the subgraph stats when there has been progress
        if (graph->hasProgress())
            ((CMasterGraph *)graph)->getStats(stats);

        // Gather the child graph stats recursively
        Owned<IThorGraphIterator> graphIter = graph->getChildGraphIterator();
        ForEach (*graphIter)
            gatherScopedGraphStats(stats, &graphIter->query());
    }

    // Gathers statistics from a graph with appropriate scoping context.
    void gatherScopedGraphStats(IStatisticGatherer & stats, CGraphBase *graph)
    {
        try
        {
            // If graph has a parent activity but no activities of its own,
            // scope it as a child graph under that activity
            if (graph->queryParentActivityId() && !graph->containsActivities())
            {
                StatsActivityScope activity(stats, graph->queryParentActivityId());
                StatsChildGraphScope subgraph(stats, graph->queryGraphId());
                gatherGraphContents(stats, graph);
            }
            else
            {
                // Otherwise, scope it as a regular subgraph
                StatsSubgraphScope subgraph(stats, graph->queryGraphId());
                gatherGraphContents(stats, graph);
            }
        }
        catch (IException *e)
        {
            StringBuffer s;
            IWARNLOG("Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }

    // Gather statistics from graph hierarchies into wuGraphStats
    // StatisticsAggregator imports any stats in the aggregation kind from statistics gatherer
    void updateGraphStats(IConstWorkUnit & currentWU, const char *graphName, unsigned wfid, CGraphBase & graph, StatisticsAggregator & statsAggregator)
    {
        // Graph statistics are written when IWUGraphStats is destroyed
        Owned<IWUGraphStats> wuGraphStats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), wfid, graph.queryGraphId(), false);
        IStatisticGatherer & statsBuilder = wuGraphStats->queryStatsBuilder();
        gatherScopedGraphStats(statsBuilder, &graph);

        Owned<IStatisticCollection> statsCollection = statsBuilder.getResult();
        // Merge any statistic kinds that are in the aggregation list into StatisticsAggregator
        statsAggregator.recordStats(statsCollection, wfid, graphName, graph.queryGraphId());
    }
};


class DeMonServer : public CSimpleInterface, implements IDeMonServer
{
private:
    Mutex mutex;
    unsigned lastReportedMsTick;
    unsigned reportRateMSecs;
    CIArrayOf<CGraphBase> activeGraphs;
    UnsignedArray graphStartsMsTick;
    double thorManagerRate = 0;
    double thorWorkerRate = 0;
    cost_type costLimit = 0;
    cost_type previousExecutionCost = 0;
    StatisticsAggregator statsAggregator;

    void reportStatus(IWorkUnit *wu, CGraphBase &graph, unsigned startTime, bool finished, bool success)
    {
        const char *graphName = graph.queryJob().queryGraphName();
        unsigned wfid = graph.queryJob().getWfid();
        StringBuffer timer, graphScope;
        formatGraphTimerLabel(timer, graphName, 0, graph.queryGraphId());
        formatGraphTimerScope(graphScope, wfid, graphName, 0, graph.queryGraphId());
        unsigned duration = msTick()-startTime;
        updateWorkunitStat(wu, SSTsubgraph, graphScope, StTimeElapsed, timer, milliToNano(duration));
        if (costLimit || finished)
        {
            const cost_type sgCost = money2cost_type(calcCost(thorManagerRate, duration) + calcCost(thorWorkerRate, duration) * queryNodeClusterWidth());
            if (finished)
            {
                if (sgCost)
                    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, graphScope, StCostExecute, NULL, sgCost, 1, 0, StatsMergeReplace);
            }

            const cost_type totalCost = previousExecutionCost + sgCost + graph.getDiskAccessCost();

            if (costLimit>0 && totalCost > costLimit && !finished)
            {
                WARNLOG("ABORT job cost exceeds limit");
                graph.fireException(MakeThorException(TE_CostExceeded, "Job cost exceeds limit"));
            }
        }
        if (finished)
        {
            const char * graphId = (memcmp(graphName,"graph",5)==0) ? graphName+5 : graphName;
            LOG(MCauditInfo,",Timing,ThorGraph,%s,%s,%s,%u,1,%u,%s,%s,%s",
                queryServerStatus().queryProperties()->queryProp("@thorname"),
                wu->queryWuid(),
                graphId,
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
            queryServerStatus().queryProperties()->setProp("@graph", graphName);
            queryServerStatus().queryProperties()->setPropInt("@subgraph", (int)graph.queryGraphId());
            queryServerStatus().queryProperties()->setPropInt("@sg_duration", (duration+59999)/60000); // round it up
        }
    }
    void updateGraphStats(IConstWorkUnit &currentWU, const char *graphName, unsigned wfid, CGraphBase & graph)
    {
        GraphStatsCollection::updateGraphStats(currentWU, graphName, wfid, graph, statsAggregator);
    }
    void reportStartGraph(CGraphBase *graph, unsigned startTimeMsTick)
    {
        unsigned wfid = graph->queryJob().getWfid();
        const char *graphName = graph->queryJob().queryGraphName();

        StringBuffer graphScope;
        formatGraphTimerScope(graphScope, wfid, graphName, 0, graph->queryGraphId());

        Owned<IWorkUnit> wu = &(graph->queryJob().queryWorkUnit().lock());
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, graphScope, StWhenStarted, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);

        reportStatus(wu, *graph, startTimeMsTick, false, true);
        queryServerStatus().commitProperties();
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
                    updateGraphStats(currentWU, graphName, wfid, graph);
                }
                Owned<IWorkUnit> wu = &currentWU.lock();
                ForEachItemIn (g2, activeGraphs)
                {
                    CGraphBase &graph = activeGraphs.item(g2);
                    unsigned startTime = graphStartsMsTick.item(g2);
                    reportStatus(wu, graph, startTime, finished, success);
                }
                updateAggregates(wu);
                queryServerStatus().commitProperties();
            }
            catch (IException *E)
            {
                IWARNLOG(E, "Failed to update progress information");
                E->Release();
            }
        }
    }
    void reportGraph(CGraphBase *graph, bool finished, bool success, unsigned startTime)
    {
        try
        {
            IConstWorkUnit &currentWU = graph->queryJob().queryWorkUnit();
            const char *graphName = ((CJobMaster &)activeGraphs.item(0).queryJob()).queryGraphName();
            unsigned wfid = graph->queryJob().getWfid();
            updateGraphStats(currentWU, graphName, wfid, *graph);

            Owned<IWorkUnit> wu = &currentWU.lock();
            // note: it is not necessary and inefficient to call updateAggregates here as the workflow
            // engine/agent will call it anyway after a graph has finished
            reportStatus(wu, *graph, startTime, finished, success);
            queryServerStatus().commitProperties();
        }
        catch (IException *e)
        {
            StringBuffer s;
            IWARNLOG(e, "Failed to update progress information");
            e->Release();
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    DeMonServer() : statsAggregator(stdAggregateKindStatistics)
    {
        lastReportedMsTick = msTick();
        reportRateMSecs = globals->getPropInt("@watchdogProgressInterval", 30) * 1000;
        thorManagerRate = getThorManagerRate();
        thorWorkerRate = getThorWorkerRate();
    }

    virtual void takeHeartBeat(MemoryBuffer &progressMb) override
    {
        synchronized block(mutex);
        if (0 == activeGraphs.ordinality())
        {
            StringBuffer urlStr;
            IWARNLOG("heartbeat packet received with no active graphs");
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
                    IWARNLOG("heartbeat received from unknown graph %" GIDPF "d", graphId);
                    break;
                }
                if (!graph->deserializeStats(slave, uncompressedMb))
                {
                    IWARNLOG("heartbeat error in graph %" GIDPF "d", graphId);
                    break;
                }
            }
            while (uncompressedMb.remaining());
        }
        unsigned now=msTick();
        if (now-lastReportedMsTick > reportRateMSecs)
        {
            reportActiveGraphs(false);
            lastReportedMsTick = msTick();
        }
    }
    virtual void startGraph(CGraphBase *graph) override
    {
        synchronized block(mutex);

        IConstWorkUnit & wu =  graph->queryJob().queryWorkUnit();
        previousExecutionCost = aggregateCost(&wu, nullptr, StCostExecute, 3);

        costLimit = getGuillotineCost(&wu);
        activeGraphs.append(*LINK(graph));
        unsigned startTime = msTick();
        graphStartsMsTick.append(startTime);
        reportStartGraph(graph, startTime);
    }
    virtual void endGraph(CGraphBase *graph, bool success) override
    {
        synchronized block(mutex);
        unsigned g = activeGraphs.find(*graph);
        if (NotFound != g)
        {
            unsigned startTime = graphStartsMsTick.item(g);
            reportGraph(graph, true, success, startTime);
            activeGraphs.remove(g);
            graphStartsMsTick.remove(g);
        }
    }
    virtual void endGraphs() override
    {
        synchronized block(mutex);
        reportActiveGraphs(true, false);
        activeGraphs.kill();
        graphStartsMsTick.kill();
    }
    // Generates aggregates and writes any modified aggregates to workunit
    virtual void updateAggregates(IWorkUnit * lockedWu) override
    {
        statsAggregator.updateAggregates(lockedWu);
    }
    virtual void loadExistingAggregates(IConstWorkUnit &workunit) override
    {
        statsAggregator.loadExistingAggregates(workunit);
    }
};


IDeMonServer *createDeMonServer()
{
    return new DeMonServer();
}
