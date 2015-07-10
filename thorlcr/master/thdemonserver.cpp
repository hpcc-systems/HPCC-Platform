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


class DeMonServer : public CSimpleInterface, implements IDeMonServer
{
private:
    Mutex mutex;
    unsigned lastReport;
    unsigned reportRate;
    CIArrayOf<CGraphBase> activeGraphs;
    UnsignedArray graphStarts;
    
    void doReportGraph(IStatisticGatherer & stats, CGraphBase *graph, bool finished)
    {
        Owned<IThorActivityIterator> iter;
        if (graph->queryOwner() && !graph->isGlobal())
            iter.setown(graph->getIterator()); // Local child graphs still send progress, but aren't connected in master
        else
            iter.setown(graph->getConnectedIterator());
        ForEach (*iter)
        {
            CMasterGraphElement &container = (CMasterGraphElement &)iter->query();
            CMasterActivity *activity = (CMasterActivity *)container.queryActivity();
            if (activity) // may not be created (if within child query)
            {
                activity_id id = container.queryId();

                unsigned outputs = container.getOutputs();
                unsigned oid = 0;
                for (; oid < outputs; oid++)
                {
                    StatsEdgeScope edgeScope(stats, id, oid);
                    activity->getEdgeStats(stats, oid); // for subgraph, may recursively call reportGraph
                }

                StatsActivityScope scope(stats, id);
                activity->getActivityStats(stats);
            }
        }
    }
    void reportGraph(IStatisticGatherer & stats, CGraphBase *graph, bool finished)
    {
        try
        {
            StatsSubgraphScope subgraph(stats, graph->queryGraphId());
            if (graph->isCreated())
                doReportGraph(stats, graph, finished);
            Owned<IThorGraphIterator> graphIter = graph->getChildGraphs();
            ForEach (*graphIter)
                reportGraph(stats, &graphIter->query(), finished);
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, unknownJob, "Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
    void reportStatus(IWorkUnit *wu, CGraphBase &graph, unsigned startTime, bool finished, bool success=true)
    {
        const char *graphname = graph.queryJob().queryGraphName();
        StringBuffer timer, graphScope;
        formatGraphTimerLabel(timer, graphname, 0, graph.queryGraphId());
        formatGraphTimerScope(graphScope, graphname, 0, graph.queryGraphId());
        unsigned duration = msTick()-startTime;
        updateWorkunitTimeStat(wu, SSTsubgraph, graphScope, StTimeElapsed, timer, milliToNano(duration));

        if (finished)
        {
            if (memcmp(graphname,"graph",5)==0)
                graphname+=5;
            LOG(daliAuditLogCat,",Timing,ThorGraph,%s,%s,%s,%u,1,%d,%s,%s,%s",
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

    void reportActiveGraphs(bool finished, bool success=true)
    {
        if (activeGraphs.ordinality())
        {
            try
            {
                IConstWorkUnit &currentWU = activeGraphs.item(0).queryJob().queryWorkUnit();
                const char *graphName = ((CJobMaster &)activeGraphs.item(0).queryJob()).queryGraphName();
                ForEachItemIn (g, activeGraphs)
                {
                    CGraphBase &graph = activeGraphs.item(g);
                    Owned<IWUGraphStats> stats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), graph.queryGraphId());
                    reportGraph(stats->queryStatsBuilder(), &graph, finished);
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
                LOG(MCwarning, unknownJob, "Failed to update progress information: %s", E->errorMessage(s).str());
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
            {
                Owned<IWUGraphStats> stats = currentWU.updateStats(graphName, SCTthor, queryStatisticsComponentName(), graph->queryGraphId());
                reportGraph(stats->queryStatsBuilder(), graph, finished);
            }

            Owned<IWorkUnit> wu = &currentWU.lock();
            reportStatus(wu, *graph, startTime, finished, success);

            queryServerStatus().commitProperties();
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, unknownJob, "Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    DeMonServer()
    {
        lastReport = msTick();
        reportRate = globals->getPropInt("@watchdogProgressInterval", 30);
    }

    virtual void takeHeartBeat(const SocketEndpoint &sender, MemoryBuffer &progressMb)
    {
        synchronized block(mutex);
        if (0 == activeGraphs.ordinality())
        {
            StringBuffer urlStr;
            LOG(MCdebugProgress, unknownJob, "heartbeat packet received with no active graphs, from=%s", sender.getUrlStr(urlStr).str());
            return;
        }
        rank_t node = querySlaveGroup().rank(sender);
        assertex(node != RANK_NULL);

        size32_t compressedProgressSz = progressMb.remaining();
        if (compressedProgressSz)
        {
            MemoryBuffer uncompressedMb;
            ThorExpand(progressMb.readDirect(compressedProgressSz), compressedProgressSz, uncompressedMb);
            do
            {
                graph_id graphId;
                uncompressedMb.read(graphId);
                CMasterGraph *graph = NULL;
                ForEachItemIn(g, activeGraphs) if (activeGraphs.item(g).queryGraphId() == graphId) graph = (CMasterGraph *)&activeGraphs.item(g);
                if (!graph)
                {
                    LOG(MCdebugProgress, unknownJob, "heartbeat received from unknown graph %" GIDPF "d", graphId);
                    break;
                }
                if (!graph->deserializeStats(node, uncompressedMb))
                {
                    LOG(MCdebugProgress, unknownJob, "heartbeat error in graph %" GIDPF "d", graphId);
                    break;
                }
            }
            while (uncompressedMb.remaining());
        }
        unsigned now=msTick();
        if (now-lastReport > 1000*reportRate)
        {
            reportActiveGraphs(false);
            lastReport = msTick();
        }
    }
    void startGraph(CGraphBase *graph)
    {
        synchronized block(mutex);
        activeGraphs.append(*LINK(graph));
        unsigned startTime = msTick();
        graphStarts.append(startTime);
        reportGraph(graph, false, true, startTime);
        const char *graphname = graph->queryJob().queryGraphName();
        if (memcmp(graphname,"graph",5)==0)
            graphname+=5;
        LOG(daliAuditLogCat,",Progress,Thor,StartSubgraph,%s,%s,%s,%u,%s,%s",
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
            reportGraph(graph, true, success, startTime);
            activeGraphs.remove(g);
            graphStarts.remove(g);
        }
    }
    void endGraphs()
    {
        synchronized block(mutex);
        reportActiveGraphs(true, false);
        activeGraphs.kill();
    }
};


IDeMonServer *createDeMonServer()
{
    return new DeMonServer();
}
