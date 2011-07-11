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

#include "thdemonserver.hpp"

#include "jmutex.hpp"
#include "jmisc.hpp"
#include "daaudit.hpp"
#include "daclient.hpp"

#include "thormisc.hpp"
#include "thorport.hpp"
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
    
    void doReportGraph(IWUGraphProgress *progress, CGraphBase *graph, bool finished)
    {
        Owned<IThorActivityIterator> iter = graph->getTraverseIterator(NULL==graph->queryOwner()); // if child query traverse all (as not setup in master, but progress will be present)
        ForEach (*iter)
        {
            CMasterGraphElement &container = (CMasterGraphElement &)iter->query();
            CMasterActivity *activity = (CMasterActivity *)container.queryActivity();
            if (activity) // may not be created (if within child query)
            {
                activity_id id = container.queryId();
                graph_id sourceGraphId = container.queryOwner().queryGraphId();
                unsigned outputs = container.getOutputs();
                unsigned oid = 0;
                for (; oid < outputs; oid++)
                {
                    StringBuffer edgeId;
                    edgeId.append(id).append('_').append(oid);
                    IPropertyTree &edge = progress->updateEdge(sourceGraphId, edgeId.str());
                    activity->getXGMML(oid, &edge); // for subgraph, may recursively call reportGraph
                }
                IPropertyTree &node = progress->updateNode(sourceGraphId, id);
                activity->getXGMML(progress, &node);
            }
        }
    }
    void reportGraph(IWUGraphProgress *progress, CGraphBase *graph, bool finished)
    {
        try
        {
            if (graph->isCreated())
                doReportGraph(progress, graph, finished);
            Owned<IThorGraphIterator> graphIter = graph->getChildGraphs();
            ForEach (*graphIter)
                reportGraph(progress, &graphIter->query(), finished);
        }
        catch (IException *e)
        {
            StringBuffer s;
            LOG(MCwarning, unknownJob, "Failed to update progress information: %s", e->errorMessage(s).str());
            e->Release();
        }
    }
    inline void reportStatus(IWorkUnit *wu, CGraphBase &graph, unsigned startTime, bool finished, bool success=true)
    {
        const char *graphname = graph.queryJob().queryGraphName();
        StringBuffer timer;
        formatGraphTimerLabel(timer, graphname, 0, graph.queryGraphId());
        unsigned duration = msTick()-startTime;
        wu->setTimerInfo(timer.str(), NULL, duration, 1, 0);
        if (finished)
        {
            if (memcmp(graphname,"graph",5)==0)
                graphname+=5;
            SCMStringBuffer wuid;
            wu->getWuid(wuid);
            LOG(daliAuditLogCat,",Timing,ThorGraph,%s,%s,%s,%u,1,%d,%s,%s,%s",
                queryServerStatus().queryProperties()->queryProp("@thorname"),
                wuid.str(),
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

    inline void reportGraph(bool finished, bool success=true)
    {
        if (activeGraphs.ordinality())
        {
            try
            {
                IConstWorkUnit &currentWU = activeGraphs.item(0).queryJob().queryWorkUnit();
                Owned<IConstWUGraphProgress> graphProgress = ((CJobMaster &)activeGraphs.item(0).queryJob()).getGraphProgress();
                Owned<IWUGraphProgress> progress = graphProgress->update();
                ForEachItemIn (g, activeGraphs)
                {
                    CGraphBase &graph = activeGraphs.item(g);
                    reportGraph(progress, &graph, finished);
                }
                progress.clear(); // clear progress(lock) now, before attempting to get lock on wu, potentially delaying progress unlock.
                graphProgress.clear();
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


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    DeMonServer()
    {
        lastReport = msTick();
        reportRate = globals->getPropInt("@watchdogProgressInterval", 30);
    }

    virtual void takeHeartBeat(HeartBeatPacket & hbpacket)
    {
        synchronized block(mutex);
        
        if((hbpacket.packetsize>sizeof(hbpacket.packetsize))&&(hbpacket.progressSize > 0))
        {
            if (0 == activeGraphs.ordinality())
            {
                StringBuffer urlStr;
                LOG(MCdebugProgress, unknownJob, "heartbeat packet received with no active graphs, from=%s", hbpacket.sender.getUrlStr(urlStr).str());
                return;
            }
            rank_t node = querySlaveGroup().rank(hbpacket.sender);
            assertex(node != RANK_NULL);

            MemoryBuffer statsMb;
            statsMb.setBuffer(hbpacket.progressSize, hbpacket.perfdata);

            while (statsMb.remaining())
            {
                graph_id graphId;
                statsMb.read(graphId);
                CMasterGraph *graph = NULL;
                ForEachItemIn(g, activeGraphs) if (activeGraphs.item(g).queryGraphId() == graphId) graph = (CMasterGraph *)&activeGraphs.item(g);
                if (!graph)
                {
                    LOG(MCdebugProgress, unknownJob, "heartbeat received from unknown graph %"GIDPF"d", graphId);
                    break;
                }
                if (!graph->deserializeStats(node, statsMb))
                {
                    LOG(MCdebugProgress, unknownJob, "heartbeat error in graph %"GIDPF"d", graphId);
                    break;
                }
            }
            unsigned now=msTick();
            if (now-lastReport > 1000*reportRate) 
            {
                reportGraph(false);
                lastReport = msTick();
            }
        }
    }
    void startGraph(CGraphBase *graph)
    {
        synchronized block(mutex);
        activeGraphs.append(*LINK(graph));
        graphStarts.append(msTick());

        reportGraph(false);
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
        reportGraph(true, success);
        unsigned p = activeGraphs.find(*graph);
        if (NotFound != p)
        {
            activeGraphs.remove(p);
            graphStarts.remove(p);
        }
    }
    void endGraphs()
    {
        synchronized block(mutex);
        reportGraph(true, false);
        activeGraphs.kill();
    }
};


IDeMonServer *createDeMonServer()
{
    return new DeMonServer();
}
