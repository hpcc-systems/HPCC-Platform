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

// Slave Watchdog
#include "platform.h"
#include <stdio.h>
#include "jsocket.hpp"
#include "jmisc.hpp"
#include "portlist.h"
#include "thorport.hpp"
#include "thormisc.hpp"
#include "thcompressutil.hpp"
#include "thwatchdog.hpp"
#include "slwatchdog.hpp"
#include "thgraphslave.hpp"

class CGraphProgressHandlerBase : public CInterfaceOf<ISlaveWatchdog>, implements IThreaded
{
    mutable CriticalSection crit;
    CGraphArray activeGraphs;
    bool stopped = true;
    bool progressEnabled = false;
    CThreaded threaded;
    SocketEndpoint self;

    void gatherAndSend()
    {
        MemoryBuffer sendMb, progressMb;
        HeartBeatPacketHeader hb;
        hb.sender = self;
        hb.tick++;
        size32_t progressSizePos = (byte *)&hb.progressSize - (byte *)&hb;
        hb.serialize(sendMb);

        hb.progressSize = gatherData(progressMb);
        sendMb.writeDirect(progressSizePos, sizeof(hb.progressSize), &hb.progressSize);
        sendMb.append(progressMb);
        size32_t packetSize = sendMb.length();
        sendMb.writeDirect(0, sizeof(hb.packetSize), &packetSize);
        sendData(sendMb);
    }
    virtual void sendData(MemoryBuffer &mb) = 0;

public:
    CGraphProgressHandlerBase() : threaded("CGraphProgressHandler", this)
    {
        self = queryMyNode()->endpoint();

        progressEnabled = globals->getPropBool("@watchdogProgressEnabled");
#ifdef _WIN32
        threaded.adjustPriority(+1); // it is critical that watchdog packets get through.
#endif
    }
    void start()
    {
        stopped = false;
        threaded.start(false);
    }
    virtual void beforeDispose() override
    {
        stop();
    }
    size32_t gatherData(MemoryBuffer &mb)
    {
        CriticalBlock b(crit);
        if (progressEnabled)
        {
            MemoryBuffer progressData;
            {
                CriticalBlock b(crit);
                ForEachItemIn(g, activeGraphs) // NB: 1 for each slavesPerProcess
                {
                    CGraphBase &graph = activeGraphs.item(g);
                    progressData.append((unsigned)graph.queryJobChannel().queryMyRank()-1);
                    if (!graph.serializeStats(progressData))
                        progressData.setLength(progressData.length()-sizeof(unsigned));
                }
            }
            size32_t sz = progressData.length();
            if (sz)
            {
                ThorCompress(progressData, mb, 0x200);
                return sz;
            }
        }
        return 0;
    }

// ISlaveWatchdog impl.
    virtual void startGraph(CGraphBase &graph) override
    {
        CriticalBlock b(crit);
        activeGraphs.append(*LINK(&graph));
        StringBuffer str("Watchdog: Start Job ");
        LOG(MCthorDetailedDebugInfo, "%s", str.append(graph.queryGraphId()).str());
    }
    virtual void stopGraph(CGraphBase &graph, MemoryBuffer *mb) override
    {
        CriticalBlock b(crit);
        if (NotFound == activeGraphs.find(graph))
        {
            if (mb)
                mb->append((size32_t)0);
        }
        else
        {
            StringBuffer str("Watchdog: Stop Job ");
            LOG(MCthorDetailedDebugInfo, "%s", str.append(graph.queryGraphId()).str());
            if (mb)
            {
                DelayedSizeMarker sizeMark(*mb);
                gatherData(*mb);
                sizeMark.write();
            }
            activeGraphs.zap(graph);
        }
    }
    virtual void stop() override
    {
        if (!stopped)
        {
#ifdef _WIN32
            threaded.adjustPriority(0); // restore to normal before stopping
#endif
            stopped = true;
            threaded.join();
            DBGLOG("Stopped watchdog");
        }
    }
    virtual void debugRequest(MemoryBuffer &msg, const IPropertyTree *req) const override
    {
        StringBuffer edgeString;
        req->getProp("@edgeId", edgeString);

        // Split edge string in activityId and edgeIdx
        const char *pEdge=edgeString.str();
        const activity_id actId = (activity_id)_atoi64(pEdge);
        if (!actId) return;

        while (*pEdge && *pEdge!='_')  ++pEdge;
        if (!*pEdge) return;
        const unsigned edgeIdx = (unsigned)_atoi64(++pEdge);

        CriticalBlock b(crit);
        ForEachItemIn(g, activeGraphs) // NB: 1 for each slavesPerProcess
        {
            CGraphBase &graph = activeGraphs.item(g);
            CGraphElementBase *element = graph.queryElement(actId);
            if (element)
            {
                CSlaveActivity *activity = (CSlaveActivity*) element->queryActivity();
                if (activity) activity->debugRequest(edgeIdx, msg);
            }
        }
    }

// IThreaded
    virtual void threadmain() override
    {
        LOG(MCthorDetailedDebugInfo, "Watchdog: thread running");
        gatherAndSend(); // send initial data
        assertex(HEARTBEAT_INTERVAL>=8);
        unsigned count = HEARTBEAT_INTERVAL+getRandom()%8-4;
        while (!stopped)
        {
            Sleep(1000);
            if (stopped)
                break;
            if (count--==0)
            {
                gatherAndSend();
                count = HEARTBEAT_INTERVAL+getRandom()%8-4;
                traceMemUsage(); // NB: this is conditional tracing (if increased significantly)
            }
        }
    }
};


class CGraphProgressUDPHandler : public CGraphProgressHandlerBase
{
    Owned<ISocket> sock;
public:
    CGraphProgressUDPHandler()
    {
        StringBuffer ipStr;
        queryMasterNode().endpoint().getHostText(ipStr);
        sock.setown(ISocket::udp_connect(getFixedPort(getMasterPortBase(), TPORT_watchdog),ipStr.str()));
        start();
    }
    virtual void sendData(MemoryBuffer &mb) override
    {
        HeartBeatPacketHeader hb;
        //Cast is to avoid warning about writing to an object with non trivial copy assignment
        memcpy(reinterpret_cast<void *>(&hb), mb.toByteArray(), sizeof(HeartBeatPacketHeader));
        if (hb.packetSize > UDP_DATA_MAX)
        {
            IWARNLOG("Progress packet too big! progress lost");
            hb.progressSize = 0;
            hb.packetSize = sizeof(HeartBeatPacketHeader);
        }
        sock->write(mb.toByteArray(), mb.length());
    }
};


class CGraphProgressMPHandler : public CGraphProgressHandlerBase
{
public:
    CGraphProgressMPHandler()
    {
        start();
    }
    virtual void sendData(MemoryBuffer &mb) override
    {
        CMessageBuffer msg;
        msg.swapWith(mb);
        queryNodeComm().send(msg, 0, MPTAG_THORWATCHDOG);
    }
};

ISlaveWatchdog *createProgressHandler(bool udp)
{
    if (udp)
        return new CGraphProgressUDPHandler();
    else
        return new CGraphProgressMPHandler();
}
