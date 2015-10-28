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

class CGraphProgressHandlerBase : public CSimpleInterface, implements ISlaveWatchdog, implements IThreaded
{
    CriticalSection crit;
    CGraphArray activeGraphs;
    bool stopped, progressEnabled;
    CThreaded threaded;
    SocketEndpoint self;

    void gatherAndSend()
    {
        MemoryBuffer sendMb, progressMb;
        HeartBeatPacketHeader hb;
        //clear struct, to avoid spurious warnings when serializing raw [unpacked] struct
        memset(&hb, 0, sizeof(HeartBeatPacketHeader));
        hb.sender = self;
        hb.tick++;
        size32_t progressSizePos = (byte *)&hb.progressSize - (byte *)&hb;
        sendMb.append(sizeof(HeartBeatPacketHeader), &hb);

        hb.progressSize = gatherData(progressMb);
        sendMb.writeDirect(progressSizePos, sizeof(hb.progressSize), &hb.progressSize);
        sendMb.append(progressMb);
        size32_t packetSize = sendMb.length();
        sendMb.writeDirect(0, sizeof(hb.packetSize), &packetSize);
        sendData(sendMb);
    }
    virtual void sendData(MemoryBuffer &mb) = 0;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CGraphProgressHandlerBase() : threaded("CGraphProgressHandler")
    {
        self = queryMyNode()->endpoint();
        stopped = true;

        progressEnabled = globals->getPropBool("@watchdogProgressEnabled");
        stopped = false;
#ifdef _WIN32
        threaded.adjustPriority(+1); // it is critical that watchdog packets get through.
#endif
        threaded.init(this);
    }
    ~CGraphProgressHandlerBase()
    {
        stop();
    }
    virtual void stop()
    {
#ifdef _WIN32
        threaded.adjustPriority(0); // restore to normal before stopping
#endif
        stopped = true;
        threaded.join();
        LOG(MCdebugProgress, thorJob, "Stopped watchdog");
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
                    graph.serializeStats(progressData);
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
    void startGraph(CGraphBase &graph)
    {
        CriticalBlock b(crit);
        activeGraphs.append(*LINK(&graph));
        StringBuffer str("Watchdog: Start Job ");
        LOG(MCdebugProgress, thorJob, "%s", str.append(graph.queryGraphId()).str());
    }
    void stopGraph(CGraphBase &graph, MemoryBuffer *mb)
    {
        CriticalBlock b(crit);
        if (NotFound != activeGraphs.find(graph))
        {
            StringBuffer str("Watchdog: Stop Job ");
            LOG(MCdebugProgress, thorJob, "%s", str.append(graph.queryGraphId()).str());
            if (mb)
            {
                DelayedSizeMarker sizeMark(*mb);
                gatherData(*mb);
                sizeMark.write();
            }
            activeGraphs.zap(graph);
        }
    }

// IThreaded
    void main()
    {
        LOG(MCdebugProgress, thorJob, "Watchdog: thread running");
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
        queryMasterNode().endpoint().getIpText(ipStr);
        sock.setown(ISocket::udp_connect(getFixedPort(getMasterPortBase(), TPORT_watchdog),ipStr.str()));
    }
    virtual void sendData(MemoryBuffer &mb)
    {
        HeartBeatPacketHeader hb;
        memcpy(&hb, mb.toByteArray(), sizeof(HeartBeatPacketHeader));
        if (hb.packetSize > UDP_DATA_MAX)
        {
            WARNLOG("Progress packet too big! progress lost");
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
    }
    virtual void sendData(MemoryBuffer &mb)
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
