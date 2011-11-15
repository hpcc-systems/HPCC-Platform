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

class CGraphProgressHandler : public CSimpleInterface, implements ISlaveWatchdog, implements IThreaded
{
    CriticalSection crit;
    CGraphArray activeGraphs;
    bool stopped, progressEnabled;
    Owned<ISocket> sock;
    CThreaded threaded;
    SocketEndpoint self;

    void sendData()
    {
        HeartBeatPacket hbpacket;
        gatherData(hbpacket);
        if(hbpacket.packetsize > 0)
        {
            MemoryBuffer mb;
            size32_t sz = ThorCompress(&hbpacket,hbpacket.packetsize, mb, 0x200);
            sock->write(mb.toByteArray(), sz);
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CGraphProgressHandler() : threaded("CGraphProgressHandler")
    {
        self = queryMyNode()->endpoint();
        self.port -= THOR_MP_INC;
        stopped = true;

        StringBuffer ipStr;
        queryClusterGroup().queryNode(0).endpoint().getIpText(ipStr);
        sock.setown(ISocket::udp_connect(getFixedPort(getMasterPortBase(), TPORT_watchdog),ipStr.str()));
        progressEnabled = globals->getPropBool("@watchdogProgressEnabled");
        sendData();                         // send initial data
        stopped = false;
#ifdef _WIN32
        threaded.adjustPriority(+1); // it is critical that watchdog packets get through.
#endif
        threaded.init(this);
    }
    ~CGraphProgressHandler()
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

    void gatherData(HeartBeatPacket &hb)
    {
        CriticalBlock b(crit);
        hb.sender = self;
        hb.progressSize = 0;
        if (progressEnabled)
        {
            CriticalBlock b(crit);
            MemoryBuffer mb;
            mb.setBuffer(DATA_MAX, hb.perfdata);
            mb.rewrite();
            ForEachItemIn(g, activeGraphs)
            {
                CGraphBase &graph = activeGraphs.item(g);
                graph.serializeStats(mb);
                if (mb.length() > (DATA_MAX-30))
                {
                    WARNLOG("Progress packet too big!");
                    break;
                }
            }
            hb.progressSize = mb.length();
        }
        hb.tick++;
        hb.packetsize = hb.packetSize();
    }

// ISlaveWatchdog impl.
    void startGraph(CGraphBase &graph)
    {
        CriticalBlock b(crit);
        activeGraphs.append(*LINK(&graph));
        StringBuffer str("Watchdog: Start Job ");
        LOG(MCdebugProgress, thorJob, "%s", str.append(graph.queryGraphId()).str());
    }
    void stopGraph(CGraphBase &graph, HeartBeatPacket *hb)
    {
        CriticalBlock b(crit);
        if (NotFound != activeGraphs.find(graph))
        {
            StringBuffer str("Watchdog: Stop Job ");
            LOG(MCdebugProgress, thorJob, "%s", str.append(graph.queryGraphId()).str());
            if (hb)
                gatherData(*hb);
            activeGraphs.zap(graph);
        }
    }

// IThreaded
    void main()
    {
        LOG(MCdebugProgress, thorJob, "Watchdog: thread running");
        assertex(HEARTBEAT_INTERVAL>=8);
        unsigned count = HEARTBEAT_INTERVAL+getRandom()%8-4;
        while (!stopped)
        {
            Sleep(1000);
            if (count--==0)
            {
                sendData();
                count = HEARTBEAT_INTERVAL+getRandom()%8-4;         
            }
        }
    }
};

ISlaveWatchdog *createProgressHandler()
{
    return new CGraphProgressHandler();
}

