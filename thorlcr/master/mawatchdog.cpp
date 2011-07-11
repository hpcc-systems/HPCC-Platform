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

//Master Watchdog Server/Monitor

#include "platform.h"
#include <stdio.h>
#include "jlib.hpp"
#include "jmisc.hpp"
#include "thormisc.hpp"


#include "thgraphmanager.hpp"
#include "thwatchdog.hpp"
#include "mawatchdog.hpp"
#include "thcompressutil.hpp"
#include "thmastermain.hpp"
#include "thexception.hpp"
#include "thdemonserver.hpp"
#include "thgraphmaster.hpp"
#include "thorport.hpp"

#define DEFAULT_SLAVEDOWNTIMEOUT (60*5)
class CMachineStatus
{
public:
    SocketEndpoint ep;
    bool alive;
    bool markdead;
    HeartBeatPacket lastpacket;
    CMachineStatus(const SocketEndpoint &_ep)
        : ep(_ep)
    {
        alive = true;
        markdead = false;
        memset(&lastpacket,0,sizeof(lastpacket));
    }
    void update(HeartBeatPacket &packet)
    {
        alive = true;
        if (markdead) {
            markdead = false;
            StringBuffer epstr;
            ep.getUrlStr(epstr);
            LOG(MCdebugProgress, unknownJob, "Watchdog : Marking Machine as Up! [%s]", epstr.str());
        }
        if(packet.progressSize > 0)
            lastpacket = packet;
    }   
};


CMasterWatchdog::CMasterWatchdog() : threaded("CMasterWatchdog")
{
    stopped = false;
    sock = NULL;
    if (globals->getPropBool("@watchdogEnabled"))
    {
        if (!sock)
            sock = ISocket::udp_create(getFixedPort(TPORT_watchdog));
        LOG(MCdebugProgress, unknownJob, "Starting watchdog");
#ifdef _WIN32
        threaded.adjustPriority(+1); // it is critical that watchdog packets get through.
#endif
        threaded.init(this);
    }
}

CMasterWatchdog::~CMasterWatchdog()
{
    stop();
    ::Release(sock);
    ForEachItemInRev(i, state) {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        delete mstate;
    }
}

void CMasterWatchdog::addSlave(const SocketEndpoint &slave)
{
    synchronized block(mutex);
    CMachineStatus *mstate=new CMachineStatus(slave);
    state.append(mstate);
}

void CMasterWatchdog::removeSlave(const SocketEndpoint &slave)
{
    synchronized block(mutex);
    CMachineStatus *ms = findSlave(slave);
    if (ms) {
        state.zap(ms);
        delete ms;
    }
}

CMachineStatus *CMasterWatchdog::findSlave(const SocketEndpoint &ep)
{
    ForEachItemInRev(i, state) {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        if (mstate->ep.equals(ep))
            return mstate;
    }
    return NULL;
}


void CMasterWatchdog::stop()
{
    threaded.adjustPriority(0); // restore to normal before stopping
    { synchronized block(mutex);
        if (stopped)
            return;
        LOG(MCdebugProgress, unknownJob, "Stopping watchdog");
        stopped = true;
    }
    if (sock)
    {
        SocketEndpoint masterEp(getMasterPortBase());
        StringBuffer ipStr;
        masterEp.getIpText(ipStr);
        Owned<ISocket> sock = ISocket::udp_connect(getFixedPort(masterEp.port, TPORT_watchdog), ipStr.str());
        HeartBeatPacket hbpacket;
        memset(&hbpacket, 0, sizeof(hbpacket));
        MemoryBuffer mb;
        size32_t sz = ThorCompress(&hbpacket, hbpacket.packetSize(), mb);
        sock->write(mb.toByteArray(), sz);
        sock->close();
    }
    threaded.join();
    LOG(MCdebugProgress, unknownJob, "Stopped watchdog");
}

void CMasterWatchdog::checkMachineStatus()
{
    synchronized block(mutex);
    ForEachItemInRev(i, state) {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        if (!mstate->alive)
        {
            StringBuffer epstr;
            mstate->ep.getUrlStr(epstr);
            if (mstate->markdead)
                abortThor(MakeThorOperatorException(TE_AbortException, "Watchdog has lost contact with Thor slave: %s (Process terminated or node down?)", epstr.str()));
            else
            {
                mstate->markdead = true;
                LOG(MCdebugProgress, unknownJob, "Watchdog : Marking Machine as Down! [%s]", epstr.str());
                //removeSlave(mstate->ep); // more TBD
            }
        }
        else {
            mstate->alive = false;
        }
    }

}

void CMasterWatchdog::main()
{
    LOG(MCdebugProgress, unknownJob, "Started watchdog");
    unsigned lastbeat=msTick();
    unsigned lastcheck=lastbeat;

    unsigned watchdogMachineTimeout = globals->getPropInt("@slaveDownTimeout", DEFAULT_SLAVEDOWNTIMEOUT);
    if (watchdogMachineTimeout <= HEARTBEAT_INTERVAL*10)
        watchdogMachineTimeout = HEARTBEAT_INTERVAL*10;
    watchdogMachineTimeout *= 1000;
    retrycount = 0;
    try {
        while (!stopped) {
            HeartBeatPacket hbpacket;
            try {
                size32_t read;
                MemoryBuffer packetCompressedMb;
                sock->readtms(packetCompressedMb.reserveTruncate(hbpacket.maxPacketSize()), hbpacket.minPacketSize(), hbpacket.maxPacketSize(), read, watchdogMachineTimeout);
                MemoryBuffer packetMb;
                read = ThorExpand(packetCompressedMb.toByteArray(), read, &hbpacket, hbpacket.maxPacketSize());
                if (0==hbpacket.packetsize)
                    break; // signal to stop
                if(read > hbpacket.minPacketSize() && read == hbpacket.packetsize)  // check for corrupt packets
                {
                    synchronized block(mutex);
                    CMachineStatus *ms = findSlave(hbpacket.sender);
                    if (ms) 
                    {
                        ms->update(hbpacket);
                        Owned<IJobManager> jobManager = getJobManager();
                        if (jobManager)
                            jobManager->queryDeMonServer()->takeHeartBeat(hbpacket);
                    }
                    else {
                        StringBuffer epstr;
                        hbpacket.sender.getUrlStr(epstr);
                        LOG(MCdebugProgress, unknownJob, "Watchdog : Unknown Machine! [%s]", epstr.str()); //TBD
                    }
                }
                else
                {
                    LOG(MCdebugProgress, unknownJob, "Receive Monitor Packet: wrong size, expected %d, got %d", hbpacket.packetsize, read);
                }
            }
            catch (IJSOCK_Exception *e)
            {
                if ((e->errorCode()!=JSOCKERR_timeout_expired)&&(e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
                    throw;
                e->Release();
            }
            if (stopped)
                break;
            unsigned now=msTick();
            if (now-lastcheck>watchdogMachineTimeout) {
                checkMachineStatus();
                lastcheck = msTick();
            }
            if (now-lastbeat>THORBEAT_INTERVAL) {
                if (retrycount<=0) retrycount=THORBEAT_RETRY_INTERVAL; else retrycount -= THORBEAT_INTERVAL;
                lastbeat = msTick();
            }
        }
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), thorJob, e,"Watchdog Server Exception");
        e->Release();
    }
}

