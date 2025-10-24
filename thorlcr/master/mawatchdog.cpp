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

#define DEFAULT_WORKERDOWNTIMEOUT (60*5)
class CMachineStatus
{
public:
    SocketEndpoint ep;
    bool alive;
    bool markdead;
    unsigned workerNum;
    CMachineStatus(const SocketEndpoint &_ep, unsigned _workerNum)
        : ep(_ep), workerNum(_workerNum)
    {
        alive = true;
        markdead = false;
    }
    void update(HeartBeatPacketHeader &packet)
    {
        alive = true;
        if (markdead)
        {
            markdead = false;
            StringBuffer epstr;
            ep.getEndpointHostText(epstr);
            DBGLOG("Watchdog : Marking Machine as Up! [%s]", epstr.str());
        }
    }   
};


CMasterWatchdog::CMasterWatchdog(bool startNow) : threaded("CMasterWatchdogBase")
{
    stopped = true;
    watchdogMachineTimeout = globals->getPropInt("@slaveDownTimeout", DEFAULT_WORKERDOWNTIMEOUT);
    if (watchdogMachineTimeout <= HEARTBEAT_INTERVAL*10)
        watchdogMachineTimeout = HEARTBEAT_INTERVAL*10;
    watchdogMachineTimeout *= 1000;
    if (startNow)
        start();
}

CMasterWatchdog::~CMasterWatchdog()
{
    stop();
    ForEachItemInRev(i, state)
    {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        delete mstate;
    }
}

void CMasterWatchdog::start()
{
    if (stopped)
    {
        DBGLOG("Starting watchdog");
        stopped = false;
        threaded.init(this, false);
#ifdef _WIN32
        threaded.adjustPriority(+1); // it is critical that watchdog packets get through.
#endif
    }
}

void CMasterWatchdog::addWorker(const SocketEndpoint &worker, unsigned workerNum)
{
    synchronized block(mutex);
    CMachineStatus *mstate=new CMachineStatus(worker, workerNum);
    state.append(mstate);
}

void CMasterWatchdog::removeWorker(const SocketEndpoint &worker)
{
    synchronized block(mutex);
    CMachineStatus *ms = findWorker(worker);
    if (ms) {
        state.zap(ms);
        delete ms;
    }
}

CMachineStatus *CMasterWatchdog::findWorker(const SocketEndpoint &ep)
{
    ForEachItemInRev(i, state)
    {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        if (mstate->ep.equals(ep))
            return mstate;
    }
    return NULL;
}


void CMasterWatchdog::stop()
{
    {
        synchronized block(mutex);
        if (stopped)
            return;
        stopped = true;
    }

    DBGLOG("Stopping watchdog");
#ifdef _WIN32
    threaded.adjustPriority(0); // restore to normal before stopping
#endif
    stopReading();
    threaded.join();
    DBGLOG("Stopped watchdog");
}

void CMasterWatchdog::checkMachineStatus()
{
    synchronized block(mutex);
    ForEachItemInRev(i, state)
    {
        CMachineStatus *mstate=(CMachineStatus *)state.item(i);
        if (!mstate->alive)
        {
            StringBuffer epstr;
            mstate->ep.getEndpointHostText(epstr);
            if (mstate->markdead)
            {
                Owned<IThorException> te = MakeThorOperatorException(TE_AbortException, "Watchdog has lost contact with Thor worker: %s (Process terminated or node down?)", epstr.str());
                te->setSlave(mstate->workerNum+1);
                abortThor(te, TEC_Watchdog);
            }
            else
            {
                mstate->markdead = true;
                Owned<IThorException> te = MakeThorOperatorException(TE_AbortException, "Watchdog : Marking Machine as Down!");
                te->setSlave(mstate->workerNum+1);
                te->setAction(tea_warning);
                DBGLOG(te);
                Owned<IJobManager> jM = getJobManager();
                if (jM)
                    jM->fireException(te);
                //removeWorker(mstate->ep); // more TBD
            }
        }
        else {
            mstate->alive = false;
        }
    }
}

unsigned CMasterWatchdog::readPacket(HeartBeatPacketHeader &hb, MemoryBuffer &mb)
{
    mb.clear();
    unsigned read = readData(mb);
    if (read)
    {
        hb.deserialize(mb);
        if (read != hb.packetSize)  // check for corrupt packets
        {
            IWARNLOG("Receive Monitor Packet: wrong size, expected %d, got %d", hb.packetSize, read);
            return 0;
        }
        mb.setLength(hb.packetSize);
        return hb.packetSize;
    }
    else
        mb.clear();
    return 0;
}

unsigned CMasterWatchdog::readData(MemoryBuffer &mb)
{
    CMessageBuffer msg;
    if (!queryNodeComm().recv(msg, RANK_ALL, MPTAG_THORWATCHDOG, NULL, watchdogMachineTimeout))
        return 0;
    mb.swapWith(msg);
    return mb.length();
}
void CMasterWatchdog::stopReading()
{
    queryNodeComm().cancel(0, MPTAG_THORWATCHDOG);
}

void CMasterWatchdog::threadmain()
{
    DBGLOG("Started watchdog");
    unsigned lastbeat=msTick();
    unsigned lastcheck=lastbeat;

    retrycount = 0;
    while (!stopped)
    {
        try
        {
            HeartBeatPacketHeader hb;
            MemoryBuffer progressData;
            unsigned sz = readPacket(hb, progressData);
            if (stopped)
                break;
            else if (sz)
            {
                synchronized block(mutex);
                CMachineStatus *ms = findWorker(hb.sender);
                if (ms)
                {
                    ms->update(hb);
                    if (progressData.remaining())
                    {
                        Owned<IJobManager> jobManager = getJobManager();
                        if (jobManager)
                            jobManager->queryDeMonServer()->takeHeartBeat(progressData);
                    }
                }
                else
                {
                    StringBuffer epstr;
                    hb.sender.getEndpointHostText(epstr);
                    DBGLOG("Watchdog : Unknown Machine! [%s]", epstr.str()); //TBD
                }
            }
            unsigned now=msTick();
            if (now-lastcheck>watchdogMachineTimeout)
            {
                checkMachineStatus();
                lastcheck = msTick();
            }
            if (now-lastbeat>THORBEAT_INTERVAL)
            {
                if (retrycount<=0) retrycount=THORBEAT_RETRY_INTERVAL; else retrycount -= THORBEAT_INTERVAL;
                lastbeat = msTick();
            }
        }
        catch (IMP_Exception *e)
        {
            if (MPERR_link_closed != e->errorCode())
            {
                FLLOG(MCexception(e), e,"Watchdog Server Exception");
                e->Release();
            }
            else
            {
                const SocketEndpoint &ep = e->queryEndpoint();
                StringBuffer epStr;
                ep.getEndpointHostText(epStr);
                unsigned worker = NotFound;
                {
                    synchronized block(mutex);
                    CMachineStatus *ms = findWorker(ep);
                    if (ms)
                        worker = ms->workerNum;
                }
                Owned<IThorException> te = MakeThorOperatorException(TE_AbortException, "Watchdog has lost connectivity with Thor worker %u [%s] (Process terminated or node down?)", worker+1, epStr.str());
                te->setSlave(worker+1);
                abortThor(te, TEC_Watchdog);
            }
        }
        catch (IException *e)
        {
            FLLOG(MCexception(e), e,"Watchdog Server Exception");
            e->Release();
            // NB: it is important to continue with master watchdog, to continue to consume packets from workers
        }
    }
}

/////////////////////

CMasterWatchdog *createMasterWatchdog(bool startNow)
{
    return new CMasterWatchdog(startNow);
}
