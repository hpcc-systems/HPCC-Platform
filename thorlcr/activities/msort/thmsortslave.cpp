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



#include "platform.h"
#include "limits.h"
#include "slave.ipp"
#include "thmsortslave.ipp"
#include "thorport.hpp"
#include "jio.hpp"
#include "tsorts.hpp"
#include "thsortu.hpp"
#include "thactivityutil.ipp"
#include "thexception.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/msort/thmsortslave.cpp $ $Id: thmsortslave.cpp 62376 2011-02-04 21:59:58Z sort $");


#define NUMSLAVEPORTS 2     // actually should be num MP tags


//--------------------------------------------------------------------------------------------
// MSortSlaveActivity
//


class MSortSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IThorDataLink *input;
    Owned<IRowStream> output;
    IHThorSortArg *helper;
    Owned<IThorSorter> sorter;
    unsigned portbase;
    rowcount_t totalrows;
    mptag_t mpTagRPC;
    Owned<IBarrier> barrier;
    SocketEndpoint server;

    bool isUnstable()
    {
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        return (algo&&algo->getAlgorithmFlags()&TAFunstable);
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    MSortSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        input = NULL;
        portbase = 0;
        totalrows = RCUNSET;
    }
    ~MSortSlaveActivity()
    {
        if (portbase) 
            freePort(portbase,NUMSLAVEPORTS);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        mpTagRPC = container.queryJob().deserializeMPTag(data);
        mptag_t barrierTag = container.queryJob().deserializeMPTag(data);
        barrier.setown(container.queryJob().createBarrier(barrierTag));
        portbase = allocPort(NUMSLAVEPORTS);
        ActPrintLog("MSortSlaveActivity::init portbase = %d, mpTagRPC = %d",portbase,(int)mpTagRPC);
        server.setLocalHost(portbase); 
        helper = (IHThorSortArg *)queryHelper();
        sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&container.queryJob().queryJobComm(),mpTagRPC));
        appendOutputLinked(this);
        server.serialize(slaveData);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        try
        {
            try { 
                startInput(input); 
            }
            catch (IException *e)
            {
                fireException(e);
                barrier->cancel();
                throw;
            }
            catch (CATCHALL)
            {
                Owned<IException> e = MakeActivityException(this, 0, "Unknown exception starting sort input");
                fireException(e);
                barrier->cancel();
                throw;
            }
            dataLinkStart("SORT", container.queryId());
            
            Linked<IRowInterfaces> rowif = queryRowInterfaces(input);
            Owned<IRowInterfaces> auxrowif = createRowInterfaces(helper->querySortedRecordSize(),queryActivityId(),queryCodeContext());
            sorter->Gather(
                rowif,
                input,
                helper->queryCompare(),
                helper->queryCompareLeftRight(),
                NULL,helper->querySerialize(),
                NULL,
                false,
                isUnstable(),
                abortSoon,
                auxrowif);
            stopInput(input);
            input = NULL;
            if (abortSoon)
            {
                ActPrintLogEx(&queryContainer(), thorlog_null, MCwarning, "MSortSlaveActivity::start aborting");
                barrier->cancel();
                return;
            }
        }
        catch (IException *e)
        {
            fireException(e);
            barrier->cancel();
            throw;
        }
        catch (CATCHALL)
        {
            Owned<IException> e = MakeActivityException(this, 0, "Unknown exception gathering sort input");
            fireException(e);
            barrier->cancel();
            throw;
        }
        ActPrintLog("SORT waiting barrier.1");
        if (!barrier->wait(false)) {
            Sleep(1000); // let original error through
            throw MakeThorException(TE_BarrierAborted,"SORT: Barrier Aborted");
        }
        ActPrintLog("SORT barrier.1 raised");
        output.setown(sorter->startMerge(totalrows));
    }
    void stop()
    {
        if (output) {
            output->stop();
            output.clear();
        }
        ActPrintLog("SORT waiting barrier.2");
        barrier->wait(false);
        ActPrintLog("SORT barrier.2 raised");
        if (input)
            stopInput(input);
        sorter->stopMerge();
        ActPrintLog("SORT waiting for merge");
        dataLinkStop();
    }
    void reset()
    {
        if (sorter) return; // JCSMORE loop - shouldn't have to recreate sorter between loop iterations
        sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&container.queryJob().queryJobComm(),mpTagRPC));
    }
    void kill()
    {
        ActPrintLog("MSortSlaveActivity::kill");
        sorter.clear();
        CSlaveActivity::kill();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon) 
            return NULL;
        OwnedConstThorRow row = output->nextRow();
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }

    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.unknownRowsOutput = false; // shuffles rows
        if (totalrows!=RCUNSET) { // NB totalrows not available until after start
            info.totalRowsMin = totalrows;
            info.totalRowsMax = totalrows;
        }
    }
};

CActivityBase *createMSortSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "MSortSlaveActivity::createMSortSlave");
    return new MSortSlaveActivity(container);
}


