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


#include "jlib.hpp"

#include "thselfjoinslave.ipp"
#include "tsorts.hpp"
#include "tsorta.hpp"
#include "thactivityutil.ipp"
#include "thorport.hpp"
#include "thsortu.hpp"
#include "thsortu.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thorxmlwrite.hpp"

#define NUMSLAVEPORTS 2     // actually should be num MP tags

class SelfJoinSlaveActivity : public CSlaveActivity, public CThorDataLink
{
private:
    Owned<IThorSorter> sorter;
    IHThorJoinArg * helper;
    unsigned portbase;
    bool isLocal;
    bool isLightweight;
    bool inputStopped;
    IThorDataLink * input;
    Owned<IRowStream> strm;     
    CThorRowArray rows;
    Owned<IThorRowSortedLoader> iloader;
    ICompare * compare;
    ISortKeySerializer * keyserializer;
    mptag_t mpTagRPC;
    Owned<IJoinHelper> joinhelper;
    CriticalSection joinHelperCrit;
    Owned<IBarrier> barrier;

    bool isUnstable()
    {
        // actually don't think currently supported by join but maybe will be sometime
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        return (algo&&algo->getAlgorithmFlags()&TAFunstable);
    }

    
    IRowStream * doLocalSelfJoin()
    {
#if THOR_TRACE_LEVEL > 5
        ActPrintLog("SELFJOIN: Performing local self-join");
#endif
        iloader.setown(createThorRowSortedLoader(rows));
        bool isempty;
        IRowStream *rs = iloader->load(input,::queryRowInterfaces(input), compare, false, abortSoon, isempty, "SELFJOIN", !isUnstable());
        stopInput(input);
        input = NULL;
        return rs;
    }

    IRowStream * doGlobalSelfJoin()
    {
#if THOR_TRACE_LEVEL > 5
        ActPrintLog("SELFJOIN: Performing global self-join");
#endif
        sorter->Gather(::queryRowInterfaces(input), input, compare, NULL, NULL, keyserializer, NULL, false, isUnstable(), abortSoon, NULL);
        stopInput(input);
        input = NULL;
        if(abortSoon)
        {
            barrier->cancel();
            return NULL;
        }
        if (!barrier->wait(false)) {
            Sleep(1000); // let original error through
            throw MakeThorException(TE_BarrierAborted,"SELFJOIN: Barrier Aborted");
        }
        rowcount_t totalrows;
        return sorter->startMerge(totalrows);
    }

    IRowStream * doLightweightSelfJoin()
    {
        IRowStream *ret = LINK(input);
        input = NULL;
        return ret;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SelfJoinSlaveActivity(CGraphElementBase *_container, bool _isLocal, bool _isLightweight)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        isLocal = _isLocal||_isLightweight;
        isLightweight = _isLightweight;
        input = NULL;
        portbase = 0;
        compare = NULL;
        keyserializer = NULL;
        inputStopped = false;
    }

    ~SelfJoinSlaveActivity()
    {
        if(portbase) 
            freePort(portbase,NUMSLAVEPORTS);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {       
        appendOutputLinked(this);
        if(!isLocal) {
            mpTagRPC = container.queryJob().deserializeMPTag(data);
            mptag_t barrierTag = container.queryJob().deserializeMPTag(data);
            barrier.setown(container.queryJob().createBarrier(barrierTag));
            portbase = allocPort(NUMSLAVEPORTS);
            SocketEndpoint server;
            server.setLocalHost(portbase);
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&container.queryJob().queryJobComm(),mpTagRPC));
            server.serialize(slaveData);
        }
        helper = static_cast <IHThorJoinArg *> (queryHelper());
        compare = helper->queryCompareLeft();                   // NB not CompareLeftRight
        keyserializer = helper->querySerializeLeft();           // hopefully never need right
        if(isLightweight) 
            ActPrintLog("SELFJOIN: LIGHTWEIGHT");
        else if(isLocal) 
            ActPrintLog("SELFJOIN: LOCAL");
        else
            ActPrintLog("SELFJOIN: GLOBAL");
    }


// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("SELFJOIN", container.queryId());
        bool hintparallelmatch = container.queryXGMML().getPropInt("hint[@name=\"parallel_match\"]")!=0;
        bool hintunsortedoutput = container.queryXGMML().getPropInt("hint[@name=\"unsorted_output\"]")!=0;

        if (helper->getJoinFlags()&JFlimitedprefixjoin) {
            CriticalBlock b(joinHelperCrit);
            // use std join helper (less efficient but implements limited prefix)
            joinhelper.setown(createJoinHelper(helper,"SELFJOIN", container.queryId(), queryRowAllocator(),hintparallelmatch,hintunsortedoutput));
        }
        else {
            CriticalBlock b(joinHelperCrit);
            joinhelper.setown(createSelfJoinHelper(helper,"SELFJOIN", container.queryId(), queryRowAllocator(),hintparallelmatch,hintunsortedoutput));
        }
        strm.setown(isLightweight? doLightweightSelfJoin() : (isLocal ? doLocalSelfJoin() : doGlobalSelfJoin()));
        assertex(strm);

        joinhelper->init(strm, NULL, ::queryRowAllocator(inputs.item(0)), ::queryRowAllocator(inputs.item(0)), ::queryRowMetaData(inputs.item(0)), &abortSoon, this);
    }

    virtual void stop()
    {
        if (input)
        {
            stopInput(input);
            input = NULL;
        }
        if(!isLocal)
        {
            barrier->wait(false);
            sorter->stopMerge();
        }
        {
            CriticalBlock b(joinHelperCrit);
            joinhelper.clear();
        }
        strm->stop();
        strm.clear();
        dataLinkStop();
    }

    virtual void kill()
    {
        rows.clear();
        CSlaveActivity::kill();
    }
    
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if(joinhelper) {
            OwnedConstThorRow row = joinhelper->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }

    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true; 
        info.unknownRowsOutput = true;
    }
    virtual void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        CriticalBlock b(joinHelperCrit);
        rowcount_t p = joinhelper?joinhelper->getLhsProgress():0;
        mb.append(p);
    }
};



CActivityBase *createSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, false, false); }

CActivityBase *createLocalSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, true, false); }

CActivityBase *createLightweightSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, true, true); }

