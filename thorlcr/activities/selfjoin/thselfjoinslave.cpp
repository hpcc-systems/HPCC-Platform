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
    ICompare * compare;
    ISortKeySerializer * keyserializer;
    mptag_t mpTagRPC;
    Owned<IJoinHelper> joinhelper;
    CriticalSection joinHelperCrit;
    Owned<IBarrier> barrier;
    SocketEndpoint server;

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
        Owned<IThorRowLoader> iLoader = createThorRowLoader(*this, ::queryRowInterfaces(input), compare, isUnstable() ? stableSort_none : stableSort_earlyAlloc, rc_mixed, SPILL_PRIORITY_SELFJOIN);
        Owned<IRowStream> rs = iLoader->load(input, abortSoon);
        stopInput(input);
        input = NULL;
        return rs.getClear();
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
        mpTagRPC = TAG_NULL;
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
        if(!isLocal)
        {
            mpTagRPC = container.queryJobChannel().deserializeMPTag(data);
            mptag_t barrierTag = container.queryJobChannel().deserializeMPTag(data);
            barrier.setown(container.queryJobChannel().createBarrier(barrierTag));
            portbase = allocPort(NUMSLAVEPORTS);
            server.setLocalHost(portbase);
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
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
    virtual void reset()
    {
        CSlaveActivity::reset();
        if (sorter) return; // JCSMORE loop - shouldn't have to recreate sorter between loop iterations
        if (!isLocal && TAG_NULL != mpTagRPC)
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
    }
    virtual void kill()
    {
        sorter.clear();
        CSlaveActivity::kill();
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
        bool hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, (JFreorderable & helper->getJoinFlags()) != 0);
        bool hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point

        if (helper->getJoinFlags()&JFlimitedprefixjoin) {
            CriticalBlock b(joinHelperCrit);
            // use std join helper (less efficient but implements limited prefix)
            joinhelper.setown(createJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
        }
        else
        {
            CriticalBlock b(joinHelperCrit);
            joinhelper.setown(createSelfJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
        }
        strm.setown(isLightweight? doLightweightSelfJoin() : (isLocal ? doLocalSelfJoin() : doGlobalSelfJoin()));
        assertex(strm);

        joinhelper->init(strm, NULL, ::queryRowAllocator(inputs.item(0)), ::queryRowAllocator(inputs.item(0)), ::queryRowMetaData(inputs.item(0)));
    }

    virtual void abort()
    {
        CSlaveActivity::abort();
        if (joinhelper)
            joinhelper->stop();
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
    
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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

