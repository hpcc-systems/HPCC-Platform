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

class SelfJoinSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

private:
    Owned<IThorSorter> sorter;
    IHThorJoinArg * helper;
    unsigned portbase;
    bool isLocal;
    bool isLightweight;
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
        return false;
    }

    IRowStream * doLocalSelfJoin()
    {
        ::ActPrintLog(this, thorDetailedLogLevel, "SELFJOIN: Performing local self-join");
        Owned<IThorRowLoader> iLoader = createThorRowLoader(*this, ::queryRowInterfaces(input), compare, isUnstable() ? stableSort_none : stableSort_earlyAlloc, rc_mixed, SPILL_PRIORITY_SELFJOIN);
        Owned<IRowStream> rs = iLoader->load(inputStream, abortSoon);
        mergeStats(stats, iLoader, spillStatistics);  // Not sure of the best policy if rs spills later on.
        PARENT::stopInput(0);
        return rs.getClear();
    }

    IRowStream * doGlobalSelfJoin()
    {
        ::ActPrintLog(this, thorDetailedLogLevel, "SELFJOIN: Performing global self-join");
        sorter->Gather(::queryRowInterfaces(input), inputStream, compare, NULL, NULL, keyserializer, NULL, NULL, false, isUnstable(), abortSoon, NULL);
        PARENT::stopInput(0);
        if (abortSoon)
        {
            barrier->cancel();
            return NULL;
        }
        if (!barrier->wait(false))
        {
            Sleep(1000); // let original error through
            throw MakeThorException(TE_BarrierAborted,"SELFJOIN: Barrier Aborted");
        }
        rowcount_t totalrows;
        return sorter->startMerge(totalrows);
    }

public:
    SelfJoinSlaveActivity(CGraphElementBase *_container, bool _isLocal, bool _isLightweight)
        : CSlaveActivity(_container, joinActivityStatistics)
    {
        helper = static_cast <IHThorJoinArg *> (queryHelper());
        isLocal = _isLocal||_isLightweight;
        isLightweight = _isLightweight;
        portbase = 0;
        compare = NULL;
        keyserializer = NULL;
        mpTagRPC = TAG_NULL;
        if (isLocal)
            setRequireInitData(false);
        appendOutputLinked(this);
    }

    ~SelfJoinSlaveActivity()
    {
        if(portbase) 
            queryJobChannel().freePort(portbase, NUMSLAVEPORTS);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {       
        if (!isLocal)
        {
            mpTagRPC = container.queryJobChannel().deserializeMPTag(data);
            mptag_t barrierTag = container.queryJobChannel().deserializeMPTag(data);
            barrier.setown(container.queryJobChannel().createBarrier(barrierTag));
            portbase = queryJobChannel().allocPort(NUMSLAVEPORTS);
            server.setLocalHost(portbase);
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
            server.serialize(slaveData);
        }
        compare = helper->queryCompareLeft();                   // NB not CompareLeftRight
        keyserializer = helper->querySerializeLeft();           // hopefully never need right
        if (isLightweight) 
            ::ActPrintLog(this, thorDetailedLogLevel, "SELFJOIN: LIGHTWEIGHT");
        else if(isLocal) 
            ::ActPrintLog(this, thorDetailedLogLevel, "SELFJOIN: LOCAL");
        else
            ::ActPrintLog(this, thorDetailedLogLevel, "SELFJOIN: GLOBAL");
    }
    virtual void reset() override
    {
        PARENT::reset();
        if (sorter) return; // JCSMORE loop - shouldn't have to recreate sorter between loop iterations
        if (!isLocal && TAG_NULL != mpTagRPC)
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
    }
    virtual void kill() override
    {
        sorter.clear();
        if (portbase)
        {
            queryJobChannel().freePort(portbase, NUMSLAVEPORTS);
            portbase = 0;
        }
        PARENT::kill();
    }

// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        bool hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, (JFreorderable & helper->getJoinFlags()) != 0);
        bool hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point

        if (helper->getJoinFlags()&JFlimitedprefixjoin)
        {
            CriticalBlock b(joinHelperCrit);
            // use std join helper (less efficient but implements limited prefix)
            joinhelper.setown(createJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
        }
        else
        {
            CriticalBlock b(joinHelperCrit);
            joinhelper.setown(createSelfJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
        }
        if (isLightweight)
            strm.set(inputStream);
        else
        {
            strm.setown(isLocal ? doLocalSelfJoin() : doGlobalSelfJoin());
            assertex(strm);
            // NB: PARENT::stopInput(0) will now have been called
        }

        joinhelper->init(strm, NULL, ::queryRowAllocator(queryInput(0)), ::queryRowAllocator(queryInput(0)), ::queryRowMetaData(queryInput(0)));
    }

    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (joinhelper)
            joinhelper->stop();
    }
    virtual void stop() override
    {
        if (hasStarted())
        {
            if (!isLocal)
            {
                barrier->wait(false);
                sorter->stopMerge();
            }
            {
                CriticalBlock b(joinHelperCrit);
                joinhelper.clear();
            }
            if (strm)
            {
                if (!isLightweight) // if lightweight strm=input and PARENT::stop handles input stop
                    strm->stop();
                strm.clear();
            }
        }
        PARENT::stop();
    }
    
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if(joinhelper) {
            OwnedConstThorRow row = joinhelper->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }

    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true; 
        info.unknownRowsOutput = true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        {
            CriticalBlock b(joinHelperCrit);
            rowcount_t p = joinhelper?joinhelper->getLhsProgress():0;
            stats.setStatistic(StNumLeftRows, p);
            mergeStats(stats, sorter, spillStatistics);    // No danger of a race with reset() because that never replaces a valid sorter
        }
        CSlaveActivity::serializeStats(mb);
    }
};



CActivityBase *createSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, false, false); }

CActivityBase *createLocalSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, true, false); }

CActivityBase *createLightweightSelfJoinSlave(CGraphElementBase *container) { return new SelfJoinSlaveActivity(container, true, true); }

