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


#include "jiface.hpp"
#include "slave.hpp"
#include "jsort.hpp"
#include "thbufdef.hpp"
#include "tsorta.hpp"

#include "commonext.hpp"
#include "thgroupsortslave.ipp"
#include "thactivityutil.ipp"

#include "jsort.hpp"
#include "thactivityutil.ipp"


class CLocalSortSlaveActivity : public CSlaveActivity, public CThorDataLink 
{
    IThorDataLink *input;
    IHThorSortArg *helper;
    ICompare *iCompare;
    Owned<IThorRowLoader> iLoader;
    Owned<IRowStream> out;
    bool unstable, eoi;
    CriticalSection statsCs;
    CRuntimeStatisticCollection spillStats;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalSortSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), spillStats(spillStatistics)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorSortArg *)queryHelper();
        iCompare = helper->queryCompare();
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        unstable = (algo&&(algo->getAlgorithmFlags()&TAFunstable));
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        dataLinkStart();
        input = inputs.item(0);
        unsigned spillPriority = container.queryGrouped() ? SPILL_PRIORITY_GROUPSORT : SPILL_PRIORITY_LARGESORT;
        iLoader.setown(createThorRowLoader(*this, queryRowInterfaces(input), iCompare, unstable ? stableSort_none : stableSort_earlyAlloc, rc_mixed, spillPriority));
        startInput(input);
        eoi = false;
        if (container.queryGrouped())
            out.setown(iLoader->loadGroup(input, abortSoon));
        else
            out.setown(iLoader->load(input, abortSoon));
        if (0 == iLoader->numRows())
            eoi = true;
    }
    void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);

        CriticalBlock block(statsCs);
        CRuntimeStatisticCollection mergedStats(spillStats);
        mergeStats(mergedStats, iLoader);
        mergedStats.serialize(mb);
    }

    void stop()
    {
        out.clear();
        stopInput(input);
        dataLinkStop();

        //Critical block
        {
            CriticalBlock block(statsCs);
            mergeStats(spillStats, iLoader);
            iLoader.clear();
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (abortSoon || eoi)
            return NULL;
        OwnedConstThorRow row = out->nextRow();
        if (!row)
        {
            if (!container.queryGrouped())
            {
                eoi = true;
                return NULL;
            }
            out.setown(iLoader->loadGroup(input, abortSoon));
            if (0 == iLoader->numRows())
                eoi = true;
            return NULL; // eog marker
        }
        dataLinkIncrement();
        return row.getClear();
    }
    virtual bool isGrouped() { return container.queryGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};

// Sorted 

class CSortedSlaveActivity : public CSlaveActivity, public CThorDataLink, public CThorSteppable
{
    IThorDataLink *input;
    IHThorSortedArg *helper;
    ICompare *icompare;
    OwnedConstThorRow prev; 

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSortedSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), CThorSteppable(this)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        dataLinkStart();
        input = inputs.item(0);
        startInput(input);
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = input->nextRow();
        if (ret && prev && icompare->docompare(prev, ret) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %" RCPF "d,  %" RCPF "d))", getDataLinkCount(), getDataLinkCount()+1);
        }
        prev.set(ret);
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret && prev && stepCompare->docompare(prev, ret, numFields) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %" RCPF "d,  %" RCPF "d))", getDataLinkCount(), getDataLinkCount()+1);
        }
        prev.set(ret);
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        return input->gatherConjunctions(collector);
    }
    void resetEOF() 
    { 
        input->resetEOF(); 
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CSlaveActivity::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};


CActivityBase *createLocalSortSlave(CGraphElementBase *container)
{
    return new CLocalSortSlaveActivity(container);
}

CActivityBase *createSortedSlave(CGraphElementBase *container)
{
    return new CSortedSlaveActivity(container);
}


