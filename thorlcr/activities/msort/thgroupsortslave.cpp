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


class CLocalSortSlaveActivity : public CSlaveActivity 
{
    typedef CSlaveActivity PARENT;

    IHThorSortArg *helper;
    ICompare *iCompare;
    Owned<IThorRowLoader> iLoader;
    Owned<IRowStream> out;
    bool unstable, eoi = false;
    CriticalSection loaderCs; // Ensure iLoader remains valid for the duration of mergeStats()

public:
    CLocalSortSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container, sortActivityStatistics)
    {
        helper = (IHThorSortArg *)queryHelper();
        iCompare = helper->queryCompare();
        unstable = helper->getAlgorithmFlags()&TAFunstable;
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        unsigned spillPriority = container.queryGrouped() ? SPILL_PRIORITY_GROUPSORT : SPILL_PRIORITY_LARGESORT;
        iLoader.setown(createThorRowLoader(*this, queryRowInterfaces(input), iCompare, unstable ? stableSort_none : stableSort_earlyAlloc, rc_mixed, spillPriority));
        eoi = false;
        if (container.queryGrouped())
            out.setown(iLoader->loadGroup(inputStream, abortSoon));
        else
            out.setown(iLoader->load(inputStream, abortSoon));
        if (0 == iLoader->numRows())
            eoi = true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        {
            CriticalBlock block(loaderCs);
            mergeStats(stats, iLoader, spillStatistics);
        }
        PARENT::serializeStats(mb);
    }

    virtual void stop()
    {
        out.clear();
        if (hasStarted())
        {
            {
                CriticalBlock block(loaderCs);
                mergeStats(stats, iLoader, spillStatistics);
            }
            iLoader.clear();
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
            out.setown(iLoader->loadGroup(inputStream, abortSoon));
            if (0 == iLoader->numRows())
                eoi = true;
            return NULL; // eog marker
        }
        dataLinkIncrement();
        return row.getClear();
    }
    virtual bool isGrouped() const override { return container.queryGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};

// Sorted 

class CSortedSlaveActivity : public CSlaveActivity, public CThorSteppable
{
    typedef CSlaveActivity PARENT;

    IHThorSortedArg *helper;
    ICompare *icompare;
    OwnedConstThorRow prev; 

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CSortedSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorSteppable(this)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow ret = inputStream->nextRow();
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
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    virtual const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow ret = inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
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
    virtual bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        return input->gatherConjunctions(collector);
    }
    virtual void resetEOF()
    { 
        inputStream->resetEOF();
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, queryInput(0));
    }
// steppable
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override
    {
        CSlaveActivity::setInputStream(index, input, consumerOrdered);
        CThorSteppable::setInputStream(index, input, consumerOrdered);
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


