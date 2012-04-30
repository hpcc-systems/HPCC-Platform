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

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalSortSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorSortArg *)queryHelper();
        iCompare = helper->queryCompare();
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        unstable = (algo&&algo->getAlgorithmFlags()&TAFunstable);
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart(activityKindStr(queryContainer().getKind()), container.queryId());
        input = inputs.item(0);
        unsigned spillPriority = container.queryGrouped() ? SPILL_PRIORITY_GROUPSORT : SPILL_PRIORITY_LARGESORT;
        iLoader.setown(createThorRowLoader(*this, queryRowInterfaces(input), iCompare, !unstable, rc_mixed, spillPriority));
        startInput(input);
        eoi = false;
        if (container.queryGrouped())
            out.setown(iLoader->loadGroup(input, abortSoon));
        else
            out.setown(iLoader->load(input, abortSoon));
        if (0 == iLoader->numRows())
            eoi = true;
    }
    void stop()
    {
        out.clear();
        stopInput(input);
        dataLinkStop();
        iLoader.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
            {
                eoi = true;
                return NULL;
            }
            row.setown(out->nextRow());
            if (!row)
                return NULL;
        }
        dataLinkIncrement();
        return row.getClear();
    }
    virtual bool isGrouped() { return false; }
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

    IRangeCompare *stepCompare;
    OwnedConstThorRow prev; 

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSortedSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), CThorSteppable(this)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        stepCompare = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("SORTED", container.queryId());
        input = inputs.item(0);
        startInput(input);
        IInputSteppingMeta *stepMeta = input->querySteppingMeta();
        if (stepMeta)
            stepCompare = stepMeta->queryCompare();
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow ret = input->nextRow();
        if (ret && prev && icompare->docompare(prev, ret) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %"RCPF"d,  %"RCPF"d))", getDataLinkCount(), getDataLinkCount()+1);
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret && prev && stepCompare->docompare(prev, ret, numFields) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %"RCPF"d,  %"RCPF"d))", getDataLinkCount(), getDataLinkCount()+1);
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


