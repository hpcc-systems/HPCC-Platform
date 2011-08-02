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

#include "thfilterslave.ipp"

class CFilterSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    bool anyThisGroup;
    IThorDataLink * input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFilterSlaveActivityBase(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        input = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start(const char *act)
    {   
        input = inputs.item(0);
        anyThisGroup = false;
        startInput(input);
        dataLinkStart(act, container.queryId());
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    virtual const void *nextRow()=0;
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
};


class CFilterSlaveActivity : public CFilterSlaveActivityBase, public CThorSteppable
{
    IHThorFilterArg *helper;
    unsigned matched;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFilterSlaveActivity(CGraphElementBase *container)
        : CFilterSlaveActivityBase(container), CThorSteppable(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFilterSlaveActivityBase::init(data,slaveData);
        helper = static_cast <IHThorFilterArg *> (queryHelper());
    }
    void start()
    {   
        ActivityTimer s(totalCycles, timeActivities, NULL);
        matched = 0;
        abortSoon = !helper->canMatchAny();
        CFilterSlaveActivityBase::start("FILTER");
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while(!abortSoon)
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                if(anyThisGroup)
                {
                    anyThisGroup = false;
                    break;
                }
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            if(helper->isValid(row))
            {
                matched++;
                anyThisGroup = true;
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon)
        {
            OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (!ret)
            {
                abortSoon = true;
                return NULL;
            }
            if (!wasCompleteMatch)
            {
                anyThisGroup = false; // RKC->GH - is this right??
                return ret.getClear();
            }
            if (helper->isValid(ret))
            {
                anyThisGroup = true;
                dataLinkIncrement();
                return ret.getClear();
            }
            if (!stepExtra.returnMismatches())
                return nextRow();
            if (stepCompare->docompare(ret, seek, numFields) != 0)
            {
                wasCompleteMatch = false;
                anyThisGroup = false; // WHY?
                return ret.getClear();
            }
        }
        return NULL;
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector) 
    { 
        return input->gatherConjunctions(collector); 
    }
    void resetEOF() 
    { 
        abortSoon = !helper->canMatchAny();
        anyThisGroup = false;
        input->resetEOF(); 
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CFilterSlaveActivityBase::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};

class CFilterProjectSlaveActivity : public CFilterSlaveActivityBase
{
    IHThorFilterProjectArg *helper;
    rowcount_t recordCount;  // NB local (not really used for global)
    Owned<IEngineRowAllocator> allocator;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFilterProjectSlaveActivity(CGraphElementBase *container) 
        : CFilterSlaveActivityBase(container)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFilterSlaveActivityBase::init(data,slaveData);
        helper = static_cast <IHThorFilterProjectArg *> (queryHelper());
        allocator.set(queryRowAllocator());
    }
    void start()
    {   
        ActivityTimer s(totalCycles, timeActivities, NULL);
        abortSoon = !helper->canMatchAny();
        recordCount = 0;
        CFilterSlaveActivityBase::start("FILTERPROJECT");
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon)
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row) {
                recordCount = 0;
                if (!anyThisGroup)
                {
                    row.setown(input->nextRow());
                    if (!row)
                    {
                        abortSoon = true;
                        break;
                    }
                }
            }
            if (!row)
                break;
            RtlDynamicRowBuilder ret(allocator);
            size32_t sz;
            try {
                sz = helper->transform(ret, row, ++recordCount);
            }
            catch (IException *e) 
            { 
                ActPrintLog(e, "helper->transform() error"); 
                throw; 
            }
            catch (CATCHALL)
            { 
                ActPrintLog("FILTERPROJECT: Unknown exception in helper->transform()"); 
                throw;
            }
            if (sz) {
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(sz);
            }
        }
        anyThisGroup = false;
        return NULL;
    }
};

class CFilterGroupSlaveActivity : public CFilterSlaveActivityBase, public CThorSteppable
{
    unsigned nextIndex;
    CThorRowArray group;
    IHThorFilterGroupArg *helper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFilterGroupSlaveActivity(CGraphElementBase *container) : CFilterSlaveActivityBase(container), CThorSteppable(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFilterSlaveActivityBase::init(data,slaveData);
        helper = (IHThorFilterGroupArg *)queryHelper();
    }
    void start()
    {   
        ActivityTimer s(totalCycles, timeActivities, NULL);
        abortSoon = !helper->canMatchAny();
        nextIndex = 0;
        CFilterSlaveActivityBase::start("FILTERGROUP");
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon)
        {
            if (group.ordinality())
            {
                if (nextIndex < group.ordinality())
                {
                    OwnedConstThorRow itm = group.itemClear(nextIndex++);
                    dataLinkIncrement();
                    return itm.getClear();
                }
                nextIndex = 0;
                group.clear();
                return NULL;
            }
            unsigned num = group.load(*input, false);
            if (num)
            {
                if (!helper->isValid(num, (const void **)group.base()))
                    group.clear(); // read next group
            }
            else
                abortSoon = true; // eof
        }
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon)
            return NULL;

        if (group.ordinality())
        {
            while (nextIndex < group.ordinality())
            {
                OwnedConstThorRow ret = group.itemClear(nextIndex++);
                if (stepCompare->docompare(ret, seek, numFields) >= 0)
                {
                    dataLinkIncrement();
                    return ret.getClear();
                }
            }
            nextIndex = 0;
            group.clear();
            //nextRowGE never returns an end of group marker. JCSMORE - Is this right?
        }

        //Notes from Roxie impl.
        //Not completely sure about this - it could lead the the start of a group being skipped, 
        //so the group filter could potentially work on a different group.  If so, we'd need to check the
        //next fields were a subset of the grouping fields - more an issue for the group activity.
        
        //MORE: What do we do with wasCompleteMatch?  something like the following????
#if 0
        loop
        {
            OwnedConstThorRow ret;
            if (stepExtra.returnMismatches())
            {
                bool matchedCompletely = true;
                ret.setown(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
                if (!wasCompleteMatch)
                    return ret.getClear();
            }
            else
                ret.setown(input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
#endif

        OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        while (ret)
        {
            group.append(ret.getClear());
            ret.setown(input->nextRow());
        }

        unsigned num = group.ordinality();
        if (num)
        {
            if (!helper->isValid(num, (const void **)group.base()))
                group.clear();
        }
        else
            abortSoon = true; // eof

        return ungroupedNextRow(); // JCSMORE doesn't look right, but as hthor/roxie
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        return input->gatherConjunctions(collector);
    }
    void resetEOF() 
    { 
        abortSoon = false;
        group.clear();
        input->resetEOF(); 
    }
    void stop()
    {
        group.clear();
        stopInput(input);
        dataLinkStop();
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CFilterSlaveActivityBase::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};



////////////////////

CActivityBase *createFilterSlave(CGraphElementBase *container)
{
    return new CFilterSlaveActivity(container);
}

CActivityBase *createFilterProjectSlave(CGraphElementBase *container)
{
    return new CFilterProjectSlaveActivity(container);
}

CActivityBase *createFilterGroupSlave(CGraphElementBase *container)
{
    return new CFilterGroupSlaveActivity(container);
}
