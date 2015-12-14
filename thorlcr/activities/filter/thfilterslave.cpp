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
    void start()
    {   
        input = inputs.item(0);
        anyThisGroup = false;
        startInput(input);
        dataLinkStart();
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
        ActivityTimer s(totalCycles, timeActivities);
        matched = 0;
        abortSoon = !helper->canMatchAny();
        CFilterSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
        ActivityTimer t(totalCycles, timeActivities);
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
        ActivityTimer s(totalCycles, timeActivities);
        abortSoon = !helper->canMatchAny();
        recordCount = 0;
        CFilterSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
    IHThorFilterGroupArg *helper;
    Owned<IThorRowLoader> groupLoader;
    Owned<IRowStream> groupStream;
    bool compressSpills;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFilterGroupSlaveActivity(CGraphElementBase *container) : CFilterSlaveActivityBase(container), CThorSteppable(this)
    {
        groupLoader.setown(createThorRowLoader(*this, NULL, stableSort_none, rc_allMem));
        helper = NULL;
        compressSpills = getOptBool(THOROPT_COMPRESS_SPILLS, true);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFilterSlaveActivityBase::init(data,slaveData);
        helper = (IHThorFilterGroupArg *)queryHelper();
    }
    void start()
    {   
        ActivityTimer s(totalCycles, timeActivities);
        abortSoon = !helper->canMatchAny();
        CFilterSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        while (!abortSoon)
        {
            if (groupStream)
            {
                OwnedConstThorRow row = groupStream->nextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
                groupStream.clear();
                return NULL;
            }
            CThorExpandingRowArray rows(*this, this);
            Owned<IRowStream> rowStream = groupLoader->loadGroup(input, abortSoon, &rows);
            if (rows.ordinality())
            {
                // JCSMORE - if isValid would take a stream, group wouldn't need to be in mem.
                if (helper->isValid(rows.ordinality(), rows.getRowArray()))
                {
                    CThorSpillableRowArray spillableRows(*this, this);
                    spillableRows.transferFrom(rows);
                    groupStream.setown(spillableRows.createRowStream(SPILL_PRIORITY_SPILLABLE_STREAM, compressSpills));
                }
                // else read next group
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
        ActivityTimer t(totalCycles, timeActivities);
        if (abortSoon)
            return NULL;

        if (groupStream)
        {
            loop
            {
                OwnedConstThorRow row = groupStream->nextRow();
                if (!row)
                    break;
                if (stepCompare->docompare(row, seek, numFields) >= 0)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            groupStream.clear();
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

        CThorExpandingRowArray rows(*this, this);
        groupStream.setown(groupLoader->loadGroup(input, abortSoon, &rows));
        if (rows.ordinality())
        {
            // JCSMORE - if isValid would take a stream, group wouldn't need to be in mem.
            if (!helper->isValid(rows.ordinality(), rows.getRowArray()))
                groupStream.clear();
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
        groupStream.clear();
        input->resetEOF(); 
    }
    void stop()
    {
        groupStream.clear();
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
