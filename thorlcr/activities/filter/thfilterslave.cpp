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

class CFilterSlaveActivityBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

protected:
    bool anyThisGroup = false;

public:
    explicit CFilterSlaveActivityBase(CGraphElementBase *_container)
        : CSlaveActivity(_container)
    {
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        PARENT::start();
        anyThisGroup = false;
    }
    virtual const void *nextRow() override =0;

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};


class CFilterSlaveActivity : public CFilterSlaveActivityBase, public CThorSteppable
{
    typedef CFilterSlaveActivityBase PARENT;

    IHThorFilterArg *helper;
    unsigned matched;

public:
    CFilterSlaveActivity(CGraphElementBase *container)
        : CFilterSlaveActivityBase(container), CThorSteppable(this)
    {
        helper = static_cast <IHThorFilterArg *> (queryHelper());
    }
    virtual void start() override
    {   
        ActivityTimer s(slaveTimerStats, timeActivities);
        matched = 0;
        if (helper->canMatchAny())
            PARENT::start();
        else
        {
            dataLinkStart();
            abortSoon = true;
            stopInput(0);
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        while (!abortSoon)
        {
            OwnedConstThorRow row = inputStream->nextRow();
            if (!row)
            {
                if(anyThisGroup)
                {
                    anyThisGroup = false;
                    break;
                }
                row.setown(inputStream->nextRow());
                if (!row)
                    break;
            }
            if (helper->isValid(row))
            {
                matched++;
                anyThisGroup = true;
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return nullptr;
    }
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) override
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        while (!abortSoon)
        {
            OwnedConstThorRow ret = inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
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
    virtual bool gatherConjunctions(ISteppedConjunctionCollector &collector) override
    { 
        return input->gatherConjunctions(collector); 
    }
    virtual void resetEOF() override
    { 
        abortSoon = !helper->canMatchAny();
        anyThisGroup = false;
        inputStream->resetEOF();
    }
// steppable
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, input, consumerOrdered);
        CThorSteppable::setInputStream(index, input, consumerOrdered);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};

class CFilterProjectSlaveActivity : public CFilterSlaveActivityBase
{
    typedef CFilterSlaveActivityBase PARENT;

    IHThorFilterProjectArg *helper;
    rowcount_t recordCount;  // NB local (not really used for global)
    Owned<IEngineRowAllocator> allocator;
public:
    CFilterProjectSlaveActivity(CGraphElementBase *container) 
        : CFilterSlaveActivityBase(container)
    {
        helper = static_cast <IHThorFilterProjectArg *> (queryHelper());
        allocator.set(queryRowAllocator());
    }
    virtual void start() override
    {   
        ActivityTimer s(slaveTimerStats, timeActivities);
        recordCount = 0;
        if (helper->canMatchAny())
            PARENT::start();
        else
        {
            dataLinkStart();
            abortSoon = true;
            stopInput(0);
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        while (!abortSoon)
        {
            OwnedConstThorRow row = inputStream->nextRow();
            if (!row) {
                recordCount = 0;
                if (!anyThisGroup)
                {
                    row.setown(inputStream->nextRow());
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
    typedef CFilterSlaveActivityBase PARENT;

    IHThorFilterGroupArg *helper;
    Owned<IThorRowLoader> groupLoader;
    Owned<IRowStream> groupStream;
    unsigned spillCompInfo;

public:
    CFilterGroupSlaveActivity(CGraphElementBase *container) : CFilterSlaveActivityBase(container), CThorSteppable(this)
    {
        helper = (IHThorFilterGroupArg *)queryHelper();
        groupLoader.setown(createThorRowLoader(*this, NULL, stableSort_none, rc_allMem));
        spillCompInfo = 0x0;
        if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
        {
            StringBuffer compType;
            getOpt(THOROPT_COMPRESS_SPILL_TYPE, compType);
            setCompFlag(compType, spillCompInfo);
        }
    }
    virtual void start() override
    {   
        ActivityTimer s(slaveTimerStats, timeActivities);
        if (helper->canMatchAny())
            PARENT::start();
        else
        {
            dataLinkStart();
            abortSoon = true;
            stopInput(0);
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
            try
            {
                groupLoader->loadGroup(inputStream, abortSoon, &rows);
            }
            catch (IException *e)
            {
                if (!isOOMException(e))
                    throw e;
                IOutputMetaData *inputOutputMeta = input->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
                throw checkAndCreateOOMContextException(this, e, "loading group for filter group operation", groupLoader->numRows(), inputOutputMeta, groupLoader->probeRow(0));
            }
            if (rows.ordinality())
            {
                // JCSMORE - if isValid would take a stream, group wouldn't need to be in mem.
                if (helper->isValid(rows.ordinality(), rows.getRowArray()))
                {
                    CThorSpillableRowArray spillableRows(*this, this);
                    spillableRows.transferFrom(rows);
                    groupStream.setown(spillableRows.createRowStream(SPILL_PRIORITY_SPILLABLE_STREAM, spillCompInfo));
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
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon)
            return NULL;

        if (groupStream)
        {
            for (;;)
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
        for (;;)
        {
            OwnedConstThorRow ret;
            if (stepExtra.returnMismatches())
            {
                bool matchedCompletely = true;
                ret.setown(inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
                if (!wasCompleteMatch)
                    return ret.getClear();
            }
            else
                ret.setown(inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
#endif

        CThorExpandingRowArray rows(*this, this);
        try
        {
            groupStream.setown(groupLoader->loadGroup(inputStream, abortSoon, &rows));
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = input->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            throw checkAndCreateOOMContextException(this, e, "loading group for filter group operation", groupLoader->numRows(), inputOutputMeta, groupLoader->probeRow(0));
        }
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
        inputStream->resetEOF();
    }
    virtual void stop() override
    {
        PARENT::stop();
        groupStream.clear();
    }
// steppable
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, input, consumerOrdered);
        CThorSteppable::setInputStream(index, input, consumerOrdered);
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
