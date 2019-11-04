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

#include "thdegroupslave.ipp"

class CDegroupSlaveActivity : public CSlaveActivity, public CThorSteppable
{
    typedef CSlaveActivity PARENT;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CDegroupSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorSteppable(this)
    { 
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        if(!input->isGrouped()) ActPrintLog("DEGROUP: Degrouping non-grouped input!");
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon)
        {
            OwnedConstThorRow row = inputStream->ungroupedNextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
            abortSoon = true;
        }
        return NULL;
    }
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) override
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    virtual const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon)
        {
            OwnedConstThorRow row = inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
            abortSoon = true;
        }
        return NULL;
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector &collector) override
    { 
        return input->gatherConjunctions(collector);
    }
    virtual void resetEOF() override
    { 
        abortSoon = false;
        inputStream->resetEOF();
    }
    virtual bool isGrouped() const override
    { 
        return false; 
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true;
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

CActivityBase *createDegroupSlave(CGraphElementBase *container)
{
    return new CDegroupSlaveActivity(container);
}



