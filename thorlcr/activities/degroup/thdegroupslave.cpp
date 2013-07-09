/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

class CDegroupSlaveActivity : public CSlaveActivity, public CThorDataLink, public CThorSteppable
{
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDegroupSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container) , CThorDataLink(this), CThorSteppable(this)
    { 
        input = NULL; 
    }
    bool isGrouped() 
    { 
        return false; 
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);

        startInput(input);
        if(!input->isGrouped()) ActPrintLog("DEGROUP: Degrouping non-grouped input!");
        dataLinkStart();
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {   
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!abortSoon)
        {
            OwnedConstThorRow row = input->ungroupedNextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
            abortSoon = true;
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
        if (!abortSoon)
        {
            OwnedConstThorRow row = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
            abortSoon = true;
        }
        return NULL;
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        return input->gatherConjunctions(collector);
    }
    void resetEOF() 
    { 
        abortSoon = false;
        input->resetEOF(); 
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
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

CActivityBase *createDegroupSlave(CGraphElementBase *container)
{
    return new CDegroupSlaveActivity(container);
}



