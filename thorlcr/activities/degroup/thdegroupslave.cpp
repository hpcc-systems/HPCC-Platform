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
        dataLinkStart("DEGROUP", container.queryId());
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



