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

#include "thdistributionslave.ipp"
#include "thactivityutil.ipp"

class CDistributionSlaveActivity : public ProcessSlaveActivity
{
    IHThorDistributionArg * helper;
    MemoryAttr ma;
    IDistributionTable * * aggy;                // should this be row?
    IThorDataLink *input;

public:
    CDistributionSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        input = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        mpTag = container.queryJob().deserializeMPTag(data);
        helper = static_cast <IHThorDistributionArg *> (queryHelper()); 
        aggy = (IDistributionTable * *)ma.allocate(helper->queryInternalRecordSize()->getMinRecordSize());
    }
    void kill()
    {
        CSlaveActivity::kill();

        helper->destruct(aggy);
        ma.clear();
    }
    void process()
    {
        helper->clearAggregate(aggy);
        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;

        try
        {
            while (!abortSoon)
            {
                OwnedConstThorRow row(input->ungroupedNextRow());
                if (!row)
                    break;
                helper->process(aggy, row);     
                processed++;
            }
            ActPrintLog("DISTRIBUTION: processed %"RCPF"d records", processed & THORDATALINK_COUNT_MASK);
        }
        catch(CATCHALL)
        {
            ActPrintLog("DISTRIBUTION: exception");
            throw;
        }
        CMessageBuffer msg;
        helper->serialize(aggy, msg);
        container.queryJob().queryJobComm().send(msg, 0, mpTag);
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }   
};

                    
CActivityBase *createDistributionSlave(CGraphElementBase *container)
{ 
    return new CDistributionSlaveActivity(container); 
}
