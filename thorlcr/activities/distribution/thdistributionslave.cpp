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
        mpTag = container.queryJobChannel().deserializeMPTag(data);
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
            ActPrintLog("DISTRIBUTION: processed %" RCPF "d records", processed & THORDATALINK_COUNT_MASK);
        }
        catch(CATCHALL)
        {
            ActPrintLog("DISTRIBUTION: exception");
            throw;
        }
        CMessageBuffer msg;
        helper->serialize(aggy, msg);
        queryJobChannel().queryJobComm().send(msg, 0, mpTag);
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
