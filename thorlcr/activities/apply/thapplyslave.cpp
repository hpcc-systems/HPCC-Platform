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

#include "thapplyslave.ipp"
#include "thactivityutil.ipp"

class CApplySlaveActivity : public ProcessSlaveActivity
{
    IHThorApplyArg *helper;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CApplySlaveActivity(CGraphElementBase *container) 
        : ProcessSlaveActivity(container)
    { 
        helper = NULL;
        input = NULL;
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorApplyArg *> (queryHelper());
    }
// IThorSlaveProcess overloaded methods
    virtual void process()
    {
        processed = 0;
        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;
        try
        {
            if (container.queryLocalOrGrouped() || firstNode())
                helper->start();
            while(!abortSoon)
            {
                ActivityTimer t(totalCycles, timeActivities);
                OwnedConstThorRow r = input->ungroupedNextRow();
                if (!r)
                    break;
                helper->apply(r);
                processed++;
            }
            if (container.queryLocalOrGrouped() || firstNode())
                helper->end();
        }
        catch(CATCHALL)
        {
            ActPrintLog("APPLY: exception");
            throw;
        }
    }
    virtual void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }
};

CActivityBase *createApplySlave(CGraphElementBase *container)
{
    return new CApplySlaveActivity(container);
}
