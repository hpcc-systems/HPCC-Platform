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

#include "slave.ipp"
#include "thactivityutil.ipp"

#include "thbufdef.hpp"
#include "thpullslave.ipp"

class PullSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    Owned<IThorDataLink> input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    PullSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    ~PullSlaveActivity() 
    {
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }

// IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input.setown(createDataLinkSmartBuffer(this,inputs.item(0),PULL_SMART_BUFFER_SIZE,true,false,RCUNBOUND,NULL,false,&container.queryJob().queryIDiskUsage()));
        startInput(input);
        dataLinkStart();
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }

    const void * nextRow()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row = input->nextRow();
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }

    virtual bool isGrouped() { return false; } // or input->isGrouped?
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};


CActivityBase *createPullSlave(CGraphElementBase *container)
{
    return new PullSlaveActivity(container);
}

