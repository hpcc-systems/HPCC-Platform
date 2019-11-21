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

#include "slave.ipp"
#include "thactivityutil.ipp"

#include "thbufdef.hpp"
#include "thpullslave.ipp"

class PullSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

public:
    PullSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        setRequireInitData(false);
        appendOutputLinked(this);
    }

// IThorDataLink methods
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        if (ensureStartFTLookAhead(0))
            setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), PULL_SMART_BUFFER_SIZE, true, false, RCUNBOUND, NULL, &container.queryJob().queryIDiskUsage()), false);
    }
    const void * nextRow() override
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow row = inputStream->nextRow();
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }

    virtual bool isGrouped() const override { return false; } // or input->isGrouped?
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


CActivityBase *createPullSlave(CGraphElementBase *container)
{
    return new PullSlaveActivity(container);
}

