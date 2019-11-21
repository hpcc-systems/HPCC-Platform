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

#include "thgroupaggregateslave.ipp"

class GroupAggregateSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    bool eof, ungroupedExistsAggregate;
    IHThorAggregateArg * helper;

public:
    GroupAggregateSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    { 
        helper = static_cast <IHThorAggregateArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = false;
        ungroupedExistsAggregate = (container.getKind() == TAKexistsaggregate) && !input->isGrouped();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon || eof)
            return NULL;
        RtlDynamicRowBuilder out(queryRowAllocator());
        size32_t sz = helper->clearAggregate(out);
        OwnedConstThorRow row = inputStream->nextRow();
        if (row)
        {
            sz = helper->processFirst(out, row);
            // NB: if ungrouped existsAggregate, no need to look at rest of input
            if (!ungroupedExistsAggregate)
            {
                while (!abortSoon)
                {
                    row.setown(inputStream->nextRow());
                    if (!row)
                        break;
                    sz = helper->processNext(out, row);
                }
            }
            if (!input->isGrouped())
                eof = true;
        }
        else
        {
            eof = true;
            if (input->isGrouped())
                return NULL;
        }
        dataLinkIncrement();
        return out.finalizeRowClear(sz);
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.fastThrough = true;
    }
    virtual bool isGrouped() const override
    {
        return false;
    }
};


CActivityBase *createGroupAggregateSlave(CGraphElementBase *container)
{
    return new GroupAggregateSlaveActivity(container);
}



