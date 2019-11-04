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

#include "thnullslave.ipp"
#include "thactivityutil.ipp"

class CNullSinkSlaveActivity : public ProcessSlaveActivity
{
public:
    CNullSinkSlaveActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        setRequireInitData(false);
    }
// IThorSlaveActivity
    virtual void process() override
    {
        start();
        stop();
    }
    virtual void endProcess() override
    {
    }
};


class CNullSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

public:
    CNullSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        appendOutputLinked(this);
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {       
    }

// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
    }
    const void * nextRow() override
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        return NULL;
    }
    virtual bool isGrouped() const override
    {
        return queryHelper()->queryOutputMeta()->isGrouped();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canReduceNumRows = true; // to 0 in fact
        info.totalRowsMax = 0;
    }
};


class CThroughSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

public:
    CThroughSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        setRequireInitData(false);
        appendOutputLinked(this);
    }
// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
    }
    const void * nextRow() override
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        return inputStream->nextRow();
    }
    virtual bool isGrouped() const override
    {
        return queryInput(0)->isGrouped();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        queryInput(0)->getMetaInfo(info);
    }
};



CActivityBase *createNullSinkSlave(CGraphElementBase *container) { return new CNullSinkSlaveActivity(container); }
CActivityBase *createNullSlave(CGraphElementBase *container) { return new CNullSlaveActivity(container); }
CActivityBase *createThroughSlave(CGraphElementBase *container) { return new CThroughSlaveActivity(container); }

