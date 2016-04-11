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

#include "slave.hpp"
#include "thactivityutil.ipp"
#include "thbuf.hpp"
#include "thbufdef.hpp"
#include "commonext.hpp"
#include "slave.ipp"

class CDependencyExecutorSlaveActivity : public CSimpleInterface
{
protected:
    size32_t savedParentExtractSz;
    const byte *savedParentExtract;
    bool global;
    Owned<IBarrier> barrier;
    CSlaveActivity *activity;

public:
    CDependencyExecutorSlaveActivity(CSlaveActivity *_activity) : activity(_activity)
    {
        global = !activity->queryContainer().queryOwner().queryOwner() || activity->queryContainer().queryOwner().isGlobal();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (global)
        {
            mptag_t barrierTag = activity->queryContainer().queryJobChannel().deserializeMPTag(data);
            barrier.setown(activity->queryContainer().queryJobChannel().createBarrier(barrierTag));
        }
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        savedParentExtractSz = parentExtractSz;
        savedParentExtract = parentExtract;
    }
    bool executeDependencies(int controlId)
    {
        if (global)
        {
            if (!barrier->wait(false))
                return false;
        }
        else
        {
            ActPrintLog(activity, "Executing dependencies");
            activity->queryContainer().executeDependencies(savedParentExtractSz, savedParentExtract, controlId, true);
        }
        return true;
    }
};


class CWhenSlaveActivity : public CSlaveActivity, public CDependencyExecutorSlaveActivity
{
    typedef CSlaveActivity PARENT;

public:
    IMPLEMENT_IINTERFACE_USING(CDependencyExecutorSlaveActivity);

    CWhenSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CDependencyExecutorSlaveActivity(this)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        CDependencyExecutorSlaveActivity::init(data, slaveData);
        appendOutputLinked(this);
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CDependencyExecutorSlaveActivity::preStart(parentExtractSz, parentExtract);
    }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
    }
    virtual void stop() override
    {
        PARENT::stop();
        if (!executeDependencies(abortSoon ? WhenFailureId : WhenSuccessId))
            abortSoon = true;
    }
    virtual bool isGrouped() const override { return input->isGrouped(); }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow row(inputStream->nextRow());
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (global)
            barrier->cancel();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.fastThrough = false;
        calcMetaInfoSize(info, queryInput(0));
    }
};

////////////////////

CActivityBase *createWhenSlave(CGraphElementBase *container)
{
    return new CWhenSlaveActivity(container);
}
