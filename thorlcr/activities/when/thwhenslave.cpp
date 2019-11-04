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

class CDependencyExecutorSlaveActivity : public CSlaveActivity
{
protected:
    size32_t savedParentExtractSz;
    const byte *savedParentExtract;
    bool global;
    Owned<IBarrier> barrier;

public:
    CDependencyExecutorSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        global = !queryContainer().queryOwner().queryOwner() || queryContainer().queryOwner().isGlobal();
        if (!global)
            setRequireInitData(false);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        if (global)
        {
            mptag_t barrierTag = queryJobChannel().deserializeMPTag(data);
            barrier.setown(queryJobChannel().createBarrier(barrierTag));
        }
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract) override
    {
        savedParentExtractSz = parentExtractSz;
        savedParentExtract = parentExtract;
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (global)
            barrier->cancel();
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
            ActPrintLog("Executing dependencies");
            queryContainer().executeDependencies(savedParentExtractSz, savedParentExtract, controlId, true);
        }
        return true;
    }
};


class CWhenSlaveActivity : public CDependencyExecutorSlaveActivity
{
    typedef CDependencyExecutorSlaveActivity PARENT;

public:
    CWhenSlaveActivity(CGraphElementBase *_container) : CDependencyExecutorSlaveActivity(_container)
    {
        appendOutputLinked(this);
    }
    virtual void stop() override
    {
        bool started = hasStarted();
        PARENT::stop();
        if (started)
        {
            if (!executeDependencies(abortSoon ? WhenFailureId : WhenSuccessId))
                abortSoon = true;
        }
    }
    virtual bool isGrouped() const override { return input->isGrouped(); }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow row(inputStream->nextRow());
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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
