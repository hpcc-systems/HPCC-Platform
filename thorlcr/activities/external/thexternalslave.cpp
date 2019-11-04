/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "jlib.hpp"
#include "jset.hpp"
#include "jqueue.tpp"
#include "commonext.hpp"

#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thalloc.hpp"
#include "eclrtl_imp.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"

/////
///////////////////
//
// CExternalSlaveActivity
//

class CNodeActivityContext : public IThorActivityContext
{
public:
    CNodeActivityContext(bool _local, unsigned _numSlaves, unsigned _curSlave)
    : local(_local), slaves(_local ? 1 : _numSlaves), curSlave(_local ? 0 : _curSlave)
    {
        assertex(curSlave < slaves);
    }

    virtual bool isLocal() const override { return local; }
    virtual unsigned numSlaves() const override { return slaves; }
    virtual unsigned numStrands() const override { return 1; }
    virtual unsigned querySlave() const override { return curSlave; }
    virtual unsigned queryStrand() const override { return 0; }
protected:
    unsigned slaves;
    unsigned curSlave;
    bool local;
};

class CExternalSlaveActivity : public CSlaveActivity
{
    IHThorExternalArg *helper;
    Owned<IRowStream> rows;
    CNodeActivityContext activityContext;
    bool grouped;

public:
    CExternalSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container),
          activityContext(container.queryLocalData() || container.queryOwner().isLocalChild(), container.queryCodeContext()->getNodes(), container.queryCodeContext()->getNodeNum())
    {
        grouped = container.queryGrouped();
        helper = (IHThorExternalArg *) queryHelper();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        CSlaveActivity::setInputStream(index, _input, consumerOrdered);
        helper->setInput(index, _input.queryStream());
    }
    virtual void start() override
    {
        //Cannot call base base start because that will error if > 1 input
        startAllInputs();
        dataLinkStart();
        rows.setown(helper->createOutput(&activityContext));
    }
    virtual void stop() override
    {
        if (rows)
        {
            rows->stop();
            rows.clear();
        }
        stopAllInputs();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        assertex(rows);
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow row = rows->nextRow();
        if (row)
            dataLinkIncrement();
        return row.getClear();
    }
    virtual bool isGrouped() const override
    {
        return grouped;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};


CActivityBase *createExternalSlave(CGraphElementBase *container)
{
    return new CExternalSlaveActivity(container);
}

//---------------------------------------------------------------------------------------------------------------------

class CExternalSinkSlaveActivity : public ProcessSlaveActivity
{
    IHThorExternalArg *helper;
    CNodeActivityContext activityContext;

public:
    CExternalSinkSlaveActivity(CGraphElementBase *_container)
        : ProcessSlaveActivity(_container),
          activityContext(container.queryLocalData() || container.queryOwner().isLocalChild(), container.queryCodeContext()->getNodes(), container.queryCodeContext()->getNodeNum())
    {
        helper = (IHThorExternalArg *) queryHelper();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        CSlaveActivity::setInputStream(index, _input, consumerOrdered);
        helper->setInput(index, _input.queryStream());
    }
    virtual void start() override
    {
        startAllInputs();
    }
    virtual void process() override
    {
        start();
        processed = THORDATALINK_STARTED;
        try
        {
            helper->execute(&activityContext);
        }
        catch(CATCHALL)
        {
            ActPrintLog("APPLY: exception");
            throw;
        }
    }
    virtual void endProcess() override
    {
        if ((processed & THORDATALINK_STARTED) && !(processed & THORDATALINK_STOPPED))
        {
            stopAllInputs();
            processed |= THORDATALINK_STOPPED;
        }
    }
};


CActivityBase *createExternalSinkSlave(CGraphElementBase *container)
{
    return new CExternalSinkSlaveActivity(container);
}
