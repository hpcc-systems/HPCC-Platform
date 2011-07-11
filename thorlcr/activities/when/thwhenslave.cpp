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

#include "slave.hpp"
#include "thactivityutil.ipp"
#include "thbuf.hpp"
#include "thbufdef.hpp"
#include "commonext.hpp"
#include "slave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/when/thwhenslave.cpp $ $Id: thwhenslave.cpp 62376 2011-02-04 21:59:58Z sort $");


class CDependencyExecutorSlaveActivity : public CSimpleInterface
{
protected:
    size32_t savedParentExtractSz;
    const byte *savedParentExtract;
    bool global;
    Owned<IBarrier> barrier;
    CSlaveActivity *activity;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDependencyExecutorSlaveActivity(CSlaveActivity *_activity) : activity(_activity)
    {
        global = !activity->queryContainer().queryOwner().queryOwner() || activity->queryContainer().queryOwner().isGlobal();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (global)
        {
            mptag_t barrierTag = activity->queryContainer().queryJob().deserializeMPTag(data);
            barrier.setown(activity->queryContainer().queryJob().createBarrier(barrierTag));
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


class CWhenSlaveActivity : public CSlaveActivity, public CDependencyExecutorSlaveActivity, public CThorDataLink
{
protected:
    Owned<IThorDataLink> input;

public:
    IMPLEMENT_IINTERFACE_USING(CDependencyExecutorSlaveActivity);

    CWhenSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CDependencyExecutorSlaveActivity(this), CThorDataLink(this)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDependencyExecutorSlaveActivity::init(data, slaveData);
        appendOutputLinked(this);
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CDependencyExecutorSlaveActivity::preStart(parentExtractSz, parentExtract);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input.set(inputs.item(0));
        startInput(input);
        dataLinkStart("WHEN", container.queryId());
    }
    virtual void stop()
    {
        stopInput(input);
        if (!executeDependencies(abortSoon ? WhenFailureId : WhenSuccessId))
            abortSoon = true;
        dataLinkStop();
    }
    virtual bool isGrouped() { return input->isGrouped(); }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row(input->nextRow());
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
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = false;
        calcMetaInfoSize(info,inputs.item(0));
    }
};

////////////////////

CActivityBase *createWhenSlave(CGraphElementBase *container)
{
    return new CWhenSlaveActivity(container);
}

///////////

class CIfActionSlaveActivity : public ProcessSlaveActivity, public CDependencyExecutorSlaveActivity
{
public:
    IMPLEMENT_IINTERFACE_USING(CDependencyExecutorSlaveActivity);

    CIfActionSlaveActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container), CDependencyExecutorSlaveActivity(this)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDependencyExecutorSlaveActivity::init(data, slaveData);
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CDependencyExecutorSlaveActivity::preStart(parentExtractSz, parentExtract);
    }
    virtual void process()
    {
        processed = THORDATALINK_STARTED;
        IHThorIfArg *helper = (IHThorIfArg *)queryHelper();
        int controlId = helper->getCondition() ? 1 : 2;
        if (!executeDependencies(controlId))
            abortSoon = true;
    }
    virtual void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
            processed |= THORDATALINK_STOPPED;
    }
    virtual void abort()
    {
        ProcessSlaveActivity::abort();
        if (global)
            barrier->cancel();
    }
};

////////////////////

CActivityBase *createIfActionSlave(CGraphElementBase *container)
{
    return new CIfActionSlaveActivity(container);
}

