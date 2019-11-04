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

class CCatchSlaveActivityBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
protected:
    IHThorCatchArg *helper;
    bool eos = false;

public:
    CCatchSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorCatchArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        PARENT::start();
        eos = false;
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};

class CCatchSlaveActivity : public CCatchSlaveActivityBase, public CThorSteppable
{
    typedef CCatchSlaveActivityBase PARENT;

public:
    CCatchSlaveActivity(CGraphElementBase *container) 
        : CCatchSlaveActivityBase(container), CThorSteppable(this)
    {
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CCatchSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!eos)
        {
            try
            {
                OwnedConstThorRow row(inputStream->nextRow());
                if (!row)
                    return NULL;
                dataLinkIncrement();
                return row.getClear();
            }
            catch (IException *e)
            {
                eos = true;
                ActPrintLog(e);
                e->Release();
                helper->onExceptionCaught();
            }
            catch (...)
            {
                eos = true;
                helper->onExceptionCaught();
            }
            throwUnexpected(); // onExceptionCaught should have thrown something
        }
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        if (!eos)
        {
            try
            {
                ActivityTimer t(slaveTimerStats, timeActivities);
                OwnedConstThorRow ret = inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
                if (ret && wasCompleteMatch)
                    dataLinkIncrement();
                return ret.getClear();
            }
            catch (IException *e)
            {
                eos = true;
                ActPrintLog(e);
                e->Release();
                helper->onExceptionCaught();
            }
            catch (...)
            {
                eos = true;
                helper->onExceptionCaught();
            }
            throwUnexpected(); // onExceptionCaught should have thrown something
        }
        return NULL;
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector) 
    { 
        return input->gatherConjunctions(collector); 
    }
    virtual void resetEOF() 
    { 
        inputStream->resetEOF();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
// steppable
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override
    {
        CCatchSlaveActivityBase::setInputStream(index, input, consumerOrdered);
        CThorSteppable::setInputStream(index, input, consumerOrdered);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};

class CSkipCatchSlaveActivity : public CCatchSlaveActivityBase
{
    bool gathered, global, grouped, running;
    Owned<IBarrier> barrier;
    Owned<IRowStream> gatheredInputStream;

    bool gather()
    {
        try
        {
            gathered = true;
            Owned<IRowWriterMultiReader> overflowBuf = createOverflowableBuffer(*this, queryRowInterfaces(input), ers_eogonly);
            running = true;
            while (running)
            {
                OwnedConstThorRow row = inputStream->nextRow();
                if (!row)
                {
                    row.setown(inputStream->nextRow());
                    if (!row)
                        break;
                    else
                        overflowBuf->putRow(NULL); // eog
                }
                overflowBuf->putRow(row.getClear());
            }
            overflowBuf->flush();
            gatheredInputStream.setown(overflowBuf->getReader());
        }
        catch (IException *)
        {
            if (global && barrier.get())
            {
                barrier->cancel();
                barrier.clear();
            }
            throw;
        }
        if (global)
            return barrier->wait(false); // if cancelled returns false
        return true;
    }

public:
    CSkipCatchSlaveActivity(CGraphElementBase *_container)
        : CCatchSlaveActivityBase(_container)
    {
        global = !container.queryLocalOrGrouped();
        if (!global)
            setRequireInitData(false);
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        CCatchSlaveActivityBase::init(data, slaveData);
        if (global)
        {
            mptag_t barrierTag = container.queryJobChannel().deserializeMPTag(data);
            barrier.setown(container.queryJobChannel().createBarrier(barrierTag));
        }
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CCatchSlaveActivityBase::start();
        running = gathered = false;
        grouped = input->isGrouped();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eos)
            return NULL;
        if (!gathered)
        {
            Owned<IException> e;
            try
            {
                if (!gather())
                {
                    eos = true;
                    return NULL;
                }
            }
            catch (IException *_e) { e.setown(_e); }
            catch (...) { throw MakeActivityException(this, 0, "Unknown exception caught"); }
            if (e.get())
            {
                eos = true;
                if (global && barrier.get())
                {
                    barrier->cancel();
                    barrier.clear();
                }
                if ((TAKcreaterowcatch == container.getKind()) && (container.queryLocalOrGrouped() || firstNode()))
                {
                    Linked<IEngineRowAllocator> allocator = queryRowAllocator();
                    RtlDynamicRowBuilder row(allocator);
                    size32_t sz = helper->transformOnExceptionCaught(row, e.get());
                    if (0 == sz)
                        return NULL;
                    dataLinkIncrement();
                    return row.finalizeRowClear(sz);
                }               
                return NULL;
            }
        }
        OwnedConstThorRow row(gatheredInputStream->nextRow());
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void abort()
    {
        CCatchSlaveActivityBase::abort();
        running = false;
        if (global && barrier.get())
        {
            barrier->cancel();
            barrier.clear();
        }
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = false;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


////////////////////

CActivityBase *createCatchSlave(CGraphElementBase *container)
{
    switch (container->getKind())
    {
        case TAKcatch:
            return new CCatchSlaveActivity(container);
        case TAKskipcatch:
        case TAKcreaterowcatch:
            return new CSkipCatchSlaveActivity(container);
        default:
            assert(false);
            return NULL;
    }
}

