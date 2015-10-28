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

class CCatchSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    Owned<IThorDataLink> input;
    IHThorCatchArg *helper;
    bool eos;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCatchSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorCatchArg *> (queryHelper());
        eos = false;
        appendOutputLinked(this);
    }
    virtual void start()
    {
        input.set(inputs.item(0));
        startInput(input);
        eos = false;
        dataLinkStart();
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
};

class CCatchSlaveActivity : public CCatchSlaveActivityBase, public CThorSteppable
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCatchSlaveActivity(CGraphElementBase *container) 
        : CCatchSlaveActivityBase(container), CThorSteppable(this)
    {
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CCatchSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!eos)
        {
            try
            {
                OwnedConstThorRow row(input->nextRow());
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
                ActivityTimer t(totalCycles, timeActivities);
                OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
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
        input->resetEOF();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CCatchSlaveActivityBase::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};

class CSkipCatchSlaveActivity : public CCatchSlaveActivityBase
{
    bool gathered, global, grouped, running;
    Owned<IBarrier> barrier;
    Owned<IRowStream> inputStream;

    bool gather()
    {
        try
        {
            gathered = true;
            Owned<IRowWriterMultiReader> overflowBuf = createOverflowableBuffer(*this, queryRowInterfaces(input), true);
            running = true;
            while (running)
            {
                OwnedConstThorRow row = input->nextRow();
                if (!row)
                {
                    row.setown(input->nextRow());
                    if (!row)
                        break;
                    else
                        overflowBuf->putRow(NULL); // eog
                }
                overflowBuf->putRow(row.getClear());
            }
            overflowBuf->flush();
            inputStream.setown(overflowBuf->getReader()); 
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
    CSkipCatchSlaveActivity(CGraphElementBase *container) 
        : CCatchSlaveActivityBase(container)
    {
        global = !container->queryLocalOrGrouped();
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {       
        CCatchSlaveActivityBase::init(data, slaveData);
        if (global)
        {
            mptag_t barrierTag = container.queryJobChannel().deserializeMPTag(data);
            barrier.setown(container.queryJobChannel().createBarrier(barrierTag));
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CCatchSlaveActivityBase::start();
        running = gathered = false;
        grouped = input->isGrouped();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
        OwnedConstThorRow row(inputStream->nextRow());
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void stop()
    {
        CCatchSlaveActivityBase::stop();
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
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = false;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
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

