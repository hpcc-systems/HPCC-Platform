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

class CCatchSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    Owned<IThorDataLink> input;
    IHThorCatchArg *helper;
    StringBuffer kindStr;
    bool eos;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCatchSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorCatchArg *> (queryHelper());
        kindStr.append(activityKindStr(container.getKind()));
        kindStr.toUpperCase();
        eos = false;
        appendOutputLinked(this);
    }
    virtual void start()
    {
        input.set(inputs.item(0));
        startInput(input);
        eos = false;
        dataLinkStart(kindStr.str(), container.queryId());
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
        ActivityTimer s(totalCycles, timeActivities, NULL);
        CCatchSlaveActivityBase::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
                ActPrintLog(e, NULL);
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
                ActivityTimer t(totalCycles, timeActivities, NULL);
                OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
                if (ret && wasCompleteMatch)
                    dataLinkIncrement();
                return ret.getClear();
            }
            catch (IException *e)
            {
                eos = true;
                ActPrintLog(e, NULL);
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
            Owned<IRowWriterMultiReader> overflowBuf = createOverflowableBuffer(queryRowInterfaces(input), CATCH_BUFFER_SIZE);
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
            mptag_t barrierTag = container.queryJob().deserializeMPTag(data);
            barrier.setown(container.queryJob().createBarrier(barrierTag));
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        CCatchSlaveActivityBase::start();
        running = gathered = false;
        grouped = input->isGrouped();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
                if ((TAKcreaterowcatch == container.getKind()) && (container.queryLocalOrGrouped() || 1 == container.queryJob().queryMyRank()))
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

