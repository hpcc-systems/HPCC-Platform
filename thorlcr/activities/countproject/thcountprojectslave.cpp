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

#include "thcountprojectslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"

class BaseCountProjectActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

protected:
    IHThorCountProjectArg *helper = nullptr;
    rowcount_t count = 0;

    virtual void start() override
    {
        PARENT::start();
        count = 0;
    }
public:
    BaseCountProjectActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorCountProjectArg *> (queryHelper());
    }
};


class LocalCountProjectActivity : public BaseCountProjectActivity
{
    typedef BaseCountProjectActivity PARENT;
    bool anyThisGroup;

public:
    LocalCountProjectActivity(CGraphElementBase *container) : BaseCountProjectActivity(container)
    {
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ActPrintLog("COUNTPROJECT: Is Local");
        anyThisGroup = false;
        PARENT::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        while (!abortSoon)
        {
            OwnedConstThorRow row(inputStream->nextRow());
            if (!row)
            {
                if (anyThisGroup) 
                    break;
                row.setown(inputStream->nextRow());
                if (!row)
                    break;
                count = 0;
            }
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t sizeGot = helper->transform(ret, row, ++count);
            if (sizeGot)
            {
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(sizeGot);
            }
        }
        count = 0;
        anyThisGroup = false;
        return NULL;        
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual void onInputFinished(rowcount_t finalCount)
    {
        // no action required
    }
};


class CountProjectActivity : public BaseCountProjectActivity, implements ILookAheadStopNotify
{
    typedef BaseCountProjectActivity PARENT;
    bool first;
    Semaphore prevRecCountSem;
    rowcount_t prevRecCount, localRecCount;

    bool haveLocalCount() { return RCUNSET != localRecCount; }
    void sendCount(rowcount_t _count)
    {
        // either called by onInputFinished(signaled by nextRow/stop) or by nextRow/stop itself
        if (lastNode())
            return;
        CMessageBuffer msg;
        msg.append(_count);
        queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag);
    }
    rowcount_t getPrevCount()
    {
        if (!firstNode())
        {
            CMessageBuffer msg;
            rowcount_t _count;
            if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag))
                return 0;
            msg.read(_count);
            return _count;
        }
        else
            return 0;
    }
    void signalNext()
    {
        if (haveLocalCount()) // if local total count known, send total now
        {
            ActPrintLog("COUNTPROJECT: row count pre-known to be %" RCPF "d", localRecCount);
            sendCount(prevRecCount + localRecCount);
        }
        else
            prevRecCountSem.signal();
    }
public:
    CountProjectActivity(CGraphElementBase *container) : BaseCountProjectActivity(container)
    {
    }   
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        PARENT::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, _input, consumerOrdered);
        setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), COUNTPROJECT_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage())); // could spot disk write output here?
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ActPrintLog( "COUNTPROJECT: Is Global");
        first = true;
        prevRecCount = 0;
        ThorDataLinkMetaInfo info;
        input->getMetaInfo(info);
        localRecCount = (info.totalRowsMin == info.totalRowsMax) ? (rowcount_t)info.totalRowsMax : RCUNSET;
        PARENT::start();
    }
    virtual void stop()
    {
        PARENT::stop();
        if (first) // nextRow, therefore getPrevCount()/sendCount() never called
        {
            prevRecCount = count = getPrevCount();
            signalNext();
        }
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        prevRecCountSem.signal();
        if (!firstNode())
            cancelReceiveMsg(queryJobChannel().queryMyRank()-1, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (first) 
        {
            first = false;
            prevRecCount = count = getPrevCount();
            signalNext();
        }
        while (!abortSoon)
        {
            OwnedConstThorRow row(inputStream->nextRow()); // NB: lookahead ensures ungrouped
            if (!row) 
                break;
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t sizeGot = helper->transform(ret, row, ++count);
            if (sizeGot)
            {
                dataLinkIncrement();
                return ret.finalizeRowClear(sizeGot);
            }
        }
        return NULL;
    }
    virtual bool isGrouped() const override { return false; }
    virtual void onInputFinished(rowcount_t localRecCount)
    {
        if (!haveLocalCount())
        {
            prevRecCountSem.wait();
            if (!abortSoon)
            {
                ActPrintLog("count is %" RCPF "d", localRecCount);
                sendCount(prevRecCount + localRecCount);
            }
        }
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.isSequential = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


CActivityBase *createLocalCountProjectSlave(CGraphElementBase *container) { return new LocalCountProjectActivity(container); }
CActivityBase *createCountProjectSlave(CGraphElementBase *container) { return new CountProjectActivity(container); }

