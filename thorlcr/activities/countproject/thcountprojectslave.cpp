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
        helper = static_cast <IHThorCountProjectArg *> (queryHelper());
        appendOutputLinked(this);
    }
};


class LocalCountProjectActivity : public BaseCountProjectActivity
{
    typedef BaseCountProjectActivity PARENT;
    bool anyThisGroup;

public:
    LocalCountProjectActivity(CGraphElementBase *_container) : BaseCountProjectActivity(_container)
    {
        setRequireInitData(false);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        anyThisGroup = false;
        PARENT::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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
    bool first = false; // until start
    Semaphore prevRecCountSem;
    rowcount_t prevRecCount = 0;
    std::atomic<rowcount_t> localRecCount = {RCUNSET};
    bool onInputFinishSends = false;

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
        if (!onInputFinishSends) // if local total count known at start() and lookahead not already sent, send now
        {
            ActPrintLog("COUNTPROJECT: row count pre-known to be %" RCPF "d", localRecCount.load());
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
        setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), COUNTPROJECT_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), true); // could spot disk write output here?
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        localRecCount = RCUNSET;
        onInputFinishSends = true;
        PARENT::start();
        first = true;
        prevRecCount = 0;
        ThorDataLinkMetaInfo info;
        input->getMetaInfo(info);
        if (info.totalRowsMin == info.totalRowsMax)
        {
            // NB: onInputFinished could have already set localRecCount before it gets here.
            rowcount_t expectedState = RCUNSET;
            if (localRecCount.compare_exchange_strong(expectedState, info.totalRowsMax))
                onInputFinishSends = false;
        }
    }
    virtual void stop()
    {
        if (first) // nextRow, therefore getPrevCount()/sendCount() never called
        {
            prevRecCount = count = getPrevCount();
            signalNext();
        }
        PARENT::stop();
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
        ActivityTimer t(slaveTimerStats, timeActivities);
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
    virtual void onInputFinished(rowcount_t _localRecCount)
    {
        /* localRecCount may already be known/set by start()
         * If it's known (set in start(), the count will be sent by nextRow() or stop()
         * If not known, send now that input read and total count known.
         */
        rowcount_t expectedState = RCUNSET;
        if (localRecCount.compare_exchange_strong(expectedState, _localRecCount))
        {
            prevRecCountSem.wait();
            if (!abortSoon)
            {
                ActPrintLog("count is %" RCPF "d", _localRecCount);
                sendCount(prevRecCount + _localRecCount);
            }
        }
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.isSequential = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


CActivityBase *createLocalCountProjectSlave(CGraphElementBase *container) { return new LocalCountProjectActivity(container); }
CActivityBase *createCountProjectSlave(CGraphElementBase *container) { return new CountProjectActivity(container); }

