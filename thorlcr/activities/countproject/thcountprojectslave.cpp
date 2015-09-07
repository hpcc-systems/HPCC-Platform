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

class BaseCountProjectActivity : public CSlaveActivity,  public CThorDataLink, implements ISmartBufferNotify
{
protected:
    IHThorCountProjectArg *helper;
    rowcount_t count;
    Owned<IThorDataLink> input;

    void start()
    {
        count = 0;
        dataLinkStart();
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    BaseCountProjectActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = NULL;
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorCountProjectArg *> (queryHelper());
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    virtual void onInputStarted(IException *)
    {
        // not needed
    }
    virtual bool startAsync() { return false; }
};


class LocalCountProjectActivity : public BaseCountProjectActivity
{
    bool anyThisGroup;

public:
    LocalCountProjectActivity(CGraphElementBase *container) : BaseCountProjectActivity(container)
    {
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ActPrintLog("COUNTPROJECT: Is Local");
        input.set(inputs.item(0));
        anyThisGroup = false;
        startInput(input);
        BaseCountProjectActivity::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        while (!abortSoon)
        {
            OwnedConstThorRow row(input->nextRow());
            if (!row)
            {
                if (anyThisGroup) 
                    break;
                row.setown(input->nextRow());
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
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual void onInputFinished(rowcount_t finalCount)
    {
        // no action required
    }
};


class CountProjectActivity : public BaseCountProjectActivity
{
private:
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
        BaseCountProjectActivity::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ActPrintLog( "COUNTPROJECT: Is Global");
        first = true;
        prevRecCount = 0;
        startInput(inputs.item(0));
        ThorDataLinkMetaInfo info;
        inputs.item(0)->getMetaInfo(info);
        localRecCount = (info.totalRowsMin == info.totalRowsMax) ? (rowcount_t)info.totalRowsMax : RCUNSET;
        input.setown(createDataLinkSmartBuffer(this, inputs.item(0), COUNTPROJECT_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, true, &container.queryJob().queryIDiskUsage())); // could spot disk write output here?
        input->start();
        BaseCountProjectActivity::start();
    }
    virtual void stop()
    {
        BaseCountProjectActivity::stop();
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
            OwnedConstThorRow row(input->nextRow()); // NB: lookahead ensures ungrouped
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
    virtual bool isGrouped() { return false; }
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
        calcMetaInfoSize(info,inputs.item(0));
    }
};


CActivityBase *createLocalCountProjectSlave(CGraphElementBase *container) { return new LocalCountProjectActivity(container); }
CActivityBase *createCountProjectSlave(CGraphElementBase *container) { return new CountProjectActivity(container); }

