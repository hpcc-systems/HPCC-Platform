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

#include "platform.h"
#include "jlib.hpp"
#include "jiface.hpp"       // IInterface defined in jlib
#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mptag.hpp"

#include "eclhelper.hpp"        // for IHThorAggregateArg
#include "slave.ipp"
#include "thbufdef.hpp"

#include "thactivityutil.ipp"
#include "thaggregateslave.ipp"

class AggregateSlaveBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
protected:
    bool hadElement = false;

    virtual void start() override
    {
        PARENT::start();
        hadElement = false;
        if (input->isGrouped())
            ActPrintLog("Grouped mismatch");
    }
    const void *getResult(const void *firstRow)
    {
        IHThorAggregateArg *helper = (IHThorAggregateArg *)baseHelper.get();
        unsigned numPartialResults = container.queryJob().querySlaves();
        if (1 == numPartialResults)
            return firstRow;

        CThorExpandingRowArray partialResults(*this, this, ers_allow, stableSort_none, true, numPartialResults);
        if (firstRow)
            partialResults.setRow(0, firstRow);
        --numPartialResults;

        size32_t sz;
        while (numPartialResults--)
        {
            CMessageBuffer msg;
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender))
                return NULL;
            if (abortSoon)
                return NULL;
            msg.read(sz);
            if (sz)
            {
                assertex(NULL == partialResults.query(sender-1));
                CThorStreamDeserializerSource mds(sz, msg.readDirect(sz));
                RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
                size32_t sz = queryRowDeserializer()->deserialize(rowBuilder, mds);
                partialResults.setRow(sender-1, rowBuilder.finalizeRowClear(sz));
            }
        }
        RtlDynamicRowBuilder rowBuilder(queryRowAllocator(), false);
        bool first = true;
        numPartialResults = container.queryJob().querySlaves();
        unsigned p=0;
        for (;p<numPartialResults; p++)
        {
            const void *row = partialResults.query(p);
            if (row)
            {
                if (first)
                {
                    first = false;
                    sz = cloneRow(rowBuilder, row, queryRowMetaData());
                }
                else
                    sz = helper->mergeAggregate(rowBuilder, row);
            }
        }
        if (first)
            sz = helper->clearAggregate(rowBuilder);
        return rowBuilder.finalizeRowClear(sz);
    }
    void sendResult(const void *row, IOutputRowSerializer *serializer, rank_t dst)
    {
        CMessageBuffer mb;
        DelayedSizeMarker sizeMark(mb);
        if (row&&hadElement) {
            CMemoryRowSerializer mbs(mb);
            serializer->serialize(mbs,(const byte *)row);
            sizeMark.write();
        }
        queryJobChannel().queryJobComm().send(mb, dst, mpTag);
    }
public:
    AggregateSlaveBase(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        appendOutputLinked(this);
        if (container.queryLocal())
            setRequireInitData(false);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocal())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
};

//

class AggregateSlaveActivity : public AggregateSlaveBase
{
    typedef AggregateSlaveBase PARENT;

    bool eof;
    IHThorAggregateArg * helper;

public:
    AggregateSlaveActivity(CGraphElementBase *container) : AggregateSlaveBase(container)
    {
        helper = (IHThorAggregateArg *)queryHelper();
        eof = false;
    }
    virtual void abort()
    {
        AggregateSlaveBase::abort();
        if (firstNode())
            cancelReceiveMsg(1, mpTag);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = false;
    }
    virtual void stop() override
    {
        stopInput(0);
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon || eof)
            return NULL;
        eof = true;

        OwnedConstThorRow next = inputStream->ungroupedNextRow();
        RtlDynamicRowBuilder resultcr(queryRowAllocator());
        size32_t sz = helper->clearAggregate(resultcr);
        if (next)
        {
            hadElement = true;
            sz = helper->processFirst(resultcr, next);
            if (container.getKind() != TAKexistsaggregate)
            {
                while (!abortSoon)
                {
                    next.setown(inputStream->ungroupedNextRow());
                    if (!next)
                        break;
                    sz = helper->processNext(resultcr, next);
                }
            }
        }
        stopInput(0);
        if (!firstNode())
        {
            OwnedConstThorRow result(resultcr.finalizeRowClear(sz));
            sendResult(result.get(),queryRowSerializer(), 1); // send partial result
            return NULL;
        }
        OwnedConstThorRow ret = getResult(hadElement ? resultcr.finalizeRowClear(sz) : nullptr);
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        sz = helper->clearAggregate(resultcr);  
        return resultcr.finalizeRowClear(sz);
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.singleRowOutput = true;
        info.totalRowsMin=1;
        info.totalRowsMax=1;
    }
};

//

class ThroughAggregateSlaveActivity : public AggregateSlaveBase
{
    typedef AggregateSlaveBase PARENT;

    IHThorThroughAggregateArg *helper;
    RtlDynamicRowBuilder partResult;
    size32_t partResultSize;
    Owned<IThorRowInterfaces> aggrowif;

    void readRest()
    {
        for (;;)
        {
            OwnedConstThorRow row = ungroupedNextRow();
            if (!row)
                break;
        }
    }
    void process(const void *r)
    {
        if (hadElement)
            partResultSize = helper->processNext(partResult, r);
        else
        {
            partResultSize = helper->processFirst(partResult, r);
            hadElement = true;
        }
    }
public:
    ThroughAggregateSlaveActivity(CGraphElementBase *container) : AggregateSlaveBase(container), partResult(NULL)
    {
        helper = (IHThorThroughAggregateArg *)queryHelper();
        partResultSize = 0;
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        aggrowif.setown(createRowInterfaces(helper->queryAggregateRecordSize()));
        partResult.setAllocator(aggrowif->queryRowAllocator()).ensureRow();
        helper->clearAggregate(partResult);
    }
    virtual void stop() override
    {
        if (inputStopped) 
            return;
        readRest();
        OwnedConstThorRow partrow = partResult.finalizeRowClear(partResultSize);
        if (!firstNode())
            sendResult(partrow.get(), aggrowif->queryRowSerializer(), 1);
        else
        {
            OwnedConstThorRow ret = getResult(partrow.getClear());
            sendResult(ret, aggrowif->queryRowSerializer(), 0); // send to master
        }
        stopInput(0);
        //GH: Shouldn't there be something like the following - in all activities with a member, otherwise the allocator may have gone??
        partResult.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (inputStopped) // JCSMORE - this should not be necessary, nextRow() should never be called after stop()
            return NULL;
        OwnedConstThorRow row = inputStream->ungroupedNextRow();
        if (!row)
            return NULL;
        process(row);
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        queryInput(0)->getMetaInfo(info);
    }
};


CActivityBase *createAggregateSlave(CGraphElementBase *container)
{
    //NB: only used if global, createGroupAggregateSlave used if child,local or grouped
    return new AggregateSlaveActivity(container);
}

CActivityBase *createThroughAggregateSlave(CGraphElementBase *container)
{
    if (container->queryOwner().queryOwner() && !container->queryOwner().isGlobal())
        throwUnexpected();
    return new ThroughAggregateSlaveActivity(container);
}

