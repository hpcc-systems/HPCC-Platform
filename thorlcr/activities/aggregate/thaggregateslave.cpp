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

class AggregateSlaveBase : public CSlaveActivity, public CThorDataLink
{
protected:
    bool hadElement, inputStopped;
    IThorDataLink *input;

    void doStopInput()
    {
        if (inputStopped)
            return;
        inputStopped = true;
        stopInput(input);
    }
    void doStart()
    {
        hadElement = false;
        inputStopped = false;
        input = inputs.item(0);
        startInput(input);
        if (input->isGrouped())
            ActPrintLog("Grouped mismatch");
    }
    const void *getResult(const void *firstRow)
    {
        IHThorAggregateArg *helper = (IHThorAggregateArg *)baseHelper.get();
        unsigned numPartialResults = container.queryJob().querySlaves();
        if (1 == numPartialResults)
            return firstRow;

        CThorExpandingRowArray partialResults(*this, this, true, stableSort_none, true, numPartialResults);
        if (hadElement)
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
    AggregateSlaveBase(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        input = NULL;
        hadElement = inputStopped = false;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocal())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
        appendOutputLinked(this);
    }
    virtual bool isGrouped() { return false; }
};

//

class AggregateSlaveActivity : public AggregateSlaveBase
{
    bool eof;
    IHThorAggregateArg * helper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

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
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        doStart();
        eof = false;
        dataLinkStart();
    }
    virtual void stop()
    {
        doStopInput();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (abortSoon || eof)
            return NULL;
        eof = true;

        OwnedConstThorRow next = input->ungroupedNextRow();
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
                    next.setown(input->ungroupedNextRow());
                    if (!next)
                        break;
                    sz = helper->processNext(resultcr, next);
                }
            }
        }
        doStopInput();
        if (!firstNode())
        {
            OwnedConstThorRow result(resultcr.finalizeRowClear(sz));
            sendResult(result.get(),queryRowSerializer(), 1); // send partial result
            return NULL;
        }
        OwnedConstThorRow ret = getResult(resultcr.finalizeRowClear(sz));
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        sz = helper->clearAggregate(resultcr);  
        return resultcr.finalizeRowClear(sz);
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
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
    IHThorThroughAggregateArg *helper;
    RtlDynamicRowBuilder partResult;
    size32_t partResultSize;
    Owned<IRowInterfaces> aggrowif;

    void doStopInput()
    {
        OwnedConstThorRow partrow = partResult.finalizeRowClear(partResultSize);
        if (!firstNode())
            sendResult(partrow.get(), aggrowif->queryRowSerializer(), 1);
        else
        {
            OwnedConstThorRow ret = getResult(partrow.getClear());
            sendResult(ret, aggrowif->queryRowSerializer(), 0); // send to master
        }
        AggregateSlaveBase::doStopInput();
        dataLinkStop();
    }
    void readRest()
    {
        loop
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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    ThroughAggregateSlaveActivity(CGraphElementBase *container) : AggregateSlaveBase(container), partResult(NULL)
    {
        helper = (IHThorThroughAggregateArg *)queryHelper();
        partResultSize = 0;
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        doStart();
        aggrowif.setown(createRowInterfaces(helper->queryAggregateRecordSize(),queryActivityId(),queryCodeContext()));
        partResult.setAllocator(aggrowif->queryRowAllocator()).ensureRow();
        helper->clearAggregate(partResult);
        dataLinkStart();
    }
    virtual void stop()
    {
        if (inputStopped) 
            return;
        readRest();
        doStopInput();
        //GH: Shouldn't there be something like the following - in all activities with a member, otherwise the allocator may have gone??
        partResult.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (inputStopped)
            return NULL;
        OwnedConstThorRow row = input->ungroupedNextRow();
        if (!row)
            return NULL;
        process(row);
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        inputs.item(0)->getMetaInfo(info);
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

