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
public:
    AggregateSlaveBase(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    { 
        input = NULL;
        hadElement = false;
    }

    void sendResult(const void *row, IOutputRowSerializer *serializer)
    {
        CMessageBuffer mb;
        size32_t start = mb.length();
        size32_t sz = 0;
        mb.append(sz);
        if (row&&hadElement) {
            CMemoryRowSerializer mbs(mb);
            serializer->serialize(mbs,(const byte *)row);
            sz = mb.length()-start-sizeof(size32_t);
            mb.writeDirect(start,sizeof(size32_t),&sz);
        }
        container.queryJob().queryJobComm().send(mb, 0, masterMpTag);
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        masterMpTag = container.queryJob().deserializeMPTag(data);
    }

    virtual void doStopInput()
    {
        stopInput(input);
    }

protected:
    mptag_t masterMpTag;
    bool hadElement;
    IThorDataLink *input;
};

//

class AggregateSlaveActivity : public AggregateSlaveBase
{
    bool eof;
    IHThorAggregateArg * helper;
    bool inputStopped;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    AggregateSlaveActivity(CGraphElementBase *container) : AggregateSlaveBase(container)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        AggregateSlaveBase::init(data, slaveData);

        appendOutputLinked(this);
        helper = (IHThorAggregateArg *)queryHelper();

        if (firstNode())
        {
            mpTag = createReplyTag();
            slaveData.append((int &)mpTag);
        }
    }
    void abort()
    {
        AggregateSlaveBase::abort();
        if (firstNode())
            cancelReceiveMsg(0, mpTag);
    }
    const void * getResult()
    {
        CMessageBuffer mb;
        if (receiveMsg(mb, 0, mpTag)) {
            size32_t sz;
            mb.read(sz);
            if (sz) {
                CThorStreamDeserializerSource ds(sz,mb.readDirect(sz));
                RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
                size32_t sz = queryRowDeserializer()->deserialize(rowBuilder,ds);
                return rowBuilder.finalizeRowClear(sz);
            }
        }
        return NULL;
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        inputStopped = false;
        dataLinkStart("AGGREGATE", container.queryId());
        input = inputs.item(0);
        startInput(input);
        if (input->isGrouped()) ActPrintLog("AGGREGATE: Grouped mismatch");
    }
    void stop()
    {
        if (!inputStopped)
        {
            inputStopped = true;
            doStopInput();
        }
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
            loop {
                next.setown(input->ungroupedNextRow());
                if (!next)
                    break;
                sz = helper->processNext(resultcr, next);
            }
        }
        inputStopped = true;
        doStopInput();
        OwnedConstThorRow result(resultcr.finalizeRowClear(sz));
        sendResult(result.get(),queryRowSerializer()); // send partial result

        if (!firstNode())
            return NULL; 

        OwnedConstThorRow ret = getResult();
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        sz = helper->clearAggregate(resultcr);  
        return resultcr.finalizeRowClear(sz);
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
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
    bool inputStopped;
    Owned<IRowInterfaces> aggrowif;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    ThroughAggregateSlaveActivity(CGraphElementBase *container) : AggregateSlaveBase(container), partResult(NULL)
    {
        partResultSize = 0;
    }

    ~ThroughAggregateSlaveActivity()
    {
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        AggregateSlaveBase::init(data, slaveData);

        appendOutputLinked(this);
        helper = (IHThorThroughAggregateArg *)queryHelper();
    }

    void readRest()
    {
        loop {
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

    void doStopInput()
    {
        if (inputStopped) 
            return;
        inputStopped = true;
        OwnedConstThorRow partrow = partResult.finalizeRowClear(partResultSize);
        sendResult(partrow.get(),aggrowif->queryRowSerializer());
        AggregateSlaveBase::doStopInput();
        dataLinkStop();
    }

    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("THROUGHAGGREGATE", container.queryId());
        input = inputs.item(0);
        inputStopped = false;
        startInput(input);
        if (input->isGrouped()) ActPrintLog("THROUGHAGGREGATE: Grouped mismatch");
        aggrowif.setown(createRowInterfaces(helper->queryAggregateRecordSize(),queryActivityId(),queryCodeContext()));
        partResult.setAllocator(aggrowif->queryRowAllocator()).ensureRow();
        helper->clearAggregate(partResult);
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (inputStopped)
            return false;
        OwnedConstThorRow row = input->ungroupedNextRow();
        if (!row)
            return NULL;
        process(row);
        dataLinkIncrement();
        return row.getClear();
    }

    bool isGrouped() { return false; }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        inputs.item(0)->getMetaInfo(info);
    }
};


CActivityBase *createAggregateSlave(CGraphElementBase *container)
{
    return new AggregateSlaveActivity(container);
}

CActivityBase *createThroughAggregateSlave(CGraphElementBase *container)
{
    return new ThroughAggregateSlaveActivity(container);
}

