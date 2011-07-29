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

#include "jlib.hpp"
#include "mpbase.hpp"
#include "mputil.hpp"

#include "thaggregate.ipp"
#include "thexception.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"

class CAggregateMasterBase : public CMasterActivity
{
    mptag_t replyTag;
public:
    CAggregateMasterBase(CMasterGraphElement * info) : CMasterActivity(info)
    {
        replyTag = TAG_NULL;
    }
    void init()
    {
        replyTag = createReplyTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(replyTag);
    }
    const void *getResult(IRowInterfaces &rowIf)
    {
        IHThorAggregateArg *helper = (IHThorAggregateArg *)queryHelper();
        unsigned numPartialResults = container.queryJob().querySlaves();

        CThorRowArray partialResults;
        partialResults.reserve(numPartialResults);

        size32_t sz;
        while (numPartialResults--)
        {
            CMessageBuffer msg;
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, replyTag, &sender))
                return NULL;
            if (abortSoon) 
                return NULL;
            msg.read(sz);
            if (sz)
            {
                assertex(NULL == partialResults.item(sender-1));
                CThorStreamDeserializerSource mds(sz, msg.readDirect(sz));
                RtlDynamicRowBuilder rowBuilder(rowIf.queryRowAllocator());
                size32_t sz = rowIf.queryRowDeserializer()->deserialize(rowBuilder, mds);
                partialResults.setRow(sender-1, rowBuilder.finalizeRowClear(sz));
            }
        }

        RtlDynamicRowBuilder result(rowIf.queryRowAllocator(), false);
        bool first = true;
        numPartialResults = container.queryJob().querySlaves();
        unsigned p=0;
        for (;p<numPartialResults; p++)
        {
            const void *row = partialResults.item(p);
            if (row)
            {
                if (first)
                {
                    first = false;
                    sz = cloneRow(result, partialResults.item(p), rowIf.queryRowMetaData());
                }
                else
                    sz = helper->mergeAggregate(result, row);
            }
        }
        if (first)
            sz = helper->clearAggregate(result);
        return result.finalizeRowClear(sz);
    }
    void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, replyTag);
    }
};

class CAggregateMaster : public CAggregateMasterBase
{
public:
    CAggregateMaster(CMasterGraphElement * info) : CAggregateMasterBase(info) { }

    void process()
    {
        mptag_t slaveTag = container.queryJob().deserializeMPTag(queryInitializationData(0));
        CMessageBuffer msg;
        size32_t sz = 0;
        msg.append(sz);
        OwnedConstThorRow result = getResult(*this);
        if (result)
        {
            CMemoryRowSerializer ms(msg);
            queryRowSerializer()->serialize(ms, (const byte *)result.get());
            sz = msg.length()-sizeof(sz);
            msg.writeDirect(0, sizeof(sz), &sz);
        }
        if (!container.queryJob().queryJobComm().send(msg, 1, slaveTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
};


CActivityBase *createAggregateActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CAggregateMaster(container);
}


class CThroughAggregateMaster : public CAggregateMasterBase
{
public:
    CThroughAggregateMaster(CMasterGraphElement *info) : CAggregateMasterBase(info) { }
    void process()
    {
        IHThorThroughAggregateArg *helper = (IHThorThroughAggregateArg *)queryHelper();
        Owned<IRowInterfaces> aggRowIf = createRowInterfaces(helper->queryAggregateRecordSize(), queryActivityId(), queryCodeContext());
        OwnedConstThorRow result = getResult(*aggRowIf);
        helper->sendResult(result.get());
    }
};

CActivityBase *createThroughAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CThroughAggregateMaster(info);
}




