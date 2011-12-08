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
public:
    CAggregateMasterBase(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
};


class CAggregateMaster : public CAggregateMasterBase
{
public:
    CAggregateMaster(CMasterGraphElement * info) : CAggregateMasterBase(info) { }
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
    CThroughAggregateMaster(CMasterGraphElement *info) : CAggregateMasterBase(info)
    {
    }
    void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(1, mpTag);
    }
    void process()
    {
        CMessageBuffer msg;
        if (receiveMsg(msg, 1, mpTag, NULL))
        {
            size32_t sz;
            msg.read(sz);
            if (sz)
            {
                IHThorThroughAggregateArg *helper = (IHThorThroughAggregateArg *)queryHelper();
                Owned<IRowInterfaces> aggRowIf = createRowInterfaces(helper->queryAggregateRecordSize(), queryActivityId(), queryCodeContext());
                CThorStreamDeserializerSource mds(sz, msg.readDirect(sz));
                RtlDynamicRowBuilder rowBuilder(aggRowIf->queryRowAllocator());
                size32_t sz = aggRowIf->queryRowDeserializer()->deserialize(rowBuilder, mds);
                OwnedConstThorRow result = rowBuilder.finalizeRowClear(sz);
                helper->sendResult(result);
            }
        }
    }
};

CActivityBase *createThroughAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CThroughAggregateMaster(info);
}




