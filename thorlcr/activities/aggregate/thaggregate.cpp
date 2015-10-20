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

#include "jlib.hpp"
#include "mpbase.hpp"
#include "mputil.hpp"
#include "thmem.hpp"
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
                Owned<IRowInterfaces> aggRowIf = createRowInterfaces(helper->queryAggregateRecordSize(), queryId(), queryCodeContext());
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




