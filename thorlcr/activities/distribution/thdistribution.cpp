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

#include "thdistribution.ipp"
#include "thexception.hpp"

class CDistributionActivityMaster : public CMasterActivity
{
public:
    CDistributionActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        serializeMPtag(dst, mpTag);
    }
    virtual void process()
    {
        CMasterActivity::process();

        IHThorDistributionArg * helper = (IHThorDistributionArg *)queryHelper();
        IOutputMetaData *rcSz = helper->queryInternalRecordSize();

        unsigned nslaves = container.queryJob().querySlaves();

        IDistributionTable * * result = (IDistributionTable * *)createThorRow(rcSz->getMinRecordSize()); // not a real row
        helper->clearAggregate(result);


        while (nslaves--)
        {
            rank_t sender;
            CMessageBuffer msg;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender))
                return;
            ::ActPrintLog(this, thorDetailedLogLevel, "Received distribution result from node %d", (unsigned)sender);
            if (msg.length())
                helper->merge(result, msg);
        }

        StringBuffer tmp;
        tmp.append("<XML>");
        helper->gatherResult(result, tmp);
        tmp.append("</XML>");
        ::ActPrintLog(this, thorDetailedLogLevel, "Distribution result: %s", tmp.str());
        helper->sendResult(tmp.length(), tmp.str());

        destroyThorRow(result);
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};


CActivityBase *createDistributionActivityMaster(CMasterGraphElement *container)
{
    return new CDistributionActivityMaster(container);
}
