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

#include "thchoosesets.ipp"
#include "thbufdef.hpp"

class CChooseSetsActivityMaster : public CMasterActivity
{
public:
    CChooseSetsActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        serializeMPtag(dst, mpTag);
    }
};

CActivityBase *createChooseSetsActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CChooseSetsActivityMaster(container);
}

class CChooseSetsPlusActivityMaster : public CChooseSetsActivityMaster
{
public:
    CChooseSetsPlusActivityMaster(CMasterGraphElement * info) : CChooseSetsActivityMaster(info)
    {
    }
    virtual void process()
    {
        CChooseSetsActivityMaster::process();

        IHThorChooseSetsArg *helper = (IHThorChooseSetsArg *)queryHelper();
        unsigned numSets = helper->getNumSets();
        unsigned nslaves = container.queryJob().querySlaves();

        MemoryBuffer countMb;
        rowcount_t *counts = (rowcount_t *)countMb.reserveTruncate((numSets*(nslaves+2)) * sizeof(rowcount_t));
        rowcount_t *totals = counts + nslaves*numSets;
        rowcount_t *tallies = totals + numSets;
        memset(counts, 0, countMb.length());

        unsigned s=nslaves;
        CMessageBuffer msg;
        while (s--)
        {
            msg.clear();
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender))
                return;
            assertex(msg.length() == numSets*sizeof(rowcount_t));
            unsigned set = (unsigned)sender - 1;
            memcpy(&counts[set*numSets], msg.toByteArray(), numSets*sizeof(rowcount_t));
        }
        for (s=0; s<nslaves; s++)
        {
            unsigned i=0;
            for (; i<numSets; i++)
                totals[i] += counts[s * numSets + i];
        }
        msg.clear();
        msg.append(numSets*sizeof(rowcount_t), totals);
        unsigned endTotalsPos = msg.length();
        for (s=0; s<nslaves; s++)
        {
            msg.rewrite(endTotalsPos);
            msg.append(numSets*sizeof(rowcount_t), tallies);
            queryJobChannel().queryJobComm().send(msg, s+1, mpTag);
            unsigned i=0;
            for (; i<numSets; i++)
                tallies[i] += counts[s * numSets + i];
        }
    }
    virtual void abort()
    {
        CChooseSetsActivityMaster::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

CActivityBase *createChooseSetsPlusActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CChooseSetsPlusActivityMaster(container);
}
