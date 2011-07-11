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

#include "thchoosesets.ipp"
#include "thbufdef.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/choosesets/thchoosesets.cpp $ $Id: thchoosesets.cpp 63725 2011-04-01 17:40:45Z jsmith $");


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
            container.queryJob().queryJobComm().send(msg, s+1, mpTag);
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
