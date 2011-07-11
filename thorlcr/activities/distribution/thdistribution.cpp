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

#include "thdistribution.ipp"
#include "thexception.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/distribution/thdistribution.cpp $ $Id: thdistribution.cpp 63725 2011-04-01 17:40:45Z jsmith $");


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
#if THOR_TRACE_LEVEL >= 5
            ActPrintLog("Received distribution result from node %d", (unsigned)sender);
#endif
            if (msg.length())
                helper->merge(result, msg);
        }

        StringBuffer tmp;
        tmp.append("<XML>").newline();
        helper->gatherResult(result, tmp);
        tmp.append("</XML>");

#if THOR_TRACE_LEVEL >= 5
        ActPrintLog("Distribution result: %s", tmp.toCharArray());
#endif

        helper->sendResult(tmp.length(), tmp.toCharArray());

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
