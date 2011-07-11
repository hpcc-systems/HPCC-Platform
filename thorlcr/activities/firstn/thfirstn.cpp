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
#include "eclhelper.hpp"        // for IHThorFirstNArg

#include "thfirstn.ipp"
#include "thbufdef.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/firstn/thfirstn.cpp $ $Id: thfirstn.cpp 63725 2011-04-01 17:40:45Z jsmith $");

class CFirstNActivityMaster : public CMasterActivity
{
    static CriticalSection singlefirstnterm;
public:
    CFirstNActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    void process()
    {
        CMasterActivity::process();

        IHThorFirstNArg *helper = (IHThorFirstNArg *)queryHelper();

        rowcount_t limit = (rowcount_t)helper->getLimit();
        rowcount_t skip = validRC(helper->numToSkip());
        unsigned nslaves = container.queryJob().querySlaves();
        unsigned s = 1;
        loop
        {
            CMessageBuffer msgMb;
            msgMb.append(limit);
            msgMb.append(skip);
            container.queryJob().queryJobComm().send(msgMb, s, mpTag);
            msgMb.clear();
            if (!receiveMsg(msgMb, s, mpTag))
                return;
            rowcount_t read;
            msgMb.read(read);
            msgMb.read(skip);

            assertex(read <= limit);
            limit -= read;
            if (s == nslaves)
                break;
            ++s;
            if (0 == limit)
            {
                for (; s<=nslaves; s++)
                {
                    if (abortSoon) return;
                    CMessageBuffer msgMb;
                    msgMb.append((rowcount_t)0);
                    msgMb.append((rowcount_t)0);
                    container.queryJob().queryJobComm().send(msgMb, s, mpTag);
                    msgMb.clear();
                    receiveMsg(msgMb, s, mpTag);
                }
                break;
            }
        }
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

CriticalSection CFirstNActivityMaster::singlefirstnterm;

CActivityBase *createFirstNActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CFirstNActivityMaster(container);
}
