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
#include "eclhelper.hpp"
#include "thlimit.ipp"

#define NUMINPARALLEL 16

class CLimitActivityMaster : public CMasterActivity
{
public:
    CLimitActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(mpTag);
    }
    void process()
    {
        unsigned slaves = container.queryJob().querySlaves();
        IHThorLimitArg *helper = (IHThorLimitArg *)queryHelper();

        rowcount_t rowLimit = (rowcount_t)helper->getRowLimit();
        rowcount_t total = 0;
        while (slaves--)
        {
            CMessageBuffer mb;
            if (!receiveMsg(mb, RANK_ALL, mpTag, NULL))
                return;
            if (abortSoon)
                return;
            rowcount_t count;
            mb.read(count);
            total += count;
            if (total > rowLimit)
                break;
        }
        switch (container.getKind())
        {
            case TAKcreaterowlimit: 
            case TAKskiplimit: 
            {
                unsigned slaves = container.queryJob().querySlaves();
                CMessageBuffer mb;
                mb.append(total);
                container.queryJob().queryJobComm().send(mb, RANK_ALL_OTHER, mpTag);
                break;
            }
            case TAKlimit:
            {
                if (total > rowLimit)
                    helper->onLimitExceeded();
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


CActivityBase *createLimitActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocal())
        return new CMasterActivity(container);
    else
        return new CLimitActivityMaster(container);
}
