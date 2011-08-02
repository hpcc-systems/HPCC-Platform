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

#include "thcatch.ipp"

class CSkipCatchActivity : public CMasterActivity
{
    Owned<IBarrier> barrier;
public:
    CSkipCatchActivity(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJob().createBarrier(mpTag));
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    virtual void process()
    {
        CMasterActivity::process();
        barrier->wait(false);
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        barrier->cancel();
    }
};

CActivityBase *createSkipCatchActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CSkipCatchActivity(container);
}

