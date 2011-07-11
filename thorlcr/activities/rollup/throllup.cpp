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

// Handling Ungrouped Rollup and Global Dedup
// (Grouped code in groupdedup)

#include "throllup.ipp"
#include "thbufdef.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/rollup/throllup.cpp $ $Id: throllup.cpp 62376 2011-02-04 21:59:58Z sort $");

class DedupRollupActivityMaster : public CMasterActivity
{
public:
    DedupRollupActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }

    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
};

CActivityBase *createDedupRollupActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
    {
        if (TAKdedup == container->getKind())
        {
            if (((IHThorDedupArg *)container->queryHelper())->compareAll())
                throw MakeThorException(0, "Global DEDUP,ALL is not supported");
        }
        return new DedupRollupActivityMaster(container);
    }
}

