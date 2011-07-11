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

#include "thlookupjoin.ipp"

#include "thexception.hpp"
#include "thbufdef.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/lookupjoin/thlookupjoin.cpp $ $Id: thlookupjoin.cpp 62376 2011-02-04 21:59:58Z sort $");

class CLookupJoinActivityMaster : public CMasterActivity
{
public:
    CLookupJoinActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
};

CActivityBase *createLookupJoinActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocal())
        return new CMasterActivity(container);
    else
        return new CLookupJoinActivityMaster(container);
}
