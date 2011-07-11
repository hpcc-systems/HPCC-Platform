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
#include <limits.h>
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"

#include "thnullactionslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/nullaction/thnullactionslave.cpp $ $Id: thnullactionslave.cpp 62376 2011-02-04 21:59:58Z sort $");


class CNullActionSlaveActivity : public CSlaveActivity, public CThorDataLink
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNullActionSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }
    ~CNullActionSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    } 
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("NULLACTION", container.queryId());
    }
    void stop() { dataLinkStop(); }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        return NULL;
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.totalRowsMin = info.totalRowsMax = 0;
        info.canReduceNumRows = true; // to 0 in fact
    }
};

CActivityBase *createNullActionSlave(CGraphElementBase *container)
{
    return new CNullActionSlaveActivity(container);
}



