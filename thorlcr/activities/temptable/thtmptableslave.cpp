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
#include "jprop.hpp"
#include "thormisc.hpp"
#include "thtmptableslave.ipp"
#include "thorport.hpp"
#include "thactivityutil.ipp"

class CTempTableSlaveActivity : public CSlaveActivity, public CThorDataLink
{
private:
    IHThorTempTableArg * helper;
    bool eof;
    unsigned currentRow;
    size32_t maxrecsize;
    bool isLocal, isLocalCache;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CTempTableSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }
    virtual bool isGrouped() { return false; }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        isLocalCache = isLocal = false;
        helper = static_cast <IHThorTempTableArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("TEMPTABLE", container.queryId());
        currentRow = 0;
        if (!isLocalCache)
        {
            isLocalCache = true;
            isLocal = container.queryOwnerId() && container.queryOwner().isLocalOnly();
        }
        eof = isLocal ? false : (container.queryJob().queryMyRank()>1);
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof || abortSoon)
            return NULL;
        RtlDynamicRowBuilder row(queryRowAllocator());
        size32_t sizeGot = helper->getRow(row, currentRow++);
        if (sizeGot)
        {
            dataLinkIncrement();
            return row.finalizeRowClear(sizeGot);
        }   
        eof = true;
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.unknownRowsOutput = false;
        if (isLocal || 1 == container.queryJob().queryMyRank())
            info.totalRowsMin = helper->numRows();
        else
            info.totalRowsMin = 0;
        info.totalRowsMax = info.totalRowsMin;
    }
};

CActivityBase *createTempTableSlave(CGraphElementBase *container)
{
    return new CTempTableSlaveActivity(container);
}
