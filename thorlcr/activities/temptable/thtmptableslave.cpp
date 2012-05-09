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

/*
 * Deprecated in 3.8, this class is being kept for backward compatibility,
 * since now the code generator is using InlineTables (below) for all
 * temporary tables and rows.
 */
class CTempTableSlaveActivity : public CSlaveActivity, public CThorDataLink
{
private:
    IHThorTempTableArg * helper;
    bool empty;
    unsigned currentRow;
    unsigned numRows;
    size32_t maxrecsize;
    bool isLocal;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CTempTableSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }
    virtual bool isGrouped() { return false; }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        isLocal = false;
        helper = static_cast <IHThorTempTableArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("TEMPTABLE", container.queryId());
        currentRow = 0;
        isLocal = container.queryOwnerId() && container.queryOwner().isLocalOnly();
        empty = isLocal ? false : !firstNode();
        numRows = helper->numRows();
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (empty || abortSoon)
            return NULL;
        // Filtering empty rows, returns the next valid row
        while (currentRow < numRows) {
            RtlDynamicRowBuilder row(queryRowAllocator());
            size32_t sizeGot = helper->getRow(row, currentRow++);
            if (sizeGot)
            {
                dataLinkIncrement();
                return row.finalizeRowClear(sizeGot);
            }
        }
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.unknownRowsOutput = false;
        if (isLocal || firstNode())
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

/*
 * This class is essentially a temp table, but this could be used for creating massive
 * test tables and will also be used when optimising NORMALISE(ds, count) to
 * DATASET(count, transform), so could end up consuming a lot of rows.
 *
 * It also uses an extended version of the helper (to get distributed flag), and it was
 * simple enough to clone. Should be simpler to deprecate the old 32bit temp table
 * in the future, too.
 */
class CInlineTableSlaveActivity : public CSlaveActivity, public CThorDataLink
{
private:
    IHThorInlineTableArg * helper;
    __uint64 startRow;
    __uint64 currentRow;
    __uint64 maxRow;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CInlineTableSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }
    virtual bool isGrouped() { return false; }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorInlineTableArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("InlineTABLE", container.queryId());
        __uint64 numRows = helper->numRows();
        // local when generated from a child query (the range is per node, don't split)
        bool isLocal = container.queryOwnerId() && container.queryOwner().isLocalOnly();
        if (!isLocal && ((helper->getFlags() & TTFdistributed) != 0))
        {
            __uint64 nodes = container.queryCodeContext()->getNodes();
            __uint64 nodeid = container.queryCodeContext()->getNodeNum();
            startRow = (nodeid * numRows) / nodes;
            maxRow = ((nodeid + 1) * numRows) / nodes;
            ActPrintLog("InlineSLAVE: numRows = %"I64F"d, nodes = %"I64F
                        "d, nodeid = %"I64F"d, start = %"I64F"d, max = %"I64F"d",
                        numRows, nodes, nodeid, startRow, maxRow);
        }
        else
        {
            startRow = 0;
            // when not distributed, only first node compute, unless local
            if (firstNode() || isLocal)
                maxRow = numRows;
            else
                maxRow = 0;
        }
        currentRow = startRow;
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon)
            return NULL;
        while (currentRow < maxRow) {
            RtlDynamicRowBuilder row(queryRowAllocator());
            size32_t sizeGot = helper->getRow(row, currentRow++);
            if (sizeGot)
            {
                dataLinkIncrement();
                return row.finalizeRowClear(sizeGot);
            }
        }
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.unknownRowsOutput = false;
        info.totalRowsMin = info.totalRowsMax = maxRow - startRow;
    }
};

CActivityBase *createInlineTableSlave(CGraphElementBase *container)
{
    return new CInlineTableSlaveActivity(container);
}
