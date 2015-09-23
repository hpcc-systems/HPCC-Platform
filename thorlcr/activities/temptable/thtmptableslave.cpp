/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "platform.h"
#include "jprop.hpp"
#include "thormisc.hpp"
#include "thtmptableslave.ipp"
#include "thorport.hpp"
#include "thactivityutil.ipp"

/*
 * This class can be used for creating massive temp tables and will also be used
 * when optimising NORMALISE(ds, count) to DATASET(count, transform), so could end
 * up consuming a lot of rows.
 *
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

    CInlineTableSlaveActivity(CGraphElementBase *_container)
    : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = NULL;
        startRow = 0;
        currentRow = 0;
        maxRow = 0;
    }
    virtual bool isGrouped() { return false; }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorInlineTableArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        dataLinkStart();
        __uint64 numRows = helper->numRows();
        // local when generated from a child query (the range is per node, don't split)
        bool isLocal = container.queryLocalData() || container.queryOwner().isLocalChild();
        if (!isLocal && ((helper->getFlags() & TTFdistributed) != 0))
        {
            __uint64 nodes = queryCodeContext()->getNodes();
            __uint64 nodeid = queryCodeContext()->getNodeNum();
            startRow = (nodeid * numRows) / nodes;
            maxRow = ((nodeid + 1) * numRows) / nodes;
            ActPrintLog("InlineSLAVE: numRows = %" I64F "d, nodes = %" I64F
                        "d, nodeid = %" I64F "d, start = %" I64F "d, max = %" I64F "d",
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
        ActivityTimer t(totalCycles, timeActivities);
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
