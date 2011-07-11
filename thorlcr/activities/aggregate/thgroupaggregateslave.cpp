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

#include "thgroupaggregateslave.ipp"



static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/aggregate/thgroupaggregateslave.cpp $ $Id: thgroupaggregateslave.cpp 62376 2011-02-04 21:59:58Z sort $");



class GroupAggregateSlaveActivity : public CSlaveActivity, public CThorDataLink
{

private:
    bool eof;
    IHThorAggregateArg * helper;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    GroupAggregateSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    { 
        input = NULL;
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorAggregateArg *> (queryHelper());
    }

    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = false;
        input=inputs.item(0);
        startInput(input);
        dataLinkStart("GROUPAGGREGATE", container.queryId());
        if (!input->isGrouped())
            ActPrintLog("WARNING: non-grouped data as input to GroupAggregate");
    }

    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon || eof)
            return NULL;
        RtlDynamicRowBuilder out(queryRowAllocator());
        size32_t sz = helper->clearAggregate(out);
        OwnedConstThorRow row = input->nextRow();
        if (row)
        {
            sz = helper->processFirst(out, row);
            loop
            {
                row.setown(input->nextRow());
                if (!row)
                    break;
                sz = helper->processNext(out, row);
            }
            if (!input->isGrouped())
                eof = true;
        }
        else
        {
            eof = true;
            if (input->isGrouped())
                return NULL;
        }
        dataLinkIncrement();
        return out.finalizeRowClear(sz);
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.fastThrough = true;
    }

    virtual bool isGrouped() { return false; }
};


CActivityBase *createGroupAggregateSlave(CGraphElementBase *container)
{
    return new GroupAggregateSlaveActivity(container);
}



