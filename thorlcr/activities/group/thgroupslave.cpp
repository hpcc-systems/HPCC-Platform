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

#include "thactivityutil.ipp"
#include "tsorta.hpp"
#include "thgroupslave.ipp"


class GroupSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorGroupArg * helper;
    bool eogNext, prevEog, eof;
    bool rolloverEnabled, useRollover;
    IThorDataLink *input;
    Owned<IRowStream> stream;
    OwnedConstThorRow next;
    Owned<IRowServer> rowServer;

    const void *getNext()
    {
        const void *row = stream->ungroupedNextRow();
        if (row)
            return row;
        else if (useRollover)
        {
            useRollover = false;
            // JCSMORE will generate time out log messages, while waiting for next nodes group
            rank_t myNode = container.queryJob().queryMyRank();
            stream.setown(createRowStreamFromNode(*this, myNode+1, container.queryJob().queryJobComm(), mpTag, abortSoon));
            return stream->nextRow();
        }
        else
            return NULL;
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    GroupSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = static_cast <IHThorGroupArg *> (queryHelper());
        rolloverEnabled = false;
        useRollover = false;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        if (!container.queryLocalOrGrouped())
        {
            mpTag = container.queryJob().deserializeMPTag(data);
            rolloverEnabled = true;
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ActPrintLog(rolloverEnabled ? "GROUP: is global" : "GROUP: is local");
        eogNext = prevEog = eof = false;
        if (rolloverEnabled)
        {
            useRollover = !lastNode();
#ifdef _TESTING
            ActPrintLog("Node number = %d, Total Nodes = %d", container.queryJob().queryMyRank(), container.queryJob().querySlaves());
#endif
        }

        input = inputs.item(0);
        stream.set(input);
        startInput(input);
        dataLinkStart("GROUP", container.queryId());        

        next.setown(stream->ungroupedNextRow());

        if (rolloverEnabled && !firstNode())  // 1st node can have nothing to send
        {
            rowcount_t sentRecs = 0;
            Owned<IThorRowCollector> collector = createThorRowCollector(*this, NULL, false, rc_mixed, SPILL_PRIORITY_SPILLABLE_STREAM);
            Owned<IRowWriter> writer = collector->getWriter();
            if (next)
            {
                ActPrintLog("GROUP: Sending first group to previous node(%d)", container.queryJob().queryMyRank()-1);
                loop
                {
                    writer->putRow(next.getLink());
                    if (abortSoon)
                        break; //always send group even when aborting
                    sentRecs++;
                    OwnedConstThorRow next2 = getNext();
                    if (!next2)
                    {
                        eof = true;
                        break;
                    }
                    else if (!helper->isSameGroup(next2, next))
                    {
                        next.setown(next2.getClear());
                        break;
                    }
                    next.setown(next2.getClear());
                }
            }
            writer.clear();
            ActPrintLog("GROUP: %"RCPF"d records to send", collector->numRows());
            Owned<IRowStream> strm = collector->getStream();
            rowServer.setown(createRowServer(this, strm, container.queryJob().queryJobComm(), mpTag));
        }
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eogNext || eof)
        {
            eogNext = false;
            return NULL;
        }
        
        OwnedConstThorRow prev = next.getClear();
        next.setown(getNext());
        if (next && !helper->isSameGroup(prev, next))
            eogNext = true;
        if (prev)
        {
            dataLinkIncrement();
            return prev.getClear();
        }
        if (prevEog)
            eof = true;
        prevEog = true;
        return NULL;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (rolloverEnabled)
        {
            info.isSequential = true;
            info.unknownRowsOutput = true; // don't know how many rolled over
        }
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual bool isGrouped() { return true; }
};


CActivityBase *createGroupSlave(CGraphElementBase *container)
{
    return new GroupSlaveActivity(container);
}
