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

#include "thactivityutil.ipp"
#include "tsorta.hpp"
#include "thgroupslave.ipp"


class GroupSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorGroupArg * helper;
    bool eogNext, prevEog, eof;
    bool rolloverEnabled, useRollover;
    IThorDataLink *input;
    Owned<IRowStream> stream, nextNodeStream;
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
            rank_t myNode = queryJobChannel().queryMyRank();
            nextNodeStream.setown(createRowStreamFromNode(*this, myNode+1, queryJobChannel().queryJobComm(), mpTag, abortSoon));
            stream.set(nextNodeStream);
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
            mpTag = container.queryJobChannel().deserializeMPTag(data);
            rolloverEnabled = true;
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        ActPrintLog(rolloverEnabled ? "GROUP: is global" : "GROUP: is local");
        eogNext = prevEog = eof = false;
        if (rolloverEnabled)
        {
            useRollover = !lastNode();
#ifdef _TESTING
            ActPrintLog("Node number = %d, Total Nodes = %d", queryJobChannel().queryMyRank(), container.queryJob().querySlaves());
#endif
        }

        input = inputs.item(0);
        stream.set(input);
        startInput(input);
        dataLinkStart();

        next.setown(getNext());

        if (rolloverEnabled && !firstNode())  // 1st node can have nothing to send
        {
            Owned<IThorRowCollector> collector = createThorRowCollector(*this, NULL, stableSort_none, rc_mixed, SPILL_PRIORITY_SPILLABLE_STREAM);
            Owned<IRowWriter> writer = collector->getWriter();
            if (next)
            {
                ActPrintLog("GROUP: Sending first group to previous node(%d)", queryJobChannel().queryMyRank()-1);
                loop
                {
                    writer->putRow(next.getLink());
                    if (abortSoon)
                        break; //always send group even when aborting
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
            ActPrintLog("GROUP: %" RCPF "d records to send", collector->numRows());
            Owned<IRowStream> strm = collector->getStream();
            rowServer.setown(createRowServer(this, strm, queryJobChannel().queryJobComm(), mpTag));
        }
    }
    virtual void stop()
    {
        if (nextNodeStream)
            nextNodeStream->stop();
        stopInput(input);
        dataLinkStop();
    }
    virtual void kill()
    {
        CSlaveActivity::kill();
        rowServer.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
