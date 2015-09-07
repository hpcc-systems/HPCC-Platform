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

#include "jio.hpp"
#include "jtime.hpp"
#include "jfile.ipp"
#include "jsort.hpp"

#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thmfilemanager.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"

IRowStream *createFirstNReadSeqVar(IRowStream *input, unsigned limit)
{
    class CFirstNReadSeqVar : public CSimpleInterface, implements IRowStream
    {
        IRowStream *input;
        unsigned limit, c;
        bool stopped;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CFirstNReadSeqVar(IRowStream *_input, unsigned _limit) : input(_input), limit(_limit), c(0), stopped(false) { }
        ~CFirstNReadSeqVar() { ::Release(input); }

// IRowStream
        const void *nextRow()
        {
            if (c<limit)
            {
                OwnedConstThorRow row = input->nextRow();
                if (row)
                {
                    c++;
                    return row.getClear();
                }
            }
            stop();
            return NULL;
        }
        virtual void stop()
        {
            if (!stopped)
            {
                stopped = true;
                input->stop();
            }
        }
    };

    return new CFirstNReadSeqVar(input, limit);
}

class TopNSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    bool eos, eog, global, grouped, inputStopped;
    ICompare *compare;
    CThorExpandingRowArray sortedRows;
    Owned<IRowStream> out;
    IThorDataLink *input;
    IHThorTopNArg *helper;
    rowidx_t topNLimit;
    Owned<IRowServer> rowServer;
    MemoryBuffer topology;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    TopNSlaveActivity(CGraphElementBase *_container, bool _global, bool _grouped)
        : CSlaveActivity(_container), CThorDataLink(this), global(_global), grouped(_grouped), sortedRows(*this, this)
    {
        assertex(!(global && grouped));
        eog = eos = false;
        inputStopped = true;
    }
    ~TopNSlaveActivity()
    {
        out.clear();
        sortedRows.kill();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = (IHThorTopNArg *) queryHelper();
        topNLimit = RIUNSET;
        compare = helper->queryCompare();

        if (!container.queryLocalOrGrouped())
        {
            mpTag = container.queryJobChannel().deserializeMPTag(data);
            unsigned tSz;
            data.read(tSz);
            topology.append(tSz, data.readDirect(tSz));
        }
    }
    IRowStream *getNextSortGroup(IRowStream *input)
    {
        if (inputStopped) return NULL;
        sortedRows.clearRows(); // NB: In a child query, this will mean the rows ptr will remain at high-water mark
        loop
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                if (grouped)
                    break;
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            if (sortedRows.ordinality() < topNLimit)
                sortedRows.binaryInsert(row.getClear(), *compare);
            else
            {
                const void *lastRow = sortedRows.query(topNLimit-1);
                if (compare->docompare(lastRow, row) > 0)
                    sortedRows.binaryInsert(row.getClear(), *compare, true);
                else // had enough and out of range
                    ;
            }
        }
        rowidx_t sortedCount = sortedRows.ordinality();
        Owned<IRowStream> retStream;
        if (global || sortedCount)
        {
            retStream.setown(sortedRows.createRowStream());
            if (global)
            {
                unsigned indent = 0;
                loop
                {
                    if (topology.getPos()>=topology.length())
                        break;

                    IArrayOf<IRowStream> streams;
                    streams.append(*retStream.getClear());
                    loop
                    {
                        unsigned node;
                        topology.read(node);
                        if (!node)
                            break; // never reading from node 0 (0 == terminator)
                        StringBuffer s;
                        s.appendN(indent, ' ').append("Merging from node: ").append(node);
                        ActPrintLog("%s", s.str());
                        streams.append(*createRowStreamFromNode(*this, node+1, queryJobChannel().queryJobComm(), mpTag, abortSoon));
                    }
                    Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
                    retStream.setown(createRowStreamMerger(streams.ordinality(), streams.getArray(), compare, false, linkcounter));
                    retStream.setown(createFirstNReadSeqVar(retStream.getClear(), topNLimit));
                    indent += 2;
                }
                if (!firstNode())
                {
                    rowServer.setown(createRowServer(this, retStream, queryJobChannel().queryJobComm(), mpTag));
                    eos = true;
                    retStream.clear();
                }
            }
        }
        if (global || 0 == topNLimit || 0 == sortedCount)
        {
            doStopInput();
            if (!global || 0 == topNLimit)
                eos = true;
        }
        return retStream.getClear();
    }
    void doStopInput()
    {
        if (!inputStopped)
        {
            inputStopped = true;
            stopInput(input);
        }
    }
// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        input = inputs.item(0);
        startInput(input);
        inputStopped = false;
        // NB: topNLimit shouldn't be stupid size, resourcing will guarantee this
        __int64 _topNLimit = helper->getLimit();
        assertex(_topNLimit < RCIDXMAX); // hopefully never this big, but if were must be max-1 for binary insert
        topNLimit = (rowidx_t)_topNLimit;
        if (0 == topNLimit)
        {
            eos = true;
            doStopInput();
        }
        else
        {
            out.setown(getNextSortGroup(input));
            eos = false;
        }
        eog = false;
        dataLinkStart();
    }
    virtual bool isGrouped() { return grouped; }
    void stop()
    {
        if (out)
            out->stop();
        doStopInput();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (abortSoon || eos)
            return NULL;
        if (NULL == out)
        {
            out.setown(getNextSortGroup(input));
            if (NULL == out)
            {
                eos = true;
                return NULL;
            }
        }
        if (grouped)
        {
            OwnedConstThorRow row = out->nextRow();
            if (row)
            {
                eog = false;
                dataLinkIncrement();
                return row.getClear();
            }
            else
            {
                if (eog)
                {
                    out.setown(getNextSortGroup(input));
                    if (NULL == out)
                        eos = true;
                    else
                    {
                        OwnedConstThorRow row = out->nextRow();
                        verifyex(row);
                        eog = false;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                }
                else
                {
                    eog = true;
                    out.setown(getNextSortGroup(input));
                    if (NULL == out)
                        eos = true;
                }
            }
        }
        else
        {
            if (!global || firstNode())
            {
                OwnedConstThorRow row = out->nextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
        }
        return NULL;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        info.totalRowsMin=0;
        info.totalRowsMax = (RIUNSET==topNLimit) ? -1 : topNLimit; // but should always be set before getMetaInfo called
    }
};

activityslaves_decl CActivityBase *createGlobalTopNSlave(CGraphElementBase *container)
{
    return new TopNSlaveActivity(container, true, false);
}

activityslaves_decl CActivityBase *createLocalTopNSlave(CGraphElementBase *container)
{
    return new TopNSlaveActivity(container, false, false);
}

activityslaves_decl CActivityBase *createGroupedTopNSlave(CGraphElementBase *container)
{
    return new TopNSlaveActivity(container, false, true);
}
