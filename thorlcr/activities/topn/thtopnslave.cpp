/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    const void **sortPtrsPtr;
    MemoryBuffer sortPtrs;
    IRowStream *out;
    IThorDataLink *input;
    IHThorTopNArg *helper;
    unsigned topNLimit, sortCount;
    Owned<IRowServer> rowServer;
    MemoryBuffer topology;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    TopNSlaveActivity(CGraphElementBase *_container, bool _global, bool _grouped) : CSlaveActivity(_container), CThorDataLink(this), global(_global), grouped(_grouped)
    {
        assertex(!(global && grouped));
        out = NULL;
        sortPtrsPtr = NULL;
        sortCount = 0;
        eog = eos = false;
        inputStopped = true;
    }
    ~TopNSlaveActivity()
    {
        ::Release(out);
        freeSortGroup();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = (IHThorTopNArg *) queryHelper();
        rowcount_t _topNLimit = (rowcount_t)helper->getLimit();
        topNLimit = (unsigned)_topNLimit;
        assertex(_topNLimit == (rowcount_t)_topNLimit);
        compare = helper->queryCompare();

        if (!container.queryLocalOrGrouped())
        {
            mpTag = container.queryJob().deserializeMPTag(data);
            unsigned tSz;
            data.read(tSz);
            topology.append(tSz, data.readDirect(tSz));
        }
        sortPtrsPtr = (const void **)sortPtrs.reserveTruncate((topNLimit+1) * sizeof(void *));
    }
    void freeSortGroup()
    {
        if (sortPtrsPtr)
        {
            while (sortCount--)
            {
                const void *row = sortPtrsPtr[sortCount];
                ReleaseThorRow(row);
            }
        }
    }
    IRowStream *getNextSortGroup(IRowStream *input)
    {
        if (inputStopped) return NULL;
        ::Release(out);
        out = NULL;
        freeSortGroup();
        sortCount = 0;
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
            if (sortCount < topNLimit)
            {
                binary_vec_insert_stable(row.getClear(), sortPtrsPtr, sortCount, *compare); // sortPtrsPtr owns
                sortCount++;
            }
            else
            {
                byte *lastRow = (byte *)*(sortPtrsPtr+(topNLimit-1));
                if (compare->docompare(lastRow, row) > 0)
                {
                    binary_vec_insert_stable(row.getLink(), sortPtrsPtr, topNLimit, *compare);
                    lastRow = (byte *)*(sortPtrsPtr+topNLimit); // Nth+1, fall out now free.
                    ReleaseThorRow(lastRow);
                }
                else // had enough and out of range
                    ;
            }
        }
        if (global || sortCount)
        {
            class CRowArrayStream : public CSimpleInterface, implements IRowStream
            {
                const void **sortPtrsPtr;
                unsigned count, pos;
            public:
                IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

                CRowArrayStream(const void **_sortPtrsPtr, unsigned _count) : sortPtrsPtr(_sortPtrsPtr), count(_count), pos(0)
                {
                }
            // IRowStream
                const void *nextRow()
                {
                    if (pos < count)
                    {
                        const void *row = sortPtrsPtr[pos++];
                        LinkThorRow(row);
                        return row;
                    }
                    return NULL;
                }
                void stop() { count = pos = 0; }
            };
            out = new CRowArrayStream(sortPtrsPtr, sortCount);
            if (global)
            {
                unsigned indent = 0;
                loop
                {
                    if (topology.getPos()>=topology.length())
                        break;

                    IArrayOf<IRowStream> streams;
                    streams.append(*out);
                    loop
                    {
                        unsigned node;
                        topology.read(node);
                        if (!node)
                            break; // never reading from node 0 (0 == terminator)
                        StringBuffer s;
                        s.appendN(indent, ' ').append("Merging from node: ").append(node);
                        ActPrintLog("%s", s.str());
                        streams.append(*createRowStreamFromNode(*this, node+1, container.queryJob().queryJobComm(), mpTag, abortSoon));
                    }
                    Owned<IRowLinkCounter> linkcounter = new CThorRowLinkCounter;
                    out = createRowStreamMerger(streams.ordinality(), streams.getArray(), compare, false, linkcounter);
                    out = createFirstNReadSeqVar(out, topNLimit);
                    indent += 2;
                }
                if (!firstNode())
                {
                    rowServer.setown(createRowServer(this, out, container.queryJob().queryJobComm(), mpTag));
                    eos = true;
                    ::Release(out);
                    out = NULL;
                }
            }
        }
        if (global || 0 == topNLimit || 0 == sortCount)
        {
            doStopInput();
            if (!global || 0 == topNLimit)
                eos = true;
        }
        return out;
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
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        inputStopped = false;
        // NB: topNLimit shouldn't be stupid size, resourcing will guarantee this
        if (0 == topNLimit)
        {
            eos = true;
            doStopInput();
        }
        else
        {
            out = getNextSortGroup(input);
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon || eos)
            return NULL;
        if (NULL == out)
        {
            out = getNextSortGroup(input);
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
                    out = getNextSortGroup(input);
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
                    out = getNextSortGroup(input);
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
        info.totalRowsMax=topNLimit;
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
