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

#include "jio.hpp"
#include "jtime.hpp"
#include "jfile.ipp"
#include "jsort.hpp"

#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thbufdef.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "eclrtl_imp.hpp"
#include "thcompressutil.hpp"

class CNextRowFeeder : public CSimpleInterface, implements IThreaded, implements IRowStream
{
    CThreaded threaded;
    CActivityBase *activity;
    Owned<ISmartRowBuffer> smartbuf;
    Owned<IRowStream> in;
    bool stopped;
    Owned<IException> exception;
    IRowInterfaces *rowInterfaces;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNextRowFeeder(CActivityBase *_activity, IRowStream *_in) : in(_in), threaded("CNextRowFeeder"), activity(_activity), rowInterfaces(_activity)
    {
        stopped = true;
        smartbuf.setown(createSmartInMemoryBuffer(activity, SMALL_SMART_BUFFER_SIZE));
        threaded.init(this);
    }
    ~CNextRowFeeder()
    {
        stopThread();
    }
    void stopThread()
    {
        if (!stopped)
        {
            stopped = true;
            threaded.join();
        }
    }
    void main()
    {
        stopped = false;
        try
        {
            Linked<IRowWriter> writer = smartbuf->queryWriter();
            while (!stopped)
            {
                OwnedConstThorRow row = in->nextRow();
                if (!row)
                {
                    row.setown(in->nextRow());
                    if (!row)
                        break;
                    else
                        writer->putRow(NULL); // eog
                }
                writer->putRow(row.getClear());
            }
            writer->flush();

            in->stop();
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, NULL);
            exception.setown(e);
        }
    }
    virtual const void *nextRow()
    {
        OwnedConstThorRow row = smartbuf->nextRow();
        if (exception) 
            throw exception.getClear();
        return row.getClear();
    }
    virtual void stop()
    {
        if (smartbuf)
        {
            smartbuf->stop(); // just in case blocked
            stopThread();
            smartbuf.clear();
            if (exception) 
                throw exception.getClear();
        }
    }
};

#define EMPTY_LOOP_LIMIT 1000

class CLoopSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    IThorDataLink *input;
    bool global;
    bool sentEndLooping;
    unsigned maxIterations;
    unsigned loopCounter;
    rtlRowBuilder extractBuilder;

    void sendLoopingCount(unsigned n)
    {
        if (!global || container.queryLocalOrGrouped())
            return;
        CMessageBuffer msg; // inform master starting
        msg.append(n);
        container.queryJob().queryJobComm().send(msg, 0, mpTag);
        receiveMsg(msg, 0, mpTag);
    }
    void sendStartLooping()
    {
        sendLoopingCount(0);
    }
    void sendEndLooping()
    {
        if (sentEndLooping)
            return;
        sentEndLooping = true;
        sendLoopingCount(0);
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLoopSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        input = NULL;
        mpTag = TAG_NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().deserializeMPTag(data);
        global = !queryContainer().queryLoopGraph()->queryGraph()->isLocalOnly();
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (!container.queryLocalOrGrouped())
            cancelReceiveMsg(0, mpTag);
    }
    void dostart()
    {
        extractBuilder.clear();
        sentEndLooping = false;
        loopCounter = 1;
        input = inputs.item(0);
        startInput(input);
    }
    void doStop()
    {
        sendEndLooping();
        stopInput(input);
        dataLinkStop();
    }
// IThorDataLink
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
    void processDone(MemoryBuffer &mb)
    {
        CSlaveActivity::processDone(mb);
        ((CSlaveGraph *)queryContainer().queryLoopGraph()->queryGraph())->serializeDone(mb);
    }
};

class CLoopSlaveActivity : public CLoopSlaveActivityBase
{
    Owned<IRowStream> curInput;
    Owned<CNextRowFeeder> nextRowFeeder;
    Owned<IRowWriterMultiReader> loopPending;
    unsigned loopPendingCount;
    unsigned flags;
    IHThorLoopArg *helper;
    bool eof, finishedLooping;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLoopSlaveActivity(CGraphElementBase *container) : CLoopSlaveActivityBase(container)
    {
        helper = (IHThorLoopArg *) queryHelper();
        flags = helper->getFlags();
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dostart();
        eof = false;
        helper->createParentExtract(extractBuilder);
        maxIterations = helper->numIterations();
        loopPending.setown(createOverflowableBuffer(this, LOOP_SMART_BUFFER_SIZE));
        loopPendingCount = 0;
        finishedLooping = ((container.getKind() == TAKloopcount) && (maxIterations == 0));
        curInput.set(input);

        class CWrapper : public CSimpleInterface, implements IRowStream
        {
            CLoopSlaveActivity &activity;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
            CWrapper(CLoopSlaveActivity &_activity) : activity(_activity) { }
            virtual const void *nextRow()
            {
                return activity.getNextRow();
            }
            virtual void stop()
            {
                activity.doStop();
            }
        };
        dataLinkStart("LOOP", container.queryId());
        nextRowFeeder.setown(new CNextRowFeeder(this, new CWrapper(*this)));
    }
    void doStop()
    {
        loopPending.clear();
        CLoopSlaveActivityBase::doStop();
    }
    const void *getNextRow()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!abortSoon && !eof)
        {
            unsigned emptyIterations = 0;
            loop
            {
                loop
                {
                    OwnedConstThorRow ret = (void *)curInput->nextRow();
                    if (!ret)
                    {
                        ret.setown(curInput->nextRow()); // more cope with groups somehow....
                        if (!ret)
                        {
                            if (finishedLooping)
                            {
                                eof = true;
                                return NULL;
                            }
                            break;
                        }
                    }

                    if (finishedLooping || 
                        ((flags & IHThorLoopArg::LFfiltered) && !helper->sendToLoop(loopCounter, ret)))
                    {
                        dataLinkIncrement();
                        return ret.getClear();
                    }
                    ++loopPendingCount;
                    loopPending->putRow(ret.getClear());
                }

                switch (container.getKind())
                {
                case TAKloopdataset:
                    throwUnexpected();
/*
                    if (!helper->loopAgain(loopCounter, loopPendingCount, loopPending))
                    {
                        if (0 == loopPendingCount)
                        {
                            eof = true;
                            return NULL;
                        }

                        curInput.set(loopPending); // clear loopPending?
                        sendEndLooping();
                        finishedLooping = true;
                        continue;       // back to the input loop again
                    }
*/
                    break;
                case TAKlooprow:
                    if (0 == loopPendingCount)
                    {
                        sendEndLooping();
                        finishedLooping = true;
                        eof = true;
                        return NULL;
                    }
                    break;
                }

                if (loopPendingCount)
                    emptyIterations = 0;
                else
                {
                    //note: any outputs which didn't go around the loop again, would return the record, reinitializing emptyIterations
                    emptyIterations++;
                    if (emptyIterations > EMPTY_LOOP_LIMIT)
                        throw MakeActivityException(this, 0, "Executed LOOP with empty input and output %u times", emptyIterations);
                    if (emptyIterations % 32 == 0)
                        ActPrintLog("Executing LOOP with empty input and output %u times", emptyIterations);
                }

                sendLoopingCount(loopCounter);
                loopPending->flush();
                curInput.setown(queryContainer().queryLoopGraph()->execute(*this, (flags & IHThorLoopArg::LFcounter)?loopCounter:0, loopPending.getClear(), loopPendingCount, extractBuilder.size(), extractBuilder.getbytes()));
                loopPending.setown(createOverflowableBuffer(this, LOOP_SMART_BUFFER_SIZE));
                loopPendingCount = 0;
                ++loopCounter;
                if ((container.getKind() == TAKloopcount) && (loopCounter > maxIterations))
                {
                    sendEndLooping();
                    finishedLooping = true;
                }
            }
        }
        return NULL;
    }
    CATCH_NEXTROW()
    {
        return nextRowFeeder->nextRow();
    }
    virtual void stop()
    {
        nextRowFeeder->stop();
    }
};

class CGraphLoopSlaveActivity : public CLoopSlaveActivityBase
{
    IHThorGraphLoopArg *helper;
    bool executed;
    unsigned flags;
    Owned<IThorGraphResults> loopResults;
    Owned<IRowStream> finalResultStream;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CGraphLoopSlaveActivity(CGraphElementBase *container) : CLoopSlaveActivityBase(container)
    {
        helper = (IHThorGraphLoopArg *)queryHelper();
        flags = helper->getFlags();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dostart();
        executed = false;
        maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        loopResults.setown(createThorGraphResults(maxIterations));
        helper->createParentExtract(extractBuilder);
        dataLinkStart("GRAPHLOOP", container.queryId());
    }
    CATCH_NEXTROW()
    {
        if (!executed)
        {
            executed = true;
            IThorResult *result = loopResults->createResult(*this, 0, this, true);
            Owned<IRowWriter> resultWriter = result->getWriter();
            loop
            {
                OwnedConstThorRow row = input->nextRow();
                if (!row)
                {
                    row.setown(input->nextRow());
                    if (!row)
                        break;
                    resultWriter->putRow(NULL);
                }
                resultWriter->putRow(row.getClear());
            }

            unsigned loopCounter=1;
            for (; loopCounter<=maxIterations; loopCounter++)
            {
                sendLoopingCount(loopCounter);
                queryContainer().queryLoopGraph()->execute(*this, (flags & IHThorGraphLoopArg::GLFcounter)?loopCounter:0, loopResults, extractBuilder.size(), extractBuilder.getbytes());
            }
            int iNumResults = loopResults->count();
            Owned<IThorResult> finalResult = loopResults->getResult(iNumResults-1); //Get the last result, which isnt necessarily 'maxIterations'
            finalResultStream.setown(finalResult->getRowStream());
        }
        OwnedConstThorRow next = finalResultStream->nextRow();
        if (next)
            dataLinkIncrement();
        return next.getClear();
    }
    virtual void stop()
    {
        finalResultStream.clear();
        loopResults.clear();
        CLoopSlaveActivityBase::doStop();
    }
};


activityslaves_decl CActivityBase *createLoopSlave(CGraphElementBase *container)
{
    return new CLoopSlaveActivity(container);
}


/////////////// local result read

class CLocalResultReadActivity : public CSlaveActivity, public CThorDataLink
{
    IThorDataLink *input;
    IHThorLocalResultReadArg *helper;
    Owned<IRowStream> resultStream;
    unsigned curRow;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalResultReadActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorLocalResultReadArg *)queryHelper();
        input = NULL;
        curRow = 0;
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        assertex(container.queryResultsGraph());
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        curRow = 0;
        abortSoon = false;
        assertex(container.queryResultsGraph());
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        Owned<IThorResult> result = graph->getResult(helper->querySequence());
        resultStream.setown(result->getRowStream());
        dataLinkStart("LOCALRESULTREAD", container.queryId());
    }
    virtual void stop()
    {
        abortSoon = true;
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!abortSoon)
        {
            OwnedConstThorRow row = resultStream->nextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createLocalResultReadSlave(CGraphElementBase *container)
{
    return new CLocalResultReadActivity(container);
}


void receiveResult(CActivityBase &activity, IRowInterfaces &inputRowIf, IRowWriter &result)
{
    const mptag_t &mpTag = activity.queryMpTag();
    ICommunicator &comm = activity.queryContainer().queryJob().queryJobComm();
    IOutputRowDeserializer *deserializer = inputRowIf.queryRowDeserializer();
    IEngineRowAllocator *allocator = inputRowIf.queryRowAllocator();
    // receive full result back
    CMessageBuffer msg;
    MemoryBuffer mb;
    Owned<ISerialStream> stream = createMemoryBufferSerialStream(mb);
    CThorStreamDeserializerSource rowSource(stream);
    loop
    {
        if (activity.queryAbortSoon())
            return;
        msg.clear();
        if (!activity.receiveMsg(msg, 0, mpTag))
            return;
        if (0 == msg.length())
            break;
        ThorExpand(msg, mb.clear());
        while (!rowSource.eos()) 
        {
            RtlDynamicRowBuilder rowBuilder(allocator);
            size32_t sz = deserializer->deserialize(rowBuilder, rowSource);
            result.putRow(rowBuilder.finalizeRowClear(sz));
        }
    }
}

/////////////// local spill write

class CLocalResultSpillActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorLocalResultSpillArg *helper;
    bool eoi, lastNull, global;
    Owned<IRowWriter> resultWriter;
    MemoryBuffer mb;

    void sendResultSoFar()
    {
        CMessageBuffer msg;
        ThorCompress(mb.toByteArray(), mb.length(), msg);
        container.queryJob().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT);
        mb.clear();
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalResultSpillActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorLocalResultSpillArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        mpTag = container.queryJob().deserializeMPTag(data);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        lastNull = eoi = false;
        abortSoon = false;
        assertex(container.queryResultsGraph());
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        bool local = container.queryOwner().isLocalOnly();
        IThorResult *result = graph->queryResult(helper->querySequence());
        if (result)
            local = result->isLocal();
        result = graph->createResult(*this, helper->querySequence(), this, local);  // NB graph owns result
        resultWriter.setown(result->getWriter());
        global = !result->isLocal();
        startInput(inputs.item(0));
        dataLinkStart("LOCALRESULTSPILL", container.queryId());
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!abortSoon && !eoi)
            return NULL;
        OwnedConstThorRow row = inputs.item(0)->nextRow();
        if (!row)
        {
            if (lastNull)
                eoi = true;
            else
                lastNull = true;
            return NULL;
        }
        if (lastNull)
        {
            if (!global)
                resultWriter->putRow(NULL);
            lastNull = false;
        }
        if (global)
        {
            CMemoryRowSerializer mbs(mb);
            queryRowSerializer()->serialize(mbs, (const byte *)row.get());
            if (mb.length() > 0x80000)
                sendResultSoFar();
        }
        else
            resultWriter->putRow(row.getClear());
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void stop()
    {
        if (global)
        {
            if (mb.length())
                sendResultSoFar();
            CMessageBuffer msg;
            container.queryJob().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT);
            receiveResult(*this, *this, *resultWriter);
        }
        stopInput(inputs.item(0));
        abortSoon = true;
        dataLinkStop();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (global)
            cancelReceiveMsg(0, mpTag);
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

/////////////// local result write

class CLocalResultWriteActivityBase : public ProcessSlaveActivity
{
protected:
    IThorDataLink *input;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalResultWriteActivityBase(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        input = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().deserializeMPTag(data);
    }
    virtual IThorResult *createResult() = 0;
    virtual void process()
    {
        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;

        IThorResult *result = createResult();

        Owned<IRowWriter> resultWriter = result->getWriter();
        if (!result->isLocal())
        {
            CMessageBuffer msg;
            MemoryBuffer mb;
            CMemoryRowSerializer mbs(mb);
            IRowInterfaces *inputRowIf = input->queryFromActivity();
            loop
            {
                loop
                {
                    OwnedConstThorRow row = input->nextRow();
                    if (!row)
                    {
                        row.setown(input->nextRow());
                        if (!row)
                            break;
                    }
                    inputRowIf->queryRowSerializer()->serialize(mbs, (const byte *)row.get());
                    if (mb.length() > 0x80000)
                        break;
                }
                msg.clear();
                if (mb.length())
                {
                    ThorCompress(mb.toByteArray(), mb.length(), msg);
                    mb.clear();
                }
                container.queryJob().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT);
                if (0 == msg.length())
                    break;
            }
            receiveResult(*this, *inputRowIf, *resultWriter);
        }
        else
        {
            loop
            {
                OwnedConstThorRow nextrec = input->nextRow();
                if (!nextrec)
                {
                    nextrec.setown(input->nextRow());
                    if (!nextrec)
                        break;
                    resultWriter->putRow(NULL);
                }
                resultWriter->putRow(nextrec.getClear());
            }
        }
    }
    virtual void abort()
    {
        ProcessSlaveActivity::abort();
        cancelReceiveMsg(0, mpTag);
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }
};

class CLocalResultWriteActivity : public CLocalResultWriteActivityBase
{
    IHThorLocalResultWriteArg *helper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalResultWriteActivity(CGraphElementBase *container) : CLocalResultWriteActivityBase(container)
    {
    }
    virtual IThorResult *createResult()
    {
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        bool local = container.queryOwner().isLocalOnly();
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        IThorResult *result = graph->queryResult(helper->querySequence());
        if (result)
            local = result->isLocal();
        return graph->createResult(*this, helper->querySequence(), this, local);
    }
};

activityslaves_decl CActivityBase *createLocalResultWriteSlave(CGraphElementBase *container)
{
    return new CLocalResultWriteActivity(container);
}

activityslaves_decl CActivityBase *createLocalResultSpillSlave(CGraphElementBase *container)
{
    return new CLocalResultSpillActivity(container);
}



activityslaves_decl CActivityBase *createGraphLoopSlave(CGraphElementBase *container)
{
    return new CGraphLoopSlaveActivity(container);
}

/////////////

class CIfActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorIfArg *helper;
    IThorDataLink *selectedInput;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CIfActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorIfArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        selectedInput = NULL;
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        selectedInput = container.whichBranch>=inputs.ordinality() ? NULL : inputs.item(container.whichBranch);
        if (selectedInput)
            startInput(selectedInput);
        dataLinkStart("CHILDIF", container.queryId());
    }
    virtual void stop()
    {
        if (selectedInput)
            stopInput(selectedInput);
        abortSoon = true;
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon)
            return NULL;
        if (!selectedInput)
            return NULL;

        OwnedConstThorRow ret = selectedInput->nextRow();
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    virtual bool isGrouped() { return selectedInput?selectedInput->isGrouped():false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createIfSlave(CGraphElementBase *container)
{
    return new CIfActivity(container);
}


//////////// NewChild acts - move somewhere else..

class CChildNormalizeSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorChildNormalizeArg *helper;
    Owned<IEngineRowAllocator> allocator;
    bool eos, ok, started;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CChildNormalizeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorChildNormalizeArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        allocator.set(queryRowAllocator());
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        started = false;
        eos = false;
        ok = false;
        dataLinkStart("CHILDNORMALIZE", container.queryId());
    }
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        if (eos)
            return NULL;

        bool ok;
        if (!started)
        {
            ok = helper->first();
            started = true;
        }
        else
            ok = helper->next();

        if (ok)
        {
            RtlDynamicRowBuilder ret(allocator);    
            do {
                unsigned thisSize = helper->transform(ret);
                if (thisSize)
                {
                    dataLinkIncrement();
                    return ret.finalizeRowClear(thisSize);
                }
                ok = helper->next();
            }
            while (ok);
        }

        eos = true;
        return NULL;
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildNormalizeSlave(CGraphElementBase *container)
{
    return new CChildNormalizeSlaveActivity(container);
}


//=====================================================================================================

class CChildAggregateSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorChildAggregateArg *helper;
    bool eos;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CChildAggregateSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorChildAggregateArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eos = false;
        dataLinkStart("CHILDAGGREGATE", container.queryId());
    }
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        if (eos)
            return NULL;
        eos = true;
        RtlDynamicRowBuilder ret(queryRowAllocator());  
        size32_t sz = helper->clearAggregate(ret);
        helper->processRows(ret);
        dataLinkIncrement();
        return ret.finalizeRowClear(sz);
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildAggregateSlave(CGraphElementBase *container)
{
    return new CChildAggregateSlaveActivity(container);
}

//=====================================================================================================

class CChildGroupAggregateActivitySlave : public CSlaveActivity, public CThorDataLink, implements IHThorGroupAggregateCallback
{
    IHThorChildGroupAggregateArg *helper;
    bool eos, gathered;
    Owned<IEngineRowAllocator> allocator;
    Owned<CThorRowAggregator> aggregated;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CChildGroupAggregateActivitySlave(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorChildGroupAggregateArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        allocator.set(queryRowAllocator());
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        gathered = eos = false;
        aggregated.clear();
        aggregated.setown(new CThorRowAggregator(*this, *helper, *helper, queryLargeMemSize()/10, 0==container.queryOwnerId()));
        aggregated->start(queryRowAllocator());
        dataLinkStart("CHILDGROUPAGGREGATE", container.queryId());
    }
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        if (eos)
            return NULL;
        if (!gathered)
        {
            helper->processRows(this);
            gathered = true;
        }
        Owned<AggregateRowBuilder> next = aggregated->nextResult();
        if (next)
        {
            dataLinkIncrement();
            return next->finalizeRowClear();
        }
        eos = true;
        return NULL;
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
//IHThorGroupAggregateCallback
    virtual void processRow(const void *src)
    {
        aggregated->addRow(src);
    }
};

activityslaves_decl CActivityBase *createChildGroupAggregateSlave(CGraphElementBase *container)
{
    return new CChildGroupAggregateActivitySlave(container);
}


//=====================================================================================================

class CChildThroughNormalizeSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorChildThroughNormalizeArg *helper;
    Owned<IEngineRowAllocator> allocator;
    OwnedConstThorRow lastInput;
    RtlDynamicRowBuilder nextOutput;
    size32_t recordsize;
    rowcount_t numProcessedLastGroup;
    bool ok;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CChildThroughNormalizeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this), nextOutput(NULL)
    {
        helper = (IHThorChildThroughNormalizeArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        allocator.set(queryRowAllocator());
        nextOutput.setAllocator(allocator);
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ok = false;
        numProcessedLastGroup = getDataLinkCount(); // is this right?
        lastInput.clear();
        nextOutput.clear();
        startInput(inputs.item(0));
        dataLinkStart("CHILDTHROUGHNORMALIZE", container.queryId());
    }
    virtual void stop()
    {
        stopInput(inputs.item(0));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        IThorDataLink *input = inputs.item(0);
        loop
        {
            if (ok)
                ok = helper->next();

            while (!ok)
            {
                lastInput.setown(input->nextRow());
                if (!lastInput)
                {
                    if (numProcessedLastGroup != getDataLinkCount()) // is this right?
                    {
                        numProcessedLastGroup = getDataLinkCount(); // is this right?
                        return NULL;
                    }
                    lastInput.setown(input->nextRow());
                    if (!lastInput)
                        return NULL;
                }

                ok = helper->first(lastInput);
            }
            
            nextOutput.ensureRow();
            do 
            {
                size32_t thisSize = helper->transform(nextOutput);
                if (thisSize)
                {
                    dataLinkIncrement();
                    return nextOutput.finalizeRowClear(thisSize);
                }
                ok = helper->next();
            } while (ok);
        }
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildThroughNormalizeSlave(CGraphElementBase *container)
{
    return new CChildThroughNormalizeSlaveActivity(container);
}

///////////

class CGraphLoopResultReadSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorGraphLoopResultReadArg *helper;
    Owned<IRowStream> resultStream;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CGraphLoopResultReadSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), CThorDataLink(this)
    {
        helper = (IHThorGraphLoopResultReadArg *)queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        abortSoon = false;
        unsigned sequence = helper->querySequence();
        if ((int)sequence >= 0)
        {
            assertex(container.queryResultsGraph());
            Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
            Owned<IThorResult> result = graph->getGraphLoopResult(sequence);
            resultStream.setown(result->getRowStream());
        }
        else
            abortSoon = true;
        dataLinkStart("GRAPHLOOPRESULTREAD", container.queryId());
    }
    virtual bool isGrouped() { return false; }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!abortSoon)
        {
            OwnedConstThorRow row = resultStream->nextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    virtual void stop()
    {
        abortSoon = true;
        dataLinkStop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

////////////////

#if 0
class CNWayGraphLoopResultReadSlaveActivity : public CSlaveActivity, implements IHThorNWayInput
{
    IHThorNWayGraphLoopResultReadArg & helper;
    CIArrayOf<CHThorActivityBase> inputs;
    __int64 graphId;
    bool grouped;

public:
    CNWayGraphLoopResultReadSlaveActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayGraphLoopResultReadArg &_arg, ThorActivityKind _kind, __int64 _graphId) : CHThorSimpleActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
    {
        grouped = helper.isGrouped();
        graphId = _graphId;
    }

    virtual bool isGrouped() 
    { 
        return grouped; 
    }

    virtual void ready()
    {
        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper.getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        if (selectionIsAll)
            throw MakeStringException(100, "ALL not yet supported for NWay graph inputs");

        unsigned max = selectionLen / sizeof(size32_t);
        const size32_t * selections = (const size32_t *)selection.getdata();
        for (unsigned i = 0; i < max; i++)
        {
            CHThorActivityBase * resultInput = new CHThorGraphLoopResultReadActivity(agent, activityId, subgraphId, helper, kind, graphId, selections[i], grouped);
            inputs.append(*resultInput);
            resultInput->ready();
        }
    }

    virtual void done()
    {
        inputs.kill();
    }

    virtual void setInput(unsigned idx, IHThorInput *_in)
    {
        throwUnexpected();
    }

    virtual const void * nextInGroup()
    {
        throwUnexpected();
    }

    virtual unsigned numConcreteOutputs() const
    {
        return inputs.ordinality();
    }

    virtual IHThorInput * queryConcreteInput(unsigned idx) const
    {
        if (inputs.isItem(idx))
            return &inputs.item(idx);
        return NULL;
    }
};

#endif


////////////////
activityslaves_decl CActivityBase *createGraphLoopResultReadSlave(CGraphElementBase *container)
{
    return new CGraphLoopResultReadSlaveActivity(container);
}

class CGraphLoopResultWriteSlaveActivity : public CLocalResultWriteActivityBase
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CGraphLoopResultWriteSlaveActivity(CGraphElementBase *container) : CLocalResultWriteActivityBase(container)
    {
    }
    virtual IThorResult *createResult()
    {
        IHThorGraphLoopResultWriteArg *helper = (IHThorGraphLoopResultWriteArg *)queryHelper();
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        bool local = container.queryOwner().isLocalOnly();
        return graph->createGraphLoopResult(*this, input->queryFromActivity(), local);
    }
};

activityslaves_decl CActivityBase *createGraphLoopResultWriteSlave(CGraphElementBase *container)
{
    return new CGraphLoopResultWriteSlaveActivity(container);
}

