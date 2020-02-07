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
#include "thmfilemanager.hpp"
#include "thbufdef.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "eclrtl_imp.hpp"
#include "thcompressutil.hpp"

class CLoopSlaveActivityBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

protected:
    bool syncIterations = false;
    bool loopIsInGlobalGraph = false;
    bool sentEndLooping = true;
    unsigned maxIterations = 0;
    unsigned loopCounter = 0;
    rtlRowBuilder extractBuilder;
    bool lastMaxEmpty = false;
    unsigned maxEmptyLoopIterations = 1000;
    mptag_t syncMpTag = TAG_NULL;

    bool sendLoopingCount(unsigned n, unsigned emptyIterations)
    {
        if (loopIsInGlobalGraph)
        {
            if (syncIterations || (lastMaxEmpty && (0 == emptyIterations)) || (!lastMaxEmpty && (emptyIterations>maxEmptyLoopIterations)) || ((0 == n) && (0 == emptyIterations)))
            {
                CMessageBuffer msg; // inform master starting
                msg.append(n);
                msg.append(emptyIterations);
                queryJobChannel().queryJobComm().send(msg, 0, syncMpTag);
                if (!syncIterations)
                {
                    lastMaxEmpty = emptyIterations>maxEmptyLoopIterations;
                    return true;
                }
                receiveMsg(msg, 0, syncMpTag);
                bool ok;
                msg.read(ok);
                return ok;
            }
        }
        return true;
    }
    void sendStartLooping()
    {
        sendLoopingCount(0, 0);
    }
    void sendEndLooping()
    {
        if (sentEndLooping)
            return;
        sentEndLooping = true;
        sendLoopingCount(0, 0);
    }
public:
    CLoopSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        maxEmptyLoopIterations = getOptUInt(THOROPT_LOOP_MAX_EMPTY, 1000);
        loopIsInGlobalGraph = container.queryOwner().isGlobal();
        appendOutputLinked(this);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        syncIterations = !queryContainer().queryLoopGraph()->queryGraph()->isLocalOnly();
        if (loopIsInGlobalGraph)
            syncMpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (loopIsInGlobalGraph)
            cancelReceiveMsg(0, syncMpTag);
    }
    virtual void start() override
    {
        PARENT::start();
        extractBuilder.clear();
        sentEndLooping = false;
        lastMaxEmpty = false;
        loopCounter = 1;
    }
    void doStop()
    {
        sendEndLooping();
        stopInput(0);
    }
// IThorDataLink
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
    void processDone(MemoryBuffer &mb)
    {
        CSlaveActivity::processDone(mb);
        ((CSlaveGraph *)queryContainer().queryLoopGraph()->queryGraph())->serializeDone(mb);
    }
    virtual void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        mb.append(loopCounter);
    }
};

class CLoopSlaveActivity : public CLoopSlaveActivityBase
{
    typedef CLoopSlaveActivityBase PARENT;

    Owned<IRowStream> curInput;
    Owned<IRowWriterMultiReader> loopPending;
    rowcount_t loopPendingCount;
    unsigned flags, lastMs;
    IHThorLoopArg *helper;
    bool eof, finishedLooping;
    Owned<IBarrier> barrier;

    class CNextRowFeeder : implements IRowStream, implements IThreaded, public CSimpleInterface
    {
        CThreaded threaded;
        CLoopSlaveActivity *activity;
        Owned<ISmartRowBuffer> smartbuf;
        bool stopped, stopping;
        Owned<IException> exception;

        void stopThread()
        {
            if (!stopped)
            {
                stopped = true;
                threaded.join();
            }
        }
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CNextRowFeeder(CLoopSlaveActivity *_activity) : threaded("CNextRowFeeder"), activity(_activity)
        {
            stopped = true;
            stopping = false;
            smartbuf.setown(createSmartInMemoryBuffer(activity, activity, SMALL_SMART_BUFFER_SIZE));
            threaded.init(this);
        }
        ~CNextRowFeeder()
        {
            stopThread();
        }
        void abort()
        {
            if (smartbuf)
            {
                smartbuf->stop(); // just in case blocked
                stopThread();
            }
        }
        virtual void threadmain() override
        {
            stopped = false;
            Linked<IRowWriter> writer = smartbuf->queryWriter();
            try
            {
                while (!stopped)
                {
                    OwnedConstThorRow row = activity->getNextRow(stopping);
                    if (!row)
                    {
                        row.setown(activity->getNextRow(stopping));
                        if (!row)
                            break;
                        else if (!stopping)
                            writer->putRow(NULL); // eog
                    }
                    if (!stopping)
                        writer->putRow(row.getClear());
                }
                activity->doStop();
            }
            catch (IException *e)
            {
                ::ActPrintLog(activity, e);
                exception.setown(e);
            }
            try { writer->flush(); }
            catch (IException *e)
            {
                ::ActPrintLog(activity, e, "Exception in writer->flush");
                if (!exception.get())
                    exception.setown(e);
            }
        }
        virtual const void *nextRow() override
        {
            OwnedConstThorRow row = smartbuf->nextRow();
            if (exception)
                throw exception.getClear();
            return row.getClear();
        }
        virtual void stop() override
        {
            /* NB: signals wants to stop and discards further rows coming out of loop,
             * but reader thread keeps looping, until finishedLooping=true.
             */
            stopping = true;
            smartbuf->stop(); // just in case blocked
            threaded.join();
            stopped = true;
            smartbuf.clear();
            if (exception)
                throw exception.getClear();
        }
    };
    Owned<CNextRowFeeder> nextRowFeeder;

public:
    CLoopSlaveActivity(CGraphElementBase *container) : CLoopSlaveActivityBase(container)
    {
        helper = (IHThorLoopArg *) queryHelper();
        flags = helper->getFlags();
        if (!loopIsInGlobalGraph || (0 == (flags & IHThorLoopArg::LFnewloopagain)))
            setRequireInitData(false);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CLoopSlaveActivityBase::init(data, slaveData);
        if (flags & IHThorLoopArg::LFnewloopagain)
        {
            if (loopIsInGlobalGraph)
            {
                mpTag = container.queryJobChannel().deserializeMPTag(data);
                barrier.setown(container.queryJobChannel().createBarrier(mpTag));
                syncIterations = true;
            }
        }
    }
    virtual void kill()
    {
        CLoopSlaveActivityBase::kill();
        loopPending.clear();
        curInput.clear();
    }
    virtual void abort()
    {
        CLoopSlaveActivityBase::abort();
        if (nextRowFeeder)
            nextRowFeeder->abort();
        if (barrier)
            barrier->cancel();
    }
// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = false;
        helper->createParentExtract(extractBuilder);
        maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        loopPending.setown(createOverflowableBuffer(*this, this, ers_forbidden, true));
        loopPendingCount = 0;
        finishedLooping = ((container.getKind() == TAKloopcount) && (maxIterations == 0));
        if ((flags & IHThorLoopArg::LFnewloopagain) && !helper->loopFirstTime())
            finishedLooping = true;
        curInput.set(inputStream);
        lastMs = msTick();

        ActPrintLog("maxIterations = %d", maxIterations);
        nextRowFeeder.setown(new CNextRowFeeder(this));
    }
    void doStop()
    {
        loopPending.clear();
        CLoopSlaveActivityBase::doStop();
    }
    const void *getNextRow(bool stopping)
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon && !eof)
        {
            unsigned emptyIterations = 0;
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow ret = (void *)curInput->nextRow();
                    if (!ret)
                    {
                        ret.setown(curInput->nextRow()); // more cope with groups somehow....
                        if (!ret)
                            break;
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
                if (abortSoon)
                    break;

                if (stopping)
                    finishedLooping = true;
                else
                {
                    switch (container.getKind())
                    {
                    case TAKloopdataset:
                        assertex(flags & IHThorLoopArg::LFnewloopagain);
                        // NB: finishedLooping set at end of loop, based on loopAgain result
                        break;
                    case TAKlooprow:
                        if (0 == loopPendingCount)
                            finishedLooping = true; // This slave has finished
                        break;
                    case TAKloopcount:
                        // NB: finishedLooping set at end of loop, so that last getNextRow() iteration spits out final rows
                        break;
                    default:
                        throwUnexpected();
                    }
                }

                if (loopPendingCount)
                    emptyIterations = 0;
                else
                {
                    //note: any outputs which didn't go around the loop again, would return the record, reinitializing emptyIterations
                    emptyIterations++;
                    // only fire error here, if local
                    if (container.queryLocalOrGrouped() && emptyIterations > maxEmptyLoopIterations)
                        throw MakeActivityException(this, 0, "Executed LOOP with empty input and output %u times", emptyIterations);
                    unsigned now = msTick();
                    if (now-lastMs > 60000)
                    {
                        ActPrintLog("Executing LOOP with empty input and output %u times", emptyIterations);
                        lastMs = now;
                    }
                }

                IThorBoundLoopGraph *boundGraph = queryContainer().queryLoopGraph();
                unsigned condLoopCounter = (flags & IHThorLoopArg::LFcounter) ? loopCounter : 0;
                unsigned loopAgain = (flags & IHThorLoopArg::LFnewloopagain) ? helper->loopAgainResult() : 0;
                ownedResults.setown(queryGraph().createThorGraphResults(3));
                // ensures remote results are available, via owning activity (i.e. this loop act)
                // so that when aggregate result is fetched from the master, it will retrieve from the act, not the (already cleaned) graph localresults
                ownedResults->setOwner(container.queryId());

                boundGraph->prepareLoopResults(*this, ownedResults);
                if (condLoopCounter) // cannot be 0
                    boundGraph->prepareCounterResult(*this, ownedResults, condLoopCounter, 2);
                if (loopAgain) // cannot be 0
                    boundGraph->prepareLoopAgainResult(*this, ownedResults, loopAgain);

                loopPending->flush();
                Owned<IThorResult> inputResult = ownedResults->getResult(1);
                inputResult->setResultStream(loopPending.getClear(), loopPendingCount);

                // ensure results prepared before graph begins
                if (syncIterations)
                {
                    // 0 signals this slave has finished, but don't stop until all have
                    if (!sendLoopingCount(finishedLooping ? 0 : loopCounter, finishedLooping ? 0 : emptyIterations))
                    {
                        sentEndLooping = true; // prevent sendEndLooping() sending end again
                        eof = true;
                        return NULL;
                    }
                    finishedLooping = false; // reset because global and execute may feed rows back to this node
                }
                else if (finishedLooping)
                {
                    eof = true;
                    return NULL;
                }

                boundGraph->queryGraph()->executeChild(extractBuilder.size(), extractBuilder.getbytes(), ownedResults, NULL);

                Owned<IThorResult> result0 = ownedResults->getResult(0);
                curInput.setown(result0->getRowStream());

                if (flags & IHThorLoopArg::LFnewloopagain)
                {
                    //Need to wait for all other slaves to finish so that the loopAgain result is calculated.
                    if (barrier)
                    {
                        if (!barrier->wait(false))
                            return NULL; // aborted
                    }

                    Owned<IThorResult> loopAgainResult = ownedResults->getResult(helper->loopAgainResult(), !queryGraph().isLocalChild());
                    assertex(loopAgainResult);
                    Owned<IRowStream> loopAgainRows = loopAgainResult->getRowStream();
                    OwnedConstThorRow row = loopAgainRows->nextRow();
                    assertex(row);
                    //Result is a row which contains a single boolean field.
                    if (!((const bool *)row.get())[0])
                        finishedLooping = true; // NB: will finish when loopPending has been consumed
                    if (barrier) // barrier passed once loopAgain result used
                    {
                        if (!barrier->wait(false))
                            return NULL; // aborted
                    }
                }
                loopPending.setown(createOverflowableBuffer(*this, this, ers_forbidden, true));
                loopPendingCount = 0;
                ++loopCounter;
                if ((container.getKind() == TAKloopcount) && (loopCounter > maxIterations))
                    finishedLooping = true; // NB: will finish when loopPending has been consumed
            }
        }
        return NULL;
    }
    CATCH_NEXTROW()
    {
        return nextRowFeeder->nextRow();
    }
    virtual void stop() override
    {
        if (nextRowFeeder)
        {
            nextRowFeeder->stop(); // NB: This will block if this slave's loop hasn't hit eof, it will continue looping until 'finishedLooping'
            nextRowFeeder.clear();
        }
        PARENT::stop();
    }
};

class CGraphLoopSlaveActivity : public CLoopSlaveActivityBase
{
    typedef CLoopSlaveActivityBase PARENT;

    IHThorGraphLoopArg *helper;
    bool executed;
    unsigned flags;
    Owned<IThorGraphResults> loopResults;
    Owned<IRowStream> finalResultStream;

public:
    CGraphLoopSlaveActivity(CGraphElementBase *container) : CLoopSlaveActivityBase(container)
    {
        helper = (IHThorGraphLoopArg *)queryHelper();
        flags = helper->getFlags();
        if (!loopIsInGlobalGraph)
            setRequireInitData(false);
    }
    virtual void kill()
    {
        CLoopSlaveActivityBase::kill();
        finalResultStream.clear();
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        executed = false;
        maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        loopResults.setown(queryGraph().createThorGraphResults(0));
        helper->createParentExtract(extractBuilder);
        ActPrintLog("maxIterations = %d", maxIterations);
    }
    CATCH_NEXTROW()
    {
        if (!executed)
        {
            executed = true;
            ThorGraphResultType resultType = thorgraphresult_nul;
            if (!queryGraph().isLocalChild())
                resultType = mergeResultTypes(resultType, thorgraphresult_distributed);
            if (input->isGrouped())
                resultType = mergeResultTypes(resultType, thorgraphresult_grouped);
            IThorResult *result = loopResults->createResult(*this, 0, this, resultType);
            Owned<IRowWriter> resultWriter = result->getWriter();
            for (;;)
            {
                OwnedConstThorRow row = inputStream->nextRow();
                if (!row)
                {
                    row.setown(inputStream->nextRow());
                    if (!row)
                        break;
                    resultWriter->putRow(NULL);
                }
                resultWriter->putRow(row.getClear());
            }

            IThorBoundLoopGraph *boundGraph = queryContainer().queryLoopGraph();
            for (; loopCounter<=maxIterations; loopCounter++)
            {
                unsigned condLoopCounter = (helper->getFlags() & IHThorGraphLoopArg::GLFcounter) ? loopCounter : 0;
                Owned<IThorGraphResults> results = boundGraph->queryGraph()->createThorGraphResults(1);
                if (condLoopCounter)
                    boundGraph->prepareCounterResult(*this, results, condLoopCounter, 0);
                sendLoopingCount(loopCounter, 0);
                size32_t parentExtractSz;
                const byte *parentExtract = queryGraph().queryParentExtract(parentExtractSz);
                boundGraph->queryGraph()->executeChild(parentExtractSz, parentExtract, results, loopResults);
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
        doStop();
        PARENT::stop();
    }
};


activityslaves_decl CActivityBase *createLoopSlave(CGraphElementBase *container)
{
    return new CLoopSlaveActivity(container);
}


/////////////// local result read

class CLocalResultReadActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorLocalResultReadArg *helper;
    Owned<IRowStream> resultStream;
    unsigned curRow;
    mptag_t replyTag;

public:
    CLocalResultReadActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorLocalResultReadArg *)queryHelper();
        curRow = 0;
        replyTag = queryMPServer().createReplyTag();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        assertex(container.queryResultsGraph());
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        curRow = 0;
        abortSoon = false;
        Owned<CGraphBase> graph;
        graph_id resultGraphId = container.queryXGMML().getPropInt("att[@name=\"_graphId\"]/@value");
        if (resultGraphId == container.queryResultsGraph()->queryGraphId())
            graph.set(container.queryResultsGraph());
        else
            graph.setown(queryJobChannel().getGraph(resultGraphId));
        Owned<IThorResult> result = graph->getResult(helper->querySequence(), queryGraph().isLocalChild());
        resultStream.setown(result->getRowStream());
    }
    virtual void stop()
    {
        abortSoon = true;
        resultStream.clear();
        PARENT::stop();
    }
    virtual void kill()
    {
        CSlaveActivity::kill();
        resultStream.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createLocalResultReadSlave(CGraphElementBase *container)
{
    return new CLocalResultReadActivity(container);
}


/////////////// local spill write

class CLocalResultSpillActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorLocalResultSpillArg *helper;
    bool eoi, lastNull;
    Owned<IRowWriter> resultWriter;
    MemoryBuffer mb;

    void sendResultSoFar()
    {
        CMessageBuffer msg;
        ThorCompress(mb.toByteArray(), mb.length(), msg);
        queryJobChannel().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT);
        mb.clear();
    }

public:
    CLocalResultSpillActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorLocalResultSpillArg *)queryHelper();
        appendOutputLinked(this);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        lastNull = eoi = false;
        abortSoon = false;
        assertex(container.queryResultsGraph());
        CGraphBase *graph = container.queryResultsGraph();
        ThorGraphResultType resultType = thorgraphresult_nul;
        if (!queryGraph().isLocalChild())
            resultType = mergeResultTypes(resultType, thorgraphresult_distributed);
        if (input->isGrouped())
            resultType = mergeResultTypes(resultType, thorgraphresult_grouped);
        IThorResult *result = graph->createResult(*this, helper->querySequence(), this, resultType);  // NB graph owns result
        resultWriter.setown(result->getWriter());
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon && !eoi)
            return NULL;
        OwnedConstThorRow row = inputStream->nextRow();
        if (!row)
        {
            if (lastNull)
                eoi = true;
            else
                lastNull = true;
            return NULL;
        }
        if (lastNull) // looks like I should only bother with this if grouped
        {
            resultWriter->putRow(NULL);
            lastNull = false;
        }
        resultWriter->putRow(row.getClear());
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void stop()
    {
        abortSoon = true;
        PARENT::stop();
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

/////////////// local result write

class CLocalResultWriteActivityBase : public ProcessSlaveActivity
{
public:
    CLocalResultWriteActivityBase(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
    }
    virtual IThorResult *createResult() = 0;
    virtual void process()
    {
        start();
        processed = THORDATALINK_STARTED;

        IThorResult *result = createResult();

        Owned<IRowWriter> resultWriter = result->getWriter();
        for (;;)
        {
            OwnedConstThorRow nextrec = inputStream->nextRow();
            if (!nextrec)
            {
                nextrec.setown(inputStream->nextRow());
                if (!nextrec)
                    break;
                resultWriter->putRow(NULL);
            }
            resultWriter->putRow(nextrec.getClear());
        }
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stop();
            processed |= THORDATALINK_STOPPED;
        }
    }
};

class CLocalResultWriteActivity : public CLocalResultWriteActivityBase
{
public:
    CLocalResultWriteActivity(CGraphElementBase *_container) : CLocalResultWriteActivityBase(_container)
    {
        setRequireInitData(false);
    }
    virtual IThorResult *createResult()
    {
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        CGraphBase *graph = container.queryResultsGraph();
        ThorGraphResultType resultType = thorgraphresult_nul;
        if (!queryGraph().isLocalChild())
            resultType = mergeResultTypes(resultType, thorgraphresult_distributed);
        if (input->isGrouped())
            resultType = mergeResultTypes(resultType, thorgraphresult_grouped);
        return graph->createResult(*this, helper->querySequence(), this, resultType);
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


class CDictionaryResultWriteActivity : public ProcessSlaveActivity
{
    IHThorDictionaryResultWriteArg *helper;
public:
    CDictionaryResultWriteActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        helper = (IHThorDictionaryResultWriteArg *)queryHelper();
        setRequireInitData(false);
    }
    virtual void process()
    {
        start();
        processed = THORDATALINK_STARTED;

        RtlLinkedDictionaryBuilder builder(queryRowAllocator(), helper->queryHashLookupInfo());
        for (;;)
        {
            const void *row = inputStream->nextRow();
            if (!row)
            {
                row = inputStream->nextRow();
                if (!row)
                    break;
            }
            builder.appendOwn(row);
        }
        CGraphBase *graph = container.queryResultsGraph();
        ThorGraphResultType resultType = thorgraphresult_nul;
        if (!queryGraph().isLocalChild())
            resultType = mergeResultTypes(resultType, thorgraphresult_distributed);
        resultType = mergeResultTypes(resultType, thorgraphresult_sparse);
        IThorResult *result = graph->createResult(*this, helper->querySequence(), this, resultType);
        Owned<IRowWriter> resultWriter = result->getWriter();
        size32_t dictSize = builder.getcount();
        const byte ** dictRows = builder.queryrows();
        for (size32_t row = 0; row < dictSize; row++)
        {
            const byte *thisRow = dictRows[row];
            if (thisRow)
                LinkThorRow(thisRow);
            resultWriter->putRow(thisRow);
        }
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stop();
            processed |= THORDATALINK_STOPPED;
        }
    }
};

CActivityBase *createDictionaryResultWriteSlave(CGraphElementBase *container)
{
    return new CDictionaryResultWriteActivity(container);
}


activityslaves_decl CActivityBase *createGraphLoopSlave(CGraphElementBase *container)
{
    return new CGraphLoopSlaveActivity(container);
}

/////////////

class CConditionalActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IThorDataLink *selectedItdl = nullptr;
    IEngineRowStream *selectedInputStream = nullptr;

    void stopUnselectedInputs()
    {
        ForEachItemIn(i, inputs)
        {
            if (i != branch)
                stopInput(i);
        }
    }
protected:
    unsigned branch = (unsigned)-1;

public:
    CConditionalActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        stopUnselectedInputs();
        if (queryInput(branch))
        {
            startInput(branch);
            CThorInput &selectedInput = inputs.item(branch);
            selectedItdl = selectedInput.itdl;
            selectedInputStream = selectedInput.queryStream();
        }
        dataLinkStart();
    }
    virtual void stop() override
    {
        if (!hasStarted())
            stopUnselectedInputs(); // i.e. all
        else if ((branch>0) && queryInput(branch)) // branch 0 stopped by PARENT::stop
            stopInput(branch);
        selectedInputStream = NULL;
        abortSoon = true;
        branch = (unsigned)-1;
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon)
            return nullptr;
        if (!selectedInputStream)
            return nullptr;
        OwnedConstThorRow ret = selectedInputStream->nextRow();
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    virtual bool isGrouped() const override { return container.queryGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        IThorDataLink *branchInput = queryInput(branch);
        if (branchInput)
            branchInput->getMetaInfo(info);
    }
};

class CIfConditionalActivity : public CConditionalActivity
{
    typedef CConditionalActivity PARENT;

    IHThorIfArg *helper;
public:
    CIfConditionalActivity(CGraphElementBase *_container) : CConditionalActivity(_container)
    {
        helper = (IHThorIfArg *)baseHelper.get();
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        branch = helper->getCondition() ? 0 : 1;
        PARENT::start();
    }
};

activityslaves_decl CActivityBase *createIfSlave(CGraphElementBase *container)
{
    return new CIfConditionalActivity(container);
}

class CCaseConditionalActivity : public CConditionalActivity
{
    typedef CConditionalActivity PARENT;

    IHThorCaseArg *helper;
public:
    CCaseConditionalActivity(CGraphElementBase *_container) : CConditionalActivity(_container)
    {
        helper = (IHThorCaseArg *)baseHelper.get();
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        branch = helper->getBranch();
        if (branch >= queryNumInputs())
            branch = queryNumInputs() - 1;
        PARENT::start();
    }
};

activityslaves_decl CActivityBase *createCaseSlave(CGraphElementBase *container)
{
    return new CCaseConditionalActivity(container);
}

class CIfActionActivity : public ProcessSlaveActivity
{
    typedef ProcessSlaveActivity PARENT;

    bool cond = false;
    IHThorIfArg *helper;

public:
    CIfActionActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container)
    {
        helper = (IHThorIfArg *)baseHelper.get();
        setRequireInitData(false);
    }
    // IThorSlaveProcess overloaded methods
    virtual void process() override
    {
        processed = THORDATALINK_STARTED;
        ActivityTimer t(slaveTimerStats, timeActivities);
        cond = helper->getCondition();
        if (cond)
        {
            if (inputs.item(1).itdl)
                stopInput(1); // Note: stopping unused branches early helps us avoid buffering splits too long.
            startInput(0);
        }
        else
        {
            stopInput(0); // Note: stopping unused branches early helps us avoid buffering splits too long.
            if (inputs.item(1).itdl)
                startInput(1);
        }
    }
    virtual void endProcess() override
    {
        if (processed & THORDATALINK_STARTED)
        {
            if (cond)
                stopInput(0);
            else
                stopInput(1);
            processed |= THORDATALINK_STOPPED;
        }
    }
};

activityslaves_decl CActivityBase *createIfActionSlave(CGraphElementBase *container)
{
    return new CIfActionActivity(container);
}



//////////// NewChild acts - move somewhere else..

class CChildNormalizeSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorChildNormalizeArg *helper;
    Owned<IEngineRowAllocator> allocator;
    bool eos, ok, started;

public:
    CChildNormalizeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorChildNormalizeArg *)queryHelper();
        allocator.set(queryRowAllocator());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        started = false;
        eos = false;
        ok = false;
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
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildNormalizeSlave(CGraphElementBase *container)
{
    return new CChildNormalizeSlaveActivity(container);
}


//=====================================================================================================

class CChildAggregateSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorChildAggregateArg *helper;
    bool eos;

public:
    CChildAggregateSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorChildAggregateArg *)queryHelper();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eos = false;
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
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildAggregateSlave(CGraphElementBase *container)
{
    return new CChildAggregateSlaveActivity(container);
}

//=====================================================================================================

class CChildGroupAggregateActivitySlave : public CSlaveActivity, implements IHThorGroupAggregateCallback
{
    typedef CSlaveActivity PARENT;

    IHThorChildGroupAggregateArg *helper;
    bool eos, gathered;
    Owned<IEngineRowAllocator> allocator;
    Owned<RowAggregator> aggregated;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CChildGroupAggregateActivitySlave(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorChildGroupAggregateArg *)queryHelper();
        setRequireInitData(false);
        allocator.set(queryRowAllocator());
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        gathered = eos = false;
        aggregated.clear();
        aggregated.setown(new RowAggregator(*helper, *helper));
        aggregated->start(queryRowAllocator(), queryCodeContext(), queryId());
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
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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

class CChildThroughNormalizeSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorChildThroughNormalizeArg *helper;
    Owned<IEngineRowAllocator> allocator;
    OwnedConstThorRow lastInput;
    RtlDynamicRowBuilder nextOutput;
    rowcount_t numProcessedLastGroup;
    bool ok;

public:
    CChildThroughNormalizeSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), nextOutput(NULL)
    {
        helper = (IHThorChildThroughNormalizeArg *)queryHelper();
        allocator.set(queryRowAllocator());
        nextOutput.setAllocator(allocator);
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        ok = false;
        numProcessedLastGroup = getDataLinkCount(); // is this right?
        lastInput.clear();
        nextOutput.clear();
    }
    CATCH_NEXTROW()
    {
        for (;;)
        {
            if (ok)
                ok = helper->next();

            while (!ok)
            {
                lastInput.setown(inputStream->nextRow());
                if (!lastInput)
                {
                    if (numProcessedLastGroup != getDataLinkCount()) // is this right?
                    {
                        numProcessedLastGroup = getDataLinkCount(); // is this right?
                        return NULL;
                    }
                    lastInput.setown(inputStream->nextRow());
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
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

activityslaves_decl CActivityBase *createChildThroughNormalizeSlave(CGraphElementBase *container)
{
    return new CChildThroughNormalizeSlaveActivity(container);
}

///////////

class CGraphLoopResultReadSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorGraphLoopResultReadArg *helper;
    Owned<IRowStream> resultStream;

public:
    CGraphLoopResultReadSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = (IHThorGraphLoopResultReadArg *)queryHelper();
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void kill()
    {
        CSlaveActivity::kill();
        resultStream.clear();
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        abortSoon = false;
        unsigned sequence = helper->querySequence();
        if ((int)sequence >= 0)
        {
            assertex(container.queryResultsGraph());
            Owned<CGraphBase> graph;
            graph_id resultGraphId = container.queryXGMML().getPropInt("att[@name=\"_graphId\"]/@value");
            if (resultGraphId == container.queryResultsGraph()->queryGraphId())
                graph.set(container.queryResultsGraph());
            else
                graph.setown(queryJobChannel().getGraph(resultGraphId));
            Owned<IThorResult> result = graph->getGraphLoopResult(sequence, queryGraph().isLocalChild());
            resultStream.setown(result->getRowStream());
        }
        else
            abortSoon = true;
        dataLinkStart();
    }
    virtual bool isGrouped() const override { return false; }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
        resultStream.clear();
        PARENT::stop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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

    virtual const void * nextRow()
    {
        throwUnexpected();
    }

    virtual unsigned numConcreteOutputs() const
    {
        return inputs.ordinality();
    }

    virtual IHThorInput * queryConcreteOutput(unsigned idx) const
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
    CGraphLoopResultWriteSlaveActivity(CGraphElementBase *_container) : CLocalResultWriteActivityBase(_container)
    {
        setRequireInitData(false);
    }
    virtual IThorResult *createResult()
    {
        CGraphBase *graph = container.queryResultsGraph();
        ThorGraphResultType resultType = thorgraphresult_nul;
        if (!queryGraph().isLocalChild())
            resultType = mergeResultTypes(resultType, thorgraphresult_distributed);
        if (input->isGrouped())
            resultType = mergeResultTypes(resultType, thorgraphresult_grouped);
        return graph->createGraphLoopResult(*this, input->queryFromActivity(), resultType);
    }
};

activityslaves_decl CActivityBase *createGraphLoopResultWriteSlave(CGraphElementBase *container)
{
    return new CGraphLoopResultWriteSlaveActivity(container);
}

