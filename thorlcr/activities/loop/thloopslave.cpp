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
        smartbuf.setown(createSmartInMemoryBuffer(activity, activity, SMALL_SMART_BUFFER_SIZE));
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
        Linked<IRowWriter> writer = smartbuf->queryWriter();
        try
        {
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
            in->stop();
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, NULL);
            exception.setown(e);
        }
        try { writer->flush(); }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "Exception in writer->flush");
            if (!exception.get())
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

class CLoopSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    IThorDataLink *input;
    bool global;
    bool sentEndLooping;
    unsigned maxIterations;
    unsigned loopCounter;
    rtlRowBuilder extractBuilder;
    bool lastMaxEmpty;
    unsigned maxEmptyLoopIterations;

    bool sendLoopingCount(unsigned n, unsigned emptyIterations)
    {
        if (!container.queryLocalOrGrouped())
        {
            if (global || (lastMaxEmpty && (0 == emptyIterations)) || (!lastMaxEmpty && (emptyIterations>maxEmptyLoopIterations)) || ((0 == n) && (0 == emptyIterations)))
            {
                CMessageBuffer msg; // inform master starting
                msg.append(n);
                msg.append(emptyIterations);
                container.queryJob().queryJobComm().send(msg, 0, mpTag);
                if (!global)
                {
                    lastMaxEmpty = emptyIterations>maxEmptyLoopIterations;
                    return true;
                }
                receiveMsg(msg, 0, mpTag);
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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLoopSlaveActivityBase(CGraphElementBase *container) : CSlaveActivity(container), CThorDataLink(this)
    {
        input = NULL;
        mpTag = TAG_NULL;
        maxEmptyLoopIterations = (unsigned)container->queryJob().getWorkUnitValueInt("@maxEmptyLoopIterations", 1000);
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
        lastMaxEmpty = false;
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
    virtual void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        mb.append(loopCounter);
    }
};

class CLoopSlaveActivity : public CLoopSlaveActivityBase
{
    Owned<IRowStream> curInput;
    Owned<CNextRowFeeder> nextRowFeeder;
    Owned<IRowWriterMultiReader> loopPending;
    rowcount_t loopPendingCount;
    unsigned flags, lastMs;
    IHThorLoopArg *helper;
    bool eof, finishedLooping;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLoopSlaveActivity(CGraphElementBase *container) : CLoopSlaveActivityBase(container)
    {
        helper = (IHThorLoopArg *) queryHelper();
        flags = helper->getFlags();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CLoopSlaveActivityBase::init(data, slaveData);
        if (!global && (flags & IHThorLoopArg::LFnewloopagain))
        {
            if (container.queryOwner().isGlobal())
                global = true;
        }
    }
    virtual void kill()
    {
        CLoopSlaveActivityBase::kill();
        loopPending.clear();
        curInput.clear();
    }
// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dostart();
        eof = false;
        helper->createParentExtract(extractBuilder);
        maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        loopPending.setown(createOverflowableBuffer(*this, this, false, true));
        loopPendingCount = 0;
        finishedLooping = ((container.getKind() == TAKloopcount) && (maxIterations == 0));
        curInput.set(input);
        lastMs = msTick();

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
        ActPrintLog("maxIterations = %d", maxIterations);
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
            while (!abortSoon)
            {
                while (!abortSoon)
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
                if (abortSoon)
                    break;

                switch (container.getKind())
                {
                case TAKloopdataset:
                    assertex(flags & IHThorLoopArg::LFnewloopagain);
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

                if (!sendLoopingCount(loopCounter, emptyIterations)) // only if global
                    return NULL;
                loopPending->flush();

                IThorBoundLoopGraph *boundGraph = queryContainer().queryLoopGraph();
                unsigned doLoopCounter = (flags & IHThorLoopArg::LFcounter) ? loopCounter:0;
                unsigned doLoopAgain = (flags & IHThorLoopArg::LFnewloopagain) ? helper->loopAgainResult() : 0;
                ownedResults.setown(queryGraph().createThorGraphResults(3));
                // ensures remote results are available, via owning activity (i.e. this loop act)
                // so that when aggreagate result is fetched from the master, it will retreive from the act, not the (already cleaned) graph localresults
                ownedResults->setOwner(container.queryId());

                boundGraph->prepareLoopResults(*this, ownedResults);
                if (doLoopCounter) // cannot be 0
                    boundGraph->prepareCounterResult(*this, ownedResults, loopCounter, 2);
                if (doLoopAgain) // cannot be 0
                    boundGraph->prepareLoopAgainResult(*this, ownedResults, helper->loopAgainResult());

                boundGraph->execute(*this, doLoopCounter, ownedResults, loopPending.getClear(), loopPendingCount, extractBuilder.size(), extractBuilder.getbytes());

                Owned<IThorResult> result0 = ownedResults->getResult(0);
                curInput.setown(result0->getRowStream());

                if (flags & IHThorLoopArg::LFnewloopagain)
                {
                    Owned<IThorResult> loopAgainResult = ownedResults->getResult(helper->loopAgainResult(), !queryGraph().isLocalChild());
                    assertex(loopAgainResult);
                    Owned<IRowStream> loopAgainRows = loopAgainResult->getRowStream();
                    OwnedConstThorRow row = loopAgainRows->nextRow();
                    assertex(row);
                    //Result is a row which contains a single boolean field.
                    if (!((const bool *)row.get())[0])
                        finishedLooping = true;
                }
                loopPending.setown(createOverflowableBuffer(*this, this, false, true));
                loopPendingCount = 0;
                ++loopCounter;
                if ((container.getKind() == TAKloopcount) && (loopCounter > maxIterations))
                    finishedLooping = true;
                if (finishedLooping)
                    sendEndLooping();
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
    virtual void kill()
    {
        CLoopSlaveActivityBase::kill();
        finalResultStream.clear();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dostart();
        executed = false;
        maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        loopResults.setown(queryGraph().createThorGraphResults(0));
        helper->createParentExtract(extractBuilder);
        ActPrintLog("maxIterations = %d", maxIterations);
        dataLinkStart("GRAPHLOOP", container.queryId());
    }
    CATCH_NEXTROW()
    {
        if (!executed)
        {
            executed = true;
            IThorResult *result = loopResults->createResult(*this, 0, this, !queryGraph().isLocalChild());
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

            for (; loopCounter<=maxIterations; loopCounter++)
            {
                sendLoopingCount(loopCounter, 0);
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
    mptag_t replyTag;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalResultReadActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorLocalResultReadArg *)queryHelper();
        input = NULL;
        curRow = 0;
        replyTag = createReplyTag();
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
        graph_id resultGraphId = container.queryXGMML().getPropInt("att[@name=\"_graphId\"]/@value");
        if (!resultGraphId)
            resultGraphId = container.queryResultsGraph()->queryGraphId();
        Owned<CGraphBase> graph = queryJob().getGraph(resultGraphId);
        Owned<IThorResult> result = graph->getResult(helper->querySequence(), queryGraph().isLocalChild());
        resultStream.setown(result->getRowStream());
        dataLinkStart("LOCALRESULTREAD", container.queryId());
    }
    virtual void stop()
    {
        abortSoon = true;
        resultStream.clear();
        dataLinkStop();
    }
    virtual void kill()
    {
        CSlaveActivity::kill();
        resultStream.clear();
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


/////////////// local spill write

class CLocalResultSpillActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorLocalResultSpillArg *helper;
    bool eoi, lastNull;
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
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        IThorResult *result = graph->createResult(*this, helper->querySequence(), this, !queryGraph().isLocalChild());  // NB graph owns result
        resultWriter.setown(result->getWriter());
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
        stopInput(inputs.item(0));
        abortSoon = true;
        dataLinkStop();
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
    virtual IThorResult *createResult() = 0;
    virtual void process()
    {
        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;

        IThorResult *result = createResult();

        Owned<IRowWriter> resultWriter = result->getWriter();
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
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        return graph->createResult(*this, helper->querySequence(), this, !queryGraph().isLocalChild());
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
        aggregated.setown(new CThorRowAggregator(*this, *helper, *helper));
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
    virtual void kill()
    {
        CSlaveActivity::kill();
        resultStream.clear();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        abortSoon = false;
        unsigned sequence = helper->querySequence();
        if ((int)sequence >= 0)
        {
            assertex(container.queryResultsGraph());
            graph_id resultGraphId = container.queryXGMML().getPropInt("att[@name=\"_graphId\"]/@value");
            if (!resultGraphId)
                resultGraphId = container.queryResultsGraph()->queryGraphId();
            Owned<CGraphBase> graph = queryJob().getGraph(resultGraphId);
            Owned<IThorResult> result = graph->getGraphLoopResult(sequence, queryGraph().isLocalChild());
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
        resultStream.clear();
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
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        return graph->createGraphLoopResult(*this, input->queryFromActivity(), !queryGraph().isLocalChild());
    }
};

activityslaves_decl CActivityBase *createGraphLoopResultWriteSlave(CGraphElementBase *container)
{
    return new CGraphLoopResultWriteSlaveActivity(container);
}

