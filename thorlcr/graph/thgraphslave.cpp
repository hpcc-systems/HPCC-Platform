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

#include "jlib.hpp"
#include "jcontainerized.hpp"
#include "jlzw.hpp"
#include "jhtree.hpp"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "commonext.hpp"
#include "thorplugin.hpp"
#include "thcodectx.hpp"
#include "thmem.hpp"
#include "thorport.hpp"
#include "slwatchdog.hpp"
#include "thgraphslave.hpp"
#include "thcompressutil.hpp"
#include "enginecontext.hpp"
#include "rmtclient.hpp"

//////////////////////////////////

class CBarrierSlave : public CInterface, implements IBarrier
{
    mptag_t tag;
    Linked<ICommunicator> comm;
    bool receiving;
    CJobChannel &jobChannel;

public:
    IMPLEMENT_IINTERFACE;

    CBarrierSlave(CJobChannel &_jobChannel, ICommunicator &_comm, mptag_t _tag) : jobChannel(_jobChannel), comm(&_comm), tag(_tag)
    {
        receiving = false;
    }
    virtual bool wait(bool exception, unsigned timeout) override
    {
        Owned<IException> e;
        CTimeMon tm(timeout);
        unsigned remaining = timeout;
        CMessageBuffer msg;
        msg.append(false);
        msg.append(false); // no exception
        if (INFINITE != timeout && tm.timedout(&remaining))
        {
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        if (!comm->send(msg, 0, tag, INFINITE != timeout ? remaining : LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierSlave::wait - Timeout sending to master");
        msg.clear();
        if (INFINITE != timeout && tm.timedout(&remaining))
        {
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        {
            BooleanOnOff onOff(receiving);
            if (!comm->recv(msg, 0, tag, NULL, remaining))
                return false;
        }
        bool aborted;
        msg.read(aborted);
        bool hasExcept;
        msg.read(hasExcept);
        if (hasExcept)
            e.setown(deserializeException(msg));
        if (aborted)
        {
            if (!exception)
                return false;
            if (e)
                throw e.getClear();
            else
                throw createBarrierAbortException();
        }   
        return true;
    }
    virtual void cancel(IException *e) override
    {
        if (receiving)
            comm->cancel(jobChannel.queryMyRank(), tag);
        CMessageBuffer msg;
        msg.append(true);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (!comm->send(msg, 0, tag, LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierSlave::cancel - Timeout sending to master");
    }
    virtual mptag_t queryTag() const override { return tag; }
};


bool canStall(IThorDataLink *input)
{
    return input->queryFromActivity()->canStall();
}

//
bool CThorInput::isFastThrough() const
{
    return itdl->queryFromActivity()->isFastThrough();
}

bool CThorInput::suppressLookAhead() const
{
    return itdl->queryFromActivity()->suppressLookAhead();
}

// 

CSlaveActivity::CSlaveActivity(CGraphElementBase *_container, const StatisticsMapping &statsMapping)
    : CActivityBase(_container, statsMapping), CEdgeProgress(this), inactiveStats(statsMapping)
{
    data = NULL;
}

CSlaveActivity::~CSlaveActivity()
{
    inputs.kill();
    outputs.kill();
    if (data) delete [] data;
    ::ActPrintLog(this, thorDetailedLogLevel, "DESTROYED");
}

bool CSlaveActivity::hasLookAhead(unsigned index) const
{
    return inputs.item(index).hasLookAhead();
}

void CSlaveActivity::setOutputStream(unsigned index, IEngineRowStream *stream)
{
    while (outputStreams.ordinality()<=index)
        outputStreams.append(nullptr);
    outputStreams.replace(stream, index);
}

void CSlaveActivity::setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
{
    CActivityBase::setInput(index, inputActivity, inputOutIdx);
    Linked<IThorDataLink> outLink;
    if (!inputActivity)
    {
        Owned<CActivityBase> nullAct = container.factory(TAKnull);

        outLink.set(((CSlaveActivity *)(nullAct.get()))->queryOutput(0)); // NB inputOutIdx irrelevant, null has single 'fake' output
        nullAct->releaseIOs(); // normally done as graph winds up, clear now to avoid circular dependencies with outputs
    }
    else
        outLink.set(((CSlaveActivity *)inputActivity)->queryOutput(inputOutIdx));
    assertex(outLink);

    while (inputs.ordinality()<=index)
        inputs.append(* new CThorInput());
    CThorInput &newInput = inputs.item(index);
    newInput.set(outLink, inputOutIdx);
    if (0 == index && !input)
    {
        input = outLink;
        inputSourceIdx = inputOutIdx;
    }
}

void CSlaveActivity::connectInputStreams(bool consumerOrdered)
{
    ForEachItemIn(index, inputs)
    {
        CThorInput &_input = inputs.item(index);
        if (_input.itdl)
            setInputStream(index, _input, consumerOrdered);
    }
}

void CSlaveActivity::setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered)
{
    if (_input.itdl)
    {
        Owned<IStrandJunction> junction;
        IEngineRowStream *_inputStream = connectSingleStream(*this, _input.itdl, _input.sourceIdx, junction, _input.itdl->isInputOrdered(consumerOrdered));
        if (queryJob().getOptBool("TRACEROWS"))
        {
            const unsigned numTraceRows = queryJob().getOptInt("numTraceRows", 10);
            CTracingStream *tracingStream = new CTracingStream(_input.itdl, _inputStream, _input.itdl->queryFromActivity()->queryHelper(), numTraceRows);
            _input.tracingStream.setown(tracingStream);
            _inputStream = tracingStream;
        }
        _input.setStream(LINK(_inputStream));
        _input.junction.setown(junction.getClear());
        if (0 == index)
            inputStream = _inputStream;
        _input.itdl->setOutputStream(_input.sourceIdx, LINK(_inputStream)); // used by debug request only at moment. // JCSMORE - this should probably be the junction outputstream if there is one
    }
}

void CSlaveActivity::setLookAhead(unsigned index, IStartableEngineRowStream *lookAhead, bool persistent)
{
    CThorInput &_input = inputs.item(index);
    _input.setLookAhead(lookAhead, persistent);
    if (0 == index)
        inputStream = lookAhead;
    if (!persistent)
        _input.startLookAhead();
}

void CSlaveActivity::startLookAhead(unsigned index)
{
    CThorInput &_input = inputs.item(index);
    _input.startLookAhead();
    if (0 == index)
        inputStream = _input.queryStream();
}

bool CSlaveActivity::isLookAheadActive(unsigned index) const
{
    CThorInput &_input = inputs.item(index);
    return _input.isLookAheadActive();
}

void CSlaveActivity::setupSpace4FileStats(unsigned where, bool statsForMultipleFiles, bool isSuper, unsigned numSubs, const StatisticsMapping & statsMapping)
{
    unsigned slotsNeeded = 0;
    if (statsForMultipleFiles) // for variable filenames: filestats will track stats from multiple files
        slotsNeeded = isSuper ? numSubs : 1;
    else // filename fixed so only use need filestats for super files as activity stats can track file stats
        // (n.b. where == 0 when single file stats tracking)
        slotsNeeded = isSuper ? numSubs : 0;
    for (unsigned i=fileStats.size(); i<where+slotsNeeded; i++)
        fileStats.push_back(new CRuntimeStatisticCollection(statsMapping));
}

bool CSlaveActivity::isInputFastThrough(unsigned index) const
{
    CThorInput &input = inputs.item(index);
    return input.isFastThrough();
}

/* If fastThrough or suppressLookAhead, return false.
 * If not (indicating needs look ahead) and has existing lookahead, start it, return false.
 * If not (indicating needs look ahead) and no existing lookahead, return true, caller will install.
 *
 * NB: only return true if new lookahead needs installing.
 */
bool CSlaveActivity::ensureStartFTLookAhead(unsigned index)
{
    CThorInput &input = inputs.item(index);
    if (input.isFastThrough() || input.suppressLookAhead())
        return false; // no look ahead required
    else
    {
        // look ahead required
        if (input.hasLookAhead())
        {
            //ActPrintLog("Already has lookahead");
            // no change, start existing look ahead
            startLookAhead(index);
            return false; // no [new] look ahead required
        }
        else
        {
            //ActPrintLog("lookahead will be inserted");
            return true; // new look ahead required
        }
    }
}

// recurse through active inputs, if _all_ are fastThrough, return true
bool CSlaveActivity::isFastThrough() const
{
    if (!hasStarted())
        return true;
    ThorDataLinkMetaInfo info;
    getMetaInfo(info);
    if (!info.fastThrough || info.canStall) // NB: JIC - but should never be marked fastThrough==true if canStall==true
        return false;
    for (unsigned i=0; i<queryNumInputs(); i++)
    {
        IThorDataLink *input = queryInput(i);
        if (input && queryInputStarted(i))
        {
            CSlaveActivity *inputAct = input->queryFromActivity();
            if (!inputAct->isFastThrough())
                return false;
        }
    }
    return true;
}

// NB: very similar to above, should possible be merged at some point
// recurse through active inputs, if _any_ are canStall, return true
bool CSlaveActivity::canStall() const
{
    if (!hasStarted())
        return false;
    ThorDataLinkMetaInfo info;
    getMetaInfo(info);
    if (info.canStall)
        return true;
    if (info.isSource || info.canBufferInput)
        return false;

    for (unsigned i=0; i<queryNumInputs(); i++)
    {
        IThorDataLink *input = queryInput(i);
        if (input && queryInputStarted(i))
        {
            CSlaveActivity *inputAct = input->queryFromActivity();
            if (inputAct->canStall())
                return true;
        }
    }
    return false;
}

// check if activity is suppressLookAhead, or if fastThrough, check that inputs are suppressLookAhead
bool CSlaveActivity::suppressLookAhead() const
{
    if (!hasStarted())
        return true;
    ThorDataLinkMetaInfo info;
    getMetaInfo(info);
    if (info.suppressLookAhead)
        return true;
    if (!info.fastThrough || info.canStall) // NB: JIC - but should never be marked fastThrough==true if canStall==true
        return false;
    for (unsigned i=0; i<queryNumInputs(); i++)
    {
        IThorDataLink *input = queryInput(i);
        if (input && queryInputStarted(i))
        {
            CSlaveActivity *inputAct = input->queryFromActivity();
            if (!inputAct->suppressLookAhead())
                return false;
        }
    }
    return true;
}


IStrandJunction *CSlaveActivity::getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks)
{
    // Default non-stranded implementation, expects activity to have 1 output.
    // By default, activities are assumed NOT to support streams
    bool inputOrdered = isInputOrdered(consumerOrdered);
    connectInputStreams(inputOrdered);
    // Return a single stream
    // Default activity impl. adds single output as stream
    streams.append(this);
    return nullptr;
}

void CSlaveActivity::appendOutput(IThorDataLink *itdl)
{
    outputs.append(itdl);
}

void CSlaveActivity::appendOutputLinked(IThorDataLink *itdl)
{
    if (itdl)
        itdl->Link();
    appendOutput(itdl);
}

IThorDataLink *CSlaveActivity::queryOutput(unsigned index) const
{
    if (index>=outputs.ordinality()) return nullptr;
    return outputs.item(index);
}

IThorDataLink *CSlaveActivity::queryInput(unsigned index) const
{
    if (index>=inputs.ordinality()) return nullptr;
    return inputs.item(index).itdl;
}

IEngineRowStream *CSlaveActivity::queryInputStream(unsigned index) const
{
    if (index>=inputs.ordinality()) return nullptr;
    return inputs.item(index).queryStream();
}

IStrandJunction *CSlaveActivity::queryInputJunction(unsigned index) const
{
    if (index>=inputs.ordinality()) return nullptr;
    return inputs.item(index).junction;
}

IEngineRowStream *CSlaveActivity::queryOutputStream(unsigned index) const
{
    if (index>=outputStreams.ordinality()) return nullptr;
    return outputStreams.item(index);
}

void CSlaveActivity::start()
{
    if (inputs.ordinality()>1)
        throwUnexpected();
    if (input)
        startInput(0);
    dataLinkStart();
}

void CSlaveActivity::startAllInputs()
{
    ActivityTimer s(slaveTimerStats, timeActivities);
    ForEachItemIn(i, inputs)
    {
        try { startInput(i); }
        catch (CATCHALL)
        {
            ActPrintLog("External(%" ACTPF "d): Error staring input %d", container.queryId(), i);
            throw;
        }
    }
}

void CSlaveActivity::startInput(unsigned index, const char *extra)
{
    VStringBuffer s("Starting input %u", index);
    if (extra)
        s.append(" ").append(extra);
    ActPrintLog("%s", s.str());

    CThorInput &_input = inputs.item(index);
#ifdef TRACE_STARTSTOP_EXCEPTIONS
    try
    {
#endif
        _input.start();
        if (0 == index)
        {
            inputStopped = false;
            inputStream = _input.queryStream();
        }
#ifdef TRACE_STARTSTOP_EXCEPTIONS
    }
    catch(IException *e)
    {
        ActPrintLog(e, "%s", s.str());
        throw;
    }
#endif
}

void CSlaveActivity::stop()
{
    if (input)
        stopInput(0);
    dataLinkStop();
}

void CSlaveActivity::stopInput(unsigned index, const char *extra)
{
    CThorInput &_input = inputs.item(index);
    if (_input.stopped)
        return;
    VStringBuffer s("Stopping input %u for", index);
    if (extra)
        s.append(" ").append(extra);
    ActPrintLog("%s", s.str());

#ifdef TRACE_STARTSTOP_EXCEPTIONS
    try
    {
#endif
        _input.stop();
        if (0 == index)
        {
            inputStopped = true;
            inputStream = _input.queryStream();
        }

#ifdef TRACE_STARTSTOP_EXCEPTIONS
    }
    catch(IException * e)
    {
        ActPrintLog(e, "%s", s.str());
        throw;
    }
#endif
}

void CSlaveActivity::stopAllInputs()
{
    ForEachItemIn(i, inputs)
    {
        stopInput(i);
    }
}

void CSlaveActivity::reset()
{
    CActivityBase::reset();
    ForEachItemIn(i, inputs)
        inputs.item(i).reset();
    inputStopped = false;
}

void CSlaveActivity::releaseIOs()
{
//  inputs.kill(); // don't want inputs to die before this dies (release in deconstructor) // JCSMORE not sure why care particularly.
    outputs.kill(); // outputs tend to be self-references, this clears them explicitly, otherwise end up leaking with circular references.
    outputStreams.kill();
}

void CSlaveActivity::clearConnections()
{
    outputStreams.kill();
    inputs.kill();
}

MemoryBuffer &CSlaveActivity::queryInitializationData(unsigned slave) const
{
    CriticalBlock b(crit);
    if (!data)
        data = new MemoryBuffer[container.queryJob().querySlaves()];
    CMessageBuffer msg;
    graph_id gid = queryContainer().queryOwner().queryGraphId();
    msg.append(smt_dataReq);
    msg.append(slave);
    msg.append(gid);
    msg.append(container.queryId());
    if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryContainer().queryJob().querySlaveMpTag(), LONGTIMEOUT))
        throwUnexpected();
    data[slave].swapWith(msg);
    return data[slave];
}

MemoryBuffer &CSlaveActivity::getInitializationData(unsigned slave, MemoryBuffer &mb) const
{
    return mb.append(queryInitializationData(slave));
}

unsigned __int64 CSlaveActivity::queryLocalCycles(unsigned __int64 totalCycles, unsigned __int64 blockedCycles, unsigned __int64 lookAheadCycles) const
{
    unsigned __int64 inputCycles = 0;
    if (1 == inputs.ordinality())
    {
        IThorDataLink *input = queryInput(0);
        inputCycles += input->queryTotalCycles();
    }
    else
    {
        switch (container.getKind())
        {
            case TAKif:
            case TAKchildif:
            case TAKifaction:
            case TAKcase:
            case TAKchildcase:
                if (inputs.ordinality() && (((unsigned)-1) != container.whichBranch))
                {
                    IThorDataLink *input = queryInput(container.whichBranch);
                    if (input)
                        inputCycles += input->queryTotalCycles();
                }
                break;
            default:
                ForEachItemIn(i, inputs)
                {
                    IThorDataLink *input = queryInput(i);
                    inputCycles += input->queryTotalCycles();
                }
                break;
        }
    }
    unsigned __int64 processCycles = totalCycles + lookAheadCycles;
    if (processCycles < inputCycles) // not sure how/if possible, but guard against
        return 0;
    processCycles -= inputCycles;
    if (processCycles < blockedCycles)
    {
        ActPrintLog("CSlaveActivity::queryLocalCycles - process %" I64F "uns < blocked %" I64F "uns", cycle_to_nanosec(processCycles), cycle_to_nanosec(blockedCycles));
        return 0;
    }
    return processCycles-blockedCycles;
}

unsigned __int64 CSlaveActivity::queryLocalCycles() const
{
    return queryLocalCycles(queryTotalCycles(), queryBlockedCycles(), queryLookAheadCycles());
}

void CSlaveActivity::serializeStats(MemoryBuffer &mb)
{
    CriticalBlock b(crit); // JCSMORE not sure what this is protecting..

    // Collates previously collected static(inactive) stats with live stats collected via a virtual callback (gatherActiveStats),
    // and updates the activity's 'stats'.
    // The callback fetches the current state of the activity's stats, called within a critical section ('statsCs'),
    // which the activity should use to protect any objects it uses whilst stats are being collected.

    CRuntimeStatisticCollection serializedStats(inactiveStats);

    {
        CriticalBlock block(statsCs);
        gatherActiveStats(serializedStats);
    }

    queryCodeContext()->gatherStats(serializedStats);

    // JCS->GH - should these be serialized as cycles, and a different mapping used on master?
    //
    // Note: Look ahead cycles are not being kept up to date in slaverStats as multiple objects and threads are updating
    // look ahead cycles.  At the moment, each thread and objects that generate look ahead cycles, track its own look ahead
    // cycles and the up to date lookahead cycles is only available with a call to queryLookAheadCycles().  The code would
    // need to be refactored to change this behaviour.
    unsigned __int64 lookAheadCycles = queryLookAheadCycles();
    unsigned __int64 localCycles = queryLocalCycles(queryTotalCycles(), queryBlockedCycles(), lookAheadCycles);
    serializedStats.setStatistic(StTimeLookAhead, (unsigned __int64)cycle_to_nanosec(lookAheadCycles));
    serializedStats.setStatistic(StTimeLocalExecute, (unsigned __int64)cycle_to_nanosec(localCycles));
    slaveTimerStats.addStatistics(serializedStats);
    serializedStats.serialize(mb);
    ForEachItemIn(i, outputs)
    {
        IThorDataLink *output = queryOutput(i);
        if (output)
            outputs.item(i)->dataLinkSerialize(mb);
        else
            serializeNullItdl(mb);
    }
}

void CSlaveActivity::debugRequest(unsigned edgeIdx, MemoryBuffer &msg)
{
    IEngineRowStream *outputStream = queryOutputStream(edgeIdx);
    IThorDebug *debug = QUERYINTERFACE(outputStream, IThorDebug); // should probably use an extended IEngineRowStream, or store in separate array instead
    if (debug) debug->debugRequest(msg);
}

bool CSlaveActivity::isGrouped() const
{
    if (!input) return false; // should possible be an error if query and not set
    return input->isGrouped();
}

IOutputMetaData *CSlaveActivity::queryOutputMeta() const
{
    return queryHelper()->queryOutputMeta();
}

void CSlaveActivity::dataLinkSerialize(MemoryBuffer &mb) const
{
    CEdgeProgress::dataLinkSerialize(mb);
}

rowcount_t CSlaveActivity::getProgressCount() const
{
    return CEdgeProgress::getCount();
}

void CSlaveActivity::debugRequest(MemoryBuffer &msg)
{
}


/// CThorStrandProcessor

CThorStrandProcessor::CThorStrandProcessor(CThorStrandedActivity &_parent, IEngineRowStream *_inputStream, unsigned _outputId)
  : parent(_parent), inputStream(_inputStream), outputId(_outputId), timeActivities(_parent.queryTimeActivities())
{
    rowsProcessed = 0;
    baseHelper.set(parent.queryHelper());
}

void CThorStrandProcessor::processAndThrowOwnedException(IException *_e)
{
    IThorException *e = QUERYINTERFACE(_e, IThorException);
    if (e)
    {
        if (!e->queryActivityId())
            setExceptionActivityInfo(parent.queryContainer(), e);
    }
    else
    {
        e = MakeActivityException(&parent, _e);
        _e->Release();
    }
    throw e;
}

void CThorStrandProcessor::stop()
{
    if (!stopped)
    {
        if (inputStream)
            inputStream->stop();
        parent.strandedStop();
    }
    stopped = true;
}


/// CThorStrandedActivity

void CThorStrandedActivity::onStartStrands()
{
    active = strands.ordinality();
    ForEachItemIn(idx, strands)
        strands.item(idx).start();
}

void CThorStrandedActivity::strandedStop()
{
    // Called from the strands... which should ensure that stop is not called more than once per strand
    //The first strand to call
    if (active)
        --active;
    if (!active)
        stop();
}

//This function is pure (But also implemented out of line) to force the derived classes to implement it.
//After calling the base class start method, and initialising any values from the helper they must call onStartStrands(),
//this must also happen before any rows are read from the strands (e.g., by a source junction)
//    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;

//For some reason gcc doesn't let you specify a function as pure virtual and define it at the same time.
void CThorStrandedActivity::start()
{
    SimpleActivityTimer t(startCycles, timeActivities);
    CSlaveActivity::start();
    startJunction(splitter);
    onStartStrands();
}

void CThorStrandedActivity::reset()
{
    assertex(active==0);
    ForEachItemIn(idx, strands)
        strands.item(idx).reset();
    resetJunction(splitter);
    CSlaveActivity::reset();
    resetJunction(sourceJunction);
}

IStrandJunction *CThorStrandedActivity::getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks)
{
    assertex(idx == 0);
    assertex(strands.empty());

    // JCSMORE these may be wrong if this is a source activity
    bool inputOrdered = input ? input->isInputOrdered(consumerOrdered) : isInputOrdered(consumerOrdered);
    //Note, numStrands == 1 is an explicit request to disable threading
    if (consumerOptions && (consumerOptions->numStrands != 1) && (strandOptions.numStrands != 1))
    {
        //Check to see if the consumer's settings should override
        if (strandOptions.numStrands == 0)
        {
            strandOptions.numStrands = consumerOptions->numStrands;
            strandOptions.blockSize = consumerOptions->blockSize;
        }
        else if (consumerOptions->numStrands > strandOptions.numStrands)
        {
            strandOptions.numStrands = consumerOptions->numStrands;
        }
    }

    Owned <IStrandJunction> recombiner;
    if (input)
    {
        if (strandOptions.numStrands == 1)
        {
            // 1 means explicitly requested single-strand.
            Owned<IStrandJunction> junction;
            IEngineRowStream *instream = connectSingleStream(ctx, input, inputSourceIdx, junction, inputOrdered);
            inputs.item(idx).junction.setown(junction.getClear());
            strands.append(*createStrandProcessor(instream));
        }
        else
        {
            PointerArrayOf<IEngineRowStream> instreams;
            recombiner.setown(input->getOutputStreams(ctx, inputSourceIdx, instreams, &strandOptions, inputOrdered, orderedCallbacks));
            if ((instreams.length() == 1) && (strandOptions.numStrands != 0))  // 0 means did not specify - we should use the strands that our upstream provides
            {
                assertex(recombiner == nullptr);
                // Create a splitter to split the input into n... and a recombiner if need to preserve sorting
                if (inputOrdered)
                {
                    branch.setown(createStrandBranch(*ctx.queryRowManager(), strandOptions.numStrands, strandOptions.blockSize, true, input->queryOutputMeta()->isGrouped(), false, orderedCallbacks));
                    splitter.set(branch->queryInputJunction());
                    recombiner.set(branch->queryOutputJunction());
                }
                else
                {
                    splitter.setown(createStrandJunction(*ctx.queryRowManager(), 1, strandOptions.numStrands, strandOptions.blockSize, false));
                }
                splitter->setInput(0, instreams.item(0));
                for (unsigned strandNo = 0; strandNo < strandOptions.numStrands; strandNo++)
                    strands.append(*createStrandProcessor(splitter->queryOutput(strandNo)));
            }
            else
            {
                // Ignore my hint and just use the width already split into...
                ForEachItemIn(strandNo, instreams)
                    strands.append(*createStrandProcessor(instreams.item(strandNo)));
            }
        }
    }
    else
    {
        unsigned numStrands = strandOptions.numStrands ? strandOptions.numStrands : 1;
        for (unsigned i=0; i < numStrands; i++)
            strands.append(*createStrandSourceProcessor(inputOrdered));

        if (inputOrdered && (numStrands > 1))
        {
            if (consumerOptions)
            {
                //If the output activities are also stranded then need to create a version of the branch
                bool isGrouped = queryOutputMeta()->isGrouped();
                branch.setown(createStrandBranch(*ctx.queryRowManager(), strandOptions.numStrands, strandOptions.blockSize, true, isGrouped, true, orderedCallbacks));
                sourceJunction.set(branch->queryInputJunction());
                recombiner.set(branch->queryOutputJunction());
                assertex((orderedCallbacks && !recombiner) || (!orderedCallbacks && recombiner));

                //This is different from the branch above.  The first "junction" has the source activity as the input, and the outputs as the result of the activity
                for (unsigned strandNo = 0; strandNo < strandOptions.numStrands; strandNo++)
                {
                    sourceJunction->setInput(strandNo, &strands.item(strandNo));
                    streams.append(sourceJunction->queryOutput(strandNo));
                }
#ifdef TRACE_STRANDS
                if (traceLevel > 2)
                    DBGLOG("Executing activity %u with %u strands", activityId, strands.ordinality());
#endif
                return recombiner.getClear();
            }
            else
                recombiner.setown(createStrandJunction(*ctx.queryRowManager(), numStrands, 1, strandOptions.blockSize, inputOrdered));
        }
    }
    ForEachItemIn(i, strands)
        streams.append(&strands.item(i));
#ifdef TRACE_STRANDS
    if (traceLevel > 2)
        DBGLOG("Executing activity %u with %u strands", activityId, strands.ordinality());
#endif

    return recombiner.getClear();
}

unsigned __int64 CThorStrandedActivity::queryTotalCycles() const
{
    unsigned __int64 total = startCycles;
    ForEachItemIn(i, strands)
    {
        CThorStrandProcessor &strand = strands.item(i);
        total += strand.queryTotalCycles();
    }
    return total;
}

void CThorStrandedActivity::dataLinkSerialize(MemoryBuffer &mb) const
{
    mb.append(getProgressCount());
}

rowcount_t CThorStrandedActivity::getProgressCount() const
{
    rowcount_t totalCount = getCount();
    ForEachItemIn(i, strands)
    {
        CThorStrandProcessor &strand = strands.item(i);
        totalCount += strand.getCount();
    }
    return totalCount;
}

// CSlaveLateStartActivity

void CSlaveLateStartActivity::lateStart(bool any)
{
    prefiltered = !any;
    if (!prefiltered)
        startInput(0);
    else
        stopInput(0);
}

void CSlaveLateStartActivity::start()
{
    Linked<CThorInput> savedInput = &inputs.item(0);
    if (!nullInput)
    {
        nullInput.setown(new CThorInput);
        nullInput->sourceIdx = savedInput->sourceIdx; // probably not needed
    }
    inputs.replace(* nullInput.getLink(), 0);
    input = NULL;
    CSlaveActivity::start();
    inputs.replace(* savedInput.getClear(), 0);
    input = inputs.item(0).itdl;
}

void CSlaveLateStartActivity::stop()
{
    if (!prefiltered)
    {
        stopInput(0);
        dataLinkStop();
    }
}

void CSlaveLateStartActivity::reset()
{
    CSlaveActivity::reset();
    prefiltered = false;
}

// CSlaveGraph

CSlaveGraph::CSlaveGraph(CJobChannel &jobChannel) : CGraphBase(jobChannel), progressActive(false)
{
    jobS = (CJobSlave *)&jobChannel.queryJob();
}

void CSlaveGraph::init(MemoryBuffer &mb)
{
    mpTag = queryJobChannel().deserializeMPTag(mb);
    startBarrierTag = queryJobChannel().deserializeMPTag(mb);
    waitBarrierTag = queryJobChannel().deserializeMPTag(mb);
    doneBarrierTag = queryJobChannel().deserializeMPTag(mb);
    startBarrier = queryJobChannel().createBarrier(startBarrierTag);
    waitBarrier = queryJobChannel().createBarrier(waitBarrierTag);
    if (doneBarrierTag != TAG_NULL)
        doneBarrier = queryJobChannel().createBarrier(doneBarrierTag);
    sourceActDependents.setown(createPTree(mb));
    unsigned subCount;
    mb.read(subCount);
    while (subCount--)
    {
        graph_id gid;
        mb.read(gid);
        Owned<CSlaveGraph> subGraph = (CSlaveGraph *)queryJobChannel().getGraph(gid);
        subGraph->init(mb);
    }
}

void CSlaveGraph::reset()
{
    CGraphBase::reset();
    progressActive = false;
}

void CSlaveGraph::initWithActData(MemoryBuffer &in, MemoryBuffer &out)
{
    activity_id id;
    for (;;)
    {
        in.read(id);
        if (0 == id) break;
        CSlaveGraphElement *element = (CSlaveGraphElement *)queryElement(id);
        assertex(element);
        out.append(id);
        out.append((size32_t)0);
        unsigned l = out.length();
        size32_t sz;
        in.read(sz);
        unsigned aread = in.getPos();
        CSlaveActivity *activity = (CSlaveActivity *)element->queryActivity();
        assertex(activity);
        element->sentActInitData->set(0);
        activity->init(in, out);
        aread = in.getPos()-aread;
        if (aread<sz)
        {
            Owned<IException> e = MakeActivityException(element, TE_SeriailzationError, "Serialization error - activity did not read all serialized data (%d byte(s) remaining)", sz-aread);
            in.readDirect(sz-aread);
            throw e.getClear();
        }
        else if (aread>sz)
            throw MakeActivityException(element, TE_SeriailzationError, "Serialization error - activity read beyond serialized data (%d byte(s))", aread-sz);
        activity->setInitialized(true);
        size32_t dl = out.length() - l;
        if (dl)
            out.writeDirect(l-sizeof(size32_t), sizeof(size32_t), &dl);
        else
            out.setLength(l-(sizeof(activity_id)+sizeof(size32_t)));
    }
    out.append((activity_id)0);
}

bool CSlaveGraph::recvActivityInitData(size32_t parentExtractSz, const byte *parentExtract)
{
    unsigned needActInit = 0;
    unsigned uninitialized = 0;
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = (CGraphElementBase &)iter->query();
        CActivityBase *activity = element.queryActivity();
        if (activity)
        {
            if (activity->needReInit())
            {
                element.sentActInitData->set(0, false); // force act init to be resent
                activity->setInitialized(false);
            }
            if (!element.sentActInitData->test(0))
                ++needActInit;
            if (!activity->queryInitialized())
                ++uninitialized;
        }
    }
    if (0 == uninitialized)
        return true;
    mptag_t replyTag = TAG_NULL;
    size32_t len = 0;
    CMessageBuffer actInitRtnData;
    actInitRtnData.append(false);
    CMessageBuffer msg;

    if (syncInitData())
    {
        CTimeMon timer;
        while (!graphCancelHandler.recv(queryJobChannel().queryJobComm(), msg, 0, mpTag, NULL, MEDIUMTIMEOUT))
        {
            if (graphCancelHandler.isCancelled())
                throw MakeStringException(0, "Aborted whilst waiting to receive actinit data for graph: %" GIDPF "d", graphId);
            // put an upper limit on time Thor can be stalled here
            unsigned mins = timer.elapsed()/60000;
            if (mins >= jobS->queryActInitWaitTimeMins())
                throw MakeStringException(0, "Timed out after %u minutes, waiting to receive actinit data for graph: %" GIDPF "u", mins, graphId);

            GraphPrintLogEx(this, thorlog_null, MCwarning, "Waited %u minutes for activity initialization message (Master may be blocked on a file lock?).", mins);
        }
        replyTag = msg.getReplyTag();
        msg.read(len);
    }
    else
    {
        if (needActInit)
        {
            // initialize any for which no data was sent
            msg.append(smt_initActDataReq); // may cause graph to be created at master
            msg.append(queryGraphId());
            msg.append(queryJobChannel().queryMyRank()-1);
            assertex(!parentExtractSz || NULL!=parentExtract);
            msg.append(parentExtractSz);
            msg.append(parentExtractSz, parentExtract);

            // NB: will only request activities that need initializaton data
            ForEach(*iter)
            {
                CSlaveGraphElement &element = (CSlaveGraphElement &)iter->query();
                CActivityBase *activity = element.queryActivity();
                if (activity)
                {
                    if (!element.sentActInitData->test(0))
                    {
                        msg.append(element.queryId());
                        // JCSMORE -> GH - do you always generate a start context serializer?
                        element.serializeStartContext(msg);
                    }
                }
            }
            msg.append((activity_id)0);
            if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                throwUnexpected();
            replyTag = queryJobChannel().deserializeMPTag(msg);
            bool error;
            msg.read(error);
            if (error)
            {
                Owned<IException> e = deserializeException(msg);
                IERRLOG(e, "Master hit exception");
                msg.clear();
                if (!queryJobChannel().queryJobComm().send(msg, 0, replyTag, LONGTIMEOUT))
                    throw MakeStringException(0, "Timeout sending init data back to master");
                throw e.getClear();
            }
            msg.read(len);
        }
    }
    Owned<IException> exception;
    if (len)
    {
        try
        {
            MemoryBuffer actInitData;
            actInitData.append(len, msg.readDirect(len));
            CriticalBlock b(progressCrit);
            initWithActData(actInitData, actInitRtnData);
        }
        catch (IException *e)
        {
            actInitRtnData.clear();
            actInitRtnData.append(true);
            serializeThorException(e, actInitRtnData);
            exception.setown(e);
        }
    }
    if (syncInitData() || needActInit)
    {
        if (!queryJobChannel().queryJobComm().send(actInitRtnData, 0, replyTag, LONGTIMEOUT))
            throw MakeStringException(0, "Timeout sending init data back to master");
    }
    if (exception)
        throw exception.getClear();
    // initialize any for which no data was sent
    ForEach(*iter)
    {
        CSlaveGraphElement &element = (CSlaveGraphElement &)iter->query();
        CSlaveActivity *activity = (CSlaveActivity *)element.queryActivity();
        if (activity && !activity->queryInitialized())
        {
            activity->setInitialized(true);
            element.sentActInitData->set(0);
            MemoryBuffer in, out;
            activity->init(in, out);
            assertex(0 == out.length());
        }
    }
    return true;
}

bool CSlaveGraph::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    if (!recvActivityInitData(parentExtractSz, parentExtract))
        throw MakeThorException(0, "preStart failure");
    CGraphBase::preStart(parentExtractSz, parentExtract);
    if (isGlobal())
    {
        if (!startBarrier->wait(false))
            return false;
    }
    return true;
}

void CSlaveGraph::start()
{
    progressActive.store(true); // remains true whilst graph is running
    setProgressUpdated(); // may remain true after graph is running
    bool forceAsync = !queryOwner() || isGlobal();
    Owned<IThorActivityIterator> iter = getSinkIterator();
    unsigned sinks = 0;
    ForEach(*iter)
        ++sinks;
    ForEach(*iter)
    {
        CGraphElementBase &container = iter->query();
        CActivityBase *sinkAct = (CActivityBase *)container.queryActivity();
        --sinks;
        sinkAct->startProcess(forceAsync || 0 != sinks); // async, unless last
    }
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            jobS->queryProgressHandler()->startGraph(*this);
    }
}

void CSlaveGraph::connect()
{
    CriticalBlock b(progressCrit);
    Owned<IThorActivityIterator> iter = getConnectedIterator(false);
    ForEach(*iter)
        iter->query().doconnect();
    iter.setown(getSinkIterator());
    ForEach(*iter)
    {
        CGraphElementBase &container = iter->query();
        CSlaveActivity *sinkAct = (CSlaveActivity *)container.queryActivity();
        sinkAct->connectInputStreams(true);
    }
}

void CSlaveGraph::executeSubGraph(size32_t parentExtractSz, const byte *parentExtract)
{
    if (isComplete())
        return;
    processStartInfo.update(ReadAllInfo);
    Owned<IException> exception;
    try
    {
        if (!doneInit)
        {
            doneInit = true;
            if (queryOwner())
            {
                if (isGlobal())
                {
                    CMessageBuffer msg;
                    if (!graphCancelHandler.recv(queryJobChannel().queryJobComm(), msg, 0, mpTag, NULL, LONGTIMEOUT))
                        throw MakeStringException(0, "Error receiving createctx data for graph: %" GIDPF "d", graphId);
                    try
                    {
                        size32_t len;
                        msg.read(len);
                        if (len)
                        {
                            MemoryBuffer initData;
                            initData.append(len, msg.readDirect(len));
                            deserializeCreateContexts(initData);
                        }
                        msg.clear();
                        msg.append(false);
                    }
                    catch (IException *e)
                    {
                        msg.clear();
                        msg.append(true);
                        serializeThorException(e, msg);
                    }
                    if (!queryJobChannel().queryJobComm().send(msg, 0, msg.getReplyTag(), LONGTIMEOUT))
                        throw MakeStringException(0, "Timeout sending init data back to master");
                }
                else
                {
                    CMessageBuffer msg;
                    msg.append(smt_initGraphReq);
                    msg.append(graphId);
                    if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                        throwUnexpected();
                    size32_t len;
                    msg.read(len);
                    if (len)
                        deserializeCreateContexts(msg);
        // could still request 1 off, onCreate serialization from master 1st.
                }
            }
            connect(); // only now do slave acts. have all their outputs prepared.
        }
        CGraphBase::executeSubGraph(parentExtractSz, parentExtract);
        jobS->querySharedAllocator()->queryRowManager()->resetPeakMemory();
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "In executeSubGraph");
        exception.setown(e);
    }
    if (TAG_NULL != executeReplyTag)
    {
        CMessageBuffer msg;
        if (exception.get())
        {
            msg.append(true);
            serializeThorException(exception, msg);
        }
        else
            msg.append(false);
        queryJobChannel().queryJobComm().send(msg, 0, executeReplyTag, LONGTIMEOUT);
    }
    else if (exception)
        throw exception.getClear();
}

void CSlaveGraph::abort(IException *e)
{
    if (aborted)
        return;
    if (!graphDone) // set pre done(), no need to abort if got that far.
        CGraphBase::abort(e);
    getDoneSem.signal();
}

void CSlaveGraph::done()
{
    if (started)
    {
        GraphPrintLog("End of sub-graph");
        progressActive.store(false);
        setProgressUpdated(); // NB: ensure collected after end of graph

        if (initialized && (!queryOwner() || isGlobal()))
        {
            if (aborted || !graphDone)
            {
                if (!getDoneSem.wait(SHORTTIMEOUT)) // wait on master to clear up, gather info from slaves
                    IWARNLOG("CSlaveGraph::done - timedout waiting for master to signal done()");
            }
            else
                getDoneSem.wait();
        }
        if (!queryOwner())
        {
            if (globals->getPropBool("@watchdogProgressEnabled"))
                jobS->queryProgressHandler()->stopGraph(*this, NULL);
        }
    }

    Owned<IException> exception;
    try
    {
        CGraphBase::done();
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "In CSlaveGraph::done");
        exception.setown(e);
    }
    if (exception.get())
        throw LINK(exception.get());
}

bool CSlaveGraph::serializeStats(MemoryBuffer &mb)
{
    unsigned beginPos = mb.length();
    mb.append(queryGraphId());

    CRuntimeStatisticCollection stats(graphStatistics);
    stats.setStatistic(StNumExecutions, numExecuted);

    ProcessInfo processActiveInfo(ReadAllInfo);
    SystemProcessInfo processElapsed = processActiveInfo - processStartInfo;
    stats.setStatistic(StTimeUser, processElapsed.getUserNs());
    stats.setStatistic(StTimeSystem, processElapsed.getSystemNs());
    stats.setStatistic(StNumContextSwitches, processElapsed.getNumContextSwitches());
    stats.setStatistic(StSizeMemory, processActiveInfo.getActiveResidentMemory());
    stats.setStatistic(StSizePeakMemory, processActiveInfo.getPeakResidentMemory());
    jobS->querySharedAllocator()->queryRowManager()->reportSummaryStatistics(stats);

    IGraphTempHandler *tempHandler = owner ? queryTempHandler(false) : queryJob().queryTempHandler();
    offset_t sizeGraphSpill = tempHandler ? tempHandler->getActiveUsageSize() : 0;
    if (tempHandler)
        stats.mergeStatistic(StSizeGraphSpill, sizeGraphSpill);

    offset_t peakTempSize = queryPeakTempSize();
    if (peakTempSize)
        stats.mergeStatistic(StSizePeakTempDisk, peakTempSize);
    if (peakTempSize + sizeGraphSpill)
        stats.mergeStatistic(StSizePeakEphemeralDisk, peakTempSize + sizeGraphSpill);
    stats.serialize(mb);

    unsigned cPos = mb.length();
    unsigned count = 0;
    mb.append(count);
    CriticalBlock b(progressCrit);
    // until started and activities initialized, activities are not ready to serialize stats.
    if ((started&&initialized) || 0 == activityCount())
    {
        if (checkProgressUpdatedAndClear() || progressActive)
        {
            Owned<IThorActivityIterator> iter = getConnectedIterator();
            ForEach (*iter)
            {
                CGraphElementBase &element = iter->query();
                CSlaveActivity &activity = (CSlaveActivity &)*element.queryActivity();
                mb.append(activity.queryContainer().queryId());
                activity.serializeStats(mb);
                ++count;
            }
            mb.writeDirect(cPos, sizeof(count), &count);
        }
        unsigned cqCountPos = mb.length();
        unsigned cq=0;
        mb.append(cq);
        Owned<IThorGraphStubIterator> childIter = getChildStubIterator();
        ForEach(*childIter)
        {
            CGraphStub &stub = childIter->query();
            if (stub.serializeStats(mb))
                ++cq;
        }
        if (count || cq)
        {
            mb.writeDirect(cqCountPos, sizeof(cq), &cq);
            return true;
        }
    }
    mb.rewrite(beginPos);
    return false;
}

void CSlaveGraph::serializeDone(MemoryBuffer &mb)
{
    mb.append(queryGraphId());
    unsigned cPos = mb.length();
    unsigned count=0;
    mb.append(count);
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        if (element.queryActivity())
        {
            CSlaveActivity &activity = (CSlaveActivity &)*element.queryActivity();
            unsigned rPos = mb.length();
            mb.append(element.queryId());
            unsigned nl=0;
            mb.append(nl); // place holder for size of mb
            unsigned l = mb.length();
            activity.processDone(mb);
            nl = mb.length()-l;
            if (0 == nl)
                mb.rewrite(rPos);
            else
            {
                mb.writeDirect(l-sizeof(nl), sizeof(nl), &nl);
                ++count;
            }
        }
    }
    mb.writeDirect(cPos, sizeof(count), &count);
}

void CSlaveGraph::getDone(MemoryBuffer &doneInfoMb)
{
    if (!started) return;
    ::GraphPrintLog(this, thorDetailedLogLevel, "Entering getDone");
    if (!queryOwner() || isGlobal())
    {
        try
        {
            serializeDone(doneInfoMb);
            if (!queryOwner())
            {
                if (globals->getPropBool("@watchdogProgressEnabled"))
                    jobS->queryProgressHandler()->stopGraph(*this, &doneInfoMb);
            }
        }
        catch (IException *)
        {
            ::GraphPrintLog(this, thorDetailedLogLevel, "Leaving getDone");
            getDoneSem.signal();
            throw;
        }
    }
    ::GraphPrintLog(this, thorDetailedLogLevel, "Leaving getDone");
    getDoneSem.signal();
}


class CThorSlaveGraphResults : public CThorGraphResults
{
    CSlaveGraph &graph;
    IArrayOf<IThorResult> globalResults;
    PointerArrayOf<CriticalSection> globalResultCrits;
    void ensureAtLeastGlobals(unsigned id)
    {
        while (globalResults.ordinality() < id)
        {
            globalResults.append(*new CThorUninitializedGraphResults(globalResults.ordinality()));
            globalResultCrits.append(new CriticalSection);
        }
    }
public:
    CThorSlaveGraphResults(CSlaveGraph &_graph,unsigned numResults) : CThorGraphResults(numResults), graph(_graph)
    {
    }
    ~CThorSlaveGraphResults()
    {
        clear();
    }
    virtual void clear()
    {
        CriticalBlock procedure(cs);
        results.kill();
        globalResults.kill();
        ForEachItemIn(i, globalResultCrits)
            delete globalResultCrits.item(i);
        globalResultCrits.kill();
    }
    IThorResult *getResult(unsigned id, bool distributed)
    {
        Linked<IThorResult> result;
        {
            CriticalBlock procedure(cs);
            ensureAtLeast(id+1);

            result.set(&results.item(id));
            if (!distributed || !result->isDistributed())
                return result.getClear();
            ensureAtLeastGlobals(id+1);
        }
        CriticalBlock b(*globalResultCrits.item(id)); // block other global requests for this result
        IThorResult *globalResult = &globalResults.item(id);
        if (!QUERYINTERFACE(globalResult, CThorUninitializedGraphResults))
            return LINK(globalResult);
        Owned<IThorResult> gr = graph.getGlobalResult(*result->queryActivity(), result->queryRowInterfaces(), ownerId, id);
        globalResults.replace(*gr.getLink(), id);
        return gr.getClear();
    }
};

IThorGraphResults *CSlaveGraph::createThorGraphResults(unsigned num)
{
    return new CThorSlaveGraphResults(*this, num);
}

IThorResult *CSlaveGraph::getGlobalResult(CActivityBase &activity, IThorRowInterfaces *rowIf, activity_id ownerId, unsigned id)
{
    mptag_t replyTag = queryMPServer().createReplyTag();
    CMessageBuffer msg;
    msg.setReplyTag(replyTag);
    msg.append(smt_getresult);
    msg.append(queryJobChannel().queryMyRank()-1);
    msg.append(graphId);
    msg.append(ownerId);
    msg.append(id);
    msg.append(replyTag);

    if (!queryJobChannel().queryJobComm().send(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
        throwUnexpected();

    Owned<IThorResult> result = ::createResult(activity, rowIf, thorgraphresult_nul);
    Owned<IRowWriter> resultWriter = result->getWriter();

    MemoryBuffer mb;
    Owned<IBufferedSerialInputStream> stream = createMemoryBufferSerialStream(mb);
    CThorStreamDeserializerSource rowSource(stream);

    for (;;)
    {
        for (;;)
        {
            if (activity.queryAbortSoon())
                return NULL;
            msg.clear();
            if (activity.receiveMsg(msg, 0, replyTag, NULL, 60*1000))
                break;
            ActPrintLog(&activity, "WARNING: tag %d timedout, retrying", (unsigned)replyTag);
        }
        if (!msg.length())
            break; // done
        else
        {
            bool error;
            msg.read(error);
            if (error)
                throw deserializeThorException(msg);
            ThorExpand(msg, mb.clear());
            while (!rowSource.eos())
            {
                RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
                size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, rowSource);
                resultWriter->putRow(rowBuilder.finalizeRowClear(sz));
            }
        }
    }
    return result.getClear();
}

///////////////////////////

class CThorCodeContextSlave : public CThorCodeContextBase, implements IEngineContext
{
    mptag_t mptag;
    Owned<IDistributedFileTransaction> superfiletransaction;
    mutable CIArrayOf<TerminationCallbackInfo> callbacks;
    mutable CriticalSection callbacksCrit;

    void invalidSetResult(const char * name, unsigned seq)
    {
        throw MakeStringException(0, "Attempt to output result ('%s',%d) from a child query", name ? name : "", (int)seq);
    }

public:
    CThorCodeContextSlave(CJobChannel &jobChannel, ILoadedDllEntry &querySo, IUserDescriptor &userDesc, mptag_t _mptag) : CThorCodeContextBase(jobChannel, querySo, userDesc), mptag(_mptag)
    {
    }
    virtual void setResultBool(const char *name, unsigned sequence, bool value) { invalidSetResult(name, sequence); }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { invalidSetResult(name, sequence); }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { invalidSetResult(stepname, sequence); }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { invalidSetResult(name, sequence); }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { invalidSetResult(name, sequence); }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) { invalidSetResult(stepname, sequence); }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { invalidSetResult(name, sequence); }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { invalidSetResult(name, sequence); }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { invalidSetResult(name, sequence); }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { invalidSetResult(name, sequence); }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { invalidSetResult(name, sequence); }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { invalidSetResult(name, sequence); }

    virtual bool getResultBool(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) { throwUnexpected(); }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual __int64 getResultInt(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual double getResultReal(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual char *getResultVarString(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }

    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }

    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source)
    {
        addWuExceptionEx(text, code, severity, MSGAUD_programmer, source);
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned aud, const char * source)
    {
        LOG(mapToLogMsgCategory((ErrorSeverity)severity, (MessageAudience)aud), "%s", text);
        Owned<IThorException> e = MakeThorException(code, "%s", text);
        e->setOrigin(source);
        e->setAction(tea_warning);
        e->setSeverity((ErrorSeverity)severity);
        jobChannel.fireException(e);
    }
    virtual unsigned getGraphLoopCounter() const override { return 0; }
    virtual IDebuggableContext *queryDebugContext() const override { return nullptr; }
    virtual unsigned getNodes() { return jobChannel.queryJob().querySlaves(); }
    virtual unsigned getNodeNum() { return jobChannel.queryMyRank()-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false)
    {
        CMessageBuffer msg;
        msg.append(smt_getPhysicalName);
        msg.append(logicalName);
        msg.append(getNodeNum());
        msg.append(create);
        if (!jobChannel.queryJobComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
            throwUnexpected();
        return (char *)msg.detach();
    }
    virtual unsigned __int64 getFileOffset(const char *logicalName)
    {
        CMessageBuffer msg;
        msg.append(smt_getFileOffset);
        if (!jobChannel.queryJobComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
            throwUnexpected();
        unsigned __int64 offset;
        msg.read(offset);
        return offset;
    }
    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        // NB: shouldn't really have fileservice being called on slaves
        if (!superfiletransaction.get())
            superfiletransaction.setown(createDistributedFileTransaction(userDesc, this));
        return superfiletransaction.get();
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override { throwUnexpected(); }
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override { throwUnexpected(); }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        DBGLOG("%s", text);
        Owned<IThorException> e = MakeThorException(code, "%s", text);
        e->setAssert(filename, lineno, column);
        e->setOrigin("user");
        e->setSeverity(SeverityError);
        if (!isAbort)
            e->setAction(tea_warning);
        jobChannel.fireException(e);
    }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)   { throwUnexpected(); }      // Should only call from master
    virtual IEngineContext *queryEngineContext() { return this; }
// IEngineContext impl.
    virtual DALI_UID getGlobalUniqueIds(unsigned num, SocketEndpoint *_foreignNode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignNode;
        if (_foreignNode && !_foreignNode->isNull())
            foreignNode.set(*_foreignNode);
        else
            foreignNode.set(globals->queryProp("@daliServers"));
        return ::getGlobalUniqueIds(num, &foreignNode);
    }
    virtual bool allowDaliAccess() const
    {
        // NB. includes access to foreign Dalis.
        // slaveDaliClient option deprecated, but maintained for compatibility
        return jobChannel.queryJob().getOptBool("allowDaliAccess", jobChannel.queryJob().getOptBool("slaveDaliClient"));
    }
    virtual bool allowSashaAccess() const
    {
        bool defaultAccess = isContainerized() ? false : true; // BM had access before, but not sure it should - kept for backwards compatibility
        return jobChannel.queryJob().getOptBool("allowSashaAccess", defaultAccess);
    }
    virtual StringBuffer &getQueryId(StringBuffer &result, bool isShared) const
    {
        return result.append(jobChannel.queryJob().queryWuid());
    }
    virtual void getManifestFiles(const char *type, StringArray &files) const override
    {
        const StringArray &dllFiles = querySo.queryManifestFiles(type, jobChannel.queryJob().queryWuid());
        ForEachItemIn(idx, dllFiles)
            files.append(dllFiles.item(idx));
    }
    virtual void onTermination(QueryTermCallback callback, const char *key, bool isShared) const
    {
        TerminationCallbackInfo *term(new TerminationCallbackInfo(callback, key));
        CriticalBlock b(callbacksCrit);
        callbacks.append(*term);
    }
};

class CThorCodeContextSlaveSharedMem : public CThorCodeContextSlave
{
    IThorAllocator *sharedAllocator;
public:
    CThorCodeContextSlaveSharedMem(CJobChannel &jobChannel, IThorAllocator *_sharedAllocator, ILoadedDllEntry &querySo, IUserDescriptor &userDesc, mptag_t mpTag)
        : CThorCodeContextSlave(jobChannel, querySo, userDesc, mpTag)
    {
        sharedAllocator = _sharedAllocator;
    }
    virtual IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return sharedAllocator->getRowAllocator(meta, activityId);
    }
    virtual IEngineRowAllocator *getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const
    {
        return sharedAllocator->getRowAllocator(meta, activityId, (roxiemem::RoxieHeapFlags)heapFlags);
    }
};

class CSlaveGraphTempHandler : public CGraphTempHandler
{
public:
    CSlaveGraphTempHandler(CJobBase &job, bool errorOnMissing) : CGraphTempHandler(job, errorOnMissing)
    {
    }
    virtual bool removeTemp(const char *name)
    {
        OwnedIFile ifile = createIFile(name);
        return ifile->remove();
    }
};

#define SLAVEGRAPHPOOLLIMIT 10
CJobSlave::CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *_workUnitInfo, const char *graphName, ILoadedDllEntry *_querySo, mptag_t _slavemptag) : CJobBase(_querySo, graphName), watchdog(_watchdog)
{
    workUnitInfo.set(_workUnitInfo);
    workUnitInfo->getProp("token", token);
    workUnitInfo->getProp("user", user);
    workUnitInfo->getProp("wuid", wuid);
    workUnitInfo->getProp("scope", scope);

    init();

    Owned<IProperties> traceHeaders = deserializeTraceDebugOptions(workUnitInfo->queryPropTree("Debug"));
    OwnedActiveSpanScope requestSpan = queryTraceManager().createServerSpan("run_graph", traceHeaders);
    ContextSpanScope spanScope(*logctx, requestSpan);
    requestSpan->setSpanAttribute("hpcc.wuid", wuid);
    requestSpan->setSpanAttribute("hpcc.graph", graphName);

    oldNodeCacheMem = 0;
    slavemptag = _slavemptag;

    IPropertyTree *plugins = workUnitInfo->queryPropTree("plugins");
    if (plugins)
    {
        StringBuffer pluginsDir, installDir, pluginsList;
        globals->getProp("@INSTALL_DIR", installDir); // could use for socachedir also?
        if (installDir.length())
            addPathSepChar(installDir);
        globals->getProp("@pluginsPath", pluginsDir);
        if (pluginsDir.length())
        {
            if (!isAbsolutePath(pluginsDir.str())) // if !absolute, then make relative to installDir if is one (e.g. master mount)
            {
                if (installDir.length())
                    pluginsDir.insert(0, installDir.str());
            }
            addPathSepChar(pluginsDir);
        }
        Owned<IPropertyTreeIterator> pluginIter = plugins->getElements("plugin");
        ForEach(*pluginIter)
        {
            StringBuffer pluginPath;
            IPropertyTree &plugin = pluginIter->query();
            pluginPath.append(pluginsDir).append(plugin.queryProp("@name"));
            if (pluginsList.length())
                pluginsList.append(ENVSEPCHAR);
            pluginsList.append(pluginPath);
        }
        pluginMap->loadFromList(pluginsList.str());
    }
    tmpHandler.setown(createTempHandler(true));

    applyMemorySettings("worker");

    unsigned sharedMemoryLimitPercentage = (unsigned)getWorkUnitValueInt("globalMemoryLimitPC", globals->getPropInt("@sharedMemoryLimit", 90));
    unsigned sharedMemoryMB = queryMemoryMB*sharedMemoryLimitPercentage/100;
    DBGLOG("Shared memory = %d%%", sharedMemoryLimitPercentage);

    sharedAllocator.setown(::createThorAllocator(queryMemoryMB, sharedMemoryMB, numChannels, memorySpillAtPercentage, *logctx, crcChecking, usePackedAllocator));

    StringBuffer remoteCompressedOutput;
    getOpt("remoteCompressedOutput", remoteCompressedOutput);
    if (remoteCompressedOutput.length())
        setRemoteOutputCompressionDefault(remoteCompressedOutput);

    actInitWaitTimeMins = getOptInt(THOROPT_ACTINIT_WAITTIME_MINS, DEFAULT_MAX_ACTINITWAITTIME_MINS);

    /* Need to make sure that the activity initialization timeout is at least as long
     * as the max LFN block time. i.e. so that the query doesn't spuriously abort with an
     * activity initialization timeout before it hits the configured max LFN block time.
     */
    if (queryMaxLfnBlockTimeMins() >= actInitWaitTimeMins)
        actInitWaitTimeMins = queryMaxLfnBlockTimeMins()+1;

    StringBuffer tempDir(globals->queryProp("@thorTempDirectory"));
    // multiple thor jobs can be running on same node, sharing same local disk for temp storage.
    // make unique by adding wuid+graphName+worker-num
#ifdef _CONTAINERIZED
    VStringBuffer uniqueSubDir("%s_%s_%u", wuid.str(), graphName, globals->getPropInt("@slavenum"));
    SetTempDir(tempDir, uniqueSubDir, "thtmp", false); // no point in clearing as dir. is unique per job+graph
#else
    // in bare-metal where only 1 Thor instance of each name, re-use same dir, in order to guarantee it will be cleared on startup
    VStringBuffer uniqueSubDir("%s_%u", globals->queryProp("@name"), globals->getPropInt("@slavenum"));
    SetTempDir(tempDir, uniqueSubDir, "thtmp", true);
#endif
}

CJobChannel *CJobSlave::addChannel(IMPServer *mpServer)
{
    unsigned nextChannelNum = jobChannels.ordinality();
    CJobSlaveChannel *channel = new CJobSlaveChannel(*this, mpServer, nextChannelNum);
    jobChannels.append(*channel);
    unsigned slaveNum = channel->queryMyRank();
    jobChannelSlaveNumbers[nextChannelNum] = slaveNum;
    jobSlaveChannelNum[slaveNum-1] = nextChannelNum;
    return channel;
}

void CJobSlave::startJob()
{
    CJobBase::startJob();
    unsigned minFreeSpace = (unsigned)getWorkUnitValueInt("MINIMUM_DISK_SPACE", 0);
    if (minFreeSpace)
    {
        unsigned __int64 freeSpace = getFreeSpace(queryBaseDirectory(grp_unknown, 0));
        if (freeSpace < ((unsigned __int64)minFreeSpace)*0x100000)
        {
            SocketEndpoint ep;
            ep.setLocalHost(0);
            StringBuffer s;
            throw MakeThorException(TE_NotEnoughFreeSpace, "Node %s has %u MB(s) of available disk space, specified minimum for this job: %u MB(s)", ep.getEndpointHostText(s).str(), (unsigned) freeSpace / 0x100000, minFreeSpace);
        }
    }
    queryThor().queryKeyedJoinService().setCurrentJob(*this);
}

void CJobSlave::endJob()
{
    if (jobEnded)
        return;

    queryThor().queryKeyedJoinService().reset();
    PARENT::endJob();
}

void CJobSlave::reportGraphEnd(graph_id gid)
{
    if (nodesLoaded) // wouldn't mean much if parallel jobs running
    {
        StringBuffer prefix("Graph[%" GIDPF "u] - JHTree node stats:");
        logNodeCacheStats(prefix);
    }
    if (!REJECTLOG(MCthorDetailedDebugInfo))
    {        
        JSocketStatistics stats;
        getSocketStatistics(stats);
        StringBuffer s;
        getSocketStatisticsString(stats,s);
        MLOG(MCthorDetailedDebugInfo, "Graph[%" GIDPF "u] - Socket statistics : %s\n", gid, s.str());
    }
    resetSocketStatistics();
}

__int64 CJobSlave::getWorkUnitValueInt(const char *prop, __int64 defVal) const
{
    StringBuffer propName(prop);
    return workUnitInfo->queryPropTree("Debug")->getPropInt64(propName.toLowerCase().str(), defVal);
}

StringBuffer &CJobSlave::getWorkUnitValue(const char *prop, StringBuffer &str) const
{
    StringBuffer propName(prop);
    workUnitInfo->queryPropTree("Debug")->getProp(propName.toLowerCase().str(), str);
    return str;
}

bool CJobSlave::getWorkUnitValueBool(const char *prop, bool defVal) const
{
    StringBuffer propName(prop);
    return workUnitInfo->queryPropTree("Debug")->getPropBool(propName.toLowerCase().str(), defVal);
}

double CJobSlave::getWorkUnitValueReal(const char *prop, double defVal) const
{
    StringBuffer propName(prop);
    return workUnitInfo->queryPropTree("Debug")->getPropReal(propName.toLowerCase().str(), defVal);
}

void CJobSlave::debugRequest(MemoryBuffer &msg, const char *request) const
{
    Owned<IPropertyTree> req = createPTreeFromXMLString(request);
    const char *command = req->queryName();

    if (strieq(command, "print"))
    {
        if (watchdog) watchdog->debugRequest(msg, req);
    }
    else if (strieq(command, "debuginfo"))
    {
        try
        {
            StringBuffer instanceDir;
            if (!req->getProp("@dir", instanceDir))
                throw makeStringException(0, "deguginfo command missing 'dir' attribute");
            JobInfoCaptureType captureFlags = (JobInfoCaptureType)req->getPropInt("@flags", 0);
            // NB: stacks are for all channels, but file naming is based on 1st channel
            rank_t myRank = queryJobChannel(0).queryMyRank();
            StringBuffer suffix;
            suffix.append((unsigned)myRank);

            msg.append(true);

            std::vector<std::string> capturedFiles;
            if (isContainerized())
            {
                addInstanceContextPaths(instanceDir);
                if (hasMask(captureFlags, JobInfoCaptureType::logs))
                {
                    StringBuffer logFilename(instanceDir);
                    addPathSepChar(logFilename);
                    logFilename.appendf("thorworker.%s.log", suffix.str());
                    copyPostMortemLogging(logFilename, hasMask(jobInfoCaptureBehaviour, JobInfoCaptureBehaviour::clearLogs));
                }
            }
            if (hasMask(captureFlags, JobInfoCaptureType::stacks))
            {
                std::vector<std::string> capturedFiles = captureDebugInfo(instanceDir, "thorworker", suffix);
                for (auto &file : capturedFiles)
                {
                    RemoteFilename rfn;
                    rfn.setLocalPath(file.c_str());
                    StringBuffer fullPath;
                    rfn.getRemotePath(fullPath);
                    msg.append(fullPath.str());
                }
            }
            msg.append("");
        }
        catch (IException *e)
        {
            msg.append(false);
            serializeException(e, msg);
            e->Release();
        }
    }
    else
        throw makeStringExceptionV(5300, "Command '%s' not supported by Thor", command);
}

IGraphTempHandler *CJobSlave::createTempHandler(bool errorOnMissing)
{
    return new CSlaveGraphTempHandler(*this, errorOnMissing);
}

mptag_t CJobSlave::deserializeMPTag(MemoryBuffer &mb)
{
    mptag_t tag;
    deserializeMPtag(mb, tag);
    if (TAG_NULL != tag)
    {
        LOG(MCthorDetailedDebugInfo, "CJobSlave::deserializeMPTag: tag = %d", (int)tag);
        for (unsigned c=0; c<queryJobChannels(); c++)
            queryJobChannel(c).queryJobComm().flush(tag);
    }
    return tag;
}

IThorAllocator *CJobSlave::getThorAllocator(unsigned channel)
{
    if (1 == numChannels)
        return CJobBase::getThorAllocator(channel);
    else
        return sharedAllocator->getSlaveAllocator(channel);
}

// IGraphCallback
CJobSlaveChannel::CJobSlaveChannel(CJobBase &_job, IMPServer *mpServer, unsigned channel) : CJobChannel(_job, mpServer, channel)
{
    codeCtx.setown(new CThorCodeContextSlave(*this, job.queryDllEntry(), *job.queryUserDescriptor(), job.querySlaveMpTag()));
    sharedMemCodeCtx.setown(new CThorCodeContextSlaveSharedMem(*this, job.querySharedAllocator(), job.queryDllEntry(), *job.queryUserDescriptor(), job.querySlaveMpTag()));
}

IBarrier *CJobSlaveChannel::createBarrier(mptag_t tag)
{
    return new CBarrierSlave(*this, *jobComm, tag);
}

void CJobSlaveChannel::runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract)
{
    if (!graph.queryOwner())
        CJobChannel::runSubgraph(graph, parentExtractSz, parentExtract);
    else
        graph.doExecuteChild(parentExtractSz, parentExtract);
    CriticalBlock b(graphRunCrit);
    if (!graph.queryOwner())
        removeSubGraph(graph);
}

///////////////

bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path)
{
    StringBuffer locationName, primaryName;
    RemoteFilename primaryRfn;
    partDesc.getFilename(0, primaryRfn);
    primaryRfn.getPath(primaryName);

    OwnedIFile primaryIFile = createIFile(primaryName.str());
    try
    {
        if (primaryIFile->exists())
        {
            location = 0;
            ifile.set(primaryIFile);
            path.append(primaryName);
            return true;
        }
    }
    catch (IException *e)
    {
        ActPrintLog(&activity->queryContainer(), e, "In ensurePrimary");
        e->Release();
    }
    unsigned l;
    for (l=1; l<partDesc.numCopies(); l++)
    {
        RemoteFilename altRfn;
        partDesc.getFilename(l, altRfn);
        locationName.clear();
        altRfn.getPath(locationName);
        assertex(locationName.length());
        OwnedIFile backupIFile = createIFile(locationName.str());
        try
        {
            if (backupIFile->exists())
            {
                if (primaryRfn.isLocal())
                {
                    ensureDirectoryForFile(primaryIFile->queryFilename());
                    Owned<IException> e = MakeActivityWarning(activity, 0, "Primary file missing: %s, copying backup %s to primary location", primaryIFile->queryFilename(), locationName.str());
                    activity->fireException(e);
                    StringBuffer tmpName(primaryIFile->queryFilename());
                    tmpName.append(".tmp");
                    OwnedIFile tmpFile = createIFile(tmpName.str());
                    CFIPScope fipScope(tmpName.str());
                    copyFile(tmpFile, backupIFile);
                    try
                    {
                        tmpFile->rename(pathTail(primaryIFile->queryFilename()));
                        location = 0;
                        ifile.set(primaryIFile);
                        path.append(primaryName);
                    }
                    catch (IException *e)
                    {
                        try { tmpFile->remove(); } catch (IException *e) { ActPrintLog(&activity->queryContainer(), "Failed to delete temporary file"); e->Release(); }
                        Owned<IException> e2 = MakeActivityWarning(activity, e, "Failed to restore primary, failed to rename %s to %s", tmpName.str(), primaryIFile->queryFilename());
                        e->Release();
                        activity->fireException(e2);
                        ifile.set(backupIFile);
                        location = l;
                        path.append(locationName);
                    }
                }
                else // JCSMORE - should use daliservix perhaps to ensure primary
                {
                    Owned<IException> e = MakeActivityWarning(activity, 0, "Primary file missing: %s, using remote copy: %s", primaryIFile->queryFilename(), locationName.str());
                    activity->fireException(e);
                    ifile.set(backupIFile);
                    location = l;
                    path.append(locationName);
                }
                return true;
            }
        }
        catch (IException *e)
        {
            Owned<IThorException> e2 = MakeActivityException(activity, e);
            e->Release();
            throw e2.getClear();
        }
    }
    return false;
}

class CEnsurePrimaryPartFile : public CInterface, implements IActivityReplicatedFile
{
    Linked<IPartDescriptor> partDesc;
    StringAttr logicalFilename;
    Owned<IReplicatedFile> part;
public:
    IMPLEMENT_IINTERFACE;

    CEnsurePrimaryPartFile(const char *_logicalFilename, IPartDescriptor *_partDesc)
        : logicalFilename(_logicalFilename), partDesc(_partDesc)
    {
    }
    virtual IFile *open(CActivityBase &activity) override
    {
        unsigned location;
        OwnedIFile iFile;
        StringBuffer filePath;
        if (globals->getPropBool("@autoCopyBackup", !isContainerized())?ensurePrimary(&activity, *partDesc, iFile, location, filePath):getBestFilePart(&activity, *partDesc, iFile, location, filePath, &activity))
            return iFile.getClear();
        else
        {
            StringBuffer locations;
            IException *e = MakeActivityException(&activity, TE_FileNotFound, "No physical file part for logical file %s, found at given locations: %s (Error = %d)", logicalFilename.get(), getFilePartLocations(*partDesc, locations).str(), GetLastError());
            IERRLOG(e);
            throw e;
        }
    }
    virtual IFile *open() override { throwUnexpected(); }
    RemoteFilenameArray &queryCopies() 
    { 
        if(!part.get()) 
            part.setown(partDesc->getReplicatedFile());
        return part->queryCopies(); 
    }
};

IActivityReplicatedFile *createEnsurePrimaryPartFile(const char *logicalFilename, IPartDescriptor *partDesc)
{
    return new CEnsurePrimaryPartFile(logicalFilename, partDesc);
}

///////////////

class CFileCache;
class CLazyFileIO : public CInterfaceOf<IFileIO>
{
    typedef CInterfaceOf<IFileIO> PARENT;

    CFileCache &cache;
    Owned<IActivityReplicatedFile> repFile;
    Linked<IExpander> expander;
    bool compressed;
    CRuntimeStatisticCollection fileStats;
    CriticalSection crit;
    Owned<IFileIO> iFileIO; // real IFileIO
    CActivityBase *activity = nullptr;
    StringAttr filename, id;

    IFileIO *getFileIO()
    {
        CriticalBlock b(crit);
        return iFileIO.getLink();
    }
    IFileIO *getClearFileIO()
    {
        CriticalBlock b(crit);
        return iFileIO.getClear();
    }
public:
    CLazyFileIO(CFileCache &_cache, const char *_filename, const char *_id, IActivityReplicatedFile *_repFile, bool _compressed, IExpander *_expander, const StatisticsMapping & _statMapping)
        : cache(_cache), filename(_filename), id(_id), repFile(_repFile), compressed(_compressed), expander(_expander),
          fileStats(_statMapping)
    {
    }
    virtual void beforeDispose() override;
    void setActivity(CActivityBase *_activity)
    {
        activity = _activity;
    }
    IFileIO *getOpenFileIO(CActivityBase &activity);
    const char *queryFindString() const { return id; } // for string HT
// IFileIO impl.
    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        Owned<IFileIO> iFileIO = getOpenFileIO(*activity);
        return iFileIO->read(pos, len, data);
    }
    virtual offset_t size() override
    {
        Owned<IFileIO> iFileIO = getOpenFileIO(*activity);
        return iFileIO->size();
    }
    virtual void close() override
    {
        /* NB: clears CLazyFileIO's ownership of the underlying IFileIO, there will be disposed on exit of this function if no other references,
         * and as a result will close the underlying file handle or remote connection.
         * There can be concurrent threads and theoretically other threads could be in some of the other CLazyFileIO methods and still have
         * references to the underlying iFileIO, in which case the last thread referencing it will release, dispose and close handle.
         * But given that we are in probably here via purgeOldest(), then it is very unlikely that there is an active read at this time.
         */
        Owned<IFileIO> openiFileIO = getClearFileIO();
        if (openiFileIO)
            mergeStats(fileStats, openiFileIO);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        switch (kind)
        {
        case StTimeDiskReadIO:
            return cycle_to_nanosec(getStatistic(StCycleDiskReadIOCycles));
        case StTimeDiskWriteIO:
            return cycle_to_nanosec(getStatistic(StCycleDiskWriteIOCycles));
        default:
            break;
        }

        Owned<IFileIO> openiFileIO = getFileIO();
        unsigned __int64 openValue = openiFileIO ? openiFileIO->getStatistic(kind) : 0;
        return openValue + fileStats.getStatisticValue(kind);
    }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        throwUnexpectedX("CDelayedFileWrapper::write() called for a cached IFileIO object");
    }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override
    {
        throwUnexpectedX("CDelayedFileWrapper::appendFile() called for a cached IFileIO object");
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpectedX("CDelayedFileWrapper::setSize() called for a cached IFileIO object");
    }
    virtual void flush() override
    {
        throwUnexpectedX("CDelayedFileWrapper::flush() called for a cached IFileIO object");
    }
};

class CFileCache : public CSimpleInterfaceOf<IThorFileCache>
{
    StringSuperHashTableOf<CLazyFileIO> files; // NB: table doesn't own entries, entries remove themselves on destruction.
    ICopyArrayOf<CLazyFileIO> openFiles;
    unsigned limit, purgeN;
    CriticalSection crit;

    void purgeOldest()
    {
        // NB: called in crit
        // will be ordered oldest first.
        dbgassertex(purgeN >= openFiles.ordinality()); // purgeOldest() should not be called unless >= limit, and purgeN always >= limit.
        for (unsigned i=0; i<purgeN; i++)
            openFiles.item(i).close();
        openFiles.removen(0, purgeN);
    }
    bool _remove(CLazyFileIO *lFile)
    {
        bool ret = files.removeExact(lFile);
        if (!ret) return false;
        openFiles.zap(*lFile);
        return true;
    }
    bool _remove(const char *id)
    {
        CLazyFileIO *lFile = files.find(id);
        if (!lFile) return false;
        bool ret = files.removeExact(lFile);
        if (!ret) return false;
        openFiles.zap(*lFile);
        return true;
    }
public:
    CFileCache(unsigned _limit) : limit(_limit)
    {
        assertex(limit);
        purgeN = globals->getPropInt("@fileCachePurgeN", 10);
        if (purgeN > limit) purgeN=limit; // why would it be, but JIC.
        DBGLOG("FileCache: limit = %d, purgeN = %d", limit, purgeN);
    }
    void opening(CLazyFileIO &lFile)
    {
        CriticalBlock b(crit);
        if (openFiles.ordinality() >= limit)
        {
            purgeOldest(); // will close purgeN
            assertex(openFiles.ordinality() < limit);
        }
        // NB: moves to end if already in openFiles, meaning head of openFiles are oldest used
        openFiles.zap(lFile);
        openFiles.append(lFile);
    }
    bool remove(const char *id)
    {
        CriticalBlock b(crit);
        return _remove(id);
    }
// IThorFileCache impl.
    virtual bool remove(const char *filename, unsigned crc) override
    {
        StringBuffer id(filename);
        if (crc)
            id.append(crc);
        CriticalBlock b(crit);
        return _remove(id);
    }
    virtual IFileIO *lookupIFileIO(CActivityBase &activity, const char *logicalFilename, IPartDescriptor &partDesc, IExpander *expander, const StatisticsMapping & _statMapping) override
    {
        StringBuffer filename;
        RemoteFilename rfn;
        partDesc.getFilename(0, rfn);
        rfn.getPath(filename);
        StringBuffer id(filename);
        unsigned crc = partDesc.queryProperties().getPropInt("@fileCrc");
        if (crc)
            id.append(crc);
        CriticalBlock b(crit);
        CLazyFileIO * file = files.find(id);
        if (!file || !file->isAliveAndLink())
        {
            Owned<IActivityReplicatedFile> repFile = createEnsurePrimaryPartFile(logicalFilename, &partDesc);
            bool compressed = partDesc.queryOwner().isCompressed();
            file = new CLazyFileIO(*this, filename, id, repFile.getClear(), compressed, expander, _statMapping);
            files.replace(* file); // NB: files does not own 'file', CLazyFileIO will remove itself from cache on destruction

            /* NB: there will be 1 CLazyFileIO per physical file part name
             * They will be linked by multiple lookups
             *
             * When the file cache hits the limit, it will calll CLazyFileIO.close()
             * This does not actually close the file, but releases CLazyFileIO's underlying real IFileIO
             * Each active CLazyFileIO file op. has a link to the underlying IFileIO.
             * Meaning, that only when there are no active ops. and close() has exited, is the underlying
             * real IFileIO actually freed and the file handle closed.
             */
        }
        file->setActivity(&activity); // an activity needed by IActivityReplicatedFile, mainly for logging purposes.
        return file;
    }
friend class CLazyFileIO;
};

void CLazyFileIO::beforeDispose()
{
    {
        CriticalBlock block(cache.crit);
        cache._remove(this);
    }
    PARENT::beforeDispose();
}

IFileIO *CLazyFileIO::getOpenFileIO(CActivityBase &activity)
{
    CriticalBlock b(crit);
    if (!iFileIO)
    {
        cache.opening(*this);
        Owned<IFile> iFile = repFile->open(activity);
        if (NULL != expander.get())
            iFileIO.setown(createCompressedFileReader(iFile, expander));
        else if (compressed)
            iFileIO.setown(createCompressedFileReader(iFile));
        else
            iFileIO.setown(iFile->open(IFOread));
        if (!iFileIO.get())
            throw MakeThorException(0, "CLazyFileIO: failed to open: %s", filename.get());
    }
    return iFileIO.getLink();
}

IThorFileCache *createFileCache(unsigned limit)
{
    return new CFileCache(limit);
}

IDelayedFile *createDelayedFile(IFileIO *iFileIO)
{
    /* NB: all this serves to do, is to create IDelayedFile shell
     * It does not implement the delay itself, the CLazyFileIO it links to does that.
     * However a IDelayedFile is expected/used by some jhtree mechanism, as a further
     * delayed route before invoking key loads.
     */

    class CDelayedFileWrapper : public CSimpleInterfaceOf<IDelayedFile>
    {
        Linked<IFileIO> lFile;

    public:
        CDelayedFileWrapper(IFileIO *_lFile) : lFile(_lFile) { }
        ~CDelayedFileWrapper()
        {
        }
        // IDelayedFile impl.
        virtual IMemoryMappedFile *getMappedFile() override { return nullptr; }
        virtual IFileIO *getFileIO() override
        {
            return lFile.getLink();
        }
    };

    // NB: CLazyFileIO can't implement IDelayedFile, purely because it's method would cause circular links
    return new CDelayedFileWrapper(iFileIO);
}

/*
 * strand stuff
 */

IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, Owned<IStrandJunction> &junction, bool consumerOrdered)
{
    if (input)
    {
        PointerArrayOf<IEngineRowStream> instreams;
        junction.setown(input->getOutputStreams(activity, idx, instreams, nullptr, consumerOrdered, nullptr));
        if (instreams.length() != 1)
        {
            assertex(instreams.length());
            if (!junction)
                junction.setown(createStrandJunction(*activity.queryRowManager(), instreams.length(), 1, activity.getOptInt("strandBlockSize"), false));
            ForEachItemIn(stream, instreams)
            {
                junction->setInput(stream, instreams.item(stream));
            }
            return junction->queryOutput(0);
        }
        else
            return instreams.item(0);
    }
    else
        return nullptr;
}

IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, bool consumerOrdered)
{
    Owned<IStrandJunction> junction;
    IEngineRowStream * result = connectSingleStream(activity, input, idx, junction, consumerOrdered);
    assertex(!junction);
    return result;
}



