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

#include "thnsplitterslave.ipp"

#include "thbuf.hpp"
#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"

interface ISharedSmartBuffer;
class NSplitterSlaveActivity;

class CSplitterOutput : public CSimpleInterfaceOf<IStartableEngineRowStream>, public CEdgeProgress, public COutputTiming, implements IThorDataLink
{
    NSplitterSlaveActivity &activity;
    Semaphore writeBlockSem;
    bool started = false, stopped = false;

    unsigned outIdx;
    rowcount_t rec = 0, max = 0;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterfaceOf<IStartableEngineRowStream>);

    CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned outIdx);

    void reset()
    {
        started = stopped = false;
        rec = max = 0;
    }
    inline bool isStopped() const { return stopped; }

// IThorDataLink impl.
    virtual CSlaveActivity *queryFromActivity() override;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override;
    virtual void dataLinkSerialize(MemoryBuffer &mb) const override { CEdgeProgress::dataLinkSerialize(mb); }
    virtual rowcount_t getProgressCount() const override { return CEdgeProgress::getCount(); }
    virtual bool isGrouped() const override;
    virtual IOutputMetaData * queryOutputMeta() const override;
    virtual bool isInputOrdered(bool consumerOrdered) const override;
    virtual void setOutputStream(unsigned index, IEngineRowStream *stream) override;
    virtual IStrandJunction *getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) override;
    virtual unsigned __int64 queryTotalCycles() const override { return COutputTiming::queryTotalCycles(); }
    virtual unsigned __int64 queryEndCycles() const { return COutputTiming::queryEndCycles(); }
    virtual void debugRequest(MemoryBuffer &mb) override;
// Stepping methods
    virtual IInputSteppingMeta *querySteppingMeta() { return nullptr; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }

// IStartableEngineRowStream
    virtual void start() override;
    virtual void stop() override;
    virtual const void *nextRow() override;
    virtual void resetEOF() { throwUnexpected(); }
};


//
// NSplitterSlaveActivity
//

class NSplitterSlaveActivity : public CSlaveActivity, implements ISharedSmartBufferCallback
{
    typedef CSlaveActivity PARENT;

    bool spill = false;
    bool eofHit = false;
    bool writeBlocked = false, pagedOut = false;
    CriticalSection startLock, writeAheadCrit;
    PointerArrayOf<Semaphore> stalledWriters;
    unsigned stoppedOutputs = 0;
    unsigned activeOutputs = 0;
    rowcount_t recsReady = 0;
    Owned<IException> writeAheadException;
    Owned<ISharedSmartBuffer> smartBuf;
    bool inputPrepared = false;
    bool inputConnected = false;
    unsigned remainingOutputs = 0;

    // NB: CWriter only used by 'balanced' splitter, which blocks write when too far ahead
    class CWriter : public CSimpleInterface, IThreaded
    {
        NSplitterSlaveActivity &parent;
        CThreadedPersistent threaded;
        bool stopped;
        rowcount_t current;

    public:
        CWriter(NSplitterSlaveActivity &_parent) : parent(_parent), threaded("CWriter", this)
        {
            current = 0;
            stopped = true;
        }
        ~CWriter() { stop(); }
        virtual void main()
        {
            Semaphore writeBlockSem;
            while (!stopped && !parent.eofHit)
                current = parent.writeahead(current, stopped, writeBlockSem);
        }
        void start()
        {
            stopped = false;
            threaded.start();
        }
        virtual void stop()
        {
            if (!stopped)
            {
                stopped = true;
                threaded.join();
            }
        }
    } writer;
    void connectInput(bool consumerOrdered)
    {
        CriticalBlock block(startLock);
        bool inputOrdered = isInputOrdered(consumerOrdered);
        if (!inputConnected)
        {
            inputConnected = true;
            connectInputStreams(inputOrdered);
        }
    }
public:
    NSplitterSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), writer(*this)
    {
        activeOutputs = container.getOutputs();
        ActPrintLog("Number of connected outputs: %u", activeOutputs);
        ForEachItemIn(o, container.outputs)
            appendOutput(new CSplitterOutput(*this, o));
    }
    virtual void reset() override
    {
        PARENT::reset();
        stoppedOutputs = 0;
        eofHit = false;
        inputPrepared = false;
        recsReady = 0;
        writeBlocked = false;
        stalledWriters.kill();
        ForEachItemIn(o, outputs)
        {
            CSplitterOutput *output = (CSplitterOutput *)outputs.item(o);
            if (output)
                output->reset();
        }
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        IHThorSplitArg *helper = (IHThorSplitArg *)queryHelper();
        int dV = getOptInt(THOROPT_SPLITTER_SPILL, -1);
        if (-1 == dV)
            spill = !helper->isBalanced();
        else
            spill = dV>0;
    }
    bool prepareInput()
    {
        CriticalBlock block(startLock);
        if (!inputPrepared)
        {
            inputPrepared = true;
            PARENT::start();
            remainingOutputs = activeOutputs;
            ForEachItemIn(o, outputs)
            {
                CSplitterOutput *output = (CSplitterOutput *)outputs.item(o);
                if (output && output->isStopped())
                    --remainingOutputs;
            }
            assertex(remainingOutputs); // must be >=1, as this output (outIdx) has invoked prepareInput
            if (1 == remainingOutputs)
                return false;
            if (smartBuf)
                smartBuf->reset();
            else
            {
                if (spill)
                {
                    StringBuffer tempname;
                    GetTempName(tempname, "nsplit", true); // use alt temp dir
                    smartBuf.setown(createSharedSmartDiskBuffer(this, tempname.str(), activeOutputs, queryRowInterfaces(input), &container.queryJob().queryIDiskUsage()));
                    ActPrintLog("Using temp spill file: %s", tempname.str());
                }
                else
                {
                    ActPrintLog("Spill is 'balanced'");
                    smartBuf.setown(createSharedSmartMemBuffer(this, activeOutputs, queryRowInterfaces(input), NSPLITTER_SPILL_BUFFER_SIZE));
                }
                // mark any outputs already stopped
                ForEachItemIn(o, outputs)
                {
                    CSplitterOutput *output = (CSplitterOutput *)outputs.item(o);
                    if (output && output->isStopped())
                        smartBuf->queryOutput(o)->stop();
                }
            }
            if (!spill)
                writer.start(); // writer keeps writing ahead as much as possible, the readahead impl. will block when has too much
        }
        return true;
    }
    inline const void *nextRow(unsigned outIdx)
    {
        if (1 == remainingOutputs) // will be true, if only 1 input connect, or only 1 input was active (others stopped) when it started reading
            return inputStream->nextRow();
        OwnedConstThorRow row = smartBuf->queryOutput(outIdx)->nextRow(); // will block until available
        if (writeAheadException)
            throw LINK(writeAheadException);
        return row.getClear();
    }
    rowcount_t writeahead(rowcount_t current, const bool &stopped, Semaphore &writeBlockSem)
    {
        // NB: readers call writeahead, which will block others
        CriticalBlock b(writeAheadCrit);
        loop
        {
            if (eofHit)
                return recsReady;
            if (current < recsReady)
                return recsReady;
            else if (writeBlocked) // NB: only used by 'balanced' splitter, which blocks write when too far ahead
            {
                stalledWriters.append(&writeBlockSem);
                CriticalUnblock ub(writeAheadCrit);
                writeBlockSem.wait(); // when active writer unblocks, signals all stalledWriters
                // recsReady or eofHit will have been updated by the blocking thread by now, loop and re-check
            }
            else
                break;
        }
        ActivityTimer t(totalCycles, queryTimeActivities());
        if (!prepareInput()) // returns true, if
        {
            return RCMAX; // signals to requester that you are the only output
        }
        pagedOut = false;
        OwnedConstThorRow row;
        loop
        {
            if (abortSoon || stopped || pagedOut)
                break;
            try
            {
                row.setown(inputStream->nextRow());
                if (!row)
                {
                    row.setown(inputStream->nextRow());
                    if (row)
                    {
                        smartBuf->putRow(nullptr, this); // may call blocked() (see ISharedSmartBufferCallback impl. below)
                        ++recsReady;
                    }
                }
            }
            catch (IException *e) { writeAheadException.setown(e); }
            if (!row || writeAheadException.get())
            {
                ActPrintLog("Splitter activity, hit end of input @ rec = %" RCPF "d", recsReady);
                eofHit = true;
                smartBuf->flush(); // signals no more rows will be written.
                break;
            }
            smartBuf->putRow(row.getClear(), this); // can block if mem limited, but other readers can progress which is the point
            ++recsReady;
        }
        return recsReady;
    }
    void inputStopped(unsigned outIdx)
    {
        CriticalBlock block(startLock);
        if (smartBuf)
        {
            /* If no output has started reading (nextRow()), then it will not have been prepared
             * If only 1 output is left, it will bypass the smart buffer when it starts.
             */
            smartBuf->queryOutput(outIdx)->stop();
        }
        ++stoppedOutputs;
        if (stoppedOutputs == activeOutputs)
        {
            writer.stop();
            PARENT::stop();
            inputPrepared = false;
        }
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (smartBuf)
            smartBuf->cancel();
    }
// ISharedSmartBufferCallback impl.
    virtual void paged() { pagedOut = true; }
    virtual void blocked()
    {
        writeBlocked = true; // Prevent other users getting beyond checking recsReady in writeahead()
        writeAheadCrit.leave();
    }
    virtual void unblocked()
    {
        writeAheadCrit.enter();
        writeBlocked = false;
        if (stalledWriters.ordinality())
        {
            ForEachItemInRev(s, stalledWriters)
                stalledWriters.popGet()->signal();
        }
    }

// IEngineRowStream
    virtual const void *nextRow() override
    {
        ActivityTimer t(totalCycles, queryTimeActivities());
        return inputStream->nextRow();
    }
    virtual void stop() override{ inputStream->stop(); }

// IThorDataLink (if single output connected)
    virtual IStrandJunction *getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) override
    {
        connectInput(consumerOrdered);
        streams.append(this);
        return nullptr;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        calcMetaInfoSize(info, queryInput(0));
    }

friend class CInputWrapper;
friend class CSplitterOutput;
friend class CWriter;
};

//
// CSplitterOutput
//

CSlaveActivity *CSplitterOutput::queryFromActivity()
{
    return &activity;
}

void CSplitterOutput::getMetaInfo(ThorDataLinkMetaInfo &info)
{
    activity.getMetaInfo(info);
}

bool CSplitterOutput::isGrouped() const
{
    return activity.isGrouped();
}

IOutputMetaData *CSplitterOutput::queryOutputMeta() const
{
    return activity.queryOutputMeta();
}
bool CSplitterOutput::isInputOrdered(bool consumerOrdered) const
{
    return activity.isInputOrdered(consumerOrdered);

}
void CSplitterOutput::setOutputStream(unsigned index, IEngineRowStream *stream)
{
    activity.setOutputStream(index, stream);
}

IStrandJunction *CSplitterOutput::getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks)
{
    activity.connectInput(consumerOrdered);
    streams.append(this);
    return nullptr;
}

void CSplitterOutput::debugRequest(MemoryBuffer &mb)
{
    activity.debugRequest(mb);
}


CSplitterOutput::CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned _outIdx)
   : CEdgeProgress(&_activity, _outIdx), activity(_activity), outIdx(_outIdx)
{
}

// IStartableEngineRowStream
void CSplitterOutput::start()
{
    ActivityTimer s(totalCycles, activity.queryTimeActivities());
    started = true;
    dataLinkStart();
}

// IEngineRowStream
void CSplitterOutput::stop()
{ 
    stopped = true;
    activity.inputStopped(outIdx);
    dataLinkStop();
}

const void *CSplitterOutput::nextRow()
{
    if (rec == max)
    {
        max = activity.writeahead(max, activity.queryAbortSoon(), writeBlockSem);
        // NB: if this is sole input that actually started, writeahead will have returned RCMAX and calls to activity.nextRow will go directly to splitter input
    }
    ActivityTimer t(totalCycles, activity.queryTimeActivities());
    const void *row = activity.nextRow(outIdx); // pass ptr to max if need more
    ++rec;
    if (row)
        dataLinkIncrement();
    return row;
}

CActivityBase *createNSplitterSlave(CGraphElementBase *container)
{
    return new NSplitterSlaveActivity(container);
}

