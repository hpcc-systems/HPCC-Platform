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

class CSplitterOutputBase : public CSimpleInterface, implements IRowStream
{
protected:
    ActivityTimeAccumulator totalCycles;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    virtual void start() = 0;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual unsigned __int64 queryTotalCycles() const { return totalCycles.totalCycles; }
};

class CSplitterOutput : public CSplitterOutputBase
{
    NSplitterSlaveActivity &activity;
    Semaphore writeBlockSem;

    unsigned output;
    rowcount_t rec, max;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned output);

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info);

    virtual void start();
    virtual void stop();
    virtual const void *nextRow();
};


//
// NSplitterSlaveActivity
//

class NSplitterSlaveActivity : public CSlaveActivity, implements ISharedSmartBufferCallback
{
    bool spill;
    bool eofHit;
    bool inputsConfigured;
    bool writeBlocked, pagedOut;
    CriticalSection startLock, writeAheadCrit;
    PointerArrayOf<Semaphore> stalledWriters;
    unsigned nstopped;
    rowcount_t recsReady;
    IThorDataLink *input;
    bool grouped;
    Owned<IException> startException, writeAheadException;
    Owned<ISharedSmartBuffer> smartBuf;

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
    class CNullInput : public CSplitterOutputBase
    {
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        virtual const void *nextRow() { throwUnexpected(); return NULL; }
        virtual void stop() { throwUnexpected(); }
        virtual void start() { throwUnexpected(); }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
        {
            ::initMetaInfo(info);
        }
    };
    class CInputWrapper : public CSplitterOutputBase
    {
        IThorDataLink *input;
        NSplitterSlaveActivity &activity;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CInputWrapper(NSplitterSlaveActivity &_activity, IThorDataLink *_input) : activity(_activity), input(_input) { }
        virtual const void *nextRow()
        {
            ActivityTimer t(totalCycles, activity.queryTimeActivities());
            return input->nextRow();
        }
        virtual void stop() { input->stop(); }
        virtual void start()
        {
            ActivityTimer s(totalCycles, activity.queryTimeActivities());
            input->start();
        }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
        {
            input->getMetaInfo(info);
        }
    };

    class CDelayedInput : public CSimpleInterface, public CThorDataLink
    {
        Owned<CSplitterOutputBase> input;
        Linked<NSplitterSlaveActivity> activity;
        mutable SpinLock processActiveLock;

        unsigned id;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDelayedInput(NSplitterSlaveActivity &_activity) : CThorDataLink(&_activity), activity(&_activity), id(0) { }
        void setInput(CSplitterOutputBase *_input, unsigned _id=0)
        {
            SpinBlock b(processActiveLock);
            input.setown(_input);
            id = _id;
        }
        virtual const void *nextRow()
        {
            OwnedConstThorRow row = input->nextRow();
            if (row)
                dataLinkIncrement();
            return row.getClear();
        }
        virtual void stop()
        {
            input->stop();
            dataLinkStop();
        }
    // IThorDataLink impl.
        virtual void start()
        {
            activity->ensureInputsConfigured();
            input->start();
            dataLinkStart(id);
        }
        virtual bool isGrouped() { return activity->inputs.item(0)->isGrouped(); }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
        {
            initMetaInfo(info);
            if (input)
                input->getMetaInfo(info);
            else
                activity->inputs.item(0)->getMetaInfo(info);
            info.canStall = !activity->spill;
        }
        virtual unsigned __int64 queryTotalCycles() const
        {
            SpinBlock b(processActiveLock);
            if (!input)
                return 0;
            return input->queryTotalCycles();
        }
    };

    IPointerArrayOf<CDelayedInput> delayInputsList;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    NSplitterSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), writer(*this)
    {
        spill = false;
        input = NULL;
        nstopped = 0;
        eofHit = inputsConfigured = writeBlocked = pagedOut = false;
        recsReady = 0;
    }
    virtual ~NSplitterSlaveActivity()
    {
        delayInputsList.kill();
    }
    void ensureInputsConfigured()
    {
        CriticalBlock block(startLock);
        if (inputsConfigured)
            return;
        inputsConfigured = true;
        unsigned noutputs = container.connectedOutputs.getCount();
        ActPrintLog("Number of connected outputs: %d", noutputs);
        if (1 == noutputs)
        {
            CIOConnection *io = NULL;
            ForEachItemIn(o, container.connectedOutputs)
            {
                io = container.connectedOutputs.item(o);
                if (io)
                    break;
            }
            assertex(io);
            ForEachItemIn(o2, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o2);
                if (o2 == o)
                    delayedInput->setInput(new CInputWrapper(*this, inputs.item(0)));
                else
                    delayedInput->setInput(new CNullInput());
            }
        }
        else
        {
            ForEachItemIn(o, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o);
                if (NULL != container.connectedOutputs.queryItem(o))
                    delayedInput->setInput(new CSplitterOutput(*this, o), o);
                else
                    delayedInput->setInput(new CNullInput());
            }
        }
    }
    void reset()
    {
        CSlaveActivity::reset();
        nstopped = 0;
        grouped = false;
        eofHit = false;
        recsReady = 0;
        writeBlocked = false;
        stalledWriters.kill();
        if (inputsConfigured)
        {
            // ensure old inputs cleared, to avoid being reused before re-setup on subsequent executions
            ForEachItemIn(o, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o);
                delayedInput->setInput(NULL);
            }
            inputsConfigured = false;
        }
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        ForEachItemIn(o, container.outputs)
        {
            Owned<CDelayedInput> delayedInput = new CDelayedInput(*this);
            delayInputsList.append(delayedInput.getLink());
            appendOutput(delayedInput.getClear());
        }
        IHThorSplitArg *helper = (IHThorSplitArg *)queryHelper();
        int dV = getOptInt(THOROPT_SPLITTER_SPILL, -1);
        if (-1 == dV)
            spill = !helper->isBalanced();
        else
            spill = dV>0;
    }
    void prepareInput(unsigned output)
    {
        CriticalBlock block(startLock);
        if (!input)
        {
            input = inputs.item(0);
            try
            {
                startInput(input);
                grouped = input->isGrouped();
                nstopped = container.connectedOutputs.getCount();
                if (smartBuf)
                    smartBuf->reset();
                else
                {
                    if (spill)
                    {
                        StringBuffer tempname;
                        GetTempName(tempname,"nsplit",true); // use alt temp dir
                        smartBuf.setown(createSharedSmartDiskBuffer(this, tempname.str(), outputs.ordinality(), queryRowInterfaces(input), &container.queryJob().queryIDiskUsage()));
                        ActPrintLog("Using temp spill file: %s", tempname.str());
                    }
                    else
                    {
                        ActPrintLog("Spill is 'balanced'");
                        smartBuf.setown(createSharedSmartMemBuffer(this, outputs.ordinality(), queryRowInterfaces(input), NSPLITTER_SPILL_BUFFER_SIZE));
                    }
                    // mark any unconnected outputs of smartBuf as already stopped.
                    ForEachItemIn(o, outputs)
                    {
                        IThorDataLink *delayedInput = outputs.item(o);
                        if (NULL == container.connectedOutputs.queryItem(o))
                            smartBuf->queryOutput(o)->stop();
                    }
                }
                if (!spill)
                    writer.start(); // writer keeps writing ahead as much as possible, the readahead impl. will block when has too much
            }
            catch (IException *e)
            {
                startException.setown(e); 
            }
        }
    }
    inline const void *nextRow(unsigned output)
    {
        OwnedConstThorRow row = smartBuf->queryOutput(output)->nextRow(); // will block until available
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
        pagedOut = false;
        OwnedConstThorRow row;
        loop
        {
            if (abortSoon || stopped || pagedOut)
                break;
            try
            {
                row.setown(input->nextRow());
                if (!row)
                {
                    row.setown(input->nextRow());
                    if (row)
                    {
                        smartBuf->putRow(NULL, this); // may call blocked() (see ISharedSmartBufferCallback impl. below)
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
    void inputStopped()
    {
        if (nstopped && --nstopped==0) 
        {
            writer.stop();
            stopInput(input);
            input = NULL;
        }
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (smartBuf)
            smartBuf->cancel();
    }
    unsigned __int64 queryTotalCycles() const
    {
        unsigned __int64 _totalCycles = totalCycles.totalCycles; // more() time
        ForEachItemIn(o, outputs)
        {
            IThorDataLink *delayedInput = outputs.item(o);
            _totalCycles += delayedInput->queryTotalCycles();
        }
        return _totalCycles;
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
friend class CInputWrapper;
friend class CSplitterOutput;
friend class CWriter;
};

//
// CSplitterOutput
//
CSplitterOutput::CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned _output)
   : activity(_activity), output(_output)
{
    rec = max = 0;
}

// IThorDataLink
void CSplitterOutput::start()
{
    ActivityTimer s(totalCycles, activity.queryTimeActivities());
    rec = max = 0;
    activity.prepareInput(output);
    if (activity.startException)
        throw LINK(activity.startException);
}

void CSplitterOutput::stop()
{ 
    CriticalBlock block(activity.startLock);
    activity.smartBuf->queryOutput(output)->stop();
    activity.inputStopped();
}

const void *CSplitterOutput::nextRow()
{
    if (rec == max)
        max = activity.writeahead(max, activity.queryAbortSoon(), writeBlockSem);
    ActivityTimer t(totalCycles, activity.queryTimeActivities());
    const void *row = activity.nextRow(output); // pass ptr to max if need more
    ++rec;
    return row;
}


void CSplitterOutput::getMetaInfo(ThorDataLinkMetaInfo &info)
{
    CThorDataLink::calcMetaInfoSize(info, activity.inputs.item(0));
}

CActivityBase *createNSplitterSlave(CGraphElementBase *container)
{
    return new NSplitterSlaveActivity(container);
}

