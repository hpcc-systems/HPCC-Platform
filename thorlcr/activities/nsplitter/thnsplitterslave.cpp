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
    unsigned __int64 totalCycles;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CSplitterOutputBase() { totalCycles = 0; }

    virtual void start() = 0;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual unsigned __int64 queryTotalCycles() const { return totalCycles; }
};

class CSplitterOutput : public CSplitterOutputBase
{
    NSplitterSlaveActivity &activity;

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

class NSplitterSlaveActivity : public CSlaveActivity
{
    bool spill;
    bool eofHit;
    bool inputsConfigured;
    CriticalSection startLock;
    unsigned nstopped;
    rowcount_t recsReady;
    IThorDataLink *input;
    bool grouped;
    Owned<IException> startException, writeAheadException;
    Owned<ISharedSmartBuffer> smartBuf;

    class CWriter : public CSimpleInterface, IThreaded
    {
        NSplitterSlaveActivity &parent;
        CThreadedPersistent threaded;
        Semaphore sem;
        bool stopped;
        rowcount_t writerMax;
        unsigned requestN;
        SpinLock recLock;

    public:
        CWriter(NSplitterSlaveActivity &_parent) : parent(_parent), threaded("CWriter", this)
        {
            stopped = true;
            writerMax = 0;
            requestN = 1000;
        }
        ~CWriter() { stop(); }
        virtual void main()
        {
            loop
            {
                sem.wait();
                if (stopped) break;
                rowcount_t request;
                {
                    SpinBlock block(recLock);
                    request = writerMax;
                }
                parent.writeahead(request, stopped);
                if (parent.eofHit)
                    break;
            }
        }
        void start()
        {
            stopped = false;
            writerMax = 0;
            threaded.start();
        }
        virtual void stop()
        {
            if (!stopped)
            {
                stopped = true;
                sem.signal();
                threaded.join();
            }
        }
        void more(rowcount_t &max) // called if output hit it's max, returns new max
        {
            if (stopped) return;
            SpinBlock block(recLock);
            if (writerMax <= max)
            {
                writerMax += requestN;
                sem.signal();
            }
            max = writerMax;
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
            ActivityTimer t(totalCycles, activity.queryTimeActivities(), NULL);
            return input->nextRow();
        }
        virtual void stop() { input->stop(); }
        virtual void start()
        {
            ActivityTimer s(totalCycles, activity.queryTimeActivities(), NULL);
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
            dataLinkStart("SPLITTEROUTPUT", activity->queryContainer().queryId(), id);
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
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    NSplitterSlaveActivity(CGraphElementBase *container) 
        : CSlaveActivity(container), writer(*this)
    {
        spill = false;
        input = NULL;
        nstopped = 0;
        eofHit = inputsConfigured = false;
        recsReady = 0;
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
            ForEachItemIn(o2, outputs)
            {
                CDelayedInput *delayedInput = (CDelayedInput *)outputs.item(o2);
                if (o2 == o)
                    delayedInput->setInput(new CInputWrapper(*this, inputs.item(0)));
                else
                    delayedInput->setInput(new CNullInput());
            }
        }
        else
        {
            ForEachItemIn(o, outputs)
            {
                CDelayedInput *delayedInput = (CDelayedInput *)outputs.item(o);
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
        if (inputsConfigured)
        {
            // ensure old inputs cleared, to avoid being reused before re-setup on subsequent executions
            ForEachItemIn(o, outputs)
            {
                CDelayedInput *delayedInput = (CDelayedInput *)outputs.item(o);
                delayedInput->setInput(NULL);
            }
            inputsConfigured = false;
        }
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        ForEachItemIn(o, container.outputs)
            appendOutput(new CDelayedInput(*this));
        IHThorSplitArg *helper = (IHThorSplitArg *)queryHelper();
        int dV = getOptInt(THOROPT_SPLITTER_SPILL, -1);
        if (-1 == dV)
        {
            spill = !helper->isBalanced();
            bool forcedUnbalanced = queryContainer().queryXGMML().getPropBool("@unbalanced", false);
            if (!spill && forcedUnbalanced)
            {
                ActPrintLog("Was marked balanced, but forced unbalanced due to UPDATE changes.");
                spill = true;
            }
        }
        else
            spill = dV>0;
    }
    inline void more(rowcount_t &max)
    {
        ActivityTimer t(totalCycles, queryTimeActivities(), NULL);
        writer.more(max);
    }
    void prepareInput(unsigned output)
    {
        CriticalBlock block(startLock);
        if (!input)
        {
            input = inputs.item(0);
            try {
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
                        CDelayedInput *delayedInput = (CDelayedInput *)outputs.item(o);
                        if (NULL == container.connectedOutputs.queryItem(o))
                            smartBuf->queryOutput(o)->stop();
                    }
                }
                writer.start();
            }
            catch (IException *e) { 
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
    void writeahead(const rowcount_t &requested, const bool &stopped)
    {
        if (eofHit)
            return;
        
        OwnedConstThorRow row;
        loop
        {
            if (abortSoon || stopped || requested<recsReady)
                break;
            try
            {
                row.setown(input->nextRow());
                if (!row)
                {
                    row.setown(input->nextRow());
                    if (row)
                    {
                        ++recsReady;
                        smartBuf->putRow(NULL);
                    }
                }
            }
            catch (IException *e) { writeAheadException.setown(e); }
            if (!row || writeAheadException.get())
            {
                ActPrintLog("Splitter activity, hit end of input @ rec = %"RCPF"d", recsReady);
                eofHit = true;
                smartBuf->flush(); // signals no more rows will be written.
                break;
            }
            ++recsReady;
            smartBuf->putRow(row.getClear()); // can block if mem limited, but other readers can progress which is the point
        }
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
        unsigned __int64 _totalCycles = totalCycles; // more() time
        ForEachItemIn(o, outputs)
        {
            CDelayedInput *delayedInput = (CDelayedInput *)outputs.item(o);
            _totalCycles += delayedInput->queryTotalCycles();
        }
        return _totalCycles;
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
    ActivityTimer s(totalCycles, activity.queryTimeActivities(), NULL);
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
        activity.more(max);
    ActivityTimer t(totalCycles, activity.queryTimeActivities(), NULL);
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

