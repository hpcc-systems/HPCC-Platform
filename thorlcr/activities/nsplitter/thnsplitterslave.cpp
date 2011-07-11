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

#include "thnsplitterslave.ipp"

#include "thbuf.hpp"
#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/nsplitter/thnsplitterslave.cpp $ $Id: thnsplitterslave.cpp 65545 2011-06-17 15:25:30Z jsmith $");

interface ISharedSmartBuffer;
class NSplitterSlaveActivity;

class SplitterOutput : public CSimpleInterface, public CThorDataLink
{
    NSplitterSlaveActivity &activity;
    unsigned __int64 totalCycles;

    unsigned output;
    rowcount_t rec, max;

    unsigned getId() { return output; } 
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SplitterOutput(NSplitterSlaveActivity &_activity, unsigned output);

    void addCycles(unsigned __int64 elapsedCycles);
// IThorDataLink
    virtual void start();
    virtual void stop();
    virtual const void *nextRow();
    bool isGrouped();
    void getMetaInfo(ThorDataLinkMetaInfo &info);
    unsigned __int64 queryTotalCycles() const;
};

#ifdef TIME_ACTIVITIES
struct Timer : public ActivityTimer
{
    SplitterOutput &output;
    unsigned __int64 elapsedCycles;
    inline Timer(SplitterOutput &_output, const bool &enabled) : ActivityTimer(elapsedCycles, enabled, NULL), output(_output) { }
    inline ~Timer()
    {
        if (enabled)
            output.addCycles(elapsedCycles);
    }
};
#else
//optimized away completely?
struct Timer
{
    inline Timer(SplitterOutput &_output, const bool &enabled) { }
};
#endif



//
// NSplitterSlaveActivity
//

class NSplitterSlaveActivity : public CSlaveActivity
{
    bool spill;
    bool eofHit;
    CriticalSection startCrit;
    unsigned nstopped;
    rowcount_t recsReady;
    SpinLock timingLock;

    class CWriter : public CSimpleInterface, IThreaded
    {
        NSplitterSlaveActivity &parent;
        CThreaded threaded;
        Semaphore sem;
        bool stopped;
        rowcount_t writerMax;
        unsigned requestN;
        SpinLock recLock;

    public:
        CWriter(NSplitterSlaveActivity &_parent) : parent(_parent), threaded("CWriter")
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
            threaded.init(this);
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

protected: friend class SplitterOutput;
           friend class CWriter;

    IThorDataLink *input;
    bool grouped;
    Owned<IException> startException, writeAheadException;
    Owned<ISharedSmartBuffer> smartBuf;

    class CNullInput : public CSimpleInterface, public CThorDataLink
    {
        NSplitterSlaveActivity &activity;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CNullInput(NSplitterSlaveActivity &_activity) : CThorDataLink(&_activity), activity(_activity)
        {
        }
        virtual const void *nextRow() { throwUnexpected(); return NULL; }
        virtual void stop() { throwUnexpected(); }
        virtual void start() { throwUnexpected(); }
        virtual bool isGrouped() { return false; }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
        {
            initMetaInfo(info);
            info.totalRowsMin=0;
            info.totalRowsMax=0;
        }
    };
    class CInputWrapper : public CSimpleInterface, public CThorDataLink
    {
        IThorDataLink *input;
        NSplitterSlaveActivity &activity;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CInputWrapper(NSplitterSlaveActivity &_activity) : CThorDataLink(&_activity), activity(_activity) { input = NULL; }
        void setInput(IThorDataLink *_input) 
        {
            if (input) return;
            input = _input;
        }
        virtual const void *nextRow() { return input->nextRow(); }
        virtual void stop() { input->stop(); }
    // IThorDataLink impl.
        virtual void start()
        {
            setInput(activity.inputs.item(0));
            input->start();
        }
        virtual bool isGrouped() { setInput(activity.inputs.item(0)); return input->isGrouped(); }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info) { setInput(activity.inputs.item(0)); input->getMetaInfo(info); }
    };

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    NSplitterSlaveActivity(CGraphElementBase *container) 
        : CSlaveActivity(container), writer(*this)
    {
        spill = false;
        input = NULL;
        nstopped = 0;
        eofHit = false;
        recsReady = 0;
    }
    void reset()
    {
        CSlaveActivity::reset();
        nstopped = 0;
        grouped = false;
        eofHit = false;
        recsReady = 0;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        unsigned numOutputs = container.connectedOutputs.ordinality();
        if (1 == numOutputs)
        {
            unsigned activeOutIdx = container.connectedOutputsIndex.item(0);
            ForEachItemIn(o, container.outputs)
            {
                if (o == activeOutIdx)
                    appendOutput(new CInputWrapper(*this));
                else
                    appendOutput(new CNullInput(*this));
            }
        }
        else
        {
            ForEachItemIn(o, container.outputs)
            {
                if (NotFound != container.connectedOutputsIndex.find(o))
                    appendOutput(new SplitterOutput(*this, o));
                else
                    appendOutput(new CNullInput(*this));
            }

            IHThorSplitArg *helper = (IHThorSplitArg *)queryHelper();
            int dV = (int)container.queryJob().getWorkUnitValueInt("splitterSpills", -1);
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
    }
    inline void more(rowcount_t &max)
    {
        writer.more(max);
    }
    void prepareInput(unsigned output)
    {
        CriticalBlock block(startCrit);
        if (!input)
        {
            input = inputs.item(0);
            try {
                startInput(input);
                grouped = input->isGrouped();
                nstopped = outputs.ordinality();
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
    void doStopInput()
    {
        CriticalBlock block(startCrit);
        if (nstopped)
        {
            nstopped = 0;
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
friend class CInputWrapper;
};

//
// SplitterOutput
//
SplitterOutput::SplitterOutput(NSplitterSlaveActivity &_activity, unsigned _output) 
   : CThorDataLink(&_activity), activity(_activity), output(_output)
{
    rec = max = 0;
    totalCycles = 0;
}

void SplitterOutput::addCycles(unsigned __int64 elapsedCycles)
{
    totalCycles += elapsedCycles; // per output time, inquired by pulling acts.
    SpinBlock b(activity.timingLock);
    activity.getTotalCyclesRef() += elapsedCycles; // Splitter act aggregate time.
}

// IThorDataLink
void SplitterOutput::start() 
{
    Timer s(*this, activity.queryTimeActivities());
    rec = max = 0;
    activity.prepareInput(output);
    if (activity.startException)
        throw LINK(activity.startException);
    dataLinkStart("SPLITTEROUTPUT", activity.queryContainer().queryId(), getId());
}

void SplitterOutput::stop() 
{ 
    CriticalBlock block(activity.startCrit);
    activity.smartBuf->queryOutput(output)->stop();
    activity.inputStopped();
    dataLinkStop();
}

const void *SplitterOutput::nextRow()
{
    Timer t(*this, activity.queryTimeActivities());
    if (rec == max)
        activity.more(max);
    OwnedConstThorRow row = activity.nextRow(output); // pass ptr to max if need more
    if (row)
        dataLinkIncrement();
    ++rec;
    return row.getClear();
}


bool SplitterOutput::isGrouped()
{
    return activity.grouped;
}

void SplitterOutput::getMetaInfo(ThorDataLinkMetaInfo &info)
{
    initMetaInfo(info);
    info.canStall = !activity.spill;
    calcMetaInfoSize(info,activity.inputs.item(0));
}

unsigned __int64 SplitterOutput::queryTotalCycles() const
{
    return totalCycles;
}


CActivityBase *createNSplitterSlave(CGraphElementBase *container)
{
    return new NSplitterSlaveActivity(container);
}

