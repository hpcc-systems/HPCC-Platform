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
#include "jset.hpp"
#include "jqueue.tpp"
#include "commonext.hpp"

#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thalloc.hpp"
#include "eclrtl_imp.hpp"

#include "thfunnelslave.ipp"

class CParallelFunnel : public CSimpleInterface, implements IRowStream
{
    class CInputHandler : public CInterface, implements IThreaded
    {
        CThreadedPersistent threaded;
        CParallelFunnel &funnel;
        Linked<IRowStream> input;
        CriticalSection stopCrit;
        StringAttr idStr;
        unsigned inputIndex;
        rowcount_t readThisInput; // purely for tracing
        bool stopping;
    public:
        CInputHandler(CParallelFunnel &_funnel, IRowStream *_input, unsigned _inputIndex) 
            : threaded("CInputHandler", this), funnel(_funnel), input(_input), inputIndex(_inputIndex)
        {
            readThisInput = 0;
            StringBuffer s(funnel.idStr);
            s.append('(').append(inputIndex).append(')');
            idStr.set(s.str());
            stopping = false;
        }
        ~CInputHandler()
        {
            // stop();      too late to call stop I think
        }
        void start()
        {
            // NB don't start in constructor
            threaded.start();
        }
        void stop()
        {
            CriticalBlock b(stopCrit);
            if (stopping) return;
            stopping = true;
        }
        void join()
        {
            threaded.join();
        }

// IThreaded impl.
        virtual void main()
        {
            bool started = false;
            try
            {
                if (funnel.startInputs)
                {
                    IThorDataLink *_input = QUERYINTERFACE(input.get(), IThorDataLink);
                    _input->start();
                }
                started = true;
                while (!stopping)
                {
                    OwnedConstThorRow row = input->ungroupedNextRow();
                    if (!row) break;

                    {
                        CriticalBlock b(stopCrit);
                        if (stopping) break;
                    }
                    CriticalBlock b(funnel.crit); // will mean first 'push' could block on fullSem, others on this crit.
                    funnel.push(row.getClear());
                    ++readThisInput;
                }
            }
            catch (IException *e)
            {
                funnel.fireException(e);
                e->Release();
            }
            // Informing EOS before stopping, may allow upstream activities to continue, if input slow to stop
            funnel.informEos(inputIndex);
            if (!started)
                return;
            try
            {
                input->stop();
            }
            catch (IException *e)
            {
                funnel.fireException(e);
                e->Release();
            }
            ActPrintLog(&funnel.activity.queryContainer(), "%s: Read %" I64F "d records", idStr.get(), readThisInput);
        }
    };

    CActivityBase &activity;
    CIArrayOf<CInputHandler> inputHandlers;
    bool startInputs;
    Linked<IException> exception;
    unsigned eoss;
    IArrayOf<IRowStream> oinstreams;
    StringAttr idStr;

    CriticalSection fullCrit, crit;
    SimpleInterThreadQueueOf<const void, true> rows;
    Semaphore fullSem;
    size32_t totSize;
    bool full, stopped;
    Linked<IOutputRowSerializer> serializer;

    void push(const void *row)
    {   
        CriticalBlock b2(fullCrit); // exclusivity for totSize / full
        if (stopped) return;
        rows.enqueue(row);
        totSize += thorRowMemoryFootprint(serializer, row);
        while (totSize > FUNNEL_MIN_BUFF_SIZE)
        {
            full = true;
            CriticalUnblock b(fullCrit);
            fullSem.wait(); // block pushers on crit
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CParallelFunnel(CActivityBase &_activity, IRowStream **instreams, unsigned numstreams) : activity(_activity)
    {
        startInputs = false;
        unsigned n = 0;
        while (n<numstreams) oinstreams.append(*LINK(instreams[n++]));
        init();
    }
    CParallelFunnel(CActivityBase &_activity, IThorDataLink **instreams, unsigned numstreams, bool _startInputs) : activity(_activity)
    {
        startInputs = _startInputs;
        unsigned n = 0;
        while (n<numstreams) oinstreams.append(*LINK(instreams[n++]));
        init();
    }
    ~CParallelFunnel()
    {
        loop
        {
            OwnedConstThorRow row = rows.dequeueNow();
            if (!row) break;
        }
    }
    void init()
    {
        idStr.set(activityKindStr(activity.queryContainer().getKind()));

        stopped = full = false;
        totSize = 0;
        eoss = 0;
        unsigned numinputs = oinstreams.ordinality();
        serializer.set(activity.queryRowSerializer());
        ForEachItemIn(i, oinstreams)
            inputHandlers.append(* new CInputHandler(*this, &oinstreams.item(i), i));
        // because of the way eos reported make sure started afterwards
        ForEachItemIn(j, inputHandlers)
            inputHandlers.item(j).start();
    }
    bool fireException(IException *e)
    {
        CriticalBlock b(crit);
        if (!exception.get())
        {
            exception.set(e);

            // nextRow() can be blocked on rows.dequeue(), abort needs to abort the SimpleInterThreadQueueOf
            rows.stop();
        }
        return true;
    }
    void informEos(unsigned input)
    {
        CriticalBlock b(crit);
        eoss++;
        if (eoss == inputHandlers.ordinality())
            rows.enqueue(NULL);
    }
    void abort()
    {
        // no action needed here, funnels ends as result of inputs ending prematurely (as before)
    }

// IRowStream impl.
    virtual void stop()
    {
        ForEachItemIn(h, inputHandlers)
        {
            CInputHandler &handler = inputHandlers.item(h);
            handler.stop();
        }
        {
            CriticalBlock b(fullCrit);
            if (full)
            {
                stopped = true; // ensure pending push()'s don't enqueue and if big row potentially block again.
                loop
                {
                    OwnedConstThorRow row = rows.dequeueNow();
                    if (!row) break;
                }
                rows.stop(); // I don't think really needed
                totSize = 0;
                fullSem.signal();
            }
        }
        ForEachItemIn(h2, inputHandlers)
        {
            CInputHandler &handler = inputHandlers.item(h2);
            handler.join();
        }
        if (exception)
            throw exception.getClear();
    }
    virtual const void *nextRow()
    {
        if (exception)
            throw exception.getClear();
        OwnedConstThorRow row = rows.dequeue();
        if (!row) {
            rows.stop();
            return NULL;
        }
        size32_t sz = thorRowMemoryFootprint(serializer, row.get());
        {
            CriticalBlock b(fullCrit);
            assertex(totSize>=sz);
            totSize -= sz;
            if (full)
            {
                full = false;
                fullSem.signal();
            }
        }
        return row.getClear();
    }

friend class CInputHandler;
};


IRowStream *createParallelFunnel(CActivityBase &activity, IRowStream **instreams, unsigned numstreams)
{
    return new CParallelFunnel(activity, instreams, numstreams);
}

IRowStream *createParallelFunnel(CActivityBase &activity, IThorDataLink **instreams, unsigned numstreams, bool startInputs)
{
    return new CParallelFunnel(activity, instreams, numstreams, startInputs);
}


///////////////////
//
// FunnelSlaveActivity
//

//class CParallelFunnel;
//interface IBitSet;
class FunnelSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IThorDataLink *current;
    unsigned currentMarker;
    bool grouped, *eog, eogNext, parallel;
    rowcount_t readThisInput;
    unsigned stopped;
    Owned<IRowStream> parallelOutput;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    FunnelSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        grouped = false;
        eog = NULL;
        current = NULL;
        currentMarker = 0;
        eogNext = false;
        readThisInput = 0;
        stopped = true;
        parallel = false;
    }
    ~FunnelSlaveActivity()
    {
        if (eog) delete [] eog;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        IHThorFunnelArg *helper = (IHThorFunnelArg *)queryHelper();
        parallel = !container.queryGrouped() && !helper->isOrdered() && getOptBool(THOROPT_PARALLEL_FUNNEL, true);
        grouped = container.queryGrouped();
        appendOutputLinked(this);
        ActPrintLog("FUNNEL mode = %s, grouped=%s", parallel?"PARALLEL":"ORDERED", grouped?"GROUPED":"UNGROUPED");
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        if (!grouped && parallel)
            parallelOutput.setown(createParallelFunnel(*this, inputs.getArray(), inputs.ordinality(), true));
        else
        {
            eogNext = false;
            stopped = 0;
            if (grouped)
            {
                if (eog)
                    delete [] eog;
                eog = new bool[inputs.ordinality()];
                unsigned i;
                for (i=0; i<inputs.ordinality(); i++) eog[i] = false;
            }
            current = NULL;
            currentMarker = 0;
            readThisInput = 0;
            ForEachItemIn(i, inputs)
            {
                IThorDataLink * input = inputs.item(i);
                try { startInput(input); }
                catch (CATCHALL)
                {
                    ActPrintLog("FUNNEL(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                    throw;
                }
                if (!current) current = input;
            }
        }
        dataLinkStart();
    }
    virtual void stop()
    {
        if (parallelOutput)
        {
            parallelOutput->stop();
            parallelOutput.clear();
        }
        else
        {
            current = NULL;
            unsigned i = stopped;
            for (;i<inputs.ordinality(); i++)
                stopInput(inputs.item(i));
            stopped = 0;
        }
        dataLinkStop();
    }
    const void * groupedNext()
    {
        if (eogNext)
        {
            eogNext = false;
            return NULL;
        }
        OwnedConstThorRow row = current->nextRow();
        if (!row)
        {
            if (!eog[currentMarker] && readThisInput)
            {
                eog[currentMarker] = true;
                return NULL;
            }
            eog[currentMarker] = true;
            loop
            {
                ActPrintLog("FUNNEL: Read %" RCPF "d records from input %d", readThisInput, currentMarker);
                if (currentMarker + 1 < inputs.ordinality())
                {
                    readThisInput = 0;
                    currentMarker++;
                    ActPrintLog("FUNNEL: changing to input %d", currentMarker);
                    ++stopped;
                    stopInput(current);
                    current = inputs.item(currentMarker);
                    // if empty stream, move on (ensuring eog,eog not returned by empty streams)
                    row.setown(current->nextRow());
                    if (row)
                        break;
                }
                else
                {
                    ActPrintLog("FUNNEL: no more inputs");
                    current = NULL;
                    return NULL;
                }
            }
        }
        else
            eog[currentMarker] = false;
        readThisInput++;
        dataLinkIncrement();
        return row.getClear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (parallelOutput)
        {
            OwnedConstThorRow row = parallelOutput->nextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        else if (current)
        {
            if (grouped)
                return groupedNext();

            OwnedConstThorRow row = current->ungroupedNextRow();
            if (row)
            {
                readThisInput++;
                dataLinkIncrement();
                return row.getClear();
            }
            loop
            {
                ActPrintLog("FUNNEL: Read %" RCPF "d records from input %d", readThisInput, currentMarker);
                if (currentMarker + 1 < inputs.ordinality())
                {
                    readThisInput = 0;
                    currentMarker++;
                    ActPrintLog("FUNNEL: changing to input %d", currentMarker);
                    ++stopped;
                    stopInput(current);
                    current = inputs.item(currentMarker);
                    row.setown(current->ungroupedNextRow());
                    if (row)
                    {
                        readThisInput++;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                }
                else
                {
                    ActPrintLog("FUNNEL: no more inputs");
                    current = NULL;
                    break;
                }
            }
        }
        return NULL;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
    virtual bool isGrouped() { return grouped; }
};

/////
///////////////////
//
// CombineSlaveActivity
//

class CombineSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorCombineArg *helper;
    bool grouped;
    bool eogNext;
    MemoryBuffer recbuf;
    CThorExpandingRowArray rows;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);


    CombineSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this), rows(*this, this)
    {
        grouped = container.queryGrouped();
    }
    void init()
    {
        helper = (IHThorCombineArg *) queryHelper();
        appendOutputLinked(this);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        init();
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        eogNext = false;
        ForEachItemIn(i, inputs)
        {
            IThorDataLink * input = inputs.item(i);
            try { startInput(input); }
            catch (CATCHALL)
            {
                ActPrintLog("COMBINE(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        dataLinkStart();
    }
    void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        loop {
            bool eog = false;
            bool err = false;
            unsigned i;
            unsigned n = inputs.ordinality();
            for (i=0;i<n;i++) {
                OwnedConstThorRow row = inputs.item(i)->nextRow();
                if (row) {
                    if (eog) {
                        err = true;
                        break;
                    }
                    rows.append(row.getClear());
                }
                else {
                    if (i&&!eog) {
                        err = true;
                        break;
                    }
                    eog = true;
                }
            }
            if (err) {
                eog = true;
                rows.kill();
                throw MakeActivityException(this, -1, "mismatched input row count for Combine");
            }
            if (eog) 
                break;
            RtlDynamicRowBuilder row(queryRowAllocator());
            size32_t sizeGot = helper->transform(row, rows.ordinality(), rows.getRowArray());
            rows.kill();
            if (sizeGot) {
                dataLinkIncrement();
                return row.finalizeRowClear(sizeGot);
            }
        }
        rows.kill();
        return NULL;
    }
    bool isGrouped()
    {
        return inputs.item(0)->isGrouped();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        // TBD I think this should say max out = lhs set.
    }
};


/////


class RegroupSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorRegroupArg *helper;
    bool grouped;
    bool eogNext;
    unsigned curinput;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    RegroupSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        grouped = container.queryGrouped();
    }
    void init()
    {
        helper = (IHThorRegroupArg *) queryHelper();
        appendOutputLinked(this);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        init();
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        curinput = 0;
        eogNext = false;
        ForEachItemIn(i, inputs)
        {
            IThorDataLink * input = inputs.item(i);
            try { startInput(input); }
            catch (CATCHALL)
            {
                ActPrintLog("REGROUP(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        dataLinkStart();
    }
    void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        unsigned n = inputs.ordinality();
        loop {
            if (curinput==n) {
                curinput = 0;
                break;
            }
            OwnedConstThorRow row = inputs.item(curinput)->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
            curinput++;
        }
        return NULL;
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
    virtual bool isGrouped() { return true; }
};

/////

class NonEmptySlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorNonEmptyArg *helper;
    bool eogNext, eoi, anyThisGroup, anyThisInput;
    unsigned curinput;
    mptag_t masterMpTag;
    bool sendReceiving;

    bool advance()
    {
        curinput++;
        if (curinput==inputs.ordinality())
        {
            eoi = true;
            return false;
        }
        else
        {
            if (container.queryLocalOrGrouped())
            {
                if (anyThisInput)
                    return false;
            }
            else
            {
                CMessageBuffer msg;
                msg.append(anyThisInput);
                {
                    BooleanOnOff onOff(sendReceiving);
                    if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, masterMpTag, LONGTIMEOUT))
                        return false;
                }
                bool othersRead;
                msg.read(othersRead);
                if (anyThisInput || othersRead)
                    return false;
            }
            anyThisInput = false;
        }
        return true;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    NonEmptySlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = (IHThorNonEmptyArg *) queryHelper();
        sendReceiving = false;
        masterMpTag = TAG_NULL;
    }

// IThorSlaveActivity overloaded methods
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocalOrGrouped())
            masterMpTag = container.queryJobChannel().deserializeMPTag(data);
        appendOutputLinked(this);
    }
    void abort()
    {
        CSlaveActivity::abort();
        eoi = true;
        if (sendReceiving)
            queryJobChannel().queryJobComm().cancel(0, masterMpTag);
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        curinput = 0;
        anyThisGroup = anyThisInput = eogNext = false;
        ForEachItemIn(i, inputs)
        {
            IThorDataLink * input = inputs.item(i);
            try { startInput(input); }
            catch (CATCHALL)
            {
                ActPrintLog("NONEMPTY(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        eoi = 0 == inputs.ordinality();
        dataLinkStart();
    }
    virtual void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi) 
            return NULL; 
        if (eogNext) { 
            eogNext = false; 
            return NULL; 
        }
        loop
        {
            OwnedConstThorRow row = inputs.item(curinput)->nextRow();
            if (row ) {
                anyThisGroup = true;
                anyThisInput = true;
                dataLinkIncrement();
                return row.getClear();
            }
            if (anyThisGroup && container.queryGrouped())
            {
                anyThisGroup = false;
                break;
            }
            if (!advance())
            {
                eoi = true;
                break;
            }
        }
        return NULL;
    }
    virtual bool isGrouped() { return container.queryGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
};


class CNWaySelectActivity : public CSlaveActivity, public CThorDataLink, public CThorSteppable
{
    IHThorNWaySelectArg *helper;
    IThorDataLink *selectedInput;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNWaySelectActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this), CThorSteppable(this)
    {
        helper = (IHThorNWaySelectArg *)queryHelper();
        selectedInput = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);

        startInput(inputs.item(0));

        unsigned whichInput = helper->getInputIndex();
        selectedInput = NULL;
        if (whichInput--)
        {
            ForEachItemIn(i, inputs)
            {
                IThorDataLink *cur = inputs.item(i);
                CActivityBase *activity = cur->queryFromActivity();
                IThorNWayInput *nWayInput = dynamic_cast<IThorNWayInput *>(activity);
                if (nWayInput)
                {
                    unsigned numRealInputs = nWayInput->numConcreteOutputs();
                    if (whichInput < numRealInputs)
                    {
                        selectedInput = nWayInput->queryConcreteInput(whichInput);
                        break;
                    }
                    whichInput -= numRealInputs;
                }
                else
                {
                    if (whichInput == 0)
                        selectedInput = cur;
                    whichInput -= 1;
                }
                if (selectedInput)
                    break;
            }
        }
        if (selectedInput)
            selectedInput->start();
        dataLinkStart();
    }
    void stop()
    {
        stopInput(inputs.item(0));
        if (selectedInput)
            selectedInput->stop();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!selectedInput)
            return NULL;
        OwnedConstThorRow ret = selectedInput->nextRow();
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        if (!selectedInput)
            return false;
        return selectedInput->gatherConjunctions(collector);
    }
    void resetEOF() 
    { 
        if (selectedInput)
            selectedInput->resetEOF(); 
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!selectedInput)
            return NULL;
        return selectedInput->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (selectedInput)
            calcMetaInfoSize(info, selectedInput);
        else if (!started())
            info.canStall = true; // unkwown if !started
    }
    virtual bool isGrouped() { return selectedInput ? selectedInput->isGrouped() : false; }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CSlaveActivity::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta()
    {
        if (selectedInput)
            return selectedInput->querySteppingMeta();
        return NULL;
    }
};


class CThorNWayInputSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IThorNWayInput
{
    IHThorNWayInputArg *helper;
    PointerArrayOf<IThorDataLink> selectedInputs;
    bool grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorNWayInputSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), CThorDataLink(this)
    {
        helper = (IHThorNWayInputArg *)queryHelper();
        grouped = helper->queryOutputMeta()->isGrouped(); // JCSMORE should match graph info, i.e. container.queryGrouped()
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper->getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        selectedInputs.kill();
        if (selectionIsAll)
        {
            ForEachItemIn(i, inputs)
                selectedInputs.append(inputs.item(i));
        }
        else
        {
            const size32_t * selections = (const size32_t *)selection.getdata();
            unsigned max = selectionLen/sizeof(size32_t);
            for (unsigned i = 0; i < max; i++)
            {
                unsigned nextIndex = selections[i];
                //Check there are no duplicates.....  Assumes there are a fairly small number of inputs, so n^2 search is ok.
                for (unsigned j=i+1; j < max; j++)
                {
                    if (nextIndex == selections[j])
                        throw MakeStringException(100, "Selection list for nway input can not contain duplicates");
                }
                if (!inputs.isItem(nextIndex-1))
                    throw MakeStringException(100, "Index %d in RANGE selection list is out of range", nextIndex);

                selectedInputs.append(inputs.item(nextIndex-1));
            }
        }
        // NB: Whatever pulls this IThorNWayInput, starts and stops the selectedInputs
        dataLinkStart();
    }
    void stop()
    {
        // NB: Whatever pulls this IThorNWayInput, starts and stops the selectedInputs
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        throwUnexpected();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual bool isGrouped() { return grouped; }
// IThorNWayInput impl.
    virtual unsigned numConcreteOutputs() const
    {
        return selectedInputs.ordinality();
    }
    virtual IThorDataLink *queryConcreteInput(unsigned idx) const
    {
        if (selectedInputs.isItem(idx))
            return selectedInputs.item(idx);
        return NULL;
    }
};



CActivityBase *createFunnelSlave(CGraphElementBase *container)
{
    return new FunnelSlaveActivity(container);
}

CActivityBase *createCombineSlave(CGraphElementBase *container)
{
    return new CombineSlaveActivity(container);
}

CActivityBase *createRegroupSlave(CGraphElementBase *container)
{
    return new RegroupSlaveActivity(container);
}

CActivityBase *createNonEmptySlave(CGraphElementBase *container)
{
    return new NonEmptySlaveActivity(container);
}

CActivityBase *createNWaySelectSlave(CGraphElementBase *container)
{
    return new CNWaySelectActivity(container);
}

CActivityBase *createNWayInputSlave(CGraphElementBase *container)
{
    return new CThorNWayInputSlaveActivity(container);
}
