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

#ifndef SLAVE_IPP
#define SLAVE_IPP

#include "jlib.hpp"
#include "jthread.hpp"

#include "thgraphslave.hpp"
#include "eclhelper.hpp"        // for IHThorArg
#include "thormisc.hpp"

#include "slave.hpp"

interface IInputSteppingMeta;
interface IRangeCompare;
class ProcessSlaveActivity : public CSlaveActivity, implements IThreaded
{
protected:
    Owned<IThorException> exception;
    CThreadedPersistent threaded;
    rowcount_t processed;
    unsigned __int64 lastCycles;
    SpinLock cycleLock;

    virtual void endProcess() = 0;
    virtual void process() { }

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    ProcessSlaveActivity(CGraphElementBase *container);
    virtual void beforeDispose();

    virtual void startProcess(bool async=true);
    virtual bool wait(unsigned timeout);
    virtual void done();

    virtual void serializeStats(MemoryBuffer &mb);

// IThreaded
    void main();
};


class CThorSteppable
{
protected:
    IRangeCompare *stepCompare;
public:
    IInputSteppingMeta *inputStepping;

    CThorSteppable(CSlaveActivity *_activity) { stepCompare = NULL; inputStepping = NULL; }
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered)
    {
        if (0 == index && input.itdl)
        {
            inputStepping = input.itdl->querySteppingMeta();
            if (inputStepping)
                stepCompare = inputStepping->queryCompare();
        }
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return inputStepping; }
};


//// NWay stuff...

interface IThorNWayInput
{
    virtual unsigned numConcreteOutputs() const = 0;
    virtual IThorDataLink *queryConcreteInput(unsigned idx) const = 0;
};


class CThorNarySlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
    
protected:
    PointerArrayOf<IThorDataLink> expandedInputs;
    Owned<IStrandJunction> *expandedJunctions = nullptr;
    IPointerArrayOf<IEngineRowStream> expandedStreams;

public:
    CThorNarySlaveActivity(CGraphElementBase *container) : CSlaveActivity(container)
    {
    }
    ~CThorNarySlaveActivity()
    {
        delete [] expandedJunctions;
    }
    virtual void start() override
    {
        ForEachItemIn(i, inputs)
        {
            IThorDataLink *cur = queryInput(i);
            CActivityBase *activity = cur->queryFromActivity();
            IThorNWayInput *nWayInput = dynamic_cast<IThorNWayInput *>(cur);
            if (nWayInput)
            {
                unsigned numRealInputs = nWayInput->numConcreteOutputs();
                for (unsigned i=0; i < numRealInputs; i++)
                {
                    IThorDataLink *curReal = nWayInput->queryConcreteInput(i);
                    expandedInputs.append(curReal);
                }
            }
            else
                expandedInputs.append(cur);
        }
        ForEachItemIn(ei, expandedInputs)
            expandedInputs.item(ei)->start();
        expandedJunctions = new Owned<IStrandJunction> [expandedInputs.length()];
        ForEachItemIn(idx, expandedInputs)
        {
            expandedStreams.append(connectSingleStream(*this, expandedInputs.item(idx), 0, expandedJunctions[idx], true));  // MORE - is the index 0 right?
            startJunction(expandedJunctions[idx]);
        }
        dataLinkStart();
    }
    void stop()
    {
        ForEachItemIn(ei, expandedStreams)
            expandedStreams.item(ei)->stop();
        ForEachItemIn(idx, expandedInputs)
            resetJunction(expandedJunctions[idx]);
        expandedInputs.kill();
        expandedStreams.kill();
        delete [] expandedJunctions;
        expandedJunctions = nullptr;
    }
};


class CThorSteppedInput : public CSimpleInterfaceOf<ISteppedInput>
{
protected:
    IEngineRowStream *inputStream;
    IThorDataLink *input;

    virtual const void *nextInputRow()
    {
        return inputStream->ungroupedNextRow();
    }
    virtual const void *nextInputRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        assertex(wasCompleteMatch);
        return inputStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return input->gatherConjunctions(collector); }
    virtual IInputSteppingMeta *queryInputSteppingMeta()
    {
        return input->querySteppingMeta();
    }
    virtual void resetEOF()
    {
        inputStream->resetEOF();
    }
public:
    CThorSteppedInput(IThorDataLink *_input, IEngineRowStream *_inputStream) : input(_input), inputStream(_inputStream)
    {
    }
};




interface IStopInput
{
    virtual void stopInput()=0;
};

#endif


