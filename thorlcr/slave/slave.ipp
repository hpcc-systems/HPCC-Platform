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
#include <atomic>

interface IInputSteppingMeta;
interface IRangeCompare;
class ProcessSlaveActivity : public CSlaveActivity, implements IThreaded
{
protected:
    Owned<IThorException> exception;
    CThreadedPersistent threaded;
    rowcount_t processed = 0;
    std::atomic<unsigned __int64> lastCycles{0};

    virtual void endProcess() = 0;
    virtual void process() { }

public:
    ProcessSlaveActivity(CGraphElementBase *container, const StatisticsMapping &statsMapping = basicActivityStatistics);
    virtual void beforeDispose();

    virtual void startProcess(bool async=true);
    virtual bool wait(unsigned timeout);
    virtual void done();

    virtual void serializeStats(MemoryBuffer &mb);

// IThreaded
    virtual void threadmain() override;
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
    virtual IThorDataLink *queryConcreteOutput(unsigned idx) const = 0;
    virtual IEngineRowStream *queryConcreteOutputStream(unsigned whichInput) const = 0;
    virtual IStrandJunction *queryConcreteOutputJunction(unsigned whichInput) const = 0;
};


class CThorNarySlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
    
protected:
    PointerArrayOf<IThorDataLink> expandedInputs;
    PointerArrayOf<IEngineRowStream> expandedStreams;
    PointerArrayOf<IStrandJunction> expandedJunctions;

public:
    CThorNarySlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        setRequireInitData(false);
    }
    virtual void start() override
    {
        ForEachItemIn(i, inputs)
        {
            IThorDataLink *curInput = queryInput(i);
            IThorNWayInput *nWayInput = dynamic_cast<IThorNWayInput *>(curInput);
            if (nWayInput)
            {
                curInput->start();
                unsigned numOutputs = nWayInput->numConcreteOutputs();
                for (unsigned i=0; i < numOutputs; i++)
                {
                    IThorDataLink *curReal = nWayInput->queryConcreteOutput(i);
                    IEngineRowStream *curRealStream = nWayInput->queryConcreteOutputStream(i);
                    IStrandJunction *curRealJunction = nWayInput->queryConcreteOutputJunction(i);
                    expandedInputs.append(curReal);
                    expandedStreams.append(curRealStream);
                    expandedJunctions.append(curRealJunction);
                }
            }
            else
            {
                expandedInputs.append(curInput);
                expandedStreams.append(queryInputStream(i));
                expandedJunctions.append(queryInputJunction(i));
            }
        }
        ForEachItemIn(ei, expandedInputs)
            expandedInputs.item(ei)->start();
        ForEachItemIn(idx, expandedInputs)
            startJunction(expandedJunctions.item(idx));
        dataLinkStart();
    }
    void stop()
    {
        stopAllInputs();
        expandedInputs.kill();
        expandedStreams.kill();
        expandedJunctions.kill();
        dataLinkStop();
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


