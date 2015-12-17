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
#ifndef HTHORSTEP_IPP_INCL
#define HTHORSTEP_IPP_INCL

#include "thorstep.ipp"

//---------------------------------------------------------------------------

class CHThorSteppedInput : public CInterface, implements ISteppedInput
{
public:
    CHThorSteppedInput(IHThorInput * _input);
    IMPLEMENT_IINTERFACE

protected:
    virtual const void * nextInputRow();
    virtual const void * nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra);
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return input->gatherConjunctions(collector); }
    virtual IInputSteppingMeta * queryInputSteppingMeta();
    virtual void resetEOF();

protected:
    IHThorInput * input;
};

//---------------------------------------------------------------------------

class CHThorNaryActivity : public CHThorMultiInputActivity
{
protected:
    InputArrayType expandedInputs;

public:
    CHThorNaryActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, ThorActivityKind _kind);

    //interface IHThorInput
    virtual void stop();
    virtual void ready();
    virtual void updateProgress(IStatisticGatherer &progress) const;
};


//---------------------------------------------------------------------------


class CHThorNWayMergeActivity : public CHThorNaryActivity
{
public:
    CHThorNWayMergeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeArg &_arg, ThorActivityKind _kind);
    ~CHThorNWayMergeActivity();

    virtual void ready();
    virtual void stop();
    virtual const void * nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
    virtual IInputSteppingMeta * querySteppingMeta();

protected:
    IHThorNWayMergeArg &helper;
    CHThorStreamMerger merger;
    CSteppingMeta meta;
};

//---------------------------------------------------------------------------

class CHThorMergeJoinBaseActivity : public CHThorNaryActivity
{
public:
    CHThorMergeJoinBaseActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind, CMergeJoinProcessor & _processor);

    //interface IHThorInput
    virtual void ready();
    virtual void stop();
    virtual const void * nextRow();
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra);
    virtual IInputSteppingMeta * querySteppingMeta();
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector);

protected:
    void afterProcessing();
    void beforeProcessing();

protected:
    IHThorNWayMergeJoinArg & helper;
    CMergeJoinProcessor & processor;
};

class CHThorAndMergeJoinActivity : public CHThorMergeJoinBaseActivity
{
public:
    CHThorAndMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind);

protected:
    CAndMergeJoinProcessor andProcessor;
};


class CHThorAndLeftMergeJoinActivity : public CHThorMergeJoinBaseActivity
{
public:
    CHThorAndLeftMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind);

protected:
    CAndLeftMergeJoinProcessor andLeftProcessor;
};

class CHThorMofNMergeJoinActivity : public CHThorMergeJoinBaseActivity
{
public:
    CHThorMofNMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind);

protected:
    CMofNMergeJoinProcessor mofNProcessor;
};


class CHThorProximityJoinActivity : public CHThorMergeJoinBaseActivity
{
public:
    CHThorProximityJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind);

protected:
    CProximityJoinProcessor proximityProcessor;
};

#endif
