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
    virtual void done();
    virtual void ready();
    virtual void updateProgress(IWUGraphProgress &progress) const;
};


//---------------------------------------------------------------------------


class CHThorNWayMergeActivity : public CHThorNaryActivity
{
public:
    CHThorNWayMergeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeArg &_arg, ThorActivityKind _kind);
    ~CHThorNWayMergeActivity();

    virtual void ready();
    virtual void done();
    virtual const void * nextInGroup();
    virtual const void * nextGE(const void * seek, unsigned numFields);
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
    virtual void done();
    virtual const void * nextInGroup();
    virtual const void * nextGE(const void * seek, unsigned numFields);
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


#ifdef archived_old_code

class CHThorNWayJoinActivity : public CHThorNaryActivity
{
public:
    CHThorNWayJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator);

    //interface IHThorInput
    virtual void ready();
    virtual void done();
    virtual const void * nextInGroup();
    virtual const void * nextGE(const void * seek, unsigned numFields);
    virtual IInputSteppingMeta * querySteppingMeta();

protected:
    void afterProcessing();
    void beforeProcessing();

protected:
    IHThorNWayMergeJoinArg & helper;
    CNaryJoinProcessor processor;
};

#endif

#endif
