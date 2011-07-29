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
#include "hthor.ipp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "jprop.hpp"
#include "jdebug.hpp"
#include "jlzw.hpp"
#include "eclhelper.hpp"
#include "workunit.hpp"
#include "jfile.hpp"

#include "hrpc.hpp"
#include "hrpcsock.hpp"

#include "dafdesc.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "thorfile.hpp"
#include "thorparse.ipp"
#include "hthorstep.ipp"

//---------------------------------------------------------------------------

CHThorSteppedInput::CHThorSteppedInput(IHThorInput * _input)
{
    input = _input;
}


const void * CHThorSteppedInput::nextInputRow()
{
    const void * ret = input->nextInGroup();
    if (!ret)
        ret = input->nextInGroup();
    return ret;
}

const void * CHThorSteppedInput::nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
{
    //Currently isCompleteMatch is not handled by hthor
    return input->nextGE(seek, numFields);
}

IInputSteppingMeta * CHThorSteppedInput::queryInputSteppingMeta()
{
    return input->querySteppingMeta();
}


void CHThorSteppedInput::resetEOF()
{
    input->resetEOF();
}


//---------------------------------------------------------------------------

CHThorNaryActivity::CHThorNaryActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _arg, ThorActivityKind _kind) : CHThorMultiInputActivity(_agent, _activityId, _subgraphId, _arg, _kind)
{
}

void CHThorNaryActivity::done()
{
    ForEachItemIn(i, expandedInputs)
        expandedInputs.item(i)->done();
    expandedInputs.kill();
    CHThorMultiInputActivity::done();
}

void CHThorNaryActivity::ready()
{
    CHThorMultiInputActivity::ready();
    ForEachItemIn(i, inputs)
    {
        IHThorInput * cur = inputs.item(i);
        IHThorNWayInput * nWayInput = dynamic_cast<IHThorNWayInput *>(cur);
        if (nWayInput)
        {
            unsigned numRealInputs = nWayInput->numConcreteOutputs();
            for (unsigned i = 0; i < numRealInputs; i++)
            {
                IHThorInput * curReal = nWayInput->queryConcreteInput(i);
                expandedInputs.append(curReal);
            }
        }
        else
            expandedInputs.append(cur);
    }
}

void CHThorNaryActivity::updateProgress(IWUGraphProgress &progress) const
{
    //This would only have an effect if progress was updated while the graph was running.
    CHThorMultiInputActivity::updateProgress(progress);
    ForEachItemIn(i, expandedInputs)
        expandedInputs.item(i)->updateProgress(progress);
}


//---------------------------------------------------------------------------

CHThorNWayMergeActivity::CHThorNWayMergeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeArg &_arg, ThorActivityKind _kind) : CHThorNaryActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg)
{
    merger.init(helper.queryCompare(), helper.dedup(), helper.querySteppingMeta()->queryCompare());
}

CHThorNWayMergeActivity::~CHThorNWayMergeActivity()
{
    merger.cleanup();
}

void CHThorNWayMergeActivity::done()    
{
    merger.done();
    CHThorNaryActivity::done();
}

const void * CHThorNWayMergeActivity::nextInGroup()
{
    const void * next = merger.nextRow();
    if (next)
        processed++;
    return next;
}

void CHThorNWayMergeActivity::ready()
{
    CHThorNaryActivity::ready();
    merger.initInputs(expandedInputs.length(), expandedInputs.getArray());
}

IInputSteppingMeta * CHThorNWayMergeActivity::querySteppingMeta() 
{
    assertex(inputs.ordinality() != 0);
    if (meta.getNumFields() == 0)
    {
        meta.init(helper.querySteppingMeta(), false);
        ForEachItemIn(i, inputs)
        {
            IInputSteppingMeta * inputMeta = inputs.item(i)->querySteppingMeta();
            meta.intersect(inputMeta);
        }
    }
    if (meta.getNumFields() == 0)
        return NULL;
    return &meta;
}

const void * CHThorNWayMergeActivity::nextGE(const void * seek, unsigned numFields)
{
    SmartStepExtra stepExtra(SSEFreadAhead, NULL);
    bool matched = true;
    const void * next = merger.nextRowGE(seek, numFields, matched, stepExtra);
    if (next)
        processed++;
    return next;
}


//---------------------------------------------------------------------------

CHThorMergeJoinBaseActivity::CHThorMergeJoinBaseActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind, CMergeJoinProcessor & _processor) : CHThorNaryActivity(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), processor(_processor)
{
}

void CHThorMergeJoinBaseActivity::done()
{
    processor.afterProcessing();
    CHThorNaryActivity::done();
}


bool CHThorMergeJoinBaseActivity::gatherConjunctions(ISteppedConjunctionCollector & collector)
{
    return processor.gatherConjunctions(collector);
}

void CHThorMergeJoinBaseActivity::ready()
{
    CHThorNaryActivity::ready();

    ForEachItemIn(i1, expandedInputs)
    {
        IHThorInput * cur = expandedInputs.item(i1);
        Owned<CHThorSteppedInput> stepInput = new CHThorSteppedInput(cur);
        processor.addInput(stepInput);
    }

    ICodeContext * codectx = agent.queryCodeContext();
    Owned<IEngineRowAllocator> inputAllocator = codectx->getRowAllocator(helper.queryInputMeta(), activityId);
    Owned<IEngineRowAllocator> outputAllocator = codectx->getRowAllocator(helper.queryOutputMeta(), activityId);
    processor.beforeProcessing(inputAllocator, outputAllocator);
}


IInputSteppingMeta * CHThorMergeJoinBaseActivity::querySteppingMeta() 
{ 
    return processor.queryInputSteppingMeta();
}

const void * CHThorMergeJoinBaseActivity::nextInGroup()
{
    const void * next = processor.nextInGroup();
    if (next)
        processed++;
    return next;
}

const void * CHThorMergeJoinBaseActivity::nextGE(const void * seek, unsigned numFields)
{
    SmartStepExtra stepExtra(SSEFreadAhead, NULL);
    bool matched = true;
    const void * next = processor.nextGE(seek, numFields, matched, stepExtra);
    if (next)
        processed++;
    return next;
}



//---------------------------------------------------------------------------

CHThorAndMergeJoinActivity::CHThorAndMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind) : CHThorMergeJoinBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind, andProcessor), andProcessor(_arg)
{
}


//---------------------------------------------------------------------------

CHThorAndLeftMergeJoinActivity::CHThorAndLeftMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind) : CHThorMergeJoinBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind, andLeftProcessor), andLeftProcessor(_arg)
{
}


//---------------------------------------------------------------------------

CHThorMofNMergeJoinActivity::CHThorMofNMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind) : CHThorMergeJoinBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind, mofNProcessor), mofNProcessor(_arg)
{
}


//---------------------------------------------------------------------------

CHThorProximityJoinActivity::CHThorProximityJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind) : CHThorMergeJoinBaseActivity(_agent, _activityId, _subgraphId, _arg, _kind, proximityProcessor), proximityProcessor(_arg)
{
}


//---------------------------------------------------------------------------

#ifdef archived_old_code

CHThorNWayJoinActivity::CHThorNWayJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, IEngineRowAllocator * _inputAllocator, IEngineRowAllocator * _outputAllocator) : CHThorNaryActivity(_agent, _activityId, _subgraphId, _arg), helper(_arg), processor(_inputAllocator, _outputAllocator, _arg)
{
}

void CHThorNWayJoinActivity::done()
{
    processor.afterProcessing();
    CHThorNaryActivity::done();
}

void CHThorNWayJoinActivity::ready()
{
    CHThorNaryActivity::ready();

    UnsignedArray inputValues;
    ForEachItemIn(i1, expandedInputs)
    {
        IHThorInput * cur = expandedInputs.item(i1);
        Owned<CHThorSteppedInput> stepInput = new CHThorSteppedInput(cur);
        inputValues.append(processor.addInput(stepInput, cur->querySteppingMeta()));
    }
    processor.addJoin(helper, inputValues);
    processor.beforeProcessing();
}


IInputSteppingMeta * CHThorNWayJoinActivity::querySteppingMeta() 
{ 
    return processor.querySteppingMeta();
}

const void * CHThorNWayJoinActivity::nextInGroup()
{
    const void * next = processor.nextInGroup();
    if (next)
        processed++;
    return next;
}

const void * CHThorNWayJoinActivity::nextGE(const void * seek, unsigned numFields)
{
    const void * next = processor.nextGE(seek, numFields);
    if (next)
        processed++;
    return next;
}

#endif

//---------------------------------------------------------------------------


extern HTHOR_API IHThorActivity *createNWayMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind)
{
    unsigned flags = _arg.getJoinFlags();
    if (flags & IHThorNWayMergeJoinArg::MJFhasrange)
        return new CHThorProximityJoinActivity(_agent, _activityId, _subgraphId, _arg, _kind);

    switch (flags & IHThorNWayMergeJoinArg::MJFkindmask)
    {
    case IHThorNWayMergeJoinArg::MJFinner:
        return new CHThorAndMergeJoinActivity(_agent, _activityId, _subgraphId, _arg, _kind);
    case IHThorNWayMergeJoinArg::MJFleftonly:
    case IHThorNWayMergeJoinArg::MJFleftouter:
        return new CHThorAndLeftMergeJoinActivity(_agent, _activityId, _subgraphId, _arg, _kind);
    case IHThorNWayMergeJoinArg::MJFmofn:
        return new CHThorMofNMergeJoinActivity(_agent, _activityId, _subgraphId, _arg, _kind);
    }
    UNIMPLEMENTED;
}


extern HTHOR_API IHThorActivity *createNWayJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & _arg, ThorActivityKind _kind)
{
    return createNWayMergeJoinActivity(_agent, _activityId, _subgraphId, _arg, _kind);
}


MAKEFACTORY(NWayMerge)
