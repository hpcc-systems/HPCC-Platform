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


#include "thjoinslave.ipp"
#include "jio.hpp"
#include "jiface.hpp"

#include "thorstep.ipp"

#include "tsorts.hpp"
#include "tsorta.hpp"
#include "thsortu.hpp"
#include "thorport.hpp"
#include "thormisc.hpp"
#include "thbufdef.hpp"
#include "limits.h"
#include "commonext.hpp"
#include "thactivityutil.ipp"
#include "thexception.hpp"

#include "tsortm.hpp"

#define BUFFERSIZE 0x10000
#define NUMSLAVEPORTS 2     // actually should be num MP tags

class JoinSlaveActivity : public CSlaveActivity, implements ILookAheadStopNotify
{
    typedef CSlaveActivity PARENT;

    unsigned secondaryInputIndex = 0;
    unsigned primaryInputIndex = 0;
    IEngineRowStream *primaryInputStream = nullptr;
    IEngineRowStream *secondaryInputStream = nullptr;
    Owned<IThorDataLink> leftInput, rightInput;
    Owned<IThorDataLink> secondaryInput, primaryInput;
    Owned<IThorSorter> sorter;
    unsigned portbase = 0;
    mptag_t mpTagRPC = TAG_NULL;
    ICompare *primarySecondaryCompare = nullptr;
    ICompare *primarySecondaryUpperCompare = nullptr; // if non-null then between join

    Owned<IRowStream> leftStream, rightStream;
    Owned<IException> secondaryStartException;
    Owned<IThorRowLoader> iLoaderL, iLoaderR;

    Owned<IBarrier> barrier;
    SocketEndpoint server;

    Owned<IJoinHelper> joinhelper;
    rowcount_t lhsProgressCount = 0, rhsProgressCount = 0;
    CriticalSection joinHelperCrit;
    CRuntimeStatisticCollection spillStats;
    IHThorJoinBaseArg *helper;
    IHThorJoinArg *helperjn;
    IHThorDenormalizeArg *helperdn;
    ICompare *leftCompare;
    ICompare *rightCompare;
    ISortKeySerializer *leftKeySerializer;
    ISortKeySerializer *rightKeySerializer;
    bool rightpartition;
    bool islocal;

    bool hintunsortedoutput = false;
    bool hintparallelmatch = false;

    bool noSortPartitionSide()
    {
        if (ALWAYS_SORT_PRIMARY)
            return false;
        return (rightpartition?helper->isRightAlreadySorted():helper->isLeftAlreadySorted());
    }

    bool noSortOtherSide()
    {
        return (rightpartition?helper->isLeftAlreadySorted():helper->isRightAlreadySorted());
    }

    bool isUnstable()
    {
        // actually don't think currently supported by join but maybe will be sometime
        return false;
    }

    class cRowStreamPlus1Adaptor: implements IRowStream, public CSimpleInterface
    {
        OwnedConstThorRow firstrow;
        Linked<IRowStream> base;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cRowStreamPlus1Adaptor(IRowStream *_base,const void *_firstrow)
            : base(_base)
        {
            firstrow.set(_firstrow);
        }

        const void *nextRow()
        {
            if (firstrow)
                return firstrow.getClear();
            return base->ungroupedNextRow();
        }

        void stop()
        {
            firstrow.clear();
            base->stop();
        }


    };  

    struct CompareReverse : public ICompare
    {
        CompareReverse() { compare = NULL; }
        ICompare *compare;
        int docompare(const void *a,const void *b) const
        {
            return -compare->docompare(b,a);
        }
    } compareReverse, compareReverseUpper;

public:
    JoinSlaveActivity(CGraphElementBase *_container, bool local)
        : CSlaveActivity(_container), spillStats(spillStatistics)
    {
        islocal = local;
        switch (container.getKind())
        {
            case TAKdenormalize:
            case TAKdenormalizegroup:
            {
                helperjn = nullptr;
                helperdn = (IHThorDenormalizeArg *)container.queryHelper();
                helper = helperdn;
                break;
            }
            case TAKjoin:
            {
                helperjn = (IHThorJoinArg *)container.queryHelper();
                helperdn = nullptr;
                helper = helperjn;
                break;
            }
            default:
                throwUnexpected();
        }
        leftCompare = helper->queryCompareLeft();
        rightCompare = helper->queryCompareRight();
        leftKeySerializer = helper->querySerializeLeft();
        rightKeySerializer = helper->querySerializeRight();
        rightpartition = (container.getKind()==TAKjoin)&&((helper->getJoinFlags()&JFpartitionright)!=0);
        if (islocal)
            setRequireInitData(false);

        hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, (JFreorderable & helper->getJoinFlags()) != 0);
        hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point

        appendOutputLinked(this);
    }

    ~JoinSlaveActivity()
    {
        if (portbase) 
            queryJobChannel().freePort(portbase, NUMSLAVEPORTS);
    }

    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        StringAttr primaryInputStr, secondaryInputStr;
        if (rightpartition)
        {
            primaryInput.set(queryInput(1));
            secondaryInput.set(queryInput(0));
            primaryInputStr.set("R");
            secondaryInputStr.set("L");
            leftInput.set(secondaryInput);
            rightInput.set(primaryInput);
        }
        else
        {
            primaryInput.set(queryInput(0));
            secondaryInput.set(queryInput(1));
            primaryInputStr.set("L");
            secondaryInputStr.set("R");
            leftInput.set(primaryInput);
            rightInput.set(secondaryInput);
        }
        ActPrintLog("JOIN partition: %s. %s started first", primaryInputStr.get(), secondaryInputStr.get());

        if (islocal)
        {
            iLoaderL.setown(createThorRowLoader(*this, ::queryRowInterfaces(leftInput), leftCompare, stableSort_earlyAlloc, rc_mixed, SPILL_PRIORITY_JOIN));
            iLoaderL->setTracingPrefix("Join left");
            iLoaderR.setown(createThorRowLoader(*this, ::queryRowInterfaces(rightInput), rightCompare, stableSort_earlyAlloc, rc_mixed, SPILL_PRIORITY_JOIN));
            iLoaderR->setTracingPrefix("Join right");
        }
        else
        {
            mpTagRPC = container.queryJobChannel().deserializeMPTag(data);
            mptag_t barrierTag = container.queryJobChannel().deserializeMPTag(data);
            barrier.setown(container.queryJobChannel().createBarrier(barrierTag));
            portbase = queryJobChannel().allocPort(NUMSLAVEPORTS);
            ActPrintLog("SortJoinSlaveActivity::init portbase = %d, mpTagRPC=%d",portbase,(int)mpTagRPC);
            server.setLocalHost(portbase); 
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
            server.serialize(slaveData);
        }

    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, _input, consumerOrdered);
        if ((rightpartition && (0 == index)) || (!rightpartition && (1 == index)))
        {
            secondaryInputIndex = index;
            secondaryInputStream = queryInputStream(secondaryInputIndex);
        }
        else
        {
            primaryInputIndex = index;
            primaryInputStream = queryInputStream(primaryInputIndex);
        }
    }
    virtual void onInputFinished(rowcount_t count) override
    {
        ActPrintLog("JOIN: %s input finished, %" RCPF "d rows read", rightpartition?"LHS":"RHS", count);
    }

    void doDataLinkStart()
    {
        dataLinkStart();
        CriticalBlock b(joinHelperCrit);
        switch(container.getKind())
        {
            case TAKjoin:
            {
                joinhelper.setown(createJoinHelper(*this, helperjn, this, hintparallelmatch, hintunsortedoutput));
                break;
            }
            case TAKdenormalize:
            case TAKdenormalizegroup:
            {
                joinhelper.setown(createDenormalizeHelper(*this, helperdn, this));
                break;
            }
            default:
                throwUnexpected();
        }
    }
    void startSecondaryInput()
    {
        try
        {
            startInput(secondaryInputIndex);

            if (ensureStartFTLookAhead(secondaryInputIndex))
            {
                IThorDataLink *secondaryInput = queryInput(secondaryInputIndex);
                // NB: lookahead told not to preserveGroups
                setLookAhead(secondaryInputIndex, createRowStreamLookAhead(this, secondaryInputStream, queryRowInterfaces(secondaryInput), JOIN_SMART_BUFFER_SIZE, ::canStall(secondaryInput),
                             false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), false);
            }
            secondaryInputStream = queryInputStream(secondaryInputIndex); // either lookahead or underlying stream, depending on whether active or not
        }
        catch (IException *e)
        {
            secondaryStartException.setown(e);
        }
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);

        CAsyncCallStart asyncSecondaryStart(std::bind(&JoinSlaveActivity::startSecondaryInput, this));
        try
        {
            startInput(primaryInputIndex);
        }
        catch (IException *e)
        {
            fireException(e);
            barrier->cancel();
            asyncSecondaryStart.wait();
            stopOtherInput();
            throw;
        }
        asyncSecondaryStart.wait();
        if (secondaryStartException)
        {
            IException *e=secondaryStartException.getClear();
            fireException(e);
            barrier->cancel();
            stopPartitionInput();
            throw e;
        }
        if (rightpartition)
        {
            leftInput.set(secondaryInput);
            rightInput.set(primaryInput);
        }
        else
        {
            leftInput.set(primaryInput);
            rightInput.set(secondaryInput);
        }

        doDataLinkStart();
        if (islocal)
            dolocaljoin();
        else
        {
            if (!doglobaljoin())
            {
                Sleep(1000); // let original error through
                throw MakeActivityException(this, TE_BarrierAborted, "JOIN: Barrier Aborted");
            }
        }
        if (!leftStream.get()||!rightStream.get())
            throw MakeActivityException(this, TE_FailedToStartJoinStreams, "Failed to start join streams");
        joinhelper->init(leftStream, rightStream, ::queryRowAllocator(queryInput(0)),::queryRowAllocator(queryInput(1)),::queryRowMetaData(queryInput(0)));
    }
    void stopLeftInput()
    {
        stopInput(0, "(L)");
    }
    void stopRightInput()
    {
        stopInput(1, "(R)");
    }
    void stopPartitionInput()
    {
        if (rightpartition)
            stopRightInput();
        else
            stopLeftInput();
    }
    void stopOtherInput()
    {
        if (rightpartition)
            stopLeftInput();
        else
            stopRightInput();
    }
    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (joinhelper)
            joinhelper->stop();
    }
    virtual void stop() override
    {
        stopLeftInput();
        stopRightInput();
        /* need to if input started, because activity might never have been started, if conditional
         * activities downstream stopped their inactive inputs.
         * stop()'s are chained like this, so that upstream splitters can be stopped as quickly as possible
         * in order to reduce buffering.
         */
        if (hasStarted())
        {
            lhsProgressCount = joinhelper->getLhsProgress();
            rhsProgressCount = joinhelper->getRhsProgress();
            {
                CriticalBlock b(joinHelperCrit);
                joinhelper.clear();
            }
            ActPrintLog("SortJoinSlaveActivity::stop");
            rightStream.clear();
            if (!islocal)
            {
                unsigned bn=noSortPartitionSide()?2:4;
                ActPrintLog("JOIN waiting barrier.%d",bn);
                barrier->wait(false);
                ActPrintLog("JOIN barrier.%d raised",bn);
                sorter->stopMerge();
            }
            leftStream.clear();
            dataLinkStop();
            leftInput.clear();
            rightInput.clear();
            secondaryInputStream = queryInputStream(secondaryInputIndex);
        }
    }
    virtual void reset() override
    {
        PARENT::reset();
        if (sorter) return; // JCSMORE loop - shouldn't have to recreate sorter between loop iterations
        if (!islocal && TAG_NULL != mpTagRPC)
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&queryJobChannel().queryJobComm(),mpTagRPC));
    }
    virtual void kill() override
    {
        sorter.clear();
        leftInput.clear();
        rightInput.clear();
        if (portbase)
        {
            queryJobChannel().freePort(portbase, NUMSLAVEPORTS);
            portbase = 0;
        }
        CSlaveActivity::kill();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if(joinhelper) 
        {
            OwnedConstThorRow row = joinhelper->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canBufferInput = true;
    }
    void dolocaljoin()
    {
        bool isemptylhs = false;
        IRowStream *leftInputStream = inputStream;
        if (helper->isLeftAlreadyLocallySorted())
        {
            ThorDataLinkMetaInfo info;
            leftInput->getMetaInfo(info);
            if (info.totalRowsMax==0) 
                isemptylhs = true;
            if (leftInput->isGrouped())
                leftStream.setown(createUngroupStream(leftInputStream));
            else
                leftStream.set(leftInputStream);
        }
        else
        {
            leftStream.setown(iLoaderL->load(leftInputStream, abortSoon));
            isemptylhs = 0 == iLoaderL->numRows();
            stopLeftInput();
            mergeStats(spillStats, iLoaderL);
        }
        IEngineRowStream *rightInputStream = queryInputStream(1);
        if (isemptylhs&&((helper->getJoinFlags()&JFrightouter)==0))
        {
            ActPrintLog("ignoring RHS as LHS empty");
            rightStream.setown(createNullRowStream());
            stopRightInput();
        }
        else if (helper->isRightAlreadyLocallySorted())
        {
            if (rightInput->isGrouped())
                rightStream.setown(createUngroupStream(rightInputStream));
            else
                rightStream.set(rightInputStream);
        }
        else
        {
            rightStream.setown(iLoaderR->load(rightInputStream, abortSoon));
            stopRightInput();
            mergeStats(spillStats, iLoaderR);
        }
    }
    bool doglobaljoin()
    {
        rightpartition = (container.getKind()==TAKjoin)&&((helper->getJoinFlags()&JFpartitionright)!=0);

        Linked<IThorRowInterfaces> primaryRowIf, secondaryRowIf;
        ICompare *primaryCompare, *secondaryCompare;
        ISortKeySerializer *primaryKeySerializer;

        Owned<IRowStream> secondaryStream, primaryStream;
        if (rightpartition)
        {
            primaryCompare = rightCompare;
            primaryKeySerializer = rightKeySerializer;
            secondaryCompare = leftCompare;
        }
        else
        {
            primaryCompare = leftCompare;
            primaryKeySerializer = leftKeySerializer;
            secondaryCompare = rightCompare;
        }
        primaryRowIf.set(queryRowInterfaces(primaryInput));
        secondaryRowIf.set(queryRowInterfaces(secondaryInput));

        primarySecondaryCompare = NULL;
        if (helper->getJoinFlags()&JFslidingmatch)
        {
            if (primaryKeySerializer) // JCSMORE shouldn't be generated
                primaryKeySerializer = NULL;
            primarySecondaryCompare = helper->queryCompareLeftRightLower();
            primarySecondaryUpperCompare = helper->queryCompareLeftRightUpper();
            if (rightpartition)
            {
                compareReverse.compare = primarySecondaryCompare;
                compareReverseUpper.compare = primarySecondaryUpperCompare;
                primarySecondaryCompare = &compareReverse;
                primarySecondaryUpperCompare = &compareReverseUpper;
            }
        }
        else
        {
            primarySecondaryUpperCompare = NULL;
            if (rightpartition)
            {
                if (rightKeySerializer)
                    primarySecondaryCompare = helper->queryCompareRightKeyLeftRow();
                else
                {
                    compareReverse.compare = helper->queryCompareLeftRight();
                    primarySecondaryCompare = &compareReverse;
                }
            }
            else
            {
                if (leftKeySerializer)
                    primarySecondaryCompare = helper->queryCompareLeftKeyRightRow();
                else
                    primarySecondaryCompare = helper->queryCompareLeftRight();
            }
        }
        dbgassertex(primarySecondaryCompare);

        OwnedConstThorRow partitionRow;
        rowcount_t totalrows;

        bool usePrimaryPartition = true;
        if (noSortPartitionSide())
        {
            partitionRow.setown(primaryInputStream->ungroupedNextRow());
            primaryStream.setown(new cRowStreamPlus1Adaptor(primaryInputStream, partitionRow));
        }
        else
        {
            sorter->Gather(primaryRowIf, primaryInputStream, primaryCompare, NULL, NULL, primaryKeySerializer, NULL, NULL, false, isUnstable(), abortSoon, NULL);
            stopPartitionInput();
            if (abortSoon)
            {
                barrier->cancel();
                return false;
            }
            ActPrintLog("JOIN waiting barrier.1");
            if (!barrier->wait(false))
                return false;
            ActPrintLog("JOIN barrier.1 raised");

            // primaryWriter will keep as much in memory as possible.
            Owned<IRowWriterMultiReader> primaryWriter = createOverflowableBuffer(*this, primaryRowIf, ers_forbidden);
            primaryStream.setown(sorter->startMerge(totalrows));
            copyRowStream(primaryStream, primaryWriter);
            primaryStream.setown(primaryWriter->getReader()); // NB: rhsWriter no longer needed after this point

            ActPrintLog("JOIN waiting barrier.2");
            if (!barrier->wait(false))
                return false;
            ActPrintLog("JOIN barrier.2 raised");
            sorter->stopMerge();
            if (0 == sorter->getGlobalCount())
                usePrimaryPartition = false;
        }
        // NB: on secondary sort, the primaryKeySerializer is used
        if (usePrimaryPartition)
            sorter->Gather(secondaryRowIf, secondaryInputStream, secondaryCompare, primarySecondaryCompare, primarySecondaryUpperCompare, primaryKeySerializer, primaryCompare, partitionRow, noSortOtherSide(), isUnstable(), abortSoon, primaryRowIf); // primaryKeySerializer *is* correct
        else
            sorter->Gather(secondaryRowIf, secondaryInputStream, secondaryCompare, nullptr, nullptr, nullptr, nullptr, partitionRow, noSortOtherSide(), isUnstable(), abortSoon, nullptr);
        mergeStats(spillStats, sorter);
        //MORE: Stats from spilling the primaryStream??
        partitionRow.clear();
        stopOtherInput();
        if (abortSoon)
        {
            barrier->cancel();
            return false;
        }
        ActPrintLog("JOIN waiting barrier.3");
        if (!barrier->wait(false))
            return false;
        ActPrintLog("JOIN barrier.3 raised");
        secondaryStream.setown(sorter->startMerge(totalrows));
        if (rightpartition)
        {
            leftStream.setown(secondaryStream.getClear());
            rightStream.setown(primaryStream.getClear());
        }
        else
        {
            leftStream.setown(primaryStream.getClear());
            rightStream.setown(secondaryStream.getClear());
        }
        return true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        CSlaveActivity::serializeStats(mb);
        CriticalBlock b(joinHelperCrit);
        if (!joinhelper)
        {
            mb.append(lhsProgressCount);
            mb.append(rhsProgressCount);
        }
        else
        {
            mb.append(joinhelper->getLhsProgress());
            mb.append(joinhelper->getRhsProgress());
        }
        spillStats.serialize(mb);
    }
};


//////////////////////


class CMergeJoinSlaveBaseActivity : public CThorNarySlaveActivity, public CThorSteppable
{
    typedef CThorNarySlaveActivity PARENT;

    IHThorNWayMergeJoinArg *helper;
    Owned<IEngineRowAllocator> inputAllocator, outputAllocator;

protected:
    CMergeJoinProcessor &processor;

    void afterProcessing();
    void beforeProcessing();

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CMergeJoinSlaveBaseActivity(CGraphElementBase *_container, CMergeJoinProcessor &_processor) : CThorNarySlaveActivity(_container), CThorSteppable(this), processor(_processor)
    {
        helper = (IHThorNWayMergeJoinArg *)queryHelper();
        setRequireInitData(false);
        inputAllocator.setown(getRowAllocator(helper->queryInputMeta()));
        outputAllocator.setown(getRowAllocator(helper->queryOutputMeta()));
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        CThorNarySlaveActivity::start();

        ForEachItemIn(i1, expandedInputs)
        {
            Owned<CThorSteppedInput> stepInput = new CThorSteppedInput(expandedInputs.item(i1), expandedStreams.item(i1));
            processor.addInput(stepInput);
        }
        processor.beforeProcessing(inputAllocator, outputAllocator);
    }
    virtual void stop() override
    {
        processor.afterProcessing();
        CThorNarySlaveActivity::stop();
    }
    CATCH_NEXTROW()
    {
        OwnedConstThorRow ret = processor.nextRow();
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        return NULL;
    }
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    virtual const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        bool matched = true;
        OwnedConstThorRow next = processor.nextGE(seek, numFields, matched, stepExtra);
        if (next)
            dataLinkIncrement();
        return next.getClear();
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canBufferInput = true;
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    {
        return processor.gatherConjunctions(collector);
    }
    virtual void resetEOF() 
    { 
        processor.queryResetEOF(); 
    }
// steppable
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override
    {
        CThorNarySlaveActivity::setInputStream(index, input, consumerOrdered);
        CThorSteppable::setInputStream(index, input, consumerOrdered);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};


class CAndMergeJoinSlaveActivity : public CMergeJoinSlaveBaseActivity
{
protected:
    CAndMergeJoinProcessor andProcessor;
public:
    CAndMergeJoinSlaveActivity(CGraphElementBase *container) : CMergeJoinSlaveBaseActivity(container, andProcessor), andProcessor(* ((IHThorNWayMergeJoinArg*)container->queryHelper()))
    {
    }
};


class CAndLeftMergeJoinSlaveActivity : public CMergeJoinSlaveBaseActivity
{
protected:
    CAndLeftMergeJoinProcessor andLeftProcessor;
public:
    CAndLeftMergeJoinSlaveActivity(CGraphElementBase *container) : CMergeJoinSlaveBaseActivity(container, andLeftProcessor), andLeftProcessor(* ((IHThorNWayMergeJoinArg*)container->queryHelper()))
    {
    }
};

class CMofNMergeJoinSlaveActivity : public CMergeJoinSlaveBaseActivity
{
    CMofNMergeJoinProcessor mofNProcessor;
public:
    CMofNMergeJoinSlaveActivity(CGraphElementBase *container) : CMergeJoinSlaveBaseActivity(container, mofNProcessor), mofNProcessor(* ((IHThorNWayMergeJoinArg*)container->queryHelper()))
    {
    }
};


class CProximityJoinSlaveActivity : public CMergeJoinSlaveBaseActivity
{
    CProximityJoinProcessor proximityProcessor;
public:
    CProximityJoinSlaveActivity(CGraphElementBase *container) : CMergeJoinSlaveBaseActivity(container, proximityProcessor), proximityProcessor(* ((IHThorNWayMergeJoinArg*)container->queryHelper()))
    {
    }
};


CActivityBase *createLocalJoinSlave(CGraphElementBase *container)
{
    return new JoinSlaveActivity(container, true);
}

CActivityBase *createJoinSlave(CGraphElementBase *container)
{
    return new JoinSlaveActivity(container, false);
}



CActivityBase *createDenormalizeSlave(CGraphElementBase *container)
{
    return new JoinSlaveActivity(container, false);
}

CActivityBase *createLocalDenormalizeSlave(CGraphElementBase *container)
{
    return new JoinSlaveActivity(container, true);
}


CActivityBase *createNWayMergeJoinActivity(CGraphElementBase *container)
{
    IHThorNWayMergeJoinArg *helper = (IHThorNWayMergeJoinArg *)container->queryHelper();
    unsigned flags = helper->getJoinFlags();
    if (flags & IHThorNWayMergeJoinArg::MJFhasrange)
        return new CProximityJoinSlaveActivity(container);

    switch (flags & IHThorNWayMergeJoinArg::MJFkindmask)
    {
        case IHThorNWayMergeJoinArg::MJFinner:
            return new CAndMergeJoinSlaveActivity(container);
        case IHThorNWayMergeJoinArg::MJFleftonly:
        case IHThorNWayMergeJoinArg::MJFleftouter:
            return new CAndLeftMergeJoinSlaveActivity(container);
        case IHThorNWayMergeJoinArg::MJFmofn:
            return new CMofNMergeJoinSlaveActivity(container);
    }
    UNIMPLEMENTED;
}
