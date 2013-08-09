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

class JoinSlaveActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify
{
    Owned<IThorDataLink> input1;
    Owned<IThorDataLink> input2;
    IHThorJoinBaseArg *helper;
    IHThorJoinArg *helperjn;
    IHThorDenormalizeArg *helperdn;
    Owned<IThorSorter> sorter;
    unsigned portbase;
    mptag_t mpTagRPC;
    ICompare *compare1;
    ICompare *compare2;
    ISortKeySerializer *keyserializer1;
    ISortKeySerializer *keyserializer2;
    ICompare *collate;
    ICompare *collateupper; // if non-null then between join

    Owned<IRowStream> strm1;  // reads from disk
    Owned<IRowStream> strm2;  // from merge 
    void *temprec;
    Semaphore in2startsem;
    Owned<IException> in2exception;
    Semaphore in1startsem;
    Owned<IException> in1exception;

    int infh;
    StringBuffer tempname;

    bool islocal;
    Owned<IBarrier> barrier;
    SocketEndpoint server;
    StringBuffer activityName;

#ifdef _TESTING
    bool started;
#endif

    Owned<IJoinHelper> joinhelper;
    rowcount_t lhsProgressCount, rhsProgressCount;
    CriticalSection joinHelperCrit;
    bool in1stopped;
    bool in2stopped;
    bool rightpartition;


    bool nosortPrimary()
    {
        if (ALWAYS_SORT_PRIMARY)
            return false;
        return (rightpartition?helper->isRightAlreadySorted():helper->isLeftAlreadySorted());
    }

    bool nosortSecondary()
    {
        return (rightpartition?helper->isLeftAlreadySorted():helper->isRightAlreadySorted());
    }

    bool isUnstable()
    {
        // actually don't think currently supported by join but maybe will be sometime
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        return (algo&&algo->getAlgorithmFlags()&TAFunstable);
    }

    class cRowStreamPlus1Adaptor: public CSimpleInterface, implements IRowStream
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

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);


    JoinSlaveActivity(CGraphElementBase *_container, bool local)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        islocal = local;
        portbase = 0;
#ifdef _TESTING
        started = false;
#endif
        in1stopped = true;
        in2stopped = true;
        infh = 0;
        lhsProgressCount = 0;
        rhsProgressCount = 0;
        mpTagRPC = TAG_NULL;
    }

    ~JoinSlaveActivity()
    {
        if (portbase) 
            freePort(portbase,NUMSLAVEPORTS);
    }

    struct cCollateReverse: public ICompare
    {
        ICompare *collate;
        int docompare(const void *a,const void *b) const 
        {
            return -collate->docompare(b,a);
        }
    } collaterev, collaterevupper;

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!islocal) {
            mpTagRPC = container.queryJob().deserializeMPTag(data);
            mptag_t barrierTag = container.queryJob().deserializeMPTag(data);
            barrier.setown(container.queryJob().createBarrier(barrierTag));
            portbase = allocPort(NUMSLAVEPORTS);
            ActPrintLog("SortJoinSlaveActivity::init portbase = %d, mpTagRPC=%d",portbase,(int)mpTagRPC);
            server.setLocalHost(portbase); 
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&container.queryJob().queryJobComm(),mpTagRPC));
            server.serialize(slaveData);
        }
        appendOutputLinked(this);
        switch (container.getKind())
        {
            case TAKdenormalize:
            case TAKdenormalizegroup:
            {
                helperjn = NULL;        
                helperdn = (IHThorDenormalizeArg *)container.queryHelper();     
                helper = helperdn;
                break;
            }
            case TAKjoin:
            {
                helperjn = (IHThorJoinArg *)container.queryHelper();        
                helperdn = NULL;        
                helper = helperjn;
                break;
            }
            default:
                throwUnexpected();
        }
        compare1 = helper->queryCompareLeft();
        compare2 = helper->queryCompareRight();
        keyserializer1 = helper->querySerializeLeft();
        keyserializer2 = helper->querySerializeRight();
        if (helper->getJoinFlags()&JFslidingmatch) {
            collate = helper->queryCompareLeftRightLower();
            collateupper = helper->queryCompareLeftRightUpper();
        }
        else {
            collate = helper->queryCompareLeftRight();
            collateupper = NULL;
        }
    }
    virtual void onInputStarted(IException *except)
    {
        if (rightpartition) {
            in1exception.set(except);
            in1startsem.signal();
        }
        else {
            in2exception.set(except);
            in2startsem.signal();
        }
    }
    virtual bool startAsync() { return true; }
    virtual void onInputFinished(rowcount_t count)
    {
        ActPrintLog("JOIN: %s input finished, %"RCPF"d rows read", rightpartition?"LHS":"RHS", count);
    }

    void doDataLinkStart(bool denorm)
    {
        dataLinkStart(activityName, container.queryId());
        CriticalBlock b(joinHelperCrit);
        if (denorm)
            joinhelper.setown(createDenormalizeHelper(*this, helperdn, this));
        else
        {
            bool hintparallelmatch = container.queryXGMML().getPropInt("hint[@name=\"parallel_match\"]/@value")!=0;
            bool hintunsortedoutput = container.queryXGMML().getPropInt("hint[@name=\"unsorted_output\"]/@value")!=0;
            joinhelper.setown(createJoinHelper(*this, helperjn, this, hintparallelmatch, hintunsortedoutput));
        }
    }

    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        rightpartition = (container.getKind()==TAKjoin)&&((helper->getJoinFlags()&JFpartitionright)!=0);
        input1.set(inputs.item(0));
        input2.set(inputs.item(1));
        if (rightpartition) {
#define JOINL_SMART_BUFFER_SIZE JOINR_SMART_BUFFER_SIZE
            input1.setown(createDataLinkSmartBuffer(this, input1,JOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(inputs.item(0)->queryFromActivity()),false,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage()));
            ActPrintLog("JOIN: Starting L then R");
            startInput(input1); 
            in1stopped = false;
            try { 
                startInput(input2);
                in2stopped = false;
            }
            catch (IException *e)
            {
                fireException(e);
                barrier->cancel();
                in1startsem.wait();
                stopInput1();
                throw;
            }
            in1startsem.wait();
            if (in1exception) 
            {
                IException *e=in1exception.getClear();
                fireException(e);
                barrier->cancel();
                stopInput2();
                throw e;
            }
        }
        else {
            input2.setown(createDataLinkSmartBuffer(this, input2,JOINR_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(inputs.item(1)->queryFromActivity()),false,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage()));
            ActPrintLog("JOIN: Starting R then L");
            startInput(input2); 
            in2stopped = false;
            try { 
                startInput(input1); 
                in1stopped = false;
            }
            catch (IException *e)
            {
                fireException(e);
                if (barrier) barrier->cancel();
                in2startsem.wait();
                stopInput2();
                throw;
            }
            in2startsem.wait();
            if (in2exception) 
            {
                IException *e=in2exception.getClear();
                fireException(e);
                if (barrier) barrier->cancel();
                stopInput1();
                throw e;
            }
        }               
        
        switch(container.getKind()) {
        case TAKjoin:
            doDataLinkStart(false);
            if (islocal)
                dolocaljoin();
            else
            {
                if (!doglobaljoin()) {
                    Sleep(1000); // let original error through
                    throw MakeThorException(TE_BarrierAborted,"JOIN: Barrier Aborted");
                }
            }
            break;
        case TAKdenormalize:
        case TAKdenormalizegroup:
            doDataLinkStart(true);
            if (islocal)
                dolocaljoin();
            else
            {
                if (!doglobaljoin()) {
                    Sleep(1000); // let original error through first
                    throw MakeThorException(TE_BarrierAborted,"DENORMALIZE: Barrier Aborted");
                }
            }
            break;
        }
        if (!strm1.get()||!strm2.get()) {
            throw MakeThorException(TE_FailedToStartJoinStreams, "Failed to start join streams");
        }
        joinhelper->init(strm1, strm2, ::queryRowAllocator(inputs.item(0)),::queryRowAllocator(inputs.item(1)),::queryRowMetaData(inputs.item(0)), &abortSoon);
    }
    void stopInput1()
    {
        if (!in1stopped) {
            stopInput(input1, "(L)");
            in1stopped = true;
        }
    }
    void stopInput2()
    {
        if (!in2stopped) {
            stopInput(input2, "(R)");
            in2stopped = true;
        }
    }
    void stop() 
    {
        stopInput1();
        stopInput2();
        lhsProgressCount = joinhelper->getLhsProgress();
        rhsProgressCount = joinhelper->getRhsProgress();
        {
            CriticalBlock b(joinHelperCrit);
            joinhelper.clear();
        }
        ActPrintLog("SortJoinSlaveActivity::stop");
        strm2.clear();
        if (!islocal) {
            unsigned bn=nosortPrimary()?2:4;
            ActPrintLog("JOIN waiting barrier.%d",bn);
            barrier->wait(false);
            ActPrintLog("JOIN barrier.%d raised",bn);
            sorter->stopMerge();
        }
        strm1.clear();
        if (infh)
            _close(infh);
        if (tempname.length())
            remove(tempname.toCharArray());
        dataLinkStop();
        input1.clear();
        input2.clear();
    }
    void reset()
    {
        if (sorter) return; // JCSMORE loop - shouldn't have to recreate sorter between loop iterations
        if (!islocal && TAG_NULL != mpTagRPC)
            sorter.setown(CreateThorSorter(this, server,&container.queryJob().queryIDiskUsage(),&container.queryJob().queryJobComm(),mpTagRPC));
    }
    void kill()
    {
        sorter.clear();
        input1.clear();
        input2.clear();
        CSlaveActivity::kill();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
    bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canBufferInput = true;
    }
    void dolocaljoin()
    {
        // NB: old version used to force both sides all to disk
        Owned<IThorRowLoader> iLoaderL = createThorRowLoader(*this, ::queryRowInterfaces(input1), compare1, true, rc_mixed, SPILL_PRIORITY_JOIN);
        Owned<IThorRowLoader> iLoaderR = createThorRowLoader(*this, ::queryRowInterfaces(input2), compare2, true, rc_mixed, SPILL_PRIORITY_JOIN);
        bool isemptylhs = false;
        if (helper->isLeftAlreadySorted()) {
            ThorDataLinkMetaInfo info;
            input1->getMetaInfo(info);      
            if (info.totalRowsMax==0) 
                isemptylhs = true;
            if (rightpartition)
                strm1.set(input1.get()); // already ungrouped
            else
                strm1.setown(createUngroupStream(input1));
        }
        else {
            StringBuffer tmpStr;
            strm1.setown(iLoaderL->load(input1, abortSoon));
            isemptylhs = 0 == iLoaderL->numRows();
            stopInput1();
        }
        if (isemptylhs&&((helper->getJoinFlags()&JFrightouter)==0)) {
            ActPrintLog("%s: ignoring RHS as LHS empty", activityName.str());
            strm2.setown(createNullRowStream());
            stopInput2();
        }
        else if (helper->isRightAlreadySorted()) 
        {
            if (rightpartition)
                strm2.set(createUngroupStream(input2));
            else
                strm2.set(input2.get()); // already ungrouped
        }
        else {
            strm2.setown(iLoaderR->load(input2, abortSoon));
            stopInput2();
        }
    }
    bool doglobaljoin()
    {
        Linked<IRowInterfaces> rowif1 = queryRowInterfaces(input1);
        Linked<IRowInterfaces> rowif2 = queryRowInterfaces(input2);
        // NB two near identical branches here - should be parameterized at some stage
        if (rightpartition)
        {
            ActPrintLog("JOIN partition right");
            rowcount_t totalrows;
            collaterev.collate = collate;
            collaterevupper.collate = collateupper;
            if (nosortPrimary())
            {
                OwnedConstThorRow partitionrow  = input2->ungroupedNextRow();
                strm2.set(new cRowStreamPlus1Adaptor(input2,partitionrow));
                sorter->Gather(rowif1,input1,compare1,&collaterev,collateupper?&collaterevupper:NULL,keyserializer2,partitionrow,nosortSecondary(),isUnstable(),abortSoon, rowif2); // keyserializer2 *is* correct
                partitionrow.clear();
                stopInput1();
                if (abortSoon)
                {
                    barrier->cancel();
                    return false;
                }
                ActPrintLog("JOIN waiting barrier.1");
                if (!barrier->wait(false))
                    return false;
                ActPrintLog("JOIN barrier.1 raised");
                strm1.setown(sorter->startMerge(totalrows));
                return true;
            }
            else
            {
                strm2.set(input2);
                sorter->Gather(rowif2,input2,compare2,NULL,NULL,keyserializer2,NULL,false,isUnstable(),abortSoon, NULL); 
                stopInput2();
                if (abortSoon)
                {
                    barrier->cancel();
                    return false;
                }
                ActPrintLog("JOIN waiting barrier.1");
                if (!barrier->wait(false))
                    return false;
                ActPrintLog("JOIN barrier.1 raised"); 
                Owned<IRowStream> rstrm2 =sorter->startMerge(totalrows);

                GetTempName(tempname.clear(),"joinspill",false); // don't use alt temp dir
                Owned<IFile> tempf = createIFile(tempname.str());
                unsigned rwFlags = DEFAULT_RWFLAGS;
                if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
                    rwFlags |= rw_compress;
                Owned<IRowWriter> tmpstrm = createRowWriter(tempf, rowif2, rwFlags);
                if (!tmpstrm)
                {
                    ActPrintLogEx(&queryContainer(), thorlog_null, MCerror, "Cannot open %s", tempname.toCharArray());
                    throw MakeErrnoException("JoinSlaveActivity::doglobaljoin");
                }
                copyRowStream(rstrm2,tmpstrm); 
                tmpstrm->flush();
                tmpstrm.clear();
                rstrm2.clear();
                try
                {
                    strm2.setown(createRowStream(tempf, rowif2, rwFlags));

                    ActPrintLog("JOIN waiting barrier.2");
                    if (!barrier->wait(false))
                        return false;
                    ActPrintLog("JOIN barrier.2 raised");
                    sorter->stopMerge();
                    sorter->Gather(rowif1,input1,compare1,&collaterev,collateupper?&collaterevupper:NULL,keyserializer2,NULL,nosortSecondary(),isUnstable(),abortSoon,rowif2); // keyserializer2 *is* correct
                    stopInput1();
                    if (abortSoon)
                    {
                        barrier->cancel();
                        return false;
                    }
                    ActPrintLog("JOIN waiting barrier.3");
                    if (!barrier->wait(false))
                        return false;
                    ActPrintLog("JOIN barrier.3 raised");
                    strm1.setown(sorter->startMerge(totalrows));
                    return true;
                }
                catch (IException *)
                {
                    if (infh)
                    {
                        _close(infh);
                        infh = 0;
                    }
                    throw;
                }
            }
        }
        else
        {
            rowcount_t totalrows;
            if (nosortPrimary())
            {
                OwnedConstThorRow partitionrow  = input1->ungroupedNextRow();
                strm1.set(new cRowStreamPlus1Adaptor(input1,partitionrow));
                sorter->Gather(rowif2,input2,compare2,collate,collateupper,keyserializer1,partitionrow,nosortSecondary(),isUnstable(),abortSoon, rowif1); // keyserializer1 *is* correct
                partitionrow.clear();
                stopInput2();
                if (abortSoon)
                {
                    barrier->cancel();
                    return false;
                }
                ActPrintLog("JOIN waiting barrier.1");
                if (!barrier->wait(false))
                    return false;
                ActPrintLog("JOIN barrier.1 raised");
                strm2.setown(sorter->startMerge(totalrows));
                return true;
            }
            else
            {
                strm1.set(input1);
                sorter->Gather(rowif1,input1,compare1,NULL,NULL,keyserializer1,NULL,false,isUnstable(),abortSoon, NULL);
                stopInput1();
                if (abortSoon)
                {
                    barrier->cancel();
                    return false;
                }
                ActPrintLog("JOIN waiting barrier.1");
                if (!barrier->wait(false))
                    return false;
                ActPrintLog("JOIN barrier.1 raised");
                Owned<IRowStream> rstrm1 = sorter->startMerge(totalrows);

                // JCSMORE - spill whole of sorted input1 to disk.
                // it could keep in memory until needed to spill..

                GetTempName(tempname.clear(),"joinspill",false); // don't use alt temp dir
                Owned<IFile> tempf = createIFile(tempname.str());
                unsigned rwFlags = DEFAULT_RWFLAGS;
                if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
                    rwFlags |= rw_compress;
                Owned<IRowWriter> tmpstrm = createRowWriter(tempf, rowif1, rwFlags);
                if (!tmpstrm)
                {
                    ActPrintLogEx(&queryContainer(), thorlog_null, MCerror, "Cannot open %s", tempname.toCharArray());
                    throw MakeErrnoException("JoinSlaveActivity::doglobaljoin");
                }
                copyRowStream(rstrm1,tmpstrm); 
                tmpstrm->flush();
                tmpstrm.clear();
                rstrm1.clear();
                try
                {
                    strm1.setown(createRowStream(tempf, rowif1, rwFlags));

                    ActPrintLog("JOIN waiting barrier.2");
                    if (!barrier->wait(false))
                        return false;
                    ActPrintLog("JOIN barrier.2 raised");
                    sorter->stopMerge();
                    sorter->Gather(rowif2,input2,compare2,collate,collateupper,keyserializer1,NULL,nosortSecondary(),isUnstable(),abortSoon,rowif1); // keyserializer1 *is* correct
                    stopInput2();
                    if (abortSoon)
                    {
                        barrier->cancel();
                        return false;
                    }
                    ActPrintLog("JOIN waiting barrier.3");
                    if (!barrier->wait(false))
                        return false;
                    ActPrintLog("JOIN barrier.3 raised");
                    strm2.setown(sorter->startMerge(totalrows));
                    return true;
                }
                catch (IException *)
                {
                    if (infh)
                    {
                        _close(infh);
                        infh = 0;
                    }
                    throw;
                }
            }
        }
        return false;
    }
    virtual void serializeStats(MemoryBuffer &mb)
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
    }
};


//////////////////////


class CMergeJoinSlaveBaseActivity : public CThorNarySlaveActivity, public CThorDataLink, public CThorSteppable
{
    IHThorNWayMergeJoinArg *helper;
    Owned<IEngineRowAllocator> inputAllocator, outputAllocator;

protected:
    CMergeJoinProcessor &processor;

    void afterProcessing();
    void beforeProcessing();

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CMergeJoinSlaveBaseActivity(CGraphElementBase *container, CMergeJoinProcessor &_processor) : CThorNarySlaveActivity(container), CThorDataLink(this), CThorSteppable(this), processor(_processor)
    {
        helper = (IHThorNWayMergeJoinArg *)queryHelper();
        inputAllocator.setown(queryJob().getRowAllocator(helper->queryInputMeta(), queryActivityId()));
        outputAllocator.setown(queryJob().getRowAllocator(helper->queryOutputMeta(), queryActivityId()));
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        CThorNarySlaveActivity::start();

        ForEachItemIn(i1, expandedInputs)
        {
            IThorDataLink *cur = expandedInputs.item(i1);
            Owned<CThorSteppedInput> stepInput = new CThorSteppedInput(cur);
            processor.addInput(stepInput);
        }
        processor.beforeProcessing(inputAllocator, outputAllocator);
        dataLinkStart();
    }
    virtual void stop()
    {
        processor.afterProcessing();
        CThorNarySlaveActivity::stop();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        OwnedConstThorRow ret = processor.nextInGroup();
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        return NULL;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        bool matched = true;
        OwnedConstThorRow next = processor.nextGE(seek, numFields, matched, stepExtra);
        if (next)
            dataLinkIncrement();
        return next.getClear();
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canBufferInput = true;
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    {
        return processor.gatherConjunctions(collector);
    }
    virtual void resetEOF() 
    { 
        processor.queryResetEOF(); 
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CThorNarySlaveActivity::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
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
