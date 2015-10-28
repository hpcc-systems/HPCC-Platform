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

#include "platform.h"
#include "jlib.hpp"
#include "jptree.hpp"

#include "commonext.hpp"

#include "thorport.hpp"
#include "thormisc.hpp"
#include "thactivityutil.ipp"
#include "thexception.hpp"
#ifndef _WIN32
#include <stdexcept>
#endif
#include "thorfile.hpp"

#include "thgraphslave.hpp"

#include "slave.ipp"

#include <new>

#define FATAL_ACTJOIN_TIMEOUT (5*60*1000)

activityslaves_decl CGraphElementBase *createSlaveContainer(IPropertyTree &xgmml, CGraphBase &owner, CGraphBase *resultsGraph);
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    registerCreateFunc(&createSlaveContainer);
    return true;
}

//---------------------------------------------------------------------------

// ProcessSlaveActivity

ProcessSlaveActivity::ProcessSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), threaded("ProcessSlaveActivity", this)
{
    processed = 0;
    lastCycles = 0;
}

ProcessSlaveActivity::~ProcessSlaveActivity()
{
    ActPrintLog("destroying ProcessSlaveActivity");
    ActPrintLog("ProcessSlaveActivity : joining process thread");
    // NB: The activity thread should have already stopped,
    //     if it is still alive at job shutdown and cannot be joined then the thread is in an unknown state.
    if (!threaded.join(FATAL_ACTJOIN_TIMEOUT))
        throw MakeThorFatal(NULL, TE_FailedToAbortSlaves, "Activity %" ACTPF "d failed to stop", container.queryId());
    ActPrintLog("AFTER ProcessSlaveActivity : joining process thread");
}

void ProcessSlaveActivity::startProcess(bool async)
{
    if (async)
        threaded.start();
    else
        main();
}

void ProcessSlaveActivity::main() 
{ 
    try
    {
#ifdef TIME_ACTIVITIES
        if (timeActivities)
        {
            {
                SpinBlock b(cycleLock);
                lastCycles = get_cycles_now(); // serializeStats will reset
            }
            process();
            {
                SpinBlock b(cycleLock);
                totalCycles.totalCycles += get_cycles_now()-lastCycles;
                lastCycles = 0; // signal not processing
            }
        }
        else
            process();
#else
        process();
#endif
    }
    catch (IException *_e)
    {
        IThorException *e = QUERYINTERFACE(_e, IThorException);
        if (e)
        {
            if (!e->queryActivityId())
            {
                e->setGraphId(container.queryOwner().queryGraphId());
                e->setActivityKind(container.getKind());
                e->setActivityId(container.queryId());
            }
        }
        else
        {
            e = MakeActivityException(this, _e);
            if (QUERYINTERFACE(_e, ISEH_Exception))
            {
                IThorException *e2 = MakeThorFatal(e, TE_SEH, "FATAL: (SEH)");
                e->Release();
                e = e2;
            }
            _e->Release();
        }
        ActPrintLog(e);
        exception.setown(e);
    }
    catch (std::exception & es)
    {
        StringBuffer m("FATAL std::exception ");
        if(dynamic_cast<std::bad_alloc *>(&es))
            m.append("out of memory (std::bad_alloc)");
        else
            m.append("standard library exception (std::exception ").append(es.what()).append(")");
        m.appendf(" in %" ACTPF "d",container.queryId());
        ActPrintLogEx(&queryContainer(), thorlog_null, MCerror, "%s", m.str());
        exception.setown(MakeThorFatal(NULL, TE_UnknownException, "%s", m.str()));
    }
    catch (CATCHALL)
    {
        ActPrintLogEx(&queryContainer(), thorlog_null, MCerror, "Unknown exception thrown in process()");
        exception.setown(MakeThorFatal(NULL, TE_UnknownException, "FATAL: Unknown exception thrown by ProcessThread"));
    }
    try { endProcess(); }
    catch (IException *e)
    {
        ActPrintLog(e, "Exception calling activity endProcess");
        fireException(e);
        exception.setown(e);
    }
}

bool ProcessSlaveActivity::wait(unsigned timeout)
{
    if (!threaded.join(timeout))
        return false;
    if (exception.get())
        throw exception.getClear();
    return true;
}

void ProcessSlaveActivity::serializeStats(MemoryBuffer &mb)
{
#ifdef TIME_ACTIVITIES
    if (timeActivities)
    {
        SpinBlock b(cycleLock);
        if (lastCycles)
        {
            unsigned __int64 nowCycles = get_cycles_now();
            totalCycles.totalCycles += nowCycles-lastCycles;
            lastCycles = nowCycles; // time accounted for
        }
    }
#endif
    CSlaveActivity::serializeStats(mb);
    mb.append(processed);
}

void ProcessSlaveActivity::done()
{
    CSlaveActivity::done();
    if (exception.get())
        throw exception.getClear();
}


#include "aggregate/thaggregateslave.ipp"
#include "aggregate/thgroupaggregateslave.ipp"
#include "apply/thapplyslave.ipp"
#include "choosesets/thchoosesetsslave.ipp"
#include "countproject/thcountprojectslave.ipp"
#include "degroup/thdegroupslave.ipp"
#include "diskread/thdiskreadslave.ipp"
#include "diskwrite/thdwslave.ipp"
#include "distribution/thdistributionslave.ipp"
#include "enth/thenthslave.ipp"
#include "fetch/thfetchslave.ipp"
#include "filter/thfilterslave.ipp"
#include "firstn/thfirstnslave.ipp"
#include "funnel/thfunnelslave.ipp"
#include "group/thgroupslave.ipp"
#include "hashdistrib/thhashdistribslave.ipp"
#include "indexread/thindexreadslave.ipp"
#include "iterate/thgroupiterateslave.ipp"
#include "iterate/thiterateslave.ipp"
#include "join/thjoinslave.ipp"
#include "keyedjoin/thkeyedjoinslave.ipp"
#include "limit/thlimitslave.ipp"
#include "merge/thmergeslave.ipp"
#include "msort/thgroupsortslave.ipp"
#include "msort/thmsortslave.ipp"
#include "normalize/thnormalizeslave.ipp"
#include "nsplitter/thnsplitterslave.ipp"
#include "null/thnullslave.ipp"
#include "nullaction/thnullactionslave.ipp"
#include "parse/thparseslave.ipp"
#include "piperead/thprslave.ipp"
#include "pipewrite/thpwslave.ipp"
#include "project/thprojectslave.ipp"
#include "pull/thpullslave.ipp"
#include "result/thresultslave.ipp"
#include "rollup/throllupslave.ipp"
#include "sample/thsampleslave.ipp"
#include "selectnth/thselectnthslave.ipp"
#include "selfjoin/thselfjoinslave.ipp"
#include "spill/thspillslave.ipp"
#include "soapcall/thsoapcallslave.ipp"
#include "temptable/thtmptableslave.ipp"
#include "topn/thtopnslave.ipp"
#include "wuidread/thwuidreadslave.ipp"
#include "wuidwrite/thwuidwriteslave.ipp"
#include "xmlwrite/thxmlwriteslave.ipp"

CActivityBase *createLookupJoinSlave(CGraphElementBase *container);
CActivityBase *createAllJoinSlave(CGraphElementBase *container);
CActivityBase *createXmlParseSlave(CGraphElementBase *container);
CActivityBase *createKeyDiffSlave(CGraphElementBase *container);
CActivityBase *createKeyPatchSlave(CGraphElementBase *container);
CActivityBase *createCsvReadSlave(CGraphElementBase *container);
CActivityBase *createXmlReadSlave(CGraphElementBase *container);
CActivityBase *createIndexWriteSlave(CGraphElementBase *container);
CActivityBase *createLoopSlave(CGraphElementBase *container);
CActivityBase *createLocalResultReadSlave(CGraphElementBase *container);
CActivityBase *createLocalResultWriteSlave(CGraphElementBase *container);
CActivityBase *createLocalResultSpillSlave(CGraphElementBase *container);
CActivityBase *createGraphLoopSlave(CGraphElementBase *container);
CActivityBase *createGraphLoopResultReadSlave(CGraphElementBase *container);
CActivityBase *createGraphLoopResultWriteSlave(CGraphElementBase *container);

CActivityBase *createIfSlave(CGraphElementBase *container);
CActivityBase *createCaseSlave(CGraphElementBase *container);
CActivityBase *createCatchSlave(CGraphElementBase *container);
CActivityBase *createChildNormalizeSlave(CGraphElementBase *container);
CActivityBase *createChildAggregateSlave(CGraphElementBase *container);
CActivityBase *createChildGroupAggregateSlave(CGraphElementBase *container);
CActivityBase *createChildThroughNormalizeSlave(CGraphElementBase *container);
CActivityBase *createWhenSlave(CGraphElementBase *container);
CActivityBase *createDictionaryWorkunitWriteSlave(CGraphElementBase *container);
CActivityBase *createDictionaryResultWriteSlave(CGraphElementBase *container);
CActivityBase *createTraceSlave(CGraphElementBase *container);


class CGenericSlaveGraphElement : public CSlaveGraphElement
{
    bool wuidread2diskread; // master decides after interrogating result and sneaks in info before slave creates
    StringAttr wuidreadFilename;
    Owned<CActivityBase> nullActivity;
    CriticalSection nullActivityCs;
public:
    CGenericSlaveGraphElement(CGraphBase &_owner, IPropertyTree &xgmml) : CSlaveGraphElement(_owner, xgmml)
    {
        wuidread2diskread = false;
        switch (getKind())
        {
            case TAKnull:
            case TAKsimpleaction:
            case TAKsideeffect:
                nullAct = true;
                break;
        }
    }
    virtual void deserializeCreateContext(MemoryBuffer &mb)
    {
        CSlaveGraphElement::deserializeCreateContext(mb);
        if (TAKworkunitread == kind)
        {
            mb.read(wuidread2diskread); // have I been converted
            if (wuidread2diskread)
                mb.read(wuidreadFilename);
        }
        haveCreateCtx = true;
    }
    virtual CActivityBase *queryActivity()
    {
        if (hasNullInput)
        {
            CriticalBlock b(nullActivityCs);
            if (!nullActivity)
                nullActivity.setown(createNullSlave(this));
            return nullActivity;
        }
        else
            return activity;
    }
    virtual CActivityBase *factory(ThorActivityKind kind)
    {
        CActivityBase *ret = NULL;
        switch (kind)
        {
            case TAKdiskread:
                ret = createDiskReadSlave(this);
                break;
            case TAKdisknormalize:
                ret = createDiskNormalizeSlave(this);
                break;
            case TAKdiskaggregate:
                ret = createDiskAggregateSlave(this);
                break;
            case TAKdiskcount:
                ret = createDiskCountSlave(this);
                break;
            case TAKdiskgroupaggregate:
                ret = createDiskGroupAggregateSlave(this);
                break;  
            case TAKindexread:
                ret = createIndexReadSlave(this);
                break;
            case TAKindexcount:
                ret = createIndexCountSlave(this);
                break;
            case TAKindexnormalize:
                ret = createIndexNormalizeSlave(this);
                break;
            case TAKindexaggregate:
                ret = createIndexAggregateSlave(this);
                break;
            case TAKindexgroupaggregate:
            case TAKindexgroupexists:
            case TAKindexgroupcount:
                ret = createIndexGroupAggregateSlave(this);
                break;
            case TAKchildaggregate:
                ret = createChildAggregateSlave(this);
                break;
            case TAKchildgroupaggregate:
                ret = createChildGroupAggregateSlave(this);
                break;
            case TAKchildthroughnormalize:
                ret = createChildThroughNormalizeSlave(this);
                break;
            case TAKchildnormalize:
                ret = createChildNormalizeSlave(this);
                break;
            case TAKspill:
                ret = createSpillSlave(this);
                break;
            case TAKdiskwrite:
                ret = createDiskWriteSlave(this);
                break;
            case TAKsort:
                if (queryGrouped() || queryLocal())
                    ret = createLocalSortSlave(this);
                else
                    ret = createMSortSlave(this);
                break;
            case TAKsorted:
                ret = createSortedSlave(this);
                break;
            case TAKtrace:
                ret = createTraceSlave(this);
                break;
            case TAKdedup:
                if (queryGrouped())
                    ret = createGroupDedupSlave(this);
                else if (queryLocal())
                    ret = createLocalDedupSlave(this);
                else
                    ret = createDedupSlave(this);
                break;
            case TAKrollupgroup:
                ret = createRollupGroupSlave(this);
                break;
            case TAKrollup:
                if (queryGrouped())
                    ret = createGroupRollupSlave(this);
                else if (queryLocal())
                    ret = createLocalRollupSlave(this);
                else
                    ret = createRollupSlave(this);
                break;
            case TAKprocess:
                if (queryGrouped())
                    ret = createGroupProcessSlave(this);
                else if (queryLocal())
                    ret = createLocalProcessSlave(this);
                else
                    ret = createProcessSlave(this);
                break;
            case TAKfilter:
                ret = createFilterSlave(this);
                break;
            case TAKfilterproject:
                ret = createFilterProjectSlave(this);
                break;
            case TAKfiltergroup:
                ret = createFilterGroupSlave(this);
                break;
            case TAKsplit:
                ret = createNSplitterSlave(this);
                break;
            case TAKproject:
                ret = createProjectSlave(this);
                break;
            case TAKprefetchproject:
                ret = createPrefetchProjectSlave(this);
                break;
            case TAKprefetchcountproject:
                break;
            case TAKiterate:
                if (queryGrouped())
                    ret = createGroupIterateSlave(this);
                else if (queryLocal())
                    ret = createLocalIterateSlave(this);
                else
                    ret = createIterateSlave(this);
                break;
            case TAKaggregate:
            case TAKexistsaggregate:
            case TAKcountaggregate:
                if (queryLocalOrGrouped())
                    ret = createGroupAggregateSlave(this);
                else
                    ret = createAggregateSlave(this);
                break;
            case TAKhashaggregate:
                ret = createHashAggregateSlave(this);
                break;
            case TAKfirstn:
                ret = createFirstNSlave(this);
                break;
            case TAKsample:
                ret = createSampleSlave(this);
                break;
            case TAKdegroup:
                ret = createDegroupSlave(this);
                break;
            case TAKjoin:
                if (queryLocalOrGrouped())
                    ret = createLocalJoinSlave(this);
                else
                    ret = createJoinSlave(this);
                break;
            case TAKhashjoin:
            case TAKhashdenormalize:
            case TAKhashdenormalizegroup:
                ret = createHashJoinSlave(this);
                break;
            case TAKlookupjoin:
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                ret = createLookupJoinSlave(this);
                break;
            case TAKalljoin:
            case TAKalldenormalize:
            case TAKalldenormalizegroup:
                ret = createAllJoinSlave(this);
                break;
            case TAKselfjoin:
                if (queryLocalOrGrouped())
                    ret = createLocalSelfJoinSlave(this);
                else
                    ret = createSelfJoinSlave(this);
                break;
            case TAKselfjoinlight:
                ret = createLightweightSelfJoinSlave(this);
                break;
            case TAKkeyedjoin:
            case TAKkeyeddenormalize:
            case TAKkeyeddenormalizegroup:
                ret = createKeyedJoinSlave(this);
                break;
            case TAKgroup:
                ret = createGroupSlave(this);
                break;
            case TAKworkunitwrite:
                ret = createWorkUnitWriteSlave(this);
                break;
            case TAKdictionaryworkunitwrite:
                ret = createDictionaryWorkunitWriteSlave(this);
                break;
            case TAKdictionaryresultwrite:
                ret = createDictionaryResultWriteSlave(this);
                break;
            case TAKfunnel:
                ret = createFunnelSlave(this);
                break;
            case TAKcombine:
                ret = createCombineSlave(this);
                break;
            case TAKregroup:
                ret = createRegroupSlave(this);
                break;
            case TAKapply:
                ret = createApplySlave(this);
                break;
            case TAKinlinetable:
                ret = createInlineTableSlave(this);
                break;
            case TAKkeyeddistribute:
                ret = createIndexDistributeSlave(this);
                break;
            case TAKhashdistribute:
                ret = createHashDistributeSlave(this);
                break;
            case TAKdistributed:
                ret = createHashDistributedSlave(this);
                break;
            case TAKhashdistributemerge:
                ret = createHashDistributeMergeSlave(this);
                break;
            case TAKhashdedup:
                if (queryLocalOrGrouped())
                    ret = createHashLocalDedupSlave(this);
                else
                    ret = createHashDedupSlave(this);
                break;
            case TAKnormalize:
                ret = createNormalizeSlave(this);
                break;
            case TAKnormalizechild:
                ret = createNormalizeChildSlave(this);
                break;
            case TAKnormalizelinkedchild:
                ret = createNormalizeLinkedChildSlave(this);
                break;
            case TAKremoteresult:
                ret = createResultSlave(this);
                break;
            case TAKpull:
                ret = createPullSlave(this);
                break;
            case TAKdenormalize:
            case TAKdenormalizegroup:
                if (queryLocalOrGrouped())
                    ret = createLocalDenormalizeSlave(this);
                else
                    ret = createDenormalizeSlave(this);
                break;
            case TAKnwayinput:
                ret = createNWayInputSlave(this);
                break;
            case TAKnwayselect:
                ret = createNWaySelectSlave(this);
                break;
            case TAKnwaymerge:
                ret = createNWayMergeActivity(this);
                break;
            case TAKnwaymergejoin:
            case TAKnwayjoin:
                ret = createNWayMergeJoinActivity(this);
                break;
            case TAKchilddataset:
                UNIMPLEMENTED;
            case TAKchilditerator:
                ret = createChildIteratorSlave(this);
                break;
            case TAKlinkedrawiterator:
                ret = createLinkedRawIteratorSlave(this);
                break;
            case TAKselectn:
                if (queryLocalOrGrouped())
                    ret = createLocalSelectNthSlave(this);
                else
                    ret = createSelectNthSlave(this);
                break;
            case TAKenth:
                if (queryLocalOrGrouped())
                    ret = createLocalEnthSlave(this);
                else
                    ret = createEnthSlave(this);
                break;
            case TAKnull:
                ret = createNullSlave(this);
                break;
            case TAKdistribution:
                ret = createDistributionSlave(this);
                break;
            case TAKcountproject:
                if (queryLocalOrGrouped())
                    ret = createLocalCountProjectSlave(this);
                else
                    ret = createCountProjectSlave(this);
                break;
            case TAKchoosesets:
                if (queryLocalOrGrouped())
                    ret = createLocalChooseSetsSlave(this);
                else
                    ret = createChooseSetsSlave(this);
                break;
            case TAKpiperead:
                ret = createPipeReadSlave(this);
                break;
            case TAKpipewrite:
                ret = createPipeWriteSlave(this);
                break;
            case TAKcsvread:
                ret = createCsvReadSlave(this);
                break;
            case TAKcsvwrite:
                ret = createCsvWriteSlave(this);
                break;
            case TAKpipethrough:
                ret = createPipeThroughSlave(this);
                break;
            case TAKindexwrite:
                ret = createIndexWriteSlave(this);
                break;
            case TAKchoosesetsenth:
                ret = createChooseSetsEnthSlave(this);
                break;
            case TAKchoosesetslast:
                ret = createChooseSetsLastSlave(this);
                break;
            case TAKfetch:
                ret = createFetchSlave(this);
                break;
            case TAKcsvfetch:
                ret = createCsvFetchSlave(this);
                break;
            case TAKxmlfetch:
                ret = createXmlFetchSlave(this);
                break;
            case TAKthroughaggregate:
                ret = createThroughAggregateSlave(this);
                break;
            case TAKcase:
            case TAKif:
            case TAKifaction:
                throwUnexpected();
                break;
            case TAKwhen_dataset:
                ret = createWhenSlave(this);
                break;
            case TAKworkunitread:
            {
                if (wuidread2diskread)
                {
                    Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(wuidreadFilename, (IHThorWorkunitReadArg *)LINK(baseHelper));
                    Owned<CActivityBase> retAct = createDiskReadSlave(this, diskReadHelper);
                    return retAct.getClear();
                }
                else
                    ret = createWuidReadSlave(this);
                break;
            }
            case TAKparse:
                ret = createParseSlave(this);
                break;
            case TAKsideeffect:
                ret = createNullActionSlave(this);
                break;
            case TAKsimpleaction:
                ret = createNullSlave(this);
                break;
            case TAKtopn:
                if (queryGrouped())
                    ret = createGroupedTopNSlave(this);
                else if (queryLocal())
                    ret = createLocalTopNSlave(this);
                else
                    ret = createGlobalTopNSlave(this);
                break;
            case TAKxmlparse:
                ret = createXmlParseSlave(this);
                break;
            case TAKxmlread:
            case TAKjsonread:
                ret = createXmlReadSlave(this);
                break;
            case TAKxmlwrite:
            case TAKjsonwrite:
                ret = createXmlWriteSlave(this, kind);
                break;
            case TAKmerge:
                if (queryLocalOrGrouped())
                    ret = createLocalMergeSlave(this);
                else
                    ret = createGlobalMergeSlave(this);
                break;
            case TAKsoap_rowdataset:
                ret = createSoapRowCallSlave(this);
                break;
            case TAKhttp_rowdataset:
                ret = createHttpRowCallSlave(this);
                break;
            case TAKsoap_rowaction:
                ret = createSoapRowActionSlave(this);
                break;
            case TAKsoap_datasetdataset:
                ret = createSoapDatasetCallSlave(this);
                break;
            case TAKsoap_datasetaction:
                ret = createSoapDatasetActionSlave(this);
                break;
            case TAKkeydiff:
                ret = createKeyDiffSlave(this);
                break;
            case TAKkeypatch:
                ret = createKeyPatchSlave(this);
                break;
            case TAKlimit:
                ret = createLimitSlave(this);
                break;
            case TAKskiplimit:
                ret = createSkipLimitSlave(this);
                break;
            case TAKcreaterowlimit:
                ret = createRowLimitSlave(this);
                break;
            case TAKnonempty:
                ret = createNonEmptySlave(this);
                break;
            case TAKlocalresultread:
                ret = createLocalResultReadSlave(this);
                break;
            case TAKlocalresultwrite:
                ret = createLocalResultWriteSlave(this);
                break;
            case TAKlocalresultspill:
                ret = createLocalResultSpillSlave(this);
                break;
            case TAKchildif:
                ret = createIfSlave(this);
                break;
            case TAKchildcase:
                ret = createCaseSlave(this);
                break;
            case TAKcatch:
            case TAKskipcatch:
            case TAKcreaterowcatch:
                ret = createCatchSlave(this);
                break;
            case TAKlooprow:
            case TAKloopcount:
            case TAKloopdataset:
                ret = createLoopSlave(this);
                break;
            case TAKgraphloop:
            case TAKparallelgraphloop:
                ret = createGraphLoopSlave(this);
                break;
            case TAKgraphloopresultread:
                ret = createGraphLoopResultReadSlave(this);
                break;
            case TAKgraphloopresultwrite:
                ret = createGraphLoopResultWriteSlave(this);
                break;
            case TAKstreamediterator:
                ret = createStreamedIteratorSlave(this);
                break;
            default:
                throw MakeStringException(TE_UnsupportedActivityKind, "Unsupported activity kind: %s", activityKindStr(kind));
        }
        return ret;
    }
};

activityslaves_decl CGraphElementBase *createSlaveContainer(IPropertyTree &xgmml, CGraphBase &owner, CGraphBase *resultsGraph)
{
    return new CGenericSlaveGraphElement(owner, xgmml);
}

activityslaves_decl IRowInterfaces *queryRowInterfaces(IThorDataLink *link) { return link?link->queryFromActivity():NULL; }
activityslaves_decl IEngineRowAllocator * queryRowAllocator(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryRowAllocator():NULL; }
activityslaves_decl IOutputRowSerializer * queryRowSerializer(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryRowSerializer():NULL; }
activityslaves_decl IOutputRowDeserializer * queryRowDeserializer(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryRowDeserializer():NULL; }
activityslaves_decl IOutputMetaData *queryRowMetaData(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryRowMetaData():NULL; }
activityslaves_decl unsigned queryActivityId(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryId():0; }
activityslaves_decl ICodeContext *queryCodeContext(IThorDataLink *link) { CActivityBase *base = link?link->queryFromActivity():NULL; return base?base->queryCodeContext():NULL; }


void dummyProc()    // to force static linking
{
}

