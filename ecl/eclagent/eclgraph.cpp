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
#include "jmisc.hpp"
#include "jisem.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jprop.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jregexp.hpp"
#include "mplog.hpp"
#include "eclagent.ipp"
#include "deftype.hpp"
#include "hthor.hpp"
#include "dalienv.hpp"
#include "dasds.hpp"

#include "hqlplugins.hpp"
#include "eclrtl.hpp"
#include "wujobq.hpp"
#include "workunit.hpp"
#include "thorfile.hpp"
#include "commonext.hpp"
#include "thorcommon.hpp"

#include <list>
#include <string>
#include <algorithm>

using roxiemem::OwnedRoxieString;

//---------------------------------------------------------------------------

static IHThorActivity * createActivity(IAgentContext & agent, unsigned activityId, unsigned subgraphId, unsigned graphId, ThorActivityKind kind, bool isLocal, bool isGrouped, IHThorArg & arg, IPropertyTree * node, EclGraphElement * graphElement)
{
    EclGraph & graph = graphElement->subgraph->parent;
    switch (kind)
    {
    case TAKdiskwrite:
    case TAKspillwrite:
        return createDiskWriteActivity(agent, activityId, subgraphId, (IHThorGenericDiskWriteArg &)arg, kind, graph);
    case TAKsort:
        return createGroupSortActivity(agent, activityId, subgraphId, (IHThorSortArg &)arg, kind, graph);
    case TAKdedup:
        return createGroupDedupActivity(agent, activityId, subgraphId, (IHThorDedupArg &)arg, kind, graph);
    case TAKfilter:
        return createFilterActivity(agent, activityId, subgraphId, (IHThorFilterArg &)arg, kind, graph);
    case TAKproject:
        return createProjectActivity(agent, activityId, subgraphId, (IHThorProjectArg &)arg, kind, graph);
    case TAKprefetchproject:
        return createPrefetchProjectActivity(agent, activityId, subgraphId, (IHThorPrefetchProjectArg &)arg, kind, graph);
    case TAKfilterproject :
        return createFilterProjectActivity(agent, activityId, subgraphId, (IHThorFilterProjectArg &)arg, kind, graph);
    case TAKrollup:
        return createRollupActivity(agent, activityId, subgraphId, (IHThorRollupArg &)arg, kind, graph);
    case TAKiterate:
        return createIterateActivity(agent, activityId, subgraphId, (IHThorIterateArg &)arg, kind, graph);
    case TAKaggregate:
    case TAKexistsaggregate:
    case TAKcountaggregate:
        return createAggregateActivity(agent, activityId, subgraphId, (IHThorAggregateArg &)arg, kind, graph);
    case TAKhashaggregate:
        return createHashAggregateActivity(agent, activityId, subgraphId, (IHThorHashAggregateArg &)arg, kind, graph, isGrouped);
    case TAKfirstn:
        return createFirstNActivity(agent, activityId, subgraphId, (IHThorFirstNArg &)arg, kind, graph);
    case TAKsample:
        return createSampleActivity(agent, activityId, subgraphId, (IHThorSampleArg &)arg, kind, graph);
    case TAKdegroup:
        return createDegroupActivity(agent, activityId, subgraphId, (IHThorDegroupArg &)arg, kind, graph);
    case TAKjoin:
    case TAKjoinlight:
    case TAKdenormalize:
    case TAKdenormalizegroup:
        return createJoinActivity(agent, activityId, subgraphId, (IHThorJoinArg &)arg, kind, graph);
    case TAKselfjoin:
    case TAKselfjoinlight:
        return createSelfJoinActivity(agent, activityId, subgraphId, (IHThorJoinArg &)arg, kind, graph);
    case TAKkeyedjoin:
    case TAKkeyeddenormalize:
    case TAKkeyeddenormalizegroup:
        return createKeyedJoinActivity(agent, activityId, subgraphId, (IHThorKeyedJoinArg &)arg, kind, graph, node);
    case TAKlookupjoin:
    case TAKlookupdenormalize:
    case TAKlookupdenormalizegroup:
    case TAKsmartjoin:
    case TAKsmartdenormalize:
    case TAKsmartdenormalizegroup:
        return createLookupJoinActivity(agent, activityId, subgraphId, (IHThorHashJoinArg &)arg, kind, graph);
    case TAKalljoin:
    case TAKalldenormalize:
    case TAKalldenormalizegroup:
        return createAllJoinActivity(agent, activityId, subgraphId, (IHThorAllJoinArg &)arg, kind, graph);
    case TAKgroup:
        return createGroupActivity(agent, activityId, subgraphId, (IHThorGroupArg &)arg, kind, graph);
    case TAKworkunitwrite:
        return createWorkUnitWriteActivity(agent, activityId, subgraphId, (IHThorWorkUnitWriteArg &)arg, kind, graph);
    case TAKdictionaryworkunitwrite:
        return createDictionaryWorkUnitWriteActivity(agent, activityId, subgraphId, (IHThorDictionaryWorkUnitWriteArg &)arg, kind, graph);
    case TAKfunnel:
        return createConcatActivity(agent, activityId, subgraphId, (IHThorFunnelArg &)arg, kind, graph);
    case TAKapply:
        return createApplyActivity(agent, activityId, subgraphId, (IHThorApplyArg &)arg, kind, graph);
    case TAKinlinetable:
        return createInlineTableActivity(agent, activityId, subgraphId, (IHThorInlineTableArg &)arg, kind, graph);
    case TAKnormalize:
        return createNormalizeActivity(agent, activityId, subgraphId, (IHThorNormalizeArg &)arg, kind, graph);
    case TAKnormalizechild:
        return createNormalizeChildActivity(agent, activityId, subgraphId, (IHThorNormalizeChildArg &)arg, kind, graph);
    case TAKnormalizelinkedchild:
        return createNormalizeLinkedChildActivity(agent, activityId, subgraphId, (IHThorNormalizeLinkedChildArg &)arg, kind, graph);
    case TAKremoteresult:
        return createRemoteResultActivity(agent, activityId, subgraphId, (IHThorRemoteResultArg &)arg, kind, graph);
    case TAKselectn:
        return createSelectNActivity(agent, activityId, subgraphId, (IHThorSelectNArg &)arg, kind, graph);
    case TAKif:
        return createIfActivity(agent, activityId, subgraphId, (IHThorIfArg &)arg, kind, graph);
    case TAKchildif:
        return createChildIfActivity(agent, activityId, subgraphId, (IHThorIfArg &)arg, kind, graph);
    case TAKchildcase:
        return createCaseActivity(agent, activityId, subgraphId, (IHThorCaseArg &)arg, kind, graph);
    case TAKnull:
        return createNullActivity(agent, activityId, subgraphId, (IHThorNullArg &)arg, kind, graph);
    case TAKdistribution:
        return createDistributionActivity(agent, activityId, subgraphId, (IHThorDistributionArg &)arg, kind, graph);
    case TAKchoosesets:
        return createChooseSetsActivity(agent, activityId, subgraphId, (IHThorChooseSetsArg &)arg, kind, graph);
    case TAKpiperead:
        return createPipeReadActivity(agent, activityId, subgraphId, (IHThorPipeReadArg &)arg, kind, graph);
    case TAKpipewrite:
        return createPipeWriteActivity(agent, activityId, subgraphId, (IHThorPipeWriteArg &)arg, kind, graph);
    case TAKcsvwrite:
        return createCsvWriteActivity(agent, activityId, subgraphId, (IHThorCsvWriteArg &)arg, kind, graph);
    case TAKxmlwrite:
    case TAKjsonwrite:
        return createXmlWriteActivity(agent, activityId, subgraphId, (IHThorXmlWriteArg &)arg, kind, graph);
    case TAKpipethrough:
        return createPipeThroughActivity(agent, activityId, subgraphId, (IHThorPipeThroughArg &)arg, kind, graph);
    case TAKchoosesetsenth:
        return createChooseSetsEnthActivity(agent, activityId, subgraphId, (IHThorChooseSetsExArg &)arg, kind, graph);
    case TAKchoosesetslast:
        return createChooseSetsLastActivity(agent, activityId, subgraphId, (IHThorChooseSetsExArg &)arg, kind, graph);
    case TAKfetch:
        return createFetchActivity(agent, activityId, subgraphId, (IHThorFetchArg &)arg, kind, graph, node);
    case TAKcsvfetch:
        return createCsvFetchActivity(agent, activityId, subgraphId, (IHThorCsvFetchArg &)arg, kind, graph, node);
    case TAKworkunitread:
        return createWorkunitReadActivity(agent, activityId, subgraphId, (IHThorWorkunitReadArg &)arg, kind, graph);
    case TAKspill:
        return createSpillActivity(agent, activityId, subgraphId, (IHThorSpillArg &)arg, kind, graph);
    case TAKlimit:
        return createLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind, graph);
    case TAKskiplimit:
        return createSkipLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind, graph);
    case TAKcatch:
        return createCatchActivity(agent, activityId, subgraphId, (IHThorCatchArg &)arg, kind, graph);
    case TAKskipcatch:
    case TAKcreaterowcatch:
        return createSkipCatchActivity(agent, activityId, subgraphId, (IHThorCatchArg &)arg, kind, graph);
    case TAKcountproject:
        return createCountProjectActivity(agent, activityId, subgraphId, (IHThorCountProjectArg &)arg, kind, graph);
    case TAKindexwrite:
        return createIndexWriteActivity(agent, activityId, subgraphId, (IHThorIndexWriteArg &)arg, kind, graph);
    case TAKparse:
        return createParseActivity(agent, activityId, subgraphId, (IHThorParseArg &)arg, kind, graph);
    case TAKsideeffect:
        return createSideEffectActivity(agent, activityId, subgraphId, (IHThorSideEffectArg &)arg, kind, graph);
    case TAKsimpleaction:
        return createActionActivity(agent, activityId, subgraphId, (IHThorActionArg &)arg, kind, graph);
    case TAKenth:
        return createEnthActivity(agent, activityId, subgraphId, (IHThorEnthArg &)arg, kind, graph);
    case TAKtopn:
        return createTopNActivity(agent, activityId, subgraphId, (IHThorTopNArg &)arg, kind, graph);
    case TAKxmlparse:
        return createXmlParseActivity(agent, activityId, subgraphId, (IHThorXmlParseArg &)arg, kind, graph);
    case TAKxmlfetch:
    case TAKjsonfetch:
        return createXmlFetchActivity(agent, activityId, subgraphId, (IHThorXmlFetchArg &)arg, kind, graph, node);
    case TAKmerge:
        return createMergeActivity(agent, activityId, subgraphId, (IHThorMergeArg &)arg, kind, graph);
    case TAKhttp_rowdataset:
        return createHttpRowCallActivity(agent, activityId, subgraphId, (IHThorHttpCallArg &)arg, kind, graph);
    case TAKsoap_rowdataset:
        return createSoapRowCallActivity(agent, activityId, subgraphId, (IHThorSoapCallArg &)arg, kind, graph);
    case TAKsoap_rowaction:
        return createSoapRowActionActivity(agent, activityId, subgraphId, (IHThorSoapActionArg &)arg, kind, graph);
    case TAKsoap_datasetdataset:
        return createSoapDatasetCallActivity(agent, activityId, subgraphId, (IHThorSoapCallArg &)arg, kind, graph);
    case TAKsoap_datasetaction:
        return createSoapDatasetActionActivity(agent, activityId, subgraphId, (IHThorSoapActionArg &)arg, kind, graph);
    case TAKchilditerator:
        return createChildIteratorActivity(agent, activityId, subgraphId, (IHThorChildIteratorArg &)arg, kind, graph);
    case TAKlinkedrawiterator:
        return createLinkedRawIteratorActivity(agent, activityId, subgraphId, (IHThorLinkedRawIteratorArg &)arg, kind, graph);
    case TAKrowresult:
        return createRowResultActivity(agent, activityId, subgraphId, (IHThorRowResultArg &)arg, kind, graph);
    case TAKdatasetresult:
        return createDatasetResultActivity(agent, activityId, subgraphId, (IHThorDatasetResultArg &)arg, kind, graph);
    case TAKwhen_dataset:
    case TAKwhen_action:
        return createWhenActionActivity(agent, activityId, subgraphId, (IHThorWhenActionArg &)arg, kind, graph, graphElement);
    case TAKsequential:
    case TAKparallel:
    case TAKemptyaction:
    case TAKifaction:
        return createDummyActivity(agent, activityId, subgraphId, arg, kind, graph);
    case TAKhashdedup:
        return createHashDedupActivity(agent, activityId, subgraphId, (IHThorHashDedupArg &)arg, kind, graph);
    case TAKhashdenormalize:
    case TAKhashdistribute:
    case TAKhashdistributemerge:
    case TAKhashjoin:
    case TAKkeyeddistribute:
    case TAKpull:
    case TAKsplit:
        throwUnexpected();  // Code generator should have removed or transformed
    case TAKchildnormalize:
        return createChildNormalizeActivity(agent, activityId, subgraphId, (IHThorChildNormalizeArg &)arg, kind, graph);
    case TAKchildaggregate:
        return createChildAggregateActivity(agent, activityId, subgraphId, (IHThorChildAggregateArg &)arg, kind, graph);
    case TAKchildgroupaggregate:
        return createChildGroupAggregateActivity(agent, activityId, subgraphId, (IHThorChildGroupAggregateArg &)arg, kind, graph);
    case TAKchildthroughnormalize:
        return createChildThroughNormalizeActivity(agent, activityId, subgraphId, (IHThorChildThroughNormalizeArg &)arg, kind, graph);
    case TAKdiskread:
    case TAKspillread:
        return createDiskReadActivity(agent, activityId, subgraphId, (IHThorDiskReadArg &)arg, kind, graph, node);
    case TAKnewdiskread:
        {
            return createNewDiskReadActivity(agent, activityId, subgraphId, (IHThorNewDiskReadArg &)arg, kind, graph, node);
        }
    case TAKdisknormalize:
        return createDiskNormalizeActivity(agent, activityId, subgraphId, (IHThorDiskNormalizeArg &)arg, kind, graph, node);
    case TAKdiskaggregate:
        return createDiskAggregateActivity(agent, activityId, subgraphId, (IHThorDiskAggregateArg &)arg, kind, graph, node);
    case TAKdiskcount:
        return createDiskCountActivity(agent, activityId, subgraphId, (IHThorDiskCountArg &)arg, kind, graph, node);
    case TAKdiskgroupaggregate:
        return createDiskGroupAggregateActivity(agent, activityId, subgraphId, (IHThorDiskGroupAggregateArg &)arg, kind, graph, node);
    case TAKindexread:
        return createIndexReadActivity(agent, activityId, subgraphId, (IHThorIndexReadArg &)arg, kind, graph, node);
    case TAKindexnormalize:
        return createIndexNormalizeActivity(agent, activityId, subgraphId, (IHThorIndexNormalizeArg &)arg, kind, graph, node);
    case TAKindexaggregate:
        return createIndexAggregateActivity(agent, activityId, subgraphId, (IHThorIndexAggregateArg &)arg, kind, graph, node);
    case TAKindexcount:
        return createIndexCountActivity(agent, activityId, subgraphId, (IHThorIndexCountArg &)arg, kind, graph, node);
    case TAKindexgroupaggregate:
    case TAKindexgroupexists:
    case TAKindexgroupcount:
        return createIndexGroupAggregateActivity(agent, activityId, subgraphId, (IHThorIndexGroupAggregateArg &)arg, kind, graph, node);
    case TAKchilddataset:
    case TAKthroughaggregate:
        UNIMPLEMENTED;
    case TAKcsvread:
        return createCsvReadActivity(agent, activityId, subgraphId, (IHThorCsvReadArg &)arg, kind, graph, node);
    case TAKxmlread:
    case TAKjsonread:
        return createXmlReadActivity(agent, activityId, subgraphId, (IHThorXmlReadArg &)arg, kind, graph, node);
    case TAKlocalresultread:
        return createLocalResultReadActivity(agent, activityId, subgraphId, (IHThorLocalResultReadArg &)arg, kind, graph, node->getPropInt("att[@name='_graphId']/@value"));
    case TAKlocalresultwrite:
        return createLocalResultWriteActivity(agent, activityId, subgraphId, (IHThorLocalResultWriteArg &)arg, kind, graph, graphId);
    case TAKdictionaryresultwrite:
        return createDictionaryResultWriteActivity(agent, activityId, subgraphId, (IHThorDictionaryResultWriteArg &)arg, kind, graph, graphId);
    case TAKlocalresultspill:
        return createLocalResultSpillActivity(agent, activityId, subgraphId, (IHThorLocalResultSpillArg &)arg, kind, graph, graphId);
    case TAKcombine:
        return createCombineActivity(agent, activityId, subgraphId, (IHThorCombineArg &)arg, kind, graph);
    case TAKcombinegroup:
        return createCombineGroupActivity(agent, activityId, subgraphId, (IHThorCombineGroupArg &)arg, kind, graph);
    case TAKregroup:
        return createRegroupActivity(agent, activityId, subgraphId, (IHThorRegroupArg &)arg, kind, graph);
    case TAKrollupgroup:
        return createRollupGroupActivity(agent, activityId, subgraphId, (IHThorRollupGroupArg &)arg, kind, graph);
    case TAKfiltergroup:
        return createFilterGroupActivity(agent, activityId, subgraphId, (IHThorFilterGroupArg &)arg, kind, graph);
    case TAKloopcount:
    case TAKlooprow:
    case TAKloopdataset:
        return createLoopActivity(agent, activityId, subgraphId, (IHThorLoopArg &)arg, kind, graph);
    case TAKgraphloop:
        return createGraphLoopActivity(agent, activityId, subgraphId, (IHThorGraphLoopArg &)arg, kind, graph);
    case TAKgraphloopresultread:
        return createGraphLoopResultReadActivity(agent, activityId, subgraphId, (IHThorGraphLoopResultReadArg &)arg, kind, graph, graphId);
    case TAKgraphloopresultwrite:
        return createGraphLoopResultWriteActivity(agent, activityId, subgraphId, (IHThorGraphLoopResultWriteArg &)arg, kind, graph, graphId);
    case TAKprocess:
        return createProcessActivity(agent, activityId, subgraphId, (IHThorProcessArg &)arg, kind, graph);
    case TAKlibrarycall:
        return createLibraryCallActivity(agent, activityId, subgraphId, (IHThorLibraryCallArg &)arg, kind, graph, node);
    case TAKsorted:
        return createSortedActivity(agent, activityId, subgraphId, (IHThorSortedArg &)arg, kind, graph);
    case TAKtrace:
        return createTraceActivity(agent, activityId, subgraphId, (IHThorTraceArg &)arg, kind, graph);
    case TAKgrouped:
        return createGroupedActivity(agent, activityId, subgraphId, (IHThorGroupedArg &)arg, kind, graph);
    case TAKnwayjoin:
        return createNWayJoinActivity(agent, activityId, subgraphId, (IHThorNWayMergeJoinArg &)arg, kind, graph);
    case TAKnwaymerge:
        return createNWayMergeActivity(agent, activityId, subgraphId, (IHThorNWayMergeArg &)arg, kind, graph);
    case TAKnwaymergejoin:
        return createNWayMergeJoinActivity(agent, activityId, subgraphId, (IHThorNWayMergeJoinArg &)arg, kind, graph);
    case TAKnwayinput:
        return createNWayInputActivity(agent, activityId, subgraphId, (IHThorNWayInputArg &)arg, kind, graph);
    case TAKnwayselect:
        return createNWaySelectActivity(agent, activityId, subgraphId, (IHThorNWaySelectArg &)arg, kind, graph);
    case TAKnwaygraphloopresultread:
        return createNWayGraphLoopResultReadActivity(agent, activityId, subgraphId, (IHThorNWayGraphLoopResultReadArg &)arg, kind, graph, graphId);
    case TAKnonempty:
        return createNonEmptyActivity(agent, activityId, subgraphId, (IHThorNonEmptyArg &)arg, kind, graph);
    case TAKcreaterowlimit:
        return createOnFailLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind, graph);
    case TAKexternalsource:
    case TAKexternalsink:
    case TAKexternalprocess:
        return createExternalActivity(agent, activityId, subgraphId, (IHThorExternalArg &)arg, kind, graph, node);
    case TAKstreamediterator:
        return createStreamedIteratorActivity(agent, activityId, subgraphId, (IHThorStreamedIteratorArg &)arg, kind, graph);
    }
    throw MakeStringException(-1, "UNIMPLEMENTED activity '%s'(kind=%d) at %s(%d)", activityKindStr(kind), kind, sanitizeSourceFile(__FILE__), __LINE__);
}

//---------------------------------------------------------------------------

EclGraphElement::EclGraphElement(EclSubGraph * _subgraph, EclSubGraph * _resultsGraph, IProbeManager * _probeManager)
{
    probeManager = _probeManager;
    subgraph = _subgraph;   // do not link
    resultsGraph = _resultsGraph;
    conditionalLink = NULL;
    onlyUpdateIfChanged = false;
    alreadyUpdated = false;
    isEof = false;
}

void EclGraphElement::addDependsOn(EclSubGraph & other, EclGraphElement * sourceActivity, int controlId)
{
    dependentOn.append(other);
    dependentOnActivity.append(*sourceActivity);
    dependentControlId.append(controlId);
}

bool EclGraphElement::alreadyUpToDate(IAgentContext & agent)
{
    if (!onlyUpdateIfChanged)
        return false;

    Owned<IHThorArg> arg = helperFactory();
    EclSubGraph * owner = subgraph->owner;
    EclGraphElement * ownerActivity = owner ? owner->idToActivity(ownerId) : NULL;
    try
    {
        arg->onCreate(agent.queryCodeContext(), ownerActivity ? ownerActivity->arg : NULL, NULL);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

    OwnedRoxieString filename;
    unsigned eclCRC;
    unsigned __int64 totalCRC;
    switch (kind)
    {
    case TAKindexwrite:
        {
            IHThorIndexWriteArg * helper = static_cast<IHThorIndexWriteArg *>(arg.get());
            filename.set(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            break;
        }
    case TAKdiskwrite:
    case TAKcsvwrite:
    case TAKxmlwrite:
    case TAKjsonwrite:
    case TAKspillwrite:
        {
            IHThorGenericDiskWriteArg * helper = static_cast<IHThorGenericDiskWriteArg *>(arg.get());
            filename.set(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            break;
        }
    default:
        UNIMPLEMENTED;
    }

    Owned<ILocalOrDistributedFile> ldFile = agent.resolveLFN(filename.get(), "Read", true, false, AccessMode::tbdRead, nullptr, isCodeSigned);
    if (!ldFile)
        return false;
    IDistributedFile * f = ldFile->queryDistributedFile();
    if (!f)
    {
        Owned<IFileDescriptor> fdesc = ldFile->getFileDescriptor();
        if (fdesc->numParts() != 1)
            return false;
        Owned<IPartDescriptor> part = fdesc->getPart(0);
        RemoteFilename rfn;
        OwnedIFile file = createIFile(part->getFilename(0, rfn));
        if (file->isFile() != fileBool::foundYes)
            return false;
        CDateTime modified;
        file->getTime(NULL, &modified, NULL);
        unsigned __int64 pseudoCrc = (unsigned __int64)modified.getSimple();
        return (totalCRC <= pseudoCrc);
    }

    IPropertyTree & cur = f->queryAttributes();
    if ((eclCRC != (unsigned)cur.getPropInt("@eclCRC")) || (totalCRC != (unsigned __int64)cur.getPropInt64("@totalCRC")))
        return false;
    return true;
}

void EclGraphElement::createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * _node)
{
    node.set(_node);
    if (node)
        isCodeSigned = isActivityCodeSigned(*node);
    kind = (ThorActivityKind)node->getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
    id = node->getPropInt("@id", 0);

    StringBuffer helperFactoryName;
    if(node->hasProp("att[@name=\"helper\"]/@value"))
        helperFactoryName.append(node->queryProp("att[@name=\"helper\"]/@value"));
    else
        helperFactoryName.append("fAc").append(id);
    helperFactory = (EclHelperFactory) dll->getEntry(helperFactoryName.str());

    isLocal = node->getPropBool("att[@name=\"local\"]/@value", false);
    isGrouped = node->getPropBool("att[@name=\"grouped\"]/@value", false);
    isSink = isActivitySink(kind);// && !node->getPropBool("att[@name='_internal']/@value", false);
    isResult = ((kind == TAKdatasetresult) || (kind == TAKrowresult));
    ownerId = node->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
    onlyUpdateIfChanged = node->getPropBool("att[@name=\"_updateIfChanged\"]/@value", false);
}


void EclGraphElement::createActivity(IAgentContext & agent, EclSubGraph * owner)
{
    if (activity)
    {
        //Query libraries reuse the graph, and can call createActivities() multiple times.
        //Do I need to call onCreate() again?  If this is a library query then nothing is context dependent
        return;
    }

    switch (kind)
    {
    case TAKif:
    case TAKcase:
        //NB: These are not allowed inside a query library, so there are no problems with different activities being created.
        if (branches.isItem(whichBranch))
        {
            EclGraphElement & input = branches.item(whichBranch);
            input.createActivity(agent, owner);
            input.conditionalLink = this;
            activity.set(input.activity);
        }
        else
        {
            arg.setown(createHelper(agent, owner));
            activity.setown(::createActivity(agent, id, subgraph->id, resultsGraph ? resultsGraph->id : 0, TAKnull, false, false, *arg, node, this));
        }
        break;
    case TAKlibrarycall:
        //activity call can have multiple children - make sure it isn't created twice
        if (activity)
            break;
        //fallthrough
    default:
        {
            if (!isEof) //dont create unnecessary activities
            {
                ForEachItemIn(i, branches)
                {
                    branches.item(i).createActivity(agent, owner);
                }
            }
            arg.setown(createHelper(agent, owner));
            activity.setown(::createActivity(agent, id, subgraph->id, resultsGraph ? resultsGraph->id : 0, kind, isLocal, isGrouped, *arg, node, this));

            ForEachItemIn(i2, branches)
            {
                EclGraphElement & input = branches.item(i2);
                IHThorInput * useInput = NULL;

                if (probeManager)
                {
                    if (input.activity)
                    {
                        IRoxieProbe *base = probeManager->createProbe(
                                                        input.queryOutput(branchIndexes.item(i2)),  //input
                                                        input.activity.get(),   //Source act
                                                        activity.get(),         //target activity
                                                        0,//input.id,
                                                        0,//id,
                                                        0);
                        useInput = & dynamic_cast<IHThorInput &> (base->queryInput());
                    }
                }
                else
                {
                    useInput = input.queryOutput(branchIndexes.item(i2));
                }
                activity->setInput(i2, useInput);
            }
            break;
        }
    }

    switch (kind)
    {
    case TAKlooprow:
    case TAKloopcount:
    case TAKloopdataset:
    case TAKgraphloop:
        {
            unsigned loopId = node->getPropInt("att[@name=\"_loopid\"]/@value");
            loopGraph.setown(new EclBoundLoopGraph(agent, subgraph->resolveLoopGraph(loopId), arg->queryOutputMeta(), id));
            activity->setBoundGraph(loopGraph);
            break;
        }
    }
}

IHThorArg * EclGraphElement::createHelper(IAgentContext & agent, EclSubGraph * owner)
{
    Owned<IHThorArg> helper = helperFactory();
    assertex(owner == subgraph->owner);
    callOnCreate(*helper, agent);
    return helper.getClear();
}


void EclGraphElement::callOnCreate(IHThorArg & helper, IAgentContext & agent)
{
    EclGraphElement * ownerActivity = subgraph->owner ? subgraph->owner->idToActivity(ownerId) : NULL;
    try
    {
        helper.onCreate(agent.queryCodeContext(), ownerActivity ? ownerActivity->arg : NULL, NULL);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}


void EclGraphElement::extractResult(size32_t & retSize, void * & ret)
{
    assertex(isResult);
    activity->extractResult(retSize, ret);
}

void EclGraphElement::executeDependentActions(IAgentContext & agent, const byte * parentExtract, int controlId)
{
    ForEachItemIn(i, dependentOn)
    {
        if (dependentControlId.item(i) == controlId)
        {
            dependentOn.item(i).execute(parentExtract);
        }
    }
}

bool EclGraphElement::prepare(IAgentContext & agent, const byte * parentExtract, bool checkDependencies)
{
    alreadyUpdated = false;
    if (checkDependencies)
    {
        ForEachItemIn(i, dependentOn)
        {
            if (dependentControlId.item(i) == 0)
                dependentOn.item(i).execute(parentExtract);
        }

        switch (kind)
        {
        case TAKindexwrite:
        case TAKdiskwrite:
        case TAKcsvwrite:
        case TAKxmlwrite:
        case TAKjsonwrite:
        case TAKspillwrite:
            alreadyUpdated = alreadyUpToDate(agent);
            if (alreadyUpdated)
                return false;
            break;
        case TAKif:
            {
                assertex(!subgraph->owner);
                Owned<IHThorArg> helper = createHelper(agent, subgraph->owner);
                try
                {
                    helper->onStart(NULL, NULL);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                whichBranch = ((IHThorIfArg *)helper.get())->getCondition() ? 0 : 1;        // True argument preceeds false...
                if (branches.isItem(whichBranch))
                    return branches.item(whichBranch).prepare(agent, parentExtract, checkDependencies);
                return true;
            }
#if 1
        // This may feel like a worthwhile opimization, but it causes issues with through spill activities.
        // However, disabling it causes issues with unwanted side effects getting evaluated
        case TAKfilter:
        case TAKfiltergroup:
        case TAKfilterproject:
            {
                Owned<IHThorArg> helper = createHelper(agent, subgraph->owner);
                try
                {
                    helper->onStart(parentExtract, NULL);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                switch (kind)
                {
                    case TAKfilter:
                        isEof = !((IHThorFilterArg *)helper.get())->canMatchAny();
                        break;
                    case TAKfiltergroup:
                        isEof = !((IHThorFilterGroupArg *)helper.get())->canMatchAny();
                        break;
                    case TAKfilterproject:
                        isEof = !((IHThorFilterProjectArg *)helper.get())->canMatchAny();
                        break;
                }
                break;
            }
#endif
#if 1
        //This doesn't really work - we really need to switch over to a similar create(),start()/stop(),reset() structure as roxie.
        //However that is far from trivial, so for the moment conditional statements won't be supported by hthor.
        case TAKifaction:
            {
                assertex(!subgraph->owner);
                Owned<IHThorArg> helper = createHelper(agent, subgraph->owner);
                //Problem if this is done in a child query...
                try
                {
                    helper->onStart(parentExtract, NULL);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                whichBranch = ((IHThorIfArg *)helper.get())->getCondition() ? 1 : 2;        // True argument preceeds false...
                executeDependentActions(agent, parentExtract, whichBranch);
                return true;
            }
        case TAKsequential:
        case TAKparallel:
            {
                Owned<IHThorArg> helper = createHelper(agent, subgraph->owner);
                unsigned numBranches = (kind == TAKsequential) ?
                                        ((IHThorSequentialArg *)helper.get())->numBranches() :
                                        ((IHThorParallelArg *)helper.get())->numBranches();
                for (unsigned branch=1; branch <= numBranches; branch++)
                    executeDependentActions(agent, parentExtract, branch);
                return true;
            }
#endif
        case TAKcase:
            {
                assertex(!subgraph->owner);
                Owned<IHThorArg> helper = createHelper(agent, subgraph->owner);
                try
                {
                    helper->onStart(NULL, NULL);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                whichBranch = ((IHThorCaseArg *)helper.get())->getBranch();
                if (whichBranch >= branches.ordinality())
                    whichBranch = branches.ordinality()-1;
                return branches.item(whichBranch).prepare(agent, parentExtract, checkDependencies);
            }
        }

        if (!isEof)    //don't prepare unnecessary branches
        {
            ForEachItemIn(i1, branches)
            {
                if (!branches.item(i1).prepare(agent, parentExtract, checkDependencies))
                    return false;
            }
        }
    }
    return true;
}


IHThorInput * EclGraphElement::queryOutput(unsigned idx)
{
    switch (kind)
    {
    case TAKif:
        if (branches.isItem(whichBranch))
            return branches.item(whichBranch).queryOutput(branchIndexes.item(whichBranch));
        break;
    }

    return activity ? activity->queryOutput(idx) : NULL;
}

void EclGraphElement::updateProgress(IStatisticGatherer &progress)
{
    if(conditionalLink)
    {
        activity->updateProgressForOther(progress, conditionalLink->id, conditionalLink->subgraph->id);
        return;
    }
    if(isSink && activity)
        activity->updateProgress(progress);
}

void EclGraphElement::ready()
{
    if (!activity)
        throw makeHThorException(kind, id, subgraph->id, 99, "Attempt to execute an activity that has not been created");
    if (!alreadyUpdated)
        activity->ready();
}

void EclGraphElement::onStart(const byte * parentExtract, CHThorDebugContext * debugContext)
{
    savedParentExtract = parentExtract;
    if (arg)
    {
        try
        {
            arg->onStart(parentExtract, NULL);
        }
        catch(IException * e)
        {
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateException, NULL, e);
            throw makeWrappedException(e);
        }
    }
}

IHThorException * EclGraphElement::makeWrappedException(IException * e)
{
    throw makeHThorException(kind, id, subgraph->id, e);
}

//---------------------------------------------------------------------------

EclSubGraph::EclSubGraph(IAgentContext & _agent, EclGraph & _parent, EclSubGraph * _owner, unsigned _seqNo, CHThorDebugContext * _debugContext, IProbeManager * _probeManager)
    : seqNo(_seqNo), parent(_parent), owner(_owner), debugContext(_debugContext), probeManager(_probeManager), isLoopBody(false)
{
    executed = false;
    created = false;
    numResults = 0;
    startGraphTime = 0;
    elapsedGraphCycles = 0;

    subgraphCodeContext.set(_agent.queryCodeContext());
    subgraphCodeContext.setWfid(parent.queryWfid());
    subgraphCodeContext.setContainer(this);
    subgraphAgentContext.setCodeContext(&subgraphCodeContext);
    subgraphAgentContext.set(&_agent);

    agent = &_agent;
    isChildGraph = false;
}

void EclSubGraph::createFromXGMML(EclGraph * graph, ILoadedDllEntry * dll, IPropertyTree * node, unsigned & subGraphSeqNo, EclSubGraph * resultsGraph)
{
    xgmml.set(node->queryPropTree("att/graph"));

    id = node->getPropInt("@id", 0);
    bool multiInstance = node->getPropBool("@multiInstance");
    if (multiInstance)
        agent = &subgraphAgentContext;
    isSink = xgmml->getPropBool("att[@name=\"rootGraph\"]/@value", false);
    parentActivityId = node->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
    numResults = xgmml->getPropInt("att[@name=\"_numResults\"]/@value", 0);
    isLoopBody = xgmml->getPropBool("@loopBody",false);
    if (multiInstance || numResults)
    {
        localResults.setown(new GraphResults(numResults));
        resultsGraph = this;
    }

    Owned<IPropertyTreeIterator> iter = xgmml->getElements("node");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        ThorActivityKind kind = (ThorActivityKind)cur.getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
        if (kind == TAKsubgraph)
        {
            Owned<IProbeManager> childProbe;
            if (probeManager)
                childProbe.setown(probeManager->startChildGraph(subGraphSeqNo, NULL));

            Owned<EclSubGraph> subgraph = new EclSubGraph(*agent, *graph, this, subGraphSeqNo++, debugContext, probeManager);
            subgraph->createFromXGMML(graph, dll, &cur, subGraphSeqNo, resultsGraph);
            if (probeManager)
                probeManager->endChildGraph(childProbe, NULL);
            subgraphs.append(*LINK(subgraph));
            graph->associateSubGraph(subgraph);
        }
        else
        {
            Owned<EclGraphElement> newElement = new EclGraphElement(this, resultsGraph, probeManager);
            newElement->createFromXGMML(dll, &cur);
            elements.append(*LINK(newElement));
            if (newElement->isSink)
                sinks.append(*LINK(newElement));
            assertex(newElement->kind != TAKsplit); // If this ever changes we need to reimplement ready()... and start sinks on multi threads.
        }
    }

    Owned<IPropertyTreeIterator> iter2 = xgmml->getElements("edge");
    ForEach(*iter2)
    {
        IPropertyTree &edge = iter2->query();

        if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
            continue;

        unsigned sourceId = edge.getPropInt("@source");
        unsigned targetId = edge.getPropInt("@target");
        unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);

        EclGraphElement * source = idToActivity(sourceId);
        EclGraphElement * target = idToActivity(targetId);

        target->branches.append(*LINK(source));
        target->branchIndexes.append(sourceOutput);
    }
}

void EclSubGraph::createActivities()
{
    if (parentActivityId)
    {
        //Don't create subgraphs for activities that haven't been created because of IFs etc.
        EclGraphElement * ownerActivity = owner->idToActivity(parentActivityId);
        if (!ownerActivity->arg)
            return;
    }

    ForEachItemIn(i, sinks)
    {
        sinks.item(i).createActivity(*agent, owner);
        if (probeManager)
            probeManager->noteSink(dynamic_cast<IActivityBase*>(sinks.item(i).activity.get()));
    }

    ForEachItemIn(i2, subgraphs)
        subgraphs.item(i2).createActivities();

    created = true;
}

void EclSubGraph::cleanupActivities()
{
    created = false;
}

IEclLoopGraph * EclSubGraph::resolveLoopGraph(unsigned id)
{
    return parent.resolveLoopGraph(id);
}

void EclSubGraph::updateProgress()
{
    if (!isChildGraph && agent->queryRemoteWorkunit())
    {
        Owned<IWUGraphStats> progress = parent.updateStats(queryStatisticsComponentType(), queryStatisticsComponentName(), parent.queryWfid(), id);
        IStatisticGatherer & stats = progress->queryStatsBuilder();
        updateProgress(stats);
        Owned<IStatisticCollection> statsCollection = stats.getResult();
        agent->mergeAggregatorStats(*statsCollection, parent.queryWfid(), parent.queryGraphName(), id);
        if (startGraphTime || elapsedGraphCycles)
        {
            WorkunitUpdate lockedwu(agent->updateWorkUnit());
            agent->updateAggregates(lockedwu);
            StringBuffer subgraphid;
            subgraphid.append(parent.queryGraphName()).append(":").append(SubGraphScopePrefix).append(id);
            if (startGraphTime)
                parent.updateWUStatistic(lockedwu, SSTsubgraph, subgraphid, StWhenStarted, nullptr, startGraphTime);
            StringBuffer scope;
            scope.append(WorkflowScopePrefix).append(parent.queryWfid()).append(":").append(subgraphid);
            if (elapsedGraphCycles)
            {
                unsigned __int64 elapsedTime = cycle_to_nanosec(elapsedGraphCycles);
                parent.updateWUStatistic(lockedwu, SSTsubgraph, subgraphid, StTimeElapsed, nullptr, elapsedTime);
                const cost_type cost = money2cost_type(calcCost(agent->queryAgentMachineCost(), nanoToMilli(elapsedTime)));
                if (cost)
                    lockedwu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsubgraph, scope, StCostExecute, NULL, cost, 1, 0, StatsMergeReplace);
            }
        }
    }
}

void EclSubGraph::updateProgress(IStatisticGatherer &progress)
{
    OwnedPtr<StatsScopeBlock> subGraph;
    OwnedPtr<StatsScopeBlock> activityScope;
    if ((isChildGraph || isLoopBody) && owner && parentActivityId != owner->parentActivityId)
    {
        activityScope.setown(new StatsActivityScope(progress, parentActivityId));
        subGraph.setown(new StatsChildGraphScope(progress, id));
    }
    else
        subGraph.setown(new StatsSubgraphScope(progress, id));
    if (startGraphTime)
        progress.addStatistic(StWhenStarted, startGraphTime);
    if (elapsedGraphCycles)
        progress.addStatistic(StTimeElapsed, cycle_to_nanosec(elapsedGraphCycles));
    ForEachItemIn(idx, elements)
    {
        EclGraphElement & cur = elements.item(idx);
        cur.updateProgress(progress);
    }
    ForEachItemIn(i2, subgraphs)
        subgraphs.item(i2).updateProgress(progress);

    Owned<IStatisticCollection> statsCollection = progress.getResult();
    const cost_type costDiskAccess = statsCollection->aggregateStatistic(StCostFileAccess);
    if (costDiskAccess)
        progress.addStatistic(StCostFileAccess, costDiskAccess);
}

bool EclSubGraph::prepare(const byte * parentExtract, bool checkDependencies)
{
    bool needToExecute = false;
    ForEachItemIn(i, sinks)
    {
        if (sinks.item(i).prepare(*agent, parentExtract, checkDependencies))
            needToExecute = true;
    }
    return needToExecute;
}


void EclSubGraph::doExecute(const byte * parentExtract, bool checkDependencies)
{
    if (executed)
        return;

    if (!prepare(parentExtract, checkDependencies))
    {
        executed = true;
        return;
    }

    if (startGraphTime == 0)
        startGraphTime = getTimeStampNowValue();
    cycle_t startGraphCycles = get_cycles_now();
    ForEachItemIn(idx, elements)
    {
        EclGraphElement & cur = elements.item(idx);
        cur.onStart(parentExtract, debugContext);
    }

    ForEachItemIn(ir, sinks)
        sinks.item(ir).ready();
    IException *e = nullptr;
    try
    {
        ForEachItemIn(ie, sinks)
           sinks.item(ie).execute();
    }
    catch (IException * _e)
    {
        e = _e;
    }
    try
    {
       ForEachItemIn(id, sinks)
           sinks.item(id).stop();
    }
    catch (IException *_e)
    {
        if (!e)
            e = _e;
        else
            _e->Release();
    }
    if (e)
        throw e;

    elapsedGraphCycles += (get_cycles_now() - startGraphCycles);
    executed = true;
}


void EclSubGraph::execute(const byte * parentExtract)
{
    if (executed)
        return;
    if(!owner)
        PROGLOG("Executing subgraph %u", id);

    assertex(sinks.ordinality());
    if (!prepare(parentExtract, true))
    {
        executed = true;
        return;
    }

    createActivities();
    if (debugContext)
        debugContext->checkBreakpoint(DebugStateGraphStart, NULL, parent.queryGraphName() );    //debug probes exist so we can now check breakpoints
    if (localResults)
        localResults->clear();
    doExecute(parentExtract, false);

    if(!owner)
    {
        PROGLOG("Completed subgraph %u", id);
        if (!parent.queryLibrary())
        {
            updateProgress();
            cleanupActivities();
            agent->updateWULogfile(nullptr);//Update workunit logfile name in case of rollover
        }
    }
}


void EclSubGraph::executeLibrary(const byte * parentExtract, IHThorGraphResults * results)
{
    reset();
    assertex(sinks.ordinality() == 0);
    localResults.set(results);
    ForEachItemIn(i, subgraphs)
    {
        EclSubGraph & cur = subgraphs.item(i);
        if (cur.isSink)
            cur.execute(parentExtract);
    }
    localResults.clear();
}


void EclSubGraph::executeChild(const byte * parentExtract, IHThorGraphResults * results, IHThorGraphResults * _graphLoopResults)
{
    localResults.set(results);
    graphLoopResults.set(_graphLoopResults);
    doExecuteChild(parentExtract);
    graphLoopResults.clear();
    localResults.clear();
}

IEclGraphResults * EclSubGraph::evaluate(unsigned parentExtractSize, const byte * parentExtract)
{
    CriticalBlock b(evaluateCrit);
    localResults.setown(new GraphResults(numResults));
    localResults->clear();
    doExecuteChild(parentExtract);
    return localResults.getClear();
}


void EclSubGraph::executeSubgraphs(const byte * parentExtract)
{
    ForEachItemIn(i2, subgraphs)
    {
        EclSubGraph & cur = subgraphs.item(i2);
        if (cur.isSink)
        {
            cur.isChildGraph = isChildGraph;
            cur.doExecute(parentExtract, true);
        }
    }
}

void EclSubGraph::doExecuteChild(const byte * parentExtract)
{
    reset();
    if (elements.ordinality() == 0)
        executeSubgraphs(parentExtract);
    else
        doExecute(parentExtract, false);
}

IHThorGraphResult * EclSubGraph::queryResult(unsigned id)
{
    return localResults->queryResult(id);
}

IHThorGraphResult * EclSubGraph::queryGraphLoopResult(unsigned id)
{
    return graphLoopResults->queryResult(id);
}

IHThorGraphResult * EclSubGraph::createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator)
{
    return localResults->createResult(id, ownedRowsetAllocator);
}

IHThorGraphResult * EclSubGraph::createGraphLoopResult(IEngineRowAllocator * ownedRowsetAllocator)
{
    return graphLoopResults->createResult(ownedRowsetAllocator);
}

void EclSubGraph::getLinkedResult(unsigned & count, const byte * * & ret, unsigned id)
{
    localResults->queryResult(id)->getLinkedResult(count, ret);
}


void EclSubGraph::getDictionaryResult(unsigned & count, const byte * * & ret, unsigned id)
{
    localResults->queryResult(id)->getLinkedResult(count, ret);
}

const void * EclSubGraph::getLinkedRowResult(unsigned id)
{
    return localResults->queryResult(id)->getLinkedRowResult();
}


EclGraphElement * EclSubGraph::idToActivity(unsigned id)
{
    ForEachItemIn(idx, elements)
    {
        EclGraphElement & cur = elements.item(idx);
        if (cur.id == id)
            return &cur;
    }
    if (owner)
        return owner->idToActivity(id);
    return NULL;
}

void EclSubGraph::reset()
{
    executed = false;
    ForEachItemIn(i, subgraphs)
        subgraphs.item(i).reset();
}

//---------------------------------------------------------------------------

void EclAgentQueryLibrary::updateProgress()
{
    if (graph)
        graph->updateLibraryProgress();
}

void EclAgentQueryLibrary::destroyGraph()
{
    graph.clear();
}

void EclGraph::associateSubGraph(EclSubGraph * subgraph)
{
    subgraphMap.setValue(subgraph->id, subgraph);
}

void EclGraph::createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * xgmml)
{
    Owned<IPropertyTreeIterator> iter = xgmml->getElements("node");
    unsigned subGraphSeqNo = 0;
    ForEach(*iter)
    {

        Owned<IProbeManager> childProbe;
        if (probeManager)
            childProbe.setown(probeManager->startChildGraph(subGraphSeqNo, NULL));

        Owned<EclSubGraph> subgraph = new EclSubGraph(*agent, *this, NULL, subGraphSeqNo++, debugContext, probeManager);
        subgraph->createFromXGMML(this, dll, &iter->query(), subGraphSeqNo, NULL);
        if (probeManager)
            probeManager->endChildGraph(childProbe, NULL);

        graphs.append(*LINK(subgraph));
        associateSubGraph(subgraph);
    }

    iter.setown(xgmml->getElements("edge"));
    ForEach(*iter)
    {
        IPropertyTree &edge = iter->query();
        EclSubGraph * source = idToGraph(edge.getPropInt("@source"));
        EclSubGraph * target = idToGraph(edge.getPropInt("@target"));
        if (!source)
        {
            DBGLOG("Missing source %d", edge.getPropInt("@source"));
            assertex(source);
        }
        if (!target)
        {
            DBGLOG("Missing target %d", edge.getPropInt("@target"));
            assertex(target);
        }
        assertex(source && target);
        EclGraphElement * targetActivity = target->idToActivity(edge.getPropInt("att[@name=\"_targetActivity\"]/@value"));
        EclGraphElement * sourceActivity = source->idToActivity(edge.getPropInt("att[@name=\"_sourceActivity\"]/@value"));
        int controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
        if (edge.getPropBool("att[@name=\"_dependsOn\"]/@value", false))
        {
            EclSubGraph * sourceGraph = sourceActivity->subgraph;//get owner
            unsigned sourceGraphContext = sourceGraph->parentActivityId;

            EclSubGraph * targetGraph = NULL;
            unsigned targetGraphContext = -1;
            for (;;)
            {
                targetGraph = targetActivity->subgraph;
                targetGraphContext = targetGraph->parentActivityId;
                if (sourceGraphContext == targetGraphContext)
                    break;

                targetActivity = recurseFindActivityFromId(targetGraph, targetGraphContext);
            }
            assertex(targetActivity && sourceActivity);
            targetActivity->addDependsOn(*source, sourceActivity, controlId);
        }
        else if (edge.getPropBool("att[@name=\"_conditionSource\"]/@value", false))
            { /* Ignore it */ }
        else if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
            { /* Ignore it */ }
        else
        {
            assertex(targetActivity);
            targetActivity->addDependsOn(*source, sourceActivity, controlId);
        }
    }
}

void EclGraph::execute(const byte * parentExtract)
{
    if (agent->queryRemoteWorkunit())
        wu->setGraphState(queryGraphName(), wfid, WUGraphRunning);

    {
        Owned<IWorkUnit> wu(agent->updateWorkUnit());
        addTimeStamp(wu, SSTgraph, queryGraphName(), StWhenStarted, wfid);
    }

    try
    {
        unsigned startTime = msTick();
        ForEachItemIn(idx, graphs)
        {
            EclSubGraph & cur = graphs.item(idx);
            if (cur.isSink)
                cur.execute(parentExtract);
        }

        {
            unsigned elapsed = msTick()-startTime;

            Owned<IWorkUnit> wu(agent->updateWorkUnit());

            StringBuffer description;
            formatGraphTimerLabel(description, queryGraphName(), 0, 0);

            unsigned __int64 elapsedNs = milliToNano(elapsed);
            updateWorkunitStat(wu, SSTgraph, queryGraphName(), StTimeElapsed, description.str(), elapsedNs, wfid);

            StringBuffer scope;
            scope.append(WorkflowScopePrefix).append(wfid).append(":").append(queryGraphName());
            const cost_type cost = money2cost_type(calcCost(agent->queryAgentMachineCost(), elapsed));
            if (cost)
                wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, scope, StCostExecute, NULL, cost, 1, 0, StatsMergeReplace);
        }

        if (agent->queryRemoteWorkunit())
            wu->setGraphState(queryGraphName(), wfid, WUGraphComplete);
    }
    catch (...)
    {
        if (agent->queryRemoteWorkunit())
            wu->setGraphState(queryGraphName(), wfid, WUGraphFailed);
        throw;
    }
}

void EclGraph::executeLibrary(const byte * parentExtract, IHThorGraphResults * results)
{
    assertex(graphs.ordinality() == 1);
    EclSubGraph & cur = graphs.item(0);
    cur.executeLibrary(parentExtract, results);
}

void EclGraph::abort()
{
    if (!aborted)
    {
        aborted = true;
        ForEachItemIn(idx, graphs)
        {
            EclSubGraph & graph = graphs.item(idx);
            ForEachItemIn(idx2, graph.sinks)
            {
                EclGraphElement & sink = graph.sinks.item(idx2);
                if (sink.activity)
                    sink.activity->stop();
            }
        }
    }
}

EclGraphElement * EclGraph::idToActivity(unsigned id)
{
    ForEachItemIn(idx, graphs)
    {
        EclGraphElement * match = graphs.item(idx).idToActivity(id);
        if (match)
            return match;
    }
    return NULL;
}


EclSubGraph * EclGraph::idToGraph(unsigned id)
{
    EclSubGraph * * match = subgraphMap.getValue(id);
    if (match)
        return *match;
    return NULL;
}


void EclGraph::updateLibraryProgress()
{
    //Check for old format embedded graph names, and don't update the stats if not the correct format
    if (!MATCHES_CONST_PREFIX(queryGraphName(), GraphScopePrefix))
        return;
    ForEachItemIn(idx, graphs)
    {
        EclSubGraph & cur = graphs.item(idx);
        unsigned wfid = cur.parent.queryWfid();

        Owned<IWUGraphStats> progress =  wu->updateStats(queryGraphName(), queryStatisticsComponentType(), queryStatisticsComponentName(), wfid, cur.id, false);
        IStatisticGatherer & stats = progress->queryStatsBuilder();
        cur.updateProgress(stats);
        Owned<IStatisticCollection> statsCollection = stats.getResult();
        agent->mergeAggregatorStats(*statsCollection, wfid, queryGraphName(), cur.id);
    }
}

//---------------------------------------------------------------------------

void UninitializedGraphResult::addRowOwn(const void * row)
{
    throwUnexpected();
}


//A bit of a hack - would be much cleaner if we link counted the rows like roxie
const void * UninitializedGraphResult::getOwnRow(unsigned whichRow)
{
    throw MakeStringException(99, "Graph Result %d accessed before it is created", id);
}


const void * UninitializedGraphResult::queryRow(unsigned whichRow)
{
    throw MakeStringException(99, "Graph Result %d accessed before it is created", id);
}

void UninitializedGraphResult::getLinkedResult(unsigned & count, const byte * * & ret)
{
    throw MakeStringException(99, "Graph Result %d accessed before it is created", id);
}

const void * UninitializedGraphResult::getLinkedRowResult()
{
    throw MakeStringException(99, "Graph Result %d accessed before it is created", id);
}



void GraphResult::addRowOwn(const void * row)
{
    assertex(meta);
    rows.append(row);
}


const void * GraphResult::getOwnRow(unsigned whichRow)
{
    if (rows.isItem(whichRow))
        return rows.itemClear(whichRow);
    return NULL;
}


const void * GraphResult::queryRow(unsigned whichRow)
{
    if (rows.isItem(whichRow))
        return rows.item(whichRow);
    return NULL;
}

void GraphResult::getLinkedResult(unsigned & count, const byte * * & ret)
{
    unsigned max = rows.ordinality();
    const byte * * rowset = rowsetAllocator->createRowset(max);
    unsigned i;
    for (i = 0; i < max; i++)
    {
        const void * next = rows.item(i);
        if (next) LinkRoxieRow(next);
        rowset[i] = (const byte *)next;
    }

    count = max;
    ret = rowset;
}

const void * GraphResult::getLinkedRowResult()
{
    assertex(rows.ordinality() == 1);
    const void * next = rows.item(0);
    LinkRoxieRow(next);
    return next;
}

//---------------------------------------------------------------------------

GraphResults::GraphResults(unsigned _maxResults)
{
    results.ensureCapacity(_maxResults);
}

void GraphResults::clear()
{
    CriticalBlock procedure(cs);
    results.kill();
//  ForEachItemIn(i, results)
//      results.item(i).clear();
}

void GraphResults::ensureAtleast(unsigned id)
{
    while (results.ordinality() < id)
        results.append(*new UninitializedGraphResult(results.ordinality()));
}

IHThorGraphResult * GraphResults::queryResult(unsigned id)
{
    CriticalBlock procedure(cs);
    ensureAtleast(id+1);
    return &results.item(id);
}

IHThorGraphResult * GraphResults::queryGraphLoopResult(unsigned id)
{
    throwUnexpected();
}

IHThorGraphResult * GraphResults::createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator)
{
    Owned<GraphResult> ret = new GraphResult(ownedRowsetAllocator);
    setResult(id, ret);
    return ret;
}

IHThorGraphResult * GraphResults::createResult(IEngineRowAllocator * ownedRowsetAllocator)
{
    return createResult(results.ordinality(), ownedRowsetAllocator);
}

void GraphResults::setResult(unsigned id, IHThorGraphResult * result)
{
    CriticalBlock procedure(cs);
    ensureAtleast(id);

    if (results.isItem(id))
        results.replace(*LINK(result), id);
    else
        results.append(*LINK(result));
}

//---------------------------------------------------------------------------

IWUGraphStats *EclGraph::updateStats(StatisticCreatorType creatorType, const char * creator, unsigned activeWfid, unsigned subgraph)
{
    return wu->updateStats(queryGraphName(), creatorType, creator, activeWfid, subgraph, false);
}

void EclGraph::updateWUStatistic(IWorkUnit *lockedwu, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * descr, unsigned __int64 value)
{
    updateWorkunitStat(lockedwu, scopeType, scope, kind, descr, value, wfid);
}

IThorChildGraph * EclGraph::resolveChildQuery(unsigned subgraphId)
{
    EclSubGraph * child = idToGraph(subgraphId);
    //GH->WW this looks wrong - it should be known much earlier
    child->isChildGraph = true;
    return LINK(child);
}


//NB: resolveLocalQuery (unlike children) can't link otherwise you get a cicular dependency.
IEclGraphResults * EclGraph::resolveLocalQuery(unsigned subgraphId)
{
    if (subgraphId == 0)
        return &globalResults;
    return idToGraph(subgraphId);
}

IEclLoopGraph * EclGraph::resolveLoopGraph(unsigned subgraphId)
{
    return idToGraph(subgraphId);
}

EclGraphElement * EclGraph::recurseFindActivityFromId(EclSubGraph * subGraph, unsigned id)
{
    ForEachItemIn(idx, subGraph->elements)
    {
        EclGraphElement & cur = subGraph->elements.item(idx);
        if (cur.id == id)
            return &cur;
    }
    return subGraph->owner ? recurseFindActivityFromId(subGraph->owner, id) : NULL;
}


//---------------------------------------------------------------------------

EclGraph * EclAgent::loadGraph(const char * graphName, IConstWorkUnit * wu, ILoadedDllEntry * dll, bool isLibrary)
{
    Owned<IConstWUGraph> wuGraph = wu->getGraph(graphName);
    assertex(wuGraph);
    Owned<IPropertyTree> xgmml = wuGraph->getXGMMLTree(false, false);

    Owned<EclGraph> eclGraph = new EclGraph(*this, graphName, wu, isLibrary, debugContext, probeManager, wuGraph->getWfid());
    eclGraph->createFromXGMML(dll, xgmml);
    statsAggregator.loadExistingAggregates(*wu);
    return eclGraph.getClear();
}

extern IProbeManager *createDebugManager(IDebuggableContext *debugContext, const char *graphName);

//In case of logfile rollover, update logfile name(s) stored in workunit

void EclAgent::updateWULogfile(IWorkUnit *outputWU)
{
    if (logMsgHandler && agentTopology->hasProp("@name"))
    {
        StringBuffer logname;
        bool ok = logMsgHandler->getLogName(logname);
        if (ok)
        {
            RemoteFilename rlf;
            rlf.setLocalPath(logname);
            rlf.getRemotePath(logname.clear());

            Owned<IWorkUnit> w;
            if (!outputWU)
            {
                w.setown(updateWorkUnit());
                outputWU = w;
            }

            outputWU->addProcess("EclAgent", agentTopology->queryProp("@name"), GetCurrentProcessId(), 0, nullptr, false, logname.str());
        }
        else
        {
            DBGLOG("ERROR: Unable to query logfile name");
            assertex(ok);
        }
    }
}
EclGraph * EclAgent::addGraph(const char * graphName)
{
    CriticalBlock thisBlock(activeGraphCritSec);
    EclGraph * graphPtr = loadGraph(graphName, queryWorkUnit(), dll, false);
    activeGraphs.append(graphPtr);
    return graphPtr;
}
void EclAgent::removeGraph(EclGraph * g)
{
    CriticalBlock thisBlock(activeGraphCritSec);
    activeGraphs.zap(g);
}
void EclAgent::executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract)
{
    assertex(parentExtractSize == 0);
    if (realThor)
    {
        if (isStandAloneExe)
            throw MakeStringException(0, "Cannot execute Thor Graph in standalone mode");
        try
        {
            executeThorGraph(graphName, *wuRead, *agentTopology);
            runWorkunitAnalyserAfterGraph(graphName);
        }
        catch (...)
        {
            runWorkunitAnalyserAfterGraph(graphName);
            throw;
        }
    }
    else
    {
        Owned<EclGraph> activeGraph;
        try
        {
            PROGLOG("Executing hthor graph %s", graphName);

            if (debugContext)
            {
                probeManager.clear(); // Hack!
                probeManager.setown(createDebugManager(debugContext, graphName));
                debugContext->checkBreakpoint(DebugStateGraphCreate, NULL, graphName);
            }
            activeGraph.setown(addGraph(graphName));
            unsigned guillotineTimeout = queryWorkUnit()->getDebugValueInt("maxRunTime", 0);
            if (guillotineTimeout)
                abortmonitor->setGuillotineTimeout(guillotineTimeout);
            StringBuffer jobTempDir;
            getTempfileBase(jobTempDir);
            if (!recursiveCreateDirectory(jobTempDir))
                throw MakeStringException(0, "Failed to create temporary directory: %s", jobTempDir.str());
            activeGraph->execute(NULL);
            updateWULogfile(nullptr);//Update workunit logfile name in case of rollover
            if (guillotineTimeout)
                abortmonitor->setGuillotineTimeout(0);
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateGraphEnd, NULL, graphName);
            removeGraph(activeGraph.get());
            if (debugContext)
            {
                if (isAborting)
                    debugContext->checkBreakpoint(DebugStateGraphAbort, NULL, NULL);
            }
        }
        catch (WorkflowException *e)
        {
            EXCLOG(e,"EclAgent::executeGraph");
            removeGraph(activeGraph.get());
            throw;
        }
        catch (IException *e)
        {
            EXCLOG(e,"EclAgent::executeGraph");
            removeGraph(activeGraph.get());
            throw;
        }
        catch (...)
        {
            PROGLOG("EclAgent::executeGraph unknown exception");
            removeGraph(activeGraph.get());
            throw;
        }
    }
}

IHThorGraphResults * EclAgent::executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, const char * embeddedGraphName, const byte * parentExtract)
{
    //Linked<EclGraph> savedGraph = activeGraph.get();
    Owned<EclGraph> activeGraph;
    try
    {
        EclAgentQueryLibrary * library = loadEclLibrary(libraryName, expectedInterfaceHash, embeddedGraphName);

        Owned<IHThorGraphResults> libraryResults = new GraphResults;

        activeGraph.set(library->graph);
        activeGraph->executeLibrary(parentExtract, libraryResults);
        return libraryResults.getClear();
    }
    catch (...)
    {
        throw;
    }
}


IThorChildGraph * EclAgent::resolveChildQuery(__int64 subgraphId, IHThorArg * colocal)
{
    throwUnexpected();
}

IEclGraphResults * EclAgent::resolveLocalQuery(__int64 activityId)
{
    throwUnexpected();
}

EclBoundLoopGraph::EclBoundLoopGraph(IAgentContext & _agent, IEclLoopGraph * _graph, IOutputMetaData * _resultMeta, unsigned _activityId) : agent(_agent)
{
    graph.set(_graph);
    resultMeta.set(_resultMeta);
    activityId = _activityId;
}

IHThorGraphResults * EclBoundLoopGraph::execute(void * counterRow, ConstPointerArray & rows, const byte * parentExtract)
{
    Owned<GraphResults> results = new GraphResults(3);

    if (!inputAllocator)
        inputAllocator.setown(agent.queryCodeContext()->getRowAllocator(resultMeta, activityId));
    IHThorGraphResult * inputResult = results->createResult(1, LINK(inputAllocator));
    ForEachItemIn(i, rows)
        inputResult->addRowOwn(rows.item(i));
    rows.kill();

    if (counterRow)
    {
        counterMeta.setown(new EclCounterMeta);
        if (!counterAllocator)
            counterAllocator.setown(agent.queryCodeContext()->getRowAllocator(counterMeta, activityId));
        IHThorGraphResult * counterResult = results->createResult(2, LINK(counterAllocator));
        counterResult->addRowOwn(counterRow);
    }

    graph->executeChild(parentExtract, results, NULL);
    return results.getClear();
}


void EclBoundLoopGraph::execute(void * counterRow, IHThorGraphResults * graphLoopResults, const byte * parentExtract)
{
    Owned<GraphResults> results = new GraphResults(1);
    if (counterRow)
    {
        counterMeta.setown(new EclCounterMeta);
        if (!counterAllocator)
            counterAllocator.setown(agent.queryCodeContext()->getRowAllocator(counterMeta, activityId));
        IHThorGraphResult * counterResult = results->createResult(0, LINK(counterAllocator));
        counterResult->addRowOwn(counterRow);
    }

    graph->executeChild(parentExtract, results, graphLoopResults);
}

