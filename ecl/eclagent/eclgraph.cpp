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

using roxiemem::OwnedRoxieString;

//---------------------------------------------------------------------------

static IHThorActivity * createActivity(IAgentContext & agent, unsigned activityId, unsigned subgraphId, unsigned graphId, ThorActivityKind kind, bool isLocal, bool isGrouped, IHThorArg & arg, IPropertyTree * node, EclGraphElement * graphElement)
{
    switch (kind)
    {
    case TAKdiskwrite: 
        return createDiskWriteActivity(agent, activityId, subgraphId, (IHThorDiskWriteArg &)arg, kind);
    case TAKsort: 
        return createGroupSortActivity(agent, activityId, subgraphId, (IHThorSortArg &)arg, kind);
    case TAKdedup: 
        return createGroupDedupActivity(agent, activityId, subgraphId, (IHThorDedupArg &)arg, kind);
    case TAKfilter: 
        return createFilterActivity(agent, activityId, subgraphId, (IHThorFilterArg &)arg, kind);
    case TAKproject: 
        return createProjectActivity(agent, activityId, subgraphId, (IHThorProjectArg &)arg, kind);
    case TAKprefetchproject:
        return createPrefetchProjectActivity(agent, activityId, subgraphId, (IHThorPrefetchProjectArg &)arg, kind);
    case TAKfilterproject : 
        return createFilterProjectActivity(agent, activityId, subgraphId, (IHThorFilterProjectArg &)arg, kind);
    case TAKrollup:
        return createRollupActivity(agent, activityId, subgraphId, (IHThorRollupArg &)arg, kind);
    case TAKiterate:
        return createIterateActivity(agent, activityId, subgraphId, (IHThorIterateArg &)arg, kind);
    case TAKaggregate:
    case TAKexistsaggregate:
    case TAKcountaggregate:
        return createAggregateActivity(agent, activityId, subgraphId, (IHThorAggregateArg &)arg, kind);
    case TAKhashaggregate:
        return createHashAggregateActivity(agent, activityId, subgraphId, (IHThorHashAggregateArg &)arg, kind, isGrouped);
    case TAKfirstn:
        return createFirstNActivity(agent, activityId, subgraphId, (IHThorFirstNArg &)arg, kind);
    case TAKsample:
        return createSampleActivity(agent, activityId, subgraphId, (IHThorSampleArg &)arg, kind);
    case TAKdegroup:
        return createDegroupActivity(agent, activityId, subgraphId, (IHThorDegroupArg &)arg, kind);
    case TAKjoin:
    case TAKjoinlight:
    case TAKdenormalize:
    case TAKdenormalizegroup:
        return createJoinActivity(agent, activityId, subgraphId, (IHThorJoinArg &)arg, kind);
    case TAKselfjoin:
    case TAKselfjoinlight:
        return createSelfJoinActivity(agent, activityId, subgraphId, (IHThorJoinArg &)arg, kind);
    case TAKkeyedjoin:
    case TAKkeyeddenormalize:
    case TAKkeyeddenormalizegroup:
        return createKeyedJoinActivity(agent, activityId, subgraphId, (IHThorKeyedJoinArg &)arg, kind);
    case TAKlookupjoin:
    case TAKlookupdenormalize:
    case TAKlookupdenormalizegroup:
    case TAKsmartjoin:
    case TAKsmartdenormalize:
    case TAKsmartdenormalizegroup:
        return createLookupJoinActivity(agent, activityId, subgraphId, (IHThorHashJoinArg &)arg, kind);
    case TAKalljoin:
    case TAKalldenormalize:
    case TAKalldenormalizegroup:
        return createAllJoinActivity(agent, activityId, subgraphId, (IHThorAllJoinArg &)arg, kind);
    case TAKgroup:
        return createGroupActivity(agent, activityId, subgraphId, (IHThorGroupArg &)arg, kind);
    case TAKworkunitwrite:
        return createWorkUnitWriteActivity(agent, activityId, subgraphId, (IHThorWorkUnitWriteArg &)arg, kind);
    case TAKdictionaryworkunitwrite:
        return createDictionaryWorkUnitWriteActivity(agent, activityId, subgraphId, (IHThorDictionaryWorkUnitWriteArg &)arg, kind);
    case TAKfunnel:
        return createConcatActivity(agent, activityId, subgraphId, (IHThorFunnelArg &)arg, kind);
    case TAKapply:
        return createApplyActivity(agent, activityId, subgraphId, (IHThorApplyArg &)arg, kind);
    case TAKinlinetable:
        return createInlineTableActivity(agent, activityId, subgraphId, (IHThorInlineTableArg &)arg, kind);
    case TAKnormalize:
        return createNormalizeActivity(agent, activityId, subgraphId, (IHThorNormalizeArg &)arg, kind);
    case TAKnormalizechild:
        return createNormalizeChildActivity(agent, activityId, subgraphId, (IHThorNormalizeChildArg &)arg, kind);
    case TAKnormalizelinkedchild:
        return createNormalizeLinkedChildActivity(agent, activityId, subgraphId, (IHThorNormalizeLinkedChildArg &)arg, kind);
    case TAKremoteresult:
        return createRemoteResultActivity(agent, activityId, subgraphId, (IHThorRemoteResultArg &)arg, kind);
    case TAKselectn:
        return createSelectNActivity(agent, activityId, subgraphId, (IHThorSelectNArg &)arg, kind);
    case TAKif:
        return createIfActivity(agent, activityId, subgraphId, (IHThorIfArg &)arg, kind);
    case TAKchildif:
        return createChildIfActivity(agent, activityId, subgraphId, (IHThorIfArg &)arg, kind);
    case TAKchildcase:
        return createCaseActivity(agent, activityId, subgraphId, (IHThorCaseArg &)arg, kind);
    case TAKnull:
        return createNullActivity(agent, activityId, subgraphId, (IHThorNullArg &)arg, kind);
    case TAKdistribution:
        return createDistributionActivity(agent, activityId, subgraphId, (IHThorDistributionArg &)arg, kind);
    case TAKchoosesets:
        return createChooseSetsActivity(agent, activityId, subgraphId, (IHThorChooseSetsArg &)arg, kind);
    case TAKpiperead:
        return createPipeReadActivity(agent, activityId, subgraphId, (IHThorPipeReadArg &)arg, kind);
    case TAKpipewrite:
        return createPipeWriteActivity(agent, activityId, subgraphId, (IHThorPipeWriteArg &)arg, kind);
    case TAKcsvwrite:
        return createCsvWriteActivity(agent, activityId, subgraphId, (IHThorCsvWriteArg &)arg, kind);
    case TAKxmlwrite:
        return createXmlWriteActivity(agent, activityId, subgraphId, (IHThorXmlWriteArg &)arg, kind);
    case TAKpipethrough:
        return createPipeThroughActivity(agent, activityId, subgraphId, (IHThorPipeThroughArg &)arg, kind);
    case TAKchoosesetsenth:
        return createChooseSetsEnthActivity(agent, activityId, subgraphId, (IHThorChooseSetsExArg &)arg, kind);
    case TAKchoosesetslast:
        return createChooseSetsLastActivity(agent, activityId, subgraphId, (IHThorChooseSetsExArg &)arg, kind);
    case TAKfetch:
        return createFetchActivity(agent, activityId, subgraphId, (IHThorFetchArg &)arg, kind);
    case TAKcsvfetch:
        return createCsvFetchActivity(agent, activityId, subgraphId, (IHThorCsvFetchArg &)arg, kind);
    case TAKworkunitread:
        return createWorkunitReadActivity(agent, activityId, subgraphId, (IHThorWorkunitReadArg &)arg, kind);
    case TAKspill:
        return createSpillActivity(agent, activityId, subgraphId, (IHThorSpillArg &)arg, kind);
    case TAKlimit:
        return createLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind);
    case TAKskiplimit:
        return createSkipLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind);
    case TAKcatch:
        return createCatchActivity(agent, activityId, subgraphId, (IHThorCatchArg &)arg, kind);
    case TAKskipcatch:
    case TAKcreaterowcatch:
        return createSkipCatchActivity(agent, activityId, subgraphId, (IHThorCatchArg &)arg, kind);
    case TAKcountproject:
        return createCountProjectActivity(agent, activityId, subgraphId, (IHThorCountProjectArg &)arg, kind);
    case TAKindexwrite:
        return createIndexWriteActivity(agent, activityId, subgraphId, (IHThorIndexWriteArg &)arg, kind);
    case TAKparse:
        return createParseActivity(agent, activityId, subgraphId, (IHThorParseArg &)arg, kind);
    case TAKsideeffect:
        return createSideEffectActivity(agent, activityId, subgraphId, (IHThorSideEffectArg &)arg, kind);
    case TAKsimpleaction:
        return createActionActivity(agent, activityId, subgraphId, (IHThorActionArg &)arg, kind);
    case TAKenth:
        return createEnthActivity(agent, activityId, subgraphId, (IHThorEnthArg &)arg, kind);
    case TAKtopn:
        return createTopNActivity(agent, activityId, subgraphId, (IHThorTopNArg &)arg, kind);
    case TAKxmlparse:
        return createXmlParseActivity(agent, activityId, subgraphId, (IHThorXmlParseArg &)arg, kind);
    case TAKxmlfetch:
        return createXmlFetchActivity(agent, activityId, subgraphId, (IHThorXmlFetchArg &)arg, kind);
    case TAKmerge: 
        return createMergeActivity(agent, activityId, subgraphId, (IHThorMergeArg &)arg, kind);
    case TAKcountdisk: 
        return NULL; // NOTE - activity gets created when needed
    case TAKhttp_rowdataset:
        return createHttpRowCallActivity(agent, activityId, subgraphId, (IHThorHttpCallArg &)arg, kind);
    case TAKsoap_rowdataset:
        return createSoapRowCallActivity(agent, activityId, subgraphId, (IHThorSoapCallArg &)arg, kind);
    case TAKsoap_rowaction:
        return createSoapRowActionActivity(agent, activityId, subgraphId, (IHThorSoapActionArg &)arg, kind);
    case TAKsoap_datasetdataset:
        return createSoapDatasetCallActivity(agent, activityId, subgraphId, (IHThorSoapCallArg &)arg, kind);
    case TAKsoap_datasetaction:
        return createSoapDatasetActionActivity(agent, activityId, subgraphId, (IHThorSoapActionArg &)arg, kind);
    case TAKchilditerator:          
        return createChildIteratorActivity(agent, activityId, subgraphId, (IHThorChildIteratorArg &)arg, kind);
    case TAKlinkedrawiterator:
        return createLinkedRawIteratorActivity(agent, activityId, subgraphId, (IHThorLinkedRawIteratorArg &)arg, kind);
    case TAKrowresult:          
        return createRowResultActivity(agent, activityId, subgraphId, (IHThorRowResultArg &)arg, kind);
    case TAKdatasetresult:          
        return createDatasetResultActivity(agent, activityId, subgraphId, (IHThorDatasetResultArg &)arg, kind);
    case TAKwhen_dataset:
    case TAKwhen_action:
        return createWhenActionActivity(agent, activityId, subgraphId, (IHThorWhenActionArg &)arg, kind, graphElement);
    case TAKsequential:
    case TAKparallel:
    case TAKemptyaction:
    case TAKifaction:
        return createDummyActivity(agent, activityId, subgraphId, arg, kind);
    case TAKhashdedup:
        return createHashDedupActivity(agent, activityId, subgraphId, (IHThorHashDedupArg &)arg, kind);
    case TAKhashdenormalize:
    case TAKhashdistribute:
    case TAKhashdistributemerge:
    case TAKhashjoin:
    case TAKkeyeddistribute:
    case TAKpull:
    case TAKsplit:
        throwUnexpected();  // Code generator should have removed or transformed
    case TAKchildnormalize:
        return createChildNormalizeActivity(agent, activityId, subgraphId, (IHThorChildNormalizeArg &)arg, kind);
    case TAKchildaggregate:
        return createChildAggregateActivity(agent, activityId, subgraphId, (IHThorChildAggregateArg &)arg, kind);
    case TAKchildgroupaggregate:
        return createChildGroupAggregateActivity(agent, activityId, subgraphId, (IHThorChildGroupAggregateArg &)arg, kind);
    case TAKchildthroughnormalize:
        return createChildThroughNormalizeActivity(agent, activityId, subgraphId, (IHThorChildThroughNormalizeArg &)arg, kind);
    case TAKdiskread:
        return createDiskReadActivity(agent, activityId, subgraphId, (IHThorDiskReadArg &)arg, kind);
    case TAKdisknormalize:
        return createDiskNormalizeActivity(agent, activityId, subgraphId, (IHThorDiskNormalizeArg &)arg, kind);
    case TAKdiskaggregate:
        return createDiskAggregateActivity(agent, activityId, subgraphId, (IHThorDiskAggregateArg &)arg, kind);
    case TAKdiskcount:
        return createDiskCountActivity(agent, activityId, subgraphId, (IHThorDiskCountArg &)arg, kind);
    case TAKdiskgroupaggregate:
        return createDiskGroupAggregateActivity(agent, activityId, subgraphId, (IHThorDiskGroupAggregateArg &)arg, kind);
    case TAKindexread:
        return createIndexReadActivity(agent, activityId, subgraphId, (IHThorIndexReadArg &)arg, kind);
    case TAKindexnormalize:
        return createIndexNormalizeActivity(agent, activityId, subgraphId, (IHThorIndexNormalizeArg &)arg, kind);
    case TAKindexaggregate:
        return createIndexAggregateActivity(agent, activityId, subgraphId, (IHThorIndexAggregateArg &)arg, kind);
    case TAKindexcount:
        return createIndexCountActivity(agent, activityId, subgraphId, (IHThorIndexCountArg &)arg, kind);
    case TAKindexgroupaggregate:
    case TAKindexgroupexists:
    case TAKindexgroupcount:
        return createIndexGroupAggregateActivity(agent, activityId, subgraphId, (IHThorIndexGroupAggregateArg &)arg, kind);
    case TAKchilddataset:
    case TAKthroughaggregate:
        UNIMPLEMENTED;
    case TAKcsvread:
        return createCsvReadActivity(agent, activityId, subgraphId, (IHThorCsvReadArg &)arg, kind);
    case TAKxmlread:
        return createXmlReadActivity(agent, activityId, subgraphId, (IHThorXmlReadArg &)arg, kind);
    case TAKlocalresultread:
        return createLocalResultReadActivity(agent, activityId, subgraphId, (IHThorLocalResultReadArg &)arg, kind, graphId);
    case TAKlocalresultwrite:
        return createLocalResultWriteActivity(agent, activityId, subgraphId, (IHThorLocalResultWriteArg &)arg, kind, graphId);
    case TAKdictionaryresultwrite:
        return createDictionaryResultWriteActivity(agent, activityId, subgraphId, (IHThorDictionaryResultWriteArg &)arg, kind, graphId);
    case TAKlocalresultspill:
        return createLocalResultSpillActivity(agent, activityId, subgraphId, (IHThorLocalResultSpillArg &)arg, kind, graphId);
    case TAKcombine:
        return createCombineActivity(agent, activityId, subgraphId, (IHThorCombineArg &)arg, kind);
    case TAKcombinegroup:
        return createCombineGroupActivity(agent, activityId, subgraphId, (IHThorCombineGroupArg &)arg, kind);
    case TAKregroup:
        return createRegroupActivity(agent, activityId, subgraphId, (IHThorRegroupArg &)arg, kind);
    case TAKrollupgroup:
        return createRollupGroupActivity(agent, activityId, subgraphId, (IHThorRollupGroupArg &)arg, kind);
    case TAKfiltergroup:
        return createFilterGroupActivity(agent, activityId, subgraphId, (IHThorFilterGroupArg &)arg, kind);
    case TAKloopcount:
    case TAKlooprow:
    case TAKloopdataset:
        return createLoopActivity(agent, activityId, subgraphId, (IHThorLoopArg &)arg, kind);
    case TAKgraphloop:
        return createGraphLoopActivity(agent, activityId, subgraphId, (IHThorGraphLoopArg &)arg, kind);
    case TAKgraphloopresultread:
        return createGraphLoopResultReadActivity(agent, activityId, subgraphId, (IHThorGraphLoopResultReadArg &)arg, kind, graphId);
    case TAKgraphloopresultwrite:
        return createGraphLoopResultWriteActivity(agent, activityId, subgraphId, (IHThorGraphLoopResultWriteArg &)arg, kind, graphId);
    case TAKprocess:
        return createProcessActivity(agent, activityId, subgraphId, (IHThorProcessArg &)arg, kind);
    case TAKlibrarycall:
        return createLibraryCallActivity(agent, activityId, subgraphId, (IHThorLibraryCallArg &)arg, kind, node);
    case TAKsorted:
        return createSortedActivity(agent, activityId, subgraphId, (IHThorSortedArg &)arg, kind);
    case TAKgrouped:
        return createGroupedActivity(agent, activityId, subgraphId, (IHThorGroupedArg &)arg, kind);
    case TAKnwayjoin:
        return createNWayJoinActivity(agent, activityId, subgraphId, (IHThorNWayMergeJoinArg &)arg, kind);
    case TAKnwaymerge:
        return createNWayMergeActivity(agent, activityId, subgraphId, (IHThorNWayMergeArg &)arg, kind);
    case TAKnwaymergejoin:
        return createNWayMergeJoinActivity(agent, activityId, subgraphId, (IHThorNWayMergeJoinArg &)arg, kind);
    case TAKnwayinput:
        return createNWayInputActivity(agent, activityId, subgraphId, (IHThorNWayInputArg &)arg, kind);
    case TAKnwayselect:
        return createNWaySelectActivity(agent, activityId, subgraphId, (IHThorNWaySelectArg &)arg, kind);
    case TAKnwaygraphloopresultread:
        return createNWayGraphLoopResultReadActivity(agent, activityId, subgraphId, (IHThorNWayGraphLoopResultReadArg &)arg, kind, graphId);
    case TAKnonempty:
        return createNonEmptyActivity(agent, activityId, subgraphId, (IHThorNonEmptyArg &)arg, kind);
    case TAKcreaterowlimit:
        return createOnFailLimitActivity(agent, activityId, subgraphId, (IHThorLimitArg &)arg, kind);
    case TAKexternalsource:
    case TAKexternalsink:
    case TAKexternalprocess:
        return createExternalActivity(agent, activityId, subgraphId, (IHThorExternalArg &)arg, kind, node);
    case TAKstreamediterator:
        return createStreamedIteratorActivity(agent, activityId, subgraphId, (IHThorStreamedIteratorArg &)arg, kind);
    }
    throw MakeStringException(-1, "UNIMPLEMENTED activity '%s'(kind=%d) at %s(%d)", activityKindStr(kind), kind, __FILE__, __LINE__);
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
        {
            IHThorDiskWriteArg * helper = static_cast<IHThorDiskWriteArg *>(arg.get());
            filename.set(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            break;
        }
    }

    Owned<ILocalOrDistributedFile> ldFile = agent.resolveLFN(filename.get(), "Read", true, false, false);
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
        if (file->isFile() != foundYes)
            return false;
        CDateTime modified;
        file->getTime(NULL, &modified, NULL);
        unsigned __int64 pseudoCrc = (unsigned __int64)modified.getSimple();
        return (totalCRC <= pseudoCrc);
    }

    IPropertyTree & cur = f->queryAttributes();
    if ((eclCRC != cur.getPropInt("@eclCRC")) || (totalCRC != cur.getPropInt64("@totalCRC")))
        return false;
    return true;
}

void EclGraphElement::createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * _node)
{
    node.set(_node);
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
                IHThorInput * probe = NULL;

                if (probeManager)
                {
                    if (input.activity)
                    {
                        IInputBase *base = probeManager->createProbe(
                                                        input.queryOutput(branchIndexes.item(i2)),  //input
                                                        input.activity.get(),   //Source act
                                                        activity.get(),         //target activity
                                                        0,//input.id, 
                                                        0,//id, 
                                                        0);
                        probe = dynamic_cast<IHThorInput *>(base);
                    }
                }
                else 
                {
                    probe = subgraph->createLegacyProbe(input.queryOutput(branchIndexes.item(i2)),
                                                    input.id,
                                                    id,
                                                    0,
                                                    agent.queryWorkUnit());
                }
                activity->setInput(i2, probe);
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
            alreadyUpdated = alreadyUpToDate(agent);
            if (alreadyUpdated)
                return false;
            break;
        case TAKif:
            {
                Owned<IHThorArg> helper = createHelper(agent, NULL);
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
                Owned<IHThorArg> helper = createHelper(agent, NULL);
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
                Owned<IHThorArg> helper = createHelper(agent, NULL);
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
                Owned<IHThorArg> helper = createHelper(agent, NULL);
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

void EclGraphElement::updateProgress(IWUGraphProgress &progress)
{
    if(conditionalLink)
    {
        activity->updateProgressForOther(progress, conditionalLink->id, conditionalLink->subgraph->id);
        return;
    }
    if(isSink && activity)
    {
        activity->updateProgress(progress);
    }
}

IHThorException * EclGraphElement::makeWrappedException(IException * e)
{
    throw makeHThorException(kind, id, subgraph->id, e);
}

//---------------------------------------------------------------------------

class GraphRunningState
{
public:
    GraphRunningState(EclGraph & parent, unsigned _id) : graphProgress(parent.getGraphProgress()), id(_id), running(true)
    {
        set(WUGraphRunning);
    }

    ~GraphRunningState()
    {
        if(running)        
            set(WUGraphFailed);
    }

    void complete()
    {
        set(WUGraphComplete);
        running = false;
    }

private:
    void set(WUGraphState state)
    {
        Owned<IWUGraphProgress> progress = graphProgress->update();
        if(id)
            progress->setNodeState(id, state);
        else
            progress->setGraphState(state);
    }

private:
    Owned<IConstWUGraphProgress> graphProgress;
    unsigned id;
    bool running;
};

EclSubGraph::EclSubGraph(IAgentContext & _agent, EclGraph & _parent, EclSubGraph * _owner, unsigned _seqNo, bool enableProbe, CHThorDebugContext * _debugContext, IProbeManager * _probeManager)
    : parent(_parent), owner(_owner), seqNo(_seqNo), probeEnabled(enableProbe), debugContext(_debugContext), probeManager(_probeManager)
{
    executed = false;
    created = false;
    numResults = 0;

    subgraphCodeContext.set(_agent.queryCodeContext());
    subgraphCodeContext.setContainer(this);
    subgraphAgentContext.setCodeContext(&subgraphCodeContext);
    subgraphAgentContext.set(&_agent);

    agent = &_agent;
    isChildGraph = false;
}

void EclSubGraph::createFromXGMML(EclGraph * graph, ILoadedDllEntry * dll, IPropertyTree * node, unsigned * subGraphSeqNo, EclSubGraph * resultsGraph)
{
    xgmml.set(node->queryPropTree("att/graph"));

    id = node->getPropInt("@id", 0);
    bool multiInstance = node->getPropBool("@multiInstance");
    if (multiInstance)
        agent = &subgraphAgentContext;

    isSink = xgmml->getPropBool("att[@name=\"rootGraph\"]/@value", false);
    parentActivityId = node->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
    numResults = xgmml->getPropInt("att[@name=\"_numResults\"]/@value", 0);
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
                childProbe.setown(probeManager->startChildGraph(*subGraphSeqNo, NULL));

            Owned<EclSubGraph> subgraph = new EclSubGraph(*agent, *graph, this, *subGraphSeqNo++, probeEnabled, debugContext, probeManager);
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
        Owned<IConstWUGraphProgress> graphProgress = parent.getGraphProgress();
        Owned<IWUGraphProgress> progress = graphProgress->update();
        updateProgress(*progress);
    }
}

void EclSubGraph::updateProgress(IWUGraphProgress &progress)
{
    if (!isChildGraph)
    {
        ForEachItemIn(idx, elements)
        {
            EclGraphElement & cur = elements.item(idx);
            cur.updateProgress(progress);
        }
        ForEachItemIn(i2, subgraphs)
            subgraphs.item(i2).updateProgress(progress);
    }
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
        throwUnexpected();
        executed = true;
        return;
    }

    ForEachItemIn(idx, elements)
    {
        EclGraphElement & cur = elements.item(idx);
        if (cur.arg)
        {
            try
            {
                cur.arg->onStart(parentExtract, NULL);
            }
            catch(IException * e)
            {
                if (debugContext)
                    debugContext->checkBreakpoint(DebugStateException, NULL, e);
                throw cur.makeWrappedException(e);
            }
        }
    }

    ForEachItemIn(ir, sinks)
        sinks.item(ir).ready();
    ForEachItemIn(ie, sinks)
        sinks.item(ie).execute();
    ForEachItemIn(id, sinks)
        sinks.item(id).done();

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

    unsigned startTime = msTick();
    createActivities();
    if (debugContext)
        debugContext->checkBreakpoint(DebugStateGraphStart, NULL, parent.queryGraphName() );    //debug probes exist so we can now check breakpoints
    if (localResults)
        localResults->clear();
    doExecute(parentExtract, false);

    if(!owner)
        PROGLOG("Completed subgraph %u", id);
    if (!parent.queryLibrary())
    {
        updateProgress();
        cleanupActivities();

        {
            Owned<IWorkUnit> wu(agent->updateWorkUnit());
            StringBuffer timer;
            timer.append("Graph ").append(parent.queryGraphName()).append(" - ").append(seqNo+1).append(" (").append(id).append(")");
            wu->setTimerInfo(timer.str(), NULL, msTick()-startTime, 1, 0);
        }
    }
    agent->updateWULogfile();//Update workunit logfile name in case of rollover
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

void EclSubGraph::getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
{
    localResults->queryResult(id)->getLinkedResult(count, ret);
}


void EclSubGraph::getDictionaryResult(unsigned & count, byte * * & ret, unsigned id)
{
    localResults->queryResult(id)->getLinkedResult(count, ret);
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

void EclGraph::associateSubGraph(EclSubGraph * subgraph)
{
    subgraphMap.setValue(subgraph->id, subgraph);
}

void EclGraph::createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * xgmml, bool enableProbe)
{
    Owned<IPropertyTreeIterator> iter = xgmml->getElements("node");
    unsigned subGraphSeqNo = 0;
    ForEach(*iter)
    {

        Owned<IProbeManager> childProbe;
        if (probeManager)
            childProbe.setown(probeManager->startChildGraph(subGraphSeqNo, NULL));

        Owned<EclSubGraph> subgraph = new EclSubGraph(*agent, *this, NULL, subGraphSeqNo++, enableProbe, debugContext, probeManager);
        subgraph->createFromXGMML(this, dll, &iter->query(), &subGraphSeqNo, NULL);
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
            loop
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
    GraphRunningState * run = NULL;
    if (agent->queryRemoteWorkunit())
        run = new GraphRunningState(*this, 0);

    aindex_t lastSink = -1;
    ForEachItemIn(idx, graphs)
    {
        EclSubGraph & cur = graphs.item(idx);
        if (cur.isSink)
            cur.execute(parentExtract);
    }

    if (run)
    {
        run->complete();
        delete run;
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
                    sink.activity->done();
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
    Owned<IConstWUGraphProgress> graphProgress = getGraphProgress();
    Owned<IWUGraphProgress> progress = graphProgress->update();
    ForEachItemIn(idx, graphs)
    {
        EclSubGraph & cur = graphs.item(idx);
        cur.updateProgress(*progress);
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

void UninitializedGraphResult::getLinkedResult(unsigned & count, byte * * & ret)
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

void GraphResult::getLinkedResult(unsigned & count, byte * * & ret)
{
    unsigned max = rows.ordinality();
    byte * * rowset = rowsetAllocator->createRowset(max);
    unsigned i;
    for (i = 0; i < max; i++)
    {
        const void * next = rows.item(i);
        if (next) LinkRoxieRow(next);
        rowset[i] = (byte *)next;
    }

    count = max;
    ret = rowset;
}

//---------------------------------------------------------------------------

GraphResults::GraphResults(unsigned _maxResults)
{
    results.ensure(_maxResults);
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



IConstWUGraphProgress * EclGraph::getGraphProgress()
{
    return wu->getGraphProgress(queryGraphName());
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
    Owned<IPropertyTree> xgmml = wuGraph->getXGMMLTree(false);

    bool probeEnabled = wuRead->getDebugValueBool("_Probe", false);

    Owned<EclGraph> eclGraph = new EclGraph(*this, graphName, wu, isLibrary, debugContext, probeManager);
    eclGraph->createFromXGMML(dll, xgmml, probeEnabled);
    return eclGraph.getClear();
}

extern IProbeManager *createDebugManager(IDebuggableContext *debugContext, const char *graphName);

#define ABORT_POLL_PERIOD (5*1000)

void EclAgent::executeThorGraph(const char * graphName)
{
    SCMStringBuffer wuid;
    wuRead->getWuid(wuid);

    SCMStringBuffer cluster;
    SCMStringBuffer owner;
    wuRead->getClusterName(cluster);
    wuRead->getUser(owner);
    int priority = wuRead->getPriorityValue();
    unsigned timelimit = queryWorkUnit()->getDebugValueInt("thorConnectTimeout", config->getPropInt("@thorConnectTimeout", 60));
    Owned<IConstWUClusterInfo> c = getTargetClusterInfo(cluster.str());
    if (!c)
        throw MakeStringException(0, "Invalid thor cluster %s", cluster.str());
    SCMStringBuffer queueName;
    c->getThorQueue(queueName);
    Owned<IJobQueue> jq = createJobQueue(queueName.str());

    bool resubmit;
    do // loop if pause interrupted graph and needs resubmitting on resume
    {
        resubmit = false; // set if job interrupted in thor
        class CWorkunitResumeHandler : public CInterface, implements ISDSSubscription
        {
            IConstWorkUnit &wu;
            StringBuffer xpath;
            StringAttr wuid;
            SubscriptionId subId;
            CriticalSection crit;
            Semaphore sem;
            
            void unsubscribe()
            {
                CriticalBlock b(crit);
                if (subId)
                {
                    SubscriptionId _subId = subId;
                    subId = 0;
                    querySDS().unsubscribe(_subId);
                }
            }
        public:
            IMPLEMENT_IINTERFACE;
            CWorkunitResumeHandler(IConstWorkUnit &_wu) : wu(_wu)
            {
                xpath.append("/WorkUnits/");
                SCMStringBuffer istr;
                wu.getWuid(istr);
                wuid.set(istr.str());
                xpath.append(wuid.get()).append("/Action");
                subId = 0;
            }
            ~CWorkunitResumeHandler()
            {
                unsubscribe();
            }
            void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
            {
                CriticalBlock b(crit);
                if (0 == subId) return;
                if (valueLen && valueLen==strlen("resume") && (0 == strncmp("resume", (const char *)valueData, valueLen)))
                    sem.signal();
            }
            bool wait()
            {
                subId = querySDS().subscribe(xpath.str(), *this, false, true);
                assertex(subId);
                PROGLOG("Job %s paused, waiting for resume/abort", wuid.get());
                bool ret = true;
                while (!sem.wait(10000))
                {
                    wu.forceReload();
                    if (WUStatePaused != wu.getState() || wu.aborting())
                    {
                        SCMStringBuffer str;
                        wu.getStateDesc(str);
                        PROGLOG("Aborting pause job %s, state : %s", wuid.get(), str.str());
                        ret = false;
                        break;
                    }
                }
                unsubscribe();
                return ret;
            }
        } workunitResumeHandler(*queryWorkUnit());

        unlockWorkUnit();
        if (WUStatePaused == queryWorkUnit()->getState()) // check initial state - and wait if paused
        {
            if (!workunitResumeHandler.wait())
                throw new WorkflowException(0,"User abort requested", 0, WorkflowException::ABORT, MSGAUD_user);
        }
        {
            Owned <IWorkUnit> w = updateWorkUnit();
            w->setState(WUStateBlocked);
        }
        unlockWorkUnit();
            
        class cPollThread: public Thread
        {
            Semaphore sem;
            bool stopped;
            unsigned starttime;
            IJobQueue *jq;
            IConstWorkUnit *wu;
        public:
            
            bool timedout;
            CTimeMon tm;
            cPollThread(IJobQueue *_jq, IConstWorkUnit *_wu, unsigned timelimit)
                : tm(timelimit)
            {
                stopped = false;
                jq = _jq;
                wu = _wu;
                timedout = false;
            }
            ~cPollThread()
            {
                stop();
            }
            int run()
            {
                while (!stopped) {
                    sem.wait(ABORT_POLL_PERIOD);
                    if (stopped)
                        break;
                    if (tm.timedout()) {
                        timedout = true;
                        stopped = true;
                        jq->cancelInitiateConversation();
                    }
                    else if (wu->aborting()) {
                        stopped = true;
                        jq->cancelInitiateConversation();
                    }

                }
                return 0;
            }
            void stop()
            {
                stopped = true;
                sem.signal();
            }
        } pollthread(jq,wuRead,timelimit*1000);

        pollthread.start();

        PROGLOG("Enqueuing on %s to run wuid=%s, graph=%s, timelimit=%d seconds, priority=%d", queueName.str(), wuid.str(), graphName, timelimit, priority);
        IJobQueueItem* item = createJobQueueItem(wuid.str());
        item->setOwner(owner.str());
        item->setPriority(priority);        
        Owned<IConversation> conversation = jq->initiateConversation(item);
        bool got = conversation.get()!=NULL;
        pollthread.stop();
        pollthread.join();
        if (!got) {
            if (pollthread.timedout)
                throw MakeStringException(0, "Query %s failed to start within specified timelimit (%d) seconds", wuid.str(), timelimit);
            throw MakeStringException(0, "Query %s cancelled (1)",wuid.str());
        }
        // get the thor ep from whoever picked up

        SocketEndpoint thorMaster;
        MemoryBuffer msg;
        if (!conversation->recv(msg,1000*60)) {
            throw MakeStringException(0, "Query %s cancelled (2)",wuid.str());
        }
        thorMaster.deserialize(msg);
        msg.clear().append(graphName);
        SocketEndpoint myep;
        myep.setLocalHost(0);
        myep.serialize(msg);  // only used for tracing
        if (!conversation->send(msg)) {
            StringBuffer s("Failed to send query to Thor on ");
            thorMaster.getUrlStr(s);
            throw MakeStringExceptionDirect(-1, s.str()); // maybe retry?
        }

        StringBuffer eps;
        PROGLOG("Thor on %s running %s",thorMaster.getUrlStr(eps).str(),wuid.str());
        MemoryBuffer reply;
        try
        {
            if (!conversation->recv(reply,INFINITE))
            {
                StringBuffer s("Failed to receive reply from thor ");
                thorMaster.getUrlStr(s);
                throw MakeStringExceptionDirect(-1, s.str());
            }
        }
        catch (IException *e)
        {
            StringBuffer s("Failed to receive reply from thor ");
            thorMaster.getUrlStr(s);
            s.append("; (").append(e->errorCode()).append(", ");
            e->errorMessage(s).append(")");
            throw MakeStringExceptionDirect(-1, s.str());
        }
        ThorReplyCodes replyCode;
        reply.read((unsigned &)replyCode);
        switch (replyCode)
        {
            case DAMP_THOR_REPLY_PAUSED:
            {
                bool isException ;
                reply.read(isException);
                if (isException)
                {
                    Owned<IException> e = deserializeException(reply);
                    StringBuffer str("Pausing job ");
                    SCMStringBuffer istr;
                    queryWorkUnit()->getWuid(istr);
                    str.append(istr.str()).append(" caused exception");
                    EXCLOG(e, str.str());
                }
                Owned <IWorkUnit> w = updateWorkUnit();
                w->setState(WUStatePaused); // will trigger executeThorGraph to pause next time around.
                WUAction action = w->getAction();
                switch (action)
                {
                    case WUActionPause:
                    case WUActionPauseNow:
                        w->setAction(WUActionUnknown);
                }
                resubmit = true; // JCSMORE - all subgraph _could_ be done, thor will check though and not rerun
                break;
            }
            case DAMP_THOR_REPLY_GOOD:
                break;
            case DAMP_THOR_REPLY_ERROR:
            {
                throw deserializeException(reply);
            }
            case DAMP_THOR_REPLY_ABORT:
                throw new WorkflowException(0,"User abort requested", 0, WorkflowException::ABORT, MSGAUD_user);
            default:
                assertex(false);
        }
        reloadWorkUnit();
    }
    while (resubmit); // if pause interrupted job (i.e. with pausenow action), resubmit graph
}

//In case of logfile rollover, update workunit logfile name(s) stored
//in SDS/WorkUnits/{WUID}/Process/EclAgent/myeclagent<log>
void EclAgent::updateWULogfile()
{
    if (logMsgHandler && config->hasProp("@name"))
    {
        StringBuffer logname;
        bool ok = logMsgHandler->getLogName(logname);
        if (ok)
        {
            RemoteFilename rlf;
            rlf.setLocalPath(logname);
            rlf.getRemotePath(logname.clear());

            Owned <IWorkUnit> w = updateWorkUnit();
            w->addProcess("EclAgent", config->queryProp("@name"), GetCurrentProcessId(), logname.str());
        }
        else
        {
            DBGLOG("ERROR: Unable to query logfile name");
            assertex(ok);
        }
    }
}

void EclAgent::executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract)
{
    assertex(parentExtractSize == 0);
    if (realThor)
    {
        if (isStandAloneExe)
            throw MakeStringException(0, "Cannot execute Thor Graph in standalone mode");
        executeThorGraph(graphName);
    }
    else
    {
        try
        {
            PROGLOG("Executing hthor graph %s", graphName);

            if (debugContext)
            {
                probeManager.clear(); // Hack!
                probeManager.setown(createDebugManager(debugContext, graphName));
                debugContext->checkBreakpoint(DebugStateGraphCreate, NULL, graphName);
            }

            activeGraph.setown(loadGraph(graphName, queryWorkUnit(), dll, false));
            unsigned guillotineTimeout = queryWorkUnit()->getDebugValueInt("maxRunTime", 0);
            if (guillotineTimeout)
                abortmonitor->setGuillotineTimeout(guillotineTimeout);
            activeGraph->execute(NULL);
            updateWULogfile();//Update workunit logfile name in case of rollover
            if (guillotineTimeout)
                abortmonitor->setGuillotineTimeout(0);
            if (debugContext)
                debugContext->checkBreakpoint(DebugStateGraphEnd, NULL, graphName);
            activeGraph.clear();
            if (debugContext)
            {
                if (isAborting)
                    debugContext->checkBreakpoint(DebugStateGraphAbort, NULL, NULL);
            }
        }
        catch (WorkflowException *e)
        {
            EXCLOG(e,"EclAgent::executeGraph");
            activeGraph.clear();
            throw;
        }
        catch (IException *e)
        {
            EXCLOG(e,"EclAgent::executeGraph");
            activeGraph.clear();
            throw;
        }
        catch (...)
        {
            PROGLOG("EclAgent::executeGraph unknown exception");
            activeGraph.clear();
            throw;
        }
    }
}

IHThorGraphResults * EclAgent::executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, bool embedded, const byte * parentExtract)
{
    Linked<EclGraph> savedGraph = activeGraph.get();

    try
    {
        EclAgentQueryLibrary * library = loadEclLibrary(libraryName, expectedInterfaceHash, embedded);

        Owned<IHThorGraphResults> libraryResults = new GraphResults;

        activeGraph.set(library->graph);
        activeGraph->executeLibrary(parentExtract, libraryResults);
        activeGraph.set(savedGraph);

        return libraryResults.getClear();
    }
    catch (...)
    {
        activeGraph.set(savedGraph);
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

