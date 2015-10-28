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
#ifndef HTHOR_INCL
#define HTHOR_INCL

#ifdef _WIN32
#ifdef HTHOR_EXPORTS
#define HTHOR_API __declspec(dllexport)
#else
#define HTHOR_API __declspec(dllimport)
#endif
#else
#define HTHOR_API
#endif

#include "eclhelper.hpp"
#include "agentctx.hpp"
#include "roxiemem.hpp"
#include "roxiehelper.hpp"

#define DEFAULT_MEM_LIMIT 300 // in MB (default copied from current value for production roxie)

void HTHOR_API setHThorRowManager(roxiemem::IRowManager * manager); // do not call after the first use (unless you have thought very very hard about this)

class PointerArray;
class EclGraphElement;

inline const void * nextUngrouped(ISimpleInputBase * input)
{
    const void * ret = input->nextInGroup();
    if (!ret)
        ret = input->nextInGroup();
    return ret;
};

interface ISteppedConjunctionCollector;
interface IInputSteppingMeta;
struct IHThorInput : public IInputBase
{
    virtual bool isGrouped() = 0;
    virtual IOutputMetaData * queryOutputMeta() const = 0;

    virtual void ready() = 0;
    virtual void done() = 0;
    virtual void updateProgress(IStatisticGatherer &progress) const = 0;
    virtual const void * nextGE(const void * seek, unsigned numFields) { throwUnexpected(); }   // can only be called on stepping fields.
    virtual IInputSteppingMeta * querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual void resetEOF() { }
};

struct IHThorNWayInput
{
    virtual unsigned numConcreteOutputs() const = 0;
    virtual IHThorInput * queryConcreteInput(unsigned idx) const = 0;
};

struct IHThorBoundLoopGraph;
struct IHThorActivity : implements IActivityBase
{
    virtual void setInput(unsigned, IHThorInput *) = 0;
    virtual IHThorInput *queryOutput(unsigned) = 0;
    virtual void ready() = 0;
    virtual void execute() = 0;
    virtual void done() = 0;
    virtual __int64 getCount() = 0;
    virtual unsigned queryOutputs() = 0;
    virtual void updateProgress(IStatisticGatherer &progress) const = 0;
    virtual void updateProgressForOther(IStatisticGatherer &progress, unsigned otherActivity, unsigned otherSubgraph) const = 0;
    virtual void extractResult(unsigned & len, void * & ret) = 0;
    virtual void setBoundGraph(IHThorBoundLoopGraph * graph) = 0;
};


extern HTHOR_API IHThorActivity *createDiskWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDiskWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIterateActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorIterateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createGroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorGroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createFilterActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorFilterArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createFilterGroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorFilterGroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createRollupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorRollupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createProjectActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorProjectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createPrefetchProjectActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorPrefetchProjectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createFilterProjectActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorFilterProjectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createGroupDedupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDedupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createHashDedupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorHashDedupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createGroupSortActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorSortArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createJoinActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSelfJoinActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorJoinArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLookupJoinActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorHashJoinArg & arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createAllJoinActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorAllJoinArg & arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createWorkUnitWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorWorkUnitWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDictionaryWorkUnitWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryWorkUnitWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createFirstNActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorFirstNArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createInlineTableActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorInlineTableArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createConcatActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorFunnelArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createApplyActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorApplyArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSampleActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorSampleArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDegroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDegroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNormalizeActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNormalizeChildActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeChildArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNormalizeLinkedChildActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNormalizeLinkedChildArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDistributionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDistributionArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createRemoteResultActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorRemoteResultArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChooseSetsActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChooseSetsLastActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChooseSetsEnthActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorChooseSetsExArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createWorkunitReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorWorkunitReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createPipeReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorPipeReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createPipeWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorPipeWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCsvWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCsvWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createXmlWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorXmlWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createPipeThroughActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorPipeThroughArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createFetchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFetchArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createKeyedJoinActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorKeyedJoinArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIfActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIfArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChildIfActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIfArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createHashAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorHashAggregateArg &arg, ThorActivityKind kind, bool _isGroupedHashAggregate);
extern HTHOR_API IHThorActivity *createNullActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorNullArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSideEffectActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSideEffectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorActionArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSelectNActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSelectNArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSpillActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSpillArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSkipLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCatchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSkipCatchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCatchArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createOnFailLimitActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLimitArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCountProjectActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCountProjectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIndexWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorIndexWriteArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCsvFetchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvFetchArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createParseActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorParseArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createEnthActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorEnthArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createTopNActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorTopNArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createXmlParseActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorXmlParseArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createXmlFetchActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorXmlFetchArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createMergeActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorMergeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createHttpRowCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorHttpCallArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSoapRowCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapCallArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSoapRowActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSoapDatasetCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapCallArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createSoapDatasetActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorSoapActionArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChildIteratorActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorChildIteratorArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createRowResultActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorRowResultArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDatasetResultActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDatasetResultArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDummyActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createWhenActionActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &arg, ThorActivityKind kind, EclGraphElement * _graphElement);
extern HTHOR_API IHThorActivity *createChildNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildNormalizeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChildAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChildGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildGroupAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createChildThroughNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorChildThroughNormalizeArg &arg, ThorActivityKind kind);

extern HTHOR_API IHThorActivity *createDiskReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDiskNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskNormalizeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDiskAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDiskCountActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskCountArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createDiskGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorDiskGroupAggregateArg &arg, ThorActivityKind kind);

extern HTHOR_API IHThorActivity *createIndexReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIndexNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexNormalizeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIndexAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIndexCountActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexCountArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createIndexGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexGroupAggregateArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCsvReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCsvReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createXmlReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorXmlReadArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLocalResultReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultReadArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createLocalResultWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultWriteArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createLocalResultSpillActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorLocalResultSpillArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createDictionaryResultWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorDictionaryResultWriteArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createCombineActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCombineArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createRollupGroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorRollupGroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createRegroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorRegroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCombineGroupActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCombineGroupArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLoopActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorLoopArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createGraphLoopActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createCaseActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorCaseArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLinkedRawIteratorActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorLinkedRawIteratorArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createProcessActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorProcessArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createLibraryCallActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorLibraryCallArg &arg, ThorActivityKind kind, IPropertyTree * node);
extern HTHOR_API IHThorActivity *createGraphLoopResultReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultReadArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createGraphLoopResultWriteActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorGraphLoopResultWriteArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createSortedActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorSortedArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createTraceActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorTraceArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createGroupedActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorGroupedArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNWayInputActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNWayInputArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNWayGraphLoopResultReadActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNWayGraphLoopResultReadArg &arg, ThorActivityKind kind, __int64 graphId);
extern HTHOR_API IHThorActivity *createNWayMergeActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNWayMergeJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNWayJoinActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorNWayMergeJoinArg & arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNWaySelectActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNWaySelectArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createNonEmptyActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorNonEmptyArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createStreamedIteratorActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorStreamedIteratorArg &arg, ThorActivityKind kind);
extern HTHOR_API IHThorActivity *createExternalActivity(IAgentContext &, unsigned _activityId, unsigned _subgraphId, IHThorExternalArg &arg, ThorActivityKind kind, IPropertyTree * graphNode);

#define OwnedHThorRowArray OwnedRowArray
class IHThorException : public IException
{
};

extern HTHOR_API IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, int code, char const * format, ...) __attribute__((format(printf, 5, 6)));
extern HTHOR_API IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, IException * exc);
extern HTHOR_API IHThorException * makeHThorException(ThorActivityKind kind, unsigned activityId, unsigned subgraphId, IException * exc, char const * extra);

#endif // HTHOR_INCL
