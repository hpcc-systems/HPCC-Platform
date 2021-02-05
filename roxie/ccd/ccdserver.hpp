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

#ifndef _CCDSERVER_INCL
#define _CCDSERVER_INCL

#include "ccdactivities.hpp"
#include "ccdfile.hpp"
#include "roxiemem.hpp"
#include "workflow.hpp"
#include "roxiehelper.hpp"
#include "roxiedebug.hpp"
#include "thorstep.hpp"

interface ILRUChain
{
    virtual ILRUChain *queryPrev() const = 0;
    virtual ILRUChain *queryNext() const = 0;
    virtual void setPrev(ILRUChain *) = 0;
    virtual void setNext(ILRUChain *) = 0;
    virtual void unchain() = 0;
};

interface IRoxieServerQueryPacket : public IInterface, public ILRUChain
{
    virtual IRoxieQueryPacket *queryPacket() const = 0;
    virtual bool hasResult() const = 0;
    virtual IMessageResult *getResult() = 0;
    virtual IMessageResult *queryResult() = 0;
    virtual void setResult(IMessageResult *r) = 0;
    virtual IRoxieServerQueryPacket *queryContinuation() = 0;
    virtual void setContinuation(IRoxieServerQueryPacket *c) = 0;
    virtual unsigned queryHash() const = 0;
    virtual void setHash(unsigned hash) = 0;
    virtual bool isEnd() const = 0;
    virtual bool isLimit(unsigned __int64 &_rowLimit, unsigned __int64 &_keyedLimit, unsigned __int64 &_stopAfter) const = 0;
    virtual bool isContinuation() const = 0;
    virtual bool isDelayed() const = 0;
    virtual unsigned getSequence() const = 0;
    virtual void setDelayed(bool _delayed) = 0;
    virtual void setPacket(IRoxieQueryPacket *packet) = 0;
    virtual void setSequence(unsigned _seq) = 0;

    virtual IRoxieQueryPacket *getDebugResponse(unsigned sequence) = 0;
    virtual void setDebugResponse(unsigned sequence, IRoxieQueryPacket *) = 0;
};

interface IProbeManager;
interface IRoxieServerLoopResultProcessor;

interface IActivityGraph;
typedef MapBetween<unsigned, unsigned, unsigned, unsigned> MapIdToActivityIndex;

class LibraryCallFactoryExtra
{
public:
    void calcUnused();
    void set(const LibraryCallFactoryExtra & _other);
    
public:
    UnsignedArray outputs;
    UnsignedArray unusedOutputs;
    unsigned maxOutputs;
    unsigned graphid;

    StringAttr libraryName;
    StringAttr embeddedGraphName;
    unsigned interfaceHash;
    bool embedded;
};

interface IRoxieServerActivity;
interface IRoxieServerChildGraph;
interface IRoxieServerContext;
interface IRoxieAgentContext;
interface IStrandJunction;

class ClusterWriteHandler;

class StrandOptions;
interface IOrderedCallbackCollection;
interface IFinalRoxieInput : extends IInputBase
{
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual void reset() = 0;

    virtual unsigned __int64 queryTotalCycles() const = 0;
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual unsigned numConcreteOutputs() const { return 1; }
    virtual IFinalRoxieInput * queryConcreteInput(unsigned idx) { assertex(idx==0); return this; }
    virtual IEngineRowStream *queryConcreteOutputStream(unsigned whichInput) = 0;
    virtual IStrandJunction *queryConcreteOutputJunction(unsigned idx) const = 0;
    virtual IRoxieServerActivity *queryActivity() = 0;
    virtual IIndexReadActivityInfo *queryIndexReadActivity() = 0;

    virtual IStrandJunction *getOutputStreams(IRoxieAgentContext *ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const StrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) = 0;  // Use StrandFlags values for flags

    inline void stopall()
    {
        for (unsigned i = 0; i < numConcreteOutputs(); i++)
            queryConcreteOutputStream(i)->stop();
    }
};

extern IEngineRowStream *connectSingleStream(IRoxieAgentContext *ctx, IFinalRoxieInput *input, unsigned idx, Owned<IStrandJunction> &junction, bool consumerOrdered);

interface ISteppedConjunctionCollector;

interface IIndexReadActivityInfo
{
    virtual IKeyArray *getKeySet() const = 0;
    virtual const IResolvedFile *getVarFileInfo() const = 0;
    virtual ITranslatorSet *getTranslators() const = 0;

    virtual void mergeSegmentMonitors(IIndexReadContext *irc) const = 0;
    virtual IRoxieServerActivity *queryActivity() = 0;
    virtual const RemoteActivityId &queryRemoteId() const = 0;
};

interface IRoxieServerErrorHandler
{
    virtual void onLimitExceeded(bool isKeyed) = 0;
    virtual const void *createLimitFailRow(bool isKeyed) = 0;
};

interface IRoxieServerActivityFactory;
interface IRoxiePackage;

interface IRoxieServerActivity : extends IActivityBase
{
    virtual void setInput(unsigned idx, unsigned sourceIdx, IFinalRoxieInput *in) = 0;
    virtual IFinalRoxieInput *queryOutput(unsigned idx) = 0;
    virtual IFinalRoxieInput *queryInput(unsigned idx) const = 0;
    virtual void execute(unsigned parentExtractSize, const byte *parentExtract) = 0;
    virtual void onCreate(IHThorArg *colocalArg) = 0;
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual IStrandJunction *getOutputStreams(IRoxieAgentContext *ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const StrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) = 0;  // Use StrandFlags values for flags

    virtual void stop() = 0;
    virtual void abort() = 0;
    virtual void reset() = 0;
    virtual void addDependency(IRoxieServerActivity &source, unsigned sourceIdx, int controlId) = 0;
    virtual unsigned queryId() const = 0;
    virtual unsigned querySubgraphId() const = 0;
    virtual const IRoxieServerActivityFactory *queryFactory() const = 0;
    virtual __int64 evaluate() = 0;
    virtual void executeChild(size32_t & retSize, void * & ret, unsigned parentExtractSize, const byte * parentExtract) = 0;
    virtual void serializeCreateStartContext(MemoryBuffer &out) = 0;
    virtual void serializeExtra(MemoryBuffer &out) = 0;
    virtual void stopSink(unsigned idx) = 0;
    virtual void connectInputStreams(bool consumerOrdered) = 0;

    //Functions to support result streaming between parallel loop/graphloop/library implementations
    virtual IFinalRoxieInput * querySelectOutput(unsigned id) = 0;
    virtual bool querySetStreamInput(unsigned id, unsigned _sourceIdx, IFinalRoxieInput * _input) = 0;
    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract) = 0;
    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract, IProbeManager *probeManager, IArrayOf<IRoxieProbe> &probes) = 0;
    virtual void resetOutputsUsed() = 0;        // use for adjusting correct number of uses for a splitter
    virtual void noteOutputUsed() = 0;
    virtual bool isPassThrough() = 0;
// Dynamic file support
    virtual const IResolvedFile *resolveLFN(const char *fileName, bool isOpt, bool isPrivilegedUser) = 0;
    virtual const IResolvedFile *queryVarFileInfo() const = 0;
//misc
    virtual IRoxieAgentContext *queryContext() = 0;
    virtual void serializeSkipInfo(MemoryBuffer &out, unsigned seekLen, const void *rawSeek, unsigned numFields, const void * seek, const SmartStepExtra &stepExtra) const = 0;
    virtual ThorActivityKind getKind() const = 0;
    virtual const IRoxieContextLogger &queryLogCtx() const = 0;
    virtual void mergeStats(MemoryBuffer &stats) = 0;
    virtual void mergeStats(const CRuntimeStatisticCollection & childStats) = 0;
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name) = 0;
    virtual IEngineRowAllocator * createRowAllocator(IOutputMetaData * metadata) = 0;
    virtual void gatherStatistics(IStatisticGatherer * statsBuilder) const = 0;
};

interface IRoxieServerActivityFactory : extends IActivityFactory
{
    virtual IRoxieServerActivity *createActivity(IRoxieAgentContext *_ctx, IProbeManager *_probemanager) const = 0;
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx) = 0;
    virtual bool isSink() const = 0;
    virtual bool isFunction() const = 0;
    virtual unsigned getInput(unsigned idx, unsigned &outputidx) const = 0;
    virtual unsigned queryId() const = 0;
    virtual unsigned querySubgraphId() const = 0;
    virtual void addDependency(unsigned source, ThorActivityKind kind, unsigned sourceIdx, int controlId, const char *edgeId) = 0;
    virtual IntArray &queryDependencies() = 0;
    virtual IntArray &queryDependencyIndexes() = 0;
    virtual IntArray &queryDependencyControlIds() = 0;
    virtual StringArray &queryDependencyEdgeIds() = 0;
    virtual ThorActivityKind getKind() const = 0;
    virtual IOutputMetaData *queryOutputMeta() const = 0;
    virtual IHThorArg &getHelper() const = 0;
    virtual IRoxieServerActivity *createFunction(IHThorArg &helper, IProbeManager *_probeManager) const = 0;
    virtual void noteProcessed(unsigned idx, unsigned processed) const = 0;
    virtual void onCreateChildQueries(IRoxieAgentContext *ctx, IHThorArg *colocalArg, IArrayOf<IActivityGraph> &childGraphs, IRoxieServerActivity *parentActivity, IProbeManager *_probeManager, const IRoxieContextLogger &_logctx, unsigned numParallel) const = 0;
    virtual void noteStarted() const = 0;
    virtual void noteStarted(unsigned idx) const = 0;
    virtual void noteDependent(unsigned target) = 0;
    virtual IActivityGraph * createChildGraph(IRoxieAgentContext * ctx, IHThorArg *colocalArg, unsigned childId, IRoxieServerActivity *parentActivity, IProbeManager * _probeManager, const IRoxieContextLogger &_logctx) const = 0;
    virtual unsigned __int64 queryLocalTimeNs() const = 0;
    virtual bool isGraphInvariant() const = 0;
    virtual unsigned numInputs() const = 0;
    virtual const StatisticsMapping &queryStatsMapping() const = 0;
    virtual bool isInputOrdered(bool consumerOrdered, unsigned idx) const = 0;
    virtual roxiemem::RoxieHeapFlags getHeapFlags() const = 0;
    virtual bool isActivityCodeSigned() const = 0;
    virtual RecordTranslationMode getEnableFieldTranslation() const = 0;
};
interface IGraphResult : public IInterface
{
    virtual void getLinkedResult(unsigned & countResult, const byte * * & result) = 0;
    virtual IEngineRowStream * createIterator() = 0;
    virtual const void * getLinkedRowResult() = 0;
};

interface IRoxieServerLoopResultProcessor
{
    virtual void noteUseIteration(unsigned whichIteration) = 0;
    virtual IFinalRoxieInput * connectIterationOutput(unsigned whichIteration, IProbeManager *probeManager, IArrayOf<IRoxieProbe> &probes, IRoxieServerActivity *targetAct, unsigned targetIdx) = 0;
};

interface IRoxieGraphResults : extends IEclGraphResults
{
public:
    virtual IEngineRowStream * createIterator(unsigned id) = 0;
};

class CGraphIterationInfo;

interface IRoxieServerChildGraph : public IInterface
{
    virtual void beforeExecute() = 0;
    virtual IFinalRoxieInput * startOutput(unsigned id, unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual IFinalRoxieInput * selectOutput(unsigned id) = 0;
    virtual void setInputResult(unsigned id, IGraphResult * result) = 0;
    virtual bool querySetInputResult(unsigned id, unsigned _sourceIdx, IFinalRoxieInput * result) = 0;
    virtual void stopUnusedOutputs() = 0;
    virtual IRoxieGraphResults * execute(size32_t parentExtractSize, const byte *parentExtract) = 0;
    virtual void afterExecute() = 0;
//sequential graph related helpers
    virtual void clearGraphLoopResults() = 0;
    virtual void executeGraphLoop(size32_t parentExtractSize, const byte *parentExtract) = 0;
    virtual void setGraphLoopResult(unsigned id, IGraphResult * result) = 0;
    virtual IEngineRowStream * getGraphLoopResult(unsigned id) = 0;
//parallel graph related helpers.
    virtual CGraphIterationInfo * selectGraphLoopOutput() = 0;
    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor) = 0;
    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor) = 0;
    virtual void gatherStatistics(IStatisticGatherer * statsBuilder) const = 0;
};

interface IQueryFactory;

extern IActivityGraph *createActivityGraph(IRoxieAgentContext *ctx, const char *graphName, unsigned id, ActivityArray &x, IRoxieServerActivity *parent, IProbeManager *probeManager, const IRoxieContextLogger &logctx, unsigned numParallel);

extern ruid_t getNextRuid();
extern void setStartRuid(unsigned restarts);

class CIndexTransformCallback : implements IThorIndexCallback, public CInterface
{
public:
    CIndexTransformCallback() { keyManager = NULL; cleanupRequired = false; };
    IMPLEMENT_IINTERFACE_O

//IThorIndexCallback
    virtual const byte * lookupBlob(unsigned __int64 id) override
    { 
        size32_t dummy; 
        cleanupRequired = true;
        return (byte *) keyManager->loadBlob(id, dummy); 
    }

public:
    inline void setManager(IKeyManager * _manager)
    {
        finishedRow();
        keyManager = _manager;
    }
    inline void finishedRow()
    {
        if (cleanupRequired && keyManager)
        {
            keyManager->releaseBlobs(); 
            cleanupRequired = false;
        }
    }

protected:
    IKeyManager * keyManager;
    bool cleanupRequired;
};

//Used to make sure setManager(NULL) is called before the manager is destroyed.
class TransformCallbackAssociation
{
public:
    TransformCallbackAssociation(CIndexTransformCallback & _callback, IKeyManager * manager) : callback(_callback)
    {
        callback.setManager(manager);
    }
    ~TransformCallbackAssociation()
    {
        callback.setManager(NULL);
    }
private:
    CIndexTransformCallback & callback;
};

extern IRoxieServerActivityFactory *createRoxieServerApplyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerNullActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerChildIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNewChildNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNewChildAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNewChildGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNewChildThroughNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDatasetResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerInlineTableActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerWorkUnitReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultStreamReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, unsigned _graphId, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerDictionaryResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, unsigned _graphId, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, unsigned _graphId);
extern IRoxieServerActivityFactory *createRoxieServerDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerHashDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerRollupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeLinkedChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSortActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerThroughSpillActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSplitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerPipeReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerPipeThroughActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerPipeWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerFilterActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerFilterGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSideEffectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSampleActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsLastActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerHashAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSpillReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDiskWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDenormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerConcatActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerRegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerCombineActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerCombineGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerRollupGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerFilterProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _loopId);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _loopId);
extern IRoxieServerActivityFactory *createRoxieServerRemoteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerIterateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerProcessActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerFirstNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSelectNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSelfJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLookupJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerAllJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerTopNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSkipLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerCatchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerCaseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _graphInvariant);
extern IRoxieServerActivityFactory *createRoxieServerIfActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _graphInvariant);
extern IRoxieServerActivityFactory *createRoxieServerParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, IResourceContext *rc);
extern IRoxieServerActivityFactory *createRoxieServerWorkUnitWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerWorkUnitWriteDictActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerRemoteResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerXmlParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDiskReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerIndexReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerIndexCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerIndexAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerIndexGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerIndexNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerDiskCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerFetchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId);
extern IRoxieServerActivityFactory *createRoxieServerDummyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool isLoadDataOnly);
extern IRoxieServerActivityFactory *createRoxieServerKeyedJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, const RemoteActivityId &_remoteId, const RemoteActivityId &_remoteId2);
extern IRoxieServerActivityFactory *createRoxieServerSoapRowCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSoapRowActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSoapDatasetCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSoapDatasetActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerLinkedRawIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerQuantileActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);

extern IRoxieServerActivityFactory *createRoxieServerNWayGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerNWayInputActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNWayMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNWayMergeJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSortedActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerNWaySelectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerLibraryCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, LibraryCallFactoryExtra & extra);
extern IRoxieServerActivityFactory *createRoxieServerNonEmptyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIfActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSequentialActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerParallelActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerPrefetchProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerStreamedIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerWhenActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerWhenActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);

extern IRoxieServerActivityFactory *createRoxieServerDistributionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerPullActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerTraceActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerExternalActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool _isRoot);

extern void throwRemoteException(IMessageUnpackCursor *extra);

#endif
