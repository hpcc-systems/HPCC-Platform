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

#ifndef _CCDSERVER_INCL
#define _CCDSERVER_INCL

#include "ccdactivities.hpp"
#include "ccdfile.hpp"
#include "roxiemem.hpp"
#include "workflow.hpp"
#include "roxiehelper.hpp"
#include "roxiedebug.hpp"
#include "thorstep.hpp"

interface IRoxieQuerySetManager;
interface IRoxieListener : extends IInterface
{
    virtual void start() = 0;
    virtual bool stop(unsigned timeout) = 0;
    virtual void stopListening() = 0;
    virtual void addAccess(bool allow, bool allowBlind, const char *ip, const char *mask, const char *query, const char *errMsg, int errCode) = 0;
    virtual unsigned queryPort() const = 0;
    virtual const SocketEndpoint &queryEndpoint() const = 0;
    virtual bool suspend(bool suspendIt) = 0;

    virtual void runOnce(const char *query) = 0;
};

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

IRoxieListener *createRoxieSocketListener(unsigned port, unsigned poolSize, unsigned listenQueue, bool suspended);
IRoxieListener *createRoxieWorkUnitListener(unsigned poolSize, bool suspended);
bool suspendRoxieListener(unsigned port, bool suspended);
extern IArrayOf<IRoxieListener> socketListeners;


interface IRoxieInput;
interface IProbeManager;
interface IRoxieServerLoopResultProcessor;

interface IWorkUnitRowReader : public IInterface
{
    virtual const void * nextInGroup() = 0;
};

interface IDeserializedResultStore : public IInterface
{
    virtual int addResult(ConstPointerArray *data, IOutputMetaData *meta) = 0;
    virtual int appendResult(int oldId, ConstPointerArray *data, IOutputMetaData *meta) = 0;
    virtual IWorkUnitRowReader *createDeserializedReader(int id) const = 0;
    virtual void serialize(unsigned & tlen, void * & tgt, int id, ICodeContext *ctx) const = 0;
};

extern IDeserializedResultStore *createDeserializedResultStore();

class FlushingStringBuffer;
interface IActivityGraph;
typedef MapBetween<unsigned, unsigned, unsigned, unsigned> MapIdToActivityIndex;

struct LibraryCallFactoryExtra
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
    unsigned interfaceHash;
    bool embedded;
};

interface IRoxieServerActivity;
interface IRoxieServerChildGraph;
interface IRoxieServerContext;
class ClusterWriteHandler;

interface IRoxieSlaveContext : extends IRoxieContextLogger
{
    virtual ICodeContext *queryCodeContext() = 0;
    virtual void checkAbort() = 0;
    virtual void notifyAbort(IException *E) = 0;
    virtual IActivityGraph * queryChildGraph(unsigned id) = 0;
    virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph) = 0;
    virtual roxiemem::IRowManager &queryRowManager() = 0;
    virtual unsigned parallelJoinPreload() = 0;
    virtual unsigned concatPreload() = 0;
    virtual unsigned fetchPreload() = 0;
    virtual unsigned fullKeyedJoinPreload() = 0;
    virtual unsigned keyedJoinPreload() = 0;
    virtual unsigned prefetchProjectPreload() = 0;
    virtual void addSlavesReplyLen(unsigned len) = 0;
    virtual const char *queryAuthToken() = 0;
    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt) = 0;
    virtual IRoxieWriteHandler *createLFN(const char *filename, bool overwrite, bool extend, const StringArray &clusters) = 0;
    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal) = 0;
    virtual IActivityGraph * getLibraryGraph(const LibraryCallFactoryExtra &extra, IRoxieServerActivity *parentActivity) = 0;
    virtual void noteProcessed(const IRoxieContextLogger &_activityContext, const IRoxieServerActivity *activity, unsigned _idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const = 0;
    virtual IProbeManager *queryProbeManager() const = 0;
    virtual bool queryTimeActivities() const = 0;
    virtual IDebuggableContext *queryDebugContext() const = 0;
    virtual void printResults(IXmlWriter *output, const char *name, unsigned sequence) = 0;
    virtual void setWUState(WUState state) = 0;
    virtual bool checkWuAborted() = 0;
    virtual IWorkUnit *updateWorkUnit() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
    virtual IRoxieServerContext *queryServerContext() = 0;
};

interface IRoxieServerContext : extends IInterface
{
    virtual IGlobalCodeContext *queryGlobalCodeContext() = 0;
    virtual FlushingStringBuffer *queryResult(unsigned sequence) = 0;
    virtual void setResultXml(const char *name, unsigned sequence, const char *xml) = 0;
    virtual void appendResultDeserialized(const char *name, unsigned sequence, ConstPointerArray *data, bool extend, IOutputMetaData *meta) = 0;
    virtual void appendResultRawContext(const char *name, unsigned sequence, int len, const void * data, int numRows, bool extend, bool saveInContext) = 0;
    virtual IWorkUnitRowReader *getWorkunitRowReader(const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, IEngineRowAllocator *rowAllocator, bool isGrouped) = 0;
    virtual roxiemem::IRowManager &queryRowManager() = 0;

    virtual void process() = 0;
    virtual void done(bool failed) = 0;
    virtual void flush(unsigned seqNo) = 0;
    virtual unsigned getMemoryUsage() = 0;
    virtual unsigned getSlavesReplyLen() = 0;

    virtual unsigned getXmlFlags() const = 0;
    virtual bool outputResultsToWorkUnit() const = 0;
    virtual bool outputResultsToSocket() const = 0;

    virtual IRoxieDaliHelper *checkDaliConnection() = 0;
};

interface IRoxieInput : extends IInterface, extends IInputBase
{
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual void stop(bool aborting) = 0;
    virtual void reset() = 0;
    virtual void checkAbort() = 0;
    virtual unsigned queryId() const = 0;

    virtual bool nextGroup(ConstPointerArray & group);
    virtual unsigned __int64 queryTotalCycles() const = 0;
    virtual unsigned __int64 queryLocalCycles() const = 0;
    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra) { throwUnexpected(); }  // can only be called on stepping fields.
    virtual IInputSteppingMeta * querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual unsigned numConcreteOutputs() const { return 1; }
    virtual IRoxieInput * queryConcreteInput(unsigned idx) { assertex(idx==0); return this; }
    virtual IRoxieInput *queryInput(unsigned idx) const = 0;
    virtual IRoxieServerActivity *queryActivity() = 0;
    virtual void resetEOF() = 0;
    virtual IIndexReadActivityInfo *queryIndexReadActivity() = 0;
};


interface ISteppedConjunctionCollector;
interface IInputSteppingMeta;

interface IIndexReadActivityInfo
{
    virtual IKeyArray *getKeySet() const = 0;
    virtual const IResolvedFile *getVarFileInfo() const = 0;
    virtual TranslatorArray *getTranslators() const = 0;

    virtual void mergeSegmentMonitors(IIndexReadContext *irc) const = 0;
    virtual IRoxieServerActivity *queryActivity() = 0;
    virtual const RemoteActivityId &queryRemoteId() const = 0;
};

interface IRoxieServerErrorHandler
{
    virtual void onLimitExceeded(bool isKeyed) = 0;
    virtual const void *createLimitFailRow(bool isKeyed) = 0;
};

interface IRoxieServerSideCache
{
    virtual IRoxieServerQueryPacket *findCachedResult(const IRoxieContextLogger &logctx, IRoxieQueryPacket *p) const = 0;
    virtual void noteCachedResult(IRoxieServerQueryPacket *out, IMessageResult *in) = 0;
};

interface IRoxieServerActivityFactory;
interface IRoxiePackage;

interface IRoxieServerActivity : extends IActivityBase
{
    virtual void setInput(unsigned idx, IRoxieInput *in) = 0;
    virtual IRoxieInput *queryOutput(unsigned idx) = 0;
    virtual void execute(unsigned parentExtractSize, const byte *parentExtract) = 0;
    virtual void onCreate(IRoxieSlaveContext *ctx, IHThorArg *colocalArg) = 0;
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual void stop(bool aborting) = 0;
    virtual void reset() = 0;
    virtual void addDependency(IRoxieServerActivity &source, unsigned sourceIdx, int controlId) = 0;
    virtual unsigned queryId() const = 0;
    virtual unsigned querySubgraphId() const = 0;
    virtual const IRoxieServerActivityFactory *queryFactory() const = 0;
    virtual __int64 evaluate() = 0;
    virtual void executeChild(size32_t & retSize, void * & ret, unsigned parentExtractSize, const byte * parentExtract) = 0;
    virtual void serializeCreateStartContext(MemoryBuffer &out) = 0;
    virtual void serializeExtra(MemoryBuffer &out) = 0;
//Functions to support result streaming between parallel loop/graphloop/library implementations
    virtual IRoxieInput * querySelectOutput(unsigned id) = 0;
    virtual bool querySetStreamInput(unsigned id, IRoxieInput * _input) = 0;
    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract) = 0;
    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes) = 0;
    virtual void resetOutputsUsed() = 0;        // use for adjusting correct number of uses for a splitter
    virtual void noteOutputUsed() = 0;
    virtual bool isPassThrough() = 0;
// Roxie server-side caching
    virtual IRoxieServerSideCache *queryServerSideCache() const = 0;
// Dynamic file support
    virtual const IResolvedFile *resolveLFN(const char *fileName, bool isOpt) = 0;
    virtual const IResolvedFile *queryVarFileInfo() const = 0;
//misc
    virtual IRoxieSlaveContext *queryContext() = 0;
    virtual void serializeSkipInfo(MemoryBuffer &out, unsigned seekLen, const void *rawSeek, unsigned numFields, const void * seek, const SmartStepExtra &stepExtra) const = 0;
    virtual ThorActivityKind getKind() const = 0;
    virtual const IRoxieContextLogger &queryLogCtx() const = 0;
};

interface IRoxieServerActivityFactory : extends IActivityFactory
{
    virtual IRoxieServerActivity *createActivity(IProbeManager *_probemanager) const = 0;
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
    virtual void noteProcessed(unsigned idx, unsigned processed, unsigned __int64 totalCycles, unsigned __int64 localCycles) const = 0;
    virtual void onCreateChildQueries(IRoxieSlaveContext *ctx, IHThorArg *colocalArg, IArrayOf<IActivityGraph> &childGraphs) const = 0;
    virtual void createChildQueries(IArrayOf<IActivityGraph> &childGraphs, IRoxieServerActivity *parentActivity, IProbeManager *_probeManager, const IRoxieContextLogger &_logctx) const = 0;
    virtual void noteStarted() const = 0;
    virtual void noteStarted(unsigned idx) const = 0;
    virtual void noteDependent(unsigned target) = 0;
    virtual IActivityGraph * createChildGraph(IRoxieSlaveContext * ctx, IHThorArg *colocalArg, unsigned childId, IRoxieServerActivity *parentActivity, IProbeManager * _probeManager, const IRoxieContextLogger &_logctx) const = 0;
    virtual unsigned __int64 queryLocalCycles() const = 0;
    virtual bool isGraphInvariant() const = 0;
    virtual IRoxieServerSideCache *queryServerSideCache() const = 0;
    virtual IDefRecordMeta *queryActivityMeta() const = 0;
    virtual void noteStatistic(unsigned statCode, unsigned __int64 value, unsigned count) const = 0;
};
interface IGraphResult : public IInterface
{
    virtual void getResult(unsigned & lenResult, void * & result) = 0;
    virtual void getLinkedResult(unsigned & countResult, byte * * & result) = 0;
    virtual IRoxieInput * createIterator() = 0;
};

interface IRoxieServerLoopResultProcessor
{
    virtual void noteUseIteration(unsigned whichIteration) = 0;
    virtual IRoxieInput * connectIterationOutput(unsigned whichIteration, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes, IRoxieServerActivity *targetAct, unsigned targetIdx) = 0;
};

interface IRoxieGraphResults : extends IEclGraphResults
{
public:
    virtual IRoxieInput * createIterator(unsigned id) = 0;
};

class CGraphIterationInfo;

interface IRoxieServerChildGraph : public IInterface
{
    virtual void beforeExecute() = 0;
    virtual IRoxieInput * startOutput(unsigned id, unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;
    virtual IRoxieInput * selectOutput(unsigned id) = 0;
    virtual void setInputResult(unsigned id, IGraphResult * result) = 0;
    virtual bool querySetInputResult(unsigned id, IRoxieInput * result) = 0;
    virtual void stopUnusedOutputs() = 0;
    virtual IRoxieGraphResults * execute(size32_t parentExtractSize, const byte *parentExtract) = 0;
    virtual void afterExecute() = 0;
//sequential graph related helpers
    virtual void clearGraphLoopResults() = 0;
    virtual void executeGraphLoop(size32_t parentExtractSize, const byte *parentExtract) = 0;
    virtual void setGraphLoopResult(unsigned id, IGraphResult * result) = 0;
    virtual IRoxieInput * getGraphLoopResult(unsigned id) = 0;
//parallel graph related helpers.
    virtual CGraphIterationInfo * selectGraphLoopOutput() = 0;
    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor) = 0;
    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor) = 0;
};

extern void doDebugRequest(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx);
interface IQueryFactory;
interface ILoadedDllEntry;

extern IActivityGraph *createActivityGraph(const char *graphName, unsigned id, ActivityArray &x, IRoxieServerActivity *parent, IProbeManager *probeManager, const IRoxieContextLogger &logctx);
extern IProbeManager *createProbeManager();
extern IProbeManager *createDebugManager(IDebuggableContext *debugContext, const char *graphName);
extern IRoxieSlaveContext *createSlaveContext(const IQueryFactory *factory, const SlaveContextLogger &logctx, unsigned timeLimit, memsize_t memoryLimit, IRoxieQueryPacket *packet);
extern IRoxieServerContext *createRoxieServerContext(IPropertyTree *context, const IQueryFactory *factory, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, unsigned priority, const IRoxieContextLogger &logctx, XmlReaderOptions xmlReadFlags);
extern IRoxieServerContext *createOnceServerContext(const IQueryFactory *factory, const IRoxieContextLogger &_logctx);
extern IRoxieServerContext *createWorkUnitServerContext(IConstWorkUnit *wu, const IQueryFactory *factory, const IRoxieContextLogger &logctx);
extern WorkflowMachine *createRoxieWorkflowMachine(IPropertyTree *_workflowInfo, bool doOnce, const IRoxieContextLogger &_logctx);

extern ruid_t getNextRuid();
extern void setStartRuid(unsigned restarts);

class CIndexTransformCallback : public CInterface, implements IThorIndexCallback 
{
public:
    CIndexTransformCallback() { keyManager = NULL; cleanupRequired = false; filepos = 0; };
    IMPLEMENT_IINTERFACE

//IThorIndexCallback
    virtual unsigned __int64 getFilePosition(const void * row)
    {
        return filepos;
    }
    virtual byte * lookupBlob(unsigned __int64 id) 
    { 
        size32_t dummy; 
        cleanupRequired = true;
        return (byte *) keyManager->loadBlob(id, dummy); 
    }


public:
    inline offset_t & getFPosRef()                              { return filepos; }
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
    offset_t filepos;
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

extern IRoxieServerActivityFactory *createRoxieServerApplyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerNullActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerChildIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNewChildNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNewChildAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNewChildGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNewChildThroughNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerDatasetResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerTempTableActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerWorkUnitReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultStreamReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLocalResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId);
extern IRoxieServerActivityFactory *createRoxieServerDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerHashDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerRollupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNormalizeLinkedChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSortActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerThroughSpillActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSplitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerPipeReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerPipeThroughActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerPipeWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerFilterActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerFilterGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSideEffectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSampleActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerChooseSetsLastActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerHashAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerDegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSpillReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerDiskWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerIndexWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerDenormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerConcatActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerRegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerCombineActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerCombineGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerRollupGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerFilterProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopId);
extern IRoxieServerActivityFactory *createRoxieServerGraphLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopId);
extern IRoxieServerActivityFactory *createRoxieServerRemoteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerIterateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerProcessActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerFirstNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSelectNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSelfJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLookupJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerAllJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerTopNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSkipLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerCatchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerCaseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant);
extern IRoxieServerActivityFactory *createRoxieServerIfActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant);
extern IRoxieServerActivityFactory *createRoxieServerParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IResourceContext *rc);
extern IRoxieServerActivityFactory *createRoxieServerWorkUnitWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerRemoteResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerXmlParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerDiskReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerIndexNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDiskCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerFetchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerDummyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool isLoadDataOnly);
extern IRoxieServerActivityFactory *createRoxieServerKeyedJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, const RemoteActivityId &_remoteId2, IPropertyTree &_graphNode);
extern IRoxieServerActivityFactory *createRoxieServerSoapRowCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSoapRowActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSoapDatasetCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSoapDatasetActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerRawIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLinkedRawIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);

extern IRoxieServerActivityFactory *createRoxieServerNWayGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId);
extern IRoxieServerActivityFactory *createRoxieServerNWayInputActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNWayMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNWayMergeJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerSortedActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerNWaySelectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerLibraryCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, LibraryCallFactoryExtra & extra);
extern IRoxieServerActivityFactory *createRoxieServerNonEmptyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerIfActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerSequentialActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerParallelActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);
extern IRoxieServerActivityFactory *createRoxieServerPrefetchProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerStreamedIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerWhenActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
extern IRoxieServerActivityFactory *createRoxieServerWhenActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot);

#endif
