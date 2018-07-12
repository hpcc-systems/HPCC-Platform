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

#ifndef _THGRAPH_HPP
#define _THGRAPH_HPP

#ifdef GRAPH_EXPORTS
    #define graph_decl DECL_EXPORT
#else
    #define graph_decl DECL_IMPORT
#endif

#undef barrier

#define LONGTIMEOUT (25*60*1000)
#define MEDIUMTIMEOUT 30000

#include "jlib.hpp"
#include "jarray.hpp"
#include "jexcept.hpp"
#include "jhash.hpp"
#include "jsuperhash.hpp"
#include "jset.hpp"

#include "mpcomm.hpp"

#include "mptag.hpp"

#include "roxiemem.hpp"
#include "thormisc.hpp"
#include "workunit.hpp"
#include "thorcommon.hpp"
#include "thmem.hpp"

#include "thor.hpp"
#include "eclhelper.hpp"

#include "thorplugin.hpp"

#define THORDATALINK_STOPPED            (RCMAX&~(RCMAX>>1))                         // dataLinkStop() was called
#define THORDATALINK_STARTED            (RCMAX&~THORDATALINK_STOPPED&~(RCMAX>>2))   // dataLinkStart() was called
#define THORDATALINK_COUNT_MASK         (RCMAX>>2)                                  // mask to extract count value only



enum ActivityAttributes { ActAttr_Source=1, ActAttr_Sink=2 };
const static unsigned defaultHeapFlags = roxiemem::RHFscanning;

#define INVALID_UNIQ_ID -1;
typedef activity_id unique_id;

enum msgids
{
    QueryInit,
    QueryDone,
    Shutdown,
    GraphInit,
    GraphEnd,
    GraphAbort,
    GraphGetResult,
    DebugRequest
};

interface ICodeContextExt : extends ICodeContext
{
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) = 0;
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) = 0;
};

interface IDiskUsage : extends IInterface
{
    virtual void increase(offset_t usage, const char *key=NULL) = 0;
    virtual void decrease(offset_t usage, const char *key=NULL) = 0;
};

interface IBackup;
interface IFileInProgressHandler;
interface IThorFileCache;
interface IKJService : extends IInterface
{
    virtual void setCurrentJob(CJobBase &job) = 0;
    virtual void reset() = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
};
interface IThorResource
{
    virtual IThorFileCache &queryFileCache() = 0;
    virtual IBackup &queryBackup() = 0;
    virtual IFileInProgressHandler &queryFileInProgressHandler() = 0;
    virtual IKJService &queryKeyedJoinService() = 0;
};

interface IBarrier : extends IInterface
{
    virtual const mptag_t queryTag() const = 0;
    virtual bool wait(bool exception, unsigned timeout=INFINITE) = 0;
    virtual void cancel(IException *e=NULL) = 0;
};

graph_decl IThorResource &queryThor();
graph_decl void setIThorResource(IThorResource &r);

interface IRowWriterMultiReader;
interface IThorResult : extends IInterface
{
    virtual IRowWriter *getWriter() = 0;
    virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count) = 0;
    virtual IRowStream *getRowStream() = 0;
    virtual IThorRowInterfaces *queryRowInterfaces() = 0;
    virtual CActivityBase *queryActivity() = 0;
    virtual bool isDistributed() const = 0;
    virtual void serialize(MemoryBuffer &mb) = 0;
    virtual void getLinkedResult(unsigned & count, const byte * * & ret) = 0;
    virtual const void * getLinkedRowResult() = 0;
};

enum ThorGraphResultType:unsigned
{
    thorgraphresult_nul = 0x00,
    thorgraphresult_distributed = 0x01,
    thorgraphresult_grouped = 0x02,
    thorgraphresult_sparse = 0x04
};
inline ThorGraphResultType mergeResultTypes(ThorGraphResultType l, ThorGraphResultType r) { return (ThorGraphResultType) (l|r); }
class CActivityBase;
// JCSMORE - based on IHThorGraphResults
interface IThorGraphResults : extends IEclGraphResults
{
    virtual void clear() = 0;
    virtual IThorResult *getResult(unsigned id, bool distributed=false) = 0;
    virtual IThorResult *createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT) = 0;
    virtual IThorResult *createResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT) = 0;
    virtual unsigned addResult(IThorResult *result) = 0;
    virtual void setResult(unsigned id, IThorResult *result) = 0;
    virtual unsigned count() = 0;
    virtual void setOwner(activity_id id) = 0;
    virtual activity_id queryOwnerId() const = 0;
};

class CGraphBase;
interface IThorBoundLoopGraph : extends IInterface
{
    virtual void prepareLoopResults(CActivityBase &activity, IThorGraphResults *results) = 0;
    virtual void prepareCounterResult(CActivityBase &activity, IThorGraphResults *results, unsigned loopCounter, unsigned pos) = 0;
    virtual void prepareLoopAgainResult(CActivityBase &activity, IThorGraphResults *results, unsigned pos) = 0;
    virtual void execute(CActivityBase &activity, unsigned counter, IThorGraphResults *results, IRowWriterMultiReader *rowStream, rowcount_t rowStreamCount, size32_t parentExtractSz, const byte * parentExtract) = 0;
    virtual void execute(CActivityBase &activity, unsigned counter, IThorGraphResults * graphLoopResults, size32_t parentExtractSz, const byte * parentExtract) = 0;
    virtual CGraphBase *queryGraph() = 0;
};

class CFileUsageEntry : public CInterface
{
    StringAttr name;
    unsigned usage;
    graph_id graphId;
    WUFileKind fileKind;
public:
    CFileUsageEntry(const char *_name, graph_id _graphId, WUFileKind _fileKind, unsigned _usage) :name(_name), graphId(_graphId), fileKind(_fileKind), usage(_usage) { }
    unsigned queryUsage() const { return usage; }
    const graph_id queryGraphId() const { return graphId; }
    const WUFileKind queryKind() const { return fileKind; }
    const char *queryName() const { return name.get(); }
    void decUsage() { --usage; }

    const char *queryFindString() const { return name; }
};

typedef IIteratorOf<CFileUsageEntry> IFileUsageIterator;

interface IGraphTempHandler : extends IInterface
{
    virtual void registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind=WUFileStandard, StringArray *clusters=NULL) = 0;
    virtual void deregisterFile(const char *name, bool kept=false) = 0;
    virtual void clearTemps() = 0;
    virtual IFileUsageIterator *getIterator() = 0;
};

class CGraphDependency : public CInterface
{
public:
    Linked<CGraphBase> graph;
    int controlId;

    CGraphDependency(CGraphBase *_graph, int _controlId) : graph(_graph), controlId(_controlId) { }
    void connect(CActivityBase *activity);
};

typedef CIArrayOf<CGraphDependency> CGraphDependencyArray;
typedef IIteratorOf<CGraphDependency> IThorGraphDependencyIterator;

class CGraphElementBase;
class CIOConnection : public CInterface
{
public:
    CGraphElementBase *activity;
    unsigned index;
    void connect(unsigned which, CActivityBase *activity);

    CIOConnection(CGraphElementBase *_activity, unsigned _index) : activity(_activity), index(_index) { }
};

class COwningSimpleIOConnection : public CIOConnection
{
public:
    COwningSimpleIOConnection(CGraphElementBase *_activity, unsigned index) : CIOConnection(_activity, index) { }
    ~COwningSimpleIOConnection() { ::Release(activity); }
};

class CIOConnectionArray : public OwnedPointerArrayOf<CIOConnection>
{
public:
    CIOConnection *queryItem(unsigned i)
    {
        if (!isItem(i))
            return NULL;
        return item(i);
    }
    unsigned getCount() const
    {
        unsigned c = 0;
        ForEachItemIn(i, *this)
        {
            CIOConnection *io = item(i);
            if (io)
                ++c;
        }
        return c;
    }
};

class CGraphStub;
typedef SimpleHashTableOf<CGraphBase, graph_id> CGraphTableCopy;
typedef OwningSimpleHashTableOf<CGraphBase, graph_id> CGraphTable;
typedef OwningSimpleHashTableOf<CGraphStub, graph_id> CChildGraphTable;
typedef CIArrayOf<CGraphBase> CGraphArray;
typedef CICopyArrayOf<CGraphBase> CGraphArrayCopy;
typedef CICopyArrayOf<CGraphStub> CGraphStubArrayCopy;
typedef IIteratorOf<CGraphBase> IThorGraphIterator;
typedef ArrayIIteratorOf<const CGraphArray, CGraphBase, IThorGraphIterator> CGraphArrayIterator;
typedef ArrayIIteratorOf<const CGraphArrayCopy, CGraphBase, IThorGraphIterator> CGraphArrayCopyIterator;


typedef IIteratorOf<CGraphStub> IThorGraphStubIterator;

class CJobBase;
class CJobChannel;
class graph_decl CGraphElementBase : public CInterface, implements IInterface
{
protected:
    CriticalSection crit;
    Owned<IHThorArg> baseHelper;
    ThorActivityKind kind;
    activity_id id, ownerId;
    StringAttr eclText;
    Owned<IPropertyTree> xgmml;
    bool isLocal, isLocalData, isGrouped, sink, prepared, onCreateCalled, onlyUpdateIfChanged, nullAct, log;
    Owned<CActivityBase> activity;
    CGraphBase *resultsGraph, *owner;
    CGraphDependencyArray dependsOn;
    Owned<IThorBoundLoopGraph> loopGraph; // really only here as master and slave derivatives set/use
    MemoryBuffer createCtxMb, startCtxMb;
    bool haveCreateCtx;
    unsigned maxCores;

public:
    IMPLEMENT_IINTERFACE;

    const void *queryFindParam() const { return &queryId(); } // for SimpleHashTableOf

    bool alreadyUpdated = false;
    EclHelperFactory helperFactory;

    CIOConnectionArray inputs, outputs, connectedInputs, connectedOutputs;

    unsigned whichBranch;
    Owned<IBitSet> sentActInitData;

    CGraphElementBase(CGraphBase &_owner, IPropertyTree &_xgmml);
    ~CGraphElementBase();

    void doconnect();
    void addInput(unsigned input, CGraphElementBase *inputAct, unsigned inputOutIdx);
    void clearConnections();
    virtual void connectInput(unsigned which, CGraphElementBase *input, unsigned inputOutIdx);
    void setResultsGraph(CGraphBase *_resultsGraph) { resultsGraph = _resultsGraph; }
    void releaseIOs();
    void addDependsOn(CGraphBase *graph, int controlId);
    IThorGraphDependencyIterator *getDependsIterator() const;
    StringBuffer &getOpt(const char *prop, StringBuffer &out) const;
    bool getOptBool(const char *prop, bool defVal=false) const;
    int getOptInt(const char *prop, int defVal=0) const;
    unsigned getOptUInt(const char *prop, unsigned defVal=0) const { return (unsigned)getOptInt(prop, defVal); }
    __int64 getOptInt64(const char *prop, __int64 defVal=0) const;
    unsigned __int64 getOptUInt64(const char *prop, unsigned __int64 defVal=0) const { return (unsigned __int64)getOptInt64(prop, defVal); }
    void ActPrintLog(const char *format, ...)  __attribute__((format(printf, 2, 3)));
    void ActPrintLog(IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
    void ActPrintLog(IException *e);
    void setBoundGraph(IThorBoundLoopGraph *graph) { loopGraph.set(graph); }
    IThorBoundLoopGraph *queryLoopGraph() { return loopGraph; }
    bool executeDependencies(size32_t parentExtractSz, const byte *parentExtract, int controlId, bool async);
    virtual void deserializeCreateContext(MemoryBuffer &mb);
    virtual void serializeCreateContext(MemoryBuffer &mb); // called after onCreate and create() (of activity)
    virtual void serializeStartContext(MemoryBuffer &mb);
    virtual bool checkUpdate() { return alreadyUpdated; }
    virtual void reset();
    void onStart(size32_t parentExtractSz, const byte *parentExtract, MemoryBuffer *startCtx=nullptr);
    void onCreate();
    void abort(IException *e);
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract);
    bool isOnCreated() const { return onCreateCalled; }
    bool isPrepared() const { return prepared; }

    CGraphBase &queryOwner() const { return *owner; }
    CGraphBase *queryResultsGraph() const { return resultsGraph; }
    IGraphTempHandler *queryTempHandler() const;
    CJobBase &queryJob() const;
    CJobChannel &queryJobChannel() const;
    unsigned getInputs() const { return inputs.ordinality(); }
    unsigned getOutputs() const { return outputs.ordinality(); }
    bool isSource() const { return isActivitySource(kind); }
    bool isSink() const { return sink; }
    inline bool doLogging() const { return log; }
    inline void setLogging(bool _log) { log = _log; }

    // NB: in almost all cases queryLocal() == queryLocalData()
    // an exception is e.g. a locally executing keyedjoin, accessing a global key
    bool queryLocal() const { return isLocal; }  // executed in isolation on each slave
    bool queryLocalData() const { return isLocalData; } // activity access local data only

    bool queryGrouped() const { return isGrouped; }
    bool queryLocalOrGrouped() { return isLocal || isGrouped; }
    CGraphElementBase *queryInput(unsigned index) const
    {
        if (inputs.isItem(index) && (NULL != inputs.item(index)))
            return inputs.item(index)->activity;
        return NULL;
    }
    IHThorArg *queryHelper() const { return baseHelper; }
    unsigned queryMaxCores() const { return maxCores; }

    IPropertyTree &queryXGMML() const { return *xgmml; }
    const activity_id &queryOwnerId() const { return ownerId; }
//
    const ThorActivityKind getKind() const { return kind; }
    const activity_id &queryId() const { return id; }
    StringBuffer &getEclText(StringBuffer& dst) const
    {
        dst.append(eclText.get());
        return dst;
    }
    virtual bool prepareContext(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async, bool connectOnly);
    void createActivity();
    CActivityBase *queryActivity() { return activity; }
//
    virtual void initActivity() { }
    virtual CActivityBase *factory(ThorActivityKind kind) { assertex(false); return NULL; }
    virtual CActivityBase *factory() { return factory(getKind()); }
    virtual CActivityBase *factorySet(ThorActivityKind kind) { CActivityBase *_activity = factory(kind); activity.setown(_activity); return _activity; }
    virtual ICodeContext *queryCodeContext();
};

typedef CIArrayOf<CGraphElementBase> CGraphElementArray;
typedef CICopyArrayOf<CGraphElementBase> CGraphElementArrayCopy;
typedef OwningSimpleHashTableOf<CGraphElementBase, activity_id> CGraphElementTable;
typedef IIteratorOf<CGraphElementBase> IThorActivityIterator;
typedef ArrayIIteratorOf<const CGraphElementArray, CGraphElementBase, IThorActivityIterator> CGraphElementArrayIterator;
class CGraphElementIterator : implements IThorActivityIterator, public CInterface
{
    SuperHashIteratorOf<CGraphElementBase> iter;
public:
    IMPLEMENT_IINTERFACE;

    CGraphElementIterator(const CGraphElementTable &table) : iter(table) { }
    virtual bool first() { return iter.first(); }
    virtual bool next() { return iter.next(); }
    virtual bool isValid() { return iter.isValid(); }
    virtual CGraphElementBase & query() { return iter.query(); }
            CGraphElementBase & get() { CGraphElementBase &c = query(); c.Link(); return c; }
};

typedef OwningStringSuperHashTableOf<CFileUsageEntry> CFileUsageTable;
class graph_decl CGraphTempHandler : implements IGraphTempHandler, public CInterface
{
protected:
    CFileUsageTable tmpFiles;
    CJobBase &job;
    mutable CriticalSection crit;
    bool errorOnMissing;

public:
    IMPLEMENT_IINTERFACE;

    CGraphTempHandler(CJobBase &_job, bool _errorOnMissing) : job(_job), errorOnMissing(_errorOnMissing) { }
    ~CGraphTempHandler()
    {
    }
    virtual void beforeDispose()
    {
        clearTemps();
    }
    virtual bool removeTemp(const char *name) = 0;
// IGraphTempHandler
    virtual void registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind, StringArray *clusters);
    virtual void deregisterFile(const char *name, bool kept=false);
    virtual void clearTemps();
    virtual IFileUsageIterator *getIterator()
    {
        class CIterator : implements IFileUsageIterator, public CInterface
        {
            SuperHashIteratorOf<CFileUsageEntry> iter;
        public:
            IMPLEMENT_IINTERFACE;
            CIterator(CFileUsageTable &table) : iter(table) { }
            virtual bool first() { return iter.first(); }
            virtual bool next() { return iter.next(); }
            virtual bool isValid() { return iter.isValid(); }
            virtual CFileUsageEntry & query() { return iter.query(); }
        };
        return new CIterator(tmpFiles);
    }
};

class graph_decl CGraphStub : public CInterface, implements IThorChildGraph
{
protected:
    graph_id graphId = 0;
public:
    IMPLEMENT_IINTERFACE;

    const void *queryFindParam() const { return &graphId; } // for SimpleHashTableOf

    const graph_id &queryGraphId() const { return graphId; }
    virtual CGraphBase &queryOriginalGraph() = 0;

    virtual void abort(IException *e) = 0;
    virtual bool serializeStats(MemoryBuffer &mb) = 0;

// IThorChildGraph
    virtual IEclGraphResults *evaluate(unsigned parentExtractSz, const byte * parentExtract) = 0;
};

class CJobBase;
interface IPropertyTree;
class graph_decl CGraphBase : public CGraphStub, implements IEclGraphResults
{
    mutable CriticalSection crit;
    CriticalSection evaluateCrit, executeCrit;
    CGraphElementTable containers;
    CGraphElementArray sinks, activeSinks;
    bool sink, complete, global, localChild;
    mutable int localOnly;
    activity_id parentActivityId;
    IPropertyTree *xgmml;
    CChildGraphTable childGraphsTable;
    CGraphStubArrayCopy orderedChildGraphs;
    Owned<IGraphTempHandler> tmpHandler;

    void clean();

    class CGraphCodeContext : implements ICodeContextExt
    {
        ICodeContextExt *ctx;
        CGraphBase *containerGraph, *parent;
    public:
        CGraphCodeContext() : parent(nullptr), containerGraph(nullptr), ctx(nullptr) { }
        void setContext(CGraphBase *_parent, CGraphBase *_containerGraph, ICodeContextExt *_ctx)
        {
            parent = _parent;
            containerGraph = _containerGraph;
            ctx = _ctx;
        }
        virtual const char *loadResource(unsigned id) { return ctx->loadResource(id); }
        virtual void setResultBool(const char *name, unsigned sequence, bool value) { ctx->setResultBool(name, sequence, value); }
        virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { ctx->setResultData(name, sequence, len, data); }
        virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { ctx->setResultDecimal(stepname, sequence, len, precision, isSigned, val); }
        virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { ctx->setResultInt(name, sequence, value, size); }
        virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { ctx->setResultRaw(name, sequence, len, data); }
        virtual void setResultReal(const char * stepname, unsigned sequence, double value) { ctx->setResultReal(stepname, sequence, value); }
        virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { ctx->setResultSet(name, sequence, isAll, len, data, transformer); }
        virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { ctx->setResultString(name, sequence, len, str); }
        virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { ctx->setResultUInt(name, sequence, value, size); }
        virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { ctx->setResultUnicode(name, sequence, len, str); }
        virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { ctx->setResultVarString(name, sequence, value); }
        virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { ctx->setResultVarUnicode(name, sequence, value); }
        virtual bool getResultBool(const char * name, unsigned sequence) { return ctx->getResultBool(name, sequence); }
        virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) { ctx->getResultData(tlen, tgt, name, sequence); }
        virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) { ctx->getResultDecimal(tlen, precision, isSigned, tgt, stepname, sequence); }
        virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { ctx->getResultRaw(tlen, tgt, name, sequence, xmlTransformer, csvTransformer); }
        virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { ctx->getResultSet(isAll, tlen, tgt, name, sequence, xmlTransformer, csvTransformer); }
        virtual __int64 getResultInt(const char * name, unsigned sequence) { return ctx->getResultInt(name, sequence); }
        virtual double getResultReal(const char * name, unsigned sequence) { return ctx->getResultReal(name, sequence); }
        virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) { ctx->getResultString(tlen, tgt, name, sequence); }
        virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) { ctx->getResultStringF(tlen, tgt, name, sequence); }
        virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) { ctx->getResultUnicode(tlen, tgt, name, sequence); }
        virtual char *getResultVarString(const char * name, unsigned sequence) { return ctx->getResultVarString(name, sequence); }
        virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) { return ctx->getResultVarUnicode(name, sequence); }
        virtual unsigned getResultHash(const char * name, unsigned sequence) { return ctx->getResultHash(name, sequence); }
        virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { return ctx->getExternalResultHash(wuid, name, sequence); }
        virtual const char *cloneVString(const char *str) const { return ctx->cloneVString(str); }
        virtual const char *cloneVString(size32_t len, const char *str) const { return ctx->cloneVString(len, str); }
        virtual char *getWuid() { return ctx->getWuid(); }
        virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { ctx->getExternalResultRaw(tlen, tgt, wuid, stepname, sequence, xmlTransformer, csvTransformer); }
        virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { ctx->executeGraph(graphName, realThor, parentExtractSize, parentExtract); }
        virtual char * getExpandLogicalName(const char * logicalName) { return ctx->getExpandLogicalName(logicalName); }
        virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) { ctx->addWuException(text, code, severity, source); }
        virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) { ctx->addWuAssertFailure(code, text, filename, lineno, column, isAbort); }
        virtual IUserDescriptor *queryUserDescriptor() { return ctx->queryUserDescriptor(); }
        virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash) { return ctx->getDatasetHash(name, hash); }
        virtual unsigned getNodes() { return ctx->getNodes(); }
        virtual unsigned getNodeNum() { return ctx->getNodeNum(); }
        virtual char *getFilePart(const char *logicalPart, bool create) { return ctx->getFilePart(logicalPart, create); }
        virtual unsigned __int64 getFileOffset(const char *logicalPart) { return ctx->getFileOffset(logicalPart); }
        virtual IDistributedFileTransaction *querySuperFileTransaction() { return ctx->querySuperFileTransaction(); }
        virtual char *getJobName() { return ctx->getJobName(); }
        virtual char *getJobOwner() { return ctx->getJobOwner(); }
        virtual char *getClusterName() { return ctx->getClusterName(); }
        virtual char *getGroupName() { return ctx->getGroupName(); }
        virtual char * queryIndexMetaData(char const * lfn, char const * xpath) { return ctx->queryIndexMetaData(lfn, xpath); }
        virtual unsigned getPriority() const { return ctx->getPriority(); }
        virtual char *getPlatform() { return ctx->getPlatform(); }
        virtual char *getEnv(const char *name, const char *defaultValue) const { return ctx->getEnv(name, defaultValue); }
        virtual char *getOS() { return ctx->getOS(); }
        virtual IThorChildGraph * resolveChildQuery(__int64 gid, IHThorArg * colocal)
        {
            return parent->getChildGraph((graph_id)gid);
        }
        virtual IEclGraphResults * resolveLocalQuery(__int64 gid)
        {
            if (gid == containerGraph->queryGraphId())
                return containerGraph;
            else
                return ctx->resolveLocalQuery(gid);
        }
        virtual char *getEnv(const char *name, const char *defaultValue) { return ctx->getEnv(name, defaultValue); }
        virtual unsigned logString(const char * text) const { return ctx->logString(text); }
        virtual const IContextLogger &queryContextLogger() const { return ctx->queryContextLogger(); }
        virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const { return ctx->getRowAllocator(meta, activityId); }
        virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const { return ctx->getRowAllocatorEx(meta, activityId, heapFlags); }
        virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override { ctx->getResultRowset(tcount, tgt, name, sequence, _rowAllocator, isGrouped, xmlTransformer, csvTransformer); }
        virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt,IEngineRowAllocator * _rowAllocator,  const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override { ctx->getResultDictionary(tcount, tgt, _rowAllocator, name, sequence, xmlTransformer, csvTransformer, hasher); }

        virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { convertRowToXML(lenResult, result, info, row, flags); }
        virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) { convertRowToJSON(lenResult, result, info, row, flags); }
        virtual IDebuggableContext *queryDebugContext() const override { return ctx->queryDebugContext(); }
        virtual unsigned getGraphLoopCounter() const
        {
            return containerGraph->queryLoopCounter();           // only called if value is valid
        }
        virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) { return ctx->getExternalResult(wuid, name, sequence); }
        virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) { return ctx->getResultForGet(name, sequence); }
        virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
        {
            return ctx->fromXml(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
        }
        virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
        {
            return ctx->fromJson(_rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
        }
        virtual IEngineContext *queryEngineContext()
        {
            return ctx->queryEngineContext();
        }
        virtual char *getDaliServers()
        {
            return ctx->getDaliServers();
        }
        virtual IWorkUnit *updateWorkUnit() const { return ctx->updateWorkUnit(); }
        virtual ISectionTimer * registerTimer(unsigned activityId, const char * name)
        {
            return ctx->registerTimer(activityId, name);
        }
   } graphCodeContext;

protected:
    Owned<IThorGraphResults> localResults, graphLoopResults;
    CGraphBase *owner, *parent, *graphResultsContainer;
    Owned<IException> abortException;
    Owned<IPropertyTree> node;
    IBarrier *startBarrier, *waitBarrier, *doneBarrier;
    mptag_t mpTag, startBarrierTag, waitBarrierTag, doneBarrierTag;
    bool connected, started, aborted, graphDone, sequential;
    bool initialized = false;
    std::atomic_bool progressUpdated;
    CJobBase &job;
    CJobChannel &jobChannel;
    mptag_t executeReplyTag;
    size32_t parentExtractSz; // keep track of sz when passed in, as may need to serialize later
    MemoryBuffer parentExtractMb; // retain copy, used if slave transmits to master (child graph 1st time initialization of global graph)
    unsigned counter;
    CReplyCancelHandler graphCancelHandler;
    bool loopBodySubgraph;

public:
    IMPLEMENT_IINTERFACE_USING(CGraphStub);

    CGraphArrayCopy dependentSubGraphs;

    CGraphBase(CJobChannel &jobChannel);
    ~CGraphBase();

    CGraphBase *cloneGraph();

    virtual CGraphBase &queryOriginalGraph() override { return *this; }
    virtual void init();
    void onCreate();
    void GraphPrintLog(const char *msg, ...) __attribute__((format(printf, 2, 3)));
    void GraphPrintLog(IException *e, const char *msg, ...) __attribute__((format(printf, 3, 4)));
    void GraphPrintLog(IException *e);
    void createFromXGMML(IPropertyTree *node, CGraphBase *owner, CGraphBase *parent, CGraphBase *resultsGraph, CGraphTableCopy &newGraphs);
    bool queryAborted() const { return aborted; }
    CJobBase &queryJob() const { return job; }
    CJobChannel &queryJobChannel() const { return jobChannel; }
    unsigned queryJobChannelNumber() const;
    IGraphTempHandler *queryTempHandler() const { assertex(tmpHandler.get()); return tmpHandler; }
    CGraphBase *queryOwner() { return owner; }
    CGraphBase *queryParent() { return parent?parent:this; }
    IMPServer &queryMPServer() const;
    void clearProgressUpdated() { progressUpdated.store(false); }
    void setProgressUpdated() { progressUpdated.store(true); }
    bool hasProgress() const { return progressUpdated; }
    bool checkProgressUpdatedAndClear() { return progressUpdated.exchange(false); }
    bool syncInitData();
    bool isComplete() const { return complete; }
    bool isGlobal() const { return global; }
    bool isStarted() const { return started; }
    bool isLocalOnly() const; // this graph and all upstream dependencies
    bool isLocalChild() const { return localChild; }
    bool isLoopSubGraph() const { return loopBodySubgraph; }
    bool containsActivities() const { return containers.count() != 0; }
    void setCompleteEx(bool tf=true) { complete = tf; }
    void setGlobal(bool tf) { global = tf; }
    void setLogging(bool tf);
    const byte *setParentCtx(size32_t _parentExtractSz, const byte *parentExtract)
    {
        parentExtractSz = _parentExtractSz;
        MemoryBuffer newParentExtract(parentExtractSz, parentExtract);
        parentExtractMb.swapWith(newParentExtract);
        return (const byte *)parentExtractMb.toByteArray();
    }
    virtual ICodeContext *queryCodeContext() { return &graphCodeContext; }
    void setLoopCounter(unsigned _counter) { counter = _counter; }
    unsigned queryLoopCounter() const { return counter; }
    virtual void setComplete(bool tf=true) { complete=tf; }
    virtual void deserializeCreateContexts(MemoryBuffer &mb);
    virtual void serializeCreateContexts(MemoryBuffer &mb);
    virtual void reset();
    void disconnectActivities()
    {
        CGraphElementIterator iter(containers);
        ForEach(iter)
        {
            CGraphElementBase &element = iter.query();
            element.releaseIOs();
        }
    }
    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    virtual void execute(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool async);
    IThorActivityIterator *getIterator()
    {
        return new CGraphElementIterator(containers);
    }
    IThorActivityIterator *getConnectedIterator(bool branchOnConditional=true);
    IThorActivityIterator *getSinkIterator() const
    {
        return new CGraphElementArrayIterator(activeSinks);
    }
    IThorActivityIterator *getAllSinkIterator() const
    {
        return new CGraphElementArrayIterator(sinks);
    }
    IPropertyTree &queryXGMML() const { return *xgmml; }
    void addActivity(CGraphElementBase *element)
    {
        if (containers.find((activity_id &)element->queryId()))
        {
            element->Release();
            return;
        }
        containers.replace(*element);
    }
    void addActiveSink(CGraphElementBase &sink)
    {
        activeSinks.append(*LINK(&sink));
    }
    unsigned activityCount() const
    {
        Owned<IPropertyTreeIterator> iter = xgmml->getElements("node");
        unsigned count=0;
        ForEach(*iter)
        {
            ThorActivityKind kind = (ThorActivityKind) iter->query().getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
            if (TAKsubgraph != kind)
                ++count;
        }
        return count;
    }
    CGraphElementBase *queryElement(activity_id id) const
    {
        CGraphElementBase *activity = containers.find(id);
        if (activity)
            return activity;
        if (owner)
            return owner->queryElement(id);
        return NULL;
    }
    bool isSink() const { return sink; }
    void setSink(bool tf)
    {
        sink = tf;
        xgmml->setPropBool("att[@name=\"rootGraph\"]/@value", tf);
    }
    const activity_id &queryParentActivityId() const { return parentActivityId; }
    void addChildGraph(CGraphStub *stub);
    unsigned queryChildGraphCount() { return childGraphsTable.ordinality(); }
    CGraphStub *getChildGraph(graph_id gid)
    {
        CriticalBlock b(crit);
        return LINK(childGraphsTable.find(gid));
    }
    IThorGraphIterator *getChildGraphIterator() const; // retrieves original child graphs
    IThorGraphStubIterator *getChildStubIterator() const; // retrieves child graph stubs, which redirect to parallel instances

    void executeChildGraphs(size32_t parentExtractSz, const byte *parentExtract);
    void doExecute(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies);
    void doExecuteChild(size32_t parentExtractSz, const byte *parentExtract);
    void executeChild(size32_t & retSize, void * & ret, size32_t parentExtractSz, const byte *parentExtract);
    void setResults(IThorGraphResults *results);
    virtual void executeChild(size32_t parentExtractSz, const byte *parentExtract, IThorGraphResults *results, IThorGraphResults *graphLoopResults);
    virtual void executeChild(size32_t parentExtractSz, const byte *parentExtract);
    virtual bool serializeStats(MemoryBuffer &mb) override { return false; }
    virtual bool prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async);
    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void start() = 0;
    virtual bool wait(unsigned timeout);
    virtual void done();
    virtual void end();
    virtual void abort(IException *e) override;
    virtual IThorGraphResults *createThorGraphResults(unsigned num);

// IExceptionHandler
    virtual bool fireException(IException *e);

    virtual IThorResult *getResult(unsigned id, bool distributed=false);
    virtual IThorResult *getGraphLoopResult(unsigned id, bool distributed=false);
    virtual IThorResult *createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    virtual IThorResult *createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    virtual IThorResult *createGraphLoopResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);

// IEclGraphResults
    virtual void getDictionaryResult(unsigned & count, const byte * * & ret, unsigned id) override;
    virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id) override;
    virtual const void * getLinkedRowResult(unsigned id);

// IThorChildGraph
//  virtual void getResult(size32_t & retSize, void * & ret, unsigned id);
//  virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id);
    virtual IEclGraphResults *evaluate(unsigned parentExtractSz, const byte * parentExtract);

friend class CGraphElementBase;
};

class CGraphTableIterator : implements IThorGraphIterator, public CInterface
{
    SuperHashIteratorOf<CGraphBase> iter;
public:
    IMPLEMENT_IINTERFACE;

    CGraphTableIterator(const CGraphTable &table) : iter(table) { }
    virtual bool first() { return iter.first(); }
    virtual bool next() { return iter.next(); }
    virtual bool isValid() { return iter.isValid(); }
    virtual CGraphBase & query() { return iter.query(); }
            CGraphBase & get() { CGraphBase &c = query(); c.Link(); return c; }
};

interface ILoadedDllEntry;
interface IConstWorkUnit;
class CThorCodeContextBase;

class graph_decl CJobBase : public CInterface, implements IDiskUsage, implements IExceptionHandler
{
protected:
    CriticalSection crit;
    Linked<ILoadedDllEntry> querySo;
    IUserDescriptor *userDesc;
    offset_t maxDiskUsage, diskUsage;
    StringAttr key, graphName;
    bool aborted, pausing, resumed;
    StringBuffer wuid, user, scope, token;
    mutable CriticalSection wuDirty;
    mutable bool dirty;
    mptag_t slavemptag;
    Owned<IGroup> jobGroup, slaveGroup, nodeGroup;
    Owned<IPropertyTree> xgmml;
    Owned<IGraphTempHandler> tmpHandler;
    bool timeActivities;
    unsigned channelsPerSlave;
    unsigned numChannels;
    unsigned maxActivityCores, globalMemoryMB, sharedMemoryMB;
    unsigned forceLogGraphIdMin, forceLogGraphIdMax;
    Owned<IContextLogger> logctx;
    Owned<IPerfMonHook> perfmonhook;
    CIArrayOf<CJobChannel> jobChannels;
    OwnedMalloc<unsigned> jobChannelSlaveNumbers;
    OwnedMalloc<unsigned> jobSlaveChannelNum;
    bool crcChecking;
    bool usePackedAllocator;
    rank_t myNodeRank;
    Owned<IPropertyTree> graphXGMML;
    unsigned memorySpillAtPercentage, sharedMemoryLimitPercentage;
    CriticalSection sharedAllocatorCrit;
    Owned<IThorAllocator> sharedAllocator;

    class CThorPluginCtx : public SimplePluginCtx
    {
    public:
        virtual int ctxGetPropInt(const char *propName, int defaultValue) const
        {
            return globals->getPropInt(propName, defaultValue);
        }
        virtual const char *ctxQueryProp(const char *propName) const
        {
            return globals->queryProp(propName);
        }
    } pluginCtx;
    SafePluginMap *pluginMap;
    virtual void endJob();
public:
    IMPLEMENT_IINTERFACE;

    CJobBase(ILoadedDllEntry *querySo, const char *graphName);
    virtual void beforeDispose();
    ~CJobBase();

    virtual void addChannel(IMPServer *mpServer) = 0;
    CJobChannel &queryJobChannel(unsigned c) const;
    CActivityBase &queryChannelActivity(unsigned c, graph_id gid, activity_id id) const;
    unsigned queryChannelsPerSlave() const { return channelsPerSlave; }
    unsigned queryJobChannels() const { return jobChannels.ordinality(); }
    inline unsigned queryJobChannelSlaveNum(unsigned channelNum) const { dbgassertex(channelNum<queryJobChannels()); return jobChannelSlaveNumbers[channelNum]; }
    inline unsigned queryJobSlaveChannelNum(unsigned slaveNum) const { dbgassertex(slaveNum && slaveNum<=querySlaves()); return jobSlaveChannelNum[slaveNum-1]; }
    ICommunicator &queryNodeComm() const { return ::queryNodeComm(); }
    const rank_t &queryMyNodeRank() const { return myNodeRank; }
    void init();
    void setXGMML(IPropertyTree *_xgmml) { xgmml.set(_xgmml); }
    IPropertyTree *queryXGMML() { return xgmml; }
    IPropertyTree *queryGraphXGMML() const { return graphXGMML; }
    bool queryAborted() const { return aborted; }
    const char *queryKey() const { return key; }
    const char *queryGraphName() const { return graphName; }
    bool queryForceLogging(graph_id graphId, bool def) const;
    const IContextLogger &queryContextLogger() const { return *logctx; }
    virtual void startJob();
    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing) = 0;
    void addDependencies(IPropertyTree *xgmml, bool failIfMissing=true);
    void addSubGraph(IPropertyTree &xgmml);

    bool queryUseCheckpoints() const;
    bool queryPausing() const { return pausing; }
    bool queryResumed() const { return resumed; }
    IGraphTempHandler *queryTempHandler() const { return tmpHandler; }
    ILoadedDllEntry &queryDllEntry() const { return *querySo; }
    IUserDescriptor *queryUserDescriptor() const { return userDesc; }
    virtual IConstWorkUnit &queryWorkUnit() const { throwUnexpected(); }
    virtual void markWuDirty() { };
    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const = 0;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const = 0;
    virtual bool getWorkUnitValueBool(const char *prop, bool defVal) const = 0;
    const char *queryWuid() const { return wuid.str(); }
    const char *queryUser() const { return user.str(); }
    const char *queryScope() const { return scope.str(); }
    IDiskUsage &queryIDiskUsage() const { return *(IDiskUsage *)this; }
    void setDiskUsage(offset_t _diskUsage) { diskUsage = _diskUsage; }
    const offset_t queryMaxDiskUsage() const { return maxDiskUsage; }
    mptag_t querySlaveMpTag() const { return slavemptag; }
    unsigned querySlaves() const { return slaveGroup->ordinality(); }
    unsigned queryNodes() const { return nodeGroup->ordinality()-1; }
    IGroup &queryJobGroup() const { return *jobGroup; }
    inline bool queryTimeActivities() const { return timeActivities; }
    unsigned queryMaxDefaultActivityCores() const { return maxActivityCores; }
    IGroup &querySlaveGroup() const { return *slaveGroup; }
    virtual mptag_t deserializeMPTag(MemoryBuffer &mb) { throwUnexpected(); }
    virtual mptag_t allocateMPTag() { throwUnexpected(); }
    virtual void freeMPTag(mptag_t tag) { throwUnexpected(); }
    StringBuffer &getOpt(const char *opt, StringBuffer &out);
    bool getOptBool(const char *opt, bool dft=false);
    int getOptInt(const char *opt, int dft=0);
    unsigned getOptUInt(const char *opt, unsigned dft=0) { return (unsigned)getOptInt(opt, dft); }
    __int64 getOptInt64(const char *opt, __int64 dft=0);
    unsigned __int64 getOptUInt64(const char *opt, unsigned __int64 dft=0) { return (unsigned __int64)getOptInt64(opt, dft); }
    IThorAllocator *querySharedAllocator() const { return sharedAllocator; }
    unsigned getWfid() const { return graphXGMML->getPropInt("@wfid"); }
    virtual IThorAllocator *getThorAllocator(unsigned channel);

    virtual void abort(IException *e);
    virtual void debugRequest(MemoryBuffer &msg, const char *request) const { }

//
    virtual void addCreatedFile(const char *file) { assertex(false); }
    virtual __int64 addNodeDiskUsage(unsigned node, __int64 sz) { assertex(false); return 0; }

// IDiskUsage
    virtual void increase(offset_t usage, const char *key=NULL);
    virtual void decrease(offset_t usage, const char *key=NULL);

// IExceptionHandler
    virtual bool fireException(IException *e) = 0;
};

interface IGraphCallback
{
    virtual void runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract) = 0;
};

interface IGraphExecutor : extends IInterface
{
    virtual void add(CGraphBase *subGraph, IGraphCallback &callback, bool checkDependencies, size32_t parentExtractSz, const byte *parentExtract) = 0;
    virtual IThreadPool &queryGraphPool() = 0 ;
    virtual void wait() = 0;
};

interface IThorAllocator;
class graph_decl CJobChannel : public CInterface, implements IGraphCallback, implements IExceptionHandler
{
protected:
    CJobBase &job;
    Owned<IThorAllocator> thorAllocator;
    Owned<IGraphExecutor> graphExecutor;
    CriticalSection crit;
    CGraphTable subGraphs;
    CGraphTableCopy allGraphs; // for lookup, includes all child graphs
    Owned<ICommunicator> jobComm;
    rank_t myrank;
    Linked<IMPServer> mpServer;
    bool aborted;
    Owned<CThorCodeContextBase> codeCtx;
    Owned<CThorCodeContextBase> sharedMemCodeCtx;
    unsigned channel;

    void removeAssociates(CGraphBase &graph)
    {
        CriticalBlock b(crit);
        allGraphs.removeExact(&graph);
        Owned<IThorGraphIterator> iter = graph.getChildGraphIterator();
        ForEach(*iter)
        {
            CGraphBase &child = iter->query();
            removeAssociates(child);
        }
    }
public:
    IMPLEMENT_IINTERFACE;

    CJobChannel(CJobBase &job, IMPServer *mpServer, unsigned channel);
    ~CJobChannel();

    CJobBase &queryJob() const { return job; }
    void clean();
    void init();
    void wait();
    virtual CGraphBase *createGraph() = 0;
    void startGraph(CGraphBase &graph, bool checkDependencies, size32_t parentExtractSize, const byte *parentExtract);
    INode *queryMyNode();
    unsigned queryChannel() const { return channel; }
    bool isPrimary() const { return 0 == channel; }
    void addDependencies(IPropertyTree *xgmml, bool failIfMissing=true);
    void addSubGraph(CGraphBase &graph)
    {
        CriticalBlock b(crit);
        subGraphs.replace(graph);
        allGraphs.replace(graph);
    }
    void removeSubGraph(CGraphBase &graph)
    {
        CriticalBlock b(crit);
        removeAssociates(graph);
        subGraphs.removeExact(&graph);
    }
    CGraphTableCopy &queryAllGraphs() { return allGraphs; }
    IThorGraphIterator *getSubGraphs();
    CGraphBase *getGraph(graph_id gid)
    {
        CriticalBlock b(crit);
        return LINK(allGraphs.find(gid));
    }

    ICodeContext &queryCodeContext() const;
    ICodeContext &querySharedMemCodeContext() const;
    IThorResult *getOwnedResult(graph_id gid, activity_id ownerId, unsigned resultId);
    IThorAllocator *queryThorAllocator() const { return thorAllocator; }
    ICommunicator &queryJobComm() const { return *jobComm; }
    IMPServer &queryMPServer() const { return *mpServer; }
    const rank_t &queryMyRank() const { return myrank; }
    mptag_t deserializeMPTag(MemoryBuffer &mb);
    IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, activity_id activityId, roxiemem::RoxieHeapFlags flags=roxiemem::RHFnone) const;
    roxiemem::IRowManager *queryRowManager() const;

    virtual void abort(IException *e);
    virtual IBarrier *createBarrier(mptag_t tag) { UNIMPLEMENTED; return NULL; }

// IGraphCallback
    virtual void runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract);
// IExceptionHandler
    virtual bool fireException(IException *e) = 0;
};

interface IOutputMetaData;

inline activity_id createCompoundActSeqId(activity_id actId, byte seq)
{
    if (seq)
        actId |= seq << 24;
    return actId;
}

class graph_decl CActivityBase : implements CInterfaceOf<IThorRowInterfaces>, implements IExceptionHandler
{
    Owned<IEngineRowAllocator> rowAllocator;
    Owned<IOutputRowSerializer> rowSerializer;
    Owned<IOutputRowDeserializer> rowDeserializer;
    CSingletonLock CABallocatorlock;
    CSingletonLock CABserializerlock;
    CSingletonLock CABdeserializerlock;

protected:
    CGraphElementBase &container;
    Linked<IHThorArg> baseHelper;
    mptag_t mpTag; // to be used by any direct inter master<->slave communication
    bool abortSoon;
    bool timeActivities; // purely for access efficiency
    size32_t parentExtractSz;
    const byte *parentExtract;
    bool receiving, cancelledReceive, initialized, reInit;
    Owned<IThorGraphResults> ownedResults; // NB: probably only to be used by loop results

public:
    CActivityBase(CGraphElementBase *container);
    ~CActivityBase();
    inline activity_id queryId() const { return container.queryId(); }
    CGraphElementBase &queryContainer() const { return container; }
    CJobBase &queryJob() const { return container.queryJob(); }
    CJobChannel &queryJobChannel() const { return container.queryJobChannel(); }
    unsigned queryJobChannelNumber() const { return queryJobChannel().queryChannel(); }
    inline IMPServer &queryMPServer() const { return queryJobChannel().queryMPServer(); }
    CGraphBase &queryGraph() const { return container.queryOwner(); }
    CActivityBase &queryChannelActivity(unsigned channel) const { return queryJob().queryChannelActivity(channel, queryGraph().queryGraphId(), queryId()); }
    inline const mptag_t queryMpTag() const { return mpTag; }
    inline bool queryAbortSoon() const { return abortSoon; }
    inline IHThorArg *queryHelper() const { return baseHelper; }
    inline bool needReInit() const { return reInit; }
    inline bool queryInitialized() const { return initialized; }
    inline void setInitialized(bool tf) { initialized = tf; }
    inline bool queryTimeActivities() const { return timeActivities; }
    inline roxiemem::RoxieHeapFlags queryHeapFlags() const { return (roxiemem::RoxieHeapFlags)container.getOptInt("heapflags", defaultHeapFlags); }

    void onStart(size32_t _parentExtractSz, const byte *_parentExtract) { parentExtractSz = _parentExtractSz; parentExtract = _parentExtract; }
    bool receiveMsg(ICommunicator &comm, CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER);
    bool receiveMsg(CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER);
    void cancelReceiveMsg(ICommunicator &comm, const rank_t rank, const mptag_t mpTag);
    void cancelReceiveMsg(const rank_t rank, const mptag_t mpTag);
    bool firstNode() { return 1 == container.queryJobChannel().queryMyRank(); }
    bool lastNode() { return container.queryJob().querySlaves() == container.queryJobChannel().queryMyRank(); }
    unsigned queryMaxCores() const { return container.queryMaxCores(); }
    IThorRowInterfaces *getRowInterfaces();
    IEngineRowAllocator *getRowAllocator(IOutputMetaData * meta, roxiemem::RoxieHeapFlags flags=roxiemem::RHFnone, byte seq=0) const;

    bool appendRowXml(StringBuffer & target, IOutputMetaData & meta, const void * row) const;
    void logRow(const char * prefix, IOutputMetaData & meta, const void * row);

    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx) { }
    virtual void clearConnections() { }
    virtual void releaseIOs() { }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract) { }
    virtual void startProcess(bool async=true) { }
    virtual bool wait(unsigned timeout) { return true; } // NB: true == success
    virtual void reset() { receiving = abortSoon = cancelledReceive = false; }
    virtual void done() { }
    virtual void kill();
    virtual void abort();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const = 0;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const = 0;
    virtual IThorGraphResults *queryResults() { return ownedResults; }

    void ActPrintLog(const char *format, ...) __attribute__((format(printf, 2, 3)));
    void ActPrintLog(IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
    void ActPrintLog(IException *e);

    IThorRowInterfaces * createRowInterfaces(IOutputMetaData * meta, byte seq=0);
    IThorRowInterfaces * createRowInterfaces(IOutputMetaData * meta, roxiemem::RoxieHeapFlags heapFlags, byte seq=0);

// IExceptionHandler
    bool fireException(IException *e);
    __declspec(noreturn) void processAndThrowOwnedException(IException * e) __attribute__((noreturn));

// IThorRowInterfaces
    virtual IEngineRowAllocator * queryRowAllocator();  
    virtual IOutputRowSerializer * queryRowSerializer(); 
    virtual IOutputRowDeserializer * queryRowDeserializer(); 
    virtual IOutputMetaData *queryRowMetaData() { return baseHelper->queryOutputMeta(); }
    virtual unsigned queryActivityId() const { return (unsigned)queryId(); }
    virtual ICodeContext *queryCodeContext() { return container.queryCodeContext(); }
    virtual roxiemem::IRowManager *queryRowManager() const { return queryJobChannel().queryRowManager(); }

    StringBuffer &getOpt(const char *prop, StringBuffer &out) const { return container.getOpt(prop, out); }
    bool getOptBool(const char *prop, bool defVal=false) const { return container.getOptBool(prop, defVal); }
    int getOptInt(const char *prop, int defVal=0) const { return container.getOptInt(prop, defVal); }
    unsigned getOptUInt(const char *prop, unsigned defVal=0) const { return container.getOptUInt(prop, defVal); }
    __int64 getOptInt64(const char *prop, __int64 defVal=0) const { return container.getOptInt64(prop, defVal); }
    unsigned __int64 getOptUInt64(const char *prop, unsigned __int64 defVal=0) const { return container.getOptUInt64(prop, defVal); }
};

interface IFileInProgressHandler : extends IInterface
{
    virtual void add(const char *fip) = 0;
    virtual void remove(const char *fip) = 0;
};

class CFIPScope
{
    StringAttr fip;
public:
    CFIPScope() { }
    CFIPScope(const char *_fip) : fip(_fip)
    {
        queryThor().queryFileInProgressHandler().add(fip);
    }
    ~CFIPScope()
    {
        if (fip)
            queryThor().queryFileInProgressHandler().remove(fip);
    }
    void set(const char *_fip)
    {
        fip.set(_fip);
    }
    void clear()
    {
        fip.clear();
    }
};

interface IDelayedFile;
interface IExpander;
interface IThorFileCache : extends IInterface
{
    virtual bool remove(const char *filename) = 0;
    virtual IDelayedFile *lookup(CActivityBase &activity, const char *logicalFilenae, IPartDescriptor &partDesc, IExpander *expander=NULL) = 0;
};

class graph_decl CThorResourceBase : implements IThorResource, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

// IThorResource
    virtual IThorFileCache &queryFileCache() override { UNIMPLEMENTED; }
    virtual IBackup &queryBackup() override  { UNIMPLEMENTED; }
    virtual IFileInProgressHandler &queryFileInProgressHandler() override  { UNIMPLEMENTED; }
    virtual IKJService &queryKeyedJoinService() override { UNIMPLEMENTED; }
};

class graph_decl CThorGraphResults : implements IThorGraphResults, public CInterface
{
protected:
    class CThorUninitializedGraphResults : implements IThorResult, public CInterface
    {
        unsigned id;

    public:
        IMPLEMENT_IINTERFACE

        CThorUninitializedGraphResults(unsigned _id) { id = _id; }
        virtual IRowWriter *getWriter() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual IRowStream *getRowStream() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual IThorRowInterfaces *queryRowInterfaces() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual CActivityBase *queryActivity() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual bool isDistributed() const { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void serialize(MemoryBuffer &mb) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void getResult(size32_t & retSize, void * & ret) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void getLinkedResult(unsigned & count, const byte * * & ret) override { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual const void * getLinkedRowResult() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
    };
    IArrayOf<IThorResult> results;
    CriticalSection cs;
    activity_id ownerId;
    void ensureAtLeast(unsigned id)
    {
        while (results.ordinality() < id)
            results.append(*new CThorUninitializedGraphResults(results.ordinality()));
    }
public:
    IMPLEMENT_IINTERFACE;

    CThorGraphResults(unsigned _numResults) { ensureAtLeast(_numResults); ownerId = 0; }
    virtual void clear()
    {
        CriticalBlock procedure(cs);
        results.kill();
    }
    virtual IThorResult *getResult(unsigned id, bool distributed)
    {
        CriticalBlock procedure(cs);
        ensureAtLeast(id+1);
        // NB: stream static after this, i.e. nothing can be added to this result
        return LINK(&results.item(id));
    }
    virtual IThorResult *createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    virtual IThorResult *createResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT)
    {
        return createResult(activity, results.ordinality(), rowIf, resultType, spillPriority);
    }
    virtual unsigned addResult(IThorResult *result)
    {
        CriticalBlock procedure(cs);
        unsigned id = results.ordinality();
        setResult(id, result);
        return id;
    }
    virtual void setResult(unsigned id, IThorResult *result)
    {
        CriticalBlock procedure(cs);
        ensureAtLeast(id);
        if (results.isItem(id))
            results.replace(*LINK(result), id);
        else
            results.append(*LINK(result));
    }
    virtual unsigned count() { return results.ordinality(); }
    virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id) override
    {
        Owned<IThorResult> result = getResult(id, true);
        result->getLinkedResult(count, ret);
    }
    virtual void getDictionaryResult(unsigned & count, const byte * * & ret, unsigned id) override
    {
        Owned<IThorResult> result = getResult(id, true);
        result->getLinkedResult(count, ret);
    }
    virtual const void * getLinkedRowResult(unsigned id)
    {
        Owned<IThorResult> result = getResult(id, true);
        return result->getLinkedRowResult();
    }

    virtual void setOwner(activity_id _ownerId) { ownerId = _ownerId; }
    virtual activity_id queryOwnerId() const { return ownerId; }
};

extern graph_decl IThorResult *createResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resulType, unsigned spillPriority=SPILL_PRIORITY_RESULT);


class CGraphElementBase;
typedef CGraphElementBase *(*CreateFunc)(IPropertyTree &node, CGraphBase &owner, CGraphBase *resultsGraph);
extern graph_decl void registerCreateFunc(CreateFunc func);
extern graph_decl CGraphElementBase *createGraphElement(IPropertyTree &node, CGraphBase &owner, CGraphBase *resultsGraph);
extern graph_decl IThorBoundLoopGraph *createBoundLoopGraph(CGraphBase *graph, IOutputMetaData *resultMeta, unsigned activityId);
extern graph_decl bool isDiskInput(ThorActivityKind kind);
extern graph_decl bool isLoopActivity(CGraphElementBase &container);
extern graph_decl void traceMemUsage();


#endif

