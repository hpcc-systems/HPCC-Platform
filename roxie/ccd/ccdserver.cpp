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

#include "jmisc.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "rtlkey.hpp"
#include "jsort.hpp"
#include "jhtree.hpp"
#include "jqueue.tpp"
#include "jisem.hpp"
#include "thorxmlread.hpp"
#include "thorrparse.ipp"
#include "thorxmlwrite.hpp"
#include "thorsoapcall.hpp"
#include "thorcommon.ipp"
#include "jlzw.hpp"
#include "javahash.hpp"
#include "javahash.tpp"
#include "thorstep.ipp"
#include "thorpipe.hpp"
#include "thorfile.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"
#include "rtlread_imp.hpp"

#include "dafdesc.hpp"
#include "dautils.hpp"

namespace ccdserver_hqlhelper
{
#include "eclhelper_base.hpp"
}

#include "ccd.hpp"
#include "ccdserver.hpp"
#include "ccdcontext.hpp"
#include "ccdactivities.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"
#include "ccddali.hpp"
#include "jsmartsock.hpp"

#include "dllserver.hpp"
#include "workflow.hpp"
#include "roxiemem.hpp"
#include "roxierowbuff.hpp"

#include "roxiehelper.hpp"
#include "roxielmj.hpp"
#include "roxierow.hpp"

#include "thorplugin.hpp"
#include "keybuild.hpp"

#define MAX_HTTP_HEADERSIZE 8000
#define MIN_PAYLOAD_SIZE 800

#pragma warning(disable : 4355)
#define DEFAULT_PARALLEL_LOOP_THREADS       1

#define PROBE
#ifdef _DEBUG
//#define FAKE_EXCEPTIONS
//#define TRACE_JOINGROUPS
//#define TRACE_SPLIT
//#define _CHECK_HEAPSORT
//#undef PARALLEL_EXECUTE
//#define TRACE_SEEK_REQUESTS
#endif

using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedRoxieString;
using roxiemem::OwnedConstRoxieRow;
using roxiemem::IRowManager;

// There is a bug in VC6 implemetation of protected which prevents nested classes from accessing owner's data. It can be tricky to work around - hence...
#if _MSC_VER==1200
#define protected public
#endif

#define TRACE_STARTSTOP  // This determines if it is available - it is enabled/disabled by a configuration option
 
static const SmartStepExtra dummySmartStepExtra(SSEFreadAhead, NULL);

inline void ReleaseRoxieRowSet(ConstPointerArray &data)
{
    ForEachItemIn(idx, data)
        ReleaseRoxieRow(data.item(idx));
    data.kill();
}

//=================================================================================

class RestartableThread : public CInterface
{
    class MyThread : public Thread
    {
        Linked<RestartableThread> owner;
    public:
        MyThread(RestartableThread *_owner, const char *name) : Thread(name), owner(_owner)
        {
        }
        virtual int run()
        {
            owner->started.signal();
            return owner->run();
        }
    };
    friend class MyThread;
    Semaphore started;
    Owned<MyThread> thread;
    CriticalSection crit;
    StringAttr name;
public:
    RestartableThread(const char *_name) : name(_name)
    {
    }
    virtual void start(const char *namePrefix)
    {
        StringBuffer s(namePrefix);
        s.append(name);
        {
            CriticalBlock b(crit);
            assertex(!thread);
            thread.setown(new MyThread(this, s));
            thread->start();
        }
        started.wait();
    }

    virtual void join()
    {
        {
            Owned<Thread> tthread;
            {
                CriticalBlock b(crit);
                tthread.setown(thread.getClear());
            }
            if (tthread)
                tthread->join();
        }
    }

    virtual int run() = 0;

};

//================================================================================

// default implementation - can be overridden for efficiency...
bool IRoxieInput::nextGroup(ConstPointerArray & group)
{
    // MORE - this should be replaced with a version that reads to a builder
    const void * next;
    while ((next = nextInGroup()) != NULL)
        group.append(next);
    if (group.ordinality())
        return true;
    return false;
}

void IRoxieInput::readAll(RtlLinkedDatasetBuilder &builder)
{
    loop
    {
        const void *nextrec = nextInGroup();
        if (!nextrec)
        {
            nextrec = nextInGroup();
            if (!nextrec)
                break;
            builder.appendEOG();
        }
        builder.appendOwn(nextrec);
    }
}

inline const void * nextUngrouped(IRoxieInput * input)
{
    const void * ret = input->nextInGroup();
    if (!ret)
        ret = input->nextInGroup();
    return ret;
};

//=================================================================================

//The following don't link their arguments because that creates a circular reference
//But I wish there was a better way
class IndirectSlaveContext : public CInterface, implements IRoxieSlaveContext
{
public:
    IndirectSlaveContext(IRoxieSlaveContext * _ctx = NULL) : ctx(_ctx) {}
    IMPLEMENT_IINTERFACE

    void set(IRoxieSlaveContext * _ctx) { ctx = _ctx; }

    virtual ICodeContext *queryCodeContext()
    {
        return ctx->queryCodeContext();
    }
    virtual void checkAbort() 
    {
        ctx->checkAbort();
    }
    virtual void notifyAbort(IException *E) 
    {
        ctx->notifyAbort(E);
    }
    virtual IActivityGraph * queryChildGraph(unsigned id) 
    {
        return ctx->queryChildGraph(id);
    }
    virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph) 
    {
        ctx->noteChildGraph(id, childGraph) ;
    }
    virtual IRowManager &queryRowManager() 
    {
        return ctx->queryRowManager();
    }
    virtual void noteStatistic(unsigned statCode, unsigned __int64 value, unsigned count) const
    {
        ctx->noteStatistic(statCode, value, count);
    }
    virtual void CTXLOG(const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        ctx->CTXLOGva(format, args);
        va_end(args);
    }
    virtual void CTXLOGva(const char *format, va_list args) const
    {
        ctx->CTXLOGva(format, args);
    }
    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const
    {
        ctx->CTXLOGa(category, prefix, text);
    }
    virtual void logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        ctx->logOperatorExceptionVA(E, file, line, format, args);
        va_end(args);
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const
    {
        ctx->logOperatorExceptionVA(E, file, line, format, args);
    }
    virtual void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        ctx->CTXLOGaeva(E, file, line, prefix, format, args);
        va_end(args);
    }
    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const
    {
        ctx->CTXLOGaeva(E, file, line, prefix, format, args);
    }
    virtual void CTXLOGl(LogItem *log) const
    {
        ctx->CTXLOGl(log);
    }
    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
    {
        return ctx->getLogPrefix(ret);
    }
    virtual unsigned queryTraceLevel() const
    {
        return ctx->queryTraceLevel();
    }
    virtual bool isIntercepted() const
    {
        return ctx->isIntercepted();
    }
    virtual bool isBlind() const
    {
        return ctx->isBlind();
    }
    virtual unsigned parallelJoinPreload() 
    {
        return ctx->parallelJoinPreload();
    }
    virtual unsigned concatPreload() 
    {
        return ctx->concatPreload();
    }
    virtual unsigned fetchPreload() 
    {
        return ctx->fetchPreload();
    }
    virtual unsigned fullKeyedJoinPreload() 
    {
        return ctx->fullKeyedJoinPreload();
    }
    virtual unsigned keyedJoinPreload() 
    {
        return ctx->keyedJoinPreload();
    }
    virtual unsigned prefetchProjectPreload() 
    {
        return ctx->prefetchProjectPreload();
    }
    virtual void addSlavesReplyLen(unsigned len) 
    {
        ctx->addSlavesReplyLen(len);
    }
    virtual const char *queryAuthToken() 
    {
        return ctx->queryAuthToken();
    }
    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt)
    {
        return ctx->resolveLFN(filename, isOpt);
    }
    virtual IRoxieWriteHandler *createLFN(const char *filename, bool overwrite, bool extend, const StringArray &clusters)
    {
        return ctx->createLFN(filename, overwrite, extend, clusters);
    }
    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal)
    {
        ctx->onFileCallback(header, lfn, isOpt, isLocal);
    }
    virtual IActivityGraph *getLibraryGraph(const LibraryCallFactoryExtra &extra, IRoxieServerActivity *parentActivity)
    {
        return ctx->getLibraryGraph(extra, parentActivity);
    }
    virtual void noteProcessed(const IRoxieContextLogger &_activityContext, const IRoxieServerActivity *_activity, unsigned _idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const
    {
        ctx->noteProcessed(_activityContext, _activity, _idx, _processed, _totalCycles, _localCycles);
    }
    virtual IProbeManager *queryProbeManager() const
    {
        return ctx->queryProbeManager();
    }
    virtual IDebuggableContext *queryDebugContext() const
    {
        return ctx->queryDebugContext();
    }
    virtual bool queryTimeActivities() const
    {
        return ctx->queryTimeActivities();
    }
    virtual void printResults(IXmlWriter *output, const char *name, unsigned sequence)
    {
        ctx->printResults(output, name, sequence);
    }
    virtual void setWUState(WUState state)
    {
        ctx->setWUState(state);
    }
    virtual bool checkWuAborted()
    {
        return ctx->checkWuAborted();
    }
    virtual IWorkUnit *updateWorkUnit() const
    {
        return ctx->updateWorkUnit();
    }
    virtual IConstWorkUnit *queryWorkUnit() const
    {
        return ctx->queryWorkUnit();
    }
    virtual IRoxieServerContext *queryServerContext()
    {
        return ctx->queryServerContext();
    }
    virtual IWorkUnitRowReader *getWorkunitRowReader(const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, IEngineRowAllocator *rowAllocator, bool isGrouped)
    {
        return ctx->getWorkunitRowReader(name, sequence, xmlTransformer, rowAllocator, isGrouped);
    }
protected:
    IRoxieSlaveContext * ctx;
};

//=================================================================================

#define RESULT_FLUSH_THRESHOLD 10000u

#ifdef _DEBUG
#define SOAP_SPLIT_THRESHOLD 100u
#define SOAP_SPLIT_RESERVE 200u
#else
#define SOAP_SPLIT_THRESHOLD 64000u
#define SOAP_SPLIT_RESERVE 65535u
#endif

//=================================================================================

class CRoxieServerActivityFactoryBase : public CActivityFactory, implements IRoxieServerActivityFactory
{
protected:
    IntArray dependencies; // things I am dependent on
    IntArray dependencyIndexes; // things I am dependent on
    IntArray dependencyControlIds; // things I am dependent on
    StringArray dependencyEdgeIds; // How to describe them to the debugger
    unsigned dependentCount; // things dependent on me

    mutable CriticalSection statsCrit;
    mutable __int64 processed;
    mutable __int64 started;
    mutable __int64 totalCycles;
    mutable __int64 localCycles;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerActivityFactoryBase(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        processed = 0;
        started = 0;
        totalCycles = 0;
        localCycles = 0;
        dependentCount = 0;
    }
    
    ~CRoxieServerActivityFactoryBase()
    {
    }

    StringBuffer &toString(StringBuffer &ret) const
    {
        return ret.appendf("%p", this);
    }

    virtual void addDependency(unsigned _source, ThorActivityKind _kind, unsigned _sourceIdx, int controlId, const char *edgeId)
    {
        dependencies.append(_source); 
        dependencyIndexes.append(_sourceIdx); 
        dependencyControlIds.append(controlId);
        dependencyEdgeIds.append(edgeId);
    }

    virtual void noteDependent(unsigned target)
    {
        dependentCount++;
    }

    virtual IntArray &queryDependencies() { return dependencies; } 
    virtual IntArray &queryDependencyIndexes() {    return dependencyIndexes; } 
    virtual IntArray &queryDependencyControlIds() { return dependencyControlIds; }
    virtual StringArray &queryDependencyEdgeIds() { return dependencyEdgeIds; }
    virtual unsigned queryId() const { return id; }
    virtual unsigned querySubgraphId() const { return subgraphId; }
    virtual ThorActivityKind getKind() const { return kind; }
    virtual IOutputMetaData * queryOutputMeta() const
    {
        return meta;
    }
    virtual bool isSink() const
    {
        return false;
    }
    virtual bool isFunction() const
    {
        return false;
    }
    virtual bool isGraphInvariant() const
    {
        return false;
    }
    virtual IHThorArg &getHelper() const
    {
        return *helperFactory();
    }
    virtual IRoxieServerActivity *createFunction(IHThorArg &arg, IProbeManager *_probeManager) const
    {
        arg.Release();
        throwUnexpected();
    }

    virtual void noteProcessed(unsigned idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const
    {
        if (_processed || _totalCycles || _localCycles)
        {
            CriticalBlock b(statsCrit);
#ifdef _DEBUG
            assertex(_totalCycles >= _localCycles);
#endif
            processed += _processed;
            totalCycles += _totalCycles;
            localCycles += _localCycles;
        }
    }

    virtual void noteStarted() const
    {
        CriticalBlock b(statsCrit);
        started ++;
    }

    virtual void noteStarted(unsigned idx) const
    {
        throwUnexpected(); // should be implemented/required by multiOutput cases only
    }

    virtual void getEdgeProgressInfo(unsigned output, IPropertyTree &edge) const
    {
        CriticalBlock b(statsCrit);
        if (output == 0)
        {
            putStatsValue(&edge, "count", "sum", processed);
            if (started)
                putStatsValue(&edge, "started", "sum", started);
        }
        else
            ERRLOG("unexpected call to getEdgeProcessInfo for output %d in activity %d", output, queryId());
    }

    virtual void getNodeProgressInfo(IPropertyTree &node) const
    {
        CActivityFactory::getNodeProgressInfo(node);
        CriticalBlock b(statsCrit);
        if (started)
            putStatsValue(&node, "_roxieStarted", "sum", started);
        if (totalCycles)
            putStatsValue(&node, "totalTime", "sum", (unsigned) (cycle_to_nanosec(totalCycles)/1000));
        if (localCycles)
            putStatsValue(&node, "localTime", "sum", (unsigned) (cycle_to_nanosec(localCycles)/1000));
    }

    virtual void resetNodeProgressInfo()
    {
        CActivityFactory::resetNodeProgressInfo();
        CriticalBlock b(statsCrit);
        started = 0;
        totalCycles = 0;
        localCycles = 0;
    }
    virtual void getActivityMetrics(StringBuffer &reply) const
    {
        CActivityFactory::getActivityMetrics(reply);
        CriticalBlock b(statsCrit);
        putStatsValue(reply, "_roxieStarted", "sum", started);
        putStatsValue(reply, "totalTime", "sum", (unsigned) (cycle_to_nanosec(totalCycles)/1000));
        putStatsValue(reply, "localTime", "sum", (unsigned) (cycle_to_nanosec(localCycles)/1000));
    }
    virtual unsigned __int64 queryLocalCycles() const
    {
        return localCycles;
    }

    virtual IQueryFactory &queryQueryFactory() const
    {
        return CActivityFactory::queryQueryFactory();
    }

    virtual ActivityArray *queryChildQuery(unsigned idx, unsigned &id)
    {
        return CActivityFactory::queryChildQuery(idx, id);
    }

    virtual void addChildQuery(unsigned id, ActivityArray *childQuery)
    {
        CActivityFactory::addChildQuery(id, childQuery);
    }

    virtual void createChildQueries(IArrayOf<IActivityGraph> &childGraphs, IRoxieServerActivity *parentActivity, IProbeManager *_probeManager, const IRoxieContextLogger &_logctx) const
    {
        ForEachItemIn(idx, childQueries)
        {
            childGraphs.append(*createActivityGraph(NULL, childQueryIndexes.item(idx), childQueries.item(idx), parentActivity, _probeManager, _logctx));
        }
    }

    virtual void onCreateChildQueries(IRoxieSlaveContext *ctx, IHThorArg *colocalArg, IArrayOf<IActivityGraph> &childGraphs) const
    {
        ForEachItemIn(idx, childGraphs)
        {
            ctx->noteChildGraph(childQueryIndexes.item(idx), &childGraphs.item(idx));
            childGraphs.item(idx).onCreate(ctx, colocalArg);
        }
    }

    IActivityGraph * createChildGraph(IRoxieSlaveContext * ctx, IHThorArg *colocalArg, unsigned childId, IRoxieServerActivity *parentActivity, IProbeManager * _probeManager, const IRoxieContextLogger &_logctx) const
    {
        unsigned match = childQueryIndexes.find(childId);
        assertex(match != NotFound);
        Owned<IActivityGraph> graph = createActivityGraph(NULL, childQueryIndexes.item(match), childQueries.item(match), parentActivity, _probeManager, _logctx);
        graph->onCreate(ctx, colocalArg);
        return graph.getClear();
    }

    virtual IRoxieServerSideCache *queryServerSideCache() const
    {
        return NULL; // Activities that wish to support server-side caching will need to do better....
    }

    virtual bool getEnableFieldTranslation() const
    {
        throwUnexpected(); // only implemented by index-related subclasses
    }

    virtual IDefRecordMeta *queryActivityMeta() const
    {
        throwUnexpected(); // only implemented by index-related subclasses
    }

    virtual void noteStatistic(unsigned statCode, unsigned __int64 value, unsigned count) const
    {
        mystats.noteStatistic(statCode, value, count);
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        // Most activities have nothing to say...
    }
};

class CRoxieServerMultiInputInfo
{
private:
    UnsignedArray inputs;
    UnsignedArray inputIndexes;

public:
    void set(unsigned idx, unsigned source, unsigned sourceidx)
    {
        if (idx==inputs.length())
        {
            inputs.append(source);
            inputIndexes.append(sourceidx);
        }
        else
        {
            while (!inputs.isItem(idx))
            {
                inputs.append(0);
                inputIndexes.append(0);
            }
            inputs.replace(source, idx);
            inputIndexes.replace(sourceidx, idx);
        }
    }

    unsigned get(unsigned idx, unsigned &sourceidx) const
    {
        if (inputs.isItem(idx))
        {
            sourceidx = inputIndexes.item(idx);
            return inputs.item(idx);
        }
        else
            return (unsigned) -1;
    }

    inline unsigned ordinality() const { return inputs.ordinality(); }
};


class CRoxieServerMultiInputFactory : public CRoxieServerActivityFactoryBase
{
private:
    CRoxieServerMultiInputInfo inputs;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerMultiInputFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactoryBase(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        inputs.set(idx, source, sourceidx);
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        return inputs.get(idx, sourceidx);
    }

    virtual unsigned numInputs() const { return inputs.ordinality(); }
};

class CWrappedException : public CInterface, implements IException
{
    Owned<IException> wrapped;
    ThorActivityKind kind;
    unsigned queryId;

public: 
    IMPLEMENT_IINTERFACE;
    CWrappedException(IException *_wrapped, ThorActivityKind _kind, unsigned _queryId) 
        : wrapped(_wrapped), kind(_kind), queryId(_queryId)
    {
    }
    virtual int             errorCode() const { return wrapped->errorCode(); }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return wrapped->errorMessage(msg).appendf(" (in %s %d)", getActivityText(kind), queryId); }
    virtual MessageAudience errorAudience() const { return wrapped->errorAudience(); }
};

class CRoxieServerActivityFactory : public CRoxieServerActivityFactoryBase
{
protected:
    unsigned input;
    unsigned inputidx;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactoryBase(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        input = (unsigned) -1;
        inputidx = 0;
    }

    inline void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        if (idx != 0)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: id = %d : setInput() parameter out of bounds idx = %d at %s(%d)", id, idx, __FILE__, __LINE__); 
        if (input != -1)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: id = %d : setInput() called twice for input = %d source = %d  inputidx = %d  sourceidx = %d at %s(%d)", id, input, source, inputidx, sourceidx, __FILE__, __LINE__); 
        input = source;
        inputidx = sourceidx;
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        if (!idx)
        {
            sourceidx = inputidx;
            return input;
        }
        return (unsigned) -1;
    }

    virtual unsigned numInputs() const { return (input == (unsigned)-1) ? 0 : 1; }
};

class CRoxieServerMultiOutputFactory : public CRoxieServerActivityFactory
{
protected:
    unsigned numOutputs;
    unsigned __int64 *processedArray;
    bool *startedArray;

    CRoxieServerMultiOutputFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        numOutputs = 0;
        processedArray = NULL;
        startedArray = NULL;
    }

    ~CRoxieServerMultiOutputFactory()
    {
        delete [] processedArray;
        delete [] startedArray;
    }

    void setNumOutputs(unsigned num)
    {
        numOutputs = num;
        if (!num)
            num = 1; // Even sink activities like to track how many records they process
        processedArray = new unsigned __int64[num];
        startedArray = new bool[num];
        for (unsigned i = 0; i < num; i++)
        {
            processedArray[i] = 0;
            startedArray[i] = 0;
        }
    }

    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const
    {
        assertex(numOutputs ? idx < numOutputs : idx==0);
        CriticalBlock b(statsCrit);
        putStatsValue(&edge, "count", "sum", processedArray[idx]);
        putStatsValue(&edge, "started", "sum", startedArray[idx]);
    }

    virtual void noteProcessed(unsigned idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const
    {
        assertex(numOutputs ? idx < numOutputs : idx==0);
        CriticalBlock b(statsCrit);
        processedArray[idx] += _processed;
        totalCycles += _totalCycles;
        localCycles += _localCycles;
    }

    virtual void noteStarted(unsigned idx) const
    {
        assertex(numOutputs ? idx < numOutputs : idx==0);
        CriticalBlock b(statsCrit);
        startedArray[idx] = true;
    }
};

class CRoxieServerInternalSinkFactory : public CRoxieServerActivityFactory
{
protected:
    bool isInternal;
    bool isRoot;
    unsigned usageCount;
public:

    CRoxieServerInternalSinkFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        usageCount = _usageCount;
        isRoot = _isRoot;
        isInternal = false; // filled in by derived class constructor
    }

    virtual bool isSink() const
    {
        //only a sink if a root activity
        return isRoot && !(isInternal && dependentCount && dependentCount==usageCount); // MORE - it's possible for this to get the answer wrong still, since usageCount does not include references from main procedure. Gavin?
    }

    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const
    {
        // There is no meaningful info to return along the dependency edge - we don't detect how many times the value has been read from the context
        // Just leave it blank is safest.
    }

};

typedef enum { STATEreset, STATEstarted, STATEstopped, STATEstarting } activityState;

const char *queryStateText(activityState state)
{
    switch (state)
    {
    case STATEreset: return "reset";
    case STATEstarted: return "started";
    case STATEstopped: return "stopped";
    case STATEstarting: return "starting";
    default: return "unknown";
    }
}

typedef ICopyArrayOf<IRoxieServerActivity> IRoxieServerActivityCopyArray;

class CParallelActivityExecutor : public CAsyncFor
{
public:
    unsigned parentExtractSize;
    const byte * parentExtract;

    CParallelActivityExecutor(IRoxieServerActivityCopyArray & _activities, unsigned _parentExtractSize, const byte * _parentExtract) : 
        activities(_activities), parentExtractSize(_parentExtractSize), parentExtract(_parentExtract) { }
    void Do(unsigned i)
    {
        activities.item(i).execute(parentExtractSize, parentExtract);
    }
private:
    IRoxieServerActivityCopyArray & activities;
};

class CRoxieServerActivity : public CInterface, implements IRoxieServerActivity, implements IRoxieInput, implements IRoxieContextLogger
{
protected:
    IRoxieInput *input;
    IHThorArg &basehelper;
    IRoxieSlaveContext *ctx;
    const IRoxieServerActivityFactory *factory;
    IRoxieServerActivityCopyArray dependencies;
    IntArray dependencyControlIds;
    IArrayOf<IActivityGraph> childGraphs;
    CachedOutputMetaData meta;
    IHThorArg *colocalParent;
    IEngineRowAllocator *rowAllocator;
    CriticalSection statecrit;

    mutable StatsCollector stats;
    unsigned processed;
    unsigned __int64 totalCycles;
    unsigned activityId;
    activityState state;
    bool createPending;
    bool debugging;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager) 
        : factory(_factory), 
          basehelper(_factory->getHelper()),
          activityId(_factory->queryId())
    {
        input = NULL;
        ctx = NULL;
        meta.set(basehelper.queryOutputMeta());
        processed = 0;
        totalCycles = 0;
        if (factory)
            factory->createChildQueries(childGraphs, this, _probeManager, *this);
        state=STATEreset;
        rowAllocator = NULL;
        debugging = _probeManager != NULL; // Don't want to collect timing stats from debug sessions
        colocalParent = NULL;
        createPending = true;
    }
    
    CRoxieServerActivity(IHThorArg & _helper) : factory(NULL), basehelper(_helper)
    {
        activityId = 0;
        input = NULL;
        ctx = NULL;
        meta.set(basehelper.queryOutputMeta());
        processed = 0;
        totalCycles = 0;
        state=STATEreset;
        rowAllocator = NULL;
        debugging = false;
        colocalParent = NULL;
        createPending = true;
    }

    inline ~CRoxieServerActivity()
    {
        CriticalBlock cb(statecrit);
        if (traceStartStop)
            DBGLOG("%p destroy state=%s", this, queryStateText(state)); // Note- CTXLOG may not be safe
        if (state!=STATEreset)
        {
            DBGLOG("STATE: Activity %d destroyed but not reset", activityId);
            state = STATEreset;  // bit pointless but there you go... 
        }
        basehelper.Release();
        ::Release(rowAllocator);
    }

    virtual const IRoxieContextLogger &queryLogCtx()const
    {
        return *this;
    }

    inline void createRowAllocator()
    {
        if (!rowAllocator) 
            rowAllocator = ctx->queryCodeContext()->getRowAllocator(meta.queryOriginal(), activityId);
    }

    // MORE - most of this is copied from ccd.hpp - can't we refactor?
    virtual void CTXLOG(const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        CTXLOGva(format, args);
        va_end(args);
    }

    virtual void CTXLOGva(const char *format, va_list args) const
    {
        StringBuffer text, prefix;
        getLogPrefix(prefix);
        text.valist_appendf(format, args);
        CTXLOGa(LOG_TRACING, prefix.str(), text.str());
    }

    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const
    {
        if (ctx)
            ctx->CTXLOGa(category, prefix, text);
        else
            DBGLOG("[%s] %s", prefix, text);
    }

    virtual void logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        StringBuffer prefix;
        getLogPrefix(prefix);
        CTXLOGaeva(E, file, line, prefix.str(), format, args);
        va_end(args);
    }

    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const
    {
        StringBuffer prefix;
        getLogPrefix(prefix);
        CTXLOGaeva(E, file, line, prefix.str(), format, args);
    }

    virtual void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const
    {
        va_list args;
        va_start(args, format);
        CTXLOGaeva(E, file, line, prefix, format, args);
        va_end(args);
    }

    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const
    {
        if (ctx)
            ctx->CTXLOGaeva(E, file, line, prefix, format, args);
        else
        {
            StringBuffer ss;
            ss.appendf("[%s] ERROR", prefix);
            if (E)
                ss.append(": ").append(E->errorCode());
            if (file)
                ss.appendf(": %s(%d) ", file, line);
            if (E)
                E->errorMessage(ss.append(": "));
            if (format)
            {
                ss.append(": ").valist_appendf(format, args);
            }
            LOG(MCoperatorProgress, unknownJob, "%s", ss.str());
        }
    }

    virtual void CTXLOGl(LogItem *log) const
    {
        if (ctx)
            ctx->CTXLOGl(log);
        else
        {
            assert(ctx);
            log->Release(); // Should never happen
        }
    }

    virtual void noteStatistic(unsigned statCode, unsigned __int64 value, unsigned count) const
    {
        if (factory)
            factory->noteStatistic(statCode, value, count);
        if (ctx)
            ctx->noteStatistic(statCode, value, count);
        stats.noteStatistic(statCode, value, count);
    }

    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
    {
        if (ctx)
            ctx->getLogPrefix(ret);
        return ret.append('@').append(activityId);
    }

    virtual bool isIntercepted() const
    {
        return ctx ? ctx->isIntercepted() : false;
    }

    virtual bool isBlind() const
    {
        return ctx ? ctx->isBlind() : blindLogging;
    }

    virtual unsigned queryTraceLevel() const 
    {
        if (ctx)
            return ctx->queryTraceLevel();
        else
            return traceLevel;
    }

    virtual bool isPassThrough()
    {
        return false;
    }

    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt)
    {
        return ctx->resolveLFN(filename, isOpt);
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        throwUnexpected(); // should be implemented in more derived class by anyone that has a remote adaptor
        return NULL;
    }

    virtual void serializeSkipInfo(MemoryBuffer &out, unsigned seekLen, const void *rawSeek, unsigned numFields, const void * seek, const SmartStepExtra &stepExtra) const
    {
        throwUnexpected(); // should be implemented in more derived class wherever needed
    }

    virtual IRoxieSlaveContext *queryContext()
    {
        return ctx;
    }

    virtual IRoxieServerActivity *queryActivity() { return this; }
    virtual IIndexReadActivityInfo *queryIndexReadActivity() { return NULL; }

    virtual bool needsAllocator() const { return false; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        ctx = _ctx;
        colocalParent = _colocalParent;
        createPending = true;
        if (needsAllocator())
            createRowAllocator();
        processed = 0;
        totalCycles = 0;
        if (factory)
            factory->onCreateChildQueries(_ctx, &basehelper, childGraphs);
    }

    virtual void serializeCreateStartContext(MemoryBuffer &out)
    {
        //This should only be called after onStart has been called on the helper
        assertex(!createPending);
        assertex(state==STATEstarted);
        unsigned startlen = out.length();
        basehelper.serializeCreateContext(out);
        basehelper.serializeStartContext(out);
        if (queryTraceLevel() > 10)
            CTXLOG("serializeCreateStartContext for %d added %d bytes", activityId, out.length()-startlen);
    }

    virtual void serializeExtra(MemoryBuffer &out) {}

    inline void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CriticalBlock cb(statecrit);
        if (state != STATEreset && state != STATEstarting)
        {
            CTXLOG("STATE: Expected state to be reset, but was %s, in activity %d", queryStateText(state), activityId);
        }
        state=STATEstarted;
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
        {
            CTXLOG("start %d", activityId);
            if (watchActivityId && watchActivityId==activityId)
            {
                CTXLOG("WATCH: start %d", activityId);
            }
        }
#endif
        executeDependencies(parentExtractSize, parentExtract, 0);
        if (input)
            input->start(parentExtractSize, parentExtract, paused);
        ensureCreated();
        basehelper.onStart(parentExtract, NULL);
        if (factory)
            factory->noteStarted();
    }

    void executeDependencies(unsigned parentExtractSize, const byte *parentExtract, unsigned controlId)
    {
        //MORE: Create a filtered list and then use asyncfor
        ForEachItemIn(idx, dependencies)
        {
            if (dependencyControlIds.item(idx) == controlId)
                dependencies.item(idx).execute(parentExtractSize, parentExtract);
        }
    }

    virtual unsigned __int64 queryTotalCycles() const
    {
        return totalCycles;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 ret = totalCycles;
        if (input) ret -= input->queryTotalCycles();
        if (ret < 0) 
            ret = 0;
        return ret;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (idx==0) 
            return input;
        else
            return NULL;
    }

    void noteProcessed(unsigned _idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const
    {
        if (factory)
        {
            if (!debugging)
                factory->noteProcessed(_idx, _processed, _totalCycles, _localCycles);
            if (ctx)
                ctx->noteProcessed(*this, this, _idx, _processed, _totalCycles, _localCycles);
        }

    }

    inline void ensureCreated()
    {
        if (createPending)
        {
            createPending = false;
            basehelper.onCreate(ctx->queryCodeContext(), colocalParent, NULL);
        }
    }

    inline void stop(bool aborting)
    {
        if (state != STATEstopped)
        {
            CriticalBlock cb(statecrit);
            if (state != STATEstopped)
            {
                state=STATEstopped;
#ifdef TRACE_STARTSTOP
                if (traceStartStop)
                {
                    CTXLOG("stop %d", activityId);
                    if (watchActivityId && watchActivityId==activityId)
                    {
                        CTXLOG("WATCH: stop %d", activityId);
                    }
                }
#endif
                if (input)
                    input->stop(aborting);
            }
        }
    }

    inline void reset()
    {
        if (state != STATEreset)
        {
            CriticalBlock cb(statecrit);
            if (state != STATEreset)
            {
                if (state==STATEstarted || state==STATEstarting)
                {
                    CTXLOG("STATE: activity %d reset without stop", activityId);
                    stop(false);
                }
                if (ctx->queryTimeActivities())
                {
                    stats.dumpStats(*this);
                    StringBuffer prefix, text;
                    getLogPrefix(prefix);
                    text.appendf("records processed - %d", processed);
                    CTXLOGa(LOG_STATISTICS, prefix.str(), text.str());
                    text.clear().appendf("total time - %d us", (unsigned) (cycle_to_nanosec(totalCycles)/1000));
                    CTXLOGa(LOG_STATISTICS, prefix.str(), text.str());
                    text.clear().appendf("local time - %d us", (unsigned) (cycle_to_nanosec(queryLocalCycles())/1000));
                    CTXLOGa(LOG_STATISTICS, prefix.str(), text.str());
                }
                state = STATEreset;
#ifdef TRACE_STARTSTOP
                if (traceStartStop)
                {
                    CTXLOG("reset %d", activityId);
                    if (watchActivityId && watchActivityId==activityId)
                    {
                        CTXLOG("WATCH: reset %d", activityId);
                    }
                }
#endif
                ForEachItemIn(idx, dependencies)
                    dependencies.item(idx).reset();
                noteProcessed(0, processed, totalCycles, queryLocalCycles());
                if (input)
                    input->reset();
                processed = 0;
                totalCycles = 0;
            }
        }
    }

    virtual void addDependency(IRoxieServerActivity &source, unsigned sourceIdx, int controlId) 
    {
        dependencies.append(source);
        dependencyControlIds.append(controlId);
    } 

    virtual void resetEOF()
    {
        //would make more sense if the default implementation (and eof member) were in the base class
    }

    // Sink activities should override this....
    virtual void execute(unsigned parentExtractSize, const byte * parentExtract) 
    {
        throw MakeStringException(ROXIE_SINK, "Internal error: execute() requires a sink");
    }

    virtual void executeChild(size32_t & retSize, void * & ret, unsigned parentExtractSize, const byte * parentExtract)
    {
        throw MakeStringException(ROXIE_SINK, "Internal error: executeChild() requires a suitable sink");
    }

    virtual __int64 evaluate() 
    {
        throw MakeStringException(ROXIE_SINK, "Internal error: evaluate() requires a function");
    }

    virtual IRoxieInput * querySelectOutput(unsigned id)
    {
        return NULL;
    }

    virtual bool querySetStreamInput(unsigned id, IRoxieInput * _input)
    {
        return false;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        assertex(!idx);
        input = _in;
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx == (unsigned) -1)
            idx = 0;
        return idx ? NULL : this;
    }

    virtual IOutputMetaData *queryOutputMeta() const
    { 
        return meta.queryOriginal(); 
    } 

    virtual unsigned queryId() const
    {
        return activityId;
    }

    virtual unsigned querySubgraphId() const
    {
        return factory->querySubgraphId();
    }

    virtual void checkAbort()
    {
        ctx->checkAbort();
    }

    IException *makeWrappedException(IException *e)
    {
        StringBuffer msg;
        ThorActivityKind activityKind = factory ? factory->getKind() : TAKnone;
        CTXLOG("makeWrappedException - %s (in %s %d)", e->errorMessage(msg).str(), getActivityText(activityKind), activityId); 
        if (QUERYINTERFACE(e, CWrappedException) ||  QUERYINTERFACE(e, IUserException))
            return e;
        else
            return new CWrappedException(e, activityKind, activityId);
    }

    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract)
    {
    }

    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes)
    {
    }

    virtual void resetOutputsUsed()
    {
    }

    virtual void noteOutputUsed()
    {
    }

    virtual IRoxieServerSideCache *queryServerSideCache() const
    {
        return factory->queryServerSideCache();
    }

    virtual const IRoxieServerActivityFactory *queryFactory() const
    {
        return factory;
    }
    inline ThorActivityKind getKind() const
    {
        return factory->getKind();
    }   

    inline bool isSink() const
    {
        return (factory != NULL) && factory->isSink();
    }
};

//=====================================================================================================

class CRoxieServerLateStartActivity : public CRoxieServerActivity
{

protected:
    IRoxieInput *input; // Don't use base class input field as we want to delay starts
    bool prefiltered;
    bool eof;

    void lateStart(unsigned parentExtractSize, const byte *parentExtract, bool any)
    {
        prefiltered = !any;
        eof = prefiltered;
        if (!prefiltered)
            input->start(parentExtractSize, parentExtract, false);
        else
        {
            if (traceStartStop)
                CTXLOG("lateStart activity stopping input early as prefiltered");
            input->stop(false);
        }
    }

public:

    CRoxieServerLateStartActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        input = NULL;
        prefiltered = false;
        eof = false;
    }

    virtual void stop(bool aborting)
    {
        if (!prefiltered)
        {
            input->stop(aborting);
        }
        else if (traceStartStop)
            CTXLOG("lateStart activity NOT stopping input late as prefiltered");
        CRoxieServerActivity::stop(aborting);
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 localCycles = totalCycles - input->queryTotalCycles();
        if (localCycles < 0)
            localCycles = 0;
        return localCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (idx==0) 
            return input;
        else
            return NULL;
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        input->reset();
        prefiltered = false;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        assertex(!idx);
        input = _in;
    }

};

//=====================================================================================================

atomic_t nextInstanceId;

extern unsigned getNextInstanceId()
{
    return atomic_add_exchange(&nextInstanceId, 1)+1;
}

atomic_t nextRuid;

ruid_t getNextRuid()
{
    ruid_t ret = atomic_add_exchange(&nextRuid, 1)+1;
    while (ret < RUID_FIRST)
        ret = atomic_add_exchange(&nextRuid, 1)+1; // ruids 0 and 1 are reserved for pings/unwanted discarder.
    return ret;
}

void setStartRuid(unsigned restarts)
{
    atomic_set(&nextRuid, restarts * 0x10000);
    atomic_set(&nextInstanceId, restarts * 10000);
}

enum { LimitSkipErrorCode = 0, KeyedLimitSkipErrorCode = 1 };

class LimitSkipException : public CInterface, public IException
{
    int code;
public:
    LimitSkipException(int _code) { code = _code; }
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return code; }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return msg.append("LimitSkipException"); }
    virtual MessageAudience errorAudience() const { return MSGAUD_internal; }
};

IException *makeLimitSkipException(bool isKeyed)
{
    // We need to make sure what we throw is IException not something derived from it....
    return new LimitSkipException(isKeyed ? KeyedLimitSkipErrorCode : LimitSkipErrorCode);
}

//=================================================================================

interface IRecordPullerCallback : extends IExceptionHandler
{
    virtual void processRow(const void *row) = 0;
    virtual void processEOG() = 0;
    virtual void processGroup(const ConstPointerArray &rows) = 0;
    virtual void processDone() = 0;
};

class RecordPullerThread : public RestartableThread
{
protected:
    IRoxieInput *input;
    IRecordPullerCallback *helper;
    Semaphore started;                      // MORE: GH->RKC I'm pretty sure this can be deleted, since handled by RestartableThread
    bool groupAtOnce, eof, eog;
    CriticalSection crit;

public:
    RecordPullerThread(bool _groupAtOnce) 
        : RestartableThread("RecordPullerThread"), groupAtOnce(_groupAtOnce)
    {
        input = NULL;
        helper = NULL;
        eof = eog = FALSE;
    }

    inline unsigned __int64 queryTotalCycles() const
    {
        return input->queryTotalCycles();
    }

    void setInput(IRecordPullerCallback *_helper, IRoxieInput *_input)
    {
        helper = _helper;
        input = _input;
    }

    IRoxieInput *queryInput() const
    {
        return input;
    }

    void start(unsigned parentExtractSize, const byte *parentExtract, bool paused, unsigned preload, bool noThread, IRoxieSlaveContext *ctx)
    {
        eof = false;
        eog = false;
        input->start(parentExtractSize, parentExtract, paused);
        try
        {
            if (preload && !paused)
            {
                if (traceLevel > 4)
                    DBGLOG("Preload fetching first %d records", preload);
                if (groupAtOnce)
                    pullGroups(preload);
                else
                    pullRecords(preload);
            }
            if (eof)
            {
                if (traceLevel > 4)
                    DBGLOG("No need to start puller after preload");
                helper->processDone();
            }
            else
            {
                if (!noThread)
                {
                    StringBuffer logPrefix("[");
                    if (ctx) ctx->getLogPrefix(logPrefix);
                    logPrefix.append("] ");
                    RestartableThread::start(logPrefix);
                    started.wait();
                }
            }
        }
        catch (IException *e)
        {
            helper->fireException(e);
        }
        catch (...)
        {
            helper->fireException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception caught in RecordPullerThread::start"));
        }
    }

    void stop(bool aborting)
    {
        if (traceStartStop)
            DBGLOG("RecordPullerThread::stop");
        {
            CriticalBlock c(crit); // stop is called on our consumer's thread. We need to take care calling stop for our input to make sure it is not in mid-nextInGroup etc etc.
            input->stop(aborting);
        }
        RestartableThread::join();
    }

    void reset()
    {
        input->reset();
    }

    virtual int run()
    {
        started.signal();
        try
        {
            if (groupAtOnce)
                pullGroups((unsigned) -1);
            else
                pullRecords((unsigned) -1);
            helper->processDone();
        }
        catch (IException *e)
        {
            helper->fireException(e);
        }
        catch (...)
        {
            helper->fireException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception caught in RecordPullerThread::run"));
        }
        return 0;
    }

    void done()
    {
        helper->processDone();
    }

    bool pullRecords(unsigned preload)
    {
        if (eof)
            return false;
        while (preload)
        {
            const void * row;
            {
                CriticalBlock c(crit); // See comments in stop for why this is needed
                row = input->nextInGroup();
            }
            if (row)
            {
                eog = false;
                helper->processRow(row);
            }
            else if (!eog)
            {
                helper->processEOG();
                eog = true;
            }
            else
            {
                eof = true;
                return false;
            }
            if (preload != (unsigned) -1)
                preload--;
        }
        return true;
    }

    void pullGroups(unsigned preload)
    {
        ConstPointerArray thisGroup;
        unsigned rowsDone = 0;
        while (preload && !eof)
        {
            const void *row;
            {
                CriticalBlock c(crit);
                row = input->nextInGroup();
            }
            if (row)
            {
                thisGroup.append(row);
                rowsDone++;
            }
            else if (thisGroup.length())
            {
                helper->processGroup(thisGroup);
                thisGroup.kill();
                if (preload != (unsigned) -1)
                {
                    if (preload > rowsDone)
                        preload -= rowsDone;
                    else
                        break;
                }
                rowsDone = 0;
            }
            else
            {
                eof = true;
                break;
            }
        }
    }
};


//=================================================================================

#define READAHEAD_SIZE 1000

// MORE - this code copied from ThreadedConcat code - may be able to common up some.

class CRoxieServerReadAheadInput : public CInterface, implements IRoxieInput, implements IRecordPullerCallback
{
    QueueOf<const void, true> buffer;
    InterruptableSemaphore ready;
    InterruptableSemaphore space;
    CriticalSection crit;
    bool eof;
    bool disabled;
    RecordPullerThread puller;
    unsigned preload;
    unsigned __int64 totalCycles;
    IRoxieSlaveContext *ctx;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieServerReadAheadInput(unsigned _preload) : puller(true), preload(_preload)
    {
        eof = false;
        disabled = false;
        totalCycles = 0;
        ctx = NULL;
    }

    void onCreate(IRoxieSlaveContext *_ctx)
    {
        ctx = _ctx;
        disabled = (ctx->queryDebugContext() != NULL);
    }

    virtual IRoxieServerActivity *queryActivity()
    {
        return puller.queryInput()->queryActivity();
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity() 
    {
        return puller.queryInput()->queryIndexReadActivity();
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        totalCycles = 0;
        if (disabled)
            puller.queryInput()->start(parentExtractSize, parentExtract, paused);
        else
        {
            space.reinit(READAHEAD_SIZE);
            ready.reinit();
            puller.start(parentExtractSize, parentExtract, paused, preload, false, ctx);
        }
    }

    virtual void stop(bool aborting)
    {
        if (disabled)
            puller.queryInput()->stop(aborting);
        else
        {
            space.interrupt();
            ready.interrupt();
            puller.stop(aborting);
        }
    }

    virtual void reset()
    {
        if (disabled)
            puller.queryInput()->reset();
        else
        {
            puller.reset();
            ForEachItemIn(idx1, buffer)
                ReleaseRoxieRow(buffer.item(idx1));
            buffer.clear();
        }
    }

    virtual void resetEOF()
    {
        throwUnexpected();
    }

    virtual IOutputMetaData * queryOutputMeta() const 
    {
        return puller.queryInput()->queryOutputMeta(); 
    }

    virtual void checkAbort() 
    {
        puller.queryInput()->checkAbort();
    }

    void setInput(unsigned idx, IRoxieInput *_in)
    {
        assertex(!idx);
        puller.setInput(this, _in);
    }

    virtual unsigned __int64 queryTotalCycles() const
    {
        return totalCycles;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 ret = totalCycles - puller.queryInput()->queryTotalCycles();
        if (ret < 0) ret = 0;
        return ret;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return puller.queryInput()->queryInput(idx);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (disabled)
            return puller.queryInput()->nextInGroup();
        else
        {
            loop
            {
                {
                    CriticalBlock b(crit);
                    if (eof && !buffer.ordinality())
                        return NULL;  // eof
                }
                ready.wait();
                const void *ret;
                {
                    CriticalBlock b(crit);
                    ret = buffer.dequeue();
                }
                space.signal();
                return ret;
            }
        }
    }

    virtual unsigned queryId() const { throwUnexpected(); }

    virtual bool fireException(IException *e)
    {
        // called from puller thread on failure
        ready.interrupt(LINK(e));
        space.interrupt(e);
        return true;
    }

    virtual void processRow(const void *row)
    {
        {
            CriticalBlock b(crit);
            buffer.enqueue(row);
        }
        ready.signal();
        space.wait();
    }

    virtual void processGroup(const ConstPointerArray &rows)
    {
        // NOTE - a bit bizarre in that it waits for the space AFTER using it.
        // But the space semaphore is only there to stop infinite readahead. And otherwise it would deadlock
        // if group was larger than max(space)
        {
            CriticalBlock b(crit);
            ForEachItemIn(idx, rows)
                buffer.enqueue(rows.item(idx));
            buffer.enqueue(NULL);
        }
        for (unsigned i2 = 0; i2 <= rows.length(); i2++) // note - does 1 extra for the null
        {
            ready.signal();
            space.wait();
        }
    }

    virtual void processEOG()
    {
        // Used when output is not grouped - just ignore
    }

    virtual void processDone()
    {
        CriticalBlock b(crit);
        eof = true;
        ready.signal();
    }
};

//=================================================================================

class CRoxieServerTwoInputActivity : public CRoxieServerActivity
{
protected:
    IRoxieInput *input1;
    Owned<CRoxieServerReadAheadInput> puller;

public:
    CRoxieServerTwoInputActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        input1 = NULL;
    }

    ~CRoxieServerTwoInputActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        input1->start(parentExtractSize, parentExtract, paused);
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        if (puller)
            puller->onCreate(_ctx);
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
    }

    virtual void stop(bool aborting)    
    {
        input1->stop(aborting);
        CRoxieServerActivity::stop(aborting); 
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 ret;
        __int64 inputCycles = input->queryTotalCycles();
        __int64 input1Cycles = input1->queryTotalCycles();
        if (puller)
            ret = totalCycles - (inputCycles > input1Cycles ? inputCycles : input1Cycles);
        else
            ret = totalCycles - (inputCycles + input1Cycles);
        if (ret < 0) 
            ret = 0;
        return ret;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        switch (idx)
        {
        case 0:
            return input;
        case 1:
            return input1;
        default:
            return NULL;
        }
    }

    virtual void reset()    
    {
        CRoxieServerActivity::reset(); 
        if (input1)
            input1->reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        switch(idx)
        {
        case 0:
            input = _in;
            break;
        case 1:
            input1 = _in;
            break;
        default:
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        }   
    }

};

//=================================================================================

class CRoxieServerMultiInputBaseActivity : public CRoxieServerActivity
{
protected:
    unsigned numInputs;
    IRoxieInput **inputArray;

public:
    CRoxieServerMultiInputBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager), numInputs(_numInputs)
    {
        inputArray = new IRoxieInput*[numInputs];
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i] = NULL;
    }

    ~CRoxieServerMultiInputBaseActivity()
    {
        delete [] inputArray;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 localCycles = totalCycles;
        for (unsigned i = 0; i < numInputs; i++)
            localCycles -= inputArray[i]->queryTotalCycles();
        if (localCycles < 0)
            localCycles = 0;
        return localCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (idx < numInputs)
            return inputArray[idx];
        else
            return NULL;
    }

    virtual void reset()    
    {
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i]->reset();
        CRoxieServerActivity::reset(); 
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        inputArray[idx] = _in;
    }

};

//=================================================================================

class CRoxieServerMultiInputActivity : public CRoxieServerMultiInputBaseActivity
{
public:
    CRoxieServerMultiInputActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputBaseActivity(_factory, _probeManager, _numInputs)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerMultiInputBaseActivity::start(parentExtractSize, parentExtract, paused);
        for (unsigned i = 0; i < numInputs; i++)
        {
            inputArray[i]->start(parentExtractSize, parentExtract, paused);
        }
    }

    virtual void stop(bool aborting)    
    {
        for (unsigned i = 0; i < numInputs; i++)
        {
            inputArray[i]->stop(aborting);
        }
        CRoxieServerMultiInputBaseActivity::stop(aborting); 
    }

};

//=====================================================================================================

class CRoxieServerInternalSinkActivity : public CRoxieServerActivity
{
protected:
    bool executed;
    CriticalSection ecrit;
    Owned<IException> exception;

public:
    CRoxieServerInternalSinkActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        executed = false;
    }

    virtual void reset()
    {
        executed = false;
        exception.clear();
        CRoxieServerActivity::reset();
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        return NULL;
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }

    virtual void onExecute() = 0;

    virtual void execute(unsigned parentExtractSize, const byte * parentExtract) 
    {
        CriticalBlock b(ecrit);
        if (exception)
            throw exception.getLink();
        if (!executed)
        {
            try
            {
                start(parentExtractSize, parentExtract, false);
                {
                    ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext()); // unfortunately this is not really best place for seeing in debugger.
                    onExecute();
                }
                stop(false);
                executed = true;
            }
            catch (IException *E)
            {
                exception.set(E); // (or maybe makeWrappedException?)
                stop(true);
                throw;
            }
            catch (...)
            {
                exception.set(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught at %s:%d", __FILE__, __LINE__));
                stop(true);
                throw;
            }
        }
    }

};


//=================================================================================

class CRoxieServerQueryPacket : public CInterface, implements IRoxieServerQueryPacket
{
protected:
    Owned<IMessageResult> result;
    Owned<IRoxieQueryPacket> packet;
    Linked<IRoxieServerQueryPacket> continuation;
    unsigned hash;
    unsigned seq;
    unsigned lastDebugSequence;
    Owned<IRoxieQueryPacket> lastDebugResponse;

    ILRUChain *prev;
    ILRUChain *next;
    bool delayed;
public:
    IMPLEMENT_IINTERFACE;
    CRoxieServerQueryPacket(IRoxieQueryPacket *p) : packet(p)
    {
        hash = 0;
        seq = 0;
        prev = NULL;
        next = NULL;
        delayed = false;
        lastDebugSequence = 0;
    }
    virtual IRoxieQueryPacket *queryPacket() const
    {
        return packet;
    }
    virtual bool isContinuation() const
    {
        return packet && (packet->queryHeader().continueSequence & ~CONTINUE_SEQUENCE_SKIPTO) != 0;
    }
    virtual bool isDelayed() const
    {
        return delayed;
    }
    virtual bool isEnd() const
    {
        return false;
    }
    virtual bool isLimit(unsigned __int64 &_rowLimit, unsigned __int64 &_keyedLimit, unsigned __int64 &_stopAfter) const
    {
        return false;
    }
    virtual bool hasResult() const 
    {
        return result != NULL;
    }
    virtual bool hasContinuation() const 
    {
        return continuation != NULL;
    }
    virtual void setDelayed(bool _delayed)
    {
        delayed = _delayed;
    }
    virtual void setPacket(IRoxieQueryPacket *_packet)
    {
        packet.setown(_packet);
    }
    virtual void setSequence(unsigned _seq)
    {
        assertex(!IsShared());
        seq = _seq;
    }
    virtual unsigned getSequence() const
    {
        return seq;
    }
    IMessageResult *getResult()
    {
        return result.getLink();
    }
    IMessageResult *queryResult()
    {
        return result;
    }
    void setResult(IMessageResult *r)
    {
        result.setown(r);
    }
    IRoxieServerQueryPacket *queryContinuation()
    {
        return continuation;
    }
    void setContinuation(IRoxieServerQueryPacket *c)
    {
        continuation.setown(c);
    }
    virtual unsigned queryHash() const
    {
        return hash;
    }
    virtual void setHash(unsigned _hash)
    {
        hash = _hash;
    }
    virtual ILRUChain *queryPrev() const { return prev; }
    virtual ILRUChain *queryNext() const { return next; }
    virtual void setPrev(ILRUChain *p) { prev = p; }
    virtual void setNext(ILRUChain *n) { next = n; }
    virtual void unchain()
    {
        if (prev && next)
        {
            prev->setNext(next);
            next->setPrev(prev);
        }
        next = NULL;
        prev = NULL;
    }

    virtual IRoxieQueryPacket *getDebugResponse(unsigned sequence)
    {
        if (sequence == lastDebugSequence)
            return lastDebugResponse.getLink();
        else if (sequence > lastDebugSequence)
        {
            lastDebugResponse.clear();
            return NULL;
        }
        else
            throwUnexpected();
    }

    virtual void setDebugResponse(unsigned sequence, IRoxieQueryPacket *response)
    {
        lastDebugSequence = sequence;
        lastDebugResponse.set(response);
    }

};

class CRoxieServerQueryPacketEndMarker : public CRoxieServerQueryPacket
{
public:
    CRoxieServerQueryPacketEndMarker() : CRoxieServerQueryPacket(NULL)
    {
    }
    virtual bool isEnd() const
    {
        return true;
    }
};

class CRoxieServerQueryPacketLimitMarker : public CRoxieServerQueryPacket
{
    unsigned __int64 rowLimit;
    unsigned __int64 keyedLimit;
    unsigned __int64 stopAfter;

public:
    CRoxieServerQueryPacketLimitMarker(unsigned __int64 _rowLimit, unsigned __int64 _keyedLimit, unsigned __int64 _stopAfter) : CRoxieServerQueryPacket(NULL)
    {
        rowLimit = _rowLimit;
        keyedLimit = _keyedLimit;
        stopAfter = _stopAfter;
    }
    virtual bool isLimit(unsigned __int64 &_rowLimit, unsigned __int64 &_keyedLimit, unsigned __int64 &_stopAfter) const
    {
        _rowLimit = rowLimit;
        _keyedLimit = keyedLimit;
        _stopAfter = stopAfter;
        return true;
    }
};

class CRoxieServerSideCache : implements IRoxieServerSideCache, implements ILRUChain
{
protected:
    unsigned cacheTableSize;
    unsigned cacheTableSpace;
    IRoxieServerQueryPacket **cacheTable;
    mutable ILRUChain *prev;
    mutable ILRUChain *next;
    mutable CriticalSection crit;

    virtual ILRUChain *queryPrev() const { return prev; }
    virtual ILRUChain *queryNext() const { return next; }
    virtual void setPrev(ILRUChain *p) { prev = p; }
    virtual void setNext(ILRUChain *n) { next = n; }
    virtual void unchain()
    {
        prev->setNext(next);
        next->setPrev(prev);
        next = NULL;
        prev = NULL;
    }

    void moveToHead(IRoxieServerQueryPacket *mru)
    {
        mru->unchain();

        mru->setNext(next);
        next->setPrev(mru);
        mru->setPrev(this);
        next = mru;
    }

    IRoxieServerQueryPacket *removeLRU()
    {
        if (next==this)
            assertex(next != this);
        IRoxieServerQueryPacket *goer = (IRoxieServerQueryPacket *) next;
        goer->unchain(); // NOTE - this will modify the value of next
        return goer;
    }

    void removeEntry(IRoxieServerQueryPacket *goer)
    {
        unsigned v = goer->queryHash() % cacheTableSize;
        loop
        {
            IRoxieServerQueryPacket *found = cacheTable[v];
            assertex(found);
            if (found == goer)
            {
                cacheTable[v] = NULL;
                unsigned vn = v;
                loop
                {
                    vn++;
                    if (vn==cacheTableSize) vn = 0;
                    IRoxieServerQueryPacket *found2 = cacheTable[vn];
                    if (!found2)
                        break;
                    unsigned vm = found2->queryHash() % cacheTableSize;
                    if (((vn+cacheTableSize-vm) % cacheTableSize)>=((vn+cacheTableSize-v) % cacheTableSize))  // diff(vn,vm)>=diff(vn,v)
                    {
                        cacheTable[v] = found2;
                        v = vn;
                        cacheTable[v] = NULL;
                    }
                }
                cacheTableSpace++;
                break;
            }
            v++;
            if (v==cacheTableSize)
                v = 0;
        }
        goer->Release();
    }

public:
    CRoxieServerSideCache(unsigned _cacheSize)
    {
        cacheTableSize = (_cacheSize*4)/3;
        cacheTable = new IRoxieServerQueryPacket *[cacheTableSize];
        memset(cacheTable, 0, cacheTableSize * sizeof(IRoxieServerQueryPacket *));
        cacheTableSpace = _cacheSize;
        prev = this;
        next = this;
    }
    ~CRoxieServerSideCache()
    {
        for (unsigned i = 0; i < cacheTableSize; i++)
        {
            ::Release(cacheTable[i]);
        }
        delete [] cacheTable;
    }

    virtual IRoxieServerQueryPacket *findCachedResult(const IRoxieContextLogger &logctx, IRoxieQueryPacket *p) const
    {
        unsigned hash = p->hash();
        unsigned et = hash % cacheTableSize;
        if (traceServerSideCache)
        {
            StringBuffer s; 
            logctx.CTXLOG("CRoxieServerSideCache::findCachedResult hash %x slot %d %s", hash, et, p->queryHeader().toString(s).str());
        }
        CriticalBlock b(crit);
        loop
        {
            IRoxieServerQueryPacket *found = cacheTable[et];
            if (!found)
                return NULL;
            if (found->queryHash() == hash && found->queryPacket()->cacheMatch(p))
            {
                const_cast<CRoxieServerSideCache *>(this)->moveToHead(found);
                if (traceServerSideCache)
                    logctx.CTXLOG("CRoxieServerSideCache::findCachedResult cache hit");
                logctx.noteStatistic(STATS_SERVERCACHEHIT, 1, 1);
                return NULL;
                // Because IMessageResult cannot be replayed, this echeme is flawed. I'm leaving the code here just as a stats gatherer to see how useful it would have been....
                //IRoxieServerQueryPacket *ret = new CRoxieServerQueryPacket(p);
                //ret->setResult(found->getResult());
                //return ret;
            }
            et++;
            if (et == cacheTableSize)
                et = 0;
        }
    }

    virtual void noteCachedResult(IRoxieServerQueryPacket *out, IMessageResult *in)
    {
        if (true) //!in->getLength()) // MORE - separate caches for hits and nohits
        {
            unsigned hash = out->queryPacket()->hash();
            out->setHash(hash);
            unsigned et = hash % cacheTableSize;
            if (traceServerSideCache)
            {
                StringBuffer s; 
                DBGLOG("CRoxieServerSideCache::noteCachedResult hash %x slot %d %s", hash, et, out->queryPacket()->queryHeader().toString(s).str());
            }
            CriticalBlock b(crit);
            loop
            {
                IRoxieServerQueryPacket *found = cacheTable[et];
                if (!found)
                {
                    if (cacheTableSpace)
                    {
                        out->setResult(LINK(in)); 
                        cacheTable[et] = LINK(out);
                        cacheTableSpace--;
                        moveToHead(out);
                        break;
                    }
                    else
                    {
                        IRoxieServerQueryPacket *goer = removeLRU();
                        removeEntry(goer);
                        et = hash % cacheTableSize;
                        continue;
                    }
                }
                else if (found->queryHash()==hash && found->queryPacket()->cacheMatch(out->queryPacket()))
                {
                    moveToHead(found);
                    return; // already in the cache. Because we don't cache until we have result, this can happen where 
                    // multiple copies of a slave query are in-flight at once.
                }
                et++;
                if (et == cacheTableSize)
                    et = 0;
            }
        }
        // MORE - do we need to worry about the attachment between the MessageUnpacker and the current row manager. May all fall out ok...
        // Can I easily spot a null result? Do I want to cache null results separately? only?
    }

    // Note that this caching mechanism (unlike the old keyed-join specific one) does not common up cases where multiple 
    // identical queries are in-flight at the same time. But if we can make it persistant between queries that will 
    // more than make up for it
};

class CRowArrayMessageUnpackCursor : public CInterface, implements IMessageUnpackCursor
{
    ConstPointerArray &data;
    Linked<IMessageResult> result;

public:
    IMPLEMENT_IINTERFACE;
    CRowArrayMessageUnpackCursor(ConstPointerArray &_data, IMessageResult *_result)
        : data(_data), result(_result)
    {
    }

    virtual bool atEOF() const
    {
        return data.length()==0;
    }

    virtual bool isSerialized() const
    {
        return false;
    }

    virtual const void * getNext(int length)
    {
        if (!data.length())
            return NULL;
        const void *ret = data.item(0);
        data.remove(0);
        return ret;
    }

};

// MORE - should possibly move more over to the lazy version used in indexread?

class CRowArrayMessageResult : public CInterface, implements IMessageResult
{
    ConstPointerArray data;
    IRowManager &rowManager;
    bool variableSize;

public:
    IMPLEMENT_IINTERFACE;
    CRowArrayMessageResult(IRowManager &_rowManager, bool _variableSize) : rowManager(_rowManager), variableSize(_variableSize)
    {
    }

    ~CRowArrayMessageResult()
    {
        ReleaseRoxieRowSet(data);
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const
    {
        CRowArrayMessageResult  *_this = (CRowArrayMessageResult  *) this;
        return new CRowArrayMessageUnpackCursor(_this->data, _this);
    }

    virtual const void *getMessageHeader(unsigned &length) const
    {
        throwUnexpected(); // should never get called - I don't have a header available
        length = 0;
        return NULL;
    }

    virtual const void *getMessageMetadata(unsigned &length) const
    {
        length = 0;
        return NULL;
    }

    virtual void discard() const
    {
        throwUnexpected();
    }

    void append(const void *row)
    {
        data.append(row);
    }
};

void throwRemoteException(IMessageUnpackCursor *extra)
{
    RecordLengthType *rowlen = (RecordLengthType *) extra->getNext(sizeof(RecordLengthType));
    if (rowlen)
    {
        char *xml = (char *) extra->getNext(*rowlen);
        ReleaseRoxieRow(rowlen);
        Owned<IPropertyTree> p = createPTreeFromXMLString(xml);
        ReleaseRoxieRow(xml);
        unsigned code = p->getPropInt("Code", 0);
        const char *msg = p->queryProp("Message");
        if (!msg)
            msg = xml;
        throw MakeStringException(code, "%s", msg);
    }
    throwUnexpected();
}

class CRemoteResultAdaptor :public CInterface, implements IRoxieInput, implements IExceptionHandler
{
    friend class CRemoteResultMerger;
    class CRemoteResultMerger
    {
        class HeapEntry : public CInterface
        {
        private:
            CRemoteResultAdaptor &adaptor;
            IMessageUnpackCursor *cursor;
        public:
            const void *current;
            bool isLast;
            bool lastIsComplete;
            IRoxieServerQueryPacket *packet; 
            unsigned seq;

        public:
            inline const void *noteResult(IMessageUnpackCursor *_cursor, bool _lastIsComplete)
            {
                cursor = _cursor;
                lastIsComplete = _lastIsComplete;
                return next();
            }

        public:
            HeapEntry(CRemoteResultAdaptor &_adaptor, IRoxieServerQueryPacket *_packet, unsigned _seq) : adaptor(_adaptor), packet(_packet), seq(_seq)
            {
                cursor = NULL;
                current = NULL;
                isLast = false;
                lastIsComplete = true;
            }
            ~HeapEntry()
            {
                ::Release(packet);
                ::Release(cursor);
                ReleaseRoxieRow(current);
            }

            bool isCompleteMatch() const
            {
                if (!isLast || lastIsComplete)
                    return true;
                else
                    return false;
            }

            const void *next()
            {
                if (cursor)
                {
                    ReleaseClearRoxieRow(current);
                    current = adaptor.getRow(cursor);
                    isLast = cursor->atEOF();
                    if (!current)
                    {
                        cursor->Release();
                        cursor = NULL;
                    }
                }
                return current;
            }
            unsigned skipTo(IRangeCompare *compare, const void *seek, unsigned numFields, bool requireExactMatch)
            {
                // MORE - This loop should possibly be a binchop... though it's not absolutely clear that is true (depends on term frequencies)
                unsigned skipped = 0;
                loop
                {
                    int c = compare->docompare(current, seek, numFields);

                    //If larger than the seek values, then we may be allowed to return an inexact match, 
                    //if equal then it is required to be an exact match,
                    if (c > 0)
                    {
                        if (!requireExactMatch || isCompleteMatch())
                            break;
                    }
                    else if ((c == 0) && isCompleteMatch())
                        break;

                    skipped++;
                    if (!next())
                        break;
                }
                return skipped;
            }

        };

        CRemoteResultAdaptor &adaptor;
        CIArrayOf<HeapEntry> heapEntries;
        UnsignedArray heap;
        IRowManager *rowManager;
        unsigned numPending;
        unsigned numFields;
        bool endSeen;
        bool remakePending;
        IRangeCompare *compare;
        bool deferredContinuation;

        inline int doCompare(unsigned l, unsigned r)
        {
            int ret = compare->docompare(heapEntries.item(l).current, heapEntries.item(r).current, numFields);
            if (!ret) ret = heapEntries.item(l).seq - heapEntries.item(r).seq;
            return ret;
        }

        void makeHeap()
        {
            /* Permute blocks to establish the heap property
               For each element p, the children are p*2+1 and p*2+2 (provided these are in range)
               The children of p must both be greater than or equal to p
               The parent of a child c is given by p = (c-1)/2
            */
            unsigned i;
            unsigned n = heap.length();
            unsigned *s = heap.getArray();
            for (i=1; i<n; i++)
            {
                unsigned r = s[i];
                int c = i; /* child */
                while (c > 0) 
                {
                    int p = (c-1)/2; /* parent */
                    if ( doCompare( s[c], s[p] ) >= 0 ) 
                        break;
                    s[c] = s[p];
                    s[p] = r;
                    c = p;
                }
            }
            remakePending = false;
        }

        void remakeHeap()
        {
            /* The row associated with block[0] will have changed
               This code restores the heap property
            */
            unsigned p = 0; /* parent */
            unsigned n = heap.length();
            unsigned *s = heap.getArray();
            while (1) 
            {
                unsigned c = p*2 + 1; /* child */
                if ( c >= n ) 
                    break;
                /* Select smaller child */
                if ( c+1 < n && doCompare( s[c+1], s[c] ) < 0 ) c += 1;
                /* If child is greater or equal than parent then we are done */
                if ( doCompare( s[c], s[p] ) >= 0 ) 
                    break;
                /* Swap parent and child */
                unsigned r = s[c];
                s[c] = s[p];
                s[p] = r;
                /* child becomes parent */
                p = c;
            }
            remakePending = false;
        }

        void append(IRoxieServerQueryPacket *p, unsigned seq)
        {
            HeapEntry &h = *new HeapEntry(adaptor, LINK(p), seq);
            IMessageResult *result = p->queryResult();
            assertex(result);
            if (h.noteResult(result->getCursor(rowManager), isCompleteMatchFlag(result)))
            {
                heapEntries.append(h);
                heap.append(heap.ordinality());
            }
            else
                h.Release();
        }

        void removeHeap(unsigned idx)
        {
            heapEntries.remove(idx);
            ForEachItemIn(i, heap)
            {
                unsigned v = heap.item(i);
                assertex(v != idx);
                if (v > idx)
                    heap.replace(v-1, i);
            }
        }

        bool isCompleteMatchFlag(IMessageResult *result)
        {
            unsigned metaLen;
            const byte *metaInfo = (const byte *) result->getMessageMetadata(metaLen);
            if (metaLen)
            {
                unsigned short continuationLen = *(unsigned short *) metaInfo;
                if (continuationLen >= sizeof(bool))
                {
                    metaInfo += sizeof(unsigned short);
                    return *(bool *) metaInfo;
                }
            }
            return true; // if no continuation info, last row was complete.
        }

    public:
        CRemoteResultMerger(CRemoteResultAdaptor &_adaptor) : adaptor(_adaptor)
        {
            init(NULL, NULL);
        }

        void init(ISteppingMeta *meta, IRowManager *_rowManager)
        {
            if (meta)
            {
                numFields = meta->getNumFields();
                compare = meta->queryCompare();
            }
            else
            {
                numFields = 0;
                compare = NULL;
            }
            rowManager = _rowManager;
            numPending = 0;
            endSeen = false;
            remakePending = false;
            deferredContinuation = false;
        }

        void reset()
        {
            heapEntries.kill();
            heap.kill();
            numPending = 0;
            endSeen = false;
            remakePending = false;
            deferredContinuation = false;
        }

        inline bool noteEndSeen()
        {
            bool hadSeen = endSeen;
            if (!endSeen)
                makeHeap();
            endSeen = true;
            return !hadSeen;
        }

        void noteResult(IRoxieServerQueryPacket *p, unsigned seq)
        {
            if (!p->isContinuation())
                append(p, seq);
            else
            {
                ForEachItemIn(idx, heapEntries)
                {
                    HeapEntry &h = heapEntries.item(idx);
                    if (h.packet == p)
                    {
                        IMessageResult *result = p->queryResult();
                        if (!h.noteResult(result->getCursor(rowManager), isCompleteMatchFlag(result)))
                        {
                            heap.zap(idx);
                            removeHeap(idx);
                        }
                        numPending--;
                        if (!numPending)
                            makeHeap();
                        return;
                    }
                }
            }
            // If we get here it must be a continuation for one that I have not yet consumed... we don't need to do anything.
            return;
        }

        unsigned skipRows(unsigned &idx, const void *seek, const void *rawSeek, unsigned numFields, unsigned seekLen, const SmartStepExtra & stepExtra)
        {
            HeapEntry &entry = heapEntries.item(idx);
            unsigned skipped = entry.current ? entry.skipTo(compare, seek, numFields, !stepExtra.returnMismatches()) : 0;
            if (!entry.current)
            {
                IRoxieServerQueryPacket *continuation = entry.packet->queryContinuation();
                if (continuation)
                {
                    continuation->Link();
                    entry.packet->Release();
                    entry.packet = continuation;
                    if (continuation->hasResult())
                    {
                        IMessageResult *result = continuation->queryResult();
                        bool lastIsCompleteMatch = isCompleteMatchFlag(result);
                        entry.noteResult(result->getCursor(rowManager), lastIsCompleteMatch);
                    }
                    else
                    {
                        if (continuation->isDelayed())
                        {
                            continuation->setDelayed(false);
                            MemoryBuffer serializedSkip;
                            adaptor.activity.serializeSkipInfo(serializedSkip, seekLen, rawSeek, numFields, seek, stepExtra);
                            continuation->setPacket(continuation->queryPacket()->insertSkipData(serializedSkip.length(), serializedSkip.toByteArray()));
                            ROQ->sendPacket(continuation->queryPacket(), adaptor.activity.queryLogCtx());
                            adaptor.sentsome.signal();
                        }
                        numPending++;
                    }
                }
                else
                {
                    heap.zap(idx);
                    removeHeap(idx);
                    idx--;
                }
            }
            return skipped;
        }

        const void * nextSteppedGE(const void * seek, const void *rawSeek, unsigned numFields, unsigned seeklen, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
        {
            // We discard all rows < seekval from all entries in heap
            // If this results in additional slave requests, we return NULL so that we can wait for them
            // If not, we rebuild the heap (if any were skipped) and return the first row
            deferredContinuation = false;
            if (heap.length())
            {
                unsigned skipped = 0;
                unsigned idx = 0;
                while(heapEntries.isItem(idx))
                {
                    skipped += skipRows(idx, seek, rawSeek, numFields, seeklen, stepExtra);
                    idx++;
                }
                if (numPending)
                    return NULL;  // can't answer yet, need more results from slaves
                else
                {
                    if (skipped)
                        makeHeap();
                    return next(wasCompleteMatch, stepExtra);
                }
            }
            else
                return NULL;
        }

        bool doContinuation(HeapEntry &topEntry, bool canDefer)
        {
            IRoxieServerQueryPacket *continuation = topEntry.packet->queryContinuation();
            if (continuation)
            {
                if (continuation->isDelayed() && canDefer)
                {
                    if (adaptor.activity.queryLogCtx().queryTraceLevel() > 10)
                        adaptor.activity.queryLogCtx().CTXLOG("Deferring continuation");
                    deferredContinuation = true;
                }
                else
                {
                    deferredContinuation = false;
                    continuation->Link();
                    topEntry.packet->Release();
                    topEntry.packet = continuation;
                    if (continuation->hasResult())
                    {
                        IMessageResult *result = continuation->queryResult();
                        bool lastIsCompleteMatch = isCompleteMatchFlag(result);
                        topEntry.noteResult(result->getCursor(rowManager), lastIsCompleteMatch);
                    }
                    else
                    {
                        if (continuation->isDelayed()) // has the continuation been requested yet?
                        {
                            continuation->Link();
                            topEntry.packet->Release();
                            topEntry.packet = continuation;
                            continuation->setDelayed(false);
                            if (adaptor.activity.queryLogCtx().queryTraceLevel() > 10)
                                adaptor.activity.queryLogCtx().CTXLOG("About to send continuation, from doContinuation");
                            ROQ->sendPacket(continuation->queryPacket(), adaptor.activity.queryLogCtx());
                            adaptor.sentsome.signal();
                        }
                        numPending++;   // we are waiting for one that is already in flight
                    }
                }
                return true;  // next not known yet
            }
            else
                return false;
        }

        const void *next(bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
        {
            OwnedConstRoxieRow ret;
            if (heap.length())
            {
                if (deferredContinuation)
                {
                    unsigned top = heap.item(0);
                    HeapEntry &topEntry = heapEntries.item(top);
                    doContinuation(topEntry, false);
                    return NULL;
                }
                if (remakePending)
                    remakeHeap();
                unsigned top = heap.item(0);
                HeapEntry &topEntry = heapEntries.item(top);
                ret.set(topEntry.current);
                wasCompleteMatch = topEntry.isCompleteMatch();
                const void *next = topEntry.next();
                if (!next)
                {
                    if (!doContinuation(topEntry, stepExtra.returnMismatches()))
                    {
                        unsigned last = heap.pop();
                        if (heap.length())
                            heap.replace(last, 0);
                        removeHeap(top);
                    }
                }
                remakePending = true;
            }
            return ret.getClear();
        }

        bool ready()
        {
            return endSeen && numPending == 0;  
        }

    };

    IRoxieServerQueryPacket *createRoxieServerQueryPacket(IRoxieQueryPacket *p, bool &cached)
    {
        if (serverSideCache && !debugContext)
        {
            IRoxieServerQueryPacket *ret = serverSideCache->findCachedResult(activity.queryLogCtx(), p);
            if (ret)
            {
                p->Release();
                cached = true;
                return ret;
            }
        }
        cached = false;
        return new CRoxieServerQueryPacket(p);
    }

#ifdef _DEBUG
    void dumpPending()
    {
        CriticalBlock b(pendingCrit);
        ForEachItemIn(idx, pending)
        {
            IRoxieServerQueryPacket &p = pending.item(idx);
            StringBuffer s;
            unsigned __int64 dummy;
            if (p.isEnd())
                s.append("END");
            else if (p.isLimit(dummy, dummy, dummy))
                s.append("LIMIT");
            else
            {
                IRoxieQueryPacket *i = p.queryPacket();
                s.appendf("%s", p.hasResult() ? "COMPLETE " : "PENDING ");
                if (i)
                {
                    RoxiePacketHeader &header = i->queryHeader();
                    header.toString(s);
                }
            }
            DBGLOG("Pending %d %s", idx, s.str());
        }
    }
#endif

    void abortPending()
    {
        CriticalBlock b(pendingCrit);
        ForEachItemIn(idx, pending)
        {
            IRoxieServerQueryPacket &p = pending.item(idx);
            if (!p.hasResult())
            {
                IRoxieQueryPacket *i = p.queryPacket();
                if (i)
                {
                    RoxiePacketHeader &header = i->queryHeader();
                    ROQ->sendAbort(header, activity.queryLogCtx());
                }
            }
        }
        pending.kill();
    }

    void checkDelayed()
    {
        if (ctx->queryDebugContext() && ctx->queryDebugContext()->getExecuteSequentially())
        {
            bool allDelayed = true;
            CriticalBlock b(pendingCrit);
            unsigned sendIdx = (unsigned) -1;
            ForEachItemIn(idx, pending)
            {
                IRoxieServerQueryPacket &p = pending.item(idx);
                if (p.queryPacket())
                {
                    if (p.isDelayed())
                    {
                        if (sendIdx == (unsigned) -1)
                            sendIdx = idx;
                    }
                    else if (!p.hasResult())
                    {
                        allDelayed = false;
                        break;
                    }
                }
            }
            if (allDelayed && sendIdx != (unsigned) -1)
            {
                if (activity.queryLogCtx().queryTraceLevel() > 10)
                    activity.queryLogCtx().CTXLOG("About to send debug-deferred from next");
                pending.item(sendIdx).setDelayed(false);
                ROQ->sendPacket(pending.item(sendIdx).queryPacket(), activity.queryLogCtx());
                sentsome.signal();
            }
        }
        else if (deferredStart)
        {
            CriticalBlock b(pendingCrit);
            ForEachItemIn(idx, pending)
            {
                IRoxieServerQueryPacket &p = pending.item(idx);
                if (p.isDelayed())
                {
                    if (activity.queryLogCtx().queryTraceLevel() > 10)
                        activity.queryLogCtx().CTXLOG("About to send deferred start from next");
                    p.setDelayed(false);
                    ROQ->sendPacket(p.queryPacket(), activity.queryLogCtx());
                    sentsome.signal();
                }
            }
            deferredStart = false;
        }
    }

    void retryPending()
    {
        CriticalBlock b(pendingCrit);
        checkDelayed();
        ForEachItemIn(idx, pending)
        {
            IRoxieServerQueryPacket &p = pending.item(idx);
            if (!p.hasResult() && !p.isDelayed())
            {
                IRoxieQueryPacket *i = p.queryPacket();
                if (i)
                {
                    if (!i->queryHeader().retry())
                    {
                        StringBuffer s;
                        IException *E = MakeStringException(ROXIE_MULTICAST_ERROR, "Failed to get response from slave(s) for %s in activity %d", i->queryHeader().toString(s).str(), queryId());
                        activity.queryLogCtx().logOperatorException(E, __FILE__, __LINE__, "CRemoteResultAdaptor::retry");
                        throw E;
                    }
                    if (!localSlave) 
                    {
                        ROQ->sendPacket(i, activity.queryLogCtx());
                        atomic_inc(&retriesSent);
                    }
                }
            }
        }
    }

    class ChannelBuffer
    {
    protected:
        unsigned bufferLeft;
        MemoryBuffer buffer;
        char *nextBuf;
        unsigned overflowSequence;
        unsigned channel;  // == bonded channel
        bool needsFlush;
        InterruptableSemaphore flowController;
        const CRemoteResultAdaptor &owner;
        CriticalSection crit;

    public:
        ChannelBuffer(const CRemoteResultAdaptor &_owner, unsigned _channel) : owner(_owner), channel(_channel), flowController(perChannelFlowLimit)
        {
            overflowSequence = 0;
            needsFlush = false;
            bufferLeft = 0;
            nextBuf = NULL;
        }

        void init(unsigned minSize)
        {
            assertex(!buffer.length());
            if (minSize < MIN_PAYLOAD_SIZE)
                minSize = MIN_PAYLOAD_SIZE;
            unsigned headerSize = sizeof(RoxiePacketHeader)+owner.headerLength();
            unsigned bufferSize = headerSize+minSize;
            if (bufferSize < mtu_size)
                bufferSize = mtu_size;
            buffer.reserveTruncate(bufferSize);
            bufferLeft = bufferSize - headerSize;
            assertex(buffer.toByteArray());
            nextBuf = (char *) buffer.toByteArray() + headerSize;
            needsFlush = false;
        }

        inline IRoxieQueryPacket *flush()
        {
            CriticalBlock cb(crit);
            Owned<IRoxieQueryPacket> ret;
            if (needsFlush)
            {
                buffer.setLength(nextBuf - buffer.toByteArray());
                RoxiePacketHeader *h = (RoxiePacketHeader *) buffer.toByteArray();
                h->init(owner.remoteId, owner.ruid, channel, overflowSequence);

                //patch logPrefix, cachedContext and parent extract into the place reserved in the message buffer
                byte * tgt = (byte*)(h+1);
                owner.copyHeader(tgt, channel);

                ret.setown(createRoxiePacket(buffer));
                if (overflowSequence == OVERFLOWSEQUENCE_MAX)
                    overflowSequence = 1; // don't wrap to 0 - that is a bit special
                else
                    overflowSequence++;
                needsFlush = false;
                bufferLeft = 0;
                if (owner.flowControlled)
                {
                    CriticalUnblock cub(crit);
                    while (!flowController.wait(1000))
                    {
                        StringBuffer s;
                        owner.activity.queryLogCtx().CTXLOG("Channel %d blocked by flow control: %s", channel, h->toString(s).str());
                    }
                }
            }
            return ret.getClear();
        }

        inline void signal()
        {
            if (owner.flowControlled)
                flowController.signal();
        }

        inline void interrupt(IException *e)
        {
            flowController.interrupt(e);
        }

        inline void *getBuffer(unsigned size)
        {
            CriticalBlock cb(crit);
            if (bufferLeft >= size)
            {
                needsFlush = true;
                void * ret = nextBuf;
                nextBuf += size;
                bufferLeft -= size;
                return ret;
            }
            else if (!needsFlush)
            {
                init(size);
                return getBuffer(size);
            }
            else if (owner.mergeOrder)
            {
                return buffer.reserve(size); // whole query needs to go as single packet if we are to merge
            }
            else
                return NULL; // will force it to flush and start a new packet
        }
    };

private:
    friend class CRemoteResultMerger;
    bool allread;
    bool contextCached;
    bool preserveOrder;

    InterruptableSemaphore sentsome;
    Owned <IMessageCollator> mc;
    Owned<IMessageUnpackCursor> mu;
    Owned<IMessageResult> mr;
    ChannelBuffer **buffers;
    IHThorArg &helper;
    unsigned __int64 stopAfter;
    unsigned resendSequence;
    IHThorArg *colocalArg;
    IArrayOf<IRoxieServerQueryPacket> pending;
    CriticalSection pendingCrit;
    IRoxieServerSideCache *serverSideCache;
    unsigned sentSequence;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IEngineRowAllocator> rowAllocator;
    CRemoteResultMerger merger;

    // this is only used to avoid recreating a bufferStream for each row.  A better solution may be needed
    MemoryBuffer tempRowBuffer;     
    Owned<ISerialStream> bufferStream;
    CThorStreamDeserializerSource rowSource;

protected:
    IRowManager *rowManager;
    IRoxieInput *owner;
    unsigned __int64 rowLimit;
    unsigned __int64 keyedLimit;
    IRoxieServerErrorHandler *errorHandler;
    CachedOutputMetaData meta;

public:
    ISteppingMeta *mergeOrder;
    IRoxieSlaveContext *ctx;
    IDebuggableContext *debugContext;
    IRoxieServerActivity &activity;
    unsigned parentExtractSize;
    const byte * parentExtract;
    bool flowControlled;
    bool deferredStart;
    MemoryBuffer logInfo;
    MemoryBuffer cachedContext;
    const RemoteActivityId &remoteId;
    ruid_t ruid;
    mutable CriticalSection buffersCrit;
    unsigned processed;
    unsigned __int64 totalCycles;

//private:   //vc6 doesn't like this being private yet accessed by nested class...
    const void *getRow(IMessageUnpackCursor *mu) 
    {
        if (!mu->isSerialized() || (meta.isFixedSize() && !deserializer))
            return mu->getNext(meta.getFixedSize());
        else
        {
            RecordLengthType *rowlen = (RecordLengthType *) mu->getNext(sizeof(RecordLengthType));
            if (rowlen)
            {
                RecordLengthType len = *rowlen;
                ReleaseRoxieRow(rowlen);
                const void *slaveRec = mu->getNext(len);
                if (deserializer && mu->isSerialized())
                {
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    tempRowBuffer.setBuffer(len, const_cast<void *>(slaveRec), false);
                    size_t outsize = deserializer->deserialize(rowBuilder, rowSource);
                    ReleaseRoxieRow(slaveRec);
                    return rowBuilder.finalizeRowClear(outsize);
                }
                else
                    return slaveRec;
            }
            else
                return NULL;
        }
    }

private:

    ChannelBuffer *queryChannelBuffer(unsigned channel, bool force=false)
    {
        CriticalBlock cb(buffersCrit);
        ChannelBuffer *b = buffers[channel];
        if (!b && force)
        {
            if (!contextCached)
            {
                logInfo.clear();
                unsigned char loggingFlags = LOGGING_FLAGSPRESENT | LOGGING_TRACELEVELSET;
                unsigned char ctxTraceLevel = activity.queryLogCtx().queryTraceLevel() + 1; // Avoid passing a 0
                if (activity.queryLogCtx().isIntercepted())
                    loggingFlags |= LOGGING_INTERCEPTED;
                if (ctx->queryTimeActivities())
                    loggingFlags |= LOGGING_TIMEACTIVITIES; 
                if (activity.queryLogCtx().isBlind())
                    loggingFlags |= LOGGING_BLIND;
                if (debugContext)
                {
                    loggingFlags |= LOGGING_DEBUGGERACTIVE;
                    logInfo.append(loggingFlags).append(ctxTraceLevel);
                    MemoryBuffer bpInfo;
                    debugContext->serialize(bpInfo);
                    bpInfo.append((__uint64)(memsize_t) &activity);
                    logInfo.append((unsigned short) bpInfo.length());
                    logInfo.append(bpInfo.length(), bpInfo.toByteArray());
                }
                else
                    logInfo.append(loggingFlags).append(ctxTraceLevel);
                StringBuffer logPrefix;
                activity.queryLogCtx().getLogPrefix(logPrefix);
                logInfo.append(logPrefix);
                activity.serializeCreateStartContext(cachedContext.clear());
                activity.serializeExtra(cachedContext);
                if (activity.queryVarFileInfo())
                {
                    activity.queryVarFileInfo()->queryTimeStamp().serialize(cachedContext);
                    cachedContext.append(activity.queryVarFileInfo()->queryCheckSum());
                }
                contextCached = true;
            }

            b = buffers[channel] = new ChannelBuffer(*this, channel);
        }
        return b;
    }

    void processRow(const void *got)
    {
        processed++;
        if (processed > rowLimit)
        {
            ReleaseRoxieRow(got);
            errorHandler->onLimitExceeded(false); // NOTE - should throw exception
            throwUnexpected();
        }
        else if (processed > keyedLimit)
        {
            ReleaseRoxieRow(got);
            errorHandler->onLimitExceeded(true); // NOTE - should throw exception
            throwUnexpected();
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CRemoteResultAdaptor(const RemoteActivityId &_remoteId, IOutputMetaData *_meta, IHThorArg &_helper, IRoxieServerActivity &_activity, bool _preserveOrder, bool _flowControlled)
        : remoteId(_remoteId), meta(_meta), activity(_activity), helper(_helper), preserveOrder(_preserveOrder), flowControlled(_flowControlled), merger(*this)
    {
        rowLimit = (unsigned __int64) -1;
        keyedLimit = (unsigned __int64) -1;
        contextCached = false;
        stopAfter = I64C(0x7FFFFFFFFFFFFFFF);
        buffers = new ChannelBuffer*[numChannels+1];
        memset(buffers, 0, (numChannels+1)*sizeof(ChannelBuffer *));
        parentExtractSize = 0;
        parentExtract = NULL;
        owner = NULL;
        mergeOrder = NULL;
        deferredStart = false;
        processed = 0;
        totalCycles = 0;
        sentSequence = 0;
        serverSideCache = activity.queryServerSideCache();
        bufferStream.setown(createMemoryBufferSerialStream(tempRowBuffer));
        rowSource.setStream(bufferStream);
    }

    ~CRemoteResultAdaptor()
    {
        if (mc)
        {
            ROQ->queryReceiveManager()->detachCollator(mc);
            mc.clear();
        }
        for (unsigned channel = 0; channel <= numChannels; channel++)
        {
            delete(buffers[channel]);
        }
        delete [] buffers;
    }

    void setMeta(IOutputMetaData *newmeta)
    {
        meta.set(newmeta);
    }

    virtual IRoxieServerActivity *queryActivity()
    {
        return &activity;
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        return NULL;
    }

    void setMergeInfo(ISteppingMeta *_mergeOrder)
    {
        mergeOrder = _mergeOrder;
        deferredStart = true;
    }

    void send(IRoxieQueryPacket *p)
    {
        if (p)
        {
            Linked<IRoxieQueryPacket> saver(p); // avoids a race with abortPending, without keeping pendingCrit locked over the send which we might prefer not to
            assertex(p->queryHeader().uid==ruid);

            // MORE: Maybe we should base the fastlane flag on some other 
            //       criteria !! (i.e A Roxie server prediction based on the 
            //       activity type/activity behaviour/expected reply size .. etc).
            //       
            //       Currently (code below) based on high priority, seq=0, and none-child activity.
            //       But this could still cause too many reply packets on the fatlane
            //       (higher priority output Q), which may cause the activities on the 
            //       low priority output Q to not get service on time.
            if ((colocalArg == 0) &&     // not a child query activity??
                    (p->queryHeader().activityId & (ROXIE_SLA_PRIORITY | ROXIE_HIGH_PRIORITY)) && 
                    (p->queryHeader().overflowSequence == 0) &&
                    (p->queryHeader().continueSequence & ~CONTINUE_SEQUENCE_SKIPTO)==0)
                p->queryHeader().retries |= ROXIE_FASTLANE;

            if (p->queryHeader().channel)
            {
                bool cached = false;
                IRoxieServerQueryPacket *rsqp = createRoxieServerQueryPacket(p, cached);
                if (deferredStart)
                    rsqp->setDelayed(true);
                rsqp->setSequence(sentSequence++);
                {
                    CriticalBlock b(pendingCrit);
                    pending.append(*rsqp);
                }
                if (!deferredStart)
                {
                    if (!cached)
                        ROQ->sendPacket(p, activity.queryLogCtx());
                    sentsome.signal();
                }
            }
            else
            {
                // Care is needed here. If I send the packet before I add to the pending there is a danger that I'll get results that I discard 
                // Need to add first, then send
                unsigned i;
                bool allCached = true;
                for (i = 1; i <= numActiveChannels; i++)
                {
                    IRoxieQueryPacket *q = p->clonePacket(i);
                    bool thisChannelCached;
                    IRoxieServerQueryPacket *rsqp = createRoxieServerQueryPacket(q, thisChannelCached);
                    if (!thisChannelCached)
                        allCached = false;
                    rsqp->setSequence(sentSequence++);
                    if (deferredStart)
                    {
                        rsqp->setDelayed(true);
                    }
                    {
                        CriticalBlock b(pendingCrit);
                        pending.append(*rsqp);
                    }
                    if (!deferredStart)
                            sentsome.signal();
                }
                if (!allCached && !deferredStart)
                    ROQ->sendPacket(p, activity.queryLogCtx());
                buffers[0]->signal(); // since replies won't come back on that channel...
                p->Release();
            }
        }
    }

    void *getMem(unsigned partNo, unsigned fileNo, unsigned size)
    {
        unsigned channel = partNo ? getBondedChannel(partNo) : 0;
        size += sizeof(PartNoType);
        ChannelBuffer *b = queryChannelBuffer(channel, true);
        char *buffer = (char *) b->getBuffer(size);
        if (!buffer)
        {
            send(b->flush());
            buffer = (char *) b->getBuffer(size);
        }
        PartNoType sp;
        sp.partNo = partNo;
        sp.fileNo = fileNo;
        memcpy(buffer, &sp, sizeof(sp));
        buffer += sizeof(sp);
        return buffer;
    }

    void injectResult(IMessageResult *result)
    {
        IRoxieServerQueryPacket *f = new CRoxieServerQueryPacket(NULL);
        f->setSequence(sentSequence++);
        f->setResult(result);
        CriticalBlock b(pendingCrit);
        pending.append(*f);
        sentsome.signal(); // MORE - arguably should only send if there is any point waking up the listener thread, to save context swicth
    }

    void flush()
    {
        for (unsigned channel = 0; channel <= numChannels; channel++)
        {
            ChannelBuffer *b = queryChannelBuffer(channel, false);
            if (b)
                send(b->flush());
        }
    }

    void interruptBuffers(IException *e)
    {
        for (unsigned channel = 0; channel <= numChannels; channel++)
        {
            ChannelBuffer *b = queryChannelBuffer(channel, false);
            if (b)
                b->interrupt(LINK(e));
        }
    }

    void senddone()
    {
        CriticalBlock b(pendingCrit);
        pending.append(*new CRoxieServerQueryPacketEndMarker);
        sentsome.signal();
    }

    bool fireException(IException *e)
    {
        {
            CriticalBlock b(pendingCrit);
            pending.append(*new CRoxieServerQueryPacketEndMarker);
        }
        interruptBuffers(e);
        if (mc)
            mc->interrupt(LINK(e));
        sentsome.interrupt(e);
        return true;
    }

    virtual void onCreate(IRoxieInput *_owner, IRoxieServerErrorHandler *_errorHandler, IRoxieSlaveContext *_ctx, IHThorArg *_colocalArg)
    {
        owner = _owner;
        errorHandler = _errorHandler;
        ctx = _ctx;
        debugContext = ctx->queryDebugContext();
        colocalArg = _colocalArg;
        if (meta.needsSerializeDisk())
        {
            deserializer.setown(meta.createDiskDeserializer(_ctx->queryCodeContext(), activity.queryId()));
            rowAllocator.setown(ctx->queryCodeContext()->getRowAllocator(meta.queryOriginal(), activity.queryId()));
        }
        if (ctx->queryDebugContext() && ctx->queryDebugContext()->getExecuteSequentially())
            deferredStart = true;
    }

    virtual unsigned queryId() const
    {
        return owner->queryId();
    }

    virtual void onStart(unsigned _parentExtractSize, const byte * _parentExtract)
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAonstart");
#endif
        sentsome.reinit();
        ruid = getNextRuid();
        rowManager = &ctx->queryRowManager();
        if (mergeOrder)
            merger.init(mergeOrder, rowManager);
        if (mc)
        {
            ROQ->queryReceiveManager()->detachCollator(mc); // Should never happen - implies someone forgot to call onReset!
        }
        mc.setown(ROQ->queryReceiveManager()->createMessageCollator(rowManager, ruid));
        allread = false;
        mu.clear();
        contextCached = false;
        processed = 0;
        totalCycles = 0;
        resendSequence = 0;
        sentSequence = 0;
        for (unsigned channel = 0; channel <= numChannels; channel++)
        {
            delete(buffers[channel]);
            buffers[channel] = NULL;
        }
        flush();
        parentExtractSize = _parentExtractSize;
        parentExtract = _parentExtract;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAstart");
#endif
        owner->start(parentExtractSize, parentExtract, paused);
        totalCycles = 0;
    }

    void checkAbort()
    {
        owner->checkAbort();
    }

    void setLimits(unsigned __int64 _rowLimit, unsigned __int64 _keyedLimit, unsigned __int64 _stopAfter)
    {
        if (ctx->queryProbeManager())
        {
            if (_rowLimit != (unsigned __int64) -1) ctx->queryProbeManager()->setNodePropertyInt(&activity, "rowLimit", _rowLimit);
            if (_keyedLimit != (unsigned __int64) -1) ctx->queryProbeManager()->setNodePropertyInt(&activity, "keyedLimit", _keyedLimit);
            if (_stopAfter != I64C(0x7FFFFFFFFFFFFFFF)) ctx->queryProbeManager()->setNodePropertyInt(&activity, "choosenLimit", _stopAfter);
        }
        {
            CriticalBlock b(pendingCrit);
            if (pending.length())
            {
#ifdef _DEBUG
                dumpPending();   // MORE - only defined in debug build - could have put the ifdef inside the dumpPending method
#endif
                assertex(pending.length()==0);
            }
            pending.append(*new CRoxieServerQueryPacketLimitMarker(_rowLimit, _keyedLimit, _stopAfter));
        }
        sentsome.signal();
        rowLimit = _rowLimit;
        keyedLimit = _keyedLimit;
        stopAfter = _stopAfter;
    }

    virtual void stop(bool aborting)
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAstop");
#endif
        onStop(aborting);
        owner->stop(aborting);
    }

    void onStop(bool aborting)
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAonstop");
#endif
        abortPending();
        interruptBuffers(NULL);
        sentsome.interrupt();
        if (mc)  // May not be set if start() chain threw exception
            mc->interrupt();
    }

    virtual void reset()
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAreset");
#endif
        owner->reset();
        onReset();
    }

    virtual void resetEOF()
    {
        throwUnexpected();
    }

    virtual void onReset()
    {
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
            activity.queryLogCtx().CTXLOG("RRAonreset");
#endif
        if (mc)
            ROQ->queryReceiveManager()->detachCollator(mc);
        merger.reset();
        pending.kill();
        if (mc && ctx)
            ctx->addSlavesReplyLen(mc->queryBytesReceived());
        mc.clear(); // Or we won't free memory for graphs that get recreated
        mu.clear(); //ditto
        mergeOrder = NULL; // MORE - is that needed?
        deferredStart = false;
    }

    virtual IOutputMetaData * queryOutputMeta() const
    {
        return helper.queryOutputMeta();
    }

    virtual unsigned __int64 queryTotalCycles() const
    {
        return totalCycles;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return owner->queryLocalCycles();
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return owner->queryInput(idx);
    }

    const void * nextSteppedGE(const void *seek, const void *rawSeek, unsigned numFields, unsigned seekLen, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        if (activity.queryLogCtx().queryTraceLevel() > 10)
        {
            StringBuffer recstr;
            unsigned i;
            for (i = 0; i < seekLen; i++)
            {
                recstr.appendf("%02x ", ((unsigned char *) rawSeek)[i]);
            }
            activity.queryLogCtx().CTXLOG("CRemoteResultAdaptor::nextSteppedGE(rawSeek=%s numFields=%d, seeklen=%d, returnMismatches=%d)", recstr.str(), numFields, seekLen, stepExtra.returnMismatches());
        }
        assertex(mergeOrder);
        if (deferredStart)
        {
            CriticalBlock b(pendingCrit);
            ForEachItemIn(idx, pending)
            {
                IRoxieServerQueryPacket &p = pending.item(idx);
                if (p.isDelayed())
                {
                    p.setDelayed(false);
                    if (activity.queryLogCtx().queryTraceLevel() > 10)
                        activity.queryLogCtx().CTXLOG("About to send deferred start from nextSteppedGE, setting requireExact to %d", !stepExtra.returnMismatches());
                    MemoryBuffer serializedSkip;
                    activity.serializeSkipInfo(serializedSkip, seekLen, rawSeek, numFields, seek, stepExtra);
                    p.setPacket(p.queryPacket()->insertSkipData(serializedSkip.length(), serializedSkip.toByteArray()));
                    ROQ->sendPacket(p.queryPacket(), activity.queryLogCtx());
                    sentsome.signal();
                }
            }
            deferredStart = false;
        }
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (processed==stopAfter)
            return NULL;
        if (allread)
            return NULL;
        loop
        {
            if (merger.ready())
            {
                const void *got = merger.nextSteppedGE(seek, rawSeek, numFields, seekLen, wasCompleteMatch, stepExtra);
                if (got)
                {
                    processRow(got);
                    return got;
                }
            }
            if (!reload()) // MORE - should pass the seek info here...
                return NULL;
        }
    }

    virtual const void *nextInGroup()
    {
        // If we are merging then we need to do a heapsort on all 
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (activity.queryLogCtx().queryTraceLevel() > 10)
        {
            activity.queryLogCtx().CTXLOG("CRemoteResultAdaptor::nextInGroup()");
        }
        loop
        {
            checkDelayed();
            if (processed==stopAfter)
                return NULL;
            if (allread)
                return NULL;
            // If we can still consume from the merger or the most recently retrieved mu, do so. 
            const void *got = NULL;
            if (mergeOrder && merger.ready())
            {
                bool matched = true;
                got = merger.next(matched, dummySmartStepExtra);
            }
            else if (mu)
                got = getRow(mu);
            if (got)
            {
                processRow(got);
                return got;
            }
            if (!reload())
                return NULL;
        }
    }

    bool reload()
    {
        // Wait for something to be returned from a slave....
        mu.clear();
        sentsome.wait();
        // must be at least an endMarker on the queue since sentsome was signalled
        { 
            CriticalBlock b(pendingCrit);
            IRoxieServerQueryPacket &top = pending.item(0);
            if (top.isLimit(rowLimit, keyedLimit, stopAfter)) // This is really a start marker...
            {
                pending.remove(0);
                return true;
            }
            else if (top.isEnd())
            {
                pending.remove(0);
                allread = true;
                if (activity.queryLogCtx().queryTraceLevel() > 5)
                    activity.queryLogCtx().CTXLOG("All read on ruid %x", ruid);
                return false;
            }
            else if (mergeOrder)
            {
                unsigned idx = 0;
                bool added = false;
                while (pending.isItem(idx))
                {
                    IRoxieServerQueryPacket &item = pending.item(idx);
                    if (item.isEnd())
                    {
                        if (merger.noteEndSeen())
                        {
                            sentsome.signal(); // Because we waited, yet didn't actually consume anything
                            added = true;
                        }
                        break;
                    }
                    else if (item.hasResult())
                    {
                        merger.noteResult(&item, item.getSequence());
                        pending.remove(idx);
                        added = true;
                    }
                    else if (item.isContinuation())
                        idx++;
                    else
                        break;
                }
                if (added)
                    return true;
            }
            else if (top.hasResult())
            {
                mr.setown(pending.item(0).getResult());
                mu.setown(mr->getCursor(rowManager));
                pending.remove(0);
                return true;
            }
        }
        getNextUnpacker();
        return true;
    }

    void getNextUnpacker()
    {
        mu.clear();
        unsigned ctxTraceLevel = activity.queryLogCtx().queryTraceLevel();
        loop
        {
            checkDelayed();
            unsigned timeout = remoteId.isSLAPriority() ? slaTimeout : (remoteId.isHighPriority() ? highTimeout : lowTimeout);
            owner->checkAbort();
            bool anyActivity;
            if (ctxTraceLevel > 5)
                activity.queryLogCtx().CTXLOG("Calling getNextUnpacker(%d)", timeout);
            mr.setown(mc->getNextResult(timeout, anyActivity));
            if (ctxTraceLevel > 6)
                activity.queryLogCtx().CTXLOG("Called getNextUnpacker(%d), activity=%d", timeout, anyActivity);
            owner->checkAbort();
            if (mr)
            {
                unsigned roxieHeaderLen;
                const RoxiePacketHeader &header = *(const RoxiePacketHeader *) mr->getMessageHeader(roxieHeaderLen);
#ifdef _DEBUG
                assertex(roxieHeaderLen == sizeof(RoxiePacketHeader));
#endif
                if (ctxTraceLevel > 5)
                {
                    StringBuffer s;
                    activity.queryLogCtx().CTXLOG("getNextUnpacker got packet %s", header.toString(s).str());
                }

                CriticalBlock b(pendingCrit);
                unsigned idx = 0;
                IRoxieServerQueryPacket *original = NULL;
                IRoxieQueryPacket *op;
                while (pending.isItem(idx))
                {
                    original = &pending.item(idx);
                    op = original->queryPacket();
                    if (op && header.matchPacket(op->queryHeader()))
                        break;
                    original = NULL;
                    idx++;
                }
                if (!original || original->hasResult())
                {
                    switch (header.activityId)
                    {
                        case ROXIE_FILECALLBACK:
                        {
                            // tell slave to abort 
                            //if (ctxTraceLevel > 5)
                            {
                                StringBuffer s;
                                activity.queryLogCtx().CTXLOG("Redundant callback on query %s", header.toString(s).str());
                            }
                            Owned<IMessageUnpackCursor> callbackData = mr->getCursor(rowManager);
                            OwnedConstRoxieRow len = callbackData->getNext(sizeof(RecordLengthType));
                            if (len)
                            {
                                RecordLengthType *rowlen = (RecordLengthType *) len.get();
                                OwnedConstRoxieRow row = callbackData->getNext(*rowlen);
                                const char *rowdata = (const char *) row.get();
                                // bool isOpt = * (bool *) rowdata;
                                // bool isLocal = * (bool *) (rowdata+1);
                                ROQ->sendAbortCallback(header, rowdata+2, activity.queryLogCtx());
                            }
                            else
                                throwUnexpected();
                            break;
                        }
                            // MORE - ROXIE_ALIVE perhaps should go here too
                        case ROXIE_TRACEINFO:
                        {
                            Owned<IMessageUnpackCursor> extra = mr->getCursor(rowManager);
                            loop
                            {
                                RecordLengthType *rowlen = (RecordLengthType *) extra->getNext(sizeof(RecordLengthType));
                                if (rowlen)
                                {
                                    char *logInfo = (char *) extra->getNext(*rowlen);
                                    MemoryBuffer buf;
                                    buf.setBuffer(*rowlen, logInfo, false);
                                    activity.queryLogCtx().CTXLOGl(new LogItem(buf));
                                    ReleaseRoxieRow(rowlen);
                                    ReleaseRoxieRow(logInfo);
                                }
                                else
                                    break;
                            }
                            break;
                        }
                        default:
                            if (ctxTraceLevel > 3)
                                activity.queryLogCtx().CTXLOG("Discarding packet %p - original %p is NULL or has result already", mr.get(), original);
                            mr->discard();
                            break;
                    }
                    mr.clear();
                }
                else 
                {
                    atomic_inc(&resultsReceived);
                    switch (header.activityId)
                    {
                    case ROXIE_DEBUGCALLBACK:
                        {
                        Owned<IMessageUnpackCursor> callbackData = mr->getCursor(rowManager);
                        OwnedConstRoxieRow len = callbackData->getNext(sizeof(RecordLengthType));
                        if (len)
                        {
                            RecordLengthType *rowlen = (RecordLengthType *) len.get();
                            OwnedConstRoxieRow row = callbackData->getNext(*rowlen);
                            char *rowdata = (char *) row.get();
                            if (ctxTraceLevel > 5)
                            {
                                StringBuffer s;
                                activity.queryLogCtx().CTXLOG("Callback on query %s for debug", header.toString(s).str());
                            }
                            MemoryBuffer slaveInfo;
                            slaveInfo.setBuffer(*rowlen, rowdata, false);
                            unsigned debugSequence;
                            slaveInfo.read(debugSequence);
                            Owned<IRoxieQueryPacket> reply = original->getDebugResponse(debugSequence);
                            if (!reply)
                                reply.setown(activity.queryContext()->queryDebugContext()->onDebugCallback(header, *rowlen, rowdata));
                            if (reply)
                            {
                                original->setDebugResponse(debugSequence, reply);
                                ROQ->sendPacket(reply, activity.queryLogCtx());
                            }

                        }
                        else
                            throwUnexpected();
                        // MORE - somehow we need to make sure slave gets a reply even if I'm not waiting (in udp layer)
                        // Leave original message on pending queue in original location - this is not a reply to it.
                        break;
                        }

                    case ROXIE_FILECALLBACK:
                        {
                        // we need to send back to the slave a message containing the file info requested.
                        Owned<IMessageUnpackCursor> callbackData = mr->getCursor(rowManager);
                        OwnedConstRoxieRow len = callbackData->getNext(sizeof(RecordLengthType));
                        if (len)
                        {
                            RecordLengthType *rowlen = (RecordLengthType *) len.get();
                            OwnedConstRoxieRow row = callbackData->getNext(*rowlen);
                            const char *rowdata = (const char *) row.get();
                            bool isOpt = * (bool *) rowdata;
                            bool isLocal = * (bool *) (rowdata+1);
                            const char *lfn = rowdata+2;
                            if (ctxTraceLevel > 5)
                            {
                                StringBuffer s;
                                activity.queryLogCtx().CTXLOG("Callback on query %s file %s", header.toString(s).str(),(const char *) lfn);
                            }
                            activity.queryContext()->onFileCallback(header, lfn, isOpt, isLocal);
                        }
                        else
                            throwUnexpected();
                        // MORE - somehow we need to make sure slave gets a reply even if I'm not waiting (in udp layer)
                        // Leave original message on pending queue in original location - this is not a reply to it.
                        break;
                        }
                    case ROXIE_KEYEDLIMIT_EXCEEDED:
                        activity.queryLogCtx().CTXLOG("ROXIE_KEYEDLIMIT_EXCEEDED");
                        errorHandler->onLimitExceeded(true);    // NOTE - should throw exception!
                        throwUnexpected();

                    case ROXIE_LIMIT_EXCEEDED:
                        activity.queryLogCtx().CTXLOG("ROXIE_LIMIT_EXCEEDED");
                        errorHandler->onLimitExceeded(false);   // NOTE - should throw exception!
                        throwUnexpected();

                    case ROXIE_TRACEINFO:
                    {
                        Owned<IMessageUnpackCursor> extra = mr->getCursor(rowManager);
                        loop
                        {
                            RecordLengthType *rowlen = (RecordLengthType *) extra->getNext(sizeof(RecordLengthType));
                            if (rowlen)
                            {
                                char *logInfo = (char *) extra->getNext(*rowlen);
                                MemoryBuffer buf;
                                buf.setBuffer(*rowlen, logInfo, false);
                                activity.queryLogCtx().CTXLOGl(new LogItem(buf));
                                ReleaseRoxieRow(rowlen);
                                ReleaseRoxieRow(logInfo);
                            }
                            else
                                break;
                        }
                        break;
                    }
                    case ROXIE_EXCEPTION:
                        if (ctxTraceLevel > 1)
                        {
                            StringBuffer s;
                            activity.queryLogCtx().CTXLOG("Exception on query %s", header.toString(s).str());
                        }
                        op->queryHeader().noteException(header.retries);
                        if (op->queryHeader().allChannelsFailed())
                        {
                            activity.queryLogCtx().CTXLOG("Multiple exceptions on query - aborting");
                            Owned<IMessageUnpackCursor> exceptionData = mr->getCursor(rowManager);
                            throwRemoteException(exceptionData);
                        }
                        // Leave it on pending queue in original location
                        break;

                    case ROXIE_ALIVE:
                        if (ctxTraceLevel > 4)
                        {
                            StringBuffer s;
                            activity.queryLogCtx().CTXLOG("ROXIE_ALIVE: %s", header.toString(s).str());
                        }
                        op->queryHeader().noteAlive(header.retries & ROXIE_RETRIES_MASK);
                        // Leave it on pending queue in original location
                        break;

                    default:
                        if (header.retries & ROXIE_RETRIES_MASK)
                            atomic_inc(&retriesNeeded);
                        unsigned metaLen;
                        const void *metaData = mr->getMessageMetadata(metaLen);
                        if (metaLen)
                        {
                            // We got back first chunk but there is more.
                            // resend the packet, with the cursor info provided.
                            // MORE - if smart-stepping, we don't want to send the continuation immediately. Other cases it's not clear that we do.
                            if (ctxTraceLevel > 1)
                            {
                                StringBuffer s;
                                activity.queryLogCtx().CTXLOG("Additional data size %d on query %s mergeOrder %p", metaLen, header.toString(s).str(), mergeOrder);
                            }
                            if (*((unsigned short *) metaData) + sizeof(unsigned short) != metaLen)
                            {
                                StringBuffer s;
                                activity.queryLogCtx().CTXLOG("Additional data size %d on query %s mergeOrder %p", metaLen, header.toString(s).str(), mergeOrder);
                                activity.queryLogCtx().CTXLOG("Additional data is corrupt");
                                throwUnexpected();
                            }

                            MemoryBuffer nextQuery;
                            nextQuery.append(sizeof(RoxiePacketHeader), &header);
                            nextQuery.append(metaLen, metaData); 
                            nextQuery.append(op->getTraceLength(), op->queryTraceInfo());
                            nextQuery.append(op->getContextLength(), op->queryContextData());
                            if (resendSequence == CONTINUESEQUENCE_MAX)
                            {
                                activity.queryLogCtx().CTXLOG("ERROR: Continuation sequence wrapped"); // shouldn't actually matter.... but suggests a very iffy query!
                                resendSequence = 1;
                            }
                            else
                                resendSequence++;
                            RoxiePacketHeader *newHeader = (RoxiePacketHeader *) nextQuery.toByteArray();
                            newHeader->continueSequence = resendSequence; // NOTE - we clear the skipTo flag since continuation of a skip is NOT a skip...
                            newHeader->retries &= ~ROXIE_RETRIES_MASK;
                            IRoxieQueryPacket *resend = createRoxiePacket(nextQuery);
                            CRoxieServerQueryPacket *fqp = new CRoxieServerQueryPacket(resend);
                            fqp->setSequence(original->getSequence()); 
                            pending.add(*fqp, idx+1); // note that pending takes ownership. sendPacket does not release.
                            original->setContinuation(LINK(fqp));
                            if (mergeOrder)
                                fqp->setDelayed(true);
                            else
                            {
                                ROQ->sendPacket(resend, activity.queryLogCtx());
                                sentsome.signal();
                            }
                            // Note that we don't attempt to cache results that have continuation records - too tricky !
                        }
                        else
                        {
                            if (serverSideCache)
                                serverSideCache->noteCachedResult(original, mr);
                        }
                        unsigned channel = header.channel;
                        {
                            ChannelBuffer *b = queryChannelBuffer(channel); // If not something is wrong, or we sent out on channel 0?
                            if (b)
                                b->signal();
                        }
                        original->setResult(mr.getClear());
                        sentsome.signal();
                        return;
                    }
                }
            }
            else
            {
                if (!anyActivity)
                {
                    activity.queryLogCtx().CTXLOG("Input has stalled - retry required?");
                    retryPending();
                }
            }
        }
    }

    inline unsigned headerLength() const
    {
        return logInfo.length() + cachedContext.length() + sizeof(unsigned) + parentExtractSize;
    }

    void copyHeader(byte *tgt, unsigned channel) const
    {
        unsigned len = logInfo.length();
        memcpy(tgt, logInfo.toByteArray(), len);
        tgt += len;
        *(unsigned *) tgt = parentExtractSize;
        tgt += sizeof(unsigned);
        memcpy(tgt, parentExtract, parentExtractSize);
        tgt += parentExtractSize;
        memcpy(tgt, cachedContext.toByteArray(), cachedContext.length());
        tgt += cachedContext.length();
    }
};

class CSkippableRemoteResultAdaptor : public CRemoteResultAdaptor
{
    Owned <IException> exception;
    bool skipping;
    ConstPointerArray buff;
    unsigned index;
    bool pulled;

    void pullInput()
    {
        try
        {
            if (exception)
                throw exception.getClear();
            unsigned __int64 count = 0;
            loop
            {
                const void * next = CRemoteResultAdaptor::nextInGroup();
                if (next == NULL)
                {
                    next = CRemoteResultAdaptor::nextInGroup();
                    if(next == NULL)
                        break;
                    buff.append(NULL);
                }
                count++;
                if (count > rowLimit)
                {
                    ReleaseRoxieRow(next);
                    ReleaseRoxieRowSet(buff);
                    errorHandler->onLimitExceeded(false); // throws an exception - user or LimitSkipException
                    throwUnexpected();
                }
                else if (count > keyedLimit)
                {
                    ReleaseRoxieRow(next);
                    ReleaseRoxieRowSet(buff);
                    errorHandler->onLimitExceeded(true); // throws an exception - user or LimitSkipException
                    throwUnexpected();
                }
                buff.append(next);
            }
        }
        catch (IException *E)
        {
            if (QUERYINTERFACE(E, LimitSkipException))
            {
                Owned<IException> cleanup = E;
                ReleaseRoxieRowSet(buff);
                const void *onfail = errorHandler->createLimitFailRow(E->errorCode() == KeyedLimitSkipErrorCode);
                if (onfail)
                    buff.append(onfail);
            }
            else
                throw;
        }
        pulled = true;
    }

public:
    CSkippableRemoteResultAdaptor(const RemoteActivityId &_remoteId, IOutputMetaData *_meta, IHThorArg &_helper, IRoxieServerActivity &_activity, bool _preserveOrder, bool _flowControlled, bool _skipping) :
        CRemoteResultAdaptor(_remoteId, _meta, _helper, _activity, _preserveOrder, _flowControlled)
    {
        skipping = _skipping;
        index = 0;
        pulled = false;
    }

    void setException(IException *E)
    {
        exception.setown(E);
    }

    virtual void onReset()
    {
        while (buff.isItem(index))
            ReleaseRoxieRow(buff.item(index++));
        buff.kill();
        pulled = false;
        exception.clear();
        CRemoteResultAdaptor::onReset();
    }

    void onStart(unsigned _parentExtractSize, const byte * _parentExtract)
    {
        index = 0;
        pulled = false;
        CRemoteResultAdaptor::onStart(_parentExtractSize, _parentExtract);
    }

    virtual const void * nextSteppedGE(const void *seek, const void *rawSeek, unsigned numFields, unsigned seeklen, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        // MORE - not sure what we need to do about the skip case... but we need at least this to prevent issues with exception getting lost
        if (exception)
            throw exception.getClear();
        return CRemoteResultAdaptor::nextSteppedGE(seek, rawSeek, numFields, seeklen, wasCompleteMatch, stepExtra);
    }

    virtual const void *nextInGroup()
    {
        if (skipping)
        {
            if(!pulled)
                pullInput();
            if(buff.isItem(index))
            {
                const void * next = buff.item(index++);
                if(next)
                    processed++;
                return next;
            }
            return NULL;
        }
        else
        {
            if (exception)
                throw exception.getClear();
            return CRemoteResultAdaptor::nextInGroup();
        }
    }
}; 

//=================================================================================

class CRoxieServerApplyActivity : public CRoxieServerInternalSinkActivity
{
    IHThorApplyArg &helper;

public:
    CRoxieServerApplyActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorApplyArg &) basehelper)
    {
    }

    virtual void onExecute() 
    {
        helper.start();
        loop
        {
            const void * next = input->nextInGroup();
            if (!next)
            {
                next = input->nextInGroup();
                if (!next)
                    break;
            }
            helper.apply(next);
            ReleaseRoxieRow(next);
        }
        helper.end();
    }

};

class CRoxieServerApplyActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerApplyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerApplyActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerApplyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerApplyActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerNullActivity : public CRoxieServerActivity
{
public:
    CRoxieServerNullActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
    }

    virtual const void *nextInGroup()
    {
        return NULL;
    }

};

IRoxieServerActivity * createRoxieServerNullActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
{
    return new CRoxieServerNullActivity(_factory, _probeManager);
}

class CRoxieServerNullActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerNullActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNullActivity(this, _probeManager);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for null activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerNullActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNullActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerPassThroughActivity : public CRoxieServerActivity
{
public:
    CRoxieServerPassThroughActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); 
    }

    virtual const void *nextInGroup()
    {
        const void * next = input->nextInGroup();
        if (next)
            processed++;
        return next;
    }

    virtual bool isPassThrough()
    {
        return true;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (next)
            processed++;
        return next;
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }
};

IRoxieServerActivity * createRoxieServerPassThroughActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
{
    return new CRoxieServerPassThroughActivity(_factory, _probeManager);
}

//=================================================================================

class CRoxieServerChildBaseActivity : public CRoxieServerActivity
{
protected:
    bool eof;
    bool first;

public:
    CRoxieServerChildBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        eof = false;
        first = true;
    }

    ~CRoxieServerChildBaseActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        first = true;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }
};

class CRoxieServerChildBaseActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerChildBaseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for %s activity", getActivityText(kind));
    }
};


//=================================================================================

class CRoxieServerChildIteratorActivity : public CRoxieServerChildBaseActivity
{
    IHThorChildIteratorArg &helper;

public:
    CRoxieServerChildIteratorActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerChildBaseActivity(_factory, _probeManager), helper((IHThorChildIteratorArg &) basehelper)
    {
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        bool ok;
        if (first)
        {
            ok = helper.first();
            first = false;
        }
        else
            ok = helper.next();

        try
        {
            while (ok)
            {
                processed++;
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                unsigned outSize = helper.transform(rowBuilder);
                if (outSize)
                    return rowBuilder.finalizeRowClear(outSize);
                ok = helper.next();
            }
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }

        eof = true;
        return NULL;
    }

};

class CRoxieServerChildIteratorActivityFactory : public CRoxieServerChildBaseActivityFactory
{
public:
    CRoxieServerChildIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerChildBaseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChildIteratorActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerChildIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChildIteratorActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChildNormalizeActivity : public CRoxieServerChildBaseActivity
{
    IHThorChildNormalizeArg &helper;

public:
    CRoxieServerChildNormalizeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerChildBaseActivity(_factory, _probeManager), helper((IHThorChildNormalizeArg &) basehelper)
    {
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        bool ok;
        if (first)
        {
            ok = helper.first();
            first = false;
        }
        else
            ok = helper.next();

        if (ok)
        {
            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                do {
                    unsigned outSize = helper.transform(rowBuilder);
                    if (outSize)
                    {
                        processed++;
                        return rowBuilder.finalizeRowClear(outSize);
                    }
                    ok = helper.next();
                }
                while (ok);
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }

        eof = true;
        return NULL;
    }

};

class CRoxieServerChildNormalizeActivityFactory : public CRoxieServerChildBaseActivityFactory
{
public:
    CRoxieServerChildNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerChildBaseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChildNormalizeActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNewChildNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChildNormalizeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChildAggregateActivity : public CRoxieServerChildBaseActivity
{
    IHThorChildAggregateArg &helper;

public:
    CRoxieServerChildAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerChildBaseActivity(_factory, _probeManager), helper((IHThorChildAggregateArg &) basehelper)
    {
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        eof = true;
        processed++;
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        helper.clearAggregate(rowBuilder);
        helper.processRows(rowBuilder);
        size32_t finalSize = meta.getRecordSize(rowBuilder.getSelf());
        return rowBuilder.finalizeRowClear(finalSize);
    }

};

class CRoxieServerChildAggregateActivityFactory : public CRoxieServerChildBaseActivityFactory
{
public:
    CRoxieServerChildAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerChildBaseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChildAggregateActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNewChildAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChildAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChildGroupAggregateActivity : public CRoxieServerChildBaseActivity, public IHThorGroupAggregateCallback
{
    IHThorChildGroupAggregateArg &helper;
    RowAggregator aggregated;

public:
    IMPLEMENT_IINTERFACE
    CRoxieServerChildGroupAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerChildBaseActivity(_factory, _probeManager), helper((IHThorChildGroupAggregateArg &) basehelper),
          aggregated(helper, helper)
    {
    }

    void processRow(const void * next)
    {
        aggregated.addRow(next);
    }
            
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerChildBaseActivity::start(parentExtractSize, parentExtract, paused);
        aggregated.start(rowAllocator);
    }

    virtual void reset()
    {
        aggregated.reset();
        CRoxieServerChildBaseActivity::reset();
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        if (first)
        {
            helper.processRows(this);
            first = false;
        }

        Owned<AggregateRowBuilder> next = aggregated.nextResult();
        if (next)
        {
            processed++;
            return next->finalizeRowClear();
        }
        eof = true;
        return NULL;
    }
};

class CRoxieServerChildGroupAggregateActivityFactory : public CRoxieServerChildBaseActivityFactory
{
public:
    CRoxieServerChildGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerChildBaseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChildGroupAggregateActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNewChildGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChildGroupAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChildThroughNormalizeActivity : public CRoxieServerChildBaseActivity
{
    IHThorChildThroughNormalizeArg &helper;
    const void * lastInput;
    unsigned numProcessedLastGroup;
    bool ok;

public:
    CRoxieServerChildThroughNormalizeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerChildBaseActivity(_factory, _probeManager), helper((IHThorChildThroughNormalizeArg &) basehelper)
    {
        lastInput = NULL;
        numProcessedLastGroup = 0;
        ok = false;
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerChildBaseActivity::stop(aborting);
        ReleaseRoxieRow(lastInput);
        lastInput = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerChildBaseActivity::start(parentExtractSize, parentExtract, paused);
        numProcessedLastGroup = processed;
        ok = false;
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        loop
        {
            if (ok)
                ok = helper.next();

            while (!ok)
            {
                ReleaseRoxieRow(lastInput);
                lastInput = input->nextInGroup();
                if (!lastInput)
                {
                    if (numProcessedLastGroup != processed)
                    {
                        numProcessedLastGroup = processed;
                        return NULL;
                    }
                    lastInput = input->nextInGroup();
                    if (!lastInput)
                        return NULL;
                }

                ok = helper.first(lastInput);
            }

            try
            {
                do 
                {
                    unsigned outSize = helper.transform(rowBuilder);
                    if (outSize)
                    {
                        processed++;
                        return rowBuilder.finalizeRowClear(outSize);
                    }
                    ok = helper.next();
                } while (ok);
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerChildThroughNormalizeActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerChildThroughNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChildThroughNormalizeActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNewChildThroughNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChildThroughNormalizeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerLinkedRawIteratorActivity : public CRoxieServerActivity
{
    IHThorLinkedRawIteratorArg &helper;

public:
    CRoxieServerLinkedRawIteratorActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorLinkedRawIteratorArg &) basehelper)
    {
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void *ret =helper.next();
        if (ret)
        {
            LinkRoxieRow(ret);
            processed++;
        }
        return ret;
    }

};

class CRoxieServerLinkedRawIteratorActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerLinkedRawIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLinkedRawIteratorActivity(this, _probeManager);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for %s activity", getActivityText(kind));
    }
};

IRoxieServerActivityFactory *createRoxieServerLinkedRawIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerLinkedRawIteratorActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerDatasetResultActivity : public CRoxieServerActivity
{
public:
    CRoxieServerDatasetResultActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected();
    }

    virtual void executeChild(size32_t & retSize, void * & ret, unsigned parentExtractSize, const byte * parentExtract)
    {
        try
        {
            start(parentExtractSize, parentExtract, false);
            {
                ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
                MemoryBuffer result;
                IRecordSize * inputMeta = input->queryOutputMeta();
                loop
                {
                    const void *nextrec = input->nextInGroup();
                    if (!nextrec)
                    {
                        nextrec = input->nextInGroup();
                        if (!nextrec)
                            break;
                    }
                    result.append(inputMeta->getRecordSize(nextrec), nextrec);
                    ReleaseRoxieRow(nextrec);
                }
                retSize = result.length();
                ret = result.detach();
            }
            stop(false);
            reset(); 
        }
        catch(IException *E)
        {
            ctx->notifyAbort(E);
            stop(true);
            reset(); 
            throw;
        }
        catch(...)
        {
            Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught at %s:%d", __FILE__, __LINE__);
            ctx->notifyAbort(E);
            stop(true);
            reset(); 
            throw;
        }
    }


};

class CRoxieServerDatasetResultActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerDatasetResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerDatasetResultActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }

};

IRoxieServerActivityFactory *createRoxieServerDatasetResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerDatasetResultActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerInlineTableActivity : public CRoxieServerActivity
{
    IHThorInlineTableArg &helper;
    __uint64 curRow;
    __uint64 numRows;

public:
    CRoxieServerInlineTableActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorInlineTableArg &) basehelper)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        curRow = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numRows = helper.numRows();
    }

    virtual bool needsAllocator() const { return true; }
    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        // Filtering empty rows, returns the next valid row
        while (curRow < numRows)
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned outSize = helper.getRow(rowBuilder, curRow++);
            if (outSize)
            {
                processed++;
                return rowBuilder.finalizeRowClear(outSize);
            }
        }
        return NULL;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

};

class CRoxieServerInlineTableActivityFactory : public CRoxieServerActivityFactory
{

public:
    CRoxieServerInlineTableActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerInlineTableActivity(this, _probeManager);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for InlineTable activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerInlineTableActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerInlineTableActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerWorkUnitReadActivity : public CRoxieServerActivity
{
    IHThorWorkunitReadArg &helper;
    Owned<IWorkUnitRowReader> wuReader; // MORE - can we use IRoxieInput instead?

public:
    CRoxieServerWorkUnitReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorWorkunitReadArg &)basehelper)
    {
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        if (!ctx->queryServerContext())
        {
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Workunit read activity cannot be executed in slave context");
        }
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        IXmlToRowTransformer * xmlTransformer = helper.queryXmlTransformer();
        OwnedRoxieString fromWuid(helper.getWUID());
        if (fromWuid)
            UNIMPLEMENTED;
        wuReader.setown(ctx->getWorkunitRowReader(helper.queryName(), helper.querySequence(), xmlTransformer, rowAllocator, meta.isGrouped()));
        // MORE _ should that be in onCreate?
    }

    virtual void reset() 
    {
        wuReader.clear();
        CRoxieServerActivity::reset(); 
    };

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void *ret = wuReader->nextInGroup();
        if (ret)
            processed++;
        return ret;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

};

class CRoxieServerWorkUnitReadActivityFactory : public CRoxieServerActivityFactory
{

public:
    CRoxieServerWorkUnitReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerWorkUnitReadActivity(this, _probeManager);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for WorkUnitRead activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerWorkUnitReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerWorkUnitReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

interface ILocalGraphEx : public IEclGraphResults
{
public:
    virtual void setResult(unsigned id, IGraphResult * result) = 0;
    virtual IRoxieInput * createResultIterator(unsigned id) = 0;
    virtual void setGraphLoopResult(IGraphResult * result) = 0;
    virtual IRoxieInput * createGraphLoopResultIterator(unsigned id) = 0;
};


class CSafeRoxieInput : public CInterface, implements IRoxieInput
{
public:
    CSafeRoxieInput(IRoxieInput * _input) : input(_input) {}
    IMPLEMENT_IINTERFACE

    virtual IOutputMetaData * queryOutputMeta() const
    {
        return input->queryOutputMeta();
    }
    virtual unsigned queryId() const
    {
        return input->queryId();
    }
    virtual unsigned __int64 queryTotalCycles() const
    {
        return input->queryTotalCycles();
    }
    virtual unsigned __int64 queryLocalCycles() const
    {
        return input->queryLocalCycles();
    }
    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return input->queryInput(idx);
    }
    virtual IRoxieServerActivity *queryActivity()
    {
        return input->queryActivity();
    }
    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        return input->queryIndexReadActivity();
    }
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CriticalBlock procedure(cs);
        input->start(parentExtractSize, parentExtract, paused);
    }
    virtual void stop(bool aborting)
    {
        CriticalBlock procedure(cs);
        input->stop(aborting);
    }
    virtual void reset()
    {
        CriticalBlock procedure(cs);
        input->reset();
    }
    virtual void resetEOF()
    {
        CriticalBlock procedure(cs);
        input->resetEOF();
    }
    virtual void checkAbort()
    {
        CriticalBlock procedure(cs);
        input->checkAbort();
    }
    virtual const void *nextInGroup()
    {
        CriticalBlock procedure(cs);
        return input->nextInGroup();
    }
    virtual bool nextGroup(ConstPointerArray & group)
    {
        CriticalBlock procedure(cs);
        return input->nextGroup(group);
    }

private:
    CriticalSection cs;
    Linked<IRoxieInput> input;
};


//=================================================================================

class CPseudoRoxieInput : public CInterface, implements IRoxieInput
{
protected:
    unsigned __int64 totalCycles;
public:
    IMPLEMENT_IINTERFACE;
    
    CPseudoRoxieInput()
    { 
        totalCycles = 0;
    }

    virtual unsigned __int64 queryTotalCycles() const
    {
        return totalCycles;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return totalCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return NULL;
    }

    virtual IRoxieServerActivity *queryActivity()
    {
        throwUnexpected();
    }
    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        throwUnexpected();
    }

    virtual IOutputMetaData * queryOutputMeta() const { throwUnexpected(); }
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) { }
    virtual void stop(bool aborting) { }
    virtual void reset() { totalCycles = 0; }
    virtual void checkAbort() { }
    virtual unsigned queryId() const { throwUnexpected(); }
    virtual void resetEOF() { }
};


class CIndirectRoxieInput : public CPseudoRoxieInput
{
public:
    CIndirectRoxieInput(IRoxieInput * _input = NULL) : input(_input)
    {
    }
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) 
    {
        input->start(parentExtractSize, parentExtract, paused);
    }
    virtual void stop(bool aborting) 
    {
        input->stop(aborting);
    }
    virtual void reset()
    { 
        input->reset();
        totalCycles = 0;
    }
    virtual void checkAbort() 
    {
        input->checkAbort();
    }

    virtual const void * nextInGroup()
    {
        return input->nextInGroup();
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        return input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 ret = totalCycles - input->queryTotalCycles();
        if (ret < 0) 
            ret = 0;
        return ret;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return input->queryInput(idx);
    }

    virtual unsigned queryId() const 
    { 
        return input->queryId();
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector)
    { 
        return input->gatherConjunctions(collector);
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); 
    }

    virtual unsigned numConcreteOutputs() const 
    {
        return input->numConcreteOutputs();
    }
    
    virtual IRoxieInput * queryConcreteInput(unsigned idx) 
    { 
        return input->queryConcreteInput(idx);
    }

    virtual IOutputMetaData * queryOutputMeta() const 
    { 
        return input->queryOutputMeta(); 
    }

    virtual IRoxieServerActivity *queryActivity() 
    {
        return input->queryActivity();
    }
    void setInput(IRoxieInput * _input)
    {
        input = _input;
    }

protected:
    IRoxieInput * input;
};


class CExtractMapperInput : public CIndirectRoxieInput
{
    unsigned savedParentExtractSize;
    const byte * savedParentExtract;
public:
    CExtractMapperInput(IRoxieInput * _input = NULL) : CIndirectRoxieInput(_input)
    {
        savedParentExtractSize = 0;
        savedParentExtract = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        input->start(savedParentExtractSize, savedParentExtract, paused);
    }

    void setParentExtract(unsigned _savedParentExtractSize, const byte * _savedParentExtract)
    {
        savedParentExtractSize = _savedParentExtractSize;
        savedParentExtract = _savedParentExtract;
    }
};


class CGraphResult : public CInterface, implements IGraphResult
{
    CriticalSection cs;
    byte **rowset;
    size32_t count;
    bool complete;

    void clear()
    {
        CriticalBlock func(cs);
        rtlReleaseRowset(count, rowset);
        rowset = NULL;
        count = 0;
        complete = false;
    }

public:
    IMPLEMENT_IINTERFACE
    CGraphResult() 
    { 
        complete = false; // dummy result is not supposed to be used...
        rowset = NULL;
        count = 0;
    }

    CGraphResult(size32_t _count, byte **_rowset)
    : count(_count), rowset(_rowset)
    { 
        complete = true;
    }
    ~CGraphResult()
    {
        clear();
    }

// interface IGraphResult
    virtual IRoxieInput * createIterator()
    {
        if (!complete)
            throw MakeStringException(ROXIE_GRAPH_PROCESSING_ERROR, "Internal Error: Reading uninitialised graph result"); 
        return new CGraphResultIterator(this);
    }

    virtual void getLinkedResult(unsigned & countResult, byte * * & result)
    {
        if (!complete)
            throw MakeStringException(ROXIE_GRAPH_PROCESSING_ERROR, "Internal Error: Reading uninitialised graph result"); 

        result = rtlLinkRowset(rowset);
        countResult = count;
     }

//other 
    const void * getRow(unsigned i)
    {
        CriticalBlock func(cs);
        if (i >= count)
            return NULL;
        const void * ret = rowset[i];
        if (ret) LinkRoxieRow(ret);
        return ret;
    }

protected:
    class CGraphResultIterator : public CPseudoRoxieInput
    {
        unsigned i;
        Linked<CGraphResult> result;

    public:
        CGraphResultIterator(CGraphResult * _result) : result(_result) { i = 0; }
        IMPLEMENT_IINTERFACE


    public:
        virtual const void * nextInGroup()
        {
            return result->getRow(i++);
        }
    };
};


//=================================================================================

class CRoxieServerLocalResultReadActivity : public CRoxieServerActivity
{
    IHThorLocalResultReadArg &helper;
    Owned<IRoxieInput> iter;
    ILocalGraphEx * graph;
    unsigned graphId;
    unsigned sequence;

public:
    CRoxieServerLocalResultReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorLocalResultReadArg &)basehelper), graphId(_graphId)
    {
        graph = NULL;
        sequence = 0;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        graph = static_cast<ILocalGraphEx *>(_ctx->queryCodeContext()->resolveLocalQuery(graphId));
        sequence = helper.querySequence();
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        iter.setown(graph->createResultIterator(sequence));
    }

    virtual void reset() 
    {
        iter.clear();
        CRoxieServerActivity::reset(); 
    };

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = iter->nextInGroup();
        if (next)
        {
            processed++;
            atomic_inc(&rowsIn);
        }
        return next;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

};

class CRoxieServerLocalResultReadActivityFactory : public CRoxieServerActivityFactory
{
     unsigned graphId;

public:
    CRoxieServerLocalResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _graphId)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), graphId(_graphId)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLocalResultReadActivity(this, _probeManager, graphId);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for LocalResultRead activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerLocalResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId)
{
    return new CRoxieServerLocalResultReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, graphId);
}

//=================================================================================

class CRoxieServerLocalResultStreamReadActivity : public CRoxieServerActivity
{
    IHThorLocalResultReadArg &helper;
    Owned<IRoxieInput> streamInput;
    unsigned sequence;

public:
    CRoxieServerLocalResultStreamReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorLocalResultReadArg &)basehelper)
    {
        sequence = 0;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        sequence = helper.querySequence();
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        assertex(streamInput != NULL);
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        streamInput->start(parentExtractSize, parentExtract, paused);
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
        streamInput->stop(aborting);
    }

    virtual void reset() 
    {
        streamInput->reset();
        CRoxieServerActivity::reset(); 
    };

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = streamInput->nextInGroup();
        if (next)
        {
            processed++;
            atomic_inc(&rowsIn);
        }
        return next;
    }

    virtual bool querySetStreamInput(unsigned id, IRoxieInput * _input)
    {
        if (id == sequence)
        {
            streamInput.set(_input);
            return true;
        }
        return false;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

};

class CRoxieServerLocalResultStreamReadActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerLocalResultStreamReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLocalResultStreamReadActivity(this, _probeManager);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for LocalResultRead activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerLocalResultStreamReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerLocalResultStreamReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerLocalResultWriteActivity : public CRoxieServerInternalSinkActivity
{
    IHThorLocalResultWriteArg &helper;
    ILocalGraphEx * graph;
    unsigned graphId;

public:
    CRoxieServerLocalResultWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorLocalResultWriteArg &)basehelper), graphId(_graphId)
    {
        graph = NULL;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerInternalSinkActivity::onCreate(_ctx, _colocalParent);
        graph = static_cast<ILocalGraphEx *>(_ctx->queryCodeContext()->resolveLocalQuery(graphId));
    }

    virtual void onExecute() 
    {
        RtlLinkedDatasetBuilder builder(rowAllocator);
        input->readAll(builder);
        Owned<CGraphResult> result = new CGraphResult(builder.getcount(), builder.linkrows());
        graph->setResult(helper.querySequence(), result);
    }

    IRoxieInput * querySelectOutput(unsigned id)
    {
        if (id == helper.querySequence())
        {
            executed = true;
            return LINK(input);
        }
        return NULL;
    }
};

class CRoxieServerLocalResultWriteActivityFactory : public CRoxieServerInternalSinkFactory
{
    unsigned graphId;
public:
    CRoxieServerLocalResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot), graphId(_graphId)
    {
        isInternal = true;
        Owned<IHThorLocalResultWriteArg> helper = (IHThorLocalResultWriteArg *) helperFactory();
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLocalResultWriteActivity(this, _probeManager, graphId);
    }

};

IRoxieServerActivityFactory *createRoxieServerLocalResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId, bool _isRoot)
{
    return new CRoxieServerLocalResultWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _graphId, _isRoot);
}

//=====================================================================================================

class CRoxieServerDictionaryResultWriteActivity : public CRoxieServerInternalSinkActivity
{
    IHThorDictionaryResultWriteArg &helper;
    ILocalGraphEx * graph;
    unsigned graphId;

public:
    CRoxieServerDictionaryResultWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorDictionaryResultWriteArg &)basehelper), graphId(_graphId)
    {
        graph = NULL;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerInternalSinkActivity::onCreate(_ctx, _colocalParent);
        graph = static_cast<ILocalGraphEx *>(_ctx->queryCodeContext()->resolveLocalQuery(graphId));
    }

    virtual void onExecute()
    {
        unsigned sequence = helper.querySequence();

        RtlLinkedDictionaryBuilder builder(rowAllocator, helper.queryHashLookupInfo());
        loop
        {
            const void *row = input->nextInGroup();
            if (!row)
            {
                row = input->nextInGroup();
                if (!row)
                    break;
            }
            builder.appendOwn(row);
            processed++;
        }
        Owned<CGraphResult> result = new CGraphResult(builder.getcount(), builder.linkrows());
        graph->setResult(helper.querySequence(), result);
    }

    IRoxieInput * querySelectOutput(unsigned id)
    {
        if (id == helper.querySequence())
        {
            executed = true;
            return LINK(input);
        }
        return NULL;
    }
};

class CRoxieServerDictionaryResultWriteActivityFactory : public CRoxieServerInternalSinkFactory
{
    unsigned graphId;
public:
    CRoxieServerDictionaryResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot), graphId(_graphId)
    {
        isInternal = true;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerDictionaryResultWriteActivity(this, _probeManager, graphId);
    }
};

IRoxieServerActivityFactory *createRoxieServerDictionaryResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId, bool _isRoot)
{
    return new CRoxieServerDictionaryResultWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _graphId, _isRoot);
}

//=================================================================================

class CRoxieServerGraphLoopResultReadActivity : public CRoxieServerActivity
{
protected:
    IHThorGraphLoopResultReadArg &helper;
    Owned<IRoxieInput> iter;
    ILocalGraphEx * graph;
    unsigned graphId;
    unsigned sequence;

public:
    CRoxieServerGraphLoopResultReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorGraphLoopResultReadArg &)basehelper), graphId(_graphId)
    {
        graph = NULL;
        sequence = 0;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        graph = static_cast<ILocalGraphEx *>(_ctx->queryCodeContext()->resolveLocalQuery(graphId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (iter)
            iter->start(parentExtractSize, parentExtract, paused);
        else
        {
            sequence = helper.querySequence();
            if ((int)sequence >= 0)
            {
                try
                {
                    iter.setown(graph->createGraphLoopResultIterator(sequence));
                }
                catch (IException * E)
                {
                    throw makeWrappedException(E);
                }
            }
        }
    }

    virtual void stop(bool aborting)
    {
        if (iter)
            iter->stop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset() 
    {
        if (iter)
            iter->reset();
        iter.clear();
        CRoxieServerActivity::reset(); 
    };

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = iter ? iter->nextInGroup() : NULL;
        if (next)
        {
            processed++;
            atomic_inc(&rowsIn);
        }
        return next;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract)
    {
        ensureCreated();
        basehelper.onStart(parentExtract, NULL);
        processor.noteUseIteration(helper.querySequence());
    }

    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes)
    {
        //helper already initialised from the gratherIterationUsage() call.
        iter.set(processor.connectIterationOutput(helper.querySequence(), probeManager, probes, this, 0));
    }
};


//variety of CRoxieServerGraphLoopResultReadActivity created internally with a predefined sequence number
class CRoxieServerInternalGraphLoopResultReadActivity : public CRoxieServerGraphLoopResultReadActivity
{
public:
    CRoxieServerInternalGraphLoopResultReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId, unsigned _sequence)
        : CRoxieServerGraphLoopResultReadActivity(_factory, _probeManager, _graphId)
    {
        sequence = _sequence;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if ((int)sequence >= 0)
        {
            try
            {
                iter.setown(graph->createGraphLoopResultIterator(sequence));
            }
            catch (IException * E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};


class CRoxieServerGraphLoopResultReadActivityFactory : public CRoxieServerActivityFactory
{
     unsigned graphId;

public:
    CRoxieServerGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _graphId)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), graphId(_graphId)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerGraphLoopResultReadActivity(this, _probeManager, graphId);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for GraphLoopResultRead activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId)
{
    return new CRoxieServerGraphLoopResultReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, graphId);
}

//=====================================================================================================

class CRoxieServerGraphLoopResultWriteActivity : public CRoxieServerInternalSinkActivity
{
    IHThorGraphLoopResultWriteArg &helper;
    ILocalGraphEx * graph;
    unsigned graphId;

public:
    CRoxieServerGraphLoopResultWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorGraphLoopResultWriteArg &)basehelper), graphId(_graphId)
    {
        graph = NULL;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerInternalSinkActivity::onCreate(_ctx, _colocalParent);
        graph = static_cast<ILocalGraphEx *>(_ctx->queryCodeContext()->resolveLocalQuery(graphId));
    }

    virtual void onExecute() 
    {
        RtlLinkedDatasetBuilder builder(rowAllocator);
        input->readAll(builder);
        Owned<CGraphResult> result = new CGraphResult(builder.getcount(), builder.linkrows());
        graph->setGraphLoopResult(result);
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==0)
            return this;
        else
            return NULL;
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); 
    }

    virtual const void *nextInGroup()
    {
        const void * next = input->nextInGroup();
        if (next)
            processed++;
        return next;
    }

    virtual bool isPassThrough()
    {
        return true;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (next)
            processed++;
        return next;
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }

    virtual IOutputMetaData * queryOutputMeta() const 
    { 
        return input->queryOutputMeta(); 
    }
};

class CRoxieServerGraphLoopResultWriteActivityFactory : public CRoxieServerInternalSinkFactory
{
    unsigned graphId;
public:
    CRoxieServerGraphLoopResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, true), graphId(_graphId)
    {
        isInternal = true;
        Owned<IHThorGraphLoopResultWriteArg> helper = (IHThorGraphLoopResultWriteArg *) helperFactory();
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerGraphLoopResultWriteActivity(this, _probeManager, graphId);
    }

};

IRoxieServerActivityFactory *createRoxieServerGraphLoopResultWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, unsigned _graphId)
{
    return new CRoxieServerGraphLoopResultWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _graphId);
}

#if 0
//=====================================================================================================

CHThorLocalResultSpillActivity::CHThorLocalResultSpillActivity(IAgentContext &_agent, ActivityId const & _id, IHThorLocalResultSpillArg &_arg)
 : CHThorSimpleActivityBase(_agent, _id, _arg), helper(_arg)
{
    next = NULL;
}

void CHThorLocalResultSpillActivity::ready()
{
    CHThorSimpleActivityBase::ready(); 
    next = input->nextInGroup();
    grouped = input->isGrouped();
    rowdata.clear();
}

const void * CHThorLocalResultSpillActivity::nextInGroup()
{
    const void * ret = next;
    next = input->nextInGroup();
    if (!ret && !next)
        return NULL;

    if (ret)
    {
        size32_t thisSize = outputMeta->getRecordSize(ret);
        rowdata.append(thisSize, ret);
        if (grouped)
            rowdata.append(next == NULL);
    }
    return ret;
}

void CHThorLocalResultSpillActivity::done()
{
    loop
    {
        const void * ret = nextInGroup();
        if (!ret)
        {
            ret = nextInGroup();
            if (!ret)
                break;
        }
        ReleaseRoxieRow(ret);
    }
    agent.setLocalResult(helper.querySequence(), rowdata.length(), rowdata.toByteArray());
    CHThorSimpleActivityBase::done(); 
}
#endif

//=================================================================================

class CRoxieServerDedupActivity : public CRoxieServerActivity
{
    IHThorDedupArg &helper;
    bool keepLeft;
    bool first;
    unsigned numKept;
    unsigned numToKeep;
    const void *kept;

public:

    CRoxieServerDedupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _keepLeft)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorDedupArg &)basehelper)
    {
        keepLeft = _keepLeft;
        kept = NULL;
        numKept = 0;
        first = true;
        numToKeep = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numKept = 0;
        first = true;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numToKeep = helper.numToKeep();
    }

    virtual void reset()
    {
        ReleaseClearRoxieRow(kept);
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (first)
        {
            kept = input->nextInGroup();
            first = false;
        }
        const void * next;
        loop
        {
            next = input->nextInGroup();
            if (!kept || !next || !helper.matches(kept,next))
            {
                numKept = 0;
                break;
            }

            if (numKept < numToKeep-1)
            {
                numKept++;
                break;
            }

            if (keepLeft)
            {
                ReleaseRoxieRow(next);
            }
            else
            {
                ReleaseRoxieRow(kept);
                kept = next;
            }
        }

        const void * ret = kept;
        kept = next;
//      CTXLOG("dedup returns %p", ret);
        if (ret) processed++;
        return ret;
    }

};

class CRoxieServerDedupAllActivity : public CRoxieServerActivity
{
    IHThorDedupArg &helper;
    unsigned     survivorIndex;
    ConstPointerArray survivors;
    bool keepLeft;
    bool eof;
    bool first;
    ICompare *primaryCompare;

public:
    CRoxieServerDedupAllActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _keepLeft)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorDedupArg &)basehelper)
    {
        keepLeft = _keepLeft;
        primaryCompare = helper.queryComparePrimary();
        eof = false;
        first = true;
        survivorIndex = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        first = true;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
#ifdef _DEBUG
        while (survivors.isItem(survivorIndex))
        {
            ReleaseRoxieRow(survivors.item(survivorIndex++));
        }
#endif
        survivors.kill();
        eof = false;
        first = true;
        CRoxieServerActivity::reset();
    }

    void dedupRange(unsigned first, unsigned last, ConstPointerArray & group)
    {
        for (unsigned idxL = first; idxL < last; idxL++)
        {
            const void * left = group.item(idxL);
            if (left)
            {
                for (unsigned idxR = first; idxR < last; idxR++)
                {
                    const void * right = group.item(idxR);
                    if ((idxL != idxR) && right)
                    {
                        if (helper.matches(left, right))
                        {
                            if (keepLeft)
                            {
                                group.replace(NULL, idxR);
                                ReleaseRoxieRow(right);
                            }
                            else
                            {
                                group.replace(NULL, idxL);
                                ReleaseRoxieRow(left);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    void dedupRangeIndirect(unsigned first, unsigned last, void *** index)
    {
        for (unsigned idxL = first; idxL < last; idxL++)
        {
            void * left = *(index[idxL]);
            if (left)
            {
                for (unsigned idxR = first; idxR < last; idxR++)
                {
                    void * right = *(index[idxR]);
                    if ((idxL != idxR) && right)
                    {
                        if (helper.matches(left, right))
                        {
                            if (keepLeft)
                            {
                                *(index[idxR]) = NULL;
                                ReleaseRoxieRow(right);
                            }
                            else
                            {
                                *(index[idxL]) = NULL;
                                ReleaseRoxieRow(left);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    bool calcNextDedupAll()
    {
        survivors.kill();
        survivorIndex = 0;

        ConstPointerArray group;
        if (eof || !input->nextGroup(group))
        {
            eof = true;
            return false;
        }

        unsigned max = group.ordinality();
        if (primaryCompare)
        {
            //hard, if not impossible, to hit this code once optimisations in place
            MemoryAttr indexbuff(max*sizeof(void **));
            void *** index = (void ***)indexbuff.bufferBase();
            qsortvecstable(const_cast<void * *>(group.getArray()), max, *primaryCompare, index);
            unsigned first = 0;
            for (unsigned idx = 1; idx < max; idx++)
            {
                if (primaryCompare->docompare(*(index[first]), *(index[idx])) != 0)
                {
                    dedupRangeIndirect(first, idx, index);
                    first = idx;
                }
            }
            dedupRangeIndirect(first, max, index);

            for(unsigned idx2=0; idx2<max; ++idx2)
            {
                void * cur = *(index[idx2]);
                if(cur)
                    survivors.append(cur);
            }
        }
        else
        {
            dedupRange(0, max, group);
            for(unsigned idx=0; idx<max; ++idx)
            {
                const void * cur = group.item(idx);
                if(cur)
                    survivors.append(cur);
            }
        }

        return true;
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (first)
        {
            calcNextDedupAll();
            first = false;
        }
        while (survivors.isItem(survivorIndex))
        {
            const void *ret = survivors.item(survivorIndex++);
            if (ret)
            {
                processed++;
                return ret;
            }
        }
        calcNextDedupAll();
        return NULL;
    }

};

class CRoxieServerDedupActivityFactory : public CRoxieServerActivityFactory
{
    bool compareAll;
    bool keepLeft;

public:
    CRoxieServerDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorDedupArg> helper = (IHThorDedupArg *) helperFactory();
        compareAll = helper->compareAll();
        keepLeft = helper->keepLeft();
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (compareAll)
            return new CRoxieServerDedupAllActivity(this, _probeManager, keepLeft);
        else
            return new CRoxieServerDedupActivity(this, _probeManager, keepLeft);
    }
};

IRoxieServerActivityFactory *createRoxieServerDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerDedupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerHashDedupActivity : public CRoxieServerActivity
{
    bool eof;
    IHThorHashDedupArg &helper;

    class HashDedupElement
    {
    public:
        HashDedupElement(unsigned _hash, const void *_keyRow)
            : hash(_hash), keyRow(_keyRow)
        {
        }
        ~HashDedupElement()
        {
            ReleaseRoxieRow(keyRow);
        }
        inline unsigned queryHash() const
        {
            return hash;
        }
        inline const void *queryRow() const
        {
            return keyRow;
        }
    private:
        unsigned hash;
        const void *keyRow;
    };

    class HashDedupTable : public SuperHashTable
    {
    public:
        HashDedupTable(IHThorHashDedupArg & _helper, unsigned _activityId) 
            : helper(_helper), 
              activityId(_activityId),
              keySize(helper.queryKeySize())
        {
        }
        virtual ~HashDedupTable() { releaseAll(); }

        virtual unsigned getHashFromElement(const void *et) const       
        {
            const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
            return element->queryHash(); 
        }
        virtual unsigned getHashFromFindParam(const void *fp) const { throwUnexpected(); }
        virtual const void * getFindParam(const void *et) const { throwUnexpected(); }
        virtual bool matchesElement(const void *et, const void *searchET) const { throwUnexpected(); }
        virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const 
        { 
            const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
            if (fphash != element->queryHash())
                return false;
            return (helper.queryKeyCompare()->docompare(element->queryRow(), key) == 0); 
        }
        virtual void onAdd(void *et) {}
        virtual void onRemove(void *et)
        {
            const HashDedupElement *element = reinterpret_cast<const HashDedupElement *>(et);
            delete element;
        }

        void onCreate(IRoxieSlaveContext *ctx)
        {
            keyRowAllocator.setown(ctx->queryCodeContext()->getRowAllocator(keySize.queryOriginal(), activityId));
        }

        void reset()
        {
            kill(); 
        }

        bool insert(const void * row)
        {
            unsigned hash = helper.queryHash()->hash(row);
            RtlDynamicRowBuilder keyRowBuilder(keyRowAllocator, true);
            size32_t thisKeySize = helper.recordToKey(keyRowBuilder, row);
            OwnedConstRoxieRow keyRow = keyRowBuilder.finalizeRowClear(thisKeySize);
            if (find(hash, keyRow.get()))
                return false;
            addNew(new HashDedupElement(hash, keyRow.getClear()), hash);
            return true;
        }

    private:
        IHThorHashDedupArg & helper;
        CachedOutputMetaData keySize;
        Owned<IEngineRowAllocator> keyRowAllocator;
        unsigned activityId;
    } table;

public:
    CRoxieServerHashDedupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorHashDedupArg &)basehelper), table(helper, activityId)
    {
        eof = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void onCreate(IRoxieSlaveContext *ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(ctx, colocalParent);
        table.onCreate(ctx);
    }

    virtual void reset()
    {
        table.reset();
        eof = false;
        CRoxieServerActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        while(!eof)
        {
            const void * next = input->nextInGroup();
            if(!next)
            {
                if (table.count() == 0)
                    eof = true;
                table.reset();
                return NULL;
            }

            if(table.insert(next))
                return next;
            else
                ReleaseRoxieRow(next);
        }
        return NULL;
    }

};

class CRoxieServerHashDedupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerHashDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerHashDedupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerHashDedupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerHashDedupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerRollupActivity : public CRoxieServerActivity
{
    IHThorRollupArg &helper;
    OwnedConstRoxieRow left;
    OwnedConstRoxieRow prev;
    OwnedConstRoxieRow right;
    bool readFirstRow;

public:
    CRoxieServerRollupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorRollupArg &)basehelper)
    {
        readFirstRow = false;
    }

    ~CRoxieServerRollupActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        readFirstRow = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()    
    {
        left.clear();
        prev.clear();
        right.clear();
        CRoxieServerActivity::reset();
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!readFirstRow)
        {
            left.setown(input->nextInGroup());
            prev.set(left);
            readFirstRow = true;
        }

        loop
        {
            right.setown(input->nextInGroup());
            if(!prev || !right || !helper.matches(prev,right))
            {
                const void * ret = left.getClear();
                if(ret)
                    processed++;
                left.setown(right.getClear());
                prev.set(left);
                return ret;
            }
            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                unsigned outSize = helper.transform(rowBuilder, left, right);
                if (outSize)
                    left.setown(rowBuilder.finalizeRowClear(outSize));
                prev.set(right);
            }
            catch(IException * E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerRollupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerRollupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerRollupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerRollupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerRollupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerNormalizeActivity : public CRoxieServerActivity
{
    IHThorNormalizeArg &helper;
    unsigned numThisRow;
    unsigned curRow;
    const void *buffer;
    unsigned numProcessedLastGroup;

public:
    CRoxieServerNormalizeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorNormalizeArg &)basehelper)
    {
        buffer = NULL;
        numThisRow = 0;
        curRow = 0;
        numProcessedLastGroup = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numThisRow = 0;
        curRow = 0;
        numProcessedLastGroup = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        ReleaseClearRoxieRow(buffer);
        CRoxieServerActivity::reset();
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            while (curRow == numThisRow)
            {
                if (buffer)
                    ReleaseClearRoxieRow(buffer);
                buffer = input->nextInGroup();
                if (!buffer && (processed == numProcessedLastGroup))
                    buffer = input->nextInGroup();
                if (!buffer)
                {
                    numProcessedLastGroup = processed;
                    return NULL;
                }

                curRow = 0;
                numThisRow = helper.numExpandedRows(buffer);
            }

            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                unsigned actualSize = helper.transform(rowBuilder, buffer, ++curRow);
                if (actualSize != 0)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(actualSize);
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }

};

class CRoxieServerNormalizeActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNormalizeActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNormalizeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerNormalizeChildActivity : public CRoxieServerActivity
{
    IHThorNormalizeChildArg &helper;
    unsigned numThisRow;
    unsigned curRow;
    const void *buffer;
    unsigned numProcessedLastGroup;
    INormalizeChildIterator * cursor;
    const void * curChildRow;

    bool advanceInput()
    {
        loop
        {
            ReleaseClearRoxieRow(buffer);
            buffer = input->nextInGroup();
            if (!buffer && (processed == numProcessedLastGroup))
                buffer = input->nextInGroup();
            if (!buffer)
            {
                numProcessedLastGroup = processed;
                return false;
            }

            curChildRow = cursor->first(buffer);
            if (curChildRow)
            {
                curRow = 0;
                return true;
            }
        }
    }

public:
    CRoxieServerNormalizeChildActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorNormalizeChildArg &)basehelper)
    {
        buffer = NULL;
        cursor = NULL;
        numThisRow = 0;
        curRow = 0;
        numProcessedLastGroup = 0;
        curChildRow = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numThisRow = 0;
        curRow = 0;
        numProcessedLastGroup = 0;
        curChildRow = NULL;

        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        cursor = helper.queryIterator();
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        cursor = NULL;
        ReleaseClearRoxieRow(buffer);
        CRoxieServerActivity::reset(); 
    }

    virtual bool needsAllocator() const { return true; }

    const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (!buffer)
            {
                if (!advanceInput())
                    return NULL;
            }
            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, buffer, curChildRow, ++curRow);
                curChildRow = cursor->next();
                if (!curChildRow)
                    ReleaseClearRoxieRow(buffer);
                if (outSize != 0)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerNormalizeChildActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerNormalizeChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNormalizeChildActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNormalizeChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNormalizeChildActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerNormalizeLinkedChildActivity : public CRoxieServerActivity
{
    IHThorNormalizeLinkedChildArg &helper;
    OwnedConstRoxieRow curParent;
    OwnedConstRoxieRow curChild;
    unsigned numProcessedLastGroup;

    bool advanceInput()
    {
        loop
        {
            curParent.setown(input->nextInGroup());
            if (!curParent && (processed == numProcessedLastGroup))
                curParent.setown(input->nextInGroup());
            if (!curParent)
            {
                numProcessedLastGroup = processed;
                return false;
            }

            curChild.set(helper.first(curParent));
            if (curChild)
                return true;
        }
    }

public:
    CRoxieServerNormalizeLinkedChildActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorNormalizeLinkedChildArg &)basehelper)
    {
        numProcessedLastGroup = 0;
    }
    ~CRoxieServerNormalizeLinkedChildActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        curParent.clear();
        curChild.clear();
        CRoxieServerActivity::reset(); 
    }

    const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (!curParent)
            {
                if (!advanceInput())
                    return NULL;
            }
            try
            {
                const void *ret = curChild.getClear();
                curChild.set(helper.next());
                if (!curChild)
                    curParent.clear();
                if (ret)
                {
                    processed++;
                    return ret;
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerNormalizeLinkedChildActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerNormalizeLinkedChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNormalizeLinkedChildActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerNormalizeLinkedChildActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNormalizeLinkedChildActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

interface ISortAlgorithm : extends IInterface
{
    virtual void prepare(IRoxieInput *input) = 0;
    virtual const void *next() = 0;
    virtual void reset() = 0;
};

class CQuickSortAlgorithm : implements CInterfaceOf<ISortAlgorithm>
{
    unsigned curIndex;
    ConstPointerArray sorted;
    ICompare *compare;

public:
    CQuickSortAlgorithm(ICompare *_compare) : compare(_compare) 
    {
        curIndex = 0;
    }

    virtual void prepare(IRoxieInput *input)
    {
        curIndex = 0;
        if (input->nextGroup(sorted))
            qsortvec(const_cast<void * *>(sorted.getArray()), sorted.ordinality(), *compare);
    }

    virtual const void *next()
    {
        if (sorted.isItem(curIndex))
            return sorted.item(curIndex++);
        return NULL;
    }

    virtual void reset()
    {
        while (sorted.isItem(curIndex))
            ReleaseRoxieRow(sorted.item(curIndex++));
        curIndex = 0;
        sorted.kill();
    }
};

class CSpillingQuickSortAlgorithm : implements CInterfaceOf<ISortAlgorithm>, implements roxiemem::IBufferedRowCallback
{
    enum {
        InitialSortElements = 0,
        //The number of rows that can be added without entering a critical section, and therefore also the number
        //of rows that might not get freed when memory gets tight.
        CommitStep=32
    };
    roxiemem::DynamicRoxieOutputRowArray rowsToSort;
    roxiemem::RoxieSimpleInputRowArray sorted;
    ICompare *compare;
    IRoxieSlaveContext * ctx;
    Owned<IDiskMerger> diskMerger;
    Owned<IRowStream> diskReader;
    Owned<IOutputMetaData> rowMeta;
    unsigned activityId;

public:
    CSpillingQuickSortAlgorithm(ICompare *_compare, IRoxieSlaveContext * _ctx, IOutputMetaData * _rowMeta, unsigned _activityId)
        : rowsToSort(&_ctx->queryRowManager(), InitialSortElements, CommitStep), ctx(_ctx), compare(_compare), rowMeta(_rowMeta), activityId(_activityId)
    {
        ctx->queryRowManager().addRowBuffer(this);
    }
    ~CSpillingQuickSortAlgorithm()
    {
        ctx->queryRowManager().removeRowBuffer(this);
        diskReader.clear();
    }

    virtual void prepare(IRoxieInput *input)
    {
        loop
        {
            const void * next = input->nextInGroup();
            if (!next)
                break;
            if (!rowsToSort.append(next))
            {
                {
                    roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
                    //We should have been called back to free any committed rows, but occasionally it may not (e.g., if
                    //the problem is global memory is exhausted) - in which case force a spill here (but add any pending
                    //rows first).
                    if (rowsToSort.numCommitted() != 0)
                    {
                        rowsToSort.flush();
                        spillRows();
                    }
                    //Ensure new rows are written to the head of the array.  It needs to be a separate call because
                    //spillRows() cannot shift active row pointer since it can be called from any thread
                    rowsToSort.flush();
                }

                if (!rowsToSort.append(next))
                {
                    ReleaseRoxieRow(next);
                    throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Insufficient memory to append sort row");
                }
            }
        }
        rowsToSort.flush();

        roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
        if (diskMerger)
        {
            spillRows();
            rowsToSort.kill();
            diskReader.setown(diskMerger->merge(compare));
        }
        else
        {
            unsigned numRows = rowsToSort.numCommitted();
            if (numRows)
            {
                const void * * rows = rowsToSort.getBlock(numRows);
                //MORE: Should this be parallel?  Should that be dependent on whether it is grouped?  Should be a hint.
                qsortvec(const_cast<void * *>(rows), numRows, *compare);
            }
            sorted.transferFrom(rowsToSort);
        }
    }

    virtual const void *next()
    {
        if(diskReader)
            return diskReader->nextRow();
        return sorted.dequeue();
    }

    virtual void reset()
    {
        //MORE: This could transfer any row pointer from sorted back to rowsToSort. It would trade
        //fewer heap allocations with not freeing up the memory from large group sorts.
        rowsToSort.clearRows();
        sorted.kill();
        //Disk reader must be cleared before the merger - or the files may still be locked.
        diskReader.clear();
        diskMerger.clear();
    }

//interface roxiemem::IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        //Spill global sorts before grouped sorts
        if (rowMeta->isGrouped())
            return 20;
        return 10;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
        return spillRows();
    }

protected:
    bool spillRows()
    {
        unsigned numRows = rowsToSort.numCommitted();
        if (numRows == 0)
            return false;

        const void * * rows = rowsToSort.getBlock(numRows);
        qsortvec(const_cast<void * *>(rows), numRows, *compare);

        Owned<IRowWriter> out = queryMerger()->createWriteBlock();
        for (unsigned i= 0; i < numRows; i++)
        {
            out->putRow(rows[i]);
        }
        rowsToSort.noteSpilled(numRows);
        return true;
    }

    IDiskMerger * queryMerger()
    {
        if (!diskMerger)
        {
            unsigned __int64 seq = (memsize_t)this ^ get_cycles_now();
            StringBuffer spillBasename;
            spillBasename.append(tempDirectory).append(PATHSEPCHAR).appendf("spill_sort_%"I64F"u", seq);
            Owned<IRowLinkCounter> linker = new RoxieRowLinkCounter();
            Owned<IRowInterfaces> rowInterfaces = createRowInterfaces(rowMeta, activityId, ctx->queryCodeContext());
            diskMerger.setown(createDiskMerger(rowInterfaces, linker, spillBasename));
        }
        return diskMerger;
    }
};

#define INSERTION_SORT_BLOCKSIZE 1024

class SortedBlock : public CInterface, implements IInterface
{
    unsigned sequence;
    const void **rows;
    unsigned length;
    unsigned pos;

    SortedBlock(const SortedBlock &);
public:
    IMPLEMENT_IINTERFACE;

    SortedBlock(unsigned _sequence, IRowManager *rowManager, unsigned activityId) : sequence(_sequence)
    {
        rows = (const void **) rowManager->allocate(INSERTION_SORT_BLOCKSIZE * sizeof(void *), activityId);
        length = 0;
        pos = 0;
    }

    ~SortedBlock()
    {
        while (pos < length)
            ReleaseRoxieRow(rows[pos++]);
        ReleaseRoxieRow(rows);
    }

    int compareTo(SortedBlock *r, ICompare *compare)
    {
        int rc = compare->docompare(rows[pos], r->rows[r->pos]);
        if (!rc)
            rc = sequence - r->sequence;
        return rc;
    }

    const void *next()
    {
        if (pos < length)
            return rows[pos++];
        else
            return NULL;
    }

    inline bool eof()
    {
        return pos==length;
    }

    bool insert(const void *next, ICompare *_compare )
    {
        unsigned b = length;
        if (b == INSERTION_SORT_BLOCKSIZE)
            return false;
        else if (b < 7)
        {
            while (b)
            {
                if (_compare->docompare(next, rows[b-1]) >= 0)
                    break;
                b--;
            }
            if (b != length)
                memmove(&rows[b+1], &rows[b], (length - b) * sizeof(void *));
            rows[b] = next;
            length++;
            return true;
        }
        else
        {
            unsigned int a = 0;
            while ((int)a<b)
            {
                int i = (a+b)/2;
                int rc = _compare->docompare(next, rows[i]);
                if (rc>=0)
                    a = i+1;
                else
                    b = i;
            }
            if (a != length)
                memmove(&rows[a+1], &rows[a], (length - a) * sizeof(void *));
            rows[a] = next;
            length++;
            return true;
        }
    }
};

class CInsertionSortAlgorithm : implements CInterfaceOf<ISortAlgorithm>
{
    SortedBlock *curBlock;
    unsigned blockNo;
    IArrayOf<SortedBlock> blocks;
    unsigned activityId;
    IRowManager *rowManager;
    ICompare *compare;

    void newBlock()
    {
        blocks.append(*curBlock);
        curBlock = new SortedBlock(blockNo++, rowManager, activityId);
    }

    inline static int doCompare(SortedBlock &l, SortedBlock &r, ICompare *compare)
    {
        return l.compareTo(&r, compare);
    }

    void makeHeap()
    {
        /* Permute blocks to establish the heap property
           For each element p, the children are p*2+1 and p*2+2 (provided these are in range)
           The children of p must both be greater than or equal to p
           The parent of a child c is given by p = (c-1)/2
        */
        unsigned i;
        unsigned n = blocks.length();
        SortedBlock **s = blocks.getArray();
        for (i=1; i<n; i++)
        {
            SortedBlock * r = s[i];
            int c = i; /* child */
            while (c > 0) 
            {
                int p = (c-1)/2; /* parent */
                if ( doCompare( blocks.item(c), blocks.item(p), compare ) >= 0 ) 
                    break;
                s[c] = s[p];
                s[p] = r;
                c = p;
            }
        }
    }

    void remakeHeap()
    {
        /* The row associated with block[0] will have changed
           This code restores the heap property
        */
        unsigned p = 0; /* parent */
        unsigned n = blocks.length();
        SortedBlock **s = blocks.getArray();
        while (1) 
        {
            unsigned c = p*2 + 1; /* child */
            if ( c >= n ) 
                break;
            /* Select smaller child */
            if ( c+1 < n && doCompare( blocks.item(c+1), blocks.item(c), compare ) < 0 ) c += 1;
            /* If child is greater or equal than parent then we are done */
            if ( doCompare( blocks.item(c), blocks.item(p), compare ) >= 0 ) 
                break;
            /* Swap parent and child */
            SortedBlock *r = s[c];
            s[c] = s[p];
            s[p] = r;
            /* child becomes parent */
            p = c;
        }
    }

public:
    CInsertionSortAlgorithm(ICompare *_compare, IRowManager *_rowManager, unsigned _activityId) 
        : compare(_compare)
    {
        rowManager = _rowManager;
        activityId = _activityId;
        curBlock = NULL;
        blockNo = 0;
    }

    virtual void reset()
    {
        blocks.kill();
        delete curBlock;
        curBlock = NULL;
        blockNo = 0;
    }

    virtual void prepare(IRoxieInput *input)
    {
        blockNo = 0;
        curBlock = new SortedBlock(blockNo++, rowManager, activityId);
        loop
        {
            const void *next = input->nextInGroup();
            if (!next)
                break;
            if (!curBlock->insert(next, compare))
            {
                newBlock();
                curBlock->insert(next, compare);
            }
        }
        if (blockNo > 1)
        {
            blocks.append(*curBlock);
            curBlock = NULL;
            makeHeap();
        }
    }

    virtual const void * next()
    {
        const void *ret;
        if (blockNo==1) // single block case..
        {
            ret = curBlock->next();
        }
        else if (blocks.length())
        {
            SortedBlock &top = blocks.item(0);
            ret = top.next();
            if (top.eof())
                blocks.replace(blocks.popGet(), 0);
            remakeHeap();
        }
        else
            ret = NULL;
        return ret;
    }
};

class CHeapSortAlgorithm : implements CInterfaceOf<ISortAlgorithm>
{
    unsigned curIndex;
    ConstPointerArray sorted;
    bool inputAlreadySorted;
    IntArray sequences;
    bool eof;
    ICompare *compare;

#ifdef _CHECK_HEAPSORT
    void checkHeap() const
    {
        unsigned n = sorted.ordinality();
        if (n)
        {
            ICompare *_compare = compare;
            void **s = sorted.getArray();
            int *sq = sequences.getArray();
            unsigned p;
#if 0
            CTXLOG("------------------------%d entries-----------------", n);
            for (p = 0; p < n; p++)
            {
                CTXLOG("HEAP %d: %d %.10s", p, sq[p], s[p] ? s[p] : "..");
            }
#endif
            for (p = 0; p < n; p++)
            {
                unsigned c = p*2+1;
                if (c<n)
                    assertex(!s[c] || (docompare(p, c, _compare, s, sq) <= 0));
                c++;
                if (c<n)
                    assertex(!s[c] || (docompare(p, c, _compare, s, sq) <= 0));
            }
        }
    }
#else
    inline void checkHeap() const {}
#endif

    const void *removeHeap()
    {
        unsigned n = sorted.ordinality();
        if (n)
        {
            const void *ret = sorted.item(0);
            if (n > 1 && ret)
            {
                ICompare *_compare = compare;
                const void **s = sorted.getArray();
                int *sq = sequences.getArray();
                unsigned v = 0; // vacancy
                loop
                {
                    unsigned c = 2*v + 1;
                    if (c < n)
                    {
                        unsigned f = c; // favourite to fill it
                        c++;
                        if (c < n && s[c] && (!s[f] || (docompare(f, c, _compare, s, sq) > 0))) // is the smaller of the children
                            f = c;
                        sq[v] = sq[f];
                        if ((s[v] = s[f]) != NULL)
                            v = f;
                        else
                            break;
                    }
                    else
                    {
                        s[v] = NULL;
                        break;
                    }
                }
            }
            checkHeap();
            return ret;
        }
        else
            return NULL;
    }

    static inline int docompare(unsigned l, unsigned r, ICompare *_compare, const void **s, int *sq)
    {
        int rc = _compare->docompare(s[l], s[r]);
        if (!rc)
            rc = sq[l] - sq[r];
        return rc;
    }

    void insertHeap(const void *next)
    {
        // Upside-down heap sort
        // Maintain a heap where every parent is lower than each of its children
        // Root (at node 0) is lowest record seen, nodes 2n+1, 2n+2 are the children
        // To insert a row, add it at end then keep swapping with parent as long as parent is greater
        // To remove a row, take row 0, then recreate heap by replacing it with smaller of two children and so on down the tree
        // Nice features:
        // 1. Deterministic
        // 2. Sort time can be overlapped with upstream/downstream processes - there is no delay between receiving last record from input and deliveriing first to output
        // 3. Already sorted case can be spotted at zero cost while reading. 
        // 4. If you don't read all the results, you don't have to complete the sort
        // BUT it is NOT stable, so we have to use a parallel array of sequence numbers

        unsigned n = sorted.ordinality();
        sorted.append(next);
        sequences.append(n);
        if (!n)
            return;
        ICompare *_compare = compare;
        const void **s = sorted.getArray();
        if (inputAlreadySorted)
        {
            if (_compare->docompare(next, s[n-1]) >= 0)
                return;
            else
            {
                // MORE - could delay creating sequences until now...
                inputAlreadySorted = false;
            }
        }
        int *sq = sequences.getArray();
        unsigned q = n;
        while (n)
        {
            unsigned parent = (n-1) / 2;
            const void *p = s[parent];
            if (_compare->docompare(p, next) <= 0)
                break;
            s[n] = p;
            sq[n] = sq[parent];
            s[parent] = next;
            sq[parent] = q;
            n = parent;
        }
    }

public:
    CHeapSortAlgorithm(ICompare *_compare) : compare(_compare)
    {
        inputAlreadySorted = true;
        curIndex = 0;
        eof = false;
    }

    virtual void reset()
    {
        eof = false;
        if (inputAlreadySorted)
        {
            while (sorted.isItem(curIndex))
                ReleaseRoxieRow(sorted.item(curIndex++));
            sorted.kill();
        }
        else
        {
            ReleaseRoxieRowSet(sorted);
        }
        inputAlreadySorted = true;
        sequences.kill();
    }

    virtual void prepare(IRoxieInput *input)
    {
        inputAlreadySorted = true;
        curIndex = 0;
        eof = false;
        assertex(sorted.ordinality()==0);
        const void *next = input->nextInGroup();
        if (!next)
        {
            eof = true;
            return;
        }
        loop
        {
            insertHeap(next);
            next = input->nextInGroup();
            if (!next)
                break;
        }
        checkHeap();
    }

    virtual const void * next()
    {
        if (inputAlreadySorted)
        {
            if (sorted.isItem(curIndex))
            {
                return sorted.item(curIndex++);
            }
            else
                return NULL;
        }
        else
            return removeHeap();
    }
};

typedef enum {heapSort, insertionSort, quickSort, spillingQuickSort, unknownSort } RoxieSortAlgorithm;

class CRoxieServerSortActivity : public CRoxieServerActivity
{
protected:
    IHThorSortArg &helper;
    ICompare *compare;
    Owned<ISortAlgorithm> sorter;
    bool readInput;
    RoxieSortAlgorithm sortAlgorithm;
    unsigned sortFlags;

public:
    CRoxieServerSortActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, RoxieSortAlgorithm _sortAlgorithm, unsigned _sortFlags)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSortArg &)basehelper), sortAlgorithm(_sortAlgorithm), sortFlags(_sortFlags)
    {
        compare = helper.queryCompare();
        readInput = false;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        switch (sortAlgorithm)
        {
        case heapSort:
            sorter.setown(new CHeapSortAlgorithm(compare));
            break;
        case insertionSort:
            sorter.setown(new CInsertionSortAlgorithm(compare, &ctx->queryRowManager(), activityId));
            break;
        case quickSort:
            sorter.setown(new CQuickSortAlgorithm(compare));
            break;
        case spillingQuickSort:
            sorter.setown(new CSpillingQuickSortAlgorithm(compare, ctx, meta, activityId));
            break;
        case unknownSort:
            sorter.clear(); // create it later....
            break;
        default:
            throwUnexpected();
            break;
        }
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        assertex(!readInput);
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        if (sorter)
            sorter->reset();
        readInput = false;
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!readInput)
        {
            if (sortAlgorithm == unknownSort)
            {
                sorter.clear();
                IHThorAlgorithm *sortMethod = static_cast<IHThorAlgorithm *>(helper.selectInterface(TAIalgorithm_1));
                OwnedRoxieString useAlgorithm(sortMethod->getAlgorithm());
                if (useAlgorithm)
                {
                    if (stricmp(useAlgorithm, "quicksort")==0)
                    {
                        if (sortFlags & TAFstable)
                            throw MakeStringException(ROXIE_UNKNOWN_ALGORITHM, "Invalid stable sort algorithm %s requested", useAlgorithm.get());
                        sorter.setown(new CQuickSortAlgorithm(compare));
                    }
                    else if (stricmp(useAlgorithm, "heapsort")==0)
                        sorter.setown(new CHeapSortAlgorithm(compare));
                    else if (stricmp(useAlgorithm, "insertionsort")==0)
                        sorter.setown(new CInsertionSortAlgorithm(compare, &ctx->queryRowManager(), activityId));
                    else
                    {
                        WARNLOG(ROXIE_UNKNOWN_ALGORITHM, "Ignoring unsupported sort order algorithm '%s', using default", useAlgorithm.get());
                        if (sortFlags & TAFunstable)
                            sorter.setown(new CQuickSortAlgorithm(compare));
                        else
                            sorter.setown(new CHeapSortAlgorithm(compare));
                    }
                }
                else
                    sorter.setown(new CHeapSortAlgorithm(compare)); // shouldn't really happen but there was a vintage of codegen that did not set the flag when algorithm not specified...
            }
            sorter->prepare(input);
            readInput = true;
        }
        const void *ret = sorter->next();
        if (ret)
            processed++;
        else
        {
            sorter->reset();
            readInput = false; // ready for next group
        }
        return ret;
    }
};

class CRoxieServerSortActivityFactory : public CRoxieServerActivityFactory
{
    RoxieSortAlgorithm sortAlgorithm;
    unsigned sortFlags;

public:
    CRoxieServerSortActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        sortAlgorithm = heapSort;
        sortFlags = TAFstable;
        Owned<IHThorSortArg> sortHelper = (IHThorSortArg *) helperFactory();
        IHThorAlgorithm *sortMethod = static_cast<IHThorAlgorithm *>(sortHelper->selectInterface(TAIalgorithm_1));
        if (sortMethod)
        {
            sortFlags = sortMethod->getAlgorithmFlags();
            if (sortFlags & TAFunstable)
                sortAlgorithm = quickSort;
            if (!(sortFlags & TAFconstant))
                sortAlgorithm = unknownSort;
            else
            {
                OwnedRoxieString useAlgorithm(sortMethod->getAlgorithm());
                if (useAlgorithm)
                {
                    if (stricmp(useAlgorithm, "quicksort")==0)
                    {
                        if (sortFlags & TAFstable)
                            throw MakeStringException(ROXIE_UNKNOWN_ALGORITHM, "Invalid stable sort algorithm %s requested", useAlgorithm.get());
                        sortAlgorithm = quickSort;
                    }
                    else if (stricmp(useAlgorithm, "spillingquicksort")==0)
                    {
                        if (sortFlags & TAFstable)
                            throw MakeStringException(ROXIE_UNKNOWN_ALGORITHM, "Invalid stable sort algorithm %s requested", useAlgorithm.get());
                        sortAlgorithm = spillingQuickSort;
                    }
                    else if (stricmp(useAlgorithm, "heapsort")==0)
                        sortAlgorithm = heapSort; // NOTE - we do allow UNSTABLE('heapsort') in order to facilitate runtime selection
                    else if (stricmp(useAlgorithm, "insertionsort")==0)
                        sortAlgorithm = insertionSort;
                    else
                    {
                        WARNLOG(ROXIE_UNKNOWN_ALGORITHM, "Ignoring unsupported sort order algorithm '%s', using default", useAlgorithm.get());
                        if (sortFlags & TAFunstable)
                            sortAlgorithm = quickSort;
                        else
                            sortAlgorithm = heapSort;
                    }
                }
            }
        }
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSortActivity(this, _probeManager, sortAlgorithm, sortFlags);
    }
};

IRoxieServerActivityFactory *createRoxieServerSortActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSortActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerSortedActivity : public CRoxieServerActivity
{
    IHThorSortedArg &helper;
    ICompare * compare;
    const void *prev; 
    IRangeCompare * stepCompare;

public:

    CRoxieServerSortedActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSortedArg &)basehelper)
    {
        prev = NULL;
        compare = helper.queryCompare();
        stepCompare = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        IInputSteppingMeta * stepMeta = input->querySteppingMeta();
        if (stepMeta)
            stepCompare = stepMeta->queryCompare();
        prev = NULL;
    }

    virtual void reset()
    {
        ReleaseClearRoxieRow(prev);
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void *ret = input->nextInGroup();
        if (ret && prev && compare->docompare(prev, ret) > 0)
        {
            // MORE - better to give mismatching rows that indexes?
            throw MakeStringException(ROXIE_NOT_SORTED, "SORTED(%u) detected incorrectly sorted rows  (row %d,  %d))", activityId, processed, processed+1);
        }
        ReleaseRoxieRow(prev);
        prev = ret;
        if (ret)
        {
            LinkRoxieRow(prev);
            processed++;
        }
        return ret;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void *ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret && prev && compare->docompare(prev, ret) > 0)
        {
            // MORE - better to give mismatching rows that indexes?
            throw MakeStringException(ROXIE_NOT_SORTED, "SORTED(%u) detected incorrectly sorted rows  (row %d,  %d))", activityId, processed, processed+1);
        }
        ReleaseRoxieRow(prev);
        prev = ret;
        if (ret)
        {
            LinkRoxieRow(prev);
            processed++;
        }
        return ret;
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); 
    }
};

class CRoxieServerSortedActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSortedActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSortedActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerSortedActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSortedActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerThroughSpillActivity : public CRoxieServerActivity
{

    /*
        BE VERY CAREFUL - this code is tricky.
        Note that starts and stops (and resets) can occur in strange orders
        The FIRST start OR stop must initialize the activity but only the first START should call the upstream start.
        The last stop should call the upstream stop.
        The first reset should call the upstream reset.

        The calculation of whether a row is needed for other (yet to come) outputs needs to work correctly even if the output in question has 
        not yet had start or stop called - for this to happen init() is called on all outputs on the first start or stop.

        Some outputs may be completely pruned away when used in a GRAPH - these outputs should not receive any start/stop/reset and should be
        ignored in the minIndex calculation

    */

public:
    IHThorArg &helper;
    unsigned activeOutputs;
    unsigned numOutputs;
    unsigned numOriginalOutputs;

    QueueOf<const void, true> buffer;
    CriticalSection crit;
    CriticalSection crit2;
    unsigned tailIdx;
    unsigned headIdx;
    Owned<IException> error;

    class OutputAdaptor : public CInterface, implements IRoxieInput
    {
        bool eof, eofpending, stopped;

    public:
        CRoxieServerThroughSpillActivity *parent;
        unsigned idx;
        unsigned oid;
        unsigned processed;
        unsigned __int64 totalCycles;

    public:
        IMPLEMENT_IINTERFACE;

        OutputAdaptor()
        {
            parent = NULL;
            oid = 0;
            idx = 0;
            processed = 0;
            totalCycles = 0;
            eofpending = false;
            eof = false;
            stopped = false;
        }

        ~OutputAdaptor()
        {
            if (traceStartStop)
                DBGLOG("%p ~OutputAdaptor %d", this, oid);
        }

        void init()
        {
            if (traceStartStop)
                DBGLOG("%p init Input adaptor %d", this, oid);
            idx = 0;
            processed = 0;
            totalCycles = 0;
            eofpending = false;
            eof = false;
            stopped = false;
        }

        virtual unsigned queryId() const
        {
            return parent->queryId();
        }

        virtual IRoxieServerActivity *queryActivity()
        {
            return parent;
        }

        virtual IIndexReadActivityInfo *queryIndexReadActivity()
        {
            return parent->queryIndexReadActivity();
        }
        
        virtual unsigned __int64 queryTotalCycles() const
        {
            return totalCycles;
        }

        virtual unsigned __int64 queryLocalCycles() const
        {
            return 0;
        }

        virtual IRoxieInput *queryInput(unsigned idx) const
        {
            return parent->queryInput(idx);
        }

        virtual const void * nextInGroup()
        {
            ActivityTimer t(totalCycles, timeActivities, parent->ctx->queryDebugContext());
            if (eof)
                return NULL;
            const void *ret = parent->readBuffered(idx, oid);
#ifdef TRACE_SPLIT
            parent->CTXLOG("Adaptor %d got back %p for record %d", oid, ret, idx);
#endif
            idx++;
            if (ret)
            {
                processed++;
                eofpending = false;
            }
            else if (eofpending)
                eof = true;
            else
                eofpending = true;
            return ret;
        }

        virtual IOutputMetaData * queryOutputMeta() const
        {
            return parent->queryOutputMeta();
        }

        virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
        {
            // NOTE: it is tempting to move the init() of all output adaptors here. However that is not a good idea, 
            // since adaptors that have not yet started or stoppped (but are going to) still need to have been init()'ed 
            // for minIndex to give the correct answers
            // therefore, we call init() on all adaptors on receipt of the first start() or stop()

            if (traceStartStop)
                parent->CTXLOG("%p start Input adaptor %d stopped = %d", this, oid, stopped);
            parent->start(oid, parentExtractSize, parentExtract, paused);
        }

        virtual void stop(bool aborting) 
        {
            if (traceStartStop)
                parent->CTXLOG("%p stop Input adaptor %d stopped = %d", this, oid, stopped);
            if (!stopped)
            {
                parent->stop(oid, idx, aborting);   // NOTE - may call init()
                stopped = true; // parent code relies on stop being called exactly once per adaptor, so make sure it is!
                idx = (unsigned) -1; // causes minIndex not to save rows for me...
            }
        };

        virtual void reset()
        {
            if (traceStartStop)
                parent->CTXLOG("%p reset Input adaptor %d stopped = %d", this, oid, stopped);
            parent->reset(oid, processed);
            processed = 0;
            idx = 0; // value should not be relevant really but this is the safest...
            stopped = false;
        };

        virtual void resetEOF()
        {
            parent->resetEOF();
        }

        virtual void checkAbort()
        {
            parent->checkAbort();
        }

    } *adaptors;
    bool *used;

    unsigned nextFreeOutput()
    {
        unsigned i = numOutputs;
        while (i)
        {
            i--;
            if (!used[i])
                return i;
        }
        throwUnexpected();
    }

    unsigned minIndex(unsigned exceptOid)
    {
        // MORE - yukky code (and slow). Could keep them heapsorted by idx or something
        // this is trying to determine whethwe any of the adaptors will in the future read a given record
        unsigned minIdx = (unsigned) -1;
        for (unsigned i = 0; i < numOutputs; i++)
        {
            if (i != exceptOid && used[i] && adaptors[i].idx < minIdx)
                minIdx = adaptors[i].idx;
        }
        return minIdx;
    }

    void initOutputs()
    {
        activeOutputs = numOutputs;
        for (unsigned i = 0; i < numOriginalOutputs; i++)
            if (used[i])
                adaptors[i].init();
        state = STATEstarting;
    }

public:
    CRoxieServerThroughSpillActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numOutputs)
        : CRoxieServerActivity(_factory, _probeManager), helper(basehelper), numOutputs(_numOutputs)
    {
        numOriginalOutputs = numOutputs;
        adaptors = new OutputAdaptor[numOutputs];
        used = new bool[numOutputs];
        for (unsigned i = 0; i < numOutputs; i++)
        {
            adaptors[i].parent = this;
            adaptors[i].oid = i;
            used[i] = false;
        }
        tailIdx = 0;
        headIdx = 0;
        activeOutputs = numOutputs;
    }

    ~CRoxieServerThroughSpillActivity()
    {
        delete [] adaptors;
        delete [] used;
    }

    const void *readBuffered(unsigned idx, unsigned oid)
    {
        CriticalBlock b(crit);
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext()); // NOTE - time spent waiting for crit not included here. Is that right?
        if (idx == headIdx) // test once without getting the crit2 sec
        {
            CriticalUnblock b1(crit);
            CriticalBlock b2(crit2);
            if (error)
            {
                throw error.getLink();
            }
            if (idx == headIdx) // test again now that we have it 
            {
                try
                {
                    const void *row = input->nextInGroup();
                    CriticalBlock b3(crit);
                    headIdx++;
                    if (row) processed++;
                    if (activeOutputs==1)
                    {
#ifdef TRACE_SPLIT
                        CTXLOG("spill %d optimised return of %p", activityId, row);
#endif
                        return row;  // optimization for the case where only one output still active.
                    }
                    buffer.enqueue(row);
                }
                catch (IException *E)
                {
#ifdef TRACE_SPLIT
                    CTXLOG("spill %d caught exception", activityId);
#endif
                    error.set(E);
                    throw;
                }
                catch (...)
                {
                    IException *E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught in CRoxieServerThroughSpillActivity::readBuffered");
                    error.set(E);
                    throw E;
                }
            }
        }
        idx -= tailIdx;
        if (!idx)
        {
            unsigned min = minIndex(oid);
            if (min > tailIdx)
            {
                tailIdx++;
                const void *ret = buffer.dequeue(); // no need to link - last puller
#ifdef TRACE_SPLIT
                CTXLOG("last puller return of %p", ret);
#endif
                return ret;
            }
        }
        const void *ret = buffer.item(idx);
        if (ret) LinkRoxieRow(ret);
#ifdef TRACE_SPLIT
        CTXLOG("standard return of %p", ret);
#endif
        return ret;
    }

    virtual void start(unsigned oid, unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CriticalBlock b(crit);
        if (error)
            throw error.getLink();
        if (factory)
            factory->noteStarted(oid);
        if (traceStartStop)
            CTXLOG("SPLIT %p: start %d child %d activeOutputs %d numOutputs %d numOriginalOutputs %d state %s", this, activityId, oid, activeOutputs, numOutputs, numOriginalOutputs, queryStateText(state));
        if (state != STATEstarted)
        {
            if (state != STATEstarting)
                initOutputs();
            tailIdx = 0;
            headIdx = 0;
            error.clear();
            try
            {
                CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
            }
            catch (IException *E)
            {
#ifdef TRACE_SPLIT
                CTXLOG("spill %d caught exception in start", activityId);
#endif
                error.set(E);
                throw;
            }
            catch (...)
            {
                IException *E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught in CRoxieServerThroughSpillActivity::start");
                error.set(E);
                throw E;
            }
        }
    }

    void stop(unsigned oid, unsigned idx, bool aborting)
    {
        // Note that OutputAdaptor code ensures that stop is not called more than once per adaptor
        CriticalBlock b(crit);
#ifdef TRACE_STARTSTOP
        if (traceStartStop)
        {
            CTXLOG("SPLIT %p: stop %d child %d activeOutputs %d numOutputs %d numOriginalOutputs %d state %s", this, activityId, oid, activeOutputs, numOutputs, numOriginalOutputs, queryStateText(state));
            if (watchActivityId && watchActivityId==activityId)
            {
                CTXLOG("WATCH: stop %d", activityId);
            }
        }
#endif
        if (state != STATEstarting && state != STATEstarted)
            initOutputs();
        if (activeOutputs > 1)
        {
            if (tailIdx==idx)
            {
                // Discard all buffered rows that are there purely for this adaptor to read them
                unsigned min = minIndex(oid);
                if (min != (unsigned) -1) 
                // what does -1 signify?? No-one wants anything? In which case can't we kill all rows?? 
                // Should never happen though if there are still some active.
                // there may be a small window where adaptors are blocked on the semaphore...
                {
#ifdef TRACE_SPLIT
                    CTXLOG("%p: Discarding buffered rows from %d to %d for oid %x (%d outputs active)", this, idx, min, oid, activeOutputs);
#endif
                    while (tailIdx < min)
                    {
                        ReleaseRoxieRow(buffer.dequeue());
                        tailIdx++;
                    }
                }
            }
            activeOutputs--;
            return;
        }
#ifdef TRACE_SPLIT
        CTXLOG("%p: All outputs done", this);
#endif
        activeOutputs = numOutputs;
        CRoxieServerActivity::stop(aborting);
    };

    void reset(unsigned oid, unsigned _processed)
    {
        if (traceStartStop)
            CTXLOG("SPLIT %p: reset %d child %d activeOutputs %d numOutputs %d numOriginalOutputs %d state %s", this, activityId, oid, activeOutputs, numOutputs, numOriginalOutputs, queryStateText(state));
        activeOutputs = numOutputs;
        while (buffer.ordinality())
            ReleaseRoxieRow(buffer.dequeue());
        error.clear();
        if (state != STATEreset) // make sure input is only reset once
            CRoxieServerActivity::reset();
    };

    virtual unsigned __int64 queryLocalCycles() const
    {
        return 0;
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // Internal logic error - we are not anybody's input
    }

    virtual IOutputMetaData * queryOutputMeta() const
    {
//      if (outputMeta)
//          return outputMeta;
//      else
            return input->queryOutputMeta(); // not always known (e.g. disk write - though Gavin _could_ fill it in)
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = nextFreeOutput(); // MORE - what is this used for?
        assertex(idx < numOriginalOutputs);
        assertex(!used[idx]);
        used[idx] = true;
        return &adaptors[idx];
    }

    virtual void resetOutputsUsed()
    {
        numOutputs = 1;
        activeOutputs = 1;
        // MORE RKC->GH should we be clearing the used array here? anywhere?
    }

    virtual void noteOutputUsed()
    {
        assertex(numOutputs < numOriginalOutputs);
        numOutputs++;
        activeOutputs = numOutputs;
    }
    virtual bool isPassThrough()
    {
        return numOutputs==1;
    }


};

class CRoxieServerThroughSpillActivityFactory : public CRoxieServerMultiOutputFactory
{
public:
    CRoxieServerThroughSpillActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiOutputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorSpillArg> helper = (IHThorSpillArg *) helperFactory();
        setNumOutputs(helper->getTempUsageCount() + 1);
    }

    CRoxieServerThroughSpillActivityFactory(IQueryFactory &_queryFactory, HelperFactory *_helperFactory, unsigned _numOutputs)
        : CRoxieServerMultiOutputFactory(0, 0, _queryFactory, _helperFactory, TAKsplit)
    {
        setNumOutputs(_numOutputs);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerThroughSpillActivity(this, _probeManager, numOutputs);
    }

};

IRoxieServerActivityFactory *createRoxieServerThroughSpillActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerThroughSpillActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

IRoxieServerActivityFactory *createRoxieServerThroughSpillActivityFactory(IQueryFactory &_queryFactory, HelperFactory *_factory, unsigned _numOutputs)
{
    return new CRoxieServerThroughSpillActivityFactory(_queryFactory, _factory, _numOutputs);
}

//----------------------------------------------------------------------------------------------

class CRoxieServerSplitActivityFactory : public CRoxieServerMultiOutputFactory
{
public:
    CRoxieServerSplitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiOutputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorSplitArg> helper = (IHThorSplitArg *) helperFactory();
        setNumOutputs(helper->numBranches());
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerThroughSpillActivity(this, _probeManager, numOutputs);
    }

};

IRoxieServerActivityFactory *createRoxieServerSplitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSplitActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}


//=====================================================================================================

#define PIPE_BUFSIZE 0x8000

static IException *createPipeFailureException(const char *cmd, unsigned retcode, IPipeProcess *pipe)
{
    StringBuffer msg;
    if(pipe->hasError())
    {
        try
        {
            char error[512];
            size32_t sz = pipe->readError(sizeof(error), error);
            if(sz && sz!=(size32_t)-1)
                msg.append(", stderr: '").append(sz, error).append("'");
        }
        catch (IException *e)
        {
            EXCLOG(e, "Error reading pipe stderr");
            e->Release();
        }
    }
    return MakeStringException(ROXIE_PIPE_ERROR, "Pipe process %s returned error %u%s", cmd, retcode, msg.str());
}

class CRoxieServerPipeReadActivity : public CRoxieServerActivity
{
    IHThorPipeReadArg &helper;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<IReadRowStream> readTransformer;
    bool groupSignalled;
public:
    CRoxieServerPipeReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorPipeReadArg &)basehelper)
    {
        groupSignalled = true;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        rowDeserializer.setown(rowAllocator->createDiskDeserializer(ctx->queryCodeContext()));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        groupSignalled = true; // i.e. don't start with a NULL row
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (!readTransformer)
        {
            OwnedRoxieString xmlIteratorPath(helper.getXmlIteratorPath());
            readTransformer.setown(createReadRowStream(rowAllocator, rowDeserializer, helper.queryXmlTransformer(), helper.queryCsvTransformer(), xmlIteratorPath, helper.getPipeFlags()));
        }
        OwnedRoxieString pipeProgram(helper.getPipeProgram());
        openPipe(pipeProgram);
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
        pipe.clear();
        readTransformer->setStream(NULL);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        while (!waitForPipe())
        {
            if (!pipe)
                return NULL;
            if (helper.getPipeFlags() & TPFgroupeachrow)
            {
                if (!groupSignalled)
                {
                    groupSignalled = true;
                    return NULL;
                }
            }
        }
        const void *ret = readTransformer->next();
        assertex(ret != NULL); // if ret can ever be NULL then we need to recode this logic
        processed++;
        groupSignalled = false;
        return ret;
    }

protected:
    bool waitForPipe()
    {
        if (!pipe)
            return false;  // done
        if (!readTransformer->eos())
            return true;
        verifyPipe();
        return false;
    }

    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess());
        if(!pipe->run(NULL, cmd, ".", false, true, true, 0x10000))
            throw MakeStringException(ROXIE_PIPE_ERROR, "Could not run pipe process %s", cmd);
        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
        readTransformer->setStream(pipeReader.get());
    }

    void verifyPipe()
    {
        if (pipe)
        {
            unsigned err = pipe->wait();
            if(err && !(helper.getPipeFlags() & TPFnofail))
            {
                throw createPipeFailureException(pipeCommand.get(), err, pipe);
            }
            pipe.clear();
        }
    }
};

class CRoxieServerPipeThroughActivity : public CRoxieServerActivity, implements IRecordPullerCallback
{
    IHThorPipeThroughArg &helper;
    RecordPullerThread puller;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    InterruptableSemaphore pipeVerified;
    InterruptableSemaphore pipeOpened;
    CachedOutputMetaData inputMeta;
    Owned<IOutputRowSerializer> rowSerializer;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<IPipeWriteXformHelper> writeTransformer;
    Owned<IReadRowStream> readTransformer;
    bool firstRead;
    bool recreate;
    bool inputExhausted;
    bool groupSignalled;

public:

    CRoxieServerPipeThroughActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorPipeThroughArg &)basehelper), puller(false)
    {
        recreate = helper.recreateEachRow();
        groupSignalled = true;
        firstRead = false;
        inputExhausted = false;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        rowSerializer.setown(inputMeta.createDiskSerializer(ctx->queryCodeContext(), activityId));
        rowDeserializer.setown(rowAllocator->createDiskDeserializer(ctx->queryCodeContext()));
        writeTransformer.setown(createPipeWriteXformHelper(helper.getPipeFlags(), helper.queryXmlOutput(), helper.queryCsvOutput(), rowSerializer));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        firstRead = true;
        inputExhausted = false;
        groupSignalled = true; // i.e. don't start with a NULL row
        pipeVerified.reinit();
        pipeOpened.reinit();
        writeTransformer->ready();
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (!readTransformer)
        {
            OwnedRoxieString xmlIterator(helper.getXmlIteratorPath());
            readTransformer.setown(createReadRowStream(rowAllocator, rowDeserializer, helper.queryXmlTransformer(), helper.queryCsvTransformer(), xmlIterator, helper.getPipeFlags()));
        }
        if(!recreate)
        {
            OwnedRoxieString pipeProgram(helper.getPipeProgram());
            openPipe(pipeProgram);
        }
        puller.start(parentExtractSize, parentExtract, paused, 0, false, ctx);  // Pipe does not support preload presently - locks up
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        puller.setInput(this, _in);
        inputMeta.set(_in->queryOutputMeta());
    }

    virtual void stop(bool aborting)
    {
        pipeVerified.interrupt(NULL);
        pipeOpened.interrupt(NULL);
        puller.stop(aborting);
        CRoxieServerActivity::stop(aborting);
        pipe.clear();
        readTransformer->setStream(NULL);
    }

    virtual void reset()
    {
        puller.reset();
        CRoxieServerActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        while (!waitForPipe())
        {
            if (!pipe)
                return NULL;
            if (helper.getPipeFlags() & TPFgroupeachrow)
            {
                if (!groupSignalled)
                {
                    groupSignalled = true;
                    return NULL;
                }
            }
        }
        const void *ret = readTransformer->next();
        assertex(ret != NULL); // if ret can ever be NULL then we need to recode this logic
        processed++;
        groupSignalled = false;
        return ret;
    }

    virtual void processRow(const void *row)
    {
        // called from puller thread
        if(recreate)
            openPipe(helper.getNameFromRow(row));
        writeTransformer->writeTranslatedText(row, pipe);
        ReleaseRoxieRow(row);
        if(recreate)
        {
            closePipe();
            pipeVerified.wait();
        }
    }

    virtual void processDone()
    {
        // called from puller thread
        if(recreate)
        {
            inputExhausted = true;
            pipeOpened.signal();
        }
        else
        {
            closePipe();
            pipeVerified.wait();
        }
    }

    virtual void processEOG()
    {
    }

    void processGroup(const ConstPointerArray &)
    {
        throwUnexpected();
    }

    virtual bool fireException(IException *e)
    {
        pipeOpened.interrupt(LINK(e));
        pipeVerified.interrupt(e);
        return true;
    }

private:
    bool waitForPipe()
    {
        if (firstRead)
        {
            pipeOpened.wait();
            firstRead = false;
        }
        if (!pipe)
            return false;  // done
        if (!readTransformer->eos())
            return true;
        verifyPipe();
        if (recreate && !inputExhausted)
            pipeOpened.wait();
        return false;
    }

    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess());
        if(!pipe->run(NULL, cmd, ".", true, true, true, 0x10000))
            throw MakeStringException(ROXIE_PIPE_ERROR, "Could not run pipe process %s", cmd);
        writeTransformer->writeHeader(pipe);
        Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
        readTransformer->setStream(pipeReader.get());
        pipeOpened.signal();
    }

    void closePipe()
    {
        writeTransformer->writeFooter(pipe);
        pipe->closeInput();
    }

    void verifyPipe()
    {
        if (pipe)
        {
            unsigned err = pipe->wait();
            if(err && !(helper.getPipeFlags() & TPFnofail))
            {
                throw createPipeFailureException(pipeCommand.get(), err, pipe);
            }
            pipe.clear();
            pipeVerified.signal();
        }
    }

};

class CRoxieServerPipeWriteActivity : public CRoxieServerInternalSinkActivity
{
    IHThorPipeWriteArg &helper;
    Owned<IPipeProcess> pipe;
    StringAttr pipeCommand;
    CachedOutputMetaData inputMeta;
    Owned<IOutputRowSerializer> rowSerializer;
    Owned<IPipeWriteXformHelper> writeTransformer;
    bool firstRead;
    bool recreate;
    bool inputExhausted;
public:
    CRoxieServerPipeWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorPipeWriteArg &)basehelper)
    {
        recreate = helper.recreateEachRow();
        firstRead = false;
        inputExhausted = false;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        inputMeta.set(input->queryOutputMeta());
        rowSerializer.setown(inputMeta.createDiskSerializer(ctx->queryCodeContext(), activityId));
        writeTransformer.setown(createPipeWriteXformHelper(helper.getPipeFlags(), helper.queryXmlOutput(), helper.queryCsvOutput(), rowSerializer));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        firstRead = true;
        inputExhausted = false;
        writeTransformer->ready();
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if(!recreate)
        {
            OwnedRoxieString pipeProgram(helper.getPipeProgram());
            openPipe(pipeProgram);
        }
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
        pipe.clear();
    }

    virtual void onExecute()
    {
        loop
        {
            const void *row = input->nextInGroup();
            if (!row)
            {
                row = input->nextInGroup();
                if (!row)
                    break;
            }
            processed++;
            if(recreate)
                openPipe(helper.getNameFromRow(row));
            writeTransformer->writeTranslatedText(row, pipe);
            ReleaseRoxieRow(row);
            if(recreate)
                closePipe();
        }
        closePipe();
    }

private:
    void openPipe(char const * cmd)
    {
        pipeCommand.setown(cmd);
        pipe.setown(createPipeProcess());
        if(!pipe->run(NULL, cmd, ".", true, false, true, 0x10000))
            throw MakeStringException(ROXIE_PIPE_ERROR, "Could not run pipe process %s", cmd);
        writeTransformer->writeHeader(pipe);
    }

    void closePipe()
    {
        writeTransformer->writeFooter(pipe);
        pipe->closeInput();
        unsigned err = pipe->wait();
        if(err && !(helper.getPipeFlags() & TPFnofail))
        {
            throw createPipeFailureException(pipeCommand.get(), err, pipe);
        }
        pipe.clear();
    }

};

class CRoxieServerPipeReadActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerPipeReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerPipeReadActivity(this, _probeManager);
    }
};

class CRoxieServerPipeThroughActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerPipeThroughActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerPipeThroughActivity(this, _probeManager);
    }
};

class CRoxieServerPipeWriteActivityFactory : public CRoxieServerInternalSinkFactory
{
public:
    CRoxieServerPipeWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
     : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerPipeWriteActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerPipeReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerPipeReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

IRoxieServerActivityFactory *createRoxieServerPipeThroughActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerPipeThroughActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

IRoxieServerActivityFactory *createRoxieServerPipeWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
{
    return new CRoxieServerPipeWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot);
}
//=====================================================================================================

class CRoxieServerStreamedIteratorActivity : public CRoxieServerActivity
{
    IHThorStreamedIteratorArg &helper;
    Owned<IRowStream> rows;

public:
    CRoxieServerStreamedIteratorActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorStreamedIteratorArg &)basehelper)
    {
    }

    ~CRoxieServerStreamedIteratorActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        rows.setown(helper.createInput());
    }

    virtual void stop(bool aborting)
    {
        if (rows)
        {
            rows->stop();
            rows.clear();
        }
        CRoxieServerActivity::stop(aborting);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        assertex(rows != NULL);
        const void * next = rows->nextRow();
        if (next)
            processed++;
        return next;
    }

};

class CRoxieServerStreamedIteratorActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerStreamedIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerStreamedIteratorActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerStreamedIteratorActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerStreamedIteratorActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerFilterActivity : public CRoxieServerLateStartActivity
{
    IHThorFilterArg &helper;
    bool anyThisGroup;
    IRangeCompare * stepCompare;

public:

    CRoxieServerFilterActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerLateStartActivity(_factory, _probeManager), helper((IHThorFilterArg &)basehelper)
    {
        anyThisGroup = false;
        stepCompare = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        anyThisGroup = false;
        CRoxieServerLateStartActivity::start(parentExtractSize, parentExtract, paused);
        lateStart(parentExtractSize, parentExtract, helper.canMatchAny());

        stepCompare = NULL;
        if (!eof)
        {
            IInputSteppingMeta * stepMeta = input->querySteppingMeta();
            if (stepMeta)
                stepCompare = stepMeta->queryCompare();
        }
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        loop
        {
            const void * ret = input->nextInGroup();
            if (!ret)
            {
                //stop returning two NULLs in a row.
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return NULL;
                }
                ret = input->nextInGroup();
                if (!ret)
                {
                    eof = true;
                    return NULL;                // eof...
                }
            }

            if (helper.isValid(ret))
            {
                anyThisGroup = true;
                processed++;
                return ret;
            }
            ReleaseRoxieRow(ret);
        }
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        //Could assert that this isn't grouped
        // MORE - will need rethinking once we rethink the nextSteppedGE interface for global smart-stepping.
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        loop
        {
            const void * ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (!ret)
            {
                eof = true;
                return NULL;
            }

            if (!wasCompleteMatch)
            {
                anyThisGroup = false; // RKC->GH - is this right??
                return ret;
            }

            if (helper.isValid(ret))
            {
                anyThisGroup = true;
                processed++;
                return ret;
            }

            if (!stepExtra.returnMismatches())
            {
                ReleaseRoxieRow(ret);
                return nextInGroup();
            }

            if (stepCompare->docompare(ret, seek, numFields) != 0)
            {
                wasCompleteMatch = false;
                anyThisGroup = false; // WHY?
                return ret;
            }

            ReleaseRoxieRow(ret);
        }
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        eof = prefiltered;
        anyThisGroup = false;
        input->resetEOF(); 
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }

};

class CRoxieServerFilterActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerFilterActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerFilterActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerFilterActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerFilterActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================
class CRoxieServerFilterGroupActivity : public CRoxieServerLateStartActivity
{
    IHThorFilterGroupArg &helper;
    unsigned curIndex;
    ConstPointerArray gathered;
    IRangeCompare * stepCompare;

public:

    CRoxieServerFilterGroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerLateStartActivity(_factory, _probeManager), helper((IHThorFilterGroupArg &)basehelper)
    {
        curIndex = 0;
        stepCompare = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerLateStartActivity::start(parentExtractSize, parentExtract, paused);
        lateStart(parentExtractSize, parentExtract, helper.canMatchAny());//sets eof
        assertex(eof == !helper.canMatchAny());

        curIndex = 0;
        stepCompare = NULL;
        if (!eof)
        {
            IInputSteppingMeta * inputStepping = input->querySteppingMeta();
            if (inputStepping)
                stepCompare = inputStepping->queryCompare();
        }
    }

    virtual void reset()
    {
        releaseGathered();
        CRoxieServerLateStartActivity::reset();
    }

    inline void releaseGathered()
    {
        while (gathered.isItem(curIndex))
            ReleaseRoxieRow(gathered.item(curIndex++));
        gathered.kill();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (eof)
                return NULL;

            if (gathered.ordinality())
            {
                if (gathered.isItem(curIndex))
                {
                    const void * ret = gathered.item(curIndex++);
                    processed++;
                    return ret;
                }
                curIndex = 0;
                gathered.kill();
                return NULL;
            }

            const void * ret = input->nextInGroup();
            while (ret)
            {
                gathered.append(ret);
                ret = input->nextInGroup();
            }

            unsigned num = gathered.ordinality();
            if (num != 0)
            {
                if (!helper.isValid(num, (const void * *)gathered.getArray()))
                    ReleaseRoxieRowSet(gathered);       // read next group
            }
            else
                eof = true;
        }
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        if (gathered.ordinality())
        {
            while (gathered.isItem(curIndex))
            {
                const void * ret = gathered.item(curIndex++);
                if (stepCompare->docompare(ret, seek, numFields) >= 0)
                {
                    processed++;
                    return ret;
                }
                ReleaseRoxieRow(ret);
            }
            curIndex = 0;
            gathered.kill();
            //nextSteppedGE never returns an end of group marker.
        }

        //Not completely sure about this - it could lead the the start of a group being skipped, 
        //so the group filter could potentially work on a different group.  If so, we'd need to check the
        //next fields were a subset of the grouping fields - more an issue for the group activity.
        
        //MORE: What do we do with wasCompleteMatch?  something like the following????
#if 0
        loop
        {
            const void * ret;
            if (stepExtra.returnMismatches())
            {
                bool matchedCompletely = true;
                ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
                if (!wasCompleteMatch)
                    return ret;
            }
            else
                ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
#endif

        const void * ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        while (ret)
        {
            gathered.append(ret);
            ret = input->nextInGroup();
        }

        unsigned num = gathered.ordinality();
        if (num != 0)
        {
            if (!helper.isValid(num, (const void * *)gathered.getArray()))
                ReleaseRoxieRowSet(gathered);       // read next group
        }
        else
            eof = true;

        return nextUngrouped(this);
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        eof = false;
        releaseGathered();
        input->resetEOF(); 
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }

};

class CRoxieServerFilterGroupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerFilterGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerFilterGroupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerFilterGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerFilterGroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerSideEffectActivity : public CRoxieServerActivity
{
    IHThorSideEffectArg &helper;
    CriticalSection ecrit;
    Owned<IException> exception;
    bool executed;
public:

    CRoxieServerSideEffectActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSideEffectArg &)basehelper)
    {
        executed = false;
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        CriticalBlock b(ecrit);
        if (exception)
            throw(exception.getLink());
        if (!executed)
        {
            try
            {
                executed = true;
                helper.action();
            }
            catch(IException *E)
            {
                exception.set(E);
                throw;
            }
        }
        return NULL;
    }

    virtual void execute(unsigned parentExtractSize, const byte * parentExtract) 
    {
        CriticalBlock b(ecrit);
        if (exception)
            throw(exception.getLink());
        if (!executed)
        {
            try
            {
                ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
                executed = true;
                start(parentExtractSize, parentExtract, false);
                helper.action();
                stop(false);
            }
            catch(IException *E)
            {
                ctx->notifyAbort(E);
                stop(true);
                exception.set(E);
                throw;
            }
        }
    }

    virtual void reset()
    {
        executed = false;
        exception.clear();
        CRoxieServerActivity::reset();
    }
};

class CRoxieServerSideEffectActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerSideEffectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSideEffectActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot && !meta.queryOriginal();
    }
};

IRoxieServerActivityFactory *createRoxieServerSideEffectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerSideEffectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerActionActivity : public CRoxieServerInternalSinkActivity
{
    IHThorActionArg &helper;
public:

    CRoxieServerActionActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorActionArg &)basehelper)
    {
    }

    virtual void onExecute() 
    {
        helper.action();
    }
};

class CRoxieServerActionActivityFactory : public CRoxieServerInternalSinkFactory
{
public:
    CRoxieServerActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerActionActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
{
    return new CRoxieServerActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot);
}

//=================================================================================

class CRoxieServerSampleActivity : public CRoxieServerActivity
{
    IHThorSampleArg &helper;
    unsigned numSamples;
    unsigned numToSkip;
    unsigned whichSample;
    bool anyThisGroup;
    bool eof;

public:

    CRoxieServerSampleActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSampleArg &)basehelper)
    {
        numSamples = 0;
        numToSkip = 0;
        whichSample = 0;
        anyThisGroup = false;
        eof = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        anyThisGroup = false;
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numSamples = helper.getProportion();
        whichSample = helper.getSampleNumber();
        numToSkip = (whichSample ? whichSample-1 : 0);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        loop
        {
            const void * ret = input->nextInGroup();
            if (!ret)
            {
                //this does work with groups - may or may not be useful...
                //reset the sample for each group.... probably best.
                numToSkip = (whichSample ? whichSample-1 : 0);
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return NULL;
                }
                ret = input->nextInGroup();
                if (!ret)
                {
                    eof = true;
                    return NULL;                // eof...
                }
            }

            if (numToSkip == 0)
            {
                anyThisGroup = true;
                numToSkip = numSamples-1;
                processed++;
                return ret;
            }
            numToSkip--;
            ReleaseRoxieRow(ret);
        }
    }
};

class CRoxieServerSampleActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSampleActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSampleActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerSampleActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSampleActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChooseSetsActivity : public CRoxieServerActivity
{
    IHThorChooseSetsArg &helper;
    unsigned numSets;
    unsigned * setCounts;
    bool done;

public:

    CRoxieServerChooseSetsActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorChooseSetsArg &)basehelper)
    {
        setCounts = NULL;
        numSets = 0;
        done = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numSets = helper.getNumSets();
        setCounts = new unsigned[numSets];
        memset(setCounts, 0, sizeof(unsigned)*numSets);
        helper.setCounts(setCounts);
    }

    virtual void reset()
    {
        delete [] setCounts;
        setCounts = NULL;
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done)
            return NULL;

        loop
        {
            const void * ret = input->nextInGroup();
            if (!ret)
            {
                ret = input->nextInGroup();
                if (!ret)
                {
                    done = true;
                    return NULL;
                }
            }
            processed++;
            switch (helper.getRecordAction(ret))
            {
            case 2:
                done = true;
                return ret;
            case 1:
                return ret;
            }

            ReleaseRoxieRow(ret);
        }
    }
};

class CRoxieServerChooseSetsActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerChooseSetsActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChooseSetsActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerChooseSetsActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChooseSetsActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerChooseSetsExActivity : public CRoxieServerActivity
{
protected:
    IHThorChooseSetsExArg &helper;
    unsigned numSets;
    unsigned curIndex;
    unsigned * setCounts;
    count_t * limits;
    bool done;
    ConstPointerArray gathered;
    virtual bool includeRow(const void * row) = 0;
    virtual void calculateSelection() = 0;

public:

    CRoxieServerChooseSetsExActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorChooseSetsExArg &)basehelper)
    {
        setCounts = NULL;
        limits = NULL;
        done = false;
        curIndex = 0;
        numSets = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        curIndex = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numSets = helper.getNumSets();
        setCounts = new unsigned[numSets];
        memset(setCounts, 0, sizeof(unsigned)*numSets);
        limits = (count_t *)calloc(sizeof(count_t), numSets);
        helper.getLimits(limits);
    }

    virtual void reset()
    {
        delete [] setCounts;
        setCounts = NULL;
        free(limits);
        limits = NULL;
        while (gathered.isItem(curIndex))
            ReleaseRoxieRow(gathered.item(curIndex++));
        gathered.kill();
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (gathered.ordinality() == 0)
        {
            curIndex = 0;
            if (!input->nextGroup(gathered))
            {
                done = true;
                return NULL;
            }

            ForEachItemIn(idx1, gathered)
            {
                unsigned category = helper.getCategory(gathered.item(idx1));
                if (category)
                    setCounts[category-1]++;
            }
            calculateSelection();
        }

        while (gathered.isItem(curIndex))
        {
            const void * row = gathered.item(curIndex);
            gathered.replace(NULL, curIndex);
            curIndex++;
            if (includeRow(row))
            {
                processed++;
                return row;
            }
            ReleaseRoxieRow(row);
        }

        gathered.kill();
        return NULL;
    }
};

class CRoxieServerChooseSetsLastActivity : public CRoxieServerChooseSetsExActivity 
{
    unsigned * numToSkip;
public:
    CRoxieServerChooseSetsLastActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager) : CRoxieServerChooseSetsExActivity(_factory, _probeManager)
    {
        numToSkip = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerChooseSetsExActivity::start(parentExtractSize, parentExtract, paused);
        numToSkip = (unsigned *)calloc(sizeof(unsigned), numSets);
    }

    virtual void reset() 
    { 
        free(numToSkip); 
        numToSkip = NULL;
        CRoxieServerChooseSetsExActivity::reset();
    }

protected:
    virtual void calculateSelection()
    {
        for (unsigned idx=0; idx < numSets; idx++)
        {
            if (setCounts[idx] < limits[idx])
                numToSkip[idx] = 0;
            else
                numToSkip[idx] = (unsigned)(setCounts[idx] - limits[idx]);
        }
    }

    virtual bool includeRow(const void * row)
    {
        unsigned category = helper.getCategory(row);
        if (category)
        {
            if (numToSkip[category-1] == 0)
                return true;
            numToSkip[category-1]--;
        }
        return false;       
    }
};

class CRoxieServerChooseSetsEnthActivity : public CRoxieServerChooseSetsExActivity 
{
    count_t * counter;
public:
    CRoxieServerChooseSetsEnthActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager) : CRoxieServerChooseSetsExActivity(_factory, _probeManager)
    {
        counter = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerChooseSetsExActivity::start(parentExtractSize, parentExtract, paused);
        counter = (count_t *)calloc(sizeof(count_t), numSets);
    }

    virtual void reset() 
    { 
        free(counter); 
        counter = NULL;
        CRoxieServerChooseSetsExActivity::reset();
    }

protected:
    virtual void calculateSelection()
    {
    }

    virtual bool includeRow(const void * row)
    {
        unsigned category = helper.getCategory(row);
        if (category)
        {
            assertex(category <= numSets);
            counter[category-1] += limits[category-1];
            if(counter[category-1] >= setCounts[category-1])
            {
                counter[category-1] -= setCounts[category-1];
                return true;
            }       
        }
        return false;       
    }
};

class CRoxieServerChooseSetsEnthActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerChooseSetsEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChooseSetsEnthActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerChooseSetsEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChooseSetsEnthActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

class CRoxieServerChooseSetsLastActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerChooseSetsLastActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerChooseSetsLastActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerChooseSetsLastActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerChooseSetsLastActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerEnthActivity : public CRoxieServerActivity
{
    IHThorEnthArg &helper;
    unsigned __int64 numerator;
    unsigned __int64 denominator;
    unsigned __int64 counter;
    bool eof;

public:

    CRoxieServerEnthActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorEnthArg &)basehelper)
    {
        eof = false;
        numerator = denominator = counter = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        numerator = helper.getProportionNumerator();
        denominator = helper.getProportionDenominator();
        if(denominator == 0) denominator = 1; //MORE: simplest way to avoid disaster in this case
        counter = (helper.getSampleNumber()-1) * greatestCommonDivisor(numerator, denominator);
        if (counter >= denominator)
            counter %= denominator;
    }

    inline bool wanted()
    {       
        counter += numerator;
        if(counter >= denominator)
        {
            counter -= denominator;
            return true;
        }       
        return false;   
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        const void * ret;
        loop
        {
            ret = input->nextInGroup();
            if(!ret) //end of group
                ret = input->nextInGroup();
            if(!ret) //eof
            {
                eof = true;
                return ret;
            }
            if (wanted())
                return ret;
            ReleaseRoxieRow(ret);
        }
    }
};

class CRoxieServerEnthActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerEnthActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerEnthActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerEnthActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerAggregateActivity : public CRoxieServerActivity
{
    IHThorAggregateArg &helper;
    bool eof;
    bool isInputGrouped;
    bool abortEarly;

public:
    CRoxieServerAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorAggregateArg &)basehelper)
    {
        eof = false;
        isInputGrouped = false;
        abortEarly = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        isInputGrouped = input->queryOutputMeta()->isGrouped();     // could be done earlier, in setInput?
        abortEarly = !isInputGrouped && (factory->getKind() == TAKexistsaggregate); // ditto
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        const void * next = input->nextInGroup();
        if (!next && isInputGrouped)
        {
            eof = true;
            return NULL;
        }

        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t finalSize = helper.clearAggregate(rowBuilder);

        if (next)
        {
            finalSize = helper.processFirst(rowBuilder, next);
            ReleaseRoxieRow(next);

            if (!abortEarly)
            {
                loop
                {
                    next = input->nextInGroup();
                    if (!next)
                        break;

                    finalSize = helper.processNext(rowBuilder, next);
                    ReleaseRoxieRow(next);
                }
            }
        }

        if (!isInputGrouped)        // either read all, or aborted early
            eof = true;

        processed++;
        return rowBuilder.finalizeRowClear(finalSize);
    }
};

class CRoxieServerAggregateActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerAggregateActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

typedef unsigned t_hashPrefix;

class CRoxieServerHashAggregateActivity : public CRoxieServerActivity
{
    IHThorHashAggregateArg &helper;
    RowAggregator aggregated;

    bool eof;
    bool gathered;
    bool isGroupedAggregate;
public:
    CRoxieServerHashAggregateActivity(const IRoxieServerActivityFactory *_factory, bool _isGroupedAggregate, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorHashAggregateArg &)basehelper),
          isGroupedAggregate(_isGroupedAggregate),
          aggregated(helper, helper)
    {
        eof = false;
        gathered = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        gathered = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    { 
        aggregated.reset();
        CRoxieServerActivity::reset(); 
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        if (!gathered)
        {
            aggregated.start(rowAllocator);
            bool eog = true;
            loop
            {
                const void * next = input->nextInGroup();
                if (!next)
                {
                    if (isGroupedAggregate)
                    {
                        if (eog)
                            eof = true;
                        break;
                    }
                    next = input->nextInGroup();
                    if (!next)
                        break;
                }
                eog = false;
                aggregated.addRow(next);
                ReleaseRoxieRow(next);
            }
            gathered = true;
        }

        Owned<AggregateRowBuilder> next = aggregated.nextResult();
        if (next)
        {
            processed++;
            return next->finalizeRowClear();
        }

        if (!isGroupedAggregate)
            eof = true;

        aggregated.reset();
        gathered = false;
        return NULL;
    }
};

class CRoxieServerHashAggregateActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerHashAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        isGroupedAggregate = _graphNode.getPropBool("att[@name='grouped']/@value");
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerHashAggregateActivity(this, isGroupedAggregate, _probeManager);
    }
protected:
    bool isGroupedAggregate;
};

IRoxieServerActivityFactory *createRoxieServerHashAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode)
{
    return new CRoxieServerHashAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _graphNode);
}

//=================================================================================

class CRoxieServerDegroupActivity : public CRoxieServerActivity
{
    IHThorDegroupArg &helper;
    bool eof;

public:
    CRoxieServerDegroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorDegroupArg &)basehelper)
    {
        eof = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        const void * ret = input->nextInGroup();
        if (!ret)
            ret = input->nextInGroup();
        if (ret)
            processed++;
        else
            eof = true;
        return ret;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        const void * ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret)
            processed++;
        else
            eof = true;
        return ret;
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        eof = false;
        input->resetEOF(); 
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }

};

class CRoxieServerDegroupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerDegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerDegroupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerDegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerDegroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerSpillReadActivity : public CRoxieServerActivity
{
    IHThorDiskReadArg &helper;
    bool needTransform;
    bool eof;
    bool anyThisGroup;
    unsigned __int64 rowLimit;
    unsigned __int64 choosenLimit;

public:
    CRoxieServerSpillReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorDiskReadArg &)basehelper)
    {
        needTransform = helper.needTransform();
        rowLimit = (unsigned __int64) -1;
        choosenLimit = 0;
        eof = false;
        anyThisGroup = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        anyThisGroup = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (helper.canMatchAny())
            eof = false;
        else
            eof = true;
        choosenLimit = helper.getChooseNLimit();
        rowLimit = helper.getRowLimit();
        helper.setCallback(NULL);       // members should not be called - change if they are
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        if (processed==choosenLimit)
        {
            eof = true;
            return NULL;
        }
        if (needTransform)
        {
            loop
            {
                const void *in = input->nextInGroup();
                if (!in)
                {
                    if (anyThisGroup)
                    {
                        anyThisGroup = false;
                        return NULL;
                    }
                    in = input->nextInGroup();
                    if (!in)
                    {
                        eof = true;
                        return NULL;                // eof...
                    }
                }
                unsigned outSize;
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                try
                {
                    outSize = helper.transform(rowBuilder, in);
                    ReleaseRoxieRow(in);
                }
                catch (IException *E)
                {
                    throw makeWrappedException(E);
                }

                if (outSize)
                {
                    anyThisGroup = true;
                    processed++;
                    if (processed==rowLimit)
                    {
                        if (traceLevel > 4)
                            DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                        helper.onLimitExceeded();
                    }
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
        }
        else
        {
            const void *ret = input->nextInGroup();
            if (ret)
            {
                processed++;
                if (processed==rowLimit)
                {
                    if (traceLevel > 4)
                        DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                    ReleaseClearRoxieRow(ret);
                    helper.onLimitExceeded(); // should not return
                    throwUnexpected();
                }
            }
            return ret;
        }
    }
};

class CRoxieServerSpillReadActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSpillReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSpillReadActivity(this, _probeManager);
    }

    virtual void addDependency(unsigned source, ThorActivityKind sourceKind, unsigned sourceIdx, int controlId, const char *edgeId)
    {
        if (sourceKind==TAKspill || sourceKind==TAKdiskwrite) // Bit of a hack - codegen probably should differentiate
            setInput(0, source, sourceIdx);
        else
            CRoxieServerActivityFactory::addDependency(source, kind, sourceIdx, controlId, edgeId);
    } 
};

IRoxieServerActivityFactory *createRoxieServerSpillReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSpillReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerSpillWriteActivity : public CRoxieServerActivity
{
public:
    CRoxieServerSpillWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
    }

    ~CRoxieServerSpillWriteActivity()
    {
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        return input->nextInGroup();
    }
};

//==================================================================================

class CRoxieServerDiskWriteActivity : public CRoxieServerInternalSinkActivity, implements IRoxiePublishCallback
{
protected:
    Owned<IExtRowWriter> outSeq;
    Owned<IOutputRowSerializer> rowSerializer;
    Linked<IFileIOStream> diskout;
    bool blockcompressed;
    bool extend;
    bool overwrite;
    bool encrypted;
    bool grouped;
    IHThorDiskWriteArg &helper;
    StringBuffer lfn;   // logical filename
    CachedOutputMetaData diskmeta;
    Owned<IRoxieWriteHandler> writer;

    bool tallycrc;
    unsigned __int64 uncompressedBytesWritten;
    CRC32 crc;

    void updateWorkUnitResult(unsigned __int64 reccount)
    {
        assertex(writer);
        // MORE - a lot of this is common with hthor
        if(lfn.length()) //this is required as long as temp files don't get a name which can be stored in the WU and automatically deleted by the WU
        {
            WorkunitUpdate wu = ctx->updateWorkUnit();
            if (wu)
            {
                unsigned flags = helper.getFlags();
                WUFileKind fileKind;
                if (TDXtemporary & flags)
                    fileKind = WUFileTemporary;
                else if(TDXjobtemp & flags)
                    fileKind = WUFileJobOwned;
                else if(TDWowned & flags)
                    fileKind = WUFileOwned;
                else
                    fileKind = WUFileStandard;
                StringArray clusters;
                writer->getClusters(clusters);
                wu->addFile(lfn.str(), &clusters, helper.getTempUsageCount(), fileKind, NULL);
                if (!(flags & TDXtemporary) && helper.getSequence() >= 0)
                {
                    Owned<IWUResult> result = wu->updateResultBySequence(helper.getSequence());
                    if (result)
                    {
                        result->setResultTotalRowCount(reccount); 
                        result->setResultStatus(ResultStatusCalculated);
                        if (helper.getFlags() & TDWresult)
                            result->setResultFilename(lfn.str());
                        else
                            result->setResultLogicalName(lfn.str());
                    }
                }
            }
        }
    }

    void resolve()
    {
        OwnedRoxieString rawLogicalName = helper.getFileName();
        assertex(rawLogicalName);
        assertex((helper.getFlags() & TDXtemporary) == 0);
        StringArray clusters;
        unsigned clusterIdx = 0;
        while(true)
        {
            OwnedRoxieString cluster(helper.getCluster(clusterIdx));
            if(!cluster)
                break;
            clusters.append(cluster);
            clusterIdx++;
        }
        if (clusters.length())
        {
            if (extend)
                throw MakeStringException(0, "Cannot combine EXTEND and CLUSTER flags on disk write of file %s", rawLogicalName.get());
        }
        else
        {
            if (roxieName.length())
                clusters.append(roxieName.str());
            else
                clusters.append(".");
        }
        writer.setown(ctx->createLFN(rawLogicalName, overwrite, extend, clusters)); // MORE - if there's a workunit, use if for scope.
        // MORE - need to check somewhere that single part if it's an existing file or an external one...
    }

public:
    CRoxieServerDiskWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorDiskWriteArg &)basehelper)
    {
        extend = ((helper.getFlags() & TDWextend) != 0);
        overwrite = ((helper.getFlags() & TDWoverwrite) != 0);
        grouped = false; // don't think we need to support it...
        diskmeta.set(helper.queryDiskRecordSize());
        blockcompressed = (((helper.getFlags() & TDWnewcompress) != 0) || (((helper.getFlags() & TDXcompress) != 0) && (diskmeta.getFixedSize() >= MIN_ROWCOMPRESS_RECSIZE))); //always use new compression
        encrypted = false; // set later
        tallycrc = true;
        uncompressedBytesWritten = 0;
    }

    ~CRoxieServerDiskWriteActivity()
    {
    }

    virtual bool needsAllocator() const
    {
        return true;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        
        resolve();
        Owned<IFileIO> io;
        void *ekey;
        size32_t ekeylen;
        helper.getEncryptKey(ekeylen, ekey);
        Owned<ICompressor> ecomp;
        if (ekeylen!=0) 
        {
            ecomp.setown(createAESCompressor256(ekeylen,ekey));
            memset(ekey,0,ekeylen);
            rtlFree(ekey);
            encrypted = true;
            blockcompressed = true;
        }
        if (blockcompressed)
            io.setown(createCompressedFileWriter(writer->queryFile(), (diskmeta.isFixedSize() ? diskmeta.getFixedSize() : 0), extend, true, ecomp));
        else
            io.setown(writer->queryFile()->open(extend ? IFOwrite : IFOcreate));
        if (!io)
            throw MakeStringException(errno, "Failed to create%s file %s for writing", (encrypted ? " encrypted" : (blockcompressed ? " compressed" : "")), writer->queryFile()->queryFilename());
        diskout.setown(createBufferedIOStream(io));
        if (extend)
            diskout->seek(0, IFSend);
        tallycrc = !factory->queryQueryFactory().getDebugValueBool("skipFileFormatCrcCheck", false) && !(helper.getFlags() & TDRnocrccheck) && !blockcompressed;
        Owned<IRowInterfaces> rowIf = createRowInterfaces(input->queryOutputMeta(), activityId, ctx->queryCodeContext());
        rowSerializer.set(rowIf->queryRowSerializer());
        unsigned rwFlags = rw_autoflush;
        if(grouped)
            rwFlags |= rw_grouped;
        if(tallycrc)
            rwFlags |= rw_crc;
        if(!factory->queryQueryFactory().getDebugValueBool("skipFileFormatCrcCheck", false) && !(helper.getFlags() & TDRnocrccheck))
            rwFlags |= rw_crc;
        outSeq.setown(createRowWriter(diskout, rowIf, rwFlags));
    }

    virtual void stop(bool aborting)
    {
        if (aborting)
        {
            if (writer)
                writer->finish(false, this);
        }
        else
        {
            outSeq->flush(&crc);
            updateWorkUnitResult(processed);
            uncompressedBytesWritten = outSeq->getPosition();
            writer->finish(true, this);
        }
        writer.clear();
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        diskout.clear();
        outSeq.clear();
        writer.clear();
        uncompressedBytesWritten = 0;
        crc.reset();
    }

    virtual void onExecute()
    {
        loop
        {
            const void *nextrec = input->nextInGroup();
            if (!nextrec)
            {
                nextrec = input->nextInGroup();
                if (!nextrec)
                    break;
            }
            processed++;
            outSeq->putRow(nextrec);
        }
    }

    virtual void setFileProperties(IFileDescriptor *desc) const
    {
        IPropertyTree &partProps = desc->queryPart(0)->queryProperties(); //properties of the first file part.
        IPropertyTree &fileProps = desc->queryProperties(); // properties of the logical file
        if (blockcompressed)
        {
            // caller has already set @size from file size...
            fileProps.setPropBool("@blockCompressed", true);
            partProps.setPropInt64("@compressedSize", partProps.getPropInt64("@size", 0));  // MORE should this be on logical too?
            fileProps.setPropInt64("@size", uncompressedBytesWritten);
            partProps.setPropInt64("@size", uncompressedBytesWritten);
        }
        else if (tallycrc)
            partProps.setPropInt64("@fileCrc", crc.get());

        if (encrypted)
            fileProps.setPropBool("@encrypted", true);

        fileProps.setPropInt64("@recordCount", processed);
        unsigned flags = helper.getFlags();
        if (flags & TDWpersist)
            fileProps.setPropBool("@persistent", true);
        if (grouped)
            fileProps.setPropBool("@grouped", true);
        if (flags & (TDWowned|TDXjobtemp|TDXtemporary))
            fileProps.setPropBool("@owned", true);
        if (flags & TDWresult)
            fileProps.setPropBool("@result", true);

        IConstWorkUnit *workUnit = ctx->queryWorkUnit();
        if (workUnit)
        {
            SCMStringBuffer owner, wuid, job;
            fileProps.setProp("@owner", workUnit->getUser(owner).str());
            fileProps.setProp("@workunit", workUnit->getWuid(wuid).str());
            fileProps.setProp("@job", workUnit->getJobName(job).str());
        }
        setExpiryTime(fileProps, helper.getExpiryDays());
        if (flags & TDWupdate)
        {
            unsigned eclCRC;
            unsigned __int64 totalCRC;
            helper.getUpdateCRCs(eclCRC, totalCRC);
            fileProps.setPropInt("@eclCRC", eclCRC);
            fileProps.setPropInt64("@totalCRC", totalCRC);
        }
        fileProps.setPropInt("@formatCrc", helper.getFormatCrc());

        IRecordSize * inputMeta = input->queryOutputMeta();
        if ((inputMeta->isFixedSize()) && !isOutputTransformed())
            fileProps.setPropInt("@recordSize", inputMeta->getFixedSize() + (grouped ? 1 : 0));

        const char *recordECL = helper.queryRecordECL();
        if (recordECL && *recordECL)
            fileProps.setProp("ECL", recordECL);
    }

    virtual IUserDescriptor *queryUserDescriptor() const
    {
        IConstWorkUnit *workUnit = ctx->queryWorkUnit();
        if (workUnit)
            return workUnit->queryUserDescriptor();
        else
            return NULL;
    }

    virtual bool isOutputTransformed() const { return false; }

};

//=================================================================================

class CRoxieServerCsvWriteActivity : public CRoxieServerDiskWriteActivity
{
    IHThorCsvWriteArg &csvHelper;
    CSVOutputStream csvOutput;

public:
    CRoxieServerCsvWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerDiskWriteActivity(_factory, _probeManager), csvHelper(static_cast<IHThorCsvWriteArg &>(helper))
    {
        csvOutput.init(csvHelper.queryCsvParameters(), false);
    }

    virtual void onExecute() 
    {
        OwnedRoxieString header(csvHelper.queryCsvParameters()->getHeader());
        if (header) 
        {
            csvOutput.beginLine();
            csvOutput.writeHeaderLn(strlen(header), header);
            diskout->write(csvOutput.length(), csvOutput.str());
        }
        loop
        {
            const void *nextrec = input->nextInGroup();
            if (!nextrec)
            {
                nextrec = input->nextInGroup();
                if (!nextrec)
                    break;
            }
            processed++;
            csvOutput.beginLine();
            csvHelper.writeRow((const byte *)nextrec, &csvOutput);
            csvOutput.endLine();
            diskout->write(csvOutput.length(), csvOutput.str());
            ReleaseRoxieRow(nextrec);
        }
        OwnedRoxieString footer(csvHelper.queryCsvParameters()->getFooter());
        if (footer) 
        {
            csvOutput.beginLine();
            csvOutput.writeHeaderLn(strlen(footer), footer);
            diskout->write(csvOutput.length(), csvOutput.str());
        }
    }

    virtual void setFileProperties(IFileDescriptor *desc) const
    {
        CRoxieServerDiskWriteActivity::setFileProperties(desc);
        IPropertyTree &props = desc->queryProperties();
        props.setProp("@format","utf8n");

        ICsvParameters *csvParameters = csvHelper.queryCsvParameters();
        StringBuffer separator;
        OwnedRoxieString rs(csvParameters->getSeparator(0));
        const char *s = rs;
        while (s &&  *s)
        {
            if (',' == *s)
                separator.append("\\,");
            else
                separator.append(*s);
            ++s;
        }
        props.setProp("@csvSeparate", separator.str());
        props.setProp("@csvQuote", rs.setown(csvParameters->getQuote(0)));
        props.setProp("@csvTerminate", rs.setown(csvParameters->getTerminator(0)));
        props.setProp("@csvEscape", rs.setown(csvParameters->getEscape(0)));
    }

    virtual bool isOutputTransformed() const { return true; }
};


class CRoxieServerXmlWriteActivity : public CRoxieServerDiskWriteActivity
{
    IHThorXmlWriteArg &xmlHelper;
    StringAttr rowTag;

public:
    CRoxieServerXmlWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerDiskWriteActivity(_factory, _probeManager), xmlHelper(static_cast<IHThorXmlWriteArg &>(helper))
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerDiskWriteActivity::start(parentExtractSize, parentExtract, paused);
        OwnedRoxieString xmlpath(xmlHelper.getXmlIteratorPath());
        if (!xmlpath)
            rowTag.set("Row");
        else
        {
            const char *path = xmlpath;
            if (*path == '/') path++;
            if (strchr(path, '/')) UNIMPLEMENTED;               // more what do we do with /mydata/row
            rowTag.set(path);
        }
    }

    virtual void onExecute() 
    {
        OwnedRoxieString suppliedHeader(xmlHelper.getHeader());
        const char *header = suppliedHeader;
        if (!header) header = "<Dataset>\n";
        diskout->write(strlen(header), header);
        CommonXmlWriter xmlOutput(xmlHelper.getXmlFlags());
        loop
        {
            OwnedConstRoxieRow nextrec = input->nextInGroup();
            if (!nextrec)
            {
                nextrec.setown(input->nextInGroup());
                if (!nextrec)
                    break;
            }
            processed++;
            xmlOutput.clear().outputBeginNested(rowTag, false);
            xmlHelper.toXML((const byte *)nextrec.get(), xmlOutput);
            xmlOutput.outputEndNested(rowTag);
            diskout->write(xmlOutput.length(), xmlOutput.str());
        }
        OwnedRoxieString suppliedFooter(xmlHelper.getFooter());
        const char * footer = suppliedFooter;
        if (!footer) footer = "</Dataset>\n";
        diskout->write(strlen(footer), footer);
    }

    virtual void reset()
    {
        CRoxieServerDiskWriteActivity::reset();
        rowTag.clear();
    }

    virtual void setFileProperties(IFileDescriptor *desc) const
    {
        CRoxieServerDiskWriteActivity::setFileProperties(desc);
        desc->queryProperties().setProp("@format","utf8n");
        desc->queryProperties().setProp("@rowTag",rowTag.get());
    }

    virtual bool isOutputTransformed() const { return true; }
};

class CRoxieServerDiskWriteActivityFactory : public CRoxieServerMultiOutputFactory
{
    bool isRoot;
public:
    CRoxieServerDiskWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerMultiOutputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
        Owned<IHThorDiskWriteArg> helper = (IHThorDiskWriteArg *) helperFactory();
        setNumOutputs(helper->getTempUsageCount());
        if (_kind!=TAKdiskwrite)
            assertex(numOutputs == 0);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        switch (numOutputs)
        {
        case 0:
            switch (kind)
            {
            case TAKdiskwrite: return new CRoxieServerDiskWriteActivity(this, _probeManager);
            case TAKcsvwrite: return new CRoxieServerCsvWriteActivity(this, _probeManager);
            case TAKxmlwrite: return new CRoxieServerXmlWriteActivity(this, _probeManager);
            };
            throwUnexpected();
        case 1:
            return new CRoxieServerSpillWriteActivity(this, _probeManager);
        default:
            return new CRoxieServerThroughSpillActivity(this, _probeManager, numOutputs);
        }
    }

    virtual bool isSink() const
    {
        return numOutputs == 0; // MORE - check with Gavin if this is right if not a temp but reread in  same job...
    }

};

IRoxieServerActivityFactory *createRoxieServerDiskWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerDiskWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerIndexWriteActivity : public CRoxieServerInternalSinkActivity, implements IRoxiePublishCallback
{
    IHThorIndexWriteArg &helper;
    bool overwrite;
    Owned<ClusterWriteHandler> clusterHandler;
    Owned<IRoxieWriteHandler> writer;
    unsigned __int64 reccount;
    unsigned int fileCrc;
    StringBuffer filename;

    void updateWorkUnitResult()
    {
        if(filename.length()) //this is required as long as temp files don't get a name which can be stored in the WU and automatically deleted by the WU
        {
            WorkunitUpdate wu = ctx->updateWorkUnit();
            if (wu)
            {
                if (!(helper.getFlags() & TDXtemporary) && helper.getSequence() >= 0)
                {
                    Owned<IWUResult> result = wu->updateResultBySequence(helper.getSequence());
                    if (result)
                    {
                        result->setResultTotalRowCount(reccount);
                        result->setResultStatus(ResultStatusCalculated);
                        result->setResultLogicalName(filename.str());
                    }
                }
                if(clusterHandler)
                    clusterHandler->finish(writer->queryFile());
            }
            CTXLOG("Created roxie index file %s", filename.str());
        }
    }

    virtual void resolve()
    {
        StringArray clusters;
        unsigned clusterIdx = 0;
        while(true)
        {
            OwnedRoxieString cluster(helper.getCluster(clusterIdx));
            if(!cluster)
                break;
            clusters.append(cluster);
            clusterIdx++;
        }

        if (roxieName.length())
            clusters.append(roxieName.str());
        else
            clusters.append(".");
        OwnedRoxieString fname(helper.getFileName());
        writer.setown(ctx->createLFN(fname, overwrite, false, clusters)); // MORE - if there's a workunit, use if for scope.
        filename.set(writer->queryFile()->queryFilename());
        if (writer->queryFile()->exists())
        {
            if (overwrite)
            {
                CTXLOG("Removing existing %s from DFS",filename.str());
                writer->queryFile()->remove();
            }
            else
                throw MakeStringException(99, "Cannot write index file %s, file already exists (missing OVERWRITE attribute?)", filename.str());
        }
    }

    void buildUserMetadata(Owned<IPropertyTree> & metadata)
    {
        size32_t nameLen;
        char * nameBuff;
        size32_t valueLen;
        char * valueBuff;
        unsigned idx = 0;
        while(helper.getIndexMeta(nameLen, nameBuff, valueLen, valueBuff, idx++))
        {
            StringBuffer name(nameLen, nameBuff);
            StringBuffer value(valueLen, valueBuff);
            if(*nameBuff == '_' && strcmp(name, "_nodeSize") != 0)
            {
                OwnedRoxieString fname(helper.getFileName());
                throw MakeStringException(0, "Invalid name %s in user metadata for index %s (names beginning with underscore are reserved)", name.str(), fname.get());
            }
            if(!validateXMLTag(name.str()))
            {
                OwnedRoxieString fname(helper.getFileName());
                throw MakeStringException(0, "Invalid name %s in user metadata for index %s (not legal XML element name)", name.str(), fname.get());
            }
            if(!metadata)
                metadata.setown(createPTree("metadata"));
            metadata->setProp(name.str(), value.str());
        }
    }

    void buildLayoutMetadata(Owned<IPropertyTree> & metadata)
    {
        if(!metadata)
            metadata.setown(createPTree("metadata"));
        metadata->setProp("_record_ECL", helper.queryRecordECL());

        void * layoutMetaBuff;
        size32_t layoutMetaSize;
        if(helper.getIndexLayout(layoutMetaSize, layoutMetaBuff))
        {
            metadata->setPropBin("_record_layout", layoutMetaSize, layoutMetaBuff);
            rtlFree(layoutMetaBuff);
        }
    }

public:
    CRoxieServerIndexWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper(static_cast<IHThorIndexWriteArg &>(basehelper))
    {
        overwrite = ((helper.getFlags() & TIWoverwrite) != 0);
        reccount = 0;
    }

    ~CRoxieServerIndexWriteActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        resolve();
    }

    virtual void onExecute()
    {
        bool isVariable = helper.queryDiskRecordSize()->isVariableSize();
        size32_t maxDiskRecordSize;
        if (isVariable)
            maxDiskRecordSize = 0x8000;
        else
        {
            maxDiskRecordSize = helper.queryDiskRecordSize()->getFixedSize();
            if (maxDiskRecordSize > 0x8000)
                throw MakeStringException(99, "Index minimum record length (%d) exceeds 32k internal limit", maxDiskRecordSize);

        }
        OwnedMalloc<char> rowBuffer(maxDiskRecordSize, true);

        unsigned __int64 fileSize = 0;
        fileCrc = -1;
        OwnedRoxieString dsName(helper.getFileName());
        if (dsName.get())
        {
            Owned<const IResolvedFile> dsFileInfo = resolveLFN(dsName, false);
            if (dsFileInfo)
            {
                fileSize = dsFileInfo->getFileSize();
            }
        }

        {
            Owned<IFileIO> io;
            try
            {
                io.setown(writer->queryFile()->open(IFOcreate));
            }
            catch(IException * e)
            {
                e->Release();
                clearKeyStoreCache(false);
                io.setown(writer->queryFile()->open(IFOcreate));
            }
            if(!io)
                throw MakeStringException(errno, "Failed to create file %s for writing", filename.str());

            Owned<IFileIOStream> out = createIOStream(io);
            unsigned flags = COL_PREFIX | HTREE_FULLSORT_KEY;
            if (helper.getFlags() & TIWrowcompress)
                flags |= HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY;
            else if (!(helper.getFlags() & TIWnolzwcompress))
                flags |= HTREE_COMPRESSED_KEY;
            if (isVariable)
                flags |= HTREE_VARSIZE;
            Owned<IPropertyTree> metadata;
            buildUserMetadata(metadata);
            buildLayoutMetadata(metadata);
            unsigned nodeSize = metadata ? metadata->getPropInt("_nodeSize", NODESIZE) : NODESIZE;
            Owned<IKeyBuilder> builder = createKeyBuilder(out, flags, maxDiskRecordSize, fileSize, nodeSize, helper.getKeyedSize(), 0);
            class BcWrapper : implements IBlobCreator
            {
                IKeyBuilder *builder;
            public:
                BcWrapper(IKeyBuilder *_builder) : builder(_builder) {}
                virtual unsigned __int64 createBlob(size32_t size, const void * ptr)
                {
                    return builder->createBlob(size, (const char *) ptr);
                }
            } bc(builder);

            // Loop thru the results
            loop
            {
                OwnedConstRoxieRow nextrec(input->nextInGroup());
                if (!nextrec)
                {
                    nextrec.setown(input->nextInGroup());
                    if (!nextrec)
                        break;
                }
                try
                {
                    unsigned __int64 fpos;
                    RtlStaticRowBuilder rowBuilder(rowBuffer, maxDiskRecordSize);
                    size32_t thisSize = helper.transform(rowBuilder, nextrec, &bc, fpos);
                    builder->processKeyData(rowBuffer, fpos, thisSize);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                reccount++;
            }
            if(metadata)
                builder->finish(metadata,&fileCrc);
            else
                builder->finish(&fileCrc);
        }
    }

    virtual void stop(bool aborting)
    {
        if (writer)
        {
            if (!aborting)
                updateWorkUnitResult();
            writer->finish(!aborting, this);
            writer.clear();
        }
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        writer.clear();
    }

    //interface IRoxiePublishCallback

    virtual void setFileProperties(IFileDescriptor *desc) const
    {
        IPropertyTree &partProps = desc->queryPart(0)->queryProperties(); //properties of the first file part.
        IPropertyTree &fileProps = desc->queryProperties(); // properties of the logical file
        // Now publish to name services
        StringBuffer dir,base;
        offset_t indexFileSize = writer->queryFile()->size();
        if(clusterHandler)
            clusterHandler->splitPhysicalFilename(dir, base);
        else
            splitFilename(filename.str(), &dir, &dir, &base, &base);

        desc->setDefaultDir(dir.str());

        //properties of the first file part.
        Owned<IPropertyTree> attrs;
        if(clusterHandler)
            attrs.setown(createPTree("Part"));  // clusterHandler is going to set attributes
        else
        {
            // add cluster
            StringBuffer mygroupname;
            desc->setNumParts(1);
            desc->setPartMask(base.str());
            attrs.set(&desc->queryPart(0)->queryProperties());
        }
        attrs->setPropInt64("@size", indexFileSize);

        CDateTime createTime, modifiedTime, accessedTime;
        writer->queryFile()->getTime(&createTime, &modifiedTime, &accessedTime);
        // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        StringBuffer timestr;
        modifiedTime.getString(timestr);
        if(timestr.length())
            attrs->setProp("@modified", timestr.str());

        if(clusterHandler)
            clusterHandler->setDescriptorParts(desc, base.str(), attrs);

        // properties of the logical file
        IPropertyTree & properties = desc->queryProperties();
        properties.setProp("@kind", "key");
        properties.setPropInt64("@size", indexFileSize);
        properties.setPropInt64("@recordCount", reccount);
        SCMStringBuffer info;
        WorkunitUpdate workUnit = ctx->updateWorkUnit();
        if (workUnit)
        {
            properties.setProp("@owner", workUnit->getUser(info).str());
            info.clear();
            properties.setProp("@workunit", workUnit->getWuid(info).str());
            info.clear();
            properties.setProp("@job", workUnit->getJobName(info).str());
        }
        char const * rececl = helper.queryRecordECL();
        if(rececl && *rececl)
            properties.setProp("ECL", rececl);

        setExpiryTime(properties, helper.getExpiryDays());
        if (helper.getFlags() & TIWupdate)
        {
            unsigned eclCRC;
            unsigned __int64 totalCRC;
            helper.getUpdateCRCs(eclCRC, totalCRC);
            properties.setPropInt("@eclCRC", eclCRC);
            properties.setPropInt64("@totalCRC", totalCRC);
        }

        properties.setPropInt("@fileCrc", fileCrc);
        properties.setPropInt("@formatCrc", helper.getFormatCrc());
        void * layoutMetaBuff;
        size32_t layoutMetaSize;
        if(helper.getIndexLayout(layoutMetaSize, layoutMetaBuff))
        {
            properties.setPropBin("_record_layout", layoutMetaSize, layoutMetaBuff);
            rtlFree(layoutMetaBuff);
        }
    }

    IUserDescriptor *queryUserDescriptor() const
    {
        IConstWorkUnit *workUnit = ctx->queryWorkUnit();
        if (workUnit)
            return workUnit->queryUserDescriptor();
        else
            return NULL;
    }
};

//=================================================================================

class CRoxieServerIndexWriteActivityFactory : public CRoxieServerMultiOutputFactory
{
public:
    CRoxieServerIndexWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerMultiOutputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        setNumOutputs(0);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerIndexWriteActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return true;
    }

};

IRoxieServerActivityFactory *createRoxieServerIndexWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerIndexWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

static inline void getLimitType(unsigned flags, bool & limitFail, bool & limitOnFail) 
{ 
    if((flags & JFmatchAbortLimitSkips) != 0) 
    { 
        limitFail = false; 
        limitOnFail = false; 
    } 
    else 
    { 
        limitOnFail = ((flags & JFonfail) != 0); 
        limitFail = !limitOnFail; 
    } 
}

class CRoxieServerJoinActivity : public CRoxieServerTwoInputActivity
{
    enum { JSfill, JSfillleft, JSfillright, JScollate, JScompare, JSleftonly, JSrightonly } state;

    IHThorJoinArg &helper;
    ICompare * collate;
    ICompare * collateupper;

    ThorActivityKind activityKind;
    bool leftOuterJoin;
    bool rightOuterJoin;
    bool exclude;
    bool limitFail;
    bool limitOnFail;
    unsigned keepLimit;
    unsigned joinLimit;
    unsigned atmostLimit;
    unsigned abortLimit;
    bool betweenjoin;

    OwnedRowArray right;
    const void * left;
    const void * pendingRight;
    unsigned rightIndex;
    BoolArray matchedRight;
    bool matchedLeft;
    Owned<IException> failingLimit;
    ConstPointerArray filteredRight;
    Owned<IRHLimitedCompareHelper> limitedhelper;

    OwnedConstRoxieRow defaultLeft;
    OwnedConstRoxieRow defaultRight;
    Owned<IEngineRowAllocator> defaultLeftAllocator;
    Owned<IEngineRowAllocator> defaultRightAllocator;

    bool cloneLeft;

    void createDefaultLeft()
    {
        if (!defaultLeft)
        {
            if (!defaultLeftAllocator)
                defaultLeftAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultLeftAllocator);
            size32_t thisSize = helper.createDefaultLeft(rowBuilder);
            defaultLeft.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

    void createDefaultRight()
    {
        if (!defaultRight)
        {
            if (!defaultRightAllocator)
                defaultRightAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
            size32_t thisSize = helper.createDefaultRight(rowBuilder);
            defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

public:
    CRoxieServerJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerTwoInputActivity(_factory, _probeManager), helper((IHThorJoinArg &)basehelper)
    {
        // MORE - some of this should be done in factory
        unsigned joinFlags = helper.getJoinFlags();
        leftOuterJoin = (joinFlags & JFleftouter) != 0;
        rightOuterJoin = (joinFlags  & JFrightouter) != 0;
        exclude = (joinFlags  & JFexclude) != 0;
        cloneLeft = (joinFlags & JFtransformmatchesleft) != 0;
        getLimitType(joinFlags, limitFail, limitOnFail);
        if (joinFlags & JFslidingmatch) 
        {
            betweenjoin = true;
            collate = helper.queryCompareLeftRightLower();
            collateupper = helper.queryCompareLeftRightUpper();
        }
        else
        {
            betweenjoin = false;
            collate = collateupper = helper.queryCompareLeftRight();
        }
        rightIndex = 0;
        state = JSfill;
        matchedLeft = false;
        joinLimit = 0;
        keepLimit = 0;   // wait until ctx available 
        atmostLimit = 0; // wait until ctx available 
        abortLimit = 0;  // wait until ctx available 
        assertex((joinFlags & (JFfirst | JFfirstleft | JFfirstright)) == 0);
        left = NULL;
        pendingRight = NULL;
        activityKind = _factory->getKind();
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        left = NULL;
        rightIndex = 0;
        state = JSfill;
        matchedLeft = false;

        CRoxieServerTwoInputActivity::start(parentExtractSize, parentExtract, paused);

        keepLimit = helper.getKeepLimit();
        if (keepLimit == 0) 
            keepLimit = (unsigned)-1;
        atmostLimit = helper.getJoinLimit();
        if(atmostLimit == 0)
            atmostLimit = (unsigned)-1;
        else
            assertex(!rightOuterJoin && !betweenjoin);
        abortLimit = helper.getMatchAbortLimit();
        if (abortLimit == 0) 
            abortLimit = (unsigned)-1;
        if (rightOuterJoin)
            createDefaultLeft();
        if ((leftOuterJoin && (activityKind==TAKjoin || activityKind==TAKjoinlight || activityKind==TAKdenormalizegroup)) || limitOnFail)
            createDefaultRight();
        if ((helper.getJoinFlags() & JFlimitedprefixjoin) && helper.getJoinLimit()) 
        {   //limited match join (s[1..n])
            limitedhelper.setown(createRHLimitedCompareHelper());
            limitedhelper->init( helper.getJoinLimit(), input1, collate, helper.queryPrefixCompare() );
        }
    }

    virtual void reset()
    {
        right.clear();
        ReleaseClearRoxieRow(left);
        ReleaseClearRoxieRow(pendingRight);
        defaultRight.clear();
        defaultLeft.clear();

        CRoxieServerTwoInputActivity::reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        switch(idx)
        {
        case 0:
            if ((helper.getJoinFlags() & JFparallel) != 0)
            {
                puller.setown(new CRoxieServerReadAheadInput(0)); // MORE - cant ask context for parallelJoinPreload as context is not yet set up.
                puller->setInput(0, _in);
                _in = puller;
            }
            input = _in;
            break;
        case 1:
            input1 = _in;
            break;
        default:
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        }   
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL : this;
    }

    void fillLeft()
    {
        matchedLeft = false;
        left = input->nextInGroup();
        if (!left)
            left = input->nextInGroup();
        if(betweenjoin && left && pendingRight && (collate->docompare(left, pendingRight) >= 0))
            fillRight();
        if (limitedhelper && 0==rightIndex)
        {
            rightIndex = 0;
            right.clear();
            matchedRight.kill();
            if (left)
            {
                limitedhelper->getGroup(right,left);
                ForEachItemIn(idx, right)
                    matchedRight.append(false);
            }
        }
    }

    void fillRight()
    {
        if (limitedhelper)
            return;
        failingLimit.clear();
        if(betweenjoin && left)
        {
            aindex_t start = 0;
            while(right.isItem(start) && (collateupper->docompare(left, right.item(start)) > 0))
                start++;
            if(start>0)
                right.clearPart(0, start);
        }
        else
            right.clear();
        rightIndex = 0;
        unsigned groupCount = 0;
        const void * next;
        while(true)
        {
            if(pendingRight)
            {
                next = pendingRight;
                pendingRight = NULL;
            }
            else
            {
                next = input1->nextInGroup();
            }
            if(!rightOuterJoin && next && (!left || (collateupper->docompare(left, next) > 0))) // if right is less than left, and not right outer, can skip group
            {
                while(next)
                {
                    ReleaseClearRoxieRow(next);
                    next = input1->nextInGroup();
                }
                continue;
            }
            while(next)
            {
                if(groupCount==abortLimit)
                {
                    if(limitFail)
                        failLimit();
                    if (ctx->queryDebugContext())
                        ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                    if(limitOnFail)
                    {
                        assertex(!failingLimit);
                        try
                        {
                            failLimit();
                        }
                        catch(IException * except)
                        {
                            failingLimit.setown(except);
                        }
                        assertex(failingLimit != NULL);
                    }
                    right.append(next);
                    do
                    {
                        next = input1->nextInGroup();
                        ReleaseRoxieRow(next);
                    } while(next);
                    break;
                }
                else if(groupCount==atmostLimit)
                {
                    right.clear();
                    groupCount = 0;
                    while(next) 
                    {
                        ReleaseRoxieRow(next);
                        next = input1->nextInGroup();
                    }
                }
                else
                {
                    right.append(next);
                    groupCount++;
                }
                next = input1->nextInGroup();
            }
            // normally only want to read one right group, but if is between join and next right group is in window for left, need to continue
            if(betweenjoin && left)
            {
                pendingRight = input1->nextInGroup();
                if(!pendingRight || (collate->docompare(left, pendingRight) < 0))
                    break;
            }
            else
                break;
        }
        matchedRight.kill();
        ForEachItemIn(idx, right)
            matchedRight.append(false);
    }

    const void * joinRecords(const void * curLeft, const void * curRight)
    {
        if (cloneLeft)
        {
            LinkRoxieRow(curLeft);
            return curLeft;
        }
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t thisSize = helper.transform(rowBuilder, curLeft, curRight);
            if (thisSize)
                return rowBuilder.finalizeRowClear(thisSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * denormalizeRecords(const void * curLeft, ConstPointerArray & rows)
    {
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned numRows = rows.ordinality();
            const void * right = numRows ? rows.item(0) : defaultRight.get();
            size32_t thisSize = helper.transform(rowBuilder, curLeft, right, numRows, (const void * *)rows.getArray());
            if (thisSize)
                return rowBuilder.finalizeRowClear(thisSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * joinException(const void * curLeft, IException * except)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t thisSize = helper.onFailTransform(rowBuilder, curLeft, defaultRight, except);
        return rowBuilder.finalizeRowClear(thisSize);
    }

    void failLimit()
    {
        helper.onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
        {
            input->queryOutputMeta()->toXML((byte *) left, xmlwrite);
        }
        throw MakeStringException(ROXIE_TOO_MANY_RESULTS, "More than %d match candidates in join %d for row %s", abortLimit, queryId(), xmlwrite.str());
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            switch (state)
            {
            case JSfill:
                fillLeft();
                state = JSfillright;
                break;

            case JSfillright:
                fillRight();
                state = JScollate;
                break;

            case JSfillleft:
                fillLeft();
                state = JScollate;
                break;

            case JScollate:
                if (right.ordinality() == 0)
                {
                    if (left == NULL)
                        return NULL;
                    state = JSleftonly;
                }
                else
                {
                    if (!left)
                        state = JSrightonly;
                    else
                    {
                        int diff;
                        if(betweenjoin)
                            diff = ((collate->docompare(left, right.item(0)) < 0) ? -1 : ((collateupper->docompare(left, right.item(right.ordinality()-1)) > 0) ? +1 : 0));
                        else
                            diff = collate->docompare(left, right.item(0));
                        bool limitExceeded =  right.ordinality()>abortLimit;
                        if (diff == 0)
                        {
                            if (limitExceeded)
                            {
                                const void * ret = NULL;
                                if(failingLimit)
                                    ret = joinException(left, failingLimit);
                                ReleaseRoxieRow(left);
                                left = NULL;
                                state = JSfillleft;
                                ForEachItemIn(idx, right)
                                    matchedRight.replace(true, idx);
                                if(ret)
                                {
                                    processed++;
                                    return ret;
                                }
                            }
                            else
                            {
                                state = JScompare;
                                joinLimit = keepLimit;
                            }
                        }
                        else if (diff < 0)
                            state = JSleftonly;
                        else if (limitExceeded)
                        {
                            // MORE - Roxie code seems to think there should be a destroyRowset(right) here....
                            state = JSfillright;
                        }
                        else
                            state = JSrightonly;
                    }
                }
                break;

            case JSrightonly:
                if (rightOuterJoin)
                {
                    switch (activityKind)
                    {
                    case TAKjoin:
                        {
                            while (right.isItem(rightIndex))
                            {
                                if (!matchedRight.item(rightIndex))
                                {
                                    const void * rhs = right.item(rightIndex++);
                                    const void *ret = joinRecords(defaultLeft, rhs);
                                    if (ret)
                                    {
                                        processed++;
                                        return ret;
                                    }
                                }
                                rightIndex++;
                            }
                            break;
                        }
                    //Probably excessive to implement the following, but possibly useful
                    case TAKdenormalize:
                        {
                            OwnedConstRoxieRow newLeft;
                            newLeft.set(defaultLeft);
                            unsigned rowSize = 0;
                            unsigned leftCount = 0;
                            while (right.isItem(rightIndex))
                            {
                                if (!matchedRight.item(rightIndex))
                                {
                                    const void * rhs = right.item(rightIndex);
                                    try
                                    {
                                        RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                        unsigned thisSize = helper.transform(rowBuilder, newLeft, rhs, ++leftCount);
                                        if (thisSize)
                                        {
                                            rowSize = thisSize;
                                            newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                        }
                                    }
                                    catch (IException *E)
                                    {
                                        throw makeWrappedException(E);
                                    }
                                }
                                rightIndex++;
                            }
                            state = JSfillright;
                            if (rowSize)
                            {
                                processed++;
                                return newLeft.getClear();
                            }
                            break;
                        }
                    case TAKdenormalizegroup:
                        {
                            filteredRight.kill();
                            while (right.isItem(rightIndex))
                            {
                                if (!matchedRight.item(rightIndex))
                                    filteredRight.append(right.item(rightIndex));
                                rightIndex++;
                            }
                            state = JSfillright;
                            if (filteredRight.ordinality())
                            {
                                const void * ret = denormalizeRecords(defaultLeft, filteredRight);
                                filteredRight.kill();

                                if (ret)
                                {
                                    processed++;
                                    return ret;
                                }
                            }
                            break;
                        }
                    }
                }
                state = JSfillright;
                break;
                
            case JSleftonly:
            {
                const void * ret = NULL;
                if (!matchedLeft && leftOuterJoin)
                {
                    switch (activityKind)
                    {
                    case TAKjoin:
                        ret = joinRecords(left, defaultRight);
                        break;
                    case TAKdenormalize:
                        ret = left;
                        left = NULL;
                        break;
                    case TAKdenormalizegroup:
                        filteredRight.kill();
                        ret = denormalizeRecords(left, filteredRight);
                        break;
                    }
                }
                ReleaseRoxieRow(left);
                left = NULL;
                state = JSfillleft;
                if (ret)
                {
                    processed++;
                    return ret;
                }
                break;
            }

            case JScompare:
                if (joinLimit != 0)
                {
                    switch (activityKind)
                    {
                    case TAKjoin:
                        {
                            while (right.isItem(rightIndex))
                            {
                                const void * rhs = right.item(rightIndex++);
                                if (helper.match(left, rhs))
                                {
                                    matchedRight.replace(true, rightIndex-1);
                                    matchedLeft = true;
                                    if (!exclude)
                                    {
                                        const void *ret = joinRecords(left, rhs);
                                        if (ret)
                                        {
                                            processed++;
                                            joinLimit--;
                                            return ret;
                                        }
                                    }
                                }
                            }
                            break;
                        }
                    case TAKdenormalize:
                        {
                            OwnedConstRoxieRow newLeft;
                            newLeft.set(left);
                            unsigned rowSize = 0;
                            unsigned leftCount = 0;
                            while (right.isItem(rightIndex) && joinLimit)
                            {
                                try
                                {
                                    const void * rhs = right.item(rightIndex++);
                                    if (helper.match(left, rhs))
                                    {
                                        matchedRight.replace(true, rightIndex-1);
                                        matchedLeft = true;
                                        if (!exclude)
                                        {
                                            RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                            unsigned thisSize = helper.transform(rowBuilder, newLeft, rhs, ++leftCount);
                                            if (thisSize)
                                            {
                                                rowSize = thisSize;
                                                newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                                joinLimit--;
                                            }
                                        }
                                    }
                                }
                                catch (IException *E)
                                {
                                    throw makeWrappedException(E);
                                }
                            }
                            state = JSleftonly;
                            rightIndex = 0;
                            if (rowSize)
                            {
                                processed++;
                                return newLeft.getClear();
                            }
                            break;
                        }
                    case TAKdenormalizegroup:
                        {
                            filteredRight.kill();
                            while (right.isItem(rightIndex))
                            {
                                const void * rhs = right.item(rightIndex++);
                                if (helper.match(left, rhs))
                                {
                                    matchedRight.replace(true, rightIndex-1);
                                    filteredRight.append(rhs);
                                    matchedLeft = true;
                                    if (filteredRight.ordinality()==joinLimit)
                                        break;
                                }
                            }
                            state = JSleftonly;
                            rightIndex = 0;

                            if (!exclude && filteredRight.ordinality())
                            {
                                const void * ret = denormalizeRecords(left, filteredRight);
                                filteredRight.kill();

                                if (ret)
                                {
                                    processed++;
                                    return ret;
                                }
                            }
                            break;
                        }
                    }
                }
                state = JSleftonly;
                rightIndex = 0;
                break;
            }
        }
    }
};

class CRoxieServerJoinActivityFactory : public CRoxieServerActivityFactory
{
    unsigned input2;
    unsigned input2idx;

public:
    CRoxieServerJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        input2 = 0;
        input2idx = 0;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerJoinActivity(this, _probeManager);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        if (idx==1)
        {
            input2 = source;
            input2idx = sourceidx;
        }
        else
            CRoxieServerActivityFactory::setInput(idx, source, sourceidx);
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        switch (idx)
        {
        case 1:
            sourceidx = input2idx;
            return input2;
        case 0:
            return CRoxieServerActivityFactory::getInput(idx, sourceidx);
        default:
            return (unsigned) -1; 
        }
    }

    virtual unsigned numInputs() const { return 2; }
};

IRoxieServerActivityFactory *createRoxieServerJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

#define CONCAT_READAHEAD 1000

MAKEPointerArray(RecordPullerThread, RecordPullerArray);

class CRoxieServerThreadedConcatActivity : public CRoxieServerActivity, implements IRecordPullerCallback
{
    QueueOf<const void, true> buffer;
    InterruptableSemaphore ready;
    InterruptableSemaphore space;
    CriticalSection crit;
    unsigned eofs;
    RecordPullerArray pullers;
    unsigned numInputs;

public:
    CRoxieServerThreadedConcatActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _grouped, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        eofs = 0;
        numInputs = _numInputs;
        for (unsigned i = 0; i < numInputs; i++)
            pullers.append(*new RecordPullerThread(_grouped));

    }

    ~CRoxieServerThreadedConcatActivity()
    {
        ForEachItemIn(idx, pullers)
            delete &pullers.item(idx);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        space.reinit(CONCAT_READAHEAD);
        ready.reinit();
        eofs = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        ForEachItemIn(idx, pullers)
        {
            pullers.item(idx).start(parentExtractSize, parentExtract, paused, ctx->concatPreload(), false, ctx);  
            // NOTE - it is ok to start the thread running while parts of the subgraph are still being started, since everything 
            // in the part of the subgraph that the thread uses has been started.
            // Note that splitters are supposed to cope with being used when only some outputs have been started.
        }
    }


    virtual void stop(bool aborting)    
    {
        space.interrupt();
        ready.interrupt();
        ForEachItemIn(idx, pullers)
            pullers.item(idx).stop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return 0;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (pullers.isItem(idx))
            return pullers.item(idx).queryInput();
        else
            return NULL;
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        ForEachItemIn(idx, pullers)
            pullers.item(idx).reset();
        ForEachItemIn(idx1, buffer)
            ReleaseRoxieRow(buffer.item(idx1));
        buffer.clear();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (pullers.isItem(idx))
            pullers.item(idx).setInput(this, _in);
        else
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            {
                CriticalBlock b(crit);
                if (eofs==numInputs && !buffer.ordinality())
                    return NULL;  // eof
            }
            ready.wait();
            const void *ret;
            {
                CriticalBlock b(crit);
                ret = buffer.dequeue();
            }
            if (ret)
                processed++;
            space.signal();
            return ret;
        }
    }

    virtual bool fireException(IException *e)
    {
        // called from puller thread on failure
        ready.interrupt(LINK(e));
        space.interrupt(e);
        return true;
    }

    virtual void processRow(const void *row)
    {
        {
            CriticalBlock b(crit);
            buffer.enqueue(row);
        }
        ready.signal();
        space.wait();
    }

    virtual void processGroup(const ConstPointerArray &rows)
    {
        // NOTE - a bit bizzare in that it waits for the space AFTER using it.
        // But the space semaphore is only there to stop infinite readahead. And otherwise it would deadlock
        // if group was larger than max(space)
        {
            CriticalBlock b(crit);
            ForEachItemIn(idx, rows)
                buffer.enqueue(rows.item(idx));
            buffer.enqueue(NULL);
        }
        for (unsigned i2 = 0; i2 <= rows.length(); i2++) // note - does 1 extra for the null
        {
            ready.signal();
            space.wait();
        }
    }

    virtual void processEOG()
    {
        // Used when output is not grouped - just ignore
    }

    virtual void processDone()
    {
        CriticalBlock b(crit);
        eofs++;
        if (eofs == numInputs)
            ready.signal();
    }
};

class CRoxieServerOrderedConcatActivity : public CRoxieServerActivity
{
    IRoxieInput *curInput;
    bool eogSeen;
    bool anyThisGroup;
    bool grouped;
    unsigned numInputs;
    unsigned inputIdx;
    IRoxieInput **inputArray;

public:
    CRoxieServerOrderedConcatActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _grouped, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        eogSeen = false;
        anyThisGroup = false;
        grouped = _grouped;
        numInputs = _numInputs;
        inputIdx = 0;
        inputArray = new IRoxieInput*[numInputs];
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i] = NULL;
    }

    ~CRoxieServerOrderedConcatActivity()
    {
        delete [] inputArray;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        inputIdx = 0;
        curInput = inputArray[inputIdx];
        eogSeen = false;
        anyThisGroup = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i]->start(parentExtractSize, parentExtract, paused);
    }

    virtual void stop(bool aborting)    
    {
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i]->stop(aborting);
        CRoxieServerActivity::stop(aborting); 
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return 0;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (idx < numInputs)
            return inputArray[idx];
        else
            return NULL;
    }

    virtual void reset()    
    {
        CRoxieServerActivity::reset(); 
        for (unsigned i = 0; i < numInputs; i++)
            inputArray[i]->reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        inputArray[idx] = _in;
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!curInput)
            return NULL;  // eof
        const void * next = curInput->nextInGroup();
        if (next)
        {
            anyThisGroup = true;
            eogSeen = false;
            processed++;
            return next;
        }
        else if (!eogSeen)
        {
            eogSeen = true;
            if (grouped)
            {
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return NULL;
                }
                else
                    return nextInGroup();
            }
            else
                return nextInGroup();
        }
        else if (inputIdx < numInputs-1)
        {
            inputIdx++;
            curInput = inputArray[inputIdx];
            eogSeen = false;
            return nextInGroup();
        }
        else
        {
            curInput = NULL;
            return NULL;
        }
    }
};

class CRoxieServerConcatActivityFactory : public CRoxieServerMultiInputFactory
{
    bool ordered;
    bool grouped;

public:
    CRoxieServerConcatActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned <IHThorFunnelArg> helper = (IHThorFunnelArg *) helperFactory();
        ordered = helper->isOrdered();
        grouped = helper->queryOutputMeta()->isGrouped();
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (ordered || (_probeManager && _probeManager->queryDebugManager()))
            return new CRoxieServerOrderedConcatActivity(this, _probeManager, grouped, numInputs());
        else
            return new CRoxieServerThreadedConcatActivity(this, _probeManager, grouped, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerConcatActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerConcatActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerNonEmptyActivity : public CRoxieServerMultiInputBaseActivity
{
    IRoxieInput * selectedInput;
    unsigned savedParentExtractSize;
    const byte * savedParentExtract;
    bool foundInput;

public:
    CRoxieServerNonEmptyActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputBaseActivity(_factory, _probeManager, _numInputs)
    {
        foundInput = false;
        selectedInput = NULL;
        savedParentExtractSize = 0;;
        savedParentExtract = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        //Don't start the inputs yet so we can short-circuit...
        CRoxieServerMultiInputBaseActivity::start(parentExtractSize, parentExtract, paused);
        savedParentExtractSize = parentExtractSize;
        savedParentExtract = parentExtract;
    }

    virtual void stop(bool aborting)    
    {
        if (foundInput)
        {
            if (selectedInput)
                selectedInput->stop(aborting);
        }
        else
        {
            for (unsigned i = 0; i < numInputs; i++)
                inputArray[i]->stop(aborting);
        }
        CRoxieServerMultiInputBaseActivity::stop(aborting); 
    }

    virtual void reset()    
    {
        CRoxieServerMultiInputBaseActivity::reset(); 
        foundInput = false;
        selectedInput = NULL;
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return 0; // Can't easily calcuate anything reliable but local processing is negligible
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!foundInput)
        {
            foundInput = true;
            //If we get an exception in this loop then stop() will stop any started inputs
            for (unsigned i=0; i < numInputs; i++)
            {
                selectedInput = inputArray[i];
                selectedInput->start(savedParentExtractSize, savedParentExtract, false);
                const void * next = selectedInput->nextInGroup();
                if (next)
                {
                    //Found a row so stop remaining
                    for (unsigned j=i+1; j < numInputs; j++)
                        inputArray[j]->stop(false);
                    processed++;
                    return next;
                }
                selectedInput->stop(false);
            }
            selectedInput = NULL;
            return NULL;
        }
        if (!selectedInput)
            return NULL;
        const void * next = selectedInput->nextInGroup();
        if (next)
            processed++;
        return next;
    }
};

class CRoxieServerNonEmptyActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerNonEmptyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNonEmptyActivity(this, _probeManager, numInputs());
    }

};

IRoxieServerActivityFactory *createRoxieServerNonEmptyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNonEmptyActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerMergeActivity : public CRoxieServerActivity
{
    IHThorMergeArg &helper;
    unsigned *mergeheap;
    unsigned activeInputs;
    unsigned numInputs;
    IRoxieInput **inputArray;
    const void **pending;
    bool first;
    ICompare *compare;
    bool dedup;

    void permute()
    {
        assertex(activeInputs == 0);
        for(unsigned i = 0; i < numInputs; i++)
            if(pullInput(i))
                mergeheap[activeInputs++] = i;
        // the tree structure: element p has children p*2+1 and p*2+2, or element c has parent (unsigned)(c-1)/2
        // the heap property: no element should be smaller than its parent
        // the dedup variant: if(dedup), the top of the heap should also not be equal to either child
        // the method: establish this by starting with the parent of the bottom element and working up to the top element, sifting each down to its correct place
        if (activeInputs >= 2)
            for(unsigned p = (activeInputs-2)/2; p > 0; --p)
                siftDown(p);
        if(dedup)
            siftDownDedupTop();
        else
            siftDown(0);
    }

    void readNext()
    {
        if(!pullInput(mergeheap[0]))
            if(!promote(0))
                return;
        // we have changed the element at the top of the heap, so need to sift it down to maintain the heap property
        if(dedup)
            siftDownDedupTop();
        else
            siftDown(0);
    }

    bool pullInput(unsigned i)
    {
        const void *next = inputArray[i]->nextInGroup();
        if (!next)
            next = inputArray[i]->nextInGroup();
        pending[i] = next;
        return (next != NULL);
    }

    bool promote(unsigned p)
    {
        activeInputs--;
        if(activeInputs == p)
            return false;
        mergeheap[p] = mergeheap[activeInputs];
        return true;
    }

    bool siftDown(unsigned p)
    {
        // assumimg that all descendents of p form a heap, sift p down to its correct position, and so include it in the heap
        bool nochange = true;
        while(1)
        {
            unsigned c = p*2 + 1;
            if(c >= activeInputs) 
                return nochange;
            if(c+1 < activeInputs)
            {
                int childcmp = BuffCompare(c+1, c);
                if((childcmp < 0) || ((childcmp == 0) && (mergeheap[c+1] < mergeheap[c])))
                    ++c;
            }
            int cmp = BuffCompare(c, p);
            if((cmp > 0) || ((cmp == 0) && (mergeheap[c] > mergeheap[p])))
                return nochange;
            nochange = false;
            unsigned r = mergeheap[c];
            mergeheap[c] = mergeheap[p];
            mergeheap[p] = r;
            p = c;
        }
    }

    void siftDownDedupTop()
    {
        // same as siftDown(0), except that it also ensures that the top of the heap is not equal to either of its children
        if(activeInputs < 2)
            return;
        unsigned c = 1;
        int childcmp = 1;
        if(activeInputs >= 3)
        {
            childcmp = BuffCompare(2, 1);
            if(childcmp < 0)
                c = 2;
        }
        int cmp = BuffCompare(c, 0);
        if(cmp > 0)
            return;
        // the following loop ensures the correct property holds on the smaller branch, and that childcmp==0 iff the top matches the other branch
        while(cmp <= 0)
        {
            if(cmp == 0)
            {
                if(mergeheap[c] < mergeheap[0])
                {
                    unsigned r = mergeheap[c];
                    mergeheap[c] = mergeheap[0];
                    mergeheap[0] = r;
                }
                ReleaseClearRoxieRow(pending[mergeheap[c]]);
                if(!pullInput(mergeheap[c]))
                    if(!promote(c))
                        break;
                siftDown(c);
            }
            else
            {
                unsigned r = mergeheap[c];
                mergeheap[c] = mergeheap[0];
                mergeheap[0] = r;
                if(siftDown(c))
                    break;
            }
            cmp = BuffCompare(c, 0);
        }
        // the following loop ensures the uniqueness property holds on the other branch too
        c = 3-c;
        if(activeInputs <= c)
            return;
        while(childcmp == 0)
        {
            if(mergeheap[c] < mergeheap[0])
            {
                unsigned r = mergeheap[c];
                mergeheap[c] = mergeheap[0];
                mergeheap[0] = r;
            }
            ReleaseClearRoxieRow(pending[mergeheap[c]]);
            if(!pullInput(mergeheap[c]))
                if(!promote(c))
                    break;
            siftDown(c);
            childcmp = BuffCompare(c, 0);
        }
    }

    inline int BuffCompare(unsigned a, unsigned b)
    {
        return compare->docompare(pending[mergeheap[a]], pending[mergeheap[b]]);
    }

public:
    CRoxieServerMergeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorMergeArg &)basehelper), numInputs(_numInputs)
    {
        activeInputs = 0;
        first = true;
        mergeheap = new unsigned[numInputs];
        inputArray = new IRoxieInput*[numInputs];
        pending = new const void *[numInputs];
        compare = helper.queryCompare();
        dedup = helper.dedup();
        for (unsigned i = 0; i < numInputs; i++)
        {
            inputArray[i] = NULL;
            pending[i] = NULL;
        }
    }

    ~CRoxieServerMergeActivity()
    {
        delete [] mergeheap;
        delete [] inputArray;
        delete [] pending;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        activeInputs = 0;
        first = true;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        for (unsigned i = 0; i < numInputs; i++)
        {
            inputArray[i]->start(parentExtractSize, parentExtract, paused);
        }

    }

    virtual void stop(bool aborting)    
    {
        for (unsigned i = 0; i < numInputs; i++)
        {
            inputArray[i]->stop(aborting);
        }
        CRoxieServerActivity::stop(aborting); 
    }

    virtual void reset()    
    {
        for (unsigned i = 0; i < numInputs; i++)
        {
            ReleaseClearRoxieRow(pending[i]);
            inputArray[i]->reset();
        }
        CRoxieServerActivity::reset(); 
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        inputArray[idx] = _in;
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (first)
        {
            permute();
            first = false;
        }
        if (activeInputs)
        {
            const void *next = pending[mergeheap[0]];
            readNext();
            if (next)
                processed++;
            return next;
        }
        else
            return NULL;
    }
};

class CRoxieServerMergeActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerMergeActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerMergeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerRegroupActivity : public CRoxieServerMultiInputActivity
{
    IHThorRegroupArg &helper;
    unsigned inputIndex;
    bool eof;
    unsigned __int64 numProcessedLastGroup;

public:
    CRoxieServerRegroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputActivity(_factory, _probeManager, _numInputs), helper((IHThorRegroupArg &)basehelper)
    {
        inputIndex = 0;
        eof = false;
        numProcessedLastGroup = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        inputIndex = 0;
        eof = false;
        numProcessedLastGroup = processed;
        CRoxieServerMultiInputActivity::start(parentExtractSize, parentExtract, paused);
    }

    const void * nextFromInputs()
    {
        unsigned initialInput = inputIndex;
        while (inputIndex < numInputs)
        {
            const void * next = inputArray[inputIndex]->nextInGroup();
            if (next)
            {
                if ((inputIndex != initialInput) && (inputIndex != initialInput+1))
                {
                    ReleaseRoxieRow(next);
                    throw MakeStringException(ROXIE_MISMATCH_GROUP_ERROR, "Mismatched groups supplied to Regroup (%d)", factory->queryId());
                }
                return next;
            }
            inputIndex++;
        }

        if ((initialInput != 0) && (initialInput+1 != numInputs))
            throw MakeStringException(ROXIE_MISMATCH_GROUP_ERROR, "Mismatched groups supplied to Regroup (%d)", factory->queryId());

        inputIndex = 0;
        return NULL;
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        const void * ret = nextFromInputs();
        if (ret)
        {
            processed++;
            return ret;
        }

        if (numProcessedLastGroup != processed)
        {
            numProcessedLastGroup = processed;
            return NULL;
        }

        eof = true;
        return NULL;
    }

#if 0
    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        //MORE: RKC: Do we want to do this i) always ii) conditionally iii) never
        if (idx)
        {
            puller.setown(new CRoxieServerReadAheadInput(0)); // MORE - cant ask context for parallelJoinPreload as context is not yet set up.
            puller->setInput(0, _in);
            CRoxieServerMultiInputActivity::setInput(idx, puller);
        }
        else
            CRoxieServerMultiInputActivity::setInput(idx, _in);
    }
#endif

};

class CRoxieServerRegroupActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerRegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerRegroupActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerRegroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerRegroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerCombineActivity : public CRoxieServerMultiInputActivity
{
    IHThorCombineArg &helper;
    unsigned __int64 numProcessedLastGroup;

public:
    CRoxieServerCombineActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputActivity(_factory, _probeManager, _numInputs), helper((IHThorCombineArg &)basehelper)
    {
        numProcessedLastGroup = 0;
    }

    ~CRoxieServerCombineActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = processed;
        CRoxieServerMultiInputActivity::start(parentExtractSize, parentExtract, paused);
    }

    void nextInputs(ConstPointerArray & out)
    {
        for (unsigned i=0; i < numInputs; i++)
        {
            const void * next = inputArray[i]->nextInGroup();
            if (next)
                out.append(next);
        }
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            ConstPointerArray group;
            nextInputs(group);
            if ((group.ordinality() == 0) && (numProcessedLastGroup == processed))
                nextInputs(group);
            if (group.ordinality() == 0)
            {
                numProcessedLastGroup = processed;
                return NULL;
            }
            else if (group.ordinality() != numInputs)
            {
                ReleaseRoxieRowSet(group);
                throw MakeStringException(ROXIE_MISMATCH_GROUP_ERROR, "Mismatched group input for Combine Activity(%d)", factory->queryId());
            }

            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, group.ordinality(), group.getArray());
                ReleaseRoxieRowSet(group);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException *E)
            {
                ReleaseRoxieRowSet(group);
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerCombineActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerCombineActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerCombineActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerCombineActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerCombineActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerCombineGroupActivity : public CRoxieServerTwoInputActivity
{
    IHThorCombineGroupArg &helper;
    unsigned __int64 numProcessedLastGroup;

public:
    CRoxieServerCombineGroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerTwoInputActivity(_factory, _probeManager), helper((IHThorCombineGroupArg &)basehelper)
    {
        numProcessedLastGroup = 0;
    }

    ~CRoxieServerCombineGroupActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = processed;
        CRoxieServerTwoInputActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        switch(idx)
        {
        case 0:
#if 0
            //MORE: RKC: Do we want to do this i) always ii) conditionally iii) never
            puller.setown(new CRoxieServerReadAheadInput(0)); // MORE - cant ask context for parallelJoinPreload as context is not yet set up.
            puller->setInput(0, _in);
            _in = puller;
#endif
            input = _in;
            break;
        case 1:
            input1 = _in;
            break;
        default:
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        }   
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL : this;
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            const void * left = input->nextInGroup();
            if (!left && (numProcessedLastGroup == processed))
                left = input->nextInGroup();

            if (!left)
            {
                if (numProcessedLastGroup == processed)
                {
                    const void * nextRight = input1->nextInGroup();
                    if (nextRight)
                    {
                        ReleaseRoxieRow(nextRight);
                        throw MakeStringException(ROXIE_MISSING_GROUP_ERROR, "Missing LEFT record for Combine Group (%d)", factory->queryId());
                    }
                }
                else
                    numProcessedLastGroup = processed;
                return NULL;
            }

            ConstPointerArray group;
            loop
            {
                const void * in = input1->nextInGroup();
                if (!in)
                    break;
                group.append(in);
            }

            if (group.ordinality() == 0)
            {
                ReleaseRoxieRow(left);
                ReleaseRoxieRowSet(group);
                throw MakeStringException(ROXIE_MISSING_GROUP_ERROR, "Missing RIGHT group for Combine Group (%d)", factory->queryId());
            }

            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, left, group.ordinality(), (const void * *)group.getArray());
                ReleaseRoxieRow(left);
                ReleaseRoxieRowSet(group);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException *E)
            {
                ReleaseRoxieRow(left);
                ReleaseRoxieRowSet(group);
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerCombineGroupActivityFactory : public CRoxieServerActivityFactory
{
    unsigned input2;
    unsigned input2idx;

public:
    CRoxieServerCombineGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        input2 = 0;
        input2idx = 0;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerCombineGroupActivity(this, _probeManager);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        if (idx==1)
        {
            input2 = source;
            input2idx = sourceidx;
        }
        else
            CRoxieServerActivityFactory::setInput(idx, source, sourceidx);
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        switch (idx)
        {
        case 1:
            sourceidx = input2idx;
            return input2;
        case 0:
            return CRoxieServerActivityFactory::getInput(idx, sourceidx);
        default:
            return (unsigned) -1; 
        }
    }

    virtual unsigned numInputs() const { return 2; }
};

IRoxieServerActivityFactory *createRoxieServerCombineGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerCombineGroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerRollupGroupActivity : public CRoxieServerActivity
{
    IHThorRollupGroupArg &helper;
    bool eof;

public:
    CRoxieServerRollupGroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorRollupGroupArg &)basehelper)
    {
        eof = false;
    }

    ~CRoxieServerRollupGroupActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        loop
        {
            ConstPointerArray group;

            loop
            {
                const void * in = input->nextInGroup();
                if (!in)
                    break;
                group.append(in);
            }

            if (group.ordinality() == 0)
            {
                eof = true;
                return NULL;
            }

            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, group.ordinality(), (const void * *)group.getArray());

                ReleaseRoxieRowSet(group);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException * E)
            {
                ReleaseRoxieRowSet(group);
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerRollupGroupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerRollupGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerRollupGroupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerRollupGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerRollupGroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerFilterProjectActivity : public CRoxieServerLateStartActivity
{
    IHThorFilterProjectArg &helper;
    unsigned numProcessedLastGroup;
    unsigned __int64 recordCount;

public:
    CRoxieServerFilterProjectActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerLateStartActivity(_factory, _probeManager), helper((IHThorFilterProjectArg &)basehelper)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
    }

    ~CRoxieServerFilterProjectActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
        CRoxieServerLateStartActivity::start(parentExtractSize, parentExtract, paused);
        lateStart(parentExtractSize, parentExtract, helper.canMatchAny()); //sets eof
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        loop
        {
            const void * in = input->nextInGroup();
            if (!in)
            {
                recordCount = 0;
                if (numProcessedLastGroup == processed)
                    in = input->nextInGroup();
                if (!in)
                {
                    numProcessedLastGroup = processed;
                    return NULL;
                }
            }

            try 
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize = helper.transform(rowBuilder, in, ++recordCount);
                ReleaseRoxieRow(in);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerFilterProjectActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerFilterProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerFilterProjectActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerFilterProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerFilterProjectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================
class CRoxieServerProjectActivity : public CRoxieServerActivity
{
    unsigned numProcessedLastGroup;
    bool count;
    unsigned __int64 recordCount;

public:
    CRoxieServerProjectActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _count)
        : CRoxieServerActivity(_factory, _probeManager), 
        count(_count)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
    }

    ~CRoxieServerProjectActivity()
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            OwnedConstRoxieRow in = input->nextInGroup();
            if (!in)
            {
                recordCount = 0;
                if (numProcessedLastGroup == processed)
                    in.setown(input->nextInGroup());
                if (!in)
                {
                    numProcessedLastGroup = processed;
                    return NULL;
                }
            }

            try 
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                size32_t outSize;
                if (count)
                    outSize = ((IHThorCountProjectArg &) basehelper).transform(rowBuilder, in, ++recordCount);
                else
                    outSize = ((IHThorProjectArg &) basehelper).transform(rowBuilder, in);
                if (outSize)
                {
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerProjectActivityFactory : public CRoxieServerActivityFactory
{
protected:
    bool count;
public:
    CRoxieServerProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        count = (_kind==TAKcountproject || _kind==TAKprefetchcountproject);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerProjectActivity(this, _probeManager, count);
    }
};

IRoxieServerActivityFactory *createRoxieServerProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerProjectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerPrefetchProjectActivity : public CRoxieServerActivity, implements IRecordPullerCallback
{
    unsigned numProcessedLastGroup;
    bool count;
    bool eof;
    bool allPulled;
    bool isThreaded;
    unsigned preload;
    unsigned __int64 recordCount;
    IHThorPrefetchProjectArg &helper;
    RecordPullerThread puller;
    InterruptableSemaphore ready;
    InterruptableSemaphore space;

    class PrefetchInfo : public CInterface
    {
    public:
        inline PrefetchInfo(IHThorPrefetchProjectArg &helper, const void *_in, unsigned __int64 _recordCount)
        {
            if (helper.preTransform(extract, _in, _recordCount))
            {
                in.setown(_in);
                recordCount = _recordCount;
            }
            else
                ReleaseRoxieRow(_in);
        }
        OwnedConstRoxieRow in;
        unsigned __int64 recordCount;
        rtlRowBuilder extract;
    };
    QueueOf<PrefetchInfo, true> pulled;
    CriticalSection pulledCrit;

public:
    CRoxieServerPrefetchProjectActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _count)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorPrefetchProjectArg &) basehelper),
        puller(false),
        count(_count)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
        eof = false;
        allPulled = false;
        isThreaded = (helper.getFlags() & PPFparallel) != 0;
        preload = 0;
    }

    ~CRoxieServerPrefetchProjectActivity()
    {
        while (pulled.ordinality())
            ::Release(pulled.dequeue());
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        puller.setInput(this, _in);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = 0;
        recordCount = 0;
        eof = false;
        allPulled = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        preload = helper.getLookahead();
        if (!preload)
            preload = ctx->prefetchProjectPreload();
        space.reinit(preload);
        ready.reinit();
        puller.start(parentExtractSize, parentExtract, paused, preload, !isThreaded, ctx);
    }

    virtual void stop(bool aborting)
    {
        space.interrupt();
        ready.interrupt();
        CRoxieServerActivity::stop(aborting);
        puller.stop(aborting);
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        puller.reset();
        allPulled = false;
        while (pulled.ordinality())
            ::Release(pulled.dequeue());
    }

    virtual PrefetchInfo *readNextRecord()
    {
        if (!isThreaded)
        {
            if (!allPulled) // This looks like it's thread unsafe but we are inside the if(!isThreaded) so should be ok
                puller.pullRecords(1);
        }
        else
            ready.wait();
        CriticalBlock b(pulledCrit);
        PrefetchInfo *ret = pulled.ordinality() ? pulled.dequeue() : NULL;
        space.signal();
        return ret;
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        loop
        {
            Owned<PrefetchInfo> in = readNextRecord();
            if (!in)
            {
                recordCount = 0;
                if (numProcessedLastGroup == processed)
                    in.setown(readNextRecord());
                if (!in)
                {
                    numProcessedLastGroup = processed;
                    eof = true;
                    return NULL;
                }
            }

            try 
            {
                if (in->in)
                {
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    size32_t outSize;
                    IThorChildGraph *child = helper.queryChild();
                    Owned<IEclGraphResults> results;
                    if (child)
                        results.setown(child->evaluate(in->extract.size(), in->extract.getbytes()));
                    outSize = helper.transform(rowBuilder, in->in, results, in->recordCount);
                    if (outSize)
                    {
                        processed++;
                        return rowBuilder.finalizeRowClear(outSize);
                    }
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }

// interface IExceptionHandler
    virtual bool fireException(IException *e)
    {
        // called from puller thread on failure
        ready.interrupt(LINK(e));
        space.interrupt(e);
        return true;
    }
// interface IRecordPullerCallback
    virtual void processRow(const void *row) 
    {
        {
            CriticalBlock b(pulledCrit);
            pulled.enqueue(new PrefetchInfo(helper, row, ++recordCount));
        }
        if (isThreaded)
        {
            ready.signal();
            space.wait();
        }
    }

    virtual void processEOG()
    {
        {
            CriticalBlock b(pulledCrit);
            pulled.enqueue(NULL);
        }
        if (isThreaded)
        {
            ready.signal();
            space.wait();
        }
    }
    virtual void processGroup(const ConstPointerArray &rows)
    {
        throwUnexpected();
    }
    virtual void processDone() 
    {
        CriticalBlock b(pulledCrit);
        allPulled = true;
        if (isThreaded)
            ready.signal();
    }
};

class CRoxieServerPrefetchProjectActivityFactory : public CRoxieServerProjectActivityFactory 
{
public:
    CRoxieServerPrefetchProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerProjectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerPrefetchProjectActivity(this, _probeManager, count);
    }
};

extern IRoxieServerActivityFactory *createRoxieServerPrefetchProjectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerPrefetchProjectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CPointerArrayRoxieInput : public CPseudoRoxieInput
{
public:
    CPointerArrayRoxieInput()
    {
        rowset = NULL;
        rowcount = 0;
        curRow = 0;
    }
    void init(size32_t _rowcount, byte **_rowset)
    {
        rowset = _rowset;
        rowcount = _rowcount;
        curRow = 0;
    }
    virtual const void * nextInGroup()
    {
        if (curRow < rowcount)
        {
            const void * ret = rowset[curRow];
            if (ret)
                LinkRoxieRow(ret);
            curRow++;
            return ret;
        }
        return NULL;
    }
protected:
    byte **rowset;
    size32_t rowcount;
    size32_t curRow;
};

class CRoxieServerLoopActivity : public CRoxieServerActivity
{
protected:
    IHThorLoopArg &helper;
    ThorActivityKind activityKind;
    unsigned maxIterations;
    bool finishedLooping;
    unsigned flags;
    bool eof;
    rtlRowBuilder loopExtractBuilder;
    unsigned loopGraphId;
    Linked<IOutputMetaData> counterMeta;

public:
    CRoxieServerLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _loopGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorLoopArg &)basehelper), loopGraphId(_loopGraphId), counterMeta(_counterMeta)
    {
        eof = false;
        finishedLooping = false;
        activityKind = factory->getKind();
        flags = helper.getFlags();
        maxIterations = 0;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        int iterations = (int) helper.numIterations();
        maxIterations = (iterations >= 0) ? iterations : 0;
        finishedLooping = ((activityKind == TAKloopcount) && (maxIterations == 0));
        if ((flags & IHThorLoopArg::LFnewloopagain) && !helper.loopFirstTime())
            finishedLooping = true;
        loopExtractBuilder.clear();
        helper.createParentExtract(loopExtractBuilder);         // could possibly delay this until execution actually happens
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
        loopExtractBuilder.clear();
    }

    void createCounterResult(IRoxieServerChildGraph * graph, unsigned counter)
    {
        if (flags & IHThorLoopArg::LFcounter)
        {
            void * counterRow = ctx->queryRowManager().allocate(sizeof(thor_loop_counter_t), activityId);
            *((thor_loop_counter_t *)counterRow) = counter;
            RtlLinkedDatasetBuilder builder(rowAllocator);
            builder.appendOwn(counterRow);
            Owned<CGraphResult> counterResult = new CGraphResult(builder.getcount(), builder.linkrows());
            graph->setInputResult(2, counterResult);
        }
    }
};

//=================================================================================

class CRoxieServerSequentialLoopActivity : public CRoxieServerLoopActivity
{
    Owned<IActivityGraph> loopQuery;
    Owned<IRoxieServerChildGraph> loopGraph;
    IRoxieInput * curInput;
    RtlLinkedDatasetBuilder *loopInputBuilder;
    CPointerArrayRoxieInput arrayInput;
    Linked<IRoxieInput> resultInput; 
    unsigned loopCounter;

public:
    CRoxieServerSequentialLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _loopGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerLoopActivity(_factory, _probeManager, _loopGraphId, _counterMeta)
    {
        curInput = NULL;
        loopCounter = 0;
        loopInputBuilder = NULL;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerLoopActivity::onCreate(_ctx, _colocalParent);
        loopQuery.set(_ctx->queryChildGraph(loopGraphId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        curInput = input;
        loopCounter = 1;
        CRoxieServerLoopActivity::start(parentExtractSize, parentExtract, paused);

        //MORE: Not sure about this, should IRoxieServerChildGraph be combined with IActivityGraph?
        loopGraph.set(loopQuery->queryLoopGraph());
        loopInputBuilder = new RtlLinkedDatasetBuilder(rowAllocator);
    }

    virtual void stop(bool aborting)
    {
        delete loopInputBuilder;
        loopInputBuilder = NULL;
        CRoxieServerLoopActivity::stop(aborting);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        unsigned emptyIterations = 0;
        loop
        {
            loop
            {
                const void * ret = curInput->nextInGroup();
                if (!ret)
                {
                    ret = curInput->nextInGroup();      // more cope with groups somehow....
                    if (!ret)
                    {
                        if (finishedLooping)
                        {
                            eof = true;
                            return NULL;
                        }
                        break;
                    }
                }

                if (finishedLooping || 
                    ((flags & IHThorLoopArg::LFfiltered) && !helper.sendToLoop(loopCounter, ret)))
                {
                    processed++;
                    return ret;
                }
                loopInputBuilder->appendOwn(ret);
            }

            switch (activityKind)
            {
            case TAKloopdataset:
                {
                    if (!(flags & IHThorLoopArg::LFnewloopagain))
                    {
                        if (!helper.loopAgain(loopCounter, loopInputBuilder->getcount(), (const void**) loopInputBuilder->queryrows()))
                        {
                            if (loopInputBuilder->getcount() == 0)
                            {
                                eof = true;
                                return NULL;
                            }
                            arrayInput.init(loopInputBuilder->getcount(), loopInputBuilder->linkrows());
                            // MORE - should builder be cleared here?
                            curInput = &arrayInput;
                            finishedLooping = true;
                            continue;       // back to the input loop again
                        }
                    }
                    break;
                }
            case TAKlooprow:
                if (!loopInputBuilder->getcount())
                {
                    finishedLooping = true;
                    eof = true;
                    return NULL;
                }
                break;
            }

            if (loopInputBuilder->getcount())
                emptyIterations = 0;
            else
            {
                //note: any outputs which didn't go around the loop again, would return the record, reinitializing emptyIterations
                emptyIterations++;
                if (emptyIterations > maxEmptyLoopIterations)
                    throw MakeStringException(ROXIE_TOO_MANY_EMPTY_LOOP, "Executed LOOP with empty input and output %u times", emptyIterations);
                if (emptyIterations % 32 == 0)
                    CTXLOG("Executing LOOP with empty input and output %u times", emptyIterations);
            }

            checkAbort();
            try 
            {
                Owned<IRoxieGraphResults> results = executeIteration(loopExtractBuilder.size(), loopExtractBuilder.getbytes(), loopCounter);
                resultInput.setown(results->createIterator(0));

                if (flags & IHThorLoopArg::LFnewloopagain)
                {
                    Owned<IRoxieInput> againResult = results->createIterator(helper.loopAgainResult());
                    OwnedConstRoxieRow row  = againResult->nextInGroup();
                    assertex(row);
                    //Result is a row which contains a single boolean field.
                    if (!((const bool *)row.get())[0])
                        finishedLooping = true;
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }

            curInput = resultInput.get();

            loopCounter++;
            if ((activityKind == TAKloopcount) && (loopCounter > maxIterations))
                finishedLooping = true;
        }
    }

    IRoxieGraphResults * executeIteration(unsigned parentExtractSize, const byte *parentExtract, unsigned counter)
    {
        try
        {
            loopGraph->beforeExecute();

            Owned<IGraphResult> inputRowsResult = new CGraphResult(loopInputBuilder->getcount(), loopInputBuilder->linkrows());
            loopInputBuilder->clear();
            loopGraph->setInputResult(1, inputRowsResult);

            createCounterResult(loopGraph, counter);

            Owned<IRoxieGraphResults> ret = loopGraph->execute(parentExtractSize, parentExtract);
            loopGraph->afterExecute();
            return ret.getClear();
        }
        catch (...)
        {
            CTXLOG("Exception thrown in loop body - cleaning up");
            loopGraph->afterExecute();
            throw;
        }
    }
};

//=================================================================================

typedef SafeQueueOf<const void, true> SafeRowQueue;
typedef SimpleInterThreadQueueOf<const void, true> InterThreadRowQueue;

class CRowQueuePseudoInput : public CPseudoRoxieInput
{
public:
    CRowQueuePseudoInput(SafeRowQueue & _input) : 
        input(_input)
    {
        eof = false;
    }

    virtual const void * nextInGroup()
    {
        if (eof)
            return NULL;
        const void * ret = input.dequeue();
        if (!ret)
            eof = true;
        return ret;
    }

protected:
    SafeRowQueue & input;
    bool eof;
};


class CRoxieServerParallelLoopActivity;
class LoopFilterPseudoInput : public CIndirectRoxieInput
{
public:
    LoopFilterPseudoInput(CRoxieServerParallelLoopActivity * _activity, IRoxieInput * _input, unsigned _counter) : 
        CIndirectRoxieInput(_input), activity(_activity), counter(_counter)
    {
    }

    virtual const void * nextInGroup();

protected:
    CRoxieServerParallelLoopActivity * activity;
    unsigned counter;
};

class LoopExecutorThread : public RestartableThread
{
protected:
    Owned<IRoxieInput> safeInput;
    CRoxieServerParallelLoopActivity * activity;
    bool eof;
    CriticalSection crit;
    unsigned flags;
    SafeRowQueue tempResults[2];
    unsigned savedParentExtractSize;
    const byte * savedParentExtract;
    IArrayOf<IActivityGraph> cachedGraphs;
    IRoxieSlaveContext *ctx;

public:
    LoopExecutorThread() 
        : RestartableThread("LoopExecutorThread")
    {
        activity = NULL;
        eof = false;
        flags = 0;
        ctx = NULL;
        savedParentExtract = NULL;
        savedParentExtractSize = 0;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return safeInput->queryInput(idx);
    }

    void setInput(CRoxieServerParallelLoopActivity * _activity, IRoxieInput *_input, unsigned _flags)
    {
        activity = _activity;
        flags = _flags;
        // stop is called on our consumer's thread. We need to take care calling stop for our input to make sure it is not in mid-nextInGroup etc etc.
        safeInput.setown(new CSafeRoxieInput(_input));
    }

    IRoxieInput *queryInput() const
    {
        return safeInput;
    }

    void onCreate(IRoxieSlaveContext * _ctx);

    void start(unsigned parentExtractSize, const byte *parentExtract, bool paused);
    void stop(bool aborting);
    void reset();

    virtual int run();

protected:
    void executeLoop();
    void executeLoopInstance(unsigned counter, unsigned numIterations, IRoxieInput * input, SafeRowQueue * spillOutput);
    IRoxieInput * createLoopIterationGraph(unsigned i, IRoxieInput * input, unsigned counter);
};


class CRoxieServerParallelLoopActivity : public CRoxieServerLoopActivity
{
    friend class LoopFilterPseudoInput;
    friend class LoopExecutorThread;

    QueueOf<const void, true> ready;
    CriticalSection helperCS;
    CriticalSection cs;
    size32_t sizeNumParallel;
    rtlDataAttr listNumParallel;
    unsigned defaultNumParallel;
    LoopExecutorThread executor;
    IProbeManager* probeManager;
    CriticalSection canAccess;
    CriticalSection scrit;
    InterruptableSemaphore readySpace;
    InterruptableSemaphore recordsReady;

protected:
    bool includeInLoop(unsigned counter, const void * row)
    {
        CriticalBlock b(helperCS);
        return helper.sendToLoop(counter, row);
    }

public:
    CRoxieServerParallelLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _loopGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerLoopActivity(_factory, _probeManager, _loopGraphId, _counterMeta),
          readySpace(parallelLoopFlowLimit)
    {
        probeManager = _probeManager;
        defaultNumParallel = 0;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        executor.onCreate(_ctx);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CriticalBlock b(scrit); // can stop while still starting, if unlucky...
        readySpace.reinit(parallelLoopFlowLimit);
        recordsReady.reinit();    
        CRoxieServerLoopActivity::start(parentExtractSize, parentExtract, paused);
        defaultNumParallel = helper.defaultParallelIterations();
        if (!defaultNumParallel)
            defaultNumParallel = DEFAULT_PARALLEL_LOOP_THREADS;
        helper.numParallelIterations(sizeNumParallel, listNumParallel.refdata());

        //MORE: If numIterations <= number of parallel iterations[1], 
        //then we don't need to create a separate thread to do the processing, and the results will also avoid
        //being transferred via a queue
        executor.start(parentExtractSize, parentExtract, paused);
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        executor.setInput(this, _in, flags);
    }

    virtual void stop(bool aborting)
    {
        CriticalBlock b(scrit); // can stop while still starting, if unlucky...
        readySpace.interrupt();
        recordsReady.interrupt();
        executor.join(); // MORE - may not be needed given stop/reset split
        CRoxieServerLoopActivity::stop(aborting);
    }

    virtual void reset()
    {
        while (ready.ordinality())
            ReleaseRoxieRow(ready.dequeue());
        executor.reset();
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (eof)
                return NULL;

            recordsReady.wait();
            CriticalBlock procedure(canAccess);
            if (ready.ordinality())
            {
                const void *result = ready.dequeue();
                readySpace.signal();
                if (result)
                    processed++;
                return result;
            }
            else
                eof = true;
        }
    }

    unsigned getNumParallel(unsigned iter)
    {
        if (iter * sizeof(unsigned) >= sizeNumParallel)
            return defaultNumParallel;
        return ((unsigned *)listNumParallel.getdata())[iter];
    }

    inline void enqueueResult(const void * row)
    {
        try
        {
            while(!readySpace.wait(1000))
            {
                CTXLOG("Blocked waiting for space in loop %p activity id: %d output queue: %d records in queue", this, queryId(), ready.ordinality());
            }
        }
        catch (...)
        {
            ReleaseRoxieRow(row);
            throw;
        }

        CriticalBlock b2(canAccess);
        ready.enqueue(row);
        recordsReady.signal();
    }

    inline void finishResults()
    {
        recordsReady.signal();
    }

    virtual bool fireException(IException *e)
    {
        readySpace.interrupt(LINK(e));
        recordsReady.interrupt(e);
        return true;
    }

    IActivityGraph * createChildGraphInstance()
    {
        return factory->createChildGraph(ctx, &helper, loopGraphId, this, probeManager, *this);
    }

    IActivityGraph * queryChildGraph()
    {
        return ctx->queryChildGraph(loopGraphId);
    }
};

//=================================================================================

const void * LoopFilterPseudoInput::nextInGroup()
{
    loop
    {
        const void * next = input->nextInGroup();
        if (!next || activity->includeInLoop(counter, next))
            return next;
        activity->enqueueResult(next);
    }
}


void LoopExecutorThread::onCreate(IRoxieSlaveContext * _ctx)
{
    //Initialise the cached graph list with the child instance that will always be created.  Other iterations will be created on demand.
    ctx = _ctx;
    cachedGraphs.append(*LINK(activity->queryChildGraph()));
}


void LoopExecutorThread::start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
{
    savedParentExtractSize = parentExtractSize;
    savedParentExtract = parentExtract;
    eof = false;
    StringBuffer logPrefix("[");
    ctx->getLogPrefix(logPrefix).append("] ");
    RestartableThread::start(logPrefix);
}

int LoopExecutorThread::run()
{
    try
    {
        executeLoop();
    }
    catch (IException *e)
    {
        activity->fireException(e);
    }
    catch (...)
    {
        activity->fireException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unexpected exception caught in LoopExecutorThread::run"));
    }
    return 0;
}

void LoopExecutorThread::stop(bool aborting)
{
    safeInput->stop(aborting);
    RestartableThread::join();
}

void LoopExecutorThread::reset()
{
    safeInput->reset();
}

void LoopExecutorThread::executeLoop()
{
    unsigned iterations = 0;
    unsigned counter = 0;
    unsigned outputIndex = 0;

    //Note, activities don't link inputs, so need to be careful that special inputs remain linked while the activity is executing.
    loop
    {
        if (activity->activityKind == TAKloopcount)
        {
            if (counter == activity->maxIterations)
                break;
        }
        else
        {
            //This condition isn't quite right because it needs to be whether the filtered
            //input is empty.  May be ok if we include that in the semantics,
            if (tempResults[1-outputIndex].ordinality() == 0)
                break;
        }

        unsigned numParallel = activity->getNumParallel(iterations);
        Linked<IRoxieInput> curInput;
        if (iterations == 0)
            curInput.set(safeInput);
        else
        {
            SafeRowQueue & inputQueue = tempResults[1-outputIndex];
            inputQueue.enqueue(NULL);
            curInput.setown(new CRowQueuePseudoInput(inputQueue));
        }

        SafeRowQueue * curOutput = NULL;
        if (counter+numParallel > activity->maxIterations)
            numParallel = activity->maxIterations - counter;
        else if (counter+numParallel < activity->maxIterations)
            curOutput = &tempResults[outputIndex];

        executeLoopInstance(counter, numParallel, curInput, curOutput);
        outputIndex = 1-outputIndex;
        counter += numParallel;
        iterations++;
    }

    //Check for TAKlooprow, where end of loop couldn't be determined ahead of time
    SafeRowQueue & inputQueue = tempResults[1-outputIndex];
    while (inputQueue.ordinality())
    {
        const void * next = inputQueue.dequeue();
        activity->enqueueResult(next);
    }
    activity->finishResults();
}

void LoopExecutorThread::executeLoopInstance(unsigned counter, unsigned numIterations, IRoxieInput * input, SafeRowQueue * spillOutput)
{
    IArrayOf<IRoxieInput> savedInputs;              // activities don't link their inputs, so this list keeps filters alive.
    Linked<IRoxieInput> curInput = input;
    unsigned i;
    for (i= 0; i != numIterations; i++)
    {
        unsigned thisCounter = counter+i+1;
        IRoxieInput * filtered = curInput;
        if (flags & IHThorLoopArg::LFfiltered)
        {
            filtered = new LoopFilterPseudoInput(activity, curInput, thisCounter);
            savedInputs.append(*filtered);
        }
        //graph is kept, so new curInput will be guaranteed to exist
        curInput.setown(createLoopIterationGraph(i, filtered, thisCounter));
    }

    try
    {
        curInput->start(savedParentExtractSize, savedParentExtract, false);
        if (spillOutput)
        {
            loop
            {
                const void * next = curInput->nextInGroup();
                if (!next)
                    break;
                spillOutput->enqueue(next);
            }
        }
        else
        {
            loop
            {
                const void * next = curInput->nextInGroup();
                if (!next)
                    break;
                activity->enqueueResult(next);
            }
        }
    }
    catch (IException *E)
    {
        ctx->notifyAbort(E);
        for (i= 0; i != numIterations; i++)
        {
            cachedGraphs.item(i).queryLoopGraph()->afterExecute();
        }
        curInput->stop(true);
        curInput->reset();
        throw;
    }
    for (i= 0; i != numIterations; i++)
    {
        cachedGraphs.item(i).queryLoopGraph()->afterExecute();
    }
    curInput->stop(false);
    curInput->reset();
}

IRoxieInput * LoopExecutorThread::createLoopIterationGraph(unsigned i, IRoxieInput * input, unsigned counter)
{
    if (!cachedGraphs.isItem(i))
        cachedGraphs.append(*activity->createChildGraphInstance());

    Linked<IRoxieServerChildGraph> loopGraph = cachedGraphs.item(i).queryLoopGraph();
    loopGraph->beforeExecute();
    if (!loopGraph->querySetInputResult(1, input))
        throwUnexpected();      // a loop which doesn't use the value  from the previous iteration.  Should probably handle even if daft.

    activity->createCounterResult(loopGraph, counter);

    return loopGraph->selectOutput(0);
}


//=================================================================================

class CCounterRowMetaData : public CInterface, implements IOutputMetaData
{
public:
    IMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void *)            { return sizeof(thor_loop_counter_t); }
    virtual size32_t getMinRecordSize() const               { return sizeof(thor_loop_counter_t); }
    virtual size32_t getFixedSize() const                   { return sizeof(thor_loop_counter_t); }
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return 0; }
    virtual void destruct(byte * self)  {}
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputMetaData * querySerializedDiskMeta() { return this; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
};

class CRoxieServerLoopActivityFactory : public CRoxieServerActivityFactory
{
    unsigned loopGraphId;
    unsigned flags;

    Linked<IOutputMetaData> counterMeta;

public:
    CRoxieServerLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopGraphId)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), loopGraphId(_loopGraphId)
    {
        Owned<IHThorLoopArg> helper = (IHThorLoopArg *) helperFactory();
        flags = helper->getFlags();
        counterMeta.setown(new CCounterRowMetaData);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (flags & IHThorLoopArg::LFparallel)
            return new CRoxieServerParallelLoopActivity(this, _probeManager, loopGraphId, counterMeta);
        else
            return new CRoxieServerSequentialLoopActivity(this, _probeManager, loopGraphId, counterMeta);
    }
};

IRoxieServerActivityFactory *createRoxieServerLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopGraphId)
{
    return new CRoxieServerLoopActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _loopGraphId);
}

//=================================================================================

class CRoxieServerGraphLoopActivity : public CRoxieServerActivity
{
protected:
    IHThorGraphLoopArg &helper;
    unsigned maxIterations;
    unsigned flags;
    rtlRowBuilder GraphExtractBuilder;
    unsigned loopGraphId;
    Linked<IOutputMetaData> counterMeta;

public:
    CRoxieServerGraphLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _GraphGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorGraphLoopArg &)basehelper), loopGraphId(_GraphGraphId), counterMeta(_counterMeta)
    {
        flags = helper.getFlags();
        maxIterations = 0;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        int iterations = (int) helper.numIterations();
        maxIterations = (iterations >= 0) ? iterations : 0;
        if (maxIterations > maxGraphLoopIterations)
            throw MakeStringException(ROXIE_TOO_MANY_GRAPH_LOOP, "Attempt to execute graph %u times", maxIterations);
        if (maxIterations != 0)
        {
            GraphExtractBuilder.clear();
            helper.createParentExtract(GraphExtractBuilder);
        }
    }

    virtual void stop(bool aborting)
    {
        CRoxieServerActivity::stop(aborting);
        GraphExtractBuilder.clear();
    }

    void createCounterResult(IRoxieServerChildGraph * graph, unsigned counter)
    {
        if (flags & IHThorGraphLoopArg::GLFcounter)
        {
            void * counterRow = ctx->queryRowManager().allocate(sizeof(thor_loop_counter_t), activityId);
            *((thor_loop_counter_t *)counterRow) = counter;
            RtlLinkedDatasetBuilder builder(rowAllocator);
            builder.appendOwn(counterRow);
            Owned<CGraphResult> counterResult = new CGraphResult(builder.getcount(), builder.linkrows());
            graph->setInputResult(0, counterResult);
        }
    }
};

//=================================================================================

class CRoxieServerSequentialGraphLoopActivity : public CRoxieServerGraphLoopActivity
{
    Owned<IActivityGraph> GraphQuery;
    Owned<IRoxieServerChildGraph> loopGraph;
    Linked<IRoxieInput> resultInput; 
    bool evaluated;

public:
    CRoxieServerSequentialGraphLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _GraphGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerGraphLoopActivity(_factory, _probeManager, _GraphGraphId, _counterMeta)
    {
        evaluated = false;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerGraphLoopActivity::onCreate(_ctx, _colocalParent);
        GraphQuery.set(_ctx->queryChildGraph(loopGraphId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerGraphLoopActivity::start(parentExtractSize, parentExtract, paused);

        //MORE: Not sure about this, should IRoxieServerChildGraph be combined with IActivityGraph?
        loopGraph.set(GraphQuery->queryLoopGraph());
        evaluated = false;
    }

    virtual void stop(bool aborting)
    {
        if (loopGraph)
            loopGraph->clearGraphLoopResults();
        CRoxieServerGraphLoopActivity::stop(aborting);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!evaluated)
        {
            executeEntireGraph();
            evaluated = true;
        }

        const void * ret = resultInput->nextInGroup();
        if (ret)
            processed++;
        return ret;
    }

    void executeIteration(unsigned parentExtractSize, const byte *parentExtract, unsigned counter)
    {
        try
        {
            loopGraph->beforeExecute();

            createCounterResult(loopGraph, counter);

            loopGraph->executeGraphLoop(parentExtractSize, parentExtract);
            loopGraph->afterExecute();
        }
        catch (...)
        {
            CTXLOG("Exception thrown in graph body - cleaning up");
            loopGraph->afterExecute();
            throw;
        }
    }

    void createInitialGraphInput()
    {
        loopGraph->clearGraphLoopResults();
        RtlLinkedDatasetBuilder builder(rowAllocator);
        input->readAll(builder);
        Owned<CGraphResult> result = new CGraphResult(builder.getcount(), builder.linkrows());
        loopGraph->setGraphLoopResult(0, result);
    }
    void executeEntireGraph()
    {
        createInitialGraphInput();

        for (unsigned loopCounter=1; loopCounter <= maxIterations; loopCounter++)
        {
            executeIteration(GraphExtractBuilder.size(), GraphExtractBuilder.getbytes(), loopCounter);
        }

        resultInput.setown(loopGraph->getGraphLoopResult(maxIterations));
    }
};

//=================================================================================

struct GraphOutputSplitterArg : public ccdserver_hqlhelper::CThorSplitArg 
{
public:
    virtual unsigned numBranches()
    {
        return 0;
    }
    virtual IOutputMetaData * queryOutputMeta()
    {
        return NULL;// get it from the parent..
    }
};

extern "C" IHThorArg * createGraphOutputSplitter() { return new GraphOutputSplitterArg; }

class CGraphIterationInfo : public CInterface
{
private:
    Owned<IRoxieServerActivityFactory> factory;     // Note - before sourceAct, so destroyed last
    unsigned sourceIdx;
    Linked<IRoxieServerActivity> sourceAct;
    Linked<IRoxieInput> sourceInput;
    unsigned numUses;
    unsigned iteration;

public:
    CGraphIterationInfo(IRoxieServerActivity * _sourceAct, IRoxieInput *_input, unsigned _sourceIdx, unsigned _iteration)
        : sourceAct(_sourceAct), sourceInput(_input), sourceIdx(_sourceIdx), iteration(_iteration)
    {
        numUses = 0;
    }

    inline void noteUsed()
    {
        numUses++;
    }

    void createSplitter(IRoxieSlaveContext *ctx, IProbeManager *probeManager)
    {
        if (numUses > 1)
        {
            factory.setown(createRoxieServerThroughSpillActivityFactory(sourceAct->queryFactory()->queryQueryFactory(), createGraphOutputSplitter, numUses));
            IRoxieServerActivity *splitter =  factory->createActivity(NULL);
            splitter->onCreate(ctx, NULL);
            IRoxieInput *input = sourceAct->queryOutput(sourceIdx);
            if (probeManager)
            {
                IInputBase * inputBase = probeManager->createProbe(static_cast<IInputBase*>(input), sourceAct, splitter, sourceIdx, 0, iteration);
                input = static_cast<IRoxieInput*>(inputBase);
                // MORE - shouldn't this be added to probes?
            }
            sourceAct.setown(splitter);
            sourceAct->setInput(0, input);
            sourceIdx = 0;
            sourceInput.clear();
        }
    }

    IRoxieInput *connectOutput(IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes, IRoxieServerActivity *targetAct, unsigned targetIdx)
    {
        // MORE - not really necessary to create splitters in separate pass, is it?
        if (factory) // we created a splitter....
            sourceInput.set(sourceAct->queryOutput(sourceIdx));
        IRoxieInput *ret = sourceInput;
        if (probeManager)
        {
            IInputBase *inputBase = probeManager->createProbe(ret, sourceAct, targetAct, sourceIdx, targetIdx, iteration);
            ret = static_cast<IRoxieInput *>(inputBase);
            probes.append(*LINK(ret));
        }
        if (factory) // we created a splitter....
            sourceIdx++;
        return ret;
    }
};



class CRoxieServerParallelGraphLoopActivity : public CRoxieServerGraphLoopActivity, implements IRoxieServerLoopResultProcessor
{
    Owned<IActivityGraph> childGraph;
    IRoxieInput * resultInput; 
    CIArrayOf<CGraphIterationInfo> outputs;
    IArrayOf<IRoxieServerChildGraph> iterationGraphs;
    Owned<CExtractMapperInput> inputExtractMapper;
    IProbeManager *probeManager;
    unsigned createLoopCounter;

    IArrayOf<IRoxieInput> probes;

public:
    CRoxieServerParallelGraphLoopActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _GraphGraphId, IOutputMetaData * _counterMeta)
        : CRoxieServerGraphLoopActivity(_factory, _probeManager, _GraphGraphId, _counterMeta), probeManager(_probeManager)
    {
        inputExtractMapper.setown(new CExtractMapperInput);
        resultInput = NULL;
        createLoopCounter = 0;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerGraphLoopActivity::onCreate(_ctx, _colocalParent);
        childGraph.set(_ctx->queryChildGraph(loopGraphId));
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        //Input needs to be handled very carefully.....
        //We don't want to call onStart on the input unless it is actually used, so don't use the base CRoxieServerActivity implementation.
        //This activity's input needs to be started with (parentExtractSize, parentExtract), but the elements in the graph need to be started with the
        //GraphExtractBuilder parent extract.  So we need to wrap the input in a pseudo-input (inputExtractMapper) that passes through a different 
        //parentExtract.  Something very similar will be needed for query library calls with streaming inputs when they are implemented.
        assertex(idx == 0);
        inputExtractMapper->setInput(_in);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerGraphLoopActivity::start(parentExtractSize, parentExtract, paused);         // initialises GraphExtractBuilder
        inputExtractMapper->setParentExtract(parentExtractSize, parentExtract);

        createExpandedGraph(GraphExtractBuilder.size(), GraphExtractBuilder.getbytes(), probeManager);
        resultInput->start(GraphExtractBuilder.size(), GraphExtractBuilder.getbytes(), paused);
    }

    virtual void stop(bool aborting)
    {
        if (resultInput)
            resultInput->stop(aborting);
        CRoxieServerGraphLoopActivity::stop(aborting);
    }

    virtual void reset()
    {
        if (resultInput)
            resultInput->reset();
        resultInput = NULL;
        iterationGraphs.kill();
        outputs.kill();
        if (probeManager)
        {
            probeManager->deleteGraph(NULL, (IArrayOf<IInputBase>*)&probes);
            probes.kill();
        }

        CRoxieServerGraphLoopActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * ret = resultInput->nextInGroup();
        if (ret)
            processed++;
        return ret;
    }

    void createExpandedGraph(unsigned parentExtractSize, const byte *parentExtract, IProbeManager *probeManager)
    {
        //result(0) is the input to the graph.
        resultInput = inputExtractMapper;
        outputs.append(* new CGraphIterationInfo(resultInput->queryActivity(), resultInput, 0, 1));

        for (createLoopCounter=1; createLoopCounter <= maxIterations; createLoopCounter++)
        {
            IRoxieServerChildGraph * graph = childGraph->createGraphLoopInstance(createLoopCounter, parentExtractSize, parentExtract, *this);
            graph->beforeExecute();
            iterationGraphs.append(*graph);
            graph->gatherIterationUsage(*this);
            CGraphIterationInfo *iteration = graph->selectGraphLoopOutput();
            outputs.append(*iteration);
        }
        createLoopCounter = 0;

        createSplitters(probeManager);

        ForEachItemIn(i2, iterationGraphs)
            iterationGraphs.item(i2).associateIterationOutputs(*this);
        resultInput = outputs.tos().connectOutput(probeManager, probes, this, 0);
    }

    void createSplitters(IProbeManager *probeManager)
    {
        ForEachItemIn(i, outputs)
        {
            CGraphIterationInfo & next = outputs.item(i);
            next.createSplitter(ctx, probeManager);
        }
    }

//IRoxieServerLoopResultProcessor
    virtual void noteUseIteration(unsigned _whichIteration)
    {
        int whichIteration = (int) _whichIteration; // May go negative - API is unsigned for historical reasons
        if (whichIteration >= 0)
        {
            if (!outputs.isItem(whichIteration))
                throw MakeStringException(ROXIE_GRAPH_PROCESSING_ERROR, "Error reading graph result %d from iteration %d", whichIteration, createLoopCounter);
            outputs.item(whichIteration).noteUsed();
        }
    }

    virtual IRoxieInput * connectIterationOutput(unsigned whichIteration, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes, IRoxieServerActivity *targetAct, unsigned targetIdx)
    {
        if (outputs.isItem(whichIteration))
        {
            CGraphIterationInfo & next = outputs.item(whichIteration);
            return next.connectOutput(probeManager, probes, targetAct, targetIdx);
        }
        return NULL;
    }
};

//=================================================================================

class CRoxieServerGraphLoopActivityFactory : public CRoxieServerActivityFactory
{
    unsigned loopGraphId;
    unsigned flags;

    Linked<IOutputMetaData> counterMeta;

public:
    CRoxieServerGraphLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopGraphId)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), loopGraphId(_loopGraphId)
    {
        Owned<IHThorGraphLoopArg> helper = (IHThorGraphLoopArg *) helperFactory();
        flags = helper->getFlags();
        counterMeta.setown(new CCounterRowMetaData);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (kind == TAKparallelgraphloop)
            return new CRoxieServerParallelGraphLoopActivity(this, _probeManager, loopGraphId, counterMeta);
        else
            return new CRoxieServerSequentialGraphLoopActivity(this, _probeManager, loopGraphId, counterMeta);
    }
};

IRoxieServerActivityFactory *createRoxieServerGraphLoopActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _loopGraphId)
{
    return new CRoxieServerGraphLoopActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _loopGraphId);
}

//=====================================================================================================

class CRoxieServerLibraryCallActivity : public CRoxieServerActivity
{
    class OutputAdaptor : public CExtractMapperInput
    {
        bool stopped;

    public:
        CRoxieServerLibraryCallActivity *parent;
        unsigned oid;
        unsigned processed;

    public:
        IMPLEMENT_IINTERFACE;

        OutputAdaptor() : CExtractMapperInput(NULL)
        {
            parent = NULL;
            oid = 0;
            init();
        }

        void init()
        {
            processed = 0;
            stopped = false;
        }

        virtual unsigned queryId() const
        {
            return parent->queryId();
        }

        virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
        {
            parent->start(oid, parentExtractSize, parentExtract, paused);
            CExtractMapperInput::start(parentExtractSize, parentExtract, paused);
        }

        virtual void stop(bool aborting) 
        {
            if (!stopped)
            {
                stopped = true;
                parent->stop(oid, aborting);  // parent code relies on stop being called exactly once per adaptor, so make sure it is!
                CExtractMapperInput::stop(aborting);
            }
        };

        virtual void reset()
        {
            parent->reset(oid, processed);
            CExtractMapperInput::reset();
            init();
        };

        virtual void checkAbort()
        {
            parent->checkAbort();
        }
    };
    
    IHThorLibraryCallArg &helper;
    unsigned numInputs;
    unsigned numOutputs;
    unsigned numActiveOutputs;
    bool started;
    OutputAdaptor* outputAdaptors;
    CExtractMapperInput * * inputAdaptors;
    bool * inputUsed;
    bool * outputUsed;
    Owned<IException> error;
    CriticalSection crit;
    rtlRowBuilder libraryExtractBuilder;
    Owned<IActivityGraph> libraryGraph;
    const LibraryCallFactoryExtra & extra;

public:
    CRoxieServerLibraryCallActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs, unsigned _numOutputs, const LibraryCallFactoryExtra & _extra)
        : CRoxieServerActivity(_factory, _probeManager),
        helper((IHThorLibraryCallArg &)basehelper), extra(_extra)
    {
        numInputs = _numInputs;
        numOutputs = _numOutputs;
        numActiveOutputs = numOutputs;
        inputAdaptors = new CExtractMapperInput*[numInputs];
        inputUsed = new bool[numInputs];
        for (unsigned i1 = 0; i1 < numInputs; i1++)
        {
            inputAdaptors[i1] = new CExtractMapperInput;
            inputUsed[i1] = false;
        }

        outputAdaptors = new OutputAdaptor[numOutputs];
        outputUsed = new bool[numOutputs];
        for (unsigned i2 = 0; i2 < numOutputs; i2++)
        {
            outputAdaptors[i2].parent = this;
            outputAdaptors[i2].oid = i2;
            outputUsed[i2] = false;
        }
        started = false;
    }

    ~CRoxieServerLibraryCallActivity()
    {
        for (unsigned i1 = 0; i1 < numInputs; i1++)
            ::Release(inputAdaptors[i1]);
        delete [] inputAdaptors;
        delete [] inputUsed;
        delete [] outputAdaptors;
        delete [] outputUsed;
    }
 
    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        libraryGraph.setown(_ctx->getLibraryGraph(extra, this));
        libraryGraph->onCreate(_ctx, _colocalParent);

        //Now map the inputs and outputs to the adapters
        IRoxieServerChildGraph * graph = libraryGraph->queryLoopGraph();
        for (unsigned i1=0; i1<numInputs; i1++)
            inputUsed[i1] = graph->querySetInputResult(i1, inputAdaptors[i1]);

        for (unsigned i2=0; i2<numOutputs; i2++)
        {
            unsigned outputIndex = extra.outputs.item(i2);
            Owned<IRoxieInput> output = graph->selectOutput(numInputs+outputIndex);
            outputAdaptors[i2].setInput(output);
        }
    }

    virtual void start(unsigned oid, unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CriticalBlock b(crit);
        if (error)
            throw error.getLink();
        if (factory)
            factory->noteStarted(oid);
        if (!started)
        {
            // even though it is not complete, we don't want to run this again if it fails.
            started = true;

            //see notes on splitter above
            try
            {
                CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
            }
            catch (IException *E)
            {
#ifdef TRACE_SPLIT
                CTXLOG("spill %d caught exception in start", activityId);
#endif
                error.set(E);
                throw;
            }
            catch (...)
            {
                IException *E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught in CRoxieServerLibraryCallActivity::start");
                error.set(E);
                throw E;
            }

            //recreate the parent extract, and use it to reinitialize the graphs...
            libraryExtractBuilder.clear();
            helper.createParentExtract(libraryExtractBuilder);

            // NOTE - do NOT set activeOutputs = numOutputs here - we must rely on the value set in reset and constructor. This is because we can see stop on
            // some inputs before we see start on others.
            for (unsigned i1 = 0; i1 < numInputs; i1++)
            {
                if (inputUsed[i1])
                    inputAdaptors[i1]->setParentExtract(parentExtractSize, parentExtract);
                else
                    inputAdaptors[i1]->stop(false);
            }

            for (unsigned i2 = 0; i2 < numOutputs; i2++)
                outputAdaptors[i2].setParentExtract(libraryExtractBuilder.size(), libraryExtractBuilder.getbytes());

            //call stop on all the unused inputs.
            IRoxieServerChildGraph * graph = libraryGraph->queryLoopGraph();
            graph->beforeExecute();
            ForEachItemIn(i3, extra.unusedOutputs)
            {
                Owned<IRoxieInput> output = graph->selectOutput(numInputs+extra.unusedOutputs.item(i3));
                output->stop(false);
            }
        }
    }

    virtual void stop(unsigned oid, bool aborting)  
    {
        CriticalBlock b(crit);
        if (--numActiveOutputs == 0)
        {
            //call stop on all the unused inputs.
            IRoxieServerChildGraph * graph = libraryGraph->queryLoopGraph();
            graph->beforeExecute();
            ForEachItemIn(i3, extra.unusedOutputs)
            {
                Owned<IRoxieInput> output = graph->selectOutput(numInputs+extra.unusedOutputs.item(i3));
                output->stop(false);
            }
            CRoxieServerActivity::stop(aborting);
        }
    }

    void reset(unsigned oid, unsigned _processed)
    {
        noteProcessed(oid, _processed, 0, 0);
        started = false;
        error.clear();
        numActiveOutputs = numOutputs;
        if (state != STATEreset) // make sure input is only reset once
        {
            CRoxieServerActivity::reset();
            libraryGraph->reset();
            //Call reset on all unused outputs from the graph - no one else will.
            IRoxieServerChildGraph * graph = libraryGraph->queryLoopGraph();
            ForEachItemIn(i3, extra.unusedOutputs)
            {
                Owned<IRoxieInput> output = graph->selectOutput(numInputs+extra.unusedOutputs.item(i3));
                output->reset();
            }
        }
    };

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        inputAdaptors[idx]->setInput(_in);
    }

public:

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // Internal logic error - we are not anybody's input
    }

    virtual IOutputMetaData * queryOutputMeta() const
    {
        throwUnexpected();      // should be called on outputs instead
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        assertex(idx!=(unsigned)-1);
        assertex(!outputUsed[idx]);
        outputUsed[idx] = true;
        return &outputAdaptors[idx];
    }
};


void LibraryCallFactoryExtra::set(const LibraryCallFactoryExtra & _other)
{
    ForEachItemIn(i1, _other.outputs)
        outputs.append(_other.outputs.item(i1));
    ForEachItemIn(i2, _other.unusedOutputs)
        unusedOutputs.append(_other.unusedOutputs.item(i2));
    maxOutputs = _other.maxOutputs;
    graphid = _other.graphid;
    libraryName.set(_other.libraryName);
    interfaceHash = _other.interfaceHash;
    embedded = _other.embedded;

}

void LibraryCallFactoryExtra::calcUnused()
{
    for (unsigned i=0; i < maxOutputs; i++)
        if (!outputs.contains(i))
            unusedOutputs.append(i);
}

class CRoxieServerLibraryCallActivityFactory : public CRoxieServerMultiOutputFactory
{
private:
    CRoxieServerMultiInputInfo inputs;
    LibraryCallFactoryExtra extra;

public:
    CRoxieServerLibraryCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, LibraryCallFactoryExtra & _extra)
        : CRoxieServerMultiOutputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        extra.set(_extra);
        extra.calcUnused();
        setNumOutputs(extra.outputs.ordinality());
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLibraryCallActivity(this, _probeManager, numInputs(), numOutputs, extra);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        inputs.set(idx, source, sourceidx);
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        return inputs.get(idx, sourceidx);
    }

    virtual unsigned numInputs() const { return inputs.ordinality(); }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        addXrefLibraryInfo(reply, extra.libraryName);
    }
};

IRoxieServerActivityFactory *createRoxieServerLibraryCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, LibraryCallFactoryExtra & _extra)
{
    return new CRoxieServerLibraryCallActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _extra);
}

//=====================================================================================================

class CRoxieServerNWayInputActivity : public CRoxieServerActivity
{
    IHThorNWayInputArg & helper;
    IRoxieInput ** inputs;
    PointerArrayOf<IRoxieInput> selectedInputs;
    unsigned numInputs;

public:
    CRoxieServerNWayInputActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorNWayInputArg &)basehelper), numInputs(_numInputs)
    {
        inputs = new IRoxieInput*[numInputs];
        for (unsigned i = 0; i < numInputs; i++)
            inputs[i] = NULL;
    }

    ~CRoxieServerNWayInputActivity()
    {
        delete [] inputs;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);

        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper.getInputSelection(selectionIsAll, selectionLen, selection.refdata());

        selectedInputs.kill();
        if (selectionIsAll)
        {
            for (unsigned i=0; i < numInputs; i++)
                selectedInputs.append(inputs[i]);
        }
        else
        {
            const size32_t * selections = (const size32_t *)selection.getdata();
            unsigned max = selectionLen/sizeof(size32_t);
            for (unsigned i = 0; i < max; i++)
            {
                unsigned nextIndex = selections[i];
                //Check there are no duplicates.....  Assumes there are a fairly small number of inputs, so n^2 search is ok.
                for (unsigned j=i+1; j < max; j++)
                {
                    if (nextIndex == selections[j])
                        throw MakeStringException(ROXIE_NWAY_INPUT_ERROR, "Selection list for nway input can not contain duplicates");
                }
                if (nextIndex > numInputs)
                    throw MakeStringException(ROXIE_NWAY_INPUT_ERROR, "Index %d in RANGE selection list is out of range", nextIndex);

                selectedInputs.append(inputs[nextIndex-1]);
            }
        }

        ForEachItemIn(i2, selectedInputs)
            selectedInputs.item(i2)->start(parentExtractSize, parentExtract, paused);
    }

    virtual void stop(bool aborting)
    {
        ForEachItemIn(i2, selectedInputs)
            selectedInputs.item(i2)->stop(aborting);

        CRoxieServerActivity::stop(aborting);
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 localCycles = totalCycles;
        ForEachItemIn(i, selectedInputs)
        {
            localCycles -= selectedInputs.item(i)->queryTotalCycles();
        }
        if (localCycles < 0)
            localCycles = 0;
        return localCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (selectedInputs.isItem(idx))
            return selectedInputs.item(idx);
        else
            return NULL;
    }

    virtual void reset()    
    {
        ForEachItemIn(i, selectedInputs)
            selectedInputs.item(i)->reset();
        selectedInputs.kill();
        CRoxieServerActivity::reset(); 
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        assertex(idx < numInputs);
        inputs[idx] = _in;
    }

    virtual const void * nextInGroup()
    {
        throwUnexpected();
    }

    virtual unsigned numConcreteOutputs() const
    {
        return selectedInputs.ordinality();
    }

    virtual IRoxieInput * queryConcreteInput(unsigned idx)
    {
        if (selectedInputs.isItem(idx))
            return selectedInputs.item(idx);
        return NULL;
    }
};

class CRoxieServerNWayInputActivityFactory : public CRoxieServerMultiInputFactory
{
//    bool ordered;
public:
    CRoxieServerNWayInputActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNWayInputActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerNWayInputActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNWayInputActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerNWayGraphLoopResultReadActivity : public CRoxieServerActivity
{
    IHThorNWayGraphLoopResultReadArg & helper;
    CIArrayOf<CRoxieServerActivity> resultReaders;
    PointerArrayOf<IRoxieInput> inputs;
    unsigned graphId;
    bool grouped;
    bool selectionIsAll;
    size32_t selectionLen;
    rtlDataAttr selection;

public:
    CRoxieServerNWayGraphLoopResultReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _graphId)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorNWayGraphLoopResultReadArg &)basehelper)
    {
        grouped = helper.isGrouped();
        graphId = _graphId;
        selectionIsAll = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);

        if (inputs.ordinality() == 0)
        {
            initInputSelection();

            unsigned max = selectionLen / sizeof(size32_t);
            const size32_t * selections = (const size32_t *)selection.getdata();
            IProbeManager * probeManager = NULL;        // MORE!!
            for (unsigned i = 0; i < max; i++)
            {
                CRoxieServerActivity * resultInput = new CRoxieServerInternalGraphLoopResultReadActivity(factory, probeManager, graphId, selections[i]);
                resultReaders.append(*resultInput);
                inputs.append(resultInput->queryOutput(0));
                resultInput->onCreate(ctx, colocalParent);
                resultInput->start(parentExtractSize, parentExtract, paused);
            }
        }
        else
        {
            ForEachItemIn(i, inputs)
                inputs.item(i)->start(parentExtractSize, parentExtract, paused);
        }
    }

    virtual void stop(bool aborting)
    {
        ForEachItemIn(i, inputs)
            inputs.item(i)->stop(aborting);

        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()    
    {
        ForEachItemIn(i, inputs)
            inputs.item(i)->reset();
        inputs.kill();
        resultReaders.kill();
        CRoxieServerActivity::reset(); 
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for nway graph result read");
    }

    virtual const void * nextInGroup()
    {
        throwUnexpected();
    }

    virtual unsigned numConcreteOutputs() const
    {
        return inputs.ordinality();
    }

    virtual IRoxieInput * queryConcreteInput(unsigned idx)
    {
        if (inputs.isItem(idx))
            return inputs.item(idx);
        return NULL;
    }

    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract)
    {
        ensureCreated();
        basehelper.onStart(parentExtract, NULL);
        initInputSelection();

        unsigned max = selectionLen / sizeof(size32_t);
        const size32_t * selections = (const size32_t *)selection.getdata();
        for (unsigned i = 0; i < max; i++)
            processor.noteUseIteration(selections[i]);
    }

    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor, unsigned parentExtractSize, const byte * parentExtract, IProbeManager *probeManager, IArrayOf<IRoxieInput> &probes)
    {
        //selection  etc. already initialised from the gratherIterationUsage() call.
        unsigned max = selectionLen / sizeof(size32_t);
        const size32_t * selections = (const size32_t *)selection.getdata();
        for (unsigned i = 0; i < max; i++)
            inputs.append(processor.connectIterationOutput(selections[i], probeManager, probes, this, i));
    }

protected:
    void initInputSelection()
    {
        helper.getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        if (selectionIsAll)
            throw MakeStringException(ROXIE_NWAY_INPUT_ERROR, "ALL not yet supported for NWay graph inputs");
    }
};


class CRoxieServerNWayGraphLoopResultReadActivityFactory : public CRoxieServerActivityFactory
{
     unsigned graphId;

public:
    CRoxieServerNWayGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _graphId)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), graphId(_graphId)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNWayGraphLoopResultReadActivity(this, _probeManager, graphId);
    }
    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for NWay GraphLoopResultRead activity");
    }
};

IRoxieServerActivityFactory *createRoxieServerNWayGraphLoopResultReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned graphId)
{
    return new CRoxieServerNWayGraphLoopResultReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, graphId);
}

//=================================================================================


class RoxieSteppedInput : public CInterface, implements ISteppedInput
{
public:
    RoxieSteppedInput(IRoxieInput * _input) { input = _input; }
    IMPLEMENT_IINTERFACE

protected:
    virtual const void * nextInputRow()
    {
#ifdef TRACE_SEEK_REQUESTS
        IRoxieContextLogger * logger = input->queryActivity();
        const void * ret = doNextInputRow();
        {
            CommonXmlWriter xmlwrite(XWFtrim|XWFopt|XWFnoindent);
            if (!ret)
                xmlwrite.outputBool(true,"eof");
            else if (input->queryOutputMeta()->hasXML())
                input->queryOutputMeta()->toXML((byte *) ret, xmlwrite);
            logger->CTXLOG("next() returns (%s)", xmlwrite.str());
        }
        return ret;
#else
        return doNextInputRow();
#endif
    }

    virtual const void * nextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
#ifdef TRACE_SEEK_REQUESTS
        IRoxieContextLogger * logger = input->queryActivity();
        {
            CommonXmlWriter xmlwrite(XWFtrim|XWFopt|XWFnoindent);
            if (input->queryOutputMeta()->hasXML())
                input->queryOutputMeta()->toXML((byte *) seek, xmlwrite);
            logger->CTXLOG("nextInputRowGE(%d, %s%s%s, %s) seek(%s)", 
                    numFields, 
                    stepExtra.readAheadManyResults() ? "readahead " : "",  
                    stepExtra.returnMismatches() ? "mismatch" : "exact",
                    stepExtra.onlyReturnFirstSeekMatch() ? " single-match" : "",
                    stepExtra.queryExtraSeeks() ? "multi-seek":"", 
                    xmlwrite.str());
        }
        const void * ret = doNextInputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        {
            CommonXmlWriter xmlwrite(XWFtrim|XWFopt|XWFnoindent);
            if (!ret)
                xmlwrite.outputBool(true,"eof");
            else if (input->queryOutputMeta()->hasXML())
                input->queryOutputMeta()->toXML((byte *) ret, xmlwrite);
            logger->CTXLOG("nextInputRowGE(%d, %s%s%s, %s) result(%s)", 
                    numFields, 
                    stepExtra.readAheadManyResults() ? "readahead " : "",  
                    stepExtra.returnMismatches() ? "mismatch" : "exact",
                    stepExtra.onlyReturnFirstSeekMatch() ? " single-match" : "",
                    stepExtra.queryExtraSeeks() ? "multi-seek":"", 
                    xmlwrite.str());
        }
        return ret;
#else
        return doNextInputRowGE(seek, numFields, wasCompleteMatch, stepExtra);
#endif
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); 
    }

    virtual IInputSteppingMeta * queryInputSteppingMeta()
    {
        return input->querySteppingMeta();
    }

    inline const void * doNextInputRow()
    {
        const void * ret = input->nextInGroup();
        if (!ret)
            ret = input->nextInGroup();
        return ret;
    }

    inline const void * doNextInputRowGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        assertex(wasCompleteMatch);
        return input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
    }


protected:
    IRoxieInput * input;
};

//=================================================================================

class CRoxieServerNaryActivity : public CRoxieServerMultiInputActivity
{
public:
    CRoxieServerNaryActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputActivity(_factory, _probeManager, _numInputs)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerMultiInputActivity::start(parentExtractSize, parentExtract, paused);
        for (unsigned i=0; i < numInputs; i++)
        {
            IRoxieInput * cur = inputArray[i];
            unsigned numRealInputs = cur->numConcreteOutputs();
            for (unsigned j = 0; j < numRealInputs; j++)
            {
                IRoxieInput * curReal = cur->queryConcreteInput(j);
                expandedInputs.append(curReal);
            }
        }
    }

    virtual void reset()    
    {
        expandedInputs.kill();
        CRoxieServerMultiInputActivity::reset(); 
    }

protected:
    PointerArrayOf<IRoxieInput> expandedInputs;
};


//=================================================================================

class CRoxieStreamMerger : public CStreamMerger
{
public:
    CRoxieStreamMerger() : CStreamMerger(true) {}

    void initInputs(unsigned _numInputs, IRoxieInput ** _inputArray)
    {
        CStreamMerger::initInputs(_numInputs);
        inputArray = _inputArray;
    }

    virtual bool pullInput(unsigned i, const void * seek, unsigned numFields, const SmartStepExtra * stepExtra)
    {
        const void * next;
        bool matches = true;
        if (seek)
            next = inputArray[i]->nextSteppedGE(seek, numFields, matches, *stepExtra);
        else
            next = nextUngrouped(inputArray[i]);
        pending[i] = next;
        pendingMatches[i] = matches;
        return (next != NULL);
    }

    virtual void releaseRow(const void * row)
    {
        ReleaseRoxieRow(row);
    }

protected:
    IRoxieInput **inputArray;
};


class CRoxieServerNWayMergeActivity : public CRoxieServerNaryActivity
{
public:
    CRoxieServerNWayMergeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerNaryActivity(_factory, _probeManager, _numInputs),
        helper((IHThorNWayMergeArg &)basehelper) 
    {
        initializedMeta = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerNaryActivity::start(parentExtractSize, parentExtract, paused);
        merger.init(helper.queryCompare(), helper.dedup(), helper.querySteppingMeta()->queryCompare());
        merger.initInputs(expandedInputs.length(), expandedInputs.getArray());
    }

    virtual void stop(bool aborting)
    {
        merger.done();
        CRoxieServerNaryActivity::stop(aborting);
    }

    virtual void reset()    
    {
        merger.cleanup();
        CRoxieServerNaryActivity::reset(); 
        initializedMeta = false;
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = merger.nextRow();
        if (next)
            processed++;
        return next;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool & wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = merger.nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (next)
            processed++;
        return next;
    }

    virtual IInputSteppingMeta * querySteppingMeta()
    {
        if (expandedInputs.ordinality() == 0)
            return NULL;
        if (!initializedMeta)
        {
            meta.init(helper.querySteppingMeta(), false);
            ForEachItemIn(i, expandedInputs)
            {
                if (meta.getNumFields() == 0)
                    break;
                IInputSteppingMeta * inputMeta = expandedInputs.item(i)->querySteppingMeta();
                meta.intersect(inputMeta);
            }
            initializedMeta = true;
        }
        if (meta.getNumFields() == 0)
            return NULL;
        return &meta;
    }

protected:
    IHThorNWayMergeArg &helper;
    CRoxieStreamMerger merger;
    CSteppingMeta meta;
    bool initializedMeta;
};


class CRoxieServerNWayMergeActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerNWayMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNWayMergeActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerNWayMergeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNWayMergeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerNWayMergeJoinActivity : public CRoxieServerNaryActivity
{
public:
    CRoxieServerNWayMergeJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs, CMergeJoinProcessor & _processor)
        : CRoxieServerNaryActivity(_factory, _probeManager, _numInputs),processor(_processor),
        helper((IHThorNWayMergeJoinArg &)basehelper) 
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerNaryActivity::start(parentExtractSize, parentExtract, paused);
        ForEachItemIn(i1, expandedInputs)
        {
            IRoxieInput * cur = expandedInputs.item(i1);
            Owned<RoxieSteppedInput> stepInput = new RoxieSteppedInput(cur);
            processor.addInput(stepInput);
        }

        ICodeContext * codectx = ctx->queryCodeContext();
        Owned<IEngineRowAllocator> inputAllocator = codectx->getRowAllocator(helper.queryInputMeta(), activityId);
        Owned<IEngineRowAllocator> outputAllocator = codectx->getRowAllocator(helper.queryOutputMeta(), activityId);
        processor.beforeProcessing(inputAllocator, outputAllocator);
    }

    virtual void stop(bool aborting)
    {
        processor.afterProcessing();
        CRoxieServerNaryActivity::stop(aborting);
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector)
    {
        return processor.gatherConjunctions(collector);
    }

    virtual void resetEOF() 
    { 
        processor.queryResetEOF(); 
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = processor.nextInGroup();
        if (next)
            processed++;
        return next;
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * next = processor.nextGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (next)
            processed++;
        return next;
    }

    virtual IInputSteppingMeta * querySteppingMeta()
    {
        return processor.queryInputSteppingMeta();
    }

protected:
    IHThorNWayMergeJoinArg & helper;
    CMergeJoinProcessor & processor;
};


class CRoxieServerAndMergeJoinActivity : public CRoxieServerNWayMergeJoinActivity
{
public:
    CRoxieServerAndMergeJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerNWayMergeJoinActivity(_factory, _probeManager, _numInputs, andProcessor), andProcessor(helper)
    {
    }

protected:
    CAndMergeJoinProcessor andProcessor;
};

class CRoxieServerAndLeftMergeJoinActivity : public CRoxieServerNWayMergeJoinActivity
{
public:
    CRoxieServerAndLeftMergeJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerNWayMergeJoinActivity(_factory, _probeManager, _numInputs, andLeftProcessor), andLeftProcessor(helper)
    {
    }

protected:
    CAndLeftMergeJoinProcessor andLeftProcessor;
};

class CRoxieServerMofNMergeJoinActivity : public CRoxieServerNWayMergeJoinActivity
{
public:
    CRoxieServerMofNMergeJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerNWayMergeJoinActivity(_factory, _probeManager, _numInputs, mofnProcessor), mofnProcessor(helper)
    {
    }

protected:
    CMofNMergeJoinProcessor mofnProcessor;
};

class CRoxieServerProximityJoinActivity : public CRoxieServerNWayMergeJoinActivity
{
public:
    CRoxieServerProximityJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerNWayMergeJoinActivity(_factory, _probeManager, _numInputs, proximityProcessor), proximityProcessor(helper)
    {
    }

protected:
    CProximityJoinProcessor proximityProcessor;
};

class CRoxieServerNWayMergeJoinActivityFactory : public CRoxieServerMultiInputFactory
{
    unsigned flags;
public:
    CRoxieServerNWayMergeJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorNWayMergeJoinArg> helper = (IHThorNWayMergeJoinArg *) helperFactory();
        flags = helper->getJoinFlags();
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (flags & IHThorNWayMergeJoinArg::MJFhasrange)
            return new CRoxieServerProximityJoinActivity(this, _probeManager, numInputs());

        switch (flags & IHThorNWayMergeJoinArg::MJFkindmask)
        {
        case IHThorNWayMergeJoinArg::MJFinner:
            return new CRoxieServerAndMergeJoinActivity(this, _probeManager, numInputs());
        case IHThorNWayMergeJoinArg::MJFleftonly:
        case IHThorNWayMergeJoinArg::MJFleftouter:
            return new CRoxieServerAndLeftMergeJoinActivity(this, _probeManager, numInputs());
        case IHThorNWayMergeJoinArg::MJFmofn:
            return new CRoxieServerMofNMergeJoinActivity(this, _probeManager, numInputs());
        default:
            throwUnexpected();
        }
    }
};

IRoxieServerActivityFactory *createRoxieServerNWayMergeJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNWayMergeJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}


//=================================================================================

class CRoxieServerNWaySelectActivity : public CRoxieServerMultiInputActivity
{
    IHThorNWaySelectArg &helper;
public:
    CRoxieServerNWaySelectActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerMultiInputActivity(_factory, _probeManager, _numInputs),
          helper((IHThorNWaySelectArg &)basehelper)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerMultiInputActivity::start(parentExtractSize, parentExtract, paused);

        unsigned whichInput = helper.getInputIndex();
        selectedInput = NULL;
        if (whichInput--)
        {
            for (unsigned i=0; i < numInputs; i++)
            {
                IRoxieInput * cur = inputArray[i];
                unsigned numRealInputs = cur->numConcreteOutputs();
                if (whichInput < numRealInputs)
                {
                    selectedInput = cur->queryConcreteInput(whichInput);
                    break;
                }
                whichInput -= numRealInputs;
            }
        }
    }

    virtual void reset()    
    {
        selectedInput = NULL;
        CRoxieServerMultiInputActivity::reset(); 
    }

    const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!selectedInput)
            return NULL;
        return selectedInput->nextInGroup();
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        if (!selectedInput)
            return false;
        return selectedInput->gatherConjunctions(collector); 
    }
    
    virtual void resetEOF() 
    { 
        if (selectedInput)
            selectedInput->resetEOF(); 
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!selectedInput)
            return NULL;
        return selectedInput->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        if (selectedInput)
            return selectedInput->querySteppingMeta();
        return NULL;
    }

protected:
    IRoxieInput * selectedInput;
};


class CRoxieServerNWaySelectActivityFactory : public CRoxieServerMultiInputFactory
{
public:
    CRoxieServerNWaySelectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerNWaySelectActivity(this, _probeManager, numInputs());
    }
};

IRoxieServerActivityFactory *createRoxieServerNWaySelectActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerNWaySelectActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerRemoteActivity : public CRoxieServerActivity, implements IRoxieServerErrorHandler
{
protected:
    IHThorRemoteArg &helper;
    CRemoteResultAdaptor remote;

public:
    CRoxieServerRemoteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteID)
        : CRoxieServerActivity(_factory, _probeManager), 
          helper((IHThorRemoteArg &)basehelper),
          remote(_remoteID, meta.queryOriginal(), helper, *this, false, false) // MORE - if they need it stable we'll have to think!
    {
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return NULL;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        remote.onCreate(this, this, _ctx, _colocalParent);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        remote.onStart(parentExtractSize, parentExtract);

        remote.setLimits(helper.getRowLimit(), (unsigned __int64) -1, I64C(0x7FFFFFFFFFFFFFFF));
        unsigned fileNo = 0;        // MORE - superfiles require us to do this per file part... maybe (needs thought)
        remote.getMem(0, fileNo, 0);  // the cached context is all we need to send

        remote.flush();
        remote.senddone();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL: &remote;
    }

    virtual void reset()
    {
        processed = remote.processed;
        remote.processed = 0;
        CRoxieServerActivity::reset();
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        helper.onLimitExceeded();
    }
    virtual const void *createLimitFailRow(bool isKeyed)
    {
        UNIMPLEMENTED; // MORE - is there an ONFAIL for a limit folded into a remote?
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

class CRoxieServerRemoteActivityFactory : public CRoxieServerActivityFactory
{
public:
    RemoteActivityId remoteId;
    bool isRoot;

    CRoxieServerRemoteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), remoteId(_remoteId), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerRemoteActivity(this, _probeManager, remoteId);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for %s activity", getActivityText(kind));
    }

    virtual bool isSink() const
    {
        //I don't think the action version of this is implemented - but this would be the code
        return isRoot && !meta.queryOriginal();
    }
};

IRoxieServerActivityFactory *createRoxieServerRemoteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, bool _isRoot)
{
    return new CRoxieServerRemoteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _isRoot);
}

//=================================================================================

class CRoxieServerIterateActivity : public CRoxieServerActivity
{
    IHThorIterateArg &helper;
    OwnedConstRoxieRow defaultRecord;
    OwnedConstRoxieRow left;
    OwnedConstRoxieRow right;
    unsigned counter;

public:
    CRoxieServerIterateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager),
          helper((IHThorIterateArg &)basehelper)
    {
        counter = 0;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        counter = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t thisSize = helper.createDefault(rowBuilder);
        defaultRecord.setown(rowBuilder.finalizeRowClear(thisSize));
    }

    virtual void reset()    
    {
        defaultRecord.clear();
        right.clear();
        left.clear();
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            right.setown(input->nextInGroup());
            if (!right)
            {
                bool skippedGroup = (left == NULL) && (counter > 0); //we have just skipped entire group, but shouldn't output a double null
                left.clear();
                counter = 0;
                if (skippedGroup) continue;
                return NULL;
            }
            try
            {
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                unsigned outSize = helper.transform(rowBuilder, left ? left : defaultRecord, right, ++counter);
                if (outSize)
                {
                    left.setown(rowBuilder.finalizeRowClear(outSize));
                    processed++;
                    return left.getLink();
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
        }
    }
};

class CRoxieServerIterateActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerIterateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerIterateActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerIterateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerIterateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerProcessActivity : public CRoxieServerActivity
{
    IHThorProcessArg &helper;
    OwnedConstRoxieRow curRight;
    OwnedConstRoxieRow initialRight;
    unsigned counter;
    Owned<IEngineRowAllocator> rightRowAllocator;

public:
    CRoxieServerProcessActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorProcessArg &)basehelper)
    {
        counter = 0;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        rightRowAllocator.setown(ctx->queryCodeContext()->getRowAllocator(QUERYINTERFACE(helper.queryRightRecordSize(), IOutputMetaData), activityId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        counter = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);

        RtlDynamicRowBuilder rowBuilder(rightRowAllocator);
        size32_t thisSize = helper.createInitialRight(rowBuilder);
        initialRight.setown(rowBuilder.finalizeRowClear(thisSize));
        curRight.set(initialRight);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());

        try
        {
            loop
            {
                const void * in = input->nextInGroup();
                if (!in)
                {
                    bool eog = (curRight != initialRight);          // processed any records?
                    counter = 0;
                    curRight.set(initialRight);
                    if (eog)
                        return NULL;

                    in = input->nextInGroup();
                    if (!in)
                        return NULL;
                }

                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                RtlDynamicRowBuilder rightRowBuilder(rightRowAllocator);
                size32_t outSize = helper.transform(rowBuilder, rightRowBuilder, in, curRight, ++counter);
                ReleaseRoxieRow(in);

                if (outSize)
                {
                    //MORE: This should be returned...
                    size32_t rightSize = rightRowAllocator->queryOutputMeta()->getRecordSize(rightRowBuilder.getSelf());
                    curRight.setown(rightRowBuilder.finalizeRowClear(rightSize));
                    processed++;
                    return rowBuilder.finalizeRowClear(outSize);
                }
            }
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }
};

class CRoxieServerProcessActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerProcessActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerProcessActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerProcessActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerProcessActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerGroupActivity : public CRoxieServerActivity
{
    IHThorGroupArg &helper;
    bool endPending;
    bool eof;
    bool first;
    const void *next;

public:
    CRoxieServerGroupActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorGroupArg &)basehelper)
    {
        next = NULL;
        endPending = false;
        eof = false;
        first = true;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        endPending = false;
        eof = false;
        first = true;
        assertex(next == NULL);
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()    
    { 
        ReleaseClearRoxieRow(next);
        CRoxieServerActivity::reset();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (first)
        {
            next = input->nextInGroup();
            first = false;
        }
        if (eof || endPending)
        {
            endPending = false;
            return NULL;
        }

        const void * prev = next;
        next = input->nextInGroup();
        if (!next)
            next = input->nextInGroup();

        if (next)
        {
            assertex(prev);
            if (!helper.isSameGroup(prev, next))
                endPending = true;
        }
        else
            eof = true;
        if (prev)
            processed++;
        return prev;
    }
};

class CRoxieServerGroupActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerGroupActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerGroupActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerGroupActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerFirstNActivity : public CRoxieServerLateStartActivity
{
    unsigned __int64 limit;
    unsigned __int64 skip;
    unsigned doneThisGroup;
    IHThorFirstNArg &helper;

public:
    CRoxieServerFirstNActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerLateStartActivity(_factory, _probeManager), helper((IHThorFirstNArg &)basehelper)
    {
        doneThisGroup = 0;
        limit = 0;
        skip = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        doneThisGroup = 0;
        CRoxieServerLateStartActivity::start(parentExtractSize, parentExtract, paused);
        limit = helper.getLimit();
        skip = helper.numToSkip();
        lateStart(parentExtractSize, parentExtract, limit > 0);
        if (limit + skip >= limit)
            limit += skip;
    }

    const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        const void *ret;
        loop
        {
            ret = input->nextInGroup();
            if (!ret)
            {
                if (meta.isGrouped())
                {
                    if (doneThisGroup > skip)
                    {
                        doneThisGroup = 0;
                        return NULL;
                    }
                    doneThisGroup = 0;
                }
                ret = input->nextInGroup();
                if (!ret)
                {
                    eof = true;
                    return NULL;
                }
            }
            doneThisGroup++;
            if (doneThisGroup > skip)
                break;
            ReleaseRoxieRow(ret);
        }

        if (doneThisGroup <= limit)
        {
            processed++;
            return ret;
        }

        ReleaseRoxieRow(ret);
        if (meta.isGrouped())
        {
            while ((ret = input->nextInGroup()) != NULL)
                ReleaseRoxieRow(ret);
            doneThisGroup = 0;
        }
        else
            eof = true;
        return NULL;
    }
};

class CRoxieServerFirstNActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerFirstNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerFirstNActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerFirstNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerFirstNActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerSelectNActivity : public CRoxieServerActivity
{
    bool done;
    IHThorSelectNArg &helper;

public:
    CRoxieServerSelectNActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSelectNArg &)basehelper)
    {
        done = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    const void *defaultRow()
    {
        if (!rowAllocator)
            createRowAllocator();      // We delay as often not needed...
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t thisSize = helper.createDefault(rowBuilder);
        return rowBuilder.finalizeRowClear(thisSize);
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done)
            return NULL;
        done = true;
        processed++; // always going to return a row!
        unsigned __int64 index = helper.getRowToSelect();
        while (--index)
        {
            const void * next = input->nextInGroup();
            if (!next)
                next = input->nextInGroup();
            if (!next)
                return defaultRow();
            ReleaseRoxieRow(next);
        }

        const void * next = input->nextInGroup();
        if (!next)
            next = input->nextInGroup();
        if (!next)
            next = defaultRow();

        return next;
    }
};

class CRoxieServerSelectNActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSelectNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSelectNActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerSelectNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSelectNActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerSelfJoinActivity : public CRoxieServerActivity
{
    IHThorJoinArg &helper;
    ICompare *collate;
    OwnedRowArray group;
    bool matchedLeft;
    BoolArray matchedRight;
    bool eof;
    bool first;
    unsigned leftIndex;
    unsigned rightIndex;
    unsigned rightOuterIndex;
    unsigned joinLimit;
    unsigned atmostLimit;
    unsigned abortLimit;
    unsigned keepLimit;
    bool leftOuterJoin;
    bool rightOuterJoin;
    bool exclude;
    bool limitFail;
    bool limitOnFail;
    bool cloneLeft;
    OwnedConstRoxieRow defaultLeft;
    OwnedConstRoxieRow defaultRight;
    OwnedConstRoxieRow lhs;
    Owned<IException> failingLimit;
    bool failingOuterAtmost;
    Owned<IEngineRowAllocator> defaultAllocator;
    Owned<IRHLimitedCompareHelper> limitedhelper;
    Owned<CRHDualCache> dualcache;
    IInputBase *dualCacheInput;

    bool fillGroup()
    {
        group.clear();
        matchedLeft = false;
        matchedRight.kill();
        failingOuterAtmost = false;
        const void * next;
        unsigned groupCount = 0;
        while((next = input->nextInGroup()) != NULL)
        {
            if(groupCount==abortLimit)
            {
                if(limitFail)
                    failLimit(next);
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                if(limitOnFail)
                {
                    assertex(!failingLimit);
                    try
                    {
                        failLimit(next);
                    }
                    catch(IException * except)
                    {
                        failingLimit.setown(except);
                    }
                    assertex(failingLimit != NULL);
                    group.append(next);
                    groupCount++;
                    break;
                }
                group.clear();
                groupCount = 0;
                while(next) 
                {
                    ReleaseRoxieRow(next);
                    next = input->nextInGroup();
                }
            }
            else if(groupCount==atmostLimit)
            {
                if(leftOuterJoin)
                {
                    group.append(next);
                    groupCount++;
                    failingOuterAtmost = true;
                    break;
                }
                else
                {
                    group.clear();
                    groupCount = 0;
                    while (next) 
                    {
                        ReleaseRoxieRow(next);
                        next = input->nextInGroup();
                    }
                }
            }
            else
            {
                group.append(next);
                groupCount++;
            }
        }
        if(group.ordinality()==0)
        {
            eof = true;
            return false;
        }
        leftIndex = 0;
        rightIndex = 0;
        rightOuterIndex = 0;
        joinLimit = keepLimit;
        ForEachItemIn(idx, group)
            matchedRight.append(false);
        return true;
    }

    void failLimit(const void * next)
    {
        helper.onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(XWFtrim|XWFopt );
        if (!ctx->isBlind() && input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
        {
            input->queryOutputMeta()->toXML((byte *) next, xmlwrite);
        }
        throw MakeStringException(ROXIE_TOO_MANY_RESULTS, "More than %d match candidates in self-join %d for row %s", abortLimit, queryId(), xmlwrite.str());
    }

    virtual bool needsAllocator() const { return true; }

    const void *joinRecords(const void * curLeft, const void * curRight, IException * except = NULL)
    {
        try
        {
            if (cloneLeft && !except)
            {
                LinkRoxieRow(curLeft);
                return curLeft;
            }
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outsize = except ? helper.onFailTransform(rowBuilder, curLeft, curRight, except) : helper.transform(rowBuilder, curLeft, curRight);
            if (outsize)
                return rowBuilder.finalizeRowClear(outsize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    void createDefaultLeft()
    {
        if (!defaultLeft)
        {
            if (!defaultAllocator)
                defaultAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultAllocator);
            size32_t thisSize = helper.createDefaultLeft(rowBuilder);
            defaultLeft.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

    void createDefaultRight()
    {
        if (!defaultRight)
        {
            if (!defaultAllocator)
                defaultAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultAllocator);
            size32_t thisSize = helper.createDefaultRight(rowBuilder);
            defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

public:
    CRoxieServerSelfJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorJoinArg &)basehelper)
    {
        collate = helper.queryCompareLeftRight();
        eof = false;
        first = true;
        keepLimit = 0;
        atmostLimit = 0;
        unsigned joinFlags = helper.getJoinFlags();
        leftOuterJoin = (joinFlags & JFleftouter) != 0;
        rightOuterJoin = (joinFlags & JFrightouter) != 0;
        cloneLeft = (joinFlags & JFtransformmatchesleft) != 0;
        exclude = (joinFlags & JFexclude) != 0;
        abortLimit = 0;
        joinLimit = 0;
        assertex((joinFlags & (JFfirst | JFfirstleft | JFfirstright)) == 0); // no longer supported
        getLimitType(joinFlags, limitFail, limitOnFail);
        if((joinFlags & JFslidingmatch) != 0)
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Internal Error: Sliding self join not supported");
        failingOuterAtmost = false;
        matchedLeft = false;
        leftIndex = 0;
        rightIndex = 0;
        rightOuterIndex = 0;
        dualCacheInput = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        first = true;
        failingLimit.clear();
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        keepLimit = helper.getKeepLimit();
        if(keepLimit == 0)
            keepLimit = (unsigned)-1;
        atmostLimit = helper.getJoinLimit();
        if(atmostLimit == 0)
            atmostLimit = (unsigned)-1;
        else
            assertex(!rightOuterJoin);
        abortLimit = helper.getMatchAbortLimit();
        if (abortLimit == 0) 
            abortLimit = (unsigned)-1;
        if (rightOuterJoin)
            createDefaultLeft();
        if (leftOuterJoin || limitOnFail)
            createDefaultRight();
        if ((helper.getJoinFlags() & JFlimitedprefixjoin) && helper.getJoinLimit()) 
        {   //limited match join (s[1..n])
            dualcache.setown(new CRHDualCache());
            dualcache->init(CRoxieServerActivity::input);
            dualCacheInput = dualcache->queryOut1();
            failingOuterAtmost = false;
            matchedLeft = false;
            leftIndex = 0;
            rightOuterIndex = 0;

            limitedhelper.setown(createRHLimitedCompareHelper());
            limitedhelper->init( helper.getJoinLimit(), dualcache->queryOut2(), collate, helper.queryPrefixCompare() );
        }
    }

    virtual void reset()
    {
        group.clear();
        CRoxieServerActivity::reset();
        defaultLeft.clear();
        defaultRight.clear();
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (limitedhelper)
        {
            while(!eof) //limited match join
            {
                if (!group.isItem(rightIndex))
                {
                    lhs.setown(dualCacheInput->nextInGroup());
                    if (lhs)
                    {
                        rightIndex = 0;
                        group.clear();
                        limitedhelper->getGroup(group,lhs);
                    }
                    else 
                    {
                        eof = true;
                    }
                }
                
                if (group.isItem(rightIndex))
                {
                    const void * rhs = group.item(rightIndex++);
                    if(helper.match(lhs, rhs))
                    {
                        const void * ret = joinRecords(lhs, rhs);
                        return ret;
                    }
                }
            }
            return NULL;
        }
        else
        {
            if (first)
            {
                first = false;
                fillGroup();
            }
            while(!eof)
            {
                if(failingOuterAtmost)
                    while(group.isItem(leftIndex))
                    {
                        const void * ret = joinRecords(group.item(leftIndex++), defaultRight);
                        if(ret)
                        {
                            processed++;
                            return ret;
                        }
                    }
                if((joinLimit == 0) || !group.isItem(rightIndex))
                {
                    if(leftOuterJoin && !matchedLeft && !failingLimit)
                    {
                        const void * ret = joinRecords(group.item(leftIndex), defaultRight);
                        if(ret)
                        {
                            matchedLeft = true;
                            processed++;
                            return ret;
                        }
                    }
                    leftIndex++;
                    matchedLeft = false;
                    rightIndex = 0;
                    joinLimit = keepLimit;
                }
                if(!group.isItem(leftIndex))
                {
                    if(failingLimit || failingOuterAtmost)
                    {
                        const void * lhs;
                        while((lhs = input->nextInGroup()) != NULL)  // dualCache never active here
                        {
                            const void * ret = joinRecords(lhs, defaultRight, failingLimit);
                            ReleaseRoxieRow(lhs);
                            if(ret)
                            {
                                processed++;
                                return ret;
                            }
                        }
                        failingLimit.clear();
                    }
                    if(rightOuterJoin && !failingLimit)
                        while(group.isItem(rightOuterIndex))
                            if(!matchedRight.item(rightOuterIndex++))
                            {
                                const void * ret = joinRecords(defaultLeft, group.item(rightOuterIndex-1));
                                if(ret)
                                {
                                    processed++;
                                    return ret;
                                }
                            }
                    if(!fillGroup())
                        return NULL;
                    continue;
                }
                const void * lhs = group.item(leftIndex);
                if(failingLimit)
                {
                    leftIndex++;
                    const void * ret = joinRecords(lhs, defaultRight, failingLimit);
                    if(ret)
                    {
                        processed++;
                        return ret;
                    }
                }
                else
                {
                    const void * rhs = group.item(rightIndex++);
                    if(helper.match(lhs, rhs))
                    {
                        matchedLeft = true;
                        matchedRight.replace(true, rightIndex-1);
                        if(!exclude)
                        {
                            const void * ret = joinRecords(lhs, rhs);
                            if(ret)
                            {
                                processed++;
                                joinLimit--;
                                return ret;
                            }
                        }
                    }
                }
            }
            return NULL;
        }
    }
};

class CRoxieServerSelfJoinActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSelfJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSelfJoinActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerSelfJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSelfJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerLookupJoinActivity : public CRoxieServerTwoInputActivity
{
private:
    class LookupTable : public CInterface
    {
    public:
        LookupTable(unsigned _size, ICompare * _leftRightCompare, ICompare * _rightCompare, IHash * _leftHash, IHash * _rightHash, bool _dedupOnAdd)
        : leftRightCompare(_leftRightCompare), rightCompare(_rightCompare), leftHash(_leftHash), rightHash(_rightHash), dedupOnAdd(_dedupOnAdd)
        {
            unsigned minsize = (4*_size)/3;
            size = 2;
            while((minsize >>= 1) > 0)
                size <<= 1;
            mask = size - 1;
            table = (const void * *)calloc(size, sizeof(void *));
            findex = BadIndex;
        }

        ~LookupTable()
        {
            unsigned i;
            for(i=0; i<size; i++)
                ReleaseRoxieRow(table[i]);
            free(table);
        }

        void add(const void * right)
        {
            findex = BadIndex;
            unsigned start = rightHash->hash(right) & mask;
            unsigned index = start;
            while(table[index])
            {
                if(dedupOnAdd && (rightCompare->docompare(table[index], right) == 0))
                {
                    ReleaseRoxieRow(right);
                    return;
                }
                index++;
                if(index==size)
                    index = 0;
                if(index==start)
                    throwUnexpected(); //table is full, should never happen
            }
            table[index] = right;
        }

        const void *find(const void * left) const
        {
            fstart = leftHash->hash(left) & mask;
            findex = fstart;
            return doFind(left);
        }

        const void *findNext(const void * left) const
        {
            if(findex == BadIndex)
                return NULL;
            advance();
            return doFind(left);
        }

        void advance() const
        {
            findex++;
            if(findex==size)
                findex = 0;
            if(findex==fstart)
                throw MakeStringException(ROXIE_JOIN_ERROR, "Internal error hthor lookup join activity (hash table full on lookup)");
        }

        const void *doFind(const void * left) const
        {
            while(table[findex])
            {
                if(leftRightCompare->docompare(left, table[findex]) == 0)
                    return table[findex];
                advance();
            }
            findex = BadIndex;
            return NULL;
        }

    private:
        ICompare * leftRightCompare;
        ICompare * rightCompare;
        IHash * leftHash;
        IHash * rightHash;
        unsigned size;
        unsigned mask;
        const void * * table;
        bool dedupOnAdd;
        unsigned mutable fstart;
        unsigned mutable findex;
        static unsigned const BadIndex;
    };

    IHThorHashJoinArg &helper;
    bool leftOuterJoin;
    bool exclude;
    bool eog;
    bool many;
    bool dedupRHS;
    bool matchedGroup;
    const void *left;
    OwnedConstRoxieRow defaultRight;
    Owned<LookupTable> table;
    unsigned keepLimit;
    unsigned atmostLimit;
    unsigned limitLimit;
    bool limitFail;
    bool limitOnFail;
    bool hasGroupLimit;
    unsigned keepCount;
    bool gotMatch;
    bool cloneLeft;
    ConstPointerArray rightGroup;
    aindex_t rightGroupIndex;
    Owned<IException> failingLimit;
    ConstPointerArray filteredRight;
    ThorActivityKind activityKind;
    Owned<IEngineRowAllocator> defaultRightAllocator;
    
    void createDefaultRight()
    {
        if (!defaultRight)
        {
            if (!defaultRightAllocator)
                defaultRightAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
            size32_t thisSize = helper.createDefaultRight(rowBuilder);
            defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

public:
    CRoxieServerLookupJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerTwoInputActivity(_factory, _probeManager), helper((IHThorHashJoinArg &)basehelper)
    {
        unsigned joinFlags = helper.getJoinFlags();
        leftOuterJoin = (joinFlags & JFleftouter) != 0;
        assertex((joinFlags & JFrightouter) == 0);
        exclude = (joinFlags & JFexclude) != 0;
        many = (joinFlags & JFmanylookup) != 0;
        cloneLeft = (joinFlags & JFtransformmatchesleft) != 0;
        dedupRHS = (joinFlags & (JFmanylookup | JFmatchrequired | JFtransformMaySkip)) == 0; // optimisation: can implicitly dedup RHS unless is many lookup, or match required, or transform may skip
        left = NULL;
        activityKind = factory->getKind();
        eog = false;
        matchedGroup = false;
        gotMatch = false;
        keepLimit = 0;
        keepCount = 0;
        atmostLimit = 0;
        limitLimit = 0;
        hasGroupLimit = false;
        getLimitType(helper.getJoinFlags(), limitFail, limitOnFail);
    }

    void loadRight()
    {
        ConstPointerArray rightset;
        unsigned i = 0;
        try
        {
            const void * next;
            while(true)
            {
                next = input1->nextInGroup();
                if(!next)
                    next = input1->nextInGroup();
                if(!next)
                    break;
                rightset.append(next);
            }
            unsigned rightord = rightset.ordinality();
            table.setown(new LookupTable(rightord, helper.queryCompareLeftRight(), helper.queryCompareRight(), helper.queryHashLeft(), helper.queryHashRight(), dedupRHS));

            for(i=0; i<rightord; i++)
                table->add(rightset.item(i));
        }
        catch (...)
        {
            unsigned rightord = rightset.ordinality();
            for ( ; i<rightord; i++)
                ReleaseRoxieRow(rightset.item(i));
            throw;
        }
    };

    virtual void reset()
    {
        CRoxieServerTwoInputActivity::reset();
        ReleaseClearRoxieRow(left);
        defaultRight.clear();
        table.clear();
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eog = false;
        matchedGroup = false;
        left = NULL;
        CRoxieServerTwoInputActivity::start(parentExtractSize, parentExtract, paused);
        keepLimit = helper.getKeepLimit();
        if(keepLimit==0) keepLimit = static_cast<unsigned>(-1);
        atmostLimit = helper.getJoinLimit();
        limitLimit = helper.getMatchAbortLimit();
        hasGroupLimit = ((atmostLimit > 0) || (limitLimit > 0));
        if(atmostLimit==0) atmostLimit = static_cast<unsigned>(-1);
        if(limitLimit==0) limitLimit = static_cast<unsigned>(-1);
        getLimitType(helper.getJoinFlags(), limitFail, limitOnFail);
        if (((activityKind==TAKlookupjoin || activityKind==TAKlookupdenormalizegroup) && leftOuterJoin) || limitOnFail)
            createDefaultRight();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx==1)
            input1 = _in;
        else
        {
            if ((helper.getJoinFlags() & JFparallel) != 0)
            {
                puller.setown(new CRoxieServerReadAheadInput(0)); // MORE - cant ask context for parallelJoinPreload as context is not yet set up.
                puller->setInput(0, _in);
                _in = puller;
            }
            CRoxieServerActivity::setInput(idx, _in);
        }
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if(!table)
            loadRight();
        switch (activityKind)
        {
            case TAKlookupjoin:
                return nextInGroupJoin();
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
                return nextInGroupDenormalize();
        }
        throwUnexpected();
    }

private:
    const void * nextInGroupJoin()
    {
        if(!table)
            loadRight();
        while(true)
        {
            const void * right = NULL;
            if(!left)
            {
                left = input->nextInGroup();
                keepCount = keepLimit;
                if(!left)
                {
                    if(matchedGroup || eog)
                    {
                        matchedGroup = false;
                        eog = true;
                        return NULL;
                    }
                    eog = true;
                    continue;
                }
                eog = false;
                gotMatch = false;
                right = getRightFirst();
            }
            else
                right = getRightNext();
            const void * ret = NULL;
            if(failingLimit)
            {
                ret = joinException(left, failingLimit);
            }
            else
            {
                while(right)
                {
                    if(helper.match(left, right))
                    {
                        gotMatch = true;
                        if(exclude)
                            break;
                        ret = joinRecords(left, right);
                        if(ret)
                            break;
                    }
                    right = getRightNext();
                    ret = NULL;
                }
                if(leftOuterJoin && !gotMatch)
                {
                    ret = joinRecords(left, defaultRight);
                    gotMatch = true;
                }
            }
            if(ret)
            {
                matchedGroup = true;
                processed++;
                if(!many || (--keepCount == 0) || failingLimit)
                {
                    ReleaseClearRoxieRow(left);
                    failingLimit.clear();
                }
                return ret;
            }
            ReleaseClearRoxieRow(left);
        }
    }

    const void * nextInGroupDenormalize()
    {
        while(true)
        {
            left = input->nextInGroup();
            if(!left)
            {
                if (!matchedGroup)
                    left = input->nextInGroup();

                if (!left)
                {
                    matchedGroup = false;
                    return NULL;
                }
            }
            gotMatch = false;

            const void * right = getRightFirst();
            const void * ret = NULL;
            if (failingLimit)
                ret = joinException(left, failingLimit);
            else if (activityKind == TAKlookupdenormalize)
            {
                OwnedConstRoxieRow newLeft;
                newLeft.set(left);
                unsigned rowSize = 0;
                unsigned leftCount = 0;
                keepCount = keepLimit;
                while (right)
                {
                    try
                    {
                        if (helper.match(left, right))
                        {
                            gotMatch = true;
                            if (exclude)
                                break;

                            RtlDynamicRowBuilder rowBuilder(rowAllocator);
                            unsigned thisSize = helper.transform(rowBuilder, newLeft, right, ++leftCount);
                            if (thisSize)
                            {
                                rowSize = thisSize;
                                newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                            }
                            if(!many || (--keepCount == 0))
                                break;
                        }
                        right = getRightNext();
                    }
                    catch (IException *E)
                    {
                        throw makeWrappedException(E);
                    }
                }
                if (rowSize)
                    ret = newLeft.getClear();
                else if (leftOuterJoin && !gotMatch)
                {
                    ret = left;
                    left = NULL;
                }
            }
            else
            {
                filteredRight.kill();
                keepCount = keepLimit;
                while (right)
                {
                    if (helper.match(left, right))
                    {
                        gotMatch = true;
                        if(exclude)
                            break;
                        filteredRight.append(right);
                        if(!many || (--keepCount == 0))
                            break;
                    }
                    right = getRightNext();
                }

                if((filteredRight.ordinality() > 0) || (leftOuterJoin && !gotMatch))
                    ret = denormalizeRecords(left, filteredRight);
                filteredRight.kill();
            }
            ReleaseRoxieRow(left);
            left = NULL;
            failingLimit.clear();
            if(ret)
            {
                matchedGroup = true;
                processed++;
                return ret;
            }
        }
    }

    const void * joinRecords(const void * left, const void * right)
    {
        if (cloneLeft)
        {
            LinkRoxieRow(left);
            return left;
        }
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned outSize = helper.transform(rowBuilder, left, right);
            if (outSize)
                return rowBuilder.finalizeRowClear(outSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * joinException(const void * left, IException * except)
    {
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned outSize = helper.onFailTransform(rowBuilder, left, defaultRight, except);
            if (outSize)
                return rowBuilder.finalizeRowClear(outSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * denormalizeRecords(const void * left, ConstPointerArray & rows)
    {
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned numRows = rows.ordinality();
            const void * right = numRows ? rows.item(0) : defaultRight.get();
            unsigned outSize = helper.transform(rowBuilder, left, right, numRows, (const void * *)rows.getArray());
            if (outSize)
                return rowBuilder.finalizeRowClear(outSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * getRightFirst() { if(hasGroupLimit) return fillRightGroup(); else return table->find(left); }
    const void * getRightNext() { if(hasGroupLimit) return readRightGroup(); else return table->findNext(left); }
    const void * readRightGroup() { if(rightGroup.isItem(rightGroupIndex)) return rightGroup.item(rightGroupIndex++); else return NULL; }

    const void *fillRightGroup()
    {
        rightGroup.kill();
        for(const void * right = table->find(left); right; right = table->findNext(left))
        {
            rightGroup.append(right);
            if(rightGroup.ordinality() > limitLimit)
            {
                if(limitFail)
                    failLimit();
                gotMatch = true;
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                if(limitOnFail)
                {
                    assertex(!failingLimit);
                    try
                    {
                        failLimit();
                    }
                    catch(IException * e)
                    {
                        failingLimit.setown(e);
                    }
                    assertex(failingLimit != NULL);
                }
                else
                {
                    rightGroup.kill();
                }
                break;
            }
            if(rightGroup.ordinality() > atmostLimit)
            {
                rightGroup.kill();
                break;
            }
        }
        rightGroupIndex = 0;
        return readRightGroup();
    }

    void failLimit()
    {
        helper.onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(XWFtrim|XWFopt );
        if(!ctx->isBlind() && input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
        {
            input->queryOutputMeta()->toXML(static_cast<const unsigned char *>(left), xmlwrite);
        }
        throw MakeStringException(ROXIE_TOO_MANY_RESULTS, "More than %u match candidates in join %d for row %s", limitLimit, queryId(), xmlwrite.str());
    }
    
};

unsigned const CRoxieServerLookupJoinActivity::LookupTable::BadIndex(static_cast<unsigned>(-1));

class CRoxieServerLookupJoinActivityFactory : public CRoxieServerJoinActivityFactory
{
public:
    CRoxieServerLookupJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorHashJoinArg> helper = (IHThorHashJoinArg *) helperFactory();
        if((helper->getJoinFlags() & (JFfirst | JFfirstleft | JFfirstright | JFslidingmatch)) != 0)
            throw MakeStringException(ROXIE_INVALID_FLAGS, "Invalid flags for lookup join activity"); // code generator should never create such an activity
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLookupJoinActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerLookupJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerLookupJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerAllJoinActivity : public CRoxieServerTwoInputActivity
{
private:
    IHThorAllJoinArg &helper;
    bool leftOuterJoin;
    bool rightOuterJoin;
    bool exclude;
    OwnedConstRoxieRow defaultRight;
    OwnedConstRoxieRow defaultLeft;
    Owned<IEngineRowAllocator> defaultRightAllocator;
    Owned<IEngineRowAllocator> defaultLeftAllocator;
    const void *left;
    unsigned countForLeft;
    ConstPointerArray rightset;
    BoolArray matchedRight; // MORE - could use a bitset...
    unsigned keepLimit;
    bool started;
    bool eog;
    bool eos;
    bool matchedLeft;
    bool matchedGroup;
    bool leftIsGrouped;
    bool cloneLeft;
    unsigned rightIndex;
    unsigned rightOrdinality;
    ThorActivityKind activityKind;
    ConstPointerArray filteredRight;

    void createDefaultLeft()
    {
        if (!defaultLeft)
        {
            if (!defaultLeftAllocator)
                defaultLeftAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultLeftAllocator);
            size32_t thisSize = helper.createDefaultLeft(rowBuilder);
            defaultLeft.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

    void createDefaultRight()
    {
        if (!defaultRight)
        {
            if (!defaultRightAllocator)
                defaultRightAllocator.setown(ctx->queryCodeContext()->getRowAllocator(input1->queryOutputMeta(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
            size32_t thisSize = helper.createDefaultRight(rowBuilder);
            defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

public:
    CRoxieServerAllJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerTwoInputActivity(_factory, _probeManager), helper((IHThorAllJoinArg &)basehelper)
    {
        unsigned joinFlags = helper.getJoinFlags();
        leftOuterJoin = (joinFlags & JFleftouter) != 0;
        rightOuterJoin = (joinFlags & JFrightouter) != 0;
        cloneLeft = (joinFlags & JFtransformmatchesleft) != 0;
        keepLimit = (unsigned) -1;
        exclude = (joinFlags & JFexclude) != 0;
        left = NULL;
        started = true;
        eog = false;
        eos = false;
        matchedLeft = false;
        matchedGroup = false;
        activityKind = factory->getKind();
        rightIndex = 0;
        rightOrdinality = 0;
        leftIsGrouped = false;
        countForLeft = 0;
    }

    virtual void reset()
    {
        defaultRight.clear();
        defaultLeft.clear();
        ReleaseRoxieRowSet(rightset);
        matchedRight.kill();
        CRoxieServerTwoInputActivity::reset();
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eog = false;
        eos = false;
        matchedLeft = false;
        matchedGroup = false;
        started = false;
        left = NULL;

        CRoxieServerTwoInputActivity::start(parentExtractSize, parentExtract, paused);
        keepLimit = helper.getKeepLimit();
        if(keepLimit==0)
            keepLimit = (unsigned) -1;
        countForLeft = keepLimit;
        leftIsGrouped = input->queryOutputMeta()->isGrouped();
        if((activityKind==TAKalljoin || activityKind==TAKalldenormalizegroup) && leftOuterJoin)
            createDefaultRight();
        if(rightOuterJoin)
            createDefaultLeft();
    }

    void loadRight()
    {
        const void * next;
        while(true)
        {
            next = input1->nextInGroup();
            if(!next)
                next = input1->nextInGroup();
            if(!next)
                break;
            rightset.append(next);
            matchedRight.append(false);
        }
        rightIndex = 0;
        rightOrdinality = rightset.ordinality();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx==1)
            input1 = _in;
        else
        {
            if ((helper.getJoinFlags() & JFparallel) != 0)
            {
                puller.setown(new CRoxieServerReadAheadInput(0)); // MORE - cant ask context for parallelJoinPreload as context is not yet set up.
                puller->setInput(0, _in);
                _in = puller;
            }
            CRoxieServerActivity::setInput(idx, _in);
        }
    }

    const void * joinRecords(const void * left, const void * right)
    {
        // MORE - could share some code with lookup join
        if (cloneLeft)
        {
            LinkRoxieRow(left);
            return left;
        }
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned outSize = helper.transform(rowBuilder, left, right);
            if (outSize)
                return rowBuilder.finalizeRowClear(outSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    const void * denormalizeRecords(const void * curLeft, ConstPointerArray & rows)
    {
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            unsigned numRows = rows.ordinality();
            const void * right = numRows ? rows.item(0) : defaultRight.get();
            unsigned outSize = helper.transform(rowBuilder, curLeft, right, numRows, rows.getArray());
            if (outSize)
                return rowBuilder.finalizeRowClear(outSize);
            else
                return NULL;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if(!started)
        {
            started = true;
            left = input->nextInGroup();
            matchedLeft = false;
            countForLeft = keepLimit;
            if(left == NULL)
            {
                eos = true;
                return NULL;
            }
            loadRight();
        }

        const void * ret;   
        const void * right;
        if(eos)
            return NULL;

        while(true)
        {
            ret = NULL;

            if((rightIndex == rightOrdinality) || (countForLeft==0))
            {
                if(leftOuterJoin && left && !matchedLeft)
                {
                    switch(activityKind)
                    {
                    case TAKalljoin:
                        ret = joinRecords(left, defaultRight);
                        break;
                    case TAKalldenormalize:
                        ret = left;
                        left = NULL;
                        break;
                    case TAKalldenormalizegroup:
                        filteredRight.kill();
                        ret = denormalizeRecords(left, filteredRight);
                        break;
                    default:
                        throwUnexpected();
                    }
                }
                rightIndex = 0;
                ReleaseRoxieRow(left);
                left = NULL;
                if(ret)
                {
                    matchedGroup = true;
                    processed++;
                    return ret;
                }
            }

            if(!left)
            {
                left = input->nextInGroup();
                matchedLeft = false;
                countForLeft = keepLimit;
            }
            if(!left)
            {
                if(eog)
                {
                    eos = true;
                    matchedGroup = false;
                    return NULL;
                }
                eog = true;
                if (matchedGroup && leftIsGrouped)
                {
                    matchedGroup = false;
                    return NULL;
                }
                matchedGroup = false;
                continue;
            }

            eog = false;
            switch(activityKind)
            {
            case TAKalljoin:
                while(rightIndex < rightOrdinality)
                {
                    right = rightset.item(rightIndex);
                    if(helper.match(left, right))
                    {
                        matchedLeft = true;
                        matchedRight.replace(true, rightIndex);
                        if(!exclude)
                            ret = joinRecords(left, right);
                    }
                    rightIndex++;
                    if(ret)
                    {
                        countForLeft--;
                        matchedGroup = true;
                        processed++;
                        return ret;
                    }
                }
                break;
            case TAKalldenormalize:
                {
                    OwnedConstRoxieRow newLeft;
                    newLeft.set(left);
                    unsigned rowSize = 0;
                    unsigned leftCount = 0;
                    while((rightIndex < rightOrdinality) && countForLeft)
                    {
                        right = rightset.item(rightIndex);
                        if(helper.match(left, right))
                        {
                            matchedLeft = true;
                            matchedRight.replace(true, rightIndex);
                            if(!exclude)
                            {
                                try
                                {
                                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                    unsigned thisSize = helper.transform(rowBuilder, newLeft, right, ++leftCount);
                                    if(thisSize)
                                    {
                                        rowSize = thisSize;
                                        newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                        --countForLeft;
                                    }
                                }
                                catch (IException *e)
                                {
                                    throw makeWrappedException(e);
                                }
                            }
                        }
                        rightIndex++;
                    }
                    if(rowSize)
                    {
                        processed++;
                        return newLeft.getClear();
                    }
                }
                break;
            case TAKalldenormalizegroup:
                filteredRight.kill();
                while((rightIndex < rightOrdinality) && countForLeft)
                {
                    right = rightset.item(rightIndex);
                    if(helper.match(left, right))
                    {
                        matchedLeft = true;
                        matchedRight.replace(true, rightIndex);
                        filteredRight.append(right);
                        --countForLeft;
                    }
                    ++rightIndex;
                }
                if(!exclude && filteredRight.ordinality())
                {
                    ret = denormalizeRecords(left, filteredRight);
                    filteredRight.kill();
                    if(ret)
                    {
                        processed++;
                        return ret;
                    }
                }
                break;
            default:
                throwUnexpected();
            }
        }
    }

};

class CRoxieServerAllJoinActivityFactory : public CRoxieServerJoinActivityFactory
{
public:
    CRoxieServerAllJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorAllJoinArg> helper = (IHThorAllJoinArg *) helperFactory();
        if((helper->getJoinFlags() & (JFfirst | JFfirstleft | JFfirstright)) != 0)
            throw MakeStringException(ROXIE_INVALID_FLAGS, "Invalid flags for join all activity"); // code generator should never create such an activity
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerAllJoinActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerAllJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerAllJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerTopNActivity : public CRoxieServerLateStartActivity
{
    unsigned limit;
    bool hasBest;
    bool eoi;
    const void **sorted;
    unsigned sortedCount;
    unsigned curIndex;
    IHThorTopNArg &helper;
    ICompare &compare;

public:
    CRoxieServerTopNActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerLateStartActivity(_factory, _probeManager), helper((IHThorTopNArg &)basehelper), compare(*helper.queryCompare())
    {
        sorted = NULL;
        sortedCount = 0;
        curIndex = 0;
        limit = 0;
        eoi = false;
        hasBest = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        assertex(sorted == NULL);
        sortedCount = 0;
        curIndex = 0;
        eoi = false;
        
        CRoxieServerLateStartActivity::start(parentExtractSize, parentExtract, paused);
        limit = (unsigned) helper.getLimit();
        hasBest = helper.hasBest();
        lateStart(parentExtractSize, parentExtract, limit > 0);
        // MORE - should we use an expanding array instead?
        if (limit > 0)
            sorted = (const void **) ctx->queryRowManager().allocate((limit+1) * sizeof(const void *), activityId); 
    }

    virtual void reset()
    {
        if (sorted)
        {
            while(curIndex < sortedCount)
                ReleaseRoxieRow(sorted[curIndex++]);
            ReleaseRoxieRow(sorted);
        }
        sorted = NULL;
        CRoxieServerLateStartActivity::reset();
    }

    bool abortEarly()
    {
        if (hasBest && (sortedCount == limit))
        {
            int compare = helper.compareBest(sorted[sortedCount-1]);
            if (compare == 0)
            {
                if (meta.isGrouped())
                {
                    //MORE: This would be more efficient if we had a away of skipping to the end of the incoming group.
                    const void * next;
                    while ((next = input->nextInGroup()) != NULL)
                        ReleaseRoxieRow(next);
                }
                else
                    eoi = true;
                return true;
            }

            //This only checks the lowest element - we could check all elements inserted, but it would increase the number of compares
            if (compare < 0)
                throw MakeStringException(ROXIE_TOPN_ROW_ERROR, "TOPN: row found that exceeds the best value");
        }
        return false;
    }

    void getSorted()
    {
        curIndex = 0;
        sortedCount = 0;
        if(eoi)
            return;
        const void * next;
        while ((next = input->nextInGroup()) != NULL)
        {
            if (sortedCount < limit)
            {
                binary_vec_insert_stable(next, sorted, sortedCount, compare);
                sortedCount++;
                if(abortEarly())
                    return;
            }
            else
            {
                if(limit && compare.docompare(sorted[sortedCount-1], next) > 0) // MORE - if stability is an issue, need to consider whether this should be > or >=
                {
                    binary_vec_insert_stable(next, sorted, sortedCount, compare); // MORE - not sure this is stable!
                    ReleaseRoxieRow(sorted[sortedCount]);
                    if(abortEarly())
                        return;
                }
                else
                {
                    ReleaseRoxieRow(next); // do not bother with insertion sort if we know next will fall off the end
                }
            }
        }
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        if (curIndex >= sortedCount)
        {
            bool eog = sortedCount != 0;
            getSorted();
            if(sortedCount == 0)
            {
                eof = true;
                return NULL;
            }
            if (eog)
                return NULL;
        }
        processed++;
        return sorted[curIndex++];
    }

};

class CRoxieServerTopNActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerTopNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerTopNActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerTopNActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerTopNActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerLimitActivity : public CRoxieServerActivity
{
protected:
    unsigned __int64 rowLimit;
    IHThorLimitArg &helper;

public:
    CRoxieServerLimitActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorLimitArg &)basehelper)
    {
        rowLimit = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        rowLimit = helper.getRowLimit();  // could conceivably depend on context so should not compute any earlier than this
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * ret = input->nextInGroup();
        if (ret)
        {
            processed++;
            if (processed > rowLimit)
            {
                ReleaseRoxieRow(ret);
                if (traceLevel > 4)
                    DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                helper.onLimitExceeded();
            }
        }
        return ret;
    }
    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        const void * ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret)
        {
            if (wasCompleteMatch)
                processed++;
            if (processed > rowLimit)
            {
                ReleaseRoxieRow(ret);
                if (traceLevel > 4)
                    DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                helper.onLimitExceeded();
            }
        }
        return ret;
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        //Do not reset the rowLimit
        input->resetEOF(); 
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }
};

class CRoxieServerLimitActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerLimitActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerLimitActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerSkipLimitActivity : public CRoxieServerLimitActivity
{
    ConstPointerArray buff;
    bool started;
    unsigned index;
    IHThorLimitTransformExtra * transformExtra;

public:
    CRoxieServerSkipLimitActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _onFail)
        : CRoxieServerLimitActivity(_factory, _probeManager)
    {
        transformExtra = NULL;
        started = false;
        index = 0;
        if (_onFail)
            transformExtra = static_cast<IHThorLimitTransformExtra *>(helper.selectInterface(TAIlimittransformextra_1));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        started = false;
        index = 0;
        CRoxieServerLimitActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        while (buff.isItem(index))
            ReleaseRoxieRow(buff.item(index++));
        buff.kill();
        started = false;
        CRoxieServerLimitActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!started)
            pullInput();
        if (buff.isItem(index))
        {
            const void * next = buff.item(index++);
            if(next)
                processed++;
            return next;
        }
        return NULL;
    }

protected:
    void pullInput()
    {
        unsigned count = 0;
        loop
        {
            const void * next = input->nextInGroup();
            if (next == NULL)
            {
                next = input->nextInGroup();
                if(next == NULL)
                    break;
                buff.append(NULL);
            }
            count++;
            if (count > rowLimit)
            {
                ReleaseRoxieRow(next);
                ReleaseRoxieRowSet(buff);
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                if (transformExtra)
                {
                    createRowAllocator();
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    size32_t outSize = transformExtra->transformOnLimitExceeded(rowBuilder);
                    if (outSize)
                        buff.append(rowBuilder.finalizeRowClear(outSize));
                }
                break;
            }
            buff.append(next);
        }
        started = true;
    }
};

class CRoxieServerSkipLimitActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSkipLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSkipLimitActivity(this, _probeManager, kind==TAKcreaterowlimit);
    }
};

IRoxieServerActivityFactory *createRoxieServerSkipLimitActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSkipLimitActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerCatchActivity : public CRoxieServerActivity
{
    IHThorCatchArg &helper;

public:
    CRoxieServerCatchActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorCatchArg &)basehelper)
    {
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        try
        {
            const void *ret = input->nextInGroup();
            if (ret)
                processed++;
            return ret;
        }
        catch (IException *E)
        {
            E->Release();
            helper.onExceptionCaught();
        }
        catch (...)
        {
            helper.onExceptionCaught();
        }
        throwUnexpected(); // onExceptionCaught should have thrown something
    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        try
        {
            ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
            const void * ret = input->nextSteppedGE(seek, numFields, wasCompleteMatch, stepExtra);
            if (ret && wasCompleteMatch)
                processed++;
            return ret;
        }
        catch (IException *E)
        {
            E->Release();
            helper.onExceptionCaught();
        }
        catch (...)
        {
            helper.onExceptionCaught();
        }
        throwUnexpected(); // onExceptionCaught should have thrown something
    }

    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) 
    { 
        return input->gatherConjunctions(collector); 
    }

    virtual void resetEOF() 
    { 
        input->resetEOF(); // MORE - why not in base class?
    }

    IInputSteppingMeta * querySteppingMeta()
    {
        return input->querySteppingMeta();
    }
};

class CRoxieServerSkipCatchActivity : public CRoxieServerActivity
{
    ConstPointerArray buff;
    bool started;
    unsigned index;
    IHThorCatchArg &helper;
    bool createRow;

public:
    CRoxieServerSkipCatchActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _createRow)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorCatchArg &)basehelper), createRow(_createRow)
    {
        started = false;
        index = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        started = false;
        index = 0;
        try
        {
            CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        }
        catch (IException *E)
        {
            onException(E);
            started = true;
        }
        catch (...)
        {
            onException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught"));
            started = true;
        }
    }

    virtual void reset()
    {
        while (buff.isItem(index))
            ReleaseRoxieRow(buff.item(index++));
        buff.kill();
        started = false;
        CRoxieServerActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (!started)
            pullInput();
        if (buff.isItem(index))
        {
            const void * next = buff.item(index++);
            if(next)
                processed++;
            return next;
        }
        return NULL;
    }

protected:
    void onException(IException *E)
    {
        input->stop(true);
        ReleaseRoxieRowSet(buff);
        if (createRow)
        {
            createRowAllocator();
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            size32_t outSize = helper.transformOnExceptionCaught(rowBuilder, E);
            if (outSize)
                buff.append(rowBuilder.finalizeRowClear(outSize));
        }
        E->Release();
    }

    void pullInput()
    {
        try
        {
            bool EOGseen = false;
            loop
            {
                const void * next = input->nextInGroup();
                buff.append(next);
                if (next == NULL)
                {
                    if (EOGseen)
                        break;
                    EOGseen = true;
                }
                else
                    EOGseen = false;
            }
        }
        catch (IException *E)
        {
            onException(E);
        }
        catch (...)
        {
            onException(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught"));
        }
        started = true;
    }
};

class CRoxieServerCatchActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerCatchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        switch (kind)
        {
        case TAKcatch:
            return new CRoxieServerCatchActivity(this, _probeManager);
        case TAKskipcatch:
            return new CRoxieServerSkipCatchActivity(this, _probeManager, false);
        case TAKcreaterowcatch:
            return new CRoxieServerSkipCatchActivity(this, _probeManager, true);
        default:
            throwUnexpected();
        }
    }
};

IRoxieServerActivityFactory *createRoxieServerCatchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerCatchActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=================================================================================

class CRoxieServerCaseActivity : public CRoxieServerActivity
{
    IHThorCaseArg &helper;
    IRoxieInput **inputs;
    unsigned cond;
    bool unusedStopped;
    IRoxieInput *in;
    unsigned numInputs;

public:
    CRoxieServerCaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned _numInputs)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorCaseArg &)basehelper), numInputs(_numInputs)
    {
        unusedStopped = false;
        cond = 0;
        inputs = new IRoxieInput*[numInputs];
        for (unsigned i = 0; i < numInputs; i++)
            inputs[i] = NULL;
        in = NULL;
    }

    ~CRoxieServerCaseActivity()
    {
        delete [] inputs;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        cond = helper.getBranch();
        //CHOOSE defaults to the last argument if out of range.
        if (cond >= numInputs)
            cond = numInputs - 1;
        inputs[cond]->start(parentExtractSize, parentExtract, paused);
        for (unsigned idx = 0; idx < numInputs; idx++)
        {
            if (idx!=cond)
                inputs[idx]->stop(false); // Note: stopping unused branches early helps us avoid buffering splits too long.
        }
        in = inputs[cond];
        unusedStopped = true;
    }

    virtual void stop(bool aborting)
    {
        for (unsigned idx = 0; idx < numInputs; idx++)
        {
            if (idx==cond || !unusedStopped)
                inputs[idx]->stop(aborting);
        }
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        for (unsigned idx = 0; idx < numInputs; idx++)
        {
            inputs[idx]->reset();
        }
        unusedStopped = false;
        in = NULL;
        CRoxieServerActivity::reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        assertex(idx < numInputs);
        inputs[idx] = _in;
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (in)
        {
            const void *ret = in->nextInGroup();
            if (ret)
                processed++;
            return ret;
        }
        return NULL;
    }
};

class CRoxieServerCaseActivityFactory : public CRoxieServerMultiInputFactory
{
    bool graphInvariant;
public:
    CRoxieServerCaseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        graphInvariant = _graphInvariant;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerCaseActivity(this, _probeManager, numInputs());
    }

    virtual bool isGraphInvariant() const
    {
        return graphInvariant;
    }
};

IRoxieServerActivityFactory *createRoxieServerCaseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant)
{
    return new CRoxieServerCaseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _graphInvariant);
}

//=================================================================================

class CRoxieServerIfActivity : public CRoxieServerActivity
{
    IHThorIfArg &helper;
    IRoxieInput *inputTrue;
    IRoxieInput *inputFalse;
    bool cond;
    bool unusedStopped;

public:
    CRoxieServerIfActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorIfArg &)basehelper)
    {
        inputFalse = NULL;
        inputTrue = NULL;
        unusedStopped = false;
        cond = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        cond = helper.getCondition();
        if (cond)
        {
            inputTrue->start(parentExtractSize, parentExtract, paused);
            if (inputFalse)
                inputFalse->stop(false); // Note: stopping unused branches early helps us avoid buffering splits too long.
        }
        else 
        {
            if (inputFalse)
                inputFalse->start(parentExtractSize, parentExtract, paused);
            inputTrue->stop(false);
        }
        unusedStopped = true;

    }

    virtual void stop(bool aborting)
    {
        if (!unusedStopped || cond)
            inputTrue->stop(aborting);
        if (inputFalse && (!unusedStopped || !cond))
            inputFalse->stop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 localCycles = totalCycles;
        localCycles -= inputTrue->queryTotalCycles();
        if (inputFalse)
            localCycles -= inputFalse->queryTotalCycles();
        if (localCycles < 0) 
            localCycles = 0;
        return localCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        switch (idx)
        {
        case 0:
            return inputTrue;
        case 1:
            return inputFalse;
        default:
            return NULL;
        }
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        IRoxieInput *in = cond ? inputTrue  : inputFalse;
        if (in)
            return in->queryIndexReadActivity();
        return NULL;
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        inputTrue->reset();
        if (inputFalse)
            inputFalse->reset();
        unusedStopped = false;
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx==1)
            inputFalse = _in;
        else
        {
            assertex(!idx);
            inputTrue = _in;
        }
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        IRoxieInput *in = cond ? inputTrue  : inputFalse;
        if (in)
        {
            const void * ret;
            if ((ret = in->nextInGroup()) != NULL)
                processed++;
            return ret;
        }
        return NULL;
    }
};

class CRoxieServerIfActivityFactory : public CRoxieServerActivityFactory
{
    unsigned input2;
    unsigned input2idx;
    bool graphInvariant;

public:
    CRoxieServerIfActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        graphInvariant = _graphInvariant;
        input2 = (unsigned)-1;
        input2idx = (unsigned)-1;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerIfActivity(this, _probeManager);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        if (idx==1)
        {
            input2 = source;
            input2idx = sourceidx;
        }
        else
            CRoxieServerActivityFactory::setInput(idx, source, sourceidx);
    }

    virtual unsigned getInput(unsigned idx, unsigned &sourceidx) const
    {
        switch (idx)
        {
        case 1:
            sourceidx = input2idx;
            return input2;
        case 0:
            return CRoxieServerActivityFactory::getInput(idx, sourceidx);
        default:
            return (unsigned) -1; 
        }
    }

    virtual unsigned numInputs() const { return (input2 == (unsigned)-1) ? 1 : 2; }

    virtual bool isGraphInvariant() const
    {
        return graphInvariant;
    }
};

IRoxieServerActivityFactory *createRoxieServerIfActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _graphInvariant)
{
    return new CRoxieServerIfActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _graphInvariant);
}

//=================================================================================

class CRoxieServerActionBaseActivity : public CRoxieServerActivity
{
    CriticalSection ecrit;
    Owned<IException> exception;
    bool executed;

public:
    CRoxieServerActionBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        executed = false;
    }

    virtual void doExecuteAction(unsigned parentExtractSize, const byte * parentExtract) = 0; 

    virtual void execute(unsigned parentExtractSize, const byte * parentExtract) 
    {
        CriticalBlock b(ecrit);
        if (exception)
            throw(exception.getLink());
        if (!executed)
        {
            try
            {
                executed = true;
                start(parentExtractSize, parentExtract, false);
                doExecuteAction(parentExtractSize, parentExtract);
                stop(false);
            }
            catch (IException * E)
            {
                ctx->notifyAbort(E);
                stop(true);
                exception.set(E);
                throw;
            }
        }
    }

    virtual void reset()
    {
        executed = false;
        exception.clear();
        CRoxieServerActivity::reset();
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        return totalCycles;
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }

    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        throwUnexpected(); // I am nobody's input
    }

};

class CRoxieServerIfActionActivity : public CRoxieServerActionBaseActivity
{
    IHThorIfArg &helper;

public:
    CRoxieServerIfActionActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActionBaseActivity(_factory, _probeManager), helper((IHThorIfArg &)basehelper)
    {
    }

    virtual void doExecuteAction(unsigned parentExtractSize, const byte * parentExtract) 
    {
        int controlId;
        {
            ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
            controlId = helper.getCondition() ? 1 : 2;
        }
        executeDependencies(parentExtractSize, parentExtract, controlId);
    }

};

class CRoxieServerIfActionActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerIfActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerIfActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerIfActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerIfActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerParallelActionActivity : public CRoxieServerActionBaseActivity
{
public:
    CRoxieServerParallelActionActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActionBaseActivity(_factory, _probeManager)
    {
    }

    virtual void doExecuteAction(unsigned parentExtractSize, const byte * parentExtract) 
    {
#ifdef PARALLEL_EXECUTE
        CParallelActivityExecutor afor(dependencies, parentExtractSize, parentExtract);
        afor.For(dependencies.ordinality(), dependencies.ordinality(), true);
#else
        ForEachItemIn(idx, dependencies)
        {
            dependencies.item(idx).execute(parentExtractSize, parentExtract);
        }
#endif
    }

};

class CRoxieServerParallelActionActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerParallelActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
        assertex(!isRoot);      // non-internal should be expanded out..
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerParallelActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerParallelActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerParallelActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerSequentialActionActivity : public CRoxieServerActionBaseActivity
{
    IHThorSequentialArg &helper;

public:
    CRoxieServerSequentialActionActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActionBaseActivity(_factory, _probeManager), helper((IHThorSequentialArg &)basehelper)
    {
    }

    virtual void doExecuteAction(unsigned parentExtractSize, const byte * parentExtract) 
    {
        unsigned numBranches = helper.numBranches();
        for (unsigned branch=1; branch <= numBranches; branch++)
            executeDependencies(parentExtractSize, parentExtract, branch);
    }

};

class CRoxieServerSequentialActionActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerSequentialActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSequentialActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerSequentialActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerSequentialActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================

class CRoxieServerWhenActivity : public CRoxieServerActivity
{
public:
    CRoxieServerWhenActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        savedExtractSize = 0;
        savedExtract = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        savedExtractSize = parentExtractSize;
        savedExtract = parentExtract;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        executeDependencies(parentExtractSize, parentExtract, WhenParallelId);        // MORE: This should probably be done in parallel!
    }

    virtual void stop(bool aborting)
    {
        if (state != STATEstopped)
            executeDependencies(savedExtractSize, savedExtract, aborting ? WhenFailureId : WhenSuccessId);
        CRoxieServerActivity::stop(aborting);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext()); // bit of a waste of time....
        return input->nextInGroup();
    }

protected:
    unsigned savedExtractSize;
    const byte *savedExtract;
};


class CRoxieServerWhenActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerWhenActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerWhenActivity(this, _probeManager);
    }
};


extern IRoxieServerActivityFactory *createRoxieServerWhenActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerWhenActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}


//=================================================================================

class CRoxieServerWhenActionActivity : public CRoxieServerActionBaseActivity
{
public:
    CRoxieServerWhenActionActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActionBaseActivity(_factory, _probeManager)
    {
        savedExtractSize = 0;
        savedExtract = NULL;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        savedExtractSize = parentExtractSize;
        savedExtract = parentExtract;
        CRoxieServerActionBaseActivity::start(parentExtractSize, parentExtract, paused);
        executeDependencies(parentExtractSize, parentExtract, WhenParallelId);        // MORE: This should probably be done in parallel!
    }

    virtual void stop(bool aborting)
    {
        if (state != STATEstopped)
            executeDependencies(savedExtractSize, savedExtract, aborting ? WhenFailureId : WhenSuccessId);
        CRoxieServerActionBaseActivity::stop(aborting);
    }

    virtual void doExecuteAction(unsigned parentExtractSize, const byte * parentExtract)
    {
        executeDependencies(parentExtractSize, parentExtract, 1);
    }


protected:
    unsigned savedExtractSize;
    const byte *savedExtract;
};


class CRoxieServerWhenActionActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerWhenActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerWhenActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
private:
    bool isRoot;
};

extern IRoxieServerActivityFactory *createRoxieServerWhenActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerWhenActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=================================================================================
class CRoxieServerParseActivity : public CRoxieServerActivity, implements IMatchedAction
{
    IHThorParseArg &helper;
    INlpParser * parser;
    INlpResultIterator * rowIter;
    const void * in;
    char * curSearchText;
    INlpParseAlgorithm * algorithm;
    size32_t curSearchTextLen;
    bool anyThisGroup;

    bool processRecord(const void * inRec)
    {
        if (helper.searchTextNeedsFree())
            rtlFree(curSearchText);

        curSearchTextLen = 0;
        curSearchText = NULL;
        helper.getSearchText(curSearchTextLen, curSearchText, inRec);

        return parser->performMatch(*this, in, curSearchTextLen, curSearchText);
    }

public:
    CRoxieServerParseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, INlpParseAlgorithm * _algorithm)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorParseArg &)basehelper), algorithm(_algorithm)
    {
        parser = NULL;
        rowIter = NULL;
        in = NULL;
        curSearchText = NULL;
        anyThisGroup = false;
        curSearchTextLen = 0;
    }

    ~CRoxieServerParseActivity()
    {
        ::Release(parser);
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        parser = algorithm->createParser(_ctx->queryCodeContext(), activityId, helper.queryHelper(), &helper);
        rowIter = parser->queryResultIter();
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        anyThisGroup = false;
        curSearchTextLen = 0;
        curSearchText = NULL;
        in = NULL;
        parser->reset();
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        if (helper.searchTextNeedsFree())
            rtlFree(curSearchText);
        curSearchText = NULL;
        ReleaseClearRoxieRow(in);
        CRoxieServerActivity::reset();
    }

    virtual unsigned onMatch(ARowBuilder & self, const void * curRecord, IMatchedResults * results, IMatchWalker * walker)
    {
        try
        {
            return helper.transform(self, curRecord, results, walker);
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    virtual const void * nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (rowIter->isValid())
            {
                anyThisGroup = true;
                OwnedConstRoxieRow out = rowIter->getRow();
                rowIter->next();
                processed++;
                return out.getClear();
            }

            ReleaseClearRoxieRow(in);
            in = input->nextInGroup();
            if (!in)
            {
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return NULL;
                }
                in = input->nextInGroup();
                if (!in)
                    return NULL;
            }

            processRecord(in);
            rowIter->first();
        }
    }

};

class CRoxieServerParseActivityFactory : public CRoxieServerActivityFactory
{
    Owned<INlpParseAlgorithm> algorithm;
    Owned<IHThorParseArg> helper;

public:
    CRoxieServerParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IResourceContext *rc)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        helper.setown((IHThorParseArg *) helperFactory());
        algorithm.setown(createThorParser(rc, *helper));
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerParseActivity(this, _probeManager, algorithm);
    }
};

IRoxieServerActivityFactory *createRoxieServerParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IResourceContext *rc)
{
    return new CRoxieServerParseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, rc);
}

//=====================================================================================================

class CRoxieServerWorkUnitWriteActivity : public CRoxieServerInternalSinkActivity
{
    IHThorWorkUnitWriteArg &helper;
    bool isReread;
    bool grouped;
    IRoxieServerContext *serverContext;

public:
    CRoxieServerWorkUnitWriteActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, bool _isReread)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorWorkUnitWriteArg &)basehelper), isReread(_isReread)
    {
        grouped = (helper.getFlags() & POFgrouped) != 0;
        serverContext = NULL;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerInternalSinkActivity::onCreate(_ctx, _colocalParent);
        serverContext = ctx->queryServerContext();
        if (!serverContext)
        {
            throw MakeStringException(ROXIE_PIPE_ERROR, "Pipe output activity cannot be executed in slave context");
        }
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onExecute() 
    {
        int sequence = helper.getSequence();
        const char *storedName = helper.queryName();
        if (!storedName)
            storedName = "Dataset";

        MemoryBuffer result;
        FlushingStringBuffer *response = NULL;
        bool saveInContext = (int) sequence < 0 || isReread;
        if (!meta.queryOriginal()) // this is a bit of a hack - don't know why no meta on an output....
            meta.set(input->queryOutputMeta());
        Owned<IOutputRowSerializer> rowSerializer;
        Owned<IXmlWriter> writer;
        if ((int) sequence >= 0)
        {
            response = serverContext->queryResult(sequence);
            if (response)
                response->startDataset("Dataset", helper.queryName(), sequence, (helper.getFlags() & POFextend) != 0);
            if (response->mlFmt==MarkupFmt_XML || response->mlFmt==MarkupFmt_JSON)
            {
                writer.setown(createIXmlWriter(serverContext->getXmlFlags(), 1, response, (response->mlFmt==MarkupFmt_JSON) ? WTJSON : WTStandard));
                writer->outputBeginArray("Row");
            }

        }
        if (serverContext->outputResultsToWorkUnit()||(response && response->isRaw))
        {
            createRowAllocator();
            rowSerializer.setown(rowAllocator->createDiskSerializer(ctx->queryCodeContext()));
        }
        __int64 initialProcessed = processed;
        RtlLinkedDatasetBuilder builder(rowAllocator);
        loop
        {
            const void *row = input->nextInGroup();
            if (saveInContext)
            {
                if (row || grouped)
                    builder.append(row);
            }
            if (grouped && (processed != initialProcessed))
            {
                if (serverContext->outputResultsToWorkUnit())
                    result.append(row == NULL);
                if (response)
                {
                    if (response->isRaw)
                        response->append(row == NULL);
                    else
                    {
                        response->append("<Row __GroupBreak__=\"1\"/>");        // sensible, but need to handle on input
                    }
                }
            }
            if (!row)
            {
                row = input->nextInGroup();
                if (!row)
                    break;
                if (saveInContext)
                    builder.append(row);
            }
            processed++;
            if (serverContext->outputResultsToWorkUnit())
            {
                CThorDemoRowSerializer serializerTarget(result);
                rowSerializer->serialize(serializerTarget, (const byte *) row);
            }
            if (response)
            {
                if (response->isRaw)
                {
                    // MORE - should be able to serialize straight to the response...
                    MemoryBuffer rowbuff;
                    CThorDemoRowSerializer serializerTarget(rowbuff);
                    rowSerializer->serialize(serializerTarget, (const byte *) row);
                    response->append(rowbuff.length(), rowbuff.toByteArray());
                }
                else if (writer)
                {
                    writer->outputBeginNested("Row", false);
                    helper.serializeXml((byte *) row, *writer);
                    writer->outputEndNested("Row");
                }
                else
                {
                    SimpleOutputWriter x;
                    helper.serializeXml((byte *) row, x);
                    x.newline();
                    response->append(x.str());
                }
                response->incrementRowCount();
                response->flush(false);
            }
            ReleaseRoxieRow(row);
        }
        if (writer)
            writer->outputEndArray("Row");
        if (saveInContext)
            serverContext->appendResultDeserialized(storedName, sequence, builder.getcount(), builder.linkrows(), (helper.getFlags() & POFextend) != 0, LINK(meta.queryOriginal()));
        if (serverContext->outputResultsToWorkUnit())
            serverContext->appendResultRawContext(storedName, sequence, result.length(), result.toByteArray(), processed, (helper.getFlags() & POFextend) != 0, false); // MORE - shame to do extra copy...
    }
};

class CRoxieServerWorkUnitWriteActivityFactory : public CRoxieServerInternalSinkFactory
{
    bool isReread;

public:
    CRoxieServerWorkUnitWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot)
    {
        isReread = usageCount > 0;
        Owned<IHThorWorkUnitWriteArg> helper = (IHThorWorkUnitWriteArg *) helperFactory();
        isInternal = (helper->getSequence()==ResultSequenceInternal);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerWorkUnitWriteActivity(this, _probeManager, isReread);
    }

};

IRoxieServerActivityFactory *createRoxieServerWorkUnitWriteActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
{
    return new CRoxieServerWorkUnitWriteActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot);

}

//=====================================================================================================

class CRoxieServerWorkUnitWriteDictActivity : public CRoxieServerInternalSinkActivity
{
    IHThorDictionaryWorkUnitWriteArg &helper;
    IRoxieServerContext *serverContext;

public:
    CRoxieServerWorkUnitWriteDictActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorDictionaryWorkUnitWriteArg &)basehelper)
    {
        serverContext = NULL;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerInternalSinkActivity::onCreate(_ctx, _colocalParent);
        serverContext = ctx->queryServerContext();
        if (!serverContext)
        {
            throw MakeStringException(ROXIE_PIPE_ERROR, "Write Dictionary activity cannot be executed in slave context");
        }
    }

    virtual bool needsAllocator() const { return true; }

    virtual void onExecute()
    {
        int sequence = helper.getSequence();
        const char *storedName = helper.queryName();
        assertex(storedName && *storedName);
        assertex(sequence < 0);

        RtlLinkedDictionaryBuilder builder(rowAllocator, helper.queryHashLookupInfo());
        loop
        {
            const void *row = input->nextInGroup();
            if (!row)
            {
                row = input->nextInGroup();
                if (!row)
                    break;
            }
            builder.appendOwn(row);
            processed++;
        }
        serverContext->appendResultDeserialized(storedName, sequence, builder.getcount(), builder.linkrows(), (helper.getFlags() & POFextend) != 0, LINK(meta.queryOriginal()));
    }
};

class CRoxieServerWorkUnitWriteDictActivityFactory : public CRoxieServerInternalSinkFactory
{

public:
    CRoxieServerWorkUnitWriteDictActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot)
    {
        Owned<IHThorDictionaryWorkUnitWriteArg> helper = (IHThorDictionaryWorkUnitWriteArg *) helperFactory();
        isInternal = (helper->getSequence()==ResultSequenceInternal);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerWorkUnitWriteDictActivity(this, _probeManager);
    }

};

IRoxieServerActivityFactory *createRoxieServerWorkUnitWriteDictActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
{
    return new CRoxieServerWorkUnitWriteDictActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot);

}

//=================================================================================

class CRoxieServerRemoteResultActivity : public CRoxieServerInternalSinkActivity
{
    IHThorRemoteResultArg &helper;

public:
    CRoxieServerRemoteResultActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerInternalSinkActivity(_factory, _probeManager), helper((IHThorRemoteResultArg &)basehelper)
    {
    }

    virtual void onExecute() 
    {
        OwnedConstRoxieRow row = input->nextInGroup();
        helper.sendResult(row);  // should be only one row or something has gone wrong!
    }

};

class CRoxieServerRemoteResultActivityFactory : public CRoxieServerInternalSinkFactory
{
public:
    CRoxieServerRemoteResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
        : CRoxieServerInternalSinkFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot)
    {
        Owned<IHThorRemoteResultArg> helper = (IHThorRemoteResultArg *) helperFactory();
        isInternal = (helper->getSequence()==ResultSequenceInternal);
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerRemoteResultActivity(this, _probeManager);
    }

};

IRoxieServerActivityFactory *createRoxieServerRemoteResultActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, unsigned _usageCount, bool _isRoot)
{
    return new CRoxieServerRemoteResultActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _usageCount, _isRoot);
}

//=================================================================================

class CRoxieServerXmlParseActivity : public CRoxieServerActivity, implements IXMLSelect
{
    IHThorXmlParseArg &helper;

    Owned<IXMLParse> xmlParser;
    const void * in;
    char * srchStr;
    unsigned numProcessedLastGroup;
    bool srchStrNeedsFree;
    Owned<IColumnProvider> lastMatch;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerXmlParseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), 
        helper((IHThorXmlParseArg &)basehelper)
    {
        srchStrNeedsFree = helper.searchTextNeedsFree();
        numProcessedLastGroup = 0;
        srchStr = NULL;
        in = NULL;
    }

    ~CRoxieServerXmlParseActivity()
    {
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        numProcessedLastGroup = 0;
        srchStr = NULL;
        in = NULL;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        if (helper.searchTextNeedsFree())
            rtlFree(srchStr);
        srchStr = NULL;
        ReleaseClearRoxieRow(in);
        xmlParser.clear();
        CRoxieServerActivity::reset();
    }

    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if(xmlParser)
            {
                loop
                {
                    if(!xmlParser->next())
                    {
                        if (srchStrNeedsFree)
                        {
                            rtlFree(srchStr);
                            srchStr = NULL;
                        }
                        xmlParser.clear();
                        break;
                    }
                    if(lastMatch)
                    {
                        try
                        {
                            RtlDynamicRowBuilder rowBuilder(rowAllocator);
                            size32_t outSize = helper.transform(rowBuilder, in, lastMatch);
                            lastMatch.clear();
                            if (outSize)
                            {
                                processed++;
                                return rowBuilder.finalizeRowClear(outSize);
                            }
                        }
                        catch (IException *E)
                        {
                            throw makeWrappedException(E);
                        }
                    }
                }
            }
            ReleaseClearRoxieRow(in);
            in = input->nextInGroup();
            if(!in)
            {
                if(numProcessedLastGroup == processed)
                    in = input->nextInGroup();
                if(!in)
                {
                    numProcessedLastGroup = processed;
                    return NULL;
                }
            }
            size32_t srchLen;
            helper.getSearchText(srchLen, srchStr, in);
            OwnedRoxieString xmlIteratorPath(helper.getXmlIteratorPath());
            xmlParser.setown(createXMLParse(srchStr, srchLen, xmlIteratorPath, *this));
        }   
    }

};

class CRoxieServerXmlParseActivityFactory : public CRoxieServerActivityFactory
{

public:
    CRoxieServerXmlParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerXmlParseActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerXmlParseActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerXmlParseActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//=====================================================================================================

class CRoxieServerDiskReadBaseActivity : public CRoxieServerActivity, implements IRoxieServerErrorHandler, implements IIndexReadContext
{
protected:
    IHThorDiskReadBaseArg &helper;
    IHThorCompoundExtra * compoundHelper;
    RemoteActivityId remoteId;              // Note we copy it rather than reference
    Owned<CSkippableRemoteResultAdaptor> remote;
    unsigned numParts;
    unsigned __int64 rowLimit;
    unsigned __int64 stopAfter;
    Linked<IInMemoryIndexManager> manager;
    Owned<IInMemoryIndexCursor> cursor;
    Owned<IDirectReader> reader;
    CThorContiguousRowBuffer deserializeSource;
    Owned<ISourceRowPrefetcher> prefetcher;
    bool eof;
    bool isKeyed;
    bool variableFileName;
    bool isOpt;
    bool sorted;
    bool maySkip;
    bool isLocal;
    CachedOutputMetaData diskSize;
    Owned<const IResolvedFile> varFileInfo;
    Owned<IFileIOArray> varFiles;

    inline bool useRemote()
    {
        return remote != NULL && numParts > 1;
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerDiskReadBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, bool _sorted, bool _maySkip, IInMemoryIndexManager *_manager)
        : CRoxieServerActivity(_factory, _probeManager), 
          helper((IHThorDiskReadBaseArg &)basehelper),
          numParts(_numParts),
          remoteId(_remoteId),
          manager(_manager),
          isLocal(_isLocal),
          sorted(_sorted),
          maySkip(_maySkip),
          deserializeSource(NULL)
    {
        if (numParts != 1 && !isLocal)  // NOTE : when numParts == 0 (variable case) we create, even though we may not use
            remote.setown(new CSkippableRemoteResultAdaptor(remoteId, meta.queryOriginal(), helper, *this, sorted, false, _maySkip));
        compoundHelper = NULL;
        eof = false;
        rowLimit = (unsigned __int64) -1;
        isKeyed = false;
        stopAfter = I64C(0x7FFFFFFFFFFFFFFF);
        diskSize.set(helper.queryDiskRecordSize());
        variableFileName = allFilesDynamic || ((helper.getFlags() & (TDXvarfilename|TDXdynamicfilename)) != 0);
        isOpt = (helper.getFlags() & TDRoptional) != 0;
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        if (remote)
            remote->onCreate(this, this, _ctx, _colocalParent);
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (compoundHelper)
        {
            rowLimit = compoundHelper->getRowLimit();
            stopAfter = compoundHelper->getChooseNLimit();
        }
        if (!helper.canMatchAny())
            eof = true;
        else
        {
            numParts = 0;
            if (variableFileName)
            {
                OwnedRoxieString fileName(helper.getFileName());
                varFileInfo.setown(resolveLFN(fileName, isOpt));
                if (varFileInfo)
                {
                    Owned<IFilePartMap> map = varFileInfo->getFileMap();
                    if (map)
                        numParts = map->getNumParts();
                }
            }
            if (!numParts)
            {
                eof = true;
            }
            else if (useRemote())
            {
                remote->onStart(parentExtractSize, parentExtract);
                remote->setLimits(rowLimit, (unsigned __int64) -1, stopAfter);
                unsigned fileNo = 0;        // MORE - superfiles require us to do this per file part... maybe (needs thought)
                // Translation into a message per channel done elsewhere....
                remote->getMem(0, fileNo, 0);
                remote->flush();
                remote->senddone();
            }
            else
            {
                if (variableFileName)
                {
                    unsigned channel = isLocal ? factory->queryQueryFactory().queryChannel() : 0;
                    varFiles.setown(varFileInfo->getIFileIOArray(isOpt, channel));
                    manager.setown(varFileInfo->getIndexManager(isOpt, channel, varFiles, diskSize, false, 0));
                }
                assertex(manager != NULL);
                helper.createSegmentMonitors(this);
                if (cursor)
                {
                    isKeyed = cursor->selectKey();
                    cursor->reset();
                }
                if (!isKeyed)
                {
                    reader.setown(manager->createReader(0, 0, 1));
                    deserializeSource.setStream(reader);
                    prefetcher.setown(diskSize.queryOriginal()->createDiskPrefetcher(ctx->queryCodeContext(), activityId));
                }
                helper.setCallback(reader ? reader->queryThorDiskCallback() : cursor);
            }
        }
    }

    virtual void append(IKeySegmentMonitor *segment)
    {
        if (!segment->isWild())
        {
            if (!cursor)
                cursor.setown(manager->createCursor());
            cursor->append(segment);
        }
    }

    virtual unsigned ordinality() const
    {
        return cursor ? cursor->ordinality() : 0;
    }

    virtual IKeySegmentMonitor *item(unsigned idx) const
    {
        return cursor ? cursor->item(idx) : 0;
    }

    virtual void setMergeBarrier(unsigned barrierOffset)
    {
        // no merging so no issue...
    }

    virtual void stop(bool aborting)
    {
        if (useRemote())
            remote->onStop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        if (useRemote())
        {
            processed = remote->processed;
            remote->processed = 0;
            remote->onReset();
        }
        varFileInfo.clear();
        eof = false;
        if (cursor)
            cursor->reset();
        deserializeSource.clearStream();
        CRoxieServerActivity::reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        assertex(compoundHelper);
        if (isKeyed) // MORE does this exist for diskread? should it?
        {
            if (helper.getFlags() & (TDRkeyedlimitskips|TDRkeyedlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(true);
            }
            else
            {
                UNIMPLEMENTED;
                //compoundHelper->onKeyedLimitExceeded(); Doesn't exist - though the flags do... interesting...
            }
        }
        else
        {
            if (helper.getFlags() & (TDRlimitskips|TDRlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(false);
            }
            else
                compoundHelper->onLimitExceeded();
        }
    }

    virtual const void * createLimitFailRow(bool isKeyed)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        IHThorSourceLimitTransformExtra *limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(helper.selectInterface(TAIsourcelimittransformextra_1));
        assertex(limitTransformExtra);
        size32_t outSize = isKeyed ? limitTransformExtra->transformOnKeyedLimitExceeded(rowBuilder) : limitTransformExtra->transformOnLimitExceeded(rowBuilder);
        if (outSize)
            return rowBuilder.finalizeRowClear(outSize);
        return NULL;
    }
};

class CRoxieServerDiskReadActivity : public CRoxieServerDiskReadBaseActivity
{
    IHThorCompoundReadExtra * readHelper;
    ConstPointerArray readrows;
    bool readAheadDone;
    unsigned readIndex;

public:
    CRoxieServerDiskReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, bool _sorted, bool _maySkip, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskReadBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _sorted, _maySkip, _manager)
    {
        compoundHelper = (IHThorDiskReadArg *)&helper;
        readHelper = (IHThorDiskReadArg *)&helper;
        readAheadDone = false;
        readIndex = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        rowLimit = compoundHelper->getRowLimit();
        stopAfter = compoundHelper->getChooseNLimit();
        CRoxieServerDiskReadBaseActivity::start(parentExtractSize, parentExtract, paused);
        readAheadDone = false;
        readIndex = 0;
    }

    virtual void reset()
    {
        while (readrows.isItem(readIndex))
            ReleaseRoxieRow(readrows.item(readIndex++));
        readrows.kill();
        readAheadDone = false;
        readIndex = 0;
        CRoxieServerDiskReadBaseActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        if (eof)
            return NULL;
        else if (useRemote())
            return remote->nextInGroup();
        else if (maySkip)
        {
            if (!readAheadDone)
            {
                unsigned preprocessed = 0;
                while (!eof)
                {
                    const void *row = _nextInGroup();
                    if (row)
                        preprocessed++;
                    if (preprocessed > rowLimit)
                    {
                        ReleaseRoxieRow(row);
                        while (readrows.isItem(readIndex))
                            ReleaseRoxieRow(readrows.item(readIndex++));
                        readrows.kill();
                        eof = true;
                        if (ctx->queryDebugContext())
                            ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                        if (helper.getFlags() & TDRlimitskips)
                            return NULL;
                        else if (helper.getFlags() & TDRlimitcreates)
                            return createLimitFailRow(false);
                        else
                            throwUnexpected();
                    }
                    if (preprocessed > stopAfter) // MORE - bit of a strange place to check
                    {
                        eof = true;
                        ReleaseRoxieRow(row);
                        break;
                    }
                    readrows.append(row);
                }
                readAheadDone = true;
            }
            if (readrows.isItem(readIndex))
            {
                const void *ret = readrows.item(readIndex++);
                if (ret)
                    processed++;
                return ret;
            }
            else
            {
                eof = true;
                return NULL;
            }
        }
        else
        {
            const void *ret = _nextInGroup();
            if (ret)
            {
                processed++;
                if (processed > rowLimit)
                {
                    if (traceLevel > 4)
                        DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
                    ReleaseRoxieRow(ret);
                    compoundHelper->onLimitExceeded();
                    throwUnexpected(); // onLimitExceeded is not supposed to return
                }
                if (processed > stopAfter) // MORE - bit of a strange place to check
                {
                    eof = true;
                    ReleaseRoxieRow(ret);
                    return NULL;
                }
            }
            return ret;
        }
    }

    const void *_nextInGroup()
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        unsigned transformedSize = 0;
        if (isKeyed)
        {
            loop
            {
                const void *nextCandidate = cursor->nextMatch();
                if (!nextCandidate)
                {
                    eof = true;
                    return NULL;
                }
                transformedSize = readHelper->transform(rowBuilder, nextCandidate);
                if (transformedSize)
                    break;
            }
        }
        else // use reader...
        {
            assertex(reader != NULL);
            loop
            {
                if (deserializeSource.eos())
                {
                    eof = true;
                    return NULL;
                }
                prefetcher->readAhead(deserializeSource);
                const byte *nextRec = deserializeSource.queryRow();
                if (cursor && cursor->isFiltered(nextRec))
                    transformedSize = 0;
                else
                    transformedSize = readHelper->transform(rowBuilder, nextRec);
                deserializeSource.finishedRow();
                if (transformedSize)
                    break;
            }
        }
        return rowBuilder.finalizeRowClear(transformedSize);
    }
};

class CRoxieServerXmlReadActivity : public CRoxieServerDiskReadBaseActivity, implements IXMLSelect
{
    IHThorXmlReadArg * readHelper;
    Owned<IXmlToRowTransformer> rowTransformer;
    Owned<IXMLParse> xmlParser;
    Owned<IColumnProvider> lastMatch;
    unsigned __int64 localOffset;
public:
    IMPLEMENT_IINTERFACE;
    CRoxieServerXmlReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, bool _sorted, bool _maySkip, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskReadBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _sorted, _maySkip, _manager)
    {
        compoundHelper = NULL;
        readHelper = (IHThorXmlReadArg *)&helper;
        localOffset = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        rowLimit = readHelper->getRowLimit();
        stopAfter = readHelper->getChooseNLimit();
        CRoxieServerDiskReadBaseActivity::start(parentExtractSize, parentExtract, paused);
        if (!useRemote())
        {
            rowTransformer.set(readHelper->queryTransformer());
            assertex(reader != NULL);
            OwnedRoxieString xmlIterator(readHelper->getXmlIteratorPath());
            xmlParser.setown(createXMLParse(*reader->querySimpleStream(), xmlIterator, *this, (0 != (TDRxmlnoroot & readHelper->getFlags()))?ptr_noRoot:ptr_none, (readHelper->getFlags() & TDRusexmlcontents) != 0));
        }
    }

    virtual void reset()
    {
        CRoxieServerDiskReadBaseActivity::reset();
        rowTransformer.clear();
        xmlParser.clear();
    }


    virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
    {
        localOffset = startOffset;
        lastMatch.set(&entry);
    }

    virtual const void *nextInGroup()
    {
        if (eof)
            return NULL;
        else if (useRemote())
            return remote->nextInGroup();
        assertex(xmlParser != NULL);
        try
        {
            while (!eof)
            {
                //call to next() will callback on the IXmlSelect interface
                bool gotNext = false;
                gotNext = xmlParser->next();
                if(!gotNext)
                    eof = true;
                else if (lastMatch)
                {
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    unsigned sizeGot = rowTransformer->transform(rowBuilder, lastMatch, reader->queryThorDiskCallback());
                    lastMatch.clear();
                    localOffset = 0;
                    if (sizeGot)
                    {
                        OwnedConstRoxieRow ret = rowBuilder.finalizeRowClear(sizeGot); 
                        if (processed > rowLimit)
                        {
                            if (traceLevel > 4)
                                DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
                            readHelper->onLimitExceeded();
                            throwUnexpected(); // onLimitExceeded is not supposed to return
                        }
                        processed++;
                        if (processed > stopAfter) // MORE - bit of a strange place to check
                        {
                            eof = true;
                            return NULL;
                        }
                        return ret.getClear();
                    }
                }
            }
            return NULL;
        }
        catch(IException *E)
        {
            throw makeWrappedException(E);
        }
    }
};

class CRoxieServerCsvReadActivity : public CRoxieServerDiskReadBaseActivity
{
    IHThorCsvReadArg *readHelper;
    ICsvParameters * csvInfo;
    unsigned headerLines;
    unsigned maxDiskSize;
    CSVSplitter csvSplitter;    
    unsigned __int64 localOffset;
    const char *quotes;
    const char *separators;
    const char *terminators;
    const char *escapes;
public:
    CRoxieServerCsvReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
                                unsigned _numParts, bool _isLocal, bool _sorted, bool _maySkip, IInMemoryIndexManager *_manager,
                                const char *_quotes, const char *_separators, const char *_terminators, const char *_escapes)
        : CRoxieServerDiskReadBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _sorted, _maySkip, _manager),
          quotes(_quotes), separators(_separators), terminators(_terminators), escapes(_escapes)
    {
        compoundHelper = NULL;
        readHelper = (IHThorCsvReadArg *)&helper;
        rowLimit = readHelper->getRowLimit();
        stopAfter = readHelper->getChooseNLimit();
        csvInfo = readHelper->queryCsvParameters();
        maxDiskSize = csvInfo->queryMaxSize();
        localOffset = 0;
        headerLines = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        rowLimit = readHelper->getRowLimit();
        stopAfter = readHelper->getChooseNLimit();
        CRoxieServerDiskReadBaseActivity::start(parentExtractSize, parentExtract, paused);
        if (!useRemote())
        {
            headerLines = csvInfo->queryHeaderLen(); 
            if (headerLines && isLocal && reader->queryFilePart() != 1)
                headerLines = 0;  // MORE - you could argue that if SINGLE not specified, should skip from all parts. But it would be painful since we have already concatenated and no-one else does...
            if (!eof)
            {
                if (varFileInfo)
                {
                    const IPropertyTree *options = varFileInfo->queryProperties();
                    if (options)
                    {
                        quotes = options->queryProp("@csvQuote");
                        separators = options->queryProp("@csvSeparate");
                        terminators = options->queryProp("@csvTerminate");
                        escapes = options->queryProp("@csvEscape");
                    }
                }
                csvSplitter.init(readHelper->getMaxColumns(), csvInfo, quotes, separators, terminators, escapes);
            }
        }
    }

    virtual const void *nextInGroup()
    {
        if (eof)
            return NULL;
        else if (useRemote())
            return remote->nextInGroup();
        try
        {
            while (!eof)
            {
                if (reader->eos())
                {
                    eof = true;
                    break;
                }
                // MORE - there are rumours of a  csvSplitter that operates on a stream... if/when it exists, this should use it
                size32_t rowSize = 4096; // MORE - make configurable
                size32_t maxRowSize = 10*1024*1024; // MORE - make configurable
                size32_t thisLineLength;
                loop
                {
                    size32_t avail;
                    const void *peek = reader->peek(rowSize, avail);
                    thisLineLength = csvSplitter.splitLine(avail, (const byte *)peek);
                    if (thisLineLength < rowSize || avail < rowSize)
                        break;
                    if (rowSize == maxRowSize)
                        throw MakeStringException(0, "Row too big");
                    if (rowSize >= maxRowSize/2)
                        rowSize = maxRowSize;
                    else
                        rowSize += rowSize;
                }

                if (headerLines)
                {
                    headerLines--;
                    reader->skip(thisLineLength);
                }
                else
                {
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    unsigned transformedSize = readHelper->transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData());
                    reader->skip(thisLineLength);
                    if (transformedSize)
                    {
                        OwnedConstRoxieRow ret = rowBuilder.finalizeRowClear(transformedSize);
                        if (processed > rowLimit)
                        {
                            readHelper->onLimitExceeded();
                            throwUnexpected(); // onLimitExceeded is not supposed to return
                        }
                        processed++;
                        if (processed > stopAfter) // MORE - bit of a strange place to check
                        {
                            eof = true;
                            return NULL;
                        }
                        return ret.getClear();
                    }
                }
            }
            return NULL;
        }
        catch(IException *E)
        {
            throw makeWrappedException(E);
        }
    }
    virtual void reset()
    {
        CRoxieServerDiskReadBaseActivity::reset();
        csvSplitter.reset();
        localOffset = 0;
    }

    virtual unsigned __int64 getFilePosition(const void * row)
    {
        UNIMPLEMENTED; // we know offset in the reader but not sure it helps us much
    }

    virtual unsigned __int64 getLocalFilePosition(const void * row)
    {
        UNIMPLEMENTED;
    }

};

class CRoxieServerDiskNormalizeActivity : public CRoxieServerDiskReadBaseActivity
{
    IHThorDiskNormalizeArg *normalizeHelper;
    bool firstPending;

public:
    CRoxieServerDiskNormalizeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, bool _sorted, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskReadBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _sorted, false, _manager)
    {
        compoundHelper = (IHThorDiskNormalizeArg *)&helper;
        normalizeHelper = (IHThorDiskNormalizeArg *)&helper;
        firstPending = true;
    }

    virtual void reset()
    {
        firstPending = true;
        CRoxieServerDiskReadBaseActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        if (eof)
            return NULL;
        else if (useRemote())
            return remote->nextInGroup();
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        unsigned transformedSize = 0;
        if (isKeyed)
        {
            loop
            {
                while (firstPending)
                {
                    const void *nextCandidate = cursor->nextMatch();
                    if (!nextCandidate)
                    {
                        eof = true;
                        return NULL;
                    }
                    if (normalizeHelper->first(nextCandidate))
                    {
                        firstPending = false;
                        break;
                    }
                }
                transformedSize = normalizeHelper->transform(rowBuilder);
                firstPending = !normalizeHelper->next();
                if (transformedSize)
                    break;
            }
        }
        else
        {
            assertex(reader != NULL);
            loop
            {
                while (firstPending)
                {
                    if (deserializeSource.eos())
                    {
                        eof = true;
                        return NULL;
                    }
                    prefetcher->readAhead(deserializeSource);
                    const byte *nextRec = deserializeSource.queryRow();
                    if (!cursor || !cursor->isFiltered(nextRec))
                    {
                        if (normalizeHelper->first(nextRec))
                            firstPending = false;
                    }
                    deserializeSource.finishedRow();
                }
                transformedSize = normalizeHelper->transform(rowBuilder);
                firstPending = !normalizeHelper->next();
                if (transformedSize)
                    break;
            }
        }
        OwnedConstRoxieRow recBuffer = rowBuilder.finalizeRowClear(transformedSize);
        processed++;
        if (processed > rowLimit)
        {
            compoundHelper->onLimitExceeded();
            throwUnexpected(); // onLimitExceeded is not supposed to return
        }
        if (processed > stopAfter) // MORE - bit of a strange place to check
        {
            eof = true;
            return NULL;
        }
        return recBuffer.getClear();
    }
};


class CRoxieServerDiskAggregateBaseActivity : public CRoxieServerDiskReadBaseActivity
{
protected:
    bool done;

public:
    CRoxieServerDiskAggregateBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskReadBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, false, false, _manager),
          done(false)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerDiskReadBaseActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL: this;
    }
};

class CRoxieServerDiskCountActivity : public CRoxieServerDiskAggregateBaseActivity
{
    IHThorDiskCountArg & countHelper;
    unsigned __int64 choosenLimit;
    IHThorSourceCountLimit *limitHelper;

    unsigned __int64 getSkippedCount()
    {
        unsigned flags = countHelper.getFlags();
        if (flags & TDRlimitskips)
            return 0;
        else if (flags & TDRlimitcreates)
            return 1;
        else
        {
            assertex(limitHelper);
            if (traceLevel > 4)
                DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
            limitHelper->onLimitExceeded(); 
            throwUnexpected(); // onLimitExceeded should always throw exception
        }
    }

public:
    CRoxieServerDiskCountActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskAggregateBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _manager),
          countHelper((IHThorDiskCountArg &)basehelper)
    {
        limitHelper = static_cast<IHThorSourceCountLimit *>(basehelper.selectInterface(TAIsourcecountlimit_1));
        choosenLimit = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        choosenLimit = countHelper.getChooseNLimit();
        if (limitHelper)
        {
            rowLimit = limitHelper->getRowLimit();
//          keyedLimit = limitHelper->getKeyedLimit(); // more - should there be one?
        }
        CRoxieServerDiskAggregateBaseActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        done = true;

        unsigned __int64 totalCount = 0;
        if (helper.canMatchAny() && !eof)
        {
            if (useRemote())
            {
                loop
                {
                    const void * next = remote->nextInGroup();
                    if (!next)
                        break;
                    if (meta.getFixedSize() == 1)
                        totalCount += *(byte *)next;
                    else
                        totalCount += *(unsigned __int64 *)next;
                    ReleaseRoxieRow(next);
                    if (totalCount > rowLimit)
                    {
                        totalCount = getSkippedCount();
                        break;
                    }
                    else if (totalCount >= choosenLimit)
                    {
                        totalCount = choosenLimit;
                        break;
                    }
                }
            }
            else
            {
                if (isKeyed)
                {
                    loop
                    {
                        const void *nextCandidate = cursor->nextMatch();
                        if (!nextCandidate)
                            break;
                        totalCount += countHelper.numValid(nextCandidate);
                        if (totalCount > rowLimit)
                        {
                            totalCount = getSkippedCount();
                            break;
                        }
                        else if (totalCount >= choosenLimit)
                        {
                            totalCount = choosenLimit;
                            break;
                        }
                    }
                }
                else
                {
                    assertex(reader != NULL);
                    while (!deserializeSource.eos())
                    {
                        prefetcher->readAhead(deserializeSource);
                        const byte *nextRec = deserializeSource.queryRow();
                        if (!cursor || !cursor->isFiltered(nextRec))
                        {
                            totalCount += countHelper.numValid(nextRec);
                        }
                        deserializeSource.finishedRow();
                        if (totalCount > rowLimit)
                        {
                            totalCount = getSkippedCount();
                            break;
                        }
                        else if (totalCount >= choosenLimit)
                        {
                            totalCount = choosenLimit;
                            break;
                        }
                    }
                }
            }
        }
        size32_t rowSize = meta.getFixedSize();
        void * result = rowAllocator->createRow();
        if (rowSize == 1)
            *(byte *)result = (byte)totalCount;
        else
        {
            assertex(rowSize == sizeof(unsigned __int64));
            *(unsigned __int64 *)result = totalCount;
        }
        return rowAllocator->finalizeRow(rowSize, result, rowSize);
    }
};

class CRoxieServerDiskAggregateActivity : public CRoxieServerDiskAggregateBaseActivity
{
    IHThorCompoundAggregateExtra & aggregateHelper;

public:
    CRoxieServerDiskAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        unsigned _numParts, bool _isLocal, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskAggregateBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _manager),
          aggregateHelper((IHThorDiskAggregateArg &)basehelper)
    {
    }

    const void * gatherMerged()
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator, false);
        size32_t finalSize = 0;
        if (useRemote())
        {
            const void * firstRow = remote->nextInGroup();
            if (!firstRow)
            {
                rowBuilder.ensureRow();
                finalSize = aggregateHelper.clearAggregate(rowBuilder);
            }
            else
            {
                 // NOTE need to clone this because going to modify below, could special case 1 row only
                finalSize = cloneRow(rowBuilder, firstRow, meta);
                ReleaseRoxieRow(firstRow);
            }
            loop
            {
                const void * next = remote->nextInGroup();
                if (!next)
                    break;
                finalSize = aggregateHelper.mergeAggregate(rowBuilder, next);
                ReleaseRoxieRow(next);
            }
        }
        else
        {
            aggregateHelper.clearAggregate(rowBuilder);
            if (helper.canMatchAny() && !eof)
            {
                if (isKeyed)
                {
                    loop
                    {
                        const void *next = cursor->nextMatch();
                        if (!next)
                            break;
                        aggregateHelper.processRow(rowBuilder, next);
                    }
                }
                else
                {
                    assertex(reader != NULL);
                    while (!deserializeSource.eos())
                    {
                        prefetcher->readAhead(deserializeSource);
                        const byte *nextRec = deserializeSource.queryRow();
                        if (!cursor || !cursor->isFiltered(nextRec))
                        {
                            aggregateHelper.processRow(rowBuilder, nextRec);
                        }
                        deserializeSource.finishedRow();
                    }
                }
            }
            finalSize = meta.getRecordSize(rowBuilder.getSelf());
        }
        return rowBuilder.finalizeRowClear(finalSize);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        const void * ret = gatherMerged();
        done = true;
        return ret;
    }
};


class CRoxieServerDiskGroupAggregateActivity : public CRoxieServerDiskAggregateBaseActivity
{
    IHThorDiskGroupAggregateArg & aggregateHelper;
    RowAggregator resultAggregator;
    bool gathered;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieServerDiskGroupAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _numParts, bool _isLocal, IInMemoryIndexManager *_manager)
        : CRoxieServerDiskAggregateBaseActivity(_factory, _probeManager, _remoteId, _numParts, _isLocal, _manager),
          aggregateHelper((IHThorDiskGroupAggregateArg &)basehelper),
          resultAggregator(aggregateHelper, aggregateHelper),
          gathered(false)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        gathered= false;
        CRoxieServerDiskAggregateBaseActivity::start(parentExtractSize, parentExtract, paused);
        resultAggregator.start(rowAllocator);
    }

    virtual void reset()
    {
        resultAggregator.reset();
        CRoxieServerDiskAggregateBaseActivity::reset();
    }

    void gatherMerged()
    {
        if (useRemote())
        {
            loop
            {
                const void * next = remote->nextInGroup();
                if (!next)
                    break;
                resultAggregator.mergeElement(next);
                ReleaseRoxieRow(next);
            }
        }
        else
        {
            if (helper.canMatchAny() && !eof)
            {
                Owned<IInMemoryFileProcessor> processor = isKeyed ?
                    createKeyedGroupAggregateRecordProcessor(cursor, resultAggregator, aggregateHelper) :
                    createUnkeyedGroupAggregateRecordProcessor(cursor, resultAggregator, aggregateHelper, manager->createReader(0, 0, 1),
                                                               ctx->queryCodeContext(), activityId);
                processor->doQuery(NULL, 0, 0, 0);
            }
        }
        gathered = true;
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done)
            return NULL;

        if (!gathered)
            gatherMerged();

        Owned<AggregateRowBuilder> next = resultAggregator.nextResult();
        if (next)
        {
            processed++;
            return next->finalizeRowClear();
        }
        done = true;
        return NULL;
    }
};


class CRoxieServerDiskReadActivityFactory : public CRoxieServerActivityFactory
{
public:
    RemoteActivityId remoteId;
    bool isLocal;
    bool sorted;
    bool maySkip;
    bool variableFileName;
    Owned<IFilePartMap> map;
    Owned<IFileIOArray> files;
    Owned<IInMemoryIndexManager> manager;
    Owned<const IResolvedFile> datafile;
    const char *quotes;
    const char *separators;
    const char *terminators;
    const char *escapes;

    CRoxieServerDiskReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), remoteId(_remoteId)
    {
        isLocal = _graphNode.getPropBool("att[@name='local']/@value") && queryFactory.queryChannel()!=0;
        Owned<IHThorDiskReadBaseArg> helper = (IHThorDiskReadBaseArg *) helperFactory();
        sorted = (helper->getFlags() & TDRunsorted) == 0;
        variableFileName = allFilesDynamic || ((helper->getFlags() & (TDXvarfilename|TDXdynamicfilename)) != 0);
        maySkip = (helper->getFlags() & (TDRkeyedlimitskips|TDRkeyedlimitcreates|TDRlimitskips|TDRlimitcreates)) != 0;
        quotes = separators = terminators = escapes = NULL;
        if (!variableFileName)
        {
            bool isOpt = (helper->getFlags() & TDRoptional) != 0;
            OwnedRoxieString fileName(helper->getFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(fileName, isOpt, true, _queryFactory.queryWorkUnit()));
            if (datafile)
                map.setown(datafile->getFileMap());
            bool isSimple = (map && map->getNumParts()==1 && !_queryFactory.getDebugValueBool("disableLocalOptimizations", false));
            if (isLocal || isSimple)
            {
                if (datafile)
                {
                    unsigned channel = isLocal ? queryFactory.queryChannel() : 0;
                    files.setown(datafile->getIFileIOArray(isOpt, channel));
                    manager.setown(datafile->getIndexManager(isOpt, channel, files, helper->queryDiskRecordSize(), _graphNode.getPropBool("att[@name=\"preload\"]/@value", false), _graphNode.getPropInt("att[@name=\"_preloadSize\"]/@value", 0)));
                    const IPropertyTree *options = datafile->queryProperties();
                    if (options)
                    {
                        quotes = options->queryProp("@csvQuote");
                        separators = options->queryProp("@csvSeparate");
                        terminators = options->queryProp("@csvTerminate");
                        escapes = options->queryProp("@csvEscape");
                    }
                }
                else
                    manager.setown(getEmptyIndexManager());
            }
        }
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        unsigned numParts = map ? map->getNumParts() : 0;
        switch (kind)
        {
        case TAKcsvread:
            return new CRoxieServerCsvReadActivity(this, _probeManager, remoteId, numParts, isLocal, sorted, maySkip, manager,
                                                   quotes, separators, terminators, escapes);
        case TAKxmlread:
            return new CRoxieServerXmlReadActivity(this, _probeManager, remoteId, numParts, isLocal, sorted, maySkip, manager);
        case TAKdiskread:
            return new CRoxieServerDiskReadActivity(this, _probeManager, remoteId, numParts, isLocal, sorted, maySkip, manager);
        case TAKdisknormalize:
            return new CRoxieServerDiskNormalizeActivity(this, _probeManager, remoteId, numParts, isLocal, sorted, manager);
        case TAKdiskcount:
            return new CRoxieServerDiskCountActivity(this, _probeManager, remoteId, numParts, isLocal, manager);
        case TAKdiskaggregate:
            return new CRoxieServerDiskAggregateActivity(this, _probeManager, remoteId, numParts, isLocal, manager);
        case TAKdiskgroupaggregate:
            return new CRoxieServerDiskGroupAggregateActivity(this, _probeManager, remoteId, numParts, isLocal, manager);
        }
        throwUnexpected();
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for %s activity", getActivityText(kind));
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
    }
};

IRoxieServerActivityFactory *createRoxieServerDiskReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerDiskReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//=================================================================================

class CRoxieServerIndexActivity : public CRoxieServerActivity, implements IRoxieServerErrorHandler
{
protected:
    IHThorIndexReadBaseArg &indexHelper;
    IHThorSteppedSourceExtra * steppedExtra;
    Linked<IKeyArray> keySet;
    Linked<TranslatorArray> translators;
    CSkippableRemoteResultAdaptor remote;
    CIndexTransformCallback callback;
    bool sorted;
    bool variableFileName;
    bool variableInfoPending;
    bool isOpt;
    bool isLocal;
    unsigned __int64 rowLimit;
    unsigned __int64 keyedLimit;
    unsigned __int64 choosenLimit;
    unsigned accepted;
    unsigned rejected;
    unsigned seekGEOffset;
    Owned<IKeyManager> tlk;
    Owned<const IResolvedFile> varFileInfo;
    const RemoteActivityId &remoteId;

    void setVariableFileInfo()
    {
        OwnedRoxieString indexName(indexHelper.getFileName());
        varFileInfo.setown(resolveLFN(indexName, isOpt));
        if (varFileInfo)
        {
            translators.setown(new TranslatorArray) ;
            keySet.setown(varFileInfo->getKeyArray(factory->queryActivityMeta(), translators, isOpt, isLocal ? factory->queryQueryFactory().queryChannel() : 0, factory->queryQueryFactory().getEnableFieldTranslation()));
        }
        variableInfoPending = false;
    }

public:
    CRoxieServerIndexActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _sorted, bool _isLocal, bool _maySkip)
        : CRoxieServerActivity(_factory, _probeManager), 
          keySet(_keySet),
          translators(_translators),
          indexHelper((IHThorIndexReadBaseArg &)basehelper),
          remote(_remoteId, meta.queryOriginal(), indexHelper, *this, _sorted, false, _maySkip),
          remoteId(_remoteId),
          sorted(_sorted),
          isLocal(_isLocal) 
    {
        indexHelper.setCallback(&callback);
        steppedExtra = static_cast<IHThorSteppedSourceExtra *>(indexHelper.selectInterface(TAIsteppedsourceextra_1));
        variableFileName = allFilesDynamic || ((indexHelper.getFlags() & (TIRvarfilename|TIRdynamicfilename)) != 0);
        variableInfoPending = false;
        isOpt = (indexHelper.getFlags() & TIRoptional) != 0;
        seekGEOffset = 0;
//        started = false;
        rejected = accepted = 0;
        rowLimit = choosenLimit = keyedLimit = 0;
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        remote.onCreate(this, this, _ctx, _colocalParent);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        accepted = 0;
        rejected = 0;
        rowLimit = (unsigned __int64) -1;
        keyedLimit = (unsigned __int64 ) -1;
        choosenLimit = I64C(0x7fffffffffffffff);
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        remote.onStart(parentExtractSize, parentExtract);
        variableInfoPending = variableFileName;
    }

    void processAllKeys()
    {
        try
        {
            if (indexHelper.canMatchAny())
            {
                if (variableInfoPending)
                    setVariableFileInfo();
                remote.setLimits(rowLimit, keyedLimit, choosenLimit);
                if (keySet)
                {
                    // MORE - this recreates the segmonitors per part but not per fileno (which is a little backwards).
                    // With soft layout support may need to recreate per fileno too (i.e. different keys in a superkey have different layout) but never per partno
                    // However order is probably better to iterate fileno's inside partnos 
                    // MORE - also not properly supporting STEPPED I fear.
                    // A superkey that mixes single and multipart or tlk and roroot keys might be hard
                    for (unsigned partNo = 0; partNo < keySet->length(); partNo++)
                    {
                        IKeyIndexBase *thisBase = keySet->queryKeyPart(partNo);
                        if (thisBase)
                        {
                            unsigned fileNo = 0;
                            IKeyIndex *thisKey = thisBase->queryPart(fileNo);
                            if (seekGEOffset && !thisKey->isTopLevelKey())
                            {
                                tlk.setown(createSingleKeyMerger(thisKey, 0, seekGEOffset, this));
                            }
                            else
                            {
                                tlk.setown(createKeyManager(thisKey, 0, this));
                                tlk->setLayoutTranslator(translators->item(fileNo));
                            }
                            createSegmentMonitors(tlk);
                            if (queryTraceLevel() > 3 || ctx->queryProbeManager())
                            {
                                StringBuffer out;
                                printKeyedValues(out, tlk, indexHelper.queryDiskRecordSize());
                                CTXLOG("Using filter %s", out.str());
                                if (ctx->queryProbeManager())
                                    ctx->queryProbeManager()->setNodeProperty(this, "filter", out.str());
                            }
                            tlk->reset();
                            loop // for each file part
                            {
                                //block for TransformCallbackAssociation
                                {
                                    TransformCallbackAssociation associate(callback, tlk);
                                    if (thisKey->isTopLevelKey())
                                    {
                                        if (thisKey->isFullySorted())
                                        {
                                            while (tlk->lookup(false))
                                            {
                                                unsigned slavePart = (unsigned) tlk->queryFpos();
                                                if (slavePart)
                                                {
                                                    accepted++;
                                                    remote.getMem(slavePart, fileNo, 0);  // the cached context is all we need to send
                                                    if (sorted && numChannels>1)
                                                        remote.flush();  // don't combine parts if we need result sorted, except on a 1-way
                                                }
                                            }
                                        }
                                        else
                                        {
                                            // MORE - we could check whether there are any matching parts if we wanted.
                                            // If people are in the habit of sending null values that would be worthwhile
                                            remote.getMem(0, fileNo, 0);
                                        }
                                    }
                                    else
                                    {
                                        if (keyedLimit != (unsigned __int64) -1)
                                        {
                                            if ((indexHelper.getFlags() & TIRcountkeyedlimit) != 0)
                                            {
                                                unsigned __int64 count = tlk->checkCount(keyedLimit);
                                                if (count > keyedLimit)
                                                {
                                                    if (traceLevel > 4)
                                                        DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                                                    onLimitExceeded(true); 
                                                }
                                                tlk->reset();
                                            }
                                        }

                                        if (processSingleKey(thisKey, translators->item(fileNo)))
                                            break;
                                    }
                                }
                                if (++fileNo < thisBase->numParts())
                                {
                                    thisKey = thisBase->queryPart(fileNo);
                                    tlk->setKey(thisKey);
                                    tlk->setLayoutTranslator(translators->item(fileNo));
                                    tlk->reset();
                                }
                                else
                                    break;
                            }
                            tlk->releaseSegmentMonitors();
                            tlk->setKey(NULL);
                        }
                    }
                }
            }
            remote.flush();
            remote.senddone();
        }
        catch (IException *E)
        {
            remote.setException(E);
        }
    }

    virtual void createSegmentMonitors(IKeyManager *key)
    {
        indexHelper.createSegmentMonitors(key);
        key->finishSegmentMonitors();
    }

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans) = 0;

    virtual void reset()
    {
        if (accepted)
            noteStatistic(STATS_ACCEPTED, accepted, 1);
        if (rejected)
            noteStatistic(STATS_REJECTED, rejected, 1);
        remote.onReset();
        CRoxieServerActivity::reset();
        if (varFileInfo)
        {
            keySet.clear();
            varFileInfo.clear();
        }
        variableInfoPending = false;
    }

    virtual void stop(bool aborting)
    {
        remote.onStop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

};


class CRoxieServerIndexReadBaseActivity : public CRoxieServerIndexActivity
{
    IHThorSourceLimitTransformExtra * limitTransformExtra;
public:
    CRoxieServerIndexReadBaseActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _sorted, bool _isLocal, bool _maySkip)
        : CRoxieServerIndexActivity(_factory, _probeManager, _remoteId, _keySet, _translators, _sorted, _isLocal, _maySkip)
    {
        limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(indexHelper.selectInterface(TAIsourcelimittransformextra_1));
    }

    virtual void reset()
    {
        remote.onReset();
        CRoxieServerIndexActivity::reset();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        try
        {
            const void *ret = remote.nextInGroup();
            if (ret)
                processed++;
            return ret;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

protected:
    virtual const void * createLimitFailRow(bool isKeyed)
    {
        createRowAllocator();
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t outSize = isKeyed ? limitTransformExtra->transformOnKeyedLimitExceeded(rowBuilder) : limitTransformExtra->transformOnLimitExceeded(rowBuilder);
        if (outSize)
            return rowBuilder.finalizeRowClear(outSize);
        return NULL;
    }

};

class CRoxieServerIndexReadActivity : public CRoxieServerIndexReadBaseActivity, implements IIndexReadActivityInfo 
{
protected:
    IHThorCompoundReadExtra & readHelper;
    ISteppingMeta *rawMeta;
    CSteppingMeta steppingMeta;
    unsigned * seekSizes;
    bool optimizeSteppedPostFilter;
    ISteppingMeta * projectedMeta;
    unsigned maxSeekLookahead;

public:
    CRoxieServerIndexReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _sorted, bool _isLocal, bool _maySkip, unsigned _maxSeekLookahead)
        : CRoxieServerIndexReadBaseActivity(_factory, _probeManager, _remoteId, _keySet, _translators, _sorted, _isLocal, _maySkip),
          readHelper((IHThorIndexReadArg &)basehelper)
    {
        rawMeta = readHelper.queryRawSteppingMeta();
        unsigned flags = indexHelper.getFlags();
        optimizeSteppedPostFilter = (flags & TIRunfilteredtransform) != 0;
        seekSizes = NULL;
        maxSeekLookahead = _maxSeekLookahead;

        if (rawMeta)
        {
            const CFieldOffsetSize * fields = rawMeta->queryFields();
            unsigned maxFields = rawMeta->getNumFields();
            seekGEOffset = fields[0].offset;
            seekSizes = new unsigned[maxFields];
            seekSizes[0] = fields[0].size;
            for (unsigned i=1; i < maxFields; i++)
                seekSizes[i] = seekSizes[i-1] + fields[i].size;
            projectedMeta = readHelper.queryProjectedSteppingMeta();
            ISteppingMeta *useMeta = projectedMeta ? projectedMeta : rawMeta;
            remote.setMergeInfo(useMeta); // also need to consider superfile case where there is a mix of multiway and singleparts.. ?
            bool hasPostFilter = readHelper.transformMayFilter() && optimizeSteppedPostFilter;
            steppingMeta.init(useMeta, hasPostFilter);
        }
    }

    ~CRoxieServerIndexReadActivity()
    {
        delete [] seekSizes;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerIndexReadBaseActivity::start(parentExtractSize, parentExtract, paused);
        steppingMeta.setDistributed();
        if (steppedExtra)
            steppingMeta.setExtra(steppedExtra);
        rowLimit = readHelper.getRowLimit();
        keyedLimit = readHelper.getKeyedLimit();
        choosenLimit = readHelper.getChooseNLimit();
        if (!paused)
            processAllKeys();
    }

    class LazyLocalKeyReader : public CInterface, implements IMessageResult, implements IMessageUnpackCursor 
    {
    public:
        IMPLEMENT_IINTERFACE;

        virtual IMessageUnpackCursor *getCursor(roxiemem::IRowManager *rowMgr) const
        {
            Link();
            return const_cast<LazyLocalKeyReader*> (this);
        }
        virtual const void *getMessageHeader(unsigned &length) const
        {
            length = 0;
            return NULL;
        }
        virtual const void *getMessageMetadata(unsigned &length) const
        {
            length = 0;
            return NULL;
        }
        virtual void discard() const
        {
            // nothing to do.
        }
        unsigned keyedCount;
        unsigned matched;
        bool EOFseen;
        Owned<IKeyIndexSet> keySet;
        Owned<IKeyManager> tlk;
        CRoxieServerIndexReadActivity &owner;
        LazyLocalKeyReader(CRoxieServerIndexReadActivity &_owner, IKeyIndex *key, IRecordLayoutTranslator * trans)
            : owner(_owner)
        {
            keyedCount = 0;
            matched = 0;
            EOFseen = false;
            keySet.setown(createKeyIndexSet());
            keySet->addIndex(LINK(key));
            if (owner.seekGEOffset)
                tlk.setown(createKeyMerger(keySet, 0, owner.seekGEOffset, &owner));
            else
                tlk.setown(createKeyManager(keySet->queryPart(0), 0, &owner));
            tlk->setLayoutTranslator(trans);
            owner.indexHelper.createSegmentMonitors(tlk);
            tlk->finishSegmentMonitors();
            tlk->reset();
        }
        virtual const void *getNext(int length)
        {
            TransformCallbackAssociation associate(owner.callback, tlk);
            while (tlk->lookup(true))
            {
                keyedCount++;
                if (keyedCount > owner.keyedLimit)
                {
                    owner.onLimitExceeded(true); // Should throw exception
                    throwUnexpected();
                }
                size32_t transformedSize;
                RtlDynamicRowBuilder rowBuilder(owner.rowAllocator);
                byte const * keyRow = tlk->queryKeyBuffer(owner.callback.getFPosRef());
                try
                {
                    transformedSize = owner.readHelper.transform(rowBuilder, keyRow);
                    owner.callback.finishedRow();
                }
                catch (IException *E)
                {
                    throw owner.makeWrappedException(E);
                }
                if (transformedSize)
                {
                    OwnedConstRoxieRow result = rowBuilder.finalizeRowClear(transformedSize);
                    matched++;
                    if (matched > owner.rowLimit)
                    {
                        owner.onLimitExceeded(false); // Should throw exception
                        throwUnexpected();
                    }
                    if (matched > owner.choosenLimit) // MORE - bit of a strange place to check
                    {
                        break;
                    }
                    owner.accepted++;
                    return result.getClear();
                }
                else
                    owner.rejected++;
            }
            EOFseen = true;
            return NULL;
        }
        virtual bool atEOF() const
        {
            return EOFseen;
        }
        virtual bool isSerialized() const
        {
            return false;
        }
    };

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans)
    {
        createRowAllocator();
        remote.injectResult(new LazyLocalKeyReader(*this, key, trans));
        return false;
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
        {
            if (indexHelper.getFlags() & (TIRkeyedlimitskips|TIRkeyedlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(true);
            }
            else
                readHelper.onKeyedLimitExceeded();
        }
        else
        {
            if (indexHelper.getFlags() & (TIRlimitskips|TIRlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(false);
            }
            else
                readHelper.onLimitExceeded();
        }
    }

    virtual void serializeSkipInfo(MemoryBuffer &out, unsigned seekLen, const void *rawSeek, unsigned numFields, const void * seek, const SmartStepExtra &stepExtra) const
    {
        out.append((unsigned short) numFields);
        out.append((unsigned short) seekLen);
        out.append((unsigned short) stepExtra.queryFlags());
        IMultipleStepSeekInfo *seeks = stepExtra.queryExtraSeeks();
        if (seeks)
        {
            unsigned lookahead = 40000/seekLen;
            if (maxSeekLookahead && (lookahead > maxSeekLookahead))
                lookahead  = maxSeekLookahead;
            seeks->ensureFilled(seek, numFields, lookahead);

            unsigned serialized = 1; // rawseek is always serialized...
            unsigned patchLength = out.length();
            out.append(serialized);  // NOTE - we come back and patch with the actual value...
            out.append(seekLen, rawSeek);
            if (seeks->ordinality())
            {
                const void *lastSeek = rawSeek;
                byte *nextSeek = NULL;
                if (projectedMeta)
                    nextSeek = (byte *) alloca(seekLen);
                for (unsigned i = 0; i < seeks->ordinality(); i++)
                {
                    if (projectedMeta)
                    {
                        RtlStaticRowBuilder rowBuilder(nextSeek-seekGEOffset, seekGEOffset+seekLen);
                        readHelper.mapOutputToInput(rowBuilder, seeks->querySeek(i), numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
                    }
                    else
                        nextSeek = (byte *) seeks->querySeek(i)+seekGEOffset;
                    int diff = memcmp(nextSeek, lastSeek, seekLen);
                    if (diff > 0)
                    {
                        serialized++;
                        out.append(seekLen, nextSeek);
                        lastSeek = (const byte *) out.reserve(0) - seekLen;
                    }
                }
                unsigned length = out.length();
                out.setWritePos(patchLength);
                out.append(serialized);
                out.setWritePos(length);
            }
        }
        else
        {
            out.append(1);
            out.append(seekLen, rawSeek);
        }

    }

    virtual const void * nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        try
        {
            unsigned seeklen = 0;
            const void *rawSeek = NULL;
            if (seek && numFields)
            {
                seeklen = seekSizes[numFields-1];
                rawSeek = (const byte *)seek + seekGEOffset;
                if (projectedMeta)
                {
                    byte * temp = (byte *) alloca(seeklen);
                    RtlStaticRowBuilder rawBuilder(temp-seekGEOffset, seekGEOffset+seeklen);
                    readHelper.mapOutputToInput(rawBuilder, seek, numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
                    rawSeek = (byte *)temp;
                }
            }
            const void *ret = remote.nextSteppedGE(seek, rawSeek, numFields, seeklen, wasCompleteMatch, stepExtra);
            if (ret && wasCompleteMatch) // GH pleas confirm the wasCompleteMatch I just added here is right
                processed++;
            return ret;
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
    }

    virtual IInputSteppingMeta * querySteppingMeta()
    {
        if (rawMeta && steppingEnabled && ((indexHelper.getFlags() & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates)) == 0))
            return &steppingMeta;
        return NULL;
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity() 
    { 
        if (variableInfoPending)
            setVariableFileInfo();
        return this; 
    }
    virtual IKeyArray *getKeySet() const 
    {
        return keySet.getLink();
    }
    virtual const IResolvedFile *getVarFileInfo() const
    {
        return varFileInfo.getLink();
    }
    virtual TranslatorArray *getTranslators() const
    {
        return translators.getLink();
    }
    virtual void mergeSegmentMonitors(IIndexReadContext *irc) const
    {
        indexHelper.createSegmentMonitors(irc); // NOTE: they will merge
    }
    virtual IRoxieServerActivity *queryActivity() { return this; }
    virtual const RemoteActivityId& queryRemoteId() const 
    {
        return remoteId;
    }
};

class CRoxieServerSimpleIndexReadActivity : public CRoxieServerActivity, implements IIndexReadActivityInfo 
{
    IHThorCompoundReadExtra & readHelper;
    IHThorIndexReadBaseArg & indexHelper;
    IHThorSourceLimitTransformExtra * limitTransformExtra;
    IHThorSteppedSourceExtra * steppedExtra;
    bool eof;
    Linked<IKeyArray>keySet;
    Owned<IKeyIndexSet>keyIndexSet;
    Owned<IKeyManager> tlk;
    Linked<TranslatorArray> translators;
    CIndexTransformCallback callback;
    unsigned __int64 keyedLimit;
    unsigned rowLimit;
    unsigned chooseNLimit;
    unsigned accepted;
    unsigned rejected;
    unsigned keyedCount;
    ISteppingMeta * rawMeta;
    ISteppingMeta * projectedMeta;
    size32_t seekGEOffset;
    unsigned * seekSizes;
    CSteppingMeta steppingMeta;
    Owned<const IResolvedFile> varFileInfo;
    const RemoteActivityId &remoteId;
    bool firstRead;
    bool variableFileName;
    bool variableInfoPending;
    bool isOpt;
    bool isLocal;
    bool optimizeSteppedPostFilter;
    // MORE there may be enough in common between this and CRoxieServerIndexActivity to warrant some refactoring

    void initKeySet()
    {
        if ((keySet->length() > 1 || rawMeta != NULL) && translators->needsTranslation())
        {
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Layout translation is not available when merging key parts or smart-stepping, as it may change record order");
        }
        keyIndexSet.setown(createKeyIndexSet());
        for (unsigned part = 0; part < keySet->length(); part++)
        {
            IKeyIndexBase *kib = keySet->queryKeyPart(part);
            if (kib)
            {
                for (unsigned subpart = 0; subpart < kib->numParts(); subpart++)
                {
                    IKeyIndex *k = kib->queryPart(subpart);
                    if (k)
                    {
                        assertex(!k->isTopLevelKey());
                        keyIndexSet->addIndex(LINK(k));
                    }
                }
            }
        }
    }

    void setVariableFileInfo()
    {
        OwnedRoxieString indexName(indexHelper.getFileName());
        varFileInfo.setown(resolveLFN(indexName, isOpt));
        translators.setown(new TranslatorArray) ;
        keySet.setown(varFileInfo->getKeyArray(factory->queryActivityMeta(), translators, isOpt, isLocal ? factory->queryQueryFactory().queryChannel() : 0, factory->queryQueryFactory().getEnableFieldTranslation()));
        initKeySet();
        variableInfoPending = false;
    }

    void onEOF()
    {
        callback.setManager(NULL);
        eof = true;
        tlk.clear();
    }

public:
    CRoxieServerSimpleIndexReadActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
            IKeyArray *_keyArray, TranslatorArray *_translatorArray, bool _isLocal)
        : CRoxieServerActivity(_factory, _probeManager), 
          readHelper((IHThorIndexReadArg &)basehelper), 
          indexHelper((IHThorIndexReadArg &)basehelper), 
          translators(_translatorArray),
          keySet(_keyArray),
          isLocal(_isLocal),
          remoteId(_remoteId)
    {
        rowLimit = 0;
        keyedLimit = 0;
        chooseNLimit = 0;
        indexHelper.setCallback(&callback);
        steppedExtra = static_cast<IHThorSteppedSourceExtra *>(indexHelper.selectInterface(TAIsteppedsourceextra_1));
        limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(indexHelper.selectInterface(TAIsourcelimittransformextra_1));
        unsigned flags = indexHelper.getFlags();
        variableFileName = allFilesDynamic || ((flags & (TIRvarfilename|TIRdynamicfilename)) != 0);
        variableInfoPending = false;
        isOpt = (flags & TIRoptional) != 0;
        optimizeSteppedPostFilter = (flags & TIRunfilteredtransform) != 0;
        firstRead = true;
        accepted = 0;
        rejected = 0;
        keyedCount = 0;
        eof = false;
        rawMeta = readHelper.queryRawSteppingMeta();
        projectedMeta = readHelper.queryProjectedSteppingMeta();
        seekGEOffset = 0;
        seekSizes = NULL;
        if (rawMeta)
        {
            // MORE - should check all keys in maxFields list can actually be keyed.
            const CFieldOffsetSize * fields = rawMeta->queryFields();
            unsigned maxFields = rawMeta->getNumFields();
            seekGEOffset = fields[0].offset;
            seekSizes = new unsigned[maxFields];
            seekSizes[0] = fields[0].size;
            for (unsigned i=1; i < maxFields; i++)
                seekSizes[i] = seekSizes[i-1] + fields[i].size;
            bool hasPostFilter = readHelper.transformMayFilter() && optimizeSteppedPostFilter;
            if (projectedMeta)
                steppingMeta.init(projectedMeta, hasPostFilter);
            else
                steppingMeta.init(rawMeta, hasPostFilter);
        }
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    ~CRoxieServerSimpleIndexReadActivity()
    {
        delete [] seekSizes;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        firstRead = true;
        accepted = 0;
        rejected = 0;
        keyedCount = 0;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (steppedExtra)
            steppingMeta.setExtra(steppedExtra);
        eof = !indexHelper.canMatchAny();
        if (variableFileName)
            variableInfoPending = true;
        else
        {
            variableInfoPending = false;
            if (!keyIndexSet)
                initKeySet();
        }
    }

    virtual IIndexReadActivityInfo *queryIndexReadActivity() 
    { 
        if (variableInfoPending)
            setVariableFileInfo();
        return this; 
    }
    virtual IKeyArray *getKeySet() const 
    {
        return keySet.getLink();
    }
    virtual const IResolvedFile *getVarFileInfo() const
    {
        return varFileInfo.getLink();
    }
    virtual TranslatorArray *getTranslators() const
    {
        return translators.getLink();
    }
    virtual void mergeSegmentMonitors(IIndexReadContext *irc) const
    {
        indexHelper.createSegmentMonitors(irc); // NOTE: they will merge
    }
    virtual IRoxieServerActivity *queryActivity() { return this; }
    virtual const RemoteActivityId& queryRemoteId() const 
    {
        return remoteId;
    }
            
    const void *nextInGroup()
    {
        bool matched = true;
        return nextSteppedGE(NULL, 0, matched, dummySmartStepExtra);
    }

    virtual const void *nextSteppedGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;
        if (firstRead)
        {
            if (variableInfoPending)
                setVariableFileInfo();

            rowLimit = (unsigned) readHelper.getRowLimit();
            chooseNLimit = (unsigned) readHelper.getChooseNLimit();

            unsigned numParts = keyIndexSet->numParts();
            if (!numParts)
            {
                onEOF();
                return NULL;
            }
            if (numParts > 1 || seekGEOffset)
            {
                tlk.setown(createKeyMerger(keyIndexSet, 0, seekGEOffset, this));
                // note that we don't set up translator because we don't support it. If that ever changes...
            }
            else
            {
                tlk.setown(createKeyManager(keyIndexSet->queryPart(0), 0, this));
                tlk->setLayoutTranslator(translators->item(0));
            }
            indexHelper.createSegmentMonitors(tlk);
            tlk->finishSegmentMonitors();
            if (queryTraceLevel() > 3 || ctx->queryProbeManager())
            {
                StringBuffer out;
                printKeyedValues(out, tlk, indexHelper.queryDiskRecordSize());
                CTXLOG("Using filter %s", out.str());
                if (ctx->queryProbeManager())
                    ctx->queryProbeManager()->setNodeProperty(this, "filter", out.str());
            }
            tlk->reset();
            callback.setManager(tlk);

            keyedLimit = readHelper.getKeyedLimit();
            if (keyedLimit != (unsigned __int64) -1)
            {
                if ((indexHelper.getFlags() & TIRcountkeyedlimit) != 0)
                {
                    unsigned __int64 count = tlk->checkCount(keyedLimit);
                    if (count > keyedLimit)
                    {
                        if ((indexHelper.getFlags() & (TIRkeyedlimitskips|TIRkeyedlimitcreates)) == 0)
                            readHelper.onKeyedLimitExceeded(); 

                        const void * ret = NULL;
                        if (indexHelper.getFlags() & TIRkeyedlimitcreates)
                            ret = createKeyedLimitOnFailRow();
                        onEOF();
                        return ret;
                    }
                    tlk->reset();
                    keyedLimit = (unsigned __int64) -1;
                }
            }
            firstRead = false;
        }

        if (accepted == chooseNLimit)
        {
            onEOF();
            return NULL;
        }
    
        const byte * rawSeek = NULL;
        unsigned seekSize = 0;
        if (seek)
        {
            seekSize = seekSizes[numFields-1];
            rawSeek = (const byte *)seek + seekGEOffset;
            if (projectedMeta)
            {
                byte *temp = (byte *) alloca(seekSize);
                RtlStaticRowBuilder rawBuilder(temp-seekGEOffset, seekGEOffset+seekSize);
                readHelper.mapOutputToInput(rawBuilder, seek, numFields);// NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
                rawSeek = (byte *)temp;
            }
#ifdef _DEBUG
//          StringBuffer seekStr;
//          for (unsigned i = 0; i < seekSize; i++)
//          {
//              seekStr.appendf("%02x ", ((unsigned char *) rawSeek)[i]);
//          }
//          DBGLOG("nextSteppedGE can skip offset %d size %d value %s", seekGEOffset, seekSize, seekStr.str());
#endif
        }
        const byte * originalRawSeek = rawSeek;
        RtlDynamicRowBuilder rowBuilder(rowAllocator, false);
        while (rawSeek ? tlk->lookupSkip(rawSeek, seekGEOffset, seekSize) : tlk->lookup(true))
        {
            checkAbort();
            keyedCount++;
            if (keyedCount > keyedLimit)
            {
                readHelper.onKeyedLimitExceeded(); 
                break;
            }

            byte const * keyRow = tlk->queryKeyBuffer(callback.getFPosRef());
#ifdef _DEBUG
//          StringBuffer recstr;
//          unsigned size = (tlk->queryRecordSize()<80)  ? tlk->queryRecordSize() : 80;
//          for (unsigned i = 0; i < size; i++)
//          {
//              recstr.appendf("%02x ", ((unsigned char *) keyRow)[i]);
//          }
//          DBGLOG("nextSteppedGE Got %s", recstr.str());
            if (originalRawSeek && memcmp(keyRow + seekGEOffset, originalRawSeek, seekSize) < 0)
                assertex(!"smart seek failure");
#endif
            size32_t transformedSize;
            rowBuilder.ensureRow();
            try
            {
                transformedSize = readHelper.transform(rowBuilder, keyRow);
                //if the post filter causes a mismatch, and the stepping condition no longer matches
                //then return a mismatch record - so the join code can start seeking on the other input.
                if (transformedSize == 0 && optimizeSteppedPostFilter && stepExtra.returnMismatches())
                {
                    if (memcmp(keyRow + seekGEOffset, originalRawSeek, seekSize) != 0)
                    {
                        transformedSize = readHelper.unfilteredTransform(rowBuilder, keyRow);
                        if (transformedSize != 0)
                            wasCompleteMatch = false;
                    }
                }
                callback.finishedRow();
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
            if (transformedSize)
            {
                accepted++;
                if (accepted > rowLimit)
                {
                    if ((indexHelper.getFlags() & (TIRlimitskips|TIRlimitcreates)) != 0)
                    {
                        throwUnexpected(); // should not have used simple variant if maySkip set...
                    }
                    if (traceLevel > 4)
                        DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                    readHelper.onLimitExceeded();
                    break;
                }
                processed++;
#ifdef _DEBUG
//              const byte *ret = (const byte *) out.get();
//              CommonXmlWriter xmlwrite(XWFnoindent|XWFtrim|XWFopt);
//              queryOutputMeta()->toXML(ret, xmlwrite);
//              DBGLOG("ROW: {%p} %s", ret, xmlwrite.str());
#endif
                return rowBuilder.finalizeRowClear(transformedSize);
            }
            else
                rejected++;
            rawSeek = NULL;
        }
        onEOF();
        return NULL;
    }

    virtual void reset()
    {
        onEOF();
        if (accepted)
            noteStatistic(STATS_ACCEPTED, accepted, 1);
        if (rejected)
            noteStatistic(STATS_REJECTED, rejected, 1);
        if (variableFileName)
        {
            varFileInfo.clear();
            translators.clear();
        }
        variableInfoPending = false;
        CRoxieServerActivity::reset();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual IInputSteppingMeta * querySteppingMeta()
    {
        if (rawMeta && steppingEnabled && ((indexHelper.getFlags() & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates)) == 0))
            return &steppingMeta;
        return NULL;
    }

protected:
    const void * createKeyedLimitOnFailRow()
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t outSize = limitTransformExtra->transformOnKeyedLimitExceeded(rowBuilder);
        if (outSize)
            return rowBuilder.finalizeRowClear(outSize);
        return NULL;
    }
};

class CRoxieServerBaseIndexActivityFactory : public CRoxieServerActivityFactory
{

public:
    Owned<IKeyArray> keySet;
    Owned<TranslatorArray> translatorArray;
    Owned<IDefRecordMeta> activityMeta;
    RemoteActivityId remoteId;
    bool isSimple;
    bool isLocal;
    bool maySkip;
    bool sorted;
    bool variableFileName;
    bool enableFieldTranslation;
    unsigned maxSeekLookahead;
    Owned<const IResolvedFile> indexfile;

    CRoxieServerSideCache *cache;

    CRoxieServerBaseIndexActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), remoteId(_remoteId)
    {
        Owned<IHThorIndexReadBaseArg> indexHelper = (IHThorIndexReadBaseArg *) helperFactory();
        unsigned flags = indexHelper->getFlags();
        sorted = (flags & TIRunordered) == 0;
        isLocal = _graphNode.getPropBool("att[@name='local']/@value") && queryFactory.queryChannel()!=0;
        rtlDataAttr indexLayoutMeta;
        size32_t indexLayoutSize;
        if(!indexHelper->getIndexLayout(indexLayoutSize, indexLayoutMeta.refdata()))
            assertex(indexLayoutSize==0);
        MemoryBuffer m;
        m.setBuffer(indexLayoutSize, indexLayoutMeta.getdata());
        activityMeta.setown(deserializeRecordMeta(m, true));
        enableFieldTranslation = queryFactory.getEnableFieldTranslation();
        translatorArray.setown(new TranslatorArray);
        variableFileName = allFilesDynamic || ((flags & (TIRvarfilename|TIRdynamicfilename)) != 0);
        if (!variableFileName)
        {
            bool isOpt = (flags & TIRoptional) != 0;
            OwnedRoxieString indexName(indexHelper->getFileName());
            indexfile.setown(queryFactory.queryPackage().lookupFileName(indexName, isOpt, true, queryFactory.queryWorkUnit()));
            if (indexfile)
                keySet.setown(indexfile->getKeyArray(activityMeta, translatorArray, isOpt, isLocal ? queryFactory.queryChannel() : 0, enableFieldTranslation));
        }
        isSimple = isLocal;
        maySkip = (flags & (TIRkeyedlimitskips|TIRlimitskips|TIRlimitcreates|TIRkeyedlimitcreates)) != 0;

        if (keySet && keySet->length()==1 && !isLocal && (flags & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates))==0)
        {
            IKeyIndexBase *thisBase = keySet->queryKeyPart(0);
            if (thisBase->numParts()==1 && !thisBase->queryPart(0)->isTopLevelKey() && !_queryFactory.getDebugValueBool("disableLocalOptimizations", false))
                isSimple = true;
        }
        int cacheSize = _graphNode.getPropInt("hint[@name='cachehits']/@value", serverSideCacheSize);
        cache = cacheSize ? new CRoxieServerSideCache(cacheSize) : NULL;
        maxSeekLookahead = _graphNode.getPropInt("hint[@name='maxseeklookahead']/@value", 0);
    }

    ~CRoxieServerBaseIndexActivityFactory()
    {
        delete cache;
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (indexfile)
            addXrefFileInfo(reply, indexfile);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for indexread activity");
    }

    virtual IRoxieServerSideCache *queryServerSideCache() const
    {
        return cache;
    }

    virtual bool getEnableFieldTranslation() const
    {
        return enableFieldTranslation; 
    }

    virtual IDefRecordMeta *queryActivityMeta() const
    {
        return activityMeta;
    }

};

class CRoxieServerIndexReadActivityFactory : public CRoxieServerBaseIndexActivityFactory
{
public:
    CRoxieServerIndexReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerBaseIndexActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (!variableFileName && (keySet==NULL || keySet->length()==0))
            return new CRoxieServerNullActivity(this, _probeManager);
        else if (isSimple && !maySkip)
            return new CRoxieServerSimpleIndexReadActivity(this, _probeManager, remoteId, keySet, translatorArray, isLocal);
        else
            return new CRoxieServerIndexReadActivity(this, _probeManager, remoteId, keySet, translatorArray, sorted, isLocal, maySkip, maxSeekLookahead);
    }
};

IRoxieServerActivityFactory *createRoxieServerIndexReadActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerIndexReadActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//--------------------------------------------------------------------------------------------------------------------------

class CRoxieServerNullCountActivity : public CRoxieServerActivity
{
    bool done;
public:
    CRoxieServerNullCountActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
        done = false;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        done = true;
        size32_t rowSize = meta.getFixedSize();
        void * nullRow = rowAllocator->createRow();
        if (rowSize == 1)
            *(byte *)nullRow = 0;
        else
        {
            assertex(rowSize == sizeof(unsigned __int64));
            *(unsigned __int64 *)nullRow = 0;
        }
        return rowAllocator->finalizeRow(rowSize, nullRow, rowSize);
    }
};

class CRoxieServerIndexCountActivity : public CRoxieServerIndexActivity
{
    IHThorCompoundCountExtra & countHelper;
    IHThorSourceCountLimit * limitHelper;
    bool done;

public:
    CRoxieServerIndexCountActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, IKeyArray * _keySet, TranslatorArray *_translators, bool _isLocal)
        : CRoxieServerIndexActivity(_factory, _probeManager, _remoteId, _keySet, _translators, false, _isLocal, false),
          countHelper((IHThorIndexCountArg &)basehelper),
          done(false)
    {
        limitHelper = static_cast<IHThorSourceCountLimit *>(basehelper.selectInterface(TAIsourcecountlimit_1));
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerIndexActivity::start(parentExtractSize, parentExtract, paused);
        choosenLimit = countHelper.getChooseNLimit();
        if (limitHelper)
        {
            rowLimit = limitHelper->getRowLimit();
            keyedLimit = limitHelper->getKeyedLimit();
        }
        if (!paused)
            processAllKeys();
    }

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans)
    {
        unsigned __int64 count = 0;
        if (countHelper.hasFilter())
        {
            while (tlk->lookup(true))
            {
                try
                {
                    count += countHelper.numValid(tlk->queryKeyBuffer(callback.getFPosRef()));
                    callback.finishedRow();
                }
                catch (IException *E)
                {
                    throw makeWrappedException(E);
                }
                accepted++;
                if (count >= choosenLimit) // MORE - what about limit?
                    break;
            }
        }
        else
            count = tlk->getCount();        //MORE: GH->RKC There should be value in providing a max limit to getCount()

        if (count)
        {
            Owned<CRowArrayMessageResult> result = new CRowArrayMessageResult(ctx->queryRowManager(), false);
            if (count > choosenLimit)
                count = choosenLimit;

            void * recBuffer = rowAllocator->createRow();
            if (meta.getFixedSize() == 1)
                *(byte *)recBuffer = (byte)count;
            else
            {
                assertex(meta.getFixedSize() == sizeof(unsigned __int64));
                *(unsigned __int64 *)recBuffer = count;
            }
            recBuffer = rowAllocator->finalizeRow(meta.getFixedSize(), recBuffer, meta.getFixedSize());
            result->append(recBuffer);
            remote.injectResult(result.getClear());

            //GH->RKC for count(,choosen)/exists passing in the previous count would short-circuit this much earlier
            if (count >= choosenLimit)
                return true;
        }
        return false;
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
        {
            if (indexHelper.getFlags() & (TIRkeyedlimitskips|TIRkeyedlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(true);
            }
            else
            {
                assertex(limitHelper); // Should not be able to generate exception if there was not one...
                limitHelper->onKeyedLimitExceeded();
            }
        }
        else
        {
            if (indexHelper.getFlags() & (TIRlimitskips|TIRlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(false);
            }
            else
            {
                assertex(limitHelper);
                limitHelper->onLimitExceeded();
            }
        }
    }

    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        done = true;
        unsigned __int64 totalCount = 0;
        bool hasLimit = rowLimit != (unsigned __int64) -1;

        try
        {
            loop
            {
                const void * next = remote.nextInGroup();
                if (!next)
                    break;
                if (meta.getFixedSize() == 1)
                    totalCount += *(byte *)next;
                else
                    totalCount += *(unsigned __int64 *) next;
                ReleaseRoxieRow(next);
                if (totalCount > rowLimit || (totalCount > choosenLimit && !hasLimit)) // can't break out early if there is a possibility of later slave throwing limit exception
                    break;
            }
            if (totalCount > rowLimit)
            {
                unsigned flags = indexHelper.getFlags();
                if (flags & TIRlimitskips)
                    totalCount = 0;
                else if (flags & TIRlimitcreates)
                    totalCount = 1;
                else
                {
                    assertex(limitHelper);
                    limitHelper->onLimitExceeded(); 
                }
            }
            else if (totalCount > choosenLimit)
                totalCount = choosenLimit;
        }
        catch (IException *E)
        {
            if (QUERYINTERFACE(E, LimitSkipException))
            {
                totalCount = 0;
                unsigned flags = indexHelper.getFlags();
                if (E->errorCode() == KeyedLimitSkipErrorCode)
                {
                    if (flags & TIRkeyedlimitcreates)
                        totalCount++;
                }
                else
                {
                    if (flags & TIRlimitcreates)
                        totalCount++;
                }
                if (totalCount > choosenLimit)
                    totalCount = choosenLimit; // would have to be weird code (and escape the optimizer...)
                E->Release();
            }
            else
                throw ;
        }

        void * result = rowAllocator->createRow();
        if (meta.getFixedSize() == 1)
            *(byte *)result = (byte)totalCount;
        else
        {
            assertex(meta.getFixedSize() == sizeof(unsigned __int64));
            *(unsigned __int64 *)result = totalCount;
        }
        return rowAllocator->finalizeRow(meta.getFixedSize(), result, meta.getFixedSize());
    }
};

class CRoxieServerIndexCountActivityFactory : public CRoxieServerBaseIndexActivityFactory
{
public:
    CRoxieServerIndexCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerBaseIndexActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (!variableFileName && (keySet==NULL || keySet->length()==0))
            return new CRoxieServerNullCountActivity(this, _probeManager);
//      else if (isSimple)
//          return new CRoxieServerSimpleIndexCountActivity(this, keySet->queryKeyPart(0)->queryPart(0));
        else
            return new CRoxieServerIndexCountActivity(this, _probeManager, remoteId, keySet, translatorArray, isLocal);
    }
};

IRoxieServerActivityFactory *createRoxieServerIndexCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerIndexCountActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//--------------------------------------------------------------------------------------------------------------------------

class CRoxieServerNullIndexAggregateActivity : public CRoxieServerActivity
{
    IHThorIndexAggregateArg &aggregateHelper;
    bool done;
public:
    CRoxieServerNullIndexAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager),
          aggregateHelper((IHThorIndexAggregateArg &)basehelper)
    {
        done = false;
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        done = true;
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t thisSize = aggregateHelper.clearAggregate(rowBuilder);
        return rowBuilder.finalizeRowClear(thisSize);
    }
};

class CRoxieServerIndexAggregateActivity : public CRoxieServerIndexActivity
{
    IHThorCompoundAggregateExtra & aggregateHelper;
    bool done;

public:
    CRoxieServerIndexAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _isLocal)
        : CRoxieServerIndexActivity(_factory, _probeManager, _remoteId, _keySet, _translators, false, _isLocal, false),
          aggregateHelper((IHThorIndexAggregateArg &)basehelper),
          done(false)
    {
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        done = false;
        CRoxieServerIndexActivity::start(parentExtractSize, parentExtract, paused);
        if (!paused)
            processAllKeys();
    }

    virtual bool needsAllocator() const { return true; }

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator, false);
        while (tlk->lookup(true))
        {
            if (!rowBuilder.exists())
            {
                rowBuilder.ensureRow();
                aggregateHelper.clearAggregate(rowBuilder);
            }
            try
            {
                aggregateHelper.processRow(rowBuilder, tlk->queryKeyBuffer(callback.getFPosRef()));
                callback.finishedRow();
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
            accepted++;
        }
        if (aggregateHelper.processedAnyRows())
        {
            size32_t size = meta.getRecordSize(rowBuilder.getSelf());
            const void * recBuffer = rowBuilder.finalizeRowClear(size);
            Owned<CRowArrayMessageResult> result = new CRowArrayMessageResult(ctx->queryRowManager(), meta.isVariableSize());
            result->append(recBuffer);
            remote.injectResult(result.getClear());
        }
        return false;
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        throwUnexpected();
    }

    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

    const void * gatherMerged()
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator, false);
        const void * firstRow = remote.nextInGroup();
        size32_t finalSize = 0;
        if (!firstRow)
        {
            rowBuilder.ensureRow();
            finalSize = aggregateHelper.clearAggregate(rowBuilder);
        }
        else
        {
             // NOTE need to clone this because going to modify below, could special case 1 row only
            finalSize = cloneRow(rowBuilder, firstRow, meta);
            ReleaseRoxieRow(firstRow);
        }
        loop
        {
            const void * next = remote.nextInGroup();
            if (!next)
                break;
            finalSize = aggregateHelper.mergeAggregate(rowBuilder, next);
            ReleaseRoxieRow(next);
        }
        return rowBuilder.finalizeRowClear(finalSize);
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (done) return NULL;
        const void * ret = gatherMerged();
        done = true;
        return ret;
    }
};

class CRoxieServerIndexAggregateActivityFactory : public CRoxieServerBaseIndexActivityFactory
{
public:
    CRoxieServerIndexAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerBaseIndexActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (!variableFileName && (keySet==NULL || keySet->length()==0))
            return new CRoxieServerNullIndexAggregateActivity(this, _probeManager);
//      else if (isSimple)
//          return new CRoxieServerSimpleIndexAggregateActivity(this, keySet->queryKeyPart(0)->queryPart(0));
        else
            return new CRoxieServerIndexAggregateActivity(this, _probeManager, remoteId, keySet, translatorArray, isLocal);
    }
};

IRoxieServerActivityFactory *createRoxieServerIndexAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerIndexAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//--------------------------------------------------------------------------------------------------------------------------

class CRoxieServerIndexGroupAggregateActivity : public CRoxieServerIndexActivity, implements IHThorGroupAggregateCallback
{
    IHThorCompoundGroupAggregateExtra & aggregateHelper;
    RowAggregator singleAggregator;
    RowAggregator resultAggregator;
    unsigned groupSegCount;
    bool gathered;
    bool eof;

public:
    CRoxieServerIndexGroupAggregateActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _isLocal)
        : CRoxieServerIndexActivity(_factory, _probeManager, _remoteId, _keySet, _translators, false, _isLocal, false),
          aggregateHelper((IHThorIndexGroupAggregateArg &)basehelper),
          singleAggregator(aggregateHelper, aggregateHelper),
          resultAggregator(aggregateHelper, aggregateHelper),
          gathered(false), eof(true)
    {
        groupSegCount = 0;
    }

    IMPLEMENT_IINTERFACE

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        gathered= false;
        CRoxieServerIndexActivity::start(parentExtractSize, parentExtract, paused);
        groupSegCount = 0;
        if (!paused)
            processAllKeys();
        resultAggregator.start(rowAllocator);
    }

    virtual bool needsAllocator() const { return true; }

    virtual void reset()
    {
        resultAggregator.reset();
        CRoxieServerIndexActivity::reset();
    }

    virtual void processRow(const void * next)
    {
        singleAggregator.addRow(next);
    }

    virtual void createSegmentMonitors(IKeyManager *key)
    {
        unsigned groupSegSize;
        ThorActivityKind kind = factory->getKind();
        if ((kind==TAKindexgroupcount || kind==TAKindexgroupexists)) 
            groupSegSize = aggregateHelper.getGroupSegmentMonitorsSize();
        else
            groupSegSize = 0;
        if (groupSegSize)
        {
            key->setMergeBarrier(groupSegSize);
            CRoxieServerIndexActivity::createSegmentMonitors(key);
            unsigned numSegs = tlk->ordinality();
            for (unsigned segNo = 0; segNo < numSegs; segNo++)
            {
                IKeySegmentMonitor *seg = tlk->item(segNo);
                if (seg->getOffset()+seg->getSize()==groupSegSize)
                {
                    groupSegCount = segNo+1;
                    break;
                }
            }
            assertex(groupSegCount);
        }
        else
            CRoxieServerIndexActivity::createSegmentMonitors(key);
    }

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans)
    {
        Owned<CRowArrayMessageResult> result = new CRowArrayMessageResult(ctx->queryRowManager(), meta.isVariableSize());
        singleAggregator.start(rowAllocator);
        ThorActivityKind kind = factory->getKind();
        while (tlk->lookup(true))
        {
            try
            {
                if (groupSegCount && !trans)
                {
                    AggregateRowBuilder &rowBuilder = singleAggregator.addRow(tlk->queryKeyBuffer(callback.getFPosRef()));
                    callback.finishedRow();
                    if (kind==TAKindexgroupcount)                   
                    {
                        unsigned __int64 count = tlk->getCurrentRangeCount(groupSegCount);
                        aggregateHelper.processCountGrouping(rowBuilder, count-1);
                    }
                    if (!tlk->nextRange(groupSegCount))
                        break;
                }
                else
                {
                    aggregateHelper.processRow(tlk->queryKeyBuffer(callback.getFPosRef()), this);
                    callback.finishedRow();
                }
            }
            catch (IException *E)
            {
                throw makeWrappedException(E);
            }
            accepted++;
        }

        loop
        {
            Owned<AggregateRowBuilder> next = singleAggregator.nextResult();
            if (!next)
                break;
            size32_t size = next->querySize();
            result->append(next->finalizeRowClear());
        }
        remote.injectResult(result.getClear());
        singleAggregator.reset();
        return false;
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);DBGLOG("%d  activityid = %d", __LINE__, activityId);
        throwUnexpected();
    }

    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

    void gatherMerged()
    {
        gathered = true;
        loop
        {
            const void * next = remote.nextInGroup();
            if (!next)
                break;
            resultAggregator.mergeElement(next);
            ReleaseRoxieRow(next);
        }
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if (eof)
            return NULL;

        if (!gathered)
            gatherMerged();

        Owned<AggregateRowBuilder> next = resultAggregator.nextResult();
        if (next)
        {
            processed++;
            return next->finalizeRowClear();
        }
        eof = true;
        return NULL;
    }
};

class CRoxieServerIndexGroupAggregateActivityFactory : public CRoxieServerBaseIndexActivityFactory
{
public:
    CRoxieServerIndexGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerBaseIndexActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (!variableFileName && (keySet==NULL || keySet->length()==0))
            return new CRoxieServerNullActivity(this, _probeManager);
//      else if (isSimple)
//          return new CRoxieServerSimpleIndexGroupAggregateActivity(this, keySet->queryKeyPart(0)->queryPart(0));
        else
            return new CRoxieServerIndexGroupAggregateActivity(this, _probeManager, remoteId, keySet, translatorArray, isLocal);
    }
};

IRoxieServerActivityFactory *createRoxieServerIndexGroupAggregateActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerIndexGroupAggregateActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//--------------------------------------------------------------------------------------------------------------------------

class CRoxieServerIndexNormalizeActivity : public CRoxieServerIndexReadBaseActivity
{
    IHThorCompoundNormalizeExtra & readHelper;

public:
    CRoxieServerIndexNormalizeActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId,
        IKeyArray * _keySet, TranslatorArray *_translators, bool _sorted, bool _isLocal)
        : CRoxieServerIndexReadBaseActivity(_factory, _probeManager, _remoteId, _keySet, _translators, _sorted, _isLocal, false),
          readHelper((IHThorIndexNormalizeArg &)basehelper)
    {
    }

    virtual bool needsAllocator() const { return true; }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerIndexReadBaseActivity::start(parentExtractSize, parentExtract, paused);
        rowLimit = readHelper.getRowLimit();
        keyedLimit = readHelper.getKeyedLimit();
        if (!paused)
            processAllKeys();
    }

    virtual bool processSingleKey(IKeyIndex *key, IRecordLayoutTranslator * trans)
    {
        unsigned keyedCount = 0;
        RtlDynamicRowBuilder rowBuilder(rowAllocator, false);
        while (tlk->lookup(true))
        {
            keyedCount++;
            if (keyedCount > keyedLimit)
            {
                if (traceLevel > 4)
                    DBGLOG("activityid = %d  line = %d", activityId, __LINE__);
                onLimitExceeded(true); 
                break;
            }
            size32_t transformedSize;
    
            if (readHelper.first(tlk->queryKeyBuffer(callback.getFPosRef())))
            {
                Owned<CRowArrayMessageResult> result = new CRowArrayMessageResult(ctx->queryRowManager(), meta.isVariableSize());
                do
                {
                    rowBuilder.ensureRow();
                    try
                    {
                        transformedSize = readHelper.transform(rowBuilder);
                    }
                    catch (IException *E)
                    {
                        throw makeWrappedException(E);
                    }

                    if (transformedSize)
                    {
                        // MORE - would be a good idea to stop these asap if rowlimit exceeded
                        result->append(rowBuilder.finalizeRowClear(transformedSize));
                        accepted++;
                    }
                    else
                        rejected++;
                } while (readHelper.next());
                remote.injectResult(result.getClear());
                callback.finishedRow();
            }
        }
        return false;
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
        {
            if (indexHelper.getFlags() & (TIRkeyedlimitskips|TIRkeyedlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(true);
            }
            else
                readHelper.onKeyedLimitExceeded();
        }
        else
        {
            if (indexHelper.getFlags() & (TIRlimitskips||TIRlimitcreates))
            {
                if (ctx->queryDebugContext())
                    ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                throw makeLimitSkipException(false);
            }
            else
                readHelper.onLimitExceeded();
        }
    }

    virtual const void *createLimitFailRow(bool isKeyed)
    {
        UNIMPLEMENTED;
    }

};

class CRoxieServerIndexNormalizeActivityFactory : public CRoxieServerBaseIndexActivityFactory
{
public:
    CRoxieServerIndexNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerBaseIndexActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (!variableFileName && (keySet==NULL || keySet->length()==0))
            return new CRoxieServerNullActivity(this, _probeManager);
        else
            return new CRoxieServerIndexNormalizeActivity(this, _probeManager, remoteId, keySet, translatorArray, sorted, isLocal);
    }
};

IRoxieServerActivityFactory *createRoxieServerIndexNormalizeActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerIndexNormalizeActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

//=================================================================================

class CRoxieServerCountDiskActivity : public CRoxieServerActivity, implements IRoxieServerErrorHandler
{
    unsigned __int64 answer;

public:
    CRoxieServerCountDiskActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, unsigned __int64 _answer)
        : CRoxieServerActivity(_factory, _probeManager), 
          answer(_answer)
    {
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual __int64 evaluate() 
    {
        return answer;
    }

    virtual void onLimitExceeded(bool isKeyed) 
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        throwUnexpected();
    }

    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

};

class CRoxieServerVariableCountDiskActivity : public CRoxieServerActivity, implements IRoxieServerErrorHandler
{

public:
    CRoxieServerVariableCountDiskActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager)
    {
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected();
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() called for source activity");
    }

    virtual __int64 evaluate() 
    {
        IHThorCountFileArg &helper = (IHThorCountFileArg &) basehelper;
        bool isOpt = (helper.getFlags() & TDRoptional) != 0;
        unsigned recsize = helper.queryRecordSize()->getFixedSize();
        assertex(recsize);
        OwnedRoxieString fname(helper.getFileName());
        Owned<const IResolvedFile> varFileInfo = resolveLFN(fname, isOpt);
        return varFileInfo->getFileSize() / recsize; 
    }

    virtual void onLimitExceeded(bool isKeyed) 
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        throwUnexpected();
    }
    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }
};

class CRoxieServerCountDiskActivityFactory : public CRoxieServerActivityFactory
{
public:
    unsigned __int64 answer;
    bool variableFileName;
    Owned<const IResolvedFile> datafile;

    CRoxieServerCountDiskActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        Owned<IHThorCountFileArg> helper = (IHThorCountFileArg *) helperFactory();
        variableFileName = allFilesDynamic || ((helper->getFlags() & (TDXvarfilename|TDXdynamicfilename)) != 0);
        assertex(helper->queryRecordSize()->isFixedSize());
        if (!variableFileName)
        {
            unsigned recsize = helper->queryRecordSize()->getFixedSize();
            assertex(recsize);
            OwnedRoxieString fileName(helper->getFileName());
            bool isOpt = (helper->getFlags() & TDRoptional) != 0;
            datafile.setown(queryFactory.queryPackage().lookupFileName(fileName, isOpt, true, queryFactory.queryWorkUnit()));
            offset_t filesize = datafile ? datafile->getFileSize() : 0;
            if (filesize % recsize != 0)
                throw MakeStringException(ROXIE_MISMATCH, "Record size mismatch for file %s - %"I64F"d is not a multiple of fixed record size %d", fileName.get(), filesize, recsize);
            answer = filesize / recsize; 
        }
        else
            answer = 0;
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
    }

    virtual IRoxieServerActivity *createFunction(IHThorArg &arg, IProbeManager *_probeManager) const
    {
        arg.Release();
        if (variableFileName)
            return new CRoxieServerVariableCountDiskActivity(this, _probeManager);
        else
            return new CRoxieServerCountDiskActivity(this, _probeManager, answer);
    }

    virtual void setInput(unsigned idx, unsigned source, unsigned sourceidx)
    {
        throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() should not be called for CountDisk activity");
    }

    virtual bool isFunction() const
    {
        return true;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return NULL;
    }
};

IRoxieServerActivityFactory *createRoxieServerDiskCountActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode)
{
    return new CRoxieServerCountDiskActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _graphNode);
}

//=================================================================================

class CRoxieServerFetchActivity : public CRoxieServerActivity, implements IRecordPullerCallback, implements IRoxieServerErrorHandler
{
    IHThorFetchBaseArg &helper;
    IHThorFetchContext * fetchContext;
    Linked<IFilePartMap> map;
    CRemoteResultAdaptor remote;
    RecordPullerThread puller;
    bool needsRHS;
    bool variableFileName;
    bool isOpt;
    Owned<const IResolvedFile> varFileInfo;

public:
    CRoxieServerFetchActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, IFilePartMap *_map)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorFetchBaseArg &)basehelper), map(_map), remote(_remoteId, meta.queryOriginal(), helper, *this, true, true), puller(false)
    {
        fetchContext = static_cast<IHThorFetchContext *>(helper.selectInterface(TAIfetchcontext_1));
        needsRHS = helper.transformNeedsRhs();
        variableFileName = allFilesDynamic || ((fetchContext->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        isOpt = (fetchContext->getFetchFlags() & FFdatafileoptional) != 0;
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        remote.onCreate(this, this, _ctx, _colocalParent);
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (idx)
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 
        puller.setInput(this, _in);
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        remote.onStart(parentExtractSize, parentExtract);
        remote.setLimits(helper.getRowLimit(), (unsigned __int64) -1, I64C(0x7FFFFFFFFFFFFFFF));
        if (variableFileName)
        {
            OwnedRoxieString fname(fetchContext->getFileName());
            varFileInfo.setown(resolveLFN(fname, isOpt));
            if (varFileInfo)
                map.setown(varFileInfo->getFileMap());
        }
        puller.start(parentExtractSize, parentExtract, paused, ctx->fetchPreload(), false, ctx);
    }

    virtual void stop(bool aborting)
    {
        // Called from remote, so no need to call back to it....
        puller.stop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        processed = remote.processed;
        remote.processed = 0;
        puller.reset();
        if (variableFileName)
        {
            varFileInfo.clear();
            map.clear();
        }
        CRoxieServerActivity::reset();
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL : &remote;
    }

    virtual void processRow(const void *row)
    {
        // called from puller thread
        offset_t rp = fetchContext->extractPosition(row);
        unsigned partNo;
        if (isLocalFpos(rp))
            partNo = getLocalFposPart(rp) + 1;
        else
            partNo = map->mapOffset(rp);
        if (needsRHS)
        {
            Owned<IEngineRowAllocator> extractAllocator = ctx->queryCodeContext()->getRowAllocator(helper.queryExtractedSize(), activityId);
            RtlDynamicRowBuilder rb(extractAllocator, true);
            unsigned rhsSize = helper.extractJoinFields(rb, row);
            char * block = (char *) remote.getMem(partNo, 0, sizeof(rp) + sizeof(rhsSize) + rhsSize); // MORE - superfiles
            *(offset_t *) block = rp;
            block += sizeof(rp);
            *(unsigned *) block = rhsSize;
            block += sizeof(rhsSize);
            memcpy(block, rb.row(), rhsSize);
        }
        else
            *(offset_t *) remote.getMem(partNo, 0, sizeof(rp)) = rp; // MORE - superfiles
        ReleaseRoxieRow(row);
    }

    void processEOG()
    {
#ifdef FETCH_PRESERVES_GROUPING
        UNIMPLEMENTED;
#endif
        // else discard is correct
    }

    void processGroup(const ConstPointerArray &)
    {
        throwUnexpected();
    }

    void processDone()
    {
        // called from puller thread
        remote.flush();
        remote.senddone();
    }

    virtual bool fireException(IException *e)
    {
        return remote.fireException(e);
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
            throwUnexpected();
        helper.onLimitExceeded();
    }
    virtual const void *createLimitFailRow(bool isKeyed)
    {
        UNIMPLEMENTED;
    }


    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

class CRoxieServerFetchActivityFactory : public CRoxieServerActivityFactory
{
    RemoteActivityId remoteId;
    Owned<IFilePartMap> map;
    bool variableFileName;
    Owned<const IResolvedFile> datafile;
public:
    CRoxieServerFetchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), remoteId(_remoteId)
    {
        Owned<IHThorFetchBaseArg> helper = (IHThorFetchBaseArg *) helperFactory();
        IHThorFetchContext *fetchContext = static_cast<IHThorFetchContext *>(helper->selectInterface(TAIfetchcontext_1));
        variableFileName = allFilesDynamic || ((fetchContext->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        if (!variableFileName)
        {
            OwnedRoxieString fname(fetchContext->getFileName());
            datafile.setown(_queryFactory.queryPackage().lookupFileName(fname,
                                                                        (fetchContext->getFetchFlags() & FFdatafileoptional) != 0,
                                                                        true,
                                                                        _queryFactory.queryWorkUnit()));
            if (datafile)
                map.setown(datafile->getFileMap());
        }
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerFetchActivity(this, _probeManager, remoteId, map);
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
    }
};

IRoxieServerActivityFactory *createRoxieServerFetchActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, IPropertyTree &_graphNode)
{
    return new CRoxieServerFetchActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _graphNode);
}

// MORE - is there any point keeping this now?

class CRoxieServerDummyActivityFactory : public CRoxieServerActivityFactory  // not a real activity - just used to properly link files
{
public:
    Owned<const IResolvedFile> indexfile;
    Owned<const IResolvedFile> datafile;
    Owned<IKeyArray> keySet;
    Owned<IFileIOArray> files;
    Owned<IFilePartMap> map;
    TranslatorArray layoutTranslators;

    CRoxieServerDummyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool isLoadDataOnly)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
        try  // does not want any missing file errors to be fatal, or throw traps - just log it
        {
            if (_graphNode.getPropBool("att[@name='_isSpill']/@value", false) || _graphNode.getPropBool("att[@name='_isSpillGlobal']/@value", false))
                return;  // ignore 'spills'
            bool isLocal = _graphNode.getPropBool("att[@name='local']/@value") && queryFactory.queryChannel()!=0;
            bool isOpt = _graphNode.getPropBool("att[@name='_isOpt']/@value") || pretendAllOpt;
            if (queryNodeIndexName(_graphNode))
            {
                indexfile.setown(queryFactory.queryPackage().lookupFileName(queryNodeIndexName(_graphNode), isOpt, true, queryFactory.queryWorkUnit()));
                if (indexfile)
                    keySet.setown(indexfile->getKeyArray(NULL, &layoutTranslators, isOpt, isLocal ? queryFactory.queryChannel() : 0, false));
            }
            if (queryNodeFileName(_graphNode))
            {
                datafile.setown(_queryFactory.queryPackage().lookupFileName(queryNodeFileName(_graphNode), isOpt, true, queryFactory.queryWorkUnit()));
                if (datafile)
                {
                    if (isLocal)
                        files.setown(datafile->getIFileIOArray(isOpt, queryFactory.queryChannel()));
                    else
                        map.setown(datafile->getFileMap());
                }
            }
        }
        catch(IException *E)
        {
            StringBuffer errors;
            E->errorMessage(errors);
            DBGLOG("%s File error = %s", (isLoadDataOnly) ? "LOADDATAONLY" : "SUSPENDED QUERY", errors.str());
            E->Release();
        }
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const { throw MakeStringException(ROXIE_INTERNAL_ERROR, "%s query %s is suspended and cannot be executed - error occurred at %s(%d)", (queryFactory.isQueryLibrary()) ? "Library" : " ", queryFactory.queryQueryName(), __FILE__, __LINE__); }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
        if (indexfile)
            addXrefFileInfo(reply, indexfile);
    }
};

IRoxieServerActivityFactory *createRoxieServerDummyActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode, bool isLoadDataOnly)
{
    return new CRoxieServerDummyActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _graphNode, isLoadDataOnly);
}

//=====================================================================================================

// Keyed joins... 
//
// Input records are pulled by a puller thread, which checks each LHS record to determine which (if any) channels it 
// may have RHS matches on, and sends the relevant fields to the relevant slaves.
// A separate thread (the caller's thread) is waiting on slave replies, and once it has all replies for a given LHS record or group of records, calls
// the transform and returns rows that are created.
// For a full-keyed join, there is a third thread that is pulling replies from index part and passing them to fetch part (check this is true)
//  
//=====================================================================================================

class CJoinGroup;

interface IJoinProcessor
{
    virtual void processEOG() = 0;
    virtual CJoinGroup *createJoinGroup(const void *row) = 0;
    virtual void noteEndReceived(CJoinGroup *jg, unsigned candidateCount) = 0;
    virtual bool fireException(IException *E) = 0;
    virtual void processCompletedGroups() = 0;
};

//------------------------------------------------------------------------------------------------------
// Class CJoinGroup has a record per LHS row, plus (if preserving grouping) a 'head of group' record
// It gathers all the corresponding RHS rows, keeping track of how may slave transactions are pending in endMarkersPending
// If preserving groups, the 'head of group' record keeps track of how many LHS records in the group are still incomplete.
// CJoinGroup records are allocated out of the Roxie row memory manager by overloading operator new, so that they are included in the 
// per-query limits etc (Note that the pointer array block is not though).
// Because of that, the exact size is significant - especially whether fit just under or just over a chunking threshold...
//
// There are two phases to the life of a JoinGroup - it is created by the puller thread that is also firing off slave requests
// notePending will be called once for every slave request. Puller thread calls noteEndReceived(0) once when done - this corresponds to the
// initial count when created.
// Slave replies and are noted by the consumer thread calling addRightMatch() and noteEndReceived(n).
// Once endMarkersPending reaches 0, JoinGroup is complete. Last thread to call noteEndReceived will process the rows and destroy the group.
// There is no need for a critsec because although multiple threads will access at different times, only the consumer thread will
// access any modifiable member variables while endMarkersPending != 0 (i.e. complete() is false). Once complete returns true there is a single
// remaining reference and the JoinGroup will be processed and destroyed.
//   
//------------------------------------------------------------------------------------------------------

class CJoinGroup : public CInterface, implements IInterface
{
protected:
    const void *left;                   // LHS row
    PointerArrayOf<KeyedJoinHeader> rows;           // matching RHS rows
    atomic_t endMarkersPending; // How many slave responses still waiting for
    CJoinGroup *groupStart;     // Head of group, or NULL if not grouping
    unsigned lastPartNo;
    unsigned pos;
    unsigned candidates;        // Number of RHS keyed candidates - note this may not be the same as rows.ordinality()

public:

#undef new
    void *operator new(size_t size, IRowManager *a, unsigned activityId)
    {
        return a->allocate(size, activityId);
    }
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

    void operator delete(void *ptr, IRowManager *a, unsigned activityId)
    {
        ReleaseRoxieRow(ptr);
    }
    void operator delete(void *ptr)
    { 
        ReleaseRoxieRow(ptr);
    }

public:
    IMPLEMENT_IINTERFACE;

    CJoinGroup(const void *_left, CJoinGroup *_groupStart)
    {
#ifdef TRACE_JOINGROUPS
        DBGLOG("Creating joinGroup %p, groupstart %p", this, _groupStart);
#endif
        candidates = 0;
        lastPartNo = 0;
        pos = 0;
        left = _left;
        groupStart = _groupStart;
        if (_groupStart)
        {
            atomic_inc(&_groupStart->endMarkersPending);
        }
        atomic_set(&endMarkersPending, 1);
    }

    ~CJoinGroup()
    {
#ifdef TRACE_JOINGROUPS
        DBGLOG("Destroying joinGroup %p", this);
#endif
        if (left)
        {
            ReleaseRoxieRow(left);
            ForEachItemIn(idx, rows)
                ReleaseRoxieRow(rows.item(idx));
            rows.kill();
        }
    }

    inline bool isHeadRecord() const
    {
        return left==NULL;
    }

    inline bool complete() const
    {
        return atomic_read(&endMarkersPending) == 0;
    }

#ifdef TRACE_JOINGROUPS
    inline void notePending(unsigned lineNo)
#else
    inline void notePending()
#endif
    {
        assert(!complete());
        atomic_inc(&endMarkersPending);
#ifdef TRACE_JOINGROUPS
        DBGLOG("CJoinGroup::notePending %p from %d, count became %d group count %d", this, lineNo, atomic_read(&endMarkersPending), groupStart ? atomic_read(&groupStart->endMarkersPending) : 0);
#endif
    }

    inline bool inGroup(CJoinGroup *leader) const
    {
        return groupStart==leader;
    }

    inline const KeyedJoinHeader *queryRow(unsigned idx) const
    {
        // Single threaded by now
        assert(complete());
        return rows.item(idx);
    }

#ifdef TRACE_JOINGROUPS
    bool noteEndReceived(unsigned candidateCount, unsigned lineNo)
#else
    bool noteEndReceived(unsigned candidateCount)
#endif
    {
        assert(!complete());
        if (candidateCount)
        {
            candidates += candidateCount;
        }
#ifdef TRACE_JOINGROUPS
        DBGLOG("CJoinGroup::noteEndReceived %p from %d, candidates %d + %d, my count was %d, group count was %d", this, lineNo, candidates, candidateCount, atomic_read(&endMarkersPending), groupStart ? atomic_read(&groupStart->endMarkersPending) : 0);
#endif
        // NOTE - as soon as endMarkersPending and groupStart->endMarkersPending are decremented to zero this object may get released asynchronously by other threads
        // There must therefore be nothing in this method after them that acceses member variables. Think of it as a delete this...
        // In particular, we can't safely reference groupStart after the dec_and_test of endMarkersPending, hence copy local first 
        CJoinGroup *localGroupStart = groupStart;
        if (atomic_dec_and_test(&endMarkersPending))
        {
            if (localGroupStart)
                return atomic_dec_and_test(&localGroupStart->endMarkersPending);
            else
                return true;
        }
        else
            return false;
    }

    inline const void *queryLeft() const
    {
        return left;
    }

    void addRightMatch(KeyedJoinHeader *right)
    {
        assert(!complete());
        unsigned short partNo = right->partNo;
        if (partNo != lastPartNo)
        {
            // MORE  - should we binchop? If we did we would need to be careful to find LAST match
            if (partNo > lastPartNo)
                pos = rows.length();
            while (pos>0)
            {
                if (rows.item(pos-1)->partNo <= partNo)
                    break;
                pos--;
            }
            lastPartNo = partNo;
        }
        rows.add(right, pos);
        pos++;
    }

    inline unsigned rowsSeen() const
    {
        assert(complete());
        return rows.length();
    }

    inline unsigned candidateCount() const
    {
        assert(complete());
        return candidates;
    }
};

#ifdef TRACE_JOINGROUPS
#define notePending() notePending(__LINE__)
#define noteEndReceived(a) noteEndReceived(a, __LINE__)
#endif

class KeyedJoinRemoteAdaptor : public CRemoteResultAdaptor // MORE - not sure it should be derived from this - makes processed all wrong, for example
{
private:
    SafeQueueOf<const void, true> ready;

public:
    IHThorKeyedJoinArg &helper;

    unsigned joinProcessed;
    bool isFullKey;
    bool eof;
    bool isSimple;
    bool allPulled;
    unsigned __int64 totalCycles;
    unsigned activityId;
    RecordPullerThread &puller;
    SafeQueueOf<const void, true> injected; // Used in isSimple mode
    Owned<IEngineRowAllocator> ccdRecordAllocator;
    IJoinProcessor &processor;

    KeyedJoinRemoteAdaptor(const RemoteActivityId &_remoteId, IHThorKeyedJoinArg &_helper, IRoxieServerActivity &_activity, bool _isFullKey, bool _isSimple, 
                           RecordPullerThread &_puller, IJoinProcessor &_processor)
        : helper(_helper), 
          CRemoteResultAdaptor(_remoteId, 0, _helper, _activity, true, true), 
          isFullKey(_isFullKey),
          isSimple(_isSimple),
          puller(_puller),
          processor(_processor),
          activityId(_activity.queryId())
    {
        joinProcessed = 0;
        totalCycles = 0;
        allPulled = false;
        eof = false;
    }

    virtual void onCreate(IRoxieInput *_owner, IRoxieServerErrorHandler *_errorHandler, IRoxieSlaveContext *_ctx, IHThorArg *_colocalArg)
    {
        CRemoteResultAdaptor::onCreate(_owner, _errorHandler, _ctx, _colocalArg);
        ccdRecordAllocator.setown(ctx->queryCodeContext()->getRowAllocator(QUERYINTERFACE(helper.queryJoinFieldsRecordSize(), IOutputMetaData), activityId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        joinProcessed = 0;
        totalCycles = 0;
        allPulled = false;
        assertex(ready.ordinality()==0);
        CRemoteResultAdaptor::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        CRemoteResultAdaptor::reset();
        while (ready.ordinality())
        {
            const void *goer = ready.dequeue();
            if (goer)
                ReleaseRoxieRow(goer);
        }
        while (injected.ordinality())
        {
            const void *goer = injected.dequeue();
            if (goer)
                ReleaseRoxieRow(goer);
        }
    }

    inline void addResult(const void *row)
    {
        ready.enqueue(row);
    }

    virtual unsigned __int64 queryTotalCycles() const
    {
        return totalCycles;
    }

    virtual IOutputMetaData * queryOutputMeta() const
    {
        return owner->queryOutputMeta();
    }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        loop
        {
            if (eof)
                return NULL;
            processSlaveResults();
            if (ready.ordinality())
            {
                const void *result = ready.dequeue();
                if (result)
                    joinProcessed++;
                return result;
            }
            else
                eof = true;
        }
    }

private:
    void processSlaveResults()
    {
        while (!ready.ordinality())
        {
            KeyedJoinHeader *fetchedData;
            if (isSimple)
            {
                while (!allPulled && !injected.ordinality())
                {
                    if (!puller.pullRecords(1))
                    {
                        puller.done();
                        allPulled = true;
                    }
                }
                fetchedData = (KeyedJoinHeader *) injected.dequeue();
            }
            else
                fetchedData = (KeyedJoinHeader *) CRemoteResultAdaptor::nextInGroup();
            if (fetchedData)
            {
                CJoinGroup *thisGroup = fetchedData->thisGroup;
                if (fetchedData->partNo == (unsigned short) -1)
                {
#ifdef TRACE_JOINGROUPS
                    CTXLOG("Got end for group %p", thisGroup);
#endif
                    unsigned candidateCount = (unsigned) fetchedData->fpos;
                    ReleaseRoxieRow(fetchedData);
                    processor.noteEndReceived(thisGroup, candidateCount); // note - this can throw exception. So release fetchdata before calling
                }
                else
                {
#ifdef TRACE_JOINGROUPS
                    CTXLOG("Reading another %d bytes for group %p data", ccdRecordSize, thisGroup);
#endif
                    thisGroup->addRightMatch(fetchedData);
                    if (isFullKey)
                    {
#ifdef TRACE_JOINGROUPS
                        CTXLOG("Calling noteEndReceived for record returned from FETCH of full keyed join");
#endif
                        processor.noteEndReceived(thisGroup, 0); // note - this can throw exception. So release fetchdata before calling
                    }
                }
            }
            else
                break;
        }
    }
};

class CRoxieServerFullKeyedJoinHead: public CRoxieServerActivity, implements IRecordPullerCallback, implements IRoxieServerErrorHandler
{
    IHThorKeyedJoinArg &helper;
    Owned<IKeyManager> tlk;
    Linked<IKeyArray> keySet;
    Linked<TranslatorArray> translators;
    CRemoteResultAdaptor remote;
    RecordPullerThread puller;
    IOutputMetaData *indexReadMeta;
    IJoinProcessor *joinHandler;
    bool variableIndexFileName;
    bool indexReadInputRecordVariable;
    bool isLocal;
    Owned<IEngineRowAllocator> indexReadAllocator;
    Owned<const IResolvedFile> varFileInfo;
    IRoxieInput *indexReadInput;
    IIndexReadActivityInfo *rootIndex;

public:
    CRoxieServerFullKeyedJoinHead(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, IKeyArray * _keySet, TranslatorArray *_translators, IOutputMetaData *_indexReadMeta, IJoinProcessor *_joinHandler, bool _isLocal)
        : CRoxieServerActivity(_factory, _probeManager), 
          helper((IHThorKeyedJoinArg &)basehelper), 
          tlk(createKeyManager(NULL, 0, this)),
          translators(_translators),
          keySet(_keySet),
          remote(_remoteId, 0, helper, *this, true, true),
          indexReadMeta(_indexReadMeta),
          joinHandler(_joinHandler),
          puller(false),
          isLocal(_isLocal)
    {
        variableIndexFileName = allFilesDynamic || ((helper.getJoinFlags() & (JFvarindexfilename|JFdynamicindexfilename)) != 0);
        indexReadInputRecordVariable = indexReadMeta->isVariableSize();
        indexReadInput = NULL;
        rootIndex = NULL;
    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        remote.onCreate(this, this, _ctx, _colocalParent);
        indexReadAllocator.setown(ctx->queryCodeContext()->getRowAllocator(indexReadMeta, activityId));
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (!idx)
            puller.setInput(this, _in);
        else if (idx==1)
            indexReadInput = _in;
        else
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 

    }

    virtual void serializeExtra(MemoryBuffer &out)
    {
        if (helper.getJoinFlags() & JFindexfromactivity)
        {
            assertex(rootIndex);
            const RemoteActivityId& indexId = rootIndex->queryRemoteId();
            indexId.serialize(out);
            // could mess about reserving space for length then patching it again, to avoid copy, but probably not worth it
            MemoryBuffer tmp;
            rootIndex->queryActivity()->serializeCreateStartContext(tmp);
            if (rootIndex->queryActivity()->queryVarFileInfo())
            {
                rootIndex->queryActivity()->queryVarFileInfo()->queryTimeStamp().serialize(tmp);
                tmp.append(rootIndex->queryActivity()->queryVarFileInfo()->queryCheckSum());
            }
            unsigned ctxlen = tmp.length();
            out.append(ctxlen).append(tmp);
        }
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (indexReadInput)
        {
            indexReadInput->start(parentExtractSize, parentExtract, true); // paused=true because we don't want to actually run the index read
            rootIndex = indexReadInput->queryIndexReadActivity();
            if (!rootIndex)
                throw MakeStringException(ROXIE_INTERNAL_ERROR,"Index in keyed join %d could not be resolved", queryId());
        }
        remote.onStart(parentExtractSize, parentExtract);
        remote.setLimits(helper.getRowLimit(), (unsigned __int64) -1, I64C(0x7FFFFFFFFFFFFFFF));
        if (rootIndex)
        {
            varFileInfo.setown(rootIndex->getVarFileInfo());
            translators.setown(rootIndex->getTranslators());
            keySet.setown(rootIndex->getKeySet());
        }
        else if (variableIndexFileName)
        {
            OwnedRoxieString indexFileName(helper.getIndexFileName());
            varFileInfo.setown(resolveLFN(indexFileName, (helper.getJoinFlags() & JFindexoptional) != 0));
            if (varFileInfo)
            {
                translators.setown(new TranslatorArray);
                keySet.setown(varFileInfo->getKeyArray(factory->queryActivityMeta(), translators, false, isLocal ? factory->queryQueryFactory().queryChannel() : 0, factory->queryQueryFactory().getEnableFieldTranslation())); // MORE - isLocal?
            }
        }
        puller.start(parentExtractSize, parentExtract, paused, ctx->fullKeyedJoinPreload(), false, ctx);
    }

    virtual void stop(bool aborting)
    {
        puller.stop(aborting);
        CRoxieServerActivity::stop(aborting);
    }

    virtual void reset()
    {
        CRoxieServerActivity::reset();
        puller.reset();
        if (varFileInfo)
        {
            keySet.clear();
            varFileInfo.clear();
        }
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL : &remote;
    }

    virtual void processRow(const void *row)
    {
        // MORE - this code seems to be pretty much duplicated below in half-keyed....

        // called from front puller thread
        // buffer up an IndexRead request
        if (helper.leftCanMatch(row))
        {
            RtlDynamicRowBuilder extractedBuilder(indexReadAllocator);
            unsigned indexReadSize = helper.extractIndexReadFields(extractedBuilder, row);
            OwnedConstRoxieRow extracted;
            if (indexReadSize)
                extracted.setown(extractedBuilder.finalizeRowClear(indexReadSize));

            CJoinGroup *jg = joinHandler->createJoinGroup(row);
            for (unsigned partNo = 0; partNo < keySet->length(); partNo++)
            {
                IKeyIndexBase *thisBase = keySet->queryKeyPart(partNo);
                if (thisBase)
                {
                    unsigned fileNo = 0;
                    IKeyIndex *thisKey = thisBase->queryPart(fileNo);
                    try
                    {
                        tlk->setKey(thisKey);
                        tlk->setLayoutTranslator(translators->item(fileNo));
                        helper.createSegmentMonitors(tlk, extracted);
                        if (rootIndex)
                            rootIndex->mergeSegmentMonitors(tlk);
                        tlk->finishSegmentMonitors();
                        tlk->reset();
                        loop
                        {
                            typedef const void * cvp;
                            if (thisKey->isTopLevelKey())
                            {
                                bool locallySorted = !thisKey->isFullySorted();
                                while (locallySorted || tlk->lookup(false)) 
                                {
                                    unsigned slavePart = locallySorted ? 0 : (unsigned) tlk->queryFpos();
                                    if (locallySorted || slavePart)
                                    {
                                        cvp *outputBuffer = (cvp *) remote.getMem(slavePart, fileNo, indexReadSize + sizeof(cvp) + (indexReadInputRecordVariable ? sizeof(unsigned) : 0));
                                        *outputBuffer++ = jg;
                                        if (indexReadInputRecordVariable)
                                        {
                                            *(unsigned *) outputBuffer = indexReadSize;
                                            outputBuffer = (cvp*) (((unsigned *) outputBuffer) + 1);
                                        }
                                        jg->notePending();
                                        memcpy(outputBuffer, extracted, indexReadSize);
                                        if (locallySorted)
                                        {
                                            for (unsigned i = 1; i < numChannels; i++)
                                                jg->notePending();
                                            break;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                // MORE - this code seems to be duplicated in half keyed
                                unsigned accepted = 0;
                                unsigned rejected = 0;
                                Owned<CRowArrayMessageResult> result = new CRowArrayMessageResult(ctx->queryRowManager(), true);
                                jg->notePending();
                                unsigned candidateCount = 0;
                                while (tlk->lookup(true))
                                {
                                    candidateCount++;
                                    atomic_inc(&indexRecordsRead);
                                    KLBlobProviderAdapter adapter(tlk);
                                    offset_t recptr;
                                    const byte *indexRow = tlk->queryKeyBuffer(recptr);
                                    if (helper.indexReadMatch(extracted, indexRow, recptr, &adapter))
                                    {
                                        KeyedJoinHeader *rhs = (KeyedJoinHeader *) ctx->queryRowManager().allocate(KEYEDJOIN_RECORD_SIZE(0), activityId);
                                        rhs->fpos = recptr; 
                                        rhs->thisGroup = jg; 
                                        rhs->partNo = partNo; 
                                        result->append(rhs);
                                    }
                                    else
                                    {
                                        rejected++;
                                        atomic_inc(&postFiltered);
                                    }
                                }
                                // output an end marker for the matches to this group
                                KeyedJoinHeader *endMarker = (KeyedJoinHeader *) ctx->queryRowManager().allocate(KEYEDJOIN_RECORD_SIZE(0), activityId);
                                endMarker->fpos = (offset_t) candidateCount;
                                endMarker->thisGroup = jg;
                                endMarker->partNo = (unsigned short) -1;
                                result->append(endMarker);
                                remote.injectResult(result.getClear());
                                if (accepted)
                                    noteStatistic(STATS_ACCEPTED, accepted, 1);
                                if (rejected)
                                    noteStatistic(STATS_REJECTED, rejected, 1);
                            }

                            if (++fileNo < thisBase->numParts())
                            {
                                thisKey = thisBase->queryPart(fileNo);
                                tlk->setKey(thisKey);
                                tlk->setLayoutTranslator(translators->item(fileNo));
                                tlk->reset();
                            }
                            else
                                break;
                        }
                        tlk->releaseSegmentMonitors();
                        tlk->setKey(NULL);
                    }
                    catch (...)
                    {
                        tlk->releaseSegmentMonitors();
                        tlk->setKey(NULL);
                        throw;
                    }
                }
            }
            joinHandler->noteEndReceived(jg, 0);
        }
        else
        {
            joinHandler->noteEndReceived(joinHandler->createJoinGroup(row), 0);
        }
    }

    void processGroup(const ConstPointerArray &)
    {
        throwUnexpected();
    }

    virtual void processEOG()
    {
        joinHandler->processEOG();
    }

    virtual void processDone()
    {
        // called from puller thread
        remote.flush();
        remote.senddone();
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
            throwUnexpected();
        helper.onLimitExceeded();
    }
    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

    virtual bool fireException(IException *e)
    {
        // called from puller thread on failure
        remote.fireException(LINK(e));
        return joinHandler->fireException(e);
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

class CRoxieServerKeyedJoinBase : public CRoxieServerActivity, implements IRecordPullerCallback, implements IRoxieServerErrorHandler, implements IJoinProcessor
{
protected:
    IHThorKeyedJoinArg &helper;
    KeyedJoinRemoteAdaptor remote;
    RecordPullerThread puller;
    OwnedConstRoxieRow defaultRight;
    Owned<IEngineRowAllocator> defaultRightAllocator;
    unsigned joinFlags;
    unsigned atMost;
    unsigned abortLimit;
    unsigned keepLimit;
    bool limitFail;
    bool limitOnFail;
    bool preserveGroups;
    bool cloneLeft;
    bool isSimple;
    bool isLocal;
    ThorActivityKind activityKind;
    CJoinGroup *groupStart;
    CriticalSection groupsCrit;
    QueueOf<CJoinGroup, false> groups;
    IRoxieInput *indexReadInput;
    IIndexReadActivityInfo *rootIndex;

    void createDefaultRight()
    {
        if (!defaultRight)
        {
            if (!defaultRightAllocator)
                defaultRightAllocator.setown(ctx->queryCodeContext()->getRowAllocator(helper.queryJoinFieldsRecordSize(), activityId));

            RtlDynamicRowBuilder rowBuilder(defaultRightAllocator);
            size32_t thisSize = helper.createDefaultRight(rowBuilder);
            defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
        }
    }

public:
    CRoxieServerKeyedJoinBase(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, unsigned _joinFlags
        , bool isFull, bool _isSimple, bool _isLocal)
        : CRoxieServerActivity(_factory, _probeManager), 
          helper((IHThorKeyedJoinArg &)basehelper), 
          remote(_remoteId, helper, *this, isFull, _isSimple, puller, *this), 
          joinFlags(_joinFlags), 
          preserveGroups(meta.isGrouped()),
          puller(false),
          isSimple(_isSimple),
          isLocal(_isLocal),
          abortLimit(0),
          keepLimit(0),
          atMost(0),
          limitFail(false),
          limitOnFail(false),
          cloneLeft(false)
    {
        groupStart = NULL;
        activityKind = _factory->getKind();
        indexReadInput = NULL;
        rootIndex = NULL;
        // MORE - code would be easier to read if I got more values from helper rather than passing from factory
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerActivity::onCreate(_ctx, _colocalParent);
        remote.onCreate(this, this, _ctx, _colocalParent);
    }

    virtual void setInput(unsigned idx, IRoxieInput *_in)
    {
        if (!idx)
            puller.setInput(this, _in);
        else if (idx==1)
            indexReadInput = _in;
        else
            throw MakeStringException(ROXIE_SET_INPUT, "Internal error: setInput() parameter out of bounds at %s(%d)", __FILE__, __LINE__); 

    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        if (indexReadInput)
        {
            indexReadInput->start(parentExtractSize, parentExtract, true); // paused=true because we don't want to actually run the index read
            rootIndex = indexReadInput->queryIndexReadActivity();
            if (!rootIndex)
                throw MakeStringException(ROXIE_INTERNAL_ERROR,"Index in keyed join %d could not be resolved", queryId());
        }
        remote.onStart(parentExtractSize, parentExtract);
        remote.setLimits(helper.getRowLimit(), (unsigned __int64) -1, I64C(0x7FFFFFFFFFFFFFFF));
        atMost = helper.getJoinLimit();
        if (atMost == 0) atMost = (unsigned)-1;
        abortLimit = helper.getMatchAbortLimit();
        if (abortLimit == 0 || atMost != (unsigned) -1) abortLimit = (unsigned)-1;
        keepLimit = helper.getKeepLimit();
        if (keepLimit == 0) keepLimit = (unsigned)-1;
        getLimitType(joinFlags, limitFail, limitOnFail);
        cloneLeft = (joinFlags & JFtransformmatchesleft) != 0;
        if (joinFlags & JFleftouter)
            createDefaultRight();
    }

    virtual void stop(bool aborting)
    {
        puller.stop(aborting);
        if (indexReadInput)
            indexReadInput->stop(aborting);
        CRoxieServerActivity::stop(aborting); 
    }

    virtual unsigned __int64 queryLocalCycles() const
    {
        __int64 localCycles = remote.totalCycles;
        localCycles -= puller.queryTotalCycles(); // MORE - debatable... but probably fair.
        if (localCycles < 0)
            localCycles = 0;
        return localCycles;
    }

    virtual IRoxieInput *queryInput(unsigned idx)
    {
        if (idx==0)
            return puller.queryInput();
        else if (idx==1)
            return indexReadInput;
        else
            return NULL;
    }

    virtual void reset()
    {
        totalCycles = remote.totalCycles;
        remote.totalCycles = 0;
        processed = remote.joinProcessed;
        remote.joinProcessed = 0;
        defaultRight.clear();
        if (indexReadInput)
            indexReadInput->reset();
        CRoxieServerActivity::reset(); 
        puller.reset();
        while (groups.ordinality())
        {
            ::Release(groups.dequeue());
        }
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        if (idx==(unsigned)-1)
            idx = 0;
        return idx ? NULL : &remote;
    }

#undef new
    virtual CJoinGroup *createJoinGroup(const void *row)
    {
        // NOTE - we need to protect access to queue, since it's also modified by consumer thread. Groupstart is only modified by puller thread.
        CriticalBlock c(groupsCrit);
        if (preserveGroups && !groupStart)
        {
            groupStart = new (&ctx->queryRowManager(), activityId) CJoinGroup(NULL,  NULL);
            groups.enqueue(groupStart);
        }
        CJoinGroup *jg = new (&ctx->queryRowManager(), activityId) CJoinGroup(row, groupStart);
        groups.enqueue(jg);
        return jg;
    }

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

    void endGroup()
    {
        CriticalBlock c(groupsCrit);
        if (groupStart)
            noteEndReceived(groupStart, 0);
        groupStart = NULL;
    }

    virtual void noteEndReceived(CJoinGroup *jg, unsigned candidateCount)
    {
        if (jg->noteEndReceived(candidateCount))
            processCompletedGroups();
    }

    void processCompletedGroups()
    {
        loop
        {
            CriticalBlock c(groupsCrit); 
            if (!groups.head()->complete())
                break;
            Owned<CJoinGroup> head = groups.dequeue();
            if (preserveGroups)
            {
                assert(head->isHeadRecord());
                assert(groups.head()->inGroup(head));
                unsigned joinGroupSize = 0;
                while (groups.ordinality() && groups.head()->inGroup(head))
                {
                    Owned<CJoinGroup> finger = groups.dequeue();
                    joinGroupSize += doJoinGroup(finger);
                }
                if (joinGroupSize)
                    remote.addResult(NULL);
            }
            else
                doJoinGroup(head);
            if (!groups.ordinality())
                break;
        }
    }

    void failLimit(const void * left)
    {
        helper.onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (input && input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
        {
            input->queryOutputMeta()->toXML((byte *) left, xmlwrite);
        }
        throw MakeStringException(ROXIE_JOIN_ERROR, "More than %d match candidates in keyed join %d for row %s", abortLimit, queryId(), xmlwrite.str());
    }

    virtual bool needsAllocator() const { return true; }

    unsigned doTransform(const void *left, const void *right, offset_t fpos_or_count, IException *except, const void **group)
    {
        if (cloneLeft && !except)
        {
            LinkRoxieRow(left);
            remote.addResult((void *) left);
            return 1;
        }
        
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        unsigned outSize;
        try
        {   
            outSize = except ? helper.onFailTransform(rowBuilder, left, right, fpos_or_count, except) : 
                      (activityKind == TAKkeyeddenormalizegroup) ? helper.transform(rowBuilder, left, right, (unsigned) fpos_or_count, group) : 
                      helper.transform(rowBuilder, left, right, fpos_or_count);
        }
        catch (IException *E)
        {
            throw makeWrappedException(E);
        }
        if (outSize)
        {
            const void *shrunk = rowBuilder.finalizeRowClear(outSize);
            remote.addResult(shrunk);
            return 1;
        }
        else
            return 0;
    }

    unsigned doJoinGroup(CJoinGroup *jg)
    {
        unsigned matched = jg->rowsSeen();
        unsigned added = 0;
        const void *left = jg->queryLeft();

        if (jg->candidateCount() > abortLimit)
        {
            if (limitFail)
                failLimit(left);
            if (ctx->queryDebugContext())
                ctx->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            if (limitOnFail)
            {
                Owned<IException> except;
                try
                {
                    failLimit(left);
                }
                catch(IException * e)
                {
                    except.setown(e);
                }
                added = doTransform(left, defaultRight, 0, except, NULL);
            }
        }
        else if (!matched || jg->candidateCount() > atMost)
        {
            switch (joinFlags & JFtypemask)
            {
            case JFleftouter:
            case JFleftonly:
                switch (activityKind)
                {
                case TAKkeyedjoin:
                case TAKkeyeddenormalizegroup:
                    added = doTransform(left, defaultRight, 0, NULL, NULL);
                    break;
                case TAKkeyeddenormalize:
                    LinkRoxieRow(left);
                    remote.addResult((void *) left);
                    added++;
                    break;
                }
                break;
            }
        }
        else if (!(joinFlags & JFexclude))
        {
            unsigned idx = 0;
            switch (activityKind)
            {
            case TAKkeyedjoin:
                while (idx < matched)
                {
                    const KeyedJoinHeader *rhs = jg->queryRow(idx);
                    added += doTransform(left, &rhs->rhsdata, rhs->fpos, NULL, NULL);
                    if (added==keepLimit)
                        break;
                    idx++;
                }
                break;
            case TAKkeyeddenormalize:
                {
                    OwnedConstRoxieRow newLeft;
                    newLeft.set(left);
                    unsigned rowSize = 0;
                    unsigned rightAdded = 0;
                    while (idx < matched)
                    {
                        const KeyedJoinHeader *rhs = jg->queryRow(idx);
                        try
                        {
                            RtlDynamicRowBuilder rowBuilder(rowAllocator);
                            size32_t transformedSize = helper.transform(rowBuilder, newLeft, &rhs->rhsdata, rhs->fpos, idx+1);
                            if (transformedSize)
                            {
                                rowSize = transformedSize;
                                newLeft.setown(rowBuilder.finalizeRowClear(rowSize));
                                rightAdded++;
                                if (rightAdded==keepLimit)
                                    break;
                            }
                            idx++;
                        }
                        catch (IException *E)
                        {
                            throw makeWrappedException(E);
                        }
                    }
                    if (rowSize)
                    {
                        remote.addResult(newLeft.getClear());
                        added++;
                    }
                }
                break;
            case TAKkeyeddenormalizegroup:
                {
                    ConstPointerArray extractedRows;
                    while (idx < matched && idx < keepLimit)
                    {
                        const KeyedJoinHeader *rhs = jg->queryRow(idx);
                        extractedRows.append((void *) &rhs->rhsdata);
                        idx++;
                    }
                    added += doTransform(left, extractedRows.item(0), extractedRows.ordinality(), NULL, (const void * *)extractedRows.getArray());
                }
                break;
            }
        }
        return added;
    }

    virtual void processDone()
    {
        // called from puller thread
        remote.flush();
        remote.senddone();
    }

    virtual void onLimitExceeded(bool isKeyed)
    {
        if (traceLevel > 4)
            DBGLOG("activityid = %d  isKeyed = %d  line = %d", activityId, isKeyed, __LINE__);
        if (isKeyed)
            throwUnexpected();
        helper.onLimitExceeded();
    }
    virtual const void *createLimitFailRow(bool isKeyed)
    {
        throwUnexpected();
    }

    virtual bool fireException(IException *e)
    {
        // called from puller thread on failure
        return remote.fireException(e);
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

#ifdef _MSC_VER
#pragma warning ( push )
#pragma warning ( disable: 4355 )
#endif

class CRoxieServerKeyedJoinActivity : public CRoxieServerKeyedJoinBase 
{
    CRoxieServerFullKeyedJoinHead head;
    Owned<IEngineRowAllocator> fetchInputAllocator;
    Linked<IFilePartMap> map;
    bool variableFetchFileName;
    Owned<const IResolvedFile> varFetchFileInfo;
    CachedOutputMetaData fetchInputFields;

public:

    CRoxieServerKeyedJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_headId, IKeyArray * _key, TranslatorArray *_translators, IOutputMetaData *_indexReadMeta,
        const RemoteActivityId &_tailId, IFilePartMap *_map, unsigned _joinFlags, bool _isLocal)
        : CRoxieServerKeyedJoinBase(_factory, _probeManager, _tailId, _joinFlags, true, false, _isLocal),
          head(_factory, _probeManager, _headId, _key, _translators, _indexReadMeta, this, _isLocal),
          map(_map)
    {
        CRoxieServerKeyedJoinBase::setInput(0, head.queryOutput(0));
        variableFetchFileName = allFilesDynamic || ((helper.getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
    }
    
    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFetchFileInfo;
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerKeyedJoinBase::onCreate(_ctx, _colocalParent);
        head.onCreate(_ctx, _colocalParent);
        fetchInputFields.set(helper.queryFetchInputRecordSize());
        fetchInputAllocator.setown(ctx->queryCodeContext()->getRowAllocator(helper.queryFetchInputRecordSize(), activityId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerKeyedJoinBase::start(parentExtractSize, parentExtract, paused);
        if (variableFetchFileName)
        {
            bool isFetchOpt = (helper.getFetchFlags() & FFdatafileoptional) != 0;
            OwnedRoxieString fname(helper.getFileName());
            varFetchFileInfo.setown(resolveLFN(fname, isFetchOpt));
            if (varFetchFileInfo)
                map.setown(varFetchFileInfo->getFileMap());
        }
        puller.start(parentExtractSize, parentExtract, paused, ctx->keyedJoinPreload(), false, ctx);
    }

    virtual void setInput(unsigned idx, IRoxieInput *in)
    {
        head.setInput(idx, in);
    }

    virtual void processRow(const void *_rhs)
    {
        // called from puller thread
        KeyedJoinHeader *rhs = (KeyedJoinHeader *) _rhs;
        CJoinGroup *jg = rhs->thisGroup;
        if (rhs->partNo != (unsigned short) -1)
        {
            unsigned partNo = map->mapOffset(rhs->fpos);
            RtlDynamicRowBuilder fetchBuilder(fetchInputAllocator, true);
            size32_t fisize = helper.extractFetchFields(fetchBuilder, jg->queryLeft());
            if (fetchInputFields.isVariableSize())
            {
                KeyedJoinHeader *outRow = (KeyedJoinHeader *) remote.getMem(partNo, 0, KEYEDJOIN_RECORD_SIZE(fisize + sizeof(fisize)));
                memcpy(outRow, rhs, KEYEDJOIN_RECORD_SIZE(0)); // MORE - copy constructor might be more appropriate....
                ReleaseRoxieRow(rhs);
                jg->notePending();
                memcpy(&outRow->rhsdata, &fisize, sizeof(fisize));
                memcpy((&outRow->rhsdata)+sizeof(fisize), fetchBuilder.row(), fisize);
            }
            else
            {
                KeyedJoinHeader *outRow = (KeyedJoinHeader *) remote.getMem(partNo, 0, KEYEDJOIN_RECORD_SIZE(fisize));
                memcpy(outRow, rhs, KEYEDJOIN_RECORD_SIZE(0)); // MORE - copy constructor might be more appropriate....
                ReleaseRoxieRow(rhs);
                jg->notePending();
                memcpy(&outRow->rhsdata, fetchBuilder.row(), fisize);
            }
        }
        else
        {
            unsigned candidateCount = (unsigned) rhs->fpos;
//          CTXLOG("Full keyed join - all results back from index");
            ReleaseRoxieRow(rhs);
            noteEndReceived(jg, candidateCount); // may throw exception - so release row before calling
        }
    }

    virtual void processEOG()
    {
        // called from front puller thread
        if (preserveGroups)
            endGroup();
    }

    void processGroup(const ConstPointerArray &)
    {
        throwUnexpected();
    }

};

#ifdef _MSC_VER
#pragma warning ( pop )
#endif

class CRoxieServerHalfKeyedJoinActivity : public CRoxieServerKeyedJoinBase 
{
    IOutputMetaData *indexReadMeta;
    Owned<IEngineRowAllocator> indexReadAllocator;
    Owned<IKeyManager> tlk;
    bool variableIndexFileName;
    bool indexReadInputRecordVariable;
    Owned<const IResolvedFile> varFileInfo;
    Linked<TranslatorArray> translators;
    Linked<IKeyArray> keySet;
    Owned<IOutputMetaData> joinPrefixedMeta;
    Owned<IEngineRowAllocator> joinFieldsAllocator;

public:
    CRoxieServerHalfKeyedJoinActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager, const RemoteActivityId &_remoteId, IKeyArray * _keySet, TranslatorArray *_translators, 
        IOutputMetaData *_indexReadMeta, unsigned _joinFlags, bool _isSimple, bool _isLocal)
        : CRoxieServerKeyedJoinBase(_factory, _probeManager, _remoteId, _joinFlags, false, _isSimple, _isLocal),
          indexReadMeta(_indexReadMeta),
          tlk(createKeyManager(NULL, 0, this)),
          keySet(_keySet),
          translators(_translators)
    {
        variableIndexFileName = allFilesDynamic || ((helper.getJoinFlags() & (JFvarindexfilename|JFdynamicindexfilename)) != 0);
        indexReadInputRecordVariable = indexReadMeta->isVariableSize();
    }

    virtual void serializeExtra(MemoryBuffer &out)
    {
        if (helper.getJoinFlags() & JFindexfromactivity)
        {
            assertex(rootIndex);
            const RemoteActivityId& indexId = rootIndex->queryRemoteId();
            indexId.serialize(out);
            // could mess about reserving space for length then patching it again, to avoid copy, but probably not worth it
            MemoryBuffer tmp;
            rootIndex->queryActivity()->serializeCreateStartContext(tmp);
            if (rootIndex->queryActivity()->queryVarFileInfo())
            {
                rootIndex->queryActivity()->queryVarFileInfo()->queryTimeStamp().serialize(tmp);
                tmp.append(rootIndex->queryActivity()->queryVarFileInfo()->queryCheckSum());
            }
            unsigned ctxlen = tmp.length();
            out.append(ctxlen).append(tmp);
        }
    }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent)
    {
        CRoxieServerKeyedJoinBase::onCreate(_ctx, _colocalParent);
        indexReadAllocator.setown(ctx->queryCodeContext()->getRowAllocator(indexReadMeta, activityId));

        IOutputMetaData *joinFieldsMeta = helper.queryJoinFieldsRecordSize();
        joinPrefixedMeta.setown(new CPrefixedOutputMeta(KEYEDJOIN_RECORD_SIZE(0), joinFieldsMeta)); // MORE - not sure if we really need this
        joinFieldsAllocator.setown(ctx->queryCodeContext()->getRowAllocator(joinPrefixedMeta, activityId));
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        CRoxieServerKeyedJoinBase::start(parentExtractSize, parentExtract, paused);
        if (rootIndex)
        {
            varFileInfo.setown(rootIndex->getVarFileInfo());
            translators.setown(rootIndex->getTranslators());
            keySet.setown(rootIndex->getKeySet());
        }
        else if (variableIndexFileName)
        {
            OwnedRoxieString indexFileName(helper.getIndexFileName());
            varFileInfo.setown(resolveLFN(indexFileName, (helper.getJoinFlags() & JFindexoptional) != 0));
            if (varFileInfo)
            {
                translators.setown(new TranslatorArray);
                keySet.setown(varFileInfo->getKeyArray(factory->queryActivityMeta(), translators, false, isLocal ? factory->queryQueryFactory().queryChannel() : 0, factory->queryQueryFactory().getEnableFieldTranslation()));
            }
        }
        puller.start(parentExtractSize, parentExtract, paused, ctx->keyedJoinPreload(), isSimple, ctx);

    }

    virtual const IResolvedFile *queryVarFileInfo() const
    {
        return varFileInfo;
    }

    virtual void reset()
    {
        CRoxieServerKeyedJoinBase::reset();
        if (varFileInfo)
        {
            keySet.clear();
            varFileInfo.clear();
        }
    }

    virtual void processRow(const void *row)
    {
        // called from front puller thread
        // buffer up an IndexRead request
        if (helper.leftCanMatch(row) && keySet)
        {
            RtlDynamicRowBuilder extractBuilder(indexReadAllocator);
            unsigned indexReadRecordSize = helper.extractIndexReadFields(extractBuilder, row);
            OwnedConstRoxieRow extracted;
            if (indexReadRecordSize)
                extracted.setown(extractBuilder.finalizeRowClear(indexReadRecordSize));

            CJoinGroup *jg = createJoinGroup(row);
            for (unsigned partNo = 0; partNo < keySet->length(); partNo++)
            {
                IKeyIndexBase *thisBase = keySet->queryKeyPart(partNo);
                if (thisBase)
                {
                    unsigned fileNo = 0;
                    IKeyIndex *thisKey = thisBase->queryPart(fileNo);
                    tlk->setKey(thisKey);
                    tlk->setLayoutTranslator(translators->item(fileNo));
                    helper.createSegmentMonitors(tlk, extracted);
                    if (rootIndex)
                        rootIndex->mergeSegmentMonitors(tlk);
                    tlk->finishSegmentMonitors();
                    try
                    {
                        tlk->reset();
                        loop
                        {
                            typedef const void * cvp;
                            if (thisKey && thisKey->isTopLevelKey())
                            {
                                bool locallySorted = (!thisKey->isFullySorted());
                                while (locallySorted || tlk->lookup(false))
                                {
                                    unsigned slavePart = locallySorted ? 0 : (unsigned) tlk->queryFpos();
                                    if (locallySorted || slavePart)
                                    {
                                        cvp *outputBuffer = (cvp *) remote.getMem(slavePart, fileNo, indexReadRecordSize + sizeof(cvp) + (indexReadInputRecordVariable ? sizeof(unsigned) : 0));
                                        *outputBuffer++ = jg;
                                        if (indexReadInputRecordVariable)
                                        {
                                            *(unsigned *) outputBuffer = indexReadRecordSize;
                                            outputBuffer = (cvp *) (((unsigned *) outputBuffer) + 1);
                                        }
                                        jg->notePending();
                                        memcpy(outputBuffer, extracted, indexReadRecordSize);
                                        if (locallySorted)
                                        {
                                            for (unsigned i = 1; i < numChannels; i++)
                                                jg->notePending();
                                            break;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                unsigned accepted = 0;
                                unsigned rejected = 0;
                                Owned<CRowArrayMessageResult> result;
                                if (!isSimple) 
                                    result.setown(new CRowArrayMessageResult(ctx->queryRowManager(), true));
                                // MORE - This code seems to be duplicated in keyedJoinHead
                                jg->notePending();
                                unsigned candidateCount = 0;
                                while (tlk->lookup(true))
                                {
                                    candidateCount++;
                                    atomic_inc(&indexRecordsRead);
                                    KLBlobProviderAdapter adapter(tlk);
                                    offset_t recptr;
                                    const byte *indexRow = tlk->queryKeyBuffer(recptr);
                                    if (helper.indexReadMatch(extracted, indexRow, recptr, &adapter))
                                    {
                                        RtlDynamicRowBuilder rb(joinFieldsAllocator, true); 
                                        CPrefixedRowBuilder pb(KEYEDJOIN_RECORD_SIZE(0), rb);
                                        accepted++;
                                        KLBlobProviderAdapter adapter(tlk);
                                        size32_t joinFieldsSize = helper.extractJoinFields(pb, indexRow, recptr, &adapter);
                                        KeyedJoinHeader *rec = (KeyedJoinHeader *) rb.getUnfinalizedClear(); // lack of finalize ok as unserialized data here.
                                        rec->fpos = recptr;
                                        rec->thisGroup = jg;
                                        rec->partNo = partNo;
                                        if (isSimple)
                                            remote.injected.enqueue(rec);
                                        else
                                            result->append(rec);
                                    }
                                    else
                                    {
                                        rejected++;
                                        atomic_inc(&postFiltered);
                                    }
                                }
                                // output an end marker for the matches to this group
                                KeyedJoinHeader *rec = (KeyedJoinHeader *) ctx->queryRowManager().allocate(KEYEDJOIN_RECORD_SIZE(0), activityId);
                                rec->fpos = (offset_t) candidateCount;
                                rec->thisGroup = jg;
                                rec->partNo = (unsigned short) -1;
                                if (isSimple)
                                    remote.injected.enqueue(rec);
                                else
                                {
                                    result->append(rec);
                                    remote.injectResult(result.getClear());
                                }
                                if (accepted)
                                    noteStatistic(STATS_ACCEPTED, accepted, 1);
                                if (rejected)
                                    noteStatistic(STATS_REJECTED, rejected, 1);
                            }
                            if (++fileNo < thisBase->numParts())
                            {
                                thisKey = thisBase->queryPart(fileNo);
                                tlk->setKey(thisKey);
                                tlk->setLayoutTranslator(translators->item(fileNo));
                                tlk->reset();
                            }
                            else
                                break;
                        }
                        tlk->releaseSegmentMonitors();
                        tlk->setKey(NULL);
                    }
                    catch (...)
                    {
                        tlk->releaseSegmentMonitors();
                        tlk->setKey(NULL);
                        throw;
                    }
                }
            }
            noteEndReceived(jg, 0);
        }
        else
        {
            noteEndReceived(createJoinGroup(row), 0);
        }
    }

    virtual void processEOG()
    {
        // called from front puller thread
        if (preserveGroups)
            endGroup();
    }

    void processGroup(const ConstPointerArray &)
    {
        throwUnexpected();
    }
};

class CRoxieServerKeyedJoinActivityFactory : public CRoxieServerMultiInputFactory
{
    Owned<const IResolvedFile> indexfile;
    Owned<const IResolvedFile> datafile;
    Owned<IKeyArray> keySet;
    Owned<TranslatorArray> translatorArray;
    Owned<IDefRecordMeta> activityMeta;
    RemoteActivityId headId;
    RemoteActivityId tailId;
    IOutputMetaData *indexReadMeta;
    Owned<IFilePartMap> map;
    Owned<IFileIOArray> files;
    unsigned joinFlags;
    bool isHalfKeyed;
    bool isLocal;
    bool enableFieldTranslation;
    bool variableFetchFileName;
    bool variableIndexFileName;
    bool isSimple;

public:
    CRoxieServerKeyedJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_headId, const RemoteActivityId &_tailId, IPropertyTree &_graphNode)
        : CRoxieServerMultiInputFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), headId(_headId), tailId(_tailId)
    {
        Owned<IHThorKeyedJoinArg> helper = (IHThorKeyedJoinArg *) helperFactory();
        isLocal = _graphNode.getPropBool("att[@name='local']/@value") && queryFactory.queryChannel()!=0;
        isSimple = isLocal;
        rtlDataAttr indexLayoutMeta;
        size32_t indexLayoutSize;
        if(!helper->getIndexLayout(indexLayoutSize, indexLayoutMeta.refdata()))
            assertex(indexLayoutSize== 0);
        MemoryBuffer m;
        m.setBuffer(indexLayoutSize, indexLayoutMeta.getdata());
        activityMeta.setown(deserializeRecordMeta(m, true));
        enableFieldTranslation = queryFactory.getEnableFieldTranslation();
        translatorArray.setown(new TranslatorArray);
        joinFlags = helper->getJoinFlags();
        variableIndexFileName = allFilesDynamic || ((joinFlags & (JFvarindexfilename|JFdynamicindexfilename)) != 0);
        variableFetchFileName = allFilesDynamic || ((helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) != 0);
        if (!variableIndexFileName)
        {
            bool isOpt = (joinFlags & JFindexoptional) != 0;
            OwnedRoxieString indexFileName(helper->getIndexFileName());
            indexfile.setown(queryFactory.queryPackage().lookupFileName(indexFileName, isOpt, true, queryFactory.queryWorkUnit()));
            if (indexfile)
                keySet.setown(indexfile->getKeyArray(activityMeta, translatorArray, isOpt, isLocal ? queryFactory.queryChannel() : 0, enableFieldTranslation));
        }
        if (keySet && keySet->length()==1 && !isSimple)
        {
            IKeyIndexBase *thisBase = keySet->queryKeyPart(0);
            if (thisBase->numParts()==1 && !thisBase->queryPart(0)->isTopLevelKey() && !_queryFactory.getDebugValueBool("disableLocalOptimizations", false))
                isSimple = true;
            // MORE - if it's a variable filename then it MAY be simple, we don't know. Tough.
        }
        if (!simpleLocalKeyedJoins)
            isSimple = false;
        isHalfKeyed = !helper->diskAccessRequired();

        indexReadMeta = QUERYINTERFACE(helper->queryIndexReadInputRecordSize(), IOutputMetaData);
        if (!isHalfKeyed && !variableFetchFileName)
        {
            bool isFetchOpt = (helper->getFetchFlags() & FFdatafileoptional) != 0;
            datafile.setown(_queryFactory.queryPackage().lookupFileName(queryNodeFileName(_graphNode), isFetchOpt, true, _queryFactory.queryWorkUnit()));
            if (datafile)
            {
                if (isLocal)
                    files.setown(datafile->getIFileIOArray(isFetchOpt, queryFactory.queryChannel()));
                else
                    map.setown(datafile->getFileMap());
            }
        }
    }

    virtual bool getEnableFieldTranslation() const
    {
        return enableFieldTranslation;
    }

    virtual IDefRecordMeta *queryActivityMeta() const
    {
        return activityMeta;
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        if (isHalfKeyed)
            return new CRoxieServerHalfKeyedJoinActivity(this, _probeManager, 
                headId, keySet, translatorArray, indexReadMeta, joinFlags, isSimple, isLocal);
        else
            return new CRoxieServerKeyedJoinActivity(this, _probeManager, 
                headId, keySet, translatorArray, indexReadMeta, 
                tailId, map, joinFlags, isLocal);
    }

    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        if (datafile)
            addXrefFileInfo(reply, datafile);
        if (indexfile)
            addXrefFileInfo(reply, indexfile);
    }
};

IRoxieServerActivityFactory *createRoxieServerKeyedJoinActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, const RemoteActivityId &_remoteId, const RemoteActivityId &_remoteId2, IPropertyTree &_graphNode)
{
    return new CRoxieServerKeyedJoinActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _remoteId, _remoteId2, _graphNode);
}

//=================================================================================

class CRoxieServerSoapActivityBase : public CRoxieServerActivity, implements ISoapCallRowProvider, implements IRoxieAbortMonitor
{
protected:
    Owned<ISoapCallHelper> soaphelper;
    IHThorSoapActionArg & helper;
    StringBuffer authToken;
    bool eof;
    CriticalSection crit;
    ClientCertificate *pClientCert;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieServerSoapActivityBase(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerActivity(_factory, _probeManager), helper((IHThorSoapActionArg &)basehelper)
    {
        eof = false;
        if (clientCert.certificate.length() > 0 && clientCert.privateKey.length() > 0 && clientCert.passphrase.length() > 0)
            pClientCert = &clientCert;
        else
            pClientCert = NULL;
    }

    // ISoapCallRowProvider
    virtual IHThorSoapActionArg * queryActionHelper() { return &helper; };
    virtual IHThorSoapCallArg * queryCallHelper() { return NULL; };
    virtual const void * getNextRow() { return NULL; };
    virtual void releaseRow(const void * r) { ReleaseRoxieRow(r); };

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        eof = false;
        CRoxieServerActivity::start(parentExtractSize, parentExtract, paused);
        authToken.append(ctx->queryAuthToken());
    }
    virtual void reset()
    {
        // MORE - Shouldn't we make sure thread is stopped etc???
        soaphelper.clear();
        CRoxieServerActivity::reset();
    }

    // IRoxieAbortMonitor
    virtual void checkForAbort() { checkAbort(); }

};

//---------------------------------------------------------------------------

class CRoxieServerSoapRowCallActivity : public CRoxieServerSoapActivityBase 
{
    IHThorSoapCallArg & callHelper;

public:
    CRoxieServerSoapRowCallActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerSoapActivityBase(_factory, _probeManager), callHelper((IHThorSoapCallArg &)basehelper)
    {
    }

    virtual IHThorSoapCallArg * queryCallHelper()
    {
        return &callHelper;
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if(eof) return NULL;

        if (soaphelper == NULL)
        {
            if (factory->getKind()==TAKhttp_rowdataset)
                soaphelper.setown(createHttpCallHelper(this, rowAllocator, authToken.str(), SCrow, pClientCert, *ctx, this));
            else
                soaphelper.setown(createSoapCallHelper(this, rowAllocator, authToken.str(), SCrow, pClientCert, *ctx, this));
            soaphelper->start();
        }

        OwnedConstRoxieRow ret = soaphelper->getRow();
        if (!ret)
        {
            eof = true;
            return NULL;
        }
        ++processed;
        return ret.getClear();
    }
};

class CRoxieServerSoapRowCallActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSoapRowCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSoapRowCallActivity(this, _probeManager);
    }

};

IRoxieServerActivityFactory *createRoxieServerSoapRowCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSoapRowCallActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//---------------------------------------------------------------------------

class CRoxieServerSoapRowActionActivity : public CRoxieServerSoapActivityBase 
{
public:
    CRoxieServerSoapRowActionActivity (const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerSoapActivityBase(_factory, _probeManager)
    {}

    virtual void execute(unsigned parentExtractSize, const byte * parentExtract)
    {
        //MORE: parentExtract not passed to start - although shouldn't be a problem.
        soaphelper.setown(createSoapCallHelper(this, NULL, ctx->queryAuthToken(), SCrow, pClientCert, *ctx, this));
        soaphelper->start();
        soaphelper->waitUntilDone();
        IException *e = soaphelper->getError();
        soaphelper.clear();
        if (e)
            throw e;
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        return NULL;
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

class CRoxieServerSoapRowActionActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerSoapRowActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSoapRowActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerSoapRowActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerSoapRowActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//---------------------------------------------------------------------------

class CRoxieServerSoapDatasetCallActivity : public CRoxieServerSoapActivityBase 
{
    IHThorSoapCallArg & callHelper;

public:
    CRoxieServerSoapDatasetCallActivity(const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerSoapActivityBase(_factory, _probeManager), callHelper((IHThorSoapCallArg &)basehelper)
    {
    }

    virtual IHThorSoapCallArg * queryCallHelper()
    {
        return &callHelper;
    }

    virtual const void *getNextRow()
    {
        CriticalBlock b(crit);

        const void *nextrec = input->nextInGroup();
        if (!nextrec)
        {
            nextrec = input->nextInGroup();
        }
        return nextrec;
    }

    virtual bool needsAllocator() const { return true; }

    virtual const void *nextInGroup()
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        if(eof) return NULL;

        if (soaphelper == NULL)
        {
            soaphelper.setown(createSoapCallHelper(this, rowAllocator, authToken.str(), SCdataset, pClientCert, *ctx, this));
            soaphelper->start();
        }

        OwnedConstRoxieRow ret = soaphelper->getRow();
        if (!ret)
        {
            eof = true;
            return NULL;
        }
        ++processed;
        return ret.getClear();
    }
};

class CRoxieServerSoapDatasetCallActivityFactory : public CRoxieServerActivityFactory
{
public:
    CRoxieServerSoapDatasetCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSoapDatasetCallActivity(this, _probeManager);
    }
};

IRoxieServerActivityFactory *createRoxieServerSoapDatasetCallActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind)
{
    return new CRoxieServerSoapDatasetCallActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind);
}

//---------------------------------------------------------------------------

class CRoxieServerSoapDatasetActionActivity : public CRoxieServerSoapActivityBase 
{
public:
    CRoxieServerSoapDatasetActionActivity (const IRoxieServerActivityFactory *_factory, IProbeManager *_probeManager)
        : CRoxieServerSoapActivityBase(_factory, _probeManager)
    {}

    virtual const void *getNextRow()
    {
        CriticalBlock b(crit);

        const void *nextrec = input->nextInGroup();
        if (!nextrec)
        {
            nextrec = input->nextInGroup();
        }
        if (nextrec)
            processed++;
        return nextrec;
    }

    virtual void execute(unsigned parentExtractSize, const byte * parentExtract)
    {
        try
        {
            start(parentExtractSize, parentExtract, false);
            soaphelper.setown(createSoapCallHelper(this, NULL, ctx->queryAuthToken(), SCdataset, pClientCert, *ctx, this));
            soaphelper->start();
            soaphelper->waitUntilDone();
            IException *e = soaphelper->getError();
            soaphelper.clear();
            if (e)
                throw e;
            stop(false);
        }
        catch (IException *E)
        {
            ctx->notifyAbort(E);
            stop(true);
            throw;
        }
        catch(...)
        {
            Owned<IException> E = MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception caught at %s:%d", __FILE__, __LINE__);
            ctx->notifyAbort(E);
            stop(true);
            throw;

        }
    }

    virtual IRoxieInput *queryOutput(unsigned idx)
    {
        return NULL;
    }

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};

class CRoxieServerSoapDatasetActionActivityFactory : public CRoxieServerActivityFactory
{
    bool isRoot;
public:
    CRoxieServerSoapDatasetActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
        : CRoxieServerActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind), isRoot(_isRoot)
    {
    }

    virtual IRoxieServerActivity *createActivity(IProbeManager *_probeManager) const
    {
        return new CRoxieServerSoapDatasetActionActivity(this, _probeManager);
    }

    virtual bool isSink() const
    {
        return isRoot;
    }
};

IRoxieServerActivityFactory *createRoxieServerSoapDatasetActionActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, bool _isRoot)
{
    return new CRoxieServerSoapDatasetActionActivityFactory(_id, _subgraphId, _queryFactory, _helperFactory, _kind, _isRoot);
}

//=====================================================================================================

class CGraphResults : public CInterface, implements IRoxieGraphResults
{
    IArrayOf<IGraphResult> results;
    CriticalSection cs;

    IGraphResult & select(unsigned idx)
    {
        CriticalBlock procedure(cs);
        if (idx >= results.ordinality())
            throw MakeStringException(ROXIE_GRAPH_PROCESSING_ERROR, "Error reading graph result %d before it is calculated", idx);

        return results.item(idx);
    }

public:
    IMPLEMENT_IINTERFACE

    void clear()
    {
        CriticalBlock procedure(cs);
        results.kill();
    }

    IRoxieInput * createIterator(unsigned id)
    {
        return select(id).createIterator();
    }
    virtual void getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
    {
        select(id).getLinkedResult(count, ret);
    }
    virtual void getDictionaryResult(unsigned & count, byte * * & ret, unsigned id)
    {
        select(id).getLinkedResult(count, ret);
    }
    void setResult(unsigned id, IGraphResult * result)
    {
        CriticalBlock procedure(cs);

        if (results.ordinality() <= id)
        {
            while (results.ordinality() < id)
                results.append(*new CGraphResult);
            results.append(*LINK(result));
        }
        else
            results.replace(*LINK(result), id);
    }
    void appendResult(IGraphResult * result)
    {
        CriticalBlock procedure(cs);
        results.append(*LINK(result));
    }
};

//===================================================================================================================

class CPseudoArg : public CInterface, implements IHThorArg
{
public:
    IMPLEMENT_IINTERFACE

    virtual IOutputMetaData * queryOutputMeta() { return NULL; }
};

class CPseudoActivity : public CRoxieServerActivity
{
public:
    CPseudoActivity(IHThorArg & _helper) : CRoxieServerActivity(_helper) {}

    virtual const void *nextInGroup()
    {
        throwUnexpected(); // I am nobody's input
    }
};


class CActivityGraph : public CInterface, implements IActivityGraph, implements IThorChildGraph, implements ILocalGraphEx, implements IRoxieServerChildGraph
{
protected:
    IArrayOf<IRoxieServerActivity> activities;
    IArrayOf<IRoxieInput> probes;
    IRoxieServerActivityCopyArray sinks;
    StringAttr graphName;
    Owned<CGraphResults> results;
    CGraphResults graphLoopResults;
    ActivityArray & graphDefinition;
    CriticalSection evaluateCrit;

    IProbeManager *probeManager;
    unsigned id;
    unsigned loopCounter;

    class ActivityGraphSlaveContext : public IndirectSlaveContext
    {
        SpinLock abortLock;
        bool aborted;
        Owned<IException> exception;
    public:
        ActivityGraphSlaveContext(const IRoxieContextLogger &_logctx) : logctx(_logctx), loopCounter(0), codeContext(NULL)
        {
            aborted = false;
        }

        // Note - we must track exceptions at the child level in case there is a CATCH in parent

        virtual void notifyAbort(IException *E)
        {
            SpinBlock b(abortLock);
            if (!aborted && QUERYINTERFACE(E, InterruptedSemaphoreException) == NULL)
            {
                aborted = true;
                exception.set(E);
            }
        }

        virtual void checkAbort()
        {
            if (aborted) // NOTE - don't bother getting lock before reading this (for speed) - a false read is very unlikely and not a problem
            {
                SpinBlock b(abortLock);
                if (!exception)
                    exception.setown(MakeStringException(ROXIE_INTERNAL_ERROR, "Query was aborted"));
                throw exception.getLink();
            }
            IndirectSlaveContext::checkAbort();
        }

        virtual ICodeContext *queryCodeContext()
        {
            return codeContext;
        }
        void setCodeContext(ICodeContext * _codeContext)
        {
            codeContext = _codeContext;
        }
        void setLoopCounter(unsigned _loopCounter)
        {
            loopCounter = _loopCounter;
        }
        virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph)
        {
            childGraphs.setValue(id, childGraph);
        }
        virtual IActivityGraph * queryChildGraph(unsigned  id)
        {
            if (queryTraceLevel() > 10)
                CTXLOG("resolveChildGraph %d", id);
            IActivityGraph *childGraph = childGraphs.getValue(id);
            assertex(childGraph);
            return childGraph;
        }
        // MORE should really redirect the other log context ones too (though mostly doesn't matter). Really should refactor to have a queryLogContext() method in IRoxieSlaveContext I think
        virtual StringBuffer &getLogPrefix(StringBuffer &ret) const
        {
            logctx.getLogPrefix(ret);
            if (loopCounter)
                ret.appendf("{%u}", loopCounter);
            return ret;
        }
    protected:
        const IRoxieContextLogger &logctx;
        unsigned loopCounter;
        ICodeContext * codeContext;
        MapXToMyClass<unsigned, unsigned, IActivityGraph> childGraphs;
    } graphSlaveContext;

    class ActivityGraphCodeContext : public IndirectCodeContext
    {
    public:
        virtual IEclGraphResults * resolveLocalQuery(__int64 activityId)
        {
            if ((unsigned) activityId == container->queryId())
                return container;
            IActivityGraph * match = slaveContext->queryChildGraph((unsigned) activityId);
            if (match)
                return match->queryLocalGraph();
            return IndirectCodeContext::resolveLocalQuery(activityId);
        }
        virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal)
        {
            IActivityGraph * match = slaveContext->queryChildGraph((unsigned) activityId);
            return LINK(match->queryChildGraph());
        }
        virtual unsigned getGraphLoopCounter() const
        {
            return container->queryLoopCounter();           // only called if value is valid
        }
        void setContainer(IRoxieSlaveContext * _slaveContext, CActivityGraph * _container)
        {
            slaveContext = _slaveContext;
            container = _container;
        }

    protected:
        IRoxieSlaveContext * slaveContext;
        CActivityGraph * container;
    } graphCodeContext;


public:
    IMPLEMENT_IINTERFACE;

    CActivityGraph(const char *_graphName, unsigned _id, ActivityArray &x, IProbeManager *_probeManager, const IRoxieContextLogger &_logctx)
        : probeManager(_probeManager), graphDefinition(x), graphName(_graphName), graphSlaveContext(_logctx)
    {
        id = x.getLibraryGraphId();
        if (!id)
            id = _id;
        loopCounter = 0;
        graphSlaveContext.setCodeContext(&graphCodeContext);
        graphCodeContext.setContainer(&graphSlaveContext, this);
    }

    ~CActivityGraph()
    {
        if (probeManager)
            probeManager->deleteGraph((IArrayOf<IActivityBase>*)&activities, (IArrayOf<IInputBase>*)&probes);
    }

    virtual const char *queryName() const
    {
        return graphName.get();
    }

    void createGraph()
    {
        ForEachItemIn(idx, graphDefinition)
        {
            IRoxieServerActivityFactory &donor = graphDefinition.serverItem(idx);
            IRoxieServerActivity &activity = *donor.createActivity(probeManager);
            activities.append(activity);
            if (donor.isSink())
            {
                sinks.append(activity);
                if (probeManager)
                    probeManager->noteSink(&activity);
            }
        }
        ForEachItemIn(idx1, graphDefinition)
        {
            IRoxieServerActivityFactory &donor = graphDefinition.serverItem(idx1);
            IRoxieServerActivity &activity = activities.item(idx1);
            unsigned inputidx = 0;
            loop
            {
                unsigned outputidx;
                unsigned source = donor.getInput(inputidx, outputidx);
                if (source==(unsigned) -1)
                    break;

                connectInput(idx1, inputidx, source, outputidx, 0);
                inputidx++;
            }
            IntArray &dependencies = donor.queryDependencies();
            IntArray &dependencyIndexes = donor.queryDependencyIndexes();
            IntArray &dependencyControlIds = donor.queryDependencyControlIds();
            StringArray &dependencyEdgeIds = donor.queryDependencyEdgeIds();
            ForEachItemIn(idx2, dependencies)
            {
                IRoxieServerActivity &dependencySourceActivity = activities.item(dependencies.item(idx2));
                unsigned dependencySourceIndex = dependencyIndexes.item(idx2);
                unsigned dependencyControlId = dependencyControlIds.item(idx2);
                activity.addDependency(dependencySourceActivity, dependencySourceIndex, dependencyControlId);
                if (probeManager)
                    probeManager->noteDependency( &dependencySourceActivity, dependencySourceIndex, dependencyControlId, dependencyEdgeIds.item(idx2), &activity);
            }
        }
    }

    void connectInput(unsigned target, unsigned targetIdx, unsigned source, unsigned sourceIdx, unsigned iteration)
    {
        IRoxieServerActivity &targetActivity = activities.item(target);
        IRoxieServerActivity &sourceActivity = activities.item(source);
        IRoxieInput * output = sourceActivity.queryOutput(sourceIdx);
        if (probeManager)
        {
            IInputBase * inputBase = probeManager->createProbe(static_cast<IInputBase*>(output), &sourceActivity, &targetActivity, sourceIdx, targetIdx, iteration);
            output = static_cast<IRoxieInput*>(inputBase);
            probes.append(*LINK(output));
        }
        targetActivity.setInput(targetIdx, output);
    }

    virtual void onCreate(IRoxieSlaveContext *ctx, IHThorArg *_colocalParent)
    {
        graphSlaveContext.set(ctx);
        if (graphDefinition.isMultiInstance())
        {
            graphCodeContext.set(ctx->queryCodeContext());
            ctx = &graphSlaveContext;
        }

        ForEachItemIn(idx, activities)
        {
            IRoxieServerActivity *activity = &activities.item(idx);
            if (activity)
                activity->onCreate(ctx, _colocalParent);
        }
    }

    virtual void abort()
    {
        ForEachItemIn(idx, sinks)
        {
            IRoxieServerActivity &sink = sinks.item(idx);
            sink.stop(true);
        }
    }

    virtual void reset()
    {
        ForEachItemIn(idx, sinks)
        {
            IRoxieServerActivity &sink = sinks.item(idx);
            sink.reset();
        }
    }

    Linked<IException> exception;
    CriticalSection eCrit;

    virtual void noteException(IException *E)
    {
        CriticalBlock b(eCrit);
        if (!exception)
        {
            if (graphSlaveContext.queryDebugContext())
            {
                graphSlaveContext.queryDebugContext()->checkBreakpoint(DebugStateException, NULL, exception);
            }
            exception.set(E);
        }
    }

    virtual void checkAbort() 
    {
        CriticalBlock b(eCrit);
        if (exception)
            throw exception.getLink();
    }

    virtual void execute()
    {
        doExecute(0, NULL);
    }

    //New child query code...
    virtual IThorChildGraph * queryChildGraph()
    {
        return this;
    }

    virtual IEclGraphResults * queryLocalGraph()
    {
        return this;
    }

    virtual IRoxieServerChildGraph * queryLoopGraph()
    {
        return this;
    }

    inline unsigned queryId() const
    {
        return id;
    }

    inline unsigned queryLoopCounter() const
    {
        return loopCounter;
    }

    void doExecute(unsigned parentExtractSize, const byte * parentExtract)
    {
        if (sinks.ordinality()==1)
            sinks.item(0).execute(parentExtractSize, parentExtract);
#ifdef PARALLEL_EXECUTE
        else if (!probeManager && !graphDefinition.isSequential())
        {
            class casyncfor: public CAsyncFor
            {
            public:
                IActivityGraph &parent;
                unsigned parentExtractSize;
                const byte * parentExtract;

                casyncfor(IRoxieServerActivityCopyArray &_sinks, IActivityGraph &_parent, unsigned _parentExtractSize, const byte * _parentExtract) : 
                    sinks(_sinks), parent(_parent), parentExtractSize(_parentExtractSize), parentExtract(_parentExtract) { }
                void Do(unsigned i)
                {
                    try
                    {
                        sinks.item(i).execute(parentExtractSize, parentExtract);
                    }
                    catch (IException *E)
                    {
                        parent.noteException(E);
                        throw;
                    }
                }
            private:
                IRoxieServerActivityCopyArray &sinks;
            } afor(sinks, *this, parentExtractSize, parentExtract);
            afor.For(sinks.ordinality(), sinks.ordinality());
        }
#endif
        else
        {
            ForEachItemIn(idx, sinks)
            {
                IRoxieServerActivity &sink = sinks.item(idx);
                sink.execute(parentExtractSize, parentExtract);
            }
        }
    }

    virtual IEclGraphResults *evaluate(unsigned parentExtractSize, const byte * parentExtract)
    {
        CriticalBlock block(evaluateCrit);
        results.setown(new CGraphResults);
        try
        {
            doExecute(parentExtractSize, parentExtract);
        }
        catch (...)
        {
            DBGLOG("Exception thrown in child query - cleaning up");
            reset();
            throw;
        }
        reset();
        return results.getClear();
    }

    //interface IRoxieServerChildGraph
    virtual void beforeExecute()
    {
        results.setown(new CGraphResults);
    }

    virtual IRoxieInput * startOutput(unsigned id, unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        IRoxieInput * ret = selectOutput(id);
        ret->start(parentExtractSize, parentExtract, paused);
        return ret;
    }

    virtual IRoxieInput * selectOutput(unsigned id)
    {
        ForEachItemIn(i, sinks)
        {
            IRoxieInput * ret = sinks.item(i).querySelectOutput(id);
            if (ret)
                return ret;
        }
        throwUnexpected();
        return NULL;
    }

    virtual void setInputResult(unsigned id, IGraphResult * result)
    {
        results->setResult(id, result);
    }

    virtual bool querySetInputResult(unsigned id, IRoxieInput * input)
    {
        ForEachItemIn(i, activities)
        {
            if (activities.item(i).querySetStreamInput(id, input))
                return true;
        }
        return false;
    }

    virtual void stopUnusedOutputs()
    {
        //Hmm not sure how to do this...
    }

    virtual void afterExecute()
    {
        ForEachItemIn(i, sinks)
        {
            sinks.item(i).stop(false);
        }
        if (graphSlaveContext.queryDebugContext())
        {
            graphSlaveContext.queryDebugContext()->checkBreakpoint(DebugStateGraphFinished, NULL, NULL);
        }
        reset();
    }

    virtual IRoxieGraphResults * execute(size32_t parentExtractSize, const byte *parentExtract)
    {
        doExecute(parentExtractSize, parentExtract);
        return LINK(results);
    }
    virtual void getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
    {
        results->getLinkedResult(count, ret, id);
    }
    virtual void getDictionaryResult(unsigned & count, byte * * & ret, unsigned id)
    {
        results->getLinkedResult(count, ret, id);
    }
    virtual void setResult(unsigned id, IGraphResult * result)
    {
        results->setResult(id, result);
    }
    virtual IRoxieInput * createResultIterator(unsigned id)
    {
        return results->createIterator(id);
    }
    virtual void setGraphLoopResult(IGraphResult * result)
    {
        graphLoopResults.appendResult(result);
    }
    virtual IRoxieInput * createGraphLoopResultIterator(unsigned id)
    {
        try
        {
            return graphLoopResults.createIterator(id);
        }
        catch (IException * e)
        {
            e->Release();
            throw MakeStringException(ROXIE_GRAPH_PROCESSING_ERROR, "Error reading graph result %d before it is calculated", id); 
        }
    }
    virtual void clearGraphLoopResults()
    {
        graphLoopResults.clear();
    }
    virtual void executeGraphLoop(size32_t parentExtractSize, const byte *parentExtract)
    {
        doExecute(parentExtractSize, parentExtract);
    }
    virtual void setGraphLoopResult(unsigned id, IGraphResult * result)
    {
        graphLoopResults.setResult(id, result);
    }
    virtual IRoxieInput * getGraphLoopResult(unsigned id)
    {
        return graphLoopResults.createIterator(id);
    }

    virtual void getProbeResponse(IPropertyTree *query)
    {
        if (probeManager)
            probeManager->getProbeResponse(query);
    }

    virtual IRoxieServerChildGraph * createGraphLoopInstance(unsigned loopCounter, unsigned parentExtractSize, const byte * parentExtract, const IRoxieContextLogger &logctx)
    {
        throwUnexpected();
    }

    virtual CGraphIterationInfo *selectGraphLoopOutput()
    {
        return NULL;
    }

    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor)
    {
        throwUnexpected();
    }

    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor)
    {
        throwUnexpected();
    }
};


class CIterationActivityGraph : public CActivityGraph
{
    IHThorArg * colocalParent;
    unsigned fixedParentExtractSize;
    const byte * fixedParentExtract;
    unsigned graphOutputActivityIndex;

public:
    CIterationActivityGraph(const char *_graphName, unsigned _id, ActivityArray &x, IProbeManager *_probeManager, 
                            unsigned _loopCounter, IRoxieSlaveContext *ctx, IHThorArg * _colocalParent, unsigned parentExtractSize, const byte * parentExtract, const IRoxieContextLogger &_logctx)
        : CActivityGraph(_graphName, _id, x, _probeManager, _logctx)
    {
        graphOutputActivityIndex = 0;
        loopCounter = _loopCounter;
        colocalParent = _colocalParent;
        graphSlaveContext.set(ctx);
        graphSlaveContext.setLoopCounter(loopCounter);
        graphCodeContext.set(ctx->queryCodeContext());

        fixedParentExtractSize = parentExtractSize;
        fixedParentExtract = parentExtract;
    }

    void createIterationGraph()
    {
        Owned<IRoxieServerActivity> pseudoActivity = new CPseudoActivity(*new CPseudoArg);

        ForEachItemIn(idx1, graphDefinition)
            activities.append(*LINK(pseudoActivity));

        graphOutputActivityIndex = queryGraphOutputIndex();
        recursiveCreateGraph(graphOutputActivityIndex);
    }

    unsigned queryGraphOutputIndex() const
    {
        ForEachItemIn(i, graphDefinition)
            if (graphDefinition.serverItem(i).getKind() == TAKgraphloopresultwrite)
                return i;
        throwUnexpected();
    }

    void recursiveCreateGraph(unsigned whichActivity)
    {
        //Check to see if already created
        IRoxieServerActivity & prevActivity = activities.item(whichActivity);
        if (prevActivity.queryId() != 0)
        {
            prevActivity.noteOutputUsed();      //We need to patch up the number of outputs for splitters.
            return;
        }

        IRoxieServerActivityFactory &donor = graphDefinition.serverItem(whichActivity);
        IRoxieServerActivity * activity = NULL;
        if (donor.isGraphInvariant())
        {
            ThorActivityKind kind = donor.getKind();
            switch (kind)
            {
            case TAKif:
            case TAKchildif:
            case TAKcase:
            case TAKchildcase:  // MORE RKC->GH - what about FILTER with a graph-invariant condition and other latestart activities?
                {
                    Owned<IHThorArg> helper = &donor.getHelper();
                    helper->onCreate(&graphCodeContext, colocalParent, NULL);
                    helper->onStart(fixedParentExtract, NULL);

                    unsigned branch;
                    switch (kind)
                    {
                    case TAKif: case TAKchildif:
                        branch = static_cast<IHThorIfArg *>(helper.get())->getCondition() ? 0 : 1;
                        break;
                    case TAKcase: case TAKchildcase:
                        branch = static_cast<IHThorCaseArg *>(helper.get())->getBranch();
                        if (branch >= donor.numInputs())
                            branch = donor.numInputs() - 1;
                        break;
                    default:
                        throwUnexpected();
                    }
                    helper.clear();

                    unsigned outputidx;
                    unsigned source = donor.getInput(branch, outputidx);
                    if (source ==(unsigned) -1)
                        activity = createRoxieServerNullActivity(&donor, probeManager);
                    else
                        activity = createRoxieServerPassThroughActivity(&donor, probeManager);

                    activities.replace(*activity, whichActivity);
                    activity->onCreate(&graphSlaveContext, colocalParent);
                    if (source ==(unsigned) -1)
                        return;

                    recursiveCreateGraph(source);
                    connectInput(whichActivity, 0, source, outputidx, loopCounter);
                    break;
                }
            }
        }

        if (!activity)
        {
            activity = donor.createActivity(probeManager);
            activities.replace(*activity, whichActivity);
            activity->onCreate(&graphSlaveContext, colocalParent);
            activity->resetOutputsUsed();

            unsigned inputidx = 0;
            loop
            {
                unsigned outputidx;
                unsigned source = donor.getInput(inputidx, outputidx);
                if (source==(unsigned) -1)
                    break;

                recursiveCreateGraph(source);
                connectInput(whichActivity, inputidx, source, outputidx, loopCounter);
                inputidx++;
            }
        }

        IntArray &dependencies = donor.queryDependencies();
        IntArray &dependencyIndexes = donor.queryDependencyIndexes();
        IntArray &dependencyControlIds = donor.queryDependencyControlIds();
        ForEachItemIn(idx2, dependencies)
        {
            unsigned input = dependencies.item(idx2);
            recursiveCreateGraph(input);
            activity->addDependency(activities.item(input),dependencyIndexes.item(idx2),dependencyControlIds.item(idx2));
        }
    }

    virtual CGraphIterationInfo *selectGraphLoopOutput()
    {
        IRoxieServerActivity &sourceActivity = activities.item(graphOutputActivityIndex);
        return new CGraphIterationInfo(&sourceActivity, sourceActivity.queryOutput(0), 0, loopCounter);
    }

    virtual void gatherIterationUsage(IRoxieServerLoopResultProcessor & processor)
    {
        ForEachItemIn(i, activities)
            activities.item(i).gatherIterationUsage(processor, fixedParentExtractSize, fixedParentExtract);
    }

    virtual void associateIterationOutputs(IRoxieServerLoopResultProcessor & processor)
    {
        ForEachItemIn(i, activities)
            activities.item(i).associateIterationOutputs(processor, fixedParentExtractSize, fixedParentExtract, probeManager, probes);
    }

};


class CDelayedActivityGraph : public CInterface, implements IActivityGraph
{
    StringAttr graphName;
    ActivityArray & graphDefinition;
    IProbeManager *probeManager;
    unsigned id;
    IRoxieSlaveContext * ctx;
    IHThorArg * colocalParent;

public:
    IMPLEMENT_IINTERFACE;

    CDelayedActivityGraph(const char *_graphName, unsigned _id, ActivityArray &x, IProbeManager *_probeManager)
        : probeManager(_probeManager), graphDefinition(x)
    {
        graphName.set(_graphName);
        id = _id;
        ctx = NULL;
        colocalParent = NULL;
    }

    virtual const char *queryName() const { return graphName.get(); }
    virtual void abort() { throwUnexpected(); }
    virtual void reset() { }
    virtual void execute() { throwUnexpected(); }
    virtual void getProbeResponse(IPropertyTree *query) { throwUnexpected(); }
    virtual void noteException(IException *E) { throwUnexpected(); }
    virtual void checkAbort() { throwUnexpected(); }
    virtual IThorChildGraph * queryChildGraph() { throwUnexpected(); }
    virtual IEclGraphResults * queryLocalGraph() { throwUnexpected(); }
    virtual IRoxieServerChildGraph * queryLoopGraph() { throwUnexpected(); }

    virtual void onCreate(IRoxieSlaveContext *_ctx, IHThorArg *_colocalParent) 
    { 
        ctx = _ctx;
        colocalParent = _colocalParent;
    }

    virtual IRoxieServerChildGraph * createGraphLoopInstance(unsigned loopCounter, unsigned parentExtractSize, const byte * parentExtract, const IRoxieContextLogger &logctx)
    {
        Owned<CIterationActivityGraph> ret = new CIterationActivityGraph(graphName, id, graphDefinition, probeManager, loopCounter, ctx, colocalParent, parentExtractSize, parentExtract, logctx);
        ret->createIterationGraph();
        return ret.getClear();
    }
};



IActivityGraph *createActivityGraph(const char *_graphName, unsigned id, ActivityArray &childFactories, IRoxieServerActivity *parentActivity, IProbeManager *_probeManager, const IRoxieContextLogger &_logctx)
{
    if (childFactories.isDelayed())
    {
        return new CDelayedActivityGraph(_graphName, id, childFactories, _probeManager);
    }
    else
    {
        Owned<IProbeManager> childProbe;
        if (_probeManager)
            childProbe.setown(_probeManager->startChildGraph(id, parentActivity));
        Owned<CActivityGraph> ret = new CActivityGraph(_graphName, id, childFactories, childProbe, _logctx);
        ret->createGraph();
        if (_probeManager)
            _probeManager->endChildGraph(childProbe, parentActivity);
        return ret.getClear();
    }
}

//================================================================================================================

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

// There is a bug in VC6 implemetation of protected which prevents nested classes from accessing owner's data. It can be tricky to work around - hence...
#if _MSC_VER==1200
#undef protected
#endif

static const char *sortAlgorithm;

class TestMetaData : public CInterface, implements IOutputMetaData 
{
public:
    IMPLEMENT_IINTERFACE;
    virtual size32_t getRecordSize(const void *) { return 10; }
    virtual size32_t getMinRecordSize() const { return 10; }
    virtual size32_t getFixedSize() const { return 10; }
    virtual void toXML(const byte * self, IXmlWriter & out) {}
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return 0; }
    virtual void destruct(byte * self)  {}
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputMetaData * querySerializedDiskMeta() { return NULL; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
} testMeta;

class TestInput : public CInterface, implements IRoxieInput
{
    char const * const *input;
    IRoxieSlaveContext *ctx;
    unsigned endSeen;
    bool eof;
    unsigned count;
    unsigned __int64 totalCycles;
    size32_t recordSize;
    unsigned activityId;

public:
    enum { STATEreset, STATEstarted, STATEstopped } state;
    bool allRead;
    IMPLEMENT_IINTERFACE;
    TestInput(IRoxieSlaveContext *_ctx, char const * const *_input) 
    { 
        ctx = _ctx; 
        input = _input; 
        count = 0;
        eof = false;
        allRead = false;
        endSeen = 0;
        recordSize = testMeta.getFixedSize();
        state = STATEreset;
        totalCycles = 0;
        activityId = 1;
    }
    virtual IOutputMetaData * queryOutputMeta() const { return &testMeta; }
    virtual void prestart(unsigned parentExtractSize, const byte *parentExtract)
    {
        ASSERT(state == STATEreset);
    }
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) 
    {
        ASSERT(state == STATEreset);
        state = STATEstarted; 
    }
    virtual IRoxieServerActivity *queryActivity()
    {
        throwUnexpected();
    }
    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        throwUnexpected();
    }
    virtual void stop(bool aborting) 
    {
        state = STATEstopped; 
    }
    virtual void reset() 
    {
        ASSERT(state == STATEstopped);
        eof = false; count = 0; endSeen = 0; allRead = false; state = STATEreset; totalCycles = 0;
    }
    virtual void resetEOF()
    {
        throwUnexpected();
    }
    virtual void checkAbort() {}
    virtual unsigned queryId() const { return activityId; };
    virtual const void *nextInGroup() 
    {
        ActivityTimer t(totalCycles, timeActivities, ctx->queryDebugContext());
        ASSERT(state == STATEstarted);
        ASSERT(allRead || !eof);
        if (eof)
            return NULL;
        const char *nextSource = input[count++];
        if (nextSource)
        {
            endSeen = 0;
            void *ret = ctx->queryRowManager().ALLOCATE(recordSize);
            memset(ret, 0, recordSize);
            strncpy((char *) ret, nextSource, recordSize);
            return ret;
        }
        else
        {
            endSeen++;
            if (endSeen==2)
                eof = true;
            return NULL;
        }
    }
    virtual bool nextGroup(ConstPointerArray & group) 
    {
        const void * next;
        while ((next = nextInGroup()) != NULL)
            group.append(next);
        if (group.ordinality())
            return true;
        return false;
    }
    virtual unsigned __int64 queryTotalCycles() const { return totalCycles; }
    virtual unsigned __int64 queryLocalCycles() const { return totalCycles; }
    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        return NULL;
    }

};

struct SortActivityTest : public ccdserver_hqlhelper::CThorSortArg {
public:
    struct CompareClass : public ICompare {
        virtual int docompare(const void * _left, const void * _right) const {
            return memcmp(_left, _right, 5);
        }
    } compare;
    virtual ICompare * queryCompare() { return &compare; }
    virtual IOutputMetaData * queryOutputMeta() 
    { 
        return &testMeta; 
    }
    virtual unsigned getAlgorithmFlags() { return TAFunstable; }
    virtual const char * getAlgorithm() { return sortAlgorithm; }
};
extern "C" IHThorArg * sortActivityTestFactory() { return new SortActivityTest; }

struct MergeActivityTest : public ccdserver_hqlhelper::CThorMergeArg {
    static bool isDedup;
public:
    struct CompareClass : public ICompare {
        virtual int docompare(const void * _left, const void * _right) const {
            return memcmp(_left, _right, 5);
        }
    } compare;
    virtual ICompare * queryCompare() { return &compare; }
    virtual IOutputMetaData * queryOutputMeta() 
    { 
        return &testMeta; 
    }
    virtual bool dedup() { return isDedup; }
};
bool MergeActivityTest::isDedup = false;
extern "C" IHThorArg * mergeActivityTestFactory() { return new MergeActivityTest; }

class CcdServerTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE(CcdServerTest);
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testHeapSort);
        CPPUNIT_TEST(testInsertionSort);
        CPPUNIT_TEST(testQuickSort);
        CPPUNIT_TEST(testMerge);
        CPPUNIT_TEST(testMergeDedup);
        CPPUNIT_TEST(testMiscellaneous);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();
protected:
    SlaveContextLogger logctx;
    Owned<const IQueryDll> queryDll;
    Owned<IRoxiePackage> package;
    Owned<IRoxieSlaveContext> ctx;
    Owned<IQueryFactory> queryFactory;

    void testSetup()
    {
        roxiemem::setTotalMemoryLimit(false, 100 * 1024 * 1024, 0, NULL);
    }

    void testCleanup()
    {
        roxiemem::releaseRoxieHeap();
    }

    void init()
    {
        package.setown(createRoxiePackage(NULL, NULL));
        ctx.setown(createSlaveContext(NULL, logctx, 0, 50*1024*1024, NULL));
        queryDll.setown(createExeQueryDll("roxie"));
        queryFactory.setown(createServerQueryFactory("test", queryDll.getLink(), *package, NULL));
        timer->reset();
    }

    void testActivity(IRoxieServerActivity *activity, char const * const *input, char const * const *output)
    {
        testActivity(activity, input, NULL, output);
    }

    void testActivity(IRoxieServerActivity *activity, char const * const *input, char const * const *input2, char const * const *output)
    {
        TestInput in(ctx, input);
        TestInput in2(ctx, input2);
        IRoxieInput *out = activity->queryOutput(0);
        IOutputMetaData *meta = out->queryOutputMeta();
        activity->setInput(0, &in);
        if (input2)
            activity->setInput(1, &in2);
        void *buf = alloca(meta->getFixedSize());

        for (unsigned iteration = 0; iteration < 8; iteration++)
        {
            // All activities should be able to be restarted multiple times in the same context (for child queries) or in a new context (for graph pooling, if we ever wanted it)
            // This should be true whether we read all, some, or none of the data.
            // Should not matter if an activity is not started
            if (iteration % 4 == 0)
                activity->onCreate(ctx, NULL);
            unsigned count = 0;
            if (iteration % 4 != 3)
            {
                activity->start(0, NULL, false);
                ASSERT(in.state == TestInput::STATEstarted);
                ASSERT(!input2 || in2.state == TestInput::STATEstarted);
                loop
                {
                    const void *next = out->nextInGroup();
                    if (!next)
                    {
                        ASSERT(output[count++] == NULL);
                        next = out->nextInGroup();
                        if (!next)
                        {
                            ASSERT(output[count++] == NULL);
                            break;
                        }
                    }
                    ASSERT(output[count] != NULL);
                    unsigned outsize = meta->getRecordSize(next);
                    memset(buf, 0, outsize);
                    strncpy((char *) buf, output[count++], outsize);
                    ASSERT(memcmp(next, buf, outsize) == 0);
                    ReleaseRoxieRow(next);
                    if (iteration % 4 == 2)
                        break;
                }
                if (iteration % 4 != 2)
                {
                    // Check that reading after end is harmless
                    in.allRead = true;
                    const void *next = out->nextInGroup();
                    ASSERT(next == NULL);
                }
            }
            activity->stop(false);
            ASSERT(in.state == TestInput::STATEstopped);
            ASSERT(!input2 || in2.state == TestInput::STATEstopped);
            activity->reset();
            ASSERT(in.state == TestInput::STATEreset);
            ASSERT(!input2 || in2.state == TestInput::STATEreset);
            ctx->queryRowManager().reportLeaks();
            ASSERT(ctx->queryRowManager().numPagesAfterCleanup(true) == 0);
        }
    }

    static int compareFunc(const void *l, const void *r)
    {
        return strcmp(*(char **) l, *(char **) r);
    }

    void testSort(unsigned type)
    {
        init();
        sortAlgorithm = NULL;
        if (type==2)
            sortAlgorithm = "heapSort";
        else if (type == 1)
            sortAlgorithm = "insertionSort";
        else
            sortAlgorithm = "quickSort";
        DBGLOG("Testing %s activity", sortAlgorithm);
        Owned <IRoxieServerActivityFactory> factory = createRoxieServerSortActivityFactory(1, 1, *queryFactory, sortActivityTestFactory, TAKsort);
        Owned <IRoxieServerActivity> activity = factory->createActivity(NULL);
        const char * test[] = { NULL, NULL };
        const char * test12345[] = { "1", "2", "3", "4", "5", NULL, NULL };
        const char * test54321[] = { "5", "4", "3", "2", "1", NULL, NULL };
        const char * test11111[] = { "1", "1", "1", "1", "1", NULL, NULL };
        const char * test11111_12345[] = { "1", "1", "1", "1", "1", NULL, "1", "2", "3", "4", "5", NULL, NULL };
        const char * test11111_54321[] = { "1", "1", "1", "1", "1", NULL, "5", "4", "3", "2", "1", NULL, NULL };
        const char * test54321_54321[] = { "5", "4", "3", "2", "1", NULL, "5", "4", "3", "2", "1", NULL, NULL };
        const char * test12345_12345[] = { "1", "2", "3", "4", "5", NULL, "1", "2", "3", "4", "5", NULL, NULL };
        testActivity(activity, test, test);
        testActivity(activity, test12345, test12345);
        testActivity(activity, test54321, test12345);
        testActivity(activity, test11111, test11111);
        testActivity(activity, test11111_12345, test11111_12345);
        testActivity(activity, test11111_54321, test11111_12345);
        testActivity(activity, test54321_54321, test12345_12345);

        // A few larger tests
        char *input[2002];
        char *output[2002];
        input[2000] = input[2001] = output[2000] = output[2001] = NULL;

        unsigned i;
        // identical
        for (i=0; i<2000; i++)
        {
            input[i] = new char[11];
            output[i] = new char[11];
            sprintf(input[i], "1");
            sprintf(output[i], "1");
        }
        testActivity(activity, input, output);
        // Ascending
        for (i=0; i<2000; i++)
        {
            sprintf(input[i], "%04d", i);
            sprintf(output[i], "%04d", i);
        }
        testActivity(activity, input, output);
        // Almost sorted
        for (i=0; i<20; i++)
        {
            unsigned h = i*100;
            sprintf(input[h], "%04d", 1900-h);
        }
        testActivity(activity, input, output);
        // Descending
        for (i=0; i<2000; i++)
        {
            sprintf(input[i], "%04d", 1999-i);
            sprintf(output[i], "%04d", i);
        }
        testActivity(activity, input, output);
        // Random
        for (i=0; i<2000; i++)
        {
            unsigned r = rand() % 1500;
            sprintf(input[i], "%04d", r);
            sprintf(output[i], "%04d", r);
        }
        qsort(output, 2000, sizeof(output[0]), compareFunc);
        testActivity(activity, input, output);

#if 0
        // Random
        #define BIGSORTSIZE 1000000
        char **linput = new char*[BIGSORTSIZE +2];
        char **loutput = new char *[BIGSORTSIZE+2];
        linput[BIGSORTSIZE] = linput[BIGSORTSIZE+1] = loutput[BIGSORTSIZE] = loutput[BIGSORTSIZE+1] = NULL;
        for (i=0; i<BIGSORTSIZE; i++)
        {
            unsigned r = rand() % 15000;
            linput[i] = loutput[i] = new char[11];
            sprintf(linput[i], "%04d", r);
        }
        qsort(loutput, BIGSORTSIZE, 4, compareFunc);
        testActivity(activity, linput, loutput);
        for (i=0; i<BIGSORTSIZE; i++)
        {
            delete [] linput[i];
        }
        delete [] linput;
        delete [] loutput;
#endif
        unsigned __int64 us = cycle_to_nanosec(factory->queryLocalCycles()/1000);
        DBGLOG("Simple %s sorts: activity time %u.%u ms", type==2?"Heap" : (type==1 ? "Insertion" : "Quick"), (int)(us/1000), (int)(us%1000));
        factory->resetNodeProgressInfo();
        if (type)
        {
            // Other than quicksort, it's supposed to be stable. Let's check that it is
            // All sort identical
            for (i=0; i<2000; i++)
            {
                sprintf(input[i], "1    %d", i);
                sprintf(output[i], "1    %d", i);
            }
            testActivity(activity, input, output);
            // Already sorted
            for (i=0; i<2000; i++)
            {
                sprintf(input[i], "%04d %d", i / 10, i);
                sprintf(output[i], "%04d %d", i / 10, i);
            }
            testActivity(activity, input, output);
            // Reverse order
            for (i=0; i<2000; i++)
            {
                sprintf(input[i], "%04d %d", 199 - (i / 10), i%10);
                sprintf(output[i], "%04d %d", i / 10, i%10);
            }
            testActivity(activity, input, output);
        }
        for (i=0; i<2000; i++)
        {
            delete [] input[i];
            delete [] output[i];
        }
        DBGLOG("Finished testing %s sort", type==2?"Heap" : (type==1 ? "Insertion" : "Quick"));
    }

    void testQuickSort()
    {
        testSort(0);
    }
    void testInsertionSort()
    {
        testSort(1);
    }
    void testHeapSort()
    {
        testSort(2);
    }
    void testMerge()
    {
        DBGLOG("testMerge");
        init();
        Owned <IRoxieServerActivityFactory> factory = createRoxieServerMergeActivityFactory(1, 1, *queryFactory, mergeActivityTestFactory, TAKmerge);
        factory->setInput(0,0,0);
        factory->setInput(1,0,0);
        Owned <IRoxieServerActivity> activity = factory->createActivity(NULL);
        const char * test[] = { NULL, NULL };
        const char * test12345[] = { "1", "2", "3", "4", "5", NULL, NULL };
        const char * test1122334455[] = { "1", "1", "2", "2", "3", "3", "4", "4", "5", "5", NULL, NULL };
        const char * test11111[] = { "1", "1", "1", "1", "1", NULL, NULL };
        const char * test1111111111[] = { "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", NULL, NULL };
        const char * test11111_12345[] = { "1", "1", "1", "1", "1", NULL, "1", "2", "3", "4", "5", NULL, NULL };
        const char * test1111112345[] = { "1", "1", "1", "1", "1", "1", "2", "3", "4", "5", NULL, NULL };
        const char * test11111111111122334455[] = { "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "2", "2", "3", "3", "4", "4", "5", "5", NULL, NULL };
        
        testActivity(activity, test, test, test);
        testActivity(activity, test12345, test, test12345);
        testActivity(activity, test, test12345, test12345);
        testActivity(activity, test12345, test12345, test1122334455);
        testActivity(activity, test11111, test, test11111);
        testActivity(activity, test, test11111, test11111);
        testActivity(activity, test11111, test11111, test1111111111);
        testActivity(activity, test11111_12345, test, test1111112345);
        testActivity(activity, test11111_12345, test11111_12345, test11111111111122334455);

        // Should really test WHICH side gets kept...
        // Should test with more than 2 inputs...
        DBGLOG("testMerge done");
    }

    void testMergeDedup()
    {
        DBGLOG("testMergeDedup");
        init();
        MergeActivityTest::isDedup = true;
        Owned <IRoxieServerActivityFactory> factory = createRoxieServerMergeActivityFactory(1, 1, *queryFactory, mergeActivityTestFactory, TAKmerge);
        factory->setInput(0,0,0);
        factory->setInput(1,0,0);
        Owned <IRoxieServerActivity> activity = factory->createActivity(NULL);
        const char * test[] = { NULL, NULL };
        const char * test12345[] = { "1", "2", "3", "4", "5", NULL, NULL };
        const char * test11111[] = { "1", "1", "1", "1", "1", NULL, NULL };
        const char * test11111_12345[] = { "1", "1", "1", "1", "1", NULL, "1", "2", "3", "4", "5", NULL, NULL };
        const char * test1111112345[] = { "1", "1", "1", "1", "1", "1", "2", "3", "4", "5", NULL, NULL };
        
        testActivity(activity, test11111, test, test11111);   // No dedup within a stream
        testActivity(activity, test11111, test11111, test11111);   // No dedup within a stream
        testActivity(activity, test, test11111, test11111);
        testActivity(activity, test, test, test);
        testActivity(activity, test12345, test, test12345);
        testActivity(activity, test, test12345, test12345);
        testActivity(activity, test12345, test12345, test12345);
        testActivity(activity, test11111_12345, test, test1111112345);
        testActivity(activity, test11111_12345, test11111_12345, test1111112345);

        // Should really test WHICH side gets kept...
        // Should test with more than 2 inputs...
        DBGLOG("testMergeDedup done");
    }

    void testMiscellaneous()
    {
        DBGLOG("sizeof(CriticalSection)=%u", (unsigned) sizeof(CriticalSection));
        DBGLOG("sizeof(SpinLock)=%u", (unsigned) sizeof(SpinLock));
        DBGLOG("sizeof(CJoinGroup)=%u", (unsigned) sizeof(CJoinGroup));
        ASSERT(sizeof(CJoinGroup) <= 120);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( CcdServerTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CcdServerTest, "CcdServerTest" );

#endif
