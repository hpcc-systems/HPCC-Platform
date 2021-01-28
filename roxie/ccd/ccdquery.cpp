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

#include <platform.h>
#include <jlib.hpp>

#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdsnmp.hpp"
#include "ccdserver.hpp"
#include "ccdcontext.hpp"

#include "thorplugin.hpp"

#include <thread>
#include <mutex>

void ActivityArray::append(IActivityFactory &cur)
{
    hash.setValue(cur.queryId(), activities.ordinality());
    activities.append(cur);
}

unsigned ActivityArray::findActivityIndex(unsigned id) const
{
    unsigned *ret = hash.getValue(id);
    if (ret)
        return *ret;
    return NotFound;
}

unsigned ActivityArray::recursiveFindActivityIndex(unsigned id)
{
    // NOTE - this returns the activity index of the PARENT of the specified activity

    unsigned *ret = hash.getValue(id);
    if (ret)
        return *ret;
    ForEachItem(idx)
    {
        IActivityFactory & cur = item(idx);
        unsigned childId;
        for (unsigned childIdx = 0;;childIdx++)
        {
            ActivityArray * children = cur.queryChildQuery(childIdx, childId);
            if (!children)
                break;
            if (children->recursiveFindActivityIndex(id) != NotFound)
            {
                hash.setValue(id, idx);
                return idx;
            }
        }
    }
    return NotFound;
}

//----------------------------------------------------------------------------------------------
// Class CQueryDll maps dlls into loadable workunits, complete with caching to ensure that a refresh of the QuerySet 
// can avoid reloading dlls, and that the same CQueryDll (and the objects it owns) can be shared between server and 
// multiple agent channels
//----------------------------------------------------------------------------------------------

class CQueryDll : implements IQueryDll, public CInterface
{
    StringAttr dllName;
    Owned <ILoadedDllEntry> dll;
    Owned <IConstWorkUnit> wu;
    std::once_flag started;
    Owned<IException> e;
    static CriticalSection dllCacheLock;
    static CopyMapStringToMyClass<CQueryDll> dllCache;

public:
    IMPLEMENT_IINTERFACE;

    CQueryDll(const char *_dllName) : dllName(_dllName)
    {
    }
private:
    void init(bool isExe)
    {
        std::call_once(started, [this, isExe]()
        {
            try
            {
                dll.setown(isExe ? createExeDllEntry(dllName) : queryRoxieDllServer().loadDll(dllName, DllLocationDirectory));
                StringBuffer wuXML;
                if (!selfTestMode && getEmbeddedWorkUnitXML(dll, wuXML))
                {
                    Owned<ILocalWorkUnit> localWU = createLocalWorkUnit(wuXML);
                    wu.setown(localWU->unlock());
                }
            }
            catch (IException *E)
            {
                EXCLOG(E);
                e.setown(E);
            }
        });
        if (e)
            throw e.getLink();
    }
public:
    virtual void beforeDispose()
    {
        CriticalBlock b(dllCacheLock);
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table. 
        // So only remove from hash table if what we find there matches the item that is being deleted.
        CQueryDll *goer = dllCache.getValue(dllName);
        if (goer == this)
            dllCache.remove(dllName);
    }
    static const CQueryDll *getQueryDll(const char *dllName, bool isExe)
    {
        CQueryDll *cached = nullptr;
        {
            CriticalBlock b(dllCacheLock);
            cached = dllCache.getValue(dllName);
            if (!cached || !cached->isAliveAndLink())
            {
                cached = new CQueryDll(dllName);
                dllCache.setValue(dllName, cached);
            }
        }
        Owned<CQueryDll> dll(cached);  // Make sure it's released if init() throws an exception
        dll->init(isExe);
        return dll.getClear();
    }
    static const IQueryDll *getWorkUnitDll(IConstWorkUnit *wu)
    {
        SCMStringBuffer dllName;
        Owned<IConstWUQuery> q = wu->getQuery();
        q->getQueryDllName(dllName);
        if (dllName.length() == 0)
        {
            if (wu->getCodeVersion() == 0)
                throw makeStringExceptionV(ROXIE_MISSING_DLL, "Attempting to load workunit %s that hasn't been compiled", wu->queryWuid());
            else
                throw makeStringExceptionV(ROXIE_MISSING_DLL, "Attempting to load workunit %s with no associated dll", wu->queryWuid());
        }
        return getQueryDll(dllName.str(), false);
    }
    virtual HelperFactory *getFactory(const char *helperName) const
    {
        return (HelperFactory *) dll->getEntry(helperName);
    }
    virtual ILoadedDllEntry *queryDll() const
    {
        return dll;
    }
    virtual IConstWorkUnit *queryWorkUnit() const
    {
        return wu;
    }
};
CriticalSection CQueryDll::dllCacheLock;
CopyMapStringToMyClass<CQueryDll> CQueryDll::dllCache;

extern const IQueryDll *createQueryDll(const char *dllName)
{
    return CQueryDll::getQueryDll(dllName, false);
}

extern const IQueryDll *createExeQueryDll(const char *exeName)
{
    return CQueryDll::getQueryDll(exeName, true);
}

extern const IQueryDll *createWuQueryDll(IConstWorkUnit *wu)
{
    return CQueryDll::getWorkUnitDll(wu);
}

// Add information to the xref information to be returned for a control:getQueryXrefInfo request

IPropertyTree * addXrefInfo(IPropertyTree &reply, const char *section, const char *name)
{
    VStringBuffer xpath("%s[@name='%s']", section, name);
    if (!reply.hasProp(xpath))
    {
        IPropertyTree *info = reply.addPropTree(section);
        info->setProp("@name", name);
        return info;
    }
    return NULL;
}

extern void addXrefFileInfo(IPropertyTree &reply, const IResolvedFile *dataFile)
{
    if (dataFile->isSuperFile())
    {
        IPropertyTree *info = addXrefInfo(reply, "SuperFile", dataFile->queryFileName());
        if (info)
        {
            int numSubs = dataFile->numSubFiles();
            for (int i = 0; i < numSubs; i++)
            {
                StringBuffer subName;
                dataFile->getSubFileName(i, subName);
                addXrefInfo(*info, "File", subName.str());
            }
        }
    }
    else
        addXrefInfo(reply, "File", dataFile->queryFileName());
}

extern void addXrefLibraryInfo(IPropertyTree &reply, const char *libraryName)
{
    addXrefInfo(reply, "Library", libraryName);
}

//----------------------------------------------------------------------------------------------
// Class CSharedOnceContext manages the context for a query's ONCE code, which is shared between
// all agent and server contexts on a node
//----------------------------------------------------------------------------------------------

class CSharedOnceContext : public CInterfaceOf<ISharedOnceContext>
{
public:
    CSharedOnceContext()
    {
    }

    ~CSharedOnceContext()
    {
    }

    virtual IDeserializedResultStore &queryOnceResultStore() const
    {
        assertex(onceResultStore!= NULL);
        return *onceResultStore;
    }

    virtual IPropertyTree &queryOnceContext(const IQueryFactory *factory, const IRoxieContextLogger &logctx) const
    {
        checkOnceDone(factory, logctx);
        assertex(onceContext != NULL);
        return *onceContext;
    }

    virtual void checkOnceDone(const IQueryFactory *factory, const IRoxieContextLogger &logctx) const
    {
        if (calculatingOnce)   // NOTE - this must be outside the critsec or you deadlock. It is still effectively protected by the critsec
            return;
        CriticalBlock b(onceCrit);
        if (!onceContext)
        {
            calculatingOnce = true;
            onceContext.setown(createPTree(ipt_lowmem));
            onceResultStore.setown(createDeserializedResultStore());
            Owned <IRoxieServerContext> ctx = createOnceServerContext(factory, logctx);
            onceManager.set(&ctx->queryRowManager());
            try
            {
                ctx->process();
                ctx->done(false);
            }
            catch (IException *E)
            {
                ctx->done(true);
                onceException.setown(E);
            }
            catch (...)
            {
                ctx->done(true);
                onceException.setown(MakeStringException(ROXIE_INTERNAL_ERROR, "Unknown exception in ONCE code"));
            }
            calculatingOnce = false;
        }
        if (onceException)
            throw onceException.getLink();
    }

protected:
    mutable CriticalSection onceCrit;
    mutable Owned<roxiemem::IRowManager> onceManager; // release AFTER resultStore
    mutable Owned<IPropertyTree> onceContext;
    mutable Owned<IDeserializedResultStore> onceResultStore;
    mutable Owned<IException> onceException;
    mutable bool calculatingOnce = false;

};

//----------------------------------------------------------------------------------------------
// Class CQueryOptions is used to store options affecting the execution of a query
// These can be set globally, by the query workunit, or by the query XML parameters
//----------------------------------------------------------------------------------------------

QueryOptions::QueryOptions()
{
    priority = 0;
    timeLimit = defaultTimeLimit[0];
    warnTimeLimit = defaultWarnTimeLimit[0];

    memoryLimit = defaultMemoryLimit;

    parallelJoinPreload = defaultParallelJoinPreload;
    fullKeyedJoinPreload = defaultFullKeyedJoinPreload;
    keyedJoinPreload = defaultKeyedJoinPreload;
    concatPreload = defaultConcatPreload;
    fetchPreload = defaultFetchPreload;
    prefetchProjectPreload = defaultPrefetchProjectPreload;
    bindCores = coresPerQuery;
    strandBlockSize = defaultStrandBlockSize;
    forceNumStrands = defaultForceNumStrands;
    heapFlags = defaultHeapFlags;

    checkingHeap = defaultCheckingHeap;
    disableLocalOptimizations = defaultDisableLocalOptimizations;
    enableFieldTranslation = fieldTranslationEnabled;
    stripWhitespaceFromStoredDataset = ((ptr_ignoreWhiteSpace & defaultXmlReadFlags) != 0);
    timeActivities = defaultTimeActivities;
    traceEnabled = defaultTraceEnabled;
    traceLimit = defaultTraceLimit;
    noSeekBuildIndex = defaultNoSeekBuildIndex;
    allSortsMaySpill = false; // No global default for this
    failOnLeaks = alwaysFailOnLeaks;
    collectFactoryStatistics = defaultCollectFactoryStatistics;
    parallelWorkflow = false;
    numWorkflowThreads = 1;
}

QueryOptions::QueryOptions(const QueryOptions &other)
{
    priority = other.priority;
    timeLimit = other.timeLimit;
    warnTimeLimit = other.warnTimeLimit;

    memoryLimit = other.memoryLimit;

    parallelJoinPreload = other.parallelJoinPreload;;
    fullKeyedJoinPreload = other.fullKeyedJoinPreload;
    keyedJoinPreload = other.keyedJoinPreload;
    concatPreload = other.concatPreload;
    fetchPreload = other.fetchPreload;
    prefetchProjectPreload = other.prefetchProjectPreload;
    bindCores = other.bindCores;
    strandBlockSize = other.strandBlockSize;
    forceNumStrands = other.forceNumStrands;
    heapFlags = other.heapFlags;

    checkingHeap = other.checkingHeap;
    disableLocalOptimizations = other.disableLocalOptimizations;
    enableFieldTranslation = other.enableFieldTranslation;
    stripWhitespaceFromStoredDataset = other.stripWhitespaceFromStoredDataset;
    timeActivities = other.timeActivities;
    traceEnabled = other.traceEnabled;
    traceLimit = other.traceLimit;
    noSeekBuildIndex = other.noSeekBuildIndex;
    allSortsMaySpill = other.allSortsMaySpill;
    failOnLeaks = other.failOnLeaks;
    collectFactoryStatistics = other.collectFactoryStatistics;

    parallelWorkflow = other.parallelWorkflow;
    numWorkflowThreads = other.numWorkflowThreads;
}

void QueryOptions::setFromWorkUnit(IConstWorkUnit &wu, const IPropertyTree *stateInfo)
{
    // calculate priority before others since it affects the defaults of others
    updateFromWorkUnit(priority, wu, "priority");
    if (stateInfo)
        updateFromContext(priority, stateInfo, "@priority");
    timeLimit = defaultTimeLimit[priority];
    warnTimeLimit = defaultWarnTimeLimit[priority];
    updateFromWorkUnit(timeLimit, wu, "timeLimit");
    updateFromWorkUnit(warnTimeLimit, wu, "warnTimeLimit");
    updateFromWorkUnitM(memoryLimit, wu, "memoryLimit");
    if (stateInfo)
    {
        updateFromContext(timeLimit, stateInfo, "@timeLimit");
        updateFromContext(warnTimeLimit, stateInfo, "@warnTimeLimit");
        updateFromContextM(memoryLimit, stateInfo, "@memoryLimit");
    }

    updateFromWorkUnit(parallelJoinPreload, wu, "parallelJoinPreload");
    updateFromWorkUnit(fullKeyedJoinPreload, wu, "fullKeyedJoinPreload");
    updateFromWorkUnit(keyedJoinPreload, wu, "keyedJoinPreload");
    updateFromWorkUnit(concatPreload, wu, "concatPreload");
    updateFromWorkUnit(fetchPreload, wu, "fetchPreload");
    updateFromWorkUnit(prefetchProjectPreload, wu, "prefetchProjectPreload");
    updateFromWorkUnit(bindCores, wu, "bindCores");
    updateFromWorkUnit(strandBlockSize, wu, "strandBlockSize");
    updateFromWorkUnit(forceNumStrands, wu, "forceNumStrands");
    updateFromWorkUnit(heapFlags, wu, "heapFlags");

    updateFromWorkUnit(checkingHeap, wu, "checkingHeap");
    updateFromWorkUnit(disableLocalOptimizations, wu, "disableLocalOptimizations");
    updateFromWorkUnit(enableFieldTranslation, wu, "layoutTranslation");  // Name is different for compatibility reasons
    updateFromWorkUnit(stripWhitespaceFromStoredDataset, wu, "stripWhitespaceFromStoredDataset");
    updateFromWorkUnit(timeActivities, wu, "timeActivities");
    updateFromWorkUnit(traceEnabled, wu, "traceEnabled");
    updateFromWorkUnit(traceLimit, wu, "traceLimit");
    updateFromWorkUnit(allSortsMaySpill, wu, "allSortsMaySpill");
    updateFromWorkUnit(failOnLeaks, wu, "failOnLeaks");
    updateFromWorkUnit(noSeekBuildIndex, wu, "noSeekBuildIndex");
    updateFromWorkUnit(collectFactoryStatistics, wu, "collectFactoryStatistics");

    updateFromWorkUnit(parallelWorkflow, wu, "parallelWorkflow");
    updateFromWorkUnit(numWorkflowThreads, wu, "numWorkflowthreads");
}

void QueryOptions::updateFromWorkUnitM(memsize_t &value, IConstWorkUnit &wu, const char *name)
{
    value = (memsize_t) wu.getDebugValueInt64(name, value);
}

void QueryOptions::updateFromWorkUnit(int &value, IConstWorkUnit &wu, const char *name)
{
    value = wu.getDebugValueInt(name, value);
}

void QueryOptions::updateFromWorkUnit(unsigned &value, IConstWorkUnit &wu, const char *name)
{
    value = (unsigned) wu.getDebugValueInt(name, value);
}

void QueryOptions::updateFromWorkUnit(bool &value, IConstWorkUnit &wu, const char *name)
{
    value = wu.getDebugValueBool(name, value);
}

void QueryOptions::updateFromWorkUnit(RecordTranslationMode &value, IConstWorkUnit &wu, const char *name)
{
    SCMStringBuffer val;
    wu.getDebugValue(name, val);
    if (val.length())
        value = getTranslationMode(val.str(), false);
}

void QueryOptions::setFromContext(const IPropertyTree *ctx)
{
    if (ctx)
    {
        // Note: priority cannot be set at context level
        updateFromContext(timeLimit, ctx, "@timeLimit", "_TimeLimit");
        updateFromContext(warnTimeLimit, ctx, "@warnTimeLimit", "_WarnTimeLimit");
        updateFromContextM(memoryLimit, ctx, "@memoryLimit", "_MemoryLimit");
        updateFromContext(parallelJoinPreload, ctx, "@parallelJoinPreload", "_ParallelJoinPreload");
        updateFromContext(fullKeyedJoinPreload, ctx, "@fullKeyedJoinPreload", "_FullKeyedJoinPreload");
        updateFromContext(keyedJoinPreload, ctx, "@keyedJoinPreload", "_KeyedJoinPreload");
        updateFromContext(concatPreload, ctx, "@concatPreload", "_ConcatPreload");
        updateFromContext(fetchPreload, ctx, "@fetchPreload", "_FetchPreload");
        updateFromContext(prefetchProjectPreload, ctx, "@prefetchProjectPreload", "_PrefetchProjectPreload");
        updateFromContext(bindCores, ctx, "@bindCores", "_bindCores");
        updateFromContext(strandBlockSize, ctx, "@strandBlockSize", "_strandBlockSize");
        updateFromContext(forceNumStrands, ctx, "@forceNumStrands", "_forceNumStrands");
        updateFromContext(heapFlags, ctx, "@heapFlags", "_HeapFlags");

        updateFromContext(checkingHeap, ctx, "@checkingHeap", "_CheckingHeap");
        // Note: disableLocalOptimizations is not permitted at context level (too late)
        // Note: enableFieldTranslation is not permitted at context level (generally too late anyway)
        updateFromContext(stripWhitespaceFromStoredDataset, ctx, "_StripWhitespaceFromStoredDataset", "@stripWhitespaceFromStoredDataset");
        updateFromContext(timeActivities, ctx, "@timeActivities", "_TimeActivities");
        updateFromContext(traceEnabled, ctx, "@traceEnabled", "_TraceEnabled");
        updateFromContext(traceLimit, ctx, "@traceLimit", "_TraceLimit");
        updateFromContext(noSeekBuildIndex, ctx, "@noSeekBuildIndex", "_NoSeekBuildIndex");
        // Note: allSortsMaySpill is not permitted at context level (too late anyway, unless I refactored)
        updateFromContext(failOnLeaks, ctx, "@failOnLeaks", "_FailOnLeaks");
        updateFromContext(collectFactoryStatistics, ctx, "@collectFactoryStatistics", "_CollectFactoryStatistics");

        updateFromContext(parallelWorkflow, ctx, "@parallelWorkflow", "_parallelWorkflow");
        updateFromContext(numWorkflowThreads, ctx, "@numWorkflowThreads", "_numWorkflowThreads");
    }
}

const char * QueryOptions::findProp(const IPropertyTree *ctx, const char *name1, const char *name2)
{
    if (name1 && ctx->hasProp(name1))
        return name1;
    else if (name2 && ctx->hasProp(name2))
        return name2;
    else
        return NULL;
}

void QueryOptions::updateFromContextM(memsize_t &value, const IPropertyTree *ctx, const char *name1, const char *name2)
{
    const char *name = findProp(ctx, name1, name2);
    if (name)
        value = (memsize_t) ctx->getPropInt64(name);
}

void QueryOptions::updateFromContext(int &value, const IPropertyTree *ctx, const char *name1, const char *name2)
{
    const char *name = findProp(ctx, name1, name2);
    if (name)
        value = ctx->getPropInt(name);
}

void QueryOptions::updateFromContext(unsigned &value, const IPropertyTree *ctx, const char *name1, const char *name2)
{
    const char *name = findProp(ctx, name1, name2);
    if (name)
        value = (unsigned) ctx->getPropInt(name);
}

void QueryOptions::updateFromContext(bool &value, const IPropertyTree *ctx, const char *name1, const char *name2)
{
    const char *name = findProp(ctx, name1, name2);
    if (name)
        value = ctx->getPropBool(name);
}

void QueryOptions::setFromAgentLoggingFlags(unsigned loggingFlags)
{
    // MORE - priority/timelimit ?
    checkingHeap = (loggingFlags & LOGGING_CHECKINGHEAP) != 0;
    timeActivities = (loggingFlags & LOGGING_TIMEACTIVITIES) != 0;
}

//----------------------------------------------------------------------------------------------
// Class CQueryFactory is the main implementation of IQueryFactory, combining a IQueryDll and a
// package context into an object that can quickly create a the query context that executes a specific
// instance of a Roxie query. 
// Caching is used to ensure that only queries that are affected by a package change need to be reloaded.
// Derived classes handle the differences between agent and server side factories
//----------------------------------------------------------------------------------------------

class CQueryFactory : implements IQueryFactory, implements IResourceContext, public CInterface
{
protected:
    const IRoxiePackage &package;
    Owned<const IQueryDll> dll;
    Linked<ISharedOnceContext> sharedOnceContext;
    MapStringToActivityArray graphMap;
    StringAttr id;
    StringBuffer errorMessage;
    MapIdToActivityFactory allActivities;

    QueryOptions options;
    bool dynamic;
    bool isSuspended;
    bool isLoadFailed;
    ClusterType targetClusterType;
    unsigned libraryInterfaceHash;
    hash64_t hashValue;

    static CriticalSection activeQueriesCrit;
    static CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> activeQueries;    // Active queries
    static CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> queryCache;       // Active and loading queries

    mutable CIArrayOf<TerminationCallbackInfo> callbacks;
    mutable CriticalSection callbacksCrit;
public:
    static CriticalSection queryCacheCrit;

protected:
    IRoxieServerActivityFactory *createActivityFactory(ThorActivityKind kind, unsigned subgraphId, IPropertyTree &node)
    {
        unsigned id = node.getPropInt("@id", 0);
        unsigned rid = id;

        if (isSuspended)
            return createRoxieServerDummyActivityFactory(id, subgraphId, *this, NULL, TAKnone, node, false); // Is there actually any point?
        switch (options.priority)
        {
        case 1:
            rid |= ROXIE_HIGH_PRIORITY;
            break;
        case 2:
            rid |= ROXIE_SLA_PRIORITY;
            break;
        }
        StringBuffer helperName;
        node.getProp("att[@name=\"helper\"]/@value", helperName);
        if (!helperName.length())
            helperName.append("fAc").append(id);
        HelperFactory *helperFactory = dll->getFactory(helperName);
        if (!helperFactory)
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Internal error: helper function %s not exported", helperName.str());

        RemoteActivityId remoteId(rid, hashValue);
        RemoteActivityId remoteId2(rid | ROXIE_ACTIVITY_FETCH, hashValue);

        switch (kind)
        {
        case TAKalljoin:
        case TAKalldenormalize:
        case TAKalldenormalizegroup:
            return createRoxieServerAllJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKapply:
            return createRoxieServerApplyActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKaggregate:
        case TAKexistsaggregate:    // could special case.
        case TAKcountaggregate:
            return createRoxieServerAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKcase:
        case TAKchildcase:
            return createRoxieServerCaseActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isGraphIndependent(node));
        case TAKcatch:
        case TAKskipcatch:
        case TAKcreaterowcatch:
            return createRoxieServerCatchActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchilditerator:
            return createRoxieServerChildIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchoosesets:
            return createRoxieServerChooseSetsActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchoosesetsenth:
            return createRoxieServerChooseSetsEnthActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchoosesetslast:
            return createRoxieServerChooseSetsLastActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKproject:
        case TAKcountproject:
            return createRoxieServerProjectActivityFactory(id, subgraphId, *this, helperFactory, kind, node); // code is common between Project, CountProject
        case TAKfilterproject:
            return createRoxieServerFilterProjectActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKdatasetresult:
        case TAKrowresult:
            return createRoxieServerDatasetResultActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKdedup:
            return createRoxieServerDedupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKdegroup:
            return createRoxieServerDegroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKcsvread:
        case TAKxmlread:
        case TAKjsonread:
        case TAKdiskread:
        {       
            if (node.getPropBool("att[@name='_isSpill']/@value", false) || node.getPropBool("att[@name='_isSpillGlobal']/@value", false))
                return createRoxieServerSpillReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
            else
                return createRoxieServerDiskReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        }
        case TAKspillread:
        case TAKmemoryspillread:
            return createRoxieServerSpillReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKdisknormalize:
        case TAKdiskcount:
        case TAKdiskaggregate:
        case TAKdiskgroupaggregate:
            return createRoxieServerDiskReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKchildnormalize:
            return createRoxieServerNewChildNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchildaggregate:
            return createRoxieServerNewChildAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchildgroupaggregate:
            return createRoxieServerNewChildGroupAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKchildthroughnormalize:
            return createRoxieServerNewChildThroughNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKcsvwrite:
        case TAKdiskwrite:
        case TAKxmlwrite:
        case TAKjsonwrite:
        case TAKmemoryspillwrite:
        case TAKspillwrite:
            return createRoxieServerDiskWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKindexwrite:
            return createRoxieServerIndexWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKenth:
            return createRoxieServerEnthActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKfetch:
        case TAKcsvfetch:
        case TAKxmlfetch:
        case TAKjsonfetch:
            return createRoxieServerFetchActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKfilter:
            return createRoxieServerFilterActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKfiltergroup:
            return createRoxieServerFilterGroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKfirstn:
            return createRoxieServerFirstNActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKfunnel:
            return createRoxieServerConcatActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKgroup:
            return createRoxieServerGroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKhashaggregate:
            return createRoxieServerHashAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKif:
        case TAKchildif:
            return createRoxieServerIfActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isGraphIndependent(node));
        case TAKifaction:
            return createRoxieServerIfActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKparallel:
            return createRoxieServerParallelActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKsequential:
            return createRoxieServerSequentialActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKindexread:
            return createRoxieServerIndexReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKindexnormalize:
            return createRoxieServerIndexNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKindexcount:
            return createRoxieServerIndexCountActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKindexaggregate:
            return createRoxieServerIndexAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
            return createRoxieServerIndexGroupAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId);
        case TAKhashdedup:
            return createRoxieServerHashDedupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKhashdenormalize:
        case TAKhashdistribute:
        case TAKhashdistributemerge:
        case TAKhashjoin:
            throwUnexpected();  // Code generator should have removed or transformed
        case TAKiterate:
            return createRoxieServerIterateActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKprocess:
            return createRoxieServerProcessActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKjoin:
        case TAKjoinlight:
        case TAKdenormalize:
        case TAKdenormalizegroup:
            return createRoxieServerJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKkeyeddistribute:
            throwUnexpected();  // Code generator should have removed or transformed
        case TAKkeyedjoin:
        case TAKkeyeddenormalize:
        case TAKkeyeddenormalizegroup:
            return createRoxieServerKeyedJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId, remoteId2);
        case TAKlimit:
            return createRoxieServerLimitActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKlookupjoin:
        case TAKlookupdenormalize:
        case TAKlookupdenormalizegroup:
        case TAKsmartjoin:
        case TAKsmartdenormalize:
        case TAKsmartdenormalizegroup:
            return createRoxieServerLookupJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKmerge:
            return createRoxieServerMergeActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnormalize:
            return createRoxieServerNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnormalizechild:
            return createRoxieServerNormalizeChildActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnormalizelinkedchild:
            return createRoxieServerNormalizeLinkedChildActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnull:
            return createRoxieServerNullActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsideeffect:
            return createRoxieServerSideEffectActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKsimpleaction:
            return createRoxieServerActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), isRootAction(node));
        case TAKparse:
            return createRoxieServerParseActivityFactory(id, subgraphId, *this, helperFactory, kind, node, this);
        case TAKworkunitwrite:
            return createRoxieServerWorkUnitWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), isRootAction(node));
        case TAKdictionaryworkunitwrite:
            return createRoxieServerWorkUnitWriteDictActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), isRootAction(node));
        case TAKpiperead:
            return createRoxieServerPipeReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKpipethrough:
            return createRoxieServerPipeThroughActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKpipewrite:
            return createRoxieServerPipeWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), isRootAction(node));
        case TAKpull:
            return createRoxieServerPullActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKtrace:
            return createRoxieServerTraceActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKlinkedrawiterator:
            return createRoxieServerLinkedRawIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKremoteresult:
            return createRoxieServerRemoteResultActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), isRootAction(node));
        case TAKrollup:
            return createRoxieServerRollupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsample:
            return createRoxieServerSampleActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKselectn:
            return createRoxieServerSelectNActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKselfjoin:
        case TAKselfjoinlight:
            return createRoxieServerSelfJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKskiplimit:
        case TAKcreaterowlimit:
            return createRoxieServerSkipLimitActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKhttp_rowdataset:
        case TAKsoap_rowdataset:
            return createRoxieServerSoapRowCallActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsoap_rowaction:
            return createRoxieServerSoapRowActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKsoap_datasetdataset:
            return createRoxieServerSoapDatasetCallActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsoap_datasetaction:
            return createRoxieServerSoapDatasetActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKsort:
            return createRoxieServerSortActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKspill:
        case TAKmemoryspillsplit:
            return createRoxieServerThroughSpillActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsplit:
            return createRoxieServerSplitActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKstreamediterator:
            return createRoxieServerStreamedIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKinlinetable:
            return createRoxieServerInlineTableActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKthroughaggregate:
            throwUnexpected(); // Concept of through aggregates has been proven not to work in Roxie - codegen should not be creating them any more.
        case TAKtopn:
            return createRoxieServerTopNActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKworkunitread:
            return createRoxieServerWorkUnitReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKxmlparse:
            return createRoxieServerXmlParseActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKquantile:
            return createRoxieServerQuantileActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKregroup:
            return createRoxieServerRegroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKcombine:
            return createRoxieServerCombineActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKcombinegroup:
            return createRoxieServerCombineGroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKrollupgroup:
            return createRoxieServerRollupGroupActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKlocalresultread:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerLocalResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, graphId);
            }
        case TAKlocalstreamread:
            return createRoxieServerLocalResultStreamReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKlocalresultwrite:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerLocalResultWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), graphId, isRootAction(node));
            }
        case TAKdictionaryresultwrite:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerDictionaryResultWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), graphId, isRootAction(node));
            }
        case TAKloopcount:
        case TAKlooprow:
        case TAKloopdataset:
            {
                unsigned loopId = node.getPropInt("att[@name=\"_loopid\"]/@value", 0);
                return createRoxieServerLoopActivityFactory(id, subgraphId, *this, helperFactory, kind, node, loopId);
            }
        case TAKremotegraph:
            return createRoxieServerRemoteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, remoteId, isRootAction(node));
        case TAKgraphloopresultread:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerGraphLoopResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, graphId);
            }
        case TAKgraphloopresultwrite:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerGraphLoopResultWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, node, usageCount(node), graphId);
            }
        case TAKnwaygraphloopresultread:
            {
                unsigned graphId  = node.getPropInt("att[@name=\"_graphId\"]/@value", 0);
                return createRoxieServerNWayGraphLoopResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, node, graphId);
            }
        case TAKnwayinput:
            return createRoxieServerNWayInputActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnwaymerge:
            return createRoxieServerNWayMergeActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnwaymergejoin:
        case TAKnwayjoin:
            return createRoxieServerNWayMergeJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKsorted:
            return createRoxieServerSortedActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKgraphloop:
        case TAKparallelgraphloop:
            {
                unsigned loopId = node.getPropInt("att[@name=\"_loopid\"]/@value", 0);
                return createRoxieServerGraphLoopActivityFactory(id, subgraphId, *this, helperFactory, kind, node, loopId);
            }
        case TAKlibrarycall:
            {
                LibraryCallFactoryExtra extra;
                extra.maxOutputs = node.getPropInt("att[@name=\"_maxOutputs\"]/@value", 0);
                extra.graphid = node.getPropInt("att[@name=\"_libraryGraphId\"]/@value", 0);
                extra.libraryName.set(node.queryProp("att[@name=\"libname\"]/@value"));
                extra.interfaceHash = node.getPropInt("att[@name=\"_interfaceHash\"]/@value", 0);
                extra.embedded = node.getPropBool("att[@name=\"embedded\"]/@value", false) ;
                if (extra.embedded)
                {
                    extra.embeddedGraphName.set(node.queryProp("att[@name=\"graph\"]/@value"));
                    if (!extra.embeddedGraphName)
                        extra.embeddedGraphName.set(extra.libraryName);
                }

                Owned<IPropertyTreeIterator> iter = node.getElements("att[@name=\"_outputUsed\"]");
                ForEach(*iter)
                    extra.outputs.append(iter->query().getPropInt("@value"));

                return createRoxieServerLibraryCallActivityFactory(id, subgraphId, *this, helperFactory, kind, node, extra);
            }
        case TAKnwayselect:
            return createRoxieServerNWaySelectActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKnonempty:
            return createRoxieServerNonEmptyActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKprefetchproject:
            return createRoxieServerPrefetchProjectActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKwhen_dataset:
            return createRoxieServerWhenActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKwhen_action:
            return createRoxieServerWhenActionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKdistribution:
            return createRoxieServerDistributionActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));
        case TAKexternalprocess:
        case TAKexternalsink:
        case TAKexternalsource:
            return createRoxieServerExternalActivityFactory(id, subgraphId, *this, helperFactory, kind, node, isRootAction(node));

        // These are not required in Roxie for the time being - code generator should trap them
        case TAKchilddataset:

        default:
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Unimplemented activity %s required", getActivityText(kind));
            break;
        }
        throwUnexpected(); // unreachable, but some compilers will complain about missing return
    }

    IActivityFactory *findActivity(unsigned id) const
    {
        if (id)
        {
            IActivityFactory **f = allActivities.getValue(id);
            if (f)
                return *f;
        }
        return NULL;
    }

    virtual IRoxieServerActivityFactory *getRoxieServerActivityFactory(unsigned id) const override
    {
        checkSuspended();
        return LINK(QUERYINTERFACE(findActivity(id), IRoxieServerActivityFactory));
    }

    virtual IAgentActivityFactory *getAgentActivityFactory(unsigned id) const override
    {
        checkSuspended();
        IActivityFactory *f = findActivity(id);
        return LINK(QUERYINTERFACE(f, IAgentActivityFactory)); // MORE - don't dynamic cast yuk
    }

    ActivityArray *loadChildGraph(IPropertyTree &graph)
    {
        // MORE - this is starting to look very much like loadGraph (on Roxie server side)
        ActivityArray *activities = new ActivityArray(true, graph.getPropBool("@delayed"), graph.getPropBool("@library"), graph.getPropBool("@sequential"), graph.getPropBool("@wfid"));
        unsigned subgraphId = graph.getPropInt("@id");
        try
        {
            Owned<IPropertyTreeIterator> nodes = graph.getElements("node");
            ForEach(*nodes)
            {
                IPropertyTree &node = nodes->query();
                loadNode(node, subgraphId, activities);
            }
            Owned<IPropertyTreeIterator> edges = graph.getElements("edge");
            ForEach(*edges)
            {
                IPropertyTree &edge = edges->query();
                unsigned source = activities->findActivityIndex(edge.getPropInt("@source",0));
                unsigned target = activities->findActivityIndex(edge.getPropInt("@target",0));

                unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                unsigned targetInput = edge.getPropInt("att[@name=\"_targetIndex\"]/@value", 0);
                int controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
                if (controlId != 0)
                {
                    const char * edgeId = edge.queryProp("@id");
                    addDependency(sourceOutput, source, target, controlId, edgeId, activities);
                }
                else
                    activities->serverItem(target).setInput(targetInput, source, sourceOutput);
            }
        }
        catch (...)
        {
            ::Release(activities);
            allActivities.kill();
            throw;
        }
        return activities;
    }

    void loadNode(IPropertyTree &node, unsigned subgraphId, ActivityArray *activities)
    {
        ThorActivityKind kind = getActivityKind(node);
        if (kind==TAKsubgraph)
        {
            IPropertyTree * childGraphNode = node.queryPropTree("att/graph");
            if (childGraphNode->getPropBool("@child"))
            {
                loadSubgraph(node, activities);
            }
            else
            {
                unsigned parentId = findParentId(node);
                assertex(parentId);
                unsigned parentIdx = activities->findActivityIndex(parentId);
                IActivityFactory &parentFactory = activities->item(parentIdx);
                ActivityArray *childQuery = loadChildGraph(*childGraphNode);
                parentFactory.addChildQuery(node.getPropInt("@id"), childQuery);
            }
        }
        else if (kind)
        {
            IRoxieServerActivityFactory *f = createActivityFactory(kind, subgraphId, node);
            if (f)
            {
                activities->append(*f);
                allActivities.setValue(f->queryId(), f);
            }
        }
    }

    void loadSubgraph(IPropertyTree &graph, ActivityArray *activities)
    {
        unsigned subgraphId = graph.getPropInt("@id");
        Owned<IPropertyTreeIterator> nodes = graph.getElements("att/graph/node");
        ForEach(*nodes)
        {
            IPropertyTree &node = nodes->query();
            loadNode(node, subgraphId, activities);
        }
        if (!isSuspended)
        {
            Owned<IPropertyTreeIterator> edges = graph.getElements("att/graph/edge");
            ForEach(*edges)
            {
                IPropertyTree &edge = edges->query();
                //Ignore edges that represent dependencies from parent activities to child activities.
                if (edge.getPropInt("att[@name=\"_childGraph\"]/@value", 0))
                    continue;

                unsigned sourceActivity = edge.getPropInt("@source", 0);
                unsigned targetActivity = edge.getPropInt("@target", 0);
                unsigned source = activities->findActivityIndex(sourceActivity);
                unsigned target = activities->recursiveFindActivityIndex(targetActivity);

                unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                unsigned targetInput = edge.getPropInt("att[@name=\"_targetIndex\"]/@value", 0);

                int controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
                if (controlId != 0)
                {
                    const char * edgeId = edge.queryProp("@id");
                    addDependency(sourceOutput, sourceActivity, targetActivity, controlId, edgeId, activities);
                }
                else
                    activities->serverItem(target).setInput(targetInput, source, sourceOutput);
            }
        }
    }

    // loadGraph loads outer level graph. This is virtual as agent is very different from Roxie server
    virtual ActivityArray *loadGraph(IPropertyTree &graph, const char *graphName) = 0;

    bool doAddDependency(unsigned sourceIdx, unsigned sourceId, unsigned targetId, int controlId, const char *edgeId, ActivityArray * activities)
    {
        // Note - the dependency is recorded with the target being the parent activity that is at the same level as the source
        // (recording it on the child that was actually dependent would mean it happened too late)
        unsigned source = activities->findActivityIndex(sourceId);
        if (source != NotFound)
        {
            unsigned target = activities->recursiveFindActivityIndex(targetId);
            activities->serverItem(target).addDependency(source, activities->serverItem(source).getKind(), sourceIdx, controlId, edgeId);
            activities->serverItem(source).noteDependent(target);
            return true;
        }

        ForEachItemIn(idx, *activities)
        {
            IActivityFactory & cur = activities->item(idx);
            unsigned childId;
            for (unsigned childIdx = 0;;childIdx++)
            {
                ActivityArray * children = cur.queryChildQuery(childIdx, childId);
                if (!children)
                    break;
                if (doAddDependency(sourceIdx, sourceId, targetId, controlId, edgeId, children))
                    return true;
            }
        }
        return false;
    }

    virtual void addDependency(unsigned sourceIdx, unsigned sourceId, unsigned targetId, int controlId, const char *edgeId, ActivityArray * activities)
    {
        doAddDependency(sourceIdx, sourceId, targetId, controlId, edgeId, activities);
    }

    void addDependencies(IPropertyTree &graph, ActivityArray *activities)
    {
        Owned<IPropertyTreeIterator> dependencies = graph.getElements("edge");
        ForEach(*dependencies)
        {
            IPropertyTree &edge = dependencies->query();
            //Ignore edges that represent dependencies from parent activities to child activities.
            if (!edge.getPropInt("att[@name=\"_childGraph\"]/@value", 0))
            {
                unsigned sourceIdx = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                int controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
                addDependency(sourceIdx, edge.getPropInt("att[@name=\"_sourceActivity\"]/@value", 0), edge.getPropInt("att[@name=\"_targetActivity\"]/@value", 0), controlId, edge.queryProp("@id"), activities);
            }
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    unsigned channelNo;

    CQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue, unsigned _channelNo, ISharedOnceContext *_sharedOnceContext, bool _dynamic)
        : package(_package), dll(_dll), sharedOnceContext(_sharedOnceContext), id(_id), dynamic(_dynamic), hashValue(_hashValue), channelNo(_channelNo)
    {
        package.Link();
        targetClusterType = RoxieCluster;
        isSuspended = false;
        isLoadFailed = false;
        libraryInterfaceHash = 0;
        options.enableFieldTranslation = package.getEnableFieldTranslation();  // NOTE - can be overridden by wu settings
        options.allSortsMaySpill = dynamic;
        addToCache();
    }

    ~CQueryFactory()
    {
        HashIterator graphs(graphMap);
        for(graphs.first();graphs.isValid();graphs.next())
        {
            ActivityArray *a = *graphMap.mapToValue(&graphs.query());
            a->Release();
        }
        package.Release();
    }

private:
    std::once_flag started;
    Owned<IException> e;
public:
    void init(const IPropertyTree *stateInfo)
    {
        std::call_once(started, [this, stateInfo]()
        {
            try
            {
                load(stateInfo);
                if (sharedOnceContext && preloadOnceData)
                {
                    Owned<StringContextLogger> logctx = new StringContextLogger(id); // NB may get linked by the onceContext
                    sharedOnceContext->checkOnceDone(this, *logctx);
                }
                addToMap();  // Publishes for agents to see
            }
            catch (IException *E)
            {
                EXCLOG(E);
                e.setown(E);
            }
        });
        if (e)
            throw e.getLink();
    }

    virtual IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const override
    {
        return globalPackageSetManager->lookupLibrary(libraryName, expectedInterfaceHash, logctx);
    }

    virtual void beforeDispose() override
    {
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table. 
        // So only remove from hash table if what we find there matches the item that is being deleted.
        hash64_t hv = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
        {
            CriticalBlock b(queryCacheCrit);
            CQueryFactory *goer = queryCache.getValue(hv);
            if (goer == this)
                queryCache.remove(hv);
        }
        {
            CriticalBlock b(activeQueriesCrit);
            CQueryFactory *goer = activeQueries.getValue(hv);
            if (goer == this)
                activeQueries.remove(hv);
        }
    }

    // There are two very similar-looking maps of queries - they have slightly different lifetimes and characteristics
    // One has fully-constructed queries suitable for use responding to a agent request.
    // The other has potentially partially-constructed queries, and is used for ensuring we only build them once
    // while allowing for parallelizing package loads.

    static CQueryFactory *getQueryFactory(hash64_t hashValue, unsigned channelNo)
    {
        hash64_t hv = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
        CriticalBlock b(activeQueriesCrit);
        CQueryFactory *factory = activeQueries.getValue(hv);
        if (factory && factory->isAliveAndLink())
            return factory;
        else
            return NULL;
    }

    void addToMap()
    {
        if (traceRoxiePackets)
            DBGLOG("addToMap %s: hashvalue = %" I64F "x channel %d", id.str(), hashValue, channelNo);
        hash64_t hv = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
        CriticalBlock b(activeQueriesCrit);
        activeQueries.setValue(hv, this);
    }

    static CQueryFactory *getCachedQuery(hash64_t hashValue, unsigned channelNo)
    {
        // NOTE - this must be called within an allQueriesCrit block
        queryCacheCrit.assertLocked();
        hash64_t hv = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
        CQueryFactory *factory = queryCache.getValue(hv);
        if (factory && factory->isAliveAndLink())
            return factory;
        else
            return nullptr;
    }

    void addToCache()
    {
        // NOTE - this must be called within an allQueriesCrit block
        queryCacheCrit.assertLocked();
        hash64_t hv = rtlHash64Data(sizeof(channelNo), &channelNo, hashValue);
        queryCache.setValue(hv, this);
    }

static hash64_t getQueryHash(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, IArrayOf<IResolvedFile> &files, bool isDynamic)
    {
        hash64_t hashValue = package.queryHash();
        if (traceLevel > 8)
            DBGLOG("getQueryHash: %s %" I64F "u from package", id, hashValue);
        if (dll)
        {
            hashValue = rtlHash64VStr(dll->queryDll()->queryName(), hashValue);
            if (traceLevel > 8)
                DBGLOG("getQueryHash: %s %" I64F "u from dll", id, hashValue);
            if (!lockSuperFiles && !allFilesDynamic && !isDynamic && !package.isCompulsory())
            {
                IConstWorkUnit *wu = dll->queryWorkUnit();
                if (wu) // wu may be null in some unit test cases
                {
                    SCMStringBuffer bStr;
                    // Don't want to include files referenced in thor graphs... in practice isDynamic also likely to be set in such cases
                    if (getClusterType(wu->getDebugValue("targetClusterType", bStr).str(), RoxieCluster) == RoxieCluster)
                    {
                        Owned<IConstWUGraphIterator> graphs = &wu->getGraphs(GraphTypeActivities);
                        ForEach(*graphs)
                        {
                            Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
                            Owned<IPropertyTreeIterator> nodes = graphXgmml->getElements(".//node");
                            ForEach(*nodes)
                            {
                                IPropertyTree &node = nodes->query();
                                ThorActivityKind kind = getActivityKind(node);
                                if (kind != TAKdiskwrite && kind != TAKspillwrite && kind != TAKindexwrite && kind != TAKpiperead && kind != TAKpipewrite)
                                {
                                    const char *fileName = queryNodeFileName(node, kind);
                                    const char *indexName = queryNodeIndexName(node, kind);
                                    // What about packages that resolve everything without dali?
                                    if (indexName)
                                    {
                                        bool isOpt = pretendAllOpt || node.getPropBool("att[@name='_isIndexOpt']/@value");
                                        bool isCodeSigned = isActivityCodeSigned(node);
                                        const IResolvedFile *indexFile = package.lookupFileName(indexName, isOpt, true, true, wu, true, isCodeSigned);
                                        if (indexFile)
                                        {
                                            hashValue = indexFile->addHash64(hashValue);
                                            if (traceLevel > 8)
                                                DBGLOG("getQueryHash: %s %" I64F "u from index %s", id, hashValue, indexName);
                                            files.append(*const_cast<IResolvedFile *>(indexFile));
                                        }
                                    }
                                    if (fileName)
                                    {
                                        if (!node.getPropBool("att[@name='_isSpill']/@value") && !node.getPropBool("att[@name='_isSpillGlobal']/@value"))
                                        {
                                            bool isCodeSigned = isActivityCodeSigned(node);
                                            bool isOpt = pretendAllOpt || node.getPropBool("att[@name='_isOpt']/@value");
                                            const IResolvedFile *dataFile = package.lookupFileName(fileName, isOpt, true, true, wu, true, isCodeSigned);
                                            if (dataFile)
                                            {
                                                hashValue = dataFile->addHash64(hashValue);
                                                if (traceLevel > 8)
                                                    DBGLOG("getQueryHash: %s %" I64F "u from index %s", id, hashValue, fileName);
                                                files.append(*const_cast<IResolvedFile *>(dataFile));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if (id)
            hashValue = rtlHash64VStr(id, hashValue);
        hashValue = rtlHash64VStr("Roxie", hashValue);  // Adds some noise into the hash - otherwise adjacent wuids tend to hash very close together
        if (traceLevel > 8)
            DBGLOG("getQueryHash: %s %" I64F "u from id", id, hashValue);
        if (stateInfo)
        {
            StringBuffer xml;
            toXML(stateInfo, xml);
            hashValue = rtlHash64Data(xml.length(), xml.str(), hashValue);
            if (traceLevel > 8)
                DBGLOG("getQueryHash: %s %" I64F "u from stateInfo", id, hashValue);
        }
        if (traceLevel > 8)
            DBGLOG("getQueryHash: %s %" I64F "u", id, hashValue);
        return hashValue;
    }
    
    virtual void load(const IPropertyTree *stateInfo)
    {
        if (!dll)
            return;
        IConstWorkUnit *wu = dll->queryWorkUnit();
        if (wu) // wu and dll may be null in some unit test cases
        {
            libraryInterfaceHash = wu->getApplicationValueInt("LibraryModule", "interfaceHash", 0);

            options.setFromWorkUnit(*wu, stateInfo);
            SCMStringBuffer bStr;
            targetClusterType = getClusterType(wu->getDebugValue("targetClusterType", bStr).str(), RoxieCluster);

            // NOTE: stateinfo overrides package info

            if (stateInfo)
            {
                // info in querySets can override the defaults from workunit for some limits
                isSuspended = stateInfo->getPropBool("@suspended", false);
            }
            if (targetClusterType == RoxieCluster)
            {
                Owned<IConstWUGraphIterator> graphs = &wu->getGraphs(GraphTypeActivities);
                SCMStringBuffer graphNameStr;
                ForEach(*graphs)
                {
                    graphs->query().getName(graphNameStr);
                    const char *graphName = graphNameStr.s.str();
                    Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
                    try
                    {
                        ActivityArray *activities = loadGraph(*graphXgmml, graphName);
                        graphMap.setValue(graphName, activities);
                    }
                    catch (IException *E)
                    {
                        StringBuffer m;
                        E->errorMessage(m);
                        suspend(m.str());
                        OERRLOG("Query %s suspended: %s", id.get(), m.str());
                        E->Release();
                    }
                }
            }
        }
    }

    virtual unsigned queryChannel() const override
    {
        return channelNo;
    }

    virtual hash64_t queryHash() const override
    {
        return hashValue;
    }

    virtual ISharedOnceContext *querySharedOnceContext() const override
    {
        return sharedOnceContext;
    }

    virtual IDeserializedResultStore &queryOnceResultStore() const override
    {
        assertex(sharedOnceContext);
        return sharedOnceContext->queryOnceResultStore();
    }

    virtual IPropertyTree &queryOnceContext(const IRoxieContextLogger &logctx) const override
    {
        assertex(sharedOnceContext);
        return sharedOnceContext->queryOnceContext(this, logctx);
    }

    virtual const char *loadResource(unsigned id)
    {
        return (const char *) queryDll()->getResource(id);
    }

    virtual ActivityArray *lookupGraphActivities(const char *name) const override
    {
        return *graphMap.getValue(name);
    }

    virtual IActivityGraph *lookupGraph(IRoxieAgentContext *ctx, const char *name, IProbeManager *probeManager, const IRoxieContextLogger &logctx, IRoxieServerActivity *parentActivity) const override
    {
        assertex(name && *name);
        ActivityArrayPtr *graph = graphMap.getValue(name);
        assertex(graph);
        Owned<IActivityGraph> ret = ::createActivityGraph(ctx, name, 0, **graph, parentActivity, probeManager, logctx, 1);
        return ret.getClear();
    }

    void getGraphStats(StringBuffer &reply, const IPropertyTree &thisGraph) const
    {
        Owned<IPropertyTree> graph = createPTreeFromIPT(&thisGraph, ipt_lowmem);
        Owned<IPropertyTreeIterator> edges = graph->getElements(".//edge");
        ForEach(*edges)
        {
            IPropertyTree &edge = edges->query();
            IActivityFactory *a = findActivity(edge.getPropInt("@source", 0));
            if (!a)
                a = findActivity(edge.getPropInt("att[@name=\"_sourceActivity\"]/@value", 0));
            if (a)
            {
                unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                a->getEdgeProgressInfo(sourceOutput, edge);
            }
        }
        Owned<IPropertyTreeIterator> nodes = graph->getElements(".//node");
        ForEach(*nodes)
        {
            IPropertyTree &node = nodes->query();
            IActivityFactory *a = findActivity(node.getPropInt("@id", 0));
            if (a)
                a->getNodeProgressInfo(node);
        }
        toXML(graph, reply);
    }

    virtual IPropertyTree* cloneQueryXGMML() const override
    {
        assertex(dll && dll->queryWorkUnit());
        Owned<IPropertyTree> tree = createPTree("Query", ipt_lowmem);
        Owned<IConstWUGraphIterator> graphs = &dll->queryWorkUnit()->getGraphs(GraphTypeActivities);
        SCMStringBuffer graphNameStr;
        ForEach(*graphs)
        {
            graphs->query().getName(graphNameStr);
            const char *graphName = graphNameStr.s.str();
            Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
            IPropertyTree *newGraph = tree->addPropTree("Graph");
            newGraph->setProp("@id", graphName);
            newGraph->addPropTree("xgmml")->addPropTree("graph", graphXgmml.getLink());
        }
        return tree.getClear();
    }

    virtual void getStats(StringBuffer &reply, const char *graphName) const override
    {
        if (dll)
        {
            assertex(dll->queryWorkUnit());
            Owned<IConstWUGraphIterator> graphs = &dll->queryWorkUnit()->getGraphs(GraphTypeActivities);
            SCMStringBuffer thisGraphNameStr;
            ForEach(*graphs)
            {
                graphs->query().getName(thisGraphNameStr);
                if (graphName)
                {
                    if (thisGraphNameStr.length() && (stricmp(graphName, thisGraphNameStr.s.str()) != 0))
                        continue; // not interested in this one
                }
                reply.appendf("<Graph id='%s'><xgmml>", thisGraphNameStr.s.str());
                Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
                getGraphStats(reply, *graphXgmml);
                reply.append("</xgmml></Graph>");
            }
        }
    }
    virtual void getActivityMetrics(StringBuffer &reply) const override
    {
        HashIterator i(allActivities);
        StringBuffer myReply;
        ForEach(i)
        {
            IActivityFactory *f = *allActivities.mapToValue(&i.query());
            f->getActivityMetrics(myReply.clear());
            if (myReply.length())
            {
                reply.appendf("  <activity query='%s' id='%d' channel='%d'\n", queryQueryName(), f->queryId(), queryChannel());
                reply.append(myReply);
                reply.append("  </activity>\n");
            }
        }
    }
    virtual void getQueryInfo(StringBuffer &reply, bool full, IArrayOf<IQueryFactory> *agentQueries, const IRoxieContextLogger &logctx) const override
    {
        Owned<IPropertyTree> xref = createPTree("Query", ipt_fast);
        xref->setProp("@id", id);
        if (suspended())
        {
            xref->setPropBool("@suspended", true);
            xref->setProp("@error", errorMessage);
        }
        if (full)
        {
            HashIterator i(allActivities);
            ForEach(i)
            {
                IActivityFactory *f = *allActivities.mapToValue(&i.query());
                f->getXrefInfo(*xref, logctx);
            }
        }
        if (agentQueries)
        {
            ForEachItemIn(idx, *agentQueries)
            {
                if (agentQueries->item(idx).suspended())
                {
                    xref->setPropBool("@suspended", true);
                    xref->setPropBool("@agentSuspended", true);
                    xref->setPropBool("@slaveSuspended", true);  // legacy name
                }
            }
        }
        toXML(xref, reply, 1, XML_Embed|XML_LineBreak|XML_SortTags);
    }
    virtual void resetQueryTimings() override
    {
        HashIterator i(allActivities);
        ForEach(i)
        {
            IActivityFactory *f = *allActivities.mapToValue(&i.query());
            f->resetNodeProgressInfo();
        }
    }
    virtual const char *queryErrorMessage() const override
    {
        return errorMessage.str();
    }
    virtual const char *queryQueryName() const override
    {
        return id;
    }
    virtual bool isQueryLibrary() const override
    {
        return libraryInterfaceHash != 0; 
    }
    virtual unsigned getQueryLibraryInterfaceHash() const override
    {
        return libraryInterfaceHash;
    }
    virtual void suspend(const char* errMsg) override
    {
        isSuspended = true;
        isLoadFailed = true;
        errorMessage.append(errMsg);
    }

    virtual bool loadFailed() const override
    {
        return isLoadFailed;
    }
    virtual bool suspended() const override
    {
        return isSuspended;
    }
    virtual const QueryOptions &queryOptions() const override
    {
        return options;
    }
    virtual ILoadedDllEntry *queryDll() const override
    {
        assertex(dll);
        return dll->queryDll();
    }
    virtual IConstWorkUnit *queryWorkUnit() const override
    {
        assertex(dll);
        return dll->queryWorkUnit();
    }
    virtual const IRoxiePackage &queryPackage() const override
    {
        return package;
    }
    virtual CRoxieWorkflowMachine *createWorkflowMachine(IConstWorkUnit *wu, bool isOnce, const IRoxieContextLogger &logctx, const QueryOptions & options) const override
    {
        throwUnexpected();  // only on server...
    }
    virtual char *getEnv(const char *name, const char *defaultValue) const
    {
        if (!defaultValue)
            defaultValue = "";
        const char *result;
        if (name && *name=='@')
        {
            // @ is shorthand for control: for legacy compatibility reasons
            StringBuffer useName;
            useName.append("control:").append(name+1);
            result = package.queryEnv(useName.str());
        }
        else
            result = package.queryEnv(name);
        if (!result && name)
            result = getenv(name);
        return strdup(result ? result : defaultValue);
    }

    virtual IRoxieAgentContext *createAgentContext(const AgentContextLogger &logctx, IRoxieQueryPacket *packet, bool hasChildren) const override
    {
        throwUnexpected();   // only implemented in derived agent class
    }

    virtual IRoxieServerContext *createContext(IPropertyTree *xml, IHpccProtocolResponse *protocol, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions xmlReadFlags, const char *querySetName) const override
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const ContextLogger &_logctx) const override
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned agentsReplyLen, unsigned bytesOut) override
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IPropertyTree *getQueryStats(time_t from, time_t to) override
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual void getGraphNames(StringArray &ret) const override
    {
        Owned<IConstWUGraphIterator> graphs = &dll->queryWorkUnit()->getGraphs(GraphTypeActivities);
        ForEach(*graphs)
        {
            SCMStringBuffer graphName;
            graphs->query().getName(graphName);
            ret.append(graphName.str());
        }
    }

    virtual bool isDynamic() const override
    {
        return dynamic;
    }

protected:
    IPropertyTree *queryWorkflowTree() const
    {
        assertex(dll->queryWorkUnit());
        return dll->queryWorkUnit()->queryWorkflowTree();
    }

    bool hasOnceSection() const
    {
        IPropertyTree *workflow = queryWorkflowTree();
        if (workflow)
            return workflow->hasProp("Item[@mode='once']");
        else
            return false;
    }

    virtual void checkSuspended() const override
    {
        if (isSuspended)
        {
            StringBuffer err;
            if (errorMessage.length())
                err.appendf(" because %s", errorMessage.str());
            throw MakeStringException(ROXIE_QUERY_SUSPENDED, "Query %s is suspended%s", id.get(), err.str());
        }
    }

    virtual void onTermination(TerminationCallbackInfo *info) const override
    {
        CriticalBlock b(callbacksCrit);
        callbacks.append(*info);
    }
};

CriticalSection CQueryFactory::activeQueriesCrit;
CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> CQueryFactory::activeQueries;     // Used to map hashes in packets to query factories

CriticalSection CQueryFactory::queryCacheCrit;
CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> CQueryFactory::queryCache;   // Used to ensure a given query is only created once

extern IQueryFactory *getQueryFactory(hash64_t hashvalue, unsigned channel)
{
    return CQueryFactory::getQueryFactory(hashvalue, channel);
}

class CRoxieServerQueryFactory : public CQueryFactory
{
    // Parts of query factory is only interesting on the server - workflow support, and tracking of total query times

protected:
    Owned<IQueryStatsAggregator> queryStats;
public:
    CRoxieServerQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue, ISharedOnceContext *_sharedOnceContext, bool _dynamic)
        : CQueryFactory(_id, _dll, _package, _hashValue, 0, _sharedOnceContext, _dynamic)
    {
        queryStats.setown(createQueryStatsAggregator(id.get(), statsExpiryTime));
    }
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned agentsReplyLen, unsigned bytesOut)
    {
        queryStats->noteQuery(startTime, failed, elapsed, memused, agentsReplyLen, bytesOut);
        queryGlobalQueryStatsAggregator()->noteQuery(startTime, failed, elapsed, memused, agentsReplyLen, bytesOut);
    }

    virtual void addDependency(unsigned sourceIdx, unsigned sourceId, unsigned targetId, int controlId, const char *edgeId, ActivityArray * activities) override
    {
        // addDependency is expected to fail occasionally on agent, but never on Roxie server
        if (!doAddDependency(sourceIdx, sourceId, targetId, controlId, edgeId, activities))
            throw MakeStringException(ROXIE_ADDDEPENDENCY_ERROR, "Failed to create dependency from %u on %u", sourceId, targetId);
    }

    virtual ActivityArray *loadGraph(IPropertyTree &graph, const char *graphName)
    {
        bool isLibraryGraph = graph.getPropBool("@library");
        bool isSequential = graph.getPropBool("@sequential");
        unsigned wfid = graph.getPropInt("@wfid");
        ActivityArray *activities = new ActivityArray(isLibraryGraph, false, isLibraryGraph, isSequential, wfid);
        if (isLibraryGraph)
            activities->setLibraryGraphId(graph.getPropInt("node/@id"));
        try
        {
            Owned<IPropertyTreeIterator> subgraphs = graph.getElements("node");
            ForEach(*subgraphs)
            {
                IPropertyTree &node = subgraphs->query();
                loadSubgraph(node, activities);
                loadNode(node, 0, activities);
            }
            addDependencies(graph, activities);
        }
        catch (...)
        {
            ::Release(activities);
            allActivities.kill();
            throw;
        }
        return activities;
    }

    virtual IRoxieServerContext *createContext(IPropertyTree *context, IHpccProtocolResponse *protocol, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions _xmlReadFlags, const char *_querySetName) const
    {
        checkSuspended();
        return createRoxieServerContext(context, protocol, this, flags, _logctx, _xmlReadFlags, _querySetName);
    }

    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const ContextLogger &_logctx) const
    {
        checkSuspended();
        return createWorkUnitServerContext(wu, this, _logctx);
    }

    virtual CRoxieWorkflowMachine *createWorkflowMachine(IConstWorkUnit *wu, bool isOnce, const IRoxieContextLogger &logctx, const QueryOptions & options) const
    {
        IPropertyTree *workflow = queryWorkflowTree();
        if (workflow)
        {
            return ::createRoxieWorkflowMachine(workflow, wu, isOnce, options.parallelWorkflow, options.numWorkflowThreads, logctx);
        }
        else
            return NULL;
    }

    virtual IPropertyTree *getQueryStats(time_t from, time_t to)
    {
        return queryStats->getStats(from, to);
    }
};

unsigned checkWorkunitVersionConsistency(const IConstWorkUnit *wu)
{
    assertex(wu);
    unsigned wuVersion = wu->getCodeVersion();
    if (wuVersion == 0)
        throw makeStringException(ROXIE_MISMATCH, "Attempting to execute a workunit that hasn't been compiled");
    if (wuVersion > ACTIVITY_INTERFACE_VERSION || wuVersion < MIN_ACTIVITY_INTERFACE_VERSION)
        throw MakeStringException(ROXIE_MISMATCH, "Workunit was compiled for eclhelper interface version %d, this roxie requires version %d..%d", wuVersion, MIN_ACTIVITY_INTERFACE_VERSION, ACTIVITY_INTERFACE_VERSION);
    if (wuVersion == 652)
    {
        // Any workunit compiled using eclcc 7.12.0-7.12.18 is not compatible
        StringBuffer buildVersion, eclVersion;
        wu->getBuildVersion(StringBufferAdaptor(buildVersion), StringBufferAdaptor(eclVersion));
        const char *version = strstr(buildVersion, "7.12.");

        //Avoid matching a version number in the path that was used to build (enclosed in [] at the end)
        const char *extra = strchr(buildVersion, '[');
        if (version && (!extra || version < extra))
        {
            const char *point = version + strlen("7.12.");
            unsigned pointVer = atoi(point);
            if (pointVer <= 18)
                throw MakeStringException(ROXIE_MISMATCH, "Workunit was compiled by eclcc version %s which is not compatible with this runtime", buildVersion.str());
        }
    }
    return wuVersion;
}

static void checkWorkunitVersionConsistency(const IQueryDll *dll)
{
    unsigned wuVersion = checkWorkunitVersionConsistency(dll->queryWorkUnit());

    EclProcessFactory processFactory = (EclProcessFactory) dll->queryDll()->getEntry("createProcess");
    if (processFactory)
    {
        Owned<IEclProcess> process = processFactory();
        assertex(process);
        if (process->getActivityVersion() != wuVersion)
            throw MakeStringException(ROXIE_MISMATCH, "Inconsistent interface versions.  Workunit was created using eclcc for version %u, but the c++ compiler used version %u", wuVersion, process->getActivityVersion());
    }
    else
        throw MakeStringException(ROXIE_MISMATCH, "Workunit did not export createProcess function");
}

extern IQueryFactory *createServerQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, bool isDynamic, bool forceRetry)
{
    IArrayOf<IResolvedFile> queryFiles; // Note - these should stay in scope long enough to ensure still cached when (if) query is loaded for real
    hash64_t hashValue = CQueryFactory::getQueryHash(id, dll, package, stateInfo, queryFiles, isDynamic);
    Owned<CQueryFactory> ret;
    {
        CriticalBlock b(CQueryFactory::queryCacheCrit);
        ret.setown(CQueryFactory::getCachedQuery(hashValue, 0));
        if (ret && ret->loadFailed() && (reloadRetriesFailed || forceRetry))  // MORE - is there a race on loadFailed?
        {
            ret.clear();
        }
		if (ret)
		    ::Release(dll);
        else
        {
            if (dll && !selfTestMode)
            {
                checkWorkunitVersionConsistency(dll);
                Owned<ISharedOnceContext> sharedOnceContext;
                IPropertyTree *workflow = dll->queryWorkUnit()->queryWorkflowTree();
                if (workflow && workflow->hasProp("Item[@mode='once']"))
                    sharedOnceContext.setown(new CSharedOnceContext);
                ret.setown(new CRoxieServerQueryFactory(id, dll, package, hashValue, sharedOnceContext, isDynamic));
            }
            else
                ret.setown(new CRoxieServerQueryFactory(id, NULL, package, hashValue, NULL, isDynamic));
        }
    }
    ret->init(stateInfo);
    return ret.getClear();
}

extern IQueryFactory *createServerQueryFactoryFromWu(IConstWorkUnit *wu, const IQueryDll *_dll)
{
    Linked<const IQueryDll> dll = _dll;
    if (!dll)
        dll.setown(createWuQueryDll(wu));
    if (!dll)
        return NULL;
    return createServerQueryFactory(wu->queryWuid(), dll.getClear(), queryRootRoxiePackage(), NULL, true, false); // MORE - if use a constant for id might cache better?
}

//==============================================================================================================================================

class CAgentQueryFactory : public CQueryFactory
{
    void addActivity(IAgentActivityFactory *activity, ActivityArray *activities)
    {
        activities->append(*activity);
        unsigned activityId = activity->queryId();
        allActivities.setValue(activityId, activity);
    }

    void loadAgentNode(IPropertyTree &node, unsigned subgraphId, ActivityArray *activities)
    {
        ThorActivityKind kind = getActivityKind(node);
        switch (kind)
        {
        case TAKcsvread:
        case TAKxmlread:
        case TAKdiskread:
        case TAKjsonread:
            if (node.getPropBool("att[@name='_isSpill']/@value", false) || node.getPropBool("att[@name='_isSpillGlobal']/@value", false))
                return;
            break;
        case TAKkeyedjoin:
        case TAKkeyeddenormalize:
        case TAKkeyeddenormalizegroup:
        case TAKdisknormalize:
        case TAKdiskcount:
        case TAKdiskaggregate:
        case TAKdiskgroupaggregate:
        case TAKindexread:
        case TAKindexnormalize:
        case TAKindexcount:
        case TAKindexaggregate:
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
        case TAKfetch:
        case TAKcsvfetch:
        case TAKxmlfetch:
        case TAKjsonfetch:
        case TAKremotegraph:
            break;
        case TAKsubgraph:
            break;
        default:
            return;
        }
        IAgentActivityFactory *newAct = NULL;
        if (kind != TAKsubgraph)
        {
            if (isSuspended)
                newAct = createRoxieDummyActivityFactory(node, subgraphId, *this, false); // MORE - is there any point?
            else
            {
                StringBuffer helperName;
                node.getProp("att[@name=\"helper\"]/@value", helperName);
                if (!helperName.length())
                    helperName.append("fAc").append(node.getPropInt("@id", 0));
                HelperFactory *helperFactory = dll->getFactory(helperName.str());
                if (!helperFactory)
                    throw MakeStringException(ROXIE_INTERNAL_ERROR, "Internal error: helper function %s not exported", helperName.str());
                switch (kind)
                {
                case TAKdiskread:
                    newAct = createRoxieDiskReadActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKcsvread:
                    newAct = createRoxieCsvReadActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKxmlread:
                case TAKjsonread:
                    newAct = createRoxieXmlReadActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKdisknormalize:
                    newAct = createRoxieDiskNormalizeActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKdiskcount:
                    newAct = createRoxieDiskCountActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKdiskaggregate:
                    newAct = createRoxieDiskAggregateActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKdiskgroupaggregate:
                    newAct = createRoxieDiskGroupAggregateActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKindexread:
                    newAct = createRoxieIndexReadActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKindexnormalize:
                    newAct = createRoxieIndexNormalizeActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKindexcount:
                    newAct = createRoxieIndexCountActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKindexaggregate:
                    newAct = createRoxieIndexAggregateActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKindexgroupaggregate:
                case TAKindexgroupexists:
                case TAKindexgroupcount:
                    newAct = createRoxieIndexGroupAggregateActivityFactory(node, subgraphId, *this, helperFactory, kind);
                    break;
                case TAKfetch:
                    newAct = createRoxieFetchActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKcsvfetch:
                    newAct = createRoxieCSVFetchActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKxmlfetch:
                case TAKjsonfetch:
                    newAct = createRoxieXMLFetchActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKkeyedjoin:
                case TAKkeyeddenormalize:
                case TAKkeyeddenormalizegroup:
                    newAct = createRoxieKeyedJoinIndexActivityFactory(node, subgraphId, *this, helperFactory);
                    if (node.getPropBool("att[@name=\"_diskAccessRequired\"]/@value"))
                    {
                        IAgentActivityFactory *newAct2 = createRoxieKeyedJoinFetchActivityFactory(node, subgraphId, *this, helperFactory);
                        unsigned activityId2 = newAct2->queryId() | ROXIE_ACTIVITY_FETCH;
                        activities->append(*newAct2);
                        allActivities.setValue(activityId2, newAct2);
                    }
                    break;
                case TAKremotegraph:
                    {
                        unsigned graphId = node.getPropInt("att[@name=\"_remoteSubGraph\"]/@value", 0);
                        newAct = createRoxieRemoteActivityFactory(node, subgraphId, *this, helperFactory, graphId);
                        break;
                    }
                default:
                    throwUnexpected();
                }
            }
            if (newAct)
            {
                addActivity(newAct, activities);
            }
        }
        else if (kind == TAKsubgraph)
        {
            // If the subgraph belongs to a remote activity, we need to be able to execute it on the agent...
            IPropertyTree * childGraphNode = node.queryPropTree("att/graph");
            if (!childGraphNode->getPropBool("@child"))
            {
                unsigned parentId = findParentId(node);
                assertex(parentId);
                unsigned parentIndex = activities->findActivityIndex(parentId);
                if (parentIndex != NotFound)
                {
                    ActivityArray *childQuery = loadChildGraph(*childGraphNode);
                    activities->item(parentIndex).addChildQuery(node.getPropInt("@id"), childQuery);
                }
            }
            // Regardless, we need to make sure we create remote activities as required throughout the graph
            Owned<IPropertyTreeIterator> nodes = node.getElements("att/graph/node");
            unsigned subgraphId = node.getPropInt("@id");
            ForEach(*nodes)
            {
                IPropertyTree &node = nodes->query();
                loadAgentNode(node, subgraphId, activities);
            }
        }
    }

    void loadOuterSubgraph(IPropertyTree &graph, ActivityArray *activities)
    {
        Owned<IPropertyTreeIterator> nodes = graph.getElements("att/graph/node");
        unsigned subgraphId = graph.getPropInt("@id");
        ForEach(*nodes)
        {
            IPropertyTree &node = nodes->query();
            loadAgentNode(node, subgraphId, activities);
        }
        loadAgentNode(graph, subgraphId, activities); // MORE - not really sure why this line is here!
    }

public:
    CAgentQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue, unsigned _channelNo, ISharedOnceContext *_sharedOnceContext, bool _dynamic)
        : CQueryFactory(_id, _dll, _package, _hashValue, _channelNo, _sharedOnceContext, _dynamic)
    {
    }

    virtual IRoxieAgentContext *createAgentContext(const AgentContextLogger &logctx, IRoxieQueryPacket *packet, bool hasChildren) const
    {
        return ::createAgentContext(this, logctx, packet, hasChildren);
    }

    virtual ActivityArray *loadGraph(IPropertyTree &graph, const char *graphName)
    {
        // MORE: common up with loadGraph for the Roxie server..
        bool isLibraryGraph = graph.getPropBool("@library");
        bool isSequential = graph.getPropBool("@sequential");
        unsigned wfid = graph.getPropInt("@wfid");
        ActivityArray *activities = new ActivityArray(isLibraryGraph, false, isLibraryGraph, isSequential, wfid);
        if (isLibraryGraph)
            activities->setLibraryGraphId(graph.getPropInt("node/@id"));
        try
        {
            if (false && isLibraryGraph)
            {
                //Really only need to do this if the library is called from a remote activity
                //but it's a bit tricky to work out since the library graph will come before the use.
                //Not a major issue since libraries won't be embedded for production queries.
                // this comment makes little sense...
                Owned<IPropertyTreeIterator> subgraphs = graph.getElements("node");
                ForEach(*subgraphs)
                {
                    IPropertyTree &node = subgraphs->query();
                    loadSubgraph(node, activities);
                    loadNode(node, 0, activities);
                }
            }
            Owned<IPropertyTreeIterator> subgraphs = graph.getElements("node");
            ForEach(*subgraphs)
            {
                IPropertyTree &subgraph = subgraphs->query();
                loadOuterSubgraph(subgraph, activities);
            }
            addDependencies(graph, activities);
        }
        catch (...)
        {
            ::Release(activities);
            allActivities.kill();
            throw;
        }
        return activities;
    }
};

IQueryFactory *createAgentQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, unsigned channel, const IPropertyTree *stateInfo, bool isDynamic, bool forceRetry)
{
    IArrayOf<IResolvedFile> queryFiles; // Note - these should stay in scope long enough to ensure still cached when (if) query is loaded for real
    Owned<CQueryFactory> ret;
    hash64_t hashValue = CQueryFactory::getQueryHash(id, dll, package, stateInfo, queryFiles, isDynamic);
    {
        CriticalBlock b(CQueryFactory::queryCacheCrit);
        ret.setown(CQueryFactory::getCachedQuery(hashValue, channel));
        if (ret)
        {
            ::Release(dll);
        }
        else if (dll)
        {
            checkWorkunitVersionConsistency(dll);
            Owned<IQueryFactory> serverFactory = CQueryFactory::getCachedQuery(hashValue, 0);
            assertex(serverFactory);
            ret.setown(new CAgentQueryFactory(id, dll, package, hashValue, channel, serverFactory->querySharedOnceContext(), isDynamic));
        }
        else
            ret.setown(new CAgentQueryFactory(id, NULL, package, hashValue, channel, NULL, isDynamic));
    }
    ret->init(stateInfo);
    return ret.getClear();
}

extern IQueryFactory *createAgentQueryFactoryFromWu(IConstWorkUnit *wu, unsigned channelNo)
{
    Owned<const IQueryDll> dll = createWuQueryDll(wu);
    if (!dll)
        return NULL;
    return createAgentQueryFactory(wu->queryWuid(), dll.getClear(), queryRootRoxiePackage(), channelNo, NULL, true, false);  // MORE - if use a constant for id might cache better?
}
