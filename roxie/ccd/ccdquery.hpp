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

#ifndef _CCDQUERY_INCL
#define _CCDQUERY_INCL

#include "ccdactivities.hpp"
#include "ccdserver.hpp"
#include "ccdkey.hpp"
#include "ccdfile.hpp"
#include "ccdprotocol.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "dllserver.hpp"
#include "thorcommon.hpp"
#include "ccddali.hpp"
#include "thorcommon.ipp"
#include "roxierow.hpp"
#include "package.h"
#include "enginecontext.hpp"

interface IQueryFactory;

interface IActivityGraph : extends IInterface
{
    virtual void abort() = 0;
    virtual void reset() = 0;
    virtual void execute() = 0;
    virtual void getProbeResponse(IPropertyTree *query) = 0;
    virtual void onCreate(IHThorArg *colocalArg) = 0;
    virtual void noteException(IException *E) = 0;
    virtual void checkAbort() = 0;
    virtual IThorChildGraph * queryChildGraph() = 0;
    virtual IEclGraphResults * queryLocalGraph() = 0;
    virtual IRoxieServerChildGraph * queryLoopGraph() = 0;
    virtual IRoxieServerChildGraph * createGraphLoopInstance(IRoxieAgentContext *ctx, unsigned loopCounter, unsigned parentExtractSize, const byte * parentExtract, const IRoxieContextLogger &logctx) = 0;
    virtual const char *queryName() const = 0;
    virtual void gatherStatistics(IStatisticGatherer * statsBuilder) const = 0;
    virtual void setPrefix(const char *prefix) = 0;
    virtual unsigned queryWorkflowId() const = 0;
};

interface IRoxiePackage;
interface IDeserializedResultStore;
class CRoxieWorkflowMachine;

interface ISharedOnceContext : extends IInterface
{
    virtual IPropertyTree &queryOnceContext(const IQueryFactory *queryFactory, const IRoxieContextLogger &_logctx) const = 0;
    virtual IDeserializedResultStore &queryOnceResultStore() const = 0;
    virtual void checkOnceDone(const IQueryFactory *queryFactory, const IRoxieContextLogger &_logctx) const = 0;
};

//----------------------------------------------------------------------------------------------
// Class CQueryOptions is used to store options affecting the execution of a query
// These can be set globally, by the query workunit, or by the query XML parameters
//----------------------------------------------------------------------------------------------

class QueryOptions
{
public:
    QueryOptions();
    QueryOptions(const QueryOptions &other);

    void setFromWorkUnit(IConstWorkUnit &wu, const IPropertyTree *stateInfo);
    void setFromContext(const IPropertyTree *ctx);
    void setFromAgentLoggingFlags(unsigned loggingFlags);


    unsigned priority;
    unsigned timeLimit;
    unsigned warnTimeLimit;
    unsigned traceLimit;

    memsize_t memoryLimit;

    int parallelJoinPreload;
    int fullKeyedJoinPreload;
    int keyedJoinPreload;
    int concatPreload;
    int fetchPreload;
    int prefetchProjectPreload;
    int bindCores;
    unsigned strandBlockSize;
    unsigned forceNumStrands;
    unsigned heapFlags;

    bool checkingHeap;
    bool disableLocalOptimizations;
    RecordTranslationMode enableFieldTranslation;
    bool stripWhitespaceFromStoredDataset;
    bool timeActivities;
    bool allSortsMaySpill;
    bool traceEnabled;
    bool failOnLeaks;
    bool collectFactoryStatistics;
    bool noSeekBuildIndex;
    bool parallelWorkflow;
    unsigned numWorkflowThreads;

private:
    static const char *findProp(const IPropertyTree *ctx, const char *name1, const char *name2);
    static void updateFromWorkUnitM(memsize_t &value, IConstWorkUnit &wu, const char *name); // Needs different name to ensure works in 32-bit where memsize_t and unsigned are same type
    static void updateFromWorkUnit(int &value, IConstWorkUnit &wu, const char *name);
    static void updateFromWorkUnit(unsigned &value, IConstWorkUnit &wu, const char *name);
    static void updateFromWorkUnit(bool &value, IConstWorkUnit &wu, const char *name);
    static void updateFromWorkUnit(RecordTranslationMode &value, IConstWorkUnit &wu, const char *name);
    static void updateFromContextM(memsize_t &val, const IPropertyTree *ctx, const char *name, const char *name2 = NULL); // Needs different name to ensure works in 32-bit where memsize_t and unsigned are same type
    static void updateFromContext(int &val, const IPropertyTree *ctx, const char *name, const char *name2 = NULL);
    static void updateFromContext(unsigned &val, const IPropertyTree *ctx, const char *name, const char *name2 = NULL);
    static void updateFromContext(bool &val, const IPropertyTree *ctx, const char *name, const char *name2 = NULL);
};

interface IQueryFactory : extends IInterface
{
    virtual IRoxieAgentContext *createAgentContext(const AgentContextLogger &logctx, IRoxieQueryPacket *packet, bool hasChildren) const = 0;
    virtual IActivityGraph *lookupGraph(IRoxieAgentContext *ctx, const char *name, IProbeManager *probeManager, const IRoxieContextLogger &logctx, IRoxieServerActivity *parentActivity) const = 0;
    virtual IAgentActivityFactory *getAgentActivityFactory(unsigned id) const = 0;
    virtual IRoxieServerActivityFactory *getRoxieServerActivityFactory(unsigned id) const = 0;
    virtual hash64_t queryHash() const = 0;
    virtual const char *queryQueryName() const = 0;
    virtual const char *queryErrorMessage() const = 0;
    virtual void suspend(const char *errMsg) = 0;
    virtual bool loadFailed() const = 0;
    virtual bool suspended() const = 0;
    virtual void getStats(StringBuffer &reply, const char *graphName) const = 0;
    virtual void resetQueryTimings() = 0;
    virtual const QueryOptions &queryOptions() const = 0;
    virtual ActivityArray *lookupGraphActivities(const char *name) const = 0;
    virtual bool isQueryLibrary() const = 0;
    virtual unsigned getQueryLibraryInterfaceHash() const = 0;
    virtual unsigned queryChannel() const = 0;
    virtual ILoadedDllEntry *queryDll() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
    virtual ISharedOnceContext *querySharedOnceContext() const = 0;
    virtual IDeserializedResultStore &queryOnceResultStore() const = 0;
    virtual IPropertyTree &queryOnceContext(const IRoxieContextLogger &logctx) const = 0;

    virtual const IRoxiePackage &queryPackage() const = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;

    virtual IPropertyTree *cloneQueryXGMML() const = 0;
    virtual CRoxieWorkflowMachine *createWorkflowMachine(IConstWorkUnit *wu, bool isOnce, const IRoxieContextLogger &logctx, const QueryOptions & options) const = 0;
    virtual char *getEnv(const char *name, const char *defaultValue) const = 0;

    virtual IRoxieServerContext *createContext(IPropertyTree *xml, IHpccProtocolResponse *protocol, unsigned flags, const ContextLogger &_logctx, PTreeReaderOptions xmlReadFlags, const char *querySetName) const = 0;
    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const ContextLogger &_logctx) const = 0;
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned agentsReplyLen, unsigned bytesOut) = 0;
    virtual IPropertyTree *getQueryStats(time_t from, time_t to) = 0;
    virtual void getGraphNames(StringArray &ret) const = 0;

    virtual IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const = 0;
    virtual void getQueryInfo(StringBuffer &result, bool full, IArrayOf<IQueryFactory> *agentQueries,const IRoxieContextLogger &logctx) const = 0;
    virtual bool isDynamic() const = 0;
    virtual void checkSuspended() const = 0;
    virtual void onTermination(TerminationCallbackInfo *info) const= 0;
};

class ActivityArray : public CInterface
{
    IArrayOf<IActivityFactory> activities; 
    MapIdToActivityIndex hash;
    bool multiInstance;
    bool delayed;
    bool library;
    bool sequential;
    unsigned libraryGraphId;
    unsigned wfid;

public:
    ActivityArray(bool _multiInstance, bool _delayed, bool _library, bool _sequential, unsigned _wfid)
     : multiInstance(_multiInstance), delayed(_delayed), library(_library), sequential(_sequential), wfid(_wfid)
    {
        libraryGraphId = 0;
    }

    unsigned findActivityIndex(unsigned id) const;
    unsigned recursiveFindActivityIndex(unsigned id);
    inline IActivityFactory &item(unsigned idx) const { return activities.item(idx); }
    inline IRoxieServerActivityFactory &serverItem(unsigned idx) const { return (IRoxieServerActivityFactory &) activities.item(idx); }
    void append(IActivityFactory &item);
    void setLibraryGraphId(unsigned value) { libraryGraphId = value; }

    inline unsigned ordinality() const { return activities.ordinality(); }
    inline bool isMultiInstance() const { return multiInstance; }
    inline bool isDelayed() const { return delayed; }
    inline bool isLibrary() const { return library; }
    inline bool isSequential() const { return sequential; }
    inline unsigned getLibraryGraphId() const { return libraryGraphId; }
    inline unsigned queryWorkflowId() const { return wfid; }
};
typedef CopyReferenceArrayOf<ActivityArray> ActivityArrayArray;

typedef ActivityArray *ActivityArrayPtr;
typedef MapStringTo<ActivityArrayPtr> MapStringToActivityArray;
typedef IActivityGraph *ActivityGraphPtr;
typedef MapStringTo<ActivityGraphPtr> MapStringToActivityGraph; // Note not linked
typedef IActivityFactory *IActivityFactoryPtr;
typedef MapBetween<unsigned, unsigned, IActivityFactoryPtr, IActivityFactoryPtr> MapIdToActivityFactory;

// Common base class for Roxie server and agent activity code - see IActivityFactory

class CActivityFactory : public CInterface
{
protected:
    IQueryFactory &queryFactory;
    HelperFactory *helperFactory;
    unsigned id;
    unsigned subgraphId;
    ThorActivityKind kind;
    RecordTranslationMode recordTranslationModeHint = RecordTranslationMode::Unspecified;
    ActivityArrayArray childQueries;
    UnsignedArray childQueryIndexes;
    CachedOutputMetaData meta;
    mutable CriticalSection statsCrit;
    mutable CRuntimeStatisticCollection mystats;
    // MORE: Could be CRuntimeSummaryStatisticCollection to include derived stats, but stats are currently converted
    // to IPropertyTrees.  Would need to serialize/deserialize and then merge/derived so that they merged properly

public:
    CActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind, IPropertyTree &_graphNode);
    ~CActivityFactory() 
    { 
        ForEachItemIn(idx, childQueries)
        {
            childQueries.item(idx).Release();
        }
    }
    virtual void addChildQuery(unsigned id, ActivityArray *childQuery);
    virtual ActivityArray *queryChildQuery(unsigned idx, unsigned &id);
    virtual unsigned queryId() const { return id; }
    virtual unsigned querySubgraphId() const { return subgraphId; }
    virtual IQueryFactory &queryQueryFactory() const { return queryFactory; }
    virtual ThorActivityKind getKind() const { return kind; }

    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
        CriticalBlock b(statsCrit);
        mystats.merge(from);
    }

    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const
    {
        // No meaningful edge info for remote agent activities...
    }

    virtual void getNodeProgressInfo(IPropertyTree &node) const
    {
        mystats.getNodeProgressInfo(node);
    }

    virtual void resetNodeProgressInfo()
    {
        mystats.reset();
    }

    virtual void getActivityMetrics(StringBuffer &reply) const
    {
        mystats.toXML(reply);
    }
    virtual void getXrefInfo(IPropertyTree &reply, const IRoxieContextLogger &logctx) const
    {
        // Default is no additional information
    }

    RecordTranslationMode getEnableFieldTranslation() const
    {
        if (recordTranslationModeHint != RecordTranslationMode::Unspecified)
            return recordTranslationModeHint;
        return queryFactory.queryOptions().enableFieldTranslation;
    }
    FileFormatMode expectedFileMode() const
    {
        switch (kind)
        {
        case TAKcsvread:
        case TAKcsvfetch:
            return FileFormatMode::csv;
        case TAKxmlread:
        case TAKxmlfetch:
            return FileFormatMode::xml;
        case TAKjsonread:
        case TAKjsonfetch:
            return FileFormatMode::json;
        case TAKfetch:
        case TAKdiskread:
        case TAKdisknormalize:
        case TAKdiskaggregate:
        case TAKdiskcount:
        case TAKdiskgroupaggregate:
        case TAKdiskexists:
        case TAKkeyedjoin:  // The fetch part...
            return FileFormatMode::flat;
        default:
#ifdef _DEBUG
            DBGLOG("UNEXPECTED kind %d", (int) kind);
            PrintStackReport();
#endif
            return FileFormatMode::flat;
        }
    }
};

extern void addXrefFileInfo(IPropertyTree &reply, const IResolvedFile *dataFile);
extern void addXrefLibraryInfo(IPropertyTree &reply, const char *libraryName);

interface IQueryDll : public IInterface
{
    virtual HelperFactory *getFactory(const char *name) const = 0;
    virtual ILoadedDllEntry *queryDll() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
};

extern const IQueryDll *createQueryDll(const char *dllName);
extern const IQueryDll *createExeQueryDll(const char *exeName);
extern const IQueryDll *createWuQueryDll(IConstWorkUnit *wu);

extern IQueryFactory *createServerQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, bool isDynamic, bool forceRetry);
extern IQueryFactory *createAgentQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, unsigned _channelNo, const IPropertyTree *stateInfo, bool isDynamic, bool forceRetry);
extern IQueryFactory *getQueryFactory(hash64_t hashvalue, unsigned channel);
extern IQueryFactory *createServerQueryFactoryFromWu(IConstWorkUnit *wu, const IQueryDll *_dll);
extern IQueryFactory *createAgentQueryFactoryFromWu(IConstWorkUnit *wu, unsigned channelNo);
extern unsigned checkWorkunitVersionConsistency(const IConstWorkUnit *wu );

inline unsigned findParentId(IPropertyTree &node)
{
    return node.getPropInt("att[@name='_parentActivity']/@value", 0);
}

inline bool isRootAction(IPropertyTree &node)
{
    return !node.getPropBool("att[@name='_internal']/@value", false);
}

inline unsigned usageCount(IPropertyTree &node)
{
    return node.getPropInt("att[@name='_globalUsageCount']/@value", 0);
}

inline bool isGraphIndependent(IPropertyTree &node)
{
    return node.getPropBool("att[@name=\"_graphIndependent\"]/@value", false);
}

inline unsigned getGraphId(IPropertyTree & node)
{
    return node.getPropInt("att[@name=\"_graphId\"]/@value", 0);
}

inline ThorActivityKind getActivityKind(const IPropertyTree & node)
{
    return (ThorActivityKind) node.getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
}

#endif
