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

#ifndef _CCDQUERY_INCL
#define _CCDQUERY_INCL

#include "ccdactivities.hpp"
#include "ccdserver.hpp"
#include "ccdkey.hpp"
#include "ccdfile.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "dllserver.hpp"
#include "layouttrans.hpp"
#include "thorcommon.hpp"
#include "ccddali.hpp"
#include "thorcommon.ipp"
#include "roxierow.hpp"
#include "package.h"

class TranslatorArray : public CInterface, implements IInterface
{
    PointerIArrayOf<IRecordLayoutTranslator> a;
public:
    IMPLEMENT_IINTERFACE;
    inline IRecordLayoutTranslator *item(unsigned idx) const { return a.item(idx); }
    inline void append(IRecordLayoutTranslator * item) { a.append(item); }
    bool needsTranslation() const
    {
        ForEachItemIn(idx, a)
        {
            if (a.item(idx) != NULL)
                return true;
        }
        return false;
    }
};

interface IQueryFactory;

interface IActivityGraph : extends IInterface
{
    virtual void abort() = 0;
    virtual void reset() = 0;
    virtual void execute() = 0;
    virtual void getProbeResponse(IPropertyTree *query) = 0;
    virtual void onCreate(IRoxieSlaveContext *ctx, IHThorArg *colocalArg) = 0;
    virtual void noteException(IException *E) = 0;
    virtual void checkAbort() = 0;
    virtual IThorChildGraph * queryChildGraph() = 0;
    virtual ILocalGraph * queryLocalGraph() = 0;
    virtual IRoxieServerChildGraph * queryLoopGraph() = 0;
    virtual IRoxieServerChildGraph * createGraphLoopInstance(unsigned loopCounter, unsigned parentExtractSize, const byte * parentExtract, const IRoxieContextLogger &logctx) = 0;
    virtual const char *queryName() const = 0;
};

interface IRoxiePackage;
interface IDeserializedResultStore;

interface ISharedOnceContext : extends IInterface
{
    virtual IPropertyTree &queryOnceContext(const IQueryFactory *queryFactory, const IRoxieContextLogger &_logctx) const = 0;
    virtual IDeserializedResultStore &queryOnceResultStore() const = 0;
    virtual void checkOnceDone(const IQueryFactory *queryFactory, const IRoxieContextLogger &_logctx) const = 0;
};

interface IQueryFactory : extends IInterface
{
    virtual IRoxieSlaveContext *createSlaveContext(const SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const = 0;
    virtual IActivityGraph *lookupGraph(const char *name, IProbeManager *probeManager, const IRoxieContextLogger &logctx, IRoxieServerActivity *parentActivity) const = 0;
    virtual ISlaveActivityFactory *getSlaveActivityFactory(unsigned id) const = 0;
    virtual IRoxieServerActivityFactory *getRoxieServerActivityFactory(unsigned id) const = 0;
    virtual hash64_t queryHash() const = 0;
    virtual const char *queryQueryName() const = 0;
    virtual const char *queryErrorMessage() const = 0;
    virtual void suspend(bool suspendIt, const char *errMsg, const char *userId, bool appendIfNewError) = 0;
    virtual bool suspended() const = 0;
    virtual void getStats(StringBuffer &reply, const char *graphName) const = 0;
    virtual void resetQueryTimings() = 0;
    virtual memsize_t getMemoryLimit() const = 0;
    virtual unsigned getTimeLimit() const = 0;
    virtual ActivityArray *lookupGraphActivities(const char *name) const = 0;
    virtual bool isQueryLibrary() const = 0;
    virtual unsigned getQueryLibraryInterfaceHash() const = 0;
    virtual unsigned queryChannel() const = 0;
    virtual ILoadedDllEntry *queryDll() const = 0;
    virtual bool getEnableFieldTranslation() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
    virtual ISharedOnceContext *querySharedOnceContext() const = 0;
    virtual IDeserializedResultStore &queryOnceResultStore() const = 0;
    virtual IPropertyTree &queryOnceContext(const IRoxieContextLogger &logctx) const = 0;

    virtual const IRoxiePackage &queryPackage() const = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;

    virtual IPropertyTree *cloneQueryXGMML() const = 0;
    virtual WorkflowMachine *createWorkflowMachine(bool isOnce, const IRoxieContextLogger &logctx) const = 0;
    virtual char *getEnv(const char *name, const char *defaultValue) const = 0;
    virtual unsigned getPriority() const = 0;
    virtual unsigned getWarnTimeLimit() const = 0;
    virtual int getDebugValueInt(const char * propname, int defVal) const = 0;
    virtual bool getDebugValueBool(const char * propname, bool defVal) const = 0;

    virtual IRoxieServerContext *createContext(IPropertyTree *xml, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, const IRoxieContextLogger &_logctx, PTreeReaderOptions xmlReadFlags) const = 0;
    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const IRoxieContextLogger &_logctx) const = 0;
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, unsigned bytesOut) = 0;
    virtual IPropertyTree *getQueryStats(time_t from, time_t to) = 0;
    virtual void getGraphNames(StringArray &ret) const = 0;

    virtual IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const = 0;
    virtual void getQueryInfo(StringBuffer &result, bool full, const IRoxieContextLogger &logctx) const = 0;
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

public:
    ActivityArray(bool _multiInstance, bool _delayed, bool _library, bool _sequential)
     : multiInstance(_multiInstance), delayed(_delayed), library(_library), sequential(_sequential)
    {
        libraryGraphId = 0;
    }

    unsigned findActivityIndex(unsigned id);
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
};
MAKEPointerArray(ActivityArray, ActivityArrayArray);

typedef ActivityArray *ActivityArrayPtr;
typedef MapStringTo<ActivityArrayPtr> MapStringToActivityArray;
typedef IActivityGraph *ActivityGraphPtr;
typedef MapStringTo<ActivityGraphPtr> MapStringToActivityGraph; // Note not linked
typedef IActivityFactory *IActivityFactoryPtr;
typedef MapBetween<unsigned, unsigned, IActivityFactoryPtr, IActivityFactoryPtr> MapIdToActivityFactory;

// Common base class for Roxie server and slave activity code - see IActivityFactory

class CActivityFactory : public CInterface
{
protected:
    IQueryFactory &queryFactory;
    HelperFactory *helperFactory;
    unsigned id;
    unsigned subgraphId;
    ThorActivityKind kind;
    ActivityArrayArray childQueries;
    UnsignedArray childQueryIndexes;
    CachedOutputMetaData meta;
    mutable StatsCollector mystats;

public:
    CActivityFactory(unsigned _id, unsigned _subgraphId, IQueryFactory &_queryFactory, HelperFactory *_helperFactory, ThorActivityKind _kind);
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

    virtual void noteStatistics(const StatsCollector &fromStats)
    {
        // Merge in the stats from this instance
        mystats.merge(fromStats);
    }

    virtual void getEdgeProgressInfo(unsigned idx, IPropertyTree &edge) const
    {
        // No meaningful edge info for remote slave activities...
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

extern IRecordLayoutTranslator *createRecordLayoutTranslator(const char *logicalName, IDefRecordMeta const * diskMeta, IDefRecordMeta const * activityMeta);
extern IQueryFactory *createServerQueryFactory(const char *id, const IQueryDll *dll, const IHpccPackage &package, const IPropertyTree *stateInfo);
extern IQueryFactory *createSlaveQueryFactory(const char *id, const IQueryDll *dll, const IHpccPackage &package, unsigned _channelNo, const IPropertyTree *stateInfo);
extern IQueryFactory *getQueryFactory(hash64_t hashvalue, unsigned channel);
extern IQueryFactory *createServerQueryFactoryFromWu(IConstWorkUnit *wu);
extern IQueryFactory *createSlaveQueryFactoryFromWu(IConstWorkUnit *wu, unsigned channelNo);

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

inline ThorActivityKind getActivityKind(IPropertyTree & node)
{
    return (ThorActivityKind) node.getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
}

#endif
