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

#ifdef _DEBUG
#define _CLEAR_ALLOCATED_ROW
#endif

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

    virtual const IRoxiePackage &queryPackage() const = 0;
    virtual IPropertyTree &queryOnceContext() const = 0;
    virtual IDeserializedResultStore &queryOnceResultStore() const = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;

    virtual IPropertyTree *cloneQueryXGMML() const = 0;
    virtual WorkflowMachine *createWorkflowMachine(bool isOnce, const IRoxieContextLogger &logctx) const = 0;
    virtual char *getEnv(const char *name, const char *defaultValue) const = 0;
    virtual unsigned getPriority() const = 0;
    virtual unsigned getWarnTimeLimit() const = 0;
    virtual int getDebugValueInt(const char * propname, int defVal) const = 0;
    virtual bool getDebugValueBool(const char * propname, bool defVal) const = 0;

    virtual IRoxieServerContext *createContext(IPropertyTree *xml, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, const IRoxieContextLogger &_logctx, const SocketEndpoint &poolEndpoint, XmlReaderOptions xmlReadFlags) const = 0;
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, unsigned bytesOut) = 0;
    virtual IPropertyTree *getQueryStats(time_t from, time_t to) = 0;
    virtual void getGraphNames(StringArray &ret) const = 0;
};

class ActivityArray : public CInterface
{
    IArrayOf<IActivityFactory> activities; 
    MapIdToActivityIndex hash;
    bool multiInstance;
    bool delayed;
    bool library;
    unsigned libraryGraphId;

public:
    ActivityArray(bool _multiInstance, bool _delayed, bool _library) { multiInstance = _multiInstance; delayed = _delayed; library = _library; libraryGraphId = 0; }

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

};


class RoxieEngineRowAllocator : public CInterface, implements IEngineRowAllocator
{
public:
    RoxieEngineRowAllocator(roxiemem::IRowManager & _rowManager, IOutputMetaData * _meta, unsigned _activityId, unsigned _allocatorId)
        : rowManager(_rowManager), meta(_meta) 
    {
        activityId = _activityId;
        allocatorId = _allocatorId;
    }

    IMPLEMENT_IINTERFACE

//interface IEngineRowsetAllocator
    virtual byte * * createRowset(unsigned count)
    {
        if (count == 0)
            return NULL;
        return (byte **) rowManager.allocate(count * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);
    }

    virtual void releaseRowset(unsigned count, byte * * rowset)
    {
        rtlReleaseRowset(count, rowset);
    }

    virtual byte * * linkRowset(byte * * rowset)
    {
        return rtlLinkRowset(rowset);
    }

    virtual byte * * appendRowOwn(byte * * rowset, unsigned newRowCount, void * row)
    {
        if (!rowset)
            rowset = createRowset(newRowCount);
        else
            rowset = (byte * *)rowManager.resizeRow(rowset, (newRowCount-1) * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);

        rowset[newRowCount-1] = (byte *)row;
        return rowset;
    }

    virtual byte * * reallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        if (!rowset)
            rowset = createRowset(newRowCount);
        else
            rowset = (byte * *)rowManager.resizeRow(rowset, oldRowCount * sizeof(void *), newRowCount * sizeof(void *), allocatorId | ACTIVITY_FLAG_ISREGISTERED);

        //New rows (if any) aren't cleared....
        return rowset;
    }

//interface IEngineAnyRowAllocator
    virtual void * createRow()
    {
        size32_t allocSize = meta.getInitialSize();
        void *ret = rowManager.allocate(allocSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
#ifdef _CLEAR_ALLOCATED_ROW
        memset(ret, 0xcc, allocSize); 
#endif
        return ret;
    }

    virtual void * createRow(size32_t & allocatedSize)
    {
        const size32_t allocSize = meta.getInitialSize();
        void *ret = rowManager.allocate(allocSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
#ifdef _CLEAR_ALLOCATED_ROW
        memset(ret, 0xcc, allocSize); 
#endif
        allocatedSize = allocSize;
        return ret;
    }

    virtual void releaseRow(const void * row)
    {
        ReleaseRoxieRow(row);
    }

    virtual void * linkRow(const void * row)
    {
        LinkRoxieRow(row);
        return const_cast<void *>(row);
    }

    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size)
    {
        void * ret = rowManager.resizeRow(row, size, newSize, allocatorId | ACTIVITY_FLAG_ISREGISTERED);
        size = newSize;
        return ret;
    }

    virtual void * finalizeRow(size32_t finalSize, void * row, size32_t oldSize)
    {
        unsigned id = allocatorId | ACTIVITY_FLAG_ISREGISTERED;
        if (meta.needsDestruct()) id |= ACTIVITY_FLAG_NEEDSDESTRUCTOR;
        return rowManager.finalizeRow(row, oldSize, finalSize, id);
    }

    virtual IOutputMetaData * queryOutputMeta()
    {
        return meta.queryOriginal();
    }
    virtual unsigned queryActivityId()
    {
        return activityId;
    }
    virtual StringBuffer &getId(StringBuffer &idStr)
    {
        return idStr.append(activityId); // MORE - may want more context info in here
    }
    virtual IOutputRowSerializer *createRowSerializer(ICodeContext *ctx)
    {
        return meta.createRowSerializer(ctx, activityId);
    }
    virtual IOutputRowDeserializer *createRowDeserializer(ICodeContext *ctx)
    {
        return meta.createRowDeserializer(ctx, activityId);
    }
protected:
    roxiemem::IRowManager & rowManager;
    CachedOutputMetaData meta;
    unsigned activityId;
    unsigned allocatorId;
};

interface IQueryDll : public IInterface
{
    virtual HelperFactory *getFactory(const char *name) const = 0;
    virtual ILoadedDllEntry *queryDll() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
};

extern const IQueryDll *createQueryDll(const char *dllName);
extern const IQueryDll *createExeQueryDll(const char *exeName);

extern IRecordLayoutTranslator *createRecordLayoutTranslator(const char *logicalName, IDefRecordMeta const * diskMeta, IDefRecordMeta const * activityMeta);
extern IQueryFactory *createRoxieServerQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo);
extern IQueryFactory *createSlaveQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, unsigned _channelNo, const IPropertyTree *stateInfo);
extern IQueryFactory *getQueryFactory(hash64_t hashvalue, unsigned channel);

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
