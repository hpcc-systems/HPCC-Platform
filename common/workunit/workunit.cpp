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
#include "workunit.hpp"
#include "jprop.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jptree.hpp"
#include "jtime.ipp"
#include "jencrypt.hpp"
#include "junicode.hpp"
#include "jlzw.hpp"
#include "jregexp.hpp"
#include "eclrtl.hpp"
#include "deftype.hpp"
#include <time.h>

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dautils.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "thorhelper.hpp"
#include "workflow.hpp"

#include "nbcd.hpp"
#include "seclib.hpp"

#include "wuerror.hpp"
#include "wujobq.hpp"
#include "environment.hpp"
#include "workunit.ipp"

static int workUnitTraceLevel = 1;

static StringBuffer &getXPath(StringBuffer &wuRoot, const char *wuid)
{
    // MORE - can fold in the date
    return wuRoot.append("/WorkUnits/").append(wuid);
}

//To be called by eclserver, but esp etc. won't know, so we need to store it.
static StringBuffer & appendLibrarySuffix(StringBuffer & suffix)
{
#ifdef _WIN32
    suffix.append("W");
#else
    suffix.append("L");
#endif
#ifdef __64BIT__
    suffix.append("64");
#else
    suffix.append("32");
 #endif
    return suffix;
}



typedef MapStringTo<bool> UniqueScopes;

static void wuAccessError(const char *username, const char *action, const char *wuscope, const char *wuid, bool excpt, bool log)
{
    StringBuffer err;
    err.append("Workunit Access Denied - action: ").append(action).append(" user:").append(username ? username : "<Unknown>");
    if (wuid)
        err.append(" workunit:").append(wuid);
    if (wuscope)
        err.append(" scope:").append(wuscope);
    //MORE - we would need more information passed in from outside if we want to make the audit message format the same as from higher level ESP calls
    SYSLOG(AUDIT_TYPE_ACCESS_FAILURE, err.str());
    if (log)
        LOG(MCuserError, "%s", err.str());
    if (excpt)
        throw MakeStringException(WUERR_AccessError, "%s", err.str());
}
static bool checkWuScopeSecAccess(const char *wuscope, ISecManager *secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    if (!secmgr || !secuser)
        return true;
    bool ret = secmgr->authorizeEx(RT_WORKUNIT_SCOPE, *secuser, wuscope)>=required;
    if (!ret && (log || excpt))
        wuAccessError(secuser->getName(), action, wuscope, NULL, excpt, log);
    return ret;
}
static bool checkWuScopeListSecAccess(const char *wuscope, ISecResourceList *scopes, int required, const char *action, bool excpt, bool log)
{
    if (!scopes)
        return true;
    bool ret=true;
    if (wuscope)
    {
        Owned<ISecResource> res=scopes->getResource(wuscope);
        if (!res || res->getAccessFlags()<required)
            ret=false;
    }
    else
    {
        for (int seq=0; ret && seq<scopes->count(); seq++)
        {
            ISecResource *res=scopes->queryResource(seq);
            if (res && res->getAccessFlags()<required)
                return false;
        }
    }
    if (!ret && (log || excpt))
        wuAccessError(NULL, action, wuscope, NULL, excpt, log);
    return ret;
}
static bool checkWuSecAccess(IConstWorkUnit &cw, ISecManager *secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    if (!secmgr || !secuser)
        return true;
    bool ret=secmgr->authorizeEx(RT_WORKUNIT_SCOPE, *secuser, cw.queryWuScope())>=required;
    if (!ret && (log || excpt))
    {
        wuAccessError(secuser->getName(), action, cw.queryWuScope(), cw.queryWuid(), excpt, log);
    }
    return ret;
}
static bool checkWuSecAccess(const char *wuid, ISecManager *secmgr, ISecUser *secuser, int required, const char *action, bool excpt, bool log)
{
    StringBuffer wuRoot;
    Owned<IRemoteConnection> conn = querySDS().connect(getXPath(wuRoot, wuid).str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        Owned<IPropertyTree> ptree=conn->getRoot();
        return checkWuScopeSecAccess(ptree->queryProp("@scope"), secmgr, secuser, required, action, excpt, log);
    }

    if (log || excpt)
        wuAccessError(secuser ? secuser->getName() : NULL, action, "Unknown", NULL, excpt, log);
    return false;
}

void doDescheduleWorkkunit(char const * wuid)
{
    StringBuffer xpath;
    xpath.append("*/*/*/");
    ncnameEscape(wuid, xpath);
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if(!conn) return;
    Owned<IPropertyTree> root = conn->getRoot();
    bool more;
    do more = root->removeProp(xpath.str()); while(more);
}

//======================================================
/*
 * Graph progress support
 */

CWuGraphStats::CWuGraphStats(IPropertyTree *_progress, StatisticCreatorType _creatorType, const char * _creator, const char * _rootScope, unsigned _id)
    : progress(_progress), creatorType(_creatorType), creator(_creator), id(_id)
{
    StatisticScopeType scopeType = SSTgraph;
    StatsScopeId rootScopeId;
    verifyex(rootScopeId.setScopeText(_rootScope));
    collector.setown(createStatisticsGatherer(_creatorType, _creator, rootScopeId));
}

void CWuGraphStats::beforeDispose()
{
    Owned<IStatisticCollection> stats = collector->getResult();

    MemoryBuffer compressed;
    {
        MemoryBuffer serialized;
        serializeStatisticCollection(serialized, stats);
        compressToBuffer(compressed, serialized.length(), serialized.toByteArray());
    }

    unsigned minActivity = 0;
    unsigned maxActivity = 0;
    stats->getMinMaxActivity(minActivity, maxActivity);

    StringBuffer tag;
    tag.append("sg").append(id);

    IPropertyTree * subgraph = createPTree(tag);
    subgraph->setProp("@c", queryCreatorTypeName(creatorType));
    subgraph->setProp("@creator", creator);
    subgraph->setPropInt("@minActivity", minActivity);
    subgraph->setPropInt("@maxActivity", maxActivity);

    //Replace the particular subgraph statistics added by this creator
    StringBuffer qualified(tag);
    qualified.append("[@creator='").append(creator).append("']");
    progress->removeProp(qualified);
    subgraph = progress->addPropTree(tag, subgraph);
    subgraph->setPropBin("Stats", compressed.length(), compressed.toByteArray());
    if (!progress->getPropBool("@stats", false))
        progress->setPropBool("@stats", true);
}

IStatisticGatherer & CWuGraphStats::queryStatsBuilder()
{
    return *collector;
}

class CConstGraphProgress : public CInterface, implements IConstWUGraphProgress
{
public:
    IMPLEMENT_IINTERFACE;
    CConstGraphProgress(const char *_wuid, const char *_graphName, IPropertyTree *_progress) : wuid(_wuid), graphName(_graphName), progress(_progress)
    {
        if (!progress)
            progress.setown(createPTree());
        formatVersion = progress->getPropInt("@format", PROGRESS_FORMAT_V);
    }
    virtual IPropertyTree * getProgressTree()
    {
        if (progress->getPropBool("@stats"))
            return createProcessTreeFromStats(); // Should we cache that?
        return LINK(progress);
    }
    virtual unsigned queryFormatVersion()
    {
        return formatVersion;
    }

protected:
    CConstGraphProgress(const char *_wuid, const char *_graphName) : wuid(_wuid), graphName(_graphName)
    {
        formatVersion = PROGRESS_FORMAT_V;
    }
    static void expandStats(IPropertyTree * target, IStatisticCollection & collection)
    {
        StringBuffer formattedValue;
        unsigned numStats = collection.getNumStatistics();
        for (unsigned i=0; i < numStats; i++)
        {
            StatisticKind kind;
            unsigned __int64 value;
            collection.getStatistic(kind, value, i);
            formatStatistic(formattedValue.clear(), value, kind);

                //Until 6.0 generate the backward compatible tag name
            const char * legacyTreeTag = queryLegacyTreeTag(kind);
            if (legacyTreeTag)
            {
                StatisticMeasure measure = queryMeasure(kind);
                if (measure == SMeasureSkew)
                {
                    //Minimum stats were always output as +ve numbers
                    if (queryStatsVariant(kind) == StSkewMin)
                        value = -value;

                    target->setPropInt64(legacyTreeTag, value/100);
                }
                else if (measure == SMeasureTimeNs)
                {
                    //Legacy timings are in ms => scale
                    target->setPropInt64(legacyTreeTag, value/1000000);
                }
                else
                    target->setProp(legacyTreeTag, formattedValue);
            }

            //Unconditionally output in the new format.
            target->setProp(queryTreeTag(kind), formattedValue);
        }
    }
    void expandProcessTreeFromStats(IPropertyTree * rootTarget, IPropertyTree * target, IStatisticCollection * collection)
    {
        expandStats(target, *collection);

        StringBuffer scopeName;
        Owned<IStatisticCollectionIterator> activityIter = &collection->getScopes(NULL);
        ForEach(*activityIter)
        {
            IStatisticCollection & cur = activityIter->query();
            cur.getScope(scopeName.clear());
            const char * id = scopeName.str();
            const char * tag;
            IPropertyTree * curTarget = target;
            switch (cur.queryScopeType())
            {
            case SSTedge:
                tag = "edge";
                id += strlen(EdgeScopePrefix);
                break;
            case SSTactivity:
                tag = "node";
                id += strlen(ActivityScopePrefix);
                break;
            case SSTsubgraph:
                //All subgraphs are added a root elements in the progress tree
                curTarget = rootTarget;
                tag = "node";
                id += strlen(SubGraphScopePrefix);
                break;
            default:
                throwUnexpected();
            }

            IPropertyTree * next = curTarget->addPropTree(tag, createPTree());
            next->setProp("@id", id);
            expandProcessTreeFromStats(rootTarget, next, &cur);
        }
    }

    IPropertyTree * createProcessTreeFromStats()
    {
        MemoryBuffer compressed;
        MemoryBuffer serialized;
        Owned<IPropertyTree> progressTree = createPTree();
        Owned<IPropertyTreeIterator> iter = progress->getElements("sg*");
        ForEach(*iter)
        {
            IPropertyTree & curSubGraph = iter->query();
            curSubGraph.getPropBin("Stats", compressed.clear());
            //Protect against updates that delete the stats while we are iterating
            if (compressed.length())
            {
                decompressToBuffer(serialized.clear(), compressed);
                Owned<IStatisticCollection> collection = createStatisticCollection(serialized);

                expandProcessTreeFromStats(progressTree, progressTree, collection);
            }
        }
        return progressTree.getClear();
    }

protected:
    Linked<IPropertyTree> progress;
    StringAttr wuid, graphName;
    unsigned formatVersion;
};

extern WORKUNIT_API IConstWUGraphProgress *createConstGraphProgress(const char *_wuid, const char *_graphName, IPropertyTree *_progress)
{
    return new CConstGraphProgress(_wuid, _graphName, _progress);
}

//--------------------------------------------------------------------------------------------------------------------

class ExtractedStatistic : public CInterfaceOf<IConstWUStatistic>
{
public:
    virtual IStringVal & getDescription(IStringVal & str, bool createDefault) const
    {
        if (!description && createDefault)
        {
            switch (kind)
            {
            case StTimeElapsed:
                {
                    if (scopeType != SSTsubgraph)
                        break;
                    //Create a default description for a root subgraph
                    const char * colon = strchr(scope, ':');
                    if (!colon)
                        break;

                    const char * subgraph = colon+1;
                    //Check for nested subgraph
                    if (strchr(subgraph, ':'))
                        break;

                    assertex(strncmp(subgraph, SubGraphScopePrefix, strlen(SubGraphScopePrefix)) == 0);
                    StringAttr graphname;
                    graphname.set(scope, colon - scope);
                    unsigned subId = atoi(subgraph + strlen(SubGraphScopePrefix));

                    StringBuffer desc;
                    formatGraphTimerLabel(desc, graphname, 0, subId);
                    str.set(desc);
                    return str;
                }
            }
        }

        str.set(description);
        return str;
    }
    virtual IStringVal & getCreator(IStringVal & str) const
    {
        str.set(creator);
        return str;
    }
    virtual IStringVal & getScope(IStringVal & str) const
    {
        str.set(scope);
        return str;
    }
    virtual IStringVal & getFormattedValue(IStringVal & str) const
    {
        StringBuffer formatted;
        formatStatistic(formatted, value, measure);
        str.set(formatted);
        return str;
    }
    virtual StatisticMeasure getMeasure() const
    {
        return measure;
    }
    virtual StatisticKind getKind() const
    {
        return kind;
    }
    virtual StatisticCreatorType getCreatorType() const
    {
        return creatorType;
    }
    virtual StatisticScopeType getScopeType() const
    {
        return scopeType;
    }
    virtual unsigned __int64 getValue() const
    {
        return value;
    }
    virtual unsigned __int64 getCount() const
    {
        return count;
    }
    virtual unsigned __int64 getMax() const
    {
        return max;
    }
    virtual unsigned __int64 getTimestamp() const
    {
        return timeStamp;
    }
    virtual bool matches(const IStatisticsFilter * filter) const
    {
        return filter->matches(creatorType, creator, scopeType, scope, measure, kind);
    }

public:
    StringAttr creator;
    StringAttr description;
    StringBuffer scope;
    StatisticMeasure measure;
    StatisticKind kind;
    StatisticCreatorType creatorType;
    StatisticScopeType scopeType;
    unsigned __int64 value;
    unsigned __int64 count;
    unsigned __int64 max;
    unsigned __int64 timeStamp;
};

class CConstGraphProgressStatisticsIterator : public CInterfaceOf<IConstWUStatisticIterator>
{
public:
    CConstGraphProgressStatisticsIterator(const char * wuid, const IStatisticsFilter * _filter) : filter(_filter)
    {
        if (filter)
            scopes.appendList(filter->queryScope(), ":");
        const char * searchGraph = "*";
        if (scopes.ordinality())
            searchGraph = scopes.item(0);

        rootPath.append("/GraphProgress/").append(wuid).append('/');
        bool singleGraph = false;
        if (!containsWildcard(searchGraph))
        {
            rootPath.append(searchGraph).append("/");
            singleGraph = true;
        }

        //Don't lock the statistics while we iterate - any partial updates must not cause problems
        if (daliClientActive())
            conn.setown(querySDS().connect(rootPath.str(), myProcessSession(), RTM_NONE, SDS_LOCK_TIMEOUT));

        if (conn && !singleGraph)
            graphIter.setown(conn->queryRoot()->getElements("*"));

        curStat.setown(new ExtractedStatistic);
        //These are currently constant for all graph statistics instances
        curStat->count = 1;
        curStat->max = 0;
        valid = false;
    }

    virtual IConstWUStatistic & query()
    {
        return *curStat;
    }

    virtual bool first()
    {
        valid = false;
        if (!conn)
            return false;

        if (graphIter && !graphIter->first())
            return false;
        ensureUniqueStatistic();
        if (!firstSubGraph())
            return false;

        valid = true;
        return true;
    }

    virtual bool next()
    {
        ensureUniqueStatistic();
        if (!nextStatistic())
        {
            if (!nextSubGraph())
            {
                if (!nextGraph())
                {
                    valid = false;
                    return false;
                }
            }
        }
        return true;
    }

    virtual bool isValid()
    {
        return valid;
    }

protected:
    bool firstSubGraph()
    {
        IPropertyTree & graphNode = graphIter ? graphIter->query() : *conn->queryRoot();
        const char * xpath = "sg*";
        StringBuffer childXpath;
        if (scopes.isItem(1))
        {
            const char * scope1 = scopes.item(1);
            if (strnicmp(scope1, "sg", 2) == 0)
            {
                childXpath.append(scope1);
                xpath = childXpath.str();
            }
        }

        subgraphIter.setown(graphNode.getElements(xpath));
        if (!subgraphIter)
            subgraphIter.setown(graphNode.getElements("sg0"));

        if (!subgraphIter->first())
            return false;
        if (firstStat())
            return true;
        return nextSubGraph();
    }

    bool nextSubGraph()
    {
        loop
        {
            if (!subgraphIter->next())
                return false;
            if (firstStat())
                return true;
        }
    }

    bool nextGraph()
    {
        if (!graphIter)
            return false;
        loop
        {
            if (!graphIter->next())
                return false;
            if (firstSubGraph())
                return true;
        }
    }

    bool firstStat()
    {
        IPropertyTree & curSubGraph = subgraphIter->query();
        if (!checkSubGraph())
            return false;

        curSubGraph.getPropBin("Stats", compressed.clear());
        //Don't crash on old format progress...
        if (compressed.length() == 0)
            return false;

        decompressToBuffer(serialized.clear(), compressed);

        Owned<IStatisticCollection> collection = createStatisticCollection(serialized);
        curStat->timeStamp = collection->queryWhenCreated();
        return beginCollection(*collection);
    }

    bool beginCollection(IStatisticCollection & collection)
    {
        collections.append(OLINK(collection));
        numStats = collection.getNumStatistics();
        curStatIndex = 0;
        if (checkScope())
        {
            if (curStatIndex < numStats)
            {
                if (checkStatistic())
                    return true;
                return nextStatistic();
            }
        }
        return nextChildScope();
    }

    bool nextStatistic()
    {
        //Finish iterating the statistics at this level.
        while (++curStatIndex < numStats)
        {
            if (checkStatistic())
                return true;
        }
        return nextChildScope();
    }

    bool nextChildScope()
    {
        loop
        {
            if (collections.ordinality() == 0)
                return false;

            IStatisticCollection * curCollection = &collections.tos();
            if (childIterators.ordinality() < collections.ordinality())
            {
                if (!filter || filter->recurseChildScopes(curStat->scopeType, curStat->scope))
                {
                    //Start iterating the children for the current collection
                    childIterators.append(curCollection->getScopes(NULL));
                    if (!childIterators.tos().first())
                    {
                        finishCollection();
                        continue;
                    }
                }
                else
                {
                    //Don't walk the child scopes
                    collections.pop();
                    continue;
                }
            }
            else if (!childIterators.tos().next())
            {
                finishCollection();
                continue;
            }

            if (beginCollection(childIterators.tos().query()))
                return true;
        }
    }

    void finishCollection()
    {
        collections.pop();
        childIterators.pop();
    }

    bool checkSubGraph()
    {
        if (!filter)
            return true;
        IPropertyTree & curSubGraph = subgraphIter->query();
        curStat->creatorType = queryCreatorType(curSubGraph.queryProp("@c"));
        curStat->creator.set(curSubGraph.queryProp("@creator"));
        return filter->matches(curStat->creatorType, curStat->creator, SSTall, NULL, SMeasureAll, StKindAll);
    }

    bool checkScope()
    {
        if (!filter)
            return true;
        IStatisticCollection * collection = &collections.tos();
        curStat->scopeType = collection->queryScopeType();
        collection->getFullScope(curStat->scope.clear());
        return filter->matches(SCTall, NULL, curStat->scopeType, curStat->scope, SMeasureAll, StKindAll);
    }

    bool checkStatistic()
    {
        IStatisticCollection & collection = collections.tos();
        collection.getStatistic(curStat->kind, curStat->value, curStatIndex);
        curStat->measure = queryMeasure(curStat->kind);
        if (!filter)
            return true;
        if (!filter->matches(SCTall, NULL, SSTall, NULL, curStat->measure, curStat->kind))
            return false;
        return true;
    }

    void ensureUniqueStatistic()
    {
        //If something else has linked this statistic, clone a unique one.
        if (curStat->IsShared())
            curStat.setown(new ExtractedStatistic(*curStat));
    }
private:
    Owned<IRemoteConnection> conn;
    Owned<ExtractedStatistic> curStat;
    const IStatisticsFilter * filter;
    StringArray scopes;
    StringBuffer rootPath;
    Owned<IPropertyTreeIterator> graphIter;
    Owned<IPropertyTreeIterator> subgraphIter;
    IArrayOf<IStatisticCollection> collections;
    IArrayOf<IStatisticCollectionIterator> childIterators;
    MemoryBuffer compressed;
    MemoryBuffer serialized;
    unsigned numStats;
    unsigned curStatIndex;
    bool valid;
};

//--------------------------------------------------------------------------------------------------------------------

mapEnums states[] = {
   { WUStateUnknown, "unknown" },
   { WUStateCompiled, "compiled" },
   { WUStateRunning, "running" },
   { WUStateCompleted, "completed" },
   { WUStateFailed, "failed" },
   { WUStateArchived, "archived" },
   { WUStateAborting, "aborting" },
   { WUStateAborted, "aborted" },
   { WUStateBlocked, "blocked" },
   { WUStateSubmitted, "submitted" },
   { WUStateScheduled, "scheduled" },
   { WUStateCompiling, "compiling" },
   { WUStateWait, "wait" },
   { WUStateUploadingFiles, "uploading_files" },
   { WUStateDebugPaused, "debugging" },
   { WUStateDebugRunning, "debug_running" },
   { WUStatePaused, "paused" },
   { WUStateSize, NULL }
};

mapEnums actions[] = {
   { WUActionUnknown, "unknown" },
   { WUActionCompile, "compile" },
   { WUActionCheck, "check" },
   { WUActionRun, "run" },
   { WUActionExecuteExisting, "execute" },
   { WUActionPause, "pause" },
   { WUActionPauseNow, "pausenow" },
   { WUActionResume, "resume" },
   { WUActionSize, NULL },
};

const char * getWorkunitStateStr(WUState state)
{
    dbgassertex(state < WUStateSize);
    return states[state].str; // MORE - should be using getEnumText, or need to take steps to ensure values remain contiguous and in order.
}

void setEnum(IPropertyTree *p, const char *propname, int value, const mapEnums *map)
{
    const char *defval = map->str;
    while (map->str)
    {
        if (value==map->val)
        {
            p->setProp(propname, map->str);
            return;
        }
        map++;
    }
    assertex(!"Unexpected value in setEnum");
    p->setProp(propname, defval);
}

static int getEnum(const char *v, const mapEnums *map)
{
    if (v)
    {
        while (map->str)
        {
            if (stricmp(v, map->str)==0)
                return map->val;
            map++;
        }
        assertex(!"Unexpected value in getEnum");
    }
    return 0;
}

static int getEnum(const IPropertyTree *p, const char *propname, const mapEnums *map)
{
    return getEnum(p->queryProp(propname),map);
}

//==========================================================================================

class CLightweightWorkunitInfo : public CInterfaceOf<IConstWorkUnitInfo>
{
public:
    CLightweightWorkunitInfo(IPropertyTree &p)
    {
        wuid.set(p.queryName());
        user.set(p.queryProp("@submitID"));
        jobName.set(p.queryProp("@jobName"));
        clusterName.set(p.queryProp("@clusterName"));
        timeScheduled.set(p.queryProp("@timeScheduled"));
        state = (WUState) getEnum(&p, "@state", states);
        action = (WUAction) getEnum(&p, "Action", actions);
        wuscope.set(p.queryProp("@scope"));
        appvalues.load(&p,"Application/*");
        totalThorTime = nanoToMilli(extractTimeCollatable(p.queryProp("@totalThorTime"), false));
        _isProtected = p.getPropBool("@protected", false);
    }
    virtual const char *queryWuid() const { return wuid.str(); }
    virtual const char *queryUser() const { return user.str(); }
    virtual const char *queryJobName() const { return jobName.str(); }
    virtual const char *queryClusterName() const { return clusterName.str(); }
    virtual const char *queryWuScope() const { return wuscope.str(); }
    virtual WUState getState() const { return state; }
    virtual const char *queryStateDesc() const { return getEnumText(state, states); }
    virtual WUAction getAction() const { return action; }
    virtual const char *queryActionDesc() const { return getEnumText(action, actions); }
    virtual bool isProtected() const { return _isProtected; }
    virtual IJlibDateTime & getTimeScheduled(IJlibDateTime & val) const
    {
        if (timeScheduled.length())
            val.setGmtString(timeScheduled.str());
        return val;
    }

    virtual unsigned getTotalThorTime() const { return totalThorTime; };
    virtual IConstWUAppValueIterator & getApplicationValues() const { return *new CArrayIteratorOf<IConstWUAppValue,IConstWUAppValueIterator> (appvalues, 0, (IConstWorkUnitInfo *) this); };
protected:
    StringAttr wuid, user, jobName, clusterName, timeScheduled, wuscope;
    mutable CachedTags<CLocalWUAppValue,IConstWUAppValue> appvalues;
    unsigned totalThorTime;
    WUState state;
    WUAction action;
    bool _isProtected;
};

extern IConstWorkUnitInfo *createConstWorkUnitInfo(IPropertyTree &p)
{
    return new CLightweightWorkunitInfo(p);
}

class CDaliWuGraphStats : public CWuGraphStats
{
public:
    CDaliWuGraphStats(IRemoteConnection *_conn, StatisticCreatorType _creatorType, const char * _creator, const char * _rootScope, unsigned _id)
        : CWuGraphStats(LINK(_conn->queryRoot()), _creatorType, _creator, _rootScope, _id), conn(_conn)
    {
    }
protected:
    Owned<IRemoteConnection> conn;
};

class CDaliWorkUnit : public CPersistedWorkUnit
{
public:
    IMPLEMENT_IINTERFACE;
    CDaliWorkUnit(IRemoteConnection *_conn, ISecManager *secmgr, ISecUser *secuser)
        : connection(_conn), CPersistedWorkUnit(secmgr, secuser)
    {
        loadPTree(connection->getRoot());
    }
    ~CDaliWorkUnit()
    {
        // NOTE - order is important - we need to construct connection before p and (especially) destroy after p
        // We use the beforeDispose() in base class to help ensure this
        p.clear();
    }
    IConstWUGraphProgress *getGraphProgress(const char *graphName) const
    {
        CriticalBlock block(crit);
        IRemoteConnection *conn = queryProgressConnection();
        if (conn)
        {
            IPTree *progress = conn->queryRoot()->queryPropTree(graphName);
            if (progress)
                return new CConstGraphProgress(p->queryName(), graphName, progress);
        }
        return NULL;
    }
    virtual WUGraphState queryGraphState(const char *graphName) const
    {
        CriticalBlock block(crit);
        IRemoteConnection *conn = queryProgressConnection();
        if (conn)
        {
            IPTree *progress = conn->queryRoot()->queryPropTree(graphName);
            if (progress)
                return (WUGraphState) progress->getPropInt("@_state", (unsigned) WUGraphUnknown);
        }
        return WUGraphUnknown;
    }
    virtual WUGraphState queryNodeState(const char *graphName, WUGraphIDType nodeId) const
    {
        CriticalBlock block(crit);
        IRemoteConnection *conn = queryProgressConnection();
        if (conn)
        {
            IPTree *progress = conn->queryRoot()->queryPropTree(graphName);
            if (progress)
            {
                StringBuffer path;
                path.append("node[@id=\"").append(nodeId).append("\"]/@_state");
                return (WUGraphState) progress->getPropInt(path, (unsigned) WUGraphUnknown);
            }
        }
        return WUGraphUnknown;
    }

    virtual void clearGraphProgress() const
    {
        CriticalBlock block(crit);
        progressConnection.clear();  // Make sure nothing is locking for read or we won't be able to lock for write
        StringBuffer path("/GraphProgress/");
        path.append(p->queryName());
        Owned<IRemoteConnection> delconn = querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        if (delconn)
            delconn->close(true);
    }

    virtual bool getRunningGraph(IStringVal &graphName, WUGraphIDType &subId) const
    {
        CriticalBlock block(crit);
        IRemoteConnection *conn = queryProgressConnection();
        if (!conn)
            return false;
        const char *name = conn->queryRoot()->queryProp("Running/@graph");
        if (name)
        {
            graphName.set(name);
            subId = conn->queryRoot()->getPropInt64("Running/@subId");
            return true;
        }
        else
            return false;
    }


    virtual void forceReload()
    {
        synchronized sync(locked); // protect locked workunits (uncommitted writes) from reload
        StringBuffer wuRoot;
        getXPath(wuRoot, p->queryName());
        IRemoteConnection *newconn = querySDS().connect(wuRoot.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
        if (!newconn)
            throw MakeStringException(WUERR_ConnectFailed, "Could not connect to workunit %s (deleted?)",p->queryName());
        CriticalBlock block(crit);
        clearCached(true);
        connection.setown(newconn);
        abortDirty = true;
        p.setown(connection->getRoot());
    }

    virtual void cleanupAndDelete(bool deldll, bool deleteOwned, const StringArray *deleteExclusions)
    {
        CPersistedWorkUnit::cleanupAndDelete(deldll, deleteOwned, deleteExclusions);
        clearGraphProgress();
        connection->close(true);
        connection.clear();
    }

    virtual void commit()
    {
        CPersistedWorkUnit::commit();
        if (connection)
            connection->commit();
    }

    virtual void _lockRemote()
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, p->queryName());
        if (connection)
            connection->changeMode(RTM_LOCK_WRITE,SDS_LOCK_TIMEOUT);
        else
            connection.setown(querySDS().connect(wuRoot.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (!connection)
            throw MakeStringException(WUERR_LockFailed, "Failed to get connection for xpath %s", wuRoot.str());
        clearCached(true);
        p.setown(connection->getRoot());
    }

    virtual void _unlockRemote()
    {
        if (connection)
        {
            try
            {
                try
                {
                    connection->commit();
                }
                catch (IException *e)
                {
                    EXCLOG(e, "Error during workunit commit");
                    connection->rollback();
                    connection->changeMode(0, SDS_LOCK_TIMEOUT);
                    throw;
                }
                connection->changeMode(0, SDS_LOCK_TIMEOUT);
            }
            catch (IException *E)
            {
                StringBuffer s;
                PrintLog("Failed to release write lock on workunit: %s", E->errorMessage(s).str());
                throw;
            }
        }
    }
    virtual void setGraphState(const char *graphName, WUGraphState state) const
    {
        Owned<IRemoteConnection> conn = getWritableProgressConnection(graphName);
        conn->queryRoot()->setPropInt("@_state", state);
    }
    virtual void setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
    {
        CriticalBlock block(crit);
        progressConnection.clear();  // Make sure nothing is locking for read or we won't be able to lock for write
        VStringBuffer path("/GraphProgress/%s", queryWuid());
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        IPTree *progress = ensurePTree(conn->queryRoot(), graphName);
        path.clear().append("node[@id=\"").append(nodeId).append("\"]");
        IPropertyTree *node = progress->queryPropTree(path.str());
        if (!node)
        {
            node = progress->addPropTree("node", createPTree());
            node->setPropInt64("@id", nodeId);
        }
        node->setPropInt("@_state", (unsigned)state);
        switch (state)
        {
            case WUGraphRunning:
            {
                IPropertyTree *running = conn->queryRoot()->setPropTree("Running", createPTree());
                running->setProp("@graph", graphName);
                running->setPropInt64("@subId", nodeId);
                break;
            }
            case WUGraphComplete:
            {
                conn->queryRoot()->removeProp("Running"); // only one thing running at any given time and one thing with lockWrite access
                break;
            }
        }
    }
    virtual IWUGraphStats *updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned subgraph) const
    {
        return new CDaliWuGraphStats(getWritableProgressConnection(graphName), creatorType, creator, graphName, subgraph);
    }

protected:
    IRemoteConnection *queryProgressConnection() const
    {
        CriticalBlock block(crit);
        if (!progressConnection)
        {
            VStringBuffer path("/GraphProgress/%s", queryWuid());
            progressConnection.setown(querySDS().connect(path, myProcessSession(), 0, SDS_LOCK_TIMEOUT)); // Note - we don't lock. The writes are atomic.
        }
        return progressConnection;
    }
    IRemoteConnection *getWritableProgressConnection(const char *graphName) const
    {
        VStringBuffer path("/GraphProgress/%s/%s", queryWuid(), graphName);
        return querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    }
    Owned<IRemoteConnection> connection;
    mutable Owned<IRemoteConnection> progressConnection;
};

class CLockedWorkUnit : public CInterface, implements ILocalWorkUnit, implements IExtendedWUInterface
{
public:
    Owned<CLocalWorkUnit> c;

    IMPLEMENT_IINTERFACE;
    CLockedWorkUnit(CLocalWorkUnit *_c) : c(_c) {}
    ~CLockedWorkUnit()
    {
        if (workUnitTraceLevel > 1)
            PrintLog("Releasing locked workunit %s", queryWuid());
        if (c)
            c->unlockRemote();
    }

    virtual IConstWorkUnit * unlock()
    {
        c->unlockRemote();
        return c.getClear();
    }
    virtual bool aborting() const
            { return c->aborting(); }
    virtual void forceReload()
            { UNIMPLEMENTED; }
    virtual WUAction getAction() const
            { return c->getAction(); }
    virtual const char *queryActionDesc() const
            { return c->queryActionDesc(); }
    virtual IStringVal & getApplicationValue(const char * application, const char * propname, IStringVal & str) const
            { return c->getApplicationValue(application, propname, str); }
    virtual int getApplicationValueInt(const char * application, const char * propname, int defVal) const
            { return c->getApplicationValueInt(application, propname, defVal); }
    virtual IConstWUAppValueIterator & getApplicationValues() const 
            { return c->getApplicationValues(); }
    virtual bool hasWorkflow() const
            { return c->hasWorkflow(); }
    virtual unsigned queryEventScheduledCount() const
            { return c->queryEventScheduledCount(); }
    virtual IPropertyTree * queryWorkflowTree() const
            { return c->queryWorkflowTree(); }
    virtual IConstWorkflowItemIterator * getWorkflowItems() const
            { return c->getWorkflowItems(); }
    virtual IWorkflowItemArray * getWorkflowClone() const
            { return c->getWorkflowClone(); }
    virtual bool requiresLocalFileUpload() const
            { return c->requiresLocalFileUpload(); }
    virtual IConstLocalFileUploadIterator * getLocalFileUploads() const
            { return c->getLocalFileUploads(); }
    virtual bool getIsQueryService() const
            { return c->getIsQueryService(); }
    virtual bool getCloneable() const
            { return c->getCloneable(); }
    virtual IUserDescriptor * queryUserDescriptor() const
            { return c->queryUserDescriptor(); }
    virtual const char *queryClusterName() const
            { return c->queryClusterName(); }
    virtual unsigned getCodeVersion() const
            { return c->getCodeVersion(); }
    virtual unsigned getWuidVersion() const
            { return c->getWuidVersion(); }
    virtual void getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const
            { c->getBuildVersion(buildVersion, eclVersion); }
    virtual bool hasDebugValue(const char * propname) const
            { return c->hasDebugValue(propname); }
    virtual IStringVal & getDebugValue(const char * propname, IStringVal & str) const
            { return c->getDebugValue(propname, str); }
    virtual int getDebugValueInt(const char * propname, int defVal) const
            { return c->getDebugValueInt(propname, defVal); }
    virtual __int64 getDebugValueInt64(const char * propname, __int64 defVal) const
            { return c->getDebugValueInt64(propname, defVal); }
    virtual bool getDebugValueBool(const char * propname, bool defVal) const
            { return c->getDebugValueBool(propname, defVal); }
    virtual IStringIterator & getDebugValues() const 
            { return c->getDebugValues(NULL); }
    virtual IStringIterator & getDebugValues(const char *prop) const 
            { return c->getDebugValues(prop); }
    virtual unsigned getExceptionCount() const
            { return c->getExceptionCount(); }
    virtual IConstWUExceptionIterator & getExceptions() const
            { return c->getExceptions(); }
    virtual unsigned getGraphCount() const
            { return c->getGraphCount(); }
    virtual unsigned getSourceFileCount() const
            { return c->getSourceFileCount(); }
    virtual unsigned getResultCount() const
            { return c->getResultCount(); }
    virtual unsigned getVariableCount() const
            { return c->getVariableCount(); }
    virtual unsigned getApplicationValueCount() const
            { return c->getApplicationValueCount(); }
    virtual IConstWUGraphIterator & getGraphs(WUGraphType type) const
            { return c->getGraphs(type); }
    virtual IConstWUGraphMetaIterator & getGraphsMeta(WUGraphType type) const
            { return c->getGraphsMeta(type); }
    virtual IConstWUGraph * getGraph(const char *name) const
            { return c->getGraph(name); }
    virtual IConstWUGraphProgress * getGraphProgress(const char * name) const
            { return c->getGraphProgress(name); }
    virtual const char *queryJobName() const
            { return c->queryJobName(); }
    virtual IConstWUPlugin * getPluginByName(const char * name) const
            { return c->getPluginByName(name); }
    virtual IConstWUPluginIterator & getPlugins() const
            { return c->getPlugins(); }
    virtual IConstWULibrary* getLibraryByName(const char *name) const
            { return c->getLibraryByName(name); }
    virtual IConstWULibraryIterator & getLibraries() const
            { return c->getLibraries(); }
    virtual WUPriorityClass getPriority() const
            { return c->getPriority(); }
    virtual int getPriorityLevel() const
            { return c->getPriorityLevel(); }
    virtual int getPriorityValue() const
            { return c->getPriorityValue(); }
    virtual IConstWUQuery * getQuery() const
            { return c->getQuery(); }
    virtual IConstWUWebServicesInfo * getWebServicesInfo() const
            { return c->getWebServicesInfo(); }
    virtual bool getRescheduleFlag() const
            { return c->getRescheduleFlag(); }
    virtual IConstWUResult * getResultByName(const char * name) const
            { return c->getResultByName(name); }
    virtual IConstWUResult * getResultBySequence(unsigned seq) const
            { return c->getResultBySequence(seq); }
    virtual unsigned getResultLimit() const
            { return c->getResultLimit(); }
    virtual IConstWUResultIterator & getResults() const
            { return c->getResults(); }
    virtual IStringVal & getScope(IStringVal & str) const
            { return c->getScope(str); }
    virtual IStringVal & getSecurityToken(IStringVal & str) const
            { return c->getSecurityToken(str); }
    virtual WUState getState() const
            { return c->getState(); }
    virtual IStringVal & getStateEx(IStringVal & str) const
            { return c->getStateEx(str); }
    virtual __int64 getAgentSession() const
            { return c->getAgentSession(); }
    virtual unsigned getAgentPID() const
            { return c->getAgentPID(); }
    virtual const char *queryStateDesc() const
            { return c->queryStateDesc(); }
    virtual bool getRunningGraph(IStringVal & graphName, WUGraphIDType & subId) const
            { return c->getRunningGraph(graphName, subId); }
    virtual IConstWUStatisticIterator & getStatistics(const IStatisticsFilter * filter) const
            { return c->getStatistics(filter); }
    virtual IConstWUStatistic * getStatistic(const char * creator, const char * scope, StatisticKind kind) const
            { return c->getStatistic(creator, scope, kind); }
    virtual IStringVal & getSnapshot(IStringVal & str) const
            { return c->getSnapshot(str); } 
    virtual const char *queryUser() const
            { return c->queryUser(); }
    virtual ErrorSeverity getWarningSeverity(unsigned code, ErrorSeverity defaultSeverity) const
            { return c->getWarningSeverity(code, defaultSeverity); }
    virtual const char *queryWuScope() const
            { return c->queryWuScope(); }
    virtual const char *queryWuid() const
            { return c->queryWuid(); }
    virtual IConstWUResult * getGlobalByName(const char * name) const
            { return c->getGlobalByName(name); }
    virtual IConstWUResult * getTemporaryByName(const char * name) const
            { return c->getTemporaryByName(name); }
    virtual IConstWUResultIterator & getTemporaries() const
            { return c->getTemporaries(); }
    virtual IConstWUResult * getVariableByName(const char * name) const
            { return c->getVariableByName(name); }
    virtual IConstWUResultIterator & getVariables() const
            { return c->getVariables(); }
    virtual bool isProtected() const
            { return c->isProtected(); }
    virtual bool isPausing() const
            { return c->isPausing(); }
    virtual IWorkUnit & lock()
            { ((CInterface *)this)->Link(); return (IWorkUnit &) *this; }
    virtual bool reload()
            { UNIMPLEMENTED; }
    virtual void subscribe(WUSubscribeOptions options)
            { c->subscribe(options); }
    virtual void requestAbort()
            { c->requestAbort(); }
    virtual unsigned calculateHash(unsigned prevHash)
            { return queryExtendedWU(c)->calculateHash(prevHash); }
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool all)
            { queryExtendedWU(c)->copyWorkUnit(cached, all); }
    virtual IPropertyTree *queryPTree() const
            { return queryExtendedWU(c)->queryPTree(); }
    virtual IPropertyTree *getUnpackedTree(bool includeProgress) const
            { return queryExtendedWU(c)->getUnpackedTree(includeProgress); }
    virtual bool archiveWorkUnit(const char *base,bool del,bool deldll,bool deleteOwned)
            { return queryExtendedWU(c)->archiveWorkUnit(base,del,deldll,deleteOwned); }
    virtual unsigned queryFileUsage(const char *filename) const
            { return c->queryFileUsage(filename); }
    virtual IJlibDateTime & getTimeScheduled(IJlibDateTime &val) const
            { return c->getTimeScheduled(val); }
    virtual unsigned getDebugAgentListenerPort() const
            { return c->getDebugAgentListenerPort(); }
    virtual IStringVal & getDebugAgentListenerIP(IStringVal &ip) const
            { return c->getDebugAgentListenerIP(ip); }
    virtual IStringVal & getXmlParams(IStringVal & params, bool hidePasswords) const
            { return c->getXmlParams(params, hidePasswords); }
    virtual const IPropertyTree *getXmlParams() const
            { return c->getXmlParams(); }
    virtual unsigned __int64 getHash() const
            { return c->getHash(); }
    virtual IStringIterator *getLogs(const char *type, const char *instance) const
            { return c->getLogs(type, instance); }
    virtual IStringIterator *getProcesses(const char *type) const
            { return c->getProcesses(type); }
    virtual IPropertyTreeIterator* getProcesses(const char *type, const char *instance) const
            { return c->getProcesses(type, instance); }
    virtual unsigned getTotalThorTime() const
            { return c->getTotalThorTime(); }
    virtual WUGraphState queryGraphState(const char *graphName) const
            { return c->queryGraphState(graphName); }
    virtual WUGraphState queryNodeState(const char *graphName, WUGraphIDType nodeId) const
            { return c->queryNodeState(graphName, nodeId); }
    virtual void setGraphState(const char *graphName, WUGraphState state) const
            { c->setGraphState(graphName, state); }
    virtual void setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
            { c->setNodeState(graphName, nodeId, state); }
    virtual IWUGraphStats *updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned subgraph) const
            { return c->updateStats(graphName, creatorType, creator, subgraph); }
    virtual void clearGraphProgress() const
            { c->clearGraphProgress(); }


    virtual void clearExceptions()
            { c->clearExceptions(); }
    virtual void commit()
            { c->commit(); }
    virtual IWUException * createException()
            { return c->createException(); }
    virtual void addProcess(const char *type, const char *instance, unsigned pid, const char *log)
            { c->addProcess(type, instance, pid, log); }
    virtual void protect(bool protectMode)
            { c->protect(protectMode); }
    virtual void setAction(WUAction action)
            { c->setAction(action); }
    virtual void setApplicationValue(const char * application, const char * propname, const char * value, bool overwrite)
            { c->setApplicationValue(application, propname, value, overwrite); }
    virtual void setApplicationValueInt(const char * application, const char * propname, int value, bool overwrite)
            { c->setApplicationValueInt(application, propname, value, overwrite); }
    virtual void incEventScheduledCount()
            { c->incEventScheduledCount(); }
    virtual void setIsQueryService(bool value)
            { c->setIsQueryService(value); }
    virtual void setCloneable(bool value)
            { c->setCloneable(value); }
    virtual void setIsClone(bool value)
            { c->setIsClone(value); }
    virtual void setClusterName(const char * value)
            { c->setClusterName(value); }
    virtual void setCodeVersion(unsigned version, const char * buildVersion, const char * eclVersion)
            { c->setCodeVersion(version, buildVersion, eclVersion); }
    virtual void setDebugValue(const char * propname, const char * value, bool overwrite)
            { c->setDebugValue(propname, value, overwrite); }
    virtual void setDebugValueInt(const char * propname, int value, bool overwrite)
            { c->setDebugValueInt(propname, value, overwrite); }
    virtual void setJobName(const char * value)
            { c->setJobName(value); }
    virtual void setPriority(WUPriorityClass cls)
            { c->setPriority(cls); }
    virtual void setPriorityLevel(int level)
            { c->setPriorityLevel(level); }
    virtual void setRescheduleFlag(bool value)
            { c->setRescheduleFlag(value); }
    virtual void setResultLimit(unsigned value)
            { c->setResultLimit(value); }
    virtual void setSecurityToken(const char *value)
            { c->setSecurityToken(value); }
    virtual void setState(WUState state)
            { c->setState(state); }
    virtual void setStateEx(const char * text)
            { c->setStateEx(text); }
    virtual void setAgentSession(__int64 sessionId)
            { c->setAgentSession(sessionId); }
    virtual void setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction)
            { c->setStatistic(creatorType, creator, scopeType, scope, kind, optDescription, value, count, maxValue, mergeAction); }
    virtual void setTracingValue(const char * propname, const char * value)
            { c->setTracingValue(propname, value); }
    virtual void setTracingValueInt(const char * propname, int value)
            { c->setTracingValueInt(propname, value); }
    virtual void setUser(const char * value)
            { c->setUser(value); }
    virtual void setWuScope(const char * value)
            { c->setWuScope(value); }
    virtual IWorkflowItem* addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
            { return c->addWorkflowItem(wfid, type, mode, success, failure, recovery, retriesAllowed, contingencyFor); }
    virtual void syncRuntimeWorkflow(IWorkflowItemArray * array)
            { c->syncRuntimeWorkflow(array); }
    virtual IWorkflowItemIterator * updateWorkflowItems()
            { return c->updateWorkflowItems(); }
    virtual void resetWorkflow()
            { c->resetWorkflow(); }
    virtual void schedule()
            { c->schedule(); }
    virtual void deschedule()
            { c->deschedule(); }
    virtual unsigned addLocalFileUpload(LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
            { return c->addLocalFileUpload(type, source, destination, eventTag); }
    virtual IWUResult * updateGlobalByName(const char * name)
            { return c->updateGlobalByName(name); }
    virtual void createGraph(const char * name, const char *label, WUGraphType type, IPropertyTree *xgmml)
            { c->createGraph(name, label, type, xgmml); }
    virtual IWUQuery * updateQuery()
            { return c->updateQuery(); }
    virtual IWUWebServicesInfo * updateWebServicesInfo(bool create)
        { return c->updateWebServicesInfo(create); }
    virtual IWUPlugin * updatePluginByName(const char * name)
            { return c->updatePluginByName(name); }
    virtual IWULibrary * updateLibraryByName(const char * name)
            { return c->updateLibraryByName(name); }
    virtual IWUResult * updateResultByName(const char * name)
            { return c->updateResultByName(name); }
    virtual IWUResult * updateResultBySequence(unsigned seq)
            { return c->updateResultBySequence(seq); }
    virtual IWUResult * updateTemporaryByName(const char * name)
            { return c->updateTemporaryByName(name); }
    virtual IWUResult * updateVariableByName(const char * name)
            { return c->updateVariableByName(name); }
    virtual void addFile(const char *fileName, StringArray *clusters, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
            { c->addFile(fileName, clusters, usageCount, fileKind, graphOwner); }
    virtual void noteFileRead(IDistributedFile *file)
            { c->noteFileRead(file); }
    virtual void releaseFile(const char *fileName)
            { c->releaseFile(fileName); }
    virtual void resetBeforeGeneration()
            { c->resetBeforeGeneration(); }
    virtual void deleteTempFiles(const char *graph, bool deleteOwned, bool deleteJobOwned)
            { c->deleteTempFiles(graph, deleteOwned, deleteJobOwned); }
    virtual void deleteTemporaries()
            { c->deleteTemporaries(); }
    virtual void addDiskUsageStats(__int64 avgNodeUsage, unsigned minNode, __int64 minNodeUsage, unsigned maxNode, __int64 maxNodeUsage, __int64 graphId)
            { c->addDiskUsageStats(avgNodeUsage, minNode, minNodeUsage, maxNode, maxNodeUsage, graphId); }
    virtual IPropertyTree * getDiskUsageStats()
            { return c->getDiskUsageStats(); }
    virtual IPropertyTreeIterator & getFileIterator() const
            { return c->getFileIterator(); }
    virtual IPropertyTreeIterator & getFilesReadIterator() const
            { return c->getFilesReadIterator(); }

    virtual void setSnapshot(const char * value)
            { c->setSnapshot(value); }
    virtual void setWarningSeverity(unsigned code, ErrorSeverity severity)
            { c->setWarningSeverity(code, severity); }
    virtual void setTimeScheduled(const IJlibDateTime &val)
            { c->setTimeScheduled(val); }
    virtual void setDebugAgentListenerPort(unsigned port)
            { c->setDebugAgentListenerPort(port); }
    virtual void setDebugAgentListenerIP(const char * ip)
            { c->setDebugAgentListenerIP(ip); }
    virtual void setXmlParams(const char *params)
            { c->setXmlParams(params); }
    virtual void setXmlParams(IPropertyTree *tree)
            { c->setXmlParams(tree); }
    virtual void setHash(unsigned __int64 hash)
            { c->setHash(hash); }

// ILocalWorkUnit - used for debugging etc
    virtual void serialize(MemoryBuffer &tgt)
            { c->serialize(tgt); }
    virtual void deserialize(MemoryBuffer &src)
            { c->deserialize(src); }

    virtual bool switchThorQueue(const char *cluster, IQueueSwitcher *qs)
            { return c->switchThorQueue(cluster,qs); }
    virtual void setAllowedClusters(const char *value)
            { c->setAllowedClusters(value); }
    virtual IStringVal& getAllowedClusters(IStringVal &str) const
            { return c->getAllowedClusters(str); }
    virtual void remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const
            { c->remoteCheckAccess(user,writeaccess); }
    virtual void setAllowAutoQueueSwitch(bool val)
            { c->setAllowAutoQueueSwitch(val); }
    virtual bool getAllowAutoQueueSwitch() const
            { return c->getAllowAutoQueueSwitch(); }
    virtual void setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash)
            { c->setLibraryInformation(name, interfaceHash, definitionHash); }
    virtual void setResultInt(const char * name, unsigned sequence, __int64 val)
            { c->setResultInt(name, sequence, val); }
    virtual void setResultUInt(const char * name, unsigned sequence, unsigned __int64 val)
            { c->setResultUInt(name, sequence, val); }
    virtual void setResultReal(const char *name, unsigned sequence, double val)
            { c->setResultReal(name, sequence, val); }
    virtual void setResultVarString(const char * stepname, unsigned sequence, const char *val)
            { c->setResultVarString(stepname, sequence, val); }
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val)
            { c->setResultVarUnicode(stepname, sequence, val); }
    virtual void setResultString(const char * stepname, unsigned sequence, int len, const char *val)
            { c->setResultString(stepname, sequence, len, val); }
    virtual void setResultData(const char * stepname, unsigned sequence, int len, const void *val)
            { c->setResultData(stepname, sequence, len, val); }
    virtual void setResultRaw(const char * name, unsigned sequence, int len, const void *val)
            { c->setResultRaw(name, sequence, len, val); }
    virtual void setResultSet(const char * name, unsigned sequence, bool isAll, size32_t len, const void *val, ISetToXmlTransformer *xform)
            { c->setResultSet(name, sequence, isAll, len, val, xform); }
    virtual void setResultUnicode(const char * name, unsigned sequence, int len, UChar const * val)
            { c->setResultUnicode(name, sequence, len, val); }
    virtual void setResultBool(const char *name, unsigned sequence, bool val)
            { c->setResultBool(name, sequence, val); }
    virtual void setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
            { c->setResultDecimal(name, sequence, len,  precision, isSigned, val); }
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend)
            { c->setResultDataset(name, sequence, len, val, numRows, extend); }
};

class CLocalWUAssociated : public CInterface, implements IConstWUAssociatedFile
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUAssociated(IPropertyTree *p);

    virtual WUFileType getType() const;
    virtual IStringVal & getDescription(IStringVal & ret) const;
    virtual IStringVal & getIp(IStringVal & ret) const;
    virtual IStringVal & getName(IStringVal & ret) const;
    virtual IStringVal & getNameTail(IStringVal & ret) const;
    virtual unsigned getCrc() const;
};

class CLocalWUQuery : public CInterface, implements IWUQuery
{
    Owned<IPropertyTree> p;
    mutable IArrayOf<IConstWUAssociatedFile> associated;
    mutable CriticalSection crit;
    mutable bool associatedCached;

private:
    void addSpecialCaseAssociated(WUFileType type, const char * propname, unsigned crc) const;
    void loadAssociated() const;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUQuery(IPropertyTree *p);

    virtual WUQueryType getQueryType() const;
    virtual IStringVal& getQueryText(IStringVal &str) const;
    virtual IStringVal& getQueryShortText(IStringVal &str) const;
    virtual IStringVal& getQueryName(IStringVal &str) const;
    virtual IStringVal & getQueryMainDefinition(IStringVal & str) const;
    virtual IStringVal& getQueryDllName(IStringVal &str) const;
    virtual unsigned getQueryDllCrc() const;
    virtual IStringVal& getQueryCppName(IStringVal &str) const;
    virtual IStringVal& getQueryResTxtName(IStringVal &str) const;
    virtual IConstWUAssociatedFile * getAssociatedFile(WUFileType type, unsigned index) const;
    virtual IConstWUAssociatedFileIterator& getAssociatedFiles() const;
    virtual bool isArchive() const;

    virtual void        setQueryType(WUQueryType qt);
    virtual void        setQueryText(const char *pstr);
    virtual void        setQueryName(const char *);
    virtual void        setQueryMainDefinition(const char * str);
    virtual void        addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc);
    virtual void        removeAssociatedFiles();
    virtual void        removeAssociatedFile(WUFileType type, const char * name, const char * desc);
};

class CLocalWUWebServicesInfo : public CInterface, implements IWUWebServicesInfo
{
    Owned<IPropertyTree> p;
    mutable CriticalSection crit;

private:
    
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUWebServicesInfo(IPropertyTree *p);

    virtual IStringVal& getModuleName(IStringVal &str) const;
    virtual IStringVal& getAttributeName(IStringVal &str) const;
    virtual IStringVal& getDefaultName(IStringVal &str) const;
    virtual IStringVal& getInfo(const char *name, IStringVal &str) const;
    virtual IStringVal& getText(const char *name, IStringVal &str) const;
    virtual unsigned getWebServicesCRC() const;
 
    virtual void        setModuleName(const char *);
    virtual void        setAttributeName(const char *);
    virtual void        setDefaultName(const char *);
    virtual void        setInfo(const char *name, const char *info);
    virtual void        setText(const char *name, const char *info);
    virtual void        setWebServicesCRC(unsigned);
};

class CLocalWUResult : public CInterface, implements IWUResult
{
    friend class CLocalWorkUnit;

    mutable CriticalSection crit;
    Owned<IPropertyTree> p;
    Owned<IProperties> xmlns;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUResult(IPropertyTree *props);
    ~CLocalWUResult() { try { p.clear(); } catch (IException *E) {E->Release();}}

    virtual WUResultStatus getResultStatus() const;
    virtual IStringVal& getResultName(IStringVal &str) const;
    virtual int         getResultSequence() const;
    virtual bool        isResultScalar() const;
    virtual IStringVal& getResultXml(IStringVal &str, bool hidePasswords) const;
    virtual unsigned    getResultFetchSize() const;
    virtual __int64     getResultTotalRowCount() const;
    virtual __int64     getResultRowCount() const;
    virtual void        getResultDataset(IStringVal & ecl, IStringVal & defs) const;
    virtual IStringVal& getResultLogicalName(IStringVal &ecl) const;
    virtual IStringVal& getResultKeyField(IStringVal& ecl) const;
    virtual unsigned    getResultRequestedRows() const;

    virtual __int64     getResultInt() const;
    virtual bool        getResultBool() const;
    virtual double      getResultReal() const;
    virtual IStringVal& getResultString(IStringVal & str, bool hidePassword) const;
    virtual IDataVal&   getResultRaw(IDataVal & data, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IDataVal&   getResultUnicode(IDataVal & data) const;
    virtual void        getResultDecimal(void * val, unsigned length, unsigned precision, bool isSigned) const;
    virtual IStringVal& getResultEclSchema(IStringVal & str) const;
    virtual __int64     getResultRawSize(IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IDataVal&   getResultRaw(IDataVal & data, __int64 from, __int64 length, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const;
    virtual IStringVal& getResultRecordSizeEntry(IStringVal & str) const;
    virtual IStringVal& getResultTransformerEntry(IStringVal & str) const;
    virtual __int64     getResultRowLimit() const;
    virtual IStringVal& getResultFilename(IStringVal & str) const;
    virtual WUResultFormat getResultFormat() const;
    virtual unsigned    getResultHash() const;
    virtual bool        getResultIsAll() const;
    virtual IProperties *queryResultXmlns();
    virtual IStringVal& getResultFieldOpt(const char *name, IStringVal &str) const;
    virtual void getSchema(IArrayOf<ITypeInfo> &types, StringAttrArray &names, IStringVal * ecl=NULL) const;

    // interface IWUResult
    virtual void        setResultStatus(WUResultStatus status);
    virtual void        setResultName(const char *name);
    virtual void        setResultSequence(unsigned seq);
    virtual void        setResultSchemaRaw(unsigned len, const void *schema);
    virtual void        setResultScalar(bool isScalar);
    virtual void        setResultRaw(unsigned len, const void *xml, WUResultFormat format);
    virtual void        setResultFetchSize(unsigned rows);      // 0 means file-loaded
    virtual void        setResultTotalRowCount(__int64 rows);   // -1 means unknown
    virtual void        setResultRowCount(__int64 rows);
    virtual void        setResultDataset(const char *ecl, const char *defs);        
    virtual void        setResultLogicalName(const char *logicalName);
    virtual void        setResultKeyField(const char * name);
    virtual void        setResultRequestedRows(unsigned req);
    virtual void        setResultRecordSizeEntry(const char * val);
    virtual void        setResultTransformerEntry(const char * val);
    virtual void        setResultInt(__int64 val);
    virtual void        setResultReal(double val);
    virtual void        setResultBool(bool val);
    virtual void        setResultString(const char * val, unsigned length);
    virtual void        setResultUnicode(const void * val, unsigned length);
    virtual void        setResultData(const void * val, unsigned length);
    virtual void        setResultDecimal(const void * val, unsigned length);
    virtual void        addResultRaw(unsigned len, const void * data, WUResultFormat format);
    virtual void        setResultRowLimit(__int64 value);
    virtual void        setResultFilename(const char * name);
    virtual void        setResultUInt(unsigned __int64 val);
    virtual void        setResultIsAll(bool value);
    virtual void        setResultFormat(WUResultFormat format);
    virtual void        setResultXML(const char *val);
    virtual void        setResultRow(unsigned len, const void * data);
    virtual void        setResultXmlns(const char *prefix, const char *uri);
    virtual void        setResultFieldOpt(const char *name, const char *value);

    virtual IPropertyTree *queryPTree() { return p; }
};

class CLocalWUPlugin : public CInterface, implements IWUPlugin
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUPlugin(IPropertyTree *p);

    virtual IStringVal& getPluginName(IStringVal &str) const;
    virtual IStringVal& getPluginVersion(IStringVal &str) const;

    virtual void        setPluginName(const char *str);
    virtual void        setPluginVersion(const char *str);
};

class CLocalWULibrary : public CInterface, implements IWULibrary
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWULibrary(IPropertyTree *p);

    virtual IStringVal & getName(IStringVal & str) const;
    virtual void setName(const char * str);
};

class CLocalWUException : public CInterface, implements IWUException
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUException(IPropertyTree *p);

    virtual IStringVal& getExceptionSource(IStringVal &str) const;
    virtual IStringVal& getExceptionMessage(IStringVal &str) const;
    virtual unsigned    getExceptionCode() const;
    virtual ErrorSeverity getSeverity() const;
    virtual IStringVal & getTimeStamp(IStringVal & dt) const;
    virtual IStringVal & getExceptionFileName(IStringVal & str) const;
    virtual unsigned    getExceptionLineNo() const;
    virtual unsigned    getExceptionColumn() const;
    virtual unsigned    getSequence() const;
    virtual void        setExceptionSource(const char *str);
    virtual void        setExceptionMessage(const char *str);
    virtual void        setExceptionCode(unsigned code);
    virtual void        setSeverity(ErrorSeverity level);
    virtual void        setTimeStamp(const char * dt);
    virtual void        setExceptionFileName(const char *str);
    virtual void        setExceptionLineNo(unsigned r);
    virtual void        setExceptionColumn(unsigned c);
};

//==========================================================================================

extern WORKUNIT_API bool isSpecialResultSequence(unsigned sequence)
{
    switch (sequence)
    {
    case ResultSequenceInternal:
    case ResultSequenceOnce:
    case ResultSequencePersist:
    case ResultSequenceStored:
        return true;
    default:
        assertex(sequence <= INT_MAX);
        return false;
    }
}

class CConstWUArrayIterator : public CInterface, implements IConstWorkUnitIterator
{
    IArrayOf<IPropertyTree> trees;
    Owned<IConstWorkUnitInfo> cur;
    unsigned curTreeNum;

    void setCurrent()
    {
        cur.setown(new CLightweightWorkunitInfo(trees.item(curTreeNum)));
    }
public:
    IMPLEMENT_IINTERFACE;
    CConstWUArrayIterator(IArrayOf<IPropertyTree> &_trees)
    {
        ForEachItemIn(t, _trees)
            trees.append(*LINK(&_trees.item(t)));
        curTreeNum = 0;
    }
    bool first()
    {
        curTreeNum = 0;
        return next();
    }
    bool isValid()
    {
        return (NULL != cur.get());
    }
    bool next()
    {
        if (curTreeNum >= trees.ordinality())
        {
            cur.clear();
            return false;
        }
        setCurrent();
        ++curTreeNum;
        return true;
    }
    IConstWorkUnitInfo & query() { return *cur; }
};
//==========================================================================================

class CStringArrayIterator : public CInterface, implements IStringIterator
{
    StringArray strings;
    unsigned idx;
public:
    IMPLEMENT_IINTERFACE;
    CStringArrayIterator() { idx = 0; };
    void append(const char *str) { strings.append(str); }
    virtual bool first() { idx = 0; return strings.isItem(idx); }
    virtual bool next() { idx ++; return strings.isItem(idx); }
    virtual bool isValid() { return strings.isItem(idx); }
    virtual IStringVal & str(IStringVal &s) { s.set(strings.item(idx)); return s; }
};

class CCachedJobNameIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CCachedJobNameIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryName()+1); return s; }
};

class CEmptyStringIterator : public CInterface, implements IStringIterator
{
public:
    IMPLEMENT_IINTERFACE;
    virtual bool first() { return false; }
    virtual bool next() { return false; }
    virtual bool isValid() { return false; }
    virtual IStringVal & str(IStringVal &s) { s.clear(); return s; }
};

mapEnums workunitSortFields[] =
{
   { WUSFuser, "@submitID" },
   { WUSFcluster, "@clusterName" },
   { WUSFjob, "@jobName" },
   { WUSFstate, "@state" },
   { WUSFpriority, "@priorityClass" },
   { WUSFprotected, "@protected" },
   { WUSFwuid, "@" },
   { WUSFecl, "Query/Text" },
   { WUSFfileread, "FilesRead/File/@name" },
   { WUSFtotalthortime, "@totalThorTime|"
                        "Statistics/Statistic[@c='summary'][@creator='thor'][@kind='TimeElapsed']/@value|"
                        "Statistics/Statistic[@c='summary'][@creator='hthor'][@kind='TimeElapsed']/@value|"
                        "Statistics/Statistic[@c='summary'][@creator='roxie'][@kind='TimeElapsed']/@value|"
                        "Statistics/Statistic[@desc='Total thor time']/@value|"
                        "Timings/Timing[@name='Total thor time']/@duration"                                 //Use Statistics first. If not found, use Timings
   },
   { WUSFwuidhigh, "@" },
   { WUSFwildwuid, "@" },
   { WUSFappvalue, "Application" },
   { WUSFterm, NULL }
};

extern const char *queryFilterXPath(WUSortField field)
{
    return getEnumText(field, workunitSortFields);
}

mapEnums querySortFields[] =
{
   { WUQSFId, "@id" },
   { WUQSFwuid, "@wuid" },
   { WUQSFname, "@name" },
   { WUQSFdll, "@dll" },
   { WUQSFmemoryLimit, "@memoryLimit" },
   { WUQSFmemoryLimitHi, "@memoryLimit" },
   { WUQSFtimeLimit, "@timeLimit" },
   { WUQSFtimeLimitHi, "@timeLimit" },
   { WUQSFwarnTimeLimit, "@warnTimeLimit" },
   { WUQSFwarnTimeLimitHi, "@warnTimeLimit" },
   { WUQSFpriority, "@priority" },
   { WUQSFpriorityHi, "@priority" },
   { WUQSFQuerySet, "@querySetId" },
   { WUQSFActivited, "@activated" },
   { WUQSFSuspendedByUser, "@suspended" },
   { WUQSFLibrary, "Library"},
   { WUQSFPublishedBy, "@publishedBy" },
   { WUQSFterm, NULL }
};

class asyncRemoveDllWorkItem: public CInterface, implements IWorkQueueItem // class only used in asyncRemoveDll
{
    StringAttr name;
public:
    IMPLEMENT_IINTERFACE;

    asyncRemoveDllWorkItem(const char * _name) : name(_name)
    {
    }
    void execute()
    {
        PROGLOG("WU removeDll %s", name.get());
        queryDllServer().removeDll(name, true, true); // <name>, removeDlls=true, removeDirectory=true
    }
};      

class asyncRemoveRemoteFileWorkItem: public CInterface, implements IWorkQueueItem // class only used in asyncRemoveFile
{
    RemoteFilename name;
public:
    IMPLEMENT_IINTERFACE;

    asyncRemoveRemoteFileWorkItem(const char * _ip, const char * _name)
    {
        SocketEndpoint ep(_ip);
        name.setPath(ep, _name);
    }
    void execute()
    {
        Owned<IFile> file = createIFile(name);
        PROGLOG("WU removeDll %s",file->queryFilename());
        file->remove();
    }
};

//==========================================================================================

class CConstQuerySetQueryIterator : public CInterface, implements IConstQuerySetQueryIterator
{
    IArrayOf<IPropertyTree> trees;
    unsigned index;
public:
    IMPLEMENT_IINTERFACE;
    CConstQuerySetQueryIterator(IArrayOf<IPropertyTree> &_trees)
    {
        ForEachItemIn(t, _trees)
            trees.append(*LINK(&_trees.item(t)));
        index = 0;
    }
    ~CConstQuerySetQueryIterator()
    {
        trees.kill();
    }
    bool first()
    {
        index = 0;
        return (trees.ordinality()!=0);
    }

    bool next()
    {
        index++;
        return (index<trees.ordinality());
    }

    bool isValid()
    {
        return (index<trees.ordinality());
    }

    IPropertyTree &query()
    {
        return trees.item(index);
    }
};

class CSecurityCache
{

};

class CConstWUIterator : public CInterface, implements IConstWorkUnitIterator
{
public:
    IMPLEMENT_IINTERFACE;
    CConstWUIterator(IPropertyTreeIterator *_ptreeIter)
        : ptreeIter(_ptreeIter)
    {
    }
    bool first()
    {
        if (!ptreeIter->first())
        {
            cur.clear();
            return false;
        }
        cur.setown(new CLightweightWorkunitInfo(ptreeIter->query()));
        return true;
    }
    bool isValid()
    {
        return (NULL != cur.get());
    }
    bool next()
    {
        if (!ptreeIter->next())
        {
            cur.clear();
            return false;
        }
        cur.setown(new CLightweightWorkunitInfo(ptreeIter->query()));
        return true;
    }
    IConstWorkUnitInfo & query() { return *cur; }
private:
    Owned<IConstWorkUnitInfo> cur;
    Owned<IPropertyTreeIterator> ptreeIter;

};

class CSecureConstWUIterator : public CInterfaceOf<IConstWorkUnitIterator>
{
public:
    CSecureConstWUIterator(IConstWorkUnitIterator *_parent, ISecManager *_secmgr=NULL, ISecUser *_secuser=NULL)
        : parent(_parent), secmgr(_secmgr), secuser(_secuser)
    {
        assertex(_secuser && _secmgr);
    }
    bool first()
    {
        if (!parent->first())
            return false;
        return getNext();
    }
    bool next()
    {
        if (!parent->next())
            return false;
        return getNext();
    }
    virtual bool isValid()
    {
        return parent->isValid();
    }
    virtual IConstWorkUnitInfo &query()
    {
        return parent->query();
    }
private:
    Owned<IConstWorkUnitIterator> parent;
    MapStringTo<int> scopePermissions;
    Linked<ISecManager> secmgr;
    Linked<ISecUser> secuser;

    bool getNext() // scan for a workunit with permissions
    {
        do
        {
            const char *scopeName = parent->query().queryWuScope();
            if (!scopeName || !*scopeName || checkScope(scopeName))
                return true;
        } while (parent->next());
        return false;
    }
    bool checkScope(const char *scopeName)
    {
        int *perms = scopePermissions.getValue(scopeName);
        int perm;
        if (!perms)
        {
            perm = secuser.get() ? secmgr->authorizeWorkunitScope(*secuser, scopeName):-1;
            scopePermissions.setValue(scopeName, perm);
        }
        else
            perm = *perms;
        return perm >= SecAccess_Read;
    }
};

CWorkUnitFactory::CWorkUnitFactory()
{
}

CWorkUnitFactory::~CWorkUnitFactory()
{
}

IWorkUnit* CWorkUnitFactory::createNamedWorkUnit(const char *wuid, const char *app, const char *scope, ISecManager *secmgr, ISecUser *secuser)
{
    checkWuScopeSecAccess(scope, secmgr, secuser, SecAccess_Write, "Create", true, true);
    Owned<CLocalWorkUnit> cw = _createWorkUnit(wuid, secmgr, secuser);
    if (scope)
        cw->setWuScope(scope);  // Note - this may check access rights and throw exception. Is that correct? We might prefer to only check access once, and this will check on the lock too...
    IWorkUnit* ret = &cw->lockRemote(false);   // Note - this may throw exception if user does not have rights.
    ret->setDebugValue("CREATED_BY", app, true);
    ret->setDebugValue("CREATED_FOR", scope, true);
    return ret;
}

IWorkUnit* CWorkUnitFactory::createWorkUnit(const char *app, const char *scope, ISecManager *secmgr, ISecUser *secuser)
{
    StringBuffer wuid("W");
    char result[32];
    time_t ltime;
    time( &ltime );
    tm *today = localtime( &ltime );   // MORE - this is not threadsafe. But I probably don't care that much!
    strftime(result, sizeof(result), "%Y%m%d-%H%M%S", today);
    wuid.append(result);
    if (workUnitTraceLevel > 1)
        PrintLog("createWorkUnit created %s", wuid.str());
    IWorkUnit* ret = createNamedWorkUnit(wuid.str(), app, scope, secmgr, secuser);
    if (workUnitTraceLevel > 1)
        PrintLog("createWorkUnit created %s", ret->queryWuid());
    addTimeStamp(ret, SSTglobal, NULL, StWhenCreated);
    return ret;
}

bool CWorkUnitFactory::deleteWorkUnit(const char * wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (workUnitTraceLevel > 1)
        PrintLog("deleteWorkUnit %s", wuid);
    StringBuffer wuRoot;
    getXPath(wuRoot, wuid);
    Owned<CLocalWorkUnit> cw = _updateWorkUnit(wuid, secmgr, secuser);
    if (!checkWuSecAccess(*cw.get(), secmgr, secuser, SecAccess_Full, "delete", true, true))
        return false;
    try
    {
        cw->cleanupAndDelete(true, true);
    }
    catch (IException *E)
    {
        StringBuffer s;
        LOG(MCexception(E, MSGCLS_warning), E, s.append("Exception during deleteWorkUnit: ").append(wuid).str());
        E->Release();
        return false;
    }
    removeWorkUnitFromAllQueues(wuid); //known active workunits wouldn't make it this far
    return true;
}

IConstWorkUnit* CWorkUnitFactory::openWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    StringBuffer wuidStr(wuid);
    wuidStr.trim();
    if (wuidStr.length() && ('w' == wuidStr.charAt(0)))
        wuidStr.setCharAt(0, 'W');

    if (!wuidStr.length() || ('W' != wuidStr.charAt(0)))
    {
        if (workUnitTraceLevel > 1)
            PrintLog("openWorkUnit %s invalid WUID", nullText(wuidStr.str()));

        return NULL;
    }

    if (workUnitTraceLevel > 1)
        PrintLog("openWorkUnit %s", wuidStr.str());
    Owned<IConstWorkUnit> wu = _openWorkUnit(wuid, secmgr, secuser);
    if (wu)
    {
        if (!checkWuSecAccess(*wu, secmgr, secuser, SecAccess_Read, "opening", true, true))
            return NULL; // Actually throws exception on failure, so won't reach here
        return wu.getClear();
    }
    else
    {
        if (workUnitTraceLevel > 0)
            PrintLog("openWorkUnit %s not found", wuidStr.str());
        return NULL;
    }
}

IWorkUnit* CWorkUnitFactory::updateWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (workUnitTraceLevel > 1)
        PrintLog("updateWorkUnit %s", wuid);
    Owned<CLocalWorkUnit> wu = _updateWorkUnit(wuid, secmgr, secuser);
    if (wu)
    {
        if (!checkWuSecAccess(*wu.get(), secmgr, secuser, SecAccess_Write, "updating", true, true))
            return NULL;
        return &wu->lockRemote(false);
    }
    else
    {
        if (workUnitTraceLevel > 0)
            PrintLog("updateWorkUnit %s not found", wuid);
        return NULL;
    }
}

int CWorkUnitFactory::setTracingLevel(int newLevel)
{
    if (newLevel)
        PrintLog("Setting workunit trace level to %d", newLevel);
    int level = workUnitTraceLevel;
    workUnitTraceLevel = newLevel;
    return level;
}

void CWorkUnitFactory::descheduleAllWorkUnits(ISecManager *secmgr, ISecUser *secuser)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Schedule", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if(!conn) return;
    Owned<IPropertyTree> root(conn->queryRoot()->getBranch("."));
    KeptAtomTable entries;
    Owned<IPropertyTreeIterator> iter(root->getElements("*/*/*/*"));
    StringBuffer wuid;
    for(iter->first(); iter->isValid(); iter->next())
    {
        char const * entry = iter->query().queryName();
        if(!entries.find(entry))
        {
            entries.addAtom(entry);
            ncnameUnescape(entry, wuid.clear());
            Owned<IWorkUnit> wu = updateWorkUnit(wuid, secmgr, secuser);
            if(wu && (wu->getState() == WUStateWait))
                wu->setState(WUStateCompleted);
        }
    }
    bool more;
    do more = root->removeProp("*"); while(more);
}

IConstQuerySetQueryIterator* CWorkUnitFactory::getQuerySetQueriesSorted( WUQuerySortField *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                            WUQuerySortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                            const void *filterbuf,  // (appended) string values for filters
                                            unsigned startoffset,
                                            unsigned maxnum,
                                            __int64 *cachehint,
                                            unsigned *total,
                                            const MapStringTo<bool> *_subset)
{
    struct PostFilters
    {
        WUQueryFilterBoolean activatedFilter;
        WUQueryFilterBoolean suspendedByUserFilter;
        PostFilters()
        {
            activatedFilter = WUQFSAll;
            suspendedByUserFilter = WUQFSAll;
        };
    } postFilters;

    class CQuerySetQueriesPager : public CSimpleInterface, implements IElementsPager
    {
        StringAttr querySet;
        StringAttr xPath;
        StringAttr sortOrder;
        PostFilters postFilters;
        StringArray unknownAttributes;
        const MapStringTo<bool> *subset;

        void populateQueryTree(const IPropertyTree* querySetTree, IPropertyTree* queryTree)
        {
            const char* querySetId = querySetTree->queryProp("@id");
            VStringBuffer path("Query%s", xPath.get());
            Owned<IPropertyTreeIterator> iter = querySetTree->getElements(path.str());
            ForEach(*iter)
            {
                IPropertyTree &query = iter->query();

                bool activated = false;
                const char* queryId = query.queryProp("@id");
                if (queryId && *queryId)
                {
                    if (subset)
                    {
                        VStringBuffer match("%s/%s", querySetId, queryId);
                        if (!subset->getValue(match))
                            continue;
                    }
                    VStringBuffer aliasXPath("Alias[@id='%s']", queryId);
                    IPropertyTree *alias = querySetTree->queryPropTree(aliasXPath.str());
                    if (alias)
                        activated = true;
                }
                if (activated && (postFilters.activatedFilter == WUQFSNo))
                    continue;
                if (!activated && (postFilters.activatedFilter == WUQFSYes))
                    continue;
                if ((postFilters.suspendedByUserFilter == WUQFSNo) && query.hasProp(getEnumText(WUQSFSuspendedByUser,querySortFields)))
                    continue;
                if ((postFilters.suspendedByUserFilter == WUQFSYes) && !query.hasProp(getEnumText(WUQSFSuspendedByUser,querySortFields)))
                    continue;

                IPropertyTree *queryWithSetId = queryTree->addPropTree("Query", createPTreeFromIPT(&query));
                queryWithSetId->setProp("@querySetId", querySetId);
                queryWithSetId->setPropBool("@activated", activated);
            }
        }
        IRemoteConnection* populateQueryTree(IPropertyTree* queryTree)
        {
            StringBuffer querySetXPath("QuerySets");
            if (!querySet.isEmpty())
                querySetXPath.appendf("/QuerySet[@id=\"%s\"]", querySet.get());
            Owned<IRemoteConnection> conn = querySDS().connect(querySetXPath.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
            if (!conn)
                return NULL;

            if (querySet.isEmpty())
            {
                Owned<IPropertyTreeIterator> querySetIter = conn->queryRoot()->getElements("*");
                ForEach(*querySetIter)
                    populateQueryTree(&querySetIter->query(), queryTree);
            }
            else
                populateQueryTree(conn->queryRoot(), queryTree);
            return conn.getClear();
        }
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CQuerySetQueriesPager(const char* _querySet, const char* _xPath, const char *_sortOrder, PostFilters& _postFilters, StringArray& _unknownAttributes, const MapStringTo<bool> *_subset)
            : querySet(_querySet), xPath(_xPath), sortOrder(_sortOrder), subset(_subset)
        {
            postFilters.activatedFilter = _postFilters.activatedFilter;
            postFilters.suspendedByUserFilter = _postFilters.suspendedByUserFilter;
            ForEachItemIn(x, _unknownAttributes)
                unknownAttributes.append(_unknownAttributes.item(x));
        }
        virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
        {
            Owned<IPropertyTree> elementTree = createPTree("Queries");
            Owned<IRemoteConnection> conn = populateQueryTree(elementTree);
            if (!conn)
                return NULL;
            Owned<IPropertyTreeIterator> iter = elementTree->getElements("*");
            if (!iter)
                return NULL;
            sortElements(iter, sortOrder.get(), NULL, NULL, unknownAttributes, elements);
            return conn.getClear();
        }
        virtual bool allMatchingElementsReceived() { return true; } //For now, dali always returns all of matched Queries.
    };
    StringAttr querySet;
    StringBuffer xPath;
    StringBuffer so;
    StringArray unknownAttributes;
    if (filters)
    {
        const char *fv = (const char *)filterbuf;
        for (unsigned i=0;filters[i]!=WUQSFterm;i++) {
            int fmt = filters[i];
            int subfmt = (fmt&0xff);
            if (subfmt==WUQSFQuerySet)
                querySet.set(fv);
            else if ((subfmt==WUQSFmemoryLimit) || (subfmt==WUQSFtimeLimit) || (subfmt==WUQSFwarnTimeLimit) || (subfmt==WUQSFpriority))
                xPath.append('[').append(getEnumText(subfmt,querySortFields)).append(">=").append(fv).append("]");
            else if ((subfmt==WUQSFmemoryLimitHi) || (subfmt==WUQSFtimeLimitHi) || (subfmt==WUQSFwarnTimeLimitHi) || (subfmt==WUQSFpriorityHi))
                xPath.append('[').append(getEnumText(subfmt,querySortFields)).append("<=").append(fv).append("]");
            else if (subfmt==WUQSFActivited)
                postFilters.activatedFilter = (WUQueryFilterBoolean) atoi(fv);
            else if (subfmt==WUQSFSuspendedByUser)
                postFilters.suspendedByUserFilter = (WUQueryFilterBoolean) atoi(fv);
            else if (!fv || !*fv)
                unknownAttributes.append(getEnumText(subfmt,querySortFields));
            else {
                xPath.append('[').append(getEnumText(subfmt,querySortFields)).append('=');
                if (fmt&WUQSFnocase)
                    xPath.append('?');
                if (fmt&WUQSFnumeric)
                    xPath.append('#');
                if (fmt&WUQSFwild)
                    xPath.append('~');
                xPath.append('"').append(fv).append("\"]");
            }
            fv = fv + strlen(fv)+1;
        }
    }
    if (sortorder) {
        for (unsigned i=0;sortorder[i]!=WUQSFterm;i++) {
            if (so.length())
                so.append(',');
            int fmt = sortorder[i];
            if (fmt&WUQSFreverse)
                so.append('-');
            if (fmt&WUQSFnocase)
                so.append('?');
            if (fmt&WUQSFnumeric)
                so.append('#');
            so.append(getEnumText(fmt&0xff,querySortFields));
        }
    }
    IArrayOf<IPropertyTree> results;
    Owned<IElementsPager> elementsPager = new CQuerySetQueriesPager(querySet.get(), xPath.str(), so.length()?so.str():NULL, postFilters, unknownAttributes, _subset);
    Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,NULL,"",cachehint,results,total,NULL);
    return new CConstQuerySetQueryIterator(results);
}

bool CWorkUnitFactory::isAborting(const char *wuid) const
{
    VStringBuffer apath("/WorkUnitAborts/%s", wuid);
    try
    {
        Owned<IRemoteConnection> acon = querySDS().connect(apath.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
        if (acon)
            return acon->queryRoot()->getPropInt(NULL) != 0;
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
    }
    return false;
}

void CWorkUnitFactory::clearAborting(const char *wuid)
{
    VStringBuffer apath("/WorkUnitAborts/%s", wuid);
    try
    {
        Owned<IRemoteConnection> acon = querySDS().connect(apath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_LOCK_SUB, SDS_LOCK_TIMEOUT);
        if (acon)
            acon->close(true);
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
    }
}

static CriticalSection deleteDllLock;
static Owned<IWorkQueueThread> deleteDllWorkQ;

static void asyncRemoveDll(const char * name)
{
    CriticalBlock b(deleteDllLock);
    if (!deleteDllWorkQ)
        deleteDllWorkQ.setown(createWorkQueueThread());
    deleteDllWorkQ->post(new asyncRemoveDllWorkItem(name));
}

static void asyncRemoveFile(const char * ip, const char * name)
{
    CriticalBlock b(deleteDllLock);
    if (!deleteDllWorkQ)
        deleteDllWorkQ.setown(createWorkQueueThread());
    deleteDllWorkQ->post(new asyncRemoveRemoteFileWorkItem(ip, name));
}

class CDaliWorkUnitFactory : public CWorkUnitFactory, implements IDaliClientShutdown
{
public:
    IMPLEMENT_IINTERFACE_USING(CWorkUnitFactory);
    CDaliWorkUnitFactory()
    {
        // Assumes dali client configuration has already been done
        sdsManager = &querySDS();
        session = myProcessSession();
        addShutdownHook(*this);
    }
    ~CDaliWorkUnitFactory()
    {
        removeShutdownHook(*this);
    }
    virtual unsigned validateRepository(bool fixErrors)
    {
        return 0;
    }
    virtual void deleteRepository(bool recreate)
    {
        Owned<IRemoteConnection> conn = sdsManager->connect("/WorkUnits", session, RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        if (conn)
            conn->close(true);
        conn.setown(sdsManager->connect("/GraphProgress", session, RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        if (conn)
            conn->close(true);
    }
    virtual void createRepository()
    {
        // Nothing to do
    }
    virtual const char *queryStoreType() const
    {
        return "Dali";
    }

    virtual CLocalWorkUnit *_createWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection *conn;
        conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_CREATE_UNIQUE, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        conn->queryRoot()->setPropInt("@wuidVersion", WUID_VERSION);
        return new CDaliWorkUnit(conn, secmgr, secuser);
    }

    virtual CLocalWorkUnit* _openWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, 0, SDS_LOCK_TIMEOUT);
        if (conn)
            return new CDaliWorkUnit(conn, secmgr, secuser);
        else
            return NULL;
    }

    virtual CLocalWorkUnit* _updateWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_LOCK_SUB, SDS_LOCK_TIMEOUT);
        if (conn)
            return new CDaliWorkUnit(conn, secmgr, secuser);
        else
            return NULL;
    }

    virtual IWorkUnit* getGlobalWorkUnit(ISecManager *secmgr, ISecUser *secuser)
    {
        // MORE - should it check security?
        StringBuffer wuRoot;
        getXPath(wuRoot, GLOBAL_WORKUNIT);
        IRemoteConnection* conn = sdsManager->connect(wuRoot.str(), session, RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        Owned<CLocalWorkUnit> cw = new CDaliWorkUnit(conn, (ISecManager *) NULL, NULL);
        return &cw->lockRemote(false);
    }
    virtual IConstWorkUnitIterator* getWorkUnitsByOwner(const char * owner, ISecManager *secmgr, ISecUser *secuser)
    {
        StringBuffer path("*");
        if (owner && *owner)
            path.append("[@submitID=?~\"").append(owner).append("\"]");
        return _getWorkUnitsByXPath(path.str(), secmgr, secuser);
    }
    IConstWorkUnitIterator* getScheduledWorkUnits(ISecManager *secmgr, ISecUser *secuser)
    {
        StringBuffer path("*");
        path.append("[@state=\"").append(getEnumText(WUStateScheduled, states)).append("\"]");
        return _getWorkUnitsByXPath(path.str(), secmgr, secuser);
    }
    virtual void clientShutdown();

    virtual unsigned numWorkUnits()
    {
        Owned<IRemoteConnection> conn = sdsManager->connect("/WorkUnits", session, 0, SDS_LOCK_TIMEOUT);
        if (!conn)
            return 0;
        IPropertyTree *root = conn->queryRoot();
        return root->numChildren();
    }

    IConstWorkUnitIterator* getWorkUnitsSorted( WUSortField sortorder, // field to sort by (and flags for desc sort etc)
                                                WUSortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                                const void *filterbuf,  // (appended) string values for filters
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                __int64 *cachehint,
                                                unsigned *total,
                                                ISecManager *secmgr,
                                                ISecUser *secuser)
    {
        class CWorkUnitsPager : public CSimpleInterface, implements IElementsPager
        {
            StringAttr xPath;
            StringAttr sortOrder;
            StringAttr nameFilterLo;
            StringAttr nameFilterHi;
            StringArray unknownAttributes;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWorkUnitsPager(const char* _xPath, const char *_sortOrder, const char* _nameFilterLo, const char* _nameFilterHi, StringArray& _unknownAttributes)
                : xPath(_xPath), sortOrder(_sortOrder), nameFilterLo(_nameFilterLo), nameFilterHi(_nameFilterHi)
            {
                ForEachItemIn(x, _unknownAttributes)
                    unknownAttributes.append(_unknownAttributes.item(x));
            }
            virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
            {
                Owned<IRemoteConnection> conn = querySDS().connect("WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
                if (!conn)
                    return NULL;
                Owned<IPropertyTreeIterator> iter = conn->getElements(xPath);
                if (!iter)
                    return NULL;
                sortElements(iter, sortOrder.get(), nameFilterLo.get(), nameFilterHi.get(), unknownAttributes, elements);
                return conn.getClear();
            }
            virtual bool allMatchingElementsReceived() { return true; }//For now, dali always returns all of matched WUs.
        };
        class CScopeChecker : public CSimpleInterface, implements ISortedElementsTreeFilter
        {
            UniqueScopes done;
            ISecManager *secmgr;
            ISecUser *secuser;
            CriticalSection crit;
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CScopeChecker(ISecManager *_secmgr,ISecUser *_secuser)
            {
                secmgr = _secmgr;
                secuser = _secuser;
            }
            bool isOK(IPropertyTree &tree)
            {
                const char *scopename = tree.queryProp("@scope");
                if (!scopename||!*scopename)
                    return true;

                {
                    CriticalBlock block(crit);
                    const bool *b = done.getValue(scopename);
                    if (b)
                        return *b;
                }
                bool ret = checkWuScopeSecAccess(scopename,secmgr,secuser,SecAccess_Read,"iterating",false,false);
                {
                    // conceivably could have already been checked and added, but ok.
                    CriticalBlock block(crit);
                    done.setValue(scopename,ret);
                }
                return ret;
            }
        };
        Owned<ISortedElementsTreeFilter> sc = new CScopeChecker(secmgr,secuser);
        StringBuffer query;
        StringBuffer so;
        StringAttr namefilter("*");
        StringAttr namefilterlo;
        StringAttr namefilterhi;
        StringArray unknownAttributes;
        if (filters) {
            const char *fv = (const char *)filterbuf;
            for (unsigned i=0;filters[i]!=WUSFterm;i++) {
                int fmt = filters[i];
                int subfmt = (fmt&0xff);
                if (subfmt==WUSFwuid)
                    namefilterlo.set(fv);
                else if (subfmt==WUSFwuidhigh)
                    namefilterhi.set(fv);
                else if (subfmt==WUSFwildwuid)
                    namefilter.set(fv);
                else if (subfmt==WUSFappvalue)
                {
                    query.append("[Application/").append(fv).append("=?~\"");
                    fv = fv + strlen(fv)+1;
                    query.append(fv).append("\"]");
                }
                else if (!fv || !*fv)
                {
                    unknownAttributes.append(getEnumText(subfmt,workunitSortFields));
                    if (subfmt==WUSFtotalthortime)
                        sortorder = (WUSortField) (sortorder | WUSFnumeric);
                }
                else {
                    query.append('[').append(getEnumText(subfmt,workunitSortFields)).append('=');
                    if (fmt&WUSFnocase)
                        query.append('?');
                    if (fmt&WUSFwild)
                        query.append('~');
                    query.append('"').append(fv).append("\"]");
                }
                fv = fv + strlen(fv)+1;
            }
        }
        query.insert(0, namefilter.get());
        if (sortorder)
        {
            if (so.length())
                so.append(',');
            if (sortorder & WUSFreverse)
                so.append('-');
            if (sortorder & WUSFnocase)
                so.append('?');
            if (sortorder & WUSFnumeric)
                so.append('#');
            so.append(getEnumText(sortorder&0xff,workunitSortFields));
        }
        IArrayOf<IPropertyTree> results;
        Owned<IElementsPager> elementsPager = new CWorkUnitsPager(query.str(), so.length()?so.str():NULL, namefilterlo.get(), namefilterhi.get(), unknownAttributes);
        Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,secmgr?sc:NULL,"",cachehint,results,total,NULL);
        return new CConstWUArrayIterator(results);
    }

    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, bool returnOnWaitState)
    {
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        Owned<WorkUnitWaiter> waiter = new WorkUnitWaiter(wuRoot.str());
        LocalIAbortHandler abortHandler(*waiter);
        WUState ret = WUStateUnknown;
        Owned<IRemoteConnection> conn = sdsManager->connect(wuRoot.str(), session, 0, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            unsigned start = msTick();
            loop
            {
                ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
                switch (ret)
                {
                case WUStateCompiled:
                case WUStateUploadingFiles:
                    if (!compiled)
                        break;
                    // fall into
                case WUStateCompleted:
                case WUStateFailed:
                case WUStateAborted:
                    waiter->unsubscribe();
                    return ret;
                case WUStateWait:
                    if(returnOnWaitState)
                    {
                        waiter->unsubscribe();
                        return ret;
                    }
                    break;
                case WUStateCompiling:
                case WUStateRunning:
                case WUStateDebugPaused:
                case WUStateDebugRunning:
                case WUStateBlocked:
                case WUStateAborting:
                    if (queryDaliServerVersion().compare("2.1")>=0)
                    {
                        SessionId agent = conn->queryRoot()->getPropInt64("@agentSession", -1);
                        if((agent>0) && querySessionManager().sessionStopped(agent, 0))
                        {
                            waiter->unsubscribe();
                            conn->reload();
                            ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
                            bool isEcl = false;
                            switch (ret)
                            {
                                case WUStateCompiling:
                                    isEcl = true;
                                    // drop into
                                case WUStateRunning:
                                case WUStateBlocked:
                                    ret = WUStateFailed;
                                    break;
                                case WUStateAborting:
                                    ret = WUStateAborted;
                                    break;
                                default:
                                    return ret;
                            }
                            WARNLOG("_waitForWorkUnit terminated: %" I64F "d state = %d",(__int64)agent,(int)ret);
                            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                            Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
                            wu->setState(ret);
                            Owned<IWUException> e = wu->createException();
                            e->setExceptionCode(isEcl ? 1001 : 1000);
                            e->setExceptionMessage(isEcl ? "EclServer terminated unexpectedly" : "Workunit terminated unexpectedly");
                            return ret;
                        }
                    }
                    break;
                }
                unsigned waited = msTick() - start;
                if (timeout==-1)
                {
                    waiter->wait(20000);  // recheck state every 20 seconds even if no timeout, in case eclagent has crashed.
                    if (waiter->aborted)
                    {
                        ret = WUStateUnknown;  // MORE - throw an exception?
                        break;
                    }
                }
                else if (waited > timeout || !waiter->wait(timeout-waited))
                {
                    ret = WUStateUnknown;  // MORE - throw an exception?
                    break;
                }
                conn->reload();
            }
        }
        waiter->unsubscribe();
        return ret;
    }

protected:
    class WorkUnitWaiter : public CInterface, implements ISDSSubscription, implements IAbortHandler
    {
        Semaphore changed;
        SubscriptionId change;
    public:
        IMPLEMENT_IINTERFACE;

        WorkUnitWaiter(const char *xpath)
        {
            change = querySDS().subscribe(xpath, *this, false);
            aborted = false;
        }
        ~WorkUnitWaiter()
        {
            assertex(change==0);
        }

        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            changed.signal();
        }
        bool wait(unsigned timeout)
        {
            return changed.wait(timeout) && !aborted;
        }
        bool onAbort()
        {
            aborted = true;
            changed.signal();
            return false;
        }
        void unsubscribe()
        {
            querySDS().unsubscribe(change);
            change = 0;
        }
        bool aborted;
    };

    IConstWorkUnitIterator * _getWorkUnitsByXPath(const char *xpath, ISecManager *secmgr, ISecUser *secuser)
    {
        Owned<IRemoteConnection> conn = sdsManager->connect("/WorkUnits", session, 0, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            CDaliVersion serverVersionNeeded("3.2");
            Owned<IPropertyTreeIterator> iter(queryDaliServerVersion().compare(serverVersionNeeded) < 0 ?
                conn->queryRoot()->getElements(xpath) :
                conn->getElements(xpath));
            return createSecureConstWUIterator(iter.getClear(), secmgr, secuser);
        }
        else
            return NULL;
    }

    ISDSManager *sdsManager;
    SessionId session;
};

extern WORKUNIT_API IConstWorkUnitIterator *createSecureConstWUIterator(IConstWorkUnitIterator *iter, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr)
        return new CSecureConstWUIterator(iter, secmgr, secuser);
    else
        return iter;
}

extern WORKUNIT_API IConstWorkUnitIterator *createSecureConstWUIterator(IPropertyTreeIterator *iter, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr)
        return new CSecureConstWUIterator(new CConstWUIterator(iter), secmgr, secuser);
    else
        return new CConstWUIterator(iter);
}


static CriticalSection factoryCrit;
static Owned<ILoadedDllEntry> workunitServerPlugin;  // NOTE - unload AFTER the factory is released!
static Owned<IWorkUnitFactory> factory;

void CDaliWorkUnitFactory::clientShutdown()
{
    CriticalBlock b(factoryCrit);
    factory.clear();
}

void clientShutdownWorkUnit()
{
    CriticalBlock b(factoryCrit);
    factory.clear();
}


extern WORKUNIT_API void setWorkUnitFactory(IWorkUnitFactory * _factory)
{
    CriticalBlock b(factoryCrit);
    factory.setown(_factory);
}

extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory()
{
    if (!factory)
    {
        CriticalBlock b(factoryCrit);
        if (!factory)   // NOTE - this "double test" paradigm is not guaranteed threadsafe on modern systems/compilers - I think in this instance that is harmless even in the (extremely) unlikely event that it resulted in the setown being called twice.
        {
            const char *forceEnv = getenv("FORCE_DALI_WORKUNITS");
            bool forceDali = forceEnv && !strieq(forceEnv, "off") && !strieq(forceEnv, "0");
            Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software/WorkUnitsServer", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
            // MORE - arguably should be looking in the config section that corresponds to the dali we connected to. If you want to allow some dalis to be configured to use a WU server and others not.
            if (conn && !forceDali)
            {
                const IPropertyTree *ptree = conn->queryRoot();
                const char *pluginName = ptree->queryProp("@plugin");
                if (!pluginName)
                    throw makeStringException(WUERR_WorkunitPluginError, "WorkUnitsServer information missing plugin name");
                workunitServerPlugin.setown(createDllEntry(pluginName, false, NULL));
                if (!workunitServerPlugin)
                    throw makeStringExceptionV(WUERR_WorkunitPluginError, "WorkUnitsServer: failed to load plugin %s", pluginName);
                WorkUnitFactoryFactory pf = (WorkUnitFactoryFactory) workunitServerPlugin->getEntry("createWorkUnitFactory");
                if (!pf)
                    throw makeStringExceptionV(WUERR_WorkunitPluginError, "WorkUnitsServer: function createWorkUnitFactory not found in plugin %s", pluginName);
                factory.setown(pf(ptree));
                if (!factory)
                    throw makeStringExceptionV(WUERR_WorkunitPluginError, "WorkUnitsServer: createWorkUnitFactory returned NULL in plugin %s", pluginName);
            }
            else
                factory.setown(new CDaliWorkUnitFactory());
        }
    }
    return factory.getLink();
}

// A SecureWorkUnitFactory allows the security params to be supplied once to the factory rather than being supplied to each call.
// They can still be supplied if you want...

class CSecureWorkUnitFactory : public CInterface, implements IWorkUnitFactory
{
public:
    IMPLEMENT_IINTERFACE;

    CSecureWorkUnitFactory(IWorkUnitFactory *_baseFactory, ISecManager *_secMgr, ISecUser *_secUser)
        : baseFactory(_baseFactory), defaultSecMgr(_secMgr), defaultSecUser(_secUser)
    {
    }
    virtual unsigned validateRepository(bool fix)
    {
        return baseFactory->validateRepository(fix);
    }
    virtual void deleteRepository(bool recreate)
    {
        return baseFactory->deleteRepository(recreate);
    }
    virtual void createRepository()
    {
        return baseFactory->createRepository();
    }
    virtual const char *queryStoreType() const
    {
        return baseFactory->queryStoreType();
    }
    virtual StringArray &getUniqueValues(WUSortField field, const char *prefix, StringArray &result) const
    {
        return baseFactory->getUniqueValues(field, prefix, result);
    }

    virtual IWorkUnit* createNamedWorkUnit(const char *wuid, const char *app, const char *user, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->createNamedWorkUnit(wuid, app, user, secMgr, secUser);
    }
    virtual IWorkUnit* createWorkUnit(const char *app, const char *user, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->createWorkUnit(app, user, secMgr, secUser);
    }
    virtual bool deleteWorkUnit(const char * wuid, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->deleteWorkUnit(wuid, secMgr, secUser);
    }
    virtual IConstWorkUnit* openWorkUnit(const char *wuid, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->openWorkUnit(wuid, secMgr, secUser);
    }
    virtual IWorkUnit* updateWorkUnit(const char *wuid, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->updateWorkUnit(wuid, secMgr, secUser);
    }
    virtual IWorkUnit * getGlobalWorkUnit(ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->getGlobalWorkUnit(secMgr, secUser);
    }

    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->getWorkUnitsByOwner(owner, secMgr, secUser);
    }
    virtual IConstWorkUnitIterator * getScheduledWorkUnits(ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->getScheduledWorkUnits(secMgr, secUser);
    }
    virtual void descheduleAllWorkUnits(ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        baseFactory->descheduleAllWorkUnits(secMgr, secUser);
    }

    virtual int setTracingLevel(int newLevel)
    {
        return baseFactory->setTracingLevel(newLevel);
    }

    virtual IConstWorkUnitIterator* getWorkUnitsSorted( WUSortField sortorder, // field to sort by
                                                        WUSortField *filters,   // NULL or list of fields to filter on (terminated by WUSFterm)
                                                        const void *filterbuf,  // (appended) string values for filters
                                                        unsigned startoffset,
                                                        unsigned maxnum,
                                                        __int64 *cachehint,
                                                        unsigned *total,
                                                        ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->getWorkUnitsSorted(sortorder,filters,filterbuf,startoffset,maxnum,cachehint, total, secMgr, secUser);
    }

    virtual IConstQuerySetQueryIterator* getQuerySetQueriesSorted( WUQuerySortField *sortorder,
                                                WUQuerySortField *filters,
                                                const void *filterbuf,
                                                unsigned startoffset,
                                                unsigned maxnum,
                                                __int64 *cachehint,
                                                unsigned *total,
                                                const MapStringTo<bool> *subset)
    {
        // MORE - why no security?
        return baseFactory->getQuerySetQueriesSorted(sortorder,filters,filterbuf,startoffset,maxnum,cachehint,total,subset);
    }

    virtual unsigned numWorkUnits()
    {
        return baseFactory->numWorkUnits();
    }

    virtual bool isAborting(const char *wuid) const
    {
        return baseFactory->isAborting(wuid);
    }

    virtual void clearAborting(const char *wuid)
    {
        baseFactory->clearAborting(wuid);
    }
    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, bool returnOnWaitState)
    {
        return baseFactory->waitForWorkUnit(wuid, timeout, compiled, returnOnWaitState);
    }
private:
    Owned<IWorkUnitFactory> baseFactory;
    Linked<ISecManager> defaultSecMgr;
    Linked<ISecUser> defaultSecUser;
};

extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory(ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return new CSecureWorkUnitFactory(getWorkUnitFactory(), secmgr, secuser);
    else
        return getWorkUnitFactory();
}

//==========================================================================================

class CStringPTreeIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryProp(NULL)); return s; }
};

class CStringPTreeTagIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeTagIterator(IPropertyTreeIterator *p) : it(p) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryName()); return s; }
};

class CStringPTreeAttrIterator : public CInterface, implements IStringIterator
{
    Owned<IPropertyTreeIterator> it;
    StringAttr name;
public:
    IMPLEMENT_IINTERFACE;
    CStringPTreeAttrIterator(IPropertyTreeIterator *p, const char *_name) : it(p), name(_name) {};
    virtual bool first() { return it->first(); }
    virtual bool next() { return it->next(); }
    virtual bool isValid() { return it->isValid(); }
    virtual IStringVal & str(IStringVal &s) { s.set(it->query().queryProp(name)); return s; }
};
//==========================================================================================

CLocalWorkUnit::CLocalWorkUnit(ISecManager *secmgr, ISecUser *secuser)
{
    clearCached(false);
    secMgr.set(secmgr);
    secUser.set(secuser);
    workflowIteratorCached = false;
    resultsCached = false;
    graphsCached = false;
    temporariesCached = false;
    variablesCached = false;
    exceptionsCached = false;
    pluginsCached = false;
    librariesCached = false;
    activitiesCached = false;
    webServicesInfoCached = false;
    roxieQueryInfoCached = false;
}

void CLocalWorkUnit::clearCached(bool clearTree)
{
    query.clear();
    webServicesInfo.clear();
    workflowIterator.clear();

    graphs.kill();
    results.kill();
    variables.kill();
    plugins.kill();
    libraries.kill();
    exceptions.kill();
    temporaries.kill();
    statistics.kill();
    appvalues.kill();

    if (clearTree)
        p.clear();

    workflowIteratorCached = false;
    resultsCached = false;
    graphsCached = false;
    temporariesCached = false;
    variablesCached = false;
    exceptionsCached = false;
    pluginsCached = false;
    librariesCached = false;
    activitiesCached = false;
    webServicesInfoCached = false;
    roxieQueryInfoCached = false;
}

void CLocalWorkUnit::loadPTree(IPropertyTree *ptree)
{
    clearCached(false);
    p.setown(ptree);
}

void CLocalWorkUnit::beforeDispose()
{
    try
    {
        unsubscribe();

        clearCached(true);

        userDesc.clear();
        secMgr.clear();
        secUser.clear();
    }
    catch (IException *E) { LOG(MCexception(E, MSGCLS_warning), E, "Exception during ~CLocalWorkUnit"); E->Release(); }
}

void CLocalWorkUnit::cleanupAndDelete(bool deldll, bool deleteOwned, const StringArray *deleteExclusions)
{
    MTIME_SECTION(queryActiveTimer(), "WUDELETE cleanupAndDelete total");
    // Delete any related things in SDS etc that might otherwise be forgotten
    if (p->getPropBool("@protected", false))
        throw MakeStringException(WUERR_WorkunitProtected, "%s: Workunit is protected",p->queryName());
    switch (getState())
    {
    case WUStateAborted:
    case WUStateCompleted:
    case WUStateFailed:
    case WUStateArchived:
        break;
    case WUStateCompiled:
        if (getAction()==WUActionRun || getAction()==WUActionUnknown)
            throw MakeStringException(WUERR_WorkunitActive, "%s: Workunit is active. Please abort before deleting this workunit.",p->queryName());
        break;
    case WUStateWait:
        throw MakeStringException(WUERR_WorkunitScheduled, "%s: Workunit is scheduled",p->queryName());
    default:
        throw MakeStringException(WUERR_WorkunitActive, "%s: Workunit is active. Please abort before deleting this workunit.",p->queryName());
        break;
    }
    if (getIsQueryService())
    {
        Owned<IPropertyTree> registry = getQueryRegistryRoot();
        if (registry)
        {
            VStringBuffer xpath("QuerySet/Query[@wuid='%s']", p->queryName());
            if (registry->hasProp(xpath.str()))
                throw MakeStringException(WUERR_WorkunitPublished, "%s: Workunit is published",p->queryName());
        }
    }
    try
    {
        if (deldll && !p->getPropBool("@isClone", false))
        {
            Owned<IConstWUQuery> q = getQuery();
            if (q)
            {
                Owned<IConstWUAssociatedFileIterator> iter = &q->getAssociatedFiles();
                SCMStringBuffer name;
                ForEach(*iter)
                {
                    IConstWUAssociatedFile & cur = iter->query();
                    cur.getNameTail(name);
                    if (!deleteExclusions || (NotFound == deleteExclusions->find(name.str())))
                    {
                        Owned<IDllEntry> entry = queryDllServer().getEntry(name.str());
                        if (entry.get())
                            asyncRemoveDll(name.str());
                        else
                        {
                            SCMStringBuffer ip, localPath;
                            cur.getName(localPath);
                            cur.getIp(ip);
                            asyncRemoveFile(ip.str(), localPath.str());
                        }
                    }
                }
            }
        }
        factory->clearAborting(queryWuid());
        deleteTempFiles(NULL, deleteOwned, true); // all, any remaining.
    }
    catch(IException *E)
    {
        StringBuffer s;
        LOG(MCexception(E, MSGCLS_warning), E, s.append("Exception during cleanupAndDelete: ").append(p->queryName()).str());
        E->Release();
    }
    catch (...) 
    { 
        WARNLOG("Unknown exception during cleanupAndDelete: %s", p->queryName()); 
    }
}

void CLocalWorkUnit::setTimeScheduled(const IJlibDateTime &val)
{
    SCMStringBuffer strval;
    val.getGmtString(strval);
    p->setProp("@timescheduled",strval.str());
}

IJlibDateTime & CLocalWorkUnit::getTimeScheduled(IJlibDateTime &val) const
{
    StringBuffer str;
    p->getProp("@timescheduled",str);
    if(str.length())
        val.setGmtString(str.str());
    return val; 
}

bool modifyAndWriteWorkUnitXML(char const * wuid, StringBuffer & buf, StringBuffer & extra, IFileIO * fileio)
{
    // kludge in extra chunks of XML such as GraphProgress and GeneratedDlls
    if(extra.length())
    {
        size32_t l = (size32_t)strlen(wuid);
        size32_t p = buf.length()-l-4; // bit of a kludge
        assertex(memcmp(buf.str()+p+2,wuid,l)==0);
        StringAttr tail(buf.str()+p);
        buf.setLength(p);
        buf.append(extra);
        buf.append(tail);
    }
    return (fileio->write(0,buf.length(),buf.str()) == buf.length());
}

bool CLocalWorkUnit::archiveWorkUnit(const char *base,bool del,bool ignoredllerrors,bool deleteOwned)
{
    CriticalBlock block(crit);
    StringBuffer path(base);
    if (!p)
        return false;
    const char *wuid = p->queryName();
    if (!wuid||!*wuid)
        return false;
    addPathSepChar(path).append(wuid).append(".xml");
    Owned<IFile> file = createIFile(path.str());
    if (!file)
        return false;
    Owned<IFileIO> fileio = file->open(IFOcreate);
    if (!fileio)
        return false;

    StringBuffer buf;
    exportWorkUnitToXML(this, buf, false, false, true);

    StringBuffer extraWorkUnitXML;
    StringBuffer xpath("/GraphProgress/");
    xpath.append(wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (conn)
    {
        Owned<IPropertyTree> tmp = createPTree("GraphProgress");
        mergePTree(tmp,conn->queryRoot());
        toXML(tmp,extraWorkUnitXML,1,XML_Format);
        conn->close();
    }

    Owned<IConstWUQuery> q = getQuery();
    if (!q)
    {
        if (!modifyAndWriteWorkUnitXML(wuid, buf, extraWorkUnitXML, fileio))
           return false;
        if (del)
        {
            if (getState()==WUStateUnknown)
                setState(WUStateArchived);  // to allow delete
            cleanupAndDelete(false,deleteOwned);    // no query, may as well delete 
        }
        return false;
    }

    StringArray deleteExclusions; // associated files not to delete, added if failure to copy
    Owned<IConstWUAssociatedFileIterator> iter = &q->getAssociatedFiles();
    Owned<IPropertyTree> generatedDlls = createPTree("GeneratedDlls");
    ForEach(*iter)
    {
        IConstWUAssociatedFile & cur = iter->query();
        SCMStringBuffer name;
        cur.getNameTail(name);
        if (name.length())
        {
            Owned<IDllEntry> entry = queryDllServer().getEntry(name.str());
            SCMStringBuffer curPath, curIp;
            cur.getName(curPath);
            cur.getIp(curIp);
            SocketEndpoint curEp(curIp.str());
            RemoteFilename curRfn;
            curRfn.setPath(curEp, curPath.str());
            StringBuffer dst(base);
            addPathSepChar(dst);
            curRfn.getTail(dst);
            Owned<IFile> dstFile = createIFile(dst.str());
            if (entry.get())
            {
                Owned<IException> exception;
                Owned<IDllLocation> loc;
                Owned<IPropertyTree> generatedDllBranch = createPTree();
                generatedDllBranch->setProp("@name", entry->queryName());
                generatedDllBranch->setProp("@kind", entry->queryKind());
                try
                {
                    loc.setown(entry->getBestLocation()); //throws exception if no readable locations
                }
                catch(IException * e)
                {
                    exception.setown(e);
                    loc.setown(entry->getBestLocationCandidate()); //this will be closest of the unreadable locations
                }
                RemoteFilename filename;
                loc->getDllFilename(filename);
                if (!exception)
                {
                    Owned<IFile> srcfile = createIFile(filename);
                    try
                    {
                        if (dstFile->exists())
                        {
                            if (streq(srcfile->queryFilename(), dstFile->queryFilename()))
                                deleteExclusions.append(name.str()); // restored workunit, referencing archive location for query dll (no longer true post HPCC-11191 fix)
                            // still want to delete if already archived but there are source file copies
                        }
                        else
                            copyFile(dstFile, srcfile);
                    }
                    catch(IException * e)
                    {
                        exception.setown(e);
                    }
                }
                if (exception)
                {
                    if (ignoredllerrors)
                    {
                        EXCLOG(exception.get(), "archiveWorkUnit (copying associated file)");
                        //copy failed, so don't delete the registred dll files
                        deleteExclusions.append(name.str());
                    }
                    else
                        throw exception.getClear();
                }
                // Record Associated path to restore back to
                StringBuffer restorePath;
                curRfn.getRemotePath(restorePath);
                generatedDllBranch->setProp("@location", restorePath.str());
                generatedDlls->addPropTree("GeneratedDll", generatedDllBranch.getClear());
            }
            else // no generated dll entry
            {
                Owned<IFile> srcFile = createIFile(curRfn);
                try
                {
                    copyFile(dstFile, srcFile);
                }
                catch (IException *e)
                {
                    VStringBuffer msg("Failed to archive associated file '%s' to destination '%s'", srcFile->queryFilename(), dstFile->queryFilename());
                    EXCLOG(e, msg.str());
                    e->Release();
                    deleteExclusions.append(name.str());
                }
            }
        }
    }
    iter.clear();
    if (generatedDlls->numChildren())
        toXML(generatedDlls, extraWorkUnitXML, 1, XML_Format);

    if (!modifyAndWriteWorkUnitXML(wuid, buf, extraWorkUnitXML, fileio))
       return false;

    if (del)
    {
        //setState(WUStateArchived);    // this isn't useful as about to delete it!
        q.clear();
        cleanupAndDelete(true, deleteOwned, &deleteExclusions);
    }

    return true;
}

IPropertyTree * pruneBranch(IPropertyTree * from, char const * xpath)
{
    Owned<IPropertyTree> ret;
    IPropertyTree * branch = from->queryPropTree(xpath);
    if(branch) {
        ret.setown(createPTreeFromIPT(branch));
        from->removeTree(branch);
    }
    return ret.getClear();
}

bool restoreWorkUnit(const char *base,const char *wuid)
{
    StringBuffer path(base);
    if (!wuid||!*wuid)
        return false;
    addPathSepChar(path).append(wuid).append(".xml");
    Owned<IFile> file = createIFile(path.str());
    if (!file)
        return false;
    Owned<IFileIO> fileio = file->open(IFOread);
    if (!fileio)
        return false;
    Owned<IPropertyTree> pt = createPTree(*fileio);
    if (!pt)
        return false;
    CDateTime dt;
    dt.setNow();
    StringBuffer dts;
    dt.getString(dts);
    pt->setProp("@restoredDate", dts.str());
    VStringBuffer xpath("/WorkUnits/%s", wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
    {
        ERRLOG("restoreWorkUnit could not create to %s", xpath.str());
        return false;
    }
    IPropertyTree *root = conn->queryRoot();
    if (root->hasChildren())
    {
        ERRLOG("restoreWorkUnit WUID %s already exists", wuid);
        return false;
    }
    Owned<IPropertyTree> gprogress = pruneBranch(pt, "GraphProgress[1]");
    Owned<IPropertyTree> generatedDlls = pruneBranch(pt, "GeneratedDlls[1]");
    Owned<IPropertyTree> associatedFiles;
    IPropertyTree *srcAssociated = pt->queryPropTree("Query/Associated");
    if (srcAssociated)
        associatedFiles.setown(createPTreeFromIPT(srcAssociated));
    root->setPropTree(NULL, pt.getClear());
    conn.clear();

    // now kludge back GraphProgress and GeneratedDlls
    if (gprogress)
    {
        VStringBuffer xpath("/GraphProgress/%s", wuid);
        conn.setown(querySDS().connect(xpath, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT));
        if (conn)
        {
            IPropertyTree *groot = conn->queryRoot();
            if (groot->hasChildren())
                WARNLOG("restoreWorkUnit WUID %s graphprogress already exists, replacing",wuid);
            groot->setPropTree(NULL, gprogress.getClear());
        }
    }

    if (generatedDlls)
    {
        Owned<IPropertyTreeIterator> dlls = generatedDlls->getElements("GeneratedDll");
        for(dlls->first(); dlls->isValid(); dlls->next())
        {
            IPropertyTree & dll = dlls->query();
            char const * name = dll.queryProp("@name");
            char const * kind = dll.queryProp("@kind");
            char const * location = dll.queryProp("@location");
            Owned<IDllEntry> got = queryDllServer().getEntry(name);
            if (!got)
            {
                RemoteFilename dstRfn;
                dstRfn.setRemotePath(location);
                StringBuffer srcPath(base);
                addPathSepChar(srcPath);
                dstRfn.getTail(srcPath);
                OwnedIFile srcFile = createIFile(srcPath);
                OwnedIFile dstFile = createIFile(dstRfn);
                copyFile(dstFile, srcFile);
                queryDllServer().registerDll(name, kind, location);
            }
        }
    }
    if (associatedFiles)
    {
        Owned<IPropertyTreeIterator> associated = associatedFiles->getElements("*");
        ForEach(*associated)
        {
            IPropertyTree &file = associated->query();
            const char *filename = file.queryProp("@filename");
            SocketEndpoint ep(file.queryProp("@ip"));
            RemoteFilename rfn;
            rfn.setPath(ep, filename);
            OwnedIFile dstFile = createIFile(rfn);
            StringBuffer srcPath(base), name;
            addPathSepChar(srcPath);
            rfn.getTail(name);
            srcPath.append(name);
            if (generatedDlls)
            {
                VStringBuffer gDllPath("GeneratedDll[@name=\"%s\"]", name.str());
                if (generatedDlls->hasProp(gDllPath))
                    continue; // generated dlls handled separately - see above
            }

            OwnedIFile srcFile = createIFile(srcPath);
            if (srcFile->exists())
            {
                try
                {
                    copyFile(dstFile, srcFile);
                }
                catch (IException *e)
                {
                    VStringBuffer msg("Failed to restore associated file '%s' to destination '%s'", srcFile->queryFilename(), dstFile->queryFilename());
                    EXCLOG(e, msg.str());
                    e->Release();
                }
            }
        }
    }
    return true;
}

void CLocalWorkUnit::loadXML(const char *xml)
{
    CriticalBlock block(crit);
    clearCached(true);
    assertex(xml);
    p.setown(createPTreeFromXMLString(xml));
}

void CLocalWorkUnit::serialize(MemoryBuffer &tgt)
{
    CriticalBlock block(crit);
    StringBuffer x;
    tgt.append(exportWorkUnitToXML(this, x, false, false, false).str());
}

void CLocalWorkUnit::deserialize(MemoryBuffer &src)
{
    CriticalBlock block(crit);
    StringAttr value;
    src.read(value);
    loadXML(value);
}

void CLocalWorkUnit::requestAbort()
{
    CriticalBlock block(crit);
    abortWorkUnit(p->queryName());
}

void CLocalWorkUnit::unlockRemote()
{
    CriticalBlock block(crit);
    locked.unlock();
    _unlockRemote();
}

IWorkUnit &CLocalWorkUnit::lockRemote(bool commit)
{
    if (secMgr)
        checkWuSecAccess(*this, secMgr.get(), secUser.get(), SecAccess_Write, "write lock", true, true);
    locked.lock();
    CriticalBlock block(crit);
    if (commit)
    {
        try
        {
            _lockRemote();
        }
        catch (IException *E)
        {
            StringBuffer s;
            PrintLog("Failed to get write lock on workunit: %s", E->errorMessage(s).str());
            locked.unlock();
            throw;
        }
    }
    return *new CLockedWorkUnit(LINK(this));
}

void CLocalWorkUnit::commit()
{
    // Nothing to do if not backed by a persistent store
}

IWorkUnit& CLocalWorkUnit::lock()
{
    return lockRemote(true);
}

const char *CLocalWorkUnit::queryWuid() const
{
    CriticalBlock block(crit);
    return p->queryName();
}

unsigned CLocalWorkUnit::getDebugAgentListenerPort() const
{
    CriticalBlock block(crit);
    return p->getPropInt("@DebugListenerPort", 0);
}

unsigned CLocalWorkUnit::getTotalThorTime() const
{
    CriticalBlock block(crit);
    return nanoToMilli(extractTimeCollatable(p->queryProp("@totalThorTime"), false));
}

void CLocalWorkUnit::setDebugAgentListenerPort(unsigned port)
{
    CriticalBlock block(crit);
    p->setPropInt("@DebugListenerPort", port);
}

IStringVal& CLocalWorkUnit::getDebugAgentListenerIP(IStringVal &ip) const
{
    CriticalBlock block(crit);
    ip.set(p->queryProp("@DebugListenerIP"));
    return ip;
}

void CLocalWorkUnit::setDebugAgentListenerIP(const char * ip)
{
    CriticalBlock block(crit);
    p->setProp("@DebugListenerIP", ip);
}

IStringVal& CLocalWorkUnit::getSecurityToken(IStringVal &str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@token"));
    return str;
}

void CLocalWorkUnit::setSecurityToken(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@token", value);
}

bool CLocalWorkUnit::getRunningGraph(IStringVal &graphName, WUGraphIDType &subId) const
{
    // Only implemented in derived classes
    return false;
}

void CLocalWorkUnit::setJobName(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@jobName", value);
}

const char *CLocalWorkUnit::queryJobName() const
{
    CriticalBlock block(crit);
    const char *ret = p->queryProp("@jobName");
    if (!ret)
        ret = "";
    return ret;
}

void CLocalWorkUnit::setClusterName(const char *value)
{
    CriticalBlock block(crit);
    p->setProp("@clusterName", value);
}

const char *CLocalWorkUnit::queryClusterName() const
{
    CriticalBlock block(crit);
    const char *ret = p->queryProp("@clusterName");
    if (!ret)
        ret = "";
    return ret;
}

void CLocalWorkUnit::setAllowedClusters(const char *value)
{
    setDebugValue("allowedclusters",value, true);
}

IStringVal& CLocalWorkUnit::getAllowedClusters(IStringVal &str) const
{
    CriticalBlock block(crit);
    getDebugValue("allowedclusters",str);
    if (str.length()!=0)
        return str;
    str.set(p->queryProp("@clusterName"));
    return str;
}

void CLocalWorkUnit::setAllowAutoQueueSwitch(bool val)
{ 
    setDebugValueInt("allowautoqueueswitch",val?1:0,true);
}
    
bool CLocalWorkUnit::getAllowAutoQueueSwitch() const
{ 
    CriticalBlock block(crit);
    return getDebugValueBool("allowautoqueueswitch",false);
}


void CLocalWorkUnit::setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash)
{
    StringBuffer suffix;

    if (name && *name)
        setApplicationValue("LibraryModule", "name", name, true);
    setApplicationValueInt("LibraryModule", "interfaceHash", interfaceHash, true);
    setApplicationValueInt("LibraryModule", "definitionHash", definitionHash, true);
    setApplicationValue("LibraryModule", "platform", appendLibrarySuffix(suffix).str(), true);
}

void CLocalWorkUnit::remoteCheckAccess(IUserDescriptor *user, bool writeaccess) const
{
    unsigned auditflags = DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED;
    if (writeaccess)
        auditflags |= DALI_LDAP_WRITE_WANTED;
    int perm = 255;
    const char *scopename = p->queryProp("@scope");
    if (scopename&&*scopename) {
        if (!user)
            user = queryUserDescriptor();
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0) {
            if (perm==-1) 
                perm = 255;
            else 
                perm = 0;
        }
    }
    if (!HASREADPERMISSION(perm))
        throw MakeStringException(WUERR_WorkunitAccessDenied, "Read access denied for workunit %s", queryWuid());
    if (writeaccess && !HASWRITEPERMISSION(perm))
        throw MakeStringException(WUERR_WorkunitAccessDenied, "Write access denied for workunit %s", queryWuid());
}


void CLocalWorkUnit::setUser(const char * value) 
{ 
    CriticalBlock block(crit);
    p->setProp("@submitID", value); 
}

const char *CLocalWorkUnit::queryUser() const
{
    CriticalBlock block(crit);
    const char *ret = p->queryProp("@submitID");
    if (!ret)
        ret = "";
    return ret;
}

void CLocalWorkUnit::setWuScope(const char * value) 
{ 
    if (value && *value)
    {
        if (checkWuScopeSecAccess(value, secMgr.get(), secUser.get(), SecAccess_Write, "Change Scope", true, true))
        {
            CriticalBlock block(crit);
            p->setProp("@scope", value);
        }
    }
}

const char *CLocalWorkUnit::queryWuScope() const
{
    CriticalBlock block(crit);
    const char *ret = p->queryProp("@scope");
    if (!ret)
        ret = "";
    return ret;
}

mapEnums priorityClasses[] = {
   { PriorityClassUnknown, "unknown" },
   { PriorityClassLow, "low" },
   { PriorityClassNormal, "normal" },
   { PriorityClassHigh, "high" },
   { PriorityClassSize, NULL },
};

void CLocalWorkUnit::setPriority(WUPriorityClass cls) 
{
    CriticalBlock block(crit);
    setEnum(p, "@priorityClass", cls, priorityClasses);
}

WUPriorityClass CLocalWorkUnit::getPriority() const 
{
    CriticalBlock block(crit);
    return (WUPriorityClass) getEnum(p, "@priorityClass", priorityClasses);
}

void CLocalWorkUnit::setState(WUState value) 
{
    if (value==WUStateAborted || value==WUStatePaused || value==WUStateCompleted || value==WUStateFailed || value==WUStateSubmitted || value==WUStateWait)
    {
        if (factory)
            factory->clearAborting(queryWuid());
    }
    CriticalBlock block(crit);
    setEnum(p, "@state", value, states);
    if (getDebugValueBool("monitorWorkunit", false))
    {
        switch(value)
        {
        case WUStateAborted:
            FLLOG(MCoperatorWarning, "Workunit %s aborted", p->queryName());
            break;
        case WUStateCompleted:
            FLLOG(MCoperatorProgress, "Workunit %s completed", p->queryName());
            break;
        case WUStateFailed:
            FLLOG(MCoperatorProgress, "Workunit %s failed", p->queryName());
            break;
        }
    }
    p->removeProp("@stateEx");
}

void CLocalWorkUnit::setStateEx(const char * text)
{
    CriticalBlock block(crit);
    p->setProp("@stateEx", text);
}

void CLocalWorkUnit::setAgentSession(__int64 sessionId)
{
    CriticalBlock block(crit);
    p->setPropInt64("@agentSession", sessionId);
}

bool CLocalWorkUnit::getIsQueryService() const 
{
    CriticalBlock block(crit);
    return p->getPropBool("@isQueryService", false);
}

void CLocalWorkUnit::setIsQueryService(bool value) 
{
    CriticalBlock block(crit);
    p->setPropBool("@isQueryService", value);
}

void CLocalWorkUnit::checkAgentRunning(WUState & state) 
{
    if (queryDaliServerVersion().compare("2.1")<0)
        return;
    switch(state)
    {
    case WUStateRunning:
    case WUStateDebugPaused:
    case WUStateDebugRunning:
    case WUStateBlocked:
    case WUStateAborting:
    case WUStateCompiling:
    case WUStatePaused:
        {
            SessionId agent = getAgentSession();
            if((agent>0) && querySessionManager().sessionStopped(agent, 0))
            {
                forceReload();
                state = (WUState) getEnum(p, "@state", states);
                bool isecl=state==WUStateCompiling;
                if (aborting())
                    state = WUStateAborted;
                else if (state==WUStateRunning || state==WUStatePaused || state==WUStateDebugPaused || state==WUStateDebugRunning || state==WUStateBlocked || state==WUStateCompiling)
                    state = WUStateFailed;
                else
                    return;
                WARNLOG("checkAgentRunning terminated: %" I64F "d state = %d",(__int64)agent,(int)state);
                Owned<IWorkUnit> w = &lock();
                w->setState(state);
                Owned<IWUException> e = w->createException();
                WUAction action = w->getAction();
                switch (action)
                {
                    case WUActionPause:
                    case WUActionPauseNow:
                    case WUActionResume:
                        w->setAction(WUActionUnknown);
                }
                if(isecl)
                {
                    e->setExceptionCode(1001);
                    e->setExceptionMessage("EclServer terminated unexpectedly");
                }
                else
                {
                    e->setExceptionCode(1000);
                    e->setExceptionMessage("Workunit terminated unexpectedly");
                }
            }
        }
    }
}

WUState CLocalWorkUnit::getState() const 
{
    CriticalBlock block(crit);
    WUState state = (WUState) getEnum(p, "@state", states);
    switch (state)
    {
    case WUStateRunning:
    case WUStateDebugPaused:
    case WUStateDebugRunning:
    case WUStateBlocked:
    case WUStateCompiling:
        if (aborting())
            state = WUStateAborting;
        break;
    case WUStateSubmitted:
        if (aborting())
            state = WUStateAborted;
        break;
    }
    const_cast<CLocalWorkUnit *>(this)->checkAgentRunning(state); //need const_cast as will change state if agent has died
    return state;
}

IStringVal& CLocalWorkUnit::getStateEx(IStringVal & str) const 
{
    CriticalBlock block(crit);
    str.set(p->queryProp("@stateEx"));
    return str;
}

__int64 CLocalWorkUnit::getAgentSession() const
{
    CriticalBlock block(crit);
    return p->getPropInt64("@agentSession", -1);
}

unsigned CLocalWorkUnit::getAgentPID() const
{
    CriticalBlock block(crit);
    return p->getPropInt("@agentPID", -1);
}

const char * CLocalWorkUnit::queryStateDesc() const
{
    // MORE - not sure about this - may prefer a separate interface
    CriticalBlock block(crit);
    try
    {
        return getEnumText(getState(), states);
    }
    catch (...)
    {
        return "???";
    }
}

void CLocalWorkUnit::setAction(WUAction value) 
{
    CriticalBlock block(crit);
    setEnum(p, "Action", value, actions);
}

WUAction CLocalWorkUnit::getAction() const 
{
    CriticalBlock block(crit);
    return (WUAction) getEnum(p, "Action", actions);
}

const char *CLocalWorkUnit::queryActionDesc() const
{
    CriticalBlock block(crit);
    return p->queryProp("Action");
}

IStringVal& CLocalWorkUnit::getApplicationValue(const char *app, const char *propname, IStringVal &str) const
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    str.set(p->queryProp(prop.str())); 
    return str;
}

int CLocalWorkUnit::getApplicationValueInt(const char *app, const char *propname, int defVal) const
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    return p->getPropInt(prop.str(), defVal); 
}

IConstWUAppValueIterator& CLocalWorkUnit::getApplicationValues() const
{
    CriticalBlock block(crit);
    appvalues.load(p,"Application/*");
    return *new CArrayIteratorOf<IConstWUAppValue,IConstWUAppValueIterator> (appvalues, 0, (IConstWorkUnit *) this);
}


void CLocalWorkUnit::setApplicationValue(const char *app, const char *propname, const char *value, bool overwrite)
{
    CriticalBlock block(crit);
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);
    if (overwrite || !p->hasProp(prop.str()))
    {
        StringBuffer sp;
        p->setProp(sp.append("Application").str(), ""); 
        p->setProp(sp.append('/').append(app).str(), ""); 
        p->setProp(prop.str(), value); 
    }
}

void CLocalWorkUnit::setApplicationValueInt(const char *app, const char *propname, int value, bool overwrite)
{
    VStringBuffer s("%d", value);
    setApplicationValue(app, propname, s, overwrite);
}

void CLocalWorkUnit::setPriorityLevel(int level) 
{
    CriticalBlock block(crit);
    p->setPropInt("PriorityFlag",  level);
}

int CLocalWorkUnit::getPriorityLevel() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("PriorityFlag"); 
}

int calcPriorityValue(const IPropertyTree * p)
{
    int priority = p->getPropInt("PriorityFlag");
    switch((WUPriorityClass) getEnum(p, "@priorityClass", priorityClasses))
    {
    case PriorityClassLow:
        priority -= 100;
        break;
    case PriorityClassHigh:
        priority += 100;
        break;
    }
    return priority;
}


int CLocalWorkUnit::getPriorityValue() const 
{
    CriticalBlock block(crit);
    return calcPriorityValue(p);
}

void CLocalWorkUnit::setRescheduleFlag(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("RescheduleFlag", (int) value); 
}

bool CLocalWorkUnit::getRescheduleFlag() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("RescheduleFlag") != 0; 
}

class NullIStringIterator : public CInterface, extends IStringIterator
{
public:
    IMPLEMENT_IINTERFACE;
    bool first() { return false; }
    bool next()  { return false; }
    bool isValid()  { return false; }
    IStringVal & str(IStringVal & str) { return str; }

};

ClusterType getClusterType(const char * platform, ClusterType dft)
{
    if (stricmp(platform, "thor") == 0)
        return ThorLCRCluster;
    if (stricmp(platform, "thorlcr") == 0)
        return ThorLCRCluster;
    if (stricmp(platform, "hthor") == 0)
        return HThorCluster;
    if (stricmp(platform, "roxie") == 0)
        return RoxieCluster;
    return dft;
}

const char *clusterTypeString(ClusterType clusterType, bool lcrSensitive)
{
    switch (clusterType)
    {
    case ThorLCRCluster:
        if (lcrSensitive)
            return "thorlcr";
        return "thor";
    case RoxieCluster:
        return "roxie";
    case HThorCluster:
        return "hthor";
    }
    throwUnexpected();
}

IPropertyTree *queryRoxieProcessTree(IPropertyTree *environment, const char *process)
{
    if (!process || !*process)
        return NULL;
    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", process);
    return environment->queryPropTree(xpath.str());
}

void getRoxieProcessServers(IPropertyTree *roxie, SocketEndpointArray &endpoints)
{
    if (!roxie)
        return;
    Owned<IPropertyTreeIterator> servers = roxie->getElements("RoxieServerProcess");
    ForEach(*servers)
    {
        IPropertyTree &server = servers->query();
        const char *netAddress = server.queryProp("@netAddress");
        if (netAddress && *netAddress)
        {
            SocketEndpoint ep(netAddress, server.getPropInt("@port", 9876));
            endpoints.append(ep);
        }
    }
}

void getRoxieProcessServers(const char *process, SocketEndpointArray &servers)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return;
    Owned<IPropertyTree> root = &env->getPTree();
    getRoxieProcessServers(queryRoxieProcessTree(root, process), servers);
}

class CEnvironmentClusterInfo: public CInterface, implements IConstWUClusterInfo
{
    StringAttr name;
    StringAttr serverQueue;
    StringAttr agentQueue;
    StringAttr roxieProcess;
    SocketEndpointArray roxieServers;
    StringAttr thorQueue;
    StringArray thorProcesses;
    StringArray primaryThorProcesses;
    StringAttr prefix;
    StringAttr ldapUser;
    StringBuffer ldapPassword;
    ClusterType platform;
    unsigned clusterWidth;
    unsigned roxieRedundancy;
    unsigned channelsPerNode;
    int roxieReplicateOffset;

public:
    IMPLEMENT_IINTERFACE;
    CEnvironmentClusterInfo(const char *_name, const char *_prefix, IPropertyTree *agent, IArrayOf<IPropertyTree> &thors, IPropertyTree *roxie)
        : name(_name), prefix(_prefix), roxieRedundancy(0), channelsPerNode(0), roxieReplicateOffset(1)
    {
        StringBuffer queue;
        if (thors.ordinality())
        {
            thorQueue.set(getClusterThorQueueName(queue.clear(), name));
            clusterWidth = 0;
            bool isMultiThor = (thors.length() > 1);
            ForEachItemIn(i,thors) 
            {
                IPropertyTree &thor = thors.item(i);
                const char* thorName = thor.queryProp("@name");
                thorProcesses.append(thorName);
                if (!isMultiThor)
                    primaryThorProcesses.append(thorName);
                else
                {
                    const char *nodeGroup = thor.queryProp("@nodeGroup");
                    if (!nodeGroup || strieq(nodeGroup, thorName))
                        primaryThorProcesses.append(thorName);
                }
                unsigned nodes = thor.getCount("ThorSlaveProcess");
                if (!nodes)
                    throw MakeStringException(WUERR_MismatchClusterSize,"CEnvironmentClusterInfo: Thor cluster can not have 0 slave processes");
                unsigned ts = nodes * thor.getPropInt("@slavesPerNode", 1);
                if (clusterWidth && (ts!=clusterWidth)) 
                    throw MakeStringException(WUERR_MismatchClusterSize,"CEnvironmentClusterInfo: mismatched thor sizes in cluster");
                clusterWidth = ts;
                bool islcr = !thor.getPropBool("@Legacy");
                if (!islcr)
                    throw MakeStringException(WUERR_MismatchThorType,"CEnvironmentClusterInfo: Legacy Thor no longer supported");
            }
            platform = ThorLCRCluster;
        }
        else if (roxie)
        {
            roxieProcess.set(roxie->queryProp("@name"));
            platform = RoxieCluster;
            getRoxieProcessServers(roxie, roxieServers);
            clusterWidth = roxieServers.length();
            ldapUser.set(roxie->queryProp("@ldapUser"));
            StringBuffer encPassword = roxie->queryProp("@ldapPassword");
            if (encPassword.length())
                decrypt(ldapPassword, encPassword);
            const char *redundancyMode = roxie->queryProp("@slaveConfig");
            if (redundancyMode && *redundancyMode)
            {
                unsigned dataCopies = roxie->getPropInt("@numDataCopies", 1);
                if (strieq(redundancyMode, "overloaded"))
                    channelsPerNode = roxie->getPropInt("@channelsPernode", 1);
                else if (strieq(redundancyMode, "full redundancy"))
                {
                    roxieRedundancy = dataCopies-1;
                    roxieReplicateOffset = 0;
                }
                else if (strieq(redundancyMode, "cyclic redundancy"))
                {
                    roxieRedundancy = dataCopies-1;
                    channelsPerNode = dataCopies;
                    roxieReplicateOffset = roxie->getPropInt("@cyclicOffset", 1);
                }
            }
        }
        else 
        {
            clusterWidth = 1;
            platform = HThorCluster;
        }

        if (agent)
        {
            assertex(!roxie);
            agentQueue.set(getClusterEclAgentQueueName(queue.clear(), name));
        }
        else if (roxie)
            agentQueue.set(getClusterRoxieQueueName(queue.clear(), name));
        // MORE - does this need to be conditional?
        serverQueue.set(getClusterEclCCServerQueueName(queue.clear(), name));
    }

    IStringVal & getName(IStringVal & str) const
    {
        str.set(name.get());
        return str;
    }
    IStringVal & getScope(IStringVal & str) const
    {
        str.set(prefix.get());
        return str;
    }
    IStringVal & getAgentQueue(IStringVal & str) const
    {
        str.set(agentQueue);
        return str;
    }
    virtual IStringVal & getServerQueue(IStringVal & str) const
    {
        str.set(serverQueue);
        return str;
    }
    IStringVal & getThorQueue(IStringVal & str) const
    {
        str.set(thorQueue);
        return str;
    }
    unsigned getSize() const 
    {
        return clusterWidth;
    }
    virtual ClusterType getPlatform() const
    {
        return platform;
    }
    IStringVal & getRoxieProcess(IStringVal & str) const
    {
        str.set(roxieProcess.get());
        return str;
    }
    const StringArray & getThorProcesses() const
    {
        return thorProcesses;
    }
    const StringArray & getPrimaryThorProcesses() const
    {
        return primaryThorProcesses;
    }

    const SocketEndpointArray & getRoxieServers() const
    {
        return roxieServers;
    }
    unsigned getRoxieRedundancy() const
    {
        return roxieRedundancy;
    }
    unsigned getChannelsPerNode() const
    {
        return channelsPerNode;
    }
    int getRoxieReplicateOffset() const
    {
        return roxieReplicateOffset;
    }
    const char *getLdapUser() const
    {
        return ldapUser.get();
    }
    virtual const char *getLdapPassword() const
    {
        return ldapPassword.str();
    }
};

IStringVal &getProcessQueueNames(IStringVal &ret, const char *process, const char *type, const char *suffix)
{
    if (process)
    {
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> env = factory->openEnvironment();
        if (env)
        {
            Owned<IPropertyTree> root = &env->getPTree();
            StringBuffer queueNames;
            StringBuffer xpath;
            xpath.appendf("%s[@process=\"%s\"]", type, process);
            Owned<IPropertyTreeIterator> targets = root->getElements("Software/Topology/Cluster");
            ForEach(*targets)
            {
                IPropertyTree &target = targets->query();
                if (target.hasProp(xpath))
                {
                    if (queueNames.length())
                        queueNames.append(',');
                    queueNames.append(target.queryProp("@name")).append(suffix);
                }
            }
            ret.set(queueNames);
        }
    }
    return ret;
}

#define ROXIE_QUEUE_EXT ".roxie"
#define THOR_QUEUE_EXT ".thor"
#define ECLCCSERVER_QUEUE_EXT ".eclserver"
#define ECLSERVER_QUEUE_EXT ECLCCSERVER_QUEUE_EXT
#define ECLSCHEDULER_QUEUE_EXT ".eclscheduler"
#define ECLAGENT_QUEUE_EXT ".agent"

extern WORKUNIT_API IStringVal &getEclCCServerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclCCServerProcess", ECLCCSERVER_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getEclServerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclServerProcess", ECLSERVER_QUEUE_EXT); // shares queue name with EclCCServer
}

extern WORKUNIT_API IStringVal &getEclSchedulerQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclSchedulerProcess", ECLSCHEDULER_QUEUE_EXT); // Shares deployment/config with EclCCServer
}

extern WORKUNIT_API IStringVal &getAgentQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "EclAgentProcess", ECLAGENT_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getRoxieQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "RoxieCluster", ROXIE_QUEUE_EXT);
}

extern WORKUNIT_API IStringVal &getThorQueueNames(IStringVal &ret, const char *process)
{
    return getProcessQueueNames(ret, process, "ThorCluster", THOR_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterThorQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(THOR_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterThorGroupName(StringBuffer &ret, const char *cluster)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env)
    {
        Owned<IPropertyTree> root = &env->getPTree();
        StringBuffer path;
        path.append("Software/ThorCluster[@name=\"").append(cluster).append("\"]");
        IPropertyTree * child = root->queryPropTree(path);
        if (child)
            getClusterGroupName(*child, ret);
    }

    return ret;
}

extern WORKUNIT_API StringBuffer &getClusterRoxieQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ROXIE_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclCCServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLCCSERVER_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLSERVER_QUEUE_EXT);
}

extern WORKUNIT_API StringBuffer &getClusterEclAgentQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLAGENT_QUEUE_EXT);
}

extern WORKUNIT_API IStringIterator *getTargetClusters(const char *processType, const char *processName)
{
    Owned<CStringArrayIterator> ret = new CStringArrayIterator;
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env)
    {
        Owned<IPropertyTree> root = &env->getPTree();
        StringBuffer xpath;
        xpath.appendf("%s", processType ? processType : "*");
        if (processName && *processName)
            xpath.appendf("[@process=\"%s\"]", processName);
        Owned<IPropertyTreeIterator> targets = root->getElements("Software/Topology/Cluster");
        ForEach(*targets)
        {
            IPropertyTree &target = targets->query();
            if (target.hasProp(xpath))
            {
                ret->append(target.queryProp("@name"));
            }
        }
    }
    return ret.getClear();
}

extern WORKUNIT_API bool isProcessCluster(const char *process)
{
    if (!process || !*process)
        return false;
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return false;

    Owned<IPropertyTree> root = &env->getPTree();
    VStringBuffer xpath("Software/*Cluster[@name=\"%s\"]", process);
    return root->hasProp(xpath.str());
}

extern WORKUNIT_API bool isProcessCluster(const char *remoteDali, const char *process)
{
    if (!remoteDali || !*remoteDali)
        return isProcessCluster(process);
    if (!process || !*process)
        return false;
    Owned<INode> remote = createINode(remoteDali, 7070);
    if (!remote)
        return false;

    //Cannot use getEnvironmentFactory() since it is using a remotedali
    VStringBuffer xpath("Environment/Software/*Cluster[@name=\"%s\"]/@name", process);
    try
    {
        Owned<IPropertyTreeIterator> clusters = querySDS().getElementsRaw(xpath, remote, 1000*60*1);
        return clusters->first();
    }
    catch (IException *E)
    {
        StringBuffer msg;
        E->errorMessage(msg);
        DBGLOG("Exception validating cluster %s/%s: %s", remoteDali, xpath.str(), msg.str());
        E->Release();
    }
    return true;
}

IConstWUClusterInfo* getTargetClusterInfo(IPropertyTree *environment, IPropertyTree *cluster)
{
    const char *clustname = cluster->queryProp("@name");

    // MORE - at the moment configenf specifies eclagent and thor queues by (in effect) placing an 'example' thor or eclagent in the topology 
    // that uses the queue that will be used.
    // We should and I hope will change that, at which point the code below gets simpler

    StringBuffer prefix(cluster->queryProp("@prefix"));
    prefix.toLowerCase();

    StringBuffer xpath;
    StringBuffer querySetName;
    
    IPropertyTree *agent = NULL;
    const char *agentName = cluster->queryProp("EclAgentProcess/@process");
    if (agentName) 
    {
        xpath.clear().appendf("Software/EclAgentProcess[@name=\"%s\"]", agentName);
        agent = environment->queryPropTree(xpath.str());
    }
    Owned<IPropertyTreeIterator> ti = cluster->getElements("ThorCluster");
    IArrayOf<IPropertyTree> thors;
    ForEach(*ti) 
    {
        const char *thorName = ti->query().queryProp("@process");
        if (thorName) 
        {
            xpath.clear().appendf("Software/ThorCluster[@name=\"%s\"]", thorName);
            thors.append(*environment->getPropTree(xpath.str()));
        }
    }
    const char *roxieName = cluster->queryProp("RoxieCluster/@process");
    return new CEnvironmentClusterInfo(clustname, prefix, agent, thors, queryRoxieProcessTree(environment, roxieName));
}

IPropertyTree* getTopologyCluster(Owned<IPropertyTree> &envRoot, const char *clustname)
{
    if (!clustname || !*clustname)
        return NULL;
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return NULL;

    envRoot.setown(&env->getPTree());
    StringBuffer xpath;
    xpath.appendf("Software/Topology/Cluster[@name=\"%s\"]", clustname);
    return envRoot->getPropTree(xpath.str());
}

bool validateTargetClusterName(const char *clustname)
{
    Owned<IPropertyTree> envRoot;
    Owned<IPropertyTree> cluster = getTopologyCluster(envRoot, clustname);
    return (cluster.get()!=NULL);
}

IConstWUClusterInfo* getTargetClusterInfo(const char *clustname)
{
    Owned<IPropertyTree> envRoot;
    Owned<IPropertyTree> cluster = getTopologyCluster(envRoot, clustname);
    if (!cluster)
        return NULL;
    return getTargetClusterInfo(envRoot, cluster);
}

unsigned getEnvironmentClusterInfo(CConstWUClusterInfoArray &clusters)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return 0;

    Owned<IPropertyTree> root = &env->getPTree();
    return getEnvironmentClusterInfo(root, clusters);
}

unsigned getEnvironmentClusterInfo(IPropertyTree* environmentRoot, CConstWUClusterInfoArray &clusters)
{
    if (!environmentRoot)
        return 0;

    Owned<IPropertyTreeIterator> clusterIter = environmentRoot->getElements("Software/Topology/Cluster");
    ForEach(*clusterIter)
    {
        IPropertyTree &node = clusterIter->query();
        Owned<IConstWUClusterInfo> cluster = getTargetClusterInfo(environmentRoot, &node);
        clusters.append(*cluster.getClear());
    }
    return clusters.ordinality();
}

const char *getTargetClusterComponentName(const char *clustname, const char *processType, StringBuffer &name)
{
    if (!clustname)
        return NULL;

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return NULL;

    Owned<IPropertyTree> root = &env->getPTree();
    StringBuffer xpath;

    xpath.appendf("Software/Topology/Cluster[@name=\"%s\"]", clustname);
    Owned<IPropertyTree> cluster = root->getPropTree(xpath.str());
    if (!cluster) 
        return NULL;

    StringBuffer xpath1;
    xpath1.appendf("%s/@process", processType);
    name.append(cluster->queryProp(xpath1.str()));
    return name.str();
}

unsigned getEnvironmentThorClusterNames(StringArray &thorNames, StringArray &groupNames, StringArray &targetNames, StringArray &queueNames)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return 0;

    Owned<IPropertyTree> root = &env->getPTree();
    Owned<IPropertyTreeIterator> allTargets = root->getElements("Software/Topology/Cluster");
    ForEach(*allTargets)
    {
        IPropertyTree &target = allTargets->query();
        const char *targetName = target.queryProp("@name");
        if (targetName && *targetName)
        {
            Owned<IPropertyTreeIterator> thorClusters = target.getElements("ThorCluster");
            ForEach(*thorClusters)
            {
                const char *thorName = thorClusters->query().queryProp("@process");
                VStringBuffer query("Software/ThorCluster[@name=\"%s\"]",thorName);
                IPropertyTree *thorCluster = root->queryPropTree(query.str());
                if (thorCluster)
                {
                    const char *groupName = thorCluster->queryProp("@nodeGroup");
                    if (!groupName||!*groupName)
                        groupName = thorName;
                    thorNames.append(thorName);
                    groupNames.append(groupName);
                    targetNames.append(targetName);
                    StringBuffer queueName(targetName);
                    queueNames.append(queueName.append(THOR_QUEUE_EXT));
                }
            }
        }
    }
    return thorNames.ordinality();
}


unsigned getEnvironmentHThorClusterNames(StringArray &eclAgentNames, StringArray &groupNames, StringArray &targetNames)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return 0;

    Owned<IPropertyTree> root = &env->getPTree();
    Owned<IPropertyTreeIterator> allEclAgents = root->getElements("Software/EclAgentProcess");
    ForEach(*allEclAgents)
    {
        IPropertyTree &eclAgent = allEclAgents->query();
        const char *eclAgentName = eclAgent.queryProp("@name");
        if (eclAgentName && *eclAgentName)
        {
            Owned<IPropertyTreeIterator> allTargets = root->getElements("Software/Topology/Cluster");
            ForEach(*allTargets)
            {
                IPropertyTree &target = allTargets->query();
                const char *targetName = target.queryProp("@name");
                if (targetName && *targetName)
                {
                    StringBuffer xpath;
                    xpath.appendf("EclAgentProcess[@process=\"%s\"]", eclAgentName);
                    if (target.hasProp(xpath) && !target.hasProp("ThorCluster"))
                    {
                        StringBuffer groupName("hthor__");
                        groupName.append(eclAgentName);

                        groupNames.append(groupName);
                        eclAgentNames.append(eclAgentName);
                        targetNames.append(targetName);
                    }
                }
            }
        }
    }
    return eclAgentNames.ordinality();
}


IStringVal& CLocalWorkUnit::getScope(IStringVal &str) const 
{
    CriticalBlock block(crit);
    if (p->hasProp("Debug/ForceScope"))
    {
        StringBuffer prefix(p->queryProp("Debug/ForceScope"));
        str.set(prefix.toLowerCase().str()); 
    }
    else
    {
        Owned <IConstWUClusterInfo> ci = getTargetClusterInfo(p->queryProp("@clusterName"));
        if (ci)
            ci->getScope(str); 
        else
            str.clear();
    }
    return str;
}

//Queries
void CLocalWorkUnit::setCodeVersion(unsigned codeVersion, const char * buildVersion, const char * eclVersion) 
{
    CriticalBlock block(crit);
    p->setPropInt("@codeVersion", codeVersion);
    p->setProp("@buildVersion", buildVersion);
    p->setProp("@eclVersion", eclVersion);
}

unsigned CLocalWorkUnit::getCodeVersion() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("@codeVersion");
}

unsigned CLocalWorkUnit::getWuidVersion() const 
{
    CriticalBlock block(crit);
    return p->getPropInt("@wuidVersion");
}

void CLocalWorkUnit::getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const 
{
    CriticalBlock block(crit);
    buildVersion.set(p->queryProp("@buildVersion"));
    eclVersion.set(p->queryProp("@eclVersion"));
}

void CLocalWorkUnit::setCloneable(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("@cloneable", value);
}

void CLocalWorkUnit::setIsClone(bool value) 
{
    CriticalBlock block(crit);
    p->setPropInt("@isClone", value);
}

bool CLocalWorkUnit::getCloneable() const 
{
    CriticalBlock block(crit);
    return p->getPropBool("@cloneable", false);
}

IUserDescriptor *CLocalWorkUnit::queryUserDescriptor() const
{
    CriticalBlock block(crit);
    if (!userDesc)
    {
        SCMStringBuffer token, user, password;
        getSecurityToken(token);
        extractToken(token.str(), queryWuid(), user, password);
        userDesc.setown(createUserDescriptor());
        userDesc->set(user.str(), password.str());
    }
    return userDesc;
}

bool CLocalWorkUnit::isProtected() const
{
    CriticalBlock block(crit);
    return p->getPropBool("@protected", false);
}

bool CLocalWorkUnit::isPausing() const
{
    CriticalBlock block(crit);
    if (WUActionPause == getAction())
    {
        switch (getState())
        {
            case WUStateRunning:
            case WUStateAborting:
                return true;
        }
    }
    return false;
}

void CLocalWorkUnit::protect(bool protectMode)
{
    CriticalBlock block(crit);
    p->setPropBool("@protected", protectMode);
}

void CLocalWorkUnit::setResultLimit(unsigned value)
{
    CriticalBlock block(crit);
    p->setPropInt("resultLimit", value);
}

unsigned CLocalWorkUnit::getResultLimit() const
{
    CriticalBlock block(crit);
    return p->getPropInt("resultLimit");
}

IStringVal & CLocalWorkUnit::getSnapshot(IStringVal & str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("SNAPSHOT")); 
    return str;
}

void CLocalWorkUnit::setSnapshot(const char * val)
{
    CriticalBlock block(crit);
    p->setProp("SNAPSHOT", val);
}

const static mapEnums warningSeverityMap[] =
{
    { SeverityInformation, "info" },
    { SeverityWarning, "warning" },
    { SeverityError, "error" },
    { SeverityAlert, "alert" },
    { SeverityIgnore, "ignore" },
    { SeverityFatal, "fatal" },
    { SeverityUnknown, NULL }
};


ErrorSeverity CLocalWorkUnit::getWarningSeverity(unsigned code, ErrorSeverity defaultSeverity) const
{
    StringBuffer xpath;
    xpath.append("OnWarnings/OnWarning[@code='").append(code).append("']");
    CriticalBlock block(crit);
    IPropertyTree * mapping = p->queryPropTree(xpath);
    if (mapping)
        return (ErrorSeverity) getEnum(mapping, "@severity", warningSeverityMap);
    return defaultSeverity;
}

void CLocalWorkUnit::setWarningSeverity(unsigned code, ErrorSeverity severity)
{
    StringBuffer xpath;
    xpath.append("OnWarnings/OnWarning[@code='").append(code).append("']");

    CriticalBlock block(crit);
    IPropertyTree * mapping = p->queryPropTree(xpath);
    if (!mapping)
    {
        IPropertyTree * onWarnings = ensurePTree(p, "OnWarnings");
        mapping = onWarnings->addPropTree("OnWarning", createPTree());
        mapping->setPropInt("@code", code);
    }

    setEnum(mapping, "@severity", severity, warningSeverityMap);
}

static int comparePropTrees(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return stricmp(l->queryName(), r->queryName());
};

unsigned CLocalWorkUnit::calculateHash(unsigned crc)
{
    // Any other values in the WU that could affect generated code should be crc'ed here
    IPropertyTree *tree = p->queryBranch("Debug");
    if (tree)
    {
        Owned<IPropertyTreeIterator> sub = tree->getElements("*");
        ICopyArrayOf<IPropertyTree> subs;
        for(sub->first(); sub->isValid(); sub->next())
            subs.append(sub->query());
        subs.sort(comparePropTrees);
        ForEachItemIn(idx, subs)
        {
            const char *name = subs.item(idx).queryName();
            const char *val = subs.item(idx).queryProp(NULL);
            crc = crc32(name, (size32_t)strlen(name), crc);
            if (val)
                crc = crc32(val, (size32_t)strlen(val), crc);
        }
    }
    Owned<IConstWUPluginIterator> plugins = &getPlugins();
    for (plugins->first();plugins->isValid();plugins->next())
    {
        IConstWUPlugin &thisplugin = plugins->query();
        SCMStringBuffer version;
        thisplugin.getPluginVersion(version);
        crc = crc32(version.str(), version.length(), crc);
    }
    return crc;
}

static void updateProp(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    if (!to->hasProp(xpath) && from->hasProp(xpath))
        to->setProp(xpath, from->queryProp(xpath));
}

static void setProp(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    if (from->hasProp(xpath))
        to->setProp(xpath, from->queryProp(xpath));
}

static void copyTree(IPropertyTree * to, const IPropertyTree * from, const char * xpath)
{
    IPropertyTree * match = from->getBranch(xpath); 
    if (match) 
        to->setPropTree(xpath, match);
}

IPropertyTree *CLocalWorkUnit::queryPTree() const
{
    return p;
}

void CLocalWorkUnit::copyWorkUnit(IConstWorkUnit *cached, bool all)
{
    CLocalWorkUnit *from = QUERYINTERFACE(cached, CLocalWorkUnit);
    if (!from)
    {
        CLockedWorkUnit *fl = QUERYINTERFACE(cached, CLockedWorkUnit);
        if (!fl)
            throw MakeStringException(WUERR_InternalUnknownImplementation, "Cached workunit not created using workunit dll");
        from = fl->c;
    }
    // Need to copy the query, the results, and the graphs from the cached query.
    // The cache is made before the query is executed so there is no need to clear them.
    if (!cached->getCloneable())
        throw MakeStringException(WUERR_CannotCloneWorkunit, "Source work unit not marked as clonable");

    const IPropertyTree * fromP = from->p;
    IPropertyTree *pt;

    CriticalBlock block(crit);
    clearCached(false);
    query.clear();
    updateProp(p, fromP, "@jobName");
    copyTree(p, fromP, "Query");
    pt = fromP->getBranch("Application/LibraryModule"); 
    if (pt)
    {
        ensurePTree(p, "Application");
        p->setPropTree("Application/LibraryModule", pt);
    }

    pt = fromP->queryBranch("Debug"); 
    if (pt)
    {
        IPropertyTree *curDebug = p->queryPropTree("Debug");
        if (curDebug)
        {
            Owned<IPropertyTreeIterator> elems = pt->getElements("*");
            ForEach(*elems)
            {
                IPropertyTree *elem = &elems->query();
                if (!curDebug->hasProp(elem->queryName()))
                    curDebug->setPropTree(elem->queryName(),LINK(elem));
            }
        }
        else
            p->setPropTree("Debug", LINK(pt));
    }
    copyTree(p, fromP, "OnWarnings");
    copyTree(p, fromP, "Plugins");
    copyTree(p, fromP, "Libraries");
    copyTree(p, fromP, "Results");
    copyTree(p, fromP, "Graphs");
    copyTree(p, fromP, "Workflow");
    copyTree(p, fromP, "WebServicesInfo");
    if (all)
    {
        // 'all' mode is used when setting up a dali WU from the embedded wu in a workunit dll

        // Merge timing info from both branches
        pt = fromP->getBranch("Timings");
        if (pt)
        {
            IPropertyTree *tgtTimings = ensurePTree(p, "Timings");
            mergePTree(tgtTimings, pt);
            pt->Release();
        }
        pt = fromP->getBranch("Statistics");
        if (pt)
        {
            IPropertyTree *tgtStatistics = ensurePTree(p, "Statistics");
            mergePTree(tgtStatistics, pt);
            pt->Release();
        }
    }

    updateProp(p, fromP, "@clusterName");
    updateProp(p, fromP, "allowedclusters");
    updateProp(p, fromP, "@submitID");
    updateProp(p, fromP, "SNAPSHOT");

    //MORE: This is very adhoc.  All options that should be cloned should really be in a common branch
    if (all)
    {
        setProp(p, fromP, "PriorityFlag");
        setProp(p, fromP, "@priorityClass");
        setProp(p, fromP, "@protected");
        setProp(p, fromP, "@clusterName");
        updateProp(p, fromP, "@scope");
    }

    //Variables may have been set up as parameters to the query - so need to preserve any values that were supplied.
    pt = fromP->getBranch("Variables");
    if (pt)
    {
        IPropertyTree *ptTgtVariables = ensurePTree(p, "Variables");

        Owned<IPropertyTreeIterator> ptiVariable = pt->getElements("Variable");
        for (ptiVariable->first(); ptiVariable->isValid(); ptiVariable->next())
        {
            IPropertyTree *ptSrcVariable = &ptiVariable->query();
            const char *name = ptSrcVariable->queryProp("@name");
            assertex(name);
            StringBuffer xpath;
            xpath.append("Variable[@name='").append(name).append("']");
            IPropertyTree *ptTgtVariable = ptTgtVariables->queryPropTree(xpath.str());
            IPropertyTree *merged = createPTreeFromIPT(ptSrcVariable); // clone entire source info...
            merged->removeProp("Value"); // except value and status
            merged->setProp("@status", "undefined");
            if (!merged->getPropBool("@isScalar"))
                merged->removeProp("totalRowCount");
            merged->removeProp("rowCount");
            // If there are any other fields that get set ONLY by eclagent, strip them out here...

            if (ptTgtVariable)
            {
                // copy status and Value from what is already set in target
                merged->setProp("@status", ptTgtVariable->queryProp("@status"));
                MemoryBuffer value;
                if (ptTgtVariable->getPropBin("Value", value))
                    merged->setPropBin("Value", value.length(), value.toByteArray());
                ptTgtVariable->removeProp(xpath.str());

                // If there are any other fields in a variable that get set by ws_ecl before submitting, copy them across here...
            }
            ptTgtVariables->addPropTree("Variable", merged);
        }
        pt->Release();
    }

    p->setProp("@codeVersion", fromP->queryProp("@codeVersion"));
    p->setProp("@buildVersion", fromP->queryProp("@buildVersion"));
    p->setProp("@eclVersion", fromP->queryProp("@eclVersion"));
    p->setProp("@hash", fromP->queryProp("@hash"));
    p->setPropBool("@cloneable", true);
    p->setPropBool("@isClone", true);
    resetWorkflow();  // the source Workflow section may have had some parts already executed...
    // resetResults(); // probably should be resetting the results as well... rather than waiting for the rerun to overwrite them
}

bool CLocalWorkUnit::hasDebugValue(const char *propname) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    return p->hasProp(prop.append(lower));
}

IStringVal& CLocalWorkUnit::getDebugValue(const char *propname, IStringVal &str) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    str.set(p->queryProp(prop.append(lower).str())); 
    return str;
}

IStringIterator& CLocalWorkUnit::getDebugValues() const
{
    return getDebugValues(NULL);
}

IStringIterator& CLocalWorkUnit::getDebugValues(const char *prop) const
{
    CriticalBlock block(crit);
    StringBuffer path("Debug/");
    if (prop)
    {
        StringBuffer lower;
        lower.append(prop).toLowerCase();
        path.append(lower);
    }
    else
        path.append("*");
    return *new CStringPTreeTagIterator(p->getElements(path.str()));
}

int CLocalWorkUnit::getDebugValueInt(const char *propname, int defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropInt(prop.str(), defVal); 
}

__int64 CLocalWorkUnit::getDebugValueInt64(const char *propname, __int64 defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropInt64(prop.str(), defVal); 
}

bool CLocalWorkUnit::getDebugValueBool(const char * propname, bool defVal) const
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    return p->getPropBool(prop.str(), defVal); 
}

IStringIterator *CLocalWorkUnit::getLogs(const char *type, const char *instance) const
{
    VStringBuffer xpath("Process/%s/", type);
    if (instance)
        xpath.append(instance);
    else
        xpath.append("*");
    CriticalBlock block(crit);
    if (p->getPropInt("@wuidVersion") < 1) // legacy wuid
    {
        // NB: instance unused
        if (streq("EclAgent", type))
            return new CStringPTreeIterator(p->getElements("Debug/eclagentlog"));
        else if (streq("Thor", type))
            return new CStringPTreeIterator(p->getElements("Debug/thorlog*"));
        VStringBuffer xpath("Debug/%s", type);
        return new CStringPTreeIterator(p->getElements(xpath.str()));
    }
    else
        return new CStringPTreeAttrIterator(p->getElements(xpath.str()), "@log");
}

IPropertyTreeIterator* CLocalWorkUnit::getProcesses(const char *type, const char *instance) const
{
    VStringBuffer xpath("Process/%s/", type);
    if (instance)
        xpath.append(instance);
    else
        xpath.append("*");
    CriticalBlock block(crit);
    return p->getElements(xpath.str());
}

IStringIterator *CLocalWorkUnit::getProcesses(const char *type) const
{
    VStringBuffer xpath("Process/%s/*", type);
    CriticalBlock block(crit);
    return new CStringPTreeTagIterator(p->getElements(xpath.str()));
}

void CLocalWorkUnit::addProcess(const char *type, const char *instance, unsigned pid, const char *log)
{
    VStringBuffer processType("Process/%s", type);
    VStringBuffer xpath("%s/%s", processType.str(), instance);
    if (log)
        xpath.appendf("[@log=\"%s\"]", log);
    CriticalBlock block(crit);
    if (!p->hasProp(xpath))
    {
        IPropertyTree *node = ensurePTree(p, processType.str());
        node = node->addPropTree(instance, createPTree());
        node->setProp("@log", log);
        node->setPropInt("@pid", pid);
    }
}

void CLocalWorkUnit::setDebugValue(const char *propname, const char *value, bool overwrite)
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure this line should be needed....
        p->setProp("Debug", ""); 
        p->setProp(prop.str(), value); 
    }
}

void CLocalWorkUnit::setDebugValueInt(const char *propname, int value, bool overwrite)
{
    StringBuffer lower;
    lower.append(propname).toLowerCase();
    CriticalBlock block(crit);
    StringBuffer prop("Debug/");
    prop.append(lower);
    if (overwrite || !p->hasProp(prop.str()))
    {
        // MORE - not sure this line should be needed....
        p->setProp("Debug", ""); 
        p->setPropInt(prop.str(), value); 
    }
}

void CLocalWorkUnit::setTracingValue(const char *propname, const char *value)
{
    CriticalBlock block(crit);
    // MORE - not sure this line should be needed....
    p->setProp("Tracing", ""); 
    StringBuffer prop("Tracing/");
    p->setProp(prop.append(propname).str(), value); 
}

void CLocalWorkUnit::setTracingValueInt(const char *propname, int value)
{
    CriticalBlock block(crit);
    StringBuffer prop("Tracing/");
    p->setPropInt(prop.append(propname).str(), value); 
}

IConstWUQuery* CLocalWorkUnit::getQuery() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if (!query)
    {
        IPropertyTree *s = p->getPropTree("Query");
        if (s)
            query.setown(new CLocalWUQuery(s)); // NB takes ownership of 's'
    }
    return query.getLink();
}

IWUQuery* CLocalWorkUnit::updateQuery()
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if (!query)
    {
        IPropertyTree *s = p->queryPropTree("Query");
        if (!s)
            s = p->addPropTree("Query", createPTreeFromXMLString("<Query fetchEntire='1'/>")); // Is this really desirable (the fetchEntire) ?
        s->Link();
        query.setown(new CLocalWUQuery(s)); 
    }
    return query.getLink();
}

void CLocalWorkUnit::loadPlugins() const
{
    CriticalBlock block(crit);
    if (!pluginsCached)
    {
        assertex(plugins.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Plugins/Plugin");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            plugins.append(*new CLocalWUPlugin(rp));
        }
        pluginsCached = true;
    }
}

IConstWUPluginIterator& CLocalWorkUnit::getPlugins() const
{
    CriticalBlock block(crit);
    loadPlugins();
    return *new CArrayIteratorOf<IConstWUPlugin,IConstWUPluginIterator> (plugins, 0, (IConstWorkUnit *) this);
}

void CLocalWorkUnit::loadLibraries() const
{
    CriticalBlock block(crit);
    if (!librariesCached)
    {
        assertex(libraries.length() == 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Libraries/Library");
        ForEach(*r)
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            libraries.append(*new CLocalWULibrary(rp));
        }
        librariesCached = true;
    }
}

IConstWULibraryIterator& CLocalWorkUnit::getLibraries() const
{
    CriticalBlock block(crit);
    loadLibraries();
    return *new CArrayIteratorOf<IConstWULibrary,IConstWULibraryIterator> (libraries, 0, (IConstWorkUnit *) this);
}

IConstWULibrary * CLocalWorkUnit::getLibraryByName(const char * search) const
{
    CriticalBlock block(crit);
    loadLibraries();
    ForEachItemIn(idx, libraries)
    {
        SCMStringBuffer name;
        IConstWULibrary &cur = libraries.item(idx);
        cur.getName(name);
        if (stricmp(name.str(), search)==0)
            return &OLINK(cur);
    }
    return NULL;
}

StringBuffer &formatGraphTimerLabel(StringBuffer &str, const char *graphName, unsigned subGraphNum, unsigned __int64 subId)
{
    str.append("Graph ").append(graphName);
    if (subGraphNum) str.append(" - ").append(subGraphNum).append(" (").append(subId).append(")");
    else if (subId) str.append(" - id(").append(subId).append(")");
    return str;
}

StringBuffer &formatGraphTimerScope(StringBuffer &str, const char *graphName, unsigned subGraphNum, unsigned __int64 subId)
{
    str.append(graphName);
    if (subId) str.append(":sg").append(subId);
    return str;
}

bool parseGraphTimerLabel(const char *label, StringAttr &graphName, unsigned & graphNum, unsigned &subGraphNum, unsigned &subId)
{
    // expects format: "Graph <graphname>[ - <subgraphnum> (<subgraphid>)]"
    unsigned len = (size32_t)strlen(label);
    if (len < 6 || (0 != memcmp(label, "Graph ", 6)))
        return false;
    graphNum = 0;
    subGraphNum = 0;
    subId = 0;
    const char *finger = label+6;
    const char *finger2 = strchr(finger, '-');

    if (NULL == finger2) // just graphName
        graphName.set(finger);
    else
    {
        graphName.set(finger, (size32_t)((finger2-1)-finger));
        finger = finger2+2; // skip '-' and space
        finger2 = strchr(finger, ' ');
        if (finger2)
        {
            subGraphNum = atoi_l(finger, (size32_t)(finger2-finger));
            finger = finger2+2; // skip space and '('
            finger2 = strchr(finger, ')');
            if (finger2)
                subId = atoi_l(finger, (size32_t)(finger2-finger));
        }
        else if (((len-(finger-label))>3) && 0 == memcmp(finger, "id(", 3)) // subgraph id only, new format.
        {
            finger += 3;
            finger2 = strchr(finger, ')');
            if (finger2)
                subId = atoi_l(finger, (size32_t)(finger2-finger));
        }
    }

    if (graphName && !memicmp(graphName, "graph", 5))
        graphNum = atoi(graphName + 5);

    return true;
}

bool parseGraphScope(const char *scope, StringAttr &graphName, unsigned & graphNum, unsigned &subGraphId)
{
    if (!MATCHES_CONST_PREFIX(scope, GraphScopePrefix))
        return false;

    graphNum = atoi(scope + CONST_STRLEN(GraphScopePrefix));

    const char * colon = strchr(scope, ':');
    if (!colon)
    {
        graphName.set(scope);
        subGraphId = 0;
        return true;
    }

    const char * subgraph = colon+1;
    graphName.set(scope, (size32_t)(colon - scope));
    if (MATCHES_CONST_PREFIX(subgraph, SubGraphScopePrefix))
        subGraphId = atoi(subgraph+CONST_STRLEN(SubGraphScopePrefix));

    return true;
}


class WorkUnitStatisticsIterator : public CArrayIteratorOf<IConstWUStatistic,IConstWUStatisticIterator>
{
    typedef CArrayIteratorOf<IConstWUStatistic,IConstWUStatisticIterator> PARENT;
public:
    WorkUnitStatisticsIterator(const IArray &a, aindex_t start, IInterface *owner, const IStatisticsFilter * _filter)
        : PARENT(a,start, owner), filter(_filter)
    {
    }

    virtual bool first()
    {
        if (!PARENT::first())
            return false;
        if (matchesFilter())
            return true;
        return next();
    }

    virtual bool next()
    {
        loop
        {
            if (!PARENT::next())
                return false;
            if (matchesFilter())
                return true;
        }
    }

protected:
    bool matchesFilter()
    {
        if (!filter)
            return true;
        return query().matches(filter);
    }

protected:
    Linked<const IStatisticsFilter> filter;
};

void CLocalWorkUnit::setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction)
{
    if (!scope || !*scope) scope = GLOBAL_SCOPE;

    const char * kindName = queryStatisticName(kind);
    StatisticMeasure measure = queryMeasure(kind);

    //creator. scope and name must all be present, and must not contain semi colons.
    assertex(creator && scope);

    CriticalBlock block(crit);
    IPropertyTree * stats = p->queryPropTree("Statistics");
    if (!stats)
        stats = p->addPropTree("Statistics", createPTree("Statistics"));

    IPropertyTree * statTree = NULL;
    if (mergeAction != StatsMergeAppend)
    {
        StringBuffer xpath;
        xpath.append("Statistic[@creator='").append(creator).append("'][@scope='").append(scope).append("'][@kind='").append(kindName).append("']");
        statTree = stats->queryPropTree(xpath.str());
    }

    if (!statTree)
    {
        statTree = stats->addPropTree("Statistic", createPTree("Statistic"));
        statTree->setProp("@creator", creator);
        statTree->setProp("@scope", scope);
        statTree->setProp("@kind", kindName);
        //These items are primarily here to facilitate filtering.
        statTree->setProp("@unit", queryMeasureName(measure));
        statTree->setProp("@c", queryCreatorTypeName(creatorType));
        statTree->setProp("@s", queryScopeTypeName(scopeType));
        statTree->setPropInt64("@ts", getTimeStampNowValue());

        if (optDescription)
            statTree->setProp("@desc", optDescription);

        if (statistics.cached)
            statistics.append(LINK(statTree));

        mergeAction = StatsMergeAppend;
    }

    if (mergeAction != StatsMergeAppend) // RKC->GH Is this right??
    {
        unsigned __int64 oldValue = statTree->getPropInt64("@value", 0);
        unsigned __int64 oldCount = statTree->getPropInt64("@count", 0);
        unsigned __int64 oldMax = statTree->getPropInt64("@max", 0);
        if (oldMax < oldValue)
            oldMax = oldValue;

        statTree->setPropInt64("@value", mergeStatisticValue(oldValue, value, mergeAction));
        statTree->setPropInt64("@count", count + oldCount);
        if (maxValue > oldMax)
            statTree->setPropInt64("@max", maxValue);
    }
    else
    {
        statTree->setPropInt64("@value", value);
        statTree->setPropInt64("@count", count);
        if (maxValue)
            statTree->setPropInt64("@max", maxValue);
        else
            statTree->removeProp("@max");
    }
    if (creatorType==SCTsummary && kind==StTimeElapsed && strsame(scope, GLOBAL_SCOPE))
    {
        StringBuffer t;
        formatTimeCollatable(t, value, false);
        p->setProp("@totalThorTime", t);
    }
}

void CLocalWorkUnit::_loadStatistics() const
{
    statistics.load(p,"Statistics/*");
}

IConstWUStatisticIterator& CLocalWorkUnit::getStatistics(const IStatisticsFilter * filter) const
{
    CriticalBlock block(crit);
    //This should be deleted in version 6.0 when support for 4.x is no longer required
    legacyTimings.load(p,"Timings/*");
    if (legacyTimings.ordinality())
        return *new WorkUnitStatisticsIterator(legacyTimings, 0, (IConstWorkUnit *) this, filter);

    statistics.load(p,"Statistics/*");
    Owned<IConstWUStatisticIterator> localStats = new WorkUnitStatisticsIterator(statistics, 0, (IConstWorkUnit *) this, filter);

    const char * wuid = p->queryName();
    Owned<IConstWUStatisticIterator> graphStats = new CConstGraphProgressStatisticsIterator(wuid, filter);
    return * new CCompoundIteratorOf<IConstWUStatisticIterator, IConstWUStatistic>(localStats, graphStats);
}

IConstWUStatistic * CLocalWorkUnit::getStatistic(const char * creator, const char * scope, StatisticKind kind) const
{
    //MORE: Optimize this....
    StatisticsFilter filter;
    filter.setCreator(creator);
    filter.setScope(scope);
    filter.setKind(kind);
    Owned<IConstWUStatisticIterator> stats = &getStatistics(&filter);
    if (stats->first())
        return LINK(&stats->query());
    return NULL;
}

IWUPlugin* CLocalWorkUnit::updatePluginByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUPlugin *existing = getPluginByName(qname);
    if (existing)
        return (IWUPlugin *) existing;
    if (!plugins.length())
        p->addPropTree("Plugins", createPTree("Plugins"));
    IPropertyTree *pl = p->queryPropTree("Plugins");
    IPropertyTree *s = pl->addPropTree("Plugin", createPTree("Plugin"));
    s->Link();
    IWUPlugin* q = new CLocalWUPlugin(s); 
    q->Link();
    plugins.append(*q);
    q->setPluginName(qname);
    return q;
}

IConstWUPlugin* CLocalWorkUnit::getPluginByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadPlugins();
    ForEachItemIn(idx, plugins)
    {
        SCMStringBuffer name;
        IConstWUPlugin &cur = plugins.item(idx);
        cur.getPluginName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IWULibrary* CLocalWorkUnit::updateLibraryByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWULibrary *existing = getLibraryByName(qname);
    if (existing)
        return (IWULibrary *) existing;
    if (!libraries.length())
        p->addPropTree("Libraries", createPTree("Libraries"));
    IPropertyTree *pl = p->queryPropTree("Libraries");
    IPropertyTree *s = pl->addPropTree("Library", createPTree("Library"));
    s->Link();
    IWULibrary* q = new CLocalWULibrary(s); 
    q->Link();
    libraries.append(*q);
    q->setName(qname);
    return q;
}

void CLocalWorkUnit::_loadExceptions() const
{
    assertex(exceptions.length() == 0);
    Owned<IPropertyTreeIterator> r = p->getElements("Exceptions/Exception");
    for (r->first(); r->isValid(); r->next())
    {
        IPropertyTree *rp = &r->query();
        rp->Link();
        exceptions.append(*new CLocalWUException(rp));
    }
}

void CLocalWorkUnit::loadExceptions() const
{
    CriticalBlock block(crit);
    if (!exceptionsCached)
    {
        _loadExceptions();
        exceptionsCached = true;
    }
}

IConstWUExceptionIterator& CLocalWorkUnit::getExceptions() const
{
    CriticalBlock block(crit);
    loadExceptions();
    return *new CArrayIteratorOf<IConstWUException,IConstWUExceptionIterator> (exceptions, 0, (IConstWorkUnit *) this);
}

unsigned CLocalWorkUnit::getExceptionCount() const
{
    CriticalBlock block(crit);
    loadExceptions();
    return exceptions.length();
}

void CLocalWorkUnit::clearExceptions()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    exceptions.kill();
    exceptionsCached = true;
    p->removeProp("Exceptions");
}


IWUException* CLocalWorkUnit::createException()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadExceptions();

    if (!exceptions.length())
        p->addPropTree("Exceptions", createPTree("Exceptions"));
    IPropertyTree *r = p->queryPropTree("Exceptions");
    IPropertyTree *s = r->addPropTree("Exception", createPTree("Exception"));
    s->setPropInt("@sequence", exceptions.ordinality());
    IWUException* q = new CLocalWUException(LINK(s)); 
    exceptions.append(*LINK(q));

    Owned<IJlibDateTime> now = createDateTimeNow();
    SCMStringBuffer temp;
    now->getString(temp);
    q->setTimeStamp(temp.str());
    return q;
}


IConstWUWebServicesInfo* CLocalWorkUnit::getWebServicesInfo() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if (!webServicesInfoCached)
    {
        assertex(!webServicesInfo);
        IPropertyTree *s = p->getPropTree("WebServicesInfo");
        if (s)
            webServicesInfo.setown(new CLocalWUWebServicesInfo(s)); // NB takes ownership of 's'
        webServicesInfoCached = true;
    }
    return webServicesInfo.getLink();
}

IWUWebServicesInfo* CLocalWorkUnit::updateWebServicesInfo(bool create)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if (!webServicesInfoCached)
    {
        IPropertyTree *s = p->queryPropTree("WebServicesInfo");
        if (!s)
        {
            if (create)
                s = p->addPropTree("WebServicesInfo", createPTreeFromXMLString("<WebServicesInfo />"));
            else
                return NULL;
        }
        s->Link();
        webServicesInfo.setown(new CLocalWUWebServicesInfo(s)); 
        webServicesInfoCached = true;
    }
    return webServicesInfo.getLink();
}

static int compareResults(IInterface * const *ll, IInterface * const *rr)
{
    CLocalWUResult *l = (CLocalWUResult *) *ll;
    CLocalWUResult *r = (CLocalWUResult *) *rr;
    return l->getResultSequence() - r->getResultSequence();
}

void CLocalWorkUnit::_loadResults() const
{
    Owned<IPropertyTreeIterator> r = p->getElements("Results/Result");
    for (r->first(); r->isValid(); r->next())
    {
        IPropertyTree *rp = &r->query();
        rp->Link();
        results.append(*new CLocalWUResult(rp));
    }
}

void CLocalWorkUnit::loadResults() const
{
    if (!resultsCached)
    {
        assertex(results.length() == 0);
        _loadResults();
        results.sort(compareResults);
        resultsCached = true;
    }
}

void CLocalWorkUnit::_loadVariables() const
{
    Owned<IPropertyTreeIterator> r = p->getElements("Variables/Variable");
    for (r->first(); r->isValid(); r->next())
    {
        IPropertyTree *rp = &r->query();
        rp->Link();
        variables.append(*new CLocalWUResult(rp));
    }
}

void CLocalWorkUnit::loadVariables() const
{
    if (!variablesCached)
    {
        assertex(variables.length() == 0);
        _loadVariables();
        variablesCached = true;
    }
}

void CLocalWorkUnit::_loadTemporaries() const
{
    Owned<IPropertyTreeIterator> r = p->getElements("Temporaries/Variable");
    for (r->first(); r->isValid(); r->next())
    {
        IPropertyTree *rp = &r->query();
        rp->Link();
        temporaries.append(*new CLocalWUResult(rp));
    }
}

void CLocalWorkUnit::loadTemporaries() const
{
    if (!temporariesCached)
    {
        assertex(temporaries.length() == 0);
        _loadTemporaries();
        temporariesCached = true;
    }
}

void CLocalWorkUnit::deleteTemporaries()
{
    CriticalBlock block(crit);
    if (temporariesCached)
    {
        temporaries.kill();
        temporariesCached = false;
    }
    p->removeProp("Temporaries");
}

IWUResult* CLocalWorkUnit::createResult()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadResults();
    if (!results.length())
        p->addPropTree("Results", createPTree("Results"));
    IPropertyTree *r = p->queryPropTree("Results");
    IPropertyTree *s = r->addPropTree("Result", createPTree());

    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    results.append(*q);
    return q;
}

IWUResult* CLocalWorkUnit::updateResultByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getResultByName(qname);
    if (existing)
        return (IWUResult *) existing;
    IWUResult* q = createResult(); 
    q->setResultName(qname);
    return q;
}

IWUResult* CLocalWorkUnit::updateResultBySequence(unsigned seq)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getResultBySequence(seq);
    if (existing)
        return (IWUResult *) existing;
    IWUResult* q = createResult(); 
    q->setResultSequence(seq);
    return q;
}

IConstWUResultIterator& CLocalWorkUnit::getResults() const
{
    CriticalBlock block(crit);
    loadResults();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (results, 0, (IConstWorkUnit *) this);
}

IConstWUResult* CLocalWorkUnit::getResultByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadResults();
    ForEachItemIn(idx, results)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = results.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResult* CLocalWorkUnit::getResultBySequence(unsigned seq) const
{
    CriticalBlock block(crit);
    loadResults();
    ForEachItemIn(idx, results)
    {
        IConstWUResult &cur = results.item(idx);
        if (cur.getResultSequence() == seq)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResultIterator& CLocalWorkUnit::getVariables() const
{
    CriticalBlock block(crit);
    loadVariables();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (variables, 0, (IConstWorkUnit *) this);
}

IConstWUResult* CLocalWorkUnit::getGlobalByName(const char *qname) const
{
    CriticalBlock block(crit);
    if (strcmp(p->queryName(), GLOBAL_WORKUNIT)==0)
        return getVariableByName(qname);

    Owned <IWorkUnit> global = factory->getGlobalWorkUnit(secMgr, secUser);
    return global->getVariableByName(qname);
}

IWUResult* CLocalWorkUnit::updateGlobalByName(const char *qname)
{
    CriticalBlock block(crit);
    if (strcmp(p->queryName(), GLOBAL_WORKUNIT)==0)
        return updateVariableByName(qname);

    Owned <IWorkUnit> global = factory->getGlobalWorkUnit(secMgr, secUser);
    return global->updateVariableByName(qname);
}

IConstWUResult* CLocalWorkUnit::getVariableByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadVariables();
    ForEachItemIn(idx, variables)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = variables.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResult* CLocalWorkUnit::getTemporaryByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadTemporaries();
    ForEachItemIn(idx, temporaries)
    {
        SCMStringBuffer name;
        IConstWUResult &cur = temporaries.item(idx);
        cur.getResultName(name);
        if (stricmp(name.str(), qname)==0)
        {
            cur.Link();
            return &cur;
        }
    }
    return NULL;
}

IConstWUResultIterator& CLocalWorkUnit::getTemporaries() const
{
    CriticalBlock block(crit);
    loadTemporaries();
    return *new CArrayIteratorOf<IConstWUResult,IConstWUResultIterator> (temporaries, 0, (IConstWorkUnit *) this);
}

IWUResult* CLocalWorkUnit::updateTemporaryByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getTemporaryByName(qname);
    if (existing)
        return (IWUResult *) existing;
    if (!temporaries.length())
        p->addPropTree("Temporaries", createPTree("Temporaries"));
    IPropertyTree *vars = p->queryPropTree("Temporaries");
    IPropertyTree *s = vars->addPropTree("Variable", createPTree("Variable"));
    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    temporaries.append(*q);
    q->setResultName(qname);
    q->setResultSequence(ResultSequenceInternal);
    return q;
}

IWUResult* CLocalWorkUnit::updateVariableByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUResult *existing = getVariableByName(qname);
    if (existing)
        return (IWUResult *) existing;
    if (!variables.length())
        p->addPropTree("Variables", createPTree("Variables"));
    IPropertyTree *vars = p->queryPropTree("Variables");
    IPropertyTree *s = vars->addPropTree("Variable", createPTree("Variable"));
    s->Link();
    IWUResult* q = new CLocalWUResult(s); 
    q->Link();
    variables.append(*q);
    q->setResultName(qname);
    q->setResultSequence(ResultSequenceStored);
    return q;
}

void CLocalWorkUnit::deleteTempFiles(const char *graph, bool deleteOwned, bool deleteJobOwned)
{
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files) return;
    Owned<IPropertyTreeIterator> iter = files->getElements("File");
    ICopyArrayOf<IPropertyTree> toRemove;
    ForEach (*iter)
    {
        IPropertyTree &file = iter->query();
        WUFileKind fileKind = (WUFileKind) file.getPropInt("@kind", WUFileStandard);
        if(file.getPropBool("@temporary")) fileKind = WUFileTemporary; // @temporary, legacy check
        bool needDelete;
        switch(fileKind)
        {
        case WUFileTemporary:
            if(graph==NULL)
                needDelete = true;
            else
            {
                const char *graphOwner = file.queryProp("@graph");
                needDelete = ((graphOwner==NULL) || (strcmp(graph, graphOwner)==0));
            }
            break;
        case WUFileJobOwned:
            needDelete = ((graph==NULL) && deleteJobOwned);
            break;
        case WUFileOwned:
            needDelete = ((graph==NULL) && deleteOwned);
            break;
        default:
            needDelete = false;
        }
        if(needDelete)
        {
            const char *name = file.queryProp("@name");
            LOG(MCdebugProgress, unknownJob, "Removing workunit file %s from DFS", name);
            queryDistributedFileDirectory().removeEntry(name, queryUserDescriptor());
            toRemove.append(file);
        }
    }
    ForEachItemIn(r, toRemove) files->removeTree(&toRemove.item(r));
}

static void _noteFileRead(IDistributedFile *file, IPropertyTree *filesRead)
{
    IDistributedSuperFile *super = file->querySuperFile();
    StringBuffer fname;
    file->getLogicalName(fname);
    if (fname.length())
    {
        StringBuffer path("File[@name=\"");
        path.append(fname).append("\"]");
        IPropertyTree *fileTree = filesRead->queryPropTree(path.str());
        if (fileTree)
            fileTree->setPropInt("@useCount", fileTree->getPropInt("@useCount")+1);
        else
        {
            StringBuffer cluster;
            file->getClusterName(0,cluster);
            fileTree = createPTree();
            fileTree->setProp("@name", fname.str());
            fileTree->setProp("@cluster", cluster.str());
            fileTree->setPropInt("@useCount", 1);
            fileTree = filesRead->addPropTree("File", fileTree);
        }
        if (super)
        {
            Owned<IDistributedFileIterator> iter = super->getSubFileIterator(false);
            ForEach (*iter)
            {
                IDistributedFile &file = iter->query();
                StringBuffer fname;
                file.getLogicalName(fname);
                Owned<IPropertyTree> subfile = createPTree();
                subfile->setProp("@name", fname.str());
                fileTree->addPropTree("Subfile", subfile.getClear());
                _noteFileRead(&file, filesRead);
            }
        }
    }
}

void CLocalWorkUnit::_loadFilesRead() const
{
    // Nothing to do
}

void CLocalWorkUnit::noteFileRead(IDistributedFile *file)
{
    if (file)
    {
        CriticalBlock block(crit);
        _loadFilesRead();
        IPropertyTree *files = p->queryPropTree("FilesRead");
        if (!files)
            files = p->addPropTree("FilesRead", createPTree());
        _noteFileRead(file, files);
    }
}

static void addFile(IPropertyTree *files, const char *fileName, const char *cluster, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
{
    StringBuffer path("File[@name=\"");
    path.append(fileName).append("\"]");
    if (cluster)
        path.append("[@cluster=\"").append(cluster).append("\"]");
    IPropertyTree *file = files->queryPropTree(path.str());
    if (file) files->removeTree(file);
    file = createPTree();
    file->setProp("@name", fileName);
    if (cluster)
        file->setProp("@cluster", cluster);
    if (graphOwner)
        file->setProp("@graph", graphOwner);
    file->setPropInt("@kind", (unsigned)fileKind);
    if (WUFileTemporary == fileKind)
        file->setPropInt("@usageCount", usageCount);
    files->addPropTree("File", file);
}

void CLocalWorkUnit::addFile(const char *fileName, StringArray *clusters, unsigned usageCount, WUFileKind fileKind, const char *graphOwner)
{
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files)
        files = p->addPropTree("Files", createPTree());
    if (!clusters)
        addFile(fileName, NULL, usageCount, fileKind, graphOwner);
    else
    {
        ForEachItemIn(c, *clusters)
            ::addFile(files, fileName, clusters->item(c), usageCount, fileKind, graphOwner);
    }
}

void CLocalWorkUnit::releaseFile(const char *fileName)
{
    StringBuffer path("File[@name=\"");
    path.append(fileName).append("\"]");
    CriticalBlock block(crit);
    IPropertyTree *files = p->queryPropTree("Files");
    if (!files) return;
    Owned<IPropertyTreeIterator> fiter = files->getElements(path.str());
    ForEach (*fiter)
    {
        IPropertyTree *file = &fiter->query();
        unsigned usageCount = file->getPropInt("@usageCount");
        if (usageCount > 1)
            file->setPropInt("@usageCount", usageCount-1);
        else
        {
            StringAttr name(file->queryProp("@name"));
            files->removeTree(file);
            if (!name.isEmpty()&&(1 == usageCount))
            {
                if (queryDistributedFileDirectory().removeEntry(fileName, queryUserDescriptor()))
                    LOG(MCdebugProgress, unknownJob, "Removed (released) file %s from DFS", name.get());
            }
        }
    }
}

void CLocalWorkUnit::clearGraphProgress() const
{
}

void CLocalWorkUnit::resetBeforeGeneration()
{
    CriticalBlock block(crit);
    //Remove all associated files
    Owned<IWUQuery> q = updateQuery();
    q->removeAssociatedFiles();

    //Remove any pre-existing workflow information
    workflowIterator.clear();
    p->removeProp("Workflow");
}

unsigned CLocalWorkUnit::queryFileUsage(const char *fileName) const
{
    StringBuffer path("Files/File[@name=\"");
    path.append(fileName).append("\"]/@usageCount");
    CriticalBlock block(crit);
    return p->getPropInt(path.str());
}

IPropertyTree *CLocalWorkUnit::getDiskUsageStats()
{
    return p->getPropTree("DiskUsageStats");
}

void CLocalWorkUnit::addDiskUsageStats(__int64 _avgNodeUsage, unsigned _minNode, __int64 _minNodeUsage, unsigned _maxNode, __int64 _maxNodeUsage, __int64 _graphId)
{
    IPropertyTree *stats = p->queryPropTree("DiskUsageStats");
    offset_t maxNodeUsage;
    if (stats)
        maxNodeUsage = stats->getPropInt64("@maxNodeUsage");
    else
    {
        stats = p->addPropTree("DiskUsageStats", createPTree());
        maxNodeUsage = 0;
    }

    if ((offset_t)_maxNodeUsage > maxNodeUsage)
    {
        // record all details at time of max node usage.
        stats->setPropInt("@minNode", _minNode);
        stats->setPropInt("@maxNode", _maxNode);
        stats->setPropInt64("@minNodeUsage", _minNodeUsage);
        stats->setPropInt64("@maxNodeUsage", _maxNodeUsage);
        stats->setPropInt64("@graphId", _graphId);
        if (_avgNodeUsage)
        {
            unsigned _skewHi = (unsigned)((100 * (_maxNodeUsage-_avgNodeUsage))/_avgNodeUsage);
            unsigned _skewLo = (unsigned)((100 * (_avgNodeUsage-_minNodeUsage))/_avgNodeUsage);
            stats->setPropInt("@skewHi", _skewHi);
            stats->setPropInt("@skewLo", _skewLo);
        }
    }
}

IPropertyTreeIterator & CLocalWorkUnit::getFileIterator() const
{
    CriticalBlock block(crit);
    return * p->getElements("Files/File");
}

IPropertyTreeIterator & CLocalWorkUnit::getFilesReadIterator() const
{
    CriticalBlock block(crit);
    _loadFilesRead();
    return * p->getElements("FilesRead/File");
}

//=================================================================================================


bool CLocalWorkUnit::switchThorQueue(const char *cluster, IQueueSwitcher *qs)
{
    CriticalBlock block(crit);
    if (qs->isAuto()&&!getAllowAutoQueueSwitch())
        return false;
    Owned<IConstWUClusterInfo> newci = getTargetClusterInfo(cluster);
    if (!newci) 
        return false;
    StringBuffer currentcluster;
    if (!p->getProp("@clusterName",currentcluster))
        return false;
    Owned<IConstWUClusterInfo> curci = getTargetClusterInfo(currentcluster.str());
    if (!curci)
        return false;
    SCMStringBuffer curqname;
    curci->getThorQueue(curqname);
    const char *wuid = p->queryName();
    void *qi = qs->getQ(curqname.str(),wuid);
    if (!qi)
        return false;
    setClusterName(cluster);
    SCMStringBuffer newqname;
    newci->getThorQueue(newqname);
    qs->putQ(newqname.str(),wuid,qi);
    return true;
}


//=================================================================================================

IPropertyTree *CLocalWorkUnit::getUnpackedTree(bool includeProgress) const
{
    Owned<IPropertyTree> ret = createPTreeFromIPT(p);
    Owned<IConstWUGraphIterator> graphIter = &getGraphs(GraphTypeAny);
    ForEach(*graphIter)
    {
        IConstWUGraph &graph  = graphIter->query();
        Owned<IPropertyTree> graphTree = graph.getXGMMLTree(includeProgress);
        SCMStringBuffer gName;
        graph.getName(gName);
        StringBuffer xpath("Graphs/Graph[@name=\"");
        xpath.append(gName.s).append("\"]/xgmml");
        IPropertyTree *xgmml = ret->queryPropTree(xpath.str());
        if (xgmml) // don't know of any reason it shouldn't exist
        {
            xgmml->removeProp("graphBin");
            xgmml->setPropTree("graph", graphTree.getClear());
        }
    }
    return ret.getClear();
}

void CLocalWorkUnit::_loadGraphs(bool heavy) const
{
    Owned<IPropertyTreeIterator> iter = p->getElements("Graphs/Graph");
    ForEach(*iter)
    {
        IPropertyTree &graph = iter->query();
        graphs.append(*new CLocalWUGraph(*this, LINK(&graph)));
    }
}

void CLocalWorkUnit::loadGraphs(bool heavy) const
{
    if (graphsCached < (heavy ? 2 : 1))
    {
        graphs.kill();
        _loadGraphs(heavy);
        graphsCached = (heavy ? 2 : 1);
    }
}

mapEnums graphTypes[] = {
   { GraphTypeAny, "unknown" },
   { GraphTypeProgress, "progress" },
   { GraphTypeEcl, "ECL" },
   { GraphTypeActivities, "activities" },
   { GraphTypeSubProgress, "subgraph" },
   { GraphTypeSize,  NULL },
};

CLocalWUGraph::CLocalWUGraph(const CLocalWorkUnit &_owner, IPropertyTree *props) : p(props), owner(_owner)
{
    wuidVersion = owner.getWuidVersion();
}

IStringVal& CLocalWUGraph::getName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal& CLocalWUGraph::getLabel(IStringVal &str) const
{
    if (wuidVersion >= 2)
    {
        str.set(p->queryProp("@label"));
        return str;
    }
    else
    {
        Owned<IPropertyTree> xgmml = getXGMMLTree(false);
        str.set(xgmml->queryProp("@label"));
        return str;
    }
}

WUGraphState CLocalWUGraph::getState() const
{
    return owner.queryGraphState(p->queryProp("@name"));
}


IStringVal& CLocalWUGraph::getXGMML(IStringVal &str, bool mergeProgress) const
{
    Owned<IPropertyTree> xgmml = getXGMMLTree(mergeProgress);
    if (xgmml)
    {
        StringBuffer x;
        toXML(xgmml, x);
        str.set(x.str());
    }
    return str;
}

unsigned CLocalWorkUnit::getGraphCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Graphs"))
    {
        return p->queryPropTree("Graphs")->numChildren();
    }
    return 0;
}

unsigned CLocalWorkUnit::getSourceFileCount() const
{
    CriticalBlock block(crit);
    _loadFilesRead();
    if (p->hasProp("FilesRead"))
    {
        return p->queryPropTree("FilesRead")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getResultCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Results"))
    {
        return p->queryPropTree("Results")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getVariableCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Variables"))
    {
        return p->queryPropTree("Variables")->numChildren();
    }
    return 0;
    
}

unsigned CLocalWorkUnit::getApplicationValueCount() const
{
    CriticalBlock block(crit);
    if (p->hasProp("Application"))
    {
        return p->queryPropTree("Application")->numChildren();
    }
    return 0;
    
}

StringBuffer &appendPTreeOpenTag(StringBuffer &s, IPropertyTree *tree, const char *name, unsigned indent)
{
    appendXMLOpenTag(s, name, NULL, false);
    Owned<IAttributeIterator> attrs = tree->getAttributes(true);
    if (attrs->first())
    {
        unsigned attributeindent = indent + (size32_t) strlen(name);
        unsigned count = attrs->count();
        bool doindent = false;
        ForEach(*attrs)
        {
            if (doindent)
                s.append('\n').appendN(attributeindent, ' ');
            else if (count > 3)
                doindent = true;
            appendXMLAttr(s, attrs->queryName()+1, attrs->queryValue());
        }
    }
    s.append('>');
    return s;
}

IStringVal &CLocalWorkUnit::getXmlParams(IStringVal &str, bool hidePasswords) const
{
    CriticalBlock block(crit);
    IPropertyTree *paramTree = p->queryPropTree("Parameters");
    if (!paramTree)
        return str;

    StringBuffer xml;
    if (!hidePasswords)
        toXML(paramTree, xml);
    else
    {
        appendPTreeOpenTag(xml.append(' '), paramTree, "Parameters", 0).append('\n');

        Owned<IPropertyTreeIterator> elems = paramTree->getElements("*");
        ForEach(*elems)
        {
            const char *paramname = elems->query().queryName();
            VStringBuffer xpath("Variables/Variable[@name='%s']/Format/@password", paramname);
            if (p->getPropBool(xpath))
                appendXMLTag(xml.append("  "), paramname, "***").append('\n');
            else
                toXML(&elems->query(), xml, 2);
        }
        appendXMLCloseTag(xml.append(' '), "Parameters").append('\n');
    }
    str.set(xml);
    return str;
}

const IPropertyTree *CLocalWorkUnit::getXmlParams() const
{
    CriticalBlock block(crit);
    return p->getPropTree("Parameters");
}

void CLocalWorkUnit::setXmlParams(const char *params)
{
    CriticalBlock block(crit);
    p->setPropTree("Parameters", createPTreeFromXMLString(params));
}

void CLocalWorkUnit::setXmlParams(IPropertyTree *tree)
{
    CriticalBlock block(crit);
    p->setPropTree("Parameters", tree);
}

unsigned __int64 CLocalWorkUnit::getHash() const
{
    CriticalBlock block(crit);
    return p->getPropInt64("@hash");
}

void CLocalWorkUnit::setHash(unsigned __int64 hash)
{
    CriticalBlock block(crit);
    p->setPropInt64("@hash", hash);
}

// getGraphs / getGraphsMeta
// These are basically the same except for the amount of preloading they do, and the type of the iterator they return...
// If a type other than any is requested, a postfilter is needed.

template <class T, class U> class CFilteredGraphIteratorOf : public CInterfaceOf<T>
{
    WUGraphType type;
    Owned<T> base;
    bool match()
    {
        return  base->query().getType()==type;
    }
public:
    CFilteredGraphIteratorOf<T,U>(T *_base, WUGraphType _type)
        : base(_base), type(_type)
    {
    }
    bool first()
    {
        if (!base->first())
            return false;
        if (match())
            return true;
        return next();
    }
    bool next()
    {
        while (base->next())
            if (match())
                return true;
        return false;
    }
    virtual bool isValid()
    {
        return base->isValid();
    }
    U & query()
    {
        return base->query();
    }
};

IConstWUGraphMetaIterator& CLocalWorkUnit::getGraphsMeta(WUGraphType type) const
{
    /* NB: this method should be 'cheap', loadGraphs() creates IConstWUGraph interfaces to the graphs
     * it does not actually pull the graph data. We only use IConstWUGraphMeta here, which never probes the xgmml.
     */

    CriticalBlock block(crit);
    loadGraphs(false);
    IConstWUGraphMetaIterator *giter = new CArrayIteratorOf<IConstWUGraph,IConstWUGraphMetaIterator> (graphs, 0, (IConstWorkUnit *) this);
    if (type!=GraphTypeAny)
        giter = new CFilteredGraphIteratorOf<IConstWUGraphMetaIterator, IConstWUGraphMeta>(giter,type);
    return *giter;
}

IConstWUGraphIterator& CLocalWorkUnit::getGraphs(WUGraphType type) const
{
    CriticalBlock block(crit);
    loadGraphs(true);
    IConstWUGraphIterator *giter = new CArrayIteratorOf<IConstWUGraph,IConstWUGraphIterator> (graphs, 0, (IConstWorkUnit *) this);
    if (type!=GraphTypeAny)
        giter = new CFilteredGraphIteratorOf<IConstWUGraphIterator, IConstWUGraph>(giter, type);
    return *giter;
}

IConstWUGraph* CLocalWorkUnit::getGraph(const char *qname) const
{
    CriticalBlock block(crit);
    VStringBuffer xpath("Graphs/Graph[@name='%s']", qname);
    // NOTE - this would go wrong if we had other graphs of same name but different type. Ignore for now.
    IPTree *graph = p->queryPropTree(xpath);
    if (graph)
        return new CLocalWUGraph(*this, LINK(graph));
    return NULL;
}

void CLocalWorkUnit::createGraph(const char * name, const char *label, WUGraphType type, IPropertyTree *xgmml)
{
    CriticalBlock block(crit);
    if (!graphs.length())
        p->addPropTree("Graphs", createPTree("Graphs"));
    IPropertyTree *r = p->queryPropTree("Graphs");
    IPropertyTree *s = r->addPropTree("Graph", createPTree());
    CLocalWUGraph *q = new CLocalWUGraph(*this, LINK(s));
    q->setName(name);
    q->setLabel(label);
    q->setType(type);
    q->setXGMMLTree(xgmml);
    graphs.append(*q);
}

IConstWUGraphProgress *CLocalWorkUnit::getGraphProgress(const char *name) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
WUGraphState CLocalWorkUnit::queryGraphState(const char *graphName) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
WUGraphState CLocalWorkUnit::queryNodeState(const char *graphName, WUGraphIDType nodeId) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
void CLocalWorkUnit::setGraphState(const char *graphName, WUGraphState state) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
void CLocalWorkUnit::setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
IWUGraphStats *CLocalWorkUnit::updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned subgraph) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}

void CLocalWUGraph::setName(const char *str)
{
    p->setProp("@name", str);
}

void CLocalWUGraph::setLabel(const char *str)
{
    p->setProp("@label", str);
}

void CLocalWUGraph::setXGMML(const char *str)
{
    setXGMMLTree(createPTreeFromXMLString(str));
}

void CLocalWUGraph::setXGMMLTree(IPropertyTree *_graph)
{
    assertex(strcmp(_graph->queryName(), "graph")==0);
    IPropertyTree *xgmml = p->setPropTree("xgmml", createPTree());
    MemoryBuffer mb;
    _graph->serialize(mb);
    // Note - we could compress further but that would introduce compatibility concerns, so don't bother
    // Cassandra workunit code actually lzw compresses the parent anyway
    xgmml->setPropBin("graphBin", mb.length(), mb.toByteArray());
    graph.setown(_graph);
}

static void expandAttributes(IPropertyTree & targetNode, IPropertyTree & progressNode)
{
    Owned<IAttributeIterator> aIter = progressNode.getAttributes();
    ForEach (*aIter)
    {
        const char *aName = aIter->queryName()+1;
        if (0 != stricmp("id", aName)) // "id" reserved.
        {
            IPropertyTree *att = targetNode.addPropTree("att", createPTree());
            att->setProp("@name", aName);
            att->setProp("@value", aIter->queryValue());
        }
    }
}

void CLocalWUGraph::mergeProgress(IPropertyTree &rootNode, IPropertyTree &progressTree, const unsigned &progressV) const
{
    IPropertyTree *graphNode = rootNode.queryPropTree("att/graph");
    if (!graphNode) return;
    unsigned nodeId = rootNode.getPropInt("@id");
    StringBuffer progressNodePath("node[@id=\"");
    progressNodePath.append(nodeId).append("\"]");
    IPropertyTree *progressNode = progressTree.queryPropTree(progressNodePath.str());
    if (progressNode)
    {
        expandAttributes(*graphNode, *progressNode);

        Owned<IPropertyTreeIterator> edges = progressNode->getElements("edge");
        ForEach (*edges)
        {
            IPropertyTree &edge = edges->query();
            StringBuffer edgePath("edge[@id=\"");
            edgePath.append(edge.queryProp("@id")).append("\"]");
            IPropertyTree *graphEdge = graphNode->queryPropTree(edgePath.str());
            if (graphEdge)
            {
                if (progressV < 1)
                    mergePTree(graphEdge, &edge);
                else
                { // must translate to XGMML format
                    expandAttributes(*graphEdge, edge);

                    // This is really only here, so that our progress format can use non-attribute values, which have different efficiency qualifies (e.g. can be external by dali)
                    Owned<IPropertyTreeIterator> iter = edge.getElements("*");
                    ForEach (*iter)
                    {
                        IPropertyTree &t = iter->query();
                        IPropertyTree *att = graphEdge->addPropTree("att", createPTree());
                        att->setProp("@name", t.queryName());
                        att->setProp("@value", t.queryProp(NULL));
                    }
                }
            }
        }
        Owned<IPropertyTreeIterator> nodes = progressNode->getElements("node");
        ForEach (*nodes)
        {
            IPropertyTree &node = nodes->query();
            StringBuffer nodePath("node[@id=\"");
            nodePath.append(node.queryProp("@id")).append("\"]");
            IPropertyTree *_node = graphNode->queryPropTree(nodePath.str());
            if (_node)
            {
                if (progressV < 1)
                    mergePTree(_node, &node);
                else
                { // must translate to XGMML format
                    expandAttributes(*_node, node);
                }
            }
        }
    }
    Owned<IPropertyTreeIterator> iter = graphNode->getElements("node");
    ForEach (*iter)
        mergeProgress(iter->query(), progressTree, progressV);
}

IPropertyTree * CLocalWUGraph::getXGMMLTreeRaw() const
{
    return p->getPropTree("xgmml");
}

IPropertyTree * CLocalWUGraph::getXGMMLTree(bool doMergeProgress) const
{
    if (!graph)
    {
        // NB: although graphBin introduced in wuidVersion==2,
        // daliadmin can retrospectively compress existing graphs, so need to check for all versions
        MemoryBuffer mb;
        if (p->getPropBin("xgmml/graphBin", mb))
            graph.setown(createPTree(mb));
        else
            graph.setown(p->getBranch("xgmml/graph"));
        if (!graph)
            return NULL;
    }
    if (!doMergeProgress)
        return graph.getLink();
    else
    {
        Owned<IPropertyTree> copy = createPTreeFromIPT(graph);
        Owned<IConstWUGraphProgress> progress = owner.getGraphProgress(p->queryProp("@name"));
        if (progress)
        {
            //MORE: Eventually this should directly access the new stats structure
            unsigned progressV = progress->queryFormatVersion();
            Owned<IPropertyTree> progressTree = progress->getProgressTree();
            Owned<IPropertyTreeIterator> nodeIterator = copy->getElements("node");
            ForEach (*nodeIterator)
                mergeProgress(nodeIterator->query(), *progressTree, progressV);
        }
        return copy.getClear();
    }
}

WUGraphType CLocalWUGraph::getType() const
{
    return (WUGraphType) getEnum(p, "@type", graphTypes);
}

IStringVal & CLocalWUGraph::getTypeName(IStringVal &str) const
{
    str.set(p->queryProp("@type"));
    if (!str.length())
        str.set("unknown");
    return str;
}

void CLocalWUGraph::setType(WUGraphType _type)
{
    setEnum(p, "@type", _type, graphTypes);
}

//=================================================================================================

mapEnums queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeSize,  NULL },
};

CLocalWUAssociated::CLocalWUAssociated(IPropertyTree *props) : p(props)
{
}

WUFileType CLocalWUAssociated::getType() const
{
    return (WUFileType)getEnum(p, "@type", queryFileTypes);
}

IStringVal & CLocalWUAssociated::getDescription(IStringVal & str) const
{
    str.set(p->queryProp("@desc"));
    return str;
}

IStringVal & CLocalWUAssociated::getIp(IStringVal & str) const
{
    str.set(p->queryProp("@ip"));
    return str;
}

IStringVal & CLocalWUAssociated::getName(IStringVal & str) const
{
    str.set(p->queryProp("@filename"));
    return str;
}

IStringVal & CLocalWUAssociated::getNameTail(IStringVal & str) const
{
    str.set(pathTail(p->queryProp("@filename")));
    return str;
}

unsigned CLocalWUAssociated::getCrc() const
{
    return p->getPropInt("@crc", 0);
}



//=================================================================================================

CLocalWUQuery::CLocalWUQuery(IPropertyTree *props) : p(props)
{
    associatedCached = false;
}

mapEnums queryTypes[] = {
   { QueryTypeUnknown, "unknown" },
   { QueryTypeEcl, "ECL" },
   { QueryTypeSql, "SQL" },
   { QueryTypeXml, "XML" },
   { QueryTypeAttribute, "Attribute" },
   { QueryTypeSize,  NULL },
};

WUQueryType CLocalWUQuery::getQueryType() const
{
    return (WUQueryType) getEnum(p, "@type", queryTypes);
}

void CLocalWUQuery::setQueryType(WUQueryType qt) 
{
    setEnum(p, "@type", qt, queryTypes);
}

IStringVal& CLocalWUQuery::getQueryText(IStringVal &str) const
{
    str.set(p->queryProp("Text"));
    return str;
}

IStringVal& CLocalWUQuery::getQueryShortText(IStringVal &str) const
{
    const char * text = p->queryProp("ShortText");
    if (text)
        str.set(text);
    else
    {
        text = p->queryProp("Text");
        if (isArchiveQuery(text))
        {
            Owned<IPropertyTree> xml = createPTreeFromXMLString(text, ipt_caseInsensitive);
            const char * path = xml->queryProp("Query/@attributePath");
            if (path)
            {
                IPropertyTree * resolved = resolveDefinitionInArchive(xml, path);
                if (resolved)
                    str.set(resolved->queryProp(NULL));
            }
            else
                str.set(xml->queryProp("Query"));
        }
        else
            str.set(text);
    }
    return str;
}

bool CLocalWUQuery::isArchive() const
{
    if (p->hasProp("@isArchive"))
        return p->getPropBool("@isArchive");
    const char *text = p->queryProp("Text");
    return isArchiveQuery(text);
}

IStringVal& CLocalWUQuery::getQueryName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal & CLocalWUQuery::getQueryMainDefinition(IStringVal & str) const
{
    str.set(p->queryProp("@main"));
    return str;
}

IStringVal& CLocalWUQuery::getQueryDllName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeDll, 0);
    if (entry)
        entry->getNameTail(str);
    return str;
}

IStringVal& CLocalWUQuery::getQueryCppName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeCpp, 0);
    if (entry)
        entry->getName(str);
    return str;
}

IStringVal& CLocalWUQuery::getQueryResTxtName(IStringVal &str) const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeResText, 0);
    if (entry)
        entry->getName(str);
    return str;
}

unsigned CLocalWUQuery::getQueryDllCrc() const
{
    Owned<IConstWUAssociatedFile> entry = getAssociatedFile(FileTypeDll, 0);
    if (entry)
        return entry->getCrc();
    return 0;
}

void CLocalWUQuery::setQueryText(const char *text)
{
    p->setProp("Text", text);
    bool isArchive = isArchiveQuery(text);
    if (isArchive)
    {
        Owned<IPropertyTree> xml = createPTreeFromXMLString(text, ipt_caseInsensitive);
        const char * path = xml->queryProp("Query/@attributePath");
        if (path)
        {
            IPropertyTree * resolved = resolveDefinitionInArchive(xml, path);
            if (resolved)
                p->setProp("ShortText", resolved->queryProp(NULL));
        }
        else
            p->setProp("ShortText", xml->queryProp("Query"));
    }
    p->setPropBool("@isArchive", isArchive);
}

void CLocalWUQuery::setQueryName(const char *qname)
{
    p->setProp("@name", qname);
}

void CLocalWUQuery::setQueryMainDefinition(const char * str)
{
    p->setProp("@main", str);
}

void CLocalWUQuery::addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc)
{
    CriticalBlock block(crit);
    loadAssociated();
    if (!associated.length())
        p->addPropTree("Associated", createPTree("Associated"));
    IPropertyTree *pl = p->queryPropTree("Associated");
    IPropertyTree *s = pl->addPropTree("File", createPTree("File"));
    setEnum(s, "@type", type, queryFileTypes);
    s->setProp("@filename", name);
    s->setProp("@ip", ip);
    s->setProp("@desc", desc);

    if (crc)
        s->setPropInt("@crc", crc);
    IConstWUAssociatedFile * q = new CLocalWUAssociated(LINK(s)); 
    associated.append(*q);
}

void CLocalWUQuery::removeAssociatedFile(WUFileType type, const char * name, const char * desc)
{
    CriticalBlock block(crit);
    associatedCached = false;
    associated.kill();
    StringBuffer xpath;
    xpath.append("Associated/File");
    if (type)
        xpath.append("[@type=\"").append(getEnumText(type, queryFileTypes)).append("\"]");
    if (name)
        xpath.append("[@filename=\"").append(name).append("\"]");
    if (desc)
        xpath.append("[@desc=\"").append(desc).append("\"]");

    p->removeProp(xpath.str());
}

void CLocalWUQuery::removeAssociatedFiles()
{
    associatedCached = false;
    associated.kill();
    p->removeProp("Associated");
}


IConstWUAssociatedFile * CLocalWUQuery::getAssociatedFile(WUFileType type, unsigned index) const
{
    CriticalBlock block(crit);
    loadAssociated();
    ForEachItemIn(idx, associated)
    {
        CLocalWUAssociated &cur = static_cast<CLocalWUAssociated &>(associated.item(idx));
        if (cur.getType() == type)
        {
            if (index-- == 0)
                return &OLINK(cur);
        }
    }
    return NULL;
}

void CLocalWUQuery::addSpecialCaseAssociated(WUFileType type, const char * propname, unsigned crc) const
{
    const char * name = p->queryProp(propname);
    if (name)
    {
        IPropertyTree *s = createPTree("File");
        setEnum(s, "@type", type, queryFileTypes);
        s->setProp("@filename", name);
        if (crc)
            s->setPropInt("@crc", crc);
        associated.append(*new CLocalWUAssociated(s));
    }
}

void CLocalWUQuery::loadAssociated() const
{
    CriticalBlock block(crit);
    if (!associatedCached)
    {
        assertex(associated.length() == 0);
        addSpecialCaseAssociated(FileTypeDll, "DllName", p->getPropInt("DllCrc", 0));
        addSpecialCaseAssociated(FileTypeCpp, "CppName", 0);
        addSpecialCaseAssociated(FileTypeResText, "ResTxtName", 0);
        Owned<IPropertyTreeIterator> r = p->getElements("Associated/File");
        for (r->first(); r->isValid(); r->next())
        {
            IPropertyTree *rp = &r->query();
            rp->Link();
            associated.append(*new CLocalWUAssociated(rp));
        }
        associatedCached = true;
    }
}

IConstWUAssociatedFileIterator& CLocalWUQuery::getAssociatedFiles() const
{
    CriticalBlock block(crit);
    loadAssociated();
    return *new CArrayIteratorOf<IConstWUAssociatedFile,IConstWUAssociatedFileIterator> (associated, 0, (IConstWUQuery *) this);
}

//========================================================================================

CLocalWUWebServicesInfo::CLocalWUWebServicesInfo(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUWebServicesInfo::getModuleName(IStringVal &str) const
{
    str.set(p->queryProp("@module"));
    return str;
}

IStringVal& CLocalWUWebServicesInfo::getAttributeName(IStringVal &str) const
{
    str.set(p->queryProp("@attribute"));
    return str;
}

IStringVal& CLocalWUWebServicesInfo::getDefaultName(IStringVal &str) const
{
    str.set(p->queryProp("@defaultName"));
    return str;
}

unsigned CLocalWUWebServicesInfo::getWebServicesCRC() const
{
    return (unsigned) p->getPropInt("@crc");
}

IStringVal& CLocalWUWebServicesInfo::getInfo(const char *name, IStringVal &str) const
{
    if (!name)
    {
        StringBuffer ws_info;
        ws_info.appendf("<%s ", p->queryName());
        Owned<IAttributeIterator> attrs = p->getAttributes();
        for(attrs->first(); attrs->isValid(); attrs->next())
        {
            const char *name = attrs->queryName()+1;
            const char *value = attrs->queryValue();
            ws_info.appendf("%s='%s' ", name, value);
        }
        ws_info.append("> \n");

        Owned<IPropertyTreeIterator> info = p->getElements("*");
        ForEach(*info)
        {
            IPropertyTree &item = info->query();
            const char *name = item.queryName();
            if (name)
            {
                MemoryBuffer mb;
                bool isbin = p->isBinary(name);
                if (isbin)
                {
                    p->getPropBin(name,mb);

                    if (mb.length())
                    {
                        unsigned len = 0;
                        mb.read(len);
                        StringBuffer encodedString;
                        StringBuffer val(len, (const char *) mb.readDirect(len));
                        encodeXML(val, encodedString);
                        ws_info.appendf("<%s>%s</%s>", name, encodedString.str(), name);
                    }
                }
                else
                {
                    StringBuffer tmp;
                    toXML(&item, tmp);
                    ws_info.append(tmp.str());
                }
            }
        }
        ws_info.appendf("</%s>", p->queryName());
        str.setLen(ws_info.str(), ws_info.length());
    }
    else
    {
        MemoryBuffer mb;
        p->getPropBin(name,mb);

        if (mb.length())
        {
            unsigned len;
            mb.read(len);
            str.setLen((const char *) mb.readDirect(len), len);
        }
    }

    return str;

}

IStringVal& CLocalWUWebServicesInfo::getText(const char *name, IStringVal &str) const
{
    str.set(p->queryProp(name));
    return str;
}

void CLocalWUWebServicesInfo::setModuleName(const char *mname)
{
    p->setProp("@module", mname);
}

void CLocalWUWebServicesInfo::setAttributeName(const char *aname)
{
    p->setProp("@attribute", aname);
}

void CLocalWUWebServicesInfo::setDefaultName(const char *dname)
{
    p->setProp("@defaultName", dname);
}

void CLocalWUWebServicesInfo::setWebServicesCRC(unsigned crc)
{
    p->setPropInt("@crc", crc);
}

void CLocalWUWebServicesInfo::setInfo(const char *name, const char *info)
{
    MemoryBuffer m;
    unsigned len = (size32_t)strlen(info);
    serializeLPString(len, info, m);
    p->setPropBin(name, m.length(), m.toByteArray());
}

void CLocalWUWebServicesInfo::setText(const char *name, const char *info)
{
    p->setProp(name, info);
}

//========================================================================================

CLocalWUResult::CLocalWUResult(IPropertyTree *props) : p(props)
{
}

mapEnums resultStatuses[] = {
   { ResultStatusUndefined, "undefined" },
   { ResultStatusCalculated, "calculated" },
   { ResultStatusSupplied, "supplied" },
   { ResultStatusFailed, "failed" },
   { ResultStatusPartial, "partial" },
   { ResultStatusSize, NULL }
};

WUResultStatus CLocalWUResult::getResultStatus() const
{
    return (WUResultStatus ) getEnum(p, "@status", resultStatuses);
}

IStringVal& CLocalWUResult::getResultName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

int CLocalWUResult::getResultSequence() const
{
    return p->getPropInt("@sequence", -1);
}

bool CLocalWUResult::isResultScalar() const
{
    return p->getPropInt("@isScalar", 1) != 0;
}

bool findSize(int size, IntArray &sizes)
{
    ForEachItemIn(idx, sizes)
    {
        if (sizes.item(idx)==size)
            return true;
    }
    return false;
}

void CLocalWUResult::getSchema(IArrayOf<ITypeInfo> &types, StringAttrArray &names, IStringVal * eclText) const
{
    MemoryBuffer schema;
    p->getPropBin("SchemaRaw", schema);
    if (schema.length())
    {
        for (;;)
        {
            StringAttr name;
            schema.read(name);
            if (*schema.readDirect(0)==type_void)
                break;
            names.append(*new StringAttrItem(name));
            types.append(*deserializeType(schema));  // MORE - nested records!
        }
        schema.skip(1);

        if (schema.length() != schema.getPos())
        {
            unsigned eclLen;
            schema.read(eclLen);
            const char * schemaData = (const char *)schema.readDirect(eclLen);
            if (eclText)
            {
                eclText->setLen(schemaData, eclLen);
                if ((eclLen == 0) && names.ordinality())
                {
                    const char * firstName = names.item(0).text;

                    StringBuffer temp;
                    temp.append("RECORD ");
                    types.item(0).getECLType(temp);
                    temp.append(" value{NAMED('").append(firstName).append("')}").append("; END;");
                    eclText->set(temp.str());
                }
            }
        }
    }
}

void readRow(StringBuffer &out, MemoryBuffer &in, TypeInfoArray &types, StringAttrArray &names)
{
    ForEachItemIn(idx, types)
    {
        StringAttrItem &name = names.item(idx);
        ITypeInfo &type = types.item(idx);
        unsigned size = type.getSize();
        switch(type.getTypeCode())
        {
        case type_data:
            if (size==UNKNOWN_LENGTH)
            {
                if (in.remaining() < sizeof(int))
                    throw MakeStringException(WUERR_CorruptResult, "corrupt workunit information");
                in.read(size);
            }
            outputXmlData(size, in.readDirect(size), name.text, out);
            break;
        case type_string:
            if (size==UNKNOWN_LENGTH)
            {
                if (in.remaining() < sizeof(int))
                    throw MakeStringException(WUERR_CorruptResult, "corrupt workunit information");
                in.read(size);
            }
            outputXmlString(size, (const char *) in.readDirect(size), name.text, out);
            break;
        case type_varstring:
            {
                if (size == UNKNOWN_LENGTH)
                    size = (size32_t)strlen((const char *) in.readDirect(0))+1;
                const char * text = (const char *) in.readDirect(size);
                outputXmlString((size32_t)strlen(text), text, name.text, out);
                break;
            }
        case type_unicode:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                    in.read(len);
                outputXmlUnicode(len, (UChar const *) in.readDirect(len*2), name.text, out);
            }
            break;
        case type_utf8:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                {
                    in.read(len);
                    size = rtlUtf8Size(len, in.readDirect(0));
                }
                outputXmlUtf8(len, (const char *) in.readDirect(size), name.text, out);
            }
            break;
        case type_qstring:
            {
                unsigned len = type.getStringLen();
                if (size==UNKNOWN_LENGTH)
                    in.read(len);
                unsigned outlen;
                char *outstr;
                rtlQStrToStrX(outlen, outstr, len, (const char *) in.readDirect(rtlQStrSize(len)));
                outputXmlString(outlen, outstr, name.text, out);
                free(outstr);
                break;
            }
        case type_int: 
        case type_swapint: 
            if (type.isSigned())
            {
                const unsigned char *raw = (const unsigned char *) in.readDirect(size);
                unsigned __int64 cval8 = 0;
                //MORE: I think this is wrong - swapped doesn't mean little/big/
                if (type.isSwappedEndian())
                {
                    unsigned idx = 0;
                    if (raw[idx] & 0x80)
                        cval8 = (__int64)-1;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[idx++];
                }
                else
                {
                    if (raw[size-1] & 0x80)
                        cval8 = (__int64)-1;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[size];
                }
                outputXmlInt((__int64) cval8, name.text, out);
            }
            else
            {
                const unsigned char *raw = (const unsigned char *) in.readDirect(size);
                unsigned __int64 cval8 = 0;
                if (type.isSwappedEndian())
                {
                    unsigned idx = 0;
                    while (size--)
                        cval8 = (cval8 << 8) | raw[idx++];
                }
                else
                {
                    while (size--)
                        cval8 = (cval8 << 8) | raw[size];
                }
                outputXmlUInt(cval8, name.text, out);
            }
            break;
        case type_boolean:
            bool cvalb;
            in.read(cvalb);
            outputXmlBool(cvalb, name.text, out);
            break;
        case type_decimal:
            if (type.isSigned())
                outputXmlDecimal(in.readDirect(size), size, type.getPrecision(), name.text, out);
            else
                outputXmlUDecimal(in.readDirect(size), size, type.getPrecision(), name.text, out);
            break;
        case type_real:
            double cvald;
            switch(size)
            {
            case 4:
                float cvalf;
                in.read(cvalf);
                cvald = cvalf;
                break;
            case 8:
                in.read(cvald);
                break;
            }
            outputXmlReal(cvald, name.text, out);
            break;
        default:
            assertex(!"unexpected type in raw record");
            break;
        }
    }
}

IStringVal& CLocalWUResult::getResultXml(IStringVal &str, bool hidePassword) const
{
    TypeInfoArray types;
    StringAttrArray names;
    getSchema(types, names);

    StringBuffer xml;
    const char * name = p->queryProp("@name");
    if (name)
        xml.appendf("<Dataset name=\'%s\'>\n", name);
    else
        xml.append("<Dataset>\n");

    if (hidePassword && p->getPropBool("Format/@password"))
    {
        xml.append(" <Row>");
        appendXMLTag(xml, name, "****");
        xml.append("</Row>\n");
    }
    else if (p->hasProp("Value"))
    {
        MemoryBuffer raw;
        p->getPropBin("Value", raw);
        unsigned __int64 numrows = getResultRowCount();
        while (numrows--)
        {
            xml.append(" <Row>");
            readRow(xml, raw, types, names);
            xml.append("</Row>\n");
        }
    }
    else if (p->hasProp("xmlValue"))
    {
        xml.append(" <Row>");
        appendXMLTag(xml, name, p->queryProp("xmlValue"));
        xml.append("</Row>\n");
    }

    xml.append("</Dataset>\n");
    str.set(xml.str());
    return str;
}

IProperties *CLocalWUResult::queryResultXmlns()
{
    CriticalBlock block(crit);
    if (xmlns)
        return xmlns;
    xmlns.setown(createProperties());
    Owned<IAttributeIterator> it = p->getAttributes();
    unsigned prefixLen = strlen("@xmlns");
    ForEach(*it)
    {
        const char *name = it->queryName();
        if (!strncmp("@xmlns", name, prefixLen))
        {
            if (name[prefixLen]==':') //normal case
                xmlns->setProp(name+prefixLen+1, it->queryValue());
            else if (!name[prefixLen]) //special case, unprefixed namespace
                xmlns->setProp("xmlns", it->queryValue());
        }
    }
    return xmlns;
}

unsigned CLocalWUResult::getResultFetchSize() const
{
    return p->getPropInt("fetchSize", 100);
}

__int64 CLocalWUResult::getResultTotalRowCount() const
{
    return p->getPropInt64("totalRowCount", -1);
}

__int64 CLocalWUResult::getResultRowCount() const
{
    return p->getPropInt64("rowCount", 0);
}

void CLocalWUResult::getResultDataset(IStringVal & ecl, IStringVal & defs) const
{
    ecl.set(p->queryProp("datasetEcl"));
    defs.set(p->queryProp("datasetEclDefs"));
}

IStringVal& CLocalWUResult::getResultLogicalName(IStringVal & val) const
{
    val.set(p->queryProp("logicalName"));
    return val;
}

IStringVal& CLocalWUResult::getResultKeyField(IStringVal & ecl) const
{
    ecl.set(p->queryProp("keyField"));
    return ecl;
}

unsigned CLocalWUResult::getResultRequestedRows() const
{
    return p->getPropInt("requestedRows", 1);
}

IStringVal& CLocalWUResult::getResultEclSchema(IStringVal & str) const
{
    TypeInfoArray types;
    StringAttrArray names;
    getSchema(types, names, &str);
    return str;
}

IStringVal& CLocalWUResult::getResultRecordSizeEntry(IStringVal & str) const
{
    str.set(p->queryProp("@recordSizeEntry"));
    return str;
}

IStringVal& CLocalWUResult::getResultTransformerEntry(IStringVal & str) const
{
    str.set(p->queryProp("@transformerEntry"));
    return str;
}

__int64 CLocalWUResult::getResultRowLimit() const
{
    return p->getPropInt64("@rowLimit");
}

IStringVal& CLocalWUResult::getResultFilename(IStringVal & str) const
{
    str.set(p->queryProp("@tempFilename"));
    return str;
}

IStringVal& CLocalWUResult::getResultFieldOpt(const char *name, IStringVal &str) const
{
    str.clear();
    if (!name || !*name)
        return str;
    IPropertyTree *format = p->queryPropTree("Format");
    if (!format)
        return str;
    VStringBuffer xpath("@%s", name);
    str.set(format->queryProp(xpath));
    return str;
}

void CLocalWUResult::setResultStatus(WUResultStatus status)
{
    setEnum(p, "@status", status, resultStatuses);
    if (status==ResultStatusUndefined)
        p->removeProp("Value");
}

void CLocalWUResult::setResultName(const char *s)
{
    p->setProp("@name", s);
}
void CLocalWUResult::setResultSequence(unsigned seq)
{
    p->setPropInt("@sequence", seq);
}
void CLocalWUResult::setResultSchemaRaw(unsigned size, const void *schema)
{
    p->setPropBin("SchemaRaw", size, schema);
}
void CLocalWUResult::setResultXmlns(const char *prefix, const char *uri)
{
    StringBuffer xpath("@xmlns");
    if (prefix && *prefix)
        xpath.append(':').append(prefix);
    p->setProp(xpath, uri);
}

void CLocalWUResult::setResultFieldOpt(const char *name, const char *value)
{
    if (!name || !*name)
        return;
    IPropertyTree *format = ensurePTree(p, "Format");
    VStringBuffer xpath("@%s", name);
    format->setProp(xpath, value);
}

void CLocalWUResult::setResultScalar(bool isScalar)
{
    p->setPropInt("@isScalar", (int) isScalar);
    if (isScalar)
        setResultTotalRowCount(1);
}
void CLocalWUResult::setResultRaw(unsigned len, const void *data, WUResultFormat format)
{
    p->setPropBin("Value", len, data);
    setResultStatus(ResultStatusSupplied);
    setResultFormat(format);
}

void CLocalWUResult::setResultFormat(WUResultFormat format)
{
    switch (format)
    {
    case ResultFormatXml: 
        p->setProp("@format","xml"); 
        break;
    case ResultFormatXmlSet: 
        p->setProp("@format","xmlSet"); 
        break;
    case ResultFormatCsv: 
        p->setProp("@format","csv"); 
        break;
    default:
        p->removeProp("@format");
        break;
    }
}

void CLocalWUResult::setResultXML(const char *val)
{
    p->setProp("xmlValue", val);
}

void CLocalWUResult::addResultRaw(unsigned len, const void *data, WUResultFormat format)
{
    p->appendPropBin("Value", len, data);
    setResultStatus(ResultStatusPartial);
    const char *existingFormat = p->queryProp("@format");
    const char *formatStr = NULL;
    switch (format)
    {
    case ResultFormatXml: 
        formatStr = "xml"; 
        break;
    case ResultFormatXmlSet: 
        formatStr = "xmlSet";
        break;
    case ResultFormatCsv: 
        formatStr = "csv";
        break;
    default:
        p->removeProp("@format");
        break;
    }
    if (format)
    {
        if (existingFormat)
        {
            if (0 != stricmp(formatStr, existingFormat))
                throw MakeStringException(WUERR_ResultFormatMismatch, "addResult format %s, does not match existing format %s", formatStr, existingFormat);
        }
        else
            p->setProp("@format", formatStr);

    }
}

void CLocalWUResult::setResultFetchSize(unsigned rows)
{
    p->setPropInt("fetchSize", rows);
}

void CLocalWUResult::setResultTotalRowCount(__int64 rows)
{
    p->setPropInt64("totalRowCount", rows);
}

void CLocalWUResult::setResultRowCount(__int64 rows)
{
    p->setPropInt64("rowCount", rows);
}

void CLocalWUResult::setResultDataset(const char *ecl, const char *defs)
{
    p->setProp("datasetEcl", ecl);
    p->setProp("datasetEclDefs", defs);
}

void CLocalWUResult::setResultLogicalName(const char *logicalName)
{
    p->setProp("logicalName", logicalName);
}

void CLocalWUResult::setResultKeyField(const char *ecl)
{
    p->setProp("keyField", ecl);
}

void CLocalWUResult::setResultRequestedRows(unsigned rows)
{
    p->setPropInt("requestedRows", rows);
}

void CLocalWUResult::setResultRecordSizeEntry(const char * entry)
{
    p->setProp("@recordSizeEntry", entry);
}

void CLocalWUResult::setResultTransformerEntry(const char * entry)
{
    p->setProp("@transformerEntry", entry);
}


void CLocalWUResult::setResultRowLimit(__int64 value)
{
    p->setPropInt64("@rowLimit", value);
}

void CLocalWUResult::setResultFilename(const char * name)
{
    p->setProp("@tempFilename", name);
}

// MORE - it's an undetected error if we call getResult... of a type that does not match schema

__int64 CLocalWUResult::getResultInt() const
{
    __int64 result = 0;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
        result = p->getPropInt64("xmlValue");
    return result;
}

bool CLocalWUResult::getResultBool() const
{
    bool result = false;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
        result = p->getPropBool("xmlValue");
    return result;
}

double CLocalWUResult::getResultReal() const
{
    double result = 0;
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
        s.read(result);
    else
    {
        const char *xmlVal = p->queryProp("xmlValue");
        if (xmlVal)
            result = atof(xmlVal);
    }
    return result;
}

void CLocalWUResult::getResultDecimal(void * val, unsigned len, unsigned precision, bool isSigned) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        assertex(s.length() == len);
        s.read(len, val);
    }
    else
    {
        const char *xmlVal = p->queryProp("xmlValue");
        if (xmlVal)
        {
            Decimal d;
            d.setString(strlen(xmlVal), xmlVal);
            if (isSigned)
                d.getDecimal(len, precision, val);
            else
                d.getUDecimal(len, precision, val);
        }
        else
            memset(val, 0, len);
    }
}

IStringVal& CLocalWUResult::getResultString(IStringVal & str, bool hidePassword) const
{
    if (hidePassword && p->getPropBool("@password"))
    {
        str.set("****");
        return str;
    }
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        unsigned len;
        s.read(len);
        str.setLen((const char *) s.readDirect(len), len);
    }
    else
    {
        p->getPropBin("xmlValue", s);
        if (p->isBinary("xmlValue"))
            str.setLen(s.toByteArray(), s.length());
        else
        {
            char *ascii = rtlUtf8ToVStr(rtlUtf8Length(s.length(), s.toByteArray()), s.toByteArray());
            str.set(ascii);
            rtlFree(ascii);
        }
    }
    return str;
}


WUResultFormat CLocalWUResult::getResultFormat() const
{
    const char * format = p->queryProp("@format");
    if (!format)
        return ResultFormatRaw;
    else if (strcmp(format, "xml") == 0)
        return ResultFormatXml;
    else if (strcmp(format, "xmlSet") == 0)
        return ResultFormatXmlSet;
    else if (strcmp(format, "csv") == 0)
        return ResultFormatCsv;
    else
        throw MakeStringException(WUERR_InvalidResultFormat, "Unrecognised result format %s", format);
}

IDataVal& CLocalWUResult::getResultRaw(IDataVal & data, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    unsigned len = s.length();
    if (len)
    {
        WUResultFormat format = getResultFormat();
        if (format == ResultFormatXml || format == ResultFormatXmlSet)
        {
            if (!xmlTransformer)
                throw MakeStringException(WUERR_MissingFormatTranslator, "No transformer supplied to translate XML format result");
            xmlTransformer->transform(data, len, s.readDirect(len), format == ResultFormatXml);
        }
        else if (format == ResultFormatCsv)
        {
            if (!csvTransformer)
                throw MakeStringException(WUERR_MissingFormatTranslator, "No transformer supplied to translate Csv format result");
            csvTransformer->transform(data, len, s.readDirect(len), true);
        }
        else
            data.setLen(s.readDirect(len), len);
    }
    else
        data.clear();
    return data;
}

unsigned CLocalWUResult::getResultHash() const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    unsigned len = s.length();
    const byte * data = (const byte *)s.toByteArray();
    return ~hashc(data, len, ~0);
}


IDataVal& CLocalWUResult::getResultUnicode(IDataVal & data) const
{
    MemoryBuffer s;
    p->getPropBin("Value", s);
    if (s.length())
    {
        unsigned len;
        s.read(len);
        data.setLen(s.readDirect(len*2), len*2);
    }
    else
    {
        StringBuffer utf8;
        if (p->getProp("xmlValue", utf8))
        {
            unsigned outlen;
            UChar *out;
            rtlUtf8ToUnicodeX(outlen, out, utf8.length(), utf8.str());
            data.setLen(out, outlen*2);
            rtlFree(out);
        }
        else
            data.clear();
    }
    return data;
}

__int64 CLocalWUResult::getResultRawSize(IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    WUResultFormat format = getResultFormat();
    if (format == ResultFormatRaw)
    {
        //MORE: This should not load the whole property...
        MemoryBuffer s;
        p->getPropBin("Value", s);
        return s.length();
    }
    else
    {
        MemoryBuffer temp;
        MemoryBuffer2IDataVal adaptor(temp);
        getResultRaw(adaptor, xmlTransformer, csvTransformer);
        return temp.length();
    }
}

IDataVal& CLocalWUResult::getResultRaw(IDataVal & data, __int64 from, __int64 length, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const
{
    WUResultFormat format = getResultFormat();
    if (format != ResultFormatRaw)
    {
        MemoryBuffer temp;
        MemoryBuffer2IDataVal adaptor(temp);
        getResultRaw(adaptor, xmlTransformer, csvTransformer);

        unsigned len = temp.length();
        if (from > len) from = len;
        if (from + length > len) length = len - from;
        data.setLen(temp.readDirect(len) + from, (size32_t)length);
        return data;
    }
    else
    {
        //MORE: This should not load the whole property, and should be different from the code above...
        MemoryBuffer s;
        p->getPropBin("Value", s);
        unsigned len = s.length();
        if (from > len) from = len;
        if (from + length > len) length = len - from;
        data.setLen(s.readDirect(len) + from, (size32_t)length);
        return data;
    }
}

bool CLocalWUResult::getResultIsAll() const
{
    return p->getPropBool("@isAll", false);
}

// MORE - it's an undetected error if we call setResult... of a type that does not match schema

void CLocalWUResult::setResultInt(__int64 val)
{
    // Note: we always serialize scalar integer results as int8, and schema must reflect this
    MemoryBuffer m;
    serializeInt8(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultUInt(unsigned __int64 val)
{
    setResultInt((__int64) val);
}

void CLocalWUResult::setResultReal(double val)
{
    // Note: we always serialize scalar real results as real8, and schema must reflect this
    MemoryBuffer m;
    serializeReal8(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultBool(bool val)
{
    MemoryBuffer m;
    serializeBool(val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultString(const char *val, unsigned len)
{
    // Note: we always serialize scalar strings with length prefix, and schema must reflect this
    MemoryBuffer m;
    serializeLPString(len, val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultUnicode(const void *val, unsigned len)
{
    // Note: we always serialize scalar strings with length prefix, and schema must reflect this
    MemoryBuffer m;
    m.append(len).append(len*2, val);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultData(const void *val, unsigned len)
{
    // Note: we always serialize scalar data with length prefix, and schema must reflect this
    MemoryBuffer m;
    serializeLPString(len, (const char *)val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultDecimal(const void *val, unsigned len)
{
    // Note: serialized as data but with length known from schema
    MemoryBuffer m;
    serializeFixedData(len, val, m);
    p->setPropBin("Value", m.length(), m.toByteArray());
    setResultRowCount(1);
    setResultTotalRowCount(1);
}

void CLocalWUResult::setResultRow(unsigned len, const void * data)
{
    p->setPropBin("Value", len, data);
    setResultRowCount(1);
    setResultTotalRowCount(1);
    setResultFormat(ResultFormatRaw);
}
void CLocalWUResult::setResultIsAll(bool value)
{
    p->setPropBool("@isAll", value);
}

//==========================================================================================

CLocalWUPlugin::CLocalWUPlugin(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUPlugin::getPluginName(IStringVal &str) const
{
    str.set(p->queryProp("@dllname"));
    return str;
}

IStringVal& CLocalWUPlugin::getPluginVersion(IStringVal &str) const
{
    str.set(p->queryProp("@version"));
    return str;
}

void CLocalWUPlugin::setPluginName(const char *str)
{
    p->setProp("@dllname", str);
}

void CLocalWUPlugin::setPluginVersion(const char *str)
{
    p->setProp("@version", str);
}

//==========================================================================================

CLocalWULibrary::CLocalWULibrary(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWULibrary::getName(IStringVal &str) const
{
    str.set(p->queryProp("@name"));
    return str;
}

void CLocalWULibrary::setName(const char *str)
{
    p->setProp("@name", str);
}

//==========================================================================================

CLocalWUException::CLocalWUException(IPropertyTree *props) : p(props)
{
}

IStringVal& CLocalWUException::getExceptionSource(IStringVal &str) const
{
    str.set(p->queryProp("@source"));
    return str;
}

IStringVal& CLocalWUException::getExceptionMessage(IStringVal &str) const
{
    str.set(p->queryProp(NULL));
    return str;
}

unsigned  CLocalWUException::getExceptionCode() const
{
    return p->getPropInt("@code", 0);
}

ErrorSeverity CLocalWUException::getSeverity() const
{
    return (ErrorSeverity)p->getPropInt("@severity", SeverityError);
}

IStringVal & CLocalWUException::getTimeStamp(IStringVal & dt) const
{
    dt.set(p->queryProp("@time"));
    return dt;
}

IStringVal & CLocalWUException::getExceptionFileName(IStringVal & str) const
{
    str.set(p->queryProp("@filename"));
    return str;
}

unsigned CLocalWUException::getExceptionLineNo() const
{
    return p->getPropInt("@row", 0);
}

unsigned CLocalWUException::getExceptionColumn() const
{
    return p->getPropInt("@col", 0);
}

unsigned CLocalWUException::getSequence() const
{
    return p->getPropInt("@sequence", 0);
}

void CLocalWUException::setExceptionSource(const char *str)
{
    p->setProp("@source", str);
}

void CLocalWUException::setExceptionMessage(const char *str)
{
    p->setProp(NULL, str);
}

void CLocalWUException::setExceptionCode(unsigned code)
{
    p->setPropInt("@code", code);
}

void CLocalWUException::setSeverity(ErrorSeverity level)
{
    p->setPropInt("@severity", level);
}

void CLocalWUException::setTimeStamp(const char *str)
{
    p->setProp("@time", str);
}

void CLocalWUException::setExceptionFileName(const char *str)
{
    p->setProp("@filename", str);
}

void CLocalWUException::setExceptionLineNo(unsigned r)
{
    p->setPropInt("@row", r);
}

void CLocalWUException::setExceptionColumn(unsigned c)
{
    p->setPropInt("@col", c);
}

//==========================================================================================

CLocalWUAppValue::CLocalWUAppValue(IPropertyTree *props, unsigned child) : p(props)
{
    StringAttrBuilder propPath(prop);
    propPath.append("*[").append(child).append("]");
}

const char * CLocalWUAppValue::queryApplication() const
{
    return p->queryName();
}

const char * CLocalWUAppValue::queryName() const
{
    IPropertyTree* val=p->queryPropTree(prop.str());
    if(val)
        return val->queryName();
    return ""; // Should not happen in normal usage
}

const char * CLocalWUAppValue::queryValue() const
{
    return p->queryProp(prop.str());
}

//==========================================================================================

CLocalWUStatistic::CLocalWUStatistic(IPropertyTree *props) : p(props)
{
}

IStringVal & CLocalWUStatistic::getCreator(IStringVal & str) const
{
    const char * creator = p->queryProp("@creator");
    str.set(creator);
    return str;
}


IStringVal & CLocalWUStatistic::getDescription(IStringVal & str, bool createDefault) const
{
    const char * desc = p->queryProp("@desc");
    if (desc)
    {
        str.set(desc); // legacy and in case it is overridden
    }
    else if (createDefault)
    {
        StatisticKind kind = getKind();
        assertex(kind != StKindNone);

        const char * scope = p->queryProp("@scope");
        assertex(scope);

        //Clean up the format of the scope when converting it to a description
        StringBuffer descriptionText;
        if (streq(scope, GLOBAL_SCOPE))
        {
            const char * creator = p->queryProp("@creator");
            descriptionText.append(creator).append(":");
            queryLongStatisticName(descriptionText, kind);
        }
        else
        {
            loop
            {
                char c = *scope++;
                if (!c)
                    break;
                if (c == ':')
                    descriptionText.append(": ");
                else
                    descriptionText.append(c);
            }

            if (kind != StTimeElapsed)
                queryLongStatisticName(descriptionText.append(": "), kind);
        }

        str.set(descriptionText);
    }
    else
        str.clear();

    return str;
}

IStringVal & CLocalWUStatistic::getType(IStringVal & str) const
{
    StatisticKind kind = getKind();
    if (kind != StKindNone)
        str.set(queryStatisticName(kind));
    return str;
}

IStringVal & CLocalWUStatistic::getFormattedValue(IStringVal & str) const
{
    StringBuffer formatted;
    formatStatistic(formatted, getValue(), getMeasure());
    str.set(formatted);
    return str;
}

StatisticCreatorType CLocalWUStatistic::getCreatorType() const
{
    return queryCreatorType(p->queryProp("@c"));
}

StatisticScopeType CLocalWUStatistic::getScopeType() const
{
    return queryScopeType(p->queryProp("@s"));
}

StatisticKind CLocalWUStatistic::getKind() const
{
    return queryStatisticKind(p->queryProp("@kind"));
}

IStringVal & CLocalWUStatistic::getScope(IStringVal & str) const
{
    const char * scope = p->queryProp("@scope");
    str.set(scope);
    return str;
}

StatisticMeasure CLocalWUStatistic::getMeasure() const
{
    return queryMeasure(p->queryProp("@unit"));
}

unsigned __int64 CLocalWUStatistic::getValue() const
{
    return p->getPropInt64("@value", 0);
}

unsigned __int64 CLocalWUStatistic::getCount() const
{
    return p->getPropInt64("@count", 0);
}

unsigned __int64 CLocalWUStatistic::getMax() const
{
    return p->getPropInt64("@max", 0);
}

unsigned __int64 CLocalWUStatistic::getTimestamp() const
{
    return p->getPropInt64("@ts", 0);
}


bool CLocalWUStatistic::matches(const IStatisticsFilter * filter) const
{
    if (!filter)
        return true;
    const char * creator = p->queryProp("@creator");
    const char * scope = p->queryProp("@scope");
    return filter->matches(getCreatorType(), creator, getScopeType(), scope, getMeasure(), getKind());
}

//==========================================================================================

CLocalWULegacyTiming::CLocalWULegacyTiming(IPropertyTree *props) : p(props)
{
}

IStringVal & CLocalWULegacyTiming::getCreator(IStringVal & str) const
{
    str.clear();
    return str;
}


IStringVal & CLocalWULegacyTiming::getDescription(IStringVal & str, bool createDefault) const
{
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal & CLocalWULegacyTiming::getType(IStringVal & str) const
{
    str.set(queryStatisticName(StTimeElapsed));
    return str;
}

IStringVal & CLocalWULegacyTiming::getFormattedValue(IStringVal & str) const
{
    StringBuffer formatted;
    formatStatistic(formatted, getValue(), getMeasure());
    str.set(formatted);
    return str;
}

StatisticCreatorType CLocalWULegacyTiming::getCreatorType() const
{
    return SCTunknown;
}

StatisticScopeType CLocalWULegacyTiming::getScopeType() const
{
    return SSTnone;
}

StatisticKind CLocalWULegacyTiming::getKind() const
{
    return StTimeElapsed;
}

IStringVal & CLocalWULegacyTiming::getScope(IStringVal & str) const
{
    str.clear();
    return str;
}

StatisticMeasure CLocalWULegacyTiming::getMeasure() const
{
    return SMeasureTimeNs;
}

unsigned __int64 CLocalWULegacyTiming::getValue() const
{
    return p->getPropInt64("@duration", 0) * 1000000;
}

unsigned __int64 CLocalWULegacyTiming::getCount() const
{
    return p->getPropInt64("@count", 0);
}

unsigned __int64 CLocalWULegacyTiming::getMax() const
{
    return p->getPropInt64("@max", 0);
}

unsigned __int64 CLocalWULegacyTiming::getTimestamp() const
{
    return 0;
}

bool CLocalWULegacyTiming::matches(const IStatisticsFilter * filter) const
{
    if (!filter)
        return true;
    const char * creator = p->queryProp("@creator");
    const char * scope = p->queryProp("@scope");
    return filter->matches(SCTall, NULL, SSTall, NULL, getMeasure(), getKind());
}

//==========================================================================================

extern WORKUNIT_API ILocalWorkUnit * createLocalWorkUnit(const char *xml)
{
    Owned<CLocalWorkUnit> cw = new CLocalWorkUnit((ISecManager *) NULL, NULL);
    if (xml)
        cw->loadPTree(createPTreeFromXMLString(xml));
    else
    {
        Owned<IPropertyTree> p = createPTree("W_LOCAL");
        p->setProp("@xmlns:xsi", "http://www.w3.org/1999/XMLSchema-instance");
        cw->loadPTree(p.getClear());
    }

    ILocalWorkUnit* ret = QUERYINTERFACE(&cw->lockRemote(false), ILocalWorkUnit);
    return ret;
}

void exportWorkUnitToXMLWithHiddenPasswords(IPropertyTree *p, IIOStream &out, unsigned extraXmlFlags)
{
    const char *name = p->queryName();
    if (!name)
        name = "__unnamed__";
    StringBuffer temp;
    writeStringToStream(out, appendPTreeOpenTag(temp, p, name, 1));

    Owned<IPropertyTreeIterator> elems = p->getElements("*", iptiter_sort);
    ForEach(*elems)
    {
        IPropertyTree &elem = elems->query();
        if (streq(elem.queryName(), "Parameters"))
        {
            writeStringToStream(out, appendPTreeOpenTag(temp.clear().append(' '), &elem, "Parameters", 2).append('\n'));
            Owned<IPropertyTreeIterator> params = elem.getElements("*", iptiter_sort);
            ForEach(*params)
            {
                IPropertyTree &param = params->query();
                const char *paramname = param.queryName();
                VStringBuffer xpath("Variables/Variable[@name='%s']/Format/@password", paramname);
                if (p->getPropBool(xpath))
                    writeStringToStream(out, appendXMLTag(temp.clear().append("  "), paramname, "****").append('\n'));
                else
                {
                    toXML(&param, out, 2, XML_Format|XML_SortTags|extraXmlFlags);
                }
            }
            writeStringToStream(out, appendXMLCloseTag(temp.clear().append(' '), "Parameters").append('\n'));
        }
        else if (streq(elem.queryName(), "Variables"))
        {
            writeStringToStream(out, appendPTreeOpenTag(temp.clear().append(' '), &elem, "Variables", 2).append('\n'));
            Owned<IPropertyTreeIterator> vars = elem.getElements("*", iptiter_sort);
            ForEach(*vars)
            {
                Owned<IPropertyTree> var = LINK(&vars->query());
                if (var->getPropBool("Format/@password"))
                {
                    var.setown(createPTreeFromIPT(var)); //copy and remove password values
                    var->removeProp("Value");
                    var->removeProp("xmlValue");
                }
                toXML(var, out, 2, XML_Format|XML_SortTags|extraXmlFlags);
            }
            writeStringToStream(out, appendXMLCloseTag(temp.clear().append(' '), "Variables").append('\n'));
        }
        else
            toXML(&elem, out, 1, XML_Format|XML_SortTags|extraXmlFlags);
    }
    writeStringToStream(out, appendXMLCloseTag(temp.clear(), name));
}

StringBuffer &exportWorkUnitToXMLWithHiddenPasswords(IPropertyTree *p, StringBuffer &str)
{
    class CAdapter : public CInterface, implements IIOStream
    {
        StringBuffer &out;
    public:
        IMPLEMENT_IINTERFACE;
        CAdapter(StringBuffer &_out) : out(_out) { }
        virtual void flush() { }
        virtual size32_t read(size32_t len, void * data) { UNIMPLEMENTED; return 0; }
        virtual size32_t write(size32_t len, const void * data) { out.append(len, (const char *)data); return len; }
    } adapter(str);
    exportWorkUnitToXMLWithHiddenPasswords(p->queryBranch(NULL), adapter, 0);
    return str;
}

void exportWorkUnitToXMLFileWithHiddenPasswords(IPropertyTree *p, const char *filename, unsigned extraXmlFlags)
{
    OwnedIFile ifile = createIFile(filename);
    OwnedIFileIO ifileio = ifile->open(IFOcreate);
    Owned<IIOStream> stream = createIOStream(ifileio);
    exportWorkUnitToXMLWithHiddenPasswords(p->queryBranch(NULL), *stream, extraXmlFlags);
}

extern WORKUNIT_API StringBuffer &exportWorkUnitToXML(const IConstWorkUnit *wu, StringBuffer &str, bool unpack, bool includeProgress, bool hidePasswords)
{
    // MORE - queryPTree isn't really safe without holding CLocalWorkUnit::crit - really need to move these functions into CLocalWorkunit
    const IExtendedWUInterface *ewu = queryExtendedWU(wu);
    if (ewu)
    {
        Linked<IPropertyTree> p;
        if (unpack||includeProgress)
            p.setown(ewu->getUnpackedTree(includeProgress));
        else
            p.set(ewu->queryPTree());
        if (hidePasswords && p->hasProp("Variables/Variable[Format/@password]"))
            return exportWorkUnitToXMLWithHiddenPasswords(p, str);
        toXML(p, str, 0, XML_Format|XML_SortTags);
    }
    else
        str.append("Unrecognized workunit format");
    return str;
}

extern WORKUNIT_API void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool unpack, bool includeProgress, bool hidePasswords)
{
    const IExtendedWUInterface *ewu = queryExtendedWU(wu);
    if (ewu)
    {
        Linked<IPropertyTree> p;
        if (unpack||includeProgress)
            p.setown(ewu->getUnpackedTree(includeProgress));
        else
            p.set(ewu->queryPTree());
        if (hidePasswords && p->hasProp("Variables/Variable[Format/@password]"))
            return exportWorkUnitToXMLFileWithHiddenPasswords(p, filename, extraXmlFlags);
        saveXML(filename, p, 0, XML_Format|XML_SortTags|extraXmlFlags);
    }
    else
        throw makeStringException(0, "Unrecognized workunit format");
}


extern WORKUNIT_API void submitWorkUnit(const char *wuid, const char *username, const char *password)
{
    MemoryBuffer buffer;
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0); // MORE - security token?
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
    assertex(workunit);

    SCMStringBuffer token;
    createToken(wuid, username, password, token);
    workunit->setSecurityToken(token.str());
    StringAttr clusterName(workunit->queryClusterName());
    if (!clusterName.length()) 
        throw MakeStringException(WUERR_InvalidCluster, "No target cluster specified");
    workunit->commit();
    workunit.clear();
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
    if (!clusterInfo) 
        throw MakeStringException(WUERR_InvalidCluster, "Unknown cluster %s", clusterName.str());
    SCMStringBuffer serverQueue;
    clusterInfo->getServerQueue(serverQueue);
    assertex(serverQueue.length());
    Owned<IJobQueue> queue = createJobQueue(serverQueue.str());
    if (!queue.get()) 
        throw MakeStringException(WUERR_InvalidQueue, "Could not create workunit queue");

    IJobQueueItem *item = createJobQueueItem(wuid);
    queue->enqueue(item);
}
extern WORKUNIT_API void abortWorkUnit(const char *wuid)
{
    StringBuffer xpath("/WorkUnitAborts/");
    xpath.append(wuid);
    Owned<IRemoteConnection> acon = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE, SDS_LOCK_TIMEOUT);
    acon->queryRoot()->setPropInt(NULL, 1);
}
extern WORKUNIT_API void secSubmitWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser)
{
    if (checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Write, "Submit", true, true))
        submitWorkUnit(wuid, secuser.getName(), secuser.credentials().getPassword());
}

extern WORKUNIT_API void secAbortWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser)
{
    if (checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Write, "Submit", true, true))
        abortWorkUnit(wuid);
}

extern WORKUNIT_API void submitWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return secSubmitWorkUnit(wuid, *secmgr, *secuser);
    if (secuser)
        return submitWorkUnit(wuid, secuser->getName(), secuser->credentials().getPassword());
    submitWorkUnit(wuid, "", "");
}

extern WORKUNIT_API void abortWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (secmgr && secuser)
        return secAbortWorkUnit(wuid, *secmgr, *secuser);
    abortWorkUnit(wuid);
}

bool CLocalWorkUnit::hasWorkflow() const
{
    return p->hasProp("Workflow");
}

unsigned CLocalWorkUnit::queryEventScheduledCount() const
{
    CriticalBlock block(crit);
    return p->getPropInt("Workflow/@eventScheduledCount", 0);
}

void CLocalWorkUnit::incEventScheduledCount()
{
    CriticalBlock block(crit);
    p->setPropInt("Workflow/@eventScheduledCount", p->getPropInt("Workflow/@eventScheduledCount", 0)+1);
}

IPropertyTree * CLocalWorkUnit::queryWorkflowTree() const
{
    CriticalBlock block(crit);
    return p->queryPropTree("Workflow");
}

IConstWorkflowItemIterator* CLocalWorkUnit::getWorkflowItems() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    if(!workflowIteratorCached)
    {
        assertex(!workflowIterator);
        Owned<IPropertyTree> s = p->getPropTree("Workflow");
        if(s)
            workflowIterator.setown(createWorkflowItemIterator(s)); 
        workflowIteratorCached = true;
    }
    return workflowIterator.getLink();
}

IWorkflowItemArray * CLocalWorkUnit::getWorkflowClone() const
{
    unsigned count = 0;
    Owned<IConstWorkflowItemIterator> iter = getWorkflowItems();
    for(iter->first(); iter->isValid(); iter->next())
        count++;
    Owned<IWorkflowItemArray> array = createWorkflowItemArray(count);
    for(iter->first(); iter->isValid(); iter->next())
        array->addClone(iter->query());
    return array.getLink();
}

IWorkflowItem * CLocalWorkUnit::addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    workflowIterator.clear();
    workflowIteratorCached = false;
    IPropertyTree * s = p->queryPropTree("Workflow");
    if(!s)
        s = p->addPropTree("Workflow", createPTree("Workflow"));
    return createWorkflowItem(s, wfid, type, mode, success, failure, recovery, retriesAllowed, contingencyFor);
}

IWorkflowItemIterator * CLocalWorkUnit::updateWorkflowItems()
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    if(!workflowIterator)
    {
        IPropertyTree * s = p->queryPropTree("Workflow");
        if(!s)
            s = p->addPropTree("Workflow", createPTree("Workflow"));
        workflowIterator.setown(createWorkflowItemIterator(s)); 
        workflowIteratorCached = true;
    }
    return workflowIterator.getLink();
}

void CLocalWorkUnit::syncRuntimeWorkflow(IWorkflowItemArray * array)
{
    Owned<IWorkflowItemIterator> iter = updateWorkflowItems();
    Owned<IWorkflowItem> item;
    for(iter->first(); iter->isValid(); iter->next())
    {
        item.setown(iter->get());
        item->syncRuntimeData(array->queryWfid(item->queryWfid()));
    }
    workflowIterator.clear();
    workflowIteratorCached = false;
}

void CLocalWorkUnit::resetWorkflow()
{
    if (hasWorkflow())
    {
        Owned<IWorkflowItemIterator> iter = updateWorkflowItems();
        Owned<IWorkflowItem> wf;
        for(iter->first(); iter->isValid(); iter->next())
        {
            wf.setown(iter->get());
            wf->reset();
        }
        workflowIterator.clear();
        workflowIteratorCached = false;
    }
}

void CLocalWorkUnit::schedule()
{
    CriticalBlock block(crit);
    if(queryEventScheduledCount() == 0) return;

    switch(getState())
    {
    case WUStateCompleted:
        setState(WUStateWait);
        break;
    case WUStateFailed:
    case WUStateArchived:
    case WUStateAborting:
    case WUStateAborted:
    case WUStateScheduled:
        throw MakeStringException(WUERR_CannotSchedule, "Cannot schedule workunit in this state");
    }

    StringBuffer rootPath;
    rootPath.append("/Schedule/").append(queryClusterName());
    Owned<IRemoteConnection> conn = querySDS().connect(rootPath.str(), myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    Owned<IPropertyTree> root = conn->getRoot();
    if(!root->hasChildren())
    {
        StringBuffer addPath;
        addPath.append("/Schedulers/").append(queryClusterName());
        Owned<IRemoteConnection> addConn = querySDS().connect(addPath.str(), myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    }

    char const * wuid = p->queryName();
    StringBuffer xpath("*/*/");
    ncnameEscape(wuid, xpath);
    bool more;
    do more = root->removeProp(xpath.str()); while(more);
        
    Owned<IConstWorkflowItemIterator> iter = getWorkflowItems();
    Owned<IWorkflowEvent> event;
    Owned<IPropertyTree> branch1, branch2;
    for(iter->first(); iter->isValid(); iter->next())
    {
        event.setown(iter->query()->getScheduleEvent());
        if(!event) continue;
        ncnameEscape(event->queryName(), xpath.clear());
        ensurePTree(root, xpath.str());
        branch1.setown(root->getPropTree(xpath.str()));
        ncnameEscape(event->queryText(), xpath.clear());
        ensurePTree(branch1, xpath.str());
        branch2.setown(branch1->getPropTree(xpath.str()));
        ncnameEscape(wuid, xpath.clear());
        ensurePTree(branch2, xpath.str());
    }
}

void CLocalWorkUnit::deschedule()
{
    if(queryEventScheduledCount() == 0) return;
    if(getState() == WUStateWait)
        setState(WUStateCompleted);
    doDescheduleWorkkunit(p->queryName());
}

mapEnums localFileUploadTypes[] = {
    { UploadTypeFileSpray, "FileSpray" },
    { UploadTypeWUResult, "WUResult" },
    { UploadTypeWUResultCsv, "WUResultCsv" },
    { UploadTypeWUResultXml, "WUResultXml" },
    { UploadTypeSize, NULL }
};

class CLocalFileUpload : public CInterface, implements IConstLocalFileUpload
{
public:
    CLocalFileUpload(IPropertyTree * _tree) : tree(_tree) {}
    CLocalFileUpload(unsigned id, LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
    {
        tree.setown(createPTree());
        tree->setPropInt("@id", id);
        setEnum(tree, "@type", type, localFileUploadTypes);
        tree->setProp("@source", source);
        tree->setProp("@destination", destination);
        if (eventTag)
            tree->setProp("@eventTag", eventTag);
    }
    IMPLEMENT_IINTERFACE;
    IPropertyTree * getTree() { return tree.getLink(); }

    virtual unsigned queryID() const { return tree->getPropInt("@id"); }
    virtual LocalFileUploadType queryType() const { return (LocalFileUploadType)getEnum(tree, "@type", localFileUploadTypes); }
    virtual IStringVal & getSource(IStringVal & ret) const { ret.set(tree->queryProp("@source")); return ret; }
    virtual IStringVal & getDestination(IStringVal & ret) const { ret.set(tree->queryProp("@destination")); return ret; }
    virtual IStringVal & getEventTag(IStringVal & ret) const { if(tree->hasProp("@eventTag")) ret.set(tree->queryProp("@eventTag")); else ret.clear(); return ret; }

private:
    Owned<IPropertyTree> tree;
};

class CLocalFileUploadIterator : public CInterface, implements IConstLocalFileUploadIterator
{
public:
    CLocalFileUploadIterator(IPropertyTree * _tree) : tree(_tree), iter(tree->getElements("LocalFileUpload")) {}
    IMPLEMENT_IINTERFACE;
    bool first() { return iter->first(); }
    bool isValid() { return iter->isValid(); }
    bool next() { return iter->next(); }
    IConstLocalFileUpload * get() { return new CLocalFileUpload(&iter->get()); }

private:
    Owned<IPropertyTree> tree;
    Owned<IPropertyTreeIterator> iter;
};

IConstLocalFileUploadIterator * CLocalWorkUnit::getLocalFileUploads() const
{
    // For this to be legally called, we must have the read-able interface. So we are already locked for (at least) read.
    CriticalBlock block(crit);
    Owned<IPropertyTree> s = p->getPropTree("LocalFileUploads");
    if(s)
        return new CLocalFileUploadIterator(s.getClear());
    else
        return NULL;
}

bool CLocalWorkUnit::requiresLocalFileUpload() const
{
    SCMStringBuffer dest;
    Owned<IConstWUResult> result;
    Owned<IConstLocalFileUploadIterator> iter(getLocalFileUploads());
    if(!iter)
        return false;
    for(iter->first(); iter->isValid(); iter->next())
    {
        Owned<IConstLocalFileUpload> upload(iter->get());
        switch(upload->queryType())
        {
        case UploadTypeWUResult:
        case UploadTypeWUResultCsv:
        case UploadTypeWUResultXml:
            upload->getDestination(dest);
            result.setown(getResultByName(dest.str()));
            if(!result)
                return true;
            break;
        default:
            throw MakeStringException(WUERR_InvalidUploadFormat, "Unsupported local file upload type %s", getEnumText(upload->queryType(), localFileUploadTypes));
        }
    }
    return false;
}

unsigned CLocalWorkUnit::addLocalFileUpload(LocalFileUploadType type, char const * source, char const * destination, char const * eventTag)
{
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    CriticalBlock block(crit);
    IPropertyTree * s = p->queryPropTree("LocalFileUploads");
    if(!s)
        s = p->addPropTree("LocalFileUploads", createPTree());
    unsigned id = s->numChildren();
    Owned<CLocalFileUpload> upload = new CLocalFileUpload(id, type, source, destination, eventTag);
    s->addPropTree("LocalFileUpload", upload->getTree());
    return id;
}

#if 0
void testConstWorkflow(IConstWorkflowItem * cwf, bool * okay, bool * dep)
{
    DBGLOG("Test workflow const iface %u", cwf->queryWfid());
    unsigned deps = 0;
    Owned<IWorkflowDependencyIterator> diter;
    switch(cwf->queryWfid())
    {
    case 1:
        assertex(!cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeNormal);
        assertex(cwf->queryState() == WFStateNull);
        diter.setown(cwf->getDependencies());
        for(diter->first(); diter->isValid(); diter->next())
            deps++;
        assertex(deps==0);
        okay[0] = true;
        break;
    case 2:
        assertex(!cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeRecovery);
        assertex(cwf->queryState() == WFStateSkip);
        okay[1] = true;
        break;
    case 3:
        assertex(cwf->queryContingencyFor() == 4);
        okay[2] = true;
        break;
    case 4:
        assertex(cwf->isScheduled());
        assertex(cwf->queryType() == WFTypeNormal);
        assertex(cwf->queryState() == WFStateReqd);
        assertex(cwf->querySuccess() == 0);
        assertex(cwf->queryFailure() == 3);
        assertex(cwf->queryRecovery() == 2);
        assertex(cwf->queryRetriesAllowed() == 10);
        assertex(cwf->queryRetriesRemaining() == 10);
        diter.setown(cwf->getDependencies());
        for(diter->first(); diter->isValid(); diter->next())
        {
            dep[diter->query()-1] = true;
            deps++;
        }
        assertex(deps==2);
        assertex(dep[0]);
        assertex(dep[1]);
        okay[3] = true;
        break;
    case 5:
        assertex(cwf->isScheduled());
        assertex(!cwf->isScheduledNow());
        assertex(cwf->querySchedulePriority() == 75);
        assertex(cwf->queryScheduleCount() == 5);
        assertex(cwf->queryScheduleCountRemaining() == 5);
        okay[4] = true;
        break;
    case 6:
        assertex(cwf->isScheduled());
        assertex(!cwf->isScheduledNow());
        assertex(cwf->querySchedulePriority() == 25);
        assertex(!cwf->hasScheduleCount());
        okay[5] = true;
        break;
    default:
        assertex(!"unknown wfid in test");
    }
}

void testRuntimeWorkflow(IRuntimeWorkflowItem * rwf, bool * okay)
{
    DBGLOG("Test workflow runtime iface %u", rwf->queryWfid());
    switch(rwf->queryWfid())
    {
    case 1:
    case 2:
    case 3:
        okay[rwf->queryWfid()-1] = true;
        break;
    case 4:
        {
            unsigned tries = 0;
            while(rwf->testAndDecRetries())
                tries++;
            assertex(tries == 10);
            assertex(rwf->queryRetriesRemaining() == 0);
            rwf->setState(WFStateFail);
            assertex(rwf->queryState() == WFStateFail);
            rwf->reset();
            assertex(rwf->queryRetriesRemaining() == 10);
            assertex(rwf->queryState() == WFStateReqd);
        }
        okay[3] = true;
        break;
    case 5:
        {
            assertex(rwf->queryScheduleCountRemaining() == 5);
            unsigned count = 0;
            do count++; while(rwf->decAndTestScheduleCountRemaining());
            assertex(count == 5);
            assertex(rwf->queryScheduleCountRemaining() == 0);
            rwf->reset();
            assertex(rwf->queryScheduleCountRemaining() == 5);
        }
        okay[4] = true;
        break;
    case 6:
        {
            assertex(!rwf->hasScheduleCount());
            unsigned count;
            for(count=0; count<20; count++)
                assertex(rwf->decAndTestScheduleCountRemaining());
        }
        okay[5] = true;
        break;
    default:
        assertex(!"unknown wfid in test");
    }
}

void testWorkflow()
{
    DBGLOG("workunit.cpp : testWorkflow");
    CLocalWorkUnit wu("W-WF-TEST", 0, 0, 0);
    Owned<IWorkflowItem> wf;
    wf.setown(wu.addWorkflowItem(1, WFTypeNormal, 0, 0, 0, 0, 0));
    wf.setown(wu.addWorkflowItem(2, WFTypeRecovery, 0, 0, 0, 0, 0));
    wf.setown(wu.addWorkflowItem(3, WFTypeFailure, 0, 0, 0, 0, 4));
    wf.setown(wu.addWorkflowItem(4, WFTypeNormal, 0, 3, 2, 10, 0));
    wf->setScheduledNow();
    wf->addDependency(1);
    wf.setown(wu.addWorkflowItem(5, WFTypeNormal, 0, 0, 0, 0, 0));
    wf->setScheduledOn("test", "foo*");
    wf->setSchedulePriority(75);
    wf->setScheduleCount(5);
    wf.setown(wu.addWorkflowItem(6, WFTypeNormal, 0, 0, 0, 0, 0));
    wf->setScheduledOn("test", "bar*");
    wf->setSchedulePriority(25);

    unsigned const n = 6;
    bool okay[n];
    bool dep[n];
    unsigned i;
    for(i=0; i<n; i++)
        okay[i] = dep[i] = 0;

    Owned<IConstWorkflowItemIterator> citer(wu.getWorkflowItems());
    for(citer->first(); citer->isValid(); citer->next())
        testConstWorkflow(citer->query(), okay, dep);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }

    Owned<IWorkflowItemIterator> miter(wu.updateWorkflowItems());
    for(miter->first(); miter->isValid(); miter->next())
    {
        Owned<IRuntimeWorkflowItem> rwf(miter->get());
        testRuntimeWorkflow(rwf, okay);
    }

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = dep[i] = false;
    }

    Owned<IWorkflowItemArray> array(wu.getWorkflowClone());
    unsigned wfid;
    for(wfid = 1; array->isValid(wfid); wfid++)
        testConstWorkflow(&array->queryWfid(wfid), okay, dep);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }

    for(wfid = 1; array->isValid(wfid); wfid++)
        testRuntimeWorkflow(&array->queryWfid(wfid), okay);

    for(i=0; i<n; i++)
    {
        assertex(okay[i]);
        okay[i] = false;
    }
}
#endif

//------------------------------------------------------------------------------------------

extern WUState waitForWorkUnitToComplete(const char * wuid, int timeout, bool returnOnWaitState)
{
    return factory->waitForWorkUnit(wuid, (unsigned) timeout, false, returnOnWaitState);
}

extern WORKUNIT_API WUState secWaitForWorkUnitToComplete(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout, bool returnOnWaitState)
{
    if (checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Read, "Wait for Complete", false, true))
        return waitForWorkUnitToComplete(wuid, timeout, returnOnWaitState);
    return WUStateUnknown;
}

extern bool waitForWorkUnitToCompile(const char * wuid, int timeout)
{
    switch(factory->waitForWorkUnit(wuid, (unsigned) timeout, true, true))
    {
    case WUStateCompiled:
    case WUStateCompleted:
    case WUStateWait:
    case WUStateUploadingFiles:
        return true;
    default:
        return false;
    }
}

extern WORKUNIT_API bool secWaitForWorkUnitToCompile(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout)
{
    if (checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Read, "Wait for Compile", false, true))
        return waitForWorkUnitToCompile(wuid, timeout);
    return false;
}

extern WORKUNIT_API bool secDebugWorkunit(const char * wuid, ISecManager &secmgr, ISecUser &secuser, const char *command, StringBuffer &response)
{
    if (strnicmp(command, "<debug:", 7) == 0 && checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Read, "Debug", false, true))
    {
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid, &secmgr, &secuser);
        SCMStringBuffer ip;
        unsigned port;
        port = wu->getDebugAgentListenerPort();
        wu->getDebugAgentListenerIP(ip);
        SocketEndpoint debugEP(ip.str(), port);
        Owned<ISocket> socket = ISocket::connect_timeout(debugEP, 1000);
        unsigned len = (size32_t)strlen(command);
        unsigned revlen = len;
        _WINREV(revlen);
        socket->write(&revlen, sizeof(revlen));
        socket->write(command, len);
        for (;;)
        {
            socket->read(&len, sizeof(len));
            _WINREV(len);                    
            if (len == 0)
                break;
            if (len & 0x80000000)
            {
                throwUnexpected();
            }
            char * mem = (char*) response.reserve(len);
            socket->read(mem, len);
        }
        return true;
    }
    return false;
}

void getSimpleResultType(IWUResult *r, Owned<ITypeInfo> &type)
{
    TypeInfoArray types;
    StringAttrArray names;
    r->getSchema(types, names, NULL);
    if (types.ordinality()==1)
        type.set(&types.item(0));
}

bool isSuppliedParamScalar(IWUResult *r, IPropertyTree &curVal, Owned<ITypeInfo> &type)
{
    if (!r->isResultScalar())
        return false;
    if (!curVal.hasChildren())
        return true;
    getSimpleResultType(r, type);
    return type && type->isScalar();
}

void updateSuppliedXmlParams(IWorkUnit * w)
{
    Owned<const IPropertyTree> params = w->getXmlParams();
    if (!params)
        return;
    Owned<IPropertyTreeIterator> elems = params->getElements("*");
    ForEach(*elems)
    {
        IPropertyTree & curVal = elems->query();
        const char *name = curVal.queryName();
        Owned<IWUResult> r = updateWorkUnitResult(w, name, -1);
        if (r)
        {
            Owned<ITypeInfo> type;
            StringBuffer s;
            if (isSuppliedParamScalar(r, curVal, type))
            {
                curVal.getProp(".", s);
                r->setResultXML(s);
                r->setResultStatus(ResultStatusSupplied);
            }
            else
            {
                toXML(&curVal, s);
                if (!type)
                    getSimpleResultType(r, type);
                bool isSet = (type && type->getTypeCode()==type_set);
                r->setResultRaw(s.length(), s.str(), isSet ? ResultFormatXmlSet : ResultFormatXml);
            }
        }
        else
            DBGLOG("WARNING: no matching variable in workunit for input parameter %s", name);
    }
}

IWUResult * updateWorkUnitResult(IWorkUnit * w, const char *name, unsigned sequence)
{
    switch ((int)sequence)
    {
    case ResultSequenceStored:
        return w->updateVariableByName(name);
    case ResultSequencePersist:
        return w->updateGlobalByName(name);
    case ResultSequenceInternal:
    case ResultSequenceOnce:
        return w->updateTemporaryByName(name);
    default:
        return w->updateResultBySequence(sequence);
    }
}

IConstWUResult * getWorkUnitResult(IConstWorkUnit * w, const char *name, unsigned sequence)
{
    switch ((int)sequence)
    {
    case ResultSequenceStored:
        return w->getVariableByName(name);

    case ResultSequencePersist:
        return w->getGlobalByName(name);

    case ResultSequenceInternal:
    case ResultSequenceOnce:
        return w->getTemporaryByName(name);

    default:
        if (name && name[0])
            return w->getResultByName(name);//name takes precedence over sequence
        else
            return w->getResultBySequence(sequence);
    }
}

extern WORKUNIT_API bool getWorkUnitCreateTime(const char *wuid,CDateTime &time)
{
    if (wuid) {
        char prefchar;
        unsigned year,month,day,hour,min,sec;
        if (sscanf(wuid, "%c%4u%2u%2u-%2u%2u%2u", &prefchar, &year, &month, &day, &hour, &min, &sec)==7) {
            time.set(year, month, day, hour, min, sec, 0, true);
//          time.setDate(year, month, day);
//          time.setTime(hour, min, sec, 0, true);  // for some reason time is local
            return true;
        }
    }
    return false;
}

extern WORKUNIT_API IStringVal& createToken(const char *wuid, const char *user, const char *password, IStringVal &str)
{
    StringBuffer wu, token("X");
    wu.append(wuid).append(';').append(user).append(';').append(password);
    encrypt(token,wu.str());
    str.set(token.str());
    return str;
}

// This will be replaced by something more secure!
extern WORKUNIT_API void extractToken(const char *token, const char *wuid, IStringVal &user, IStringVal &password)
{
    if (token && *token)
    {
        StringBuffer wu;
        decrypt(wu, token+1);
        const char *finger = strchr(wu.str(),';');
        if (finger && strnicmp(wuid, wu.str(), finger-wu.str())==0)
        {
            const char *finger1 = strchr(++finger,';');
            if(finger1)
            {
                user.setLen(finger, (size32_t)(finger1-finger));
                finger1++;
                password.setLen(finger1, (size32_t)(wu.str() + wu.length() - finger1));
                return;
            }
        }
        throw MakeStringException(WUERR_InvalidSecurityToken, "Invalid call to extractToken");
    }
}

extern WORKUNIT_API WUState getWorkUnitState(const char* state)
{
    return (WUState) getEnum(state, states);
}

const LogMsgCategory MCschedconn = MCprogress(1000);    // Category used to inform about schedule synchronization

class CWorkflowScheduleConnection : public CInterface, implements IWorkflowScheduleConnection
{
public:
    CWorkflowScheduleConnection(char const * wuid)
    {
        basexpath.append("/WorkflowSchedule/").append(wuid);
        flagxpath.append(basexpath.str()).append("/Active");
    }

    IMPLEMENT_IINTERFACE;

    virtual void lock()
    {
        LOG(MCschedconn, "Locking base schedule connection");
        baseconn.setown(querySDS().connect(basexpath.str(), myProcessSession(), RTM_CREATE_QUERY | RTM_LOCK_WRITE, INFINITE));
        if(!baseconn)
            throw MakeStringException(WUERR_ScheduleLockFailed, "Could not get base workflow schedule lock");
    }

    virtual void unlock()
    {
        LOG(MCschedconn, "Unlocking base schedule connection");
        baseconn.clear();
    }

    virtual void setActive()
    {
        LOG(MCschedconn, "Setting active flag in schedule connection");
        flagconn.setown(querySDS().connect(flagxpath.str(), myProcessSession(), RTM_CREATE | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, INFINITE));
        if(!flagconn)
            throw MakeStringException(WUERR_ScheduleLockFailed, "Could not get active workflow schedule lock");
    }

    virtual void resetActive()
    {
        LOG(MCschedconn, "Resetting active flag in schedule connection");
        flagconn.clear();
    }

    virtual bool queryActive()
    {
        return baseconn->queryRoot()->hasProp("Active");
    }

    virtual bool pull(IWorkflowItemArray * workflow)
    {
        assertex(baseconn);
        Owned<IPropertyTree> root = baseconn->getRoot();
        Owned<IPropertyTree> eventQueue = root->getPropTree("EventQueue");
        if(!eventQueue) return false;
        if(!eventQueue->hasProp("Item")) return false;
        {
            Owned<IPropertyTreeIterator> eventItems = eventQueue->getElements("Item");
            Owned<IPropertyTree> eventItem;
            Owned<IRuntimeWorkflowItemIterator> wfItems = workflow->getSequenceIterator();
            Owned<IRuntimeWorkflowItem> wfItem;
            for(eventItems->first(); eventItems->isValid(); eventItems->next())
            {
                eventItem.setown(&eventItems->get());
                const char * eventName = eventItem->queryProp("@name");
                const char * eventText = eventItem->queryProp("@text");
                for(wfItems->first(); wfItems->isValid(); wfItems->next())
                {
                    wfItem.setown(wfItems->get());
                    if(wfItem->queryState() != WFStateWait)
                        continue;
                    Owned<IWorkflowEvent> targetEvent = wfItem->getScheduleEvent();
                    if(!targetEvent || !targetEvent->matches(eventName, eventText))
                        continue;
                    wfItem->setEvent(eventName, eventText);
                    wfItem->setState(WFStateReqd);
                    resetDependentsState(workflow, *wfItem);
                }
            }
        }
        bool more;
        do
            more = eventQueue->removeProp("Item");
        while(more);
        return true;
    }

    virtual void push(char const * name, char const * text)
    {
        assertex(baseconn);
        Owned<IPropertyTree> root = baseconn->getRoot();
        ensurePTree(root, "EventQueue");
        Owned<IPropertyTree> eventQueue = root->getPropTree("EventQueue");
        Owned<IPropertyTree> eventItem = createPTree();
        eventItem->setProp("@name", name);
        eventItem->setProp("@text", text);
        eventQueue->addPropTree("Item", eventItem.getLink());
    }

    virtual void remove()
    {
        if (baseconn)
        {
            baseconn->close(true);
            baseconn.clear();
        }
    }

private:
    void resetItemStateAndDependents(IWorkflowItemArray * workflow, unsigned wfid) const
    {
        if (wfid)
            resetItemStateAndDependents(workflow, workflow->queryWfid(wfid));
    }

    void resetItemStateAndDependents(IWorkflowItemArray * workflow, IRuntimeWorkflowItem & item) const
    {
        switch(item.queryState())
        {
        case WFStateDone:
        case WFStateFail:
            {
                item.setState(WFStateNull);
                resetItemStateAndDependents(workflow, item.queryPersistWfid());
                resetDependentsState(workflow, item);
                break;
            }
        }
    }

    void resetDependentsState(IWorkflowItemArray * workflow, IRuntimeWorkflowItem & item) const
    {
        Owned<IWorkflowDependencyIterator> iter(item.getDependencies());
        for(iter->first(); iter->isValid(); iter->next())
        {
            IRuntimeWorkflowItem & dep = workflow->queryWfid(iter->query());
            resetItemStateAndDependents(workflow, dep);
        }
    }

private:
    StringBuffer basexpath;
    StringBuffer flagxpath;
    Owned<IRemoteConnection> baseconn;
    Owned<IRemoteConnection> flagconn;
};

extern WORKUNIT_API IWorkflowScheduleConnection * getWorkflowScheduleConnection(char const * wuid)
{
    return new CWorkflowScheduleConnection(wuid);
}

extern WORKUNIT_API IExtendedWUInterface * queryExtendedWU(IConstWorkUnit * wu)
{
    return QUERYINTERFACE(wu, IExtendedWUInterface);
}

extern WORKUNIT_API const IExtendedWUInterface * queryExtendedWU(const IConstWorkUnit * wu)
{
    return QUERYINTERFACE(wu, const IExtendedWUInterface);
}


extern WORKUNIT_API void addExceptionToWorkunit(IWorkUnit * wu, ErrorSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column)
{
    Owned<IWUException> we = wu->createException();
    we->setSeverity(severity);
    we->setExceptionMessage(text);
    if (source)
        we->setExceptionSource(source);
    if (code)
        we->setExceptionCode(code);
    if (filename)
        we->setExceptionFileName(filename);
    if (lineno)
    {
        we->setExceptionLineNo(lineno);
        if (column)
            we->setExceptionColumn(lineno);
    }
}

const char * skipLeadingXml(const char * text)
{
    if (!text)
        return NULL;

    //skip utf8 BOM, probably excessive
    if (memcmp(text, UTF8_BOM, 3) == 0)
        text += 3;

    loop
    {
        if (isspace(*text))
            text++;
        else if (text[0] == '<' && text[1] == '?')
        {
            text += 2;
            loop
            {
                if (!*text) break;
                if (text[0] == '?' && text[1] == '>')
                {
                    text += 2;
                    break;
                }
                text++;
            }
        }
        else if (text[0] == '<' && text[1] == '!' && text[2] == '-' && text[3] == '-')
        {
            text += 4;
            loop
            {
                if (!*text) break;
                if (text[0] == '-' && text[1] == '-' && text[2] == '>')
                {
                    text += 3;
                    break;
                }
                text++;
            }
        }
        else
            break;
    }

    return text;
}

extern WORKUNIT_API bool isArchiveQuery(const char * text)
{
    text = skipLeadingXml(text);
    if (!text)
        return false;
    const char * archivePrefix = "<Archive";
    return memicmp(text, archivePrefix, strlen(archivePrefix)) == 0;
}

extern WORKUNIT_API bool isQueryManifest(const char * text)
{
    text = skipLeadingXml(text);
    if (!text)
        return false;
    const char * manifestPrefix = "<Manifest";
    return memicmp(text, manifestPrefix, strlen(manifestPrefix)) == 0;
}

//------------------------------------------------------------------------------
// Named Alias helper function

static IPropertyTree * resolveQueryByDll(IPropertyTree * queryRegistry, const char * dll)
{
    StringBuffer xpath;
    xpath.append("Query[@dll=\"").append(dll).append("\"]");
    return queryRegistry->getPropTree(xpath);
}


static IPropertyTree * resolveQueryByWuid(IPropertyTree * queryRegistry, const char * wuid)
{
    StringBuffer xpath;
    xpath.append("Query[@wuid=\"").append(wuid).append("\"]");
    return queryRegistry->getPropTree(xpath);
}


static void clearAliases(IPropertyTree * queryRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Alias[@id=\"").append(lcId).append("\"]");

    Owned<IPropertyTreeIterator> iter = queryRegistry->getElements(xpath);
    ForEach(*iter)
    {
        queryRegistry->removeProp(xpath.str());
    }
}

IPropertyTree * addNamedQuery(IPropertyTree * queryRegistry, const char * name, const char * wuid, const char * dll, bool library, const char *userid, const char *snapshot)
{
    StringBuffer lcName(name);
    lcName.toLowerCase();
    StringBuffer xpath;
    xpath.append("Query[@name=\"").append(lcName.str()).append("\"]");

    Owned<IPropertyTreeIterator> iter = queryRegistry->getElements(xpath);
    unsigned seq = 1;
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        const char *thisWuid = item.queryProp("@wuid");
        if (strieq(wuid, thisWuid))
            return &item;
        unsigned thisSeq = item.getPropInt("@seq");
        if (thisSeq >= seq)
            seq = thisSeq + 1;
    }

    StringBuffer id;
    id.append(lcName).append(".").append(seq);
    IPropertyTree * newEntry = createPTree("Query", ipt_caseInsensitive);
    newEntry->setProp("@name", lcName);
    newEntry->setProp("@wuid", wuid);
    newEntry->setProp("@dll", dll);
    newEntry->setProp("@id", id);
    newEntry->setPropInt("@seq", seq);
    if (library)
        newEntry->setPropBool("@isLibrary", true);
    if (userid && *userid)
        newEntry->setProp("@publishedBy", userid);
    if (snapshot && *snapshot)
        newEntry->setProp("@snapshot", snapshot);
    return queryRegistry->addPropTree("Query", newEntry);
}

void removeNamedQuery(IPropertyTree * queryRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    clearAliases(queryRegistry, lcId);
    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(lcId).append("\"]");
    queryRegistry->removeProp(xpath);
}


void removeDllFromNamedQueries(IPropertyTree * queryRegistry, const char * dll)
{
    Owned<IPropertyTree> match = resolveQueryByDll(queryRegistry, dll);
    if (!match)
        return;
    clearAliases(queryRegistry, match->queryProp("@id"));
    queryRegistry->removeTree(match);
}

void removeWuidFromNamedQueries(IPropertyTree * queryRegistry, const char * wuid)
{
    Owned<IPropertyTree> match = resolveQueryByWuid(queryRegistry, wuid);
    if (!match)
        return;
    clearAliases(queryRegistry, match->queryProp("@id"));
    queryRegistry->removeTree(match);
}

void removeAliasesFromNamedQuery(IPropertyTree * queryRegistry, const char * id)
{
    clearAliases(queryRegistry, id);
}

void setQueryAlias(IPropertyTree * queryRegistry, const char * name, const char * value)
{
    StringBuffer lcName(name);
    lcName.toLowerCase();

    StringBuffer xpath;
    xpath.append("Alias[@name=\"").append(lcName).append("\"]");
    IPropertyTree * match = queryRegistry->queryPropTree(xpath);
    if (!match)
    {
        IPropertyTree * newEntry = createPTree("Alias");
        newEntry->setProp("@name", lcName);
        match = queryRegistry->addPropTree("Alias", newEntry);
    }
    match->setProp("@id", value);
}

extern WORKUNIT_API IPropertyTree * getQueryById(IPropertyTree * queryRegistry, const char *queryid)
{
    if (!queryRegistry || !queryid)
        return NULL;
    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(queryid).append("\"]");
    return queryRegistry->getPropTree(xpath);
}

extern WORKUNIT_API IPropertyTree * getQueryById(const char *queryset, const char *queryid, bool readonly)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(queryset, readonly);
    return getQueryById(queryRegistry, queryid);
}

extern WORKUNIT_API IPropertyTree * resolveQueryAlias(IPropertyTree * queryRegistry, const char * alias)
{
    if (!queryRegistry || !alias)
        return NULL;

    StringBuffer xpath;
    unsigned cnt = 0;
    StringBuffer lc(alias);
    const char * search = lc.toLowerCase().str();
    loop
    {
        xpath.set("Alias[@name='").append(search).append("']/@id");
        const char * queryId = queryRegistry->queryProp(xpath);
        if (!queryId)
            break;
        //Check for too many alias indirections.
        if (cnt++ > 10)
            return NULL;
        search = lc.set(queryId).toLowerCase().str();
    }

    return getQueryById(queryRegistry, search);
}

extern WORKUNIT_API IPropertyTree * resolveQueryAlias(const char *queryset, const char *alias, bool readonly)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(queryset, readonly);
    return resolveQueryAlias(queryRegistry, alias);
}

void setQuerySuspendedState(IPropertyTree * queryRegistry, const char *id, bool suspend, const char *userid)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Query[@id=\"").append(lcId).append("\"]");
    IPropertyTree *tree = queryRegistry->queryPropTree(xpath);
    if (tree)
    {
        if (tree->getPropBool("@suspended", false) == suspend)
            return;
        if (suspend)
        {
            tree->addPropBool("@suspended", true);
            if (userid && *userid)
                tree->addProp("@suspendedBy", userid);
        }
        else
        {
            tree->removeProp("@suspended");
            tree->removeProp("@suspendedBy");
        }
    }
    else
        throw MakeStringException((suspend)? QUERRREG_SUSPEND : QUERRREG_UNSUSPEND, "Modifying query suspended state failed.  Could not find query %s", id);
}

void setQueryCommentForNamedQuery(IPropertyTree * queryRegistry, const char *id, const char *queryComment)
{
    if (queryComment)
    {
        StringBuffer lcId(id);
        lcId.toLowerCase();

        StringBuffer xpath;
        xpath.append("Query[@id=\"").append(lcId).append("\"]");
        IPropertyTree *tree = queryRegistry->queryPropTree(xpath);
        if (tree)
            tree->setProp("@queryComment", queryComment);
        else
            throw MakeStringException(QUERRREG_COMMENT,  "Could not find query %s", id);
    }
}

extern WORKUNIT_API IPropertyTree * getQueryRegistryRoot()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/QuerySets", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (conn)
        return conn->getRoot();
    else
        return NULL;
}

extern WORKUNIT_API void checkAddLibrariesToQueryEntry(IPropertyTree *queryTree, IConstWULibraryIterator *libraries)
{
    if (!queryTree || !libraries)
        return;
    if (queryTree->hasProp("@libCount")) //already added
        return;
    unsigned libCount=0;
    ForEach(*libraries)
    {
        IConstWULibrary &library = libraries->query();
        SCMStringBuffer libname;
        if (!library.getName(libname).length())
            continue;
        queryTree->addProp("Library", libname.str());
        libCount++;
    }
    queryTree->setPropInt("@libCount", libCount);
}

extern WORKUNIT_API void checkAddLibrariesToQueryEntry(IPropertyTree *queryTree, IConstWorkUnit *cw)
{
    Owned<IConstWULibraryIterator> libraries = &cw->getLibraries();
    checkAddLibrariesToQueryEntry(queryTree, libraries);
}

extern WORKUNIT_API IPropertyTree * getQueryRegistry(const char * wsEclId, bool readonly)
{
    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/QuerySets/QuerySet[@id=\"").append(wsEclId).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (conn)
        return conn->getRoot();
    if (readonly)
        return NULL;

    //Lock the QuerySets in case another thread/client wants to check/add the same QuerySet.
    Owned<IRemoteConnection> globalLock = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Re-check if the QuerySet has been added between checking the 1st time and gaining the globalLock.
    conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
    if (conn)
        return conn->getRoot();

    conn.setown(querySDS().connect("/QuerySets/QuerySet", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, SDS_LOCK_TIMEOUT));
    if (!conn)
        throwUnexpected();
    IPropertyTree * root = conn->queryRoot();
    root->setProp("@id",wsEclId);
    conn->commit();
    return LINK(root);
}

IPropertyTree * addNamedPackageSet(IPropertyTree * packageRegistry, const char * name, IPropertyTree *packageInfo, bool overWrite)
{
    StringBuffer xpath;
    StringBuffer lcName(name);
    lcName.toLowerCase();
    // see if "name" already exists
    xpath.append("Package[@id='").append(name).append("']");
    IPropertyTree *pkgTree = packageRegistry->queryPropTree(xpath.str());
    if (pkgTree)
    {
        if (overWrite)
            packageRegistry->removeTree(pkgTree);
        else
            throw MakeStringException(WUERR_PackageAlreadyExists, "Package name %s already exists, either delete it or specify overwrite",name);
    }
    
    IPropertyTree *tree = packageRegistry->addPropTree("Package", packageInfo);
    tree->setProp("@id", lcName);
    return tree;
}

void removeNamedPackage(IPropertyTree * packageRegistry, const char * id)
{
    StringBuffer lcId(id);
    lcId.toLowerCase();

    StringBuffer xpath;
    xpath.append("Package[@id=\"").append(lcId).append("\"]");
    packageRegistry->removeProp(xpath);
}

extern WORKUNIT_API IPropertyTree * getPackageSetRegistry(const char * wsEclId, bool readonly)
{
    //Only lock the branch for the target we're interested in.
    StringBuffer xpath;
    xpath.append("/PackageSets/PackageSet[@id=\"").append(wsEclId).append("\"]");
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (conn)
        return conn->getRoot();
    if (readonly)
        return NULL;

    //Lock the PackageSets in case another thread/client wants to check/add the same PackageSet.
    Owned<IRemoteConnection> globalLock = querySDS().connect("/PackageSets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);

    //Re-check if the PackageSet has been added between checking the 1st time and gaining the globalLock.
    conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
    if (conn)
        return conn->getRoot();

    conn.setown(querySDS().connect("/PackageSets/PackageSet", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, SDS_LOCK_TIMEOUT));
    if (!conn)
        throwUnexpected();
    IPropertyTree* root = conn->queryRoot();
    root->setProp("@id",wsEclId);
    conn->commit();
    return LINK(root);
}

void addQueryToQuerySet(IWorkUnit *workunit, IPropertyTree *queryRegistry, const char *queryName, WUQueryActivationOptions activateOption, StringBuffer &newQueryId, const char *userid)
{
    StringBuffer cleanQueryName;
    appendUtf8XmlName(cleanQueryName, strlen(queryName), queryName);

    SCMStringBuffer dllName;
    Owned<IConstWUQuery> q = workunit->getQuery();
    q->getQueryDllName(dllName);
    if (!dllName.length())
        throw MakeStringException(WUERR_InvalidDll, "Cannot deploy query - no associated dll.");

    StringBuffer currentTargetClusterType;
    queryRegistry->getProp("@targetclustertype", currentTargetClusterType); 

    SCMStringBuffer targetClusterType;
    workunit->getDebugValue("targetclustertype", targetClusterType);

    SCMStringBuffer snapshot;
    workunit->getSnapshot(snapshot);

    if (currentTargetClusterType.length() < 1) 
    {
        queryRegistry->setProp("@targetclustertype", targetClusterType.str());
    }
    else
    {
        if (strcmp(currentTargetClusterType.str(), "roxie") == 0 && strcmp(currentTargetClusterType.str(), targetClusterType.str())!=0)
        {
            throw MakeStringException(WUERR_MismatchClusterType, "TargetClusterTypes of workunit and queryset do not match.");
        }
    }

    IPropertyTree *newEntry = addNamedQuery(queryRegistry, cleanQueryName, workunit->queryWuid(), dllName.str(), isLibrary(workunit), userid, snapshot.str());
    Owned<IConstWULibraryIterator> libraries = &workunit->getLibraries();
    checkAddLibrariesToQueryEntry(newEntry, libraries);
    newQueryId.append(newEntry->queryProp("@id"));
    workunit->setIsQueryService(true); //will check querysets before delete
    workunit->commit();

    activateQuery(queryRegistry, activateOption, queryName, newQueryId, userid);
}

void activateQuery(IPropertyTree *queryRegistry, WUQueryActivationOptions activateOption, const char *queryName, const char *queryId, const char *userid)
{
    StringBuffer cleanQueryName;
    appendUtf8XmlName(cleanQueryName, strlen(queryName), queryName);

    if (activateOption == ACTIVATE_SUSPEND_PREVIOUS|| activateOption == ACTIVATE_DELETE_PREVIOUS)
    {
        Owned<IPropertyTree> prevQuery = resolveQueryAlias(queryRegistry, cleanQueryName);
        setQueryAlias(queryRegistry, cleanQueryName, queryId);
        if (prevQuery && !streq(queryId, prevQuery->queryProp("@id")))
        {
            if (activateOption == ACTIVATE_SUSPEND_PREVIOUS)
                setQuerySuspendedState(queryRegistry, prevQuery->queryProp("@id"), true, userid);
            else 
                removeNamedQuery(queryRegistry, prevQuery->queryProp("@id"));
        }
    }
    else if (activateOption == MAKE_ACTIVATE || activateOption == MAKE_ACTIVATE_LOAD_DATA_ONLY)
        setQueryAlias(queryRegistry, cleanQueryName, queryId);
}

void addQueryToQuerySet(IWorkUnit *workunit, const char *querySetName, const char *queryName, WUQueryActivationOptions activateOption, StringBuffer &newQueryId, const char *userid)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, false);
    addQueryToQuerySet(workunit, queryRegistry, queryName, activateOption, newQueryId, userid);
}

bool removeQuerySetAlias(const char *querySetName, const char *alias)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    StringBuffer xpath;
    xpath.appendf("Alias[@name='%s']", alias);
    IPropertyTree *t = queryRegistry->queryPropTree(xpath);
    return queryRegistry->removeTree(t);
}

void addQuerySetAlias(const char *querySetName, const char *alias, const char *id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, false);
    setQueryAlias(queryRegistry, alias, id);
}

void setSuspendQuerySetQuery(const char *querySetName, const char *id, bool suspend, const char *userid)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    setQuerySuspendedState(queryRegistry, id, suspend, userid);
}

void deleteQuerySetQuery(const char *querySetName, const char *id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    removeNamedQuery(queryRegistry, id);
}

void removeQuerySetAliasesFromNamedQuery(const char *querySetName, const char * id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    clearAliases(queryRegistry, id);
}

void setQueryCommentForNamedQuery(const char *querySetName, const char *id, const char *queryComment)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    setQueryCommentForNamedQuery(queryRegistry, id, queryComment);
}

const char *queryIdFromQuerySetWuid(IPropertyTree *queryRegistry, const char *wuid, const char *queryName, IStringVal &id)
{
    if (!queryRegistry)
        return NULL;
    StringBuffer xpath;
    xpath.appendf("Query[@wuid='%s']", wuid);
    if (queryName && *queryName)
        xpath.appendf("[@name='%s']", queryName);
    IPropertyTree *q = queryRegistry->queryPropTree(xpath.str());
    if (q)
    {
        id.set(q->queryProp("@id"));
    }
    return id.str();
}

const char *queryIdFromQuerySetWuid(const char *querySetName, const char *wuid, const char *queryName, IStringVal &id)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySetName, true);
    return queryIdFromQuerySetWuid(queryRegistry, wuid, queryName, id);
}

extern WORKUNIT_API void gatherLibraryNames(StringArray &names, StringArray &unresolved, IWorkUnitFactory &workunitFactory, IConstWorkUnit &cw, IPropertyTree *queryset)
{
    Owned<IConstWULibraryIterator> wulibraries = &cw.getLibraries();
    ForEach(*wulibraries)
    {
        SCMStringBuffer libname;
        IConstWULibrary &wulibrary = wulibraries->query();
        wulibrary.getName(libname);
        if (names.contains(libname.str()) || unresolved.contains(libname.str()))
            continue;

        Owned<IPropertyTree> query = resolveQueryAlias(queryset, libname.str());
        if (query && query->getPropBool("@isLibrary"))
        {
            const char *wuid = query->queryProp("@wuid");
            Owned<IConstWorkUnit> libcw = workunitFactory.openWorkUnit(wuid);
            if (libcw)
            {
                names.appendUniq(libname.str());
                gatherLibraryNames(names, unresolved, workunitFactory, *libcw, queryset);
                continue;
            }
        }

        unresolved.appendUniq(libname.str());
    }
}

bool looksLikeAWuid(const char * wuid, const char firstChar)
{
    if (!wuid)
        return false;
    if (wuid[0] != firstChar)
        return false;
    if (!isdigit(wuid[1]) || !isdigit(wuid[2]) || !isdigit(wuid[3]) || !isdigit(wuid[4]))
        return false;
    if (!isdigit(wuid[5]) || !isdigit(wuid[6]) || !isdigit(wuid[7]) || !isdigit(wuid[8]))
        return false;
    return (wuid[9]=='-');
}

IPropertyTree * resolveDefinitionInArchive(IPropertyTree * archive, const char * path)
{
    IPropertyTree * module = archive;
    const char * dot = strrchr(path, '.');

    StringBuffer xpath;
    if (dot)
    {
        xpath.clear().append("Module[@key='").appendLower(dot-path, path).append("']");
        module = archive->queryPropTree(xpath);

        path = dot+1;
    }
    else
        module = archive->queryPropTree("Module[@key='']");

    if (!module)
        return NULL;

    xpath.clear().append("Attribute[@key='").appendLower(strlen(path), path).append("']");
    return module->queryPropTree(xpath);
}

extern WORKUNIT_API void associateLocalFile(IWUQuery * query, WUFileType type, const char * name, const char * description, unsigned crc)
{
    StringBuffer hostname;
    queryHostIP().getIpText(hostname);

    StringBuffer fullPathname;
    makeAbsolutePath(name, fullPathname);
    query->addAssociatedFile(type, fullPathname, hostname, description, crc);
}

extern WORKUNIT_API void descheduleWorkunit(char const * wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
    if(workunit)
        workunit->deschedule();
    else
        doDescheduleWorkkunit(wuid);
}

extern WORKUNIT_API void updateWorkunitTimeStat(IWorkUnit * wu, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * description, unsigned __int64 value)
{
    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, description, value, 1, 0, StatsMergeReplace);
}

extern WORKUNIT_API void updateWorkunitTimings(IWorkUnit * wu, ITimeReporter *timer)
{
    StringBuffer scope;
    for (unsigned i = 0; i < timer->numSections(); i++)
    {
        StatisticScopeType scopeType= timer->getScopeType(i);
        timer->getScope(i, scope.clear());
        StatisticKind kind = timer->getTimerType(i);
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, NULL, timer->getTime(i), timer->getCount(i), timer->getMaxTime(i), StatsMergeReplace);
    }
}

extern WORKUNIT_API void getWorkunitTotalTime(IConstWorkUnit* workunit, const char* creator, unsigned __int64 & totalTimeNs, unsigned __int64 & totalThisTimeNs)
{
    StatisticsFilter summaryTimeFilter(SCTsummary, creator, SSTglobal, GLOBAL_SCOPE, SMeasureTimeNs, StTimeElapsed);
    Owned<IConstWUStatistic> totalThorTime = getStatistic(workunit, summaryTimeFilter);
    Owned<IConstWUStatistic> totalThisThorTime = workunit->getStatistic(queryStatisticsComponentName(), GLOBAL_SCOPE, StTimeElapsed);
    if (totalThorTime)
        totalTimeNs = totalThorTime->getValue();
    else
        totalTimeNs = 0;
    if (totalThisThorTime)
        totalThisTimeNs = totalThisThorTime->getValue();
    else
        totalThisTimeNs = 0;
}

extern WORKUNIT_API void addTimeStamp(IWorkUnit * wu, StatisticScopeType scopeType, const char * scope, StatisticKind kind)
{
    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);
}


IConstWUStatistic * getStatistic(IConstWorkUnit * wu, const IStatisticsFilter & filter)
{
    Owned<IConstWUStatisticIterator> iter = &wu->getStatistics(&filter);
    if (iter->first())
        return &OLINK(iter->query());
    return NULL;
}


class GlobalStatisticGatherer : public CInterfaceOf<IStatisticGatherer>
{
public:
    GlobalStatisticGatherer(IWorkUnit * _wu) : wu(_wu) {}

    virtual void beginScope(const StatsScopeId & id)
    {
        prevLenStack.append(scope.length());
        if (scope.length())
            scope.append(":");
        id.getScopeText(scope);
        scopeTypeStack.append(id.queryScopeType());
    }
    virtual void beginSubGraphScope(unsigned id)
    {
        StatsScopeId scopeId(SSTsubgraph, id);
        beginScope(scopeId);
    }
    virtual void beginActivityScope(unsigned id)
    {
        StatsScopeId scopeId(SSTactivity, id);
        beginScope(scopeId);
    }
    virtual void beginEdgeScope(unsigned id, unsigned oid)
    {
        StatsScopeId scopeId(SSTedge, id, oid);
        beginScope(scopeId);
    }
    virtual void endScope()
    {
        scope.setLength(prevLenStack.popGet());
        scopeTypeStack.pop();
    }
    virtual void addStatistic(StatisticKind kind, unsigned __int64 value)
    {
        StatisticScopeType scopeType = scopeTypeStack.ordinality() ? (StatisticScopeType)scopeTypeStack.tos() : SSTglobal;
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, NULL, value, 1, 0, StatsMergeAppend);
    }
    virtual void updateStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction)
    {
        StatisticScopeType scopeType = scopeTypeStack.ordinality() ? (StatisticScopeType)scopeTypeStack.tos() : SSTglobal;
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, NULL, value, 1, 0, mergeAction);
    }
    virtual IStatisticCollection * getResult()
    {
        return NULL;
    }

protected:
    Linked<IWorkUnit> wu;
    StringBuffer scope;
    UnsignedArray prevLenStack;
    UnsignedArray scopeTypeStack;
};

IStatisticGatherer * createGlobalStatisticGatherer(IWorkUnit * wu)
{
    return new GlobalStatisticGatherer(wu);
}
