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
#ifndef _CONTAINERIZED
#include "environment.hpp"
#endif
#include "daqueue.hpp"
#include "workunit.ipp"
#include "digisign.hpp"

#include <list>
#include <string>
#include <algorithm>

using namespace cryptohelper;

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
    if (!secmgr || !secuser)
        return true;
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    bool ret=secmgr->authorizeEx(RT_WORKUNIT_SCOPE, *secuser, cw->queryWuScope())>=required;
    if (!ret && (log || excpt))
    {
        wuAccessError(secuser->getName(), action, cw->queryWuScope(), cw->queryWuid(), excpt, log);
    }
    return ret;
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

CWuGraphStats::CWuGraphStats(IPropertyTree *_progress, StatisticCreatorType _creatorType, const char * _creator, unsigned wfid, const char * _rootScope, unsigned _id)
    : progress(_progress), creatorType(_creatorType), creator(_creator), id(_id)
{
    StatsScopeId graphScopeId;
    verifyex(graphScopeId.setScopeText(_rootScope));

    StatsScopeId rootScopeId(SSTworkflow,wfid);
    collector.setown(createStatisticsGatherer(_creatorType, _creator, rootScopeId));
    collector->beginScope(graphScopeId);
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

    //Replace the particular subgraph statistics added by this creator
    StringBuffer qualified(tag);
    qualified.append("[@creator='").append(creator).append("']");
    progress->removeProp(qualified);
    IPropertyTree * subgraph = progress->addPropTree(tag);
    subgraph->setProp("@c", queryCreatorTypeName(creatorType));
    subgraph->setProp("@creator", creator);
    subgraph->setPropInt("@minActivity", minActivity);
    subgraph->setPropInt("@maxActivity", maxActivity);
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
            target->setProp(queryTreeTag(kind), formattedValue);
        }
    }
    void expandProcessTreeFromStats(IPropertyTree * rootTarget, IPropertyTree * target, IStatisticCollection * collection)
    {
        expandStats(target, *collection);

        StringBuffer scopeName;
        Owned<IStatisticCollectionIterator> activityIter = &collection->getScopes(NULL, false);
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
            case SSTchildgraph:
            case SSTworkflow:
            case SSTgraph:
                // SSTworkflow and SSTgraph may be safely ignored.  They are not required to produce the statistics.
                continue;
            case SSTfunction:
                //MORE:Should function scopes be included in the graph scope somehow, and if so how?
                continue;
            default:
                throwUnexpected();
            }

            IPropertyTree * next = curTarget->addPropTree(tag);
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

/*
 * Create a user friendly description a scope/stats combination.  Only currently used for elapsed time for root subgraphs
 */
static void createDefaultDescription(StringBuffer & description, StatisticKind kind, StatisticScopeType scopeType, const char * scope)
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

            formatGraphTimerLabel(description, graphname, 0, subId);
            return;
        }
    }
    describeScope(description, scope);
}

/* Represents a single statistic */
class ExtractedStatistic : public CInterfaceOf<IConstWUStatistic>
{
public:
    virtual IStringVal & getDescription(IStringVal & str, bool createDefault) const
    {
        if (!description && createDefault)
        {
            StringBuffer desc;
            createDefaultDescription(desc, kind, scopeType, scope);
            str.set(desc);
            return str;
        }

        str.set(description);
        return str;
    }
    virtual IStringVal & getCreator(IStringVal & str) const
    {
        str.set(creator);
        return str;
    }
    virtual const char * queryScope() const
    {
        return scope ? scope : "";
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

public:
    StringBuffer creator;
    StringBuffer description;
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

//---------------------------------------------------------------------------------------------------------------------

/*
 * The following compare functions are used to ensure that comparison are consistent with
 * compareScopeName() in jstats.  This ensures that the scope iterators from different sources
 * are processed in a consistent order
 */

static int compareGraphNode(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    unsigned lwfid = l->getPropInt("@wfid");
    unsigned rwfid = r->getPropInt("@wfid");
    if (lwfid != rwfid)
        return lwfid > rwfid ? +1 : -1;

    const char * lname = l->queryName();
    const char * rname = r->queryName();
    return compareScopeName(lname, rname);
}

static int compareSubGraphStatsNode(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return compareScopeName(l->queryName(), r->queryName());
}

static int compareSubGraphNode(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return l->getPropInt("@id") - r->getPropInt("@id");
}

static int compareActivityNode(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    return l->getPropInt("@id") - r->getPropInt("@id");
}

static int compareEdgeNode(IInterface * const *ll, IInterface * const *rr)
{
    IPropertyTree *l = (IPropertyTree *) *ll;
    IPropertyTree *r = (IPropertyTree *) *rr;
    //MORE: Edge needs more work
    const char * leftId = l->queryProp("@id");
    const char * rightId = r->queryProp("@id");
    unsigned leftAc = atoi(leftId);
    unsigned rightAc = atoi(rightId);
    if (leftAc != rightAc)
        return (int)(leftAc - rightAc);
    const char * leftSep = strchr(leftId, '_');
    const char * rightSep = strchr(rightId, '_');
    assertex(leftSep && rightSep);
    return atoi(leftSep+1) - atoi(rightSep+1);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * A class for implementing a scope iterator that walks through graph progress information
 */
class CConstGraphProgressScopeIterator : public CInterfaceOf<IConstWUScopeIterator>
{
public:
    CConstGraphProgressScopeIterator(const char * wuid, const ScopeFilter & _filter, __uint64 _minVersion) : filter(_filter), minVersion(_minVersion)
    {
        //Examine the filter, and determine if we only need to look at a single graph/subgraph
        StringAttr singleGraph;
        const StringArray & scopesToMatch = filter.queryScopes();
        if (scopesToMatch)
        {
            bool seenGraph = false;
            bool seenSubGraph = false;
            ForEachItemIn(iScope, scopesToMatch)
            {
                StringArray ids;
                ids.appendList(scopesToMatch.item(iScope), ":");

                ForEachItemIn(i, ids)
                {
                    const char * curId = ids.item(i);
                    StatsScopeId id(curId);
                    switch (id.queryScopeType())
                    {
                    case SSTgraph:
                        if (seenGraph)
                        {
                            if (singleGraph && !streq(singleGraph, curId))
                                singleGraph.clear();
                        }
                        else
                        {
                            if (!id.isWildcard())
                                singleGraph.set(curId);
                            seenGraph = true;
                        }
                        break;
                    case SSTsubgraph:
                        if (seenSubGraph)
                        {
                            if (singleSubGraph && !streq(singleSubGraph, curId))
                                singleSubGraph.clear();
                        }
                        else
                        {
                            if (!id.isWildcard())
                                singleSubGraph.set(curId);
                            seenSubGraph = true;
                        }
                        break;
                    }
                }
            }
        }

        rootPath.append("/GraphProgress/").append(wuid).append('/');
        if (singleGraph)
            rootPath.append(singleGraph).append("/");

        //Don't lock the statistics while we iterate - any partial updates must not cause problems
        if (daliClientActive())
            conn.setown(querySDS().connect(rootPath.str(), myProcessSession(), RTM_NONE, SDS_LOCK_TIMEOUT));

        if (conn && !singleGraph)
        {
            graphIter.setown(conn->queryRoot()->getElements("*"));
            graphIter.setown(createSortedIterator(*graphIter, compareGraphNode));
        }

        valid = false;
    }

    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IConstWUScopeIterator>)

    virtual bool first()
    {
        valid = false;
        if (!conn)
            return false;

        if (graphIter && !graphIter->first())
            return false;

        if (!firstSubGraph())
        {
            if (!nextGraph())
                return false;
        }

        valid = true;
        return true;
    }

    virtual bool next()
    {
        if (!nextChildScope())
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

    virtual bool nextSibling() override
    {
        if (collections.ordinality() == 0)
            return false;

        assertex(childIterators.ordinality() < collections.ordinality());
        collections.pop();
        //next will call childIterator.next() - walking the next sibling
        return next();
    }

    virtual bool nextParent() override
    {
        if (collections.ordinality() == 0)
            return false;

        assertex(childIterators.ordinality() < collections.ordinality());
        collections.pop();

        if (collections.ordinality() == 0)
            return false;

        //Finish with this node - so next will move onto the sibling of the parent node.
        finishCollection();

        return next();
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
        if (singleSubGraph)
        {
            childXpath.append(singleSubGraph);
            xpath = childXpath.str();
        }

        subgraphIter.setown(graphNode.getElements(xpath));
        if (subgraphIter)
        {
            if (!singleSubGraph)
                subgraphIter.setown(createSortedIterator(*subgraphIter, compareSubGraphStatsNode));
        }
        else
            subgraphIter.setown(graphNode.getElements("sg0"));

        if (!subgraphIter->first())
            return false;
        if (firstStat())
            return true;
        return nextSubGraph();
    }

    bool nextSubGraph()
    {
        for (;;)
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
        for (;;)
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
        statsIterator.timeStamp = collection->queryWhenCreated();
        if (!beginCollection(*collection))
            return false;

        // When workflow is root element, it is just a container.  Ignore the workflow element here
        // as WorkUnitStatisticsScopeIterator will produce workflow scope - don't want duplicates.
        // (Note: workflow element never contains stats).
        if (collections.tos().queryScopeType() == SSTworkflow)
        {
            if (!next())
                return false;
        }

        //The root element of a collection is a graph - but it is only there to nest the subgraphs in.
        //Do not iterate it as a separate element - unless it has some stats.
        IStatisticCollection & curCollection = collections.tos();
        if ((curCollection.queryScopeType() != SSTgraph) || (curCollection.getNumStatistics() != 0))
            return true;
        return next();
    }

    bool beginCollection(IStatisticCollection & collection)
    {
        collections.append(OLINK(collection));
        curScopeType = collection.queryScopeType();
        collection.getFullScope(curScopeName.clear());

        ScopeCompare result = filter.compare(curScopeName);
        if (result & SCequal)
            return true;

        //If this scope cannot be the parent of a match then discard it.
        if (!(result & SCparent))
            collections.pop();

        //walk the next element
        return nextChildScope();
    }

    bool nextChildScope()
    {
        for (;;)
        {
            if (collections.ordinality() == 0)
                return false;

            IStatisticCollection * curCollection = &collections.tos();
            if (childIterators.ordinality() < collections.ordinality())
            {
                ScopeCompare result = filter.compare(curScopeName);

                //Do not walk children scopes if it is unrelated
                if (result & SCparent)
                {
                    //Start iterating the children for the current collection
                    childIterators.append(curCollection->getScopes(NULL, true));
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
        IPropertyTree & curSubGraph = subgraphIter->query();

        StatisticCreatorType creatorType = queryCreatorType(curSubGraph.queryProp("@c"), SCTnone);
        const char * creator = curSubGraph.queryProp("@creator");

        //MORE: Check minVersion and allow early filtering

        //MORE: Potentially filter by creator type??
//        if (!filter->matches(creatorType, creator, SSTall, NULL, SMeasureAll, StKindAll, AnyStatisticValue))
//            return false;

        statsIterator.creatorType = creatorType;
        statsIterator.creator.set(creator);
        return true;
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties) override
    {
        if ((whichProperties & PTstatistics))
        {
            /*
            MORE: Code from the statsIterator class should be inlined here - code will be much simpler
            but it currently needs an implementation of the IConstWUStatistic 3rd parameter
            IStatisticCollection & collection = collections.tos();
            ForEachItemIn(i, collection)
            {
                StatisticKind kind;
                unsigned __int64 value;
                collection->getStatistic(kind, value, i);
                visitor.noteStatistic(kind, value, *this);
            }
            */
            statsIterator.reset(curScopeName, curScopeType, collections.tos());
            ForEach(statsIterator)
                statsIterator.play(visitor);
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        return collections.tos().getStatistic(kind, value);
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        return nullptr;
    }

    virtual const char * queryHint(const char * kind) const override
    {
        return nullptr;
    }

    virtual const char * queryScope() const override
    {
        return curScopeName;
    }
    virtual StatisticScopeType getScopeType() const override
    {
        return curScopeType;
    }

private:
    class ScopeStatisticsIterator : public CInterfaceOf<IConstWUStatistic>
    {
        friend class CConstGraphProgressScopeIterator; // cleaner if this was removed + setScope() functions added.
    public:
        void reset(const char * _scopeName, StatisticScopeType _scopeType, IStatisticCollection & _collection)
        {
            scope.set(_scopeName);
            scopeType = _scopeType;
            collection = &_collection;
            numStats = collection->getNumStatistics();
        }

// interface IConstWUStatisticIterator
        IConstWUStatistic & query()
        {
            return *this;
        }
        bool first()
        {
            curStatIndex = 0;
            if (curStatIndex >= numStats)
                return false;

            collection->getStatistic(kind, value, curStatIndex);
            //MORE: Allow filtering:
/*
 *             if (filter && !filter->matches(SCTall, NULL, SSTall, NULL, queryMeasure(kind), kind, value))
                    continue;
 */
            return true;
        }
        bool next()
        {
            for (;;)
            {
                ++curStatIndex;
                if (curStatIndex >= numStats)
                    return false;
                collection->getStatistic(kind, value, curStatIndex);
                //MORE: Allow stats filtering
                return true;
            }
        }
        bool isValid()
        {
            return curStatIndex < numStats;
        }

//interface IConstWUStatistic
        virtual IStringVal & getDescription(IStringVal & str, bool createDefault) const override
        {
            if (createDefault)
            {
                StringBuffer description;
                createDefaultDescription(description, kind, scopeType, scope);
                str.set(description);
            }
            return str;
        }
        virtual IStringVal & getCreator(IStringVal & str) const override
        {
            str.set(creator);
            return str;
        }
        virtual const char * queryScope() const override
        {
            return scope;
        }
        virtual IStringVal & getFormattedValue(IStringVal & str) const override
        {
            StringBuffer formatted;
            formatStatistic(formatted, value, kind);
            str.set(formatted);
            return str;
        }
        virtual StatisticMeasure getMeasure() const override
        {
            return queryMeasure(kind);
        }
        virtual StatisticKind getKind() const override
        {
            return kind;
        }
        virtual StatisticCreatorType getCreatorType() const override
        {
            return creatorType;
        }
        virtual StatisticScopeType getScopeType() const override
        {
            return scopeType;
        }
        virtual unsigned __int64 getValue() const override
        {
            return value;
        }
        virtual unsigned __int64 getCount() const override
        {
            return 1;
        }
        virtual unsigned __int64 getMax() const override
        {
            return 0;
        }
        virtual unsigned __int64 getTimestamp() const override
        {
            return timeStamp;
        }

        void play(IWuScopeVisitor & visitor)
        {
            visitor.noteStatistic(kind, value, *this);
        }

    protected:
        IStatisticCollection * collection = nullptr;
        StringBuffer creator;
        StringBuffer scope;
        StatisticKind kind;
        StatisticCreatorType creatorType;
        StatisticScopeType scopeType;
        unsigned __int64 value = 0;
        unsigned __int64 timeStamp;
        unsigned curStatIndex = 0;
        unsigned numStats = 0;
    } statsIterator;

    Owned<IRemoteConnection> conn;
    StringBuffer curScopeName;
    StatisticScopeType curScopeType = SSTnone;
    __uint64 minVersion;
    const ScopeFilter & filter;
    StringBuffer rootPath;
    StringAttr singleSubGraph;
    Owned<IPropertyTreeIterator> graphIter;
    Owned<IPropertyTreeIterator> subgraphIter;
    IArrayOf<IStatisticCollection> collections;
    IArrayOf<IStatisticCollectionIterator> childIterators; // Iterator(n) through collections(n) - created once iterating children
    MemoryBuffer compressed;
    MemoryBuffer serialized;
    bool valid;
};


int compareNoteScopeOrder(IConstWUException & left, IConstWUException & right)
{
    return compareScopeName(left.queryScope(), right.queryScope());
}

int compareNoteScopeOrder(IInterface * const * left, IInterface * const * right)
{
    return compareNoteScopeOrder(*static_cast<IConstWUException *>(*left), *static_cast<IConstWUException *>(*right));
}

class NotesIterator : public CInterfaceOf<IConstWUScopeIterator>
{
public:
    NotesIterator(const IConstWorkUnit * wu, const ScopeFilter & _filter)
    {
        Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
        ForEach(*exceptions)
        {
            IConstWUException & exception = exceptions->query();
            if (exception.queryScope()!=nullptr)
                notes.append(OLINK(exception));
        }
        notes.sort(compareNoteScopeOrder);
    }

    virtual bool first() override
    {
        baseIndex = 0;
        return updateCurrent();
    }

    virtual bool next() override
    {
        baseIndex += numCurrentScope;
        return updateCurrent();
    }

    virtual bool isValid() override
    {
        return notes.isItem(baseIndex);
    }

    virtual bool nextSibling() override
    {
        //Search until the current scope is not a child of the previous scope
        StringBuffer savedScope(queryScope());
        for (;;)
        {
            if (!next())
                return false;

            if (compareScopes(queryScope(), savedScope) != SCchild)
                return true;
        }
    }

    virtual bool nextParent() override
    {
        //Search until the current scope is not a child of the previous parent scope
        StringBuffer parentScope;
        if (getParentScope(parentScope, queryScope()))
        {
            for (;;)
            {
                if (!next())
                    return false;

                if (compareScopes(queryScope(), parentScope) != SCchild)
                    return true;
            }
        }
        else
        {
            finish();
            return false;
        }
    }

    virtual const char * queryScope() const  override
    {
        const char * scope = notes.item(baseIndex).queryScope();
        return scope ? scope : "";
    }

    virtual StatisticScopeType getScopeType() const override
    {
        const char * tail = queryScopeTail(queryScope());
        StatsScopeId id(tail);
        return id.queryScopeType();
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties = PTall) override
    {
        if (whichProperties & PTnotes)
        {
            for (unsigned i=0; i < numCurrentScope; i++)
            {
                IConstWUException & cur = notes.item(baseIndex + i);
                visitor.noteException(cur);
            }
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        return false;
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        return nullptr;
    }

    virtual const char * queryHint(const char * kind) const override
    {
        return nullptr;
    }

private:
    bool updateCurrent()
    {
        if (!notes.isItem(baseIndex))
            return false;

        // set numCurrentScope to number of notes in the current scope
        unsigned next = baseIndex+1;
        while (next < notes.ordinality())
        {
            if (compareNoteScopeOrder(notes.item(baseIndex), notes.item(next)) != 0)
                break;
            next++;
        }
        numCurrentScope = (next - baseIndex);
        return true;
    }
    void finish()
    {
        baseIndex = notes.ordinality();
    }

    unsigned baseIndex = 0;
    unsigned numCurrentScope = 0;
    IArrayOf<IConstWUException> notes;
};


int compareStatisticScopes(IConstWUStatistic & left, IConstWUStatistic & right)
{
    return compareScopeName(left.queryScope(), right.queryScope());
}

int compareStatisticScopes(IInterface * const * left, IInterface * const * right)
{
    return compareStatisticScopes(*static_cast<IConstWUStatistic *>(*left), *static_cast<IConstWUStatistic *>(*right));
}

/*
 * An implementation of IConstWUScopeIterator for global workunit statistics.
 */
class WorkUnitStatisticsScopeIterator : public CInterfaceOf<IConstWUScopeIterator>
{
public:
    WorkUnitStatisticsScopeIterator(const IArrayOf<IConstWUStatistic> & _statistics, const ScopeFilter & _filter)
    {
        ForEachItemIn(i, _statistics)
        {
            IConstWUStatistic & cur = _statistics.item(i);
            if (_filter.compare(cur.queryScope()) & SCequal)
                statistics.append(OLINK(cur));
        }
        statistics.sort(compareStatisticScopes);
    }

    virtual bool first() override
    {
        curIndex = 0;
        return initScope();
    }

    virtual bool next() override
    {
        curIndex += numStatistics;
        return initScope();
    }

    virtual bool nextSibling() override
    {
        //Search until the current scope is not a child of the previous scope
        StringBuffer savedScope(queryScope());
        for (;;)
        {
            if (!next())
                return false;

            if (compareScopes(queryScope(), savedScope) != SCchild)
                return true;
        }
    }

    virtual bool nextParent() override
    {
        //Search until the current scope is not a child of the previous parent scope
        StringBuffer parentScope;
        if (getParentScope(parentScope, queryScope()))
        {
            for (;;)
            {
                if (!next())
                    return false;

                if (compareScopes(queryScope(), parentScope) != SCchild)
                    return true;
            }
        }
        else
        {
            finish();
            return false;
        }
    }

    virtual bool isValid() override
    {
        return statistics.isItem(curIndex);
    }

    virtual const char * queryScope() const override
    {
        return statistics.item(curIndex).queryScope();
    }

    virtual StatisticScopeType getScopeType() const override
    {
        return statistics.item(curIndex).getScopeType();
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties) override
    {
        if (whichProperties & PTstatistics)
        {
            for (unsigned i=0; i < numStatistics; i++)
            {
                IConstWUStatistic & cur = statistics.item(curIndex + i);
                visitor.noteStatistic(cur.getKind(), cur.getValue(), cur);
            }
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        for (unsigned i=0; i < numStatistics; i++)
        {
            IConstWUStatistic & cur = statistics.item(curIndex + i);
            if (cur.getKind() == kind)
            {
                value = cur.getValue();
                return true;
            }
        }
        return false;
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        return nullptr;
    }

    virtual const char * queryHint(const char * kind) const
    {
        return nullptr;
    }

protected:
    inline IConstWUStatistic & queryStatistic(unsigned i)
    {
        return statistics.item(curIndex + i);
    }

    bool initScope()
    {
        if (!statistics.isItem(curIndex))
            return false;

        unsigned next = curIndex+1;
        while (next < statistics.ordinality())
        {
            if (compareStatisticScopes(statistics.item(curIndex), statistics.item(next)) != 0)
                break;
            next++;
        }
        numStatistics = (next - curIndex);
        return true;
    }

    void finish()
    {
        curIndex = statistics.ordinality();
    }

protected:
    IArrayOf<IConstWUStatistic> statistics;
    unsigned curIndex = 0;
    unsigned numStatistics = 1;
};



/*
 * An implementation of IConstWUScopeIterator for the query graphs.
 */
class GraphScopeIterator : public CInterfaceOf<IConstWUScopeIterator>
{
private:
    //This uses a state machine - this enumeration contains the different states.
    enum State
    {
        //Following are the states which represent valid scopes
        SGraph,
        SChildGraph,
        SSubGraph,
        SEdge,
        SActivity,
        //The following are internal states.
        SGraphFirstEdge,
        SGraphFirstSubGraph,
        SGraphFirst,
        SGraphEnd,
        SGraphNext,

        SSubGraphFirstEdge,
        SSubGraphFirstActivity,
        SSubGraphEnd,
        SSubGraphNext,

        SEdgeNext,
        SEdgeEnd,

        SActivityNext,
        SActivityEnd,

        SChildGraphFirstEdge,
        SChildGraphFirstSubGraph,
        SChildGraphFirst,
        SChildGraphNext,
        SChildGraphEnd,

        SDone
    };
    State state = SDone;
    State nextState = SDone;

public:
    GraphScopeIterator(const IConstWorkUnit * wu, const ScopeFilter & _filter) : graphIter(&wu->getGraphs(GraphTypeAny)), filter(_filter)
    {
    }

    virtual bool first() override
    {
        state = SGraphFirst;
        return nextScope();
    }

    virtual bool next() override
    {
        if (!selectNext())
            return false;

        return nextScope();
    }

    virtual bool nextSibling() override
    {
        if (!selectNextSibling())
            return false;

        return nextScope();
    }

    virtual bool nextParent() override
    {
        if (!selectNextParent())
            return false;

        return nextScope();
    }

    virtual bool isValid() override
    {
        return graphIter->isValid();
    }

    virtual const char * queryScope() const override
    {
        return curScopeName.str();
    }

    virtual StatisticScopeType getScopeType() const override
    {
        return scopeType;
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties) override
    {
        switch (scopeType)
        {
        case SSTgraph:
            return;
        }

        IPropertyTree & cur = treeIters.tos().query();
        switch (scopeType)
        {
        case SSTgraph:
            break;
        case SSTsubgraph:
            break;
        case SSTactivity:
        {
            if (whichProperties & PTattributes)
            {
                playAttribute(visitor, WaLabel);
                Owned<IPropertyTreeIterator> attrs = cur.getElements("att");
                ForEach(*attrs)
                {
                    IPropertyTree & cur = attrs->query();
                    WuAttr attr = queryGraphChildAttToWuAttr(cur.queryProp("@name"));
                    if (attr != WaNone)
                        visitor.noteAttribute(attr, cur.queryProp("@value"));
                }
            }
            if (whichProperties & PThints)
            {
                Owned<IPropertyTreeIterator> hints = cur.getElements("hint");
                ForEach(*hints)
                {
                    IPropertyTree & cur = hints->query();
                    visitor.noteHint(cur.queryProp("@name"), cur.queryProp("@value"));
                }
            }
            break;
        }
        case SSTedge:
            if (whichProperties & PTattributes)
            {
                //MORE This will eventually need to walk the attributes and map the names.
                //Need to be careful if they need to be mapped differently depending on the context.
                playAttribute(visitor, WaLabel);
                playAttribute(visitor, WaIdSource);
                playAttribute(visitor, WaIdTarget);
                playAttribute(visitor, WaSourceIndex);
                playAttribute(visitor, WaTargetIndex);
                playAttribute(visitor, WaIsDependency);
            }
            break;
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        return false;
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        if (!treeIters.ordinality())
            return nullptr;
        //MORE - check that the attribute is value for the current scope type (to prevent defaults being returned)
        return queryAttributeValue(treeIters.tos().query(), attr, scratchpad);
    }

    virtual const char * queryHint(const char * kind) const override
    {
        //MORE: Needs to be implemented!
        return nullptr;
    }

private:
    void playAttribute(IWuScopeVisitor & visitor, WuAttr kind)
    {
        StringBuffer scratchpad;
        const char * value = queryAttributeValue(treeIters.tos().query(), kind, scratchpad);
        if (value)
            visitor.noteAttribute(kind, value);
    }

    void pushIterator(IPropertyTreeIterator * iter, State state)
    {
        treeIters.append(*LINK(iter));
        stateStack.append(state);
    }

    State popIterator()
    {
        treeIters.pop();
        return (State)stateStack.popGet();
    }

    void pushScope(const char * id)
    {
        scopeLengths.append(curScopeName.length());
        curScopeName.append(":").append(id);
    }

    void popScope()
    {
        curScopeName.setLength(scopeLengths.popGet());
    }

    bool doNextScope()
    {
        for(;;)
        {
            switch (state)
            {
            case SGraph:
            {
                IConstWUGraph & graph = graphIter->query();
                unsigned wfid = graph.getWfid();
                curScopeName.clear();
                if (wfid != 0)
                    curScopeName.append(WorkflowScopePrefix).append(wfid).append(':');
                graph.getName(StringBufferAdaptor(curScopeName));
                scopeType = SSTgraph;
                return true;
            }
            case SSubGraph:
                scopeId.set(SubGraphScopePrefix).append(treeIters.tos().query().getPropInt("@id"));
                pushScope(scopeId);
                scopeType = SSTsubgraph;
                return true;
            case SEdge:
                scopeId.set(EdgeScopePrefix).append(treeIters.tos().query().queryProp("@id"));
                pushScope(scopeId);
                scopeType = SSTedge;
                return true;
            case SActivity:
                if (treeIters.tos().query().getPropInt("att[@name='_kind']/@value") == TAKsubgraph)
                {
                    state = SActivityNext;
                    break;
                }
                scopeId.set(ActivityScopePrefix).append(treeIters.tos().query().getPropInt("@id"));
                pushScope(scopeId);
                scopeType = SSTactivity;
                return true;
            case SChildGraph:
            {
                unsigned numIters = treeIters.ordinality();
                //This should really be implemented by a filter on the node - but it would require _kind/_parentActivity to move to the node tag
                if (treeIters.tos().query().getPropInt("att[@name='_kind']/@value") != TAKsubgraph)
                {
                    state = SChildGraphNext;
                    break;
                }
                unsigned parentActivityId = treeIters.item(numIters-2).query().getPropInt("@id");
                unsigned parentId = treeIters.tos().query().getPropInt("att[@name='_parentActivity']/@value");
                if (parentId != parentActivityId)
                {
                    state = SChildGraphNext;
                    break;
                }
                scopeId.set(ChildGraphScopePrefix).append(treeIters.tos().query().getPropInt("@id"));
                pushScope(scopeId);
                scopeType = SSTchildgraph;
                return true;
            }
            //Graph iteration
            case SGraphFirst:
                if (!graphIter->first())
                    state = SDone;
                else
                    state = SGraph;
                break;
            case SGraphEnd:
                state = SGraphNext;
                break;
            case SGraphNext:
                if (!graphIter->next())
                    state = SDone;
                else
                    state = SGraph;
                break;
            //Edge iteration
            case SGraphFirstEdge:
            {
                //Walk dependencies - should possibly have a different SST e.g., SSTdependency since they do not
                //share many characteristics with edges - e.g. no flowing records => few/no stats.
                curGraph.setown(graphIter->query().getXGMMLTree(false));
                Owned<IPropertyTreeIterator> treeIter = curGraph->getElements("edge");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareEdgeNode));
                    treeIter->first();
                    pushIterator(treeIter, SGraphFirstSubGraph);
                    state = SEdge;
                }
                else
                    state = SGraphFirstSubGraph;
                break;
            }
            case SChildGraphFirstEdge:
            {
                Owned<IPropertyTreeIterator> treeIter = treeIters.tos().query().getElements("att/graph/edge");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareEdgeNode));
                    pushIterator(treeIter, SChildGraphFirstSubGraph);
                    state = SEdge;
                }
                else
                    state = SChildGraphFirstSubGraph;
                break;
            }
            case SEdgeEnd:
                popScope();
                state = SEdgeNext;
                break;
            case SEdgeNext:
                if (treeIters.tos().next())
                    state = SEdge;
                else
                    state = popIterator();
                break;
            //Subgraph iteration
            case SGraphFirstSubGraph:
            {
                Owned<IPropertyTreeIterator> treeIter = curGraph->getElements("node");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareSubGraphNode));
                    treeIter->first();
                    pushIterator(treeIter, SGraphNext);
                    state = SSubGraph;
                }
                else
                    state = SGraphNext;
                break;
            }
            case SChildGraphFirstSubGraph:
            {
                Owned<IPropertyTreeIterator> treeIter = treeIters.tos().query().getElements("att/graph/node");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareSubGraphNode));
                    pushIterator(treeIter, SChildGraphEnd);
                    state = SSubGraph;
                }
                else
                    state = SChildGraphEnd;
                break;
            }
            case SSubGraphFirstEdge:
            {
                Owned<IPropertyTreeIterator> treeIter = treeIters.tos().query().getElements("att/graph/edge");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareEdgeNode));
                    pushIterator(treeIter, SSubGraphFirstActivity);
                    state = SEdge;
                }
                else
                    state = SSubGraphFirstActivity;
                break;
            }
            case SSubGraphFirstActivity:
            {
                Owned<IPropertyTreeIterator> treeIter = treeIters.tos().query().getElements("att/graph/node");
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareActivityNode));
                    pushIterator(treeIter, SSubGraphEnd);
                    state = SActivity;
                }
                else
                    state = SSubGraphEnd;
                break;
            }
            case SSubGraphEnd:
                popScope();
                state = SSubGraphNext;
                break;
            case SChildGraphEnd:
                popScope();
                state = SChildGraphNext;
                break;
            case SSubGraphNext:
                if (treeIters.tos().next())
                    state = SSubGraph;
                else
                    state = popIterator();
                break;
            case SActivityEnd:
                popScope();
                state = SActivityNext;
                break;
            case SActivityNext:
                if (treeIters.tos().next())
                    state = SActivity;
                else
                    state = popIterator();
                break;
            case SChildGraphNext:
                if (treeIters.tos().next())
                    state = SChildGraph;
                else
                    state = popIterator();
                break;
            case SChildGraphFirst:
            {
                unsigned numIters = treeIters.ordinality();
                IPropertyTreeIterator & graphIter = treeIters.item(numIters-2);
                Owned<IPropertyTreeIterator> treeIter = graphIter.query().getElements("att/graph/node");
                //Really want to filter by <att name="_parentActivity" value="<parentid>">
                if (treeIter && treeIter->first())
                {
                    treeIter.setown(createSortedIterator(*treeIter, compareSubGraphNode));
                    pushIterator(treeIter, SActivityEnd);
                    state = SChildGraph;
                }
                else
                    state = SActivityEnd;
                break;
            }
            case SDone:
                return false;
            default:
                throwUnexpected();
            }
        }
    }

    bool nextScope()
    {
        for(;;)
        {
            if (!doNextScope())
                return false;

            ScopeCompare cmp = filter.compare(curScopeName);
            if (cmp & SCequal)
                return true;

            //MORE: Optimize next based on result of compare
            if (!selectNext())
                return false;
        }
    }

    bool selectNext()
    {
        switch (state)
        {
        case SGraph:
            state = SGraphFirstEdge;
            break;
        case SChildGraph:
            state = SChildGraphFirstEdge;
            break;
        case SSubGraph:
            state = SSubGraphFirstEdge;
            break;
        case SEdge:
            state = SEdgeEnd;
            break;
        case SActivity:
            state = SChildGraphFirst;
            break;
        case SDone:
            return false;
        default:
            throwUnexpected();
        }

        return true;
    }

    bool selectNextSibling()
    {
        switch (state)
        {
        case SGraph:
            state = SGraphEnd;
            break;
        case SChildGraph:
            state = SChildGraphEnd;
            break;
        case SSubGraph:
            state = SSubGraphEnd;
            break;
        case SEdge:
            state = SEdgeEnd;
            break;
        case SActivity:
            state = SActivityEnd;
            break;
        case SDone:
            return false;
        default:
            throwUnexpected();
        }

        return true;
    }

    bool selectNextParent()
    {
        switch (state)
        {
        case SGraph:
            state = SDone;
            break;
        case SChildGraph:
        case SSubGraph:
        case SEdge:
        case SActivity:
            popScope();
            state = popIterator();
            break;
        case SDone:
            return false;
        default:
            throwUnexpected();
        }
        return true;
    }

protected:
    const ScopeFilter & filter;
    Owned<IConstWUGraphIterator> graphIter;
    Owned<IPropertyTree> curGraph;
    IArrayOf<IPropertyTreeIterator> treeIters;
    UnsignedArray scopeLengths;
    UnsignedArray stateStack;
    StringBuffer curScopeName;
    StringBuffer scopeId;
    StatisticScopeType scopeType = SSTnone;
};

static int compareWorkflow(IInterface * const * pLeft, IInterface * const * pRight)
{
    IConstWorkflowItem * left = static_cast<IConstWorkflowItem *>(*pLeft);
    IConstWorkflowItem * right = static_cast<IConstWorkflowItem *>(*pRight);
    return left->queryWfid() - right->queryWfid();
}

static const char * trueToStr(bool value) { return value ? "true" : nullptr; }

/*
 * An implementation of IConstWUScopeIterator for the workflow information.
 */
class WorkflowStatisticsScopeIterator : public CInterfaceOf<IConstWUScopeIterator>
{
public:
    WorkflowStatisticsScopeIterator(IConstWorkflowItemIterator * wfIter)
    {
        ForEach(*wfIter)
            workflow.append(*LINK(wfIter->query()));
        workflow.sort(compareWorkflow);
    }

    virtual bool first() override
    {
        if (workflow.empty())
            return false;
        curWorkflow = 0;
        return initWorkflowItem();
    }

    virtual bool next() override
    {
        if (!workflow.isItem(++curWorkflow))
            return false;
        return initWorkflowItem();
    }

    virtual bool nextSibling() override
    {
        return next();
    }

    virtual bool nextParent() override
    {
        curWorkflow = NotFound;
        return false;
    }

    virtual bool isValid() override
    {
        return workflow.isItem(curWorkflow);
    }

    virtual const char * queryScope() const override
    {
        return curScope.str();
    }

    virtual StatisticScopeType getScopeType() const override
    {
        return SSTworkflow;
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties) override
    {
        if (whichProperties & PTattributes)
        {
            {
                StringBuffer scratchpad;
                Owned<IWorkflowDependencyIterator> depends = workflow.item(curWorkflow).getDependencies();
                ForEach(*depends)
                        visitor.noteAttribute(WaIdDependency, getValueText(depends->query(), scratchpad, WorkflowScopePrefix));
            }
            play(visitor, { WaIsScheduled, WaIdSuccess, WaIdFailure, WaIdRecovery, WaIdPersist, WaIdScheduled,
                            WaPersistName, WaLabel, WaMode, WaType, WaState, WaCluster, WaCriticalSection });
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        return false;
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        auto wf = &workflow.item(curWorkflow);
        StringBufferAdaptor adaptor(scratchpad);
        switch (attr)
        {
        case WaIdDependencyList:
        {
            bool first = true;
            Owned<IWorkflowDependencyIterator> depends = workflow.item(curWorkflow).getDependencies();
            ForEach(*depends)
            {
                if (first)
                    scratchpad.append("[");
                else
                    scratchpad.append(",");
                scratchpad.append('"').append(WorkflowScopePrefix).append(depends->query()).append('"');
                first = false;
            }
            if (first)
                return nullptr;
            scratchpad.append("]");
            return scratchpad.str();
        }
        case WaIsScheduled: return trueToStr(wf->isScheduled());
        case WaIdSuccess: return getWfidText(wf->querySuccess(), scratchpad);
        case WaIdFailure: return getWfidText(wf->queryFailure(), scratchpad);
        case WaIdRecovery: return getWfidText(wf->queryRecovery(), scratchpad);
        case WaIdPersist: return getWfidText(wf->queryPersistWfid(), scratchpad);
        case WaIdScheduled: return getWfidText(wf->queryScheduledWfid(), scratchpad);
        case WaPersistName: return queryOptString(wf->getPersistName(adaptor));
        case WaLabel: return queryOptString(wf->getLabel(adaptor));
        case WaMode: return wf->queryMode() != WFModeNormal ? queryWorkflowModeText(wf->queryMode()) : nullptr;
        case WaType: return wf->queryType() != WFTypeNormal ? queryWorkflowTypeText(wf->queryType()) : nullptr;
        case WaState: return queryWorkflowStateText(wf->queryState());
        case WaCluster: return queryOptString(wf->queryCluster(adaptor));
        case WaCriticalSection: return queryOptString(wf->getCriticalName(adaptor));
        /*
        The followng attributes are not generated - I'm not convinced they are very useful, but they could be added later.
        virtual bool isScheduledNow() const = 0;
        virtual IWorkflowEvent * getScheduleEvent() const = 0;
        virtual unsigned querySchedulePriority() const = 0;
        virtual bool hasScheduleCount() const = 0;
        virtual unsigned queryScheduleCount() const = 0;
        virtual unsigned queryRetriesAllowed() const = 0;
        virtual int queryPersistCopies() const = 0;  // 0 - unmangled name,  < 0 - use default, > 0 - max number
        virtual bool queryPersistRefresh() const = 0;
        virtual unsigned queryScheduleCountRemaining() const = 0;
        virtual unsigned queryRetriesRemaining() const = 0;
        virtual int queryFailCode() const = 0;
        virtual const char * queryFailMessage() const = 0;
        virtual const char * queryEventName() const = 0;
        virtual const char * queryEventExtra() const = 0;
        */
        }
        return nullptr;
    }

    virtual const char * queryHint(const char * kind) const
    {
        return nullptr;
    }

protected:
    bool initWorkflowItem()
    {
        curScope.clear().append(WorkflowScopePrefix).append(workflow.item(curWorkflow).queryWfid());
        return true;
    }

    const char * getValueText(unsigned value, StringBuffer & scratchpad, const char * prefix = nullptr) const
    {
        return scratchpad.clear().append(prefix).append(value).str();
    }

    const char * queryOptString(IStringVal & value) const
    {
        const char * text = value.str();
        if (!text || !*text)
            return nullptr;
        return text;
    }

    const char * getWfidText(unsigned value, StringBuffer & scratchpad) const
    {
        if (!value)
            return nullptr;
        return scratchpad.clear().append(WorkflowScopePrefix).append(value).str();
    }

    void play(IWuScopeVisitor & visitor, const std::initializer_list<WuAttr> & attrs) const
    {
        StringBuffer scratchpad;
        for (auto attr : attrs)
        {
            const char * value = queryAttribute(attr, scratchpad.clear());
            if (value)
                visitor.noteAttribute(attr, value);
        }
    }

protected:
    IArrayOf<IConstWorkflowItem> workflow;
    unsigned curWorkflow = 0;
    StringBuffer curScope;
};




/*
 * An implementation of IConstWUScopeIterator that combines results from multiple sources.
 */
class CompoundStatisticsScopeIterator : public CInterfaceOf<IConstWUScopeIterator>
{
    class VisitorMapper : implements IWuScopeVisitor
    {
    public:
        VisitorMapper(const WuScopeFilter & _filter, IWuScopeVisitor & _visitor) :
            filter(_filter), visitor(_visitor)
        {}

        virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
        {
            if (filter.includeStatistic(kind))
                visitor.noteStatistic(kind, value, extra);
        }

        virtual void noteAttribute(WuAttr attr, const char * value) override
        {
            if (filter.includeAttribute(attr))
                visitor.noteAttribute(attr, value);
        }

        virtual void noteHint(const char * kind, const char * value) override
        {
            if (filter.includeHint(kind))
                visitor.noteHint(kind, value);
        }
        virtual void noteException(IConstWUException & exception) override
        {
            visitor.noteException(exception);
        }

    protected:
        const WuScopeFilter & filter;
        IWuScopeVisitor & visitor;
    };

public:
    CompoundStatisticsScopeIterator(const WuScopeFilter & _filter) : filter(_filter)
    {
    }
    void addIter(IConstWUScopeIterator * iter)
    {
        if (iter)
        {
            iters.append(OLINK(*iter));
            assertex(iters.ordinality() <= sizeof(activeIterMask)*8);
        }
    }

    virtual bool first() override
    {
        activeIterMask = 0;
        ForEachItemIn(i, iters)
        {
            if (iters.item(i).first())
                activeIterMask |= (1U << i);
        }

        return findNextScope();
    }

    virtual bool next() override
    {
        selectNext();
        return findNextScope();
    }

    virtual bool nextSibling() override
    {
        selectNextSibling();
        return findNextScope();
    }

    virtual bool nextParent() override
    {
        selectNextParent();
        return findNextScope();
    }

    virtual bool isValid() override
    {
        return (activeIterMask != 0);
    }

    virtual const char * queryScope() const override
    {
        return iters.item(firstMatchIter).queryScope();
    }

    virtual StatisticScopeType getScopeType() const override
    {
        return iters.item(firstMatchIter).getScopeType();
    }

    virtual void playProperties(IWuScopeVisitor & visitor, WuPropertyTypes whichProperties) override
    {
        VisitorMapper mappedVisitor(filter, visitor);
        whichProperties &= filter.properties;
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
                iters.item(i).playProperties(mappedVisitor, whichProperties);
        }
    }

    virtual bool getStat(StatisticKind kind, unsigned __int64 & value) const override
    {
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
                if (iters.item(i).getStat(kind, value))
                    return true;
            }
        }
        return false;
    }

    virtual const char * queryAttribute(WuAttr attr, StringBuffer & scratchpad) const override
    {
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
                const char * value = iters.item(i).queryAttribute(attr, scratchpad);
                if (value)
                    return value;
            }
        }
        return nullptr;
    }

    virtual const char * queryHint(const char * kind) const override
    {
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
                const char * value = iters.item(i).queryHint(kind);
                if (value)
                    return value;
            }
        }
        return nullptr;
    }

    inline bool iterMatchesCurrentScope(unsigned i) const { return ((1U << i) & matchIterMask) != 0; }
    inline bool isAlive(unsigned i) const { return ((1U << i) & activeIterMask) != 0; }

protected:
    //Calculate which iterators contain the scope that should come next
    bool findNextScope()
    {
        for(;;)
        {
            if (activeIterMask == 0)
                return false;

            unsigned mask = 0;
            const char * scope = nullptr;
            ForEachItemIn(i, iters)
            {
                if (isAlive(i))
                {
                    const char * iterScope = iters.item(i).queryScope();
                    if (mask)
                    {
                        int compare = compareScopeName(scope, iterScope);
                        if (compare == 0)
                        {
                            mask |= (1U << i);
                        }
                        else if (compare > 0)
                        {
                            scope = iterScope;
                            mask = (1U << i);
                            firstMatchIter = i;
                        }
                    }
                    else
                    {
                        scope = iterScope;
                        mask = (1U << i);
                        firstMatchIter = i;
                    }
                }
            }
            matchIterMask = mask;

            while (activeScopes)
            {
                const char * activeScope = activeScopes.tos();
                //If the next scope if not a child of one of the active scopes then that active scope will no longer match.
                if (compareScopes(scope, activeScope) & SCchild)
                    break;
                activeScopes.pop();
            }

            //The top most scope will be the deepest.  Check if the current scope is close enough to return as a match.
            bool include = false;
            if (activeScopes)
            {
                const char * activeScope = activeScopes.tos();

                //code above has ensured that this scope must be a child of (and therefore deeper) than activeScope
                unsigned nesting = queryScopeDepth(scope) - queryScopeDepth(activeScope);
                if (nesting <= filter.include.nestedDepth)
                    include = true;
            }

            //Check to see if this is a new match
            if (filter.compareMatchScopes(scope) & SCequal)
            {
                if (!include)
                    include = filter.include.matchedScope;

                //Only add it to the list of active scopes if it can match child elements
                if (filter.include.nestedDepth != 0)
                    activeScopes.append(scope);
            }

            if (include)
            {
                if (!filter.includeScope(scope))
                    include = false;
                else if (filter.requiredStats.size())
                {
                    //MORE: This would be cleaner as a member of filter - but it needs access to the stats.
                    for (unsigned iReq=0; iReq < filter.requiredStats.size(); iReq++)
                    {
                        const StatisticValueFilter & cur = filter.requiredStats[iReq];
                        unsigned __int64 value;
                        if (getStat(cur.queryKind(), value))
                        {
                            if (!cur.matches(value))
                                include = false;
                        }
                        else
                            include = false;
                    }
                }
            }

            if (include)
                return true;

            //MORE: Optimize based on filter.compareScope()
            selectNext();
        }
    }

    void checkScopeOrder(unsigned input, const char * prevScope)
    {
        if (isAlive(input))
        {
            const char * curScope = iters.item(input).queryScope();
            int compare = compareScopeName(prevScope, curScope);
            if (compare >= 0)
                throw MakeStringException(0, "Out of order (%u) scopes %s,%s = %d", input, prevScope, curScope, compare);
        }
    }

    void selectNext()
    {
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
#ifdef _DEBUG
                StringBuffer prevScope(iters.item(i).queryScope());
#endif

                if (!iters.item(i).next())
                    activeIterMask &= ~(1U << i);
#ifdef _DEBUG
                checkScopeOrder(i, prevScope);
#endif
            }
        }
    }

    void selectNextSibling()
    {

        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
#ifdef _DEBUG
                StringBuffer prevScope(iters.item(i).queryScope());
#endif

                if (!iters.item(i).nextSibling())
                    activeIterMask &= ~(1U << i);
#ifdef _DEBUG
                checkScopeOrder(i, prevScope);
#endif
            }
        }
    }

    void selectNextParent()
    {
        ForEachItemIn(i, iters)
        {
            if (iterMatchesCurrentScope(i))
            {
#ifdef _DEBUG
                StringBuffer prevScope(iters.item(i).queryScope());
#endif

                if (!iters.item(i).nextParent())
                    activeIterMask &= ~(1U << i);
#ifdef _DEBUG
                checkScopeOrder(i, prevScope);
#endif
            }
        }
    }

protected:
    const WuScopeFilter & filter;
    IArrayOf<IConstWUScopeIterator> iters;
    unsigned curIter = 0;
    unsigned firstMatchIter = 0;
    unsigned matchIterMask = 0; // bit set of iterators which have a valid entry for the current scope
    unsigned activeIterMask = 0; // bit set of iterators which are still active
    StringArray activeScopes;
};

//---------------------------------------------------------------------------------------------------------------------

class AggregateFilter
{
public:
    AggregateFilter(StatisticKind _search) : search(_search) {}

    inline bool matches(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) const
    {
        return (kind == search);
    }

protected:
    StatisticKind search;
    //MORE: Allow filtering by scope?
};

class StatisticAggregator : public CInterfaceOf<IWuScopeVisitor>
{
public:
    StatisticAggregator(StatisticKind _search) : filter(_search) {}

    virtual void noteAttribute(WuAttr attr, const char * value) override { throwUnexpected(); }
    virtual void noteHint(const char * kind, const char * value) override { throwUnexpected(); }
    virtual void noteException(IConstWUException & exception) { throwUnexpected();}
protected:
    AggregateFilter filter;
};

class SimpleAggregator : public StatisticAggregator
{
public:
    SimpleAggregator(StatisticKind _search) : StatisticAggregator(_search) {}

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
    {
        if (filter.matches(kind, value, extra))
            summary.noteValue(value);
    }

    //How should these be reported?  Should there be a playAggregates(IWuAggregatedScopeVisitor)
    //with a noteAggregate(value, variant, value, grouping)?
protected:
    StatsAggregation summary;
};


class SimpleReferenceAggregator : public StatisticAggregator
{
public:
    SimpleReferenceAggregator(StatisticKind _search, StatsAggregation & _summary) : StatisticAggregator(_search), summary(_summary) {}

    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
    {
        if (filter.matches(kind, value, extra))
            summary.noteValue(value);
    }

    //How should these be reported?  Should there be a playAggregates(IWuAggregatedScopeVisitor)
    //with a noteAggregate(value, variant, value, grouping)?
protected:
    StatsAggregation & summary;
};


class GroupedAggregator : public StatisticAggregator
{
public:
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
    {
        if (filter.matches(kind, value, extra))
        {
            StatsAggregation & match = summary; // look up in hash table.
            match.noteValue(value);
        }
    }

protected:
    //HashTable of (possible groupings->summary)
    StatsAggregation summary;
};


class CompoundAggregator : implements StatisticAggregator
{
public:
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra)
    {
        ForEachItemIn(i, aggregators)
            aggregators.item(i).noteStatistic(kind, value, extra);
    }

protected:
    IArrayOf<StatisticAggregator> aggregators;
};


//To calculate aggregates, create a scope iterator, and an instance of a StatisticAggregator, and play the attributes through the interface

//---------------------------------------------------------------------------------------------------------------------


//Extract an argument of the format abc[def[ghi...,]],... as (abc,def[...])
static bool extractOption(const char * & finger, StringBuffer & option, StringBuffer & arg)
{
    const char * start = finger;
    if (!*start)
        return false;

    const char * cur = start;
    const char * bra = nullptr;
    const char * end = nullptr;
    unsigned braDepth = 0;
    char next;
    arg.clear();
    for (;;)
    {
        next = *cur;
        if (!next || ((next == ',') && (braDepth == 0)))
            break;

        switch (next)
        {
        case '[':
            if (braDepth == 0)
            {
                if (bra)
                    throw makeStringExceptionV(0, "Multiple [ in filter : %s", bra);
                bra = cur;
            }
            braDepth++;
            break;
        case ']':
            if (braDepth > 0)
            {
                if (--braDepth == 0)
                {
                    end = cur;
                    arg.append(cur - (bra+1), bra+1);
                }
            }
            break;
        }

        cur++;
    }

    if (braDepth != 0)
        throw makeStringExceptionV(0, "Mismatched ] in filter : %s", start);

    option.clear();
    if (bra)
    {
        if (cur != end+1)
            throw makeStringExceptionV(0, "Text follows closing bracket: %s", end);
        option.append(bra-start, start);
    }
    else
        option.append(cur-start, start);

    if (next)
        finger = cur+1;
    else
        finger = cur;
    return true;
}

static unsigned readOptValue(const char * start, const char * end, unsigned dft, const char * type)
{
    if (start == end)
        return dft;
    char * next;
    unsigned value = (unsigned)strtoll(start, &next, 10);
    if (next != end)
        throw makeStringExceptionV(0, "Unexpected characters in %s option '%s'", type, next);
    return value;
}

static unsigned readValue(const char * start, const char * type)
{
    if (*start == '\0')
        throw makeStringExceptionV(0, "Expected a value for the %s option", type);

    char * next;
    unsigned value = (unsigned)strtoll(start, &next, 10);
    if (*next != '\0')
        throw makeStringExceptionV(0, "Unexpected characters in %s option '%s'", type, next);
    return value;
}

/*
Scope service matching Syntax:  * indicates
Which items are matched:
    scope[<scope-id>]* | stype[<scope-type>]* | id[<id>]* - which scopes should be matched?
    depth[n | low..high] - range of depths to search for a match
    source[global|stats|graph|all]* - which sources to search within the workunit
    where[<statistickind> | <statistickind> (=|<|<=|>|>=) value | <statistickind>=low..high] - a statistic filter
Which items are include in the results:
    matched[true|false] - are the matched scopes returned?
    nested[<depth>|all] - how deep within a scope should be matched (default = 0 if matched[true], all if matched[false])
    includetype[<scope-type>] - which scope types should be included?
Which properties of the items are returned:
    properties[statistics|hints|attributes|scope|all]
    statistic[<statistic-kind>|none|all] - include statistic
    attribute[<attribute-name>|none|all] - include attribute
    hint[<hint-name>] - include hint
    property[<statistic-kind>|<attribute-name>|<hint-name>] - include property
    measure[<measure>] - all statistics with a particular measure
    version[<version>] - minimum version to return
*/

enum { FOscope, FOstype, FOid, FOdepth, FOsource, FOwhere, FOmatched, FOnested, FOinclude, FOproperties, FOstatistic, FOattribute, FOhint, FOproperty, FOmeasure, FOversion, FOunknown };
//Some of the following contains aliases for the same option e.g. stat and statistic
static constexpr EnumMapping filterOptions[] = {
        { FOscope, "scope" }, { FOstype, "stype" }, { FOid, "id" },
        { FOdepth, "depth" }, { FOsource, "source" }, { FOwhere, "where" },
        { FOmatched, "matched" }, { FOnested, "nested" },
        { FOinclude, "include" }, { FOinclude, "includetype" },
        { FOproperties, "props" }, { FOproperties, "properties" }, // some aliases
        { FOstatistic, "stat" }, { FOstatistic, "statistic" }, { FOattribute, "attr" }, { FOattribute, "attribute" }, { FOhint, "hint" },
        { FOproperty, "prop" }, { FOproperty, "property" },
        { FOmeasure, "measure" }, { FOversion, "version" }, { 0, nullptr} };
static constexpr EnumMapping sourceMappings[] = {
        { SSFsearchGlobalStats, "global" }, { SSFsearchGraphStats, "stats" }, { SSFsearchGraphStats, "statistics" }, { SSFsearchGraph, "graph" }, { SSFsearchExceptions, "exception" }, { SSFsearchWorkflow, "workflow" },
        { (int)SSFsearchAll, "all" }, { 0, nullptr } };
static constexpr EnumMapping propertyMappings[] = {
        { PTstatistics, "stat" }, { PTstatistics, "statistic" }, { PTattributes, "attr" }, { PTattributes, "attribute" }, { PThints, "hint" },{ PTnotes, "note" },
        { PTstatistics, "stats" }, { PTstatistics, "statistics" }, { PTattributes, "attrs" }, { PTattributes, "attributes" }, { PThints, "hints" }, { PTnotes, "notes" },
        { PTnone, "none" }, { PTscope, "scope" }, { PTall, "all" }, { 0, nullptr } };



WuScopeSourceFlags querySource(const char * source) { return (WuScopeSourceFlags)getEnum(source, sourceMappings, SSFunknown); }
const char * querySourceText(WuScopeSourceFlags source) { return getEnumText(source, sourceMappings, nullptr); }

WuScopeFilter::WuScopeFilter(const char * filter)
{
    addFilter(filter);
    finishedFilter();
}

WuScopeFilter & WuScopeFilter::addFilter(const char * filter)
{
    if (!filter)
        return *this;

    StringBuffer option;
    StringBuffer arg;
    while (extractOption(filter, option, arg))
    {
        switch (getEnum(option, filterOptions, FOunknown))
        {
        case FOscope:
            addScope(arg);
            break;
        case FOstype:
            addScopeType(arg);
            break;
        case FOid:
            addId(arg);
            break;
        case FOdepth:
        {
            //Allow depth[n], depth[a,b] or depth[a..b]
            const char * comma = strchr(arg, ',');
            const char * dotdot = strstr(arg, "..");
            if (comma)
            {
                unsigned low = readOptValue(arg, comma, 0, "depth");
                if (comma[1])
                {
                    scopeFilter.setDepth(low, readValue(comma+1, "depth"));
                }
                else
                    scopeFilter.setDepth(low, UINT_MAX);
            }
            else if (dotdot)
            {
                unsigned low = readOptValue(arg, dotdot, 0, "depth");
                if (dotdot[2])
                    scopeFilter.setDepth(low, readValue(dotdot+2, "depth"));
                else
                    scopeFilter.setDepth(low, UINT_MAX);
            }
            else
            {
                scopeFilter.setDepth(readValue(arg, "depth"));
            }
            break;
        }
        case FOsource:
            addSource(arg);
            break;
        case FOwhere: // where[stat<op>value]
            addRequiredStat(arg);
            break;
        case FOmatched:
            setIncludeMatch(strToBool(arg));
            break;
        case FOnested:
            if (strieq(arg, "all"))
                setIncludeNesting(UINT_MAX);
            else if (isdigit(*arg))
                setIncludeNesting(atoi(arg));
            else
                throw makeStringExceptionV(0, "Expected a value for the nesting depth: %s", arg.str());
            break;
        case FOinclude:
            setIncludeScopeType(arg);
            break;
        case FOproperties:
        {
            WuPropertyTypes prop = (WuPropertyTypes)getEnum(arg, propertyMappings, PTunknown);
            if (prop == PTunknown)
                throw makeStringExceptionV(0, "Unexpected properties '%s'", arg.str());
            addOutputProperties(prop);
            break;
        }
        case FOstatistic:
            addOutputStatistic(arg);
            break;
        case FOattribute:
            addOutputAttribute(arg);
            break;
        case FOhint:
            addOutputHint(arg);
            break;
        case FOproperty:
            addOutput(arg);
            break;
        case FOmeasure:
            setMeasure(arg);
            break;
        case FOversion:
            if (isdigit(*arg))
                minVersion = atoi64(arg);
            else
                throw makeStringExceptionV(0, "Expected a value for the version: %s", arg.str());
            break;
        default:
            throw makeStringExceptionV(0, "Unrecognised filter option: %s", option.str());
        }
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::addScope(const char * scope)
{
    if (scope)
    {
        validateScope(scope);
        scopeFilter.addScope(scope);
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::addScopeType(const char * scopeType)
{
    if (scopeType)
    {
        StatisticScopeType sst = queryScopeType(scopeType, SSTmax);
        if (sst == SSTmax)
            throw makeStringExceptionV(0, "Unrecognised scope type '%s'", scopeType);

        scopeFilter.addScopeType(sst);
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::addId(const char * id)
{
    validateScopeId(id);
    scopeFilter.addId(id);
    return *this;
}

WuScopeFilter & WuScopeFilter::addOutput(const char * prop)
{
    WuAttr attr = queryWuAttribute(prop, WaNone);
    if (attr != WaNone)
    {
        WuAttr singleKindAttr = getSingleKindOfListAttribute(attr);
        if (singleKindAttr != WaNone)
            addOutputAttribute(singleKindAttr);
        else
            addOutputAttribute(attr);

    } else if (queryStatisticKind(prop, StMax) != StMax)
        addOutputStatistic(prop);
    else
        addOutputHint(prop);
    return *this;
}

WuScopeFilter & WuScopeFilter::addOutputStatistic(const char * prop)
{
    if (!prop)
        return *this;

    StatisticKind kind = queryStatisticKind(prop, StMax);
    if (kind == StMax)
        throw makeStringExceptionV(0, "Unrecognised statistic '%s'", prop);

    return addOutputStatistic(kind);
}

WuScopeFilter & WuScopeFilter::addOutputStatistic(StatisticKind stat)
{
    if (stat != StKindNone)
    {
        if (stat != StKindAll)
            desiredStats.append(stat);
        else
            desiredStats.kill();
        properties |= PTstatistics;
    }
    else
        properties &= ~PTstatistics;
    return *this;
}

WuScopeFilter & WuScopeFilter::addOutputAttribute(const char * prop)
{
    if (!prop)
        return *this;

    WuAttr attr = queryWuAttribute(prop, WaMax);
    if (attr == WaMax)
        throw makeStringExceptionV(0, "Unrecognised attribute '%s'", prop);

    return addOutputAttribute(attr);
}

WuScopeFilter & WuScopeFilter::addOutputAttribute(WuAttr attr)
{
    if (attr != WaNone)
    {
        if (attr != WaAll)
            desiredAttrs.append(attr);
        else
            desiredAttrs.kill();
        properties |= PTattributes;
    }
    else
        properties &= ~PTattributes;
    return *this;
}


WuScopeFilter & WuScopeFilter::addOutputHint(const char * prop)
{
    if (strieq(prop, "none"))
    {
        desiredHints.kill();
        properties &= ~PThints;
    }
    else
    {
        if (!strieq(prop, "all"))
            desiredHints.append(prop);
        else
            desiredHints.kill();
        properties |= PThints;
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::setIncludeMatch(bool value)
{
    include.matchedScope = value;
    return *this;
}

WuScopeFilter & WuScopeFilter::setIncludeNesting(unsigned depth)
{
    include.nestedDepth = depth;
    return *this;
}

WuScopeFilter & WuScopeFilter::setIncludeScopeType(const char * scopeType)
{
    if (scopeType)
    {
        StatisticScopeType sst = queryScopeType(scopeType, SSTmax);
        if (sst == SSTmax)
            throw makeStringExceptionV(0, "Unrecognised scope type '%s'", scopeType);

        include.scopeTypes.append(sst);
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::setMeasure(const char * measure)
{
    if (measure)
    {
        desiredMeasure = queryMeasure(measure, SMeasureNone);
        if (desiredMeasure == SMeasureNone)
            throw makeStringExceptionV(0, "Unrecognised measure '%s'", measure);
        properties |= PTstatistics;
    }
    return *this;
}

WuScopeFilter & WuScopeFilter::addOutputProperties(WuPropertyTypes mask)
{
    if (properties == PTnone)
        properties = mask;
    else
        properties |= mask;
    return *this;
}

WuScopeFilter & WuScopeFilter::addRequiredStat(StatisticKind statKind, stat_type lowValue, stat_type highValue)
{
    requiredStats.emplace_back(statKind, lowValue, highValue);
    return *this;
}

WuScopeFilter & WuScopeFilter::addRequiredStat(StatisticKind statKind)
{
    requiredStats.emplace_back(statKind, 0, MaxStatisticValue);
    return *this;
}

//process a filter in one of the following forms:
//  <statistic-name>
//  <statistic-name> (=|<|<=|>|>=) <value>
//  <statistic-name>=[<low>]..[<high>]

void WuScopeFilter::addRequiredStat(const char * filter)
{
    const char * stat = filter;
    const char * cur = stat;
    while (isalpha(*cur))
        cur++;

    StringBuffer statisticName(cur-stat, stat);
    StatisticKind statKind = queryStatisticKind(statisticName, StKindNone);
    if (statKind == StKindNone)
        throw makeStringExceptionV(0, "Unknown statistic name '%s'", statisticName.str());

    //Skip any spaces before a comparison operator.
    while (*cur && isspace(*cur))
        cur++;

    //Save the operator, and skip over any non digits.
    const char * op = cur;
    switch (*op)
    {
    case '=':
        cur++;
        break;
    case '<':
    case '>':
        if (op[1] == '=')
            cur += 2;
        else
            cur++;
        break;
    case '\0':
        break;
    default:
        throw makeStringExceptionV(0, "Unknown comparison '%s'", op);
    }

    const char * next;
    stat_type value = readStatisticValue(cur, &next, queryMeasure(statKind));
    stat_type lowValue = 0;
    stat_type highValue = MaxStatisticValue;
    switch (op[0])
    {
    case '=':
    {
        //Allow a,b or a..b to specify a range - either bound may be omitted.
        if (next[0] == ',')
        {
            lowValue = value;
            next++;
            if (*next != '\0')
                highValue = readStatisticValue(next, &next, queryMeasure(statKind));
        }
        else if (strncmp(next, "..", 2) == 0)
        {
            lowValue = value;
            next += 2;
            if (*next != '\0')
                highValue = readStatisticValue(next, &next, queryMeasure(statKind));
        }
        else
        {
            lowValue = value;
            highValue = value;
        }
        break;
    }
    case '<':
        if (op[1] == '=')
            highValue = value;
        else
            highValue = value-1;
        break;
    case '>':
        if (op[1] == '=')
            lowValue = value;
        else
            lowValue = value+1;
        break;
    }

    if (*next)
        throw makeStringExceptionV(0, "Trailing characters in where '%s'", next);

    requiredStats.emplace_back(statKind, lowValue, highValue);
}

WuScopeFilter & WuScopeFilter::addSource(const char * source)
{
    WuScopeSourceFlags mask = querySource(source);
    if (mask == SSFunknown)
        throw makeStringExceptionV(0, "Unexpected source '%s'", source);
    if (!mask)
        sourceFlags = mask;
    else
        sourceFlags |= mask;
    return *this;
}

WuScopeFilter & WuScopeFilter::setDepth(unsigned low, unsigned high)
{
    scopeFilter.setDepth(low, high);
    return *this;
}

bool WuScopeFilter::matchOnly(StatisticScopeType scopeType) const
{
    //If there is a post filter on the scopeType, then check if it matches
    if (include.scopeTypes.ordinality() == 1)
        return (include.scopeTypes.item(0) == scopeType);

    //If filter doesn't match nested items, then check if the scope filter matches.
    if (include.nestedDepth == 0)
    {
        if (scopeFilter.matchOnly(scopeType))
            return true;
    }
    return false;
}


//Called once the filter has been updated to optimize the filter
void WuScopeFilter::finishedFilter()
{
    scopeFilter.finishedFilter();

    assertex(!optimized);
    optimized = true;

    if ((include.nestedDepth == 0) && !include.matchedScope)
        include.nestedDepth = UINT_MAX;
    preFilterScope = include.matchedScope && (include.nestedDepth == 0);
    if (scopeFilter.canAlwaysPreFilter())
        preFilterScope = true;

    //If the source flags have not been explicitly set then calculate which sources will provide the results
    if (!sourceFlags)
    {
        sourceFlags = SSFsearchAll;

        //Use the other options to reduce the number of elements that are searched.
        //If not interested in scopes on their own then...
        if (!(properties & PTscope))
        {
            //Global stats and graph stats only contain stats => remove if not interested in them
            if (!(properties & PTstatistics))
                sourceFlags &= ~(SSFsearchGlobalStats|SSFsearchGraphStats);

            //graph, workflow only contains attributes and hints => remove if not interested
            if (!(properties & (PTattributes|PThints)))
                sourceFlags &= ~(SSFsearchGraph|SSFsearchWorkflow);
        }

        if (!(properties & PTnotes))
            sourceFlags &= ~SSFsearchExceptions;
    }

    //Optimize sources if they haven't been explicitly specified
    //Most of the following are dependent on internal knowledge of the way that information is represented
    if (matchOnly(SSTworkflow))
    {
        //Workflow is not nested within a graph
        sourceFlags &= ~(SSFsearchGraph|SSFsearchGraphStats);
        setDepth(1, 1);
    }
    else if (matchOnly(SSTgraph))
    {
        //Graph starts are stored globally, not in the graph stats.
        sourceFlags &= ~(SSFsearchGraphStats|SSFsearchWorkflow);

        if (!(properties & (PTattributes|PThints)))
            sourceFlags &= ~(SSFsearchGraph);

        //This should really be setDepth(2,2) but workunits prior to 7.4 did not have graph ids prefixed by the wfid
        //Remove once 7.2 is a distant memory (see HPCC-22887)
        setDepth(1, 2);
    }
    else if (matchOnly(SSTsubgraph))
    {
        sourceFlags &= ~(SSFsearchWorkflow);
    }
    else if (matchOnly(SSTcompilestage))
    {
        //compile stages are not stored in the graph
        sourceFlags &= ~(SSFsearchGraphStats|SSFsearchGraph|SSFsearchWorkflow);
    }
    else if (matchOnly(SSTactivity))
    {
        //information about activities is not stored globally
        sourceFlags &= ~(SSFsearchGlobalStats|SSFsearchWorkflow);
    }

    // Everything stored in the graphs stats has a depth of 3 or more - so ignore if there are not matches in that depth
    if (include.nestedDepth == 0)
    {
        if (scopeFilter.compareDepth(3) > 0)    // 3 is larger than any scope in the filter
            sourceFlags &= ~(SSFsearchGraphStats);
        else if (scopeFilter.compareDepth(3) == 0)    // 3 matches the maximum scope in the filter
        {
            //Subgraph WhenStarted and TimeElapsed are stored globally.  If that is all that is required
            //then do not include the subgraph timings
            if (desiredStats.ordinality())
            {
                bool needGraphStats = false;
                ForEachItemIn(i, desiredStats)
                {
                    unsigned stat = desiredStats.item(i);
                    if ((stat != StWhenStarted) && (stat != StTimeElapsed))
                        needGraphStats = true;
                }
                if (!needGraphStats)
                {
                    sourceFlags &= ~(SSFsearchGraphStats);
                    if (matchOnly(SSTsubgraph))
                        sourceFlags &= SSFsearchGlobalStats;
                }
            }
        }
    }

    if (scopeFilter.compareDepth(1) < 0)
    {
        //If minimum match depth is > 1 then it will never match workflow
        sourceFlags &= ~(SSFsearchWorkflow);
    }

    //The xml graph is never updated, so do not check it if minVersion != 0
    if (minVersion != 0)
    {
        sourceFlags &= ~(SSFsearchGraph|SSFsearchWorkflow);
    }
}


bool WuScopeFilter::includeStatistic(StatisticKind kind) const
{
    if (!(properties & PTstatistics))
        return false;
    if ((desiredMeasure != SMeasureAll) && (queryMeasure(kind) != desiredMeasure))
        return false;
    if (desiredStats.empty())
        return true;
    return desiredStats.contains(kind);
}

bool WuScopeFilter::includeAttribute(WuAttr attr) const
{
    if (!(properties & PTattributes))
        return false;
    if (desiredAttrs.empty())
        return true;
    return desiredAttrs.contains(attr);
}

bool WuScopeFilter::includeHint(const char * kind) const
{
    if (!(properties & PThints))
        return false;
    if (desiredHints.empty())
        return true;
    return desiredHints.contains(kind);
}

bool WuScopeFilter::includeScope(const char * scope) const
{
    if (include.scopeTypes)
    {
        const char * tail = queryScopeTail(scope);
        StatsScopeId id(tail);
        if (!include.scopeTypes.contains(id.queryScopeType()))
            return false;
    }
    return true;
}

const static ScopeFilter nullScopeFilter {};
const ScopeFilter & WuScopeFilter::queryIterFilter() const
{
    if (preFilterScope)
        return scopeFilter;
    else
        return nullScopeFilter;
}

ScopeCompare WuScopeFilter::compareMatchScopes(const char * scope) const
{
    return scopeFilter.compare(scope);
}


StringBuffer & WuScopeFilter::describe(StringBuffer & out) const
{
    scopeFilter.describe(out);
    if (requiredStats.size())
    {
        out.append(",where[");
        bool first = false;
        for (const auto & stat : requiredStats)
        {
            if (!first)
                out.append(",");
            stat.describe(out);
            first = false;
        }
        out.append("]");
    }

    {
        StringBuffer sources;
        for (unsigned mask=1; mask; mask *= 2)
        {
            if (sourceFlags & mask)
            {
                const char * source = querySourceText((WuScopeSourceFlags)mask);
                if (source)
                    sources.append(",").append(source);
            }
        }
        if (sources)
            out.append(",source[").append(sources.str()+1).append("]");
    }

    if (include.nestedDepth != UINT_MAX)
        out.appendf(",nested[%u]", include.nestedDepth);

    if (include.scopeTypes)
    {
        out.append(",include[");
        ForEachItemIn(i, include.scopeTypes)
        {
            if (i)
                out.append(",");
            out.append(queryScopeTypeName((StatisticScopeType)include.scopeTypes.item(i)));
        }
        out.append("]");
    }

    {
        StringBuffer props;
        if (properties == PTnone)
            props.append(",none");
        else if (properties == PTall)
            props.append(",all");
        else
        {
            if (properties & PTstatistics)
                props.append(",stat");
            if (properties & PTattributes)
                props.append(",attr");
            if (properties & PThints)
                props.append(",hint");
            if (properties & PTscope)
                props.append(",scope");
            if (properties & PTnotes)
                props.append(",note");
        }
        out.append(",properties[").append(props.str()+1).append("]");
    }

    if (desiredStats)
    {
        out.append(",stat[");
        ForEachItemIn(i, desiredStats)
        {
            if (i)
                out.append(",");
            out.append(queryStatisticName((StatisticKind)desiredStats.item(i)));
        }
        out.append("]");
    }

    if (desiredAttrs)
    {
        out.append(",attr[");
        ForEachItemIn(i, desiredAttrs)
        {
            if (i)
                out.append(",");
            out.append(queryWuAttributeName((WuAttr)desiredAttrs.item(i)));
        }
        out.append("]");
    }

    if (desiredHints)
    {
        out.append(",hint[");
        ForEachItemIn(i, desiredHints)
        {
            if (i)
                out.append(",");
            out.append(desiredHints.item(i));
        }
        out.append("]");
    }

    if (desiredMeasure != SMeasureAll)
        out.append(",measure[").append(queryMeasureName(desiredMeasure)).append("]");

    if (minVersion != 0)
        out.appendf(",version(%" I64F "u)", minVersion);

    return out;
}
//--------------------------------------------------------------------------------------------------------------------

EnumMapping states[] = {
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

EnumMapping actions[] = {
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

EnumMapping priorityClasses[] = {
   { PriorityClassUnknown, "unknown" },
   { PriorityClassLow, "low" },
   { PriorityClassNormal, "normal" },
   { PriorityClassHigh, "high" },
   { PriorityClassSize, NULL },
};

const char * getWorkunitStateStr(WUState state)
{
    dbgassertex(state < WUStateSize);
    return states[state].str; // MORE - should be using getEnumText, or need to take steps to ensure values remain contiguous and in order.
}

void setEnum(IPropertyTree *p, const char *propname, int value, const EnumMapping *map)
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

static int getEnum(const IPropertyTree *p, const char *propname, const EnumMapping *map)
{
    return getEnum(p->queryProp(propname),map);
}

const char * getWorkunitActionStr(WUAction action)
{
    return getEnumText(action, actions);
}

WUAction getWorkunitAction(const char *actionStr)
{
    return (WUAction) getEnum(actionStr, actions);
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
        priority = (WUPriorityClass) getEnum(&p, "@priorityClass", priorityClasses);
        priorityLevel = calcPriorityValue(&p);
        wuscope.set(p.queryProp("@scope"));
        appvalues.loadBranch(&p,"Application");
        totalThorTime = (unsigned)nanoToMilli(extractTimeCollatable(p.queryProp("@totalThorTime"), false));
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
    virtual WUPriorityClass getPriority() const { return priority; }
    virtual const char *queryPriorityDesc() const { return getEnumText(priority, priorityClasses); }
    virtual int getPriorityLevel() const { return priorityLevel; }
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
    WUPriorityClass priority;
    int priorityLevel;
    bool _isProtected;
};

extern IConstWorkUnitInfo *createConstWorkUnitInfo(IPropertyTree &p)
{
    return new CLightweightWorkunitInfo(p);
}

class CDaliWuGraphStats : public CWuGraphStats
{
public:
    CDaliWuGraphStats(IRemoteConnection *_conn, StatisticCreatorType _creatorType, const char * _creator, unsigned _wfid, const char * _rootScope, unsigned _id)
        : CWuGraphStats(LINK(_conn->queryRoot()), _creatorType, _creator, _wfid, _rootScope, _id), conn(_conn)
    {
    }
protected:
    Owned<IRemoteConnection> conn;
};

CWorkUnitWatcher::CWorkUnitWatcher(IWorkUnitSubscriber *_subscriber, WUSubscribeOptions flags, const char *wuid) : subscriber(_subscriber)
{
    abortId = 0;
    stateId = 0;
    actionId = 0;
    assertex((flags & ~SubscribeOptionAbort) == 0);
    if (flags & SubscribeOptionAbort)
    {
        VStringBuffer xpath("/WorkUnitAborts/%s", wuid);
        abortId = querySDS().subscribe(xpath.str(), *this, false, true);
    }
}
CWorkUnitWatcher::~CWorkUnitWatcher()
{
    assertex(abortId==0 && stateId==0 && actionId==0);
}

void CWorkUnitWatcher::unsubscribe()
{
    CriticalBlock b(crit);
    if (abortId)
        querySDS().unsubscribe(abortId);
    if (stateId)
        querySDS().unsubscribe(stateId);
    if (actionId)
        querySDS().unsubscribe(actionId);
    abortId = 0;
    stateId = 0;
    actionId = 0;
}

void CWorkUnitWatcher::notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
{
    CriticalBlock b(crit);
    if (id==stateId)
        subscriber->notify(SubscribeOptionState, valueLen, valueData);
    else if (id==actionId)
        subscriber->notify(SubscribeOptionAction, valueLen, valueData);
    else if (id==abortId)
        subscriber->notify(SubscribeOptionAbort, valueLen, valueData);
}


class CDaliWorkUnitWatcher : public CWorkUnitWatcher
{
public:
    CDaliWorkUnitWatcher(IWorkUnitSubscriber *_subscriber, WUSubscribeOptions flags, const char *wuid)
    : CWorkUnitWatcher(_subscriber, (WUSubscribeOptions) (flags & SubscribeOptionAbort), wuid)
    {
        if (flags & SubscribeOptionState)
        {
            VStringBuffer xpath("/WorkUnits/%s/State", wuid);
            stateId = querySDS().subscribe(xpath.str(), *this);
        }
        if (flags & SubscribeOptionAction)
        {
            VStringBuffer xpath("/WorkUnits/%s/Action", wuid);
            actionId = querySDS().subscribe(xpath.str(), *this);
        }
    }
};

void CPersistedWorkUnit::subscribe(WUSubscribeOptions options)
{
    CriticalBlock block(crit);
    assertex(options==SubscribeOptionAbort);
    if (!abortWatcher)
    {
        abortWatcher.setown(new CWorkUnitWatcher(this, SubscribeOptionAbort, p->queryName()));
        abortDirty = true;
    }
}

void CPersistedWorkUnit::unsubscribe()
{
    CriticalBlock block(crit);
    if (abortWatcher)
    {
        abortWatcher->unsubscribe();
        abortWatcher.clear();
    }
}

bool CPersistedWorkUnit::aborting() const
{
    CriticalBlock block(crit);
    if (abortDirty)
    {
        StringBuffer apath;
        apath.append("/WorkUnitAborts/").append(p->queryName());
        Owned<IRemoteConnection> acon = querySDS().connect(apath.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT);
        if (acon)
            abortState = acon->queryRoot()->getPropInt(NULL) != 0;
        else
            abortState = false;
        abortDirty = false;
    }
    return abortState;
}

class CDaliWorkUnit : public CPersistedWorkUnit
{
public:
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
        Owned<IRemoteConnection> conn = getProgressConnection();
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
        Owned<IRemoteConnection> conn = getProgressConnection();
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
        Owned<IRemoteConnection> conn = getProgressConnection();
        if (conn)
        {
            IPTree *progress = conn->queryRoot()->queryPropTree(graphName);
            if (progress)
            {
                StringBuffer path;
                // NOTE - the node state info still uses the old graph layout, even when the stats are using the new...
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
        Owned<IRemoteConnection> conn = getProgressConnection();
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
        CriticalBlock block(crit);
        clearCached(true);
        connection->reload();
        progressConnection.clear();
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
                IERRLOG("Failed to release write lock on workunit: %s", E->errorMessage(s).str());
                throw;
            }
        }
    }
    virtual void setGraphState(const char *graphName, unsigned wfid, WUGraphState state) const
    {
        Owned<IRemoteConnection> conn = getWritableProgressConnection(graphName, wfid);
        conn->queryRoot()->setPropInt("@_state", state);
    }
    virtual void setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
    {
        CriticalBlock block(crit);
        progressConnection.clear();  // Make sure nothing is locking for read or we won't be able to lock for write
        VStringBuffer path("/GraphProgress/%s", queryWuid());
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        IPTree *progress = ensurePTree(conn->queryRoot(), graphName);
        // NOTE - the node state info still uses the old graph layout, even when the stats are using the new...
        path.clear().append("node[@id=\"").append(nodeId).append("\"]");
        IPropertyTree *node = progress->queryPropTree(path.str());
        if (!node)
        {
            node = progress->addPropTree("node");
            node->setPropInt64("@id", nodeId);
        }
        node->setPropInt("@_state", (unsigned)state);
        switch (state)
        {
            case WUGraphRunning:
            {
                IPropertyTree *running = conn->queryRoot()->setPropTree("Running");
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
    virtual IWUGraphStats *updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned _wfid, unsigned subgraph) const override
    {
        return new CDaliWuGraphStats(getWritableProgressConnection(graphName, _wfid), creatorType, creator, _wfid, graphName, subgraph);
    }

protected:
    IRemoteConnection *getProgressConnection() const
    {
        CriticalBlock block(crit);
        if (!progressConnection)
        {
            VStringBuffer path("/GraphProgress/%s", queryWuid());
            progressConnection.setown(querySDS().connect(path, myProcessSession(), 0, SDS_LOCK_TIMEOUT)); // Note - we don't lock. The writes are atomic.
        }
        return progressConnection.getLink();
    }
    IRemoteConnection *getWritableProgressConnection(const char *graphName, unsigned wfid) const
    {
        CriticalBlock block(crit);
        progressConnection.clear(); // Make sure subsequent reads from this workunit get the changes I am making
        VStringBuffer path("/GraphProgress/%s/%s", queryWuid(), graphName);
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        IPropertyTree * root = conn->queryRoot();
        assertex(wfid);

        if (!root->hasProp("@wfid"))
        {
            root->setPropInt("@wfid", wfid);
        }
        else
        {
            //Ideally the following code would check that the wfids are passed consistently.
            //However there is an obscure problem with out of line functions being called from multiple workflow
            //ids, and possibly library graphs.
            //Stats for library graphs should be nested below the library call activity
            //assertex(root->getPropInt("@wfid", 0) == wfid); // check that wfid is passed consistently
        }

        return conn.getClear();
    }
    IPropertyTree *getGraphProgressTree() const
    {
        Owned<IRemoteConnection> conn = getProgressConnection();
        if (conn)
        {
            Owned<IPropertyTree> tmp = createPTree("GraphProgress");
            mergePTree(tmp,conn->queryRoot());
            return tmp.getClear();
        }
        return NULL;
    }
    Owned<IRemoteConnection> connection;
    mutable Owned<IRemoteConnection> progressConnection;
};

class CLockedWorkUnit : implements ILocalWorkUnit, implements IExtendedWUInterface, public CInterface
{
public:
    Owned<CLocalWorkUnit> c;

    IMPLEMENT_IINTERFACE;
    CLockedWorkUnit(CLocalWorkUnit *_c) : c(_c) {}
    ~CLockedWorkUnit()
    {
        if (workUnitTraceLevel > 1)
            DBGLOG("Releasing locked workunit %s", queryWuid());
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
    virtual const char *queryPriorityDesc() const
            { return c->queryPriorityDesc(); }
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
    virtual IConstWUResult * getQueryResultByName(const char * name) const
            { return c->getQueryResultByName(name); }
    virtual unsigned getResultLimit() const
            { return c->getResultLimit(); }
    virtual IConstWUResultIterator & getResults() const
            { return c->getResults(); }
    virtual IStringVal & getScope(IStringVal & str) const
            { return c->getScope(str); }
    virtual IStringVal & getWorkunitDistributedAccessToken(IStringVal & str) const
            { return c->getWorkunitDistributedAccessToken(str); }
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
    virtual IConstWUScopeIterator & getScopeIterator(const WuScopeFilter & filter) const override
            { return c->getScopeIterator(filter); }
    virtual bool getStatistic(stat_type & value, const char * scope, StatisticKind kind) const override
            { return c->getStatistic(value, scope, kind); }
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
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool copyStats, bool all)
            { queryExtendedWU(c)->copyWorkUnit(cached, copyStats, all); }
    virtual IPropertyTree *queryPTree() const
            { return queryExtendedWU(c)->queryPTree(); }
    virtual IPropertyTree *getUnpackedTree(bool includeProgress) const
            { return queryExtendedWU(c)->getUnpackedTree(includeProgress); }
    virtual bool archiveWorkUnit(const char *base,bool del,bool deldll,bool deleteOwned,bool exportAssociatedFiles)
            { return queryExtendedWU(c)->archiveWorkUnit(base,del,deldll,deleteOwned,exportAssociatedFiles); }
    virtual unsigned queryFileUsage(const char *filename) const
            { return c->queryFileUsage(filename); }
    virtual IConstWUFileUsageIterator * getFieldUsage() const
            { return c->getFieldUsage(); }
    virtual bool getFieldUsageArray(StringArray & filenames, StringArray & columnnames, const char * clusterName) const
            { return c->getFieldUsageArray(filenames, columnnames, clusterName); }
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
    virtual void setGraphState(const char *graphName, unsigned wfid, WUGraphState state) const
            { c->setGraphState(graphName, wfid, state); }
    virtual void setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
            { c->setNodeState(graphName, nodeId, state); }
    virtual IWUGraphStats *updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned _wfid, unsigned subgraph) const override
            { return c->updateStats(graphName, creatorType, creator, _wfid, subgraph); }
    virtual void clearGraphProgress() const
            { c->clearGraphProgress(); }
    virtual IStringVal & getAbortBy(IStringVal & str) const
            { return c->getAbortBy(str); }
    virtual unsigned __int64 getAbortTimeStamp() const
            { return c->getAbortTimeStamp(); }


    virtual void clearExceptions(const char *source=nullptr)
            { c->clearExceptions(source); }
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
    virtual void setTracingValueInt64(const char * propname, __int64 value)
            { c->setTracingValueInt64(propname, value); }
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
    virtual void createGraph(const char * name, const char *label, WUGraphType type, IPropertyTree *xgmml, unsigned wfid)
            { c->createGraph(name, label, type, xgmml, wfid); }
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
    virtual void noteFieldUsage(IPropertyTree * usage)
            { c->noteFieldUsage(usage); }
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

class CLocalWUAssociated : implements IConstWUAssociatedFile, public CInterface
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
    virtual unsigned getMinActivityId() const;
    virtual unsigned getMaxActivityId() const;
};

class CLocalWUQuery : implements IWUQuery, public CInterface
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
    virtual bool hasArchive() const
    {
        return p->getPropBool("@hasArchive");
    }

    virtual void        setQueryType(WUQueryType qt);
    virtual void        setQueryText(const char *pstr);
    virtual void        setQueryName(const char *);
    virtual void        setQueryMainDefinition(const char * str);
    virtual void        addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc, unsigned minActivity, unsigned maxActivity);
    virtual void        removeAssociatedFiles();
    virtual void        removeAssociatedFile(WUFileType type, const char * name, const char * desc);
};

class CLocalWUWebServicesInfo : implements IWUWebServicesInfo, public CInterface
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

class CLocalWUResult : implements IWUResult, public CInterface
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
    virtual void        getResultWriteLocation(IStringVal & _graph, unsigned & _activityId) const;

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
    virtual void        setResultWriteLocation(const char * _graph, unsigned _activityId);

    virtual IPropertyTree *queryPTree() { return p; }
};

class CLocalWUPlugin : implements IWUPlugin, public CInterface
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

class CLocalWULibrary : implements IWULibrary, public CInterface
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWULibrary(IPropertyTree *p);

    virtual IStringVal & getName(IStringVal & str) const;
    virtual void setName(const char * str);
};

class CLocalWUException : implements IWUException, public CInterface
{
    Owned<IPropertyTree> p;

public:
    IMPLEMENT_IINTERFACE;
    CLocalWUException(IPropertyTree *p);

    virtual IStringVal& getExceptionSource(IStringVal &str) const override;
    virtual IStringVal& getExceptionMessage(IStringVal &str) const override;
    virtual unsigned    getExceptionCode() const override;
    virtual ErrorSeverity getSeverity() const override;
    virtual IStringVal & getTimeStamp(IStringVal & dt) const override;
    virtual IStringVal & getExceptionFileName(IStringVal & str) const override;
    virtual unsigned    getExceptionLineNo() const override;
    virtual unsigned    getExceptionColumn() const override;
    virtual unsigned    getActivityId() const override;
    virtual unsigned    getSequence() const override;
    virtual const char * queryScope() const override;
    virtual unsigned    getPriority() const override;
    virtual void        setExceptionSource(const char *str) override;
    virtual void        setExceptionMessage(const char *str) override;
    virtual void        setExceptionCode(unsigned code) override;
    virtual void        setSeverity(ErrorSeverity level) override;
    virtual void        setTimeStamp(const char * dt) override;
    virtual void        setExceptionFileName(const char *str) override;
    virtual void        setExceptionLineNo(unsigned r) override;
    virtual void        setExceptionColumn(unsigned c) override;
    virtual void        setActivityId(unsigned _id) override;
    virtual void        setScope(const char * _scope) override;
    virtual void        setPriority(unsigned _priority) override;
};

//==========================================================================================

extern WORKUNIT_API bool isSpecialResultSequence(unsigned sequence)
{
    switch ((int) sequence)
    {
    case ResultSequenceInternal:
    case ResultSequenceOnce:
    case ResultSequencePersist:
    case ResultSequenceStored:
        return true;
    default:
        assertex(sequence <= INT_MAX);
        if ((int) sequence >= LibraryBaseSequence)
            return true;
        return false;
    }
}

class CConstWUArrayIterator : implements IConstWorkUnitIterator, public CInterface
{
    unsigned curTreeNum;
    IArrayOf<IPropertyTree> trees;
    Owned<IConstWorkUnitInfo> cur;

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


class CLocalWUFieldUsage : public CInterface, implements IConstWUFieldUsage
{
    Owned<IPropertyTree> p;
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUFieldUsage(IPropertyTree& _p) { p.setown(&_p); }

    virtual const char * queryName() const { return p->queryProp("@name"); }
};

class CConstWUFieldUsageIterator : public CInterface, implements IConstWUFieldUsageIterator
{
public:
   IMPLEMENT_IINTERFACE;
   CConstWUFieldUsageIterator(IPropertyTreeIterator * tree) { iter.setown(tree); }
   bool                  first() override { return iter->first(); }
   bool                  isValid() override { return iter->isValid(); }
   bool                  next() override { return iter->next(); }
   IConstWUFieldUsage *  get() const override { return new CLocalWUFieldUsage(iter->get()); }
private:
   Owned<IPropertyTreeIterator> iter;
};

class CLocalWUFileUsage : public CInterface, implements IConstWUFileUsage
{
    Owned<IPropertyTree> p;
public:
    IMPLEMENT_IINTERFACE;
    CLocalWUFileUsage(IPropertyTree& _p) { p.setown(&_p); }

    virtual const char * queryName() const { return p->queryProp("@name"); }
    virtual const char * queryType() const { return p->queryProp("@type"); }
    virtual unsigned getNumFields() const { return p->getPropInt("@numFields"); }
    virtual unsigned getNumFieldsUsed() const { return p->getPropInt("@numFieldsUsed"); }
    virtual IConstWUFieldUsageIterator * getFields() const { return new CConstWUFieldUsageIterator(p->getElements("fields/field")); }
};

class CConstWUFileUsageIterator : public CInterface, implements IConstWUFileUsageIterator
{
public:
   IMPLEMENT_IINTERFACE;
   CConstWUFileUsageIterator(IPropertyTreeIterator * tree) { iter.setown(tree); }
   bool                 first() override { return iter->first(); }
   bool                 isValid() override { return iter->isValid(); }
   bool                 next() override { return iter->next(); }
   IConstWUFileUsage *  get() const override { return new CLocalWUFileUsage(iter->get()); }
private:
   Owned<IPropertyTreeIterator> iter;
};


//==========================================================================================

class CCachedJobNameIterator : implements IStringIterator, public CInterface
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

EnumMapping workunitSortFields[] =
{
   { WUSFuser, "@submitID" },
   { WUSFcluster, "@clusterName" },
   { WUSFjob, "@jobName" },
   { WUSFstate, "@state" },
   { WUSFpriority, "@priorityClass" },
   { WUSFprotected, "@protected" },
   { WUSFwuid, "@" },
   { WUSFecl, "Query/ShortText" },
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
   { WUSFfilewritten, "Files/File/@name" },
   { WUSFterm, NULL }
};

extern const char *queryFilterXPath(WUSortField field)
{
    return getEnumText(field, workunitSortFields);
}

EnumMapping querySortFields[] =
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

class CConstQuerySetQueryIterator : implements IConstQuerySetQueryIterator, public CInterface
{
    unsigned index;
    IArrayOf<IPropertyTree> trees;
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

class CConstWUIterator : implements IConstWorkUnitIterator, public CInterface
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
        SecAccessFlags perm;
        if (!perms)
        {
            perm = secuser.get() ? secmgr->authorizeWorkunitScope(*secuser, scopeName) : SecAccess_Unavailable;
            scopePermissions.setValue(scopeName, perm);
        }
        else
            perm = (SecAccessFlags)*perms;
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
    cw->setDistributedAccessToken(secuser ? secuser->getName() : "");//create and sign the workunit distributed access token
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
        DBGLOG("createWorkUnit created %s", wuid.str());
    IWorkUnit* ret = createNamedWorkUnit(wuid.str(), app, scope, secmgr, secuser);
    if (workUnitTraceLevel > 1)
        DBGLOG("createWorkUnit created %s", ret->queryWuid());
    addTimeStamp(ret, SSTglobal, NULL, StWhenCreated);
    return ret;
}

bool CWorkUnitFactory::deleteWorkUnitEx(const char * wuid, bool throwException, ISecManager *secmgr, ISecUser *secuser)
{
    if (workUnitTraceLevel > 1)
        DBGLOG("deleteWorkUnit %s", wuid);
    StringBuffer wuRoot;
    getXPath(wuRoot, wuid);
    Owned<CLocalWorkUnit> cw = _updateWorkUnit(wuid, secmgr, secuser);
    if (!checkWuSecAccess(*cw.get(), secmgr, secuser, SecAccess_Full, "delete", true, true))
        return false;

    if (throwException)
        cw->cleanupAndDelete(true, true);
    else
    {
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
    }
    removeWorkUnitFromAllQueues(wuid); //known active workunits wouldn't make it this far
    return true;
}

bool CWorkUnitFactory::deleteWorkUnit(const char * wuid, ISecManager *secmgr, ISecUser *secuser)
{
    return deleteWorkUnitEx(wuid, false, secmgr, secuser);
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
            DBGLOG("openWorkUnit %s invalid WUID", nullText(wuidStr.str()));

        return NULL;
    }

    if (workUnitTraceLevel > 1)
        DBGLOG("openWorkUnit %s", wuidStr.str());
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
            IERRLOG("openWorkUnit %s not found", wuidStr.str());
        return NULL;
    }
}

IWorkUnit* CWorkUnitFactory::updateWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser)
{
    if (workUnitTraceLevel > 1)
        DBGLOG("updateWorkUnit %s", wuid);
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
            IERRLOG("updateWorkUnit %s not found", wuid);
        return NULL;
    }
}

IPropertyTree * pruneBranch(IPropertyTree * from, char const * xpath)
{
    Owned<IPropertyTree> ret;
    IPropertyTree * branch = from->queryPropTree(xpath);
    if(branch)
    {
        ret.setown(createPTreeFromIPT(branch));
        from->removeTree(branch);
    }
    return ret.getClear();
}

bool CWorkUnitFactory::restoreWorkUnit(const char *base, const char *wuid, bool restoreAssociated)
{
    StringBuffer path(base);
    addPathSepChar(path).append(wuid).append(".xml");
    Owned<IPTree> pt = createPTreeFromXMLFile(path);
    if (!pt)
        return false;
    CDateTime dt;
    dt.setNow();
    StringBuffer dts;
    dt.getString(dts);
    pt->setProp("@restoredDate", dts.str());
    Owned<IPropertyTree> generatedDlls = pruneBranch(pt, "GeneratedDlls[1]");
    Owned<IPropertyTree> associatedFiles;
    IPropertyTree *srcAssociated = pt->queryPropTree("Query/Associated");
    if (srcAssociated)
        associatedFiles.setown(createPTreeFromIPT(srcAssociated));
    // The updating of the repo is implementation specific...
    if (!_restoreWorkUnit(pt.getClear(), wuid))
        return false;
    // now kludge back GeneratedDlls
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


int CWorkUnitFactory::setTracingLevel(int newLevel)
{
    if (newLevel)
        DBGLOG("Setting workunit trace level to %d", newLevel);
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

void CWorkUnitFactory::reportAbnormalTermination(const char *wuid, WUState &state, SessionId agent)
{
    WARNLOG("reportAbnormalTermination: session stopped unexpectedly: %" I64F "d state: %d", (__int64) agent, (int) state);
    bool isEcl = false;
    switch (state)
    {
        case WUStateAborting:
            state = WUStateAborted;
            break;
        case WUStateCompiling:
            isEcl = true;
            // drop into
        default:
            state = WUStateFailed;
    }
    Owned<IWorkUnit> wu = updateWorkUnit(wuid, NULL, NULL);
    wu->setState(state);
    Owned<IWUException> e = wu->createException();
    e->setExceptionCode(isEcl ? 1001 : 1000);
    e->setExceptionMessage(isEcl ? "EclCC terminated unexpectedly" : "Workunit terminated unexpectedly");
}

static CriticalSection deleteDllLock;
static IWorkQueueThread *deleteDllWorkQ = nullptr;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    CriticalBlock b(deleteDllLock);
    if (deleteDllWorkQ)
        ::Release(deleteDllWorkQ);
    deleteDllWorkQ = nullptr;
}
static void asyncRemoveDll(const char * name)
{
    CriticalBlock b(deleteDllLock);
    if (!deleteDllWorkQ)
        deleteDllWorkQ = createWorkQueueThread();
    deleteDllWorkQ->post(new asyncRemoveDllWorkItem(name));
}

static void asyncRemoveFile(const char * ip, const char * name)
{
    CriticalBlock b(deleteDllLock);
    if (!deleteDllWorkQ)
        deleteDllWorkQ = createWorkQueueThread();
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
    virtual bool initializeStore()
    {
        throwUnexpected(); // Used when loading a plugin factory - not applicable here
    }
    virtual IWorkUnitWatcher *getWatcher(IWorkUnitSubscriber *subscriber, WUSubscribeOptions options, const char *wuid) const
    {
        return new CDaliWorkUnitWatcher(subscriber, options, wuid);
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
        conn->queryRoot()->setProp("@totalThorTime", "");
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
    virtual bool _restoreWorkUnit(IPTree *_pt, const char *wuid)
    {
        Owned<IPTree> pt(_pt);
        Owned<IPropertyTree> gprogress = pruneBranch(pt, "GraphProgress[1]");
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        Owned<IRemoteConnection> conn = sdsManager->connect(wuRoot.str(), myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        if (!conn)
        {
            ERRLOG("restoreWorkUnit could not create to %s", wuRoot.str());
            return false;
        }
        IPropertyTree *root = conn->queryRoot();
        if (root->hasChildren())
        {
            ERRLOG("restoreWorkUnit WUID %s already exists", wuid);
            return false;
        }
        root->setPropTree(NULL, pt.getClear());
        conn.clear();

        // now kludge back GraphProgress
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
        return true;
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

    /**
     * Add a filter to an xpath query, with the appropriate filter flags
     */
    static void appendFilterToQueryString(StringBuffer& query, int flags, const char* name, const char* value)
    {
        query.append('[').append(name).append('=');
        if (flags & WUSFnocase)
            query.append('?');
        if (flags & WUSFwild)
            query.append('~');
        query.append('"').append(value).append("\"]");
    };

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
        class CQueryOrFilter : public CInterface
        {
            unsigned flags;
            StringAttr name;
            StringArray values;
        public:
            IMPLEMENT_IINTERFACE;
            CQueryOrFilter(unsigned _flags, const char *_name, const char *_value)
            : flags(_flags), name(_name)
            {
                values.appendListUniq(_value, "|");
            };

            const char* queryName() { return name.get(); };
            unsigned querySearchFlags() { return flags; };
            const StringArray& queryValues() const { return values; };
        };
        class CMultiPTreeIterator : public CInterfaceOf<IPropertyTreeIterator>
        {
        public:
            virtual bool first() override
            {
                curSource = 0;
                while (sources.isItem(curSource))
                {
                    if (sources.item(curSource).first())
                        return true;
                    curSource++;
                }
                return false;
            }
            virtual bool next() override
            {
                if (sources.isItem(curSource))
                {
                    if (sources.item(curSource).next())
                        return true;
                    curSource++;
                    while (sources.isItem(curSource))
                    {
                        if (sources.item(curSource).first())
                            return true;
                        curSource++;
                    }
                }
                return false;
            }
            virtual bool isValid() override
            {
                return sources.isItem(curSource);
            }
            virtual IPropertyTree & query() override
            {
                return sources.item(curSource).query();
            }
            void addSource(IPropertyTreeIterator &source)
            {
                sources.append(source);
            }
        private:
            IArrayOf<IPropertyTreeIterator> sources;
            unsigned curSource = 0;
        };
        class CWorkUnitsPager : public CSimpleInterface, implements IElementsPager
        {
            StringAttr xPath;
            StringAttr sortOrder;
            StringAttr nameFilterLo;
            StringAttr nameFilterHi;
            StringArray unknownAttributes;
            Owned<CQueryOrFilter> orFilter;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWorkUnitsPager(const char* _xPath, CQueryOrFilter* _orFilter, const char *_sortOrder, const char* _nameFilterLo, const char* _nameFilterHi, StringArray& _unknownAttributes)
                : xPath(_xPath), orFilter(_orFilter), sortOrder(_sortOrder), nameFilterLo(_nameFilterLo), nameFilterHi(_nameFilterHi)
            {
                ForEachItemIn(x, _unknownAttributes)
                    unknownAttributes.append(_unknownAttributes.item(x));
            }
            virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
            {
                Owned<IRemoteConnection> conn = querySDS().connect("WorkUnits", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
                if (!conn)
                    return NULL;
                Owned<IPropertyTreeIterator> iter;
                if (!orFilter)
                {
                    iter.setown(conn->getElements(xPath.get()));
                }
                else
                {
                    Owned <CMultiPTreeIterator> multi = new CMultiPTreeIterator;
                    bool added = false;
                    const char* fieldName = orFilter->queryName();
                    unsigned flags = orFilter->querySearchFlags();
                    const StringArray& values = orFilter->queryValues();
                    ForEachItemIn(i, values)
                    {
                        StringBuffer path(xPath.get());
                        const char* value = values.item(i);
                        if (!isEmptyString(value))
                        {
                            appendFilterToQueryString(path, flags, fieldName, value);
                            IPropertyTreeIterator *itr = conn->getElements(path.str());
                            if (itr)
                            {
                                multi->addSource(*itr);
                                added = true;
                            }
                        }
                    }
                    if (added)
                        iter.setown(multi.getClear());
                }
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
        Owned<CQueryOrFilter> orFilter;
        Owned<ISortedElementsTreeFilter> sc = new CScopeChecker(secmgr,secuser);
        StringBuffer query;
        StringBuffer so;
        StringAttr namefilter("*");
        StringAttr namefilterlo;
        StringAttr namefilterhi;
        StringArray unknownAttributes;
        if (filters)
        {
            const char *fv = (const char *) filterbuf;
            for (unsigned i=0;filters[i]!=WUSFterm;i++)
            {
                assertex(fv);
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
                    const char *app = fv;
                    fv = fv + strlen(fv)+1;
                    query.append("[Application/").append(app);
                    if (*fv)
                        query.append("=?~\"").append(fv).append('\"');
                    query.append("]");
                }
                else if (subfmt==WUSFtotalthortime)
                {
                    query.append("[@totalThorTime>=\"");
                    formatTimeCollatable(query, milliToNano(atoi(fv)), false);
                    query.append("\"]");
                }
                else if (!*fv)
                {
                    unknownAttributes.append(getEnumText(subfmt,workunitSortFields));
                    if (subfmt==WUSFtotalthortime)
                        sortorder = (WUSortField) (sortorder & ~WUSFnumeric);
                }
                else
                {
                    const char* fieldName = getEnumText(subfmt,workunitSortFields);
                    if (!strchr(fv, '|'))
                        appendFilterToQueryString(query, fmt, fieldName, fv);
                    else if (orFilter)
                        throw MakeStringException(WUERR_InvalidUserInput, "Multiple OR filters not allowed");
                    else
                    {
                        if (!strieq(fieldName, getEnumText(WUSFstate,workunitSortFields)) &&
                            !strieq(fieldName, getEnumText(WUSFuser,workunitSortFields)) &&
                            !strieq(fieldName, getEnumText(WUSFcluster,workunitSortFields)))
                            throw MakeStringException(WUERR_InvalidUserInput, "OR filters not allowed for %s", fieldName);
                        orFilter.setown(new CQueryOrFilter(fmt, fieldName, fv));
                    }
                }
                fv = fv + strlen(fv)+1;
            }
        }
        if ((sortorder&0xff)==WUSFtotalthortime)
            sortorder = (WUSortField) (sortorder & ~WUSFnumeric);
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
        Owned<IElementsPager> elementsPager = new CWorkUnitsPager(query.str(), orFilter.getClear(), so.length()?so.str():NULL, namefilterlo.get(), namefilterhi.get(), unknownAttributes);
        Owned<IRemoteConnection> conn=getElementsPaged(elementsPager,startoffset,maxnum,secmgr?sc:NULL,"",cachehint,results,total,NULL);
        return new CConstWUArrayIterator(results);
    }

    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, std::list<WUState> expectedStates)
    {
        WUState ret = WUStateUnknown;
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        Owned<IRemoteConnection> conn = sdsManager->connect(wuRoot.str(), session, 0, SDS_LOCK_TIMEOUT);
        if (timeout == 0) //no need to subscribe
        {
            ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
            auto it = std::find(expectedStates.begin(), expectedStates.end(), ret);
            if (it != expectedStates.end())
                return ret;
            switch (ret)
            {
            case WUStateCompiled:
            case WUStateUploadingFiles:
                if (!compiled)
                    break;
            //fall through
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateAborted:
                return ret;
            default:
                break;
            }
            return WUStateUnknown;
        }

        Owned<WorkUnitWaiter> waiter = new WorkUnitWaiter(wuid, SubscribeOptionState);
        LocalIAbortHandler abortHandler(*waiter);
        if (conn)
        {
            SessionId agent = -1;
            bool agentSessionStopped = false;
            unsigned start = msTick();
            for (;;)
            {
                ret = (WUState) getEnum(conn->queryRoot(), "@state", states);
                auto it = std::find(expectedStates.begin(), expectedStates.end(), ret);
                if (it != expectedStates.end())
                    return ret;
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
                    return ret;
                case WUStateWait:
                    break;
                case WUStateCompiling:
                case WUStateRunning:
                case WUStateDebugPaused:
                case WUStateDebugRunning:
                case WUStateBlocked:
                case WUStateAborting:
                    if (agentSessionStopped)
                    {
                        reportAbnormalTermination(wuid, ret, agent);
                        return ret;
                    }
                    if (queryDaliServerVersion().compare("2.1")>=0)
                    {
                        agent = conn->queryRoot()->getPropInt64("@agentSession", -1);
                        if((agent>0) && querySessionManager().sessionStopped(agent, 0))
                        {
                            agentSessionStopped = true;
                            conn->reload();
                            continue;
                        }
                    }
                    break;
                }
                agentSessionStopped = false; // reset for state changes such as WUStateWait then WUStateRunning again
                unsigned waited = msTick() - start;
                if (timeout==-1 || waited + 20000 < timeout)
                {
                    waiter->wait(20000);  // recheck state every 20 seconds, in case eclagent has crashed.
                    if (waiter->isAborted())
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
        return ret;
    }

    virtual WUAction waitForWorkUnitAction(const char * wuid, WUAction original)
    {
        Owned<WorkUnitWaiter> waiter = new WorkUnitWaiter(wuid, SubscribeOptionAction);
        LocalIAbortHandler abortHandler(*waiter);
        WUAction ret = WUActionUnknown;
        StringBuffer wuRoot;
        getXPath(wuRoot, wuid);
        Owned<IRemoteConnection> conn = sdsManager->connect(wuRoot.str(), session, 0, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            unsigned start = msTick();
            for (;;)
            {
                ret = (WUAction) getEnum(conn->queryRoot(), "Action", actions);
                if (ret != original)
                    break;
                unsigned waited = msTick() - start;
                waiter->wait(20000);  // recheck state every 20 seconds even if no timeout, in case eclagent has crashed.
                if (waiter->isAborted())
                {
                    ret = WUActionUnknown;  // MORE - throw an exception?
                    break;
                }
                conn->reload();
            }
        }
        waiter->unsubscribe();
        return ret;
    }

protected:
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
            Owned<IRemoteConnection> env = querySDS().connect("/Environment", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
            IPropertyTree *pluginInfo = NULL;
            if (env)
            {
                SocketEndpoint targetDali = queryCoven().queryGroup().queryNode(0).endpoint();
                IPropertyTree *daliInfo = findDaliProcess(env->queryRoot(), targetDali);
                if (daliInfo)
                {
                    const char *daliName = daliInfo->queryProp("@name");
                    if (daliName)
                    {
                        VStringBuffer xpath("Software/DaliServerPlugin[@type='WorkunitServer'][@daliServers='%s']", daliName);
                        pluginInfo = env->queryRoot()->queryPropTree(xpath);
                    }
                    if (!pluginInfo)
                        pluginInfo = daliInfo->queryPropTree("Plugin[@type='WorkunitServer']");  // Compatibility with early betas of 6.0 ...
                }
            }
            if (pluginInfo && !forceDali)
                factory.setown( (IWorkUnitFactory *) loadPlugin(pluginInfo));
            else
                factory.setown(new CDaliWorkUnitFactory());
        }
    }
    return factory.getLink();
}

extern WORKUNIT_API IWorkUnitFactory * getDaliWorkUnitFactory()
{
    if (!factory)
    {
        CriticalBlock b(factoryCrit);
        if (!factory)   // NOTE - this "double test" paradigm is not guaranteed threadsafe on modern systems/compilers - I think in this instance that is harmless even in the (extremely) unlikely event that it resulted in the setown being called twice.
            factory.setown(new CDaliWorkUnitFactory());
    }
    return factory.getLink();
}

// A SecureWorkUnitFactory allows the security params to be supplied once to the factory rather than being supplied to each call.
// They can still be supplied if you want...

class CSecureWorkUnitFactory : implements IWorkUnitFactory, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CSecureWorkUnitFactory(IWorkUnitFactory *_baseFactory, ISecManager *_secMgr, ISecUser *_secUser)
        : baseFactory(_baseFactory), defaultSecMgr(_secMgr), defaultSecUser(_secUser)
    {
    }
    virtual bool initializeStore()
    {
        throwUnexpected(); // Used when loading a plugin factory - not applicable here
    }
    virtual IWorkUnitWatcher *getWatcher(IWorkUnitSubscriber *subscriber, WUSubscribeOptions options, const char *wuid) const
    {
        return baseFactory->getWatcher(subscriber, options, wuid);
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
    virtual bool deleteWorkUnitEx(const char * wuid, bool throwException, ISecManager *secMgr, ISecUser *secUser)
    {
        if (!secMgr) secMgr = defaultSecMgr.get();
        if (!secUser) secUser = defaultSecUser.get();
        return baseFactory->deleteWorkUnitEx(wuid, throwException, secMgr, secUser);
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
    virtual bool restoreWorkUnit(const char *base, const char *wuid, bool restoreAssociated)
    {
        return baseFactory->restoreWorkUnit(base, wuid, restoreAssociated);
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
    virtual WUState waitForWorkUnit(const char * wuid, unsigned timeout, bool compiled, std::list<WUState> expectedStates)
    {
        return baseFactory->waitForWorkUnit(wuid, timeout, compiled, expectedStates);
    }
    virtual WUAction waitForWorkUnitAction(const char * wuid, WUAction original)
    {
        return baseFactory->waitForWorkUnitAction(wuid, original);
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

class CStringPTreeIterator : implements IStringIterator, public CInterface
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

class CStringPTreeTagIterator : implements IStringIterator, public CInterface
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

class CStringPTreeAttrIterator : implements IStringIterator, public CInterface
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
    if (extra.length())
    {
        const char *bufStart = buf.str();
        const char *bufPtr = bufStart+buf.length();
        while (bufStart != bufPtr)
        {
            --bufPtr;
            if (!isspace(*bufPtr))
                break;
        }
        assertex('>' == *bufPtr);
        size_t l = strlen(wuid);
        assertex((size_t)(bufPtr-bufStart) > l+2); // e.g. at least </W20171111-111111>
        bufPtr -= l+2; // skip back over </wuid
        assertex(0 == memcmp(bufPtr, "</", 2) );
        assertex(0 == memcmp(bufPtr+2, wuid, l));
        buf.insert(bufPtr-bufStart, extra);
    }
    return (fileio->write(0,buf.length(),buf.str()) == buf.length());
}

bool CLocalWorkUnit::archiveWorkUnit(const char *base, bool del, bool ignoredllerrors, bool deleteOwned, bool exportAssociatedFiles)
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
    Owned<IPTree> graphProgress = getGraphProgressTree();
    if (graphProgress)
    {
        toXML(graphProgress,extraWorkUnitXML,1,XML_Format);
        graphProgress.clear();
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
                if (exportAssociatedFiles)
                {
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
                            //copy failed, so don't delete the registered dll files
                            deleteExclusions.append(name.str());
                        }
                        else
                            throw exception.getClear();
                    }
                }
                // Record Associated path to restore back to
                StringBuffer restorePath;
                curRfn.getRemotePath(restorePath);
                generatedDllBranch->setProp("@location", restorePath.str());
                generatedDlls->addPropTree("GeneratedDll", generatedDllBranch.getClear());
            }
            else if (exportAssociatedFiles) // no generated dll entry
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

void CLocalWorkUnit::loadXML(const char *xml)
{
    CriticalBlock block(crit);
    clearCached(true);
    assertex(xml);
    p.setown(createPTreeFromXMLString(xml,ipt_lowmem));
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
            IERRLOG("Failed to get write lock on workunit: %s", E->errorMessage(s).str());
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
    return (unsigned)nanoToMilli(extractTimeCollatable(p->queryProp("@totalThorTime"), false));
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

IStringVal & CLocalWorkUnit::getWorkunitDistributedAccessToken(IStringVal & datoken) const
{
    CriticalBlock block(crit);
    datoken.set(p->queryProp("@distributedAccessToken"));
    return datoken;
}

bool CLocalWorkUnit::setDistributedAccessToken(const char * user)
{
    CriticalBlock block(crit);

    //If this format changes, you must update the isWorkunitDAToken() and extractFromWorkunitDAToken() methods
    VStringBuffer datoken("HPCC[u=%s,w=%s]", user, queryWuid());

    IDigitalSignatureManager * pDSM = queryDigitalSignatureManagerInstanceFromEnv();
    if (pDSM && pDSM->isDigiSignerConfigured())
    {
        datoken.append(pDSM->queryKeyName()).append(';');

        StringBuffer b64Signature;
        if (pDSM->digiSign(b64Signature, datoken))
        {
            datoken.append(b64Signature.str());
        }
        else
        {
            ERRLOG("Cannot create workunit Distributed Access Token, digisign failed");
            return false;
        }
    }
    else
    {
        WARNLOG("Cannot sign Distributed Access Token, digisign signing not configured");
        datoken.append(";");
    }
    p->setProp("@distributedAccessToken", datoken);
    return true;
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
    SecAccessFlags perm = SecAccess_Full;
    const char *scopename = p->queryProp("@scope");
    if (scopename&&*scopename) {
        if (!user)
            user = queryUserDescriptor();
        perm = querySessionManager().getPermissionsLDAP("workunit",scopename,user,auditflags);
        if (perm<0) {
            if (perm == SecAccess_Unavailable)
                perm = SecAccess_Full;
            else 
                perm = SecAccess_None;
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

const char *CLocalWorkUnit::queryPriorityDesc() const
{
    return getEnumText(getPriority(), priorityClasses);
}

void CLocalWorkUnit::setState(WUState value) 
{
    if (value==WUStateAborted || value==WUStatePaused || value==WUStateCompleted || value==WUStateFailed || value==WUStateSubmitted || value==WUStateWait)
    {
        if (factory)
            factory->clearAborting(queryWuid());
    }
    CriticalBlock block(crit);
    setEnum(p, "@state", value, states);  // For historical reasons, we use state to store the state
    setEnum(p, "State", value, states);   // But we can only subscribe to elements, not attributes
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

bool CLocalWorkUnit::hasApplicationValue(const char *app, const char *propname) const
{
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);

    CriticalBlock block(crit);
    return p->hasProp(prop);
}

IStringVal& CLocalWorkUnit::getApplicationValue(const char *app, const char *propname, IStringVal &str) const
{
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);

    CriticalBlock block(crit);
    str.set(p->queryProp(prop.str())); 
    return str;
}

int CLocalWorkUnit::getApplicationValueInt(const char *app, const char *propname, int defVal) const
{
    StringBuffer prop("Application/");
    prop.append(app).append('/').append(propname);

    CriticalBlock block(crit);
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

class NullIStringIterator : implements IStringIterator, public CInterface
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

bool extractFromWorkunitDAToken(const char * distributedAccessToken, StringBuffer * wuid, StringBuffer * user, StringBuffer * privKey)
{
    if (!isWorkunitDAToken(distributedAccessToken))
    {
        DBGLOG("Not a valid workunit distributed access token");
        return false;
    }

    //Extract the string between [ and ]
    const char * pEndBracket = strchr(distributedAccessToken + 5, ']');
    StringBuffer tokenValues;
    tokenValues.append(pEndBracket - distributedAccessToken - 5, distributedAccessToken + 5);

    StringArray nameValues;
    nameValues.appendList(tokenValues.str(), ",",true);

    if (user || wuid)
    {
        for (int x = 0; x < nameValues.ordinality(); x++)
        {
            if (user && 0==strncmp(nameValues[x],"u=",2))//Extract user
                user->append(nameValues[x] + 2);
            if (wuid && 0==strncmp(nameValues[x],"w=",2))//Extract wuid
                wuid->append(nameValues[x] + 2);
        }
    }

    if (privKey)
    {
        const char * finger = pEndBracket;
        ++finger;
        while (*finger && *finger != ';')
            privKey->append(1, finger++);
    }
    return true;
}

//Verifies the given signed workunit distributed access token was created
//and signed by the given wuid and user, and that the workunit is still active
//Returns:
//   0 : Success, token is valid and workunit is active
//   1 : Signature does not verify (wuid/username don't match, or signature does not verify)
//   2 : Workunit not active
//   Throws if unable to open workunit
wuTokenStates verifyWorkunitDAToken(const char * ctxUser, const char * daToken)
{
    if (isEmptyString(daToken))
    {
        ERRLOG("verifyWorkunitDAToken : Token must be provided");
        return wuTokenInvalid;
    }

    StringBuffer tokWuid;
    StringBuffer tokUser;
    if (!extractFromWorkunitDAToken(daToken, &tokWuid, &tokUser, nullptr))//get the wuid and user
    {
        //Not a valid workunit distributed access token
        return wuTokenInvalid;
    }

    //Validate signature
    IDigitalSignatureManager * pDSM = queryDigitalSignatureManagerInstanceFromEnv();
    if (pDSM && pDSM->isDigiVerifierConfigured())
    {
        const char * finger;
        StringBuffer token;//receives copy of everything up until signature
        for (finger = daToken; *finger && *finger != ';'; finger++)
            token.append(1, finger);
        token.append(1, finger);//append ;

        StringBuffer sig(++finger);
        if (!pDSM->digiVerify(sig, token))
        {
            ERRLOG("verifyWorkunitDAToken : workunit distributed access token does not verify");
            return wuTokenInvalid;
        }
    }

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(tokWuid.str());
    if(!cw)
    {
        throw MakeStringException(WUERR_WorkunitAccessDenied,"verifyWorkunitDAToken : Cannot open workunit %s",tokWuid.str());
    }

    //Verify user matches
    bool wuUserExist = !isEmptyString(cw->queryUser());
    bool tokUserExist = !isEmptyString(tokUser.str());
    bool ctxUserExist = !isEmptyString(ctxUser);
    if (wuUserExist && tokUserExist)
    {
        //if both users are found, they must match
        if (!streq(tokUser.str(), cw->queryUser()))
        {
            ERRLOG("verifyWorkunitDAToken : Token user (%s) does not match WU user (%s)", tokUser.str(), cw->queryUser());
            return wuTokenInvalid;//Possible Internal error
        }
        else if (ctxUserExist && !streq(tokUser.str(), ctxUser))//ctxUser will be empty if security not enabled
        {
            ERRLOG("verifyWorkunitDAToken : Token user (%s) does not match Context user (%s)", cw->queryUser(), ctxUser);
            return wuTokenInvalid;
        }
    }
    else if (!wuUserExist && !tokUserExist)//both users will be empty if no security enabled
    {
        if (ctxUserExist)
        {
            ERRLOG("verifyWorkunitDAToken : Security enabled but WU user and Token user not specified");
            return wuTokenInvalid;
        }
        //both users empty and no context user means if no security enabled
    }
    else
    {
        //one user found, but not the other, treat as an error
        ERRLOG("verifyWorkunitDAToken : WU user %s and Token user %s must be provided", wuUserExist ? cw->queryUser() : "(NULL)", tokUserExist ? tokUser.str() : "(NULL)");
        return wuTokenInvalid;
    }

    // no need to compare tokWuid with workunit wuid, because it will always match

    bool wuActive;
    switch (cw->getState())
    {
    case WUStateRunning:
    case WUStateDebugRunning:
    case WUStateBlocked:
    case WUStateAborting:
    case WUStateUploadingFiles:
#ifdef _DEBUG
        DBGLOG("verifyWorkunitDAToken : Workunit token validated for %s %s, state is '%s'", cw->queryWuid(), cw->queryUser(), getWorkunitStateStr(cw->getState()));
#endif
        wuActive = true;
        break;
    default:
        ERRLOG("verifyWorkunitDAToken : Workunit %s not active, state is '%s'", cw->queryWuid(), getWorkunitStateStr(cw->getState()));
        wuActive = false;
        break;
    }

    return wuActive ? wuTokenValid : wuTokenWorkunitInactive;
}

bool CLocalWorkUnit::resolveFilePrefix(StringBuffer & prefix, const char * queue) const
{
    if (hasApplicationValue("prefix", queue))
    {
        getApplicationValue("prefix", queue, StringBufferAdaptor(prefix));
        return true;
    }

#ifndef _CONTAINERIZED
    Owned<IConstWUClusterInfo> ci = getTargetClusterInfo(queue);
    if (ci)
    {
        ci->getScope(StringBufferAdaptor(prefix));
        return true;
    }
#endif
    return false;
}

IStringVal& CLocalWorkUnit::getScope(IStringVal &str) const 
{
    StringBuffer prefix;
    CriticalBlock block(crit);
    if (p->hasProp("Debug/ForceScope"))
    {
        prefix.append(p->queryProp("Debug/ForceScope")).toLowerCase();
    }
    else
    {
        resolveFilePrefix(prefix, p->queryProp("@clusterName"));
    }
    str.set(prefix.str());
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
        userDesc.setown(createUserDescriptor());
        SCMStringBuffer token;
        getWorkunitDistributedAccessToken(token);
        userDesc->set(queryUser(), token.str());//use token as password
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

const static EnumMapping warningSeverityMap[] =
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
        mapping = onWarnings->addPropTree("OnWarning");
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

void CLocalWorkUnit::copyWorkUnit(IConstWorkUnit *cached, bool copyStats, bool all)
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
    pt = fromP->getBranch("Application/prefix");
    if (pt)
    {
        ensurePTree(p, "Application");
        p->setPropTree("Application/prefix", pt);
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
    if (copyStats)
    {
        // Merge timing info from both branches
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
    setProp(p, fromP, "@eventScheduledCount");

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
    p->setProp("@totalThorTime", fromP->queryProp("@totalThorTime"));
    p->setProp("@hash", fromP->queryProp("@hash"));
    p->setPropBool("@cloneable", true);
    p->setPropBool("@isClone", true);
    resetWorkflow();  // the source Workflow section may have had some parts already executed...
    Owned<IPropertyTreeIterator> results = p->getElements("Results/Result");
    ForEach(*results)
    {
        CLocalWUResult result(LINK(&results->query()));
        result.setResultStatus(ResultStatusUndefined);
    }

    copyTree(p, fromP, "usedsources"); // field usage
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
        node = node->addPropTree(instance);
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

void CLocalWorkUnit::setTracingValueInt64(const char *propname, __int64 value)
{
    CriticalBlock block(crit);
    VStringBuffer prop("Tracing/%s", propname);
    p->setPropInt64(prop.str(), value);
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

StringBuffer &formatGraphTimerScope(StringBuffer &str, unsigned wfid, const char *graphName, unsigned subGraphNum, unsigned __int64 subId)
{
    if (wfid)
        str.append(WorkflowScopePrefix).append(wfid).append(":");
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

    graphNum = atoi(scope + strlen(GraphScopePrefix));
    subGraphId = 0;

    const char * colon = strchr(scope, ':');
    if (!colon)
    {
        graphName.set(scope);
        return true;
    }

    const char * subgraph = colon+1;
    graphName.set(scope, (size32_t)(colon - scope));
    if (MATCHES_CONST_PREFIX(subgraph, SubGraphScopePrefix))
        subGraphId = atoi(subgraph+strlen(SubGraphScopePrefix));
    return true;
}


void CLocalWorkUnit::setStatistic(StatisticCreatorType creatorType, const char * creator, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * optDescription, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, StatsMergeAction mergeAction)
{
    if (!scope) scope = GLOBAL_SCOPE;

    const char * kindName = queryStatisticName(kind);
    StatisticMeasure measure = queryMeasure(kind);

    //creator. scope and name must all be present, and must not contain semi colons.
    assertex(creator && scope);

    CriticalBlock block(crit);
    IPropertyTree * stats = p->queryPropTree("Statistics");
    if (!stats)
        stats = p->addPropTree("Statistics");

    IPropertyTree * statTree = NULL;
    if (mergeAction != StatsMergeAppend)
    {
        StringBuffer xpath;
        xpath.append("Statistic[@creator='").append(creator).append("'][@scope='").append(scope).append("'][@kind='").append(kindName).append("']");
        statTree = stats->queryPropTree(xpath.str());
    }

    if (!statTree)
    {
        statTree = stats->addPropTree("Statistic");
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

    //Whenever a graph time is updated recalculate the total time spent in thor, and save it
    if ((scopeType == SSTgraph) && (kind == StTimeElapsed))
    {
        _loadStatistics();
        stat_type totalTime = 0;
        ForEachItemIn(i, statistics)
        {
            IConstWUStatistic & cur = statistics.item(i);
            if ((cur.getScopeType() == SSTgraph) && (cur.getKind() == StTimeElapsed))
                totalTime += cur.getValue();
        }
        StringBuffer t;
        formatTimeCollatable(t, totalTime, false);
        p->setProp("@totalThorTime", t);
    }
}

void CLocalWorkUnit::_loadStatistics() const
{
    statistics.load(p,"Statistics/*");
}


bool CLocalWorkUnit::getStatistic(stat_type & value, const char * scope, StatisticKind kind) const
{
    //MORE: Optimize this....
    WuScopeFilter filter;
    filter.addScope(scope).setIncludeNesting(0).addRequiredStat(kind).addOutputStatistic(kind).finishedFilter();
    Owned<IConstWUScopeIterator> stats = &getScopeIterator(filter);
    if (stats->first())
        return stats->getStat(kind, value);
    return false;
}

IConstWUScopeIterator & CLocalWorkUnit::getScopeIterator(const WuScopeFilter & filter) const
{
    assertex(filter.isOptimized());
    WuScopeSourceFlags sources = filter.sourceFlags;

    Owned<CompoundStatisticsScopeIterator> compoundIter = new CompoundStatisticsScopeIterator(filter);
    if (sources & SSFsearchGlobalStats)
    {
        {
            CriticalBlock block(crit);
            statistics.loadBranch(p,"Statistics");
        }

        Owned<IConstWUScopeIterator> localStats(new WorkUnitStatisticsScopeIterator(statistics, filter.queryIterFilter()));
        compoundIter->addIter(localStats);
    }

    if (sources & SSFsearchGraphStats)
    {
        const char * wuid = p->queryName();
        Owned<IConstWUScopeIterator> scopeIter(new CConstGraphProgressScopeIterator(wuid, filter.queryIterFilter(), filter.minVersion));
        compoundIter->addIter(scopeIter);
    }

    if (sources & SSFsearchGraph)
    {
        Owned<IConstWUScopeIterator> graphIter(new GraphScopeIterator(this, filter.queryIterFilter()));
        compoundIter->addIter(graphIter);
    }

    if (sources & SSFsearchWorkflow)
    {
        Owned<IConstWorkflowItemIterator> iter = getWorkflowItems();
        if (iter)
        {
            Owned<IConstWUScopeIterator> workflowIter(new WorkflowStatisticsScopeIterator(iter));
            compoundIter->addIter(workflowIter);
        }
    }
    if (sources & SSFsearchExceptions)
    {
        Owned<IConstWUScopeIterator> notesIter(new NotesIterator(this,filter.queryIterFilter()));
        compoundIter->addIter(notesIter);
    }


    return *compoundIter.getClear();
}

IWUPlugin* CLocalWorkUnit::updatePluginByName(const char *qname)
{
    CriticalBlock block(crit);
    IConstWUPlugin *existing = getPluginByName(qname);
    if (existing)
        return (IWUPlugin *) existing;
    if (!plugins.length())
        p->addPropTree("Plugins");
    IPropertyTree *pl = p->queryPropTree("Plugins");
    IPropertyTree *s = pl->addPropTree("Plugin");
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
        p->addPropTree("Libraries");
    IPropertyTree *pl = p->queryPropTree("Libraries");
    IPropertyTree *s = pl->addPropTree("Library");
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

void CLocalWorkUnit::clearExceptions(const char *source)
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadExceptions();
    ForEachItemInRev(idx, exceptions)
    {
        IWUException &e = exceptions.item(idx);
        SCMStringBuffer s;
        e.getExceptionSource(s);
        if (source)
        {
            if (!strieq(s.s, source))
                continue;
        }
        else
        {
            if (strieq(s.s, "eclcc") || strieq(s.s, "eclccserver") || strieq(s.s, "eclserver") )
                break;
        }
        VStringBuffer xpath("Exceptions/Exception[@sequence='%d']", e.getSequence());
        p->removeProp(xpath);
        exceptions.remove(idx);
    }
    if (exceptions.length() == 0)
        p->removeProp("Exceptions");
}


IWUException* CLocalWorkUnit::createException()
{
    CriticalBlock block(crit);
    // For this to be legally called, we must have the write-able interface. So we are already locked for write.
    loadExceptions();

    if (!exceptions.length())
        p->addPropTree("Exceptions");
    IPropertyTree *r = p->queryPropTree("Exceptions");
    IPropertyTree *s = r->addPropTree("Exception");
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
                s = p->addPropTree("WebServicesInfo");
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
        p->addPropTree("Results");
    IPropertyTree *r = p->queryPropTree("Results");
    IPropertyTree *s = r->addPropTree("Result");

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

IConstWUResult* CLocalWorkUnit::getQueryResultByName(const char *qname) const
{
    CriticalBlock block(crit);
    loadResults();
    ForEachItemIn(idx, results)
    {
        IConstWUResult &cur = results.item(idx);
        if (!isSpecialResultSequence(cur.getResultSequence()))
        {
            SCMStringBuffer name;
            cur.getResultName(name);
            if (stricmp(name.str(), qname)==0)
            {
                cur.Link();
                return &cur;
            }
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
    IPropertyTree *vars = (temporaries.length()) ? p->queryPropTree("Temporaries") : p->addPropTree("Temporaries");
    IPropertyTree *s = vars->addPropTree("Variable");
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
        p->addPropTree("Variables");
    IPropertyTree *vars = p->queryPropTree("Variables");
    IPropertyTree *s = vars->addPropTree("Variable");
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
            fileTree->setPropBool("@super", true);
            Owned<IDistributedFileIterator> iter = super->getSubFileIterator(false);
            ForEach (*iter)
            {
                IDistributedFile &file = iter->query();
                StringBuffer fname;
                file.getLogicalName(fname);
                fileTree->addPropTree("Subfile")->setProp("@name", fname.str());
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
        IPropertyTree *files = p->queryPropTree("FilesRead");
        if (!files)
            files = p->addPropTree("FilesRead");
        _noteFileRead(file, files);
    }
}

void CLocalWorkUnit::noteFieldUsage(IPropertyTree * fieldUsage)
{
    if (fieldUsage)
    {
        CriticalBlock block(crit);
        p->addPropTree("usedsources", fieldUsage);
    }
}

void CLocalWorkUnit::_loadFilesWritten() const
{
    // Nothing to do
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
        files = p->addPropTree("Files");
    if (!clusters)
        ::addFile(files, fileName, NULL, usageCount, fileKind, graphOwner);
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

IConstWUFileUsageIterator * CLocalWorkUnit::getFieldUsage() const
{
    CriticalBlock block(crit);
    IPropertyTree* fieldUsageTree = p->queryPropTree("usedsources");

    if (!fieldUsageTree)
        return NULL;

    IPropertyTreeIterator* iter = fieldUsageTree->getElements("*");
    return new CConstWUFileUsageIterator(iter);
}

bool isFilenameResolved(StringBuffer& filename)
{
    size32_t length = filename.length();

    // With current implementation, if filename is surrounded by single quotes, it means that the filename was resolved at compile time.
    if (filename.length() >= 2 && filename.charAt(0) == '\'' && filename.charAt(length-1) == '\'')
        return true;
    else
        return false;
}

bool CLocalWorkUnit::getFieldUsageArray(StringArray & filenames, StringArray & columnnames, const char * clusterName) const
{
    bool scopeLoaded = false;
    StringBuffer defaultScope;

    Owned<IConstWUFileUsageIterator> files = getFieldUsage();

    if (!files)
        return false; // this query was not compiled with recordFieldUsage option.

    ForEach(*files)
    {    
        Owned<IConstWUFileUsage> file = files->get();

        StringBuffer filename(file->queryName());
        size32_t length = filename.length();
        
        if (length == 0)
            throw MakeStringException(WUERR_InvalidFieldUsage, "Invalid FieldUsage found in WU. Cannot enforce view security.");

        StringBuffer normalizedFilename;
        
        // Two cases to handle:
        // 1. Filename was known at compile time, and is surrounded in single quotes (i.e. 'filename').
        // 2. Filename could not be resolved at compile time (i.e. filename is an input to a query), 
        //    and is a raw expression WITHOUT surrounding single quotes (i.e. STORED('input_filename')).
        if (isFilenameResolved(filename))
        {
            // filename cannot be empty (i.e. empty single quotes '')
            if (length == 2)
                throw MakeStringException(WUERR_InvalidFieldUsage, "Invalid FieldUsage found in WU. Cannot enforce view security.");
        
            // Remove surrounding single quotes
            StringAttr cleanFilename(filename.str()+1, length-2);

            // When a filename doesn't start with a tilde (~), it means scope is omitted and is relying on a default scope.
            // We need to load a default scope from config and prefix the filename with it.
            if (cleanFilename.str()[0] != '~')
            {
                // loading a default scope from config is expensive, and should be only done once and be reused later.
                if (!scopeLoaded)
                {
                    //MORE: This should actually depend on the cluster that was active when the file was read!
                    if (!resolveFilePrefix(defaultScope, clusterName))
                        throw MakeStringException(WUERR_InvalidCluster, "Unknown cluster %s", clusterName);
                    scopeLoaded = true;
                }

                normalizedFilename.append(defaultScope.str());
                normalizedFilename.append(cleanFilename.str());
            }
            else
            {
                normalizedFilename.append(cleanFilename); 
            }
        }
        else
        {
            // When filename is an unresolved expression, simply treat the expression as a "non-existent" filename.
            // It will have an effect of this query accessing a non-existent filename, and will be denied access unconditionally.
            normalizedFilename.append(filename.str());
        }

        Owned<IConstWUFieldUsageIterator> fields = file->getFields();
        ForEach(*fields)
        {    
            Owned<IConstWUFieldUsage> field = fields->get();
            filenames.append(normalizedFilename.str());
            columnnames.append(field->queryName());
        }
    }

    return true;
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
        stats = p->addPropTree("DiskUsageStats");
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
    _loadFilesWritten();
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

    const char * currentcluster = queryClusterName();
    const char *wuid = p->queryName();
    StringBuffer curqname;
    getClusterThorQueueName(curqname, currentcluster);

    void *qi = qs->getQ(curqname.str(),wuid);
    if (!qi)
        return false;

    setClusterName(cluster);

    StringBuffer newqname;
    getClusterThorQueueName(newqname, cluster);
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

EnumMapping graphTypes[] = {
   { GraphTypeAny, "unknown" },
   { GraphTypeProgress, "progress" },
   { GraphTypeEcl, "ECL" },
   { GraphTypeActivities, "activities" },
   { GraphTypeSubProgress, "subgraph" },
   { GraphTypeSize,  NULL },
};

WUGraphType getGraphTypeFromString(const char* type)
{
    return (WUGraphType) getEnum(type, graphTypes);
}

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

StringBuffer &appendPTreeOpenTag(StringBuffer &s, IPropertyTree *tree, const char *name, unsigned indent, bool hidePasswords)
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
            if (hidePasswords && streq(attrs->queryName(), "@token"))
                continue;
#ifndef _DEBUG
            if (hidePasswords && streq(attrs->queryName(), "@distributedAccessToken"))
                continue;
#endif
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
        appendPTreeOpenTag(xml.append(' '), paramTree, "Parameters", 0, false).append('\n');

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

void CLocalWorkUnit::createGraph(const char * name, const char *label, WUGraphType type, IPropertyTree *xgmml, unsigned wfid)
{
    CriticalBlock block(crit);
    if (!graphs.length())
        p->addPropTree("Graphs");
    IPropertyTree *r = p->queryPropTree("Graphs");
    IPropertyTree *s = r->addPropTree("Graph");
    CLocalWUGraph *q = new CLocalWUGraph(*this, LINK(s));
    q->setName(name);
    q->setLabel(label);
    q->setType(type);
    q->setWfid(wfid);
    q->setXGMMLTree(xgmml);
    graphs.append(*q);
}

IConstWUGraphProgress *CLocalWorkUnit::getGraphProgress(const char *name) const
{
/*    Owned<IRemoteConnection> conn = getProgressConnection();
    if (conn)
    {
        IPTree *progress = conn->queryRoot()->queryPropTree(graphName);
        if (progress)
            return new CConstGraphProgress(p->queryName(), graphName, progress);
    }
    */
    return NULL;
}
WUGraphState CLocalWorkUnit::queryGraphState(const char *graphName) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
WUGraphState CLocalWorkUnit::queryNodeState(const char *graphName, WUGraphIDType nodeId) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
void CLocalWorkUnit::setGraphState(const char *graphName, unsigned wfid, WUGraphState state) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
void CLocalWorkUnit::setNodeState(const char *graphName, WUGraphIDType nodeId, WUGraphState state) const
{
    throwUnexpected();   // Should only be used for persisted workunits
}
IWUGraphStats *CLocalWorkUnit::updateStats(const char *graphName, StatisticCreatorType creatorType, const char * creator, unsigned _wfid, unsigned subgraph) const
{
    return new CWuGraphStats(LINK(p), creatorType, creator, _wfid, graphName, subgraph);
}

void CLocalWUGraph::setName(const char *str)
{
    p->setProp("@name", str);
}

void CLocalWUGraph::setLabel(const char *str)
{
    p->setProp("@label", str);
}

void CLocalWUGraph::setWfid(unsigned wfid)
{
    p->setPropInt("@wfid", wfid);
}

void CLocalWUGraph::setXGMML(const char *str)
{
    setXGMMLTree(createPTreeFromXMLString(str,ipt_lowmem));
}

void CLocalWUGraph::setXGMMLTree(IPropertyTree *_graph)
{
    assertex(strcmp(_graph->queryName(), "graph")==0);
    IPropertyTree *xgmml = p->setPropTree("xgmml");
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
            IPropertyTree *att = targetNode.addPropTree("att");
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
                        IPropertyTree *att = graphEdge->addPropTree("att");
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
            graph.setown(createPTree(mb, ipt_lowmem));
        else
            graph.setown(p->getBranch("xgmml/graph"));
        if (!graph)
            return NULL;
    }
    if (!doMergeProgress)
        return graph.getLink();
    else
    {
        Owned<IPropertyTree> copy = createPTreeFromIPT(graph, ipt_lowmem);
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

unsigned CLocalWUGraph::getWfid() const
{
    return p->getPropInt("@wfid", 0);
}

void CLocalWUGraph::setType(WUGraphType _type)
{
    setEnum(p, "@type", _type, graphTypes);
}

//=================================================================================================

EnumMapping queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeLog, "log" },
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

unsigned CLocalWUAssociated::getMinActivityId() const
{
    return p->getPropInt("@minActivity", 0);
}

unsigned CLocalWUAssociated::getMaxActivityId() const
{
    return p->getPropInt("@maxActivity", 0);
}


//=================================================================================================

CLocalWUQuery::CLocalWUQuery(IPropertyTree *props) : p(props)
{
    associatedCached = false;
}

EnumMapping queryTypes[] = {
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
    const char *text = p->queryProp("Text");
    if (!text)
        text = p->queryProp("ShortText");
    str.set(text);
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
            Owned<IPropertyTree> xml = createPTreeFromXMLString(text, ipt_caseInsensitive|ipt_lowmem);
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
    bool isArchive = isArchiveQuery(text);
    if (isArchive)
    {
        p->setProp("Text", text);
        Owned<IPropertyTree> xml = createPTreeFromXMLString(text, ipt_caseInsensitive|ipt_lowmem);
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
    else
    {
        p->setProp("Text", text);     // At some point in the future we may be able to remove this,
                                      // but as long as there may be new workunits compiled by old systems, we can't
        p->setProp("ShortText", text);
    }
    p->setPropBool("@isArchive", isArchive);
    if (isArchive)
        p->setPropBool("@hasArchive", true); //preserved if setQueryText is called multiple times.  Should setting this be more explicit?
}

void CLocalWUQuery::setQueryName(const char *qname)
{
    p->setProp("@name", qname);
}

void CLocalWUQuery::setQueryMainDefinition(const char * str)
{
    p->setProp("@main", str);
}

void CLocalWUQuery::addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc, unsigned minActivity, unsigned maxActivity)
{
    CriticalBlock block(crit);
    loadAssociated();
    StringBuffer xpath;
    xpath.append("Associated/File[@filename=\"").append(name).append("\"]");
    if (p->hasProp(xpath))
        return;
    if (!associated.length())
        p->addPropTree("Associated");
    IPropertyTree *pl = p->queryPropTree("Associated");
    IPropertyTree *s = pl->addPropTree("File");
    setEnum(s, "@type", type, queryFileTypes);
    s->setProp("@filename", name);
    s->setProp("@ip", ip);
    s->setProp("@desc", desc);

    if (crc)
        s->setPropInt("@crc", crc);
    if (minActivity)
        s->setPropInt("@minActivity", minActivity);
    if (maxActivity)
        s->setPropInt("@maxActivity", maxActivity);
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

EnumMapping resultStatuses[] = {
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
            default:
                throwUnexpected();
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

void CLocalWUResult::getResultWriteLocation(IStringVal & _graph, unsigned & _activityId) const
{
    _graph.set(p->queryProp("@graph"));
    _activityId = p->getPropInt("@activity", 0);
}

void CLocalWUResult::setResultStatus(WUResultStatus status)
{
    setEnum(p, "@status", status, resultStatuses);
    if (status==ResultStatusUndefined)
    {
        p->removeProp("Value");
        p->removeProp("totalRowCount");
        p->removeProp("rowCount");
        p->removeProp("@format");
        p->removeProp("@tempFileNmae");
        p->removeProp("logicalName");
    }
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

void CLocalWUResult::setResultWriteLocation(const char * _graph, unsigned _activityId)
{
    p->setProp("@graph", _graph);
    p->setPropInt("@activity", _activityId);
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
        existingFormat = nullptr;
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

unsigned CLocalWUException::getActivityId() const
{
    const char * scope = queryScope();
    if (scope)
    {
        const char * colon = strrchr(scope, ':');
        if (colon && hasPrefix(colon+1, ActivityScopePrefix, true))
            return atoi(colon+1+strlen(ActivityScopePrefix));
    }
    return p->getPropInt("@activity", 0);
}

unsigned CLocalWUException::getSequence() const
{
    return p->getPropInt("@sequence", 0);
}

const char * CLocalWUException::queryScope() const
{
    return p->queryProp("@scope");
}

unsigned CLocalWUException::getPriority() const
{
    return p->getPropInt("@prio", 0);
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

void CLocalWUException::setActivityId(unsigned _id)
{
    p->setPropInt("@activity", _id);
}

void CLocalWUException::setScope(const char * _scope)
{
    p->setProp("@scope", _scope);
}

void CLocalWUException::setPriority(unsigned _priority)
{
    p->setPropInt("@prio", _priority);
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
        if (isGlobalScope(scope))
        {
            const char * creator = p->queryProp("@creator");
            descriptionText.append(creator).append(":");
            queryLongStatisticName(descriptionText, kind);
        }
        else
        {
            for (;;)
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

IStringVal & CLocalWUStatistic::getFormattedValue(IStringVal & str) const
{
    StringBuffer formatted;
    formatStatistic(formatted, getValue(), getMeasure());
    str.set(formatted);
    return str;
}

StatisticCreatorType CLocalWUStatistic::getCreatorType() const
{
    return queryCreatorType(p->queryProp("@c"), SCTnone);
}

StatisticScopeType CLocalWUStatistic::getScopeType() const
{
    return queryScopeType(p->queryProp("@s"), SSTnone);
}

StatisticKind CLocalWUStatistic::getKind() const
{
    return queryStatisticKind(p->queryProp("@kind"), StKindNone);
}

const char * CLocalWUStatistic::queryScope() const
{
    const char * scope = p->queryProp("@scope");
    if (scope && streq(scope, LEGACY_GLOBAL_SCOPE))
        scope = GLOBAL_SCOPE;
    return scope;
}

StatisticMeasure CLocalWUStatistic::getMeasure() const
{
    return queryMeasure(p->queryProp("@unit"), SMeasureNone);
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


//==========================================================================================

extern WORKUNIT_API ILocalWorkUnit * createLocalWorkUnit(const char *xml)
{
    Owned<CLocalWorkUnit> cw = new CLocalWorkUnit((ISecManager *) NULL, NULL);
    if (xml)
        cw->loadPTree(createPTreeFromXMLString(xml, ipt_lowmem));
    else
    {
        Owned<IPropertyTree> p = createPTree("W_LOCAL", ipt_lowmem);
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
    writeStringToStream(out, appendPTreeOpenTag(temp, p, name, 1, true));

    Owned<IPropertyTreeIterator> elems = p->getElements("*", iptiter_sort);
    ForEach(*elems)
    {
        IPropertyTree &elem = elems->query();
        if (streq(elem.queryName(), "Parameters"))
        {
            writeStringToStream(out, appendPTreeOpenTag(temp.clear().append(' '), &elem, "Parameters", 2, false).append('\n'));
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
            writeStringToStream(out, appendPTreeOpenTag(temp.clear().append(' '), &elem, "Variables", 2, false).append('\n'));
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
        if (hidePasswords)
            return exportWorkUnitToXMLWithHiddenPasswords(p, str);
        else
            return toXML(p, str, 0, XML_Format|XML_SortTags);
    }
    else
        return str.append("Unrecognized workunit format");
}

extern WORKUNIT_API void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool unpack, bool includeProgress, bool hidePasswords, bool regressionTest)
{
    const IExtendedWUInterface *ewu = queryExtendedWU(wu);
    if (ewu)
    {
        Linked<IPropertyTree> p;
        if (unpack||includeProgress)
            p.setown(ewu->getUnpackedTree(includeProgress));
        else if (regressionTest)
            p.setown(createPTreeFromIPT(ewu->queryPTree()));
        else
            p.set(ewu->queryPTree());
        if (hidePasswords)
            return exportWorkUnitToXMLFileWithHiddenPasswords(p, filename, extraXmlFlags);

        if (regressionTest)
        {
            //This removes any items from the xml that will vary from run to run, so they can be binary compared from run to run
            //The following attributes change with the build.  Simpler to remove rather than needing to updated each build
            p->removeProp("@buildVersion");
            p->removeProp("@eclVersion");
            p->removeProp("@hash");

            //Remove statistics, and extract them to a separate file
            IPropertyTree * stats = p->queryPropTree("Statistics");
            if (stats)
            {
                StringBuffer statsFilename;
                statsFilename.append(filename).append(".stats");
                saveXML(statsFilename, stats, 0, (XML_Format|XML_SortTags|extraXmlFlags) & ~XML_LineBreakAttributes);
                p->removeProp("Statistics");
            }
            //Now remove timestamps from exceptions
            Owned<IPropertyTreeIterator> elems = p->getElements("Exceptions/Exception", iptiter_sort);
            ForEach(*elems)
            {
                IPropertyTree &elem = elems->query();
                elem.removeProp("@time");
            }
        }
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
    StringAttr clusterName(workunit->queryClusterName());
    if (!clusterName.length()) 
        throw MakeStringException(WUERR_InvalidCluster, "No target cluster specified");
    workunit->commit();
    workunit.clear();

    StringBuffer serverQueue;
    getClusterEclCCServerQueueName(serverQueue, clusterName);
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
    if (!checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Write, "Submit", true, true))
        return;

    abortWorkUnit(wuid);
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    if(!cw)
        return;

    WorkunitUpdate wu(&cw->lock());
    const char *abortBy = secuser.getName();
    if (abortBy && *abortBy)
        wu->setTracingValue("AbortBy", abortBy);
    wu->setTracingValueInt64("AbortTimeStamp", getTimeStampNowValue());
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
    if (p->hasProp("@eventScheduledCount"))
        return p->getPropInt("@eventScheduledCount", 0);
    else
        return p->getPropInt("Workflow/@eventScheduledCount", 0);  // Legacy location for this setting
}

void CLocalWorkUnit::incEventScheduledCount()
{
    CriticalBlock block(crit);
    p->setPropInt("@eventScheduledCount", queryEventScheduledCount()+1);
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
        s = p->addPropTree("Workflow");
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
            s = p->addPropTree("Workflow");
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

EnumMapping localFileUploadTypes[] = {
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
        s = p->addPropTree("LocalFileUploads");
    unsigned id = s->numChildren();
    Owned<CLocalFileUpload> upload = new CLocalFileUpload(id, type, source, destination, eventTag);
    s->addPropTree("LocalFileUpload", upload->getTree());
    return id;
}

IStringVal & CLocalWorkUnit::getAbortBy(IStringVal & str) const
{
    CriticalBlock block(crit);
    str.set(p->queryProp("Tracing/AbortBy"));
    return str;
}

unsigned __int64 CLocalWorkUnit::getAbortTimeStamp() const
{
    CriticalBlock block(crit);
    return p->getPropInt64("Tracing/AbortTimeStamp", 0);
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

extern WUState waitForWorkUnitToComplete(const char * wuid, int timeout, std::list<WUState> expectedStates)
{
    return factory->waitForWorkUnit(wuid, (unsigned) timeout, false, expectedStates);
}

extern WORKUNIT_API WUState secWaitForWorkUnitToComplete(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout, std::list<WUState> expectedStates)
{
    if (checkWuSecAccess(wuid, &secmgr, &secuser, SecAccess_Read, "Wait for Complete", false, true))
        return waitForWorkUnitToComplete(wuid, timeout, expectedStates);
    return WUStateUnknown;
}

extern bool waitForWorkUnitToCompile(const char * wuid, int timeout)
{
    switch(factory->waitForWorkUnit(wuid, (unsigned) timeout, true, { WUStateWait }))
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
        try
        {
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
        catch (IException *E)
        {
            VStringBuffer msg("In secDebugWorkunit wuid %s port %d ip %s command %s", wuid, port, ip.str(), command);
            EXCLOG(E, msg.str());
            throw;
        }
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

extern WORKUNIT_API WUState getWorkUnitState(const char* state)
{
    return (WUState) getEnum(state, states);
}

constexpr LogMsgCategory MCschedconn = MCprogress(1000);    // Category used to inform about schedule synchronization

class CWorkflowScheduleConnection : implements IWorkflowScheduleConnection, public CInterface
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
        IPropertyTree *eventQueue = ensurePTree(root, "EventQueue");
        IPropertyTree *eventItem = eventQueue->addPropTree("Item");
        eventItem->setProp("@name", name);
        eventItem->setProp("@text", text);
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


extern WORKUNIT_API void addExceptionToWorkunit(IWorkUnit * wu, ErrorSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, unsigned activity)
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
    if (activity)
        we->setActivityId(activity);
}

const char * skipLeadingXml(const char * text)
{
    if (!text)
        return NULL;

    //skip utf8 BOM, probably excessive
    if (memcmp(text, UTF8_BOM, 3) == 0)
        text += 3;

    for (;;)
    {
        if (isspace(*text))
            text++;
        else if (text[0] == '<' && text[1] == '?')
        {
            text += 2;
            for (;;)
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
            for (;;)
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
    IPropertyTree * newEntry = createPTree("Query", ipt_caseInsensitive|ipt_lowmem);
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
        match = queryRegistry->addPropTree("Alias");
        match->setProp("@name", lcName);
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
    for (;;)
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

extern WORKUNIT_API void associateLocalFile(IWUQuery * query, WUFileType type, const char * name, const char * description, unsigned crc, unsigned minActivity, unsigned maxActivity)
{
    StringBuffer fullPathName;
    makeAbsolutePath(name, fullPathName);
    if (isContainerized())
    {
        const char *dllserver_root = getenv("HPCC_DLLSERVER_PATH");
        assertex(dllserver_root != nullptr);
        StringBuffer destPathName(dllserver_root);
        addNonEmptyPathSepChar(destPathName);
        splitFilename(fullPathName.str(), nullptr, nullptr, &destPathName, &destPathName);
        OwnedIFile source = createIFile(fullPathName);
        OwnedIFile target = createIFile(destPathName);
        if (!target->exists())
        {
            source->copyTo(target, 0, NULL, true);
        }
        query->addAssociatedFile(type, destPathName, "localhost", description, crc, minActivity, maxActivity);
        // Should we delete the local files? May not matter...
    }
    else
    {
        StringBuffer hostname;
        queryHostIP().getIpText(hostname);
        query->addAssociatedFile(type, fullPathName, hostname, description, crc, minActivity, maxActivity);
    }
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

extern WORKUNIT_API void updateWorkunitStat(IWorkUnit * wu, StatisticScopeType scopeType, const char * scope, StatisticKind kind, const char * description, unsigned __int64 value, unsigned wfid)
{
    StringBuffer scopestr;
    if (wfid && scope && *scope)
        scopestr.append(WorkflowScopePrefix).append(wfid).append(":").append(scope);
    else
        scopestr.set(scope);
    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scopestr, kind, description, value, 1, 0, StatsMergeReplace);
}

class WuTimingUpdater : implements ITimeReportInfo
{
public:
    WuTimingUpdater(IWorkUnit * _wu, StatisticScopeType _scopeType, StatisticKind _kind)
    : wu(_wu), scopeType(_scopeType), kind(_kind)
    { }

    virtual void report(const char * scope, const __int64 totaltime, const __int64 maxtime, const unsigned count)
    {
        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, nullptr, totaltime, count, maxtime, StatsMergeReplace);
    }

protected:
    IWorkUnit * wu;
    StatisticScopeType scopeType;
    StatisticKind kind;
};


extern WORKUNIT_API void updateWorkunitTimings(IWorkUnit * wu, ITimeReporter *timer)
{
    WuTimingUpdater target(wu, SSTsection, StTimeTotalExecute);
    timer->report(target);
}

extern WORKUNIT_API void updateWorkunitTimings(IWorkUnit * wu, StatisticScopeType scopeType, StatisticKind kind, ITimeReporter *timer)
{
    WuTimingUpdater target(wu, scopeType, kind);
    timer->report(target);
}


extern WORKUNIT_API void addTimeStamp(IWorkUnit * wu, StatisticScopeType scopeType, const char * scope, StatisticKind kind, unsigned wfid)
{
    StringBuffer scopestr;
    if (wfid && scope && *scope)
        scopestr.append(WorkflowScopePrefix).append(wfid).append(":").append(scope);
    else
        scopestr.set(scope);

    wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scopestr, kind, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);
}

extern WORKUNIT_API double calculateThorCost(__int64 timeNs, unsigned clusterWidth)
{
    IPropertyTree *costs = queryCostsConfiguration();
    if (costs)
    {
        double thor_master_rate = costs->getPropReal("thor/@master", 0.0);
        double thor_slave_rate = costs->getPropReal("thor/@slave", 0.0);

        return calcCost(thor_master_rate, timeNs) + calcCost(thor_slave_rate, timeNs) * clusterWidth;
    }
    return 0;
}

void aggregateStatistic(StatsAggregation & result, IConstWorkUnit * wu, const WuScopeFilter & filter, StatisticKind search)
{
    SimpleReferenceAggregator aggregator(search, result);
    Owned<IConstWUScopeIterator> it = &wu->getScopeIterator(filter);
    ForEach(*it)
        it->playProperties(aggregator);
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
    virtual void beginChildGraphScope(unsigned id)
    {
        StatsScopeId scopeId(SSTchildgraph, id);
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

extern WORKUNIT_API IPropertyTree * getWUGraphProgress(const char * wuid, bool readonly)
{
    if (!wuid || !*wuid)
        return NULL;

    VStringBuffer path("/GraphProgress/%s", wuid);
    Owned<IRemoteConnection> conn = querySDS().connect(path.str(),myProcessSession(),readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
    if (conn)
        return conn->getRoot();
    else
        return NULL;
}

void addWorkunitException(IWorkUnit * wu, IError * error, bool removeTimeStamp)
{
    ErrorSeverity wuSeverity = SeverityInformation;
    ErrorSeverity severity = error->getSeverity();

    switch (severity)
    {
    case SeverityIgnore:
        return;
    case SeverityInformation:
        break;
    case SeverityWarning:
        wuSeverity = SeverityWarning;
        break;
    case SeverityError:
    case SeverityFatal:
        wuSeverity = SeverityError;
        break;
    }

    Owned<IWUException> exception = wu->createException();
    exception->setSeverity(wuSeverity);

    StringBuffer msg;
    exception->setExceptionCode(error->errorCode());
    exception->setExceptionMessage(error->errorMessage(msg).str());
    const char * source = queryCreatorTypeName(queryStatisticsComponentType());
    exception->setExceptionSource(source);

    exception->setExceptionFileName(error->getFilename());
    exception->setExceptionLineNo(error->getLine());
    exception->setExceptionColumn(error->getColumn());
    if (removeTimeStamp)
        exception->setTimeStamp(nullptr);

    if (error->getActivity())
        exception->setActivityId(error->getActivity());
    if (error->queryScope())
        exception->setScope(error->queryScope());
}


IError * WorkUnitErrorReceiver::mapError(IError * error)
{
    return LINK(error);
}

void WorkUnitErrorReceiver::report(IError* eclError)
{
    addWorkunitException(wu, eclError, removeTimeStamp);
}

size32_t WorkUnitErrorReceiver::errCount()
{
    unsigned count = 0;
    Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
    ForEach(*exceptions)
        if (exceptions->query().getSeverity() == SeverityError)
            count++;
    return count;
}

size32_t WorkUnitErrorReceiver::warnCount()
{
    unsigned count = 0;
    Owned<IConstWUExceptionIterator> exceptions = &wu->getExceptions();
    ForEach(*exceptions)
        if (exceptions->query().getSeverity() == SeverityWarning)
            count++;
    return count;
}

bool isValidPriorityValue(const char *priority)
{
    if (isEmptyString(priority))
        return false;
    if (strieq("SLA", priority) || strieq("LOW", priority) || strieq("HIGH", priority) || strieq("NONE", priority))
        return true;
    return false;
}

bool isValidMemoryValue(const char *memoryUnit)
{
    if (isEmptyString(memoryUnit) || !isdigit(*memoryUnit))
        return false;
    while (isdigit(*++memoryUnit));

    if (!*memoryUnit)
        return true;

    switch (toupper(*memoryUnit++))
    {
        case 'E':
        case 'P':
        case 'T':
        case 'G':
        case 'M':
        case 'K':
            if (!*memoryUnit || strieq("B", memoryUnit))
                return true;
            break;
        case 'B':
            if (!*memoryUnit)
                return true;
            break;
    }
    return false;
}

#ifdef _CONTAINERIZED

static void setResources(StringBuffer &jobYaml, const IConstWorkUnit *workunit, const char *process)
{
    StringBuffer s;
    unsigned memRequest = workunit->getDebugValueInt(s.clear().appendf("%s-memRequest", process), 0);
    unsigned memLimit = workunit->getDebugValueInt(s.clear().appendf("%s-memLimit", process), 0);
    if (memLimit && memLimit < memRequest)
        memLimit = memRequest;
    if (memRequest)
        jobYaml.replaceString("#request-memory", s.clear().appendf("memory: \"%uMi\"", memRequest));
    if (memLimit)
        jobYaml.replaceString("#limit-memory", s.clear().appendf("memory: \"%uMi\"", memLimit));
    unsigned cpuRequest = workunit->getDebugValueInt(s.clear().appendf("%s-cpuRequest", process), 0);
    unsigned cpuLimit = workunit->getDebugValueInt(s.clear().appendf("%s-cpuLimit", process), 0);
    if (cpuLimit && cpuLimit < cpuRequest)
        cpuLimit = cpuRequest;
    if (cpuRequest)
        jobYaml.replaceString("#request-cpu", s.clear().appendf("cpu: \"%um\"", cpuRequest));
    if (cpuLimit)
        jobYaml.replaceString("#limit-cpu", s.clear().appendf("cpu: \"%um\"", cpuLimit));
}


void deleteK8sJob(const char *componentName, const char *job)
{
    VStringBuffer jobname("%s-%s", componentName, job);
    jobname.toLowerCase();
    VStringBuffer deleteJob("kubectl delete job/%s", jobname.str());
    StringBuffer output, error;
    bool ret = runExternalCommand(output, error, deleteJob.str(), nullptr);
    DBGLOG("kubectl delete output: %s", output.str());
    if (error.length())
        DBGLOG("kubectl delete error: %s", error.str());
    if (ret)
        throw makeStringException(0, "Failed to run kubectl delete");
}

void waitK8sJob(const char *componentName, const char *job, const char *condition)
{
    VStringBuffer jobname("%s-%s", componentName, job);
    jobname.toLowerCase();

    if (isEmptyString(condition))
        condition = "condition=complete";

    // MORE - blocks indefinitely here if you request too many resources
    VStringBuffer waitJob("kubectl wait --for=%s --timeout=10h job/%s", condition, jobname.str());  // MORE - make timeout configurable
    StringBuffer output, error;
    bool ret = runExternalCommand(output, error, waitJob.str(), nullptr);
    DBGLOG("kubectl wait output: %s", output.str());
    if (error.length())
        DBGLOG("kubectl wait error: %s", error.str());
    if (ret)
        throw makeStringException(0, "Failed to run kubectl wait");
}

void launchK8sJob(const char *componentName, const char *wuid, const char *job, const std::list<std::pair<std::string, std::string>> &extraParams)
{
    VStringBuffer jobname("%s-%s", componentName, job);
    jobname.toLowerCase();
    VStringBuffer args("\"--workunit=%s\"", wuid);
    for (const auto &p: extraParams)
        args.append(',').newline().append("\"--").append(p.first.c_str()).append('=').append(p.second.c_str()).append("\"");
    VStringBuffer jobSpecFilename("/etc/config/%s-jobspec.yaml", componentName);
    StringBuffer jobYaml;
    jobYaml.loadFile(jobSpecFilename, false);
    jobYaml.replaceString("%jobname", jobname.str());
    jobYaml.replaceString("%args", args.str());

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    if (factory)
    {
        Owned<IConstWorkUnit> workunit = factory->openWorkUnit(wuid);
        if (workunit)
            setResources(jobYaml, workunit, componentName);
    }

    StringBuffer output, error;
    unsigned ret = runExternalCommand(output, error, "kubectl apply -f -", jobYaml.str());
    DBGLOG("kubectl output: %s", output.str());
    if (error.length())
        DBGLOG("kubectl error: %s", error.str());
    if (ret)
    {
        DBGLOG("Using job yaml %s", jobYaml.str());
        throw makeStringException(0, "Failed to start kubectl job");
    }
}

void runK8sJob(const char *componentName, const char *wuid, const char *job, bool del, const std::list<std::pair<std::string, std::string>> &extraParams)
{
    launchK8sJob(componentName, wuid, job, extraParams);
    waitK8sJob(componentName, job);
    if (del)
        deleteK8sJob(componentName, job);
}

#endif
