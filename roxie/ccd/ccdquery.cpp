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

#include <platform.h>
#include <jlib.hpp>

#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdsnmp.hpp"

#include "thorplugin.hpp"
#include "layouttrans.hpp"

void ActivityArray::append(IActivityFactory &cur)
{
    hash.setValue(cur.queryId(), activities.ordinality());
    activities.append(cur);
}

unsigned ActivityArray::findActivityIndex(unsigned id)
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
// multiple slave channels
//----------------------------------------------------------------------------------------------

class CQueryDll : public CInterface, implements IQueryDll
{
    StringAttr dllName;
    Owned <ILoadedDllEntry> dll;
    Owned <IConstWorkUnit> wu;
    static CriticalSection dllCacheLock;
    static CopyMapStringToMyClass<CQueryDll> dllCache;

public:
    IMPLEMENT_IINTERFACE;

    CQueryDll(const char *_dllName, ILoadedDllEntry *_dll) : dllName(_dllName), dll(_dll)
    {
        StringBuffer wuXML;
        if (getEmbeddedWorkUnitXML(dll, wuXML))
        {
            Owned<ILocalWorkUnit> localWU = createLocalWorkUnit();
            localWU->loadXML(wuXML);
            wu.setown(localWU->unlock());
        }
        CriticalBlock b(dllCacheLock);
        dllCache.setValue(dllName, this);
    }
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
        CriticalBlock b(dllCacheLock);
        CQueryDll *dll = LINK(dllCache.getValue(dllName));
        if (dll && dll->isAlive())
            return dll;
        else
        {
            Owned<ILoadedDllEntry> dll = isExe ? createExeDllEntry(dllName) : queryRoxieDllServer().loadDll(dllName, DllLocationDirectory);
            assertex(dll != NULL);
            return new CQueryDll(dllName, dll.getClear());
        }
    }
    static const IQueryDll *getWorkUnitDll(const char *wuid)
    {
        Owned <IRoxieDaliHelper> daliHelper = connectToDali();
        Owned<IConstWorkUnit> wu = daliHelper->attachWorkunit(wuid, NULL);
        if (wu)
        {
            SCMStringBuffer dllName;
            Owned<IConstWUQuery> q = wu->getQuery();
            q->getQueryDllName(dllName);
            return getQueryDll(dllName.str(), false);
        }
        else
            return NULL;
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

extern const IQueryDll *createWuQueryDll(const char *wuid)
{
    return CQueryDll::getWorkUnitDll(wuid);
}


//----------------------------------------------------------------------------------------------
// Class CQueryFactory is the main implementation of IQueryFactory, combining a IQueryDll and a
// package context into an object that can quickly create a the query context that executes a specific
// instance of a Roxie query. 
// Caching is used to ensure that only queries that are affected by a package change need to be reloaded.
// Derived classes handle the differences between slave and server side factories
//----------------------------------------------------------------------------------------------

class CQueryFactory : public CInterface, implements IQueryFactory, implements IResourceContext
{
protected:
    const IRoxiePackage &package;
    Owned<const IQueryDll> dll;
    MapStringToActivityArray graphMap;
    StringAttr id;
    StringBuffer errorMessage;
    MapIdToActivityFactory allActivities;

    bool isSuspended;
    bool enableFieldTranslation;
    unsigned timeLimit;
    unsigned warnTimeLimit;
    memsize_t memoryLimit;
    unsigned priority;
    unsigned libraryInterfaceHash;
    hash64_t hashValue;

    static SpinLock queriesCrit;
    static CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> queryMap;

public:
    static CriticalSection queryCreateLock;

protected:
    IRoxieServerActivityFactory *createActivityFactory(ThorActivityKind kind, unsigned subgraphId, IPropertyTree &node)
    {
        unsigned id = node.getPropInt("@id", 0);

        if (isSuspended)
            return createRoxieServerDummyActivityFactory(id, subgraphId, *this, NULL, TAKnone, node, false); // Is there actually any point?

        StringBuffer helperName;
        node.getProp("att[@name=\"helper\"]/@value", helperName);
        if (!helperName.length())
            helperName.append("fAc").append(id);
        HelperFactory *helperFactory = dll->getFactory(helperName);
        if (!helperFactory)
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Internal error: helper function %s not exported", helperName.str());

        switch (kind)
        {
        case TAKalljoin:
        case TAKalldenormalize:
        case TAKalldenormalizegroup:
            return createRoxieServerAllJoinActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKapply:
            return createRoxieServerApplyActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKaggregate:
        case TAKexistsaggregate:    // could special case.
        case TAKcountaggregate:
            return createRoxieServerAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKcase:
        case TAKchildcase:
            return createRoxieServerCaseActivityFactory(id, subgraphId, *this, helperFactory, kind, isGraphIndependent(node));
        case TAKcatch:
        case TAKskipcatch:
        case TAKcreaterowcatch:
            return createRoxieServerCatchActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchilditerator:
            return createRoxieServerChildIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchoosesets:
            return createRoxieServerChooseSetsActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchoosesetsenth:
            return createRoxieServerChooseSetsEnthActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchoosesetslast:
            return createRoxieServerChooseSetsLastActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKproject:
        case TAKcountproject:
            return createRoxieServerProjectActivityFactory(id, subgraphId, *this, helperFactory, kind); // code is common between Project, CountProject
        case TAKfilterproject:
            return createRoxieServerFilterProjectActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKdatasetresult:
        case TAKrowresult:
            return createRoxieServerDatasetResultActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKdedup:
            return createRoxieServerDedupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKdegroup:
            return createRoxieServerDegroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKcsvread:
        case TAKxmlread:
        case TAKdiskread:
        {       
            if (node.getPropBool("att[@name='_isSpill']/@value", false) || node.getPropBool("att[@name='_isSpillGlobal']/@value", false))
                return createRoxieServerSpillReadActivityFactory(id, subgraphId, *this, helperFactory, kind);
            else
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerDiskReadActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        }
        case TAKmemoryspillread:
            return createRoxieServerSpillReadActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKdisknormalize:
        case TAKdiskcount:
        case TAKdiskaggregate:
        case TAKdiskgroupaggregate:
        {
            RemoteActivityId remoteId(id, hashValue);
            return createRoxieServerDiskReadActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
        }
        case TAKchildnormalize:
            return createRoxieServerNewChildNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchildaggregate:
            return createRoxieServerNewChildAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchildgroupaggregate:
            return createRoxieServerNewChildGroupAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKchildthroughnormalize:
            return createRoxieServerNewChildThroughNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKcsvwrite:
        case TAKdiskwrite:
        case TAKxmlwrite:
        case TAKmemoryspillwrite:
            return createRoxieServerDiskWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKindexwrite:
            return createRoxieServerIndexWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKenth:
            return createRoxieServerEnthActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKfetch:
        case TAKcsvfetch:
        case TAKxmlfetch:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerFetchActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKfilter:
            return createRoxieServerFilterActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKfiltergroup:
            return createRoxieServerFilterGroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKfirstn:
            return createRoxieServerFirstNActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKfunnel:
            return createRoxieServerConcatActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKgroup:
            return createRoxieServerGroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKhashaggregate:
            return createRoxieServerHashAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKif:
        case TAKchildif:
            return createRoxieServerIfActivityFactory(id, subgraphId, *this, helperFactory, kind, isGraphIndependent(node));
        case TAKifaction:
            return createRoxieServerIfActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKparallel:
            return createRoxieServerParallelActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKsequential:
            return createRoxieServerSequentialActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKindexread:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerIndexReadActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKindexnormalize:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerIndexNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKindexcount:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerIndexCountActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKindexaggregate:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerIndexAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerIndexGroupAggregateActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, node);
            }
        case TAKcountdisk:
            return createRoxieServerDiskCountActivityFactory(id, subgraphId, *this, helperFactory, kind, node);
        case TAKhashdedup:
            return createRoxieServerHashDedupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKhashdenormalize:
        case TAKhashdistribute:
        case TAKhashdistributemerge:
        case TAKhashjoin:
            throwUnexpected();  // Code generator should have removed or transformed
        case TAKiterate:
            return createRoxieServerIterateActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKprocess:
            return createRoxieServerProcessActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKjoin:
        case TAKjoinlight:
        case TAKdenormalize:
        case TAKdenormalizegroup:
            return createRoxieServerJoinActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKkeyeddistribute:
            throwUnexpected();  // Code generator should have removed or transformed
        case TAKkeyedjoin:
        case TAKkeyeddenormalize:
        case TAKkeyeddenormalizegroup:
        {
            RemoteActivityId remoteId(id, hashValue);
            RemoteActivityId remoteId2(id | ROXIE_ACTIVITY_FETCH, hashValue);
            return createRoxieServerKeyedJoinActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, remoteId2, node);
        }
        case TAKlimit:
            return createRoxieServerLimitActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKlookupjoin:
        case TAKlookupdenormalize:
        case TAKlookupdenormalizegroup:
            return createRoxieServerLookupJoinActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKmerge:
            return createRoxieServerMergeActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnormalize:
            return createRoxieServerNormalizeActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnormalizechild:
            return createRoxieServerNormalizeChildActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnormalizelinkedchild:
            return createRoxieServerNormalizeLinkedChildActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnull:
            return createRoxieServerNullActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsideeffect:
            return createRoxieServerSideEffectActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKsimpleaction:
            return createRoxieServerActionActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), isRootAction(node));
        case TAKparse:
            return createRoxieServerParseActivityFactory(id, subgraphId, *this, helperFactory, kind, this);
        case TAKworkunitwrite:
            return createRoxieServerWorkUnitWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), isRootAction(node));
        case TAKpiperead:
            return createRoxieServerPipeReadActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKpipethrough:
            return createRoxieServerPipeThroughActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKpipewrite:
            return createRoxieServerPipeWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), isRootAction(node));
        case TAKpull:
            throwUnexpected(); //code generator strips for non-thor
        case TAKrawiterator:
            return createRoxieServerRawIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKlinkedrawiterator:
            return createRoxieServerLinkedRawIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKremoteresult:
            return createRoxieServerRemoteResultActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), isRootAction(node));
        case TAKrollup:
            return createRoxieServerRollupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsample:
            return createRoxieServerSampleActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKselectn:
            return createRoxieServerSelectNActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKselfjoin:
        case TAKselfjoinlight:
            return createRoxieServerSelfJoinActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKskiplimit:
        case TAKcreaterowlimit:
            return createRoxieServerSkipLimitActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsoap_rowdataset:
            return createRoxieServerSoapRowCallActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsoap_rowaction:
            return createRoxieServerSoapRowActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKsoap_datasetdataset:
            return createRoxieServerSoapDatasetCallActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsoap_datasetaction:
            return createRoxieServerSoapDatasetActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));
        case TAKsort:
            return createRoxieServerSortActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKspill:
        case TAKmemoryspillsplit:
            return createRoxieServerThroughSpillActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsplit:
            return createRoxieServerSplitActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKstreamediterator:
            return createRoxieServerStreamedIteratorActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKtemptable:
        case TAKtemprow:
            return createRoxieServerTempTableActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKinlinetable:
            return createRoxieServerInlineTableActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKthroughaggregate:
            throwUnexpected(); // Concept of through aggregates has been proven not to work in Roxie - codegen should not be creating them any more.
        case TAKtopn:
            return createRoxieServerTopNActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKworkunitread:
            return createRoxieServerWorkUnitReadActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKxmlparse:
            return createRoxieServerXmlParseActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKregroup:
            return createRoxieServerRegroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKcombine:
            return createRoxieServerCombineActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKcombinegroup:
            return createRoxieServerCombineGroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKrollupgroup:
            return createRoxieServerRollupGroupActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKlocalresultread:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerLocalResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, graphId);
            }
        case TAKlocalstreamread:
            return createRoxieServerLocalResultStreamReadActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKlocalresultwrite:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerLocalResultWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), graphId, isRootAction(node));
            }
        case TAKloopcount:
        case TAKlooprow:
        case TAKloopdataset:
            {
                unsigned loopId = node.getPropInt("att[@name=\"_loopid\"]/@value", 0);
                return createRoxieServerLoopActivityFactory(id, subgraphId, *this, helperFactory, kind, loopId);
            }
        case TAKremotegraph:
            {
                RemoteActivityId remoteId(id, hashValue);
                return createRoxieServerRemoteActivityFactory(id, subgraphId, *this, helperFactory, kind, remoteId, isRootAction(node));
            }
        case TAKgraphloopresultread:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerGraphLoopResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, graphId);
            }
        case TAKgraphloopresultwrite:
            {
                unsigned graphId = getGraphId(node);
                return createRoxieServerGraphLoopResultWriteActivityFactory(id, subgraphId, *this, helperFactory, kind, usageCount(node), graphId);
            }
        case TAKnwaygraphloopresultread:
            {
                unsigned graphId  = node.getPropInt("att[@name=\"_graphId\"]/@value", 0);
                return createRoxieServerNWayGraphLoopResultReadActivityFactory(id, subgraphId, *this, helperFactory, kind, graphId);
            }
        case TAKnwayinput:
            return createRoxieServerNWayInputActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnwaymerge:
            return createRoxieServerNWayMergeActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnwaymergejoin:
        case TAKnwayjoin:
            return createRoxieServerNWayMergeJoinActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKsorted:
            return createRoxieServerSortedActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKgraphloop:
        case TAKparallelgraphloop:
            {
                unsigned loopId = node.getPropInt("att[@name=\"_loopid\"]/@value", 0);
                return createRoxieServerGraphLoopActivityFactory(id, subgraphId, *this, helperFactory, kind, loopId);
            }
        case TAKlibrarycall:
            {
                LibraryCallFactoryExtra extra;
                extra.maxOutputs = node.getPropInt("att[@name=\"_maxOutputs\"]/@value", 0);
                extra.graphid = node.getPropInt("att[@name=\"_graphid\"]/@value", 0);
                extra.libraryName.set(node.queryProp("att[@name=\"libname\"]/@value"));
                extra.interfaceHash = node.getPropInt("att[@name=\"_interfaceHash\"]/@value", 0);
                extra.embedded = node.getPropBool("att[@name=\"embedded\"]/@value", false) ;

                Owned<IPropertyTreeIterator> iter = node.getElements("att[@name=\"_outputUsed\"]");
                ForEach(*iter)
                    extra.outputs.append(iter->query().getPropInt("@value"));

                return createRoxieServerLibraryCallActivityFactory(id, subgraphId, *this, helperFactory, kind, extra);
            }
        case TAKnwayselect:
            return createRoxieServerNWaySelectActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKnonempty:
            return createRoxieServerNonEmptyActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKprefetchproject:
            return createRoxieServerPrefetchProjectActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKwhen_dataset:
            return createRoxieServerWhenActivityFactory(id, subgraphId, *this, helperFactory, kind);
        case TAKwhen_action:
            return createRoxieServerWhenActionActivityFactory(id, subgraphId, *this, helperFactory, kind, isRootAction(node));

        // These are not required in Roxie for the time being - code generator should trap them
        case TAKdistribution:
        case TAKchilddataset:

        default:
            throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "Unimplemented activity %s required", getActivityText(kind));
            break;
        }
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

    virtual IRoxieServerActivityFactory *getRoxieServerActivityFactory(unsigned id) const
    {
        checkSuspended();
        return LINK(QUERYINTERFACE(findActivity(id), IRoxieServerActivityFactory));
    }

    virtual ISlaveActivityFactory *getSlaveActivityFactory(unsigned id) const
    {
        checkSuspended();
        IActivityFactory *f = findActivity(id);
        return LINK(QUERYINTERFACE(f, ISlaveActivityFactory)); // MORE - don't dynamic cast yuk
    }

    ActivityArray *loadChildGraph(IPropertyTree &graph)
    {
        // MORE - this is starting to look very much like loadGraph (on Roxie server side)
        ActivityArray *activities = new ActivityArray(true, graph.getPropBool("@delayed"), graph.getPropBool("@library"), graph.getPropBool("@sequential"));
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
                activities->serverItem(target).setInput(targetInput, source, sourceOutput);
            }
        }
        catch (...)
        {
            ::Release(activities);
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
                unsigned source = activities->findActivityIndex(edge.getPropInt("@source", 0));
                unsigned target = activities->recursiveFindActivityIndex(edge.getPropInt("@target", 0));

                unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                unsigned targetInput = edge.getPropInt("att[@name=\"_targetIndex\"]/@value", 0);
                activities->serverItem(target).setInput(targetInput, source, sourceOutput);
            }
        }
    }

    // loadGraph loads outer level graph. This is virtual as slave is very different from Roxie server
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

    CQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue, unsigned _channelNo) 
        : id(_id), package(_package), dll(_dll), channelNo(_channelNo), hashValue(_hashValue)
    {
        package.Link();
        isSuspended = false;
        libraryInterfaceHash = 0;
        priority = 0;
        memoryLimit = defaultMemoryLimit;
        timeLimit = defaultTimeLimit[priority];
        warnTimeLimit = 0;
        enableFieldTranslation = fieldTranslationEnabled;

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

    virtual void beforeDispose()
    {
        SpinBlock b(queriesCrit);
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table. 
        // So only remove from hash table if what we find there matches the item that is being deleted.
        CQueryFactory *goer = queryMap.getValue(hashValue+channelNo);
        if (goer == this)
            queryMap.remove(hashValue+channelNo);
    }

    static IQueryFactory *getQueryFactory(hash64_t hashValue, unsigned channelNo)
    {
        SpinBlock b(queriesCrit);
        CQueryFactory *factory = LINK(queryMap.getValue(hashValue+channelNo));
        if (factory && factory->isAlive())
            return factory;
        else
            return NULL;
    }

    static hash64_t getQueryHash(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo)
    {
        hash64_t hashValue = rtlHash64VStr(dll->queryDll()->queryName(), package.queryHash());
        hashValue = rtlHash64VStr(id, hashValue);
        if (stateInfo)
        {
            StringBuffer xml;
            toXML(stateInfo, xml);
            hashValue = rtlHash64Data(xml.length(), xml.str(), hashValue);
        }
        return hashValue;
    }
    
    virtual void load(const IPropertyTree *stateInfo)
    {
        IConstWorkUnit *wu = dll->queryWorkUnit();
        if (wu) // wu may be null in some unit test cases
        {
            libraryInterfaceHash = wu->getApplicationValueInt("LibraryModule", "interfaceHash", 0);

            // calculate priority before others since it affects the defaults of others
            priority = wu->getDebugValueInt("@priority", 0);
            if (stateInfo)
                priority = stateInfo->getPropInt("@priority", priority);

            memoryLimit = (memsize_t) wu->getDebugValueInt64("memoryLimit", defaultMemoryLimit);
            timeLimit = (unsigned) wu->getDebugValueInt("timeLimit", defaultTimeLimit[priority]);
            warnTimeLimit = (unsigned) wu->getDebugValueInt("warnTimeLimit", 0);
            SCMStringBuffer bStr;
            enableFieldTranslation = strToBool(wu->getDebugValue("layoutTranslationEnabled", bStr).str());

            // MORE - does package override stateInfo, or vice versa?

            if (stateInfo)
            {
                // info in querySets can override the defaults from workunit for some limits
                isSuspended = stateInfo->getPropBool("@suspended", false);
                memoryLimit = (memsize_t) stateInfo->getPropInt64("@memoryLimit", memoryLimit);
                timeLimit = (unsigned) stateInfo->getPropInt("@timeLimit", timeLimit);
                warnTimeLimit = (unsigned) stateInfo->getPropInt("@warnTimeLimit", warnTimeLimit);
            }

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
                    suspend(true, m.str(), NULL, false);
                    ERRLOG("Query %s suspended: %s", id.get(), m.str());
                    E->Release();
                }
            }
        }
        SpinBlock b(queriesCrit);
        queryMap.setValue(hashValue+channelNo, this);
    }

    virtual unsigned queryChannel() const
    {
        return channelNo;
    }

    virtual hash64_t queryHash() const
    {
        return hashValue;
    }

    virtual const char *loadResource(unsigned id)
    {
        return (const char *) queryDll()->getResource(id);
    }

    virtual ActivityArray *lookupGraphActivities(const char *name) const
    {
        return *graphMap.getValue(name);
    }

    virtual IActivityGraph *lookupGraph(const char *name, IProbeManager *probeManager, const IRoxieContextLogger &logctx, IRoxieServerActivity *parentActivity) const
    {
        ActivityArrayPtr *graph = graphMap.getValue(name);
        assertex(graph);
        Owned<IActivityGraph> ret = ::createActivityGraph(name, 0, **graph, parentActivity, probeManager, logctx);
        return ret.getClear();
    }

    void getGraphStats(StringBuffer &reply, const IPropertyTree &thisGraph) const
    {
        Owned<IPropertyTree> graph = createPTreeFromIPT(&thisGraph);
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

    virtual IPropertyTree* cloneQueryXGMML() const
    {
        assertex(dll->queryWorkUnit());
        Owned<IPropertyTree> tree = createPTree("Query");
        Owned<IConstWUGraphIterator> graphs = &dll->queryWorkUnit()->getGraphs(GraphTypeActivities);
        SCMStringBuffer graphNameStr;
        ForEach(*graphs)
        {
            graphs->query().getName(graphNameStr);
            const char *graphName = graphNameStr.s.str();
            Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
            IPropertyTree *newGraph = createPTree();
            newGraph->setProp("@id", graphName);
            IPropertyTree *newXGMML = createPTree();
            newXGMML->addPropTree("graph", graphXgmml.getLink());
            newGraph->addPropTree("xgmml", newXGMML);
            tree->addPropTree("Graph", newGraph);
        }
        return tree.getClear();
    }

    virtual void getStats(StringBuffer &reply, const char *graphName) const
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
            reply.appendf("<Graph id='%s'><xgmml><graph>", thisGraphNameStr.s.str());
            Owned<IPropertyTree> graphXgmml = graphs->query().getXGMMLTree(false);
            getGraphStats(reply, *graphXgmml);
            reply.append("</graph></xgmml></Graph>");
        }
    }
    virtual void getActivityMetrics(StringBuffer &reply) const
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
    virtual void resetQueryTimings()
    {
        HashIterator i(allActivities);
        ForEach(i)
        {
            IActivityFactory *f = *allActivities.mapToValue(&i.query());
            f->resetNodeProgressInfo();
        }
    }
    virtual const char *queryErrorMessage() const
    {
        return errorMessage.str();
    }
    virtual const char *queryQueryName() const
    {
        return id;
    }
    virtual bool isQueryLibrary() const 
    {
        return libraryInterfaceHash != 0; 
    }
    virtual unsigned getQueryLibraryInterfaceHash() const 
    {
        return libraryInterfaceHash;
    }
    virtual void suspend(bool suspendit, const char* errMsg, const char *userId, bool appendIfNewError)
    {
        // MORE - should wait until no queries active before returning
        isSuspended = suspendit; // Atomic enough for our purposes I think - at least until the wait stuff is in place
        if (appendIfNewError)
        {
            if (errorMessage.length())
            {
                // MORE - not the most efficient code, but this error condition should not occur in production
                if (strstr(errorMessage.str(), errMsg) == 0)
                    errorMessage.appendf(", %s", errMsg);
            }
            else
                errorMessage.append(errMsg);
        }
        else
            errorMessage.clear().append(errMsg);
    }

    virtual bool suspended() const
    {
        return isSuspended;
    }
    virtual memsize_t getMemoryLimit() const
    {
        return memoryLimit;
    }
    virtual unsigned getTimeLimit() const
    {
        return timeLimit;
    }
    virtual ILoadedDllEntry *queryDll() const 
    {
        return dll->queryDll();
    }
    virtual const IRoxiePackage &queryPackage() const
    {
        return package;
    }
    virtual WorkflowMachine *createWorkflowMachine(bool isOnce, const IRoxieContextLogger &logctx) const 
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
        return strdup(result ? result : defaultValue);
    }
    virtual unsigned getPriority() const
    {
        return priority;
    }
    virtual unsigned getWarnTimeLimit() const
    {
        return warnTimeLimit;
    }
    virtual int getDebugValueInt(const char * propname, int defVal) const
    {
        assertex(dll->queryWorkUnit());
        return dll->queryWorkUnit()->getDebugValueInt(propname, defVal);
    }
    virtual bool getDebugValueBool(const char * propname, bool defVal) const
    {
        assertex(dll->queryWorkUnit());
        return dll->queryWorkUnit()->getDebugValueBool(propname, defVal);
    }
    bool getEnableFieldTranslation() const
    {
        return enableFieldTranslation;
    }

    virtual IRoxieSlaveContext *createSlaveContext(const SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        throwUnexpected();   // only implemented in derived slave class
    }

    virtual IPropertyTree &queryOnceContext() const
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IDeserializedResultStore &queryOnceResultStore() const
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IRoxieServerContext *createContext(IPropertyTree *xml, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, const IRoxieContextLogger &_logctx, XmlReaderOptions xmlReadFlags) const
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const IRoxieContextLogger &_logctx) const
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, unsigned bytesOut)
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual IPropertyTree *getQueryStats(time_t from, time_t to)
    {
        throwUnexpected();   // only implemented in derived server class
    }
    virtual void getGraphNames(StringArray &ret) const
    {
        Owned<IConstWUGraphIterator> graphs = &dll->queryWorkUnit()->getGraphs(GraphTypeActivities);
        ForEach(*graphs)
        {
            SCMStringBuffer graphName;
            graphs->query().getName(graphName);
            ret.append(graphName.str());
        }
    }

protected:
    void checkSuspended() const
    {
        if (isSuspended)
        {
            StringBuffer err;
            if (errorMessage.length())
                err.appendf(" because %s", errorMessage.str());
            throw MakeStringException(ROXIE_QUERY_SUSPENDED, "Query %s is suspended%s", id.get(), err.str());
        }
    }

};

CriticalSection CQueryFactory::queryCreateLock;
SpinLock CQueryFactory::queriesCrit;
CopyMapXToMyClass<hash64_t, hash64_t, CQueryFactory> CQueryFactory::queryMap;

extern IQueryFactory *getQueryFactory(hash64_t hashvalue, unsigned channel)
{
    return CQueryFactory::getQueryFactory(hashvalue, channel);
}

class CRoxieServerQueryFactory : public CQueryFactory
{
    // Parts of query factory is only interesting on the server - once support, workflow support, and tracking of total query times

protected:
    mutable CriticalSection onceCrit;
    mutable Owned<roxiemem::IRowManager> onceManager; // release AFTER resultStore
    mutable Owned<IPropertyTree> onceContext;
    mutable Owned<IDeserializedResultStore> onceResultStore;
    mutable Owned<IException> onceException;

    Owned<IQueryStatsAggregator> queryStats;

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

public:
    CRoxieServerQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue) : CQueryFactory(_id, _dll, _package, _hashValue, 0)
    {
        queryStats.setown(createQueryStatsAggregator(id.get(), statsExpiryTime));
    }

    virtual void noteQuery(time_t startTime, bool failed, unsigned elapsed, unsigned memused, unsigned slavesReplyLen, unsigned bytesOut)
    {
        queryStats->noteQuery(startTime, failed, elapsed, memused, slavesReplyLen, bytesOut);
    }

    virtual void addDependency(unsigned sourceIdx, unsigned sourceId, unsigned targetId, int controlId, const char *edgeId, ActivityArray * activities)
    {
        // addDependency is expected to fail occasionally on slave, but never on Roxie server
        if (!doAddDependency(sourceIdx, sourceId, targetId, controlId, edgeId, activities))
            throw MakeStringException(ROXIE_ADDDEPENDENCY_ERROR, "Failed to create dependency from %u on %u", sourceId, targetId);
    }

    virtual ActivityArray *loadGraph(IPropertyTree &graph, const char *graphName)
    {
        bool isLibraryGraph = graph.getPropBool("@library");
        bool isSequential = graph.getPropBool("@sequential");
        ActivityArray *activities = new ActivityArray(isLibraryGraph, false, isLibraryGraph, isSequential);
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
            throw;
        }
        return activities;
    }

    virtual IDeserializedResultStore &queryOnceResultStore() const
    {
        assertex(onceResultStore!= NULL);
        return *onceResultStore;
    }

    virtual IPropertyTree &queryOnceContext() const
    {
        assertex(onceContext != NULL);
        return *onceContext;
    }

    virtual void checkOnceDone(const IRoxieContextLogger &_logctx) const
    {
        if (hasOnceSection())
        {
            CriticalBlock b(onceCrit);
            if (!onceContext)
            {
                onceContext.setown(createPTree());
                onceResultStore.setown(createDeserializedResultStore());
                Owned <IRoxieServerContext> ctx = createOnceServerContext(this, _logctx);
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
            }
            if (onceException)
                throw onceException.getLink();
        }
    }

    virtual IRoxieServerContext *createContext(IPropertyTree *context, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, const IRoxieContextLogger &_logctx, XmlReaderOptions _xmlReadFlags) const
    {
        checkSuspended();
        checkOnceDone(_logctx);
        return createRoxieServerContext(context, this, client, isXml, isRaw, isBlocked, httpHelper, trim, priority, _logctx, _xmlReadFlags);
    }

    virtual IRoxieServerContext *createContext(IConstWorkUnit *wu, const IRoxieContextLogger &_logctx) const
    {
        checkSuspended();
        checkOnceDone(_logctx);
        return createWorkUnitServerContext(wu, this, _logctx);
    }

    virtual WorkflowMachine *createWorkflowMachine(bool isOnce, const IRoxieContextLogger &logctx) const
    {
        IPropertyTree *workflow = queryWorkflowTree();
        if (workflow)
        {
            return ::createRoxieWorkflowMachine(workflow, isOnce, logctx);
        }
        else
            return NULL;
    }

    virtual IPropertyTree *getQueryStats(time_t from, time_t to)
    {
        return queryStats->getStats(from, to);
    }

};

IQueryFactory *createServerQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo)
{
    CriticalBlock b(CQueryFactory::queryCreateLock);
    hash64_t hashValue = CQueryFactory::getQueryHash(id, dll, package, stateInfo);
    IQueryFactory *cached = getQueryFactory(hashValue, 0);
    if (cached)
    {
        ::Release(dll);
        return cached;
    }
    Owned<CRoxieServerQueryFactory> newFactory = new CRoxieServerQueryFactory(id, dll, package, hashValue);
    newFactory->load(stateInfo);
    return newFactory.getClear();
}

extern IQueryFactory *createServerQueryFactoryFromWu(const char *wuid)
{
    Owned<const IQueryDll> dll = createWuQueryDll(wuid);
    if (!dll)
        return NULL;
    return createServerQueryFactory(wuid, dll.getClear(), queryRootPackage(), NULL); // MORE - if use a constant for id might cache better?
}

//==============================================================================================================================================

class CSlaveQueryFactory : public CQueryFactory
{
    void addActivity(ISlaveActivityFactory *activity, ActivityArray *activities)
    {
        activities->append(*activity);
        unsigned activityId = activity->queryId();
        allActivities.setValue(activityId, activity);
    }

    void loadSlaveNode(IPropertyTree &node, unsigned subgraphId, ActivityArray *activities)
    {
        ThorActivityKind kind = getActivityKind(node);
        switch (kind)
        {
        case TAKcsvread:
        case TAKxmlread:
        case TAKdiskread:
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
        case TAKremotegraph:
            break;
        case TAKsubgraph:
            break;
        default:
            return;
        }
        ISlaveActivityFactory *newAct = NULL;
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
                    newAct = createRoxieXMLFetchActivityFactory(node, subgraphId, *this, helperFactory);
                    break;
                case TAKkeyedjoin:
                case TAKkeyeddenormalize:
                case TAKkeyeddenormalizegroup:
                    newAct = createRoxieKeyedJoinIndexActivityFactory(node, subgraphId, *this, helperFactory);
                    if (node.getPropBool("att[@name=\"_diskAccessRequired\"]/@value"))
                    {
                        ISlaveActivityFactory *newAct2 = createRoxieKeyedJoinFetchActivityFactory(node, subgraphId, *this, helperFactory);
                        unsigned activityId2 = newAct2->queryId() | ROXIE_ACTIVITY_FETCH;
                        activities->append(*newAct2);
                        allActivities.setValue(activityId2, newAct2);
                    }
                    break;
                case TAKremotegraph:
                    {
                        unsigned graphId = node.getPropInt("att[@name=\"_graphid\"]/@value", 0);
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
            // If the subgraph belongs to a remote activity, we need to be able to execute it on the slave...
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
                loadSlaveNode(node, subgraphId, activities);
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
            loadSlaveNode(node, subgraphId, activities);
        }
        loadSlaveNode(graph, subgraphId, activities); // MORE - not really sure why this line is here!
    }

public:
    CSlaveQueryFactory(const char *_id, const IQueryDll *_dll, const IRoxiePackage &_package, hash64_t _hashValue, unsigned _channelNo)
        : CQueryFactory(_id, _dll, _package, _hashValue, _channelNo)
    {
    }

    virtual IRoxieSlaveContext *createSlaveContext(const SlaveContextLogger &logctx, IRoxieQueryPacket *packet) const
    {
        return ::createSlaveContext(this, logctx, timeLimit, memoryLimit, packet);
    }

    virtual ActivityArray *loadGraph(IPropertyTree &graph, const char *graphName)
    {
        // MORE: common up with loadGraph for the Roxie server..
        bool isLibraryGraph = graph.getPropBool("@library");
        bool isSequential = graph.getPropBool("@sequential");
        ActivityArray *activities = new ActivityArray(isLibraryGraph, false, isLibraryGraph, isSequential);
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
            throw;
        }
        return activities;
    }
};

IQueryFactory *createSlaveQueryFactory(const char *id, const IQueryDll *dll, const IRoxiePackage &package, unsigned channel, const IPropertyTree *stateInfo)
{
    CriticalBlock b(CQueryFactory::queryCreateLock);
    hash64_t hashValue = CQueryFactory::getQueryHash(id, dll, package, stateInfo);
    IQueryFactory *cached = getQueryFactory(hashValue, channel);
    if (cached)
    {
        ::Release(dll);
        return cached;
    }
    Owned<CSlaveQueryFactory> newFactory = new CSlaveQueryFactory(id, dll, package, hashValue, channel);
    newFactory->load(stateInfo);
    return newFactory.getClear();
}

extern IQueryFactory *createSlaveQueryFactoryFromWu(const char *wuid, unsigned channelNo)
{
    Owned<const IQueryDll> dll = createWuQueryDll(wuid);
    if (!dll)
        return NULL;
    return createSlaveQueryFactory(wuid, dll.getClear(), queryRootPackage(), channelNo, NULL);  // MORE - if use a constant for id might cache better?
}

IRecordLayoutTranslator * createRecordLayoutTranslator(const char *logicalName, IDefRecordMeta const * diskMeta, IDefRecordMeta const * activityMeta)
{
    try
    {
        return ::createRecordLayoutTranslator(diskMeta, activityMeta);
    }
    catch (IException *E)
    {
        StringBuffer q, d;
        getRecordMetaAsString(q, activityMeta);
        getRecordMetaAsString(d, diskMeta);
        DBGLOG("Activity: %s", q.str());
        DBGLOG("Disk: %s", d.str());
        StringBuffer m;
        m.appendf("In index %s:", logicalName);
        E->errorMessage(m);
        E->Release();
        DBGLOG("%s", m.str());
        throw MakeStringException(ROXIE_RCD_LAYOUT_TRANSLATOR, "%s", m.str());
    }
}
