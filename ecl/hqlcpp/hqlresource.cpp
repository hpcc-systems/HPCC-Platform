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
#include "platform.h"
#include "jlib.hpp"

#include "hqlexpr.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlthql.hpp"
#include "hqlcatom.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqltcppc.ipp"
#include "hqlttcpp.ipp"

#include "hqlresource.ipp"
#include "../../thorlcr/thorutil/thbufdef.hpp"

#define MINIMAL_CHANGES
#define MAX_INLINE_COMMON_COUNT 5
//#define TRACE_RESOURCING
//#define VERIFY_RESOURCING
//#define SPOT_UNCONDITIONAL_CONDITIONS

#define DEFAULT_LARGEMEM_BUFFER_SIZE (0x58000000) // ~ 1.4GB
#define DEFAULT_MAX_SOCKETS 2000 // configurable by setting max_sockets in .ini
#define DEFAULT_TOTAL_MEMORY ((1024*1024*1800)-DEFAULT_LARGEMEM_BUFFER_SIZE)
#define FIXED_CLUSTER_SIZE 400
#define MEM_Const_Minimal (1*1024*1024)
#define DEFAULT_MAX_ACTIVITIES  100

//=== The following provides information about how each kind of activity is resourced ====

static void setHashResources(IHqlExpression * expr, CResources & resources, const CResourceOptions & options)
{
    unsigned memneeded = MEM_Const_Minimal+resources.clusterSize*4*DISTRIBUTE_SINGLE_BUFFER_SIZE+DISTRIBUTE_PULL_BUFFER_SIZE;
    resources.set(RESslavememory, memneeded).set(REShashdist, 1);
}


//MORE: Use a single function to map an hqlexpression to an activity kind.
void getResources(IHqlExpression * expr, CResources & resources, const CResourceOptions & options)
{
    //MORE: What effect should a child query have?  Definitely the following, but what about other resourcing?
    if (options.isChildQuery || queryHint(expr, lightweightAtom))
    {
        resources.setLightweight();
        return;
    }

    bool isLocal = isLocalActivity(expr);
    bool isGrouped = isGroupedActivity(expr);
    switch (expr->getOperator())
    {
    case no_join:
    case no_selfjoin:
    case no_denormalize:
    case no_denormalizegroup:
        if (isKeyedJoin(expr) || expr->hasAttribute(_lightweight_Atom))
            resources.setLightweight();
        else if (expr->hasAttribute(lookupAtom))
        {
            if (expr->hasAttribute(fewAtom))
            {
                resources.setLightweight().set(RESslavememory, MEM_Const_Minimal+LOOKUPJOINL_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
            else
            {
                resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+LOOKUPJOINL_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
        }
        else if (expr->hasAttribute(hashAtom))
        {
            resources.setHeavyweight();
            setHashResources(expr, resources, options);
        }
        else
        {
            resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+SORT_BUFFER_TOTAL+JOINR_SMART_BUFFER_SIZE);
            if (!isLocal)
            {
#ifndef SORT_USING_MP
                resources.setManyToManySockets(2);
#endif
            }
        }
        break;
    case no_dedup:
        if (isGrouped || (!expr->hasAttribute(allAtom) && !expr->hasAttribute(hashAtom)))
        {
            resources.setLightweight();
            if (!isGrouped && !isLocal)
            {
                resources.set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
                resources.setManyToMasterSockets(1);
            }
        }
        else if (isLocal)
        {
            resources.setHeavyweight().set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
            //This can't be right....
            //resources.setManyToMasterSockets(1);
        }
        else
        {
            //hash dedup.
            resources.setHeavyweight();
            setHashResources(expr, resources, options);
        }
        break;
    case no_rollup:
        resources.setLightweight();
        if (!isGrouped && !isLocal)
        {
            resources.set(RESslavememory, MEM_Const_Minimal+DEDUP_SMART_BUFFER_SIZE);
            //MORE: Is this still correct?
            resources.setManyToMasterSockets(1);
        }
        break;
    case no_distribute:
    case no_keyeddistribute:
        resources.setLightweight();
        setHashResources(expr, resources, options);
        break;
    case no_subsort:
        if (expr->hasAttribute(manyAtom))
            resources.setHeavyweight();
        else
            resources.setLightweight();
        break;
    case no_sort:
        if (isGrouped)
        {
            if (expr->hasAttribute(manyAtom))
                resources.setHeavyweight();
            else
                resources.setLightweight();
        }
        else if (expr->hasAttribute(fewAtom) && isLocal)
            resources.setLightweight();
        else if (isLocal)
            resources.setHeavyweight();
        else
        {
            resources.setHeavyweight();
#ifndef SORT_USING_MP
            resources.setManyToManySockets(2);
#endif
        }
        break;
    case no_topn:
        resources.setLightweight();
        break;
    case no_pipe:
        //surely it should be something like this.
        resources.setLightweight().set(RESslavesocket, 1);
        break;
    case no_table:
        {
            IHqlExpression * mode = expr->queryChild(2);
            if (mode && mode->getOperator() == no_pipe)
            {
                resources.setLightweight().set(RESslavesocket, 1);
            }
            else
            {
                resources.setLightweight();
                if (expr->hasAttribute(_workflowPersist_Atom) && expr->hasAttribute(distributedAtom))
                    setHashResources(expr, resources, options);         // may require a hash distribute
            }
            break;
        }
    case no_output:
        {
            IHqlExpression * filename = expr->queryChild(1);
            if (expr->hasAttribute(_spill_Atom))
            {
                //resources.setLightweight();   // assume no resources(!)
            }
            else if (filename && filename->getOperator() == no_pipe)
            {
                resources.setLightweight().set(RESslavesocket, 1);
            }
            else if (filename && !filename->isAttribute())
            {
                resources.setLightweight();
            }
            else
            {
                resources.setLightweight().set(RESslavememory, WORKUNITWRITE_SMART_BUFFER_SIZE);
            }
            break;
        }
    case no_distribution:
        resources.setLightweight().set(RESmastersocket, 16).set(RESslavesocket, 1);
        break;
    case no_aggregate:
    case no_newaggregate:
        {
            IHqlExpression * grouping = queryRealChild(expr, 3);
            if (grouping)
            {
                //Is this really correct???
                resources.setLightweight();
                setHashResources(expr, resources, options);
            }
            else
            {
                resources.setLightweight();
                //if (!isGrouped)
                //  resources.set(RESmastersocket, 16).set(RESslavesocket, 1);
            }
        }
        break;
    case no_hqlproject:
        resources.setLightweight();
        //Add a flag onto count project to indicate it is a different variety.
        if (expr->hasAttribute(_countProject_Atom) && !isLocal)
            resources.set(RESslavememory, COUNTPROJECT_SMART_BUFFER_SIZE);
        break;
    case no_enth:
        resources.setLightweight();
        if (!isLocal)
            resources.set(RESslavememory, CHOOSESETS_SMART_BUFFER_SIZE);
        break;
    case no_metaactivity:
        if (expr->hasAttribute(pullAtom))
            resources.setLightweight().set(RESslavememory, PULL_SMART_BUFFER_SIZE);
        break;
    case no_setresult:
    case no_extractresult:
    case no_outputscalar:
        resources.setLightweight();//.set(RESmastersocket, 1).set(RESslavesocket, 1);
        break;
    case no_choosesets:
        resources.setLightweight();
        if (!isLocal || expr->hasAttribute(enthAtom) || expr->hasAttribute(lastAtom))
            resources.set(RESslavememory, CHOOSESETS_SMART_BUFFER_SIZE);
        break;
    case no_iterate:
        resources.setLightweight();
        if (!isGrouped && !isLocal)
            resources.setManyToMasterSockets(1).set(RESslavememory, ITERATE_SMART_BUFFER_SIZE);
        break;
    case no_choosen:
        resources.setLightweight().set(RESslavememory, FIRSTN_SMART_BUFFER_SIZE);
        break;
    case no_spill:
    case no_spillgraphresult:
        //assumed to take no resources;
        break;
    case no_addfiles:
    case no_merge:
        {
            resources.setLightweight();
            unsigned bufSize = FUNNEL_PERINPUT_BUFF_SIZE*expr->numChildren();
            if (bufSize < FUNNEL_MIN_BUFF_SIZE) bufSize = FUNNEL_MIN_BUFF_SIZE;
            resources.set(RESslavememory, MEM_Const_Minimal+bufSize);
            break;
        }
    case no_compound:
        //MORE: Should really be the total resources for the lhs...  Really needs more thought.
        break;
    case no_libraryselect:
        //Do not allocate any resources for this, we don't want it to cause a spill under any circumstances
        break;
    case no_split:
        //Should really be included in the cost....
    default:
        resources.setLightweight();
        break;
    }
}


CResources & CResources::setLightweight()                           
{ 
    return set(RESslavememory, 0x10000).set(RESactivities, 1); 
}


CResources & CResources::setHeavyweight()
{ 
    return set(RESslavememory, 0x100000).set(RESheavy, 1).set(RESactivities, 1); 
}


//-------------------------------------------------------------------------------------------


const char * queryResourceName(ResourceType kind)
{
    switch (kind)
    {
    case RESslavememory:    return "Slave Memory";
    case RESslavesocket:    return "Slave Sockets";
    case RESmastermemory:   return "Master Memory";
    case RESmastersocket:   return "Master Sockets";
    case REShashdist:       return "Hash Distributes"; 
    case RESheavy:          return "Heavyweight";
    case RESactivities:     return "Activities";
    }
    return "Unknown";
}

inline ResourcerInfo * queryResourceInfo(IHqlExpression * expr) { return (ResourcerInfo *)expr->queryBody()->queryTransformExtra(); }

inline bool isResourcedActivity(IHqlExpression * expr)
{
    if (!expr)
        return false;
    ResourcerInfo * extra = queryResourceInfo(expr);
    return extra && extra->isResourcedActivity();
}

bool isWorthForcingHoist(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_selectnth:
        case no_filter:
        case no_newaggregate:
            if (isResourcedActivity(expr->queryChild(0)))
                return true;
            break;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}


void CResources::add(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
        resource[i] += other.resource[i];
}

bool CResources::addExceeds(const CResources & other, const CResources & limit) const
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] + other.resource[i] > limit.resource[i])
        {
            //DBGLOG("Cannot merge because limit for %s exceeded (%d cf %d)", queryResourceName((ResourceType)i), resource[i] + other.resource[i], limit.resource[i]);
            return true;
        }
    }
    return false;
}

StringBuffer & CResources::getExceedsReason(StringBuffer & reasonText, const CResources & other, const CResources & limit) const
{
    bool first = true;
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] + other.resource[i] > limit.resource[i])
        {
            if (!first) reasonText.append(", ");
            first = false;
            reasonText.appendf("%s (%d>%d)", queryResourceName((ResourceType)i), resource[i] + other.resource[i], limit.resource[i]);
        }
    }
    return reasonText;
}

bool CResources::exceeds(const CResources & limit) const
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] > limit.resource[i])
            return true;
    }
    return false;
}

void CResources::maximize(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
    {
        if (resource[i] < other.resource[i])
           resource[i] = other.resource[i];
    }
}

CResources & CResources::setManyToMasterSockets(unsigned numSockets)
{
    set(RESslavesocket, numSockets);
    set(RESmastersocket, numSockets * clusterSize);
    return *this;
}

CResources & CResources::setManyToManySockets(unsigned numSockets)
{
    return set(RESslavesocket, numSockets * clusterSize);
}

void CResources::sub(const CResources & other)
{
    for (unsigned i = 0; i < RESmax; i++)
        resource[i] -= other.resource[i];
}

//===========================================================================

inline bool isAffectedByResourcing(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
    case no_constant:
    case no_attr:
        return false;
    }
    return true;
}


bool isSimpleAggregateResult(IHqlExpression * expr)
{
    //MORE: no_extractresult is really what is meant
    if (expr->getOperator() != no_extractresult)
        return false;
    IHqlExpression * value = expr->queryChild(0);
    if (value->getOperator() != no_datasetfromrow)
        return false;
    //MORE: This currently doesn't hoist selects from nested records, but not sure there is a syntax to do that either.
    IHqlExpression * ds = value->queryChild(0);
    if (!isSelectFirstRow(ds))
        return false;
    ds = ds->queryChild(0);
    if (ds->getOperator() != no_newaggregate)
        return false;
    return true;
}

bool lightweightAndReducesDatasetSize(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_hqlproject:
    case no_newusertable:
        return reducesRowSize(expr);
    case no_dedup:
        if (isGroupedActivity(expr) || (!expr->hasAttribute(allAtom) && !expr->hasAttribute(hashAtom)))
            return true;
        break;
    case no_aggregate:
    case no_newaggregate:
        {
            IHqlExpression * grouping = queryRealChild(expr, 3);
            if (grouping)
                return false;
            return true;
        }
    case no_rollupgroup:
    case no_rollup:
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
    case no_filter:
    case no_limit:
    case no_filtergroup:
        return true;
    case no_group:
        //removing grouping will reduce size of the spill file.
        if (expr->queryType()->queryGroupInfo() == NULL)
            return true;
        break;
    }
    return false;
}


bool heavyweightAndReducesSizeOrSkew(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_aggregate:
    case no_newaggregate:
        {
            //more; hash aggregate?
            break;
        }
    case no_distribute:
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

IHqlExpression * CResourceOptions::createSpillName(bool isGraphResult)
{
    if (isGraphResult)
        return getSizetConstant(nextResult++);

    StringBuffer s;
    s.append("~spill::");
    getUniqueId(s);
    return createConstant(s.str());
}

//---------------------------------------------------------------------------

IHqlExpression * appendUniqueAttr(IHqlExpression * expr)
{
    return replaceOwnedProperty(expr, createUniqueId());
}


bool queryAddUniqueToActivity(IHqlExpression * expr)
{
    if (!isSourceActivity(expr))
        return false;
    switch (expr->getOperator())
    {
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_xmlproject:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_rows:
    case no_allnodes:
    case no_thisnode:
    case no_select:         // it can get lost, and that then causes inconsistent trees.
        return false;
    }
    return true;
}

//---------------------------------------------------------------------------

ResourceGraphLink::ResourceGraphLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode, LinkKind _linkKind)
{
    assertex(_sourceGraph);
    sourceGraph.set(_sourceGraph);
    sourceNode.set(_sourceNode);
    sinkGraph.set(_sinkGraph);
    sinkNode.set(_sinkNode);

    assertex(!sinkGraph || sinkNode);
    linkKind = _linkKind;
    trace("create");
}


ResourceGraphLink::~ResourceGraphLink()
{
    trace("delete");
}

void ResourceGraphLink::changeSourceGraph(ResourceGraphInfo * newGraph)
{
    sourceGraph->sinks.zap(*this);
    sourceGraph.set(newGraph);
    newGraph->sinks.append(*this);
    trace("change source");
}

void ResourceGraphLink::changeSinkGraph(ResourceGraphInfo * newGraph)
{
    sinkGraph->sources.zap(*this);
    sinkGraph.set(newGraph);
    newGraph->sources.append(*this);
    trace("change sink");
}


bool ResourceGraphLink::isRedundantLink()
{
    ResourcerInfo * info = queryResourceInfo(sourceNode);
    return info->expandRatherThanSpill(false);
}

void ResourceGraphLink::trace(const char * name)
{
#ifdef TRACE_RESOURCING
    PrintLog("%s: %lx source(%lx,%lx) sink(%lx,%lx) %s", name, this, sourceGraph.get(), sourceNode->queryBody(), sinkGraph.get(), sinkNode ? sinkNode->queryBody() : NULL, 
             linkKind == SequenceLink ? "sequence" : "");
#endif
}



ResourceGraphDependencyLink::ResourceGraphDependencyLink(ResourceGraphInfo * _sourceGraph, IHqlExpression * _sourceNode, ResourceGraphInfo * _sinkGraph, IHqlExpression * _sinkNode)
: ResourceGraphLink(_sourceGraph, _sourceNode, _sinkGraph, _sinkNode, UnconditionalLink)
{
}


void ResourceGraphDependencyLink::changeSourceGraph(ResourceGraphInfo * newGraph)
{
    sourceGraph.set(newGraph);
    trace("change source");
}

void ResourceGraphDependencyLink::changeSinkGraph(ResourceGraphInfo * newGraph)
{
    sinkGraph.set(newGraph);
    newGraph->dependsOn.append(*this);
    trace("change sink");
}


//---------------------------------------------------------------------------

ResourceGraphInfo::ResourceGraphInfo(CResourceOptions * _options) : resources(_options->clusterSize)
{
    options = _options;
    depth = 0;
    depthSequence = -1;
    beenResourced = false;
    isUnconditional = false;
    mergedConditionSource = false;
    hasConditionSource = false;
    hasSequentialSource = false;
    isDead = false;
    startedGeneratingResourced = false;
    inheritedExpandedDependencies = false;
    cachedDependent.other = NULL;
}

ResourceGraphInfo::~ResourceGraphInfo()
{
}

bool ResourceGraphInfo::addCondition(IHqlExpression * condition)
{
    if (conditions.find(*condition) == NotFound)
    {
        conditions.append(*LINK(condition));

#ifdef SPOT_UNCONDITIONAL_CONDITIONS
        IAtom * name = condition->queryName();
        IAtom * invName = NULL;
        if (name == trueAtom)
            invName = falseAtom;
        else if (name == falseAtom)
            invName = trueAtom;
        
        if (invName)
        {
            IHqlExpression * parent = condition->queryChild(1);
            OwnedHqlExpr invTag = createAttribute(invName, LINK(condition->queryChild(0)), LINK(parent));
            if (conditions.find(*invTag) != NotFound)
            {
                if (!parent)
                    return true;
                return addCondition(parent);
            }
        }
#endif

    }
    return false;
}

bool ResourceGraphInfo::isSharedInput(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (unbalancedExternalSources.contains(*body))
        return false;
    if (queryResourceInfo(expr)->expandRatherThanSplit())
        return false;
    unsigned numUses = 0;
    ForEachItemIn(i, balancedExternalSources)
    {
        if (body == &balancedExternalSources.item(i))
            numUses++;
    }
    //NumUses could be zero if an input should be expanded, and that input is shared by another graph which also expands
    //the input.  E.g. project(meta(diskread).
    return numUses > 1;
}

void ResourceGraphInfo::addSharedInput(IHqlExpression * expr, IHqlExpression * mapped)
{
    sharedInputs.append(*LINK(expr));
    sharedInputs.append(*LINK(mapped));
}

IHqlExpression * ResourceGraphInfo::queryMappedSharedInput(IHqlExpression * expr)
{
    unsigned max = sharedInputs.ordinality();
    for (unsigned i=0; i < max; i+= 2)
    {
        if (expr == &sharedInputs.item(i))
            return &sharedInputs.item(i+1);
    }
    return NULL;
}

bool ResourceGraphInfo::allocateResources(const CResources & value, const CResources & limit)
{
    if (resources.addExceeds(value, limit))
        return false;
    resources.add(value);
    return true;
}


bool ResourceGraphInfo::containsActiveSinks()
{
    ForEachItemIn(idx, sinks)
    {
        ResourcerInfo * info = queryResourceInfo(sinks.item(idx).sourceNode);
        if (!info->expandRatherThanSpill(false))
            return true;
    }
    return false;
}

void ResourceGraphInfo::display()
{
    StringBuffer s;
    s.appendf("graph %p src(", this);
    ForEachItemIn(idxs, sources)
        s.appendf("%p ", sources.item(idxs).sourceGraph.get());
    s.append(") dep(");
    ForEachItemIn(idxd, dependsOn)
        s.appendf("%p ", dependsOn.item(idxd).sourceGraph.get());
    s.append(")");
    if (isUnconditional)
        s.append(" <unconditional>");
    DBGLOG("%s", s.str());
}

void ResourceGraphInfo::getMergeFailReason(StringBuffer & reasonText, ResourceGraphInfo * otherGraph, const CResources & limit)
{
    resources.getExceedsReason(reasonText, otherGraph->resources, limit);
}

unsigned ResourceGraphInfo::getDepth()
{
    //If no graphs have been merged since this was last called it is still valid.
    if (depthSequence == options->state.updateSequence)
        return depth;

    depthSequence = options->state.updateSequence;
    depth = 0;
    ForEachItemIn(idx, dependsOn)
    {
        ResourceGraphInfo * source = dependsOn.item(idx).sourceGraph;
        if (source->getDepth() >= depth)
            depth = source->getDepth() + 1;
    }
    ForEachItemIn(idx2, sources)
    {
        ResourceGraphInfo * source = sources.item(idx2).sourceGraph;
        if (source->getDepth() >= depth)
            depth = source->getDepth() + 1;
    }
    return depth;
}

bool ResourceGraphInfo::hasSameConditions(ResourceGraphInfo & other)
{
    if (conditions.ordinality() != other.conditions.ordinality())
        return false;
    ForEachItemIn(i, conditions)
        if (other.conditions.find(conditions.item(i)) == NotFound)
            return false;
    return true;
}

bool ResourceGraphInfo::evalDependentOn(ResourceGraphInfo & other, bool ignoreSources)
{
    ForEachItemIn(idx, dependsOn)
    {
        ResourceGraphInfo * cur = dependsOn.item(idx).sourceGraph;
        if (cur == &other)
            return true;
        if (cur->isDependentOn(other, false))
            return true;
    }
    ForEachItemIn(idx2, sources)
    {
        ResourceGraphInfo * cur = sources.item(idx2).sourceGraph;
        if ((cur == &other) && !ignoreSources)
            return true;
        if (cur->isDependentOn(other, false))
            return true;
    }
    return false;
}


bool ResourceGraphInfo::isDependentOn(ResourceGraphInfo & other, bool ignoreSources)
{
    //Cache the last query so that traversal doesn't convert a graph into a tree walk
    if ((cachedDependent.other == &other) && (cachedDependent.ignoreSources == ignoreSources) &&
        (cachedDependent.updateSequence == options->state.updateSequence))
        return cachedDependent.value;

    if (getDepth() <= other.getDepth())
        return false;

    cachedDependent.other = &other;
    cachedDependent.ignoreSources = ignoreSources;
    cachedDependent.updateSequence = options->state.updateSequence;
    cachedDependent.value = evalDependentOn(other, ignoreSources);
    return cachedDependent.value;
}


bool ResourceGraphInfo::isVeryCheap()
{
    if (sinks.ordinality() != 1)
        return false;

    IHqlExpression * sourceNode = sinks.item(0).sourceNode;
    if (isSimpleAggregateResult(sourceNode))
        return true;

//  Not sure about the following....
//  if (sourceNode->getOperator() == no_setresult)
//      return true;

    //Could be other examples...
    return false;
}


bool ResourceGraphInfo::mergeInSource(ResourceGraphInfo & other, const CResources & limit)
{
    bool mergeConditions = false;
    if (!isUnconditional)
    {
        //if it is conditional and it is very cheap then it is still more efficient to merge 
        //rather than read from a spill file.
        if (other.isUnconditional || !hasSameConditions(other))
        {
            if ((hasConditionSource || !isVeryCheap()) && (other.sinks.ordinality() != 1))
                return false;
            mergeConditions = true;
        }
    }

    if (isDependentOn(other, true))
        return false;

    if (!options->canSplit())
    {
        //Don't merge two graphs that will cause a splitter to be created
        //Either already used internally, or an output will be merged twice
        HqlExprArray mergeNodes;
        ForEachItemIn(idx, sources)
        {
            ResourceGraphLink & cur = sources.item(idx);
            if (cur.sourceGraph == &other)
            {
                IHqlExpression * sourceNode = cur.sourceNode->queryBody();
                ResourcerInfo * info = queryResourceInfo(sourceNode);
                if ((info->numInternalUses() != 0) || (mergeNodes.find(*sourceNode) != NotFound) || info->preventMerge())
                    return false;
                mergeNodes.append(*LINK(sourceNode));
            }
        }
    }

    if (options->checkResources() && !allocateResources(other.resources, limit))
        return false;

    if (hasSequentialSource && other.hasSequentialSource)
        return false;

    mergeGraph(other, mergeConditions);
    return true;
}

void ResourceGraphInfo::mergeGraph(ResourceGraphInfo & other, bool mergeConditions)
{
#ifdef TRACE_RESOURCING
    DBGLOG("Merging%s source into%s sink", other.isUnconditional ? "" : " conditional", isUnconditional ? "" : " conditional");
    other.display();
    display();
    PrintLog("Merge %p into %p", &other, this);
#endif

    if (other.hasConditionSource)
        hasConditionSource = true;

    if (other.hasSequentialSource)
    {
        assertex(!hasSequentialSource);
        hasSequentialSource = true;
    }

    //Recalculate the dependents, because sources of the source merged in may no longer be indirect
    //although they may be via another path.  
    options->noteGraphsChanged();

    //If was very cheap and merged into an unconditional graph, make sure this is now tagged as
    //unconditional...
    if (other.isUnconditional)
        isUnconditional = true;

    //We need to stop spills being an arm of a conditional branch - otherwise they won't get executed.
    //so see if we have merged any conditional branches in
    if (other.mergedConditionSource)
        mergedConditionSource = true;

    if (mergeConditions)
    {
        //Replace conditions with those of parent.  Only called when a very simple graph is
        //merged in, so replace conditions instead of merging
        conditions.kill();
        ForEachItemIn(i, other.conditions)
            conditions.append(OLINK(other.conditions.item(i)));
    }
    
    //sources and sinks are updated elsewhere...
}


bool ResourceGraphInfo::mergeInSibling(ResourceGraphInfo & other, const CResources & limit)
{
    if ((!isUnconditional || !other.isUnconditional) && !hasSameConditions(other))
        return false;

    if (hasSequentialSource && other.hasSequentialSource)
        return false;

    if (isDependentOn(other, false) || other.isDependentOn(*this, false))
        return false;

    if (options->checkResources() && !allocateResources(other.resources, limit))
        return false;

    mergeGraph(other, false);
    return true;
}


void ResourceGraphInfo::removeResources(const CResources & value)
{
    resources.sub(value);
}



//---------------------------------------------------------------------------

unsigned ChildDependentArray::findOriginal(IHqlExpression * expr)
{
    ForEachItem(i)
    {
        if (item(i).original == expr)
            return i;
    }
    return NotFound;
}
//---------------------------------------------------------------------------

static void appendCloneProperty(HqlExprArray & args, IHqlExpression * expr, IAtom * name)
{
    IHqlExpression * prop = expr->queryAttribute(name);
    if (prop)
        args.append(*LINK(prop));
}

ResourcerInfo::ResourcerInfo(IHqlExpression * _original, CResourceOptions * _options) 
{ 
    original.set(_original); 
    numUses = 0;
    numExternalUses = 0;
    gatheredDependencies = false;
    containsActivity = false;
    isActivity = false;
    transformed = NULL;
    conditionSourceCount = 0;
    pathToExpr = PathUnknown;
    outputToUseForSpill = NULL;
    isAlreadyInScope = false;
    isSpillPoint = false;
    options = _options;
    balanced = true;
    currentSource = 0;
    linkedFromChild = false;
    neverSplit = false;
    isConditionalFilter = false;
    projectResult = true; // projected must also be non empty to actually project this dataset
    visited = false;
}

void ResourcerInfo::setConditionSource(IHqlExpression * condition, bool isFirst)            
{ 
    if (isFirst)
    {
        conditionSourceCount++; 
        graph->hasConditionSource = true;
    }
}

bool ResourcerInfo::addCondition(IHqlExpression * condition)
{
    conditions.append(*LINK(condition));
    return graph->addCondition(condition);
}

void ResourcerInfo::addProjected(IHqlExpression * next)
{
    if (projectResult && !projected.contains(*next))
        projected.append(*LINK(next));
}

void ResourcerInfo::clearProjected()
{
    projectResult = false;
    projected.kill();
}


void ResourcerInfo::addSpillFlags(HqlExprArray & args, bool isRead)
{
    IHqlExpression * grouping = (IHqlExpression *)original->queryType()->queryGroupInfo();
    if (grouping)
        args.append(*createAttribute(groupedAtom));

    if (outputToUseForSpill)
    {
        assertex(isRead);
        appendCloneProperty(args, outputToUseForSpill, __compressed__Atom);
        appendCloneProperty(args, outputToUseForSpill, jobTempAtom);
        appendCloneProperty(args, outputToUseForSpill, _spill_Atom);
    }
    else
    {
        args.append(*createAttribute(__compressed__Atom));
        args.append(*createAttribute(_spill_Atom));
        args.append(*createAttribute(_noReplicate_Atom));
        if (options->targetRoxie())
            args.append(*createAttribute(_noAccess_Atom));
    }

    if (isRead)
    {
        args.append(*createAttribute(_noVirtual_Atom));         // Don't interpret virtual fields as virtual...
        if (!original->isDataset() || hasSingleRow(original))
            args.append(*createAttribute(rowAtom));                 // add a flag so the selectnth[1] can be removed later...
    }
    else
    {
        unsigned numUses = numExternalUses;
        if (false) // options->cloneConditionals)
        {
            //Remove all the links from conditional graphs...
            ForEachItemIn(i1, graph->sinks)
            {
                ResourceGraphLink & cur = graph->sinks.item(i1);
                if ((cur.sourceNode == original) && !cur.sinkGraph->isUnconditional)
                    numUses--;
            }
            numUses += calcNumConditionalUses();
        }
        args.append(*createAttribute(_tempCount_Atom, getSizetConstant(numUses)));
    }
}

bool ResourcerInfo::alwaysExpand()
{
    return (original->getOperator() == no_mapto);
}

static IHqlExpression * stripTrueFalse(IHqlExpression * expr)
{
    if (expr->queryName() == instanceAtom)
    {
        IHqlExpression * parent = expr->queryChild(2);
        if (parent)
            parent = stripTrueFalse(parent);
        return createAttribute(instanceAtom, LINK(expr->queryChild(0)), LINK(expr->queryChild(1)), parent);
    }
    else
    {
        IHqlExpression * parent = expr->queryChild(1);
        if (parent && parent->isAttribute())
            parent = stripTrueFalse(parent);
        return createAttribute(tempAtom, LINK(expr->queryChild(0)), parent);
    }
}

unsigned ResourcerInfo::calcNumConditionalUses()
{
    //for thor and hthor, where the conditional graph is cloned, the maximum number of times it can be read
    //is 1 for a shared conditional graph, or once for each combination of conditions it is used as the
    //input for.  However if it is used more than once in a single branch of a condition it needs to be counted
    //several times.
    //It is always better to overestimate than underestimate. (But even better to get it right.)
    HqlExprArray uniqueConditions;
    UnsignedArray uniqueCounts;
    ForEachItemIn(idx, conditions)
    {
        IHqlExpression & cur = conditions.item(idx);
        OwnedHqlExpr unique = stripTrueFalse(&cur);
        unsigned numOccurences = 0;
        ForEachItemIn(j, conditions)
            if (&conditions.item(j) == &cur)
                numOccurences++;

        unsigned match = uniqueConditions.find(*unique);
        if (match == NotFound)
        {
            uniqueConditions.append(*unique.getClear());
            uniqueCounts.append(numOccurences);
        }
        else
        {
            if (uniqueCounts.item(match) < numOccurences)
                uniqueCounts.replace(numOccurences, match);
        }
    }

    unsigned condUses = 0;
    ForEachItemIn(k, uniqueCounts)
        condUses += uniqueCounts.item(k);
    return condUses;
}


// Each aggregate has form.  Setresult(select(selectnth(aggregate...,1),field),name,seq)
IHqlExpression * ResourcerInfo::createAggregation(IHqlExpression * expr)
{
    LinkedHqlExpr transformed = expr;
    ForEachItemIn(idx, aggregates)
    {
        IHqlExpression & cur = aggregates.item(idx);
        IHqlExpression * row2ds = cur.queryChild(0);
        IHqlExpression * selectnth = row2ds->queryChild(0);
        IHqlExpression * aggregate = selectnth->queryChild(0);
        IHqlExpression * aggregateRecord = aggregate->queryChild(1);
        assertex(aggregate->getOperator() == no_newaggregate);
        HqlExprArray aggargs, setargs;
        //Through aggregate has form (dataset, record, transform, list of set results);
        aggargs.append(*LINK(transformed));
        aggargs.append(*LINK(aggregateRecord));
        IHqlExpression * mapped = replaceSelector(aggregate->queryChild(2), original, expr->queryNormalizedSelector());
        aggargs.append(*mapped);

        OwnedHqlExpr active = createDataset(no_anon, LINK(aggregateRecord), NULL);
        OwnedHqlExpr mappedSelect = replaceSelector(cur.queryChild(1), row2ds, active);
        setargs.append(*LINK(active));
        setargs.append(*LINK(mappedSelect));
        unwindChildren(setargs, &cur, 1);
        aggargs.append(*createValue(no_extractresult, makeVoidType(), setargs));
        transformed.setown(createDataset(no_throughaggregate, aggargs));
    }

    return transformed.getClear();
}

bool ResourcerInfo::useGraphResult()
{
    if (!options->useGraphResults)
        return false;

    if (linkedFromChild)
        return true;
    //Roxie converts spills into splitters, so best to retain them
    if (options->targetClusterType == RoxieCluster)
        return false;
    return true;
}

bool ResourcerInfo::useGlobalResult()
{
    return (linkedFromChild && !useGraphResult());
}

IHqlExpression * ResourcerInfo::createSpillName()
{
    if (outputToUseForSpill)
        return LINK(outputToUseForSpill->queryChild(1));
    if (!spillName)
    {
        if (useGraphResult())
            spillName.setown(options->createSpillName(true));
        else if (useGlobalResult())
        {
            StringBuffer valueText;
            getUniqueId(valueText.append("spill"));
            spillName.setown(createConstant(valueText.str()));
        }
        else
            spillName.setown(options->createSpillName(false));
    }
    return LINK(spillName);
}


IHqlExpression * ResourcerInfo::createSpilledRead(IHqlExpression * spillReason)
{
    OwnedHqlExpr dataset;
    HqlExprArray args;

    IHqlExpression * record = original->queryRecord();
    bool loseDistribution = true;
    if (useGraphResult())
    {
        args.append(*LINK(record));
        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        if (isGrouped(original))
            args.append(*createAttribute(groupedAtom));
        if (!original->isDataset())
            args.append(*createAttribute(rowAtom));
        args.append(*createAttribute(_spill_Atom));
        IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
        if (recordCountAttr)
            args.append(*LINK(recordCountAttr));
        if (options->targetThor() && original->isDataset() && !options->isChildQuery)
            args.append(*createAttribute(_distributed_Atom));
        if (original->isDictionary())
            dataset.setown(createDictionary(no_getgraphresult, args));
        else
            dataset.setown(createDataset(no_getgraphresult, args));
        loseDistribution = false;
    }
    else if (useGlobalResult())
    {
        args.append(*LINK(record));
        args.append(*createAttribute(nameAtom, createSpillName()));
        args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
        addSpillFlags(args, true);
        IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
        if (recordCountAttr)
            args.append(*LINK(recordCountAttr));
        if (original->isDictionary())
            dataset.setown(createDictionary(no_workunit_dataset, args));
        else
            dataset.setown(createDataset(no_workunit_dataset, args));
    }
    else
    {
        if (spilledDataset)
        {
            args.append(*LINK(spilledDataset));
            args.append(*createSpillName());
        }
        else
        {
            args.append(*createSpillName());
            args.append(*LINK(record));
        }
        args.append(*createValue(no_thor));
        addSpillFlags(args, true);
        args.append(*createUniqueId());
        if (options->isChildQuery && options->targetRoxie())
        {
            args.append(*createAttribute(_colocal_Atom));
            args.append(*createLocalAttribute());
        }
        if (spillReason)
            args.append(*LINK(spillReason));

        if (spilledDataset)
            dataset.setown(createDataset(no_readspill, args));
        else
        {
            IHqlExpression * recordCountAttr = queryRecordCountInfo(original);
            if (recordCountAttr)
                args.append(*LINK(recordCountAttr));
            dataset.setown(createDataset(no_table, args));
        }

        loseDistribution = false;
    }

    dataset.setown(preserveTableInfo(dataset, original, loseDistribution, NULL));
    return wrapRowOwn(dataset.getClear());
}

IHqlExpression * ResourcerInfo::createSpilledWrite(IHqlExpression * transformed)
{
    assertex(!outputToUseForSpill);
    HqlExprArray args;

    if (useGraphResult())
    {
        assertex(options->graphIdExpr);
        args.append(*LINK(transformed));
        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        args.append(*createAttribute(_spill_Atom));
        return createValue(no_setgraphresult, makeVoidType(), args);
    }
    else if (useGlobalResult())
    {
        args.append(*LINK(transformed));
        args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
        args.append(*createAttribute(namedAtom, createSpillName()));
        addSpillFlags(args, false);
        return createValue(no_output, makeVoidType(), args);
    }
    else
    {
        if (options->createSpillAsDataset)
        {
            IHqlExpression * value = LINK(transformed);
            if (value->isDatarow())
                value = createDatasetFromRow(value);
            spilledDataset.setown(createDataset(no_commonspill, value));
            args.append(*LINK(spilledDataset));
        }
        else
            args.append(*LINK(transformed));

        args.append(*createSpillName());
        addSpillFlags(args, false);
        if (options->createSpillAsDataset)
            return createValue(no_writespill, makeVoidType(), args);

        return createValue(no_output, makeVoidType(), args);
    }
}


bool ResourcerInfo::okToSpillThrough()
{
    bool isGraphResult = useGraphResult();
    if (isGraphResult)
        return options->allowThroughResult;

    if (useGlobalResult())
        return false;

    return (options->allowThroughSpill && !options->createSpillAsDataset);
}

bool ResourcerInfo::spillSharesSplitter()
{
    if (outputToUseForSpill || useGraphResult() || useGlobalResult())
        return false;
    if (okToSpillThrough())
        return false;
    if (!isSplit() || !balanced)
        return false;
    return true;
}

IHqlExpression * ResourcerInfo::createSpiller(IHqlExpression * transformed, bool reuseSplitter)
{
    if (outputToUseForSpill)
        return LINK(transformed);

    if (!okToSpillThrough())
    {
        OwnedHqlExpr split;
        if (reuseSplitter)
        {
            assertex(transformed->getOperator() == no_split);
            split.set(transformed);
        }
        else
        {
            if (transformed->isDataset())
                split.setown(createDataset(no_split, LINK(transformed), createComma(createAttribute(balancedAtom), createUniqueId())));
            else
                split.setown(createRow(no_split, LINK(transformed), createComma(createAttribute(balancedAtom), createUniqueId())));
            split.setown(cloneInheritedAnnotations(original, split));
        }

        splitterOutput.setown(createSpilledWrite(split));
        return split.getClear();
    }

    HqlExprArray args;
    node_operator op;
    args.append(*LINK(transformed));
    if (useGraphResult())
    {
        op = no_spillgraphresult;
        args.append(*LINK(options->graphIdExpr));
        args.append(*createSpillName());
        args.append(*createAttribute(_spill_Atom));
    }
    else
    {
        op = no_spill;
        args.append(*createSpillName());
        addSpillFlags(args, false);
    }


    OwnedHqlExpr spill;
    if (original->isDatarow())
        spill.setown(createRow(op, args));
    else
        spill.setown(createDataset(op, args));
    return cloneInheritedAnnotations(original, spill);
}

IHqlExpression * ResourcerInfo::createSplitter(IHqlExpression * transformed)
{
    if (transformed->getOperator() == no_libraryscopeinstance)
        return LINK(transformed);

    IHqlExpression * attr = createUniqueId();
    if (balanced)
        attr = createComma(attr, createAttribute(balancedAtom));
    OwnedHqlExpr split;
    if (transformed->isDataset())
        split.setown(createDataset(no_split, LINK(transformed), attr));
    else
        split.setown(createRow(no_split, LINK(transformed), attr));
    return cloneInheritedAnnotations(original, split);
}

IHqlExpression * ResourcerInfo::createTransformedExpr(IHqlExpression * expr)
{
    LinkedHqlExpr transformed = expr;
    if (aggregates.ordinality())
        transformed.setown(createAggregation(transformed));

    if (spillSharesSplitter())
    {
        transformed.setown(createSplitter(transformed));

        if (isExternalSpill())
            transformed.setown(createSpiller(transformed, true));
    }
    else
    {
        if (isExternalSpill())
            transformed.setown(createSpiller(transformed, false));
        if (isSplit())
            transformed.setown(createSplitter(transformed));
        else if (isSpilledWrite())
            transformed.setown(createSpilledWrite(transformed));
    }

    return transformed.getClear();
}

bool ResourcerInfo::expandRatherThanSpill(bool noteOtherSpills)
{
    if (options->shareDontExpand)
        return expandRatherThanSplit();

    IHqlExpression * expr = original;
    if (!options->minimiseSpills || linkedFromChild)
        noteOtherSpills = false;

    if (noteOtherSpills)
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->isSpilledWrite())
            return (info->transformed == NULL);
    }
    bool isFiltered = false;
    bool isProcessed = false;
    loop
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->neverSplit)
            return true;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_table:
            {
                //This is only executed for hthor/thor.  Roxie has used expandRatherThanSplit().
                //We need to balance the saving from reading reduced data in the other branches with the cost of
                //writing the spill file to disk.
                if (isFiltered && (numExternalUses >= options->filteredSpillThreshold))
                    return false;
                IHqlExpression * mode = expr->queryChild(2);
                switch (mode->getOperator())
                {
                case no_thor: case no_flat:
                    //MORE: The following is possibly better - but roxie should be able to read from non spill data files in child queries fine
                    //if ((options->targetClusterType == RoxieCluster) && linkedFromChild)) return false;
                    return true;
                default:
                    return false;
                }
            }
        case no_stepped:
            return true;
        case no_inlinetable:
            {
                IHqlExpression * transforms = expr->queryChild(0);
                //The inline table code means this should generate smaller code, and more efficient
                if (!isFiltered && !isProcessed && transforms->isConstant())
                    return true;

                if (transforms->numChildren() > MAX_INLINE_COMMON_COUNT)
                    return false;
                return true;
            }
        case no_getresult:
        case no_temptable:
        case no_rows:
        case no_xmlproject:
        case no_workunit_dataset:
            return !isProcessed && !isFiltered;
        case no_getgraphresult:
            return !expr->hasAttribute(_streaming_Atom);         // we must not duplicate streaming inputs!
        case no_keyindex:
        case no_newkeyindex:
            if (!isFiltered)
                return true;
            return options->cloneFilteredIndex;
        case no_datasetfromrow:
            if (getNumActivityArguments(expr) == 0)
                return true;
            return false;
        case no_fail:
        case no_null:
            return !expr->isAction();
        case no_assertsorted:
        case no_sorted:
        case no_grouped:
        case no_distributed:
        case no_preservemeta:
        case no_nofold:
        case no_nohoist:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            expr = expr->queryChild(0);
            break;
        case no_newusertable:
        case no_limit:
        case no_keyedlimit:
            expr = expr->queryChild(0);
            isProcessed = true;
            break;
        case no_hqlproject:
            if (expr->hasAttribute(_countProject_Atom) || expr->hasAttribute(prefetchAtom))
                return false;
            expr = expr->queryChild(0);
            isProcessed = true;
            break;
            //MORE: Not so sure about all the following, include them so behaviour doesn't change 
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_datasetfromdictionary:
            expr = expr->queryChild(0);
            break;
        case no_filter:
            isFiltered = true;
            expr = expr->queryChild(0);
            break;
        case no_select:
            {
                if (options->targetClusterType == RoxieCluster)
                    return false;

                if (!isNewSelector(expr))
                    return true;
                expr = expr->queryChild(0);
                break;
            }
        default:
            return false;
        }

        //The following reduces the number of spills by taking into account other spills.
        if (noteOtherSpills)
        {
            ResourcerInfo * info = queryResourceInfo(expr);
            if (info)
            {
                if (info->isSpilledWrite())
                    return (info->transformed == NULL);
                if (info->numExternalUses)
                {
                    if (isFiltered && (numExternalUses >= options->filteredSpillThreshold))
                        return false;
                    return true;
                }
            }
        }
    }
}

bool ResourcerInfo::expandRatherThanSplit()
{
    //MORE: This doesn't really work - should do indexMatching first.
    //should only expand if one side that uses this is also filtered
    IHqlExpression * expr = original;
    loop
    {
        ResourcerInfo * info = queryResourceInfo(expr);
        if (info && info->neverSplit)
            return true;
        switch (expr->getOperator())
        {
        case no_keyindex:
        case no_newkeyindex:
        case no_rowset:
        case no_getgraphloopresultset:
            return true;
        case no_null:
            return !expr->isAction();
        case no_inlinetable:
            if (options->expandSingleConstRow && hasSingleRow(expr))
            {
                IHqlExpression * values = expr->queryChild(0);
                if (values->queryChild(0)->isConstant())
                    return true;
            }
            return false;
        case no_stepped:
        case no_rowsetrange:
        case no_rowsetindex:
            return true;
        case no_sorted:
        case no_grouped:
        case no_distributed:
        case no_preservemeta:
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            break;
        case no_select:
            if (options->targetClusterType == RoxieCluster)
                return false;
            if (!isNewSelector(expr))
            {
                if (!hasLinkCountedModifier(expr))
                    return false;
                return true;
            }
            break;
        case no_rows:
            //If executing in a child query then you'll have less thread contention if the iterator is duplicated
            //So should probably uncomment the following.
            //return true;
            return false;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

bool neverCommonUp(IHqlExpression * expr)
{
    loop
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_keyindex:
        case no_newkeyindex:
            return true;
        case no_filter:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}


bool ResourcerInfo::neverCommonUp()
{
    return ::neverCommonUp(original);
}

bool ResourcerInfo::isExternalSpill()
{
    if (expandRatherThanSpill(true) || (numInternalUses() == 0))
        return false;
    return (numExternalUses != 0);
}

bool ResourcerInfo::isSplit()
{ 
    return numSplitPaths() > 1; 
}

unsigned ResourcerInfo::numSplitPaths()
{
    unsigned internal = numInternalUses();
    if ((internal == 0) || !options->allowSplitBetweenSubGraphs)
        return internal;
    //MORE
    return internal;
}


bool ResourcerInfo::isSpilledWrite()
{
    if (numInternalUses() == 0)
        return true;
    return false;
}


IHqlExpression * ResourcerInfo::wrapRowOwn(IHqlExpression * expr)
{
    if (!original->isDataset() && !original->isDictionary())
        expr = createRow(no_selectnth, expr, getSizetConstant(1));
    return expr;
}

//---------------------------------------------------------------------------

EclResourcer::EclResourcer(IErrorReceiver * _errors, IConstWorkUnit * _wu, ClusterType _targetClusterType, unsigned _clusterSize, const HqlCppOptions & _translatorOptions)
{ 
    wu.set(_wu);
    errors = _errors;
    lockTransformMutex(); 
    targetClusterType = _targetClusterType; 
    clusterSize = _clusterSize ? _clusterSize : FIXED_CLUSTER_SIZE;
    insideNeverSplit = false;
    insideSteppedNeverSplit = false;
    sequential = false;
    options.minimizeSpillSize = _translatorOptions.minimizeSpillSize;

    unsigned totalMemory = _translatorOptions.resourceMaxMemory ? _translatorOptions.resourceMaxMemory : DEFAULT_TOTAL_MEMORY;
    unsigned maxSockets = _translatorOptions.resourceMaxSockets ? _translatorOptions.resourceMaxSockets : DEFAULT_MAX_SOCKETS;
    unsigned maxActivities = _translatorOptions.resourceMaxActivities ? _translatorOptions.resourceMaxActivities : DEFAULT_MAX_ACTIVITIES;
    unsigned maxHeavy = _translatorOptions.resourceMaxHeavy;
    unsigned maxDistribute = _translatorOptions.resourceMaxDistribute;

    resourceLimit = new CResources(0);
    resourceLimit->set(RESactivities, maxActivities);
    switch (targetClusterType)
    {
    case ThorLCRCluster:
        resourceLimit->set(RESheavy, maxHeavy).set(REShashdist, maxDistribute);
        resourceLimit->set(RESmastersocket, maxSockets).set(RESslavememory,totalMemory);
        resourceLimit->set(RESslavesocket, maxSockets).set(RESmastermemory, totalMemory);
        break;
    default:
        resourceLimit->set(RESheavy, 0xffffffff).set(REShashdist, 0xffffffff);
        resourceLimit->set(RESmastersocket, 0xffffffff).set(RESslavememory, 0xffffffff);
        resourceLimit->set(RESslavesocket, 0xffffffff).set(RESmastermemory, 0xffffffff);
        clusterSize = 1;
        break;
    }

    if (_translatorOptions.unlimitedResources)
    {
        resourceLimit->set(RESheavy, 0xffffffff).set(REShashdist, 0xffffffff);
        resourceLimit->set(RESmastersocket, 0xffffffff).set(RESslavememory,0xffffffff);
        resourceLimit->set(RESslavesocket, 0xffffffff).set(RESmastermemory,0xffffffff);
    }


    options.isChildQuery = false;
    options.targetClusterType = targetClusterType;
    options.filteredSpillThreshold = _translatorOptions.filteredReadSpillThreshold;
    options.allowThroughSpill = _translatorOptions.allowThroughSpill;
    options.allowThroughResult = (targetClusterType != RoxieCluster) && (targetClusterType != ThorLCRCluster);
    options.cloneFilteredIndex = (targetClusterType != RoxieCluster);
    options.spillSharedConditionals = (targetClusterType == RoxieCluster);
    options.shareDontExpand = (targetClusterType == RoxieCluster);
    options.graphIdExpr = NULL;
    //MORE The following doesn't always work - it gets sometimes confused about spill files - see latestheaderbuild for an example.
    //Try again once cloneConditionals is false for thor
    options.minimiseSpills = _translatorOptions.minimiseSpills;
    spillMultiCondition = _translatorOptions.spillMultiCondition;
    spotThroughAggregate = _translatorOptions.spotThroughAggregate && (targetClusterType != RoxieCluster) && (targetClusterType != ThorLCRCluster);
    options.noConditionalLinks = (targetClusterType == RoxieCluster);
    options.hoistResourced = _translatorOptions.hoistResourced;
    options.useGraphResults = false;        // modified by later call
    options.groupedChildIterators = _translatorOptions.groupedChildIterators;
    options.allowSplitBetweenSubGraphs = false;//(targetClusterType == RoxieCluster);
    options.clusterSize = clusterSize;
    options.preventKeyedSplit = _translatorOptions.preventKeyedSplit;
    options.preventSteppedSplit = _translatorOptions.preventSteppedSplit;
    options.minimizeSkewBeforeSpill = _translatorOptions.minimizeSkewBeforeSpill;
    options.expandSingleConstRow = true;
    options.createSpillAsDataset = _translatorOptions.optimizeSpillProject && (targetClusterType != HThorCluster);
    options.combineSiblings = _translatorOptions.combineSiblingGraphs && (targetClusterType != HThorCluster) && (targetClusterType != RoxieCluster);
    options.optimizeSharedInputs = _translatorOptions.optimizeSharedGraphInputs && options.combineSiblings;
}

EclResourcer::~EclResourcer()               
{ 
    delete resourceLimit;
    unlockTransformMutex(); 
}

void EclResourcer::setChildQuery(bool value) 
{ 
    options.isChildQuery = value; 
    if (value)
        options.createSpillAsDataset = false;
}

void EclResourcer::setNewChildQuery(IHqlExpression * graphIdExpr, unsigned numResults) 
{ 
    options.graphIdExpr = graphIdExpr; 
    options.nextResult = numResults;
}

void EclResourcer::changeGraph(IHqlExpression * expr, ResourceGraphInfo * newGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    info->graph.set(newGraph);
    ForEachItemInRev(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);
        if (cur.sourceNode == expr)
            cur.changeSourceGraph(newGraph);
        else if (cur.sinkNode == expr)
            cur.changeSinkGraph(newGraph);
        assertex(cur.sinkGraph != cur.sourceGraph);
    }
}


ResourceGraphInfo * EclResourcer::createGraph()
{
    ResourceGraphInfo * graph = new ResourceGraphInfo(&options);
    graphs.append(*LINK(graph));
    //PrintLog("Create graph %p", graph);
    return graph;
}

void EclResourcer::connectGraphs(ResourceGraphInfo * sourceGraph, IHqlExpression * sourceNode, ResourceGraphInfo * sinkGraph, IHqlExpression * sinkNode, LinkKind kind)
{
    ResourceGraphLink * link = new ResourceGraphLink(sourceGraph, sourceNode, sinkGraph, sinkNode, kind);
    links.append(*link);
    if (sourceGraph)
        sourceGraph->sinks.append(*link);   
    if (sinkGraph)
        sinkGraph->sources.append(*link);
}


ResourcerInfo * EclResourcer::queryCreateResourceInfo(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    ResourcerInfo * info = (ResourcerInfo *)body->queryTransformExtra();
    if (!info)
    {
        info = new ResourcerInfo(expr, &options);
        body->setTransformExtraOwned(info);
    }
    return info;
}


void EclResourcer::replaceGraphReferences(IHqlExpression * expr, ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (info->isActivity && info->graph != oldGraph)
        return;

    info->graph.set(newGraph);
    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx=first; idx < last; idx++)
        replaceGraphReferences(expr->queryChild(idx), oldGraph, newGraph);
}


void EclResourcer::removeLink(ResourceGraphLink & link, bool keepExternalUses)
{
    ResourcerInfo * info = (ResourcerInfo *)queryResourceInfo(link.sourceNode);
    assertex(info && info->numExternalUses > 0);
    if (!keepExternalUses)
        info->numExternalUses--;
    if (link.sinkGraph)
        link.sinkGraph->sources.zap(link);
    link.sourceGraph->sinks.zap(link);
    links.zap(link);
}

void EclResourcer::replaceGraphReferences(ResourceGraphInfo * oldGraph, ResourceGraphInfo * newGraph)
{
    ForEachItemIn(idx1, oldGraph->sinks)
    {
        ResourceGraphLink & sink = oldGraph->sinks.item(idx1);
        replaceGraphReferences(sink.sourceNode, oldGraph, newGraph);
    }

    ForEachItemInRev(idx2, links)
    {
        ResourceGraphLink & cur = links.item(idx2);
        if (cur.sourceGraph == oldGraph)
        {
            if (cur.sinkGraph == newGraph)
                removeLink(cur, false);
            else
                cur.changeSourceGraph(newGraph);
        }
        else if (cur.sinkGraph == oldGraph)
        {
            if (cur.sourceGraph == newGraph)
                removeLink(cur, false);
            else
                cur.changeSinkGraph(newGraph);
        }
    }
}

//------------------------------------------------------------------------------------------
// PASS1: Gather information about splitter locations..
void EclResourcer::tagActiveCursors(HqlExprCopyArray & activeRows)
{
    ForEachItemIn(i, activeRows)
    {
        IHqlExpression & cur = activeRows.item(i);
        activeSelectors.append(cur);
        queryCreateResourceInfo(&cur)->isAlreadyInScope = true;
    }
}

inline bool projectSelectorDatasetToField(IHqlExpression * row)
{
    return ((row->getOperator() == no_selectnth) && getFieldCount(row->queryRecord()) > 1);
}


static HqlTransformerInfo eclHoistLocatorInfo("EclHoistLocator");
class EclHoistLocator : public NewHqlTransformer
{
public:
    EclHoistLocator(ChildDependentArray & _matched) : NewHqlTransformer(eclHoistLocatorInfo), matched(_matched)
    {
        alwaysSingle = true;
    }

    void analyseChild(IHqlExpression * expr, bool _alwaysSingle)
    {
        alwaysSingle = _alwaysSingle;
        analyse(expr, 0);
    }

    void noteDataset(IHqlExpression * expr, IHqlExpression * hoisted, bool alwaysHoist)
    {
        unsigned match = matched.findOriginal(expr);
        if (match == NotFound)
        {
            if (!hoisted)
                hoisted = expr;

            CChildDependent & depend = * new CChildDependent(expr, hoisted, alwaysHoist, alwaysSingle);
            matched.append(depend);
        }
        else
        {
            CChildDependent & prev = matched.item(match);
            if (alwaysHoist && !prev.alwaysHoist)
                prev.alwaysHoist = true;
            if (alwaysSingle && !prev.isSingleNode)
                prev.isSingleNode = true;
        }
    }

    void noteScalar(IHqlExpression * expr, IHqlExpression * value)
    {
        unsigned match = matched.findOriginal(expr);
        if (match == NotFound)
        {
            OwnedHqlExpr hoisted;
            IHqlExpression * projected = NULL;
            if (value->getOperator() == no_select)
            {
                bool isNew;
                IHqlExpression * row = querySelectorDataset(value, isNew);
                if(isNew || row->isDatarow())
                {
                    if (projectSelectorDatasetToField(row))
                        projected = value;

                    hoisted.set(row);
                }
                else
                {
                    //very unusual - possibly a thisnode(myfield) hoisted from an allnodes
                    //use a createrow of a single element in the default behavour below
                }
            }
            else if (value->getOperator() == no_createset)
            {
                IHqlExpression * ds = value->queryChild(0);
                IHqlExpression * selected = value->queryChild(1);

                OwnedHqlExpr field;
                //Project down to a single field.so implicit fields can still be optimized
                if (selected->getOperator() == no_select)
                    field.set(selected->queryChild(1));
                else
                    field.setown(createField(valueId, selected->getType(), NULL));
                OwnedHqlExpr record = createRecord(field);
                OwnedHqlExpr self = getSelf(record);
                OwnedHqlExpr assign = createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(selected));
                OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(record->getType()), LINK(assign));
                hoisted.setown(createDataset(no_newusertable, LINK(ds), createComma(LINK(record), LINK(transform))));
            }

            if (!hoisted)
            {
                OwnedHqlExpr field = createField(valueId, value->getType(), NULL);
                OwnedHqlExpr record = createRecord(field);
                OwnedHqlExpr self = getSelf(record);
                OwnedHqlExpr assign = createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(value));
                OwnedHqlExpr transform = createValue(no_transform, makeTransformType(record->getType()), LINK(assign));
                hoisted.setown(createRow(no_createrow, LINK(transform)));
            }

            CChildDependent & depend = * new CChildDependent(expr, hoisted, true, true);
            depend.projected = projected;
            matched.append(depend);
        }
    }

protected:
    ChildDependentArray & matched;
    bool alwaysSingle;
};



class EclChildSplitPointLocator : public EclHoistLocator
{
public:
    EclChildSplitPointLocator(IHqlExpression * _original, HqlExprCopyArray & _selectors, ChildDependentArray & _matches, bool _groupedChildIterators)
    : EclHoistLocator(_matches), selectors(_selectors), groupedChildIterators(_groupedChildIterators)
    { 
        original = _original;
        okToSelect = false; 
        gathered = false;
        conditionalDepth = 0;
        executedOnce = false;
        switch (original->getOperator())
        {
        case no_call:
        case no_externalcall:
        case no_libraryscopeinstance:
            okToSelect = true;
            break;
        }
    }

    void findSplitPoints(IHqlExpression * expr, unsigned from, unsigned to, bool _alwaysSingle, bool _executedOnce)
    {
        alwaysSingle = _alwaysSingle;
        for (unsigned i=from; i < to; i++)
        {
            IHqlExpression * cur = expr->queryChild(i);
            executedOnce = _executedOnce || cur->isAttribute();     // assume attributes are only executed once.
            findSplitPoints(cur);
        }
        alwaysSingle = false;
    }

protected:
    void findSplitPoints(IHqlExpression * expr)
    {
        //containsNonActiveDataset() would be nice - but that isn't percolated outside assigns etc.
        if (containsAnyDataset(expr) || containsMustHoist(expr))
        {
            if (!gathered)
            {
                gatherAmbiguousSelectors(original);
                gathered = true;
            }
            analyse(expr, 0);
        }
    }

    bool queryHoistDataset(IHqlExpression * ds)
    {
        bool alwaysHoist = true;
        if (executedOnce)
        {
            if (conditionalDepth != 0)
                alwaysHoist = false;
        }
        return alwaysHoist;
    }

    bool queryNoteDataset(IHqlExpression * ds)
    {
        bool alwaysHoist = queryHoistDataset(ds);
        //MORE: It should be possible to remove this condition, but it causes problems with resourcing hsss.xhql amongst others -> disable for the moment
        if (alwaysHoist)
            noteDataset(ds, ds, alwaysHoist);
        return alwaysHoist;
    }


    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (alreadyVisited(expr))
            return;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_select:
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(expr, isNew);
                if (isNew)
                {
                    if (isEvaluateable(ds))
                    {
                        //MORE: Following isn't a very nice test - stops implicit denormalize getting messed up
                        if (expr->isDataset())
                            break;

                        //Debtable.....
                        //Don't hoist counts on indexes or dataset - it may mean they are evaluated more frequently than need be.
                        //If dependencies and root graphs are handled correctly this could be deleted.
                        if (isCompoundAggregate(ds))
                            break;

                        if (!expr->isDatarow() && !expr->isDataset() && !expr->isDictionary())
                        {
                            if (queryHoistDataset(ds))
                            {
                                noteScalar(expr, expr);
                                return;
                            }
                        }
                        else
                        {
                            if (queryNoteDataset(ds))
                                return;
                        }
                    }
                }
                break;
            }
        case no_createset:
            {
                IHqlExpression * ds = expr->queryChild(0);
                if (isEvaluateable(ds))
                {
                    if (queryHoistDataset(ds))
                    {
                        noteScalar(expr, expr);
//??                    queryNoteDataset(ds);
                        return;
                    }
                }
                break;
            }
        case no_assign:
            {
                IHqlExpression * rhs = expr->queryChild(1);
                //if rhs is a new, evaluatable, dataset then we want to add it
                if ((rhs->isDataset() || rhs->isDictionary()) && isEvaluateable(rhs))
                {
                    if (queryNoteDataset(rhs))
                        return;
                }
                break;
            }
        case no_sizeof:
        case no_allnodes:
        case no_nohoist:
        case no_forcegraph:
            return;
        case no_globalscope:
        case no_evalonce:
            if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                noteDataset(expr, expr->queryChild(0), true);
            else
                noteScalar(expr, expr->queryChild(0));
            return;
        case no_thisnode:
            throwUnexpected();
        case no_getgraphresult:
            if (expr->hasAttribute(_streaming_Atom) || expr->hasAttribute(_distributed_Atom))
            {
                noteDataset(expr, expr, true);
                return;
            }
            break;
        case no_getgraphloopresult:
            noteDataset(expr, expr, true);
            return;
        case no_createdictionary:
            if (isEvaluateable(expr) && !isConstantDictionary(expr))
                noteDataset(expr, expr, true);
            return;

        case no_selectnth:
            if (expr->queryChild(1)->isConstant())
            {
                IHqlExpression * ds = expr->queryChild(0);
                switch (ds->getOperator())
                {
                case no_getgraphresult:
                    if (!expr->hasAttribute(_streaming_Atom) && !expr->hasAttribute(_distributed_Atom))
                        break;
                    //fallthrough
                case no_getgraphloopresult:
                    noteDataset(expr, expr, true);
                    return;
                }
            }
            break;
        }

        bool wasOkToSelect = okToSelect;
        if (expr->isDataset())
        {
            switch (expr->getOperator())
            {
            case no_compound_diskread:
            case no_compound_disknormalize:
            case no_compound_diskaggregate:
            case no_compound_diskcount:
            case no_compound_diskgroupaggregate:
            case no_compound_indexread:
            case no_compound_indexnormalize:
            case no_compound_indexaggregate:
            case no_compound_indexcount:
            case no_compound_indexgroupaggregate:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_childaggregate:
            case no_compound_childcount:
            case no_compound_childgroupaggregate:
            case no_compound_selectnew:
            case no_compound_inline:
            case no_newkeyindex:
            case no_keyindex:
            case no_table:
                okToSelect = false;
                break;
            }

            if (okToSelect && isEvaluateable(expr))
            {
                if (queryNoteDataset(expr))
                    return;
            }
        }
        else
            okToSelect = true;

        switch (op)
        {
        case no_if:
        case no_choose:
        case no_chooseds:
            {
                IHqlExpression * cond = expr->queryChild(0);
                analyseExpr(cond);
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth++;
                doAnalyseChildren(expr, 1);
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth--;
                break;
            }
        case no_mapto:
            {
                analyseExpr(expr->queryChild(0));
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth++;
                analyseExpr(expr->queryChild(1));
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    conditionalDepth--;
                break;
            }
        default:
            NewHqlTransformer::analyseExpr(expr);
            break;
        }

        okToSelect = wasOkToSelect;
    }

    bool isCompoundAggregate(IHqlExpression * ds)
    {
        return false;
        //Generates worse code unless we take into account whether or not the newDisk operation flags are enabled.
        if (!isTrivialSelectN(ds))
            return false;
        IHqlExpression * agg = ds->queryChild(0);
        if (isSimpleCountAggregate(agg, true))
            return true;
        return false;
    }

    void gatherAmbiguousSelectors(IHqlExpression * expr)
    {
        //Horrible code to try and cope with ambiguous left selectors.
        //o Tree is ambiguous so same child expression can occur in different contexts - so can't depend on the context it is found in to work out if can hoist
        //o If any selector that is hidden within child expressions matches one in scope then can't hoist it.
        //If the current expression creates a selector, then can't hoist anything that depends on it [only add to hidden if in selectors to reduce searching]
        //o Want to hoist as much as possible.
        if (selectors.empty())
            return;

        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        unsigned max = expr->numChildren();
        unsigned i;
        HqlExprCopyArray hiddenSelectors;
        for (i = 0; i < first; i++)
            expr->queryChild(i)->gatherTablesUsed(&hiddenSelectors, NULL);
        for (i = last; i < max; i++)
            expr->queryChild(i)->gatherTablesUsed(&hiddenSelectors, NULL);

        ForEachItemIn(iSel, selectors)
        {
            IHqlExpression & cur = selectors.item(iSel);
            if (hiddenSelectors.contains(cur))
                ambiguousSelectors.append(cur);
        }

        switch (getChildDatasetType(expr))
        {
        case childdataset_datasetleft:
        case childdataset_left:
            {
                IHqlExpression * ds = expr->queryChild(0);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                break;
            }
        case childdataset_same_left_right:
        case childdataset_top_left_right:
        case childdataset_nway_left_right:
            {
                IHqlExpression * ds = expr->queryChild(0);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                if (selectors.contains(*right))
                    ambiguousSelectors.append(*right);
                break;
            }
        case childdataset_leftright: 
            {
                IHqlExpression * leftDs = expr->queryChild(0);
                IHqlExpression * rightDs = expr->queryChild(1);
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, leftDs, selSeq);
                OwnedHqlExpr right = createSelector(no_right, rightDs, selSeq);
                if (selectors.contains(*left))
                    ambiguousSelectors.append(*left);
                if (selectors.contains(*right))
                    ambiguousSelectors.append(*right);
                break;
            }
        }
    }

    bool isEvaluateable(IHqlExpression * ds, bool ignoreInline = false)
    {
        //Don't hoist an alias - it could create unnecessary duplicate spills - hoist its input
        if (ds->getOperator() == no_dataset_alias)
            return false;

        //Not allowed to hoist
        if (isContextDependent(ds, (conditionalDepth == 0), true))
            return false;

        //MORE: Needs more work for child queries - need a GroupedChildIterator activity
        if (isGrouped(ds) && selectors.ordinality() && !groupedChildIterators)
            return false;

        //Check datasets are available
        HqlExprCopyArray scopeUsed;
        ds->gatherTablesUsed(NULL, &scopeUsed);
        ForEachItemIn(i, scopeUsed)
        {
            IHqlExpression & cur = scopeUsed.item(i);
            if (!selectors.contains(cur))
                return false;

            if (ambiguousSelectors.contains(cur))
                return false;
        }

        if (!isEfficientToHoistDataset(ds, ignoreInline))
            return false;

        return true;
    }

    bool isEfficientToHoistDataset(IHqlExpression * ds, bool ignoreInline) const
    {
        //MORE: This whole function could do with some significant improvements.  Whether it is inefficient to hoist
        //depends on at least the following...
        //a) cost of serializing v cost of re-evaluating (which can depend on the engine).
        //b) How many times it will be evaluated in the child context
        if (ds->getOperator() == no_createdictionary)
            return true;

        if (isInlineTrivialDataset(ds))
            return false;

#ifdef MINIMAL_CHANGES
        if (!ignoreInline)
        {
            //Generally this appears to be better to hoist since it involves calling a transform.
            if (ds->getOperator() == no_dataset_from_transform)
                return true;

            if (canProcessInline(NULL, ds))
                return false;
        }
#endif

        return true;
    }

protected:
    IHqlExpression * original;
    HqlExprCopyArray & selectors;
    HqlExprCopyArray ambiguousSelectors;
    unsigned conditionalDepth;
    bool okToSelect;
    bool gathered;
    bool groupedChildIterators;
    bool executedOnce;
};



void EclResourcer::gatherChildSplitPoints(IHqlExpression * expr, ResourcerInfo * info, unsigned first, unsigned last)
{
    //NB: Don't call member functions to ensure correct nesting of transform mutexes.
    EclChildSplitPointLocator locator(expr, activeSelectors, info->childDependents, options.groupedChildIterators);
    unsigned max = expr->numChildren();

    //If child queries are supported then don't hoist the expressions if they might only be evaluated once
    //because they may be conditional
    switch (expr->getOperator())
    {
    case no_setresult:
    case no_selectnth:
        //set results, only done once=>don't hoist conditionals
        locator.findSplitPoints(expr, last, max, true, true);
        return;
    case no_loop:
        if ((options.targetClusterType == ThorLCRCluster) && !options.isChildQuery)
        {
            //This is ugly!  The body is executed in parallel, so don't force that as a work unit result
            //It means some child query expressions within loops don't get forced into work unit writes
            //but that just means that the generated code will be not as good as it could be.
            const unsigned bodyArg = 4;
            locator.findSplitPoints(expr, 1, bodyArg, true, false);
            locator.findSplitPoints(expr, bodyArg, bodyArg+1, false, false);
            locator.findSplitPoints(expr, bodyArg+1, max, true, false);
            return;
        }
        break;
    }
    locator.findSplitPoints(expr, 0, first, true, true);          // IF() conditions only evaluated once... => don't force
    locator.findSplitPoints(expr, last, max, true, false);
}


class EclThisNodeLocator : public EclHoistLocator
{
public:
    EclThisNodeLocator(ChildDependentArray & _matches)
        : EclHoistLocator(_matches)
    { 
        allNodesDepth = 0;
    }

protected:
    virtual void analyseExpr(IHqlExpression * expr)
    {
        //NB: This doesn't really work for no_thisnode occurring in multiple contexts.  We should probably hoist it from everywhere if it is hoistable from anywhere,
        //    although theoretically that gives us problems with ambiguous selectors.
        if (alreadyVisited(expr) || !containsThisNode(expr))
            return;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_allnodes:
            allNodesDepth++;
            NewHqlTransformer::analyseExpr(expr);
            allNodesDepth--;
            return;
        case no_thisnode:
            if (allNodesDepth == 0)
            {
                if (expr->isDataset() || expr->isDatarow() || expr->isDictionary())
                    noteDataset(expr, expr->queryChild(0), true);
                else
                    noteScalar(expr, expr->queryChild(0));
                return;
            }
            allNodesDepth--;
            NewHqlTransformer::analyseExpr(expr);
            allNodesDepth++;
            return;
        }
        NewHqlTransformer::analyseExpr(expr);
    }

protected:
    unsigned allNodesDepth;
};


static bool isPotentialCompoundSteppedIndexRead(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
            return false;
        case no_compound_indexread:
        case no_newkeyindex:
            return true;
        case no_getgraphloopresult:
            return true; // Could be an index read in another graph iteration, so don't combine
        case no_keyedlimit:
        case no_preload:
        case no_filter:
        case no_hqlproject:
        case no_newusertable:
        case no_limit:
        case no_sorted:
        case no_preservemeta:
        case no_distributed:
        case no_grouped:
        case no_stepped:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
            break;
        case no_choosen:
            {
                IHqlExpression * arg2 = expr->queryChild(2);
                if (arg2 && !arg2->isPure())
                    return false;
                break;
            }
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

bool EclResourcer::findSplitPoints(IHqlExpression * expr, bool isProjected)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (info && info->visited)
    {
        if (!isProjected)
            info->clearProjected();

        if (info->isAlreadyInScope || info->isActivity || !info->containsActivity)
            return info->containsActivity;
    }
    else
    {
        info = queryCreateResourceInfo(expr);
        info->visited = true;
        if (!isProjected)
            info->clearProjected();

        bool isActivity = true;
        switch (expr->getOperator())
        {
        case no_select:
            //either a select from a setresult or use of a child-dataset
            if (isNewSelector(expr))
            {
                info->containsActivity = findSplitPoints(expr->queryChild(0), false);
                assertex(queryResourceInfo(expr->queryChild(0))->isActivity);
            }
            if (expr->isDataset() || expr->isDatarow())
            {
                info->isActivity = true;
                info->containsActivity = true;
            }
            return info->containsActivity;
        case no_mapto:
            throwUnexpected();
            info->containsActivity = findSplitPoints(expr->queryChild(1), false);
            return info->containsActivity;
        case no_activerow:
            info->isActivity = true;
            info->containsActivity = false;
            return false;
        case no_rowset:                         // don't resource this as an activity
            isActivity = false;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_getgraphloopresultset:
            info->isActivity = false;
            info->containsActivity = false;
            return false;
        case no_datasetlist:
            isActivity = false;
            break;
        case no_rowsetrange:
            {
                //Don't resource this as an activity if it is a function of the input graph rows, 
                //however we do want to if it is coming from a dataset list.
                IHqlExpression * ds = expr->queryChild(0);
                //MORE: Should walk further down the tree to allow for nested rowsetranges etc.
                if (ds->getOperator() == no_rowset || ds->getOperator() == no_getgraphloopresultset)
                {
                    info->isActivity = false;
                    info->containsActivity = false;
                    return false;
                }
                isActivity = false;
                break;
            }
        }

        ITypeInfo * type = expr->queryType();
        if (!type || type->isScalar())
            return false;

        info->isActivity = isActivity;
        info->containsActivity = true;
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);

    if (options.hoistResourced)
    {
        switch (expr->getOperator())
        {
        case no_allnodes:
            {
                //MORE: This needs to recursively walk and lift any contained no_selfnode, but don't go past another nested no_allnodes;
                EclThisNodeLocator locator(info->childDependents);
                locator.analyseChild(expr->queryChild(0), true);
                break;
            }
        case no_childquery:
            throwUnexpected();
        default:
            {
                for (unsigned idx=first; idx < last; idx++)
                {
                    IHqlExpression * cur = expr->queryChild(idx);
                    findSplitPoints(cur, false);
                }
                gatherChildSplitPoints(expr, info, first, last);
                break;
            }
        }

        ForEachItemIn(i2, info->childDependents)
        {
            CChildDependent & cur = info->childDependents.item(i2);
            ResourcerInfo * hoistedInfo = queryCreateResourceInfo(cur.hoisted);
            if (cur.projected)
                hoistedInfo->addProjected(cur.projected);
            else
                hoistedInfo->clearProjected();

            if (cur.alwaysHoist)
                findSplitPoints(cur.hoisted, (cur.projected != NULL));

            allChildDependents.append(OLINK(cur));
        }
    }
    else
    {
        for (unsigned idx=first; idx < last; idx++)
            findSplitPoints(expr->queryChild(idx), false);
    }

    return info->containsActivity;
}


void EclResourcer::findSplitPoints(HqlExprArray & exprs)
{
    //Finding items that should be hoisted from child queries, and evaluated at this level is tricky:
    //* After hoisting a child expression, that may in turn contain other child expressions.
    //* Some child expressions are only worth hoisting if they are a simple function of something
    //  that will be evaluated here.
    //* If we're creating a single project for each x[1] then that needs to be done after all the
    //  expressions to hoist have been found.
    //* The usage counts have to correct - including
    //* The select child dependents for a ds[n] need to be inherited by the project(ds)[n]
    //=>
    //First walk the expression tree gathering all the expressions that will be used.
    //Project the expressions that need projecting.
    //Finally walk the tree again, this time calculating the correct usage counts.
    //
    //On reflection the hoisting and projecting could be implemented as a completely separate transformation.
    //However, it shares a reasonable amount of the logic with the rest of the resourcing, so keep together
    //for the moment.
    ForEachItemIn(idx, exprs)
        findSplitPoints(&exprs.item(idx), false);

    extendSplitPoints();
    projectChildDependents();

    deriveUsageCounts(exprs);
}

void EclResourcer::extendSplitPoints()
{
    //NB: findSplitPoints might call this array to be extended
    for (unsigned i1=0; i1 < allChildDependents.ordinality(); i1++)
    {
        CChildDependent & cur = allChildDependents.item(i1);
        if (!cur.alwaysHoist && isWorthForcingHoist(cur.hoisted))
            findSplitPoints(cur.hoisted, (cur.projected != NULL));
    }
}

IHqlExpression * EclResourcer::projectChildDependent(IHqlExpression * expr)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->projectResult || info->projected.empty())
        return expr;

    if (info->projectedExpr)
        return info->projectedExpr;

    assertex(expr->getOperator() == no_selectnth);
    IHqlExpression * row = expr;
    assertex(projectSelectorDatasetToField(row));

    unsigned totalFields = getFieldCount(row->queryRecord());
    if (totalFields == info->projected.ordinality())
    {
        info->clearProjected();
        return expr;
    }

    //Create a projection containing each of the fields that are used from the child queries.
    IHqlExpression * ds = row->queryChild(0);
    HqlExprArray fields;
    HqlExprArray values;
    ForEachItemIn(i, info->projected)
    {
        IHqlExpression * value = &info->projected.item(i);
        IHqlExpression * field = value->queryChild(1);
        LinkedHqlExpr projectedField = field;
        //Check for a very unusual situation where the same field is projected from two different sub records
        while (fields.contains(*projectedField))
            projectedField.setown(cloneFieldMangleName(field));  // Generates a new mangled name each time

        OwnedHqlExpr activeDs = createRow(no_activetable, LINK(ds->queryNormalizedSelector()));
        fields.append(*LINK(projectedField));
        values.append(*replaceSelector(value, row, activeDs));
    }

    OwnedHqlExpr projectedRecord = createRecord(fields);
    OwnedHqlExpr self = getSelf(projectedRecord);
    HqlExprArray assigns;
    ForEachItemIn(i2, fields)
    {
        IHqlExpression * field = &fields.item(i2);
        IHqlExpression * value = &values.item(i2);
        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(field)), LINK(value)));
    }
    OwnedHqlExpr transform = createValue(no_newtransform, makeTransformType(projectedRecord->getType()), assigns);
    OwnedHqlExpr projectedDs = createDataset(no_newusertable, LINK(ds), createComma(LINK(projectedRecord), LINK(transform)));
    info->projectedExpr.setown(replaceChild(row, 0, projectedDs));

    findSplitPoints(info->projectedExpr, false);

    //Ensure child dependents that are no longer used are not processed - otherwise the usage counts will be wrong.
    ForEachItemIn(ic, info->childDependents)
    {
        CChildDependent & cur = info->childDependents.item(ic);
        //Clearing these ensures that isResourcedActivity returns false;
        cur.hoisted.clear();
        cur.projectedHoisted.clear();
    }

    return info->projectedExpr;
}

void EclResourcer::projectChildDependents()
{
    ForEachItemIn(i2, allChildDependents)
    {
        CChildDependent & cur = allChildDependents.item(i2);
        if (isResourcedActivity(cur.hoisted))
            cur.projectedHoisted.set(projectChildDependent(cur.hoisted));
    }
}

void EclResourcer::deriveUsageCounts(IHqlExpression * expr)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !(info->containsActivity || info->isActivity))
        return;

    bool savedInsideNeverSplit = insideNeverSplit;
    bool savedInsideSteppedNeverSplit = insideSteppedNeverSplit;
    if (insideSteppedNeverSplit && info)
    {
        if (!isPotentialCompoundSteppedIndexRead(expr) && (expr->getOperator() != no_datasetlist) && expr->getOperator() != no_inlinetable)
            insideSteppedNeverSplit = false;
    }

    if (info->numUses)
    {
        if (insideNeverSplit || insideSteppedNeverSplit)
            info->neverSplit = true;

        if (info->isAlreadyInScope || info->isActivity || !info->containsActivity)
        {
            info->numUses++;
            return;
        }
    }
    else
    {
        info->numUses++;
        if (insideNeverSplit || insideSteppedNeverSplit)
            info->neverSplit = true;

        switch (expr->getOperator())
        {
        case no_select:
            //either a select from a setresult or use of a child-dataset
            if (isNewSelector(expr))
                deriveUsageCounts(expr->queryChild(0));
            return;
        case no_activerow:
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
        case no_rowset:                         // don't resource this as an activity
        case no_getgraphloopresultset:
            return;
        case no_keyedlimit:
            if (options.preventKeyedSplit)
                insideNeverSplit = true;
            break;
        case no_filter:
            if (options.preventKeyedSplit && filterIsKeyed(expr))
                insideNeverSplit = true;
            else
            {
                LinkedHqlExpr invariant;
                OwnedHqlExpr cond = extractFilterConditions(invariant, expr, expr->queryNormalizedSelector(), false);
                if (invariant)
                    info->isConditionalFilter = true;
            }
            break;
        case no_hqlproject:
        case no_newusertable:
        case no_aggregate:
        case no_newaggregate:
            if (options.preventKeyedSplit && expr->hasAttribute(keyedAtom))
                insideNeverSplit = true;
            break;
        case no_stepped:
        case no_mergejoin:
        case no_nwayjoin:
            if (options.preventSteppedSplit)
                insideSteppedNeverSplit = true;
            break;
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
            insideNeverSplit = true;
            break;
        }

        ITypeInfo * type = expr->queryType();
        if (!type || type->isScalar())
        {
            insideNeverSplit = savedInsideNeverSplit;
            insideSteppedNeverSplit = savedInsideSteppedNeverSplit;
            return;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);

    for (unsigned idx=first; idx < last; idx++)
        deriveUsageCounts(expr->queryChild(idx));

    insideNeverSplit = savedInsideNeverSplit;
    insideSteppedNeverSplit = savedInsideSteppedNeverSplit;
}


void EclResourcer::deriveUsageCounts(const HqlExprArray & exprs)
{
    ForEachItemIn(idx2, exprs)
        deriveUsageCounts(&exprs.item(idx2));

    ForEachItemIn(i2, allChildDependents)
    {
        CChildDependent & cur = allChildDependents.item(i2);
        if (isResourcedActivity(cur.projectedHoisted))
            deriveUsageCounts(cur.projectedHoisted);
    }
}

//------------------------------------------------------------------------------------------
// PASS2: Actually create the subgraphs based on splitters.

void EclResourcer::createInitialGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, LinkKind linkKind, bool forceNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    LinkKind childLinkKind = UnconditionalLink;
    Linked<ResourceGraphInfo> thisGraph = ownerGraph;
    bool forceNewChildGraph = false; 
    if (info->isActivity)
    {
        //Need to ensure no_libraryselects are not separated from the no_libraryscopeinstance
        //so ensure they are placed in the same graph.
        node_operator op = expr->getOperator();
        if (op == no_libraryselect)
        {
            ResourcerInfo * moduleInfo = queryResourceInfo(expr->queryChild(1));
            if (!info->graph && moduleInfo->graph)
                info->graph.set(moduleInfo->graph);
        }

        if (info->graph)
        {
            connectGraphs(info->graph, expr, ownerGraph, owner, linkKind);
            info->numExternalUses++;
            return;
        }

        unsigned numUses = info->numUses;
        switch (op)
        {
        case no_libraryscopeinstance:
            numUses = 1;
            break;
        case no_libraryselect:
            forceNewGraph = true;
            break;
        }

        if (!ownerGraph || numUses > 1 || (linkKind != UnconditionalLink) || forceNewGraph)
        {
            thisGraph.setown(createGraph());
            connectGraphs(thisGraph, expr, ownerGraph, owner, linkKind);
            info->numExternalUses++;
            if (!ownerGraph && sequential)
                thisGraph->hasSequentialSource = true;
        }
        info->graph.set(thisGraph);

        switch (expr->getOperator())
        {
        case no_compound:
            //NB: First argument is forced into a separate graph
            createInitialGraph(expr->queryChild(0), expr, NULL, UnconditionalLink, true);
            createInitialGraph(expr->queryChild(1), expr, thisGraph, UnconditionalLink, false);
            return;
        case no_executewhen:
            {
                bool newGraph = expr->isAction() && (options.targetClusterType == HThorCluster);
                createInitialGraph(expr->queryChild(0), expr, thisGraph, UnconditionalLink, newGraph);
                createInitialGraph(expr->queryChild(1), expr, thisGraph, UnconditionalLink, true);
                return;
            }
        case no_keyindex:
        case no_newkeyindex:
            return;
        case no_parallel:
            {
                ForEachChild(i, expr)
                    createInitialGraph(expr->queryChild(i), expr, thisGraph, UnconditionalLink, true);
                return;
            }
        case no_if:
        case no_choose:
        case no_chooseds:
            //conditional nodes, the child branches are marked as conditional
            childLinkKind = UnconditionalLink;
            thisGraph->mergedConditionSource = true;
            if (!options.noConditionalLinks || expr->isAction())
                forceNewChildGraph = true;
            break;
        case no_filter:
            if (info->isConditionalFilter)
            {
                thisGraph->mergedConditionSource = true;
                if (!options.noConditionalLinks)
                    forceNewChildGraph = true;
            }
            break;
//      case no_nonempty:
        case no_sequential:
            {
                unsigned first = getFirstActivityArgument(expr);
                unsigned last = first + getNumActivityArguments(expr);
                createInitialGraph(expr->queryChild(first), expr, thisGraph, SequenceLink, true);
                for (unsigned idx = first+1; idx < last; idx++)
                    createInitialGraph(expr->queryChild(idx), expr, thisGraph, SequenceLink, true);
                return;
            }
        case no_case:
        case no_map:
            {
                throwUnexpected();
            }
        case no_output:
            {
                //Tag the inputs to an output statement, so that if a spill was going to occur we read
                //from the output file instead of spilling.
                //Needs the grouping to be saved in the same way.  Could cope with compressed matching, but not
                //much point - since fairly unlikely.
                IHqlExpression * filename = expr->queryChild(1);
                if (filename && (filename->getOperator() == no_constant) && !expr->hasAttribute(xmlAtom) && !expr->hasAttribute(csvAtom))
                {
                    IHqlExpression * dataset = expr->queryChild(0);
                    if (expr->hasAttribute(groupedAtom) == (dataset->queryType()->queryGroupInfo() != NULL))
                    {
                        StringBuffer filenameText;
                        filename->queryValue()->getStringValue(filenameText);
                        ResourcerInfo * childInfo = queryResourceInfo(dataset);
                        if (!childInfo->linkedFromChild && !isUpdatedConditionally(expr))
                            childInfo->outputToUseForSpill = expr;
                    }
                }
                if (isUpdatedConditionally(expr))
                    thisGraph->mergedConditionSource = true;
                break;
            }
        case no_buildindex:
            if (isUpdatedConditionally(expr))
                thisGraph->mergedConditionSource = true;
            break;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx = first; idx < last; idx++)
        createInitialGraph(expr->queryChild(idx), expr, thisGraph, childLinkKind, forceNewChildGraph);

    ForEachItemIn(i2, info->childDependents)
    {
        CChildDependent & cur = info->childDependents.item(i2);
        if (isResourcedActivity(cur.projectedHoisted))
            createInitialGraph(cur.projectedHoisted, expr, thisGraph, SequenceLink, true);
    }
}


void EclResourcer::createInitialGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
        createInitialGraph(&exprs.item(idx), NULL, NULL, UnconditionalLink, false);
}

void EclResourcer::createInitialRemoteGraph(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerGraph, bool forceNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    Linked<ResourceGraphInfo> thisGraph = ownerGraph;
    if (info->isActivity)
    {
        if (info->graph)
        {
            connectGraphs(info->graph, expr, ownerGraph, owner, UnconditionalLink);
            info->numExternalUses++;
            return;
        }

        if (!ownerGraph || forceNewGraph)
        {
            thisGraph.setown(createGraph());
            connectGraphs(thisGraph, expr, ownerGraph, owner, UnconditionalLink);
            info->numExternalUses++;
        }
        info->graph.set(thisGraph);

        switch (expr->getOperator())
        {
        case no_compound:
            createInitialRemoteGraph(expr->queryChild(0), expr, NULL, true);
            createInitialRemoteGraph(expr->queryChild(1), expr, thisGraph, false);
            return;
        case no_executewhen:
            createInitialRemoteGraph(expr->queryChild(0), expr, thisGraph, false);
            createInitialRemoteGraph(expr->queryChild(1), expr, thisGraph, true);
            return;
        }
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx = first; idx < last; idx++)
        createInitialRemoteGraph(expr->queryChild(idx), expr, thisGraph, false);
}


void EclResourcer::createInitialRemoteGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
        createInitialRemoteGraph(&exprs.item(idx), NULL, NULL, false);
}

//------------------------------------------------------------------------------------------
// PASS3: Tag graphs/links that are conditional or unconditional

void EclResourcer::markChildDependentsAsUnconditional(ResourcerInfo * info, IHqlExpression * condition)
{
    if (options.hoistResourced)
    {
        ForEachItemIn(i2, info->childDependents)
        {
            CChildDependent & cur = info->childDependents.item(i2);
            if (isResourcedActivity(cur.projectedHoisted))
                markAsUnconditional(cur.projectedHoisted, NULL, condition);
        }
    }
}

void EclResourcer::markAsUnconditional(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, IHqlExpression * condition)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx=first; idx < last; idx++)
            markAsUnconditional(expr->queryChild(idx), ownerGraph, condition);
        return;
    }

    if (condition)
        if (info->addCondition(condition))
            condition = NULL;

    if (info->pathToExpr == ResourcerInfo::PathUnconditional)
        return;

    if ((info->pathToExpr == ResourcerInfo::PathConditional) && condition)
    {
        if (targetClusterType == RoxieCluster)
        {
            if (spillMultiCondition)
            {
                if (info->graph != ownerGraph)
                    info->graph->isUnconditional = true;
            }
            return;
        }
        else
        {
            if (info->graph != ownerGraph)
                return;
        }
    }

    bool wasConditional = (info->pathToExpr == ResourcerInfo::PathConditional);
    if (!condition)
    {
        info->pathToExpr = ResourcerInfo::PathUnconditional;
        if (info->graph != ownerGraph)
            info->graph->isUnconditional = true;
    }
    else
        info->pathToExpr = ResourcerInfo::PathConditional;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_if:
    case no_choose:
    case no_chooseds:
        if (options.noConditionalLinks)
            break;
        if (condition)
            markCondition(expr, condition, wasConditional);
        else
        {
            //This list is processed in a second phase.
            if (rootConditions.find(*expr) == NotFound)
                rootConditions.append(*LINK(expr));
        }
        markChildDependentsAsUnconditional(info, condition);
        return;
    case no_filter:
        if (!info->isConditionalFilter || options.noConditionalLinks)
            break;

        if (condition)
            markCondition(expr, condition, wasConditional);
        else
        {
            //This list is processed in a second phase.
            if (rootConditions.find(*expr) == NotFound)
                rootConditions.append(*LINK(expr));
        }
        markChildDependentsAsUnconditional(info, condition);
        return;
    case no_sequential:
//  case no_nonempty:
        if (!options.isChildQuery)
        {
            unsigned first = getFirstActivityArgument(expr);
            unsigned last = first + getNumActivityArguments(expr);
            IHqlExpression * child0 = expr->queryChild(0);
            markAsUnconditional(child0, info->graph, condition);
            queryResourceInfo(child0)->graph->hasConditionSource = true;        // force it to generate even if contains something very simple e.g., null action
            for (unsigned idx = first+1; idx < last; idx++)
            {
                OwnedHqlExpr tag = createAttribute(instanceAtom, LINK(expr), getSizetConstant(idx), LINK(condition));
                IHqlExpression * child = expr->queryChild(idx);
                markAsUnconditional(child, queryResourceInfo(child)->graph, tag);

                queryResourceInfo(child)->setConditionSource(tag, !wasConditional);
            }
            markChildDependentsAsUnconditional(info, condition);
            return;
        }
        break;

    case no_case:
    case no_map:
        UNIMPLEMENTED;
    }

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    for (unsigned idx=first; idx < last; idx++)
        markAsUnconditional(expr->queryChild(idx), info->graph, condition);

    markChildDependentsAsUnconditional(info, condition);
}


void EclResourcer::markConditionBranch(unsigned childIndex, IHqlExpression * expr, IHqlExpression * condition, bool wasConditional)
{
    IHqlExpression * child = queryRealChild(expr, childIndex);
    if (child)
    {
        OwnedHqlExpr tag;
        if (expr->getOperator() == no_if)
            tag.setown(createAttribute(((childIndex==1) ? trueAtom : falseAtom), LINK(expr), LINK(condition)));
        else
            tag.setown(createAttribute(trueAtom, LINK(expr), LINK(condition), getSizetConstant(childIndex)));
        markAsUnconditional(child, queryResourceInfo(child)->graph, tag);

        queryResourceInfo(child)->setConditionSource(tag, !wasConditional);
    }
}


void EclResourcer::markCondition(IHqlExpression * expr, IHqlExpression * condition, bool wasConditional)
{
    if (expr->getOperator() == no_filter)
    {
        markConditionBranch(0, expr, condition, wasConditional);
    }
    else
    {
        ForEachChildFrom(i, expr, 1)
            markConditionBranch(i, expr, condition, wasConditional);
    }
}

void EclResourcer::markConditions(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
        markAsUnconditional(&exprs.item(idx), NULL, NULL);

    ForEachItemIn(idx2, rootConditions)
        markCondition(&rootConditions.item(idx2), NULL, false);
}



//------------------------------------------------------------------------------------------
// PASS4: Split subgraphs based on resource requirements for activities
//This will need to be improved if we allow backtracking to get the best combination of activities to fit in the subgraph

void EclResourcer::createResourceSplit(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerNewGraph, ResourceGraphInfo * originalGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);

    info->graph.setown(createGraph());
    info->graph->isUnconditional = originalGraph->isUnconditional;
    changeGraph(expr, info->graph);
    connectGraphs(info->graph, expr, ownerNewGraph, owner, UnconditionalLink);
    info->numExternalUses++;
}



void EclResourcer::getResources(IHqlExpression * expr, CResources & exprResources)
{
    ::getResources(expr, exprResources, options);
}


bool EclResourcer::calculateResourceSpillPoints(IHqlExpression * expr, ResourceGraphInfo * graph, CResources & resourcesSoFar, bool hasGoodSpillPoint, bool canSpill)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return true;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        if (last - first == 1)
            return calculateResourceSpillPoints(expr->queryChild(first), graph, resourcesSoFar, hasGoodSpillPoint, canSpill);
        for (unsigned idx = first; idx < last; idx++)
            calculateResourceSpillPoints(expr->queryChild(idx), graph, resourcesSoFar, false, canSpill);
        return true;
    }

    if (info->graph != graph)
        return true;

    CResources exprResources(clusterSize);
    getResources(expr, exprResources);

    info->isSpillPoint = false;
    Owned<CResources> curResources = LINK(&resourcesSoFar);
    if (resourcesSoFar.addExceeds(exprResources, *resourceLimit))
    {
        if (hasGoodSpillPoint)
            return false;
        info->isSpillPoint = true;
        spilled = true;
        curResources.setown(new CResources(clusterSize));
        if (exprResources.exceeds(*resourceLimit))
            throwError2(HQLERR_CannotResourceActivity, getOpString(expr->getOperator()), clusterSize);
    }

    if (options.minimizeSkewBeforeSpill)
    {
        if (canSpill && heavyweightAndReducesSizeOrSkew(expr))
        {
            //if the input activity is going to cause us to run out of resources, then it is going to be better to split here than anywhere else
            //this code may conceivably cause extra spills far away because the hash distributes are moved.
            IHqlExpression * childExpr = expr->queryChild(0);
            ResourcerInfo * childInfo = queryResourceInfo(childExpr);

            if (childInfo->graph == graph)
            {
                CResources childResources(clusterSize);
                getResources(childExpr, childResources);
                childResources.add(exprResources);
                if (curResources->addExceeds(childResources, *resourceLimit))
                {
                    info->isSpillPoint = true;
                    spilled = true;
                    calculateResourceSpillPoints(childExpr, graph, exprResources, false, true);
                    return true;
                }
            }
            //otherwise continue as normal.
        }
    }

    curResources->add(exprResources);

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    if (hasGoodSpillPoint)
    {
        if (exprResources.resource[RESheavy] || exprResources.resource[REShashdist] || last-first != 1)
            hasGoodSpillPoint = false;
    }
    else if (!info->isSpillPoint && canSpill)
    {
        if (lightweightAndReducesDatasetSize(expr) || queryHint(expr, spillAtom))
        {
            CResources savedResources(*curResources);
            if (!calculateResourceSpillPoints(expr->queryChild(0), graph, *curResources, true, true))
            {
                curResources->set(savedResources);
                info->isSpillPoint = true;
                spilled = true;
                calculateResourceSpillPoints(expr->queryChild(0), graph, exprResources, false, true);
            }
            return true;
        }
    }

    node_operator op = expr->getOperator();
    if ((op == no_if) || (op == no_choose) || (op == no_chooseds))
    {
        //For conditions, spill on intersection of resources used, not union.
        CResources savedResources(*curResources);
        if (!calculateResourceSpillPoints(expr->queryChild(1), graph, *curResources, hasGoodSpillPoint, true))
            return false;
        ForEachChildFrom(i, expr, 2)
        {
            if (expr->queryChild(i)->isAttribute())
                continue;
            if (!calculateResourceSpillPoints(expr->queryChild(i), graph, savedResources, hasGoodSpillPoint, true))
                return false;
            curResources->maximize(savedResources);
        }
    }
    else
    {
        for (unsigned idx = first; idx < last; idx++)
            if (!calculateResourceSpillPoints(expr->queryChild(idx), graph, *curResources, hasGoodSpillPoint, true))
                return false;
    }
    return true;
}

void EclResourcer::insertResourceSpillPoints(IHqlExpression * expr, IHqlExpression * owner, ResourceGraphInfo * ownerOriginalGraph, ResourceGraphInfo * ownerNewGraph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return;

    if (!info->isActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx = first; idx < last; idx++)
            insertResourceSpillPoints(expr->queryChild(idx), expr, ownerOriginalGraph, ownerNewGraph);
        return;
    }

    ResourceGraphInfo * originalGraph = info->graph;    //NB: Graph will never cease to exist, so don't need to link.
    if (originalGraph != ownerOriginalGraph)
        return;

    if (info->isSpillPoint)
        createResourceSplit(expr, owner, ownerNewGraph, originalGraph);
    else if (info->graph != ownerNewGraph)
        changeGraph(expr, ownerNewGraph);

    CResources exprResources(clusterSize);
    getResources(expr, exprResources);

    bool ok = info->graph->allocateResources(exprResources, *resourceLimit);
    assertex(ok);

    node_operator op = expr->getOperator();
    if ((op == no_if) || (op == no_choose) || (op == no_chooseds))
    {
        CResources savedResources(info->graph->resources);
        insertResourceSpillPoints(expr->queryChild(1), expr, originalGraph, info->graph);

        ForEachChildFrom(i, expr, 2)
        {
            if (expr->queryChild(i)->isAttribute())
                continue;
            CResources branchResources(info->graph->resources);
            info->graph->resources.set(savedResources);
            insertResourceSpillPoints(expr->queryChild(i), expr, originalGraph, info->graph);
            info->graph->resources.maximize(branchResources);
        }
    }
    else
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx = first; idx < last; idx++)
            insertResourceSpillPoints(expr->queryChild(idx), expr, originalGraph, info->graph);
    }
}


void EclResourcer::resourceSubGraph(ResourceGraphInfo * graph)
{
    if (graph->beenResourced)
        return;
    graph->beenResourced = true;
    ForEachItemIn(idx, graph->sources)
        resourceSubGraph(graph->sources.item(idx).sourceGraph);

    IHqlExpression * sourceNode = graph->sinks.item(0).sourceNode->queryBody();
#ifdef _DEBUG
    //Sanity check, ensure there is only a single sink for this graph.  
    //However because libraryselects are tightly bound to their library instance there may be multiple library selects.  
    //They won't affect the resourcing though, since they'll plug into the same library instance, and the selects use no resources.
    ForEachItemIn(idx2, graph->sinks)
    {
        IHqlExpression * thisSink = graph->sinks.item(idx2).sourceNode->queryBody();
        if (thisSink->getOperator() != no_libraryselect)
            assertex(thisSink == sourceNode);
    }
#endif
    spilled = false;
    CResources resources(clusterSize);
    calculateResourceSpillPoints(sourceNode, graph, resources, false, false);
    if (spilled)
        insertResourceSpillPoints(sourceNode, NULL, graph, graph);
    else
        graph->resources.set(resources);
}

void EclResourcer::resourceSubGraphs(HqlExprArray & exprs)
{
    ForEachItemIn(idx, graphs)
        resourceSubGraph(&graphs.item(idx));
}


//------------------------------------------------------------------------------------------
// PASS5: Link subrgaphs with dependency information so they don't get merged by accident.

void EclResourcer::addDependencySource(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr)
{
    //MORE: Should we check this doesn't already exist?
    dependencySource.search.append(*LINK(search));
    dependencySource.graphs.append(*LINK(curGraph));
    dependencySource.exprs.append(*LINK(expr));
}

void EclResourcer::addDependencyUse(IHqlExpression * search, ResourceGraphInfo * curGraph, IHqlExpression * expr)
{
    unsigned index = dependencySource.search.find(*search);
    if (index != NotFound)
    {
        if (&dependencySource.graphs.item(index) == curGraph)
        {
            //Don't give a warning if get/set is within the same activity (e.g., within a local())
            if (&dependencySource.exprs.item(index) != expr)
            //errors->reportWarning(HQLWRN_RecursiveDependendencies, HQLWRN_RecursiveDependendencies_Text, *codeGeneratorAtom, 0, 0, 0);
                errors->reportError(HQLWRN_RecursiveDependendencies, HQLWRN_RecursiveDependendencies_Text, codeGeneratorId->str(), 0, 0, 0);
        }
        else
        {
            ResourceGraphLink * link = new ResourceGraphDependencyLink(&dependencySource.graphs.item(index), &dependencySource.exprs.item(index), curGraph, expr);
            curGraph->dependsOn.append(*link);
            links.append(*link);
        }
    }
}


void EclResourcer::addRefExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr)
{
    IHqlExpression * filename = queryTableFilename(expr);
    if (filename)
    {
        OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
        addDependencySource(value, curGraph, activityExpr);
    }
}

bool EclResourcer::addExprDependency(IHqlExpression * expr, ResourceGraphInfo * curGraph, IHqlExpression * activityExpr)
{
    switch (expr->getOperator())
    {
    case no_buildindex:
    case no_output:
        {
            IHqlExpression * filename = queryRealChild(expr, 1);
            if (filename)
            {
                switch (filename->getOperator())
                {
                case no_pipe:
//                  allWritten = true;
                    break;
                default:
                    OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
                    addDependencySource(value, curGraph, activityExpr);
                    break;
                }
            }
            else
            {
                IHqlExpression * seq = querySequence(expr);
                assertex(seq && seq->queryValue());
                IHqlExpression * name = queryResultName(expr);
                OwnedHqlExpr value = createAttribute(resultAtom, LINK(seq), LINK(name));
                addDependencySource(value, curGraph, activityExpr);
            }
            return true;
        }
    case no_keydiff:
        {
            addRefExprDependency(expr->queryChild(0), curGraph, activityExpr);
            addRefExprDependency(expr->queryChild(1), curGraph, activityExpr);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_keypatch:
        {
            addRefExprDependency(expr->queryChild(0), curGraph, activityExpr);
            OwnedHqlExpr patchName = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(1)));
            addDependencyUse(patchName, curGraph, activityExpr);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_table:
        {
            IHqlExpression * filename = expr->queryChild(0);
            OwnedHqlExpr value = createAttribute(fileAtom, getNormalizedFilename(filename));
            addDependencyUse(value, curGraph, activityExpr);
            return !filename->isConstant();
        }
    case no_select:
        return isNewSelector(expr);
    case no_workunit_dataset:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, nameAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_getresult:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_getgraphresult:
        {
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(expr->queryChild(1)), LINK(expr->queryChild(2)));
            addDependencyUse(value, curGraph, activityExpr);
            return false;
        }
    case no_setgraphresult:
        {
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(expr->queryChild(1)), LINK(expr->queryChild(2)));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_ensureresult:
    case no_setresult:
    case no_extractresult:
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            OwnedHqlExpr value = createAttribute(resultAtom, LINK(sequence), LINK(name));
            addDependencySource(value, curGraph, activityExpr);
            return true;
        }
    case no_attr:
    case no_attr_link:
    case no_record:
    case no_field:
        return false; //no need to look any further
    default:
        return true;
    }
}


void EclResourcer::doAddChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    if (expr->queryTransformExtra())
        return;
    expr->setTransformExtraUnlinked(expr);

    if (addExprDependency(expr, graph, activityExpr))
    {
        ForEachChild(idx, expr)
            doAddChildDependencies(expr->queryChild(idx), graph, activityExpr);
    }
}

void EclResourcer::addChildDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    if (graph)
    {
        lockTransformMutex();
        doAddChildDependencies(expr, graph, activityExpr);
        unlockTransformMutex();
    }
}

void EclResourcer::addDependencies(IHqlExpression * expr, ResourceGraphInfo * graph, IHqlExpression * activityExpr)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (info && info->containsActivity)
    {
        if (info->isActivity)
        {
            if (info->gatheredDependencies)
                return;
            info->gatheredDependencies = true;
            graph = info->graph;
            activityExpr = expr;
        }
        if (addExprDependency(expr, graph, activityExpr))
        {
            unsigned first = getFirstActivityArgument(expr);
            unsigned last = first + getNumActivityArguments(expr);
            ForEachChild(idx, expr)
            {
                if ((idx >= first) && (idx < last))
                    addDependencies(expr->queryChild(idx), graph, activityExpr);
                else
                    addChildDependencies(expr->queryChild(idx), graph, activityExpr);
            }
        }
    }
    else
        addChildDependencies(expr, graph, activityExpr);

    ForEachItemIn(i, info->childDependents)
    {
        CChildDependent & cur = info->childDependents.item(i);
        if (isResourcedActivity(cur.projectedHoisted))
        {
            addDependencies(cur.projectedHoisted, NULL, NULL);

            ResourcerInfo * sourceInfo = queryResourceInfo(cur.projectedHoisted);
            if (cur.isSingleNode)
                sourceInfo->noteUsedFromChild();
            ResourceGraphLink * link = new ResourceGraphDependencyLink(sourceInfo->graph, cur.projectedHoisted, graph, expr);
            graph->dependsOn.append(*link);
            links.append(*link);
        }
    }
}

void EclResourcer::addDependencies(HqlExprArray & exprs)
{
    ForEachItemIn(idx, exprs)
        addDependencies(&exprs.item(idx), NULL, NULL);
}

void EclResourcer::spotUnbalancedSplitters(IHqlExpression * expr, unsigned whichSource, IHqlExpression * path, ResourceGraphInfo * graph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    if (graph && info->graph && info->graph != graph)
    {
        if ((info->currentSource == whichSource) && (info->pathToSplitter != path))
            graph->unbalancedExternalSources.append(*LINK(expr->queryBody()));
        info->currentSource = whichSource;
        info->pathToSplitter.set(path);
        return;
    }
    else
    {
        if (info->currentSource == whichSource)
        {
            if (info->pathToSplitter != path)
                info->balanced = false;
            return;
        }

        info->currentSource = whichSource;
        info->pathToSplitter.set(path);
    }

    if (info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        bool modify = false;
        if (num > 1)
        {
            switch (expr->getOperator())
            {
            case no_addfiles:
                if (expr->hasAttribute(_ordered_Atom) || expr->hasAttribute(_orderedPull_Atom) || isGrouped(expr))
                    modify = true;
                break;
            default:
                modify = true;
                break;
            }
        }

        unsigned last = first + num;
        for (unsigned idx = first; idx < last; idx++)
        {
            OwnedHqlExpr childPath = modify ? createAttribute(pathAtom, getSizetConstant(idx), LINK(path)) : LINK(path);
            spotUnbalancedSplitters(expr->queryChild(idx), whichSource, childPath, graph);
        }
    }


    //Now check dependencies between graphs (for roxie)
    if (!graph)
    {
        if (info->graph)
        {
            GraphLinkArray & graphLinks = info->graph->dependsOn;
            ForEachItemIn(i, graphLinks)
            {
                ResourceGraphLink & link = graphLinks.item(i);
                if (link.sinkNode == expr)
                {
                    OwnedHqlExpr childPath = createAttribute(dependencyAtom, LINK(link.sourceNode), LINK(path));
                    spotUnbalancedSplitters(link.sourceNode, whichSource, childPath, graph);
                }
            }
        }
        else
        {
            ForEachItemIn(i, links)
            {
                ResourceGraphLink & link = links.item(i);
                if (link.sinkNode == expr)
                {
                    OwnedHqlExpr childPath = createAttribute(dependencyAtom, LINK(link.sourceNode), LINK(path));
                    spotUnbalancedSplitters(link.sourceNode, whichSource, childPath, graph);
                }
            }
        }
    }
}

void EclResourcer::spotUnbalancedSplitters(HqlExprArray & exprs)
{
    unsigned curSource = 1;
    switch (targetClusterType)
    {
    case HThorCluster:
        break;
    case ThorLCRCluster:
        {
            //Thor only handles one graph at a time, so only walk expressions within a single graph.
            ForEachItemIn(i1, graphs)
            {
                ResourceGraphInfo & curGraph = graphs.item(i1);
                ForEachItemIn(i2, curGraph.sinks)
                {
                    ResourceGraphLink & cur = curGraph.sinks.item(i2);
                    spotUnbalancedSplitters(cur.sourceNode, curSource++, 0, &curGraph);
                }
            }
        }
        break;
    case RoxieCluster:
        {
            //Roxie pulls all at once, so need to analyse globally.
            ForEachItemIn(idx, exprs)
                spotUnbalancedSplitters(&exprs.item(idx), curSource++, 0, NULL);
            break;
        }
    }
}

void EclResourcer::spotSharedInputs(IHqlExpression * expr, ResourceGraphInfo * graph)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info)
        return;

    if (info->graph && info->graph != graph)
    {
        IHqlExpression * body = expr->queryBody();
        if (!graph->unbalancedExternalSources.contains(*body))
            graph->balancedExternalSources.append(*LINK(body));

        return;
    }

    if (info->isSplit())
    {
        //overload currentSource to track if we have visited this splitter before.  It cannot have value value NotFound up to now
        if (info->currentSource == NotFound)
            return;
        info->currentSource = NotFound;
    }

    if (info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        unsigned last = first + num;
        for (unsigned idx = first; idx < last; idx++)
        {
            spotSharedInputs(expr->queryChild(idx), graph);
        }
    }
}

void EclResourcer::spotSharedInputs()
{
    //Thor only handles one graph at a time, so only walk expressions within a single graph.
    ForEachItemIn(i1, graphs)
    {
        ResourceGraphInfo & curGraph = graphs.item(i1);
        HqlExprCopyArray visited;
        ForEachItemIn(i2, curGraph.sinks)
        {
            ResourceGraphLink & cur = curGraph.sinks.item(i2);
            IHqlExpression * curExpr = cur.sourceNode->queryBody();
            if (!visited.contains(*curExpr))
            {
                ResourcerInfo * info = queryResourceInfo(curExpr);
                if (!info->isExternalSpill() && !info->expandRatherThanSpill(true))
                {
                    spotSharedInputs(curExpr, &curGraph);
                    visited.append(*curExpr);
                }
            }
        }
    }
}

//------------------------------------------------------------------------------------------
// PASS6: Merge sub graphs that can share resources and don't have dependencies
// MORE: Once sources are merged, should try merging between trees.

static bool conditionsMatch(const HqlExprArray & left, const HqlExprArray & right)
{
    if (left.ordinality() != right.ordinality())
        return false;

    ForEachItemIn(i, left)
    {
        if (!left.contains(right.item(i)) || !right.contains(left.item(i)))
            return false;
    }
    return true;
}


bool EclResourcer::queryMergeGraphLink(ResourceGraphLink & link)
{
    if (link.linkKind == UnconditionalLink)
    {
        //Don't combine any dependencies 
        const GraphLinkArray & sinks = link.sourceGraph->sinks;
        ForEachItemIn(i1, sinks)
        {
            ResourceGraphLink & cur = sinks.item(i1);
            if (cur.sinkGraph && cur.sourceNode->isAction())
                return false;
        }

        //Roxie pulls all subgraphs at same time, so no problem with conditional links since handled at run time.
        if (options.noConditionalLinks)
            return true;

        //No conditionals in the sink graph=>will be executed just as frequently
        if (!link.sinkGraph->mergedConditionSource)
            return true;

        //Is this the only place this source graph is used?  If so, always fine to merge
        if (sinks.ordinality() == 1)
            return true;

        //1) if context the source graph is being merged into is unconditional, then it is ok [ could have conditional and unconditional paths to same graph]
        //2) if context is conditional, then we don't really want to do it unless the conditions on all sinks are identical, and the only links occur between these two graphs.
        //   (situation occurs with spill fed into two branches of a join).
        bool isConditionalInSinkGraph = false;
        bool accessedFromManyGraphs = false;
        ForEachItemIn(i, sinks)
        {
            ResourceGraphLink & cur = sinks.item(i);
            if (cur.sinkNode)
            {
                if (cur.sinkGraph != link.sinkGraph)
                    accessedFromManyGraphs = true;
                else
                {
                    if (!isConditionalInSinkGraph)
                    {
                        ResourcerInfo * sinkInfo = queryResourceInfo(cur.sinkNode);

                        //If this is conditional, don't merge if there is a link to another graph
                        if ((!cur.isDependency() && sinkInfo->isConditionExpr()) ||
                        //if (sinkInfo->isConditionExpr() ||
                            (!sinkInfo->isUnconditional() && sinkInfo->conditions.ordinality()))
                            isConditionalInSinkGraph = true;
                    }
                }
            }
        }

        if (isConditionalInSinkGraph && accessedFromManyGraphs)
            return false;

        return true;
    }

    return false;
}


unsigned EclResourcer::getMaxDepth() const
{
    unsigned maxDepth = 0;
    for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
    {
        unsigned depth = graphs.item(idx).getDepth();
        if (depth > maxDepth)
            maxDepth = depth;
    }
    return maxDepth;
}


void EclResourcer::mergeSubGraphs(unsigned pass)
{
    unsigned maxDepth = getMaxDepth();
    for (unsigned curDepth = maxDepth+1; curDepth-- != 0;)
    {
mergeAgain:
        for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
        {
            ResourceGraphInfo & cur = graphs.item(idx);
            if ((cur.getDepth() == curDepth) && !cur.isDead)
            {
                bool tryAgain;
                do
                {
                    tryAgain = false;
                    for (unsigned idxSource = 0; idxSource < cur.sources.ordinality(); /*incremented in loop*/)
                    {
                        ResourceGraphLink & curLink = cur.sources.item(idxSource);
                        ResourceGraphInfo * source = curLink.sourceGraph;
                        IHqlExpression * sourceNode = curLink.sourceNode;
                        bool tryToMerge;
                        bool expandSourceInPlace = queryResourceInfo(sourceNode)->expandRatherThanSpill(false);
                        if (pass == 0)
                            tryToMerge = !expandSourceInPlace;
                        else
                            tryToMerge = expandSourceInPlace;

                        if (tryToMerge)
                        {
                            bool ok = true;
                            ResourcerInfo * sourceResourceInfo = queryResourceInfo(sourceNode);
                            if (sourceResourceInfo->outputToUseForSpill && (targetClusterType == HThorCluster))
                            {
                                if (curLink.sinkNode != sourceResourceInfo->outputToUseForSpill)
                                    ok = false;
                            }
                            unsigned curSourceDepth = source->getDepth();
                            //MORE: Merging identical conditionals?
                            if (ok && queryMergeGraphLink(curLink) &&
                                !sourceResourceInfo->expandRatherThanSplit() &&
                                cur.mergeInSource(*source, *resourceLimit))
                            {
                                //NB: Following cannot remove sources below the current index.
                                replaceGraphReferences(source, &cur);
                                source->isDead = true;
#ifdef VERIFY_RESOURCING
                                checkRecursion(&cur);
#endif
                                unsigned newDepth = cur.getDepth();
                                //Unusual: The source we are merging with has just increased in depth, so any
                                //dependents have also increased in depth.  Need to try again at different depth
                                //to see if one of those will merge in.
                                if (newDepth > curSourceDepth)
                                {
                                    curDepth += (newDepth - curSourceDepth);
                                    goto mergeAgain;
                                }
                                //depth of this element has changed, so don't check to see if it merges with any other
                                //sources on this iteration.
                                if (newDepth != curDepth)
                                {
                                    tryAgain = false;
                                    break;
                                }
                                tryAgain = true;
                            }
                            else
                                idxSource++;
                        }
                        else
                            idxSource++;
                    }
                } while (tryAgain);
            }
        }
    }
}

void EclResourcer::mergeSiblings()
{
    unsigned maxDepth = getMaxDepth();
    for (unsigned curDepth = maxDepth+1; curDepth-- != 0;)
    {
        for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
        {
            ResourceGraphInfo & cur = graphs.item(idx);
            if ((cur.getDepth() == curDepth) && !cur.isDead)
            {
                ForEachItemIn(idxSource, cur.sources)
                {
                    ResourceGraphLink & curLink = cur.sources.item(idxSource);
                    ResourceGraphInfo * source = curLink.sourceGraph;
                    IHqlExpression * sourceNode = curLink.sourceNode;
                    ResourcerInfo * sourceInfo = queryResourceInfo(sourceNode);
                    if (sourceInfo->neverSplit || sourceInfo->expandRatherThanSplit())
                        continue;

                    for (unsigned iSink = 0; iSink < source->sinks.ordinality(); )
                    {
                        ResourceGraphLink & secondLink = source->sinks.item(iSink);
                        ResourceGraphInfo * sink = secondLink.sinkGraph;
                        if (sink && (sink != &cur) && !sink->isDead && sourceNode->queryBody() == secondLink.sourceNode->queryBody())
                        {
                            if (cur.mergeInSibling(*sink, *resourceLimit))
                            {
                                //NB: Following cannot remove sources below the current index.
                                replaceGraphReferences(sink, &cur);
                                sink->isDead = true;
                            }
                            else
                                iSink++;
                        }
                        else
                           iSink++;
                    }
                }
            }
        }
    }
}

void EclResourcer::mergeSubGraphs()
{
    for (unsigned pass=0; pass < 2; pass++)
        mergeSubGraphs(pass);

    if (options.combineSiblings)
        mergeSiblings();

    ForEachItemInRev(idx2, graphs)
    {
        if (graphs.item(idx2).isDead)
            graphs.remove(idx2);
    }
}

//------------------------------------------------------------------------------------------
// PASS7: Optimize aggregates off of splitters into through aggregates.

bool EclResourcer::optimizeAggregate(IHqlExpression * expr)
{
    if (!isSimpleAggregateResult(expr))
        return false;

    //expr is a no_extractresult 
    if (queryResourceInfo(expr)->childDependents.ordinality())
        return false;

    IHqlExpression * row2ds = expr->queryChild(0);          // no_datasetfromrow
    IHqlExpression * selectNth = row2ds->queryChild(0);

    ResourcerInfo * row2dsInfo = queryResourceInfo(row2ds);

    //If more than one set result for the same aggregate don't merge the aggregation because
    //it messes up the internal count.  Should really fix it and do multiple stores in the
    //through aggregate.
    if (row2dsInfo->numInternalUses() > 1)
        return false;

    //Be careful not to lose any spills...
    if (row2dsInfo->numExternalUses)
        return false;

    ResourcerInfo * selectNthInfo = queryResourceInfo(selectNth);
    if (selectNthInfo->numExternalUses)
        return false;

    IHqlExpression * aggregate = selectNth->queryChild(0);      // no_newaggregate
    IHqlExpression * parent = aggregate->queryChild(0);
    ResourcerInfo * info = queryResourceInfo(parent);
    if (info->numInternalUses() <= 1)
        return false;

    //ok, we can go ahead and merge.
    info->aggregates.append(*LINK(expr));
//  info->numExternalUses--;
//  info->numUses--;
    return true;
}


void EclResourcer::optimizeAggregates()
{
    for (unsigned idx = 0; idx < graphs.ordinality(); idx++)
    {
        ResourceGraphInfo & cur = graphs.item(idx);
        for (unsigned idxSink = 0; idxSink < cur.sinks.ordinality(); /*incremented in loop*/)
        {
            ResourceGraphLink & link = cur.sinks.item(idxSink);
            if ((link.sinkGraph == NULL) && optimizeAggregate(link.sourceNode))
                cur.sinks.remove(idxSink);
            else
                idxSink++;
        }
    }
}

//------------------------------------------------------------------------------------------
// PASS8: Improve efficiency by merging the split points slightly

IHqlExpression * EclResourcer::findPredecessor(IHqlExpression * expr, IHqlExpression * search, IHqlExpression * prev)
{
    if (expr == search)
        return prev;

    ResourcerInfo * info = queryResourceInfo(expr);
    if (info && info->containsActivity)
    {
        unsigned first = getFirstActivityArgument(expr);
        unsigned last = first + getNumActivityArguments(expr);
        for (unsigned idx=first; idx < last; idx++)
        {
            IHqlExpression * match = findPredecessor(expr->queryChild(idx), search, expr);
            if (match)
                return match;
        }
    }
    return NULL;
}

IHqlExpression * EclResourcer::findPredecessor(ResourcerInfo * search)
{
    ForEachItemIn(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);

        if (cur.sourceGraph == search->graph)
        {
            IHqlExpression * match = findPredecessor(cur.sourceNode, search->original, NULL);
            if (match)
                return match;
        }
    }
    return NULL;
}

void EclResourcer::moveExternalSpillPoints()
{
    if (options.minimizeSpillSize == 0)
        return;

    //if we have a external spill point where all the external outputs reduce their data significantly
    //either via a project or a filter, then it might be worth including those activities in the main
    //graph and ,if all external children reduce data, then may be best to filter 
    ForEachItemIn(idx, links)
    {
        ResourceGraphLink & cur = links.item(idx);
        if ((cur.linkKind == UnconditionalLink) && cur.sinkGraph)
        {
            while (lightweightAndReducesDatasetSize(cur.sinkNode))
            {
                ResourcerInfo * sourceInfo = queryResourceInfo(cur.sourceNode);
                if (!sourceInfo->isExternalSpill() || (sourceInfo->numExternalUses > options.minimizeSpillSize))
                    break;

                ResourcerInfo * sinkInfo = queryResourceInfo(cur.sinkNode);
                if (sinkInfo->numInternalUses() != 1)
                    break;

                IHqlExpression * sinkPred = findPredecessor(sinkInfo);
                sinkInfo->graph.set(cur.sourceGraph);
                sourceInfo->numExternalUses--;
                sinkInfo->numExternalUses++;
                cur.sourceNode.set(cur.sinkNode);
                cur.sinkNode.set(sinkPred);
            }
        }
    }
}


//------------------------------------------------------------------------------------------
// PASS9: Create a new expression tree representing the information

static HqlTransformerInfo childDependentReplacerInfo("ChildDependentReplacer");
class ChildDependentReplacer : public MergingHqlTransformer
{
public:
    ChildDependentReplacer(const HqlExprCopyArray & _childDependents, const HqlExprArray & _replacements) 
        : MergingHqlTransformer(childDependentReplacerInfo), childDependents(_childDependents), replacements(_replacements)
    {
    }

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        unsigned match = childDependents.find(*expr);
        if (match != NotFound)
            return LINK(&replacements.item(match));

        return MergingHqlTransformer::createTransformed(expr);
    }

protected:
    const HqlExprCopyArray & childDependents;
    const HqlExprArray & replacements;
};


static IHqlExpression * getScalarReplacement(CChildDependent & cur, ResourcerInfo * hoistedInfo, IHqlExpression * replacement)
{
    IHqlExpression * value = cur.original;
    //First skip any wrappers which are there to cause things to be hoisted.
    loop
    {
        node_operator op = value->getOperator();
        if ((op != no_globalscope) && (op != no_thisnode) && (op != no_evalonce))
            break;
        value = value->queryChild(0);
    }

    //Now modify the spilled result depending on how the spilled result was created (see EclHoistLocator::noteScalar() above)
    if (value->getOperator() == no_select)
    {
        IHqlExpression * field = value->queryChild(1);
        bool isNew;
        IHqlExpression * ds = querySelectorDataset(value, isNew);
        if(isNew || ds->isDatarow())
        {
            if (cur.hoisted != cur.projectedHoisted)
            {
                assertex(cur.projected);
                unsigned match = hoistedInfo->projected.find(*cur.projected);
                assertex(match != NotFound);
                IHqlExpression * projectedRecord = cur.projectedHoisted->queryRecord();
                IHqlExpression * projectedField = projectedRecord->queryChild(match);
                return createNewSelectExpr(LINK(replacement), LINK(projectedField));
            }
            return replaceSelectorDataset(value, replacement);
        }
        //Very unusual - can occur when a thisnode(somefield) is extracted from an allnodes.
        //It will have gone through the default case in the noteScalar() code
    }
    else if (value->getOperator() == no_createset)
    {
        IHqlExpression * record = replacement->queryRecord();
        IHqlExpression * field = record->queryChild(0);
        return createValue(no_createset, cur.original->getType(), LINK(replacement), createSelectExpr(LINK(replacement->queryNormalizedSelector()), LINK(field)));
    }

    IHqlExpression * record = replacement->queryRecord();
    return createNewSelectExpr(LINK(replacement), LINK(record->queryChild(0)));
}


IHqlExpression * EclResourcer::replaceResourcedReferences(ResourcerInfo * info, IHqlExpression * expr)
{
    if (!isAffectedByResourcing(expr))
        return LINK(expr);

    LinkedHqlExpr mapped = expr;
    if (info && (info->childDependents.ordinality()))
    {
        HqlExprCopyArray originals;
        HqlExprArray replacements;
        ForEachItemIn(i, info->childDependents)
        {
            CChildDependent & cur = info->childDependents.item(i);
            if (!isResourcedActivity(cur.projectedHoisted))
                continue;

            OwnedHqlExpr replacement = createResourced(cur.projectedHoisted, NULL, false, false);

            IHqlExpression * original = cur.original;
            if (!original->isDataset() && !original->isDatarow() && !original->isDictionary())
                replacement.setown(getScalarReplacement(cur, queryResourceInfo(cur.hoisted), replacement));

            originals.append(*original);
            replacements.append(*replacement.getClear());
        }

        if (originals.ordinality())
        {
            ChildDependentReplacer replacer(originals, replacements);
            mapped.setown(replacer.transformRoot(mapped));
        }
    }
    return mapped.getClear();
}

IHqlExpression * EclResourcer::doCreateResourced(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, bool expandInParent, bool defineSideEffect)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    node_operator op = expr->getOperator();
    HqlExprArray args;
    bool same = true;

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    OwnedHqlExpr transformed;
    switch (op)
    {
    case no_if:
    case no_choose:
    case no_chooseds:
        {
            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * resourced;
                if ((idx < first) || (idx >= last))
                    resourced = replaceResourcedReferences(info, child);
                else
                    resourced = createResourced(child, ownerGraph, expandInParent, false);
                if (child != resourced) 
                    same = false;
                args.append(*resourced);
            }
            break;
        }
    case no_case:
    case no_map:
        UNIMPLEMENTED;
    case no_keyed:
        return LINK(expr);
    case no_compound:
        transformed.setown(createResourced(expr->queryChild(1), ownerGraph, expandInParent, false));
        break;
    case no_executewhen:
        {
            args.append(*createResourced(expr->queryChild(0), ownerGraph, expandInParent, false));
            args.append(*createResourced(expr->queryChild(1), ownerGraph, expandInParent, false));
            assertex(args.item(1).getOperator() == no_callsideeffect);
            unwindChildren(args, expr, 2);
            same = false;
            break;
        }
    case no_select:
        {
            IHqlExpression * ds = expr->queryChild(0);
            OwnedHqlExpr newDs = createResourced(ds, ownerGraph, expandInParent, false);
            if (ds != newDs)
            {
                args.append(*LINK(newDs));
                unwindChildren(args, expr, 1);
                if (!expr->hasAttribute(newAtom) && isNewSelector(expr) && (newDs->getOperator() != no_select))
                    args.append(*LINK(queryNewSelectAttrExpr()));
                same = false;
            }
            break;
        }
    case no_join:
    case no_denormalize:
    case no_denormalizegroup:
        if (false)//if (isKeyedJoin(expr))
        {
            args.append(*createResourced(expr->queryChild(0), ownerGraph, expandInParent, false));
            args.append(*LINK(expr->queryChild(1)));
            unsigned max = expr->numChildren();
            for (unsigned idx=2; idx < max; idx++)
                args.append(*replaceResourcedReferences(info, expr->queryChild(idx)));
            same = false;
            break;
        }
        //fall through...
    default:
        {
            IHqlExpression * activeTable = NULL;
            // Check to see if the activity has a dataset which is in scope for the rest of its arguments.
            // If so we'll need to remap references from the children.
            if (hasActiveTopDataset(expr) && (first != last))
                activeTable = expr->queryChild(0);

            ForEachChild(idx, expr)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * resourced;
                if ((idx < first) || (idx >= last))
                {
                    LinkedHqlExpr mapped = child;
                    if (activeTable && isAffectedByResourcing(child))
                    {
                        IHqlExpression * activeTableTransformed = &args.item(0);
                        if (activeTable != activeTableTransformed)
                            mapped.setown(scopedReplaceSelector(child, activeTable, activeTableTransformed));
                    }
                    resourced = replaceResourcedReferences(info, mapped);
                }
                else
                    resourced = createResourced(child, ownerGraph, expandInParent, false);
                if (child != resourced) 
                    same = false;
                args.append(*resourced);
            }
        }
        break;
    }

    if (!transformed)
        transformed.setown(same ? LINK(expr) : expr->clone(args));

    if (!expandInParent)
    {
        if (!transformed->isAction())
            transformed.setown(info->createTransformedExpr(transformed));
        else if (defineSideEffect)
            transformed.setown(createValue(no_definesideeffect, LINK(transformed), createUniqueId()));
    }

    return transformed.getClear();
}

/*
 Need to be careful because result should not reuse the same expression tree unless that element is a splitter.

createResourced()
{
    if (!isActivity)
        if (!containsActivity)
            replace any refs to the activeTable with whatever it has been mapped to
        else
            recurse
    else if (!isDefinedInSameGraph)
        expand/create reader
        set active table
    else isSplitter and alreadyGeneratedForThisGraph
        return previous result
    else
        create transformed  
}
  */

void EclResourcer::doCheckRecursion(ResourceGraphInfo * graph, PointerArray & visited)
{
    visited.append(graph);
    ForEachItemIn(idxD, graph->dependsOn)
        checkRecursion(graph->dependsOn.item(idxD).sourceGraph, visited);
    ForEachItemIn(idxS, graph->sources)
        checkRecursion(graph->sources.item(idxS).sourceGraph, visited);
    visited.pop();
}

void EclResourcer::checkRecursion(ResourceGraphInfo * graph, PointerArray & visited)
{
    if (visited.find(graph) != NotFound)
        throwUnexpected();
    doCheckRecursion(graph, visited);
}

void EclResourcer::checkRecursion(ResourceGraphInfo * graph)
{
    PointerArray visited;
    doCheckRecursion(graph, visited);
}

IHqlExpression * EclResourcer::createResourced(IHqlExpression * expr, ResourceGraphInfo * ownerGraph, bool expandInParent, bool defineSideEffect)
{
    ResourcerInfo * info = queryResourceInfo(expr);
    if (!info || !info->containsActivity)
        return replaceResourcedReferences(info, expr);

    if (!info->isActivity)
    {
        assertex(!defineSideEffect);
        HqlExprArray args;
        bool same = true;
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            IHqlExpression * curResourced = createResourced(cur, ownerGraph, expandInParent, false);
            args.append(*curResourced);
            if (cur != curResourced)
                same = false;
        }
        if (same)
            return LINK(expr);
        return expr->clone(args);
    }

    if (info->graph != ownerGraph)
    {
        assertex(!defineSideEffect);
        bool isShared = options.optimizeSharedInputs && ownerGraph && ownerGraph->isSharedInput(expr);
        if (isShared)
        {
            IHqlExpression * mapped = ownerGraph->queryMappedSharedInput(expr->queryBody());
            if (mapped)
                return LINK(mapped);
        }

        IHqlExpression * source;
        if (info->expandRatherThanSpill(true))
        {
            bool expandChildInParent = options.minimiseSpills ? expandInParent : true;
            OwnedHqlExpr resourced = doCreateResourced(expr, ownerGraph, expandChildInParent, false);
            if (queryAddUniqueToActivity(resourced))
                source = appendUniqueAttr(resourced);
            else
                source = LINK(resourced);
        }
        else
        {
            if (!expr->isAction())
            {
                OwnedHqlExpr reason;
                if (ownerGraph && options.checkResources())
                {
                    StringBuffer reasonText;
                    ownerGraph->getMergeFailReason(reasonText, info->graph, *resourceLimit);
                    if (reasonText.length())
                    {
                        reasonText.insert(0, "Resource limit spill: ");
                        reason.setown(createAttribute(_spillReason_Atom, createConstant(reasonText.str())));
                    }
                }

                source = info->createSpilledRead(reason);
            }
            else
            {
                IHqlExpression * uid = info->transformed->queryAttribute(_uid_Atom);
                source = createValue(no_callsideeffect, makeVoidType(), LINK(uid));
                //source = LINK(info->transformed);
            }
        }

        if (isShared)
        {
            source = createDatasetF(no_split, source, createAttribute(balancedAtom), createUniqueId(), NULL);
            ownerGraph->addSharedInput(expr->queryBody(), source);
        }

        return source;
    }

    if (!expandInParent && info->transformed && info->isSplit())
    {
        return LINK(info->transformed);
    }

    OwnedHqlExpr resourced = doCreateResourced(expr, ownerGraph, expandInParent, defineSideEffect);
    if (queryAddUniqueToActivity(resourced))// && !resourced->hasAttribute(_internal_Atom))
        resourced.setown(appendUniqueAttr(resourced));

    if (!expandInParent)
    {
        info->transformed = resourced;
    }
    return resourced.getClear();
}

void EclResourcer::createResourced(ResourceGraphInfo * graph, HqlExprArray & transformed)
{
    if (graph->createdGraph || graph->isDead)
        return;
    if (!graph->containsActiveSinks() && (!graph->hasConditionSource))
        return;

#ifdef VERIFY_RESOURCING
    checkRecursion(graph);
#endif
//  DBGLOG("Prepare to CreateResourced(%p)", graph);
    if (graph->startedGeneratingResourced)
        throwError(HQLWRN_RecursiveDependendencies);

    graph->startedGeneratingResourced = true;
    ForEachItemIn(idxD, graph->dependsOn)
        createResourced(graph->dependsOn.item(idxD).sourceGraph, transformed);
    ForEachItemIn(idxS, graph->sources)
        createResourced(graph->sources.item(idxS).sourceGraph, transformed);

//  DBGLOG("Create resourced %p", graph);

    //First generate the graphs for all the unconditional sinks
    HqlExprArray args;
    ForEachItemIn(idx, graph->sinks)
    {
        ResourceGraphLink & sink = graph->sinks.item(idx);
        IHqlExpression * sinkNode = sink.sourceNode;
        ResourcerInfo * info = queryResourceInfo(sinkNode);
        //If graph is unconditional, any condition sinks are forced to be generated (and spilt)
        if (!info->transformed)
        {
            // if it is a spiller, then it will be generated from another sink
            if (!info->isExternalSpill())
            {
                IHqlExpression * resourced = createResourced(sinkNode, graph, false, sinkNode->isAction() && sink.sinkGraph);
                assertex(info->transformed);
                args.append(*resourced);
            }
        }
    }

    ForEachItemIn(i2, graph->sinks)
    {
        ResourceGraphLink & sink = graph->sinks.item(i2);
        IHqlExpression * sinkNode = sink.sourceNode;
        ResourcerInfo * info = queryResourceInfo(sinkNode);
        IHqlExpression * splitter = info->splitterOutput;
        if (splitter && !args.contains(*splitter))
            args.append(*LINK(splitter));
    }

    if (args.ordinality() == 0)
        graph->isDead = true;
    else
    {
        if (options.useGraphResults)
            args.append(*createAttribute(childAtom));
        graph->createdGraph.setown(createValue(no_subgraph, makeVoidType(), args));
        transformed.append(*LINK(graph->createdGraph));
    }
}


void EclResourcer::inheritRedundantDependencies(ResourceGraphInfo * thisGraph)
{
    if (thisGraph->inheritedExpandedDependencies)
        return;
    thisGraph->inheritedExpandedDependencies = true;
    ForEachItemIn(idx, thisGraph->sources)
    {
        ResourceGraphLink & cur = thisGraph->sources.item(idx);
        if (cur.isRedundantLink())
        {
            inheritRedundantDependencies(cur.sourceGraph);
            ForEachItemIn(i, cur.sourceGraph->dependsOn)
            {
                ResourceGraphLink & curDepend = cur.sourceGraph->dependsOn.item(i);
                ResourceGraphLink * link = new ResourceGraphDependencyLink(curDepend.sourceGraph, curDepend.sourceNode, thisGraph, cur.sinkNode);
                thisGraph->dependsOn.append(*link);
                links.append(*link);
            }
        }
    }
}


void EclResourcer::createResourced(HqlExprArray & transformed)
{
    //Before removing null links (e.g., where the source graph is expanded inline), need to make sure
    //dependencies are cloned, otherwise graphs can be generated in the wrong order
    ForEachItemIn(idx1, graphs)
        inheritRedundantDependencies(&graphs.item(idx1));

    ForEachItemInRev(idx2, links)
    {
        ResourceGraphLink & cur = links.item(idx2);
        if (cur.isRedundantLink())
            removeLink(cur, true);
    }

    ForEachItemIn(idx3, graphs)
        createResourced(&graphs.item(idx3), transformed);
}

static int compareGraphDepth(CInterface * * _l, CInterface * * _r)
{
    ResourceGraphInfo * l = (ResourceGraphInfo *)*_l;
    ResourceGraphInfo * r = (ResourceGraphInfo *)*_r;
    return l->getDepth() - r->getDepth();
}

static int compareLinkDepth(CInterface * * _l, CInterface * * _r)
{
    ResourceGraphLink * l = (ResourceGraphLink *)*_l;
    ResourceGraphLink * r = (ResourceGraphLink *)*_r;
    int diff = l->sourceGraph->getDepth() - r->sourceGraph->getDepth();
    if (diff) return diff;
    if (l->sinkGraph)
        if (r->sinkGraph)
            return l->sinkGraph->getDepth() - r->sinkGraph->getDepth();
        else
            return -1;
    else
        if (r->sinkGraph)
            return +1;
        else
            return 0;
}

void EclResourcer::display(StringBuffer & out)
{
    CIArrayOf<ResourceGraphInfo> sortedGraphs;
    ForEachItemIn(j1, graphs)
        sortedGraphs.append(OLINK(graphs.item(j1)));
    sortedGraphs.sort(compareGraphDepth);

    out.append("Graphs:\n");
    ForEachItemIn(i, sortedGraphs)
    {
        ResourceGraphInfo & cur = sortedGraphs.item(i);
        out.appendf("%d: depth(%d) uncond(%d) cond(%d) %s {%p}\n", i, cur.getDepth(), cur.isUnconditional, cur.hasConditionSource, cur.isDead ? "dead" : "", &cur);
        ForEachItemIn(j, cur.sources)
        {
            ResourceGraphLink & link = cur.sources.item(j);
            out.appendf("  Source: %p %s\n", link.sinkNode.get(), getOpString(link.sinkNode->getOperator()));
        }
        ForEachItemIn(k, cur.sinks)
        {
            ResourceGraphLink & link = cur.sinks.item(k);
            IHqlExpression * sourceNode = link.sourceNode;
            ResourcerInfo * sourceInfo = queryResourceInfo(sourceNode);
            out.appendf("  Sink: %p %s cond(%d,%d) int(%d) ext(%d)\n", sourceNode, getOpString(sourceNode->getOperator()), sourceInfo->conditions.ordinality(), sourceInfo->conditionSourceCount, sourceInfo->numInternalUses(), sourceInfo->numExternalUses);
        }
        ForEachItemIn(i3, dependencySource.graphs)
        {
            if (&dependencySource.graphs.item(i3) == &cur)
            {
                StringBuffer s;
                toECL(&dependencySource.search.item(i3), s);
                out.appendf("  Creates: %s\n", s.str());
            }
        }
    }
    out.append("Links:\n");

    CIArrayOf<ResourceGraphLink> sortedLinks;
    ForEachItemIn(j2, links)
        sortedLinks.append(OLINK(links.item(j2)));
    sortedLinks.sort(compareLinkDepth);
    ForEachItemIn(i2, sortedLinks)
    {
        ResourceGraphLink & link = sortedLinks.item(i2);
        unsigned len = out.length();
        out.appendf("  Source: %d %s", sortedGraphs.find(*link.sourceGraph), getOpString(link.sourceNode->getOperator()));
        if (link.sinkNode)
        {
            out.padTo(len+30);
            out.appendf("  Sink: %d %s", sortedGraphs.find(*link.sinkGraph), getOpString(link.sinkNode->getOperator()));
        }
        if (link.linkKind == SequenceLink)
            out.append(" <sequence>");
        out.newline();
    }
}

void EclResourcer::trace()
{
    StringBuffer s;
    display(s);
    DBGLOG("%s", s.str());
}

//---------------------------------------------------------------------------

void EclResourcer::resourceGraph(HqlExprArray & exprs, HqlExprArray & transformed)
{
    //NB: This only resources a single level of queries.  SubQueries should be resourced in a separate
    //pass so that commonality between different activities/subgraphs isn't introduced/messed up.
    findSplitPoints(exprs);
    createInitialGraphs(exprs);
    markConditions(exprs);
    if (options.checkResources())
        resourceSubGraphs(exprs);
    addDependencies(exprs);
#ifdef TRACE_RESOURCING
    trace();
#endif
    mergeSubGraphs();
#ifdef TRACE_RESOURCING
    trace();
#endif
    spotUnbalancedSplitters(exprs);
    if (options.optimizeSharedInputs)
        spotSharedInputs();

    if (spotThroughAggregate)
        optimizeAggregates();
    moveExternalSpillPoints();
    createResourced(transformed);
}


void EclResourcer::resourceRemoteGraph(HqlExprArray & exprs, HqlExprArray & transformed)
{
    //NB: This only resources a single level of queries.  SubQueries should be resourced in a separate
    //pass so that commonality between different activities/subgraphs isn't introduced/messed up.
    findSplitPoints(exprs);
    createInitialRemoteGraphs(exprs);
    markConditions(exprs);
    addDependencies(exprs);
#ifdef TRACE_RESOURCING
    trace();
#endif
    mergeSubGraphs();
#ifdef TRACE_RESOURCING
    trace();
#endif
    createResourced(transformed);
}


//---------------------------------------------------------------------------

void expandLists(HqlExprArray & args, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_comma:
    case no_compound:
    case no_parallel:       
    case no_actionlist:
        // for the moment, expand root parallel nodes, it generates much better code.
        // I should really come up with a better way of implementing sequential/parallel.
        {
            ForEachChild(idx, expr)
                expandLists(args, expr->queryChild(idx));
            break;
        }
    default:
        args.append(*LINK(expr));
        break;
    }
}

IHqlExpression * resourceThorGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr)
{
    HqlExprArray transformed;
    {
        EclResourcer resourcer(translator.queryErrors(), translator.wu(), targetClusterType, clusterSize, translator.queryOptions());
        if (graphIdExpr)
            resourcer.setNewChildQuery(graphIdExpr, 0);

        HqlExprArray exprs;
        expandLists(exprs, expr);
        resourcer.resourceGraph(exprs, transformed);
    }
    hoistNestedCompound(translator, transformed);
    return createActionList(transformed);
}


static IHqlExpression * doResourceGraph(HqlCppTranslator & translator, HqlExprCopyArray * activeRows, IHqlExpression * expr, 
                                        ClusterType targetClusterType, unsigned clusterSize,
                                        IHqlExpression * graphIdExpr, unsigned * numResults, bool isChild, bool useGraphResults, bool sequential)
{
    HqlExprArray transformed;
    {
        EclResourcer resourcer(translator.queryErrors(), translator.wu(), targetClusterType, clusterSize, translator.queryOptions());
        if (isChild)
            resourcer.setChildQuery(true);
        resourcer.setNewChildQuery(graphIdExpr, *numResults);
        resourcer.setUseGraphResults(useGraphResults);
        resourcer.setSequential(sequential);

        if (activeRows)
            resourcer.tagActiveCursors(*activeRows);

        HqlExprArray exprs;
        expandLists(exprs, expr);

        resourcer.resourceGraph(exprs, transformed);
        *numResults = resourcer.numGraphResults();
    }
    hoistNestedCompound(translator, transformed);
    return createActionList(transformed);
}


IHqlExpression * resourceLibraryGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize, IHqlExpression * graphIdExpr, unsigned * numResults)
{
    return doResourceGraph(translator, NULL, expr, targetClusterType, clusterSize, graphIdExpr, numResults, false, true, false);       //?? what value for isChild (e.g., thor library call).  Need to gen twice?
}


IHqlExpression * resourceNewChildGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned * numResults, bool sequential)
{
    return doResourceGraph(translator, &activeRows, expr, targetClusterType, 0, graphIdExpr, numResults, true, true, sequential);
}

IHqlExpression * resourceLoopGraph(HqlCppTranslator & translator, HqlExprCopyArray & activeRows, IHqlExpression * expr, ClusterType targetClusterType, IHqlExpression * graphIdExpr, unsigned * numResults, bool insideChildQuery)
{
    return doResourceGraph(translator, &activeRows, expr, targetClusterType, 0, graphIdExpr, numResults, insideChildQuery, true, false);
}

IHqlExpression * resourceRemoteGraph(HqlCppTranslator & translator, IHqlExpression * expr, ClusterType targetClusterType, unsigned clusterSize)
{
    HqlExprArray transformed;
    {
        EclResourcer resourcer(translator.queryErrors(), translator.wu(), targetClusterType, clusterSize, translator.queryOptions());

        HqlExprArray exprs;
        expandLists(exprs, expr);

        resourcer.resourceRemoteGraph(exprs, transformed);
    }
    hoistNestedCompound(translator, transformed);
    return createActionList(transformed);
}


/*
Conditions:

They are nasty.  We process the tree in two passes.  First we tag anything which must be evaluated, and
save a list of condition statements to process later.
Second pass we tag conditionals.
a) all left and right branches of a condition are tagged. [conditionSourceCount]
b) all conditional expressions are tagged with the conditions they are evaluated for.
   [if the condition lists match then it should be possible to merge the graphs]
c) The spill count for an node should ignore the number of links from conditional graphs,
   but should add the number of conditions.
d) if (a, b(f1) +b(f2), c) needs to link b twice though!

*/

/*
This transformer converts spill activities to no_dataset/no_output, and also converts splitters of splitters into
a single splitter.
*/

class SpillActivityTransformer : public NewHqlTransformer
{
public:
    SpillActivityTransformer();

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    bool isUnbalanced(IHqlExpression * body)
    {
        ANewTransformInfo * info = queryTransformExtra(body);
        return info->spareByte1 != 0;
    }
    void setUnbalanced(IHqlExpression * body)
    {
        ANewTransformInfo * info = queryTransformExtra(body);
        info->spareByte1 = true;
    }
};

static HqlTransformerInfo spillActivityTransformerInfo("SpillActivityTransformer");
SpillActivityTransformer::SpillActivityTransformer() 
: NewHqlTransformer(spillActivityTransformerInfo)
{ 
}

void SpillActivityTransformer::analyseExpr(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (alreadyVisited(body))
        return;
    if (body->getOperator() == no_split)
    {
        IHqlExpression * input = body->queryChild(0);
        if (input->getOperator() == no_split)
        {
            loop
            {
                IHqlExpression * cur = input->queryChild(0);
                if (cur->getOperator() != no_split)
                    break;
                input = cur;
            }
            if (!body->hasAttribute(balancedAtom))
                setUnbalanced(input->queryBody());
        }
    }
    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * SpillActivityTransformer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * annotation = queryTransformAnnotation(expr);
    if (annotation)
        return annotation;

    switch (expr->getOperator())
    {
    case no_split:
        {
            IHqlExpression * input = expr->queryChild(0);
            if (input->getOperator() == no_split)
                return transform(input);
            OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
            if (transformed->hasAttribute(balancedAtom) && isUnbalanced(expr))
                return removeProperty(transformed, balancedAtom);
            return transformed.getClear();
        }
    case no_writespill:
        {
            HqlExprArray args;
            transformChildren(expr, args);
            return createValue(no_output, makeVoidType(), args);
        }
    case no_commonspill:
        return transform(expr->queryChild(0));
    case no_readspill:
        {
            OwnedHqlExpr ds = transform(expr->queryChild(0));
            HqlExprArray args;
            args.append(*transform(expr->queryChild(1)));
            args.append(*LINK(ds->queryRecord()));
            ForEachChildFrom(i, expr, 2)
            {
                IHqlExpression * cur = expr->queryChild(i);
                args.append(*transform(cur));
            }
            IHqlExpression * recordCountAttr = queryRecordCountInfo(expr);
            if (recordCountAttr)
                args.append(*LINK(recordCountAttr));
            return createDataset(no_table, args);
        }
    }
    return NewHqlTransformer::createTransformed(expr);
}

IHqlExpression * convertSpillsToActivities(IHqlExpression * expr)
{
    SpillActivityTransformer transformer;
    transformer.analyse(expr, 0);
    return transformer.transformRoot(expr);
}
